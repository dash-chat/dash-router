//! Size-aware packing of wire messages (spec 2026-09-22 §3.3): gossip
//! refuses messages over its `max_message_size`, so every broadcast is
//! split to fit a byte budget the embedder chooses.

use std::collections::{BTreeMap, BTreeSet};

use dash_router_core::{Entry, Interest, Op, Ranges, Seq, WireLog, WireMessage, group_ops};
use polestar::prelude::Id;
use serde::{Serialize, de::DeserializeOwned};

/// Bytes kept free under the gossip layer's `max_message_size`: Dash
/// Chat's signed CBOR envelope (~150 bytes), iroh-gossip's per-message
/// framing (p2panda-net checks only the payload against the limit), and
/// slack.
pub const WIRE_HEADROOM_BYTES: usize = 296;

/// [`wire_budget`] of p2panda-net's default `max_message_size` (4096);
/// the budget for a transport that reports no limit.
pub const DEFAULT_MAX_WIRE_BYTES: usize = wire_budget(4096);

/// The packing budget for an overlay whose messages are capped at
/// `max_message_size` bytes.
pub const fn wire_budget(max_message_size: usize) -> usize {
    max_message_size.saturating_sub(WIRE_HEADROOM_BYTES)
}

fn have_len<N, L>(sender: N, batch: &[(L, Seq, Op)]) -> usize
where
    N: Id + Serialize + DeserializeOwned,
    L: WireLog,
{
    WireMessage::have(sender, group_ops(batch.to_vec()))
        .encode()
        .len()
}

/// Greedily pack `ops` (already in `(log, seq)` order) into Haves that
/// each encode to at most `budget` bytes. An op that cannot fit alone is
/// sent header-only; a header that cannot fit alone is dropped and
/// counted (second return value).
pub fn pack_have<N, L>(
    sender: N,
    ops: Vec<(L, Seq, Op)>,
    budget: usize,
) -> (Vec<WireMessage<N, L>>, u64)
where
    N: Id + Serialize + DeserializeOwned,
    L: WireLog,
{
    let mut msgs = Vec::new();
    let mut dropped = 0u64;
    let mut batch: Vec<(L, Seq, Op)> = Vec::new();
    for item in ops {
        batch.push(item);
        if have_len(sender, &batch) <= budget {
            continue;
        }
        let mut last = batch.pop().expect("just pushed");
        if !batch.is_empty() {
            msgs.push(WireMessage::have(
                sender,
                group_ops(std::mem::take(&mut batch)),
            ));
        }
        // `last` alone.
        if have_len(sender, std::slice::from_ref(&last)) > budget {
            last.2.payload = None;
            if have_len(sender, std::slice::from_ref(&last)) > budget {
                dropped += 1;
                continue;
            }
        }
        batch.push(last);
    }
    if !batch.is_empty() {
        msgs.push(WireMessage::have(sender, group_ops(batch)));
    }
    (msgs, dropped)
}

/// Pack a Want (spec 2026-09-29 §5.2). Whole entries are packed greedily,
/// several channels to a frame. An entry that does not fit a frame alone
/// is cut into author-prefix scopes that do: the authors are ordered by
/// prefix, cut only where the prefix changes (authors sharing a prefix
/// stay together), and each slice is scoped `[p_k, p_{k+1} - 1]` so every
/// prefix falls in exactly one slice. A slice is a complete Interest, so
/// the receiver needs no reassembly and a lost frame under-asks only its
/// interval. A run of same-prefix authors (or a single author) whose
/// ranges alone exceed the budget is truncated to a *closed* prefix of its
/// range boundaries, or dropped from the have: either claims less than is
/// held, so the emitter is over-answered, never under-answered. Truncated
/// authors are counted (second return value).
pub fn pack_want<N, L>(
    sender: N,
    interest: Interest<L>,
    budget: usize,
) -> (Vec<WireMessage<N, L>>, u64)
where
    N: Id + Serialize + DeserializeOwned,
    L: WireLog,
{
    let len = |i: &Interest<L>| WireMessage::want(sender, i.clone()).encode().len();
    let mut msgs = Vec::new();
    let mut truncated = 0u64;
    let mut cur: Interest<L> = Interest::empty();
    let flush = |msgs: &mut Vec<WireMessage<N, L>>, cur: &mut Interest<L>| {
        if !cur.is_empty() {
            msgs.push(WireMessage::want(sender, std::mem::take(cur)));
        }
    };
    for (c, entry) in interest.entries() {
        let single = Interest::single(*c, entry.clone());
        if len(&single) <= budget {
            let mut trial = cur.clone();
            trial.insert(*c, entry.clone());
            if len(&trial) <= budget {
                cur = trial;
            } else {
                flush(&mut msgs, &mut cur);
                cur = single;
            }
            continue;
        }
        flush(&mut msgs, &mut cur);
        for slice in slice_entry::<L>(c, entry, budget, &len, &mut truncated) {
            msgs.push(WireMessage::want(sender, Interest::single(*c, slice)));
        }
    }
    flush(&mut msgs, &mut cur);
    (msgs, truncated)
}

/// postcard's varint width for a `u32`.
fn varint_len(v: u32) -> usize {
    match v {
        0..=0x7f => 1,
        0x80..=0x3fff => 2,
        0x4000..=0x1f_ffff => 3,
        0x20_0000..=0xfff_ffff => 4,
        _ => 5,
    }
}

/// Cut `entry` into scoped slices that each fit `budget` (see
/// [`pack_want`]). Cuts happen only between prefix runs.
fn slice_entry<L: WireLog>(
    c: &L::Channel,
    entry: &Entry<L>,
    budget: usize,
    len: &impl Fn(&Interest<L>) -> usize,
    truncated: &mut u64,
) -> Vec<Entry<L>> {
    // Authors by (prefix, author): the map is in author order, which need
    // not be prefix order for an arbitrary author type.
    let mut authors: Vec<(u32, &L::Author, &Ranges)> = entry
        .have
        .iter()
        .map(|(a, r)| (L::author_prefix(a), a, r))
        .collect();
    authors.sort_by(|x, y| (x.0, x.1).cmp(&(y.0, y.1)));
    // Runs of equal prefix, each run kept whole.
    type Run<L> = Vec<(<L as dash_router_core::Log>::Author, Ranges)>;
    let mut runs: Vec<(u32, Run<L>)> = Vec::new();
    for (p, a, r) in authors {
        match runs.last_mut() {
            Some((rp, run)) if *rp == p => run.push((*a, r.clone())),
            _ => runs.push((p, vec![(*a, r.clone())])),
        }
    }
    // A slice's real bounds are prefixes, which postcard encodes as up
    // to five bytes each, so measure with the entry's own bounds and keep
    // room for the widest possible pair.
    let slack = (5 - varint_len(entry.lo)) + (5 - varint_len(entry.hi));
    let fits = |lo: u32, hi: u32, have: &BTreeMap<L::Author, Ranges>| {
        Entry::<L>::new(lo, hi, have.clone())
            .ok()
            .is_some_and(|e| len(&Interest::single(*c, e)) + slack <= budget)
    };
    // Greedy slices of whole runs. `slices[k] = (first prefix, have)`.
    let mut slices: Vec<(u32, BTreeMap<L::Author, Ranges>)> = Vec::new();
    let mut cur: BTreeMap<L::Author, Ranges> = BTreeMap::new();
    let mut cur_first: Option<u32> = None;
    for (p, run) in runs {
        let mut trial = cur.clone();
        trial.extend(run.iter().cloned());
        // Bounds don't affect the encoded size, so test with the widest.
        if fits(entry.lo, entry.hi, &trial) {
            cur = trial;
            cur_first.get_or_insert(p);
            continue;
        }
        if let Some(first) = cur_first.take() {
            slices.push((first, std::mem::take(&mut cur)));
        }
        // The run alone, shrunk until it fits: drop the last closed range
        // (or the open tail) of the last author, then the author itself.
        let mut run: BTreeMap<L::Author, Ranges> = run.into_iter().collect();
        let mut touched: BTreeSet<L::Author> = BTreeSet::new();
        while !run.is_empty() && !fits(entry.lo, entry.hi, &run) {
            let (&a, r) = run.iter().next_back().expect("non-empty");
            let b = r.boundaries();
            let keep = if b.len() % 2 == 1 {
                b.len() - 1
            } else {
                b.len().saturating_sub(2)
            };
            touched.insert(a);
            if keep == 0 {
                run.remove(&a);
            } else {
                run.insert(
                    a,
                    Ranges::from_boundaries(b[..keep].to_vec()).expect("a prefix stays ordered"),
                );
            }
        }
        *truncated += touched.len() as u64;
        cur = run;
        cur_first = Some(p);
    }
    if let Some(first) = cur_first {
        slices.push((first, cur));
    }
    if slices.is_empty() {
        // Nothing survived truncation. A pure want over the whole scope is
        // the last resort; if even that does not fit, the channel cannot
        // be asked about in this budget at all.
        if fits(entry.lo, entry.hi, &BTreeMap::new()) {
            slices.push((entry.lo, BTreeMap::new()));
        } else {
            *truncated += 1;
            return Vec::new();
        }
    }
    // Scope each slice: the first from `entry.lo`, each up to the next
    // slice's first prefix, the last to `entry.hi`.
    let n = slices.len();
    slices
        .iter()
        .enumerate()
        .map(|(k, (first, have))| {
            let lo = if k == 0 { entry.lo } else { *first };
            let hi = if k + 1 == n {
                entry.hi
            } else {
                slices[k + 1].0 - 1
            };
            Entry::new(lo, hi, have.clone()).expect("cuts fall between prefix runs")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dash_router_core::{Log, Op, Pair, WireBody};

    fn op(n: usize) -> Op {
        Op {
            header: vec![7; 40],
            payload: Some(vec![1; n]),
        }
    }

    #[test]
    fn have_splits_to_fit_budget_and_keeps_order() {
        let ops: Vec<(u8, Seq, Op)> = (0..12u32).map(|q| (q as u8 / 4, q, op(300))).collect();
        let (msgs, dropped) = pack_have(1u32, ops.clone(), 1000);
        assert_eq!(dropped, 0);
        assert!(msgs.len() >= 4, "12 × ~350 bytes cannot fit in 3 × 1000");
        let mut seen = Vec::new();
        for m in &msgs {
            assert!(m.encode().len() <= 1000, "message over budget");
            match &m.body {
                WireBody::Have(groups) => {
                    for (log, seqs) in groups {
                        for (seq, _) in seqs {
                            seen.push((*log, *seq));
                        }
                    }
                }
                _ => panic!("not a Have"),
            }
        }
        assert_eq!(
            seen,
            ops.iter().map(|(l, q, _)| (*l, *q)).collect::<Vec<_>>()
        );
    }

    /// Review focus 4.
    #[test]
    fn oversize_op_goes_header_only() {
        let ops = vec![
            (0u8, 0u32, op(10)),
            (0u8, 1u32, op(5000)),
            (0u8, 2u32, op(10)),
        ];
        let (msgs, dropped) = pack_have(1u32, ops, 1000);
        assert_eq!(dropped, 0);
        let all: Vec<(Seq, Op)> = msgs
            .iter()
            .flat_map(|m| match &m.body {
                WireBody::Have(g) => g.iter().flat_map(|(_, s)| s.clone()).collect::<Vec<_>>(),
                _ => vec![],
            })
            .collect();
        assert_eq!(all.len(), 3, "the big op is still advertised");
        assert!(all[1].1.payload.is_none(), "…without its payload");
        assert!(all[0].1.payload.is_some() && all[2].1.payload.is_some());
        assert!(msgs.iter().all(|m| m.encode().len() <= 1000));
    }

    #[test]
    fn header_too_big_for_budget_is_dropped_and_counted() {
        let huge = Op {
            header: vec![1; 2000],
            payload: None,
        };
        let (msgs, dropped) = pack_have(1u32, vec![(0u8, 0u32, huge)], 1000);
        assert!(msgs.is_empty());
        assert_eq!(dropped, 1);
    }

    fn scopes_of(msgs: &[WireMessage<u32, Pair>], c: u8) -> Vec<(u32, u32, Vec<u8>)> {
        msgs.iter()
            .filter_map(|m| match &m.body {
                WireBody::Want(i) => i
                    .get(&c)
                    .map(|e| (e.lo, e.hi, e.have.keys().copied().collect())),
                WireBody::Have(_) => None,
            })
            .collect()
    }

    /// Whole entries pack several channels to a frame, split across frames
    /// where they must, each entry unchanged.
    #[test]
    fn want_packs_whole_entries_across_frames() {
        let mut interest: Interest<Pair> = Interest::empty();
        for c in 0..50u8 {
            interest.insert(c, Entry::whole(BTreeMap::from([(1u8, Ranges::from(3))])));
        }
        let (msgs, truncated) = pack_want(1u32, interest.clone(), 120);
        assert_eq!(truncated, 0);
        assert!(msgs.len() > 1);
        assert!(msgs.iter().all(|m| m.encode().len() <= 120));
        let mut got: Interest<Pair> = Interest::empty();
        for m in &msgs {
            if let WireBody::Want(i) = &m.body {
                for (c, e) in i.entries() {
                    assert!(got.get(c).is_none(), "each channel in exactly one frame");
                    got.insert(*c, e.clone());
                }
            }
        }
        assert_eq!(got, interest);
    }

    /// A channel with more authors than fit a frame is cut into scoped
    /// slices that tile the prefix space: every author lands in the slice
    /// whose scope holds its prefix, and no prefix is left unasked.
    #[test]
    fn want_slices_a_big_channel_into_scopes_that_tile() {
        let interest: Interest<Pair> = Interest::single(
            1u8,
            Entry::whole((0..60u8).map(|a| (a, Ranges::from(3))).collect()),
        );
        let (msgs, truncated) = pack_want(1u32, interest, 64);
        assert_eq!(truncated, 0);
        assert!(msgs.len() > 1, "60 authors do not fit in 64 bytes");
        assert!(msgs.iter().all(|m| m.encode().len() <= 64));
        let scopes = scopes_of(&msgs, 1);
        assert_eq!(
            scopes.len(),
            msgs.len(),
            "one slice per frame, all channel 1"
        );
        assert_eq!(scopes[0].0, 0, "the first slice starts at the bottom");
        assert_eq!(
            scopes[scopes.len() - 1].1,
            u32::MAX,
            "the last reaches the top"
        );
        for w in scopes.windows(2) {
            assert_eq!(w[1].0, w[0].1 + 1, "adjacent scopes touch");
        }
        let mut all: Vec<u8> = Vec::new();
        for (lo, hi, authors) in &scopes {
            for a in authors {
                let p = Pair::author_prefix(a);
                assert!(*lo <= p && p <= *hi, "author {a} inside its slice's scope");
            }
            all.extend(authors);
        }
        assert_eq!(all, (0..60u8).collect::<Vec<_>>());
    }

    /// An author whose ranges alone exceed the budget is claimed as a
    /// closed prefix of what is held (never an open tail, which would claim
    /// more), and counted.
    #[test]
    fn want_truncates_an_author_that_cannot_fit() {
        let boundaries: Vec<Seq> = (0..400u32).map(|i| i * 2).collect(); // 200 closed ranges
        let full = Ranges::from_boundaries(boundaries.clone()).unwrap();
        let interest: Interest<Pair> =
            Interest::single(1u8, Entry::whole(BTreeMap::from([(5u8, full)])));
        let (msgs, truncated) = pack_want(1u32, interest, 64);
        assert_eq!(truncated, 1);
        assert_eq!(msgs.len(), 1);
        assert!(msgs[0].encode().len() <= 64);
        let (lo, hi, _) = scopes_of(&msgs, 1)[0];
        assert_eq!(
            (lo, hi),
            (0, u32::MAX),
            "still speaks for the whole channel"
        );
        let WireBody::Want(i) = &msgs[0].body else {
            panic!()
        };
        let claimed = i.get(&1u8).unwrap().have_of(&5u8);
        let b = claimed.boundaries();
        assert!(!b.is_empty() && b.len() % 2 == 0, "a closed prefix");
        assert_eq!(&boundaries[..b.len()], b, "of the original boundaries");
    }

    #[test]
    fn empty_input_packs_to_nothing() {
        let (msgs, _) = pack_have(1u32, Vec::<(u8, Seq, Op)>::new(), 1000);
        assert!(msgs.is_empty());
        let (msgs, _) = pack_want(1u32, Interest::<u8>::empty(), 1000);
        assert!(msgs.is_empty());
    }
}
