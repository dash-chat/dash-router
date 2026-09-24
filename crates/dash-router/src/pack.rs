//! Size-aware packing of wire messages (spec 2026-09-22 §3.3): gossip
//! refuses messages over its `max_message_size`, so every broadcast is
//! split to fit a byte budget the embedder chooses.

use std::collections::BTreeSet;

use dash_router_core::{LogRanges, Op, Seq, WireLog, WireMessage, group_ops};
use polestar::prelude::Id;
use serde::{Serialize, de::DeserializeOwned};

/// p2panda-net's default `max_message_size` (4096) minus room for Dash
/// Chat's signed CBOR envelope (~150 bytes) and slack.
pub const DEFAULT_MAX_WIRE_BYTES: usize = 3800;

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

/// Greedily pack a Want: channels first (they are tiny and are what an
/// unknown-author subscription rides on), then one log's ranges at a
/// time. A single log whose ranges alone exceed the budget is dropped
/// and counted; the next Want cycle retries with whatever changed.
/// Every piece is signed by `sender` and carries `origin` (the router's
/// `Effect::SendWant` origin: `sender` for an own Want, the wanter for a
/// relay), so the receiver files all pieces under one wanter.
pub fn pack_want<N, L>(
    sender: N,
    origin: N,
    ranges: LogRanges<L>,
    channels: BTreeSet<L::Channel>,
    budget: usize,
) -> (Vec<WireMessage<N, L>>, u64)
where
    N: Id + Serialize + DeserializeOwned,
    L: WireLog,
{
    let mut msgs = Vec::new();
    let mut dropped = 0u64;
    let mut cur_ranges: LogRanges<L> = LogRanges::empty();
    let mut cur_channels: BTreeSet<L::Channel> = BTreeSet::new();
    let len = |r: &LogRanges<L>, p: &BTreeSet<L::Channel>| {
        WireMessage::want(sender, origin, r.clone(), p.clone())
            .encode()
            .len()
    };
    let flush =
        |msgs: &mut Vec<WireMessage<N, L>>, r: &mut LogRanges<L>, p: &mut BTreeSet<L::Channel>| {
            if !r.is_empty() || !p.is_empty() {
                msgs.push(WireMessage::want(
                    sender,
                    origin,
                    std::mem::replace(r, LogRanges::empty()),
                    std::mem::take(p),
                ));
            }
        };
    for channel in channels {
        cur_channels.insert(channel);
        if len(&cur_ranges, &cur_channels) > budget {
            cur_channels.remove(&channel);
            flush(&mut msgs, &mut cur_ranges, &mut cur_channels);
            cur_channels.insert(channel);
            if len(&cur_ranges, &cur_channels) > budget {
                cur_channels.remove(&channel);
                dropped += 1;
            }
        }
    }
    for (log, r) in ranges.iter() {
        cur_ranges.insert(*log, r.clone());
        if len(&cur_ranges, &cur_channels) > budget {
            cur_ranges.remove(log);
            flush(&mut msgs, &mut cur_ranges, &mut cur_channels);
            cur_ranges.insert(*log, r.clone());
            if len(&cur_ranges, &cur_channels) > budget {
                cur_ranges.remove(log);
                dropped += 1;
            }
        }
    }
    flush(&mut msgs, &mut cur_ranges, &mut cur_channels);
    (msgs, dropped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dash_router_core::{Op, Ranges, WireBody};

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

    #[test]
    fn want_splits_ranges_and_carries_channels_first() {
        let ranges = LogRanges::from_pairs((0..200u8).map(|l| (l, Ranges::from(3))));
        let channels: BTreeSet<u8> = (0..50).collect();
        let (msgs, dropped) = pack_want(1u32, 9u32, ranges, channels.clone(), 300);
        assert_eq!(dropped, 0);
        assert!(msgs.len() > 1);
        assert!(msgs.iter().all(|m| m.encode().len() <= 300));
        let mut got_channels = BTreeSet::new();
        let mut got_logs = 0;
        for m in &msgs {
            if let WireBody::Want {
                origin,
                ranges,
                channels,
            } = &m.body
            {
                assert_eq!(*origin, 9, "every piece carries the origin");
                got_channels.extend(channels.iter().copied());
                got_logs += ranges.iter().count();
            }
        }
        assert_eq!(got_channels, channels);
        assert_eq!(got_logs, 200);
    }

    #[test]
    fn empty_input_packs_to_nothing() {
        let (msgs, _) = pack_have(1u32, Vec::<(u8, Seq, Op)>::new(), 1000);
        assert!(msgs.is_empty());
        let (msgs, _) = pack_want(1u32, 1u32, LogRanges::<u8>::empty(), BTreeSet::new(), 1000);
        assert!(msgs.is_empty());
    }
}
