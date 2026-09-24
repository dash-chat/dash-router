# Channel-nested LogRanges Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Nest `LogRanges` by channel then author so a shared LogId is stored and sent once per author group, and rename the `Log` trait's halves from `Prefix` to `Channel` + `Author`.

**Architecture:** The `Log` trait gains `Author` and a lossless `new(channel, author)` constructor; single-author ids (integers, `UpTo`) use polestar's `IdUnit` as their author. `LogRanges<L>` becomes `BTreeMap<L::Channel, BTreeMap<L::Author, Ranges>>` behind an API still keyed by `L`, plus two channel-level accessors the router and node use instead of filtering every log. The wire encoding changes, so the wire version bumps to 2.

**Tech Stack:** Rust workspace; `polestar` (git, patched to `/home/michael/proj/polestar-rs`); `serde` + `postcard` for the wire; `proptest` for oracle tests.

**Spec:** `docs/superpowers/specs/2026-09-24-channel-nested-log-ranges-design.md`

## Global Constraints

- The word "prefix" leaves the codebase entirely (types, methods, fields, doc comments, test names). The 2026-09-22 spec file is history and is **not** edited.
- `LogRanges` public API stays keyed by `L`; the only signature change is `iter()` yielding `(L, &Ranges)` by value.
- Known-but-empty markers are per log: `from_pairs`, `insert`, `union` keep empty `Ranges`; `intersection`, `difference` drop them. No outer channel entry may ever have an empty inner map.
- `Log::new(l.channel(), l.author()) == l` for every `l`.
- `WIRE_VERSION = 2`, `GOSSIP_TOPIC = "dash-router/v2"`; the Want field is `channels`.
- Every commit compiles and passes `cargo test --workspace`. Run `cargo fmt` before each commit.
- Commit messages end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- The model-world author is `polestar::id::IdUnit` (serde derive is already on polestar-rs main `72fb422`, which the workspace builds against via `[patch]`). No local unit type.

## Review Focus

1. **Removing the last author of a channel** must delete the channel entry, so `channels()` and a later `channel(c)` don't report a ghost channel. Pinned in Task 3 (`remove_prunes_an_emptied_channel`).
2. **A marker (empty `Ranges`) under a channel with real siblings** must survive `union` and be dropped by `intersection`/`difference` without disturbing its siblings. Pinned in Task 3 (proptest oracle checks key survival, and `marker_under_a_populated_channel`).
3. **`channel(c)` for an unknown channel** must yield nothing, not panic. Pinned in Task 3 (`channel_of_unknown_channel_is_empty`).
4. **A v1 peer's Want** (old encoding, `version = 1`) must be rejected by `decode`, not misparsed into garbage ranges. Pinned in Task 5 (`v1_messages_are_rejected`).
5. **Packing a Want whose authors share one channel** must still respect the byte budget when the channel is split across pieces: each piece re-encodes its own channel key. Pinned in Task 5 (`want_pieces_each_carry_their_channel`).

---

### Task 1: Rename `Prefix` → `Channel` across the workspace

Pure rename, no behavior change. The compiler drives it: after editing `log.rs`, every remaining `Prefix`/`prefix` is a compile error or a grep hit.

**Files:**
- Modify: `crates/dash-router-core/src/log.rs`
- Modify: `crates/dash-router-core/src/wire.rs`
- Modify: `crates/dash-router-core/src/router.rs`
- Modify: `crates/dash-router-core/src/node.rs`
- Modify: `crates/dash-router-core/tests/router.rs`, `crates/dash-router-core/tests/node.rs`
- Modify: `crates/dash-router/src/pack.rs`, `shell.rs`, `handle.rs`, `transport.rs`, `disk.rs`
- Modify: `crates/dash-router/tests/conformance.rs`, `loop.rs`
- Modify: `crates/dash-router-net-model/tests/net.rs`

**Interfaces:**
- Produces: `trait Log { type Channel: Id; fn channel(&self) -> Self::Channel; }`; `Pair { channel: u8, author: u8 }`; `WireBody::Want { origin, ranges, channels: BTreeSet<L::Channel> }`; `RouterAction::Open(BTreeSet<L::Channel>)`, `RouterAction::RecvWant { .., channels }`, `Effect::SendWant { .., channels }`; `RouterState::others_channels()`, `relayed_want_channels()`; `NodeAction::Subscribe(L::Channel)`, `Unsubscribe(L::Channel)`; `pack_want(sender, origin, ranges, channels, budget)`; `RouterHandle::subscribe(channel)`, `unsubscribe(channel)`.

- [ ] **Step 1: Rewrite the trait, macro and `Pair` in `log.rs`**

Replace the top of `crates/dash-router-core/src/log.rs` (module doc through the `Pair` impls) with:

```rust
//! A log identity with a *channel*: the half shared across authors, which
//! is what a subscription names. Subscribing to a channel means every
//! author's log under it, now and in the future. For the integer ids used
//! by tests, models and the sim the channel is the id itself, so a
//! subscription to `7` is exactly a subscription to log `7`.
//!
//! Vocabulary follows p2panda: a log is single-author, and the thing one
//! log id names across authors is a channel. At the Dash Chat boundary
//! `Channel` is p2panda's `LogId`.

use polestar::prelude::Id;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub trait Log: Id {
    type Channel: Id;
    fn channel(&self) -> Self::Channel;
}

macro_rules! self_channeled {
    ($($t:ty),* $(,)?) => {
        $(impl Log for $t {
            type Channel = Self;
            fn channel(&self) -> Self { *self }
        })*
    };
}
self_channeled!(u8, u16, u32, u64, usize);

/// The bounded ids polestar models and the core's own model tests use for
/// `L`: self-channeled like the integers, so model-checked scenarios keep
/// their single-author meaning.
impl<const N: usize, const WRAP: bool> Log for polestar::id::UpTo<N, WRAP>
where
    Self: Id,
{
    type Channel = Self;
    fn channel(&self) -> Self {
        *self
    }
}

/// `Log` plus the serde bounds the wire needs on both halves. Blanket:
/// nothing implements this by hand.
pub trait WireLog:
    Log<Channel: Serialize + DeserializeOwned> + Serialize + DeserializeOwned
{
}
impl<T> WireLog for T where
    T: Log<Channel: Serialize + DeserializeOwned> + Serialize + DeserializeOwned
{
}

/// A two-level log id for tests and examples: `(channel, author)`, the
/// shape Dash Chat's `(LogId, author)` has. Orders channel-first so a
/// channel's logs are contiguous, exactly as the relay store keys them.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Pair {
    pub channel: u8,
    pub author: u8,
}

impl Pair {
    pub const fn new(channel: u8, author: u8) -> Self {
        Self { channel, author }
    }
}

impl std::fmt::Display for Pair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.channel, self.author)
    }
}

impl Log for Pair {
    type Channel = u8;
    fn channel(&self) -> u8 {
        self.channel
    }
}
```

And the tests module in the same file:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_ids_are_their_own_channel() {
        assert_eq!(7u8.channel(), 7u8);
        assert_eq!(9u32.channel(), 9u32);
    }

    #[test]
    fn pair_channel_is_the_first_half() {
        let l = Pair {
            channel: 3,
            author: 9,
        };
        assert_eq!(l.channel(), 3);
        assert_eq!(l.to_string(), "3/9");
    }

    #[test]
    fn pair_orders_by_channel_then_author() {
        let a = Pair {
            channel: 1,
            author: 9,
        };
        let b = Pair {
            channel: 2,
            author: 0,
        };
        assert!(a < b);
    }

    #[test]
    fn wire_log_is_satisfied_by_ints_and_pair() {
        fn wire<L: WireLog>() {}
        wire::<u8>();
        wire::<u32>();
        wire::<Pair>();
    }
}
```

- [ ] **Step 2: Mechanically rename the rest of the workspace**

Run, from the workspace root:

```bash
files=$(grep -rlE 'Prefix|prefix' --include='*.rs' crates)
sed -i -E \
  -e 's/L::Prefix/L::Channel/g' \
  -e 's/Log<Prefix = u8>/Log<Channel = u8>/g' \
  -e 's/\.prefix\(\)/.channel()/g' \
  -e 's/\bprefixes\b/channels/g' \
  -e 's/\bprefix\b/channel/g' \
  -e 's/\bPrefix\b/Channel/g' \
  -e 's/others_prefixes/others_channels/g' \
  -e 's/relayed_want_prefixes/relayed_want_channels/g' \
  -e 's/RecvPrefixWant/RecvChannelWant/g' \
  -e 's/prefix_/channel_/g' \
  -e 's/_prefix\b/_channel/g' \
  -e 's/_prefixes\b/_channels/g' \
  $files
grep -rniE 'prefix' --include='*.rs' crates
```

The final grep must print nothing. If it prints doc-comment lines the regexes missed (for example "prefix scan" in `disk.rs`, which describes a redb key range scan, not a channel), reword them by hand: in `disk.rs` say "key-range scan" for the redb sense. `disk.rs:55-56` ("prefix first so a prefix's logs are one contiguous key range") becomes "channel first so a channel's logs are one contiguous key range".

- [ ] **Step 3: Fix what sed cannot**

`Pair` field accesses in `crates/dash-router/tests/conformance.rs:93` now read `self.channel * 16 + self.author` (sed did this; confirm). `WireBody::Want { origin, prefixes, .. }` patterns are now `channels`. `tracing::debug!(%prefix, ...)` in `shell.rs:484,506` is now `%channel`. Doc comments in `router.rs` that say "a prefix wanter" now say "a channel wanter"; read each renamed comment once for grammar.

- [ ] **Step 4: Build and test**

Run: `cargo fmt && cargo check --workspace --all-targets && cargo test --workspace`
Expected: all green, same test count as before.

- [ ] **Step 5: Commit**

```bash
git add -A crates
git commit -m "Rename Log::Prefix to Channel across the workspace

p2panda's word for what one log id names across authors. No behavior
change.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: `Log::Author` and `Log::new`

Additive. Nothing consumes `Author` yet.

**Files:**
- Modify: `crates/dash-router-core/src/log.rs`

**Interfaces:**
- Consumes: Task 1's `Log` trait.
- Produces: `trait Log: Id { type Channel: Id; type Author: Id; fn channel(&self) -> Self::Channel; fn author(&self) -> Self::Author; fn new(channel: Self::Channel, author: Self::Author) -> Self; }`. Integers and `UpTo` have `Author = polestar::id::IdUnit`. `Pair` has `Author = u8`. `WireLog` also bounds `Author: Serialize + DeserializeOwned`.

- [ ] **Step 1: Write the failing tests**

Append to the `tests` module in `log.rs`:

```rust
    #[test]
    fn new_is_the_inverse_of_channel_and_author() {
        let p = Pair::new(3, 9);
        assert_eq!(Pair::new(p.channel(), p.author()), p);
        assert_eq!(<u8 as Log>::new(7u8.channel(), 7u8.author()), 7u8);
        assert_eq!(<u32 as Log>::new(9u32.channel(), 9u32.author()), 9u32);
    }

    #[test]
    fn single_author_ids_have_the_unit_author() {
        assert_eq!(5u8.author(), polestar::id::IdUnit);
        assert_eq!(
            polestar::id::UpTo::<4>::new(2).author(),
            polestar::id::IdUnit
        );
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p dash-router-core --lib log::tests`
Expected: compile error, "no associated item named `new`" / "no method named `author`".

- [ ] **Step 3: Extend the trait and impls**

In `log.rs`, replace the trait, the macro, the `UpTo` impl, `WireLog` and the `Pair` `Log` impl with:

```rust
use polestar::{id::IdUnit, prelude::Id};

pub trait Log: Id {
    /// The half shared across authors; what a subscription names.
    type Channel: Id;
    /// The per-author half.
    type Author: Id;
    fn channel(&self) -> Self::Channel;
    fn author(&self) -> Self::Author;
    /// Inverse of `(channel(), author())`: for every `l`,
    /// `Self::new(l.channel(), l.author()) == l`. A nested `LogRanges`
    /// rebuilds ids through this when iterated.
    fn new(channel: Self::Channel, author: Self::Author) -> Self;
}

macro_rules! self_channeled {
    ($($t:ty),* $(,)?) => {
        $(impl Log for $t {
            type Channel = Self;
            type Author = IdUnit;
            fn channel(&self) -> Self { *self }
            fn author(&self) -> IdUnit { IdUnit }
            fn new(channel: Self, _: IdUnit) -> Self { channel }
        })*
    };
}
self_channeled!(u8, u16, u32, u64, usize);

impl<const N: usize, const WRAP: bool> Log for polestar::id::UpTo<N, WRAP>
where
    Self: Id,
{
    type Channel = Self;
    type Author = IdUnit;
    fn channel(&self) -> Self {
        *self
    }
    fn author(&self) -> IdUnit {
        IdUnit
    }
    fn new(channel: Self, _: IdUnit) -> Self {
        channel
    }
}

pub trait WireLog:
    Log<Channel: Serialize + DeserializeOwned, Author: Serialize + DeserializeOwned>
    + Serialize
    + DeserializeOwned
{
}
impl<T> WireLog for T where
    T: Log<Channel: Serialize + DeserializeOwned, Author: Serialize + DeserializeOwned>
        + Serialize
        + DeserializeOwned
{
}

impl Log for Pair {
    type Channel = u8;
    type Author = u8;
    fn channel(&self) -> u8 {
        self.channel
    }
    fn author(&self) -> u8 {
        self.author
    }
    fn new(channel: u8, author: u8) -> Self {
        Self { channel, author }
    }
}
```

Keep the existing `Pair::new` const fn; the trait's `new` delegates to the same field layout. Keep `impl Pair { pub const fn new }` as is so tests calling `Pair::new(1, 2)` resolve to the inherent method without ambiguity.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p dash-router-core --lib log::tests`
Expected: PASS, including the two new tests.

Then: `cargo test --workspace`
Expected: green.

- [ ] **Step 5: Commit**

```bash
git add crates/dash-router-core/src/log.rs
git commit -m "Log: add Author and a lossless new(channel, author)

Single-author ids use polestar's IdUnit.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: Nest `LogRanges` by channel

The core change. `iter()` changes signature, so this task also touches every caller that dereferenced the yielded key.

**Files:**
- Modify: `crates/dash-router-core/src/ranges.rs:198-307` and its tests
- Modify (iter ripple): `crates/dash-router-core/src/router.rs:155,160-168,214-223`; `crates/dash-router-core/src/node.rs:87-96,188-198,377-386`; `crates/dash-router-core/src/storage.rs:24-45,67-69,91-101,142-186,196`; `crates/dash-router/src/pack.rs:119-129`; `crates/dash-router/src/disk.rs:128-135,354,510,587`; `crates/dash-router/src/shell.rs:184,338,433-437,604-610,627`; `crates/dash-router/tests/panda_swarm.rs:223`

**Interfaces:**
- Consumes: `Log::{Channel, Author, channel, author, new}` from Task 2.
- Produces:
  ```rust
  pub struct LogRanges<L: Log>(BTreeMap<L::Channel, BTreeMap<L::Author, Ranges>>);
  impl<L: Log> LogRanges<L> {
      pub fn empty() -> Self;
      pub fn from_pairs(iter: impl IntoIterator<Item = (L, Ranges)>) -> Self;
      pub fn get(&self, log: &L) -> Option<&Ranges>;
      pub fn iter(&self) -> impl Iterator<Item = (L, &Ranges)>;       // L by value
      pub fn len(&self) -> usize;                                      // number of logs
      pub fn channel(&self, c: &L::Channel) -> impl Iterator<Item = (L, &Ranges)>;
      pub fn channels(&self) -> impl Iterator<Item = &L::Channel>;
      pub fn is_empty(&self) -> bool;
      pub fn contains(&self, log: &L, seq: Seq) -> bool;
      pub fn insert(&mut self, log: L, ranges: Ranges);
      pub fn remove(&mut self, log: &L) -> Option<Ranges>;
      pub fn union(&self, other: &Self) -> Self;
      pub fn intersection(&self, other: &Self) -> Self;
      pub fn difference(&self, other: &Self) -> Self;
  }
  ```
  `Storage<L: Log>`, `EvictableStorage<L: Log>`, `OpsMap<L: Log>`, `StoreEffect<L: Log>`, `ranges_of<L: Log>`.

- [ ] **Step 1: Write the failing tests**

Replace the `LogRanges` tests in `ranges.rs` (`empty_valued_keys_are_preserved_by_construction_and_union` stays as is; it uses `u8` and the public API) and add, inside `mod tests`:

```rust
    use crate::log::{Log, Pair};

    fn lr(pairs: &[(Pair, &[Seq])]) -> LogRanges<Pair> {
        LogRanges::from_pairs(pairs.iter().map(|(l, b)| (*l, r(b))))
    }

    #[test]
    fn channel_groups_every_author_under_it() {
        let m = lr(&[
            (Pair::new(1, 1), &[0, 3]),
            (Pair::new(1, 2), &[5]),
            (Pair::new(2, 1), &[0, 1]),
        ]);
        let under_1: Vec<(Pair, Ranges)> = m.channel(&1).map(|(l, r)| (l, r.clone())).collect();
        assert_eq!(
            under_1,
            vec![(Pair::new(1, 1), r(&[0, 3])), (Pair::new(1, 2), r(&[5]))]
        );
        assert_eq!(m.channels().copied().collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(m.len(), 3);
    }

    #[test]
    fn channel_of_unknown_channel_is_empty() {
        let m = lr(&[(Pair::new(1, 1), &[0, 3])]);
        assert_eq!(m.channel(&9).count(), 0);
    }

    #[test]
    fn channel_matches_filtered_iter() {
        let m = lr(&[
            (Pair::new(1, 1), &[0, 3]),
            (Pair::new(1, 2), &[]),
            (Pair::new(3, 0), &[7]),
        ]);
        for c in [0u8, 1, 2, 3] {
            let via_channel: Vec<Pair> = m.channel(&c).map(|(l, _)| l).collect();
            let via_iter: Vec<Pair> = m.iter().filter(|(l, _)| l.channel() == c).map(|(l, _)| l).collect();
            assert_eq!(via_channel, via_iter, "channel {c}");
        }
    }

    #[test]
    fn remove_prunes_an_emptied_channel() {
        let mut m = lr(&[(Pair::new(1, 1), &[0, 3]), (Pair::new(1, 2), &[5])]);
        assert_eq!(m.remove(&Pair::new(1, 1)), Some(r(&[0, 3])));
        assert_eq!(m.channels().count(), 1, "channel 1 still has author 2");
        assert_eq!(m.remove(&Pair::new(1, 2)), Some(r(&[5])));
        assert_eq!(m.channels().count(), 0, "channel 1 is gone with its last author");
        assert_eq!(m.channel(&1).count(), 0);
        assert_eq!(m.remove(&Pair::new(1, 2)), None);
        assert_eq!(m, LogRanges::empty());
    }

    #[test]
    fn marker_under_a_populated_channel() {
        // author 2 is a known-but-empty marker next to a real sibling.
        let a = lr(&[(Pair::new(1, 1), &[0, 3]), (Pair::new(1, 2), &[])]);
        let b = lr(&[(Pair::new(1, 1), &[3, 5])]);
        let u = a.union(&b);
        assert_eq!(u.get(&Pair::new(1, 2)), Some(&Ranges::empty()), "marker survives union");
        assert_eq!(u.get(&Pair::new(1, 1)), Some(&r(&[0, 5])));
        assert!(a.intersection(&b).get(&Pair::new(1, 2)).is_none());
        assert!(a.difference(&b).get(&Pair::new(1, 2)).is_none());
        assert_eq!(a.difference(&b).get(&Pair::new(1, 1)), Some(&r(&[0, 3])));
    }

    #[test]
    fn iter_rebuilds_full_ids_in_channel_then_author_order() {
        let m = lr(&[
            (Pair::new(2, 0), &[1]),
            (Pair::new(1, 9), &[1]),
            (Pair::new(1, 3), &[1]),
        ]);
        let ids: Vec<Pair> = m.iter().map(|(l, _)| l).collect();
        assert_eq!(ids, vec![Pair::new(1, 3), Pair::new(1, 9), Pair::new(2, 0)]);
    }

    #[test]
    fn nested_serde_roundtrips_and_is_shorter_than_flat() {
        let m = lr(&[
            (Pair::new(1, 1), &[0, 3]),
            (Pair::new(1, 2), &[5]),
            (Pair::new(1, 3), &[0, 1]),
        ]);
        let bytes = postcard::to_stdvec(&m).unwrap();
        let back: LogRanges<Pair> = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back, m);
        let flat: BTreeMap<Pair, Ranges> = m.iter().map(|(l, r)| (l, r.clone())).collect();
        let flat_bytes = postcard::to_stdvec(&flat).unwrap();
        assert!(bytes.len() < flat_bytes.len(), "{} vs {}", bytes.len(), flat_bytes.len());
        let u: LogRanges<u8> = LogRanges::from_pairs([(4u8, r(&[0, 2]))]);
        let ub = postcard::to_stdvec(&u).unwrap();
        assert_eq!(postcard::from_bytes::<LogRanges<u8>>(&ub).unwrap(), u);
    }

    // --- Nested set ops against a flat oracle -----------------------------

    fn arb_pair() -> impl Strategy<Value = Pair> {
        (0u8..3, 0u8..3).prop_map(|(c, a)| Pair::new(c, a))
    }

    fn arb_log_ranges() -> impl Strategy<Value = LogRanges<Pair>> {
        proptest::collection::btree_map(arb_pair(), arb_ranges(), 0..6)
            .prop_map(LogRanges::from_pairs)
    }

    fn flat(m: &LogRanges<Pair>) -> BTreeMap<Pair, Ranges> {
        m.iter().map(|(l, r)| (l, r.clone())).collect()
    }

    /// The per-log rules from the type doc, applied to flat maps.
    fn flat_union(a: &BTreeMap<Pair, Ranges>, b: &BTreeMap<Pair, Ranges>) -> BTreeMap<Pair, Ranges> {
        let mut out = a.clone();
        for (l, r) in b {
            let merged = out.get(l).map(|m| m.union(r)).unwrap_or_else(|| r.clone());
            out.insert(*l, merged);
        }
        out
    }

    fn flat_intersection(a: &BTreeMap<Pair, Ranges>, b: &BTreeMap<Pair, Ranges>) -> BTreeMap<Pair, Ranges> {
        a.iter()
            .filter_map(|(l, r)| b.get(l).map(|o| (*l, r.intersection(o))))
            .filter(|(_, r)| !r.is_empty())
            .collect()
    }

    fn flat_difference(a: &BTreeMap<Pair, Ranges>, b: &BTreeMap<Pair, Ranges>) -> BTreeMap<Pair, Ranges> {
        a.iter()
            .map(|(l, r)| (*l, b.get(l).map(|o| r.difference(o)).unwrap_or_else(|| r.clone())))
            .filter(|(_, r)| !r.is_empty())
            .collect()
    }

    proptest! {
        #[test]
        fn nested_union_matches_flat_oracle(a in arb_log_ranges(), b in arb_log_ranges()) {
            prop_assert_eq!(flat(&a.union(&b)), flat_union(&flat(&a), &flat(&b)));
        }

        #[test]
        fn nested_intersection_matches_flat_oracle(a in arb_log_ranges(), b in arb_log_ranges()) {
            prop_assert_eq!(flat(&a.intersection(&b)), flat_intersection(&flat(&a), &flat(&b)));
        }

        #[test]
        fn nested_difference_matches_flat_oracle(a in arb_log_ranges(), b in arb_log_ranges()) {
            prop_assert_eq!(flat(&a.difference(&b)), flat_difference(&flat(&a), &flat(&b)));
        }

        #[test]
        fn no_channel_is_ever_left_empty(a in arb_log_ranges(), b in arb_log_ranges(), victim in arb_pair()) {
            let mut m = a.union(&b);
            m.remove(&victim);
            for out in [m.clone(), a.intersection(&b), a.difference(&b)] {
                for c in out.channels() {
                    prop_assert!(out.channel(c).count() > 0, "channel {c} has no authors");
                }
            }
        }

        #[test]
        fn from_pairs_then_iter_roundtrips(a in arb_log_ranges()) {
            let again = LogRanges::from_pairs(a.iter().map(|(l, r)| (l, r.clone())));
            prop_assert_eq!(again, a);
        }
    }
```

Add `use std::collections::BTreeMap;` inside `mod tests` if not already in scope (it is imported at the file top; `use super::*` brings it in).

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p dash-router-core --lib ranges::tests`
Expected: compile errors: no method `channel`, `channels`, `len`; `L: Log` bound missing (`Pair` has no `Ord`-only path issue but `lr`'s `from_pairs` works; the `channel` calls fail).

- [ ] **Step 3: Rewrite `LogRanges`**

Replace everything from the `LogRanges` type doc (`/// Ranges across several logs.`) through the end of `impl<L: Ord + Clone> LogRanges<L>` in `ranges.rs` with:

```rust
/// Ranges across several logs, nested by channel then author. A log absent
/// from the map is *unknown*; a log present with an empty [`Ranges`] value
/// is *known but empty* — a marker meaning "this log exists and nothing is
/// held/wanted/relayed for it," distinct from not knowing about the log at
/// all (e.g. a freshly-created log with no data yet still wants
/// everything). Key presence therefore carries meaning independent of the
/// value's emptiness: construction ([`Self::from_pairs`], [`Self::insert`])
/// and [`Self::union`] preserve marker keys, while the derived, wire-bound
/// results of [`Self::intersection`] and [`Self::difference`] drop empty
/// entries, since those represent "nothing to say about this log" rather
/// than a knowledge marker.
///
/// The nesting stores and sends each channel once for all its authors.
/// Invariant: no channel entry has an empty author map ([`Self::remove`]
/// prunes). p2panda nests the other way round (author outer) because sync
/// walks per peer; the router nests channel outer because subscriptions
/// and the relay store are keyed by channel.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound(
    serialize = "L::Channel: Serialize, L::Author: Serialize",
    deserialize = "L::Channel: Deserialize<'de>, L::Author: Deserialize<'de>"
))]
pub struct LogRanges<L: Log>(BTreeMap<L::Channel, BTreeMap<L::Author, Ranges>>);

impl<L: Log> Default for LogRanges<L> {
    fn default() -> Self {
        Self(BTreeMap::new())
    }
}

impl<L: Log> LogRanges<L> {
    pub fn empty() -> Self {
        Self::default()
    }

    /// Build from pairs. A pair with an empty `Ranges` is kept as a
    /// known-but-empty marker, not dropped.
    pub fn from_pairs(iter: impl IntoIterator<Item = (L, Ranges)>) -> Self {
        let mut out = Self::empty();
        for (log, r) in iter {
            out.insert(log, r);
        }
        out
    }

    pub fn get(&self, log: &L) -> Option<&Ranges> {
        self.0.get(&log.channel())?.get(&log.author())
    }

    /// Every log, channel-major then author order, with its id rebuilt
    /// through [`Log::new`]. (Edition 2024: the returned iterator captures
    /// `&self` without an explicit `+ '_`.)
    pub fn iter(&self) -> impl Iterator<Item = (L, &Ranges)> {
        self.0.iter().flat_map(|(c, authors)| {
            let c = *c;
            authors.iter().map(move |(a, r)| (L::new(c, *a), r))
        })
    }

    /// Number of logs (markers included).
    pub fn len(&self) -> usize {
        self.0.values().map(BTreeMap::len).sum()
    }

    /// Every log under one channel; nothing if the channel is unknown.
    pub fn channel(&self, c: &L::Channel) -> impl Iterator<Item = (L, &Ranges)> {
        let c = *c;
        self.0
            .get(&c)
            .into_iter()
            .flat_map(move |authors| authors.iter().map(move |(a, r)| (L::new(c, *a), r)))
    }

    /// Every channel with at least one log.
    pub fn channels(&self) -> impl Iterator<Item = &L::Channel> {
        self.0.keys()
    }

    /// True when every known log maps to an empty range (vacuously true
    /// when no logs are known at all). A `LogRanges` holding only
    /// known-but-empty markers is still "empty" in this sense: there is
    /// nothing to fetch, send or accept, even though logs are known.
    pub fn is_empty(&self) -> bool {
        self.0.values().flat_map(BTreeMap::values).all(Ranges::is_empty)
    }

    pub fn contains(&self, log: &L, seq: Seq) -> bool {
        self.get(log).is_some_and(|r| r.contains(seq))
    }

    /// Set `log`'s ranges, including an empty one: presence of the key is
    /// itself meaningful (see the type doc), so an empty `ranges` is kept
    /// as a known-but-empty marker rather than removing the key.
    pub fn insert(&mut self, log: L, ranges: Ranges) {
        self.0
            .entry(log.channel())
            .or_default()
            .insert(log.author(), ranges);
    }

    /// Forget a log entirely (its "known" marker included). A channel
    /// whose last log is removed is forgotten too.
    pub fn remove(&mut self, log: &L) -> Option<Ranges> {
        let c = log.channel();
        let authors = self.0.get_mut(&c)?;
        let removed = authors.remove(&log.author());
        if authors.is_empty() {
            self.0.remove(&c);
        }
        removed
    }

    /// Keeps every key present in either operand, even where the merged
    /// value is empty, so a known-but-empty marker in either side survives
    /// (e.g. `held.union(&novel)` never loses a log's "known" status).
    pub fn union(&self, other: &Self) -> Self {
        let mut out = self.clone();
        for (c, theirs) in &other.0 {
            let mine = out.0.entry(*c).or_default();
            for (a, r) in theirs {
                let merged = match mine.get(a) {
                    Some(m) => m.union(r),
                    None => r.clone(),
                };
                mine.insert(*a, merged);
            }
        }
        out
    }

    /// Only the logs present in both operands, with each empty result
    /// dropped: this produces wire-bound content (what to send/accept),
    /// where "nothing in common" must not be represented as a key.
    pub fn intersection(&self, other: &Self) -> Self {
        let mut out = Self::empty();
        for (c, mine) in &self.0 {
            let Some(theirs) = other.0.get(c) else {
                continue;
            };
            for (a, r) in mine {
                if let Some(o) = theirs.get(a) {
                    let x = r.intersection(o);
                    if !x.is_empty() {
                        out.0.entry(*c).or_default().insert(*a, x);
                    }
                }
            }
        }
        out
    }

    /// Everything in `self` not in `other`, with each empty result dropped
    /// (see [`Self::intersection`] on why: this is derived, wire-bound
    /// content, not a knowledge marker).
    pub fn difference(&self, other: &Self) -> Self {
        let mut out = Self::empty();
        for (c, mine) in &self.0 {
            let theirs = other.0.get(c);
            for (a, r) in mine {
                let d = match theirs.and_then(|t| t.get(a)) {
                    Some(o) => r.difference(o),
                    None => r.clone(),
                };
                if !d.is_empty() {
                    out.0.entry(*c).or_default().insert(*a, d);
                }
            }
        }
        out
    }
}
```

Add `use crate::log::Log;` at the top of `ranges.rs`.

- [ ] **Step 4: Ripple `L: Log` bounds and the by-value `iter()`**

Apply these edits. Every one is the same rule: `iter()` now yields `L`, not `&L`.

`crates/dash-router-core/src/storage.rs`:
- `pub trait Storage<L: Ord>` → `pub trait Storage<L: Log>`; `EvictableStorage<L: Ord>` → `<L: Log>`; `pub struct OpsMap<L: Ord>` → `<L: Log>`; `impl<L: Ord + Clone> Storage<L> for OpsMap<L>` and `impl<L: Ord + Clone> EvictableStorage<L>` → `impl<L: Log>`; `impl<L: Ord> OpsMap<L>` → `impl<L: Log>`; `pub enum StoreEffect<L: Ord>` → `<L: Log>`. Add `use crate::log::Log;`. Any other `L: Ord` bound in that file (the relay/ext store machines) becomes `L: Log` too; the compiler lists them.
- In `fetch`, `evict_payloads`, `evict`: `for (log, r) in ranges.iter()` bodies use `self.0.get(&log)` / `get_mut(&log)` / `self.0.remove(&log)`, and `out.push((log, seq, op.clone()))`.

`crates/dash-router-core/src/router.rs`:
- line 155: `(*log, r.complement())` → `(log, r.complement())`.
- lines 160-168 (`held_under`): `named.get(log)` → `named.get(&log)`; `(*log, r.clone())` → `(log, r.clone())`.
- lines 214-223 (`next_want`): `(*log, r.clone())` → `(log, r.clone())`.

`crates/dash-router-core/src/node.rs`:
- `relay_logs_under`: `.map(|(log, _)| (*log, Ranges::full()))` → `(log, Ranges::full())`.
- `ranges_of<L: Ord + Clone>` → `ranges_of<L: Log>`; body unchanged (`log.clone()` on a `Copy` type is fine; or write `*log`).
- lines 377-386: `if !s.is_subscribed(&log)`; `if *pl == log && r.contains(*seq)`; `NodeEffect::Deliver(log, *seq)`.

`crates/dash-router/src/pack.rs` lines 119-129: `cur_ranges.insert(log, r.clone())` (twice) and `cur_ranges.remove(&log)` (twice).

`crates/dash-router/src/disk.rs`:
- `fn set_log_entry<L: LogKey>` keeps its signature (`log: &L`); callers unchanged.
- lines 354, 510, 587: `log_bounds(&log)` and any `self.held.get(&log)` / `set_log_entry(.., &log, ..)`; the compiler points at each.
- `pub struct DiskRelayStore<L: LogKey>` holds a `LogRanges<L>`, which now needs `L: Log`. Change the struct bound and its three impl blocks (`impl<L: LogKey> DiskRelayStore<L>`, `impl<L: LogKey> Storage<L> for ..`, `impl<L: LogKey> EvictableStorage<L> for ..`) to `L: LogKey + Log`. `LogKey` itself stays `Ord + Clone`, so the `[u8; 32]` / `[u8; 64]` impls and their key-encoding tests at `disk.rs:920-940` are untouched. Any `L: LogKey` on a free function that takes a `LogRanges<L>` (`set_log_entry`) becomes `L: LogKey + Log` too; the compiler lists them.

`crates/dash-router/src/shell.rs`:
- line 184, 338, 627: `.iter().count()` → `.len()`.
- lines 433-437: `.map(|(log, _)| (log, Ranges::full()))`.
- lines 604-610 (mirror of node.rs 377-386): `is_subscribed(&log)`, `Deliver(log, ..)` or whatever the shell does with `*log`; drop the deref.

`crates/dash-router/tests/panda_swarm.rs:223`: `held.contains(&l, *s)`.

Then: `cargo check --workspace --all-targets 2>&1 | grep -E '^error' -A5` and fix any remaining site by the same rule.

- [ ] **Step 5: Run the tests**

Run: `cargo fmt && cargo test --workspace`
Expected: green, including the new `ranges::tests` and the proptests. The existing `u8`-keyed tests and every model test pass untouched.

- [ ] **Step 6: Commit**

```bash
git add -A crates
git commit -m "LogRanges: nest by channel then author

The public API stays keyed by L; iter() now yields L by value and two
channel-level accessors are added. Storage bounds move from Ord to Log.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Router and node use the channel accessors

Replace the three linear "filter every held log by channel" scans with `held.channel(c)` lookups. Behavior is identical; the existing router and node tests are the regression net.

**Files:**
- Modify: `crates/dash-router-core/src/router.rs:158-168,214-223`
- Modify: `crates/dash-router-core/src/node.rs:86-96`
- Modify: `crates/dash-router/src/shell.rs:431-437`

**Interfaces:**
- Consumes: `LogRanges::channel(&L::Channel)` from Task 3.
- Produces: no new interfaces.

- [ ] **Step 1: Write a test that pins the equivalence**

Add to `crates/dash-router-core/tests/router.rs` inside `mod channel` (renamed from `mod prefix` in Task 1), after `others_wants_includes_held_logs_under_wanted_channels`:

```rust
    /// `held_under` is a channel lookup, not a scan: a wanter naming
    /// channel 1 gets both of channel 1's logs and nothing from channel 2.
    #[test]
    fn channel_want_answers_every_author_under_the_channel_only() {
        let m = machine();
        let a1 = Pair::new(1, 1);
        let a9 = Pair::new(1, 9);
        let b5 = Pair::new(2, 5);
        let s = RouterState::new(
            0u32,
            held(&[
                (a1, Ranges::from(0)),
                (a9, Ranges::range(0, 4)),
                (b5, Ranges::from(0)),
            ]),
        );
        let (s, _) = m
            .transition(
                s,
                A::RecvWant {
                    from: 7,
                    origin: 7,
                    ranges: LogRanges::empty(),
                    channels: BTreeSet::from([1u8]),
                },
            )
            .unwrap();
        let (s, _) = m
            .transition(s, A::ArmHaveTimer(Duration::ZERO.into()))
            .unwrap();
        let (_, fx) = m.transition(s, A::FireHave).unwrap();
        let have = sent_have(&fx).expect("a Have is sent");
        assert_eq!(have.get(&a1), Some(&Ranges::from(0)));
        assert_eq!(have.get(&a9), Some(&Ranges::range(0, 4)));
        assert_eq!(have.get(&b5), None, "other channel: untouched");
    }
```

`machine()`, `held()`, `sent_have()`, `A` and `Duration` are the module's existing helpers and imports; nothing new is needed.

- [ ] **Step 2: Run it**

Run: `cargo test -p dash-router-core --test router channel`
Expected: PASS already (the scan is correct); this test guards the rewrite.

- [ ] **Step 3: Rewrite the three scans**

`router.rs` `held_under`:

```rust
    /// Held logs with data under any of `channels` that `named` does not
    /// mention: the wholesale half of a Want's answer.
    fn held_under(&self, channels: &BTreeSet<L::Channel>, named: &LogRanges<L>) -> LogRanges<L> {
        LogRanges::from_pairs(channels.iter().flat_map(|c| {
            self.held
                .channel(c)
                .filter(|(log, r)| !r.is_empty() && named.get(log).is_none())
                .map(|(log, r)| (log, r.clone()))
        }))
    }
```

`router.rs` `next_want`'s `named_under_open`:

```rust
        let named_under_open = LogRanges::from_pairs(
            self.open
                .iter()
                .flat_map(|c| wanted.channel(c).map(|(log, r)| (log, r.clone()))),
        );
```

`node.rs` `relay_logs_under`:

```rust
    /// Every relay-held log under `channel`, as full ranges: what Subscribe migrates.
    fn relay_logs_under(&self, channel: &L::Channel) -> LogRanges<L> {
        LogRanges::from_pairs(
            self.relay
                .0
                .held_all()
                .channel(channel)
                .map(|(log, _)| (log, Ranges::full())),
        )
    }
```

`shell.rs` `on_subscribe`:

```rust
        let under: LogRanges<L> = match self.relay.held_all().await {
            Ok(all) => LogRanges::from_pairs(
                all.channel(&channel).map(|(log, _)| (log, Ranges::full())),
            ),
```

- [ ] **Step 4: Run the tests**

Run: `cargo fmt && cargo test --workspace`
Expected: green.

- [ ] **Step 5: Commit**

```bash
git add -A crates
git commit -m "Router, node, shell: look up channels instead of scanning held

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: Wire version 2

The nested `LogRanges` encoding changed every Want in Task 3. Bump the version and topic, and pin the wire-level properties.

**Files:**
- Modify: `crates/dash-router-core/src/wire.rs:14-35,115-172`
- Modify: `crates/dash-router/src/pack.rs` tests

**Interfaces:**
- Consumes: `LogRanges<L>` serde from Task 3.
- Produces: `WIRE_VERSION = 2`, `GOSSIP_TOPIC = "dash-router/v2"`.

- [ ] **Step 1: Write the failing tests**

In `wire.rs` `mod tests`, add:

```rust
    #[test]
    fn version_is_2_and_topic_matches() {
        assert_eq!(WIRE_VERSION, 2);
        assert_eq!(GOSSIP_TOPIC, "dash-router/v2");
    }

    /// A v1 Want (flat LogRanges, `version = 1`) must be rejected, not
    /// misparsed: the version byte is checked before anything else.
    #[test]
    fn v1_messages_are_rejected() {
        let mut bytes =
            WireMessage::<u32, u8>::want(7, 7, LogRanges::empty(), BTreeSet::new()).encode();
        bytes[0] = 1;
        let err = WireMessage::<u32, u8>::decode(&bytes).unwrap_err();
        assert!(err.to_string().contains("unknown wire version 1"), "{err}");
    }

    /// The point of nesting: one channel, many authors, encodes the channel once.
    #[test]
    fn want_encodes_a_shared_channel_once() {
        use crate::log::Log;

        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        struct Wide {
            channel: [u8; 8],
            author: u8,
        }
        impl std::fmt::Display for Wide {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}/{}", self.channel, self.author)
            }
        }
        impl Log for Wide {
            type Channel = [u8; 8];
            type Author = u8;
            fn channel(&self) -> [u8; 8] {
                self.channel
            }
            fn author(&self) -> u8 {
                self.author
            }
            fn new(channel: [u8; 8], author: u8) -> Self {
                Self { channel, author }
            }
        }

        let channel = [0xAB; 8];
        let ranges = LogRanges::from_pairs((0..10u8).map(|a| (Wide { channel, author: a }, Ranges::from(a as u32))));
        let bytes = WireMessage::<u32, Wide>::want(1, 1, ranges, BTreeSet::new()).encode();
        let hits = bytes.windows(8).filter(|w| *w == &channel[..]).count();
        assert_eq!(hits, 1, "channel bytes appear once in {} bytes", bytes.len());
    }
```

`Wide` satisfies polestar's `Id` blanket (`Copy + Ord + Hash + Display + Debug + Send + Sync + 'static`) through the derives and the `Display` impl above.

In `pack.rs` `mod tests`, add:

```rust
    /// Review focus 5: pieces of a split Want each re-encode their own
    /// channel key, so a channel spanning pieces never blows the budget.
    #[test]
    fn want_pieces_each_carry_their_channel() {
        use dash_router_core::Pair;
        let ranges = LogRanges::from_pairs((0..60u8).map(|a| (Pair::new(1, a), Ranges::from(3))));
        let (msgs, dropped) = pack_want(1u32, 9u32, ranges, BTreeSet::new(), 64);
        assert_eq!(dropped, 0);
        assert!(msgs.len() > 1, "60 authors do not fit in 64 bytes");
        assert!(msgs.iter().all(|m| m.encode().len() <= 64));
        let total: usize = msgs
            .iter()
            .map(|m| match &m.body {
                WireBody::Want { ranges, .. } => ranges.len(),
                WireBody::Have(_) => 0,
            })
            .sum();
        assert_eq!(total, 60);
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p dash-router-core --lib wire::tests && cargo test -p dash-router --lib pack::tests`
Expected: `version_is_2_and_topic_matches` fails (`1 != 2`); `v1_messages_are_rejected` fails because `bytes[0] = 1` is the current version and decodes fine; the other two pass or fail on compile detail only.

- [ ] **Step 3: Bump the version**

In `wire.rs`:

```rust
/// Bump together with [`GOSSIP_TOPIC`] on breaking change.
/// v1: Want carries channels (spec 2026-09-22 §3.5) and its origin.
/// v2: `LogRanges` is nested by channel then author (spec 2026-09-24).
pub const WIRE_VERSION: u8 = 2;

pub const GOSSIP_TOPIC: &str = "dash-router/v2";

const _: () = {
    assert!(
        WIRE_VERSION == 2,
        "bump the gossip topic suffix with the wire version"
    );
```

Update the existing test `wire_messages_round_trip_and_reject_unknown_versions`: its `bad[0] = WIRE_VERSION + 1` still works. Update the serde bounds on `WireMessage` and `WireBody` to:

```rust
#[serde(bound(
    serialize = "N: Serialize, L: Serialize, L::Channel: Serialize, L::Author: Serialize",
    deserialize = "N: Deserialize<'de>, L: Deserialize<'de>, L::Channel: Deserialize<'de>, L::Author: Deserialize<'de>"
))]
```

(`L: Serialize` stays because `Have` still carries full `L` ids.)

- [ ] **Step 4: Run the tests**

Run: `cargo fmt && cargo test --workspace`
Expected: green. `grep -rn 'dash-router/v1' crates docs/*.md README.md 2>/dev/null` prints nothing outside the historical specs.

- [ ] **Step 5: Commit**

```bash
git add -A crates
git commit -m "Wire v2: nested LogRanges on the wire

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: DESIGN.md

**Files:**
- Modify: `DESIGN.md:74-79`

- [ ] **Step 1: Update the sketch**

Replace the `LogRanges` comment and struct in `DESIGN.md` (currently `struct LogRanges(HashMap<LogId, Ranges>);` and the paragraph above it) with:

```rust
/// A log is `(Channel, Author)`; `Channel` is Dash Chat's LogId, shared by
/// every author writing under one topic. Nested channel-first so a
/// channel is stored and sent once for all its authors.
///
/// A key absent means "unknown"; a key present with an empty Ranges means
/// "known but empty" (e.g. a fresh subscription that wants everything).
/// Construction/union preserve that empty-valued marker; intersection/
/// difference (wire-bound results) prune it — "nothing in common" is not a
/// key. is_empty() means every known key maps to an empty range.
struct LogRanges(BTreeMap<Channel, BTreeMap<Author, Ranges>>);
```

- [ ] **Step 2: Commit**

```bash
git add DESIGN.md
git commit -m "DESIGN: LogRanges is nested by channel

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: Dash Chat's `RouterLog` (out of repo, do last)

Only after Tasks 1-6 are merged and Dash Chat is pointed at that dash-router revision.

**Files:**
- Modify: `/home/michael/work/dash-chat/crates/dashchat-node/src/lan_router.rs:95-150`

- [ ] **Step 1: Update the impl**

```rust
    impl dash_router::core::Log for RouterLog {
        type Channel = LogId;
        type Author = [u8; 32];
        fn channel(&self) -> LogId {
            LogId::from(p2panda::Hash::from_bytes(self.log_id))
        }
        fn author(&self) -> [u8; 32] {
            self.author
        }
        fn new(channel: LogId, author: [u8; 32]) -> Self {
            Self {
                log_id: *channel.as_bytes(),
                author,
            }
        }
    }
```

Rename the inherent `author(&self) -> anyhow::Result<VerifyingKey>` to `verifying_key` and fix its callers (`grep -rn '\.author()' crates/dashchat-node/src/lan_router.rs`). Every `subscribe(LogId::from_topic(..))` call is unchanged in meaning. `LogId` must satisfy polestar's `Id` blanket: it is `Copy + Ord + Hash + Debug + Display` already; it lacks `TryFrom<usize>`, so add a newtype or an impl in `lan_router.rs` if the compiler asks.

- [ ] **Step 2: Build and run Dash Chat's lan-router tests**

Run: `cd /home/michael/work/dash-chat && cargo test -p dashchat-node --features lan-router lan_router`
Expected: green.

- [ ] **Step 3: Commit in dash-chat**

```bash
git commit -am "lan-router: RouterLog implements Log::{Channel, Author, new}

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```
