# LAN Router, dash-router side — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `dash-router` embeddable by Dash Chat: rename the shell crate, add prefix subscriptions to the protocol, an embedder-supplied gossip transport with verified sender identity, size-aware wire packing, and a 64-byte relay-store key.

**Architecture:** The protocol change (prefix subscriptions) lands first in the pure core and its models, then is mirrored in the tokio shell exactly the way every earlier shell change was mirrored: `NodeMachine` and `NodeCore` stay line-for-line transcriptions of each other and the lockstep conformance test proves it. Everything else is additive shell surface (`PeerKey`, `GossipTransport`, `pack.rs`) that Dash Chat's `lan_router` module consumes. This plan ships on its own; the Dash Chat plan (`~/work/dash-chat-lan-router/docs/superpowers/plans/2026-09-22-lan-router.md`) depends on every task here.

**Tech Stack:** Rust edition 2024, polestar (git), tokio 1, redb 4, postcard, p2panda-net/p2panda-core 0.7.1 behind the `p2panda` feature, proptest.

**Spec:** `docs/superpowers/specs/2026-09-22-dash-chat-lan-router-design.md` §3 (all of it) — this plan is that section, task by task. The Dash Chat side (§4) is the other plan.

## Global Constraints

- `dash-router-core` stays pure: no RNG, no clock, no I/O, no tokio.
- Wire becomes `WIRE_VERSION = 1`; gossip topic string `"dash-router/v1"`; the `const _: () = assert!(WIRE_VERSION == 1, ..)` reminder in `panda.rs` is updated in the same commit as the bump.
- `NodeMachine` (`dash-router-core/src/node.rs`) and `NodeCore` (`dash-router/src/shell.rs`) are deliberate duplicates: every semantic change is made to both, in the same task, and `tests/conformance.rs` must pass after it.
- The integer log ids used by tests, models and sim (`u8`, `u32`, …) get `Prefix = Self`, so their existing scenarios keep their meaning. A new two-level example type `Pair` (Task 2) is what tests the real prefix semantics.
- `dash-router` (the shell crate) depends on `p2panda-net` + `p2panda-core` only, behind the off-by-default `p2panda` feature. Never the `p2panda` umbrella crate (spec §3.1).
- `PeerKey`, `PeerIdentity`, `GossipTransport` and `pack.rs` are p2panda-free and NOT feature-gated; only the `VerifyingKey` impls in `panda.rs` are.
- Storage errors degrade, never crash (unchanged posture).
- Sim baselines in `sim-baseline/` change with the protocol (Want messages now carry prefixes; the known-but-empty marker is gone). Task 6 recaptures them once, verified bit-identical across two runs. Coverage must stay 100% on `lan-20` and `lan-50-lossy`; if it drops, stop and investigate.
- Run `cargo test -p <crate>` per task and `cargo test --workspace` at the end of every task that touches more than one crate. Ignored p2panda tests run once, manually, in Task 11.
- zsh: quote glob arguments (`--include='*.rs'`).
- Commit messages end with: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`

## Review Focus

Inputs the spec implies but no single section spells out. Each has a test pinned to the owning task.

1. **A Have for a log whose author is new, under a subscribed prefix** must land in ext and be `Delivered`, not parked in the relay (Task 4 test `have_for_new_author_under_subscribed_prefix_is_delivered`, Task 5 mirror).
2. **A prefix Want from a node that already holds part of a log** must be answered only with the ranges it named for that log, never the whole log again (Task 3 test `prefix_want_excludes_logs_the_wanter_named`).
3. **Unsubscribe(prefix) then a new Have under it** goes back to the relay, and `open` no longer carries the prefix (Task 4 test `unsubscribe_prefix_parks_later_haves_in_relay`).
4. **A single op larger than the byte budget** is still advertised header-only rather than blocking every op behind it (Task 9 test `oversize_op_goes_header_only`).
5. **A wire message whose claimed sender disagrees with the verified envelope author** is dropped and counted, and a matching one is processed (Task 7 test `sender_must_match_envelope_author`).
6. **A Want with no ranges and no prefixes** must still not arm the have timer (finding 8(a) kept; Task 5 updates the existing gate and its test).

---

### Task 1: Rename `dash-router-net` → `dash-router`

Spec §3.0. Mechanical; no behaviour change.

**Files:**
- Move: `crates/dash-router-net/` → `crates/dash-router/` (git mv)
- Modify: `crates/dash-router/Cargo.toml` (package name, description)
- Modify: `crates/dash-router/src/lib.rs` (re-exports)
- Modify: every file that names the crate: `crates/dash-router-sim/Cargo.toml`, `crates/dash-router-sim/src/{lib,scenario,behavior}.rs`, `crates/dash-router-core/src/{node,storage}.rs`, `crates/dash-router-net-model/Cargo.toml` (only if it dev-depends on the shell; check), `crates/dash-router/src/panda.rs` (doc comment run instructions), `crates/dash-router/tests/*.rs`, `docs/superpowers/specs/*.md`, `docs/superpowers/plans/*.md`, `README.md` if it mentions the crate.

**Interfaces:**
- Produces: crate `dash-router` with `pub use dash_router_core as core; pub use dash_router_policy as policy;` in addition to everything `dash-router` exported. `dash-router-net-model` keeps its name.

- [ ] **Step 1: Move the directory and rename the package**

```bash
cd /home/michael/work/dash-router
git mv crates/dash-router crates/dash-router
sed -i 's/^name = "dash-router"$/name = "dash-router"/' crates/dash-router/Cargo.toml
grep -n '^name' crates/dash-router/Cargo.toml
```

Expected: `name = "dash-router"`.

- [ ] **Step 2: Rewrite every reference, sparing `dash-router-net-model`**

The model crate's name contains the old name as a prefix, so use a negative-lookahead-free trick: rename `-model` variants to a sentinel, replace, restore.

```bash
cd /home/michael/work/dash-router
files=$(grep -rl 'dash-router\|dash_router' --include='*.rs' --include='*.toml' --include='*.md' crates docs README.md 2>/dev/null)
for f in $files; do
  sed -i -e 's/dash-router-net-model/dash-router-net-model/g' -e 's/dash_router_net_model/dash_router_net_model/g' \
         -e 's/dash-router/dash-router/g' -e 's/dash_router/dash_router/g' \
         -e 's/dash-router-net-model/dash-router-net-model/g' -e 's/dash_router_net_model/dash_router_net_model/g' "$f"
done
grep -rn 'dash-router\b\|dash_router\b' --include='*.rs' --include='*.toml' --include='*.md' crates docs | grep -v 'net-model\|net_model'
```

Expected: the final grep prints nothing.

- [ ] **Step 3: Add the re-exports**

In `crates/dash-router/src/lib.rs`, after the `pub mod` lines:

```rust
/// The pure protocol core, re-exported so an embedder depends on one crate.
pub use dash_router_core as core;
/// The interval/debounce policies, likewise.
pub use dash_router_policy as policy;
```

- [ ] **Step 4: Build and test the whole workspace**

```bash
cargo build --workspace --all-targets && cargo test --workspace 2>&1 | tail -20
```

Expected: everything compiles; all non-ignored tests pass, same counts as before the move.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "Rename dash-router-net to dash-router; re-export core and policy

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: The `Log` trait and the `Pair` example type (core)

Spec §3.5 "Log type". Pure addition; nothing uses it yet.

**Files:**
- Create: `crates/dash-router-core/src/log.rs`
- Modify: `crates/dash-router-core/src/lib.rs` (add `pub mod log;` and `pub use log::{Log, Pair, WireLog};`)

**Interfaces:**
- Produces:
  ```rust
  pub trait Log: Id { type Prefix: Id; fn prefix(&self) -> Self::Prefix; }
  pub trait WireLog: Log<Prefix: Serialize + DeserializeOwned> + Serialize + DeserializeOwned {}  // blanket impl
  pub struct Pair { pub prefix: u8, pub author: u8 }  // Log<Prefix = u8>, Display "p/a"
  ```
  `Log` is implemented for `u8, u16, u32, u64, usize` with `Prefix = Self`.

- [ ] **Step 1: Write the failing tests**

Create `crates/dash-router-core/src/log.rs` with only the tests first:

```rust
//! A log identity with a *prefix*: the part a subscription names (spec
//! 2026-09-22 §3.5). Subscribing to a prefix means every log under it, now
//! and in the future. For the integer ids used by tests, models and the
//! sim the prefix is the id itself, so a prefix subscription to `7` is
//! exactly a subscription to log `7`.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_ids_are_their_own_prefix() {
        assert_eq!(7u8.prefix(), 7u8);
        assert_eq!(9u32.prefix(), 9u32);
    }

    #[test]
    fn pair_prefix_is_the_first_half() {
        let l = Pair { prefix: 3, author: 9 };
        assert_eq!(l.prefix(), 3);
        assert_eq!(l.to_string(), "3/9");
    }

    #[test]
    fn pair_orders_by_prefix_then_author() {
        let a = Pair { prefix: 1, author: 9 };
        let b = Pair { prefix: 2, author: 0 };
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

- [ ] **Step 2: Run to verify it fails**

Add `pub mod log;` to `crates/dash-router-core/src/lib.rs`, then:

```bash
cargo test -p dash-router-core log:: 2>&1 | grep -E 'error|cannot find' | head -5
```

Expected: compile errors (`Log`, `Pair`, `WireLog` not found).

- [ ] **Step 3: Implement**

Above the tests in `log.rs`:

```rust
use polestar::prelude::Id;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub trait Log: Id {
    type Prefix: Id;
    fn prefix(&self) -> Self::Prefix;
}

macro_rules! self_prefixed {
    ($($t:ty),* $(,)?) => {
        $(impl Log for $t {
            type Prefix = Self;
            fn prefix(&self) -> Self { *self }
        })*
    };
}
self_prefixed!(u8, u16, u32, u64, usize);

/// `Log` plus the serde bounds the wire needs on both halves. Blanket:
/// nothing implements this by hand.
pub trait WireLog:
    Log<Prefix: Serialize + DeserializeOwned> + Serialize + DeserializeOwned
{
}
impl<T> WireLog for T where
    T: Log<Prefix: Serialize + DeserializeOwned> + Serialize + DeserializeOwned
{
}

/// A two-level log id for tests and examples: `(prefix, author)`, the
/// shape Dash Chat's `(LogId, author)` has. Orders prefix-first so a
/// prefix's logs are contiguous, exactly as the relay store keys them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Pair {
    pub prefix: u8,
    pub author: u8,
}

impl Pair {
    pub const fn new(prefix: u8, author: u8) -> Self {
        Self { prefix, author }
    }
}

impl std::fmt::Display for Pair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.prefix, self.author)
    }
}

impl Log for Pair {
    type Prefix = u8;
    fn prefix(&self) -> u8 {
        self.prefix
    }
}
```

In `lib.rs` add `pub use log::{Log, Pair, WireLog};`.

- [ ] **Step 4: Run to verify it passes**

```bash
cargo test -p dash-router-core log:: 2>&1 | tail -5
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/dash-router-core/src/log.rs crates/dash-router-core/src/lib.rs
git commit -m "core: Log trait with a Prefix, Pair example type, WireLog alias

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: Wire v1 and prefix-aware router (core)

Spec §3.5 "Wire", "Router state and actions". The router learns `open` prefixes and answers prefix Wants. The node glue (Task 4) and shell (Task 5) get compile-through edits only in this task so the workspace keeps building; their prefix *semantics* arrive in their own tasks.

**Files:**
- Modify: `crates/dash-router-core/src/wire.rs`
- Modify: `crates/dash-router-core/src/router.rs`
- Modify: `crates/dash-router-core/tests/router.rs` (new tests)
- Modify (compile-through only): `crates/dash-router-core/src/node.rs`, `crates/dash-router-core/tests/node.rs`, `crates/dash-router/src/shell.rs`, `crates/dash-router/src/panda.rs`, `crates/dash-router/tests/conformance.rs`, `crates/dash-router-sim/src/behavior.rs`, `crates/dash-router-net-model/tests/net.rs`

**Interfaces:**
- Produces:
  ```rust
  pub const WIRE_VERSION: u8 = 1;
  pub enum WireBody<L: Log> { Want { ranges: LogRanges<L>, prefixes: BTreeSet<L::Prefix> }, Have(Vec<(L, Vec<(Seq, Op)>)>) }
  impl WireMessage<N, L> { pub fn want(sender: N, ranges: LogRanges<L>, prefixes: BTreeSet<L::Prefix>) -> Self; pub fn have(..) /* unchanged */ }
  pub struct Record<L: Log, T> { pub ranges: LogRanges<L>, pub prefixes: BTreeSet<L::Prefix>, pub ttl_left: T }
  RouterState { .., pub open: BTreeSet<L::Prefix>, .. }
  RouterAction::Open(BTreeSet<L::Prefix>)
  RouterAction::RecvWant { from: N, ranges: LogRanges<L>, prefixes: BTreeSet<L::Prefix> }
  Effect::SendWant { ranges: LogRanges<L>, prefixes: BTreeSet<L::Prefix> }
  RouterState::next_want(&self) -> (LogRanges<L>, BTreeSet<L::Prefix>)
  RouterState::others_prefixes(&self) -> BTreeSet<L::Prefix>
  ```
  All `L: Id` bounds in `router.rs` become `L: Log`; in `wire.rs` and `node.rs` `L: Id + Serialize + DeserializeOwned` becomes `L: WireLog`.

- [ ] **Step 1: Write the failing router tests**

Append to `crates/dash-router-core/tests/router.rs` (keep the file's existing helpers; `A` is its alias for `RouterAction`, adapt names if they differ — read the top of the file first):

```rust
mod prefix {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use dash_router_core::{
        Effect, LogRanges, Pair, Ranges, RouterAction, RouterConfig, RouterMachine, RouterState,
    };
    use polestar::prelude::*;
    use polestar::time::RealTime;

    type M = RouterMachine<u32, Pair, RealTime>;
    type A = RouterAction<u32, Pair, RealTime>;

    fn machine() -> M {
        RouterMachine::new(RouterConfig {
            want_ttl: Duration::from_millis(500).into(),
            have_ttl: Duration::from_millis(500).into(),
        })
    }

    fn held(pairs: &[(Pair, Ranges)]) -> LogRanges<Pair> {
        LogRanges::from_pairs(pairs.iter().cloned())
    }

    fn sent_have(fx: &[Effect<Pair>]) -> Option<LogRanges<Pair>> {
        fx.iter().find_map(|e| match e {
            Effect::SendHave(r) => Some(r.clone()),
            _ => None,
        })
    }

    fn sent_want(fx: &[Effect<Pair>]) -> Option<(LogRanges<Pair>, BTreeSet<u8>)> {
        fx.iter().find_map(|e| match e {
            Effect::SendWant { ranges, prefixes } => Some((ranges.clone(), prefixes.clone())),
            _ => None,
        })
    }

    /// Spec §3.5: a prefix Want is answered with every held log under the
    /// prefix that the wanter did not name explicitly; a log it named gets
    /// only its named ranges.
    #[test]
    fn prefix_want_excludes_logs_the_wanter_named() {
        let m = machine();
        let a1 = Pair::new(1, 1);
        let a2 = Pair::new(1, 2);
        let other = Pair::new(2, 1);
        let s = RouterState::new(
            0u32,
            held(&[
                (a1, Ranges::range(0, 10)),
                (a2, Ranges::range(0, 5)),
                (other, Ranges::range(0, 3)),
            ]),
        );
        // Peer 7 holds a1 up to 4 and wants its tail; knows nothing else under prefix 1.
        let (s, _) = m
            .transition(
                s,
                A::RecvWant {
                    from: 7,
                    ranges: held(&[(a1, Ranges::from(4))]),
                    prefixes: BTreeSet::from([1u8]),
                },
            )
            .unwrap();
        let (s, _) = m.transition(s, A::ArmHaveTimer(Duration::ZERO.into())).unwrap();
        let (_, fx) = m.transition(s, A::FireHave).unwrap();
        let have = sent_have(&fx).expect("a Have is sent");
        assert_eq!(have.get(&a1), Some(&Ranges::range(4, 10)), "named log: only the named tail");
        assert_eq!(have.get(&a2), Some(&Ranges::range(0, 5)), "unnamed log under prefix: wholesale");
        assert_eq!(have.get(&other), None, "other prefix: untouched");
    }

    /// `Open` prefixes ride on every own Want even when no ranges are wanted.
    #[test]
    fn open_prefixes_are_sent_with_fire_want() {
        let m = machine();
        let s = RouterState::new(0u32, LogRanges::empty());
        let (s, _) = m.transition(s, A::Open(BTreeSet::from([4u8, 5u8]))).unwrap();
        let (s, _) = m.transition(s, A::ArmWantTimer(Duration::ZERO.into())).unwrap();
        let (_, fx) = m.transition(s, A::FireWant).unwrap();
        let (ranges, prefixes) = sent_want(&fx).expect("a Want is sent");
        assert!(ranges.is_empty());
        assert_eq!(prefixes, BTreeSet::from([4u8, 5u8]));
    }

    /// A received Want's prefixes are relayed once (seen-set), like ranges.
    #[test]
    fn received_prefixes_are_relayed_once() {
        let m = machine();
        let s = RouterState::new(0u32, LogRanges::empty());
        let want = |from| A::RecvWant {
            from,
            ranges: LogRanges::empty(),
            prefixes: BTreeSet::from([9u8]),
        };
        let (s, fx1) = m.transition(s, want(1)).unwrap();
        assert_eq!(sent_want(&fx1).map(|(_, p)| p), Some(BTreeSet::from([9u8])));
        let (_, fx2) = m.transition(s, want(2)).unwrap();
        assert!(sent_want(&fx2).is_none(), "already relayed within want_ttl");
    }

    /// Eviction policy input: logs under a peer's wanted prefix count as wanted.
    #[test]
    fn others_wants_includes_held_logs_under_wanted_prefixes() {
        let m = machine();
        let a1 = Pair::new(1, 1);
        let s = RouterState::new(0u32, held(&[(a1, Ranges::range(0, 10))]));
        let (s, _) = m
            .transition(
                s,
                A::RecvWant {
                    from: 7,
                    ranges: LogRanges::empty(),
                    prefixes: BTreeSet::from([1u8]),
                },
            )
            .unwrap();
        assert_eq!(s.others_wants().get(&a1), Some(&Ranges::range(0, 10)));
    }
}
```

- [ ] **Step 2: Run to verify they fail**

```bash
cargo test -p dash-router-core --test router prefix:: 2>&1 | grep -E '^error' | head -5
```

Expected: compile errors (`Open`, `prefixes` field, `SendWant { .. }` unknown).

- [ ] **Step 3: Change the wire**

In `crates/dash-router-core/src/wire.rs`:

```rust
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    log::{Log, WireLog},
    op::Op,
    ranges::{LogRanges, Seq},
};

/// Bump together with the gossip topic on breaking change.
/// v1: Want carries prefixes (spec 2026-09-22 §3.5).
pub const WIRE_VERSION: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound(
    serialize = "N: Serialize, L: Serialize, L::Prefix: Serialize",
    deserialize = "N: Deserialize<'de>, L: Deserialize<'de>, L::Prefix: Deserialize<'de>"
))]
pub struct WireMessage<N, L: Log> {
    pub version: u8,
    /// Gossip strips the transport sender; we carry our own. Checked
    /// against the transport's verified author when it has one (shell).
    pub sender: N,
    pub body: WireBody<L>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound(
    serialize = "L: Serialize, L::Prefix: Serialize",
    deserialize = "L: Deserialize<'de>, L::Prefix: Deserialize<'de>"
))]
pub enum WireBody<L: Log> {
    /// `ranges`: gaps and open tails for logs the wanter already knows.
    /// `prefixes`: "and every log under these that I did not name".
    Want {
        ranges: LogRanges<L>,
        prefixes: BTreeSet<L::Prefix>,
    },
    /// Hydrated ops grouped per log, in (log, seq) order; payloads may be
    /// None (GC'd). Grouping avoids repeating the log id per op.
    Have(Vec<(L, Vec<(Seq, Op)>)>),
}

impl<N: Serialize + DeserializeOwned, L: WireLog> WireMessage<N, L> {
    pub fn want(sender: N, ranges: LogRanges<L>, prefixes: BTreeSet<L::Prefix>) -> Self {
        Self {
            version: WIRE_VERSION,
            sender,
            body: WireBody::Want { ranges, prefixes },
        }
    }
    // `have`, `encode`, `decode` unchanged.
}
```

Update the test at the bottom of `wire.rs` (currently `WireMessage::want(7, LogRanges::from_pairs([(1u8, Ranges::from(3))]))`) to pass `BTreeSet::from([2u8])` as the third argument and assert it round-trips.

- [ ] **Step 4: Change the router**

In `crates/dash-router-core/src/router.rs`:

1. Imports: add `use std::collections::BTreeSet;` and `use crate::log::Log;`. Replace every `L: Id` bound in this file with `L: Log` (the `impl<N: Id, L: Id, T: TimeInterval>` blocks, `Record<L: Ord, T>` → `Record<L: Log, T>`, `RouterState<N: Ord, L: Ord, T>` → `RouterState<N: Ord, L: Log, T>`, `RouterAction<N, L: Ord, T>` → `<N, L: Log, T>`, `Effect<L: Ord>` → `Effect<L: Log>`, `combined_ranges<L: Id, T>` → `<L: Log, T>`).

2. `Record` gains prefixes and a constructor:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Record<L: Log, T> {
    pub ranges: LogRanges<L>,
    /// Wholesale interests carried by a Want; always empty on Have records.
    pub prefixes: BTreeSet<L::Prefix>,
    pub ttl_left: T,
}

impl<L: Log, T> Record<L, T> {
    fn ranges_only(ranges: LogRanges<L>, ttl_left: T) -> Self {
        Self { ranges, prefixes: BTreeSet::new(), ttl_left }
    }
}
```

3. `RouterState` gains `pub open: BTreeSet<L::Prefix>` (doc: "This node's wholesale interests: every log under these prefixes, known or not. Set by `Open` from the node glue's subscriptions."), initialised to `BTreeSet::new()` in `new`.

4. Replace `others_wants`, `next_want`, and add helpers:

```rust
    /// Held logs with data whose prefix is in `prefixes` and that `named`
    /// does not mention: the wholesale half of a Want's answer.
    fn held_under(&self, prefixes: &BTreeSet<L::Prefix>, named: &LogRanges<L>) -> LogRanges<L> {
        LogRanges::from_pairs(
            self.held
                .iter()
                .filter(|(log, r)| {
                    !r.is_empty() && prefixes.contains(&log.prefix()) && named.get(log).is_none()
                })
                .map(|(log, r)| (*log, r.clone())),
        )
    }

    /// Union of recent Wants from other nodes, with each peer's prefix
    /// interests expanded over what this node holds (spec §3.5).
    pub fn others_wants(&self) -> LogRanges<L> {
        self.wants.values().fold(LogRanges::empty(), |acc, r| {
            acc.union(&r.ranges).union(&self.held_under(&r.prefixes, &r.ranges))
        })
    }

    /// Union of recent Want prefixes from other nodes.
    pub fn others_prefixes(&self) -> BTreeSet<L::Prefix> {
        self.wants.values().flat_map(|r| r.prefixes.iter().copied()).collect()
    }

    /// DESIGN.md §1, plus this node's open prefixes. Prefixes are never
    /// suppressed by others' prefixes: two nodes' explicit knowledge under
    /// the same prefix differs, so one node's answer is not the other's.
    pub fn next_want(&self) -> (LogRanges<L>, BTreeSet<L::Prefix>) {
        (self.wanted().difference(&self.others_wants()), self.open.clone())
    }

    pub fn relayed_want_prefixes(&self) -> BTreeSet<L::Prefix> {
        self.relayed_wants.iter().flat_map(|r| r.prefixes.iter().copied()).collect()
    }

    fn note_relayed_wants(&mut self, ranges: LogRanges<L>, prefixes: BTreeSet<L::Prefix>, ttl: T) {
        self.relayed_wants.push(Record { ranges, prefixes, ttl_left: ttl });
    }
```

`next_have` is unchanged in text (it uses `others_wants`, which now expands prefixes). `note_relayed_haves` and `note_have` construct `Record::ranges_only(..)`.

5. Actions and effects:

```rust
    /// Replace this node's wholesale interests (the node glue's
    /// subscriptions, by prefix).
    Open(BTreeSet<L::Prefix>),
    /// A Want message arrives from another node.
    RecvWant { from: N, ranges: LogRanges<L>, prefixes: BTreeSet<L::Prefix> },
```

```rust
pub enum Effect<L: Log> {
    /// Broadcast a Want to the LAN.
    SendWant { ranges: LogRanges<L>, prefixes: BTreeSet<L::Prefix> },
    SendHave(LogRanges<L>),
    Accept(LogRanges<L>),
}
```

6. Transitions:

```rust
            RouterAction::Open(prefixes) => {
                s.open = prefixes;
            }

            RouterAction::FireWant => {
                let Some(timer) = &s.want_timer else { bail!("want timer not armed") };
                ensure!(timer.remaining.is_zero(), "want timer not due");
                s.want_timer = None;
                let (ranges, prefixes) = s.next_want();
                if !ranges.is_empty() || !prefixes.is_empty() {
                    s.note_relayed_wants(ranges.clone(), prefixes.clone(), self.config.want_ttl);
                    fx.push(Effect::SendWant { ranges, prefixes });
                }
            }

            RouterAction::RecvWant { from, ranges, prefixes } => {
                ensure!(from != s.id, "received own message");
                let relay_ranges = ranges.difference(&s.relayed_want_ranges());
                let relay_prefixes: BTreeSet<L::Prefix> = prefixes
                    .difference(&s.relayed_want_prefixes())
                    .copied()
                    .collect();
                if !relay_ranges.is_empty() || !relay_prefixes.is_empty() {
                    s.note_relayed_wants(relay_ranges.clone(), relay_prefixes.clone(), self.config.want_ttl);
                    fx.push(Effect::SendWant { ranges: relay_ranges, prefixes: relay_prefixes });
                }
                s.wants.insert(from, Record { ranges, prefixes, ttl_left: self.config.want_ttl });
            }
```

Every other arm keeps its text; `Tick` already decays `relayed_wants` records whole.

- [ ] **Step 5: Compile-through edits elsewhere**

Make the rest of the workspace build without changing semantics yet:

- `crates/dash-router-core/src/node.rs`: bounds `L: Id + Serialize + DeserializeOwned` → `L: WireLog` (import `crate::log::WireLog`). In `NodeAction::Recv` → `WireBody::Want { ranges, prefixes }` → `RouterAction::RecvWant { from: wire.sender, ranges, prefixes }`. In `route_router_fx`: `Effect::SendWant { ranges, prefixes } => out.push(NodeEffect::Broadcast(WireMessage::want(s.router.id, ranges, prefixes)))`. Also forbid `RouterAction::Open` alongside `Held`/`Push` in the `NodeAction::Router` guard (same `ensure!`, message "held/push/open are the node glue's business").
- `crates/dash-router-core/tests/router.rs` line ~51: `Effect::SendWant(r) => Some(r)` becomes `Effect::SendWant { ranges, .. } => Some(ranges)`.
- `crates/dash-router-core/tests/node.rs` and `crates/dash-router/src/shell.rs` tests, `crates/dash-router/tests/conformance.rs`: every `WireMessage::want(a, b)` → `WireMessage::want(a, b, BTreeSet::new())`; every pattern `WireBody::Want(r)` → `WireBody::Want { ranges: r, .. }`.
- `crates/dash-router/src/shell.rs`: same two edits as node.rs (`on_wire` Want arm and `route_fx` SendWant arm); bounds `L: Id + Serialize + DeserializeOwned` → `L: WireLog` on `NodeCore`, `spawn`, `route_outs`.
- `crates/dash-router/src/panda.rs`: `TOPIC_NAME = "dash-router/v1"`, `assert!(WIRE_VERSION == 1, ..)`.
- `crates/dash-router-sim/src/behavior.rs` lines ~326 and ~418: `WireBody::Want(_)` → `WireBody::Want { .. }`.
- `crates/dash-router-net-model/tests/net.rs` ~233: `RouterAction::RecvWant { from, ranges }` → add `prefixes: BTreeSet::new()`; `crates/dash-router-net-model/src/net.rs` bounds → `L: WireLog`.

```bash
cargo build --workspace --all-targets 2>&1 | grep -E '^(error|warning: unused)' | head
```

Expected: clean.

- [ ] **Step 6: Run the router tests and the whole workspace**

```bash
cargo test -p dash-router-core --test router 2>&1 | tail -5
cargo test --workspace 2>&1 | grep -E 'test result|FAILED|panicked' | head -20
```

Expected: the four new `prefix::` tests pass; all existing tests still pass (no semantics changed for `Prefix = Self` types yet: `open` is empty everywhere until Task 4).

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "core: wire v1 with prefix Wants; router Open prefixes and prefix-aware Have answers

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Prefix subscriptions in `NodeMachine` (core)

Spec §3.5 "Node glue". Subscriptions become prefixes; Subscribe migrates every relay log under the prefix; the known-but-empty marker goes away (the prefix Want carries that intent).

**Files:**
- Modify: `crates/dash-router-core/src/node.rs`
- Modify: `crates/dash-router-core/tests/node.rs`

**Interfaces:**
- Produces:
  ```rust
  NodeState { .., pub subscriptions: BTreeSet<L::Prefix> }
  impl NodeState { pub fn is_subscribed(&self, log: &L) -> bool; pub fn new(id, machine, subscriptions: impl IntoIterator<Item = L::Prefix>) -> Self }
  NodeAction::Subscribe(L::Prefix), NodeAction::Unsubscribe(L::Prefix)
  ```
  `held_union` no longer inserts empty markers.

- [ ] **Step 1: Write the failing tests**

Append to `crates/dash-router-core/tests/node.rs` (read its helpers `n`, `l`, `lr` first and reuse them; the module below uses `Pair` explicitly):

```rust
mod prefix {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use dash_router_core::{
        LogRanges, NodeAction, NodeEffect, NodeMachine, NodeState, Op, Pair, Ranges, RouterConfig,
        WireBody, WireMessage,
    };
    use polestar::prelude::*;
    use polestar::time::RealTime;

    type M = NodeMachine<u32, Pair, RealTime>;

    fn machine() -> M {
        NodeMachine::new(
            RouterConfig {
                want_ttl: Duration::from_millis(500).into(),
                have_ttl: Duration::from_millis(500).into(),
            },
            1 << 20,
        )
    }

    fn op(b: u8) -> Op {
        Op { header: vec![b], payload: Some(vec![b; 4]) }
    }

    fn have(from: u32, log: Pair, seqs: &[u32]) -> WireMessage<u32, Pair> {
        WireMessage::have(from, vec![(log, seqs.iter().map(|&q| (q, op(q as u8))).collect())])
    }

    /// Review focus 1: a never-seen author under a subscribed prefix is ext data.
    #[test]
    fn have_for_new_author_under_subscribed_prefix_is_delivered() {
        let m = machine();
        let s = NodeState::new(0u32, m.clone(), [1u8]);
        let new_author = Pair::new(1, 42);
        let (s, fx) = m.transition(s, NodeAction::Recv(have(7, new_author, &[0, 1]))).unwrap();
        assert!(fx.contains(&NodeEffect::Deliver(new_author, 0)));
        assert!(fx.contains(&NodeEffect::Deliver(new_author, 1)));
        assert_eq!(s.ext.0.held_all().get(&new_author), Some(&Ranges::range(0, 2)));
        assert!(s.relay.0.held_all().get(&new_author).is_none());
    }

    /// Subscribe(prefix) migrates every author's relay log under it to ext.
    #[test]
    fn subscribe_prefix_migrates_all_relay_logs_under_it() {
        let m = machine();
        let s = NodeState::new(0u32, m.clone(), []); // pure relay
        let a1 = Pair::new(1, 1);
        let a2 = Pair::new(1, 2);
        let other = Pair::new(2, 1);
        let (s, _) = m.transition(s, NodeAction::Recv(have(7, a1, &[0]))).unwrap();
        let (s, _) = m.transition(s, NodeAction::Recv(have(7, a2, &[0, 1]))).unwrap();
        let (s, _) = m.transition(s, NodeAction::Recv(have(7, other, &[0]))).unwrap();
        let (s, _) = m.transition(s, NodeAction::Subscribe(1u8)).unwrap();
        assert!(s.is_subscribed(&a1) && s.is_subscribed(&a2) && !s.is_subscribed(&other));
        assert_eq!(s.ext.0.held_all().get(&a1), Some(&Ranges::range(0, 1)));
        assert_eq!(s.ext.0.held_all().get(&a2), Some(&Ranges::range(0, 2)));
        assert!(s.relay.0.held_all().get(&a1).is_none());
        assert!(s.relay.0.held_all().get(&a2).is_none());
        assert_eq!(s.relay.0.held_all().get(&other), Some(&Ranges::range(0, 1)));
        assert_eq!(s.router.open, BTreeSet::from([1u8]));
    }

    /// Review focus 3.
    #[test]
    fn unsubscribe_prefix_parks_later_haves_in_relay() {
        let m = machine();
        let s = NodeState::new(0u32, m.clone(), [1u8]);
        let (s, _) = m.transition(s, NodeAction::Unsubscribe(1u8)).unwrap();
        assert!(s.router.open.is_empty());
        let a1 = Pair::new(1, 1);
        let (s, fx) = m.transition(s, NodeAction::Recv(have(7, a1, &[0]))).unwrap();
        assert!(!fx.iter().any(|e| matches!(e, NodeEffect::Deliver(..))));
        assert_eq!(s.relay.0.held_all().get(&a1), Some(&Ranges::range(0, 1)));
    }

    /// The prefix Want replaces the known-but-empty marker: a subscription
    /// with nothing stored sends `prefixes`, not a full range for a log.
    #[test]
    fn empty_subscription_wants_by_prefix_not_marker() {
        let m = machine();
        let s = NodeState::new(0u32, m.clone(), [1u8]);
        assert!(s.held_union().is_empty(), "no marker");
        let (s, _) = m
            .transition(s, NodeAction::Router(RouterAction::ArmWantTimer(Duration::ZERO.into())))
            .unwrap();
        let (_, fx) = m.transition(s, NodeAction::Router(RouterAction::FireWant)).unwrap();
        let want = fx.iter().find_map(|e| match e {
            NodeEffect::Broadcast(WireMessage { body: WireBody::Want { ranges, prefixes }, .. }) => {
                Some((ranges.clone(), prefixes.clone()))
            }
            _ => None,
        });
        assert_eq!(want, Some((LogRanges::empty(), BTreeSet::from([1u8]))));
    }
}
```

(Add `use dash_router_core::RouterAction;` inside the module.)

- [ ] **Step 2: Run to verify they fail**

```bash
cargo test -p dash-router-core --test node prefix:: 2>&1 | grep -E '^error|panicked' | head -5
```

Expected: compile errors (`is_subscribed`, `Subscribe(1u8)` type mismatch).

- [ ] **Step 3: Implement in `node.rs`**

```rust
pub struct NodeState<N: Id, L: Log, T: TimeInterval> {
    pub router: SM<RouterMachine<N, L, T>>,
    pub relay: SM<RelayStoreMachine<L>>,
    pub ext: SM<ExtStoreMachine<L>>,
    /// Subscriptions by prefix (spec 2026-09-22 §3.5): a log is subscribed
    /// iff its prefix is.
    pub subscriptions: BTreeSet<L::Prefix>,
}

/// held snapshot = relay ∪ ext. No empty markers: a subscribed prefix
/// with nothing stored is advertised by the Want's `prefixes`, not by a
/// per-log marker (there is no log to name before its author is known).
fn held_union<L: Log>(relay: &RelayStoreState<L>, ext: &ExtStoreState<L>) -> LogRanges<L> {
    relay.0.held_all().union(&ext.0.held_all())
}
```

`NodeState::held_union(&self)` calls `held_union(&self.relay, &self.ext)`. Add:

```rust
    pub fn is_subscribed(&self, log: &L) -> bool {
        self.subscriptions.contains(&log.prefix())
    }

    /// Every relay-held log under `prefix`, as full ranges: what Subscribe migrates.
    fn relay_logs_under(&self, prefix: &L::Prefix) -> LogRanges<L> {
        LogRanges::from_pairs(
            self.relay.0.held_all().iter()
                .filter(|(log, _)| log.prefix() == *prefix)
                .map(|(log, _)| (*log, Ranges::full())),
        )
    }
```

`NodeState::new` takes `subscriptions: impl IntoIterator<Item = L::Prefix>`, and after `let mut router = RouterState::new(id, held);` sets `router.open = subscriptions.clone();`.

`NodeAction::{Subscribe, Unsubscribe}(L::Prefix)`; doc: "Start caring about every log under a prefix: migrate relay-side bytes for them to ext and open the prefix on the router."

Transitions:

```rust
            NodeAction::Subscribe(prefix) => {
                s.subscriptions.insert(prefix);
                let under = s.relay_logs_under(&prefix);
                if !under.is_empty() {
                    for (log, seq, op) in s.relay.0.fetch(&under) {
                        s.ext.step(ExtStoreAction::Ingest(log, seq, op))?;
                    }
                    s.relay.step(RelayStoreAction::Evict(under))?;
                }
                s.router.step(RouterAction::Open(s.subscriptions.clone()))?;
                self.reconcile_held(&mut s)?;
            }
            NodeAction::Unsubscribe(prefix) => {
                s.subscriptions.remove(&prefix);
                s.router.step(RouterAction::Open(s.subscriptions.clone()))?;
                self.reconcile_held(&mut s)?;
            }
```

`ingest_parked`: `if s.is_subscribed(log)`. `route_router_fx` Accept arm: `if !s.is_subscribed(log) { continue; }`. The `NodeAction::Router` guard from Task 3 already forbids `Open`.

- [ ] **Step 4: Fix the existing node tests' expectations**

Existing tests in `tests/node.rs` that relied on the empty marker now see the prefix instead. Find them:

```bash
grep -n "Ranges::full()\|known-but-empty\|WireBody::Want" crates/dash-router-core/tests/node.rs
```

For each assertion that expected a Want carrying `(log, Ranges::full())` for a subscribed-but-empty log, change it to expect `ranges` without that log and `prefixes` containing the log's id (with `u8`, `Prefix = Self`, so the prefix is the log id). The test around line 284 is the known one. `NodeState::new(.., [l(0)])` calls keep compiling since `l(0)` is a `u8`.

```bash
cargo test -p dash-router-core 2>&1 | grep -E 'test result|FAILED|panicked' | head
```

Expected: all pass, including the four `prefix::` tests.

- [ ] **Step 5: Commit**

```bash
git add crates/dash-router-core
git commit -m "core: NodeMachine subscribes by prefix; Subscribe migrates every log under it

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: Prefix subscriptions in the shell (`NodeCore`, handle, conformance)

The async mirror of Task 4, plus the embedder API. After this task the conformance test again proves `NodeCore` ≡ `NodeMachine`.

**Files:**
- Modify: `crates/dash-router/src/shell.rs`
- Modify: `crates/dash-router/src/handle.rs`
- Modify: `crates/dash-router/tests/conformance.rs`
- Modify: `crates/dash-router/tests/loop.rs` (only if a subscribe call's type changes — it uses `u8`, so likely none)

**Interfaces:**
- Produces:
  ```rust
  Command<L: Log>::Subscribe { prefix: L::Prefix, reply }, ::Unsubscribe { prefix: L::Prefix, reply }
  RouterHandle<L: Log>::subscribe(&self, prefix: L::Prefix) / unsubscribe(&self, prefix: L::Prefix)
  NodeCore { pub subscriptions: BTreeSet<L::Prefix>, .. }  pub fn is_subscribed(&self, log: &L) -> bool
  NodeCore::on_subscribe(now, prefix: L::Prefix) / on_unsubscribe(now, prefix: L::Prefix)
  spawn(.., subscriptions: BTreeSet<L::Prefix>, ..)
  ```

- [ ] **Step 1: Write the failing shell test**

In `crates/dash-router/src/shell.rs`'s `mod tests`, add (reuse the module's `Scripted`, `config`, `op`, `wire` helpers; read them first):

```rust
    /// Mirror of core `prefix::subscribe_prefix_migrates_all_relay_logs_under_it`
    /// and review focus 1, over the async core with `Pair` logs.
    #[tokio::test]
    async fn subscribe_prefix_migrates_relay_logs_and_delivers_new_authors() {
        use dash_router_core::Pair;
        let mut c: NodeCore<u32, Pair, OpsMap<Pair>, OpsMap<Pair>, Scripted> = NodeCore::new(
            0,
            config(1 << 20),
            BTreeSet::new(),
            OpsMap::default(),
            OpsMap::default(),
            Scripted::ms(&[1000]),
        );
        c.init().await.unwrap();
        let a1 = Pair::new(1, 1);
        let a2 = Pair::new(1, 2);
        let have = |from: u32, log: Pair, seqs: &[u32]| {
            WireMessage::have(from, vec![(log, seqs.iter().map(|&q| (q, op(q as u8, true))).collect())])
        };
        c.on_wire(Duration::from_millis(10), wire(have(7, a1, &[0]))).await.unwrap();
        c.on_wire(Duration::from_millis(11), wire(have(7, a2, &[0, 1]))).await.unwrap();
        assert!(Storage::held_all(&c.ext).is_empty(), "unsubscribed: relay only");

        c.on_subscribe(Duration::from_millis(20), 1u8).await.unwrap();
        assert_eq!(Storage::held_all(&c.ext).get(&a1), Some(&Ranges::range(0, 1)));
        assert_eq!(Storage::held_all(&c.ext).get(&a2), Some(&Ranges::range(0, 2)));
        assert!(Storage::held_all(&c.relay).is_empty());
        assert_eq!(c.router.open, BTreeSet::from([1u8]));

        let a3 = Pair::new(1, 3);
        let out = c.on_wire(Duration::from_millis(30), wire(have(7, a3, &[0]))).await.unwrap();
        assert!(out.iter().any(|o| matches!(o, Out::Event(RouterEvent::Delivered(l, 0)) if *l == a3)));
    }
```

If the module's `op` helper has a different signature, adapt the call; do not change the helper.

- [ ] **Step 2: Run to verify it fails**

```bash
cargo test -p dash-router subscribe_prefix_migrates 2>&1 | grep -E '^error|panicked' | head -5
```

Expected: compile error (`on_subscribe` takes `L`, `open`/`is_subscribed` mismatch).

- [ ] **Step 3: Implement in `shell.rs`**

- `NodeCore.subscriptions: BTreeSet<L::Prefix>`; `new(.., subscriptions: BTreeSet<L::Prefix>, ..)` and after building the `RouterState`: `state.open = subscriptions.clone();` (make `state` `mut`).
- Add:

```rust
    pub fn is_subscribed(&self, log: &L) -> bool {
        self.subscriptions.contains(&log.prefix())
    }
```

- `on_subscribe(&mut self, now, prefix: L::Prefix)`:

```rust
        let mut out = self.advance_to(now).await?;
        self.subscriptions.insert(prefix);
        let under: LogRanges<L> = match self.relay.held_all().await {
            Ok(all) => LogRanges::from_pairs(
                all.iter()
                    .filter(|(log, _)| log.prefix() == prefix)
                    .map(|(log, _)| (*log, Ranges::full())),
            ),
            Err(_) => {
                self.relay_errors += 1;
                LogRanges::empty()
            }
        };
        if !under.is_empty() {
            match self.relay.fetch(&under).await {
                Ok(ops) => {
                    for (l, seq, op) in ops {
                        if let Err(e) = self.ext.ingest(l, seq, op).await {
                            out.push(Out::Event(RouterEvent::StorageError(StorageErrorReport {
                                context: "on_subscribe: ext.ingest",
                                message: e.to_string(),
                            })));
                        }
                    }
                }
                Err(_) => self.relay_errors += 1,
            }
            if self.relay.evict(&under).await.is_err() {
                self.relay_errors += 1;
            }
        }
        self.router.step(RouterAction::Open(self.subscriptions.clone()))?;
        let touched: BTreeSet<L> = under.iter().map(|(l, _)| *l).collect();
        self.reconcile_held(Some(&touched), &mut out).await?;
        Ok(out)
```

- `on_unsubscribe(&mut self, now, prefix: L::Prefix)`: remove, `Open`, then `reconcile_held(None, ..)` (a full rebuild; there is no per-log marker to drop any more, but `Held` must be re-stepped so the router sees the same snapshot the reference does).
- `reconcile_held`: delete both marker branches (`if m.is_empty() && !self.subscriptions.contains(log)` → always `if m.is_empty() { remove } else { insert }`; and the `for log in &self.subscriptions { insert empty }` loop in the `None` arm).
- `ingest_parked`, `route_fx` Accept, `hydrate`: `self.is_subscribed(log)`.
- `on_wire` Want arm: `let ranges_nonempty = !ranges.is_empty() || !prefixes.is_empty();` (rename the variable `want_nonempty`).
- `spawn(.., subscriptions: BTreeSet<L::Prefix>, ..)`; the `Command::Subscribe { prefix, reply }` / `Unsubscribe` arms call `on_subscribe(now, prefix)`.
- Bounds: `NodeCore`'s impl block `L: WireLog`; `spawn` `L: WireLog + 'static` (Id already brings Send + Sync + 'static).

In `handle.rs`: `Command<L: Log>` with `Subscribe { prefix: L::Prefix, reply }` and `Unsubscribe { prefix: L::Prefix, reply }`; `RouterHandle<L: Log>`; `subscribe(&self, prefix: L::Prefix)`, `unsubscribe(&self, prefix: L::Prefix)`; `RouterEvent<L: Log>` keeps `Delivered(L, Seq)`.

- [ ] **Step 4: Update the conformance driver**

In `crates/dash-router/tests/conformance.rs`:
- `Step::Subscribe(u8)` stays (`Prefix = Self`); the SUT call becomes `on_subscribe(now, log)` — same argument, new meaning; the reference call `NodeAction::Subscribe(log)` likewise.
- The `RecvWant` step builds `WireMessage::want(from, ranges, BTreeSet::new())`; add a second generator arm `Step::RecvPrefixWant { from: u32, prefix: u8 }` (weight 1) that sends `WireMessage::want(from, LogRanges::empty(), BTreeSet::from([prefix]))` to both machines, with the same have-timer arming gate (`!ranges.is_empty() || !prefixes.is_empty()`).
- The projection comparison (`assert_router_matches` / the `subscriptions` comparison near line 505) compares `BTreeSet<u8>` on both sides — unchanged types, so it compiles; also compare `core.router.open` with `ref_state.router.open`.

Update the shell unit tests that asserted the marker (`grep -n "Ranges::full()" crates/dash-router/src/shell.rs` around lines 1061 and 1146): they now expect `WireBody::Want { ranges, prefixes }` with `prefixes.contains(&0)` and `ranges.get(&0) == None` for a subscribed-but-empty log.

- [ ] **Step 5: Run the crate and the workspace**

```bash
cargo test -p dash-router 2>&1 | grep -E 'test result|FAILED|panicked' | head
cargo test --workspace 2>&1 | grep -E 'test result|FAILED|panicked' | head -20
```

Expected: all pass, including `conformance` (proptest, 256 cases by default) and `loop`.

- [ ] **Step 6: Commit**

```bash
git add crates/dash-router
git commit -m "shell: NodeCore mirrors prefix subscriptions; RouterHandle subscribes by prefix

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: Model and sim: bounds, and baseline recapture

Spec §3.5 "Models and tests". The net model and sim already compile (Task 3); this task checks their behaviour and recaptures the baselines the protocol change moves.

**Files:**
- Modify: `crates/dash-router-net-model/src/net.rs` (bounds only, if any `L: Id + Serialize` remain)
- Modify: `crates/dash-router-sim/src/behavior.rs` (only if `node.subscriptions` iteration needs a type tweak; with `u8` it should not)
- Recapture: `sim-baseline/`

- [ ] **Step 1: Run the model and sim tests**

```bash
cargo test -p dash-router-net-model -p dash-router-sim 2>&1 | grep -E 'test result|FAILED|panicked' | head
```

Expected: model tests pass. Sim baseline comparisons (`crates/dash-router-sim/tests/sim.rs`) are expected to FAIL on message counts, since every Want now also carries prefixes and the marker is gone. If anything *else* fails (a panic, coverage below 100%), stop and investigate before touching baselines.

- [ ] **Step 2: Recapture baselines, verified bit-identical**

```bash
cargo run --release --bin sim -- crates/dash-router-sim/scenarios/example.yaml --out sim-baseline
cargo run --release --bin sim -- crates/dash-router-sim/scenarios/example.yaml --out target/sim-verify
diff -r sim-baseline target/sim-verify && echo IDENTICAL
```

Expected: `IDENTICAL`. Then read the headline numbers:

```bash
grep -E 'coverage_rate|t_full_ms_mean|want_msgs|have_msgs' sim-baseline/*.yaml | head -30
git diff --stat sim-baseline
```

Coverage on `lan-20` and `lan-50-lossy` stays 100%; `want_msgs` may rise (prefix Wants are not suppressed by others' prefixes); `t_full` should be within noise of the old baseline. `lan-20-cap-pressure` still shows `eviction_exercised` and `cap_pressure_exercised`. If coverage dropped or `t_full` moved by more than ~20%, stop and report; that is a behavioural regression, not a recapture.

- [ ] **Step 3: Run everything**

```bash
cargo test --workspace 2>&1 | grep -E 'test result|FAILED|panicked' | head -20
```

Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "model, sim: prefix subscriptions; recapture baselines

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: `PeerKey`, `PeerIdentity`, and the sender check

Spec §3.2. The transport can now hand the shell a verified author; the shell drops any message whose claimed sender disagrees.

**Files:**
- Modify: `crates/dash-router/src/transport.rs` (`PeerKey`, `PeerIdentity`, `Incoming.author`)
- Modify: `crates/dash-router/src/panda.rs` (`VerifyingKey` impls; `Incoming { author: None, .. }`)
- Modify: `crates/dash-router/src/shell.rs` (check in `on_wire`; `N: PeerIdentity` bound)
- Modify: `crates/dash-router/src/lib.rs` (export `PeerKey`, `PeerIdentity`)
- Modify: `crates/dash-router/tests/conformance.rs` (`incoming()` sets `author: None`)

**Interfaces:**
- Produces:
  ```rust
  pub struct PeerKey(pub [u8; 32]);   // Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord; From<[u8; 32]>
  pub trait PeerIdentity { fn peer_key(&self) -> Option<PeerKey>; }   // impl for u8,u16,u32,u64,usize → None
  pub struct Incoming { pub remote: Option<IpAddr>, pub author: Option<PeerKey>, pub bytes: Vec<u8> }
  // feature p2panda: impl From<VerifyingKey> for PeerKey; impl TryFrom<PeerKey> for VerifyingKey; impl PeerIdentity for VerifyingKey (Some)
  ```

- [ ] **Step 1: Write the failing shell test**

In `shell.rs` `mod tests`:

```rust
    /// A wire identity with a transport key, for the sender check.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
    struct Keyed(u8);
    impl std::fmt::Display for Keyed {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "k{}", self.0)
        }
    }
    impl crate::transport::PeerIdentity for Keyed {
        fn peer_key(&self) -> Option<crate::transport::PeerKey> {
            Some(crate::transport::PeerKey([self.0; 32]))
        }
    }

    /// Review focus 5 (spec §3.2).
    #[tokio::test]
    async fn sender_must_match_envelope_author() {
        use crate::transport::PeerKey;
        let mut c: NodeCore<Keyed, u8, OpsMap<u8>, OpsMap<u8>, Scripted> = NodeCore::new(
            Keyed(0), config(1 << 20), BTreeSet::from([0u8]), OpsMap::default(), OpsMap::default(), Scripted::ms(&[1000]),
        );
        c.init().await.unwrap();
        let msg = WireMessage::want(Keyed(2), LogRanges::from_pairs([(0u8, Ranges::full())]), BTreeSet::new());
        let forged = Incoming { remote: None, author: Some(PeerKey([3; 32])), bytes: msg.encode() };
        c.on_wire(Duration::from_millis(1), forged).await.unwrap();
        assert_eq!(c.dropped_msgs, 1, "claimed k2, envelope says k3: dropped");
        assert!(c.router.wants.is_empty());

        let genuine = Incoming { remote: None, author: Some(PeerKey([2; 32])), bytes: msg.encode() };
        c.on_wire(Duration::from_millis(2), genuine).await.unwrap();
        assert_eq!(c.dropped_msgs, 1);
        assert!(c.router.wants.contains_key(&Keyed(2)));

        let unverified = Incoming { remote: None, author: None, bytes: msg.encode() };
        c.on_wire(Duration::from_millis(3), unverified).await.unwrap();
        assert_eq!(c.dropped_msgs, 1, "no envelope: no check (loopback / plain gossip)");
    }
```

- [ ] **Step 2: Run to verify it fails**

```bash
cargo test -p dash-router sender_must_match 2>&1 | grep -E '^error' | head -3
```

Expected: `PeerKey`/`PeerIdentity` not found; `Incoming` has no field `author`.

- [ ] **Step 3: Implement**

`transport.rs`:

```rust
/// A transport-level node identity: 32 key bytes, p2panda-free. The
/// p2panda transport fills it from the verified gossip envelope (spec
/// 2026-09-22 §3.2); loopback leaves it `None`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerKey(pub [u8; 32]);

impl From<[u8; 32]> for PeerKey {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

/// How a wire identity `N` maps to a transport identity, if it has one.
/// The shell drops a message whose `sender.peer_key()` disagrees with the
/// transport's verified `Incoming::author`.
pub trait PeerIdentity {
    fn peer_key(&self) -> Option<PeerKey>;
}

macro_rules! no_peer_key {
    ($($t:ty),* $(,)?) => { $(impl PeerIdentity for $t { fn peer_key(&self) -> Option<PeerKey> { None } })* };
}
no_peer_key!(u8, u16, u32, u64, usize);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Incoming {
    pub remote: Option<IpAddr>,
    /// The transport-verified author, when the transport verifies one.
    pub author: Option<PeerKey>,
    pub bytes: Vec<u8>,
}
```

Loopback's `recv` sets `author: None`. `lib.rs`: `pub use transport::{GossipTransport?..}` — for now add `PeerIdentity, PeerKey` to the existing `pub use transport::{..}` line.

`panda.rs` (inside the feature-gated module):

```rust
use crate::transport::{PeerIdentity, PeerKey};

impl From<VerifyingKey> for PeerKey {
    fn from(k: VerifyingKey) -> Self {
        PeerKey(*k.as_bytes())
    }
}

impl TryFrom<PeerKey> for VerifyingKey {
    type Error = anyhow::Error;
    fn try_from(k: PeerKey) -> Result<Self> {
        VerifyingKey::from_bytes(&k.0).map_err(|e| anyhow::anyhow!("invalid peer key: {e}"))
    }
}

impl PeerIdentity for VerifyingKey {
    fn peer_key(&self) -> Option<PeerKey> {
        Some((*self).into())
    }
}
```

(`VerifyingKey::as_bytes` and `from_bytes` are the p2panda-core 0.7.1 names, used in Dash Chat's `queries.rs`; if the compiler disagrees, `cargo doc -p p2panda-core --open` and use the names it shows.) `PandaTransport::recv` sets `author: None` (it has no envelope).

`shell.rs` `on_wire`, right after the own-sender drop:

```rust
        if let Some(author) = inc.author
            && msg.sender.peer_key() != Some(author)
        {
            self.dropped_msgs += 1;
            return Ok(out);
        }
```

Add `+ PeerIdentity` to `N`'s bound on the `NodeCore` impl block and on `spawn`. The conformance test's `incoming()` helper and every `Incoming { .. }` literal in tests gain `author: None`.

- [ ] **Step 4: Run**

```bash
cargo test -p dash-router 2>&1 | grep -E 'test result|FAILED|panicked' | head
cargo check -p dash-router --features p2panda 2>&1 | tail -2
cargo test --workspace 2>&1 | grep -E 'test result|FAILED' | head -20
```

Expected: all pass; the feature build is clean.

- [ ] **Step 5: Commit**

```bash
git add crates/dash-router
git commit -m "shell: PeerKey/PeerIdentity; drop wire messages whose sender contradicts the verified author

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 8: `GossipTransport` over embedder-supplied gossip

Spec §3.1. Lives in `transport.rs` (p2panda-free, so not feature-gated; the spec names `panda.rs`, but nothing in it needs p2panda and a p2panda-free home lets it be unit-tested without the feature).

**Files:**
- Modify: `crates/dash-router/src/transport.rs`
- Modify: `crates/dash-router/src/lib.rs` (export `GossipPublisher, GossipSubscription, GossipTransport`)

**Interfaces:**
- Produces:
  ```rust
  #[trait_variant::make(Send)] pub trait GossipPublisher { async fn publish(&mut self, bytes: Vec<u8>) -> Result<()>; }
  #[trait_variant::make(Send)] pub trait GossipSubscription { async fn next(&mut self) -> Option<(PeerKey, Vec<u8>)>; }
  pub struct GossipTransport<P, S>;  impl GossipTransport<P, S> { pub fn new(publisher: P, subscription: S) -> Self }
  impl<P: GossipPublisher, S: GossipSubscription> Transport for GossipTransport<P, S>
  ```

- [ ] **Step 1: Write the failing test**

In `transport.rs` (next to the existing loopback test):

```rust
#[cfg(test)]
mod gossip_tests {
    use super::*;
    use tokio::sync::mpsc;

    struct ChanPub(mpsc::Sender<Vec<u8>>);
    impl GossipPublisher for ChanPub {
        async fn publish(&mut self, bytes: Vec<u8>) -> Result<()> {
            self.0.send(bytes).await.map_err(|_| anyhow::anyhow!("closed"))
        }
    }
    struct ChanSub(mpsc::Receiver<(PeerKey, Vec<u8>)>);
    impl GossipSubscription for ChanSub {
        async fn next(&mut self) -> Option<(PeerKey, Vec<u8>)> {
            self.0.recv().await
        }
    }

    #[tokio::test]
    async fn gossip_transport_forwards_publishes_and_tags_incoming_with_author() {
        let (pub_tx, mut pub_rx) = mpsc::channel(4);
        let (sub_tx, sub_rx) = mpsc::channel(4);
        let mut t = GossipTransport::new(ChanPub(pub_tx), ChanSub(sub_rx));

        t.broadcast(vec![1, 2, 3]).await.unwrap();
        assert_eq!(pub_rx.recv().await, Some(vec![1, 2, 3]));

        sub_tx.send((PeerKey([9; 32]), vec![4])).await.unwrap();
        assert_eq!(
            t.recv().await,
            Some(Incoming { remote: None, author: Some(PeerKey([9; 32])), bytes: vec![4] })
        );

        drop(sub_tx);
        assert_eq!(t.recv().await, None, "closed subscription = transport shut down");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
cargo test -p dash-router gossip_transport_forwards 2>&1 | grep -E '^error' | head -3
```

Expected: `GossipTransport` not found.

- [ ] **Step 3: Implement**

```rust
/// What an embedder that already runs a gossip overlay hands us: publish
/// bytes on the well-known topic, and a stream of `(verified author,
/// bytes)`. Dash Chat implements these over p2panda's ephemeral stream,
/// whose signed envelope is where `PeerKey` comes from (spec §3.1).
#[trait_variant::make(Send)]
pub trait GossipPublisher {
    async fn publish(&mut self, bytes: Vec<u8>) -> Result<()>;
}

#[trait_variant::make(Send)]
pub trait GossipSubscription {
    /// `None` = the overlay is gone; the transport reports shutdown.
    async fn next(&mut self) -> Option<(PeerKey, Vec<u8>)>;
}

/// A [`Transport`] over an embedder-supplied gossip pair. The overlay's
/// membership is the LAN boundary, so `remote` is always `None`.
pub struct GossipTransport<P, S> {
    publisher: P,
    subscription: S,
}

impl<P, S> GossipTransport<P, S> {
    pub fn new(publisher: P, subscription: S) -> Self {
        Self { publisher, subscription }
    }
}

impl<P: GossipPublisher, S: GossipSubscription> Transport for GossipTransport<P, S> {
    async fn broadcast(&mut self, bytes: Vec<u8>) -> Result<()> {
        self.publisher.publish(bytes).await
    }

    async fn recv(&mut self) -> Option<Incoming> {
        let (author, bytes) = self.subscription.next().await?;
        Some(Incoming { remote: None, author: Some(author), bytes })
    }
}
```

Export from `lib.rs`.

- [ ] **Step 4: Run and commit**

```bash
cargo test -p dash-router transport 2>&1 | tail -3
git add crates/dash-router
git commit -m "shell: GossipTransport over embedder-supplied publisher/subscription

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 9: Size-aware Have and Want packing

Spec §3.3. Gossip rejects messages over its `max_message_size` (4096 by default); Dash Chat's ephemeral envelope adds ~150 bytes on top. The shell packs every broadcast under a byte budget.

**Files:**
- Create: `crates/dash-router/src/pack.rs`
- Modify: `crates/dash-router/src/shell.rs` (`CoreConfig.max_wire_bytes`, `hydrate` → `Vec`, `SendWant` → `pack_want`, `oversize_drops` counter)
- Modify: `crates/dash-router/src/handle.rs` (`StatsSnapshot.oversize_drops`)
- Modify: `crates/dash-router/src/lib.rs` (`pub mod pack;`)
- Modify: every `CoreConfig { .. }` literal: `shell.rs` tests, `tests/loop.rs`, `tests/panda_swarm.rs`, `tests/conformance.rs` (add `max_wire_bytes: 3800`)

**Interfaces:**
- Produces:
  ```rust
  pub const DEFAULT_MAX_WIRE_BYTES: usize = 3800;
  pub fn pack_have<N, L>(sender: N, ops: Vec<(L, Seq, Op)>, budget: usize) -> (Vec<WireMessage<N, L>>, u64)
  pub fn pack_want<N, L>(sender: N, ranges: LogRanges<L>, prefixes: BTreeSet<L::Prefix>, budget: usize) -> (Vec<WireMessage<N, L>>, u64)
  // where N: Id + Serialize + DeserializeOwned, L: WireLog. Second tuple element = items dropped as unpackable.
  CoreConfig { .., pub max_wire_bytes: usize }
  StatsSnapshot { .., pub oversize_drops: u64 }
  ```
  `ops` passed to `pack_have` are already sorted and deduplicated in `(log, seq)` order (the caller, `hydrate`, does that today).

- [ ] **Step 1: Write the failing tests**

Create `crates/dash-router/src/pack.rs` with its tests:

```rust
//! Size-aware packing of wire messages (spec 2026-09-22 §3.3): gossip
//! refuses messages over its `max_message_size`, so every broadcast is
//! split to fit a byte budget the embedder chooses.

#[cfg(test)]
mod tests {
    use super::*;
    use dash_router_core::{Op, Ranges, WireBody};

    fn op(n: usize) -> Op {
        Op { header: vec![7; 40], payload: Some(vec![1; n]) }
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
        assert_eq!(seen, ops.iter().map(|(l, q, _)| (*l, *q)).collect::<Vec<_>>());
    }

    /// Review focus 4.
    #[test]
    fn oversize_op_goes_header_only() {
        let ops = vec![(0u8, 0u32, op(10)), (0u8, 1u32, op(5000)), (0u8, 2u32, op(10))];
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
        let huge = Op { header: vec![1; 2000], payload: None };
        let (msgs, dropped) = pack_have(1u32, vec![(0u8, 0u32, huge)], 1000);
        assert!(msgs.is_empty());
        assert_eq!(dropped, 1);
    }

    #[test]
    fn want_splits_ranges_and_carries_prefixes_first() {
        let ranges = LogRanges::from_pairs((0..200u8).map(|l| (l, Ranges::from(3))));
        let prefixes: BTreeSet<u8> = (0..50).collect();
        let (msgs, dropped) = pack_want(1u32, ranges, prefixes.clone(), 300);
        assert_eq!(dropped, 0);
        assert!(msgs.len() > 1);
        assert!(msgs.iter().all(|m| m.encode().len() <= 300));
        let mut got_prefixes = BTreeSet::new();
        let mut got_logs = 0;
        for m in &msgs {
            if let WireBody::Want { ranges, prefixes } = &m.body {
                got_prefixes.extend(prefixes.iter().copied());
                got_logs += ranges.iter().count();
            }
        }
        assert_eq!(got_prefixes, prefixes);
        assert_eq!(got_logs, 200);
    }

    #[test]
    fn empty_input_packs_to_nothing() {
        let (msgs, _) = pack_have(1u32, Vec::<(u8, Seq, Op)>::new(), 1000);
        assert!(msgs.is_empty());
        let (msgs, _) = pack_want(1u32, LogRanges::<u8>::empty(), BTreeSet::new(), 1000);
        assert!(msgs.is_empty());
    }
}
```

- [ ] **Step 2: Run to verify they fail**

Add `pub mod pack;` to `lib.rs`, then:

```bash
cargo test -p dash-router pack:: 2>&1 | grep -E '^error' | head -3
```

Expected: `pack_have` / `pack_want` not found.

- [ ] **Step 3: Implement**

Above the tests in `pack.rs`:

```rust
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
    WireMessage::have(sender, group_ops(batch.to_vec())).encode().len()
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
            msgs.push(WireMessage::have(sender, group_ops(std::mem::take(&mut batch))));
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

/// Greedily pack a Want: prefixes first (they are tiny and are what an
/// unknown-author subscription rides on), then one log's ranges at a
/// time. A single log whose ranges alone exceed the budget is dropped
/// and counted; the next Want cycle retries with whatever changed.
pub fn pack_want<N, L>(
    sender: N,
    ranges: LogRanges<L>,
    prefixes: BTreeSet<L::Prefix>,
    budget: usize,
) -> (Vec<WireMessage<N, L>>, u64)
where
    N: Id + Serialize + DeserializeOwned,
    L: WireLog,
{
    let mut msgs = Vec::new();
    let mut dropped = 0u64;
    let mut cur_ranges: LogRanges<L> = LogRanges::empty();
    let mut cur_prefixes: BTreeSet<L::Prefix> = BTreeSet::new();
    let len = |r: &LogRanges<L>, p: &BTreeSet<L::Prefix>| {
        WireMessage::want(sender, r.clone(), p.clone()).encode().len()
    };
    let flush = |msgs: &mut Vec<WireMessage<N, L>>, r: &mut LogRanges<L>, p: &mut BTreeSet<L::Prefix>| {
        if !r.is_empty() || !p.is_empty() {
            msgs.push(WireMessage::want(
                sender,
                std::mem::replace(r, LogRanges::empty()),
                std::mem::take(p),
            ));
        }
    };
    for prefix in prefixes {
        cur_prefixes.insert(prefix);
        if len(&cur_ranges, &cur_prefixes) > budget {
            cur_prefixes.remove(&prefix);
            flush(&mut msgs, &mut cur_ranges, &mut cur_prefixes);
            cur_prefixes.insert(prefix);
            if len(&cur_ranges, &cur_prefixes) > budget {
                cur_prefixes.remove(&prefix);
                dropped += 1;
            }
        }
    }
    for (log, r) in ranges.iter() {
        cur_ranges.insert(*log, r.clone());
        if len(&cur_ranges, &cur_prefixes) > budget {
            cur_ranges.remove(log);
            flush(&mut msgs, &mut cur_ranges, &mut cur_prefixes);
            cur_ranges.insert(*log, r.clone());
            if len(&cur_ranges, &cur_prefixes) > budget {
                cur_ranges.remove(log);
                dropped += 1;
            }
        }
    }
    flush(&mut msgs, &mut cur_ranges, &mut cur_prefixes);
    (msgs, dropped)
}
```

`WireBody` is used only by the tests; import it inside `mod tests`, not at module level.

- [ ] **Step 4: Wire it into the shell**

`shell.rs`:
- `CoreConfig` gains `pub max_wire_bytes: usize` (doc: "Every broadcast is packed to encode at or under this many bytes; see `pack::DEFAULT_MAX_WIRE_BYTES`."). `NodeCore` stores it as `max_wire_bytes` and gains `pub oversize_drops: u64`.
- `hydrate` returns `Result<Vec<WireMessage<N, L>>>`: after the sort+dedup, `let (msgs, dropped) = pack_have(self.router.id, ops, self.max_wire_bytes); self.oversize_drops += dropped; Ok(msgs)`.
- `route_fx`: `Effect::SendHave(r) => for msg in self.hydrate(&r, out).await? { out.push(Out::Broadcast(msg)); }` and `Effect::SendWant { ranges, prefixes } => { let (msgs, dropped) = pack_want(self.router.id, ranges, prefixes, self.max_wire_bytes); self.oversize_drops += dropped; out.extend(msgs.into_iter().map(Out::Broadcast)); }`.
- Update the `hydrate` doc comment: it now yields one *or more* Haves; the model's single Have and the shell's split Haves carry the same ops in the same order.
- `Command::Stats` reply adds `oversize_drops: core.oversize_drops`; `StatsSnapshot` gains the field.
- Every `CoreConfig { .. }` literal gains `max_wire_bytes: dash_router::pack::DEFAULT_MAX_WIRE_BYTES` (or `3800` inside `shell.rs` tests).

Add a shell test: with `max_wire_bytes: 600` and three 300-byte-payload ops appended on one log, a received full-range Want yields at least two `Out::Broadcast` Haves each encoding ≤ 600 bytes, together carrying all three ops in order.

- [ ] **Step 5: Run**

```bash
cargo test -p dash-router 2>&1 | grep -E 'test result|FAILED|panicked' | head
cargo test --workspace 2>&1 | grep -E 'test result|FAILED' | head -20
```

Expected: all pass. The conformance test compares state projections, not broadcast counts, so the split is invisible to it; if it compares `Out::Broadcast` lists anywhere, fold split Haves back together before comparing (concatenate their op lists per log) rather than weakening the assertion.

- [ ] **Step 6: Commit**

```bash
git add crates/dash-router
git commit -m "shell: pack Haves and Wants under a byte budget; count oversize drops

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 10: `LogKey for [u8; 64]`

Spec §3.4.

**Files:**
- Modify: `crates/dash-router/src/disk.rs`

- [ ] **Step 1: Write the failing test**

In `disk.rs`'s existing key round-trip test (around line 907), add:

```rust
        let k64 = [0xABu8; 64];
        let mut b = Vec::new();
        k64.write_key(&mut b);
        assert_eq!(b.len(), <[u8; 64] as LogKey>::WIDTH);
        assert_eq!(<[u8; 64] as LogKey>::read_key(&b), Some(k64));
        assert_eq!(<[u8; 64] as LogKey>::read_key(&b[..63]), None);
```

- [ ] **Step 2: Run to verify it fails**

```bash
cargo test -p dash-router disk::tests 2>&1 | grep -E '^error' | head -3
```

Expected: `LogKey` not implemented for `[u8; 64]`.

- [ ] **Step 3: Implement**

Next to the `[u8; 32]` impl:

```rust
/// Two 32-byte halves (Dash Chat: `LogId ++ author`), prefix first so a
/// prefix's logs are one contiguous key range.
impl LogKey for [u8; 64] {
    const WIDTH: usize = 64;

    fn write_key(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self);
    }

    fn read_key(bytes: &[u8]) -> Option<Self> {
        bytes.get(..64)?.try_into().ok()
    }
}
```

- [ ] **Step 4: Run and commit**

```bash
cargo test -p dash-router disk 2>&1 | tail -3
git add crates/dash-router/src/disk.rs
git commit -m "disk: LogKey for [u8; 64]

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 11: Docs and final verification

**Files:**
- Modify: `crates/dash-router/src/panda.rs` (module doc: trust stance now mentions the envelope-verified path via `GossipTransport`)
- Modify: `docs/superpowers/specs/2026-09-21-real-world-shell-design.md` (one line under "Resolved": wire is now v1 with prefix Wants, see the 2026-09-22 spec)
- Modify: `README.md` if it lists crates

- [ ] **Step 1: Doc touch-ups**

Add to the top of `panda.rs`'s module doc a paragraph: "`PandaTransport` is the standalone transport (own p2panda stack, unsigned gossip, `author: None`). An embedder with its own p2panda node uses `GossipTransport` over its ephemeral stream instead, and the shell then checks `WireMessage::sender` against the envelope's verified author."

- [ ] **Step 2: Full verification**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets 2>&1 | grep -E '^(warning|error)' | head
cargo clippy -p dash-router --features p2panda --all-targets 2>&1 | grep -E '^(warning|error)' | head
cargo test --workspace 2>&1 | grep -E 'test result|FAILED' | head -20
```

Expected: fmt clean, no new clippy warnings, all tests pass.

- [ ] **Step 3: Run the ignored p2panda tests once**

These bind real sockets and multicast on the host. Run them; if the machine cannot multicast (some CI boxes), report that and skip, do not mark the task done as if they passed.

```bash
cargo test -p dash-router --features p2panda -- --ignored two_panda_nodes_gossip_on_localhost 2>&1 | tail -5
cargo test -p dash-router --features p2panda --test panda_swarm -- --ignored --nocapture 2>&1 | tail -15
```

Expected: both pass. The swarm's fifty nodes still fully replicate over the sparse overlay with prefix Wants (their logs are `u8`, so each subscription's prefix is the log itself).

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "docs: panda trust stance covers GossipTransport; shell spec notes wire v1

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

Report back with the swarm test's tail output and the baseline headline numbers from Task 6.
