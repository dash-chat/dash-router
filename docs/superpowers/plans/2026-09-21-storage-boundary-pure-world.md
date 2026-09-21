# Storage Boundary (Pure World) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the router storage-less (ranges-only decisions), add the storage traits and machines, the `NodeMachine` glue, and the postcard wire format, and rewire net-model and sim onto the new composition.

**Architecture:** `RouterMachine` becomes a pure gossip brain over `LogRanges` (no op bytes, no subscriptions). Op bytes live in two storage machines (`RelayStoreMachine`, `ExtStoreMachine`) behind a `Storage` trait; a pure `NodeMachine` composes router + stores + subscriptions and implements the routing table from the spec. Net-model and sim drive `NodeMachine` per node; flights carry `WireMessage`s.

**Tech Stack:** Rust 2024, polestar (`Machine`, `StateMachine`, `owned_update`, `transition`), postcard + serde, proptest, existing sim harness.

**Spec:** `docs/superpowers/specs/2026-09-21-storage-and-shell-design.md` — read it first; every task argues from it. The tokio shell (spec §7) is a separate, later plan.

## Global Constraints

- Workspace: `/home/michael/work/dash-router`, edition 2024, resolver 3.
- Test gate per task: **Tasks 1–4 gate on `cargo test -p dash-router-core`** (net-model and sim are knowingly broken from Task 2 until Tasks 5–6 restore them; do not chase their compile errors early). Tasks 5–7 gate on the full `cargo test --workspace`.
- Commit messages end with:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
- Never `use polestar::…::Machine` directly for method resolution: `use polestar::prelude::*` brings the `Machine` trait; `StateMachine` is `use polestar::StateMachine`.
- Inside any `transition`, advance a nested `StateMachine` with the consuming `transition(self, action)` (via `owned_update` or by move), never `step(&mut)` — `step` poisons state on error.
- All collections in machine state are `BTreeMap`/`BTreeSet`/sorted `Vec` (canonical equality/hash). No `HashMap`, no RNG, no clocks in any `Machine`.
- Naming: the Have seen-set field is `relayed_haves` (renamed from `relayed`); there is no `fresh` anywhere after Task 2.

---

### Task 1: Wire format module (`wire.rs`) and serde on core types

**Files:**
- Modify: `Cargo.toml` (workspace root) — add `postcard = { version = "1", features = ["use-std"] }` to `[workspace.dependencies]`.
- Modify: `crates/dash-router-core/Cargo.toml` — add `serde = { workspace = true }`, `postcard = { workspace = true }` to `[dependencies]`.
- Modify: `crates/dash-router-core/src/ranges.rs` — serde derives.
- Modify: `crates/dash-router-core/src/message.rs` — serde derive on `Op` only (the rest of the module dies in Task 2).
- Create: `crates/dash-router-core/src/wire.rs`
- Modify: `crates/dash-router-core/src/lib.rs` — `pub mod wire;` + re-exports.

**Interfaces:**
- Consumes: `Op`, `Seq`, `Ranges`, `LogRanges` (existing).
- Produces: `WireMessage<N, L> { version: u8, sender: N, body: WireBody<L> }`, `WireBody<L>::{Want(LogRanges<L>), Have(Vec<(L, Seq, Op)>)}`, `WireMessage::encode(&self) -> Vec<u8>`, `WireMessage::decode(&[u8]) -> anyhow::Result<Self>`, `pub const WIRE_VERSION: u8 = 0`. Tasks 4–6 build `Broadcast(WireMessage)` effects and flights from these.

- [ ] **Step 1: Add dependencies**

Workspace `Cargo.toml` `[workspace.dependencies]`: add `postcard = { version = "1", features = ["use-std"] }`. Core `Cargo.toml`: add `serde.workspace = true` and `postcard.workspace = true`.

- [ ] **Step 2: Add serde derives to `Ranges`, `LogRanges`, `Op`**

In `ranges.rs`, extend the existing derive lists on `Ranges` and `LogRanges` with `serde::Serialize, serde::Deserialize`. `LogRanges<L: Ord>`'s inner `BTreeMap<L, Ranges>` serializes with `L: Serialize + DeserializeOwned` bounds added by the derive automatically. In `message.rs`, add the same two derives to `Op`.

- [ ] **Step 3: Write the failing round-trip test (in-module)**

Create `wire.rs` with only the test module first:

```rust
//! The versioned LAN broadcast payload. postcard-encoded, matching
//! p2panda's own serialization choice. See spec §6.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Op, ranges::{LogRanges, Ranges}};

    #[test]
    fn wire_messages_round_trip_and_reject_unknown_versions() {
        let want: WireMessage<u32, u8> = WireMessage::want(
            7,
            LogRanges::from_pairs([(1u8, Ranges::from(3))]),
        );
        let have: WireMessage<u32, u8> = WireMessage::have(
            7,
            vec![(1u8, 0, Op { header: vec![9], payload: Some(vec![9, 9]) })],
        );
        for msg in [want, have] {
            let bytes = msg.encode();
            assert_eq!(WireMessage::decode(&bytes).unwrap(), msg);
        }

        let mut bad = WireMessage::<u32, u8>::want(7, LogRanges::empty()).encode();
        bad[0] = WIRE_VERSION + 1; // version is the first postcard field (u8)
        assert!(WireMessage::<u32, u8>::decode(&bad).is_err());
    }
}
```

- [ ] **Step 4: Run to verify failure**

Run: `cargo test -p dash-router-core wire`
Expected: compile error — `WireMessage` not defined.

- [ ] **Step 5: Implement**

```rust
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{Op, ranges::{LogRanges, Seq}};

/// Bump together with the gossip topic on breaking change.
pub const WIRE_VERSION: u8 = 0;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WireMessage<N, L: Ord> {
    pub version: u8,
    /// Gossip strips the transport sender; we carry our own.
    pub sender: N,
    pub body: WireBody<L>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WireBody<L: Ord> {
    Want(LogRanges<L>),
    /// Hydrated ops in (log, seq) order; payloads may be None (GC'd).
    Have(Vec<(L, Seq, Op)>),
}

impl<N: Serialize + DeserializeOwned, L: Ord + Serialize + DeserializeOwned> WireMessage<N, L> {
    pub fn want(sender: N, ranges: LogRanges<L>) -> Self {
        Self { version: WIRE_VERSION, sender, body: WireBody::Want(ranges) }
    }

    pub fn have(sender: N, ops: Vec<(L, Seq, Op)>) -> Self {
        Self { version: WIRE_VERSION, sender, body: WireBody::Have(ops) }
    }

    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("wire types serialize infallibly")
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        let msg: Self = postcard::from_bytes(bytes)?;
        anyhow::ensure!(msg.version == WIRE_VERSION, "unknown wire version {}", msg.version);
        Ok(msg)
    }
}
```

In `lib.rs`: add `pub mod wire;` and `pub use wire::{WIRE_VERSION, WireBody, WireMessage};`.

- [ ] **Step 6: Run to verify pass**

Run: `cargo test -p dash-router-core wire` → PASS. Then `cargo test -p dash-router-core` → everything still green (nothing else touched).

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat(core): postcard wire format module"
```

---

### Task 2: Storage-less router refactor

The biggest task: rewrite `router.rs` per spec §2 and rework its tests. `message.rs` shrinks to just `Op` (rename to `op.rs`). Downstream crates break here — that is expected until Tasks 5–6; the gate is `cargo test -p dash-router-core`.

**Files:**
- Modify: `crates/dash-router-core/src/router.rs` (full rewrite of state/actions/effects/transition; ~40% deletion)
- Create: `crates/dash-router-core/src/op.rs` (move `Op` here; delete `message.rs`)
- Modify: `crates/dash-router-core/src/lib.rs`
- Modify: `crates/dash-router-core/src/wire.rs` (import `Op` from `crate::op`)
- Test: `crates/dash-router-core/tests/router.rs` (rewrite)

**Interfaces:**
- Consumes: `LogRanges`, `Ranges`, `Seq` (existing); polestar `Machine`, `TimeInterval`, `Id`.
- Produces (Tasks 4–6 depend on these exact shapes):

```rust
pub struct RouterConfig<T> { pub want_ttl: T, pub have_ttl: T }   // relay_cap GONE
pub struct RouterState<N: Ord, L: Ord, T> {
    pub id: N,
    pub held: LogRanges<L>,          // empty-range key = known-but-empty log
    pub wants: BTreeMap<N, Record<L, T>>,
    pub haves: BTreeMap<N, Record<L, T>>,
    pub relayed_haves: Vec<Record<L, T>>,
    pub relayed_wants: Vec<Record<L, T>>,
    pub want_timer: Option<Timer<T>>,
    pub have_timer: Option<Timer<T>>,
}
impl RouterState { pub fn new(id: N, held: LogRanges<L>) -> Self }
pub enum RouterAction<N, L: Ord, T> {
    Tick(T), ArmWantTimer(T), ArmHaveTimer(T), FireWant, FireHave,
    RecvWant { from: N, ranges: LogRanges<L> },
    RecvHave { from: N, ranges: LogRanges<L> },
    Held(LogRanges<L>),
    Push(LogRanges<L>),
}
pub enum Effect<L: Ord> { SendWant(LogRanges<L>), SendHave(LogRanges<L>), Accept(LogRanges<L>) }
```

- [ ] **Step 1: Move `Op` to `op.rs`, delete `message.rs`**

Create `op.rs` containing the `Op` struct verbatim from `message.rs` (with its serde derives from Task 1). Delete `message.rs` (`Message`, `MessageEnvelope`, `HaveOps`, `have_ops_ranges` all die — nothing replaces them; the wire module already covers transport). `lib.rs` becomes:

```rust
pub mod op;
pub mod ranges;
pub mod router;
pub mod wire;

pub use op::Op;
pub use ranges::{LogRanges, Ranges, Seq};
pub use router::{Effect, RouterAction, RouterConfig, RouterMachine, RouterState};
pub use wire::{WIRE_VERSION, WireBody, WireMessage};
```

Fix `wire.rs`'s import to `crate::op::Op`. Core will not compile yet (router still references message types) — proceed.

- [ ] **Step 2: Rewrite `router.rs`**

Keep unchanged (modulo the `relayed` → `relayed_haves` rename): `Timer`, `Record`, `combined_ranges`, `RouterMachine`, `RouterStateMachine`, the `Tick`/`ArmWantTimer`/`ArmHaveTimer` transition arms, and the module doc's timer paragraphs. Delete: `subscriptions`, `store`, `known_logs`, `ops_in`, `unrelayed`, `holds`, `is_relayed`, `relay_usage`, `gc`, the `Append` and `Recv` arms, `relay_cap`. New/changed pieces in full:

```rust
impl<N: Id, L: Id, T: TimeInterval> RouterState<N, L, T> {
    pub fn new(id: N, held: LogRanges<L>) -> Self {
        Self {
            id,
            held,
            wants: BTreeMap::new(),
            haves: BTreeMap::new(),
            relayed_haves: Vec::new(),
            relayed_wants: Vec::new(),
            want_timer: None,
            have_timer: None,
        }
    }

    /// Everything not held, for every known log: the gaps plus the open
    /// tail. A known-but-empty log (empty range in `held`) wants everything.
    pub fn wanted(&self) -> LogRanges<L> {
        LogRanges::from_pairs(self.held.iter().map(|(log, r)| (*log, r.complement())))
    }

    pub fn others_wants(&self) -> LogRanges<L> { /* unchanged body */ }
    pub fn recent_haves(&self) -> LogRanges<L> { /* unchanged body */ }

    /// DESIGN.md §1.
    pub fn next_want(&self) -> LogRanges<L> {
        self.wanted().difference(&self.others_wants())
    }

    /// DESIGN.md §3 — now ranges, not ops; hydration happens above.
    pub fn next_have(&self) -> LogRanges<L> {
        self.held
            .intersection(&self.others_wants())
            .difference(&self.recent_haves())
    }

    pub fn relayed_have_ranges(&self) -> LogRanges<L> { combined_ranges(&self.relayed_haves) }
    pub fn relayed_want_ranges(&self) -> LogRanges<L> { combined_ranges(&self.relayed_wants) }

    fn note_relayed_haves(&mut self, ranges: LogRanges<L>, ttl: T) {
        self.relayed_haves.push(Record { ranges, ttl_left: ttl });
    }
    fn note_relayed_wants(&mut self, ranges: LogRanges<L>, ttl: T) {
        self.relayed_wants.push(Record { ranges, ttl_left: ttl });
    }

    /// Record an own or received Have emission for §3 suppression.
    fn note_have(&mut self, key: N, ranges: LogRanges<L>, ttl: T) {
        self.haves.insert(key, Record { ranges, ttl_left: ttl });
    }
}
```

New transition arms (FireWant is unchanged except `Effect::SendWant(ranges)` replaces the envelope; FireHave analogous):

```rust
RouterAction::FireHave => {
    let Some(timer) = &s.have_timer else { bail!("have timer not armed") };
    ensure!(timer.remaining.is_zero(), "have timer not due");
    s.have_timer = None;
    let ranges = s.next_have();
    if !ranges.is_empty() {
        s.note_have(s.id, ranges.clone(), self.config.have_ttl);
        s.note_relayed_haves(ranges.clone(), self.config.have_ttl);
        fx.push(Effect::SendHave(ranges));
    }
}

RouterAction::Push(ranges) => {
    ensure!(!ranges.is_empty(), "empty push");
    // The author certainly holds what it pushes.
    s.held = s.held.union(&ranges);
    let relay = ranges.difference(&s.relayed_have_ranges());
    if !relay.is_empty() {
        s.note_have(s.id, relay.clone(), self.config.have_ttl);
        s.note_relayed_haves(relay.clone(), self.config.have_ttl);
        fx.push(Effect::SendHave(relay));
    }
}

RouterAction::Held(ranges) => {
    // Absolute snapshot from the storage layer; replaces wholesale.
    s.held = ranges;
}

RouterAction::RecvWant { from, ranges } => {
    ensure!(from != s.id, "received own message");
    let relay = ranges.difference(&s.relayed_want_ranges());
    if !relay.is_empty() {
        s.note_relayed_wants(relay.clone(), self.config.want_ttl);
        fx.push(Effect::SendWant(relay));
    }
    s.wants.insert(from, Record { ranges, ttl_left: self.config.want_ttl });
}

RouterAction::RecvHave { from, ranges } => {
    ensure!(from != s.id, "received own message");
    let novel = ranges.difference(&s.held);
    if !novel.is_empty() {
        s.held = s.held.union(&novel);
        fx.push(Effect::Accept(novel));      // Accept BEFORE SendHave (spec §2.3)
    }
    let relay = ranges.difference(&s.relayed_have_ranges());
    if !relay.is_empty() {
        s.note_relayed_haves(relay.clone(), self.config.have_ttl);
        fx.push(Effect::SendHave(relay));
    }
    s.note_have(from, ranges, self.config.have_ttl);
}
```

Update the module doc: drop the GC and Append bullets from "deliberate simplifications"; add one line that hydration and storage live above the router (pointer to spec §5).

- [ ] **Step 3: Rewrite `tests/router.rs`**

Current file has 9 tests. Their fates, with `use dash_router_core::{Effect, LogRanges, Ranges, RouterAction as A, RouterConfig, RouterMachine, RouterState}` and the existing `FiniteTime` type aliases kept:

Helper rework (ops are gone — everything is ranges):

```rust
fn lr(pairs: impl IntoIterator<Item = (usize, Ranges)>) -> LogRanges<L> {
    LogRanges::from_pairs(pairs.into_iter().map(|(l, r)| (UpTo::new(l), r)))
}
fn sends_have(fx: &[Effect<L>]) -> Vec<&LogRanges<L>> {
    fx.iter().filter_map(|e| match e { Effect::SendHave(r) => Some(r), _ => None }).collect()
}
fn sends_want(fx: &[Effect<L>]) -> Vec<&LogRanges<L>> { /* same for SendWant */ }
fn accepts(fx: &[Effect<L>]) -> Vec<&LogRanges<L>> { /* same for Accept */ }
```

New/adapted tests (write each in full during implementation; the essential shape):

1. `a_want_floods_once_per_hop_and_never_echoes` — adapt the existing flooding test: drive B with `A::RecvWant { from: a, ranges: lr([(0, Ranges::full())]) }`; assert one `SendWant` with the full range; repeat the same action → no `SendWant` (seen-set); after `Tick` past `want_ttl`, it relays again.
2. `a_have_floods_once_and_accepts_only_novelty` — `RecvHave` with ranges `0..2` on log 0: fx == `[Accept(0..2), SendHave(0..2)]` in that order; second identical `RecvHave` from another peer: no Accept (held), no SendHave (seen-set).
3. `seen_set_records_expire_independently` — port the existing test verbatim, replacing op-carrying Haves with `RecvHave` ranges; same tick pattern, same assertion (fails under a top-off design).
4. `push_grows_held_suppresses_echo_and_floods` — fresh state with `held = lr([(0, Ranges::empty())])`; `Push(lr([(0, Ranges::range(0,1))]))` → one `SendHave`; immediately `RecvHave` of the same range from a peer → no `SendHave` (own emission noted), no `Accept` (held grew); a second `Push` of the same range → no fx.
5. `a_held_snapshot_shrink_reopens_the_want` — held `0..3` on log 0, `Held(lr([(0, Ranges::range(0,1))]))`, then arm+fire want: the `SendWant` includes `1..` (complement of what's held now).
6. `an_empty_held_key_wants_the_whole_log` — `RouterState::new(id, lr([(0, Ranges::empty())]))`, arm+fire want → `SendWant` contains `Ranges::full()` for log 0.
7. `want_then_have_backfills_a_late_subscriber` — adapt: node holds `0..2`; `RecvWant` full from peer; `ArmHaveTimer(t(0))`; `FireHave` → `SendHave(0..2)`; and the emission suppresses an immediate repeat (`next_have` empty because own have recorded).
8. Existing timer-discipline tests (`Tick` can't pass a due timer, arm-twice disabled, fire-when-not-due disabled) — keep, they compile with trivial constructor updates.
9. Drop entirely: any test about `store` contents, GC, `Append` seq assignment, `Deliver`/`Store` effects, freshness (`every_have_is_relayed_once_regardless_of_freshness_or_novelty` collapses into test 2).

- [ ] **Step 4: Run core tests**

Run: `cargo test -p dash-router-core`
Expected: PASS (router unit + integration + wire). `cargo test --workspace` WILL fail in net-model/sim — expected; do not fix here.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(core)!: storage-less router — ranges-only decisions, Push, Held snapshots"
```

---

### Task 3: Storage traits, `OpsMap`, and the two storage machines

**Files:**
- Create: `crates/dash-router-core/src/storage.rs`
- Modify: `crates/dash-router-core/src/lib.rs` (`pub mod storage;` + re-exports)

**Interfaces:**
- Consumes: `Op`, `Seq`, `Ranges`, `LogRanges`.
- Produces (Task 4 depends on exact shapes):

```rust
pub type Units = u64;
pub trait Storage<L: Ord> {
    fn held_of(&self, logs: &BTreeSet<L>) -> LogRanges<L>;
    fn held_all(&self) -> LogRanges<L>;
    fn fetch(&self, ranges: &LogRanges<L>) -> Vec<(L, Seq, Op)>;
    fn ingest(&mut self, log: L, seq: Seq, op: Op);
}
pub trait EvictableStorage<L: Ord>: Storage<L> {
    fn usage(&self) -> Units;
    fn held_payloads(&self) -> LogRanges<L>;
    fn evict_payloads(&mut self, ranges: &LogRanges<L>);
    fn evict(&mut self, ranges: &LogRanges<L>);
}
pub struct OpsMap<L: Ord>(BTreeMap<L, BTreeMap<Seq, Op>>);   // impls both traits
pub struct RelayStoreMachine<L> { pub cap: Units, /* PhantomData */ }
pub struct RelayStoreState<L: Ord>(pub OpsMap<L>);
pub enum RelayStoreAction<L: Ord> { Ingest(L, Seq, Op), EvictPayloads(LogRanges<L>), Evict(LogRanges<L>) }
pub struct ExtStoreMachine<L> { /* PhantomData */ }
pub struct ExtStoreState<L: Ord>(pub OpsMap<L>);
pub enum ExtStoreAction<L: Ord> { Ingest(L, Seq, Op), NativeSync(L, Seq, Op), AppGc(LogRanges<L>) }
pub enum StoreEffect<L: Ord> { HeldChanged(LogRanges<L>) }   // Fx = Vec<StoreEffect<L>>
```

- [ ] **Step 1: Write failing in-module tests**

```rust
#[test]
fn ingest_is_idempotent_and_upgrades_headers() {
    let mut m = OpsMap::<u8>::default();
    let header_only = Op { header: vec![1], payload: None };
    let full = Op { header: vec![1], payload: Some(vec![2]) };
    m.ingest(0, 0, header_only.clone());
    m.ingest(0, 0, header_only.clone());          // no-op
    assert_eq!(m.fetch(&m.held_all()).len(), 1);
    m.ingest(0, 0, full.clone());                 // payload upgrade
    assert_eq!(m.fetch(&m.held_all())[0].2, full);
    m.ingest(0, 0, header_only);                  // never downgrades
    assert_eq!(m.fetch(&m.held_all())[0].2, full);
}

#[test]
fn held_of_mirrors_the_request_with_empty_ranges() {
    let mut m = OpsMap::<u8>::default();
    m.ingest(0, 0, Op::default());
    let held = m.held_of(&BTreeSet::from([0, 5]));
    assert_eq!(held.get(&0).unwrap(), &Ranges::from_seqs([0]));
    assert!(held.get(&5).unwrap().is_empty(), "requested-but-absent log appears empty");
}

#[test]
fn usage_counts_units_and_eviction_shrinks_them() {
    let mut m = OpsMap::<u8>::default();
    m.ingest(0, 0, Op { header: vec![1], payload: Some(vec![2]) }); // 2 units
    m.ingest(0, 1, Op { header: vec![1], payload: None });          // 1 unit
    assert_eq!(m.usage(), 3);
    m.evict_payloads(&m.held_all());
    assert_eq!(m.usage(), 2);
    assert!(m.held_payloads().is_empty());
    assert_eq!(m.held_all().get(&0).unwrap(), &Ranges::from_seqs([0, 1]), "headers survive");
    m.evict(&m.held_all());
    assert_eq!(m.usage(), 0);
    assert!(m.held_all().is_empty());
}

#[test]
fn relay_machine_refuses_ingest_over_cap_and_emits_held_changed() {
    let m = RelayStoreMachine::<u8>::new(2);
    let payload_op = Op { header: vec![1], payload: Some(vec![2]) };
    let (s, fx) = m.transition(RelayStoreState::default(),
        RelayStoreAction::Ingest(0, 0, payload_op.clone())).unwrap();
    assert_eq!(fx, vec![StoreEffect::HeldChanged(s.0.held_all())]);
    assert!(m.transition(s.clone(), RelayStoreAction::Ingest(0, 1, payload_op)).is_err(),
        "would exceed cap: not enabled");
    // Re-ingesting a held op is a no-op and emits nothing.
    let held = s.0.held_all();
    let (s2, fx2) = m.transition(s, RelayStoreAction::Ingest(0, 0,
        Op { header: vec![1], payload: Some(vec![2]) })).unwrap();
    assert_eq!(s2.0.held_all(), held);
    assert!(fx2.is_empty());
}

#[test]
fn ext_machine_native_sync_and_app_gc_report_held_changes() {
    let m = ExtStoreMachine::<u8>::default();
    let (s, fx) = m.transition(ExtStoreState::default(),
        ExtStoreAction::NativeSync(0, 0, Op::default())).unwrap();
    assert!(matches!(fx[0], StoreEffect::HeldChanged(_)));
    let (s, fx) = m.transition(s, ExtStoreAction::AppGc(
        LogRanges::from_pairs([(0, Ranges::full())]))).unwrap();
    assert!(s.0.held_all().is_empty());
    assert_eq!(fx, vec![StoreEffect::HeldChanged(LogRanges::empty())]);
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test -p dash-router-core storage` → compile errors.

- [ ] **Step 3: Implement**

`OpsMap` implementation notes (write the whole module in one pass):
- `ingest`: `let slot = self.0.entry(log).or_default().entry(seq)`; insert if vacant; if occupied and `op.payload.is_some()` and existing payload is `None`, replace. Nothing else.
- `held_all`: `LogRanges::from_pairs(self.0.iter().map(|(l, ops)| (*l, Ranges::from_seqs(ops.keys().copied()))))`; drop logs whose op map is empty (evict removes empty maps).
- `held_of(logs)`: for each requested log, its ranges or `Ranges::empty()` — presence mirrors the request.
- `fetch(ranges)`: iterate `ranges.iter()`, for each log walk the op map filtering `r.contains(seq)` (closed ranges only in practice; iterating the *stored* seqs and filtering by containment avoids open-range iteration entirely).
- `usage`: sum `2`/`1` per op by payload presence.
- `evict_payloads(ranges)`: set matching payloads to `None`. `evict(ranges)`: remove matching seqs, drop empty log entries.
- Both machines: `Machine` impls with `Error = anyhow::Error`, `Fx = Vec<StoreEffect<L>>`. `RelayStoreMachine::Ingest` computes the would-be usage delta first (`+2/+1` if vacant, `+1` if upgrade) and `ensure!(self.cap >= s.0.usage() + delta, "relay store at cap")`. Emit `HeldChanged(s.0.held_all())` only when `held_all()` actually changed (payload upgrades and re-ingests change nothing and emit nothing; `EvictPayloads` likewise emits nothing — headers still held). `L: Id` bound throughout.
- Derives on all states/actions/effects: `Clone, Debug, PartialEq, Eq, Hash` (+ `Default` on states/`OpsMap`).

`lib.rs`: `pub mod storage;` + `pub use storage::{EvictableStorage, ExtStoreAction, ExtStoreMachine, ExtStoreState, OpsMap, RelayStoreAction, RelayStoreMachine, RelayStoreState, Storage, StoreEffect, Units};`

- [ ] **Step 4: Run to verify pass** — `cargo test -p dash-router-core` → PASS.

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat(core): Storage traits, OpsMap, relay and external store machines"`

---

### Task 4: `NodeMachine` — the glue routing table

**Files:**
- Create: `crates/dash-router-core/src/node.rs`
- Modify: `crates/dash-router-core/src/lib.rs`
- Test: `crates/dash-router-core/tests/node.rs`

**Interfaces:**
- Consumes: everything from Tasks 1–3 plus `StateMachine`, `owned_update` patterns.
- Produces (Tasks 5–6 depend on these):

```rust
pub struct NodeMachine<N, L, T> {
    pub router: RouterMachine<N, L, T>,
    pub relay: RelayStoreMachine<L>,
    pub ext: ExtStoreMachine<L>,
}
impl NodeMachine { pub fn new(config: RouterConfig<T>, relay_cap: Units) -> Self }
pub struct NodeState<N: Ord, L: Ord, T> {
    pub router: RouterState<N, L, T>,
    pub relay: RelayStoreState<L>,
    pub ext: ExtStoreState<L>,
    pub subscriptions: BTreeSet<L>,
}
impl NodeState {
    /// held snapshot = relay ∪ ext ∪ empty keys for subscriptions
    pub fn held_union(&self) -> LogRanges<L>;
    pub fn new(id: N, subscriptions: impl IntoIterator<Item = L>) -> Self;  // router starts with held_union()
}
pub enum NodeAction<N, L: Ord, T> {
    Router(RouterAction<N, L, T>),        // RecvWant/RecvHave NOT enabled here
    Recv(WireMessage<N, L>),              // the only receive path (bytes park here)
    Authored(L, Seq, Op),
    Subscribe(L),
    Unsubscribe(L),
    NativeSync(L, Seq, Op),
    AppGc(LogRanges<L>),
    RelayEvict(LogRanges<L>),             // nondeterministic in model; policy in sim
}
pub enum NodeEffect<N, L: Ord> {
    Broadcast(WireMessage<N, L>),
    Deliver(L, Seq),
}
```

Note the state fields are plain states, not nested `StateMachine`s — the `NodeMachine` holds the three machine configs itself and calls their `transition`s directly; this avoids triple-wrapped `Arc`s and keeps `NodeState` a plain value. (Deviation from the spec's sketch, same semantics; note it in the module doc.)

- [ ] **Step 1: Write failing tests** (`tests/node.rs`, `UpTo`/`FiniteTime` aliases as in `tests/router.rs`)

```rust
/// Receiving a Have: bytes routed by subscription, novel subscribed ops
/// delivered, the flood re-broadcast fully hydrated.
#[test]
fn recv_have_routes_bytes_delivers_and_rebroadcasts_hydrated() {
    let m = machine();                            // want_ttl/have_ttl = t(2), cap = 100
    let s = NodeState::new(n(0), [l(0)]);         // subscribed to log 0 only
    let op0 = Op { header: vec![7], payload: Some(vec![7]) };
    let op1 = Op { header: vec![8], payload: Some(vec![8]) };
    let wire = WireMessage::have(n(1), vec![(l(0), 0, op0.clone()), (l(1), 0, op1.clone())]);
    let (s, fx) = m.transition(s, NodeAction::Recv(wire)).unwrap();

    assert_eq!(s.ext.0.held_all(), lr([(0, Ranges::from_seqs([0]))]), "subscribed op → ext");
    assert_eq!(s.relay.0.held_all(), lr([(1, Ranges::from_seqs([0]))]), "unsubscribed op → relay");
    assert!(fx.contains(&NodeEffect::Deliver(l(0), 0)));
    assert!(!fx.contains(&NodeEffect::Deliver(l(1), 0)), "relay ops are not delivered");
    let broadcast = fx.iter().find_map(|e| match e {
        NodeEffect::Broadcast(w) => Some(w), _ => None }).unwrap();
    assert_eq!(broadcast.sender, n(0), "re-signed");
    assert_eq!(broadcast.body, WireBody::Have(vec![(l(0), 0, op0), (l(1), 0, op1)]),
        "relay hydrated from the just-ingested bytes");
    assert_eq!(s.router.held, s.held_union(), "router view reconciled");
}

/// Authoring: ingest to ext, Push, hydrated broadcast; duplicate delivery
/// of the same op later teaches nothing.
#[test]
fn authored_ops_push_and_the_echo_is_absorbed() {
    let m = machine();
    let s = NodeState::new(n(0), [l(0)]);
    let op = Op { header: vec![7], payload: Some(vec![7]) };
    let (s, fx) = m.transition(s, NodeAction::Authored(l(0), 0, op.clone())).unwrap();
    assert_eq!(s.ext.0.held_all(), lr([(0, Ranges::from_seqs([0]))]));
    assert!(matches!(&fx[..], [NodeEffect::Broadcast(w)]
        if w.body == WireBody::Have(vec![(l(0), 0, op.clone())])));
    // The flood comes back from a neighbour: no re-broadcast, no delivery.
    let (_, fx) = m.transition(s, NodeAction::Recv(
        WireMessage::have(n(1), vec![(l(0), 0, op)]))).unwrap();
    assert!(fx.is_empty());
}

/// Subscribe migrates relay bytes to ext without changing held.
#[test]
fn subscribe_migrates_and_preserves_held() {
    let m = machine();
    let s = NodeState::new(n(0), [l(0)]);
    let op = Op { header: vec![7], payload: Some(vec![7]) };
    let (s, _) = m.transition(s, NodeAction::Recv(
        WireMessage::have(n(1), vec![(l(1), 0, op.clone())]))).unwrap();
    let held_before = s.router.held.clone();
    let (s, fx) = m.transition(s, NodeAction::Subscribe(l(1))).unwrap();
    assert!(s.relay.0.held_all().is_empty(), "relay side emptied");
    assert_eq!(s.ext.0.fetch(&s.ext.0.held_all()), vec![(l(1), 0, op)], "bytes moved");
    assert_eq!(s.router.held, held_before, "spec §5: migration never changes held");
    assert!(fx.is_empty(), "no traffic from a subscription change");
}

/// The relay sheds ingests at cap; eviction reopens room and shrinks held.
#[test]
fn relay_cap_sheds_then_eviction_reopens() {
    let m = tiny(2);                              // cap = 2 units
    let s = NodeState::new(n(0), []);             // pure relay
    let full = |b: u8| Op { header: vec![b], payload: Some(vec![b]) };
    let (s, fx) = m.transition(s, NodeAction::Recv(
        WireMessage::have(n(1), vec![(l(0), 0, full(1)), (l(0), 1, full(2))]))).unwrap();
    assert_eq!(s.relay.0.held_all(), lr([(0, Ranges::from_seqs([0]))]), "second op shed");
    // The Have still floods in full: relaying is not conditioned on storing.
    assert!(matches!(fx.last().unwrap(), NodeEffect::Broadcast(_)));
    let (s, _) = m.transition(s, NodeAction::RelayEvict(lr([(0, Ranges::from_seqs([0]))]))).unwrap();
    assert!(s.relay.0.held_all().is_empty());
    assert_eq!(s.router.held, s.held_union(), "shrink reconciled");
}

/// Native sync and AppGc reach the router as snapshots; direct router
/// Recv actions are the network's business and not enabled here.
#[test]
fn ext_spontaneity_reconciles_and_smuggled_recvs_are_disabled() {
    let m = machine();
    let s = NodeState::new(n(0), [l(0)]);
    let (s, _) = m.transition(s, NodeAction::NativeSync(l(0), 0, Op::default())).unwrap();
    assert!(s.router.held.contains(&l(0), 0));
    let (s, _) = m.transition(s, NodeAction::AppGc(lr([(0, Ranges::full())]))).unwrap();
    assert!(!s.router.held.contains(&l(0), 0));
    assert!(m.transition(s, NodeAction::Router(
        RouterAction::RecvWant { from: n(1), ranges: lr([(0, Ranges::full())]) })).is_err());
}
```

Helpers `machine()`, `tiny(cap)`, `n`, `l`, `lr` as in the router tests.

- [ ] **Step 2: Run to verify failure** — `cargo test -p dash-router-core --test node` → compile error.

- [ ] **Step 3: Implement `node.rs`**

Structure of `transition` (each arm builds on private helpers; write them in this shape):

```rust
fn transition(&self, mut s: NodeState<..>, action: NodeAction<..>) -> TransitionResult<Self> {
    let mut out = vec![];
    match action {
        NodeAction::Router(a) => {
            ensure!(!matches!(a, RouterAction::RecvWant { .. } | RouterAction::RecvHave { .. }),
                "receiving is the node's business: use NodeAction::Recv");
            let fx = self.router_step(&mut s, a)?;
            self.route_router_fx(&mut s, fx, &BTreeMap::new(), &mut out)?;
        }
        NodeAction::Recv(wire) => match wire.body {
            WireBody::Want(ranges) => {
                let fx = self.router_step(&mut s, RouterAction::RecvWant { from: wire.sender, ranges })?;
                self.route_router_fx(&mut s, fx, &BTreeMap::new(), &mut out)?;
            }
            WireBody::Have(ops) => {
                let parked: BTreeMap<(L, Seq), Op> =
                    ops.iter().cloned().map(|(l, q, o)| ((l, q), o)).collect();
                let ranges = ranges_of(&parked);
                let fx = self.router_step(&mut s, RouterAction::RecvHave { from: wire.sender, ranges })?;
                // Ingest ALL parked bytes first (idempotent; payload upgrades
                // are invisible to the router's novelty check — spec §5).
                self.ingest_parked(&mut s, &parked)?;
                self.reconcile_held(&mut s)?;
                self.route_router_fx(&mut s, fx, &parked, &mut out)?;
            }
        },
        NodeAction::Authored(log, seq, op) => {
            self.ext_step(&mut s, ExtStoreAction::Ingest(log, seq, op))?;
            self.reconcile_held(&mut s)?;
            let ranges = LogRanges::from_pairs([(log, Ranges::from_seqs([seq]))]);
            let fx = self.router_step(&mut s, RouterAction::Push(ranges))?;
            self.route_router_fx(&mut s, fx, &BTreeMap::new(), &mut out)?;
        }
        NodeAction::Subscribe(log) => {
            s.subscriptions.insert(log);
            let all = LogRanges::from_pairs([(log, Ranges::full())]);
            for (l, q, o) in s.relay.0.fetch(&all) {
                self.ext_step(&mut s, ExtStoreAction::Ingest(l, q, o))?;
            }
            self.relay_step(&mut s, RelayStoreAction::Evict(all))?;
            self.reconcile_held(&mut s)?;
        }
        NodeAction::Unsubscribe(log) => { s.subscriptions.remove(&log); self.reconcile_held(&mut s)?; }
        NodeAction::NativeSync(l, q, o) => { self.ext_step(&mut s, ExtStoreAction::NativeSync(l, q, o))?; self.reconcile_held(&mut s)?; }
        NodeAction::AppGc(r) => { self.ext_step(&mut s, ExtStoreAction::AppGc(r))?; self.reconcile_held(&mut s)?; }
        NodeAction::RelayEvict(r) => { self.relay_step(&mut s, RelayStoreAction::Evict(r))?; self.reconcile_held(&mut s)?; }
    }
    Ok((s, out))
}
```

Helper semantics:
- `router_step`/`relay_step`/`ext_step`: take the sub-state by `std::mem::take` (states are `Default`) or clone, call the sub-machine's `transition`, write back, return fx. On error, propagate (the whole node action is then not-enabled; polestar's `transition` contract discards the state).
- `ingest_parked`: for each `((log, seq), op)`: if `s.subscriptions.contains(&log)` → `ext_step(Ingest)`; else → check `s.relay.0.usage()` headroom for the op's units first and **skip (shed) silently** when it would exceed the cap (spec §5: shed until eviction frees room), otherwise `relay_step(Ingest)`.
- `reconcile_held`: `let snap = s.held_union(); router_step(&mut s, RouterAction::Held(snap))` — cheap and unconditional after any storage change; storage-machine `HeldChanged` fx are consumed by this call (drop them — the snapshot supersedes deltas).
- `route_router_fx(s, fx, parked, out)`: in fx order —
  - `Accept(novel)`: for each `(log, r)` in `novel.iter()` with `s.subscriptions.contains(log)`: for each seq in the *parked keys* filtered by `r.contains(seq)` (never iterate `Ranges` directly — open ranges), push `NodeEffect::Deliver(log, seq)`.
  - `SendWant(r)`: `out.push(NodeEffect::Broadcast(WireMessage::want(s.router.id, r)))`.
  - `SendHave(r)`: hydrate `let mut ops = s.relay.0.fetch(&r); ops.extend(s.ext.0.fetch(&r)); ops.sort(); ops.dedup_by(|a, b| (a.0, a.1) == (b.0, b.1));` then `out.push(NodeEffect::Broadcast(WireMessage::have(s.router.id, ops)))`. If hydration comes back empty (everything evicted since), broadcast nothing.
- `held_union`: `relay.held_all() ∪ ext.held_all()` then `for l in &subscriptions { if absent, insert (l, Ranges::empty()) }`.
- `NodeState::new(id, subs)`: build subscriptions, empty stores, `RouterState::new(id, held_union_of_that)`.

Derives: `Clone, Debug, PartialEq, Eq, Hash` on machine/state/action/effect. `lib.rs`: `pub mod node;` + `pub use node::{NodeAction, NodeEffect, NodeMachine, NodeState};`

- [ ] **Step 4: Run to verify pass** — `cargo test -p dash-router-core` → PASS.

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat(core): NodeMachine glue — routing table over router + stores"`

---

### Task 5: net-model on `NodeMachine`

**Files:**
- Modify: `crates/dash-router-net-model/src/net.rs`
- Modify: `crates/dash-router-net-model/tests/net.rs`
- (Leave `topology.rs`, `fair.rs` untouched.)

**Interfaces:**
- Consumes: `NodeMachine`, `NodeState`, `NodeAction`, `NodeEffect`, `WireMessage`.
- Produces: `NetMachine<N, L, T, K>` with `NetState { nodes: BTreeMap<N, NodeState<N, L, T>>, inflight: Vec<Flight<N, L>> }`, `Flight { to: N, message: WireMessage<N, L> }`, `NetAction::{Node(N, NodeAction), Deliver(UpTo<K>), Drop(UpTo<K>), Duplicate(UpTo<K>)}`, net-level fx `Vec<(N, NodeEffect<N, L>)>`.

- [ ] **Step 1: Update `net.rs`**

Mechanical shape changes, keeping the current structure:
- `NetMachine` now holds `topology` plus a `NodeMachine<N, L, T>` (one shared config for all nodes): `NetMachine::new(topology, node_machine)`.
- `NetState.nodes: BTreeMap<N, NodeState<N, L, T>>` (plain states — the machine lives in `NetMachine`). `NetState::new(nodes: impl IntoIterator<Item = NodeState<..>>)` keys by `state.router.id`.
- `apply`: `owned_update(id, |_, node_state| self.node_machine.transition(node_state, action))`, then `absorb_fx`: `NodeEffect::Broadcast(wire)` → one `Flight { to: neighbor, message: wire.clone() }` per topology neighbor of `id` (`ensure!(inflight.len() < K)` per push, as today); `NodeEffect::Deliver(..)` → bubble as `(id, effect)`.
- `Deliver(i)`: pop flight `i`, `apply(flight.to, NodeAction::Recv(flight.message))`.
- Accessors: `node(&self, id) -> &NodeState` and the existing `node_mut` counterpart.

- [ ] **Step 2: Update `tests/net.rs`**

The existing 7 tests survive with translated vocabulary:
- `routers(count)` helper becomes `nodes(count)`: one shared `NodeMachine::new(router_config(), CAP)` in `machine()`; states via `NodeState::new(n(id), [l(0)])`.
- `R::Append(l(0), op(7))` → `NodeAction::Authored(l(0), 0, op(7))` (seq now explicit — the tests always author seq 0, or count upward where a test authors repeatedly).
- `R::Tick`/`ArmWantTimer`/`FireWant`/`FireHave` → wrapped as `NodeAction::Router(RouterAction::…)`.
- `holders()` reads `node.ext.0.held_all().contains(&log, seq) || node.relay.0.held_all().contains(&log, seq)` — add a `NodeState::holds(&self, log, seq) -> bool` convenience in core if the expression grates.
- The smuggled-recv test now asserts `NodeAction::Router(RouterAction::RecvWant{..})` is disabled (mirrors the node test) and `NetAction::Deliver` on empty inflight stays disabled.
- Flight-count assertions: re-derive each number by hand from the new semantics before editing the assertion — Want relays and Have floods have the same counts as before; the backfill test's `assert_eq!(net.inflight.len(), 2/4)` logic is unchanged in spirit.

- [ ] **Step 3: Run** — `cargo test -p dash-router-net-model` → PASS. (`cargo test --workspace` still red only in sim.)

- [ ] **Step 4: Commit** — `git add -A && git commit -m "feat(net-model)!: drive NodeMachine per node; flights carry WireMessages"`

---### Task 6: sim on the new composition

**Files:**
- Modify: `crates/dash-router-sim/src/lib.rs` (`SimNet` aliases → `NodeMachine`-based)
- Modify: `crates/dash-router-sim/src/behavior.rs`
- Modify: `crates/dash-router-sim/src/scenario.rs`
- Modify: `crates/dash-router-sim/src/metrics.rs`
- Modify: `crates/dash-router-sim/tests/sim.rs`
- Modify: `crates/dash-router-sim/scenarios/example.yaml`

**Interfaces:**
- Consumes: everything above. Produces: the same CLI/report surface as today (`RunRecord` fields unchanged where possible), so baselines stay comparable.

- [ ] **Step 1: Mechanical retype**

- `SimNet = NetMachine<NodeId, LogId, RealTime, K>` (unchanged alias, new innards). Scenario building: `NodeMachine::new(router_config, relay_cap)` shared; nodes `NodeState::new(id, logs)`.
- `relay_cap` moves from `RouterSpec` into scenario config as `relay_cap` under a new `storage:` key (keep the YAML field name; adjust the deserialization path). `RouterConfig` loses it.
- Behavior event/action mapping: `Node(n, Tick/Arm/Fire…)` → `NodeAction::Router(…)` wrapping; `Append` events → `NodeAction::Authored(log, seq, op)` where the behavior tracks `next_seq: BTreeMap<(NodeId, LogId), Seq>` (it already effectively knows this from metrics' `authored`); Deliver stays `NetAction::Deliver(UpTo::new(idx))`.

- [ ] **Step 2: Fx attribution rework in `handle_fx`**

Net fx are now `(N, NodeEffect)`. Adjust:
- Coverage: `NodeEffect::Deliver(log, seq)` marks covered for that node (replaces the old Store/Deliver pair). Relay-side receipt has no effect-level signal anymore — coverage semantics change to **subscriber coverage only**, which is what the metric always claimed to measure (`expected_coverage = nodes − 1` still holds while every node subscribes to every writer's log — the current scenario shape; assert this invariant in `Scenario::validate` so nobody quietly breaks the metric later).
- "Taught nothing" (redundancy/duplicate-reply counters): a delivered Recv whose action produced **no `Deliver` and no state-growth** — compute growth by comparing `node.router.held` before/after the transition (the behavior already snapshots state around steps for latency sampling; reuse that).
- Relay occupancy sampling: `node.relay.0.usage()` replaces the old `relay_usage()`.

- [ ] **Step 3: Retune expectations, run the suite**

Run: `cargo test -p dash-router-sim` — the 4 e2e tests must pass with the same thresholds (lossless full coverage; lossy > 0.9 with loss+backfill exercised; determinism; `jump_to` replay). Any threshold miss is a bug to diagnose, not a threshold to lower — the protocol semantics did not change, only its factoring (push now flows `Authored → Push → SendHave` instead of `Append`'s inline fresh Have).

- [ ] **Step 4: Rerun the example scenarios and record the new baseline**

```bash
cargo run --release --bin sim -- crates/dash-router-sim/scenarios/example.yaml --out sim-baseline
git add sim-baseline && git status
```

Compare headline numbers against the pre-refactor ones from the previous baseline run; expect coverage/t_full/msgs-per-op within noise. Report any drift in the commit message rather than hiding it. (If a `sweep` binary or justfile recipe references removed fields, fix those call sites in this task too.)

- [ ] **Step 5: Full workspace gate** — `cargo test --workspace` → PASS; `cargo clippy --workspace` → no new warnings.

- [ ] **Step 6: Commit** — `git add -A && git commit -m "feat(sim)!: drive NodeMachine composition; subscriber-coverage metrics"`

---

### Task 7: DESIGN.md edit — `fresh` label removed, push survives

**Files:**
- Modify: `DESIGN.md` (message enum ~lines 56–67; §2 "Emitting Fresh Haves" ~lines 127–131; §106 lists the three cases)

**Interfaces:** none (prose).

- [ ] **Step 1: Edit**

- In the `Message` enum: delete the `fresh` field and its comment from `Have`.
- Line 106: "three cases … Wants, Fresh Haves, and Non-Fresh Haves" → "three cases of data being emitted and received by nodes: Wants, pushed Haves (authored data), and repair Haves (Want-triggered)."
- §2 heading "Emitting Fresh Haves" → "Emitting pushed Haves". Rewrite the body to state: authors emit a Have for newly authored ops after a brief debounce; the message carries **no marker** — receivers treat it identically to any other Have, relaying it under the same seen-set rules; this may create gaps that future Wants backfill.
- Sanity-check §3's title/body still reads correctly without the fresh/non-fresh distinction (retitle "Emitting Non-Fresh Haves" → "Emitting repair Haves").

- [ ] **Step 2: Verify no dangling references**

Run: `grep -ni fresh DESIGN.md crates -r --include=*.rs`
Expected: no protocol-meaning hits (comments referring to history are fine but prefer removing them).

- [ ] **Step 3: Commit** — `git add -A && git commit -m "docs: DESIGN.md — fresh label removed, push path stays unlabeled"`

---

## Self-Review (performed at plan-writing time)

- **Spec coverage:** §2 → Task 2; §3.1–3.2 → Task 3 (async mirror §3.3 and §3.5 adapter are the shell plan, out of scope here by the split announced at planning); §4 → Task 3; §5 → Task 4; §6 → Task 1; §8's model-check property list → deferred with `dash-router-model-check` (spec's own scope line); §9 steps 1–6, 8 → Tasks 1–7. Gap check: eviction *policy* (`eviction_candidates`, spec §5) is intentionally in Task 6's sim territory only if a scenario exercises cap pressure — current scenarios don't; the nondeterministic `RelayEvict` action (Task 4) is the modeled surface. Flagged, not silently dropped.
- **Type consistency:** `relayed_haves`/`note_relayed_haves`, `Effect<L>` (no `N`), `NodeEffect<N, L>`, `WireMessage<N, L>`, `held_of(&BTreeSet<L>)` used consistently across tasks.
- **Placeholder scan:** the deliberate "unchanged body" markers point at existing code in the same file being modified (exact fn names given); all new code is written out.
