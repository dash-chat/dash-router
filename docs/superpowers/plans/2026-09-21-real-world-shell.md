# Real World Shell Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the pure-world node into a running tokio LAN node: a shared policy crate, a reachable eviction subsystem with sim evidence, and the `dash-router` shell with a redb relay store, loopback + p2panda transports, and lockstep conformance against `NodeMachine`.

**Architecture:** One tokio task owns everything mutable (`NodeCore`), a thin imperative rind around the same pure `RouterMachine` transitions the model uses. Storage is behind async traits with a blanket sync→async bridge so the conformance test runs the real shell over the model's `OpsMap`. Tasks 1–5 are pure-world/sim work (the inherited eviction follow-ups); Tasks 6–12 build the shell. One plan, strictly ordered — later tasks consume interfaces earlier tasks produce.

**Tech Stack:** tokio 1 (rt, sync, time, macros, test-util), redb (latest), p2panda-net 0.7 (feature-gated), postcard, proptest, rand 0.9.

**Spec:** `docs/superpowers/specs/2026-09-21-real-world-shell-design.md` (this plan's authority), which expands `docs/superpowers/specs/2026-09-21-storage-and-shell-design.md`. All five §9 decisions were approved by the user as drafted: degrade-not-crash error posture, redb, unsigned wire v1, byte-less `Delivered(L, Seq)`, and the `dash-router-policy` crate.

## Global Constraints

- `dash-router-core` stays pure: no RNG, no clock, no I/O, no tokio. Additions to it in this plan (Task 2, one trait method in Task 6) are pure functions/actions only.
- `dash-router-policy` must not depend on tokio (spec §1). `rand` + `serde` only.
- tokio appears only in `dash-router`. p2panda deps appear only in `dash-router` behind the **off-by-default** cargo feature `p2panda`, and only inside `transport.rs` (spec §6).
- **No router protocol changes this round** (user ruling, 2026-09-21): the shed-at-cap churn loop and Unsubscribe keep-wanting behavior stay as-is, documented and deferred. Do not "fix" either in passing; `unsubscribe_keeps_advertising_and_keeps_wanting` in `crates/dash-router-core/tests/node.rs` must keep passing unchanged.
- Storage errors degrade, never crash the node task (spec §3): relay-store errors are treated as sheds; ext-store errors surface as `RouterEvent::StorageError` while gossip continues.
- Wire stays `WIRE_VERSION = 0`, unsigned, postcard; gossip topic string is `"dash-router/v0"` (spec §6.1).
- `Delivered(L, Seq)` carries no bytes (spec §5). Subscription persistence is the embedder's job: `spawn` takes the initial subscription set.
- Sim baselines in `sim-baseline/` are recaptured deliberately (metric/params-echo fields change in Tasks 3–5); every capture is verified bit-identical across two runs before committing (Task 5 does the one recapture).
- Workspace edition 2024; run tests with `cargo test -p <crate>` per task and `cargo test --workspace` at the end of every task that touches more than one crate.
- zsh: quote glob arguments (`--include='*.rs'`), or grep/find calls fail.
- Commit messages end with: `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`

---

### Task 1: The `dash-router-policy` crate

Move `IntervalPolicy` out of the sim into a no-tokio policy crate and add the push-debounce policy the shell needs (spec §1, §4).

**Files:**
- Create: `crates/dash-router-policy/Cargo.toml`
- Create: `crates/dash-router-policy/src/lib.rs`
- Delete: `crates/dash-router-sim/src/policy.rs`
- Modify: `crates/dash-router-sim/src/lib.rs` (replace `pub mod policy;` with a re-export)
- Modify: `crates/dash-router-sim/Cargo.toml` (add the dependency)

**Interfaces:**
- Consumes: nothing new.
- Produces: `dash_router_policy::IntervalPolicy` (moved verbatim: `sample(&self, rng: &mut impl Rng, n: usize) -> Duration`), and `dash_router_policy::PushDebouncePolicy { window_ms: u64, max_latency_ms: u64 }` with `fn deadline(&self, oldest_pending: Duration, latest_append: Duration) -> Duration`. Tasks 9–10 consume both; the sim keeps consuming `IntervalPolicy` through its existing `crate::policy::` paths via the re-export.

- [ ] **Step 1: Create the crate**

`crates/dash-router-policy/Cargo.toml`:

```toml
[package]
name = "dash-router-policy"
description = "Pure timing policies shared by the simulator and the tokio shell"
version.workspace = true
edition.workspace = true

[dependencies]
rand = { workspace = true }
serde = { workspace = true }

[dev-dependencies]
rand_chacha = { workspace = true }
```

`crates/dash-router-policy/src/lib.rs`: start with the **entire current contents** of `crates/dash-router-sim/src/policy.rs` (module doc, `IntervalPolicy`, `default_alpha`, tests), moved without behavior change. Adjust the module doc's first line to: `//! Pure timing policies: the tuning subject, shared by the simulator and the tokio shell.`

- [ ] **Step 2: Write the failing debounce test** (append to `lib.rs`'s test module)

```rust
#[test]
fn debounce_extends_on_appends_but_the_max_latency_cap_wins() {
    let p = PushDebouncePolicy {
        window_ms: 100,
        max_latency_ms: 250,
    };
    let ms = Duration::from_millis;
    // One lone append: flush a window after it.
    assert_eq!(p.deadline(ms(1000), ms(1000)), ms(1100));
    // A later append extends the quiet window...
    assert_eq!(p.deadline(ms(1000), ms(1120)), ms(1220));
    // ...until the cap from the OLDEST pending append wins.
    assert_eq!(p.deadline(ms(1000), ms(1200)), ms(1250));
    assert_eq!(p.deadline(ms(1000), ms(1400)), ms(1250));
}
```

- [ ] **Step 3: Run it to make sure it fails**

Run: `cargo test -p dash-router-policy debounce`
Expected: FAIL — `PushDebouncePolicy` not defined.

- [ ] **Step 4: Implement `PushDebouncePolicy`**

```rust
/// When to flush pending pushed appends (spec §4). Pure: given the times,
/// returns the flush deadline; the shell owns the clock.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PushDebouncePolicy {
    /// Quiet window after the latest append before flushing.
    pub window_ms: u64,
    /// Hard cap after the OLDEST pending append, so a steady append
    /// stream still pushes instead of re-arming forever.
    pub max_latency_ms: u64,
}

impl PushDebouncePolicy {
    /// The moment to flush, given when the oldest still-pending append
    /// happened and when the latest one did.
    pub fn deadline(&self, oldest_pending: Duration, latest_append: Duration) -> Duration {
        let window = latest_append + Duration::from_millis(self.window_ms);
        let cap = oldest_pending + Duration::from_millis(self.max_latency_ms);
        window.min(cap)
    }
}
```

- [ ] **Step 5: Run: `cargo test -p dash-router-policy`** — expected: PASS (moved IntervalPolicy tests + the new one).

- [ ] **Step 6: Rewire the sim**

- `crates/dash-router-sim/Cargo.toml`: add `dash-router-policy = { path = "../dash-router-policy" }` under `[dependencies]`.
- Delete `crates/dash-router-sim/src/policy.rs`.
- In `crates/dash-router-sim/src/lib.rs`, replace the line `pub mod policy;` with:

```rust
pub use dash_router_policy as policy;
```

All existing `crate::policy::IntervalPolicy` paths in `behavior.rs`/`scenario.rs` keep compiling unchanged.

- [ ] **Step 7: Run: `cargo test --workspace`** — expected: PASS, no other edits needed.

- [ ] **Step 8: Commit**

```bash
git add crates/dash-router-policy crates/dash-router-sim Cargo.lock
git commit -m "feat(policy): extract dash-router-policy crate; add PushDebouncePolicy"
```

---

### Task 2: Reachable eviction — `eviction_candidates` + `RelayEvictPayloads`

The pure-world final review found the eviction subsystem unreachable at the composition level (spec §1 inherited follow-ups): `RelayStoreAction::EvictPayloads` exists but no `NodeAction` reaches it, and no policy computes what to evict. Make both real, purely.

**Files:**
- Modify: `crates/dash-router-core/src/node.rs`
- Modify: `crates/dash-router-core/src/lib.rs` (export `eviction_candidates`)
- Test: `crates/dash-router-core/tests/node.rs`

**Interfaces:**
- Consumes: `EvictableStorage::held_payloads()`, `RouterState::others_wants()` (both exist).
- Produces: `pub fn eviction_candidates<L: Id>(relay_held_payloads: &LogRanges<L>, others_wants: &LogRanges<L>) -> LogRanges<L>` (free function in `node.rs`, exported from `lib.rs`); `NodeState::eviction_candidates(&self) -> LogRanges<L>` (method delegating to it); `NodeAction::RelayEvictPayloads(LogRanges<L>)`. Task 4 (sim maintenance) and Task 9 (shell maintenance) consume all three — the shell calls the **free function** so model and shell share one policy.

- [ ] **Step 1: Write the failing tests** (append to `crates/dash-router-core/tests/node.rs`, reusing that file's existing helpers `n`, `l`, `lr`, `machine`, `tiny` and types `N = UpTo<3>`, `L = UpTo<2>`, `T = FiniteTime<4, 1000>`)

```rust
/// Payload eviction is reachable from the node level: candidates spare
/// recently-wanted ranges, evicting frees units, headers keep advertising.
#[test]
fn relay_evict_payloads_frees_units_and_keeps_advertising() {
    let m = machine();
    let s = NodeState::new(n(0), std::iter::empty()); // no subscriptions: bytes land in the relay
    let op = |h: u8| Op {
        header: vec![h],
        payload: Some(vec![h; 4]),
    };
    let have = WireMessage::have(n(1), vec![(l(1), vec![(0, op(10)), (1, op(11))])]);
    let (s, _) = m.transition(s, NodeAction::Recv(have)).unwrap();
    assert_eq!(s.relay.0.usage(), 4);

    // Peer 2 wants seq 0: its payload must survive to answer the Want.
    let want = WireMessage::want(n(2), lr([(1, Ranges::range(0, 1))]));
    let (s, _) = m.transition(s, NodeAction::Recv(want)).unwrap();
    let candidates = s.eviction_candidates();
    assert_eq!(
        candidates,
        lr([(1, Ranges::range(1, 2))]),
        "the wanted seq 0 is spared; only seq 1's payload is a candidate"
    );

    let (s, fx) = m
        .transition(s, NodeAction::RelayEvictPayloads(candidates))
        .unwrap();
    assert!(fx.is_empty(), "headers survive: nothing broadcast or delivered");
    assert_eq!(s.relay.0.usage(), 3, "one payload unit freed");
    assert_eq!(s.relay.0.held_payloads(), lr([(1, Ranges::range(0, 1))]));
    assert_eq!(
        s.router.held.get(&l(1)),
        Some(&Ranges::range(0, 2)),
        "both seqs still advertised (headers held)"
    );
}

/// With no outstanding Wants, every relay payload is a candidate.
#[test]
fn eviction_candidates_cover_everything_when_nothing_is_wanted() {
    let m = machine();
    let s = NodeState::new(n(0), std::iter::empty());
    let op = Op {
        header: vec![9],
        payload: Some(vec![9]),
    };
    let have = WireMessage::have(n(1), vec![(l(0), vec![(0, op)])]);
    let (s, _) = m.transition(s, NodeAction::Recv(have)).unwrap();
    assert_eq!(s.eviction_candidates(), s.relay.0.held_payloads());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p dash-router-core --test node evict`
Expected: FAIL — no method `eviction_candidates`, no variant `RelayEvictPayloads`.

- [ ] **Step 3: Implement in `node.rs`**

Free function (near `group_ops`/`ranges_of`):

```rust
/// Payload-eviction candidates: relay payloads nobody currently wants
/// (DESIGN.md's payloads-first GC). Evicting a wanted payload would force
/// the network to re-send it, so recent Wants are spared. Pure policy,
/// shared verbatim by the model composition and the tokio shell.
pub fn eviction_candidates<L: Id>(
    relay_held_payloads: &LogRanges<L>,
    others_wants: &LogRanges<L>,
) -> LogRanges<L> {
    relay_held_payloads.difference(others_wants)
}
```

Method on `NodeState` (in the existing `impl<N: Id, L: Id, T: TimeInterval> NodeState<N, L, T>` block):

```rust
/// See [`eviction_candidates`].
pub fn eviction_candidates(&self) -> LogRanges<L> {
    eviction_candidates(&self.relay.0.held_payloads(), &self.router.others_wants())
}
```

New `NodeAction` variant (after `RelayEvict`):

```rust
/// The relay dropped payloads (keeping headers) to reclaim space:
/// DESIGN.md's payloads-first GC stage. Policy lives above the machine
/// ([`eviction_candidates`]); the model takes the ranges as an action.
RelayEvictPayloads(LogRanges<L>),
```

New transition arm (next to the `RelayEvict` arm):

```rust
NodeAction::RelayEvictPayloads(ranges) => {
    self.relay_step(&mut s, RelayStoreAction::EvictPayloads(ranges))?;
    self.reconcile_held(&mut s)?;
}
```

In `lib.rs`, add `eviction_candidates` to the `pub use node::{...}` list.

- [ ] **Step 4: Run: `cargo test --workspace`** — expected: PASS (the new variant breaks no exhaustive matches; sim/net-model only construct actions).

- [ ] **Step 5: Commit**

```bash
git add crates/dash-router-core
git commit -m "feat(core): eviction_candidates policy + NodeAction::RelayEvictPayloads"
```

---

### Task 3: Sim — partial subscription

Today every node subscribes to every log, so all bytes land in ext stores and the relay path is never exercised. Add a `subscribers` knob and per-log coverage accounting (prerequisite for cap pressure, spec §1).

**Files:**
- Modify: `crates/dash-router-sim/src/scenario.rs`
- Modify: `crates/dash-router-sim/src/metrics.rs`
- Modify: `crates/dash-router-sim/src/behavior.rs`
- Test: `crates/dash-router-sim/tests/sim.rs`

**Interfaces:**
- Consumes: `NodeState::new(id, subscriptions)` (exists).
- Produces: `WorkloadSpec.subscribers: Option<u32>`; `ScenarioSpec::subscribers_of(&self, log: LogId) -> BTreeSet<NodeId>`; `ScenarioSpec::expected_coverage(&self) -> BTreeMap<LogId, BTreeSet<NodeId>>`; `SimParams.expected: BTreeMap<LogId, BTreeSet<NodeId>>`; `Metrics::new(expected: BTreeMap<LogId, BTreeSet<NodeId>>)`. Tasks 4–5 build on these.

- [ ] **Step 1: Write the failing tests**

Append to `crates/dash-router-sim/tests/sim.rs`:

```rust
/// With `subscribers: k`, log w is subscribed by nodes (w + i) % nodes for
/// i in 0..k, coverage counts only those subscribers (minus the author),
/// and unsubscribed traffic finally lands in relay stores.
#[test]
fn partial_subscription_exercises_the_relay_path() {
    let yaml = r#"
defaults: { seeds: 1, duration_ms: 3000 }
scenarios:
  partial:
    nodes: 6
    topology: { kind: path }
    loss: 0.0
    latency_ms: { distribution: uniform, min_ms: 1, max_ms: 3 }
    router: { want_ttl_ms: 500, have_ttl_ms: 500 }
    storage: { relay_cap: 1048576 }
    policy:
      want: { kind: fixed, min_ms: 100, max_ms: 200 }
      have: { kind: fixed, min_ms: 20, max_ms: 60 }
    workload: { writers: 2, appends_per_sec: 4.0, subscribers: 2 }
"#;
    let config = dash_router_sim::Config::from_yaml(yaml).unwrap();
    let spec = &config.scenarios["partial"];

    // Log 0: author node 0, subscribers {0, 1}; expected coverage {1}.
    let expected = spec.expected_coverage();
    assert_eq!(
        expected[&0],
        std::collections::BTreeSet::from([1u32]),
        "author excluded from its own log's expected set"
    );
    assert_eq!(expected[&1], std::collections::BTreeSet::from([2u32]));

    let mut sim = spec.build(0, &config.defaults).unwrap();
    let record = sim.run(0).unwrap();
    assert_eq!(record.ops_missed, 0, "subscribers still fully covered");
    assert!(
        record.relay_occupancy_max > 0,
        "unsubscribed nodes hold relayed bytes: the relay path is live"
    );
}
```

- [ ] **Step 2: Run: `cargo test -p dash-router-sim partial`** — expected: FAIL (unknown field `subscribers`; today an unknown YAML field errors or, if ignored, `expected_coverage` doesn't exist).

- [ ] **Step 3: Implement `scenario.rs`**

Add to `WorkloadSpec`:

```rust
/// Nodes subscribed per log: log `w` (authored by node `w`) is subscribed
/// by nodes `(w + i) % nodes` for `i in 0..subscribers`. `None` = every
/// node subscribes to every log (the original shape).
#[serde(default)]
pub subscribers: Option<u32>,
```

In `validate()`, **replace** the `full_subscription` ensure and the `full_subscription` method with:

```rust
if let Some(k) = self.workload.subscribers {
    ensure!(
        k >= 2,
        "need at least 2 subscribers per log: with only the author, the coverage metric is vacuous"
    );
    ensure!(k <= self.nodes, "more subscribers than nodes");
}
```

(The long comment above the old ensure goes with it; the per-log expected sets below are now the source of truth for coverage.)

Add methods to `ScenarioSpec`:

```rust
/// The nodes subscribed to `log` (always includes the author, node `log`).
pub fn subscribers_of(&self, log: LogId) -> BTreeSet<NodeId> {
    let k = self.workload.subscribers.unwrap_or(self.nodes);
    (0..k).map(|i| (log as NodeId + i) % self.nodes).collect()
}

/// Per-log coverage targets: each log's subscribers minus its author
/// (`Deliver` never fires for a node's own authored data).
pub fn expected_coverage(&self) -> BTreeMap<LogId, BTreeSet<NodeId>> {
    (0..self.workload.writers)
        .map(|log| {
            let mut subs = self.subscribers_of(log);
            subs.remove(&(log as NodeId));
            (log, subs)
        })
        .collect()
}
```

(Import `BTreeSet` alongside the existing `BTreeMap` import, and `LogId`/`NodeId` from the crate root.)

In `build()`, replace the uniform-subscription `SimNetState::new(...)` with:

```rust
let state = SimNetState::new((0..self.nodes).map(|id| {
    let subs: Vec<LogId> = logs
        .iter()
        .copied()
        .filter(|&log| self.subscribers_of(log).contains(&id))
        .collect();
    NodeState::new(id, subs)
}));
```

and add `expected: self.expected_coverage(),` to the `SimParams` literal.

- [ ] **Step 4: Implement `metrics.rs`**

Replace `expected_coverage: usize` with `expected: BTreeMap<LogId, BTreeSet<NodeId>>` and rework:

```rust
impl Metrics {
    pub fn new(expected: BTreeMap<LogId, BTreeSet<NodeId>>) -> Self {
        Self {
            expected,
            ..Default::default()
        }
    }

    pub fn delivered(&mut self, node: NodeId, log: LogId, seq: u32, now: Duration) {
        let Some(expected) = self.expected.get(&log) else {
            return;
        };
        if !expected.contains(&node) {
            return;
        }
        if let Some(op) = self.ops.get_mut(&(log, seq)) {
            op.covered.insert(node);
            if op.full_at.is_none() && op.covered.len() >= expected.len() {
                op.full_at = Some(now);
            }
        }
    }
}
```

Update the `expected_coverage` doc comment on the struct field to describe the per-log sets. `authored`, `all_covered`, `finish` are unchanged.

- [ ] **Step 5: Implement `behavior.rs`**

`SimParams` gains `pub expected: BTreeMap<LogId, BTreeSet<NodeId>>` (import `BTreeSet`). In `SimBehavior::new`, replace `Metrics::new(params.n)` with `Metrics::new(params.expected.clone())`.

- [ ] **Step 6: Run: `cargo test --workspace`** — expected: PASS, including the untouched existing sim tests (their scenarios omit `subscribers` → full subscription → identical expected sets).

- [ ] **Step 7: Commit**

```bash
git add crates/dash-router-sim
git commit -m "feat(sim): partial-subscription knob with per-log coverage accounting"
```

---

### Task 4: Sim — cap pressure: relay maintenance, NativeSync/AppGc proposals, the scenario

Give the sim the missing spontaneous behaviors (spec §1 inherited follow-ups): a per-node relay-maintenance loop that proposes `RelayEvictPayloads`/`RelayEvict` from `eviction_candidates`, plus workload knobs proposing `NativeSync` and `AppGc`; then a committed cap-pressure scenario.

**Files:**
- Modify: `crates/dash-router-sim/src/scenario.rs`
- Modify: `crates/dash-router-sim/src/behavior.rs`
- Modify: `crates/dash-router-sim/src/metrics.rs`
- Modify: `crates/dash-router-sim/src/report.rs`
- Modify: `crates/dash-router-sim/src/bin/sim.rs`
- Modify: `crates/dash-router-sim/scenarios/example.yaml`
- Test: `crates/dash-router-sim/tests/sim.rs`

**Interfaces:**
- Consumes: `NodeState::eviction_candidates()`, `NodeAction::RelayEvictPayloads` (Task 2); `SimParams.expected` (Task 3).
- Produces: `StorageSpec { relay_cap, evict_at: f64, maintain_interval_ms: Option<u64> }`; `WorkloadSpec.native_sync_per_sec: f64`, `WorkloadSpec.app_gc: Option<AppGcSpec>`; `AppGcSpec { interval_ms: u64, keep_last: u32 }`; `RunRecord.{payload_evictions, full_evictions, native_syncs, app_gc_runs}`; `Sometimes.{eviction_exercised, cap_pressure_exercised}`. Task 5 recaptures baselines over all of it; Task 9's shell maintenance mirrors the eviction policy.

- [ ] **Step 1: Write the failing tests** (append to `crates/dash-router-sim/tests/sim.rs`)

```rust
/// A tight relay cap plus maintenance: the relay saturates, eviction runs,
/// usage never exceeds the cap, and subscribers still get covered.
#[test]
fn cap_pressure_evicts_and_stays_within_cap() {
    let yaml = r#"
defaults: { seeds: 4, duration_ms: 3000 }
scenarios:
  pressure:
    nodes: 6
    topology: { kind: path }
    loss: 0.0
    latency_ms: { distribution: uniform, min_ms: 1, max_ms: 3 }
    router: { want_ttl_ms: 500, have_ttl_ms: 500 }
    storage: { relay_cap: 8, evict_at: 0.75, maintain_interval_ms: 100 }
    policy:
      want: { kind: fixed, min_ms: 100, max_ms: 200 }
      have: { kind: fixed, min_ms: 20, max_ms: 60 }
    workload: { writers: 2, appends_per_sec: 8.0, subscribers: 2 }
"#;
    let config = dash_router_sim::Config::from_yaml(yaml).unwrap();
    let spec = &config.scenarios["pressure"];
    let mut evictions = 0;
    for seed in 0..4 {
        let mut sim = spec.build(seed, &config.defaults).unwrap();
        let record = sim.run(seed).unwrap();
        assert!(record.relay_occupancy_max <= 8, "cap is a hard bound");
        evictions += record.payload_evictions + record.full_evictions;
    }
    assert!(evictions > 0, "maintenance never fired under sustained pressure");
}

/// NativeSync counts toward coverage; AppGc runs and (kept-as-is ruling)
/// reopens wanted gaps — the run still terminates at the drain cap.
#[test]
fn native_sync_and_app_gc_are_proposed() {
    let yaml = r#"
defaults: { seeds: 2, duration_ms: 2000 }
scenarios:
  spontaneous:
    nodes: 4
    topology: { kind: path }
    loss: 0.0
    latency_ms: { distribution: uniform, min_ms: 1, max_ms: 3 }
    router: { want_ttl_ms: 500, have_ttl_ms: 500 }
    storage: { relay_cap: 1048576 }
    policy:
      want: { kind: fixed, min_ms: 100, max_ms: 200 }
      have: { kind: fixed, min_ms: 20, max_ms: 60 }
    workload:
      writers: 2
      appends_per_sec: 8.0
      native_sync_per_sec: 4.0
      app_gc: { interval_ms: 400, keep_last: 1 }
"#;
    let config = dash_router_sim::Config::from_yaml(yaml).unwrap();
    let spec = &config.scenarios["spontaneous"];
    let (mut syncs, mut gcs) = (0, 0);
    for seed in 0..2 {
        let mut sim = spec.build(seed, &config.defaults).unwrap();
        let record = sim.run(seed).unwrap();
        syncs += record.native_syncs;
        gcs += record.app_gc_runs;
    }
    assert!(syncs > 0, "native sync never proposed");
    assert!(gcs > 0, "app gc never proposed");
}
```

- [ ] **Step 2: Run: `cargo test -p dash-router-sim --test sim cap_pressure`** — expected: FAIL (unknown fields).

- [ ] **Step 3: Implement `scenario.rs`**

`StorageSpec` becomes:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageSpec {
    pub relay_cap: Units,
    /// Maintenance evicts once usage reaches `evict_at * relay_cap`.
    #[serde(default = "default_evict_at")]
    pub evict_at: f64,
    /// Per-node relay-maintenance interval; `None` = maintenance off
    /// (the pre-eviction shape).
    #[serde(default)]
    pub maintain_interval_ms: Option<u64>,
}

fn default_evict_at() -> f64 {
    0.75
}
```

`WorkloadSpec` gains:

```rust
/// Poisson rate of out-of-band `NativeSync` ingests (a random subscriber
/// receives a random already-authored op outside the gossip). 0 = off.
#[serde(default)]
pub native_sync_per_sec: f64,
/// Periodic application GC of subscribed ext stores. Note (kept-as-is
/// ruling, 2026-09-21): GC'd ranges leave `held`, so nodes re-Want them —
/// expect refill traffic when this is on. Off in baseline scenarios.
#[serde(default)]
pub app_gc: Option<AppGcSpec>,
```

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppGcSpec {
    pub interval_ms: u64,
    /// Per log, keep the newest `keep_last` seqs; GC everything older.
    pub keep_last: u32,
}
```

`validate()` additions:

```rust
ensure!(
    (0.0..=1.0).contains(&self.storage.evict_at) && self.storage.evict_at > 0.0,
    "evict_at must be in (0, 1]"
);
ensure!(
    self.workload.native_sync_per_sec >= 0.0,
    "negative native_sync_per_sec"
);
```

`SimParams` (in `behavior.rs`) gains `pub relay_cap: Units, pub evict_at: f64, pub maintain_interval: Option<Duration>, pub native_sync_per_sec: f64, pub app_gc: Option<AppGcSpec>`; `build()` fills them (`maintain_interval: self.storage.maintain_interval_ms.map(ms)`).

- [ ] **Step 4: Implement `behavior.rs`**

New events in `Ev`: `Maintain(NodeId)`, `NativeSync`, `AppGc`. New field on `SimBehavior`: `authored_ops: BTreeMap<(LogId, Seq), Op>` (record `authored_ops.insert((log, seq), op.clone())` in the `Ev::Append` arm, next to `metrics.authored`).

Initialization block (where the first `schedule_next_append`/`Sample` happen):

```rust
if let Some(interval) = self.params.maintain_interval {
    for n in state.nodes.keys().copied().collect::<Vec<_>>() {
        self.schedule(interval, Ev::Maintain(n));
    }
}
if self.params.native_sync_per_sec > 0.0 {
    self.schedule_next_native_sync();
}
if let Some(gc) = &self.params.app_gc {
    self.schedule(Duration::from_millis(gc.interval_ms), Ev::AppGc);
}
```

with the helper (next to `schedule_next_append`):

```rust
fn schedule_next_native_sync(&mut self) {
    let exp = rand_distr::Exp::new(self.params.native_sync_per_sec).expect("rate > 0");
    let gap = Duration::from_secs_f64(self.rng.sample(exp));
    self.schedule(self.now + gap, Ev::NativeSync);
}
```

Event arms:

```rust
Ev::Maintain(n) => {
    let node = state.node(&n);
    let threshold =
        ((self.params.evict_at * self.params.relay_cap as f64) as Units).max(1);
    if node.relay.0.usage() >= threshold {
        let candidates = node.eviction_candidates();
        if !candidates.is_empty() {
            // Payloads-first (DESIGN.md GC): units freed = one per payload.
            let freed: usize = candidates.iter().filter_map(|(_, r)| r.len()).sum();
            self.metrics.payload_evictions += freed as u64;
            self.advance(state, n, &mut actions);
            actions.push(SimNetAction::Node(
                n,
                NodeAction::RelayEvictPayloads(candidates),
            ));
            self.tags.push_back(Tag::Plumbing);
        } else {
            // Headers-later stage: shed whole non-wanted ranges.
            let full = node
                .relay
                .0
                .held_all()
                .difference(&node.router.others_wants());
            if !full.is_empty() {
                self.metrics.full_evictions += 1;
                self.advance(state, n, &mut actions);
                actions.push(SimNetAction::Node(n, NodeAction::RelayEvict(full)));
                self.tags.push_back(Tag::Plumbing);
            }
        }
    }
    if self.now < self.params.duration * 2 {
        let interval = self.params.maintain_interval.expect("scheduled only when set");
        self.schedule(self.now + interval, Ev::Maintain(n));
    }
}

Ev::NativeSync => {
    if self.now <= self.params.duration {
        if !self.authored_ops.is_empty() {
            let idx = self.rng.random_range(0..self.authored_ops.len());
            let ((log, seq), op) = self
                .authored_ops
                .iter()
                .nth(idx)
                .map(|(k, v)| (*k, v.clone()))
                .expect("idx < len");
            let subs: Vec<NodeId> = self
                .params
                .expected
                .get(&log)
                .map(|s| s.iter().copied().collect())
                .unwrap_or_default();
            if !subs.is_empty() {
                let node = subs[self.rng.random_range(0..subs.len())];
                self.advance(state, node, &mut actions);
                actions.push(SimNetAction::Node(
                    node,
                    NodeAction::NativeSync(log, seq, op),
                ));
                self.tags.push_back(Tag::Plumbing);
                self.metrics.native_syncs += 1;
                // Out-of-band arrival still counts as this node having the
                // op: no NodeEffect::Deliver fires for a NativeSync.
                self.metrics.delivered(node, log, seq, self.now);
            }
        }
        self.schedule_next_native_sync();
    }
}

Ev::AppGc => {
    let gc_spec = self.params.app_gc.clone().expect("scheduled only when set");
    for n in state.nodes.keys().copied().collect::<Vec<_>>() {
        let node = state.node(&n);
        let mut gc = LogRanges::empty();
        for log in &node.subscriptions {
            if let Some(r) = node.ext.0.held_all().get(log)
                && let Some(last) = r.last()
                && last + 1 > gc_spec.keep_last
            {
                gc.insert(*log, Ranges::range(0, last + 1 - gc_spec.keep_last));
            }
        }
        if !gc.is_empty() {
            self.metrics.app_gc_runs += 1;
            self.advance(state, n, &mut actions);
            actions.push(SimNetAction::Node(n, NodeAction::AppGc(gc)));
            self.tags.push_back(Tag::Plumbing);
        }
    }
    if self.now < self.params.duration {
        self.schedule(
            self.now + Duration::from_millis(gc_spec.interval_ms),
            Ev::AppGc,
        );
    }
}
```

(Add `Ranges` to the `dash_router_core` import list; `Task 5` touches `delivered`'s signature — here it keeps the Task 3 shape.)

- [ ] **Step 5: Implement metrics/report plumbing**

`Metrics` + `RunRecord` gain `pub payload_evictions: u64, pub full_evictions: u64, pub native_syncs: u64, pub app_gc_runs: u64` (copied through in `finish`). `Sometimes` gains:

```rust
pub eviction_exercised: bool,
pub cap_pressure_exercised: bool,
```

computed in `ScenarioReport::new` alongside the existing ones:

```rust
eviction_exercised: runs.iter().any(|r| r.payload_evictions + r.full_evictions > 0),
cap_pressure_exercised: runs.iter().any(|r| {
    r.relay_occupancy_max as f64 >= params.storage.evict_at * params.storage.relay_cap as f64
}),
```

In `bin/sim.rs`, extend the printed checks array with `("eviction_exercised", report.sometimes.eviction_exercised)` and `("cap_pressure_exercised", report.sometimes.cap_pressure_exercised)`.

- [ ] **Step 6: Add the committed scenario** (append to `crates/dash-router-sim/scenarios/example.yaml`)

```yaml
  # Saturate the relays: 20 nodes, only 6 subscribers per log, a cap that
  # cannot hold 4 logs' payloads, maintenance evicting payloads-first.
  # Expect: occupancy pinned near the cap, evictions > 0, coverage held by
  # the ext-side subscribers, shed-then-re-Want churn visible in traffic
  # (documented, deferred — spec §5 of the storage design).
  lan-20-cap-pressure:
    nodes: 20
    topology: { kind: random-tree, extra_edges: 0.2 }
    loss: 0.02
    latency_ms: { distribution: log-normal, median_ms: 3.0, sigma: 0.6 }
    router: { want_ttl_ms: 500, have_ttl_ms: 500 }
    storage: { relay_cap: 64, evict_at: 0.75, maintain_interval_ms: 500 }
    policy:
      want: { kind: density-scaled, min_ms: 300, max_ms: 700, ref_n: 20 }
      have: { kind: fixed, min_ms: 30, max_ms: 120 }
    workload:
      writers: 4
      appends_per_sec: 2.0
      subscribers: 6
      native_sync_per_sec: 0.2
```

- [ ] **Step 7: Run: `cargo test --workspace`** — expected: PASS. Then eyeball the new scenario once: `just sim example target/cap-check` and confirm `lan-20-cap-pressure` reports eviction_exercised and cap_pressure_exercised (do not commit `target/cap-check`).

- [ ] **Step 8: Commit**

```bash
git add crates/dash-router-sim
git commit -m "feat(sim): relay maintenance eviction, NativeSync/AppGc proposals, cap-pressure scenario"
```

---

### Task 5: Sim — push-vs-pull latency metric + baseline recapture

Split delivery latency by how the op arrived (spec §1): rooted in an author's `Push` flood vs. a Want-triggered repair (`FireHave`). The wire carries no marker; the attribution is action-level — each new Have flight inherits its origin from the action that created it, and relays preserve it.

**Files:**
- Modify: `crates/dash-router-sim/src/metrics.rs`
- Modify: `crates/dash-router-sim/src/behavior.rs`
- Modify: `crates/dash-router-sim/src/report.rs`
- Test: `crates/dash-router-sim/tests/sim.rs`
- Recapture: `sim-baseline/` (all scenarios, including `lan-20-cap-pressure`)

**Interfaces:**
- Consumes: `Tag`/`sync_inflight` machinery (exists), Task 3–4 metrics shape.
- Produces: `HaveOrigin { Push, Repair }` (in `metrics.rs`); `Metrics::delivered(&mut self, node, log, seq, now, origin: Option<HaveOrigin>)` (breaking signature change — Task 4's NativeSync call site passes `None`); `RunRecord.{push_deliveries, pull_deliveries, t_push_ms, t_pull_ms}`; `Summary.push_share`.

- [ ] **Step 1: Write the failing unit test** (append to `crates/dash-router-sim/tests/sim.rs`)

```rust
/// Origin attribution: push-flood deliveries and repair deliveries land in
/// separate latency buckets; out-of-band (None) deliveries in neither.
#[test]
fn delivery_latency_splits_by_have_origin() {
    use dash_router_sim::metrics::{HaveOrigin, Metrics};
    use std::collections::{BTreeMap, BTreeSet};
    use std::time::Duration;

    let expected = BTreeMap::from([(0u8, BTreeSet::from([1u32, 2, 3]))]);
    let mut m = Metrics::new(expected);
    let ms = Duration::from_millis;
    m.authored(0, 0, ms(0));
    m.delivered(1, 0, 0, ms(10), Some(HaveOrigin::Push));
    m.delivered(2, 0, 0, ms(500), Some(HaveOrigin::Repair));
    m.delivered(3, 0, 0, ms(20), None); // e.g. native sync
    // A repeat never double-counts.
    m.delivered(1, 0, 0, ms(999), Some(HaveOrigin::Repair));
    let r = m.finish(0);
    assert_eq!(r.push_deliveries, 1);
    assert_eq!(r.pull_deliveries, 1);
    assert_eq!(r.t_push_ms.max, 10.0);
    assert_eq!(r.t_pull_ms.max, 500.0);
    assert_eq!(r.ops_fully_covered, 1);
}
```

- [ ] **Step 2: Run: `cargo test -p dash-router-sim --test sim origin`** — expected: FAIL.

- [ ] **Step 3: Implement `metrics.rs`**

```rust
/// How a Have flight came to exist: rooted in an author's push flood, or
/// in a Want-triggered repair fire. Relays preserve the origin.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HaveOrigin {
    Push,
    Repair,
}
```

`Metrics` gains `push_latencies: Vec<f64>, pull_latencies: Vec<f64>` (private). `delivered` becomes:

```rust
pub fn delivered(
    &mut self,
    node: NodeId,
    log: LogId,
    seq: u32,
    now: Duration,
    origin: Option<HaveOrigin>,
) {
    let Some(expected) = self.expected.get(&log) else {
        return;
    };
    if !expected.contains(&node) {
        return;
    }
    if let Some(op) = self.ops.get_mut(&(log, seq)) {
        if op.covered.insert(node) {
            let latency = (now - op.born).as_secs_f64() * 1000.0;
            match origin {
                Some(HaveOrigin::Push) => self.push_latencies.push(latency),
                Some(HaveOrigin::Repair) => self.pull_latencies.push(latency),
                None => {}
            }
        }
        if op.full_at.is_none() && op.covered.len() >= expected.len() {
            op.full_at = Some(now);
        }
    }
}
```

`RunRecord` gains `pub push_deliveries: u64, pub pull_deliveries: u64, pub t_push_ms: Stats, pub t_pull_ms: Stats`; `finish` fills them from the two vectors (`len() as u64`, `Stats::of`). Export `HaveOrigin` from `metrics` (and add it to the crate-root `pub use metrics::{...}` list in `lib.rs`).

- [ ] **Step 4: Implement `behavior.rs` attribution**

- `Tag` changes: add variant `FireHave`; `Tag::Recv` gains `origin: Option<HaveOrigin>`.
- New field: `flight_origins: BTreeMap<Flight<NodeId, LogId>, HaveOrigin>`.
- `Ev::FireHave` arm: the tag pushed alongside `RouterAction::FireHave` becomes `Tag::FireHave` (was `Plumbing`; Arm/Tick pushes stay `Plumbing`).
- `Ev::Deliver` arm: before pushing `Tag::Recv`, look up `let origin = self.flight_origins.remove(&flight);` and carry it in the tag. In the `Ev::Drop` arm, also `self.flight_origins.remove(&flight);` (bounds the map). Identical concurrent flights share one entry — first-wins; the duplicate is a redundant receive and records nothing (rulinged, negligible).
- `handle_fx`: restructure the top to compute the origin context before consuming the tag:

```rust
let tag = self.tags.pop_front();
ensure!(tag.is_some(), "effects arrived with no proposed action to attribute them to");
let tag = tag.unwrap();
let ctx = match &tag {
    Tag::Append => Some(HaveOrigin::Push),
    Tag::FireHave => Some(HaveOrigin::Repair),
    Tag::Recv { origin, .. } => *origin,
    Tag::Plumbing => None,
};
match tag { ... existing arms, plus `Tag::FireHave => {}` ... }
self.sync_inflight(state, ctx);
```

  and inside the `Tag::Recv` arm, deliveries pass the origin through: `self.metrics.delivered(*node, *log, *seq, self.now, origin)` (destructure `origin` from the tag).
- `sync_inflight(&mut self, state: &SimNetState, origin: Option<HaveOrigin>)`: in the new-flight loop, after the want/have count, tag Have flights:

```rust
if matches!(flight.message.body, WireBody::Have(_))
    && let Some(o) = origin
{
    self.flight_origins.entry(flight.clone()).or_insert(o);
}
```

- Task 4's NativeSync call site becomes `self.metrics.delivered(node, log, seq, self.now, None);`.

- [ ] **Step 5: `report.rs`**: `Summary` gains `pub push_share: f64` — in `ScenarioReport::new`:

```rust
push_share: ratio(
    runs.iter().map(|r| r.push_deliveries).sum(),
    runs.iter().map(|r| r.push_deliveries + r.pull_deliveries).sum(),
),
```

- [ ] **Step 6: Add the integration assertion** (append to the existing `partial_subscription_exercises_the_relay_path` test, after the current asserts)

```rust
    assert!(
        record.push_deliveries + record.pull_deliveries > 0,
        "every gossip delivery carries an origin"
    );
```

- [ ] **Step 7: Run: `cargo test --workspace`** — expected: PASS.

- [ ] **Step 8: Recapture baselines, verified bit-identical**

```bash
cargo run --release --bin sim -- crates/dash-router-sim/scenarios/example.yaml --out sim-baseline
cargo run --release --bin sim -- crates/dash-router-sim/scenarios/example.yaml --out target/sim-verify
diff -r sim-baseline target/sim-verify
```

The diff must be empty. Sanity-check the headlines: `lan-20`/`lan-50-lossy` coverage stays 100% and message counts match the pre-change baselines (only new fields and the params echo may differ — if t_full/coverage/msgs moved, something behavioral leaked in; stop and investigate). `lan-20-cap-pressure` must show `eviction_exercised` and `cap_pressure_exercised`.

- [ ] **Step 9: Commit**

```bash
git add crates/dash-router-sim sim-baseline
git commit -m "feat(sim): push-vs-pull delivery latency metric; recapture baselines"
```

---

### Task 6: `dash-router` scaffold — async storage traits + `MemStore`

Create the shell crate with the async storage boundary and the in-memory selfish store (spec §3.5 via §6.2, §7). Includes one small pure addition to core: `ingest_delta` joins the `EvictableStorage` trait so the blanket bridge can carry it.

**Files:**
- Create: `crates/dash-router/Cargo.toml`
- Create: `crates/dash-router/src/lib.rs`
- Create: `crates/dash-router/src/storage.rs`
- Create: `crates/dash-router/src/mem.rs`
- Modify: `crates/dash-router-core/src/storage.rs` (trait method + OpsMap impl)

**Interfaces:**
- Consumes: `Storage`/`EvictableStorage`/`OpsMap` (core), `OpsMap::ingest_delta` (inherent, becomes trait method too).
- Produces (consumed by Tasks 7–11): traits `AsyncStorage<L>` (`held_of`, `held_all`, `fetch`, `ingest`), `AsyncEvictableStorage<L>` (`usage`, `ingest_delta`, `held_payloads`, `evict_payloads`, `evict`), `WatchableStorage<L>` (`changed() -> broadcast::Receiver<BTreeSet<L>>`); blanket impls of the first two for any sync `Storage`/`EvictableStorage`; `MemStore<L>` (Clone-shared, `new()`, `snapshot() -> OpsMap<L>`, `insert_out_of_band(log, seq, op)`).

- [ ] **Step 1: Core trait addition (with test)**

In `crates/dash-router-core/src/storage.rs`, add to the `EvictableStorage` trait:

```rust
/// The unit delta `ingest(log, seq, op)` would add: 0 duplicate, 1
/// payload-upgrade or new header-only, 2 new payload-bearing. Cap checks
/// must use the same arithmetic as the store (see [`OpsMap::ingest_delta`]).
fn ingest_delta(&self, log: &L, seq: Seq, op: &Op) -> Units;
```

and to `impl EvictableStorage for OpsMap`:

```rust
fn ingest_delta(&self, log: &L, seq: Seq, op: &Op) -> Units {
    OpsMap::ingest_delta(self, log, seq, op)
}
```

(The inherent method stays; existing callers are untouched.) Run: `cargo test --workspace` — PASS.

- [ ] **Step 2: Create the crate**

`crates/dash-router/Cargo.toml`:

```toml
[package]
name = "dash-router"
description = "The tokio shell: a real Dash Router node over async storage and a LAN transport"
version.workspace = true
edition.workspace = true

[dependencies]
dash-router-core = { path = "../dash-router-core" }
dash-router-policy = { path = "../dash-router-policy" }
anyhow = { workspace = true }
postcard = { workspace = true }
serde = { workspace = true }
rand = { workspace = true }
tokio = { version = "1", features = ["rt", "sync", "time", "macros"] }
trait-variant = "0.1"

[dev-dependencies]
proptest = { workspace = true }
rand_chacha = { workspace = true }
tokio = { version = "1", features = ["rt", "sync", "time", "macros", "test-util"] }
tempfile = "3"
```

`src/lib.rs`:

```rust
//! The tokio shell: one node task owning pure `RouterMachine` transitions,
//! async storage behind traits, and a pluggable LAN transport. See
//! docs/superpowers/specs/2026-09-21-real-world-shell-design.md.

pub mod mem;
pub mod storage;

pub use mem::MemStore;
pub use storage::{AsyncEvictableStorage, AsyncStorage, WatchableStorage};
```

- [ ] **Step 3: Write the failing tests** (in a `#[cfg(test)] mod tests` inside `storage.rs` and `mem.rs`)

`storage.rs` tests:

```rust
use super::*;
use dash_router_core::OpsMap;

/// Any pure sync store is an async store via the blanket bridge — this is
/// what the conformance test runs the real shell over.
#[tokio::test]
async fn blanket_bridge_delegates_to_the_sync_store() {
    let mut m = OpsMap::<u8>::default();
    let op = Op {
        header: vec![1],
        payload: Some(vec![2]),
    };
    AsyncStorage::ingest(&mut m, 0, 0, op.clone()).await.unwrap();
    assert_eq!(AsyncEvictableStorage::usage(&m).await.unwrap(), 2);
    assert_eq!(
        AsyncEvictableStorage::ingest_delta(&m, &0, 0, &op).await.unwrap(),
        0,
        "duplicate"
    );
    let held = AsyncStorage::held_all(&m).await.unwrap();
    assert_eq!(AsyncStorage::fetch(&m, &held).await.unwrap().len(), 1);
    AsyncEvictableStorage::evict(&mut m, &held).await.unwrap();
    assert!(AsyncStorage::held_all(&m).await.unwrap().is_empty());
}
```

`mem.rs` tests:

```rust
use super::*;

#[tokio::test]
async fn mem_store_is_shared_and_hints_after_ingest() {
    let mut store = MemStore::<u8>::new();
    let handle = store.clone();
    let mut hints = store.changed();
    store
        .ingest(3, 0, Op { header: vec![1], payload: None })
        .await
        .unwrap();
    assert_eq!(hints.recv().await.unwrap(), BTreeSet::from([3]));
    assert!(handle.snapshot().held_all().contains(&3, 0), "clone shares state");

    // The out-of-band path (an embedder's native sync) also hints.
    handle.insert_out_of_band(4, 0, Op::default());
    assert_eq!(hints.recv().await.unwrap(), BTreeSet::from([4]));
}
```

- [ ] **Step 4: Run: `cargo test -p dash-router`** — expected: FAIL (nothing implemented).

- [ ] **Step 5: Implement `storage.rs`**

```rust
//! The async storage boundary (spec §3): fallible, awaitable counterparts
//! of the core's sync traits, plus a blanket bridge so any pure sync store
//! (e.g. `OpsMap`) is usable directly.

use std::collections::BTreeSet;

use anyhow::Result;
use dash_router_core::{EvictableStorage, LogRanges, Op, Seq, Storage, Units};
use tokio::sync::broadcast;

// trait_variant desugars each `async fn` to `-> impl Future + Send`, so
// Task 10's `tokio::spawn` can prove the node task's future is Send while
// impls still get written with plain `async fn` syntax.
#[trait_variant::make(Send)]
pub trait AsyncStorage<L: Ord> {
    /// Ranges held for exactly the requested logs; a requested-but-unknown
    /// log appears with an empty range, mirroring the request.
    async fn held_of(&self, logs: &BTreeSet<L>) -> Result<LogRanges<L>>;
    async fn held_all(&self) -> Result<LogRanges<L>>;
    async fn fetch(&self, ranges: &LogRanges<L>) -> Result<Vec<(L, Seq, Op)>>;
    async fn ingest(&mut self, log: L, seq: Seq, op: Op) -> Result<()>;
}

#[trait_variant::make(Send)]
pub trait AsyncEvictableStorage<L: Ord>: AsyncStorage<L> {
    async fn usage(&self) -> Result<Units>;
    /// The unit delta `ingest` would add: the shed-at-cap check must use
    /// the same arithmetic as the store (`OpsMap::ingest_delta`).
    async fn ingest_delta(&self, log: &L, seq: Seq, op: &Op) -> Result<Units>;
    async fn held_payloads(&self) -> Result<LogRanges<L>>;
    async fn evict_payloads(&mut self, ranges: &LogRanges<L>) -> Result<()>;
    async fn evict(&mut self, ranges: &LogRanges<L>) -> Result<()>;
}

/// Lossy change hints from a store with writers of its own (spec §2).
pub trait WatchableStorage<L: Ord>: AsyncStorage<L> {
    /// Logs whose held ranges may have changed; the empty set means
    /// "anything" (re-read `held_all`). Lossy by design — a missed hint
    /// is repaired by the next Want/Have cycle.
    fn changed(&self) -> broadcast::Receiver<BTreeSet<L>>;
}

impl<L: Ord + Clone + Send + Sync, S: Storage<L> + Send + Sync> AsyncStorage<L> for S {
    async fn held_of(&self, logs: &BTreeSet<L>) -> Result<LogRanges<L>> {
        Ok(Storage::held_of(self, logs))
    }
    async fn held_all(&self) -> Result<LogRanges<L>> {
        Ok(Storage::held_all(self))
    }
    async fn fetch(&self, ranges: &LogRanges<L>) -> Result<Vec<(L, Seq, Op)>> {
        Ok(Storage::fetch(self, ranges))
    }
    async fn ingest(&mut self, log: L, seq: Seq, op: Op) -> Result<()> {
        Storage::ingest(self, log, seq, op);
        Ok(())
    }
}

impl<L: Ord + Clone + Send + Sync, S: EvictableStorage<L> + Send + Sync> AsyncEvictableStorage<L> for S {
    async fn usage(&self) -> Result<Units> {
        Ok(EvictableStorage::usage(self))
    }
    async fn ingest_delta(&self, log: &L, seq: Seq, op: &Op) -> Result<Units> {
        Ok(EvictableStorage::ingest_delta(self, log, seq, op))
    }
    async fn held_payloads(&self) -> Result<LogRanges<L>> {
        Ok(EvictableStorage::held_payloads(self))
    }
    async fn evict_payloads(&mut self, ranges: &LogRanges<L>) -> Result<()> {
        EvictableStorage::evict_payloads(self, ranges);
        Ok(())
    }
    async fn evict(&mut self, ranges: &LogRanges<L>) -> Result<()> {
        EvictableStorage::evict(self, ranges);
        Ok(())
    }
}
```

- [ ] **Step 6: Implement `mem.rs`**

```rust
//! In-memory selfish/ext store (spec §6.2): a shared handle around an
//! `OpsMap` plus a lossy `changed()` stream fed by its own writes.

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use dash_router_core::{LogRanges, Op, OpsMap, Seq, Storage};
use tokio::sync::broadcast;

use crate::storage::{AsyncStorage, WatchableStorage};

/// `Clone` shares the same store, so tests and embedders keep a handle to
/// a store the shell owns. The mutex is held only for synchronous map
/// operations — never across an await.
#[derive(Clone, Debug)]
pub struct MemStore<L: Ord> {
    inner: Arc<Mutex<OpsMap<L>>>,
    tx: broadcast::Sender<BTreeSet<L>>,
}

impl<L: Ord + Clone> MemStore<L> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(OpsMap::default())),
            tx: broadcast::channel(64).0,
        }
    }

    pub fn snapshot(&self) -> OpsMap<L> {
        self.inner.lock().expect("mem store poisoned").clone()
    }

    /// A write from outside the shell (the embedder's native sync): stores
    /// and hints, exactly like a shell-side ingest.
    pub fn insert_out_of_band(&self, log: L, seq: Seq, op: Op) {
        self.inner
            .lock()
            .expect("mem store poisoned")
            .ingest(log.clone(), seq, op);
        let _ = self.tx.send(BTreeSet::from([log])); // no receivers is fine
    }
}

impl<L: Ord + Clone> Default for MemStore<L> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L: Ord + Clone + Send + Sync> AsyncStorage<L> for MemStore<L> {
    async fn held_of(&self, logs: &BTreeSet<L>) -> Result<LogRanges<L>> {
        Ok(self.inner.lock().expect("mem store poisoned").held_of(logs))
    }
    async fn held_all(&self) -> Result<LogRanges<L>> {
        Ok(self.inner.lock().expect("mem store poisoned").held_all())
    }
    async fn fetch(&self, ranges: &LogRanges<L>) -> Result<Vec<(L, Seq, Op)>> {
        Ok(self.inner.lock().expect("mem store poisoned").fetch(ranges))
    }
    async fn ingest(&mut self, log: L, seq: Seq, op: Op) -> Result<()> {
        self.inner
            .lock()
            .expect("mem store poisoned")
            .ingest(log.clone(), seq, op);
        let _ = self.tx.send(BTreeSet::from([log]));
        Ok(())
    }
}

impl<L: Ord + Clone + Send + Sync> WatchableStorage<L> for MemStore<L> {
    fn changed(&self) -> broadcast::Receiver<BTreeSet<L>> {
        self.tx.subscribe()
    }
}
```

(No blanket conflict: `MemStore` does not implement the sync `Storage`.)

- [ ] **Step 7: Run: `cargo test --workspace`** — expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/dash-router crates/dash-router-core Cargo.lock
git commit -m "feat(net): dash-router crate with async storage boundary and MemStore"
```

---

### Task 7: The redb disk relay store

The relay's disk cache (spec §6.2, redb **[approved]**): one `ops` table keyed `(log bytes, seq BE)` so `held_all`/`fetch` are ordered prefix scans, with usage/held summaries scanned at startup and maintained incrementally (single writer: us).

**Files:**
- Create: `crates/dash-router/src/disk.rs`
- Modify: `crates/dash-router/src/lib.rs` (`pub mod disk;` + re-export `DiskRelayStore`, `LogKey`)
- Modify: `crates/dash-router/Cargo.toml` (add `redb`)

**Interfaces:**
- Consumes: `AsyncStorage`/`AsyncEvictableStorage` (Task 6).
- Produces: `trait LogKey: Ord + Clone { const WIDTH: usize; fn write_key(&self, out: &mut Vec<u8>); fn read_key(bytes: &[u8]) -> Self; }` with impls for `[u8; 32]`, `u32`, `u8` (big-endian); `DiskRelayStore<L: LogKey>` with `pub fn open(path: &Path) -> Result<Self>` implementing both async storage traits. Task 10's spawn takes any `AsyncEvictableStorage`, so this store plugs in without further glue.

**redb note for the implementer:** add the dependency with `cargo add redb --package dash-router` (latest stable) and consult that version's docs.rs for exact signatures — the shapes to use are `Database::create(path)`, `TableDefinition::<&[u8], &[u8]>::new("ops")`, `begin_write()/open_table()/insert()/remove()/commit()`, `begin_read()/open_table()/range::<&[u8]>(start..end)`. Keep this task's trait surface fixed regardless of redb's API details. redb calls run inline in the async fns **[decision]**: single writer, small values, a cache we may lose — `spawn_blocking` would buy little and cost `'static` copies; revisit under profiling.

- [ ] **Step 1: Write the failing tests** (`#[cfg(test)]` in `disk.rs`)

```rust
use super::*;
use dash_router_core::{OpsMap, Ranges, Storage as _};

fn op(h: u8, payload: bool) -> Op {
    Op { header: vec![h], payload: payload.then(|| vec![h; 8]) }
}

#[tokio::test]
async fn disk_store_matches_the_opsmap_oracle_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("relay.redb");
    let mut oracle = OpsMap::<u32>::default();
    {
        let mut store = DiskRelayStore::<u32>::open(&path).unwrap();
        // Interleave ingests (incl. duplicate + payload upgrade) and evictions.
        for (log, seq, o) in [
            (7, 0, op(1, true)),
            (7, 1, op(2, false)),
            (9, 0, op(3, true)),
            (7, 1, op(4, true)),  // upgrade
            (7, 0, op(1, true)),  // duplicate
        ] {
            assert_eq!(
                store.ingest_delta(&log, seq, &o).await.unwrap(),
                oracle.ingest_delta(&log, seq, &o),
            );
            store.ingest(log, seq, o.clone()).await.unwrap();
            oracle.ingest(log, seq, o);
        }
        let gc = LogRanges::from_pairs([(7u32, Ranges::range(0, 1))]);
        store.evict_payloads(&gc).await.unwrap();
        oracle.evict_payloads(&gc);
        let cut = LogRanges::from_pairs([(9u32, Ranges::full())]);
        store.evict(&cut).await.unwrap();
        oracle.evict(&cut);

        assert_eq!(store.held_all().await.unwrap(), oracle.held_all());
        assert_eq!(store.held_payloads().await.unwrap(), oracle.held_payloads());
        assert_eq!(store.usage().await.unwrap(), oracle.usage());
        let mut got = store.fetch(&oracle.held_all()).await.unwrap();
        let mut want = oracle.fetch(&oracle.held_all());
        got.sort();
        want.sort();
        assert_eq!(got, want);
    }
    // Reopen: the startup scan rebuilds the same summaries.
    let store = DiskRelayStore::<u32>::open(&path).unwrap();
    assert_eq!(store.held_all().await.unwrap(), oracle.held_all());
    assert_eq!(store.held_payloads().await.unwrap(), oracle.held_payloads());
    assert_eq!(store.usage().await.unwrap(), oracle.usage());
}

#[test]
fn log_keys_are_fixed_width_and_order_preserving() {
    let mut a = Vec::new();
    let mut b = Vec::new();
    3u32.write_key(&mut a);
    300u32.write_key(&mut b);
    assert_eq!(a.len(), <u32 as LogKey>::WIDTH);
    assert!(a < b, "big-endian keeps numeric order");
    assert_eq!(<u32 as LogKey>::read_key(&a), 3);
    let arr = [9u8; 32];
    let mut k = Vec::new();
    arr.write_key(&mut k);
    assert_eq!(<[u8; 32] as LogKey>::read_key(&k), arr);
}
```

Also add a proptest mirroring the oracle test over arbitrary small op sequences (logs in `0..4u32`, seqs in `0..8`, random payload flags, interleaved `evict_payloads`/`evict` of random ranges), asserting `held_all`/`held_payloads`/`usage` equality after every step — this is the guard on the incremental cache.

- [ ] **Step 2: Run: `cargo test -p dash-router disk`** — expected: FAIL.

- [ ] **Step 3: Implement `disk.rs`**

Key codec:

```rust
/// A log id usable as a fixed-width, order-preserving byte key.
pub trait LogKey: Ord + Clone {
    const WIDTH: usize;
    fn write_key(&self, out: &mut Vec<u8>);
    fn read_key(bytes: &[u8]) -> Self;
}
// impls: [u8; 32] (copy), u32 / u8 (to_be_bytes / from_be_bytes)
```

Store shape and rules:

```rust
pub struct DiskRelayStore<L: LogKey> {
    db: redb::Database,
    /// Maintained incrementally (single writer: this store); rebuilt by a
    /// full scan in `open`. Corruption here is repaired by reopen.
    held: LogRanges<L>,
    payloads: LogRanges<L>,
    usage: Units,
}
```

- Row: key = `L::write_key ++ seq.to_be_bytes()`, value = `postcard::to_stdvec(&op)`.
- `open`: create/open db and table, full scan building the three summaries (`Ranges::from_seqs` per log; skip empty logs — mirror `OpsMap::held_all`/`held_payloads`).
- `ingest`: read existing row first (this also powers `ingest_delta`); apply the never-downgrade rule (`OpsMap::ingest` semantics: keep an existing payload over an incoming `None`); write; update `usage` by the delta and patch `held`/`payloads` via `insert`/`union` of the single seq.
- `evict_payloads(ranges)` / `evict(ranges)`: for each named log, prefix-scan its rows, rewrite/remove those whose seq the range contains, then **rebuild that log's entries** in all three summaries from a fresh prefix scan (recompute-per-touched-log keeps the incremental cache trivially correct; eviction is rare).
- `held_of`: answer from `held`, mirroring the request with empty ranges for absent logs (same contract as `OpsMap::held_of`).
- `fetch`: per requested log, prefix scan and filter by `ranges.contains`.
- `ingest_delta`: read the row, apply the `OpsMap::ingest_delta` match (2/1 new, 1 upgrade, 0 duplicate).

- [ ] **Step 4: Run: `cargo test -p dash-router`** — expected: PASS (including proptest).

- [ ] **Step 5: Commit**

```bash
git add crates/dash-router Cargo.lock
git commit -m "feat(net): redb-backed DiskRelayStore with scanned-then-incremental summaries"
```

---

### Task 8: LAN predicate, `Transport` trait, loopback transport

The pluggable transport boundary (spec §6.1, §7): a trait the p2panda adapter and the test loopback both implement, plus the pure LAN predicate.

**Files:**
- Create: `crates/dash-router/src/lan.rs`
- Create: `crates/dash-router/src/transport.rs`
- Modify: `crates/dash-router/src/lib.rs` (`pub mod lan; pub mod transport;` + re-exports `is_lan`, `Transport`, `Incoming`, `LoopbackHub`)

**Interfaces:**
- Consumes: nothing beyond tokio.
- Produces: `pub fn is_lan(ip: IpAddr) -> bool`; `pub struct Incoming { pub remote: Option<IpAddr>, pub bytes: Vec<u8> }` (`remote: None` means the transport itself scopes membership, e.g. an mDNS-discovered overlay); `trait Transport { async fn broadcast(&mut self, bytes: Vec<u8>) -> Result<()>; async fn recv(&mut self) -> Option<Incoming>; }` (annotated `#[trait_variant::make(Send)]`, like the storage traits); `LoopbackHub::new()`, `LoopbackHub::join(&self, addr: IpAddr) -> LoopbackTransport`. Tasks 9–11 use the loopback; Task 12 implements the trait over p2panda.

- [ ] **Step 1: Write the failing tests**

`lan.rs`:

```rust
#[test]
fn lan_predicate_accepts_private_v4_and_loopback_only() {
    let yes = ["10.0.0.1", "172.16.0.1", "172.31.255.255", "192.168.1.10", "127.0.0.1", "::1"];
    let no = ["8.8.8.8", "172.32.0.1", "100.64.0.1", "2001:db8::1", "fe80::1"];
    for ip in yes {
        assert!(is_lan(ip.parse().unwrap()), "{ip} should be LAN");
    }
    for ip in no {
        assert!(!is_lan(ip.parse().unwrap()), "{ip} should be dropped");
    }
}
```

`transport.rs`:

```rust
#[tokio::test]
async fn loopback_broadcasts_to_everyone_but_the_sender() {
    let hub = LoopbackHub::new();
    let mut a = hub.join("192.168.0.1".parse().unwrap());
    let mut b = hub.join("192.168.0.2".parse().unwrap());
    let mut c = hub.join("192.168.0.3".parse().unwrap());
    a.broadcast(vec![1, 2, 3]).await.unwrap();
    for t in [&mut b, &mut c] {
        let got = t.recv().await.unwrap();
        assert_eq!(got.bytes, vec![1, 2, 3]);
        assert_eq!(got.remote, Some("192.168.0.1".parse().unwrap()));
    }
    b.broadcast(vec![9]).await.unwrap();
    let got = a.recv().await.unwrap();
    assert_eq!(got.bytes, vec![9], "a never sees its own earlier broadcast");
}
```

- [ ] **Step 2: Run: `cargo test -p dash-router lan loopback`** — expected: FAIL.

- [ ] **Step 3: Implement**

`lan.rs`:

```rust
//! The LAN boundary predicate (DESIGN.md; spec §6.1): the protocol only
//! runs between private-range peers. Loopback is accepted for local
//! development and tests.

use std::net::IpAddr;

pub fn is_lan(ip: IpAddr) -> bool {
    match ip {
        // is_private() is exactly DESIGN.md's three ranges:
        // 10/8, 172.16/12, 192.168/16.
        IpAddr::V4(v4) => v4.is_private() || v4.is_loopback(),
        IpAddr::V6(v6) => v6.is_loopback(),
    }
}
```

`transport.rs`:

```rust
//! The transport boundary (spec §7): what makes the conformance and e2e
//! tests possible without p2panda anywhere near them.

use std::net::IpAddr;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyhow::Result;
use tokio::sync::broadcast;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Incoming {
    /// The remote's address for the LAN check; `None` means the transport
    /// itself scopes membership (e.g. an mDNS-discovered overlay).
    pub remote: Option<IpAddr>,
    pub bytes: Vec<u8>,
}

#[trait_variant::make(Send)]
pub trait Transport {
    async fn broadcast(&mut self, bytes: Vec<u8>) -> Result<()>;
    /// `None` = the transport has shut down.
    async fn recv(&mut self) -> Option<Incoming>;
}

/// An in-process broadcast domain: every joined transport hears every
/// other's broadcasts (never its own), tagged with the sender's address.
#[derive(Clone)]
pub struct LoopbackHub {
    tx: broadcast::Sender<(u64, IpAddr, Vec<u8>)>,
    next_id: Arc<AtomicU64>,
}

pub struct LoopbackTransport {
    id: u64,
    addr: IpAddr,
    tx: broadcast::Sender<(u64, IpAddr, Vec<u8>)>,
    rx: broadcast::Receiver<(u64, IpAddr, Vec<u8>)>,
}

impl LoopbackHub {
    pub fn new() -> Self {
        Self {
            tx: broadcast::channel(1024).0,
            next_id: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn join(&self, addr: IpAddr) -> LoopbackTransport {
        LoopbackTransport {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            addr,
            tx: self.tx.clone(),
            rx: self.tx.subscribe(),
        }
    }
}

impl Default for LoopbackHub {
    fn default() -> Self {
        Self::new()
    }
}

impl Transport for LoopbackTransport {
    async fn broadcast(&mut self, bytes: Vec<u8>) -> Result<()> {
        let _ = self.tx.send((self.id, self.addr, bytes)); // no listeners is fine
        Ok(())
    }

    async fn recv(&mut self) -> Option<Incoming> {
        loop {
            match self.rx.recv().await {
                Ok((id, _, _)) if id == self.id => continue,
                Ok((_, addr, bytes)) => {
                    return Some(Incoming {
                        remote: Some(addr),
                        bytes,
                    });
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue, // gossip is lossy
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}
```

Add `trait-variant = "0.1"` to `[dependencies]` if Task 6 has not already (see its Step 5 note).

- [ ] **Step 4: Run: `cargo test -p dash-router`** — expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/dash-router Cargo.lock
git commit -m "feat(net): LAN predicate, Transport trait, in-process loopback hub"
```

---

### Task 9: `NodeCore` — the shell's routing table

The heart of the shell (spec §2–§4): a struct owning the pure `RouterState` plus both async stores, with one method per input source. Every method advances time first, runs pure transitions, and routes effects with `.await`s — the async re-implementation of `NodeMachine`'s glue (deliberate duplication; Task 11 checks they agree). The select loop (Task 10) stays thin.

**Files:**
- Create: `crates/dash-router/src/shell.rs` (NodeCore + `IntervalSource`)
- Create: `crates/dash-router/src/handle.rs` (this task: `RouterEvent`, `StorageErrorReport`; Task 10 adds commands)
- Modify: `crates/dash-router/src/lib.rs` (`pub mod handle; pub mod shell;` + re-exports)
- Modify: `crates/dash-router-core/src/node.rs` + `lib.rs` (make two helpers `pub`, add `LogRanges::remove`)

**Interfaces:**
- Consumes: `RouterMachine`/`RouterState`/`RouterAction`/`Effect` (core), `eviction_candidates` (Task 2), async storage traits + `MemStore` (Task 6), `is_lan`/`Incoming` (Task 8), `IntervalPolicy`/`PushDebouncePolicy` (Task 1).
- Produces (Tasks 10–11 consume):
  - core: `pub fn group_ops<L: PartialEq>(ops: Vec<(L, Seq, Op)>) -> Vec<(L, Vec<(Seq, Op)>)>` and `pub fn ranges_of<L: Ord + Clone>(parked: &BTreeMap<(L, Seq), Op>) -> LogRanges<L>` (visibility change only), `LogRanges::remove(&mut self, log: &L) -> Option<Ranges>`;
  - `trait IntervalSource { fn next_want(&mut self) -> Duration; fn next_have(&mut self) -> Duration; }` + `PolicyIntervals { want, have: IntervalPolicy, n: usize, rng: StdRng }`;
  - `CoreConfig { router: RouterConfig<RealTime>, relay_cap: Units, evict_at: f64, debounce: PushDebouncePolicy }`;
  - `NodeCore<N, L, E, R, I>` with `new(id, config, subscriptions, ext, relay, intervals)`, `async init()`, `fn next_deadline() -> Option<Duration>`, and async `advance_to(now)`, `on_wire(now, Incoming)`, `on_append(now, log, seq, op)`, `on_subscribe(now, log)`, `on_unsubscribe(now, log)`, `on_hint(now, BTreeSet<L>)`, `on_maintain(now)` — each returning `Result<Vec<Out<N, L>>>`;
  - `enum Out<N, L: Ord> { Broadcast(WireMessage<N, L>), Event(RouterEvent<L>) }`;
  - `enum RouterEvent<L> { Delivered(L, Seq), StorageError(StorageErrorReport) }`, `StorageErrorReport { pub context: &'static str, pub message: String }`.

**Semantics that bind (the reviewer checks each against `NodeMachine` in `crates/dash-router-core/src/node.rs`):**
1. All times are `Duration` since the shell's epoch. `advance_to` never `Tick`s past an armed timer or a due push flush; when a timer is due it fires it, routes the fx, and re-arms (`FireWant` → always re-arm want; `FireHave` → re-arm only if `router.wants` is non-empty, mirroring `ArmHaveTimer`'s guard).
2. A received Have parks ALL bytes, hands the router only `ranges_of(parked)`, ingests all parked (subscribed → ext; else relay, shed when `usage + ingest_delta > cap`), reconciles held for the touched logs, then routes the router's fx — the exact order of `NodeMachine`'s `Recv` arm.
3. Hydration merges relay + ext fetches, sorts `(log, seq)` with the payload-bearing copy first, dedups, groups with the shared `group_ops` — byte-for-byte the `SendHave` arm of `route_router_fx`.
4. Error posture (spec §3 [approved]): relay-store errors count in `relay_errors` and behave as sheds/evictions; ext-store errors become `Out::Event(StorageError)` while the method keeps going — EXCEPT `on_append`, whose ext-ingest failure fails the command (returns `Err`; the caller replies to the embedder and the loop continues). `held_of`/`held_all` read errors leave the stale `held_cache` standing (no `Held` snapshot that turn).
5. `Push` is debounced (spec §4): `on_append` ingests + reconciles immediately (so the node never Wants its own data) but only accumulates `pending_push`; the flush (at `PushDebouncePolicy::deadline(oldest_pending, latest_append)`) runs `RouterAction::Push` and broadcasts the hydrated Have.
6. Maintenance mirrors the sim's Task 4 policy exactly: at/over `evict_at * relay_cap`, evict `eviction_candidates(relay.held_payloads(), router.others_wants())` payloads-first (no `Held` needed — headers survive); when candidates are empty, evict `relay.held_all() − others_wants` fully and reconcile the touched logs.
7. Unsubscribe (kept-as-is ruling): remove the subscription and reconcile; if data is still held the log keeps advertising AND wanting. Reconcile drops a log's marker only when it is empty AND unsubscribed.
8. `on_wire` drops without state change: non-LAN `Some(remote)`, undecodable bytes, and `sender == self` echoes (`dropped_msgs` counts all three).

- [ ] **Step 1: Core edits (with the workspace green after)**

In `crates/dash-router-core/src/node.rs`, make `group_ops` and `ranges_of` `pub` (doc comments noting they are shared with the shell so grouping cannot drift); export both from `lib.rs`. In `ranges.rs`, add to `LogRanges`:

```rust
/// Forget a log entirely (its "known" marker included).
pub fn remove(&mut self, log: &L) -> Option<Ranges> {
    self.0.remove(log)
}
```

Run: `cargo test --workspace` — PASS.

- [ ] **Step 2: Write `handle.rs` (types only this task)**

```rust
//! The embedding API's data types (spec §5). Task 10 adds the command
//! channel and `RouterHandle`.

use dash_router_core::Seq;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouterEvent<L> {
    /// Novel subscribed data landed in the ext store. No bytes: the
    /// embedder owns that store; handing bytes again would invite a
    /// second source of truth [approved].
    Delivered(L, Seq),
    /// The ext store (the embedder's data path) failed; the node keeps
    /// gossiping from what it has [approved: degrade, don't crash].
    StorageError(StorageErrorReport),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageErrorReport {
    pub context: &'static str,
    pub message: String,
}
```

- [ ] **Step 3: Write the failing tests** (`#[cfg(test)]` in `shell.rs`; helpers shown once, reused by all)

```rust
use std::collections::BTreeSet;
use std::time::Duration;

use dash_router_core::{
    EvictableStorage, LogRanges, Op, OpsMap, Ranges, RouterConfig, Storage, WireBody, WireMessage,
};
// NOTE: OpsMap has both the sync traits and (via the blanket bridge) the
// async ones; assertions below use UFCS on the sync traits to disambiguate.

use super::*;
use crate::handle::RouterEvent;
use crate::transport::Incoming;

/// Deterministic intervals for tests: pops from the front, repeats the
/// last entry forever.
struct Scripted(Vec<Duration>, usize);
impl Scripted {
    fn ms(script: &[u64]) -> Self {
        Scripted(script.iter().map(|&m| Duration::from_millis(m)).collect(), 0)
    }
    fn next(&mut self) -> Duration {
        let i = self.1.min(self.0.len() - 1);
        self.1 += 1;
        self.0[i]
    }
}
impl IntervalSource for Scripted {
    fn next_want(&mut self) -> Duration {
        self.next()
    }
    fn next_have(&mut self) -> Duration {
        self.next()
    }
}

type Core = NodeCore<u32, u8, OpsMap<u8>, OpsMap<u8>, Scripted>;

fn config(cap: Units) -> CoreConfig {
    CoreConfig {
        router: RouterConfig {
            want_ttl: Duration::from_millis(500).into(),
            have_ttl: Duration::from_millis(500).into(),
        },
        relay_cap: cap,
        evict_at: 0.75,
        debounce: PushDebouncePolicy { window_ms: 100, max_latency_ms: 250 },
    }
}

async fn core(cap: Units, subs: &[u8]) -> Core {
    let mut c = NodeCore::new(
        0u32,
        config(cap),
        subs.iter().copied().collect::<BTreeSet<u8>>(),
        OpsMap::default(),
        OpsMap::default(),
        Scripted::ms(&[100]),
    );
    c.init().await.unwrap();
    c
}

fn op(h: u8, payload: bool) -> Op {
    Op { header: vec![h], payload: payload.then(|| vec![h; 4]) }
}

fn wire(msg: WireMessage<u32, u8>) -> Incoming {
    Incoming { remote: Some("192.168.0.9".parse().unwrap()), bytes: msg.encode() }
}

fn broadcasts(out: &[Out<u32, u8>]) -> Vec<&WireMessage<u32, u8>> {
    out.iter()
        .filter_map(|o| match o {
            Out::Broadcast(m) => Some(m),
            _ => None,
        })
        .collect()
}

fn delivered(out: &[Out<u32, u8>]) -> Vec<(u8, u32)> {
    out.iter()
        .filter_map(|o| match o {
            Out::Event(RouterEvent::Delivered(l, s)) => Some((*l, *s)),
            _ => None,
        })
        .collect()
}

/// Mirror of `recv_have_routes_bytes_delivers_and_rebroadcasts_hydrated`
/// in dash-router-core/tests/node.rs, over the async routing table.
#[tokio::test]
async fn recv_have_delivers_subscribed_and_rebroadcasts_hydrated() {
    let mut c = core(100, &[0]).await;
    let have = WireMessage::have(
        7,
        vec![(0u8, vec![(0, op(1, true))]), (1u8, vec![(0, op(2, true))])],
    );
    let out = c.on_wire(Duration::ZERO, wire(have)).await.unwrap();
    assert_eq!(delivered(&out), vec![(0, 0)], "only the subscribed log delivers");
    let bs = broadcasts(&out);
    assert_eq!(bs.len(), 1, "the flood relays once, hydrated");
    let WireBody::Have(groups) = &bs[0].body else { panic!("expected Have") };
    assert_eq!(groups.len(), 2, "both logs rebroadcast");
    // Subscribed bytes in ext, the rest in the relay.
    assert!(Storage::held_all(&c.ext).contains(&0, 0));
    assert!(Storage::held_all(&c.relay).contains(&1, 0));
}

/// Shed-at-cap: parked ops that don't fit are dropped silently and the
/// rebroadcast carries only what was stored (the documented truncation).
#[tokio::test]
async fn relay_sheds_at_cap_and_broadcasts_only_stored_ops() {
    let mut c = core(2, &[]).await; // room for exactly one payload op
    let have = WireMessage::have(7, vec![(1u8, vec![(0, op(1, true)), (1, op(2, true))])]);
    let out = c.on_wire(Duration::ZERO, wire(have)).await.unwrap();
    assert_eq!(EvictableStorage::usage(&c.relay), 2, "one op stored, one shed");
    let bs = broadcasts(&out);
    let WireBody::Have(groups) = &bs[0].body else { panic!() };
    assert_eq!(groups[0].1.len(), 1, "hydration only finds the stored op");
}

/// Push debounce: appends accumulate; the flush pushes one hydrated Have
/// carrying every pending op; a steady stream is capped by max_latency.
#[tokio::test]
async fn append_debounce_flushes_one_have_for_the_batch() {
    let mut c = core(100, &[0]).await;
    let ms = Duration::from_millis;
    let out = c.on_append(ms(0), 0, 0, op(1, true)).await.unwrap();
    assert!(broadcasts(&out).is_empty(), "no immediate push");
    let out = c.on_append(ms(50), 0, 1, op(2, true)).await.unwrap();
    assert!(broadcasts(&out).is_empty());
    assert_eq!(c.next_deadline(), Some(ms(100)), "want timer at 100 ties the flush window 50+100; flush due at 150");
    let out = c.advance_to(ms(150)).await.unwrap();
    let haves: Vec<_> = broadcasts(&out)
        .into_iter()
        .filter(|m| matches!(m.body, WireBody::Have(_)))
        .collect();
    assert_eq!(haves.len(), 1, "one flush for the whole batch");
    let WireBody::Have(groups) = &haves[0].body else { panic!() };
    assert_eq!(groups[0].1.len(), 2, "both appends carried");
}

/// The want timer fires on schedule and a peer's Want arms the have timer;
/// the eventual Have reply is hydrated from storage.
#[tokio::test]
async fn want_fire_and_have_reply_flow() {
    let mut c = core(100, &[0]).await; // empty subscribed log: wants everything
    let out = c.advance_to(Duration::from_millis(100)).await.unwrap();
    let bs = broadcasts(&out);
    assert!(
        matches!(&bs[0].body, WireBody::Want(r) if r.get(&0) == Some(&Ranges::full())),
        "fresh subscription wants the whole log"
    );
    // Seed storage, then a peer wants it.
    let _ = c.on_append(Duration::from_millis(100), 0, 0, op(1, true)).await.unwrap();
    let want = WireMessage::want(7, LogRanges::from_pairs([(0u8, Ranges::full())]));
    let out = c.on_wire(Duration::from_millis(110), wire(want)).await.unwrap();
    assert!(broadcasts(&out).iter().any(|m| matches!(m.body, WireBody::Want(_))), "the Want floods on");
    assert!(c.router.have_timer.is_some(), "witnessed Want arms the have timer");
    let out = c.advance_to(Duration::from_millis(400)).await.unwrap();
    assert!(
        broadcasts(&out).iter().any(|m| matches!(&m.body, WireBody::Have(g) if !g.is_empty())),
        "the reply is hydrated"
    );
}

/// Maintenance evicts payloads nobody wants once usage crosses the line.
#[tokio::test]
async fn maintain_evicts_payloads_first() {
    let mut c = core(4, &[]).await;
    let have = WireMessage::have(7, vec![(1u8, vec![(0, op(1, true)), (1, op(2, true))])]);
    let _ = c.on_wire(Duration::ZERO, wire(have)).await.unwrap();
    assert_eq!(EvictableStorage::usage(&c.relay), 4);
    let _ = c.on_maintain(Duration::from_millis(1)).await.unwrap();
    assert_eq!(EvictableStorage::usage(&c.relay), 2, "payloads evicted, headers kept");
    assert!(EvictableStorage::held_payloads(&c.relay).is_empty());
    assert!(!Storage::held_all(&c.relay).is_empty(), "still advertising headers");
}

/// Non-LAN senders, garbage bytes, and own echoes are dropped statelessly.
#[tokio::test]
async fn on_wire_drops_foreign_garbage_and_echoes() {
    let mut c = core(100, &[0]).await;
    let held = c.router.held.clone();
    let foreign = Incoming {
        remote: Some("8.8.8.8".parse().unwrap()),
        bytes: WireMessage::have(7, vec![(0u8, vec![(0, op(1, true))])]).encode(),
    };
    let garbage = Incoming { remote: Some("192.168.0.9".parse().unwrap()), bytes: vec![0xff, 0x00] };
    let echo = wire(WireMessage::want(0, LogRanges::from_pairs([(0u8, Ranges::full())])));
    for inc in [foreign, garbage, echo] {
        assert!(c.on_wire(Duration::ZERO, inc).await.unwrap().is_empty());
    }
    assert_eq!(c.dropped_msgs, 3);
    assert_eq!(c.router.held, held, "no state change from dropped input");
}
```

- [ ] **Step 4: Run: `cargo test -p dash-router shell`** — expected: FAIL (nothing implemented).

- [ ] **Step 5: Implement `shell.rs`**

Module doc: cite spec §2 (one owner, pure transitions inside) and the deliberate-duplication note (§3). Definitions:

```rust
pub trait IntervalSource {
    fn next_want(&mut self) -> Duration;
    fn next_have(&mut self) -> Duration;
}

/// Production intervals: the policy crate sampled with a seeded RNG.
/// `n` is the network-size estimate (a constant until the protocol grows
/// an estimator — the sim uses the true size as an oracle).
pub struct PolicyIntervals {
    pub want: IntervalPolicy,
    pub have: IntervalPolicy,
    pub n: usize,
    pub rng: rand::rngs::StdRng,
}
// impl IntervalSource by sampling.

#[derive(Clone, Debug)]
pub struct CoreConfig {
    pub router: RouterConfig<RealTime>,
    pub relay_cap: Units,
    /// Maintenance evicts once relay usage reaches `evict_at * relay_cap`.
    pub evict_at: f64,
    pub debounce: PushDebouncePolicy,
}

pub enum Out<N, L: Ord> {
    Broadcast(WireMessage<N, L>),
    Event(RouterEvent<L>),
}

pub struct NodeCore<N, L: Ord, E, R, I> {
    machine: RouterMachine<N, L, RealTime>,
    pub router: RouterState<N, L, RealTime>,
    pub ext: E,
    pub relay: R,
    pub subscriptions: BTreeSet<L>,
    /// Derived: last known ext ∪ relay ∪ empty markers for subscriptions.
    held_cache: LogRanges<L>,
    intervals: I,
    debounce: PushDebouncePolicy,
    relay_cap: Units,
    evict_at: f64,
    pending_push: LogRanges<L>,
    pending_since: Option<Duration>,
    latest_append: Duration,
    /// Shell time the router has been ticked up to.
    now: Duration,
    pub dropped_msgs: u64,
    pub relay_errors: u64,
}
```

`advance_to` — the tick/fire engine (binding semantics #1; the conformance driver replicates this loop verbatim against the reference, so keep it exactly this shape):

```rust
pub async fn advance_to(&mut self, now: Duration) -> anyhow::Result<Vec<Out<N, L>>> {
    let mut out = Vec::new();
    loop {
        // 1. Fire everything due at the current instant.
        if let Some(oldest) = self.pending_since
            && self.debounce.deadline(oldest, self.latest_append) <= self.now
        {
            let fx = self.flush_push()?;
            self.route_fx(fx, &BTreeMap::new(), &mut out).await?;
            continue;
        }
        if self.router.want_timer.as_ref().is_some_and(|t| t.remaining.is_zero()) {
            let fx = self.router_step(RouterAction::FireWant)?;
            self.route_fx(fx, &BTreeMap::new(), &mut out).await?;
            let next = self.intervals.next_want();
            self.router_step(RouterAction::ArmWantTimer(next.into()))?;
            continue;
        }
        if self.router.have_timer.as_ref().is_some_and(|t| t.remaining.is_zero()) {
            let fx = self.router_step(RouterAction::FireHave)?;
            self.route_fx(fx, &BTreeMap::new(), &mut out).await?;
            if !self.router.wants.is_empty() {
                let next = self.intervals.next_have();
                self.router_step(RouterAction::ArmHaveTimer(next.into()))?;
            }
            continue;
        }
        // 2. Caught up?
        if self.now >= now {
            break;
        }
        // 3. Tick to the nearest of: target, armed timers, push deadline.
        //    (All are strictly ahead of self.now after step 1.)
        let mut step = now - self.now;
        for t in [&self.router.want_timer, &self.router.have_timer].into_iter().flatten() {
            step = step.min(*t.remaining);
        }
        if let Some(oldest) = self.pending_since {
            step = step.min(self.debounce.deadline(oldest, self.latest_append) - self.now);
        }
        self.router_step(RouterAction::Tick(step.into()))?;
        self.now += step;
    }
    Ok(out)
}
```

`router_step` clones `self.router`, calls `self.machine.transition`, writes back, returns fx (same clone-based stepping as `NodeMachine`, same rationale). `flush_push` takes `pending_push`/clears `pending_since` and runs `RouterAction::Push` when non-empty. `route_fx`, `ingest_parked`, `hydrate`, `reconcile_held(touched: Option<&BTreeSet<L>>, out)` follow binding semantics #2–#4 above, transcribing `NodeMachine::route_router_fx`/`ingest_parked`/`held_union` with awaits — hydration uses the now-`pub` `group_ops`; Have parking uses the now-`pub` `ranges_of`. Reconcile's patch rule (semantics #7): per touched log, `merged = ext.held_of ∪ relay.held_of`; empty AND unsubscribed → `held_cache.remove(log)`; else `held_cache.insert(log, merged)`; full rebuild (`None`) is `held_all ∪ held_all` plus empty markers for subscriptions; both end with `RouterAction::Held(held_cache.clone())` — skipped (stale cache stands) when a read errored. `on_wire`/`on_append`/`on_subscribe`/`on_unsubscribe`/`on_hint`/`on_maintain` per binding semantics #2, #5, #6, #8 (subscribe migrates relay→ext then evicts the log from relay, as `NodeAction::Subscribe` does). Every `on_*` method starts with `advance_to(now)` and appends to its output.

- [ ] **Step 6: Run: `cargo test -p dash-router`** — expected: PASS. Then `cargo test --workspace` — PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/dash-router crates/dash-router-core
git commit -m "feat(net): NodeCore, the async routing table with debounced push and maintenance"
```

---

### Task 10: `spawn` — the select loop, `RouterHandle`, end-to-end loopback

The thin rind (spec §2's five sources, §5's API): one tokio task around `NodeCore`, a command handle, an event stream.

**Files:**
- Modify: `crates/dash-router/src/handle.rs` (Command, RouterHandle)
- Modify: `crates/dash-router/src/shell.rs` (spawn + the loop)
- Modify: `crates/dash-router/src/lib.rs` (re-export `spawn`, `RouterHandle`, `Command`)
- Test: `crates/dash-router/tests/loop.rs`

**Interfaces:**
- Consumes: everything from Tasks 6–9.
- Produces (the embedding API, spec §5):

```rust
pub enum Command<L> {
    Append { log: L, seq: Seq, op: Op, reply: oneshot::Sender<anyhow::Result<()>> },
    Subscribe { log: L, reply: oneshot::Sender<anyhow::Result<()>> },
    Unsubscribe { log: L, reply: oneshot::Sender<anyhow::Result<()>> },
    Shutdown,
}

pub struct RouterHandle<L> { /* mpsc::Sender<Command<L>> */ }
impl<L: Send> RouterHandle<L> {
    pub async fn append(&self, log: L, seq: Seq, op: Op) -> anyhow::Result<()>;
    pub async fn subscribe(&self, log: L) -> anyhow::Result<()>;
    pub async fn unsubscribe(&self, log: L) -> anyhow::Result<()>;
    pub async fn shutdown(self) -> anyhow::Result<()>;
}

pub fn spawn<N, L, E, R, T, I>(
    id: N,
    config: CoreConfig,
    maintain_interval: Duration,
    subscriptions: BTreeSet<L>,
    ext: E,     // WatchableStorage + Send + 'static
    relay: R,   // AsyncEvictableStorage + Send + 'static
    transport: T,
    intervals: I,
) -> (RouterHandle<L>, mpsc::Receiver<RouterEvent<L>>, JoinHandle<anyhow::Result<()>>)
```

- [ ] **Step 1: Write the failing e2e tests** (`crates/dash-router/tests/loop.rs`; both `#[tokio::test(start_paused = true)]` — virtual time auto-advances when the runtime is idle, so real seconds never pass)

```rust
//! Two real shells over the loopback transport: the whole §2 select loop,
//! end to end, no p2panda.

use std::collections::BTreeSet;
use std::time::Duration;

use dash_router_core::{Op, OpsMap, RouterConfig, Storage, Units};
// The relay store is a plain OpsMap through the blanket sync bridge:
// MemStore is the *watchable ext* store and implements no eviction.
use dash_router::{
    CoreConfig, LoopbackHub, MemStore, PolicyIntervals, RouterEvent, spawn,
};
use dash_router_policy::{IntervalPolicy, PushDebouncePolicy};
use rand::SeedableRng;

fn config() -> CoreConfig {
    CoreConfig {
        router: RouterConfig {
            want_ttl: Duration::from_millis(500).into(),
            have_ttl: Duration::from_millis(500).into(),
        },
        relay_cap: 1024 as Units,
        evict_at: 0.75,
        debounce: PushDebouncePolicy { window_ms: 50, max_latency_ms: 200 },
    }
}

fn intervals(seed: u64) -> PolicyIntervals {
    PolicyIntervals {
        want: IntervalPolicy::Fixed { min_ms: 100.0, max_ms: 200.0 },
        have: IntervalPolicy::Fixed { min_ms: 20.0, max_ms: 60.0 },
        n: 2,
        rng: rand::rngs::StdRng::seed_from_u64(seed),
    }
}

async fn next_delivery(events: &mut tokio::sync::mpsc::Receiver<RouterEvent<u8>>) -> (u8, u32) {
    loop {
        match tokio::time::timeout(Duration::from_secs(60), events.recv())
            .await
            .expect("delivery within virtual 60s")
            .expect("event stream open")
        {
            RouterEvent::Delivered(l, s) => return (l, s),
            RouterEvent::StorageError(e) => panic!("unexpected storage error: {e:?}"),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn push_reaches_the_other_shell() {
    let hub = LoopbackHub::new();
    let b_ext = MemStore::<u8>::new();
    let (a, _a_events, _a_task) = {
        let (h, e, t) = spawn(
            1u32, config(), Duration::from_secs(1),
            BTreeSet::from([0u8]), MemStore::new(), OpsMap::default(),
            hub.join("192.168.0.1".parse().unwrap()), intervals(1),
        );
        (h, e, t)
    };
    let (_b, mut b_events, _b_task) = spawn(
        2u32, config(), Duration::from_secs(1),
        BTreeSet::from([0u8]), b_ext.clone(), OpsMap::default(),
        hub.join("192.168.0.2".parse().unwrap()), intervals(2),
    );

    let op = Op { header: vec![1], payload: Some(vec![9; 16]) };
    a.append(0, 0, op.clone()).await.unwrap();
    assert_eq!(next_delivery(&mut b_events).await, (0, 0));
    assert!(
        Storage::held_all(&b_ext.snapshot()).contains(&0, 0),
        "bytes are in B's own store"
    );
}

#[tokio::test(start_paused = true)]
async fn late_joiner_repairs_via_want() {
    let hub = LoopbackHub::new();
    let (a, _a_events, _a_task) = spawn(
        1u32, config(), Duration::from_secs(1),
        BTreeSet::from([0u8]), MemStore::<u8>::new(), OpsMap::default(),
        hub.join("192.168.0.1".parse().unwrap()), intervals(1),
    );
    a.append(0, 0, Op { header: vec![1], payload: Some(vec![7]) }).await.unwrap();
    a.append(0, 1, Op { header: vec![2], payload: Some(vec![8]) }).await.unwrap();
    // Let the push flood into an empty room.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // C joins afterwards: only Want/repair can teach it.
    let (_c, mut c_events, _c_task) = spawn(
        3u32, config(), Duration::from_secs(1),
        BTreeSet::from([0u8]), MemStore::new(), OpsMap::default(),
        hub.join("192.168.0.3".parse().unwrap()), intervals(3),
    );
    let mut got = BTreeSet::new();
    got.insert(next_delivery(&mut c_events).await);
    got.insert(next_delivery(&mut c_events).await);
    assert_eq!(got, BTreeSet::from([(0u8, 0u32), (0, 1)]));
}
```

- [ ] **Step 2: Run: `cargo test -p dash-router --test loop`** — expected: FAIL (`spawn` missing).

- [ ] **Step 3: Implement `handle.rs` commands + `RouterHandle`** (oneshot round-trips; `shutdown` sends `Command::Shutdown` and treats a closed channel as already-down).

- [ ] **Step 4: Implement `spawn` in `shell.rs`**

The loop (spec §2's five sources; each arm: compute `now = epoch.elapsed()`, call the `NodeCore` method, route the outputs):

- Take `hints = ext.changed()` before constructing the core (the core owns `ext`; the sender inside it keeps the channel alive).
- `core.init().await?`, then loop over `tokio::select!`:
  1. `cmd_rx.recv()`: `None`/`Shutdown` → break (drain nothing — spec §5: durable state is already in the stores; router state is deliberately ephemeral). `Append` → `on_append`; `Ok(outs)` replies `Ok(())` and routes, `Err(e)` replies `Err(e)` and continues (degrade posture).
  2. `transport.recv()`: `None` → break; `Some(inc)` → `on_wire`.
  3. `hints.recv()`: `Ok(logs)` → `on_hint(now, logs)`; `Err(Lagged)` → `on_hint(now, BTreeSet::new())` (empty = re-read everything — hints are lossy-mergeable); `Err(Closed)` is unreachable while the core holds the store.
  4. `sleep`: `tokio::time::sleep_until(epoch_instant + deadline)` for `core.next_deadline()` (a far-future fallback when `None`) → `advance_to(now)`.
  5. `maintain.tick()` (a `tokio::time::interval` with `MissedTickBehavior::Delay`) → `on_maintain(now)`.
- Routing outputs: `Out::Broadcast(msg)` → `transport.broadcast(msg.encode()).await` (a send error breaks the loop — the transport is gone); `Out::Event(e)` → `event_tx.send(e).await` ignoring a closed receiver (an embedder that dropped the stream still gets gossip).

- [ ] **Step 5: Run: `cargo test -p dash-router`** — expected: PASS (unit + e2e). Then `cargo test --workspace`.

- [ ] **Step 6: Commit**

```bash
git add crates/dash-router
git commit -m "feat(net): spawn select loop, RouterHandle, loopback end-to-end tests"
```

---

### Task 11: Lockstep conformance — `NodeCore` vs `NodeMachine`

Where the deliberately duplicated routing table earns its keep (spec §8). Proptest drives the same action sequence into the real shell core (over sync `OpsMap`s via the blanket bridge, zero-window debounce) and the pure `NodeMachine`, comparing state projections and effect streams after every step.

**Deviation from spec §8, on the record:** plain `proptest` with a generated `Vec<Step>`, not the `proptest-state-machine` crate — the reference model IS our state machine, so the crate's Reference/SUT scaffolding would duplicate what `NodeMachine` already is; plain sequences shrink fine and keep the harness ~200 lines. The spec's intent (lockstep per action, projected-state equality) is implemented exactly.

**Zero-debounce ruling (carried from the design):** conformance runs with `PushDebouncePolicy { window_ms: 0, max_latency_ms: 0 }`, so an `Append` step maps to the reference's atomic `NodeAction::Authored`. The debounce policy itself is pure-tested (Task 1) and e2e-tested (Task 10); lockstep checks routing equivalence, not batching.

**Files:**
- Test: `crates/dash-router/tests/conformance.rs`

**Interfaces:**
- Consumes: `NodeCore` + `Scripted`-style intervals (Task 9), `NodeMachine`/`NodeState`/`NodeAction` (core), blanket sync bridge (Task 6).
- Produces: nothing new — a test.

- [ ] **Step 1: Write the harness and a fixed regression sequence first**

Types: `N = u32`, `L = u8`, `T = RealTime`. Step vocabulary and mapping:

```rust
#[derive(Clone, Debug)]
enum Step {
    RecvWant { from: u32, log: u8, start: u32, end: u32 },
    RecvHave { from: u32, log: u8, seqs: Vec<(u32, bool)> }, // (seq, has_payload)
    Append { log: u8 },                                      // seq = next per log
    Subscribe(u8),
    Unsubscribe(u8),
    Advance(u64),                                            // ms
}
```

- Both sides start identically: SUT `NodeCore::new(0, config, subs, OpsMap::default(), OpsMap::default(), script)` + `init()`; reference `NodeState::new(0, subs)` + `NodeAction::Router(ArmWantTimer(script'))` where `script` and `script'` are equal cloned interval sequences.
- `RecvWant`/`RecvHave` → SUT `on_wire(now, Incoming { remote: Some(lan_ip), bytes: msg.encode() })`; reference `NodeAction::Recv(msg)` — the SAME `WireMessage` value. After a `RecvWant`, mirror the SUT's have-arming: if the reference's `router.have_timer` is `None` and `router.wants` is non-empty, apply `NodeAction::Router(ArmHaveTimer(script'.next_have()))`.
- `Append` → SUT `on_append(now, log, seq, op)` then `advance_to(now)` (zero debounce flushes immediately); reference `NodeAction::Authored(log, seq, op)`. Ops are deterministic: `Op { header: vec![log, seq as u8], payload: Some(vec![seq as u8]) }`.
- `Subscribe`/`Unsubscribe` → the matching `NodeAction`.
- `Advance(ms)` → SUT `advance_to(now + ms)`; reference: replicate `advance_to`'s loop verbatim with `NodeAction::Router(...)` actions (`Tick` in the same chunks — stop at each timer's remaining — then `FireWant`+`ArmWantTimer(script'.next_want())` / `FireHave` (+`ArmHaveTimer` iff `wants` non-empty) when due). Identical chunking is guaranteed because both sides stop at the same timer boundaries; if they ever disagree, the projection comparison fails — which is the point.
- After EVERY step compare:

```rust
fn project(s: &RouterState<u32, u8, RealTime>) -> impl PartialEq + Debug {
    (
        s.held.clone(),
        s.wants.clone(),
        s.haves.clone(),
        s.relayed_want_ranges(), // normalized: the SUT may split records
        s.relayed_have_ranges(), // differently across pushes
        s.want_timer.clone(),
        s.have_timer.clone(),
    )
}
```

  plus `core.ext.held_all() == ref.ext.0.held_all()`, same for relay, `core.subscriptions == ref.subscriptions`, and the per-step effect streams: SUT `Out::Broadcast(m)` multiset == reference `NodeEffect::Broadcast(m)` multiset (sort both `Vec<WireMessage>`s — `WireMessage: Ord`), SUT `Delivered` set == reference `NodeEffect::Deliver` set.
- Reference transitions that return `Err` (e.g. a relay-cap `ensure`) must not happen: the mapping only produces enabled actions (the shed path is inside `ingest_parked` on both sides). Any reference `Err` fails the test with the step index.
- Wrap the whole sequence in `tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(...)` inside the proptest closure.

Write one `#[test] fn fixed_regression_sequence()` first with a hand-picked ~10-step sequence covering: subscribe, append, advance-past-want-fire, recv want, advance-past-have-fire, recv have (mixed subscribed/unsubscribed logs), unsubscribe, advance past TTL expiry.

- [ ] **Step 2: Run it** — expected: PASS if Task 9 transcribed faithfully; any failure here is a real divergence — debug it now with the fixed sequence before adding proptest noise. Do not weaken the projection to make it pass; fix the shell (or, if the shell is right and the model wrong, STOP and escalate — that contradicts Task 9's review).

- [ ] **Step 3: Add the property**

```rust
proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..Default::default() })]
    #[test]
    fn shell_matches_the_node_machine(
        steps in proptest::collection::vec(step_strategy(), 1..40),
        intervals in proptest::collection::vec(20u64..400, 4..16),
        subs in proptest::collection::btree_set(0u8..3, 0..3),
    ) { run_lockstep(subs, intervals, steps)?; }
}
```

`step_strategy()`: `from: 1..4u32`, `log: 0..3u8`, seqs/starts `0..8u32`, `Advance(10..600ms)`, weighted roughly 3:3:2:1:1:2 across the six variants.

- [ ] **Step 4: Run: `cargo test -p dash-router --test conformance`** — expected: PASS, ~64 cases.

- [ ] **Step 5: Commit**

```bash
git add crates/dash-router
git commit -m "test(net): lockstep conformance of NodeCore against NodeMachine"
```

---

### Task 12: p2panda transport (feature-gated) + closeout

The real transport behind the boundary (spec §6.1), compiled only under the `p2panda` feature so the default workspace stays light; then the docs closeout.

**Files:**
- Create: `crates/dash-router/src/panda.rs`
- Modify: `crates/dash-router/Cargo.toml` (feature + optional deps)
- Modify: `crates/dash-router/src/lib.rs` (`#[cfg(feature = "p2panda")] pub mod panda;`)
- Modify: `docs/superpowers/specs/2026-09-21-real-world-shell-design.md` (status + resolved decisions)

**Interfaces:**
- Consumes: `Transport`/`Incoming` (Task 8), `WIRE_VERSION` (core).
- Produces: `PandaTransport` implementing `Transport`; `pub async fn spawn_panda(private_key: p2panda_core::PrivateKey) -> anyhow::Result<(PandaTransport, p2panda_core::PublicKey)>` (the returned key is the node's wire identity `N`).

**p2panda-net 0.7 notes for the implementer (verify against docs.rs/p2panda-net/0.7 — the builder API is the current rewrite):** construction chains `AddressBook::builder().spawn()` → `Endpoint::builder(address_book).spawn()` → `MdnsDiscovery::builder(address_book, endpoint).spawn()` (LAN scoping by construction — mDNS only) → `Gossip::builder(address_book, endpoint).spawn()`; a topic is joined via the gossip handle's `stream(topic)`, published to with `publish(bytes)`, received via its subscription stream. The topic id derives from the string `"dash-router/v0"` (hash to the 32-byte topic type with p2panda's own hash; the suffix bumps with `WIRE_VERSION` — add a compile-time reminder next to the constant: `const _: () = assert!(WIRE_VERSION == 0, "bump the gossip topic suffix with the wire version");`). If the delivering peer's socket address is exposed on receive, carry it as `Incoming::remote: Some(ip)`; otherwise `remote: None` is correct — the mDNS-scoped overlay is the membership boundary (spec §6.1, unsigned-wire [approved] rationale). If a signature or bound doesn't line up (e.g. `PublicKey` serde), wrap in a local newtype rather than changing core.

- [ ] **Step 1: Feature + deps**

```toml
[features]
p2panda = ["dep:p2panda-net", "dep:p2panda-core"]

[dependencies]
p2panda-net = { version = "0.7", optional = true }
p2panda-core = { version = "0.7", optional = true }
```

(If the builders need more of the tokio feature set, widen the crate's tokio features rather than adding a second tokio dep.)

- [ ] **Step 2: Implement `panda.rs`** — `spawn_panda` wires the builders on the well-known topic and returns the transport + own public key; `impl Transport for PandaTransport` maps `broadcast` → publish and `recv` → next gossip payload as `Incoming`. Module doc records the trust stance: no signatures in v1, LAN membership is the boundary [approved].

- [ ] **Step 3: Smoke test** (in `panda.rs`, `#[cfg(test)]`):

```rust
#[tokio::test(flavor = "multi_thread")]
#[ignore = "binds real sockets and mDNS; run manually: cargo test -p dash-router --features p2panda -- --ignored"]
async fn two_panda_nodes_gossip_on_localhost() { /* two spawn_panda nodes,
    node A broadcasts a probe, node B receives it within a 30s timeout */ }
```

- [ ] **Step 4: Build both ways**

Run: `cargo build -p dash-router` and `cargo build -p dash-router --features p2panda` and `cargo clippy --workspace --all-targets` — all clean. Run the ignored smoke test once manually; if the environment blocks sockets/mDNS, note the failure mode in the commit message rather than faking it.

- [ ] **Step 5: Docs closeout**

In `docs/superpowers/specs/2026-09-21-real-world-shell-design.md`: change the Status line to `**Status: ACCEPTED, implemented by docs/superpowers/plans/2026-09-21-real-world-shell.md.**` and append a short **Resolved** block: the five §9 answers (all as drafted, user-approved 2026-09-21), plus the relay-pull ruling (keep DESIGN.md wanting semantics as-is; shed-at-cap churn and Unsubscribe keep-wanting deferred, user-chosen 2026-09-21), plus the two implementation deviations if they occurred (plain proptest for §8; zero-debounce lockstep).

- [ ] **Step 6: Final gate**

Run: `cargo test --workspace && cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings`
Expected: all clean (run `cargo fmt --all` first if needed).

- [ ] **Step 7: Commit**

```bash
git add crates/dash-router docs Cargo.lock
git commit -m "feat(net): feature-gated p2panda transport; spec closeout"
```
