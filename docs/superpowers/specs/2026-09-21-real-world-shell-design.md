# Real World: the `dash-router-net` Tokio Shell — design draft

**Status: DRAFT for review.** Expands §3.3, §3.5, §7, §8 and the §10 open
questions of `2026-09-21-storage-and-shell-design.md` into a concrete
design. Where §10 left a question open, this draft takes a position and
marks it **[decision]** — each is up for challenge. Nothing here changes
the pure world; the pure-world plan executes independently.

## 1. Shape and scope

Two new crates:

- **`dash-router-net`** — the tokio shell: node task, async storage traits,
  standalone stores (in-memory selfish store, disk relay store), command
  API, lockstep conformance tests. Depends on `dash-router-core`, tokio,
  postcard; p2panda only behind the transport boundary (§6).
- **`dash-router-policy`** — tiny no-tokio crate holding `IntervalPolicy`
  (moved from `dash-router-sim/src/policy.rs`) and the push-debounce
  policy. Rationale **[decision]**: the sim and the shell both need it;
  core forbids RNG so it can't live there without polluting the purity
  story; making sim depend on the shell would drag tokio into the sim.
  A ~100-line crate is the honest home.

Out of scope for this round: the `dash-router-p2panda` adapter's
*implementation* (designed here at the interface level, §6.3, built when
Dash Chat integration starts) and `dash-router-model-check`.

**Inherited follow-ups from the pure-world final review** (this plan's
scope, not optional): the eviction subsystem is currently unreachable at
the composition level and must become real here — add an
`EvictPayloads`-tier `NodeAction` (the payloads-first, headers-later GC
from DESIGN.md), implement `eviction_candidates` (the pure policy over
`held_payloads()` + router recency), give the sim a cap-pressure scenario
with a realistic `relay_cap` and behavior proposals for
`RelayEvict`/`NativeSync`/`AppGc`, and add the push-vs-pull propagation
latency metric (separable at the action level even though the wire has no
marker). Also on the table for this round: whether `Unsubscribe` should
keep *wanting* an unsubscribed log's open tail (today it does — tested,
documented — making the log a perpetual pull source into the capped relay
until `AppGc`), and the shed-at-cap churn loop the spec now documents in
§5 (a saturated relay Wants, receives, and re-sheds the same data each
`have_ttl`).

## 2. The node task: one owner, pure transitions inside

One tokio task owns everything mutable — no locks, no shared state. The
shell is a thin imperative rind around the same pure `RouterMachine`
transitions the model uses:

```rust
pub struct NodeShell<L, E, R> {
    router: RouterMachine<PublicKey, L, RealTime>,   // config
    state: RouterState<PublicKey, L, RealTime>,      // the pure state, verbatim
    ext: E,            // AsyncStorage + WatchableStorage (Dash Chat's, or standalone)
    relay: R,          // AsyncStorage + AsyncEvictableStorage (disk)
    subscriptions: BTreeSet<L>,
    held_cache: LogRanges<L>,   // derived: last known ext ∪ relay ∪ empty-sub keys
    ticks: TickBuffer,          // wall clock → Tick(RealTime) discretisation
    timers: TimerMap<TimerKind> // Want / Have / PushDebounce deadlines
}
```

The loop is `tokio::select!` over five sources; every branch reduces to
"build one or more `RouterAction`s, run the pure transition, route the
effects with `.await`s":

1. **Gossip inbound**: decode `WireMessage` (postcard, version check) →
   LAN-boundary check on the transport address (drop silently otherwise) →
   `RecvWant`/`RecvHave`. For a Have, park the bytes, hand the router only
   ranges, then run the routing table (§3).
2. **Timer queue**: due `Want`/`Have` deadlines re-inject
   `FireWant`/`FireHave`; after each fire, sample `IntervalPolicy` and
   re-arm. A due `PushDebounce` deadline flushes the pending push set (§4).
3. **Command channel** (§5): `Append`, `Subscribe`, `Unsubscribe`,
   `Shutdown`.
4. **`ext.changed()` hints**: re-read `held_of(hinted logs)` (or
   `held_all` on the empty hint), patch `held_cache`, snapshot
   `Held(held_cache)` to the router.
5. **Relay maintenance interval**: sample `usage()`; over cap → compute
   `eviction_candidates` (pure function over `state.wants`/`state.haves`
   plus `relay.held_payloads()`), evict, re-snapshot `Held`.

**Before every action batch** the `TickBuffer` converts elapsed wall time
into one `Tick(RealTime)` action, so timers and TTL decay behave exactly
as in the model. The seen-set representation stays the model's
countdown `Vec<Record>` — same state type, no shell-side divergence.
(An absolute-deadline `VecDeque` variant would only pay off if profiling
shows tick-decrement cost, and it would split the state representation
from the model's; parked unless measured. See pure-world review notes.)

## 3. The routing table, async edition

The shell reimplements `NodeMachine`'s routing table with `.await`s —
this duplication is deliberate (the model's glue is the reference; the
lockstep test in §8 checks they agree). Per received Have:

1. Park bytes as `BTreeMap<(L, Seq), Op>`.
2. `RecvHave { from, ranges }` → router fx.
3. Ingest **all** parked bytes: subscribed logs → `ext.ingest(...)`,
   others → relay if under cap, shed otherwise. (Idempotent ingest
   absorbs duplicates and payload upgrades.)
4. Re-read the touched logs' held (`held_of`), patch `held_cache`,
   `Held` snapshot.
5. Route fx in order: `Accept ∩ subscriptions` → deliver events on the
   delivery stream (§5); `SendWant(r)` → broadcast; `SendHave(r)` →
   hydrate from both stores (`fetch`, merge, dedup, group per log),
   broadcast — skip if hydration comes back empty.

**Storage errors** (the async traits are fallible; the sync model ones are
total): an `ingest`/`fetch` error on the **relay** store degrades — log,
skip that op (shedding is already legal, so a failed relay write is
indistinguishable from a shed); an error on the **ext** store is the
embedder's data path failing and is surfaced on the event stream as
`RouterEvent::StorageError` while the node keeps gossiping from what it
has. `held_of`/`held_all` errors retry with backoff; until a read
succeeds the stale `held_cache` stands (honest-eventually, same weakened
invariant the spec already accepts). **[decision]** — the alternative
(crash the node task on any storage error) is cleaner but turns a
transient disk hiccup into a LAN-visible outage.

## 4. Push debounce

`Append` commands do not push immediately: the shell accumulates appended
`(L, Seq)` into a pending `LogRanges` and arms a `PushDebounce` deadline
(policy: fixed short window, e.g. 50–200 ms, in `dash-router-policy`;
re-arming on further appends is **capped** by a max-latency bound so a
steady append stream still pushes). On flush: `Push(pending)` → router →
hydrated `SendHave` broadcast, pending cleared. Ops are already in the
ext store before the push (ingest happens at `Append` time), so a crash
between append and flush loses only the *push*, not the data — the next
Want/Have cycle repairs it.

## 5. The embedding API

```rust
pub struct RouterHandle<L> { /* mpsc::Sender<Command<L>> + shutdown */ }

impl RouterHandle<L> {
    pub async fn append(&self, log: L, seq: Seq, op: Op) -> Result<()>;
    pub async fn subscribe(&self, log: L) -> Result<()>;    // + relay→ext migration
    pub async fn unsubscribe(&self, log: L) -> Result<()>;  // keep-advertising semantics
    pub async fn shutdown(self) -> Result<()>;
}

pub enum RouterEvent<L> {
    Delivered(L, Seq),          // novel subscribed data landed in ext
    StorageError(StorageErrorReport),
}
// spawn returns (RouterHandle, impl Stream<Item = RouterEvent<L>>, JoinHandle)
```

`Delivered` carries `(L, Seq)` only — the bytes are already in the ext
store, which the embedder owns; handing bytes again would invite a second
source of truth. Subscription persistence is the embedder's job (spec §7):
`spawn` takes the initial subscription set.

Startup: initial `held_all` on both stores → first `Held` snapshot → arm
the first Want timer. Shutdown: drain the select loop, flush nothing (all
durable state is already in the stores; router state is rebuilt from
snapshots next start — wants/haves/seen-sets are deliberately ephemeral).

## 6. p2panda integration

### 6.1 Transport (in `dash-router-net`)

- Gossip via p2panda-net ephemeral streams on the well-known topic
  `"dash-router/v0"` — the version suffix bumps with `WIRE_VERSION`.
- LAN boundary: predicate over the remote's socket address against the
  three private IPv4 ranges (DESIGN.md); applied on receive *and* we rely
  on p2panda-net's local discovery (mDNS) so sends are LAN-scoped by
  construction. Non-LAN packets are dropped without decode.
- `WireMessage.sender` is the node's p2panda `PublicKey` (transport strips
  sender identity from gossip). No signature in v1: the LAN-membership
  boundary is the trust boundary, matching DESIGN.md's privacy stance.
  **[decision]** — signing every message is cheap to add later inside
  `WireMessage` without touching the core.

### 6.2 Standalone stores (in `dash-router-net`)

- **Selfish store**: in-memory `OpsMap` behind the blanket sync→async
  impl, plus a trivial `changed()` stream fed by its own writes.
- **Relay disk store**: **redb** **[decision]**. Reasons: pure Rust
  (no C toolchain), single-file, B-tree tables give ordered
  `(L, Seq)`-prefix range scans — which is exactly `held_all`/`fetch`'s
  access pattern — and its single-writer model matches the one-owner node
  task. fjall's LSM write throughput is wasted on our write rate; sqlite
  is a heavier dependency for a cache we're allowed to lose.
  Schema: one table `ops: (L: [u8; 32], seq: u32-BE) → (header, Option<payload>)`;
  `usage` and the held summary are computed by scan at startup and cached
  in memory thereafter (the spec's no-summary-contract allows this; the
  cache is maintained incrementally as an optimization since this store
  has a single writer — us).

### 6.3 `dash-router-p2panda` adapter (interface now, built at integration)

Implements `AsyncStorage + WatchableStorage` over p2panda-store's
`OperationStore`/`LogStore`: packs `Header` bytes + body into `Op`,
maps our `L` to p2panda log ids, derives held ranges from log heights +
gap scans. `changed()`: Dash Chat calls a write hook after its own writes
and after native-sync sessions complete; a coarse poll (tens of seconds)
backstops anything missed — safe because hints are lossy-mergeable by
design. **To verify at build time**: whether current p2panda-store gives
any notification we can subscribe to directly (spec §10 carry-over).

## 7. Crate/file layout

```
crates/dash-router-policy/src/lib.rs        IntervalPolicy, PushDebouncePolicy
crates/dash-router-net/src/
  storage.rs      AsyncStorage/AsyncEvictableStorage/WatchableStorage + blanket sync impl
  mem.rs          in-memory selfish store + changed() stream
  disk.rs         redb relay store
  shell.rs        NodeShell: select loop, routing table, TickBuffer/TimerMap
  handle.rs       RouterHandle, Command, RouterEvent
  transport.rs    Transport trait: broadcast/recv (p2panda impl + test loopback)
  lan.rs          LAN-boundary predicate
tests/
  conformance.rs  lockstep vs NodeMachine (proptest-state-machine)
  loop.rs         two shells over the loopback transport, end-to-end
```

`transport.rs`'s trait is what makes the conformance test possible: the
lockstep SUT runs the real shell over an in-channel loopback transport and
the blanket sync→async stores, no p2panda anywhere in the test.

## 8. Conformance

`proptest-state-machine` lockstep per spec §8: reference =
`NodeMachine`; SUT = `NodeShell` on a current-thread runtime, driven by
the same action sequence (commands, injected wire messages, manually
stepped time — the `TickBuffer` accepts a test clock). Projection:
`(router state, ext held, relay held, subscriptions)` — `held_cache` and
timer bookkeeping are derived and excluded. This is where the duplicated
routing table (§3) earns its keep or gets caught.

## 9. Open questions for this review

1. **Error posture** (§3): degrade-and-report vs crash-the-task on storage
   errors — I chose degrade; is that right for Dash Chat's ops model?
2. **redb** (§6.2) — any prior leaning toward fjall/sqlite?
3. **Unsigned wire messages** (§6.1) — acceptable for v1 given the LAN
   trust boundary?
4. **`Delivered(L, Seq)` without bytes** (§5) — does Dash Chat want the
   op bytes in the event instead of re-reading its own store?
5. **`dash-router-policy` as a third crate** (§1) vs folding policies into
   `dash-router-net` and letting the sim depend on a feature-gated subset.
