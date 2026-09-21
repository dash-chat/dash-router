# Storage Boundary and Tokio Shell

**Status:** draft for review
**Date:** 2026-09-21
**Scope:** the storage-less router refactor, the two storage machines, the
`Storage` trait, the `NodeMachine` glue, the wire format, and the
`dash-router-net` tokio shell over p2panda. Out of scope: the `Rewire`
net-model action, interval retuning, `dash-router-model-check`.

## 1. Goal

Integrate Dash Router with Dash Chat's p2panda store (the "selfish store")
while keeping the sans-I/O core exactly identical between model, simulation,
and production. The load-bearing decision: **the router holds no op bytes and
knows nothing about storage, subscriptions, or wire format.** It is a pure
gossip brain over `LogRanges`. Everything else is layered around it.

```
┌────────────────────────────────────────────────────────────┐
│ dash-router-net (tokio shell)                              │
│   gossip pub/sub · timers · async Storage impls · postcard │
├────────────────────────────────────────────────────────────┤
│ NodeMachine (pure glue; the routing table)                 │
│   subscriptions · byte parking · hydration · eviction      │
│   ┌──────────────┐ ┌───────────────────┐ ┌───────────────┐ │
│   │ RouterMachine│ │ RelayStoreMachine │ │ ExtStoreMachine│ │
│   │ (decisions,  │ │ (Ingest, Evict)   │ │ (Ingest,       │ │
│   │  ranges only)│ │                   │ │  NativeSync,   │ │
│   └──────────────┘ └───────────────────┘ │  AppGc)        │ │
│                                          └───────────────┘ │
├────────────────────────────────────────────────────────────┤
│ dash-router-net-model / dash-router-sim                    │
│   drive NodeMachine per node; in-memory storage machines   │
└────────────────────────────────────────────────────────────┘
```

The same `NodeMachine` routing table has two executors: the pure composed
machine (model, sim, conformance reference) and the async shell (production).
The only code the model does not cover is the shell's async plumbing and the
disk-backed store implementations.

## 2. RouterMachine: storage-less

### 2.1 State

```rust
pub struct RouterState<N, L, T> {
    pub id: N,
    /// Everything this node holds (header at least), across all storage,
    /// as reported by the layer above. A key with an EMPTY range means
    /// "I know about this log and hold none of it" — this is how interest
    /// in a freshly subscribed log is expressed, and it is what makes the
    /// next Want ask for the whole log. known_logs() = held.keys().
    pub held: LogRanges<L>,
    pub wants: BTreeMap<N, Record<L, T>>,      // unchanged
    pub haves: BTreeMap<N, Record<L, T>>,      // unchanged
    pub relayed: Vec<Record<L, T>>,            // unchanged
    pub relayed_wants: Vec<Record<L, T>>,      // unchanged
    pub want_timer: Option<Timer<T>>,          // unchanged
    pub have_timer: Option<Timer<T>>,          // unchanged
}
```

Removed: `subscriptions`, `store`. The router no longer knows which ops are
"its own"; Deliver routing, GC exemption, and migration are all above it.
`RouterConfig` keeps `want_ttl` and `have_ttl`; `relay_cap` moves to the
relay store (§4).

`held` is maintained two ways, both idempotent:
- **Optimistically**: `Accept` (below) grows it immediately, so back-to-back
  receives dedup correctly without waiting for storage.
- **Authoritatively**: the `Held(LogRanges)` action replaces it wholesale
  (absolute snapshot, never a delta). Shrinks — relay eviction, app-side GC,
  data loss — need no special handling; the next snapshot is simply smaller.

Consequence, accepted: `held` may briefly over-claim (advertise a range that
storage just evicted). Hydration then sends what actually exists and the
protocol's normal repair covers the gap. "A Have never advertises what the
sender lacks" weakens from invariant to eventually-true; this is honest,
because a router and a disk store genuinely are two components with a
consistency boundary.

### 2.2 Actions

```rust
pub enum RouterAction<N, L, T> {
    Tick(T),                                  // unchanged semantics
    ArmWantTimer(T),                          // unchanged
    ArmHaveTimer(T),                          // unchanged
    FireWant,                                 // unchanged
    FireHave,                                 // unchanged
    RecvWant { from: N, ranges: LogRanges<L> },
    RecvHave { from: N, ranges: LogRanges<L> },
    Held(LogRanges<L>),                       // absolute snapshot from above
}
```

Removed: `Append`/`Authored` (see §2.4), `Recv(MessageEnvelope)` (split into
the two ranges-only variants; op bytes never enter the router), and any
notion of `fresh`.

### 2.3 Effects

```rust
pub enum Effect<L> {
    SendWant(LogRanges<L>),
    SendHave(LogRanges<L>),        // ranges ARE the hydration request
    Accept(LogRanges<L>),          // novel received ranges; layer above
                                   // ingests the parked bytes and routes
}
```

Removed: `Send(MessageEnvelope)` (the router no longer builds wire
messages), `Store`, `Deliver`, `Migrate` (all storage routing is above).
Note `Effect` loses its `N` parameter: sender identity is stamped on at the
wire layer.

**Ordering contract:** effects are applied in list order. `RecvHave` emits
`Accept(novel)` before `SendHave(relay_portion)`, so by the time the glue
hydrates the relayed flood, the parked bytes are already ingested.

### 2.4 Removal of "fresh", and its consequence

With flooding, `fresh` was already inert as a relay signal. Removing the
*notion* removes more: the author-side push path (DESIGN.md §2's debounced
fresh Have on authoring). The protocol becomes **pull-only for new data**:
an authored op sits until some node's Want interval elapses, the Want
floods, and the author (or any holder) answers with a Have. Worst-case
propagation latency for new data rises from ~have-debounce to
~(want interval + have interval + flood time). At chat-scale intervals
(hundreds of ms) this is acceptable; the sim will quantify it, and the push
path can be reintroduced later as a pure emission-policy change if latency
matters (it would not touch storage or wire format).

This also deletes `Authored`: authorship reaches the router the same way
native sync does — the external store grows, a `Held` snapshot arrives, and
the complement shrinks. One mechanism for all three of {local append,
p2panda native sync, migration}.

DESIGN.md §2 and the `fresh` field in the message enum need a corresponding
edit (listed as a work item, §9).

## 3. The `Storage` trait

This is the seam Dash Chat implements, so it gets the fullest treatment.
Two layers: a **sync-shaped core trait** (implemented by the model/sim
in-memory stores, used by the pure `NodeMachine`) and an **async mirror in
the shell** (implemented by the disk-backed relay store and by Dash Chat's
p2panda wrapper).

### 3.1 Core types

`Op`, `Seq`, `Ranges`, `LogRanges` stay in `dash-router-core::types`; the
`router` module no longer references `Op`. `Op` remains
`{ header: Bytes, payload: Option<Bytes> }` — payload separately droppable,
per DESIGN.md GC.

```rust
/// Cap-accounting units: 2 for an op with payload, 1 header-only.
pub type Units = u64;
```

### 3.2 Sync trait (core; no I/O deps)

```rust
/// Read/write surface of one op store. Implemented by the in-memory
/// model/sim stores; mirrored by AsyncStorage in the shell.
pub trait Storage<L: Ord> {
    /// Ranges for which AT LEAST the header is held. Keys may map to
    /// empty ranges: "this log exists here, nothing held" (used to
    /// express interest for freshly subscribed logs). This is the value
    /// that feeds the router's Held snapshots (unioned across stores).
    fn held(&self) -> LogRanges<L>;

    /// Ranges for which the full payload is held. Always ⊆ held().
    fn held_payloads(&self) -> LogRanges<L>;

    /// Every held op intersecting `ranges`, in (log, seq) order.
    /// Gaps and evicted payloads are silently reflected in the output
    /// (missing entries / payload: None) — fetch never fails on absence.
    /// This is hydration: SendWant/SendHave ranges go in, ops come out.
    fn fetch(&self, ranges: &LogRanges<L>) -> Vec<(L, Seq, Op)>;

    /// Idempotent. Re-ingesting a held op is a no-op, EXCEPT that an op
    /// carrying a payload upgrades a held header-only op. Never evicts.
    fn ingest(&mut self, log: L, seq: Seq, op: Op);
}

/// The relay store additionally submits to glue-driven eviction.
/// The external store does NOT implement this: its shrinkage is
/// spontaneous (AppGc), never commanded by Dash Router.
pub trait EvictableStorage<L: Ord>: Storage<L> {
    /// Current usage in cap units. Cheap; called on every ingest cycle.
    fn usage(&self) -> Units;

    /// Drop payloads (keep headers) for held ops in `ranges`.
    fn evict_payloads(&mut self, ranges: &LogRanges<L>);

    /// Drop ops entirely (headers too) in `ranges`.
    fn evict(&mut self, ranges: &LogRanges<L>);
}
```

Design notes, in anticipation of revision:

- **`fetch` is total, not fallible.** Absence is data (the protocol expects
  gaps); only the async mirror adds an error channel, for I/O failure.
- **No `contains`/point queries.** Every consumer works in ranges; a point
  query is `fetch` of a unit range. Keeps the trait at four methods.
- **No transactionality.** Each method is atomic on its own; the glue never
  needs multi-call atomicity because `Held` snapshots are idempotent
  reconciliation, not a ledger. If an ingest lands and the process dies
  before the router hears, the next snapshot repairs it.
- **`held_payloads` earns its place** twice: eviction policy (payloads
  drop first, so the policy needs to know which ranges still have them)
  and Have hydration honesty (a header-only op is still advertisable and
  fetchable; the receiver sees `payload: None` and may Want it again
  later — same semantics as today's GC).
- **No change notification here.** Notification is push-shaped and
  async-native; it lives in the shell trait (§3.3) and, in the model, in
  the storage machines' effects. Putting a callback in the sync trait
  would smuggle I/O shape into the core.

### 3.3 Async mirror (shell crate)

```rust
#[async_trait]
pub trait AsyncStorage<L: Ord>: Send + Sync {
    async fn held(&self) -> Result<LogRanges<L>>;
    async fn held_payloads(&self) -> Result<LogRanges<L>>;
    async fn fetch(&self, ranges: &LogRanges<L>) -> Result<Vec<(L, Seq, Op)>>;
    async fn ingest(&self, log: L, seq: Seq, op: Op) -> Result<()>;
}

#[async_trait]
pub trait AsyncEvictableStorage<L>: AsyncStorage<L> {
    async fn usage(&self) -> Result<Units>;
    async fn evict_payloads(&self, ranges: &LogRanges<L>) -> Result<()>;
    async fn evict(&self, ranges: &LogRanges<L>) -> Result<()>;
}

/// Implemented by stores that change behind Dash Router's back
/// (Dash Chat's store, written by p2panda native sync and GC'd by the
/// app). The notification carries no data: on any change the shell
/// re-reads held() and snapshots the union to the router. Coalescing,
/// lossy notification is therefore fine — one notification after N
/// writes produces one correct snapshot.
pub trait WatchableStorage<L>: AsyncStorage<L> {
    fn changed(&self) -> impl Stream<Item = ()> + Send;
}
```

- A blanket impl lifts any sync `Storage` into `AsyncStorage`
  (for tests and the standalone in-memory case).
- v1 `fetch` returns a `Vec`; if hydrating large backfills proves heavy,
  it becomes a stream — a shell-only change, invisible to the core.

### 3.4 Who implements what

| Implementation | Traits | Lives in | Used by |
|---|---|---|---|
| `ExtStoreState` (BTreeMap) | `Storage` | core | model, sim, conformance |
| `RelayStoreState` (BTreeMap) | `Storage + Evictable` | core | model, sim, conformance |
| Relay disk store (redb/fjall/sqlite, TBD at impl time) | `AsyncStorage + AsyncEvictable` | dash-router-net | production, both modes |
| Dash Chat's p2panda wrapper | `AsyncStorage + Watchable` | Dash Chat | integrated production |
| Standalone selfish store | `AsyncStorage + Watchable` | dash-router-net | standalone production |

**Standalone is not the null store.** A standalone node still authors and
subscribes, so it still needs a selfish-side store; `dash-router-net` ships
a simple one. The "null" degenerate case is a pure relay that subscribes to
nothing — that is just an external store whose `held()` is forever empty.

## 4. The storage machines (model/sim)

Two distinct machines, one trait. They share an internal ops-map helper
struct for the BTreeMap plumbing, but their action vocabularies differ
because their behaviors differ, and in a polestar model the action set *is*
the model. Merging them would give the relay store phantom spontaneous
actions or hide two models behind a mode flag.

```rust
// RelayStoreMachine — does only what the glue tells it.
enum RelayStoreAction<L> {
    Ingest(L, Seq, Op),
    EvictPayloads(LogRanges<L>),   // nondeterministic in model checking:
    Evict(LogRanges<L>),           // "any evictable range" — the model
}                                  // verifies the protocol under EVERY
                                   // eviction policy; sim plugs in the real one

// ExtStoreMachine — an environment actor wearing a storage interface.
enum ExtStoreAction<L> {
    Ingest(L, Seq, Op),            // glue-driven (flow-back, migration, append)
    NativeSync(L, Seq, Op),        // spontaneous: p2panda synced it directly
    AppGc(LogRanges<L>),           // spontaneous: the app dropped data
}
```

Both emit one effect: `HeldChanged(LogRanges<L>)`, an absolute snapshot of
their own `held()`, whenever it changes. Cap enforcement (`relay_cap`, in
`Units`) is `RelayStoreMachine` config; the model checks "usage never
exceeds cap" as an invariant of that machine, composed.

## 5. NodeMachine: the glue, and the routing table

Pure composition; this is the reference for conformance and the shape the
shell reimplements.

```rust
struct NodeState<N, L, T> {
    router: StateMachine<RouterMachine<N, L, T>>,
    relay:  StateMachine<RelayStoreMachine<L>>,
    ext:    StateMachine<ExtStoreMachine<L>>,
    subscriptions: BTreeSet<L>,          // live HERE, not in the router
}

enum NodeAction<N, L, T> {
    Router(RouterAction<N, L, T>),       // incl. wire-derived RecvWant/RecvHave
    Subscribe(L),
    Unsubscribe(L),
    Ext(ExtStoreAction<L>),              // NativeSync / AppGc proposable;
                                         // Ingest is internal-only
    RelayEvict(LogRanges<L>),            // model: nondeterministic
}

enum NodeEffect<L> {
    Broadcast(WireMessage<L>),           // fully hydrated, ready for the LAN
    Deliver(L, Seq),                     // subscribed op reached this node
}
```

Routing table (each row is a pure function of the composed state; the shell
implements the same rows with `.await`s):

| Trigger | Glue does |
|---|---|
| wire message arrives | parse; **park the op bytes**; feed router `RecvWant`/`RecvHave` (ranges only) |
| router fx `Accept(ranges)` | split by `subscriptions`; ingest parked bytes → ext (subscribed, then emit `Deliver`) / relay (rest); run eviction policy if relay over cap |
| router fx `SendWant(r)` | wrap as wire message, `Broadcast` |
| router fx `SendHave(r)` | hydrate: `relay.fetch(r) ∪ ext.fetch(r)`; wrap, `Broadcast` |
| any `HeldChanged` from either store | router `Held(relay.held ∪ ext.held ∪ empty-keys for subscriptions)` |
| `Subscribe(L)` | add to set; migrate: `relay.fetch(L-range)` → `ext.ingest` → `relay.evict(L-range)`; snapshot `Held` |
| `Unsubscribe(L)` | remove from set; nothing else (option-1 semantics: keep advertising until the app actually drops the data, at which point `AppGc` → shrinking snapshot ends it naturally) |
| local append (standalone) / Dash Chat "authored" | `ext.ingest`; the resulting `HeldChanged` → `Held` is the router's only notification |

Eviction policy (sim/shell): a pure function
`eviction_candidates(usage, cap, relay_held, relay_payloads, recency) -> LogRanges`
implementing DESIGN.md's payloads-oldest-first-then-headers, where `recency`
("least recently mentioned") comes from a pure read of the router's
`wants`/`haves` records — the same sibling-state-read trick as hydration.

Invariant worth checking at this level: **`Subscribe`/`Unsubscribe` never
change the router's `held`** — now true by construction (the router has no
subscribe action), so the check is really that migration is range-preserving
across the two stores.

## 6. Wire format

Module `dash_router_core::wire` (serde + postcard deps in core are
acceptable; no I/O). p2panda uses postcard, so we do too.

```rust
/// The versioned, self-describing LAN broadcast payload.
struct WireMessage<N, L> {
    version: u8,          // bump on breaking change, together with the topic
    sender: N,            // gossip strips transport sender; we carry our own
    body: WireBody<L>,
}
enum WireBody<L> {
    Want(LogRanges<L>),
    Have(Vec<(L, Seq, Op)>),   // no `fresh` field; ops may be payload-less
}
```

- Broadcast over one well-known constant gossip topic whose name embeds the
  protocol version ("changing along with protocol breaking changes").
- Unknown `version` ⇒ drop silently.
- No conversion to p2panda's `LogRanges` is needed anywhere: we never call
  p2panda-sync's APIs; ephemeral gossip streams carry our own postcard
  bytes. (This deletes a work item from the original report.)
- Wire round-trip proptests live beside the module; they need no tokio.

## 7. dash-router-net: the tokio shell

One task per node, structured as `tokio::select!` over:

1. **Gossip subscription** (p2panda ephemeral stream, well-known topic):
   decode → LAN-boundary check on the transport address (private ranges per
   DESIGN.md; drop otherwise) → park bytes → router `RecvWant/RecvHave` →
   route fx per §5.
2. **Timer queue** (`TimerMap`): due entries re-inject `FireWant`/`FireHave`;
   after each fire, sample the `IntervalPolicy` (moved from
   `dash-router-sim` into a shared home, §9) and re-arm via
   `ArmWantTimer`/`ArmHaveTimer`. A `TickBuffer` discretises elapsed wall
   time into `Tick(RealTime)` before every action batch.
3. **Command channel** (the embedding API): append, subscribe, unsubscribe.
4. **`WatchableStorage::changed`** streams: on any tick, re-read `held()`
   from both stores, snapshot `Held` to the router.

Startup sequence: load subscriptions from the embedder (their persistence is
the app's job — Dash Chat re-subscribes on startup) → initial `Held`
snapshot → arm the first Want timer. The relay store's disk contents survive
restarts and simply appear in that first snapshot; nothing else special.

Discovery (mDNS/swarm-discovery) and transport are p2panda-net's job; the
shell's only network-shaped logic is the LAN-boundary predicate and the
topic name.

## 8. Conformance and properties

- **Lockstep** (`proptest-state-machine`): reference = `NodeMachine`
  (the full composition, so the routing table itself is covered); SUT =
  the shell glue driven over an in-memory transport with the blanket
  sync→async storage impls; `check_invariants` projects shell state and
  asserts equality with the composed model state. This is where
  `map_state` finally gets asserted.
- **Model checking** (later crate, but the properties are fixed now):
  relay usage ≤ cap; no double `Deliver`; hydration honesty is eventual
  (a `held` over-claim is always corrected by the next snapshot); every
  subscribed node's ext store eventually reaches the max authored seq
  under fair schedules; `NativeSync` racing an in-flight Want never
  double-delivers; migration preserves `held`.
- **Sim**: existing scenarios rerun on the new composition; new metrics:
  propagation latency of authored ops under pull-only (the §2.4
  consequence, quantified), eviction churn under cap pressure.

## 9. Work plan

1. **Core refactor**: storage-less `RouterState` (`held` with empty-key
   interest), actions/effects per §2, delete `fresh` everywhere, rework
   core tests to ranges-only. The biggest diff; almost all deletion.
2. **`types`/`wire` modules**: move `Op` out of `router`; `WireMessage` +
   postcard round-trip tests.
3. **Storage**: sync traits + `RelayStoreState`/`ExtStoreState` + the two
   machines, unit tests for idempotent ingest, payload upgrade, eviction
   accounting.
4. **`NodeMachine`** in core: routing table + tests for each row; the
   migration and pull-only-propagation scenarios as machine-level tests.
5. **net-model update**: `NetState.nodes` becomes `NodeMachine` state
   machines; `Broadcast` absorbed into flights; tests updated.
6. **sim update**: behavior proposes `NodeAction`s (incl. `NativeSync`/
   `AppGc`/`RelayEvict` with policy); rerun scenarios, record the new
   baseline, quantify §2.4.
7. **`dash-router-net`**: async traits, shell loop, standalone stores,
   command API; lockstep conformance test.
8. **DESIGN.md edit**: remove `fresh` from the message enum and §2, note
   pull-only propagation.

Each step leaves the workspace green before the next begins.

## 10. Open questions (tracked, not blocking)

- Disk engine for the relay store (redb vs fjall vs sqlite) — decide at
  step 7; the trait insulates everything else.
- Whether `IntervalPolicy` moves to core or a small `dash-router-policy`
  crate — decide at step 6 when the sim and shell both need it.
- Streaming `fetch` for large hydrations — revisit if sim shows Have sizes
  that make `Vec` collection hurt.
- Reintroducing an author-push path if measured propagation latency under
  pull-only is unacceptable — a pure emission-policy change, deliberately
  left out per YAGNI.
