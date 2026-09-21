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
    pub relayed_haves: Vec<Record<L, T>>,      // renamed from `relayed`
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
    Push(LogRanges<L>),                       // client-initiated fast Have
}
```

Removed: `Append`/`Authored` (subsumed by `Push` + ingest, see §2.4),
`Recv(MessageEnvelope)` (split into the two ranges-only variants; op bytes
never enter the router), and any notion of `fresh`.

`Push(ranges)` is the author-side fast path: it emits
`SendHave(ranges − relayed_haves)` immediately and notes the emission in
`relayed_haves`, exactly as a relay would. No label travels on the wire —
receivers treat a pushed Have identically to a slow-repair Have (it floods
by the same seen-set rules). Debouncing/coalescing of rapid appends is the
*caller's* timing policy (glue/shell), like interval sampling — the router
stays policy-free.

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

### 2.4 Removal of the "fresh" label (push stays)

With flooding, `fresh` was already inert as a relay signal, so the *label*
goes: no wire field, no receiver-side distinction, no separate emission
rules in the router. What stays is the author-side **push**: on authoring,
the client initiates a fast Have via `Push` (previous section), which is
treated by every receiver exactly like a slow-repair Have. New-data
propagation latency stays at ~debounce + flood time; the pull path
(Want-triggered Haves) remains the repair mechanism it always was.

`Authored` as a distinct action dissolves into two existing mechanisms: the
op bytes reach storage by ingest (whose `HeldChanged` → `Held` snapshot
grows the router's view), and the fast announcement is a `Push`. Native
sync and migration use the first mechanism only — no push, since p2panda
already carried the data to whoever it could.

DESIGN.md's message enum (`fresh` field) and §2 need a corresponding edit:
the emission behavior survives, the freshness *distinction* does not
(listed as a work item, §9).

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
    /// Held ranges (header at least) for exactly the given logs. A
    /// requested log with nothing held appears with an empty range —
    /// presence in the result mirrors the request, so the caller can
    /// patch its per-log cache without ambiguity.
    fn held_of(&self, logs: &BTreeSet<L>) -> LogRanges<L>;

    /// Held ranges for ALL logs. Startup and full-resync only — never
    /// the hot path. Keys may map to empty ranges ("log known here,
    /// nothing held").
    fn held_all(&self) -> LogRanges<L>;

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

    /// Ranges for which the full payload is held (⊆ held). Feeds the
    /// eviction policy (payloads drop first). Deliberately NOT on
    /// `Storage`: only the eviction policy consumes it, so the external
    /// store — the implementation we don't control — never pays for it.
    fn held_payloads(&self) -> LogRanges<L>;

    /// Drop payloads (keep headers) for held ops in `ranges`.
    fn evict_payloads(&mut self, ranges: &LogRanges<L>);

    /// Drop ops entirely (headers too) in `ranges`.
    fn evict(&mut self, ranges: &LogRanges<L>);
}
```

Design notes, in anticipation of revision:

- **The held queries are summary reads, not scans — that is a contract.**
  Implementations are expected to maintain the ranges summary
  incrementally (updated on every ingest/evict), so `held_of` is a lookup
  and even `held_all` is O(size of the summary). The summary is small by
  nature: `Ranges` grows with the number of *contiguous runs* (i.e. with
  fragmentation), not with op count — a million-op log with no gaps is
  one pair of numbers. A store that must scan to answer is a
  non-conforming (but functional) implementation.
- **`held_of` is the hot path; `held_all` is startup.** Change
  notifications carry the touched logs (§3.3), so steady-state
  reconciliation re-reads only those logs and patches the glue's per-log
  cache; the full query runs once at startup and on lost-hint resync.
- **`fetch` is total, not fallible.** Absence is data (the protocol expects
  gaps); only the async mirror adds an error channel, for I/O failure.
- **No `contains`/point queries.** Every consumer works in ranges; a point
  query is `fetch` of a unit range.
- **No transactionality.** Each method is atomic on its own; the glue never
  needs multi-call atomicity because `Held` snapshots are idempotent
  reconciliation, not a ledger. If an ingest lands and the process dies
  before the router hears, the next snapshot repairs it.
- **No change notification here.** Notification is push-shaped and
  async-native; it lives in the shell trait (§3.3) and, in the model, in
  the storage machines' effects. Putting a callback in the sync trait
  would smuggle I/O shape into the core.

### 3.3 Async mirror (shell crate)

```rust
#[async_trait]
pub trait AsyncStorage<L: Ord>: Send + Sync {
    async fn held_of(&self, logs: &BTreeSet<L>) -> Result<LogRanges<L>>;
    async fn held_all(&self) -> Result<LogRanges<L>>;
    async fn fetch(&self, ranges: &LogRanges<L>) -> Result<Vec<(L, Seq, Op)>>;
    async fn ingest(&self, log: L, seq: Seq, op: Op) -> Result<()>;
}

#[async_trait]
pub trait AsyncEvictableStorage<L>: AsyncStorage<L> {
    async fn usage(&self) -> Result<Units>;
    async fn held_payloads(&self) -> Result<LogRanges<L>>;
    async fn evict_payloads(&self, ranges: &LogRanges<L>) -> Result<()>;
    async fn evict(&self, ranges: &LogRanges<L>) -> Result<()>;
}

/// Implemented by stores that change behind Dash Router's back
/// (Dash Chat's store, written by p2panda native sync and GC'd by the
/// app). Each item is a HINT naming the logs that changed: the shell
/// re-reads held_of(those logs) and patches its cached union before
/// snapshotting Held to the router. An EMPTY set means "unknown —
/// re-read everything" (held_all). Because reconciliation is
/// re-read-and-patch, coalescing or lossily merging notifications is
/// always safe: one hint after N writes yields one correct patch.
pub trait WatchableStorage<L>: AsyncStorage<L> {
    fn changed(&self) -> impl Stream<Item = BTreeSet<L>> + Send;
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
a simple one. Keeping it as a genuine external store (rather than folding
authored ops into the relay store) preserves the authored/relayed
distinction — GC exemption, Deliver, and the push path all key off it. The
"null" degenerate case is a pure relay that subscribes to nothing — that is
just an external store whose held ranges are forever empty.

### 3.5 Relation to p2panda-store / p2panda-sync

Reuse happens **inside the adapter, not in the trait**. Reasons the trait
stays ours:

- p2panda's `LogRanges` is one contiguous `(from, until)` per log and
  cannot express our gapped `Ranges`; adopting it would lose exactly the
  expressiveness DESIGN.md's diff arithmetic depends on.
- p2panda-store's `OperationStore`/`LogStore` traits are keyed by structured
  `p2panda_core::Header`s (public key, seq_num, hashes); our `Op` is
  deliberately opaque bytes ("both payloads opaque to relays in general").
  Binding the core trait to those types would drag p2panda into
  `dash-router-core` and break relay opacity.
- p2panda-sync's session-oriented `SyncProtocol` machinery is the wrong
  shape for an open-ended broadcast protocol; nothing there maps.

What *is* worth building once, by us rather than by every embedder: a
`dash-router-p2panda` adapter (crate or module in `dash-router-net`)
implementing `AsyncStorage + WatchableStorage` generically over
p2panda-store's traits — the held-summary maintenance, the header/body ↔
`Op` packing, and the change-hint stream live there. **To verify at impl
time** (§10): whether current p2panda-store exposes any change
notification; if not, the `changed()` stream needs a small hook on Dash
Chat's side (it knows when it writes) plus a coarse poll as fallback for
native-sync writes.

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
their own `held_all()`, whenever it changes. (The model machines emit full
snapshots; the shell's hint-based partial re-reads in §3.3 reconstruct the
same value through the per-log cache.) Cap enforcement (`relay_cap`, in
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
| local append (standalone) / Dash Chat "authored" | `ext.ingest` (→ `HeldChanged` → `Held`), then router `Push(authored ranges)` — debounced/coalesced by the caller — whose `SendHave` hydrates and broadcasts as usual |

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
4. **`WatchableStorage::changed`** streams: on a hint, `held_of` the named
   logs (or `held_all` on an empty hint), patch the shell's per-log held
   cache, snapshot `Held` to the router. The cache is derived state — it
   never appears in the conformance projection.

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
  push-path propagation latency vs. pull-path repair latency (now separable,
  since push is an explicit `Push` action), eviction churn under cap
  pressure.

## 9. Work plan

1. **Core refactor**: storage-less `RouterState` (`held` with empty-key
   interest), actions/effects per §2 (incl. `Push` and the
   `relayed` → `relayed_haves` rename), delete `fresh` everywhere, rework
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
   command API; lockstep conformance test; the `dash-router-p2panda`
   adapter (§3.5).
8. **DESIGN.md edit**: remove `fresh` from the message enum and reword §2
   (push survives unlabeled).

Each step leaves the workspace green before the next begins.

## 10. Open questions (tracked, not blocking)

- Disk engine for the relay store (redb vs fjall vs sqlite) — decide at
  step 7; the trait insulates everything else.
- Whether `IntervalPolicy` moves to core or a small `dash-router-policy`
  crate — decide at step 6 when the sim and shell both need it.
- Streaming `fetch` for large hydrations — revisit if sim shows Have sizes
  that make `Vec` collection hurt.
- Whether current p2panda-store exposes any change-notification mechanism
  (§3.5) — verify against the real API at step 7; fallback is a Dash
  Chat-side write hook plus a coarse poll for native-sync writes.
- Push debounce policy (how long to coalesce rapid appends before `Push`)
  — a shell/sim tuning knob alongside the interval policies.
