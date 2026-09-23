# Dash Chat LAN router integration — design

Hook a `dash-router` shell into a Dash Chat node so ops in the node's
p2panda store replicate to nearby devices over LAN gossip, without touching
the existing mailbox and log-sync paths. Work spans two repos: small
additions to `dash-router` (this repo) and one contained module in
`dash-chat` (`crates/dashchat-node/src/lan_router.rs`).

Follows the shell spec (`2026-09-21-real-world-shell-design.md`), whose
§6.3 sketched this adapter. Decisions marked **[approved]** were agreed in
the brainstorm on 2026-09-22.

## 1. Decisions

- **Share the p2panda node's gossip; never spawn a second p2panda stack
  [approved].** Dash Chat's actor privately owns the p2panda `Node`, which
  exposes `ephemeral_stream(topic)` but not its `Gossip` actor. A new actor
  command hands the router an ephemeral publisher/subscriber pair on the
  router topic; a new `GossipTransport` (§3.1) wraps that pair.
- **The ephemeral envelope is accepted [approved].** Each message costs a
  signed CBOR wrapper (~150 bytes) and in return the transport learns the
  verified author. `WireMessage.sender` stays in v1; the shell now *checks*
  it against the envelope author and drops mismatches (§4). Removing the
  field is a later, wire-breaking change.
- **Log identity is `(LogId, author)` and subscriptions name the LogId
  prefix [approved].** Dash Chat's LogId is `blake3(topic)`, one log per
  author per topic. Topics stay out of the router. A router subscription
  names a prefix (the LogId) and means every author's log under it, now
  and in the future (§3.5).
- **Serve only acked ops [approved].** The ext adapter mirrors
  `MailboxStore::get_log`: a log's held range stops at the acked height, so
  a body about to be tombstoned never leaves the device.
- **Relay store is the existing redb `DiskRelayStore` [approved]**, at
  `<data>/lan_router.redb`.
- **Everything is a no-op when `NodeConfig.enable_lan_router` is false
  [approved]**, and the whole module sits behind a `lan-router` cargo
  feature on `dashchat-node`. The config field exists regardless of the
  feature; with the feature off it is ignored.
- **The shell crate is renamed `dash-router-net` → `dash-router`
  [approved].** It is the crate embedders use; core, policy and the models
  are its internals. It re-exports `dash_router_core` and
  `dash_router_policy` so an embedder depends on one crate.
  `dash-router-net-model` keeps its name: it models the *network*
  (broadcast, loss, reordering), not the net crate.
- **Every topic is routed, inbox topics included [approved].** With
  prefix subscriptions this needs nothing special: Dash Chat subscribes to
  `LogId::from_topic(topic)` once per topic. A contact request from an
  author the owner has not met yet (the advertised inbox; the QR exchange
  is one-way) is simply another log under a subscribed prefix, and a relay
  that holds it can serve it even if the owner was off the LAN when it was
  sent.

## 2. Dependency wiring (dash-chat)

`crates/dashchat-node/Cargo.toml`:

```toml
[features]
lan-router = ["dep:dash-router"]

[dependencies]
dash-router = { git = "https://github.com/maackle/dash-router", optional = true, features = ["p2panda"] }
```

(A `path` dep during development; switch to `git` before merge.) Core and
policy types come through `dash_router::core` and `dash_router::policy`.

Workspace `Cargo.toml` gains `[patch.crates-io]` entries for `p2panda-core`
and `p2panda-net` pointing at the `dash-chat/p2panda` fork branch already
used everywhere else, so `dash-router`'s registry deps resolve to the
same crates and `SigningKey`/`VerifyingKey`/`Topic` are one type. Both
sides are 0.7.1, so the patch is version-compatible. `dash-router`'s own
`[patch]` for a local polestar checkout does not propagate; the git
polestar is used.

## 3. dash-router changes

### 3.0 Rename

`crates/dash-router-net` → `crates/dash-router`, package name
`dash-router`, `lib.rs` adds `pub use dash_router_core as core;` and
`pub use dash_router_policy as policy;`. Every reference in the sim, core
doc comments, the model crate's dev-deps, tests and the two earlier specs
is updated. One mechanical commit before any functional change.

### 3.1 `GossipTransport` over embedder-supplied gossip (`panda.rs`)

The ephemeral stream types live in the `p2panda` umbrella crate, not in
`p2panda-net`. In Dash Chat that crate is a fork, so a `dash-router` that
named those types would have to either pin itself to Dash Chat's fork or
trust the fork to stay API-compatible with the registry version of a much
larger crate (sqlite store, spaces, encryption, blobs, stream processing).
Two five-line traits avoid that coupling, keep `dash-router` depending on
`p2panda-net` only, and keep its test builds light. So `panda.rs` gains a
transport over two small traits the embedder implements:

```rust
#[trait_variant::make(Send)]
pub trait GossipPublisher { async fn publish(&mut self, bytes: Vec<u8>) -> Result<()>; }
#[trait_variant::make(Send)]
pub trait GossipSubscription {
    /// `(verified author, bytes)`; `None` = closed.
    async fn next(&mut self) -> Option<(PeerKey, Vec<u8>)>;
}

pub struct GossipTransport<P, S> { publisher: P, subscription: S }
impl<P: GossipPublisher, S: GossipSubscription> Transport for GossipTransport<P, S> { .. }
```

Dash Chat implements the two traits over its ephemeral publisher and
subscription pair. `Transport` semantics are unchanged; the existing
`PandaTransport` and `spawn_panda` stay for standalone use and tests.

### 3.2 `PeerKey` and `Incoming.author` (`transport.rs`)

```rust
/// A transport-level node identity: 32 key bytes, p2panda-free.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerKey(pub [u8; 32]);

impl From<[u8; 32]> for PeerKey { .. }
#[cfg(feature = "p2panda")]
impl From<VerifyingKey> for PeerKey { .. }
#[cfg(feature = "p2panda")]
impl TryFrom<PeerKey> for VerifyingKey { .. }

/// How a wire identity `N` maps to a transport identity, if it has one.
pub trait PeerIdentity {
    fn peer_key(&self) -> Option<PeerKey>;
}
```

`PeerIdentity` is implemented for `VerifyingKey` (feature-gated, `Some`)
and for the integer id types the tests and sim use (`None`). `NodeCore`'s
`N` bound gains `PeerIdentity`.

`Incoming` gains `author: Option<PeerKey>`. The shell's `on_wire`, after
decoding, drops the message and bumps `dropped_msgs` when `author` is
`Some(a)` and `msg.sender.peer_key() != Some(a)`. Loopback and the existing
`PandaTransport` set `None` (unchanged behaviour). This closes the "claim
any sender" hole for free on the ephemeral path.

### 3.3 Size-aware Have batching (`shell.rs`)

Gossip rejects messages over `max_message_size` (p2panda default 4096
bytes; Dash Chat does not override it). `hydrate` currently returns one
`WireMessage` per `SendHave`. Change: `hydrate` returns `Vec<WireMessage>`,
greedily packing hydrated ops in `(log, seq)` order so each encoded message
stays under a `CoreConfig.max_wire_bytes` budget (default 3800, leaving
room for the envelope). A single op larger than the budget is sent alone
with its payload stripped (header only, `payload: None`) so the range is
still advertised; the peer fetches the body through native sync later.
`Want` messages are bounded the same way: split `LogRanges` across
messages by encoded size.

Conformance impact: the lockstep test compares state projections, not
broadcast counts, so splitting is invisible to it. The `hydrate` doc
comment's "byte-for-byte one Have" claim is updated.

### 3.4 `LogKey for [u8; 64]` (`disk.rs`)

The relay store's key must hold author (32) + LogId (32). Add the impl next
to the existing `[u8; 32]`.

### 3.5 Prefix subscriptions (`dash-router-core`, models, shell)

The protocol change that makes "subscribe to a topic" expressible without
the router knowing topics. Wire version bumps to 1 (topic
`dash-router/v1`).

**Log type.** Core gains

```rust
pub trait Log: Id {
    type Prefix: Id;
    fn prefix(&self) -> Self::Prefix;
}
```

with `Prefix = Self` and `prefix = identity` for the integer ids the tests,
models and sim use, so every existing scenario keeps its meaning (a prefix
subscription to `7` is a subscription to log `7`). Dash Chat's
`RouterLog(LogId, author)` has `Prefix = LogId`.

**Wire.** `WireBody::Want` becomes

```rust
Want { origin: N, ranges: LogRanges<L>, prefixes: BTreeSet<L::Prefix> }
```

`ranges` is what it is today: gaps and open tails for logs the wanter
already knows. `prefixes` says "and every log under these that I have not
named in `ranges`". `origin` is the wanting node: a relayer re-signs the
message (`sender`) but keeps `origin`, and an answerer keys recent Wants by
origin and records nothing for an echo of its own Want (otherwise a relayed
echo would name the answerer's logs on the relayer's behalf and stop them
going wholesale to a prefix wanter behind it). Have is unchanged.

**Router state and actions.** `RouterState` gains `open:
BTreeSet<L::Prefix>` (this node's wholesale interests), set by a new
`RouterAction::Open(BTreeSet<L::Prefix>)` from the node glue on
subscribe/unsubscribe. `Record` (recent wants) carries the wanter's
prefixes too, and so does `relayed_wants`. `FireWant` sends
`Want { ranges: wanted(), prefixes: open }`. `next_have` becomes the union
over each recent Want record of: `held ∩ record.ranges` plus, for every
held log whose prefix is in `record.prefixes` and which `record.ranges`
does not name, that log's full held range; then minus `recent_haves` as
today. A wanter that already knows a log names it explicitly and so never
gets it wholesale again. Want relaying and suppression treat prefixes as
one more set to union and diff.

**Node glue (`NodeMachine` and `NodeCore`).** `subscriptions` becomes
`BTreeSet<L::Prefix>`; a log is subscribed iff its prefix is.
`Subscribe(prefix)` migrates every relay log under that prefix into ext
and issues `Open`; `Unsubscribe` the reverse. `reconcile_held` drops the
"known-but-empty marker" for subscribed logs with no data: the prefix
entry in Want now carries that intent, and the marker returns naturally
once a first op for the log lands. The embedder API is
`RouterHandle::subscribe(prefix)` / `unsubscribe(prefix)`; `Delivered`
still reports the full `L`.

**Models and tests.** `dash-router-net-model`, the sim behaviours and the
conformance driver take `Log` in place of `Id` on `L` and pass prefixes
through; with `Prefix = Self` their existing assertions hold unchanged. New
core tests: a prefix Want is answered with every held log under the
prefix except those named explicitly; a Subscribe migrates all relay logs
under the prefix; a Want relayed onward keeps its prefixes.

This work is sequenced first in the implementation plan, as its own phase
with the models green before any shell or Dash Chat wiring.

## 4. Dash Chat: the `lan_router` module

One file, `crates/dashchat-node/src/lan_router.rs`, `#[cfg(feature =
"lan-router")]`, plus a stub with the same public surface when the feature
is off so call sites in `node.rs` stay unconditional.

```rust
pub struct LanRouter { handle: RouterHandle<RouterLog>, task: JoinHandle<..> }

impl LanRouter {
    /// `None` when disabled by config or feature.
    pub async fn spawn(node: &Node) -> Result<Option<Self>>;
    pub async fn subscribe_topic(&self, topic: TopicId) -> Result<()>;
    pub async fn unsubscribe_topic(&self, topic: TopicId) -> Result<()>;
    pub fn hint_changed(&self, author: DeviceId, log_id: LogId);
    pub async fn shutdown(self);
}
```

### 4.1 Identity and log type

- `N = DeviceId` (the node's ed25519 key, already `VerifyingKey`).
- `RouterLog([u8; 64])`: LogId bytes then author key bytes, prefix
  first so the relay store's ordered scans group a topic's logs together.
  Implements `Serialize`, `Ord`, `Log` (`Prefix = LogId`) and `LogKey`
  conversion. Helpers `RouterLog::new(log_id, author)` and `split()`.

### 4.2 Ext store adapter: `OpStoreExt`

Implements `AsyncStorage<RouterLog>` and `WatchableStorage<RouterLog>` over
`OpStore` plus an `Arc<RwLock<HashMap<LogId, TopicId>>>` topic map.

- `held_of` / `held_all`: for each known `(author, log_id)`, range
  `[pruned_from, acked_height]` from `get_log_heights` and the ack
  watermark. p2panda logs are contiguous by construction, so no gap scan.
- `fetch`: `get_log(author, log_id, from)` filtered to the requested
  ranges and clamped at the acked height; `Op { header: header.encode(),
  payload: body.map(to_bytes) }`.
- `ingest`: decode `Header`, build `Operation`, resolve the topic from the
  map (unknown log id → error, counted, op dropped), and push it into a
  long-lived per-topic `mpsc` whose `ReceiverStream` was handed to the actor
  via `Command::Import` at `subscribe_topic` time. Returns `Ok` once queued;
  validation failures downstream are silent and repaired by the next held
  snapshot, exactly as the shell spec allows.
- `changed()`: a `broadcast::Sender<BTreeSet<RouterLog>>` fed by
  `hint_changed`, which `ack_operation` in `app_processing.rs` calls after
  a successful ack (one line, guarded by `if let Some(r) = &self.lan_router`).

### 4.3 Subscriptions

One router subscription per topic: `subscribe_topic(topic)` inserts
`LogId::from_topic(topic) → topic` into the topic map (§4.2), opens the
per-topic import channel, and calls `RouterHandle::subscribe(log_id)`.
That covers every author's log on the topic, known or not yet known, and
migrates anything the relay already holds under it.

- **Startup:** `LanRouter::spawn` calls `subscribe_topic` for every topic
  `initialize_stored_topics` enumerates, inbox topics included.
- **Runtime:** `initialize_topic` calls `subscribe_topic`; the paths that
  drop a topic call `unsubscribe_topic`. One-line hooks behind the
  `Option`.

No author enumeration, no membership hooks, no persistence beyond what
Dash Chat already keeps for its own topics.

### 4.4 Transport

A new actor `Command::RouterStream { topic, reply }` calls
`inner.ephemeral_stream::<ByteBuf>(topic)` and replies with the pair.
`lan_router.rs` implements the two gossip traits from §3.1 over the pair
and builds a `GossipTransport`. Router
topic: `Topic::from(Hash::digest(dash_router::GOSSIP_TOPIC.as_bytes()))`
(the exported constant, currently `"dash-router/v1"`, whose suffix tracks
`WIRE_VERSION`), hashed with the network id the same way Dash Chat's other
ALPNs/topics are, so different networks never share an overlay.

### 4.5 Config and lifecycle

- `NodeConfig.enable_lan_router: bool`, default `false`; `testing()` leaves
  it false.
- `Node::init` calls `LanRouter::spawn(&self)` after the actor and stores
  exist and before `initialize_stored_topics`. With the flag false or the
  feature off this returns `None` and nothing else runs: no relay file, no
  gossip topic, no hooks firing.
- `Node::shutdown` calls `LanRouter::shutdown` first.
- Router policy values (want/have intervals, debounce, relay cap) are
  constants in `lan_router.rs` for v1, taken from the swarm test.
- `RouterEvent::Delivered` is logged at debug; the imported op already
  reaches the UI through the normal app-processor notification.
  `StorageError` is logged at warn.

## 5. Known gaps (v1, documented not fixed)

- **Future multi-log topics.** If Dash Chat later associates a topic
  with logs whose LogId is not `blake3(topic)`, each such LogId is one
  more prefix to subscribe to when the association is made. Prefix
  subscription migrates whatever the relay already holds under it, so an
  association made after the ops arrived loses nothing. No router change
  needed.
- `app_processing.rs` registers the author of every `ExternalStream` op as
  a bootstrap node (assumes mailbox origin). Router-imported ops trigger the
  same. Harmless on a LAN; noted for cleanup.
- Network-size estimate `n` for interval sampling is a constant.

## 6. Testing

dash-router:
- Unit: `[u8; 64]` LogKey round-trip; Have/Want splitting stays under the
  budget on a large fixture and preserves `(log, seq)` order; oversize op
  goes header-only; envelope/sender mismatch is dropped and counted;
  prefix-subscription tests as listed in §3.5.
- Existing conformance, loop, model and sim tests stay green with
  `Prefix = Self`.

dash-chat (`--features lan-router`):
- Unit: `OpStoreExt` held/fetch against a real `OpStore` with acked and
  unacked ops; `ingest` routes to the right topic channel; unknown log id
  is rejected.
- Integration (`tests/lan_router.rs`, multi-thread, `#[ignore]` like the
  swarm test since it needs real mDNS): two `TestNode`s with mDNS active,
  relay off, no mailbox, `enable_lan_router: true`, contact established
  out-of-band; a message sent on A appears in B's projection. A second
  case: no prior contact, A sends a contact request to B's inbox topic and
  it arrives via the router alone. A control run
  with the flag false must *not* converge, proving the router did the work.
- `cargo check` on `dashchat-node` with default features must not pull any
  dash-router crate (verified via `cargo tree`).
