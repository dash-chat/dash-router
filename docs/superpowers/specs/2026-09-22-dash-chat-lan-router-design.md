# Dash Chat LAN router integration — design

Hook a `dash-router-net` shell into a Dash Chat node so ops in the node's
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
- **Log identity is `(author, LogId)` [approved].** Dash Chat's LogId is
  `blake3(topic)`, one log per author per topic. The adapter subscribes to
  that pair for every author it knows on a subscribed topic. Inbox topics
  (unknown authors write) stay outside the router in v1.
- **Serve only acked ops [approved].** The ext adapter mirrors
  `MailboxStore::get_log`: a log's held range stops at the acked height, so
  a body about to be tombstoned never leaves the device.
- **Relay store is the existing redb `DiskRelayStore` [approved]**, at
  `<data>/lan_router.redb`.
- **Everything is a no-op when `NodeConfig.enable_lan_router` is false
  [approved]**, and the whole module sits behind a `lan-router` cargo
  feature on `dashchat-node`. The config field exists regardless of the
  feature; with the feature off it is ignored.

## 2. Dependency wiring (dash-chat)

`crates/dashchat-node/Cargo.toml`:

```toml
[features]
lan-router = ["dep:dash-router-net", "dep:dash-router-core", "dep:dash-router-policy"]

[dependencies]
dash-router-net    = { git = "https://github.com/maackle/dash-router", optional = true, features = ["p2panda"] }
dash-router-core   = { git = "...", optional = true }
dash-router-policy = { git = "...", optional = true }
```

(A `path` dep during development; switch to `git` before merge.)

Workspace `Cargo.toml` gains `[patch.crates-io]` entries for `p2panda-core`
and `p2panda-net` pointing at the `dash-chat/p2panda` fork branch already
used everywhere else, so `dash-router-net`'s registry deps resolve to the
same crates and `SigningKey`/`VerifyingKey`/`Topic` are one type. Both
sides are 0.7.1, so the patch is version-compatible. `dash-router`'s own
`[patch]` for a local polestar checkout does not propagate; the git
polestar is used.

## 3. dash-router changes

### 3.1 `GossipTransport` over embedder-supplied gossip (`panda.rs`)

`dash-router-net` must not depend on the `p2panda` umbrella crate, so the
ephemeral stream types never appear here. Instead `panda.rs` gains a
transport over two small traits the embedder implements:

```rust
#[trait_variant::make(Send)]
pub trait GossipPublisher { async fn publish(&mut self, bytes: Vec<u8>) -> Result<()>; }
#[trait_variant::make(Send)]
pub trait GossipSubscription {
    /// `(verified author's encoded key, bytes)`; `None` = closed.
    async fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)>;
}

pub struct GossipTransport<P, S> { publisher: P, subscription: S }
impl<P: GossipPublisher, S: GossipSubscription> Transport for GossipTransport<P, S> { .. }
```

Dash Chat implements the two traits over its ephemeral publisher and
subscription pair. `Transport` semantics are unchanged; the existing
`PandaTransport` and `spawn_panda` stay for standalone use and tests.

### 3.2 `Incoming.author: Option<Vec<u8>>` (`transport.rs`)

`Incoming` gains `author: Option<Vec<u8>>` (encoded key bytes, so the
trait stays p2panda-free). The shell's `on_wire`, after decoding, drops
the message and bumps `dropped_msgs` when `author` is `Some` and does not
equal the encoded `msg.sender`. Loopback and the existing `PandaTransport`
set `None` (unchanged behaviour). This closes the "claim any sender"
hole for free on the ephemeral path.

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
    pub async fn note_author(&self, topic: TopicId, author: DeviceId) -> Result<()>;
    pub fn hint_changed(&self, author: DeviceId, log_id: LogId);
    pub async fn shutdown(self);
}
```

### 4.1 Identity and log type

- `N = DeviceId` (the node's ed25519 key, already `VerifyingKey`).
- `RouterLog([u8; 64])`: author key bytes then LogId bytes. Implements
  `Serialize`, `Ord`, and `LogKey` conversion. Helpers `RouterLog::new(author,
  log_id)` and `split()`.

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

`LanRouter::spawn` seeds subscriptions from the same enumeration
`initialize_stored_topics` uses, minus inbox topics, and for each topic
subscribes `(author, LogId::from_topic(topic))` for:

- every author with a height in `get_log_heights(log_id)`,
- direct chats: both parties,
- groups: `group_store.members(chat_id)`.

Runtime additions: `initialize_topic` calls `subscribe_topic`; the
group-member paths in `app_processing.rs` that add a member call
`note_author`. Both are one-line hooks behind the `Option`.

### 4.4 Transport

A new actor `Command::RouterStream { topic, reply }` calls
`inner.ephemeral_stream::<ByteBuf>(topic)` and replies with the pair.
`lan_router.rs` implements the two gossip traits from §3.1 over the pair
and builds a `GossipTransport`. Router
topic: `Topic::from(Hash::digest(b"dash-router/v0"))`, hashed with the
network id the same way Dash Chat's other ALPNs/topics are, so different
networks never share an overlay.

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

- Inbox topics are not routed.
- `app_processing.rs` registers the author of every `ExternalStream` op as
  a bootstrap node (assumes mailbox origin). Router-imported ops trigger the
  same. Harmless on a LAN; noted for cleanup.
- Network-size estimate `n` for interval sampling is a constant.

## 6. Testing

dash-router:
- Unit: `[u8; 64]` LogKey round-trip; Have/Want splitting stays under the
  budget on a large fixture and preserves `(log, seq)` order; oversize op
  goes header-only; envelope/sender mismatch is dropped and counted.
- Existing conformance and loop tests stay green.

dash-chat (`--features lan-router`):
- Unit: `OpStoreExt` held/fetch against a real `OpStore` with acked and
  unacked ops; `ingest` routes to the right topic channel; unknown log id
  is rejected.
- Integration (`tests/lan_router.rs`, multi-thread, `#[ignore]` like the
  swarm test since it needs real mDNS): two `TestNode`s with mDNS active,
  relay off, no mailbox, `enable_lan_router: true`, contact established
  out-of-band; a message sent on A appears in B's projection. A control run
  with the flag false must *not* converge, proving the router did the work.
- `cargo check` on `dashchat-node` with default features must not pull any
  dash-router crate (verified via `cargo tree`).
