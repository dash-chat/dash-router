# LAN router: proving router-only replication end to end

Addendum to `2026-09-22-dash-chat-lan-router-design.md` §6. Status: approved
2026-09-23 (user: "just make sure there's a test of the form you suggested",
the store-and-forward variant below).

## Problem

The dash-chat integration tests in `crates/dashchat-node/tests/lan_router.rs`
run two nodes over real mDNS. p2panda's native log sync also runs over mDNS
and converges the same ops, often first, so the tests can only report the
router's delivered count, not assert it. The tests are `#[ignore]`d and
serialise on a mutex.

## Facts the design rests on

- p2panda's native log sync runs only between nodes that (a) can connect and
  (b) share a topic. Sync sessions are gated on both the initiating and the
  accepting side by `ConnectionAuthoriser::can_connect_on_topic`, which the
  umbrella exposes as `Node::topic_block(node_id, topic)`. The per-topic
  lists are consulted nowhere else: gossip, ephemeral streams and HyParView
  membership ignore them.
- `Node::block(node_id)` is the global list, enforced by the iroh endpoint
  hooks on every outbound dial (`before_connect`) and every accepted
  handshake (`after_handshake`), for every protocol including gossip.
- In `NodeConfig::testing()` there is no mDNS and no relay, so a node can
  reach only peers it was introduced to (`insert_peer_addr`), plus whatever
  p2panda's discovery walker learns from them. The walker falls back to any
  known node when there are no bootstraps, and a discovery session hands
  over the peer's transport infos, so a chain A–B–C leaks C's address to A
  within a walk or two. Topology alone therefore does not prevent native
  sync between A and C; a block does.
- Router relays hold ops for topics they do not subscribe to: an author
  pushes a Have for newly authored ops after a debounce (DESIGN.md), an
  unsubscribed receiver parks them in its relay store, and a later prefix
  Want from anyone is answered from that store.
- The router's `Incoming.remote` is `None` over the gossip transport, and
  `is_lan` accepts loopback, so localhost tests are not filtered out.

## Design

Two test-only switches on Dash Chat's `Node`, both behind the `testing`
feature, both no-ops on a node with no networking layer:

- `block_peer(node_id)`: forwards to p2panda `Node::block`. Used to make "A
  and C are never directly connected" a guarantee rather than a topology.
- `block_native_sync_with(node_id)`: records the id in the node actor and
  calls p2panda `Node::topic_block(node_id, topic)` for every topic already
  subscribed and, inside the actor's single stream-opening path, for every
  topic subscribed later. Opening a stream is the one place a topic becomes
  syncable, so applying the block there closes the race between subscribing
  and the first sync session.

One observation hook so a test can wait on a relay instead of sleeping:

- dash-router `RouterHandle::relay_held() -> LogRanges<L>`: what the relay
  store (not the ext store) holds, per log. A plain read served by the node
  task, next to `stats()`.
- dash-chat `Node::lan_router_relay_holds(topic) -> Option<bool>` (testing +
  lan-router): whether the router's relay holds at least one op on `topic`
  by any author; `None` when the router is not running.

## Tests (dash-chat, `tests/lan_router.rs`, not ignored, localhost)

All use `NodeConfig::testing().random_network_id()` with
`enable_lan_router: true`, no mailbox, and explicit introductions only.

1. **Switch controls** (no router needed): two nodes that block native sync
   with each other do not converge a contact request within a bounded wait;
   two nodes that block each other globally do not either. These are the
   negative controls for everything below.
2. **Pair, native sync off.** A and B block native sync with each other,
   then are introduced. Contact request, accept, and a direct-chat message
   converge, and both delivered counts are positive. With native sync off,
   the router is the only path.
3. **Relay through a node that never subscribes.** A and C block each other
   globally; A knows B, B knows A and C, C knows B. A and C establish
   contact and exchange a message. B's contact list stays empty, B's
   delivered count stays zero, B's relay holds the direct-chat topic, and
   A's and C's delivered counts are positive.
4. **Store and forward for an owner who was away.** A creates a contact QR
   code and shuts down, never having met anyone. B and C start and are
   introduced. C adds the contact (publishing a request on A's inbox topic,
   which B never subscribes). The test waits until B's relay holds an op on
   one of A's inbox topics, then shuts C down. A restarts from its stored
   state, is introduced to B only, and the next contact request A sees is
   C's. A's delivered count is positive; B's is zero. This settles the
   claim in §1 of the main spec that a relay serves an owner who was off
   the LAN when the op was sent.
5. **Real LAN smoke test.** The existing mDNS pair test stays, ignored, as
   the only test that exercises real multicast. Its control twin is
   removed; the switch controls replace it.

## Out of scope

LAN scoping of the router overlay (deferred; see memory
`lan-router-scoping-followup`). Production code paths are untouched apart
from the two feature-gated switches and the relay read.
