//! The real transport behind the boundary (spec §6.1): p2panda-net's gossip
//! protocol scoped to the local-area network via active mDNS discovery.
//!
//! ## Trust stance \[approved\]
//!
//! Wire messages carry no signatures in v1 (spec §9): the transport itself
//! -- an mDNS-discovered overlay -- is the membership boundary, mirroring
//! [`crate::lan::is_lan`] for the loopback/LAN transports. A node that can
//! join the gossip overlay is, by construction, on the LAN. We therefore
//! never populate [`crate::transport::Incoming::remote`]; p2panda-net's
//! gossip subscription does not expose the delivering peer's socket
//! address, and even if it did, the overlay boundary already does the
//! LAN-membership job that `remote` exists for on the other transports.
//!
//! ## Deviation from the task brief's sketch
//!
//! The brief's construction chain was `AddressBook` -> `Endpoint` ->
//! `MdnsDiscovery` -> `Gossip`. In p2panda-net 0.7.1 this is not sufficient
//! for two mDNS-discovered peers to actually join the same gossip overlay:
//! mDNS only resolves *transport addresses* for nearby nodes, it does not
//! tell the address book which *topics* those nodes are interested in, and
//! `Gossip::stream` bootstraps a topic's overlay only from address-book
//! entries already tagged with that topic (see `p2panda_net::AddressBook::
//! node_infos_by_topics`). Populating that tag is exactly what
//! `p2panda_net::Discovery` (confidential, random-walk topic discovery)
//! does, walking whatever nodes are already known -- including nodes mDNS
//! just found on the LAN. p2panda-net's own `TestNode` test harness and the
//! `chat.rs` example both always pair `MdnsDiscovery` with `Discovery`;
//! this module follows that pattern. `Discovery` still only ever walks
//! nodes already known to the address book, and mDNS is the only source of
//! those nodes here, so the LAN scoping from the brief is preserved -- no
//! bootstrap/relay/internet path is configured.
//!
//! Also, p2panda-core 0.7.1 has no `PrivateKey`/`PublicKey` types (as
//! sketched in the brief); the real names are [`p2panda_core::SigningKey`]
//! and [`p2panda_core::VerifyingKey`], used directly rather than wrapped.

use std::collections::BTreeSet;

use anyhow::{Context, Result};
use dash_router_core::WIRE_VERSION;
use futures_util::StreamExt;
use p2panda_core::{Hash, SigningKey, Topic, VerifyingKey};
use p2panda_net::gossip::{GossipEvent, GossipHandle, GossipSubscription};
use p2panda_net::iroh_mdns::MdnsDiscoveryMode;
use p2panda_net::{AddressBook, Discovery, Endpoint, Gossip, MdnsDiscovery};
use tokio::sync::{broadcast, watch};

use crate::transport::{Incoming, Transport};

/// The well-known gossip topic name for this application's wire protocol.
/// Bump the suffix together with [`WIRE_VERSION`] on any breaking wire
/// change -- see the compile-time reminder below.
const TOPIC_NAME: &str = "dash-router/v1";

const _: () = assert!(
    WIRE_VERSION == 1,
    "bump the gossip topic suffix with the wire version"
);

fn topic() -> Topic {
    Hash::digest(TOPIC_NAME.as_bytes()).into()
}

/// A [`Transport`] backed by a real p2panda-net gossip overlay, scoped to
/// the local-area network by active mDNS discovery. See the module docs
/// for the trust stance and the deviation from the task brief's sketch.
pub struct PandaTransport {
    handle: GossipHandle,
    subscription: GossipSubscription,
    /// The node's current direct gossip neighbours on our topic, folded
    /// from p2panda's membership events by a small tracker task. This is
    /// the set a `broadcast` actually hands bytes to; everyone else hears
    /// them only via further gossip hops.
    neighbours: watch::Receiver<BTreeSet<VerifyingKey>>,
    // Kept alive for as long as the transport lives: dropping any of these
    // tears down the corresponding actor (address book, endpoint, mDNS,
    // discovery, gossip).
    _address_book: AddressBook,
    _endpoint: Endpoint,
    _mdns: MdnsDiscovery,
    _discovery: Discovery,
    _gossip: Gossip,
}

/// Spawn a p2panda-net node identified by `private_key` and join the
/// well-known gossip topic. Returns the transport and the node's public
/// key, which is the wire identity `N`.
pub async fn spawn_panda(private_key: SigningKey) -> Result<(PandaTransport, VerifyingKey)> {
    let address_book = AddressBook::builder()
        .spawn()
        .await
        .context("spawning p2panda address book")?;

    let endpoint = Endpoint::builder(address_book.clone())
        .signing_key(private_key)
        .spawn()
        .await
        .context("spawning p2panda endpoint")?;
    let public_key = endpoint.node_id();

    // Active mode: we advertise our own address on the LAN so peers can
    // find us too, not just the other way around.
    let mdns = MdnsDiscovery::builder(address_book.clone(), endpoint.clone())
        .mode(MdnsDiscoveryMode::Active)
        .spawn()
        .await
        .context("spawning mDNS discovery")?;

    // Confidential topic discovery: walks nodes mDNS already found on the
    // LAN to learn which of them are interested in our topic (see module
    // docs for why this is required, not optional, for gossip to connect
    // mDNS-discovered peers).
    let discovery = Discovery::builder(address_book.clone(), endpoint.clone())
        .spawn()
        .await
        .context("spawning p2panda discovery")?;

    let gossip = Gossip::builder(address_book.clone(), endpoint.clone())
        .spawn()
        .await
        .context("spawning gossip")?;

    // Subscribe to membership events *before* joining the topic: p2panda
    // only delivers events emitted after the subscription exists, and the
    // `Joined` event fires during `stream()`.
    let events = gossip
        .events()
        .await
        .context("subscribing to gossip events")?;
    let neighbours = track_neighbours(topic(), events);

    let handle = gossip
        .stream(topic())
        .await
        .context("joining gossip topic")?;
    let subscription = handle.subscribe();

    Ok((
        PandaTransport {
            handle,
            subscription,
            neighbours,
            _address_book: address_book,
            _endpoint: endpoint,
            _mdns: mdns,
            _discovery: discovery,
            _gossip: gossip,
        },
        public_key,
    ))
}

impl PandaTransport {
    /// Watch this node's direct gossip neighbours on the wire topic. The
    /// receiver outlives the transport (it just stops changing once the
    /// gossip actor is gone), so an embedder can hand the transport to
    /// [`crate::spawn`] and keep observing the overlay from outside.
    pub fn neighbours(&self) -> watch::Receiver<BTreeSet<VerifyingKey>> {
        self.neighbours.clone()
    }
}

/// Fold p2panda's membership events for `topic` into a watch channel.
/// Ends when the events channel closes (all `Gossip` handles dropped).
fn track_neighbours(
    topic: Topic,
    mut events: broadcast::Receiver<GossipEvent>,
) -> watch::Receiver<BTreeSet<VerifyingKey>> {
    let (tx, rx) = watch::channel(BTreeSet::new());
    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(GossipEvent::Joined { topic: t, nodes }) if t == topic => {
                    tx.send_modify(|set| set.extend(nodes));
                }
                Ok(GossipEvent::NeighbourUp { topic: t, node }) if t == topic => {
                    tx.send_modify(|set| {
                        set.insert(node);
                    });
                }
                Ok(GossipEvent::NeighbourDown { topic: t, node }) if t == topic => {
                    tx.send_modify(|set| {
                        set.remove(&node);
                    });
                }
                Ok(GossipEvent::Left { topic: t }) if t == topic => {
                    tx.send_modify(|set| set.clear());
                }
                Ok(_) => {}
                // Lagged: we lost some events. The set may now be stale
                // until the next Up/Down for the affected node; membership
                // events are rare enough that this is acceptable for a
                // diagnostic view.
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
    rx
}

impl Transport for PandaTransport {
    async fn broadcast(&mut self, bytes: Vec<u8>) -> Result<()> {
        self.handle
            .publish(bytes)
            .await
            .map_err(|err| anyhow::anyhow!("gossip publish failed: {err}"))
    }

    async fn recv(&mut self) -> Option<Incoming> {
        loop {
            match self.subscription.next().await {
                Some(Ok(bytes)) => {
                    return Some(Incoming {
                        // The overlay membership is the LAN boundary; see
                        // module docs.
                        remote: None,
                        bytes,
                    });
                }
                // Lossy broadcast stream lagged; gossip is unreliable by
                // design, so skip and keep listening.
                Some(Err(_lagged)) => continue,
                None => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "binds real sockets and mDNS; run manually: cargo test -p dash-router --features p2panda -- --ignored"]
    async fn two_panda_nodes_gossip_on_localhost() {
        let (mut a, _a_key) = spawn_panda(SigningKey::generate())
            .await
            .expect("spawn node A");
        let (mut b, _b_key) = spawn_panda(SigningKey::generate())
            .await
            .expect("spawn node B");

        // Give mDNS + discovery a moment to find each other and populate
        // the gossip overlay before we start publishing.
        tokio::time::sleep(Duration::from_secs(2)).await;

        let probe = b"hello from node A".to_vec();

        // Retry the broadcast: the overlay may still be converging.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut got = None;
        while tokio::time::Instant::now() < deadline && got.is_none() {
            a.broadcast(probe.clone()).await.expect("broadcast");
            got = tokio::time::timeout(Duration::from_millis(500), b.recv())
                .await
                .ok()
                .flatten();
        }

        let incoming = got.expect("node B receives node A's probe within 30s");
        assert_eq!(incoming.bytes, probe);
    }
}
