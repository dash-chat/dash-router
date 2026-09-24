//! The transport boundary (spec §7): what makes the conformance and e2e
//! tests possible without p2panda anywhere near them.

use std::net::IpAddr;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyhow::Result;
use tokio::sync::broadcast;

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
    /// The remote's address for the LAN check; `None` means the transport
    /// itself scopes membership (e.g. an mDNS-discovered overlay).
    pub remote: Option<IpAddr>,
    /// The transport-verified author, when the transport verifies one.
    pub author: Option<PeerKey>,
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
                        author: None,
                        bytes,
                    });
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue, // gossip is lossy
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}

/// What an embedder that already runs a gossip overlay hands us: publish
/// bytes on the well-known topic, and a stream of `(verified author,
/// bytes)`. Dash Chat implements these over p2panda's ephemeral stream,
/// whose signed envelope is where `PeerKey` comes from (spec §3.1).
#[trait_variant::make(Send)]
pub trait GossipPublisher {
    /// Publish one wire message. An `Err` stops the router task: the shell
    /// treats a failed broadcast as the transport being gone, ends its
    /// loop and the task's `JoinHandle` resolves to `Ok(())`, after which
    /// every `RouterHandle` call fails. Return `Err` only when the overlay
    /// is unusable, not for a transient hiccup (gossip is lossy anyway;
    /// swallowing a dropped message is always safe).
    async fn publish(&mut self, bytes: Vec<u8>) -> Result<()>;
}

#[trait_variant::make(Send)]
pub trait GossipSubscription {
    /// `None` = the overlay is gone; the transport reports shutdown.
    async fn next(&mut self) -> Option<(PeerKey, Vec<u8>)>;
}

/// A [`Transport`] over an embedder-supplied gossip pair.
///
/// It performs no address filtering: `remote` is always `None` (the pair
/// exposes no socket address), so the shell's `is_lan` check never fires,
/// and every broadcast floods to every member of the embedder's gossip
/// overlay, wherever they are. The overlay's membership *is* the router's
/// reach. An embedder whose overlay is not LAN-scoped (e.g. one that also
/// bootstraps or relays over the internet) is responsible for scoping it,
/// for instance with a dedicated LAN-only topic. Otherwise Wants and Haves
/// cross the internet too, and a Want — which lists the logs and channels
/// its node is interested in — becomes an interest signal visible to every
/// overlay member, not just to peers on the local network.
pub struct GossipTransport<P, S> {
    publisher: P,
    subscription: S,
}

impl<P, S> GossipTransport<P, S> {
    pub fn new(publisher: P, subscription: S) -> Self {
        Self {
            publisher,
            subscription,
        }
    }
}

impl<P: GossipPublisher, S: GossipSubscription> Transport for GossipTransport<P, S> {
    async fn broadcast(&mut self, bytes: Vec<u8>) -> Result<()> {
        self.publisher.publish(bytes).await
    }

    async fn recv(&mut self) -> Option<Incoming> {
        let (author, bytes) = self.subscription.next().await?;
        Some(Incoming {
            remote: None,
            author: Some(author),
            bytes,
        })
    }
}

#[cfg(test)]
mod gossip_tests {
    use super::*;
    use tokio::sync::mpsc;

    struct ChanPub(mpsc::Sender<Vec<u8>>);
    impl GossipPublisher for ChanPub {
        async fn publish(&mut self, bytes: Vec<u8>) -> Result<()> {
            self.0
                .send(bytes)
                .await
                .map_err(|_| anyhow::anyhow!("closed"))
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
            Some(Incoming {
                remote: None,
                author: Some(PeerKey([9; 32])),
                bytes: vec![4]
            })
        );

        drop(sub_tx);
        assert_eq!(
            t.recv().await,
            None,
            "closed subscription = transport shut down"
        );
    }
}

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
