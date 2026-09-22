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
