//! The versioned LAN broadcast payload. postcard-encoded, matching
//! p2panda's own serialization choice. See spec §6.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    log::{Log, WireLog},
    op::Op,
    ranges::{LogRanges, Seq},
};

/// Bump together with [`GOSSIP_TOPIC`] on breaking change.
/// v1: Want carries channels (spec 2026-09-22 §3.5) and its origin.
pub const WIRE_VERSION: u8 = 1;

/// The well-known gossip topic name this wire protocol runs on. Every
/// transport (the standalone p2panda one, and an embedder's own overlay
/// behind `GossipTransport`) derives its topic from this one name, so
/// nodes of the same wire version meet and nodes of different versions
/// never share an overlay. Its suffix is [`WIRE_VERSION`]; bump both
/// together (checked at compile time below).
pub const GOSSIP_TOPIC: &str = "dash-router/v1";

const _: () = {
    assert!(
        WIRE_VERSION == 1,
        "bump the gossip topic suffix with the wire version"
    );
    let topic = GOSSIP_TOPIC.as_bytes();
    assert!(
        WIRE_VERSION < 10 && topic[topic.len() - 1] == b'0' + WIRE_VERSION,
        "GOSSIP_TOPIC's suffix must be the wire version"
    );
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound(
    serialize = "N: Serialize, L: Serialize, L::Channel: Serialize, L::Author: Serialize",
    deserialize = "N: Deserialize<'de>, L: Deserialize<'de>, L::Channel: Deserialize<'de>, L::Author: Deserialize<'de>"
))]
pub struct WireMessage<N, L: Log> {
    pub version: u8,
    /// Gossip strips the transport sender; we carry our own. Checked
    /// against the transport's verified author when it has one (shell).
    pub sender: N,
    pub body: WireBody<N, L>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound(
    serialize = "N: Serialize, L: Serialize, L::Channel: Serialize, L::Author: Serialize",
    deserialize = "N: Deserialize<'de>, L: Deserialize<'de>, L::Channel: Deserialize<'de>, L::Author: Deserialize<'de>"
))]
pub enum WireBody<N, L: Log> {
    /// `origin`: the node that wants, preserved by every relayer (the
    /// message's `sender` is re-signed at each hop), so an answerer keys
    /// the Want by its wanter and ignores echoes of its own.
    /// `ranges`: gaps and open tails for logs the wanter already knows.
    /// `channels`: "and every log under these that I did not name".
    Want {
        origin: N,
        ranges: LogRanges<L>,
        channels: BTreeSet<L::Channel>,
    },
    /// Hydrated ops grouped per log, in (log, seq) order; payloads may be
    /// None (GC'd). Grouping avoids repeating the log id per op.
    Have(Vec<(L, Vec<(Seq, Op)>)>),
}

impl<N: Serialize + DeserializeOwned, L: WireLog> WireMessage<N, L> {
    /// A Want signed by `sender` on behalf of `origin` (`sender` itself
    /// for an own Want, the incoming Want's origin for a relay).
    pub fn want(
        sender: N,
        origin: N,
        ranges: LogRanges<L>,
        channels: BTreeSet<L::Channel>,
    ) -> Self {
        Self {
            version: WIRE_VERSION,
            sender,
            body: WireBody::Want {
                origin,
                ranges,
                channels,
            },
        }
    }

    pub fn have(sender: N, ops: Vec<(L, Vec<(Seq, Op)>)>) -> Self {
        Self {
            version: WIRE_VERSION,
            sender,
            body: WireBody::Have(ops),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("wire types serialize infallibly")
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        let msg: Self = postcard::from_bytes(bytes)?;
        anyhow::ensure!(
            msg.version == WIRE_VERSION,
            "unknown wire version {}",
            msg.version
        );
        Ok(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        op::Op,
        ranges::{LogRanges, Ranges},
    };

    #[test]
    fn wire_messages_round_trip_and_reject_unknown_versions() {
        let want: WireMessage<u32, u8> = WireMessage::want(
            7,
            5,
            LogRanges::from_pairs([(1u8, Ranges::from(3))]),
            BTreeSet::from([2u8]),
        );
        match &want.body {
            WireBody::Want {
                origin, channels, ..
            } => {
                assert_eq!(*origin, 5, "the origin survives a relayer's re-signing");
                assert_eq!(channels, &BTreeSet::from([2u8]));
            }
            WireBody::Have(_) => unreachable!(),
        }
        let have: WireMessage<u32, u8> = WireMessage::have(
            7,
            vec![(
                1u8,
                vec![(
                    0,
                    Op {
                        header: vec![9],
                        payload: Some(vec![9, 9]),
                    },
                )],
            )],
        );
        for msg in [want, have] {
            let bytes = msg.encode();
            assert_eq!(WireMessage::decode(&bytes).unwrap(), msg);
        }

        let mut bad =
            WireMessage::<u32, u8>::want(7, 7, LogRanges::empty(), BTreeSet::new()).encode();
        bad[0] = WIRE_VERSION + 1; // version is the first postcard field (u8)
        assert!(WireMessage::<u32, u8>::decode(&bad).is_err());
    }
}
