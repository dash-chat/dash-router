//! The versioned LAN broadcast payload. postcard-encoded, matching
//! p2panda's own serialization choice. See spec §6.

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{Op, ranges::{LogRanges, Seq}};

/// Bump together with the gossip topic on breaking change.
pub const WIRE_VERSION: u8 = 0;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WireMessage<N, L: Ord> {
    pub version: u8,
    /// Gossip strips the transport sender; we carry our own.
    pub sender: N,
    pub body: WireBody<L>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WireBody<L: Ord> {
    Want(LogRanges<L>),
    /// Hydrated ops grouped per log, in (log, seq) order; payloads may be
    /// None (GC'd). Grouping avoids repeating the 32-byte log id per op.
    Have(Vec<(L, Vec<(Seq, Op)>)>),
}

impl<N: Serialize + DeserializeOwned, L: Ord + Serialize + DeserializeOwned> WireMessage<N, L> {
    pub fn want(sender: N, ranges: LogRanges<L>) -> Self {
        Self { version: WIRE_VERSION, sender, body: WireBody::Want(ranges) }
    }

    pub fn have(sender: N, ops: Vec<(L, Vec<(Seq, Op)>)>) -> Self {
        Self { version: WIRE_VERSION, sender, body: WireBody::Have(ops) }
    }

    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("wire types serialize infallibly")
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        let msg: Self = postcard::from_bytes(bytes)?;
        anyhow::ensure!(msg.version == WIRE_VERSION, "unknown wire version {}", msg.version);
        Ok(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Op, ranges::{LogRanges, Ranges}};

    #[test]
    fn wire_messages_round_trip_and_reject_unknown_versions() {
        let want: WireMessage<u32, u8> = WireMessage::want(
            7,
            LogRanges::from_pairs([(1u8, Ranges::from(3))]),
        );
        let have: WireMessage<u32, u8> = WireMessage::have(
            7,
            vec![(1u8, vec![(0, Op { header: vec![9], payload: Some(vec![9, 9]) })])],
        );
        for msg in [want, have] {
            let bytes = msg.encode();
            assert_eq!(WireMessage::decode(&bytes).unwrap(), msg);
        }

        let mut bad = WireMessage::<u32, u8>::want(7, LogRanges::empty()).encode();
        bad[0] = WIRE_VERSION + 1; // version is the first postcard field (u8)
        assert!(WireMessage::<u32, u8>::decode(&bad).is_err());
    }
}
