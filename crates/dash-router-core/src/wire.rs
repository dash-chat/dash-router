//! The versioned LAN broadcast payload. postcard-encoded, matching
//! p2panda's own serialization choice. See spec §6.

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    interest::Interest,
    log::{Log, WireLog},
    op::Op,
    ranges::Seq,
};

/// Bump together with [`GOSSIP_TOPIC`] on breaking change.
/// v1: Want carries an [`Interest`] (spec 2026-09-29): per channel, the
/// emitter's have within an author-prefix scope, no origin.
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
    pub body: WireBody<L>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound(
    serialize = "L: Serialize, L::Channel: Serialize, L::Author: Serialize",
    deserialize = "L: Deserialize<'de>, L::Channel: Deserialize<'de>, L::Author: Deserialize<'de>"
))]
pub enum WireBody<L: Log> {
    /// What the emitter has under each channel it is interested in, within
    /// each entry's author scope; it asks for everything else in scope. No
    /// origin: a Want is not attributable to a wanter. A relayer re-signs
    /// `sender` and forwards the Interest whole.
    Want(Interest<L>),
    /// Hydrated ops grouped per log, in (log, seq) order; payloads may be
    /// None (GC'd). Grouping avoids repeating the log id per op.
    Have(Vec<(L, Vec<(Seq, Op)>)>),
}

impl<N: Serialize + DeserializeOwned, L: WireLog> WireMessage<N, L> {
    /// A Want signed by `sender`: its own interest, or a relayed one.
    pub fn want(sender: N, interest: Interest<L>) -> Self {
        Self {
            version: WIRE_VERSION,
            sender,
            body: WireBody::Want(interest),
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
    use std::collections::BTreeMap;

    use super::*;
    use crate::{interest::Entry, log::Pair, op::Op, ranges::Ranges};

    #[test]
    fn wire_messages_round_trip_and_reject_unknown_versions() {
        let mut interest: Interest<Pair> = Interest::empty();
        interest.insert(2, Entry::whole(BTreeMap::from([(1u8, Ranges::from(3))])));
        interest.insert(5, Entry::default());
        let want: WireMessage<u32, Pair> = WireMessage::want(7, interest.clone());
        match &want.body {
            WireBody::Want(i) => assert_eq!(i, &interest),
            WireBody::Have(_) => unreachable!(),
        }
        let have: WireMessage<u32, Pair> = WireMessage::have(
            7,
            vec![(
                Pair::new(1, 1),
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

        let mut bad = WireMessage::<u32, Pair>::want(7, Interest::empty()).encode();
        bad[0] = WIRE_VERSION + 1; // version is the first postcard field (u8)
        assert!(WireMessage::<u32, Pair>::decode(&bad).is_err());
    }

    /// The point of nesting: one channel, many authors, encodes the
    /// channel once. Ten authors under one channel cost far less than ten
    /// single-author interests would if each repeated the channel.
    #[test]
    fn want_encodes_a_shared_channel_once() {
        #[derive(
            Clone,
            Copy,
            Debug,
            Default,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Serialize,
            Deserialize,
        )]
        struct Ch([u8; 8]);
        impl std::fmt::Display for Ch {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}", self.0)
            }
        }
        #[derive(
            Clone,
            Copy,
            Debug,
            Default,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Serialize,
            Deserialize,
        )]
        struct Wide {
            channel: Ch,
            author: u8,
        }
        impl std::fmt::Display for Wide {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}/{}", self.channel, self.author)
            }
        }
        impl Log for Wide {
            type Channel = Ch;
            type Author = u8;
            fn channel(&self) -> Ch {
                self.channel
            }
            fn author(&self) -> u8 {
                self.author
            }
            fn new(channel: Ch, author: u8) -> Self {
                Self { channel, author }
            }
            fn author_prefix(author: &u8) -> u32 {
                (*author as u32) << 24
            }
        }

        let channel = Ch([0xAB; 8]);
        let ten: Interest<Wide> = Interest::single(
            channel,
            Entry::whole((0..10u8).map(|a| (a, Ranges::from(a as u32))).collect()),
        );
        let one: Interest<Wide> = Interest::single(
            channel,
            Entry::whole(BTreeMap::from([(0u8, Ranges::from(0))])),
        );
        let ten_len = WireMessage::want(1u32, ten).encode().len();
        let one_len = WireMessage::want(1u32, one).encode().len();
        // Ten authors add well under ten copies of the 8-byte channel.
        assert!(
            ten_len < one_len + 9 * 8,
            "ten authors: {ten_len} bytes; one author: {one_len} bytes"
        );
    }
}
