//! Wire messages, per DESIGN.md, wrapped in an envelope carrying the sender.
//!
//! p2panda's gossip subscription strips the sender's node id, so the envelope
//! restores it. Keeping it out of [`Message`] means the protocol payload stays
//! exactly what DESIGN.md describes, and the sender is read the same way
//! whatever the variant.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ranges::{LogRanges, Ranges, Seq};

/// One log entry. Both parts are opaque to relays.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Op {
    /// A relatively small blob, independently useful.
    pub header: Vec<u8>,
    /// May be dropped separately from the header during GC.
    pub payload: Option<Vec<u8>>,
}

/// Ops carried by a Have, grouped by log.
pub type HaveOps<L> = BTreeMap<L, BTreeMap<Seq, Op>>;

/// The ranges covered by a set of Have ops.
pub fn have_ops_ranges<L: Ord + Clone>(ops: &HaveOps<L>) -> LogRanges<L> {
    LogRanges::from_pairs(
        ops.iter()
            .map(|(log, seqs)| (log.clone(), Ranges::from_seqs(seqs.keys().copied()))),
    )
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Message<L: Ord> {
    /// A request for others to send Haves covering these ranges.
    Want { ranges: LogRanges<L> },
    /// Ops being shared.
    Have {
        ops: HaveOps<L>,
        /// Set only by the author, immediately after creating the data.
        /// A fresh Have is relayed immediately by everyone who receives it.
        fresh: bool,
    },
}

/// A [`Message`] together with the node that sent it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageEnvelope<N, L: Ord> {
    pub from: N,
    pub message: Message<L>,
}

impl<N, L: Ord> MessageEnvelope<N, L> {
    pub fn new(from: N, message: Message<L>) -> Self {
        Self { from, message }
    }

    /// A Want envelope.
    pub fn want(from: N, ranges: LogRanges<L>) -> Self {
        Self::new(from, Message::Want { ranges })
    }

    /// A Have envelope.
    pub fn have(from: N, ops: HaveOps<L>, fresh: bool) -> Self {
        Self::new(from, Message::Have { ops, fresh })
    }
}
