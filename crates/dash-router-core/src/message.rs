//! Wire messages, per DESIGN.md, plus a sender field.
//!
//! p2panda's gossip subscription strips the sender's node id, so every
//! message carries `from` explicitly.

use std::collections::BTreeMap;

use crate::ranges::{LogRanges, Ranges, Seq};

/// One log entry. Both parts are opaque to relays.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
pub enum Message<N, L: Ord> {
    /// A request for others to send Haves covering these ranges.
    Want { from: N, ranges: LogRanges<L> },
    /// Ops being shared.
    Have {
        from: N,
        ops: HaveOps<L>,
        /// Set only by the author, immediately after creating the data.
        /// A fresh Have is relayed immediately by everyone who receives it.
        fresh: bool,
    },
}

impl<N: Copy, L: Ord> Message<N, L> {
    pub fn from(&self) -> N {
        match self {
            Message::Want { from, .. } | Message::Have { from, .. } => *from,
        }
    }
}
