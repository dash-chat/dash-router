//! The unit of data carried by a Have. Opaque to relays.

use serde::{Deserialize, Serialize};

/// One log entry. Both parts are opaque to relays.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Op {
    /// A relatively small blob, independently useful.
    pub header: Vec<u8>,
    /// May be dropped separately from the header during GC.
    pub payload: Option<Vec<u8>>,
}
