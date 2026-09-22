//! The embedding API's data types (spec §5). Task 10 adds the command
//! channel and `RouterHandle`.

use dash_router_core::Seq;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouterEvent<L> {
    /// Novel subscribed data landed in the ext store. No bytes: the
    /// embedder owns that store; handing bytes again would invite a
    /// second source of truth [approved].
    Delivered(L, Seq),
    /// The ext store (the embedder's data path) failed; the node keeps
    /// gossiping from what it has [approved: degrade, don't crash].
    StorageError(StorageErrorReport),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageErrorReport {
    pub context: &'static str,
    pub message: String,
}
