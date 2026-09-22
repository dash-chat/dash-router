//! In-memory selfish/ext store (spec §6.2): a shared handle around an
//! `OpsMap` plus a lossy `changed()` stream fed by its own writes.
//!
//! `MemStore` implements the sync [`Storage`] trait directly (the mutex is
//! held only for synchronous map operations, never across an await), so it
//! picks up [`crate::storage::AsyncStorage`] "for free" via the blanket
//! bridge in `storage.rs` — a hand-written `AsyncStorage` impl here would
//! conflict with that blanket impl (E0119: rustc cannot rule out `MemStore`
//! also implementing the foreign `Storage` trait, since `MemStore` is a
//! local type). See the task report for details.

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use dash_router_core::{LogRanges, Op, OpsMap, Seq, Storage};
use tokio::sync::broadcast;

use crate::storage::WatchableStorage;

/// `Clone` shares the same store, so tests and embedders keep a handle to
/// a store the shell owns. The mutex is held only for synchronous map
/// operations — never across an await.
#[derive(Clone, Debug)]
pub struct MemStore<L: Ord> {
    inner: Arc<Mutex<OpsMap<L>>>,
    tx: broadcast::Sender<BTreeSet<L>>,
}

impl<L: Ord + Clone + Default> MemStore<L> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(OpsMap::default())),
            tx: broadcast::channel(64).0,
        }
    }
}

impl<L: Ord + Clone> MemStore<L> {
    pub fn snapshot(&self) -> OpsMap<L> {
        self.inner.lock().expect("mem store poisoned").clone()
    }

    /// A write from outside the shell (the embedder's native sync): stores
    /// and hints, exactly like a shell-side ingest.
    pub fn insert_out_of_band(&self, log: L, seq: Seq, op: Op) {
        Storage::ingest(
            &mut *self.inner.lock().expect("mem store poisoned"),
            log.clone(),
            seq,
            op,
        );
        let _ = self.tx.send(BTreeSet::from([log])); // no receivers is fine
    }
}

impl<L: Ord + Clone + Default> Default for MemStore<L> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L: Ord + Clone> Storage<L> for MemStore<L> {
    fn held_of(&self, logs: &BTreeSet<L>) -> LogRanges<L> {
        Storage::held_of(&*self.inner.lock().expect("mem store poisoned"), logs)
    }
    fn held_all(&self) -> LogRanges<L> {
        Storage::held_all(&*self.inner.lock().expect("mem store poisoned"))
    }
    fn fetch(&self, ranges: &LogRanges<L>) -> Vec<(L, Seq, Op)> {
        Storage::fetch(&*self.inner.lock().expect("mem store poisoned"), ranges)
    }
    fn ingest(&mut self, log: L, seq: Seq, op: Op) {
        Storage::ingest(
            &mut *self.inner.lock().expect("mem store poisoned"),
            log.clone(),
            seq,
            op,
        );
        let _ = self.tx.send(BTreeSet::from([log]));
    }
}

impl<L: Ord + Clone + Send + Sync> WatchableStorage<L> for MemStore<L> {
    fn changed(&self) -> broadcast::Receiver<BTreeSet<L>> {
        self.tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::AsyncStorage;

    #[tokio::test]
    async fn mem_store_is_shared_and_hints_after_ingest() {
        let mut store = MemStore::<u8>::new();
        let handle = store.clone();
        let mut hints = store.changed();
        AsyncStorage::ingest(
            &mut store,
            3,
            0,
            Op {
                header: vec![1],
                payload: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(hints.recv().await.unwrap(), BTreeSet::from([3]));
        assert!(
            Storage::held_all(&handle.snapshot()).contains(&3, 0),
            "clone shares state"
        );

        // The out-of-band path (an embedder's native sync) also hints.
        handle.insert_out_of_band(4, 0, Op::default());
        assert_eq!(hints.recv().await.unwrap(), BTreeSet::from([4]));
    }
}
