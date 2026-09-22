//! The async storage boundary (spec §3): fallible, awaitable counterparts
//! of the core's sync traits, plus a blanket bridge so any pure sync store
//! (e.g. `OpsMap`) is usable directly.

use std::collections::BTreeSet;

use anyhow::Result;
use dash_router_core::{EvictableStorage, LogRanges, Op, Seq, Storage, Units};
use tokio::sync::broadcast;

// trait_variant desugars each `async fn` to `-> impl Future + Send`, so
// Task 10's `tokio::spawn` can prove the node task's future is Send while
// impls still get written with plain `async fn` syntax.
#[trait_variant::make(Send)]
pub trait AsyncStorage<L: Ord> {
    /// Ranges held for exactly the requested logs; a requested-but-unknown
    /// log appears with an empty range, mirroring the request.
    async fn held_of(&self, logs: &BTreeSet<L>) -> Result<LogRanges<L>>;
    async fn held_all(&self) -> Result<LogRanges<L>>;
    async fn fetch(&self, ranges: &LogRanges<L>) -> Result<Vec<(L, Seq, Op)>>;
    async fn ingest(&mut self, log: L, seq: Seq, op: Op) -> Result<()>;
}

#[trait_variant::make(Send)]
pub trait AsyncEvictableStorage<L: Ord>: AsyncStorage<L> {
    async fn usage(&self) -> Result<Units>;
    /// The unit delta `ingest` would add: the shed-at-cap check must use
    /// the same arithmetic as the store (`OpsMap::ingest_delta`).
    async fn ingest_delta(&self, log: &L, seq: Seq, op: &Op) -> Result<Units>;
    async fn held_payloads(&self) -> Result<LogRanges<L>>;
    async fn evict_payloads(&mut self, ranges: &LogRanges<L>) -> Result<()>;
    async fn evict(&mut self, ranges: &LogRanges<L>) -> Result<()>;
}

/// Lossy change hints from a store with writers of its own (spec §2).
pub trait WatchableStorage<L: Ord>: AsyncStorage<L> {
    /// Logs whose held ranges may have changed; the empty set means
    /// "anything" (re-read `held_all`). Lossy by design — a missed hint
    /// is repaired by the next Want/Have cycle.
    fn changed(&self) -> broadcast::Receiver<BTreeSet<L>>;
}

impl<L: Ord + Clone + Send + Sync, S: Storage<L> + Send + Sync> AsyncStorage<L> for S {
    async fn held_of(&self, logs: &BTreeSet<L>) -> Result<LogRanges<L>> {
        Ok(Storage::held_of(self, logs))
    }
    async fn held_all(&self) -> Result<LogRanges<L>> {
        Ok(Storage::held_all(self))
    }
    async fn fetch(&self, ranges: &LogRanges<L>) -> Result<Vec<(L, Seq, Op)>> {
        Ok(Storage::fetch(self, ranges))
    }
    async fn ingest(&mut self, log: L, seq: Seq, op: Op) -> Result<()> {
        Storage::ingest(self, log, seq, op);
        Ok(())
    }
}

impl<L: Ord + Clone + Send + Sync, S: EvictableStorage<L> + Send + Sync> AsyncEvictableStorage<L>
    for S
{
    async fn usage(&self) -> Result<Units> {
        Ok(EvictableStorage::usage(self))
    }
    async fn ingest_delta(&self, log: &L, seq: Seq, op: &Op) -> Result<Units> {
        Ok(EvictableStorage::ingest_delta(self, log, seq, op))
    }
    async fn held_payloads(&self) -> Result<LogRanges<L>> {
        Ok(EvictableStorage::held_payloads(self))
    }
    async fn evict_payloads(&mut self, ranges: &LogRanges<L>) -> Result<()> {
        EvictableStorage::evict_payloads(self, ranges);
        Ok(())
    }
    async fn evict(&mut self, ranges: &LogRanges<L>) -> Result<()> {
        EvictableStorage::evict(self, ranges);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dash_router_core::OpsMap;

    /// Any pure sync store is an async store via the blanket bridge — this is
    /// what the conformance test runs the real shell over.
    #[tokio::test]
    async fn blanket_bridge_delegates_to_the_sync_store() {
        let mut m = OpsMap::<u8>::default();
        let op = Op {
            header: vec![1],
            payload: Some(vec![2]),
        };
        AsyncStorage::ingest(&mut m, 0, 0, op.clone())
            .await
            .unwrap();
        assert_eq!(AsyncEvictableStorage::usage(&m).await.unwrap(), 2);
        assert_eq!(
            AsyncEvictableStorage::ingest_delta(&m, &0, 0, &op)
                .await
                .unwrap(),
            0,
            "duplicate"
        );
        let held = AsyncStorage::held_all(&m).await.unwrap();
        assert_eq!(AsyncStorage::fetch(&m, &held).await.unwrap().len(), 1);
        AsyncEvictableStorage::evict(&mut m, &held).await.unwrap();
        assert!(AsyncStorage::held_all(&m).await.unwrap().is_empty());
    }

    /// Self-review check (not in the brief): the whole point of
    /// `#[trait_variant::make(Send)]` is that a generic node task can
    /// `tokio::spawn` a future holding one of these stores. Prove the
    /// futures really are `Send` for both `OpsMap` (via the blanket) and
    /// `MemStore` (via `Storage` + the blanket).
    #[test]
    fn futures_are_send() {
        fn assert_send<T: Send>(_: T) {}

        let mut m = OpsMap::<u8>::default();
        assert_send(AsyncStorage::ingest(&mut m, 0, 0, Op::default()));
        assert_send(AsyncEvictableStorage::usage(&m));

        let mut ms = crate::mem::MemStore::<u8>::new();
        assert_send(AsyncStorage::ingest(&mut ms, 0, 0, Op::default()));
    }
}
