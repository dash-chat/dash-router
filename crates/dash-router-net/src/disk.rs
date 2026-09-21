//! Redb-backed disk relay store (spec §6.2): a single `ops` table keyed
//! `log bytes ++ seq BE`, so both `held_all` and `fetch` are ordered
//! prefix scans per log. `held`/`payloads`/`usage` are the same summaries
//! `OpsMap` computes on the fly, but here they are scanned once at `open`
//! and then maintained incrementally, since this store is always the sole
//! writer to its file.
//!
//! redb calls run inline in these `async fn`s (not via `spawn_blocking`):
//! single writer, small values, a cache we can afford to lose and rebuild
//! by reopening. Revisit under profiling if that stops being true.

use std::{collections::BTreeSet, path::Path};

use anyhow::Result;
use dash_router_core::{EvictableStorage, LogRanges, Op, Ranges, Seq, Storage, Units};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ops");

/// A log id usable as a fixed-width, order-preserving byte key: encoding
/// must be monotonic (big-endian for integers) so a byte-lexicographic scan
/// of the table visits sequences in log, then seq, order.
pub trait LogKey: Ord + Clone {
    const WIDTH: usize;
    fn write_key(&self, out: &mut Vec<u8>);
    fn read_key(bytes: &[u8]) -> Self;
}

impl LogKey for [u8; 32] {
    const WIDTH: usize = 32;

    fn write_key(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self);
    }

    fn read_key(bytes: &[u8]) -> Self {
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes[..32]);
        arr
    }
}

impl LogKey for u32 {
    const WIDTH: usize = 4;

    fn write_key(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.to_be_bytes());
    }

    fn read_key(bytes: &[u8]) -> Self {
        u32::from_be_bytes(bytes[..4].try_into().expect("4-byte key"))
    }
}

impl LogKey for u8 {
    const WIDTH: usize = 1;

    fn write_key(&self, out: &mut Vec<u8>) {
        out.push(*self);
    }

    fn read_key(bytes: &[u8]) -> Self {
        bytes[0]
    }
}

fn row_key<L: LogKey>(log: &L, seq: Seq) -> Vec<u8> {
    let mut out = Vec::with_capacity(L::WIDTH + 4);
    log.write_key(&mut out);
    out.extend_from_slice(&seq.to_be_bytes());
    out
}

fn row_seq<L: LogKey>(key: &[u8]) -> Seq {
    Seq::from_be_bytes(key[L::WIDTH..].try_into().expect("4-byte seq suffix"))
}

/// Inclusive `[lo, hi]` byte bounds covering every seq for one log: since
/// keys are fixed-width, `log bytes ++ 0x00.. .. 0x00` through
/// `log bytes ++ 0xff.. .. 0xff` brackets exactly this log's rows.
fn log_bounds<L: LogKey>(log: &L) -> (Vec<u8>, Vec<u8>) {
    let mut lo = Vec::with_capacity(L::WIDTH + 4);
    log.write_key(&mut lo);
    lo.extend_from_slice(&0u32.to_be_bytes());
    let mut hi = Vec::with_capacity(L::WIDTH + 4);
    log.write_key(&mut hi);
    hi.extend_from_slice(&u32::MAX.to_be_bytes());
    (lo, hi)
}

/// Replace `log`'s entry in `map` with `ranges`, dropping the key entirely
/// when `ranges` is empty (mirroring `OpsMap::held_all`/`held_payloads`,
/// which omit logs with nothing held rather than keep an empty marker).
fn set_log_entry<L: LogKey>(map: &mut LogRanges<L>, log: &L, ranges: Ranges) {
    let kept: Vec<(L, Ranges)> = map
        .iter()
        .filter(|(l, _)| *l != log)
        .map(|(l, r)| (l.clone(), r.clone()))
        .collect();
    *map = LogRanges::from_pairs(kept);
    if !ranges.is_empty() {
        map.insert(log.clone(), ranges);
    }
}

/// The relay's disk cache: a redb-backed [`AsyncEvictableStorage`].
pub struct DiskRelayStore<L: LogKey> {
    db: Database,
    /// Maintained incrementally (single writer: this store); rebuilt by a
    /// full scan in [`Self::open`]. Corruption here is repaired by reopen.
    held: LogRanges<L>,
    payloads: LogRanges<L>,
    usage: Units,
}

impl<L: LogKey> DiskRelayStore<L> {
    pub fn open(path: &Path) -> Result<Self> {
        let db = Database::create(path)?;
        {
            // Ensure the table exists even for a brand-new file.
            let write_txn = db.begin_write()?;
            let _ = write_txn.open_table(TABLE)?;
            write_txn.commit()?;
        }

        let mut per_log: std::collections::BTreeMap<L, (Vec<Seq>, Vec<Seq>)> =
            std::collections::BTreeMap::new();
        let mut usage: Units = 0;
        {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(TABLE)?;
            for row in table.range::<&[u8]>(..)? {
                let (k, v) = row?;
                let key = k.value();
                let log = L::read_key(&key[..L::WIDTH]);
                let seq = row_seq::<L>(key);
                let op: Op = postcard::from_bytes(v.value())?;
                usage += if op.payload.is_some() { 2 } else { 1 };
                let entry = per_log.entry(log).or_default();
                entry.0.push(seq);
                if op.payload.is_some() {
                    entry.1.push(seq);
                }
            }
        }

        let mut held = LogRanges::empty();
        let mut payloads = LogRanges::empty();
        for (log, (seqs, pseqs)) in per_log {
            held.insert(log.clone(), Ranges::from_seqs(seqs));
            if !pseqs.is_empty() {
                payloads.insert(log, Ranges::from_seqs(pseqs));
            }
        }

        Ok(Self {
            db,
            held,
            payloads,
            usage,
        })
    }

    /// Recompute `held`/`payloads` for exactly this log from a fresh
    /// prefix scan: eviction is rare, so recompute-per-touched-log keeps
    /// the incremental cache trivially correct rather than requiring a
    /// delta-tracking eviction path.
    fn rebuild_log(&mut self, log: &L) -> Result<()> {
        let (lo, hi) = log_bounds(log);
        let mut seqs = vec![];
        let mut pseqs = vec![];
        {
            let read_txn = self.db.begin_read()?;
            let table = read_txn.open_table(TABLE)?;
            for row in table.range(lo.as_slice()..=hi.as_slice())? {
                let (k, v) = row?;
                let seq = row_seq::<L>(k.value());
                let op: Op = postcard::from_bytes(v.value())?;
                seqs.push(seq);
                if op.payload.is_some() {
                    pseqs.push(seq);
                }
            }
        }
        set_log_entry(&mut self.held, log, Ranges::from_seqs(seqs));
        set_log_entry(&mut self.payloads, log, Ranges::from_seqs(pseqs));
        Ok(())
    }
}

// `DiskRelayStore` implements the *sync* `Storage`/`EvictableStorage`
// traits, not `AsyncStorage`/`AsyncEvictableStorage` directly: the latter
// has a blanket impl in `storage.rs` for any `S: Storage<L> + Send + Sync`,
// and rustc's coherence check cannot rule out `DiskRelayStore` also getting
// a foreign `Storage` impl elsewhere, so a hand-written `AsyncStorage` impl
// here would conflict with that blanket (E0119) — see `mem.rs`'s doc
// comment for the same reasoning. Implementing the sync traits picks up
// `AsyncStorage`/`AsyncEvictableStorage` "for free" via the blanket bridge.
//
// redb errors here (corruption, I/O failure) are treated as fatal for this
// single-embedded-writer store, matching how `MemStore` treats mutex
// poisoning: `.expect(...)`, not a recoverable `Result`, since the sync
// `Storage` trait has no room for one. `open`'s `Result` remains the
// fallible surface for construction (missing directory, bad file, etc).
impl<L: LogKey> Storage<L> for DiskRelayStore<L> {
    fn held_of(&self, logs: &BTreeSet<L>) -> LogRanges<L> {
        LogRanges::from_pairs(logs.iter().map(|log| {
            (
                log.clone(),
                self.held.get(log).cloned().unwrap_or_else(Ranges::empty),
            )
        }))
    }

    fn held_all(&self) -> LogRanges<L> {
        self.held.clone()
    }

    fn fetch(&self, ranges: &LogRanges<L>) -> Vec<(L, Seq, Op)> {
        let read_txn = self.db.begin_read().expect("redb begin_read");
        let table = read_txn.open_table(TABLE).expect("redb open_table");
        let mut out = vec![];
        for (log, r) in ranges.iter() {
            if r.is_empty() {
                continue;
            }
            let (lo, hi) = log_bounds(log);
            for row in table
                .range(lo.as_slice()..=hi.as_slice())
                .expect("redb range")
            {
                let (k, v) = row.expect("redb row");
                let seq = row_seq::<L>(k.value());
                if r.contains(seq) {
                    let op: Op = postcard::from_bytes(v.value()).expect("postcard decode");
                    out.push((log.clone(), seq, op));
                }
            }
        }
        out
    }

    fn ingest(&mut self, log: L, seq: Seq, op: Op) {
        let key = row_key(&log, seq);
        let mut is_new = false;
        let mut upgraded = false;
        let mut delta: Units = 0;
        {
            let write_txn = self.db.begin_write().expect("redb begin_write");
            {
                let mut table = write_txn.open_table(TABLE).expect("redb open_table");
                let existing: Option<Op> = table
                    .get(key.as_slice())
                    .expect("redb get")
                    .map(|v| postcard::from_bytes(v.value()).expect("postcard decode"));
                match existing {
                    None => {
                        is_new = true;
                        delta = if op.payload.is_some() { 2 } else { 1 };
                        let bytes = postcard::to_stdvec(&op).expect("postcard encode");
                        table
                            .insert(key.as_slice(), bytes.as_slice())
                            .expect("redb insert");
                    }
                    Some(e) if e.payload.is_none() && op.payload.is_some() => {
                        upgraded = true;
                        delta = 1;
                        let bytes = postcard::to_stdvec(&op).expect("postcard encode");
                        table
                            .insert(key.as_slice(), bytes.as_slice())
                            .expect("redb insert");
                    }
                    Some(_) => {
                        // Duplicate: op already held at least as good. No write.
                    }
                }
            }
            write_txn.commit().expect("redb commit");
        }
        self.usage += delta;
        if is_new {
            let merged = self
                .held
                .get(&log)
                .map(|r| r.union(&Ranges::from_seqs([seq])))
                .unwrap_or_else(|| Ranges::from_seqs([seq]));
            self.held.insert(log.clone(), merged);
        }
        if (is_new && op.payload.is_some()) || upgraded {
            let merged = self
                .payloads
                .get(&log)
                .map(|r| r.union(&Ranges::from_seqs([seq])))
                .unwrap_or_else(|| Ranges::from_seqs([seq]));
            self.payloads.insert(log, merged);
        }
    }
}

impl<L: LogKey> EvictableStorage<L> for DiskRelayStore<L> {
    fn usage(&self) -> Units {
        self.usage
    }

    fn ingest_delta(&self, log: &L, seq: Seq, op: &Op) -> Units {
        let key = row_key(log, seq);
        let read_txn = self.db.begin_read().expect("redb begin_read");
        let table = read_txn.open_table(TABLE).expect("redb open_table");
        let existing: Option<Op> = table
            .get(key.as_slice())
            .expect("redb get")
            .map(|v| postcard::from_bytes(v.value()).expect("postcard decode"));
        match existing {
            None => {
                if op.payload.is_some() {
                    2
                } else {
                    1
                }
            }
            Some(e) if e.payload.is_none() && op.payload.is_some() => 1,
            Some(_) => 0,
        }
    }

    fn held_payloads(&self) -> LogRanges<L> {
        self.payloads.clone()
    }

    fn evict_payloads(&mut self, ranges: &LogRanges<L>) {
        for (log, r) in ranges.iter() {
            if r.is_empty() {
                continue;
            }
            let (lo, hi) = log_bounds(log);
            let mut units_removed: Units = 0;
            {
                let write_txn = self.db.begin_write().expect("redb begin_write");
                {
                    let mut table = write_txn.open_table(TABLE).expect("redb open_table");
                    let mut updates = vec![];
                    for row in table
                        .range(lo.as_slice()..=hi.as_slice())
                        .expect("redb range")
                    {
                        let (k, v) = row.expect("redb row");
                        let key = k.value().to_vec();
                        let seq = row_seq::<L>(&key);
                        if r.contains(seq) {
                            let op: Op =
                                postcard::from_bytes(v.value()).expect("postcard decode");
                            if op.payload.is_some() {
                                units_removed += 1;
                                updates.push((
                                    key,
                                    Op {
                                        header: op.header,
                                        payload: None,
                                    },
                                ));
                            }
                        }
                    }
                    for (key, op) in updates {
                        let bytes = postcard::to_stdvec(&op).expect("postcard encode");
                        table
                            .insert(key.as_slice(), bytes.as_slice())
                            .expect("redb insert");
                    }
                }
                write_txn.commit().expect("redb commit");
            }
            self.usage -= units_removed;
            self.rebuild_log(log).expect("rebuild_log");
        }
    }

    fn evict(&mut self, ranges: &LogRanges<L>) {
        for (log, r) in ranges.iter() {
            if r.is_empty() {
                continue;
            }
            let (lo, hi) = log_bounds(log);
            let mut units_removed: Units = 0;
            {
                let write_txn = self.db.begin_write().expect("redb begin_write");
                {
                    let mut table = write_txn.open_table(TABLE).expect("redb open_table");
                    let mut removals = vec![];
                    for row in table
                        .range(lo.as_slice()..=hi.as_slice())
                        .expect("redb range")
                    {
                        let (k, v) = row.expect("redb row");
                        let key = k.value().to_vec();
                        let seq = row_seq::<L>(&key);
                        if r.contains(seq) {
                            let op: Op =
                                postcard::from_bytes(v.value()).expect("postcard decode");
                            units_removed += if op.payload.is_some() { 2 } else { 1 };
                            removals.push(key);
                        }
                    }
                    for key in removals {
                        table.remove(key.as_slice()).expect("redb remove");
                    }
                }
                write_txn.commit().expect("redb commit");
            }
            self.usage -= units_removed;
            self.rebuild_log(log).expect("rebuild_log");
        }
    }
}

#[cfg(test)]
mod tests {
    // Only bring `AsyncStorage`/`AsyncEvictableStorage` into scope here, not
    // the sync `Storage`/`EvictableStorage` traits `DiskRelayStore` also
    // implements: both would apply to `store.ingest(...)` etc. and make
    // dot-call resolution ambiguous. The oracle's sync calls below use
    // fully-qualified `dash_router_core::Storage::...` paths instead, which
    // doesn't bring the trait into scope.
    use dash_router_core::{LogRanges, Op, OpsMap, Ranges, Seq};
    use proptest::prelude::*;

    use super::{DiskRelayStore, LogKey};
    use crate::storage::{AsyncEvictableStorage, AsyncStorage};

    fn op(h: u8, payload: bool) -> Op {
        Op {
            header: vec![h],
            payload: payload.then(|| vec![h; 8]),
        }
    }

    #[tokio::test]
    async fn disk_store_matches_the_opsmap_oracle_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("relay.redb");
        let mut oracle = OpsMap::<u32>::default();
        {
            let mut store = DiskRelayStore::<u32>::open(&path).unwrap();
            // Interleave ingests (incl. duplicate + payload upgrade) and evictions.
            for (log, seq, o) in [
                (7, 0, op(1, true)),
                (7, 1, op(2, false)),
                (9, 0, op(3, true)),
                (7, 1, op(4, true)), // upgrade
                (7, 0, op(1, true)), // duplicate
            ] {
                assert_eq!(
                    store.ingest_delta(&log, seq, &o).await.unwrap(),
                    dash_router_core::EvictableStorage::ingest_delta(&oracle, &log, seq, &o),
                );
                store.ingest(log, seq, o.clone()).await.unwrap();
                dash_router_core::Storage::ingest(&mut oracle, log, seq, o);
            }
            let gc = LogRanges::from_pairs([(7u32, Ranges::range(0, 1))]);
            store.evict_payloads(&gc).await.unwrap();
            dash_router_core::EvictableStorage::evict_payloads(&mut oracle, &gc);
            let cut = LogRanges::from_pairs([(9u32, Ranges::full())]);
            store.evict(&cut).await.unwrap();
            dash_router_core::EvictableStorage::evict(&mut oracle, &cut);

            let oracle_held_all = dash_router_core::Storage::held_all(&oracle);
            assert_eq!(store.held_all().await.unwrap(), oracle_held_all);
            assert_eq!(
                store.held_payloads().await.unwrap(),
                dash_router_core::EvictableStorage::held_payloads(&oracle)
            );
            assert_eq!(store.usage().await.unwrap(), dash_router_core::EvictableStorage::usage(&oracle));
            let mut got = store.fetch(&oracle_held_all).await.unwrap();
            let mut want = dash_router_core::Storage::fetch(&oracle, &oracle_held_all);
            got.sort();
            want.sort();
            assert_eq!(got, want);
        }
        // Reopen: the startup scan rebuilds the same summaries.
        let store = DiskRelayStore::<u32>::open(&path).unwrap();
        assert_eq!(store.held_all().await.unwrap(), dash_router_core::Storage::held_all(&oracle));
        assert_eq!(
            store.held_payloads().await.unwrap(),
            dash_router_core::EvictableStorage::held_payloads(&oracle)
        );
        assert_eq!(store.usage().await.unwrap(), dash_router_core::EvictableStorage::usage(&oracle));
    }

    /// Task 10's node task `tokio::spawn`s a future holding an
    /// `AsyncEvictableStorage`; prove `DiskRelayStore`'s futures (picked up
    /// via the blanket bridge over the sync `Storage`/`EvictableStorage`
    /// impls above) really are `Send`, the same check `storage.rs` runs for
    /// `OpsMap` and `MemStore`.
    #[test]
    fn futures_are_send() {
        fn assert_send<T: Send>(_: T) {}

        let mut store = DiskRelayStore::<u32>::open(
            &tempfile::tempdir().unwrap().path().join("relay.redb"),
        )
        .unwrap();
        assert_send(AsyncStorage::ingest(&mut store, 0, 0, Op::default()));
        assert_send(AsyncEvictableStorage::usage(&store));
    }

    #[test]
    fn log_keys_are_fixed_width_and_order_preserving() {
        let mut a = Vec::new();
        let mut b = Vec::new();
        3u32.write_key(&mut a);
        300u32.write_key(&mut b);
        assert_eq!(a.len(), <u32 as LogKey>::WIDTH);
        assert!(a < b, "big-endian keeps numeric order");
        assert_eq!(<u32 as LogKey>::read_key(&a), 3);
        let arr = [9u8; 32];
        let mut k = Vec::new();
        arr.write_key(&mut k);
        assert_eq!(<[u8; 32] as LogKey>::read_key(&k), arr);
    }

    /// A tiny op-log for the proptest, run against both the disk store and
    /// the `OpsMap` oracle in lockstep, asserting the three summaries match
    /// after every single step. This is the guard on the incremental cache:
    /// any drift between the incremental update path and a fresh scan would
    /// show up here.
    #[derive(Clone, Debug)]
    enum Step {
        Ingest { log: u32, seq: Seq, payload: bool, h: u8 },
        EvictPayloads { log: u32, start: Seq, end: Seq },
        Evict { log: u32, start: Seq, end: Seq },
    }

    fn step_strategy() -> impl Strategy<Value = Step> {
        prop_oneof![
            (0..4u32, 0..8u32, any::<bool>(), any::<u8>()).prop_map(|(log, seq, payload, h)| {
                Step::Ingest { log, seq, payload, h }
            }),
            (0..4u32, 0..8u32, 0..8u32).prop_map(|(log, start, end)| Step::EvictPayloads {
                log,
                start: start.min(end),
                end: start.max(end),
            }),
            (0..4u32, 0..8u32, 0..8u32).prop_map(|(log, start, end)| Step::Evict {
                log,
                start: start.min(end),
                end: start.max(end),
            }),
        ]
    }

    proptest! {
        #[test]
        fn disk_store_tracks_the_opsmap_oracle_over_arbitrary_steps(
            steps in prop::collection::vec(step_strategy(), 0..40)
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("relay.redb");
                let mut store = DiskRelayStore::<u32>::open(&path).unwrap();
                let mut oracle = OpsMap::<u32>::default();

                for step in steps {
                    match step {
                        Step::Ingest { log, seq, payload, h } => {
                            let o = op(h, payload);
                            let want_delta = dash_router_core::EvictableStorage::ingest_delta(&oracle, &log, seq, &o);
                            let got_delta = store.ingest_delta(&log, seq, &o).await.unwrap();
                            prop_assert_eq!(got_delta, want_delta);
                            store.ingest(log, seq, o.clone()).await.unwrap();
                            dash_router_core::Storage::ingest(&mut oracle, log, seq, o);
                        }
                        Step::EvictPayloads { log, start, end } => {
                            let ranges =
                                LogRanges::from_pairs([(log, Ranges::range(start, end))]);
                            store.evict_payloads(&ranges).await.unwrap();
                            dash_router_core::EvictableStorage::evict_payloads(&mut oracle, &ranges);
                        }
                        Step::Evict { log, start, end } => {
                            let ranges =
                                LogRanges::from_pairs([(log, Ranges::range(start, end))]);
                            store.evict(&ranges).await.unwrap();
                            dash_router_core::EvictableStorage::evict(&mut oracle, &ranges);
                        }
                    }
                    prop_assert_eq!(store.held_all().await.unwrap(), dash_router_core::Storage::held_all(&oracle));
                    prop_assert_eq!(
                        store.held_payloads().await.unwrap(),
                        dash_router_core::EvictableStorage::held_payloads(&oracle)
                    );
                    prop_assert_eq!(store.usage().await.unwrap(), dash_router_core::EvictableStorage::usage(&oracle));
                }
                Ok(())
            })?;
        }
    }
}
