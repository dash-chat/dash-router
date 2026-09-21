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

use std::{
    collections::BTreeSet,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

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
///
/// Spec §3: relay-store errors are indistinguishable from sheds/evictions —
/// a transient disk hiccup must not kill the node task. So every fallible
/// redb call in the `Storage`/`EvictableStorage` impls below degrades
/// rather than panics: a failed write is treated as a shed (dropped, cache
/// left as-is), a failed read returns only what was readable. All summary
/// reads (`held_of`/`held_all`/`held_payloads`/`usage`/`ingest_delta`)
/// serve the in-memory cache directly and never touch redb at all, so they
/// cannot fail. Every swallowed error is counted in `io_errors` so this
/// degradation isn't silent.
pub struct DiskRelayStore<L: LogKey> {
    db: Database,
    /// Maintained incrementally (single writer: this store); rebuilt by a
    /// full scan in [`Self::open`]. A redb error during an update leaves
    /// this cache exactly as it was (see the module-level note above) —
    /// corruption or drift here is repaired by reopening.
    held: LogRanges<L>,
    payloads: LogRanges<L>,
    usage: Units,
    /// Count of redb operations that failed and were swallowed (degraded
    /// to a no-op/shed) rather than propagated. `AtomicU64` rather than a
    /// plain field so `fetch`/`ingest_delta`-style `&self` reads can bump
    /// it too; single store owner, so `Relaxed` ordering is enough.
    io_errors: AtomicU64,
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
            io_errors: AtomicU64::new(0),
        })
    }

    /// Count of redb operations that failed and were degraded (dropped
    /// write / partial read) rather than panicking. See the struct's
    /// doc comment: this is the diagnostic surface for that degradation.
    pub fn io_errors(&self) -> u64 {
        self.io_errors.load(Ordering::Relaxed)
    }

    fn note_io_error(&self) {
        self.io_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Recompute `held`/`payloads` for exactly this log from a fresh
    /// prefix scan: eviction is rare, so recompute-per-touched-log keeps
    /// the incremental cache trivially correct rather than requiring a
    /// delta-tracking eviction path. Returns `Err` on any redb failure
    /// without touching `held`/`payloads` (they're only written after the
    /// whole scan succeeds), so callers can leave the cache as-is on error.
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
// Controller ruling (fix round 1): redb errors must degrade, never panic —
// spec §3 treats relay-store errors as indistinguishable from sheds/
// evictions, so a transient disk hiccup must not kill the node task. Every
// fallible redb call below is therefore wrapped in a private `anyhow`
// closure and handled with `.ok()`/`match`, not `?`/`.expect(...)`:
// `ingest`/`evict`/`evict_payloads` drop the whole operation (cache
// untouched) on any failure, and `fetch` returns whatever was readable
// before the failure. `open`'s `Result` is still the fallible surface for
// construction. Every swallowed error bumps `io_errors`.
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
        let mut out = vec![];
        let read_txn = match self.db.begin_read() {
            Ok(txn) => txn,
            Err(_) => {
                self.note_io_error();
                return out;
            }
        };
        let table = match read_txn.open_table(TABLE) {
            Ok(t) => t,
            Err(_) => {
                self.note_io_error();
                return out;
            }
        };
        for (log, r) in ranges.iter() {
            if r.is_empty() {
                continue;
            }
            let (lo, hi) = log_bounds(log);
            let rows = match table.range(lo.as_slice()..=hi.as_slice()) {
                Ok(rows) => rows,
                Err(_) => {
                    self.note_io_error();
                    continue;
                }
            };
            for row in rows {
                let (k, v) = match row {
                    Ok(kv) => kv,
                    Err(_) => {
                        self.note_io_error();
                        continue;
                    }
                };
                let seq = row_seq::<L>(k.value());
                if !r.contains(seq) {
                    continue;
                }
                match postcard::from_bytes::<Op>(v.value()) {
                    Ok(op) => out.push((log.clone(), seq, op)),
                    Err(_) => self.note_io_error(),
                }
            }
        }
        // Whatever was readable before any failure — indistinguishable
        // from those rows having already been evicted.
        out
    }

    fn ingest(&mut self, log: L, seq: Seq, op: Op) {
        let key = row_key(&log, seq);
        // Do every fallible redb/postcard call inside this closure, and
        // only touch `self.held`/`self.payloads`/`self.usage` if it fully
        // succeeds: a failure anywhere here is a dropped write (a shed),
        // not a partial one, so the cache must stay exactly as it was.
        let outcome: anyhow::Result<(bool, bool, Units)> = (|| {
            let write_txn = self.db.begin_write()?;
            let (is_new, upgraded, delta) = {
                let mut table = write_txn.open_table(TABLE)?;
                let existing: Option<Op> = table
                    .get(key.as_slice())?
                    .map(|v| postcard::from_bytes(v.value()))
                    .transpose()?;
                match existing {
                    None => {
                        let delta = if op.payload.is_some() { 2 } else { 1 };
                        let bytes = postcard::to_stdvec(&op)?;
                        table.insert(key.as_slice(), bytes.as_slice())?;
                        (true, false, delta)
                    }
                    Some(e) if e.payload.is_none() && op.payload.is_some() => {
                        let bytes = postcard::to_stdvec(&op)?;
                        table.insert(key.as_slice(), bytes.as_slice())?;
                        (false, true, 1)
                    }
                    Some(_) => {
                        // Duplicate: op already held at least as good. No write.
                        (false, false, 0)
                    }
                }
            };
            write_txn.commit()?;
            Ok((is_new, upgraded, delta))
        })();

        let (is_new, upgraded, delta) = match outcome {
            Ok(outcome) => outcome,
            Err(_) => {
                self.note_io_error();
                return;
            }
        };
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

    /// Served entirely from the `held`/`payloads` cache — never touches
    /// redb, so this can't fail. This mirrors `OpsMap::ingest_delta`
    /// exactly as long as the cache is accurate, which `ingest`/`evict`/
    /// `evict_payloads` maintain as an invariant (falling back to a stale
    /// cache, never a wrong one, on I/O failure — see their doc comments).
    fn ingest_delta(&self, log: &L, seq: Seq, op: &Op) -> Units {
        let already_held = self.held.get(log).is_some_and(|r| r.contains(seq));
        if !already_held {
            return if op.payload.is_some() { 2 } else { 1 };
        }
        let has_payload = self.payloads.get(log).is_some_and(|r| r.contains(seq));
        if !has_payload && op.payload.is_some() {
            1
        } else {
            0
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
            // As in `ingest`: every fallible call is inside this closure,
            // and `self.usage`/the cache are only touched on full success.
            let outcome: anyhow::Result<Units> = (|| {
                let write_txn = self.db.begin_write()?;
                let mut units_removed: Units = 0;
                {
                    let mut table = write_txn.open_table(TABLE)?;
                    let mut updates = vec![];
                    for row in table.range(lo.as_slice()..=hi.as_slice())? {
                        let (k, v) = row?;
                        let key = k.value().to_vec();
                        let seq = row_seq::<L>(&key);
                        if r.contains(seq) {
                            let op: Op = postcard::from_bytes(v.value())?;
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
                        let bytes = postcard::to_stdvec(&op)?;
                        table.insert(key.as_slice(), bytes.as_slice())?;
                    }
                }
                write_txn.commit()?;
                Ok(units_removed)
            })();

            match outcome {
                Ok(units_removed) => {
                    self.usage -= units_removed;
                    // The write already committed; a failure rebuilding the
                    // cache leaves it stale (still shows the pre-eviction
                    // payload range) rather than guessed-at — repaired by
                    // the next reopen's full scan.
                    if self.rebuild_log(log).is_err() {
                        self.note_io_error();
                    }
                }
                Err(_) => self.note_io_error(),
            }
        }
    }

    fn evict(&mut self, ranges: &LogRanges<L>) {
        for (log, r) in ranges.iter() {
            if r.is_empty() {
                continue;
            }
            let (lo, hi) = log_bounds(log);
            let outcome: anyhow::Result<Units> = (|| {
                let write_txn = self.db.begin_write()?;
                let mut units_removed: Units = 0;
                {
                    let mut table = write_txn.open_table(TABLE)?;
                    let mut removals = vec![];
                    for row in table.range(lo.as_slice()..=hi.as_slice())? {
                        let (k, v) = row?;
                        let key = k.value().to_vec();
                        let seq = row_seq::<L>(&key);
                        if r.contains(seq) {
                            let op: Op = postcard::from_bytes(v.value())?;
                            units_removed += if op.payload.is_some() { 2 } else { 1 };
                            removals.push(key);
                        }
                    }
                    for key in removals {
                        table.remove(key.as_slice())?;
                    }
                }
                write_txn.commit()?;
                Ok(units_removed)
            })();

            match outcome {
                Ok(units_removed) => {
                    self.usage -= units_removed;
                    if self.rebuild_log(log).is_err() {
                        self.note_io_error();
                    }
                }
                Err(_) => self.note_io_error(),
            }
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

    /// Fix round 1 (controller ruling): redb errors must degrade, never
    /// panic, since spec §3 treats relay-store errors as indistinguishable
    /// from sheds/evictions. Fakes a redb failure cheaply by writing a
    /// non-postcard row directly through redb (bypassing `DiskRelayStore`
    /// entirely, so its cache doesn't know the row exists) and checking
    /// that `fetch` skips it instead of panicking, and that `ingest` over
    /// the same corrupted row drops the write (leaving the cache and
    /// `usage` exactly as they were) instead of panicking — both counted
    /// in `io_errors` rather than silent.
    #[tokio::test]
    async fn degraded_redb_reads_are_swallowed_not_panicked() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("relay.redb");
        let mut store = DiskRelayStore::<u32>::open(&path).unwrap();

        store.ingest(1, 0, op(1, true)).await.unwrap();
        assert_eq!(store.io_errors(), 0);

        // Corrupt a second row directly through redb: `DiskRelayStore`
        // never sees this write, so its `held`/`payloads` cache has no
        // idea seq 1 exists at all.
        let corrupt_key = super::row_key(&1u32, 1);
        {
            let write_txn = store.db.begin_write().unwrap();
            {
                let mut table = write_txn.open_table(super::TABLE).unwrap();
                table
                    .insert(corrupt_key.as_slice(), b"not valid postcard".as_slice())
                    .unwrap();
            }
            write_txn.commit().unwrap();
        }

        // `fetch` returns only what was readable — the corrupted row is
        // skipped, not panicked on.
        let all = LogRanges::from_pairs([(1u32, Ranges::full())]);
        let got = store.fetch(&all).await.unwrap();
        assert_eq!(got, vec![(1u32, 0, op(1, true))]);
        assert!(store.io_errors() >= 1, "the decode failure was counted");

        // `ingest` reading that same corrupted row as its "existing" value
        // fails, so the whole write is dropped as a shed: cache and usage
        // stay exactly as they were, no panic.
        let held_before = store.held_all().await.unwrap();
        let payloads_before = store.held_payloads().await.unwrap();
        let usage_before = store.usage().await.unwrap();
        let errors_before = store.io_errors();
        store.ingest(1, 1, op(9, true)).await.unwrap();
        assert_eq!(
            store.held_all().await.unwrap(),
            held_before,
            "held cache untouched by a shed ingest"
        );
        assert_eq!(
            store.held_payloads().await.unwrap(),
            payloads_before,
            "payload cache untouched by a shed ingest"
        );
        assert_eq!(
            store.usage().await.unwrap(),
            usage_before,
            "usage untouched by a shed ingest"
        );
        assert!(
            store.io_errors() > errors_before,
            "the failed ingest was also counted"
        );
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
