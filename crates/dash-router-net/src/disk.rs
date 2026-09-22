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
///
/// `read_key` is fallible (`None` rather than panicking) because it decodes
/// bytes redb hands back from disk: a key shorter than `WIDTH` is a
/// malformed row (corruption, a foreign writer, a truncated file), not a
/// programmer error, and trait methods here never panic on that — see
/// [`row_seq`] and the fix-round-2 note on [`DiskRelayStore`].
pub trait LogKey: Ord + Clone {
    const WIDTH: usize;
    fn write_key(&self, out: &mut Vec<u8>);
    fn read_key(bytes: &[u8]) -> Option<Self>;
}

impl LogKey for [u8; 32] {
    const WIDTH: usize = 32;

    fn write_key(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self);
    }

    fn read_key(bytes: &[u8]) -> Option<Self> {
        bytes.get(..32)?.try_into().ok()
    }
}

impl LogKey for u32 {
    const WIDTH: usize = 4;

    fn write_key(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.to_be_bytes());
    }

    fn read_key(bytes: &[u8]) -> Option<Self> {
        Some(u32::from_be_bytes(bytes.get(..4)?.try_into().ok()?))
    }
}

impl LogKey for u8 {
    const WIDTH: usize = 1;

    fn write_key(&self, out: &mut Vec<u8>) {
        out.push(*self);
    }

    fn read_key(bytes: &[u8]) -> Option<Self> {
        bytes.first().copied()
    }
}

fn row_key<L: LogKey>(log: &L, seq: Seq) -> Vec<u8> {
    let mut out = Vec::with_capacity(L::WIDTH + 4);
    log.write_key(&mut out);
    out.extend_from_slice(&seq.to_be_bytes());
    out
}

/// The seq suffix of a row key, or `None` if `key` isn't exactly
/// `L::WIDTH + 4` bytes — a malformed row, treated the same as a row we
/// can't decode: skipped and counted, never panicked on (Ruling B: trait
/// methods never panic; a row we can't parse is a row we don't hold).
fn row_seq<L: LogKey>(key: &[u8]) -> Option<Seq> {
    if key.len() != L::WIDTH + 4 {
        return None;
    }
    let suffix: [u8; 4] = key[L::WIDTH..].try_into().ok()?;
    Some(Seq::from_be_bytes(suffix))
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
///
/// **Invariant (fix round 2):** after any degraded operation the cache may
/// *under*-report — treat data as absent/evicted that redb still holds —
/// but must never *over*-report — claim to hold data redb no longer
/// reflects. Under-reporting is the safe direction: the protocol already
/// treats un-advertised data as absent and repairs it through the normal
/// want/have cycle or the next reopen's full scan; over-reporting would
/// promise data a `fetch` can't actually produce. This is why a failed
/// [`Self::rebuild_log`] after a *committed* evict drops that log from the
/// cache entirely (see [`Self::drop_log_from_cache`]) rather than leaving
/// the pre-eviction entries in place.
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
        let mut io_errors: u64 = 0;
        {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(TABLE)?;
            // Row-level problems (a malformed key, an undecodable value) are
            // skipped-and-counted rather than failing the whole open: a row
            // we can't parse is a row we don't hold, not a reason to refuse
            // to start. Only transaction/table-level failures (begin_read,
            // open_table, range itself) still propagate via `?`, since
            // there's no store to construct at all if those fail.
            for row in table.range::<&[u8]>(..)? {
                let (k, v) = match row {
                    Ok(kv) => kv,
                    Err(_) => {
                        io_errors += 1;
                        continue;
                    }
                };
                let key = k.value();
                let Some(log) = key.get(..L::WIDTH).and_then(L::read_key) else {
                    io_errors += 1;
                    continue;
                };
                let Some(seq) = row_seq::<L>(key) else {
                    io_errors += 1;
                    continue;
                };
                let op: Op = match postcard::from_bytes(v.value()) {
                    Ok(op) => op,
                    Err(_) => {
                        io_errors += 1;
                        continue;
                    }
                };
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
            io_errors: AtomicU64::new(io_errors),
        })
    }

    /// Count of redb operations that failed and were degraded (dropped
    /// write / partial read / skipped malformed row) rather than
    /// panicking. See the struct's doc comment: this is the diagnostic
    /// surface for that degradation.
    pub fn io_errors(&self) -> u64 {
        self.io_errors.load(Ordering::Relaxed)
    }

    fn note_io_error(&self) {
        self.io_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Drop `log` from both `held` and `payloads` entirely. Used when a
    /// redb error leaves us unable to trust what's still on disk for this
    /// log (see the struct doc's under-report-not-over-report invariant):
    /// under-reporting "we hold nothing for this log" is always safe,
    /// since the protocol treats un-advertised data as absent and repairs
    /// it later, while over-reporting could promise a `fetch` can't
    /// deliver.
    fn drop_log_from_cache(&mut self, log: &L) {
        set_log_entry(&mut self.held, log, Ranges::empty());
        set_log_entry(&mut self.payloads, log, Ranges::empty());
    }

    /// Recompute `held`/`payloads` for exactly this log from a fresh
    /// prefix scan: eviction is rare, so recompute-per-touched-log keeps
    /// the incremental cache trivially correct rather than requiring a
    /// delta-tracking eviction path.
    ///
    /// A malformed row's key (see [`row_seq`]) is skipped-and-counted, not
    /// fatal to the rebuild. Only a transaction/table-level failure
    /// (`begin_read`/`open_table`/`range`) or an undecodable value fails
    /// the whole rebuild — `held`/`payloads` are only written after the
    /// scan fully succeeds, so callers can tell "scan failed, cache
    /// unchanged" (`Err`) apart from "scan succeeded" (`Ok`).
    fn rebuild_log(&mut self, log: &L) -> Result<()> {
        let (lo, hi) = log_bounds(log);
        let mut seqs = vec![];
        let mut pseqs = vec![];
        {
            let read_txn = self.db.begin_read()?;
            let table = read_txn.open_table(TABLE)?;
            for row in table.range(lo.as_slice()..=hi.as_slice())? {
                let (k, v) = row?;
                let Some(seq) = row_seq::<L>(k.value()) else {
                    self.note_io_error();
                    continue;
                };
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
                let Some(seq) = row_seq::<L>(k.value()) else {
                    self.note_io_error();
                    continue;
                };
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
            // A malformed key (see `row_seq`) is skipped-and-counted rather
            // than failing the whole per-log write; returned alongside the
            // units removed so the outer match can count it without a
            // second borrow of `self` from inside the closure.
            let outcome: anyhow::Result<(Units, u64)> = (|| {
                let write_txn = self.db.begin_write()?;
                let mut units_removed: Units = 0;
                let mut malformed: u64 = 0;
                {
                    let mut table = write_txn.open_table(TABLE)?;
                    let mut updates = vec![];
                    for row in table.range(lo.as_slice()..=hi.as_slice())? {
                        let (k, v) = row?;
                        let key = k.value().to_vec();
                        let Some(seq) = row_seq::<L>(&key) else {
                            malformed += 1;
                            continue;
                        };
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
                Ok((units_removed, malformed))
            })();

            match outcome {
                Ok((units_removed, malformed)) => {
                    self.usage -= units_removed;
                    for _ in 0..malformed {
                        self.note_io_error();
                    }
                    // The write already committed. On success, rebuild the
                    // cache from a fresh scan; on failure, we can no longer
                    // trust the pre-eviction entries for this log (they may
                    // now over-report), so drop it from the cache entirely
                    // instead — under-reporting is always the safe
                    // direction (see the struct doc's invariant).
                    if self.rebuild_log(log).is_err() {
                        self.note_io_error();
                        self.drop_log_from_cache(log);
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
            let outcome: anyhow::Result<(Units, u64)> = (|| {
                let write_txn = self.db.begin_write()?;
                let mut units_removed: Units = 0;
                let mut malformed: u64 = 0;
                {
                    let mut table = write_txn.open_table(TABLE)?;
                    let mut removals = vec![];
                    for row in table.range(lo.as_slice()..=hi.as_slice())? {
                        let (k, v) = row?;
                        let key = k.value().to_vec();
                        let Some(seq) = row_seq::<L>(&key) else {
                            malformed += 1;
                            continue;
                        };
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
                Ok((units_removed, malformed))
            })();

            match outcome {
                Ok((units_removed, malformed)) => {
                    self.usage -= units_removed;
                    for _ in 0..malformed {
                        self.note_io_error();
                    }
                    if self.rebuild_log(log).is_err() {
                        self.note_io_error();
                        self.drop_log_from_cache(log);
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
            assert_eq!(
                store.usage().await.unwrap(),
                dash_router_core::EvictableStorage::usage(&oracle)
            );
            let mut got = store.fetch(&oracle_held_all).await.unwrap();
            let mut want = dash_router_core::Storage::fetch(&oracle, &oracle_held_all);
            got.sort();
            want.sort();
            assert_eq!(got, want);
        }
        // Reopen: the startup scan rebuilds the same summaries.
        let store = DiskRelayStore::<u32>::open(&path).unwrap();
        assert_eq!(
            store.held_all().await.unwrap(),
            dash_router_core::Storage::held_all(&oracle)
        );
        assert_eq!(
            store.held_payloads().await.unwrap(),
            dash_router_core::EvictableStorage::held_payloads(&oracle)
        );
        assert_eq!(
            store.usage().await.unwrap(),
            dash_router_core::EvictableStorage::usage(&oracle)
        );
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

    /// Fix round 2, finding 1: a row whose key is shorter than
    /// `L::WIDTH + 4` used to panic (`row_seq`'s `.expect(...)`, `u32`'s
    /// `read_key`'s `.expect(...)`); it must instead be skipped and
    /// counted. Faked cheaply by hand-inserting a too-short raw key
    /// directly through redb. `fetch`'s per-log prefix scan never even
    /// visits a key this malformed (it falls outside every `u32` log's
    /// 8-byte bounds), so this specifically exercises `open`'s unscoped
    /// full-table scan, which does visit it.
    #[tokio::test]
    async fn a_malformed_length_key_is_skipped_and_counted_not_panicked() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("relay.redb");
        {
            let mut store = DiskRelayStore::<u32>::open(&path).unwrap();
            store.ingest(3, 0, op(1, true)).await.unwrap();

            let write_txn = store.db.begin_write().unwrap();
            {
                let mut table = write_txn.open_table(super::TABLE).unwrap();
                table
                    .insert([1u8, 2, 3].as_slice(), b"irrelevant".as_slice())
                    .unwrap();
            }
            write_txn.commit().unwrap();
            assert_eq!(store.io_errors(), 0);
        }

        let store = DiskRelayStore::<u32>::open(&path).unwrap();
        assert!(
            store.io_errors() > 0,
            "the malformed-length row was skipped and counted on reopen, not panicked on"
        );
        assert_eq!(
            store.held_all().await.unwrap().get(&3u32),
            Some(&Ranges::from_seqs([0])),
            "the legitimate row still round-trips despite the malformed one"
        );
    }

    /// Fix round 2, finding 2: a `rebuild_log` failure that happens *after*
    /// an evict/evict_payloads write has already committed must not leave
    /// the cache over-reporting (still advertising rows the write just
    /// removed, or — as tested here — leaving payload/held ranges whose
    /// accuracy `rebuild_log` couldn't actually reconfirm). Faked cheaply:
    /// corrupt a value at a seq the eviction below doesn't target, so the
    /// write itself succeeds, but `rebuild_log`'s full-log rescan (which,
    /// unlike the write's own range-filtered scan, decodes every row in
    /// the log unconditionally) trips over it and fails.
    #[tokio::test]
    async fn a_rebuild_failure_after_a_committed_evict_drops_the_log_rather_than_over_reporting() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("relay.redb");
        let mut store = DiskRelayStore::<u32>::open(&path).unwrap();

        store.ingest(2, 0, op(1, true)).await.unwrap();
        store.ingest(2, 1, op(2, true)).await.unwrap();
        assert_eq!(store.io_errors(), 0);

        // Corrupt seq 1's value directly, bypassing the API: the cache
        // still (correctly, for now) believes seq 1 is held with a
        // payload. This row is outside the eviction range below, so the
        // eviction's own write succeeds — only the post-write rescan
        // visits it.
        let key1 = super::row_key(&2u32, 1);
        {
            let write_txn = store.db.begin_write().unwrap();
            {
                let mut table = write_txn.open_table(super::TABLE).unwrap();
                table
                    .insert(key1.as_slice(), b"not valid postcard".as_slice())
                    .unwrap();
            }
            write_txn.commit().unwrap();
        }

        // Evict just seq 0's payload. The write succeeds; the subsequent
        // `rebuild_log` rescans the whole log and fails on seq 1.
        let ranges = LogRanges::from_pairs([(2u32, Ranges::range(0, 1))]);
        store.evict_payloads(&ranges).await.unwrap();

        // Under-report, not over-report: log 2 is dropped from both
        // summaries entirely rather than left showing pre-eviction (now
        // unverifiable) ranges.
        assert!(
            store.held_all().await.unwrap().get(&2u32).is_none(),
            "a failed rebuild drops the log from held rather than over-reporting"
        );
        assert!(
            store.held_payloads().await.unwrap().get(&2u32).is_none(),
            "a failed rebuild drops the log from held_payloads rather than over-reporting"
        );
        assert!(store.io_errors() > 0, "the failed rebuild was counted");
    }

    /// Task 10's node task `tokio::spawn`s a future holding an
    /// `AsyncEvictableStorage`; prove `DiskRelayStore`'s futures (picked up
    /// via the blanket bridge over the sync `Storage`/`EvictableStorage`
    /// impls above) really are `Send`, the same check `storage.rs` runs for
    /// `OpsMap` and `MemStore`.
    #[test]
    fn futures_are_send() {
        fn assert_send<T: Send>(_: T) {}

        let mut store =
            DiskRelayStore::<u32>::open(&tempfile::tempdir().unwrap().path().join("relay.redb"))
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
        assert_eq!(<u32 as LogKey>::read_key(&a), Some(3));
        let arr = [9u8; 32];
        let mut k = Vec::new();
        arr.write_key(&mut k);
        assert_eq!(<[u8; 32] as LogKey>::read_key(&k), Some(arr));
        // A malformed (too-short) key is `None`, not a panic — the fix
        // round 2 requirement `LogKey::read_key` exists to satisfy.
        assert_eq!(<u32 as LogKey>::read_key(&[1, 2]), None);
        assert_eq!(<[u8; 32] as LogKey>::read_key(&[1, 2]), None);
        assert_eq!(<u8 as LogKey>::read_key(&[]), None);
    }

    /// A tiny op-log for the proptest, run against both the disk store and
    /// the `OpsMap` oracle in lockstep, asserting the three summaries match
    /// after every single step. This is the guard on the incremental cache:
    /// any drift between the incremental update path and a fresh scan would
    /// show up here.
    #[derive(Clone, Debug)]
    enum Step {
        Ingest {
            log: u32,
            seq: Seq,
            payload: bool,
            h: u8,
        },
        EvictPayloads {
            log: u32,
            start: Seq,
            end: Seq,
        },
        Evict {
            log: u32,
            start: Seq,
            end: Seq,
        },
    }

    fn step_strategy() -> impl Strategy<Value = Step> {
        prop_oneof![
            (0..4u32, 0..8u32, any::<bool>(), any::<u8>()).prop_map(|(log, seq, payload, h)| {
                Step::Ingest {
                    log,
                    seq,
                    payload,
                    h,
                }
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
