//! Collectors and the per-run record.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::{LogId, NodeId};

/// Coverage bookkeeping for one authored op.
#[derive(Clone, Debug)]
struct OpCoverage {
    born: Duration,
    covered: BTreeSet<NodeId>,
    full_at: Option<Duration>,
}

/// Live collectors, updated by the behavior as effects flow past.
#[derive(Clone, Debug, Default)]
pub struct Metrics {
    /// Per-log coverage targets: the subscriber set (minus the author) that
    /// must deliver an op for it to count as fully covered.
    expected: BTreeMap<LogId, BTreeSet<NodeId>>,
    ops: BTreeMap<(LogId, u32), OpCoverage>,

    pub want_msgs: u64,
    /// Have broadcasts, of any origin. The wire no longer marks a Have as
    /// "fresh" (author-origin) vs. a periodic reply — that distinction was
    /// dropped along with the storage-less router refactor — so this is a
    /// single count where the old model split into two.
    pub have_msgs: u64,

    pub receives: u64,
    /// Receives that produced no `NodeEffect::Deliver` and no growth of the
    /// receiving node's `router.held` ranges: pure overhead.
    pub redundant_receives: u64,
    /// Have receives (any Have, not just replies — the wire no longer
    /// marks fresh-vs-reply, see `have_msgs`) that taught nothing: the
    /// NACK-implosion signature (several nodes answered the same Want).
    pub duplicate_replies: u64,
    /// Have receives that *did* teach (a `Deliver` or `held` growth):
    /// repair working.
    pub backfill_receives: u64,

    pub drops: u64,
    /// Appends skipped because the in-flight buffer had no headroom.
    pub shed_appends: u64,
    /// Timer fires deferred for the same reason.
    pub fire_backpressure: u64,
    /// Deliveries converted to drops after too many deferrals.
    pub forced_drops: u64,

    relay_samples: Vec<(f64, usize)>,
    inflight_samples: Vec<usize>,
}

impl Metrics {
    pub fn new(expected: BTreeMap<LogId, BTreeSet<NodeId>>) -> Self {
        Self {
            expected,
            ..Default::default()
        }
    }

    pub fn authored(&mut self, log: LogId, seq: u32, now: Duration) {
        self.ops.insert(
            (log, seq),
            OpCoverage {
                born: now,
                covered: BTreeSet::new(),
                full_at: None,
            },
        );
    }

    pub fn delivered(&mut self, node: NodeId, log: LogId, seq: u32, now: Duration) {
        let Some(expected) = self.expected.get(&log) else {
            return;
        };
        if !expected.contains(&node) {
            return;
        }
        if let Some(op) = self.ops.get_mut(&(log, seq)) {
            op.covered.insert(node);
            if op.full_at.is_none() && op.covered.len() >= expected.len() {
                op.full_at = Some(now);
            }
        }
    }

    pub fn sample_occupancy(&mut self, relay_mean: f64, relay_max: usize, inflight: usize) {
        self.relay_samples.push((relay_mean, relay_max));
        self.inflight_samples.push(inflight);
    }

    /// Whether every authored op has reached full coverage.
    pub fn all_covered(&self) -> bool {
        self.ops.values().all(|op| op.full_at.is_some())
    }

    /// Freeze into the serializable record.
    pub fn finish(&self, seed: u64) -> RunRecord {
        let full_times: Vec<f64> = self
            .ops
            .values()
            .filter_map(|op| op.full_at.map(|t| (t - op.born).as_secs_f64() * 1000.0))
            .collect();
        let authored = self.ops.len() as u64;
        let fully_covered = full_times.len() as u64;
        RunRecord {
            seed,
            ops_authored: authored,
            ops_fully_covered: fully_covered,
            ops_missed: authored - fully_covered,
            t_full_ms: Stats::of(&full_times),
            want_msgs: self.want_msgs,
            have_msgs: self.have_msgs,
            receives: self.receives,
            redundant_receives: self.redundant_receives,
            redundancy: ratio(self.redundant_receives, self.receives),
            duplicate_replies: self.duplicate_replies,
            backfill_receives: self.backfill_receives,
            drops: self.drops,
            shed_appends: self.shed_appends,
            fire_backpressure: self.fire_backpressure,
            forced_drops: self.forced_drops,
            relay_occupancy_mean: mean(self.relay_samples.iter().map(|(m, _)| *m)),
            relay_occupancy_max: self
                .relay_samples
                .iter()
                .map(|(_, x)| *x)
                .max()
                .unwrap_or(0),
            inflight_mean: mean(self.inflight_samples.iter().map(|&x| x as f64)),
            inflight_max: self.inflight_samples.iter().copied().max().unwrap_or(0),
        }
    }
}

fn ratio(a: u64, b: u64) -> f64 {
    if b == 0 { 0.0 } else { a as f64 / b as f64 }
}

fn mean(xs: impl IntoIterator<Item = f64>) -> f64 {
    let (mut sum, mut n) = (0.0, 0u64);
    for x in xs {
        sum += x;
        n += 1;
    }
    if n == 0 { 0.0 } else { sum / n as f64 }
}

/// Summary statistics of a sample.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Stats {
    pub mean: f64,
    pub p95: f64,
    pub max: f64,
}

impl Stats {
    pub fn of(xs: &[f64]) -> Self {
        if xs.is_empty() {
            return Self::default();
        }
        let mut sorted = xs.to_vec();
        sorted.sort_by(|a, b| a.total_cmp(b));
        let p95_idx = ((sorted.len() as f64) * 0.95).ceil() as usize - 1;
        Self {
            mean: mean(sorted.iter().copied()),
            p95: sorted[p95_idx.min(sorted.len() - 1)],
            max: *sorted.last().unwrap(),
        }
    }
}

/// Everything one seeded run reports.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RunRecord {
    pub seed: u64,
    pub ops_authored: u64,
    pub ops_fully_covered: u64,
    pub ops_missed: u64,
    /// Time from append to full coverage, over covered ops.
    pub t_full_ms: Stats,
    pub want_msgs: u64,
    pub have_msgs: u64,
    pub receives: u64,
    pub redundant_receives: u64,
    pub redundancy: f64,
    pub duplicate_replies: u64,
    pub backfill_receives: u64,
    pub drops: u64,
    pub shed_appends: u64,
    pub fire_backpressure: u64,
    pub forced_drops: u64,
    pub relay_occupancy_mean: f64,
    pub relay_occupancy_max: usize,
    pub inflight_mean: f64,
    pub inflight_max: usize,
}
