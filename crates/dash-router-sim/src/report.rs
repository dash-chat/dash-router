//! Per-scenario reports, YAML serialization, and baseline comparison.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{
    metrics::{RunRecord, Stats},
    scenario::ScenarioSpec,
};

/// Everything one scenario produced: an echo of its parameters, every
/// seeded run's record, and cross-seed aggregates.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScenarioReport {
    pub scenario: String,
    pub params: ScenarioSpec,
    pub runs: Vec<RunRecord>,
    pub summary: Summary,
    /// Path-coverage checks: across the seed set, did any run exercise
    /// this behavior at all? A `false` here means the scenario is not
    /// testing what you think it is.
    pub sometimes: Sometimes,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Summary {
    pub coverage_rate: f64,
    pub t_full_ms_mean: Stats,
    pub t_full_ms_p95: Stats,
    pub messages_per_op: f64,
    pub redundancy: Stats,
    pub duplicate_replies_per_op: f64,
    pub drops: f64,
    pub backpressure_events: f64,
    pub relay_occupancy_max: usize,
    pub inflight_max: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Sometimes {
    pub loss_exercised: bool,
    pub backfill_exercised: bool,
    pub backpressure_exercised: bool,
    pub eviction_exercised: bool,
    pub cap_pressure_exercised: bool,
}

impl ScenarioReport {
    pub fn new(scenario: String, params: ScenarioSpec, runs: Vec<RunRecord>) -> Self {
        let n = runs.len().max(1) as f64;
        let total_ops: u64 = runs.iter().map(|r| r.ops_authored).sum();
        let covered: u64 = runs.iter().map(|r| r.ops_fully_covered).sum();
        let messages: u64 = runs.iter().map(|r| r.want_msgs + r.have_msgs).sum();
        let summary = Summary {
            coverage_rate: ratio(covered, total_ops),
            t_full_ms_mean: Stats::of(&runs.iter().map(|r| r.t_full_ms.mean).collect::<Vec<_>>()),
            t_full_ms_p95: Stats::of(&runs.iter().map(|r| r.t_full_ms.p95).collect::<Vec<_>>()),
            messages_per_op: ratio(messages, total_ops),
            redundancy: Stats::of(&runs.iter().map(|r| r.redundancy).collect::<Vec<_>>()),
            duplicate_replies_per_op: ratio(
                runs.iter().map(|r| r.duplicate_replies).sum(),
                total_ops,
            ),
            drops: runs.iter().map(|r| r.drops).sum::<u64>() as f64 / n,
            backpressure_events: runs
                .iter()
                .map(|r| r.shed_appends + r.fire_backpressure + r.forced_drops)
                .sum::<u64>() as f64
                / n,
            relay_occupancy_max: runs
                .iter()
                .map(|r| r.relay_occupancy_max)
                .max()
                .unwrap_or(0),
            inflight_max: runs.iter().map(|r| r.inflight_max).max().unwrap_or(0),
        };
        let sometimes = Sometimes {
            loss_exercised: runs.iter().any(|r| r.drops > 0),
            backfill_exercised: runs.iter().any(|r| r.backfill_receives > 0),
            backpressure_exercised: runs
                .iter()
                .any(|r| r.shed_appends + r.fire_backpressure + r.forced_drops > 0),
            eviction_exercised: runs
                .iter()
                .any(|r| r.payload_evictions + r.full_evictions > 0),
            cap_pressure_exercised: runs.iter().any(|r| {
                r.relay_occupancy_max as f64
                    >= params.storage.evict_at * params.storage.relay_cap as f64
            }),
        };
        Self {
            scenario,
            params,
            runs,
            summary,
            sometimes,
        }
    }

    pub fn to_yaml(&self) -> anyhow::Result<String> {
        Ok(serde_norway::to_string(self)?)
    }

    pub fn from_yaml_file(path: &Path) -> anyhow::Result<Self> {
        Ok(serde_norway::from_str(&std::fs::read_to_string(path)?)?)
    }

    /// One-line human summary.
    pub fn headline(&self) -> String {
        format!(
            "{}: coverage {:.1}% | t_full p95 {:.0}ms | {:.1} msgs/op | redundancy {:.2} | backpressure {:.0}/run",
            self.scenario,
            self.summary.coverage_rate * 100.0,
            self.summary.t_full_ms_p95.mean,
            self.summary.messages_per_op,
            self.summary.redundancy.mean,
            self.summary.backpressure_events,
        )
    }

    /// Compare against a baseline report, row per metric.
    pub fn diff(&self, baseline: &ScenarioReport) -> String {
        let rows: Vec<(&str, f64, f64)> = vec![
            (
                "coverage_rate",
                baseline.summary.coverage_rate,
                self.summary.coverage_rate,
            ),
            (
                "t_full_ms_mean",
                baseline.summary.t_full_ms_mean.mean,
                self.summary.t_full_ms_mean.mean,
            ),
            (
                "t_full_ms_p95",
                baseline.summary.t_full_ms_p95.mean,
                self.summary.t_full_ms_p95.mean,
            ),
            (
                "messages_per_op",
                baseline.summary.messages_per_op,
                self.summary.messages_per_op,
            ),
            (
                "redundancy",
                baseline.summary.redundancy.mean,
                self.summary.redundancy.mean,
            ),
            (
                "duplicate_replies_per_op",
                baseline.summary.duplicate_replies_per_op,
                self.summary.duplicate_replies_per_op,
            ),
            ("drops", baseline.summary.drops, self.summary.drops),
            (
                "backpressure_events",
                baseline.summary.backpressure_events,
                self.summary.backpressure_events,
            ),
        ];
        let mut out = format!(
            "{:<26} {:>12} {:>12} {:>9}\n",
            "metric", "baseline", "current", "delta"
        );
        for (name, base, cur) in rows {
            let delta = if base.abs() < f64::EPSILON {
                if cur.abs() < f64::EPSILON {
                    0.0
                } else {
                    f64::INFINITY
                }
            } else {
                (cur - base) / base * 100.0
            };
            out.push_str(&format!(
                "{name:<26} {base:>12.3} {cur:>12.3} {delta:>+8.1}%\n"
            ));
        }
        out
    }
}

fn ratio(a: u64, b: u64) -> f64 {
    if b == 0 { 0.0 } else { a as f64 / b as f64 }
}
