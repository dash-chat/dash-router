//! Scenario configuration: the YAML shape and its translation into a
//! machine, an initial state, and a behavior.
//!
//! The model itself has almost no knobs — a topology and a
//! [`RouterConfig`] per node. Everything else in here configures the
//! *driver*: what gets delivered when, what gets lost, what intervals get
//! sampled, who appends and how often.

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, ensure};
use dash_router_core::{NodeMachine, NodeState, RouterConfig, Units};
use dash_router_net_model::Topology;
use polestar::time::RealTime;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde::{Deserialize, Serialize};

use crate::{
    LogId, NodeId, SimNet, SimNetState,
    behavior::{SimBehavior, SimParams},
    policy::IntervalPolicy,
    sim::Simulation,
};

/// A whole config file: defaults plus named scenarios.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub defaults: Defaults,
    pub scenarios: BTreeMap<String, ScenarioSpec>,
}

impl Config {
    pub fn from_yaml(yaml: &str) -> anyhow::Result<Self> {
        Ok(serde_norway::from_str(yaml)?)
    }
}

/// Values a scenario inherits unless it overrides them.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Defaults {
    pub seeds: u64,
    pub duration_ms: u64,
    pub sample_interval_ms: u64,
}

impl Default for Defaults {
    fn default() -> Self {
        Self {
            seeds: 8,
            duration_ms: 30_000,
            sample_interval_ms: 1_000,
        }
    }
}

/// One scenario: a network, a fault model, a policy, a workload.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScenarioSpec {
    pub nodes: u32,
    pub topology: TopologySpec,
    /// Per-message loss probability in [0, 1].
    #[serde(default)]
    pub loss: f64,
    pub latency_ms: LatencySpec,
    pub router: RouterSpec,
    pub storage: StorageSpec,
    pub policy: PolicySpec,
    pub workload: WorkloadSpec,
    pub seeds: Option<u64>,
    pub duration_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum TopologySpec {
    /// A uniformly random labelled tree, plus `extra_edges * nodes`
    /// random cross-links (0.0 = pure spanning tree).
    RandomTree {
        #[serde(default)]
        extra_edges: f64,
    },
    /// First node is the hub.
    Star,
    Path,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "distribution", rename_all = "kebab-case")]
pub enum LatencySpec {
    Uniform {
        min_ms: f64,
        max_ms: f64,
    },
    /// Log-normal around a median: the long-tail shape real LANs have.
    LogNormal {
        median_ms: f64,
        sigma: f64,
    },
}

impl LatencySpec {
    pub fn sample(&self, rng: &mut impl Rng) -> Duration {
        let ms = match self {
            LatencySpec::Uniform { min_ms, max_ms } => rng.random_range(*min_ms..=*max_ms),
            LatencySpec::LogNormal { median_ms, sigma } => {
                let dist = rand_distr::LogNormal::new(median_ms.ln(), *sigma)
                    .expect("validated at load time");
                rng.sample(dist)
            }
        };
        Duration::from_secs_f64(ms.max(0.001) / 1000.0)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RouterSpec {
    pub want_ttl_ms: u64,
    pub have_ttl_ms: u64,
}

/// Storage-layer knobs. Split from [`RouterSpec`] because the storage-less
/// router has no cap of its own: `relay_cap` bounds the node's
/// `RelayStoreMachine` (the shell's decision, not the protocol's).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageSpec {
    pub relay_cap: Units,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PolicySpec {
    pub want: IntervalPolicy,
    pub have: IntervalPolicy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkloadSpec {
    /// Number of writer nodes; writer `w` authors log `w`, and every
    /// node subscribes to every log.
    pub writers: u8,
    /// Poisson arrival rate of appends, summed across all writers.
    pub appends_per_sec: f64,
    #[serde(default = "default_payload")]
    pub payload_bytes: usize,
}

fn default_payload() -> usize {
    64
}

fn ms(v: u64) -> Duration {
    Duration::from_millis(v)
}

impl ScenarioSpec {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(self.nodes >= 2, "need at least 2 nodes");
        ensure!(
            (0.0..1.0).contains(&self.loss),
            "loss must be in [0, 1): a network that loses everything never converges"
        );
        ensure!(
            (self.workload.writers as u32) <= self.nodes,
            "more writers than nodes"
        );
        ensure!(self.workload.writers >= 1, "need at least one writer");
        // The coverage metric counts subscriber deliveries and assumes
        // `expected_coverage = nodes - 1`, i.e. every node subscribes to
        // every writer's log. `build` below subscribes every node to the
        // full `0..writers` log set unconditionally — this scenario shape
        // has no knob for a narrower subscription — so the invariant holds
        // by construction, and there is no per-scenario data to check
        // beyond `writers <= nodes` above. Assert `full_subscription()`
        // explicitly (rather than relying on that comment) so a future
        // per-node subscription knob fails this loudly instead of quietly
        // corrupting the metric.
        ensure!(
            self.full_subscription(),
            "subscriber-coverage metric requires every node to subscribe to every writer's log"
        );
        if let LatencySpec::LogNormal { median_ms, sigma } = &self.latency_ms {
            ensure!(*median_ms > 0.0 && *sigma >= 0.0, "bad log-normal latency");
        }
        Ok(())
    }

    /// Whether this scenario shape subscribes every node to every writer's
    /// log, as `build` constructs it — the invariant `expected_coverage =
    /// nodes - 1` relies on.
    fn full_subscription(&self) -> bool {
        (self.workload.writers as u32) <= self.nodes
    }

    pub fn topology(&self, seed: u64) -> Topology<NodeId> {
        let ids: Vec<NodeId> = (0..self.nodes).collect();
        match &self.topology {
            TopologySpec::Star => Topology::star(ids),
            TopologySpec::Path => Topology::path(ids),
            TopologySpec::RandomTree { extra_edges } => {
                let mut topo = Topology::random_tree(&ids, seed);
                let extras = (extra_edges * self.nodes as f64).round() as usize;
                let mut rng = ChaCha8Rng::seed_from_u64(seed ^ 0x5eed_ed6e);
                let mut added = 0;
                while added < extras {
                    let a = rng.random_range(0..self.nodes);
                    let b = rng.random_range(0..self.nodes);
                    if a != b {
                        topo.add_edge(a, b);
                        added += 1;
                    }
                }
                topo
            }
        }
    }

    /// Build one seeded, ready-to-run simulation.
    pub fn build(&self, seed: u64, defaults: &Defaults) -> anyhow::Result<Simulation> {
        self.validate().context("invalid scenario")?;
        let duration = ms(self.duration_ms.unwrap_or(defaults.duration_ms));
        let topology = self.topology(seed);
        let router_config = RouterConfig::<RealTime> {
            want_ttl: ms(self.router.want_ttl_ms).into(),
            have_ttl: ms(self.router.have_ttl_ms).into(),
        };
        let node_machine = NodeMachine::new(router_config, self.storage.relay_cap);
        let logs: Vec<LogId> = (0..self.workload.writers).collect();
        let state =
            SimNetState::new((0..self.nodes).map(|id| NodeState::new(id, logs.iter().copied())));
        let params = SimParams {
            n: self.nodes as usize,
            loss: self.loss,
            latency: self.latency_ms.clone(),
            want_policy: self.policy.want.clone(),
            have_policy: self.policy.have.clone(),
            writers: self.workload.writers,
            appends_per_sec: self.workload.appends_per_sec,
            payload_bytes: self.workload.payload_bytes,
            duration,
            sample_interval: ms(defaults.sample_interval_ms),
        };
        ensure!(
            topology.nodes().count() == self.nodes as usize,
            "topology node count mismatch"
        );
        let behavior = SimBehavior::new(topology.clone(), params, seed);
        let net = SimNet::new(topology, node_machine);
        Ok(Simulation::new(net, state, behavior, duration))
    }

    pub fn seeds(&self, defaults: &Defaults) -> u64 {
        self.seeds.unwrap_or(defaults.seeds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_config_round_trips_and_builds() {
        let yaml = r#"
defaults:
  seeds: 2
  duration_ms: 1000
scenarios:
  tiny:
    nodes: 5
    topology: { kind: random-tree, extra_edges: 0.2 }
    loss: 0.1
    latency_ms: { distribution: uniform, min_ms: 1, max_ms: 5 }
    router: { want_ttl_ms: 500, have_ttl_ms: 500 }
    storage: { relay_cap: 65536 }
    policy:
      want: { kind: density-scaled, min_ms: 200, max_ms: 400, ref_n: 10 }
      have: { kind: fixed, min_ms: 20, max_ms: 80 }
    workload: { writers: 2, appends_per_sec: 4.0 }
"#;
        let config = Config::from_yaml(yaml).unwrap();
        let spec = &config.scenarios["tiny"];
        assert_eq!(spec.seeds(&config.defaults), 2);
        spec.build(0, &config.defaults).unwrap();
    }
}
