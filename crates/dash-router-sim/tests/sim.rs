//! End-to-end simulation tests: determinism, convergence, repair.

use dash_router_sim::{Config, report::ScenarioReport};

fn config(loss: f64, nodes: u32, duration_ms: u64) -> Config {
    let yaml = format!(
        r#"
defaults:
  seeds: 3
  duration_ms: {duration_ms}
scenarios:
  test:
    nodes: {nodes}
    topology: {{ kind: random-tree, extra_edges: 0.0 }}
    loss: {loss}
    latency_ms: {{ distribution: uniform, min_ms: 2, max_ms: 20 }}
    # want_ttl may exceed the want interval: Wants flood the network, so
    # suppression is fed by requests every node eventually hears, and
    # multi-hop repair no longer depends on ttl expiry. (An earlier,
    # non-flooding protocol starved here — kept above the interval floor
    # deliberately as a regression check on that.)
    router: {{ want_ttl_ms: 800, have_ttl_ms: 800 }}
    storage: {{ relay_cap: 1048576 }}
    policy:
      want: {{ kind: density-scaled, min_ms: 400, max_ms: 900, ref_n: 10 }}
      have: {{ kind: fixed, min_ms: 30, max_ms: 120 }}
    workload: {{ writers: 2, appends_per_sec: 3.0 }}
"#
    );
    Config::from_yaml(&yaml).unwrap()
}

#[test]
fn a_lossless_network_reaches_full_coverage() {
    let config = config(0.0, 8, 5_000);
    let spec = &config.scenarios["test"];
    let mut sim = spec.build(7, &config.defaults).unwrap();
    let record = sim.run(7).unwrap();
    assert!(record.ops_authored > 0, "workload should author ops");
    assert_eq!(
        record.ops_missed, 0,
        "lossless flood must cover everyone: {record:?}"
    );
    assert_eq!(record.drops, 0);
}

#[test]
fn a_lossy_network_repairs_through_want_have() {
    let config = config(0.2, 8, 8_000);
    let spec = &config.scenarios["test"];
    let mut runs = Vec::new();
    for seed in 0..3 {
        let mut sim = spec.build(seed, &config.defaults).unwrap();
        runs.push(sim.run(seed).unwrap());
    }
    let report = ScenarioReport::new("test".into(), spec.clone(), runs);
    assert!(
        report.sometimes.loss_exercised,
        "20% loss must drop something"
    );
    assert!(
        report.summary.coverage_rate > 0.9,
        "repair should recover most ops: {:?}",
        report.summary
    );
    assert!(
        report.sometimes.backfill_exercised,
        "recovery implies non-fresh Haves taught something"
    );
}

#[test]
fn identical_seeds_reproduce_identical_runs() {
    let config = config(0.15, 6, 3_000);
    let spec = &config.scenarios["test"];
    let a = spec.build(42, &config.defaults).unwrap().run(42).unwrap();
    let b = spec.build(42, &config.defaults).unwrap().run(42).unwrap();
    assert_eq!(a, b, "same seed must replay exactly");
    let c = spec.build(43, &config.defaults).unwrap().run(43).unwrap();
    assert_ne!(a, c, "different seeds should diverge");
}

/// With `subscribers: k`, log w is subscribed by nodes (w + i) % nodes for
/// i in 0..k, coverage counts only those subscribers (minus the author),
/// and unsubscribed traffic finally lands in relay stores.
#[test]
fn partial_subscription_exercises_the_relay_path() {
    let yaml = r#"
defaults: { seeds: 1, duration_ms: 3000 }
scenarios:
  partial:
    nodes: 6
    topology: { kind: path }
    loss: 0.0
    latency_ms: { distribution: uniform, min_ms: 1, max_ms: 3 }
    router: { want_ttl_ms: 500, have_ttl_ms: 500 }
    storage: { relay_cap: 1048576 }
    policy:
      want: { kind: fixed, min_ms: 100, max_ms: 200 }
      have: { kind: fixed, min_ms: 20, max_ms: 60 }
    workload: { writers: 2, appends_per_sec: 4.0, subscribers: 2 }
"#;
    let config = dash_router_sim::Config::from_yaml(yaml).unwrap();
    let spec = &config.scenarios["partial"];

    // Log 0: author node 0, subscribers {0, 1}; expected coverage {1}.
    let expected = spec.expected_coverage();
    assert_eq!(
        expected[&0],
        std::collections::BTreeSet::from([1u32]),
        "author excluded from its own log's expected set"
    );
    assert_eq!(expected[&1], std::collections::BTreeSet::from([2u32]));

    let mut sim = spec.build(0, &config.defaults).unwrap();
    let record = sim.run(0).unwrap();
    assert_eq!(record.ops_missed, 0, "subscribers still fully covered");
    assert!(
        record.relay_occupancy_max > 0,
        "unsubscribed nodes hold relayed bytes: the relay path is live"
    );
    assert!(
        record.push_deliveries + record.pull_deliveries > 0,
        "every gossip delivery carries an origin"
    );
}

/// A tight relay cap plus maintenance: the relay saturates, eviction runs,
/// usage never exceeds the cap, and subscribers still get covered.
#[test]
fn cap_pressure_evicts_and_stays_within_cap() {
    let yaml = r#"
defaults: { seeds: 4, duration_ms: 3000 }
scenarios:
  pressure:
    nodes: 6
    topology: { kind: path }
    loss: 0.0
    latency_ms: { distribution: uniform, min_ms: 1, max_ms: 3 }
    router: { want_ttl_ms: 500, have_ttl_ms: 500 }
    storage: { relay_cap: 8, evict_at: 0.75, maintain_interval_ms: 100 }
    policy:
      want: { kind: fixed, min_ms: 100, max_ms: 200 }
      have: { kind: fixed, min_ms: 20, max_ms: 60 }
    workload: { writers: 2, appends_per_sec: 8.0, subscribers: 2 }
"#;
    let config = dash_router_sim::Config::from_yaml(yaml).unwrap();
    let spec = &config.scenarios["pressure"];
    let mut evictions = 0;
    for seed in 0..4 {
        let mut sim = spec.build(seed, &config.defaults).unwrap();
        let record = sim.run(seed).unwrap();
        assert!(record.relay_occupancy_max <= 8, "cap is a hard bound");
        evictions += record.payload_evictions + record.full_evictions;
    }
    assert!(
        evictions > 0,
        "maintenance never fired under sustained pressure"
    );
}

/// NativeSync counts toward coverage; AppGc runs and (kept-as-is ruling)
/// reopens wanted gaps — the run still terminates at the drain cap.
#[test]
fn native_sync_and_app_gc_are_proposed() {
    let yaml = r#"
defaults: { seeds: 2, duration_ms: 2000 }
scenarios:
  spontaneous:
    nodes: 4
    topology: { kind: path }
    loss: 0.0
    latency_ms: { distribution: uniform, min_ms: 1, max_ms: 3 }
    router: { want_ttl_ms: 500, have_ttl_ms: 500 }
    storage: { relay_cap: 1048576 }
    policy:
      want: { kind: fixed, min_ms: 100, max_ms: 200 }
      have: { kind: fixed, min_ms: 20, max_ms: 60 }
    workload:
      writers: 2
      appends_per_sec: 8.0
      native_sync_per_sec: 4.0
      app_gc: { interval_ms: 400, keep_last: 1 }
"#;
    let config = dash_router_sim::Config::from_yaml(yaml).unwrap();
    let spec = &config.scenarios["spontaneous"];
    let (mut syncs, mut gcs) = (0, 0);
    for seed in 0..2 {
        let mut sim = spec.build(seed, &config.defaults).unwrap();
        let record = sim.run(seed).unwrap();
        syncs += record.native_syncs;
        gcs += record.app_gc_runs;
    }
    assert!(syncs > 0, "native sync never proposed");
    assert!(gcs > 0, "app gc never proposed");
}

#[test]
fn jump_to_replays_to_the_same_state() {
    let config = config(0.1, 5, 1_500);
    let spec = &config.scenarios["test"];
    let mut sim = spec.build(3, &config.defaults).unwrap();
    for _ in 0..40 {
        if !sim.step().unwrap() {
            break;
        }
    }
    let mid = sim.net_state().clone();
    let steps = sim.steps();
    for _ in 0..20 {
        if !sim.step().unwrap() {
            break;
        }
    }
    sim.jump_to(steps).unwrap();
    assert_eq!(sim.net_state(), &mid);
    assert_eq!(sim.steps(), steps);
}

/// Origin attribution: push-flood deliveries and repair deliveries land in
/// separate latency buckets; out-of-band (None) deliveries in neither.
#[test]
fn delivery_latency_splits_by_have_origin() {
    use dash_router_sim::metrics::{DriverMetrics, HaveOrigin, Metrics};
    use std::collections::{BTreeMap, BTreeSet};
    use std::time::Duration;

    let expected = BTreeMap::from([(0u8, BTreeSet::from([1u32, 2, 3]))]);
    let mut m = Metrics::new(expected);
    let ms = Duration::from_millis;
    m.authored(0, 0, ms(0));
    m.delivered(1, 0, 0, ms(10), Some(HaveOrigin::Push));
    m.delivered(2, 0, 0, ms(500), Some(HaveOrigin::Repair));
    m.delivered(3, 0, 0, ms(20), None); // e.g. native sync
    // A repeat never double-counts.
    m.delivered(1, 0, 0, ms(999), Some(HaveOrigin::Repair));
    let r = m.finish(&DriverMetrics::default(), 0);
    assert_eq!(r.push_deliveries, 1);
    assert_eq!(r.pull_deliveries, 1);
    assert_eq!(r.t_push_ms.max, 10.0);
    assert_eq!(r.t_pull_ms.max, 500.0);
    assert_eq!(r.ops_fully_covered, 1);
}
