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
    router: {{ want_ttl_ms: 800, have_ttl_ms: 800, relay_cap: 1048576 }}
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
