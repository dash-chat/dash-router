//! Sweep want/have interval maximums over a grid and plot messages per
//! op: `sweep [--out dir] [--steps n] [--want-max 300:3000] ...`.
//!
//! The base scenario is fixed in code (a 20-node LAN, the `lan-20`
//! scenario from `scenarios/example.yaml`); only the interval policies'
//! `max_ms` vary. Each `min_ms` is held at the given value.

use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

use anyhow::Context;
use clap::Parser;
use dash_router_sim::{
    policy::IntervalPolicy,
    scenario::{
        Defaults, LatencySpec, PolicySpec, RouterSpec, ScenarioSpec, StorageSpec, TopologySpec,
        WorkloadSpec,
    },
    sweep::{self, Grid},
};

#[derive(Parser)]
#[command(about = "Sweep want/have interval maximums and plot messages per op")]
struct Args {
    /// Directory to write sweep.csv and sweep.html into.
    #[arg(long, default_value = "sweep-out")]
    out: PathBuf,
    /// Points per axis, spaced geometrically between the range ends.
    #[arg(long, default_value_t = 12)]
    steps: usize,
    /// Fixed lower end of the want interval.
    #[arg(long, default_value_t = 300.0)]
    want_min: f64,
    /// Range of want max_ms to sweep, as `lo:hi`.
    #[arg(long, default_value = "300:3000", value_parser = parse_range)]
    want_max: (f64, f64),
    /// Fixed lower end of the have interval.
    #[arg(long, default_value_t = 30.0)]
    have_min: f64,
    /// Range of have max_ms to sweep, as `lo:hi`.
    #[arg(long, default_value = "30:1000", value_parser = parse_range)]
    have_max: (f64, f64),
    /// Seeds per cell.
    #[arg(long, default_value_t = 8)]
    seeds: u64,
    /// Append window per run, in ms.
    #[arg(long, default_value_t = 20_000)]
    duration_ms: u64,
}

fn parse_range(s: &str) -> Result<(f64, f64), String> {
    let (lo, hi) = s.split_once(':').ok_or("expected lo:hi")?;
    let lo: f64 = lo.parse().map_err(|e| format!("{e}"))?;
    let hi: f64 = hi.parse().map_err(|e| format!("{e}"))?;
    if !(lo > 0.0 && hi >= lo) {
        return Err("need 0 < lo <= hi".into());
    }
    Ok((lo, hi))
}

/// The `lan-20` scenario, with policy maximums as placeholders.
fn base(args: &Args) -> ScenarioSpec {
    ScenarioSpec {
        nodes: 20,
        topology: TopologySpec::RandomTree { extra_edges: 0.2 },
        loss: 0.02,
        latency_ms: LatencySpec::LogNormal {
            median_ms: 3.0,
            sigma: 0.6,
        },
        router: RouterSpec {
            want_ttl_ms: 500,
            have_ttl_ms: 500,
        },
        storage: StorageSpec { relay_cap: 1 << 20 },
        policy: PolicySpec {
            want: IntervalPolicy::DensityScaled {
                min_ms: args.want_min,
                max_ms: args.want_min,
                ref_n: 20,
                alpha: 1.0,
            },
            have: IntervalPolicy::Fixed {
                min_ms: args.have_min,
                max_ms: args.have_min,
            },
        },
        workload: WorkloadSpec {
            writers: 4,
            appends_per_sec: 2.0,
            payload_bytes: 64,
            subscribers: None,
        },
        seeds: Some(args.seeds),
        duration_ms: Some(args.duration_ms),
    }
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let grid = Grid {
        want_min_ms: args.want_min,
        want_max_ms: sweep::geomspace_ms(args.want_max.0, args.want_max.1, args.steps),
        have_min_ms: args.have_min,
        have_max_ms: sweep::geomspace_ms(args.have_max.0, args.have_max.1, args.steps),
    };
    let base = base(&args);
    let defaults = Defaults::default();
    let total = grid.cells().count();
    let done = AtomicUsize::new(0);
    eprintln!("sweeping {total} cells x {} seeds", args.seeds);

    let cells = sweep::run_grid(&base, &defaults, &grid, |c| {
        let n = done.fetch_add(1, Ordering::Relaxed) + 1;
        eprintln!(
            "[{n:>3}/{total}] want {:>5} have {:>5}: {:6.1} msgs/op, coverage {:.1}%",
            c.want_max_ms,
            c.have_max_ms,
            c.messages_per_op,
            c.coverage_rate * 100.0
        );
    })?;

    std::fs::create_dir_all(&args.out)?;
    let csv = args.out.join("sweep.csv");
    let html = args.out.join("sweep.html");
    std::fs::write(&csv, sweep::to_csv(&cells)).with_context(|| csv.display().to_string())?;
    std::fs::write(
        &html,
        sweep::to_html(&grid, &cells, "Messages per op vs. want/have interval max"),
    )
    .with_context(|| html.display().to_string())?;
    println!("wrote {} and {}", csv.display(), html.display());
    Ok(())
}
