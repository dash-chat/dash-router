//! Run scenario files: `sim scenarios.yaml [--scenario name] [--out dir] [--baseline dir]`.

use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use dash_router_sim::{Config, report::ScenarioReport};

#[derive(Parser)]
#[command(about = "Seeded discrete-event simulation of Dash Router networks")]
struct Args {
    /// Scenario config (YAML).
    config: PathBuf,
    /// Run only this scenario.
    #[arg(long)]
    scenario: Option<String>,
    /// Directory to write per-scenario YAML reports into.
    #[arg(long, default_value = "sim-out")]
    out: PathBuf,
    /// Directory of prior reports to diff against.
    #[arg(long)]
    baseline: Option<PathBuf>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = Config::from_yaml(&std::fs::read_to_string(&args.config)?)
        .with_context(|| format!("reading {}", args.config.display()))?;
    std::fs::create_dir_all(&args.out)?;

    for (name, spec) in &config.scenarios {
        if let Some(only) = &args.scenario
            && only != name
        {
            continue;
        }
        let seeds = spec.seeds(&config.defaults);
        let mut runs = Vec::with_capacity(seeds as usize);
        for seed in 0..seeds {
            let mut sim = spec
                .build(seed, &config.defaults)
                .with_context(|| format!("building {name} seed {seed}"))?;
            let record = sim
                .run(seed)
                .with_context(|| format!("running {name} seed {seed}"))?;
            runs.push(record);

            let dot_path = args.out.join(format!("{name}.{seed:04}.topology.dot"));
            std::fs::write(
                &dot_path,
                spec.topology(seed).to_dot(&format!("{name} seed {seed}")),
            )?;
        }
        let report = ScenarioReport::new(name.clone(), spec.clone(), runs);
        println!("{}", report.headline());
        for (check, ok) in [
            ("loss_exercised", report.sometimes.loss_exercised),
            ("backfill_exercised", report.sometimes.backfill_exercised),
            (
                "backpressure_exercised",
                report.sometimes.backpressure_exercised,
            ),
        ] {
            if !ok {
                println!("  sometimes NOT hit: {check}");
            }
        }

        let out_path = args.out.join(format!("{name}.yaml"));
        std::fs::write(&out_path, report.to_yaml()?)?;

        if let Some(dir) = &args.baseline {
            let base_path = dir.join(format!("{name}.yaml"));
            if base_path.exists() {
                let baseline = ScenarioReport::from_yaml_file(&base_path)?;
                println!("{}", report.diff(&baseline));
            } else {
                println!("  no baseline for {name} in {}", dir.display());
            }
        }
    }
    Ok(())
}
