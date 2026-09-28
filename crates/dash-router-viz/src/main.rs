//! Serve one dash-router scenario through the simviz viewer.

use std::{net::SocketAddr, path::PathBuf};

use anyhow::Context;
use clap::Parser;
use dash_router_sim::Config;
use simviz::{Simulation, Viewer};

/// simviz server for dash-router simulations
#[derive(Parser)]
struct Cli {
    /// Scenario config (YAML)
    #[arg(default_value = "crates/dash-router-sim/scenarios/example.yaml")]
    config: PathBuf,

    /// Scenario to show; default: the first in the file
    #[arg(long)]
    scenario: Option<String>,

    /// Seed for the run
    #[arg(long, default_value_t = 0)]
    seed: u64,

    /// Port to listen on
    #[arg(short, long, default_value_t = 3001)]
    port: u16,

    /// Instead of serving, record this many steps to a JSON file and exit
    #[arg(long, value_name = "STEPS")]
    record: Option<usize>,

    /// Where to write the recording
    #[arg(long, default_value = "recording.json")]
    out: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    let config = Config::from_yaml(&std::fs::read_to_string(&cli.config)?)
        .with_context(|| format!("reading {}", cli.config.display()))?;
    let (scenario, presenter) =
        dash_router_viz::scenario(&config, cli.scenario.as_deref(), cli.seed)?;
    let mut viewer = Viewer::new(Simulation::new(scenario), presenter);

    if let Some(steps) = cli.record {
        let recording = viewer.record(steps)?;
        std::fs::write(&cli.out, serde_json::to_vec(&recording)?)?;
        println!(
            "wrote {} frames to {}",
            recording.frames.len(),
            cli.out.display()
        );
        return Ok(());
    }

    simviz::serve(viewer, SocketAddr::from(([127, 0, 0, 1], cli.port))).await
}
