mod config;

use std::path::PathBuf;
use clap::Parser;
use runner::config::RunnerConfiguration;
use crate::config::load_config;

/// qscheduler — task scheduler service
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// Path to the configuration file
    config: PathBuf,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stdout)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Cli::parse();
    tracing::info!(config = ?args.config, "starting qscheduler");
    let config = load_config(&args.config).expect("failed to load config");
    service::run(env!("CARGO_PKG_VERSION"), config.service, RunnerConfiguration { machine: config.machine }).await;
}
