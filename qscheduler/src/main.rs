mod config;

use crate::config::load_config;
use clap::Parser;
use runner::config::RunnerConfiguration;
use std::path::PathBuf;

/// qscheduler — task scheduler service
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// Path to the configuration file
    config: PathBuf,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = Cli::parse();
    let config = load_config(&args.config).expect("failed to load config");

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    if let Some(log_path) = &config.log {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .expect("failed to open log file");
        tracing_subscriber::fmt()
            .with_writer(std::sync::Mutex::new(file))
            .with_env_filter(env_filter)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_writer(std::io::stdout)
            .with_env_filter(env_filter)
            .init();
    }

    tracing::info!(config = ?args.config, "starting qscheduler");
    service::run(
        env!("CARGO_PKG_VERSION"),
        config.service,
        RunnerConfiguration {
            machines: config.machines,
        },
    )
    .await;
}
