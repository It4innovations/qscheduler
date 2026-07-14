use clap::Parser;
use runner::config::MachineConfiguration;
use runner::error::RunnerError;
use runner::{BackendConfig, IqmBackendConfig, NotifyConfig};
use service::config::ServiceConfiguration;
use std::path::PathBuf;
use std::time::Duration;

/// qscheduler — task scheduler service
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(long)]
    pub log: Option<PathBuf>,

    #[clap(subcommand)]
    pub command: CliCommand,
}

#[derive(Parser)]
enum CliCommand {
    Serve(ServeOpts),
    /// Manage machines
    Machine(Box<MachineArgs>),
}

#[derive(Parser)]
struct ServeOpts {
    #[arg(long, default_value = "4300")]
    pub port: u16,
}

#[derive(Parser)]
struct MachineArgs {
    #[clap(subcommand)]
    command: MachineCommand,
}

#[derive(Parser)]
enum MachineCommand {
    /// Add a new machine
    Add(MachineConfigArgs),
    /// Update an existing machine's configuration
    Update(MachineConfigArgs),
}

#[derive(Parser)]
struct MachineConfigArgs {
    /// Machine name
    name: String,

    /// Maximum number of queued tasks
    #[arg(long, default_value = "4")]
    queue_size: usize,
    /// URL to POST task completion callbacks to
    #[arg(long)]
    notify_url: Option<String>,
    /// Token included in callback payloads
    #[arg(long)]
    notify_token: Option<String>,

    /// How often is session consumption stored into DB.
    #[arg(long, value_parser = parse_human_time, default_value = "5s")]
    session_check_interval: Duration,

    /// Maximum duration a session on this machine may request. Sessions requesting
    /// longer are rejected at submit time.
    #[arg(long, value_parser = parse_human_time, default_value = "2h")]
    max_session_time: Duration,

    #[clap(subcommand)]
    backend: MachineBackendArgs,
}

impl MachineConfigArgs {
    pub fn into_machine_configuration(self) -> MachineConfiguration {
        let notify = self.notify_url.map(|url| NotifyConfig {
            url,
            token: self.notify_token,
        });
        MachineConfiguration {
            name: self.name,
            queue_size: self.queue_size,
            session_check_interval_ms: self.session_check_interval.as_millis() as u32,
            max_session_time_ms: self.max_session_time.as_millis() as u64,
            notify,
            backend: self.backend.into_config(),
        }
    }
}

fn parse_human_time(s: &str) -> Result<Duration, humantime::DurationError> {
    humantime::parse_duration(s)
}

#[derive(Parser)]
struct IqmBackendArgs {
    /// IQM server base URL
    #[arg(long)]
    url: String,
    /// Authentication token
    #[arg(long)]
    token: String,
    /// Machine name on the IQM server
    #[arg(long)]
    machine_name: String,

    /// Job status polling interval in milliseconds
    #[arg(long, value_parser = parse_human_time)]
    check_interval: Duration,
}

// #[derive(Parser)]
// struct MachineSetupArgs {
//     /// Machine name
//     name: String,
//
//     #[clap(flatten)]
//     config: MachineConfigArgs,
//
//     #[clap(subcommand)]
//     backend: MachineBackendArgs,
// }

#[derive(Parser)]
enum MachineBackendArgs {
    Iqm(IqmBackendArgs),
    Test,
}

impl MachineBackendArgs {
    pub fn into_config(self) -> BackendConfig {
        match self {
            MachineBackendArgs::Test => BackendConfig::Test,
            MachineBackendArgs::Iqm(args) => BackendConfig::Iqm(IqmBackendConfig {
                url: args.url,
                token: args.token,
                machine_name: args.machine_name,
                check_interval: args.check_interval,
            }),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = Cli::parse();

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    if let Some(log_path) = &args.log {
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

    match args.command {
        CliCommand::Serve(opts) => {
            dotenvy::dotenv().ok();
            let config = ServiceConfiguration { port: opts.port };
            tracing::info!(config = ?config, "starting qscheduler");
            match service::run(env!("CARGO_PKG_VERSION"), config).await {
                Ok(_) => tracing::info!("qscheduler stopped successfully"),
                Err(e) => tracing::error!("qscheduler stopped with error: {}", e),
            }
        }
        CliCommand::Machine(machine_args) => {
            dotenvy::dotenv().ok();
            match machine_args.command {
                MachineCommand::Add(args) => cmd_machine_add(args).await,
                MachineCommand::Update(args) => cmd_machine_update(args).await,
            }
        }
    }
}

async fn connect_db() -> sqlx::postgres::PgPool {
    runner::db::create_pool().await.unwrap_or_else(|e| {
        eprintln!("Error: failed to connect to database: {e}");
        std::process::exit(1);
    })
}

async fn cmd_machine_add(args: MachineConfigArgs) {
    let pool = connect_db().await;
    let config = args.into_machine_configuration();
    match runner::db::insert_machine(&pool, &config).await {
        Ok(id) => {
            println!(
                "Machine '{name}' added successfully (id={id}).",
                name = config.name
            );
            println!("Restart the service for the change to take effect.");
        }
        Err(RunnerError::MachineAlreadyExists(_)) => {
            tracing::error!(name = %config.name, "machine already exists");
            eprintln!(
                "Error: a machine named '{name}' already exists.",
                name = config.name
            );
            std::process::exit(1);
        }
        Err(e) => {
            tracing::error!(name = %config.name, error = %e, "failed to add machine");
            eprintln!("Error: failed to add machine: {e}");
            std::process::exit(1);
        }
    }
}

async fn cmd_machine_update(args: MachineConfigArgs) {
    let pool = connect_db().await;
    match runner::db::get_machine_by_name(&pool, &args.name).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            tracing::error!(name = %args.name, "machine not found");
            eprintln!("Error: machine '{}' not found.", args.name);
            std::process::exit(1);
        }
        Err(e) => {
            tracing::error!(name = %args.name, error = %e, "failed to fetch machine");
            eprintln!("Error: failed to fetch machine '{}': {e}", args.name);
            std::process::exit(1);
        }
    };
    let new_backend = args.backend.into_config();
    match runner::db::update_machine(
        &pool,
        &args.name,
        args.queue_size as i32,
        &new_backend,
        args.notify_url.as_deref(),
        args.notify_token.as_deref(),
    )
    .await
    {
        Ok(true) => {
            println!("Machine '{}' updated successfully.", args.name);
            println!("Restart the service for the change to take effect.");
        }
        Ok(false) => {
            tracing::error!(name = %args.name, "machine not found");
            eprintln!("Error: machine '{}' not found.", args.name);
            std::process::exit(1);
        }
        Err(e) => {
            tracing::error!(name = %args.name, error = %e, "failed to update machine");
            eprintln!("Error: failed to update machine '{}': {e}", args.name);
            std::process::exit(1);
        }
    }
}
