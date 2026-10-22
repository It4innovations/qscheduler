use clap::Parser;
use runner::config::{MachineConfiguration, NotifierConfiguration};
use runner::error::RunnerError;
use runner::{BackendConfig, IqmBackendConfig};
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
    /// Manage notifiers (HTTP callback targets selected by tasks and sessions)
    Notifier(NotifierArgs),
    /// Print the OpenAPI spec of the REST API (no database needed)
    Openapi(OpenapiOpts),
}

#[derive(Parser)]
struct OpenapiOpts {
    /// Write the spec to this file instead of stdout
    #[arg(long)]
    pub output: Option<PathBuf>,
}

#[derive(Parser)]
struct NotifierArgs {
    #[clap(subcommand)]
    command: NotifierCommand,
}

#[derive(Parser)]
enum NotifierCommand {
    /// Add a new notifier
    Add(NotifierAddArgs),
    /// Remove a notifier; tasks and sessions that use it stop sending notifications
    Remove(NotifierRemoveArgs),
    /// List registered notifiers (tokens are not shown)
    List,
}

#[derive(Parser)]
struct NotifierAddArgs {
    /// Notifier name (at most 512 characters); tasks and sessions select the notifier by it
    name: String,
    /// URL to POST task completion and session open/close events to
    #[arg(long)]
    url: String,
    /// Token included in event payloads
    #[arg(long)]
    token: Option<String>,
}

#[derive(Parser)]
struct NotifierRemoveArgs {
    /// Notifier name
    name: String,
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
    /// Remove a machine
    Remove(MachineRemoveArgs),
}

#[derive(Parser)]
struct MachineRemoveArgs {
    /// Machine name
    name: String,

    /// Remove the machine even if it has tasks or sessions; they are deleted together with
    /// their consumption records
    #[arg(long)]
    force: bool,
}

#[derive(Parser)]
struct MachineConfigArgs {
    /// Machine name
    name: String,

    /// Maximum number of queued tasks
    #[arg(long, default_value = "4")]
    queue_size: usize,
    /// How often session consumption is stored into DB and task time limits are checked.
    #[arg(long, value_parser = parse_human_time, default_value = "2s")]
    limit_check_interval: Duration,

    /// Maximum duration a session on this machine may request. Sessions requesting
    /// longer are rejected at submit time.
    #[arg(long, value_parser = parse_human_time, default_value = "2h")]
    max_session_time: Duration,

    /// Execution time limit for tasks submitted without an explicit `time_limit_ms`.
    /// Counted from when the backend starts running the task.
    #[arg(long, value_parser = parse_human_time, default_value = "1h")]
    default_task_time_limit: Duration,

    /// Maximum assumed skew between this host's clock and the backend's. Running tasks' time
    /// is estimated so that it never exceeds the true time as long as the backend's clock
    /// lags ours by at most this much.
    #[arg(long, value_parser = parse_human_time, default_value = "4s")]
    max_clock_skew: Duration,

    #[clap(subcommand)]
    backend: MachineBackendArgs,
}

impl MachineConfigArgs {
    pub fn into_machine_configuration(self) -> MachineConfiguration {
        MachineConfiguration {
            name: self.name,
            queue_size: self.queue_size,
            limit_check_interval_ms: self.limit_check_interval.as_millis() as u32,
            max_session_time_ms: self.max_session_time.as_millis() as u64,
            default_task_time_limit_ms: self.default_task_time_limit.as_millis() as u64,
            max_clock_skew_ms: self.max_clock_skew.as_millis() as u64,
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

    /// Job status polling interval, e.g. `1s` or `500ms`
    #[arg(long, value_parser = parse_human_time)]
    poll_interval: Duration,
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
                poll_interval: args.poll_interval,
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
    } else if matches!(args.command, CliCommand::Serve(_)) {
        tracing_subscriber::fmt()
            .with_writer(std::io::stdout)
            .with_env_filter(env_filter)
            .init();
    } else {
        // Management commands keep stdout for their own output, so it stays parseable.
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
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
                MachineCommand::Remove(args) => cmd_machine_remove(args).await,
            }
        }
        CliCommand::Notifier(notifier_args) => {
            dotenvy::dotenv().ok();
            match notifier_args.command {
                NotifierCommand::Add(args) => cmd_notifier_add(args).await,
                NotifierCommand::Remove(args) => cmd_notifier_remove(args).await,
                NotifierCommand::List => cmd_notifier_list().await,
            }
        }
        CliCommand::Openapi(opts) => cmd_openapi(opts),
    }
}

fn cmd_openapi(opts: OpenapiOpts) {
    let spec = service::openapi()
        .to_pretty_json()
        .unwrap_or_else(|e| die(format!("failed to serialize OpenAPI spec: {e}")));
    match opts.output {
        Some(path) => std::fs::write(&path, spec + "\n")
            .unwrap_or_else(|e| die(format!("failed to write {}: {e}", path.display()))),
        None => println!("{spec}"),
    }
}

const RESTART_HINT: &str = "Restart the service for the change to take effect.";

/// Prints `Error: {msg}` to stderr and exits with status 1.
fn die(msg: impl std::fmt::Display) -> ! {
    eprintln!("Error: {msg}");
    std::process::exit(1)
}

async fn connect_db() -> sqlx::postgres::PgPool {
    runner::db::create_pool().await.unwrap_or_else(|e| {
        die(format!("failed to connect to database: {e}"));
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
            println!("{RESTART_HINT}");
        }
        Err(RunnerError::MachineAlreadyExists(_)) => {
            tracing::error!(name = %config.name, "machine already exists");
            die(format!(
                "a machine named '{name}' already exists.",
                name = config.name
            ));
        }
        Err(e) => {
            tracing::error!(name = %config.name, error = %e, "failed to add machine");
            die(format!("failed to add machine: {e}"));
        }
    }
}

async fn cmd_machine_update(args: MachineConfigArgs) {
    let pool = connect_db().await;
    let config = args.into_machine_configuration();
    match runner::db::update_machine(&pool, &config).await {
        Ok(true) => {
            println!("Machine '{}' updated successfully.", config.name);
            println!("{RESTART_HINT}");
        }
        Ok(false) => {
            tracing::error!(name = %config.name, "machine not found");
            die(format!("machine '{}' not found.", config.name));
        }
        Err(e) => {
            tracing::error!(name = %config.name, error = %e, "failed to update machine");
            die(format!("failed to update machine '{}': {e}", config.name));
        }
    }
}

async fn cmd_machine_remove(args: MachineRemoveArgs) {
    use runner::db::RemoveMachineOutcome;
    let pool = connect_db().await;
    match runner::db::remove_machine(&pool, &args.name, args.force).await {
        Ok(RemoveMachineOutcome::Removed { tasks, sessions }) => {
            if tasks > 0 || sessions > 0 {
                println!(
                    "Machine '{}' removed together with {tasks} task(s) and {sessions} session(s).",
                    args.name
                );
                println!(
                    "Time consumed by these tasks and sessions was not returned to their projects."
                );
            } else {
                println!("Machine '{}' removed successfully.", args.name);
            }
            println!("{RESTART_HINT}");
        }
        Ok(RemoveMachineOutcome::HasDependents { tasks, sessions }) => {
            eprintln!(
                "Error: machine '{}' has {tasks} task(s) and {sessions} session(s); not removed.",
                args.name
            );
            eprintln!(
                "Use --force to remove it anyway. All these tasks and sessions \
                 (and their consumption records) will be removed as well."
            );
            eprintln!("Time they consumed will not be returned to their projects.");
            std::process::exit(1);
        }
        Ok(RemoveMachineOutcome::NotFound) => {
            tracing::error!(name = %args.name, "machine not found");
            die(format!("machine '{}' not found.", args.name));
        }
        Err(e) => {
            tracing::error!(name = %args.name, error = %e, "failed to remove machine");
            die(format!("failed to remove machine '{}': {e}", args.name));
        }
    }
}

async fn cmd_notifier_add(args: NotifierAddArgs) {
    let pool = connect_db().await;
    let config = NotifierConfiguration {
        name: args.name,
        url: args.url,
        token: args.token,
    };
    match runner::db::insert_notifier(&pool, &config).await {
        Ok(id) => {
            println!(
                "Notifier '{name}' added successfully (id={id}).",
                name = config.name
            );
            println!("{RESTART_HINT}");
        }
        Err(RunnerError::NotifierAlreadyExists(_)) => {
            tracing::error!(name = %config.name, "notifier already exists");
            die(format!(
                "a notifier named '{name}' already exists.",
                name = config.name
            ));
        }
        Err(e @ RunnerError::InvalidNotifierName(_)) => {
            die(e);
        }
        Err(e) => {
            tracing::error!(name = %config.name, error = %e, "failed to add notifier");
            die(format!("failed to add notifier: {e}"));
        }
    }
}

async fn cmd_notifier_list() {
    let pool = connect_db().await;
    let notifiers = runner::db::load_notifiers(&pool).await.unwrap_or_else(|e| {
        tracing::error!(error = %e, "failed to list notifiers");
        die(format!("failed to list notifiers: {e}"));
    });
    if notifiers.is_empty() {
        println!("No notifiers registered.");
        return;
    }
    let width = notifiers
        .iter()
        .map(|(_, n)| n.name.chars().count())
        .max()
        .unwrap_or(0)
        .max("NAME".len());
    println!("{:<width$}  {:<5}  URL", "NAME", "TOKEN");
    for (_, n) in notifiers {
        let token = if n.token.is_some() { "yes" } else { "no" };
        println!("{:<width$}  {token:<5}  {}", n.name, n.url);
    }
}

async fn cmd_notifier_remove(args: NotifierRemoveArgs) {
    let pool = connect_db().await;
    match runner::db::remove_notifier(&pool, &args.name).await {
        Ok(true) => {
            println!("Notifier '{}' removed successfully.", args.name);
            println!("Tasks and sessions that used it no longer send notifications.");
            println!("{RESTART_HINT}");
        }
        Ok(false) => {
            tracing::error!(name = %args.name, "notifier not found");
            die(format!("notifier '{}' not found.", args.name));
        }
        Err(e) => {
            tracing::error!(name = %args.name, error = %e, "failed to remove notifier");
            die(format!("failed to remove notifier '{}': {e}", args.name));
        }
    }
}
