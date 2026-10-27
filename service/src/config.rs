use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ServiceConfiguration {
    pub port: u16,
    /// Maximum size of a request body (e.g. a task payload), in MiB.
    pub max_payload_mb: usize,
    /// How long a finished task stays in memory; afterwards it is served from the DB.
    pub task_retention: std::time::Duration,
}
