use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ServiceConfiguration {
    pub port: u16,
    /// Maximum size of a request body (e.g. a task payload), in bytes.
    pub max_payload: usize,
    /// How long a finished task or a closed session stays in memory; afterwards it is served from
    /// the DB.
    pub task_retention: std::time::Duration,
    /// Maximum number of non-final tasks plus unclosed owned sessions a single user may have.
    pub max_user_active_items: u32,
    /// Maximum total size of the payloads of a single user's non-final tasks, in bytes.
    pub max_user_active_payload: u64,
}
