use crate::MachineId;
use crate::backend::BackendConfig;
use crate::callback::NotifyConfig;
use serde::{Deserialize, Serialize};

/// Default maximum session duration (2 hours) for machines whose stored
/// configuration predates the `max_session_time_ms` field.
fn default_max_session_time_ms() -> u64 {
    2 * 60 * 60 * 1000
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MachineConfiguration {
    pub name: String,
    pub queue_size: usize,
    pub session_check_interval_ms: u32,
    /// Maximum duration a session on this machine may request. Sessions requesting
    /// longer than this are rejected at submit time.
    #[serde(default = "default_max_session_time_ms")]
    pub max_session_time_ms: u64,
    pub notify: Option<NotifyConfig>,
    pub backend: BackendConfig,
}

#[derive(Debug)]
pub struct RunnerConfiguration {
    pub machines: Vec<(MachineId, MachineConfiguration)>,
}
