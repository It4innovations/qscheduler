use crate::MachineId;
use crate::backend::BackendConfig;
use crate::callback::NotifierId;
use serde::{Deserialize, Serialize};

/// Default maximum session duration (2 hours) for machines whose stored
/// configuration predates the `max_session_time_ms` field.
fn default_max_session_time_ms() -> u64 {
    2 * 60 * 60 * 1000
}

/// Default task time limit (1 hour) for machines whose stored configuration predates the
/// `default_task_time_limit_ms` field.
fn default_task_time_limit_ms() -> u64 {
    60 * 60 * 1000
}

/// Default maximum clock skew (4 seconds) for machines whose stored configuration predates the
/// `max_clock_skew_ms` field.
fn default_max_clock_skew_ms() -> u64 {
    4 * 1000
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MachineConfiguration {
    pub name: String,
    pub queue_size: usize,
    /// How often session consumption is checkpointed and task time limits are checked.
    /// Accepts the pre-rename `session_check_interval_ms` key of stored configurations.
    #[serde(alias = "session_check_interval_ms")]
    pub limit_check_interval_ms: u32,
    /// Maximum duration a session on this machine may request. Sessions requesting
    /// longer than this are rejected at submit time.
    #[serde(default = "default_max_session_time_ms")]
    pub max_session_time_ms: u64,
    /// Time limit applied to tasks submitted without an explicit `time_limit_ms`.
    #[serde(default = "default_task_time_limit_ms")]
    pub default_task_time_limit_ms: u64,
    /// Assumed upper bound of how far the backend's clock may lag behind ours; used to keep the
    /// running-time estimate of tasks from ever exceeding the true time.
    #[serde(default = "default_max_clock_skew_ms")]
    pub max_clock_skew_ms: u64,
    /// Whether pulse-level tasks (`kind=pulse`) may be submitted to this machine.
    #[serde(default)]
    pub pulse_access: bool,
    pub backend: BackendConfig,
}

/// An HTTP callback target. Tasks and sessions select it by `name`.
#[derive(Debug, Clone)]
pub struct NotifierConfiguration {
    pub name: String,
    /// URL to POST task/session events to.
    pub url: String,
    /// Included in every payload, if set.
    pub token: Option<String>,
}

#[derive(Debug)]
pub struct RunnerConfiguration {
    pub machines: Vec<(MachineId, MachineConfiguration)>,
    pub notifiers: Vec<(NotifierId, NotifierConfiguration)>,
}

#[cfg(test)]
mod tests {
    use super::MachineConfiguration;
    use crate::backend::BackendConfig;
    use std::time::Duration;

    #[test]
    fn parses_pre_rename_interval_keys() {
        let config: MachineConfiguration = serde_json::from_str(
            r#"{"name": "m", "queue_size": 1, "session_check_interval_ms": 5000,
                "notify": null, "backend": {"type": "iqm", "url": "u", "token": "t",
                "machine_name": "n", "check_interval": {"secs": 1, "nanos": 0}}}"#,
        )
        .unwrap();
        assert_eq!(config.limit_check_interval_ms, 5000);
        let BackendConfig::Iqm(iqm) = config.backend else {
            panic!("expected IQM backend config");
        };
        assert_eq!(iqm.poll_interval, Duration::from_secs(1));
    }
}
