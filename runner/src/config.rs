use crate::MachineId;
use crate::backend::BackendConfig;
use crate::callback::NotifierId;
use crate::error::RunnerError;
use serde::{Deserialize, Serialize};

/// Default maximum number of tasks a machine submits to its backend at once.
pub const DEFAULT_QUEUE_SIZE: usize = 4;
/// Default period of session checkpoints and task time limit checks (2 seconds).
pub const DEFAULT_LIMIT_CHECK_INTERVAL_MS: u32 = 2 * 1000;
/// Default maximum session duration (2 hours).
pub const DEFAULT_MAX_SESSION_TIME_MS: u64 = 2 * 60 * 60 * 1000;
/// Default time limit of tasks submitted without one (1 hour).
pub const DEFAULT_TASK_TIME_LIMIT_MS: u64 = 60 * 60 * 1000;
/// Default maximum clock skew between this host and a backend (4 seconds).
pub const DEFAULT_MAX_CLOCK_SKEW_MS: u64 = 4 * 1000;

/// For machines whose stored configuration predates the `max_session_time_ms` field.
fn default_max_session_time_ms() -> u64 {
    DEFAULT_MAX_SESSION_TIME_MS
}

/// For machines whose stored configuration predates the `default_task_time_limit_ms` field.
fn default_task_time_limit_ms() -> u64 {
    DEFAULT_TASK_TIME_LIMIT_MS
}

/// For machines whose stored configuration predates the `max_clock_skew_ms` field.
fn default_max_clock_skew_ms() -> u64 {
    DEFAULT_MAX_CLOCK_SKEW_MS
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

impl MachineConfiguration {
    /// Rejects configurations a launcher cannot run with: an empty name, a zero queue size, or
    /// a zero check/poll interval.
    pub fn validate(&self) -> crate::Result<()> {
        let invalid = |msg: &str| Err(RunnerError::InvalidMachineConfig(msg.to_string()));
        if self.name.trim().is_empty() {
            return invalid("name must not be empty");
        }
        if self.queue_size == 0 {
            return invalid("queue_size must be at least 1");
        }
        if self.limit_check_interval_ms == 0 {
            return invalid("limit_check_interval must be positive");
        }
        if let BackendConfig::Iqm(iqm) = &self.backend
            && iqm.poll_interval.is_zero()
        {
            return invalid("poll_interval must be positive");
        }
        Ok(())
    }
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
    use crate::IqmBackendConfig;
    use crate::backend::BackendConfig;
    use std::time::Duration;

    fn test_config() -> MachineConfiguration {
        MachineConfiguration {
            name: "m".to_string(),
            queue_size: 1,
            limit_check_interval_ms: 1000,
            max_session_time_ms: 1000,
            default_task_time_limit_ms: 1000,
            max_clock_skew_ms: 0,
            pulse_access: false,
            backend: BackendConfig::Test,
        }
    }

    #[test]
    fn validate_accepts_valid_config() {
        assert!(test_config().validate().is_ok());
    }

    #[test]
    fn validate_rejects_invalid_config() {
        let mut c = test_config();
        c.name = " ".to_string();
        assert!(c.validate().is_err());
        let mut c = test_config();
        c.queue_size = 0;
        assert!(c.validate().is_err());
        let mut c = test_config();
        c.limit_check_interval_ms = 0;
        assert!(c.validate().is_err());
        let mut c = test_config();
        c.backend = BackendConfig::Iqm(IqmBackendConfig {
            url: "u".to_string(),
            token: "t".to_string(),
            machine_name: "n".to_string(),
            poll_interval: Duration::ZERO,
        });
        assert!(c.validate().is_err());
    }

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
