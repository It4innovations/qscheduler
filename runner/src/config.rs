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
/// Default time limit of tasks submitted without one (15 minutes).
pub const DEFAULT_TASK_TIME_LIMIT_MS: u64 = 15 * 60 * 1000;
/// Default maximum time limit a task may ask for (15 minutes).
pub const DEFAULT_MAX_TASK_TIME_LIMIT_MS: u64 = 15 * 60 * 1000;
/// Default maximum clock skew between this host and a backend (4 seconds).
pub const DEFAULT_MAX_CLOCK_SKEW_MS: u64 = 4 * 1000;
/// Default half-time of the decay of users' fair share usage (90 minutes).
pub const DEFAULT_DECAY_HALF_TIME_MS: u64 = 90 * 60 * 1000;
/// Default maximum number of non-final tasks a single user may have.
pub const DEFAULT_MAX_USER_ACTIVE_ITEMS: u32 = 2000;
/// Format of the machine configurations written to the DB. Rows without a version predate
/// versioning and are upgraded when loaded (see `MachineConfiguration::from_stored`).
pub const MACHINE_CONFIG_VERSION: u32 = 1;

/// For machines whose stored configuration predates the `max_session_time_ms` field.
fn default_max_session_time_ms() -> u64 {
    DEFAULT_MAX_SESSION_TIME_MS
}

/// For machines whose stored configuration predates the `default_task_time_limit_ms` field.
fn default_task_time_limit_ms() -> u64 {
    DEFAULT_TASK_TIME_LIMIT_MS
}

/// For machines whose stored configuration predates the `max_task_time_limit_ms` field.
fn default_max_task_time_limit_ms() -> u64 {
    DEFAULT_MAX_TASK_TIME_LIMIT_MS
}

/// For machines whose stored configuration predates the `max_clock_skew_ms` field.
fn default_max_clock_skew_ms() -> u64 {
    DEFAULT_MAX_CLOCK_SKEW_MS
}

/// For machines whose stored configuration predates the `decay_half_time_ms` field.
fn default_decay_half_time_ms() -> u64 {
    DEFAULT_DECAY_HALF_TIME_MS
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
    /// Maximum `time_limit_ms` a task may ask for; tasks asking for more are rejected at submit
    /// time. `default_task_time_limit_ms` must not exceed it.
    #[serde(default = "default_max_task_time_limit_ms")]
    pub max_task_time_limit_ms: u64,
    /// Assumed upper bound of how far the backend's clock may lag behind ours; used to keep the
    /// running-time estimate of tasks from ever exceeding the true time.
    #[serde(default = "default_max_clock_skew_ms")]
    pub max_clock_skew_ms: u64,
    /// Time after which a user's accumulated fair share usage drops to half.
    #[serde(default = "default_decay_half_time_ms")]
    pub decay_half_time_ms: u64,
    /// Whether pulse-level tasks (`kind=pulse`) may be submitted to this machine.
    #[serde(default)]
    pub pulse_access: bool,
    pub backend: BackendConfig,
}

/// A machine configuration as written to the DB.
#[derive(Serialize)]
struct StoredMachineConfiguration<'a> {
    version: u32,
    #[serde(flatten)]
    config: &'a MachineConfiguration,
}

/// The part of a stored machine configuration that decides how to read the rest.
#[derive(Deserialize)]
struct StoredVersion {
    version: Option<u32>,
}

impl MachineConfiguration {
    /// The JSON to store in the DB.
    pub(crate) fn to_stored(&self) -> crate::Result<String> {
        serde_json::to_string(&StoredMachineConfiguration {
            version: MACHINE_CONFIG_VERSION,
            config: self,
        })
        .map_err(|e| RunnerError::GenericError(e.to_string()))
    }

    /// Reads a configuration stored in the DB. The returned flag is set if it was unversioned and
    /// got upgraded, so it should be stored again.
    pub(crate) fn from_stored(stored: &str) -> crate::Result<(Self, bool)> {
        // Serde errors may quote the stored values, which include the backend token.
        let invalid =
            |_| RunnerError::GenericError("Cannot parse machine configuration from DB".into());
        let StoredVersion { version } = serde_json::from_str(stored).map_err(invalid)?;
        match version {
            None => {
                let mut config: Self = serde_json::from_str(stored).map_err(invalid)?;
                config.upgrade_legacy();
                Ok((config, true))
            }
            Some(MACHINE_CONFIG_VERSION) => {
                Ok((serde_json::from_str(stored).map_err(invalid)?, false))
            }
            Some(version) => Err(RunnerError::GenericError(format!(
                "Unsupported machine configuration version {version}"
            ))),
        }
    }

    /// Upgrades a configuration stored before versioning: the default task time limit is reset to
    /// the current default, and the maximum is raised if needed to stay above it.
    fn upgrade_legacy(&mut self) {
        tracing::warn!(
            machine = %self.name,
            old_ms = self.default_task_time_limit_ms,
            new_ms = DEFAULT_TASK_TIME_LIMIT_MS,
            "unversioned machine configuration: default task time limit reset"
        );
        self.default_task_time_limit_ms = DEFAULT_TASK_TIME_LIMIT_MS;
        self.max_task_time_limit_ms = self.max_task_time_limit_ms.max(DEFAULT_TASK_TIME_LIMIT_MS);
    }

    /// Rejects configurations a launcher cannot run with: an empty name, a zero queue size, a
    /// zero check/poll interval, a decay half-time below 1 ms, or a default task time limit above
    /// the maximum one.
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
        if self.decay_half_time_ms == 0 {
            return invalid("decay_half_time must be at least 1 ms");
        }
        if self.default_task_time_limit_ms > self.max_task_time_limit_ms {
            return invalid("default_task_time_limit must not exceed max_task_time_limit");
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
    /// How long a terminal task or a closed session stays in memory; afterwards it is served from
    /// the DB.
    pub task_retention: std::time::Duration,
    /// Maximum number of non-final tasks plus unclosed owned sessions a single user may have;
    /// further tasks and sessions are rejected.
    pub max_user_active_items: u32,
    /// Maximum total size of the payloads of a single user's non-final tasks, in bytes; submits
    /// that would exceed it are rejected.
    pub max_user_active_payload: u64,
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
            max_task_time_limit_ms: 1000,
            max_clock_skew_ms: 0,
            decay_half_time_ms: 1000,
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
        c.decay_half_time_ms = 0;
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

    /// A stored configuration without a version, with the given extra JSON fields.
    fn legacy_config(extra: &str) -> String {
        format!(
            r#"{{"name": "m", "queue_size": 1, "limit_check_interval_ms": 1000,
                 "backend": {{"type": "test"}}{extra}}}"#
        )
    }

    #[test]
    fn stored_config_round_trips_with_version() {
        let mut c = test_config();
        c.default_task_time_limit_ms = 500;
        let stored = c.to_stored().unwrap();
        let json: serde_json::Value = serde_json::from_str(&stored).unwrap();
        assert_eq!(json["version"], super::MACHINE_CONFIG_VERSION);
        assert_eq!(json["queue_size"], 1);

        let (parsed, upgraded) = MachineConfiguration::from_stored(&stored).unwrap();
        assert!(!upgraded);
        assert_eq!(parsed.default_task_time_limit_ms, 500);
        assert_eq!(parsed.max_task_time_limit_ms, 1000);
    }

    #[test]
    fn unversioned_config_gets_default_task_time_limits() {
        let stored = legacy_config(r#", "default_task_time_limit_ms": 7200000"#);
        let (parsed, upgraded) = MachineConfiguration::from_stored(&stored).unwrap();
        assert!(upgraded);
        assert_eq!(
            parsed.default_task_time_limit_ms,
            super::DEFAULT_TASK_TIME_LIMIT_MS
        );
        assert_eq!(
            parsed.max_task_time_limit_ms,
            super::DEFAULT_MAX_TASK_TIME_LIMIT_MS
        );
        assert_eq!(parsed.queue_size, 1);
        assert!(parsed.validate().is_ok());
    }

    #[test]
    fn unversioned_config_max_task_time_limit_stays_valid() {
        let stored = legacy_config(r#", "max_task_time_limit_ms": 7200000"#);
        let (parsed, _) = MachineConfiguration::from_stored(&stored).unwrap();
        assert_eq!(parsed.max_task_time_limit_ms, 7_200_000);

        let stored = legacy_config(r#", "max_task_time_limit_ms": 1000"#);
        let (parsed, _) = MachineConfiguration::from_stored(&stored).unwrap();
        assert_eq!(
            parsed.max_task_time_limit_ms,
            super::DEFAULT_TASK_TIME_LIMIT_MS
        );
        assert!(parsed.validate().is_ok());
    }

    #[test]
    fn unsupported_config_version_is_rejected() {
        let stored = legacy_config(r#", "version": 2"#);
        let error = MachineConfiguration::from_stored(&stored)
            .unwrap_err()
            .to_string();
        assert!(error.contains("version 2"), "{error}");
    }

    #[test]
    fn invalid_stored_config_error_hides_values() {
        let stored = r#"{"version": 1, "name": "secret-value", "queue_size": "secret-value"}"#;
        let error = MachineConfiguration::from_stored(stored)
            .unwrap_err()
            .to_string();
        assert!(!error.contains("secret-value"), "{error}");
    }

    #[test]
    fn validate_default_task_time_limit_within_max() {
        let mut c = test_config();
        c.default_task_time_limit_ms = c.max_task_time_limit_ms;
        assert!(c.validate().is_ok());
        c.default_task_time_limit_ms = c.max_task_time_limit_ms + 1;
        let error = c.validate().unwrap_err().to_string();
        assert!(error.contains("max_task_time_limit"), "{error}");
    }

    #[test]
    fn stored_config_without_max_task_time_limit_gets_default() {
        let parsed: MachineConfiguration = serde_json::from_str(
            r#"{"name": "m", "queue_size": 1, "limit_check_interval_ms": 1000,
                "backend": {"type": "test"}}"#,
        )
        .unwrap();
        assert_eq!(
            parsed.max_task_time_limit_ms,
            super::DEFAULT_MAX_TASK_TIME_LIMIT_MS
        );
    }

    #[test]
    fn default_decay_half_time_is_90_minutes() {
        assert_eq!(super::DEFAULT_DECAY_HALF_TIME_MS, 5_400_000);
    }

    #[test]
    fn validate_decay_half_time_bounds() {
        let mut c = test_config();
        c.decay_half_time_ms = 0;
        let error = c.validate().unwrap_err().to_string();
        assert!(error.contains("at least 1 ms"), "{error}");
        c.decay_half_time_ms = 1;
        assert!(c.validate().is_ok());
        c.decay_half_time_ms = u64::MAX;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn decay_half_time_survives_serialization() {
        let mut c = test_config();
        c.decay_half_time_ms = 1234;
        let stored = serde_json::to_string(&c).unwrap();
        let parsed: MachineConfiguration = serde_json::from_str(&stored).unwrap();
        assert_eq!(parsed.decay_half_time_ms, 1234);
    }

    #[test]
    fn stored_config_without_decay_half_time_gets_default() {
        let parsed: MachineConfiguration = serde_json::from_str(
            r#"{"name": "m", "queue_size": 1, "limit_check_interval_ms": 1000,
                "backend": {"type": "test"}}"#,
        )
        .unwrap();
        assert_eq!(parsed.decay_half_time_ms, super::DEFAULT_DECAY_HALF_TIME_MS);
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
        assert_eq!(config.decay_half_time_ms, super::DEFAULT_DECAY_HALF_TIME_MS);
        let BackendConfig::Iqm(iqm) = config.backend else {
            panic!("expected IQM backend config");
        };
        assert_eq!(iqm.poll_interval, Duration::from_secs(1));
    }
}
