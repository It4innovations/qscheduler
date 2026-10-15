use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Maximum length of a suspension message, in characters (matches the DB column).
pub const MAX_SUSPENSION_MESSAGE_LEN: usize = 512;

/// What happens to a machine's existing work when it is suspended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum SuspendCancel {
    /// Only new tasks and sessions are rejected; existing work is computed as normal.
    #[default]
    None,
    /// Additionally fails all tasks not yet submitted to the backend and closes all sessions
    /// (an open session's submitted tasks fail as well).
    Waiting,
    /// Like `waiting`, and also fails every task already submitted to the backend, asking the
    /// backend to cancel it.
    All,
}

impl SuspendCancel {
    pub(crate) fn as_db_str(self) -> &'static str {
        match self {
            SuspendCancel::None => "none",
            SuspendCancel::Waiting => "waiting",
            SuspendCancel::All => "all",
        }
    }

    pub(crate) fn from_db_str(s: &str) -> crate::Result<Self> {
        match s {
            "none" => Ok(SuspendCancel::None),
            "waiting" => Ok(SuspendCancel::Waiting),
            "all" => Ok(SuspendCancel::All),
            _ => Err(crate::error::RunnerError::GenericError(format!(
                "Invalid suspension cancel value {s:?} in DB"
            ))),
        }
    }
}

/// A machine suspension — the active one, or a past one from the history.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct Suspension {
    pub message: String,
    pub cancel: SuspendCancel,
    pub suspended_at: DateTime<Utc>,
    /// Absent while the suspension is still active.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resumed_at: Option<DateTime<Utc>>,
}

/// The error recorded on a task failed because its machine was suspended.
pub(crate) fn suspension_task_error(message: &str) -> String {
    format!("Machine suspended: {message}")
}
