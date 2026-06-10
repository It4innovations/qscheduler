use crate::machine::MachineId;
use crate::session::SessionId;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RunnerError {
    #[error("Invalid machine {0:?}")]
    InvalidMachine(MachineId),
    #[error("Invalid session {0:?}")]
    InvalidSession(SessionId),
    #[error("Session {0:?} is not running")]
    NonRunningSession(SessionId),
    #[error("Task fails with: {0}")]
    TaskFail(String),
    #[error("Machine '{0}' already exists")]
    MachineAlreadyExists(String),
    #[error("Project '{0}' already exists")]
    ProjectAlreadyExists(String),
    #[error("Project '{0}' not found")]
    ProjectNotFound(String),
    #[error("Project '{0}' has exceeded its time limit")]
    ProjectLimitExceeded(String),
    #[error("Database error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("Error: {0}")]
    GenericError(String),
}
