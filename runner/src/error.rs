use crate::machine::MachineId;
use crate::session::SessionId;
use crate::task::TaskId;
use std::time::Duration;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RunnerError {
    #[error("Invalid machine name {0:?}")]
    InvalidMachineName(String),
    #[error("Invalid machine id {0:?}")]
    InvalidMachineId(MachineId),
    #[error("Invalid session {0:?}")]
    InvalidSession(SessionId),
    #[error("Invalid task {0:?}")]
    InvalidTask(TaskId),
    #[error("Session {0:?} is not running")]
    NonRunningSession(SessionId),
    #[error("Task {0:?} already finished")]
    TaskAlreadyFinished(TaskId),
    #[error("Task {0:?} has not been submitted to a backend yet")]
    TaskNotSubmitted(TaskId),
    #[error("Unknown artifact {0:?}")]
    UnknownArtifact(String),
    #[error("Session {0:?} already closed")]
    SessionAlreadyClosed(SessionId),
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
    #[error("Project '{0}' is not active")]
    ProjectNotActive(String),
    #[error(
        "Session duration {requested:?} exceeds machine '{machine}' maximum session duration of {limit:?}"
    )]
    SessionDurationExceedsLimit {
        machine: String,
        limit: Duration,
        requested: Duration,
    },
    #[error("Machine '{machine}' is suspended: {message}")]
    MachineSuspended { machine: String, message: String },
    #[error("Machine '{0}' is already suspended")]
    MachineAlreadySuspended(String),
    #[error("Machine '{0}' is not suspended")]
    MachineNotSuspended(String),
    #[error("Invalid suspension message: {0}")]
    InvalidSuspensionMessage(String),
    #[error("Unknown notifier {0:?}")]
    UnknownNotifier(String),
    #[error("Notifier '{0}' already exists")]
    NotifierAlreadyExists(String),
    #[error("Invalid notifier name: {0}")]
    InvalidNotifierName(String),
    #[error("Database error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("Error: {0}")]
    GenericError(String),
}
