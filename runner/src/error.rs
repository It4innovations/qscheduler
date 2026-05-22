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

    #[error("Error: {0}")]
    GenericError(String),
}
