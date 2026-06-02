mod backend;
mod backend_iqm;
mod backend_test;
mod callback;
pub mod config;
pub mod core;
pub mod error;
mod launcher;
pub mod machine;
pub mod reactor;
mod session;
pub mod task;

use crate::error::RunnerError;

pub use machine::MachineId;
pub use session::{SessionConfig, SessionId, SessionState};
pub use task::TaskId;

type Result<T> = std::result::Result<T, RunnerError>;
