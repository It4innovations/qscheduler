mod backend;
mod backend_iqm;
mod backend_test;
mod callback;
pub mod config;
pub mod core;
pub mod db;
pub mod error;
mod launcher;
pub mod machine;
pub mod reactor;
mod session;
mod project;
pub mod task;


use crate::error::RunnerError;

pub use backend::BackendConfig;
pub use backend_iqm::IqmBackendConfig;
pub use machine::MachineId;
pub use session::{SessionConfig, SessionId, SessionState};
pub use task::TaskId;
pub use project::Project;

pub type Result<T> = std::result::Result<T, RunnerError>;
