mod backend;
mod backend_iqm;
mod backend_test;
mod callback;
pub mod config;
pub mod core;
pub mod db;
pub mod error;
mod id;
mod launcher;
pub mod machine;
mod project;
pub mod reactor;
mod session;
pub mod suspension;
pub mod task;

use crate::error::RunnerError;

pub use backend::{BackendConfig, BackendStream, BackendVersion};
pub use backend_iqm::{IqmBackendConfig, IqmVersion};
pub use callback::{MAX_NOTIFIER_NAME_LEN, NotifierId, NotifierInfo};
pub use machine::{MachineConfig, MachineId, MachineInfo};
pub use project::Project;
pub use session::{SessionConfig, SessionId, SessionInfo, SessionState};
pub use suspension::{SuspendCancel, Suspension};
pub use task::TaskId;

pub type Result<T> = std::result::Result<T, RunnerError>;
