use crate::core::Core;
use crate::session::{SessionConfig, SessionId};
use crate::task::{TaskConfig, TaskId};
use std::sync::{Arc, Mutex};

pub fn submit_task(core: Arc<Mutex<Core>>, config: TaskConfig) -> crate::Result<TaskId> {
    core.lock().unwrap().add_task(config)
}

pub fn create_session(core: Arc<Mutex<Core>>, config: SessionConfig) -> crate::Result<SessionId> {
    core.lock().unwrap().add_session(config)
}
