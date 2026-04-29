use crate::core::Core;
use crate::task::{Task, TaskConfig, TaskId};

pub fn submit_task(core: &mut Core, config: TaskConfig) -> TaskId {
    let task_id = core.add_task(config);
    task_id
}