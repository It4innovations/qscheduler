use crate::callback::NotifyTaskState;
use crate::machine::MachineId;
use crate::session::SessionId;
use bytes::Bytes;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::num::NonZeroU64;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct TaskId(NonZeroU64);

impl Default for TaskId {
    fn default() -> Self {
        Self(NonZeroU64::MIN)
    }
}

impl Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TaskId {
    pub fn bump(&mut self) -> TaskId {
        let result = TaskId(self.0);
        self.0 = NonZeroU64::new(self.0.get() + 1).unwrap();
        result
    }

    #[inline]
    pub fn as_u64(&self) -> u64 {
        self.0.get()
    }
}

impl TryFrom<u64> for TaskId {
    type Error = ();
    fn try_from(v: u64) -> Result<Self, Self::Error> {
        NonZeroU64::new(v).map(TaskId).ok_or(())
    }
}

pub enum TaskState {
    Waiting,
    Running,
    Finished,
    Failed { error: String },
    Cancelled,
}

impl TaskState {
    pub(crate) fn to_notify(&self) -> Option<NotifyTaskState> {
        match self {
            TaskState::Waiting | TaskState::Running => None,
            TaskState::Finished => Some(NotifyTaskState::Finished),
            TaskState::Failed { .. } => Some(NotifyTaskState::Failed),
            TaskState::Cancelled => Some(NotifyTaskState::Cancelled),
        }
    }
}

pub struct TaskConfig {
    pub machine_id: MachineId,
    pub session_id: Option<SessionId>,
    pub payload: Bytes,
}

impl Debug for TaskConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskConfig")
            .field("machine_id", &self.machine_id)
            .field("session_id", &self.session_id)
            .field("payload", &format_args!("<{} bytes>", self.payload.len()))
            .finish()
    }
}

pub(crate) struct Task {
    id: TaskId,
    state: TaskState,
    backend_id: Option<String>,
    config: TaskConfig,
}

impl Task {
    pub fn new(task_id: TaskId, config: TaskConfig) -> Self {
        Task {
            id: task_id,
            state: TaskState::Waiting,
            backend_id: None,
            config,
        }
    }
    pub fn id(&self) -> TaskId {
        self.id
    }
    pub fn config(&self) -> &TaskConfig {
        &self.config
    }
    pub fn backend_id(&self) -> Option<&str> {
        self.backend_id.as_deref()
    }

    pub fn set_backend_id(&mut self, backend_id: String) {
        self.backend_id = Some(backend_id);
    }

    pub(crate) fn state(&self) -> &TaskState {
        &self.state
    }

    pub(crate) fn set_state(&mut self, state: TaskState) {
        self.state = state;
    }
}

#[derive(Default)]
pub(crate) struct TaskMap(HashMap<TaskId, Task>);

impl TaskMap {
    // #[inline]
    // pub fn get_task(&self, task_id: TaskId) -> &Task {
    //     self.0.get(&task_id).unwrap()
    // }

    #[inline]
    pub fn get_task_mut(&mut self, task_id: TaskId) -> &mut Task {
        self.0.get_mut(&task_id).unwrap()
    }

    pub fn find_task(&self, task_id: TaskId) -> Option<&Task> {
        self.0.get(&task_id)
    }

    pub fn find_task_mut(&mut self, task_id: TaskId) -> Option<&mut Task> {
        self.0.get_mut(&task_id)
    }

    pub fn insert(&mut self, task: Task) {
        let task_id = task.id();
        self.0.insert(task_id, task);
    }
}
