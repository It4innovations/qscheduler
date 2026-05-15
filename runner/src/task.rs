use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use crate::machine::MachineId;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct TaskId(NonZeroU32);

impl Default for TaskId {
    fn default() -> Self {
        Self(NonZeroU32::MIN)
    }
}

impl TaskId {
    pub fn bump(&mut self) -> TaskId {
        let result = TaskId(self.0);
        self.0 = NonZeroU32::new(self.0.get() + 1).unwrap();
        result
    }

    #[inline]
    pub fn as_u32(&self) -> u32 {
        self.0.get()
    }
}

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskState {
    Waiting,
    Running,
    Finished,
    Failed,
}

pub struct TaskConfig {
    pub machine_id: MachineId,
    pub repeats: u32,
    pub max_waiting_time: Duration,
    pub max_compute_time: Duration,
    pub callback_url: Option<String>,
    pub callback_token: Option<String>,
    pub payload: Arc<[u8]>
}

pub(crate) struct Task {
    task_id: TaskId,
    state: TaskState,
    config: TaskConfig,
}

impl Task {
    pub fn new(task_id: TaskId, config: TaskConfig) -> Self {
        Task {
            task_id,
            state: TaskState::Waiting,
            config,
        }
    }
    pub fn id(&self) -> TaskId {
        self.task_id
    }
    pub fn config(&self) -> &TaskConfig {
        &self.config
    }

    pub(crate) fn state(&self) -> &TaskState {
        &self.state
    }

    pub(crate) fn set_state(&mut self, state: TaskState) {
        self.state = state;
    }
}

impl TryFrom<u32> for TaskId {
    type Error = ();
    fn try_from(v: u32) -> Result<Self, Self::Error> {
        NonZeroU32::new(v).map(TaskId).ok_or(())
    }
}

