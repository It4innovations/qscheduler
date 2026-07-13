use crate::machine::MachineId;
use crate::project::ProjectId;
use crate::session::SessionId;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::num::NonZeroU64;
use std::time::Duration;

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

    #[inline]
    pub fn as_i64(&self) -> i64 {
        self.0.get() as i64
    }
}

impl TryFrom<u64> for TaskId {
    type Error = ();
    fn try_from(v: u64) -> Result<Self, Self::Error> {
        NonZeroU64::new(v).map(TaskId).ok_or(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CompletedState {
    pub consumed: Duration,
    pub timestamp: DateTime<Utc>,
}

impl CompletedState {
    pub fn new_zero_now() -> Self {
        CompletedState {
            consumed: Duration::ZERO,
            timestamp: Utc::now(),
        }
    }

    /// Milliseconds of consumed time, or `None` if nothing was consumed (e.g. a task
    /// cancelled before it ever ran).
    pub fn consumed_ms(&self) -> Option<i64> {
        duration_ms(self.consumed)
    }
}

/// Milliseconds in `d`, or `None` if `d` is zero. Shared zero-omission rule for
/// [`CompletedState::consumed_ms`] and the DB's `exec_time_ms` columns.
pub fn duration_ms(d: Duration) -> Option<i64> {
    if d.is_zero() {
        None
    } else {
        Some(d.as_millis() as i64)
    }
}

#[derive(Debug, Clone)]
pub enum TaskState {
    Waiting,
    Running,
    Finished {
        completed: CompletedState,
    },
    Failed {
        completed: CompletedState,
        error: String,
    },
    Cancelled {
        completed: CompletedState,
    },
}

impl TaskState {
    pub fn is_final(&self) -> bool {
        match self {
            TaskState::Waiting | TaskState::Running => false,
            TaskState::Finished { .. } | TaskState::Failed { .. } | TaskState::Cancelled { .. } => {
                true
            }
        }
    }

    pub fn simple_error(error: String) -> Self {
        TaskState::Failed {
            error,
            completed: CompletedState::new_zero_now(),
        }
    }

    pub fn simple_cancelled() -> Self {
        TaskState::Cancelled {
            completed: CompletedState::new_zero_now(),
        }
    }

    pub fn completed(&self) -> Option<&CompletedState> {
        match self {
            TaskState::Waiting | TaskState::Running => None,
            TaskState::Finished { completed, .. }
            | TaskState::Failed { completed, .. }
            | TaskState::Cancelled { completed, .. } => Some(completed),
        }
    }
}

/// The current state of a task, as a plain string (no embedded data).
#[derive(Debug, Clone, Copy, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum TaskStateKind {
    /// Task is queued and waiting to be assigned to a machine.
    Waiting,
    /// Task is currently executing on a machine.
    Running,
    /// Task completed successfully.
    Finished,
    /// Task failed. The `error` field contains the failure reason.
    Failed,
    /// Task was cancelled before it could finish.
    Cancelled,
}

impl From<&TaskState> for TaskStateKind {
    fn from(s: &TaskState) -> Self {
        match s {
            TaskState::Waiting => Self::Waiting,
            TaskState::Running => Self::Running,
            TaskState::Finished { .. } => Self::Finished,
            TaskState::Failed { .. } => Self::Failed,
            TaskState::Cancelled { .. } => Self::Cancelled,
        }
    }
}

pub enum TaskParent {
    Session(SessionId),
    Project(ProjectId),
}

impl TaskParent {
    pub fn new(session_id: Option<SessionId>, project_id: Option<ProjectId>) -> Self {
        assert_ne!(session_id.is_some(), project_id.is_some());
        if let Some(session_id) = session_id {
            TaskParent::Session(session_id)
        } else if let Some(project_id) = project_id {
            TaskParent::Project(project_id)
        } else {
            unreachable!()
        }
    }

    #[inline]
    pub fn session_id(&self) -> Option<SessionId> {
        match self {
            TaskParent::Session(s) => Some(*s),
            TaskParent::Project(_) => None,
        }
    }

    #[inline]
    pub fn project_id(&self) -> Option<ProjectId> {
        match self {
            TaskParent::Project(p) => Some(*p),
            TaskParent::Session(_) => None,
        }
    }
}

pub struct TaskConfig {
    pub machine_id: MachineId,
    pub parent: TaskParent,
    pub user: String,
    pub payload: Bytes,
}

impl Debug for TaskConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut f = f.debug_struct("TaskConfig");
        f.field("machine_id", &self.machine_id);
        match self.parent {
            TaskParent::Project(project_id) => f.field("parent", &project_id),
            TaskParent::Session(session_id) => f.field("session", &session_id),
        };
        f.field("user", &self.user);
        f.field("payload", &format_args!("<{} bytes>", self.payload.len()));
        f.finish()
    }
}

pub(crate) struct Task {
    id: TaskId,
    state: TaskState,
    backend_id: Option<String>,
    created_at: DateTime<Utc>,
    config: TaskConfig,
}

impl Task {
    pub fn new(task_id: TaskId, created_at: DateTime<Utc>, config: TaskConfig) -> Self {
        Task {
            id: task_id,
            state: TaskState::Waiting,
            backend_id: None,
            created_at,
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

    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
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

/// A snapshot of a task's public-facing data, resolved names and all — the shape returned by
/// `GET /tasks/{id}` and embedded in the task completion callback.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct TaskInfo {
    pub id: u64,
    /// Session the task ran in, if it was session-parented. Mutually exclusive with `project`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<u64>,
    /// Name of the project the task's time is charged to, if it was project-parented directly
    /// (not via a session). Mutually exclusive with `session`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    pub user: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend_id: Option<String>,
    /// Name of the target machine.
    pub machine: String,
    pub state: TaskStateKind,
    pub created_at: DateTime<Utc>,
    /// Absent if the task is still `waiting` or `running`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    /// Milliseconds of execution time consumed, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exectime_ms: Option<i64>,
    /// Present only when `state` is `"failed"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl TaskInfo {
    /// Builds a `TaskInfo` for `task`, using `state` rather than `task.state()` — at some
    /// notify call sites in `launcher.rs` the notification fires just before the task's stored
    /// state is updated, so the caller passes the about-to-be-applied state explicitly.
    pub(crate) fn build(
        task: &Task,
        state: &TaskState,
        machine: String,
        project: Option<String>,
    ) -> TaskInfo {
        let (finished_at, exectime_ms, error) = match state {
            TaskState::Waiting | TaskState::Running => (None, None, None),
            TaskState::Finished { completed } => {
                (Some(completed.timestamp), completed.consumed_ms(), None)
            }
            TaskState::Failed { completed, error } => (
                Some(completed.timestamp),
                completed.consumed_ms(),
                Some(error.clone()),
            ),
            TaskState::Cancelled { completed } => {
                (Some(completed.timestamp), completed.consumed_ms(), None)
            }
        };
        TaskInfo {
            id: task.id().as_u64(),
            session: task.config().parent.session_id().map(|s| s.as_u64()),
            project,
            user: task.config().user.clone(),
            backend_id: task.backend_id().map(String::from),
            machine,
            state: TaskStateKind::from(state),
            created_at: task.created_at(),
            finished_at,
            exectime_ms,
            error,
        }
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
