use crate::callback::{NotifierId, NotifierMap};
use crate::machine::MachineId;
use crate::project::ProjectId;
use crate::session::SessionId;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
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

/// What kind of job a task's payload describes; selects the backend submission route.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum TaskKind {
    /// A batch of quantum circuits (IQM: JSON `CircuitJobDefinition`).
    #[default]
    Circuit,
    /// A compiled pulse-level job (IQM: protobuf `RunDefinition`, e.g. from IQM Pulla).
    Pulse,
}

impl TaskKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskKind::Circuit => "circuit",
            TaskKind::Pulse => "pulse",
        }
    }

    pub fn from_db(s: &str) -> Self {
        match s {
            "circuit" => TaskKind::Circuit,
            "pulse" => TaskKind::Pulse,
            other => unreachable!("unknown task kind '{other}' loaded from db"),
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
    pub kind: TaskKind,
    pub payload: Bytes,
    /// Effective execution time limit, counted from when the backend first reports the task
    /// as running. Resolved at submit time (explicit value or the machine's default).
    pub time_limit: Duration,
    /// Notifier that receives the task's completion event, if any.
    pub notifier: Option<NotifierId>,
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
        f.field("kind", &self.kind);
        f.field("time_limit", &self.time_limit);
        f.field("notifier", &self.notifier);
        f.field("payload", &format_args!("<{} bytes>", self.payload.len()));
        f.finish()
    }
}

pub(crate) struct Task {
    id: TaskId,
    state: TaskState,
    backend_id: Option<String>,
    created_at: DateTime<Utc>,
    /// When the backend first reported the task as running (start of the time limit).
    started_at: Option<DateTime<Utc>>,
    config: TaskConfig,
}

impl Task {
    pub fn new(task_id: TaskId, created_at: DateTime<Utc>, config: TaskConfig) -> Self {
        Task {
            id: task_id,
            state: TaskState::Waiting,
            backend_id: None,
            created_at,
            started_at: None,
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

    pub fn started_at(&self) -> Option<DateTime<Utc>> {
        self.started_at
    }

    pub fn set_started_at(&mut self, started_at: DateTime<Utc>) {
        self.started_at = Some(started_at);
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
    /// Free-form identifier of the user who submitted the task.
    pub user: String,
    /// Whether the payload is a gate-level circuit or a pulse-level program.
    pub kind: TaskKind,
    /// Backend-assigned job ID. Absent until the task is submitted to the backend.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend_id: Option<String>,
    /// Name of the target machine.
    pub machine: String,
    pub state: TaskStateKind,
    pub created_at: DateTime<Utc>,
    /// Execution time limit in milliseconds, counted from `started_at`. When exceeded, the task
    /// is cancelled on the backend and ends `failed`. Absent only for tasks created before time
    /// limits existed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_limit_ms: Option<u64>,
    /// When the backend started running the task, on the backend's clock. Absent until then.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    /// When the task reached its final state; absent while it is `waiting` or `running`. For a
    /// task that ran on the backend, this is the backend's timestamp (`finished_at - started_at`
    /// equals `exectime_ms`); for a task ended by the scheduler itself (e.g. cancelled while
    /// queued), it is the scheduler's time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    /// Milliseconds of execution time consumed, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exectime_ms: Option<i64>,
    /// Present only when `state` is `"failed"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Name of the notifier that receives the task's completion event. Absent if the task was
    /// submitted without `notify`, or its notifier has since been removed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notify: Option<String>,
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
        notifiers: &NotifierMap,
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
            kind: task.config().kind,
            backend_id: task.backend_id().map(String::from),
            machine,
            state: TaskStateKind::from(state),
            created_at: task.created_at(),
            time_limit_ms: Some(task.config().time_limit.as_millis() as u64),
            started_at: task.started_at(),
            finished_at,
            exectime_ms,
            error,
            notify: notifiers.name(task.config().notifier),
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

    pub fn iter(&self) -> impl Iterator<Item = &Task> {
        self.0.values()
    }

    /// Removes every entry for which `keep` returns `false`.
    pub fn retain(&mut self, mut keep: impl FnMut(&Task) -> bool) {
        self.0.retain(|_, v| keep(v));
    }
}
