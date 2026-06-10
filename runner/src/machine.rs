use crate::backend::{Backend, BackendConfig};
use crate::callback::NotifyConfig;
use crate::error::RunnerError;
use crate::session::{Session, SessionId};
use crate::task::{Task, TaskId};
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::fmt::Display;
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, Default)]
pub struct MachineId(u32);

impl Display for MachineId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Copy)]
pub(crate) enum QueueItem {
    Task(TaskId),
    Session(SessionId),
}

pub struct MachineConfig {
    pub name: String,
    pub queue_size: usize,
    pub notify: Option<NotifyConfig>,
    pub backend: BackendConfig,
}

struct RunningSession {
    session_id: SessionId,
    queue: VecDeque<TaskId>,
}

/// A task that was already submitted to the backend before a restart and must be
/// re-attached to the launcher's backend on startup instead of being re-queued.
pub(crate) struct ResumeTask {
    pub task_id: TaskId,
    pub backend_id: String,
    pub payload: Bytes,
}

pub(crate) struct Machine {
    machine_id: MachineId,
    queue: VecDeque<QueueItem>,
    running_session: Option<RunningSession>,
    config: MachineConfig,
    notifier: Arc<Notify>,
    backend: Arc<dyn Backend>,
}

impl Machine {
    pub fn new(machine_id: MachineId, config: MachineConfig, backend: Arc<dyn Backend>) -> Self {
        Machine {
            machine_id,
            queue: VecDeque::new(),
            running_session: None,
            config,
            notifier: Arc::new(Notify::new()),
            backend
        }
    }

    pub fn id(&self) -> MachineId {
        self.machine_id
    }

    pub fn config(&self) -> &MachineConfig {
        &self.config
    }

    pub fn backend(&self) -> &Arc<dyn Backend> {
        &self.backend
    }

    pub fn queue_task(&mut self, task: &Task) -> crate::Result<()> {
        if let Some(s_id) = task.config().parent.session_id() {
            match &mut self.running_session {
                Some(s) if s_id == s.session_id => {
                    s.queue.push_back(task.id());
                }
                _ => return Err(RunnerError::NonRunningSession(s_id)),
            }
        } else {
            self.queue.push_back(QueueItem::Task(task.id()));
        }
        Ok(())
    }

    pub fn queue_session(&mut self, session: &Session) -> crate::Result<()> {
        self.queue.push_back(QueueItem::Session(session.id()));
        Ok(())
    }

    pub fn wake_launcher(&self) {
        self.notifier.notify_one();
    }

    pub fn start_session(&mut self, session_id: SessionId) {
        assert!(self.running_session.is_none());
        self.running_session = Some(RunningSession {
            session_id,
            queue: VecDeque::new(),
        });
    }

    pub fn close_session(&mut self) {
        assert!(self.running_session.is_some());
        self.running_session = None;
    }

    #[inline]
    pub fn pop_session_task(&mut self) -> Option<TaskId> {
        self.running_session
            .as_mut()
            .and_then(|s| s.queue.pop_front())
    }

    #[inline]
    pub fn pop_queue_item(&mut self, allow_sessions: bool) -> Option<QueueItem> {
        if !allow_sessions && let Some(QueueItem::Session(_)) = self.queue.front() {
            return None;
        }
        self.queue.pop_front()
    }

    pub fn running_session_id(&self) -> Option<SessionId> {
        self.running_session.as_ref().map(|s| s.session_id)
    }

    pub fn notifier(&self) -> &Arc<Notify> {
        &self.notifier
    }
}

impl From<u32> for MachineId {
    fn from(v: u32) -> Self {
        MachineId(v)
    }
}

impl MachineId {
    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

#[derive(Default)]
pub(crate) struct MachineMap(HashMap<MachineId, Machine>);

impl MachineMap {
    pub fn find_machine(&self, machine_id: MachineId) -> crate::Result<&Machine> {
        self.0
            .get(&machine_id)
            .ok_or(RunnerError::InvalidMachine(machine_id))
    }
    pub fn find_machine_mut(&mut self, machine_id: MachineId) -> crate::Result<&mut Machine> {
        self.0
            .get_mut(&machine_id)
            .ok_or(RunnerError::InvalidMachine(machine_id))
    }
    pub fn get_machine(&self, machine_id: MachineId) -> &Machine {
        self.0.get(&machine_id).unwrap()
    }
    pub fn get_machine_mut(&mut self, machine_id: MachineId) -> &mut Machine {
        self.0.get_mut(&machine_id).unwrap()
    }
    pub fn insert(&mut self, machine: Machine) {
        let machine_id = machine.machine_id;
        self.0.insert(machine_id, machine);
    }

    pub fn iter(&self) -> impl Iterator<Item = &Machine> {
        self.0.values()
    }
}
