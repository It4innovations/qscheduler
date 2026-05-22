use crate::backend::BackendConfig;
use crate::callback::NotifyConfig;
use crate::error::RunnerError;
use crate::session::{Session, SessionId};
use crate::task::{Task, TaskId};
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
    pub notify: Option<NotifyConfig>,
    pub backend: BackendConfig,
}

struct RunningSession {
    session_id: SessionId,
    running_task: Option<TaskId>,
    queue: VecDeque<TaskId>,
}

enum MachineState {
    Idle,
    RunningTask(TaskId),
    RunningSession(RunningSession),
}

pub(crate) struct Machine {
    machine_id: MachineId,
    queue: VecDeque<QueueItem>,
    state: MachineState,
    config: MachineConfig,
    notifier: Arc<Notify>,
}

impl Machine {
    pub fn new(machine_id: MachineId, config: MachineConfig) -> Self {
        Machine {
            machine_id,
            queue: VecDeque::new(),
            state: MachineState::Idle,
            config,
            notifier: Arc::new(Notify::new()),
        }
    }

    pub fn id(&self) -> MachineId {
        self.machine_id
    }

    pub fn config(&self) -> &MachineConfig {
        &self.config
    }

    pub fn queue_task(&mut self, task: &Task) -> crate::Result<()> {
        if let Some(s_id) = task.config().session_id {
            match &mut self.state {
                MachineState::RunningSession(s) if s_id == s.session_id => {
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

    pub fn complete_task(&mut self, task_id: TaskId) {
        match &mut self.state {
            MachineState::Idle => unreachable!(),
            MachineState::RunningTask(running_id) => {
                assert_eq!(*running_id, task_id);
                self.state = MachineState::Idle;
            }
            MachineState::RunningSession(s) => {
                assert_eq!(s.running_task, Some(task_id));
                s.running_task = None;
            }
        }
    }

    pub fn wake_launcher(&self) {
        self.notifier.notify_one();
    }

    // fn start_task(&mut self, core_ref: &CoreRef, task_map: &mut TaskMap, task_id: TaskId) {
    //     {
    //         let task = task_map.get_task_mut(task_id);
    //         task.set_state(TaskState::Running);
    //     }
    //     let core_ref = core_ref.clone();
    //     tokio::spawn(async move {
    //         let r = run_task(&core_ref, task_id).await;
    //         let mut core = core_ref.lock().unwrap();
    //         let CoreSplitMut { task_map, machine_map, .. } = core.split_mut();
    //         let task = task_map.get_task_mut(task_id);
    //         match r {
    //             Ok(()) => task.set_state(TaskState::Finished),
    //             Err(e) => task.set_state(TaskState::Failed { error: e.to_string() })
    //         }
    //         let machine_id = task.config().machine_id;
    //         let machine = machine_map.get_machine_mut(machine_id);
    //         machine.complete_task(task_id);
    //         machine.try_wake(&core_ref, task_map);
    //     });
    // }

    // pub fn try_wake(&mut self, core_ref: &CoreRef, task_map: &mut TaskMap) {
    //     match &mut self.state {
    //         MachineState::Idle => {
    //             match self.queue.pop_front() {
    //                 Some(QueueItem::Task(task_id)) => {
    //                     self.state = MachineState::RunningTask(task_id);
    //                     self.start_task(core_ref, task_map, task_id);
    //                 }
    //                 Some(QueueItem::Session(session_id)) => {
    //                     self.state = MachineState::RunningSession(RunningSession{
    //                         session_id,
    //                         running_task: None,
    //                         queue: VecDeque::new(),
    //                     });
    //                 }
    //                 None => return
    //             }
    //         }
    //         | MachineState::RunningSession(s) if s.running_task.is_none() => {
    //             if let Some(task_id) = s.queue.pop_front() {
    //                 s.running_task = Some(task_id);
    //                 self.start_task(core_ref, task_map, task_id);
    //             }
    //         }
    //         _ => return
    //     }
    // }

    pub fn start_session(&mut self, session_id: SessionId) {
        assert!(matches!(self.state, MachineState::Idle));
        self.state = MachineState::RunningSession(RunningSession {
            session_id,
            running_task: None,
            queue: VecDeque::new(),
        });
    }

    pub fn close_session(&mut self) {
        assert!(matches!(self.state, MachineState::RunningSession(_)));
        self.state = MachineState::Idle;
    }

    #[inline]
    pub fn pop_session_task(&mut self) -> Option<TaskId> {
        match self.state {
            MachineState::RunningSession(ref mut s) => s.queue.pop_front(),
            _ => None,
        }
    }

    pub fn set_running_task(&mut self, task_id: TaskId) {
        match self.state {
            MachineState::RunningSession(ref mut s) => {
                s.running_task = Some(task_id);
            }
            MachineState::Idle => {
                self.state = MachineState::RunningTask(task_id);
            }
            MachineState::RunningTask(_) => {
                unreachable!()
            }
        }
    }

    #[inline]
    pub fn pop_queue_item(&mut self) -> Option<QueueItem> {
        self.queue.pop_front()
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

#[derive(Default)]
pub(crate) struct MachineMap(HashMap<MachineId, Machine>);

impl MachineMap {
    // pub fn find_machine(&self, machine_id: MachineId) -> crate::Result<&Machine> {
    //     self.0
    //         .get(&machine_id)
    //         .ok_or(RunnerError::InvalidMachine(machine_id))
    // }
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
