use crate::config::RunnerConfiguration;
use crate::launcher::start_launcher;
use crate::machine::{Machine, MachineConfig, MachineId, MachineMap};
use crate::session::{Session, SessionConfig, SessionId, SessionMap, SessionState};
use crate::task::{Task, TaskConfig, TaskId, TaskMap, TaskState};
use std::sync::{Arc, Mutex};

#[allow(dead_code)]
pub(crate) struct CoreSplit<'a> {
    pub machine_map: &'a MachineMap,
    pub task_map: &'a TaskMap,
    pub session_map: &'a SessionMap,
    pub core_ref: &'a CoreRef,
}

#[allow(dead_code)]
pub(crate) struct CoreSplitMut<'a> {
    pub machine_map: &'a mut MachineMap,
    pub task_map: &'a mut TaskMap,
    pub session_map: &'a mut SessionMap,
    pub core_ref: &'a CoreRef,
}

pub struct Core {
    machine_map: MachineMap,
    task_map: TaskMap,
    session_map: SessionMap,
    task_id_counter: TaskId,
    session_id_counter: SessionId,
    core_ref: Option<CoreRef>,
}

pub type CoreRef = Arc<Mutex<Core>>;

impl Core {
    pub fn new(config: RunnerConfiguration) -> CoreRef {
        let mut machine_map: MachineMap = Default::default();
        for m in config.machines {
            let machine_id = MachineId::from(m.id);
            machine_map.insert(Machine::new(
                machine_id,
                MachineConfig {
                    name: m.name,
                    queue_size: m.queue_size,
                    notify: m.notify,
                    backend: m.backend,
                },
            ));
        }
        let core_ref = Arc::new(Mutex::new(Core {
            machine_map,
            task_map: Default::default(),
            session_map: Default::default(),
            session_id_counter: Default::default(),
            task_id_counter: Default::default(),
            core_ref: None,
        }));
        core_ref.lock().unwrap().core_ref = Some(core_ref.clone());

        {
            let core = core_ref.lock().unwrap();
            core.start_launchers();
        }

        core_ref
    }

    pub(crate) fn start_launchers(&self) {
        let core_ref = self.core_ref.as_ref().unwrap();
        for m in self.machine_map.iter() {
            start_launcher(core_ref, m.id(), m.config())
        }
    }

    pub(crate) fn split(&self) -> CoreSplit<'_> {
        CoreSplit {
            machine_map: &self.machine_map,
            task_map: &self.task_map,
            session_map: &self.session_map,
            core_ref: self.core_ref.as_ref().unwrap(),
        }
    }

    pub(crate) fn split_mut(&mut self) -> CoreSplitMut<'_> {
        CoreSplitMut {
            machine_map: &mut self.machine_map,
            task_map: &mut self.task_map,
            session_map: &mut self.session_map,
            core_ref: self.core_ref.as_ref().unwrap(),
        }
    }

    pub(crate) fn add_task(&mut self, config: TaskConfig) -> crate::Result<TaskId> {
        let task_id = self.task_id_counter.bump();
        let CoreSplitMut {
            machine_map,
            task_map,
            core_ref: _,
            ..
        } = self.split_mut();
        let machine = machine_map.find_machine_mut(config.machine_id)?;
        let task = Task::new(task_id, config);
        machine.queue_task(&task)?;
        task_map.insert(task);
        machine.wake_launcher();
        Ok(task_id)
    }

    pub(crate) fn add_session(&mut self, config: SessionConfig) -> crate::Result<SessionId> {
        let session_id = self.session_id_counter.bump();
        let CoreSplitMut {
            machine_map,
            session_map,
            core_ref: _,
            task_map: _,
            ..
        } = self.split_mut();
        let machine = machine_map.find_machine_mut(config.machine_id)?;
        let session = Session::new(session_id, config);
        machine.queue_session(&session)?;
        session_map.insert(session);
        machine.wake_launcher();
        Ok(session_id)
    }

    pub fn task_state(&self, task_id: TaskId) -> Option<&TaskState> {
        self.task_map.find_task(task_id).map(|t| t.state())
    }

    pub fn session_state(&self, session_id: SessionId) -> Option<SessionState> {
        self.session_map.find_session(session_id).map(|s| s.state)
    }

    // pub(crate) fn get_task(&self, task_id: TaskId) -> &Task {
    //     self.task_map.get_task(task_id)
    // }
    //
    // pub(crate) fn get_task_mut(&mut self, task_id: TaskId) -> &mut Task {
    //     self.task_map.get_task_mut(task_id)
    // }
    //
    // pub(crate) fn set_task_state(&mut self, task_id: TaskId, state: crate::task::TaskState) {
    //     self.get_task_mut(task_id).set_state(state);
    // }
    //
    // pub fn task_state(&self, task_id: TaskId) -> Option<crate::task::TaskState> {
    //     self.tasks.get(&task_id).map(|t| t.state().clone())
    // }
}
