use std::collections::{HashMap, VecDeque};
use crate::config::RunnerConfiguration;
use crate::machine::{Machine, MachineId};
use crate::task::{Task, TaskConfig, TaskId};

pub struct Core {
    tasks: HashMap<TaskId, Task>,
    queue: VecDeque<TaskId>,
    current_task: Option<TaskId>,
    machines: HashMap<MachineId, Machine>,
    task_id_counter: TaskId,
    config: RunnerConfiguration
}

impl Core {
    pub fn new(config: RunnerConfiguration) -> Self {
        Core {
            machines: HashMap::new(),
            queue: VecDeque::new(),
            tasks: HashMap::new(),
            current_task: None,
            task_id_counter: Default::default(),
            config
        }
    }

    pub(crate) fn add_task(&mut self, config: TaskConfig) -> TaskId {
        let task_id = self.task_id_counter.bump();
        let task = Task::new(task_id, config);
        self.tasks.insert(task_id, task);
        self.queue.push_back(task_id);
        task_id
    }

    pub(crate) fn get_task(&self, task_id: TaskId) -> &Task {
        self.tasks.get(&task_id).unwrap()
    }

    pub(crate) fn get_task_mut(&mut self, task_id: TaskId) -> &mut Task {
        self.tasks.get_mut(&task_id).unwrap()
    }

    pub(crate) fn set_task_state(&mut self, task_id: TaskId, state: crate::task::TaskState) {
        self.get_task_mut(task_id).set_state(state);
    }

    pub(crate) fn backend_url(&self) -> &str {
        &self.config.machine.backend_url
    }
}