use crate::backend::{FromBackendMessage, create_backend};
use crate::config::RunnerConfiguration;
use crate::db;
use crate::db::close_dead_session;
use crate::error::RunnerError;
use crate::launcher::start_launcher;
use crate::machine::{Machine, MachineConfig, MachineId, MachineMap, ResumeTask};
use crate::project::{ProjectId, ProjectMap};
use crate::session::{Session, SessionConfig, SessionId, SessionMap, SessionState};
use crate::task::{Task, TaskConfig, TaskId, TaskMap, TaskParent, TaskState};
use bytes::Bytes;
use sqlx::postgres::PgPool;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;

#[allow(dead_code)]
pub(crate) struct CoreSplit<'a> {
    pub machine_map: &'a MachineMap,
    pub task_map: &'a TaskMap,
    pub session_map: &'a SessionMap,
    pub project_map: &'a ProjectMap,
    pub core_ref: &'a CoreRef,
}

#[allow(dead_code)]
pub(crate) struct CoreSplitMut<'a> {
    pub machine_map: &'a mut MachineMap,
    pub task_map: &'a mut TaskMap,
    pub session_map: &'a mut SessionMap,
    pub project_map: &'a mut ProjectMap,
    pub core_ref: &'a CoreRef,
}

pub struct Core {
    machine_map: MachineMap,
    task_map: TaskMap,
    session_map: SessionMap,
    project_map: ProjectMap,
    pool: PgPool,
    core_ref: Option<CoreRef>,
}

pub type CoreRef = Arc<Mutex<Core>>;

impl Core {
    pub async fn new(config: RunnerConfiguration, pool: PgPool) -> crate::Result<CoreRef> {
        let mut machine_map: MachineMap = Default::default();
        let mut backend_receivers = HashMap::with_capacity(config.machines.len());
        for (machine_id, m) in config.machines {
            let (backend, backend_receiver) = create_backend(&m.backend);
            machine_map.insert(Machine::new(
                machine_id,
                MachineConfig {
                    name: m.name,
                    queue_size: m.queue_size,
                    session_check_interval: Duration::from_millis(
                        m.session_check_interval_ms as u64,
                    ),
                    max_session_time: Duration::from_millis(m.max_session_time_ms),
                    notify: m.notify,
                    backend: m.backend,
                },
                backend,
            ));
            backend_receivers.insert(machine_id, backend_receiver);
        }
        let core_ref = Arc::new(Mutex::new(Core {
            machine_map,
            task_map: Default::default(),
            session_map: Default::default(),
            project_map: Default::default(),
            pool,
            core_ref: None,
        }));
        core_ref.lock().unwrap().core_ref = Some(core_ref.clone());

        let resume_tasks = Core::restore(&core_ref).await?;

        {
            let core = core_ref.lock().unwrap();
            core.start_launchers(backend_receivers, resume_tasks);
        }

        Ok(core_ref)
    }

    /// Rebuild in-memory state from the database after a (re)start.
    ///
    /// Reads all non-terminal tasks and sessions, then:
    /// - re-queues tasks that were still waiting,
    /// - re-attaches tasks that were already submitted to the backend (so their
    ///   state is reconciled against the backend by the launcher),
    /// - re-queues sessions that were waiting but never opened,
    /// - closes any session that was open and cancels all of its tasks.
    ///
    /// Must run before [`Core::start_launchers`] so queues are populated first.
    async fn restore(core_ref: &CoreRef) -> crate::Result<HashMap<MachineId, Vec<ResumeTask>>> {
        let pool = core_ref.lock().unwrap().pool().clone();
        let tasks = match db::load_active_tasks(&pool).await {
            Ok(t) => t,
            Err(e) => {
                tracing::error!(error = %e, "failed to load tasks for restoration");
                return Err(RunnerError::GenericError("Restoration failed".into()));
            }
        };
        let sessions = match db::load_active_sessions(&pool, &tasks).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "failed to load sessions for restoration");
                return Err(RunnerError::GenericError("Restoration failed".into()));
            }
        };
        // Load only the projects actually needed: tasks parented directly by a
        // project (not via a session) touch project_map during exec-time accounting,
        // and every session carries a project_id that's read once it opens/closes
        // (a re-queued session re-opens after restart and hits the same accounting).
        let project_ids: HashSet<ProjectId> = tasks
            .iter()
            .filter_map(|t| t.project_id)
            .filter_map(|id| ProjectId::try_from(id).ok())
            .chain(
                sessions
                    .iter()
                    .filter_map(|s| ProjectId::try_from(s.project_id).ok()),
            )
            .collect();
        let projects = match db::load_projects_by_ids(&pool, project_ids.iter()).await {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(error = %e, "failed to load projects for restoration");
                return Err(RunnerError::GenericError("Restoration failed".into()));
            }
        };

        // Cancelled tasks whose session is no longer active (data anomaly): cancel directly.
        let mut orphan_cancels: Vec<TaskId> = Vec::new();
        let mut woken: HashSet<MachineId> = HashSet::new();
        let mut resume_tasks: HashMap<MachineId, Vec<ResumeTask>> = HashMap::new();
        let mut dead_session_ids: Vec<SessionId> = Vec::new();
        let task_len = tasks.len();
        {
            let mut core = core_ref.lock().unwrap();
            let CoreSplitMut {
                machine_map,
                task_map,
                session_map,
                project_map,
                ..
            } = core.split_mut();

            for project in projects {
                project_map.add_project(project);
            }

            for s in &sessions {
                let Ok(session_id) = SessionId::try_from(s.id as u64) else {
                    continue;
                };
                let machine_id = MachineId::from(s.machine_id as u32);
                let project_id =
                    ProjectId::try_from(s.project_id as u32).expect("Invalid project id");
                let config = SessionConfig {
                    machine_id,
                    project_id,
                    time_limit: Duration::from_millis(s.time_limit_ms as u64),
                };
                let mut session = Session::new(session_id, config);
                if s.opened {
                    session.state = SessionState::Closed;
                    session_map.insert(session);
                    if !s.closed {
                        dead_session_ids.push(session_id);
                    }
                } else {
                    // Never opened (and therefore has no tasks yet): re-queue it.
                    if let Ok(machine) = machine_map.find_machine_mut(machine_id) {
                        let _ = machine.queue_session(&session);
                        woken.insert(machine_id);
                    }
                    session_map.insert(session);
                }
            }

            for t in tasks {
                let Ok(task_id) = TaskId::try_from(t.id as u64) else {
                    continue;
                };
                let machine_id = MachineId::from(t.machine_id as u32);
                let payload = Bytes::from(t.payload.clone().unwrap_or_default());
                let session_id = t.session_id.map(|id| SessionId::try_from(id).unwrap());
                let project_id = t.project_id.map(|id| ProjectId::try_from(id).unwrap());
                let config = TaskConfig {
                    machine_id,
                    parent: TaskParent::new(session_id, project_id),
                    user: t.user,
                    payload: payload.clone(),
                };

                if let Some(backend_id) = t.backend_id {
                    // Already submitted to the backend: re-attach for monitoring.
                    let mut task = Task::new(task_id, config);
                    task.set_state(TaskState::Running);
                    task.set_backend_id(backend_id.clone());
                    task_map.insert(task);
                    if machine_map.find_machine(machine_id).is_ok() {
                        resume_tasks
                            .entry(machine_id)
                            .or_default()
                            .push(ResumeTask {
                                task_id,
                                backend_id,
                                payload,
                                cancel: session_id.is_some(),
                            });
                    }
                } else if session_id.is_some() {
                    orphan_cancels.push(task_id);
                } else {
                    // Still waiting in the queue: re-queue it.
                    let task = Task::new(task_id, config);
                    if let Ok(machine) = machine_map.find_machine_mut(machine_id)
                        && machine.queue_task(&task).is_ok()
                    {
                        woken.insert(machine_id);
                    }
                    task_map.insert(task);
                }
            }

            for machine_id in &woken {
                if let Ok(machine) = machine_map.find_machine(*machine_id) {
                    machine.wake_launcher();
                }
            }
        }

        for session_id in dead_session_ids {
            close_dead_session(&pool, session_id).await;
        }

        if !orphan_cancels.is_empty() {
            db::update_tasks_cancelled(&pool, &orphan_cancels).await;
        }

        if task_len > 0 || !sessions.is_empty() {
            tracing::info!(
                restored_tasks = task_len,
                restored_sessions = sessions.len(),
                "restored state from database"
            );
        }
        Ok(resume_tasks)
    }

    pub(crate) fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub(crate) fn start_launchers(
        &self,
        mut backend_receivers: HashMap<MachineId, UnboundedReceiver<FromBackendMessage>>,
        mut resume_tasks: HashMap<MachineId, Vec<ResumeTask>>,
    ) {
        let core_ref = self.core_ref.as_ref().unwrap();
        for m in self.machine_map.iter() {
            let machine_id = m.id();
            let backend_receiver = backend_receivers.remove(&machine_id).unwrap();
            let resume_tasks = resume_tasks.remove(&machine_id).unwrap_or_default();
            start_launcher(
                core_ref,
                machine_id,
                m.config(),
                m.backend().clone(),
                backend_receiver,
                resume_tasks,
            );
        }
    }

    #[inline]
    pub(crate) fn split(&self) -> CoreSplit<'_> {
        CoreSplit {
            machine_map: &self.machine_map,
            task_map: &self.task_map,
            session_map: &self.session_map,
            project_map: &self.project_map,
            core_ref: self.core_ref.as_ref().unwrap(),
        }
    }

    #[inline]
    pub(crate) fn split_mut(&mut self) -> CoreSplitMut<'_> {
        CoreSplitMut {
            machine_map: &mut self.machine_map,
            task_map: &mut self.task_map,
            session_map: &mut self.session_map,
            project_map: &mut self.project_map,
            core_ref: self.core_ref.as_ref().unwrap(),
        }
    }

    pub(crate) fn validate_session_config(&self, config: &SessionConfig) -> crate::Result<()> {
        let machine = self.machine_map.find_machine(config.machine_id)?;
        let limit = machine.config().max_session_time;
        if config.time_limit > limit {
            return Err(RunnerError::SessionDurationExceedsLimit {
                machine: machine.config().name.clone(),
                limit,
                requested: config.time_limit,
            });
        }
        let project = self
            .project_map
            .find_project(config.project_id)
            .ok_or_else(|| RunnerError::ProjectNotFound(config.project_id.to_string()))?;
        if !project.active {
            return Err(RunnerError::ProjectNotActive(project.name.clone()));
        }
        if !project.has_time_for(config.time_limit) {
            return Err(RunnerError::ProjectLimitExceeded(project.name.clone()));
        }
        Ok(())
    }

    pub(crate) fn validate_task_config(&self, config: &TaskConfig) -> crate::Result<()> {
        let machine = self.machine_map.find_machine(config.machine_id)?;
        if let Some(s_id) = config.parent.session_id() {
            match machine.running_session_id() {
                Some(id) if id == s_id => {}
                _ => return Err(RunnerError::NonRunningSession(s_id)),
            }
        }
        if let Some(project_id) = config.parent.project_id()
            && let Some(project) = self.project_map.find_project(project_id)
        {
            if !project.active {
                return Err(RunnerError::ProjectNotActive(project.name.clone()));
            }
            if project.is_over_limit() {
                return Err(RunnerError::ProjectLimitExceeded(project.name.clone()));
            }
        }
        Ok(())
    }

    pub(crate) fn add_task(
        &mut self,
        task_id: TaskId,
        config: TaskConfig,
    ) -> crate::Result<TaskId> {
        let CoreSplitMut {
            machine_map,
            task_map,
            ..
        } = self.split_mut();
        let machine = machine_map.find_machine_mut(config.machine_id)?;
        let task = Task::new(task_id, config);
        machine.queue_task(&task)?;
        task_map.insert(task);
        machine.wake_launcher();
        Ok(task_id)
    }

    pub(crate) fn add_session(
        &mut self,
        session_id: SessionId,
        config: SessionConfig,
    ) -> crate::Result<SessionId> {
        let CoreSplitMut {
            machine_map,
            session_map,
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

    pub fn get_arch(
        &self,
        machine_id: MachineId,
    ) -> crate::Result<oneshot::Receiver<crate::Result<String>>> {
        let machine = self.machine_map.find_machine(machine_id)?;
        Ok(Arc::clone(machine.backend()).get_arch())
    }

    pub fn get_calibration(
        &self,
        machine_id: MachineId,
        calibration_id: &str,
        endpoint: &str,
    ) -> crate::Result<oneshot::Receiver<crate::Result<String>>> {
        let machine = self.machine_map.find_machine(machine_id)?;
        Ok(Arc::clone(machine.backend()).get_calibration(calibration_id, endpoint))
    }
}
