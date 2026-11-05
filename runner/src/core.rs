use crate::backend::{BackendFuture, BackendStream, FromBackendMessage, create_backend};
use crate::callback::{NotifierId, NotifierMap};
use crate::config::{MachineConfiguration, RunnerConfiguration};
use crate::db::close_dead_session;
use crate::error::RunnerError;
use crate::launcher::start_launcher;
use crate::machine::{Machine, MachineConfig, MachineId, MachineMap, ResumeTask};
use crate::project::{ProjectId, ProjectMap};
use crate::session::{Session, SessionConfig, SessionId, SessionInfo, SessionMap, SessionState};
use crate::task::{Task, TaskConfig, TaskId, TaskInfo, TaskKind, TaskMap, TaskParent, TaskState};
use crate::user::{User, UserId, UserMap};
use crate::{BackendVersion, db};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use sqlx::postgres::PgPool;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[allow(dead_code)]
pub(crate) struct CoreSplit<'a> {
    pub machine_map: &'a MachineMap,
    pub task_map: &'a TaskMap,
    pub session_map: &'a SessionMap,
    pub project_map: &'a ProjectMap,
    pub user_map: &'a UserMap,
    pub notifiers: &'a NotifierMap,
    pub core_ref: &'a CoreRef,
}

#[allow(dead_code)]
pub(crate) struct CoreSplitMut<'a> {
    pub machine_map: &'a mut MachineMap,
    pub task_map: &'a mut TaskMap,
    pub session_map: &'a mut SessionMap,
    pub project_map: &'a mut ProjectMap,
    pub user_map: &'a mut UserMap,
    pub notifiers: &'a mut NotifierMap,
    pub core_ref: &'a CoreRef,
}

/// A running launcher of one machine.
struct LauncherHandle {
    stop: watch::Sender<bool>,
    join: JoinHandle<()>,
}

pub struct Core {
    machine_map: MachineMap,
    task_map: TaskMap,
    session_map: SessionMap,
    project_map: ProjectMap,
    user_map: UserMap,
    max_user_active_items: u32,
    max_user_active_payload: u64,
    notifiers: NotifierMap,
    pool: PgPool,
    core_ref: Option<CoreRef>,
    launchers: HashMap<MachineId, LauncherHandle>,
    /// Serializes suspend/resume, which update the DB and then the in-memory state.
    suspension_lock: Arc<tokio::sync::Mutex<()>>,
    /// Serializes adding, updating and removing machines and notifiers.
    admin_lock: Arc<tokio::sync::Mutex<()>>,
}

pub type CoreRef = Arc<Mutex<Core>>;

/// Quantum-computer artifacts served by `Core::get_qc_artifact`: the data a client needs to
/// compile pulse-level jobs (e.g. with IQM Pulla).
pub const QC_ARTIFACTS: &[&str] = &[
    "settings",
    "chip-design-records",
    "channel-properties",
    "static-quantum-architectures",
    "duts",
];

impl Core {
    pub async fn new(config: RunnerConfiguration, pool: PgPool) -> crate::Result<CoreRef> {
        let mut machine_map: MachineMap = Default::default();
        let mut backend_receivers = HashMap::with_capacity(config.machines.len());
        for (machine_id, m) in config.machines {
            let (machine, backend_receiver) = create_machine(machine_id, m);
            machine_map.insert(machine);
            backend_receivers.insert(machine_id, backend_receiver);
        }
        let core_ref = Arc::new(Mutex::new(Core {
            machine_map,
            task_map: Default::default(),
            session_map: Default::default(),
            project_map: Default::default(),
            user_map: Default::default(),
            max_user_active_items: config.max_user_active_items,
            max_user_active_payload: config.max_user_active_payload,
            notifiers: NotifierMap::start(config.notifiers),
            pool,
            core_ref: None,
            launchers: HashMap::new(),
            suspension_lock: Default::default(),
            admin_lock: Default::default(),
        }));
        core_ref.lock().unwrap().core_ref = Some(core_ref.clone());

        let mut resume_tasks = Core::restore(&core_ref, None).await?;

        {
            let mut core = core_ref.lock().unwrap();
            for (machine_id, backend_receiver) in backend_receivers {
                let resume_tasks = resume_tasks.remove(&machine_id).unwrap_or_default();
                core.start_launcher(machine_id, backend_receiver, resume_tasks);
            }
        }
        start_eviction(&core_ref, config.task_retention);

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
    /// With `only` set, just the state of that machine is restored (when its launcher is
    /// restarted after a reconfiguration); projects already in memory are kept as they are.
    ///
    /// Must run before [`Core::start_launcher`] so queues are populated first.
    pub(crate) async fn restore(
        core_ref: &CoreRef,
        only: Option<MachineId>,
    ) -> crate::Result<HashMap<MachineId, Vec<ResumeTask>>> {
        let pool = core_ref.lock().unwrap().pool().clone();
        let wanted = |raw_machine_id: i32| only.is_none_or(|m| m.as_i32() == raw_machine_id);
        // An id unknown to the loaded notifiers (one CLI-added after startup) needs no
        // filtering: `NotifierMap` treats it like no notifier.
        let restore_notifier = |raw: Option<i32>| raw.map(NotifierId::from_db);
        let mut tasks = match db::load_active_tasks(&pool).await {
            Ok(t) => t,
            Err(e) => {
                tracing::error!(error = %e, "failed to load tasks for restoration");
                return Err(RunnerError::GenericError("Restoration failed".into()));
            }
        };
        let mut sessions = match db::load_active_sessions(&pool, &tasks).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "failed to load sessions for restoration");
                return Err(RunnerError::GenericError("Restoration failed".into()));
            }
        };
        tasks.retain(|t| wanted(t.machine_id));
        sessions.retain(|s| wanted(s.machine_id));
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
        let suspensions = match db::load_active_suspensions(&pool).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "failed to load machine suspensions for restoration");
                return Err(RunnerError::GenericError("Restoration failed".into()));
            }
        };

        // Cancelled tasks whose session is no longer active (data anomaly): cancel directly.
        let mut orphan_cancels: Vec<TaskId> = Vec::new();
        let mut woken: HashSet<MachineId> = HashSet::new();
        let mut resume_tasks: HashMap<MachineId, Vec<ResumeTask>> = HashMap::new();
        let mut dead_sessions: Vec<(SessionId, DateTime<Utc>)> = Vec::new();
        let now = Utc::now();
        let task_len = tasks.len();
        {
            let mut core = core_ref.lock().unwrap();
            let CoreSplitMut {
                machine_map,
                task_map,
                session_map,
                project_map,
                user_map,
                ..
            } = core.split_mut();

            // A project already in memory carries in-flight time and checkpoints of other
            // machines' work that its DB row may not reflect yet.
            for project in projects {
                if project_map.find_project(project.id).is_none() {
                    project_map.add_project(project);
                }
            }

            // Only the "rejects new work" part of a suspension is restored: its `cancel` policy
            // was applied to existing work when the suspension started.
            for (machine_id, suspension) in suspensions {
                if only.is_some_and(|m| m != machine_id) {
                    continue;
                }
                match machine_map.find_machine_mut(machine_id) {
                    Ok(machine) => machine.set_suspension(Some(suspension)),
                    Err(_) => tracing::warn!(
                        %machine_id,
                        "suspension references unknown machine on restore"
                    ),
                }
            }

            for s in &sessions {
                let Ok(session_id) = SessionId::try_from(s.id as u64) else {
                    tracing::error!(
                        raw_id = s.id,
                        "skipping session with invalid id during restore"
                    );
                    continue;
                };
                let machine_id = MachineId::from_db(s.machine_id);
                let project_id = match ProjectId::try_from(s.project_id as u32) {
                    Ok(id) => id,
                    Err(_) => {
                        tracing::error!(
                            %session_id,
                            raw_project_id = s.project_id,
                            "invalid project id on session during restore"
                        );
                        panic!("Invalid project id");
                    }
                };
                let config = SessionConfig {
                    machine_id,
                    project_id,
                    owner_id: UserId::from_db(s.user_id),
                    time_limit: Duration::from_millis(s.time_limit_ms as u64),
                    notifier: restore_notifier(s.notifier_id),
                };
                let mut session = Session::new(session_id, s.created_at, config);
                session.consumed = Duration::from_millis(s.exec_time_ms.unwrap_or(0) as u64);
                if let Some(opened_at) = s.opened_at {
                    // Was opened but crashed before (or without) a clean close: prefer the DB's
                    // own closed_at if it's already set (closed, just not yet reflected at the
                    // task level — see the doc comment on `load_active_sessions`), else fall
                    // back to the last checkpoint (`updated_at`) rather than the restart time,
                    // which would overstate how long the session was actually open.
                    let closed_at = s.closed_at.or(s.updated_at).unwrap_or(now);
                    // A session with `closed_at` set was already charged when it closed.
                    if s.closed_at.is_none()
                        && let Ok(machine) = machine_map.find_machine_mut(machine_id)
                    {
                        machine.record_closed_session(session.config.owner_id, session.consumed);
                    }
                    session.set_restored_closed(opened_at, closed_at, &s.owner, user_map);
                    session_map.insert(session);
                    if s.closed_at.is_none() {
                        dead_sessions.push((session_id, closed_at));
                    }
                } else {
                    // Never opened (and therefore has no tasks yet): re-queue it.
                    match machine_map.find_machine_mut(machine_id) {
                        Ok(machine) => {
                            if machine.queue_session(&session).is_ok() {
                                woken.insert(machine_id);
                            } else {
                                tracing::warn!(
                                    %session_id,
                                    %machine_id,
                                    "failed to requeue session on restore (queue full)"
                                );
                            }
                        }
                        Err(_) => {
                            tracing::warn!(
                                %session_id,
                                %machine_id,
                                "session references unknown machine on restore, not requeued"
                            );
                        }
                    }
                    user_map.insert(session.config.owner_id, &s.owner);
                    keep_active_session(session_map, user_map, session);
                }
            }

            for t in tasks {
                let Ok(task_id) = TaskId::try_from(t.id as u64) else {
                    tracing::error!(
                        raw_id = t.id,
                        "skipping task with invalid id during restore"
                    );
                    continue;
                };
                let machine_id = MachineId::from_db(t.machine_id);
                let payload = Bytes::from(t.payload.clone().unwrap_or_default());
                let session_id = t.session_id.map(|id| {
                    SessionId::try_from(id).unwrap_or_else(|_| {
                        tracing::error!(
                            %task_id,
                            raw_session_id = id,
                            "invalid session id on task during restore"
                        );
                        panic!("Invalid session id");
                    })
                });
                let project_id = t.project_id.map(|id| {
                    ProjectId::try_from(id).unwrap_or_else(|_| {
                        tracing::error!(
                            %task_id,
                            raw_project_id = id,
                            "invalid project id on task during restore"
                        );
                        panic!("Invalid project id");
                    })
                });
                // Rows predating per-task limits have no `time_limit_ms`: use the machine's
                // default (an unknown machine never runs the task, so the value is moot).
                let time_limit = match t.time_limit_ms {
                    Some(ms) => Duration::from_millis(ms as u64),
                    None => machine_map
                        .find_machine(machine_id)
                        .map_or(Duration::ZERO, |m| m.config().default_task_time_limit),
                };
                let config = TaskConfig {
                    machine_id,
                    parent: TaskParent::new(session_id, project_id),
                    user_id: UserId::from_db(t.user_id),
                    kind: TaskKind::from_db(&t.kind),
                    payload: payload.clone(),
                    time_limit,
                    notifier: restore_notifier(t.notifier_id),
                };
                if let Some(backend_id) = t.backend_id {
                    // Already submitted to the backend: re-attach for monitoring.
                    let mut task = Task::new(task_id, t.created_at, config);
                    task.set_state(TaskState::Running, user_map);
                    task.set_backend_id(backend_id.clone());
                    if let Some(started_at) = t.started_at {
                        task.set_started_at(started_at);
                    }
                    if let Ok(machine) = machine_map.find_machine_mut(machine_id) {
                        machine.record_new_task_in_backend(task.config());
                        resume_tasks
                            .entry(machine_id)
                            .or_default()
                            .push(ResumeTask {
                                task_id,
                                backend_id,
                                payload,
                                cancel: session_id.is_some(),
                                started_at: t.started_at,
                                time_limit,
                                project_id,
                            });
                    } else {
                        tracing::warn!(
                            %task_id,
                            %machine_id,
                            "submitted task references unknown machine on restore, will never be re-attached for monitoring"
                        );
                    }
                    keep_restored_task(task_map, user_map, &t.user_name, task);
                } else if session_id.is_some() {
                    // Cancelled right away and never kept, so not counted as its user's.
                    orphan_cancels.push(task_id);
                } else {
                    // Still waiting in the queue: re-queue it.
                    let task = Task::new(task_id, t.created_at, config);
                    match machine_map.find_machine_mut(machine_id) {
                        Ok(machine) => {
                            if machine.queue_task(&task).is_ok() {
                                woken.insert(machine_id);
                            } else {
                                tracing::warn!(
                                    %task_id,
                                    %machine_id,
                                    "failed to requeue task on restore (queue full)"
                                );
                            }
                        }
                        Err(_) => {
                            tracing::warn!(
                                %task_id,
                                %machine_id,
                                "task references unknown machine on restore, not requeued"
                            );
                        }
                    }
                    keep_restored_task(task_map, user_map, &t.user_name, task);
                }
            }

            for machine_id in &woken {
                if let Ok(machine) = machine_map.find_machine(*machine_id) {
                    machine.wake_launcher();
                }
            }
        }

        for (session_id, closed_at) in dead_sessions {
            close_dead_session(&pool, session_id, closed_at).await;
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

    pub(crate) fn notifiers(&self) -> &NotifierMap {
        &self.notifiers
    }

    pub(crate) fn notifiers_mut(&mut self) -> &mut NotifierMap {
        &mut self.notifiers
    }

    pub(crate) fn suspension_lock(&self) -> Arc<tokio::sync::Mutex<()>> {
        self.suspension_lock.clone()
    }

    pub(crate) fn admin_lock(&self) -> Arc<tokio::sync::Mutex<()>> {
        self.admin_lock.clone()
    }

    /// Starts the launcher of a machine already in the machine map.
    pub(crate) fn start_launcher(
        &mut self,
        machine_id: MachineId,
        backend_receiver: UnboundedReceiver<FromBackendMessage>,
        resume_tasks: Vec<ResumeTask>,
    ) {
        let core_ref = self.core_ref.as_ref().unwrap();
        let backend = self.machine_map.get_machine(machine_id).backend().clone();
        let (stop, stop_receiver) = watch::channel(false);
        let join = start_launcher(
            core_ref,
            machine_id,
            backend,
            backend_receiver,
            resume_tasks,
            stop_receiver,
        );
        self.launchers
            .insert(machine_id, LauncherHandle { stop, join });
    }

    /// Signals a machine's launcher to stop and returns its handle so the caller can await it
    /// (outside the core lock, which the launcher needs to finish).
    pub(crate) fn stop_launcher(&mut self, machine_id: MachineId) -> Option<JoinHandle<()>> {
        let handle = self.launchers.remove(&machine_id)?;
        handle.stop.send_replace(true);
        Some(handle.join)
    }

    /// Signals all launchers to stop and returns their handles so the caller can await them
    /// (outside the core lock, which the launchers need to finish).
    pub(crate) fn begin_shutdown(&mut self) -> Vec<JoinHandle<()>> {
        let machine_ids: Vec<MachineId> = self.launchers.keys().copied().collect();
        machine_ids
            .into_iter()
            .filter_map(|machine_id| self.stop_launcher(machine_id))
            .collect()
    }

    /// Removes a machine (whose launcher is stopped) together with all its tasks and sessions
    /// from memory. Returns the removed machine.
    pub(crate) fn purge_machine(&mut self, machine_id: MachineId) -> Option<Machine> {
        let user_map = &mut self.user_map;
        self.task_map.retain(|t| {
            let keep = t.config().machine_id != machine_id;
            if !keep && !t.state().is_final() {
                user_map.task_deactivated(t.config().user_id, t.config().payload.len() as u64);
            }
            keep
        });
        self.session_map.retain(|s| {
            let keep = s.config.machine_id != machine_id;
            if !keep && !matches!(s.state, SessionState::Closed { .. }) {
                user_map.session_deactivated(s.config.owner_id);
            }
            keep
        });
        self.machine_map.remove(machine_id)
    }

    /// Creates a machine (with its backend) and adds it to the machine map; its launcher is
    /// not started yet.
    pub(crate) fn insert_machine(
        &mut self,
        machine_id: MachineId,
        config: MachineConfiguration,
    ) -> UnboundedReceiver<FromBackendMessage> {
        let (machine, backend_receiver) = create_machine(machine_id, config);
        self.machine_map.insert(machine);
        backend_receiver
    }

    #[inline]
    pub(crate) fn split(&self) -> CoreSplit<'_> {
        CoreSplit {
            machine_map: &self.machine_map,
            task_map: &self.task_map,
            session_map: &self.session_map,
            project_map: &self.project_map,
            user_map: &self.user_map,
            notifiers: &self.notifiers,
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
            user_map: &mut self.user_map,
            notifiers: &mut self.notifiers,
            core_ref: self.core_ref.as_ref().unwrap(),
        }
    }

    pub(crate) fn validate_session_config(&self, config: &SessionConfig) -> crate::Result<()> {
        self.check_session_params(config.machine_id, config.project_id, config.time_limit)?;
        self.check_session_owner_limit(config.owner_id)
    }

    /// Like `validate_session_config`, for an owner named `owner`, who may not be known yet.
    pub(crate) fn check_new_session(
        &self,
        owner: &str,
        machine_id: MachineId,
        project_id: ProjectId,
        time_limit: Duration,
    ) -> crate::Result<()> {
        self.check_session_params(machine_id, project_id, time_limit)?;
        self.check_item_limit(owner, self.user_map.find_by_name(owner))
    }

    fn check_session_params(
        &self,
        machine_id: MachineId,
        project_id: ProjectId,
        time_limit: Duration,
    ) -> crate::Result<()> {
        let machine = self.machine_map.find_machine(machine_id)?;
        machine.check_not_suspended()?;
        let limit = machine.config().max_session_time;
        if time_limit > limit {
            return Err(RunnerError::SessionDurationExceedsLimit {
                machine: machine.config().name.clone(),
                limit,
                requested: time_limit,
            });
        }
        let project = self
            .project_map
            .find_project(project_id)
            .ok_or_else(|| RunnerError::ProjectNotFound(project_id.to_string()))?;
        if !project.active {
            return Err(RunnerError::ProjectNotActive(project.name.clone()));
        }
        if !project.has_time_for(time_limit) {
            return Err(RunnerError::ProjectLimitExceeded(project.name.clone()));
        }
        Ok(())
    }

    /// Fails if the user can't own another session. A user that isn't loaded passes here;
    /// `add_session` reports it.
    fn check_session_owner_limit(&self, owner_id: UserId) -> crate::Result<()> {
        match self.user_map.find_user(owner_id) {
            Some(user) => self.check_item_limit(&user.name, Some(user)),
            None => Ok(()),
        }
    }

    pub(crate) fn validate_task_config(&self, config: &TaskConfig) -> crate::Result<()> {
        let machine = self.machine_map.find_machine(config.machine_id)?;
        machine.check_not_suspended()?;
        if config.kind == TaskKind::Pulse && !machine.config().pulse_access {
            return Err(RunnerError::PulseAccessDenied(
                machine.config().name.clone(),
            ));
        }
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
        self.check_user_limits(config.user_id, config.payload.len() as u64)
    }

    /// Fails if the user can't have another active item with a payload of `payload_size` bytes.
    /// A user that isn't loaded passes here; `add_task` reports it.
    fn check_user_limits(&self, user_id: UserId, payload_size: u64) -> crate::Result<()> {
        match self.user_map.find_user(user_id) {
            Some(user) => self.check_limits_of(&user.name, Some(user), payload_size),
            None => Ok(()),
        }
    }

    /// Like `check_user_limits`, for an upload of the user named `name`, which may not be known
    /// yet: a user that isn't loaded has no active items.
    pub(crate) fn check_user_upload(&self, name: &str, payload_size: u64) -> crate::Result<()> {
        self.check_limits_of(name, self.user_map.find_by_name(name), payload_size)
    }

    fn check_limits_of(
        &self,
        name: &str,
        user: Option<&User>,
        payload_size: u64,
    ) -> crate::Result<()> {
        let active = user.map_or(0, |u| u.active_payload_bytes);
        if active.saturating_add(payload_size) > self.max_user_active_payload {
            return Err(RunnerError::UserPayloadLimitExceeded {
                user: name.to_string(),
                requested: payload_size,
                active,
                limit: self.max_user_active_payload,
            });
        }
        self.check_item_limit(name, user)
    }

    fn check_item_limit(&self, name: &str, user: Option<&User>) -> crate::Result<()> {
        if user.is_some_and(|u| u.active_items >= self.max_user_active_items) {
            return Err(RunnerError::UserActiveItemLimitExceeded {
                user: name.to_string(),
                limit: self.max_user_active_items,
            });
        }
        Ok(())
    }

    pub(crate) fn add_task(
        &mut self,
        task_id: TaskId,
        created_at: DateTime<Utc>,
        config: TaskConfig,
    ) -> crate::Result<TaskId> {
        // Re-checked: concurrent submits of the user may have been added while the task row was
        // inserted.
        self.check_user_limits(config.user_id, config.payload.len() as u64)?;
        let CoreSplitMut {
            machine_map,
            task_map,
            user_map,
            ..
        } = self.split_mut();
        let machine = machine_map.find_machine_mut(config.machine_id)?;
        // Re-checked: the machine may have been suspended while the task row was inserted.
        machine.check_not_suspended()?;
        // Fails only if the user was evicted while the task row was inserted, which takes far
        // less than the eviction's idle period.
        if user_map.find_user(config.user_id).is_none() {
            return Err(RunnerError::GenericError(format!(
                "user {} is not loaded",
                config.user_id
            )));
        }
        let task = Task::new(task_id, created_at, config);
        machine.queue_task(&task)?;
        user_map.task_activated(task.config().user_id, task.config().payload.len() as u64)?;
        task_map.insert(task);
        machine.wake_launcher();
        Ok(task_id)
    }

    pub(crate) fn add_session(
        &mut self,
        session_id: SessionId,
        created_at: DateTime<Utc>,
        config: SessionConfig,
    ) -> crate::Result<SessionId> {
        // Re-checked: concurrent requests of the owner may have been added while the session row
        // was inserted.
        self.check_session_owner_limit(config.owner_id)?;
        let CoreSplitMut {
            machine_map,
            session_map,
            user_map,
            ..
        } = self.split_mut();
        let machine = machine_map.find_machine_mut(config.machine_id)?;
        // Re-checked: the machine may have been suspended while the session row was inserted.
        machine.check_not_suspended()?;
        // Fails only if the owner was evicted while the session row was inserted, which takes far
        // less than the eviction's idle period.
        if user_map.find_user(config.owner_id).is_none() {
            return Err(RunnerError::GenericError(format!(
                "user {} is not loaded",
                config.owner_id
            )));
        }
        let session = Session::new(session_id, created_at, config);
        machine.queue_session(&session)?;
        keep_active_session(session_map, user_map, session);
        machine.wake_launcher();
        Ok(session_id)
    }

    pub fn task_info(&self, task_id: TaskId) -> Option<TaskInfo> {
        let task = self.task_map.find_task(task_id)?;
        let machine = self
            .machine_map
            .get_machine(task.config().machine_id)
            .config()
            .name
            .clone();
        let project = task
            .config()
            .parent
            .project_id()
            .and_then(|pid| self.project_map.find_project(pid))
            .map(|p| p.name.clone());
        Some(TaskInfo::build(
            task,
            task.state(),
            machine,
            project,
            &self.user_map,
            &self.notifiers,
        ))
    }

    pub fn session_info(&self, session_id: SessionId) -> Option<SessionInfo> {
        let session = self.session_map.find_session(session_id)?;
        let owner = self.user_map.get_user(session.config.owner_id).name.clone();
        let machine = self
            .machine_map
            .get_machine(session.config.machine_id)
            .config()
            .name
            .clone();
        let project = self
            .project_map
            .find_project(session.config.project_id)
            .map(|p| p.name.clone())
            .unwrap_or_default();
        Some(SessionInfo::build(
            session,
            machine,
            project,
            owner,
            &self.notifiers,
        ))
    }

    pub fn get_arch(&self, machine_id: MachineId) -> crate::Result<BackendFuture<String>> {
        let machine = self.machine_map.find_machine(machine_id)?;
        Ok(Arc::clone(machine.backend()).get_arch())
    }

    pub fn get_about(
        &self,
        machine_id: MachineId,
    ) -> crate::Result<(&'static str, BackendFuture<BackendVersion>)> {
        let machine = self.machine_map.find_machine(machine_id)?;
        let backend = Arc::clone(machine.backend());
        let name = backend.get_name();
        let version = backend.get_version();
        Ok((name, version))
    }

    pub fn get_calibration(
        &self,
        machine_id: MachineId,
        calibration_id: &str,
        endpoint: Option<&str>,
    ) -> crate::Result<BackendFuture<String>> {
        let machine = self.machine_map.find_machine(machine_id)?;
        Ok(Arc::clone(machine.backend()).get_calibration(calibration_id, endpoint))
    }

    /// Fetches one of [`QC_ARTIFACTS`] from the machine's backend; other names fail with
    /// [`RunnerError::UnknownArtifact`], so the proxy can't reach arbitrary backend resources.
    pub fn get_qc_artifact(
        &self,
        machine_id: MachineId,
        name: &str,
        accept: Option<&str>,
    ) -> crate::Result<BackendFuture<BackendStream>> {
        let machine = self.machine_map.find_machine(machine_id)?;
        if !QC_ARTIFACTS.contains(&name) {
            return Err(RunnerError::UnknownArtifact(name.to_string()));
        }
        Ok(Arc::clone(machine.backend()).get_qc_artifact(name, accept))
    }

    /// The machine and backend job id of a task resident in memory.
    pub(crate) fn task_backend_ref(&self, task_id: TaskId) -> Option<(MachineId, Option<String>)> {
        self.task_map
            .find_task(task_id)
            .map(|t| (t.config().machine_id, t.backend_id().map(String::from)))
    }

    pub(crate) fn get_task_result(
        &self,
        machine_id: MachineId,
        backend_id: &str,
    ) -> crate::Result<BackendFuture<BackendStream>> {
        let machine = self.machine_map.find_machine(machine_id)?;
        Ok(Arc::clone(machine.backend()).get_task_result(backend_id))
    }

    pub(crate) fn get_task_artifact(
        &self,
        machine_id: MachineId,
        backend_id: &str,
        name: &str,
        accept: Option<&str>,
    ) -> crate::Result<BackendFuture<BackendStream>> {
        let machine = self.machine_map.find_machine(machine_id)?;
        Ok(Arc::clone(machine.backend()).get_task_artifact(backend_id, name, accept))
    }
}

/// Creates a machine and its backend from a stored configuration.
/// Keeps a session that isn't closed in memory and counts it as its owner's active item; the
/// owner has to be loaded. As with `keep_restored_task`, doing both in one place keeps the count
/// matching what can release it.
fn keep_active_session(session_map: &mut SessionMap, user_map: &mut UserMap, session: Session) {
    user_map
        .session_activated(session.config.owner_id)
        .expect("owner is loaded");
    session_map.insert(session);
}

/// Keeps a restored task in memory and counts it as its user's active item. Doing both in one
/// place keeps the user's counters matching the tasks that can ever release them: a task that
/// isn't kept must not be counted, as nothing would uncount it.
fn keep_restored_task(task_map: &mut TaskMap, user_map: &mut UserMap, user_name: &str, task: Task) {
    let config = task.config();
    user_map.insert(config.user_id, user_name);
    user_map
        .task_activated(config.user_id, config.payload.len() as u64)
        .expect("user was just inserted");
    task_map.insert(task);
}

fn create_machine(
    machine_id: MachineId,
    m: MachineConfiguration,
) -> (Machine, UnboundedReceiver<FromBackendMessage>) {
    let (backend, backend_receiver) = create_backend(&m.backend);
    let machine = Machine::new(
        machine_id,
        MachineConfig {
            name: m.name,
            queue_size: m.queue_size,
            decay_half_time: Duration::from_millis(m.decay_half_time_ms),
            limit_check_interval: Duration::from_millis(m.limit_check_interval_ms as u64),
            max_session_time: Duration::from_millis(m.max_session_time_ms),
            default_task_time_limit: Duration::from_millis(m.default_task_time_limit_ms),
            max_task_time_limit: Duration::from_millis(m.max_task_time_limit_ms),
            max_clock_skew: Duration::from_millis(m.max_clock_skew_ms),
            pulse_access: m.pulse_access,
            backend: m.backend,
        },
        backend,
    );
    (machine, backend_receiver)
}

/// Periodically drops terminal tasks and closed sessions older than `retention` from memory;
/// `GET /tasks/{id}` and friends then fall back to the DB. Users idle for `retention` are dropped as well; they
/// are reloaded from the DB when they submit again.
fn start_eviction(core_ref: &CoreRef, retention: Duration) {
    let core_ref = Arc::downgrade(core_ref);
    let period = (retention / 2).clamp(Duration::from_secs(1), Duration::from_secs(60));
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            let Some(core_ref) = core_ref.upgrade() else {
                break;
            };
            let mut core = core_ref.lock().unwrap();
            // One `now` for all, so no user of a task or a session still in memory is evicted.
            let now = Instant::now();
            let evicted = core.task_map.evict_finished(now, retention);
            if evicted > 0 {
                tracing::debug!(evicted, "Evicted finished tasks from memory");
            }
            let evicted = core.session_map.evict_closed(now, retention);
            if evicted > 0 {
                tracing::debug!(evicted, "Evicted closed sessions from memory");
            }
            let evicted = core.user_map.evict_idle(now, retention);
            if evicted > 0 {
                tracing::debug!(evicted, "Evicted idle users from memory");
            }
        }
    });
}
