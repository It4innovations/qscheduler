use crate::backend::{Backend, FromBackendMessage};
use crate::callback::{Notifier, NotifierMap, NotifyEvent};
use crate::core::{Core, CoreRef, CoreSplitMut};
use crate::db;
use crate::machine::{Machine, MachineMap, QueueItem, ResumeTask};
use crate::project::{ProjectId, ProjectMap};
use crate::session::{Session, SessionInfo, SessionMap, SessionState};
use crate::suspension::SuspendCancel;
use crate::task::{CompletedState, Task, TaskInfo, TaskKind, TaskMap, TaskState};
use crate::{MachineId, SessionId, TaskId};
use bytes::Bytes;
use chrono::{DateTime, SubsecRound, Utc};
use std::collections::HashMap;
use std::future::pending;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior, sleep_until};
use tracing::{debug, warn};

pub fn start_launcher(
    core_ref: &CoreRef,
    machine_id: MachineId,
    backend: Arc<dyn Backend>,
    backend_receiver: UnboundedReceiver<FromBackendMessage>,
    restore_tasks: Vec<ResumeTask>,
    notifiers: Arc<NotifierMap>,
    shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    let core_ref = core_ref.clone();
    tokio::spawn(async move {
        launcher_main(
            core_ref,
            machine_id,
            backend,
            backend_receiver,
            notifiers,
            restore_tasks,
            shutdown,
        )
        .await
    })
}

struct RunningSession {
    session_id: SessionId,
    project_id: ProjectId,
    deadline: Instant,
    opened_at: Instant,
    exec_time: Duration,
}

/// Error recorded on a task cancelled because it exceeded its time limit.
const TASK_TIME_LIMIT_ERROR: &str = "Task time limit exceeded";
/// Error recorded on a task failed because its project ran out of time.
const PROJECT_LIMIT_ERROR: &str = "Project time limit exceeded";

/// A task the backend reported as executing, tracked until its terminal state.
///
/// Its running time is an estimate that must never exceed the true time (so a task is never
/// failed early), kept as "at least `elapsed` had passed at local `elapsed_at`" and advanced by
/// our monotonic clock; wall-clock time is only used once, to map the backend's start time.
struct RunningTask {
    elapsed: Duration,
    elapsed_at: Instant,
    time_limit: Duration,
    /// Project charged for the task's time; `None` for session tasks (charged via the session).
    project_id: Option<ProjectId>,
    /// Estimate already added to the project's `in_flight`. Never persisted or reported: on
    /// completion it's replaced by the backend's exact exec time (see `settle_in_flight`).
    charged: Duration,
}

impl RunningTask {
    fn new(elapsed: Duration, time_limit: Duration, project_id: Option<ProjectId>) -> Self {
        RunningTask {
            elapsed,
            elapsed_at: Instant::now(),
            time_limit,
            project_id,
            charged: Duration::ZERO,
        }
    }

    /// Estimated running time now (never more than the true time).
    fn elapsed(&self) -> Duration {
        self.elapsed + self.elapsed_at.elapsed()
    }

    /// Raises the estimate to another lower bound of the running time, if it's larger. Taking
    /// the larger of two lower bounds keeps it a lower bound.
    fn raise_elapsed(&mut self, elapsed: Duration) {
        let current = self.elapsed();
        self.elapsed = current.max(elapsed);
        self.elapsed_at = Instant::now();
    }

    fn is_over_time_limit(&self) -> bool {
        self.elapsed() >= self.time_limit
    }
}

/// Lower bound of a task's running time from the backend's reported start, assuming the
/// backend's clock lags ours by at most `max_clock_skew`: time since `started_at + skew`.
fn elapsed_since_backend_start(started_at: DateTime<Utc>, max_clock_skew: Duration) -> Duration {
    let skew = chrono::Duration::from_std(max_clock_skew).unwrap_or(chrono::Duration::MAX);
    (Utc::now() - started_at)
        .checked_sub(&skew)
        .and_then(|d| d.to_std().ok())
        .unwrap_or(Duration::ZERO)
}

enum DbUpdate {
    TaskSubmitted {
        task_id: TaskId,
        backend_id: String,
    },
    TaskStarted {
        task_id: TaskId,
        timestamp: DateTime<Utc>,
    },
    TaskFinished {
        task_id: TaskId,
        completed: CompletedState,
    },
    TaskFailed {
        task_id: TaskId,
        completed: CompletedState,
        error: String,
    },
    TaskCancelled {
        task_id: TaskId,
        completed: CompletedState,
    },
    SessionOpened {
        session_id: SessionId,
        timestamp: DateTime<Utc>,
    },
    SessionClosed {
        session_id: SessionId,
        exec_time: Duration,
        timestamp: DateTime<Utc>,
        cancelled_tasks: Vec<TaskId>,
    },
    SessionExecTime {
        session_id: SessionId,
        exec_time_ms: i64,
    },
}

/// Resolves a project's name for embedding in a `TaskInfo`/`SessionInfo` notify payload.
fn project_name(project_map: &ProjectMap, project_id: ProjectId) -> Option<String> {
    project_map.find_project(project_id).map(|p| p.name.clone())
}

/// Sends a notify event, warning if the notify worker's channel has already closed instead of
/// silently dropping the event.
fn send_notify(notifier: &Notifier, event: NotifyEvent) {
    if notifier.sender.send(event).is_err() {
        warn!(notifier = %notifier.name, "failed to send notify event, notify worker channel closed");
    }
}

/// Sends the completion event of `task` (in `state`, which may be about to be applied) to the
/// task's notifier, if it has one.
fn notify_task(
    notifiers: &NotifierMap,
    task: &Task,
    state: &TaskState,
    machine_name: &str,
    project_map: &ProjectMap,
) {
    let Some(notifier) = notifiers.get(task.config().notifier) else {
        return;
    };
    let project = task
        .config()
        .parent
        .project_id()
        .and_then(|pid| project_name(project_map, pid));
    let info = TaskInfo::build(task, state, machine_name.to_string(), project, notifiers);
    send_notify(notifier, NotifyEvent::Task { task: info });
}

/// Sends the current state of `session` to the session's notifier, if it has one.
fn notify_session(
    notifiers: &NotifierMap,
    session: &Session,
    machine_name: &str,
    project_map: &ProjectMap,
) {
    let Some(notifier) = notifiers.get(session.config.notifier) else {
        return;
    };
    let project = project_name(project_map, session.config.project_id).unwrap_or_default();
    let info = SessionInfo::build(session, machine_name.to_string(), project, notifiers);
    send_notify(notifier, NotifyEvent::Session { session: info });
}

fn pick_task(
    core: &mut Core,
    machine_id: MachineId,
    running_session: &mut Option<RunningSession>,
    no_assignments: bool,
    notifiers: &NotifierMap,
    db_updates: &mut Vec<DbUpdate>,
) -> Option<(TaskId, TaskKind, Bytes)> {
    let CoreSplitMut {
        machine_map,
        task_map,
        session_map,
        project_map,
        ..
    } = core.split_mut();
    let machine = machine_map.get_machine_mut(machine_id);
    loop {
        if running_session.is_some() {
            if let Some(task_id) = machine.pop_session_task() {
                let Some(task) = task_map.find_task_mut(task_id) else {
                    continue;
                };
                return Some((task_id, task.config().kind, task.config().payload.clone()));
            } else {
                break None;
            }
        } else {
            match machine.pop_queue_item(no_assignments) {
                None => break None,
                Some(QueueItem::Task(task_id)) => {
                    let Some(task) = task_map.find_task_mut(task_id) else {
                        continue;
                    };
                    return Some((task_id, task.config().kind, task.config().payload.clone()));
                }
                Some(QueueItem::Session(session_id)) => {
                    let Some(session) = session_map.find_session_mut(session_id) else {
                        continue;
                    };
                    assert!(running_session.is_none());

                    let project_id = session.config.project_id;
                    let now = Instant::now();
                    session.state = SessionState::Open {
                        opened_at: Utc::now(),
                    };
                    machine.start_session(session_id);
                    *running_session = Some(RunningSession {
                        session_id,
                        project_id,
                        deadline: now + session.config.time_limit,
                        opened_at: now,
                        exec_time: Duration::ZERO,
                    });
                    notify_session(notifiers, session, &machine.config().name, project_map);
                    db_updates.push(DbUpdate::SessionOpened {
                        session_id,
                        timestamp: Utc::now(),
                    });
                    continue;
                }
            }
        }
    }
}

/// Marks a submitted task as cancelling and asks the backend to cancel it. A task whose
/// submission the backend hasn't confirmed yet (no `backend_id`) is cancelled once its
/// `TaskSubmitted` message arrives.
fn request_backend_cancel(
    task_map: &TaskMap,
    backend: &Arc<dyn Backend>,
    task_id: TaskId,
    cancelling: &mut bool,
) {
    if *cancelling {
        return;
    }
    *cancelling = true;
    if let Some(backend_id) = task_map.find_task(task_id).and_then(|t| t.backend_id()) {
        debug!(%task_id, "Cancelling submitted task");
        Arc::clone(backend).cancel_task(task_id, backend_id);
    } else {
        debug!(%task_id, "Cancelling not fully submitted task; will be cancelled later");
    }
}

/// Periodic check of submitted tasks: adds running tasks' estimated time to their projects'
/// `in_flight`, then cancels every task that exceeded its own time limit or whose project is
/// over its limit (`consumed + in_flight`). Project charging and the project limit apply only
/// to project-parented tasks; a session task is accounted through its session (see the session
/// branch of the periodic check).
/// The reason is remembered in `fail_reasons` so that the backend's cancellation confirmation
/// is recorded as a failure.
fn check_running_tasks(
    running_tasks: &mut HashMap<TaskId, RunningTask>,
    fail_reasons: &mut HashMap<TaskId, &'static str>,
    submitted_tasks: &mut HashMap<TaskId, bool>,
    task_map: &TaskMap,
    project_map: &mut ProjectMap,
    backend: &Arc<dyn Backend>,
) {
    for rt in running_tasks.values_mut() {
        if let Some(project_id) = rt.project_id {
            // Capped at the task's own limit: past it the task is being cancelled, and if the
            // backend can't confirm that (e.g. unreachable), the estimate mustn't grow without
            // bound. The exact exec time replaces it once the task's final state arrives.
            let elapsed = rt.elapsed().min(rt.time_limit);
            project_map
                .get_project_mut(project_id)
                .add_in_flight(elapsed.saturating_sub(rt.charged));
            rt.charged = rt.charged.max(elapsed);
        }
    }
    for (&task_id, cancelling) in submitted_tasks.iter_mut() {
        if *cancelling {
            continue;
        }
        let reason = if running_tasks
            .get(&task_id)
            .is_some_and(|rt| rt.is_over_time_limit())
        {
            TASK_TIME_LIMIT_ERROR
        } else if task_map
            .find_task(task_id)
            .and_then(|t| t.config().parent.project_id())
            .and_then(|pid| project_map.find_project(pid))
            .is_some_and(|p| p.is_over_limit())
        {
            PROJECT_LIMIT_ERROR
        } else {
            continue;
        };
        debug!(%task_id, reason, "Failing submitted task");
        fail_reasons.insert(task_id, reason);
        request_backend_cancel(task_map, backend, task_id, cancelling);
    }
}

/// Cancels every submitted task of `s`, cancels its remaining queued tasks, and frees the
/// machine. Shared by the deadline/over-limit close path and the on-demand session cancel path.
#[allow(clippy::too_many_arguments)]
fn close_running_session(
    s: &RunningSession,
    exec_time: Duration,
    task_map: &mut TaskMap,
    session_map: &mut SessionMap,
    machine_map: &mut MachineMap,
    project_map: &ProjectMap,
    machine_id: MachineId,
    backend: &Arc<dyn Backend>,
    submitted_tasks: &mut HashMap<TaskId, bool>,
    notifiers: &NotifierMap,
    db_updates: &mut Vec<DbUpdate>,
) {
    for (task_id, cancelling) in submitted_tasks.iter_mut() {
        request_backend_cancel(task_map, backend, *task_id, cancelling);
    }
    let session = session_map.get_session_mut(s.session_id);
    let now = Utc::now();
    session.state.close(now);
    session.consumed = exec_time;
    let machine = machine_map.get_machine_mut(machine_id);
    let mut cancelled_tasks = Vec::new();
    let now = Utc::now();
    while let Some(task_id) = machine.pop_session_task() {
        debug!(%task_id, "Cancelling unsubmitted task");
        let task = task_map.get_task_mut(task_id);
        task.set_state(TaskState::Cancelled {
            completed: CompletedState {
                consumed: Duration::ZERO,
                timestamp: now,
            },
        });
        notify_task(
            notifiers,
            task,
            task.state(),
            &machine.config().name,
            project_map,
        );
        cancelled_tasks.push(task_id);
    }
    machine.close_session();
    notify_session(notifiers, session, &machine.config().name, project_map);
    db_updates.push(DbUpdate::SessionClosed {
        session_id: s.session_id,
        exec_time,
        timestamp: now,
        cancelled_tasks,
    });
}

/// Closes a session that never opened, already removed from the machine's queue.
fn close_queued_session(
    session_id: SessionId,
    session_map: &mut SessionMap,
    machine: &Machine,
    project_map: &ProjectMap,
    notifiers: &NotifierMap,
    db_updates: &mut Vec<DbUpdate>,
) {
    let session = session_map.get_session_mut(session_id);
    let now = Utc::now();
    session.state.close(now);
    notify_session(notifiers, session, &machine.config().name, project_map);
    db_updates.push(DbUpdate::SessionClosed {
        session_id,
        exec_time: Duration::ZERO,
        timestamp: now,
        cancelled_tasks: Vec::new(),
    });
}

/// Everything needed to fail tasks with one error (a machine suspension's).
struct FailContext<'a> {
    task_map: &'a mut TaskMap,
    project_map: &'a mut ProjectMap,
    machine_name: String,
    notifiers: &'a NotifierMap,
    db_updates: &'a mut Vec<DbUpdate>,
    error: &'a str,
}

impl FailContext<'_> {
    /// Fails a task that was never handed to the backend (already removed from its queue).
    fn fail_queued_task(&mut self, task_id: TaskId) {
        debug!(%task_id, "Failing queued task");
        self.fail(task_id, CompletedState::new_zero_now(), Duration::ZERO);
    }

    /// Fails a task handed to the backend right away, without waiting for the backend to
    /// confirm its cancellation (it may be unreachable), and asks the backend to cancel it. The
    /// task stops being tracked, so the backend's later reports about it are ignored. A project
    /// task is charged the estimated running time (a lower bound of the true time).
    fn fail_submitted_task(
        &mut self,
        task_id: TaskId,
        backend: &Arc<dyn Backend>,
        submitted_tasks: &mut HashMap<TaskId, bool>,
        running_tasks: &mut HashMap<TaskId, RunningTask>,
        fail_reasons: &mut HashMap<TaskId, &'static str>,
    ) {
        debug!(%task_id, "Failing submitted task");
        let Some(cancelling) = submitted_tasks.remove(&task_id) else {
            return;
        };
        fail_reasons.remove(&task_id);
        let (consumed, charged) = running_tasks
            .remove(&task_id)
            .map(|rt| (rt.elapsed().min(rt.time_limit), rt.charged))
            .unwrap_or_default();
        // Without a backend id yet, the late `TaskSubmitted` message cancels the job.
        if !cancelling && let Some(backend_id) = self.task_map.get_task_mut(task_id).backend_id() {
            Arc::clone(backend).cancel_task(task_id, backend_id);
        }
        let completed = CompletedState {
            consumed,
            timestamp: Utc::now(),
        };
        self.fail(task_id, completed, charged);
    }

    fn fail(&mut self, task_id: TaskId, completed: CompletedState, charged: Duration) {
        let task = self.task_map.get_task_mut(task_id);
        task.set_state(TaskState::Failed {
            completed,
            error: self.error.to_string(),
        });
        let project_id = task.config().parent.project_id();
        if let Some(project_id) = project_id
            && (!completed.consumed.is_zero() || !charged.is_zero())
        {
            self.project_map
                .get_project_mut(project_id)
                .settle_in_flight(charged, completed.consumed);
        }
        notify_task(
            self.notifiers,
            task,
            task.state(),
            &self.machine_name,
            self.project_map,
        );
        self.db_updates.push(DbUpdate::TaskFailed {
            task_id,
            completed,
            error: self.error.to_string(),
        });
    }
}

enum LauncherEvent {
    Notified,
    SessionEnd,
    /// Periodic session quota checkpoint, task time limit and project limit check.
    PeriodicCheck,
    BackendMessage(FromBackendMessage),
    /// Service shutdown: run one last iteration that persists pending state, then exit.
    Shutdown,
}

#[tracing::instrument(skip_all, fields(%machine_id))]
async fn launcher_main(
    core_ref: CoreRef,
    machine_id: MachineId,
    backend: Arc<dyn Backend>,
    mut backend_receiver: UnboundedReceiver<FromBackendMessage>,
    notifiers: Arc<NotifierMap>,
    resume_tasks: Vec<ResumeTask>,
    mut shutdown: watch::Receiver<bool>,
) {
    debug!(%machine_id, "Starting launcher");
    let (notifier, queue_size, max_clock_skew, mut limit_check_interval) = {
        let core = core_ref.lock().unwrap();
        let machine = core.split().machine_map.get_machine(machine_id);
        let period = machine.config().limit_check_interval;
        // No immediate first tick: the first check comes one full period after start.
        let mut limit_check_interval = tokio::time::interval_at(Instant::now() + period, period);
        limit_check_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let queue_size = machine.config().queue_size;
        (
            machine.notifier().clone(),
            queue_size,
            machine.config().max_clock_skew,
            limit_check_interval,
        )
    };
    let pool = core_ref.lock().unwrap().pool().clone();
    assert!(queue_size > 0);
    let mut submitted_tasks: HashMap<TaskId, bool> = HashMap::new();
    // Tasks the backend reported as executing, until their terminal state.
    let mut running_tasks: HashMap<TaskId, RunningTask> = HashMap::new();
    // Tasks cancelled for exceeding a limit (with the error to record), awaiting the backend's
    // confirmation.
    let mut fail_reasons: HashMap<TaskId, &'static str> = HashMap::new();

    for rt in resume_tasks {
        debug!(task_id = %rt.task_id, "Re-attaching submitted task to backend");
        // resume_task re-registers the task with the backend before acting on `cancel`,
        // so cancel_task can't be called separately here: the backend wouldn't know the
        // task yet and would panic (or error) looking it up.
        backend
            .clone()
            .resume_task(rt.task_id, rt.backend_id, rt.payload, rt.cancel);
        submitted_tasks.insert(rt.task_id, rt.cancel);
        // Restored projects start with `in_flight` at zero (it's never persisted) and their
        // `consumed` from the DB excludes unfinished tasks, so the task starts uncharged here and
        // is charged exactly once by the next periodic check. The local observation time didn't
        // survive the restart, so the estimate comes from the backend's start alone.
        if !rt.cancel
            && let Some(started_at) = rt.started_at
        {
            running_tasks.insert(
                rt.task_id,
                RunningTask::new(
                    elapsed_since_backend_start(started_at, max_clock_skew),
                    rt.time_limit,
                    rt.project_id,
                ),
            );
        }
    }

    let mut running_session: Option<RunningSession> = None;
    let mut shutting_down = false;

    loop {
        let event = select! {
            // An error means the sender was dropped, which is a shutdown as well.
            _ = shutdown.changed(), if !shutting_down =>
                LauncherEvent::Shutdown,
            _ = notifier.notified() =>
                LauncherEvent::Notified,
            _ = async {
                if let Some(s) = &running_session {
                    sleep_until(s.deadline).await;
                } else {
                    pending().await
                }
            } =>
                LauncherEvent::SessionEnd,
            _ = async {
                if running_session.is_some() || !submitted_tasks.is_empty() {
                    limit_check_interval.tick().await;
                } else {
                    pending().await
                }
            } =>
                LauncherEvent::PeriodicCheck,
            msg = backend_receiver.recv() => {
                if msg.is_none() {
                    tracing::error!(%machine_id, "backend channel closed unexpectedly");
                }
                LauncherEvent::BackendMessage(msg.unwrap())
            }
        };

        let mut db_updates: Vec<DbUpdate> = Vec::new();
        {
            let mut core = core_ref.lock().unwrap();
            let CoreSplitMut {
                task_map,
                session_map,
                machine_map,
                project_map,
                ..
            } = core.split_mut();
            match event {
                LauncherEvent::Notified => { /* Do nothing */ }
                LauncherEvent::Shutdown => {
                    debug!(%machine_id, "Launcher shutting down");
                    shutting_down = true;
                    // Checkpoint the running session so its time since the last periodic
                    // checkpoint is not lost; restore closes it at this checkpoint.
                    if let Some(s) = running_session.as_mut() {
                        let exec_time = Instant::now() - s.opened_at;
                        let delta = exec_time - s.exec_time;
                        s.exec_time = exec_time;
                        project_map
                            .get_project_mut(s.project_id)
                            .update_consumed(delta);
                        session_map.get_session_mut(s.session_id).consumed = exec_time;
                        db_updates.push(DbUpdate::SessionExecTime {
                            session_id: s.session_id,
                            exec_time_ms: exec_time.as_millis() as i64,
                        });
                    }
                }
                LauncherEvent::SessionEnd | LauncherEvent::PeriodicCheck => {
                    if let Some(s) = running_session.as_mut() {
                        let exec_time = Instant::now() - s.opened_at;
                        let delta = exec_time - s.exec_time;
                        s.exec_time = exec_time;
                        let project = project_map.get_project_mut(s.project_id);
                        project.update_consumed(delta);
                        if matches!(event, LauncherEvent::PeriodicCheck) && !project.is_over_limit()
                        {
                            debug!(session_id = %s.session_id, delta_ms=delta.as_millis(), "Session check passed");
                            session_map.get_session_mut(s.session_id).consumed = exec_time;
                            db_updates.push(DbUpdate::SessionExecTime {
                                session_id: s.session_id,
                                exec_time_ms: exec_time.as_millis() as i64,
                            });
                        } else {
                            debug!(session_id=%s.session_id, "Session overtime/end of session");
                            close_running_session(
                                s,
                                exec_time,
                                task_map,
                                session_map,
                                machine_map,
                                project_map,
                                machine_id,
                                &backend,
                                &mut submitted_tasks,
                                &notifiers,
                                &mut db_updates,
                            );
                            running_session = None;
                        }
                    }
                    if matches!(event, LauncherEvent::PeriodicCheck) {
                        check_running_tasks(
                            &mut running_tasks,
                            &mut fail_reasons,
                            &mut submitted_tasks,
                            task_map,
                            project_map,
                            &backend,
                        );
                    }
                }
                LauncherEvent::BackendMessage(msg) => match msg {
                    FromBackendMessage::TaskSubmitted {
                        task_id,
                        backend_task_id,
                    } => {
                        if submitted_tasks.get(&task_id).copied() == Some(false) {
                            let task = task_map.get_task_mut(task_id);
                            task.set_backend_id(backend_task_id.clone());
                            db_updates.push(DbUpdate::TaskSubmitted {
                                task_id,
                                backend_id: backend_task_id,
                            });
                        } else {
                            // Either not in map (already finished/cancelled) or cancelling=true
                            // (session ended before backend confirmed submission)
                            debug!(%task_id, "Cancelling task on late backend submission");
                            Arc::clone(&backend).cancel_task(task_id, &backend_task_id);
                        }
                    }
                    FromBackendMessage::ExecutionStarted {
                        task_id,
                        started_at,
                        observed_at,
                    } => {
                        if let Some(&cancelling) = submitted_tasks.get(&task_id) {
                            let task = task_map.get_task_mut(task_id);
                            // Truncated to the DB's precision so the value is stable across a
                            // restart.
                            let started_at = started_at.trunc_subsecs(6);
                            // The larger of two lower bounds of the running time: since our own
                            // observation (the execution started before it), and since the
                            // backend's start shifted by the maximum clock skew.
                            let elapsed = observed_at
                                .elapsed()
                                .max(elapsed_since_backend_start(started_at, max_clock_skew));
                            match running_tasks.get_mut(&task_id) {
                                // Already tracked (e.g. restored, then re-reported): only ever
                                // raised, so it's never reset or charged twice.
                                Some(rt) => rt.raise_elapsed(elapsed),
                                None if !cancelling => {
                                    running_tasks.insert(
                                        task_id,
                                        RunningTask::new(
                                            elapsed,
                                            task.config().time_limit,
                                            task.config().parent.project_id(),
                                        ),
                                    );
                                }
                                None => {}
                            }
                            if task.started_at().is_none() && !cancelling {
                                task.set_started_at(started_at);
                                db_updates.push(DbUpdate::TaskStarted {
                                    task_id,
                                    timestamp: started_at,
                                });
                            }
                        }
                    }
                    FromBackendMessage::TaskStateChange { task_id, .. }
                        if !submitted_tasks.contains_key(&task_id) =>
                    {
                        // Already terminal on our side (failed by a machine suspension, or a
                        // duplicate report); the backend's late view of it is irrelevant.
                        debug!(%task_id, "Ignoring state change of no longer tracked task");
                    }
                    FromBackendMessage::TaskStateChange { task_id, mut state } => {
                        // In-flight time charged to the project so far; replaced by the exact
                        // exec time below.
                        let mut charged = Duration::ZERO;
                        if state.is_final() {
                            if let Some(rt) = running_tasks.remove(&task_id) {
                                charged = rt.charged;
                            }
                            if let Some(reason) = fail_reasons.remove(&task_id)
                                && let TaskState::Cancelled { completed } = state
                            {
                                state = TaskState::Failed {
                                    completed,
                                    error: reason.to_string(),
                                };
                            }
                        }
                        let mut update_state = true;
                        let exec_time = match &state {
                            TaskState::Waiting => unreachable!(),
                            TaskState::Running => Duration::ZERO,
                            TaskState::Finished { completed } => {
                                assert!(submitted_tasks.remove(&task_id).is_some());
                                notify_task(
                                    &notifiers,
                                    task_map.find_task(task_id).unwrap(),
                                    &state,
                                    &machine_map.get_machine(machine_id).config().name,
                                    project_map,
                                );
                                db_updates.push(DbUpdate::TaskFinished {
                                    task_id,
                                    completed: *completed,
                                });
                                completed.consumed
                            }
                            TaskState::Failed { completed, error } => {
                                assert!(submitted_tasks.remove(&task_id).is_some());
                                notify_task(
                                    &notifiers,
                                    task_map.find_task(task_id).unwrap(),
                                    &state,
                                    &machine_map.get_machine(machine_id).config().name,
                                    project_map,
                                );
                                db_updates.push(DbUpdate::TaskFailed {
                                    task_id,
                                    completed: *completed,
                                    error: error.clone(),
                                });
                                completed.consumed
                            }
                            TaskState::Cancelled { completed } => {
                                if submitted_tasks.remove(&task_id).is_none() {
                                    debug!(%task_id, "Ignoring cancel of already-finished task");
                                    update_state = false;
                                } else {
                                    notify_task(
                                        &notifiers,
                                        task_map.find_task(task_id).unwrap(),
                                        &state,
                                        &machine_map.get_machine(machine_id).config().name,
                                        project_map,
                                    );
                                    db_updates.push(DbUpdate::TaskCancelled {
                                        task_id,
                                        completed: *completed,
                                    });
                                }
                                completed.consumed
                            }
                        };
                        if update_state {
                            let task = task_map.get_task_mut(task_id);
                            task.set_state(state);
                            if (!exec_time.is_zero() || !charged.is_zero())
                                && let Some(project_id) = task.config().parent.project_id()
                            {
                                project_map
                                    .get_project_mut(project_id)
                                    .settle_in_flight(charged, exec_time);
                            }
                        }
                    }
                },
            }

            let machine = machine_map.get_machine_mut(machine_id);
            let session_cancels = machine.take_session_cancel_requests();
            let task_cancels = machine.take_task_cancel_requests();

            for session_id in session_cancels {
                if running_session.as_ref().map(|s| s.session_id) == Some(session_id) {
                    let s = running_session.take().unwrap();
                    let exec_time = Instant::now() - s.opened_at;
                    let delta = exec_time - s.exec_time;
                    project_map
                        .get_project_mut(s.project_id)
                        .update_consumed(delta);
                    debug!(%session_id, "Cancelling running session on request");
                    close_running_session(
                        &s,
                        exec_time,
                        task_map,
                        session_map,
                        machine_map,
                        project_map,
                        machine_id,
                        &backend,
                        &mut submitted_tasks,
                        &notifiers,
                        &mut db_updates,
                    );
                } else {
                    let machine = machine_map.get_machine_mut(machine_id);
                    if machine.remove_queued_session(session_id) {
                        debug!(%session_id, "Cancelling queued session on request");
                        close_queued_session(
                            session_id,
                            session_map,
                            machine,
                            project_map,
                            &notifiers,
                            &mut db_updates,
                        );
                    }
                }
            }

            for task_id in task_cancels {
                if let Some(cancelling) = submitted_tasks.get_mut(&task_id) {
                    debug!(%task_id, "Task cancel requested");
                    request_backend_cancel(task_map, &backend, task_id, cancelling);
                } else {
                    let machine = machine_map.get_machine_mut(machine_id);
                    if machine.remove_queued_task(task_id) {
                        debug!(%task_id, "Cancelling queued task on request");
                        let task = task_map.get_task_mut(task_id);
                        task.set_state(TaskState::simple_cancelled());
                        notify_task(
                            &notifiers,
                            task,
                            task.state(),
                            &machine.config().name,
                            project_map,
                        );
                        db_updates.push(DbUpdate::TaskCancelled {
                            task_id,
                            completed: CompletedState::new_zero_now(),
                        });
                    }
                }
            }

            let suspend_cancels = machine_map
                .get_machine_mut(machine_id)
                .take_suspend_cancel_requests();
            for (cancel, error) in suspend_cancels {
                debug!(?cancel, "Applying machine suspension to existing work");
                let mut ctx = FailContext {
                    task_map,
                    project_map,
                    machine_name: machine_map.get_machine(machine_id).config().name.clone(),
                    notifiers: &notifiers,
                    db_updates: &mut db_updates,
                    error: &error,
                };
                match cancel {
                    SuspendCancel::None => {}
                    SuspendCancel::Waiting | SuspendCancel::All => {
                        let machine = machine_map.get_machine_mut(machine_id);
                        for task_id in machine.take_queued_tasks() {
                            ctx.fail_queued_task(task_id);
                        }
                        let queued_sessions = machine.take_queued_sessions();
                        for session_id in queued_sessions {
                            close_queued_session(
                                session_id,
                                session_map,
                                machine_map.get_machine(machine_id),
                                ctx.project_map,
                                &notifiers,
                                ctx.db_updates,
                            );
                        }
                        if let Some(s) = running_session.take() {
                            let machine = machine_map.get_machine_mut(machine_id);
                            while let Some(task_id) = machine.pop_session_task() {
                                ctx.fail_queued_task(task_id);
                            }
                            // Its submitted tasks fail as well: closing the session would cancel
                            // them anyway.
                            let session_tasks: Vec<TaskId> = submitted_tasks
                                .keys()
                                .copied()
                                .filter(|t| {
                                    ctx.task_map.find_task(*t).is_some_and(|t| {
                                        t.config().parent.session_id() == Some(s.session_id)
                                    })
                                })
                                .collect();
                            for task_id in session_tasks {
                                ctx.fail_submitted_task(
                                    task_id,
                                    &backend,
                                    &mut submitted_tasks,
                                    &mut running_tasks,
                                    &mut fail_reasons,
                                );
                            }
                            let exec_time = Instant::now() - s.opened_at;
                            let delta = exec_time - s.exec_time;
                            ctx.project_map
                                .get_project_mut(s.project_id)
                                .update_consumed(delta);
                            debug!(session_id = %s.session_id, "Closing running session on machine suspension");
                            close_running_session(
                                &s,
                                exec_time,
                                ctx.task_map,
                                session_map,
                                machine_map,
                                ctx.project_map,
                                machine_id,
                                &backend,
                                &mut submitted_tasks,
                                &notifiers,
                                ctx.db_updates,
                            );
                        }
                        if cancel == SuspendCancel::All {
                            let task_ids: Vec<TaskId> = submitted_tasks.keys().copied().collect();
                            for task_id in task_ids {
                                ctx.fail_submitted_task(
                                    task_id,
                                    &backend,
                                    &mut submitted_tasks,
                                    &mut running_tasks,
                                    &mut fail_reasons,
                                );
                            }
                        }
                    }
                }
            }

            // No new submissions while shutting down: the backend id of a task submitted now
            // would not be recorded before exit.
            while !shutting_down
                && submitted_tasks.len() < queue_size
                && let Some((task_id, kind, payload)) = pick_task(
                    &mut core,
                    machine_id,
                    &mut running_session,
                    submitted_tasks.is_empty(),
                    &notifiers,
                    &mut db_updates,
                )
            {
                let project_limit_exceeded = {
                    let split = core.split();
                    split
                        .task_map
                        .find_task(task_id)
                        .and_then(|t| t.config().parent.project_id())
                        .and_then(|pid| split.project_map.find_project(pid))
                        .map(|p| p.is_over_limit())
                        .unwrap_or(false)
                };
                if project_limit_exceeded {
                    let error = PROJECT_LIMIT_ERROR.to_string();
                    let state = TaskState::simple_error(error.clone());
                    let completed = *state.completed().unwrap();
                    let split = core.split();
                    notify_task(
                        &notifiers,
                        split.task_map.find_task(task_id).unwrap(),
                        &state,
                        &split.machine_map.get_machine(machine_id).config().name,
                        split.project_map,
                    );
                    core.split_mut()
                        .task_map
                        .get_task_mut(task_id)
                        .set_state(state);
                    db_updates.push(DbUpdate::TaskFailed {
                        task_id,
                        completed,
                        error,
                    });
                } else {
                    backend.clone().submit_task(task_id, kind, payload);
                    submitted_tasks.insert(task_id, false);
                }
            }
        }

        for update in db_updates {
            match update {
                DbUpdate::TaskSubmitted {
                    task_id,
                    backend_id,
                } => db::update_task_backend_id(&pool, task_id, &backend_id).await,
                DbUpdate::TaskStarted { task_id, timestamp } => {
                    db::update_task_started(&pool, task_id, timestamp).await
                }
                DbUpdate::TaskFinished { task_id, completed } => {
                    db::update_task_finished(&pool, task_id, completed).await;
                }
                DbUpdate::TaskFailed {
                    task_id,
                    completed,
                    error,
                } => {
                    db::update_task_failed(&pool, task_id, completed, &error).await;
                }
                DbUpdate::TaskCancelled { task_id, completed } => {
                    db::update_task_cancelled(&pool, task_id, completed).await;
                }
                DbUpdate::SessionOpened {
                    session_id,
                    timestamp,
                } => db::update_session_opened(&pool, session_id, timestamp).await,
                DbUpdate::SessionClosed {
                    session_id,
                    exec_time,
                    timestamp,
                    cancelled_tasks,
                } => {
                    db::close_session_with_tasks(
                        &pool,
                        session_id,
                        exec_time,
                        timestamp,
                        &cancelled_tasks,
                    )
                    .await
                }
                DbUpdate::SessionExecTime {
                    session_id,
                    exec_time_ms,
                } => db::update_session_exec_time(&pool, session_id, exec_time_ms).await,
            }
        }

        if shutting_down {
            debug!(%machine_id, "Launcher stopped, pending DB updates flushed");
            break;
        }
    }
}
