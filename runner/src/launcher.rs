use crate::backend::{Backend, FromBackendMessage};
use crate::callback::{NotifyEvent, notify_worker};
use crate::core::{Core, CoreRef, CoreSplitMut};
use crate::db;
use crate::machine::{MachineConfig, MachineMap, QueueItem, ResumeTask};
use crate::project::{ProjectId, ProjectMap};
use crate::session::{SessionInfo, SessionMap, SessionState};
use crate::task::{CompletedState, TaskInfo, TaskMap, TaskState};
use crate::{MachineId, SessionId, TaskId};
use bytes::Bytes;
use chrono::{DateTime, SubsecRound, Utc};
use std::collections::{HashMap, HashSet};
use std::future::pending;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::{Instant, MissedTickBehavior, sleep_until};
use tracing::{debug, warn};

pub fn start_launcher(
    core_ref: &CoreRef,
    machine_id: MachineId,
    machine_config: &MachineConfig,
    backend: Arc<dyn Backend>,
    backend_receiver: UnboundedReceiver<FromBackendMessage>,
    restore_tasks: Vec<ResumeTask>,
) {
    let core_ref = core_ref.clone();
    let notify_sender = machine_config.notify.as_ref().map(|nc| {
        let notify_config = nc.clone();
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<NotifyEvent>();
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create tokio runtime for notifier")
                .block_on(notify_worker(receiver, notify_config));
        });
        sender
    });
    tokio::spawn(async move {
        launcher_main(
            core_ref,
            machine_id,
            backend,
            backend_receiver,
            notify_sender,
            restore_tasks,
        )
        .await
    });
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
fn send_notify(sender: &UnboundedSender<NotifyEvent>, event: NotifyEvent) {
    if sender.send(event).is_err() {
        warn!("failed to send notify event, notify worker channel closed");
    }
}

fn pick_task(
    core: &mut Core,
    machine_id: MachineId,
    running_session: &mut Option<RunningSession>,
    no_assignments: bool,
    notify_sender: &Option<UnboundedSender<NotifyEvent>>,
    db_updates: &mut Vec<DbUpdate>,
) -> Option<(TaskId, Bytes)> {
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
                return Some((task_id, task.config().payload.clone()));
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
                    return Some((task_id, task.config().payload.clone()));
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
                    if let Some(sender) = notify_sender {
                        let info = SessionInfo::build(
                            session,
                            machine.config().name.clone(),
                            project_name(project_map, project_id).unwrap_or_default(),
                        );
                        send_notify(sender, NotifyEvent::Session { session: info });
                    }
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

/// The instant a task started at `started_at` (wall clock, as reported by the backend)
/// exceeds `time_limit`; already-elapsed time counts against the limit.
fn time_limit_deadline(started_at: DateTime<Utc>, time_limit: Duration) -> Instant {
    let elapsed = (Utc::now() - started_at).to_std().unwrap_or(Duration::ZERO);
    Instant::now() + time_limit.saturating_sub(elapsed)
}

/// Cancels every running task whose time limit deadline has passed. The task is remembered in
/// `timed_out` so that the backend's cancellation confirmation is recorded as a failure.
fn check_task_deadlines(
    task_deadlines: &mut HashMap<TaskId, Instant>,
    timed_out: &mut HashSet<TaskId>,
    submitted_tasks: &mut HashMap<TaskId, bool>,
    task_map: &TaskMap,
    backend: &Arc<dyn Backend>,
) {
    let now = Instant::now();
    task_deadlines.retain(|&task_id, deadline| {
        if *deadline > now {
            return true;
        }
        if let Some(cancelling) = submitted_tasks.get_mut(&task_id)
            && !*cancelling
        {
            debug!(%task_id, "Task time limit exceeded");
            timed_out.insert(task_id);
            request_backend_cancel(task_map, backend, task_id, cancelling);
        }
        false
    });
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
    notify_sender: &Option<UnboundedSender<NotifyEvent>>,
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
        if let Some(sender) = notify_sender {
            let proj = task
                .config()
                .parent
                .project_id()
                .and_then(|pid| project_name(project_map, pid));
            let info = TaskInfo::build(task, task.state(), machine.config().name.clone(), proj);
            send_notify(sender, NotifyEvent::Task { task: info });
        }
        cancelled_tasks.push(task_id);
    }
    machine.close_session();
    if let Some(sender) = notify_sender {
        let proj = project_name(project_map, session.config.project_id).unwrap_or_default();
        let info = SessionInfo::build(session, machine.config().name.clone(), proj);
        send_notify(sender, NotifyEvent::Session { session: info });
    }
    db_updates.push(DbUpdate::SessionClosed {
        session_id: s.session_id,
        exec_time,
        timestamp: now,
        cancelled_tasks,
    });
}

enum LauncherEvent {
    Notified,
    SessionEnd,
    /// Periodic session quota checkpoint and task time limit check.
    PeriodicCheck,
    BackendMessage(FromBackendMessage),
}

#[tracing::instrument(skip_all, fields(%machine_id))]
async fn launcher_main(
    core_ref: CoreRef,
    machine_id: MachineId,
    backend: Arc<dyn Backend>,
    mut backend_receiver: UnboundedReceiver<FromBackendMessage>,
    notify_sender: Option<UnboundedSender<NotifyEvent>>,
    resume_tasks: Vec<ResumeTask>,
) {
    debug!(%machine_id, "Starting launcher");
    let (notifier, queue_size, mut limit_check_interval) = {
        let core = core_ref.lock().unwrap();
        let machine = core.split().machine_map.get_machine(machine_id);
        let mut limit_check_interval = tokio::time::interval(machine.config().limit_check_interval);
        limit_check_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let queue_size = machine.config().queue_size;
        (machine.notifier().clone(), queue_size, limit_check_interval)
    };
    let pool = core_ref.lock().unwrap().pool().clone();
    assert!(queue_size > 0);
    let mut submitted_tasks: HashMap<TaskId, bool> = HashMap::new();
    // Time limit deadlines of tasks the backend reported as running.
    let mut task_deadlines: HashMap<TaskId, Instant> = HashMap::new();
    // Tasks cancelled for exceeding their time limit, awaiting the backend's confirmation.
    let mut timed_out: HashSet<TaskId> = HashSet::new();

    for rt in resume_tasks {
        debug!(task_id = %rt.task_id, "Re-attaching submitted task to backend");
        // resume_task re-registers the task with the backend before acting on `cancel`,
        // so cancel_task can't be called separately here: the backend wouldn't know the
        // task yet and would panic (or error) looking it up.
        backend
            .clone()
            .resume_task(rt.task_id, rt.backend_id, rt.payload, rt.cancel);
        submitted_tasks.insert(rt.task_id, rt.cancel);
        if !rt.cancel
            && let Some(started_at) = rt.started_at
        {
            task_deadlines.insert(rt.task_id, time_limit_deadline(started_at, rt.time_limit));
        }
    }

    let mut running_session: Option<RunningSession> = None;

    loop {
        let event = select! {
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
                if running_session.is_some() || !task_deadlines.is_empty() {
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
                                &notify_sender,
                                &mut db_updates,
                            );
                            running_session = None;
                        }
                    }
                    if matches!(event, LauncherEvent::PeriodicCheck) {
                        check_task_deadlines(
                            &mut task_deadlines,
                            &mut timed_out,
                            &mut submitted_tasks,
                            task_map,
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
                    } => {
                        // Start the time limit clock on the first start report of a task that
                        // isn't being cancelled. A task restored with a persisted `started_at`
                        // already has its deadline, so a repeated report is ignored.
                        if submitted_tasks.get(&task_id) == Some(&false) {
                            let task = task_map.get_task_mut(task_id);
                            if task.started_at().is_none() {
                                // Truncated to the DB's precision so the value is stable across
                                // a restart.
                                let started_at = started_at.trunc_subsecs(6);
                                task.set_started_at(started_at);
                                task_deadlines.insert(
                                    task_id,
                                    time_limit_deadline(started_at, task.config().time_limit),
                                );
                                db_updates.push(DbUpdate::TaskStarted {
                                    task_id,
                                    timestamp: started_at,
                                });
                            }
                        }
                    }
                    FromBackendMessage::TaskStateChange { task_id, mut state } => {
                        if state.is_final() {
                            task_deadlines.remove(&task_id);
                            if timed_out.remove(&task_id)
                                && let TaskState::Cancelled { completed } = state
                            {
                                state = TaskState::Failed {
                                    completed,
                                    error: TASK_TIME_LIMIT_ERROR.to_string(),
                                };
                            }
                        }
                        let mut update_state = true;
                        let exec_time = match &state {
                            TaskState::Waiting => unreachable!(),
                            TaskState::Running => Duration::ZERO,
                            TaskState::Finished { completed } => {
                                assert!(submitted_tasks.remove(&task_id).is_some());
                                if let Some(sender) = &notify_sender {
                                    let task = task_map.find_task(task_id).unwrap();
                                    let machine_name =
                                        machine_map.get_machine(machine_id).config().name.clone();
                                    let proj = task
                                        .config()
                                        .parent
                                        .project_id()
                                        .and_then(|pid| project_name(project_map, pid));
                                    let info = TaskInfo::build(task, &state, machine_name, proj);
                                    send_notify(sender, NotifyEvent::Task { task: info });
                                }
                                db_updates.push(DbUpdate::TaskFinished {
                                    task_id,
                                    completed: *completed,
                                });
                                completed.consumed
                            }
                            TaskState::Failed { completed, error } => {
                                assert!(submitted_tasks.remove(&task_id).is_some());
                                if let Some(sender) = &notify_sender {
                                    let task = task_map.find_task(task_id).unwrap();
                                    let machine_name =
                                        machine_map.get_machine(machine_id).config().name.clone();
                                    let proj = task
                                        .config()
                                        .parent
                                        .project_id()
                                        .and_then(|pid| project_name(project_map, pid));
                                    let info = TaskInfo::build(task, &state, machine_name, proj);
                                    send_notify(sender, NotifyEvent::Task { task: info });
                                }
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
                                    if let Some(sender) = &notify_sender {
                                        let task = task_map.find_task(task_id).unwrap();
                                        let machine_name = machine_map
                                            .get_machine(machine_id)
                                            .config()
                                            .name
                                            .clone();
                                        let proj = task
                                            .config()
                                            .parent
                                            .project_id()
                                            .and_then(|pid| project_name(project_map, pid));
                                        let info =
                                            TaskInfo::build(task, &state, machine_name, proj);
                                        send_notify(sender, NotifyEvent::Task { task: info });
                                    }
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
                            if !exec_time.is_zero()
                                && let Some(project_id) = task.config().parent.project_id()
                            {
                                project_map
                                    .get_project_mut(project_id)
                                    .update_consumed(exec_time);
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
                        &notify_sender,
                        &mut db_updates,
                    );
                } else {
                    let machine = machine_map.get_machine_mut(machine_id);
                    if machine.remove_queued_session(session_id) {
                        debug!(%session_id, "Cancelling queued session on request");
                        let session = session_map.get_session_mut(session_id);
                        let now = Utc::now();
                        session.state.close(now);
                        if let Some(sender) = &notify_sender {
                            let proj = project_name(project_map, session.config.project_id)
                                .unwrap_or_default();
                            let info =
                                SessionInfo::build(session, machine.config().name.clone(), proj);
                            send_notify(sender, NotifyEvent::Session { session: info });
                        }
                        db_updates.push(DbUpdate::SessionClosed {
                            session_id,
                            exec_time: Duration::ZERO,
                            timestamp: now,
                            cancelled_tasks: Vec::new(),
                        });
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
                        if let Some(sender) = &notify_sender {
                            let proj = task
                                .config()
                                .parent
                                .project_id()
                                .and_then(|pid| project_name(project_map, pid));
                            let info = TaskInfo::build(
                                task,
                                task.state(),
                                machine.config().name.clone(),
                                proj,
                            );
                            send_notify(sender, NotifyEvent::Task { task: info });
                        }
                        db_updates.push(DbUpdate::TaskCancelled {
                            task_id,
                            completed: CompletedState::new_zero_now(),
                        });
                    }
                }
            }

            while submitted_tasks.len() < queue_size
                && let Some((task_id, payload)) = pick_task(
                    &mut core,
                    machine_id,
                    &mut running_session,
                    submitted_tasks.is_empty(),
                    &notify_sender,
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
                    let error = "Project time limit exceeded".to_string();
                    let state = TaskState::simple_error(error.clone());
                    let completed = *state.completed().unwrap();
                    if let Some(sender) = &notify_sender {
                        let split = core.split();
                        let task = split.task_map.find_task(task_id).unwrap();
                        let machine_name = split
                            .machine_map
                            .get_machine(machine_id)
                            .config()
                            .name
                            .clone();
                        let proj = task
                            .config()
                            .parent
                            .project_id()
                            .and_then(|pid| project_name(split.project_map, pid));
                        let info = TaskInfo::build(task, &state, machine_name, proj);
                        send_notify(sender, NotifyEvent::Task { task: info });
                    }
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
                    backend.clone().submit_task(task_id, payload);
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
    }
}
