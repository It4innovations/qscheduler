use crate::backend::{Backend, FromBackendMessage, create_backend};
use crate::callback::{NotifyEvent, NotifyTaskState, notify_worker};
use crate::core::{Core, CoreRef, CoreSplitMut};
use crate::db;
use crate::machine::{MachineConfig, QueueItem, ResumeTask};
use crate::task::TaskState;
use crate::project::ProjectId;
use crate::{MachineId, SessionId, SessionState, TaskId};
use bytes::Bytes;
use std::collections::HashMap;
use std::future::pending;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::{Instant, sleep_until};
use tracing::debug;

pub fn start_launcher(core_ref: &CoreRef, machine_id: MachineId, machine_config: &MachineConfig, restore_tasks: Vec<ResumeTask>) {
    let core_ref = core_ref.clone();
    let (backend, backend_receiver) = create_backend(&machine_config.backend);
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
    deadline: Instant,
}

enum DbUpdate {
    TaskSubmitted { task_id: TaskId, backend_id: String },
    TaskFinished { task_id: TaskId, exec_time: Duration },
    TaskFailed { task_id: TaskId, exec_time: Duration, error: String  },
    TaskCancelled { task_id: TaskId, exec_time: Duration },
    SessionOpened(SessionId),
    SessionClosed { session_id: SessionId, cancelled_tasks: Vec<TaskId> },
}

fn pick_task(
    core: &mut Core,
    machine_id: MachineId,
    running_session: &mut Option<RunningSession>,
    no_assignments: bool,
    db_updates: &mut Vec<DbUpdate>,
) -> Option<(TaskId, Bytes)> {
    let CoreSplitMut {
        machine_map,
        task_map,
        session_map,
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
                    session.state = SessionState::Open;
                    machine.start_session(session_id);
                    *running_session = Some(RunningSession {
                        session_id,
                        deadline: Instant::now() + session.config.time_limit,
                    });
                    db_updates.push(DbUpdate::SessionOpened(session_id));
                    continue;
                }
            }
        }
    }
}

enum LauncherEvent {
    Notified,
    SessionEnd,
    BackendMessage(FromBackendMessage),
}


async fn launcher_main(
    core_ref: CoreRef,
    machine_id: MachineId,
    backend: Arc<dyn Backend>,
    mut backend_receiver: UnboundedReceiver<FromBackendMessage>,
    notify_sender: Option<UnboundedSender<NotifyEvent>>,
    resume_tasks: Vec<ResumeTask>,
) {
    debug!(%machine_id, "Starting launcher");
    let (notifier, queue_size) = {
        let core = core_ref.lock().unwrap();
        let machine = core.split().machine_map.get_machine(machine_id);
        (machine.notifier().clone(), machine.config().queue_size)
    };
    let pool = core_ref.lock().unwrap().pool().clone();
    assert!(queue_size > 0);
    let mut submitted_tasks: HashMap<TaskId, bool> = HashMap::new();

    for rt in resume_tasks {
        debug!(task_id = %rt.task_id, "Re-attaching submitted task to backend");
        backend.clone().resume_task(rt.task_id, rt.backend_id, rt.payload);
        submitted_tasks.insert(rt.task_id, false);
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
            msg = backend_receiver.recv() =>
                LauncherEvent::BackendMessage(msg.unwrap())
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
                LauncherEvent::SessionEnd => {
                    let s = running_session.take().unwrap();
                    debug!(session_id = %s.session_id, "Session overtime");
                    for (task_id, cancelling) in submitted_tasks.iter_mut() {
                        if !*cancelling {
                            *cancelling = true;
                            let task = task_map.get_task_mut(*task_id);
                            if let Some(backend_id) = task.backend_id() {
                                debug!(%task_id, "Cancelling submitted task");
                                Arc::clone(&backend).cancel_task(*task_id, backend_id);
                            } else {
                                debug!(%task_id, "Cancelling not fully submitted task; will be cancelled later");
                            }
                        }
                    }
                    let session = session_map.get_session_mut(s.session_id);
                    session.state = SessionState::Closed;
                    let machine = machine_map.get_machine_mut(machine_id);
                    let mut cancelled_tasks = Vec::new();
                    while let Some(task_id) = machine.pop_session_task() {
                        debug!(%task_id, "Cancelling unsubmitted task");
                        let task = task_map.get_task_mut(task_id);
                        task.set_state(TaskState::Cancelled);
                        if let Some(sender) = &notify_sender {
                            let _ = sender.send(NotifyEvent {
                                task_id,
                                state: NotifyTaskState::Cancelled,
                            });
                        }
                        cancelled_tasks.push(task_id);
                    }
                    machine.close_session();
                    db_updates.push(DbUpdate::SessionClosed {
                        session_id: s.session_id,
                        cancelled_tasks,
                    });
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
                    FromBackendMessage::TaskStateChange { task_id, state, exec_time } => {
                        let mut update_state = true;
                        match &state {
                            TaskState::Waiting => unreachable!(),
                            TaskState::Running => {}
                            TaskState::Finished => {
                                assert!(submitted_tasks.remove(&task_id).is_some());
                                if let Some(sender) = &notify_sender {
                                    let _ = sender.send(NotifyEvent {
                                        task_id,
                                        state: NotifyTaskState::Finished,
                                    });
                                }
                                db_updates.push(DbUpdate::TaskFinished { task_id, exec_time });
                            }
                            TaskState::Failed { error } => {
                                assert!(submitted_tasks.remove(&task_id).is_some());
                                if let Some(sender) = &notify_sender {
                                    let _ = sender.send(NotifyEvent {
                                        task_id,
                                        state: NotifyTaskState::Failed,
                                    });
                                }
                                db_updates.push(DbUpdate::TaskFailed {
                                    task_id,
                                    exec_time,
                                    error: error.clone(),
                                });
                            }
                            TaskState::Cancelled => {
                                if submitted_tasks.remove(&task_id).is_none() {
                                    debug!(%task_id, "Ignoring cancel of already-finished task");
                                    update_state = false;
                                } else {
                                    if let Some(sender) = &notify_sender {
                                        let _ = sender.send(NotifyEvent {
                                            task_id,
                                            state: NotifyTaskState::Cancelled,
                                        });
                                    }
                                    db_updates.push(DbUpdate::TaskCancelled { task_id, exec_time });
                                }
                            }
                        }
                        if update_state {
                            let task = task_map.get_task_mut(task_id);
                            task.set_state(state);
                            if !exec_time.is_zero() && let Some(project_id) = task.config().parent.project_id() {
                                project_map.get_project_mut(project_id).update_consumed(exec_time);
                            }
                        }
                    }
                },
            }
            while submitted_tasks.len() < queue_size
                && let Some((task_id, payload)) = pick_task(
                    &mut core,
                    machine_id,
                    &mut running_session,
                    submitted_tasks.is_empty(),
                    &mut db_updates,
                )
            {
                let CoreSplitMut { task_map, project_map, .. } = core.split_mut();

                // Check that project has enough time
                if task_map.find_task(task_id)
                    .and_then(|t| t.config().parent.project_id())
                    .map(|pid| {
                        let p = project_map.find_project(pid).expect("Project has to be cached");
                        p.consumed > p.limit
                    }).unwrap_or(false) {
                        let error = "Project time limit exceeded".to_string();
                        core.split_mut().task_map.get_task_mut(task_id)
                            .set_state(TaskState::Failed { error: error.clone() });
                        if let Some(sender) = &notify_sender {
                            let _ = sender.send(NotifyEvent { task_id, state: NotifyTaskState::Failed });
                        }
                        db_updates.push(DbUpdate::TaskFailed { task_id, exec_time: Duration::ZERO, error });
                        continue;
                }
                backend.clone().submit_task(task_id, payload);
                submitted_tasks.insert(task_id, false);
            }
        }

        for update in db_updates {
            match update {
                DbUpdate::TaskSubmitted { task_id, backend_id } => {
                    db::update_task_backend_id(&pool, task_id, &backend_id).await
                }
                DbUpdate::TaskFinished { task_id, exec_time } => {
                    db::update_task_finished(&pool, task_id, exec_time).await;
                }
                DbUpdate::TaskFailed { task_id, exec_time, error } => {
                    db::update_task_failed(&pool, task_id, exec_time, &error).await;
                }
                DbUpdate::TaskCancelled { task_id, exec_time } => {
                    db::update_task_cancelled(&pool, task_id, exec_time).await;
                }
                DbUpdate::SessionOpened(session_id) => {
                    db::update_session_opened(&pool, session_id).await
                }
                DbUpdate::SessionClosed { session_id, cancelled_tasks } => {
                    db::close_session_with_tasks(&pool, session_id, &cancelled_tasks).await
                }
            }
        }
    }
}
