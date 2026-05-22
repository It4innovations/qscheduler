use crate::backend::{Backend, BackendConfig};
use crate::callback::{NotifyConfig, NotifyEvent, NotifyTaskState, notify_worker};
use crate::core::{CoreRef, CoreSplitMut};
use crate::machine::QueueItem;
use crate::task::TaskState;
use crate::{MachineId, SessionId, SessionState};
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{Instant, sleep_until};
use tracing::debug;

pub fn start_launcher(
    core_ref: &CoreRef,
    machine_id: MachineId,
    config: &BackendConfig,
    notify_config: Option<&NotifyConfig>,
) {
    let core_ref = core_ref.clone();
    let backend = Backend::new(config);
    let notify_sender = notify_config.map(|nc| {
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
    tokio::spawn(async move { launcher_main(core_ref, machine_id, backend, notify_sender).await });
}

struct RunningSession {
    session_id: SessionId,
    deadline: Instant,
}

async fn launcher_main(
    core_ref: CoreRef,
    machine_id: MachineId,
    backend: Backend,
    notify_sender: Option<UnboundedSender<NotifyEvent>>,
) {
    debug!(%machine_id, "Starting launcher");
    let notifier = {
        let core = core_ref.lock().unwrap();
        let machine = core.split().machine_map.get_machine(machine_id);
        machine.notifier().clone()
    };
    'outer: loop {
        notifier.notified().await;
        let mut running_session = None;
        loop {
            let new_task = {
                let mut core = core_ref.lock().unwrap();
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
                            machine.set_running_task(task_id);
                            task.set_state(TaskState::Running);
                            break Some((task_id, task.config().payload.clone()));
                        } else {
                            break None;
                        }
                    } else {
                        match machine.pop_queue_item() {
                            None => break None,
                            Some(QueueItem::Task(task_id)) => {
                                let Some(task) = task_map.find_task_mut(task_id) else {
                                    continue;
                                };
                                machine.set_running_task(task_id);
                                task.set_state(TaskState::Running);
                                break Some((task_id, task.config().payload.clone()));
                            }
                            Some(QueueItem::Session(session_id)) => {
                                let Some(session) = session_map.find_session_mut(session_id) else {
                                    continue;
                                };
                                assert!(running_session.is_none());
                                session.state = SessionState::Open;
                                machine.start_session(session_id);
                                running_session = Some(RunningSession {
                                    session_id,
                                    deadline: Instant::now() + session.config.time_limit,
                                });
                            }
                        }
                    }
                }
            };
            if let Some((task_id, payload)) = new_task {
                debug!(%task_id, "Starting task");
                let task_f = backend.run_task(payload);
                let r = task_f.await;
                debug!(%task_id, "Task finished");
                let now = Instant::now();
                let (notify_event, cancelled_ids) = {
                    let mut core = core_ref.lock().unwrap();
                    let CoreSplitMut {
                        task_map,
                        machine_map,
                        session_map,
                        ..
                    } = core.split_mut();
                    let task = task_map.get_task_mut(task_id);
                    let notify_state = match r {
                        Ok(()) => {
                            task.set_state(TaskState::Finished);
                            NotifyTaskState::Finished
                        }
                        Err(e) => {
                            task.set_state(TaskState::Failed {
                                error: e.to_string(),
                            });
                            NotifyTaskState::Failed
                        }
                    };
                    let notify_event = NotifyEvent {
                        task_id,
                        state: notify_state,
                    };
                    let machine_id = task.config().machine_id;
                    let machine = machine_map.get_machine_mut(machine_id);
                    machine.complete_task(task_id);
                    let mut cancelled_ids = Vec::new();
                    if let Some(s) = &running_session
                        && now > s.deadline
                    {
                        debug!(session_id = %s.session_id, "Session overtime");
                        let session = session_map.get_session_mut(s.session_id);
                        session.state = SessionState::Closed;
                        running_session = None;
                        while let Some(task_id) = machine.pop_session_task() {
                            let task = task_map.get_task_mut(task_id);
                            task.set_state(TaskState::Cancelled);
                            cancelled_ids.push(task_id);
                        }
                        machine.close_session();
                    }
                    (notify_event, cancelled_ids)
                };
                if let Some(sender) = &notify_sender {
                    let _ = sender.send(notify_event);
                    for cancelled_id in cancelled_ids {
                        let _ = sender.send(NotifyEvent {
                            task_id: cancelled_id,
                            state: NotifyTaskState::Cancelled,
                        });
                    }
                }
            } else {
                if let Some(session) = &running_session {
                    let session_id = session.session_id;
                    let deadline = session.deadline;
                    tokio::select! {
                        _ = sleep_until(deadline) => {
                            debug!(%session_id, "Session overtime");
                            let mut core = core_ref.lock().unwrap();
                            let CoreSplitMut { session_map, machine_map, .. } = core.split_mut();
                            let session = session_map.get_session_mut(session_id);
                            session.state = SessionState::Closed;
                            machine_map.get_machine_mut(machine_id).close_session();
                            running_session = None;
                        }
                        _ = notifier.notified() => {}
                    }
                } else {
                    continue 'outer;
                }
            };
        }
    }
}
