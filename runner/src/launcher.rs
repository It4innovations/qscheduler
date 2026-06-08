use crate::backend::{Backend, FromBackendMessage, create_backend};
use crate::callback::{NotifyEvent, NotifyTaskState, notify_worker};
use crate::core::{Core, CoreRef, CoreSplitMut};
use crate::machine::{MachineConfig, QueueItem};
use crate::task::TaskState;
use crate::{MachineId, SessionId, SessionState, TaskId};
use bytes::Bytes;
use std::collections::HashMap;
use std::future::pending;
use std::sync::Arc;
use tokio::select;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::{Instant, sleep_until};
use tracing::debug;

pub fn start_launcher(core_ref: &CoreRef, machine_id: MachineId, machine_config: &MachineConfig) {
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
        )
        .await
    });
}

struct RunningSession {
    session_id: SessionId,
    deadline: Instant,
}

fn pick_task(
    core: &mut Core,
    machine_id: MachineId,
    running_session: &mut Option<RunningSession>,
    no_assignments: bool,
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
) {
    debug!(%machine_id, "Starting launcher");
    let (notifier, queue_size) = {
        let core = core_ref.lock().unwrap();
        let machine = core.split().machine_map.get_machine(machine_id);
        (machine.notifier().clone(), machine.config().queue_size)
    };
    assert!(queue_size > 0);
    let mut submitted_tasks: HashMap<TaskId, bool> = HashMap::new();
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
        let mut core = core_ref.lock().unwrap();
        let CoreSplitMut {
            task_map,
            session_map,
            machine_map,
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
                }
                machine.close_session();
            }
            LauncherEvent::BackendMessage(msg) => match msg {
                FromBackendMessage::TaskSubmitted {
                    task_id,
                    backend_task_id,
                } => {
                    if submitted_tasks.contains_key(&task_id) {
                        let task = task_map.get_task_mut(task_id);
                        task.set_backend_id(backend_task_id);
                    } else {
                        debug!(task_id = %task_id, "Finished submitting of already canceled task");
                        Arc::clone(&backend).cancel_task(task_id, &backend_task_id);
                    }
                }
                FromBackendMessage::TaskStateChange { task_id, state } => {
                    match state {
                        TaskState::Waiting => unreachable!(),
                        TaskState::Running => {}
                        TaskState::Finished | TaskState::Failed { .. } | TaskState::Cancelled => {
                            assert!(submitted_tasks.remove(&task_id).is_some());
                            if let Some(sender) = &notify_sender {
                                let _ = sender.send(NotifyEvent {
                                    task_id,
                                    state: state.to_notify().unwrap(),
                                });
                            }
                        }
                    }
                    let task = task_map.get_task_mut(task_id);
                    task.set_state(state);
                }
            },
        }
        while submitted_tasks.len() < queue_size
            && let Some((task_id, payload)) = pick_task(
                &mut core,
                machine_id,
                &mut running_session,
                submitted_tasks.is_empty(),
            )
        {
            backend.clone().submit_task(task_id, payload);
            submitted_tasks.insert(task_id, false);
        }
    }
}
