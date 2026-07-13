use crate::TaskId;
use crate::backend::{Backend, FromBackendMessage};
use crate::error::RunnerError;
use crate::task::{CompletedState, TaskState};
use bytes::Bytes;
use chrono::Utc;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;
use tokio::sync::{mpsc, oneshot};

struct TestTask {
    started: Option<Instant>,
    state: TaskState,
}

struct TestBackend {
    backend_sender: UnboundedSender<FromBackendMessage>,
    tasks: Arc<Mutex<HashMap<TaskId, TestTask>>>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum TestBackendResult {
    Ok,
    Fail { message: String },
}

#[derive(Debug, Deserialize)]
struct TestBackendTaskBody {
    result: TestBackendResult,
    #[serde(default)]
    submit_time: f32,
    #[serde(default)]
    compute_time: f32,
}

impl Backend for TestBackend {
    fn cancel_task(self: Arc<Self>, task_id: TaskId, _backend_id: &str) {
        let mut tasks = self.tasks.lock().unwrap();
        let task = tasks.get_mut(&task_id).unwrap();
        if task.state.is_final() {
            return;
        }
        let exec_time = if let Some(started) = task.started {
            
            Instant::now() - started
        } else {
            Duration::ZERO
        };
        let state = TaskState::Cancelled {
            completed: CompletedState {
                consumed: exec_time,
                timestamp: Utc::now(),
            },
        };
        task.state = state.clone();
        let _ = self
            .backend_sender
            .send(FromBackendMessage::TaskStateChange { task_id, state });
    }

    fn resume_task(
        self: Arc<Self>,
        task_id: TaskId,
        backend_id: String,
        payload: Bytes,
        cancel: bool,
    ) {
        // The test backend keeps no state across restarts, so re-run the task from
        // its stored payload; it progresses to finished/failed as a fresh submission.
        self.clone().submit_task(task_id, payload);
        if cancel {
            self.cancel_task(task_id, &backend_id);
        }
    }

    fn submit_task(self: Arc<Self>, task_id: TaskId, payload: Bytes) {
        let sender = self.backend_sender.clone();
        let tasks = self.tasks.clone();
        {
            let mut tasks = tasks.lock().unwrap();
            tasks.insert(
                task_id,
                TestTask {
                    started: None,
                    state: TaskState::Waiting,
                },
            );
        }
        tokio::spawn(async move {
            let Ok(body) = serde_json::from_slice::<TestBackendTaskBody>(payload.as_ref()) else {
                let _ = sender.send(FromBackendMessage::TaskStateChange {
                    task_id,
                    state: TaskState::simple_error("Cannot parse task body".to_string()),
                });
                return;
            };
            let _ = sender.send(FromBackendMessage::TaskSubmitted {
                task_id,
                backend_task_id: format!("test-{task_id}"),
            });
            if body.submit_time > 0.0 {
                let ms = (body.submit_time * 1000.0).round() as u64;
                tokio::time::sleep(tokio::time::Duration::from_millis(ms)).await;
            }
            {
                let mut tasks = tasks.lock().unwrap();
                let task = tasks.get_mut(&task_id).unwrap();
                if task.state.is_final() {
                    return;
                }
                task.started = Some(Instant::now());
            }
            let _ = sender.send(FromBackendMessage::TaskStateChange {
                task_id,
                state: TaskState::Running,
            });
            if body.compute_time > 0.0 {
                let ms = (body.compute_time * 1000.0).round() as u64;
                tokio::time::sleep(tokio::time::Duration::from_millis(ms)).await;
            }
            let new_state = {
                let mut tasks = tasks.lock().unwrap();
                let task = tasks.get_mut(&task_id).unwrap();
                if task.state.is_final() {
                    return;
                }
                let completed = CompletedState {
                    timestamp: Utc::now(),
                    consumed: Instant::now() - task.started.unwrap(),
                };
                let new_state = match body.result {
                    TestBackendResult::Ok => TaskState::Finished { completed },
                    TestBackendResult::Fail { message } => TaskState::Failed {
                        completed,
                        error: message,
                    },
                };
                task.state = new_state.clone();
                new_state
            };
            let _ = sender.send(FromBackendMessage::TaskStateChange {
                task_id,
                state: new_state,
            });
        });
    }

    fn get_arch(self: Arc<Self>) -> Receiver<crate::Result<String>> {
        let (sx, rx) = oneshot::channel();
        let _ = sx.send(Ok("{\"arch\": \"Test\"}".to_string()));
        rx
    }

    fn get_calibration(
        self: Arc<Self>,
        _calibration_id: &str,
        _endpoint: &str,
    ) -> Receiver<crate::Result<String>> {
        let (sx, rx) = oneshot::channel();
        let _ = sx.send(Err(RunnerError::GenericError(
            "get_calibration not supported by test backend".to_string(),
        )));
        rx
    }
}

pub fn start_test_backend() -> (Arc<dyn Backend>, UnboundedReceiver<FromBackendMessage>) {
    let (b_sender, b_receiver) = mpsc::unbounded_channel();
    let backend = TestBackend {
        backend_sender: b_sender,
        tasks: Arc::new(Mutex::new(HashMap::new())),
    };
    (Arc::new(backend) as Arc<dyn Backend>, b_receiver)
}
