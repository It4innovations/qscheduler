use crate::TaskId;
use crate::backend::{Backend, BackendFuture, ByteStream, FromBackendMessage};
use crate::error::RunnerError;
use crate::task::{CompletedState, TaskState};
use bytes::Bytes;
use chrono::Utc;
use futures_util::stream;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

struct TestTask {
    started: Option<Instant>,
    state: TaskState,
    result: String,
    artifacts: HashMap<String, String>,
}

struct TestBackend {
    backend_sender: UnboundedSender<FromBackendMessage>,
    // Keyed by the synthetic backend id ("test-{task_id}") rather than TaskId, so
    // get_task_result/get_task_artifact (which only ever receive a backend id, not a
    // TaskId) can look a task up directly.
    tasks: Arc<Mutex<HashMap<String, TestTask>>>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum TestOutcome {
    Ok,
    Fail { message: String },
}

#[derive(Debug, Deserialize)]
struct TestBackendTaskBody {
    outcome: TestOutcome,
    /// Result value reported once the task completes (finished or failed); defaults
    /// to `""` if not given.
    #[serde(default)]
    result: String,
    /// Named artifacts reported once the task completes (finished or failed).
    #[serde(default)]
    artifacts: HashMap<String, String>,
    #[serde(default)]
    submit_time: f32,
    #[serde(default)]
    compute_time: f32,
}

impl Backend for TestBackend {
    fn cancel_task(self: Arc<Self>, task_id: TaskId, backend_id: &str) {
        let mut tasks = self.tasks.lock().unwrap();
        let task = tasks.get_mut(backend_id).unwrap();
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
        let backend_id = format!("test-{task_id}");
        {
            let mut tasks = tasks.lock().unwrap();
            tasks.insert(
                backend_id.clone(),
                TestTask {
                    started: None,
                    state: TaskState::Waiting,
                    result: String::new(),
                    artifacts: HashMap::new(),
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
                backend_task_id: backend_id.clone(),
            });
            if body.submit_time > 0.0 {
                let ms = (body.submit_time * 1000.0).round() as u64;
                tokio::time::sleep(tokio::time::Duration::from_millis(ms)).await;
            }
            {
                let mut tasks = tasks.lock().unwrap();
                let task = tasks.get_mut(&backend_id).unwrap();
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
                let task = tasks.get_mut(&backend_id).unwrap();
                if task.state.is_final() {
                    return;
                }
                let completed = CompletedState {
                    timestamp: Utc::now(),
                    consumed: Instant::now() - task.started.unwrap(),
                };
                let new_state = match body.outcome {
                    TestOutcome::Ok => TaskState::Finished { completed },
                    TestOutcome::Fail { message } => TaskState::Failed {
                        completed,
                        error: message,
                    },
                };
                task.result = body.result;
                task.artifacts = body.artifacts;
                task.state = new_state.clone();
                new_state
            };
            let _ = sender.send(FromBackendMessage::TaskStateChange {
                task_id,
                state: new_state,
            });
        });
    }

    fn get_arch(self: Arc<Self>) -> BackendFuture<String> {
        Box::pin(std::future::ready(Ok("{\"arch\": \"Test\"}".to_string())))
    }

    fn get_calibration(
        self: Arc<Self>,
        _calibration_id: &str,
        _endpoint: &str,
    ) -> BackendFuture<String> {
        Box::pin(std::future::ready(Err(RunnerError::GenericError(
            "get_calibration not supported by test backend".to_string(),
        ))))
    }

    fn get_task_result(self: Arc<Self>, backend_id: &str) -> BackendFuture<ByteStream> {
        let result = {
            let tasks = self.tasks.lock().unwrap();
            match tasks.get(backend_id) {
                None => Err(RunnerError::GenericError(format!(
                    "Unknown test task '{backend_id}'"
                ))),
                Some(task) => match task.state {
                    TaskState::Finished { .. } | TaskState::Failed { .. } => {
                        Ok(task.result.clone())
                    }
                    _ => Err(RunnerError::GenericError(
                        "Task result is only available once the task has completed".to_string(),
                    )),
                },
            }
        };
        Box::pin(std::future::ready(result.map(string_to_stream)))
    }

    fn get_task_artifact(
        self: Arc<Self>,
        backend_id: &str,
        name: &str,
    ) -> BackendFuture<ByteStream> {
        let result = {
            let tasks = self.tasks.lock().unwrap();
            match tasks.get(backend_id) {
                None => Err(RunnerError::GenericError(format!(
                    "Unknown test task '{backend_id}'"
                ))),
                Some(task) => match task.state {
                    TaskState::Finished { .. } | TaskState::Failed { .. } => task
                        .artifacts
                        .get(name)
                        .cloned()
                        .ok_or_else(|| RunnerError::UnknownArtifact(name.to_string())),
                    _ => Err(RunnerError::GenericError(
                        "Task artifacts are only available once the task has completed".to_string(),
                    )),
                },
            }
        };
        Box::pin(std::future::ready(result.map(string_to_stream)))
    }
}

fn string_to_stream(s: String) -> ByteStream {
    Box::pin(stream::once(async move { Ok(Bytes::from(s)) }))
}

pub fn start_test_backend() -> (Arc<dyn Backend>, UnboundedReceiver<FromBackendMessage>) {
    let (b_sender, b_receiver) = mpsc::unbounded_channel();
    let backend = TestBackend {
        backend_sender: b_sender,
        tasks: Arc::new(Mutex::new(HashMap::new())),
    };
    (Arc::new(backend) as Arc<dyn Backend>, b_receiver)
}
