use crate::TaskId;
use crate::backend::{Backend, FromBackendMessage};
use crate::task::TaskState;
use bytes::Bytes;
use serde::Deserialize;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;
use crate::error::RunnerError;

struct TestBackend {
    backend_sender: UnboundedSender<FromBackendMessage>,
    cancelled_tasks: Arc<Mutex<HashSet<TaskId>>>,
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
        self.cancelled_tasks.lock().unwrap().insert(task_id);
        let _ = self
            .backend_sender
            .send(FromBackendMessage::TaskStateChange {
                task_id,
                state: TaskState::Cancelled,
            });
    }

    fn submit_task(self: Arc<Self>, task_id: TaskId, payload: Bytes) {
        let sender = self.backend_sender.clone();
        let cancelled_tasks = self.cancelled_tasks.clone();
        tokio::spawn(async move {
            let Ok(body) = serde_json::from_slice::<TestBackendTaskBody>(payload.as_ref()) else {
                let _ = sender.send(FromBackendMessage::TaskStateChange {
                    task_id,
                    state: TaskState::Failed {
                        error: "Cannot parse task body".to_string(),
                    },
                });
                return;
            };
            if body.submit_time > 0.0 {
                let ms = (body.submit_time * 1000.0).round() as u64;
                tokio::time::sleep(tokio::time::Duration::from_millis(ms)).await;
            }
            let _ = sender.send(FromBackendMessage::TaskSubmitted {
                task_id,
                backend_task_id: format!("test-{task_id}"),
            });
            let _ = sender.send(FromBackendMessage::TaskStateChange {
                task_id,
                state: TaskState::Running,
            });
            if body.compute_time > 0.0 {
                let ms = (body.compute_time * 1000.0).round() as u64;
                tokio::time::sleep(tokio::time::Duration::from_millis(ms)).await;
            }
            if cancelled_tasks.lock().unwrap().contains(&task_id) {
                return;
            }
            let new_state = match body.result {
                TestBackendResult::Ok => TaskState::Finished,
                TestBackendResult::Fail { message } => TaskState::Failed { error: message },
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

    fn get_calibration(self: Arc<Self>, _calibration_id: &str, _endpoint: &str) -> Receiver<crate::Result<String>> {
        let (sx, rx) = oneshot::channel();
        let _ = sx.send(Err(RunnerError::GenericError("get_calibration not supported by test backend".to_string())));
        rx
    }
}

pub fn start_test_backend() -> (Arc<dyn Backend>, UnboundedReceiver<FromBackendMessage>) {
    let (b_sender, b_receiver) = mpsc::unbounded_channel();
    let backend = TestBackend {
        backend_sender: b_sender,
        cancelled_tasks: Arc::new(Mutex::new(HashSet::new())),
    };
    (Arc::new(backend) as Arc<dyn Backend>, b_receiver)
}
