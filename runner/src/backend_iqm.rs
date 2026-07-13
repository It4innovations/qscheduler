use crate::TaskId;
use crate::backend::{Backend, FromBackendMessage};
use crate::error::RunnerError;
use crate::task::{CompletedState, TaskState};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use reqwest::{Method, RequestBuilder};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{MissedTickBehavior, sleep};
use tokio::{select, spawn};
use tracing::{debug, log};

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct IqmBackendConfig {
    pub url: String,
    pub token: String,
    pub machine_name: String,
    pub check_interval: Duration,
}

enum MonitorCommand {
    NewTask { task_id: TaskId, backend_id: String },
    UnregisterTask { task_id: TaskId },
}

struct IqmBackend {
    config: IqmBackendConfig,
    client: reqwest::Client,
    backend_sender: UnboundedSender<FromBackendMessage>,
    monitor_sender: UnboundedSender<MonitorCommand>,
}

#[derive(Deserialize)]
struct IdField {
    id: String,
}

#[derive(Deserialize)]
struct JobError {
    message: String,
}

#[derive(Deserialize)]
struct TimelineEntry {
    status: String,
    timestamp: DateTime<Utc>,
}

#[derive(Deserialize)]
struct JobStatusResponse {
    status: String,
    #[serde(default)]
    errors: Vec<JobError>,
    // Sibling of `status`, not nested under a "data" object — the IQM job API returns
    // {"status": ..., "timeline": [...], ...} flat.
    #[serde(default)]
    timeline: Vec<TimelineEntry>,
}

impl JobStatusResponse {
    fn get_exec_time(&self) -> Option<(DateTime<Utc>, Duration)> {
        let timeline = &self.timeline;
        let start_idx = timeline
            .iter()
            .position(|e| e.status == "execution_started")?;
        let started = &timeline[start_idx];
        let ended = timeline[start_idx + 1..].iter().find(|e| {
            e.status == "execution_ended" || e.status == "cancelled" || e.status == "failed"
        })?;
        Some((
            ended.timestamp,
            (ended.timestamp - started.timestamp)
                .to_std()
                .unwrap_or_default(),
        ))
    }
}

impl Backend for IqmBackend {
    fn cancel_task(self: Arc<Self>, task_id: TaskId, backend_id: &str) {
        let backend_id = backend_id.to_string();
        tokio::spawn(async move {
            let url = &format!("{}/cancel", backend_id);
            for _ in 0..10 {
                let builder = self.setup_job_request(Method::POST, url, false);
                let result = builder.send().await;
                match result {
                    Ok(resp) if resp.status().is_success() => {
                        log::debug!("cancel task {} with status {}", backend_id, resp.status());
                        return;
                    }
                    e => {
                        log::debug!("cancel task {} with status {:?}", backend_id, e);
                    }
                }
                sleep(Duration::from_millis(2000)).await;
            }
            log::error!("Failed to cancel task from backend");
            let _ = self
                .monitor_sender
                .send(MonitorCommand::UnregisterTask { task_id });
        });
    }

    fn resume_task(
        self: Arc<Self>,
        task_id: TaskId,
        backend_id: String,
        _payload: Bytes,
        cancel: bool,
    ) {
        // The job already exists on the backend; re-register it for polling so the
        // monitor reconciles its current state. Do not re-submit the circuit.
        debug!(%task_id, %backend_id, "Resuming monitoring of IQM job");
        if cancel {
            self.clone().cancel_task(task_id, &backend_id);
        };
        let _ = self.monitor_sender.send(MonitorCommand::NewTask {
            task_id,
            backend_id,
        });
    }

    fn submit_task(self: Arc<Self>, task_id: TaskId, payload: Bytes) {
        tokio::spawn(async move {
            let url = &format!("{}/circuit", self.config.machine_name);
            let builder = self.setup_job_request(Method::POST, url, true);
            log::debug!("Connecting to {url}");
            let result = builder.body(payload).send().await;
            match result {
                Err(e) => self.send_task_error(task_id, format!("IQM request failed: {}", e)),
                Ok(resp) => {
                    let http_status = resp.status();
                    if !http_status.is_success() {
                        let body = resp.text().await.unwrap_or_default();
                        self.send_task_error(
                            task_id,
                            format!("IQM backend HTTP {}: {}", http_status, body),
                        );
                        return;
                    }
                    match resp.json::<IdField>().await {
                        Err(e) => {
                            self.send_task_error(
                                task_id,
                                format!("Parsing IQM backend failed: {e}"),
                            );
                        }
                        Ok(id_field) => {
                            self.backend_sender
                                .send(FromBackendMessage::TaskSubmitted {
                                    task_id,
                                    backend_task_id: id_field.id.clone(),
                                })
                                .unwrap();
                            self.monitor_sender
                                .send(MonitorCommand::NewTask {
                                    task_id,
                                    backend_id: id_field.id,
                                })
                                .unwrap();
                        }
                    }
                }
            }
        });
    }

    fn get_arch(self: Arc<Self>) -> oneshot::Receiver<crate::Result<String>> {
        let (sx, rx) = oneshot::channel();
        let request = self.setup_qc_request(Method::GET, "artifacts/static-quantum-architectures");
        fetch_to_oneshot(request, sx);
        rx
    }

    fn get_calibration(
        self: Arc<Self>,
        calibration_id: &str,
        end_point: &str,
    ) -> Receiver<crate::Result<String>> {
        let (sx, rx) = oneshot::channel();
        let request = self.setup_calibration_request(Method::GET, calibration_id, end_point);
        fetch_to_oneshot(request, sx);
        rx
    }

    fn get_task_result(self: Arc<Self>, backend_id: &str) -> Receiver<crate::Result<String>> {
        let (sx, rx) = oneshot::channel();
        let request = self.setup_job_request(Method::GET, backend_id, false);
        fetch_to_oneshot(request, sx);
        rx
    }

    fn get_task_artifact(
        self: Arc<Self>,
        backend_id: &str,
        name: &str,
    ) -> Receiver<crate::Result<String>> {
        let (sx, rx) = oneshot::channel();
        let end_point = format!("{backend_id}/artifacts/{name}");
        let request = self.setup_job_request(Method::GET, &end_point, false);
        fetch_to_oneshot(request, sx);
        rx
    }
}

impl IqmBackend {
    pub fn setup_qc_request(&self, method: Method, end_point: &str) -> RequestBuilder {
        let url = format!(
            "{}/api/v1/quantum-computers/{}/{}",
            self.config.url, self.config.machine_name, end_point
        );
        debug!(%method, %url, "IQM QC request");
        self.client
            .request(method, &url)
            .header("Authorization", format!("Bearer {}", self.config.token))
            .header("Accept", "application/json")
    }

    pub fn setup_calibration_request(
        &self,
        method: Method,
        calibration_id: &str,
        end_point: &str,
    ) -> RequestBuilder {
        let url = format!(
            "{}/api/v1/calibration-sets/{}/{}/{}",
            self.config.url, self.config.machine_name, calibration_id, end_point
        );
        debug!(%method, %url, "IQM calibration request");
        self.client
            .request(method, &url)
            .header("Authorization", format!("Bearer {}", self.config.token))
            .header("Accept", "application/json")
    }

    pub fn setup_job_request(
        &self,
        method: Method,
        end_point: &str,
        content_type: bool,
    ) -> RequestBuilder {
        let url = format!("{}/api/v1/jobs/{}", self.config.url, end_point);
        debug!(%method, %url, "IQM job request");
        let mut builder = self
            .client
            .request(method, &url)
            .header("Authorization", format!("Bearer {}", self.config.token))
            .header("Accept", "application/json");
        if content_type {
            builder = builder.header("Content-Type", "application/json; charset=UTF-8")
        }
        builder
    }

    pub fn send_task_error(&self, task_id: TaskId, message: String) {
        let _ = self
            .backend_sender
            .send(FromBackendMessage::TaskStateChange {
                task_id,
                state: TaskState::simple_error(message),
            });
    }
}

fn fetch_to_oneshot(request: RequestBuilder, sx: oneshot::Sender<crate::Result<String>>) {
    spawn(async move {
        let result = match request.send().await {
            Err(e) => Err(RunnerError::GenericError(format!(
                "IQM request failed: {e}"
            ))),
            Ok(resp) => {
                let http_status = resp.status();
                if !http_status.is_success() {
                    let body = resp.text().await.unwrap_or_default();
                    Err(RunnerError::GenericError(format!(
                        "IQM backend HTTP {http_status}: {body}"
                    )))
                } else {
                    resp.text().await.map_err(|e| {
                        RunnerError::GenericError(format!("Failed to read IQM response: {e}"))
                    })
                }
            }
        };
        let _ = sx.send(result);
    });
}

pub fn start_iqm_backend(
    config: &IqmBackendConfig,
) -> (Arc<dyn Backend>, UnboundedReceiver<FromBackendMessage>) {
    let (b_sender, b_receiver) = mpsc::unbounded_channel();
    let (m_sender, m_receiver) = mpsc::unbounded_channel();
    let backend = Arc::new(IqmBackend {
        backend_sender: b_sender,
        monitor_sender: m_sender,
        config: config.clone(),
        client: reqwest::Client::new(),
    });
    let backend2 = backend.clone();
    tokio::spawn(async move {
        iqm_main(backend2, m_receiver).await;
    });
    (backend, b_receiver)
}

struct MonitoredTask {
    task_id: TaskId,
    iqm_task_id: String,
    fails: u32,
}

const MAX_FAILS: u32 = 120;

async fn iqm_main(
    backend: Arc<IqmBackend>,
    mut monitor_receiver: UnboundedReceiver<MonitorCommand>,
) {
    let mut monitored_tasks: Vec<MonitoredTask> = Vec::new();
    let mut interval = tokio::time::interval(backend.config.check_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval.tick().await; // consume the immediate first tick
    let mut to_delete: Vec<usize> = Vec::new();
    loop {
        select! {
            entry = monitor_receiver.recv() => {
                match entry {
                    Some(MonitorCommand::NewTask {task_id, backend_id}) => monitored_tasks.push(MonitoredTask {
                        task_id, iqm_task_id: backend_id, fails: 0
                    }),
                    Some(MonitorCommand::UnregisterTask { task_id}) => {
                        monitored_tasks.retain(|m| m.task_id != task_id);
                    }
                    None => return,
                }
            }
            _ = interval.tick(), if !monitored_tasks.is_empty() => {
                to_delete.clear();
                for (idx, task) in monitored_tasks.iter_mut().enumerate() {
                    let request = backend.setup_job_request(Method::GET, &task.iqm_task_id, false);
                    let result = request.send().await;
                    if let Some(new_state) = process_result(task, result).await {
                        if !matches!(&new_state, TaskState::Running) {
                            to_delete.push(idx);
                        }
                        let _ = backend.backend_sender.send(FromBackendMessage::TaskStateChange {
                            task_id: task.task_id,
                            state: new_state,
                        });
                    }

                }
                for idx in to_delete.iter().rev() {
                    monitored_tasks.remove(*idx);
                }
            }
        }
    }
}

async fn process_result(
    task: &mut MonitoredTask,
    result: Result<reqwest::Response, reqwest::Error>,
) -> Option<TaskState> {
    match result {
        Err(e) => {
            log::error!("IQM polling job {}: {}", task.iqm_task_id, e);
            task.fails += 1;
            if task.fails > MAX_FAILS {
                return Some(TaskState::simple_error(format!(
                    "IQM poll do not respond: {}",
                    e
                )));
            }
            None
        }
        Ok(resp) => {
            let http_status = resp.status();
            if !http_status.is_success() {
                log::error!(
                    "IQM polling job {}: HTTP status {}",
                    task.iqm_task_id,
                    http_status
                );
                task.fails += 1;
                if task.fails > MAX_FAILS {
                    return Some(TaskState::simple_error(format!(
                        "IQM poll do not respond: {http_status}"
                    )));
                }
                return None;
            }
            match resp.json::<JobStatusResponse>().await {
                Err(_) => Some(TaskState::simple_error(
                    "Could not parse IQM response".to_string(),
                )),
                Ok(data) => {
                    let status = data.status.as_str();
                    match status {
                        "waiting" => None,
                        status => {
                            let (end_time, exec_time) = data.get_exec_time().unwrap_or_default();
                            let completed = CompletedState {
                                consumed: exec_time,
                                timestamp: end_time,
                            };
                            let new_state = match status {
                                "processing" => TaskState::Running,
                                "completed" => TaskState::Finished { completed },
                                "failed" => {
                                    let msgs: Vec<_> =
                                        data.errors.into_iter().map(|e| e.message).collect();
                                    let error = msgs.join("; ");
                                    TaskState::Failed { completed, error }
                                }
                                "cancelled" => TaskState::Cancelled { completed },
                                state_name => TaskState::Failed {
                                    completed,
                                    error: format!("Invalid task state: {state_name}"),
                                },
                            };
                            Some(new_state)
                        }
                    }
                }
            }
        }
    }
}
