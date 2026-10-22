use crate::TaskId;
use crate::backend::{Backend, BackendFuture, BackendStream, BackendVersion, FromBackendMessage};
use crate::error::RunnerError;
use crate::task::{CompletedState, TaskKind, TaskState};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use reqwest::{Method, RequestBuilder};
use serde::Deserialize;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{Instrument, debug, error, warn};

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct IqmBackendConfig {
    pub url: String,
    pub token: String,
    pub machine_name: String,
    /// Accepts the pre-rename `check_interval` key of stored configurations.
    #[serde(alias = "check_interval")]
    pub poll_interval: Duration,
}

enum MonitorCommand {
    NewTask {
        task_id: TaskId,
        backend_id: String,
    },
    /// Cancel the job; retried on the task's later polls until an attempt succeeds.
    Cancel {
        task_id: TaskId,
        backend_id: String,
    },
    /// Outcome of a cancel attempt spawned by the monitor.
    CancelDone {
        task_id: TaskId,
        ok: bool,
    },
}

/// Timeout of a single job poll or cancel request, so an unreachable server (e.g. dropped
/// packets) can't stall the monitor loop; a timed-out request counts as a failed attempt.
const IQM_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Upper bound of the exponential backoff between polls of an unreachable job.
const MAX_POLL_BACKOFF: Duration = Duration::from_secs(60);

struct IqmBackend {
    config: IqmBackendConfig,
    client: reqwest::Client,
    backend_sender: UnboundedSender<FromBackendMessage>,
    monitor_sender: UnboundedSender<MonitorCommand>,
}

/// Version fields of the IQM `GET /about` response; other keys are ignored.
#[derive(Debug, Clone, Deserialize, serde::Serialize, utoipa::ToSchema)]
pub struct IqmVersion {
    pub qccsw_version: String,
    pub server_version: String,
    pub station_control_version: String,
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
    /// When the job's execution started, per the IQM timeline.
    fn execution_started(&self) -> Option<DateTime<Utc>> {
        self.timeline
            .iter()
            .find(|e| e.status == "execution_started")
            .map(|e| e.timestamp)
    }

    /// Timestamp of the newest timeline entry.
    fn last_timestamp(&self) -> Option<DateTime<Utc>> {
        self.timeline.iter().map(|e| e.timestamp).max()
    }

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

/// IQM job route suffix and request content type for a task of the given kind.
fn submit_route(kind: TaskKind) -> (&'static str, &'static str) {
    match kind {
        TaskKind::Circuit => ("circuit", "application/json; charset=UTF-8"),
        TaskKind::Pulse => ("run", "application/protobuf"),
    }
}

impl Backend for IqmBackend {
    fn get_name(&self) -> &'static str {
        "iqm"
    }

    fn cancel_task(self: Arc<Self>, task_id: TaskId, backend_id: &str) {
        // The monitor owns cancellation: it retries a failed attempt on the task's later polls
        // (with the same backoff), so a cancel issued while IQM is unreachable isn't lost.
        if self
            .monitor_sender
            .send(MonitorCommand::Cancel {
                task_id,
                backend_id: backend_id.to_string(),
            })
            .is_err()
        {
            error!(%task_id, "monitor channel closed while requesting task cancel");
        }
    }

    #[tracing::instrument(skip(self, _payload))]
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
        if self
            .monitor_sender
            .send(MonitorCommand::NewTask {
                task_id,
                backend_id: backend_id.clone(),
            })
            .is_err()
        {
            error!(%task_id, "monitor channel closed while resuming task");
        }
        if cancel {
            self.cancel_task(task_id, &backend_id);
        }
    }

    fn submit_task(self: Arc<Self>, task_id: TaskId, kind: TaskKind, payload: Bytes) {
        let span = tracing::info_span!("iqm_submit_task", %task_id, kind = kind.as_str());
        tokio::spawn(
            async move {
                let (route, content_type) = submit_route(kind);
                let url = &format!("{}/{route}", self.config.machine_name);
                let builder = self.setup_job_request(Method::POST, url, Some(content_type), None);
                debug!(%url, "connecting to IQM backend");
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
                                if self
                                    .backend_sender
                                    .send(FromBackendMessage::TaskSubmitted {
                                        task_id,
                                        backend_task_id: id_field.id.clone(),
                                    })
                                    .is_err()
                                {
                                    error!(
                                        "backend channel closed while reporting task submission"
                                    );
                                }
                                if self
                                    .monitor_sender
                                    .send(MonitorCommand::NewTask {
                                        task_id,
                                        backend_id: id_field.id,
                                    })
                                    .is_err()
                                {
                                    error!(
                                        "monitor channel closed while registering task for polling"
                                    );
                                }
                            }
                        }
                    }
                }
            }
            .instrument(span),
        );
    }

    fn get_arch(self: Arc<Self>) -> BackendFuture<String> {
        let request =
            self.setup_qc_request(Method::GET, "artifacts/static-quantum-architectures", None);
        Box::pin(fetch_text(request))
    }

    fn get_version(self: Arc<Self>) -> BackendFuture<BackendVersion> {
        let url = format!("{}/about", self.config.url);
        debug!(%url, "IQM about request");
        let request = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.config.token))
            .header("Accept", "application/json");
        Box::pin(async move {
            let text = fetch_text(request).await?;
            let version: IqmVersion = serde_json::from_str(&text).map_err(|e| {
                RunnerError::GenericError(format!("Invalid IQM /about response: {e}"))
            })?;
            Ok(BackendVersion::Iqm(version))
        })
    }

    fn get_calibration(
        self: Arc<Self>,
        calibration_id: &str,
        end_point: Option<&str>,
    ) -> BackendFuture<String> {
        let request = self.setup_calibration_request(Method::GET, calibration_id, end_point);
        Box::pin(fetch_text(request))
    }

    fn get_qc_artifact(
        self: Arc<Self>,
        name: &str,
        accept: Option<&str>,
    ) -> BackendFuture<BackendStream> {
        let request = self.setup_qc_request(Method::GET, &format!("artifacts/{name}"), accept);
        let name = name.to_string();
        Box::pin(fetch_stream(
            request,
            Some(move || RunnerError::UnknownArtifact(name)),
        ))
    }

    fn get_task_result(self: Arc<Self>, backend_id: &str) -> BackendFuture<BackendStream> {
        let request = self.setup_job_request(Method::GET, backend_id, None, None);
        Box::pin(fetch_stream(request, None::<fn() -> RunnerError>))
    }

    fn get_task_artifact(
        self: Arc<Self>,
        backend_id: &str,
        name: &str,
        accept: Option<&str>,
    ) -> BackendFuture<BackendStream> {
        let end_point = format!("{backend_id}/artifacts/{name}");
        let request = self.setup_job_request(Method::GET, &end_point, None, accept);
        let name = name.to_string();
        Box::pin(fetch_stream(
            request,
            Some(move || RunnerError::UnknownArtifact(name)),
        ))
    }
}

impl IqmBackend {
    /// `accept` defaults to `application/json`.
    pub fn setup_qc_request(
        &self,
        method: Method,
        end_point: &str,
        accept: Option<&str>,
    ) -> RequestBuilder {
        let url = format!(
            "{}/api/v1/quantum-computers/{}/{}",
            self.config.url, self.config.machine_name, end_point
        );
        debug!(%method, %url, "IQM QC request");
        self.client
            .request(method, &url)
            .header("Authorization", format!("Bearer {}", self.config.token))
            .header("Accept", accept.unwrap_or("application/json"))
    }

    pub fn setup_calibration_request(
        &self,
        method: Method,
        calibration_id: &str,
        end_point: Option<&str>,
    ) -> RequestBuilder {
        let mut url = format!(
            "{}/api/v1/calibration-sets/{}/{}",
            self.config.url, self.config.machine_name, calibration_id
        );
        if let Some(end_point) = end_point {
            url = format!("{url}/{end_point}");
        }
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
        content_type: Option<&str>,
        accept: Option<&str>,
    ) -> RequestBuilder {
        let url = format!("{}/api/v1/jobs/{}", self.config.url, end_point);
        debug!(%method, %url, "IQM job request");
        let mut builder = self
            .client
            .request(method, &url)
            .header("Authorization", format!("Bearer {}", self.config.token))
            .header("Accept", accept.unwrap_or("application/json"));
        if let Some(content_type) = content_type {
            builder = builder.header("Content-Type", content_type)
        }
        builder
    }

    pub fn send_task_error(&self, task_id: TaskId, message: String) {
        if self
            .backend_sender
            .send(FromBackendMessage::TaskStateChange {
                task_id,
                state: TaskState::simple_error(message),
            })
            .is_err()
        {
            error!(%task_id, "backend channel closed while reporting task error");
        }
    }
}

async fn fetch_text(request: RequestBuilder) -> crate::Result<String> {
    let resp = request
        .send()
        .await
        .map_err(|e| RunnerError::GenericError(format!("IQM request failed: {e}")))?;
    let http_status = resp.status();
    if !http_status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(RunnerError::GenericError(format!(
            "IQM backend HTTP {http_status}: {body}"
        )));
    }
    resp.text()
        .await
        .map_err(|e| RunnerError::GenericError(format!("Failed to read IQM response: {e}")))
}

async fn fetch_stream(
    request: RequestBuilder,
    not_found: Option<impl FnOnce() -> RunnerError>,
) -> crate::Result<BackendStream> {
    let resp = request
        .send()
        .await
        .map_err(|e| RunnerError::GenericError(format!("IQM request failed: {e}")))?;
    let http_status = resp.status();
    if http_status == reqwest::StatusCode::NOT_FOUND
        && let Some(not_found) = not_found
    {
        return Err(not_found());
    }
    if !http_status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(RunnerError::GenericError(format!(
            "IQM backend HTTP {http_status}: {body}"
        )));
    }
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let stream = resp.bytes_stream().map(|chunk| {
        chunk.map_err(|e| RunnerError::GenericError(format!("IQM stream error: {e}")))
    });
    Ok(BackendStream {
        content_type,
        stream: Box::pin(stream),
    })
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum CancelState {
    None,
    /// Requested but not yet delivered; an attempt is made on the task's next poll.
    Pending,
    /// An attempt is running.
    InFlight,
    /// IQM accepted the cancel; the job's final state arrives via polling.
    Sent,
}

struct MonitoredTask {
    task_id: TaskId,
    iqm_task_id: String,
    /// Consecutive failed polls; reset by any successful poll.
    fails: u32,
    /// Earliest time of the next poll (pushed out by backoff after failures).
    next_poll: Instant,
    /// Whether `FromBackendMessage::ExecutionStarted` was already sent for this task.
    start_reported: bool,
    cancel: CancelState,
}

impl MonitoredTask {
    fn new(task_id: TaskId, iqm_task_id: String) -> Self {
        MonitoredTask {
            task_id,
            iqm_task_id,
            fails: 0,
            next_poll: Instant::now(),
            start_reported: false,
            cancel: CancelState::None,
        }
    }
}

/// Delay before the next poll after `fails` consecutive failures: the poll interval, doubled
/// per further failure, capped at `MAX_POLL_BACKOFF`.
fn poll_backoff(poll_interval: Duration, fails: u32) -> Duration {
    let factor = 1u32 << fails.saturating_sub(1).min(16);
    poll_interval.saturating_mul(factor).min(MAX_POLL_BACKOFF)
}

/// Spawns one cancel attempt; the outcome is reported back as `MonitorCommand::CancelDone`.
fn spawn_cancel_attempt(backend: &Arc<IqmBackend>, task_id: TaskId, iqm_task_id: &str) {
    let backend = backend.clone();
    let url = format!("{iqm_task_id}/cancel");
    let span = tracing::info_span!("iqm_cancel_task", %task_id, backend_id = %iqm_task_id);
    tokio::spawn(
        async move {
            let result = backend
                .setup_job_request(Method::POST, &url, None, None)
                .timeout(IQM_REQUEST_TIMEOUT)
                .send()
                .await;
            let ok = match result {
                Ok(resp) if resp.status().is_success() => {
                    debug!(status = %resp.status(), "cancel task succeeded");
                    true
                }
                result => {
                    warn!(?result, "cancel attempt failed, will retry on next poll");
                    false
                }
            };
            if backend
                .monitor_sender
                .send(MonitorCommand::CancelDone { task_id, ok })
                .is_err()
            {
                error!("monitor channel closed while reporting cancel result");
            }
        }
        .instrument(span),
    );
}

async fn iqm_main(
    backend: Arc<IqmBackend>,
    mut monitor_receiver: UnboundedReceiver<MonitorCommand>,
) {
    let poll_interval = backend.config.poll_interval;
    let mut monitored_tasks: Vec<MonitoredTask> = Vec::new();
    // Cancels requested for tasks not (yet) monitored: applied if the task's `NewTask`
    // arrives later (a cancel can race ahead of it). Never starts monitoring by itself, so a
    // cancel for an already-finished task can't produce a second final state.
    let mut early_cancels: HashSet<TaskId> = HashSet::new();
    let mut interval = tokio::time::interval(poll_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval.tick().await; // consume the immediate first tick
    let mut to_delete: Vec<usize> = Vec::new();
    loop {
        select! {
            entry = monitor_receiver.recv() => {
                match entry {
                    Some(MonitorCommand::NewTask { task_id, backend_id }) => {
                        if !monitored_tasks.iter().any(|m| m.task_id == task_id) {
                            let mut task = MonitoredTask::new(task_id, backend_id);
                            if early_cancels.remove(&task_id) {
                                task.cancel = CancelState::Pending;
                            }
                            monitored_tasks.push(task);
                        }
                    }
                    Some(MonitorCommand::Cancel { task_id, backend_id }) => {
                        match monitored_tasks.iter_mut().find(|m| m.task_id == task_id) {
                            Some(task) => {
                                if task.cancel == CancelState::None || task.cancel == CancelState::Pending {
                                    task.cancel = CancelState::InFlight;
                                    spawn_cancel_attempt(&backend, task_id, &task.iqm_task_id);
                                }
                            }
                            None => {
                                // One best-effort attempt; retried only if the task shows up.
                                early_cancels.insert(task_id);
                                spawn_cancel_attempt(&backend, task_id, &backend_id);
                            }
                        }
                    }
                    Some(MonitorCommand::CancelDone { task_id, ok }) => {
                        if let Some(task) = monitored_tasks.iter_mut().find(|m| m.task_id == task_id)
                            && task.cancel == CancelState::InFlight
                        {
                            task.cancel = if ok { CancelState::Sent } else { CancelState::Pending };
                        }
                    }
                    None => return,
                }
            }
            _ = interval.tick(), if !monitored_tasks.is_empty() => {
                to_delete.clear();
                let now = Instant::now();
                for (idx, task) in monitored_tasks.iter_mut().enumerate() {
                    if task.next_poll > now {
                        continue;
                    }
                    if task.cancel == CancelState::Pending {
                        task.cancel = CancelState::InFlight;
                        spawn_cancel_attempt(&backend, task.task_id, &task.iqm_task_id);
                    }
                    let request = backend
                        .setup_job_request(Method::GET, &task.iqm_task_id, None, None)
                        .timeout(IQM_REQUEST_TIMEOUT);
                    let result = request.send().await;
                    // Local receive time: anything the response reports happened before it.
                    let received_at = Instant::now();
                    match process_result(task, result, received_at, &backend.backend_sender).await {
                        Ok(new_state) => {
                            task.fails = 0;
                            task.next_poll = now;
                            if let Some(new_state) = new_state {
                                if !matches!(&new_state, TaskState::Running) {
                                    to_delete.push(idx);
                                }
                                if backend.backend_sender.send(FromBackendMessage::TaskStateChange {
                                    task_id: task.task_id,
                                    state: new_state,
                                }).is_err() {
                                    error!(task_id = %task.task_id, "backend channel closed while reporting poll result");
                                }
                            }
                        }
                        Err(error) => {
                            // Never give up: IQM is the source of truth for the job's outcome and
                            // exec time, so keep polling (with backoff) until it answers.
                            task.fails += 1;
                            let delay = poll_backoff(poll_interval, task.fails);
                            task.next_poll = Instant::now() + delay;
                            warn!(
                                task_id = %task.task_id,
                                backend_id = %task.iqm_task_id,
                                fails = task.fails,
                                retry_in_ms = delay.as_millis(),
                                %error,
                                "IQM polling failed"
                            );
                        }
                    }
                }
                for idx in to_delete.iter().rev() {
                    monitored_tasks.remove(*idx);
                }
            }
        }
    }
}

#[tracing::instrument(skip_all, fields(task_id = %task.task_id, backend_id = %task.iqm_task_id))]
/// Interprets one poll response: `Ok` with the job's new state (if it changed to something
/// worth reporting), or `Err` describing a failed poll (transport error, non-success status,
/// unparsable body), which the caller retries with backoff.
async fn process_result(
    task: &mut MonitoredTask,
    result: Result<reqwest::Response, reqwest::Error>,
    received_at: Instant,
    sender: &UnboundedSender<FromBackendMessage>,
) -> Result<Option<TaskState>, String> {
    match result {
        Err(e) => Err(format!("request failed: {e}")),
        Ok(resp) => {
            let http_status = resp.status();
            if !http_status.is_success() {
                return Err(format!("HTTP status {http_status}"));
            }
            match resp.json::<JobStatusResponse>().await {
                Err(e) => Err(format!("could not parse IQM response: {e}")),
                Ok(data) => {
                    if !task.start_reported
                        && let Some(started_at) = data.execution_started()
                    {
                        task.start_reported = true;
                        if sender
                            .send(FromBackendMessage::ExecutionStarted {
                                task_id: task.task_id,
                                started_at,
                                observed_at: received_at,
                            })
                            .is_err()
                        {
                            error!("backend channel closed while reporting task start");
                        }
                    }
                    let status = data.status.as_str();
                    Ok(match status {
                        "waiting" => None,
                        status => {
                            // IQM timestamps; a job that never executed (e.g. cancelled or
                            // failed during compilation) consumed nothing and ends at its last
                            // timeline entry.
                            let (end_time, exec_time) = data.get_exec_time().unwrap_or_else(|| {
                                (
                                    data.last_timestamp().unwrap_or_else(Utc::now),
                                    Duration::ZERO,
                                )
                            });
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
                    })
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{JobStatusResponse, submit_route};
    use crate::task::TaskKind;
    use std::time::Duration;

    #[test]
    fn submit_route_by_kind() {
        assert_eq!(
            submit_route(TaskKind::Circuit),
            ("circuit", "application/json; charset=UTF-8")
        );
        assert_eq!(
            submit_route(TaskKind::Pulse),
            ("run", "application/protobuf")
        );
    }

    #[test]
    fn exec_time_from_timeline() {
        let data: JobStatusResponse = serde_json::from_str(
            r#"{"status": "completed", "timeline": [
                {"status": "created", "timestamp": "2026-01-01T10:00:00Z"},
                {"status": "execution_started", "timestamp": "2026-01-01T10:00:05Z"},
                {"status": "execution_ended", "timestamp": "2026-01-01T10:00:07.500Z"},
                {"status": "completed", "timestamp": "2026-01-01T10:00:08Z"}]}"#,
        )
        .unwrap();
        let (end, exec) = data.get_exec_time().unwrap();
        assert_eq!(end.to_rfc3339(), "2026-01-01T10:00:07.500+00:00");
        assert_eq!(exec, Duration::from_millis(2500));
    }

    #[test]
    fn never_executed_job_ends_at_last_entry() {
        let data: JobStatusResponse = serde_json::from_str(
            r#"{"status": "cancelled", "timeline": [
                {"status": "created", "timestamp": "2026-01-01T10:00:00Z"},
                {"status": "cancelled", "timestamp": "2026-01-01T10:00:02Z"}]}"#,
        )
        .unwrap();
        assert!(data.get_exec_time().is_none());
        assert_eq!(
            data.last_timestamp().unwrap().to_rfc3339(),
            "2026-01-01T10:00:02+00:00"
        );
    }
}
