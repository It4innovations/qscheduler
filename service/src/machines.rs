use crate::{AdminAuth, AppState, client_error, internal_error};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use runner::config::{
    DEFAULT_LIMIT_CHECK_INTERVAL_MS, DEFAULT_MAX_CLOCK_SKEW_MS, DEFAULT_MAX_SESSION_TIME_MS,
    DEFAULT_QUEUE_SIZE, DEFAULT_TASK_TIME_LIMIT_MS, MachineConfiguration,
};
use runner::core::Core;
use runner::error::RunnerError;
use runner::reactor::{
    add_machine, get_machine_id_by_name, get_machine_info, list_machine_suspensions, list_machines,
    remove_machine, resume_machine, suspend_machine, update_machine,
};
use runner::{
    BackendConfig, BackendVersion, IqmBackendConfig, MachineId, MachineInfo, SuspendCancel,
    Suspension,
};
use std::sync::Arc;
use std::time::Duration;

pub(crate) fn find_machine(core: &Core, machine: &str) -> Result<MachineId, (StatusCode, String)> {
    get_machine_id_by_name(core, machine).map_err(|e| client_error(StatusCode::NOT_FOUND, e))
}

/// Fetch the backend's architecture description.
#[utoipa::path(
    get,
    path = "/machine/{machine}/arch",
    params(("machine" = String, Path, description = "Machine ID")),
    responses(
        (status = 200, description = "Architecture JSON", body = String),
        (status = 404, description = "Machine not found"),
        (status = 500, description = "Backend error")
    )
)]
pub(crate) async fn get_machine_arch(
    State(state): State<Arc<AppState>>,
    Path(machine): Path<String>,
) -> Result<String, (StatusCode, String)> {
    let future = {
        let core = state.core_ref.lock().unwrap();
        let machine_id = find_machine(&core, &machine)?;
        core.get_arch(machine_id).map_err(internal_error)?
    };
    future.await.map_err(internal_error)
}

/// Backend identification returned by `GET /machine/{machine}/about`.
#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct BackendAbout {
    /// Backend kind, e.g. `IQM` or `test`.
    #[serde(rename = "type")]
    pub backend_type: &'static str,
    pub version: BackendVersion,
}

/// Describe the machine's backend: its type and version information.
#[utoipa::path(
    get,
    path = "/machine/{machine}/about",
    params(("machine" = String, Path, description = "Machine name")),
    responses(
        (status = 200, description = "Backend type and version, e.g. `{\"type\": \"IQM\", \"version\": {\"server_version\": \"...\"}}`", body = BackendAbout),
        (status = 404, description = "Machine not found"),
        (status = 500, description = "Backend error")
    )
)]
pub(crate) async fn get_machine_about(
    State(state): State<Arc<AppState>>,
    Path(machine): Path<String>,
) -> Result<Json<BackendAbout>, (StatusCode, String)> {
    let (name, future) = {
        let core = state.core_ref.lock().unwrap();
        let machine_id = find_machine(&core, &machine)?;
        core.get_about(machine_id).map_err(internal_error)?
    };
    future
        .await
        .map(|version| {
            Json(BackendAbout {
                backend_type: name,
                version,
            })
        })
        .map_err(internal_error)
}

/// Fetch a calibration data set from the backend.
#[utoipa::path(
    get,
    path = "/machine/{machine}/calibration/{calibration}/{endpoint}",
    params(
        ("machine" = String, Path, description = "Machine ID"),
        ("calibration" = String, Path, description = "Calibration name"),
        ("endpoint" = String, Path, description = "Endpoint")
    ),
    responses(
        (status = 200, description = "Calibration JSON", body = String),
        (status = 404, description = "Machine not found"),
        (status = 500, description = "Backend error")
    )
)]
pub(crate) async fn get_machine_calibration(
    State(state): State<Arc<AppState>>,
    Path((machine, calibration, endpoint)): Path<(String, String, String)>,
) -> Result<String, (StatusCode, String)> {
    let future = {
        let core = state.core_ref.lock().unwrap();
        let machine_id = find_machine(&core, &machine)?;
        core.get_calibration(machine_id, &calibration, &endpoint)
            .map_err(internal_error)?
    };
    future.await.map_err(internal_error)
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
pub(crate) struct SuspendRequest {
    /// Why the machine is suspended (at most 512 characters). Returned in the error of every
    /// rejected task/session submission, and in the error of every task failed by `cancel`.
    message: String,
    /// What happens to the machine's existing work:
    /// `none` — it is computed as normal;
    /// `waiting` — tasks not yet submitted to the backend fail, and all sessions are closed
    /// (the tasks of an open session fail);
    /// `all` — like `waiting`, and tasks already submitted to the backend fail too (their
    /// cancellation on the backend is requested).
    #[serde(default)]
    #[schema(default = "none")]
    cancel: SuspendCancel,
}

/// Suspend a machine. While suspended, the machine rejects every new task and session
/// (including tasks submitted into an already open session) with HTTP 409 and the suspension
/// message. Existing work is handled according to `cancel`.
#[utoipa::path(
    post,
    path = "/machine/{machine}/suspension",
    params(("machine" = String, Path, description = "Machine name")),
    request_body = SuspendRequest,
    responses(
        (status = 201, description = "Machine suspended", body = Suspension),
        (status = 404, description = "Machine not found"),
        (status = 409, description = "Machine is already suspended"),
        (status = 422, description = "`message` is empty or longer than 512 characters")
    )
)]
pub(crate) async fn suspend_machine_handler(
    State(state): State<Arc<AppState>>,
    Path(machine): Path<String>,
    Json(request): Json<SuspendRequest>,
) -> Result<(StatusCode, Json<Suspension>), (StatusCode, String)> {
    let machine_id = find_machine(&state.core_ref.lock().unwrap(), &machine)?;
    let suspension = suspend_machine(&state.core_ref, machine_id, request.message, request.cancel)
        .await
        .map_err(|e| match &e {
            RunnerError::MachineAlreadySuspended(_) => client_error(StatusCode::CONFLICT, e),
            RunnerError::InvalidSuspensionMessage(_) => {
                client_error(StatusCode::UNPROCESSABLE_ENTITY, e)
            }
            _ => internal_error(&e),
        })?;
    Ok((StatusCode::CREATED, Json(suspension)))
}

/// End a machine's suspension; it accepts new tasks and sessions again. Work failed or closed
/// by the suspension stays so.
#[utoipa::path(
    delete,
    path = "/machine/{machine}/suspension",
    params(("machine" = String, Path, description = "Machine name")),
    responses(
        (status = 200, description = "The ended suspension", body = Suspension),
        (status = 404, description = "Machine not found"),
        (status = 409, description = "Machine is not suspended")
    )
)]
pub(crate) async fn resume_machine_handler(
    State(state): State<Arc<AppState>>,
    Path(machine): Path<String>,
) -> Result<Json<Suspension>, (StatusCode, String)> {
    let machine_id = find_machine(&state.core_ref.lock().unwrap(), &machine)?;
    let suspension = resume_machine(&state.core_ref, machine_id)
        .await
        .map_err(|e| match &e {
            RunnerError::MachineNotSuspended(_) => client_error(StatusCode::CONFLICT, e),
            _ => internal_error(&e),
        })?;
    Ok(Json(suspension))
}

/// List a machine's suspensions, newest first. The active suspension, if any, is the first
/// entry and has no `resumed_at`.
#[utoipa::path(
    get,
    path = "/machine/{machine}/suspensions",
    params(("machine" = String, Path, description = "Machine name")),
    responses(
        (status = 200, description = "Suspension history", body = Vec<Suspension>),
        (status = 404, description = "Machine not found")
    )
)]
pub(crate) async fn list_machine_suspensions_handler(
    State(state): State<Arc<AppState>>,
    Path(machine): Path<String>,
) -> Result<Json<Vec<Suspension>>, (StatusCode, String)> {
    let machine_id = find_machine(&state.core_ref.lock().unwrap(), &machine)?;
    list_machine_suspensions(&state.core_ref, machine_id)
        .await
        .map(Json)
        .map_err(internal_error)
}

fn default_queue_size() -> usize {
    DEFAULT_QUEUE_SIZE
}

fn default_limit_check_interval_ms() -> u32 {
    DEFAULT_LIMIT_CHECK_INTERVAL_MS
}

fn default_max_session_time_ms() -> u64 {
    DEFAULT_MAX_SESSION_TIME_MS
}

fn default_task_time_limit_ms() -> u64 {
    DEFAULT_TASK_TIME_LIMIT_MS
}

fn default_max_clock_skew_ms() -> u64 {
    DEFAULT_MAX_CLOCK_SKEW_MS
}

/// The backend a machine dispatches its tasks to.
#[derive(serde::Deserialize, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "lowercase")]
pub(crate) enum BackendRequest {
    /// An IQM server.
    Iqm {
        /// IQM server base URL.
        url: String,
        /// Authentication token.
        token: String,
        /// Machine name on the IQM server.
        machine_name: String,
        /// Job status polling interval, in milliseconds.
        poll_interval_ms: u64,
    },
    /// The in-process test backend.
    Test,
}

/// A machine's configuration. Omitted fields get their default values.
#[derive(serde::Deserialize, utoipa::ToSchema)]
pub(crate) struct MachineSettings {
    /// Maximum number of tasks submitted to the backend at once.
    #[serde(default = "default_queue_size")]
    #[schema(default = 4)]
    queue_size: usize,
    /// How often session consumption is stored into the DB and task time limits are checked,
    /// in milliseconds.
    #[serde(default = "default_limit_check_interval_ms")]
    #[schema(default = 2000)]
    limit_check_interval_ms: u32,
    /// Maximum duration a session on this machine may request, in milliseconds.
    #[serde(default = "default_max_session_time_ms")]
    #[schema(default = 7200000)]
    max_session_time_ms: u64,
    /// Time limit of tasks submitted without an explicit `time_limit_ms`, in milliseconds.
    #[serde(default = "default_task_time_limit_ms")]
    #[schema(default = 3600000)]
    default_task_time_limit_ms: u64,
    /// Maximum assumed lag of the backend's clock behind ours, in milliseconds.
    #[serde(default = "default_max_clock_skew_ms")]
    #[schema(default = 4000)]
    max_clock_skew_ms: u64,
    backend: BackendRequest,
}

impl MachineSettings {
    fn into_configuration(self, name: String) -> MachineConfiguration {
        MachineConfiguration {
            name,
            queue_size: self.queue_size,
            limit_check_interval_ms: self.limit_check_interval_ms,
            max_session_time_ms: self.max_session_time_ms,
            default_task_time_limit_ms: self.default_task_time_limit_ms,
            max_clock_skew_ms: self.max_clock_skew_ms,
            backend: match self.backend {
                BackendRequest::Test => BackendConfig::Test,
                BackendRequest::Iqm {
                    url,
                    token,
                    machine_name,
                    poll_interval_ms,
                } => BackendConfig::Iqm(IqmBackendConfig {
                    url,
                    token,
                    machine_name,
                    poll_interval: Duration::from_millis(poll_interval_ms),
                }),
            },
        }
    }
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
pub(crate) struct CreateMachineRequest {
    /// Unique machine name.
    name: String,
    #[serde(flatten)]
    settings: MachineSettings,
}

/// A machine's backend as reported by the API (without its token).
#[derive(serde::Serialize, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "lowercase")]
pub(crate) enum BackendResponse {
    Iqm {
        url: String,
        machine_name: String,
        poll_interval_ms: u64,
    },
    Test,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
pub(crate) struct MachineResponse {
    name: String,
    queue_size: usize,
    limit_check_interval_ms: u64,
    max_session_time_ms: u64,
    default_task_time_limit_ms: u64,
    max_clock_skew_ms: u64,
    backend: BackendResponse,
    /// The active suspension, if the machine is suspended.
    suspension: Option<Suspension>,
}

impl MachineResponse {
    fn from_info(info: MachineInfo) -> MachineResponse {
        let config = info.config;
        MachineResponse {
            name: config.name,
            queue_size: config.queue_size,
            limit_check_interval_ms: config.limit_check_interval.as_millis() as u64,
            max_session_time_ms: config.max_session_time.as_millis() as u64,
            default_task_time_limit_ms: config.default_task_time_limit.as_millis() as u64,
            max_clock_skew_ms: config.max_clock_skew.as_millis() as u64,
            backend: match config.backend {
                BackendConfig::Test => BackendResponse::Test,
                BackendConfig::Iqm(iqm) => BackendResponse::Iqm {
                    url: iqm.url,
                    machine_name: iqm.machine_name,
                    poll_interval_ms: iqm.poll_interval.as_millis() as u64,
                },
            },
            suspension: info.suspension,
        }
    }
}

fn machine_config_error(e: RunnerError) -> (StatusCode, String) {
    match &e {
        RunnerError::InvalidMachineName(_) => client_error(StatusCode::NOT_FOUND, e),
        RunnerError::MachineAlreadyExists(_) => client_error(StatusCode::CONFLICT, e),
        RunnerError::InvalidMachineConfig(_) => client_error(StatusCode::UNPROCESSABLE_ENTITY, e),
        _ => internal_error(&e),
    }
}

/// List all machines, sorted by name. Backend tokens are not returned.
#[utoipa::path(
    get,
    path = "/machines",
    security(("admin_token" = [])),
    responses(
        (status = 200, description = "All machines", body = Vec<MachineResponse>),
        (status = 401, description = "Missing or invalid admin token"),
        (status = 403, description = "Admin API disabled (`ADMIN_TOKEN` not set)")
    )
)]
pub(crate) async fn list_machines_handler(
    State(state): State<Arc<AppState>>,
    _admin: AdminAuth,
) -> Json<Vec<MachineResponse>> {
    let machines = list_machines(&state.core_ref.lock().unwrap());
    Json(
        machines
            .into_iter()
            .map(MachineResponse::from_info)
            .collect(),
    )
}

/// Register a machine. It accepts tasks and sessions right away.
#[utoipa::path(
    post,
    path = "/machines",
    security(("admin_token" = [])),
    request_body = CreateMachineRequest,
    responses(
        (status = 201, description = "Machine registered", body = MachineResponse),
        (status = 401, description = "Missing or invalid admin token"),
        (status = 403, description = "Admin API disabled (`ADMIN_TOKEN` not set)"),
        (status = 409, description = "A machine with this name already exists"),
        (status = 422, description = "Invalid configuration (e.g. zero `queue_size`)")
    )
)]
pub(crate) async fn create_machine_handler(
    State(state): State<Arc<AppState>>,
    _admin: AdminAuth,
    Json(body): Json<CreateMachineRequest>,
) -> Result<(StatusCode, Json<MachineResponse>), (StatusCode, String)> {
    let config = body.settings.into_configuration(body.name);
    let info = add_machine(&state.core_ref, config)
        .await
        .map_err(machine_config_error)?;
    Ok((StatusCode::CREATED, Json(MachineResponse::from_info(info))))
}

/// Get a machine's configuration. The backend token is not returned.
#[utoipa::path(
    get,
    path = "/machine/{machine}",
    security(("admin_token" = [])),
    params(("machine" = String, Path, description = "Machine name")),
    responses(
        (status = 200, description = "Machine configuration", body = MachineResponse),
        (status = 401, description = "Missing or invalid admin token"),
        (status = 403, description = "Admin API disabled (`ADMIN_TOKEN` not set)"),
        (status = 404, description = "Machine not found")
    )
)]
pub(crate) async fn get_machine_handler(
    State(state): State<Arc<AppState>>,
    _admin: AdminAuth,
    Path(machine): Path<String>,
) -> Result<Json<MachineResponse>, (StatusCode, String)> {
    let core = state.core_ref.lock().unwrap();
    let machine_id = find_machine(&core, &machine)?;
    let info = get_machine_info(&core, machine_id).map_err(internal_error)?;
    Ok(Json(MachineResponse::from_info(info)))
}

/// Replace a machine's configuration (fields omitted from the body get their default values).
/// The machine is restarted with the new configuration, as on a service restart: queued tasks
/// and sessions are kept, tasks already handed to the backend are re-attached, and an open
/// session is closed (its unfinished tasks are cancelled). New tasks and sessions are rejected
/// with HTTP 409 while the restart is in progress.
#[utoipa::path(
    put,
    path = "/machine/{machine}",
    security(("admin_token" = [])),
    params(("machine" = String, Path, description = "Machine name")),
    request_body = MachineSettings,
    responses(
        (status = 200, description = "Machine reconfigured", body = MachineResponse),
        (status = 401, description = "Missing or invalid admin token"),
        (status = 403, description = "Admin API disabled (`ADMIN_TOKEN` not set)"),
        (status = 404, description = "Machine not found"),
        (status = 422, description = "Invalid configuration (e.g. zero `queue_size`)")
    )
)]
pub(crate) async fn update_machine_handler(
    State(state): State<Arc<AppState>>,
    _admin: AdminAuth,
    Path(machine): Path<String>,
    Json(body): Json<MachineSettings>,
) -> Result<Json<MachineResponse>, (StatusCode, String)> {
    let config = body.into_configuration(machine);
    let info = update_machine(&state.core_ref, config)
        .await
        .map_err(machine_config_error)?;
    Ok(Json(MachineResponse::from_info(info)))
}

#[derive(serde::Deserialize, utoipa::IntoParams)]
pub(crate) struct RemoveMachineQuery {
    /// Remove the machine even if it has tasks or sessions; they are deleted together with
    /// their consumption records (the consumed time is not returned to their projects).
    #[serde(default)]
    force: bool,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
pub(crate) struct MachineRemoved {
    /// Number of removed tasks.
    tasks: i64,
    /// Number of removed sessions.
    sessions: i64,
}

/// Remove a machine. Without `force`, only a machine without any tasks and sessions is
/// removed. With `force`, its tasks and sessions are deleted as well, and jobs already handed
/// to the backend get a best-effort cancel request.
#[utoipa::path(
    delete,
    path = "/machine/{machine}",
    security(("admin_token" = [])),
    params(("machine" = String, Path, description = "Machine name"), RemoveMachineQuery),
    responses(
        (status = 200, description = "Machine removed", body = MachineRemoved),
        (status = 401, description = "Missing or invalid admin token"),
        (status = 403, description = "Admin API disabled (`ADMIN_TOKEN` not set)"),
        (status = 404, description = "Machine not found"),
        (status = 409, description = "The machine has tasks or sessions and `force` is not set")
    )
)]
pub(crate) async fn remove_machine_handler(
    State(state): State<Arc<AppState>>,
    _admin: AdminAuth,
    Path(machine): Path<String>,
    Query(query): Query<RemoveMachineQuery>,
) -> Result<Json<MachineRemoved>, (StatusCode, String)> {
    let (tasks, sessions) = remove_machine(&state.core_ref, &machine, query.force)
        .await
        .map_err(|e| match &e {
            RunnerError::InvalidMachineName(_) => client_error(StatusCode::NOT_FOUND, e),
            RunnerError::MachineHasDependents { .. } => client_error(StatusCode::CONFLICT, e),
            _ => internal_error(&e),
        })?;
    Ok(Json(MachineRemoved { tasks, sessions }))
}
