use crate::{AppState, client_error, internal_error};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use runner::core::Core;
use runner::error::RunnerError;
use runner::reactor::{
    get_machine_id_by_name, list_machine_suspensions, resume_machine, suspend_machine,
};
use runner::{BackendVersion, MachineId, SuspendCancel, Suspension};
use std::sync::Arc;

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
