use crate::{AppState, client_error, internal_error};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use runner::core::Core;
use runner::reactor::get_machine_id_by_name;
use runner::{BackendVersion, MachineId};
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
