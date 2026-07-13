use crate::{AppState, internal_error};
use axum::{
    extract::{Path, State},
    http::StatusCode,
};
use runner::MachineId;
use runner::core::Core;
use runner::reactor::get_machine_id_by_name;
use std::sync::Arc;

pub(crate) fn find_machine(core: &Core, machine: &str) -> Result<MachineId, (StatusCode, String)> {
    get_machine_id_by_name(core, machine).map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))
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
