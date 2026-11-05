use axum::{Json, extract::State, http::StatusCode};
use runner::DebugSchedulerState;
use runner::reactor::get_debug_state;
use std::sync::Arc;

use crate::{AdminAuth, AppState, internal_error};

/// Get the internal state of the scheduler: per machine, each user's queue, usage and task average.
///
/// **For internal debugging only. The format is not guaranteed to be stable across versions**, and
/// a change to it is not treated as a breaking API change, so don't build on it.
///
/// Requires the admin token. Reading the state doesn't change it, so usage values are as of the
/// machine's last decay (see `secs_since_decay`), which are the values the scheduler compares.
#[utoipa::path(
    get,
    path = "/debug/scheduler",
    security(("admin_token" = [])),
    responses(
        (status = 200, description = "Scheduler state; the format can change between versions", body = DebugSchedulerState),
        (status = 401, description = "Missing or invalid admin token"),
        (status = 403, description = "Admin API disabled (`ADMIN_TOKEN` not set)"),
        (status = 500, description = "Database error while resolving user names")
    )
)]
pub(crate) async fn get_debug_scheduler_handler(
    _admin: AdminAuth,
    State(state): State<Arc<AppState>>,
) -> Result<Json<DebugSchedulerState>, (StatusCode, String)> {
    let debug_state = get_debug_state(&state.core_ref)
        .await
        .map_err(internal_error)?;
    Ok(Json(debug_state))
}
