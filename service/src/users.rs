use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use runner::UserConsumption;
use runner::reactor::{get_user_consumption, list_users};
use std::sync::Arc;

use crate::{AppState, client_error, internal_error};

/// List the names of all known users, sorted. A user becomes known by submitting a task or
/// owning a session.
#[utoipa::path(
    get,
    path = "/users",
    responses(
        (status = 200, description = "Names of all known users", body = Vec<String>)
    )
)]
pub(crate) async fn list_users_handler(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<String>>, (StatusCode, String)> {
    let names = list_users(&state.core_ref).await.map_err(internal_error)?;
    Ok(Json(names))
}

/// Get the time consumed by a user, per project and machine, sorted by project and machine.
///
/// A user is charged the time of the sessions it owns and the execution time of its tasks outside
/// a session; tasks inside a session are not charged to their user. Finished tasks and
/// checkpointed session time are included, while running tasks are included only once they
/// finish. Removing a machine removes its entries. A combination the user has not consumed any
/// time on is not listed.
#[utoipa::path(
    get,
    path = "/users/{name}/consumption",
    params(("name" = String, Path, description = "User name")),
    responses(
        (status = 200, description = "The user's consumed time", body = Vec<UserConsumption>),
        (status = 404, description = "User not found")
    )
)]
pub(crate) async fn get_user_consumption_handler(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<Vec<UserConsumption>>, (StatusCode, String)> {
    let consumption = get_user_consumption(&state.core_ref, &name)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| client_error(StatusCode::NOT_FOUND, format!("user '{name}' not found")))?;
    Ok(Json(consumption))
}
