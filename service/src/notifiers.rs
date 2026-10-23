use crate::{AdminAuth, AppState, client_error, internal_error};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use runner::NotifierInfo;
use runner::config::NotifierConfiguration;
use runner::error::RunnerError;
use runner::reactor::{add_notifier, list_notifiers, remove_notifier};
use std::sync::Arc;

#[derive(serde::Deserialize, utoipa::ToSchema)]
pub(crate) struct CreateNotifierRequest {
    /// Unique notifier name (at most 512 characters); tasks and sessions select the notifier
    /// by it in their `notify` field.
    name: String,
    /// URL to POST task completion and session open/close events to.
    url: String,
    /// Token included in every event payload.
    token: Option<String>,
}

/// List registered notifiers, sorted by name. Tokens are not returned.
#[utoipa::path(
    get,
    path = "/notifiers",
    security(("admin_token" = [])),
    responses(
        (status = 200, description = "Registered notifiers", body = Vec<NotifierInfo>),
        (status = 401, description = "Missing or invalid admin token"),
        (status = 403, description = "Admin API disabled (`ADMIN_TOKEN` not set)")
    )
)]
pub(crate) async fn list_notifiers_handler(
    State(state): State<Arc<AppState>>,
    _admin: AdminAuth,
) -> Json<Vec<NotifierInfo>> {
    Json(list_notifiers(&state.core_ref.lock().unwrap()))
}

/// Register a notifier — an HTTP callback target for task and session events. It can be
/// selected by new tasks and sessions right away.
#[utoipa::path(
    post,
    path = "/notifiers",
    security(("admin_token" = [])),
    request_body = CreateNotifierRequest,
    responses(
        (status = 201, description = "Notifier registered", body = NotifierInfo),
        (status = 401, description = "Missing or invalid admin token"),
        (status = 403, description = "Admin API disabled (`ADMIN_TOKEN` not set)"),
        (status = 409, description = "A notifier with this name already exists"),
        (status = 422, description = "Invalid name (empty or longer than 512 characters) or URL")
    )
)]
pub(crate) async fn create_notifier_handler(
    State(state): State<Arc<AppState>>,
    _admin: AdminAuth,
    Json(body): Json<CreateNotifierRequest>,
) -> Result<(StatusCode, Json<NotifierInfo>), (StatusCode, String)> {
    let config = NotifierConfiguration {
        name: body.name,
        url: body.url,
        token: body.token,
    };
    let info = add_notifier(&state.core_ref, config)
        .await
        .map_err(|e| match &e {
            RunnerError::NotifierAlreadyExists(_) => client_error(StatusCode::CONFLICT, e),
            RunnerError::InvalidNotifierName(_) | RunnerError::InvalidNotifierUrl(_) => {
                client_error(StatusCode::UNPROCESSABLE_ENTITY, e)
            }
            _ => internal_error(&e),
        })?;
    Ok((StatusCode::CREATED, Json(info)))
}

/// Remove a notifier. Events already queued for it are still delivered; tasks and sessions
/// that use it stop sending notifications.
#[utoipa::path(
    delete,
    path = "/notifiers/{name}",
    security(("admin_token" = [])),
    params(("name" = String, Path, description = "Notifier name")),
    responses(
        (status = 204, description = "Notifier removed"),
        (status = 401, description = "Missing or invalid admin token"),
        (status = 403, description = "Admin API disabled (`ADMIN_TOKEN` not set)"),
        (status = 404, description = "Notifier not found")
    )
)]
pub(crate) async fn remove_notifier_handler(
    State(state): State<Arc<AppState>>,
    _admin: AdminAuth,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    remove_notifier(&state.core_ref, &name)
        .await
        .map_err(|e| match &e {
            RunnerError::NotifierNotFound(_) => client_error(StatusCode::NOT_FOUND, e),
            _ => internal_error(&e),
        })?;
    Ok(StatusCode::NO_CONTENT)
}
