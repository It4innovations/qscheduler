use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use runner::SessionId;
use runner::reactor::{create_session, get_project_id_by_name};
use runner::{SessionConfig, SessionState};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

use crate::machines::find_machine;
use crate::{AppState, internal_error};

#[derive(Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct CreateSessionParams {
    machine: String,
    project: String,
    time_limit_ms: u64,
}

/// Current state of a session.
#[derive(serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SessionStateResponse {
    /// Session is queued and waiting for the machine to become available.
    Waiting,
    /// Session is open and ready to accept tasks.
    Open,
    /// Session has ended and no longer accepts tasks.
    Closed,
}

impl From<SessionState> for SessionStateResponse {
    fn from(s: SessionState) -> Self {
        match s {
            SessionState::Waiting => Self::Waiting,
            SessionState::Open => Self::Open,
            SessionState::Closed => Self::Closed,
        }
    }
}

#[utoipa::path(
    get,
    path = "/sessions/{id}",
    params(("id" = u64, Path, description = "Session ID")),
    responses(
        (status = 200, description = "Session state", body = SessionStateResponse),
        (status = 404, description = "Session not found")
    )
)]
pub(crate) async fn get_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> Result<Json<SessionStateResponse>, StatusCode> {
    let session_id = SessionId::try_from(id).map_err(|_| StatusCode::NOT_FOUND)?;
    let core = state.core_ref.lock().unwrap();
    let session_state = core
        .session_state(session_id)
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(SessionStateResponse::from(session_state)))
}

#[utoipa::path(
    post,
    path = "/sessions",
    params(CreateSessionParams),
    responses(
        (status = 201, description = "Session created", body = u64),
        (status = 400, description = "Missing or invalid fields")
    )
)]
pub(crate) async fn create_session_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<CreateSessionParams>,
) -> Result<(StatusCode, Json<u64>), (StatusCode, String)> {
    let machine_id = find_machine(&state.core_ref.lock().unwrap(), &params.machine)?;
    let project_id = get_project_id_by_name(&state.core_ref, &params.project)
        .await
        .map_err(|e| (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()))?;
    let config = SessionConfig {
        machine_id,
        project_id,
        time_limit: Duration::from_millis(params.time_limit_ms),
    };
    let session_id = create_session(&state.core_ref, config)
        .await
        .map_err(internal_error)?;
    tracing::info!(session_id = session_id.as_u64(), "session created");
    Ok((StatusCode::CREATED, Json(session_id.as_u64())))
}
