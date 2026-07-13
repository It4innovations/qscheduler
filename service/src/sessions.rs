use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use runner::error::RunnerError;
use runner::reactor::{cancel_session, create_session, get_project_id_by_name, get_session_info};
use runner::{SessionConfig, SessionId, SessionInfo};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

use crate::machines::find_machine;
use crate::{AppState, internal_error};

#[derive(Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct CreateSessionParams {
    /// Name of the target machine.
    machine: String,
    /// Name of the project the session's time is charged to.
    project: String,
    /// Session lifetime in milliseconds.
    time_limit_ms: u64,
}

/// Get a session's current info (state, timings, and its machine/project).
#[utoipa::path(
    get,
    path = "/sessions/{id}",
    params(("id" = u64, Path, description = "Session ID")),
    responses(
        (status = 200, description = "Session info", body = SessionInfo),
        (status = 404, description = "Session not found")
    )
)]
pub(crate) async fn get_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> Result<Json<SessionInfo>, (StatusCode, String)> {
    let session_id = SessionId::try_from(id)
        .map_err(|_| (StatusCode::NOT_FOUND, "invalid session id".to_string()))?;
    let info = get_session_info(&state.core_ref, session_id)
        .await
        .map_err(|e| internal_error(&e))?
        .ok_or((StatusCode::NOT_FOUND, format!("session '{id}' not found")))?;
    Ok(Json(info))
}

/// Create a session — a time-bounded, exclusive reservation of one machine for one project.
/// Tasks in the session are cancelled when the session closes.
#[utoipa::path(
    post,
    path = "/sessions",
    params(CreateSessionParams),
    responses(
        (status = 201, description = "Session created", body = u64),
        (status = 402, description = "The project has exceeded its time limit, or the project is not active."),
        (status = 404, description = "Unknown `machine`."),
        (status = 422, description = "Unknown `project`, or `time_limit_ms` exceeds the machine's `--max-session-time`.")
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
        .map_err(|e| match &e {
            RunnerError::ProjectLimitExceeded(_) | RunnerError::ProjectNotActive(_) => {
                (StatusCode::PAYMENT_REQUIRED, e.to_string())
            }
            RunnerError::SessionDurationExceedsLimit { .. } => {
                (StatusCode::UNPROCESSABLE_ENTITY, e.to_string())
            }
            _ => internal_error(&e),
        })?;
    tracing::info!(session_id = session_id.as_u64(), "session created");
    Ok((StatusCode::CREATED, Json(session_id.as_u64())))
}

/// Request cancellation of a session. Cancellation is asynchronous: a queued (`"waiting"`)
/// session is removed from the queue, while an `"open"` session is closed and its tasks are
/// cancelled (both those already submitted to the backend and those still queued); in both
/// cases the session eventually reaches the `"closed"` state.
#[utoipa::path(
    delete,
    path = "/sessions/{id}",
    params(("id" = u64, Path, description = "Session ID")),
    responses(
        (status = 202, description = "Cancellation requested"),
        (status = 404, description = "Session not found"),
        (status = 409, description = "Session already closed")
    )
)]
pub(crate) async fn cancel_session_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> Result<StatusCode, (StatusCode, String)> {
    let session_id = SessionId::try_from(id)
        .map_err(|_| (StatusCode::NOT_FOUND, "invalid session id".to_string()))?;
    cancel_session(&state.core_ref, session_id)
        .await
        .map_err(|e| match &e {
            RunnerError::InvalidSession(_) => (StatusCode::NOT_FOUND, e.to_string()),
            RunnerError::SessionAlreadyClosed(_) => (StatusCode::CONFLICT, e.to_string()),
            _ => internal_error(&e),
        })?;
    tracing::info!(
        session_id = session_id.as_u64(),
        "session cancellation requested"
    );
    Ok(StatusCode::ACCEPTED)
}
