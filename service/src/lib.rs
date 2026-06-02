pub mod config;

use std::sync::Arc;
use std::time::Duration;

use crate::config::ServiceConfiguration;
use axum::{
    Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
};
use runner::config::RunnerConfiguration;
use runner::core::{Core, CoreRef};
use runner::error::RunnerError;
use runner::reactor::{create_session, submit_task};
use runner::task::{TaskConfig, TaskId, TaskState};
use runner::{MachineId, SessionId};
use runner::{SessionConfig, SessionState};
use serde::Deserialize;
use tokio::net::TcpListener;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

struct AppState {
    version: &'static str,
    core: CoreRef,
}

#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
struct CreateTaskParams {
    session_id: Option<u64>,
    machine_id: u32,
}

#[derive(Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
struct CreateSessionParams {
    machine_id: u32,
    time_limit_secs: u64,
}

#[utoipa::path(
    get,
    path = "/version",
    responses(
        (status = 200, description = "Service version", body = String)
    )
)]
async fn version_handler(State(state): State<Arc<AppState>>) -> String {
    format!("qscheduler v{}", state.version)
}

#[utoipa::path(
    post,
    path = "/tasks",
    params(CreateTaskParams),
    request_body(content = Vec<u8>, content_type = "application/octet-stream"),
    responses(
        (status = 201, description = "Task created", body = u32),
        (status = 400, description = "Missing or invalid fields")
    )
)]
async fn create_task(
    State(state): State<Arc<AppState>>,
    Query(params): Query<CreateTaskParams>,
    body: Bytes,
) -> Result<(StatusCode, Json<u64>), (StatusCode, String)> {
    let task =
        create_task_config(params, body).map_err(|e| (StatusCode::UNPROCESSABLE_ENTITY, e))?;
    tracing::debug!(task_config=?task, "new task");
    let task_id = submit_task(Arc::clone(&state.core), task).map_err(|e| {
        let status = match &e {
            RunnerError::InvalidMachine(_)
            | RunnerError::InvalidSession(_)
            | RunnerError::NonRunningSession(_) => StatusCode::UNPROCESSABLE_ENTITY,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, e.to_string())
    })?;
    tracing::info!(%task_id, "task submitted");
    Ok((StatusCode::CREATED, Json(task_id.as_u64())))
}

fn create_task_config(params: CreateTaskParams, body: Bytes) -> Result<TaskConfig, String> {
    Ok(TaskConfig {
        machine_id: MachineId::from(params.machine_id),
        session_id: params
            .session_id
            .map(SessionId::try_from)
            .transpose()
            .map_err(|_| "Invalid session_id")?,
        payload: body,
    })
}

#[utoipa::path(
    get,
    path = "/tasks/{id}",
    params(("id" = u32, Path, description = "Task ID")),
    responses(
        (status = 200, description = "Task state", body = String),
        (status = 404, description = "Task not found")
    )
)]
async fn get_task(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> Result<Json<TaskState>, StatusCode> {
    let task_id = TaskId::try_from(id).map_err(|_| StatusCode::NOT_FOUND)?;
    let core = state.core.lock().unwrap();
    let task_state = core.task_state(task_id).ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(task_state.clone()))
}

#[utoipa::path(
    get,
    path = "/sessions/{id}",
    params(("id" = u64, Path, description = "Session ID")),
    responses(
        (status = 200, description = "Session state", body = String),
        (status = 404, description = "Session not found")
    )
)]
async fn get_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> Result<Json<SessionState>, StatusCode> {
    let session_id = SessionId::try_from(id).map_err(|_| StatusCode::NOT_FOUND)?;
    let core = state.core.lock().unwrap();
    let session_state = core
        .session_state(session_id)
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(session_state))
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
async fn create_session_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<CreateSessionParams>,
) -> Result<(StatusCode, Json<u64>), (StatusCode, String)> {
    let config = SessionConfig {
        machine_id: MachineId::from(params.machine_id),
        time_limit: Duration::from_secs(params.time_limit_secs),
    };
    let session_id = create_session(Arc::clone(&state.core), config)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    tracing::info!(session_id = session_id.as_u64(), "session created");
    Ok((StatusCode::CREATED, Json(session_id.as_u64())))
}

#[derive(OpenApi)]
struct ApiDoc;

pub async fn run(
    version: &'static str,
    service_conf: ServiceConfiguration,
    runner_conf: RunnerConfiguration,
) {
    let state = Arc::new(AppState {
        version,
        core: Core::new(runner_conf),
    });

    let (router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(version_handler))
        .routes(routes!(create_task))
        .routes(routes!(get_task))
        .routes(routes!(get_session))
        .routes(routes!(create_session_handler))
        .with_state(state)
        .split_for_parts();

    let app = router.route(
        "/api-docs/openapi.json",
        get(move || async move { axum::Json(api) }),
    );

    let port = service_conf.port;
    let listener = TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap_or_else(|_| panic!("failed to bind 0.0.0.0:{port}"));

    tracing::info!("listening on {}", listener.local_addr().unwrap());

    axum::serve(listener, app).await.expect("server error");
}
