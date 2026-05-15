pub mod config;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{Json, body::Bytes, extract::{Path, Query, State}, http::StatusCode, routing::get};
use serde::Deserialize;
use tokio::net::TcpListener;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;
use runner::config::RunnerConfiguration;
use runner::core::Core;
use runner::machine::MachineId;
use runner::reactor::submit_task;
use runner::task::{TaskConfig, TaskId, TaskState};
use crate::config::ServiceConfiguration;

struct AppState {
    version: &'static str,
    core: Arc<Mutex<Core>>,
}

#[derive(Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
struct CreateTaskParams {
    machine_id: u32,
    repeats: u32,
    max_waiting_time_secs: u64,
    max_compute_time_secs: u64,
    callback_url: Option<String>,
    callback_token: Option<String>,
}

#[utoipa::path(
    get,
    path = "/version",
    responses(
        (status = 200, description = "Service version", body = String)
    )
)]
async fn version_handler(State(state): State<Arc<AppState>>) -> String {
    format!("qscheduler {}", state.version)
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
) -> (StatusCode, Json<u32>) {
    let task = TaskConfig {
        machine_id: MachineId::from(params.machine_id),
        repeats: params.repeats,
        max_waiting_time: Duration::from_secs(params.max_waiting_time_secs),
        max_compute_time: Duration::from_secs(params.max_compute_time_secs),
        callback_url: params.callback_url,
        callback_token: params.callback_token,
        payload: Arc::from(body.as_ref()),
    };

    let task_id = submit_task(Arc::clone(&state.core), task);
    tracing::info!(task_id = task_id.as_u32(), "task submitted");
    (StatusCode::CREATED, Json(task_id.as_u32()))
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
    Path(id): Path<u32>,
) -> Result<Json<TaskState>, StatusCode> {
    let task_id = TaskId::try_from(id).map_err(|_| StatusCode::NOT_FOUND)?;
    let task_state = state.core.lock().unwrap().task_state(task_id).ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(task_state))
}

#[derive(OpenApi)]
struct ApiDoc;

pub async fn run(version: &'static str, service_conf: ServiceConfiguration, runner_conf: RunnerConfiguration) {
    let state = Arc::new(AppState {
        version,
        core: Arc::new(Mutex::new(Core::new(runner_conf))),
    });

    let (router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(version_handler))
        .routes(routes!(create_task))
        .routes(routes!(get_task))
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

    axum::serve(listener, app)
        .await
        .expect("server error");
}
