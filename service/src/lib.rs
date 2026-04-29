pub mod config;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{Json, extract::State, http::StatusCode};
use serde::Deserialize;
use tokio::net::TcpListener;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;
use utoipa_swagger_ui::SwaggerUi;
use runner::config::RunnerConfiguration;
use runner::core::Core;
use runner::machine::MachineId;
use runner::reactor::submit_task;
use runner::task::{TaskConfig};
use crate::config::ServiceConfiguration;

struct AppState {
    version: &'static str,
    core: Mutex<Core>,
}

#[derive(Deserialize, utoipa::ToSchema)]
struct CreateTaskRequest {
    machine_id: u32,
    repeats: u32,
    max_compile_time_secs: u64,
    max_waiting_time_secs: u64,
    max_compute_time_secs: u64,
    callback_url: Option<String>,
    callback_token: Option<String>,
    payload: Vec<u8>,
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
    request_body = CreateTaskRequest,
    responses(
        (status = 201, description = "Task created", body = u32)
    )
)]
async fn create_task(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateTaskRequest>,
) -> (StatusCode, Json<u32>) {
    let task = TaskConfig {
        machine_id: MachineId::from(req.machine_id),
        repeats: req.repeats,
        max_compile_time: Duration::from_secs(req.max_compile_time_secs),
        max_waiting_time: Duration::from_secs(req.max_waiting_time_secs),
        max_compute_time: Duration::from_secs(req.max_compute_time_secs),
        callback_url: req.callback_url,
        callback_token: req.callback_token,
        payload: Arc::from(req.payload)
    };

    let task_id = {
        let mut core = state.core.lock().unwrap();
        submit_task(&mut core, task)
    };

    tracing::info!(task_id = task_id.as_u32(), "task submitted");

    (StatusCode::CREATED, Json(task_id.as_u32()))
}

#[derive(OpenApi)]
#[openapi(components(schemas(CreateTaskRequest)))]
struct ApiDoc;

pub async fn run(version: &'static str, service_conf: ServiceConfiguration, runner_conf: RunnerConfiguration) {
    let state = Arc::new(AppState {
        version,
        core: Mutex::new(Core::new(runner_conf)),
    });

    let (router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(version_handler))
        .routes(routes!(create_task))
        .with_state(state)
        .split_for_parts();

    let app = router
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", api));
    let port = service_conf.port;
    let listener = TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect(&format!("failed to bind 0.0.0.0:{port}"));

    tracing::info!("listening on {}", listener.local_addr().unwrap());

    axum::serve(listener, app)
        .await
        .expect("server error");
}
