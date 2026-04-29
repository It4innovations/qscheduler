pub mod config;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{Json, extract::{Multipart, State}, http::StatusCode};
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

/// Task configuration fields, sent as the `metadata` JSON part of a multipart request.
#[derive(Deserialize, utoipa::ToSchema)]
struct CreateTaskMetadata {
    #[schema(example = 0)]
    machine_id: u32,
    #[schema(example = 1)]
    repeats: u32,
    #[schema(example = 30)]
    max_compile_time_secs: u64,
    #[schema(example = 60)]
    max_waiting_time_secs: u64,
    #[schema(example = 120)]
    max_compute_time_secs: u64,
    #[schema(example = "https://example.com/callback")]
    callback_url: Option<String>,
    #[schema(example = "secret-token")]
    callback_token: Option<String>,
}

/// Multipart form for creating a task: a JSON `metadata` part and a binary `payload` part.
#[derive(utoipa::ToSchema)]
struct CreateTaskRequest {
    metadata: CreateTaskMetadata,
    #[schema(value_type = String, format = Binary, content_media_type = "application/octet-stream")]
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
    request_body(content_type = "multipart/form-data", content = inline(CreateTaskRequest)),
    responses(
        (status = 201, description = "Task created", body = u32),
        (status = 400, description = "Missing or invalid multipart parts")
    )
)]
async fn create_task(
    State(state): State<Arc<AppState>>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<u32>), StatusCode> {
    let mut metadata: Option<CreateTaskMetadata> = None;
    let mut payload: Option<Vec<u8>> = None;

    while let Some(field) = multipart.next_field().await.map_err(|_| StatusCode::BAD_REQUEST)? {
        match field.name() {
            Some("metadata") => {
                let bytes = field.bytes().await.map_err(|_| StatusCode::BAD_REQUEST)?;
                metadata = Some(serde_json::from_slice(&bytes).map_err(|_| StatusCode::BAD_REQUEST)?);
            }
            Some("payload") => {
                let bytes = field.bytes().await.map_err(|_| StatusCode::BAD_REQUEST)?;
                payload = Some(bytes.to_vec());
            }
            _ => {}
        }
    }

    let metadata = metadata.ok_or(StatusCode::BAD_REQUEST)?;
    let payload = payload.unwrap_or_default();

    let task = TaskConfig {
        machine_id: MachineId::from(metadata.machine_id),
        repeats: metadata.repeats,
        max_compile_time: Duration::from_secs(metadata.max_compile_time_secs),
        max_waiting_time: Duration::from_secs(metadata.max_waiting_time_secs),
        max_compute_time: Duration::from_secs(metadata.max_compute_time_secs),
        callback_url: metadata.callback_url,
        callback_token: metadata.callback_token,
        payload: Arc::from(payload),
    };

    let task_id = {
        let mut core = state.core.lock().unwrap();
        submit_task(&mut core, task)
    };

    tracing::info!(task_id = task_id.as_u32(), "task submitted");

    Ok((StatusCode::CREATED, Json(task_id.as_u32())))
}

#[derive(OpenApi)]
#[openapi(components(schemas(CreateTaskRequest, CreateTaskMetadata)))]
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
