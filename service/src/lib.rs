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
use runner::reactor::{create_session, create_project, get_project_by_name, list_projects as list_projects_reactor, submit_task, get_project_id_by_name};
use runner::task::{TaskConfig, TaskId, TaskParent, TaskState};
use runner::{MachineId, Project, SessionId};
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
    project: Option<String>,
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
        create_task_config(&state.core, params, body).await.map_err(|e| (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()))?;
    tracing::debug!(task_config=?task, "new task");
    let task_id = submit_task(&state.core, task).await.map_err(|e| {
        match &e {
            RunnerError::InvalidMachine(_)
            | RunnerError::InvalidSession(_)
            | RunnerError::NonRunningSession(_) => (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()),
            RunnerError::ProjectLimitExceeded(_) => (StatusCode::PAYMENT_REQUIRED, e.to_string()),
            _ => internal_error(&e),
        }
    })?;
    tracing::info!(%task_id, "task submitted");
    Ok((StatusCode::CREATED, Json(task_id.as_u64())))
}

async fn create_task_config(core_ref: &CoreRef, params: CreateTaskParams, body: Bytes) -> runner::Result<TaskConfig> {
    if params.session_id.is_some() == params.project.is_some() {
        return Err(RunnerError::GenericError("Task has to fill 'project' or 'session_id' but not both".to_string()));
    }
    let session_id = params
        .session_id
        .map(SessionId::try_from)
        .transpose()
        .map_err(|_| RunnerError::GenericError("Invalid session_id".to_string()))?;
    let project_id = if let Some(name) = params.project {
        Some(get_project_id_by_name(core_ref, &name).await?)
    } else {
        None
    };
    Ok(TaskConfig {
        machine_id: MachineId::from(params.machine_id),
        parent: TaskParent::new(session_id, project_id),
        payload: body,
    })
}

/// Current state of a task.
#[derive(serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "state")]
enum TaskStateResponse {
    /// Task is queued and waiting to be assigned to a machine.
    Waiting,
    /// Task is currently executing on a machine.
    Running,
    /// Task completed successfully.
    Finished,
    /// Task failed. The `error` field contains the failure reason.
    Failed { error: String },
    /// Task was cancelled before it could finish.
    Cancelled,
}

impl From<&TaskState> for TaskStateResponse {
    fn from(s: &TaskState) -> Self {
        match s {
            TaskState::Waiting => Self::Waiting,
            TaskState::Running => Self::Running,
            TaskState::Finished => Self::Finished,
            TaskState::Failed { error } => Self::Failed { error: error.clone() },
            TaskState::Cancelled => Self::Cancelled,
        }
    }
}

#[utoipa::path(
    get,
    path = "/tasks/{id}",
    params(("id" = u64, Path, description = "Task ID")),
    responses(
        (status = 200, description = "Task state", body = TaskStateResponse),
        (status = 404, description = "Task not found")
    )
)]
async fn get_task(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> Result<Json<TaskStateResponse>, StatusCode> {
    let task_id = TaskId::try_from(id).map_err(|_| StatusCode::NOT_FOUND)?;
    let core = state.core.lock().unwrap();
    let task_state = core.task_state(task_id).ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(TaskStateResponse::from(task_state)))
}

/// Current state of a session.
#[derive(serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
enum SessionStateResponse {
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
async fn get_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> Result<Json<SessionStateResponse>, StatusCode> {
    let session_id = SessionId::try_from(id).map_err(|_| StatusCode::NOT_FOUND)?;
    let core = state.core.lock().unwrap();
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
async fn create_session_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<CreateSessionParams>,
) -> Result<(StatusCode, Json<u64>), (StatusCode, String)> {
    let config = SessionConfig {
        machine_id: MachineId::from(params.machine_id),
        time_limit: Duration::from_secs(params.time_limit_secs),
    };
    let session_id = create_session(&state.core, config)
        .await
        .map_err(|e| internal_error(e))?;
    tracing::info!(session_id = session_id.as_u64(), "session created");
    Ok((StatusCode::CREATED, Json(session_id.as_u64())))
}

#[utoipa::path(
    get,
    path = "/machine/{machine_id}/arch",
    params(("machine_id" = u32, Path, description = "Machine ID")),
    responses(
        (status = 200, description = "Architecture JSON", body = String),
        (status = 404, description = "Machine not found"),
        (status = 500, description = "Backend error")
    )
)]
async fn get_machine_arch(
    State(state): State<Arc<AppState>>,
    Path(machine_id): Path<u32>,
) -> Result<String, (StatusCode, String)> {
    let receiver = {
        let core = state.core.lock().unwrap();
        core.get_arch(MachineId::from(machine_id)).map_err(|e| match &e {
            RunnerError::InvalidMachine(_) => (StatusCode::NOT_FOUND, e.to_string()),
            _ => internal_error(&e),
        })?
    };
    receiver
        .await
        .map_err(|_| internal_error("backend channel closed"))?
        .map_err(|e| internal_error(e))
}

#[utoipa::path(
    get,
    path = "/machine/{machine_id}/calibration/{calibration}/{endpoint}",
    params(
        ("machine_id" = u32, Path, description = "Machine ID"),
        ("calibration" = String, Path, description = "Calibration name"),
        ("endpoint" = String, Path, description = "Endpoint")
    ),
    responses(
        (status = 200, description = "Calibration JSON", body = String),
        (status = 404, description = "Machine not found"),
        (status = 500, description = "Backend error")
    )
)]
async fn get_machine_calibration(
    State(state): State<Arc<AppState>>,
    Path((machine_id, calibration, endpoint)): Path<(u32, String, String)>,
) -> Result<String, (StatusCode, String)> {
    let receiver = {
        let core = state.core.lock().unwrap();
        core.get_calibration(MachineId::from(machine_id), &calibration, &endpoint)
            .map_err(|e| match &e {
                RunnerError::InvalidMachine(_) => (StatusCode::NOT_FOUND, e.to_string()),
                _ => internal_error(&e),
            })?
    };
    receiver
        .await
        .map_err(|_| internal_error("backend channel closed"))?
        .map_err(|e| internal_error(e))
}

fn internal_error(e: impl std::fmt::Display) -> (StatusCode, String) {
    let msg = e.to_string();
    tracing::error!("{msg}");
    (StatusCode::INTERNAL_SERVER_ERROR, msg)
}

fn default_active() -> bool { true }

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct CreateProjectRequest {
    name: String,
    limit_ms: i64,
    #[serde(default = "default_active")]
    #[schema(default = true)]
    active: bool,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
struct ProjectResponse {
    name: String,
    consumed_ms: i64,
    limit_ms: i64,
    active: bool,
}

impl ProjectResponse {
    pub fn from_project(project: Project) -> ProjectResponse {
        ProjectResponse {
            name: project.name,
            consumed_ms: project.consumed.as_millis() as i64,
            limit_ms: project.limit.as_millis() as i64,
            active: project.active,
        }
    }
}

#[utoipa::path(
    get,
    path = "/projects/{name}",
    params(("name" = String, Path, description = "Project name")),
    responses(
        (status = 200, description = "Project info", body = ProjectResponse),
        (status = 404, description = "Project not found")
    )
)]
async fn get_project_handler(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<ProjectResponse>, (StatusCode, String)> {
    let project = get_project_by_name(&state.core, &name)
        .await
        .map_err(|e| internal_error(&e))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("project '{name}' not found")))?;
    Ok(Json(ProjectResponse::from_project(project)))
}

#[utoipa::path(
    post,
    path = "/projects",
    request_body = CreateProjectRequest,
    responses(
        (status = 201, description = "Project created", body = ProjectResponse),
        (status = 409, description = "Project already exists")
    )
)]
async fn create_project_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateProjectRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let limit = Duration::from_millis(body.limit_ms as u64);
    create_project(&state.core, body.name, body.active, limit)
        .await
        .map_err(|e| match &e {
            RunnerError::ProjectAlreadyExists(_) => (StatusCode::CONFLICT, e.to_string()),
            _ => internal_error(&e),
        })?;
    Ok(StatusCode::CREATED)
}

#[utoipa::path(
    get,
    path = "/projects",
    responses(
        (status = 200, description = "List of projects", body = Vec<ProjectResponse>)
    )
)]
async fn list_projects_handler(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<ProjectResponse>>, (StatusCode, String)> {
    let rows = list_projects_reactor(&state.core)
        .await
        .map_err(|e| internal_error(e))?;
    Ok(Json(rows.into_iter().map(ProjectResponse::from_project).collect()))
}

#[derive(OpenApi)]
struct ApiDoc;

pub async fn run(
    version: &'static str,
    service_conf: ServiceConfiguration,
) -> runner::Result<()> {
    let pool = runner::db::create_pool().await?;
    let machines = runner::db::load_machines(&pool).await?;
    if machines.is_empty() {
        tracing::warn!("no machines found in database");
    } else {
        tracing::info!(count = machines.len(), "loaded machines from database");
        for m in &machines {
            tracing::info!(id = m.id, name = %m.name, backend = ?m.backend, "machine loaded");
        }
    }
    let runner_conf = RunnerConfiguration { machines };
    let state = Arc::new(AppState {
        version,
        core: Core::new(runner_conf, pool).await?,
    });

    let (router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(version_handler))
        .routes(routes!(create_task))
        .routes(routes!(get_task))
        .routes(routes!(get_session))
        .routes(routes!(create_session_handler))
        .routes(routes!(get_machine_arch))
        .routes(routes!(get_machine_calibration))
        .routes(routes!(create_project_handler))
        .routes(routes!(list_projects_handler))
        .routes(routes!(get_project_handler))
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
    Ok(())
}
