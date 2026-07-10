use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use runner::Project;
use runner::error::RunnerError;
use runner::reactor::{
    create_project, get_project_by_name, list_projects as list_projects_reactor, update_project,
};
use std::sync::Arc;
use std::time::Duration;

use crate::{AppState, internal_error};

fn default_active() -> bool {
    true
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
pub(crate) struct CreateProjectRequest {
    /// Unique project name.
    name: String,
    /// Time budget, in milliseconds.
    limit_ms: i64,
    /// Whether the project can accept new tasks/sessions.
    #[serde(default = "default_active")]
    #[schema(default = true)]
    active: bool,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
pub(crate) struct ProjectResponse {
    /// Unique project name.
    name: String,
    /// Time consumed so far, in milliseconds.
    consumed_ms: i64,
    /// Time budget, in milliseconds.
    limit_ms: i64,
    /// Whether the project can accept new tasks/sessions.
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

/// Get a single project by name.
#[utoipa::path(
    get,
    path = "/projects/{name}",
    params(("name" = String, Path, description = "Project name")),
    responses(
        (status = 200, description = "Project info", body = ProjectResponse),
        (status = 404, description = "Project not found")
    )
)]
pub(crate) async fn get_project_handler(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<ProjectResponse>, (StatusCode, String)> {
    let project = get_project_by_name(&state.core_ref, &name)
        .await
        .map_err(|e| internal_error(&e))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("project '{name}' not found")))?;
    Ok(Json(ProjectResponse::from_project(project)))
}

/// Register a project — a named, time-accounted budget that tasks and sessions are charged
/// against.
#[utoipa::path(
    post,
    path = "/projects",
    request_body = CreateProjectRequest,
    responses(
        (status = 201, description = "Project created; empty body"),
        (status = 409, description = "A project with this name already exists")
    )
)]
pub(crate) async fn create_project_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateProjectRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let limit = Duration::from_millis(body.limit_ms as u64);
    create_project(&state.core_ref, body.name, body.active, limit)
        .await
        .map_err(|e| match &e {
            RunnerError::ProjectAlreadyExists(_) => (StatusCode::CONFLICT, e.to_string()),
            _ => internal_error(&e),
        })?;
    Ok(StatusCode::CREATED)
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
pub(crate) struct UpdateProjectRequest {
    /// Whether the project can accept new tasks/sessions. Omit to leave unchanged.
    active: Option<bool>,
    /// Time budget, in milliseconds. Omit to leave unchanged.
    limit_ms: Option<i64>,
}

/// Update a project's `active` flag and/or time `limit_ms`. Fields omitted from the request
/// body are left unchanged. Does not affect `consumed_ms`.
#[utoipa::path(
    patch,
    path = "/projects/{name}",
    params(("name" = String, Path, description = "Project name")),
    request_body = UpdateProjectRequest,
    responses(
        (status = 200, description = "Project updated", body = ProjectResponse),
        (status = 404, description = "Project not found")
    )
)]
pub(crate) async fn update_project_handler(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(body): Json<UpdateProjectRequest>,
) -> Result<Json<ProjectResponse>, (StatusCode, String)> {
    let limit = body.limit_ms.map(|ms| Duration::from_millis(ms as u64));
    let project = update_project(&state.core_ref, &name, body.active, limit)
        .await
        .map_err(|e| match &e {
            RunnerError::ProjectNotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
            _ => internal_error(&e),
        })?;
    Ok(Json(ProjectResponse::from_project(project)))
}

/// List all projects.
#[utoipa::path(
    get,
    path = "/projects",
    responses(
        (status = 200, description = "List of projects", body = Vec<ProjectResponse>)
    )
)]
pub(crate) async fn list_projects_handler(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<ProjectResponse>>, (StatusCode, String)> {
    let rows = list_projects_reactor(&state.core_ref)
        .await
        .map_err(internal_error)?;
    Ok(Json(
        rows.into_iter()
            .map(ProjectResponse::from_project)
            .collect(),
    ))
}
