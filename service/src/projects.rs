use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use runner::Project;
use runner::error::RunnerError;
use runner::reactor::{
    create_project, get_project_by_name, list_projects as list_projects_reactor,
};
use std::sync::Arc;
use std::time::Duration;

use crate::{AppState, internal_error};

fn default_active() -> bool {
    true
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
pub(crate) struct CreateProjectRequest {
    name: String,
    limit_ms: i64,
    #[serde(default = "default_active")]
    #[schema(default = true)]
    active: bool,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
pub(crate) struct ProjectResponse {
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

#[utoipa::path(
    post,
    path = "/projects",
    request_body = CreateProjectRequest,
    responses(
        (status = 201, description = "Project created", body = ProjectResponse),
        (status = 409, description = "Project already exists")
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
