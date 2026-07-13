use crate::{AppState, internal_error};
use axum::{
    Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
};
use runner::SessionId;
use runner::core::CoreRef;
use runner::error::RunnerError;
use runner::reactor::{
    cancel_task, get_machine_id_by_name, get_project_id_by_name, get_task_info, submit_task,
};
use runner::task::{TaskConfig, TaskId, TaskInfo, TaskParent};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct CreateTaskParams {
    /// Session to associate the task with. The session must be in the `"open"` state or the
    /// task is rejected. Exactly one of `project` / `session_id` must be given.
    session_id: Option<u64>,
    /// Name of the project to charge the task's time to. Exactly one of `project` / `session_id`
    /// must be given.
    project: Option<String>,
    /// Name of the target machine.
    machine: String,
    /// Free-form identifier of the user submitting the task, stored alongside the task.
    user: String,
}

/// Submit a task for execution.
#[utoipa::path(
    post,
    path = "/tasks",
    params(CreateTaskParams),
    request_body(
        content = Vec<u8>,
        content_type = "application/octet-stream",
        description = "Raw task payload forwarded to the backend."
    ),
    responses(
        (status = 201, description = "Task created", body = u32),
        (status = 402, description = "The project has exceeded its time limit, or the project is not active."),
        (status = 422, description = "Neither or both of `project`/`session_id` given, unknown `machine`/`project`, or the session is invalid or not open.")
    )
)]
pub(crate) async fn create_task(
    State(state): State<Arc<AppState>>,
    Query(params): Query<CreateTaskParams>,
    body: Bytes,
) -> Result<(StatusCode, Json<u64>), (StatusCode, String)> {
    let task = create_task_config(&state.core_ref, params, body)
        .await
        .map_err(|e| (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()))?;
    tracing::debug!(task_config=?task, "new task");
    let task_id = submit_task(&state.core_ref, task)
        .await
        .map_err(|e| match &e {
            RunnerError::InvalidSession(_) | RunnerError::NonRunningSession(_) => {
                (StatusCode::UNPROCESSABLE_ENTITY, e.to_string())
            }
            RunnerError::ProjectLimitExceeded(_) | RunnerError::ProjectNotActive(_) => {
                (StatusCode::PAYMENT_REQUIRED, e.to_string())
            }
            _ => internal_error(&e),
        })?;
    tracing::info!(%task_id, "task submitted");
    Ok((StatusCode::CREATED, Json(task_id.as_u64())))
}

async fn create_task_config(
    core_ref: &CoreRef,
    params: CreateTaskParams,
    body: Bytes,
) -> runner::Result<TaskConfig> {
    if params.session_id.is_some() == params.project.is_some() {
        return Err(RunnerError::GenericError(
            "Task has to fill 'project' or 'session_id' but not both".to_string(),
        ));
    }
    let machine_id = get_machine_id_by_name(&core_ref.lock().unwrap(), &params.machine)?;
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
        machine_id,
        parent: TaskParent::new(session_id, project_id),
        user: params.user,
        payload: body,
    })
}

/// Get a task's current info (state, timings, and its session/project/machine/user).
#[utoipa::path(
    get,
    path = "/tasks/{id}",
    params(("id" = u64, Path, description = "Task ID")),
    responses(
        (status = 200, description = "Task info", body = TaskInfo),
        (status = 404, description = "Task not found")
    )
)]
pub(crate) async fn get_task(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> Result<Json<TaskInfo>, (StatusCode, String)> {
    let task_id =
        TaskId::try_from(id).map_err(|_| (StatusCode::NOT_FOUND, "invalid task id".to_string()))?;
    let info = get_task_info(&state.core_ref, task_id)
        .await
        .map_err(|e| internal_error(&e))?
        .ok_or((StatusCode::NOT_FOUND, format!("task '{id}' not found")))?;
    Ok(Json(info))
}

/// Request cancellation of a task. Cancellation is asynchronous: a queued task is removed from
/// the queue, while a running task is cancelled on the backend; in both cases the task
/// eventually reaches the `"cancelled"` state (poll `GET /tasks/{id}` to observe it).
#[utoipa::path(
    delete,
    path = "/tasks/{id}",
    params(("id" = u64, Path, description = "Task ID")),
    responses(
        (status = 202, description = "Cancellation requested"),
        (status = 404, description = "Task not found"),
        (status = 409, description = "Task already in a terminal state (finished, failed, or cancelled)")
    )
)]
pub(crate) async fn cancel_task_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> Result<StatusCode, (StatusCode, String)> {
    let task_id =
        TaskId::try_from(id).map_err(|_| (StatusCode::NOT_FOUND, "invalid task id".to_string()))?;
    cancel_task(&state.core_ref, task_id)
        .await
        .map_err(|e| match &e {
            RunnerError::InvalidTask(_) => (StatusCode::NOT_FOUND, e.to_string()),
            RunnerError::TaskAlreadyFinished(_) => (StatusCode::CONFLICT, e.to_string()),
            _ => internal_error(&e),
        })?;
    tracing::info!(%task_id, "task cancellation requested");
    Ok(StatusCode::ACCEPTED)
}

/// Fetch a task's raw result from the backend that ran it, forwarded verbatim (no caching,
/// no parsing).
#[utoipa::path(
    get,
    path = "/tasks/{id}/result",
    params(("id" = u64, Path, description = "Task ID")),
    responses(
        (status = 200, description = "Raw result JSON as returned by the backend", body = String),
        (status = 404, description = "Task not found"),
        (status = 409, description = "Task has not been submitted to a backend yet"),
        (status = 500, description = "Backend request failed")
    )
)]
pub(crate) async fn get_task_result(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> Result<String, (StatusCode, String)> {
    let task_id =
        TaskId::try_from(id).map_err(|_| (StatusCode::NOT_FOUND, "invalid task id".to_string()))?;
    let receiver = {
        let core = state.core_ref.lock().unwrap();
        core.get_task_result(task_id).map_err(|e| match &e {
            RunnerError::InvalidTask(_) => (StatusCode::NOT_FOUND, e.to_string()),
            RunnerError::TaskNotSubmitted(_) => (StatusCode::CONFLICT, e.to_string()),
            _ => internal_error(&e),
        })?
    };
    receiver
        .await
        .map_err(|_| internal_error("backend channel closed"))?
        .map_err(internal_error)
}

/// Fetch a named artifact produced for a task from the backend that ran it, forwarded
/// verbatim (no caching, no parsing).
#[utoipa::path(
    get,
    path = "/tasks/{id}/artifacts/{name}",
    params(
        ("id" = u64, Path, description = "Task ID"),
        ("name" = String, Path, description = "Artifact name")
    ),
    responses(
        (status = 200, description = "Raw artifact JSON as returned by the backend", body = String),
        (status = 404, description = "Task not found"),
        (status = 409, description = "Task has not been submitted to a backend yet"),
        (status = 500, description = "Backend request failed")
    )
)]
pub(crate) async fn get_task_artifact(
    State(state): State<Arc<AppState>>,
    Path((id, name)): Path<(u64, String)>,
) -> Result<String, (StatusCode, String)> {
    let task_id =
        TaskId::try_from(id).map_err(|_| (StatusCode::NOT_FOUND, "invalid task id".to_string()))?;
    let receiver = {
        let core = state.core_ref.lock().unwrap();
        core.get_task_artifact(task_id, &name)
            .map_err(|e| match &e {
                RunnerError::InvalidTask(_) => (StatusCode::NOT_FOUND, e.to_string()),
                RunnerError::TaskNotSubmitted(_) => (StatusCode::CONFLICT, e.to_string()),
                _ => internal_error(&e),
            })?
    };
    receiver
        .await
        .map_err(|_| internal_error("backend channel closed"))?
        .map_err(internal_error)
}
