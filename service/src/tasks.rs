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
use runner::reactor::{cancel_task, get_machine_id_by_name, get_project_id_by_name, submit_task};
use runner::task::{TaskConfig, TaskId, TaskParent, TaskState};
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
        payload: body,
    })
}

/// Current state of a task.
#[derive(serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "state")]
pub(crate) enum TaskStateResponse {
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
            TaskState::Failed { error } => Self::Failed {
                error: error.clone(),
            },
            TaskState::Cancelled => Self::Cancelled,
        }
    }
}

/// Get the current state of a task.
#[utoipa::path(
    get,
    path = "/tasks/{id}",
    params(("id" = u64, Path, description = "Task ID")),
    responses(
        (status = 200, description = "Task state", body = TaskStateResponse),
        (status = 404, description = "Task not found")
    )
)]
pub(crate) async fn get_task(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> Result<Json<TaskStateResponse>, StatusCode> {
    let task_id = TaskId::try_from(id).map_err(|_| StatusCode::NOT_FOUND)?;
    let core = state.core_ref.lock().unwrap();
    let task_state = core.task_state(task_id).ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(TaskStateResponse::from(task_state)))
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
