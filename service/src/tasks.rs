use crate::{AppState, client_error, client_warn, internal_error};
use axum::{
    Json,
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{
        HeaderMap, StatusCode,
        header::{ACCEPT, CONTENT_TYPE},
    },
    response::Response,
};
use runner::BackendStream;
use runner::SessionId;
use runner::core::CoreRef;
use runner::error::RunnerError;
use runner::reactor::{
    cancel_task, get_machine_default_task_time_limit, get_machine_id_by_name,
    get_project_id_by_name, get_task_info, resolve_notifier, submit_task,
};
use runner::task::{TaskConfig, TaskId, TaskInfo, TaskKind, TaskParent};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

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
    /// Maximum execution time in milliseconds, counted from when the backend starts running
    /// the task (queue time does not count). When exceeded, the task is cancelled on the backend
    /// and ends `failed` with the error `Task time limit exceeded`. The limit is checked every
    /// machine `limit_check_interval_ms` against a running-time estimate that never exceeds the
    /// true time (see the machine's `max_clock_skew_ms`), so a task is never stopped early but
    /// may overrun slightly. Defaults to the machine's `default_task_time_limit_ms`.
    time_limit_ms: Option<u64>,
    /// Name of the notifier to send the task's completion event to. No notification is sent
    /// when omitted.
    notify: Option<String>,
    /// What the payload describes: `circuit` (default) or `pulse`. Pulse tasks are only
    /// accepted by machines with pulse-level access enabled.
    #[param(inline)]
    kind: Option<TaskKind>,
}

/// Rejects a body whose declared `Content-Type` clearly doesn't match `kind`;
/// `application/octet-stream` (or no content type) is accepted for both.
fn check_content_type(kind: TaskKind, headers: &HeaderMap) -> runner::Result<()> {
    let Some(content_type) = headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok()) else {
        return Ok(());
    };
    let mime = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let expected = match mime.as_str() {
        "application/json" => TaskKind::Circuit,
        "application/protobuf" | "application/x-protobuf" => TaskKind::Pulse,
        _ => return Ok(()),
    };
    if expected != kind {
        return Err(RunnerError::GenericError(format!(
            "Content-Type '{content_type}' does not match task kind '{}'",
            kind.as_str()
        )));
    }
    Ok(())
}

/// Submit a task for execution.
///
/// The payload is forwarded to the backend verbatim. For IQM machines, a `circuit` task carries
/// a JSON `CircuitJobDefinition`; a `pulse` task carries a protobuf `RunDefinition`, e.g.
/// compiled with IQM Pulla (`IQMClient.get_standard_compiler()`, compile, then
/// `serialize_run_definition(rd).SerializeToString()`). The compiler's inputs are available
/// from `GET /machine/{machine}/artifacts/{name}` and `GET /machine/{machine}/calibration/{calibration}`.
/// Fetch a pulse task's results from `GET /tasks/{id}/artifacts/sweep_results` (protobuf,
/// decode with `deserialize_sweep_results`).
#[utoipa::path(
    post,
    path = "/tasks",
    params(CreateTaskParams),
    request_body(
        content(
            (Vec<u8> = "application/octet-stream"),
            (Vec<u8> = "application/json"),
            (Vec<u8> = "application/protobuf"),
        ),
        description = "Raw task payload forwarded to the backend: JSON for `circuit` tasks, protobuf for `pulse` tasks. A JSON content type with `kind=pulse`, or a protobuf one with `kind=circuit`, is rejected. For the `iqm` backend, a JSON circuit description or a protobuf `RunDefinition` accepted by the IQM API. For the `test` backend, a JSON object `{\"outcome\": {\"type\": \"Ok\"}, \"result\": \"42\", \"artifacts\": {\"log\": \"...\"}, \"submit_time\": 0.5, \"compute_time\": 1.5}`: `outcome` (required) is `{\"type\": \"Ok\"}` or `{\"type\": \"Fail\", \"message\": \"...\"}`; `result` (default `\"\"`) is what `GET /tasks/{id}/result` returns; `artifacts` (default `{}`) maps names to the values `GET /tasks/{id}/artifacts/{name}` returns; `submit_time` and `compute_time` (default 0) are seconds to wait before acknowledging the submission and before reporting the result."
    ),
    responses(
        (status = 201, description = "Task created", body = u32),
        (status = 402, description = "The project has exceeded its time limit, or the project is not active."),
        (status = 409, description = "The machine is suspended (the body contains the suspension message), or is being reconfigured or removed."),
        (status = 422, description = "Neither or both of `project`/`session_id` given, unknown `machine`/`project`/`notify`, `time_limit_ms` is zero, the session is invalid or not open, `kind=pulse` on a machine without pulse-level access, or a `Content-Type` that doesn't match `kind`.")
    )
)]
pub(crate) async fn create_task(
    State(state): State<Arc<AppState>>,
    Query(params): Query<CreateTaskParams>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<u64>), (StatusCode, String)> {
    let task = create_task_config(&state.core_ref, params, &headers, body)
        .await
        .map_err(|e| client_error(StatusCode::UNPROCESSABLE_ENTITY, e))?;
    tracing::debug!(task_config=?task, "new task");
    let task_id = submit_task(&state.core_ref, task)
        .await
        .map_err(|e| match &e {
            RunnerError::InvalidSession(_)
            | RunnerError::NonRunningSession(_)
            | RunnerError::PulseAccessDenied(_) => {
                client_error(StatusCode::UNPROCESSABLE_ENTITY, e)
            }
            RunnerError::ProjectLimitExceeded(_) | RunnerError::ProjectNotActive(_) => {
                client_warn(StatusCode::PAYMENT_REQUIRED, e)
            }
            RunnerError::MachineSuspended { .. } | RunnerError::MachineUnavailable(_) => {
                client_error(StatusCode::CONFLICT, e)
            }
            _ => internal_error(&e),
        })?;
    tracing::info!(%task_id, "task submitted");
    Ok((StatusCode::CREATED, Json(task_id.as_u64())))
}

async fn create_task_config(
    core_ref: &CoreRef,
    params: CreateTaskParams,
    headers: &HeaderMap,
    body: Bytes,
) -> runner::Result<TaskConfig> {
    let kind = params.kind.unwrap_or_default();
    check_content_type(kind, headers)?;
    if params.session_id.is_some() == params.project.is_some() {
        return Err(RunnerError::GenericError(
            "Task has to fill 'project' or 'session_id' but not both".to_string(),
        ));
    }
    if params.time_limit_ms == Some(0) {
        return Err(RunnerError::GenericError(
            "'time_limit_ms' must be positive".to_string(),
        ));
    }
    let (machine_id, time_limit, notifier) = {
        let core = core_ref.lock().unwrap();
        let machine_id = get_machine_id_by_name(&core, &params.machine)?;
        let default = get_machine_default_task_time_limit(&core, machine_id);
        let time_limit = params.time_limit_ms.map_or(default, Duration::from_millis);
        let notifier = resolve_notifier(&core, params.notify.as_deref())?;
        (machine_id, time_limit, notifier)
    };
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
        kind,
        payload: body,
        time_limit,
        notifier,
    })
}

/// Get a task's current info (state, timings, and its session/project/machine/user). Finished
/// tasks remain available after a service restart.
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
        TaskId::try_from(id).map_err(|_| client_error(StatusCode::NOT_FOUND, "invalid task id"))?;
    let info = get_task_info(&state.core_ref, task_id)
        .await
        .map_err(|e| internal_error(&e))?
        .ok_or_else(|| client_error(StatusCode::NOT_FOUND, format!("task '{id}' not found")))?;
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
        TaskId::try_from(id).map_err(|_| client_error(StatusCode::NOT_FOUND, "invalid task id"))?;
    cancel_task(&state.core_ref, task_id)
        .await
        .map_err(|e| match &e {
            RunnerError::InvalidTask(_) => client_error(StatusCode::NOT_FOUND, e),
            RunnerError::TaskAlreadyFinished(_) => client_error(StatusCode::CONFLICT, e),
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
        (status = 200, description = "Raw result as returned by the backend, with the backend's `Content-Type`: the full job-status JSON document for `iqm`, the payload's `result` string for `test`", body = String),
        (status = 404, description = "Task not found"),
        (status = 409, description = "Task has not been submitted to a backend yet"),
        (status = 500, description = "Backend request failed")
    )
)]
pub(crate) async fn get_task_result(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> Result<Response, (StatusCode, String)> {
    let task_id =
        TaskId::try_from(id).map_err(|_| client_error(StatusCode::NOT_FOUND, "invalid task id"))?;
    let future = {
        let core = state.core_ref.lock().unwrap();
        core.get_task_result(task_id).map_err(|e| match &e {
            RunnerError::InvalidTask(_) => client_error(StatusCode::NOT_FOUND, e),
            RunnerError::TaskNotSubmitted(_) => client_error(StatusCode::CONFLICT, e),
            _ => internal_error(&e),
        })?
    };
    let stream = future.await.map_err(internal_error)?;
    Ok(stream_response(stream))
}

/// Forwards backend bytes with the backend's content type (`application/octet-stream` if it
/// sent none).
pub(crate) fn stream_response(stream: BackendStream) -> Response {
    Response::builder()
        .header(
            CONTENT_TYPE,
            stream
                .content_type
                .as_deref()
                .unwrap_or("application/octet-stream"),
        )
        .body(Body::from_stream(stream.stream))
        .unwrap()
}

/// The request's `Accept` header, to forward to the backend.
pub(crate) fn accept_header(headers: &HeaderMap) -> Option<&str> {
    headers.get(ACCEPT).and_then(|v| v.to_str().ok())
}

/// Fetch a named artifact produced for a task from the backend that ran it, forwarded
/// verbatim (no caching, no parsing). The request's `Accept` header is forwarded too.
#[utoipa::path(
    get,
    path = "/tasks/{id}/artifacts/{name}",
    params(
        ("id" = u64, Path, description = "Task ID"),
        ("name" = String, Path, description = "Artifact name")
    ),
    responses(
        (status = 200, description = "Raw artifact as returned by the backend, with the backend's `Content-Type` (e.g. protobuf for a pulse task's `sweep_results`)", body = String),
        (status = 404, description = "Task not found, or the backend has no artifact of this name"),
        (status = 409, description = "Task has not been submitted to a backend yet"),
        (status = 500, description = "Backend request failed")
    )
)]
pub(crate) async fn get_task_artifact(
    State(state): State<Arc<AppState>>,
    Path((id, name)): Path<(u64, String)>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    let task_id =
        TaskId::try_from(id).map_err(|_| client_error(StatusCode::NOT_FOUND, "invalid task id"))?;
    let future = {
        let core = state.core_ref.lock().unwrap();
        core.get_task_artifact(task_id, &name, accept_header(&headers))
            .map_err(|e| match &e {
                RunnerError::InvalidTask(_) => client_error(StatusCode::NOT_FOUND, e),
                RunnerError::TaskNotSubmitted(_) => client_error(StatusCode::CONFLICT, e),
                _ => internal_error(&e),
            })?
    };
    let stream = future.await.map_err(|e| match &e {
        RunnerError::UnknownArtifact(_) => client_error(StatusCode::NOT_FOUND, e),
        _ => internal_error(&e),
    })?;
    Ok(stream_response(stream))
}
