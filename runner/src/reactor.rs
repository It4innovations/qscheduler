use crate::core::{Core, CoreRef, CoreSplitMut};
use crate::error::RunnerError;
use crate::project::{Project, ProjectId};
use crate::session::{SessionConfig, SessionId, SessionState};
use crate::task::{TaskConfig, TaskId};
use crate::{MachineId, db};
use std::time::Duration;

pub async fn submit_task(core_ref: &CoreRef, config: TaskConfig) -> crate::Result<TaskId> {
    tracing::debug!("Submitting task");
    let pool = {
        let core = core_ref.lock().unwrap();
        core.validate_task_config(&config)?;
        core.pool().clone()
    };
    let task_id = db::insert_task(&pool, &config).await?;
    tracing::debug!(%task_id, "New task");
    let result = core_ref.lock().unwrap().add_task(task_id, config);
    if result.is_err() {
        tracing::debug!("Deleting task after failed submit");
        db::delete_task(&pool, task_id).await;
    }
    result
}

pub async fn cancel_task(core_ref: &CoreRef, task_id: TaskId) -> crate::Result<()> {
    let mut core = core_ref.lock().unwrap();
    let CoreSplitMut {
        task_map,
        machine_map,
        ..
    } = core.split_mut();
    let task = task_map
        .find_task(task_id)
        .ok_or(RunnerError::InvalidTask(task_id))?;
    if task.state().is_final() {
        return Err(RunnerError::TaskAlreadyFinished(task_id));
    }
    let machine_id = task.config().machine_id;
    let machine = machine_map.get_machine_mut(machine_id);
    machine.request_task_cancel(task_id);
    machine.wake_launcher();
    Ok(())
}

pub async fn cancel_session(core_ref: &CoreRef, session_id: SessionId) -> crate::Result<()> {
    let mut core = core_ref.lock().unwrap();
    let CoreSplitMut {
        session_map,
        machine_map,
        ..
    } = core.split_mut();
    let session = session_map
        .find_session(session_id)
        .ok_or(RunnerError::InvalidSession(session_id))?;
    if matches!(session.state, SessionState::Closed) {
        return Err(RunnerError::SessionAlreadyClosed(session_id));
    }
    let machine_id = session.config.machine_id;
    let machine = machine_map.get_machine_mut(machine_id);
    machine.request_session_cancel(session_id);
    machine.wake_launcher();
    Ok(())
}

pub async fn create_session(core_ref: &CoreRef, config: SessionConfig) -> crate::Result<SessionId> {
    let pool = {
        let core = core_ref.lock().unwrap();
        core.validate_session_config(&config)?;
        core.pool().clone()
    };
    let session_id = db::insert_session(&pool, &config).await?;
    tracing::debug!(%session_id, "New session");
    let result = core_ref.lock().unwrap().add_session(session_id, config);
    if result.is_err() {
        tracing::debug!("Deleting session after failed submit");
        db::delete_session(&pool, session_id).await;
    }
    result
}

pub async fn create_project(
    core_ref: &CoreRef,
    name: String,
    active: bool,
    limit: Duration,
) -> crate::Result<ProjectId> {
    let pool = core_ref.lock().unwrap().pool().clone();
    tracing::debug!(
        name,
        active_ms = active,
        limit_ms = limit.as_millis(),
        "New project"
    );
    let id = db::insert_project(&pool, name.as_str(), active, limit).await?;
    let mut core = core_ref.lock().unwrap();
    if core
        .split()
        .project_map
        .find_project_by_name(&name)
        .is_none()
    {
        core.split_mut().project_map.add_project(Project {
            id,
            name,
            active,
            consumed: Duration::ZERO,
            limit,
        });
    }
    Ok(id)
}

pub async fn list_projects(core_ref: &CoreRef) -> crate::Result<Vec<Project>> {
    tracing::debug!("Listing projects");
    let pool = core_ref.lock().unwrap().pool().clone();
    db::list_projects(&pool).await
}

pub async fn get_project_by_name(core_ref: &CoreRef, name: &str) -> crate::Result<Option<Project>> {
    let cached = {
        let core = core_ref.lock().unwrap();
        let map = &core.split().project_map;
        map.find_project_by_name(name).cloned()
    };
    if let Some(project) = cached {
        return Ok(Some(project));
    }
    let pool = core_ref.lock().unwrap().pool().clone();
    let project = db::find_project_by_name(&pool, name).await?;
    if let Some(p) = &project {
        let mut core = core_ref.lock().unwrap();
        // Re-check under lock: project may have been cached (and updated) while we
        // were fetching from DB. Prefer the cached version to avoid overwriting a
        // more recent consumed value with a stale one from the DB read.
        if let Some(cached) = core.split().project_map.find_project_by_name(name).cloned() {
            return Ok(Some(cached));
        }
        core.split_mut().project_map.add_project(p.clone());
    }
    Ok(project)
}

pub fn get_machine_id_by_name(core: &Core, name: &str) -> crate::Result<MachineId> {
    core.split().machine_map.find_machine_by_name(name)
}

pub async fn get_project_id_by_name(core_ref: &CoreRef, name: &str) -> crate::Result<ProjectId> {
    let cached = core_ref
        .lock()
        .unwrap()
        .split()
        .project_map
        .find_project_id_by_name(name);
    match cached {
        Some(id) => Ok(id),
        None => {
            let pool = core_ref.lock().unwrap().pool().clone();
            let project = db::find_project_by_name(&pool, name)
                .await?
                .ok_or_else(|| crate::error::RunnerError::ProjectNotFound(name.to_string()))?;
            let id = project.id;
            let mut core = core_ref.lock().unwrap();
            if core
                .split()
                .project_map
                .find_project_id_by_name(name)
                .is_none()
            {
                core.split_mut().project_map.add_project(project);
            }
            Ok(id)
        }
    }
}
