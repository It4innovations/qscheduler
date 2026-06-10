use std::time::Duration;
use crate::session::{SessionConfig, SessionId};
use crate::task::{TaskConfig, TaskId};
use sqlx::Row;
use sqlx::postgres::PgPool;
use crate::project::{Project, ProjectId};

pub async fn create_pool() -> sqlx::Result<PgPool> {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPool::connect(&database_url).await?;
    sqlx::migrate!("../migrations")
        .run(&pool)
        .await
        .expect("failed to run migrations");
    Ok(pool)
}

pub fn is_already_exists_error(err: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = err {
        db_err.code().as_deref() == Some("23505")
    } else {
        false
    }
}

pub async fn insert_machine(
    pool: &PgPool,
    name: &str,
    queue_size: i32,
    config: &crate::backend::BackendConfig,
    notify_url: Option<&str>,
    notify_token: Option<&str>,
) -> crate::Result<i32> {
    let machine_type = match config {
        crate::backend::BackendConfig::Iqm(_) => "iqm",
        crate::backend::BackendConfig::Test => "test",
    };
    let config_str = serde_json::to_string(config)
        .map_err(|e| crate::error::RunnerError::GenericError(e.to_string()))?;
    let row = sqlx::query(
        "INSERT INTO machines (name, type, queue_size, config, notify_url, notify_token) \
         VALUES ($1, $2::machine_type, $3, $4::json, $5, $6) RETURNING id",
    )
    .bind(name)
    .bind(machine_type)
    .bind(queue_size)
    .bind(&config_str)
    .bind(notify_url)
    .bind(notify_token)
    .fetch_one(pool)
    .await
    .map_err(|e| {
        if is_already_exists_error(&e) {
            crate::error::RunnerError::MachineAlreadyExists(name.to_string())
        } else {
            crate::error::RunnerError::Sqlx(e)
        }
    })?;
    Ok(row.get("id"))
}

pub struct MachineRow {
    pub queue_size: i32,
    pub config: crate::backend::BackendConfig,
    pub notify_url: Option<String>,
    pub notify_token: Option<String>,
}

pub async fn get_machine_by_name(
    pool: &PgPool,
    name: &str,
) -> crate::Result<Option<MachineRow>> {
    let row = sqlx::query(
        "SELECT queue_size, config::text AS config_text, notify_url, notify_token \
         FROM machines WHERE name = $1",
    )
    .bind(name)
    .fetch_optional(pool)
    .await?;
    if let Some(row) = row {
        let queue_size: i32 = row.get("queue_size");
        let config_text: String = row.get("config_text");
        let config: crate::backend::BackendConfig = serde_json::from_str(&config_text)
            .map_err(|e| crate::error::RunnerError::GenericError(e.to_string()))?;
        Ok(Some(MachineRow {
            queue_size,
            config,
            notify_url: row.get("notify_url"),
            notify_token: row.get("notify_token"),
        }))
    } else {
        Ok(None)
    }
}

pub async fn update_machine(
    pool: &PgPool,
    name: &str,
    queue_size: i32,
    config: &crate::backend::BackendConfig,
    notify_url: Option<&str>,
    notify_token: Option<&str>,
) -> crate::Result<bool> {
    let config_str = serde_json::to_string(config)
        .map_err(|e| crate::error::RunnerError::GenericError(e.to_string()))?;
    let result = sqlx::query(
        "UPDATE machines SET config = $2::json, queue_size = $3, \
         notify_url = $4, notify_token = $5 WHERE name = $1",
    )
    .bind(name)
    .bind(&config_str)
    .bind(queue_size)
    .bind(notify_url)
    .bind(notify_token)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// A non-terminal task loaded from the database during startup restoration.
pub struct RestoredTask {
    pub id: i64,
    pub machine_id: i32,
    pub session_id: Option<i64>,
    pub project_id: Option<i32>,
    pub backend_id: Option<String>,
    pub payload: Option<Vec<u8>>,
}

/// A non-terminal session loaded from the database during startup restoration.
pub struct RestoredSession {
    pub id: i64,
    pub machine_id: i32,
    pub time_limit_secs: i32,
    /// True if the session had been opened (`opened_at` is set) but not yet closed.
    pub opened: bool,
}

fn exec_time_ms(exec_time: Duration) -> Option<i64> {
    if exec_time.is_zero() { None } else { Some(exec_time.as_millis() as i64) }
}

pub struct ProjectRow {
    pub id: String,
    pub consumed_ms: i64,
    pub limit_ms: i64,
    pub active: bool,
}

pub(crate) async fn insert_project(pool: &PgPool, name: &str, active: bool, limit: Duration) -> crate::Result<ProjectId> {
    sqlx::query(
        "INSERT INTO projects (name, limit_ms, active) VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(name)
    .bind(limit.as_millis() as i64)
    .bind(active)
    .fetch_one(pool)
    .await
    .map(|row| { let id: i32 = row.get("id"); ProjectId::try_from(id).unwrap() })
    .map_err(|e| {
        if is_already_exists_error(&e) {
            crate::error::RunnerError::ProjectAlreadyExists(name.to_string())
        } else {
            crate::error::RunnerError::Sqlx(e)
        }
    })
}

pub(crate) async fn find_project_by_name(pool: &PgPool, name: &str) -> crate::Result<Option<Project>> {
    sqlx::query("SELECT id, consumed_ms, limit_ms, active FROM projects WHERE name = $1")
        .bind(name)
        .fetch_optional(pool)
        .await
        .map(|opt| opt.map(|row| {
            let id: i32 = row.get("id");
            let consumed_ms: i64 = row.get("consumed_ms");
            let limit_ms: i64 = row.get("limit_ms");
            Project {
                id: ProjectId::try_from(id).unwrap(),
                name: name.to_string(),
                consumed: Duration::from_millis(consumed_ms as u64),
                limit: Duration::from_millis(limit_ms as u64),
                active: row.get("active"),
            }
        }))
        .map_err(crate::error::RunnerError::Sqlx)
}

pub async fn list_projects(pool: &PgPool) -> crate::Result<Vec<(Project)>> {
    let rows = sqlx::query(
        "SELECT id, name, consumed_ms, limit_ms, active FROM projects ORDER BY id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let id: i32 = row.get("id");
            let consumed_ms: i64 = row.get("consumed_ms");
            let limit_ms: i64 = row.get("limit_ms");
            (Project {
                id: ProjectId::try_from(id).unwrap(),
                name: row.get("name"),
                consumed: Duration::from_millis(consumed_ms as u64),
            limit: Duration::from_millis(limit_ms as u64),
            active: row.get("active"),
        })
        })
        .collect())
}

pub async fn update_task_finished(pool: &PgPool, task_id: TaskId, exec_time: Duration) {
    if let Err(e) = sqlx::query(
        "UPDATE tasks SET state = 'finished', finished_at = NOW(), exec_time_ms = $2 WHERE id = $1",
    )
    .bind(task_id.as_u64() as i64)
    .bind(exec_time_ms(exec_time))
    .execute(pool)
    .await
    {
        tracing::error!(%task_id, error = %e, "failed to update task finished in db");
    }
}

pub async fn update_task_failed(pool: &PgPool, task_id: TaskId, exec_time: Duration, error: &str) {
    if let Err(e) = sqlx::query(
        "UPDATE tasks SET state = 'failed', finished_at = NOW(), error = $2, exec_time_ms = $3 WHERE id = $1",
    )
    .bind(task_id.as_u64() as i64)
    .bind(error)
    .bind(exec_time_ms(exec_time))
    .execute(pool)
    .await
    {
        tracing::error!(%task_id, error = %e, "failed to update task failed in db");
    }
}

pub async fn update_task_cancelled(pool: &PgPool, task_id: TaskId, exec_time: Duration) {
    if let Err(e) = sqlx::query(
        "UPDATE tasks SET state = 'cancelled', finished_at = NOW(), exec_time_ms = $2 WHERE id = $1",
    )
    .bind(task_id.as_u64() as i64)
    .bind(exec_time_ms(exec_time))
    .execute(pool)
    .await
    {
        tracing::error!(%task_id, error = %e, "failed to update task cancelled in db");
    }
}

pub async fn update_session_opened(pool: &PgPool, session_id: SessionId) {
    if let Err(e) = sqlx::query("UPDATE sessions SET opened_at = NOW() WHERE id = $1")
        .bind(session_id.as_u64() as i64)
        .execute(pool)
        .await
    {
        tracing::error!(%session_id, error = %e, "failed to update session opened_at in db");
    }
}

pub async fn close_session_with_tasks(
    pool: &PgPool,
    session_id: SessionId,
    cancelled_tasks: &[TaskId],
) {
    if let Err(e) = try_close_session_with_tasks(pool, session_id, cancelled_tasks).await {
        tracing::error!(%session_id, error = %e, "failed to close session in db, changes rolled back");
    }
}

async fn try_close_session_with_tasks(
    pool: &PgPool,
    session_id: SessionId,
    cancelled_tasks: &[TaskId],
) -> sqlx::Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("UPDATE sessions SET closed_at = NOW() WHERE id = $1")
        .bind(session_id.as_u64() as i64)
        .execute(&mut *tx)
        .await?;
    for &task_id in cancelled_tasks {
        sqlx::query("UPDATE tasks SET state = 'cancelled', finished_at = NOW() WHERE id = $1")
            .bind(task_id.as_u64() as i64)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await
}

pub async fn delete_task(pool: &PgPool, task_id: TaskId) {
    if let Err(e) = sqlx::query("DELETE FROM tasks WHERE id = $1")
        .bind(task_id.as_u64() as i64)
        .execute(pool)
        .await
    {
        tracing::error!(%task_id, error = %e, "failed to delete task from db after in-memory error");
    }
}

pub async fn delete_session(pool: &PgPool, session_id: SessionId) {
    if let Err(e) = sqlx::query("DELETE FROM sessions WHERE id = $1")
        .bind(session_id.as_u64() as i64)
        .execute(pool)
        .await
    {
        tracing::error!(%session_id, error = %e, "failed to delete session from db after in-memory error");
    }
}

pub async fn insert_task(pool: &PgPool, config: &TaskConfig) -> crate::Result<TaskId> {
    let row = sqlx::query(
        "INSERT INTO tasks (machine_id, session_id, project_id, payload) VALUES ($1, $2, $3, $4) RETURNING id",
    )
    .bind(config.machine_id.as_u32() as i32)
    .bind(config.parent.session_id().map(|s| s.as_i64()))
    .bind(config.parent.project_id().map(|p| p.as_i32()))
    .bind(config.payload.as_ref())
    .fetch_one(pool)
    .await?;

    let id: i64 = row.get("id");
    TaskId::try_from(id as u64)
        .map_err(|_| crate::error::RunnerError::GenericError("invalid task id from db".into()))
}

pub async fn insert_session(pool: &PgPool, config: &SessionConfig) -> crate::Result<SessionId> {
    let row = sqlx::query(
        "INSERT INTO sessions (machine_id, time_limit_secs) VALUES ($1, $2) RETURNING id",
    )
    .bind(config.machine_id.as_u32() as i32)
    .bind(config.time_limit.as_secs() as i32)
    .fetch_one(pool)
    .await?;

    let id: i64 = row.get("id");
    SessionId::try_from(id as u64)
        .map_err(|_| crate::error::RunnerError::GenericError("invalid session id from db".into()))
}

pub async fn update_task_backend_id(pool: &PgPool, task_id: TaskId, backend_id: &str) {
    if let Err(e) = sqlx::query("UPDATE tasks SET backend_id = $2 WHERE id = $1")
        .bind(task_id.as_u64() as i64)
        .bind(backend_id)
        .execute(pool)
        .await
    {
        tracing::error!(%task_id, error = %e, "failed to update task backend_id in db");
    }
}

pub async fn load_machines(pool: &PgPool) -> crate::Result<Vec<crate::config::MachineConfiguration>> {
    let rows = sqlx::query(
        "SELECT id, name, queue_size, config::text AS config_text, notify_url, notify_token \
         FROM machines ORDER BY id",
    )
    .fetch_all(pool)
    .await?;
    let mut machines = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i32 = row.get("id");
        let name: String = row.get("name");
        let queue_size: i32 = row.get("queue_size");
        let config_text: String = row.get("config_text");
        let notify_url: Option<String> = row.get("notify_url");
        let notify_token: Option<String> = row.get("notify_token");
        let backend: crate::backend::BackendConfig = serde_json::from_str(&config_text)
            .map_err(|e| {
                crate::error::RunnerError::GenericError(format!(
                    "failed to parse config for machine '{name}': {e}"
                ))
            })?;
        let notify = notify_url.map(|url| crate::callback::NotifyConfig {
            url,
            token: notify_token,
        });
        machines.push(crate::config::MachineConfiguration {
            id: id as u32,
            name,
            queue_size: queue_size as usize,
            notify,
            backend,
        });
    }
    Ok(machines)
}

/// Load all tasks that are still in a non-terminal state (stored as `waiting`).
pub async fn load_active_tasks(pool: &PgPool) -> sqlx::Result<Vec<RestoredTask>> {
    let rows = sqlx::query(
        "SELECT id, machine_id, session_id, project_id, backend_id, payload FROM tasks WHERE state = 'waiting'",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| RestoredTask {
            id: row.get("id"),
            machine_id: row.get("machine_id"),
            session_id: row.get("session_id"),
            project_id: row.get("project_id"),
            backend_id: row.get("backend_id"),
            payload: row.get("payload"),
        })
        .collect())
}

/// Load all sessions that have not been closed yet (waiting or open).
pub async fn load_active_sessions(pool: &PgPool) -> sqlx::Result<Vec<RestoredSession>> {
    let rows = sqlx::query(
        "SELECT id, machine_id, time_limit_secs, (opened_at IS NOT NULL) AS opened \
         FROM sessions WHERE closed_at IS NULL",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| RestoredSession {
            id: row.get("id"),
            machine_id: row.get("machine_id"),
            time_limit_secs: row.get("time_limit_secs"),
            opened: row.get("opened"),
        })
        .collect())
}
