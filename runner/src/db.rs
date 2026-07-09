use crate::MachineId;
use crate::config::MachineConfiguration;
use crate::error::RunnerError;
use crate::project::{Project, ProjectId};
use crate::session::{SessionConfig, SessionId};
use crate::task::{TaskConfig, TaskId};
use sqlx::Row;
use sqlx::postgres::PgPool;
use std::time::Duration;

pub async fn create_pool() -> sqlx::Result<PgPool> {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPool::connect(&database_url).await?;
    sqlx::migrate!("../migrations")
        .run(&pool)
        .await
        .expect("failed to run migrations");
    Ok(pool)
}

pub async fn ping(pool: &PgPool) -> bool {
    sqlx::query("SELECT 1").execute(pool).await.is_ok()
}

pub fn is_already_exists_error(err: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = err {
        db_err.code().as_deref() == Some("23505")
    } else {
        false
    }
}

pub async fn insert_machine(pool: &PgPool, config: &MachineConfiguration) -> crate::Result<i32> {
    let machine_type = match config.backend {
        crate::backend::BackendConfig::Iqm(_) => "iqm",
        crate::backend::BackendConfig::Test => "test",
    };
    let config_str = serde_json::to_string(config)
        .map_err(|e| crate::error::RunnerError::GenericError(e.to_string()))?;
    let row = sqlx::query(
        "INSERT INTO machines (name, type, config) \
         VALUES ($1, $2::machine_type, $3::json) RETURNING id",
    )
    .bind(&config.name)
    .bind(machine_type)
    .bind(&config_str)
    .fetch_one(pool)
    .await
    .map_err(|e| {
        if is_already_exists_error(&e) {
            crate::error::RunnerError::MachineAlreadyExists(config.name.to_string())
        } else {
            crate::error::RunnerError::Sqlx(e)
        }
    })?;
    use sqlx::Row;
    Ok(row.get("id"))
}

pub struct MachineRow {
    pub queue_size: i32,
    pub config: crate::backend::BackendConfig,
    pub notify_url: Option<String>,
    pub notify_token: Option<String>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for MachineRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        let config_str: String = row.try_get("config")?;
        let config = serde_json::from_str(&config_str).map_err(|e| sqlx::Error::ColumnDecode {
            index: "config".to_string(),
            source: e.into(),
        })?;
        Ok(MachineRow {
            queue_size: row.try_get("queue_size")?,
            config,
            notify_url: row.try_get("notify_url")?,
            notify_token: row.try_get("notify_token")?,
        })
    }
}

pub async fn get_machine_by_name(pool: &PgPool, name: &str) -> crate::Result<Option<MachineRow>> {
    sqlx::query_as::<_, MachineRow>(
        "SELECT queue_size, config::text AS config, notify_url, notify_token \
         FROM machines WHERE name = $1",
    )
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(crate::error::RunnerError::Sqlx)
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
#[derive(sqlx::FromRow)]
pub struct RestoredTask {
    pub id: i64,
    pub machine_id: i32,
    pub session_id: Option<i64>,
    pub project_id: Option<i32>,
    pub backend_id: Option<String>,
    pub user: String,
    pub payload: Option<Vec<u8>>,
}

/// A non-terminal session loaded from the database during startup restoration.
#[derive(sqlx::FromRow)]
pub struct RestoredSession {
    pub id: i64,
    pub machine_id: i32,
    pub project_id: i32,
    pub time_limit_ms: i64,
    pub opened: bool,
    pub closed: bool,
}

fn exec_time_ms(exec_time: Duration) -> Option<i64> {
    if exec_time.is_zero() {
        None
    } else {
        Some(exec_time.as_millis() as i64)
    }
}

#[derive(sqlx::FromRow)]
pub struct ProjectRow {
    pub id: i32,
    pub name: String,
    pub consumed_ms: i64,
    pub limit_ms: i64,
    pub active: bool,
}

impl From<ProjectRow> for Project {
    fn from(r: ProjectRow) -> Self {
        Project {
            id: ProjectId::try_from(r.id).unwrap(),
            name: r.name,
            consumed: Duration::from_millis(r.consumed_ms as u64),
            limit: Duration::from_millis(r.limit_ms as u64),
            active: r.active,
        }
    }
}

pub(crate) async fn insert_project(
    pool: &PgPool,
    name: &str,
    active: bool,
    limit: Duration,
) -> crate::Result<ProjectId> {
    sqlx::query("INSERT INTO projects (name, limit_ms, active) VALUES ($1, $2, $3) RETURNING id")
        .bind(name)
        .bind(limit.as_millis() as i64)
        .bind(active)
        .fetch_one(pool)
        .await
        .map(|row| {
            use sqlx::Row;
            let id: i32 = row.get("id");
            ProjectId::try_from(id).unwrap()
        })
        .map_err(|e| {
            if is_already_exists_error(&e) {
                crate::error::RunnerError::ProjectAlreadyExists(name.to_string())
            } else {
                crate::error::RunnerError::Sqlx(e)
            }
        })
}

pub(crate) async fn find_project_by_name(
    pool: &PgPool,
    name: &str,
) -> crate::Result<Option<Project>> {
    sqlx::query_as::<_, ProjectRow>(
        "SELECT id, name, consumed_ms, limit_ms, active FROM projects WHERE name = $1",
    )
    .bind(name)
    .fetch_optional(pool)
    .await
    .map(|opt| opt.map(Project::from))
    .map_err(crate::error::RunnerError::Sqlx)
}

pub async fn list_projects(pool: &PgPool) -> crate::Result<Vec<Project>> {
    sqlx::query_as::<_, ProjectRow>(
        "SELECT id, name, consumed_ms, limit_ms, active FROM projects ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .map(|rows| rows.into_iter().map(Project::from).collect())
    .map_err(crate::error::RunnerError::Sqlx)
}

/// Load only the projects referenced by the given ids, for restoring [`ProjectMap`]
/// with just the entries needed by restored tasks (rather than every project).
pub async fn load_projects_by_ids(
    pool: &PgPool,
    ids: impl Iterator<Item = &ProjectId>,
) -> crate::Result<Vec<Project>> {
    let ids: Vec<i32> = ids.map(|id| id.as_i32()).collect();
    sqlx::query_as::<_, ProjectRow>(
        "SELECT id, name, consumed_ms, limit_ms, active FROM projects WHERE id = ANY($1)",
    )
    .bind(&ids)
    .fetch_all(pool)
    .await
    .map(|rows| rows.into_iter().map(Project::from).collect())
    .map_err(crate::error::RunnerError::Sqlx)
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

pub async fn update_tasks_cancelled(pool: &PgPool, task_ids: &[TaskId]) {
    let task_ids: Vec<i64> = task_ids.iter().map(|id| id.as_i64()).collect();
    if let Err(e) =
        sqlx::query("UPDATE tasks SET state = 'cancelled', finished_at = NOW() WHERE id = ANY($1)")
            .bind(task_ids.as_slice())
            .execute(pool)
            .await
    {
        tracing::error!(?task_ids, error = %e, "failed to update task cancelled in db");
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
    if let Err(e) =
        sqlx::query("UPDATE sessions SET opened_at = NOW(), updated_at = NOW() WHERE id = $1")
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
    exec_time: Duration,
    cancelled_tasks: &[TaskId],
) {
    if let Err(e) = try_close_session_with_tasks(pool, session_id, exec_time, cancelled_tasks).await
    {
        tracing::error!(%session_id, error = %e, "failed to close session in db, changes rolled back");
    }
}

pub async fn close_dead_session(pool: &PgPool, session_id: SessionId) {
    if let Err(e) =
        sqlx::query("UPDATE sessions SET updated_at = NULL, closed_at = NOW() WHERE id = $1")
            .bind(session_id.as_u64() as i64)
            .execute(pool)
            .await
    {
        tracing::error!(%session_id, error = %e, "failed to close session in db, changes rolled back");
    }
}

async fn try_close_session_with_tasks(
    pool: &PgPool,
    session_id: SessionId,
    exec_time: Duration,
    cancelled_tasks: &[TaskId],
) -> sqlx::Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        "UPDATE sessions SET updated_at = NULL, closed_at = NOW(), exec_time_ms = $2 WHERE id = $1",
    )
    .bind(session_id.as_u64() as i64)
    .bind(exec_time.as_millis() as i64)
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
        "INSERT INTO tasks (machine_id, session_id, project_id, \"user\", payload) VALUES ($1, $2, $3, $4, $5) RETURNING id",
    )
    .bind(config.machine_id.as_u32() as i32)
    .bind(config.parent.session_id().map(|s| s.as_i64()))
    .bind(config.parent.project_id().map(|p| p.as_i32()))
    .bind(&config.user)
    .bind(config.payload.as_ref())
    .fetch_one(pool)
    .await?;

    use sqlx::Row;
    let id: i64 = row.get("id");
    TaskId::try_from(id as u64)
        .map_err(|_| crate::error::RunnerError::GenericError("invalid task id from db".into()))
}

pub async fn update_session_exec_time(pool: &PgPool, session_id: SessionId, exec_time_ms: i64) {
    if let Err(e) = sqlx::query("UPDATE sessions SET exec_time_ms = $2 WHERE id = $1")
        .bind(session_id.as_i64())
        .bind(exec_time_ms)
        .execute(pool)
        .await
    {
        tracing::error!(%session_id, error = %e, "failed to update session exec_time_ms in db");
    }
}

pub async fn insert_session(pool: &PgPool, config: &SessionConfig) -> crate::Result<SessionId> {
    let row = sqlx::query(
        "INSERT INTO sessions (machine_id, project_id, time_limit_ms) VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(config.machine_id.as_u32() as i32)
    .bind(config.project_id.as_i32())
    .bind(config.time_limit.as_millis() as i64)
    .fetch_one(pool)
    .await?;

    use sqlx::Row;
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

pub async fn load_machines(pool: &PgPool) -> crate::Result<Vec<(MachineId, MachineConfiguration)>> {
    let rows = sqlx::query("SELECT id, config::text AS config FROM machines ORDER BY id")
        .fetch_all(pool)
        .await?;
    let mut machines = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i32 = row.get("id");
        let machine_id = MachineId::from(id as u32);
        let config_str: &str = row.get("config");
        let config: MachineConfiguration = serde_json::from_str(config_str).map_err(|_e| {
            RunnerError::GenericError("Cannot parse machine configuration from DB".into())
        })?;
        machines.push((machine_id, config));
    }
    Ok(machines)
}

/// Load all tasks that are still in a non-terminal state (stored as `waiting`).
pub async fn load_active_tasks(pool: &PgPool) -> sqlx::Result<Vec<RestoredTask>> {
    sqlx::query_as::<_, RestoredTask>(
        "SELECT id, machine_id, session_id, project_id, backend_id, \"user\", payload FROM tasks WHERE state = 'waiting'",
    )
    .fetch_all(pool)
    .await
}

/// Load all sessions that have not been closed yet (waiting or open), plus any
/// already-closed session still referenced by a non-terminal task: closing a
/// session and cancelling its in-flight tasks isn't atomic (the task-side update
/// only lands once the backend confirms cancellation), so a crash can leave
/// `closed_at` set while one of its tasks is still `waiting` with a `backend_id`.
pub async fn load_active_sessions(
    pool: &PgPool,
    active_tasks: &[RestoredTask],
) -> sqlx::Result<Vec<RestoredSession>> {
    let task_session_ids: Vec<i64> = active_tasks.iter().filter_map(|t| t.session_id).collect();
    sqlx::query_as::<_, RestoredSession>(
        "SELECT id, machine_id, project_id, time_limit_ms, (opened_at IS NOT NULL) AS opened, (closed_at IS NOT NULL) AS closed \
         FROM sessions WHERE closed_at IS NULL OR id = ANY($1)",
    )
    .bind(&task_session_ids)
    .fetch_all(pool)
    .await
}
