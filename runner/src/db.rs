use crate::MachineId;
use crate::callback::NotifierId;
use crate::config::{MachineConfiguration, NotifierConfiguration};
use crate::error::RunnerError;
use crate::project::{Project, ProjectId};
use crate::session::{SessionConfig, SessionId, SessionInfo, SessionStateKind};
use crate::suspension::{SuspendCancel, Suspension};
use crate::task::{CompletedState, TaskConfig, TaskId, TaskInfo, TaskStateKind};
use chrono::{DateTime, Utc};
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

fn machine_type(config: &MachineConfiguration) -> &'static str {
    match config.backend {
        crate::backend::BackendConfig::Iqm(_) => "iqm",
        crate::backend::BackendConfig::Test => "test",
    }
}

fn serialize_machine_config(config: &MachineConfiguration) -> crate::Result<String> {
    serde_json::to_string(config).map_err(|e| RunnerError::GenericError(e.to_string()))
}

pub async fn insert_machine(pool: &PgPool, config: &MachineConfiguration) -> crate::Result<i32> {
    let config_str = serialize_machine_config(config)?;
    let row = sqlx::query(
        "INSERT INTO machines (name, type, config) \
         VALUES ($1, $2::machine_type, $3::json) RETURNING id",
    )
    .bind(&config.name)
    .bind(machine_type(config))
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

/// Replaces the stored configuration (and backend type) of the machine named `config.name`.
/// Returns `false` if no such machine exists.
pub async fn update_machine(pool: &PgPool, config: &MachineConfiguration) -> crate::Result<bool> {
    let config_str = serialize_machine_config(config)?;
    let result = sqlx::query(
        "UPDATE machines SET type = $2::machine_type, config = $3::json WHERE name = $1",
    )
    .bind(&config.name)
    .bind(machine_type(config))
    .bind(&config_str)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub enum RemoveMachineOutcome {
    NotFound,
    /// The machine still has tasks or sessions and `force` was not set; nothing was removed.
    HasDependents {
        tasks: i64,
        sessions: i64,
    },
    /// The machine was removed together with the given number of tasks and sessions.
    Removed {
        tasks: i64,
        sessions: i64,
    },
}

/// Removes a machine. Without `force`, the machine is removed only if it has no tasks and no
/// sessions. With `force`, its tasks and sessions are deleted as well. Project `consumed_ms`
/// totals are left untouched (deleting rows does not fire the consumption triggers).
pub async fn remove_machine(
    pool: &PgPool,
    name: &str,
    force: bool,
) -> crate::Result<RemoveMachineOutcome> {
    let mut tx = pool.begin().await?;
    let Some(row) = sqlx::query("SELECT id FROM machines WHERE name = $1 FOR UPDATE")
        .bind(name)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(RemoveMachineOutcome::NotFound);
    };
    let machine_id: i32 = row.get("id");
    let counts = sqlx::query(
        "SELECT \
           (SELECT COUNT(*) FROM tasks WHERE machine_id = $1 \
              OR session_id IN (SELECT id FROM sessions WHERE machine_id = $1)) AS tasks, \
           (SELECT COUNT(*) FROM sessions WHERE machine_id = $1) AS sessions",
    )
    .bind(machine_id)
    .fetch_one(&mut *tx)
    .await?;
    let tasks: i64 = counts.get("tasks");
    let sessions: i64 = counts.get("sessions");
    if (tasks > 0 || sessions > 0) && !force {
        return Ok(RemoveMachineOutcome::HasDependents { tasks, sessions });
    }
    sqlx::query(
        "DELETE FROM tasks WHERE machine_id = $1 \
         OR session_id IN (SELECT id FROM sessions WHERE machine_id = $1)",
    )
    .bind(machine_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM sessions WHERE machine_id = $1")
        .bind(machine_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM machines WHERE id = $1")
        .bind(machine_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(RemoveMachineOutcome::Removed { tasks, sessions })
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
    pub created_at: DateTime<Utc>,
    pub time_limit_ms: Option<i64>,
    pub started_at: Option<DateTime<Utc>>,
    pub notifier_id: Option<i32>,
}

/// A non-terminal session loaded from the database during startup restoration.
#[derive(sqlx::FromRow)]
pub struct RestoredSession {
    pub id: i64,
    pub machine_id: i32,
    pub project_id: i32,
    pub time_limit_ms: i64,
    pub opened_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub closed_at: Option<DateTime<Utc>>,
    pub exec_time_ms: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub notifier_id: Option<i32>,
}

fn exec_time_ms(exec_time: Duration) -> Option<i64> {
    crate::task::duration_ms(exec_time)
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
            id: ProjectId::from_db(r.id),
            name: r.name,
            consumed: Duration::from_millis(r.consumed_ms as u64),
            limit: Duration::from_millis(r.limit_ms as u64),
            active: r.active,
            in_flight: Duration::ZERO,
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
            ProjectId::from_db(id)
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

pub(crate) async fn update_project(
    pool: &PgPool,
    name: &str,
    active: Option<bool>,
    limit_ms: Option<i64>,
) -> crate::Result<Option<Project>> {
    sqlx::query_as::<_, ProjectRow>(
        "UPDATE projects SET active = COALESCE($2, active), limit_ms = COALESCE($3, limit_ms) \
         WHERE name = $1 RETURNING id, name, consumed_ms, limit_ms, active",
    )
    .bind(name)
    .bind(active)
    .bind(limit_ms)
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

pub async fn update_task_finished(pool: &PgPool, task_id: TaskId, completed: CompletedState) {
    if let Err(e) = sqlx::query(
        "UPDATE tasks SET state = 'finished', finished_at = $3, exec_time_ms = $2 WHERE id = $1",
    )
    .bind(task_id.as_u64() as i64)
    .bind(exec_time_ms(completed.consumed))
    .bind(completed.timestamp)
    .execute(pool)
    .await
    {
        tracing::error!(%task_id, error = %e, "failed to update task finished in db");
    }
}

pub async fn update_task_failed(
    pool: &PgPool,
    task_id: TaskId,
    completed: CompletedState,
    error: &str,
) {
    if let Err(e) = sqlx::query(
        "UPDATE tasks SET state = 'failed', finished_at = $4, error = $2, exec_time_ms = $3 WHERE id = $1",
    )
    .bind(task_id.as_u64() as i64)
    .bind(error)
        .bind(exec_time_ms(completed.consumed))
        .bind(completed.timestamp)
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

pub async fn update_task_cancelled(pool: &PgPool, task_id: TaskId, completed: CompletedState) {
    if let Err(e) = sqlx::query(
        "UPDATE tasks SET state = 'cancelled', finished_at = $3, exec_time_ms = $2 WHERE id = $1",
    )
    .bind(task_id.as_u64() as i64)
    .bind(exec_time_ms(completed.consumed))
    .bind(completed.timestamp)
    .execute(pool)
    .await
    {
        tracing::error!(%task_id, error = %e, "failed to update task cancelled in db");
    }
}

pub async fn update_session_opened(pool: &PgPool, session_id: SessionId, timestamp: DateTime<Utc>) {
    if let Err(e) =
        sqlx::query("UPDATE sessions SET opened_at = $2, updated_at = NOW() WHERE id = $1")
            .bind(session_id.as_u64() as i64)
            .bind(timestamp)
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
    timestamp: DateTime<Utc>,
    cancelled_tasks: &[TaskId],
) {
    if let Err(e) =
        try_close_session_with_tasks(pool, session_id, exec_time, timestamp, cancelled_tasks).await
    {
        tracing::error!(%session_id, error = %e, "failed to close session in db, changes rolled back");
    }
}

pub async fn close_dead_session(pool: &PgPool, session_id: SessionId, closed_at: DateTime<Utc>) {
    if let Err(e) =
        sqlx::query("UPDATE sessions SET updated_at = NULL, closed_at = $2 WHERE id = $1")
            .bind(session_id.as_u64() as i64)
            .bind(closed_at)
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
    timestamp: DateTime<Utc>,
    cancelled_tasks: &[TaskId],
) -> sqlx::Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        "UPDATE sessions SET updated_at = NULL, closed_at = $3, exec_time_ms = $2 WHERE id = $1",
    )
    .bind(session_id.as_u64() as i64)
    .bind(exec_time.as_millis() as i64)
    .bind(timestamp)
    .execute(&mut *tx)
    .await?;
    for &task_id in cancelled_tasks {
        sqlx::query("UPDATE tasks SET state = 'cancelled', finished_at = $2 WHERE id = $1")
            .bind(task_id.as_u64() as i64)
            .bind(timestamp)
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

pub async fn insert_task(
    pool: &PgPool,
    config: &TaskConfig,
) -> crate::Result<(TaskId, DateTime<Utc>)> {
    let row = sqlx::query(
        "INSERT INTO tasks (machine_id, session_id, project_id, \"user\", payload, time_limit_ms, notifier_id) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id, created_at",
    )
    .bind(config.machine_id.as_i32())
    .bind(config.parent.session_id().map(|s| s.as_i64()))
    .bind(config.parent.project_id().map(|p| p.as_i32()))
    .bind(&config.user)
    .bind(config.payload.as_ref())
    .bind(config.time_limit.as_millis() as i64)
    .bind(config.notifier.map(|n| n.as_i32()))
    .fetch_one(pool)
    .await?;

    use sqlx::Row;
    let id: i64 = row.get("id");
    let created_at: DateTime<Utc> = row.get("created_at");
    let task_id = TaskId::try_from(id as u64)
        .map_err(|_| crate::error::RunnerError::GenericError("invalid task id from db".into()))?;
    Ok((task_id, created_at))
}

/// Records when the backend first reported the task as running; later reports are ignored.
pub async fn update_task_started(pool: &PgPool, task_id: TaskId, started_at: DateTime<Utc>) {
    if let Err(e) =
        sqlx::query("UPDATE tasks SET started_at = $2 WHERE id = $1 AND started_at IS NULL")
            .bind(task_id.as_i64())
            .bind(started_at)
            .execute(pool)
            .await
    {
        tracing::error!(%task_id, error = %e, "failed to update task started_at in db");
    }
}

pub async fn update_session_exec_time(pool: &PgPool, session_id: SessionId, exec_time_ms: i64) {
    if let Err(e) =
        sqlx::query("UPDATE sessions SET exec_time_ms = $2, updated_at = NOW() WHERE id = $1")
            .bind(session_id.as_i64())
            .bind(exec_time_ms)
            .execute(pool)
            .await
    {
        tracing::error!(%session_id, error = %e, "failed to update session exec_time_ms in db");
    }
}

pub async fn insert_session(
    pool: &PgPool,
    config: &SessionConfig,
) -> crate::Result<(SessionId, DateTime<Utc>)> {
    let row = sqlx::query(
        "INSERT INTO sessions (machine_id, project_id, time_limit_ms, notifier_id) \
         VALUES ($1, $2, $3, $4) RETURNING id, created_at",
    )
    .bind(config.machine_id.as_i32())
    .bind(config.project_id.as_i32())
    .bind(config.time_limit.as_millis() as i64)
    .bind(config.notifier.map(|n| n.as_i32()))
    .fetch_one(pool)
    .await?;

    use sqlx::Row;
    let id: i64 = row.get("id");
    let created_at: DateTime<Utc> = row.get("created_at");
    let session_id = SessionId::try_from(id as u64).map_err(|_| {
        crate::error::RunnerError::GenericError("invalid session id from db".into())
    })?;
    Ok((session_id, created_at))
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
        let machine_id = MachineId::from_db(id);
        let config_str: &str = row.get("config");
        let config: MachineConfiguration = serde_json::from_str(config_str).map_err(|_e| {
            RunnerError::GenericError("Cannot parse machine configuration from DB".into())
        })?;
        machines.push((machine_id, config));
    }
    Ok(machines)
}

pub async fn insert_notifier(
    pool: &PgPool,
    config: &NotifierConfiguration,
) -> crate::Result<NotifierId> {
    crate::callback::validate_notifier_name(&config.name)?;
    let row =
        sqlx::query("INSERT INTO notifiers (name, url, token) VALUES ($1, $2, $3) RETURNING id")
            .bind(&config.name)
            .bind(&config.url)
            .bind(&config.token)
            .fetch_one(pool)
            .await
            .map_err(|e| {
                if is_already_exists_error(&e) {
                    RunnerError::NotifierAlreadyExists(config.name.clone())
                } else {
                    RunnerError::Sqlx(e)
                }
            })?;
    let id: i32 = row.get("id");
    Ok(NotifierId::from_db(id))
}

/// Removes a notifier; tasks and sessions that referenced it stop notifying (the FK is
/// `ON DELETE SET NULL`). Returns `false` if no such notifier exists.
pub async fn remove_notifier(pool: &PgPool, name: &str) -> crate::Result<bool> {
    let result = sqlx::query("DELETE FROM notifiers WHERE name = $1")
        .bind(name)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn load_notifiers(
    pool: &PgPool,
) -> crate::Result<Vec<(NotifierId, NotifierConfiguration)>> {
    let rows = sqlx::query("SELECT id, name, url, token FROM notifiers ORDER BY id")
        .fetch_all(pool)
        .await?;
    Ok(rows
        .iter()
        .map(|row| {
            let id: i32 = row.get("id");
            (
                NotifierId::from_db(id),
                NotifierConfiguration {
                    name: row.get("name"),
                    url: row.get("url"),
                    token: row.get("token"),
                },
            )
        })
        .collect())
}

fn suspension_from_row(row: &sqlx::postgres::PgRow) -> crate::Result<Suspension> {
    Ok(Suspension {
        message: row.try_get("message")?,
        cancel: SuspendCancel::from_db_str(row.try_get("cancel")?)?,
        suspended_at: row.try_get("suspended_at")?,
        resumed_at: row.try_get("resumed_at")?,
    })
}

/// Records a new active suspension of a machine. Fails with
/// [`RunnerError::MachineAlreadySuspended`] if the machine already has one.
pub(crate) async fn insert_suspension(
    pool: &PgPool,
    machine_id: MachineId,
    machine_name: &str,
    message: &str,
    cancel: SuspendCancel,
) -> crate::Result<Suspension> {
    let row = sqlx::query(
        "INSERT INTO machine_suspensions (machine_id, message, cancel) \
         VALUES ($1, $2, $3::suspension_cancel) \
         RETURNING message, cancel::text AS cancel, suspended_at, resumed_at",
    )
    .bind(machine_id.as_i32())
    .bind(message)
    .bind(cancel.as_db_str())
    .fetch_one(pool)
    .await
    .map_err(|e| {
        if is_already_exists_error(&e) {
            RunnerError::MachineAlreadySuspended(machine_name.to_string())
        } else {
            RunnerError::Sqlx(e)
        }
    })?;
    suspension_from_row(&row)
}

/// Ends the machine's active suspension. Returns it, or `None` if the machine wasn't suspended.
pub(crate) async fn end_suspension(
    pool: &PgPool,
    machine_id: MachineId,
) -> crate::Result<Option<Suspension>> {
    let row = sqlx::query(
        "UPDATE machine_suspensions SET resumed_at = GREATEST(NOW(), suspended_at) \
         WHERE machine_id = $1 AND resumed_at IS NULL \
         RETURNING message, cancel::text AS cancel, suspended_at, resumed_at",
    )
    .bind(machine_id.as_i32())
    .fetch_optional(pool)
    .await?;
    row.as_ref().map(suspension_from_row).transpose()
}

/// All suspensions of a machine, newest first.
pub(crate) async fn list_suspensions(
    pool: &PgPool,
    machine_id: MachineId,
) -> crate::Result<Vec<Suspension>> {
    let rows = sqlx::query(
        "SELECT message, cancel::text AS cancel, suspended_at, resumed_at \
         FROM machine_suspensions WHERE machine_id = $1 ORDER BY suspended_at DESC, id DESC",
    )
    .bind(machine_id.as_i32())
    .fetch_all(pool)
    .await?;
    rows.iter().map(suspension_from_row).collect()
}

/// The active suspension of every suspended machine.
pub(crate) async fn load_active_suspensions(
    pool: &PgPool,
) -> crate::Result<Vec<(MachineId, Suspension)>> {
    let rows = sqlx::query(
        "SELECT machine_id, message, cancel::text AS cancel, suspended_at, resumed_at \
         FROM machine_suspensions WHERE resumed_at IS NULL",
    )
    .fetch_all(pool)
    .await?;
    rows.iter()
        .map(|row| {
            let machine_id: i32 = row.try_get("machine_id")?;
            Ok((MachineId::from_db(machine_id), suspension_from_row(row)?))
        })
        .collect()
}

/// Load all tasks that are still in a non-terminal state (stored as `waiting`).
pub async fn load_active_tasks(pool: &PgPool) -> sqlx::Result<Vec<RestoredTask>> {
    sqlx::query_as::<_, RestoredTask>(
        "SELECT id, machine_id, session_id, project_id, backend_id, \"user\", payload, created_at, \
                time_limit_ms, started_at, notifier_id \
         FROM tasks WHERE state = 'waiting'",
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
        "SELECT id, machine_id, project_id, time_limit_ms, opened_at, updated_at, closed_at, \
                exec_time_ms, created_at, notifier_id \
         FROM sessions WHERE closed_at IS NULL OR id = ANY($1)",
    )
    .bind(&task_session_ids)
    .fetch_all(pool)
    .await
}

#[derive(sqlx::FromRow)]
struct TaskInfoRow {
    id: i64,
    machine: String,
    session_id: Option<i64>,
    project: Option<String>,
    backend_id: Option<String>,
    user: String,
    state: String,
    error: Option<String>,
    finished_at: Option<DateTime<Utc>>,
    exec_time_ms: Option<i64>,
    created_at: DateTime<Utc>,
    time_limit_ms: Option<i64>,
    started_at: Option<DateTime<Utc>>,
    notify: Option<String>,
}

impl TaskInfoRow {
    fn into_task_info(self) -> TaskInfo {
        let state = match self.state.as_str() {
            "waiting" => TaskStateKind::Waiting,
            "finished" => TaskStateKind::Finished,
            "failed" => TaskStateKind::Failed,
            "cancelled" => TaskStateKind::Cancelled,
            other => unreachable!("unknown task state '{other}' loaded from db"),
        };
        TaskInfo {
            id: self.id as u64,
            session: self.session_id.map(|id| id as u64),
            project: self.project,
            user: self.user,
            backend_id: self.backend_id,
            machine: self.machine,
            state,
            created_at: self.created_at,
            time_limit_ms: self.time_limit_ms.map(|ms| ms as u64),
            started_at: self.started_at,
            finished_at: self.finished_at,
            exectime_ms: self.exec_time_ms,
            error: self.error,
            notify: self.notify,
        }
    }
}

/// Look up a task's public info directly from the DB, for a task no longer resident in
/// `Core`'s in-memory `TaskMap` — i.e. a terminal task from before the last restart
/// (`Core::restore` only reloads non-terminal rows).
pub(crate) async fn find_task_info(
    pool: &PgPool,
    task_id: TaskId,
) -> crate::Result<Option<TaskInfo>> {
    sqlx::query_as::<_, TaskInfoRow>(
        "SELECT t.id, m.name AS machine, t.session_id, p.name AS project, t.backend_id, \
                t.\"user\", t.state::text AS state, t.error, t.finished_at, t.exec_time_ms, \
                t.created_at, t.time_limit_ms, t.started_at, n.name AS notify \
         FROM tasks t \
         JOIN machines m ON m.id = t.machine_id \
         LEFT JOIN projects p ON p.id = t.project_id \
         LEFT JOIN notifiers n ON n.id = t.notifier_id \
         WHERE t.id = $1",
    )
    .bind(task_id.as_i64())
    .fetch_optional(pool)
    .await
    .map(|opt| opt.map(TaskInfoRow::into_task_info))
    .map_err(crate::error::RunnerError::Sqlx)
}

#[derive(sqlx::FromRow)]
struct SessionInfoRow {
    id: i64,
    machine: String,
    project: String,
    time_limit_ms: i64,
    opened_at: Option<DateTime<Utc>>,
    closed_at: Option<DateTime<Utc>>,
    exec_time_ms: Option<i64>,
    created_at: DateTime<Utc>,
    notify: Option<String>,
}

impl SessionInfoRow {
    fn into_session_info(self) -> SessionInfo {
        let state = if self.closed_at.is_some() {
            SessionStateKind::Closed
        } else if self.opened_at.is_some() {
            SessionStateKind::Open
        } else {
            SessionStateKind::Waiting
        };
        SessionInfo {
            id: self.id as u64,
            state,
            machine: self.machine,
            project: self.project,
            time_limit_ms: self.time_limit_ms,
            created_at: self.created_at,
            opened_at: self.opened_at,
            closed_at: self.closed_at,
            // sessions.exec_time_ms can hold a literal 0 (unlike tasks.exec_time_ms, which is
            // always written through the zero-omitting duration_ms helper) — normalize here to
            // match TaskInfo.exectime_ms's omit-when-zero convention.
            exectime_ms: self.exec_time_ms.filter(|&ms| ms != 0),
            notify: self.notify,
        }
    }
}

/// Look up a session's public info directly from the DB, for a session no longer resident in
/// `Core`'s in-memory `SessionMap` (a closed session not referenced by any active task).
pub(crate) async fn find_session_info(
    pool: &PgPool,
    session_id: SessionId,
) -> crate::Result<Option<SessionInfo>> {
    sqlx::query_as::<_, SessionInfoRow>(
        "SELECT s.id, m.name AS machine, p.name AS project, s.time_limit_ms, s.opened_at, \
                s.closed_at, s.exec_time_ms, s.created_at, n.name AS notify \
         FROM sessions s \
         JOIN machines m ON m.id = s.machine_id \
         JOIN projects p ON p.id = s.project_id \
         LEFT JOIN notifiers n ON n.id = s.notifier_id \
         WHERE s.id = $1",
    )
    .bind(session_id.as_i64())
    .fetch_optional(pool)
    .await
    .map(|opt| opt.map(SessionInfoRow::into_session_info))
    .map_err(crate::error::RunnerError::Sqlx)
}
