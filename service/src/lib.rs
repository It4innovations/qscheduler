pub mod config;
mod machines;
mod notifiers;
mod projects;
mod sessions;
mod tasks;

use std::sync::Arc;

use crate::config::ServiceConfiguration;
use axum::{
    Json,
    extract::{DefaultBodyLimit, FromRequestParts, State},
    http::{StatusCode, header::AUTHORIZATION, request::Parts},
    routing::get,
};
use runner::config::RunnerConfiguration;
use runner::core::{Core, CoreRef};
use runner::reactor::check_db_health;
use serde::Serialize;
use tokio::net::TcpListener;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

struct AppState {
    version: &'static str,
    core_ref: CoreRef,
    /// Bearer token required by the admin endpoints; `None` disables them.
    admin_token: Option<String>,
}

/// Extractor guarding the admin endpoints (machine and notifier management): requires an
/// `Authorization: Bearer <ADMIN_TOKEN>` header.
pub(crate) struct AdminAuth;

impl FromRequestParts<Arc<AppState>> for AdminAuth {
    type Rejection = (StatusCode, String);

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let Some(expected) = &state.admin_token else {
            return Err(client_error(
                StatusCode::FORBIDDEN,
                "admin API is disabled; set ADMIN_TOKEN to enable it",
            ));
        };
        let provided = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        match provided {
            Some(token) if constant_time_eq(token.as_bytes(), expected.as_bytes()) => Ok(AdminAuth),
            _ => Err(client_warn(
                StatusCode::UNAUTHORIZED,
                "missing or invalid admin token",
            )),
        }
    }
}

/// Compares two byte strings in time independent of where they differ.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Registers the `admin_token` bearer security scheme used by the admin endpoints.
struct AdminSecurity;

impl utoipa::Modify for AdminSecurity {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};
        openapi
            .components
            .get_or_insert_with(Default::default)
            .add_security_scheme(
                "admin_token",
                SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
            );
    }
}

/// Returns the service version string.
#[utoipa::path(
    get,
    path = "/version",
    responses(
        (status = 200, description = "Service version string, e.g. `qscheduler v1.0.0`.", body = String)
    )
)]
async fn version_handler(State(state): State<Arc<AppState>>) -> String {
    format!("qscheduler v{}", state.version)
}

#[derive(Serialize, utoipa::ToSchema)]
struct HealthStatus {
    status: &'static str,
}

/// Health check. Reports whether the service is up and able to reach the database.
#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "Service is healthy", body = HealthStatus),
        (status = 503, description = "Service is unhealthy (database unreachable)", body = HealthStatus)
    )
)]
async fn health_handler(State(state): State<Arc<AppState>>) -> (StatusCode, Json<HealthStatus>) {
    if check_db_health(&state.core_ref).await {
        (StatusCode::OK, Json(HealthStatus { status: "ok" }))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthStatus {
                status: "unhealthy",
            }),
        )
    }
}

fn internal_error(e: impl std::fmt::Display) -> (StatusCode, String) {
    let msg = e.to_string();
    tracing::error!("{msg}");
    (StatusCode::INTERNAL_SERVER_ERROR, msg)
}

/// Maps a business-rule rejection to its HTTP status, logging it at `debug` so the reason a
/// request was rejected is visible in server logs without relying on the client to report it.
pub(crate) fn client_error(status: StatusCode, e: impl std::fmt::Display) -> (StatusCode, String) {
    let msg = e.to_string();
    tracing::debug!(%status, "{msg}");
    (status, msg)
}

/// Like [`client_error`], but for less-expected rejections (e.g. a project over its time limit)
/// that are worth surfacing more prominently.
pub(crate) fn client_warn(status: StatusCode, e: impl std::fmt::Display) -> (StatusCode, String) {
    let msg = e.to_string();
    tracing::warn!(%status, "{msg}");
    (status, msg)
}

#[derive(OpenApi)]
#[openapi(modifiers(&AdminSecurity), info(
    title = "qscheduler",
    description = include_str!("api_description.md"),
    // Explicit `env!` so a version bump always recompiles the spec (utoipa's implicit
    // defaults are read by the proc macro and not tracked by Cargo).
    version = env!("CARGO_PKG_VERSION"),
    license(name = "Apache-2.0", identifier = "Apache-2.0")
))]
struct ApiDoc;

/// All API routes; the single source for both the served router and the OpenAPI spec.
fn api_router() -> OpenApiRouter<Arc<AppState>> {
    OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(version_handler))
        .routes(routes!(health_handler))
        .routes(routes!(tasks::create_task))
        .routes(routes!(tasks::get_task))
        .routes(routes!(tasks::cancel_task_handler))
        .routes(routes!(tasks::get_task_result))
        .routes(routes!(tasks::get_task_artifact))
        .routes(routes!(sessions::get_session))
        .routes(routes!(sessions::create_session_handler))
        .routes(routes!(sessions::cancel_session_handler))
        .routes(routes!(machines::get_machine_arch))
        .routes(routes!(machines::get_machine_about))
        .routes(routes!(machines::get_machine_calibration))
        .routes(routes!(machines::get_machine_calibration_set))
        .routes(routes!(machines::get_machine_artifact))
        .routes(routes!(
            machines::suspend_machine_handler,
            machines::resume_machine_handler
        ))
        .routes(routes!(machines::list_machine_suspensions_handler))
        .routes(routes!(
            machines::list_machines_handler,
            machines::create_machine_handler
        ))
        .routes(routes!(
            machines::get_machine_handler,
            machines::update_machine_handler,
            machines::remove_machine_handler
        ))
        .routes(routes!(
            notifiers::list_notifiers_handler,
            notifiers::create_notifier_handler
        ))
        .routes(routes!(notifiers::remove_notifier_handler))
        .routes(routes!(projects::create_project_handler))
        .routes(routes!(projects::list_projects_handler))
        .routes(routes!(projects::get_project_handler))
        .routes(routes!(projects::update_project_handler))
}

/// The OpenAPI spec of the REST API, as served at `/api-docs/openapi.json`. Needs no database.
pub fn openapi() -> utoipa::openapi::OpenApi {
    api_router().split_for_parts().1
}

pub async fn run(version: &'static str, service_conf: ServiceConfiguration) -> runner::Result<()> {
    let pool = runner::db::create_pool().await?;
    let machines = runner::db::load_machines(&pool).await?;
    if machines.is_empty() {
        tracing::warn!("no machines found in database");
    } else {
        tracing::info!(count = machines.len(), "loaded machines from database");
        for (machine_id, m) in &machines {
            tracing::info!(id = %machine_id, name = %m.name, backend = ?m.backend, "machine loaded");
        }
    }
    let notifiers = runner::db::load_notifiers(&pool).await?;
    for (notifier_id, n) in &notifiers {
        tracing::info!(id = %notifier_id, name = %n.name, url = %n.url, "notifier loaded");
    }
    let runner_conf = RunnerConfiguration {
        machines,
        notifiers,
    };
    let core_ref = Core::new(runner_conf, pool).await?;
    let admin_token = std::env::var("ADMIN_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    if admin_token.is_none() {
        tracing::info!("ADMIN_TOKEN not set, machine and notifier management API disabled");
    }
    let state = Arc::new(AppState {
        version,
        core_ref: core_ref.clone(),
        admin_token,
    });

    let (router, api) = api_router().with_state(state).split_for_parts();

    let app = router
        .route(
            "/api-docs/openapi.json",
            get(move || async move { Json(api) }),
        )
        .layer(DefaultBodyLimit::max(
            service_conf.max_payload_mb * 1024 * 1024,
        ));

    let port = service_conf.port;
    let listener = TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap_or_else(|_| panic!("failed to bind 0.0.0.0:{port}"));

    tracing::info!("listening on {}", listener.local_addr().unwrap());

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");
    tracing::info!("HTTP server stopped, flushing pending DB updates");
    runner::reactor::shutdown(&core_ref).await;
    Ok(())
}

/// Resolves on SIGINT (ctrl-c) or, on Unix, SIGTERM (e.g. `docker stop`, systemd).
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install ctrl-c handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received");
}
