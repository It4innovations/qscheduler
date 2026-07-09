pub mod config;
mod machines;
mod projects;
mod sessions;
mod tasks;

use std::sync::Arc;

use crate::config::ServiceConfiguration;
use axum::{Json, extract::State, http::StatusCode, routing::get};
use runner::config::RunnerConfiguration;
use runner::core::{Core, CoreRef};
use tokio::net::TcpListener;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

struct AppState {
    version: &'static str,
    core_ref: CoreRef,
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

fn internal_error(e: impl std::fmt::Display) -> (StatusCode, String) {
    let msg = e.to_string();
    tracing::error!("{msg}");
    (StatusCode::INTERNAL_SERVER_ERROR, msg)
}

#[derive(OpenApi)]
struct ApiDoc;

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
    let runner_conf = RunnerConfiguration { machines };
    let state = Arc::new(AppState {
        version,
        core_ref: Core::new(runner_conf, pool).await?,
    });

    let (router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(version_handler))
        .routes(routes!(tasks::create_task))
        .routes(routes!(tasks::get_task))
        .routes(routes!(tasks::cancel_task_handler))
        .routes(routes!(sessions::get_session))
        .routes(routes!(sessions::create_session_handler))
        .routes(routes!(sessions::cancel_session_handler))
        .routes(routes!(machines::get_machine_arch))
        .routes(routes!(machines::get_machine_calibration))
        .routes(routes!(projects::create_project_handler))
        .routes(routes!(projects::list_projects_handler))
        .routes(routes!(projects::get_project_handler))
        .with_state(state)
        .split_for_parts();

    let app = router.route(
        "/api-docs/openapi.json",
        get(move || async move { Json(api) }),
    );

    let port = service_conf.port;
    let listener = TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap_or_else(|_| panic!("failed to bind 0.0.0.0:{port}"));

    tracing::info!("listening on {}", listener.local_addr().unwrap());

    axum::serve(listener, app).await.expect("server error");
    Ok(())
}
