use crate::backend_iqm::IqmBackendConfig;
use bytes::Bytes;
use serde::Deserialize;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;

#[derive(Debug, Deserialize, serde::Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum BackendConfig {
    Iqm(IqmBackendConfig),
    Test,
}

use crate::TaskId;
use crate::backend_iqm::start_iqm_backend;
use crate::backend_test::start_test_backend;
use crate::task::TaskState;

/// A boxed future standing in for an `async fn` return on a dyn-compatible trait.
pub(crate) type BackendFuture<T> = Pin<Box<dyn Future<Output = crate::Result<T>> + Send>>;
/// A boxed stream of raw result/artifact bytes as they arrive from the backend.
pub(crate) type ByteStream = Pin<Box<dyn futures_util::Stream<Item = crate::Result<Bytes>> + Send>>;

pub(crate) trait Backend: Send + Sync {
    fn submit_task(self: Arc<Self>, task_id: TaskId, payload: Bytes);
    /// Re-attach to a task that was already submitted before a restart, using the
    /// backend job id persisted in the database. The backend resumes monitoring it
    /// and reports its current state via [`FromBackendMessage`].
    fn resume_task(
        self: Arc<Self>,
        task_id: TaskId,
        backend_id: String,
        payload: Bytes,
        cancel: bool,
    );
    fn cancel_task(self: Arc<Self>, task_id: TaskId, backend_id: &str);
    fn get_arch(self: Arc<Self>) -> BackendFuture<String>;
    fn get_calibration(
        self: Arc<Self>,
        calibration_id: &str,
        endpoint: &str,
    ) -> BackendFuture<String>;
    fn get_task_result(self: Arc<Self>, backend_id: &str) -> BackendFuture<ByteStream>;
    fn get_task_artifact(
        self: Arc<Self>,
        backend_id: &str,
        name: &str,
    ) -> BackendFuture<ByteStream>;
}

pub(crate) enum FromBackendMessage {
    TaskSubmitted {
        task_id: TaskId,
        backend_task_id: String,
    },
    TaskStateChange {
        task_id: TaskId,
        state: TaskState,
    },
}

pub fn create_backend(
    config: &BackendConfig,
) -> (Arc<dyn Backend>, UnboundedReceiver<FromBackendMessage>) {
    match config {
        BackendConfig::Iqm(config) => start_iqm_backend(config),
        BackendConfig::Test => start_test_backend(),
    }
}
