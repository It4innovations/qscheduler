use crate::backend_iqm::IqmBackendConfig;
use bytes::Bytes;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum BackendConfig {
    Iqm(IqmBackendConfig),
    Test,
}

use crate::TaskId;
use crate::backend_iqm::start_iqm_backend;
use crate::backend_test::start_test_backend;
use crate::task::TaskState;

pub(crate) trait Backend: Send + Sync {
    fn submit_task(&self, task_id: TaskId, payload: Bytes);
    fn cancel_task(&self, task_id: TaskId, backend_id: &str);
    fn get_arch(&self) -> oneshot::Receiver<crate::Result<String>>;
    fn get_calibration(&self, calibration_id: &str, endpoint: &str) -> oneshot::Receiver<crate::Result<String>>;
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
) -> (Box<dyn Backend>, UnboundedReceiver<FromBackendMessage>) {
    match config {
        BackendConfig::Iqm(config) => start_iqm_backend(config),
        BackendConfig::Test => start_test_backend(),
    }
}
