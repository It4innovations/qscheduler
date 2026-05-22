use crate::error::RunnerError;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub enum BackendConfig {
    Iqm { backend_url: String },
    Test,
}

#[derive(Clone)]
pub(crate) enum Backend {
    Iqm { backend_url: String },
    Test,
}

impl Backend {
    pub fn new(config: &BackendConfig) -> Self {
        match config {
            BackendConfig::Iqm { backend_url } => Backend::Iqm { backend_url: backend_url.clone() },
            BackendConfig::Test => Backend::Test,
        }
    }
    pub async fn run_task(&self, payload: Arc<[u8]>) -> crate::Result<()> {
        match self {
            Backend::Iqm { backend_url } => {
                let _ = backend_url;
                todo!()
            },
            Backend::Test => {
                let message: TestBackendMessage = serde_json::from_slice(payload.as_ref())
                    .map_err(|err| {
                        RunnerError::GenericError(format!(
                            "Error deserializing test payload: {:?}",
                            err
                        ))
                    })?;
                message.process().await
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct TestBackendMessage {
    result: TestBackendResult,
    #[serde(default)]
    wait: f32,
}

impl TestBackendMessage {
    pub async fn process(self) -> crate::Result<()> {
        if self.wait > 0.0 {
            let ms = (self.wait * 1000.0).round() as u64;
            tokio::time::sleep(tokio::time::Duration::from_millis(ms)).await;
        }
        match self.result {
            TestBackendResult::Ok => Ok(()),
            TestBackendResult::Fail { message } => Err(RunnerError::TaskFail(message)),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum TestBackendResult {
    Ok,
    Fail { message: String },
}
