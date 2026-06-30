use crate::MachineId;
use crate::backend::BackendConfig;
use crate::callback::NotifyConfig;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Deserialize, Serialize)]
pub struct MachineConfiguration {
    pub name: String,
    pub queue_size: usize,
    pub session_check_interval_ms: u32,
    pub notify: Option<NotifyConfig>,
    pub backend: BackendConfig,
}

#[derive(Debug)]
pub struct RunnerConfiguration {
    pub machines: Vec<(MachineId, MachineConfiguration)>,
}
