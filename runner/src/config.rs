use crate::backend::BackendConfig;
use crate::callback::NotifyConfig;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct MachineConfiguration {
    pub id: u32,
    pub name: String,
    pub queue_size: usize,
    pub notify: Option<NotifyConfig>,
    pub backend: BackendConfig,
}

#[derive(Debug)]
pub struct RunnerConfiguration {
    pub machines: Vec<MachineConfiguration>,
}
