use std::path::Path;
use serde::Deserialize;
use runner::config::MachineConfiguration;
use service::config::ServiceConfiguration;

#[derive(Debug, Deserialize)]
pub struct ServerConfiguration {
    pub service: ServiceConfiguration,
    pub machine: MachineConfiguration,
}

pub fn load_config(path: &Path) -> anyhow::Result<ServerConfiguration> {
    let contents = std::fs::read_to_string(path)?;
    let config = toml::from_str(&contents)?;
    Ok(config)
}