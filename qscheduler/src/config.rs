use runner::config::MachineConfiguration;
use serde::Deserialize;
use service::config::ServiceConfiguration;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
pub struct ServerConfiguration {
    pub log: Option<PathBuf>,
    pub service: ServiceConfiguration,
    pub machines: Vec<MachineConfiguration>,
}

pub fn load_config(path: &Path) -> anyhow::Result<ServerConfiguration> {
    let contents = std::fs::read_to_string(path)?;
    let config = toml::from_str(&contents)?;
    Ok(config)
}
