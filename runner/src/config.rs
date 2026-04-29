use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct MachineConfiguration {
    pub name: String,
    pub backend_url: String,
}

#[derive(Debug)]
pub struct RunnerConfiguration {
    pub machine: MachineConfiguration,
}