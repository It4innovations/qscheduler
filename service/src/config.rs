use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ServiceConfiguration {
    pub port: u16,
}
