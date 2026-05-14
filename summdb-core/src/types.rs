use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Platform {
    pub os: String,
    pub arch: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestRecord {
    pub repo: String,
    pub digest: String,
    pub media_type: String,
    pub size: u64,
    pub platform: Option<Platform>,
    pub layers: Vec<String>,
}
