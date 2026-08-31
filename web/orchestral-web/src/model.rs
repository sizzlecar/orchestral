use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceView {
    pub id: String,
    pub name: String,
    pub created_at_unix_ms: i64,
    pub last_seen_at_unix_ms: i64,
    pub current: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionView {
    pub id: String,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    #[serde(default)]
    pub run_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PairingClaim {
    pub token: String,
    pub device: DeviceView,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventsResponse {
    pub after: u64,
    pub next: u64,
    #[serde(default)]
    pub records: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamEvent {
    Durable { id: Option<String>, data: String },
    Telemetry { data: String },
    Error { data: String },
    KeepAlive,
}
