use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadView {
    pub id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InteractionSubmitRequest {
    pub request_id: Option<String>,
    pub input: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SubmitStatus {
    Started,
    Merged,
    Rejected,
    Queued,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InteractionSubmitResponse {
    pub status: SubmitStatus,
    pub interaction_id: Option<String>,
    pub task_id: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEventView {
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
    pub role: String,
    pub content: String,
}
