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
pub struct UploadedArtifact {
    pub artifact_ref: String,
    pub file_name: String,
    pub media_type: String,
    pub byte_size: u64,
    pub sha256: String,
    pub download_url: String,
    #[serde(default)]
    pub agent_url: Option<String>,
    #[serde(default)]
    pub expires_at: Option<String>,
}

impl UploadedArtifact {
    pub fn command_value(&self) -> Value {
        serde_json::json!({
            "artifact_ref": self.artifact_ref,
            "digest": self.sha256,
            "file_name": self.file_name,
            "media_type": self.media_type,
            "byte_size": self.byte_size,
        })
    }
}

/// Durable browser intent written before an Agent control request leaves the
/// device. Replays retain the original Run/Command identity, so multiple tabs
/// and ambiguous network failures cannot execute the input twice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxEntry {
    pub id: String,
    pub connector_id: Option<String>,
    pub session_id: String,
    pub input: String,
    pub attachments: Vec<UploadedArtifact>,
    pub native_anchor_id: Option<String>,
    pub created_at_unix_ms: i64,
    pub operation: OutboxOperation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutboxOperation {
    Start { run_id: String },
    Steer { run_id: String, command_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionView {
    pub id: String,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    #[serde(default)]
    pub run_ids: Vec<String>,
    #[serde(default)]
    pub connector_id: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub preview: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
}

impl SessionView {
    pub fn key(&self) -> String {
        match &self.connector_id {
            Some(connector_id) => format!("{connector_id}\0{}", self.id),
            None => self.id.clone(),
        }
    }

    pub fn history_run_id(&self) -> Option<String> {
        self.connector_id
            .as_ref()
            .map(|connector_id| format!("agent-history:{connector_id}:{}", self.id))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentConnectorView {
    pub connector_id: String,
    pub display_name: String,
    pub agent_family: String,
    pub capabilities: AgentSessionCapabilitiesView,
    #[serde(default)]
    pub creation: Option<AgentSessionCreationView>,
    #[serde(default)]
    pub actions: Vec<AgentSessionActionView>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSessionCreationView {
    pub accepts_cwd: bool,
    #[serde(default)]
    pub default_cwd: Option<String>,
    #[serde(default)]
    pub input_schema: Option<Value>,
    #[serde(default)]
    pub connection_hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSessionCapabilitiesView {
    pub list: bool,
    pub read: bool,
    pub create: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSessionActionView {
    pub action_id: String,
    pub title: String,
    pub description: String,
    #[serde(default)]
    pub input_schema: Option<Value>,
    #[serde(default)]
    pub execution: AgentSessionActionExecutionView,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentSessionActionExecutionView {
    #[default]
    Immediate,
    Run,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSessionPage {
    #[serde(default)]
    pub sessions: Vec<AgentSessionSummary>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSessionSummary {
    pub connector_id: String,
    pub session_id: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub preview: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub created_at_unix_ms: Option<i64>,
    #[serde(default)]
    pub updated_at_unix_ms: Option<i64>,
    pub state: String,
}

impl AgentSessionSummary {
    pub fn into_session(self) -> SessionView {
        let connector_id = self.connector_id;
        let session_id = self.session_id;
        let history_run_id = format!("agent-history:{connector_id}:{session_id}");
        SessionView {
            id: session_id,
            created_at_unix_ms: self.created_at_unix_ms.unwrap_or_default(),
            updated_at_unix_ms: self.updated_at_unix_ms.unwrap_or_default(),
            run_ids: vec![history_run_id],
            connector_id: Some(connector_id),
            title: self.title,
            preview: self.preview,
            cwd: self.cwd,
            state: Some(self.state),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSessionActionOutcome {
    pub status: AgentSessionActionStatusView,
    #[serde(default)]
    pub session: Option<AgentSessionSummary>,
    #[serde(default)]
    pub content: Vec<Value>,
    #[serde(default)]
    pub details: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum AgentSessionActionStatusView {
    Completed,
    Running { run_id: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSessionDetail {
    pub summary: AgentSessionSummary,
    #[serde(default)]
    pub turns: Vec<AgentSessionTurn>,
    #[serde(default)]
    pub pending_requests: Vec<Value>,
    /// Host-controlled Run snapshots associated with this native Agent
    /// session. These preserve the command target across a browser reload.
    #[serde(default)]
    pub controlled_runs: Vec<Value>,
    /// Canonical Host session-stream cursor captured before this snapshot was
    /// read. It closes the snapshot/subscription race across tabs and devices.
    #[serde(default)]
    pub stream_cursor: Option<u64>,
    /// Opaque cursor for the next, older page of session history.
    #[serde(default)]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSessionTurn {
    pub turn_id: String,
    pub status: String,
    #[serde(default)]
    pub activities: Vec<AgentSessionActivity>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSessionActivity {
    pub activity_id: String,
    pub kind: String,
    pub status: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub content: Vec<Value>,
    #[serde(default)]
    pub details: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSessionChangeView {
    pub connector_id: String,
    pub session_id: String,
    pub sequence: u64,
    pub change: AgentSessionChangeKindView,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentSessionChangeKindView {
    ActivityUpsert {
        turn_id: String,
        turn_status: String,
        activity: AgentSessionActivity,
    },
    TurnStatus {
        turn_id: String,
        status: String,
    },
    RefreshRequired {
        reason: String,
    },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connector_capabilities_and_action_schema_survive_http_decoding() {
        let connector: AgentConnectorView = serde_json::from_value(serde_json::json!({
            "connector_id": "fixture/local",
            "provider_binding": "fixture/provider",
            "agent_family": "coding-agent",
            "display_name": "Fixture",
            "capabilities": {"list": true, "read": true, "create": true},
            "creation": {
                "accepts_cwd": true,
                "default_cwd": "/fixture",
                "connection_hint": "Shared owner",
                "input_schema": {
                    "type": "object",
                    "properties": {"mode": {"type": "string"}}
                }
            },
            "actions": [{
                "action_id": "session.rename",
                "title": "Rename",
                "description": "Rename this session",
                "execution": "run",
                "input_schema": {
                    "type": "object",
                    "required": ["name"],
                    "properties": {"name": {"type": "string"}}
                }
            }]
        }))
        .unwrap();

        assert!(connector.capabilities.create);
        assert!(connector.creation.as_ref().unwrap().accepts_cwd);
        assert_eq!(
            connector
                .creation
                .as_ref()
                .and_then(|creation| creation.connection_hint.as_deref()),
            Some("Shared owner")
        );
        assert_eq!(connector.actions[0].action_id, "session.rename");
        assert_eq!(
            connector.actions[0].execution,
            AgentSessionActionExecutionView::Run
        );
        assert_eq!(
            connector.actions[0].input_schema.as_ref().unwrap()["required"][0],
            "name"
        );
    }

    #[test]
    fn running_action_outcome_preserves_run_identity() {
        let outcome: AgentSessionActionOutcome = serde_json::from_value(serde_json::json!({
            "status": {"state": "running", "run_id": "review-run-1"},
            "session": null,
            "content": [],
            "details": null
        }))
        .unwrap();
        assert_eq!(
            outcome.status,
            AgentSessionActionStatusView::Running {
                run_id: "review-run-1".to_owned()
            }
        );
    }

    #[test]
    fn session_detail_preserves_the_older_history_cursor() {
        let detail: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "codex/local",
                "session_id": "thread-1",
                "state": "idle"
            },
            "turns": [],
            "pending_requests": [],
            "next_cursor": "activity-offset-v1:40"
        }))
        .unwrap();

        assert_eq!(detail.next_cursor.as_deref(), Some("activity-offset-v1:40"));
    }

    #[test]
    fn outbox_round_trip_preserves_idempotency_and_r2_artifacts() {
        let entry = OutboxEntry {
            id: "steer:command-1".to_owned(),
            connector_id: Some("codex/local".to_owned()),
            session_id: "thread-1".to_owned(),
            input: "review this".to_owned(),
            attachments: vec![UploadedArtifact {
                artifact_ref: "sha256:abc".to_owned(),
                file_name: "report.txt".to_owned(),
                media_type: "text/plain".to_owned(),
                byte_size: 12,
                sha256: "abc".to_owned(),
                download_url: "https://files.example/report.txt".to_owned(),
                agent_url: None,
                expires_at: None,
            }],
            native_anchor_id: Some("native-tail".to_owned()),
            created_at_unix_ms: 42,
            operation: OutboxOperation::Steer {
                run_id: "run-1".to_owned(),
                command_id: "command-1".to_owned(),
            },
        };

        let encoded = serde_json::to_value(&entry).unwrap();
        let decoded: OutboxEntry = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, entry);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamEvent {
    Durable { id: Option<String>, data: String },
    Telemetry { data: String },
    SessionChanged { id: Option<String>, data: String },
    Error { data: String },
    KeepAlive,
}
