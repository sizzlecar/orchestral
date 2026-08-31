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
    pub actions: Vec<AgentSessionActionView>,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamEvent {
    Durable { id: Option<String>, data: String },
    Telemetry { data: String },
    Error { data: String },
    KeepAlive,
}
