use std::collections::BTreeMap;

use orchestral_core::agent_connector::{
    AgentConnectorError, AgentConnectorId, AgentSessionActivity, AgentSessionActivityId,
    AgentSessionActivityKind, AgentSessionActivityStatus, AgentSessionDetail, AgentSessionPage,
    AgentSessionState, AgentSessionSummary, AgentSessionTurn, AgentSessionTurnId,
    AgentSessionTurnStatus,
};
use orchestral_core::agent_protocol::wire::{AgentSessionId, Content};
use serde_json::{json, Map, Value};

#[derive(Debug, Clone)]
pub(crate) struct NormalizationLimits {
    pub max_text_chars: usize,
    pub max_detail_chars: usize,
    pub max_activities_per_turn: usize,
}

impl Default for NormalizationLimits {
    fn default() -> Self {
        Self {
            max_text_chars: 32_000,
            max_detail_chars: 8_000,
            max_activities_per_turn: 2_000,
        }
    }
}

pub(crate) fn session_page(
    connector_id: &AgentConnectorId,
    result: &Value,
) -> Result<AgentSessionPage, AgentConnectorError> {
    let data = result
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| AgentConnectorError::protocol("thread/list omitted data"))?;
    let sessions = data
        .iter()
        .map(|thread| session_summary(connector_id, thread))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AgentSessionPage {
        sessions,
        next_cursor: result
            .get("nextCursor")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

pub(crate) fn session_detail(
    connector_id: &AgentConnectorId,
    result: &Value,
    limits: &NormalizationLimits,
) -> Result<AgentSessionDetail, AgentConnectorError> {
    let thread = result
        .get("thread")
        .ok_or_else(|| AgentConnectorError::protocol("thread/read omitted thread"))?;
    let turns = thread
        .get("turns")
        .and_then(Value::as_array)
        .map(|turns| {
            turns
                .iter()
                .map(|turn| normalize_turn(turn, limits))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    Ok(AgentSessionDetail {
        summary: session_summary(connector_id, thread)?,
        turns,
        pending_requests: Vec::new(),
    })
}

pub(crate) fn session_summary(
    connector_id: &AgentConnectorId,
    thread: &Value,
) -> Result<AgentSessionSummary, AgentConnectorError> {
    let id = required_string(thread, "id", "thread")?;
    let status = thread.get("status").unwrap_or(&Value::Null);
    let mut extensions = BTreeMap::new();
    copy_string_extension(thread, "modelProvider", &mut extensions);
    copy_string_extension(thread, "cliVersion", &mut extensions);
    if let Some(source) = thread.get("source") {
        extensions.insert("source".to_owned(), bounded_json(source, 1_000));
    }
    Ok(AgentSessionSummary {
        connector_id: connector_id.clone(),
        session_id: AgentSessionId::new(id),
        title: optional_string(thread, "name"),
        preview: optional_string(thread, "preview"),
        cwd: optional_string(thread, "cwd"),
        created_at_unix_ms: timestamp_ms(thread.get("createdAt")),
        updated_at_unix_ms: timestamp_ms(thread.get("updatedAt")),
        state: session_state(status),
        extensions,
    })
}

fn normalize_turn(
    turn: &Value,
    limits: &NormalizationLimits,
) -> Result<AgentSessionTurn, AgentConnectorError> {
    let id = required_string(turn, "id", "turn")?;
    let items = turn
        .get("items")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let activities = items
        .iter()
        .take(limits.max_activities_per_turn)
        .enumerate()
        .map(|(index, item)| normalize_activity(id, index, item, limits))
        .collect::<Vec<_>>();
    Ok(AgentSessionTurn {
        turn_id: AgentSessionTurnId::new(id),
        status: turn_status(turn.get("status").and_then(Value::as_str)),
        activities,
    })
}

fn normalize_activity(
    turn_id: &str,
    index: usize,
    item: &Value,
    limits: &NormalizationLimits,
) -> AgentSessionActivity {
    let item_type = item.get("type").and_then(Value::as_str).unwrap_or("other");
    let item_id = item
        .get("id")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{turn_id}-item-{index}"));
    let (kind, title, text) = match item_type {
        "userMessage" => (
            AgentSessionActivityKind::UserMessage,
            Some("User".to_owned()),
            message_content(item.get("content")),
        ),
        "agentMessage" => (
            AgentSessionActivityKind::AgentMessage,
            Some("Agent".to_owned()),
            optional_string(item, "text"),
        ),
        "reasoning" => (
            AgentSessionActivityKind::Reasoning,
            Some("Reasoning".to_owned()),
            reasoning_content(item),
        ),
        "plan" => (
            AgentSessionActivityKind::Plan,
            Some("Plan".to_owned()),
            json_text(item.get("items")),
        ),
        "commandExecution" => (
            AgentSessionActivityKind::Command,
            optional_string(item, "command"),
            optional_string(item, "aggregatedOutput"),
        ),
        "fileChange" => (
            AgentSessionActivityKind::FileChange,
            Some("File changes".to_owned()),
            json_text(item.get("changes")),
        ),
        "contextCompaction" => (
            AgentSessionActivityKind::Compaction,
            Some("Context compacted".to_owned()),
            None,
        ),
        "mcpToolCall" | "dynamicToolCall" | "collabAgentToolCall" | "webSearch" => (
            AgentSessionActivityKind::ToolCall,
            optional_string(item, "tool")
                .or_else(|| optional_string(item, "name"))
                .or_else(|| Some(item_type.to_owned())),
            json_text(item.get("result")),
        ),
        _ => (
            AgentSessionActivityKind::Other,
            Some(item_type.to_owned()),
            None,
        ),
    };
    let content = text
        .filter(|text| !text.is_empty())
        .map(|text| vec![Content::text(truncate_chars(&text, limits.max_text_chars))])
        .unwrap_or_default();
    AgentSessionActivity {
        activity_id: AgentSessionActivityId::new(item_id),
        kind,
        status: activity_status(item),
        title: title.map(|title| truncate_chars(&title, 1_000)),
        content,
        details: selected_details(item, limits.max_detail_chars),
    }
}

fn selected_details(item: &Value, max_chars: usize) -> Value {
    let mut selected = Map::new();
    for key in [
        "type",
        "status",
        "cwd",
        "exitCode",
        "durationMs",
        "phase",
        "server",
        "tool",
    ] {
        if let Some(value) = item.get(key) {
            selected.insert(key.to_owned(), bounded_json(value, max_chars));
        }
    }
    Value::Object(selected)
}

fn bounded_json(value: &Value, max_chars: usize) -> Value {
    match value {
        Value::String(text) => Value::String(truncate_chars(text, max_chars)),
        Value::Null | Value::Bool(_) | Value::Number(_) => value.clone(),
        _ => Value::String(truncate_chars(&value.to_string(), max_chars)),
    }
}

fn session_state(status: &Value) -> AgentSessionState {
    match status.get("type").and_then(Value::as_str) {
        Some("idle") => AgentSessionState::Idle,
        Some("active") => {
            let flags = status
                .get("activeFlags")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or_default();
            if flags.iter().any(|flag| flag == "waitingOnApproval") {
                AgentSessionState::WaitingApproval
            } else if flags.iter().any(|flag| flag == "waitingOnUserInput") {
                AgentSessionState::WaitingInput
            } else {
                AgentSessionState::Active
            }
        }
        Some("systemError") => AgentSessionState::Unavailable,
        _ => AgentSessionState::Detached,
    }
}

fn turn_status(status: Option<&str>) -> AgentSessionTurnStatus {
    match status {
        Some("inProgress") => AgentSessionTurnStatus::Active,
        Some("interrupted") => AgentSessionTurnStatus::Interrupted,
        Some("failed") => AgentSessionTurnStatus::Failed,
        Some("completed") => AgentSessionTurnStatus::Completed,
        _ => AgentSessionTurnStatus::Pending,
    }
}

fn activity_status(item: &Value) -> AgentSessionActivityStatus {
    match item.get("status").and_then(Value::as_str) {
        Some("inProgress") => AgentSessionActivityStatus::Active,
        Some("failed") => AgentSessionActivityStatus::Failed,
        Some("declined") => AgentSessionActivityStatus::Declined,
        Some("interrupted") => AgentSessionActivityStatus::Interrupted,
        Some("pending") => AgentSessionActivityStatus::Pending,
        _ => AgentSessionActivityStatus::Completed,
    }
}

fn message_content(value: Option<&Value>) -> Option<String> {
    let content = value?.as_array()?;
    let parts = content
        .iter()
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>();
    (!parts.is_empty()).then(|| parts.join("\n"))
}

fn reasoning_content(item: &Value) -> Option<String> {
    ["summary", "content"]
        .into_iter()
        .filter_map(|key| json_text(item.get(key)))
        .reduce(|mut left, right| {
            left.push('\n');
            left.push_str(&right);
            left
        })
}

fn json_text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Array(values) => {
            let text = values
                .iter()
                .filter_map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .or_else(|| value.get("text").and_then(Value::as_str).map(str::to_owned))
                })
                .collect::<Vec<_>>()
                .join("\n");
            (!text.is_empty()).then_some(text)
        }
        other => Some(other.to_string()),
    }
}

fn timestamp_ms(value: Option<&Value>) -> Option<i64> {
    value?.as_i64()?.checked_mul(1_000)
}

fn optional_string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_owned)
}

fn required_string<'a>(
    value: &'a Value,
    key: &str,
    subject: &str,
) -> Result<&'a str, AgentConnectorError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| AgentConnectorError::protocol(format!("{subject} omitted {key}")))
}

fn copy_string_extension(source: &Value, key: &str, target: &mut BTreeMap<String, Value>) {
    if let Some(value) = source.get(key).and_then(Value::as_str) {
        target.insert(key.to_owned(), json!(value));
    }
}

fn truncate_chars(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.to_owned();
    }
    let mut truncated = value.chars().take(limit).collect::<String>();
    truncated.push_str("\n… truncated by Orchestral …");
    truncated
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_known_items_and_bounds_large_outputs() {
        let detail = session_detail(
            &AgentConnectorId::new("codex/local"),
            &json!({
                "thread": {
                    "id": "thread-1",
                    "preview": "fix it",
                    "cwd": "/work",
                    "createdAt": 10,
                    "updatedAt": 20,
                    "status": {"type": "active", "activeFlags": ["waitingOnApproval"]},
                    "turns": [{
                        "id": "turn-1",
                        "status": "completed",
                        "items": [
                            {"type": "userMessage", "id": "u1", "content": [{"type": "text", "text": "hello"}]},
                            {"type": "commandExecution", "id": "c1", "command": "cargo test", "status": "completed", "aggregatedOutput": "abcdefghijklmnop"},
                            {"type": "fileChange", "id": "f1", "status": "completed", "changes": ["src/lib.rs"]}
                        ]
                    }]
                }
            }),
            &NormalizationLimits {
                max_text_chars: 8,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(detail.summary.state, AgentSessionState::WaitingApproval);
        assert_eq!(detail.turns[0].activities.len(), 3);
        assert_eq!(
            detail.turns[0].activities[1].kind,
            AgentSessionActivityKind::Command
        );
        let body = serde_json::to_string(&detail.turns[0].activities[1].content).unwrap();
        assert!(body.contains("truncated by Orchestral"));
    }

    #[test]
    fn unknown_items_remain_visible_without_copying_the_payload() {
        let activity = normalize_activity(
            "turn-1",
            0,
            &json!({"type": "futureItem", "id": "x", "secretPayload": "large"}),
            &NormalizationLimits::default(),
        );
        assert_eq!(activity.kind, AgentSessionActivityKind::Other);
        assert_eq!(activity.title.as_deref(), Some("futureItem"));
        assert!(activity.details.get("secretPayload").is_none());
    }
}
