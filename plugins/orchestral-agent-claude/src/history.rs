use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use orchestral_core::agent_connector::{
    AgentConnectorError, AgentSessionActivity, AgentSessionActivityId, AgentSessionActivityKind,
    AgentSessionActivityStatus, AgentSessionTurn, AgentSessionTurnId, AgentSessionTurnStatus,
};
use orchestral_core::agent_protocol::wire::Content;
use serde_json::{json, Value};

const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;

/// Ignore only the unfinished tail while Claude is appending a JSONL record.
/// A corrupt committed record must not silently become missing history.
pub(crate) fn records(path: &Path) -> Result<Vec<Value>, AgentConnectorError> {
    let file = std::fs::File::open(path).map_err(super::io_error)?;
    let mut reader = BufReader::new(file);
    let mut result = Vec::new();
    loop {
        let mut line = Vec::new();
        let read = reader
            .by_ref()
            .take((MAX_RECORD_BYTES + 1) as u64)
            .read_until(b'\n', &mut line)
            .map_err(super::io_error)?;
        if read == 0 {
            break;
        }
        if line.len() > MAX_RECORD_BYTES {
            return Err(AgentConnectorError::protocol(
                "Claude history record is too large",
            ));
        }
        match serde_json::from_slice(&line) {
            Ok(value) => result.push(value),
            Err(_) if line.last() != Some(&b'\n') => break,
            Err(error) => {
                return Err(AgentConnectorError::protocol(format!(
                    "invalid Claude history record: {error}"
                )))
            }
        }
    }
    Ok(result)
}

pub(crate) fn timestamp(value: &Value) -> Option<i64> {
    value
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(|text| chrono::DateTime::parse_from_rfc3339(text).ok())
        .map(|time| time.timestamp_millis())
        .filter(|time| *time >= 0)
}

pub(crate) fn text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Array(blocks) => blocks
            .iter()
            .filter_map(|block| block.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

/// Follow the current conversation leaf. Old branches left by a rewind must
/// not appear as new messages, and subagent transcripts are separate sessions.
fn conversation(records: &[Value]) -> Vec<&Value> {
    let messages: Vec<_> = records
        .iter()
        .filter(|record| {
            record.get("isSidechain").and_then(Value::as_bool) != Some(true)
                && matches!(
                    record.get("type").and_then(Value::as_str),
                    Some("user" | "assistant" | "system" | "attachment")
                )
                && record.get("uuid").and_then(Value::as_str).is_some()
        })
        .collect();
    let by_id: HashMap<_, _> = messages
        .iter()
        .filter_map(|record| {
            record
                .get("uuid")
                .and_then(Value::as_str)
                .map(|id| (id, *record))
        })
        .collect();
    let Some(last) = messages.last() else {
        return Vec::new();
    };
    // Older exports have no ancestry. Preserve their append order.
    if !last
        .get("parentUuid")
        .is_some_and(|value| value.is_string())
    {
        return messages;
    }
    let mut ids = HashSet::new();
    let mut next = last.get("uuid").and_then(Value::as_str);
    while let Some(id) = next {
        if !ids.insert(id) {
            break;
        }
        next = by_id
            .get(id)
            .and_then(|record| record.get("parentUuid"))
            .and_then(Value::as_str);
    }
    messages
        .into_iter()
        .filter(|record| {
            record
                .get("uuid")
                .and_then(Value::as_str)
                .is_some_and(|id| ids.contains(id))
        })
        .collect()
}

pub(crate) fn turns(records: &[Value]) -> Vec<AgentSessionTurn> {
    let mut turns = Vec::<AgentSessionTurn>::new();
    let mut tools = BTreeMap::<String, (usize, usize)>::new();
    for record in conversation(records) {
        let kind = record
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let uuid = record
            .get("uuid")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let content = record.pointer("/message/content").unwrap_or(&Value::Null);
        let peer_input = record
            .get("origin")
            .and_then(|origin| origin.get("kind"))
            .and_then(Value::as_str)
            == Some("peer")
            || record
                .get("inputOrigin")
                .and_then(|origin| origin.get("kind"))
                .and_then(Value::as_str)
                == Some("peer");
        let is_user = kind == "user"
            && (record.get("isMeta").and_then(Value::as_bool) != Some(true) || peer_input)
            && (content.is_string()
                || content.as_array().is_some_and(|blocks| {
                    blocks.iter().any(|block| {
                        matches!(
                            block.get("type").and_then(Value::as_str),
                            Some("text" | "image")
                        )
                    })
                }));
        if is_user {
            // A subsequent user turn establishes that the earlier turn ended.
            if let Some(turn) = turns.last_mut() {
                if turn.status == AgentSessionTurnStatus::Active {
                    turn.status = AgentSessionTurnStatus::Completed;
                }
            }
            turns.push(AgentSessionTurn {
                turn_id: AgentSessionTurnId::new(format!("claude-turn:{uuid}")),
                status: AgentSessionTurnStatus::Active,
                failure: None,
                activities: Vec::new(),
            });
            let mut activity = activity(
                uuid,
                AgentSessionActivityKind::UserMessage,
                &text(content),
                record,
            );
            if let Some(client_id) = record.get("promptId").and_then(Value::as_str) {
                activity.details["clientId"] = json!(client_id);
            } else if peer_input {
                activity.details["clientId"] = json!(uuid);
            }
            turns.last_mut().unwrap().activities.push(activity);
        }
        if turns.is_empty() {
            continue;
        }
        if kind == "attachment"
            && record.pointer("/attachment/type").and_then(Value::as_str) == Some("queued_command")
            && record
                .pointer("/attachment/origin/kind")
                .and_then(Value::as_str)
                == Some("peer")
        {
            if let Some(source_id) = record
                .pointer("/attachment/source_uuid")
                .and_then(Value::as_str)
            {
                let mut item = activity(
                    source_id,
                    AgentSessionActivityKind::UserMessage,
                    &text(record.pointer("/attachment/prompt").unwrap_or(&Value::Null)),
                    record,
                );
                item.details["clientId"] = json!(source_id);
                turns.last_mut().unwrap().activities.push(item);
            }
        }
        if kind == "system" {
            if record.get("subtype").and_then(Value::as_str) == Some("turn_duration") {
                turns.last_mut().unwrap().status = AgentSessionTurnStatus::Completed;
            } else if record.get("subtype").and_then(Value::as_str) == Some("compact_boundary") {
                turns.last_mut().unwrap().activities.push(activity(
                    uuid,
                    AgentSessionActivityKind::Compaction,
                    "Conversation compacted",
                    record,
                ));
            }
        }
        if kind == "assistant" {
            if let Some(blocks) = content.as_array() {
                for (index, block) in blocks.iter().enumerate() {
                    let id = format!("{uuid}:{index}");
                    let activity = match block.get("type").and_then(Value::as_str) {
                        Some("text") => activity(
                            &id,
                            AgentSessionActivityKind::AgentMessage,
                            block
                                .get("text")
                                .and_then(Value::as_str)
                                .unwrap_or_default(),
                            record,
                        ),
                        Some("thinking") => activity(
                            &id,
                            AgentSessionActivityKind::Reasoning,
                            block
                                .get("thinking")
                                .and_then(Value::as_str)
                                .unwrap_or_default(),
                            record,
                        ),
                        Some("tool_use") => {
                            let Some(tool_id) = block.get("id").and_then(Value::as_str) else {
                                continue;
                            };
                            let name = block.get("name").and_then(Value::as_str).unwrap_or("Tool");
                            let mut item =
                                activity(&format!("tool:{tool_id}"), tool_kind(name), "", record);
                            item.title = Some(name.to_owned());
                            item.status = AgentSessionActivityStatus::Active;
                            item.details["arguments"] =
                                block.get("input").cloned().unwrap_or(Value::Null);
                            tools.insert(
                                tool_id.to_owned(),
                                (turns.len() - 1, turns.last().unwrap().activities.len()),
                            );
                            item
                        }
                        _ => continue,
                    };
                    turns.last_mut().unwrap().activities.push(activity);
                }
            }
            if record
                .pointer("/message/stop_reason")
                .and_then(Value::as_str)
                == Some("end_turn")
            {
                turns.last_mut().unwrap().status = AgentSessionTurnStatus::Completed;
            }
        }
        if kind == "user" && !is_user {
            for block in content.as_array().into_iter().flatten() {
                if block.get("type").and_then(Value::as_str) != Some("tool_result") {
                    continue;
                }
                if let Some((turn_index, activity_index)) = block
                    .get("tool_use_id")
                    .and_then(Value::as_str)
                    .and_then(|id| tools.get(id))
                    .copied()
                {
                    let item = &mut turns[turn_index].activities[activity_index];
                    item.status = if block.get("is_error").and_then(Value::as_bool) == Some(true) {
                        AgentSessionActivityStatus::Failed
                    } else {
                        AgentSessionActivityStatus::Completed
                    };
                    let result = text(block.get("content").unwrap_or(&Value::Null));
                    if !result.is_empty() {
                        item.content.push(Content::text(result));
                    }
                }
            }
        }
    }
    turns
}

fn activity(
    id: &str,
    kind: AgentSessionActivityKind,
    text: &str,
    record: &Value,
) -> AgentSessionActivity {
    AgentSessionActivity {
        activity_id: AgentSessionActivityId::new(format!("claude:{id}")),
        occurred_at_unix_ms: timestamp(record),
        kind,
        status: AgentSessionActivityStatus::Completed,
        title: None,
        content: if text.is_empty() {
            Vec::new()
        } else {
            vec![Content::text(text)]
        },
        details: json!({}),
    }
}

fn tool_kind(name: &str) -> AgentSessionActivityKind {
    match name {
        "Bash" | "PowerShell" => AgentSessionActivityKind::Command,
        "Edit" | "Write" | "NotebookEdit" => AgentSessionActivityKind::FileChange,
        _ => AgentSessionActivityKind::ToolCall,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn peer_input_consumed_mid_turn_uses_original_submission_identity() {
        let records = [
            json!({"type":"user","uuid":"first","message":{"content":"initial task"}}),
            json!({"type":"attachment","uuid":"attachment","parentUuid":"first","attachment":{"type":"queued_command","source_uuid":"peer-input","prompt":"additional task","origin":{"kind":"peer"}}}),
            json!({"type":"assistant","uuid":"reply","parentUuid":"attachment","message":{"content":[{"type":"text","text":"continued"}]}}),
        ];
        let result = turns(&records);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].activities.len(), 3);
        assert_eq!(
            result[0].activities[1].activity_id.as_str(),
            "claude:peer-input"
        );
        assert_eq!(result[0].activities[1].details["clientId"], "peer-input");
        assert_eq!(
            result[0].activities[1].kind,
            AgentSessionActivityKind::UserMessage
        );
    }

    #[test]
    fn peer_input_stays_visible_while_internal_meta_messages_are_hidden() {
        let records = vec![
            json!({"type":"user","uuid":"u1","parentUuid":null,"isMeta":true,"origin":{"kind":"peer"},"message":{"content":"sent from another client"}}),
            json!({"type":"user","uuid":"internal","parentUuid":"u1","isMeta":true,"message":{"content":"internal context"}}),
            json!({"type":"assistant","uuid":"a1","parentUuid":"internal","message":{"content":[{"type":"text","text":"response"}],"stop_reason":"end_turn"}}),
        ];
        let turns = turns(&records);
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].activities.len(), 2);
        assert_eq!(
            turns[0].activities[0].content,
            vec![Content::text("sent from another client")]
        );
        assert_eq!(
            turns[0].activities[1].content,
            vec![Content::text("response")]
        );
    }

    #[test]
    fn tool_results_update_the_original_tool_and_rewinds_select_the_current_branch() {
        let records = vec![
            json!({"type":"user","uuid":"u1","parentUuid":null,"message":{"content":"inspect"}}),
            json!({"type":"assistant","uuid":"a1","parentUuid":"u1","message":{"content":[{"type":"tool_use","id":"t1","name":"Read","input":{"file_path":"src/lib.rs"}}]}}),
            json!({"type":"user","uuid":"r1","parentUuid":"a1","message":{"content":[{"type":"tool_result","tool_use_id":"t1","content":"contents"}]}}),
            json!({"type":"assistant","uuid":"old","parentUuid":"r1","message":{"content":[{"type":"text","text":"discarded"}]}}),
            json!({"type":"assistant","uuid":"new","parentUuid":"r1","message":{"content":[{"type":"text","text":"selected"}],"stop_reason":"end_turn"}}),
        ];
        let turns = turns(&records);
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].activities.len(), 3);
        assert_eq!(
            turns[0].activities[1].status,
            AgentSessionActivityStatus::Completed
        );
        assert_eq!(
            turns[0].activities[2].content,
            vec![Content::text("selected")]
        );
        assert_eq!(turns[0].status, AgentSessionTurnStatus::Completed);
    }

    #[test]
    fn incomplete_tail_is_ignored_but_committed_corruption_is_reported() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("history.jsonl");
        std::fs::write(&path, b"{\"type\":\"user\"}\n{\"typ").unwrap();
        assert_eq!(records(&path).unwrap().len(), 1);
        std::fs::write(&path, b"invalid\n").unwrap();
        assert!(records(&path).is_err());
    }
}
