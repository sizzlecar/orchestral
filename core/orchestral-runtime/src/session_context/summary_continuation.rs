//! Compact, atomic receipts for the Artifact page output contract.

use super::*;

pub(super) fn artifact_page_observation(group: &SessionCompactionGroup) -> Option<String> {
    // A receipt for a successful page must not displace an error from the
    // same atomic exchange. Keep the general outcome renderer in that case.
    if group
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .any(|content| matches!(content, ModelContent::ToolResult { is_error: true, .. }))
    {
        return None;
    }
    let mut receipts = Vec::new();
    for message in &group.messages {
        for content in &message.content {
            let ModelContent::ToolResult {
                call_id,
                result,
                is_error: false,
            } = content
            else {
                continue;
            };
            let artifact_ref = result.get("artifact_ref")?.as_str()?;
            let offset = result.get("offset")?.as_u64()?;
            let next = result.get("next_offset")?.as_u64()?;
            let size = result.get("total_bytes")?.as_u64()?;
            let complete = result.get("complete")?.as_bool()?;
            let bytes = result.get("bytes_read")?.as_u64()?;
            let text = result.get("content")?.as_str()?;
            if artifact_ref.is_empty()
                || next > size
                || next.checked_sub(offset) != Some(bytes)
                || bytes != text.len() as u64
                || complete != (next == size)
                || (!complete && bytes == 0)
            {
                return None;
            }
            let name = group
                .messages
                .iter()
                .flat_map(|message| &message.content)
                .find_map(|content| match content {
                    ModelContent::ToolCall {
                        call_id: candidate,
                        name,
                        ..
                    } if candidate == call_id => Some(name),
                    _ => None,
                })?;
            let continuation = serde_json::json!({"artifact_ref":artifact_ref,"next_offset":next,"complete":complete});
            receipts.push(format!(
                "session_seq={}..{} {name} status=succeeded {}",
                group.source.first_session_seq,
                group.source.last_session_seq,
                canonical_summary_json(&continuation).ok()?,
            ));
        }
    }
    (!receipts.is_empty()).then(|| receipts.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::model_protocol::ModelToolCallId;

    fn page_group() -> SessionCompactionGroup {
        let call_id = ModelToolCallId::new("page");
        SessionCompactionGroup {
            source: single_range(3),
            messages: vec![
                ModelMessage {
                    role: ModelRole::Assistant,
                    content: vec![ModelContent::ToolCall {
                        call_id: call_id.clone(),
                        name: "custom_page_tool".to_owned(),
                        arguments: serde_json::json!({}),
                        extensions: Default::default(),
                    }],
                },
                ModelMessage {
                    role: ModelRole::Tool,
                    content: vec![ModelContent::ToolResult {
                        call_id,
                        result: serde_json::json!({
                            "artifact_ref":"opaque-reference", "offset":0,
                            "next_offset":6,"total_bytes":64,"complete":false,
                            "bytes_read":6,"content":"你好",
                        }),
                        is_error: false,
                    }],
                },
            ],
        }
    }

    #[test]
    fn continuation_requires_a_matching_successful_call_and_consistent_utf8_page() {
        assert!(artifact_page_observation(&page_group()).is_some());
        for corruption in [
            "offset",
            "next_offset",
            "complete",
            "bytes_read",
            "call",
            "error",
        ] {
            let mut group = page_group();
            let ModelContent::ToolResult {
                call_id,
                result,
                is_error,
            } = &mut group.messages[1].content[0]
            else {
                unreachable!();
            };
            match corruption {
                "offset" => result["offset"] = serde_json::json!(7),
                "next_offset" => result["next_offset"] = serde_json::json!(65),
                "complete" => result["complete"] = serde_json::json!(true),
                "bytes_read" => result["bytes_read"] = serde_json::json!(2),
                "call" => *call_id = ModelToolCallId::new("unrelated"),
                _ => *is_error = true,
            }
            assert!(artifact_page_observation(&group).is_none(), "{corruption}");
        }
        let mut mixed = page_group();
        mixed.messages[1].content.push(ModelContent::ToolResult {
            call_id: ModelToolCallId::new("failed-check"),
            result: serde_json::json!({"reason":"check failed"}),
            is_error: true,
        });
        assert!(artifact_page_observation(&mixed).is_none());
    }
}
