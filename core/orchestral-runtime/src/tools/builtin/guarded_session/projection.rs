//! Model-readable recall is a projection of immutable journal facts. Preserve
//! content indexes for RFC 6901 pointers while hiding provider-owned state.

use orchestral_core::agent_session::{AgentSessionEvent, AgentSessionRecord};
use orchestral_core::model_protocol::{ModelContent, ModelMessage};
use serde_json::{json, Value};

pub(super) struct ReadableRecord {
    pub(super) value: Value,
    hidden: Vec<String>,
}

impl ReadableRecord {
    pub(super) fn new(record: &AgentSessionRecord) -> Result<Self, String> {
        let mut result = Self {
            value: serde_json::to_value(record).map_err(|error| error.to_string())?,
            hidden: Vec::new(),
        };
        let mut project = |message: &ModelMessage, field: &str| {
            let content = result.value["payload"][field]["content"]
                .as_array_mut()
                .expect("typed ModelMessage serializes with a content array");
            for (index, part) in message.content.iter().enumerate() {
                let path = format!("/payload/{field}/content/{index}");
                match part {
                    ModelContent::ToolCall { .. } => {
                        content[index]["extensions"] = json!({});
                        result.hidden.push(format!("{path}/extensions"));
                    }
                    ModelContent::Data { media_type, .. } if media_type.starts_with("image/") => {
                        content[index]["value"] = Value::Null;
                        result.hidden.push(format!("{path}/value"));
                    }
                    ModelContent::Text { .. }
                    | ModelContent::Json { .. }
                    | ModelContent::Data { .. }
                    | ModelContent::ToolResult { .. } => {}
                    // Continuation and future unknown variants are opaque;
                    // new readable kinds must be explicitly admitted here.
                    _ => {
                        content[index] = Value::Null;
                        result.hidden.push(path);
                    }
                }
            }
        };
        match &record.payload {
            AgentSessionEvent::RunInputCommitted { message }
            | AgentSessionEvent::RunOutputCommitted { message, .. } => project(message, "message"),
            AgentSessionEvent::ToolExchangeCommitted {
                assistant, tool, ..
            } => {
                project(assistant, "assistant");
                project(tool, "tool");
            }
            AgentSessionEvent::CompactionCommitted { summary, .. }
            | AgentSessionEvent::ActiveRunCompactionCommitted { summary, .. } => {
                project(summary, "summary")
            }
            _ => {}
        }
        Ok(result)
    }

    pub(super) fn select(&self, pointer: &str) -> Result<&Value, String> {
        if self.hidden.iter().any(|path| {
            pointer == path
                || pointer
                    .strip_prefix(path)
                    .is_some_and(|tail| tail.starts_with('/'))
        }) {
            return Err(
                "json_pointer selects state unavailable in model-readable Session recall"
                    .to_owned(),
            );
        }
        self.value.pointer(pointer).ok_or_else(|| {
            "json_pointer does not identify a field in the readable record".to_owned()
        })
    }
}
