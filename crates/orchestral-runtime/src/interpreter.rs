use async_trait::async_trait;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orchestral_core::executor::ExecutionResult;
use orchestral_core::interpreter::{
    InterpretDeltaSink, InterpretError, InterpretRequest, InterpretResult, ResultInterpreter,
};
use orchestral_planners::{LlmClient, LlmRequest, StreamChunkCallback};

const DEFAULT_INTERPRETER_PROMPT: &str = "You are the response synthesizer for Orchestral.
Produce a concise, user-facing answer from execution state.
Rules:
- Return plain text only (no JSON, no markdown code fences).
- If completed: summarize key outcome and artifacts.
- If waiting_user: ask exactly what user should provide next.
- If waiting_event: explain what event is awaited.
- If failed: explain likely cause and one concrete next step.
- Do not mention internal implementation details unless they help recovery.
- Do not dump raw command output verbatim when it is long; summarize and highlight key points.
- Reply in the same language as the user's intent.";

#[derive(Debug, Clone)]
pub struct LlmResultInterpreterConfig {
    pub model: String,
    pub temperature: f32,
    pub system_prompt: String,
    pub timeout_secs: u64,
}

impl Default for LlmResultInterpreterConfig {
    fn default() -> Self {
        Self {
            model: "gpt-4o-mini".to_string(),
            temperature: 0.2,
            system_prompt: DEFAULT_INTERPRETER_PROMPT.to_string(),
            timeout_secs: 12,
        }
    }
}

pub struct LlmResultInterpreter<C: LlmClient> {
    client: C,
    config: LlmResultInterpreterConfig,
}

impl<C: LlmClient> LlmResultInterpreter<C> {
    pub fn new(client: C, config: LlmResultInterpreterConfig) -> Self {
        Self { client, config }
    }
}

#[async_trait]
impl<C: LlmClient> ResultInterpreter for LlmResultInterpreter<C> {
    async fn interpret(
        &self,
        request: InterpretRequest,
    ) -> Result<InterpretResult, InterpretError> {
        let user = build_user_prompt(&request);
        let response = tokio::time::timeout(
            Duration::from_secs(self.config.timeout_secs),
            self.client.complete(LlmRequest {
                system: self.config.system_prompt.clone(),
                user,
                model: self.config.model.clone(),
                temperature: self.config.temperature,
            }),
        )
        .await
        .map_err(|_| {
            InterpretError::Failed(format!(
                "interpreter timeout after {}s",
                self.config.timeout_secs
            ))
        })?
        .map_err(|e| InterpretError::Failed(e.to_string()))?;

        let normalized = normalize_interpreter_message(response.trim());
        let message = postprocess_message(&request, normalized.trim());
        let message = message.trim();
        if message.is_empty() {
            return Err(InterpretError::Failed(
                "llm interpreter returned empty output".to_string(),
            ));
        }

        Ok(
            InterpretResult::new(message).with_metadata(serde_json::json!({
                "source": "llm",
                "model": self.config.model,
            })),
        )
    }

    async fn interpret_stream(
        &self,
        request: InterpretRequest,
        sink: Option<Arc<dyn InterpretDeltaSink>>,
    ) -> Result<InterpretResult, InterpretError> {
        let user = build_user_prompt(&request);
        let chunk_collector = Arc::new(Mutex::new(String::new()));
        let sink_clone = sink.clone();
        let collector_clone = chunk_collector.clone();
        let on_chunk: StreamChunkCallback = Arc::new(move |chunk: String| {
            if let Ok(mut guard) = collector_clone.lock() {
                guard.push_str(&chunk);
            }
            if let Some(sink) = sink_clone.as_ref().cloned() {
                let chunk_owned = chunk.clone();
                tokio::spawn(async move {
                    sink.on_delta(&chunk_owned).await;
                });
            }
        });

        let raw = tokio::time::timeout(
            Duration::from_secs(self.config.timeout_secs),
            self.client.complete_stream(
                LlmRequest {
                    system: self.config.system_prompt.clone(),
                    user,
                    model: self.config.model.clone(),
                    temperature: self.config.temperature,
                },
                on_chunk,
            ),
        )
        .await
        .map_err(|_| {
            InterpretError::Failed(format!(
                "interpreter timeout after {}s",
                self.config.timeout_secs
            ))
        })?
        .map_err(|e| InterpretError::Failed(e.to_string()))?;

        let fallback_collected = chunk_collector
            .lock()
            .ok()
            .map(|s| s.clone())
            .unwrap_or_default();
        let raw = if raw.trim().is_empty() {
            fallback_collected
        } else {
            raw
        };
        let normalized = normalize_interpreter_message(raw.trim());
        let message = postprocess_message(&request, normalized.trim());
        let message = message.trim();
        if message.is_empty() {
            return Err(InterpretError::Failed(
                "llm interpreter returned empty output".to_string(),
            ));
        }
        if let Some(sink) = sink {
            sink.on_done().await;
        }
        Ok(
            InterpretResult::new(message).with_metadata(serde_json::json!({
                "source": "llm",
                "model": self.config.model,
            })),
        )
    }
}

fn normalize_interpreter_message(raw: &str) -> String {
    if let Ok(value) = serde_json::from_str::<Value>(raw) {
        return extract_message_from_json(&value).unwrap_or_else(|| raw.to_string());
    }
    if let Some(candidate) = extract_json_fence(raw) {
        if let Ok(value) = serde_json::from_str::<Value>(&candidate) {
            return extract_message_from_json(&value).unwrap_or_else(|| raw.to_string());
        }
    }
    raw.to_string()
}

fn extract_message_from_json(value: &Value) -> Option<String> {
    if let Some(message) = value
        .get("message")
        .and_then(|v| v.as_str())
        .map(ToString::to_string)
    {
        return Some(message);
    }
    if let Some(steps) = value.get("steps").and_then(|v| v.as_array()) {
        for step in steps {
            if step
                .get("action")
                .and_then(|v| v.as_str())
                .map(|v| v == "final_answer")
                .unwrap_or(false)
            {
                if let Some(message) = step
                    .get("params")
                    .and_then(|p| p.get("message"))
                    .and_then(|v| v.as_str())
                {
                    return Some(message.to_string());
                }
            }
        }
    }
    value.as_str().map(ToString::to_string)
}

fn extract_json_fence(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    let start = trimmed.find("```")?;
    let after_start = &trimmed[start + 3..];
    let after_lang = if let Some(pos) = after_start.find('\n') {
        &after_start[pos + 1..]
    } else {
        return None;
    };
    let end = after_lang.rfind("```")?;
    let candidate = after_lang[..end].trim();
    if candidate.starts_with('{') || candidate.starts_with('[') {
        Some(candidate.to_string())
    } else {
        None
    }
}

fn postprocess_message(request: &InterpretRequest, message: &str) -> String {
    if !matches!(request.execution_result, ExecutionResult::Completed) {
        return message.to_string();
    }
    if !looks_like_listing_output(message) {
        return message.to_string();
    }
    let Some(command) = first_shell_command(&request.plan) else {
        return message.to_string();
    };
    if !command.trim_start().starts_with("ls") {
        return message.to_string();
    }
    let entries: Vec<&str> = message
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    if entries.len() < 3 {
        return message.to_string();
    }
    let preview = entries
        .iter()
        .take(10)
        .copied()
        .collect::<Vec<_>>()
        .join(", ");
    let remain = entries.len().saturating_sub(10);
    if remain > 0 {
        format!(
            "Directory entries ({total}): {preview}, +{remain} more.",
            total = entries.len(),
            preview = preview,
            remain = remain
        )
    } else {
        format!(
            "Directory entries ({total}): {preview}.",
            total = entries.len(),
            preview = preview
        )
    }
}

fn first_shell_command(plan: &orchestral_core::types::Plan) -> Option<&str> {
    plan.steps
        .iter()
        .find(|s| s.action == "shell")
        .and_then(|s| s.params.get("command"))
        .and_then(|v| v.as_str())
}

fn looks_like_listing_output(message: &str) -> bool {
    let lines: Vec<&str> = message
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    if lines.len() < 3 {
        return false;
    }
    lines.iter().all(|line| {
        !line.contains(' ')
            && !line.contains(':')
            && !line.starts_with('{')
            && !line.starts_with('[')
            && line.chars().all(|ch| ch != '\t')
    })
}

fn build_user_prompt(request: &InterpretRequest) -> String {
    let status = execution_status(&request.execution_result);
    let steps = request
        .plan
        .steps
        .iter()
        .take(20)
        .map(|s| format!("{}:{} deps={:?}", s.id, s.action, s.depends_on))
        .collect::<Vec<_>>()
        .join("\n");
    let completed = serde_json::to_string(&request.completed_step_ids).unwrap_or_default();
    let ws_preview = working_set_preview(&request.working_set_snapshot);

    format!(
        "Intent:\n{}\n\nStatus:\n{}\n\nPlan Steps:\n{}\n\nCompleted Step IDs:\n{}\n\nWorking Set Preview:\n{}\n\nWrite final assistant reply now.",
        request.intent, status, steps, completed, ws_preview
    )
}

fn execution_status(result: &ExecutionResult) -> String {
    match result {
        ExecutionResult::Completed => "completed".to_string(),
        ExecutionResult::Failed { step_id, error } => {
            format!("failed at {}: {}", step_id, truncate(error, 600))
        }
        ExecutionResult::WaitingUser {
            step_id,
            prompt,
            approval,
        } => {
            let wait_kind = if approval.is_some() {
                "waiting_user(approval)"
            } else {
                "waiting_user(input)"
            };
            format!("{} at {}: {}", wait_kind, step_id, truncate(prompt, 600))
        }
        ExecutionResult::WaitingEvent {
            step_id,
            event_type,
        } => format!("waiting_event at {} for {}", step_id, event_type),
    }
}

fn working_set_preview(snapshot: &std::collections::HashMap<String, Value>) -> String {
    let mut items = snapshot
        .iter()
        .filter_map(|(k, v)| {
            if let Some(text) = v.as_str() {
                if text.trim().is_empty() {
                    return None;
                }
                return Some(format!("{}={}", k, truncate(text, 240)));
            }
            if v.is_number() || v.is_boolean() {
                return Some(format!("{}={}", k, v));
            }
            None
        })
        .take(12)
        .collect::<Vec<_>>();
    items.sort();
    if items.is_empty() {
        "(no simple scalar outputs)".to_string()
    } else {
        items.join("\n")
    }
}

fn truncate(input: &str, max_chars: usize) -> String {
    let count = input.chars().count();
    if count <= max_chars {
        return input.to_string();
    }
    let mut out: String = input.chars().take(max_chars).collect();
    out.push_str("...");
    out
}
