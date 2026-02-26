use async_trait::async_trait;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::planner::{LlmClient, LlmRequest, StreamChunkCallback};
use crate::system_prompts::default_interpreter_prompt;
use orchestral_core::executor::ExecutionResult;
use orchestral_core::interpreter::{
    InterpretDeltaSink, InterpretError, InterpretRequest, InterpretResult, ResultInterpreter,
};

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
            system_prompt: default_interpreter_prompt().to_string(),
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
    if let Some(guarded) = skill_only_completion_message(request) {
        return guarded;
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

fn skill_only_completion_message(request: &InterpretRequest) -> Option<String> {
    if request.plan.steps.is_empty() {
        return None;
    }
    if !request
        .plan
        .steps
        .iter()
        .all(|step| step.action.starts_with("skill__"))
    {
        return None;
    }
    if has_explicit_write_evidence(&request.working_set_snapshot) {
        return None;
    }

    let skill_name = request
        .working_set_snapshot
        .get("skill")
        .and_then(|v| v.as_str())
        .or_else(|| request.plan.steps.first().map(|step| step.action.as_str()))
        .unwrap_or("skill");

    if contains_cjk(&request.intent) {
        Some(format!(
            "已完成技能 `{}` 的说明加载，但这一步仅返回操作指南，不会直接修改或生成文件。当前没有检测到文件写入结果。若要我实际改表，我将继续执行具体文件操作并在完成后返回写入路径与校验结果。",
            skill_name
        ))
    } else {
        Some(format!(
            "Loaded skill instructions for `{}`, but this step is instruction-only and does not directly create or modify files. No file-write evidence was detected. If you want real edits, I should execute concrete file operations next and report verified output paths.",
            skill_name
        ))
    }
}

fn has_explicit_write_evidence(snapshot: &std::collections::HashMap<String, Value>) -> bool {
    for key in ["wrote_file", "saved", "written"] {
        if snapshot.get(key).and_then(|v| v.as_bool()) == Some(true) {
            return true;
        }
    }
    false
}

fn contains_cjk(text: &str) -> bool {
    text.chars().any(|ch| {
        ('\u{4E00}'..='\u{9FFF}').contains(&ch)
            || ('\u{3400}'..='\u{4DBF}').contains(&ch)
            || ('\u{3040}'..='\u{30FF}').contains(&ch)
            || ('\u{AC00}'..='\u{D7AF}').contains(&ch)
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

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::types::{Plan, Step};
    use std::collections::HashMap;

    #[test]
    fn postprocess_message_guards_skill_only_completion_in_chinese() {
        let request = InterpretRequest {
            intent: "假设一个员工 把需要填的地方按要求填上".to_string(),
            plan: Plan::new("skill only", vec![Step::action("s1", "skill__xlsx")]),
            execution_result: ExecutionResult::Completed,
            completed_step_ids: vec!["s1".into()],
            working_set_snapshot: HashMap::from([
                ("skill".to_string(), Value::String("xlsx".to_string())),
                (
                    "instructions".to_string(),
                    Value::String("long skill instructions".to_string()),
                ),
            ]),
        };

        let message = postprocess_message(&request, "我已保存文件 docs/xxx.xlsx");
        assert!(message.contains("仅返回操作指南"));
        assert!(message.contains("没有检测到文件写入结果"));
    }

    #[test]
    fn postprocess_message_keeps_non_skill_completion_message() {
        let request = InterpretRequest {
            intent: "list docs".to_string(),
            plan: Plan::new("shell list", vec![Step::action("s1", "shell")]),
            execution_result: ExecutionResult::Completed,
            completed_step_ids: vec!["s1".into()],
            working_set_snapshot: HashMap::new(),
        };
        let original = "done";
        let message = postprocess_message(&request, original);
        assert_eq!(message, original);
    }
}
