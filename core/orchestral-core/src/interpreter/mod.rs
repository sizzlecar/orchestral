//! Result interpreter abstractions.
//!
//! This module converts low-level execution outcomes into user-facing summaries.

use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::executor::ExecutionResult;
use crate::types::{Plan, StepId};

/// Input payload for result interpretation.
#[derive(Debug, Clone)]
pub struct InterpretRequest {
    /// Original user intent content.
    pub intent: String,
    /// Plan used for this execution turn.
    pub plan: Plan,
    /// Current execution result.
    pub execution_result: ExecutionResult,
    /// Completed step ids in this task checkpoint.
    pub completed_step_ids: Vec<StepId>,
    /// Task working set snapshot at the end of execution.
    pub working_set_snapshot: HashMap<String, Value>,
}

/// Output payload from interpreter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterpretResult {
    /// User-facing assistant message.
    pub message: String,
    /// Optional structured payload for downstream UI/renderers.
    #[serde(default)]
    pub metadata: Value,
}

impl InterpretResult {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            metadata: Value::Null,
        }
    }

    pub fn with_metadata(mut self, metadata: Value) -> Self {
        self.metadata = metadata;
        self
    }
}

/// Result interpreter errors.
#[derive(Debug, Error)]
pub enum InterpretError {
    #[error("failed to interpret result: {0}")]
    Failed(String),
}

/// Interpreter trait.
#[async_trait]
pub trait ResultInterpreter: Send + Sync {
    async fn interpret(&self, request: InterpretRequest)
        -> Result<InterpretResult, InterpretError>;

    async fn interpret_stream(
        &self,
        request: InterpretRequest,
        sink: Option<std::sync::Arc<dyn InterpretDeltaSink>>,
    ) -> Result<InterpretResult, InterpretError> {
        let output = self.interpret(request).await?;
        if let Some(sink) = sink {
            sink.on_delta(&output.message).await;
            sink.on_done().await;
        }
        Ok(output)
    }
}

#[async_trait]
pub trait InterpretDeltaSink: Send + Sync {
    async fn on_delta(&self, delta: &str);
    async fn on_done(&self) {}
}

/// Deterministic fallback interpreter.
pub struct NoopResultInterpreter;

#[async_trait]
impl ResultInterpreter for NoopResultInterpreter {
    async fn interpret(
        &self,
        request: InterpretRequest,
    ) -> Result<InterpretResult, InterpretError> {
        let completed = request.completed_step_ids.len();
        let total = request.plan.steps.len();
        let message = match &request.execution_result {
            ExecutionResult::Completed => {
                let preview = working_set_preview(&request.working_set_snapshot)
                    .unwrap_or_else(|| "Task completed.".to_string());
                format!("Done ({} / {} steps). {}", completed, total, preview)
            }
            ExecutionResult::Failed { step_id, error } => format!(
                "Execution failed at step '{}' after {} / {} steps: {}",
                step_id, completed, total, error
            ),
            ExecutionResult::WaitingUser { prompt, .. } => format!(
                "Need your input to continue ({} / {} steps done): {}",
                completed, total, prompt
            ),
            ExecutionResult::WaitingEvent {
                step_id,
                event_type,
            } => format!(
                "Waiting for event '{}' at step '{}' ({} / {} steps done).",
                event_type, step_id, completed, total
            ),
        };
        Ok(
            InterpretResult::new(message).with_metadata(serde_json::json!({
                "completed_steps": completed,
                "total_steps": total,
            })),
        )
    }
}

fn working_set_preview(snapshot: &HashMap<String, Value>) -> Option<String> {
    for key in ["result", "content", "stdout", "body"] {
        if let Some(text) = snapshot.get(key).and_then(|v| v.as_str()) {
            let text = summarize_text_output(text);
            if !text.is_empty() {
                return Some(text);
            }
        }
    }
    None
}

fn summarize_text_output(text: &str) -> String {
    let lines = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    if lines.len() >= 4 {
        let preview = lines.iter().take(8).copied().collect::<Vec<_>>().join(", ");
        let remain = lines.len().saturating_sub(8);
        if remain > 0 {
            return format!("{} (+{} more)", preview, remain);
        }
        return preview;
    }

    let count = text.chars().count();
    if count <= 240 {
        return text.to_string();
    }
    let mut out: String = text.chars().take(240).collect();
    out.push_str("...");
    out
}
