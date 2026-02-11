use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio::time::{timeout, Duration};
use tracing::{debug, warn};

use orchestral_channels::{ChannelEvent, CliRuntime};

use super::event_projection::{project_event, UiEvent};
use crate::runtime::protocol::{ActivityKind, RuntimeMsg, TransientSlot};

const TURN_SETTLE_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Clone)]
pub struct RuntimeClient {
    runtime: Arc<CliRuntime>,
    thread_id: String,
    submit_lock: Arc<Mutex<()>>,
}

impl RuntimeClient {
    pub async fn from_config(
        config: PathBuf,
        thread_id_override: Option<String>,
    ) -> anyhow::Result<Self> {
        let runtime = CliRuntime::from_config(config, thread_id_override)
            .await
            .context("failed to build cli runtime from config")?;
        Ok(Self {
            thread_id: runtime.thread_id().to_string(),
            runtime: Arc::new(runtime),
            submit_lock: Arc::new(Mutex::new(())),
        })
    }

    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub async fn submit_input(
        &self,
        input: String,
        runtime_tx: mpsc::Sender<RuntimeMsg>,
    ) -> anyhow::Result<()> {
        let _guard = self.submit_lock.lock().await;

        let mut rx = self
            .runtime
            .subscribe_events()
            .await
            .context("failed to subscribe events")?;
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let (expected_interaction_tx, expected_interaction_rx) =
            watch::channel::<Option<String>>(None);
        let (turn_settled_tx, turn_settled_rx) = oneshot::channel::<()>();
        let thread_id = self.thread_id.clone();
        let runtime_tx_events = runtime_tx.clone();

        let forward = tokio::spawn(async move {
            let mut turn_settled_tx = Some(turn_settled_tx);
            let mut completed_steps: usize = 0;
            let mut total_steps: usize = 0;
            let mut last_preview: Option<String> = None;
            let mut last_assistant_fingerprint: Option<String> = None;
            let mut last_streamed_assistant: Option<String> = None;
            let mut streamed_assistant: String = String::new();
            let mut stream_active = false;
            let mut last_approval_fingerprint: Option<String> = None;
            let mut assistant_rendered = false;
            let mut pending_execution_end = false;

            loop {
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                    msg = rx.recv() => {
                        let event = match msg {
                            Ok(event) => event,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                warn!(skipped, "cli runtime event stream lagged; ui may miss intermediate events");
                                let _ = runtime_tx_events
                                    .send(RuntimeMsg::OutputTransient {
                                        slot: TransientSlot::Status,
                                        text: format!("Syncing UI stream... skipped {} events", skipped),
                                    })
                                    .await;
                                continue;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        };
                        if event.thread_id() != thread_id {
                            continue;
                        }
                        if let Some(expected_interaction_id) = expected_interaction_rx.borrow().as_deref() {
                            if let Some(event_interaction_id) = event_interaction_id(&event) {
                                if event_interaction_id != expected_interaction_id {
                                    continue;
                                }
                            }
                        }
                        let Some(ui_event) = project_event(&event) else {
                            continue;
                        };

                        match ui_event {
                            UiEvent::PlanningStarted => {
                                let _ = runtime_tx_events.send(RuntimeMsg::PlanningStart).await;
                                let _ = runtime_tx_events
                                    .send(RuntimeMsg::OutputTransient {
                                        slot: TransientSlot::Footer,
                                        text: "Planning... Ctrl+C to interrupt".to_string(),
                                    })
                                    .await;
                            }
                            UiEvent::PlanningCompleted { step_count, steps } => {
                                total_steps = step_count.unwrap_or(steps.len());
                                let plan = if steps.is_empty() {
                                    "Plan: no actions".to_string()
                                } else {
                                    let actions = steps
                                        .iter()
                                        .take(4)
                                        .map(|s| format!("{}:{}", s.id, s.action))
                                        .collect::<Vec<_>>()
                                        .join(" -> ");
                                    format!("Plan: {}", actions)
                                };
                                let _ = runtime_tx_events.send(RuntimeMsg::OutputPersist(plan)).await;
                                let _ = runtime_tx_events
                                    .send(RuntimeMsg::OutputTransient {
                                        slot: TransientSlot::Inline,
                                        text: format!("{} step(s) queued", total_steps),
                                    })
                                    .await;
                                let _ = runtime_tx_events.send(RuntimeMsg::PlanningEnd).await;
                            }
                            UiEvent::ExecutionStarted => {
                                let _ = runtime_tx_events
                                    .send(RuntimeMsg::ExecutionStart { total: total_steps })
                                    .await;
                                let _ = runtime_tx_events
                                    .send(RuntimeMsg::OutputTransient {
                                        slot: TransientSlot::Footer,
                                        text: "Executing... Ctrl+C to interrupt".to_string(),
                                    })
                                    .await;
                            }
                            UiEvent::StepStarted {
                                step_id,
                                action,
                                input_summary,
                                ..
                            } => {
                                let action = action.unwrap_or_else(|| "-".to_string());
                                let kind = classify_activity_kind(&action);
                                let _ = runtime_tx_events
                                    .send(RuntimeMsg::ActivityStart {
                                        kind,
                                        step_id: step_id.clone(),
                                        action: action.clone(),
                                        input_summary: input_summary.clone(),
                                    })
                                    .await;
                                let input_summary = input_summary
                                    .map(|s| format!(" | {}", s))
                                    .unwrap_or_default();
                                let _ = runtime_tx_events
                                    .send(RuntimeMsg::OutputTransient {
                                        slot: TransientSlot::Status,
                                        text: format!("Running {} ({}){}", step_id, action, input_summary),
                                    })
                                    .await;
                            }
                            UiEvent::StepCompleted { step_id, action, output } => {
                                completed_steps = completed_steps.saturating_add(1);
                                let action_name = action.unwrap_or_else(|| "-".to_string());
                                if let Some(preview) = output.preview.clone() {
                                    last_preview = Some(preview);
                                }
                                let output_path = output.path.clone();
                                let mut parts = Vec::new();
                                if let Some(status) = output.status {
                                    parts.push(format!("status={}", status));
                                }
                                if let Some(bytes) = output.bytes {
                                    parts.push(format!("bytes={}", bytes));
                                }
                                let detail = if parts.is_empty() {
                                    String::new()
                                } else {
                                    format!(" {}", parts.join(" "))
                                };
                                let _ = runtime_tx_events
                                    .send(RuntimeMsg::ExecutionProgress { step: completed_steps })
                                    .await;
                                let _ = runtime_tx_events
                                    .send(RuntimeMsg::ActivityItem {
                                        step_id: step_id.clone(),
                                        action: action_name.clone(),
                                        line: if detail.is_empty() {
                                            "completed".to_string()
                                        } else {
                                            detail.trim().to_string()
                                        },
                                    })
                                    .await;
                                if let Some(path) = output_path {
                                    let _ = runtime_tx_events
                                        .send(RuntimeMsg::ActivityItem {
                                            step_id: step_id.clone(),
                                            action: action_name.clone(),
                                            line: format!("file: {}", path),
                                        })
                                        .await;
                                }
                                if let Some(preview) = output.preview {
                                    for line in preview_to_activity_lines(&preview) {
                                        let _ = runtime_tx_events
                                            .send(RuntimeMsg::ActivityItem {
                                                step_id: step_id.clone(),
                                                action: action_name.clone(),
                                                line,
                                            })
                                            .await;
                                    }
                                }
                                let _ = runtime_tx_events
                                    .send(RuntimeMsg::ActivityEnd {
                                        step_id,
                                        action: action_name,
                                        failed: false,
                                    })
                                    .await;
                            }
                            UiEvent::StepFailed { step_id, action, message } => {
                                let action_name = action.unwrap_or_else(|| "-".to_string());
                                let _ = runtime_tx_events
                                    .send(RuntimeMsg::ActivityItem {
                                        step_id: step_id.clone(),
                                        action: action_name.clone(),
                                        line: format!(
                                            "failed {}",
                                            message.unwrap_or_else(|| "unknown error".to_string())
                                        ),
                                    })
                                    .await;
                                let _ = runtime_tx_events
                                    .send(RuntimeMsg::ActivityEnd {
                                        step_id,
                                        action: action_name,
                                        failed: true,
                                    })
                                    .await;
                            }
                            UiEvent::InputRequired {
                                prompt,
                                waiting_kind,
                                approval_reason,
                                approval_command,
                            } => {
                                let is_approval = matches!(waiting_kind.as_deref(), Some("approval"))
                                    || approval_reason.is_some()
                                    || prompt
                                        .as_deref()
                                        .map(|p| p.to_ascii_lowercase().contains("requires approval"))
                                        .unwrap_or(false);
                                let waiting_line = if is_approval {
                                    let reason = approval_reason
                                        .or(prompt)
                                        .unwrap_or_else(|| "approval required".to_string());
                                    let fingerprint = format!(
                                        "{}|{}",
                                        reason,
                                        approval_command.clone().unwrap_or_default()
                                    );
                                    if last_approval_fingerprint.as_deref() != Some(&fingerprint) {
                                        last_approval_fingerprint = Some(fingerprint);
                                        let _ = runtime_tx_events
                                            .send(RuntimeMsg::ApprovalRequested {
                                                reason,
                                                command: approval_command,
                                            })
                                            .await;
                                    }
                                    String::new()
                                } else {
                                    format!(
                                        "Waiting: {}",
                                        prompt.unwrap_or_else(|| "input required".to_string())
                                    )
                                };
                                if !waiting_line.is_empty() {
                                    let _ = runtime_tx_events
                                        .send(RuntimeMsg::OutputPersist(waiting_line))
                                        .await;
                                }
                                pending_execution_end = false;
                                let _ = runtime_tx_events.send(RuntimeMsg::ExecutionEnd).await;
                                mark_turn_settled(&mut turn_settled_tx);
                            }
                            UiEvent::ExecutionCompleted { status } => {
                                debug!(
                                    status = ?status,
                                    pending_execution_end = pending_execution_end,
                                    assistant_rendered = assistant_rendered,
                                    "tui event: execution_completed"
                                );
                                match status.as_deref() {
                                    Some("completed") => {
                                        // For completed turns, assistant output is emitted after
                                        // execution_completed. Delay ExecutionEnd until reply arrives.
                                        pending_execution_end = true;
                                    }
                                    Some("clarification") => {
                                        // Clarification also emits assistant output after execution_completed.
                                        pending_execution_end = true;
                                    }
                                    Some(other) => {
                                        let _ = runtime_tx_events
                                            .send(RuntimeMsg::OutputPersist(format!("Status: {}", other)))
                                            .await;
                                        pending_execution_end = false;
                                        let _ = runtime_tx_events.send(RuntimeMsg::ExecutionEnd).await;
                                        mark_turn_settled(&mut turn_settled_tx);
                                    }
                                    None => {
                                        pending_execution_end = true;
                                    }
                                }
                            }
                            UiEvent::TaskFailed { message } => {
                                let _ = runtime_tx_events
                                    .send(RuntimeMsg::Error(
                                        message.unwrap_or_else(|| "task failed".to_string()),
                                    ))
                                    .await;
                            }
                            UiEvent::TaskCompleted
                            | UiEvent::TurnStarted
                            | UiEvent::TurnQueued
                            | UiEvent::TurnResumed => {}
                            UiEvent::ReplanningStarted { message } => {
                                let line = message.unwrap_or_else(|| {
                                    "Execution failed, retrying with recovery plan...".to_string()
                                });
                                let _ = runtime_tx_events.send(RuntimeMsg::OutputPersist(line)).await;
                            }
                            UiEvent::ReplanningCompleted { message } => {
                                let line = message
                                    .unwrap_or_else(|| "Recovery plan ready, resuming execution.".to_string());
                                let _ = runtime_tx_events.send(RuntimeMsg::OutputPersist(line)).await;
                            }
                            UiEvent::ReplanningFailed { message, error } => {
                                let mut line = format!(
                                    "Recovery planning failed: {}",
                                    message.unwrap_or_else(|| "unknown error".to_string())
                                );
                                if let Some(error) = error {
                                    if !error.trim().is_empty() {
                                        line.push_str(" | ");
                                        line.push_str(error.trim());
                                    }
                                }
                                let _ = runtime_tx_events.send(RuntimeMsg::OutputPersist(line)).await;
                            }
                            UiEvent::AssistantOutput { message } => {
                                let normalized = message.trim().to_string();
                                if normalized.is_empty() {
                                    continue;
                                }
                                debug!(
                                    len = normalized.len(),
                                    pending_execution_end = pending_execution_end,
                                    stream_active = stream_active,
                                    "tui event: assistant_output"
                                );
                                let streamed_trimmed = streamed_assistant.trim().to_string();
                                if (!streamed_trimmed.is_empty() && streamed_trimmed == normalized)
                                    || last_streamed_assistant.as_deref() == Some(normalized.as_str())
                                {
                                    // Stream already rendered this answer live.
                                    stream_active = false;
                                    streamed_assistant.clear();
                                    assistant_rendered = true;
                                    debug!("tui action: assistant_output deduped against streamed text");
                                    if pending_execution_end {
                                        debug!("tui action: execution_end after assistant_output (dedup)");
                                        pending_execution_end = false;
                                        let _ = runtime_tx_events.send(RuntimeMsg::ExecutionEnd).await;
                                        mark_turn_settled(&mut turn_settled_tx);
                                        assistant_rendered = false;
                                        last_preview = None;
                                        last_assistant_fingerprint = None;
                                        last_streamed_assistant = None;
                                    }
                                    mark_turn_settled(&mut turn_settled_tx);
                                    continue;
                                }
                                // If a stream was open but no deduplicated final arrived, prefer the
                                // explicit assistant_output payload to avoid losing visible replies.
                                if stream_active {
                                    stream_active = false;
                                    streamed_assistant.clear();
                                }
                                if last_assistant_fingerprint.as_deref() != Some(normalized.as_str())
                                {
                                    last_assistant_fingerprint = Some(normalized.clone());
                                    assistant_rendered = true;
                                    let _ = runtime_tx_events
                                        .send(RuntimeMsg::OutputPersist(normalized))
                                        .await;
                                }
                                mark_turn_settled(&mut turn_settled_tx);
                                if pending_execution_end {
                                    debug!("tui action: execution_end after assistant_output");
                                    pending_execution_end = false;
                                    let _ = runtime_tx_events.send(RuntimeMsg::ExecutionEnd).await;
                                    mark_turn_settled(&mut turn_settled_tx);
                                    assistant_rendered = false;
                                    last_preview = None;
                                    last_assistant_fingerprint = None;
                                    last_streamed_assistant = None;
                                    streamed_assistant.clear();
                                    stream_active = false;
                                }
                            }
                            UiEvent::AssistantStreamDelta { delta, done } => {
                                debug!(
                                    delta_len = delta.len(),
                                    done = done,
                                    pending_execution_end = pending_execution_end,
                                    "tui event: assistant_stream_delta"
                                );
                                if !delta.is_empty() {
                                    stream_active = true;
                                    streamed_assistant.push_str(&delta);
                                    assistant_rendered = true;
                                    let _ = runtime_tx_events
                                        .send(RuntimeMsg::AssistantDelta {
                                            chunk: delta,
                                            done: false,
                                        })
                                        .await;
                                }
                                if done {
                                    stream_active = false;
                                    let streamed_final = streamed_assistant.trim().to_string();
                                    if !streamed_final.is_empty() {
                                        let _ = runtime_tx_events
                                            .send(RuntimeMsg::OutputPersist(streamed_final.clone()))
                                            .await;
                                        last_streamed_assistant = Some(streamed_final);
                                    }
                                    let _ = runtime_tx_events
                                        .send(RuntimeMsg::AssistantDelta {
                                            chunk: String::new(),
                                            done: true,
                                        })
                                        .await;
                                    mark_turn_settled(&mut turn_settled_tx);
                                    if pending_execution_end && !streamed_assistant.trim().is_empty() {
                                        debug!("tui action: execution_end after assistant_stream done");
                                        pending_execution_end = false;
                                        let _ = runtime_tx_events.send(RuntimeMsg::ExecutionEnd).await;
                                        mark_turn_settled(&mut turn_settled_tx);
                                        assistant_rendered = false;
                                        last_preview = None;
                                        last_assistant_fingerprint = None;
                                        last_streamed_assistant = None;
                                    }
                                    streamed_assistant.clear();
                                }
                            }
                            UiEvent::TurnRejected { reason } => {
                                let _ = runtime_tx_events
                                    .send(RuntimeMsg::OutputPersist(format!(
                                        "Waiting: {}",
                                        reason.unwrap_or_else(|| "turn rejected".to_string())
                                    )))
                                    .await;
                                pending_execution_end = false;
                                let _ = runtime_tx_events.send(RuntimeMsg::ExecutionEnd).await;
                                mark_turn_settled(&mut turn_settled_tx);
                            }
                        }
                    }
                }
            }

            if pending_execution_end {
                debug!(
                    assistant_rendered = assistant_rendered,
                    "tui action: execution_end at forwarder shutdown"
                );
                if !assistant_rendered {
                    if let Some(preview) = last_preview {
                        let _ = runtime_tx_events
                            .send(RuntimeMsg::OutputPersist(preview))
                            .await;
                    }
                }
                let _ = runtime_tx_events.send(RuntimeMsg::ExecutionEnd).await;
                mark_turn_settled(&mut turn_settled_tx);
            }
        });

        let submit_result = self.runtime.submit_input(input).await;

        match submit_result {
            Ok(response) => {
                if response.interaction_id.is_none() {
                    if let Some(message) = response.message {
                        let _ = runtime_tx
                            .send(RuntimeMsg::OutputPersist(format!("Waiting: {}", message)))
                            .await;
                    }
                    let _ = runtime_tx.send(RuntimeMsg::ExecutionEnd).await;
                } else {
                    let _ = expected_interaction_tx.send(response.interaction_id.clone());
                    match timeout(TURN_SETTLE_TIMEOUT, turn_settled_rx).await {
                        Ok(Ok(())) | Ok(Err(_)) => {
                            // Forwarder observed terminal turn event.
                        }
                        Err(_) => {
                            warn!("timeout waiting for terminal turn event; forcing execution end");
                            let _ = runtime_tx.send(RuntimeMsg::ExecutionEnd).await;
                        }
                    }
                }
            }
            Err(err) => {
                let _ = stop_tx.send(true);
                let _ = forward.await;
                let _ = runtime_tx
                    .send(RuntimeMsg::Error(format!("runtime error: {}", err)))
                    .await;
                return Err(anyhow::anyhow!(err));
            }
        }

        let _ = stop_tx.send(true);
        let _ = forward.await;

        Ok(())
    }

    pub async fn interrupt(&self, runtime_tx: mpsc::Sender<RuntimeMsg>) {
        match self.runtime.interrupt().await {
            Ok(()) => {}
            Err(err) => {
                let _ = runtime_tx
                    .send(RuntimeMsg::Error(format!("interrupt failed: {}", err)))
                    .await;
                return;
            }
        }
        let _ = runtime_tx
            .send(RuntimeMsg::OutputTransient {
                slot: TransientSlot::Status,
                text: "Interrupt requested".to_string(),
            })
            .await;
    }
}

fn mark_turn_settled(turn_settled_tx: &mut Option<oneshot::Sender<()>>) {
    if let Some(tx) = turn_settled_tx.take() {
        let _ = tx.send(());
    }
}

fn event_interaction_id(event: &ChannelEvent) -> Option<&str> {
    match event {
        ChannelEvent::UserInput { interaction_id, .. }
        | ChannelEvent::AssistantOutput { interaction_id, .. }
        | ChannelEvent::Artifact { interaction_id, .. } => Some(interaction_id.as_str()),
        ChannelEvent::SystemTrace {
            interaction_id,
            payload,
            ..
        } => interaction_id
            .as_ref()
            .map(|id| id.as_str())
            .or_else(|| payload.get("interaction_id").and_then(|v| v.as_str())),
        ChannelEvent::ExternalEvent { interaction_id, .. } => {
            interaction_id.as_ref().map(|id| id.as_str())
        }
    }
}

fn classify_activity_kind(action: &str) -> ActivityKind {
    match action {
        "file_write" | "edit" | "patch" | "write" => ActivityKind::Edited,
        "http" | "search" | "read_file" | "list_files" | "find" | "grep" => ActivityKind::Explored,
        _ => ActivityKind::Ran,
    }
}

fn preview_to_activity_lines(preview: &str) -> Vec<String> {
    let mut out = Vec::new();
    let lines: Vec<&str> = preview.lines().collect();
    let max_lines = 8usize;
    if lines.is_empty() {
        out.push("(empty)".to_string());
        return out;
    }

    for line in lines.iter().take(max_lines) {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut snippet: String = trimmed.chars().take(180).collect();
        if trimmed.chars().count() > 180 {
            snippet.push_str("...");
        }
        out.push(snippet);
    }
    if lines.len() > max_lines {
        out.push(format!("... +{} lines", lines.len() - max_lines));
    }
    if out.is_empty() {
        out.push("(empty)".to_string());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_event_interaction_id_from_assistant_output() {
        let event = ChannelEvent::assistant_output("thread-1", "int-1", json!({"message":"ok"}));
        assert_eq!(event_interaction_id(&event), Some("int-1"));
    }

    #[test]
    fn test_event_interaction_id_from_system_trace_payload() {
        let event = ChannelEvent::trace("thread-1", "info", json!({"interaction_id":"int-2"}));
        assert_eq!(event_interaction_id(&event), Some("int-2"));
    }
}
