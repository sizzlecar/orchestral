use std::collections::HashMap;

use anyhow::Context;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::timeout;
use tracing::{debug, warn};

use orchestral_runtime::api::SubmitStatus;

use crate::runtime::event_projection::{project_event, AgentProgressKind, UiEvent};
use crate::runtime::protocol::{RuntimeMsg, TransientSlot};

use super::event_support::{
    channel_event_label, classify_activity_kind, event_interaction_id, format_running_label,
    log_preview, mark_turn_settled, normalize_step_action_label, preview_to_activity_lines,
    ui_event_label,
};
use super::{
    RuntimeClient, FORWARD_DRAIN_IDLE_TIMEOUT, TURN_SETTLE_GRACE_TIMEOUT, TURN_SETTLE_TIMEOUT,
};

impl RuntimeClient {
    pub async fn submit_input(
        &self,
        input: String,
        runtime_tx: mpsc::Sender<RuntimeMsg>,
    ) -> anyhow::Result<()> {
        let input_len = input.len();
        let input_preview = log_preview(&input, 80);
        debug!(
            thread_id = %self.thread_id,
            input_len,
            input_preview = %input_preview,
            "submit_chain: runtime_client submit_input start"
        );
        let _guard = self.submit_lock.lock().await;
        debug!("submit_chain: runtime_client submit lock acquired");

        let mut rx = self
            .runtime
            .subscribe_events()
            .await
            .context("failed to subscribe events")?;
        debug!("submit_chain: runtime_client subscribed event stream");
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let (expected_interaction_tx, expected_interaction_rx) =
            watch::channel::<Option<String>>(None);
        let (turn_settled_tx, turn_settled_rx) = oneshot::channel::<()>();
        let thread_id = self.thread_id.clone();
        let runtime_tx_events = runtime_tx.clone();

        let forward = tokio::spawn(async move {
            debug!(thread_id = %thread_id, "submit_chain: forward task started");
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
            let mut assistant_output_received = false;
            let mut pending_execution_end = false;
            let mut stop_requested = false;
            let mut agent_action_counts: HashMap<String, usize> = HashMap::new();
            let mut active_agent_groups: HashMap<String, String> = HashMap::new();

            loop {
                let event = if stop_requested {
                    match timeout(FORWARD_DRAIN_IDLE_TIMEOUT, rx.recv()).await {
                        Ok(Ok(event)) => event,
                        Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped))) => {
                            warn!(skipped, "cli runtime event stream lagged while draining; ui may miss intermediate events");
                            let _ = runtime_tx_events
                                .send(RuntimeMsg::OutputTransient {
                                    slot: TransientSlot::Status,
                                    text: format!(
                                        "Syncing UI stream... skipped {} events",
                                        skipped
                                    ),
                                })
                                .await;
                            continue;
                        }
                        Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => break,
                        Err(_) => {
                            debug!(
                                idle_ms = FORWARD_DRAIN_IDLE_TIMEOUT.as_millis() as u64,
                                "forward: drain idle timeout reached"
                            );
                            break;
                        }
                    }
                } else {
                    tokio::select! {
                        changed = stop_rx.changed() => {
                            if changed.is_err() || *stop_rx.borrow() {
                                debug!("forward: stop signal received; draining pending events");
                                stop_requested = true;
                            }
                            continue;
                        }
                        msg = rx.recv() => {
                            match msg {
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
                            }
                        }
                    }
                };
                let event_interaction_id_for_log =
                    event_interaction_id(&event).map(ToString::to_string);
                debug!(
                    event_type = %channel_event_label(&event),
                    event_thread_id = %event.thread_id(),
                    event_interaction_id = ?event_interaction_id_for_log,
                    expected_interaction_id = ?expected_interaction_rx.borrow().clone(),
                    stop_requested = stop_requested,
                    "submit_chain: forward received channel event"
                );
                if event.thread_id() != thread_id {
                    debug!(
                        event_thread_id = %event.thread_id(),
                        expected_thread_id = %thread_id,
                        "submit_chain: forward skip event (thread mismatch)"
                    );
                    continue;
                }
                if let Some(expected_interaction_id) = expected_interaction_rx.borrow().as_deref() {
                    if let Some(event_interaction_id) = event_interaction_id(&event) {
                        if event_interaction_id != expected_interaction_id {
                            debug!(
                                expected_interaction_id = %expected_interaction_id,
                                event_interaction_id = %event_interaction_id,
                                "submit_chain: forward skip event (interaction mismatch)"
                            );
                            continue;
                        }
                    }
                }
                let Some(ui_event) = project_event(&event) else {
                    debug!(
                        event_type = %channel_event_label(&event),
                        "submit_chain: forward dropped non-ui event"
                    );
                    continue;
                };
                debug!(
                    ui_event = %ui_event_label(&ui_event),
                    "submit_chain: forward projected ui event"
                );

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
                    UiEvent::PlanningCompleted {
                        step_count,
                        steps,
                        output_type,
                    } => {
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
                        let _ = runtime_tx_events
                            .send(RuntimeMsg::OutputPersist(plan))
                            .await;
                        if let Some(output_type) = output_type {
                            let _ = runtime_tx_events
                                .send(RuntimeMsg::OutputPersist(format!(
                                    "PlanningOutput: {}",
                                    output_type
                                )))
                                .await;
                        }
                        let _ = runtime_tx_events
                            .send(RuntimeMsg::OutputTransient {
                                slot: TransientSlot::Inline,
                                text: format!("{} step(s) queued", total_steps),
                            })
                            .await;
                        let _ = runtime_tx_events.send(RuntimeMsg::PlanningEnd).await;
                    }
                    UiEvent::ExecutionStarted { execution_mode } => {
                        let _ = runtime_tx_events
                            .send(RuntimeMsg::ExecutionStart {
                                total: total_steps,
                                execution_mode,
                            })
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
                        let action_label = normalize_step_action_label(&step_id, action);
                        let kind = classify_activity_kind(&action_label);
                        let _ = runtime_tx_events
                            .send(RuntimeMsg::ActivityStart {
                                kind,
                                step_id: step_id.clone(),
                                action: action_label.clone(),
                                input_summary: input_summary.clone(),
                            })
                            .await;
                        let input_summary = input_summary
                            .map(|s| format!(" | {}", s))
                            .unwrap_or_default();
                        let running_label = format_running_label(&step_id, &action_label);
                        let _ = runtime_tx_events
                            .send(RuntimeMsg::OutputTransient {
                                slot: TransientSlot::Status,
                                text: format!("{}{}", running_label, input_summary),
                            })
                            .await;
                    }
                    UiEvent::StepCompleted {
                        step_id,
                        action,
                        output,
                    } => {
                        completed_steps = completed_steps.saturating_add(1);
                        let action_name = normalize_step_action_label(&step_id, action);
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
                            .send(RuntimeMsg::ExecutionProgress {
                                step: completed_steps,
                            })
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
                        if let Some(agent_group) = active_agent_groups.remove(&step_id) {
                            let _ = runtime_tx_events
                                .send(RuntimeMsg::ActivityEnd {
                                    step_id: step_id.clone(),
                                    action: agent_group,
                                    failed: false,
                                })
                                .await;
                        }
                        let _ = runtime_tx_events
                            .send(RuntimeMsg::ActivityEnd {
                                step_id,
                                action: action_name,
                                failed: false,
                            })
                            .await;
                    }
                    UiEvent::StepFailed {
                        step_id,
                        action,
                        message,
                    } => {
                        let action_name = normalize_step_action_label(&step_id, action);
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
                        if let Some(agent_group) = active_agent_groups.remove(&step_id) {
                            let _ = runtime_tx_events
                                .send(RuntimeMsg::ActivityEnd {
                                    step_id: step_id.clone(),
                                    action: agent_group,
                                    failed: true,
                                })
                                .await;
                        }
                        let _ = runtime_tx_events
                            .send(RuntimeMsg::ActivityEnd {
                                step_id,
                                action: action_name,
                                failed: true,
                            })
                            .await;
                    }
                    UiEvent::AgentProgress {
                        step_id,
                        kind,
                        action,
                        message,
                    } => match kind {
                        AgentProgressKind::Iteration | AgentProgressKind::Note => {
                            if let Some(line) = message.filter(|m| !m.trim().is_empty()) {
                                let activity_action = step_id.clone();
                                let _ = runtime_tx_events
                                    .send(RuntimeMsg::ActivityItem {
                                        step_id,
                                        action: activity_action,
                                        line,
                                    })
                                    .await;
                            }
                        }
                        AgentProgressKind::ActionStarted => {
                            let base_action = action.unwrap_or_else(|| "agent_action".to_string());
                            if let Some(previous_group) = active_agent_groups.remove(&step_id) {
                                let _ = runtime_tx_events
                                    .send(RuntimeMsg::ActivityEnd {
                                        step_id: step_id.clone(),
                                        action: previous_group,
                                        failed: true,
                                    })
                                    .await;
                            }
                            let counter_key =
                                format!("{}::{}", step_id, base_action.to_ascii_lowercase());
                            let next = agent_action_counts
                                .entry(counter_key)
                                .and_modify(|n| *n += 1)
                                .or_insert(1);
                            let group_action = format!("{} #{}", base_action, *next);
                            active_agent_groups.insert(step_id.clone(), group_action.clone());
                            let _ = runtime_tx_events
                                .send(RuntimeMsg::ActivityStart {
                                    kind: classify_activity_kind(&base_action),
                                    step_id,
                                    action: group_action,
                                    input_summary: None,
                                })
                                .await;
                        }
                        AgentProgressKind::ActionCompleted | AgentProgressKind::ActionFailed => {
                            let group_action =
                                active_agent_groups.remove(&step_id).unwrap_or_else(|| {
                                    action.clone().unwrap_or_else(|| "agent_action".to_string())
                                });
                            if let Some(line) = message.filter(|m| !m.trim().is_empty()) {
                                let _ = runtime_tx_events
                                    .send(RuntimeMsg::ActivityItem {
                                        step_id: step_id.clone(),
                                        action: group_action.clone(),
                                        line,
                                    })
                                    .await;
                            }
                            let _ = runtime_tx_events
                                .send(RuntimeMsg::ActivityEnd {
                                    step_id,
                                    action: group_action,
                                    failed: matches!(kind, AgentProgressKind::ActionFailed),
                                })
                                .await;
                        }
                    },
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
                    UiEvent::ExecutionCompleted {
                        status,
                        execution_mode,
                    } => {
                        debug!(
                            status = ?status,
                            execution_mode = ?execution_mode,
                            pending_execution_end = pending_execution_end,
                            assistant_rendered = assistant_rendered,
                            "tui event: execution_completed"
                        );
                        if let Some(mode) = execution_mode {
                            let _ = runtime_tx_events
                                .send(RuntimeMsg::OutputPersist(format!(
                                    "ExecutionMode: {}",
                                    mode
                                )))
                                .await;
                        }
                        match status.as_deref() {
                            Some("completed") | Some("clarification") | None => {
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
                        let _ = runtime_tx_events
                            .send(RuntimeMsg::OutputPersist(line))
                            .await;
                    }
                    UiEvent::ReplanningCompleted { message } => {
                        let line = message.unwrap_or_else(|| {
                            "Recovery plan ready, resuming execution.".to_string()
                        });
                        let _ = runtime_tx_events
                            .send(RuntimeMsg::OutputPersist(line))
                            .await;
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
                        let _ = runtime_tx_events
                            .send(RuntimeMsg::OutputPersist(line))
                            .await;
                    }
                    UiEvent::AssistantOutput { message, status } => {
                        let normalized = message.trim().to_string();
                        if normalized.is_empty() {
                            continue;
                        }
                        assistant_output_received = true;
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
                            stream_active = false;
                            streamed_assistant.clear();
                            assistant_rendered = true;
                            debug!("tui action: assistant_output deduped against streamed text");
                            if pending_execution_end {
                                debug!("tui action: execution_end after assistant_output (dedup)");
                                pending_execution_end = false;
                                let _ = runtime_tx_events.send(RuntimeMsg::ExecutionEnd).await;
                                assistant_rendered = false;
                                last_preview = None;
                                last_assistant_fingerprint = None;
                                last_streamed_assistant = None;
                            }
                            mark_turn_settled(&mut turn_settled_tx);
                            continue;
                        }
                        if stream_active {
                            stream_active = false;
                            streamed_assistant.clear();
                        }
                        if last_assistant_fingerprint.as_deref() != Some(normalized.as_str()) {
                            last_assistant_fingerprint = Some(normalized.clone());
                            assistant_rendered = true;
                            if let Some(status) = status.as_deref() {
                                if status != "completed" {
                                    let _ = runtime_tx_events
                                        .send(RuntimeMsg::OutputPersist(format!(
                                            "Status: {}",
                                            status
                                        )))
                                        .await;
                                }
                            }
                            debug!(
                                line_len = normalized.len(),
                                "tui action: output_persist from assistant_output"
                            );
                            let _ = runtime_tx_events
                                .send(RuntimeMsg::AssistantOutput(normalized.clone()))
                                .await;
                            let _ = runtime_tx_events
                                .send(RuntimeMsg::OutputPersist(normalized))
                                .await;
                        }
                        if pending_execution_end {
                            debug!("tui action: execution_end after assistant_output");
                            pending_execution_end = false;
                            let _ = runtime_tx_events.send(RuntimeMsg::ExecutionEnd).await;
                            assistant_rendered = false;
                            last_preview = None;
                            last_assistant_fingerprint = None;
                            last_streamed_assistant = None;
                            streamed_assistant.clear();
                            stream_active = false;
                        }
                        mark_turn_settled(&mut turn_settled_tx);
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
                            let has_streamed_final = !streamed_final.is_empty();
                            if has_streamed_final {
                                last_streamed_assistant = Some(streamed_final);
                            }
                            let _ = runtime_tx_events
                                .send(RuntimeMsg::AssistantDelta {
                                    chunk: String::new(),
                                    done: true,
                                })
                                .await;
                            if !has_streamed_final {
                                debug!("tui action: assistant_stream done without content; waiting for assistant_output");
                            }
                            if pending_execution_end {
                                debug!(
                                    has_streamed_final = has_streamed_final,
                                    "tui action: assistant_stream done; waiting assistant_output before execution_end"
                                );
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

            if pending_execution_end {
                debug!(
                    assistant_rendered = assistant_rendered,
                    assistant_output_received = assistant_output_received,
                    "tui action: execution_end at forwarder shutdown"
                );
                if !assistant_output_received {
                    if let Some(streamed_final) = last_streamed_assistant.take() {
                        let _ = runtime_tx_events
                            .send(RuntimeMsg::OutputPersist(streamed_final))
                            .await;
                    } else if !assistant_rendered {
                        if let Some(preview) = last_preview {
                            let _ = runtime_tx_events
                                .send(RuntimeMsg::OutputPersist(preview))
                                .await;
                        }
                    }
                } else if !assistant_rendered {
                    if let Some(preview) = last_preview {
                        let _ = runtime_tx_events
                            .send(RuntimeMsg::OutputPersist(preview))
                            .await;
                    }
                }
                let _ = runtime_tx_events.send(RuntimeMsg::ExecutionEnd).await;
                mark_turn_settled(&mut turn_settled_tx);
            }
            debug!(thread_id = %thread_id, "submit_chain: forward task exiting");
        });

        let submit_result = self.runtime.submit_input(input).await;
        debug!("submit_input returned, result_ok={}", submit_result.is_ok());

        match submit_result {
            Ok(response) => {
                debug!(
                    interaction_id = ?response.interaction_id,
                    status = ?response.status,
                    "submit response received"
                );
                if response.interaction_id.is_none() {
                    if let Some(message) = response.message {
                        let _ = runtime_tx
                            .send(RuntimeMsg::OutputPersist(format!("Waiting: {}", message)))
                            .await;
                    }
                    let _ = runtime_tx.send(RuntimeMsg::ExecutionEnd).await;
                } else {
                    debug!(
                        expected_interaction_id = ?response.interaction_id,
                        "submit_chain: setting expected interaction id"
                    );
                    if matches!(response.status, SubmitStatus::Merged) {
                        let _ = runtime_tx
                            .send(RuntimeMsg::OutputTransient {
                                slot: TransientSlot::Status,
                                text: "Merged into current running turn...".to_string(),
                            })
                            .await;
                    }
                    let _ = expected_interaction_tx.send(response.interaction_id.clone());
                    debug!(
                        "waiting for turn_settled (timeout={}s)",
                        TURN_SETTLE_TIMEOUT.as_secs()
                    );
                    let mut turn_settled_rx = turn_settled_rx;
                    match timeout(TURN_SETTLE_TIMEOUT, &mut turn_settled_rx).await {
                        Ok(Ok(())) | Ok(Err(_)) => {
                            debug!("turn settled normally");
                        }
                        Err(_) => {
                            warn!(
                                timeout_secs = TURN_SETTLE_TIMEOUT.as_secs(),
                                "soft timeout waiting for terminal turn event; entering grace window"
                            );
                            let _ = runtime_tx
                                .send(RuntimeMsg::OutputTransient {
                                    slot: TransientSlot::Status,
                                    text: "Still waiting for assistant response...".to_string(),
                                })
                                .await;
                            match timeout(TURN_SETTLE_GRACE_TIMEOUT, turn_settled_rx).await {
                                Ok(Ok(())) | Ok(Err(_)) => {
                                    debug!("turn settled during grace window");
                                }
                                Err(_) => {
                                    warn!(
                                        timeout_secs = TURN_SETTLE_GRACE_TIMEOUT.as_secs(),
                                        "timeout waiting for terminal turn event after grace window; forcing execution end"
                                    );
                                    let _ = runtime_tx
                                        .send(RuntimeMsg::Error(format!(
                                            "timed out waiting for assistant response ({}s + {}s)",
                                            TURN_SETTLE_TIMEOUT.as_secs(),
                                            TURN_SETTLE_GRACE_TIMEOUT.as_secs()
                                        )))
                                        .await;
                                    let _ = runtime_tx.send(RuntimeMsg::ExecutionEnd).await;
                                }
                            }
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

        debug!("submit_chain: stopping forward task");
        let _ = stop_tx.send(true);
        let _ = forward.await;
        debug!("submit_chain: runtime_client submit_input finished");

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
