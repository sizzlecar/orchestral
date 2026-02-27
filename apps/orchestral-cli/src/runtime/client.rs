use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio::time::{timeout, Duration};
use tracing::{debug, warn};

use orchestral_composition::{ComposedRuntimeAppBuilder, RuntimeTarget};
use orchestral_core::store::Event as ChannelEvent;
use orchestral_runtime::api::{RuntimeApi, RuntimeAppBuilder, SubmitStatus};

use crate::channel::CliRuntime;

use super::event_projection::{project_event, UiEvent};
use crate::runtime::protocol::{ActivityKind, RuntimeMsg, TransientSlot};

const TURN_SETTLE_TIMEOUT: Duration = Duration::from_secs(30);
const TURN_SETTLE_GRACE_TIMEOUT: Duration = Duration::from_secs(60);
const FORWARD_DRAIN_IDLE_TIMEOUT: Duration = Duration::from_millis(120);

#[derive(Clone)]
pub struct RuntimeClient {
    runtime: Arc<CliRuntime>,
    thread_id: String,
    submit_lock: Arc<Mutex<()>>,
}

impl RuntimeClient {
    pub async fn from_config(
        config: Option<PathBuf>,
        thread_id_override: Option<String>,
    ) -> anyhow::Result<Self> {
        let config = resolve_runtime_config_path(config)?;
        let app_builder: Arc<dyn RuntimeAppBuilder> =
            Arc::new(ComposedRuntimeAppBuilder::new(RuntimeTarget::Cli));
        let api = Arc::new(
            RuntimeApi::from_config_path_with_builder(config, app_builder)
                .await
                .context("failed to build runtime api")?,
        );
        let runtime = CliRuntime::from_api(api, thread_id_override)
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
                        let _ = runtime_tx_events
                            .send(RuntimeMsg::OutputPersist(plan))
                            .await;
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
                    UiEvent::AssistantOutput { message } => {
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
                            // Stream already rendered this answer live.
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
                        // If a stream was open but no deduplicated final arrived, prefer the
                        // explicit assistant_output payload to avoid losing visible replies.
                        if stream_active {
                            stream_active = false;
                            streamed_assistant.clear();
                        }
                        if last_assistant_fingerprint.as_deref() != Some(normalized.as_str()) {
                            last_assistant_fingerprint = Some(normalized.clone());
                            assistant_rendered = true;
                            debug!(
                                line_len = normalized.len(),
                                "tui action: output_persist from assistant_output"
                            );
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
                                debug!(
                                            "tui action: assistant_stream done without content; waiting for assistant_output"
                                        );
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

const GENERATED_CONFIG_DIR: &str = ".orchestral/generated";
const GENERATED_CONFIG_FILE: &str = "default.cli.yaml";

fn resolve_runtime_config_path(explicit: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    if let Some(path) = explicit {
        if !path.exists() {
            anyhow::bail!("config file not found: {}", path.display());
        }
        return Ok(path);
    }

    if let Some(found) = discover_config_path() {
        return Ok(found);
    }

    generate_default_config()
}

fn discover_config_path() -> Option<PathBuf> {
    let candidates = [
        PathBuf::from(".orchestral/config.yaml"),
        PathBuf::from(".orchestral/config.yml"),
        PathBuf::from("configs/orchestral.cli.yaml"),
        PathBuf::from("configs/orchestral.yaml"),
        PathBuf::from("orchestral.yaml"),
    ];
    candidates.into_iter().find(|path| path.exists())
}

fn generate_default_config() -> anyhow::Result<PathBuf> {
    let cwd = std::env::current_dir().context("resolve current directory failed")?;
    let dir = cwd.join(GENERATED_CONFIG_DIR);
    fs::create_dir_all(&dir)
        .with_context(|| format!("create default config dir '{}' failed", dir.display()))?;
    let path = dir.join(GENERATED_CONFIG_FILE);

    let needs_write = match fs::read_to_string(&path) {
        Ok(existing) => existing != embedded_default_config(),
        Err(_) => true,
    };

    if needs_write {
        fs::write(&path, embedded_default_config())
            .with_context(|| format!("write generated config '{}' failed", path.display()))?;
    }

    Ok(path)
}

fn embedded_default_config() -> &'static str {
    r#"version: 1

app:
  name: orchestral-cli
  environment: development

runtime:
  max_interactions_per_thread: 10
  auto_cleanup: true
  concurrency_policy: interrupt_and_start_new
  strict_exports: true

planner:
  mode: llm
  backend: openai
  model_profile: fast
  max_history: 20
  dynamic_model_selection: true

interpreter:
  mode: auto

context:
  history_limit: 50
  max_tokens: 4096
  include_history: true
  include_references: true

extensions:
  mcp:
    enabled: true
    auto_discover: true
  skill:
    enabled: true
    auto_discover: true

providers:
  default_backend: openai
  default_model: fast
  backends:
    - name: openai
      kind: openai
      api_key_env: OPENAI_API_KEY
      config:
        timeout_secs: 30
  models:
    - name: fast
      backend: openai
      model: gpt-4o-mini
      temperature: 0.2

actions:
  hot_reload: false
  actions:
    - name: echo
      kind: echo
      description: Echo text back.
      interface:
        input_schema:
          type: object
          properties:
            message:
              type: string
          required: [message]
        output_schema:
          type: object
          properties:
            result:
              type: string
          required: [result]
      config:
        prefix: "Echo: "
    - name: shell
      kind: shell
      description: Run a shell command.
      interface:
        input_schema:
          type: object
          properties:
            command:
              type: string
            args:
              type: array
              items:
                type: string
          required: [command]
        output_schema:
          type: object
          properties:
            stdout:
              type: string
            stderr:
              type: string
            status:
              type: integer
          required: [stdout, stderr, status]
      config:
        timeout_ms: 10000
    - name: file_read
      kind: file_read
      description: Read a file from workspace.
      interface:
        input_schema:
          type: object
          properties:
            path:
              type: string
          required: [path]
        output_schema:
          type: object
          properties:
            content:
              type: string
            path:
              type: string
          required: [content, path]
      config:
        root_dir: "."
"#
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

fn normalize_step_action_label(step_id: &str, action: Option<String>) -> String {
    action
        .and_then(|raw| {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .unwrap_or_else(|| step_id.to_string())
}

fn format_running_label(step_id: &str, action_label: &str) -> String {
    if action_label == step_id {
        format!("Running {}", step_id)
    } else {
        format!("Running {} ({})", step_id, action_label)
    }
}

fn channel_event_label(event: &ChannelEvent) -> &'static str {
    match event {
        ChannelEvent::UserInput { .. } => "UserInput",
        ChannelEvent::AssistantOutput { .. } => "AssistantOutput",
        ChannelEvent::Artifact { .. } => "Artifact",
        ChannelEvent::SystemTrace { .. } => "SystemTrace",
        ChannelEvent::ExternalEvent { .. } => "ExternalEvent",
    }
}

fn ui_event_label(event: &UiEvent) -> &'static str {
    match event {
        UiEvent::AssistantOutput { .. } => "AssistantOutput",
        UiEvent::AssistantStreamDelta { .. } => "AssistantStreamDelta",
        UiEvent::TurnStarted => "TurnStarted",
        UiEvent::TurnResumed => "TurnResumed",
        UiEvent::TurnRejected { .. } => "TurnRejected",
        UiEvent::TurnQueued => "TurnQueued",
        UiEvent::ReplanningStarted { .. } => "ReplanningStarted",
        UiEvent::ReplanningCompleted { .. } => "ReplanningCompleted",
        UiEvent::ReplanningFailed { .. } => "ReplanningFailed",
        UiEvent::PlanningStarted => "PlanningStarted",
        UiEvent::PlanningCompleted { .. } => "PlanningCompleted",
        UiEvent::ExecutionStarted => "ExecutionStarted",
        UiEvent::ExecutionCompleted { .. } => "ExecutionCompleted",
        UiEvent::StepStarted { .. } => "StepStarted",
        UiEvent::StepCompleted { .. } => "StepCompleted",
        UiEvent::StepFailed { .. } => "StepFailed",
        UiEvent::InputRequired { .. } => "InputRequired",
        UiEvent::TaskCompleted => "TaskCompleted",
        UiEvent::TaskFailed { .. } => "TaskFailed",
    }
}

fn log_preview(text: &str, max_chars: usize) -> String {
    text.chars().take(max_chars).collect()
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

    #[test]
    fn test_normalize_step_action_label_falls_back_to_step_id_for_empty_action() {
        assert_eq!(
            normalize_step_action_label("process_xlsx", Some("".to_string())),
            "process_xlsx"
        );
        assert_eq!(
            normalize_step_action_label("process_xlsx", Some("   ".to_string())),
            "process_xlsx"
        );
        assert_eq!(
            normalize_step_action_label("process_xlsx", None),
            "process_xlsx"
        );
        assert_eq!(
            normalize_step_action_label("process_xlsx", Some("file_read".to_string())),
            "file_read"
        );
    }

    #[test]
    fn test_format_running_label_avoids_duplicate_parentheses() {
        assert_eq!(
            format_running_label("process_xlsx", "process_xlsx"),
            "Running process_xlsx"
        );
        assert_eq!(
            format_running_label("read_skill_docs", "file_read"),
            "Running read_skill_docs (file_read)"
        );
    }
}
