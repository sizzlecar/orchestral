use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use serde_json::json;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::time::{sleep, Duration};

use orchestral_runtime::{project_event, RuntimeApp, UiEvent};
use orchestral_stores::Event;

use crate::runtime::protocol::{ActivityKind, RuntimeMsg, TransientSlot};

#[derive(Clone)]
pub struct RuntimeClient {
    app: Arc<RuntimeApp>,
    thread_id: String,
    submit_lock: Arc<Mutex<()>>,
}

impl RuntimeClient {
    pub async fn from_config(
        config: PathBuf,
        thread_id_override: Option<String>,
    ) -> anyhow::Result<Self> {
        let app = RuntimeApp::from_config_path(config)
            .await
            .context("failed to build runtime app from config")?;
        if let Some(thread_id) = thread_id_override {
            let mut thread = app.orchestrator.thread_runtime.thread.write().await;
            thread.id = thread_id;
        }
        let thread_id = app.orchestrator.thread_runtime.thread_id().await;
        Ok(Self {
            app: Arc::new(app),
            thread_id,
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

        let mut rx = self.app.orchestrator.thread_runtime.subscribe_events();
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let thread_id = self.thread_id.clone();
        let runtime_tx_events = runtime_tx.clone();

        let forward = tokio::spawn(async move {
            let mut completed_steps: usize = 0;
            let mut total_steps: usize = 0;
            let mut last_preview: Option<String> = None;

            loop {
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                    msg = rx.recv() => {
                        let Ok(event) = msg else { break; };
                        if event.thread_id() != thread_id {
                            continue;
                        }
                        let Some(ui_event) = project_event(&event) else {
                            continue;
                        };

                        match ui_event {
                            UiEvent::PlanningStarted { .. } => {
                                let _ = runtime_tx_events.send(RuntimeMsg::PlanningStart).await;
                                let _ = runtime_tx_events
                                    .send(RuntimeMsg::OutputTransient {
                                        slot: TransientSlot::Footer,
                                        text: "Planning... Ctrl+C to interrupt".to_string(),
                                    })
                                    .await;
                            }
                            UiEvent::PlanningCompleted { step_count, steps, .. } => {
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
                            UiEvent::ExecutionStarted { .. } => {
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
                            UiEvent::StepCompleted { step_id, action, output, .. } => {
                                completed_steps = completed_steps.saturating_add(1);
                                let action_name = action.unwrap_or_else(|| "-".to_string());
                                if let Some(preview) = output.preview.clone() {
                                    last_preview = Some(preview);
                                }
                                let mut parts = Vec::new();
                                if let Some(status) = output.status {
                                    parts.push(format!("status={}", status));
                                }
                                if let Some(path) = output.path {
                                    parts.push(format!("path={}", path));
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
                            UiEvent::StepFailed { step_id, action, message, .. } => {
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
                            UiEvent::InputRequired { prompt, .. } => {
                                let _ = runtime_tx_events
                                    .send(RuntimeMsg::OutputPersist(format!(
                                        "Waiting: {}",
                                        prompt.unwrap_or_else(|| "input required".to_string())
                                    )))
                                    .await;
                            }
                            UiEvent::ExecutionCompleted { status, .. } => {
                                match status.as_deref() {
                                    Some("completed") => {
                                        let _ = runtime_tx_events.send(RuntimeMsg::ExecutionEnd).await;
                                        if let Some(preview) = last_preview.clone() {
                                            let _ = runtime_tx_events
                                                .send(RuntimeMsg::OutputPersist("Final Answer".to_string()))
                                                .await;
                                            let _ = runtime_tx_events
                                                .send(RuntimeMsg::OutputPersist(preview))
                                                .await;
                                        }
                                    }
                                    Some(other) => {
                                        let _ = runtime_tx_events
                                            .send(RuntimeMsg::OutputPersist(format!("Status: {}", other)))
                                            .await;
                                    }
                                    None => {}
                                }
                            }
                            UiEvent::TaskFailed { message, .. } => {
                                let _ = runtime_tx_events
                                    .send(RuntimeMsg::Error(
                                        message.unwrap_or_else(|| "task failed".to_string()),
                                    ))
                                    .await;
                            }
                            UiEvent::TaskCompleted { .. }
                            | UiEvent::TurnStarted { .. }
                            | UiEvent::TurnResumed { .. }
                            | UiEvent::TurnRejected { .. }
                            | UiEvent::TurnQueued { .. } => {}
                        }
                    }
                }
            }
        });

        let request = Event::user_input(&self.thread_id, "tui", json!({ "message": input }));
        let result = self.app.orchestrator.handle_event(request).await;
        // Give projected runtime events a short drain window before stopping forwarder.
        sleep(Duration::from_millis(120)).await;
        let _ = stop_tx.send(true);
        let _ = forward.await;

        if let Err(err) = result {
            let _ = runtime_tx
                .send(RuntimeMsg::Error(format!("runtime error: {}", err)))
                .await;
            return Err(anyhow::anyhow!(err));
        }

        Ok(())
    }

    pub async fn interrupt(&self, runtime_tx: mpsc::Sender<RuntimeMsg>) {
        self.app
            .orchestrator
            .thread_runtime
            .cancel_all_active()
            .await;
        let _ = runtime_tx
            .send(RuntimeMsg::OutputTransient {
                slot: TransientSlot::Status,
                text: "Interrupt requested".to_string(),
            })
            .await;
    }
}

fn classify_activity_kind(action: &str) -> ActivityKind {
    match action {
        "file_write" | "edit" | "patch" | "write" => ActivityKind::Edited,
        "http" | "search" | "read_file" | "list_files" | "find" | "grep" => {
            ActivityKind::Explored
        }
        _ => ActivityKind::Ran,
    }
}

fn preview_to_activity_lines(preview: &str) -> Vec<String> {
    let mut out = Vec::new();
    let lines: Vec<&str> = preview.lines().collect();
    let max_lines = 8usize;
    if lines.is_empty() {
        out.push("output: (empty)".to_string());
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
        out.push(format!("output: {}", snippet));
    }
    if lines.len() > max_lines {
        out.push(format!("output: ... +{} lines", lines.len() - max_lines));
    }
    if out.is_empty() {
        out.push("output: (empty)".to_string());
    }
    out
}
