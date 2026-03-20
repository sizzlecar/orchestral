use serde_json::Value;

use orchestral_core::store::Event;

const MAX_PREVIEW_CHARS: usize = 320;

#[derive(Debug, Clone)]
pub struct StepSummary {
    pub id: String,
    pub action: String,
}

#[derive(Debug, Clone)]
pub struct StepOutput {
    pub status: Option<i64>,
    pub path: Option<String>,
    pub bytes: Option<u64>,
    pub preview: Option<String>,
}

#[derive(Debug, Clone)]
pub enum UiEvent {
    AssistantOutput {
        message: String,
        status: Option<String>,
    },
    AssistantStreamDelta {
        delta: String,
        done: bool,
    },
    TurnStarted,
    TurnResumed,
    TurnRejected {
        reason: Option<String>,
    },
    TurnQueued,
    ReplanningStarted {
        message: Option<String>,
    },
    ReplanningCompleted {
        message: Option<String>,
    },
    ReplanningFailed {
        message: Option<String>,
        error: Option<String>,
    },
    PlanningStarted,
    PlanningCompleted {
        step_count: Option<usize>,
        steps: Vec<StepSummary>,
        output_type: Option<String>,
    },
    ExecutionStarted {
        execution_mode: Option<String>,
    },
    ExecutionCompleted {
        status: Option<String>,
        execution_mode: Option<String>,
        agent_loop_continue: bool,
    },
    StepStarted {
        step_id: String,
        action: Option<String>,
        input_summary: Option<String>,
    },
    StepCompleted {
        step_id: String,
        action: Option<String>,
        output: StepOutput,
    },
    StepFailed {
        step_id: String,
        action: Option<String>,
        message: Option<String>,
    },
    AgentProgress {
        step_id: String,
        kind: AgentProgressKind,
        action: Option<String>,
        message: Option<String>,
    },
    InputRequired {
        prompt: Option<String>,
        waiting_kind: Option<String>,
        approval_reason: Option<String>,
        approval_command: Option<String>,
    },
    TaskCompleted,
    TaskFailed {
        message: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProgressKind {
    Note,
    Iteration,
    ActionStarted,
    ActionCompleted,
    ActionFailed,
}

pub fn project_event(event: &Event) -> Option<UiEvent> {
    match event {
        Event::AssistantOutput { payload, .. } => payload
            .get("message")
            .and_then(|v| v.as_str())
            .map(|message| UiEvent::AssistantOutput {
                message: message.to_string(),
                status: payload
                    .get("result")
                    .and_then(|v| v.get("status"))
                    .and_then(|v| v.as_str())
                    .map(str::to_string),
            }),
        Event::SystemTrace { payload, .. } => {
            let category = payload.get("category")?.as_str()?;
            match category {
                "runtime_lifecycle" => project_lifecycle_event(payload),
                "execution_progress" => project_execution_progress(payload),
                "assistant_stream" => project_assistant_stream(payload),
                _ => None,
            }
        }
        _ => None,
    }
}

fn project_assistant_stream(payload: &Value) -> Option<UiEvent> {
    let done = payload
        .get("done")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let delta = payload
        .get("delta")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    if delta.is_empty() && !done {
        return None;
    }
    Some(UiEvent::AssistantStreamDelta { delta, done })
}

fn project_lifecycle_event(payload: &Value) -> Option<UiEvent> {
    let event_type = payload.get("event_type")?.as_str()?;
    let metadata = payload.get("metadata");
    match event_type {
        "turn_started" => Some(UiEvent::TurnStarted),
        "turn_resumed" => Some(UiEvent::TurnResumed),
        "turn_rejected" => Some(UiEvent::TurnRejected {
            reason: payload
                .get("message")
                .and_then(|v| v.as_str())
                .map(str::to_string),
        }),
        "turn_queued" => Some(UiEvent::TurnQueued),
        "replanning_started" => Some(UiEvent::ReplanningStarted {
            message: payload
                .get("message")
                .and_then(|v| v.as_str())
                .map(str::to_string),
        }),
        "replanning_completed" => Some(UiEvent::ReplanningCompleted {
            message: payload
                .get("message")
                .and_then(|v| v.as_str())
                .map(str::to_string),
        }),
        "replanning_failed" => Some(UiEvent::ReplanningFailed {
            message: payload
                .get("message")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            error: metadata
                .and_then(|m| m.get("error"))
                .and_then(|v| v.as_str())
                .map(str::to_string),
        }),
        "planning_started" => Some(UiEvent::PlanningStarted),
        "planning_completed" => {
            let step_count = metadata
                .and_then(|m| m.get("step_count"))
                .and_then(as_usize);
            let steps = metadata
                .and_then(|m| m.get("steps"))
                .and_then(parse_step_summaries)
                .unwrap_or_default();
            let output_type = metadata
                .and_then(|m| m.get("output_type"))
                .and_then(|v| v.as_str())
                .map(str::to_string);
            Some(UiEvent::PlanningCompleted {
                step_count,
                steps,
                output_type,
            })
        }
        "execution_started" => Some(UiEvent::ExecutionStarted {
            execution_mode: metadata
                .and_then(|m| m.get("execution_mode"))
                .and_then(|v| v.as_str())
                .map(str::to_string),
        }),
        "execution_completed" => {
            let status = metadata
                .and_then(|m| m.get("status"))
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let agent_loop_continue = metadata
                .and_then(|m| m.get("agent_loop_continue"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if matches!(status.as_deref(), Some("waiting_user" | "waiting_event")) {
                return Some(UiEvent::InputRequired {
                    prompt: metadata
                        .and_then(|m| m.get("prompt").or_else(|| m.get("event_type")))
                        .and_then(|v| v.as_str())
                        .map(str::to_string),
                    waiting_kind: metadata
                        .and_then(|m| m.get("waiting_kind"))
                        .and_then(|v| v.as_str())
                        .map(str::to_string),
                    approval_reason: metadata
                        .and_then(|m| m.get("approval_reason"))
                        .and_then(|v| v.as_str())
                        .map(str::to_string),
                    approval_command: metadata
                        .and_then(|m| m.get("approval_command"))
                        .and_then(|v| v.as_str())
                        .map(str::to_string),
                });
            }
            Some(UiEvent::ExecutionCompleted {
                status,
                execution_mode: metadata
                    .and_then(|m| m.get("execution_mode"))
                    .and_then(|v| v.as_str())
                    .map(str::to_string),
                agent_loop_continue,
            })
        }
        _ => None,
    }
}

fn project_execution_progress(payload: &Value) -> Option<UiEvent> {
    let phase = payload.get("phase")?.as_str()?;
    let step_id = payload
        .get("step_id")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let action = payload
        .get("action")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let metadata = payload.get("metadata");
    let input_summary = metadata
        .and_then(|m| m.get("input_summary"))
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let message = payload
        .get("message")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    match phase {
        "step_started" => Some(UiEvent::StepStarted {
            step_id: step_id?,
            action,
            input_summary,
        }),
        "step_completed" => Some(UiEvent::StepCompleted {
            step_id: step_id?,
            action,
            output: parse_step_output(metadata),
        }),
        "step_failed" => Some(UiEvent::StepFailed {
            step_id: step_id?,
            action,
            message,
        }),
        "agent_progress" => Some(UiEvent::AgentProgress {
            step_id: step_id?,
            kind: parse_agent_progress_kind(metadata)?,
            action: metadata
                .and_then(|m| m.get("agent_action"))
                .and_then(|v| v.as_str())
                .map(str::to_string),
            message,
        }),
        "step_waiting_user" | "step_waiting_event" => Some(UiEvent::InputRequired {
            prompt: message,
            waiting_kind: metadata
                .and_then(|m| m.get("waiting_kind"))
                .and_then(|v| v.as_str())
                .map(str::to_string),
            approval_reason: metadata
                .and_then(|m| m.get("approval_reason"))
                .and_then(|v| v.as_str())
                .map(str::to_string),
            approval_command: metadata
                .and_then(|m| m.get("approval_command"))
                .and_then(|v| v.as_str())
                .map(str::to_string),
        }),
        "task_completed" => Some(UiEvent::TaskCompleted),
        "task_failed" => Some(UiEvent::TaskFailed { message }),
        _ => None,
    }
}

fn parse_step_summaries(value: &Value) -> Option<Vec<StepSummary>> {
    let items = value.get("items")?.as_array()?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let id = item.get("id").and_then(|v| v.as_str())?.to_string();
        let action = item.get("action").and_then(|v| v.as_str())?.to_string();
        out.push(StepSummary { id, action });
    }
    Some(out)
}

fn parse_step_output(metadata: Option<&Value>) -> StepOutput {
    let status = metadata
        .and_then(|m| m.get("status"))
        .and_then(|v| v.as_i64());
    let path = metadata
        .and_then(|m| m.get("path"))
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let bytes = metadata
        .and_then(|m| m.get("bytes"))
        .and_then(|v| v.as_u64());
    let preview = metadata
        .and_then(|m| m.get("output_preview"))
        .and_then(|v| v.as_str())
        .map(|s| truncate(s, MAX_PREVIEW_CHARS));
    StepOutput {
        status,
        path,
        bytes,
        preview,
    }
}

fn as_usize(value: &Value) -> Option<usize> {
    value
        .as_u64()
        .and_then(|n| usize::try_from(n).ok())
        .or_else(|| value.as_i64().and_then(|n| usize::try_from(n).ok()))
}

fn parse_agent_progress_kind(metadata: Option<&Value>) -> Option<AgentProgressKind> {
    let raw = metadata
        .and_then(|m| m.get("agent_progress_kind"))
        .and_then(|v| v.as_str())
        .unwrap_or("note");
    match raw {
        "note" => Some(AgentProgressKind::Note),
        "iteration" => Some(AgentProgressKind::Iteration),
        "action_started" => Some(AgentProgressKind::ActionStarted),
        "action_completed" => Some(AgentProgressKind::ActionCompleted),
        "action_failed" => Some(AgentProgressKind::ActionFailed),
        _ => None,
    }
}

fn truncate(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut preview: String = text.chars().take(max_chars).collect();
    preview.push_str("...");
    preview
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::store::Event;
    use serde_json::json;

    #[test]
    fn test_project_execution_progress_agent_progress() {
        let event = Event::trace(
            "thread-1",
            "info",
            json!({
                "category": "execution_progress",
                "interaction_id": "int-1",
                "task_id": "task-1",
                "phase": "agent_progress",
                "step_id": "summarize_excel",
                "message": "iteration 2/8",
                "metadata": {
                    "agent_progress_kind": "iteration"
                }
            }),
        );

        let projected = project_event(&event);
        match projected {
            Some(UiEvent::AgentProgress {
                step_id,
                kind,
                action,
                message,
            }) => {
                assert_eq!(step_id, "summarize_excel");
                assert_eq!(kind, AgentProgressKind::Iteration);
                assert_eq!(action, None);
                assert_eq!(message.as_deref(), Some("iteration 2/8"));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn test_project_execution_completed_preserves_agent_loop_continue_flag() {
        let event = Event::trace(
            "thread-1",
            "info",
            json!({
                "category": "runtime_lifecycle",
                "interaction_id": "int-1",
                "task_id": "task-1",
                "event_type": "execution_completed",
                "metadata": {
                    "status": "failed",
                    "execution_mode": "mini_plan",
                    "agent_loop_continue": true
                }
            }),
        );

        let projected = project_event(&event);
        match projected {
            Some(UiEvent::ExecutionCompleted {
                status,
                execution_mode,
                agent_loop_continue,
            }) => {
                assert_eq!(status.as_deref(), Some("failed"));
                assert_eq!(execution_mode.as_deref(), Some("mini_plan"));
                assert!(agent_loop_continue);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
}
