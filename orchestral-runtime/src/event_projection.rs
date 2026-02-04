//! Projection helpers from internal events to client-friendly realtime events.
//!
//! This layer provides a stable payload shape for CLI/Web renderers and
//! supports SSE framing out of the box.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use orchestral_stores::Event;

const MAX_PREVIEW_CHARS: usize = 320;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct StepSummary {
    pub id: String,
    pub action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct StepOutput {
    pub status: Option<i64>,
    pub path: Option<String>,
    pub bytes: Option<u64>,
    pub preview: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum UiEvent {
    TurnStarted {
        thread_id: String,
        interaction_id: Option<String>,
        task_id: Option<String>,
    },
    TurnResumed {
        thread_id: String,
        interaction_id: Option<String>,
        task_id: Option<String>,
    },
    TurnRejected {
        thread_id: String,
        reason: String,
    },
    TurnQueued {
        thread_id: String,
    },
    PlanningStarted {
        thread_id: String,
        interaction_id: Option<String>,
        task_id: Option<String>,
    },
    PlanningCompleted {
        thread_id: String,
        interaction_id: Option<String>,
        task_id: Option<String>,
        goal: Option<String>,
        step_count: Option<usize>,
        steps: Vec<StepSummary>,
    },
    ExecutionStarted {
        thread_id: String,
        interaction_id: Option<String>,
        task_id: Option<String>,
    },
    ExecutionCompleted {
        thread_id: String,
        interaction_id: Option<String>,
        task_id: Option<String>,
        status: Option<String>,
    },
    StepStarted {
        thread_id: String,
        task_id: String,
        step_id: String,
        action: Option<String>,
        input_summary: Option<String>,
    },
    StepCompleted {
        thread_id: String,
        task_id: String,
        step_id: String,
        action: Option<String>,
        output: StepOutput,
    },
    StepFailed {
        thread_id: String,
        task_id: String,
        step_id: String,
        action: Option<String>,
        message: Option<String>,
    },
    InputRequired {
        thread_id: String,
        task_id: Option<String>,
        step_id: Option<String>,
        source: String,
        prompt: Option<String>,
    },
    TaskCompleted {
        thread_id: String,
        task_id: String,
    },
    TaskFailed {
        thread_id: String,
        task_id: String,
        message: Option<String>,
    },
}

impl UiEvent {
    pub fn event_name(&self) -> &'static str {
        match self {
            UiEvent::TurnStarted { .. } => "turn_started",
            UiEvent::TurnResumed { .. } => "turn_resumed",
            UiEvent::TurnRejected { .. } => "turn_rejected",
            UiEvent::TurnQueued { .. } => "turn_queued",
            UiEvent::PlanningStarted { .. } => "planning_started",
            UiEvent::PlanningCompleted { .. } => "planning_completed",
            UiEvent::ExecutionStarted { .. } => "execution_started",
            UiEvent::ExecutionCompleted { .. } => "execution_completed",
            UiEvent::StepStarted { .. } => "step_started",
            UiEvent::StepCompleted { .. } => "step_completed",
            UiEvent::StepFailed { .. } => "step_failed",
            UiEvent::InputRequired { .. } => "input_required",
            UiEvent::TaskCompleted { .. } => "task_completed",
            UiEvent::TaskFailed { .. } => "task_failed",
        }
    }

    pub fn to_sse_frame(&self) -> String {
        let data = serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string());
        format!("event: {}\ndata: {}\n\n", self.event_name(), data)
    }
}

/// Project internal runtime/store event into stable UI event.
pub fn project_event(event: &Event) -> Option<UiEvent> {
    let Event::SystemTrace {
        thread_id, payload, ..
    } = event
    else {
        return None;
    };
    let category = payload.get("category")?.as_str()?;
    match category {
        "runtime_lifecycle" => project_lifecycle_event(thread_id, payload),
        "execution_progress" => project_execution_progress(thread_id, payload),
        _ => None,
    }
}

fn project_lifecycle_event(thread_id: &str, payload: &Value) -> Option<UiEvent> {
    let event_type = payload.get("event_type")?.as_str()?;
    let interaction_id = get_string(payload, "interaction_id");
    let task_id = get_string(payload, "task_id");
    let metadata = payload.get("metadata");

    match event_type {
        "turn_started" => Some(UiEvent::TurnStarted {
            thread_id: thread_id.to_string(),
            interaction_id,
            task_id,
        }),
        "turn_resumed" => Some(UiEvent::TurnResumed {
            thread_id: thread_id.to_string(),
            interaction_id,
            task_id,
        }),
        "turn_rejected" => Some(UiEvent::TurnRejected {
            thread_id: thread_id.to_string(),
            reason: get_string(payload, "message").unwrap_or_else(|| "rejected".to_string()),
        }),
        "turn_queued" => Some(UiEvent::TurnQueued {
            thread_id: thread_id.to_string(),
        }),
        "planning_started" => Some(UiEvent::PlanningStarted {
            thread_id: thread_id.to_string(),
            interaction_id,
            task_id,
        }),
        "planning_completed" => {
            let goal = metadata.and_then(|m| get_string(m, "goal"));
            let step_count = metadata
                .and_then(|m| m.get("step_count"))
                .and_then(as_usize);
            let steps = metadata
                .and_then(|m| m.get("steps"))
                .and_then(parse_step_summaries)
                .unwrap_or_default();
            Some(UiEvent::PlanningCompleted {
                thread_id: thread_id.to_string(),
                interaction_id,
                task_id,
                goal,
                step_count,
                steps,
            })
        }
        "execution_started" => Some(UiEvent::ExecutionStarted {
            thread_id: thread_id.to_string(),
            interaction_id,
            task_id,
        }),
        "execution_completed" => {
            let status = metadata
                .and_then(|m| m.get("status"))
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let output = UiEvent::ExecutionCompleted {
                thread_id: thread_id.to_string(),
                interaction_id,
                task_id: task_id.clone(),
                status: status.clone(),
            };
            if matches!(status.as_deref(), Some("waiting_user")) {
                return Some(UiEvent::InputRequired {
                    thread_id: thread_id.to_string(),
                    task_id,
                    step_id: metadata
                        .and_then(|m| m.get("step_id"))
                        .and_then(|v| v.as_str())
                        .map(str::to_string),
                    source: "waiting_user".to_string(),
                    prompt: metadata
                        .and_then(|m| m.get("prompt"))
                        .and_then(|v| v.as_str())
                        .map(str::to_string),
                });
            }
            if matches!(status.as_deref(), Some("waiting_event")) {
                return Some(UiEvent::InputRequired {
                    thread_id: thread_id.to_string(),
                    task_id,
                    step_id: metadata
                        .and_then(|m| m.get("step_id"))
                        .and_then(|v| v.as_str())
                        .map(str::to_string),
                    source: "waiting_event".to_string(),
                    prompt: metadata
                        .and_then(|m| m.get("event_type"))
                        .and_then(|v| v.as_str())
                        .map(str::to_string),
                });
            }
            Some(output)
        }
        _ => None,
    }
}

fn project_execution_progress(thread_id: &str, payload: &Value) -> Option<UiEvent> {
    let phase = payload.get("phase")?.as_str()?;
    let task_id = payload
        .get("task_id")
        .and_then(|v| v.as_str())
        .map(str::to_string)?;
    let step_id = payload
        .get("step_id")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let action = payload
        .get("action")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let message = payload
        .get("message")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let metadata = payload.get("metadata");

    match phase {
        "step_started" => Some(UiEvent::StepStarted {
            thread_id: thread_id.to_string(),
            task_id,
            step_id: step_id?,
            action,
            input_summary: metadata
                .and_then(|m| m.get("input_summary"))
                .and_then(|v| v.as_str())
                .map(str::to_string),
        }),
        "step_completed" => Some(UiEvent::StepCompleted {
            thread_id: thread_id.to_string(),
            task_id,
            step_id: step_id?,
            action,
            output: parse_step_output(metadata),
        }),
        "step_failed" => Some(UiEvent::StepFailed {
            thread_id: thread_id.to_string(),
            task_id,
            step_id: step_id?,
            action,
            message,
        }),
        "step_waiting_user" => Some(UiEvent::InputRequired {
            thread_id: thread_id.to_string(),
            task_id: Some(task_id),
            step_id,
            source: "step_waiting_user".to_string(),
            prompt: message,
        }),
        "step_waiting_event" => Some(UiEvent::InputRequired {
            thread_id: thread_id.to_string(),
            task_id: Some(task_id),
            step_id,
            source: "step_waiting_event".to_string(),
            prompt: message,
        }),
        "task_completed" => Some(UiEvent::TaskCompleted {
            thread_id: thread_id.to_string(),
            task_id,
        }),
        "task_failed" => Some(UiEvent::TaskFailed {
            thread_id: thread_id.to_string(),
            task_id,
            message,
        }),
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

fn get_string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(|v| v.as_str()).map(str::to_string)
}

fn as_usize(value: &Value) -> Option<usize> {
    value
        .as_u64()
        .and_then(|n| usize::try_from(n).ok())
        .or_else(|| value.as_i64().and_then(|n| usize::try_from(n).ok()))
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
    use chrono::Utc;
    use serde_json::json;

    use super::*;

    #[test]
    fn test_project_runtime_lifecycle_plan_completed() {
        let event = Event::SystemTrace {
            thread_id: "thread-1".to_string(),
            level: "info".to_string(),
            payload: json!({
                "category":"runtime_lifecycle",
                "event_type":"planning_completed",
                "interaction_id":"i1",
                "task_id":"t1",
                "metadata":{
                    "goal":"test",
                    "step_count":2,
                    "steps":{"items":[{"id":"s1","action":"echo"}]}
                }
            }),
            timestamp: Utc::now(),
        };

        let projected = project_event(&event).expect("projected");
        match projected {
            UiEvent::PlanningCompleted {
                interaction_id,
                task_id,
                step_count,
                steps,
                ..
            } => {
                assert_eq!(interaction_id.as_deref(), Some("i1"));
                assert_eq!(task_id.as_deref(), Some("t1"));
                assert_eq!(step_count, Some(2));
                assert_eq!(steps.len(), 1);
                assert_eq!(steps[0].action, "echo");
            }
            _ => panic!("expected planning_completed"),
        }
    }

    #[test]
    fn test_project_step_completed_contains_output_summary() {
        let event = Event::SystemTrace {
            thread_id: "thread-1".to_string(),
            level: "info".to_string(),
            payload: json!({
                "category":"execution_progress",
                "phase":"step_completed",
                "task_id":"t1",
                "step_id":"s1",
                "action":"file_write",
                "metadata":{
                    "status":0,
                    "path":"target/out.txt",
                    "bytes":12,
                    "output_preview":"hello world"
                }
            }),
            timestamp: Utc::now(),
        };

        let projected = project_event(&event).expect("projected");
        match projected {
            UiEvent::StepCompleted { output, .. } => {
                assert_eq!(output.status, Some(0));
                assert_eq!(output.path.as_deref(), Some("target/out.txt"));
                assert_eq!(output.bytes, Some(12));
                assert_eq!(output.preview.as_deref(), Some("hello world"));
            }
            _ => panic!("expected step_completed"),
        }
    }

    #[test]
    fn test_sse_frame_has_event_and_data() {
        let evt = UiEvent::TurnQueued {
            thread_id: "thread-1".to_string(),
        };
        let frame = evt.to_sse_frame();
        assert!(frame.contains("event: turn_queued"));
        assert!(frame.contains("data: "));
        assert!(frame.ends_with("\n\n"));
    }
}
