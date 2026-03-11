use tokio::sync::oneshot;

use orchestral_core::store::Event as ChannelEvent;

use crate::runtime::event_projection::UiEvent;
use crate::runtime::protocol::ActivityKind;

pub(super) fn mark_turn_settled(turn_settled_tx: &mut Option<oneshot::Sender<()>>) {
    if let Some(tx) = turn_settled_tx.take() {
        let _ = tx.send(());
    }
}

pub(super) fn event_interaction_id(event: &ChannelEvent) -> Option<&str> {
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

pub(super) fn classify_activity_kind(action: &str) -> ActivityKind {
    match action {
        "file_write" | "edit" | "patch" | "write" => ActivityKind::Edited,
        "http" | "search" | "read_file" | "list_files" | "find" | "grep" => ActivityKind::Explored,
        _ => ActivityKind::Ran,
    }
}

pub(super) fn normalize_step_action_label(step_id: &str, action: Option<String>) -> String {
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

pub(super) fn format_running_label(step_id: &str, action_label: &str) -> String {
    if action_label == step_id {
        format!("Running {}", step_id)
    } else {
        format!("Running {} ({})", step_id, action_label)
    }
}

pub(super) fn channel_event_label(event: &ChannelEvent) -> &'static str {
    match event {
        ChannelEvent::UserInput { .. } => "UserInput",
        ChannelEvent::AssistantOutput { .. } => "AssistantOutput",
        ChannelEvent::Artifact { .. } => "Artifact",
        ChannelEvent::SystemTrace { .. } => "SystemTrace",
        ChannelEvent::ExternalEvent { .. } => "ExternalEvent",
    }
}

pub(super) fn ui_event_label(event: &UiEvent) -> &'static str {
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
        UiEvent::AgentProgress { .. } => "AgentProgress",
        UiEvent::InputRequired { .. } => "InputRequired",
        UiEvent::TaskCompleted => "TaskCompleted",
        UiEvent::TaskFailed { .. } => "TaskFailed",
    }
}

pub(super) fn log_preview(text: &str, max_chars: usize) -> String {
    text.chars().take(max_chars).collect()
}

pub(super) fn preview_to_activity_lines(preview: &str) -> Vec<String> {
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
