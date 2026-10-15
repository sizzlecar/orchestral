//! Session-scoped `/loop` command and scheduled task menu for the TUI.

use std::time::Instant;

use orchestral_core::agent_protocol::wire::AgentSessionId;
use orchestral_runtime::tools::{
    format_duration_compact, parse_loop_command, ParsedLoopCommand, SessionScheduleRegistry,
};

use super::menu::{Choice, LocalAction, Menu, MenuKind};
use super::state::{TranscriptEntry, UiEffect, UiState};

const USAGE_HELP: &str = "No scheduled tasks active in this session.\n\n\
Usage:\n  \
/loop <interval> <prompt>     Recurring prompt (e.g. /loop 5m check the build)\n  \
/loop <prompt> every <time>   Trailing interval (e.g. /loop run tests every 10m)\n  \
/loop in <time> <prompt>      One-shot timer (e.g. /loop in 15m check deployment)\n  \
/loop <prompt>                Recurring prompt with default 10m interval\n  \
/loop run <id>                Trigger a scheduled task immediately\n  \
/loop delete <id>             Cancel a scheduled task by ID (e.g. /loop delete 1)\n  \
/loop clear                   Cancel all scheduled tasks in this session\n\n\
You can also ask the Agent in natural language to create, list, or cancel timers.";

pub(crate) fn menu(schedules: &SessionScheduleRegistry, state: &UiState) -> Menu {
    let session_id = AgentSessionId::new(&state.session_id);
    let tasks = schedules.list(&session_id);
    if tasks.is_empty() {
        return super::services::detail("Scheduled tasks · none active · Esc back", USAGE_HELP);
    }
    let mut choices = Vec::new();
    for task in &tasks {
        let preview = task.prompt.split_whitespace().collect::<Vec<_>>().join(" ");
        choices.push(Choice::action(
            format!(
                "{}  {}  (next in {} · ran {}x)  {}",
                task.id,
                task.schedule_spec,
                format_duration_compact(task.remaining_secs),
                task.fire_count,
                preview
            ),
            LocalAction::TriggerScheduledTask(task.id.clone()),
            "Run this scheduled task immediately (or as soon as the current request finishes)",
        ));
        choices.push(Choice::action(
            format!("Cancel {}", task.id),
            LocalAction::CancelScheduledTask(task.id.clone()),
            "Stop and remove this scheduled task from the session",
        ));
    }
    if tasks.len() > 1 {
        choices.push(Choice::action(
            "Cancel all scheduled tasks",
            LocalAction::ClearScheduledTasks,
            "Stop and remove all scheduled tasks in this session",
        ));
    }
    Menu::new(
        MenuKind::Commands,
        format!(
            "Scheduled tasks · {} active · select an action",
            tasks.len()
        ),
        choices,
    )
}

pub(crate) fn route_effect(
    effect: &UiEffect,
    schedules: &SessionScheduleRegistry,
    state: &mut UiState,
) -> Option<Vec<UiEffect>> {
    let session_id = AgentSessionId::new(&state.session_id);
    match effect {
        UiEffect::HostCommand { command }
            if command == "/loop" || command.starts_with("/loop ") =>
        {
            match parse_loop_command(command) {
                Ok(ParsedLoopCommand::List) => {
                    state.menu = Some(menu(schedules, state));
                }
                Ok(ParsedLoopCommand::ClearAll) => {
                    let removed = schedules.clear_session(&session_id);
                    if removed.is_empty() {
                        state.ui_notice =
                            Some("No active scheduled tasks in this session".to_owned());
                    } else {
                        let count = removed.len();
                        state.ui_notice = Some(format!(
                            "Cancelled {count} scheduled task{}",
                            if count == 1 { "" } else { "s" }
                        ));
                    }
                }
                Ok(ParsedLoopCommand::Delete { id }) => {
                    if let Some(removed) = schedules.delete(&session_id, &id) {
                        state.ui_notice = Some(format!(
                            "Cancelled {} ({})",
                            removed.id, removed.schedule_spec
                        ));
                    } else {
                        state.ui_notice =
                            Some(format!("No scheduled task matching '{id}' in this session"));
                    }
                }
                Ok(ParsedLoopCommand::RunNow { id }) => {
                    if let Some(view) = schedules.mark_due_now(&session_id, &id, Instant::now()) {
                        state.ui_notice =
                            Some(format!("Triggered {} ({})", view.id, view.schedule_spec));
                    } else {
                        state.ui_notice =
                            Some(format!("No scheduled task matching '{id}' in this session"));
                    }
                }
                Ok(ParsedLoopCommand::Create {
                    prompt,
                    interval,
                    recurring,
                }) => match schedules.create(&session_id, prompt, interval, recurring, None) {
                    Ok(created) => {
                        state.ui_notice = Some(format!(
                            "Scheduled {} · {} · next in {}",
                            created.id,
                            created.schedule_spec,
                            format_duration_compact(created.remaining_secs)
                        ));
                        state.transcript.push(TranscriptEntry::system(format!(
                            "Scheduled {} ({}): {}",
                            created.id, created.schedule_spec, created.prompt
                        )));
                        state.transcript_revision = state.transcript_revision.wrapping_add(1);
                        state.viewport.changed();
                    }
                    Err(error) => {
                        state.ui_notice = Some(error);
                    }
                },
                Err(error) => {
                    state.ui_notice = Some(error);
                }
            }
            Some(Vec::new())
        }
        UiEffect::MenuChoice {
            kind: MenuKind::Commands,
            value,
        } if value == "/loop" => {
            state.menu = Some(menu(schedules, state));
            Some(Vec::new())
        }
        UiEffect::LocalAction {
            action: LocalAction::TriggerScheduledTask(id),
            ..
        } => {
            state.menu = None;
            if let Some(view) = schedules.mark_due_now(&session_id, id, Instant::now()) {
                state.ui_notice = Some(format!("Triggered {} ({})", view.id, view.schedule_spec));
            } else {
                state.ui_notice = Some(format!("Scheduled task '{id}' is no longer active"));
            }
            Some(Vec::new())
        }
        UiEffect::LocalAction {
            action: LocalAction::CancelScheduledTask(id),
            ..
        } => {
            if let Some(removed) = schedules.delete(&session_id, id) {
                if schedules.count(&session_id) == 0 {
                    state.menu = None;
                } else {
                    state.menu = Some(menu(schedules, state));
                }
                state.ui_notice = Some(format!(
                    "Cancelled {} ({})",
                    removed.id, removed.schedule_spec
                ));
            } else {
                state.menu = None;
                state.ui_notice = Some(format!("Scheduled task '{id}' is no longer active"));
            }
            Some(Vec::new())
        }
        UiEffect::LocalAction {
            action: LocalAction::ClearScheduledTasks,
            ..
        } => {
            state.menu = None;
            let removed = schedules.clear_session(&session_id);
            let count = removed.len();
            state.ui_notice = Some(format!(
                "Cancelled {count} scheduled task{}",
                if count == 1 { "" } else { "s" }
            ));
            Some(Vec::new())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::{update, UiMsg};

    #[test]
    fn slash_loop_creates_lists_triggers_and_cancels_session_tasks() {
        let schedules = SessionScheduleRegistry::new();
        let mut state = UiState::new("session-loop", "model-a");

        // 1. Create a recurring loop via `/loop 5m check build`
        update(
            &mut state,
            UiMsg::InsertText("/loop 5m check build".to_owned()),
        );
        let effects = update(&mut state, UiMsg::Submit);
        assert_eq!(effects.len(), 1);
        assert!(route_effect(&effects[0], &schedules, &mut state).is_some());
        assert_eq!(schedules.count(&AgentSessionId::new("session-loop")), 1);
        assert!(state.ui_notice.as_deref().is_some_and(|notice| notice
            .contains("Scheduled loop-1")
            && notice.contains("every 5m")));

        // 2. Open the `/loop` menu
        update(&mut state, UiMsg::InsertText("/loop".to_owned()));
        let effects = update(&mut state, UiMsg::Submit);
        assert!(route_effect(&effects[0], &schedules, &mut state).is_some());
        let menu = state.menu.as_ref().expect("menu opened");
        assert!(menu.title.contains("1 active"));
        assert_eq!(menu.choices.len(), 2);

        // 3. Cancel via `/loop delete 1`
        state.menu = None;
        update(&mut state, UiMsg::InsertText("/loop delete 1".to_owned()));
        let effects = update(&mut state, UiMsg::Submit);
        assert!(route_effect(&effects[0], &schedules, &mut state).is_some());
        assert_eq!(schedules.count(&AgentSessionId::new("session-loop")), 0);
        assert!(state
            .ui_notice
            .as_deref()
            .is_some_and(|notice| notice.contains("Cancelled loop-1")));
    }
}
