//! Session-scoped `/goal` autonomous objective command and menu for the TUI.

use orchestral_core::agent_protocol::wire::AgentSessionId;
use orchestral_runtime::tools::{
    parse_goal_command, ParsedGoalCommand, SessionGoalRegistry, SessionGoalStatus,
};

use super::menu::{Choice, LocalAction, Menu, MenuKind};
use super::state::{TranscriptEntry, UiEffect, UiState};

const USAGE_HELP: &str = "No autonomous goal is active in this session.\n\n\
Usage:\n  \
/goal <objective>                 Start an autonomous multi-turn goal (default max 8 turns)\n  \
/goal --max-turns <N> <objective> Start a goal with a custom turn ceiling (1..=50)\n  \
/goal status                      Inspect the current goal status, turn count, and checkpoint note\n  \
/goal pause                       Pause automatic continuation after the current turn\n  \
/goal resume                      Resume a paused or blocked goal for the next turn\n  \
/goal clear                       Clear the goal from this session\n\n\
While a goal is active, Orchestral automatically starts the next turn when a turn completes \
cleanly and the session is idle, stopping as soon as the Agent calls `goal_update(status=\"completed\")`, \
requests your input/approval, or reaches `max_turns`.";

pub(crate) fn sync_badge(goals: &SessionGoalRegistry, state: &mut UiState) {
    let session_id = AgentSessionId::new(&state.session_id);
    state.goal_badge = goals.get(&session_id).and_then(|view| match view.status {
        SessionGoalStatus::Active => Some(format!("goal {}/{}", view.turns_used, view.max_turns)),
        SessionGoalStatus::Paused => Some(format!(
            "goal paused {}/{}",
            view.turns_used, view.max_turns
        )),
        SessionGoalStatus::Blocked => Some(format!(
            "goal blocked {}/{}",
            view.turns_used, view.max_turns
        )),
        SessionGoalStatus::Completed => None,
    });
}

pub(crate) fn menu(goals: &SessionGoalRegistry, state: &UiState) -> Menu {
    let session_id = AgentSessionId::new(&state.session_id);
    let Some(goal) = goals.get(&session_id) else {
        return super::services::detail("Session goal · none active · Esc back", USAGE_HELP);
    };
    let preview = goal
        .objective
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let note_desc = goal
        .progress_note
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .map(|s| format!("Checkpoint: {s}"))
        .unwrap_or_else(|| format!("Objective: {preview}"));

    let mut choices = Vec::new();
    match goal.status {
        SessionGoalStatus::Active => {
            choices.push(Choice::action(
                format!(
                    "Pause goal ({}/{}) · {}",
                    goal.turns_used, goal.max_turns, preview
                ),
                LocalAction::PauseGoal,
                note_desc.clone(),
            ));
        }
        SessionGoalStatus::Paused | SessionGoalStatus::Blocked => {
            choices.push(Choice::action(
                format!(
                    "Resume goal ({}/{}) · {}",
                    goal.turns_used, goal.max_turns, preview
                ),
                LocalAction::ResumeGoal,
                format!("Continue autonomous execution · {note_desc}"),
            ));
        }
        SessionGoalStatus::Completed => {
            choices.push(Choice::action(
                format!(
                    "Completed ({}/{}) · {} (select to clear)",
                    goal.turns_used, goal.max_turns, preview
                ),
                LocalAction::ClearGoal,
                note_desc.clone(),
            ));
        }
    }
    choices.push(Choice::action(
        "Clear session goal",
        LocalAction::ClearGoal,
        "Remove the current goal from this session",
    ));
    Menu::new(
        MenuKind::Commands,
        format!(
            "Session goal · {} ({}/{}) · select an action",
            goal.status.as_str(),
            goal.turns_used,
            goal.max_turns
        ),
        choices,
    )
}

pub(crate) fn route_effect(
    effect: &UiEffect,
    goals: &SessionGoalRegistry,
    state: &mut UiState,
) -> Option<Vec<UiEffect>> {
    let session_id = AgentSessionId::new(&state.session_id);
    match effect {
        UiEffect::HostCommand { command }
            if command == "/goal" || command.starts_with("/goal ") =>
        {
            let mut out = Vec::new();
            match parse_goal_command(command) {
                Ok(ParsedGoalCommand::Status) => {
                    state.menu = Some(menu(goals, state));
                }
                Ok(ParsedGoalCommand::Pause) => {
                    if let Some(paused) = goals.pause(&session_id) {
                        state.ui_notice = Some(format!(
                            "Paused goal ({}/{}): {}",
                            paused.turns_used, paused.max_turns, paused.objective
                        ));
                    } else {
                        state.ui_notice = Some("No active goal in this session".to_owned());
                    }
                }
                Ok(ParsedGoalCommand::Resume) => match goals.resume_next_turn(&session_id) {
                    Ok(resumed) => {
                        state.ui_notice = Some(format!(
                            "Resumed goal ({}/{}): {}",
                            resumed.turns_used, resumed.max_turns, resumed.objective
                        ));
                        if state.run_id.is_none() && state.phase.accepts_new_run() {
                            out.push(UiEffect::StartRun {
                                input: resumed.continuation_prompt(),
                            });
                        }
                    }
                    Err(error) => {
                        state.ui_notice = Some(error);
                    }
                },
                Ok(ParsedGoalCommand::Clear) => {
                    if let Some(removed) = goals.clear(&session_id) {
                        state.ui_notice = Some(format!("Cleared goal: {}", removed.objective));
                    } else {
                        state.ui_notice = Some("No active goal in this session".to_owned());
                    }
                }
                Ok(ParsedGoalCommand::Start {
                    objective,
                    max_turns,
                }) => {
                    if state.run_id.is_some() || !state.phase.accepts_new_run() {
                        state.ui_notice = Some(
                            "Wait for the current Run to finish before starting a new /goal"
                                .to_owned(),
                        );
                    } else {
                        match goals.start(&session_id, objective, max_turns) {
                            Ok(started) => {
                                state.ui_notice = Some(format!(
                                    "Goal started (turn 1/{}): {}",
                                    started.max_turns, started.objective
                                ));
                                state.transcript.push(TranscriptEntry::system(format!(
                                    "Goal mode started (max {} turns): {}",
                                    started.max_turns, started.objective
                                )));
                                state.transcript_revision =
                                    state.transcript_revision.wrapping_add(1);
                                state.viewport.changed();
                                out.push(UiEffect::StartRun {
                                    input: started.initial_prompt(),
                                });
                            }
                            Err(error) => {
                                state.ui_notice = Some(error);
                            }
                        }
                    }
                }
                Err(error) => {
                    state.ui_notice = Some(error);
                }
            }
            sync_badge(goals, state);
            Some(out)
        }
        UiEffect::MenuChoice {
            kind: MenuKind::Commands,
            value,
        } if value == "/goal" => {
            state.menu = Some(menu(goals, state));
            Some(Vec::new())
        }
        UiEffect::LocalAction {
            action: LocalAction::PauseGoal,
            ..
        } => {
            state.menu = None;
            if let Some(paused) = goals.pause(&session_id) {
                state.ui_notice = Some(format!(
                    "Paused goal ({}/{}): {}",
                    paused.turns_used, paused.max_turns, paused.objective
                ));
            }
            sync_badge(goals, state);
            Some(Vec::new())
        }
        UiEffect::LocalAction {
            action: LocalAction::ResumeGoal,
            ..
        } => {
            state.menu = None;
            let mut out = Vec::new();
            match goals.resume_next_turn(&session_id) {
                Ok(resumed) => {
                    state.ui_notice = Some(format!(
                        "Resumed goal ({}/{}): {}",
                        resumed.turns_used, resumed.max_turns, resumed.objective
                    ));
                    if state.run_id.is_none() && state.phase.accepts_new_run() {
                        out.push(UiEffect::StartRun {
                            input: resumed.continuation_prompt(),
                        });
                    }
                }
                Err(error) => {
                    state.ui_notice = Some(error);
                }
            }
            sync_badge(goals, state);
            Some(out)
        }
        UiEffect::LocalAction {
            action: LocalAction::ClearGoal,
            ..
        } => {
            state.menu = None;
            if let Some(removed) = goals.clear(&session_id) {
                state.ui_notice = Some(format!("Cleared goal: {}", removed.objective));
            } else {
                state.ui_notice = Some("No active goal in this session".to_owned());
            }
            sync_badge(goals, state);
            Some(Vec::new())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn goal_command_starts_pauses_resumes_and_clears() {
        let goals = SessionGoalRegistry::new();
        let mut state = UiState::new("session-1", "test-model");

        let effects = route_effect(
            &UiEffect::HostCommand {
                command: "/goal --max-turns 4 build and test feature".to_owned(),
            },
            &goals,
            &mut state,
        )
        .unwrap();
        assert_eq!(effects.len(), 1);
        assert_eq!(state.goal_badge.as_deref(), Some("goal 1/4"));

        route_effect(
            &UiEffect::HostCommand {
                command: "/goal pause".to_owned(),
            },
            &goals,
            &mut state,
        )
        .unwrap();
        assert_eq!(state.goal_badge.as_deref(), Some("goal paused 1/4"));

        let resume_effects = route_effect(
            &UiEffect::HostCommand {
                command: "/goal resume".to_owned(),
            },
            &goals,
            &mut state,
        )
        .unwrap();
        assert_eq!(resume_effects.len(), 1);
        assert_eq!(state.goal_badge.as_deref(), Some("goal 2/4"));

        route_effect(
            &UiEffect::HostCommand {
                command: "/goal clear".to_owned(),
            },
            &goals,
            &mut state,
        )
        .unwrap();
        assert!(state.goal_badge.is_none());
    }
}
