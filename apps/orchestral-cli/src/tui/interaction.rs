//! Focus routing shared by keyboard, command completion and modal selectors.
use super::menu::{self, FileReference, Menu, MenuKind};
use super::state::{UiEffect, UiMsg, UiPhase, UiState};
use unicode_segmentation::UnicodeSegmentation;

enum CompletionQuery<'a> {
    Commands(&'a str),
    Files(&'a str),
}

fn completion_query(state: &UiState) -> Option<CompletionQuery<'_>> {
    if state.completion_dismissed
        || state.menu.is_some()
        || matches!(
            state.phase,
            UiPhase::WaitingInput | UiPhase::WaitingApproval | UiPhase::Cancelling
        )
    {
        return None;
    }
    let before = &state.composer[..state.composer_cursor];
    if before.starts_with('/')
        && !before.starts_with("//")
        && !before.chars().any(char::is_whitespace)
    {
        Some(CompletionQuery::Commands(before))
    } else {
        let start = reference_start(before)?;
        Some(CompletionQuery::Files(&before[start + 1..]))
    }
}

pub(crate) fn file_completion_active(state: &UiState) -> bool {
    matches!(completion_query(state), Some(CompletionQuery::Files(_)))
}

pub(crate) fn completion(state: &UiState) -> Option<Menu> {
    let mut menu = match completion_query(state)? {
        CompletionQuery::Commands(before) => {
            let mut menu = super::services::help(state);
            if before != "/" {
                menu.choices = menu::commands(state).into();
                menu.query = before.to_owned();
            }
            menu
        }
        CompletionQuery::Files(query) => {
            let mut menu = Menu::new(
                MenuKind::Files,
                if state.files.len() >= 50_000 {
                    "Files · first 50,000 paths (index limit)"
                } else if state.files_loaded {
                    "Files · path reference"
                } else {
                    "Files · indexing…"
                },
                Vec::new(),
            );
            menu.choices = state.files.clone();
            menu.query = query.to_owned();
            menu
        }
    };
    menu.selected = state
        .completion_selected
        .min(menu.filtered().len().saturating_sub(1));
    Some(menu)
}

fn reference_start(before: &str) -> Option<usize> {
    let start = before.rfind('@')?;
    if start > 0 && !before[..start].chars().next_back()?.is_whitespace() {
        return None;
    }
    (!before[start..].contains(['\n', '"'])).then_some(start)
}

pub(crate) fn route(state: &mut UiState, msg: &UiMsg) -> Option<Vec<UiEffect>> {
    if let Some(menu) = &mut state.menu {
        match msg {
            UiMsg::Escape | UiMsg::Cancel => {
                state.menu = menu.parent.take().map(|menu| *menu);
                return Some(Vec::new());
            }
            UiMsg::InsertText(text) if text == " " && menu.toggle.is_some() => {
                let action = menu.toggle.clone().expect("toggle checked");
                let return_to = menu.parent.clone();
                state.menu = None;
                return Some(vec![UiEffect::LocalAction { action, return_to }]);
            }
            UiMsg::InsertText(text) => {
                if menu.kind != MenuKind::Detail
                    && menu.query.len().saturating_add(text.len()) <= 4096
                {
                    menu.query.push_str(text);
                }
                menu.selected = 0;
                return Some(Vec::new());
            }
            UiMsg::Backspace => {
                let end = menu
                    .query
                    .grapheme_indices(true)
                    .next_back()
                    .map_or(0, |(i, _)| i);
                menu.query.truncate(end);
                menu.selected = 0;
                return Some(Vec::new());
            }
            UiMsg::History { .. }
            | UiMsg::SelectCandidate { .. }
            | UiMsg::ScrollUp(_)
            | UiMsg::ScrollDown(_) => {
                let (up, distance) = match msg {
                    UiMsg::History { up } | UiMsg::SelectCandidate { up } => (*up, 1),
                    UiMsg::ScrollUp(rows) => (true, *rows),
                    UiMsg::ScrollDown(rows) => (false, *rows),
                    _ => unreachable!(),
                };
                let count = if menu.kind == MenuKind::Detail {
                    let width = state.terminal_size.0.saturating_sub(4).max(1) as usize;
                    let height =
                        (state.terminal_size.1 / 2).clamp(3, 10).saturating_sub(2) as usize;
                    let rows = super::viewport::wrap(
                        "detail",
                        menu.detail
                            .as_deref()
                            .unwrap_or_default()
                            .lines()
                            .map(|line| {
                                ratatui::text::Line::raw(super::text::plain(line).into_owned())
                            })
                            .collect(),
                        width,
                    );
                    rows.len().saturating_sub(height) + 1
                } else {
                    menu.filtered().len()
                };
                menu.selected = if up {
                    menu.selected.saturating_sub(distance)
                } else {
                    menu.selected
                        .saturating_add(distance)
                        .min(count.saturating_sub(1))
                };
                return Some(Vec::new());
            }
            UiMsg::Submit | UiMsg::Complete => {
                let Some(choice) = menu.selected().cloned() else {
                    return Some(Vec::new());
                };
                let kind = menu.kind;
                if let Some(action) = choice.action {
                    let return_to = state.menu.take().map(Box::new);
                    return Some(vec![UiEffect::LocalAction { action, return_to }]);
                }
                state.menu = None;
                return Some(vec![UiEffect::MenuChoice {
                    kind,
                    value: choice.value,
                }]);
            }
            UiMsg::Edit(_)
            | UiMsg::MoveCursorLeft
            | UiMsg::MoveCursorRight
            | UiMsg::MoveCursorStart
            | UiMsg::MoveCursorEnd
            | UiMsg::Delete => return Some(Vec::new()),
            _ => {}
        }
    }
    if matches!(msg, UiMsg::InsertText(_) | UiMsg::Backspace | UiMsg::Delete) {
        state.completion_selected = 0;
        state.completion_dismissed = false;
    }
    let menu = completion(state)?;
    match msg {
        UiMsg::SelectCandidate { up } | UiMsg::History { up } => {
            state.completion_selected = if *up {
                menu.selected.saturating_sub(1)
            } else {
                (menu.selected + 1).min(menu.filtered().len().saturating_sub(1))
            };
            Some(Vec::new())
        }
        UiMsg::Complete | UiMsg::Submit => {
            let choice = menu.selected().cloned()?;
            if menu.kind == MenuKind::Commands {
                if state.composer == "/" {
                    state.composer.clear();
                    state.composer_cursor = 0;
                    if let Some(action) = choice.action {
                        return Some(vec![UiEffect::LocalAction {
                            action,
                            return_to: Some(Box::new(menu)),
                        }]);
                    }
                    if matches!(msg, UiMsg::Submit) {
                        if choice.value == "/quit" {
                            return Some(super::state::update(state, UiMsg::Quit));
                        }
                        return Some(vec![UiEffect::HostCommand {
                            command: choice.value,
                        }]);
                    }
                }
                // Unknown slash commands require explicit Tab selection; Enter preserves the error.
                if matches!(msg, UiMsg::Submit) && state.composer != choice.value {
                    return None;
                }
                state.composer = choice.value;
                state.composer_cursor = state.composer.len();
                if matches!(msg, UiMsg::Submit) {
                    None
                } else {
                    Some(Vec::new())
                }
            } else {
                let start = reference_start(&state.composer[..state.composer_cursor])?;
                let marker = format!("@{}", serde_json::to_string(&choice.value).ok()?);
                state
                    .composer
                    .replace_range(start..state.composer_cursor, &marker);
                state.composer_cursor = start + marker.len();
                state.references.push(FileReference {
                    path: choice.value.into(),
                    root: choice.description.into(),
                    marker,
                });
                Some(Vec::new())
            }
        }
        // Close completion without discarding the draft or interrupting the Agent.
        UiMsg::Escape => {
            state.completion_selected = 0;
            state.completion_dismissed = true;
            Some(Vec::new())
        }
        _ => None,
    }
}
