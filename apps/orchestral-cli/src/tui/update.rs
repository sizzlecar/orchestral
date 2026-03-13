use crossterm::event::{KeyCode, KeyModifiers};
use std::time::Duration;

use crate::runtime::{ActivityKind, RuntimeMsg, TransientSlot};

use super::app::{ActivityGroup, App, AppMode};
use super::bottom_pane::modal::ModalAction;
use super::protocol::UiMsg;

pub fn update(app: &mut App, msg: UiMsg) {
    match msg {
        UiMsg::Resize(w, h) => {
            app.width = w;
            app.height = h;
            app.set_dirty();
        }
        UiMsg::Paste(text) => {
            let before_len = app.bottom.composer.buffer.len();
            let before_cursor = app.bottom.composer.cursor;
            let inserted = app.bottom.composer.insert_text(&text);
            tracing::debug!(
                mode = ?app.mode,
                paste_len = text.len(),
                before_len,
                before_cursor,
                inserted,
                after_len = app.bottom.composer.buffer.len(),
                after_cursor = app.bottom.composer.cursor,
                "submit_chain: paste_received"
            );
            if inserted {
                app.set_dirty();
            }
        }
        UiMsg::AnimTick => {
            let mut changed = false;
            changed |= app.spinner.tick();
            changed |= app.shimmer.tick();
            if changed {
                app.set_dirty();
            }
        }
        UiMsg::UiTick => {}
        UiMsg::ScrollUp => {
            app.history_scroll_back = app.history_scroll_back.saturating_add(3);
            app.set_dirty();
        }
        UiMsg::ScrollDown => {
            app.history_scroll_back = app.history_scroll_back.saturating_sub(3);
            app.set_dirty();
        }
        UiMsg::Key(key) => {
            if key.code == KeyCode::PageUp {
                app.history_scroll_back = app.history_scroll_back.saturating_add(10);
                app.set_dirty();
                return;
            }
            if key.code == KeyCode::PageDown {
                app.history_scroll_back = app.history_scroll_back.saturating_sub(10);
                app.set_dirty();
                return;
            }
            if key.code == KeyCode::Home {
                app.history_scroll_back = u16::MAX;
                app.set_dirty();
                return;
            }
            if key.code == KeyCode::End {
                app.history_scroll_back = 0;
                app.set_dirty();
                return;
            }
            if key.code == KeyCode::Char('m') && key.modifiers.contains(KeyModifiers::CONTROL) {
                let next = !app.mouse_capture_enabled;
                app.mouse_capture_enabled = next;
                app.queue_set_mouse_capture(next);
                app.transient.insert(
                    TransientSlot::Footer,
                    if next {
                        "Mouse capture ON (trackpad scroll). Ctrl+M to allow text selection/copy."
                            .to_string()
                    } else {
                        "Mouse capture OFF (selection/copy enabled). Ctrl+M to re-enable trackpad scroll."
                            .to_string()
                    },
                );
                app.set_dirty();
                return;
            }

            if key.code == KeyCode::F(1)
                || (key.code == KeyCode::Char('k') && key.modifiers.contains(KeyModifiers::CONTROL))
            {
                app.bottom.toggle_help_modal();
                app.set_dirty();
                return;
            }

            if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
                app.queue_interrupt();
                app.transient
                    .insert(TransientSlot::Status, "Interrupting...".to_string());
                app.transient
                    .insert(TransientSlot::Footer, "Interrupt requested...".to_string());
                app.set_dirty();
                return;
            }

            if key.code == KeyCode::Esc {
                if app.bottom.top_modal().is_some() {
                    app.bottom.modals.pop();
                    app.set_dirty();
                    return;
                }
                app.should_quit = true;
                app.set_dirty();
                return;
            }

            if let Some((action, modal_prefix)) = app.bottom.handle_key_modal(key) {
                tracing::debug!(
                    action = ?action,
                    has_prefix = modal_prefix.is_some(),
                    "tui update: modal action received"
                );
                match action {
                    ModalAction::SubmitApprove => {
                        update(app, UiMsg::SubmitInput("/approve".to_string()));
                    }
                    ModalAction::SubmitDeny => {
                        update(app, UiMsg::SubmitInput("/deny".to_string()));
                    }
                    ModalAction::ApproveAndRemember => {
                        if let Some(prefix) = modal_prefix {
                            app.remember_approved_prefix(prefix.clone());
                            push_history_line(
                                app,
                                format!("Approval rule added for this session: `{}`", prefix),
                            );
                        }
                        update(app, UiMsg::SubmitInput("/approve".to_string()));
                    }
                    ModalAction::Close | ModalAction::None => {
                        app.set_dirty();
                    }
                }
                return;
            }

            if key.code == KeyCode::Enter {
                let buffer_len = app.bottom.composer.buffer.len();
                let cursor = app.bottom.composer.cursor;
                let mode_before = app.mode;
                let input = app.bottom.composer.take_buffer();
                tracing::debug!(
                    mode = ?mode_before,
                    buffer_len,
                    cursor,
                    trimmed_len = input.len(),
                    trimmed_preview = %log_preview(&input, 80),
                    "submit_chain: enter_pressed"
                );
                if !input.is_empty() {
                    tracing::debug!(input_len = input.len(), "tui update: submit from Enter");
                    update(app, UiMsg::SubmitInput(input));
                } else {
                    app.transient.insert(
                        TransientSlot::Status,
                        "Input is empty. Type a message before submitting.".to_string(),
                    );
                    app.set_dirty();
                }
                return;
            }

            if app.bottom.handle_key_composer(key) {
                app.set_dirty();
            }
        }
        UiMsg::SubmitInput(input) => {
            let next_turn_id = app.current_turn_id.saturating_add(1);
            tracing::debug!(
                turn_id = next_turn_id,
                mode_before = ?app.mode,
                input_len = input.len(),
                input_preview = %log_preview(&input, 80),
                "submit_chain: submit_input_queued"
            );
            app.current_turn_id = app.current_turn_id.saturating_add(1);
            app.queue_submit(input.clone());
            push_history_line(app, format!("> {}", input));
            prune_old_activities(app, 8);
            app.active_activity = None;
            app.turn_started_at = Some(std::time::Instant::now());
            app.turn_elapsed_reported = false;
            app.assistant_stream_line = None;
            app.assistant_output_persisted = false;
            app.assistant_last_persist_range = None;
            app.history_scroll_back = 0;
            app.mode = AppMode::Planning;
            app.spinner.enabled = true;
            app.shimmer.enabled = true;
            app.transient
                .insert(TransientSlot::Status, "Planning...".to_string());
            app.transient.insert(
                TransientSlot::Footer,
                "Working... Ctrl+C to interrupt".to_string(),
            );
            tracing::debug!(
                turn_id = app.current_turn_id,
                history_len = app.history.len(),
                mode = ?app.mode,
                "submit_chain: submit_input_state_applied"
            );
            app.set_dirty();
        }
        UiMsg::Runtime(msg) => handle_runtime(app, msg),
        UiMsg::Interrupt => {
            app.queue_interrupt();
            app.set_dirty();
        }
        UiMsg::Quit => {
            app.should_quit = true;
            app.set_dirty();
        }
    }
}

fn handle_runtime(app: &mut App, msg: RuntimeMsg) {
    match msg {
        RuntimeMsg::PlanningStart => {
            app.mode = AppMode::Planning;
            app.spinner.enabled = true;
            app.shimmer.enabled = true;
            app.transient
                .insert(TransientSlot::Status, "Planning...".to_string());
            app.set_dirty();
        }
        RuntimeMsg::PlanningEnd => {
            app.mode = AppMode::Executing;
            app.shimmer.enabled = true;
            app.transient
                .insert(TransientSlot::Status, "Executing...".to_string());
            app.set_dirty();
        }
        RuntimeMsg::ExecutionStart { total, .. } => {
            let _ = total;
            app.mode = AppMode::Executing;
            app.spinner.enabled = true;
            app.shimmer.enabled = true;
            app.transient
                .insert(TransientSlot::Status, "Executing...".to_string());
            app.set_dirty();
        }
        RuntimeMsg::ExecutionProgress { step } => {
            let _ = step;
            app.set_dirty();
        }
        RuntimeMsg::ExecutionEnd => {
            tracing::debug!(mode = ?app.mode, "tui update: ExecutionEnd received");
            if matches!(app.mode, AppMode::Planning) {
                tracing::debug!("tui update: ExecutionEnd ignored (mode=Planning)");
                app.set_dirty();
                return;
            }
            enter_waiting_input(app);
            app.submitted_once = true;
            if app.once {
                app.should_quit = true;
            }
            app.set_dirty();
        }
        RuntimeMsg::OutputPersist(line) => {
            let line_preview = log_preview(&line, 80);
            tracing::debug!(
                mode = ?app.mode,
                line_len = line.len(),
                line_preview = %line_preview,
                "tui update: OutputPersist received"
            );
            if matches!(app.mode, AppMode::Planning | AppMode::Executing) {
                app.history_scroll_back = 0;
            }
            if let Some(stream_idx) = app.assistant_stream_line.take() {
                if stream_idx < app.history.len() {
                    let streamed = app.history.remove(stream_idx);
                    if app.history_scroll_back > 0 {
                        app.history_scroll_back = app.history_scroll_back.saturating_sub(1);
                    }
                    tracing::debug!(
                        stream_len = streamed.len(),
                        stream_preview = %log_preview(&streamed, 80),
                        same_text = is_same_reply_text(&streamed, &line),
                        "tui update: removed streamed line before OutputPersist"
                    );
                }
            }
            if let Some(status) = parse_execution_status(&line) {
                tracing::debug!(status = %status, "tui update: OutputPersist is execution status");
                if status != "completed" {
                    enter_waiting_input(app);
                }
                app.set_dirty();
                return;
            }
            if line.trim_start().starts_with("Waiting:") {
                push_history_multiline(app, &line);
                enter_waiting_input(app);
                app.set_dirty();
                return;
            }
            if is_noise_line(&line) {
                tracing::debug!("tui update: OutputPersist filtered as noise");
                app.set_dirty();
                return;
            }
            let should_replace_previous_assistant = app.assistant_output_persisted
                && app.assistant_stream_line.is_none()
                && !matches!(app.mode, AppMode::WaitingInput);
            if should_replace_previous_assistant {
                if let Some((start, end)) = app.assistant_last_persist_range.take() {
                    if start < end && end <= app.history.len() {
                        let removed = (end - start) as u16;
                        app.history.drain(start..end);
                        if app.history_scroll_back > 0 {
                            app.history_scroll_back =
                                app.history_scroll_back.saturating_sub(removed);
                        }
                        tracing::debug!(
                            start,
                            end,
                            removed,
                            "tui update: replaced previous assistant output block"
                        );
                    }
                }
            }
            let history_len_before = app.history.len();
            push_history_multiline(app, &line);
            app.assistant_output_persisted = true;
            if app.history.len() > history_len_before {
                app.assistant_last_persist_range = Some((history_len_before, app.history.len()));
            } else if !should_replace_previous_assistant {
                app.assistant_last_persist_range = None;
            }
            tracing::debug!(
                inserted = app.history.len() != history_len_before,
                history_len = app.history.len(),
                "tui update: OutputPersist pushed to history"
            );
            app.set_dirty();
        }
        RuntimeMsg::AssistantDelta { chunk, done } => {
            if app.assistant_output_persisted {
                tracing::debug!(
                    chunk_len = chunk.len(),
                    done,
                    "tui update: AssistantDelta ignored after OutputPersist"
                );
                return;
            }
            // Stream end marker can arrive with empty chunk after final text has already
            // been persisted; avoid inserting a blank history line in that case.
            if chunk.is_empty() {
                if done {
                    app.set_dirty();
                }
                return;
            }
            if matches!(app.mode, AppMode::Planning | AppMode::Executing) {
                app.history_scroll_back = 0;
            }
            if let Some(idx) = app.assistant_stream_line {
                if let Some(line) = app.history.get_mut(idx) {
                    line.push_str(&chunk);
                }
            } else {
                push_history_line(app, String::new());
                let idx = app.history.len().saturating_sub(1);
                if let Some(line) = app.history.get_mut(idx) {
                    line.push_str(&chunk);
                }
                app.assistant_stream_line = Some(idx);
            }
            let _ = done;
            app.set_dirty();
        }
        RuntimeMsg::OutputTransient { slot, text } => {
            if matches!(slot, TransientSlot::Inline) {
                app.set_dirty();
                return;
            }
            app.transient.insert(slot, text);
            app.set_dirty();
        }
        RuntimeMsg::ApprovalRequested { reason, command } => {
            tracing::debug!(
                mode = ?app.mode,
                reason = %reason,
                command = ?command,
                "tui update: ApprovalRequested received"
            );
            if let Some(cmd) = command.clone() {
                if app.is_command_auto_approved(&cmd) {
                    app.history
                        .push(format!("Auto-approved by rule: `{}`", cmd));
                    update(app, UiMsg::SubmitInput("/approve".to_string()));
                    return;
                }
            }
            app.bottom.open_approval_modal(reason, command);
            app.transient.insert(
                TransientSlot::Footer,
                "Approval required: use Up/Down then Enter".to_string(),
            );
            app.set_dirty();
        }
        RuntimeMsg::ActivityStart {
            kind,
            step_id,
            action,
            input_summary,
        } => {
            let key = make_activity_key(app.current_turn_id, &step_id, &action);
            let idx = upsert_activity_group(app, key, kind, action.clone());
            if let Some(summary) = input_summary.filter(|s| !s.is_empty()) {
                app.activities[idx]
                    .items
                    .push(format!("input: {}", summary.trim()));
            }
            app.active_activity = Some(idx);
            app.shimmer.enabled = true;
            app.set_dirty();
        }
        RuntimeMsg::ActivityItem {
            step_id,
            action,
            line,
        } => {
            let key = make_activity_key(app.current_turn_id, &step_id, &action);
            if let Some(idx) = app.activity_index.get(&key).copied() {
                app.activities[idx].items.push(line);
            }
            app.set_dirty();
        }
        RuntimeMsg::ActivityEnd {
            step_id,
            action,
            failed,
        } => {
            let key = make_activity_key(app.current_turn_id, &step_id, &action);
            if let Some(idx) = app.activity_index.get(&key).copied() {
                if failed {
                    app.activities[idx].failed = true;
                }
                if app.active_activity == Some(idx) {
                    app.active_activity = None;
                    app.shimmer.enabled = app.spinner.enabled;
                }
            }
            app.set_dirty();
        }
        RuntimeMsg::Error(err) => {
            push_history_line(app, format!("Error: {}", err));
            enter_waiting_input(app);
            app.set_dirty();
        }
    }
}

fn enter_waiting_input(app: &mut App) {
    tracing::debug!(
        from_mode = ?app.mode,
        turn_id = app.current_turn_id,
        history_len = app.history.len(),
        "tui update: enter_waiting_input"
    );
    maybe_append_elapsed_line(app);
    app.mode = AppMode::WaitingInput;
    app.spinner.enabled = false;
    app.shimmer.enabled = false;
    app.active_activity = None;
    app.transient
        .insert(TransientSlot::Status, "Idle".to_string());
    app.transient.insert(
        TransientSlot::Footer,
        "Ready. Enter submit | Ctrl+K help | /exit quit".to_string(),
    );
}

fn maybe_append_elapsed_line(app: &mut App) {
    if app.current_turn_id == 0 || app.turn_elapsed_reported {
        return;
    }
    let Some(started_at) = app.turn_started_at else {
        return;
    };

    let elapsed = started_at.elapsed();
    push_history_line(app, format!("Worked for {}", format_elapsed(elapsed)));
    app.turn_elapsed_reported = true;
}

fn format_elapsed(elapsed: Duration) -> String {
    let secs = elapsed.as_secs();
    if secs == 0 {
        return format!("{}ms", elapsed.as_millis().max(1));
    }
    if secs < 60 {
        return format!("{}s", secs);
    }
    let mins = secs / 60;
    let rem_secs = secs % 60;
    if rem_secs == 0 {
        format!("{}m", mins)
    } else {
        format!("{}m {}s", mins, rem_secs)
    }
}

fn push_history_multiline(app: &mut App, text: &str) {
    let mut pushed = false;
    for line in text.lines() {
        if app.history.last().map(String::as_str) != Some(line) {
            push_history_line(app, line.to_string());
        } else {
            tracing::debug!(
                line_len = line.len(),
                line_preview = %log_preview(line, 80),
                "tui update: skip consecutive duplicate line"
            );
        }
        pushed = true;
    }
    if !pushed {
        // For duplicated multi-line replies, every split line can already exist in
        // history. In that case we must not fallback to pushing the full raw text,
        // otherwise the whole reply appears again as a single long wrapped line.
        if text.contains('\n') {
            tracing::debug!(
                line_len = text.len(),
                line_preview = %log_preview(text, 80),
                "tui update: skip fallback for duplicated multiline text"
            );
            return;
        }
        if app.history.last().map(String::as_str) != Some(text) {
            push_history_line(app, text.to_string());
        } else {
            tracing::debug!(
                line_len = text.len(),
                line_preview = %log_preview(text, 80),
                "tui update: skip fallback consecutive duplicate line"
            );
        }
    }
}

fn parse_execution_status(line: &str) -> Option<String> {
    let status = line.strip_prefix("Status:")?.trim().to_ascii_lowercase();
    if status.is_empty() {
        None
    } else {
        Some(status)
    }
}

fn is_noise_line(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.starts_with("Plan:")
        || trimmed == "Final Answer"
        || trimmed.starts_with("Status:")
        || trimmed.ends_with("step(s) queued")
        || trimmed.starts_with("<!doctype html")
        || trimmed.starts_with("<html")
}

fn is_same_reply_text(a: &str, b: &str) -> bool {
    let norm_a = normalize_reply_text(a);
    let norm_b = normalize_reply_text(b);
    if norm_a.is_empty() || norm_b.is_empty() {
        return false;
    }
    norm_a == norm_b || norm_a.contains(&norm_b) || norm_b.contains(&norm_a)
}

fn normalize_reply_text(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn log_preview(line: &str, max_chars: usize) -> String {
    line.chars().take(max_chars).collect()
}

fn make_activity_key(turn_id: usize, step: &str, action: &str) -> String {
    format!("{}::{}::{}", turn_id, step, action)
}

fn upsert_activity_group(app: &mut App, key: String, kind: ActivityKind, title: String) -> usize {
    if let Some(idx) = app.activity_index.get(&key).copied() {
        return idx;
    }
    let idx = app.activities.len();
    app.activities.push(ActivityGroup {
        key: key.clone(),
        turn_id: app.current_turn_id,
        kind,
        title,
        items: Vec::new(),
        failed: false,
    });
    app.activity_index.insert(key, idx);
    idx
}

fn push_history_line(app: &mut App, line: String) {
    // Keep viewport stable while user is scrolled up.
    if app.history_scroll_back > 0 {
        app.history_scroll_back = app.history_scroll_back.saturating_add(1);
    }
    app.history.push(line);
}

fn prune_old_activities(app: &mut App, keep_turns: usize) {
    let min_turn = app
        .current_turn_id
        .saturating_sub(keep_turns.saturating_sub(1));
    app.activities.retain(|g| g.turn_id >= min_turn);
    app.activity_index.clear();
    for (idx, group) in app.activities.iter().enumerate() {
        app.activity_index.insert(group.key.clone(), idx);
    }
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    use super::{log_preview, update};
    use crate::runtime::RuntimeMsg;
    use crate::tui::app::{App, AppMode};
    use crate::tui::protocol::UiMsg;

    #[test]
    fn test_log_preview_handles_multibyte() {
        let line = "你好，世界！".repeat(30);
        let preview = log_preview(&line, 80);
        assert_eq!(preview.chars().count(), 80);
    }

    #[test]
    fn test_log_preview_short_string() {
        assert_eq!(log_preview("hello", 80), "hello");
    }

    #[test]
    fn test_output_persist_dedupes_same_line_in_one_turn() {
        let mut app = App::new(120, 40, false);
        update(&mut app, UiMsg::SubmitInput("hello".to_string()));

        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::OutputPersist("same reply".to_string())),
        );
        let history_len_after_first = app.history.len();

        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::OutputPersist("same reply".to_string())),
        );

        assert_eq!(app.history.len(), history_len_after_first);
        let occurrences = app
            .history
            .iter()
            .filter(|line| line.trim() == "same reply")
            .count();
        assert_eq!(occurrences, 1);
    }

    #[test]
    fn test_assistant_delta_done_without_chunk_does_not_insert_blank_line() {
        let mut app = App::new(120, 40, false);
        update(&mut app, UiMsg::SubmitInput("hello".to_string()));
        let before = app.history.clone();

        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::AssistantDelta {
                chunk: String::new(),
                done: true,
            }),
        );

        assert_eq!(app.history, before);
        assert_eq!(app.assistant_stream_line, None);
    }

    #[test]
    fn test_approval_modal_enter_submits_approve_once() {
        let mut app = App::new(120, 40, false);
        app.mode = AppMode::WaitingInput;

        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::ApprovalRequested {
                reason: "needs approval".to_string(),
                command: Some("rm /tmp/demo.txt".to_string()),
            }),
        );
        assert!(app.bottom.top_modal().is_some());

        update(
            &mut app,
            UiMsg::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
        );

        assert!(app.bottom.top_modal().is_none());
        assert_eq!(app.take_pending_submit(), Some("/approve".to_string()));
        assert_eq!(app.mode, AppMode::Planning);
        assert_eq!(app.history.last().map(String::as_str), Some("> /approve"));
    }

    #[test]
    fn test_approval_modal_remember_prefix_and_submit() {
        let mut app = App::new(120, 40, false);
        app.mode = AppMode::WaitingInput;
        let command = "rm /Users/demo/file.txt".to_string();

        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::ApprovalRequested {
                reason: "needs approval".to_string(),
                command: Some(command.clone()),
            }),
        );
        assert!(app.bottom.top_modal().is_some());

        update(
            &mut app,
            UiMsg::Key(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE)),
        );

        assert!(app.bottom.top_modal().is_none());
        assert_eq!(app.take_pending_submit(), Some("/approve".to_string()));
        assert!(app.is_command_auto_approved(&command));
        assert!(app
            .history
            .iter()
            .any(|line| line.contains("Approval rule added for this session")));
    }

    #[test]
    fn test_stream_then_outputpersist_dedupes_final_reply() {
        let mut app = App::new(120, 40, false);
        update(&mut app, UiMsg::SubmitInput("hello".to_string()));

        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::AssistantDelta {
                chunk: "您好".to_string(),
                done: false,
            }),
        );
        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::AssistantDelta {
                chunk: String::new(),
                done: true,
            }),
        );
        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::OutputPersist("您好".to_string())),
        );

        let occurrences = app
            .history
            .iter()
            .filter(|line| line.trim() == "您好")
            .count();
        assert_eq!(occurrences, 1);
    }

    #[test]
    fn test_outputpersist_before_execution_end_reply_kept_in_history() {
        let mut app = App::new(120, 40, false);
        update(
            &mut app,
            UiMsg::SubmitInput("我第一句话说了什么？".to_string()),
        );
        assert_eq!(app.mode, AppMode::Planning);
        update(&mut app, UiMsg::Runtime(RuntimeMsg::PlanningEnd));
        assert_eq!(app.mode, AppMode::Executing);

        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::OutputPersist(
                "您第一句话说的是：“你好”。".to_string(),
            )),
        );
        update(&mut app, UiMsg::Runtime(RuntimeMsg::ExecutionEnd));

        assert!(app
            .history
            .iter()
            .any(|line| line.contains("您第一句话说的是：“你好”。")));
        assert_eq!(app.mode, AppMode::WaitingInput);
    }

    #[test]
    fn test_outputpersist_after_execution_end_reply_still_rendered() {
        let mut app = App::new(120, 40, false);
        update(&mut app, UiMsg::SubmitInput("late reply test".to_string()));
        update(&mut app, UiMsg::Runtime(RuntimeMsg::PlanningEnd));
        assert_eq!(app.mode, AppMode::Executing);

        update(&mut app, UiMsg::Runtime(RuntimeMsg::ExecutionEnd));
        assert_eq!(app.mode, AppMode::WaitingInput);
        let before = app.history.len();

        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::OutputPersist(
                "late assistant reply".to_string(),
            )),
        );

        assert_eq!(app.mode, AppMode::WaitingInput);
        assert!(app.history.len() > before);
        assert_eq!(
            app.history.last().map(String::as_str),
            Some("late assistant reply")
        );
    }

    #[test]
    fn test_repeated_multiline_outputpersist_does_not_add_full_text_fallback_duplicate() {
        let mut app = App::new(120, 40, false);
        update(&mut app, UiMsg::SubmitInput("fetch and save".to_string()));
        update(&mut app, UiMsg::Runtime(RuntimeMsg::PlanningEnd));

        let reply = "saved response body.\nurl: https://www.google.com\npath: /tmp/google_response.html\nbytes: 54755".to_string();
        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::OutputPersist(reply.clone())),
        );
        let history_after_first = app.history.clone();

        update(&mut app, UiMsg::Runtime(RuntimeMsg::OutputPersist(reply)));

        assert_eq!(app.history, history_after_first);
        assert!(app.history.iter().all(|line| !line.contains('\n')));
    }

    #[test]
    fn test_multiline_stream_then_outputpersist_replaces_stream_line_without_duplicate() {
        let mut app = App::new(120, 40, false);
        update(
            &mut app,
            UiMsg::SubmitInput("save conversation".to_string()),
        );
        update(&mut app, UiMsg::Runtime(RuntimeMsg::PlanningEnd));

        let reply = "saved locally.\npath: /tmp/conversation.txt\nbytes: 723".to_string();
        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::AssistantDelta {
                chunk: reply.clone(),
                done: false,
            }),
        );
        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::AssistantDelta {
                chunk: String::new(),
                done: true,
            }),
        );
        update(&mut app, UiMsg::Runtime(RuntimeMsg::OutputPersist(reply)));

        let matches = app
            .history
            .iter()
            .filter(|line| {
                line.contains("saved locally.") || line.contains("path: /tmp/conversation.txt")
            })
            .count();
        assert_eq!(matches, 2);
        assert!(app.history.iter().all(|line| !line.contains("\npath:")));
    }

    #[test]
    fn test_late_assistant_delta_after_outputpersist_is_ignored() {
        let mut app = App::new(120, 40, false);
        update(
            &mut app,
            UiMsg::SubmitInput("fetch github status".to_string()),
        );
        update(&mut app, UiMsg::Runtime(RuntimeMsg::PlanningEnd));

        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::AssistantDelta {
                chunk: "GitHub status fetched ".to_string(),
                done: false,
            }),
        );

        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::OutputPersist(
                "GitHub status fetched and saved locally.".to_string(),
            )),
        );

        let before = app.history.clone();
        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::AssistantDelta {
                chunk: "late streamed tail".to_string(),
                done: false,
            }),
        );
        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::AssistantDelta {
                chunk: String::new(),
                done: true,
            }),
        );

        assert_eq!(app.history, before);
        assert!(app.assistant_output_persisted);
        assert_eq!(app.assistant_stream_line, None);
    }

    #[test]
    fn test_second_outputpersist_before_execution_end_replaces_previous_block() {
        let mut app = App::new(120, 40, false);
        update(&mut app, UiMsg::SubmitInput("save body".to_string()));
        update(&mut app, UiMsg::Runtime(RuntimeMsg::PlanningEnd));

        let first = "已保存到本地。\n路径: /tmp/old.txt\n大小: 100 字节".to_string();
        let final_msg =
            "已成功保存响应体到本地文件。\n路径: /tmp/new.txt\n大小: 120 字节".to_string();
        update(&mut app, UiMsg::Runtime(RuntimeMsg::OutputPersist(first)));
        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::OutputPersist(final_msg.clone())),
        );

        let joined = app.history.join("\n");
        assert!(!joined.contains("/tmp/old.txt"));
        assert!(joined.contains("/tmp/new.txt"));
        assert_eq!(
            app.history
                .iter()
                .filter(|line| line.contains("已成功保存响应体到本地文件。"))
                .count(),
            1
        );
    }

    #[test]
    fn test_stream_provisional_then_final_output_replaces_stream_line() {
        let mut app = App::new(120, 40, false);
        update(
            &mut app,
            UiMsg::SubmitInput("what was my first line".to_string()),
        );
        update(&mut app, UiMsg::Runtime(RuntimeMsg::PlanningEnd));

        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::AssistantDelta {
                chunk: "让我先确认上下文...".to_string(),
                done: false,
            }),
        );
        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::AssistantDelta {
                chunk: String::new(),
                done: true,
            }),
        );
        update(
            &mut app,
            UiMsg::Runtime(RuntimeMsg::OutputPersist(
                "您第一句话说的是：“你好”。".to_string(),
            )),
        );

        let joined = app.history.join("\n");
        assert!(!joined.contains("让我先确认上下文..."));
        assert!(joined.contains("您第一句话说的是：“你好”。"));
    }
}
