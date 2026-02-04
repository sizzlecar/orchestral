use crossterm::event::{KeyCode, KeyModifiers};

use crate::runtime::{ActivityKind, RuntimeMsg, TransientSlot};

use super::app::{ActivityGroup, App, AppMode};
use super::protocol::UiMsg;

pub fn update(app: &mut App, msg: UiMsg) {
    match msg {
        UiMsg::Resize(w, h) => {
            app.width = w;
            app.height = h;
            app.set_dirty();
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
        UiMsg::Key(key) => {
            if key.code == KeyCode::F(1)
                || (key.code == KeyCode::Char('k')
                    && key.modifiers.contains(KeyModifiers::CONTROL))
            {
                app.bottom.toggle_help_modal();
                app.set_dirty();
                return;
            }

            if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
                app.queue_interrupt();
                app.transient
                    .insert(TransientSlot::Status, "Interrupting...".to_string());
                app.transient.insert(
                    TransientSlot::Footer,
                    "Interrupt requested...".to_string(),
                );
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

            if app.bottom.handle_key_modal(key) {
                app.set_dirty();
                return;
            }

            if key.code == KeyCode::Enter {
                let input = app.bottom.composer.take_buffer();
                if !input.is_empty() {
                    update(app, UiMsg::SubmitInput(input));
                }
                return;
            }

            if app.bottom.handle_key_composer(key) {
                app.set_dirty();
            }
        }
        UiMsg::SubmitInput(input) => {
            app.current_turn_id = app.current_turn_id.saturating_add(1);
            app.queue_submit(input.clone());
            app.history.push(format!("> {}", input));
            app.active_activity = None;
            app.mode = AppMode::Planning;
            app.spinner.enabled = true;
            app.shimmer.enabled = true;
            app.transient
                .insert(TransientSlot::Status, "Planning...".to_string());
            app.transient
                .insert(TransientSlot::Footer, "Working... Ctrl+C to interrupt".to_string());
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
        RuntimeMsg::ExecutionStart { total } => {
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
            enter_waiting_input(app);
            app.submitted_once = true;
            if app.once {
                app.should_quit = true;
            }
            app.set_dirty();
        }
        RuntimeMsg::OutputPersist(line) => {
            if let Some(status) = parse_execution_status(&line) {
                if status != "completed" {
                    enter_waiting_input(app);
                }
                app.set_dirty();
                return;
            }
            if line.trim_start().starts_with("Waiting:") {
                enter_waiting_input(app);
                app.history.push(line);
                let wait = app.history.last().cloned().unwrap_or_default();
                if wait.contains("requires approval")
                    || wait.contains("/approve")
                    || wait.contains("/deny")
                {
                    app.transient.insert(
                        TransientSlot::Footer,
                        "Approval required: type /approve or /deny".to_string(),
                    );
                }
                app.set_dirty();
                return;
            }
            if is_noise_line(&line) {
                app.set_dirty();
                return;
            }
            if handle_activity_extra_line(app, &line) {
                app.set_dirty();
                return;
            }
            app.history.push(line);
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
        RuntimeMsg::ActivityStart {
            kind,
            step_id,
            action,
            input_summary,
        } => {
            let key = make_activity_key(app.current_turn_id, &step_id, &action);
            let idx = upsert_activity_group(
                app,
                key,
                kind,
                action.clone(),
            );
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
            enter_waiting_input(app);
            app.history.push(format!("Error: {}", err));
            app.set_dirty();
        }
    }
}

fn enter_waiting_input(app: &mut App) {
    app.mode = AppMode::WaitingInput;
    app.spinner.enabled = false;
    app.shimmer.enabled = false;
    app.active_activity = None;
    app.transient.insert(TransientSlot::Status, "Idle".to_string());
    app.transient.insert(
        TransientSlot::Footer,
        "Ready. Enter submit | Ctrl+K help | /exit quit".to_string(),
    );
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

fn make_activity_key(turn_id: usize, step: &str, action: &str) -> String {
    format!("{}::{}::{}", turn_id, step, action)
}

fn handle_activity_extra_line(app: &mut App, line: &str) -> bool {
    let Some(group) = app
        .activities
        .iter_mut()
        .rev()
        .find(|g| g.turn_id == app.current_turn_id)
    else {
        return false;
    };

    if let Some(body) = line.strip_prefix("Echo:") {
        let text = body.trim();
        let detail = if text.is_empty() {
            "output: (empty)".to_string()
        } else {
            format!("output: {}", text)
        };
        group.items.push(detail);
        return true;
    }

    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('<') || trimmed.starts_with('>') {
        return false;
    }
    group.items.push(format!("output: {}", trimmed));
    true
}

fn upsert_activity_group(
    app: &mut App,
    key: String,
    kind: ActivityKind,
    title: String,
) -> usize {
    if let Some(idx) = app.activity_index.get(&key).copied() {
        return idx;
    }
    let idx = app.activities.len();
    app.activities.push(ActivityGroup {
        turn_id: app.current_turn_id,
        kind,
        title,
        items: Vec::new(),
        failed: false,
    });
    app.activity_index.insert(key, idx);
    idx
}
