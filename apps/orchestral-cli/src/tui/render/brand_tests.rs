//! Render the real projection and exercise its controls at constrained sizes.
use ratatui::backend::TestBackend;
use ratatui::style::Color;
use ratatui::Terminal;

use super::render;
use super::tests::render_to_string;
use crate::tui::menu::{Choice, Menu, MenuKind};
use crate::tui::{update, ApprovalChoice, UiEffect, UiMsg, UiPhase, UiState};

#[test]
fn minimum_terminal_approval_keeps_operation_and_each_choice_visible() {
    let mut state = UiState::new("session", "local-model");
    update(
        &mut state,
        UiMsg::RunStarted {
            run_id: "run".into(),
        },
    );
    update(
        &mut state,
        UiMsg::WaitingApproval {
            run_id: "run".into(),
            request_id: "approval".into(),
            summary: "Write src/lib.rs".into(),
            session_approval_available: true,
        },
    );
    update(&mut state, UiMsg::SelectApproval(ApprovalChoice::Deny));
    let text = render_to_string(&state, 20, 6);
    for visible in [
        "approval",
        "Write src/lib.rs",
        "a  Allow once",
        "s  Session",
        "› d  Deny",
    ] {
        assert!(text.contains(visible), "{text}");
    }
    let selected = state.approval_choice;
    assert!(
        matches!(update(&mut state, UiMsg::Approval(selected)).as_slice(),
        [UiEffect::ResolveApproval { request_id, choice: ApprovalChoice::Deny, .. }]
        if request_id == "approval")
    );
    assert_eq!(state.phase, UiPhase::WaitingApproval);
}

#[test]
fn minimum_terminal_keeps_question_answer_and_cursor_available() {
    let mut state = UiState::new("session", "local-model");
    update(
        &mut state,
        UiMsg::RunStarted {
            run_id: "run".into(),
        },
    );
    update(
        &mut state,
        UiMsg::WaitingInput {
            run_id: "run".into(),
            request_id: "question".into(),
            prompt: "Which package should receive the change?".into(),
        },
    );
    update(&mut state, UiMsg::InsertText("runtime".into()));

    let mut terminal = Terminal::new(TestBackend::new(20, 6)).unwrap();
    terminal
        .draw(|frame| {
            render(frame, &state);
        })
        .unwrap();
    let text = render_to_string(&state, 20, 6);
    assert!(text.contains("Which package"), "{text}");
    assert!(text.contains("› runtime"), "{text}");
    assert!(text.contains("enter · ^C stop"), "{text}");
    let cursor = terminal.backend().cursor_position();
    assert!(cursor.y > 0 && cursor.y < 5);
    assert_eq!(
        terminal.backend().buffer()[(cursor.x - 1, cursor.y)].symbol(),
        "e"
    );
    assert!(matches!(update(&mut state, UiMsg::Submit).as_slice(),
        [UiEffect::ResolveInput { request_id, value, .. }]
        if request_id == "question" && value == "runtime"));
    assert_eq!(
        state.phase,
        UiPhase::WaitingInput,
        "Rendering and submit do not invent a resolution"
    );
}

#[test]
fn minimum_terminal_menu_keeps_selected_choice_visible_and_actionable() {
    let mut state = UiState::new("session", "local-model");
    update(
        &mut state,
        UiMsg::RunStarted {
            run_id: "run".into(),
        },
    );
    update(
        &mut state,
        UiMsg::OpenMenu(Menu::new(
            MenuKind::Models,
            "Choose a model",
            vec![
                Choice::new("Model A", "model-a", "First local model"),
                Choice::new("Model B", "model-b", "Second local model"),
            ],
        )),
    );
    update(&mut state, UiMsg::SelectCandidate { up: false });
    let text = render_to_string(&state, 20, 6);
    assert!(text.contains("› Model B"), "{text}");
    assert!(text.lines().next().unwrap().contains("running"), "{text}");
    assert!(text.contains("↑↓ enter · esc"), "{text}");
    assert!(matches!(update(&mut state, UiMsg::Submit).as_slice(),
        [UiEffect::MenuChoice { kind: MenuKind::Models, value }] if value == "model-b"));
}

#[test]
fn narrow_header_preserves_complete_mark_and_phase() {
    for (phase, label) in [
        (UiPhase::Idle, "ready"),
        (UiPhase::Running, "running"),
        (UiPhase::WaitingInput, "input"),
        (UiPhase::WaitingApproval, "approval"),
        (UiPhase::Cancelling, "stopping"),
        (UiPhase::Completed, "replied"),
        (UiPhase::Incomplete, "incomplete"),
        (UiPhase::Failed, "failed"),
        (UiPhase::Cancelled, "cancelled"),
    ] {
        let mut state = UiState::new("session", "model");
        state.phase = phase;
        let text = render_to_string(&state, 20, 6);
        let header = text.lines().next().unwrap();
        assert!(header.contains("_>."), "{header}");
        assert!(header.contains(label), "{header}");
    }
}

#[test]
fn long_model_metadata_does_not_hide_steer_or_cancel_controls() {
    let mut state = UiState::new("session", "provider/".repeat(30));
    update(
        &mut state,
        UiMsg::RunStarted {
            run_id: "run".into(),
        },
    );
    update(
        &mut state,
        UiMsg::StreamDelta {
            delta_id: "delta".into(),
            output_id: "answer".into(),
            order: 1,
            text: "Latest answer".into(),
        },
    );
    for width in [20, 40, 64, 100] {
        let text = render_to_string(&state, width, 12);
        let footer = text.lines().last().unwrap();
        assert!(footer.contains("enter"), "{footer}");
        assert!(footer.contains("stop"), "{footer}");
        assert!(text.contains("Latest answer"), "{text}");
    }
    assert!(matches!(update(&mut state, UiMsg::Cancel).as_slice(),
        [UiEffect::CancelRun { run_id }] if run_id == "run"));
    assert_eq!(state.phase, UiPhase::Cancelling);
    let text = render_to_string(&state, 20, 6);
    assert!(text.contains("Stopping"), "{text}");
    assert!(text.contains("stopping…"), "{text}");
    assert!(update(&mut state, UiMsg::Submit).is_empty());
}

#[test]
fn brand_palette_keeps_conversation_clean_and_panel_boundaries_visible() {
    let mut state = UiState::new("session", "local-model");
    update(&mut state, UiMsg::InsertText("Hello".into()));
    let mut terminal = Terminal::new(TestBackend::new(60, 16)).unwrap();
    terminal
        .draw(|frame| {
            render(frame, &state);
        })
        .unwrap();
    let buffer = terminal.backend().buffer();
    assert_eq!(buffer[(2, 0)].symbol(), "_");
    assert_eq!(buffer[(2, 0)].bg, Color::Rgb(131, 255, 107));
    assert_eq!(buffer[(2, 1)].fg, Color::Rgb(245, 240, 207));
    assert_eq!(buffer[(2, 1)].bg, Color::Rgb(29, 32, 39));
    assert!(buffer
        .content
        .iter()
        .any(|cell| cell.symbol() == "━" && cell.fg == Color::Rgb(85, 91, 107)));
    let cursor = terminal.backend().cursor_position();
    assert_eq!(buffer[(cursor.x - 1, cursor.y)].bg, Color::Rgb(39, 43, 53));
}

#[test]
fn terminal_and_no_color_modes_preserve_readable_user_colors() {
    let mut state = UiState::new("session", "local-model");
    state.theme = "terminal".into();
    let mut terminal = Terminal::new(TestBackend::new(40, 12)).unwrap();
    terminal
        .draw(|frame| {
            render(frame, &state);
        })
        .unwrap();
    let buffer = terminal.backend().buffer();
    assert_eq!(buffer[(39, 0)].bg, Color::Reset);
    assert_eq!(buffer[(39, 11)].bg, Color::Reset);
    assert!(!buffer.content.iter().any(|cell| cell.fg == Color::DarkGray));

    state.color_enabled = false;
    terminal
        .draw(|frame| {
            render(frame, &state);
        })
        .unwrap();
    assert!(terminal
        .backend()
        .buffer()
        .content
        .iter()
        .all(|cell| cell.fg == Color::Reset && cell.bg == Color::Reset));
    assert!(render_to_string(&state, 40, 12).contains("_>."));

    state.color_enabled = true;
    state.theme = "light".into();
    terminal
        .draw(|frame| {
            render(frame, &state);
        })
        .unwrap();
    let mark = &terminal.backend().buffer()[(2, 0)];
    assert_eq!(mark.fg, Color::White);
    assert_eq!(mark.bg, Color::Rgb(36, 124, 72));
}
