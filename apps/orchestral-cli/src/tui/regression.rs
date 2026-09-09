use super::{update, UiEffect, UiMsg, UiPhase, UiState};

#[test]
fn pending_question_preserves_draft_and_treats_slash_as_answer() {
    let mut state = UiState::new("session", "model");
    update(
        &mut state,
        UiMsg::RunStarted {
            run_id: "run".to_owned(),
        },
    );
    update(&mut state, UiMsg::InsertText("future guidance".to_owned()));
    let draft = (state.composer.clone(), state.composer_cursor);
    update(
        &mut state,
        UiMsg::WaitingInput {
            run_id: "run".to_owned(),
            request_id: "question".to_owned(),
            prompt: "Which path?".to_owned(),
        },
    );
    update(&mut state, UiMsg::InsertText("/a/real/path".to_owned()));
    update(&mut state, UiMsg::ToggleHelp);
    assert!(update(&mut state, UiMsg::Escape).is_empty());
    assert_eq!(state.phase, UiPhase::WaitingInput);
    let effects = update(&mut state, UiMsg::Submit);
    assert!(
        matches!(effects.as_slice(), [UiEffect::ResolveInput { value, .. }] if value == "/a/real/path")
    );
    assert_eq!(state.phase, UiPhase::WaitingInput);
    assert!(state.composer.is_empty());
    update(
        &mut state,
        UiMsg::RequestSubmissionAccepted {
            request_id: "question".to_owned(),
        },
    );
    update(
        &mut state,
        UiMsg::RequestResolved {
            request_id: "question".to_owned(),
        },
    );
    assert_eq!((state.composer.clone(), state.composer_cursor), draft);
    assert!(state.transcript.last().unwrap().continuation);
}

#[test]
fn submitted_answers_ignore_stale_requests_and_extra_enter_preserves_the_draft() {
    // Include a slash command: restored drafts must not bypass the guard through completion.
    for draft in ["future guidance", "/new"] {
        let mut state = UiState::new("session", "model");
        update(
            &mut state,
            UiMsg::RunStarted {
                run_id: "run".to_owned(),
            },
        );
        update(&mut state, UiMsg::InsertText(draft.to_owned()));
        let question = || UiMsg::WaitingInput {
            run_id: "run".to_owned(),
            request_id: "question".to_owned(),
            prompt: "Choose".to_owned(),
        };
        update(&mut state, question());
        update(&mut state, UiMsg::InsertText("answer".to_owned()));
        assert!(matches!(
            update(&mut state, UiMsg::Submit).as_slice(),
            [UiEffect::ResolveInput { .. }]
        ));
        for _ in 0..2 {
            update(&mut state, question());
            assert!(update(&mut state, UiMsg::Submit).is_empty());
            update(
                &mut state,
                UiMsg::RequestSubmissionAccepted {
                    request_id: "question".to_owned(),
                },
            );
        }
        assert_eq!(
            state
                .transcript
                .iter()
                .filter(|entry| entry.text == "answer")
                .count(),
            1
        );
        update(
            &mut state,
            UiMsg::RequestResolved {
                request_id: "question".to_owned(),
            },
        );
        update(&mut state, question());
        assert_eq!(state.phase, UiPhase::Running);
        assert!(state.pending.is_none());
        for _ in 0..2 {
            assert!(update(&mut state, UiMsg::Submit).is_empty());
            assert_eq!(state.composer, draft);
        }
        update(&mut state, UiMsg::MoveCursorEnd);
        assert!(!update(&mut state, UiMsg::Submit).is_empty());
    }
}

#[test]
fn rejected_answer_remains_editable_and_only_the_accepted_retry_enters_history() {
    let mut state = UiState::new("session", "model");
    update(
        &mut state,
        UiMsg::RunStarted {
            run_id: "run".to_owned(),
        },
    );
    update(&mut state, UiMsg::InsertText("draft".to_owned()));
    update(
        &mut state,
        UiMsg::WaitingInput {
            run_id: "run".to_owned(),
            request_id: "question".to_owned(),
            prompt: "Choose".to_owned(),
        },
    );
    update(&mut state, UiMsg::InsertText("answer".to_owned()));
    update(&mut state, UiMsg::Submit);
    update(
        &mut state,
        UiMsg::RequestSubmissionFailed {
            request_id: "question".to_owned(),
        },
    );
    assert_eq!(state.phase, UiPhase::WaitingInput);
    assert_eq!(state.composer, "answer");
    assert!(state.transcript.is_empty());
    update(&mut state, UiMsg::InsertText(" corrected".to_owned()));
    assert!(
        matches!(update(&mut state, UiMsg::Submit).as_slice(), [UiEffect::ResolveInput { value, .. }] if value == "answer corrected")
    );
    // Durable resolution is sufficient even if it arrives before an acknowledgement.
    update(
        &mut state,
        UiMsg::RequestResolved {
            request_id: "question".to_owned(),
        },
    );
    update(
        &mut state,
        UiMsg::RequestSubmissionAccepted {
            request_id: "question".to_owned(),
        },
    );
    assert_eq!(state.transcript.len(), 1);
    assert_eq!(state.transcript[0].text, "answer corrected");
    assert_eq!(state.composer, "draft");
}

#[test]
fn approvals_require_one_submission_and_a_rejection_allows_explicit_retry() {
    use super::ApprovalChoice;
    let mut state = UiState::new("session", "model");
    let approval = || UiMsg::WaitingApproval {
        run_id: "run".to_owned(),
        request_id: "approval".to_owned(),
        summary: "Run command".to_owned(),
        session_approval_available: false,
    };
    update(&mut state, approval());
    assert!(matches!(
        update(&mut state, UiMsg::Approval(ApprovalChoice::Allow)).as_slice(),
        [UiEffect::ResolveApproval { .. }]
    ));
    update(&mut state, approval());
    assert!(update(&mut state, UiMsg::Approval(ApprovalChoice::Allow)).is_empty());
    update(
        &mut state,
        UiMsg::RequestSubmissionFailed {
            request_id: "approval".to_owned(),
        },
    );
    assert!(matches!(
        update(&mut state, UiMsg::Approval(ApprovalChoice::Deny)).as_slice(),
        [UiEffect::ResolveApproval {
            choice: ApprovalChoice::Deny,
            ..
        }]
    ));
    update(
        &mut state,
        UiMsg::RequestSubmissionAccepted {
            request_id: "approval".to_owned(),
        },
    );
    update(&mut state, approval());
    assert!(update(&mut state, UiMsg::Approval(ApprovalChoice::Deny)).is_empty());
    update(
        &mut state,
        UiMsg::RequestResolved {
            request_id: "approval".to_owned(),
        },
    );
    update(&mut state, approval());
    assert_eq!(state.phase, UiPhase::Running);
    assert!(state.pending.is_none());
}

#[test]
fn refreshing_files_preserves_the_selected_path_when_earlier_entries_are_added() {
    use super::menu::Choice;
    let mut state = UiState::new("session", "model");
    let choice = |path| Choice::new(path, path, "/workspace");
    update(
        &mut state,
        UiMsg::FilesLoaded(vec![choice("b.rs"), choice("c.rs")]),
    );
    update(&mut state, UiMsg::InsertText("@".to_owned()));
    update(&mut state, UiMsg::SelectCandidate { up: false });
    update(
        &mut state,
        UiMsg::FilesLoaded(vec![choice("a.rs"), choice("b.rs"), choice("c.rs")]),
    );
    let menu = super::interaction::completion(&state).unwrap();
    assert_eq!(menu.selected().unwrap().value, "c.rs");
}

#[test]
fn cancellation_restores_question_draft_and_does_not_repeat_control() {
    let mut state = UiState::new("session", "model");
    update(&mut state, UiMsg::InsertText("unsent draft".to_owned()));
    update(
        &mut state,
        UiMsg::WaitingInput {
            run_id: "run".to_owned(),
            request_id: "question".to_owned(),
            prompt: "Choose".to_owned(),
        },
    );
    update(
        &mut state,
        UiMsg::InsertText("unfinished answer".to_owned()),
    );
    assert!(matches!(
        update(&mut state, UiMsg::Cancel).as_slice(),
        [UiEffect::CancelRun { .. }]
    ));
    assert_eq!(
        state.menu.as_ref().unwrap().detail.as_deref(),
        Some("unfinished answer")
    );
    assert!(update(&mut state, UiMsg::Cancel).is_empty());
    update(
        &mut state,
        UiMsg::Cancelled {
            reason: "user stopped".to_owned(),
        },
    );
    assert_eq!(state.composer, "unsent draft");
}

#[test]
fn inserting_joiners_keeps_the_cursor_on_grapheme_boundaries() {
    let mut state = UiState::new("session", "model");
    update(&mut state, UiMsg::InsertText("👩💻".to_owned()));
    update(&mut state, UiMsg::MoveCursorLeft);
    update(&mut state, UiMsg::InsertText("\u{200d}".to_owned()));
    assert_eq!(state.composer_cursor, state.composer.len());
    update(&mut state, UiMsg::Backspace);
    assert!(state.composer.is_empty());
}

#[test]
fn long_input_rejects_over_limit_without_losing_draft() {
    let mut state = UiState::new("session", "model");
    update(&mut state, UiMsg::InsertText("draft".to_owned()));
    update(&mut state, UiMsg::InsertText("x".repeat(1024 * 1024)));
    assert_eq!(state.composer, "draft");
    assert!(state.ui_notice.as_deref().unwrap().contains("1 MiB"));
}
