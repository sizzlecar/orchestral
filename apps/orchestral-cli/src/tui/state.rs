use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use orchestral_core::agent_protocol::wire::ToolActivityState;

use super::activity::{ActivityProjection, ActivityReducer, ActivityStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UiPhase {
    Idle,
    Running,
    WaitingInput,
    WaitingApproval,
    Cancelling,
    Completed,
    Failed,
    Cancelled,
}

impl UiPhase {
    fn accepts_new_run(self) -> bool {
        matches!(
            self,
            Self::Idle | Self::Completed | Self::Failed | Self::Cancelled
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TranscriptRole {
    User,
    Assistant,
    Tool,
    System,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TranscriptEntry {
    pub id: Option<String>,
    pub role: TranscriptRole,
    pub text: String,
    pub tool_status: Option<ActivityStatus>,
}

impl TranscriptEntry {
    pub(crate) fn user(text: impl Into<String>) -> Self {
        Self {
            id: None,
            role: TranscriptRole::User,
            text: text.into(),
            tool_status: None,
        }
    }

    pub(crate) fn assistant(id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            id: Some(id.into()),
            role: TranscriptRole::Assistant,
            text: text.into(),
            tool_status: None,
        }
    }

    pub(crate) fn system(text: impl Into<String>) -> Self {
        Self {
            id: None,
            role: TranscriptRole::System,
            text: text.into(),
            tool_status: None,
        }
    }

    pub(crate) fn error(id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            id: Some(id.into()),
            role: TranscriptRole::Error,
            text: text.into(),
            tool_status: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PendingOverlay {
    Input { request_id: String, prompt: String },
    Approval { request_id: String, summary: String },
}

impl PendingOverlay {
    pub(crate) fn request_id(&self) -> &str {
        match self {
            Self::Input { request_id, .. } | Self::Approval { request_id, .. } => request_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApprovalChoice {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UiEffect {
    StartRun {
        input: String,
    },
    Steer {
        run_id: String,
        input: String,
    },
    ResolveInput {
        run_id: String,
        request_id: String,
        value: String,
    },
    ResolveApproval {
        run_id: String,
        request_id: String,
        choice: ApprovalChoice,
    },
    CancelRun {
        run_id: String,
    },
    Quit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UiMsg {
    InsertText(String),
    Backspace,
    Delete,
    MoveCursorLeft,
    MoveCursorRight,
    MoveCursorStart,
    MoveCursorEnd,
    Submit,
    Approval(ApprovalChoice),
    Cancel,
    Quit,
    ScrollUp(usize),
    ScrollDown(usize),
    Tick {
        now: Instant,
    },
    RunStarted {
        run_id: String,
    },
    StreamDelta {
        delta_id: String,
        output_id: String,
        order: u64,
        text: String,
    },
    OutputCommitted {
        output_id: String,
        text: String,
    },
    ToolActivity {
        activity_id: String,
        tool_name: String,
        state: ToolActivityState,
    },
    ProgressReported {
        summary: String,
    },
    ProcessActivity {
        run_id: String,
        session_id: u64,
        running: bool,
    },
    ProcessInventory {
        run_id: String,
        session_ids: Vec<u64>,
    },
    WaitingInput {
        run_id: String,
        request_id: String,
        prompt: String,
    },
    WaitingApproval {
        run_id: String,
        request_id: String,
        summary: String,
    },
    RequestResolved {
        request_id: String,
    },
    Stopping,
    Notice {
        id: String,
        message: String,
        is_error: bool,
    },
    Completed {
        final_text: Option<String>,
    },
    Failed {
        message: String,
    },
    Cancelled {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UiState {
    pub session_id: String,
    pub model: String,
    pub phase: UiPhase,
    pub run_id: Option<String>,
    pub transcript: Vec<TranscriptEntry>,
    pub composer: String,
    pub composer_cursor: usize,
    pub pending: Option<PendingOverlay>,
    pub scroll_back: usize,
    pub working_detail: Option<String>,
    pub working_elapsed: Duration,
    pub animation_frame: u64,
    stream_output_id: Option<String>,
    stream_chunks: BTreeMap<u64, String>,
    seen_delta_ids: BTreeSet<String>,
    activity_reducer: ActivityReducer,
    active_processes: BTreeSet<u64>,
    last_tick: Option<Instant>,
}

impl UiState {
    pub(crate) fn new(session_id: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            model: model.into(),
            phase: UiPhase::Idle,
            run_id: None,
            transcript: Vec::new(),
            composer: String::new(),
            composer_cursor: 0,
            pending: None,
            scroll_back: 0,
            working_detail: None,
            working_elapsed: Duration::ZERO,
            animation_frame: 0,
            stream_output_id: None,
            stream_chunks: BTreeMap::new(),
            seen_delta_ids: BTreeSet::new(),
            activity_reducer: ActivityReducer::default(),
            active_processes: BTreeSet::new(),
            last_tick: None,
        }
    }

    pub(crate) fn streamed_text(&self) -> String {
        self.stream_chunks.values().cloned().collect()
    }

    pub(crate) fn active_process_count(&self) -> usize {
        self.active_processes.len()
    }

    #[cfg(test)]
    pub(crate) fn committed_assistant_text(&self) -> Option<&str> {
        self.transcript
            .iter()
            .rev()
            .find(|entry| entry.role == TranscriptRole::Assistant)
            .map(|entry| entry.text.as_str())
    }

    fn composer_editable(&self) -> bool {
        !matches!(self.phase, UiPhase::WaitingApproval | UiPhase::Cancelling)
    }

    fn clear_stream(&mut self) {
        self.stream_output_id = None;
        self.stream_chunks.clear();
        self.seen_delta_ids.clear();
    }

    fn take_composer(&mut self) -> Option<String> {
        if self.composer.trim().is_empty() {
            return None;
        }
        let input = std::mem::take(&mut self.composer);
        self.composer_cursor = 0;
        Some(input)
    }

    fn insert_text(&mut self, text: &str) {
        self.composer.insert_str(self.composer_cursor, text);
        self.composer_cursor += text.len();
    }

    fn move_cursor_left(&mut self) {
        if let Some((index, _)) = self.composer[..self.composer_cursor]
            .char_indices()
            .next_back()
        {
            self.composer_cursor = index;
        }
    }

    fn move_cursor_right(&mut self) {
        if let Some(character) = self.composer[self.composer_cursor..].chars().next() {
            self.composer_cursor += character.len_utf8();
        }
    }

    fn backspace(&mut self) {
        let end = self.composer_cursor;
        self.move_cursor_left();
        if self.composer_cursor < end {
            self.composer.drain(self.composer_cursor..end);
        }
    }

    fn delete(&mut self) {
        let start = self.composer_cursor;
        self.move_cursor_right();
        let end = self.composer_cursor;
        if start < end {
            self.composer.drain(start..end);
            self.composer_cursor = start;
        }
    }

    fn upsert_tool(&mut self, projection: ActivityProjection) {
        let id = format!("tool:{}", projection.id);
        if let Some(entry) = self
            .transcript
            .iter_mut()
            .find(|entry| entry.id.as_deref() == Some(id.as_str()))
        {
            entry.text = projection.summary;
            entry.tool_status = Some(projection.status);
            return;
        }
        self.transcript.push(TranscriptEntry {
            id: Some(id),
            role: TranscriptRole::Tool,
            text: projection.summary,
            tool_status: Some(projection.status),
        });
    }

    fn settle_tools(&mut self, state: ToolActivityState) {
        for projection in self.activity_reducer.settle(state) {
            self.upsert_tool(projection);
        }
    }

    fn commit_output(&mut self, output_id: String, text: String) {
        let id = format!("output:{output_id}");
        if let Some(entry) = self
            .transcript
            .iter_mut()
            .find(|entry| entry.id.as_deref() == Some(id.as_str()))
        {
            entry.text = text;
        } else {
            self.transcript.push(TranscriptEntry::assistant(id, text));
        }
        self.clear_stream();
        self.scroll_back = 0;
    }

    fn reconcile_delivery(&mut self, final_text: String) {
        let most_recent_user = self
            .transcript
            .iter()
            .rposition(|entry| entry.role == TranscriptRole::User);
        let assistant = self
            .transcript
            .iter_mut()
            .enumerate()
            .rev()
            .find(|(index, entry)| {
                entry.role == TranscriptRole::Assistant
                    && most_recent_user.is_none_or(|user| *index > user)
            })
            .map(|(_, entry)| entry);
        if let Some(assistant) = assistant {
            assistant.text = final_text;
        } else {
            self.transcript
                .push(TranscriptEntry::assistant("delivery", final_text));
        }
    }
}

pub(crate) fn update(state: &mut UiState, msg: UiMsg) -> Vec<UiEffect> {
    match msg {
        UiMsg::InsertText(text) if state.composer_editable() => state.insert_text(&text),
        UiMsg::Backspace if state.composer_editable() => state.backspace(),
        UiMsg::Delete if state.composer_editable() => state.delete(),
        UiMsg::MoveCursorLeft if state.composer_editable() => state.move_cursor_left(),
        UiMsg::MoveCursorRight if state.composer_editable() => state.move_cursor_right(),
        UiMsg::MoveCursorStart if state.composer_editable() => state.composer_cursor = 0,
        UiMsg::MoveCursorEnd if state.composer_editable() => {
            state.composer_cursor = state.composer.len()
        }
        UiMsg::InsertText(_)
        | UiMsg::Backspace
        | UiMsg::Delete
        | UiMsg::MoveCursorLeft
        | UiMsg::MoveCursorRight
        | UiMsg::MoveCursorStart
        | UiMsg::MoveCursorEnd => {}
        UiMsg::Submit => return submit(state),
        UiMsg::Approval(choice) => return resolve_approval(state, choice),
        UiMsg::Cancel => return cancel(state),
        UiMsg::Quit => return vec![UiEffect::Quit],
        UiMsg::ScrollUp(rows) => state.scroll_back = state.scroll_back.saturating_add(rows),
        UiMsg::ScrollDown(rows) => state.scroll_back = state.scroll_back.saturating_sub(rows),
        UiMsg::Tick { now } => {
            if state.run_id.is_some() {
                if matches!(state.phase, UiPhase::Running | UiPhase::Cancelling) {
                    if let Some(last_tick) = state.last_tick {
                        state.working_elapsed += now.saturating_duration_since(last_tick);
                    }
                }
                state.last_tick = Some(now);
                if matches!(state.phase, UiPhase::Running | UiPhase::Cancelling) {
                    state.animation_frame = state.animation_frame.wrapping_add(1);
                }
            }
        }
        UiMsg::RunStarted { run_id } => {
            if state.run_id.as_deref() != Some(run_id.as_str()) {
                state.working_elapsed = Duration::ZERO;
                state.animation_frame = 0;
                state.active_processes.clear();
                state.last_tick = None;
            }
            state.run_id = Some(run_id);
            state.phase = UiPhase::Running;
            state.pending = None;
        }
        UiMsg::StreamDelta {
            delta_id,
            output_id,
            order,
            text,
        } => {
            if state.stream_output_id.as_deref() != Some(output_id.as_str()) {
                state.clear_stream();
                state.stream_output_id = Some(output_id);
            }
            if state.seen_delta_ids.insert(delta_id) {
                state.stream_chunks.entry(order).or_insert(text);
                state.scroll_back = 0;
            }
        }
        UiMsg::OutputCommitted { output_id, text } => state.commit_output(output_id, text),
        UiMsg::ToolActivity {
            activity_id,
            tool_name,
            state: activity_state,
        } => {
            let projection = state
                .activity_reducer
                .observe(activity_id, tool_name, activity_state);
            state.upsert_tool(projection);
        }
        UiMsg::ProgressReported { summary } => state.working_detail = Some(summary),
        UiMsg::ProcessActivity {
            run_id,
            session_id,
            running,
        } => {
            if state.run_id.as_deref() == Some(run_id.as_str()) {
                if running {
                    state.active_processes.insert(session_id);
                } else {
                    state.active_processes.remove(&session_id);
                }
            }
        }
        UiMsg::ProcessInventory {
            run_id,
            session_ids,
        } => {
            if state.run_id.as_deref() == Some(run_id.as_str()) {
                state.active_processes = session_ids.into_iter().collect();
            }
        }
        UiMsg::WaitingInput {
            run_id,
            request_id,
            prompt,
        } => {
            state.run_id = Some(run_id);
            state.phase = UiPhase::WaitingInput;
            state.pending = Some(PendingOverlay::Input { request_id, prompt });
        }
        UiMsg::WaitingApproval {
            run_id,
            request_id,
            summary,
        } => {
            state.run_id = Some(run_id);
            state.phase = UiPhase::WaitingApproval;
            state.pending = Some(PendingOverlay::Approval {
                request_id,
                summary,
            });
        }
        UiMsg::RequestResolved { request_id } => {
            if state.pending.as_ref().map(PendingOverlay::request_id) == Some(request_id.as_str()) {
                state.pending = None;
                state.phase = UiPhase::Running;
            }
        }
        UiMsg::Stopping => {
            state.pending = None;
            state.phase = UiPhase::Cancelling;
        }
        UiMsg::Notice {
            id,
            message,
            is_error,
        } => {
            if let Some(entry) = state
                .transcript
                .iter_mut()
                .find(|entry| entry.id.as_deref() == Some(id.as_str()))
            {
                entry.text = message;
                entry.role = if is_error {
                    TranscriptRole::Error
                } else {
                    TranscriptRole::System
                };
            } else if is_error {
                state.transcript.push(TranscriptEntry::error(id, message));
            } else {
                let mut entry = TranscriptEntry::system(message);
                entry.id = Some(id);
                state.transcript.push(entry);
            }
        }
        UiMsg::Completed { final_text } => {
            state.settle_tools(ToolActivityState::Succeeded);
            if let Some(final_text) = final_text {
                state.reconcile_delivery(final_text);
            }
            state.clear_stream();
            state.pending = None;
            state.working_detail = None;
            state.active_processes.clear();
            state.last_tick = None;
            state.run_id = None;
            state.phase = UiPhase::Completed;
        }
        UiMsg::Failed { message } => {
            state.settle_tools(ToolActivityState::Failed);
            state.transcript.push(TranscriptEntry::error(
                "terminal-failure",
                format!("Run failed: {message}"),
            ));
            state.clear_stream();
            state.pending = None;
            state.working_detail = None;
            state.active_processes.clear();
            state.last_tick = None;
            state.run_id = None;
            state.phase = UiPhase::Failed;
        }
        UiMsg::Cancelled { reason } => {
            state.settle_tools(ToolActivityState::Cancelled);
            state
                .transcript
                .push(TranscriptEntry::system(format!("Run cancelled: {reason}")));
            state.clear_stream();
            state.pending = None;
            state.working_detail = None;
            state.active_processes.clear();
            state.last_tick = None;
            state.run_id = None;
            state.phase = UiPhase::Cancelled;
        }
    }
    Vec::new()
}

fn submit(state: &mut UiState) -> Vec<UiEffect> {
    if state.phase.accepts_new_run() {
        let Some(input) = state.take_composer() else {
            return Vec::new();
        };
        state.transcript.push(TranscriptEntry::user(input.clone()));
        state.activity_reducer.begin_run();
        state.clear_stream();
        state.pending = None;
        state.working_detail = None;
        state.working_elapsed = Duration::ZERO;
        state.animation_frame = 0;
        state.active_processes.clear();
        state.last_tick = None;
        state.run_id = None;
        state.phase = UiPhase::Running;
        state.scroll_back = 0;
        return vec![UiEffect::StartRun { input }];
    }

    if state.phase == UiPhase::Running {
        let Some(run_id) = state.run_id.clone() else {
            return Vec::new();
        };
        let Some(input) = state.take_composer() else {
            return Vec::new();
        };
        state.transcript.push(TranscriptEntry::user(input.clone()));
        state.scroll_back = 0;
        return vec![UiEffect::Steer { run_id, input }];
    }

    if state.phase == UiPhase::WaitingInput {
        let (Some(run_id), Some(PendingOverlay::Input { request_id, .. })) =
            (state.run_id.clone(), state.pending.clone())
        else {
            return Vec::new();
        };
        let Some(value) = state.take_composer() else {
            return Vec::new();
        };
        state.transcript.push(TranscriptEntry::user(value.clone()));
        state.pending = None;
        state.phase = UiPhase::Running;
        state.scroll_back = 0;
        return vec![UiEffect::ResolveInput {
            run_id,
            request_id,
            value,
        }];
    }

    Vec::new()
}

fn resolve_approval(state: &mut UiState, choice: ApprovalChoice) -> Vec<UiEffect> {
    let (Some(run_id), Some(PendingOverlay::Approval { request_id, .. })) =
        (state.run_id.clone(), state.pending.clone())
    else {
        return Vec::new();
    };
    if state.phase != UiPhase::WaitingApproval {
        return Vec::new();
    }
    state.pending = None;
    state.phase = UiPhase::Running;
    vec![UiEffect::ResolveApproval {
        run_id,
        request_id,
        choice,
    }]
}

fn cancel(state: &mut UiState) -> Vec<UiEffect> {
    if !matches!(
        state.phase,
        UiPhase::Running | UiPhase::WaitingInput | UiPhase::WaitingApproval
    ) {
        return Vec::new();
    }
    let Some(run_id) = state.run_id.clone() else {
        return Vec::new();
    };
    state.pending = None;
    state.phase = UiPhase::Cancelling;
    vec![UiEffect::CancelRun { run_id }]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn type_and_submit(state: &mut UiState, text: &str) -> Vec<UiEffect> {
        update(state, UiMsg::InsertText(text.to_owned()));
        update(state, UiMsg::Submit)
    }

    #[test]
    fn lifecycle_and_commands_follow_the_ui_transition_contract() {
        let mut state = UiState::new("session-a", "model-a");
        assert_eq!(state.phase, UiPhase::Idle);
        assert_eq!(
            type_and_submit(&mut state, "inspect this"),
            vec![UiEffect::StartRun {
                input: "inspect this".to_owned()
            }]
        );
        assert_eq!(state.phase, UiPhase::Running);
        assert!(update(&mut state, UiMsg::Cancel).is_empty());

        update(
            &mut state,
            UiMsg::RunStarted {
                run_id: "run-a".to_owned(),
            },
        );
        assert_eq!(
            type_and_submit(&mut state, "also inspect tests"),
            vec![UiEffect::Steer {
                run_id: "run-a".to_owned(),
                input: "also inspect tests".to_owned(),
            }]
        );

        update(
            &mut state,
            UiMsg::WaitingInput {
                run_id: "run-a".to_owned(),
                request_id: "input-a".to_owned(),
                prompt: "Which crate?".to_owned(),
            },
        );
        assert_eq!(state.phase, UiPhase::WaitingInput);
        assert_eq!(
            type_and_submit(&mut state, "orchestral-runtime"),
            vec![UiEffect::ResolveInput {
                run_id: "run-a".to_owned(),
                request_id: "input-a".to_owned(),
                value: "orchestral-runtime".to_owned(),
            }]
        );

        update(
            &mut state,
            UiMsg::WaitingApproval {
                run_id: "run-a".to_owned(),
                request_id: "approval-a".to_owned(),
                summary: "Run cargo test".to_owned(),
            },
        );
        assert_eq!(state.phase, UiPhase::WaitingApproval);
        assert!(type_and_submit(&mut state, "ignored").is_empty());
        assert_eq!(
            update(&mut state, UiMsg::Approval(ApprovalChoice::Allow)),
            vec![UiEffect::ResolveApproval {
                run_id: "run-a".to_owned(),
                request_id: "approval-a".to_owned(),
                choice: ApprovalChoice::Allow,
            }]
        );
        assert_eq!(state.phase, UiPhase::Running);
        update(
            &mut state,
            UiMsg::WaitingApproval {
                run_id: "run-a".to_owned(),
                request_id: "approval-b".to_owned(),
                summary: "Publish artifact".to_owned(),
            },
        );
        assert_eq!(
            update(&mut state, UiMsg::Approval(ApprovalChoice::Deny)),
            vec![UiEffect::ResolveApproval {
                run_id: "run-a".to_owned(),
                request_id: "approval-b".to_owned(),
                choice: ApprovalChoice::Deny,
            }]
        );
        update(
            &mut state,
            UiMsg::WaitingInput {
                run_id: "run-a".to_owned(),
                request_id: "input-b".to_owned(),
                prompt: "Optional detail".to_owned(),
            },
        );
        update(
            &mut state,
            UiMsg::RequestResolved {
                request_id: "input-b".to_owned(),
            },
        );
        assert_eq!(state.phase, UiPhase::Running);
        assert_eq!(
            update(&mut state, UiMsg::Cancel),
            vec![UiEffect::CancelRun {
                run_id: "run-a".to_owned()
            }]
        );
        assert_eq!(state.phase, UiPhase::Cancelling);
        update(
            &mut state,
            UiMsg::Cancelled {
                reason: "user requested".to_owned(),
            },
        );
        assert_eq!(state.phase, UiPhase::Cancelled);

        state.composer.clear();
        state.composer_cursor = 0;
        type_and_submit(&mut state, "second run");
        update(
            &mut state,
            UiMsg::RunStarted {
                run_id: "run-b".to_owned(),
            },
        );
        update(
            &mut state,
            UiMsg::Completed {
                final_text: Some("done".to_owned()),
            },
        );
        assert_eq!(state.phase, UiPhase::Completed);

        type_and_submit(&mut state, "third run");
        update(
            &mut state,
            UiMsg::Failed {
                message: "provider unavailable".to_owned(),
            },
        );
        assert_eq!(state.phase, UiPhase::Failed);
    }

    #[test]
    fn illegal_actions_emit_no_agent_command() {
        let mut state = UiState::new("session", "model");
        for msg in [
            UiMsg::Approval(ApprovalChoice::Allow),
            UiMsg::Approval(ApprovalChoice::Deny),
            UiMsg::Cancel,
            UiMsg::Submit,
        ] {
            assert!(update(&mut state, msg).is_empty());
        }

        type_and_submit(&mut state, "start");
        assert!(update(&mut state, UiMsg::Approval(ApprovalChoice::Allow)).is_empty());
        update(
            &mut state,
            UiMsg::RunStarted {
                run_id: "run".to_owned(),
            },
        );
        update(&mut state, UiMsg::Cancel);
        for msg in [
            UiMsg::Approval(ApprovalChoice::Deny),
            UiMsg::Submit,
            UiMsg::Cancel,
        ] {
            assert!(update(&mut state, msg).is_empty());
        }
    }

    #[test]
    fn one_thousand_lossy_duplicate_and_reordered_streams_reconcile_to_durable_output() {
        for case in 0..1_000_u64 {
            let mut state = UiState::new("session", "model");
            type_and_submit(&mut state, "question");
            update(
                &mut state,
                UiMsg::RunStarted {
                    run_id: format!("run-{case}"),
                },
            );
            let final_text = format!("最终结果-{case}-🧪");
            let chunks = ["最终", "结果-", &format!("{case}"), "-🧪"];
            let order = if case % 2 == 0 {
                [2, 0, 3, 1]
            } else {
                [1, 3, 0, 2]
            };
            for index in order {
                if !(case % 5 == 0 && index == 1) {
                    update(
                        &mut state,
                        UiMsg::StreamDelta {
                            delta_id: format!("delta-{case}-{index}"),
                            output_id: "answer".to_owned(),
                            order: index as u64,
                            text: chunks[index].to_owned(),
                        },
                    );
                }
            }
            update(
                &mut state,
                UiMsg::StreamDelta {
                    delta_id: format!("delta-{case}-0"),
                    output_id: "answer".to_owned(),
                    order: 0,
                    text: "duplicate must be ignored".to_owned(),
                },
            );
            update(
                &mut state,
                UiMsg::OutputCommitted {
                    output_id: "answer".to_owned(),
                    text: final_text.clone(),
                },
            );
            update(
                &mut state,
                UiMsg::Completed {
                    final_text: Some(final_text.clone()),
                },
            );
            assert_eq!(state.streamed_text(), "");
            assert_eq!(state.committed_assistant_text(), Some(final_text.as_str()));
            assert_eq!(
                state
                    .transcript
                    .iter()
                    .filter(|entry| entry.role == TranscriptRole::Assistant)
                    .count(),
                1
            );
        }
    }

    #[test]
    fn composer_edits_on_utf8_boundaries() {
        let mut state = UiState::new("session", "model");
        update(&mut state, UiMsg::InsertText("A中🧪B".to_owned()));
        update(&mut state, UiMsg::MoveCursorLeft);
        update(&mut state, UiMsg::Backspace);
        assert_eq!(state.composer, "A中B");
        update(&mut state, UiMsg::MoveCursorStart);
        update(&mut state, UiMsg::Delete);
        assert_eq!(state.composer, "中B");
        update(&mut state, UiMsg::MoveCursorEnd);
        update(&mut state, UiMsg::InsertText("\n第二行".to_owned()));
        assert_eq!(state.composer, "中B\n第二行");
    }

    #[test]
    fn work_clock_pauses_for_user_requests_and_processes_are_run_scoped() {
        let mut state = UiState::new("session", "model");
        let start = Instant::now();
        update(
            &mut state,
            UiMsg::RunStarted {
                run_id: "run-a".to_owned(),
            },
        );
        update(&mut state, UiMsg::Tick { now: start });
        update(
            &mut state,
            UiMsg::Tick {
                now: start + Duration::from_secs(2),
            },
        );
        assert_eq!(state.working_elapsed, Duration::from_secs(2));
        assert_eq!(state.animation_frame, 2);

        update(
            &mut state,
            UiMsg::WaitingApproval {
                run_id: "run-a".to_owned(),
                request_id: "approval-a".to_owned(),
                summary: "Run a command".to_owned(),
            },
        );
        update(
            &mut state,
            UiMsg::Tick {
                now: start + Duration::from_secs(12),
            },
        );
        assert_eq!(state.working_elapsed, Duration::from_secs(2));
        update(
            &mut state,
            UiMsg::RequestResolved {
                request_id: "approval-a".to_owned(),
            },
        );
        update(
            &mut state,
            UiMsg::Tick {
                now: start + Duration::from_secs(13),
            },
        );
        assert_eq!(state.working_elapsed, Duration::from_secs(3));

        update(
            &mut state,
            UiMsg::ProcessActivity {
                run_id: "stale-run".to_owned(),
                session_id: 1,
                running: true,
            },
        );
        assert_eq!(state.active_process_count(), 0);
        update(
            &mut state,
            UiMsg::ProcessActivity {
                run_id: "run-a".to_owned(),
                session_id: 7,
                running: true,
            },
        );
        assert_eq!(state.active_process_count(), 1);
        update(
            &mut state,
            UiMsg::ProcessInventory {
                run_id: "run-a".to_owned(),
                session_ids: vec![8, 9],
            },
        );
        assert_eq!(state.active_process_count(), 2);
    }

    #[test]
    fn tool_activity_reduces_repeated_calls_into_one_transcript_entry() {
        let mut state = UiState::new("session", "model");
        state.activity_reducer.begin_run();
        for index in 0..16 {
            let activity_id = format!("read-{index}");
            update(
                &mut state,
                UiMsg::ToolActivity {
                    activity_id: activity_id.clone(),
                    tool_name: "file_read".to_owned(),
                    state: ToolActivityState::Running,
                },
            );
            update(
                &mut state,
                UiMsg::ToolActivity {
                    activity_id,
                    tool_name: "file_read".to_owned(),
                    state: ToolActivityState::Succeeded,
                },
            );
        }
        let tools = state
            .transcript
            .iter()
            .filter(|entry| entry.role == TranscriptRole::Tool)
            .collect::<Vec<_>>();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].text, "Read 16 files");
        assert_eq!(tools[0].tool_status, Some(ActivityStatus::Succeeded));
    }
}
