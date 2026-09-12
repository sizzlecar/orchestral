use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use orchestral_core::agent_protocol::wire::{ToolActivityEvidence, ToolActivityState};

use super::activity::{ActivityDetail, ActivityProjection, ActivityReducer, ActivityStatus};
use super::editor::{self, Edit, InputHistory};
use super::menu::{Choice, FileReference, Menu, MenuKind};
use super::viewport::Viewport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UiPhase {
    Idle,
    Running,
    WaitingInput,
    WaitingApproval,
    Cancelling,
    Completed,
    Incomplete,
    Failed,
    Cancelled,
}

impl UiPhase {
    fn accepts_new_run(self) -> bool {
        matches!(
            self,
            Self::Idle | Self::Completed | Self::Incomplete | Self::Failed | Self::Cancelled
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
    pub continuation: bool,
    pub id: Option<String>,
    pub role: TranscriptRole,
    pub text: String,
    pub tool_status: Option<ActivityStatus>,
    pub tool_details: Vec<ActivityDetail>,
}

impl TranscriptEntry {
    pub(crate) fn user(text: impl Into<String>) -> Self {
        Self {
            continuation: false,
            id: None,
            role: TranscriptRole::User,
            text: text.into(),
            tool_status: None,
            tool_details: Vec::new(),
        }
    }

    pub(crate) fn assistant(id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            continuation: false,
            id: Some(id.into()),
            role: TranscriptRole::Assistant,
            text: text.into(),
            tool_status: None,
            tool_details: Vec::new(),
        }
    }

    pub(crate) fn system(text: impl Into<String>) -> Self {
        Self {
            continuation: false,
            id: None,
            role: TranscriptRole::System,
            text: text.into(),
            tool_status: None,
            tool_details: Vec::new(),
        }
    }

    pub(crate) fn error(id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            continuation: false,
            id: Some(id.into()),
            role: TranscriptRole::Error,
            text: text.into(),
            tool_status: None,
            tool_details: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PendingOverlay {
    Input {
        request_id: String,
        prompt: String,
    },
    Approval {
        request_id: String,
        summary: String,
        session_approval_available: bool,
    },
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
    AllowSession,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RequestSubmission {
    Answer { request_id: String, value: String },
    Approval { request_id: String },
    Accepted { request_id: String },
}

impl RequestSubmission {
    fn request_id(&self) -> &str {
        match self {
            Self::Answer { request_id, .. }
            | Self::Approval { request_id }
            | Self::Accepted { request_id } => request_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ComposerOrigin {
    Edited,
    RestoredQuestionDraft,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UiEffect {
    HostCommand {
        command: String,
    },
    MenuChoice {
        kind: MenuKind,
        value: String,
    },
    LocalAction {
        action: super::menu::LocalAction,
        return_to: Option<Box<super::menu::Menu>>,
    },
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
    OpenMenu(Menu),
    FilesLoaded(Vec<Choice>),
    Complete,
    SelectCandidate {
        up: bool,
    },
    InsertText(String),
    Backspace,
    Delete,
    MoveCursorLeft,
    MoveCursorRight,
    MoveCursorStart,
    MoveCursorEnd,
    Edit(Edit),
    History {
        up: bool,
    },
    Escape,
    ToggleHelp,
    ToggleTools,
    ToggleInput,
    FollowOutput,
    Submit,
    SelectApproval(ApprovalChoice),
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
        evidence: Vec<ToolActivityEvidence>,
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
        session_approval_available: bool,
    },
    RequestResolved {
        request_id: String,
    },
    RequestSubmissionAccepted {
        request_id: String,
    },
    RequestSubmissionFailed {
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
    Incomplete {
        message: String,
    },
    Cancelled {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UiState {
    pub session_id: String,
    pub project: String,
    pub session_title: String,
    pub model: String,
    pub context_budget: Option<u64>,
    pub terminal_size: (u16, u16),
    pub phase: UiPhase,
    pub run_id: Option<String>,
    pub transcript: Vec<TranscriptEntry>,
    pub composer: String,
    pub composer_cursor: usize,
    pub pending: Option<PendingOverlay>,
    pub approval_choice: ApprovalChoice,
    pub viewport: Viewport,
    pub tools_expanded: bool,
    pub ui_notice: Option<String>,
    pub exit_requested: bool,
    pub approval_selected: bool,
    pub input_history: InputHistory,
    pub menu: Option<Menu>,
    pub files: std::sync::Arc<Vec<Choice>>,
    pub files_loaded: bool,
    pub references: Vec<FileReference>,
    pub completion_selected: usize,
    pub completion_dismissed: bool,
    pub theme: String,
    pub color_enabled: bool,
    pub host_busy: bool,
    pub suspended_draft: Option<(String, usize)>,
    pub transcript_revision: u64,
    pub transcript_dirty_from: usize,
    pub input_expanded: bool,
    pub working_detail: Option<String>,
    pub working_elapsed: Duration,
    pub animation_frame: u64,
    request_submission: Option<RequestSubmission>,
    resolved_requests: BTreeSet<String>,
    composer_origin: ComposerOrigin,
    stream_output_id: Option<String>,
    stream_chunks: BTreeMap<u64, String>,
    seen_delta_ids: BTreeSet<String>,
    activity_reducer: ActivityReducer,
    active_processes: BTreeSet<u64>,
    last_tick: Option<Instant>,
}

impl UiState {
    pub(crate) fn new(session_id: impl Into<String>, model: impl Into<String>) -> Self {
        let session_id = session_id.into();
        Self {
            session_title: session_id.clone(),
            session_id,
            project: "workspace".to_owned(),
            model: model.into(),
            context_budget: None,
            terminal_size: (80, 24),
            phase: UiPhase::Idle,
            run_id: None,
            transcript: Vec::new(),
            composer: String::new(),
            composer_cursor: 0,
            pending: None,
            approval_choice: ApprovalChoice::Allow,
            viewport: Viewport::default(),
            tools_expanded: false,
            ui_notice: None,
            exit_requested: false,
            approval_selected: false,
            input_history: InputHistory::default(),
            menu: None,
            files: Default::default(),
            files_loaded: false,
            references: Vec::new(),
            completion_selected: 0,
            completion_dismissed: false,
            theme: "dark".to_owned(),
            color_enabled: true,
            host_busy: false,
            suspended_draft: None,
            transcript_revision: 0,
            transcript_dirty_from: 0,
            input_expanded: false,
            working_detail: None,
            working_elapsed: Duration::ZERO,
            animation_frame: 0,
            request_submission: None,
            resolved_requests: BTreeSet::new(),
            composer_origin: ComposerOrigin::Edited,
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

    pub(crate) fn stream_key(&self) -> String {
        self.stream_output_id
            .as_ref()
            .map_or_else(|| "stream".to_owned(), |id| format!("output:{id}"))
    }

    fn restore_question_draft(&mut self) {
        if let Some((draft, cursor)) = self.suspended_draft.take() {
            let unsent = std::mem::replace(&mut self.composer, draft);
            self.composer_cursor = cursor;
            self.composer_origin = ComposerOrigin::RestoredQuestionDraft;
            if !unsent.is_empty() {
                self.menu = Some(super::services::detail(
                    "Unsubmitted answer · previous draft restored",
                    unsent,
                ));
            }
        }
    }

    pub(crate) fn request_submission_pending(&self) -> bool {
        self.request_submission.is_some()
    }

    fn accept_request_submission(&mut self, request_id: &str) {
        if self
            .request_submission
            .as_ref()
            .map(RequestSubmission::request_id)
            != Some(request_id)
        {
            return;
        }
        if let Some(RequestSubmission::Answer { value, .. }) = self.request_submission.take() {
            let mut entry = TranscriptEntry::user(value);
            entry.continuation = true;
            self.transcript.push(entry);
            self.transcript_revision = self.transcript_revision.wrapping_add(1);
            self.viewport.follow();
        }
        self.request_submission = Some(RequestSubmission::Accepted {
            request_id: request_id.to_owned(),
        });
    }

    fn fail_request_submission(&mut self, request_id: &str) {
        if self
            .request_submission
            .as_ref()
            .map(RequestSubmission::request_id)
            != Some(request_id)
        {
            return;
        }
        if let Some(RequestSubmission::Answer { value, .. }) = self.request_submission.take() {
            self.composer_cursor = value.len();
            self.composer = value;
            self.composer_origin = ComposerOrigin::Edited;
        }
        self.approval_selected = false;
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
        !self.request_submission_pending()
            && !matches!(self.phase, UiPhase::WaitingApproval | UiPhase::Cancelling)
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
        self.input_history.push(&input);
        self.composer_cursor = 0;
        Some(input)
    }

    fn insert_text(&mut self, text: &str) {
        if self.composer.len().saturating_add(text.len()) > 1024 * 1024 {
            self.ui_notice = Some("Input exceeds 1 MiB; draft preserved".to_owned());
            return;
        }
        self.ui_notice = None;
        self.composer.insert_str(self.composer_cursor, text);
        self.composer_cursor += text.len();
        editor::snap_cursor(&self.composer, &mut self.composer_cursor);
    }

    fn move_cursor_left(&mut self) {
        editor::edit(&mut self.composer, &mut self.composer_cursor, Edit::Left);
    }

    fn move_cursor_right(&mut self) {
        editor::edit(&mut self.composer, &mut self.composer_cursor, Edit::Right);
    }

    fn backspace(&mut self) {
        editor::edit(
            &mut self.composer,
            &mut self.composer_cursor,
            Edit::Backspace,
        );
    }

    fn delete(&mut self) {
        editor::edit(&mut self.composer, &mut self.composer_cursor, Edit::Delete);
    }

    fn upsert_tool(&mut self, projection: ActivityProjection) {
        let id = format!("tool:{}", projection.id);
        if let Some((index, entry)) = self
            .transcript
            .iter_mut()
            .enumerate()
            .find(|(_, entry)| entry.id.as_deref() == Some(id.as_str()))
        {
            self.transcript_dirty_from = self.transcript_dirty_from.min(index);
            entry.text = projection.summary;
            entry.tool_status = Some(projection.status);
            entry.tool_details = projection.details;
            return;
        }
        self.transcript.push(TranscriptEntry {
            continuation: false,
            id: Some(id),
            role: TranscriptRole::Tool,
            text: projection.summary,
            tool_status: Some(projection.status),
            tool_details: projection.details,
        });
    }

    fn settle_tools(&mut self, state: ToolActivityState) {
        if let Some(run_id) = &self.run_id {
            let notice_id = format!("history-unfinished-{run_id}");
            if let Some(index) = self
                .transcript
                .iter()
                .position(|entry| entry.id.as_deref() == Some(notice_id.as_str()))
            {
                self.transcript_dirty_from = self.transcript_dirty_from.min(index);
                self.transcript
                    .retain(|entry| entry.id.as_deref() != Some(notice_id.as_str()));
            }
        }
        for projection in self.activity_reducer.settle(state) {
            self.upsert_tool(projection);
        }
    }

    fn commit_output(&mut self, output_id: String, text: String) {
        let id = format!("output:{output_id}");
        if let Some((index, entry)) = self
            .transcript
            .iter_mut()
            .enumerate()
            .find(|(_, entry)| entry.id.as_deref() == Some(id.as_str()))
        {
            self.transcript_dirty_from = self.transcript_dirty_from.min(index);
            entry.text = text;
        } else {
            self.transcript.push(TranscriptEntry::assistant(id, text));
        }
        self.clear_stream();
        self.viewport.changed();
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
            });
        if let Some((index, assistant)) = assistant {
            self.transcript_dirty_from = self.transcript_dirty_from.min(index);
            assistant.text = final_text;
        } else {
            self.transcript
                .push(TranscriptEntry::assistant("delivery", final_text));
        }
    }
}

pub(crate) fn update(state: &mut UiState, msg: UiMsg) -> Vec<UiEffect> {
    state.transcript_dirty_from = state.transcript_dirty_from.min(state.transcript.len());
    if state.menu.is_none() {
        if matches!(msg, UiMsg::Submit)
            && state.composer_origin == ComposerOrigin::RestoredQuestionDraft
            && !state.composer.is_empty()
        {
            state.ui_notice =
                Some("Previous draft restored; edit or move the cursor before sending".to_owned());
            return Vec::new();
        }
        if state.composer_editable()
            && matches!(
                msg,
                UiMsg::InsertText(_)
                    | UiMsg::Backspace
                    | UiMsg::Delete
                    | UiMsg::MoveCursorLeft
                    | UiMsg::MoveCursorRight
                    | UiMsg::MoveCursorStart
                    | UiMsg::MoveCursorEnd
                    | UiMsg::Edit(_)
                    | UiMsg::History { .. }
                    | UiMsg::Complete
            )
        {
            state.composer_origin = ComposerOrigin::Edited;
        }
    }
    if matches!(
        msg,
        UiMsg::RunStarted { .. }
            | UiMsg::StreamDelta { .. }
            | UiMsg::OutputCommitted { .. }
            | UiMsg::ToolActivity { .. }
            | UiMsg::Completed { .. }
            | UiMsg::Failed { .. }
            | UiMsg::Cancelled { .. }
            | UiMsg::Incomplete { .. }
            | UiMsg::Notice { .. }
            | UiMsg::Submit
    ) {
        state.transcript_revision = state.transcript_revision.wrapping_add(1);
    }
    if let Some(effects) = super::interaction::route(state, &msg) {
        return effects;
    }
    match msg {
        UiMsg::OpenMenu(menu) => {
            state.menu = Some(menu);
        }
        UiMsg::FilesLoaded(files) => {
            let selected = super::interaction::completion(state)
                .filter(|menu| menu.kind == MenuKind::Files)
                .and_then(|menu| menu.selected().map(|choice| choice.value.clone()));
            state.files = std::sync::Arc::new(files);
            state.files_loaded = true;
            if let Some(selected) = selected {
                state.completion_selected = super::interaction::completion(state)
                    .and_then(|menu| {
                        menu.filtered()
                            .iter()
                            .position(|choice| choice.value == selected)
                    })
                    .unwrap_or(0);
            }
        }
        UiMsg::Complete | UiMsg::SelectCandidate { .. } => {}
        UiMsg::InsertText(text) if state.composer_editable() => state.insert_text(&text),
        UiMsg::Backspace if state.composer_editable() => state.backspace(),
        UiMsg::Delete if state.composer_editable() => state.delete(),
        UiMsg::MoveCursorLeft if state.composer_editable() => state.move_cursor_left(),
        UiMsg::MoveCursorRight if state.composer_editable() => state.move_cursor_right(),
        UiMsg::MoveCursorStart if state.composer_editable() => {
            editor::edit(&mut state.composer, &mut state.composer_cursor, Edit::Start);
        }
        UiMsg::MoveCursorEnd if state.composer_editable() => {
            editor::edit(&mut state.composer, &mut state.composer_cursor, Edit::End);
        }
        UiMsg::Edit(action) if state.composer_editable() => {
            editor::edit(&mut state.composer, &mut state.composer_cursor, action);
        }
        UiMsg::History { up } if state.composer_editable() => {
            if !editor::vertical(&state.composer, &mut state.composer_cursor, up) {
                state
                    .input_history
                    .navigate(&mut state.composer, &mut state.composer_cursor, up);
            }
        }
        UiMsg::InsertText(_)
        | UiMsg::Backspace
        | UiMsg::Delete
        | UiMsg::MoveCursorLeft
        | UiMsg::MoveCursorRight
        | UiMsg::MoveCursorStart
        | UiMsg::MoveCursorEnd
        | UiMsg::Edit(_)
        | UiMsg::History { .. } => {}
        UiMsg::Submit => return submit(state),
        UiMsg::ToggleHelp => {
            state.menu = if state.menu.is_some() {
                None
            } else {
                Some(super::services::help(state))
            };
        }
        UiMsg::ToggleInput => state.input_expanded = !state.input_expanded,
        UiMsg::ToggleTools => state.tools_expanded = !state.tools_expanded,
        UiMsg::FollowOutput => state.viewport.follow(),
        UiMsg::Escape => {
            state.ui_notice = None;
            return cancel(state);
        }
        UiMsg::SelectApproval(choice) if state.phase == UiPhase::WaitingApproval => {
            state.approval_choice = choice;
            state.approval_selected = true;
        }
        UiMsg::SelectApproval(_) => {}
        UiMsg::Approval(choice) => return resolve_approval(state, choice),
        UiMsg::Cancel if state.phase.accepts_new_run() => {
            if state.composer.is_empty() {
                state.ui_notice = Some("Ctrl+D or /quit to exit".to_owned());
            } else {
                state.composer.clear();
                state.composer_cursor = 0;
            }
        }
        UiMsg::Cancel => return cancel(state),
        UiMsg::Quit => {
            if state.phase.accepts_new_run() && !state.host_busy {
                return vec![UiEffect::Quit];
            }
            state.exit_requested = true;
            return cancel(state);
        }
        UiMsg::ScrollUp(rows) => {
            state.viewport.movement = state
                .viewport
                .movement
                .saturating_sub(rows.min(isize::MAX as usize) as isize)
        }
        UiMsg::ScrollDown(rows) => {
            state.viewport.movement = state
                .viewport
                .movement
                .saturating_add(rows.min(isize::MAX as usize) as isize)
        }
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
                state.request_submission = None;
                state.resolved_requests.clear();
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
                state.viewport.changed();
            }
        }
        UiMsg::OutputCommitted { output_id, text } => state.commit_output(output_id, text),
        UiMsg::ToolActivity {
            activity_id,
            tool_name,
            state: activity_state,
            evidence,
        } => {
            let projection =
                state
                    .activity_reducer
                    .observe(activity_id, tool_name, activity_state, evidence);
            state.upsert_tool(projection);
            state.viewport.changed();
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
            if state.resolved_requests.contains(&request_id) {
                return Vec::new();
            }
            if state.pending.as_ref().map(PendingOverlay::request_id) != Some(request_id.as_str())
                && state.suspended_draft.is_none()
            {
                state.suspended_draft =
                    Some((std::mem::take(&mut state.composer), state.composer_cursor));
                state.composer_cursor = 0;
                state.composer_origin = ComposerOrigin::Edited;
            }
            state.run_id = Some(run_id);
            state.phase = UiPhase::WaitingInput;
            state.pending = Some(PendingOverlay::Input { request_id, prompt });
        }
        UiMsg::WaitingApproval {
            run_id,
            request_id,
            summary,
            session_approval_available,
        } => {
            if state.resolved_requests.contains(&request_id) {
                return Vec::new();
            }
            let same_request = state.run_id.as_deref() == Some(run_id.as_str())
                && matches!(
                    &state.pending,
                    Some(PendingOverlay::Approval {
                        request_id: current_request_id,
                        ..
                    }) if current_request_id == &request_id
                );
            if !same_request
                || (state.approval_choice == ApprovalChoice::AllowSession
                    && !session_approval_available)
            {
                state.approval_choice = ApprovalChoice::Allow;
                state.approval_selected = false;
            }
            state.run_id = Some(run_id);
            state.phase = UiPhase::WaitingApproval;
            state.pending = Some(PendingOverlay::Approval {
                request_id,
                summary,
                session_approval_available,
            });
        }
        UiMsg::RequestResolved { request_id } => {
            state.accept_request_submission(&request_id);
            state.resolved_requests.insert(request_id.clone());
            if state
                .request_submission
                .as_ref()
                .map(RequestSubmission::request_id)
                == Some(request_id.as_str())
            {
                state.request_submission = None;
            }
            if state.pending.as_ref().map(PendingOverlay::request_id) == Some(request_id.as_str()) {
                state.restore_question_draft();
                state.pending = None;
                if matches!(
                    state.phase,
                    UiPhase::WaitingInput | UiPhase::WaitingApproval
                ) {
                    state.phase = UiPhase::Running;
                }
            }
        }
        UiMsg::RequestSubmissionAccepted { request_id } => {
            state.accept_request_submission(&request_id);
        }
        UiMsg::RequestSubmissionFailed { request_id } => {
            state.fail_request_submission(&request_id);
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
            if let Some((index, entry)) = state
                .transcript
                .iter_mut()
                .enumerate()
                .find(|(_, entry)| entry.id.as_deref() == Some(id.as_str()))
            {
                state.transcript_dirty_from = state.transcript_dirty_from.min(index);
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
            state.request_submission = None;
            state.restore_question_draft();
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
        UiMsg::Incomplete { message } => {
            state.request_submission = None;
            state.restore_question_draft();
            state.settle_tools(ToolActivityState::Failed);
            state.transcript.push(TranscriptEntry::system(format!(
                "Run incomplete: {message}"
            )));
            state.clear_stream();
            state.pending = None;
            state.working_detail = None;
            state.active_processes.clear();
            state.last_tick = None;
            state.run_id = None;
            state.phase = UiPhase::Incomplete;
        }
        UiMsg::Failed { message } => {
            state.request_submission = None;
            state.restore_question_draft();
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
            state.request_submission = None;
            state.restore_question_draft();
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
    if state.request_submission_pending() {
        state.ui_notice = Some("Response submitted; waiting for confirmation".to_owned());
        return Vec::new();
    }
    if state.host_busy {
        state.ui_notice =
            Some("Wait for the session operation; your draft is preserved".to_owned());
        return Vec::new();
    }
    let command = state.composer.trim();
    if state.phase != UiPhase::WaitingInput
        && command.starts_with('/')
        && !command.starts_with("//")
    {
        let command = command.to_owned();
        let name = command.split_whitespace().next().unwrap_or_default();
        if !super::menu::COMMANDS
            .iter()
            .any(|(known, _, _)| *known == name)
        {
            state.ui_notice = Some(format!(
                "Unknown command: {name}. Use /help; // sends a literal slash."
            ));
            return Vec::new();
        }
        state.composer.clear();
        state.composer_cursor = 0;
        if command == "/quit" {
            return update(state, UiMsg::Quit);
        }
        return vec![UiEffect::HostCommand { command }];
    }
    if state.phase != UiPhase::WaitingInput && state.composer.starts_with("//") {
        state.composer.remove(0);
        state.composer_cursor = state.composer_cursor.saturating_sub(1);
    }
    if state.phase.accepts_new_run() {
        let Some(input) = state.take_composer() else {
            return Vec::new();
        };
        if state.transcript.is_empty() {
            state.session_title = input
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .chars()
                .take(60)
                .collect();
        }
        state.transcript.push(TranscriptEntry::user(input.clone()));
        state.activity_reducer.begin_run();
        state.request_submission = None;
        state.resolved_requests.clear();
        state.clear_stream();
        state.pending = None;
        state.working_detail = None;
        state.working_elapsed = Duration::ZERO;
        state.animation_frame = 0;
        state.active_processes.clear();
        state.last_tick = None;
        state.run_id = None;
        state.phase = UiPhase::Running;
        state.viewport.follow();
        return vec![UiEffect::StartRun { input }];
    }

    if state.phase == UiPhase::Running {
        let Some(run_id) = state.run_id.clone() else {
            return Vec::new();
        };
        let Some(input) = state.take_composer() else {
            return Vec::new();
        };
        let mut entry = TranscriptEntry::user(input.clone());
        entry.continuation = true;
        state.transcript.push(entry);
        state.viewport.follow();
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
        state.request_submission = Some(RequestSubmission::Answer {
            request_id: request_id.clone(),
            value: value.clone(),
        });
        return vec![UiEffect::ResolveInput {
            run_id,
            request_id,
            value,
        }];
    }

    Vec::new()
}

fn resolve_approval(state: &mut UiState, choice: ApprovalChoice) -> Vec<UiEffect> {
    let (
        Some(run_id),
        Some(PendingOverlay::Approval {
            request_id,
            session_approval_available,
            ..
        }),
    ) = (state.run_id.clone(), state.pending.clone())
    else {
        return Vec::new();
    };
    if state.phase != UiPhase::WaitingApproval || state.request_submission_pending() {
        return Vec::new();
    }
    if choice == ApprovalChoice::AllowSession && !session_approval_available {
        return Vec::new();
    }
    state.request_submission = Some(RequestSubmission::Approval {
        request_id: request_id.clone(),
    });
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
    state.restore_question_draft();
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
            UiMsg::RequestSubmissionAccepted {
                request_id: "input-a".to_owned(),
            },
        );
        update(
            &mut state,
            UiMsg::RequestResolved {
                request_id: "input-a".to_owned(),
            },
        );

        update(
            &mut state,
            UiMsg::WaitingApproval {
                run_id: "run-a".to_owned(),
                request_id: "approval-a".to_owned(),
                summary: "Run cargo test".to_owned(),
                session_approval_available: false,
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
        assert_eq!(state.phase, UiPhase::WaitingApproval);
        update(
            &mut state,
            UiMsg::RequestResolved {
                request_id: "approval-a".to_owned(),
            },
        );
        update(
            &mut state,
            UiMsg::WaitingApproval {
                run_id: "run-a".to_owned(),
                request_id: "approval-b".to_owned(),
                summary: "Publish artifact".to_owned(),
                session_approval_available: false,
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
            UiMsg::RequestResolved {
                request_id: "approval-b".to_owned(),
            },
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
    fn skills_slash_command_never_becomes_model_input() {
        let mut state = UiState::new("session-a", "model-a");

        assert_eq!(
            type_and_submit(&mut state, "/skills disable xlsx"),
            vec![UiEffect::HostCommand {
                command: "/skills disable xlsx".to_owned()
            }]
        );
        assert_eq!(state.phase, UiPhase::Idle);
        assert!(state.transcript.is_empty());

        update(
            &mut state,
            UiMsg::RunStarted {
                run_id: "run-a".to_owned(),
            },
        );
        assert_eq!(
            type_and_submit(&mut state, "/skills list"),
            vec![UiEffect::HostCommand {
                command: "/skills list".to_owned()
            }]
        );
        assert_eq!(state.phase, UiPhase::Running);
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
    fn approval_selection_is_scoped_to_the_current_prompt() {
        let mut state = UiState::new("session", "model");
        update(
            &mut state,
            UiMsg::WaitingApproval {
                run_id: "run".to_owned(),
                request_id: "approval-a".to_owned(),
                summary: "First approval".to_owned(),
                session_approval_available: true,
            },
        );
        assert_eq!(state.approval_choice, ApprovalChoice::Allow);

        update(&mut state, UiMsg::SelectApproval(ApprovalChoice::Deny));
        assert_eq!(state.approval_choice, ApprovalChoice::Deny);

        update(
            &mut state,
            UiMsg::WaitingApproval {
                run_id: "run".to_owned(),
                request_id: "approval-a".to_owned(),
                summary: "First approval, reconciled again".to_owned(),
                session_approval_available: true,
            },
        );
        assert_eq!(
            state.approval_choice,
            ApprovalChoice::Deny,
            "reconciling the same request must not reset keyboard selection"
        );

        assert_eq!(
            update(&mut state, UiMsg::Approval(ApprovalChoice::AllowSession)),
            vec![UiEffect::ResolveApproval {
                run_id: "run".to_owned(),
                request_id: "approval-a".to_owned(),
                choice: ApprovalChoice::AllowSession,
            }]
        );

        update(
            &mut state,
            UiMsg::WaitingApproval {
                run_id: "run".to_owned(),
                request_id: "approval-b".to_owned(),
                summary: "Second approval".to_owned(),
                session_approval_available: false,
            },
        );
        assert_eq!(state.approval_choice, ApprovalChoice::Allow);
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
                session_approval_available: false,
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
                    evidence: Vec::new(),
                },
            );
            update(
                &mut state,
                UiMsg::ToolActivity {
                    activity_id,
                    tool_name: "file_read".to_owned(),
                    state: ToolActivityState::Succeeded,
                    evidence: Vec::new(),
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
