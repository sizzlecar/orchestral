use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use futures_util::StreamExt;
use orchestral_core::agent_protocol::wire::{
    AgentCommand, AgentCommandEnvelope, AgentEvent, AgentJournalRecord, AgentRunState,
    AgentTelemetry, AgentTelemetryEnvelope, AgentTerminalState, ApprovalDecision, CommandAck,
    CommandAckState, CommandId, Content, ContentBody, PendingRequest, PendingRequestPayload,
    RequestId, RequestResolution, RunId,
};
use orchestral_runtime::{
    AgentClient, AgentControlEvent, AgentRunHandle, ExecSessionEvent, ExecSessionStatus,
    InMemoryHostApprovalBroker,
};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use super::services::Tasks as ServiceTasks;
use super::terminal::TerminalSession;
use super::{
    render_cached, update, ApprovalChoice, RenderCache, UiEffect, UiMsg, UiPhase, UiState,
};

const RECONCILE_INTERVAL: Duration = Duration::from_millis(500);
const ANIMATION_INTERVAL: Duration = Duration::from_millis(200);
const FILE_REFRESH_INTERVAL: Duration = Duration::from_secs(2);
static COMMAND_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub(crate) struct TuiResume {
    pub history: Option<orchestral_core::session_history::SessionHistory>,
    pub run: Option<AgentRunHandle>,
}

pub(crate) async fn run_tui(
    mut client: AgentClient,
    host: &mut Arc<crate::agent::AgentHost>,
    mut options: crate::agent::AgentRunOptions,
    resume: TuiResume,
) -> Result<()> {
    let mut approval_broker = host.approvals.clone();
    let mut process_supervisor = host.process_supervisor.clone();
    let mut terminal = TerminalSession::enter().context("enter TUI terminal mode")?;
    let mut input = EventStream::new();
    let (agent_tx, mut agent_rx) = mpsc::unbounded_channel();
    let mut active = None;
    let mut state = UiState::new(client.session_id().as_str(), host.model.clone());
    state.project = host
        .workspace_root
        .file_name()
        .unwrap_or(host.workspace_root.as_os_str())
        .to_string_lossy()
        .into_owned();
    state.context_budget = Some(host.metadata.context_budget);
    state.color_enabled = std::env::var_os("NO_COLOR").is_none();
    let mut drafts = std::collections::BTreeMap::new();
    let (service_tx, mut service_rx) = mpsc::unbounded_channel();
    let mut service_tasks = ServiceTasks::new(service_tx);
    let (files_tx, mut files_rx) = mpsc::unbounded_channel();
    let mut file_index = super::files::FileIndex::new(host.metadata.workspaces.clone(), files_tx);
    file_index.refresh();
    let mut file_completion_open = false;
    let mut file_refresh_tick = tokio::time::interval(FILE_REFRESH_INTERVAL);
    file_refresh_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    if let Some(history) = &resume.history {
        state.session_title = history.summary.title.clone();
        state.transcript = super::history_entries(history);
        for entry in &state.transcript {
            if entry.role == super::state::TranscriptRole::User {
                state.input_history.push(&entry.text);
            }
        }
        state
            .transcript
            .push(super::state::TranscriptEntry::system(format!(
                "Resumed session {}",
                history.summary.session_id
            )));
    }
    if let Some(handle) = resume.run {
        let last_run_seq = resume
            .history
            .as_ref()
            .and_then(|history| {
                history
                    .runs
                    .iter()
                    .find(|run| run.registration.run_id() == handle.run_id())
            })
            .map(|run| run.last_run_seq())
            .unwrap_or(0);
        let observer = observe_run(&handle, agent_tx.clone()).await?;
        update(
            &mut state,
            UiMsg::RunStarted {
                run_id: handle.run_id().as_str().to_owned(),
            },
        );
        active = Some(ActiveRun {
            handle,
            observer,
            last_run_seq,
            delta_order: 0,
            auto_resolved_approvals: BTreeSet::new(),
        });
        if reconcile_active(&mut active, &mut state, &approval_broker).await? {
            stop_active(&mut active);
        }
    }
    let mut reconcile_tick = tokio::time::interval(RECONCILE_INTERVAL);
    reconcile_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut animation_tick = tokio::time::interval(ANIMATION_INTERVAL);
    animation_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut process_events = process_supervisor.subscribe();
    let mut process_events_open = true;
    let mut cache = RenderCache::default();
    let mut needs_redraw = true;
    let mut quit = false;

    while !quit {
        let had_active_run = active.is_some();
        if needs_redraw {
            let mut anchor = state.viewport.anchor.clone();
            let mut size = state.terminal_size;
            terminal
                .draw(|frame| {
                    size = (frame.area().width, frame.area().height);
                    anchor = render_cached(frame, &state, &mut cache);
                })
                .context("render TUI")?;
            state.terminal_size = size;
            state.viewport.anchor = anchor;
            state.viewport.movement = 0;
            state.transcript_dirty_from = state.transcript.len();
            needs_redraw = false;
        }
        tokio::select! {
            event = input.next() => {
                let event = event.context("terminal event stream closed")?
                    .context("read terminal event")?;
                if matches!(event, Event::Resize(_, _)) {
                    terminal.resize().context("resize TUI")?;
                }
                if let Some(message) = terminal_event_message(event, &state) {
                    if matches!(message, UiMsg::ToggleHelp | UiMsg::Quit) {
                        service_tasks.cancel_read(&mut state);
                    } else if state.menu.is_none() && matches!(message, UiMsg::Escape | UiMsg::Cancel)
                        && service_tasks.cancel_read(&mut state) {
                        needs_redraw = true;
                        continue;
                    }
                    if matches!(message, UiMsg::Submit) && state.menu.is_none() {
                        if let Err(error) = super::menu::validate_references(&state.references, &state.composer) {
                            state.ui_notice = Some(error.to_string()); needs_redraw = true; continue;
                        }
                    }
                    let effects = update(&mut state, message);
                    let mut execution = Vec::new();
                    for effect in effects {
                        if matches!(&effect, UiEffect::MenuChoice { kind: super::menu::MenuKind::Commands, value } if value == "/quit") {
                            service_tasks.cancel_read(&mut state);
                            execution.extend(update(&mut state, UiMsg::Quit));
                        } else if !service_tasks.dispatch(&effect, host.clone(), options.clone(), &mut state) { execution.push(effect); }
                    }
                    let effects = execution;
                    quit = execute_effects(
                        effects,
                        &client,
                        &approval_broker,
                        &agent_tx,
                        &mut active,
                        &mut state,
                    ).await?;
                }
                needs_redraw = true;
            }
            Some(files) = files_rx.recv() => {
                file_index.received();
                update(&mut state, UiMsg::FilesLoaded(files));
                needs_redraw = true;
            }
            _ = file_refresh_tick.tick(), if file_completion_open => {
                file_index.refresh();
            }
            Some((generation, result)) = service_rx.recv() => {
                if !service_tasks.accept(generation) { continue; }
                state.host_busy = false;
                state.ui_notice = None;
                match result {
                    Err(error) => state.ui_notice = Some(error),
                    Ok(super::services::Response::Menu(menu)) => { update(&mut state, UiMsg::OpenMenu(menu)); }
                    Ok(super::services::Response::Notice(message)) => state.ui_notice = Some(message),
                    Ok(super::services::Response::Model { host: next, options: next_options }) => {
                        let old = std::mem::replace(host, next);
                        service_tasks.cleanup.push(tokio::spawn(async move { old.shutdown().await; }));
                        options = next_options;
                        client = host.client(orchestral_core::agent_protocol::wire::AgentSessionId::new(&state.session_id));
                        approval_broker = host.approvals.clone();
                        process_supervisor = host.process_supervisor.clone();
                        process_events = process_supervisor.subscribe();
                        process_events_open = true;
                        state.model = host.model.clone();
                        state.context_budget = Some(host.metadata.context_budget);
                        state.ui_notice = Some("Model selected for the next request".to_owned());
                    }
                    Ok(super::services::Response::Session { client: next, history, run }) => {
                        drafts.insert(state.session_id.clone(), (state.composer.clone(), state.composer_cursor, state.references.clone()));
                        let mut next_state = UiState::new(next.session_id().as_str(), host.model.clone());
                        next_state.terminal_size = state.terminal_size;
                        next_state.exit_requested = state.exit_requested;
                        next_state.context_budget = Some(host.metadata.context_budget);
                        next_state.project = state.project.clone();
                        next_state.files = state.files.clone(); next_state.files_loaded = state.files_loaded;
                        next_state.theme = state.theme.clone(); next_state.color_enabled = state.color_enabled;
                        if let Some((text, cursor, references)) = drafts.remove(next.session_id().as_str()) {
                            next_state.composer = text; next_state.composer_cursor = cursor; next_state.references = references;
                        }
                        if let Some(history) = &history {
                            next_state.session_title = history.summary.title.clone();
                            next_state.transcript = super::history_entries(history);
                            for entry in &next_state.transcript { if entry.role == super::state::TranscriptRole::User { next_state.input_history.push(&entry.text); } }
                            next_state.ui_notice = Some(format!("Resumed · {:?}", history.summary.status));
                        }
                        state = next_state; client = next;
                        if let Some(handle) = run {
                            let last_run_seq = history.as_ref().and_then(|h| h.runs.iter().find(|r| r.registration.run_id() == handle.run_id())).map_or(0, |r| r.last_run_seq());
                            let observer = observe_run(&handle, agent_tx.clone()).await?;
                            update(&mut state, UiMsg::RunStarted { run_id: handle.run_id().as_str().to_owned() });
                            active = Some(ActiveRun { handle, observer, last_run_seq, delta_order: 0, auto_resolved_approvals: BTreeSet::new() });
                            if reconcile_active(&mut active, &mut state, &approval_broker).await? { stop_active(&mut active); }
                        }
                    }
                }
                needs_redraw = true;
            }
            forwarded = agent_rx.recv() => {
                if let Some(forwarded) = forwarded {
                    handle_forwarded(
                        forwarded,
                        &approval_broker,
                        &mut active,
                        &mut state,
                    ).await?;
                    needs_redraw = true;
                }
            }
            _ = reconcile_tick.tick(), if active.is_some() => {
                if reconcile_active(&mut active, &mut state, &approval_broker).await? {
                    stop_active(&mut active);
                }
                needs_redraw = true;
            }
            _ = animation_tick.tick(), if active.is_some() => {
                update(&mut state, UiMsg::Tick { now: Instant::now() });
                needs_redraw = true;
            }
            process_event = process_events.recv(), if active.is_some() && process_events_open => {
                match process_event {
                    Ok(event) => {
                        project_process_event(&mut state, event);
                        needs_redraw = true;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        if let Some(run_id) = state.run_id.as_deref() {
                            let run_id = RunId::new(run_id);
                            if let Ok(sessions) = process_supervisor.list(&run_id) {
                                update(
                                    &mut state,
                                    UiMsg::ProcessInventory {
                                        run_id: run_id.as_str().to_owned(),
                                        session_ids: sessions.into_iter().map(|id| id.get()).collect(),
                                    },
                                );
                                needs_redraw = true;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        process_events_open = false;
                    }
                }
            }
        }
        let completing_files = super::interaction::file_completion_active(&state);
        if (completing_files && !file_completion_open) || (had_active_run && active.is_none()) {
            file_index.refresh();
            file_refresh_tick.reset();
        }
        file_completion_open = completing_files;
        if state.exit_requested && active.is_some() && state.phase != UiPhase::Cancelling {
            let effects = update(&mut state, UiMsg::Quit);
            quit |= execute_effects(
                effects,
                &client,
                &approval_broker,
                &agent_tx,
                &mut active,
                &mut state,
            )
            .await?;
        }
        quit |= state.exit_requested && active.is_none() && !state.host_busy;
    }

    stop_active(&mut active);
    terminal.restore().context("restore terminal after TUI")?;
    service_tasks.finish().await;
    Ok(())
}

fn project_process_event(state: &mut UiState, event: ExecSessionEvent) {
    update(
        state,
        UiMsg::ProcessActivity {
            run_id: event.snapshot.run_id.as_str().to_owned(),
            session_id: event.snapshot.session_id.get(),
            running: matches!(event.snapshot.status, ExecSessionStatus::Running),
        },
    );
}

struct ActiveRun {
    handle: AgentRunHandle,
    observer: JoinHandle<()>,
    last_run_seq: u64,
    delta_order: u64,
    auto_resolved_approvals: BTreeSet<String>,
}

enum ForwardedAgentEvent {
    Event {
        run_id: String,
        event: AgentControlEvent,
    },
    Lagged {
        run_id: String,
    },
    Closed {
        run_id: String,
    },
}

fn terminal_event_message(event: Event, state: &UiState) -> Option<UiMsg> {
    match event {
        Event::Key(key) => key_message(key, state),
        Event::Paste(text) => Some(UiMsg::InsertText(text)),
        // The TUI deliberately does not enable terminal mouse capture. Mouse
        // drag therefore remains the terminal's native text selection/copy
        // gesture; transcript scrolling stays available via PgUp/PgDn.
        Event::Mouse(_) => None,
        Event::Resize(_, _) | Event::FocusGained | Event::FocusLost => None,
    }
}

fn key_message(key: KeyEvent, state: &UiState) -> Option<UiMsg> {
    use super::editor::Edit;
    if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
        return None;
    }
    if key.code == KeyCode::F(1)
        || (key.modifiers.contains(KeyModifiers::CONTROL)
            && matches!(key.code, KeyCode::Char(']' | '5')))
    {
        return Some(UiMsg::ToggleHelp);
    }
    if key.kind == KeyEventKind::Repeat && key.code == KeyCode::Enter {
        return None;
    }
    if state.menu.is_some() {
        return match key.code {
            KeyCode::Esc => Some(UiMsg::Escape),
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Some(UiMsg::Cancel)
            }
            KeyCode::Up => Some(UiMsg::SelectCandidate { up: true }),
            KeyCode::Down => Some(UiMsg::SelectCandidate { up: false }),
            KeyCode::PageUp => Some(UiMsg::ScrollUp(5)),
            KeyCode::PageDown => Some(UiMsg::ScrollDown(5)),
            KeyCode::Enter => Some(UiMsg::Submit),
            KeyCode::Backspace => Some(UiMsg::Backspace),
            KeyCode::Char(c)
                if !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                Some(UiMsg::InsertText(c.to_string()))
            }
            _ => None,
        };
    }
    if key.kind == KeyEventKind::Repeat && matches!(key.code, KeyCode::Enter) {
        return None;
    }
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        return match key.code {
            KeyCode::Char('c' | 'C') => Some(UiMsg::Cancel),
            KeyCode::Char('d' | 'D') if state.composer.is_empty() && state.run_id.is_none() => {
                Some(UiMsg::Quit)
            }
            KeyCode::Char('d' | 'D') => Some(UiMsg::Delete),
            KeyCode::Char('a' | 'A') => Some(UiMsg::MoveCursorStart),
            KeyCode::Char('e' | 'E') => Some(UiMsg::MoveCursorEnd),
            KeyCode::Char('j' | 'J') => Some(UiMsg::InsertText("\n".to_owned())),
            KeyCode::Char('w' | 'W') => Some(UiMsg::Edit(Edit::DeleteWord)),
            KeyCode::Char('u' | 'U') => Some(UiMsg::Edit(Edit::DeleteToStart)),
            KeyCode::Char('k' | 'K') => Some(UiMsg::Edit(Edit::DeleteToEnd)),
            KeyCode::Char('p' | 'P') => Some(UiMsg::ToggleInput),
            KeyCode::Char('o' | 'O') => Some(UiMsg::ToggleTools),
            KeyCode::Left => Some(UiMsg::Edit(Edit::WordLeft)),
            KeyCode::Right => Some(UiMsg::Edit(Edit::WordRight)),
            _ => None,
        };
    }
    if key.modifiers.contains(KeyModifiers::ALT) {
        return match key.code {
            KeyCode::Char('b') | KeyCode::Left => Some(UiMsg::Edit(Edit::WordLeft)),
            KeyCode::Char('f') | KeyCode::Right => Some(UiMsg::Edit(Edit::WordRight)),
            KeyCode::Backspace => Some(UiMsg::Edit(Edit::DeleteWord)),
            _ => None,
        };
    }
    if state.phase == UiPhase::WaitingApproval {
        let session_available = matches!(
            &state.pending,
            Some(super::state::PendingOverlay::Approval {
                session_approval_available: true,
                ..
            })
        );
        return match key.code {
            KeyCode::Char('a' | 'A') => Some(UiMsg::Approval(ApprovalChoice::Allow)),
            KeyCode::Char('s' | 'S') if session_available => {
                Some(UiMsg::Approval(ApprovalChoice::AllowSession))
            }
            KeyCode::Char('d' | 'D') => Some(UiMsg::Approval(ApprovalChoice::Deny)),
            KeyCode::Up => Some(UiMsg::SelectApproval(previous_approval_choice(
                state.approval_choice,
                session_available,
            ))),
            KeyCode::Down => Some(UiMsg::SelectApproval(next_approval_choice(
                state.approval_choice,
                session_available,
            ))),
            KeyCode::Enter if state.approval_selected => {
                Some(UiMsg::Approval(state.approval_choice))
            }
            KeyCode::Esc => Some(UiMsg::Escape),
            KeyCode::PageUp => Some(UiMsg::ScrollUp(5)),
            KeyCode::PageDown => Some(UiMsg::ScrollDown(5)),
            KeyCode::End => Some(UiMsg::FollowOutput),
            _ => None,
        };
    }
    match key.code {
        KeyCode::Esc => Some(UiMsg::Escape),
        KeyCode::Enter if key.modifiers.contains(KeyModifiers::SHIFT) => {
            Some(UiMsg::InsertText("\n".to_owned()))
        }
        KeyCode::Enter => Some(UiMsg::Submit),
        KeyCode::Backspace => Some(UiMsg::Backspace),
        KeyCode::Delete => Some(UiMsg::Delete),
        KeyCode::Left => Some(UiMsg::MoveCursorLeft),
        KeyCode::Right => Some(UiMsg::MoveCursorRight),
        KeyCode::Home => Some(UiMsg::MoveCursorStart),
        KeyCode::End => Some(UiMsg::FollowOutput),
        KeyCode::PageUp => Some(UiMsg::ScrollUp(5)),
        KeyCode::PageDown => Some(UiMsg::ScrollDown(5)),
        KeyCode::Up => Some(UiMsg::History { up: true }),
        KeyCode::Down => Some(UiMsg::History { up: false }),
        KeyCode::Tab => Some(UiMsg::Complete),
        KeyCode::Char(character) => Some(UiMsg::InsertText(character.to_string())),
        _ => None,
    }
}

fn next_approval_choice(current: ApprovalChoice, session_available: bool) -> ApprovalChoice {
    match (current, session_available) {
        (ApprovalChoice::Allow, true) => ApprovalChoice::AllowSession,
        (ApprovalChoice::Allow, false) | (ApprovalChoice::AllowSession, _) => ApprovalChoice::Deny,
        (ApprovalChoice::Deny, _) => ApprovalChoice::Allow,
    }
}

fn previous_approval_choice(current: ApprovalChoice, session_available: bool) -> ApprovalChoice {
    match (current, session_available) {
        (ApprovalChoice::Allow, _) => ApprovalChoice::Deny,
        (ApprovalChoice::AllowSession, _) => ApprovalChoice::Allow,
        (ApprovalChoice::Deny, true) => ApprovalChoice::AllowSession,
        (ApprovalChoice::Deny, false) => ApprovalChoice::Allow,
    }
}

async fn execute_effects(
    effects: Vec<UiEffect>,
    client: &AgentClient,
    approval_broker: &Arc<InMemoryHostApprovalBroker>,
    agent_tx: &mpsc::UnboundedSender<ForwardedAgentEvent>,
    active: &mut Option<ActiveRun>,
    state: &mut UiState,
) -> Result<bool> {
    for effect in effects {
        match effect {
            UiEffect::HostCommand { .. }
            | UiEffect::MenuChoice { .. }
            | UiEffect::LocalAction { .. } => {
                unreachable!("Host commands are dispatched before execution")
            }
            UiEffect::StartRun { input } => {
                if active.is_some() {
                    notice(state, "start-active", "A Run is already active", true);
                    continue;
                }
                match client.start_text(input.clone()).await {
                    Ok(handle) => {
                        let run_id = handle.run_id().as_str().to_owned();
                        match observe_run(&handle, agent_tx.clone()).await {
                            Ok(observer) => {
                                *active = Some(ActiveRun {
                                    handle,
                                    observer,
                                    last_run_seq: 0,
                                    delta_order: 0,
                                    auto_resolved_approvals: BTreeSet::new(),
                                });
                                update(state, UiMsg::RunStarted { run_id });
                                if reconcile_active(active, state, approval_broker).await? {
                                    stop_active(active);
                                }
                            }
                            Err(error) => {
                                update(
                                    state,
                                    UiMsg::Failed {
                                        message: format!("could not observe Run: {error}"),
                                    },
                                );
                            }
                        }
                    }
                    Err(error) => {
                        update(
                            state,
                            UiMsg::Failed {
                                message: format!("could not start Run: {error}"),
                            },
                        );
                        restore_submission(state, &input);
                    }
                }
            }
            UiEffect::Steer { run_id, input } => {
                if let Some(run) = matching_active(active, &run_id, state) {
                    match run.handle.steer_text(input.clone()).await {
                        Ok(ack) => {
                            if !matches!(
                                ack.state,
                                CommandAckState::Accepted { .. } | CommandAckState::Applied { .. }
                            ) {
                                restore_submission(state, &input);
                            }
                            project_ack(state, ack, "steer");
                        }
                        Err(error) => {
                            restore_submission(state, &input);
                            notice(state, "steer-error", error.to_string(), true);
                        }
                    }
                } else {
                    restore_submission(state, &input);
                }
            }
            UiEffect::ResolveInput {
                run_id,
                request_id,
                value,
            } => {
                if let Some(run) = matching_active(active, &run_id, state) {
                    match run
                        .handle
                        .resolve_input_text(RequestId::new(&request_id), value.clone())
                        .await
                    {
                        Ok(ack) => {
                            project_request_ack(state, &request_id, ack, "input");
                        }
                        Err(error) => {
                            update(state, UiMsg::RequestSubmissionFailed { request_id });
                            notice(state, "input-error", error.to_string(), true);
                        }
                    }
                } else {
                    update(state, UiMsg::RequestSubmissionFailed { request_id });
                }
            }
            UiEffect::ResolveApproval {
                run_id,
                request_id,
                choice,
            } => {
                if let Some(run) = matching_active(active, &run_id, state) {
                    let request_id = RequestId::new(request_id);
                    let response = match choice {
                        ApprovalChoice::Allow | ApprovalChoice::AllowSession => {
                            let grant = if choice == ApprovalChoice::AllowSession {
                                approval_broker
                                    .approve_for_session(&request_id, approval_expiry_ms())
                            } else {
                                approval_broker.approve(&request_id, approval_expiry_ms())
                            };
                            match grant {
                                Ok(grant_ref) => RequestResolution::Approval {
                                    decision: ApprovalDecision::Allow,
                                    grant_ref: Some(grant_ref),
                                },
                                Err(error) => {
                                    update(
                                        state,
                                        UiMsg::RequestSubmissionFailed {
                                            request_id: request_id.as_str().to_owned(),
                                        },
                                    );
                                    notice(state, "approval-error", error.to_string(), true);
                                    continue;
                                }
                            }
                        }
                        ApprovalChoice::Deny => RequestResolution::Approval {
                            decision: ApprovalDecision::Deny,
                            grant_ref: None,
                        },
                    };
                    let command = AgentCommandEnvelope::new(
                        next_command_id("approval"),
                        run.handle.run_id().clone(),
                        Some(request_id.clone()),
                        AgentCommand::ResolveRequest { response },
                    )
                    .context("build TUI approval command")?;
                    match run.handle.command(command).await {
                        Ok(ack) => project_request_ack(state, request_id.as_str(), ack, "approval"),
                        Err(error) => {
                            update(
                                state,
                                UiMsg::RequestSubmissionFailed {
                                    request_id: request_id.as_str().to_owned(),
                                },
                            );
                            notice(state, "approval-command-error", error.to_string(), true)
                        }
                    }
                } else {
                    update(state, UiMsg::RequestSubmissionFailed { request_id });
                }
            }
            UiEffect::CancelRun { run_id } => {
                if let Some(run) = matching_active(active, &run_id, state) {
                    match run
                        .handle
                        .cancel("TUI cancellation requested by user")
                        .await
                    {
                        Ok(ack) => project_ack(state, ack, "cancel"),
                        Err(error) => notice(state, "cancel-error", error.to_string(), true),
                    }
                }
            }
            UiEffect::Quit => {
                if let Some(run) = active.as_ref() {
                    let _ = run.handle.cancel("TUI exited by user").await;
                }
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn matching_active<'a>(
    active: &'a mut Option<ActiveRun>,
    run_id: &str,
    state: &mut UiState,
) -> Option<&'a mut ActiveRun> {
    match active.as_mut() {
        Some(run) if run.handle.run_id().as_str() == run_id => Some(run),
        _ => {
            notice(
                state,
                "stale-action",
                "Ignored an action for a Run that is no longer active",
                true,
            );
            None
        }
    }
}

async fn observe_run(
    handle: &AgentRunHandle,
    sender: mpsc::UnboundedSender<ForwardedAgentEvent>,
) -> Result<JoinHandle<()>> {
    let mut receiver = handle.subscribe().await.context("subscribe to Agent Run")?;
    let run_id = handle.run_id().as_str().to_owned();
    Ok(tokio::spawn(async move {
        loop {
            match receiver.recv().await {
                Ok(event) => {
                    if sender
                        .send(ForwardedAgentEvent::Event {
                            run_id: run_id.clone(),
                            event,
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    if sender
                        .send(ForwardedAgentEvent::Lagged {
                            run_id: run_id.clone(),
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    let _ = sender.send(ForwardedAgentEvent::Closed { run_id });
                    return;
                }
            }
        }
    }))
}

async fn handle_forwarded(
    forwarded: ForwardedAgentEvent,
    approval_broker: &Arc<InMemoryHostApprovalBroker>,
    active: &mut Option<ActiveRun>,
    state: &mut UiState,
) -> Result<()> {
    let run_id = match &forwarded {
        ForwardedAgentEvent::Event { run_id, .. }
        | ForwardedAgentEvent::Lagged { run_id }
        | ForwardedAgentEvent::Closed { run_id } => run_id,
    };
    if active
        .as_ref()
        .is_none_or(|run| run.handle.run_id().as_str() != run_id)
    {
        return Ok(());
    }

    match forwarded {
        ForwardedAgentEvent::Event {
            event: AgentControlEvent::Telemetry(telemetry),
            ..
        } => project_telemetry(active.as_mut().expect("active checked"), state, telemetry),
        ForwardedAgentEvent::Event {
            event: AgentControlEvent::Durable(_),
            ..
        }
        | ForwardedAgentEvent::Lagged { .. } => {
            if reconcile_active(active, state, approval_broker).await? {
                stop_active(active);
            }
        }
        ForwardedAgentEvent::Closed { .. } => {
            if reconcile_active(active, state, approval_broker).await? {
                stop_active(active);
            } else {
                notice(
                    state,
                    "control-stream-closed",
                    "Agent control stream closed before a terminal result",
                    true,
                );
            }
        }
        ForwardedAgentEvent::Event { .. } => {}
    }
    Ok(())
}

async fn reconcile_active(
    active: &mut Option<ActiveRun>,
    state: &mut UiState,
    approval_broker: &Arc<InMemoryHostApprovalBroker>,
) -> Result<bool> {
    let Some(run) = active.as_mut() else {
        return Ok(false);
    };
    let records = run
        .handle
        .events(run.last_run_seq)
        .await
        .context("read durable Agent events")?;
    let mut terminal = false;
    for record in records {
        if record.event.run_seq <= run.last_run_seq {
            continue;
        }
        run.last_run_seq = record.event.run_seq;
        terminal |= project_durable(state, &record);
    }
    if terminal {
        return Ok(true);
    }

    let view = run.handle.inspect().await.context("inspect Agent Run")?;
    match &view.state {
        AgentRunState::Stopping => {
            update(state, UiMsg::Stopping);
        }
        AgentRunState::Unknown { reason, .. } => notice(
            state,
            "continuity-unknown",
            format!("Run continuity is unknown: {reason}"),
            true,
        ),
        AgentRunState::Terminal { terminal } => {
            project_terminal_view(
                state,
                terminal,
                view.delivery.as_ref(),
                view.partial_delivery.as_ref(),
            );
            return Ok(true);
        }
        _ if state.phase != UiPhase::Cancelling => {
            if let Some(request) = view.pending_requests.first() {
                if !try_resolve_remembered_approval(run, state, approval_broker, request).await? {
                    project_pending(state, run.handle.run_id(), request);
                }
            }
        }
        _ => {}
    }
    Ok(false)
}

async fn try_resolve_remembered_approval(
    run: &mut ActiveRun,
    state: &mut UiState,
    approval_broker: &Arc<InMemoryHostApprovalBroker>,
    request: &PendingRequest,
) -> Result<bool> {
    if !matches!(
        &request.payload,
        PendingRequestPayload::Approval {
            session_approval_scope: Some(_),
            ..
        }
    ) {
        return Ok(false);
    }
    let request_key = request.request_id.as_str().to_owned();
    if run.auto_resolved_approvals.contains(&request_key) {
        return Ok(true);
    }
    let grant_ref =
        match approval_broker.approve_if_remembered(&request.request_id, approval_expiry_ms()) {
            Ok(Some(grant_ref)) => grant_ref,
            Ok(None) => return Ok(false),
            Err(error) => {
                notice(
                    state,
                    "session-approval-error",
                    format!("Could not apply remembered approval: {error}"),
                    true,
                );
                return Ok(false);
            }
        };
    let response = RequestResolution::Approval {
        decision: ApprovalDecision::Allow,
        grant_ref: Some(grant_ref),
    };
    let command = AgentCommandEnvelope::new(
        next_command_id("session-approval"),
        run.handle.run_id().clone(),
        Some(request.request_id.clone()),
        AgentCommand::ResolveRequest { response },
    )
    .context("build remembered TUI approval command")?;
    let ack = run
        .handle
        .command(command)
        .await
        .context("resolve remembered TUI approval")?;
    let accepted = matches!(
        &ack.state,
        CommandAckState::Accepted { .. } | CommandAckState::Applied { .. }
    );
    project_ack(state, ack, "remembered approval");
    if accepted {
        run.auto_resolved_approvals.insert(request_key);
        update(
            state,
            UiMsg::RequestResolved {
                request_id: request.request_id.as_str().to_owned(),
            },
        );
    }
    Ok(accepted)
}

fn project_durable(state: &mut UiState, record: &AgentJournalRecord) -> bool {
    match &record.event.payload {
        AgentEvent::RunStarted => {
            update(
                state,
                UiMsg::RunStarted {
                    run_id: record.event.run_id.as_str().to_owned(),
                },
            );
        }
        AgentEvent::OutputCommitted { output_id, content } => {
            update(
                state,
                UiMsg::OutputCommitted {
                    output_id: output_id.as_str().to_owned(),
                    text: display_contents(content),
                },
            );
        }
        AgentEvent::RequestOpened { request } => {
            project_pending(state, &record.event.run_id, request);
        }
        AgentEvent::RequestResolved { request_id, .. }
        | AgentEvent::RequestClosed { request_id, .. } => {
            update(
                state,
                UiMsg::RequestResolved {
                    request_id: request_id.as_str().to_owned(),
                },
            );
        }
        AgentEvent::StopRequested { .. } => {
            update(state, UiMsg::Stopping);
        }
        AgentEvent::DeliveryCommitted { delivery } => {
            update(
                state,
                UiMsg::Completed {
                    final_text: Some(display_content(&delivery.final_response)),
                },
            );
            return true;
        }
        AgentEvent::RunIncomplete {
            reason,
            partial_delivery,
        } => {
            let partial = partial_delivery
                .as_ref()
                .and_then(|delivery| delivery.response.as_ref())
                .map(display_content)
                .filter(|text| !text.is_empty());
            let message = match partial {
                Some(partial) => format!("{reason:?}\nPartial output: {partial}"),
                None => format!("{reason:?}"),
            };
            update(state, UiMsg::Incomplete { message });
            return true;
        }
        AgentEvent::RunFailed { failure } => {
            update(
                state,
                UiMsg::Failed {
                    message: format!("[{}] {}", failure.code, failure.message),
                },
            );
            return true;
        }
        AgentEvent::RunCancelled { reason } => {
            update(
                state,
                UiMsg::Cancelled {
                    reason: reason.clone(),
                },
            );
            return true;
        }
        AgentEvent::ContinuityLost { reason, .. } => notice(
            state,
            "continuity-lost",
            format!("Run continuity lost: {reason}"),
            true,
        ),
        AgentEvent::ContinuityRestored { reason, .. } => notice(
            state,
            "continuity-restored",
            format!("Run continuity restored: {reason}"),
            false,
        ),
        _ => {}
    }
    false
}

fn project_pending(state: &mut UiState, run_id: &RunId, request: &PendingRequest) {
    match &request.payload {
        PendingRequestPayload::Input { prompt, .. } => {
            update(
                state,
                UiMsg::WaitingInput {
                    run_id: run_id.as_str().to_owned(),
                    request_id: request.request_id.as_str().to_owned(),
                    prompt: display_contents(prompt),
                },
            );
        }
        PendingRequestPayload::Approval {
            requested_scope,
            session_approval_scope,
            reason,
            ..
        } => {
            update(
                state,
                UiMsg::WaitingApproval {
                    run_id: run_id.as_str().to_owned(),
                    request_id: request.request_id.as_str().to_owned(),
                    summary: format!("{reason}\nEffects: {}", requested_scope.join(", ")),
                    session_approval_available: session_approval_scope.is_some(),
                },
            );
        }
        PendingRequestPayload::ExternalAction { name, .. } => notice(
            state,
            "external-action-unsupported",
            format!("External action '{name}' is not supported by this TUI"),
            true,
        ),
        _ => notice(
            state,
            "pending-request-unsupported",
            "This TUI does not support the requested interaction type",
            true,
        ),
    }
}

fn project_terminal_view(
    state: &mut UiState,
    terminal: &AgentTerminalState,
    delivery: Option<&orchestral_core::agent_protocol::wire::AgentDelivery>,
    partial: Option<&orchestral_core::agent_protocol::wire::PartialDelivery>,
) {
    match terminal {
        AgentTerminalState::Delivered { .. } => update(
            state,
            UiMsg::Completed {
                final_text: delivery.map(|delivery| display_content(&delivery.final_response)),
            },
        ),
        AgentTerminalState::Incomplete { reason } => update(
            state,
            UiMsg::Incomplete {
                message: partial
                    .and_then(|partial| partial.response.as_ref())
                    .map(|content| {
                        format!("{reason:?}\nPartial output: {}", display_content(content))
                    })
                    .unwrap_or_else(|| format!("{reason:?}")),
            },
        ),
        AgentTerminalState::Cancelled { reason } => update(
            state,
            UiMsg::Cancelled {
                reason: reason.clone(),
            },
        ),
        AgentTerminalState::Failed { failure } => update(
            state,
            UiMsg::Failed {
                message: format!("[{}] {}", failure.code, failure.message),
            },
        ),
        _ => update(
            state,
            UiMsg::Failed {
                message: "Run ended in an unsupported terminal state".to_owned(),
            },
        ),
    };
}

fn project_telemetry(run: &mut ActiveRun, state: &mut UiState, telemetry: AgentTelemetryEnvelope) {
    let telemetry_id = telemetry.telemetry_id.as_str().to_owned();
    match telemetry.payload {
        AgentTelemetry::OutputDelta { output_id, delta } => {
            let text = display_content(&delta);
            if !text.is_empty() {
                run.delta_order = run.delta_order.saturating_add(1);
                update(
                    state,
                    UiMsg::StreamDelta {
                        delta_id: telemetry_id,
                        output_id: output_id.as_str().to_owned(),
                        order: run.delta_order,
                        text,
                    },
                );
            }
        }
        AgentTelemetry::ProgressReported { message, fraction } => {
            let summary = fraction
                .map(|fraction| format!("{:.0}% {message}", fraction * 100.0))
                .unwrap_or(message);
            update(state, UiMsg::ProgressReported { summary });
        }
        AgentTelemetry::ToolActivity {
            activity_id,
            tool_name,
            state: activity_state,
            evidence,
        } => {
            update(
                state,
                UiMsg::ToolActivity {
                    activity_id: activity_id.as_str().to_owned(),
                    tool_name,
                    state: activity_state,
                    evidence,
                },
            );
        }
        _ => {}
    }
}

fn restore_submission(state: &mut UiState, input: &str) {
    if state.composer.is_empty() {
        state.composer = input.to_owned();
        state.composer_cursor = input.len();
    } else {
        state.menu = Some(super::services::detail(
            "Input was not accepted · original draft retained",
            input,
        ));
    }
}

fn project_request_ack(state: &mut UiState, request_id: &str, ack: CommandAck, operation: &str) {
    let request_id = request_id.to_owned();
    let message = if matches!(
        ack.state,
        CommandAckState::Accepted { .. } | CommandAckState::Applied { .. }
    ) {
        UiMsg::RequestSubmissionAccepted { request_id }
    } else {
        UiMsg::RequestSubmissionFailed { request_id }
    };
    update(state, message);
    project_ack(state, ack, operation);
}

fn project_ack(state: &mut UiState, ack: CommandAck, operation: &str) {
    match ack.state {
        CommandAckState::Accepted { .. } => state.ui_notice = Some(format!("{operation} accepted")),
        CommandAckState::Applied { .. } => state.ui_notice = Some(format!("{operation} applied")),
        CommandAckState::Rejected { code, message, .. } => notice(
            state,
            format!("{operation}-rejected"),
            format!("{operation} rejected ({code:?}): {message}"),
            true,
        ),
        CommandAckState::Unsupported { feature, .. } => notice(
            state,
            format!("{operation}-unsupported"),
            format!("{operation} unsupported: {feature}"),
            true,
        ),
        _ => notice(
            state,
            format!("{operation}-ack-unknown"),
            format!("{operation} returned an unsupported acknowledgement"),
            true,
        ),
    }
}

fn display_contents(contents: &[Content]) -> String {
    contents
        .iter()
        .map(display_content)
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn display_content(content: &Content) -> String {
    match &content.body {
        ContentBody::Inline(serde_json::Value::String(text)) => text.clone(),
        body => serde_json::to_string(body).unwrap_or_else(|_| "<unprintable content>".to_owned()),
    }
}

fn notice(state: &mut UiState, id: impl Into<String>, message: impl Into<String>, is_error: bool) {
    update(
        state,
        UiMsg::Notice {
            id: id.into(),
            message: message.into(),
            is_error,
        },
    );
}

fn next_command_id(kind: &str) -> CommandId {
    CommandId::new(format!(
        "tui-{kind}-{}-{}",
        std::process::id(),
        COMMAND_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

fn approval_expiry_ms() -> i64 {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    now_ms.saturating_add(5 * 60 * 1_000)
}

fn stop_active(active: &mut Option<ActiveRun>) {
    if let Some(run) = active.take() {
        run.observer.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::key_message;
    use crate::tui::{ApprovalChoice, UiMsg, UiPhase, UiState};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    #[test]
    fn approval_keys_cannot_become_composer_text() {
        let mut state = UiState::new("session", "model");
        state.phase = UiPhase::WaitingApproval;
        assert_eq!(
            key_message(
                KeyEvent::new(KeyCode::Char('a'), KeyModifiers::NONE),
                &state
            ),
            Some(UiMsg::Approval(ApprovalChoice::Allow))
        );
        assert_eq!(
            key_message(
                KeyEvent::new(KeyCode::Char('d'), KeyModifiers::NONE),
                &state
            ),
            Some(UiMsg::Approval(ApprovalChoice::Deny))
        );
        assert_eq!(
            key_message(
                KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE),
                &state
            ),
            None
        );

        assert_eq!(
            key_message(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE), &state),
            Some(UiMsg::SelectApproval(ApprovalChoice::Deny))
        );
        crate::tui::update(&mut state, UiMsg::SelectApproval(ApprovalChoice::Deny));
        assert_eq!(
            key_message(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), &state),
            Some(UiMsg::Approval(ApprovalChoice::Deny))
        );
        assert_eq!(
            key_message(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE), &state),
            Some(UiMsg::SelectApproval(ApprovalChoice::Allow))
        );

        crate::tui::update(
            &mut state,
            UiMsg::WaitingApproval {
                run_id: "run".to_owned(),
                request_id: "mcp-approval".to_owned(),
                summary: "Call a risky MCP Tool".to_owned(),
                session_approval_available: true,
            },
        );
        assert_eq!(
            key_message(
                KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE),
                &state
            ),
            Some(UiMsg::Approval(ApprovalChoice::AllowSession))
        );
        assert_eq!(
            key_message(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE), &state),
            Some(UiMsg::SelectApproval(ApprovalChoice::AllowSession))
        );
        crate::tui::update(
            &mut state,
            UiMsg::SelectApproval(ApprovalChoice::AllowSession),
        );
        assert_eq!(
            key_message(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE), &state),
            Some(UiMsg::SelectApproval(ApprovalChoice::Deny))
        );
    }

    #[test]
    fn bracketed_paste_payload_remains_one_insert_message() {
        let state = UiState::new("session", "model");
        assert_eq!(
            super::terminal_event_message(
                crossterm::event::Event::Paste("中文\nemoji 🚀".to_owned()),
                &state,
            ),
            Some(UiMsg::InsertText("中文\nemoji 🚀".to_owned()))
        );
    }
}
