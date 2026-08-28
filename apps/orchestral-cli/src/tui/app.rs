use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use crossterm::event::{
    Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseEventKind,
};
use futures_util::StreamExt;
use orchestral_core::agent_protocol::wire::{
    AgentCommand, AgentCommandEnvelope, AgentEvent, AgentJournalRecord, AgentRunState,
    AgentTelemetry, AgentTelemetryEnvelope, AgentTerminalState, ApprovalDecision, CommandAck,
    CommandAckState, CommandId, Content, ContentBody, PendingRequest, PendingRequestPayload,
    RequestId, RequestResolution, RunId,
};
use orchestral_runtime::{
    AgentClient, AgentControlEvent, AgentRunHandle, ExecSessionEvent, ExecSessionStatus,
    InMemoryHostApprovalBroker, ProcessSupervisor,
};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use super::terminal::TerminalSession;
use super::{render, update, ApprovalChoice, UiEffect, UiMsg, UiPhase, UiState};

const RECONCILE_INTERVAL: Duration = Duration::from_millis(200);
const ANIMATION_INTERVAL: Duration = Duration::from_millis(80);
static COMMAND_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub(crate) async fn run_tui(
    client: AgentClient,
    approval_broker: Arc<InMemoryHostApprovalBroker>,
    process_supervisor: Arc<ProcessSupervisor>,
    model: String,
) -> Result<()> {
    let mut terminal = TerminalSession::enter().context("enter TUI terminal mode")?;
    let mut input = EventStream::new();
    let (agent_tx, mut agent_rx) = mpsc::unbounded_channel();
    let mut active = None;
    let mut state = UiState::new(client.session_id().as_str(), model);
    let mut reconcile_tick = tokio::time::interval(RECONCILE_INTERVAL);
    reconcile_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut animation_tick = tokio::time::interval(ANIMATION_INTERVAL);
    animation_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut process_events = process_supervisor.subscribe();
    let mut quit = false;

    while !quit {
        terminal
            .draw(|frame| render(frame, &state))
            .context("render TUI")?;
        tokio::select! {
            event = input.next() => {
                let event = event.context("terminal event stream closed")?
                    .context("read terminal event")?;
                if matches!(event, Event::Resize(_, _)) {
                    terminal.resize().context("resize TUI")?;
                }
                if let Some(message) = terminal_event_message(event, &state) {
                    let effects = update(&mut state, message);
                    quit = execute_effects(
                        effects,
                        &client,
                        &approval_broker,
                        &agent_tx,
                        &mut active,
                        &mut state,
                    ).await?;
                }
            }
            forwarded = agent_rx.recv() => {
                if let Some(forwarded) = forwarded {
                    handle_forwarded(forwarded, &mut active, &mut state).await?;
                }
            }
            _ = reconcile_tick.tick(), if active.is_some() => {
                if reconcile_active(&mut active, &mut state).await? {
                    stop_active(&mut active);
                }
            }
            _ = animation_tick.tick(), if active.is_some() => {
                update(&mut state, UiMsg::Tick { now: Instant::now() });
            }
            process_event = process_events.recv(), if active.is_some() => {
                match process_event {
                    Ok(event) => project_process_event(&mut state, event),
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
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => {}
                }
            }
        }
    }

    stop_active(&mut active);
    terminal.restore().context("restore terminal after TUI")
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
        Event::Mouse(mouse) => match mouse.kind {
            MouseEventKind::ScrollUp => Some(UiMsg::ScrollUp(3)),
            MouseEventKind::ScrollDown => Some(UiMsg::ScrollDown(3)),
            _ => None,
        },
        Event::Resize(_, _) | Event::FocusGained | Event::FocusLost => None,
    }
}

fn key_message(key: KeyEvent, state: &UiState) -> Option<UiMsg> {
    if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
        return None;
    }
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        return match key.code {
            KeyCode::Char('c') | KeyCode::Char('C') => Some(UiMsg::Cancel),
            KeyCode::Char('a') | KeyCode::Char('A') => Some(UiMsg::MoveCursorStart),
            KeyCode::Char('e') | KeyCode::Char('E') => Some(UiMsg::MoveCursorEnd),
            _ => None,
        };
    }
    if state.phase == UiPhase::WaitingApproval {
        return match key.code {
            KeyCode::Char('a') | KeyCode::Char('A') => Some(UiMsg::Approval(ApprovalChoice::Allow)),
            KeyCode::Char('d') | KeyCode::Char('D') => Some(UiMsg::Approval(ApprovalChoice::Deny)),
            KeyCode::Esc => Some(UiMsg::Quit),
            KeyCode::PageUp | KeyCode::Up => Some(UiMsg::ScrollUp(5)),
            KeyCode::PageDown | KeyCode::Down => Some(UiMsg::ScrollDown(5)),
            _ => None,
        };
    }
    match key.code {
        KeyCode::Esc => Some(UiMsg::Quit),
        KeyCode::Enter
            if key
                .modifiers
                .intersects(KeyModifiers::SHIFT | KeyModifiers::ALT) =>
        {
            Some(UiMsg::InsertText("\n".to_owned()))
        }
        KeyCode::Enter => Some(UiMsg::Submit),
        KeyCode::Backspace => Some(UiMsg::Backspace),
        KeyCode::Delete => Some(UiMsg::Delete),
        KeyCode::Left => Some(UiMsg::MoveCursorLeft),
        KeyCode::Right => Some(UiMsg::MoveCursorRight),
        KeyCode::Home => Some(UiMsg::MoveCursorStart),
        KeyCode::End => Some(UiMsg::MoveCursorEnd),
        KeyCode::PageUp | KeyCode::Up => Some(UiMsg::ScrollUp(5)),
        KeyCode::PageDown | KeyCode::Down => Some(UiMsg::ScrollDown(5)),
        KeyCode::Tab => Some(UiMsg::InsertText("    ".to_owned())),
        KeyCode::Char(character) => Some(UiMsg::InsertText(character.to_string())),
        _ => None,
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
            UiEffect::StartRun { input } => {
                if active.is_some() {
                    notice(state, "start-active", "A Run is already active", true);
                    continue;
                }
                match client.start_text(input).await {
                    Ok(handle) => {
                        let run_id = handle.run_id().as_str().to_owned();
                        match observe_run(&handle, agent_tx.clone()).await {
                            Ok(observer) => {
                                *active = Some(ActiveRun {
                                    handle,
                                    observer,
                                    last_run_seq: 0,
                                    delta_order: 0,
                                });
                                update(state, UiMsg::RunStarted { run_id });
                                if reconcile_active(active, state).await? {
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
                    }
                }
            }
            UiEffect::Steer { run_id, input } => {
                if let Some(run) = matching_active(active, &run_id, state) {
                    match run.handle.steer_text(input).await {
                        Ok(ack) => project_ack(state, ack, "steer"),
                        Err(error) => notice(state, "steer-error", error.to_string(), true),
                    }
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
                        .resolve_input_text(RequestId::new(request_id), value)
                        .await
                    {
                        Ok(ack) => project_ack(state, ack, "input"),
                        Err(error) => notice(state, "input-error", error.to_string(), true),
                    }
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
                        ApprovalChoice::Allow => {
                            let now_ms = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as i64;
                            match approval_broker
                                .approve(&request_id, now_ms.saturating_add(5 * 60 * 1_000))
                            {
                                Ok(grant_ref) => RequestResolution::Approval {
                                    decision: ApprovalDecision::Allow,
                                    grant_ref: Some(grant_ref),
                                },
                                Err(error) => {
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
                        Some(request_id),
                        AgentCommand::ResolveRequest { response },
                    )
                    .context("build TUI approval command")?;
                    match run.handle.command(command).await {
                        Ok(ack) => project_ack(state, ack, "approval"),
                        Err(error) => {
                            notice(state, "approval-command-error", error.to_string(), true)
                        }
                    }
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
            if reconcile_active(active, state).await? {
                stop_active(active);
            }
        }
        ForwardedAgentEvent::Closed { .. } => {
            if reconcile_active(active, state).await? {
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

async fn reconcile_active(active: &mut Option<ActiveRun>, state: &mut UiState) -> Result<bool> {
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
                project_pending(state, run.handle.run_id(), request);
            }
        }
        _ => {}
    }
    Ok(false)
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
        AgentEvent::RequestResolved { request_id, .. } => {
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
            update(state, UiMsg::Failed { message });
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
            reason,
            ..
        } => {
            update(
                state,
                UiMsg::WaitingApproval {
                    run_id: run_id.as_str().to_owned(),
                    request_id: request.request_id.as_str().to_owned(),
                    summary: format!("{reason}\nEffects: {}", requested_scope.join(", ")),
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
            UiMsg::Failed {
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
            details,
        } => {
            update(
                state,
                UiMsg::ToolActivity {
                    activity_id: activity_id.as_str().to_owned(),
                    tool_name,
                    state: activity_state,
                    details,
                },
            );
        }
        _ => {}
    }
}

fn project_ack(state: &mut UiState, ack: CommandAck, operation: &str) {
    match ack.state {
        CommandAckState::Accepted { .. } | CommandAckState::Applied { .. } => {}
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

fn stop_active(active: &mut Option<ActiveRun>) {
    if let Some(run) = active.take() {
        run.observer.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::key_message;
    use crate::tui::{ApprovalChoice, UiMsg, UiPhase, UiState};
    use crossterm::event::{
        KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
    };

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

    #[test]
    fn mouse_wheel_maps_to_bounded_transcript_scroll() {
        let state = UiState::new("session", "model");
        let event = |kind| {
            crossterm::event::Event::Mouse(MouseEvent {
                kind,
                column: 0,
                row: 0,
                modifiers: KeyModifiers::NONE,
            })
        };
        assert_eq!(
            super::terminal_event_message(event(MouseEventKind::ScrollUp), &state),
            Some(UiMsg::ScrollUp(3))
        );
        assert_eq!(
            super::terminal_event_message(event(MouseEventKind::ScrollDown), &state),
            Some(UiMsg::ScrollDown(3))
        );
        assert_eq!(
            super::terminal_event_message(event(MouseEventKind::Down(MouseButton::Left)), &state),
            None
        );
    }
}
