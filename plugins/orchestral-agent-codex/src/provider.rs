use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use futures_util::stream::{self, StreamExt};
use orchestral_core::agent_connector::{
    AgentSessionActionInvocation, SESSION_COMPACT_ACTION, SESSION_REVIEW_ACTION,
};
use orchestral_core::agent_protocol::spi::{
    AgentProvider, AgentProviderStream, AgentRecovery, AgentRecoveryRequest, AgentStart,
    AgentStartError,
};
use orchestral_core::agent_protocol::wire::{
    AgentAdmission, AgentCapabilities, AgentCommand, AgentCommandEnvelope, AgentDelivery,
    AgentDescriptor, AgentDescriptorEnvelope, AgentEvent, AgentEventDraft, AgentEventId,
    AgentExecutionRef, AgentFailure, AgentId, AgentProtocolError, AgentProtocolErrorCode,
    AgentProviderId, AgentProviderStreamItem, AgentRejection, AgentRejectionCode, AgentSessionId,
    AgentStartRequest, AgentTelemetry, AgentTelemetryEnvelope, ApprovalDecision, CancelSupport,
    Content, ContentBody, ControlCapabilities, DeliveryId, Digest, EffectMediation, Extensions,
    OutputId, PendingRequest, PendingRequestKind, PendingRequestPayload, ProtocolVersion,
    Provenance, ProviderCommandDisposition, ProviderCommandOutcome, RequestId, RequestResolution,
    RunId, TelemetryId, ToolActivityEvidence, ToolActivityId, ToolActivityState,
};
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tokio::time::{timeout, Duration};

use crate::transport::{CodexRpcClient, CodexTransportError, CodexTransportEvent};
use crate::{CodexConnector, ConnectedClient};

const PROVIDER_ID: &str = "codex/app-server";
const AGENT_ID: &str = "codex/local";

#[derive(Default)]
pub(crate) struct ProviderState {
    runs: BTreeMap<RunId, Arc<CodexRun>>,
    sessions: BTreeMap<AgentSessionId, RunId>,
    loaded_sessions: BTreeSet<AgentSessionId>,
}

impl ProviderState {
    pub(crate) fn reset_connection_state(&mut self) {
        self.loaded_sessions.clear();
    }

    pub(crate) fn mark_loaded(&mut self, session_id: AgentSessionId) {
        self.loaded_sessions.insert(session_id);
    }
}

struct CodexRun {
    request: AgentStartRequest,
    execution: AgentExecutionRef,
    admission: AgentAdmission,
    sender: broadcast::Sender<Result<AgentProviderStreamItem, AgentProtocolError>>,
    durable: Mutex<Vec<AgentEventDraft>>,
    turn_id: Mutex<Option<String>>,
    final_response: Mutex<String>,
    pending: Mutex<BTreeMap<RequestId, NativePendingRequest>>,
    commands: Mutex<BTreeMap<String, (Digest, ProviderCommandDisposition)>>,
    telemetry_seq: AtomicU64,
    detached: AtomicBool,
    terminal: AtomicBool,
}

#[derive(Clone)]
struct NativePendingRequest {
    rpc_id: Value,
    kind: PendingRequestKind,
    params: Value,
}

impl CodexConnector {
    fn provider_state(&self) -> MutexGuard<'_, ProviderState> {
        self.provider_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn provider_descriptor() -> AgentDescriptorEnvelope {
        AgentDescriptorEnvelope::seal(AgentDescriptor {
            provider_id: AgentProviderId::new(PROVIDER_ID),
            agent_id: AgentId::new(AGENT_ID),
            supported_protocol_versions: vec![ProtocolVersion::new(1, 0)],
            accepted_content_types: BTreeSet::from(["text/plain".to_owned()]),
            capabilities: AgentCapabilities {
                session_reuse: true,
                structured_output: false,
                controls: ControlCapabilities {
                    steer: true,
                    cancel: CancelSupport::Confirmed,
                    recover: true,
                },
                pending_request_kinds: BTreeSet::from([
                    PendingRequestKind::Input,
                    PendingRequestKind::Approval,
                ]),
                supported_limits: BTreeSet::new(),
                resources: Vec::new(),
                effect_mediation: EffectMediation::HostMediated,
            },
            extensions: Extensions::new(),
        })
        .expect("static Codex descriptor must be valid")
    }

    async fn start_native_run(
        &self,
        connected: Arc<ConnectedClient>,
        run: Arc<CodexRun>,
        notifications: broadcast::Receiver<CodexTransportEvent>,
    ) -> Result<(), AgentStartError> {
        let session_id = run.request.run.spec.session_id.clone();
        let action =
            AgentSessionActionInvocation::from_run(&run.request.run.spec).map_err(|error| {
                self.remove_failed_start(&run);
                AgentStartError::Rejected(AgentRejection::new(
                    AgentRejectionCode::InvalidSpec,
                    error.to_string(),
                ))
            })?;
        if let Some(action) = &action {
            validate_native_action(action).inspect_err(|_error| {
                self.remove_failed_start(&run);
            })?;
        }
        self.invalidate_session_cache(&session_id);
        let needs_resume = self
            .provider_state()
            .loaded_sessions
            .insert(session_id.clone());
        if needs_resume {
            if let Err(error) = connected
                .rpc
                .request(
                    "thread/resume",
                    json!({
                        "threadId": session_id.as_str(),
                        "persistExtendedHistory": true
                    }),
                )
                .await
            {
                let loaded_without_rollout = unmaterialized_resume_without_rollout(&error)
                    && client_has_loaded_session(&connected.rpc, &session_id).await;
                if !loaded_without_rollout {
                    self.remove_failed_start(&run);
                    return Err(start_transport_error(error, false));
                }
            }
        }
        if let Some(action) = action {
            return self
                .start_native_action(connected, run, action, notifications)
                .await;
        }
        let input = codex_input(&run.request).map_err(|error| {
            self.remove_failed_start(&run);
            AgentStartError::Rejected(AgentRejection::new(
                AgentRejectionCode::InvalidSpec,
                error.to_string(),
            ))
        })?;
        let result = match connected
            .rpc
            .request(
                "turn/start",
                json!({"threadId": session_id.as_str(), "input": input}),
            )
            .await
        {
            Ok(result) => result,
            Err(
                error @ (CodexTransportError::Timeout
                | CodexTransportError::Closed
                | CodexTransportError::Disconnected(_)),
            ) => {
                return Err(start_transport_error(error, true));
            }
            Err(error) => {
                self.remove_failed_start(&run);
                return Err(start_transport_error(error, false));
            }
        };
        let turn_id = result
            .pointer("/turn/id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                AgentStartError::OutcomeUnknown(protocol_error(
                    "Codex turn/start omitted turn.id",
                    false,
                ))
            })?
            .to_owned();
        establish_turn(&run, &turn_id);
        tokio::spawn(async move {
            monitor_native_run(connected.rpc.clone(), run, turn_id, notifications).await;
        });
        Ok(())
    }

    async fn start_native_action(
        &self,
        connected: Arc<ConnectedClient>,
        run: Arc<CodexRun>,
        action: AgentSessionActionInvocation,
        mut notifications: broadcast::Receiver<CodexTransportEvent>,
    ) -> Result<(), AgentStartError> {
        let session_id = run.execution.session_id.clone();
        let turn_id = match action.action_id.as_str() {
            SESSION_COMPACT_ACTION => {
                if !action.arguments.is_null() {
                    self.remove_failed_start(&run);
                    return Err(invalid_action("session.compact takes no arguments"));
                }
                if let Err(error) = connected
                    .rpc
                    .request(
                        "thread/compact/start",
                        json!({"threadId": session_id.as_str()}),
                    )
                    .await
                {
                    return self.action_transport_failure(&run, error);
                }
                *lock(&run.final_response) = "Session context compacted.".to_owned();
                match timeout(
                    Duration::from_secs(30),
                    wait_for_compaction_turn(&session_id, &mut notifications),
                )
                .await
                {
                    Ok(Ok(turn_id)) => turn_id,
                    Ok(Err(error)) => return Err(error),
                    Err(_) => {
                        run.detached.store(true, Ordering::SeqCst);
                        return Err(AgentStartError::OutcomeUnknown(protocol_error(
                            "Codex started compaction but did not identify its turn",
                            true,
                        )));
                    }
                }
            }
            SESSION_REVIEW_ACTION => {
                let target = review_target(&action.arguments).inspect_err(|_error| {
                    self.remove_failed_start(&run);
                })?;
                let result = match connected
                    .rpc
                    .request(
                        "review/start",
                        json!({
                            "threadId": session_id.as_str(),
                            "target": target,
                            "delivery": "inline"
                        }),
                    )
                    .await
                {
                    Ok(result) => result,
                    Err(error) => return self.action_transport_failure(&run, error),
                };
                result
                    .pointer("/turn/id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .ok_or_else(|| {
                        AgentStartError::OutcomeUnknown(protocol_error(
                            "Codex review/start omitted turn.id",
                            false,
                        ))
                    })?
                    .to_owned()
            }
            other => {
                self.remove_failed_start(&run);
                return Err(invalid_action(format!(
                    "Codex does not support Run session action {other}"
                )));
            }
        };

        establish_turn(&run, &turn_id);
        tokio::spawn(async move {
            monitor_native_run(connected.rpc.clone(), run, turn_id, notifications).await;
        });
        Ok(())
    }

    fn action_transport_failure(
        &self,
        run: &Arc<CodexRun>,
        error: CodexTransportError,
    ) -> Result<(), AgentStartError> {
        match error {
            error @ (CodexTransportError::Timeout
            | CodexTransportError::Closed
            | CodexTransportError::Disconnected(_)) => Err(start_transport_error(error, true)),
            error => {
                self.remove_failed_start(run);
                Err(start_transport_error(error, false))
            }
        }
    }

    fn remove_failed_start(&self, run: &CodexRun) {
        let mut state = self.provider_state();
        state.runs.remove(&run.execution.run_id);
        state.sessions.remove(&run.execution.session_id);
        state.loaded_sessions.remove(&run.execution.session_id);
    }
}

fn unmaterialized_resume_without_rollout(error: &CodexTransportError) -> bool {
    matches!(
        error,
        CodexTransportError::Rpc(message)
            if message.contains("no rollout found for thread id")
    )
}

async fn client_has_loaded_session(rpc: &CodexRpcClient, session_id: &AgentSessionId) -> bool {
    let mut cursor = None;
    loop {
        let result = match rpc
            .request(
                "thread/loaded/list",
                json!({"cursor": cursor, "limit": 200}),
            )
            .await
        {
            Ok(result) => result,
            Err(_) => return false,
        };
        if result
            .get("data")
            .and_then(Value::as_array)
            .is_some_and(|ids| {
                ids.iter()
                    .any(|id| id.as_str() == Some(session_id.as_str()))
            })
        {
            return true;
        }
        cursor = result
            .get("nextCursor")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if cursor.is_none() {
            return false;
        }
    }
}

#[async_trait]
impl AgentProvider for CodexConnector {
    fn describe(&self) -> AgentDescriptorEnvelope {
        Self::provider_descriptor()
    }

    async fn start(&self, request: AgentStartRequest) -> Result<AgentStart, AgentStartError> {
        let descriptor = Self::provider_descriptor();
        request
            .validate_for_descriptor(&descriptor)
            .map_err(|error| {
                AgentRejection::new(AgentRejectionCode::RunIdConflict, error.to_string())
            })?;
        let compatibility = descriptor
            .descriptor
            .check_run_compatibility(&request.run)
            .map_err(AgentStartError::Rejected)?;
        let admission = AgentAdmission {
            skipped_optional_bindings: compatibility.skipped_optional_bindings.clone(),
        };
        admission
            .validate_against(&request.run, &compatibility)
            .map_err(|error| {
                AgentRejection::new(AgentRejectionCode::InvalidSpec, error.to_string())
            })?;
        let execution = AgentExecutionRef::for_start(&request, &descriptor).map_err(|error| {
            AgentRejection::new(AgentRejectionCode::RunIdConflict, error.to_string())
        })?;

        let run = {
            let mut state = self.provider_state();
            if let Some(existing) = state.runs.get(&request.run.spec.run_id) {
                if existing.request != request || existing.execution != execution {
                    return Err(AgentRejection::new(
                        AgentRejectionCode::RunIdConflict,
                        "run_id already belongs to a different Codex start",
                    )
                    .into());
                }
                return Ok(AgentStart {
                    execution: existing.execution.clone(),
                    admission: existing.admission.clone(),
                    stream: stream_for(existing),
                });
            }
            if state.sessions.contains_key(&request.run.spec.session_id) {
                let existing_terminal = state
                    .sessions
                    .get(&request.run.spec.session_id)
                    .and_then(|run_id| state.runs.get(run_id))
                    .is_some_and(|run| run.terminal.load(Ordering::SeqCst));
                if existing_terminal {
                    state.sessions.remove(&request.run.spec.session_id);
                } else {
                    return Err(AgentRejection::new(
                        AgentRejectionCode::SessionConflict,
                        "Codex permits one Orchestral Run per active session",
                    )
                    .into());
                }
            }
            let (sender, _) = broadcast::channel(512);
            let run = Arc::new(CodexRun {
                request: request.clone(),
                execution: execution.clone(),
                admission: admission.clone(),
                sender,
                durable: Mutex::new(Vec::new()),
                turn_id: Mutex::new(None),
                final_response: Mutex::new(String::new()),
                pending: Mutex::new(BTreeMap::new()),
                commands: Mutex::new(BTreeMap::new()),
                telemetry_seq: AtomicU64::new(0),
                detached: AtomicBool::new(false),
                terminal: AtomicBool::new(false),
            });
            state.sessions.insert(
                request.run.spec.session_id.clone(),
                request.run.spec.run_id.clone(),
            );
            state
                .runs
                .insert(request.run.spec.run_id.clone(), Arc::clone(&run));
            run
        };

        let connected = match self.client().await {
            Ok(connected) => connected,
            Err(error) => {
                self.remove_failed_start(&run);
                return Err(AgentStartError::Rejected(AgentRejection::new(
                    AgentRejectionCode::ProviderUnavailable,
                    error.to_string(),
                )));
            }
        };
        let notifications = connected.rpc.subscribe();
        self.start_native_run(connected, Arc::clone(&run), notifications)
            .await?;
        Ok(AgentStart {
            execution,
            admission,
            stream: stream_for(&run),
        })
    }

    async fn command(
        &self,
        execution: &AgentExecutionRef,
        command: AgentCommandEnvelope,
    ) -> Result<ProviderCommandDisposition, AgentProtocolError> {
        command.verify_digest()?;
        if command.run_id != execution.run_id {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::RunIdConflict,
                "command run does not match execution",
            ));
        }
        let run = self
            .provider_state()
            .runs
            .get(&execution.run_id)
            .cloned()
            .ok_or_else(|| {
                AgentProtocolError::new(AgentProtocolErrorCode::RunNotFound, "Codex run not found")
            })?;
        if run.execution != *execution {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::RunIdConflict,
                "execution identity does not match Codex run",
            ));
        }
        if let Some((digest, disposition)) = lock(&run.commands).get(command.command_id.as_str()) {
            if *digest != command.command_digest {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::DuplicateConflict,
                    "command_id was reused with different content",
                ));
            }
            let mut duplicate = disposition.clone();
            duplicate.duplicate = true;
            return Ok(duplicate);
        }
        let connected = self.client().await.map_err(connector_to_protocol)?;
        apply_native_command(&connected.rpc, &run, &command).await?;
        let disposition = ProviderCommandDisposition {
            command_id: command.command_id.clone(),
            run_id: command.run_id.clone(),
            outcome: ProviderCommandOutcome::Accepted,
            duplicate: false,
        };
        lock(&run.commands).insert(
            command.command_id.as_str().to_owned(),
            (command.command_digest.clone(), disposition.clone()),
        );
        Ok(disposition)
    }

    async fn recover(
        &self,
        request: AgentRecoveryRequest,
    ) -> Result<AgentRecovery, AgentProtocolError> {
        request.validate_for(&Self::provider_descriptor())?;
        let run = self
            .provider_state()
            .runs
            .get(&request.execution.run_id)
            .cloned()
            .ok_or_else(|| {
                AgentProtocolError::new(
                    AgentProtocolErrorCode::RunNotFound,
                    "Codex run is not available for process-local recovery",
                )
            })?;
        if run.request != request.start_request || run.execution != request.execution {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::RunIdConflict,
                "Codex recovery identity does not match the original run",
            ));
        }
        if !run.detached.load(Ordering::SeqCst) {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "Codex run is not detached",
            ));
        }

        let connected = self.client().await.map_err(connector_to_protocol)?;
        let notifications = connected.rpc.subscribe();
        let session_id = run.execution.session_id.clone();
        connected
            .rpc
            .request(
                "thread/resume",
                json!({
                    "threadId": session_id.as_str(),
                    "persistExtendedHistory": true
                }),
            )
            .await
            .map_err(transport_to_protocol)?;
        self.provider_state()
            .loaded_sessions
            .insert(session_id.clone());
        let result = connected
            .rpc
            .request(
                "thread/read",
                json!({"threadId": session_id.as_str(), "includeTurns": true}),
            )
            .await
            .map_err(transport_to_protocol)?;
        let turn_id = lock(&run.turn_id).clone().ok_or_else(|| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "Codex recovery omitted the original turn identity",
            )
        })?;
        let turn = result
            .pointer("/thread/turns")
            .and_then(Value::as_array)
            .and_then(|turns| {
                turns
                    .iter()
                    .find(|turn| turn.get("id").and_then(Value::as_str) == Some(turn_id.as_str()))
            })
            .ok_or_else(|| {
                AgentProtocolError::new(
                    AgentProtocolErrorCode::ProviderUnavailable,
                    "Codex authoritative thread history omitted the detached turn",
                )
                .with_retryable(true)
            })?;

        run.detached.store(false, Ordering::SeqCst);
        match turn.get("status").and_then(Value::as_str) {
            Some("inProgress") => {
                let monitored_run = Arc::clone(&run);
                tokio::spawn(async move {
                    monitor_native_run(
                        connected.rpc.clone(),
                        monitored_run,
                        turn_id,
                        notifications,
                    )
                    .await;
                });
            }
            Some("completed" | "interrupted" | "failed") => {
                restore_final_response(&run, turn);
                finish_run(&run, Some(turn));
            }
            status => {
                run.detached.store(true, Ordering::SeqCst);
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::ProviderUnavailable,
                    format!("Codex returned an unsupported recovered turn status: {status:?}"),
                )
                .with_retryable(true));
            }
        }
        Ok(AgentRecovery::reattached(stream_for(&run)))
    }
}

async fn apply_native_command(
    rpc: &CodexRpcClient,
    run: &Arc<CodexRun>,
    command: &AgentCommandEnvelope,
) -> Result<(), AgentProtocolError> {
    let thread_id = run.execution.session_id.as_str();
    let turn_id = lock(&run.turn_id).clone().ok_or_else(|| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidTransition,
            "Codex turn is not established",
        )
    })?;
    match &command.payload {
        AgentCommand::Steer { content } => {
            rpc.request(
                "turn/steer",
                json!({
                    "threadId": thread_id,
                    "expectedTurnId": turn_id,
                    "input": codex_content(content)?
                }),
            )
            .await
            .map_err(transport_to_protocol)?;
        }
        AgentCommand::Cancel { .. } => {
            rpc.request(
                "turn/interrupt",
                json!({"threadId": thread_id, "turnId": turn_id}),
            )
            .await
            .map_err(transport_to_protocol)?;
        }
        AgentCommand::ResolveRequest { response } => {
            let request_id = command.request_id.as_ref().ok_or_else(|| {
                AgentProtocolError::new(
                    AgentProtocolErrorCode::RequestNotFound,
                    "resolution omitted request id",
                )
            })?;
            let native = lock(&run.pending).remove(request_id).ok_or_else(|| {
                AgentProtocolError::new(
                    AgentProtocolErrorCode::RequestNotFound,
                    "pending Codex request was not found",
                )
            })?;
            if native.kind != response.kind() {
                lock(&run.pending).insert(request_id.clone(), native);
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::RequestTypeMismatch,
                    "resolution kind does not match pending Codex request",
                ));
            }
            let result = native_resolution(&native, response)?;
            if let Err(error) = rpc.respond(native.rpc_id.clone(), result).await {
                lock(&run.pending).insert(request_id.clone(), native);
                return Err(transport_to_protocol(error));
            }
            publish_event(
                run,
                AgentEventDraft {
                    event_id: AgentEventId::new(format!(
                        "codex-{}-request-{}-resolved",
                        run.execution.run_id.as_str(),
                        request_id.as_str()
                    )),
                    run_id: run.execution.run_id.clone(),
                    causation_id: Some(command.command_id.clone()),
                    source_fingerprint: None,
                    payload: AgentEvent::RequestResolved {
                        request_id: request_id.clone(),
                        resolution: response.clone(),
                        resolution_digest: response.digest()?,
                    },
                },
            );
        }
        _ => {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::Unsupported,
                "Codex adapter does not support this command type",
            ));
        }
    }
    Ok(())
}

async fn monitor_native_run(
    rpc: Arc<CodexRpcClient>,
    run: Arc<CodexRun>,
    turn_id: String,
    mut notifications: broadcast::Receiver<CodexTransportEvent>,
) {
    loop {
        let message = match notifications.recv().await {
            Ok(CodexTransportEvent::Message(message)) => message,
            Ok(CodexTransportEvent::Disconnected { reason }) => {
                run.detached.store(true, Ordering::SeqCst);
                let _ = run.sender.send(Err(protocol_error(
                    format!("Codex app-server disconnected: {reason}"),
                    true,
                )));
                return;
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                let _ = run.sender.send(Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::SequenceGap,
                    format!("Codex notification subscriber lagged by {skipped}"),
                )));
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => {
                run.detached.store(true, Ordering::SeqCst);
                let _ = run.sender.send(Err(protocol_error(
                    "Codex app-server notification channel closed",
                    true,
                )));
                return;
            }
        };
        if !belongs_to_run(&message, &run.execution.session_id, &turn_id) {
            continue;
        }
        let method = message
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match method {
            "item/agentMessage/delta" => {
                if let Some(delta) = message.pointer("/params/delta").and_then(Value::as_str) {
                    lock(&run.final_response).push_str(delta);
                    publish_telemetry(
                        &run,
                        AgentTelemetry::OutputDelta {
                            output_id: output_id(&run),
                            delta: Content::text(delta),
                        },
                    );
                }
            }
            "item/completed" => handle_completed_item(&run, message.pointer("/params/item")),
            "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
                open_native_request(&run, &message, PendingRequestKind::Approval);
            }
            "item/tool/requestUserInput" => {
                open_native_request(&run, &message, PendingRequestKind::Input);
            }
            "item/tool/call" => {
                if let Some(id) = message.get("id") {
                    let _ = rpc
                        .respond(
                            id.clone(),
                            json!({
                                "contentItems": [{
                                    "type": "inputText",
                                    "text": "Orchestral does not provide this dynamic tool"
                                }],
                                "success": false
                            }),
                        )
                        .await;
                }
            }
            "turn/completed" => {
                finish_run(&run, message.pointer("/params/turn"));
                return;
            }
            _ => {}
        }
    }
}

fn establish_turn(run: &Arc<CodexRun>, turn_id: &str) {
    *lock(&run.turn_id) = Some(turn_id.to_owned());
    publish_event(
        run,
        AgentEventDraft {
            event_id: event_id(run, "started"),
            run_id: run.execution.run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RunStarted,
        },
    );
}

async fn wait_for_compaction_turn(
    session_id: &AgentSessionId,
    notifications: &mut broadcast::Receiver<CodexTransportEvent>,
) -> Result<String, AgentStartError> {
    loop {
        match notifications.recv().await {
            Ok(CodexTransportEvent::Message(message)) => {
                if message.get("method").and_then(Value::as_str) != Some("item/started")
                    || message.pointer("/params/threadId").and_then(Value::as_str)
                        != Some(session_id.as_str())
                    || message.pointer("/params/item/type").and_then(Value::as_str)
                        != Some("contextCompaction")
                {
                    continue;
                }
                return message
                    .pointer("/params/turnId")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        AgentStartError::OutcomeUnknown(protocol_error(
                            "Codex compaction item omitted turnId",
                            false,
                        ))
                    });
            }
            Ok(CodexTransportEvent::Disconnected { reason }) => {
                return Err(AgentStartError::OutcomeUnknown(protocol_error(
                    format!("Codex disconnected while starting compaction: {reason}"),
                    true,
                )));
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                return Err(AgentStartError::OutcomeUnknown(protocol_error(
                    format!("Codex compaction notifications lagged by {skipped}"),
                    true,
                )));
            }
            Err(broadcast::error::RecvError::Closed) => {
                return Err(AgentStartError::OutcomeUnknown(protocol_error(
                    "Codex notification channel closed while starting compaction",
                    true,
                )));
            }
        }
    }
}

fn invalid_action(message: impl Into<String>) -> AgentStartError {
    AgentStartError::Rejected(AgentRejection::new(
        AgentRejectionCode::InvalidSpec,
        message,
    ))
}

fn validate_native_action(action: &AgentSessionActionInvocation) -> Result<(), AgentStartError> {
    match action.action_id.as_str() {
        SESSION_COMPACT_ACTION if action.arguments.is_null() => Ok(()),
        SESSION_COMPACT_ACTION => Err(invalid_action("session.compact takes no arguments")),
        SESSION_REVIEW_ACTION => review_target(&action.arguments).map(|_| ()),
        other => Err(invalid_action(format!(
            "Codex does not support Run session action {other}"
        ))),
    }
}

fn review_target(arguments: &Value) -> Result<Value, AgentStartError> {
    let object = arguments
        .as_object()
        .ok_or_else(|| invalid_action("session.review arguments must be an object"))?;
    let allowed = ["target", "branch", "sha", "title", "instructions"];
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(invalid_action(format!(
            "session.review does not accept argument {key}"
        )));
    }
    let target = action_string(object, "target")?;
    match target.as_str() {
        "uncommitted_changes" => Ok(json!({"type": "uncommittedChanges"})),
        "base_branch" => Ok(json!({
            "type": "baseBranch",
            "branch": action_string(object, "branch")?
        })),
        "commit" => {
            let mut target = json!({
                "type": "commit",
                "sha": action_string(object, "sha")?
            });
            if let Some(title) = optional_action_string(object, "title")? {
                target["title"] = Value::String(title);
            }
            Ok(target)
        }
        "custom" => Ok(json!({
            "type": "custom",
            "instructions": action_string(object, "instructions")?
        })),
        _ => Err(invalid_action(
            "session.review target must be uncommitted_changes, base_branch, commit, or custom",
        )),
    }
}

fn action_string(
    object: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<String, AgentStartError> {
    optional_action_string(object, field)?
        .ok_or_else(|| invalid_action(format!("session.review requires {field}")))
}

fn optional_action_string(
    object: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<Option<String>, AgentStartError> {
    match object.get(field) {
        None => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
        Some(_) => Err(invalid_action(format!(
            "session.review {field} must be a non-empty string"
        ))),
    }
}

fn handle_completed_item(run: &Arc<CodexRun>, item: Option<&Value>) {
    let Some(item) = item else { return };
    match item.get("type").and_then(Value::as_str) {
        Some("agentMessage") => {
            if let Some(text) = item.get("text").and_then(Value::as_str) {
                *lock(&run.final_response) = text.to_owned();
            }
        }
        Some("commandExecution") => {
            let command = item
                .get("command")
                .and_then(Value::as_str)
                .unwrap_or("command");
            publish_tool(
                run,
                item,
                "exec_command",
                vec![ToolActivityEvidence::Command {
                    command: safe_text(command, 512),
                }],
            );
        }
        Some("fileChange") => publish_tool(
            run,
            item,
            "file_change",
            vec![ToolActivityEvidence::Note {
                text: "Codex changed workspace files".to_owned(),
            }],
        ),
        Some("mcpToolCall") => publish_tool(run, item, "mcp", Vec::new()),
        Some("dynamicToolCall") => publish_tool(run, item, "dynamic_tool", Vec::new()),
        Some("contextCompaction") => publish_tool(
            run,
            item,
            "session.compact",
            vec![ToolActivityEvidence::Note {
                text: "Codex compacted the native session context".to_owned(),
            }],
        ),
        _ => {}
    }
}

fn publish_tool(
    run: &Arc<CodexRun>,
    item: &Value,
    tool_name: &str,
    evidence: Vec<ToolActivityEvidence>,
) {
    let id = item.get("id").and_then(Value::as_str).unwrap_or("unknown");
    let state = match item.get("status").and_then(Value::as_str) {
        Some("failed") => ToolActivityState::Failed,
        Some("interrupted") => ToolActivityState::Cancelled,
        _ => ToolActivityState::Succeeded,
    };
    publish_telemetry(
        run,
        AgentTelemetry::ToolActivity {
            activity_id: ToolActivityId::new(format!("codex-{id}")),
            tool_name: tool_name.to_owned(),
            state,
            evidence,
        },
    );
}

fn open_native_request(run: &Arc<CodexRun>, message: &Value, kind: PendingRequestKind) {
    let Some(rpc_id) = message.get("id").cloned() else {
        return;
    };
    let params = message.get("params").cloned().unwrap_or(Value::Null);
    let native_key = params
        .get("approvalId")
        .or_else(|| params.get("itemId"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| rpc_id.to_string());
    let request_id = RequestId::new(format!("codex-{native_key}"));
    let payload = match kind {
        PendingRequestKind::Approval => {
            let reason = params
                .get("reason")
                .and_then(Value::as_str)
                .or_else(|| params.get("command").and_then(Value::as_str))
                .unwrap_or("Codex requests approval for an effect");
            PendingRequestPayload::Approval {
                operation_digest: Digest::sha256(serde_json::to_vec(&params).unwrap_or_default()),
                requested_scope: approval_scopes(
                    message.get("method").and_then(Value::as_str),
                    &params,
                ),
                session_approval_scope: None,
                reason: safe_text(reason, 1_000),
            }
        }
        PendingRequestKind::Input => PendingRequestPayload::Input {
            prompt: vec![Content::text(input_prompt(&params))],
            input_schema: None,
        },
        _ => return,
    };
    lock(&run.pending).insert(
        request_id.clone(),
        NativePendingRequest {
            rpc_id,
            kind,
            params,
        },
    );
    publish_event(
        run,
        AgentEventDraft {
            event_id: AgentEventId::new(format!(
                "codex-{}-request-{}",
                run.execution.run_id.as_str(),
                request_id.as_str()
            )),
            run_id: run.execution.run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RequestOpened {
                request: PendingRequest {
                    request_id,
                    blocking: true,
                    payload,
                },
            },
        },
    );
}

fn finish_run(run: &Arc<CodexRun>, turn: Option<&Value>) {
    if run.terminal.load(Ordering::SeqCst) {
        return;
    }
    let status = turn
        .and_then(|turn| turn.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("failed");
    match status {
        "completed" => {
            if run.terminal.swap(true, Ordering::SeqCst) {
                return;
            }
            let response = lock(&run.final_response).clone();
            let output_event_id = event_id(run, "output");
            publish_event(
                run,
                AgentEventDraft {
                    event_id: output_event_id.clone(),
                    run_id: run.execution.run_id.clone(),
                    causation_id: None,
                    source_fingerprint: None,
                    payload: AgentEvent::OutputCommitted {
                        output_id: output_id(run),
                        content: vec![Content::text(response.clone())],
                    },
                },
            );
            publish_event(
                run,
                AgentEventDraft {
                    event_id: event_id(run, "delivered"),
                    run_id: run.execution.run_id.clone(),
                    causation_id: None,
                    source_fingerprint: None,
                    payload: AgentEvent::DeliveryCommitted {
                        delivery: AgentDelivery {
                            delivery_id: DeliveryId::new(format!(
                                "codex-{}-delivery",
                                run.execution.run_id.as_str()
                            )),
                            run_id: run.execution.run_id.clone(),
                            spec_digest: run.execution.spec_digest.clone(),
                            final_response: Content::text(response),
                            outputs: Vec::new(),
                            artifacts: Vec::new(),
                            unresolved_issues: Vec::new(),
                            usage: None,
                            provenance: Provenance {
                                provider_id: run.execution.provider_id.clone(),
                                agent_id: run.execution.agent_id.clone(),
                                supporting_event_ids: vec![output_event_id],
                                extensions: Extensions::new(),
                            },
                        },
                    },
                },
            );
        }
        "interrupted" => {
            if run.terminal.swap(true, Ordering::SeqCst) {
                return;
            }
            publish_event(
                run,
                AgentEventDraft {
                    event_id: event_id(run, "cancelled"),
                    run_id: run.execution.run_id.clone(),
                    causation_id: None,
                    source_fingerprint: None,
                    payload: AgentEvent::RunCancelled {
                        reason: "Codex turn was interrupted".to_owned(),
                    },
                },
            )
        }
        _ => {
            let message = turn
                .and_then(|turn| turn.pointer("/error/message"))
                .and_then(Value::as_str)
                .unwrap_or("Codex turn failed");
            publish_failure(run, "codex_turn_failed", message, false);
        }
    }
}

fn restore_final_response(run: &CodexRun, turn: &Value) {
    let response = turn
        .get("items")
        .and_then(Value::as_array)
        .and_then(|items| {
            items.iter().rev().find_map(|item| {
                (item.get("type").and_then(Value::as_str) == Some("agentMessage"))
                    .then(|| item.get("text").and_then(Value::as_str))
                    .flatten()
            })
        });
    if let Some(response) = response {
        *lock(&run.final_response) = response.to_owned();
    }
}

fn publish_failure(run: &Arc<CodexRun>, code: &str, message: &str, retryable: bool) {
    if run.terminal.swap(true, Ordering::SeqCst) {
        return;
    }
    publish_event(
        run,
        AgentEventDraft {
            event_id: event_id(run, "failed"),
            run_id: run.execution.run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RunFailed {
                failure: AgentFailure {
                    code: code.to_owned(),
                    message: message.to_owned(),
                    retryable,
                    details: Value::Null,
                },
            },
        },
    );
}

fn publish_event(run: &Arc<CodexRun>, draft: AgentEventDraft) {
    lock(&run.durable).push(draft.clone());
    let _ = run
        .sender
        .send(Ok(AgentProviderStreamItem::Event(Box::new(draft))));
}

fn publish_telemetry(run: &Arc<CodexRun>, payload: AgentTelemetry) {
    let sequence = run.telemetry_seq.fetch_add(1, Ordering::SeqCst) + 1;
    let telemetry = AgentProviderStreamItem::Telemetry(AgentTelemetryEnvelope {
        telemetry_id: TelemetryId::new(format!(
            "codex-{}-telemetry-{}",
            run.execution.run_id.as_str(),
            sequence
        )),
        run_id: run.execution.run_id.clone(),
        provider_seq: Some(sequence),
        payload,
    });
    let _ = run.sender.send(Ok(telemetry));
}

fn stream_for(run: &CodexRun) -> AgentProviderStream {
    let receiver = run.sender.subscribe();
    let replay = lock(&run.durable)
        .clone()
        .into_iter()
        .map(|draft| Ok(AgentProviderStreamItem::Event(Box::new(draft))));
    let replay = stream::iter(replay);
    if run.terminal.load(Ordering::SeqCst) || run.detached.load(Ordering::SeqCst) {
        return replay.boxed();
    }
    let live = stream::unfold(receiver, |mut receiver| async move {
        match receiver.recv().await {
            Ok(item) => Some((item, receiver)),
            Err(broadcast::error::RecvError::Lagged(skipped)) => Some((
                Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::SequenceGap,
                    format!("Codex stream subscriber lagged by {skipped}"),
                )),
                receiver,
            )),
            Err(broadcast::error::RecvError::Closed) => None,
        }
    });
    replay.chain(live).boxed()
}

fn native_resolution(
    native: &NativePendingRequest,
    resolution: &RequestResolution,
) -> Result<Value, AgentProtocolError> {
    match resolution {
        RequestResolution::Approval { decision, .. } => {
            let decision = match decision {
                ApprovalDecision::Allow => "accept",
                ApprovalDecision::Deny => "decline",
                _ => {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::Unsupported,
                        "Codex adapter does not support this approval decision",
                    ));
                }
            };
            Ok(json!({ "decision": decision }))
        }
        RequestResolution::Input { content } => {
            let answer = content_text(content)?;
            let questions = native
                .params
                .get("questions")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or_default();
            let mut answers = serde_json::Map::new();
            for question in questions {
                if let Some(id) = question.get("id").and_then(Value::as_str) {
                    answers.insert(id.to_owned(), json!({"answers": [answer.clone()]}));
                }
            }
            Ok(json!({"answers": answers}))
        }
        RequestResolution::ExternalResult { .. } => Err(AgentProtocolError::new(
            AgentProtocolErrorCode::RequestTypeMismatch,
            "Codex dynamic tool results are not declared by this adapter",
        )),
        _ => Err(AgentProtocolError::new(
            AgentProtocolErrorCode::Unsupported,
            "Codex adapter does not support this request resolution",
        )),
    }
}

fn codex_input(request: &AgentStartRequest) -> Result<Value, AgentProtocolError> {
    codex_content(&request.run.spec.input)
}

fn codex_content(content: &[Content]) -> Result<Value, AgentProtocolError> {
    let text = content_text(content)?;
    Ok(json!([{"type": "text", "text": text}]))
}

fn content_text(content: &[Content]) -> Result<String, AgentProtocolError> {
    content
        .iter()
        .map(|content| match (&content.media_type[..], &content.body) {
            ("text/plain", ContentBody::Inline(Value::String(text))) => Ok(text.clone()),
            _ => Err(AgentProtocolError::new(
                AgentProtocolErrorCode::Unsupported,
                "Codex adapter currently accepts inline text/plain content only",
            )),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|parts| parts.join("\n"))
}

fn belongs_to_run(message: &Value, session_id: &AgentSessionId, turn_id: &str) -> bool {
    let params = message.get("params").unwrap_or(&Value::Null);
    params
        .get("threadId")
        .and_then(Value::as_str)
        .is_none_or(|id| id == session_id.as_str())
        && params
            .get("turnId")
            .and_then(Value::as_str)
            .or_else(|| params.pointer("/turn/id").and_then(Value::as_str))
            .is_none_or(|id| id == turn_id)
}

fn input_prompt(params: &Value) -> String {
    let text = params
        .get("questions")
        .and_then(Value::as_array)
        .map(|questions| {
            questions
                .iter()
                .filter_map(|question| question.get("question").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    if text.is_empty() {
        "Codex requests user input".to_owned()
    } else {
        safe_text(&text, 4_000)
    }
}

fn approval_scopes(method: Option<&str>, params: &Value) -> Vec<String> {
    let mut scopes = vec!["process".to_owned()];
    if method == Some("item/fileChange/requestApproval") {
        scopes.push("filesystem_write".to_owned());
    }
    if params.get("networkApprovalContext").is_some() {
        scopes.push("network".to_owned());
    }
    scopes
}

fn safe_text(value: &str, limit: usize) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .take(limit)
        .collect()
}

fn output_id(run: &CodexRun) -> OutputId {
    OutputId::new(format!("codex-{}-response", run.execution.run_id.as_str()))
}

fn event_id(run: &CodexRun, suffix: &str) -> AgentEventId {
    AgentEventId::new(format!("codex-{}-{suffix}", run.execution.run_id.as_str()))
}

fn start_transport_error(error: CodexTransportError, outcome_unknown: bool) -> AgentStartError {
    let rejection_code = match &error {
        CodexTransportError::Rpc(message) if message.contains("already has an active writer") => {
            AgentRejectionCode::SessionConflict
        }
        _ => AgentRejectionCode::ProviderUnavailable,
    };
    let protocol = transport_to_protocol(error);
    if outcome_unknown {
        AgentStartError::OutcomeUnknown(protocol)
    } else {
        AgentStartError::Rejected(
            AgentRejection::new(rejection_code, protocol.message)
                .with_retryable(protocol.retryable),
        )
    }
}

fn transport_to_protocol(error: CodexTransportError) -> AgentProtocolError {
    let retryable = matches!(
        error,
        CodexTransportError::Io(_)
            | CodexTransportError::Closed
            | CodexTransportError::Disconnected(_)
            | CodexTransportError::Timeout
    );
    protocol_error(error.to_string(), retryable)
}

fn connector_to_protocol(
    error: orchestral_core::agent_connector::AgentConnectorError,
) -> AgentProtocolError {
    AgentProtocolError::new(
        AgentProtocolErrorCode::ProviderUnavailable,
        error.to_string(),
    )
    .with_retryable(error.retryable)
}

fn protocol_error(message: impl Into<String>, retryable: bool) -> AgentProtocolError {
    AgentProtocolError::new(AgentProtocolErrorCode::ProviderUnavailable, message)
        .with_retryable(retryable)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::StreamExt;
    use orchestral_core::agent_protocol::reference::AgentRunStatus;
    use orchestral_core::agent_protocol::spi::AgentProvider;
    use orchestral_core::agent_protocol::wire::{
        AgentCommandEnvelope, AgentRunEnvelope, ApprovalGrantRef, CommandId, ProviderBindingRef,
    };
    use orchestral_runtime::AgentController;
    use tokio::io::{duplex, AsyncBufReadExt, AsyncWriteExt, BufReader};

    use super::*;

    #[test]
    fn active_writer_is_a_typed_session_conflict() {
        let error = start_transport_error(
            CodexTransportError::Rpc("thread fixture already has an active writer".to_owned()),
            false,
        );
        assert!(matches!(
            error,
            AgentStartError::Rejected(AgentRejection {
                code: AgentRejectionCode::SessionConflict,
                ..
            })
        ));
    }

    fn start_request(session_id: &str, run_id: &str) -> AgentStartRequest {
        let descriptor = CodexConnector::provider_descriptor();
        AgentStartRequest::new(
            AgentRunEnvelope::new(
                ProtocolVersion::new(1, 0),
                AgentSessionId::new(session_id),
                RunId::new(run_id),
                vec![Content::text("fix the failing test")],
            )
            .unwrap(),
            ProviderBindingRef::new("codex/local"),
            &descriptor,
        )
        .unwrap()
    }

    fn action_start_request(
        session_id: &str,
        run_id: &str,
        action_id: &str,
        arguments: Value,
    ) -> AgentStartRequest {
        let descriptor = CodexConnector::provider_descriptor();
        let mut run = AgentRunEnvelope::new(
            ProtocolVersion::new(1, 0),
            AgentSessionId::new(session_id),
            RunId::new(run_id),
            vec![Content::text("session action")],
        )
        .unwrap();
        AgentSessionActionInvocation {
            action_id: orchestral_core::agent_connector::AgentSessionActionId::new(action_id),
            arguments,
        }
        .insert_into(&mut run.spec)
        .unwrap();
        AgentStartRequest::new(
            AgentRunEnvelope::seal(run.spec).unwrap(),
            ProviderBindingRef::new("codex/local"),
            &descriptor,
        )
        .unwrap()
    }

    async fn next_request(
        lines: &mut tokio::io::Lines<BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>>,
    ) -> Value {
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap()
    }

    #[test]
    fn descriptor_promises_only_implemented_controls() {
        let descriptor = CodexConnector::provider_descriptor();
        assert!(descriptor.descriptor.capabilities.session_reuse);
        assert!(descriptor.descriptor.capabilities.controls.steer);
        assert_eq!(
            descriptor.descriptor.capabilities.controls.cancel,
            CancelSupport::Confirmed
        );
        assert!(descriptor.descriptor.capabilities.controls.recover);
        assert_eq!(
            descriptor.descriptor.capabilities.pending_request_kinds,
            BTreeSet::from([PendingRequestKind::Input, PendingRequestKind::Approval])
        );
    }

    #[test]
    fn approval_mapping_never_grants_session_scope_implicitly() {
        let native = NativePendingRequest {
            rpc_id: json!(1),
            kind: PendingRequestKind::Approval,
            params: json!({}),
        };
        let result = native_resolution(
            &native,
            &RequestResolution::Approval {
                decision: ApprovalDecision::Allow,
                grant_ref: Some(
                    orchestral_core::agent_protocol::wire::ApprovalGrantRef::new("single-use"),
                ),
            },
        )
        .unwrap();
        assert_eq!(result, json!({"decision": "accept"}));
    }

    #[tokio::test]
    async fn streams_codex_turn_to_normalized_delivery() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            assert_eq!(resume["method"], "thread/resume");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": resume["id"], "result": {"thread": {"id": "thread-1"}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let start = next_request(&mut lines).await;
            assert_eq!(start["method"], "turn/start");
            server_write
                .write_all(format!("{}\n", json!({"id": start["id"], "result": {"turn": {"id": "turn-1", "status": "inProgress", "items": []}}})).as_bytes())
                .await
                .unwrap();
            for notification in [
                json!({"method": "item/agentMessage/delta", "params": {"threadId": "thread-1", "turnId": "turn-1", "itemId": "a1", "delta": "done"}}),
                json!({"method": "item/completed", "params": {"threadId": "thread-1", "turnId": "turn-1", "item": {"type": "commandExecution", "id": "c1", "command": "cargo test", "status": "completed"}}}),
                json!({"method": "item/completed", "params": {"threadId": "thread-1", "turnId": "turn-1", "item": {"type": "agentMessage", "id": "a1", "text": "done"}}}),
                json!({"method": "turn/completed", "params": {"threadId": "thread-1", "turn": {"id": "turn-1", "status": "completed", "items": []}}}),
            ] {
                server_write
                    .write_all(format!("{notification}\n").as_bytes())
                    .await
                    .unwrap();
            }
        });

        let mut stream = connector
            .start(start_request("thread-1", "run-1"))
            .await
            .unwrap()
            .stream;
        let mut event_types = Vec::new();
        let mut saw_tool = false;
        while let Some(item) = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
        {
            let item = item.unwrap();
            item.validate_integrity().unwrap();
            match item {
                AgentProviderStreamItem::Event(event) => {
                    event_types.push(format!("{:?}", event.payload));
                    if matches!(event.payload, AgentEvent::DeliveryCommitted { .. }) {
                        break;
                    }
                }
                AgentProviderStreamItem::Telemetry(telemetry) => {
                    saw_tool |= matches!(telemetry.payload, AgentTelemetry::ToolActivity { .. });
                }
                _ => {}
            }
        }
        assert!(event_types
            .iter()
            .any(|event| event.starts_with("RunStarted")));
        assert!(event_types
            .iter()
            .any(|event| event.starts_with("OutputCommitted")));
        assert!(event_types
            .iter()
            .any(|event| event.starts_with("DeliveryCommitted")));
        assert!(saw_tool);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn compact_action_uses_native_method_and_run_lifecycle() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            assert_eq!(resume["method"], "thread/resume");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": resume["id"], "result": {"thread": {"id": "thread-compact"}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let compact = next_request(&mut lines).await;
            assert_eq!(compact["method"], "thread/compact/start");
            assert_eq!(compact["params"], json!({"threadId": "thread-compact"}));
            server_write
                .write_all(format!("{}\n", json!({"id": compact["id"], "result": {}})).as_bytes())
                .await
                .unwrap();
            for notification in [
                json!({"method": "item/started", "params": {"threadId": "thread-compact", "turnId": "turn-compact", "item": {"type": "contextCompaction", "id": "compact-1"}}}),
                json!({"method": "item/completed", "params": {"threadId": "thread-compact", "turnId": "turn-compact", "item": {"type": "contextCompaction", "id": "compact-1"}}}),
                json!({"method": "turn/completed", "params": {"threadId": "thread-compact", "turn": {"id": "turn-compact", "status": "completed", "items": []}}}),
            ] {
                server_write
                    .write_all(format!("{notification}\n").as_bytes())
                    .await
                    .unwrap();
            }
        });

        let mut stream = connector
            .start(action_start_request(
                "thread-compact",
                "run-compact",
                SESSION_COMPACT_ACTION,
                Value::Null,
            ))
            .await
            .unwrap()
            .stream;
        let mut delivered = None;
        while let Some(item) = timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
        {
            if let AgentProviderStreamItem::Event(event) = item.unwrap() {
                if let AgentEvent::DeliveryCommitted { delivery } = event.payload {
                    delivered = Some(delivery);
                    break;
                }
            }
        }
        assert_eq!(
            delivered.unwrap().final_response,
            Content::text("Session context compacted.")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn review_action_maps_typed_target_to_native_review() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": resume["id"], "result": {"thread": {"id": "thread-review"}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let review = next_request(&mut lines).await;
            assert_eq!(review["method"], "review/start");
            assert_eq!(
                review["params"],
                json!({
                    "threadId": "thread-review",
                    "target": {"type": "baseBranch", "branch": "main"},
                    "delivery": "inline"
                })
            );
            server_write
                .write_all(format!("{}\n", json!({"id": review["id"], "result": {"turn": {"id": "turn-review", "status": "inProgress", "items": []}, "reviewThreadId": "thread-review"}})).as_bytes())
                .await
                .unwrap();
            for notification in [
                json!({"method": "item/agentMessage/delta", "params": {"threadId": "thread-review", "turnId": "turn-review", "itemId": "review-message", "delta": "No findings."}}),
                json!({"method": "turn/completed", "params": {"threadId": "thread-review", "turn": {"id": "turn-review", "status": "completed", "items": []}}}),
            ] {
                server_write
                    .write_all(format!("{notification}\n").as_bytes())
                    .await
                    .unwrap();
            }
        });

        let mut stream = connector
            .start(action_start_request(
                "thread-review",
                "run-review",
                SESSION_REVIEW_ACTION,
                json!({"target": "base_branch", "branch": "main"}),
            ))
            .await
            .unwrap()
            .stream;
        let mut delivered = None;
        while let Some(item) = timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
        {
            if let AgentProviderStreamItem::Event(event) = item.unwrap() {
                if let AgentEvent::DeliveryCommitted { delivery } = event.payload {
                    delivered = Some(delivery);
                    break;
                }
            }
        }
        assert_eq!(
            delivered.unwrap().final_response,
            Content::text("No findings.")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn relays_approval_steer_and_cancel_without_session_grant() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": resume["id"], "result": {"thread": {"id": "thread-2"}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let start = next_request(&mut lines).await;
            server_write
                .write_all(format!("{}\n", json!({"id": start["id"], "result": {"turn": {"id": "turn-2", "status": "inProgress", "items": []}}})).as_bytes())
                .await
                .unwrap();
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "method": "item/commandExecution/requestApproval",
                            "id": 70,
                            "params": {
                                "threadId": "thread-2", "turnId": "turn-2", "itemId": "command-1",
                                "command": "git push", "reason": "network access"
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let approval = next_request(&mut lines).await;
            assert_eq!(
                approval,
                json!({"id": 70, "result": {"decision": "accept"}})
            );

            let steer = next_request(&mut lines).await;
            assert_eq!(steer["method"], "turn/steer");
            assert_eq!(steer["params"]["expectedTurnId"], "turn-2");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": steer["id"], "result": {"turnId": "turn-2"}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let cancel = next_request(&mut lines).await;
            assert_eq!(cancel["method"], "turn/interrupt");
            server_write
                .write_all(format!("{}\n", json!({"id": cancel["id"], "result": {}})).as_bytes())
                .await
                .unwrap();
            server_write
                .write_all(format!("{}\n", json!({"method": "turn/completed", "params": {"threadId": "thread-2", "turn": {"id": "turn-2", "status": "interrupted", "items": []}}})).as_bytes())
                .await
                .unwrap();
        });

        let started = connector
            .start(start_request("thread-2", "run-2"))
            .await
            .unwrap();
        let execution = started.execution;
        let mut stream = started.stream;
        let request_id = loop {
            let item = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if let AgentProviderStreamItem::Event(event) = item {
                if let AgentEvent::RequestOpened { request } = event.payload {
                    break request.request_id;
                }
            }
        };
        let approval = AgentCommandEnvelope::new(
            CommandId::new("approval-command"),
            RunId::new("run-2"),
            Some(request_id),
            AgentCommand::ResolveRequest {
                response: RequestResolution::Approval {
                    decision: ApprovalDecision::Allow,
                    grant_ref: Some(ApprovalGrantRef::new("single-use-grant")),
                },
            },
        )
        .unwrap();
        let first = connector
            .command(&execution, approval.clone())
            .await
            .unwrap();
        assert!(!first.duplicate);
        let duplicate = connector.command(&execution, approval).await.unwrap();
        assert!(duplicate.duplicate);

        connector
            .command(
                &execution,
                AgentCommandEnvelope::new(
                    CommandId::new("steer-command"),
                    RunId::new("run-2"),
                    None,
                    AgentCommand::Steer {
                        content: vec![Content::text("focus on tests")],
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();
        connector
            .command(
                &execution,
                AgentCommandEnvelope::new(
                    CommandId::new("cancel-command"),
                    RunId::new("run-2"),
                    None,
                    AgentCommand::Cancel {
                        reason: "user stopped".to_owned(),
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();

        loop {
            let item = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if matches!(
                item,
                AgentProviderStreamItem::Event(event)
                    if matches!(event.payload, AgentEvent::RunCancelled { .. })
            ) {
                break;
            }
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn host_controller_accepts_codex_event_sequence() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = Arc::new(CodexConnector::with_client(rpc, "codex/test"));
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": resume["id"], "result": {"thread": {"id": "thread-3"}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let start = next_request(&mut lines).await;
            server_write
                .write_all(format!("{}\n", json!({"id": start["id"], "result": {"turn": {"id": "turn-3", "status": "inProgress", "items": []}}})).as_bytes())
                .await
                .unwrap();
            for notification in [
                json!({"method": "item/completed", "params": {"threadId": "thread-3", "turnId": "turn-3", "item": {"type": "agentMessage", "id": "a3", "text": "verified"}}}),
                json!({"method": "turn/completed", "params": {"threadId": "thread-3", "turn": {"id": "turn-3", "status": "completed", "items": []}}}),
            ] {
                server_write
                    .write_all(format!("{notification}\n").as_bytes())
                    .await
                    .unwrap();
            }
        });
        let provider: Arc<dyn AgentProvider> = connector;
        let controller = Arc::new(
            AgentController::new(provider, ProviderBindingRef::new("codex/local")).unwrap(),
        );
        let run = AgentRunEnvelope::new(
            ProtocolVersion::new(1, 0),
            AgentSessionId::new("thread-3"),
            RunId::new("run-3"),
            vec![Content::text("verify")],
        )
        .unwrap();
        let execution = controller.start(run).await.unwrap();
        let view = tokio::time::timeout(
            Duration::from_secs(1),
            controller.wait_for_terminal(&execution.run_id),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(view.state.status(), AgentRunStatus::Delivered);
        assert_eq!(
            view.delivery.unwrap().final_response,
            Content::text("verified")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn app_server_disconnect_becomes_unknown_continuity_not_a_false_failure() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = Arc::new(CodexConnector::with_client(rpc, "codex/test"));
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": resume["id"], "result": {"thread": {"id": "thread-disconnect"}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let start = next_request(&mut lines).await;
            server_write
                .write_all(format!("{}\n", json!({"id": start["id"], "result": {"turn": {"id": "turn-disconnect", "status": "inProgress", "items": []}}})).as_bytes())
                .await
                .unwrap();
            // EOF is deliberately ambiguous: the native turn may still have
            // reached Codex even though this app-server process disappeared.
        });
        let provider: Arc<dyn AgentProvider> = connector;
        let controller = Arc::new(
            AgentController::new(provider, ProviderBindingRef::new("codex/local")).unwrap(),
        );
        let run = AgentRunEnvelope::new(
            ProtocolVersion::new(1, 0),
            AgentSessionId::new("thread-disconnect"),
            RunId::new("run-disconnect"),
            vec![Content::text("keep working")],
        )
        .unwrap();
        let execution = controller.start(run).await.unwrap();
        server.await.unwrap();

        let view = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let view = controller.inspect(&execution.run_id).await.unwrap();
                if view.state.status() == AgentRunStatus::Unknown {
                    break view;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(view.state.status(), AgentRunStatus::Unknown);
        assert!(view.delivery.is_none());
        assert!(matches!(
            view.state,
            orchestral_core::agent_protocol::wire::AgentRunState::Unknown { ref reason, .. }
                if reason.contains("disconnected")
        ));
    }

    #[tokio::test]
    async fn failed_resume_does_not_poison_the_next_attempt_for_that_session() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let first_resume = next_request(&mut lines).await;
            assert_eq!(first_resume["method"], "thread/resume");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": first_resume["id"], "error": {"code": -32000, "message": "temporary load failure"}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let second_resume = next_request(&mut lines).await;
            assert_eq!(second_resume["method"], "thread/resume");
            assert_eq!(second_resume["params"]["threadId"], "thread-retry");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": second_resume["id"], "result": {"thread": {"id": "thread-retry"}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let start = next_request(&mut lines).await;
            assert_eq!(start["method"], "turn/start");
            server_write
                .write_all(format!("{}\n", json!({"id": start["id"], "result": {"turn": {"id": "turn-retry", "status": "inProgress", "items": []}}})).as_bytes())
                .await
                .unwrap();
            server_write
                .write_all(format!("{}\n", json!({"method": "turn/completed", "params": {"threadId": "thread-retry", "turn": {"id": "turn-retry", "status": "completed", "items": []}}})).as_bytes())
                .await
                .unwrap();
        });

        assert!(matches!(
            connector
                .start(start_request("thread-retry", "run-retry-1"))
                .await,
            Err(AgentStartError::Rejected(_))
        ));
        let mut stream = connector
            .start(start_request("thread-retry", "run-retry-2"))
            .await
            .unwrap()
            .stream;
        loop {
            let item = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if matches!(
                item,
                AgentProviderStreamItem::Event(event)
                    if matches!(event.payload, AgentEvent::DeliveryCommitted { .. })
            ) {
                break;
            }
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn loaded_unmaterialized_thread_can_receive_its_first_turn_after_reconnect() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            assert_eq!(resume["method"], "thread/resume");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": resume["id"],
                            "error": {
                                "code": -32000,
                                "message": "no rollout found for thread id thread-empty-loaded"
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let loaded = next_request(&mut lines).await;
            assert_eq!(loaded["method"], "thread/loaded/list");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": loaded["id"],
                            "result": {"data": ["thread-empty-loaded"], "nextCursor": null}
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let start = next_request(&mut lines).await;
            assert_eq!(start["method"], "turn/start");
            assert_eq!(start["params"]["threadId"], "thread-empty-loaded");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": start["id"],
                            "result": {"turn": {"id": "turn-first", "status": "inProgress", "items": []}}
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "method": "turn/completed",
                            "params": {
                                "threadId": "thread-empty-loaded",
                                "turn": {"id": "turn-first", "status": "completed", "items": []}
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });

        let mut stream = connector
            .start(start_request("thread-empty-loaded", "run-first"))
            .await
            .unwrap()
            .stream;
        loop {
            let item = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if matches!(
                item,
                AgentProviderStreamItem::Event(event)
                    if matches!(event.payload, AgentEvent::DeliveryCommitted { .. })
            ) {
                break;
            }
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn reconnect_reads_authoritative_turn_without_starting_duplicate_work() {
        let (first_client_io, first_server_io) = duplex(1024 * 1024);
        let (first_client_read, first_client_write) = tokio::io::split(first_client_io);
        let (first_server_read, mut first_server_write) = tokio::io::split(first_server_io);
        let first_rpc = CodexRpcClient::from_io(
            first_client_read,
            first_client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let (second_client_io, second_server_io) = duplex(1024 * 1024);
        let (second_client_read, second_client_write) = tokio::io::split(second_client_io);
        let (second_server_read, mut second_server_write) = tokio::io::split(second_server_io);
        let second_rpc = CodexRpcClient::from_io(
            second_client_read,
            second_client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = Arc::new(CodexConnector::with_reconnect_client(first_rpc, second_rpc));
        let first_server = tokio::spawn(async move {
            let mut lines = BufReader::new(first_server_read).lines();
            let resume = next_request(&mut lines).await;
            first_server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": resume["id"], "result": {"thread": {"id": "thread-recover"}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let start = next_request(&mut lines).await;
            assert_eq!(start["method"], "turn/start");
            first_server_write
                .write_all(format!("{}\n", json!({"id": start["id"], "result": {"turn": {"id": "turn-recover", "status": "inProgress", "items": []}}})).as_bytes())
                .await
                .unwrap();
        });
        let second_server = tokio::spawn(async move {
            let mut lines = BufReader::new(second_server_read).lines();
            let resume = next_request(&mut lines).await;
            assert_eq!(resume["method"], "thread/resume");
            second_server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": resume["id"], "result": {"thread": {"id": "thread-recover"}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let read = next_request(&mut lines).await;
            assert_eq!(read["method"], "thread/read");
            second_server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": read["id"],
                            "result": {"thread": {
                                "id": "thread-recover",
                                "turns": [{
                                    "id": "turn-recover",
                                    "status": "completed",
                                    "items": [{
                                        "type": "agentMessage",
                                        "id": "answer-recover",
                                        "text": "recovered answer"
                                    }]
                                }]
                            }}
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });

        let provider: Arc<dyn AgentProvider> = connector;
        let controller = Arc::new(
            AgentController::new(provider, ProviderBindingRef::new("codex/local")).unwrap(),
        );
        let run = AgentRunEnvelope::new(
            ProtocolVersion::new(1, 0),
            AgentSessionId::new("thread-recover"),
            RunId::new("run-recover"),
            vec![Content::text("perform once")],
        )
        .unwrap();
        let execution = controller.start(run).await.unwrap();
        first_server.await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if controller
                    .inspect(&execution.run_id)
                    .await
                    .unwrap()
                    .state
                    .status()
                    == AgentRunStatus::Unknown
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        controller.recover(&execution.run_id).await.unwrap();
        let view = tokio::time::timeout(
            Duration::from_secs(1),
            controller.wait_for_terminal(&execution.run_id),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(view.state.status(), AgentRunStatus::Delivered);
        assert_eq!(
            view.delivery.unwrap().final_response,
            Content::text("recovered answer")
        );
        second_server.await.unwrap();
    }
}
