use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use futures_util::stream::{self, StreamExt};
use orchestral_core::agent_connector::AgentSessionActionInvocation;
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

use crate::transport::{AcpRpcClient, AcpTransportError, AcpTransportEvent};
use crate::AcpConnector;

const PROVIDER_ID: &str = "acp/stdio";

#[derive(Default)]
pub(crate) struct ProviderState {
    runs: BTreeMap<RunId, Arc<AcpRun>>,
    sessions: BTreeMap<AgentSessionId, RunId>,
    pub(crate) loaded_sessions: std::collections::BTreeSet<AgentSessionId>,
}

impl ProviderState {
    pub(crate) fn reset_connection_state(&mut self) {
        self.loaded_sessions.clear();
    }
}

struct NativePendingRequest {
    rpc_id: Value,
    options: Vec<Value>,
}

struct AcpRun {
    request: AgentStartRequest,
    execution: AgentExecutionRef,
    admission: AgentAdmission,
    sender: broadcast::Sender<Result<AgentProviderStreamItem, AgentProtocolError>>,
    durable: Mutex<Vec<AgentEventDraft>>,
    final_response: Mutex<String>,
    pending: Mutex<BTreeMap<RequestId, NativePendingRequest>>,
    commands: Mutex<BTreeMap<String, (Digest, ProviderCommandDisposition)>>,
    telemetry_seq: AtomicU64,
    terminal: AtomicBool,
}

impl AcpConnector {
    fn provider_descriptor(&self) -> AgentDescriptorEnvelope {
        AgentDescriptorEnvelope::seal(AgentDescriptor {
            provider_id: AgentProviderId::new(PROVIDER_ID),
            agent_id: AgentId::new(self.config.connector_id.clone()),
            supported_protocol_versions: vec![ProtocolVersion::new(1, 0)],
            accepted_content_types: std::collections::BTreeSet::from(["text/plain".to_owned()]),
            capabilities: AgentCapabilities {
                session_reuse: true,
                structured_output: false,
                controls: ControlCapabilities {
                    steer: false,
                    cancel: CancelSupport::Confirmed,
                    recover: false,
                },
                pending_request_kinds: std::collections::BTreeSet::from([
                    PendingRequestKind::Approval,
                ]),
                supported_limits: std::collections::BTreeSet::new(),
                resources: Vec::new(),
                effect_mediation: EffectMediation::HostMediated,
            },
            extensions: Extensions::new(),
        })
        .expect("ACP connector descriptor must be valid")
    }

    async fn ensure_loaded(
        &self,
        rpc: &AcpRpcClient,
        session_id: &AgentSessionId,
    ) -> Result<(), AgentStartError> {
        if self.provider_state().loaded_sessions.contains(session_id) {
            return Ok(());
        }
        let summary = self.resolve_summary(session_id).await.map_err(|error| {
            AgentStartError::Rejected(AgentRejection::new(
                AgentRejectionCode::InvalidSpec,
                error.to_string(),
            ))
        })?;
        let cwd = summary.cwd.ok_or_else(|| {
            AgentStartError::Rejected(AgentRejection::new(
                AgentRejectionCode::InvalidSpec,
                "ACP session has no working directory",
            ))
        })?;
        rpc.request(
            "session/load",
            json!({"sessionId":session_id.as_str(),"cwd":cwd,"mcpServers":[]}),
        )
        .await
        .map_err(|error| {
            AgentStartError::Rejected(
                AgentRejection::new(AgentRejectionCode::ProviderUnavailable, error.to_string())
                    .with_retryable(is_retryable_transport(&error)),
            )
        })?;
        self.provider_state()
            .loaded_sessions
            .insert(session_id.clone());
        Ok(())
    }

    fn remove_run(&self, run: &AcpRun) {
        let mut state = self.provider_state();
        state.runs.remove(&run.execution.run_id);
        state.sessions.remove(&run.execution.session_id);
    }
}

#[async_trait]
impl AgentProvider for AcpConnector {
    fn describe(&self) -> AgentDescriptorEnvelope {
        self.provider_descriptor()
    }

    async fn start(&self, request: AgentStartRequest) -> Result<AgentStart, AgentStartError> {
        let descriptor = self.provider_descriptor();
        request
            .validate_for_descriptor(&descriptor)
            .map_err(|error| {
                AgentRejection::new(AgentRejectionCode::RunIdConflict, error.to_string())
            })?;
        if AgentSessionActionInvocation::from_run(&request.run.spec)
            .map_err(|error| {
                AgentRejection::new(AgentRejectionCode::InvalidSpec, error.to_string())
            })?
            .is_some()
        {
            return Err(AgentRejection::new(
                AgentRejectionCode::UnsupportedCapability,
                "ACP connector does not declare a native session action",
            )
            .into());
        }
        let input = acp_prompt(&request).map_err(|error| {
            AgentRejection::new(AgentRejectionCode::InvalidSpec, error.to_string())
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
                        "run_id already belongs to a different ACP start",
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
                        "ACP permits one Orchestral Run per active session",
                    )
                    .into());
                }
            }
            let (sender, _) = broadcast::channel(512);
            let run = Arc::new(AcpRun {
                request: request.clone(),
                execution: execution.clone(),
                admission: admission.clone(),
                sender,
                durable: Mutex::new(Vec::new()),
                final_response: Mutex::new(String::new()),
                pending: Mutex::new(BTreeMap::new()),
                commands: Mutex::new(BTreeMap::new()),
                telemetry_seq: AtomicU64::new(0),
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
                self.remove_run(&run);
                return Err(AgentStartError::Rejected(
                    AgentRejection::new(AgentRejectionCode::ProviderUnavailable, error.to_string())
                        .with_retryable(error.retryable),
                ));
            }
        };
        if let Err(error) = self
            .ensure_loaded(&connected.rpc, &execution.session_id)
            .await
        {
            self.remove_run(&run);
            return Err(error);
        }
        let events = connected.rpc.subscribe();
        publish_event(
            &run,
            AgentEventDraft {
                event_id: event_id(&run, "started"),
                run_id: run.execution.run_id.clone(),
                causation_id: None,
                source_fingerprint: None,
                payload: AgentEvent::RunStarted,
            },
        );
        let monitored = Arc::clone(&run);
        tokio::spawn(async move {
            drive_prompt(connected.rpc.clone(), monitored, input, events).await;
        });
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
                "command run does not match ACP execution",
            ));
        }
        let run = self
            .provider_state()
            .runs
            .get(&execution.run_id)
            .cloned()
            .ok_or_else(|| {
                AgentProtocolError::new(AgentProtocolErrorCode::RunNotFound, "ACP run not found")
            })?;
        if run.execution != *execution {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::RunIdConflict,
                "execution identity does not match ACP run",
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
        apply_command(&connected.rpc, &run, &command).await?;
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
        _request: AgentRecoveryRequest,
    ) -> Result<AgentRecovery, AgentProtocolError> {
        Err(AgentProtocolError::new(
            AgentProtocolErrorCode::Unsupported,
            "ACP 0.21 does not define reconnecting to an in-flight prompt",
        ))
    }
}

async fn drive_prompt(
    rpc: Arc<AcpRpcClient>,
    run: Arc<AcpRun>,
    input: Value,
    mut events: broadcast::Receiver<AcpTransportEvent>,
) {
    let prompt = rpc.request(
        "session/prompt",
        json!({"sessionId":run.execution.session_id.as_str(),"prompt":input}),
    );
    tokio::pin!(prompt);
    loop {
        tokio::select! {
            biased;
            response = &mut prompt => {
                drain_run_events(&rpc, &run, &mut events).await;
                match response {
                    Ok(response) => finish_prompt(&run, &response),
                    Err(error) => {
                        let _ = run.sender.send(Err(transport_to_protocol(error)));
                    }
                }
                return;
            }
            event = events.recv() => {
                match event {
                    Ok(AcpTransportEvent::Message(message)) => handle_message(&rpc, &run, message).await,
                    Ok(AcpTransportEvent::Disconnected { reason }) => {
                        let _ = run.sender.send(Err(protocol_error(
                            format!("ACP Agent disconnected: {reason}"), true,
                        )));
                        return;
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        let _ = run.sender.send(Err(AgentProtocolError::new(
                            AgentProtocolErrorCode::SequenceGap,
                            format!("ACP event subscriber lagged by {skipped}"),
                        )));
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        let _ = run.sender.send(Err(protocol_error("ACP event channel closed", true)));
                        return;
                    }
                }
            }
        }
    }
}

async fn drain_run_events(
    rpc: &AcpRpcClient,
    run: &Arc<AcpRun>,
    events: &mut broadcast::Receiver<AcpTransportEvent>,
) {
    loop {
        match events.try_recv() {
            Ok(AcpTransportEvent::Message(message)) => handle_message(rpc, run, message).await,
            Ok(AcpTransportEvent::Disconnected { .. })
            | Err(broadcast::error::TryRecvError::Empty)
            | Err(broadcast::error::TryRecvError::Closed) => return,
            Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
        }
    }
}

async fn handle_message(rpc: &AcpRpcClient, run: &Arc<AcpRun>, message: Value) {
    let method = message.get("method").and_then(Value::as_str);
    let session_id = message.pointer("/params/sessionId").and_then(Value::as_str);
    if session_id != Some(run.execution.session_id.as_str()) {
        return;
    }
    match method {
        Some("session/update") => {
            if let Some(update) = message.pointer("/params/update") {
                handle_update(run, update);
            }
        }
        Some("session/request_permission") => open_permission(run, &message),
        Some(_) if message.get("id").is_some() => {
            let _ = rpc
                .respond_error(
                    message["id"].clone(),
                    -32601,
                    "Orchestral did not advertise this ACP client method",
                )
                .await;
        }
        _ => {}
    }
}

fn handle_update(run: &Arc<AcpRun>, update: &Value) {
    match update.get("sessionUpdate").and_then(Value::as_str) {
        Some("agent_message_chunk") => {
            if let Some(text) = update.pointer("/content/text").and_then(Value::as_str) {
                lock(&run.final_response).push_str(text);
                publish_telemetry(
                    run,
                    AgentTelemetry::OutputDelta {
                        output_id: output_id(run),
                        delta: Content::text(text),
                    },
                );
            }
        }
        Some("agent_thought_chunk") => {
            if let Some(text) = update.pointer("/content/text").and_then(Value::as_str) {
                publish_telemetry(
                    run,
                    AgentTelemetry::ProgressReported {
                        message: safe_text(text, 512),
                        fraction: None,
                    },
                );
            }
        }
        Some("plan") => publish_telemetry(
            run,
            AgentTelemetry::ProgressReported {
                message: "ACP Agent updated its plan".to_owned(),
                fraction: None,
            },
        ),
        Some("tool_call" | "tool_call_update") => publish_tool(run, update),
        _ => {}
    }
}

fn publish_tool(run: &Arc<AcpRun>, update: &Value) {
    let id = update
        .get("toolCallId")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let title = update
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("ACP tool call");
    let state = match update.get("status").and_then(Value::as_str) {
        Some("failed") => ToolActivityState::Failed,
        Some("completed") => ToolActivityState::Succeeded,
        _ => ToolActivityState::Running,
    };
    let tool_name = update
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("acp_tool")
        .to_owned();
    publish_telemetry(
        run,
        AgentTelemetry::ToolActivity {
            activity_id: ToolActivityId::new(format!("acp-{id}")),
            tool_name,
            state,
            evidence: vec![ToolActivityEvidence::Note {
                text: safe_text(title, 512),
            }],
        },
    );
}

fn open_permission(run: &Arc<AcpRun>, message: &Value) {
    let Some(rpc_id) = message.get("id").cloned() else {
        return;
    };
    let params = message.get("params").cloned().unwrap_or(Value::Null);
    let tool = params.get("toolCall").cloned().unwrap_or(Value::Null);
    let native_id = tool
        .get("toolCallId")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| rpc_id.to_string());
    let request_id = RequestId::new(format!("acp-{native_id}"));
    let title = tool
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("ACP Agent requests permission");
    let options = params
        .get("options")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let requested_scope = permission_scopes(&tool);
    let session_scope = Digest::sha256(
        serde_json::to_vec(&json!({"title":title,"scope":requested_scope})).unwrap_or_default(),
    );
    lock(&run.pending).insert(request_id.clone(), NativePendingRequest { rpc_id, options });
    publish_event(
        run,
        AgentEventDraft {
            event_id: AgentEventId::new(format!(
                "acp-{}-request-{}",
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
                    payload: PendingRequestPayload::Approval {
                        operation_digest: Digest::sha256(
                            serde_json::to_vec(&tool).unwrap_or_default(),
                        ),
                        requested_scope,
                        session_approval_scope: Some(session_scope),
                        reason: safe_text(title, 1_000),
                    },
                },
            },
        },
    );
}

async fn apply_command(
    rpc: &AcpRpcClient,
    run: &Arc<AcpRun>,
    command: &AgentCommandEnvelope,
) -> Result<(), AgentProtocolError> {
    match &command.payload {
        AgentCommand::Cancel { .. } => {
            let pending = {
                let mut pending = lock(&run.pending);
                std::mem::take(&mut *pending)
            };
            for (_, native) in pending {
                rpc.respond(native.rpc_id, json!({"outcome":{"outcome":"cancelled"}}))
                    .await
                    .map_err(transport_to_protocol)?;
            }
            rpc.notify(
                "session/cancel",
                json!({"sessionId":run.execution.session_id.as_str()}),
            )
            .await
            .map_err(transport_to_protocol)?;
        }
        AgentCommand::ResolveRequest { response } => {
            let request_id = command.request_id.as_ref().ok_or_else(|| {
                AgentProtocolError::new(
                    AgentProtocolErrorCode::RequestNotFound,
                    "ACP permission resolution omitted request id",
                )
            })?;
            let native = lock(&run.pending).remove(request_id).ok_or_else(|| {
                AgentProtocolError::new(
                    AgentProtocolErrorCode::RequestNotFound,
                    "pending ACP permission was not found",
                )
            })?;
            let result = permission_resolution(&native.options, response)?;
            if let Err(error) = rpc.respond(native.rpc_id.clone(), result).await {
                lock(&run.pending).insert(request_id.clone(), native);
                return Err(transport_to_protocol(error));
            }
            publish_event(
                run,
                AgentEventDraft {
                    event_id: AgentEventId::new(format!(
                        "acp-{}-request-{}-resolved",
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
        AgentCommand::Steer { .. } => {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::Unsupported,
                "ACP 0.21 does not define steering an active prompt",
            ));
        }
        _ => {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::Unsupported,
                "ACP adapter does not support this command",
            ));
        }
    }
    Ok(())
}

fn permission_resolution(
    options: &[Value],
    response: &RequestResolution,
) -> Result<Value, AgentProtocolError> {
    let RequestResolution::Approval { decision, .. } = response else {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::RequestTypeMismatch,
            "ACP permission requires an approval resolution",
        ));
    };
    let kinds: &[&str] = match decision {
        ApprovalDecision::Allow => &["allow_once", "allow_always"],
        ApprovalDecision::Deny => &["reject_once", "reject_always"],
        _ => {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::Unsupported,
                "ACP adapter does not recognize this approval decision",
            ));
        }
    };
    let option_id = kinds.iter().find_map(|kind| {
        options.iter().find_map(|option| {
            (option.get("kind").and_then(Value::as_str) == Some(*kind))
                .then(|| option.get("optionId").and_then(Value::as_str))
                .flatten()
        })
    });
    Ok(option_id.map_or_else(
        || json!({"outcome":{"outcome":"cancelled"}}),
        |option_id| json!({"outcome":{"outcome":"selected","optionId":option_id}}),
    ))
}

fn permission_scopes(tool: &Value) -> Vec<String> {
    let mut scopes = vec!["external_side_effect".to_owned()];
    if tool
        .get("locations")
        .and_then(Value::as_array)
        .is_some_and(|locations| !locations.is_empty())
    {
        scopes.push("filesystem_write".to_owned());
    }
    match tool.get("kind").and_then(Value::as_str) {
        Some("fetch" | "search") => scopes.push("network".to_owned()),
        Some("execute") => scopes.push("process".to_owned()),
        _ => {}
    }
    scopes
}

fn finish_prompt(run: &Arc<AcpRun>, response: &Value) {
    if run.terminal.load(Ordering::SeqCst) {
        return;
    }
    match response.get("stopReason").and_then(Value::as_str) {
        Some("end_turn") => finish_delivery(run),
        Some("cancelled") => {
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
                        reason: "ACP Agent cancelled the prompt".to_owned(),
                    },
                },
            );
        }
        reason => publish_failure(
            run,
            "acp_prompt_failed",
            &format!("ACP prompt stopped with unsupported reason: {reason:?}"),
            false,
        ),
    }
}

fn finish_delivery(run: &Arc<AcpRun>) {
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
                        "acp-{}-delivery",
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

fn publish_failure(run: &Arc<AcpRun>, code: &str, message: &str, retryable: bool) {
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

fn publish_event(run: &Arc<AcpRun>, draft: AgentEventDraft) {
    lock(&run.durable).push(draft.clone());
    let _ = run
        .sender
        .send(Ok(AgentProviderStreamItem::Event(Box::new(draft))));
}

fn publish_telemetry(run: &Arc<AcpRun>, payload: AgentTelemetry) {
    let sequence = run.telemetry_seq.fetch_add(1, Ordering::SeqCst) + 1;
    let _ = run.sender.send(Ok(AgentProviderStreamItem::Telemetry(
        AgentTelemetryEnvelope {
            telemetry_id: TelemetryId::new(format!(
                "acp-{}-telemetry-{sequence}",
                run.execution.run_id.as_str()
            )),
            run_id: run.execution.run_id.clone(),
            provider_seq: Some(sequence),
            payload,
        },
    )));
}

fn stream_for(run: &AcpRun) -> AgentProviderStream {
    let receiver = run.sender.subscribe();
    let replay = lock(&run.durable)
        .clone()
        .into_iter()
        .map(|draft| Ok(AgentProviderStreamItem::Event(Box::new(draft))));
    let replay = stream::iter(replay);
    if run.terminal.load(Ordering::SeqCst) {
        return replay.boxed();
    }
    let live = stream::unfold(receiver, |mut receiver| async move {
        match receiver.recv().await {
            Ok(item) => Some((item, receiver)),
            Err(broadcast::error::RecvError::Lagged(skipped)) => Some((
                Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::SequenceGap,
                    format!("ACP stream subscriber lagged by {skipped}"),
                )),
                receiver,
            )),
            Err(broadcast::error::RecvError::Closed) => None,
        }
    });
    replay.chain(live).boxed()
}

fn acp_prompt(request: &AgentStartRequest) -> Result<Value, AgentProtocolError> {
    request
        .run
        .spec
        .input
        .iter()
        .map(|content| match (&content.media_type[..], &content.body) {
            ("text/plain", ContentBody::Inline(Value::String(text))) => {
                Ok(json!({"type":"text","text":text}))
            }
            _ => Err(AgentProtocolError::new(
                AgentProtocolErrorCode::Unsupported,
                "ACP adapter currently accepts inline text/plain content only",
            )),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

fn output_id(run: &AcpRun) -> OutputId {
    OutputId::new(format!("acp-{}-response", run.execution.run_id.as_str()))
}

fn event_id(run: &AcpRun, suffix: &str) -> AgentEventId {
    AgentEventId::new(format!("acp-{}-{suffix}", run.execution.run_id.as_str()))
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

fn connector_to_protocol(
    error: orchestral_core::agent_connector::AgentConnectorError,
) -> AgentProtocolError {
    AgentProtocolError::new(
        AgentProtocolErrorCode::ProviderUnavailable,
        error.to_string(),
    )
    .with_retryable(error.retryable)
}

fn transport_to_protocol(error: AcpTransportError) -> AgentProtocolError {
    let retryable = is_retryable_transport(&error);
    protocol_error(error.to_string(), retryable)
}

fn is_retryable_transport(error: &AcpTransportError) -> bool {
    matches!(
        error,
        AcpTransportError::Io(_)
            | AcpTransportError::Closed
            | AcpTransportError::Disconnected(_)
            | AcpTransportError::Timeout
    )
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
    use std::collections::BTreeMap;
    use std::time::Duration;

    use orchestral_core::agent_connector::{AgentConnector, CreateAgentSessionRequest};
    use orchestral_core::agent_protocol::wire::{
        AgentCommandEnvelope, AgentRunEnvelope, ApprovalGrantRef, CommandId, ProviderBindingRef,
    };
    use orchestral_runtime::AgentController;
    use tokio::io::{duplex, AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::time::timeout;

    use super::*;
    use crate::{AcpConnectorConfig, AcpProcessConfig};

    #[tokio::test]
    async fn acp_fixture_drives_prompt_permission_tool_and_delivery_through_controller() {
        let (client_io, server_io) = duplex(256 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = AcpRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(2),
            1024 * 1024,
        );
        let connector = Arc::new(AcpConnector::with_client(
            AcpConnectorConfig::new(
                "acp/fixture",
                "ACP Fixture",
                AcpProcessConfig::new("unused"),
            ),
            rpc,
        ));
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let create = read_request(&mut lines).await;
            write_result(&mut server_write, &create, json!({"sessionId":"s1"})).await;
            let prompt = read_request(&mut lines).await;
            assert_eq!(prompt["method"], "session/prompt");
            assert_eq!(prompt["params"]["prompt"][0]["text"], "fix it");
            for update in [
                json!({"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Fixed "}}),
                json!({"sessionUpdate":"tool_call","toolCallId":"tool-1","title":"Edit src/lib.rs","kind":"edit","status":"in_progress","locations":[{"path":"src/lib.rs"}]}),
            ] {
                write_notification(
                    &mut server_write,
                    "session/update",
                    json!({"sessionId":"s1","update":update}),
                )
                .await;
            }
            server_write.write_all(format!("{}\n", json!({
                "jsonrpc":"2.0","id":"permission-1","method":"session/request_permission",
                "params":{"sessionId":"s1","toolCall":{"toolCallId":"tool-1","title":"Edit src/lib.rs","kind":"edit","locations":[{"path":"src/lib.rs"}]},"options":[
                    {"optionId":"once","kind":"allow_once","name":"Allow once"},
                    {"optionId":"deny","kind":"reject_once","name":"Deny"}
                ]}
            })).as_bytes()).await.unwrap();
            let permission = read_request(&mut lines).await;
            assert_eq!(permission["id"], "permission-1");
            assert_eq!(permission["result"]["outcome"]["optionId"], "once");
            write_notification(&mut server_write, "session/update", json!({"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"successfully."}}})).await;
            write_result(&mut server_write, &prompt, json!({"stopReason":"end_turn"})).await;
            let second_prompt = read_request(&mut lines).await;
            assert_eq!(second_prompt["method"], "session/prompt");
            write_notification(&mut server_write, "session/update", json!({"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Second turn."}}})).await;
            write_result(
                &mut server_write,
                &second_prompt,
                json!({"stopReason":"end_turn"}),
            )
            .await;
        });

        connector
            .create_session(CreateAgentSessionRequest {
                cwd: Some("/repo".to_owned()),
                title: None,
                extensions: BTreeMap::new(),
            })
            .await
            .unwrap();
        let controller = Arc::new(
            AgentController::new(connector.clone(), ProviderBindingRef::new("acp/fixture"))
                .unwrap(),
        );
        let run = AgentRunEnvelope::new(
            ProtocolVersion::new(1, 0),
            AgentSessionId::new("s1"),
            RunId::new("run-1"),
            vec![Content::text("fix it")],
        )
        .unwrap();
        let execution = controller.start(run).await.unwrap();
        let request_id = timeout(Duration::from_secs(2), async {
            loop {
                let view = controller.inspect(&execution.run_id).await.unwrap();
                if let Some(request) = view.pending_requests.into_iter().next() {
                    break request.request_id;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let command = AgentCommandEnvelope::new(
            CommandId::new("allow-1"),
            RunId::new("run-1"),
            Some(request_id),
            AgentCommand::ResolveRequest {
                response: RequestResolution::Approval {
                    decision: ApprovalDecision::Allow,
                    grant_ref: Some(ApprovalGrantRef::new("fixture-grant")),
                },
            },
        )
        .unwrap();
        controller.command(command).await.unwrap();
        let delivery = timeout(
            Duration::from_secs(2),
            controller.wait_for_terminal(&execution.run_id),
        )
        .await
        .unwrap()
        .unwrap()
        .delivery
        .unwrap();
        assert_eq!(
            delivery.final_response,
            Content::text("Fixed successfully.")
        );
        let second_run = AgentRunEnvelope::new(
            ProtocolVersion::new(1, 0),
            AgentSessionId::new("s1"),
            RunId::new("run-2"),
            vec![Content::text("continue")],
        )
        .unwrap();
        let second_execution = controller.start(second_run).await.unwrap();
        let second_delivery = timeout(
            Duration::from_secs(2),
            controller.wait_for_terminal(&second_execution.run_id),
        )
        .await
        .unwrap()
        .unwrap()
        .delivery
        .unwrap();
        assert_eq!(
            second_delivery.final_response,
            Content::text("Second turn.")
        );
        server.await.unwrap();
    }

    async fn read_request(
        lines: &mut tokio::io::Lines<BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>>,
    ) -> Value {
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap()
    }

    async fn write_result<W: tokio::io::AsyncWrite + Unpin>(
        writer: &mut W,
        request: &Value,
        result: Value,
    ) {
        writer
            .write_all(
                format!(
                    "{}\n",
                    json!({"jsonrpc":"2.0","id":request["id"],"result":result})
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    }

    async fn write_notification<W: tokio::io::AsyncWrite + Unpin>(
        writer: &mut W,
        method: &str,
        params: Value,
    ) {
        writer
            .write_all(
                format!(
                    "{}\n",
                    json!({"jsonrpc":"2.0","method":method,"params":params})
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    }
}
