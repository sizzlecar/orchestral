use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::stream::{self, StreamExt};
use orchestral_core::agent_connector::{AgentSessionActionInvocation, AgentSessionState};
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
    RunId, TelemetryId, ToolActivityEvidence, ToolActivityId, ToolActivityState, UsageReport,
};
use serde_json::{json, Value};
use tokio::sync::{broadcast, Mutex as AsyncMutex};

use crate::transport::{ClaudeStream, TransportError, TransportEvent};
use crate::{lock, ClaudeCodeConnector, CONNECTOR_ID};

#[derive(Default)]
pub(crate) struct ProviderState {
    runs: BTreeMap<RunId, Arc<ClaudeRun>>,
    sessions: BTreeMap<AgentSessionId, RunId>,
    #[cfg(test)]
    clients: std::collections::VecDeque<Arc<ClaudeStream>>,
}

impl ProviderState {
    pub(crate) fn session_state(&self, id: &AgentSessionId) -> Option<AgentSessionState> {
        let run = self.sessions.get(id).and_then(|run| self.runs.get(run))?;
        if run.terminal.load(Ordering::Acquire) {
            return Some(AgentSessionState::Idle);
        }
        let pending = lock(&run.pending);
        if pending
            .values()
            .any(|item| item.tool_name == "AskUserQuestion")
        {
            Some(AgentSessionState::WaitingInput)
        } else if !pending.is_empty() {
            Some(AgentSessionState::WaitingApproval)
        } else {
            Some(AgentSessionState::Active)
        }
    }
}

#[derive(Clone)]
struct NativeRequest {
    id: String,
    tool_name: String,
    input: Value,
}

struct ClaudeRun {
    request: AgentStartRequest,
    execution: AgentExecutionRef,
    admission: AgentAdmission,
    rpc: Mutex<Option<Arc<ClaudeStream>>>,
    critical: broadcast::Sender<Result<AgentEventDraft, AgentProtocolError>>,
    telemetry: broadcast::Sender<AgentTelemetryEnvelope>,
    durable: Mutex<Vec<AgentEventDraft>>,
    final_response: Mutex<String>,
    pending: Mutex<BTreeMap<RequestId, NativeRequest>>,
    commands: Mutex<BTreeMap<String, (Digest, ProviderCommandDisposition)>>,
    command_gate: AsyncMutex<()>,
    event_gate: AsyncMutex<()>,
    telemetry_seq: AtomicU64,
    terminal: AtomicBool,
    cancelling: AtomicBool,
    loss: Mutex<Option<AgentProtocolError>>,
}

impl ClaudeCodeConnector {
    fn provider_descriptor(&self) -> AgentDescriptorEnvelope {
        AgentDescriptorEnvelope::seal(AgentDescriptor {
            provider_id: AgentProviderId::new("claude-code/stream-json"),
            agent_id: AgentId::new(CONNECTOR_ID),
            supported_protocol_versions: vec![ProtocolVersion::new(1, 0)],
            accepted_content_types: BTreeSet::from(["text/plain".to_owned()]),
            capabilities: AgentCapabilities {
                session_reuse: true,
                structured_output: false,
                controls: ControlCapabilities {
                    steer: false,
                    cancel: CancelSupport::Confirmed,
                    recover: false,
                },
                pending_request_kinds: BTreeSet::from([
                    PendingRequestKind::Approval,
                    PendingRequestKind::Input,
                ]),
                supported_limits: BTreeSet::new(),
                resources: Vec::new(),
                effect_mediation: EffectMediation::ProviderManaged,
            },
            extensions: Extensions::new(),
        })
        .expect("Claude provider descriptor must be valid")
    }

    fn remove_run(&self, run: &ClaudeRun) {
        let mut state = self.provider_state();
        state.sessions.remove(&run.execution.session_id);
        state.runs.remove(&run.execution.run_id);
    }

    async fn spawn_stream(
        &self,
        cwd: &Path,
        args: &[String],
    ) -> Result<Arc<ClaudeStream>, TransportError> {
        #[cfg(test)]
        if let Some(client) = self.provider_state().clients.pop_front() {
            return Ok(client);
        }
        ClaudeStream::spawn(&self.config, cwd, args).await
    }
}

#[async_trait]
impl AgentProvider for ClaudeCodeConnector {
    fn describe(&self) -> AgentDescriptorEnvelope {
        self.provider_descriptor()
    }

    async fn start(&self, request: AgentStartRequest) -> Result<AgentStart, AgentStartError> {
        let descriptor = self.provider_descriptor();
        request
            .validate_for_descriptor(&descriptor)
            .map_err(|error| reject(AgentRejectionCode::InvalidSpec, error.to_string()))?;
        if AgentSessionActionInvocation::from_run(&request.run.spec)
            .map_err(|error| reject(AgentRejectionCode::InvalidSpec, error.to_string()))?
            .is_some()
        {
            return Err(reject(
                AgentRejectionCode::UnsupportedCapability,
                "Claude connector does not declare session actions",
            ));
        }
        let input = prompt(&request.run.spec.input)
            .map_err(|error| reject(AgentRejectionCode::InvalidSpec, error.to_string()))?;
        let compatibility = descriptor
            .descriptor
            .check_run_compatibility(&request.run)
            .map_err(AgentStartError::Rejected)?;
        let admission = AgentAdmission {
            skipped_optional_bindings: compatibility.skipped_optional_bindings.clone(),
        };
        admission
            .validate_against(&request.run, &compatibility)
            .map_err(|error| reject(AgentRejectionCode::InvalidSpec, error.to_string()))?;
        let execution = AgentExecutionRef::for_start(&request, &descriptor)
            .map_err(|error| reject(AgentRejectionCode::InvalidSpec, error.to_string()))?;
        {
            let state = self.provider_state();
            if let Some(run) = state.runs.get(&execution.run_id) {
                if run.request != request || run.execution != execution {
                    return Err(reject(
                        AgentRejectionCode::RunIdConflict,
                        "run_id belongs to another Claude input",
                    ));
                }
                return Ok(AgentStart {
                    execution: run.execution.clone(),
                    admission: run.admission.clone(),
                    stream: stream_for(run.clone()),
                });
            }
        }
        let detail = orchestral_core::agent_connector::AgentConnector::read_session(
            self,
            &execution.session_id,
        )
        .await
        .map_err(|error| reject(AgentRejectionCode::InvalidSpec, error.to_string()))?;
        if self
            .native_owner(&execution.session_id)
            .await
            .map_err(|error| reject(AgentRejectionCode::ProviderUnavailable, error.to_string()))?
        {
            return Err(reject(
                AgentRejectionCode::SessionConflict,
                "该会话正在原 Claude 终端运行，请刷新会话后通过现有进程的发送通道提交消息。",
            ));
        }
        let cwd = detail.summary.cwd.ok_or_else(|| {
            reject(
                AgentRejectionCode::InvalidSpec,
                "Claude session has no working directory",
            )
        })?;
        let resume = self
            .transcript_exists(&execution.session_id)
            .await
            .map_err(|error| reject(AgentRejectionCode::ProviderUnavailable, error.to_string()))?;
        let args = process_args(&execution.session_id, resume);
        let run = {
            let mut state = self.provider_state();
            if state
                .sessions
                .get(&execution.session_id)
                .and_then(|id| state.runs.get(id))
                .is_some_and(|run| !run.terminal.load(Ordering::Acquire))
            {
                return Err(reject(
                    AgentRejectionCode::SessionConflict,
                    "Claude already has an active Host Run for this session",
                ));
            }
            let (critical, _) = broadcast::channel(1024);
            let (telemetry, _) = broadcast::channel(512);
            let run = Arc::new(ClaudeRun {
                request: request.clone(),
                execution: execution.clone(),
                admission: admission.clone(),
                rpc: Mutex::new(None),
                critical,
                telemetry,
                durable: Mutex::default(),
                final_response: Mutex::default(),
                pending: Mutex::default(),
                commands: Mutex::default(),
                command_gate: AsyncMutex::new(()),
                event_gate: AsyncMutex::new(()),
                telemetry_seq: AtomicU64::new(0),
                terminal: AtomicBool::new(false),
                cancelling: AtomicBool::new(false),
                loss: Mutex::new(None),
            });
            state
                .sessions
                .insert(execution.session_id.clone(), execution.run_id.clone());
            state.runs.insert(execution.run_id.clone(), run.clone());
            run
        };
        let rpc = match self.spawn_stream(Path::new(&cwd), &args).await {
            Ok(rpc) => rpc,
            Err(error) => {
                self.remove_run(&run);
                return Err(reject(
                    AgentRejectionCode::ProviderUnavailable,
                    error.to_string(),
                ));
            }
        };
        let events = rpc.subscribe();
        if let Err(error) = rpc
            .control(json!({"subtype":"initialize","hooks":null}))
            .await
        {
            let _ = rpc.close().await;
            self.remove_run(&run);
            return Err(reject(
                AgentRejectionCode::ProviderUnavailable,
                error.to_string(),
            ));
        }
        *lock(&run.rpc) = Some(rpc.clone());
        publish(&run, "started", AgentEvent::RunStarted, None);
        let stream = stream_for(run.clone());
        // Commit input before exposing command control. An immediate cancel
        // must never overtake the first user message on stdin.
        if let Err(error) = rpc.write(&json!({"type":"user","session_id":run.execution.session_id.as_str(),"uuid":uuid::Uuid::new_v4().to_string(),"parent_tool_use_id":null,"message":{"role":"user","content":input}})).await {
            lose_continuity(&run, &error.to_string());
            let _ = rpc.close().await;
        } else {
            tokio::spawn(drive(rpc, run, events));
        }
        Ok(AgentStart {
            execution,
            admission,
            stream,
        })
    }

    async fn command(
        &self,
        execution: &AgentExecutionRef,
        command: AgentCommandEnvelope,
    ) -> Result<ProviderCommandDisposition, AgentProtocolError> {
        command.verify_digest()?;
        let run = self
            .provider_state()
            .runs
            .get(&execution.run_id)
            .cloned()
            .ok_or_else(|| {
                protocol(
                    AgentProtocolErrorCode::RunNotFound,
                    "Claude Run does not exist",
                )
            })?;
        if run.execution != *execution || command.run_id != execution.run_id {
            return Err(protocol(
                AgentProtocolErrorCode::RunIdConflict,
                "Claude command execution identity mismatch",
            ));
        }
        let _guard = run.command_gate.lock().await;
        if let Some((digest, disposition)) = lock(&run.commands).get(command.command_id.as_str()) {
            if *digest != command.command_digest {
                return Err(protocol(
                    AgentProtocolErrorCode::DuplicateConflict,
                    "Claude command id was reused with different input",
                ));
            }
            let mut duplicate = disposition.clone();
            duplicate.duplicate = true;
            return Ok(duplicate);
        }
        if run.terminal.load(Ordering::Acquire) {
            return Err(protocol(
                AgentProtocolErrorCode::TerminalRun,
                "Claude Run has ended",
            ));
        }
        let rpc = lock(&run.rpc).clone().ok_or_else(|| {
            protocol(
                AgentProtocolErrorCode::ProviderUnavailable,
                "Claude stream is not initialized",
            )
        })?;
        match &command.payload {
            AgentCommand::Cancel { reason } => {
                let _events = run.event_gate.lock().await;
                if run.cancelling.load(Ordering::Acquire) {
                    return Err(protocol(
                        AgentProtocolErrorCode::InvalidTransition,
                        "Claude cancellation is already pending",
                    ));
                }
                run.cancelling.store(true, Ordering::Release);
                // An interrupt acknowledgement alone is not terminal evidence.
                // The driver confirms cancellation after the process exits.
                if let Err(error) = rpc.control(json!({"subtype":"interrupt"})).await {
                    run.cancelling.store(false, Ordering::Release);
                    return Err(transport_error(error));
                }
                publish(
                    &run,
                    "stop-requested",
                    AgentEvent::StopRequested {
                        reason: reason.clone(),
                    },
                    Some(command.command_id.clone()),
                );
            }
            AgentCommand::ResolveRequest { response } => {
                let _events = run.event_gate.lock().await;
                let id = command.request_id.as_ref().ok_or_else(|| {
                    protocol(
                        AgentProtocolErrorCode::RequestNotFound,
                        "Claude response requires a request id",
                    )
                })?;
                let native = lock(&run.pending).get(id).cloned().ok_or_else(|| {
                    protocol(
                        AgentProtocolErrorCode::RequestNotFound,
                        "Claude request is no longer pending",
                    )
                })?;
                let result = resolution(&native, response)?;
                rpc.respond(&native.id, result)
                    .await
                    .map_err(transport_error)?;
                lock(&run.pending).remove(id);
                publish(
                    &run,
                    &format!("request:{id}:resolved"),
                    AgentEvent::RequestResolved {
                        request_id: id.clone(),
                        resolution: response.clone(),
                        resolution_digest: response.digest()?,
                    },
                    Some(command.command_id.clone()),
                );
            }
            _ => {
                return Err(protocol(
                    AgentProtocolErrorCode::Unsupported,
                    "Claude connector supports cancellation and request resolution",
                ))
            }
        }
        let disposition = ProviderCommandDisposition {
            command_id: command.command_id.clone(),
            run_id: command.run_id.clone(),
            outcome: ProviderCommandOutcome::Accepted,
            duplicate: false,
        };
        lock(&run.commands).insert(
            command.command_id.as_str().to_owned(),
            (command.command_digest, disposition.clone()),
        );
        Ok(disposition)
    }

    async fn recover(
        &self,
        _request: AgentRecoveryRequest,
    ) -> Result<AgentRecovery, AgentProtocolError> {
        Err(protocol(AgentProtocolErrorCode::Unsupported, "Claude stdio cannot reconnect to an in-flight process; its saved session remains available for a new turn"))
    }
}

fn process_args(id: &AgentSessionId, resume: bool) -> Vec<String> {
    [
        "--print",
        "--verbose",
        "--input-format=stream-json",
        "--output-format=stream-json",
        "--include-partial-messages",
        "--permission-prompt-tool=stdio",
        "--permission-mode=manual",
    ]
    .into_iter()
    .map(str::to_owned)
    .chain([format!(
        "{}={id}",
        if resume { "--resume" } else { "--session-id" }
    )])
    .collect()
}

async fn drive(
    rpc: Arc<ClaudeStream>,
    run: Arc<ClaudeRun>,
    mut events: broadcast::Receiver<TransportEvent>,
) {
    loop {
        let event = match events.recv().await {
            Ok(event) => event,
            Err(error) => {
                lose_continuity(&run, &error.to_string());
                let _ = rpc.close().await;
                return;
            }
        };
        match event {
            TransportEvent::Disconnected(reason) => {
                let closed = rpc.close().await;
                let _guard = run.event_gate.lock().await;
                if run.cancelling.load(Ordering::Acquire) && closed.is_ok() {
                    cancel(&run);
                } else {
                    lose_continuity(&run, &reason);
                }
                return;
            }
            TransportEvent::Message(message) => {
                if message
                    .get("parent_tool_use_id")
                    .is_some_and(|value| !value.is_null())
                {
                    continue;
                }
                if let Some(id) = message.get("session_id").and_then(Value::as_str) {
                    if id != run.execution.session_id.as_str() {
                        lose_continuity(&run, "Claude stream returned another session");
                        let _ = rpc.close().await;
                        return;
                    }
                }
                match message.get("type").and_then(Value::as_str) {
                    Some("result") => {
                        if let Err(error) = rpc.close().await {
                            lose_continuity(&run, &error.to_string());
                            return;
                        }
                        let _guard = run.event_gate.lock().await;
                        close_pending(&run);
                        if run.cancelling.load(Ordering::Acquire) {
                            cancel(&run);
                        } else if message.get("subtype").and_then(Value::as_str) == Some("success")
                            && message.get("is_error").and_then(Value::as_bool) != Some(true)
                        {
                            deliver(&run, &message);
                        } else {
                            fail(&run, "claude_turn_failed", &result_error(&message));
                        }
                        return;
                    }
                    Some("assistant") => handle_assistant(&run, &message),
                    Some("user") => handle_tools(&run, &message),
                    Some("stream_event") => {
                        if let Some(text) =
                            message.pointer("/event/delta/text").and_then(Value::as_str)
                        {
                            telemetry(
                                &run,
                                AgentTelemetry::OutputDelta {
                                    output_id: output_id(&run),
                                    delta: Content::text(text),
                                },
                            );
                        } else if let Some(text) = message
                            .pointer("/event/delta/thinking")
                            .and_then(Value::as_str)
                        {
                            telemetry(
                                &run,
                                AgentTelemetry::ProgressReported {
                                    message: text.chars().take(512).collect(),
                                    fraction: None,
                                },
                            );
                        }
                    }
                    Some("control_request") => {
                        let _guard = run.event_gate.lock().await;
                        if let Some(id) = message.get("request_id").and_then(Value::as_str) {
                            if message.pointer("/request/subtype").and_then(Value::as_str)
                                == Some("can_use_tool")
                            {
                                open_request(&run, id, &message["request"]);
                            } else {
                                let _ = rpc
                                    .respond_error(
                                        id,
                                        "Orchestral did not advertise this control callback",
                                    )
                                    .await;
                            }
                        }
                    }
                    Some("control_cancel_request") => {
                        let _guard = run.event_gate.lock().await;
                        if let Some(id) = message.get("request_id").and_then(Value::as_str) {
                            let id = RequestId::new(format!("claude:{id}"));
                            if lock(&run.pending).remove(&id).is_some() {
                                publish(
                                    &run,
                                    &format!("request:{id}:closed"),
                                    AgentEvent::RequestClosed {
                                        request_id: id,
                                        reason: "Claude withdrew the request".to_owned(),
                                    },
                                    None,
                                );
                            }
                        }
                    }
                    Some("system") => telemetry(
                        &run,
                        AgentTelemetry::ProgressReported {
                            message: "Claude Code is running".to_owned(),
                            fraction: None,
                        },
                    ),
                    _ => {}
                }
            }
        }
    }
}

fn handle_assistant(run: &ClaudeRun, message: &Value) {
    let text = crate::history::text(message.pointer("/message/content").unwrap_or(&Value::Null));
    if !text.is_empty() {
        *lock(&run.final_response) = text;
    }
    for block in message
        .pointer("/message/content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if block.get("type").and_then(Value::as_str) == Some("tool_use") {
            tool_activity(
                run,
                block.get("id").and_then(Value::as_str).unwrap_or("unknown"),
                block.get("name").and_then(Value::as_str).unwrap_or("Tool"),
                ToolActivityState::Running,
                crate::history::text(&block["input"]),
            );
        }
    }
}

fn handle_tools(run: &ClaudeRun, message: &Value) {
    for block in message
        .pointer("/message/content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if block.get("type").and_then(Value::as_str) == Some("tool_result") {
            tool_activity(
                run,
                block
                    .get("tool_use_id")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
                "Tool",
                if block.get("is_error").and_then(Value::as_bool) == Some(true) {
                    ToolActivityState::Failed
                } else {
                    ToolActivityState::Succeeded
                },
                crate::history::text(&block["content"]),
            );
        }
    }
}

fn tool_activity(run: &ClaudeRun, id: &str, name: &str, state: ToolActivityState, note: String) {
    telemetry(
        run,
        AgentTelemetry::ToolActivity {
            activity_id: ToolActivityId::new(format!("claude:tool:{id}")),
            tool_name: name.to_owned(),
            state,
            evidence: if note.is_empty() {
                Vec::new()
            } else {
                vec![ToolActivityEvidence::Note {
                    text: note.chars().take(2048).collect(),
                }]
            },
        },
    );
}

fn open_request(run: &ClaudeRun, native_id: &str, request: &Value) {
    let id = RequestId::new(format!("claude:{native_id}"));
    if lock(&run.pending).contains_key(&id) {
        return;
    }
    let native = NativeRequest {
        id: native_id.to_owned(),
        tool_name: request
            .get("tool_name")
            .and_then(Value::as_str)
            .unwrap_or("Tool")
            .to_owned(),
        input: request.get("input").cloned().unwrap_or(json!({})),
    };
    let payload = if native.tool_name == "AskUserQuestion" {
        let questions = native
            .input
            .get("questions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|question| {
                let mut text = question
                    .get("question")
                    .and_then(Value::as_str)
                    .unwrap_or("Claude asks for input")
                    .to_owned();
                for option in question
                    .get("options")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    if let Some(label) = option.get("label").and_then(Value::as_str) {
                        text.push_str(&format!("\n- {label}"));
                    }
                }
                text
            })
            .collect::<Vec<_>>()
            .join("\n\n");
        PendingRequestPayload::Input {
            prompt: vec![Content::text(if questions.is_empty() {
                "Claude asks for input".to_owned()
            } else {
                questions
            })],
            input_schema: None,
        }
    } else {
        let reason = format!("{}: {}", native.tool_name, native.input);
        PendingRequestPayload::Approval {
            operation_digest: Digest::sha256(
                serde_json::to_vec(&json!({"tool_name":native.tool_name,"input":native.input}))
                    .unwrap_or_default(),
            ),
            requested_scope: vec!["external_side_effect".to_owned()],
            session_approval_scope: None,
            reason: reason.chars().take(4096).collect(),
        }
    };
    lock(&run.pending).insert(id.clone(), native);
    publish(
        run,
        &format!("request:{id}:opened"),
        AgentEvent::RequestOpened {
            request: PendingRequest {
                request_id: id,
                blocking: true,
                payload,
            },
        },
        None,
    );
}

fn resolution(
    native: &NativeRequest,
    response: &RequestResolution,
) -> Result<Value, AgentProtocolError> {
    match response {
        RequestResolution::Approval { decision, .. } if native.tool_name != "AskUserQuestion" => {
            match decision {
                ApprovalDecision::Allow => {
                    Ok(json!({"behavior":"allow","updatedInput":native.input}))
                }
                ApprovalDecision::Deny => {
                    Ok(json!({"behavior":"deny","message":"Declined by the Orchestral user"}))
                }
                _ => Err(protocol(
                    AgentProtocolErrorCode::Unsupported,
                    "unsupported Claude approval decision",
                )),
            }
        }
        RequestResolution::Input { content } if native.tool_name == "AskUserQuestion" => {
            let answer = prompt(content)?;
            let text = crate::history::text(&answer);
            let questions = native
                .input
                .get("questions")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    protocol(
                        AgentProtocolErrorCode::InvalidSpec,
                        "Claude input request omitted questions",
                    )
                })?;
            let mut input = native.input.clone();
            let answers = if questions.len() == 1 {
                json!({questions[0].get("question").and_then(Value::as_str).unwrap_or("answer"):text})
            } else {
                let answers: Value = serde_json::from_str(&text).map_err(|_| protocol(AgentProtocolErrorCode::InvalidSpec, "Reply to multiple Claude questions with a JSON object keyed by each question"))?;
                if !questions.iter().all(|question| {
                    question
                        .get("question")
                        .and_then(Value::as_str)
                        .is_some_and(|key| answers.get(key).is_some_and(Value::is_string))
                }) {
                    return Err(protocol(
                        AgentProtocolErrorCode::InvalidSpec,
                        "answer every Claude question",
                    ));
                }
                answers
            };
            input["answers"] = answers;
            Ok(json!({"behavior":"allow","updatedInput":input}))
        }
        _ => Err(protocol(
            AgentProtocolErrorCode::RequestTypeMismatch,
            "Claude request response type does not match",
        )),
    }
}

fn close_pending(run: &ClaudeRun) {
    let pending = std::mem::take(&mut *lock(&run.pending));
    for (id, _) in pending {
        publish(
            run,
            &format!("request:{id}:closed"),
            AgentEvent::RequestClosed {
                request_id: id,
                reason: "Claude turn ended".to_owned(),
            },
            None,
        );
    }
}

fn deliver(run: &ClaudeRun, result: &Value) {
    if run.terminal.swap(true, Ordering::AcqRel) {
        return;
    }
    let text = result
        .get("result")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| lock(&run.final_response).clone());
    let event = event_id(run, "output");
    publish(
        run,
        "output",
        AgentEvent::OutputCommitted {
            output_id: output_id(run),
            content: vec![Content::text(&text)],
        },
        None,
    );
    publish(
        run,
        "delivered",
        AgentEvent::DeliveryCommitted {
            delivery: AgentDelivery {
                delivery_id: DeliveryId::new(format!("claude:{}:delivery", run.execution.run_id)),
                run_id: run.execution.run_id.clone(),
                spec_digest: run.execution.spec_digest.clone(),
                final_response: Content::text(text),
                outputs: Vec::new(),
                artifacts: Vec::new(),
                unresolved_issues: Vec::new(),
                usage: Some(UsageReport {
                    input_tokens: result
                        .pointer("/usage/input_tokens")
                        .and_then(Value::as_u64),
                    output_tokens: result
                        .pointer("/usage/output_tokens")
                        .and_then(Value::as_u64),
                    tool_calls: None,
                    cost: None,
                }),
                provenance: Provenance {
                    provider_id: run.execution.provider_id.clone(),
                    agent_id: run.execution.agent_id.clone(),
                    supporting_event_ids: vec![event],
                    extensions: Extensions::new(),
                },
            },
        },
        None,
    );
}

fn fail(run: &ClaudeRun, code: &str, message: &str) {
    if run.terminal.swap(true, Ordering::AcqRel) {
        return;
    }
    close_pending(run);
    publish(
        run,
        "failed",
        AgentEvent::RunFailed {
            failure: AgentFailure {
                code: code.to_owned(),
                message: message.to_owned(),
                retryable: false,
                details: Value::Null,
            },
        },
        None,
    );
}

fn cancel(run: &ClaudeRun) {
    if run.terminal.swap(true, Ordering::AcqRel) {
        return;
    }
    close_pending(run);
    publish(
        run,
        "cancelled",
        AgentEvent::RunCancelled {
            reason: "Claude process confirmed interruption".to_owned(),
        },
        None,
    );
}

fn result_error(result: &Value) -> String {
    result
        .get("errors")
        .and_then(Value::as_array)
        .map(|errors| {
            errors
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join("\n")
        })
        .filter(|text| !text.is_empty())
        .or_else(|| {
            result
                .get("result")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| {
            format!(
                "Claude turn ended: {}",
                result.get("subtype").unwrap_or(&Value::Null)
            )
        })
}

fn publish(
    run: &ClaudeRun,
    suffix: &str,
    payload: AgentEvent,
    causation_id: Option<orchestral_core::agent_protocol::wire::CommandId>,
) {
    let draft = AgentEventDraft {
        event_id: event_id(run, suffix),
        run_id: run.execution.run_id.clone(),
        causation_id,
        source_fingerprint: None,
        payload,
    };
    lock(&run.durable).push(draft.clone());
    let _ = run.critical.send(Ok(draft));
}

fn telemetry(run: &ClaudeRun, payload: AgentTelemetry) {
    let seq = run.telemetry_seq.fetch_add(1, Ordering::Relaxed) + 1;
    let _ = run.telemetry.send(AgentTelemetryEnvelope {
        telemetry_id: TelemetryId::new(format!("claude:{}:telemetry:{seq}", run.execution.run_id)),
        run_id: run.execution.run_id.clone(),
        provider_seq: Some(seq),
        payload,
    });
}

fn stream_for(run: Arc<ClaudeRun>) -> AgentProviderStream {
    let critical = run.critical.subscribe();
    let telemetry = run.telemetry.subscribe();
    let records = lock(&run.durable).clone();
    let ended = records.iter().any(|record| terminal_event(&record.payload));
    let replay = stream::iter(
        records
            .into_iter()
            .map(|draft| Ok(AgentProviderStreamItem::Event(Box::new(draft)))),
    );
    if let Some(error) = lock(&run.loss).clone() {
        return replay
            .chain(stream::once(async move { Err(error) }))
            .boxed();
    }
    if ended {
        return replay.boxed();
    }
    replay.chain(stream::unfold((critical, telemetry, false), |(mut critical, mut telemetry, ended)| async move {
        if ended { return None; }
        let item = loop { break tokio::select! {
            biased;
            event = critical.recv() => match event {
                Ok(Ok(event)) => Ok(AgentProviderStreamItem::Event(Box::new(event))),
                Ok(Err(error)) => Err(error),
                Err(error) => Err(protocol(AgentProtocolErrorCode::SequenceGap, error.to_string())),
            },
            event = telemetry.recv() => match event {
                Ok(event) => Ok(AgentProviderStreamItem::Telemetry(event)),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(error) => Err(protocol(AgentProtocolErrorCode::SequenceGap, error.to_string())),
            },
        }; };
        let ended = item.is_err() || matches!(&item, Ok(AgentProviderStreamItem::Event(event)) if terminal_event(&event.payload));
        Some((item, (critical, telemetry, ended)))
    })).boxed()
}

fn lose_continuity(run: &ClaudeRun, message: &str) {
    if run.terminal.swap(true, Ordering::AcqRel) {
        return;
    }
    let error = protocol(AgentProtocolErrorCode::ProviderUnavailable, message);
    *lock(&run.loss) = Some(error.clone());
    let _ = run.critical.send(Err(error));
}

fn terminal_event(event: &AgentEvent) -> bool {
    matches!(
        event,
        AgentEvent::DeliveryCommitted { .. }
            | AgentEvent::RunFailed { .. }
            | AgentEvent::RunCancelled { .. }
    )
}

fn prompt(content: &[Content]) -> Result<Value, AgentProtocolError> {
    content
        .iter()
        .map(|content| match (&content.media_type[..], &content.body) {
            ("text/plain", ContentBody::Inline(Value::String(text))) => {
                Ok(json!({"type":"text","text":text}))
            }
            _ => Err(protocol(
                AgentProtocolErrorCode::Unsupported,
                "Claude connector currently accepts inline text/plain content",
            )),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

fn output_id(run: &ClaudeRun) -> OutputId {
    OutputId::new(format!("claude:{}:response", run.execution.run_id))
}
fn event_id(run: &ClaudeRun, suffix: &str) -> AgentEventId {
    AgentEventId::new(format!("claude:{}:{suffix}", run.execution.run_id))
}
fn protocol(code: AgentProtocolErrorCode, message: impl Into<String>) -> AgentProtocolError {
    AgentProtocolError::new(code, message)
}
fn transport_error(error: TransportError) -> AgentProtocolError {
    protocol(
        AgentProtocolErrorCode::ProviderUnavailable,
        error.to_string(),
    )
}
fn reject(code: AgentRejectionCode, message: impl Into<String>) -> AgentStartError {
    AgentRejection::new(code, message).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::agent_connector::{AgentConnector, CreateAgentSessionRequest};
    use orchestral_core::agent_protocol::wire::{
        AgentRunEnvelope, ApprovalGrantRef, CommandId, ProviderBindingRef,
    };
    use orchestral_runtime::AgentController;
    use tokio::io::{
        AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, Lines, ReadHalf, WriteHalf,
    };
    use tokio::time::{timeout, Duration};

    type Reader = Lines<BufReader<ReadHalf<DuplexStream>>>;

    async fn setup() -> (
        tempfile::TempDir,
        Arc<ClaudeCodeConnector>,
        AgentSessionId,
        Reader,
        WriteHalf<DuplexStream>,
    ) {
        let root = tempfile::tempdir().unwrap();
        let connector = Arc::new(crate::tests::connector(root.path()));
        let session = connector
            .create_session(CreateAgentSessionRequest {
                cwd: Some(root.path().to_string_lossy().into_owned()),
                title: None,
                options: Value::Null,
                extensions: BTreeMap::new(),
            })
            .await
            .unwrap();
        let (client, server) = tokio::io::duplex(128 * 1024);
        let (reader, writer) = tokio::io::split(client);
        let (server_reader, server_writer) = tokio::io::split(server);
        connector
            .provider_state()
            .clients
            .push_back(ClaudeStream::from_io(
                reader,
                writer,
                Duration::from_secs(2),
                None,
            ));
        (
            root,
            connector,
            session.session_id,
            BufReader::new(server_reader).lines(),
            server_writer,
        )
    }

    async fn read(reader: &mut Reader) -> Value {
        serde_json::from_str(&reader.next_line().await.unwrap().unwrap()).unwrap()
    }

    async fn write(writer: &mut WriteHalf<DuplexStream>, value: Value) {
        writer
            .write_all(format!("{value}\n").as_bytes())
            .await
            .unwrap();
    }

    async fn initialize(reader: &mut Reader, writer: &mut WriteHalf<DuplexStream>) {
        let request = read(reader).await;
        assert_eq!(request["request"]["subtype"], "initialize");
        write(writer, json!({"type":"control_response","response":{"subtype":"success","request_id":request["request_id"],"response":{}}})).await;
    }

    fn envelope(session: &AgentSessionId, run: &str) -> AgentRunEnvelope {
        AgentRunEnvelope::new(
            ProtocolVersion::new(1, 0),
            session.clone(),
            RunId::new(run),
            vec![Content::text("inspect workspace")],
        )
        .unwrap()
    }

    async fn request(controller: &AgentController, run: &RunId) -> RequestId {
        timeout(Duration::from_secs(2), async {
            loop {
                if let Some(pending) = controller
                    .inspect(run)
                    .await
                    .unwrap()
                    .pending_requests
                    .first()
                {
                    break pending.request_id.clone();
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn controller_projects_stream_approval_delivery_and_duplicate_commands() {
        let (_root, connector, session, mut reader, mut writer) = setup().await;
        #[cfg(unix)]
        connector
            .enable_native_approvals(std::path::Path::new("/fixture/orchestral"))
            .await
            .unwrap();
        let session_for_server = session.clone();
        let server = tokio::spawn(async move {
            initialize(&mut reader, &mut writer).await;
            let input = read(&mut reader).await;
            assert_eq!(input["message"]["content"][0]["text"], "inspect workspace");
            assert_eq!(input["session_id"], session_for_server.as_str());
            write(&mut writer, json!({"type":"stream_event","session_id":session_for_server,"event":{"delta":{"type":"text_delta","text":"Inspecting"}}})).await;
            write(&mut writer, json!({"type":"control_request","request_id":"permission","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"cargo check"}}})).await;
            let permission = read(&mut reader).await;
            assert_eq!(permission["response"]["request_id"], "permission");
            assert_eq!(permission["response"]["response"]["behavior"], "allow");
            assert_eq!(
                permission["response"]["response"]["updatedInput"]["command"],
                "cargo check"
            );
            write(&mut writer, json!({"type":"result","subtype":"success","is_error":false,"session_id":session_for_server,"result":"Inspected","usage":{"input_tokens":20,"output_tokens":5}})).await;
        });
        let controller = Arc::new(
            AgentController::new(connector.clone(), ProviderBindingRef::new(CONNECTOR_ID)).unwrap(),
        );
        let run = envelope(&session, "first");
        let execution = controller.start(run.clone()).await.unwrap();
        let pending = request(&controller, &execution.run_id).await;
        #[cfg(unix)]
        {
            let registry = connector.config.config_dir.join("sessions");
            std::fs::create_dir_all(&registry).unwrap();
            std::fs::write(registry.join(format!("{}.json",std::process::id())),
                json!({"pid":std::process::id(),"sessionId":session.as_str(),"cwd":_root.path(),"status":"idle"}).to_string()).unwrap();
            let mut hook =
                tokio::net::UnixStream::connect(&connector.approval_bridge.get().unwrap().path)
                    .await
                    .unwrap();
            hook.write_all(format!("{}\n",json!({"request_id":uuid::Uuid::new_v4().to_string(),"input":{"session_id":session.as_str(),"hook_event_name":"PermissionRequest","tool_name":"Bash","tool_input":{"command":"cargo check"}}})).as_bytes()).await.unwrap();
            let mut answer = String::new();
            BufReader::new(hook).read_line(&mut answer).await.unwrap();
            assert_eq!(answer, "{}\n");
            assert!(connector.native_approvals.pending(&session).is_empty());
        }
        let command = AgentCommandEnvelope::new(
            CommandId::new("allow"),
            execution.run_id.clone(),
            Some(pending),
            AgentCommand::ResolveRequest {
                response: RequestResolution::Approval {
                    decision: ApprovalDecision::Allow,
                    grant_ref: Some(ApprovalGrantRef::new("host-grant")),
                },
            },
        )
        .unwrap();
        controller.command(command.clone()).await.unwrap();
        controller.command(command).await.unwrap();
        let view = timeout(
            Duration::from_secs(2),
            controller.wait_for_terminal(&execution.run_id),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            view.delivery.as_ref().unwrap().final_response,
            Content::text("Inspected")
        );
        assert_eq!(
            view.delivery
                .as_ref()
                .unwrap()
                .usage
                .as_ref()
                .unwrap()
                .input_tokens,
            Some(20)
        );
        assert_eq!(controller.start(run).await.unwrap(), execution);
        assert_eq!(connector.provider_state().runs.len(), 1);
        server.await.unwrap();
        #[cfg(unix)]
        {
            // A finished SDK Run must not suppress approvals when the user
            // later opens the same session in a native terminal.
            let mut hook =
                tokio::net::UnixStream::connect(&connector.approval_bridge.get().unwrap().path)
                    .await
                    .unwrap();
            hook.write_all(format!("{}\n",json!({"request_id":uuid::Uuid::new_v4().to_string(),"input":{"session_id":session.as_str(),"hook_event_name":"PermissionRequest","tool_name":"Bash","tool_input":{"command":"cargo check"}}})).as_bytes()).await.unwrap();
            timeout(Duration::from_secs(2), async {
                while connector.native_approvals.pending(&session).is_empty() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            drop(hook);
            timeout(Duration::from_secs(2), async {
                while !connector.native_approvals.pending(&session).is_empty() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn ask_user_question_returns_the_answer_in_updated_tool_input() {
        let (_root, connector, session, mut reader, mut writer) = setup().await;
        let server = tokio::spawn(async move {
            initialize(&mut reader, &mut writer).await;
            read(&mut reader).await;
            write(&mut writer, json!({"type":"control_request","request_id":"question","request":{"subtype":"can_use_tool","tool_name":"AskUserQuestion","input":{"questions":[{"question":"Which target?","options":[{"label":"local"}]}]}}})).await;
            let answer = read(&mut reader).await;
            assert_eq!(
                answer["response"]["response"]["updatedInput"]["answers"]["Which target?"],
                "local"
            );
            write(
                &mut writer,
                json!({"type":"result","subtype":"success","result":"Selected local"}),
            )
            .await;
        });
        let controller = Arc::new(
            AgentController::new(connector, ProviderBindingRef::new(CONNECTOR_ID)).unwrap(),
        );
        let execution = controller
            .start(envelope(&session, "question"))
            .await
            .unwrap();
        let pending = request(&controller, &execution.run_id).await;
        controller
            .command(
                AgentCommandEnvelope::new(
                    CommandId::new("answer"),
                    execution.run_id.clone(),
                    Some(pending),
                    AgentCommand::ResolveRequest {
                        response: RequestResolution::Input {
                            content: vec![Content::text("local")],
                        },
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let view = timeout(
            Duration::from_secs(2),
            controller.wait_for_terminal(&execution.run_id),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            view.delivery.unwrap().final_response,
            Content::text("Selected local")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn interrupt_is_confirmed_by_stream_shutdown_and_failures_are_terminal() {
        let (_root, connector, session, mut reader, mut writer) = setup().await;
        let server = tokio::spawn(async move {
            initialize(&mut reader, &mut writer).await;
            read(&mut reader).await;
            let request = read(&mut reader).await;
            assert_eq!(request["request"]["subtype"], "interrupt");
            write(&mut writer,json!({"type":"control_response","response":{"subtype":"success","request_id":request["request_id"],"response":{}}})).await;
        });
        let controller = Arc::new(
            AgentController::new(connector, ProviderBindingRef::new(CONNECTOR_ID)).unwrap(),
        );
        let execution = controller
            .start(envelope(&session, "cancel"))
            .await
            .unwrap();
        controller
            .command(
                AgentCommandEnvelope::new(
                    CommandId::new("stop"),
                    execution.run_id.clone(),
                    None,
                    AgentCommand::Cancel {
                        reason: "stop".to_owned(),
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let view = timeout(
            Duration::from_secs(2),
            controller.wait_for_terminal(&execution.run_id),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            view.state.status(),
            orchestral_core::agent_protocol::reference::AgentRunStatus::Cancelled
        );
        server.await.unwrap();

        let (_root, connector, session, mut reader, mut writer) = setup().await;
        let server = tokio::spawn(async move {
            initialize(&mut reader, &mut writer).await;
            read(&mut reader).await;
            write(&mut writer,json!({"type":"result","subtype":"error_max_turns","is_error":true,"errors":["Turn limit reached"]})).await;
        });
        let controller = Arc::new(
            AgentController::new(connector, ProviderBindingRef::new(CONNECTOR_ID)).unwrap(),
        );
        let execution = controller
            .start(envelope(&session, "failure"))
            .await
            .unwrap();
        let view = timeout(
            Duration::from_secs(2),
            controller.wait_for_terminal(&execution.run_id),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            view.state.status(),
            orchestral_core::agent_protocol::reference::AgentRunStatus::Failed
        );
        assert!(view.delivery.is_none());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn a_session_owned_by_a_live_terminal_is_rejected_before_dispatch() {
        let (root, connector, session, _reader, _writer) = setup().await;
        let sessions = connector.config.config_dir.join("sessions");
        std::fs::create_dir_all(&sessions).unwrap();
        std::fs::write(
            sessions.join(format!("{}.json", std::process::id())),
            json!({"pid":std::process::id(),"sessionId":session,"cwd":root.path(),"status":"idle"})
                .to_string(),
        )
        .unwrap();
        let controller = Arc::new(
            AgentController::new(connector.clone(), ProviderBindingRef::new(CONNECTOR_ID)).unwrap(),
        );
        assert!(controller
            .start(envelope(&session, "conflict"))
            .await
            .is_err());
        assert_eq!(connector.provider_state().clients.len(), 1);
        assert!(connector.provider_state().runs.is_empty());
    }

    #[test]
    fn approval_cannot_answer_a_question_and_deny_preserves_native_permission_rules() {
        let native = NativeRequest {
            id: "request".to_owned(),
            tool_name: "Write".to_owned(),
            input: json!({"file_path":"src/lib.rs","content":"value"}),
        };
        assert_eq!(
            resolution(
                &native,
                &RequestResolution::Approval {
                    decision: ApprovalDecision::Deny,
                    grant_ref: None
                }
            )
            .unwrap()["behavior"],
            "deny"
        );
        assert!(resolution(
            &native,
            &RequestResolution::Input {
                content: vec![Content::text("yes")]
            }
        )
        .is_err());
    }

    #[tokio::test]
    async fn unexpected_stdout_loss_preserves_unknown_continuity() {
        let (_root, connector, session, mut reader, mut writer) = setup().await;
        let server = tokio::spawn(async move {
            initialize(&mut reader, &mut writer).await;
            read(&mut reader).await;
            // No result or cancellation acknowledgement: execution outcome is unknown.
        });
        let controller = Arc::new(
            AgentController::new(connector, ProviderBindingRef::new(CONNECTOR_ID)).unwrap(),
        );
        let execution = controller.start(envelope(&session, "lost")).await.unwrap();
        assert!(timeout(
            Duration::from_secs(2),
            controller.wait_for_terminal(&execution.run_id)
        )
        .await
        .unwrap()
        .is_err());
        let view = controller.inspect(&execution.run_id).await.unwrap();
        assert_eq!(
            view.state.status(),
            orchestral_core::agent_protocol::reference::AgentRunStatus::Unknown
        );
        assert!(view.delivery.is_none());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn completed_sessions_accept_a_new_run_without_replaying_the_old_input() {
        let (_root, connector, session, mut reader, mut writer) = setup().await;
        let first = tokio::spawn(async move {
            initialize(&mut reader, &mut writer).await;
            read(&mut reader).await;
            write(
                &mut writer,
                json!({"type":"result","subtype":"success","result":"First response"}),
            )
            .await;
        });
        let controller = Arc::new(
            AgentController::new(connector.clone(), ProviderBindingRef::new(CONNECTOR_ID)).unwrap(),
        );
        let execution = controller.start(envelope(&session, "first")).await.unwrap();
        timeout(
            Duration::from_secs(2),
            controller.wait_for_terminal(&execution.run_id),
        )
        .await
        .unwrap()
        .unwrap();
        first.await.unwrap();
        let (client, server) = tokio::io::duplex(16384);
        let (reader, writer) = tokio::io::split(client);
        let (reader_server, mut writer_server) = tokio::io::split(server);
        connector
            .provider_state()
            .clients
            .push_back(ClaudeStream::from_io(
                reader,
                writer,
                Duration::from_secs(2),
                None,
            ));
        let second = tokio::spawn(async move {
            let mut reader_server = BufReader::new(reader_server).lines();
            initialize(&mut reader_server, &mut writer_server).await;
            assert_eq!(read(&mut reader_server).await["type"], "user");
            write(
                &mut writer_server,
                json!({"type":"result","subtype":"success","result":"Second response"}),
            )
            .await;
        });
        let execution = controller
            .start(envelope(&session, "second"))
            .await
            .unwrap();
        let view = timeout(
            Duration::from_secs(2),
            controller.wait_for_terminal(&execution.run_id),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            view.delivery.unwrap().final_response,
            Content::text("Second response")
        );
        assert_eq!(connector.provider_state().runs.len(), 2);
        second.await.unwrap();
    }
}
