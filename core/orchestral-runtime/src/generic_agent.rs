//! Provider-neutral Generic Agent implementation.
//!
//! Tools are optional and can only enter through the Host-owned guarded Tool
//! runtime. A model tool call never carries authority by itself.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use futures_util::{stream, StreamExt};
use orchestral_core::agent_protocol::{
    spi::{
        AgentProvider, AgentProviderStream, AgentRecovery, AgentRecoveryRequest, AgentStart,
        AgentStartError,
    },
    wire::{
        AgentAdmission, AgentCapabilities, AgentCommand, AgentCommandEnvelope, AgentDelivery,
        AgentDescriptor, AgentDescriptorEnvelope, AgentEvent, AgentEventDraft, AgentEventId,
        AgentExecutionRef, AgentFailure, AgentId, AgentProtocolError, AgentProtocolErrorCode,
        AgentProviderId, AgentProviderStreamItem, AgentRejection, AgentRejectionCode,
        AgentStartRequest, AgentTelemetry, AgentTelemetryEnvelope, ApprovalDecision,
        BindingRequirement, CancelSupport, CommandId, Content, ContentBody, ControlCapabilities,
        DeliveryId, Digest, EffectMediation, IncompleteReason, OutputId, PartialDelivery,
        PartialDeliveryId, PendingRequest, PendingRequestKind, PendingRequestPayload, Provenance,
        ProviderCommandDisposition, ProviderCommandOutcome, RequestId, RequestResolution,
        ResourceBindingId, ResourceBindingMode, ResourceBindingSkip, ResourceBindingSkipCode,
        ResourceCapability, ResourceKind, RunId, RunLimitKind, TelemetryId, UsageReport,
    },
    AGENT_PROTOCOL_V1,
};
use orchestral_core::agent_session::{
    AgentSessionError, AgentSessionEvent, AgentSessionEventDraft, AgentSessionEventId,
    AgentSessionJournalStore, InMemoryAgentSessionJournalStore,
};
use orchestral_core::executor::{ExecutionProgressEvent, ExecutionProgressReporter};
use orchestral_core::model_protocol::{
    ModelBackend, ModelContent, ModelError, ModelErrorCode, ModelEvent, ModelFinishReason,
    ModelMessage, ModelRequest, ModelRequestId, ModelRole, ModelToolCallId, ModelToolDefinition,
    ModelUsage,
};
use orchestral_core::tool_protocol::{
    ApprovalBinding, ApprovalCapability, RunToolGrant, ToolCallId, ToolInvocation, ToolOutcome,
    ToolOutput,
};
use orchestral_core::types::{Plan, WorkflowId};
use serde::Deserialize;
use tokio::sync::{broadcast, oneshot, watch};
use tokio_util::sync::CancellationToken;

use crate::approval_bridge::AgentApprovalBridge;
use crate::generic_agent_checkpoint::{
    CreateGenericRunOutcome, GenericAgentCheckpointStore, GenericAgentRunRegistration,
    GenericCheckpointDraft, GenericCheckpointError, GenericCheckpointEvent,
    GenericCheckpointEventId, GenericCheckpointPhase, GenericLoopBoundary, GenericModelObservation,
    GenericObservedToolCall, InMemoryGenericAgentCheckpointStore, StoredGenericAgentRun,
};
use crate::skill::{
    ActivatedSkillSet, SkillActivationOutcome, SkillActivationRequest, SkillRuntime,
};
use crate::tool_runtime::{AgentToolRuntime, GuardedToolResult, ToolRuntimeError};
use crate::workflow_strategy::{WorkflowExecutionRequest, WorkflowExecutionStrategy};
use crate::{
    AgentSessionContextEngine, JsonSizeTokenMeter, ModelTokenMeter, SessionContextError,
    SessionContextRequest,
};

const WORKFLOW_TOOL_NAME: &str = "orchestral_workflow";
const SKILL_ACTIVATE_TOOL_NAME: &str = "orchestral_skill_activate";
const REQUEST_INPUT_TOOL_NAME: &str = "orchestral_request_input";

#[derive(Debug, Clone)]
pub struct GenericAgentConfig {
    pub provider_id: AgentProviderId,
    pub agent_id: AgentId,
    pub system_prompt: String,
    pub stream_buffer: usize,
    pub max_model_rounds: u64,
    pub max_tool_calls: u64,
    pub max_context_tokens: u64,
    pub reserved_output_tokens: u64,
}

impl GenericAgentConfig {
    pub fn new(provider_id: impl Into<String>, agent_id: impl Into<String>) -> Self {
        Self {
            provider_id: AgentProviderId::new(provider_id),
            agent_id: AgentId::new(agent_id),
            system_prompt: "You are a helpful, precise assistant.".to_owned(),
            stream_buffer: 128,
            max_model_rounds: 8,
            max_tool_calls: 32,
            max_context_tokens: 128 * 1024,
            reserved_output_tokens: 4 * 1024,
        }
    }
}

#[derive(Clone)]
pub struct InternalGenericAgentProvider {
    inner: Arc<GenericInner>,
}

struct GenericInner {
    backend: Arc<dyn ModelBackend>,
    descriptor: AgentDescriptorEnvelope,
    config: GenericAgentConfig,
    tools: Option<GenericTools>,
    skills: Option<Arc<SkillRuntime>>,
    session_journal: Arc<dyn AgentSessionJournalStore>,
    context_engine: AgentSessionContextEngine,
    checkpoint_store: Arc<dyn GenericAgentCheckpointStore>,
    config_digest: Digest,
    state: Mutex<GenericState>,
}

struct GenericTools {
    runtime: Arc<dyn AgentToolRuntime>,
    runtime_contract_digest: Digest,
    run_grant: RunToolGrant,
    model_definitions: Vec<ModelToolDefinition>,
    workflow: Option<Arc<WorkflowExecutionStrategy>>,
    approval_bridge: Option<Arc<dyn AgentApprovalBridge>>,
}

#[derive(Default)]
struct GenericState {
    runs: BTreeMap<RunId, GenericRun>,
    sessions: BTreeMap<orchestral_core::agent_protocol::wire::AgentSessionId, GenericSession>,
}

#[derive(Default)]
struct GenericSession {
    active_run: Option<RunId>,
}

struct GenericRun {
    request: AgentStartRequest,
    execution: AgentExecutionRef,
    admission: AgentAdmission,
    durable_events: Vec<AgentEventDraft>,
    sender: broadcast::Sender<Result<AgentProviderStreamItem, AgentProtocolError>>,
    terminal: bool,
    cancellation: CancellationToken,
    cancel_command: Option<(CommandId, String)>,
    commands: BTreeMap<CommandId, StoredCommand>,
    queued_steers: VecDeque<QueuedSteer>,
    steer_signal: watch::Sender<u64>,
    pending_inputs: BTreeMap<RequestId, PendingInput>,
    pending_approvals: BTreeMap<RequestId, PendingApproval>,
    checkpoint_seq: u64,
}

struct QueuedSteer {
    command_id: CommandId,
    content: Vec<Content>,
    message: ModelMessage,
}

struct PendingInput {
    responder: Option<oneshot::Sender<InputResponse>>,
}

#[derive(Clone)]
struct InputResponse {
    command_id: CommandId,
    resolution: RequestResolution,
}

struct PendingApproval {
    binding: ApprovalBinding,
    responder: Option<oneshot::Sender<ApprovalResponse>>,
}

struct ApprovalResponse {
    command_id: CommandId,
    resolution: RequestResolution,
    capability: Option<ApprovalCapability>,
}

struct StoredCommand {
    digest: Digest,
    outcome: ProviderCommandOutcome,
}

struct GenericExecutionSeed {
    published_resource_skips: BTreeSet<ResourceBindingId>,
    run_started: bool,
    next_model_round: u64,
    total_usage: ModelUsage,
    tool_call_count: u64,
    last_response: String,
    supporting_event_ids: Vec<AgentEventId>,
}

enum GenericRecoveryContinuation {
    ModelLoop {
        restore_initial_input: bool,
    },
    Input {
        round: u64,
        request_id: ModelRequestId,
        request_digest: Digest,
        observation: GenericModelObservation,
        call: GenericObservedToolCall,
        arguments: serde_json::Value,
        prompt: String,
        request_open: bool,
        committed_response: Option<InputResponse>,
        resolved_response: Option<InputResponse>,
        response: Option<oneshot::Receiver<InputResponse>>,
    },
}

impl GenericExecutionSeed {
    fn fresh() -> Self {
        Self {
            published_resource_skips: BTreeSet::new(),
            run_started: false,
            next_model_round: 1,
            total_usage: ModelUsage::default(),
            tool_call_count: 0,
            last_response: String::new(),
            supporting_event_ids: Vec::new(),
        }
    }
}

impl InternalGenericAgentProvider {
    /// Replaces the process-lifetime checkpoint WAL before this Provider is
    /// cloned or bound to a controller.
    pub fn with_checkpoint_store(
        mut self,
        checkpoint_store: Arc<dyn GenericAgentCheckpointStore>,
    ) -> Result<Self, AgentProtocolError> {
        let inner = Arc::get_mut(&mut self.inner).ok_or_else(|| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "Generic Agent checkpoint store must be bound before the Provider is shared",
            )
        })?;
        inner.checkpoint_store = checkpoint_store;
        Ok(self)
    }

    pub fn new(
        backend: Arc<dyn ModelBackend>,
        config: GenericAgentConfig,
    ) -> Result<Self, AgentProtocolError> {
        Self::build(
            backend,
            config,
            None,
            None,
            Arc::new(InMemoryAgentSessionJournalStore::default()),
            Arc::new(JsonSizeTokenMeter::default()),
        )
    }

    pub fn new_with_session_journal(
        backend: Arc<dyn ModelBackend>,
        config: GenericAgentConfig,
        session_journal: Arc<dyn AgentSessionJournalStore>,
        token_meter: Arc<dyn ModelTokenMeter>,
    ) -> Result<Self, AgentProtocolError> {
        Self::build(backend, config, None, None, session_journal, token_meter)
    }

    pub fn new_with_tools(
        backend: Arc<dyn ModelBackend>,
        config: GenericAgentConfig,
        runtime: Arc<dyn AgentToolRuntime>,
        run_grant: RunToolGrant,
    ) -> Result<Self, AgentProtocolError> {
        Self::build(
            backend,
            config,
            Some(configure_tools(runtime, run_grant, None, None)?),
            None,
            Arc::new(InMemoryAgentSessionJournalStore::default()),
            Arc::new(JsonSizeTokenMeter::default()),
        )
    }

    pub fn new_with_tools_and_session_journal(
        backend: Arc<dyn ModelBackend>,
        config: GenericAgentConfig,
        runtime: Arc<dyn AgentToolRuntime>,
        run_grant: RunToolGrant,
        session_journal: Arc<dyn AgentSessionJournalStore>,
        token_meter: Arc<dyn ModelTokenMeter>,
    ) -> Result<Self, AgentProtocolError> {
        Self::build(
            backend,
            config,
            Some(configure_tools(runtime, run_grant, None, None)?),
            None,
            session_journal,
            token_meter,
        )
    }

    /// Enables Host-mediated approval while keeping capability issuance out of
    /// both the model and the Generic Agent implementation.
    pub fn new_with_tools_approval_and_session_journal(
        backend: Arc<dyn ModelBackend>,
        config: GenericAgentConfig,
        runtime: Arc<dyn AgentToolRuntime>,
        run_grant: RunToolGrant,
        approval_bridge: Arc<dyn AgentApprovalBridge>,
        session_journal: Arc<dyn AgentSessionJournalStore>,
        token_meter: Arc<dyn ModelTokenMeter>,
    ) -> Result<Self, AgentProtocolError> {
        Self::build(
            backend,
            config,
            Some(configure_tools(
                runtime,
                run_grant,
                None,
                Some(approval_bridge),
            )?),
            None,
            session_journal,
            token_meter,
        )
    }

    /// Enables explicit complex-workflow selection while retaining one Generic
    /// Agent loop and the same guarded Tool Runtime for direct and DAG calls.
    pub fn new_with_workflow_and_session_journal(
        backend: Arc<dyn ModelBackend>,
        config: GenericAgentConfig,
        runtime: Arc<dyn AgentToolRuntime>,
        run_grant: RunToolGrant,
        workflow: Arc<WorkflowExecutionStrategy>,
        session_journal: Arc<dyn AgentSessionJournalStore>,
        token_meter: Arc<dyn ModelTokenMeter>,
    ) -> Result<Self, AgentProtocolError> {
        if !workflow.uses_tool_runtime(&runtime) {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "Generic Agent and Workflow must share one guarded Tool Runtime",
            ));
        }
        Self::build(
            backend,
            config,
            Some(configure_tools(runtime, run_grant, Some(workflow), None)?),
            None,
            session_journal,
            token_meter,
        )
    }

    /// Enables the independent Skill Context Plane. The catalog must still be
    /// bound into each Run before descriptors or activation are visible.
    pub fn new_with_skills_and_session_journal(
        backend: Arc<dyn ModelBackend>,
        config: GenericAgentConfig,
        skills: Arc<SkillRuntime>,
        session_journal: Arc<dyn AgentSessionJournalStore>,
        token_meter: Arc<dyn ModelTokenMeter>,
    ) -> Result<Self, AgentProtocolError> {
        Self::build(
            backend,
            config,
            None,
            Some(skills),
            session_journal,
            token_meter,
        )
    }

    /// Composition-root constructor for the ordinary CLI/API Agent: Skill
    /// context and guarded Tools remain separate runtimes sharing only the
    /// Generic Agent loop.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_tools_approval_skills_and_session_journal(
        backend: Arc<dyn ModelBackend>,
        config: GenericAgentConfig,
        runtime: Arc<dyn AgentToolRuntime>,
        run_grant: RunToolGrant,
        approval_bridge: Arc<dyn AgentApprovalBridge>,
        skills: Arc<SkillRuntime>,
        session_journal: Arc<dyn AgentSessionJournalStore>,
        token_meter: Arc<dyn ModelTokenMeter>,
    ) -> Result<Self, AgentProtocolError> {
        Self::build(
            backend,
            config,
            Some(configure_tools(
                runtime,
                run_grant,
                None,
                Some(approval_bridge),
            )?),
            Some(skills),
            session_journal,
            token_meter,
        )
    }

    fn build(
        backend: Arc<dyn ModelBackend>,
        config: GenericAgentConfig,
        tools: Option<GenericTools>,
        skills: Option<Arc<SkillRuntime>>,
        session_journal: Arc<dyn AgentSessionJournalStore>,
        token_meter: Arc<dyn ModelTokenMeter>,
    ) -> Result<Self, AgentProtocolError> {
        let model_descriptor = backend.descriptor();
        model_descriptor.validate().map_err(model_protocol_error)?;
        if (tools.is_some() || skills.is_some()) && !model_descriptor.capabilities.tool_calls {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::Unsupported,
                "configured ModelBackend does not support model function calls",
            ));
        }
        if let Some(conflict) = tools.as_ref().and_then(|tools| {
            tools.model_definitions.iter().find_map(|definition| {
                [SKILL_ACTIVATE_TOOL_NAME, REQUEST_INPUT_TOOL_NAME]
                    .contains(&definition.name.as_str())
                    .then(|| definition.name.clone())
            })
        }) {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                format!("reserved Generic Agent function name is already registered: {conflict}"),
            ));
        }
        if config.stream_buffer == 0
            || config.max_model_rounds == 0
            || config.max_tool_calls == 0
            || config.max_context_tokens == 0
            || config.reserved_output_tokens >= config.max_context_tokens
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "Generic Agent buffers and loop limits must be non-zero",
            ));
        }
        let has_tools = tools.is_some();
        let has_input_requests = model_descriptor.capabilities.tool_calls;
        let has_approval = tools
            .as_ref()
            .and_then(|tools| tools.approval_bridge.as_ref())
            .is_some();
        let mut supported_limits =
            BTreeSet::from([RunLimitKind::ModelSteps, RunLimitKind::InputTokens]);
        if has_tools {
            supported_limits.insert(RunLimitKind::ToolCalls);
        }
        let descriptor = AgentDescriptorEnvelope::seal(AgentDescriptor {
            provider_id: config.provider_id.clone(),
            agent_id: config.agent_id.clone(),
            supported_protocol_versions: vec![AGENT_PROTOCOL_V1],
            accepted_content_types: BTreeSet::from(["text/plain".to_owned()]),
            capabilities: AgentCapabilities {
                session_reuse: true,
                structured_output: false,
                controls: ControlCapabilities {
                    steer: true,
                    cancel: CancelSupport::Confirmed,
                    recover: true,
                },
                pending_request_kinds: {
                    let mut kinds = BTreeSet::new();
                    if has_input_requests {
                        kinds.insert(PendingRequestKind::Input);
                    }
                    if has_approval {
                        kinds.insert(PendingRequestKind::Approval);
                    }
                    kinds
                },
                supported_limits,
                resources: skills
                    .as_ref()
                    .map(|_| {
                        vec![ResourceCapability {
                            kind: ResourceKind::new(
                                orchestral_core::skill_protocol::SKILL_CATALOG_RESOURCE_KIND_V1,
                            ),
                            modes: BTreeSet::from([ResourceBindingMode::Snapshot]),
                            max_bindings: Some(1),
                        }]
                    })
                    .unwrap_or_default(),
                effect_mediation: if has_tools {
                    EffectMediation::HostMediated
                } else {
                    EffectMediation::None
                },
            },
            extensions: Default::default(),
        })?;
        let config_digest = generic_config_digest(
            &config,
            &model_descriptor,
            tools.as_ref(),
            skills.as_ref().map(|skills| skills.catalog()),
            has_approval,
            has_input_requests,
        )?;
        let context_engine = AgentSessionContextEngine::new(session_journal.clone(), token_meter);
        Ok(Self {
            inner: Arc::new(GenericInner {
                backend,
                descriptor,
                config,
                tools,
                skills,
                session_journal,
                context_engine,
                checkpoint_store: Arc::new(InMemoryGenericAgentCheckpointStore::default()),
                config_digest,
                state: Mutex::new(GenericState::default()),
            }),
        })
    }

    fn state(&self) -> MutexGuard<'_, GenericState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn stream_for(run: &GenericRun) -> AgentProviderStream {
        let receiver = run.sender.subscribe();
        let replay = run
            .durable_events
            .clone()
            .into_iter()
            .map(|draft| Ok(AgentProviderStreamItem::Event(Box::new(draft))));
        let replay_stream = stream::iter(replay);
        if run.terminal {
            return replay_stream.boxed();
        }
        let live = stream::unfold(receiver, |mut receiver| async move {
            loop {
                match receiver.recv().await {
                    Ok(item) => return Some((item, receiver)),
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        return Some((
                            Err(AgentProtocolError::new(
                                AgentProtocolErrorCode::SequenceGap,
                                format!("Generic Agent stream subscriber lagged by {skipped}"),
                            )),
                            receiver,
                        ));
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        });
        replay_stream.chain(live).boxed()
    }

    fn rejection(code: AgentRejectionCode, message: impl Into<String>) -> AgentStartError {
        AgentStartError::Rejected(AgentRejection::new(code, message))
    }
}

#[async_trait]
impl AgentProvider for InternalGenericAgentProvider {
    fn describe(&self) -> AgentDescriptorEnvelope {
        self.inner.descriptor.clone()
    }

    async fn start(&self, request: AgentStartRequest) -> Result<AgentStart, AgentStartError> {
        request
            .validate_for_descriptor(&self.inner.descriptor)
            .map_err(|error| {
                Self::rejection(AgentRejectionCode::RunIdConflict, error.to_string())
            })?;
        let mut compatibility = self
            .inner
            .descriptor
            .descriptor
            .check_run_compatibility(&request.run)
            .map_err(AgentStartError::Rejected)?;
        let run_skills = resolve_run_skill_binding(
            self.inner.skills.as_ref(),
            &request,
            &mut compatibility.skipped_optional_bindings,
        )?;
        let admission = AgentAdmission {
            skipped_optional_bindings: compatibility.skipped_optional_bindings.clone(),
        };
        admission
            .validate_against(&request.run, &compatibility)
            .map_err(|error| Self::rejection(AgentRejectionCode::InvalidSpec, error.to_string()))?;
        let user_message = agent_input_message(&request)
            .map_err(|error| Self::rejection(AgentRejectionCode::InvalidSpec, error.to_string()))?;
        let execution =
            AgentExecutionRef::for_start(&request, &self.inner.descriptor).map_err(|error| {
                Self::rejection(AgentRejectionCode::RunIdConflict, error.to_string())
            })?;

        let (stream, cancellation, steer_updates) = {
            let mut state = self.state();
            if let Some(existing) = state.runs.get(&request.run.spec.run_id) {
                if existing.execution != execution || existing.request != request {
                    return Err(Self::rejection(
                        AgentRejectionCode::RunIdConflict,
                        "run_id already belongs to another immutable start",
                    ));
                }
                return Ok(AgentStart {
                    execution: existing.execution.clone(),
                    admission: existing.admission.clone(),
                    stream: Self::stream_for(existing),
                });
            }

            let session = state
                .sessions
                .entry(request.run.spec.session_id.clone())
                .or_default();
            if session.active_run.is_some() {
                return Err(Self::rejection(
                    AgentRejectionCode::SessionConflict,
                    "Generic Agent permits one active Run per session",
                ));
            }
            match self
                .inner
                .checkpoint_store
                .create_run(GenericAgentRunRegistration {
                    request: request.clone(),
                    execution: execution.clone(),
                    admission: admission.clone(),
                    config_digest: self.inner.config_digest.clone(),
                }) {
                Ok(CreateGenericRunOutcome::Created) => {}
                Ok(CreateGenericRunOutcome::ExactExisting) => {
                    return Err(AgentStartError::OutcomeUnknown(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidTransition,
                        "Generic Agent private WAL already owns this Run; use recovery",
                    )))
                }
                Err(error) => return Err(checkpoint_start_error(error)),
            }
            session.active_run = Some(request.run.spec.run_id.clone());

            let (sender, _) = broadcast::channel(self.inner.config.stream_buffer);
            let cancellation = CancellationToken::new();
            let (steer_signal, steer_updates) = watch::channel(0_u64);
            let run = GenericRun {
                request: request.clone(),
                execution: execution.clone(),
                admission: admission.clone(),
                durable_events: Vec::new(),
                sender,
                terminal: false,
                cancellation: cancellation.clone(),
                cancel_command: None,
                commands: BTreeMap::new(),
                queued_steers: VecDeque::new(),
                steer_signal: steer_signal.clone(),
                pending_inputs: BTreeMap::new(),
                pending_approvals: BTreeMap::new(),
                checkpoint_seq: 0,
            };
            let stream = Self::stream_for(&run);
            state.runs.insert(request.run.spec.run_id.clone(), run);
            (stream, cancellation, steer_updates)
        };

        let model_definitions = model_definitions_for_run(&self.inner, run_skills.is_some());
        let context_result = project_model_messages(
            &self.inner,
            &request,
            &model_definitions,
            run_skills.as_deref(),
            Some(user_message.clone()),
        )
        .await;

        let model_messages = match context_result {
            Ok(messages) => messages,
            Err(error) => {
                let inner = self.inner.clone();
                let failed_request = request.clone();
                let failed_user_message = user_message.clone();
                tokio::spawn(async move {
                    fail_before_model(
                        inner,
                        &failed_request,
                        &failed_user_message,
                        session_failure(error),
                    );
                });
                return Ok(AgentStart {
                    execution,
                    admission,
                    stream,
                });
            }
        };

        if let Err(failure) = commit_loop_boundary(
            &self.inner,
            &request.run.spec.run_id,
            1,
            &ModelUsage::default(),
            0,
            "",
            &[],
        ) {
            let inner = self.inner.clone();
            let failed_request = request.clone();
            let failed_user_message = user_message.clone();
            tokio::spawn(async move {
                fail_before_model(inner, &failed_request, &failed_user_message, failure);
            });
            return Ok(AgentStart {
                execution,
                admission,
                stream,
            });
        }

        let inner = self.inner.clone();
        let run_admission = admission.clone();
        tokio::spawn(async move {
            execute_model_run(
                inner,
                request,
                run_admission,
                user_message,
                model_messages,
                model_definitions,
                run_skills,
                GenericExecutionSeed::fresh(),
                cancellation,
                steer_updates,
            )
            .await;
        });
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
        let approval_bridge = self
            .inner
            .tools
            .as_ref()
            .and_then(|tools| tools.approval_bridge.clone());

        let (request_id, resolution, binding) = {
            let mut state = self.state();
            let run = state.runs.get_mut(&command.run_id).ok_or_else(|| {
                AgentProtocolError::new(AgentProtocolErrorCode::RunNotFound, "run does not exist")
            })?;
            validate_execution_and_duplicate(run, execution, &command)?;
            if let Some(existing) = run.commands.get(&command.command_id) {
                return Ok(ProviderCommandDisposition {
                    command_id: command.command_id,
                    run_id: command.run_id,
                    outcome: existing.outcome.clone(),
                    duplicate: true,
                });
            }

            match &command.payload {
                AgentCommand::Cancel { .. } if run.terminal => {
                    return record_command(
                        &self.inner,
                        run,
                        &command,
                        ProviderCommandOutcome::Rejected {
                            code: AgentProtocolErrorCode::TerminalRun,
                            message: "Run is already terminal".to_owned(),
                        },
                    );
                }
                AgentCommand::Cancel { .. } if run.cancel_command.is_some() => {
                    return record_command(
                        &self.inner,
                        run,
                        &command,
                        ProviderCommandOutcome::Rejected {
                            code: AgentProtocolErrorCode::InvalidTransition,
                            message: "cancellation is already in progress".to_owned(),
                        },
                    );
                }
                AgentCommand::Cancel { reason } => {
                    let disposition = record_command(
                        &self.inner,
                        run,
                        &command,
                        ProviderCommandOutcome::Accepted,
                    )?;
                    run.cancel_command = Some((command.command_id.clone(), reason.clone()));
                    run.cancellation.cancel();
                    return Ok(disposition);
                }
                AgentCommand::Steer { .. } if run.terminal => {
                    return record_command(
                        &self.inner,
                        run,
                        &command,
                        ProviderCommandOutcome::Rejected {
                            code: AgentProtocolErrorCode::TerminalRun,
                            message: "Run is already terminal".to_owned(),
                        },
                    );
                }
                AgentCommand::Steer { .. } if run.cancel_command.is_some() => {
                    return record_command(
                        &self.inner,
                        run,
                        &command,
                        ProviderCommandOutcome::Rejected {
                            code: AgentProtocolErrorCode::InvalidTransition,
                            message: "Run cancellation is already in progress".to_owned(),
                        },
                    );
                }
                AgentCommand::Steer { content } => {
                    let message = match agent_content_message(content) {
                        Ok(message) => message,
                        Err(error) => {
                            return record_command(
                                &self.inner,
                                run,
                                &command,
                                ProviderCommandOutcome::Rejected {
                                    code: error.code,
                                    message: error.message,
                                },
                            );
                        }
                    };
                    if run.queued_steers.len() >= self.inner.config.stream_buffer {
                        return record_command(
                            &self.inner,
                            run,
                            &command,
                            ProviderCommandOutcome::Rejected {
                                code: AgentProtocolErrorCode::InvalidTransition,
                                message: "Steer input buffer is full".to_owned(),
                            },
                        );
                    }
                    let disposition = record_command(
                        &self.inner,
                        run,
                        &command,
                        ProviderCommandOutcome::Accepted,
                    )?;
                    run.queued_steers.push_back(QueuedSteer {
                        command_id: command.command_id.clone(),
                        content: content.clone(),
                        message,
                    });
                    let signal = run.steer_signal.clone();
                    drop(state);
                    signal.send_modify(|generation| {
                        *generation = generation.saturating_add(1);
                    });
                    return Ok(disposition);
                }
                AgentCommand::ResolveRequest { response } => {
                    let Some(request_id) = command.request_id.as_ref() else {
                        unreachable!("validated ResolveRequest always carries request_id")
                    };
                    if run.terminal {
                        return record_command(
                            &self.inner,
                            run,
                            &command,
                            ProviderCommandOutcome::Rejected {
                                code: AgentProtocolErrorCode::TerminalRun,
                                message: "Run is already terminal".to_owned(),
                            },
                        );
                    }
                    if let RequestResolution::Input { content } = response {
                        if let Err(error) = agent_content_message(content) {
                            return record_command(
                                &self.inner,
                                run,
                                &command,
                                ProviderCommandOutcome::Rejected {
                                    code: error.code,
                                    message: error.message,
                                },
                            );
                        }
                        let Some(pending) = run.pending_inputs.get(request_id) else {
                            return record_command(
                                &self.inner,
                                run,
                                &command,
                                ProviderCommandOutcome::Rejected {
                                    code: AgentProtocolErrorCode::RequestNotFound,
                                    message: "input request is not pending".to_owned(),
                                },
                            );
                        };
                        if !pending
                            .responder
                            .as_ref()
                            .is_some_and(|responder| !responder.is_closed())
                        {
                            let disposition = record_command(
                                &self.inner,
                                run,
                                &command,
                                ProviderCommandOutcome::Rejected {
                                    code: AgentProtocolErrorCode::InvalidTransition,
                                    message: "input waiter is no longer active".to_owned(),
                                },
                            )?;
                            run.pending_inputs.remove(request_id);
                            return Ok(disposition);
                        }
                        let disposition = record_command(
                            &self.inner,
                            run,
                            &command,
                            ProviderCommandOutcome::Accepted,
                        )?;
                        let mut pending = run
                            .pending_inputs
                            .remove(request_id)
                            .expect("pending input was checked before its command commit");
                        if let Some(responder) = pending.responder.take() {
                            let _ = responder.send(InputResponse {
                                command_id: command.command_id.clone(),
                                resolution: response.clone(),
                            });
                        }
                        return Ok(disposition);
                    }
                    if !matches!(response, RequestResolution::Approval { .. }) {
                        return record_command(
                            &self.inner,
                            run,
                            &command,
                            ProviderCommandOutcome::Rejected {
                                code: AgentProtocolErrorCode::RequestTypeMismatch,
                                message:
                                    "request resolution kind is not pending in this Generic Agent"
                                        .to_owned(),
                            },
                        );
                    }
                    let Some(pending) = run.pending_approvals.get(request_id) else {
                        return record_command(
                            &self.inner,
                            run,
                            &command,
                            ProviderCommandOutcome::Rejected {
                                code: AgentProtocolErrorCode::RequestNotFound,
                                message: "approval request is not pending".to_owned(),
                            },
                        );
                    };
                    if approval_bridge.is_none() {
                        return record_command(
                            &self.inner,
                            run,
                            &command,
                            ProviderCommandOutcome::Unsupported {
                                feature: "approval".to_owned(),
                            },
                        );
                    }
                    (
                        request_id.clone(),
                        response.clone(),
                        pending.binding.clone(),
                    )
                }
                _ => {
                    return record_command(
                        &self.inner,
                        run,
                        &command,
                        ProviderCommandOutcome::Unsupported {
                            feature: "unknown_command".to_owned(),
                        },
                    );
                }
            }
        };

        let bridge = approval_bridge.expect("approval bridge presence was checked");
        let capability = match &resolution {
            RequestResolution::Approval {
                decision: ApprovalDecision::Allow,
                grant_ref: Some(grant_ref),
            } => match bridge.resolve(&request_id, grant_ref, &binding).await {
                Ok(capability) => Some(capability),
                Err(error) => {
                    let mut state = self.state();
                    let run = state.runs.get_mut(&command.run_id).ok_or_else(|| {
                        AgentProtocolError::new(
                            AgentProtocolErrorCode::RunNotFound,
                            "run disappeared while resolving approval",
                        )
                    })?;
                    return record_command(
                        &self.inner,
                        run,
                        &command,
                        ProviderCommandOutcome::Rejected {
                            code: AgentProtocolErrorCode::InvalidSpec,
                            message: error.to_string(),
                        },
                    );
                }
            },
            RequestResolution::Approval {
                decision: ApprovalDecision::Deny,
                grant_ref: None,
            } => None,
            _ => unreachable!("command shape and approval kind were validated"),
        };

        let mut state = self.state();
        let run = state.runs.get_mut(&command.run_id).ok_or_else(|| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::RunNotFound,
                "run disappeared while resolving approval",
            )
        })?;
        validate_execution_and_duplicate(run, execution, &command)?;
        if let Some(existing) = run.commands.get(&command.command_id) {
            return Ok(ProviderCommandDisposition {
                command_id: command.command_id,
                run_id: command.run_id,
                outcome: existing.outcome.clone(),
                duplicate: true,
            });
        }
        let Some(pending) = run.pending_approvals.get(&request_id) else {
            return record_command(
                &self.inner,
                run,
                &command,
                ProviderCommandOutcome::Rejected {
                    code: AgentProtocolErrorCode::RequestNotFound,
                    message: "approval request is no longer pending".to_owned(),
                },
            );
        };
        if pending.binding != binding {
            return record_command(
                &self.inner,
                run,
                &command,
                ProviderCommandOutcome::Rejected {
                    code: AgentProtocolErrorCode::InvalidDigest,
                    message: "approval binding changed while resolving request".to_owned(),
                },
            );
        }
        if !pending
            .responder
            .as_ref()
            .is_some_and(|responder| !responder.is_closed())
        {
            let disposition = record_command(
                &self.inner,
                run,
                &command,
                ProviderCommandOutcome::Rejected {
                    code: AgentProtocolErrorCode::InvalidTransition,
                    message: "approval waiter is no longer active".to_owned(),
                },
            )?;
            run.pending_approvals.remove(&request_id);
            return Ok(disposition);
        }
        let disposition =
            record_command(&self.inner, run, &command, ProviderCommandOutcome::Accepted)?;
        let mut pending = run
            .pending_approvals
            .remove(&request_id)
            .expect("pending approval was checked before its command commit");
        if let Some(responder) = pending.responder.take() {
            let _ = responder.send(ApprovalResponse {
                command_id: command.command_id.clone(),
                resolution,
                capability,
            });
        }
        Ok(disposition)
    }

    async fn recover(
        &self,
        request: AgentRecoveryRequest,
    ) -> Result<AgentRecovery, AgentProtocolError> {
        request.validate_for(&self.inner.descriptor)?;
        {
            let state = self.state();
            if let Some(run) = state.runs.get(&request.execution.run_id) {
                if run.execution != request.execution || run.request != request.start_request {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::RunIdConflict,
                        "recovery identity does not match the Generic Agent Run",
                    ));
                }
                return Ok(AgentRecovery::reattached(Self::stream_for(run)));
            }
        }

        let stored = self
            .inner
            .checkpoint_store
            .load_run(&request.execution.run_id)
            .map_err(checkpoint_recovery_error)?
            .ok_or_else(|| {
                AgentProtocolError::new(
                    AgentProtocolErrorCode::RunNotFound,
                    "Generic Agent private WAL has no matching Run",
                )
            })?;
        let projection = stored.validate().map_err(checkpoint_recovery_error)?;
        if stored.registration.request != request.start_request
            || stored.registration.execution != request.execution
            || stored.registration.config_digest != self.inner.config_digest
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::RunIdConflict,
                "recovery identity or Generic Agent configuration does not match the private WAL",
            ));
        }
        let recovery_events = checkpoint_recovery_events(&stored)?;

        match projection.phase {
            GenericCheckpointPhase::Terminal => {
                let replay = stream::iter(
                    recovery_events
                        .into_iter()
                        .map(|draft| Ok(AgentProviderStreamItem::Event(Box::new(draft)))),
                )
                .boxed();
                Ok(AgentRecovery::staged(replay, async { Ok(()) }))
            }
            GenericCheckpointPhase::ModelAttemptOpen {
                round, request_id, ..
            } => Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "Generic Agent recovery is unsafe while a model attempt outcome is unknown",
            )
            .with_details(serde_json::json!({
                "boundary": "model_attempt_open",
                "round": round,
                "request_id": request_id,
            }))),
            GenericCheckpointPhase::ModelAttemptObserved {
                boundary,
                round,
                request_id,
                request_digest,
                observation,
            } => stage_input_recovery(
                self.inner.clone(),
                stored,
                boundary,
                round,
                request_id,
                request_digest,
                observation,
                recovery_events,
            ),
            GenericCheckpointPhase::Stable(boundary) => stage_loop_recovery(
                self.inner.clone(),
                stored,
                boundary,
                recovery_events,
                GenericRecoveryContinuation::ModelLoop {
                    restore_initial_input: false,
                },
            ),
            GenericCheckpointPhase::Prepared => stage_loop_recovery(
                self.inner.clone(),
                stored,
                GenericLoopBoundary {
                    next_model_round: 1,
                    usage: ModelUsage::default(),
                    tool_call_count: 0,
                    last_response: String::new(),
                    supporting_event_ids: Vec::new(),
                },
                recovery_events,
                GenericRecoveryContinuation::ModelLoop {
                    restore_initial_input: true,
                },
            ),
        }
    }
}

fn checkpoint_recovery_events(
    stored: &StoredGenericAgentRun,
) -> Result<Vec<AgentEventDraft>, AgentProtocolError> {
    let mut events = Vec::new();
    for record in &stored.records {
        match &record.payload {
            GenericCheckpointEvent::ProviderEventsCommitted { events: committed } => {
                events.extend(committed.iter().cloned());
            }
            GenericCheckpointEvent::CommandCommitted { command, outcome } => {
                events.push(
                    ProviderCommandDisposition {
                        command_id: command.command_id.clone(),
                        run_id: command.run_id.clone(),
                        outcome: outcome.clone(),
                        duplicate: false,
                    }
                    .to_event_draft()?,
                );
            }
            GenericCheckpointEvent::LoopBoundaryCommitted { .. }
            | GenericCheckpointEvent::ModelAttemptStarted { .. }
            | GenericCheckpointEvent::ModelAttemptObserved { .. } => {}
        }
    }
    Ok(events)
}

#[allow(clippy::too_many_arguments)]
fn stage_input_recovery(
    inner: Arc<GenericInner>,
    stored: StoredGenericAgentRun,
    boundary: GenericLoopBoundary,
    round: u64,
    request_id: ModelRequestId,
    request_digest: Digest,
    observation: GenericModelObservation,
    recovery_events: Vec<AgentEventDraft>,
) -> Result<AgentRecovery, AgentProtocolError> {
    if !matches!(
        observation.finish_reason,
        ModelFinishReason::ToolCalls | ModelFinishReason::Stop
    ) || observation.tool_calls.len() != 1
    {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::Unsupported,
            "observed model recovery currently requires one fresh input request",
        )
        .with_details(serde_json::json!({
            "boundary": "model_attempt_observed",
            "round": round,
            "request_id": request_id,
        })));
    }
    let call = observation
        .tool_calls
        .first()
        .cloned()
        .expect("one observed Tool call was checked");
    if call.name != REQUEST_INPUT_TOOL_NAME || !call.ended {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::Unsupported,
            "observed model recovery currently supports only a complete input request",
        )
        .with_details(serde_json::json!({
            "boundary": "model_attempt_observed",
            "round": round,
            "request_id": request_id,
        })));
    }
    let pending_call = PendingModelToolCall {
        call_id: call.call_id.clone(),
        name: call.name.clone(),
        arguments: call.arguments.clone(),
        ended: call.ended,
    };
    let arguments = parse_tool_arguments(&pending_call).map_err(observed_recovery_error)?;
    let prompt = parse_input_request(arguments.clone()).map_err(observed_recovery_error)?;
    let input_request_id = input_request_id(stored.registration.run_id(), round, &call.call_id);
    let expected_request = PendingRequest {
        request_id: input_request_id.clone(),
        blocking: true,
        payload: PendingRequestPayload::Input {
            prompt: vec![Content::text(prompt.clone())],
            input_schema: None,
        },
    };
    let mut request_open = false;
    let mut resolved_response = None;
    for event in &recovery_events {
        match &event.payload {
            AgentEvent::RequestOpened { request } if request.request_id == input_request_id => {
                if request_open || request != &expected_request {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "recovered input request does not match its observed model call",
                    ));
                }
                request_open = true;
            }
            AgentEvent::RequestResolved {
                request_id,
                resolution,
                ..
            } if request_id == &input_request_id => {
                if !request_open
                    || resolved_response.is_some()
                    || !matches!(resolution, RequestResolution::Input { .. })
                {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "recovered input resolution does not match its pending request",
                    ));
                }
                let command_id = event.causation_id.clone().ok_or_else(|| {
                    AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "recovered input resolution has no causating command",
                    )
                })?;
                resolved_response = Some(InputResponse {
                    command_id,
                    resolution: resolution.clone(),
                });
            }
            AgentEvent::RequestOpened { .. } | AgentEvent::RequestResolved { .. } => {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered interaction crossed the observed input request boundary",
                ));
            }
            _ => {}
        }
    }
    if let Some(response) = &resolved_response {
        validate_recovered_input_resolution(&stored.records, &input_request_id, response)?;
    }
    stage_loop_recovery(
        inner,
        stored,
        boundary,
        recovery_events,
        GenericRecoveryContinuation::Input {
            round,
            request_id,
            request_digest,
            observation,
            call,
            arguments,
            prompt,
            request_open,
            committed_response: None,
            resolved_response,
            response: None,
        },
    )
}

fn validate_recovered_input_resolution(
    records: &[crate::generic_agent_checkpoint::GenericCheckpointRecord],
    request_id: &RequestId,
    response: &InputResponse,
) -> Result<(), AgentProtocolError> {
    let mut matching_commands = 0_usize;
    for record in records {
        let GenericCheckpointEvent::CommandCommitted { command, outcome } = &record.payload else {
            continue;
        };
        if command.command_id != response.command_id {
            continue;
        }
        matching_commands = matching_commands.saturating_add(1);
        let matches_resolution = matches!(
            &command.payload,
            AgentCommand::ResolveRequest { response: command_response }
                if command_response == &response.resolution
        );
        if outcome != &ProviderCommandOutcome::Accepted
            || command.request_id.as_ref() != Some(request_id)
            || !matches_resolution
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered input resolution does not match its accepted command",
            ));
        }
    }
    if matching_commands != 1 {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered input resolution has no unique accepted command",
        ));
    }
    Ok(())
}

fn stage_loop_recovery(
    inner: Arc<GenericInner>,
    stored: StoredGenericAgentRun,
    boundary: GenericLoopBoundary,
    recovery_events: Vec<AgentEventDraft>,
    mut continuation: GenericRecoveryContinuation,
) -> Result<AgentRecovery, AgentProtocolError> {
    let checkpoint_seq = stored.last_checkpoint_seq();
    let registration = stored.registration;
    let request = registration.request.clone();
    let execution = registration.execution.clone();
    let admission = registration.admission.clone();
    let run_id = execution.run_id.clone();
    let user_message = agent_input_message(&request)?;
    let run_skills = resolve_recovery_skill_binding(&inner, &registration)?;
    let model_definitions = model_definitions_for_run(&inner, run_skills.is_some());
    let (commands, queued_steers, mut pending_resolutions) =
        reconstruct_recovery_commands(&stored.records, &recovery_events)?;
    match &mut continuation {
        GenericRecoveryContinuation::ModelLoop { .. } if !pending_resolutions.is_empty() => {
            let pending = pending_resolutions
                .values()
                .next()
                .expect("non-empty pending resolution map was checked");
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "stable recovery cannot apply an accepted request resolution",
            )
            .with_details(serde_json::json!({
                "boundary": "accepted_resolution_pending",
                "command_id": pending.command_id,
            })));
        }
        GenericRecoveryContinuation::Input {
            round,
            call,
            request_open,
            committed_response,
            resolved_response,
            ..
        } => {
            let expected_request_id = input_request_id(&run_id, *round, &call.call_id);
            *committed_response = pending_resolutions.remove(&expected_request_id);
            if !pending_resolutions.is_empty() {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "accepted request resolution crossed the recovered input boundary",
                ));
            }
            if committed_response.is_some() && resolved_response.is_some() {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered input resolution was both pending and already applied",
                ));
            }
            if let Some(response) = committed_response.as_ref().or(resolved_response.as_ref()) {
                if !*request_open || !matches!(response.resolution, RequestResolution::Input { .. })
                {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "accepted resolution does not match the recovered pending input request",
                    ));
                }
            }
        }
        GenericRecoveryContinuation::ModelLoop { .. } => {}
    }
    let mut pending_inputs = BTreeMap::new();
    if let GenericRecoveryContinuation::Input {
        round,
        call,
        request_open: true,
        committed_response: None,
        resolved_response: None,
        response,
        ..
    } = &mut continuation
    {
        let request_id = input_request_id(&run_id, *round, &call.call_id);
        let (responder, receiver) = oneshot::channel();
        pending_inputs.insert(
            request_id,
            PendingInput {
                responder: Some(responder),
            },
        );
        *response = Some(receiver);
    }
    let published_resource_skips = recovery_events
        .iter()
        .filter_map(|event| match &event.payload {
            AgentEvent::ResourceBindingSkipped { skip } => Some(skip.binding_id.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let run_started = recovery_events
        .iter()
        .any(|event| matches!(&event.payload, AgentEvent::RunStarted));
    let started_event_id = AgentEventId::new(format!("generic-{}-started", run_id.as_str()));
    let mut supporting_event_ids = boundary.supporting_event_ids;
    if run_started && !supporting_event_ids.contains(&started_event_id) {
        supporting_event_ids.push(started_event_id);
    }
    let seed = GenericExecutionSeed {
        published_resource_skips,
        run_started,
        next_model_round: boundary.next_model_round,
        total_usage: boundary.usage,
        tool_call_count: boundary.tool_call_count,
        last_response: boundary.last_response,
        supporting_event_ids,
    };
    let (sender, _) = broadcast::channel(inner.config.stream_buffer);
    let cancellation = CancellationToken::new();
    let (steer_signal, steer_updates) = watch::channel(0_u64);
    let run = GenericRun {
        request: request.clone(),
        execution: execution.clone(),
        admission: admission.clone(),
        durable_events: recovery_events,
        sender,
        terminal: false,
        cancellation: cancellation.clone(),
        cancel_command: None,
        commands,
        queued_steers,
        steer_signal,
        pending_inputs,
        pending_approvals: BTreeMap::new(),
        checkpoint_seq,
    };
    let replay = InternalGenericAgentProvider::stream_for(&run);

    Ok(AgentRecovery::staged(replay, async move {
        let restore_initial_input = matches!(
            &continuation,
            GenericRecoveryContinuation::ModelLoop {
                restore_initial_input: true
            }
        );
        let initial_input = restore_initial_input.then(|| user_message.clone());
        let model_messages = project_model_messages(
            &inner,
            &request,
            &model_definitions,
            run_skills.as_deref(),
            initial_input,
        )
        .await
        .map_err(session_context_recovery_error)?;
        if let GenericRecoveryContinuation::Input {
            round,
            request_id,
            request_digest,
            ..
        } = &continuation
        {
            let rebuilt =
                model_request_for_round(&request, *round, &model_messages, &model_definitions);
            if rebuilt.request_id != *request_id
                || model_request_digest(&rebuilt).map_err(checkpoint_stream_error)?
                    != *request_digest
            {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered model request no longer matches the observed private WAL attempt",
                ));
            }
        }
        {
            let mut state = inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.runs.contains_key(&run_id) {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidTransition,
                    "Generic Agent Run was recovered concurrently",
                ));
            }
            let session = state
                .sessions
                .entry(request.run.spec.session_id.clone())
                .or_default();
            if session
                .active_run
                .as_ref()
                .is_some_and(|active| active != &run_id)
            {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidTransition,
                    "another Generic Agent Run already owns this Session",
                ));
            }
            session.active_run = Some(run_id.clone());
            state.runs.insert(run_id.clone(), run);
        }

        if restore_initial_input {
            if let Err(failure) =
                commit_loop_boundary(&inner, &run_id, 1, &ModelUsage::default(), 0, "", &[])
            {
                let mut state = inner
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let error = state
                    .runs
                    .get_mut(&run_id)
                    .map(|run| poison_run_after_checkpoint_failure(run, failure.clone()))
                    .unwrap_or_else(|| checkpoint_stream_error(failure));
                return Err(error);
            }
        }

        tokio::spawn(async move {
            match continuation {
                GenericRecoveryContinuation::ModelLoop { .. } => {
                    execute_model_run(
                        inner,
                        request,
                        admission,
                        user_message,
                        model_messages,
                        model_definitions,
                        run_skills,
                        seed,
                        cancellation,
                        steer_updates,
                    )
                    .await;
                }
                GenericRecoveryContinuation::Input {
                    round,
                    request_id,
                    observation,
                    call,
                    arguments,
                    prompt,
                    request_open,
                    committed_response,
                    resolved_response,
                    response,
                    ..
                } => {
                    resume_observed_input(
                        inner,
                        request,
                        admission,
                        user_message,
                        model_messages,
                        model_definitions,
                        run_skills,
                        seed,
                        cancellation,
                        steer_updates,
                        round,
                        request_id,
                        observation,
                        call,
                        arguments,
                        prompt,
                        request_open,
                        committed_response,
                        resolved_response,
                        response,
                    )
                    .await;
                }
            }
        });
        Ok(())
    }))
}

fn resolve_recovery_skill_binding(
    inner: &GenericInner,
    registration: &GenericAgentRunRegistration,
) -> Result<Option<Arc<SkillRuntime>>, AgentProtocolError> {
    let mut skipped = registration.admission.skipped_optional_bindings.clone();
    let skills =
        resolve_run_skill_binding(inner.skills.as_ref(), &registration.request, &mut skipped)
            .map_err(recovery_start_error)?;
    if skipped != registration.admission.skipped_optional_bindings {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovery resource admission does not match the immutable start",
        ));
    }
    Ok(skills)
}

fn reconstruct_recovery_commands(
    records: &[crate::generic_agent_checkpoint::GenericCheckpointRecord],
    recovery_events: &[AgentEventDraft],
) -> Result<
    (
        BTreeMap<CommandId, StoredCommand>,
        VecDeque<QueuedSteer>,
        BTreeMap<RequestId, InputResponse>,
    ),
    AgentProtocolError,
> {
    let applied_commands = recovery_events
        .iter()
        .filter_map(|event| {
            matches!(
                &event.payload,
                AgentEvent::InputCommitted { .. }
                    | AgentEvent::RequestResolved { .. }
                    | AgentEvent::StopRequested { .. }
            )
            .then(|| event.causation_id.clone())
            .flatten()
        })
        .collect::<BTreeSet<_>>();
    let mut commands = BTreeMap::new();
    let mut queued_steers = VecDeque::new();
    let mut pending_resolutions = BTreeMap::new();
    for record in records {
        let GenericCheckpointEvent::CommandCommitted { command, outcome } = &record.payload else {
            continue;
        };
        commands.insert(
            command.command_id.clone(),
            StoredCommand {
                digest: command.command_digest.clone(),
                outcome: outcome.clone(),
            },
        );
        if outcome != &ProviderCommandOutcome::Accepted {
            continue;
        }
        match &command.payload {
            AgentCommand::Cancel { .. } => {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidTransition,
                    "stable recovery cannot restart a Run with an accepted cancellation",
                )
                .with_details(serde_json::json!({
                    "boundary": "accepted_cancel_pending",
                    "command_id": command.command_id,
                })))
            }
            AgentCommand::Steer { content } if !applied_commands.contains(&command.command_id) => {
                queued_steers.push_back(QueuedSteer {
                    command_id: command.command_id.clone(),
                    content: content.clone(),
                    message: agent_content_message(content)?,
                })
            }
            AgentCommand::ResolveRequest { response }
                if !applied_commands.contains(&command.command_id) =>
            {
                let request_id = command.request_id.clone().ok_or_else(|| {
                    AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "accepted request resolution has no request identity",
                    )
                })?;
                if pending_resolutions
                    .insert(
                        request_id,
                        InputResponse {
                            command_id: command.command_id.clone(),
                            resolution: response.clone(),
                        },
                    )
                    .is_some()
                {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "multiple accepted resolutions target the same pending request",
                    ));
                }
            }
            AgentCommand::Steer { .. } | AgentCommand::ResolveRequest { .. } => {}
            _ => {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::Unsupported,
                    "stable recovery encountered an unsupported accepted command",
                ))
            }
        }
    }
    Ok((commands, queued_steers, pending_resolutions))
}

fn validate_execution_and_duplicate(
    run: &GenericRun,
    execution: &AgentExecutionRef,
    command: &AgentCommandEnvelope,
) -> Result<(), AgentProtocolError> {
    if run.execution != *execution {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::RunIdConflict,
            "execution reference does not match the Generic Agent Run",
        ));
    }
    if let Some(existing) = run.commands.get(&command.command_id) {
        if existing.digest != command.command_digest {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::DuplicateConflict,
                "command_id was reused with different content",
            ));
        }
    }
    Ok(())
}

fn record_command(
    inner: &GenericInner,
    run: &mut GenericRun,
    command: &AgentCommandEnvelope,
    outcome: ProviderCommandOutcome,
) -> Result<ProviderCommandDisposition, AgentProtocolError> {
    let disposition = ProviderCommandDisposition {
        command_id: command.command_id.clone(),
        run_id: command.run_id.clone(),
        outcome: outcome.clone(),
        duplicate: false,
    };
    let durable_disposition = disposition.to_event_draft()?;
    if let Err(failure) = append_checkpoint_to_run(
        inner,
        run,
        &command.run_id,
        GenericCheckpointEventId::new(format!(
            "generic-{}-command-{}",
            command.run_id.as_str(),
            command.command_id.as_str()
        )),
        GenericCheckpointEvent::CommandCommitted {
            command: command.clone(),
            outcome: outcome.clone(),
        },
    ) {
        return Err(poison_run_after_checkpoint_failure(run, failure));
    }
    run.commands.insert(
        command.command_id.clone(),
        StoredCommand {
            digest: command.command_digest.clone(),
            outcome: outcome.clone(),
        },
    );
    run.durable_events.push(durable_disposition);
    Ok(disposition)
}

async fn project_model_messages(
    inner: &GenericInner,
    request: &AgentStartRequest,
    model_definitions: &[ModelToolDefinition],
    run_skills: Option<&SkillRuntime>,
    initial_input: Option<ModelMessage>,
) -> Result<Vec<ModelMessage>, SessionContextError> {
    if let Some(message) = initial_input {
        inner
            .session_journal
            .append(AgentSessionEventDraft {
                event_id: AgentSessionEventId::new(format!(
                    "generic-{}-input",
                    request.run.spec.run_id.as_str()
                )),
                session_id: request.run.spec.session_id.clone(),
                run_id: request.run.spec.run_id.clone(),
                payload: AgentSessionEvent::RunInputCommitted { message },
            })
            .await?;
    }
    let backend_context_limit = inner
        .backend
        .descriptor()
        .capabilities
        .max_context_tokens
        .unwrap_or(inner.config.max_context_tokens)
        .min(inner.config.max_context_tokens);
    let max_context_tokens = request
        .run
        .spec
        .limits
        .max_input_tokens
        .map(|limit| {
            limit
                .saturating_add(inner.config.reserved_output_tokens)
                .min(backend_context_limit)
        })
        .unwrap_or(backend_context_limit);
    inner
        .context_engine
        .project(SessionContextRequest {
            session_id: request.run.spec.session_id.clone(),
            current_run_id: request.run.spec.run_id.clone(),
            system_message: system_message_for_run(&inner.config, run_skills),
            tools: model_definitions.to_vec(),
            max_context_tokens,
            reserved_output_tokens: inner.config.reserved_output_tokens,
            config_digest: inner.config_digest.clone(),
            allowed_skill_digests: run_skills
                .map(|skills| {
                    skills
                        .catalog()
                        .skills
                        .iter()
                        .map(|descriptor| (descriptor.skill_id.clone(), descriptor.digest.clone()))
                        .collect()
                })
                .unwrap_or_default(),
        })
        .await
        .map(|projection| projection.messages)
}

async fn execute_model_run(
    inner: Arc<GenericInner>,
    request: AgentStartRequest,
    admission: AgentAdmission,
    user_message: ModelMessage,
    mut model_messages: Vec<ModelMessage>,
    model_tools: Vec<ModelToolDefinition>,
    run_skills: Option<Arc<SkillRuntime>>,
    seed: GenericExecutionSeed,
    cancellation: CancellationToken,
    mut steer_updates: watch::Receiver<u64>,
) {
    let run_id = request.run.spec.run_id.clone();
    let GenericExecutionSeed {
        published_resource_skips,
        run_started,
        next_model_round,
        mut total_usage,
        mut tool_call_count,
        mut last_response,
        mut supporting_event_ids,
    } = seed;
    let started_event_id = AgentEventId::new(format!("generic-{}-started", run_id.as_str()));
    for (index, skip) in admission.skipped_optional_bindings.into_iter().enumerate() {
        if !published_resource_skips.contains(&skip.binding_id) {
            if !publish_durable(
                &inner,
                &run_id,
                AgentEventDraft {
                    event_id: AgentEventId::new(format!(
                        "generic-{}-resource-skip-{}",
                        run_id.as_str(),
                        index + 1
                    )),
                    run_id: run_id.clone(),
                    causation_id: None,
                    source_fingerprint: None,
                    payload: AgentEvent::ResourceBindingSkipped { skip },
                },
            ) {
                return;
            }
        }
    }
    if !run_started {
        if !publish_durable(
            &inner,
            &run_id,
            AgentEventDraft {
                event_id: started_event_id.clone(),
                run_id: run_id.clone(),
                causation_id: None,
                source_fingerprint: None,
                payload: AgentEvent::RunStarted,
            },
        ) {
            return;
        }
    }
    if !supporting_event_ids.contains(&started_event_id) {
        supporting_event_ids.push(started_event_id.clone());
    }

    let model_round_limit = request
        .run
        .spec
        .limits
        .max_model_steps
        .unwrap_or(inner.config.max_model_rounds)
        .min(inner.config.max_model_rounds);
    let tool_call_limit = request
        .run
        .spec
        .limits
        .max_tool_calls
        .unwrap_or(inner.config.max_tool_calls)
        .min(inner.config.max_tool_calls);
    let mut has_usage = total_usage.input_tokens.is_some() || total_usage.output_tokens.is_some();

    'model_rounds: for round in next_model_round..=model_round_limit {
        steer_updates.borrow_and_update();
        if let Err(failure) = commit_queued_steers(&inner, &request, &mut model_messages).await {
            emit_failure(&inner, &request, &user_message, failure);
            return;
        }
        let model_request = model_request_for_round(&request, round, &model_messages, &model_tools);
        if let Err(failure) = commit_model_attempt(&inner, &run_id, round, &model_request) {
            emit_failure(&inner, &request, &user_message, failure);
            return;
        }
        let model_cancellation = cancellation.child_token();
        let mut model_stream = match tokio::select! {
            _ = cancellation.cancelled() => {
                emit_cancel(&inner, &request, &user_message);
                return;
            }
            changed = steer_updates.changed() => {
                if changed.is_err() {
                    emit_failure(
                        &inner,
                        &request,
                        &user_message,
                        agent_failure("steer_channel_closed", "Steer control channel closed", true),
                    );
                    return;
                }
                model_cancellation.cancel();
                if let Err(failure) =
                    commit_queued_steers(&inner, &request, &mut model_messages).await
                {
                    emit_failure(&inner, &request, &user_message, failure);
                    return;
                }
                if let Err(failure) = commit_loop_boundary(
                    &inner,
                    &run_id,
                    round.saturating_add(1),
                    &total_usage,
                    tool_call_count,
                    &last_response,
                    &supporting_event_ids,
                ) {
                    emit_failure(&inner, &request, &user_message, failure);
                    return;
                }
                continue 'model_rounds;
            }
            result = inner.backend.start(model_request.clone(), model_cancellation.clone()) => result,
        } {
            Ok(stream) => stream,
            Err(error) => {
                if cancellation.is_cancelled() {
                    emit_cancel(&inner, &request, &user_message);
                } else {
                    emit_failure(&inner, &request, &user_message, model_failure(error));
                }
                return;
            }
        };

        let mut expected_sequence = 1;
        let mut response = String::new();
        let mut round_usage = None;
        let mut tool_calls = Vec::<PendingModelToolCall>::new();
        loop {
            let item = tokio::select! {
                _ = cancellation.cancelled() => {
                    emit_cancel(&inner, &request, &user_message);
                    return;
                }
                changed = steer_updates.changed() => {
                    if changed.is_err() {
                        emit_failure(
                            &inner,
                            &request,
                            &user_message,
                            agent_failure("steer_channel_closed", "Steer control channel closed", true),
                        );
                        return;
                    }
                    model_cancellation.cancel();
                    if let Err(failure) =
                        commit_queued_steers(&inner, &request, &mut model_messages).await
                    {
                        emit_failure(&inner, &request, &user_message, failure);
                        return;
                    }
                    if let Err(failure) = commit_loop_boundary(
                        &inner,
                        &run_id,
                        round.saturating_add(1),
                        &total_usage,
                        tool_call_count,
                        &last_response,
                        &supporting_event_ids,
                    ) {
                        emit_failure(&inner, &request, &user_message, failure);
                        return;
                    }
                    continue 'model_rounds;
                }
                item = model_stream.next() => item,
            };
            let event = match item {
                Some(Ok(event)) => event,
                Some(Err(error)) => {
                    emit_failure(&inner, &request, &user_message, model_failure(error));
                    return;
                }
                None => {
                    emit_failure(
                        &inner,
                        &request,
                        &user_message,
                        agent_failure(
                            "model_stream_ended",
                            "model stream ended without Finish",
                            true,
                        ),
                    );
                    return;
                }
            };
            if let Err(error) = event.validate_for(&model_request.request_id, expected_sequence) {
                emit_failure(&inner, &request, &user_message, model_failure(error));
                return;
            }
            expected_sequence += 1;
            match event.payload {
                ModelEvent::TextDelta { delta } => {
                    response.push_str(&delta);
                    publish_telemetry(
                        &inner,
                        &run_id,
                        AgentTelemetryEnvelope {
                            telemetry_id: TelemetryId::new(format!(
                                "generic-{}-round-{round}-delta-{}",
                                run_id.as_str(),
                                event.sequence
                            )),
                            run_id: run_id.clone(),
                            provider_seq: Some(event.sequence),
                            payload: AgentTelemetry::OutputDelta {
                                output_id: OutputId::new(format!(
                                    "generic-{}-response",
                                    run_id.as_str()
                                )),
                                delta: Content::text(delta),
                            },
                        },
                    );
                }
                ModelEvent::ToolCallStart { call_id, name } => {
                    if tool_calls.iter().any(|call| call.call_id == call_id) {
                        emit_failure(
                            &inner,
                            &request,
                            &user_message,
                            model_event_failure("duplicate model Tool call id"),
                        );
                        return;
                    }
                    tool_calls.push(PendingModelToolCall {
                        call_id,
                        name,
                        arguments: String::new(),
                        ended: false,
                    });
                }
                ModelEvent::ToolCallArgumentsDelta { call_id, delta } => {
                    let Some(call) = tool_calls
                        .iter_mut()
                        .find(|call| call.call_id == call_id && !call.ended)
                    else {
                        emit_failure(
                            &inner,
                            &request,
                            &user_message,
                            model_event_failure("Tool arguments arrived before start or after end"),
                        );
                        return;
                    };
                    call.arguments.push_str(&delta);
                }
                ModelEvent::ToolCallEnd { call_id } => {
                    let Some(call) = tool_calls
                        .iter_mut()
                        .find(|call| call.call_id == call_id && !call.ended)
                    else {
                        emit_failure(
                            &inner,
                            &request,
                            &user_message,
                            model_event_failure("Tool call ended without one active start"),
                        );
                        return;
                    };
                    call.ended = true;
                }
                ModelEvent::Usage { usage: observed } => round_usage = Some(observed),
                ModelEvent::Finish { reason } => {
                    let committed_usage = round_usage.take();
                    if let Err(failure) = commit_model_observation(
                        &inner,
                        &run_id,
                        round,
                        &model_request.request_id,
                        GenericModelObservation {
                            finish_reason: reason.clone(),
                            response: response.clone(),
                            usage: committed_usage.clone(),
                            tool_calls: tool_calls
                                .iter()
                                .map(|call| GenericObservedToolCall {
                                    call_id: call.call_id.clone(),
                                    name: call.name.clone(),
                                    arguments: call.arguments.clone(),
                                    ended: call.ended,
                                })
                                .collect(),
                        },
                    ) {
                        emit_failure(&inner, &request, &user_message, failure);
                        return;
                    }
                    if let Some(observed) = committed_usage.clone() {
                        merge_usage(&mut total_usage, observed);
                        has_usage = true;
                    }
                    match reason {
                        ModelFinishReason::Stop
                            if tool_calls.is_empty() && !response.is_empty() =>
                        {
                            if let Err(failure) = append_session_event(
                                &inner,
                                AgentSessionEventDraft {
                                    event_id: AgentSessionEventId::new(format!(
                                        "generic-{}-output-{round}",
                                        run_id.as_str(),
                                    )),
                                    session_id: request.run.spec.session_id.clone(),
                                    run_id: run_id.clone(),
                                    payload: AgentSessionEvent::RunOutputCommitted {
                                        request_id: model_request.request_id.clone(),
                                        message: ModelMessage::text(
                                            ModelRole::Assistant,
                                            response.clone(),
                                        ),
                                        usage: committed_usage,
                                    },
                                },
                            )
                            .await
                            {
                                emit_failure(&inner, &request, &user_message, failure);
                                return;
                            }
                            match try_emit_delivery(
                                &inner,
                                &request,
                                response.clone(),
                                has_usage.then_some(total_usage.clone()),
                                tool_call_count,
                                supporting_event_ids.clone(),
                            ) {
                                DeliveryCommit::Committed => {
                                    finish_session(&inner, &request);
                                    return;
                                }
                                DeliveryCommit::SteerPending => {
                                    last_response = response.clone();
                                    model_messages
                                        .push(ModelMessage::text(ModelRole::Assistant, response));
                                    if let Err(failure) =
                                        commit_queued_steers(&inner, &request, &mut model_messages)
                                            .await
                                    {
                                        emit_failure(&inner, &request, &user_message, failure);
                                        return;
                                    }
                                    if let Err(failure) = commit_loop_boundary(
                                        &inner,
                                        &run_id,
                                        round.saturating_add(1),
                                        &total_usage,
                                        tool_call_count,
                                        &last_response,
                                        &supporting_event_ids,
                                    ) {
                                        emit_failure(&inner, &request, &user_message, failure);
                                        return;
                                    }
                                    continue 'model_rounds;
                                }
                                DeliveryCommit::CancelPending => {
                                    emit_cancel(&inner, &request, &user_message);
                                    return;
                                }
                                DeliveryCommit::CheckpointFailed => return,
                                DeliveryCommit::AlreadyTerminal => return,
                            }
                        }
                        ModelFinishReason::Length => {
                            emit_incomplete(
                                &inner,
                                &request,
                                &user_message,
                                response,
                                has_usage.then_some(total_usage),
                                tool_call_count,
                                started_event_id,
                                RunLimitKind::OutputTokens,
                                "model output limit reached",
                            );
                            return;
                        }
                        ModelFinishReason::Cancelled => {
                            emit_cancel(&inner, &request, &user_message);
                            return;
                        }
                        ModelFinishReason::ToolCalls | ModelFinishReason::Stop
                            if !tool_calls.is_empty() => {}
                        _ => {
                            emit_failure(
                                &inner,
                                &request,
                                &user_message,
                                agent_failure(
                                    "model_incomplete",
                                    format!(
                                        "model ended without a deliverable response: {reason:?}"
                                    ),
                                    false,
                                ),
                            );
                            return;
                        }
                    }

                    if tool_calls.iter().any(|call| !call.ended) {
                        emit_failure(
                            &inner,
                            &request,
                            &user_message,
                            model_event_failure("model finished with an incomplete Tool call"),
                        );
                        return;
                    }
                    let mut assistant_content = Vec::new();
                    if !response.is_empty() {
                        last_response = response.clone();
                        assistant_content.push(ModelContent::Text {
                            text: response.clone(),
                        });
                    }
                    let mut parsed_calls = Vec::with_capacity(tool_calls.len());
                    for call in tool_calls {
                        let arguments = match parse_tool_arguments(&call) {
                            Ok(arguments) => arguments,
                            Err(failure) => {
                                emit_failure(&inner, &request, &user_message, failure);
                                return;
                            }
                        };
                        assistant_content.push(ModelContent::ToolCall {
                            call_id: call.call_id.clone(),
                            name: call.name.clone(),
                            arguments: arguments.clone(),
                        });
                        parsed_calls.push((call, arguments));
                    }
                    let assistant_message = ModelMessage {
                        role: ModelRole::Assistant,
                        content: assistant_content,
                    };

                    let mut tool_results = Vec::with_capacity(parsed_calls.len());
                    let mut activated_context_messages = Vec::new();
                    for (call, arguments) in parsed_calls {
                        if call.name == REQUEST_INPUT_TOOL_NAME {
                            let prompt = match parse_input_request(arguments) {
                                Ok(prompt) => prompt,
                                Err(failure) => {
                                    emit_failure(&inner, &request, &user_message, failure);
                                    return;
                                }
                            };
                            let result = match await_agent_input(
                                inner.clone(),
                                &run_id,
                                round,
                                &call.call_id,
                                prompt,
                                cancellation.clone(),
                            )
                            .await
                            {
                                InputWaitOutcome::Resolved(result) => result,
                                InputWaitOutcome::Cancelled => {
                                    emit_cancel(&inner, &request, &user_message);
                                    return;
                                }
                                InputWaitOutcome::Failed(failure) => {
                                    emit_failure(&inner, &request, &user_message, failure);
                                    return;
                                }
                            };
                            tool_results.push(ModelContent::ToolResult {
                                call_id: call.call_id,
                                result,
                                is_error: false,
                            });
                            continue;
                        }
                        if call.name == SKILL_ACTIVATE_TOOL_NAME {
                            let Some(skills) = run_skills.as_ref() else {
                                emit_failure(
                                    &inner,
                                    &request,
                                    &user_message,
                                    agent_failure(
                                        "skill_catalog_unavailable",
                                        "model requested Skill activation without a bound Skill catalog",
                                        false,
                                    ),
                                );
                                return;
                            };
                            let observation = match execute_skill_activation(
                                &inner,
                                &request,
                                skills,
                                round,
                                &call.call_id,
                                arguments,
                            )
                            .await
                            {
                                Ok(observation) => observation,
                                Err(failure) => {
                                    emit_failure(&inner, &request, &user_message, failure);
                                    return;
                                }
                            };
                            if let Some(message) = observation.context_message {
                                activated_context_messages.push(message);
                            }
                            tool_results.push(ModelContent::ToolResult {
                                call_id: call.call_id,
                                result: observation.result,
                                is_error: observation.is_error,
                            });
                            continue;
                        }
                        let Some(tools) = inner.tools.as_ref() else {
                            emit_failure(
                                &inner,
                                &request,
                                &user_message,
                                agent_failure(
                                    "tool_runtime_unavailable",
                                    "model requested an effect Tool but this Agent has no Host Tool runtime",
                                    false,
                                ),
                            );
                            return;
                        };
                        if tool_call_count >= tool_call_limit {
                            emit_incomplete(
                                &inner,
                                &request,
                                &user_message,
                                last_response,
                                has_usage.then_some(total_usage),
                                tool_call_count,
                                started_event_id,
                                RunLimitKind::ToolCalls,
                                "Tool call limit reached",
                            );
                            return;
                        }
                        tool_call_count += 1;
                        if call.name == WORKFLOW_TOOL_NAME {
                            let remaining_tool_calls =
                                tool_call_limit.saturating_sub(tool_call_count);
                            if remaining_tool_calls == 0 {
                                emit_incomplete(
                                    &inner,
                                    &request,
                                    &user_message,
                                    last_response,
                                    has_usage.then_some(total_usage),
                                    tool_call_count,
                                    started_event_id,
                                    RunLimitKind::ToolCalls,
                                    "Workflow has no remaining Tool call budget",
                                );
                                return;
                            }
                            let observation = match execute_workflow_call(
                                inner.clone(),
                                tools,
                                &run_id,
                                &call.call_id,
                                arguments,
                                remaining_tool_calls,
                                cancellation.clone(),
                            )
                            .await
                            {
                                WorkflowCallExecution::Observed(observation) => observation,
                                WorkflowCallExecution::Cancelled => {
                                    emit_cancel(&inner, &request, &user_message);
                                    return;
                                }
                            };
                            tool_call_count =
                                tool_call_count.saturating_add(observation.tool_calls);
                            let Some(workflow_event_id) = publish_workflow_output(
                                &inner,
                                &run_id,
                                round,
                                &call.call_id,
                                observation.result.clone(),
                            ) else {
                                return;
                            };
                            supporting_event_ids.push(workflow_event_id);
                            tool_results.push(ModelContent::ToolResult {
                                call_id: call.call_id,
                                result: observation.result,
                                is_error: observation.is_error,
                            });
                            continue;
                        }
                        let tool_id = match tools.runtime.resolve_tool_id(&call.name) {
                            Ok(Some(tool_id)) => tool_id,
                            Ok(None) => {
                                emit_failure(
                                    &inner,
                                    &request,
                                    &user_message,
                                    agent_failure(
                                        "tool_not_found",
                                        format!("model requested an unknown Tool: {}", call.name),
                                        false,
                                    ),
                                );
                                return;
                            }
                            Err(error) => {
                                emit_failure(
                                    &inner,
                                    &request,
                                    &user_message,
                                    agent_failure(
                                        "tool_runtime_unavailable",
                                        error.to_string(),
                                        true,
                                    ),
                                );
                                return;
                            }
                        };
                        let invocation = ToolInvocation {
                            run_id: run_id.clone(),
                            call_id: ToolCallId::new(call.call_id.as_str()),
                            tool_id,
                            arguments,
                        };
                        let result = tools
                            .runtime
                            .invoke(
                                invocation.clone(),
                                tools.run_grant.clone(),
                                None,
                                cancellation.clone(),
                            )
                            .await;
                        let result = match result {
                            GuardedToolResult::ApprovalRequired { binding, summary } => {
                                match await_tool_approval(
                                    inner.clone(),
                                    tools,
                                    &run_id,
                                    round,
                                    &call.call_id,
                                    binding,
                                    summary,
                                    cancellation.clone(),
                                )
                                .await
                                {
                                    ApprovalWaitOutcome::Allowed(capability) => {
                                        tools
                                            .runtime
                                            .invoke(
                                                invocation,
                                                tools.run_grant.clone(),
                                                Some(capability),
                                                cancellation.clone(),
                                            )
                                            .await
                                    }
                                    ApprovalWaitOutcome::Denied => GuardedToolResult::Outcome {
                                        outcome: ToolOutcome::Rejected {
                                            code: "approval_denied".to_owned(),
                                            message: "Host denied this Tool invocation".to_owned(),
                                        },
                                        cached: false,
                                    },
                                    ApprovalWaitOutcome::Cancelled => {
                                        emit_cancel(&inner, &request, &user_message);
                                        return;
                                    }
                                    ApprovalWaitOutcome::Failed(failure) => {
                                        emit_failure(&inner, &request, &user_message, failure);
                                        return;
                                    }
                                }
                            }
                            result => result,
                        };
                        match result {
                            GuardedToolResult::ApprovalRequired { binding, .. } => {
                                emit_failure(
                                    &inner,
                                    &request,
                                    &user_message,
                                    AgentFailure {
                                        code: "approval_capability_rejected".to_owned(),
                                        message: "Tool still requires approval after the Host resolved the exact request".to_owned(),
                                        retryable: false,
                                        details: serde_json::to_value(binding)
                                            .unwrap_or(serde_json::Value::Null),
                                    },
                                );
                                return;
                            }
                            GuardedToolResult::Outcome {
                                outcome: ToolOutcome::UnknownEffect { .. },
                                ..
                            } if cancellation.is_cancelled() => {
                                // The effect journal deliberately retains UnknownEffect, while
                                // the Agent Run still observes the user's cancellation as its
                                // terminal control outcome. A late Tool result is never accepted.
                                emit_cancel(&inner, &request, &user_message);
                                return;
                            }
                            GuardedToolResult::Outcome {
                                outcome: ToolOutcome::UnknownEffect { message },
                                ..
                            } => {
                                emit_failure(
                                    &inner,
                                    &request,
                                    &user_message,
                                    agent_failure("tool_unknown_effect", message, false),
                                );
                                return;
                            }
                            GuardedToolResult::Outcome {
                                outcome: ToolOutcome::Cancelled,
                                ..
                            } if cancellation.is_cancelled() => {
                                emit_cancel(&inner, &request, &user_message);
                                return;
                            }
                            GuardedToolResult::Outcome { outcome, .. } => {
                                let (result, is_error) = model_tool_result(outcome);
                                tool_results.push(ModelContent::ToolResult {
                                    call_id: call.call_id,
                                    result,
                                    is_error,
                                });
                            }
                        }
                    }
                    let tool_message = ModelMessage {
                        role: ModelRole::Tool,
                        content: tool_results,
                    };
                    if let Err(failure) = append_session_event(
                        &inner,
                        AgentSessionEventDraft {
                            event_id: AgentSessionEventId::new(format!(
                                "generic-{}-tool-exchange-{round}",
                                run_id.as_str()
                            )),
                            session_id: request.run.spec.session_id.clone(),
                            run_id: run_id.clone(),
                            payload: AgentSessionEvent::ToolExchangeCommitted {
                                request_id: model_request.request_id.clone(),
                                assistant: assistant_message.clone(),
                                tool: tool_message.clone(),
                                usage: committed_usage,
                            },
                        },
                    )
                    .await
                    {
                        emit_failure(&inner, &request, &user_message, failure);
                        return;
                    }
                    model_messages.extend(activated_context_messages);
                    model_messages.push(assistant_message);
                    model_messages.push(tool_message);
                    if let Err(failure) = commit_loop_boundary(
                        &inner,
                        &run_id,
                        round.saturating_add(1),
                        &total_usage,
                        tool_call_count,
                        &last_response,
                        &supporting_event_ids,
                    ) {
                        emit_failure(&inner, &request, &user_message, failure);
                        return;
                    }
                    continue 'model_rounds;
                }
                _ => {
                    emit_failure(
                        &inner,
                        &request,
                        &user_message,
                        agent_failure(
                            "unknown_model_event",
                            "model backend emitted an unsupported event",
                            false,
                        ),
                    );
                    return;
                }
            }
        }
    }

    emit_incomplete(
        &inner,
        &request,
        &user_message,
        last_response,
        has_usage.then_some(total_usage),
        tool_call_count,
        started_event_id,
        RunLimitKind::ModelSteps,
        "model step limit reached",
    );
}

#[allow(clippy::too_many_arguments)]
async fn resume_observed_input(
    inner: Arc<GenericInner>,
    request: AgentStartRequest,
    admission: AgentAdmission,
    user_message: ModelMessage,
    mut model_messages: Vec<ModelMessage>,
    model_tools: Vec<ModelToolDefinition>,
    run_skills: Option<Arc<SkillRuntime>>,
    mut seed: GenericExecutionSeed,
    cancellation: CancellationToken,
    steer_updates: watch::Receiver<u64>,
    round: u64,
    model_request_id: ModelRequestId,
    observation: GenericModelObservation,
    call: GenericObservedToolCall,
    arguments: serde_json::Value,
    prompt: String,
    request_open: bool,
    committed_response: Option<InputResponse>,
    resolved_response: Option<InputResponse>,
    response: Option<oneshot::Receiver<InputResponse>>,
) {
    let run_id = request.run.spec.run_id.clone();
    if let Some(usage) = observation.usage.clone() {
        merge_usage(&mut seed.total_usage, usage);
    }
    let mut assistant_content = Vec::new();
    if !observation.response.is_empty() {
        seed.last_response = observation.response.clone();
        assistant_content.push(ModelContent::Text {
            text: observation.response,
        });
    }
    assistant_content.push(ModelContent::ToolCall {
        call_id: call.call_id.clone(),
        name: call.name,
        arguments,
    });
    let assistant_message = ModelMessage {
        role: ModelRole::Assistant,
        content: assistant_content,
    };
    let input = if let Some(resolved_response) = resolved_response {
        input_resolution_outcome(resolved_response.resolution)
    } else if let Some(committed_response) = committed_response {
        commit_input_response(
            &inner,
            &run_id,
            round,
            &call.call_id,
            input_request_id(&run_id, round, &call.call_id),
            committed_response,
        )
    } else if request_open {
        await_recovered_agent_input(
            inner.clone(),
            &run_id,
            round,
            &call.call_id,
            response.expect("reattached input request owns a response channel"),
            cancellation.clone(),
        )
        .await
    } else {
        await_agent_input(
            inner.clone(),
            &run_id,
            round,
            &call.call_id,
            prompt,
            cancellation.clone(),
        )
        .await
    };
    let result = match input {
        InputWaitOutcome::Resolved(result) => result,
        InputWaitOutcome::Cancelled => {
            emit_cancel(&inner, &request, &user_message);
            return;
        }
        InputWaitOutcome::Failed(failure) => {
            emit_failure(&inner, &request, &user_message, failure);
            return;
        }
    };
    let tool_message = ModelMessage {
        role: ModelRole::Tool,
        content: vec![ModelContent::ToolResult {
            call_id: call.call_id,
            result,
            is_error: false,
        }],
    };
    if let Err(failure) = append_session_event(
        &inner,
        AgentSessionEventDraft {
            event_id: AgentSessionEventId::new(format!(
                "generic-{}-tool-exchange-{round}",
                run_id.as_str()
            )),
            session_id: request.run.spec.session_id.clone(),
            run_id: run_id.clone(),
            payload: AgentSessionEvent::ToolExchangeCommitted {
                request_id: model_request_id,
                assistant: assistant_message.clone(),
                tool: tool_message.clone(),
                usage: observation.usage,
            },
        },
    )
    .await
    {
        emit_failure(&inner, &request, &user_message, failure);
        return;
    }
    model_messages.push(assistant_message);
    model_messages.push(tool_message);
    seed.next_model_round = round.saturating_add(1);
    if let Err(failure) = commit_loop_boundary(
        &inner,
        &run_id,
        seed.next_model_round,
        &seed.total_usage,
        seed.tool_call_count,
        &seed.last_response,
        &seed.supporting_event_ids,
    ) {
        emit_failure(&inner, &request, &user_message, failure);
        return;
    }
    execute_model_run(
        inner,
        request,
        admission,
        user_message,
        model_messages,
        model_tools,
        run_skills,
        seed,
        cancellation,
        steer_updates,
    )
    .await;
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InputRequestArguments {
    prompt: String,
}

fn parse_input_request(arguments: serde_json::Value) -> Result<String, AgentFailure> {
    let arguments =
        serde_json::from_value::<InputRequestArguments>(arguments).map_err(|error| {
            agent_failure(
                "input_request_arguments_invalid",
                format!("model emitted invalid input request arguments: {error}"),
                false,
            )
        })?;
    if arguments.prompt.trim().is_empty() {
        return Err(agent_failure(
            "input_request_arguments_invalid",
            "input request prompt must not be empty",
            false,
        ));
    }
    Ok(arguments.prompt)
}

enum InputWaitOutcome {
    Resolved(serde_json::Value),
    Cancelled,
    Failed(AgentFailure),
}

fn input_request_id(run_id: &RunId, round: u64, model_call_id: &ModelToolCallId) -> RequestId {
    RequestId::new(format!(
        "input:{}:{round}:{}",
        run_id.as_str(),
        model_call_id.as_str()
    ))
}

async fn await_agent_input(
    inner: Arc<GenericInner>,
    run_id: &RunId,
    round: u64,
    model_call_id: &ModelToolCallId,
    prompt: String,
    cancellation: CancellationToken,
) -> InputWaitOutcome {
    let request_id = input_request_id(run_id, round, model_call_id);
    let (responder, response) = oneshot::channel();
    let registration = {
        let mut state = inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match state.runs.get_mut(run_id) {
            None => Err(agent_failure(
                "input_run_missing",
                "Run disappeared before its input request was opened",
                true,
            )),
            Some(run) if run.terminal || run.pending_inputs.contains_key(&request_id) => {
                Err(agent_failure(
                    "input_request_conflict",
                    "input request identity is no longer available",
                    false,
                ))
            }
            Some(run) => {
                run.pending_inputs.insert(
                    request_id.clone(),
                    PendingInput {
                        responder: Some(responder),
                    },
                );
                Ok(())
            }
        }
    };
    if let Err(failure) = registration {
        return InputWaitOutcome::Failed(failure);
    }
    if !publish_durable(
        &inner,
        run_id,
        AgentEventDraft {
            event_id: AgentEventId::new(format!(
                "generic-{}-input-{round}-{}-opened",
                run_id.as_str(),
                model_call_id.as_str()
            )),
            run_id: run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RequestOpened {
                request: PendingRequest {
                    request_id: request_id.clone(),
                    blocking: true,
                    payload: PendingRequestPayload::Input {
                        prompt: vec![Content::text(prompt)],
                        input_schema: None,
                    },
                },
            },
        },
    ) {
        remove_pending_input(&inner, run_id, &request_id);
        return InputWaitOutcome::Failed(agent_failure(
            "generic_checkpoint",
            "input request could not be committed to the private WAL",
            true,
        ));
    }

    let response = tokio::select! {
        biased;
        _ = cancellation.cancelled() => None,
        response = response => Some(response),
    };
    let Some(response) = response else {
        remove_pending_input(&inner, run_id, &request_id);
        return InputWaitOutcome::Cancelled;
    };
    let response = match response {
        Ok(response) => response,
        Err(_) => {
            remove_pending_input(&inner, run_id, &request_id);
            return InputWaitOutcome::Failed(agent_failure(
                "input_waiter_closed",
                "input response channel closed before resolution",
                true,
            ));
        }
    };
    commit_input_response(&inner, run_id, round, model_call_id, request_id, response)
}

async fn await_recovered_agent_input(
    inner: Arc<GenericInner>,
    run_id: &RunId,
    round: u64,
    model_call_id: &ModelToolCallId,
    response: oneshot::Receiver<InputResponse>,
    cancellation: CancellationToken,
) -> InputWaitOutcome {
    let request_id = input_request_id(run_id, round, model_call_id);
    let response = tokio::select! {
        biased;
        _ = cancellation.cancelled() => None,
        response = response => Some(response),
    };
    let Some(response) = response else {
        remove_pending_input(&inner, run_id, &request_id);
        return InputWaitOutcome::Cancelled;
    };
    let response = match response {
        Ok(response) => response,
        Err(_) => {
            remove_pending_input(&inner, run_id, &request_id);
            return InputWaitOutcome::Failed(agent_failure(
                "input_waiter_closed",
                "recovered input response channel closed before resolution",
                true,
            ));
        }
    };
    commit_input_response(&inner, run_id, round, model_call_id, request_id, response)
}

fn commit_input_response(
    inner: &GenericInner,
    run_id: &RunId,
    round: u64,
    model_call_id: &ModelToolCallId,
    request_id: RequestId,
    response: InputResponse,
) -> InputWaitOutcome {
    let resolution_digest = match response.resolution.digest() {
        Ok(digest) => digest,
        Err(error) => {
            return InputWaitOutcome::Failed(agent_failure(
                "input_resolution_invalid",
                error.to_string(),
                false,
            ));
        }
    };
    if !publish_durable(
        inner,
        run_id,
        AgentEventDraft {
            event_id: AgentEventId::new(format!(
                "generic-{}-input-{round}-{}-resolved",
                run_id.as_str(),
                model_call_id.as_str()
            )),
            run_id: run_id.clone(),
            causation_id: Some(response.command_id),
            source_fingerprint: None,
            payload: AgentEvent::RequestResolved {
                request_id,
                resolution: response.resolution.clone(),
                resolution_digest,
            },
        },
    ) {
        return InputWaitOutcome::Failed(agent_failure(
            "generic_checkpoint",
            "input resolution could not be committed to the private WAL",
            true,
        ));
    }
    input_resolution_outcome(response.resolution)
}

fn input_resolution_outcome(resolution: RequestResolution) -> InputWaitOutcome {
    match resolution {
        RequestResolution::Input { content } => InputWaitOutcome::Resolved(serde_json::json!({
            "content": content,
        })),
        _ => InputWaitOutcome::Failed(agent_failure(
            "input_resolution_invalid",
            "input request received a non-input resolution",
            false,
        )),
    }
}

fn remove_pending_input(inner: &GenericInner, run_id: &RunId, request_id: &RequestId) {
    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(run) = state.runs.get_mut(run_id) {
        run.pending_inputs.remove(request_id);
    }
}

enum ApprovalWaitOutcome {
    Allowed(ApprovalCapability),
    Denied,
    Cancelled,
    Failed(AgentFailure),
}

async fn await_tool_approval(
    inner: Arc<GenericInner>,
    tools: &GenericTools,
    run_id: &RunId,
    round: u64,
    model_call_id: &ModelToolCallId,
    binding: ApprovalBinding,
    summary: String,
    cancellation: CancellationToken,
) -> ApprovalWaitOutcome {
    let Some(bridge) = tools.approval_bridge.clone() else {
        return ApprovalWaitOutcome::Failed(AgentFailure {
            code: "approval_interaction_not_connected".to_owned(),
            message:
                "Tool requires Host approval, but this Agent has no approval interaction bridge"
                    .to_owned(),
            retryable: false,
            details: serde_json::to_value(binding).unwrap_or(serde_json::Value::Null),
        });
    };
    if binding.run_id != *run_id || binding.call_id.as_str() != model_call_id.as_str() {
        return ApprovalWaitOutcome::Failed(agent_failure(
            "approval_binding_mismatch",
            "Tool Runtime returned an approval binding for another invocation",
            false,
        ));
    }
    let operation_digest = match binding.digest() {
        Ok(digest) => digest,
        Err(error) => {
            return ApprovalWaitOutcome::Failed(agent_failure(
                "approval_binding_invalid",
                error.to_string(),
                false,
            ));
        }
    };
    let requested_scope = match approval_scope_names(&binding) {
        Ok(scopes) => scopes,
        Err(failure) => return ApprovalWaitOutcome::Failed(failure),
    };
    let request_id = RequestId::new(format!(
        "approval:{}:{round}:{}",
        run_id.as_str(),
        model_call_id.as_str()
    ));
    if let Err(error) = bridge.stage(&request_id, binding.clone()).await {
        return ApprovalWaitOutcome::Failed(agent_failure(
            "approval_bridge",
            error.to_string(),
            true,
        ));
    }

    let (responder, response) = oneshot::channel();
    let registration = {
        let mut state = inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match state.runs.get_mut(run_id) {
            None => Err(agent_failure(
                "approval_run_missing",
                "Run disappeared before its approval request was opened",
                true,
            )),
            Some(run) if run.terminal || run.pending_approvals.contains_key(&request_id) => {
                Err(agent_failure(
                    "approval_request_conflict",
                    "approval request identity is no longer available",
                    false,
                ))
            }
            Some(run) => {
                run.pending_approvals.insert(
                    request_id.clone(),
                    PendingApproval {
                        binding: binding.clone(),
                        responder: Some(responder),
                    },
                );
                Ok(())
            }
        }
    };
    if let Err(failure) = registration {
        let _ = bridge.clear(&request_id).await;
        return ApprovalWaitOutcome::Failed(failure);
    }
    if !publish_durable(
        &inner,
        run_id,
        AgentEventDraft {
            event_id: AgentEventId::new(format!(
                "generic-{}-approval-{round}-{}-opened",
                run_id.as_str(),
                model_call_id.as_str()
            )),
            run_id: run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RequestOpened {
                request: PendingRequest {
                    request_id: request_id.clone(),
                    blocking: true,
                    payload: PendingRequestPayload::Approval {
                        operation_digest,
                        requested_scope,
                        reason: summary,
                    },
                },
            },
        },
    ) {
        remove_pending_approval(&inner, run_id, &request_id);
        let _ = bridge.clear(&request_id).await;
        return ApprovalWaitOutcome::Failed(agent_failure(
            "generic_checkpoint",
            "approval request could not be committed to the private WAL",
            true,
        ));
    }

    let response = tokio::select! {
        biased;
        _ = cancellation.cancelled() => None,
        response = response => Some(response),
    };
    let Some(response) = response else {
        remove_pending_approval(&inner, run_id, &request_id);
        let _ = bridge.clear(&request_id).await;
        return ApprovalWaitOutcome::Cancelled;
    };
    let response = match response {
        Ok(response) => response,
        Err(_) => {
            remove_pending_approval(&inner, run_id, &request_id);
            let _ = bridge.clear(&request_id).await;
            return ApprovalWaitOutcome::Failed(agent_failure(
                "approval_waiter_closed",
                "approval response channel closed before resolution",
                true,
            ));
        }
    };
    let resolution_digest = match response.resolution.digest() {
        Ok(digest) => digest,
        Err(error) => {
            let _ = bridge.clear(&request_id).await;
            return ApprovalWaitOutcome::Failed(agent_failure(
                "approval_resolution_invalid",
                error.to_string(),
                false,
            ));
        }
    };
    if !publish_durable(
        &inner,
        run_id,
        AgentEventDraft {
            event_id: AgentEventId::new(format!(
                "generic-{}-approval-{round}-{}-resolved",
                run_id.as_str(),
                model_call_id.as_str()
            )),
            run_id: run_id.clone(),
            causation_id: Some(response.command_id),
            source_fingerprint: None,
            payload: AgentEvent::RequestResolved {
                request_id: request_id.clone(),
                resolution: response.resolution.clone(),
                resolution_digest,
            },
        },
    ) {
        let _ = bridge.clear(&request_id).await;
        return ApprovalWaitOutcome::Failed(agent_failure(
            "generic_checkpoint",
            "approval resolution could not be committed to the private WAL",
            true,
        ));
    }
    if let Err(error) = bridge.clear(&request_id).await {
        return ApprovalWaitOutcome::Failed(agent_failure(
            "approval_bridge",
            error.to_string(),
            true,
        ));
    }
    match (response.resolution, response.capability) {
        (
            RequestResolution::Approval {
                decision: ApprovalDecision::Allow,
                ..
            },
            Some(capability),
        ) => ApprovalWaitOutcome::Allowed(capability),
        (
            RequestResolution::Approval {
                decision: ApprovalDecision::Deny,
                ..
            },
            None,
        ) => ApprovalWaitOutcome::Denied,
        _ => ApprovalWaitOutcome::Failed(agent_failure(
            "approval_resolution_invalid",
            "approval resolution and capability do not agree",
            false,
        )),
    }
}

fn remove_pending_approval(inner: &GenericInner, run_id: &RunId, request_id: &RequestId) {
    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(run) = state.runs.get_mut(run_id) {
        run.pending_approvals.remove(request_id);
    }
}

fn approval_scope_names(binding: &ApprovalBinding) -> Result<Vec<String>, AgentFailure> {
    binding
        .requested_scopes
        .iter()
        .map(|scope| {
            serde_json::to_value(scope)
                .ok()
                .and_then(|value| value.as_str().map(str::to_owned))
                .ok_or_else(|| {
                    agent_failure(
                        "approval_scope_invalid",
                        "Tool effect scope could not be represented in Agent Protocol",
                        false,
                    )
                })
        })
        .collect()
}

struct SkillCallObservation {
    result: serde_json::Value,
    is_error: bool,
    context_message: Option<ModelMessage>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillActivateArguments {
    name: String,
    expected_digest: Digest,
    reason: String,
}

async fn execute_skill_activation(
    inner: &GenericInner,
    request: &AgentStartRequest,
    skills: &SkillRuntime,
    round: u64,
    call_id: &ModelToolCallId,
    arguments: serde_json::Value,
) -> Result<SkillCallObservation, AgentFailure> {
    let parsed = match serde_json::from_value::<SkillActivateArguments>(arguments) {
        Ok(parsed) => parsed,
        Err(error) => {
            return Ok(SkillCallObservation {
                result: serde_json::json!({
                    "code": "skill_activation_arguments_invalid",
                    "message": error.to_string(),
                }),
                is_error: true,
                context_message: None,
            })
        }
    };
    let records = inner
        .session_journal
        .load_session(&request.run.spec.session_id)
        .await
        .map_err(session_journal_failure)?;
    let active = ActivatedSkillSet::replay(&records)
        .map_err(|error| agent_failure("skill_session_state", error.to_string(), false))?;
    match skills.activate(
        SkillActivationRequest {
            name: parsed.name,
            expected_digest: parsed.expected_digest,
            reason: parsed.reason,
        },
        &active,
    ) {
        Ok(SkillActivationOutcome::Activated(activation)) => {
            append_session_event(
                inner,
                AgentSessionEventDraft {
                    event_id: AgentSessionEventId::new(format!(
                        "generic-{}-skill-{}-{}",
                        request.run.spec.run_id.as_str(),
                        round,
                        call_id.as_str()
                    )),
                    session_id: request.run.spec.session_id.clone(),
                    run_id: request.run.spec.run_id.clone(),
                    payload: AgentSessionEvent::SkillActivated {
                        activation: Box::new(activation.clone()),
                    },
                },
            )
            .await?;
            let descriptor = &activation.package.descriptor;
            Ok(SkillCallObservation {
                result: serde_json::json!({
                    "status": "activated",
                    "name": descriptor.name,
                    "skill_id": descriptor.skill_id,
                    "version": descriptor.version,
                    "digest": descriptor.digest,
                    "source": descriptor.source,
                    "trust": descriptor.trust,
                }),
                is_error: false,
                context_message: Some(crate::session_context::skill_activation_message(
                    &activation,
                )),
            })
        }
        Ok(SkillActivationOutcome::AlreadyActive(descriptor)) => Ok(SkillCallObservation {
            result: serde_json::json!({
                "status": "already_active",
                "name": descriptor.name,
                "skill_id": descriptor.skill_id,
                "digest": descriptor.digest,
            }),
            is_error: false,
            context_message: None,
        }),
        Err(error) => Ok(SkillCallObservation {
            result: serde_json::json!({
                "code": "skill_activation_rejected",
                "message": error.to_string(),
            }),
            is_error: true,
            context_message: None,
        }),
    }
}

struct PendingModelToolCall {
    call_id: ModelToolCallId,
    name: String,
    arguments: String,
    ended: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkflowToolArguments {
    plan: Plan,
}

struct GenericWorkflowProgressReporter {
    inner: Arc<GenericInner>,
    run_id: RunId,
    workflow_id: WorkflowId,
    total_steps: u64,
    completed_steps: AtomicU64,
    sequence: AtomicU64,
}

impl GenericWorkflowProgressReporter {
    fn new(
        inner: Arc<GenericInner>,
        run_id: RunId,
        workflow_id: WorkflowId,
        total_steps: usize,
    ) -> Self {
        Self {
            inner,
            run_id,
            workflow_id,
            total_steps: total_steps as u64,
            completed_steps: AtomicU64::new(0),
            sequence: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl ExecutionProgressReporter for GenericWorkflowProgressReporter {
    async fn report(&self, event: ExecutionProgressEvent) -> Result<(), String> {
        if event.workflow_id != self.workflow_id {
            return Err("workflow progress crossed an Agent Run task boundary".to_owned());
        }
        let completed = match event.phase.as_str() {
            "step_completed" => self.completed_steps.fetch_add(1, Ordering::AcqRel) + 1,
            "workflow_completed" => {
                self.completed_steps
                    .store(self.total_steps, Ordering::Release);
                self.total_steps
            }
            _ => self.completed_steps.load(Ordering::Acquire),
        };
        let fraction = (self.total_steps > 0)
            .then_some((completed.min(self.total_steps) as f64) / (self.total_steps as f64));
        let target = event
            .step_id
            .as_ref()
            .map(|step_id| format!(" [{}]", step_id.as_str()))
            .unwrap_or_default();
        let message = event
            .message
            .unwrap_or_else(|| format!("workflow {}{}", event.phase, target));
        let sequence = self.sequence.fetch_add(1, Ordering::AcqRel) + 1;
        publish_telemetry(
            &self.inner,
            &self.run_id,
            AgentTelemetryEnvelope {
                telemetry_id: TelemetryId::new(format!(
                    "generic-{}-workflow-progress-{sequence}",
                    self.run_id.as_str()
                )),
                run_id: self.run_id.clone(),
                provider_seq: None,
                payload: AgentTelemetry::ProgressReported { message, fraction },
            },
        );
        Ok(())
    }
}

struct WorkflowCallObservation {
    result: serde_json::Value,
    is_error: bool,
    tool_calls: u64,
}

enum WorkflowCallExecution {
    Observed(WorkflowCallObservation),
    Cancelled,
}

async fn execute_workflow_call(
    inner: Arc<GenericInner>,
    tools: &GenericTools,
    run_id: &RunId,
    call_id: &ModelToolCallId,
    arguments: serde_json::Value,
    remaining_tool_calls: u64,
    cancellation: CancellationToken,
) -> WorkflowCallExecution {
    let parsed = match serde_json::from_value::<WorkflowToolArguments>(arguments) {
        Ok(parsed) => parsed,
        Err(error) => {
            return WorkflowCallExecution::Observed(WorkflowCallObservation {
                result: workflow_error("invalid_workflow", error.to_string()),
                is_error: true,
                tool_calls: 0,
            })
        }
    };
    let Some(workflow) = tools.workflow.as_ref() else {
        return WorkflowCallExecution::Observed(WorkflowCallObservation {
            result: workflow_error(
                "workflow_unavailable",
                "Generic Agent has no configured Workflow execution strategy",
            ),
            is_error: true,
            tool_calls: 0,
        });
    };
    let workflow_id = WorkflowId::new(format!("workflow:{}:{}", run_id.as_str(), call_id.as_str()));
    let reporter = Arc::new(GenericWorkflowProgressReporter::new(
        inner,
        run_id.clone(),
        workflow_id.clone(),
        parsed.plan.steps.len(),
    ));
    let request = WorkflowExecutionRequest::new(
        run_id.clone(),
        workflow_id,
        parsed.plan,
        tools.run_grant.clone(),
    )
    .with_cancellation(cancellation.clone())
    .with_progress_reporter(reporter)
    .with_max_tool_calls(remaining_tool_calls);
    let snapshot = match workflow.execute(request).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return WorkflowCallExecution::Observed(WorkflowCallObservation {
                result: workflow_error("workflow_rejected", error.to_string()),
                is_error: true,
                tool_calls: 0,
            })
        }
    };
    if cancellation.is_cancelled() {
        return WorkflowCallExecution::Cancelled;
    }
    let tool_calls = snapshot.tool_calls;
    let (result, is_error) = snapshot.tool_result();
    WorkflowCallExecution::Observed(WorkflowCallObservation {
        result,
        is_error,
        tool_calls,
    })
}

fn workflow_error(code: &str, message: impl Into<String>) -> serde_json::Value {
    serde_json::json!({
        "status": "rejected",
        "code": code,
        "message": message.into(),
    })
}

fn publish_workflow_output(
    inner: &GenericInner,
    run_id: &RunId,
    round: u64,
    call_id: &ModelToolCallId,
    result: serde_json::Value,
) -> Option<AgentEventId> {
    let event_id = AgentEventId::new(format!(
        "generic-{}-workflow-{round}-{}",
        run_id.as_str(),
        call_id.as_str()
    ));
    publish_durable(
        inner,
        run_id,
        AgentEventDraft {
            event_id: event_id.clone(),
            run_id: run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::OutputCommitted {
                output_id: OutputId::new(format!(
                    "generic-{}-workflow-{round}-{}",
                    run_id.as_str(),
                    call_id.as_str()
                )),
                content: vec![Content {
                    media_type: "application/json".to_owned(),
                    schema_id: None,
                    body: ContentBody::Inline(result),
                }],
            },
        },
    )
    .then_some(event_id)
}

fn parse_tool_arguments(call: &PendingModelToolCall) -> Result<serde_json::Value, AgentFailure> {
    let raw = if call.arguments.trim().is_empty() {
        "{}"
    } else {
        call.arguments.as_str()
    };
    let arguments = serde_json::from_str::<serde_json::Value>(raw).map_err(|error| {
        agent_failure(
            "invalid_tool_arguments",
            format!(
                "model emitted invalid JSON arguments for {}: {error}",
                call.name
            ),
            false,
        )
    })?;
    if !arguments.is_object() {
        return Err(agent_failure(
            "invalid_tool_arguments",
            format!(
                "model Tool arguments for {} must be a JSON object",
                call.name
            ),
            false,
        ));
    }
    Ok(arguments)
}

fn model_tool_result(outcome: ToolOutcome) -> (serde_json::Value, bool) {
    match outcome {
        ToolOutcome::Completed {
            output: ToolOutput::Inline(output),
        } => (output, false),
        ToolOutcome::Completed {
            output: ToolOutput::Artifact(artifact),
        } => (
            serde_json::json!({
                "kind": "artifact",
                "artifact": artifact.artifact,
                "media_type": artifact.media_type,
                "byte_size": artifact.byte_size,
                "summary": artifact.summary,
            }),
            false,
        ),
        other => (
            serde_json::to_value(other).unwrap_or_else(|error| {
                serde_json::json!({
                    "status": "failed",
                    "code": "tool_result_serialization",
                    "message": error.to_string(),
                })
            }),
            true,
        ),
    }
}

fn merge_usage(total: &mut ModelUsage, observed: ModelUsage) {
    total.input_tokens = add_optional(total.input_tokens, observed.input_tokens);
    total.output_tokens = add_optional(total.output_tokens, observed.output_tokens);
}

fn add_optional(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.saturating_add(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn agent_input_message(request: &AgentStartRequest) -> Result<ModelMessage, AgentProtocolError> {
    agent_content_message(&request.run.spec.input)
}

fn agent_content_message(items: &[Content]) -> Result<ModelMessage, AgentProtocolError> {
    let mut content = Vec::with_capacity(items.len());
    for item in items {
        match (&item.media_type[..], &item.body) {
            ("text/plain", ContentBody::Inline(serde_json::Value::String(text)))
                if !text.is_empty() =>
            {
                content.push(ModelContent::Text { text: text.clone() });
            }
            _ => {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidSpec,
                    "text-first Generic Agent accepts inline text/plain input only",
                ));
            }
        }
    }
    Ok(ModelMessage {
        role: ModelRole::User,
        content,
    })
}

fn commit_loop_boundary(
    inner: &GenericInner,
    run_id: &RunId,
    next_model_round: u64,
    usage: &ModelUsage,
    tool_call_count: u64,
    last_response: &str,
    supporting_event_ids: &[AgentEventId],
) -> Result<(), AgentFailure> {
    append_checkpoint(
        inner,
        run_id,
        GenericCheckpointEventId::new(format!(
            "generic-{}-boundary-{next_model_round}",
            run_id.as_str()
        )),
        GenericCheckpointEvent::LoopBoundaryCommitted {
            next_model_round,
            usage: usage.clone(),
            tool_call_count,
            last_response: last_response.to_owned(),
            supporting_event_ids: supporting_event_ids.to_vec(),
        },
    )
}

fn commit_model_attempt(
    inner: &GenericInner,
    run_id: &RunId,
    round: u64,
    request: &ModelRequest,
) -> Result<(), AgentFailure> {
    append_checkpoint(
        inner,
        run_id,
        GenericCheckpointEventId::new(format!("generic-{}-model-attempt-{round}", run_id.as_str())),
        GenericCheckpointEvent::ModelAttemptStarted {
            round,
            request_id: request.request_id.clone(),
            request_digest: model_request_digest(request)?,
        },
    )
}

fn model_request_for_round(
    request: &AgentStartRequest,
    round: u64,
    messages: &[ModelMessage],
    tools: &[ModelToolDefinition],
) -> ModelRequest {
    ModelRequest {
        request_id: ModelRequestId::new(format!(
            "model-{}-{round}",
            request.run.spec.run_id.as_str()
        )),
        messages: messages.to_vec(),
        tools: tools.to_vec(),
        output_schema: None,
        max_output_tokens: request.run.spec.limits.max_output_tokens,
        extensions: Default::default(),
    }
}

fn model_request_digest(request: &ModelRequest) -> Result<Digest, AgentFailure> {
    serde_jcs::to_vec(request)
        .map(Digest::sha256)
        .map_err(|error| {
            agent_failure(
                "generic_checkpoint",
                format!("could not digest model request: {error}"),
                true,
            )
        })
}

fn commit_model_observation(
    inner: &GenericInner,
    run_id: &RunId,
    round: u64,
    request_id: &ModelRequestId,
    observation: GenericModelObservation,
) -> Result<(), AgentFailure> {
    append_checkpoint(
        inner,
        run_id,
        GenericCheckpointEventId::new(format!(
            "generic-{}-model-observed-{round}",
            run_id.as_str()
        )),
        GenericCheckpointEvent::ModelAttemptObserved {
            round,
            request_id: request_id.clone(),
            observation,
        },
    )
}

fn append_checkpoint(
    inner: &GenericInner,
    run_id: &RunId,
    event_id: GenericCheckpointEventId,
    payload: GenericCheckpointEvent,
) -> Result<(), AgentFailure> {
    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let run = state.runs.get_mut(run_id).ok_or_else(|| {
        agent_failure(
            "generic_checkpoint",
            "Run disappeared before its private checkpoint was committed",
            true,
        )
    })?;
    append_checkpoint_to_run(inner, run, run_id, event_id, payload)
}

fn append_checkpoint_to_run(
    inner: &GenericInner,
    run: &mut GenericRun,
    run_id: &RunId,
    event_id: GenericCheckpointEventId,
    payload: GenericCheckpointEvent,
) -> Result<(), AgentFailure> {
    let expected_previous = run.checkpoint_seq;
    inner
        .checkpoint_store
        .append(
            run_id,
            expected_previous,
            GenericCheckpointDraft {
                event_id,
                run_id: run_id.clone(),
                payload,
            },
        )
        .map_err(checkpoint_failure)?;
    run.checkpoint_seq = expected_previous.saturating_add(1);
    Ok(())
}

fn checkpoint_provider_events(
    inner: &GenericInner,
    run: &mut GenericRun,
    run_id: &RunId,
    events: &[AgentEventDraft],
) -> Result<(), AgentFailure> {
    let first = events
        .first()
        .expect("Provider checkpoint event batch is never empty");
    let last = events
        .last()
        .expect("Provider checkpoint event batch is never empty");
    append_checkpoint_to_run(
        inner,
        run,
        run_id,
        GenericCheckpointEventId::new(format!(
            "generic-{}-provider-{}-{}",
            run_id.as_str(),
            first.event_id.as_str(),
            last.event_id.as_str()
        )),
        GenericCheckpointEvent::ProviderEventsCommitted {
            events: events.to_vec(),
        },
    )
}

fn take_queued_steers(inner: &GenericInner, run_id: &RunId) -> Vec<QueuedSteer> {
    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state
        .runs
        .get_mut(run_id)
        .map(|run| run.queued_steers.drain(..).collect())
        .unwrap_or_default()
}

async fn commit_queued_steers(
    inner: &GenericInner,
    request: &AgentStartRequest,
    model_messages: &mut Vec<ModelMessage>,
) -> Result<usize, AgentFailure> {
    let run_id = &request.run.spec.run_id;
    let queued = take_queued_steers(inner, run_id);
    let count = queued.len();
    for steer in queued {
        append_session_event(
            inner,
            AgentSessionEventDraft {
                event_id: AgentSessionEventId::new(format!(
                    "generic-{}-steer-{}",
                    run_id.as_str(),
                    steer.command_id.as_str()
                )),
                session_id: request.run.spec.session_id.clone(),
                run_id: run_id.clone(),
                payload: AgentSessionEvent::RunInputCommitted {
                    message: steer.message.clone(),
                },
            },
        )
        .await?;
        if !publish_durable(
            inner,
            run_id,
            AgentEventDraft {
                event_id: AgentEventId::new(format!(
                    "generic-{}-steer-{}-committed",
                    run_id.as_str(),
                    steer.command_id.as_str()
                )),
                run_id: run_id.clone(),
                causation_id: Some(steer.command_id),
                source_fingerprint: None,
                payload: AgentEvent::InputCommitted {
                    content: steer.content,
                },
            },
        ) {
            return Err(agent_failure(
                "generic_checkpoint",
                "steer input could not be committed to the private WAL",
                true,
            ));
        }
        model_messages.push(steer.message);
    }
    Ok(count)
}

fn publish_durable(inner: &GenericInner, run_id: &RunId, draft: AgentEventDraft) -> bool {
    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(run) = state.runs.get_mut(run_id) else {
        return false;
    };
    if run.terminal {
        return false;
    }
    if let Err(failure) = checkpoint_provider_events(inner, run, run_id, &[draft.clone()]) {
        poison_run_after_checkpoint_failure(run, failure);
        return false;
    }
    let terminal = matches!(
        draft.payload,
        AgentEvent::DeliveryCommitted { .. }
            | AgentEvent::RunIncomplete { .. }
            | AgentEvent::RunFailed { .. }
            | AgentEvent::RunCancelled { .. }
    );
    run.durable_events.push(draft.clone());
    run.terminal = terminal;
    let _ = run
        .sender
        .send(Ok(AgentProviderStreamItem::Event(Box::new(draft))));
    true
}

fn publish_telemetry(inner: &GenericInner, run_id: &RunId, telemetry: AgentTelemetryEnvelope) {
    let state = inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(run) = state.runs.get(run_id) {
        if !run.terminal {
            let _ = run
                .sender
                .send(Ok(AgentProviderStreamItem::Telemetry(telemetry)));
        }
    }
}

enum DeliveryCommit {
    Committed,
    SteerPending,
    CancelPending,
    CheckpointFailed,
    AlreadyTerminal,
}

fn try_emit_delivery(
    inner: &GenericInner,
    request: &AgentStartRequest,
    response: String,
    usage: Option<ModelUsage>,
    tool_calls: u64,
    mut supporting_event_ids: Vec<AgentEventId>,
) -> DeliveryCommit {
    let run_id = &request.run.spec.run_id;
    let output_event_id = AgentEventId::new(format!("generic-{}-output", run_id.as_str()));
    let output = AgentEventDraft {
        event_id: output_event_id.clone(),
        run_id: run_id.clone(),
        causation_id: None,
        source_fingerprint: None,
        payload: AgentEvent::OutputCommitted {
            output_id: OutputId::new(format!("generic-{}-response", run_id.as_str())),
            content: vec![Content::text(response.clone())],
        },
    };
    supporting_event_ids.push(output_event_id);
    let delivery = AgentEventDraft {
        event_id: AgentEventId::new(format!("generic-{}-delivered", run_id.as_str())),
        run_id: run_id.clone(),
        causation_id: None,
        source_fingerprint: None,
        payload: AgentEvent::DeliveryCommitted {
            delivery: AgentDelivery {
                delivery_id: DeliveryId::new(format!("generic-{}-delivery", run_id.as_str())),
                run_id: run_id.clone(),
                spec_digest: request.run.spec_digest.clone(),
                final_response: Content::text(response),
                outputs: Vec::new(),
                artifacts: Vec::new(),
                unresolved_issues: Vec::new(),
                usage: agent_usage(usage, tool_calls),
                provenance: provenance(inner, supporting_event_ids),
            },
        },
    };

    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(run) = state.runs.get_mut(run_id) else {
        return DeliveryCommit::AlreadyTerminal;
    };
    if run.terminal {
        return DeliveryCommit::AlreadyTerminal;
    }
    if run.cancel_command.is_some() {
        return DeliveryCommit::CancelPending;
    }
    if !run.queued_steers.is_empty() {
        return DeliveryCommit::SteerPending;
    }
    if let Err(failure) =
        checkpoint_provider_events(inner, run, run_id, &[output.clone(), delivery.clone()])
    {
        poison_run_after_checkpoint_failure(run, failure);
        return DeliveryCommit::CheckpointFailed;
    }
    run.durable_events.push(output.clone());
    run.durable_events.push(delivery.clone());
    run.terminal = true;
    let _ = run
        .sender
        .send(Ok(AgentProviderStreamItem::Event(Box::new(output))));
    let _ = run
        .sender
        .send(Ok(AgentProviderStreamItem::Event(Box::new(delivery))));
    DeliveryCommit::Committed
}

fn emit_incomplete(
    inner: &GenericInner,
    request: &AgentStartRequest,
    _user_message: &ModelMessage,
    response: String,
    usage: Option<ModelUsage>,
    tool_calls: u64,
    started_event_id: AgentEventId,
    limit: RunLimitKind,
    unresolved_issue: &str,
) {
    let run_id = &request.run.spec.run_id;
    let partial_delivery = (!response.is_empty()).then(|| PartialDelivery {
        partial_delivery_id: PartialDeliveryId::new(format!("generic-{}-partial", run_id.as_str())),
        run_id: run_id.clone(),
        spec_digest: request.run.spec_digest.clone(),
        response: Some(Content::text(response)),
        outputs: Vec::new(),
        artifacts: Vec::new(),
        unresolved_issues: vec![unresolved_issue.to_owned()],
        usage: agent_usage(usage, tool_calls),
        provenance: provenance(inner, vec![started_event_id]),
    });
    if publish_durable(
        inner,
        run_id,
        AgentEventDraft {
            event_id: AgentEventId::new(format!("generic-{}-incomplete", run_id.as_str())),
            run_id: run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RunIncomplete {
                reason: IncompleteReason::LimitReached { limit },
                partial_delivery,
            },
        },
    ) {
        finish_session(inner, request);
    }
}

fn emit_failure(
    inner: &GenericInner,
    request: &AgentStartRequest,
    _user_message: &ModelMessage,
    failure: AgentFailure,
) {
    let run_id = &request.run.spec.run_id;
    if publish_durable(
        inner,
        run_id,
        AgentEventDraft {
            event_id: AgentEventId::new(format!("generic-{}-failed", run_id.as_str())),
            run_id: run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RunFailed { failure },
        },
    ) {
        finish_session(inner, request);
    }
}

fn emit_cancel(inner: &GenericInner, request: &AgentStartRequest, user_message: &ModelMessage) {
    let run_id = &request.run.spec.run_id;
    let cancellation = {
        let state = inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .runs
            .get(run_id)
            .and_then(|run| run.cancel_command.clone())
    };
    if let Some((command_id, reason)) = cancellation {
        if !publish_durable(
            inner,
            run_id,
            AgentEventDraft {
                event_id: AgentEventId::new(format!("generic-{}-stop", run_id.as_str())),
                run_id: run_id.clone(),
                causation_id: Some(command_id),
                source_fingerprint: None,
                payload: AgentEvent::StopRequested {
                    reason: reason.clone(),
                },
            },
        ) {
            return;
        }
        if !publish_durable(
            inner,
            run_id,
            AgentEventDraft {
                event_id: AgentEventId::new(format!("generic-{}-cancelled", run_id.as_str())),
                run_id: run_id.clone(),
                causation_id: None,
                source_fingerprint: None,
                payload: AgentEvent::RunCancelled { reason },
            },
        ) {
            return;
        }
    } else {
        emit_failure(
            inner,
            request,
            user_message,
            AgentFailure {
                code: "unexpected_model_cancellation".to_owned(),
                message: "model request cancelled without an Agent Cancel command".to_owned(),
                retryable: true,
                details: serde_json::Value::Null,
            },
        );
        return;
    }
    finish_session(inner, request);
}

fn finish_session(inner: &GenericInner, request: &AgentStartRequest) {
    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let session = state
        .sessions
        .entry(request.run.spec.session_id.clone())
        .or_default();
    if session.active_run.as_ref() == Some(&request.run.spec.run_id) {
        session.active_run = None;
    }
}

fn provenance(inner: &GenericInner, supporting_event_ids: Vec<AgentEventId>) -> Provenance {
    Provenance {
        provider_id: inner.config.provider_id.clone(),
        agent_id: inner.config.agent_id.clone(),
        supporting_event_ids,
        extensions: Default::default(),
    }
}

fn agent_usage(usage: Option<ModelUsage>, tool_calls: u64) -> Option<UsageReport> {
    if usage.is_none() && tool_calls == 0 {
        return None;
    }
    let usage = usage.unwrap_or_default();
    Some(UsageReport {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        tool_calls: (tool_calls > 0).then_some(tool_calls),
        cost: None,
    })
}

fn agent_failure(
    code: impl Into<String>,
    message: impl Into<String>,
    retryable: bool,
) -> AgentFailure {
    AgentFailure {
        code: code.into(),
        message: message.into(),
        retryable,
        details: serde_json::Value::Null,
    }
}

fn model_event_failure(message: impl Into<String>) -> AgentFailure {
    agent_failure("model_protocol", message, false)
}

fn resolve_run_skill_binding(
    configured: Option<&Arc<SkillRuntime>>,
    request: &AgentStartRequest,
    skipped: &mut Vec<ResourceBindingSkip>,
) -> Result<Option<Arc<SkillRuntime>>, AgentStartError> {
    let Some(skills) = configured else {
        return Ok(None);
    };
    let skipped_ids = skipped
        .iter()
        .map(|skip| skip.binding_id.clone())
        .collect::<BTreeSet<_>>();
    let mut resolved = None;
    for binding in request.run.spec.resources.iter().filter(|binding| {
        binding.resource.kind.as_str()
            == orchestral_core::skill_protocol::SKILL_CATALOG_RESOURCE_KIND_V1
            && !skipped_ids.contains(&binding.binding_id)
    }) {
        let matches = binding.resource.id == skills.catalog().resource_id
            && binding.resource.revision.as_str() == skills.catalog().revision.as_str();
        if matches {
            resolved = Some(skills.clone());
            continue;
        }
        let reason = format!(
            "Skill catalog binding does not match Host snapshot id={} revision={}",
            skills.catalog().resource_id,
            skills.catalog().revision
        );
        if binding.requirement == BindingRequirement::Required {
            return Err(InternalGenericAgentProvider::rejection(
                AgentRejectionCode::UnsupportedResource,
                reason,
            ));
        }
        skipped.push(ResourceBindingSkip {
            binding_id: binding.binding_id.clone(),
            code: ResourceBindingSkipCode::ResolutionFailed,
            reason,
        });
    }
    Ok(resolved)
}

fn model_definitions_for_run(
    inner: &GenericInner,
    skill_catalog_bound: bool,
) -> Vec<ModelToolDefinition> {
    let mut definitions = inner
        .tools
        .as_ref()
        .map(|tools| tools.model_definitions.clone())
        .unwrap_or_default();
    if inner.backend.descriptor().capabilities.tool_calls {
        definitions.push(request_input_definition());
    }
    if skill_catalog_bound {
        definitions.push(skill_activate_definition());
    }
    definitions
}

fn request_input_definition() -> ModelToolDefinition {
    ModelToolDefinition {
        name: REQUEST_INPUT_TOOL_NAME.to_owned(),
        description: "Ask the user for information that is required before the current Run can continue. Use only when the answer cannot be derived from available context or Tools.".to_owned(),
        input_schema: serde_json::json!({
            "type": "object",
            "required": ["prompt"],
            "properties": {
                "prompt": {
                    "type": "string",
                    "minLength": 1,
                    "description": "A concise question for the user"
                }
            },
            "additionalProperties": false
        }),
    }
}

fn system_message_for_run(
    config: &GenericAgentConfig,
    skills: Option<&SkillRuntime>,
) -> Option<ModelMessage> {
    let mut sections = Vec::new();
    if !config.system_prompt.trim().is_empty() {
        sections.push(config.system_prompt.clone());
    }
    if let Some(skills) = skills {
        sections.push(skills.descriptor_context());
    }
    (!sections.is_empty()).then(|| ModelMessage::text(ModelRole::System, sections.join("\n\n")))
}

fn configure_tools(
    runtime: Arc<dyn AgentToolRuntime>,
    run_grant: RunToolGrant,
    workflow: Option<Arc<WorkflowExecutionStrategy>>,
    approval_bridge: Option<Arc<dyn AgentApprovalBridge>>,
) -> Result<GenericTools, AgentProtocolError> {
    run_grant.bounds.validate().map_err(|error| {
        AgentProtocolError::new(AgentProtocolErrorCode::InvalidSpec, error.message)
    })?;
    let runtime_contract_digest = runtime
        .execution_contract_digest()
        .map_err(tool_runtime_error)?;
    let mut model_definitions = runtime
        .model_tool_schemas()
        .map_err(tool_runtime_error)?
        .into_iter()
        .map(|schema| ModelToolDefinition {
            name: schema.name,
            description: schema.description,
            input_schema: schema.input_schema,
        })
        .collect::<Vec<_>>();
    if model_definitions.is_empty() {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidSpec,
            "tool-enabled Generic Agent requires at least one registered Tool",
        ));
    }
    if workflow.is_some() {
        if model_definitions
            .iter()
            .any(|definition| definition.name == WORKFLOW_TOOL_NAME)
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                format!(
                    "reserved Generic Agent Tool name is already registered: {WORKFLOW_TOOL_NAME}"
                ),
            ));
        }
        model_definitions.push(workflow_tool_definition());
    }
    Ok(GenericTools {
        runtime,
        runtime_contract_digest,
        run_grant,
        model_definitions,
        workflow,
        approval_bridge,
    })
}

fn skill_activate_definition() -> ModelToolDefinition {
    ModelToolDefinition {
        name: SKILL_ACTIVATE_TOOL_NAME.to_owned(),
        description: "Activate one immutable Skill descriptor for this Session. This loads instructions into context only; it does not grant Tool or MCP permissions.".to_owned(),
        input_schema: serde_json::json!({
            "type": "object",
            "required": ["name", "expected_digest", "reason"],
            "properties": {
                "name": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Exact descriptor name from the bound Skill catalog"
                },
                "expected_digest": {
                    "type": "string",
                    "pattern": "^[0-9a-fA-F]{64}$",
                    "description": "Exact immutable digest shown in the descriptor"
                },
                "reason": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Why this Skill is relevant to the current user task"
                }
            },
            "additionalProperties": false
        }),
    }
}

fn workflow_tool_definition() -> ModelToolDefinition {
    ModelToolDefinition {
        name: WORKFLOW_TOOL_NAME.to_owned(),
        description: "Execute a dependency-aware workflow for a complex task. Prefer a direct answer or one ordinary Tool for simple work.".to_owned(),
        input_schema: serde_json::json!({
            "type": "object",
            "required": ["plan"],
            "properties": {
                "plan": {
                    "type": "object",
                    "required": ["goal", "steps"],
                    "properties": {
                        "goal": { "type": "string", "minLength": 1 },
                        "steps": {
                            "type": "array",
                            "minItems": 1,
                            "items": {
                                "type": "object",
                                "required": ["id", "action"],
                                "properties": {
                                    "id": { "type": "string", "minLength": 1 },
                                    "action": { "type": "string", "minLength": 1 },
                                    "kind": { "type": "string", "enum": ["action", "system"] },
                                    "depends_on": { "type": "array", "items": { "type": "string" } },
                                    "exports": { "type": "array", "items": { "type": "string" } },
                                    "io_bindings": { "type": "array" },
                                    "params": {}
                                },
                                "additionalProperties": false
                            }
                        },
                        "confidence": { "type": ["number", "null"], "minimum": 0, "maximum": 1 },
                        "on_complete": { "type": ["string", "null"] },
                        "on_failure": { "type": ["string", "null"] }
                    },
                    "additionalProperties": false
                }
            },
            "additionalProperties": false
        }),
    }
}

fn generic_config_digest(
    config: &GenericAgentConfig,
    model_descriptor: &orchestral_core::model_protocol::ModelDescriptor,
    tools: Option<&GenericTools>,
    skills: Option<&orchestral_core::skill_protocol::SkillCatalogDescriptor>,
    approval_enabled: bool,
    input_requests_enabled: bool,
) -> Result<Digest, AgentProtocolError> {
    let tool_contract = tools.map(|tools| {
        serde_json::json!({
            "runtime_contract_digest": &tools.runtime_contract_digest,
            "run_grant": &tools.run_grant,
            "model_definitions": &tools.model_definitions,
            "workflow": tools
                .workflow
                .as_ref()
                .map(|workflow| workflow.recovery_contract()),
        })
    });
    let value = serde_json::json!({
        "provider_id": config.provider_id,
        "agent_id": config.agent_id,
        "system_prompt": config.system_prompt,
        "model_descriptor": model_descriptor,
        "max_model_rounds": config.max_model_rounds,
        "max_tool_calls": config.max_tool_calls,
        "max_context_tokens": config.max_context_tokens,
        "reserved_output_tokens": config.reserved_output_tokens,
        "tool_contract": tool_contract,
        "skill_catalog": skills,
        "approval_enabled": approval_enabled,
        "input_requests_enabled": input_requests_enabled,
        "steer_enabled": true,
    });
    let bytes = serde_jcs::to_vec(&value).map_err(|error| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidSpec,
            format!("could not digest Generic Agent configuration: {error}"),
        )
    })?;
    Ok(Digest::sha256(bytes))
}

async fn append_session_event(
    inner: &GenericInner,
    draft: AgentSessionEventDraft,
) -> Result<(), AgentFailure> {
    inner
        .session_journal
        .append(draft)
        .await
        .map(|_| ())
        .map_err(session_journal_failure)
}

fn session_journal_failure(error: AgentSessionError) -> AgentFailure {
    agent_failure("session_journal", error.to_string(), true)
}

fn checkpoint_failure(error: GenericCheckpointError) -> AgentFailure {
    agent_failure("generic_checkpoint", error.to_string(), true)
}

fn checkpoint_stream_error(failure: AgentFailure) -> AgentProtocolError {
    AgentProtocolError::new(AgentProtocolErrorCode::ProviderUnavailable, failure.message)
        .with_retryable(failure.retryable)
        .with_details(failure.details)
}

fn poison_run_after_checkpoint_failure(
    run: &mut GenericRun,
    failure: AgentFailure,
) -> AgentProtocolError {
    let error = checkpoint_stream_error(failure);
    run.terminal = true;
    run.cancellation.cancel();
    let _ = run.sender.send(Err(error.clone()));
    error
}

fn checkpoint_start_error(error: GenericCheckpointError) -> AgentStartError {
    AgentStartError::OutcomeUnknown(
        AgentProtocolError::new(
            AgentProtocolErrorCode::ProviderUnavailable,
            error.to_string(),
        )
        .with_retryable(true),
    )
}

fn checkpoint_recovery_error(error: GenericCheckpointError) -> AgentProtocolError {
    match error {
        GenericCheckpointError::Unavailable(message) => AgentProtocolError::new(
            AgentProtocolErrorCode::ProviderUnavailable,
            format!("Generic Agent checkpoint storage is unavailable: {message}"),
        )
        .with_retryable(true),
        GenericCheckpointError::RunNotFound(run_id) => AgentProtocolError::new(
            AgentProtocolErrorCode::RunNotFound,
            format!("Generic Agent checkpoint Run does not exist: {run_id}"),
        ),
        other => AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            format!("Generic Agent private WAL cannot be trusted for recovery: {other}"),
        ),
    }
}

fn observed_recovery_error(failure: AgentFailure) -> AgentProtocolError {
    AgentProtocolError::new(
        AgentProtocolErrorCode::InvalidDigest,
        format!(
            "observed Generic Agent continuation is invalid: {}",
            failure.message
        ),
    )
    .with_details(failure.details)
}

fn recovery_start_error(error: AgentStartError) -> AgentProtocolError {
    match error {
        AgentStartError::Rejected(rejection) => {
            AgentProtocolError::new(AgentProtocolErrorCode::Unsupported, rejection.message)
                .with_retryable(rejection.retryable)
                .with_details(rejection.details)
        }
        AgentStartError::OutcomeUnknown(error) => error,
        _ => AgentProtocolError::new(
            AgentProtocolErrorCode::Internal,
            "Generic Agent recovery encountered an unknown start error",
        ),
    }
}

fn session_context_recovery_error(error: SessionContextError) -> AgentProtocolError {
    let failure = session_failure(error);
    let code = if failure.retryable {
        AgentProtocolErrorCode::ProviderUnavailable
    } else {
        AgentProtocolErrorCode::InvalidSpec
    };
    AgentProtocolError::new(code, failure.message)
        .with_retryable(failure.retryable)
        .with_details(failure.details)
}

fn session_failure(error: SessionContextError) -> AgentFailure {
    match error {
        SessionContextError::ContextOverflow { used, budget } => AgentFailure {
            code: "context_overflow".to_owned(),
            message: format!("pinned model context uses {used} tokens but budget is {budget}"),
            retryable: false,
            details: serde_json::json!({ "used": used, "budget": budget }),
        },
        other => agent_failure("session_context", other.to_string(), true),
    }
}

fn fail_before_model(
    inner: Arc<GenericInner>,
    request: &AgentStartRequest,
    user_message: &ModelMessage,
    failure: AgentFailure,
) {
    let run_id = &request.run.spec.run_id;
    let started_event_id = AgentEventId::new(format!("generic-{}-started", run_id.as_str()));
    if !publish_durable(
        &inner,
        run_id,
        AgentEventDraft {
            event_id: started_event_id.clone(),
            run_id: run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RunStarted,
        },
    ) {
        return;
    }
    emit_failure(&inner, request, user_message, failure);
}

fn model_protocol_error(error: ModelError) -> AgentProtocolError {
    let code = match error.code {
        ModelErrorCode::InvalidRequest => AgentProtocolErrorCode::InvalidSpec,
        ModelErrorCode::Unsupported => AgentProtocolErrorCode::Unsupported,
        ModelErrorCode::Protocol => AgentProtocolErrorCode::InvalidTransition,
        ModelErrorCode::Unavailable
        | ModelErrorCode::RateLimited
        | ModelErrorCode::Authentication
        | ModelErrorCode::Cancelled
        | ModelErrorCode::Internal => AgentProtocolErrorCode::ProviderUnavailable,
        _ => AgentProtocolErrorCode::ProviderUnavailable,
    };
    AgentProtocolError::new(code, error.message)
        .with_retryable(error.retryable)
        .with_details(error.details)
}

fn tool_runtime_error(error: ToolRuntimeError) -> AgentProtocolError {
    AgentProtocolError::new(
        AgentProtocolErrorCode::ProviderUnavailable,
        error.to_string(),
    )
}

fn model_failure(error: ModelError) -> AgentFailure {
    AgentFailure {
        code: format!("model_{:?}", error.code).to_lowercase(),
        message: error.message,
        retryable: error.retryable,
        details: error.details,
    }
}
