//! Provider-neutral Generic Agent implementation.
//!
//! Tools are optional and can only enter through the Host-owned guarded Tool
//! runtime. A model tool call never carries authority by itself.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
        ArtifactRefWithDigest, BindingRequirement, CancelSupport, CommandId, Content, ContentBody,
        ControlCapabilities, DeliveryId, Digest, EffectMediation, IncompleteReason, MoneyAmount,
        OutputId, PartialDelivery, PartialDeliveryId, PendingRequest, PendingRequestKind,
        PendingRequestPayload, Provenance, ProviderCommandDisposition, ProviderCommandOutcome,
        RequestId, RequestResolution, ResourceBindingMode, ResourceBindingSkip,
        ResourceBindingSkipCode, ResourceCapability, ResourceKind, RunId, RunLimitKind,
        TelemetryId, UsageReport,
    },
    AGENT_PROTOCOL_V1,
};
use orchestral_core::agent_session::{
    AgentSessionError, AgentSessionEvent, AgentSessionEventDraft, AgentSessionEventId,
    AgentSessionJournalStore, AgentSessionRecord, InMemoryAgentSessionJournalStore,
};
use orchestral_core::executor::{ExecutionProgressEvent, ExecutionProgressReporter};
use orchestral_core::model_protocol::{
    ModelBackend, ModelContent, ModelError, ModelErrorCode, ModelEvent, ModelFinishReason,
    ModelMessage, ModelRequest, ModelRequestId, ModelRole, ModelToolCallId, ModelToolDefinition,
    ModelUsage,
};
use orchestral_core::skill_protocol::SkillActivation;
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
    AppendGenericCheckpointOutcome, CreateGenericRunOutcome, GenericAgentCheckpointStore,
    GenericAgentRunRegistration, GenericCheckpointDraft, GenericCheckpointError,
    GenericCheckpointEvent, GenericCheckpointEventId, GenericCheckpointPhase, GenericLoopBoundary,
    GenericModelContextTrace, GenericModelObservation, GenericObservedToolCall,
    InMemoryGenericAgentCheckpointStore, StoredGenericAgentRun,
};
use crate::skill::{
    ActivatedSkillSet, SkillActivationOutcome, SkillActivationRequest, SkillRuntime,
};
use crate::tool_runtime::{AgentToolRuntime, GuardedToolResult, ToolRuntimeError};
use crate::workflow_strategy::{WorkflowExecutionRequest, WorkflowExecutionStrategy};
use crate::{
    AgentSessionCompactor, AgentSessionContextEngine, AgentSessionSummarizer, JsonSizeTokenMeter,
    ModelTokenMeter, ModelTokenMeterDescriptor, SessionCompactionPolicy, SessionContextError,
    SessionContextProjection, SessionContextRequest, SessionSummarizerDescriptor,
};

const WORKFLOW_TOOL_NAME: &str = "orchestral_workflow";
const SKILL_ACTIVATE_TOOL_NAME: &str = "orchestral_skill_activate";
const REQUEST_INPUT_TOOL_NAME: &str = "orchestral_request_input";
const TOOL_ACTIVITY_TELEMETRY_NAMESPACE: &str = "orchestral/tool_activity/v1";
const RUN_STOP_RUNNING: u8 = 0;
const RUN_STOP_HOST_CANCEL: u8 = 1;
const RUN_STOP_DEADLINE: u8 = 2;
const RUN_STOP_COMPLETING: u8 = 3;
const TOKENS_PER_MILLION: u128 = 1_000_000;

/// Host-owned, provider-neutral token pricing used to enforce a Run cost
/// ceiling before a model request is dispatched. Providers with cached,
/// tiered, or otherwise non-linear pricing must leave this unset until an
/// equivalent conservative policy is available.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelCostPolicy {
    pub currency: String,
    pub input_microunits_per_million_tokens: u64,
    pub output_microunits_per_million_tokens: u64,
}

impl ModelCostPolicy {
    pub fn new(
        currency: impl Into<String>,
        input_microunits_per_million_tokens: u64,
        output_microunits_per_million_tokens: u64,
    ) -> Result<Self, AgentProtocolError> {
        let policy = Self {
            currency: currency.into(),
            input_microunits_per_million_tokens,
            output_microunits_per_million_tokens,
        };
        policy.validate()?;
        Ok(policy)
    }

    fn validate(&self) -> Result<(), AgentProtocolError> {
        if self.currency.len() != 3
            || !self.currency.bytes().all(|byte| byte.is_ascii_uppercase())
            || (self.input_microunits_per_million_tokens == 0
                && self.output_microunits_per_million_tokens == 0)
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "model cost policy requires an uppercase currency and at least one positive rate",
            ));
        }
        Ok(())
    }

    pub fn quote(&self, input_tokens: u64, output_tokens: u64) -> MoneyAmount {
        let input = u128::from(input_tokens)
            .saturating_mul(u128::from(self.input_microunits_per_million_tokens));
        let output = u128::from(output_tokens)
            .saturating_mul(u128::from(self.output_microunits_per_million_tokens));
        let microunits = input
            .saturating_add(output)
            .div_ceil(TOKENS_PER_MILLION)
            .min(u128::from(u64::MAX)) as u64;
        MoneyAmount {
            currency: self.currency.clone(),
            microunits,
        }
    }

    fn max_output_tokens_within(
        &self,
        input_tokens: u64,
        output_tokens: u64,
        ceiling: &MoneyAmount,
    ) -> Option<u64> {
        if ceiling.currency != self.currency
            || self.quote(input_tokens, 0).microunits > ceiling.microunits
        {
            return None;
        }
        if self.output_microunits_per_million_tokens == 0 {
            return Some(output_tokens);
        }
        let mut low = 0_u64;
        let mut high = output_tokens;
        while low < high {
            let candidate = low.saturating_add(high).saturating_add(1) / 2;
            if self.quote(input_tokens, candidate).microunits <= ceiling.microunits {
                low = candidate;
            } else {
                high = candidate.saturating_sub(1);
            }
        }
        Some(low)
    }
}

#[derive(Debug, Clone)]
pub struct GenericAgentConfig {
    pub provider_id: AgentProviderId,
    pub agent_id: AgentId,
    pub system_prompt: String,
    pub stream_buffer: usize,
    pub max_model_rounds: u64,
    pub max_tool_calls: u64,
    pub history_limit: usize,
    pub max_context_tokens: u64,
    pub reserved_output_tokens: u64,
    pub model_cost_policy: Option<ModelCostPolicy>,
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
            history_limit: 128,
            max_context_tokens: 128 * 1024,
            reserved_output_tokens: 4 * 1024,
            model_cost_policy: None,
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
    session_compactor: Option<Arc<AgentSessionCompactor>>,
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
    stop_cause: Arc<AtomicU8>,
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

#[derive(Clone)]
struct ApprovalResponse {
    command_id: CommandId,
    resolution: RequestResolution,
    capability: Option<ApprovalCapability>,
}

struct RecoveredResolution {
    command_id: CommandId,
    resolution: RequestResolution,
    capability: Option<ApprovalCapability>,
}

struct RecoveredApprovalWaiter {
    request_id: RequestId,
    binding: ApprovalBinding,
    replayed_outcome: Option<ToolOutcome>,
    responder: Option<oneshot::Sender<ApprovalResponse>>,
    response: Option<oneshot::Receiver<ApprovalResponse>>,
    bridge: Arc<dyn AgentApprovalBridge>,
}

struct StoredCommand {
    digest: Digest,
    outcome: ProviderCommandOutcome,
}

struct GenericExecutionSeed {
    run_started: bool,
    next_model_round: u64,
    total_usage: ModelUsage,
    tool_call_count: u64,
    last_response: String,
    supporting_event_ids: Vec<AgentEventId>,
}

// Recovery state is created once per Run and retained behind the provider's
// Run allocation; keeping the variants explicit is safer than obscuring their
// durable-boundary fields behind unrelated heap payload types.
#[allow(clippy::large_enum_variant)]
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
    Approval {
        round: u64,
        request_id: ModelRequestId,
        request_digest: Digest,
        observation: GenericModelObservation,
        call: GenericObservedToolCall,
        arguments: serde_json::Value,
        request: PendingRequest,
        binding: Option<ApprovalBinding>,
        committed_response: Option<ApprovalResponse>,
        resolved_response: Option<ApprovalResponse>,
        response: Option<oneshot::Receiver<ApprovalResponse>>,
    },
    Skill {
        round: u64,
        request_id: ModelRequestId,
        request_digest: Digest,
        observation: GenericModelObservation,
        call: GenericObservedToolCall,
        arguments: serde_json::Value,
        recovered_observation: Option<SkillCallObservation>,
    },
    Workflow {
        round: u64,
        request_id: ModelRequestId,
        request_digest: Digest,
        observation: GenericModelObservation,
        call: GenericObservedToolCall,
        arguments: serde_json::Value,
        recovery_replay: bool,
    },
    WorkflowOutput {
        round: u64,
        request_id: ModelRequestId,
        request_digest: Digest,
        observation: GenericModelObservation,
        call: GenericObservedToolCall,
        arguments: serde_json::Value,
        outcome: WorkflowCallObservation,
        workflow_event_id: AgentEventId,
    },
    Tool {
        round: u64,
        request_id: ModelRequestId,
        request_digest: Digest,
        observation: GenericModelObservation,
        call: GenericObservedToolCall,
        arguments: serde_json::Value,
    },
}

impl GenericExecutionSeed {
    fn fresh() -> Self {
        Self {
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

    /// Binds one explicit Session compaction strategy before this Provider is
    /// shared. The policy becomes part of the immutable Generic Agent config
    /// digest; summaries remain durable Journal facts with their own strategy
    /// and version provenance.
    pub fn with_session_compaction(
        mut self,
        summarizer: Arc<dyn AgentSessionSummarizer>,
        policy: SessionCompactionPolicy,
    ) -> Result<Self, AgentProtocolError> {
        policy.validate().map_err(|error| {
            AgentProtocolError::new(AgentProtocolErrorCode::InvalidSpec, error.to_string())
        })?;
        let inner = Arc::get_mut(&mut self.inner).ok_or_else(|| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "Session compaction must be bound before the Generic Agent Provider is shared",
            )
        })?;
        if inner.session_compactor.is_some() {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "Session compaction is already bound",
            ));
        }
        let compactor =
            AgentSessionCompactor::new(inner.session_journal.clone(), summarizer, policy).map_err(
                |error| {
                    AgentProtocolError::new(AgentProtocolErrorCode::InvalidSpec, error.to_string())
                },
            )?;
        let config_digest = bind_session_compaction_config_digest(
            &inner.config_digest,
            compactor.policy(),
            compactor.summarizer_descriptor(),
        )?;
        inner.config_digest = config_digest;
        inner.session_compactor = Some(Arc::new(compactor));
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
        let token_meter_descriptor = token_meter.meter_descriptor();
        token_meter_descriptor
            .validate()
            .map_err(model_protocol_error)?;
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
            || config.history_limit == 0
            || config.max_context_tokens == 0
            || config.reserved_output_tokens >= config.max_context_tokens
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "Generic Agent buffers and loop limits must be non-zero",
            ));
        }
        if let Some(policy) = &config.model_cost_policy {
            policy.validate()?;
        }
        let has_tools = tools.is_some();
        let has_input_requests = model_descriptor.capabilities.tool_calls;
        let has_approval = tools
            .as_ref()
            .and_then(|tools| tools.approval_bridge.as_ref())
            .is_some();
        let mut supported_limits = BTreeSet::from([
            RunLimitKind::Deadline,
            RunLimitKind::ModelSteps,
            RunLimitKind::InputTokens,
            RunLimitKind::OutputTokens,
        ]);
        if has_tools {
            supported_limits.insert(RunLimitKind::ToolCalls);
        }
        if config.model_cost_policy.is_some() {
            supported_limits.insert(RunLimitKind::Cost);
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
            &token_meter_descriptor,
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
                session_compactor: None,
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
            match receiver.recv().await {
                Ok(item) => Some((item, receiver)),
                Err(broadcast::error::RecvError::Lagged(skipped)) => Some((
                    Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::SequenceGap,
                        format!("Generic Agent stream subscriber lagged by {skipped}"),
                    )),
                    receiver,
                )),
                Err(broadcast::error::RecvError::Closed) => None,
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
        if let (Some(limit), Some(policy)) = (
            request.run.spec.limits.max_cost.as_ref(),
            self.inner.config.model_cost_policy.as_ref(),
        ) {
            if limit.currency != policy.currency {
                return Err(Self::rejection(
                    AgentRejectionCode::UnsupportedCapability,
                    format!(
                        "cost limit currency {} does not match configured model pricing currency {}",
                        limit.currency, policy.currency
                    ),
                ));
            }
        }
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
            let stop_cause = Arc::new(AtomicU8::new(RUN_STOP_RUNNING));
            arm_run_deadline(
                request.run.spec.limits.deadline_unix_ms,
                cancellation.clone(),
                stop_cause.clone(),
            );
            let (steer_signal, steer_updates) = watch::channel(0_u64);
            let run = GenericRun {
                request: request.clone(),
                execution: execution.clone(),
                admission: admission.clone(),
                durable_events: Vec::new(),
                sender,
                terminal: false,
                cancellation: cancellation.clone(),
                stop_cause,
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
            None,
            None,
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
        tokio::spawn(async move {
            execute_model_run(ModelRunExecution {
                inner,
                request,
                user_message,
                model_messages,
                model_tools: model_definitions,
                run_skills,
                seed: GenericExecutionSeed::fresh(),
                cancellation,
                steer_updates,
            })
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
            if !run.terminal && run.stop_cause.load(Ordering::SeqCst) != RUN_STOP_RUNNING {
                return record_command(
                    &self.inner,
                    run,
                    &command,
                    ProviderCommandOutcome::Rejected {
                        code: AgentProtocolErrorCode::InvalidTransition,
                        message: "Run termination is already in progress".to_owned(),
                    },
                );
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
                AgentCommand::Cancel { reason } => {
                    if run
                        .stop_cause
                        .compare_exchange(
                            RUN_STOP_RUNNING,
                            RUN_STOP_HOST_CANCEL,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_err()
                    {
                        return record_command(
                            &self.inner,
                            run,
                            &command,
                            ProviderCommandOutcome::Rejected {
                                code: AgentProtocolErrorCode::InvalidTransition,
                                message: "Run termination is already in progress".to_owned(),
                            },
                        );
                    }
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
                        if pending
                            .responder
                            .as_ref()
                            .is_none_or(|responder| responder.is_closed())
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
        if pending
            .responder
            .as_ref()
            .is_none_or(|responder| responder.is_closed())
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
        let disposition = record_command_with_approval(
            &self.inner,
            run,
            &command,
            ProviderCommandOutcome::Accepted,
            capability.clone(),
        )?;
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
            } => stage_observed_recovery(
                self.inner.clone(),
                stored,
                boundary,
                round,
                request_id,
                request_digest,
                observation,
                recovery_events,
            ),
            GenericCheckpointPhase::WorkflowAttemptOpen {
                boundary,
                round,
                request_id,
                request_digest,
                observation,
                call_id,
                arguments_digest,
            } => stage_started_workflow_recovery(
                self.inner.clone(),
                stored,
                boundary,
                round,
                request_id,
                request_digest,
                observation,
                call_id,
                arguments_digest,
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
            GenericCheckpointEvent::CommandCommitted {
                command, outcome, ..
            } => {
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
            | GenericCheckpointEvent::ModelAttemptObserved { .. }
            | GenericCheckpointEvent::WorkflowAttemptStarted { .. } => {}
        }
    }
    Ok(events)
}

#[allow(clippy::too_many_arguments)]
fn stage_observed_recovery(
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
    if !call.ended {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::Unsupported,
            "observed model recovery requires a complete Tool call",
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
        extensions: call.extensions.clone(),
        ended: call.ended,
    };
    let arguments = parse_tool_arguments(&pending_call).map_err(observed_recovery_error)?;
    if call.name != REQUEST_INPUT_TOOL_NAME {
        if call.name == SKILL_ACTIVATE_TOOL_NAME {
            return stage_skill_recovery(
                inner,
                stored,
                boundary,
                round,
                request_id,
                request_digest,
                observation,
                call,
                arguments,
                recovery_events,
            );
        }
        if call.name == WORKFLOW_TOOL_NAME {
            return stage_workflow_recovery(
                inner,
                stored,
                boundary,
                round,
                request_id,
                request_digest,
                observation,
                call,
                arguments,
                recovery_events,
            );
        }
        let approval_id = approval_request_id(stored.registration.run_id(), round, &call.call_id);
        let has_approval_interaction = recovery_events.iter().any(|event| match &event.payload {
            AgentEvent::RequestOpened { request } => request.request_id == approval_id,
            AgentEvent::RequestResolved { request_id, .. } => request_id == &approval_id,
            _ => false,
        });
        return if has_approval_interaction {
            stage_approval_recovery(
                inner,
                stored,
                boundary,
                round,
                request_id,
                request_digest,
                observation,
                call,
                arguments,
                recovery_events,
            )
        } else {
            stage_tool_recovery(
                inner,
                stored,
                boundary,
                round,
                request_id,
                request_digest,
                observation,
                call,
                arguments,
                recovery_events,
            )
        };
    }
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

#[allow(clippy::too_many_arguments)]
fn stage_workflow_recovery(
    inner: Arc<GenericInner>,
    stored: StoredGenericAgentRun,
    boundary: GenericLoopBoundary,
    round: u64,
    request_id: ModelRequestId,
    request_digest: Digest,
    observation: GenericModelObservation,
    call: GenericObservedToolCall,
    arguments: serde_json::Value,
    recovery_events: Vec<AgentEventDraft>,
) -> Result<AgentRecovery, AgentProtocolError> {
    if recovery_events.iter().any(|event| {
        matches!(
            &event.payload,
            AgentEvent::RequestOpened { .. } | AgentEvent::RequestResolved { .. }
        )
    }) {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered Workflow crossed an interaction boundary",
        ));
    }
    stage_loop_recovery(
        inner,
        stored,
        boundary,
        recovery_events,
        GenericRecoveryContinuation::Workflow {
            round,
            request_id,
            request_digest,
            observation,
            call,
            arguments,
            recovery_replay: false,
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn stage_started_workflow_recovery(
    inner: Arc<GenericInner>,
    stored: StoredGenericAgentRun,
    boundary: GenericLoopBoundary,
    round: u64,
    request_id: ModelRequestId,
    request_digest: Digest,
    observation: GenericModelObservation,
    call_id: ModelToolCallId,
    arguments_digest: Digest,
    recovery_events: Vec<AgentEventDraft>,
) -> Result<AgentRecovery, AgentProtocolError> {
    let call = observation
        .tool_calls
        .iter()
        .find(|call| call.call_id == call_id)
        .cloned()
        .ok_or_else(|| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "Workflow start fence has no matching observed model call",
            )
        })?;
    if call.name != WORKFLOW_TOOL_NAME
        || !call.ended
        || Digest::sha256(call.arguments.as_bytes()) != arguments_digest
    {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "Workflow start fence differs from its observed model call",
        ));
    }
    let arguments = parse_tool_arguments(&PendingModelToolCall {
        call_id: call.call_id.clone(),
        name: call.name.clone(),
        arguments: call.arguments.clone(),
        extensions: call.extensions.clone(),
        ended: call.ended,
    })
    .map_err(observed_recovery_error)?;
    let recovered_output = recovered_workflow_output(
        stored.registration.run_id(),
        round,
        &call.call_id,
        &recovery_events,
    )?;
    let Some((workflow_event_id, outcome)) = recovered_output else {
        let workflow = inner
            .tools
            .as_ref()
            .and_then(|tools| tools.workflow.as_ref())
            .ok_or_else(|| {
                AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered Workflow call has no bound execution strategy",
                )
            })?;
        if !workflow.supports_recovery_replay() {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::Unsupported,
                "Workflow execution contract does not support deterministic recovery replay",
            )
            .with_details(serde_json::json!({
                "boundary": "workflow_attempt_open",
                "round": round,
                "request_id": request_id,
                "call_id": call_id,
            })));
        }
        return stage_loop_recovery(
            inner,
            stored,
            boundary,
            recovery_events,
            GenericRecoveryContinuation::Workflow {
                round,
                request_id,
                request_digest,
                observation,
                call,
                arguments,
                recovery_replay: true,
            },
        );
    };
    let tool_call_limit = stored
        .registration
        .request
        .run
        .spec
        .limits
        .max_tool_calls
        .unwrap_or(inner.config.max_tool_calls)
        .min(inner.config.max_tool_calls);
    let outer_count = boundary.tool_call_count.saturating_add(1);
    if outer_count >= tool_call_limit
        || outer_count.saturating_add(outcome.tool_calls) > tool_call_limit
    {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "durable Workflow output exceeds the Run Tool call limit",
        ));
    }
    stage_loop_recovery(
        inner,
        stored,
        boundary,
        recovery_events,
        GenericRecoveryContinuation::WorkflowOutput {
            round,
            request_id,
            request_digest,
            observation,
            call,
            arguments,
            outcome,
            workflow_event_id,
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn stage_skill_recovery(
    inner: Arc<GenericInner>,
    stored: StoredGenericAgentRun,
    boundary: GenericLoopBoundary,
    round: u64,
    request_id: ModelRequestId,
    request_digest: Digest,
    observation: GenericModelObservation,
    call: GenericObservedToolCall,
    arguments: serde_json::Value,
    recovery_events: Vec<AgentEventDraft>,
) -> Result<AgentRecovery, AgentProtocolError> {
    if recovery_events.iter().any(|event| {
        matches!(
            &event.payload,
            AgentEvent::RequestOpened { .. } | AgentEvent::RequestResolved { .. }
        )
    }) {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered Skill activation crossed an interaction boundary",
        ));
    }
    stage_loop_recovery(
        inner,
        stored,
        boundary,
        recovery_events,
        GenericRecoveryContinuation::Skill {
            round,
            request_id,
            request_digest,
            observation,
            call,
            arguments,
            recovered_observation: None,
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn stage_tool_recovery(
    inner: Arc<GenericInner>,
    stored: StoredGenericAgentRun,
    boundary: GenericLoopBoundary,
    round: u64,
    request_id: ModelRequestId,
    request_digest: Digest,
    observation: GenericModelObservation,
    call: GenericObservedToolCall,
    arguments: serde_json::Value,
    recovery_events: Vec<AgentEventDraft>,
) -> Result<AgentRecovery, AgentProtocolError> {
    if recovery_events.iter().any(|event| {
        matches!(
            &event.payload,
            AgentEvent::RequestOpened { .. } | AgentEvent::RequestResolved { .. }
        )
    }) {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered direct Tool crossed an interaction boundary",
        ));
    }
    stage_loop_recovery(
        inner,
        stored,
        boundary,
        recovery_events,
        GenericRecoveryContinuation::Tool {
            round,
            request_id,
            request_digest,
            observation,
            call,
            arguments,
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn stage_approval_recovery(
    inner: Arc<GenericInner>,
    stored: StoredGenericAgentRun,
    boundary: GenericLoopBoundary,
    round: u64,
    request_id: ModelRequestId,
    request_digest: Digest,
    observation: GenericModelObservation,
    call: GenericObservedToolCall,
    arguments: serde_json::Value,
    recovery_events: Vec<AgentEventDraft>,
) -> Result<AgentRecovery, AgentProtocolError> {
    let approval_request_id =
        approval_request_id(stored.registration.run_id(), round, &call.call_id);
    let mut opened_request = None;
    let mut resolved_response = None;
    for event in &recovery_events {
        match &event.payload {
            AgentEvent::RequestOpened { request } if request.request_id == approval_request_id => {
                if opened_request.is_some()
                    || !request.blocking
                    || !matches!(request.payload, PendingRequestPayload::Approval { .. })
                {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "recovered approval request is not a unique blocking request",
                    ));
                }
                opened_request = Some(request.clone());
            }
            AgentEvent::RequestResolved {
                request_id: resolved,
                resolution,
                ..
            } if resolved == &approval_request_id => {
                if opened_request.is_none()
                    || resolved_response.is_some()
                    || !matches!(resolution, RequestResolution::Approval { .. })
                {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "recovered approval resolution does not match its pending request",
                    ));
                }
                let command_id = event.causation_id.clone().ok_or_else(|| {
                    AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "recovered approval resolution has no causating command",
                    )
                })?;
                resolved_response = Some(recovered_approval_response(
                    &stored.records,
                    &approval_request_id,
                    &command_id,
                    resolution,
                )?);
            }
            AgentEvent::RequestOpened { .. } | AgentEvent::RequestResolved { .. } => {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered interaction crossed the observed approval request boundary",
                ));
            }
            _ => {}
        }
    }
    let request = opened_request.ok_or_else(|| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::Unsupported,
            "observed effect Tool recovery currently requires a durable approval request",
        )
    })?;
    stage_loop_recovery(
        inner,
        stored,
        boundary,
        recovery_events,
        GenericRecoveryContinuation::Approval {
            round,
            request_id,
            request_digest,
            observation,
            call,
            arguments,
            request,
            binding: None,
            committed_response: None,
            resolved_response,
            response: None,
        },
    )
}

async fn prepare_recovered_tool(
    inner: &GenericInner,
    run_id: &RunId,
    call: &GenericObservedToolCall,
    arguments: &serde_json::Value,
    cancellation: CancellationToken,
) -> Result<GuardedToolResult, AgentFailure> {
    let tools = inner.tools.as_ref().ok_or_else(|| {
        agent_failure(
            "tool_runtime_unavailable",
            "recovered Tool call has no bound Tool Runtime",
            false,
        )
    })?;
    let tool_id = tools
        .runtime
        .resolve_tool_id(&call.name)
        .map_err(|error| {
            agent_failure(
                "tool_runtime_unavailable",
                format!("recovered Tool catalog is unavailable: {error}"),
                true,
            )
        })?
        .ok_or_else(|| {
            agent_failure(
                "tool_not_found",
                "recovered Tool is no longer registered",
                false,
            )
        })?;
    Ok(tools
        .runtime
        .invoke(
            ToolInvocation {
                run_id: run_id.clone(),
                call_id: ToolCallId::new(call.call_id.as_str()),
                tool_id,
                arguments: arguments.clone(),
            },
            tools.run_grant.clone(),
            None,
            cancellation,
        )
        .await)
}

async fn recover_committed_tool_outcome(
    inner: &GenericInner,
    run_id: &RunId,
    call: &GenericObservedToolCall,
    arguments: &serde_json::Value,
) -> Result<ToolOutcome, AgentProtocolError> {
    let tools = inner.tools.as_ref().ok_or_else(|| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered Tool exchange has no bound Tool Runtime",
        )
    })?;
    let tool_id = tools
        .runtime
        .resolve_tool_id(&call.name)
        .map_err(|error| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::ProviderUnavailable,
                format!("recovered Tool catalog is unavailable: {error}"),
            )
            .with_retryable(true)
        })?
        .ok_or_else(|| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered Tool is no longer registered",
            )
        })?;
    let recovered = tools
        .runtime
        .recover_outcome(
            ToolInvocation {
                run_id: run_id.clone(),
                call_id: ToolCallId::new(call.call_id.as_str()),
                tool_id,
                arguments: arguments.clone(),
            },
            tools.run_grant.clone(),
        )
        .await
        .map_err(|error| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "durable Tool effect cannot validate the recovered Session exchange",
            )
            .with_details(serde_json::json!({
                "code": error.code,
                "message": error.message,
            }))
        })?;
    recovered.ok_or_else(|| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "Session Tool exchange has no durable Tool outcome",
        )
    })
}

struct RecoveredApprovalPreparation<'a> {
    run_id: &'a RunId,
    round: u64,
    call: &'a GenericObservedToolCall,
    arguments: &'a serde_json::Value,
    opened_request: &'a PendingRequest,
    persisted_response: Option<&'a ApprovalResponse>,
    attach_waiter: bool,
    cancellation: CancellationToken,
}

async fn prepare_recovered_approval(
    inner: &GenericInner,
    preparation: RecoveredApprovalPreparation<'_>,
) -> Result<RecoveredApprovalWaiter, AgentProtocolError> {
    let RecoveredApprovalPreparation {
        run_id,
        round,
        call,
        arguments,
        opened_request,
        persisted_response,
        attach_waiter,
        cancellation,
    } = preparation;
    let tools = inner.tools.as_ref().ok_or_else(|| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered approval has no bound Tool Runtime",
        )
    })?;
    let bridge = tools.approval_bridge.clone().ok_or_else(|| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered approval has no Host approval bridge",
        )
    })?;
    let tool_id = tools
        .runtime
        .resolve_tool_id(&call.name)
        .map_err(|error| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::ProviderUnavailable,
                format!("recovered Tool catalog is unavailable: {error}"),
            )
            .with_retryable(true)
        })?
        .ok_or_else(|| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered approval Tool is no longer registered",
            )
        })?;
    let invocation = ToolInvocation {
        run_id: run_id.clone(),
        call_id: ToolCallId::new(call.call_id.as_str()),
        tool_id,
        arguments: arguments.clone(),
    };
    let (binding, summary, replayed_outcome) = match tools
        .runtime
        .invoke(
            invocation.clone(),
            tools.run_grant.clone(),
            None,
            cancellation,
        )
        .await
    {
        GuardedToolResult::ApprovalRequired { binding, summary } => (binding, Some(summary), None),
        GuardedToolResult::Outcome {
            outcome,
            cached: true,
        } => {
            let capability = persisted_response
                .and_then(|response| match &response.resolution {
                    RequestResolution::Approval {
                        decision: ApprovalDecision::Allow,
                        ..
                    } => response.capability.as_ref(),
                    _ => None,
                })
                .ok_or_else(|| {
                    AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "recovered Tool outcome has no durable Allow capability",
                    )
                })?;
            let binding = capability.claims.binding.clone();
            let args_digest = invocation.args_digest().map_err(|error| {
                AgentProtocolError::new(AgentProtocolErrorCode::InvalidDigest, error.to_string())
            })?;
            if binding.run_id != invocation.run_id
                || binding.call_id != invocation.call_id
                || binding.tool_id != invocation.tool_id
                || binding.args_digest != args_digest
            {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "persisted approval capability crossed its recovered Tool invocation",
                ));
            }
            (binding, None, Some(outcome))
        }
        GuardedToolResult::Outcome {
            outcome,
            cached: false,
        } => {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovery executed a Tool before validating its durable approval state",
            )
            .with_details(serde_json::to_value(outcome).unwrap_or(serde_json::Value::Null)))
        }
    };
    if binding.run_id != *run_id || binding.call_id.as_str() != call.call_id.as_str() {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "reconstructed approval binding crossed its Run or Tool call",
        ));
    }
    let operation_digest = binding.digest().map_err(|error| {
        AgentProtocolError::new(AgentProtocolErrorCode::InvalidDigest, error.to_string())
    })?;
    let requested_scope = approval_scope_names(&binding).map_err(observed_recovery_error)?;
    let request_id = approval_request_id(run_id, round, &call.call_id);
    // A committed effect replays before the Tool Runtime can re-emit its
    // presentation-only summary. The authority-bearing request fields remain
    // fully derivable from the persisted capability binding.
    let request_matches = opened_request.request_id == request_id
        && opened_request.blocking
        && matches!(
            &opened_request.payload,
            PendingRequestPayload::Approval {
                operation_digest: actual_digest,
                requested_scope: actual_scope,
                reason,
            } if actual_digest == &operation_digest
                && actual_scope == &requested_scope
                && summary.as_ref().is_none_or(|expected| reason == expected)
        );
    if !request_matches {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "reconstructed approval does not match the durable pending request",
        ));
    }
    let (responder, response) = if attach_waiter {
        bridge
            .stage(&request_id, binding.clone())
            .await
            .map_err(|error| {
                AgentProtocolError::new(
                    AgentProtocolErrorCode::ProviderUnavailable,
                    format!("Host approval bridge could not restage the request: {error}"),
                )
                .with_retryable(true)
            })?;
        let (responder, response) = oneshot::channel();
        (Some(responder), Some(response))
    } else {
        (None, None)
    };
    Ok(RecoveredApprovalWaiter {
        request_id,
        binding,
        replayed_outcome,
        responder,
        response,
        bridge,
    })
}

fn validate_recovered_input_resolution(
    records: &[crate::generic_agent_checkpoint::GenericCheckpointRecord],
    request_id: &RequestId,
    response: &InputResponse,
) -> Result<(), AgentProtocolError> {
    let mut matching_commands = 0_usize;
    for record in records {
        let GenericCheckpointEvent::CommandCommitted {
            command, outcome, ..
        } = &record.payload
        else {
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

fn recovered_approval_response(
    records: &[crate::generic_agent_checkpoint::GenericCheckpointRecord],
    request_id: &RequestId,
    command_id: &CommandId,
    resolution: &RequestResolution,
) -> Result<ApprovalResponse, AgentProtocolError> {
    let mut matching = None;
    for record in records {
        let GenericCheckpointEvent::CommandCommitted {
            command,
            outcome,
            approval_capability,
        } = &record.payload
        else {
            continue;
        };
        if &command.command_id != command_id {
            continue;
        }
        if matching.is_some() {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered approval resolution has duplicate causating commands",
            ));
        }
        let matches_resolution = matches!(
            &command.payload,
            AgentCommand::ResolveRequest { response } if response == resolution
        );
        if outcome != &ProviderCommandOutcome::Accepted
            || command.request_id.as_ref() != Some(request_id)
            || !matches_resolution
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered approval resolution does not match its accepted command",
            ));
        }
        let valid_capability = matches!(
            (resolution, approval_capability),
            (
                RequestResolution::Approval {
                    decision: ApprovalDecision::Allow,
                    ..
                },
                Some(_)
            ) | (
                RequestResolution::Approval {
                    decision: ApprovalDecision::Deny,
                    ..
                },
                None
            )
        );
        if !valid_capability {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered approval resolution has inconsistent capability evidence",
            ));
        }
        matching = Some(ApprovalResponse {
            command_id: command.command_id.clone(),
            resolution: resolution.clone(),
            capability: approval_capability.clone(),
        });
    }
    matching.ok_or_else(|| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered approval resolution has no unique accepted command",
        )
    })
}

fn recovered_model_output_tokens(
    budgets: &BTreeMap<u64, Option<u64>>,
    round: u64,
) -> Result<Option<u64>, AgentProtocolError> {
    budgets.get(&round).copied().ok_or_else(|| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered model attempt has no durable output-budget reservation",
        )
    })
}

fn stage_loop_recovery(
    inner: Arc<GenericInner>,
    stored: StoredGenericAgentRun,
    boundary: GenericLoopBoundary,
    recovery_events: Vec<AgentEventDraft>,
    mut continuation: GenericRecoveryContinuation,
) -> Result<AgentRecovery, AgentProtocolError> {
    let model_output_budgets = stored
        .records
        .iter()
        .filter_map(|record| match &record.payload {
            GenericCheckpointEvent::ModelAttemptStarted {
                round,
                max_output_tokens,
                ..
            } => Some((*round, *max_output_tokens)),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    let checkpoint_seq = stored.last_checkpoint_seq();
    let registration = stored.registration;
    let request = registration.request.clone();
    let execution = registration.execution.clone();
    let admission = registration.admission.clone();
    let run_id = execution.run_id.clone();
    let recovered_attempt_input_budget = request
        .run
        .spec
        .limits
        .max_input_tokens
        .map(|limit| limit.saturating_sub(boundary.usage.input_tokens.unwrap_or(0)));
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
        GenericRecoveryContinuation::Skill { .. }
        | GenericRecoveryContinuation::Workflow { .. }
        | GenericRecoveryContinuation::WorkflowOutput { .. }
        | GenericRecoveryContinuation::Tool { .. }
            if !pending_resolutions.is_empty() =>
        {
            let pending = pending_resolutions
                .values()
                .next()
                .expect("non-empty pending resolution map was checked");
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "direct Tool recovery cannot apply an accepted request resolution",
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
            if let Some(recovered) = pending_resolutions.remove(&expected_request_id) {
                if recovered.capability.is_some() {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "input resolution unexpectedly carried an approval capability",
                    ));
                }
                *committed_response = Some(InputResponse {
                    command_id: recovered.command_id,
                    resolution: recovered.resolution,
                });
            }
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
        GenericRecoveryContinuation::Approval {
            round,
            call,
            committed_response,
            resolved_response,
            ..
        } => {
            let expected_request_id = approval_request_id(&run_id, *round, &call.call_id);
            if let Some(recovered) = pending_resolutions.remove(&expected_request_id) {
                let valid = matches!(
                    (&recovered.resolution, &recovered.capability),
                    (
                        RequestResolution::Approval {
                            decision: ApprovalDecision::Allow,
                            ..
                        },
                        Some(_)
                    ) | (
                        RequestResolution::Approval {
                            decision: ApprovalDecision::Deny,
                            ..
                        },
                        None
                    )
                );
                if !valid {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "accepted approval resolution has inconsistent capability evidence",
                    ));
                }
                *committed_response = Some(ApprovalResponse {
                    command_id: recovered.command_id,
                    resolution: recovered.resolution,
                    capability: recovered.capability,
                });
            }
            if !pending_resolutions.is_empty() {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "accepted request resolution crossed the recovered approval boundary",
                ));
            }
            if committed_response.is_some() && resolved_response.is_some() {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered approval resolution was both pending and already applied",
                ));
            }
        }
        GenericRecoveryContinuation::ModelLoop { .. }
        | GenericRecoveryContinuation::Skill { .. }
        | GenericRecoveryContinuation::Workflow { .. }
        | GenericRecoveryContinuation::WorkflowOutput { .. }
        | GenericRecoveryContinuation::Tool { .. } => {}
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
    let run_started = recovery_events
        .iter()
        .any(|event| matches!(&event.payload, AgentEvent::RunStarted));
    let started_event_id = AgentEventId::new(format!("generic-{}-started", run_id.as_str()));
    let mut supporting_event_ids = boundary.supporting_event_ids;
    if run_started && !supporting_event_ids.contains(&started_event_id) {
        supporting_event_ids.push(started_event_id);
    }
    let seed = GenericExecutionSeed {
        run_started,
        next_model_round: boundary.next_model_round,
        total_usage: boundary.usage,
        tool_call_count: boundary.tool_call_count,
        last_response: boundary.last_response,
        supporting_event_ids,
    };
    let (sender, _) = broadcast::channel(inner.config.stream_buffer);
    let cancellation = CancellationToken::new();
    let stop_cause = Arc::new(AtomicU8::new(RUN_STOP_RUNNING));
    arm_run_deadline(
        request.run.spec.limits.deadline_unix_ms,
        cancellation.clone(),
        stop_cause.clone(),
    );
    let (steer_signal, steer_updates) = watch::channel(0_u64);
    let mut run = GenericRun {
        request: request.clone(),
        execution: execution.clone(),
        admission: admission.clone(),
        durable_events: recovery_events,
        sender,
        terminal: false,
        cancellation: cancellation.clone(),
        stop_cause,
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
            None,
            recovered_attempt_input_budget,
        )
        .await
        .map_err(session_context_recovery_error)?;
        let mut session_exchange_committed = false;
        if let GenericRecoveryContinuation::Input {
            round,
            request_id,
            request_digest,
            observation,
            call,
            arguments,
            resolved_response,
            ..
        } = &continuation
        {
            let expected_exchange = resolved_response
                .as_ref()
                .map(|response| {
                    input_resolution_result(&response.resolution).map(|result| {
                        observed_input_exchange_messages(observation, call, arguments, &result)
                    })
                })
                .transpose()
                .map_err(checkpoint_stream_error)?;
            let session_exchange_seq = recovered_tool_exchange_committed(
                &inner,
                &request,
                *round,
                request_id,
                expected_exchange.as_ref(),
                &[],
                observation.usage.as_ref(),
            )
            .await?;
            session_exchange_committed = session_exchange_seq.is_some();
            let prior_model_messages = if let Some(exchange_seq) = session_exchange_seq {
                let (assistant, tool) = expected_exchange
                    .as_ref()
                    .expect("a committed input exchange has a private resolved response");
                if !model_messages.ends_with(&[assistant.clone(), tool.clone()]) {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "recovered Session input exchange is not the final projected context",
                    ));
                }
                Some(
                    project_model_messages(
                        &inner,
                        &request,
                        &model_definitions,
                        run_skills.as_deref(),
                        None,
                        Some(exchange_seq.saturating_sub(1)),
                        recovered_attempt_input_budget,
                    )
                    .await
                    .map_err(session_context_recovery_error)?,
                )
            } else {
                None
            };
            let request_messages = prior_model_messages
                .as_deref()
                .unwrap_or(model_messages.as_slice());
            let rebuilt = model_request_for_round(
                &request,
                *round,
                request_messages,
                &model_definitions,
                recovered_model_output_tokens(&model_output_budgets, *round)?,
            );
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
        let mut staged_approval = None;
        if let GenericRecoveryContinuation::Approval {
            round,
            request_id,
            request_digest,
            observation,
            call,
            arguments,
            request: opened_request,
            binding,
            committed_response,
            resolved_response,
            response,
            ..
        } = &mut continuation
        {
            let persisted_response = committed_response.as_ref().or(resolved_response.as_ref());
            let prepared = prepare_recovered_approval(
                &inner,
                RecoveredApprovalPreparation {
                    run_id: &run_id,
                    round: *round,
                    call,
                    arguments,
                    opened_request,
                    persisted_response,
                    attach_waiter: committed_response.is_none() && resolved_response.is_none(),
                    cancellation: cancellation.clone(),
                },
            )
            .await?;
            let expected_exchange = match resolved_response.as_ref() {
                Some(resolution) => recovered_approval_exchange_messages(
                    observation,
                    call,
                    arguments,
                    resolution,
                    prepared.replayed_outcome.as_ref(),
                )?,
                None => None,
            };
            let expected_retained_artifacts = prepared
                .replayed_outcome
                .as_ref()
                .map(retained_artifacts_for_outcome)
                .unwrap_or_default();
            let session_exchange_seq = recovered_tool_exchange_committed(
                &inner,
                &request,
                *round,
                request_id,
                expected_exchange.as_ref(),
                &expected_retained_artifacts,
                observation.usage.as_ref(),
            )
            .await?;
            session_exchange_committed = session_exchange_seq.is_some();
            let prior_model_messages = if let Some(exchange_seq) = session_exchange_seq {
                let (assistant, tool) = expected_exchange
                    .as_ref()
                    .expect("a committed approval exchange has a recoverable durable result");
                if !model_messages.ends_with(&[assistant.clone(), tool.clone()]) {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "recovered Session approval exchange is not the final projected context",
                    ));
                }
                Some(
                    project_model_messages(
                        &inner,
                        &request,
                        &model_definitions,
                        run_skills.as_deref(),
                        None,
                        Some(exchange_seq.saturating_sub(1)),
                        recovered_attempt_input_budget,
                    )
                    .await
                    .map_err(session_context_recovery_error)?,
                )
            } else {
                None
            };
            let request_messages = prior_model_messages
                .as_deref()
                .unwrap_or(model_messages.as_slice());
            let rebuilt = model_request_for_round(
                &request,
                *round,
                request_messages,
                &model_definitions,
                recovered_model_output_tokens(&model_output_budgets, *round)?,
            );
            if rebuilt.request_id != *request_id
                || model_request_digest(&rebuilt).map_err(checkpoint_stream_error)?
                    != *request_digest
            {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered approval model request no longer matches the private WAL attempt",
                ));
            }
            if committed_response
                .as_ref()
                .or(resolved_response.as_ref())
                .is_some_and(|response| {
                    response
                        .capability
                        .as_ref()
                        .is_some_and(|capability| capability.claims.binding != prepared.binding)
                })
            {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "persisted approval capability does not match the reconstructed binding",
                ));
            }
            if let Some(responder) = prepared.responder {
                run.pending_approvals.insert(
                    prepared.request_id.clone(),
                    PendingApproval {
                        binding: prepared.binding.clone(),
                        responder: Some(responder),
                    },
                );
                staged_approval = Some((prepared.bridge.clone(), prepared.request_id.clone()));
            }
            *binding = Some(prepared.binding);
            *response = prepared.response;
        }
        if let GenericRecoveryContinuation::Skill {
            round,
            request_id,
            request_digest,
            observation,
            call,
            arguments,
            recovered_observation,
        } = &mut continuation
        {
            let skills = run_skills.as_deref().ok_or_else(|| {
                AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered Skill call has no bound Skill catalog",
                )
            })?;
            let prepared = prepare_recovered_skill(
                &inner,
                &request,
                skills,
                *round,
                request_id,
                observation,
                call,
                arguments,
            )
            .await?;
            if prepared.activation_committed {
                let context_message =
                    prepared
                        .observation
                        .context_message
                        .clone()
                        .ok_or_else(|| {
                            AgentProtocolError::new(
                                AgentProtocolErrorCode::InvalidDigest,
                                "recovered Skill activation has no immutable context message",
                            )
                        })?;
                if model_messages
                    .iter()
                    .filter(|message| *message == &context_message)
                    .count()
                    != 1
                {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "recovered Skill activation is not uniquely projected into context",
                    ));
                }
            }
            if let Some(record) = &prepared.exchange_record {
                let AgentSessionEvent::ToolExchangeCommitted {
                    assistant, tool, ..
                } = &record.payload
                else {
                    unreachable!("recovered Skill exchange shape was checked");
                };
                if !model_messages.ends_with(&[assistant.clone(), tool.clone()]) {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "recovered Skill exchange is not the final projected Session context",
                    ));
                }
            }
            let prior_model_messages = if let Some(prior_seq) = prepared.prior_session_seq {
                Some(
                    project_model_messages(
                        &inner,
                        &request,
                        &model_definitions,
                        run_skills.as_deref(),
                        None,
                        Some(prior_seq),
                        recovered_attempt_input_budget,
                    )
                    .await
                    .map_err(session_context_recovery_error)?,
                )
            } else {
                None
            };
            let request_messages = prior_model_messages
                .as_deref()
                .unwrap_or(model_messages.as_slice());
            let rebuilt = model_request_for_round(
                &request,
                *round,
                request_messages,
                &model_definitions,
                recovered_model_output_tokens(&model_output_budgets, *round)?,
            );
            if rebuilt.request_id != *request_id
                || model_request_digest(&rebuilt).map_err(checkpoint_stream_error)?
                    != *request_digest
            {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered Skill model request no longer matches the private WAL attempt",
                ));
            }
            session_exchange_committed = prepared.exchange_record.is_some();
            if prepared.activation_committed {
                *recovered_observation = Some(prepared.observation);
            }
        }
        if let GenericRecoveryContinuation::Workflow {
            round,
            request_id,
            request_digest,
            call,
            ..
        } = &continuation
        {
            if inner
                .tools
                .as_ref()
                .and_then(|tools| tools.workflow.as_ref())
                .is_none()
            {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered Workflow call has no bound execution strategy",
                ));
            }
            if recovered_tool_exchange_record(&inner, &request, *round, request_id)
                .await?
                .is_some()
                || run.durable_events.iter().any(|event| {
                    event.event_id == workflow_output_event_id(&run_id, *round, &call.call_id)
                })
            {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "Workflow outcome exists without its private start fence",
                ));
            }
            let rebuilt = model_request_for_round(
                &request,
                *round,
                &model_messages,
                &model_definitions,
                recovered_model_output_tokens(&model_output_budgets, *round)?,
            );
            if rebuilt.request_id != *request_id
                || model_request_digest(&rebuilt).map_err(checkpoint_stream_error)?
                    != *request_digest
            {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered Workflow model request no longer matches the private WAL attempt",
                ));
            }
        }
        if let GenericRecoveryContinuation::WorkflowOutput {
            round,
            request_id,
            request_digest,
            observation,
            call,
            arguments,
            outcome,
            ..
        } = &continuation
        {
            let expected_exchange = observed_tool_exchange_messages(
                observation,
                call,
                arguments,
                &outcome.result,
                outcome.is_error,
            );
            let session_exchange_seq = recovered_tool_exchange_committed(
                &inner,
                &request,
                *round,
                request_id,
                Some(&expected_exchange),
                &[],
                observation.usage.as_ref(),
            )
            .await?;
            session_exchange_committed = session_exchange_seq.is_some();
            let prior_model_messages = if let Some(exchange_seq) = session_exchange_seq {
                let (assistant, tool) = &expected_exchange;
                if !model_messages.ends_with(&[assistant.clone(), tool.clone()]) {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "recovered Workflow exchange is not the final projected Session context",
                    ));
                }
                Some(
                    project_model_messages(
                        &inner,
                        &request,
                        &model_definitions,
                        run_skills.as_deref(),
                        None,
                        Some(exchange_seq.saturating_sub(1)),
                        recovered_attempt_input_budget,
                    )
                    .await
                    .map_err(session_context_recovery_error)?,
                )
            } else {
                None
            };
            let request_messages = prior_model_messages
                .as_deref()
                .unwrap_or(model_messages.as_slice());
            let rebuilt = model_request_for_round(
                &request,
                *round,
                request_messages,
                &model_definitions,
                recovered_model_output_tokens(&model_output_budgets, *round)?,
            );
            if rebuilt.request_id != *request_id
                || model_request_digest(&rebuilt).map_err(checkpoint_stream_error)?
                    != *request_digest
            {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered Workflow model request no longer matches the private WAL attempt",
                ));
            }
        }
        if let GenericRecoveryContinuation::Tool {
            round,
            request_id,
            request_digest,
            observation,
            call,
            arguments,
            ..
        } = &mut continuation
        {
            let recovered_exchange =
                recovered_tool_exchange_record(&inner, &request, *round, request_id).await?;
            let prior_model_messages = if let Some(record) = &recovered_exchange {
                let AgentSessionEvent::ToolExchangeCommitted {
                    assistant, tool, ..
                } = &record.payload
                else {
                    unreachable!("recovered Tool exchange shape was checked");
                };
                if !model_messages.ends_with(&[assistant.clone(), tool.clone()]) {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "recovered direct Tool exchange is not the final projected context",
                    ));
                }
                Some(
                    project_model_messages(
                        &inner,
                        &request,
                        &model_definitions,
                        run_skills.as_deref(),
                        None,
                        Some(record.session_seq.saturating_sub(1)),
                        recovered_attempt_input_budget,
                    )
                    .await
                    .map_err(session_context_recovery_error)?,
                )
            } else {
                None
            };
            let request_messages = prior_model_messages
                .as_deref()
                .unwrap_or(model_messages.as_slice());
            let rebuilt = model_request_for_round(
                &request,
                *round,
                request_messages,
                &model_definitions,
                recovered_model_output_tokens(&model_output_budgets, *round)?,
            );
            if rebuilt.request_id != *request_id
                || model_request_digest(&rebuilt).map_err(checkpoint_stream_error)?
                    != *request_digest
            {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered direct Tool model request no longer matches the private WAL attempt",
                ));
            }
            if let Some(record) = recovered_exchange {
                let outcome =
                    recover_committed_tool_outcome(&inner, &run_id, call, arguments).await?;
                if matches!(outcome, ToolOutcome::UnknownEffect { .. }) {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "Session Tool exchange cannot be backed by an unknown effect",
                    ));
                }
                let retained_artifacts = retained_artifacts_for_outcome(&outcome);
                let (result, is_error) = model_tool_result(outcome);
                let (assistant, tool) = observed_tool_exchange_messages(
                    observation,
                    call,
                    arguments,
                    &result,
                    is_error,
                );
                let expected_payload = AgentSessionEvent::ToolExchangeCommitted {
                    request_id: request_id.clone(),
                    assistant,
                    tool,
                    retained_artifacts,
                    usage: observation.usage.clone(),
                };
                if record.payload != expected_payload {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "direct Tool Session exchange differs from its durable Effect outcome",
                    ));
                }
                session_exchange_committed = true;
            }
        }
        let install_result = {
            let mut state = inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.runs.contains_key(&run_id) {
                Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidTransition,
                    "Generic Agent Run was recovered concurrently",
                ))
            } else if state
                .sessions
                .get(&request.run.spec.session_id)
                .and_then(|session| session.active_run.as_ref())
                .is_some_and(|active| active != &run_id)
            {
                Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidTransition,
                    "another Generic Agent Run already owns this Session",
                ))
            } else {
                state
                    .sessions
                    .entry(request.run.spec.session_id.clone())
                    .or_default()
                    .active_run = Some(run_id.clone());
                state.runs.insert(run_id.clone(), run);
                Ok(())
            }
        };
        if let Err(error) = install_result {
            if let Some((bridge, request_id)) = staged_approval {
                let _ = bridge.clear(&request_id).await;
            }
            return Err(error);
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
            if seed.run_started && cancellation.is_cancelled() {
                emit_cancel(&inner, &request, &user_message);
                return;
            }
            match continuation {
                GenericRecoveryContinuation::ModelLoop { .. } => {
                    execute_model_run(ModelRunExecution {
                        inner,
                        request,
                        user_message,
                        model_messages,
                        model_tools: model_definitions,
                        run_skills,
                        seed,
                        cancellation,
                        steer_updates,
                    })
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
                        session_exchange_committed,
                        response,
                    )
                    .await;
                }
                GenericRecoveryContinuation::Approval {
                    round,
                    request_id,
                    observation,
                    call,
                    arguments,
                    binding,
                    committed_response,
                    resolved_response,
                    response,
                    ..
                } => {
                    resume_observed_approval(
                        inner,
                        request,
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
                        binding.expect("recovered approval binding was prepared"),
                        committed_response,
                        resolved_response,
                        session_exchange_committed,
                        response,
                    )
                    .await;
                }
                GenericRecoveryContinuation::Skill {
                    round,
                    request_id,
                    observation,
                    call,
                    arguments,
                    recovered_observation,
                    ..
                } => {
                    resume_observed_skill(
                        inner,
                        request,
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
                        recovered_observation,
                        session_exchange_committed,
                    )
                    .await;
                }
                GenericRecoveryContinuation::Workflow {
                    round,
                    request_id,
                    observation,
                    call,
                    arguments,
                    recovery_replay,
                    ..
                } => {
                    resume_observed_workflow(
                        inner,
                        request,
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
                        recovery_replay,
                    )
                    .await;
                }
                GenericRecoveryContinuation::WorkflowOutput {
                    round,
                    request_id,
                    observation,
                    call,
                    arguments,
                    outcome,
                    workflow_event_id,
                    ..
                } => {
                    resume_observed_workflow_output(
                        inner,
                        request,
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
                        outcome,
                        workflow_event_id,
                        session_exchange_committed,
                    )
                    .await;
                }
                GenericRecoveryContinuation::Tool {
                    round,
                    request_id,
                    observation,
                    call,
                    arguments,
                    ..
                } => {
                    resume_observed_tool(
                        inner,
                        request,
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
                        session_exchange_committed,
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

type RecoveredCommandProjection = (
    BTreeMap<CommandId, StoredCommand>,
    VecDeque<QueuedSteer>,
    BTreeMap<RequestId, RecoveredResolution>,
);

fn reconstruct_recovery_commands(
    records: &[crate::generic_agent_checkpoint::GenericCheckpointRecord],
    recovery_events: &[AgentEventDraft],
) -> Result<RecoveredCommandProjection, AgentProtocolError> {
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
        let GenericCheckpointEvent::CommandCommitted {
            command,
            outcome,
            approval_capability,
        } = &record.payload
        else {
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
                        RecoveredResolution {
                            command_id: command.command_id.clone(),
                            resolution: response.clone(),
                            capability: approval_capability.clone(),
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
    record_command_with_approval(inner, run, command, outcome, None)
}

fn record_command_with_approval(
    inner: &GenericInner,
    run: &mut GenericRun,
    command: &AgentCommandEnvelope,
    outcome: ProviderCommandOutcome,
    approval_capability: Option<ApprovalCapability>,
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
            approval_capability,
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

#[derive(Debug, Clone, Copy, Default)]
struct ModelContextBudget {
    remaining_input_tokens: Option<u64>,
    reserved_output_tokens: Option<u64>,
}

async fn project_model_context(
    inner: &GenericInner,
    request: &AgentStartRequest,
    model_definitions: &[ModelToolDefinition],
    run_skills: Option<&SkillRuntime>,
    initial_input: Option<ModelMessage>,
    through_session_seq: Option<u64>,
    budget: ModelContextBudget,
) -> Result<SessionContextProjection, SessionContextError> {
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
    // A cursor is a request to reproduce an earlier durable model boundary.
    // Replaying it must never append a new compaction fact to the Session.
    if through_session_seq.is_none() {
        if let Some(compactor) = &inner.session_compactor {
            compactor
                .compact_if_needed(&request.run.spec.session_id, &request.run.spec.run_id)
                .await?;
        }
    }
    let backend_context_limit = inner
        .backend
        .descriptor()
        .capabilities
        .max_context_tokens
        .unwrap_or(inner.config.max_context_tokens)
        .min(inner.config.max_context_tokens);
    let reserved_output_tokens = budget
        .reserved_output_tokens
        .unwrap_or(inner.config.reserved_output_tokens)
        .min(inner.config.reserved_output_tokens);
    let max_context_tokens = budget
        .remaining_input_tokens
        .or(request.run.spec.limits.max_input_tokens)
        .map(|limit| {
            limit
                .saturating_add(reserved_output_tokens)
                .min(backend_context_limit)
        })
        .unwrap_or(backend_context_limit);
    inner
        .context_engine
        .project(SessionContextRequest {
            session_id: request.run.spec.session_id.clone(),
            current_run_id: request.run.spec.run_id.clone(),
            through_session_seq,
            system_message: system_message_for_run(&inner.config, run_skills),
            tools: model_definitions.to_vec(),
            history_limit: inner.config.history_limit,
            max_context_tokens,
            reserved_output_tokens,
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
}

async fn project_model_messages(
    inner: &GenericInner,
    request: &AgentStartRequest,
    model_definitions: &[ModelToolDefinition],
    run_skills: Option<&SkillRuntime>,
    initial_input: Option<ModelMessage>,
    through_session_seq: Option<u64>,
    remaining_input_tokens: Option<u64>,
) -> Result<Vec<ModelMessage>, SessionContextError> {
    project_model_context(
        inner,
        request,
        model_definitions,
        run_skills,
        initial_input,
        through_session_seq,
        ModelContextBudget {
            remaining_input_tokens,
            reserved_output_tokens: None,
        },
    )
    .await
    .map(|projection| projection.messages)
}

async fn project_committed_model_messages(
    inner: &GenericInner,
    request: &AgentStartRequest,
    model_definitions: &[ModelToolDefinition],
    run_skills: Option<&SkillRuntime>,
) -> Result<Vec<ModelMessage>, AgentFailure> {
    project_model_messages(
        inner,
        request,
        model_definitions,
        run_skills,
        None,
        None,
        None,
    )
    .await
    .map_err(session_failure)
}

struct ModelRunExecution {
    inner: Arc<GenericInner>,
    request: AgentStartRequest,
    user_message: ModelMessage,
    model_messages: Vec<ModelMessage>,
    model_tools: Vec<ModelToolDefinition>,
    run_skills: Option<Arc<SkillRuntime>>,
    seed: GenericExecutionSeed,
    cancellation: CancellationToken,
    steer_updates: watch::Receiver<u64>,
}

async fn execute_model_run(execution: ModelRunExecution) {
    let ModelRunExecution {
        inner,
        request,
        user_message,
        mut model_messages,
        model_tools,
        run_skills,
        seed,
        cancellation,
        mut steer_updates,
    } = execution;
    let run_id = request.run.spec.run_id.clone();
    let GenericExecutionSeed {
        run_started,
        next_model_round,
        mut total_usage,
        mut tool_call_count,
        mut last_response,
        mut supporting_event_ids,
    } = seed;
    let started_event_id = AgentEventId::new(format!("generic-{}-started", run_id.as_str()));
    if !run_started
        && !publish_durable(
            &inner,
            &run_id,
            AgentEventDraft {
                event_id: started_event_id.clone(),
                run_id: run_id.clone(),
                causation_id: None,
                source_fingerprint: None,
                payload: AgentEvent::RunStarted,
            },
        )
    {
        return;
    }
    if !supporting_event_ids.contains(&started_event_id) {
        supporting_event_ids.push(started_event_id.clone());
    }
    if cancellation.is_cancelled() {
        emit_cancel(&inner, &request, &user_message);
        return;
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
        if cancellation.is_cancelled() {
            emit_cancel(&inner, &request, &user_message);
            return;
        }
        if let Err(failure) = commit_queued_steers(&inner, &request, &mut model_messages).await {
            emit_failure(&inner, &request, &user_message, failure);
            return;
        }
        let remaining_input = match remaining_input_tokens(&request, &total_usage) {
            Ok(remaining) => remaining,
            Err(limit) => {
                emit_limit_reached(
                    &inner,
                    &request,
                    last_response,
                    has_usage.then_some(total_usage),
                    tool_call_count,
                    started_event_id,
                    limit,
                );
                return;
            }
        };
        let output_reserve = match output_reserve_tokens(&inner.config, &request, &total_usage) {
            Ok(reserve) => reserve,
            Err(limit) => {
                emit_limit_reached(
                    &inner,
                    &request,
                    last_response,
                    has_usage.then_some(total_usage),
                    tool_call_count,
                    started_event_id,
                    limit,
                );
                return;
            }
        };
        let model_context = match project_model_context(
            &inner,
            &request,
            &model_tools,
            run_skills.as_deref(),
            None,
            None,
            ModelContextBudget {
                remaining_input_tokens: remaining_input,
                reserved_output_tokens: Some(inner.config.reserved_output_tokens),
            },
        )
        .await
        {
            Ok(context) => context,
            Err(SessionContextError::ContextOverflow { budget, .. })
                if remaining_input == Some(budget) =>
            {
                emit_limit_reached(
                    &inner,
                    &request,
                    last_response,
                    has_usage.then_some(total_usage),
                    tool_call_count,
                    started_event_id,
                    RunLimitKind::InputTokens,
                );
                return;
            }
            Err(error) => {
                emit_failure(&inner, &request, &user_message, session_failure(error));
                return;
            }
        };
        let dispatch_budget = match model_dispatch_budget(
            &inner.config,
            &request,
            &total_usage,
            model_context.used_input_tokens,
            output_reserve,
        ) {
            Ok(budget) => budget,
            Err(limit) => {
                emit_limit_reached(
                    &inner,
                    &request,
                    last_response,
                    has_usage.then_some(total_usage),
                    tool_call_count,
                    started_event_id,
                    limit,
                );
                return;
            }
        };
        let context_trace = model_context_trace(&model_context, inner.config.history_limit);
        model_messages = model_context.messages;
        let model_request = model_request_for_round(
            &request,
            round,
            &model_messages,
            &model_tools,
            dispatch_budget.max_output_tokens,
        );
        if let Err(failure) =
            commit_model_attempt(&inner, &run_id, round, &model_request, &context_trace)
        {
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
                ModelEvent::ToolCallStart {
                    call_id,
                    name,
                    extensions,
                } => {
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
                        extensions,
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
                                    extensions: call.extensions.clone(),
                                    ended: call.ended,
                                })
                                .collect(),
                        },
                    ) {
                        emit_failure(&inner, &request, &user_message, failure);
                        return;
                    }
                    if let Err(failure) = validate_observed_usage(
                        &inner.config,
                        &request,
                        &total_usage,
                        committed_usage.as_ref(),
                        dispatch_budget,
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
                                    if let Some(limit) = continuation_limit(
                                        &inner.config,
                                        &request,
                                        &total_usage,
                                        round,
                                        model_round_limit,
                                    ) {
                                        emit_limit_reached(
                                            &inner,
                                            &request,
                                            response,
                                            has_usage.then_some(total_usage),
                                            tool_call_count,
                                            started_event_id,
                                            limit,
                                        );
                                        return;
                                    }
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
                                DeliveryCommit::TerminationPending => {
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
                                IncompleteRun {
                                    response,
                                    usage: has_usage.then_some(total_usage),
                                    tool_calls: tool_call_count,
                                    started_event_id,
                                    limit: RunLimitKind::OutputTokens,
                                    unresolved_issue: "model output limit reached",
                                },
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

                    if let Some(limit) = continuation_limit(
                        &inner.config,
                        &request,
                        &total_usage,
                        round,
                        model_round_limit,
                    ) {
                        emit_limit_reached(
                            &inner,
                            &request,
                            response,
                            has_usage.then_some(total_usage),
                            tool_call_count,
                            started_event_id,
                            limit,
                        );
                        return;
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
                            extensions: call.extensions.clone(),
                        });
                        parsed_calls.push((call, arguments));
                    }
                    let assistant_message = ModelMessage {
                        role: ModelRole::Assistant,
                        content: assistant_content,
                    };

                    let mut tool_results = Vec::with_capacity(parsed_calls.len());
                    let mut retained_artifacts = BTreeMap::<String, ArtifactRefWithDigest>::new();
                    for (call, arguments) in parsed_calls {
                        if cancellation.is_cancelled() {
                            emit_cancel(&inner, &request, &user_message);
                            return;
                        }
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
                        tool_call_count = match reserve_tool_call(tool_call_count, tool_call_limit)
                        {
                            Ok(next) => next,
                            Err(limit) => {
                                emit_limit_reached(
                                    &inner,
                                    &request,
                                    last_response,
                                    has_usage.then_some(total_usage),
                                    tool_call_count,
                                    started_event_id,
                                    limit,
                                );
                                return;
                            }
                        };
                        if call.name == WORKFLOW_TOOL_NAME {
                            let remaining_tool_calls =
                                tool_call_limit.saturating_sub(tool_call_count);
                            if remaining_tool_calls == 0 {
                                emit_incomplete(
                                    &inner,
                                    &request,
                                    IncompleteRun {
                                        response: last_response,
                                        usage: has_usage.then_some(total_usage),
                                        tool_calls: tool_call_count,
                                        started_event_id,
                                        limit: RunLimitKind::ToolCalls,
                                        unresolved_issue:
                                            "Workflow has no remaining Tool call budget",
                                    },
                                );
                                return;
                            }
                            if let Err(failure) = commit_workflow_attempt_started(
                                &inner,
                                &run_id,
                                round,
                                &model_request.request_id,
                                &call.call_id,
                                &call.arguments,
                            ) {
                                emit_failure(&inner, &request, &user_message, failure);
                                return;
                            }
                            publish_tool_activity(
                                &inner,
                                &run_id,
                                round,
                                &call.call_id,
                                &call.name,
                                "running",
                            );
                            let observation = match execute_workflow_call(
                                inner.clone(),
                                tools,
                                WorkflowCallRequest {
                                    run_id: &run_id,
                                    call_id: &call.call_id,
                                    arguments,
                                    remaining_tool_calls,
                                    cancellation: cancellation.clone(),
                                    recovery_replay: false,
                                },
                            )
                            .await
                            {
                                WorkflowCallExecution::Observed(observation) => observation,
                                WorkflowCallExecution::Cancelled => {
                                    emit_cancel(&inner, &request, &user_message);
                                    return;
                                }
                                WorkflowCallExecution::UnknownEffect(message) => {
                                    if let Err(failure) = append_effect_uncertainty(
                                        &inner,
                                        &request,
                                        round,
                                        &call.call_id,
                                        WORKFLOW_TOOL_NAME,
                                        &message,
                                    )
                                    .await
                                    {
                                        emit_failure(&inner, &request, &user_message, failure);
                                        return;
                                    }
                                    emit_failure(
                                        &inner,
                                        &request,
                                        &user_message,
                                        agent_failure("tool_unknown_effect", message, false),
                                    );
                                    return;
                                }
                                WorkflowCallExecution::RecoveryFailed(failure) => {
                                    emit_failure(&inner, &request, &user_message, failure);
                                    return;
                                }
                            };
                            tool_call_count =
                                tool_call_count.saturating_add(observation.tool_calls);
                            publish_tool_activity(
                                &inner,
                                &run_id,
                                round,
                                &call.call_id,
                                &call.name,
                                if observation.is_error {
                                    "failed"
                                } else {
                                    "succeeded"
                                },
                            );
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
                        publish_tool_activity(
                            &inner,
                            &run_id,
                            round,
                            &call.call_id,
                            &call.name,
                            "running",
                        );
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
                                    ToolApprovalWaitRequest {
                                        run_id: &run_id,
                                        round,
                                        model_call_id: &call.call_id,
                                        binding,
                                        summary,
                                        cancellation: cancellation.clone(),
                                    },
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
                                publish_tool_activity(
                                    &inner,
                                    &run_id,
                                    round,
                                    &call.call_id,
                                    &call.name,
                                    "failed",
                                );
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
                                outcome: ToolOutcome::UnknownEffect { message },
                                ..
                            } if cancellation.is_cancelled() => {
                                publish_tool_activity(
                                    &inner,
                                    &run_id,
                                    round,
                                    &call.call_id,
                                    &call.name,
                                    "cancelled",
                                );
                                // The effect journal deliberately retains UnknownEffect, while
                                // the Agent Run still observes the user's cancellation as its
                                // terminal control outcome. A late Tool result is never accepted.
                                if let Err(failure) = append_effect_uncertainty(
                                    &inner,
                                    &request,
                                    round,
                                    &call.call_id,
                                    &call.name,
                                    &message,
                                )
                                .await
                                {
                                    emit_failure(&inner, &request, &user_message, failure);
                                    return;
                                }
                                emit_cancel(&inner, &request, &user_message);
                                return;
                            }
                            GuardedToolResult::Outcome {
                                outcome: ToolOutcome::UnknownEffect { message },
                                ..
                            } => {
                                publish_tool_activity(
                                    &inner,
                                    &run_id,
                                    round,
                                    &call.call_id,
                                    &call.name,
                                    "failed",
                                );
                                if let Err(failure) = append_effect_uncertainty(
                                    &inner,
                                    &request,
                                    round,
                                    &call.call_id,
                                    &call.name,
                                    &message,
                                )
                                .await
                                {
                                    emit_failure(&inner, &request, &user_message, failure);
                                    return;
                                }
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
                                publish_tool_activity(
                                    &inner,
                                    &run_id,
                                    round,
                                    &call.call_id,
                                    &call.name,
                                    "cancelled",
                                );
                                emit_cancel(&inner, &request, &user_message);
                                return;
                            }
                            GuardedToolResult::Outcome { outcome, .. } => {
                                for artifact in retained_artifacts_for_outcome(&outcome) {
                                    retained_artifacts.insert(
                                        artifact.artifact_ref.as_str().to_owned(),
                                        artifact,
                                    );
                                }
                                let (result, is_error) = model_tool_result(outcome);
                                publish_tool_activity(
                                    &inner,
                                    &run_id,
                                    round,
                                    &call.call_id,
                                    &call.name,
                                    if is_error { "failed" } else { "succeeded" },
                                );
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
                                retained_artifacts: retained_artifacts.into_values().collect(),
                                usage: committed_usage,
                            },
                        },
                    )
                    .await
                    {
                        emit_failure(&inner, &request, &user_message, failure);
                        return;
                    }
                    model_messages = match project_committed_model_messages(
                        &inner,
                        &request,
                        &model_tools,
                        run_skills.as_deref(),
                    )
                    .await
                    {
                        Ok(messages) => messages,
                        Err(failure) => {
                            emit_failure(&inner, &request, &user_message, failure);
                            return;
                        }
                    };
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
        IncompleteRun {
            response: last_response,
            usage: has_usage.then_some(total_usage),
            tool_calls: tool_call_count,
            started_event_id,
            limit: RunLimitKind::ModelSteps,
            unresolved_issue: "model step limit reached",
        },
    );
}

#[allow(clippy::too_many_arguments)]
async fn resume_observed_workflow_output(
    inner: Arc<GenericInner>,
    request: AgentStartRequest,
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
    outcome: WorkflowCallObservation,
    workflow_event_id: AgentEventId,
    session_exchange_committed: bool,
) {
    let run_id = request.run.spec.run_id.clone();
    if let Some(usage) = observation.usage.clone() {
        merge_usage(&mut seed.total_usage, usage);
    }
    if !observation.response.is_empty() {
        seed.last_response = observation.response.clone();
    }
    seed.tool_call_count = seed
        .tool_call_count
        .saturating_add(1)
        .saturating_add(outcome.tool_calls);
    if !seed.supporting_event_ids.contains(&workflow_event_id) {
        seed.supporting_event_ids.push(workflow_event_id);
    }
    if !session_exchange_committed {
        let (assistant_message, tool_message) = observed_tool_exchange_messages(
            &observation,
            &call,
            &arguments,
            &outcome.result,
            outcome.is_error,
        );
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
                    assistant: assistant_message,
                    tool: tool_message,
                    retained_artifacts: Vec::new(),
                    usage: observation.usage,
                },
            },
        )
        .await
        {
            emit_failure(&inner, &request, &user_message, failure);
            return;
        }
        model_messages = match project_committed_model_messages(
            &inner,
            &request,
            &model_tools,
            run_skills.as_deref(),
        )
        .await
        {
            Ok(messages) => messages,
            Err(failure) => {
                emit_failure(&inner, &request, &user_message, failure);
                return;
            }
        };
    }
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
    execute_model_run(ModelRunExecution {
        inner,
        request,
        user_message,
        model_messages,
        model_tools,
        run_skills,
        seed,
        cancellation,
        steer_updates,
    })
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn resume_observed_workflow(
    inner: Arc<GenericInner>,
    request: AgentStartRequest,
    user_message: ModelMessage,
    _model_messages: Vec<ModelMessage>,
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
    recovery_replay: bool,
) {
    let run_id = request.run.spec.run_id.clone();
    if let Some(usage) = observation.usage.clone() {
        merge_usage(&mut seed.total_usage, usage);
    }
    if !observation.response.is_empty() {
        seed.last_response = observation.response.clone();
    }
    let tool_call_limit = request
        .run
        .spec
        .limits
        .max_tool_calls
        .unwrap_or(inner.config.max_tool_calls)
        .min(inner.config.max_tool_calls);
    let has_usage =
        seed.total_usage.input_tokens.is_some() || seed.total_usage.output_tokens.is_some();
    let started_event_id = AgentEventId::new(format!("generic-{}-started", run_id.as_str()));
    seed.tool_call_count = match reserve_tool_call(seed.tool_call_count, tool_call_limit) {
        Ok(next) => next,
        Err(limit) => {
            emit_limit_reached(
                &inner,
                &request,
                seed.last_response,
                has_usage.then_some(seed.total_usage),
                seed.tool_call_count,
                started_event_id,
                limit,
            );
            return;
        }
    };
    let remaining_tool_calls = tool_call_limit.saturating_sub(seed.tool_call_count);
    if remaining_tool_calls == 0 {
        emit_incomplete(
            &inner,
            &request,
            IncompleteRun {
                response: seed.last_response,
                usage: has_usage.then_some(seed.total_usage),
                tool_calls: seed.tool_call_count,
                started_event_id,
                limit: RunLimitKind::ToolCalls,
                unresolved_issue: "Workflow has no remaining Tool call budget",
            },
        );
        return;
    }
    if let Err(failure) = commit_workflow_attempt_started(
        &inner,
        &run_id,
        round,
        &model_request_id,
        &call.call_id,
        &call.arguments,
    ) {
        emit_failure(&inner, &request, &user_message, failure);
        return;
    }
    let Some(tools) = inner.tools.as_ref() else {
        emit_failure(
            &inner,
            &request,
            &user_message,
            agent_failure(
                "tool_runtime_unavailable",
                "recovered Workflow has no Host Tool runtime",
                false,
            ),
        );
        return;
    };
    let workflow_observation = match execute_workflow_call(
        inner.clone(),
        tools,
        WorkflowCallRequest {
            run_id: &run_id,
            call_id: &call.call_id,
            arguments: arguments.clone(),
            remaining_tool_calls,
            cancellation: cancellation.clone(),
            recovery_replay,
        },
    )
    .await
    {
        WorkflowCallExecution::Observed(observation) => observation,
        WorkflowCallExecution::Cancelled => {
            emit_cancel(&inner, &request, &user_message);
            return;
        }
        WorkflowCallExecution::UnknownEffect(message) => {
            if let Err(failure) = append_effect_uncertainty(
                &inner,
                &request,
                round,
                &call.call_id,
                WORKFLOW_TOOL_NAME,
                &message,
            )
            .await
            {
                emit_failure(&inner, &request, &user_message, failure);
                return;
            }
            emit_failure(
                &inner,
                &request,
                &user_message,
                agent_failure("tool_unknown_effect", message, false),
            );
            return;
        }
        WorkflowCallExecution::RecoveryFailed(failure) => {
            emit_failure(&inner, &request, &user_message, failure);
            return;
        }
    };
    seed.tool_call_count = seed
        .tool_call_count
        .saturating_add(workflow_observation.tool_calls);
    let Some(workflow_event_id) = publish_workflow_output(
        &inner,
        &run_id,
        round,
        &call.call_id,
        workflow_observation.result.clone(),
    ) else {
        return;
    };
    seed.supporting_event_ids.push(workflow_event_id);
    let (assistant_message, tool_message) = observed_tool_exchange_messages(
        &observation,
        &call,
        &arguments,
        &workflow_observation.result,
        workflow_observation.is_error,
    );
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
                assistant: assistant_message,
                tool: tool_message,
                retained_artifacts: Vec::new(),
                usage: observation.usage,
            },
        },
    )
    .await
    {
        emit_failure(&inner, &request, &user_message, failure);
        return;
    }
    let model_messages = match project_committed_model_messages(
        &inner,
        &request,
        &model_tools,
        run_skills.as_deref(),
    )
    .await
    {
        Ok(messages) => messages,
        Err(failure) => {
            emit_failure(&inner, &request, &user_message, failure);
            return;
        }
    };
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
    execute_model_run(ModelRunExecution {
        inner,
        request,
        user_message,
        model_messages,
        model_tools,
        run_skills,
        seed,
        cancellation,
        steer_updates,
    })
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn resume_observed_skill(
    inner: Arc<GenericInner>,
    request: AgentStartRequest,
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
    recovered_observation: Option<SkillCallObservation>,
    session_exchange_committed: bool,
) {
    let run_id = request.run.spec.run_id.clone();
    if let Some(usage) = observation.usage.clone() {
        merge_usage(&mut seed.total_usage, usage);
    }
    if !observation.response.is_empty() {
        seed.last_response = observation.response.clone();
    }
    if session_exchange_committed {
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
        execute_model_run(ModelRunExecution {
            inner,
            request,
            user_message,
            model_messages,
            model_tools,
            run_skills,
            seed,
            cancellation,
            steer_updates,
        })
        .await;
        return;
    }
    let skill_observation = if let Some(observation) = recovered_observation {
        observation
    } else {
        let Some(skills) = run_skills.as_deref() else {
            emit_failure(
                &inner,
                &request,
                &user_message,
                agent_failure(
                    "skill_catalog_unavailable",
                    "recovered Skill activation has no bound Skill catalog",
                    false,
                ),
            );
            return;
        };
        match execute_skill_activation(
            &inner,
            &request,
            skills,
            round,
            &call.call_id,
            arguments.clone(),
        )
        .await
        {
            Ok(observation) => observation,
            Err(failure) => {
                emit_failure(&inner, &request, &user_message, failure);
                return;
            }
        }
    };
    let (assistant_message, tool_message) = observed_tool_exchange_messages(
        &observation,
        &call,
        &arguments,
        &skill_observation.result,
        skill_observation.is_error,
    );
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
                retained_artifacts: Vec::new(),
                usage: observation.usage,
            },
        },
    )
    .await
    {
        emit_failure(&inner, &request, &user_message, failure);
        return;
    }
    model_messages = match project_committed_model_messages(
        &inner,
        &request,
        &model_tools,
        run_skills.as_deref(),
    )
    .await
    {
        Ok(messages) => messages,
        Err(failure) => {
            emit_failure(&inner, &request, &user_message, failure);
            return;
        }
    };
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
    execute_model_run(ModelRunExecution {
        inner,
        request,
        user_message,
        model_messages,
        model_tools,
        run_skills,
        seed,
        cancellation,
        steer_updates,
    })
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn resume_observed_approval(
    inner: Arc<GenericInner>,
    request: AgentStartRequest,
    user_message: ModelMessage,
    model_messages: Vec<ModelMessage>,
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
    binding: ApprovalBinding,
    committed_response: Option<ApprovalResponse>,
    resolved_response: Option<ApprovalResponse>,
    session_exchange_committed: bool,
    response: Option<oneshot::Receiver<ApprovalResponse>>,
) {
    let run_id = request.run.spec.run_id.clone();
    let Some(tools) = inner.tools.as_ref() else {
        emit_failure(
            &inner,
            &request,
            &user_message,
            agent_failure(
                "tool_runtime_unavailable",
                "recovered approval has no Host Tool runtime",
                false,
            ),
        );
        return;
    };
    let Some(bridge) = tools.approval_bridge.clone() else {
        emit_failure(
            &inner,
            &request,
            &user_message,
            agent_failure(
                "approval_interaction_not_connected",
                "recovered approval has no Host approval bridge",
                false,
            ),
        );
        return;
    };
    if let Some(usage) = observation.usage.clone() {
        merge_usage(&mut seed.total_usage, usage);
    }
    if !observation.response.is_empty() {
        seed.last_response = observation.response.clone();
    }
    seed.tool_call_count = seed.tool_call_count.saturating_add(1);

    if session_exchange_committed {
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
        execute_model_run(ModelRunExecution {
            inner,
            request,
            user_message,
            model_messages,
            model_tools,
            run_skills,
            seed,
            cancellation,
            steer_updates,
        })
        .await;
        return;
    }

    let approval = if let Some(resolved_response) = resolved_response {
        approval_response_outcome(resolved_response)
    } else if let Some(committed_response) = committed_response {
        commit_approval_response(
            &inner,
            bridge.as_ref(),
            &run_id,
            round,
            &call.call_id,
            approval_request_id(&run_id, round, &call.call_id),
            committed_response,
        )
        .await
    } else {
        await_recovered_tool_approval(
            inner.clone(),
            bridge,
            &run_id,
            round,
            &call.call_id,
            response.expect("pending recovered approval owns a response channel"),
            cancellation.clone(),
        )
        .await
    };
    let invocation = ToolInvocation {
        run_id: run_id.clone(),
        call_id: ToolCallId::new(call.call_id.as_str()),
        tool_id: binding.tool_id.clone(),
        arguments: arguments.clone(),
    };
    let guarded = match approval {
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
    };
    continue_observed_tool(
        inner,
        request,
        user_message,
        model_messages,
        model_tools,
        run_skills,
        seed,
        cancellation,
        steer_updates,
        round,
        model_request_id,
        observation,
        call,
        arguments,
        guarded,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn resume_observed_tool(
    inner: Arc<GenericInner>,
    request: AgentStartRequest,
    user_message: ModelMessage,
    model_messages: Vec<ModelMessage>,
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
    session_exchange_committed: bool,
) {
    let run_id = request.run.spec.run_id.clone();
    let Some(tools) = inner.tools.as_ref() else {
        emit_failure(
            &inner,
            &request,
            &user_message,
            agent_failure(
                "tool_runtime_unavailable",
                "recovered Tool call has no Host Tool runtime",
                false,
            ),
        );
        return;
    };
    if let Some(usage) = observation.usage.clone() {
        merge_usage(&mut seed.total_usage, usage);
    }
    if !observation.response.is_empty() {
        seed.last_response = observation.response.clone();
    }
    seed.tool_call_count = seed.tool_call_count.saturating_add(1);

    if session_exchange_committed {
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
        execute_model_run(ModelRunExecution {
            inner,
            request,
            user_message,
            model_messages,
            model_tools,
            run_skills,
            seed,
            cancellation,
            steer_updates,
        })
        .await;
        return;
    }

    let prepared = match prepare_recovered_tool(
        &inner,
        &run_id,
        &call,
        &arguments,
        cancellation.clone(),
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(failure) => {
            emit_failure(&inner, &request, &user_message, failure);
            return;
        }
    };
    let guarded = match prepared {
        GuardedToolResult::ApprovalRequired { binding, summary } => {
            let invocation = ToolInvocation {
                run_id: run_id.clone(),
                call_id: ToolCallId::new(call.call_id.as_str()),
                tool_id: binding.tool_id.clone(),
                arguments: arguments.clone(),
            };
            match await_tool_approval(
                inner.clone(),
                tools,
                ToolApprovalWaitRequest {
                    run_id: &run_id,
                    round,
                    model_call_id: &call.call_id,
                    binding,
                    summary,
                    cancellation: cancellation.clone(),
                },
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
        outcome @ GuardedToolResult::Outcome { .. } => outcome,
    };
    continue_observed_tool(
        inner,
        request,
        user_message,
        model_messages,
        model_tools,
        run_skills,
        seed,
        cancellation,
        steer_updates,
        round,
        model_request_id,
        observation,
        call,
        arguments,
        guarded,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn continue_observed_tool(
    inner: Arc<GenericInner>,
    request: AgentStartRequest,
    user_message: ModelMessage,
    _model_messages: Vec<ModelMessage>,
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
    guarded: GuardedToolResult,
) {
    let run_id = request.run.spec.run_id.clone();
    let (result, is_error, retained_artifacts) = match guarded {
        GuardedToolResult::ApprovalRequired { binding, .. } => {
            emit_failure(
                &inner,
                &request,
                &user_message,
                AgentFailure {
                    code: "approval_capability_rejected".to_owned(),
                    message:
                        "Tool still requires approval after recovery resolved the exact request"
                            .to_owned(),
                    retryable: false,
                    details: serde_json::to_value(binding).unwrap_or(serde_json::Value::Null),
                },
            );
            return;
        }
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::UnknownEffect { message },
            ..
        } if cancellation.is_cancelled() => {
            if let Err(failure) = append_effect_uncertainty(
                &inner,
                &request,
                round,
                &call.call_id,
                &call.name,
                &message,
            )
            .await
            {
                emit_failure(&inner, &request, &user_message, failure);
                return;
            }
            emit_cancel(&inner, &request, &user_message);
            return;
        }
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::UnknownEffect { message },
            ..
        } => {
            if let Err(failure) = append_effect_uncertainty(
                &inner,
                &request,
                round,
                &call.call_id,
                &call.name,
                &message,
            )
            .await
            {
                emit_failure(&inner, &request, &user_message, failure);
                return;
            }
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
            let retained_artifacts = retained_artifacts_for_outcome(&outcome);
            let (result, is_error) = model_tool_result(outcome);
            (result, is_error, retained_artifacts)
        }
    };
    let (assistant_message, tool_message) =
        observed_tool_exchange_messages(&observation, &call, &arguments, &result, is_error);
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
                retained_artifacts,
                usage: observation.usage,
            },
        },
    )
    .await
    {
        emit_failure(&inner, &request, &user_message, failure);
        return;
    }
    let model_messages = match project_committed_model_messages(
        &inner,
        &request,
        &model_tools,
        run_skills.as_deref(),
    )
    .await
    {
        Ok(messages) => messages,
        Err(failure) => {
            emit_failure(&inner, &request, &user_message, failure);
            return;
        }
    };
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
    execute_model_run(ModelRunExecution {
        inner,
        request,
        user_message,
        model_messages,
        model_tools,
        run_skills,
        seed,
        cancellation,
        steer_updates,
    })
    .await;
}

async fn recovered_tool_exchange_record(
    inner: &GenericInner,
    request: &AgentStartRequest,
    round: u64,
    request_id: &ModelRequestId,
) -> Result<Option<AgentSessionRecord>, AgentProtocolError> {
    let records = inner
        .session_journal
        .load_session(&request.run.spec.session_id)
        .await
        .map_err(|error| session_context_recovery_error(SessionContextError::Journal(error)))?;
    recovered_tool_exchange_record_from(&records, request, round, request_id)
}

fn recovered_tool_exchange_record_from(
    records: &[AgentSessionRecord],
    request: &AgentStartRequest,
    round: u64,
    request_id: &ModelRequestId,
) -> Result<Option<AgentSessionRecord>, AgentProtocolError> {
    let mut matching = records
        .iter()
        .filter(|record| match &record.payload {
            AgentSessionEvent::ToolExchangeCommitted {
                request_id: actual, ..
            }
            | AgentSessionEvent::RunOutputCommitted {
                request_id: actual, ..
            } => actual == request_id,
            _ => false,
        })
        .cloned()
        .collect::<Vec<_>>();
    if matching.len() > 1 {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered model attempt has multiple Session outcomes",
        ));
    }
    let Some(record) = matching.pop() else {
        return Ok(None);
    };
    let expected_event_id = AgentSessionEventId::new(format!(
        "generic-{}-tool-exchange-{round}",
        request.run.spec.run_id.as_str()
    ));
    if record.run_id != request.run.spec.run_id
        || record.session_id != request.run.spec.session_id
        || record.event_id != expected_event_id
        || !matches!(
            &record.payload,
            AgentSessionEvent::ToolExchangeCommitted {
                request_id: actual,
                ..
            } if actual == request_id
        )
    {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered Session Tool exchange crossed its Run or model attempt",
        ));
    }
    Ok(Some(record))
}

async fn recovered_tool_exchange_committed(
    inner: &GenericInner,
    request: &AgentStartRequest,
    round: u64,
    request_id: &ModelRequestId,
    expected_messages: Option<&(ModelMessage, ModelMessage)>,
    retained_artifacts: &[ArtifactRefWithDigest],
    usage: Option<&ModelUsage>,
) -> Result<Option<u64>, AgentProtocolError> {
    let Some(record) = recovered_tool_exchange_record(inner, request, round, request_id).await?
    else {
        return Ok(None);
    };
    let Some((assistant, tool)) = expected_messages else {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "Session contains a Tool exchange without a recoverable durable result",
        ));
    };
    let expected_payload = AgentSessionEvent::ToolExchangeCommitted {
        request_id: request_id.clone(),
        assistant: assistant.clone(),
        tool: tool.clone(),
        retained_artifacts: retained_artifacts.to_vec(),
        usage: usage.cloned(),
    };
    if record.payload != expected_payload {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered Session Tool exchange does not match the private model observation",
        ));
    }
    Ok(Some(record.session_seq))
}

struct RecoveredSkillPreparation {
    observation: SkillCallObservation,
    activation_committed: bool,
    exchange_record: Option<AgentSessionRecord>,
    prior_session_seq: Option<u64>,
}

#[allow(clippy::too_many_arguments)]
async fn prepare_recovered_skill(
    inner: &GenericInner,
    request: &AgentStartRequest,
    skills: &SkillRuntime,
    round: u64,
    request_id: &ModelRequestId,
    observation: &GenericModelObservation,
    call: &GenericObservedToolCall,
    arguments: &serde_json::Value,
) -> Result<RecoveredSkillPreparation, AgentProtocolError> {
    let records = inner
        .session_journal
        .load_session(&request.run.spec.session_id)
        .await
        .map_err(|error| session_context_recovery_error(SessionContextError::Journal(error)))?;
    let expected_activation_id =
        skill_activation_event_id(&request.run.spec.run_id, round, &call.call_id);
    let activation_record = records
        .iter()
        .find(|record| record.event_id == expected_activation_id)
        .cloned();
    if activation_record.as_ref().is_some_and(|record| {
        record.run_id != request.run.spec.run_id
            || record.session_id != request.run.spec.session_id
            || !matches!(record.payload, AgentSessionEvent::SkillActivated { .. })
    }) {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered Skill activation event crossed its Run or has the wrong shape",
        ));
    }
    let exchange_record =
        recovered_tool_exchange_record_from(&records, request, round, request_id)?;
    if activation_record
        .as_ref()
        .zip(exchange_record.as_ref())
        .is_some_and(|(activation, exchange)| activation.session_seq >= exchange.session_seq)
    {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered Skill activation was not committed before its Tool exchange",
        ));
    }
    let final_outcome_seq = exchange_record
        .as_ref()
        .map(|record| record.session_seq)
        .or_else(|| activation_record.as_ref().map(|record| record.session_seq));
    if final_outcome_seq.is_some_and(|sequence| {
        records
            .last()
            .is_none_or(|record| record.session_seq != sequence)
    }) {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered Skill outcome is not the final Session record",
        ));
    }
    let first_outcome_seq = activation_record
        .as_ref()
        .map(|record| record.session_seq)
        .into_iter()
        .chain(exchange_record.as_ref().map(|record| record.session_seq))
        .min();
    let prior_records = records
        .iter()
        .filter(|record| first_outcome_seq.is_none_or(|first| record.session_seq < first))
        .cloned()
        .collect::<Vec<_>>();
    let active = ActivatedSkillSet::replay(&prior_records).map_err(|error| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            format!("recovered Skill state is invalid: {error}"),
        )
    })?;
    let evaluation = evaluate_skill_activation(skills, arguments.clone(), &active);
    match (&activation_record, &evaluation.activation) {
        (
            Some(AgentSessionRecord {
                payload: AgentSessionEvent::SkillActivated { activation },
                ..
            }),
            Some(expected),
        ) if activation.as_ref() == expected => {}
        (Some(_), _) => {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered Skill activation differs from the observed model call",
            ))
        }
        (None, Some(_)) if exchange_record.is_some() => {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered Skill Tool exchange is missing its activation event",
            ))
        }
        _ => {}
    }
    if let Some(record) = &exchange_record {
        let (assistant, tool) = observed_tool_exchange_messages(
            observation,
            call,
            arguments,
            &evaluation.observation.result,
            evaluation.observation.is_error,
        );
        let expected = AgentSessionEvent::ToolExchangeCommitted {
            request_id: request_id.clone(),
            assistant,
            tool,
            retained_artifacts: Vec::new(),
            usage: observation.usage.clone(),
        };
        if record.payload != expected {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered Skill Tool exchange differs from its activation outcome",
            ));
        }
    }
    Ok(RecoveredSkillPreparation {
        observation: evaluation.observation,
        activation_committed: activation_record.is_some(),
        exchange_record,
        prior_session_seq: first_outcome_seq.map(|sequence| sequence.saturating_sub(1)),
    })
}

#[allow(clippy::too_many_arguments)]
async fn resume_observed_input(
    inner: Arc<GenericInner>,
    request: AgentStartRequest,
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
    session_exchange_committed: bool,
    response: Option<oneshot::Receiver<InputResponse>>,
) {
    let run_id = request.run.spec.run_id.clone();
    if let Some(usage) = observation.usage.clone() {
        merge_usage(&mut seed.total_usage, usage);
    }
    if !observation.response.is_empty() {
        seed.last_response = observation.response.clone();
    }
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
    let (assistant_message, tool_message) =
        observed_input_exchange_messages(&observation, &call, &arguments, &result);
    if !session_exchange_committed {
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
                    retained_artifacts: Vec::new(),
                    usage: observation.usage,
                },
            },
        )
        .await
        {
            emit_failure(&inner, &request, &user_message, failure);
            return;
        }
        model_messages = match project_committed_model_messages(
            &inner,
            &request,
            &model_tools,
            run_skills.as_deref(),
        )
        .await
        {
            Ok(messages) => messages,
            Err(failure) => {
                emit_failure(&inner, &request, &user_message, failure);
                return;
            }
        };
    }
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
    execute_model_run(ModelRunExecution {
        inner,
        request,
        user_message,
        model_messages,
        model_tools,
        run_skills,
        seed,
        cancellation,
        steer_updates,
    })
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

fn approval_request_id(run_id: &RunId, round: u64, model_call_id: &ModelToolCallId) -> RequestId {
    RequestId::new(format!(
        "approval:{}:{round}:{}",
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
    match input_resolution_result(&resolution) {
        Ok(result) => InputWaitOutcome::Resolved(result),
        Err(failure) => InputWaitOutcome::Failed(failure),
    }
}

fn input_resolution_result(
    resolution: &RequestResolution,
) -> Result<serde_json::Value, AgentFailure> {
    match resolution {
        RequestResolution::Input { content } => Ok(serde_json::json!({
            "content": content,
        })),
        _ => Err(agent_failure(
            "input_resolution_invalid",
            "input request received a non-input resolution",
            false,
        )),
    }
}

fn observed_input_exchange_messages(
    observation: &GenericModelObservation,
    call: &GenericObservedToolCall,
    arguments: &serde_json::Value,
    result: &serde_json::Value,
) -> (ModelMessage, ModelMessage) {
    observed_tool_exchange_messages(observation, call, arguments, result, false)
}

fn observed_tool_exchange_messages(
    observation: &GenericModelObservation,
    call: &GenericObservedToolCall,
    arguments: &serde_json::Value,
    result: &serde_json::Value,
    is_error: bool,
) -> (ModelMessage, ModelMessage) {
    let mut assistant_content = Vec::new();
    if !observation.response.is_empty() {
        assistant_content.push(ModelContent::Text {
            text: observation.response.clone(),
        });
    }
    assistant_content.push(ModelContent::ToolCall {
        call_id: call.call_id.clone(),
        name: call.name.clone(),
        arguments: arguments.clone(),
        extensions: call.extensions.clone(),
    });
    (
        ModelMessage {
            role: ModelRole::Assistant,
            content: assistant_content,
        },
        ModelMessage {
            role: ModelRole::Tool,
            content: vec![ModelContent::ToolResult {
                call_id: call.call_id.clone(),
                result: result.clone(),
                is_error,
            }],
        },
    )
}

fn recovered_approval_exchange_messages(
    observation: &GenericModelObservation,
    call: &GenericObservedToolCall,
    arguments: &serde_json::Value,
    response: &ApprovalResponse,
    replayed_outcome: Option<&ToolOutcome>,
) -> Result<Option<(ModelMessage, ModelMessage)>, AgentProtocolError> {
    let outcome = match &response.resolution {
        RequestResolution::Approval {
            decision: ApprovalDecision::Allow,
            ..
        } => replayed_outcome.cloned(),
        RequestResolution::Approval {
            decision: ApprovalDecision::Deny,
            ..
        } => {
            if replayed_outcome.is_some() {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "denied approval unexpectedly has a durable Tool outcome",
                ));
            }
            Some(ToolOutcome::Rejected {
                code: "approval_denied".to_owned(),
                message: "Host denied this Tool invocation".to_owned(),
            })
        }
        _ => {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered approval exchange has a non-approval resolution",
            ))
        }
    };
    let Some(outcome) = outcome else {
        return Ok(None);
    };
    if matches!(outcome, ToolOutcome::UnknownEffect { .. }) {
        return Ok(None);
    }
    let (result, is_error) = model_tool_result(outcome);
    Ok(Some(observed_tool_exchange_messages(
        observation,
        call,
        arguments,
        &result,
        is_error,
    )))
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

struct ToolApprovalWaitRequest<'a> {
    run_id: &'a RunId,
    round: u64,
    model_call_id: &'a ModelToolCallId,
    binding: ApprovalBinding,
    summary: String,
    cancellation: CancellationToken,
}

async fn await_tool_approval(
    inner: Arc<GenericInner>,
    tools: &GenericTools,
    request: ToolApprovalWaitRequest<'_>,
) -> ApprovalWaitOutcome {
    let ToolApprovalWaitRequest {
        run_id,
        round,
        model_call_id,
        binding,
        summary,
        cancellation,
    } = request;
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
    let request_id = approval_request_id(run_id, round, model_call_id);
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
    commit_approval_response(
        &inner,
        bridge.as_ref(),
        run_id,
        round,
        model_call_id,
        request_id,
        response,
    )
    .await
}

async fn commit_approval_response(
    inner: &GenericInner,
    bridge: &dyn AgentApprovalBridge,
    run_id: &RunId,
    round: u64,
    model_call_id: &ModelToolCallId,
    request_id: RequestId,
    response: ApprovalResponse,
) -> ApprovalWaitOutcome {
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
        inner,
        run_id,
        AgentEventDraft {
            event_id: AgentEventId::new(format!(
                "generic-{}-approval-{round}-{}-resolved",
                run_id.as_str(),
                model_call_id.as_str()
            )),
            run_id: run_id.clone(),
            causation_id: Some(response.command_id.clone()),
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
    approval_response_outcome(response)
}

fn approval_response_outcome(response: ApprovalResponse) -> ApprovalWaitOutcome {
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

async fn await_recovered_tool_approval(
    inner: Arc<GenericInner>,
    bridge: Arc<dyn AgentApprovalBridge>,
    run_id: &RunId,
    round: u64,
    model_call_id: &ModelToolCallId,
    response: oneshot::Receiver<ApprovalResponse>,
    cancellation: CancellationToken,
) -> ApprovalWaitOutcome {
    let request_id = approval_request_id(run_id, round, model_call_id);
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
                "recovered approval response channel closed before resolution",
                true,
            ));
        }
    };
    commit_approval_response(
        &inner,
        bridge.as_ref(),
        run_id,
        round,
        model_call_id,
        request_id,
        response,
    )
    .await
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

#[derive(Clone)]
struct SkillCallObservation {
    result: serde_json::Value,
    is_error: bool,
    context_message: Option<ModelMessage>,
}

struct SkillActivationEvaluation {
    observation: SkillCallObservation,
    activation: Option<SkillActivation>,
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
    let records = inner
        .session_journal
        .load_session(&request.run.spec.session_id)
        .await
        .map_err(session_journal_failure)?;
    let active = ActivatedSkillSet::replay(&records)
        .map_err(|error| agent_failure("skill_session_state", error.to_string(), false))?;
    let evaluation = evaluate_skill_activation(skills, arguments, &active);
    if let Some(activation) = evaluation.activation {
        append_session_event(
            inner,
            AgentSessionEventDraft {
                event_id: skill_activation_event_id(&request.run.spec.run_id, round, call_id),
                session_id: request.run.spec.session_id.clone(),
                run_id: request.run.spec.run_id.clone(),
                payload: AgentSessionEvent::SkillActivated {
                    activation: Box::new(activation),
                },
            },
        )
        .await?;
    }
    Ok(evaluation.observation)
}

fn skill_activation_event_id(
    run_id: &RunId,
    round: u64,
    call_id: &ModelToolCallId,
) -> AgentSessionEventId {
    AgentSessionEventId::new(format!(
        "generic-{}-skill-{}-{}",
        run_id.as_str(),
        round,
        call_id.as_str()
    ))
}

fn evaluate_skill_activation(
    skills: &SkillRuntime,
    arguments: serde_json::Value,
    active: &ActivatedSkillSet,
) -> SkillActivationEvaluation {
    let parsed = match serde_json::from_value::<SkillActivateArguments>(arguments) {
        Ok(parsed) => parsed,
        Err(error) => {
            return SkillActivationEvaluation {
                observation: SkillCallObservation {
                    result: serde_json::json!({
                        "code": "skill_activation_arguments_invalid",
                        "message": error.to_string(),
                    }),
                    is_error: true,
                    context_message: None,
                },
                activation: None,
            }
        }
    };
    match skills.activate(
        SkillActivationRequest {
            name: parsed.name,
            expected_digest: parsed.expected_digest,
            reason: parsed.reason,
        },
        active,
    ) {
        Ok(SkillActivationOutcome::Activated(activation)) => {
            let descriptor = &activation.package.descriptor;
            SkillActivationEvaluation {
                observation: SkillCallObservation {
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
                },
                activation: Some(activation),
            }
        }
        Ok(SkillActivationOutcome::AlreadyActive(descriptor)) => SkillActivationEvaluation {
            observation: SkillCallObservation {
                result: serde_json::json!({
                    "status": "already_active",
                    "name": descriptor.name,
                    "skill_id": descriptor.skill_id,
                    "digest": descriptor.digest,
                }),
                is_error: false,
                context_message: None,
            },
            activation: None,
        },
        Err(error) => SkillActivationEvaluation {
            observation: SkillCallObservation {
                result: serde_json::json!({
                    "code": "skill_activation_rejected",
                    "message": error.to_string(),
                }),
                is_error: true,
                context_message: None,
            },
            activation: None,
        },
    }
}

struct PendingModelToolCall {
    call_id: ModelToolCallId,
    name: String,
    arguments: String,
    extensions: BTreeMap<String, serde_json::Value>,
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
    UnknownEffect(String),
    RecoveryFailed(AgentFailure),
}

struct WorkflowCallRequest<'a> {
    run_id: &'a RunId,
    call_id: &'a ModelToolCallId,
    arguments: serde_json::Value,
    remaining_tool_calls: u64,
    cancellation: CancellationToken,
    recovery_replay: bool,
}

async fn execute_workflow_call(
    inner: Arc<GenericInner>,
    tools: &GenericTools,
    call: WorkflowCallRequest<'_>,
) -> WorkflowCallExecution {
    let parsed = match serde_json::from_value::<WorkflowToolArguments>(call.arguments) {
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
    let workflow_id = WorkflowId::new(format!(
        "workflow:{}:{}",
        call.run_id.as_str(),
        call.call_id.as_str()
    ));
    let reporter = Arc::new(GenericWorkflowProgressReporter::new(
        inner,
        call.run_id.clone(),
        workflow_id.clone(),
        parsed.plan.steps.len(),
    ));
    let request = WorkflowExecutionRequest::new(
        call.run_id.clone(),
        workflow_id,
        parsed.plan,
        tools.run_grant.clone(),
    )
    .with_cancellation(call.cancellation.clone())
    .with_progress_reporter(reporter)
    .with_max_tool_calls(call.remaining_tool_calls);
    let request = if call.recovery_replay {
        request.with_recovery_replay()
    } else {
        request
    };
    let snapshot = match workflow.execute(request).await {
        Ok(snapshot) => snapshot,
        Err(crate::workflow_strategy::WorkflowExecutionError::UnknownEffect {
            message, ..
        }) => return WorkflowCallExecution::UnknownEffect(message),
        Err(error) if call.recovery_replay => {
            return WorkflowCallExecution::RecoveryFailed(agent_failure(
                "workflow_recovery",
                error.to_string(),
                false,
            ))
        }
        Err(error) => {
            return WorkflowCallExecution::Observed(WorkflowCallObservation {
                result: workflow_error("workflow_rejected", error.to_string()),
                is_error: true,
                tool_calls: 0,
            })
        }
    };
    if call.cancellation.is_cancelled() {
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
    let event_id = workflow_output_event_id(run_id, round, call_id);
    publish_durable(
        inner,
        run_id,
        AgentEventDraft {
            event_id: event_id.clone(),
            run_id: run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::OutputCommitted {
                output_id: workflow_output_id(run_id, round, call_id),
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

fn workflow_output_event_id(run_id: &RunId, round: u64, call_id: &ModelToolCallId) -> AgentEventId {
    AgentEventId::new(format!(
        "generic-{}-workflow-{round}-{}",
        run_id.as_str(),
        call_id.as_str()
    ))
}

fn workflow_output_id(run_id: &RunId, round: u64, call_id: &ModelToolCallId) -> OutputId {
    OutputId::new(format!(
        "generic-{}-workflow-{round}-{}",
        run_id.as_str(),
        call_id.as_str()
    ))
}

fn recovered_workflow_output(
    run_id: &RunId,
    round: u64,
    call_id: &ModelToolCallId,
    recovery_events: &[AgentEventDraft],
) -> Result<Option<(AgentEventId, WorkflowCallObservation)>, AgentProtocolError> {
    let expected_event_id = workflow_output_event_id(run_id, round, call_id);
    let Some(event) = recovery_events
        .iter()
        .find(|event| event.event_id == expected_event_id)
    else {
        return Ok(None);
    };
    let AgentEvent::OutputCommitted { output_id, content } = &event.payload else {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "durable Workflow output event has the wrong shape",
        ));
    };
    let [Content {
        media_type,
        schema_id,
        body: ContentBody::Inline(result),
    }] = content.as_slice()
    else {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "durable Workflow output must contain one inline JSON value",
        ));
    };
    if event.run_id != *run_id
        || output_id != &workflow_output_id(run_id, round, call_id)
        || media_type != "application/json"
        || schema_id.is_some()
    {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "durable Workflow output crossed its Run or output identity",
        ));
    }
    let status = result
        .get("status")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "durable Workflow output has no status",
            )
        })?;
    if !matches!(
        status,
        "completed" | "failed" | "waiting_user" | "waiting_event" | "rejected"
    ) {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "durable Workflow output has an unknown status",
        ));
    }
    let tool_calls = match result.get("tool_calls") {
        Some(value) => value.as_u64().ok_or_else(|| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "durable Workflow output has an invalid Tool call count",
            )
        })?,
        None if status == "rejected" => 0,
        None => {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "durable Workflow output has no Tool call count",
            ))
        }
    };
    if status == "rejected" && tool_calls != 0 {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "rejected Workflow output cannot report executed Tool calls",
        ));
    }
    Ok(Some((
        expected_event_id,
        WorkflowCallObservation {
            result: result.clone(),
            is_error: status != "completed",
            tool_calls,
        },
    )))
}

fn commit_workflow_attempt_started(
    inner: &GenericInner,
    run_id: &RunId,
    round: u64,
    request_id: &ModelRequestId,
    call_id: &ModelToolCallId,
    arguments: &str,
) -> Result<(), AgentFailure> {
    append_checkpoint(
        inner,
        run_id,
        GenericCheckpointEventId::new(format!(
            "generic-{}-workflow-started-{round}-{}",
            run_id.as_str(),
            call_id.as_str()
        )),
        GenericCheckpointEvent::WorkflowAttemptStarted {
            round,
            request_id: request_id.clone(),
            call_id: call_id.clone(),
            arguments_digest: Digest::sha256(arguments.as_bytes()),
        },
    )
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

fn retained_artifacts_for_outcome(outcome: &ToolOutcome) -> Vec<ArtifactRefWithDigest> {
    match outcome {
        ToolOutcome::Completed {
            output: ToolOutput::Artifact(artifact),
        } => vec![artifact.artifact.clone()],
        _ => Vec::new(),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ModelDispatchBudget {
    projected_input_tokens: u64,
    max_output_tokens: Option<u64>,
}

fn reserve_tool_call(current: u64, limit: u64) -> Result<u64, RunLimitKind> {
    (current < limit)
        .then(|| current.saturating_add(1))
        .ok_or(RunLimitKind::ToolCalls)
}

fn remaining_input_tokens(
    request: &AgentStartRequest,
    usage: &ModelUsage,
) -> Result<Option<u64>, RunLimitKind> {
    let Some(limit) = request.run.spec.limits.max_input_tokens else {
        return Ok(None);
    };
    let remaining = limit.saturating_sub(usage.input_tokens.unwrap_or(0));
    (remaining > 0)
        .then_some(Some(remaining))
        .ok_or(RunLimitKind::InputTokens)
}

fn output_reserve_tokens(
    config: &GenericAgentConfig,
    request: &AgentStartRequest,
    usage: &ModelUsage,
) -> Result<u64, RunLimitKind> {
    let remaining = request
        .run
        .spec
        .limits
        .max_output_tokens
        .map(|limit| limit.saturating_sub(usage.output_tokens.unwrap_or(0)));
    if remaining == Some(0) {
        return Err(RunLimitKind::OutputTokens);
    }
    Ok(remaining
        .unwrap_or(config.reserved_output_tokens)
        .min(config.reserved_output_tokens))
}

fn model_dispatch_budget(
    config: &GenericAgentConfig,
    request: &AgentStartRequest,
    usage: &ModelUsage,
    projected_input_tokens: u64,
    output_reserve_tokens: u64,
) -> Result<ModelDispatchBudget, RunLimitKind> {
    if request
        .run
        .spec
        .limits
        .max_input_tokens
        .is_some_and(|limit| {
            usage
                .input_tokens
                .unwrap_or(0)
                .saturating_add(projected_input_tokens)
                > limit
        })
    {
        return Err(RunLimitKind::InputTokens);
    }

    let mut output_cap = output_reserve_tokens;
    let mut bounded_output = request.run.spec.limits.max_output_tokens.is_some();
    if let Some(ceiling) = &request.run.spec.limits.max_cost {
        let policy = config
            .model_cost_policy
            .as_ref()
            .expect("cost limits are admitted only with a bound cost policy");
        let total_input = usage
            .input_tokens
            .unwrap_or(0)
            .saturating_add(projected_input_tokens);
        let total_output = usage.output_tokens.unwrap_or(0);
        let Some(allowed_output) = policy.max_output_tokens_within(
            total_input,
            total_output.saturating_add(output_cap),
            ceiling,
        ) else {
            return Err(RunLimitKind::Cost);
        };
        output_cap = allowed_output.saturating_sub(total_output).min(output_cap);
        if output_cap == 0 {
            return Err(RunLimitKind::Cost);
        }
        bounded_output = true;
    }

    Ok(ModelDispatchBudget {
        projected_input_tokens,
        max_output_tokens: bounded_output.then_some(output_cap),
    })
}

fn validate_observed_usage(
    config: &GenericAgentConfig,
    request: &AgentStartRequest,
    previous: &ModelUsage,
    observed: Option<&ModelUsage>,
    dispatch: ModelDispatchBudget,
) -> Result<(), AgentFailure> {
    let needs_input = request.run.spec.limits.max_input_tokens.is_some()
        || request.run.spec.limits.max_cost.as_ref().is_some_and(|_| {
            config
                .model_cost_policy
                .as_ref()
                .is_some_and(|policy| policy.input_microunits_per_million_tokens > 0)
        });
    let needs_output = request.run.spec.limits.max_output_tokens.is_some()
        || request.run.spec.limits.max_cost.as_ref().is_some_and(|_| {
            config
                .model_cost_policy
                .as_ref()
                .is_some_and(|policy| policy.output_microunits_per_million_tokens > 0)
        });
    let Some(observed) = observed else {
        if needs_input || needs_output {
            return Err(agent_failure(
                "model_usage_missing",
                "model usage is required to enforce the requested Run token or cost limit",
                false,
            ));
        }
        return Ok(());
    };
    if needs_input && observed.input_tokens.is_none() {
        return Err(agent_failure(
            "model_input_usage_missing",
            "model input usage is required to enforce the requested Run limit",
            false,
        ));
    }
    if needs_output && observed.output_tokens.is_none() {
        return Err(agent_failure(
            "model_output_usage_missing",
            "model output usage is required to enforce the requested Run limit",
            false,
        ));
    }
    if observed
        .input_tokens
        .is_some_and(|tokens| tokens > dispatch.projected_input_tokens)
    {
        return Err(agent_failure(
            "model_input_usage_exceeded_reservation",
            "model reported more input tokens than the bound token meter reserved",
            false,
        ));
    }
    if let Some(output_cap) = dispatch.max_output_tokens {
        if observed
            .output_tokens
            .is_some_and(|tokens| tokens > output_cap)
        {
            return Err(agent_failure(
                "model_output_usage_exceeded_reservation",
                "model reported more output tokens than the request budget allowed",
                false,
            ));
        }
    }

    let next_input = previous
        .input_tokens
        .unwrap_or(0)
        .saturating_add(observed.input_tokens.unwrap_or(0));
    let next_output = previous
        .output_tokens
        .unwrap_or(0)
        .saturating_add(observed.output_tokens.unwrap_or(0));
    if request
        .run
        .spec
        .limits
        .max_input_tokens
        .is_some_and(|limit| next_input > limit)
    {
        return Err(agent_failure(
            "model_input_limit_violated",
            "model input usage exceeded the immutable Run limit",
            false,
        ));
    }
    if request
        .run
        .spec
        .limits
        .max_output_tokens
        .is_some_and(|limit| next_output > limit)
    {
        return Err(agent_failure(
            "model_output_limit_violated",
            "model output usage exceeded the immutable Run limit",
            false,
        ));
    }
    if let Some(ceiling) = &request.run.spec.limits.max_cost {
        let actual = config
            .model_cost_policy
            .as_ref()
            .expect("cost limits are admitted only with a bound cost policy")
            .quote(next_input, next_output);
        if actual.currency != ceiling.currency || actual.microunits > ceiling.microunits {
            return Err(agent_failure(
                "model_cost_limit_violated",
                "model usage exceeded the immutable Run cost limit",
                false,
            ));
        }
    }
    Ok(())
}

fn exhausted_usage_limit(
    config: &GenericAgentConfig,
    request: &AgentStartRequest,
    usage: &ModelUsage,
) -> Option<RunLimitKind> {
    if request
        .run
        .spec
        .limits
        .max_input_tokens
        .is_some_and(|limit| usage.input_tokens.unwrap_or(0) >= limit)
    {
        return Some(RunLimitKind::InputTokens);
    }
    if request
        .run
        .spec
        .limits
        .max_output_tokens
        .is_some_and(|limit| usage.output_tokens.unwrap_or(0) >= limit)
    {
        return Some(RunLimitKind::OutputTokens);
    }
    if let (Some(ceiling), Some(policy)) = (
        request.run.spec.limits.max_cost.as_ref(),
        config.model_cost_policy.as_ref(),
    ) {
        let actual = policy.quote(
            usage.input_tokens.unwrap_or(0),
            usage.output_tokens.unwrap_or(0),
        );
        if actual.microunits >= ceiling.microunits {
            return Some(RunLimitKind::Cost);
        }
    }
    None
}

fn continuation_limit(
    config: &GenericAgentConfig,
    request: &AgentStartRequest,
    usage: &ModelUsage,
    completed_round: u64,
    model_round_limit: u64,
) -> Option<RunLimitKind> {
    if completed_round >= model_round_limit {
        return Some(RunLimitKind::ModelSteps);
    }
    exhausted_usage_limit(config, request, usage)
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
    context: &GenericModelContextTrace,
) -> Result<(), AgentFailure> {
    append_checkpoint(
        inner,
        run_id,
        GenericCheckpointEventId::new(format!("generic-{}-model-attempt-{round}", run_id.as_str())),
        GenericCheckpointEvent::ModelAttemptStarted {
            round,
            request_id: request.request_id.clone(),
            request_digest: model_request_digest(request)?,
            max_output_tokens: request.max_output_tokens,
            context: context.clone(),
        },
    )
}

fn model_context_trace(
    projection: &SessionContextProjection,
    history_limit: usize,
) -> GenericModelContextTrace {
    GenericModelContextTrace {
        through_session_seq: projection.through_session_seq,
        included_ranges: projection.included_ranges.clone(),
        deferred_ranges: projection.deferred_ranges.clone(),
        config_digest: projection.config_digest.clone(),
        history_limit,
        used_input_tokens: projection.used_input_tokens,
        input_budget_tokens: projection.input_budget_tokens,
    }
}

fn model_request_for_round(
    request: &AgentStartRequest,
    round: u64,
    messages: &[ModelMessage],
    tools: &[ModelToolDefinition],
    max_output_tokens: Option<u64>,
) -> ModelRequest {
    ModelRequest {
        request_id: ModelRequestId::new(format!(
            "model-{}-{round}",
            request.run.spec.run_id.as_str()
        )),
        messages: messages.to_vec(),
        tools: tools.to_vec(),
        output_schema: None,
        max_output_tokens,
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
    let outcome = inner
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
    if matches!(outcome, AppendGenericCheckpointOutcome::Appended) {
        run.checkpoint_seq = expected_previous.saturating_add(1);
    }
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
    if let Err(failure) =
        checkpoint_provider_events(inner, run, run_id, std::slice::from_ref(&draft))
    {
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

fn publish_tool_activity(
    inner: &GenericInner,
    run_id: &RunId,
    round: u64,
    call_id: &ModelToolCallId,
    tool_name: &str,
    status: &str,
) {
    let activity_id = format!(
        "generic-{}-tool-{round}-{}",
        run_id.as_str(),
        call_id.as_str()
    );
    let summary = match status {
        "running" => tool_name.to_owned(),
        "succeeded" => format!("{tool_name} completed"),
        "cancelled" => format!("{tool_name} cancelled"),
        _ => format!("{tool_name} failed"),
    };
    publish_telemetry(
        inner,
        run_id,
        AgentTelemetryEnvelope {
            telemetry_id: TelemetryId::new(format!("{activity_id}-{status}")),
            run_id: run_id.clone(),
            provider_seq: None,
            payload: AgentTelemetry::Extension {
                namespace: TOOL_ACTIVITY_TELEMETRY_NAMESPACE.to_owned(),
                value: serde_json::json!({
                    "activity_id": activity_id,
                    "tool_name": tool_name,
                    "summary": summary,
                    "status": status,
                }),
            },
        },
    );
}

enum DeliveryCommit {
    Committed,
    SteerPending,
    TerminationPending,
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
                usage: agent_usage(inner, usage, tool_calls),
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
    if run.stop_cause.load(Ordering::SeqCst) != RUN_STOP_RUNNING {
        return DeliveryCommit::TerminationPending;
    }
    if !run.queued_steers.is_empty() {
        return DeliveryCommit::SteerPending;
    }
    if run
        .stop_cause
        .compare_exchange(
            RUN_STOP_RUNNING,
            RUN_STOP_COMPLETING,
            Ordering::SeqCst,
            Ordering::SeqCst,
        )
        .is_err()
    {
        return DeliveryCommit::TerminationPending;
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

struct IncompleteRun {
    response: String,
    usage: Option<ModelUsage>,
    tool_calls: u64,
    started_event_id: AgentEventId,
    limit: RunLimitKind,
    unresolved_issue: &'static str,
}

fn emit_limit_reached(
    inner: &GenericInner,
    request: &AgentStartRequest,
    response: String,
    usage: Option<ModelUsage>,
    tool_calls: u64,
    started_event_id: AgentEventId,
    limit: RunLimitKind,
) {
    let unresolved_issue = match limit {
        RunLimitKind::Deadline => "Run deadline reached",
        RunLimitKind::ModelSteps => "model step limit reached",
        RunLimitKind::ToolCalls => "Tool call limit reached",
        RunLimitKind::InputTokens => "model input token limit reached",
        RunLimitKind::OutputTokens => "model output token limit reached",
        RunLimitKind::Cost => "model cost limit reached",
        _ => "Run limit reached",
    };
    emit_incomplete(
        inner,
        request,
        IncompleteRun {
            response,
            usage,
            tool_calls,
            started_event_id,
            limit,
            unresolved_issue,
        },
    );
}

fn emit_incomplete(inner: &GenericInner, request: &AgentStartRequest, incomplete: IncompleteRun) {
    let IncompleteRun {
        response,
        usage,
        tool_calls,
        started_event_id,
        limit,
        unresolved_issue,
    } = incomplete;
    let run_id = &request.run.spec.run_id;
    let partial_delivery = (!response.is_empty()).then(|| PartialDelivery {
        partial_delivery_id: PartialDeliveryId::new(format!("generic-{}-partial", run_id.as_str())),
        run_id: run_id.clone(),
        spec_digest: request.run.spec_digest.clone(),
        response: Some(Content::text(response)),
        outputs: Vec::new(),
        artifacts: Vec::new(),
        unresolved_issues: vec![unresolved_issue.to_owned()],
        usage: agent_usage(inner, usage, tool_calls),
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

fn current_unix_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn deadline_delay_ms(deadline_unix_ms: i64, now_unix_ms: i64) -> Option<u64> {
    let remaining_ms = deadline_unix_ms.saturating_sub(now_unix_ms);
    (remaining_ms > 0).then_some(remaining_ms as u64)
}

fn arm_run_deadline(
    deadline_unix_ms: Option<i64>,
    cancellation: CancellationToken,
    stop_cause: Arc<AtomicU8>,
) {
    let Some(deadline_unix_ms) = deadline_unix_ms else {
        return;
    };
    let Some(remaining_ms) = deadline_delay_ms(deadline_unix_ms, current_unix_ms()) else {
        if stop_cause
            .compare_exchange(
                RUN_STOP_RUNNING,
                RUN_STOP_DEADLINE,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
        {
            cancellation.cancel();
        }
        return;
    };
    tokio::spawn(async move {
        tokio::select! {
            _ = cancellation.cancelled() => {}
            _ = tokio::time::sleep(Duration::from_millis(remaining_ms)) => {
                if stop_cause
                    .compare_exchange(
                        RUN_STOP_RUNNING,
                        RUN_STOP_DEADLINE,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    cancellation.cancel();
                }
            }
        }
    });
}

fn emit_deadline_incomplete(inner: &GenericInner, request: &AgentStartRequest) {
    let run_id = &request.run.spec.run_id;
    if publish_durable(
        inner,
        run_id,
        AgentEventDraft {
            event_id: AgentEventId::new(format!("generic-{}-deadline", run_id.as_str())),
            run_id: run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RunIncomplete {
                reason: IncompleteReason::LimitReached {
                    limit: RunLimitKind::Deadline,
                },
                partial_delivery: None,
            },
        },
    ) {
        finish_session(inner, request);
    }
}

fn emit_cancel(inner: &GenericInner, request: &AgentStartRequest, user_message: &ModelMessage) {
    let run_id = &request.run.spec.run_id;
    let (stop_cause, cancel_command) = {
        let state = inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .runs
            .get(run_id)
            .map_or((RUN_STOP_RUNNING, None), |run| {
                (
                    run.stop_cause.load(Ordering::SeqCst),
                    run.cancel_command.clone(),
                )
            })
    };
    if stop_cause == RUN_STOP_DEADLINE {
        emit_deadline_incomplete(inner, request);
        return;
    }
    if stop_cause == RUN_STOP_HOST_CANCEL {
        let Some((command_id, reason)) = cancel_command else {
            emit_failure(
                inner,
                request,
                user_message,
                agent_failure(
                    "cancel_command_missing",
                    "Host cancellation won the termination race without a durable command",
                    true,
                ),
            );
            return;
        };
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
    let cancellation = state
        .runs
        .get(&request.run.spec.run_id)
        .map(|run| run.cancellation.clone());
    let session = state
        .sessions
        .entry(request.run.spec.session_id.clone())
        .or_default();
    if session.active_run.as_ref() == Some(&request.run.spec.run_id) {
        session.active_run = None;
    }
    drop(state);
    if let Some(cancellation) = cancellation {
        cancellation.cancel();
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

fn agent_usage(
    inner: &GenericInner,
    usage: Option<ModelUsage>,
    tool_calls: u64,
) -> Option<UsageReport> {
    if usage.is_none() && tool_calls == 0 {
        return None;
    }
    let usage = usage.unwrap_or_default();
    let cost = inner.config.model_cost_policy.as_ref().map(|policy| {
        policy.quote(
            usage.input_tokens.unwrap_or(0),
            usage.output_tokens.unwrap_or(0),
        )
    });
    Some(UsageReport {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        tool_calls: (tool_calls > 0).then_some(tool_calls),
        cost,
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
    token_meter: &ModelTokenMeterDescriptor,
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
        "token_meter": token_meter,
        "max_model_rounds": config.max_model_rounds,
        "max_tool_calls": config.max_tool_calls,
        "history_limit": config.history_limit,
        "max_context_tokens": config.max_context_tokens,
        "reserved_output_tokens": config.reserved_output_tokens,
        "model_cost_policy": config.model_cost_policy.as_ref().map(|policy| serde_json::json!({
            "currency": policy.currency,
            "input_microunits_per_million_tokens": policy.input_microunits_per_million_tokens,
            "output_microunits_per_million_tokens": policy.output_microunits_per_million_tokens,
        })),
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

fn bind_session_compaction_config_digest(
    base_config_digest: &Digest,
    policy: &SessionCompactionPolicy,
    summarizer: &SessionSummarizerDescriptor,
) -> Result<Digest, AgentProtocolError> {
    let value = serde_json::json!({
        "contract": "generic-agent-session-compaction/v1",
        "base_config_digest": base_config_digest,
        "policy": policy,
        "summarizer": summarizer,
    });
    let bytes = serde_jcs::to_vec(&value).map_err(|error| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidSpec,
            format!("could not bind Session compaction configuration: {error}"),
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

async fn append_effect_uncertainty(
    inner: &GenericInner,
    request: &AgentStartRequest,
    round: u64,
    model_call_id: &ModelToolCallId,
    tool_name: &str,
    message: &str,
) -> Result<(), AgentFailure> {
    append_session_event(
        inner,
        AgentSessionEventDraft {
            event_id: AgentSessionEventId::new(format!(
                "generic-{}-effect-uncertainty-{round}-{}",
                request.run.spec.run_id.as_str(),
                model_call_id.as_str()
            )),
            session_id: request.run.spec.session_id.clone(),
            run_id: request.run.spec.run_id.clone(),
            payload: AgentSessionEvent::EffectUncertaintyCommitted {
                effect_call_id: ToolCallId::new(model_call_id.as_str()),
                model_call_id: model_call_id.clone(),
                tool_name: tool_name.to_owned(),
                message: message.to_owned(),
            },
        },
    )
    .await
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

#[cfg(test)]
mod run_limit_tests {
    use super::*;
    use orchestral_core::agent_protocol::wire::{
        AgentRunEnvelope, AgentSessionId, ProviderBindingRef, RunLimits,
    };

    fn request() -> AgentStartRequest {
        let descriptor = AgentDescriptorEnvelope::seal(AgentDescriptor {
            provider_id: AgentProviderId::new("test/provider"),
            agent_id: AgentId::new("test/agent"),
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
                pending_request_kinds: BTreeSet::new(),
                supported_limits: BTreeSet::from([
                    RunLimitKind::Deadline,
                    RunLimitKind::ModelSteps,
                    RunLimitKind::ToolCalls,
                    RunLimitKind::InputTokens,
                    RunLimitKind::OutputTokens,
                    RunLimitKind::Cost,
                ]),
                resources: Vec::new(),
                effect_mediation: EffectMediation::None,
            },
            extensions: Default::default(),
        })
        .expect("test descriptor is valid");
        let run = AgentRunEnvelope::new(
            AGENT_PROTOCOL_V1,
            AgentSessionId::new("limit-session"),
            RunId::new("limit-run"),
            vec![Content::text("bounded request")],
        )
        .expect("test Run is valid");
        AgentStartRequest::new(run, ProviderBindingRef::new("limit-binding"), &descriptor)
            .expect("test start is valid")
    }

    #[test]
    fn one_thousand_boundaries_per_run_limit_never_reserve_past_the_ceiling() {
        let mut request = request();
        let mut config = GenericAgentConfig::new("test/provider", "test/agent");
        config.reserved_output_tokens = 10_000;
        config.model_cost_policy = Some(
            ModelCostPolicy::new("USD", 1_000_000, 1_000_000)
                .expect("linear test pricing is valid"),
        );

        for boundary in 1_u64..=1_000 {
            request.run.spec.limits = RunLimits {
                max_model_steps: Some(boundary),
                ..RunLimits::default()
            };
            assert_eq!(
                continuation_limit(
                    &config,
                    &request,
                    &ModelUsage::default(),
                    boundary.saturating_sub(1),
                    boundary,
                ),
                None
            );
            assert_eq!(
                continuation_limit(
                    &config,
                    &request,
                    &ModelUsage::default(),
                    boundary,
                    boundary,
                ),
                Some(RunLimitKind::ModelSteps)
            );

            assert_eq!(reserve_tool_call(boundary - 1, boundary), Ok(boundary));
            assert_eq!(
                reserve_tool_call(boundary, boundary),
                Err(RunLimitKind::ToolCalls)
            );

            let now = 1_000_000_i64;
            assert_eq!(
                deadline_delay_ms(now + boundary as i64, now),
                Some(boundary)
            );
            assert_eq!(deadline_delay_ms(now, now), None);

            let token_limit = boundary.saturating_mul(2);
            request.run.spec.limits = RunLimits {
                max_input_tokens: Some(token_limit),
                ..RunLimits::default()
            };
            let previous = ModelUsage {
                input_tokens: Some(boundary),
                output_tokens: None,
            };
            assert_eq!(
                remaining_input_tokens(&request, &previous),
                Ok(Some(boundary))
            );
            assert!(model_dispatch_budget(&config, &request, &previous, boundary, 1).is_ok());
            assert_eq!(
                model_dispatch_budget(&config, &request, &previous, boundary.saturating_add(1), 1,),
                Err(RunLimitKind::InputTokens)
            );

            request.run.spec.limits = RunLimits {
                max_output_tokens: Some(token_limit),
                ..RunLimits::default()
            };
            let previous = ModelUsage {
                input_tokens: None,
                output_tokens: Some(boundary),
            };
            assert_eq!(
                output_reserve_tokens(&config, &request, &previous),
                Ok(boundary)
            );
            let dispatch = model_dispatch_budget(&config, &request, &previous, 1, boundary)
                .expect("remaining output budget is reservable");
            assert_eq!(dispatch.max_output_tokens, Some(boundary));
            let exhausted = ModelUsage {
                input_tokens: None,
                output_tokens: Some(token_limit),
            };
            assert_eq!(
                output_reserve_tokens(&config, &request, &exhausted),
                Err(RunLimitKind::OutputTokens)
            );

            request.run.spec.limits = RunLimits {
                max_cost: Some(MoneyAmount {
                    currency: "USD".to_owned(),
                    microunits: boundary.saturating_mul(2).saturating_add(4),
                }),
                ..RunLimits::default()
            };
            let previous = ModelUsage {
                input_tokens: Some(boundary),
                output_tokens: Some(boundary),
            };
            let dispatch = model_dispatch_budget(&config, &request, &previous, 1, 16)
                .expect("cost ceiling admits the exact reservation");
            assert_eq!(dispatch.max_output_tokens, Some(3));
            assert!(validate_observed_usage(
                &config,
                &request,
                &previous,
                Some(&ModelUsage {
                    input_tokens: Some(1),
                    output_tokens: Some(3),
                }),
                dispatch,
            )
            .is_ok());
            request.run.spec.limits.max_cost = Some(MoneyAmount {
                currency: "USD".to_owned(),
                microunits: boundary.saturating_mul(2),
            });
            assert_eq!(
                model_dispatch_budget(&config, &request, &previous, 1, 16),
                Err(RunLimitKind::Cost)
            );
        }
    }
}
