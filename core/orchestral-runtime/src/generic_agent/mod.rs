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
        TelemetryId, ToolActivityEvidence, ToolActivityId, ToolActivityState, UsageReport,
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
use orchestral_core::skill_protocol::SkillLoad;
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
use crate::skill::{LoadedSkillSet, SkillLoadOutcome, SkillRuntime};
use crate::tool_runtime::{AgentToolRuntime, GuardedToolResult, ToolRuntimeError};
use crate::workflow_strategy::{WorkflowExecutionRequest, WorkflowExecutionStrategy};
use crate::{
    AgentSessionCompactor, AgentSessionContextEngine, AgentSessionSummarizer, JsonSizeTokenMeter,
    ModelTokenMeter, ModelTokenMeterDescriptor, SessionCompactionPolicy, SessionContextError,
    SessionContextProjection, SessionContextRequest, SessionSummarizerDescriptor,
};

const WORKFLOW_TOOL_NAME: &str = "orchestral_workflow";
const SKILL_READ_TOOL_NAME: &str = "skill_read";
const REQUEST_INPUT_TOOL_NAME: &str = "orchestral_request_input";
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
    pub continuation: ContinuationPolicy,
    pub history_limit: usize,
    pub max_context_tokens: u64,
    pub reserved_output_tokens: u64,
    pub model_cost_policy: Option<ModelCostPolicy>,
}

/// Host ceiling for one continuous Agent turn.
///
/// An absent ceiling means that normal progress is not stopped by an arbitrary
/// number of model or Tool exchanges. Per-Run limits from Agent Protocol are
/// intersected with these Host ceilings when either side explicitly supplies
/// one. Deadline, token, cost, cancellation, and terminal-state checks remain
/// independent continuation boundaries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ContinuationPolicy {
    pub max_model_steps: Option<u64>,
    pub max_tool_calls: Option<u64>,
}

impl ContinuationPolicy {
    pub fn effective_model_steps(self, requested: Option<u64>) -> Option<u64> {
        intersect_limit(requested, self.max_model_steps)
    }

    pub fn effective_tool_calls(self, requested: Option<u64>) -> Option<u64> {
        intersect_limit(requested, self.max_tool_calls)
    }

    fn validate(self) -> Result<(), AgentProtocolError> {
        if self.max_model_steps == Some(0) || self.max_tool_calls == Some(0) {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "configured continuation ceilings must be positive when present",
            ));
        }
        Ok(())
    }
}

fn intersect_limit(requested: Option<u64>, host_ceiling: Option<u64>) -> Option<u64> {
    match (requested, host_ceiling) {
        (Some(requested), Some(host_ceiling)) => Some(requested.min(host_ceiling)),
        (Some(limit), None) | (None, Some(limit)) => Some(limit),
        (None, None) => None,
    }
}

impl GenericAgentConfig {
    pub fn new(provider_id: impl Into<String>, agent_id: impl Into<String>) -> Self {
        Self {
            provider_id: AgentProviderId::new(provider_id),
            agent_id: AgentId::new(agent_id),
            system_prompt: concat!(
                "You are Orchestral, a provider-neutral agent running in a local application. ",
                "You and the user share a Host-provided workspace. Work toward the user's ",
                "requested outcome using the supplied context and Tools. Tool definitions and ",
                "Host policy are authoritative capability boundaries. Inspect available ",
                "evidence before making claims, take relevant reversible actions when the ",
                "request is clear, and ask only when a material choice or required fact cannot ",
                "be derived. Treat explicit ordering, preconditions, and requested final states ",
                "as acceptance constraints: establish them before dependent work and verify ",
                "them before delivery. Do not broaden completed work with unrequested ",
                "integration, publication, cleanup, or reversal. ",
                "Prefer a dedicated Tool over a shell equivalent when one is available. For ",
                "workspace text changes, use file_write to create a file or intentionally ",
                "replace a complete file, and use apply_patch for targeted changes to existing ",
                "files. Inspect existing content before changing it and run relevant ",
                "verification. Permission is owned by the Host, not inferred by you. When an ",
                "exec_command needed for the user's request cannot run in the default sandbox, ",
                "request sandbox_permissions='require_escalated' with a concise justification ",
                "so the Host can apply policy or ask the user; do not offload the command to the ",
                "user merely because approval is needed. Treat every Tool failure as an observation to ",
                "correct or safely work around; report completion only from successful evidence."
            )
            .to_owned(),
            stream_buffer: 128,
            continuation: ContinuationPolicy::default(),
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

mod command;
mod coordinator;
mod provider;
mod provider_spi;
mod recovery_activate;
mod recovery_approval;
mod recovery_dispatch;
mod recovery_entry;
mod recovery_stage;
use recovery_activate::*;
use recovery_approval::*;
use recovery_dispatch::*;
use recovery_stage::*;
mod recovery_loop;
use recovery_loop::*;
mod recovery_projection;
use recovery_projection::*;
mod context;
use context::*;
mod model_step;
use model_step::*;
mod tool_step;
use tool_step::*;
mod recovery_resume;
mod recovery_tool;
use recovery_resume::*;
use recovery_tool::*;
mod control;
use control::*;
mod skills;
use skills::*;
mod workflow;
use workflow::*;
mod state_flow;
use state_flow::*;
mod completion;
use completion::*;
mod setup;
use setup::*;

#[cfg(test)]
mod tests;
