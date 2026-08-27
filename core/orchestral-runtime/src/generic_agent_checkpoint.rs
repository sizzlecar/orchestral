//! Provider-private write-ahead journal for the in-process Generic Agent.
//!
//! This journal is deliberately separate from the Host Agent journal. The
//! Host journal proves public protocol state; this journal proves whether the
//! Generic Agent may safely reconstruct private model-loop work after restart.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::RwLock;

use orchestral_core::agent_protocol::wire::{
    AgentAdmission, AgentCommandEnvelope, AgentEvent, AgentEventDraft, AgentEventId,
    AgentExecutionRef, AgentStartRequest, CommandId, Digest, ProviderCommandOutcome, RunId,
};
use orchestral_core::agent_session::SessionSourceRange;
use orchestral_core::model_protocol::{
    ModelFinishReason, ModelRequestId, ModelToolCallId, ModelUsage,
};
use orchestral_core::tool_protocol::ApprovalCapability;
use serde::{Deserialize, Serialize};

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn is_empty(&self) -> bool {
                self.0.trim().is_empty()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

string_id!(GenericCheckpointEventId);

/// Immutable Generic Agent identity bound before any model attempt begins.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenericAgentRunRegistration {
    pub request: AgentStartRequest,
    pub execution: AgentExecutionRef,
    pub admission: AgentAdmission,
    /// Binds the model contract, system prompt, Host Tool authority and
    /// schemas, Skill catalog, and loop limits that determine reconstructed
    /// execution.
    pub config_digest: Digest,
}

/// One model Tool call reconstructed from the canonical streaming protocol.
/// Raw argument bytes are retained so recovery can apply the same JSON/schema
/// validation without asking the model to repeat the call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenericObservedToolCall {
    pub call_id: ModelToolCallId,
    pub name: String,
    #[serde(default)]
    pub arguments: String,
    pub ended: bool,
}

/// Durable aggregate produced only after a valid terminal Model stream event
/// has been observed. It is Provider-private execution state, not a public
/// Agent event and not a claim that any Tool effect has occurred.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenericModelObservation {
    pub finish_reason: ModelFinishReason,
    #[serde(default)]
    pub response: String,
    #[serde(default)]
    pub usage: Option<ModelUsage>,
    #[serde(default)]
    pub tool_calls: Vec<GenericObservedToolCall>,
}

/// Durable provenance for the exact Session Context projected before one
/// model attempt. The request digest proves bytes; this trace explains which
/// Journal ranges and Host limits produced those bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenericModelContextTrace {
    pub through_session_seq: u64,
    pub included_ranges: Vec<SessionSourceRange>,
    pub deferred_ranges: Vec<SessionSourceRange>,
    pub config_digest: Digest,
    pub history_limit: usize,
    pub used_input_tokens: u64,
    pub input_budget_tokens: u64,
}

impl GenericModelContextTrace {
    fn validate(&self) -> Result<(), GenericCheckpointError> {
        if !self.config_digest.is_sha256()
            || self.history_limit == 0
            || self.input_budget_tokens == 0
            || self.used_input_tokens > self.input_budget_tokens
        {
            return Err(GenericCheckpointError::InvalidData(
                "model Context trace requires a config digest and valid Host limits".to_owned(),
            ));
        }
        let ranges = self
            .included_ranges
            .iter()
            .chain(self.deferred_ranges.iter())
            .collect::<Vec<_>>();
        for range in &ranges {
            range.validate().map_err(invalid_data)?;
            if range.last_session_seq > self.through_session_seq {
                return Err(GenericCheckpointError::InvalidData(
                    "model Context range exceeds its Session Journal cursor".to_owned(),
                ));
            }
        }
        for (index, left) in ranges.iter().enumerate() {
            if ranges.iter().skip(index + 1).any(|right| {
                left.first_session_seq <= right.last_session_seq
                    && right.first_session_seq <= left.last_session_seq
            }) {
                return Err(GenericCheckpointError::InvalidData(
                    "model Context trace ranges must not overlap".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

impl GenericModelObservation {
    fn validate(&self) -> Result<(), GenericCheckpointError> {
        let mut call_ids = BTreeSet::new();
        if self.tool_calls.iter().any(|call| {
            call.call_id.is_empty()
                || call.name.trim().is_empty()
                || !call_ids.insert(call.call_id.clone())
        }) {
            return Err(GenericCheckpointError::InvalidData(
                "model observation Tool calls require unique identities and names".to_owned(),
            ));
        }
        Ok(())
    }
}

impl GenericAgentRunRegistration {
    pub fn run_id(&self) -> &RunId {
        &self.execution.run_id
    }

    pub fn validate(&self) -> Result<(), GenericCheckpointError> {
        self.request
            .run
            .validate_integrity()
            .map_err(invalid_data)?;
        self.execution.validate_integrity().map_err(invalid_data)?;
        self.admission.validate_integrity().map_err(invalid_data)?;
        if !self.config_digest.is_sha256()
            || self.execution.run_id != self.request.run.spec.run_id
            || self.execution.session_id != self.request.run.spec.session_id
            || self.execution.spec_digest != self.request.run.spec_digest
            || self.execution.binding_ref != self.request.provider_binding
            || self.execution.descriptor_digest != self.request.expected_descriptor_digest
        {
            return Err(GenericCheckpointError::InvalidData(
                "Generic Agent registration identities or config digest do not agree".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Private facts required to distinguish a stable loop boundary from an
/// uncertain model attempt. `ModelAttemptStarted` is written before polling
/// `ModelBackend::start`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
// This is a versioned wire contract. Boxing individual fields would change the
// public Rust construction API without changing the serialized representation.
#[allow(clippy::large_enum_variant)]
pub enum GenericCheckpointEvent {
    LoopBoundaryCommitted {
        next_model_round: u64,
        #[serde(default)]
        usage: ModelUsage,
        tool_call_count: u64,
        #[serde(default)]
        last_response: String,
        #[serde(default)]
        supporting_event_ids: Vec<AgentEventId>,
    },
    ModelAttemptStarted {
        round: u64,
        request_id: ModelRequestId,
        request_digest: Digest,
        #[serde(default)]
        max_output_tokens: Option<u64>,
        context: GenericModelContextTrace,
    },
    ModelAttemptObserved {
        round: u64,
        request_id: ModelRequestId,
        observation: GenericModelObservation,
    },
    /// Written before entering the Workflow DAG executor. Once present, a
    /// missing durable Workflow output is intentionally outcome-unknown and
    /// must never be reconstructed by rerunning the DAG.
    WorkflowAttemptStarted {
        round: u64,
        request_id: ModelRequestId,
        call_id: ModelToolCallId,
        arguments_digest: Digest,
    },
    CommandCommitted {
        command: AgentCommandEnvelope,
        outcome: ProviderCommandOutcome,
        #[serde(default)]
        approval_capability: Option<ApprovalCapability>,
    },
    /// Provider drafts are persisted before they enter the live Provider
    /// stream. A delivery batch may contain OutputCommitted followed by the
    /// single terminal DeliveryCommitted event.
    ProviderEventsCommitted { events: Vec<AgentEventDraft> },
}

impl GenericCheckpointEvent {
    fn validate(&self, run_id: &RunId) -> Result<(), GenericCheckpointError> {
        match self {
            Self::LoopBoundaryCommitted {
                next_model_round,
                supporting_event_ids,
                ..
            } => {
                if *next_model_round == 0
                    || supporting_event_ids.iter().any(AgentEventId::is_empty)
                    || supporting_event_ids.iter().collect::<BTreeSet<_>>().len()
                        != supporting_event_ids.len()
                {
                    return Err(GenericCheckpointError::InvalidData(
                        "loop boundary requires a positive round and unique event references"
                            .to_owned(),
                    ));
                }
            }
            Self::ModelAttemptStarted {
                round,
                request_id,
                request_digest,
                max_output_tokens,
                context,
            } => {
                if *round == 0
                    || request_id.is_empty()
                    || !request_digest.is_sha256()
                    || *max_output_tokens == Some(0)
                {
                    return Err(GenericCheckpointError::InvalidData(
                        "model attempt requires a round, request identity, and digest".to_owned(),
                    ));
                }
                context.validate()?;
            }
            Self::ModelAttemptObserved {
                round,
                request_id,
                observation,
            } => {
                if *round == 0 || request_id.is_empty() {
                    return Err(GenericCheckpointError::InvalidData(
                        "model observation requires a round and request identity".to_owned(),
                    ));
                }
                observation.validate()?;
            }
            Self::WorkflowAttemptStarted {
                round,
                request_id,
                call_id,
                arguments_digest,
            } => {
                if *round == 0
                    || request_id.is_empty()
                    || call_id.is_empty()
                    || !arguments_digest.is_sha256()
                {
                    return Err(GenericCheckpointError::InvalidData(
                        "workflow attempt requires model, call, and argument identities".to_owned(),
                    ));
                }
            }
            Self::CommandCommitted {
                command,
                outcome,
                approval_capability,
            } => {
                command.verify_digest().map_err(invalid_data)?;
                outcome.validate_shape().map_err(invalid_data)?;
                if command.run_id != *run_id {
                    return Err(GenericCheckpointError::InvalidData(
                        "checkpoint command crossed a Run boundary".to_owned(),
                    ));
                }
                let accepted_allow = matches!(
                    (&command.payload, outcome),
                    (
                        orchestral_core::agent_protocol::wire::AgentCommand::ResolveRequest {
                            response: orchestral_core::agent_protocol::wire::RequestResolution::Approval {
                                decision: orchestral_core::agent_protocol::wire::ApprovalDecision::Allow,
                                ..
                            }
                        },
                        ProviderCommandOutcome::Accepted
                    )
                );
                if accepted_allow != approval_capability.is_some()
                    || approval_capability.as_ref().is_some_and(|capability| {
                        capability.claims.binding.run_id != *run_id
                            || !capability.authenticator.is_sha256()
                    })
                {
                    return Err(GenericCheckpointError::InvalidData(
                        "checkpoint approval capability does not match its accepted command"
                            .to_owned(),
                    ));
                }
            }
            Self::ProviderEventsCommitted { events } => {
                if events.is_empty() {
                    return Err(GenericCheckpointError::InvalidData(
                        "Provider event checkpoint batch must not be empty".to_owned(),
                    ));
                }
                let mut event_ids = BTreeSet::new();
                let mut terminal_seen = false;
                for (index, event) in events.iter().enumerate() {
                    event.validate_integrity().map_err(invalid_data)?;
                    if event.run_id != *run_id || !event_ids.insert(&event.event_id) {
                        return Err(GenericCheckpointError::InvalidData(
                            "Provider checkpoint events must be unique and Run-bound".to_owned(),
                        ));
                    }
                    let terminal = is_terminal_event(&event.payload);
                    if terminal_seen || (terminal && index + 1 != events.len()) {
                        return Err(GenericCheckpointError::InvalidData(
                            "terminal Provider event must be the final event in its batch"
                                .to_owned(),
                        ));
                    }
                    terminal_seen = terminal;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenericCheckpointDraft {
    pub event_id: GenericCheckpointEventId,
    pub run_id: RunId,
    pub payload: GenericCheckpointEvent,
}

impl GenericCheckpointDraft {
    pub fn validate(&self) -> Result<(), GenericCheckpointError> {
        if self.event_id.is_empty() || self.run_id.is_empty() {
            return Err(GenericCheckpointError::InvalidData(
                "Generic checkpoint identities must not be empty".to_owned(),
            ));
        }
        self.payload.validate(&self.run_id)
    }

    pub fn digest(&self) -> Result<Digest, GenericCheckpointError> {
        self.validate()?;
        canonical_digest(self)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenericCheckpointRecord {
    pub checkpoint_seq: u64,
    pub draft_digest: Digest,
    pub event_digest: Digest,
    pub event_id: GenericCheckpointEventId,
    pub run_id: RunId,
    pub payload: GenericCheckpointEvent,
}

#[derive(Serialize)]
struct GenericCheckpointRecordDigestView<'a> {
    checkpoint_seq: u64,
    draft_digest: &'a Digest,
    event_id: &'a GenericCheckpointEventId,
    run_id: &'a RunId,
    payload: &'a GenericCheckpointEvent,
}

impl GenericCheckpointRecord {
    /// Seals one validated private-WAL draft at the store-assigned sequence.
    /// Concrete checkpoint-store plugins use this after enforcing their
    /// compare-and-append boundary.
    pub fn seal(
        draft: GenericCheckpointDraft,
        checkpoint_seq: u64,
    ) -> Result<Self, GenericCheckpointError> {
        draft.validate()?;
        if checkpoint_seq == 0 {
            return Err(GenericCheckpointError::InvalidData(
                "checkpoint sequence must be positive".to_owned(),
            ));
        }
        let draft_digest = draft.digest()?;
        let mut record = Self {
            checkpoint_seq,
            draft_digest,
            event_digest: Digest::sha256([]),
            event_id: draft.event_id,
            run_id: draft.run_id,
            payload: draft.payload,
        };
        record.event_digest = record.computed_event_digest()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), GenericCheckpointError> {
        if self.checkpoint_seq == 0
            || self.event_id.is_empty()
            || self.run_id.is_empty()
            || !self.draft_digest.is_sha256()
            || self.computed_event_digest()? != self.event_digest
        {
            return Err(GenericCheckpointError::InvalidData(
                "Generic checkpoint record identity or digest is invalid".to_owned(),
            ));
        }
        self.payload.validate(&self.run_id)
    }

    fn computed_event_digest(&self) -> Result<Digest, GenericCheckpointError> {
        canonical_digest(&GenericCheckpointRecordDigestView {
            checkpoint_seq: self.checkpoint_seq,
            draft_digest: &self.draft_digest,
            event_id: &self.event_id,
            run_id: &self.run_id,
            payload: &self.payload,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredGenericAgentRun {
    pub registration: GenericAgentRunRegistration,
    pub records: Vec<GenericCheckpointRecord>,
}

impl StoredGenericAgentRun {
    pub fn validate(&self) -> Result<GenericAgentCheckpointProjection, GenericCheckpointError> {
        self.registration.validate()?;
        replay_generic_agent_checkpoint(self)
    }

    pub fn last_checkpoint_seq(&self) -> u64 {
        self.records
            .last()
            .map(|record| record.checkpoint_seq)
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GenericLoopBoundary {
    pub next_model_round: u64,
    pub usage: ModelUsage,
    pub tool_call_count: u64,
    pub last_response: String,
    pub supporting_event_ids: Vec<AgentEventId>,
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum GenericCheckpointPhase {
    Prepared,
    Stable(GenericLoopBoundary),
    ModelAttemptOpen {
        boundary: GenericLoopBoundary,
        round: u64,
        request_id: ModelRequestId,
        request_digest: Digest,
    },
    ModelAttemptObserved {
        boundary: GenericLoopBoundary,
        round: u64,
        request_id: ModelRequestId,
        request_digest: Digest,
        observation: GenericModelObservation,
    },
    WorkflowAttemptOpen {
        boundary: GenericLoopBoundary,
        round: u64,
        request_id: ModelRequestId,
        request_digest: Digest,
        observation: GenericModelObservation,
        call_id: ModelToolCallId,
        arguments_digest: Digest,
    },
    Terminal,
}

impl GenericCheckpointPhase {
    pub fn is_stable(&self) -> bool {
        matches!(self, Self::Stable(_))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GenericAgentCheckpointProjection {
    pub phase: GenericCheckpointPhase,
    pub provider_events: Vec<AgentEventDraft>,
    pub commands: BTreeMap<CommandId, CommandCheckpoint>,
    pub last_checkpoint_seq: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommandCheckpoint {
    pub command: AgentCommandEnvelope,
    pub outcome: ProviderCommandOutcome,
    pub approval_capability: Option<ApprovalCapability>,
}

pub fn replay_generic_agent_checkpoint(
    run: &StoredGenericAgentRun,
) -> Result<GenericAgentCheckpointProjection, GenericCheckpointError> {
    run.registration.validate()?;
    let run_id = run.registration.run_id();
    let mut phase = GenericCheckpointPhase::Prepared;
    let mut provider_events = Vec::new();
    let mut provider_event_digests = BTreeMap::<AgentEventId, Digest>::new();
    let mut commands = BTreeMap::<CommandId, CommandCheckpoint>::new();
    let mut checkpoint_ids = BTreeMap::<GenericCheckpointEventId, Digest>::new();

    for (index, record) in run.records.iter().enumerate() {
        record.validate()?;
        let expected_seq = index as u64 + 1;
        if record.run_id != *run_id || record.checkpoint_seq != expected_seq {
            return Err(GenericCheckpointError::InvalidData(format!(
                "Generic checkpoint sequence mismatch at {expected_seq}"
            )));
        }
        if let Some(existing) =
            checkpoint_ids.insert(record.event_id.clone(), record.draft_digest.clone())
        {
            return Err(if existing == record.draft_digest {
                GenericCheckpointError::InvalidData(
                    "stored Generic checkpoint contains a duplicate record".to_owned(),
                )
            } else {
                GenericCheckpointError::EventConflict(record.event_id.clone())
            });
        }
        if matches!(phase, GenericCheckpointPhase::Terminal) {
            return Err(GenericCheckpointError::InvalidData(
                "Generic checkpoint contains facts after terminal".to_owned(),
            ));
        }

        match &record.payload {
            GenericCheckpointEvent::LoopBoundaryCommitted {
                next_model_round,
                usage,
                tool_call_count,
                last_response,
                supporting_event_ids,
            } => {
                match &phase {
                    GenericCheckpointPhase::Prepared if *next_model_round == 1 => {}
                    GenericCheckpointPhase::ModelAttemptOpen { round, .. }
                    | GenericCheckpointPhase::ModelAttemptObserved { round, .. }
                    | GenericCheckpointPhase::WorkflowAttemptOpen { round, .. }
                        if *next_model_round > *round => {}
                    _ => {
                        return Err(GenericCheckpointError::InvalidData(
                            "loop boundary does not close the current checkpoint phase".to_owned(),
                        ))
                    }
                }
                phase = GenericCheckpointPhase::Stable(GenericLoopBoundary {
                    next_model_round: *next_model_round,
                    usage: usage.clone(),
                    tool_call_count: *tool_call_count,
                    last_response: last_response.clone(),
                    supporting_event_ids: supporting_event_ids.clone(),
                });
            }
            GenericCheckpointEvent::ModelAttemptStarted {
                round,
                request_id,
                request_digest,
                max_output_tokens: _,
                context: _,
            } => {
                let GenericCheckpointPhase::Stable(boundary) = &phase else {
                    return Err(GenericCheckpointError::InvalidData(
                        "model attempt did not begin at a stable loop boundary".to_owned(),
                    ));
                };
                if *round != boundary.next_model_round {
                    return Err(GenericCheckpointError::InvalidData(
                        "model attempt round does not match the stable boundary".to_owned(),
                    ));
                }
                phase = GenericCheckpointPhase::ModelAttemptOpen {
                    boundary: boundary.clone(),
                    round: *round,
                    request_id: request_id.clone(),
                    request_digest: request_digest.clone(),
                };
            }
            GenericCheckpointEvent::ModelAttemptObserved {
                round,
                request_id,
                observation,
            } => {
                let GenericCheckpointPhase::ModelAttemptOpen {
                    boundary,
                    round: open_round,
                    request_id: open_request_id,
                    request_digest,
                } = &phase
                else {
                    return Err(GenericCheckpointError::InvalidData(
                        "model observation did not close an open attempt".to_owned(),
                    ));
                };
                if round != open_round || request_id != open_request_id {
                    return Err(GenericCheckpointError::InvalidData(
                        "model observation identity does not match its open attempt".to_owned(),
                    ));
                }
                phase = GenericCheckpointPhase::ModelAttemptObserved {
                    boundary: boundary.clone(),
                    round: *round,
                    request_id: request_id.clone(),
                    request_digest: request_digest.clone(),
                    observation: observation.clone(),
                };
            }
            GenericCheckpointEvent::WorkflowAttemptStarted {
                round,
                request_id,
                call_id,
                arguments_digest,
            } => {
                let GenericCheckpointPhase::ModelAttemptObserved {
                    boundary,
                    round: observed_round,
                    request_id: observed_request_id,
                    request_digest,
                    observation,
                } = &phase
                else {
                    return Err(GenericCheckpointError::InvalidData(
                        "workflow attempt did not begin from an observed model call".to_owned(),
                    ));
                };
                let matching_call = observation.tool_calls.iter().find(|call| {
                    call.call_id == *call_id
                        && call.name == "orchestral_workflow"
                        && call.ended
                        && Digest::sha256(call.arguments.as_bytes()) == *arguments_digest
                });
                if round != observed_round
                    || request_id != observed_request_id
                    || matching_call.is_none()
                {
                    return Err(GenericCheckpointError::InvalidData(
                        "workflow attempt identity does not match its observed model call"
                            .to_owned(),
                    ));
                }
                phase = GenericCheckpointPhase::WorkflowAttemptOpen {
                    boundary: boundary.clone(),
                    round: *round,
                    request_id: request_id.clone(),
                    request_digest: request_digest.clone(),
                    observation: observation.clone(),
                    call_id: call_id.clone(),
                    arguments_digest: arguments_digest.clone(),
                };
            }
            GenericCheckpointEvent::CommandCommitted {
                command,
                outcome,
                approval_capability,
            } => {
                if let Some(existing) = commands.get(&command.command_id) {
                    if existing.command != *command
                        || existing.outcome != *outcome
                        || existing.approval_capability != *approval_capability
                    {
                        return Err(GenericCheckpointError::InvalidData(
                            "command identity was reused with different checkpoint content"
                                .to_owned(),
                        ));
                    }
                    return Err(GenericCheckpointError::InvalidData(
                        "stored Generic checkpoint contains a duplicate command".to_owned(),
                    ));
                }
                commands.insert(
                    command.command_id.clone(),
                    CommandCheckpoint {
                        command: command.clone(),
                        outcome: outcome.clone(),
                        approval_capability: approval_capability.clone(),
                    },
                );
            }
            GenericCheckpointEvent::ProviderEventsCommitted { events } => {
                for event in events {
                    let digest = event.computed_digest().map_err(invalid_data)?;
                    if let Some(existing) = provider_event_digests.get(&event.event_id) {
                        if existing != &digest {
                            return Err(GenericCheckpointError::InvalidData(
                                "Provider event identity was reused with different content"
                                    .to_owned(),
                            ));
                        }
                        return Err(GenericCheckpointError::InvalidData(
                            "stored Generic checkpoint contains a duplicate Provider event"
                                .to_owned(),
                        ));
                    }
                    provider_event_digests.insert(event.event_id.clone(), digest);
                    provider_events.push(event.clone());
                    if is_terminal_event(&event.payload) {
                        phase = GenericCheckpointPhase::Terminal;
                    }
                }
            }
        }
    }

    Ok(GenericAgentCheckpointProjection {
        phase,
        provider_events,
        commands,
        last_checkpoint_seq: run.last_checkpoint_seq(),
    })
}

fn is_terminal_event(event: &AgentEvent) -> bool {
    matches!(
        event,
        AgentEvent::DeliveryCommitted { .. }
            | AgentEvent::RunIncomplete { .. }
            | AgentEvent::RunFailed { .. }
            | AgentEvent::RunCancelled { .. }
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateGenericRunOutcome {
    Created,
    ExactExisting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendGenericCheckpointOutcome {
    Appended,
    ExactDuplicate,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum GenericCheckpointError {
    #[error("Generic Agent checkpoint storage is unavailable: {0}")]
    Unavailable(String),
    #[error("Generic Agent checkpoint Run does not exist: {0}")]
    RunNotFound(RunId),
    #[error("Generic Agent checkpoint Run conflicts with durable state: {0}")]
    RunConflict(RunId),
    #[error(
        "Generic Agent checkpoint sequence conflict for {run_id}: expected previous {expected_previous}, durable previous {actual_previous}"
    )]
    SequenceConflict {
        run_id: RunId,
        expected_previous: u64,
        actual_previous: u64,
    },
    #[error("Generic Agent checkpoint event identity conflict: {0}")]
    EventConflict(GenericCheckpointEventId),
    #[error("Generic Agent checkpoint data is invalid: {0}")]
    InvalidData(String),
}

/// Synchronous metadata WAL used on the Generic Agent's existing commit
/// critical sections. Implementations must keep records small and must not do
/// heavyweight parsing, embedding, or remote work in this call path.
pub trait GenericAgentCheckpointStore: Send + Sync {
    fn load_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<StoredGenericAgentRun>, GenericCheckpointError>;

    fn create_run(
        &self,
        registration: GenericAgentRunRegistration,
    ) -> Result<CreateGenericRunOutcome, GenericCheckpointError>;

    fn append(
        &self,
        run_id: &RunId,
        expected_previous: u64,
        draft: GenericCheckpointDraft,
    ) -> Result<AppendGenericCheckpointOutcome, GenericCheckpointError>;
}

#[derive(Default)]
pub struct InMemoryGenericAgentCheckpointStore {
    runs: RwLock<BTreeMap<RunId, StoredGenericAgentRun>>,
}

impl GenericAgentCheckpointStore for InMemoryGenericAgentCheckpointStore {
    fn load_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<StoredGenericAgentRun>, GenericCheckpointError> {
        let run = self
            .runs
            .read()
            .map_err(|_| GenericCheckpointError::Unavailable("reader lock poisoned".to_owned()))?
            .get(run_id)
            .cloned();
        if let Some(run) = &run {
            run.validate()?;
        }
        Ok(run)
    }

    fn create_run(
        &self,
        registration: GenericAgentRunRegistration,
    ) -> Result<CreateGenericRunOutcome, GenericCheckpointError> {
        registration.validate()?;
        let run_id = registration.run_id().clone();
        let mut runs = self
            .runs
            .write()
            .map_err(|_| GenericCheckpointError::Unavailable("writer lock poisoned".to_owned()))?;
        if let Some(existing) = runs.get(&run_id) {
            return if existing.registration == registration {
                Ok(CreateGenericRunOutcome::ExactExisting)
            } else {
                Err(GenericCheckpointError::RunConflict(run_id))
            };
        }
        runs.insert(
            run_id,
            StoredGenericAgentRun {
                registration,
                records: Vec::new(),
            },
        );
        Ok(CreateGenericRunOutcome::Created)
    }

    fn append(
        &self,
        run_id: &RunId,
        expected_previous: u64,
        draft: GenericCheckpointDraft,
    ) -> Result<AppendGenericCheckpointOutcome, GenericCheckpointError> {
        draft.validate()?;
        if draft.run_id != *run_id {
            return Err(GenericCheckpointError::InvalidData(
                "checkpoint append crossed a Run boundary".to_owned(),
            ));
        }
        let draft_digest = draft.digest()?;
        let mut runs = self
            .runs
            .write()
            .map_err(|_| GenericCheckpointError::Unavailable("writer lock poisoned".to_owned()))?;
        let run = runs
            .get_mut(run_id)
            .ok_or_else(|| GenericCheckpointError::RunNotFound(run_id.clone()))?;
        if let Some(existing) = run
            .records
            .iter()
            .find(|record| record.event_id == draft.event_id)
        {
            return if existing.draft_digest == draft_digest {
                Ok(AppendGenericCheckpointOutcome::ExactDuplicate)
            } else {
                Err(GenericCheckpointError::EventConflict(draft.event_id))
            };
        }
        let actual_previous = run.last_checkpoint_seq();
        if actual_previous != expected_previous {
            return Err(GenericCheckpointError::SequenceConflict {
                run_id: run_id.clone(),
                expected_previous,
                actual_previous,
            });
        }
        let record = GenericCheckpointRecord::seal(draft, actual_previous + 1)?;
        let mut candidate = run.clone();
        candidate.records.push(record.clone());
        candidate.validate()?;
        run.records.push(record);
        Ok(AppendGenericCheckpointOutcome::Appended)
    }
}

fn canonical_digest(value: &impl Serialize) -> Result<Digest, GenericCheckpointError> {
    serde_jcs::to_vec(value)
        .map(Digest::sha256)
        .map_err(|error| GenericCheckpointError::InvalidData(error.to_string()))
}

fn invalid_data(error: impl fmt::Display) -> GenericCheckpointError {
    GenericCheckpointError::InvalidData(error.to_string())
}

#[cfg(test)]
mod tests {
    use orchestral_core::agent_protocol::wire::{
        AgentDescriptor, AgentDescriptorEnvelope, AgentId, AgentProviderId, AgentRunEnvelope,
        AgentSessionId, Content, ProviderBindingRef,
    };
    use orchestral_core::agent_protocol::AGENT_PROTOCOL_V1;

    use super::*;

    fn registration() -> GenericAgentRunRegistration {
        let descriptor = AgentDescriptorEnvelope::seal(AgentDescriptor {
            provider_id: AgentProviderId::new("test/generic"),
            agent_id: AgentId::new("generic-v1"),
            supported_protocol_versions: vec![AGENT_PROTOCOL_V1],
            accepted_content_types: BTreeSet::from(["text/plain".to_owned()]),
            capabilities: Default::default(),
            extensions: Default::default(),
        })
        .unwrap();
        let run = AgentRunEnvelope::new(
            AGENT_PROTOCOL_V1,
            AgentSessionId::new("session-1"),
            RunId::new("run-1"),
            vec![Content::text("hello")],
        )
        .unwrap();
        let request =
            AgentStartRequest::new(run, ProviderBindingRef::new("binding-1"), &descriptor).unwrap();
        GenericAgentRunRegistration {
            execution: AgentExecutionRef::for_start(&request, &descriptor).unwrap(),
            request,
            admission: AgentAdmission::default(),
            config_digest: Digest::sha256("config-v1"),
        }
    }

    fn boundary(run_id: &RunId, next_model_round: u64) -> GenericCheckpointDraft {
        GenericCheckpointDraft {
            event_id: GenericCheckpointEventId::new(format!("boundary-{next_model_round}")),
            run_id: run_id.clone(),
            payload: GenericCheckpointEvent::LoopBoundaryCommitted {
                next_model_round,
                usage: ModelUsage::default(),
                tool_call_count: 0,
                last_response: String::new(),
                supporting_event_ids: Vec::new(),
            },
        }
    }

    fn context_trace() -> GenericModelContextTrace {
        GenericModelContextTrace {
            through_session_seq: 1,
            included_ranges: vec![SessionSourceRange {
                first_session_seq: 1,
                last_session_seq: 1,
            }],
            deferred_ranges: Vec::new(),
            config_digest: Digest::sha256("config-v1"),
            history_limit: 128,
            used_input_tokens: 10,
            input_budget_tokens: 100,
        }
    }

    #[test]
    fn model_context_trace_rejects_overlapping_or_over_budget_provenance() {
        let mut trace = context_trace();
        trace.deferred_ranges = trace.included_ranges.clone();
        assert!(trace.validate().is_err());

        let mut trace = context_trace();
        trace.used_input_tokens = trace.input_budget_tokens + 1;
        assert!(trace.validate().is_err());
    }

    #[test]
    fn stable_open_and_observed_model_boundaries_are_distinguishable_after_replay() {
        let store = InMemoryGenericAgentCheckpointStore::default();
        let registration = registration();
        let run_id = registration.run_id().clone();
        store.create_run(registration).unwrap();
        store.append(&run_id, 0, boundary(&run_id, 1)).unwrap();
        let stable = store.load_run(&run_id).unwrap().unwrap();
        assert!(stable.validate().unwrap().phase.is_stable());

        store
            .append(
                &run_id,
                1,
                GenericCheckpointDraft {
                    event_id: GenericCheckpointEventId::new("attempt-1"),
                    run_id: run_id.clone(),
                    payload: GenericCheckpointEvent::ModelAttemptStarted {
                        round: 1,
                        request_id: ModelRequestId::new("model-run-1-1"),
                        request_digest: Digest::sha256("request-1"),
                        max_output_tokens: None,
                        context: context_trace(),
                    },
                },
            )
            .unwrap();
        let uncertain = store.load_run(&run_id).unwrap().unwrap();
        assert!(matches!(
            uncertain.validate().unwrap().phase,
            GenericCheckpointPhase::ModelAttemptOpen { .. }
        ));

        store
            .append(
                &run_id,
                2,
                GenericCheckpointDraft {
                    event_id: GenericCheckpointEventId::new("observed-1"),
                    run_id: run_id.clone(),
                    payload: GenericCheckpointEvent::ModelAttemptObserved {
                        round: 1,
                        request_id: ModelRequestId::new("model-run-1-1"),
                        observation: GenericModelObservation {
                            finish_reason: ModelFinishReason::ToolCalls,
                            response: "calling a Tool".to_owned(),
                            usage: Some(ModelUsage {
                                input_tokens: Some(10),
                                output_tokens: Some(5),
                            }),
                            tool_calls: vec![GenericObservedToolCall {
                                call_id: ModelToolCallId::new("call-1"),
                                name: "echo".to_owned(),
                                arguments: r#"{"value":"hello"}"#.to_owned(),
                                ended: true,
                            }],
                        },
                    },
                },
            )
            .unwrap();
        let observed = store.load_run(&run_id).unwrap().unwrap();
        assert!(matches!(
            observed.validate().unwrap().phase,
            GenericCheckpointPhase::ModelAttemptObserved {
                round: 1,
                observation: GenericModelObservation { ref tool_calls, .. },
                ..
            } if tool_calls.len() == 1
        ));

        store.append(&run_id, 3, boundary(&run_id, 2)).unwrap();
        assert!(matches!(
            store
                .load_run(&run_id)
                .unwrap()
                .unwrap()
                .validate()
                .unwrap()
                .phase,
            GenericCheckpointPhase::Stable(GenericLoopBoundary {
                next_model_round: 2,
                ..
            })
        ));
    }

    #[test]
    fn invalid_transition_and_event_equivocation_never_advance_the_wal() {
        let store = InMemoryGenericAgentCheckpointStore::default();
        let registration = registration();
        let run_id = registration.run_id().clone();
        store.create_run(registration).unwrap();
        assert!(store.append(&run_id, 0, boundary(&run_id, 2)).is_err());
        assert_eq!(
            store
                .load_run(&run_id)
                .unwrap()
                .unwrap()
                .last_checkpoint_seq(),
            0
        );

        let original = boundary(&run_id, 1);
        store.append(&run_id, 0, original.clone()).unwrap();
        assert_eq!(
            store.append(&run_id, 1, original).unwrap(),
            AppendGenericCheckpointOutcome::ExactDuplicate
        );
        let mut conflict = boundary(&run_id, 2);
        conflict.event_id = GenericCheckpointEventId::new("boundary-1");
        assert!(matches!(
            store.append(&run_id, 1, conflict),
            Err(GenericCheckpointError::EventConflict(_))
        ));
        assert_eq!(
            store
                .load_run(&run_id)
                .unwrap()
                .unwrap()
                .last_checkpoint_seq(),
            1
        );
    }
}
