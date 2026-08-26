use async_trait::async_trait;
use futures_util::stream::BoxStream;

use super::types::{
    AgentAdmission, AgentCommandEnvelope, AgentDescriptorEnvelope, AgentExecutionRef,
    AgentProtocolError, AgentProviderStreamItem, AgentRejection, AgentStartRequest,
    ProviderCommandDisposition,
};

/// Unsequenced Provider observations plus best-effort telemetry. The Host
/// journal assigns normalized `run_seq` after validating each event draft.
pub type AgentProviderStream =
    BoxStream<'static, Result<AgentProviderStreamItem, AgentProtocolError>>;

/// Atomic result of `start`: the stream is established before native work may
/// publish events, avoiding a subscribe-after-complete race for no-replay agents.
pub struct AgentStart {
    pub execution: AgentExecutionRef,
    /// Descriptor admission decision. Every skipped binding must also appear as
    /// a durable `ResourceBindingSkipped` event before `RunStarted`.
    pub admission: AgentAdmission,
    pub stream: AgentProviderStream,
}

/// Immutable Host evidence supplied when reconnecting an existing logical Run.
///
/// A Provider may reattach native work or reconstruct its private state, but it
/// must preserve the same start/execution identity and must not duplicate
/// externally observable effects. The Host separately verifies the replayed
/// Provider event prefix before restoring continuity.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentRecoveryRequest {
    pub start_request: AgentStartRequest,
    pub execution: AgentExecutionRef,
}

impl AgentRecoveryRequest {
    pub fn new(
        start_request: AgentStartRequest,
        execution: AgentExecutionRef,
        descriptor: &AgentDescriptorEnvelope,
    ) -> Result<Self, AgentProtocolError> {
        execution.validate_for(&start_request, descriptor)?;
        Ok(Self {
            start_request,
            execution,
        })
    }

    pub fn validate_for(
        &self,
        descriptor: &AgentDescriptorEnvelope,
    ) -> Result<(), AgentProtocolError> {
        self.execution.validate_for(&self.start_request, descriptor)
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AgentStartError {
    /// The Provider guarantees no native execution was created.
    #[error(transparent)]
    Rejected(#[from] AgentRejection),
    /// A native start may have happened. Callers may only retry/reconcile the
    /// same run/spec/descriptor identity; they must not allocate a new Run.
    #[error("Agent start outcome is unknown: {0}")]
    OutcomeUnknown(AgentProtocolError),
}

/// Control-plane boundary for a complete Agent.
///
/// Implementations may wrap an in-process Generic Agent or an opaque external
/// Agent. Provider-native thread/session/turn handles must remain private to the
/// adapter and must never be used as [`AgentExecutionRef`].
#[async_trait]
pub trait AgentProvider: Send + Sync {
    /// Describe only capabilities that the implementation actually enforces.
    fn describe(&self) -> AgentDescriptorEnvelope;

    /// Idempotently start one immutable `AgentStartRequest` identity: Run,
    /// Session, spec digest, Provider binding, and descriptor digest.
    ///
    /// Repeating the same identity returns the existing execution. Reusing a
    /// `run_id` with any different identity component must return
    /// `RunIdConflict` and must not create provider work.
    async fn start(&self, request: AgentStartRequest) -> Result<AgentStart, AgentStartError>;

    /// Apply an idempotent command to an existing run.
    async fn command(
        &self,
        execution: &AgentExecutionRef,
        command: AgentCommandEnvelope,
    ) -> Result<ProviderCommandDisposition, AgentProtocolError>;

    /// Recover the Provider-native observation stream for an existing run.
    ///
    /// Provider-native cursors remain adapter-private. Normalized
    /// `events(after_run_seq)` replay belongs to the Host Agent journal, not
    /// this SPI. Replayed drafts must retain stable event IDs for exact dedupe.
    async fn recover(
        &self,
        request: AgentRecoveryRequest,
    ) -> Result<AgentProviderStream, AgentProtocolError>;
}
