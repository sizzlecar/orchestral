use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;

use async_trait::async_trait;
use futures_util::stream::BoxStream;

use super::types::{
    AgentAdmission, AgentCommandEnvelope, AgentDescriptorEnvelope, AgentEventDraft,
    AgentExecutionRef, AgentProtocolError, AgentProtocolErrorCode, AgentProviderStreamItem,
    AgentRejection, AgentStartRequest, ProviderCommandDisposition,
};

/// Unsequenced Provider observations plus best-effort telemetry. The Host
/// journal assigns normalized `run_seq` after validating each event draft.
pub type AgentProviderStream =
    BoxStream<'static, Result<AgentProviderStreamItem, AgentProtocolError>>;

/// One-shot Provider continuation that the Host polls only after recovery
/// evidence has been accepted and `ContinuityRestored` is durable.
///
/// A reconstructing Provider must keep all newly created model, Tool, and
/// native work quiescent until this future is polled. Dropping the future must
/// leave that work abandoned. A Provider that merely reattaches work which was
/// already running may use [`AgentRecovery::reattached`].
pub type AgentRecoveryConfirmation =
    Pin<Box<dyn Future<Output = Result<(), AgentProtocolError>> + Send + 'static>>;

/// Atomic first phase of recovery: a replay stream plus a one-shot continuation
/// gate. The Host validates the stream prefix before opening the gate.
pub struct AgentRecovery {
    stream: AgentProviderStream,
    confirmation: AgentRecoveryConfirmation,
}

impl AgentRecovery {
    /// Recovery for native work that was already running and is only being
    /// reattached. No newly reconstructed execution may be hidden behind this
    /// convenience constructor.
    pub fn reattached(stream: AgentProviderStream) -> Self {
        Self::staged(stream, async { Ok(()) })
    }

    /// Recovery whose newly reconstructed execution remains gated by
    /// `confirmation` until the Host has durably restored continuity.
    pub fn staged<F>(stream: AgentProviderStream, confirmation: F) -> Self
    where
        F: Future<Output = Result<(), AgentProtocolError>> + Send + 'static,
    {
        Self {
            stream,
            confirmation: Box::pin(confirmation),
        }
    }

    pub fn into_parts(self) -> (AgentProviderStream, AgentRecoveryConfirmation) {
        (self.stream, self.confirmation)
    }
}

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
/// externally observable effects. `committed_provider_prefix` is the exact,
/// Host-authenticated Provider prefix already present in the Run journal. It
/// lets a stateless adapter rebuild replay state without guessing from one
/// provider's private history format. The Host still independently verifies
/// the replayed prefix before restoring continuity.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentRecoveryRequest {
    pub start_request: AgentStartRequest,
    pub execution: AgentExecutionRef,
    pub committed_provider_prefix: Vec<AgentEventDraft>,
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
            committed_provider_prefix: Vec::new(),
        })
    }

    pub fn with_committed_provider_prefix(
        mut self,
        committed_provider_prefix: Vec<AgentEventDraft>,
    ) -> Result<Self, AgentProtocolError> {
        validate_committed_provider_prefix(&self.execution, committed_provider_prefix.as_slice())?;
        self.committed_provider_prefix = committed_provider_prefix;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        descriptor: &AgentDescriptorEnvelope,
    ) -> Result<(), AgentProtocolError> {
        self.execution
            .validate_for(&self.start_request, descriptor)?;
        validate_committed_provider_prefix(
            &self.execution,
            self.committed_provider_prefix.as_slice(),
        )
    }
}

fn validate_committed_provider_prefix(
    execution: &AgentExecutionRef,
    prefix: &[AgentEventDraft],
) -> Result<(), AgentProtocolError> {
    let mut event_ids = BTreeSet::new();
    for draft in prefix {
        draft.validate_integrity()?;
        if draft.run_id != execution.run_id {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "committed Provider recovery event belongs to another Run",
            ));
        }
        if !event_ids.insert(draft.event_id.as_str()) {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "committed Provider recovery prefix contains a duplicate event identity",
            ));
        }
    }
    Ok(())
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
    /// this SPI. Replayed native observation drafts must retain stable event
    /// IDs for exact dedupe. Synchronous `command` dispositions are validated
    /// and retained by the Host and are not part of this recovery stream.
    async fn recover(
        &self,
        request: AgentRecoveryRequest,
    ) -> Result<AgentRecovery, AgentProtocolError>;
}
