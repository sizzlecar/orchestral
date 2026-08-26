//! Provider-neutral Agent Protocol v1 contracts.
//!
//! This module is the control-plane boundary for complete agents. It deliberately
//! does not expose model requests, prompts, plans, MCP transports, or provider
//! native session identifiers. A stateless model API belongs behind a model
//! backend and cannot implement [`spi::AgentProvider`] directly.

mod journal;
mod provider;
mod reducer;
mod types;

/// Serializable Agent Protocol v1 contract. This allowlist is intentionally
/// explicit: adding an internal helper type must not silently expand the wire
/// compatibility surface.
pub mod wire {
    pub use super::types::{
        AgentAdmission, AgentCapabilities, AgentCommand, AgentCommandEnvelope, AgentDelivery,
        AgentDescriptor, AgentDescriptorEnvelope, AgentEvent, AgentEventAuthority, AgentEventDraft,
        AgentEventEnvelope, AgentEventId, AgentExecutionRef, AgentFailure, AgentId,
        AgentJournalRecord, AgentProtocolError, AgentProtocolErrorCode, AgentProviderId,
        AgentProviderStreamItem, AgentRejection, AgentRejectionCode, AgentRunEnvelope,
        AgentRunSpec, AgentRunState, AgentRunView, AgentSessionId, AgentStartRequest,
        AgentTelemetry, AgentTelemetryEnvelope, AgentTerminalState, ApprovalDecision,
        ApprovalGrantRef, ArtifactRef, ArtifactRefWithDigest, BindingRequirement, CancelSupport,
        CommandAck, CommandAckState, CommandId, Content, ContentBody, ControlCapabilities,
        DeliveryId, Digest, EffectMediation, Extensions, IncompleteReason, MoneyAmount,
        NamedOutput, OutputId, PartialDelivery, PartialDeliveryId, PendingRequest,
        PendingRequestKind, PendingRequestPayload, ProtocolVersion, Provenance, ProviderBindingRef,
        ProviderCommandDisposition, ProviderCommandOutcome, ReconciliationProof,
        ReconciliationProofRef, RequestId, RequestResolution, ResourceBinding, ResourceBindingId,
        ResourceBindingMode, ResourceBindingSkip, ResourceBindingSkipCode, ResourceCapability,
        ResourceId, ResourceKind, ResourceRef, ResourceRevision, RunId, RunLimitKind, RunLimits,
        SchemaRef, TelemetryId, UsageReport,
    };
}

/// Rust SPI implemented by a complete in-process Agent or an opaque external
/// Agent adapter. Stream transport types live here; they are not JSON wire
/// snapshots themselves.
pub mod spi {
    pub use super::journal::{
        AgentJournalStore, AgentJournalStoreError, AgentRunRegistration, AppendAgentRecordOutcome,
        CreateAgentRunOutcome, InMemoryAgentJournalStore, StoredAgentRun,
    };
    pub use super::provider::{
        AgentProvider, AgentProviderStream, AgentRecovery, AgentRecoveryConfirmation,
        AgentRecoveryRequest, AgentStart, AgentStartError,
    };
    pub use super::types::AgentCompatibility;
}

/// Deterministic Host-side reference aggregate used by controllers and the
/// external conformance testkit. Its internal projection is not v1 wire API.
pub mod reference {
    pub use super::reducer::{
        AgentRunReducer, ApplyOutcome, ReconciliationProofVerifier, SequencedApply,
    };
    pub use super::types::{AgentContinuityState, AgentRunStatus};
}

/// Current Agent Protocol version implemented by this crate.
pub const AGENT_PROTOCOL_V1: wire::ProtocolVersion = wire::ProtocolVersion::new(1, 0);
