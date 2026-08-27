use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::{stream, StreamExt};
use orchestral_core::{
    agent_protocol::{
        reference::AgentRunStatus,
        spi::{AgentJournalStore, AgentProvider, AgentRecoveryRequest, InMemoryAgentJournalStore},
        wire::{
            AgentCommand, AgentCommandEnvelope, AgentEvent, AgentEventAuthority,
            AgentProtocolErrorCode, AgentProviderStreamItem, AgentRunEnvelope, AgentSessionId,
            AgentStartRequest, AgentTelemetry, ApprovalDecision, BindingRequirement,
            CommandAckState, CommandId, Content, ContentBody, IncompleteReason, PendingRequestKind,
            PendingRequestPayload, ProviderBindingRef, ProviderCommandOutcome, RequestResolution,
            ResourceBinding, ResourceBindingId, ResourceBindingMode, ResourceId, ResourceKind,
            ResourceRef, ResourceRevision, RunId, RunLimitKind,
        },
        AGENT_PROTOCOL_V1,
    },
    agent_session::{
        AgentSessionEvent, AgentSessionEventDraft, AgentSessionEventId, AgentSessionJournalStore,
        InMemoryAgentSessionJournalStore,
    },
    executor::Executor,
    model_protocol::{
        ModelBackend, ModelCapabilities, ModelContent, ModelDescriptor, ModelError, ModelEvent,
        ModelEventId, ModelFinishReason, ModelMessage, ModelRequest, ModelRequestId, ModelRole,
        ModelStream, ModelStreamEvent, ModelToolCallId,
    },
    normalizer::PlanNormalizer,
    skill_protocol::{
        SkillCompatibility, SkillDependencies, SkillId, SkillPackage, SkillSource, SkillSourceKind,
        SkillTrustLevel, SKILL_CATALOG_RESOURCE_KIND_V1,
    },
    tool_effect::InMemoryToolEffectJournalStore,
    tool_protocol::{
        ApprovalPolicy, EffectScope, HostApprovalVerifier, HostToolPolicy,
        InMemoryApprovalCapabilityStore, ModelToolSchema, RunToolGrant, ToolCallId,
        ToolConcurrency, ToolDescriptor, ToolId, ToolIdempotency, ToolInvocation, ToolOutcome,
        ToolPolicyBounds, ToolRestriction,
    },
};
use orchestral_runtime::{
    api::AgentApi, ActivatedSkillSet, AgentClient, AgentControlError, AgentControlEvent,
    AgentController, AppendGenericCheckpointOutcome, CreateGenericRunOutcome,
    GenericAgentCheckpointStore, GenericAgentConfig, GenericAgentRunRegistration,
    GenericCheckpointDraft, GenericCheckpointError, GenericCheckpointEvent, GenericCheckpointPhase,
    GuardedToolExecution, GuardedToolExecutor, GuardedToolResult, GuardedToolRuntime,
    InMemoryBlobStore, InMemoryGenericAgentCheckpointStore, InMemoryHostApprovalBroker,
    InternalGenericAgentProvider, JsonSizeTokenMeter, SessionCompactionInput,
    SessionCompactionPolicy, SessionContextError, SessionSummary, SkillActivationOutcome,
    SkillActivationPolicy, SkillActivationRequest, SkillHostProfile, SkillRuntime,
    StoredGenericAgentRun, ToolArtifactStore, WorkflowExecutionStrategy,
};
use serde_json::json;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

struct ScriptedModel;

struct BlockingModel;

struct CountingBlockingModel {
    starts: Arc<AtomicUsize>,
}

struct WalInspectingModel {
    checkpoint_store: Arc<InMemoryGenericAgentCheckpointStore>,
    run_id: RunId,
    observed_open_attempt: AtomicUsize,
}

struct RecoveryAfterRestoreModel {
    host_journal: Arc<InMemoryAgentJournalStore>,
    run_id: RunId,
    starts: AtomicUsize,
}

struct RecoveryIdentityModel {
    revision: &'static str,
    starts: Arc<AtomicUsize>,
}

struct PreparedRecoveryModel {
    starts: Arc<AtomicUsize>,
}

#[derive(Default)]
struct PausedAttemptState {
    paused_once: bool,
    release: bool,
    crashed: bool,
}

#[derive(Clone, Copy)]
enum CheckpointCrashCut {
    InitialBoundary,
    ModelAttempt,
    ModelObservation,
    InputRequestOpen,
    InputRequestResolve,
    InputToolExchangeBoundary,
    ApprovalRequestResolve,
    ApprovalToolExchangeBoundary,
    DirectToolExchangeBoundary,
}

struct PausingCheckpointStore {
    inner: InMemoryGenericAgentCheckpointStore,
    cut: CheckpointCrashCut,
    state: std::sync::Mutex<PausedAttemptState>,
    release: std::sync::Condvar,
    paused: Notify,
}

impl PausingCheckpointStore {
    fn at(cut: CheckpointCrashCut) -> Self {
        Self {
            inner: InMemoryGenericAgentCheckpointStore::default(),
            cut,
            state: std::sync::Mutex::new(PausedAttemptState::default()),
            release: std::sync::Condvar::new(),
            paused: Notify::new(),
        }
    }

    fn release_as_crash(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.release = true;
        self.release.notify_all();
    }

    fn allow_recovery_writes(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .crashed = false;
    }
}

#[derive(Default)]
struct FailingProviderEventCheckpointStore {
    inner: InMemoryGenericAgentCheckpointStore,
    provider_event_attempts: AtomicUsize,
}

#[derive(Default)]
struct FailingCommandCheckpointStore {
    inner: InMemoryGenericAgentCheckpointStore,
    command_attempts: AtomicUsize,
}

#[derive(Default)]
struct AckLostAfterInputOpenCheckpointStore {
    inner: InMemoryGenericAgentCheckpointStore,
    acknowledgement_lost: AtomicBool,
    unavailable: AtomicBool,
}

#[derive(Default)]
struct AckLostAfterInputResolveCheckpointStore {
    inner: InMemoryGenericAgentCheckpointStore,
    acknowledgement_lost: AtomicBool,
    unavailable: AtomicBool,
}

#[derive(Default)]
struct AckLostAfterApprovalOpenCheckpointStore {
    inner: InMemoryGenericAgentCheckpointStore,
    acknowledgement_lost: AtomicBool,
    unavailable: AtomicBool,
}

#[derive(Default)]
struct AckLostAfterApprovalResolveCheckpointStore {
    inner: InMemoryGenericAgentCheckpointStore,
    acknowledgement_lost: AtomicBool,
    unavailable: AtomicBool,
}

#[derive(Default)]
struct AckLostAfterModelObservationCheckpointStore {
    inner: InMemoryGenericAgentCheckpointStore,
    acknowledgement_lost: AtomicBool,
    unavailable: AtomicBool,
}

#[derive(Default)]
struct AckLostAfterWorkflowStartCheckpointStore {
    inner: InMemoryGenericAgentCheckpointStore,
    acknowledgement_lost: AtomicBool,
    unavailable: AtomicBool,
}

#[derive(Default)]
struct AckLostAfterWorkflowOutputCheckpointStore {
    inner: InMemoryGenericAgentCheckpointStore,
    acknowledgement_lost: AtomicBool,
    unavailable: AtomicBool,
}

impl AckLostAfterInputOpenCheckpointStore {
    fn allow_recovery_writes(&self) {
        self.unavailable.store(false, Ordering::SeqCst);
    }
}

impl AckLostAfterInputResolveCheckpointStore {
    fn allow_recovery_writes(&self) {
        self.unavailable.store(false, Ordering::SeqCst);
    }
}

impl AckLostAfterApprovalOpenCheckpointStore {
    fn allow_recovery_writes(&self) {
        self.unavailable.store(false, Ordering::SeqCst);
    }
}

impl AckLostAfterApprovalResolveCheckpointStore {
    fn allow_recovery_writes(&self) {
        self.unavailable.store(false, Ordering::SeqCst);
    }
}

impl AckLostAfterModelObservationCheckpointStore {
    fn allow_recovery_writes(&self) {
        self.unavailable.store(false, Ordering::SeqCst);
    }
}

impl AckLostAfterWorkflowStartCheckpointStore {
    fn allow_recovery_writes(&self) {
        self.unavailable.store(false, Ordering::SeqCst);
    }
}

impl AckLostAfterWorkflowOutputCheckpointStore {
    fn allow_recovery_writes(&self) {
        self.unavailable.store(false, Ordering::SeqCst);
    }
}

impl GenericAgentCheckpointStore for FailingProviderEventCheckpointStore {
    fn load_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<StoredGenericAgentRun>, GenericCheckpointError> {
        self.inner.load_run(run_id)
    }

    fn create_run(
        &self,
        registration: GenericAgentRunRegistration,
    ) -> Result<CreateGenericRunOutcome, GenericCheckpointError> {
        self.inner.create_run(registration)
    }

    fn append(
        &self,
        run_id: &RunId,
        expected_previous: u64,
        draft: GenericCheckpointDraft,
    ) -> Result<AppendGenericCheckpointOutcome, GenericCheckpointError> {
        if matches!(
            &draft.payload,
            GenericCheckpointEvent::ProviderEventsCommitted { .. }
        ) {
            self.provider_event_attempts.fetch_add(1, Ordering::SeqCst);
            return Err(GenericCheckpointError::Unavailable(
                "injected Provider event WAL failure".to_owned(),
            ));
        }
        self.inner.append(run_id, expected_previous, draft)
    }
}

impl GenericAgentCheckpointStore for FailingCommandCheckpointStore {
    fn load_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<StoredGenericAgentRun>, GenericCheckpointError> {
        self.inner.load_run(run_id)
    }

    fn create_run(
        &self,
        registration: GenericAgentRunRegistration,
    ) -> Result<CreateGenericRunOutcome, GenericCheckpointError> {
        self.inner.create_run(registration)
    }

    fn append(
        &self,
        run_id: &RunId,
        expected_previous: u64,
        draft: GenericCheckpointDraft,
    ) -> Result<AppendGenericCheckpointOutcome, GenericCheckpointError> {
        if matches!(
            &draft.payload,
            GenericCheckpointEvent::CommandCommitted { .. }
        ) {
            self.command_attempts.fetch_add(1, Ordering::SeqCst);
            return Err(GenericCheckpointError::Unavailable(
                "injected command WAL failure".to_owned(),
            ));
        }
        self.inner.append(run_id, expected_previous, draft)
    }
}

impl GenericAgentCheckpointStore for AckLostAfterInputOpenCheckpointStore {
    fn load_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<StoredGenericAgentRun>, GenericCheckpointError> {
        self.inner.load_run(run_id)
    }

    fn create_run(
        &self,
        registration: GenericAgentRunRegistration,
    ) -> Result<CreateGenericRunOutcome, GenericCheckpointError> {
        self.inner.create_run(registration)
    }

    fn append(
        &self,
        run_id: &RunId,
        expected_previous: u64,
        draft: GenericCheckpointDraft,
    ) -> Result<AppendGenericCheckpointOutcome, GenericCheckpointError> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(GenericCheckpointError::Unavailable(
                "simulated Provider process loss after durable input request".to_owned(),
            ));
        }
        let loses_acknowledgement = matches!(
            &draft.payload,
            GenericCheckpointEvent::ProviderEventsCommitted { events }
                if events.iter().any(|event| {
                    matches!(&event.payload, AgentEvent::RequestOpened { request }
                        if matches!(&request.payload, PendingRequestPayload::Input { .. }))
                })
        );
        let outcome = self.inner.append(run_id, expected_previous, draft)?;
        if loses_acknowledgement && !self.acknowledgement_lost.swap(true, Ordering::SeqCst) {
            self.unavailable.store(true, Ordering::SeqCst);
            return Err(GenericCheckpointError::Unavailable(
                "input RequestOpened commit acknowledgement was lost".to_owned(),
            ));
        }
        Ok(outcome)
    }
}

impl GenericAgentCheckpointStore for AckLostAfterInputResolveCheckpointStore {
    fn load_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<StoredGenericAgentRun>, GenericCheckpointError> {
        self.inner.load_run(run_id)
    }

    fn create_run(
        &self,
        registration: GenericAgentRunRegistration,
    ) -> Result<CreateGenericRunOutcome, GenericCheckpointError> {
        self.inner.create_run(registration)
    }

    fn append(
        &self,
        run_id: &RunId,
        expected_previous: u64,
        draft: GenericCheckpointDraft,
    ) -> Result<AppendGenericCheckpointOutcome, GenericCheckpointError> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(GenericCheckpointError::Unavailable(
                "simulated Provider process loss after durable input resolution".to_owned(),
            ));
        }
        let loses_acknowledgement = matches!(
            &draft.payload,
            GenericCheckpointEvent::ProviderEventsCommitted { events }
                if events.iter().any(|event| {
                    matches!(&event.payload, AgentEvent::RequestResolved { resolution, .. }
                        if matches!(resolution, RequestResolution::Input { .. }))
                })
        );
        let outcome = self.inner.append(run_id, expected_previous, draft)?;
        if loses_acknowledgement && !self.acknowledgement_lost.swap(true, Ordering::SeqCst) {
            self.unavailable.store(true, Ordering::SeqCst);
            return Err(GenericCheckpointError::Unavailable(
                "input RequestResolved commit acknowledgement was lost".to_owned(),
            ));
        }
        Ok(outcome)
    }
}

impl GenericAgentCheckpointStore for AckLostAfterApprovalOpenCheckpointStore {
    fn load_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<StoredGenericAgentRun>, GenericCheckpointError> {
        self.inner.load_run(run_id)
    }

    fn create_run(
        &self,
        registration: GenericAgentRunRegistration,
    ) -> Result<CreateGenericRunOutcome, GenericCheckpointError> {
        self.inner.create_run(registration)
    }

    fn append(
        &self,
        run_id: &RunId,
        expected_previous: u64,
        draft: GenericCheckpointDraft,
    ) -> Result<AppendGenericCheckpointOutcome, GenericCheckpointError> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(GenericCheckpointError::Unavailable(
                "simulated Provider process loss after durable approval request".to_owned(),
            ));
        }
        let loses_acknowledgement = matches!(
            &draft.payload,
            GenericCheckpointEvent::ProviderEventsCommitted { events }
                if events.iter().any(|event| {
                    matches!(&event.payload, AgentEvent::RequestOpened { request }
                        if matches!(&request.payload, PendingRequestPayload::Approval { .. }))
                })
        );
        let outcome = self.inner.append(run_id, expected_previous, draft)?;
        if loses_acknowledgement && !self.acknowledgement_lost.swap(true, Ordering::SeqCst) {
            self.unavailable.store(true, Ordering::SeqCst);
            return Err(GenericCheckpointError::Unavailable(
                "approval RequestOpened commit acknowledgement was lost".to_owned(),
            ));
        }
        Ok(outcome)
    }
}

impl GenericAgentCheckpointStore for AckLostAfterApprovalResolveCheckpointStore {
    fn load_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<StoredGenericAgentRun>, GenericCheckpointError> {
        self.inner.load_run(run_id)
    }

    fn create_run(
        &self,
        registration: GenericAgentRunRegistration,
    ) -> Result<CreateGenericRunOutcome, GenericCheckpointError> {
        self.inner.create_run(registration)
    }

    fn append(
        &self,
        run_id: &RunId,
        expected_previous: u64,
        draft: GenericCheckpointDraft,
    ) -> Result<AppendGenericCheckpointOutcome, GenericCheckpointError> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(GenericCheckpointError::Unavailable(
                "simulated Provider process loss after durable approval resolution".to_owned(),
            ));
        }
        let loses_acknowledgement = matches!(
            &draft.payload,
            GenericCheckpointEvent::ProviderEventsCommitted { events }
                if events.iter().any(|event| {
                    matches!(&event.payload, AgentEvent::RequestResolved { resolution, .. }
                        if matches!(resolution, RequestResolution::Approval { .. }))
                })
        );
        let outcome = self.inner.append(run_id, expected_previous, draft)?;
        if loses_acknowledgement && !self.acknowledgement_lost.swap(true, Ordering::SeqCst) {
            self.unavailable.store(true, Ordering::SeqCst);
            return Err(GenericCheckpointError::Unavailable(
                "approval RequestResolved commit acknowledgement was lost".to_owned(),
            ));
        }
        Ok(outcome)
    }
}

impl GenericAgentCheckpointStore for AckLostAfterModelObservationCheckpointStore {
    fn load_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<StoredGenericAgentRun>, GenericCheckpointError> {
        self.inner.load_run(run_id)
    }

    fn create_run(
        &self,
        registration: GenericAgentRunRegistration,
    ) -> Result<CreateGenericRunOutcome, GenericCheckpointError> {
        self.inner.create_run(registration)
    }

    fn append(
        &self,
        run_id: &RunId,
        expected_previous: u64,
        draft: GenericCheckpointDraft,
    ) -> Result<AppendGenericCheckpointOutcome, GenericCheckpointError> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(GenericCheckpointError::Unavailable(
                "simulated Provider process loss after durable model observation".to_owned(),
            ));
        }
        let loses_acknowledgement = matches!(
            &draft.payload,
            GenericCheckpointEvent::ModelAttemptObserved { .. }
        );
        let outcome = self.inner.append(run_id, expected_previous, draft)?;
        if loses_acknowledgement && !self.acknowledgement_lost.swap(true, Ordering::SeqCst) {
            self.unavailable.store(true, Ordering::SeqCst);
            return Err(GenericCheckpointError::Unavailable(
                "model observation commit acknowledgement was lost".to_owned(),
            ));
        }
        Ok(outcome)
    }
}

impl GenericAgentCheckpointStore for AckLostAfterWorkflowStartCheckpointStore {
    fn load_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<StoredGenericAgentRun>, GenericCheckpointError> {
        self.inner.load_run(run_id)
    }

    fn create_run(
        &self,
        registration: GenericAgentRunRegistration,
    ) -> Result<CreateGenericRunOutcome, GenericCheckpointError> {
        self.inner.create_run(registration)
    }

    fn append(
        &self,
        run_id: &RunId,
        expected_previous: u64,
        draft: GenericCheckpointDraft,
    ) -> Result<AppendGenericCheckpointOutcome, GenericCheckpointError> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(GenericCheckpointError::Unavailable(
                "simulated Provider process loss after durable Workflow start".to_owned(),
            ));
        }
        let loses_acknowledgement = matches!(
            &draft.payload,
            GenericCheckpointEvent::WorkflowAttemptStarted { .. }
        );
        let outcome = self.inner.append(run_id, expected_previous, draft)?;
        if loses_acknowledgement && !self.acknowledgement_lost.swap(true, Ordering::SeqCst) {
            self.unavailable.store(true, Ordering::SeqCst);
            return Err(GenericCheckpointError::Unavailable(
                "Workflow start commit acknowledgement was lost".to_owned(),
            ));
        }
        Ok(outcome)
    }
}

impl GenericAgentCheckpointStore for AckLostAfterWorkflowOutputCheckpointStore {
    fn load_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<StoredGenericAgentRun>, GenericCheckpointError> {
        self.inner.load_run(run_id)
    }

    fn create_run(
        &self,
        registration: GenericAgentRunRegistration,
    ) -> Result<CreateGenericRunOutcome, GenericCheckpointError> {
        self.inner.create_run(registration)
    }

    fn append(
        &self,
        run_id: &RunId,
        expected_previous: u64,
        draft: GenericCheckpointDraft,
    ) -> Result<AppendGenericCheckpointOutcome, GenericCheckpointError> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(GenericCheckpointError::Unavailable(
                "simulated Provider process loss after durable Workflow output".to_owned(),
            ));
        }
        let loses_acknowledgement = matches!(
            &draft.payload,
            GenericCheckpointEvent::ProviderEventsCommitted { events }
                if events.iter().any(|event| matches!(
                    &event.payload,
                    AgentEvent::OutputCommitted { content, .. }
                        if content.iter().any(|content| matches!(
                            &content.body,
                            ContentBody::Inline(value)
                                if value["status"] == json!("completed")
                                    && value["tool_calls"] == json!(2)
                        ))
                ))
        );
        let outcome = self.inner.append(run_id, expected_previous, draft)?;
        if loses_acknowledgement && !self.acknowledgement_lost.swap(true, Ordering::SeqCst) {
            self.unavailable.store(true, Ordering::SeqCst);
            return Err(GenericCheckpointError::Unavailable(
                "Workflow output commit acknowledgement was lost".to_owned(),
            ));
        }
        Ok(outcome)
    }
}

impl GenericAgentCheckpointStore for PausingCheckpointStore {
    fn load_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<StoredGenericAgentRun>, GenericCheckpointError> {
        self.inner.load_run(run_id)
    }

    fn create_run(
        &self,
        registration: GenericAgentRunRegistration,
    ) -> Result<CreateGenericRunOutcome, GenericCheckpointError> {
        self.inner.create_run(registration)
    }

    fn append(
        &self,
        run_id: &RunId,
        expected_previous: u64,
        draft: GenericCheckpointDraft,
    ) -> Result<AppendGenericCheckpointOutcome, GenericCheckpointError> {
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.crashed {
                return Err(GenericCheckpointError::Unavailable(
                    "simulated Provider process loss".to_owned(),
                ));
            }
            let at_cut = match (&self.cut, &draft.payload) {
                (
                    CheckpointCrashCut::InitialBoundary,
                    GenericCheckpointEvent::LoopBoundaryCommitted {
                        next_model_round: 1,
                        ..
                    },
                )
                | (
                    CheckpointCrashCut::ModelAttempt,
                    GenericCheckpointEvent::ModelAttemptStarted { .. },
                )
                | (
                    CheckpointCrashCut::ModelObservation,
                    GenericCheckpointEvent::ModelAttemptObserved { .. },
                ) => true,
                (
                    CheckpointCrashCut::InputRequestOpen,
                    GenericCheckpointEvent::ProviderEventsCommitted { events },
                ) => events.iter().any(|event| {
                    matches!(&event.payload, AgentEvent::RequestOpened { request }
                        if matches!(&request.payload, PendingRequestPayload::Input { .. }))
                }),
                (
                    CheckpointCrashCut::InputRequestResolve,
                    GenericCheckpointEvent::ProviderEventsCommitted { events },
                ) => events.iter().any(|event| {
                    matches!(&event.payload, AgentEvent::RequestResolved { resolution, .. }
                        if matches!(resolution, RequestResolution::Input { .. }))
                }),
                (
                    CheckpointCrashCut::ApprovalRequestResolve,
                    GenericCheckpointEvent::ProviderEventsCommitted { events },
                ) => events.iter().any(|event| {
                    matches!(&event.payload, AgentEvent::RequestResolved { resolution, .. }
                        if matches!(resolution, RequestResolution::Approval { .. }))
                }),
                (
                    CheckpointCrashCut::InputToolExchangeBoundary,
                    GenericCheckpointEvent::LoopBoundaryCommitted {
                        next_model_round: 2,
                        ..
                    },
                ) => true,
                (
                    CheckpointCrashCut::ApprovalToolExchangeBoundary,
                    GenericCheckpointEvent::LoopBoundaryCommitted {
                        next_model_round: 2,
                        ..
                    },
                ) => true,
                (
                    CheckpointCrashCut::DirectToolExchangeBoundary,
                    GenericCheckpointEvent::LoopBoundaryCommitted {
                        next_model_round: 2,
                        ..
                    },
                ) => true,
                _ => false,
            };
            if !state.paused_once && at_cut {
                state.paused_once = true;
                self.paused.notify_waiters();
                while !state.release {
                    state = self
                        .release
                        .wait(state)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                state.crashed = true;
                return Err(GenericCheckpointError::Unavailable(
                    "simulated Provider process loss".to_owned(),
                ));
            }
        }
        self.inner.append(run_id, expected_previous, draft)
    }
}

struct SteerAccumulatingModel {
    rounds: AtomicUsize,
    first_started: Notify,
}

struct InputRequestModel {
    rounds: AtomicUsize,
}

struct ToolLoopModel {
    rounds: AtomicUsize,
}

struct BudgetGuardToolLoopModel {
    rounds: AtomicUsize,
    oversized_dispatches: AtomicUsize,
    input_budget: u64,
}

struct ArtifactLoopModel {
    rounds: AtomicUsize,
    large_value: String,
}

struct SkillRecoveryModel {
    rounds: AtomicUsize,
    digest: String,
    round_two_messages: Arc<Mutex<Option<Vec<ModelMessage>>>>,
}

const RECOVERY_SKILL_INSTRUCTIONS: &str =
    "RECOVERY SKILL: preserve the durable activation result exactly once.";

struct ApprovalLoopModel {
    rounds: AtomicUsize,
    expect_allowed: bool,
}

struct EchoTool {
    calls: AtomicUsize,
}

struct UnknownEffectTool;

struct WalInspectingEchoTool {
    calls: AtomicUsize,
    checkpoint_store: Arc<InMemoryGenericAgentCheckpointStore>,
    run_id: RunId,
    observed_before_execute: AtomicUsize,
}

struct LargeResultTool {
    value: String,
}

struct RestartSessionModel {
    response: &'static str,
    expect_prior_turn: bool,
}

struct CompactionAwareModel {
    requests: AtomicUsize,
}

struct RecordingSessionSummarizer {
    calls: Arc<AtomicUsize>,
}

struct WorkflowLoopModel {
    rounds: AtomicUsize,
}

struct GatedWorkflowEcho {
    calls: AtomicUsize,
    first_started: Notify,
    release_first: Notify,
}

#[async_trait]
impl ModelBackend for ScriptedModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "scripted-model".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                ..ModelCapabilities::default()
            },
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        let request_id = request.request_id;
        Ok(Box::pin(stream::iter([
            Ok(ModelStreamEvent {
                request_id: request_id.clone(),
                event_id: ModelEventId::new("scripted-delta"),
                sequence: 1,
                payload: ModelEvent::TextDelta {
                    delta: "hello from the neutral model".to_owned(),
                },
            }),
            Ok(ModelStreamEvent {
                request_id,
                event_id: ModelEventId::new("scripted-finish"),
                sequence: 2,
                payload: ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            }),
        ])))
    }
}

#[async_trait]
impl ModelBackend for BlockingModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "blocking-model".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                ..ModelCapabilities::default()
            },
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        Ok(Box::pin(stream::pending()))
    }
}

#[async_trait]
impl ModelBackend for CountingBlockingModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "counting-blocking-model".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                ..ModelCapabilities::default()
            },
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(stream::pending()))
    }
}

#[async_trait]
impl ModelBackend for WalInspectingModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "wal-inspecting-model".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                ..ModelCapabilities::default()
            },
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        let stored = self
            .checkpoint_store
            .load_run(&self.run_id)
            .expect("checkpoint WAL remains readable")
            .expect("Run is registered before model start");
        let projection = stored.validate().expect("checkpoint WAL replays");
        assert!(matches!(
            projection.phase,
            GenericCheckpointPhase::ModelAttemptOpen { round: 1, .. }
        ));
        let context = stored
            .records
            .iter()
            .find_map(|record| match &record.payload {
                GenericCheckpointEvent::ModelAttemptStarted { context, .. } => Some(context),
                _ => None,
            })
            .expect("model attempt stores its Session Context provenance");
        assert_eq!(context.config_digest, stored.registration.config_digest);
        assert_eq!(context.through_session_seq, 1);
        assert_eq!(context.included_ranges.len(), 1);
        assert!(context.used_input_tokens <= context.input_budget_tokens);
        self.observed_open_attempt.fetch_add(1, Ordering::SeqCst);
        let request_id = request.request_id;
        Ok(Box::pin(stream::iter([
            Ok(ModelStreamEvent {
                request_id: request_id.clone(),
                event_id: ModelEventId::new("wal-answer"),
                sequence: 1,
                payload: ModelEvent::TextDelta {
                    delta: "write ahead confirmed".to_owned(),
                },
            }),
            Ok(ModelStreamEvent {
                request_id,
                event_id: ModelEventId::new("wal-finish"),
                sequence: 2,
                payload: ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            }),
        ])))
    }
}

#[async_trait]
impl ModelBackend for RecoveryAfterRestoreModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "blocking-model".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                ..ModelCapabilities::default()
            },
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        let stored = self
            .host_journal
            .load_run(&self.run_id)
            .await
            .expect("Host journal remains readable")
            .expect("Host Run remains registered");
        assert!(stored
            .records
            .iter()
            .any(|record| matches!(&record.event.payload, AgentEvent::ContinuityRestored { .. })));
        self.starts.fetch_add(1, Ordering::SeqCst);
        let request_id = request.request_id;
        Ok(Box::pin(stream::iter([
            Ok(ModelStreamEvent {
                request_id: request_id.clone(),
                event_id: ModelEventId::new("recovered-answer"),
                sequence: 1,
                payload: ModelEvent::TextDelta {
                    delta: "continued after durable recovery".to_owned(),
                },
            }),
            Ok(ModelStreamEvent {
                request_id,
                event_id: ModelEventId::new("recovered-finish"),
                sequence: 2,
                payload: ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            }),
        ])))
    }
}

#[async_trait]
impl ModelBackend for RecoveryIdentityModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "recovery-identity-model".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                tool_calls: true,
                ..ModelCapabilities::default()
            },
            extensions: BTreeMap::from([("test/revision".to_owned(), json!(self.revision))]),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(stream::pending()))
    }
}

#[async_trait]
impl ModelBackend for PreparedRecoveryModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "prepared-recovery-model".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                ..ModelCapabilities::default()
            },
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        assert!(request.messages.iter().any(|message| {
            message.role == ModelRole::User
                && message.content.iter().any(|content| {
                    matches!(
                        content,
                        ModelContent::Text { text }
                            if text == "recover the registered initial input"
                    )
                })
        }));
        self.starts.fetch_add(1, Ordering::SeqCst);
        let request_id = request.request_id;
        Ok(Box::pin(stream::iter([
            Ok(ModelStreamEvent {
                request_id: request_id.clone(),
                event_id: ModelEventId::new("prepared-recovery-answer"),
                sequence: 1,
                payload: ModelEvent::TextDelta {
                    delta: "prepared recovery completed".to_owned(),
                },
            }),
            Ok(ModelStreamEvent {
                request_id,
                event_id: ModelEventId::new("prepared-recovery-finish"),
                sequence: 2,
                payload: ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            }),
        ])))
    }
}

#[async_trait]
impl ModelBackend for SteerAccumulatingModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "steer-accumulating-model".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                ..ModelCapabilities::default()
            },
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        let round = self.rounds.fetch_add(1, Ordering::SeqCst);
        if round == 0 {
            self.first_started.notify_one();
        }
        let user_messages = request
            .messages
            .iter()
            .filter(|message| message.role == ModelRole::User)
            .count();
        if user_messages < 101 {
            return Ok(Box::pin(stream::pending()));
        }
        let request_id = request.request_id;
        Ok(Box::pin(stream::iter([
            Ok(ModelStreamEvent {
                request_id: request_id.clone(),
                event_id: ModelEventId::new("steer-answer"),
                sequence: 1,
                payload: ModelEvent::TextDelta {
                    delta: "all steering inputs applied".to_owned(),
                },
            }),
            Ok(ModelStreamEvent {
                request_id,
                event_id: ModelEventId::new("steer-finish"),
                sequence: 2,
                payload: ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            }),
        ])))
    }
}

#[async_trait]
impl ModelBackend for InputRequestModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "input-request-model".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                tool_calls: true,
                ..ModelCapabilities::default()
            },
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        assert!(request
            .tools
            .iter()
            .any(|tool| tool.name == "orchestral_request_input"));
        let round = self.rounds.fetch_add(1, Ordering::SeqCst);
        let request_id = request.request_id;
        if round == 0 {
            return Ok(Box::pin(stream::iter([
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("input-start"),
                    sequence: 1,
                    payload: ModelEvent::ToolCallStart {
                        call_id: ModelToolCallId::new("input-call"),
                        name: "orchestral_request_input".to_owned(),
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("input-arguments"),
                    sequence: 2,
                    payload: ModelEvent::ToolCallArgumentsDelta {
                        call_id: ModelToolCallId::new("input-call"),
                        delta: r#"{"prompt":"Which city should I use?"}"#.to_owned(),
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("input-end"),
                    sequence: 3,
                    payload: ModelEvent::ToolCallEnd {
                        call_id: ModelToolCallId::new("input-call"),
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id,
                    event_id: ModelEventId::new("input-finish"),
                    sequence: 4,
                    payload: ModelEvent::Finish {
                        reason: ModelFinishReason::ToolCalls,
                    },
                }),
            ])));
        }

        assert!(request.messages.iter().any(|message| {
            message.role == ModelRole::Tool
                && message.content.iter().any(|content| {
                    matches!(
                        content,
                        ModelContent::ToolResult {
                            call_id,
                            result,
                            is_error: false,
                        } if call_id.as_str() == "input-call"
                            && result.to_string().contains("Shanghai")
                    )
                })
        }));
        Ok(Box::pin(stream::iter([
            Ok(ModelStreamEvent {
                request_id: request_id.clone(),
                event_id: ModelEventId::new("input-answer"),
                sequence: 1,
                payload: ModelEvent::TextDelta {
                    delta: "Using Shanghai".to_owned(),
                },
            }),
            Ok(ModelStreamEvent {
                request_id,
                event_id: ModelEventId::new("input-answer-finish"),
                sequence: 2,
                payload: ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            }),
        ])))
    }
}

#[async_trait]
impl ModelBackend for ToolLoopModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "tool-loop-model".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                tool_calls: true,
                ..ModelCapabilities::default()
            },
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        assert!(request.tools.iter().any(|tool| tool.name == "echo"));
        assert!(request
            .tools
            .iter()
            .any(|tool| tool.name == "orchestral_request_input"));
        let round = self.rounds.fetch_add(1, Ordering::SeqCst);
        let request_id = request.request_id;
        if round == 0 {
            return Ok(Box::pin(stream::iter([
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("tool-start"),
                    sequence: 1,
                    payload: ModelEvent::ToolCallStart {
                        call_id: ModelToolCallId::new("echo-call"),
                        name: "echo".to_owned(),
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("tool-arguments"),
                    sequence: 2,
                    payload: ModelEvent::ToolCallArgumentsDelta {
                        call_id: ModelToolCallId::new("echo-call"),
                        delta: r#"{"value":"hello"}"#.to_owned(),
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("tool-end"),
                    sequence: 3,
                    payload: ModelEvent::ToolCallEnd {
                        call_id: ModelToolCallId::new("echo-call"),
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id,
                    event_id: ModelEventId::new("tool-finish"),
                    sequence: 4,
                    payload: ModelEvent::Finish {
                        reason: ModelFinishReason::ToolCalls,
                    },
                }),
            ])));
        }

        assert!(request.messages.iter().any(|message| {
            message.role == ModelRole::Tool
                && message.content.iter().any(|content| {
                    matches!(
                        content,
                        ModelContent::ToolResult {
                            call_id,
                            result,
                            is_error: false,
                        } if call_id.as_str() == "echo-call"
                            && result == &json!({ "result": "hello" })
                    )
                })
        }));
        Ok(Box::pin(stream::iter([
            Ok(ModelStreamEvent {
                request_id: request_id.clone(),
                event_id: ModelEventId::new("answer-delta"),
                sequence: 1,
                payload: ModelEvent::TextDelta {
                    delta: "tool said hello".to_owned(),
                },
            }),
            Ok(ModelStreamEvent {
                request_id,
                event_id: ModelEventId::new("answer-finish"),
                sequence: 2,
                payload: ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            }),
        ])))
    }
}

#[async_trait]
impl ModelBackend for BudgetGuardToolLoopModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "budget-guard-tool-loop-model".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                tool_calls: true,
                max_context_tokens: Some(self.input_budget + 500),
                ..ModelCapabilities::default()
            },
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        let input_tokens = serde_jcs::to_vec(&(&request.messages, &request.tools))
            .expect("test request is serializable")
            .len() as u64;
        if input_tokens > self.input_budget {
            self.oversized_dispatches.fetch_add(1, Ordering::SeqCst);
        }
        let round = self.rounds.fetch_add(1, Ordering::SeqCst);
        let request_id = request.request_id;
        if round == 0 {
            return Ok(Box::pin(stream::iter([
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("budget-tool-start"),
                    sequence: 1,
                    payload: ModelEvent::ToolCallStart {
                        call_id: ModelToolCallId::new("budget-tool-call"),
                        name: "echo".to_owned(),
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("budget-tool-arguments"),
                    sequence: 2,
                    payload: ModelEvent::ToolCallArgumentsDelta {
                        call_id: ModelToolCallId::new("budget-tool-call"),
                        delta: r#"{"value":"expand context"}"#.to_owned(),
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("budget-tool-end"),
                    sequence: 3,
                    payload: ModelEvent::ToolCallEnd {
                        call_id: ModelToolCallId::new("budget-tool-call"),
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id,
                    event_id: ModelEventId::new("budget-tool-finish"),
                    sequence: 4,
                    payload: ModelEvent::Finish {
                        reason: ModelFinishReason::ToolCalls,
                    },
                }),
            ])));
        }
        Ok(Box::pin(stream::iter([
            Ok(ModelStreamEvent {
                request_id: request_id.clone(),
                event_id: ModelEventId::new("budget-answer"),
                sequence: 1,
                payload: ModelEvent::TextDelta {
                    delta: "oversized request reached backend".to_owned(),
                },
            }),
            Ok(ModelStreamEvent {
                request_id,
                event_id: ModelEventId::new("budget-answer-finish"),
                sequence: 2,
                payload: ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            }),
        ])))
    }
}

#[async_trait]
impl ModelBackend for ArtifactLoopModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "artifact-loop-model".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                tool_calls: true,
                max_context_tokens: Some(16_384),
                ..ModelCapabilities::default()
            },
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        let round = self.rounds.fetch_add(1, Ordering::SeqCst);
        let request_id = request.request_id;
        if round == 0 {
            let arguments = json!({ "value": "seed" }).to_string();
            return Ok(Box::pin(stream::iter([
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("artifact-tool-start"),
                    sequence: 1,
                    payload: ModelEvent::ToolCallStart {
                        call_id: ModelToolCallId::new("artifact-echo-call"),
                        name: "echo".to_owned(),
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("artifact-tool-arguments"),
                    sequence: 2,
                    payload: ModelEvent::ToolCallArgumentsDelta {
                        call_id: ModelToolCallId::new("artifact-echo-call"),
                        delta: arguments,
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("artifact-tool-end"),
                    sequence: 3,
                    payload: ModelEvent::ToolCallEnd {
                        call_id: ModelToolCallId::new("artifact-echo-call"),
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id,
                    event_id: ModelEventId::new("artifact-tool-finish"),
                    sequence: 4,
                    payload: ModelEvent::Finish {
                        reason: ModelFinishReason::ToolCalls,
                    },
                }),
            ])));
        }

        let serialized = serde_json::to_string(&request.messages).unwrap();
        assert!(!serialized.contains(&self.large_value));
        assert!(request.messages.iter().any(|message| {
            message.role == ModelRole::Tool
                && message.content.iter().any(|content| {
                    matches!(
                        content,
                        ModelContent::ToolResult {
                            call_id,
                            result,
                            is_error: false,
                        } if call_id.as_str() == "artifact-echo-call"
                            && result["kind"] == json!("artifact")
                            && result["artifact"]["artifact_ref"].as_str().is_some()
                            && result["artifact"]["digest"]
                                .as_str()
                                .is_some_and(|digest| digest.len() == 64)
                            && result["summary"].as_str().is_some()
                    )
                })
        }));
        Ok(Box::pin(stream::iter([
            Ok(ModelStreamEvent {
                request_id: request_id.clone(),
                event_id: ModelEventId::new("artifact-answer"),
                sequence: 1,
                payload: ModelEvent::TextDelta {
                    delta: "artifact reference observed".to_owned(),
                },
            }),
            Ok(ModelStreamEvent {
                request_id,
                event_id: ModelEventId::new("artifact-answer-finish"),
                sequence: 2,
                payload: ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            }),
        ])))
    }
}

#[async_trait]
impl ModelBackend for SkillRecoveryModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "skill-recovery-model".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                tool_calls: true,
                ..ModelCapabilities::default()
            },
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        assert!(request
            .tools
            .iter()
            .any(|tool| tool.name == "orchestral_skill_activate"));
        let round = self.rounds.fetch_add(1, Ordering::SeqCst);
        let request_id = request.request_id;
        if round == 0 {
            let arguments = json!({
                "name": "recovery-skill",
                "expected_digest": self.digest,
                "reason": "recover the exact Skill activation"
            })
            .to_string();
            return Ok(Box::pin(stream::iter([
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("skill-recovery-start"),
                    sequence: 1,
                    payload: ModelEvent::ToolCallStart {
                        call_id: ModelToolCallId::new("activate-recovery-skill"),
                        name: "orchestral_skill_activate".to_owned(),
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("skill-recovery-arguments"),
                    sequence: 2,
                    payload: ModelEvent::ToolCallArgumentsDelta {
                        call_id: ModelToolCallId::new("activate-recovery-skill"),
                        delta: arguments,
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("skill-recovery-end"),
                    sequence: 3,
                    payload: ModelEvent::ToolCallEnd {
                        call_id: ModelToolCallId::new("activate-recovery-skill"),
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id,
                    event_id: ModelEventId::new("skill-recovery-finish"),
                    sequence: 4,
                    payload: ModelEvent::Finish {
                        reason: ModelFinishReason::ToolCalls,
                    },
                }),
            ])));
        }

        *self
            .round_two_messages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(request.messages.clone());
        assert!(request.messages.iter().any(|message| {
            message.content.iter().any(|content| {
                matches!(content, ModelContent::Text { text }
                    if text.contains(RECOVERY_SKILL_INSTRUCTIONS))
            })
        }));
        assert!(request.messages.iter().any(|message| {
            message.role == ModelRole::Tool
                && message.content.iter().any(|content| {
                    matches!(
                        content,
                        ModelContent::ToolResult {
                            call_id,
                            result,
                            is_error: false,
                        } if call_id.as_str() == "activate-recovery-skill"
                            && result["status"] == json!("activated")
                    )
                })
        }));
        Ok(Box::pin(stream::iter([
            Ok(ModelStreamEvent {
                request_id: request_id.clone(),
                event_id: ModelEventId::new("skill-recovery-answer"),
                sequence: 1,
                payload: ModelEvent::TextDelta {
                    delta: "Skill recovery completed".to_owned(),
                },
            }),
            Ok(ModelStreamEvent {
                request_id,
                event_id: ModelEventId::new("skill-recovery-answer-finish"),
                sequence: 2,
                payload: ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            }),
        ])))
    }
}

#[async_trait]
impl ModelBackend for ApprovalLoopModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "approval-loop-model".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                tool_calls: true,
                ..ModelCapabilities::default()
            },
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        assert!(request.tools.iter().any(|tool| tool.name == "echo"));
        assert!(request
            .tools
            .iter()
            .any(|tool| tool.name == "orchestral_request_input"));
        let round = self.rounds.fetch_add(1, Ordering::SeqCst);
        let request_id = request.request_id;
        if round == 0 {
            return Ok(Box::pin(stream::iter([
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("approval-tool-start"),
                    sequence: 1,
                    payload: ModelEvent::ToolCallStart {
                        call_id: ModelToolCallId::new("approval-echo-call"),
                        name: "echo".to_owned(),
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("approval-tool-arguments"),
                    sequence: 2,
                    payload: ModelEvent::ToolCallArgumentsDelta {
                        call_id: ModelToolCallId::new("approval-echo-call"),
                        delta: r#"{"value":"approved value"}"#.to_owned(),
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("approval-tool-end"),
                    sequence: 3,
                    payload: ModelEvent::ToolCallEnd {
                        call_id: ModelToolCallId::new("approval-echo-call"),
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id,
                    event_id: ModelEventId::new("approval-tool-finish"),
                    sequence: 4,
                    payload: ModelEvent::Finish {
                        reason: ModelFinishReason::ToolCalls,
                    },
                }),
            ])));
        }

        assert!(request.messages.iter().any(|message| {
            message.role == ModelRole::Tool
                && message.content.iter().any(|content| match content {
                    ModelContent::ToolResult {
                        call_id,
                        result,
                        is_error,
                    } if call_id.as_str() == "approval-echo-call" => {
                        if self.expect_allowed {
                            !*is_error && result == &json!({ "result": "approved value" })
                        } else {
                            *is_error
                                && result["status"] == json!("rejected")
                                && result["code"] == json!("approval_denied")
                        }
                    }
                    _ => false,
                })
        }));
        Ok(Box::pin(stream::iter([
            Ok(ModelStreamEvent {
                request_id: request_id.clone(),
                event_id: ModelEventId::new("approval-answer"),
                sequence: 1,
                payload: ModelEvent::TextDelta {
                    delta: if self.expect_allowed {
                        "approved tool completed"
                    } else {
                        "tool approval denied"
                    }
                    .to_owned(),
                },
            }),
            Ok(ModelStreamEvent {
                request_id,
                event_id: ModelEventId::new("approval-answer-finish"),
                sequence: 2,
                payload: ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            }),
        ])))
    }
}

#[async_trait]
impl GuardedToolExecutor for EchoTool {
    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ToolOutcome::Completed {
            output: json!({ "result": execution.invocation.arguments["value"].clone() }).into(),
        }
    }
}

#[async_trait]
impl GuardedToolExecutor for UnknownEffectTool {
    async fn execute(&self, _execution: GuardedToolExecution) -> ToolOutcome {
        ToolOutcome::UnknownEffect {
            message: "simulated process loss after durable Tool invocation".to_owned(),
        }
    }
}

#[async_trait]
impl GuardedToolExecutor for WalInspectingEchoTool {
    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        let projection = self
            .checkpoint_store
            .load_run(&self.run_id)
            .expect("private WAL remains readable at Tool execution")
            .expect("Tool execution belongs to a registered Run")
            .validate()
            .expect("private WAL remains valid at Tool execution");
        assert!(matches!(
            projection.phase,
            GenericCheckpointPhase::ModelAttemptObserved {
                round: 1,
                observation: orchestral_runtime::GenericModelObservation {
                    ref tool_calls,
                    ..
                },
                ..
            } if tool_calls.len() == 1
                && tool_calls[0].call_id.as_str() == "echo-call"
                && tool_calls[0].ended
        ));
        self.observed_before_execute.fetch_add(1, Ordering::SeqCst);
        self.calls.fetch_add(1, Ordering::SeqCst);
        ToolOutcome::Completed {
            output: json!({ "result": execution.invocation.arguments["value"].clone() }).into(),
        }
    }
}

fn recovery_identity_tool_runtime(
    host_bounds: ToolPolicyBounds,
    restriction_bounds: ToolPolicyBounds,
) -> Arc<GuardedToolRuntime<InMemoryApprovalCapabilityStore>> {
    let verifier = HostApprovalVerifier::new(
        b"0123456789abcdef0123456789abcdef",
        InMemoryApprovalCapabilityStore::default(),
    )
    .expect("valid Host signing key");
    let runtime = Arc::new(
        GuardedToolRuntime::new(
            HostToolPolicy {
                bounds: host_bounds,
            },
            verifier,
        )
        .expect("valid Host Tool policy"),
    );
    runtime
        .register(
            ToolDescriptor {
                tool_id: ToolId::new("test/recovery-echo"),
                model_schema: ModelToolSchema {
                    name: "recovery_echo".to_owned(),
                    description: "Echo one recovery test value".to_owned(),
                    input_schema: json!({
                        "type": "object",
                        "required": ["value"],
                        "properties": { "value": { "type": "string" } },
                        "additionalProperties": false
                    }),
                },
                output_schema: json!({
                    "type": "object",
                    "required": ["result"],
                    "properties": { "result": { "type": "string" } },
                    "additionalProperties": false
                }),
                effect_scopes: BTreeSet::new(),
                restriction: ToolRestriction {
                    bounds: restriction_bounds,
                },
                idempotency: ToolIdempotency::IdempotentWithKey,
                concurrency: ToolConcurrency::ParallelSafe,
            },
            Arc::new(EchoTool {
                calls: AtomicUsize::new(0),
            }),
        )
        .expect("recovery identity Tool registers");
    runtime
}

fn durable_approval_runtime(
    signing_key: &[u8],
    bounds: &ToolPolicyBounds,
    effect_journal: Arc<InMemoryToolEffectJournalStore>,
    tool: Arc<EchoTool>,
) -> Arc<GuardedToolRuntime<InMemoryApprovalCapabilityStore>> {
    let verifier =
        HostApprovalVerifier::new(signing_key, InMemoryApprovalCapabilityStore::default())
            .expect("valid Host signing key");
    let runtime = Arc::new(
        GuardedToolRuntime::new_with_effect_journal(
            HostToolPolicy {
                bounds: bounds.clone(),
            },
            verifier,
            effect_journal,
        )
        .expect("valid approval Tool policy"),
    );
    runtime
        .register(
            ToolDescriptor {
                tool_id: ToolId::new("test/approval-echo"),
                model_schema: ModelToolSchema {
                    name: "echo".to_owned(),
                    description: "Echo one string after Host approval".to_owned(),
                    input_schema: json!({
                        "type": "object",
                        "required": ["value"],
                        "properties": { "value": { "type": "string" } },
                        "additionalProperties": false
                    }),
                },
                output_schema: json!({
                    "type": "object",
                    "required": ["result"],
                    "properties": { "result": { "type": "string" } },
                    "additionalProperties": false
                }),
                effect_scopes: BTreeSet::from([EffectScope::Process]),
                restriction: ToolRestriction {
                    bounds: bounds.clone(),
                },
                idempotency: ToolIdempotency::IdempotentWithKey,
                concurrency: ToolConcurrency::ParallelSafe,
            },
            tool,
        )
        .expect("approval Tool registers");
    runtime
}

fn durable_direct_runtime(
    bounds: &ToolPolicyBounds,
    effect_journal: Arc<InMemoryToolEffectJournalStore>,
    tool: Arc<dyn GuardedToolExecutor>,
) -> Arc<GuardedToolRuntime<InMemoryApprovalCapabilityStore>> {
    let verifier = HostApprovalVerifier::new(
        b"0123456789abcdef0123456789abcdef",
        InMemoryApprovalCapabilityStore::default(),
    )
    .expect("valid Host signing key");
    let runtime = Arc::new(
        GuardedToolRuntime::new_with_effect_journal(
            HostToolPolicy {
                bounds: bounds.clone(),
            },
            verifier,
            effect_journal,
        )
        .expect("valid direct Tool policy"),
    );
    runtime
        .register(
            ToolDescriptor {
                tool_id: ToolId::new("test/echo"),
                model_schema: ModelToolSchema {
                    name: "echo".to_owned(),
                    description: "Echo one string".to_owned(),
                    input_schema: json!({
                        "type": "object",
                        "required": ["value"],
                        "properties": { "value": { "type": "string" } },
                        "additionalProperties": false
                    }),
                },
                output_schema: json!({
                    "type": "object",
                    "required": ["result"],
                    "properties": { "result": { "type": "string" } },
                    "additionalProperties": false
                }),
                effect_scopes: BTreeSet::new(),
                restriction: ToolRestriction {
                    bounds: bounds.clone(),
                },
                idempotency: ToolIdempotency::IdempotentWithKey,
                concurrency: ToolConcurrency::ParallelSafe,
            },
            tool,
        )
        .expect("direct Tool registers");
    runtime
}

#[async_trait]
impl GuardedToolExecutor for LargeResultTool {
    async fn execute(&self, _execution: GuardedToolExecution) -> ToolOutcome {
        ToolOutcome::Completed {
            output: json!({ "result": self.value }).into(),
        }
    }
}

#[async_trait]
impl GuardedToolExecutor for GatedWorkflowEcho {
    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            self.first_started.notify_one();
            self.release_first.notified().await;
        }
        ToolOutcome::Completed {
            output: json!({ "result": execution.invocation.arguments["value"].clone() }).into(),
        }
    }
}

#[async_trait]
impl ModelBackend for WorkflowLoopModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "workflow-loop-model".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                tool_calls: true,
                ..ModelCapabilities::default()
            },
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        assert!(request.tools.iter().any(|tool| tool.name == "echo"));
        assert!(request
            .tools
            .iter()
            .any(|tool| tool.name == "orchestral_workflow"));
        let round = self.rounds.fetch_add(1, Ordering::SeqCst);
        let request_id = request.request_id;
        if round == 0 {
            let arguments = json!({
                "plan": {
                    "goal": "run two ordered echoes",
                    "steps": [
                        {
                            "id": "first",
                            "action": "echo",
                            "kind": "action",
                            "depends_on": [],
                            "exports": ["result"],
                            "params": { "value": "first" }
                        },
                        {
                            "id": "second",
                            "action": "echo",
                            "kind": "action",
                            "depends_on": ["first"],
                            "exports": ["result"],
                            "params": { "value": "second" }
                        }
                    ]
                }
            })
            .to_string();
            return Ok(Box::pin(stream::iter([
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("workflow-start"),
                    sequence: 1,
                    payload: ModelEvent::ToolCallStart {
                        call_id: ModelToolCallId::new("workflow-call"),
                        name: "orchestral_workflow".to_owned(),
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("workflow-arguments"),
                    sequence: 2,
                    payload: ModelEvent::ToolCallArgumentsDelta {
                        call_id: ModelToolCallId::new("workflow-call"),
                        delta: arguments,
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id: request_id.clone(),
                    event_id: ModelEventId::new("workflow-end"),
                    sequence: 3,
                    payload: ModelEvent::ToolCallEnd {
                        call_id: ModelToolCallId::new("workflow-call"),
                    },
                }),
                Ok(ModelStreamEvent {
                    request_id,
                    event_id: ModelEventId::new("workflow-finish"),
                    sequence: 4,
                    payload: ModelEvent::Finish {
                        reason: ModelFinishReason::ToolCalls,
                    },
                }),
            ])));
        }

        assert!(request.messages.iter().any(|message| {
            message.role == ModelRole::Tool
                && message.content.iter().any(|content| {
                    matches!(
                        content,
                        ModelContent::ToolResult {
                            call_id,
                            result,
                            is_error: false,
                        } if call_id.as_str() == "workflow-call"
                            && result["status"] == json!("completed")
                            && result["tool_calls"] == json!(2)
                            && result["working_set"]["result"] == json!("second")
                    )
                })
        }));
        Ok(Box::pin(stream::iter([
            Ok(ModelStreamEvent {
                request_id: request_id.clone(),
                event_id: ModelEventId::new("workflow-answer"),
                sequence: 1,
                payload: ModelEvent::TextDelta {
                    delta: "workflow complete".to_owned(),
                },
            }),
            Ok(ModelStreamEvent {
                request_id,
                event_id: ModelEventId::new("workflow-answer-finish"),
                sequence: 2,
                payload: ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            }),
        ])))
    }
}

#[async_trait]
impl ModelBackend for RestartSessionModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "restart-session-model".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                max_context_tokens: Some(16_384),
                ..ModelCapabilities::default()
            },
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        let serialized = serde_json::to_string(&request.messages).unwrap();
        if self.expect_prior_turn {
            assert!(serialized.contains("first question"));
            assert!(serialized.contains("first answer"));
            assert!(serialized.contains("second question"));
        } else {
            assert!(!serialized.contains("first answer"));
        }
        let request_id = request.request_id;
        Ok(Box::pin(stream::iter([
            Ok(ModelStreamEvent {
                request_id: request_id.clone(),
                event_id: ModelEventId::new("restart-answer"),
                sequence: 1,
                payload: ModelEvent::TextDelta {
                    delta: self.response.to_owned(),
                },
            }),
            Ok(ModelStreamEvent {
                request_id,
                event_id: ModelEventId::new("restart-finish"),
                sequence: 2,
                payload: ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            }),
        ])))
    }
}

#[async_trait]
impl orchestral_runtime::AgentSessionSummarizer for RecordingSessionSummarizer {
    async fn summarize(
        &self,
        input: SessionCompactionInput,
    ) -> Result<SessionSummary, SessionContextError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(input.source.first_session_seq, 1);
        assert_eq!(input.source.last_session_seq, 2);
        assert_eq!(input.messages.len(), 2);
        Ok(SessionSummary {
            message: ModelMessage::text(ModelRole::System, "durable compaction summary marker"),
            strategy: "recording-integration-summary".to_owned(),
            model: None,
            version: "1".to_owned(),
        })
    }
}

#[async_trait]
impl ModelBackend for CompactionAwareModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "compaction-aware-model".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                max_context_tokens: Some(16_384),
                ..ModelCapabilities::default()
            },
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        let request_index = self.requests.fetch_add(1, Ordering::SeqCst);
        let serialized = serde_json::to_string(&request.messages).unwrap();
        let response = match request_index {
            0 => {
                assert!(serialized.contains("raw first question"));
                assert!(!serialized.contains("durable compaction summary marker"));
                "raw first answer"
            }
            1 => {
                assert!(serialized.contains("durable compaction summary marker"));
                assert!(serialized.contains("second question"));
                assert!(!serialized.contains("raw first question"));
                assert!(!serialized.contains("raw first answer"));
                "second answer"
            }
            _ => panic!("unexpected model request after two terminal Runs"),
        };
        let request_id = request.request_id;
        Ok(Box::pin(stream::iter([
            Ok(ModelStreamEvent {
                request_id: request_id.clone(),
                event_id: ModelEventId::new(format!("compaction-answer-{request_index}")),
                sequence: 1,
                payload: ModelEvent::TextDelta {
                    delta: response.to_owned(),
                },
            }),
            Ok(ModelStreamEvent {
                request_id,
                event_id: ModelEventId::new(format!("compaction-finish-{request_index}")),
                sequence: 2,
                payload: ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            }),
        ])))
    }
}

#[tokio::test]
async fn neutral_model_stream_becomes_an_inspectable_agent_delivery() {
    let provider = Arc::new(
        InternalGenericAgentProvider::new(
            Arc::new(ScriptedModel),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
        )
        .expect("Generic Agent accepts the neutral backend"),
    );
    let controller = Arc::new(
        AgentController::new(provider, ProviderBindingRef::new("generic-binding"))
            .expect("controller binds the Generic Agent"),
    );
    let run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        AgentSessionId::new("generic-session"),
        RunId::new("generic-run"),
        vec![Content::text("say hello")],
    )
    .expect("valid text Run");

    let execution = controller.start(run).await.expect("Run starts");
    let view = controller
        .wait_for_terminal(&execution.run_id)
        .await
        .expect("Run reaches a terminal delivery");
    let journal = controller
        .events(&execution.run_id, 0)
        .await
        .expect("Run journal remains readable");

    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert_eq!(view.last_run_seq, Some(4));
    assert_eq!(journal.len(), 4);
    let delivery = view.delivery.expect("Delivered Run exposes its delivery");
    assert!(matches!(
        delivery.final_response.body,
        ContentBody::Inline(serde_json::Value::String(ref text))
            if text == "hello from the neutral model"
    ));
}

#[tokio::test]
async fn model_attempt_is_in_the_private_wal_before_backend_start() {
    let run_id = RunId::new("wal-run");
    let checkpoint_store = Arc::new(InMemoryGenericAgentCheckpointStore::default());
    let model = Arc::new(WalInspectingModel {
        checkpoint_store: checkpoint_store.clone(),
        run_id: run_id.clone(),
        observed_open_attempt: AtomicUsize::new(0),
    });
    let provider = Arc::new(
        InternalGenericAgentProvider::new(
            model.clone(),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
        )
        .expect("Generic Agent accepts the neutral backend")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let controller = Arc::new(
        AgentController::new(provider, ProviderBindingRef::new("wal-binding"))
            .expect("controller binds the Generic Agent"),
    );
    let run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        AgentSessionId::new("wal-session"),
        run_id.clone(),
        vec![Content::text("prove the model attempt is write-ahead")],
    )
    .expect("valid text Run");

    let execution = controller.start(run).await.expect("Run starts");
    controller
        .wait_for_terminal(&execution.run_id)
        .await
        .expect("Run reaches delivery");

    assert_eq!(model.observed_open_attempt.load(Ordering::SeqCst), 1);
    let stored = checkpoint_store
        .load_run(&run_id)
        .expect("private WAL remains readable")
        .expect("private Run registration is durable");
    let projection = stored
        .validate()
        .expect("private WAL replays after delivery");
    assert_eq!(projection.phase, GenericCheckpointPhase::Terminal);
    assert!(matches!(
        &stored.records[0].payload,
        GenericCheckpointEvent::LoopBoundaryCommitted {
            next_model_round: 1,
            ..
        }
    ));
    assert!(stored.records.iter().any(|record| matches!(
        &record.payload,
        GenericCheckpointEvent::ModelAttemptStarted { round: 1, .. }
    )));

    let host_journal = controller
        .events(&run_id, 0)
        .await
        .expect("Host journal remains readable");
    let host_provider_digests = host_journal
        .iter()
        .filter(|record| matches!(&record.authority, AgentEventAuthority::Provider))
        .map(|record| record.draft_digest.clone())
        .collect::<Vec<_>>();
    let private_provider_digests = projection
        .provider_events
        .iter()
        .map(|event| {
            event
                .computed_digest()
                .expect("private event remains valid")
        })
        .collect::<Vec<_>>();
    assert_eq!(private_provider_digests, host_provider_digests);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn model_observation_is_write_ahead_of_pending_interaction() {
    let run_id = RunId::new("model-observation-wal-run");
    let checkpoint_store = Arc::new(PausingCheckpointStore::at(
        CheckpointCrashCut::ModelObservation,
    ));
    let model = Arc::new(InputRequestModel {
        rounds: AtomicUsize::new(0),
    });
    let provider = Arc::new(
        InternalGenericAgentProvider::new(
            model.clone(),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
        )
        .expect("input-capable Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let controller = Arc::new(
        AgentController::new(
            provider,
            ProviderBindingRef::new("model-observation-wal-binding"),
        )
        .expect("controller binds the Generic Agent"),
    );
    let paused = checkpoint_store.paused.notified();
    let execution = controller
        .start(
            AgentRunEnvelope::new(
                AGENT_PROTOCOL_V1,
                AgentSessionId::new("model-observation-wal-session"),
                run_id.clone(),
                vec![Content::text(
                    "request input only after durable observation",
                )],
            )
            .expect("valid input Run"),
        )
        .await
        .expect("Run starts");
    tokio::time::timeout(std::time::Duration::from_secs(1), paused)
        .await
        .expect("model observation reaches the injected crash cut");

    assert!(matches!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("private Run remains registered")
            .validate()
            .expect("committed WAL prefix remains valid")
            .phase,
        GenericCheckpointPhase::ModelAttemptOpen { round: 1, .. }
    ));
    assert!(controller
        .events(&run_id, 0)
        .await
        .expect("Host Journal remains readable")
        .iter()
        .all(|record| !matches!(&record.event.payload, AgentEvent::RequestOpened { .. })));

    checkpoint_store.release_as_crash();
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("simulated process loss reaches the Host")
    .expect_err("uncommitted model observation cannot become a terminal outcome");
    assert!(matches!(
        error,
        AgentControlError::ContinuityUnknown(ref actual) if actual == &run_id
    ));
    assert_eq!(model.rounds.load(Ordering::SeqCst), 1);
    assert!(controller
        .events(&run_id, 0)
        .await
        .expect("Host Journal remains readable after loss")
        .iter()
        .all(|record| !matches!(&record.event.payload, AgentEvent::RequestOpened { .. })));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observed_input_continuation_recovers_before_request_open() {
    let run_id = RunId::new("observed-input-recovery-run");
    let checkpoint_store = Arc::new(PausingCheckpointStore::at(
        CheckpointCrashCut::InputRequestOpen,
    ));
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let host_journal = Arc::new(InMemoryAgentJournalStore::default());
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let first_model = Arc::new(InputRequestModel {
        rounds: AtomicUsize::new(0),
    });
    let first_provider = Arc::new(
        InternalGenericAgentProvider::new_with_session_journal(
            first_model.clone(),
            config.clone(),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("first input-capable Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let first_controller = Arc::new(
        AgentController::with_journal_store(
            first_provider,
            ProviderBindingRef::new("observed-input-recovery-binding"),
            host_journal.clone(),
        )
        .expect("first controller binds"),
    );
    let paused = checkpoint_store.paused.notified();
    let execution = first_controller
        .start(
            AgentRunEnvelope::new(
                AGENT_PROTOCOL_V1,
                AgentSessionId::new("observed-input-recovery-session"),
                run_id.clone(),
                vec![Content::text("recover before opening the input request")],
            )
            .expect("valid input Run"),
        )
        .await
        .expect("Run starts");
    tokio::time::timeout(std::time::Duration::from_secs(1), paused)
        .await
        .expect("input RequestOpened reaches the injected crash cut");
    assert!(matches!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("private Run remains registered")
            .validate()
            .expect("observed WAL remains valid")
            .phase,
        GenericCheckpointPhase::ModelAttemptObserved { round: 1, .. }
    ));
    assert!(first_controller
        .events(&run_id, 0)
        .await
        .expect("Host Journal remains readable")
        .iter()
        .all(|record| !matches!(&record.event.payload, AgentEvent::RequestOpened { .. })));

    checkpoint_store.release_as_crash();
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        first_controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("simulated process loss reaches the Host")
    .expect_err("unopened input continuation is not terminal");
    assert!(matches!(
        error,
        AgentControlError::ContinuityUnknown(ref actual) if actual == &run_id
    ));
    assert_eq!(first_model.rounds.load(Ordering::SeqCst), 1);
    drop(first_controller);
    checkpoint_store.allow_recovery_writes();

    let replacement_model = Arc::new(InputRequestModel {
        rounds: AtomicUsize::new(1),
    });
    let replacement_provider = Arc::new(
        InternalGenericAgentProvider::new_with_session_journal(
            replacement_model.clone(),
            config,
            session_journal,
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("replacement Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("replacement Provider binds the same private WAL"),
    );
    let replacement_controller = Arc::new(
        AgentController::with_journal_store(
            replacement_provider,
            ProviderBindingRef::new("observed-input-recovery-binding"),
            host_journal,
        )
        .expect("replacement controller binds"),
    );
    replacement_controller
        .recover(&run_id)
        .await
        .expect("durable observed input continuation is recoverable");
    let pending = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let view = replacement_controller
                .inspect(&run_id)
                .await
                .expect("recovered Run remains inspectable");
            if let Some(request) = view.pending_requests.first() {
                break request.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("recovered continuation opens its original input request");
    assert_eq!(pending.kind(), PendingRequestKind::Input);
    replacement_controller
        .command(
            AgentCommandEnvelope::new(
                CommandId::new("resolve-recovered-observed-input"),
                run_id.clone(),
                Some(pending.request_id.clone()),
                AgentCommand::ResolveRequest {
                    response: RequestResolution::Input {
                        content: vec![Content::text("Shanghai")],
                    },
                },
            )
            .expect("valid correlated input resolution"),
        )
        .await
        .expect("recovered input resolution is accepted");
    let view = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        replacement_controller.wait_for_terminal(&run_id),
    )
    .await
    .expect("recovered input continuation completes promptly")
    .expect("recovered input continuation reaches Delivery");
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert_eq!(replacement_model.rounds.load(Ordering::SeqCst), 2);
    let events = replacement_controller
        .events(&run_id, 0)
        .await
        .expect("recovered Host Journal remains readable");
    assert_eq!(
        events
            .iter()
            .filter(|record| matches!(&record.event.payload, AgentEvent::RequestOpened { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|record| matches!(&record.event.payload, AgentEvent::RequestResolved { .. }))
            .count(),
        1
    );
    assert_eq!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("recovered Run remains registered")
            .validate()
            .expect("recovered WAL remains valid")
            .phase,
        GenericCheckpointPhase::Terminal
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opened_input_request_reattaches_without_duplicate_or_command_race() {
    let run_id = RunId::new("opened-input-recovery-run");
    let checkpoint_store = Arc::new(AckLostAfterInputOpenCheckpointStore::default());
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let host_journal = Arc::new(InMemoryAgentJournalStore::default());
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let first_model = Arc::new(InputRequestModel {
        rounds: AtomicUsize::new(0),
    });
    let first_provider = Arc::new(
        InternalGenericAgentProvider::new_with_session_journal(
            first_model.clone(),
            config.clone(),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("first input-capable Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let first_controller = Arc::new(
        AgentController::with_journal_store(
            first_provider,
            ProviderBindingRef::new("opened-input-recovery-binding"),
            host_journal.clone(),
        )
        .expect("first controller binds"),
    );
    let execution = first_controller
        .start(
            AgentRunEnvelope::new(
                AGENT_PROTOCOL_V1,
                AgentSessionId::new("opened-input-recovery-session"),
                run_id.clone(),
                vec![Content::text("reattach the durably opened input request")],
            )
            .expect("valid input Run"),
        )
        .await
        .expect("Run starts");
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        first_controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("lost RequestOpened acknowledgement reaches the Host")
    .expect_err("lost Provider continuity is not terminal");
    assert!(matches!(
        error,
        AgentControlError::ContinuityUnknown(ref actual) if actual == &run_id
    ));
    let stored = checkpoint_store
        .load_run(&run_id)
        .expect("private WAL remains readable")
        .expect("private Run remains registered");
    let projection = stored.validate().expect("private WAL remains valid");
    assert!(matches!(
        projection.phase,
        GenericCheckpointPhase::ModelAttemptObserved { round: 1, .. }
    ));
    assert_eq!(
        projection
            .provider_events
            .iter()
            .filter(|event| matches!(&event.payload, AgentEvent::RequestOpened { .. }))
            .count(),
        1
    );
    assert!(first_controller
        .events(&run_id, 0)
        .await
        .expect("Host Journal remains readable")
        .iter()
        .all(|record| !matches!(&record.event.payload, AgentEvent::RequestOpened { .. })));
    drop(first_controller);
    checkpoint_store.allow_recovery_writes();

    let replacement_model = Arc::new(InputRequestModel {
        rounds: AtomicUsize::new(1),
    });
    let replacement_provider = Arc::new(
        InternalGenericAgentProvider::new_with_session_journal(
            replacement_model.clone(),
            config,
            session_journal,
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("replacement Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("replacement Provider binds the same private WAL"),
    );
    let replacement_controller = Arc::new(
        AgentController::with_journal_store(
            replacement_provider,
            ProviderBindingRef::new("opened-input-recovery-binding"),
            host_journal,
        )
        .expect("replacement controller binds"),
    );
    replacement_controller
        .recover(&run_id)
        .await
        .expect("durably opened input request is recoverable");
    let pending = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let view = replacement_controller
                .inspect(&run_id)
                .await
                .expect("recovered Run remains inspectable");
            if let Some(request) = view.pending_requests.first() {
                break request.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("private RequestOpened is replayed to the Host");
    let ack = replacement_controller
        .command(
            AgentCommandEnvelope::new(
                CommandId::new("resolve-reattached-input"),
                run_id.clone(),
                Some(pending.request_id.clone()),
                AgentCommand::ResolveRequest {
                    response: RequestResolution::Input {
                        content: vec![Content::text("Shanghai")],
                    },
                },
            )
            .expect("valid correlated input resolution"),
        )
        .await
        .expect("reattached responder accepts the first immediate command");
    assert!(matches!(
        ack.state,
        CommandAckState::Accepted { .. } | CommandAckState::Applied { .. }
    ));
    let view = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        replacement_controller.wait_for_terminal(&run_id),
    )
    .await
    .expect("reattached input continuation completes promptly")
    .expect("reattached input continuation reaches Delivery");
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert_eq!(replacement_model.rounds.load(Ordering::SeqCst), 2);
    let events = replacement_controller
        .events(&run_id, 0)
        .await
        .expect("recovered Host Journal remains readable");
    assert_eq!(
        events
            .iter()
            .filter(|record| matches!(&record.event.payload, AgentEvent::RequestOpened { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|record| matches!(&record.event.payload, AgentEvent::RequestResolved { .. }))
            .count(),
        1
    );
    assert_eq!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("recovered Run remains registered")
            .validate()
            .expect("recovered WAL remains valid")
            .phase,
        GenericCheckpointPhase::Terminal
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_input_resolution_recovers_without_asking_twice() {
    let run_id = RunId::new("accepted-input-recovery-run");
    let command_id = CommandId::new("accepted-input-resolution");
    let checkpoint_store = Arc::new(PausingCheckpointStore::at(
        CheckpointCrashCut::InputRequestResolve,
    ));
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let host_journal = Arc::new(InMemoryAgentJournalStore::default());
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let first_model = Arc::new(InputRequestModel {
        rounds: AtomicUsize::new(0),
    });
    let first_provider = Arc::new(
        InternalGenericAgentProvider::new_with_session_journal(
            first_model,
            config.clone(),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("first input-capable Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let first_controller = Arc::new(
        AgentController::with_journal_store(
            first_provider,
            ProviderBindingRef::new("accepted-input-recovery-binding"),
            host_journal.clone(),
        )
        .expect("first controller binds"),
    );
    let execution = first_controller
        .start(
            AgentRunEnvelope::new(
                AGENT_PROTOCOL_V1,
                AgentSessionId::new("accepted-input-recovery-session"),
                run_id.clone(),
                vec![Content::text("recover my accepted input answer")],
            )
            .expect("valid input Run"),
        )
        .await
        .expect("Run starts");
    let pending = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let view = first_controller
                .inspect(&run_id)
                .await
                .expect("Run remains inspectable");
            if let Some(request) = view.pending_requests.first() {
                break request.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first process opens the input request");
    let paused = checkpoint_store.paused.notified();
    let ack = first_controller
        .command(
            AgentCommandEnvelope::new(
                command_id.clone(),
                run_id.clone(),
                Some(pending.request_id.clone()),
                AgentCommand::ResolveRequest {
                    response: RequestResolution::Input {
                        content: vec![Content::text("Shanghai")],
                    },
                },
            )
            .expect("valid correlated input resolution"),
        )
        .await
        .expect("input resolution command is accepted");
    assert!(matches!(ack.state, CommandAckState::Accepted { .. }));
    tokio::time::timeout(std::time::Duration::from_secs(1), paused)
        .await
        .expect("RequestResolved reaches the injected crash cut");
    let stored = checkpoint_store
        .load_run(&run_id)
        .expect("private WAL remains readable")
        .expect("private Run remains registered");
    let projection = stored.validate().expect("private WAL remains valid");
    assert!(matches!(
        projection.phase,
        GenericCheckpointPhase::ModelAttemptObserved { round: 1, .. }
    ));
    assert!(matches!(
        projection
            .commands
            .get(&command_id)
            .map(|command| &command.outcome),
        Some(ProviderCommandOutcome::Accepted)
    ));
    assert!(projection
        .provider_events
        .iter()
        .all(|event| !matches!(&event.payload, AgentEvent::RequestResolved { .. })));

    checkpoint_store.release_as_crash();
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        first_controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("simulated process loss reaches the Host")
    .expect_err("accepted but unapplied input is not terminal");
    assert!(matches!(
        error,
        AgentControlError::ContinuityUnknown(ref actual) if actual == &run_id
    ));
    drop(first_controller);
    checkpoint_store.allow_recovery_writes();

    let replacement_model = Arc::new(InputRequestModel {
        rounds: AtomicUsize::new(1),
    });
    let replacement_provider = Arc::new(
        InternalGenericAgentProvider::new_with_session_journal(
            replacement_model.clone(),
            config,
            session_journal,
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("replacement Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("replacement Provider binds the same private WAL"),
    );
    let replacement_controller = Arc::new(
        AgentController::with_journal_store(
            replacement_provider,
            ProviderBindingRef::new("accepted-input-recovery-binding"),
            host_journal,
        )
        .expect("replacement controller binds"),
    );
    replacement_controller
        .recover(&run_id)
        .await
        .expect("accepted input resolution is recoverable");
    let view = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        replacement_controller.wait_for_terminal(&run_id),
    )
    .await
    .expect("accepted input continuation completes without another answer")
    .expect("accepted input continuation reaches Delivery");
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert!(view.pending_requests.is_empty());
    assert_eq!(replacement_model.rounds.load(Ordering::SeqCst), 2);
    assert!(matches!(
        replacement_controller
            .command_ack(&run_id, &command_id)
            .await
            .expect("original command remains queryable")
            .state,
        CommandAckState::Applied { .. }
    ));
    let events = replacement_controller
        .events(&run_id, 0)
        .await
        .expect("recovered Host Journal remains readable");
    assert_eq!(
        events
            .iter()
            .filter(|record| matches!(&record.event.payload, AgentEvent::RequestOpened { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|record| matches!(&record.event.payload, AgentEvent::RequestResolved { .. }))
            .count(),
        1
    );
    assert_eq!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("recovered Run remains registered")
            .validate()
            .expect("recovered WAL remains valid")
            .phase,
        GenericCheckpointPhase::Terminal
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_input_resolution_recovers_before_session_tool_exchange() {
    let run_id = RunId::new("durable-input-resolution-recovery-run");
    let command_id = CommandId::new("durable-input-resolution");
    let session_id = AgentSessionId::new("durable-input-resolution-recovery-session");
    let checkpoint_store = Arc::new(AckLostAfterInputResolveCheckpointStore::default());
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let host_journal = Arc::new(InMemoryAgentJournalStore::default());
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let first_provider = Arc::new(
        InternalGenericAgentProvider::new_with_session_journal(
            Arc::new(InputRequestModel {
                rounds: AtomicUsize::new(0),
            }),
            config.clone(),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("first input-capable Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let first_controller = Arc::new(
        AgentController::with_journal_store(
            first_provider,
            ProviderBindingRef::new("durable-input-resolution-recovery-binding"),
            host_journal.clone(),
        )
        .expect("first controller binds"),
    );
    let execution = first_controller
        .start(
            AgentRunEnvelope::new(
                AGENT_PROTOCOL_V1,
                session_id.clone(),
                run_id.clone(),
                vec![Content::text("recover my durable input answer")],
            )
            .expect("valid input Run"),
        )
        .await
        .expect("Run starts");
    let pending = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let view = first_controller
                .inspect(&run_id)
                .await
                .expect("Run remains inspectable");
            if let Some(request) = view.pending_requests.first() {
                break request.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first process opens the input request");
    let ack = first_controller
        .command(
            AgentCommandEnvelope::new(
                command_id.clone(),
                run_id.clone(),
                Some(pending.request_id),
                AgentCommand::ResolveRequest {
                    response: RequestResolution::Input {
                        content: vec![Content::text("Shanghai")],
                    },
                },
            )
            .expect("valid correlated input resolution"),
        )
        .await
        .expect("input resolution command is accepted");
    assert!(matches!(ack.state, CommandAckState::Accepted { .. }));
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        first_controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("lost RequestResolved acknowledgement reaches the Host")
    .expect_err("durable but unacknowledged input resolution is not terminal");
    assert!(matches!(
        error,
        AgentControlError::ContinuityUnknown(ref actual) if actual == &run_id
    ));

    let stored = checkpoint_store
        .load_run(&run_id)
        .expect("private WAL remains readable")
        .expect("private Run remains registered");
    let projection = stored.validate().expect("private WAL remains valid");
    assert!(matches!(
        projection.phase,
        GenericCheckpointPhase::ModelAttemptObserved { round: 1, .. }
    ));
    assert_eq!(
        projection
            .provider_events
            .iter()
            .filter(|event| matches!(&event.payload, AgentEvent::RequestResolved { .. }))
            .count(),
        1
    );
    assert!(session_journal
        .load_session(&session_id)
        .await
        .expect("Session Journal remains readable")
        .iter()
        .all(|record| !matches!(
            &record.payload,
            AgentSessionEvent::ToolExchangeCommitted { .. }
        )));
    assert!(first_controller
        .events(&run_id, 0)
        .await
        .expect("Host Journal remains readable")
        .iter()
        .all(|record| !matches!(&record.event.payload, AgentEvent::RequestResolved { .. })));
    drop(first_controller);
    checkpoint_store.allow_recovery_writes();

    let replacement_model = Arc::new(InputRequestModel {
        rounds: AtomicUsize::new(1),
    });
    let replacement_provider = Arc::new(
        InternalGenericAgentProvider::new_with_session_journal(
            replacement_model.clone(),
            config,
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("replacement Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("replacement Provider binds the same private WAL"),
    );
    let replacement_controller = Arc::new(
        AgentController::with_journal_store(
            replacement_provider,
            ProviderBindingRef::new("durable-input-resolution-recovery-binding"),
            host_journal,
        )
        .expect("replacement controller binds"),
    );
    replacement_controller
        .recover(&run_id)
        .await
        .expect("durable input resolution is recoverable");
    let view = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        replacement_controller.wait_for_terminal(&run_id),
    )
    .await
    .expect("durable input continuation completes without another answer")
    .expect("durable input continuation reaches Delivery");
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert!(view.pending_requests.is_empty());
    assert_eq!(replacement_model.rounds.load(Ordering::SeqCst), 2);
    assert!(matches!(
        replacement_controller
            .command_ack(&run_id, &command_id)
            .await
            .expect("original command remains queryable")
            .state,
        CommandAckState::Applied { .. }
    ));
    let events = replacement_controller
        .events(&run_id, 0)
        .await
        .expect("recovered Host Journal remains readable");
    assert_eq!(
        events
            .iter()
            .filter(|record| matches!(&record.event.payload, AgentEvent::RequestOpened { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|record| matches!(&record.event.payload, AgentEvent::RequestResolved { .. }))
            .count(),
        1
    );
    assert_eq!(
        session_journal
            .load_session(&session_id)
            .await
            .expect("recovered Session Journal remains readable")
            .iter()
            .filter(|record| matches!(
                &record.payload,
                AgentSessionEvent::ToolExchangeCommitted { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("recovered Run remains registered")
            .validate()
            .expect("recovered WAL remains valid")
            .phase,
        GenericCheckpointPhase::Terminal
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_input_tool_exchange_recovers_before_private_loop_boundary() {
    let run_id = RunId::new("committed-input-exchange-recovery-run");
    let session_id = AgentSessionId::new("committed-input-exchange-recovery-session");
    let checkpoint_store = Arc::new(PausingCheckpointStore::at(
        CheckpointCrashCut::InputToolExchangeBoundary,
    ));
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let host_journal = Arc::new(InMemoryAgentJournalStore::default());
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let first_provider = Arc::new(
        InternalGenericAgentProvider::new_with_session_journal(
            Arc::new(InputRequestModel {
                rounds: AtomicUsize::new(0),
            }),
            config.clone(),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("first input-capable Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let first_controller = Arc::new(
        AgentController::with_journal_store(
            first_provider,
            ProviderBindingRef::new("committed-input-exchange-recovery-binding"),
            host_journal.clone(),
        )
        .expect("first controller binds"),
    );
    let execution = first_controller
        .start(
            AgentRunEnvelope::new(
                AGENT_PROTOCOL_V1,
                session_id.clone(),
                run_id.clone(),
                vec![Content::text("recover my committed Tool exchange")],
            )
            .expect("valid input Run"),
        )
        .await
        .expect("Run starts");
    let pending = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let view = first_controller
                .inspect(&run_id)
                .await
                .expect("Run remains inspectable");
            if let Some(request) = view.pending_requests.first() {
                break request.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first process opens the input request");
    let paused = checkpoint_store.paused.notified();
    let ack = first_controller
        .command(
            AgentCommandEnvelope::new(
                CommandId::new("committed-input-exchange-resolution"),
                run_id.clone(),
                Some(pending.request_id),
                AgentCommand::ResolveRequest {
                    response: RequestResolution::Input {
                        content: vec![Content::text("Shanghai")],
                    },
                },
            )
            .expect("valid correlated input resolution"),
        )
        .await
        .expect("input resolution command is accepted");
    assert!(matches!(ack.state, CommandAckState::Accepted { .. }));
    tokio::time::timeout(std::time::Duration::from_secs(1), paused)
        .await
        .expect("Session Tool exchange reaches the next private boundary");

    let projection = checkpoint_store
        .load_run(&run_id)
        .expect("private WAL remains readable")
        .expect("private Run remains registered")
        .validate()
        .expect("private WAL remains valid");
    assert!(matches!(
        projection.phase,
        GenericCheckpointPhase::ModelAttemptObserved { round: 1, .. }
    ));
    assert_eq!(
        projection
            .provider_events
            .iter()
            .filter(|event| matches!(&event.payload, AgentEvent::RequestResolved { .. }))
            .count(),
        1
    );
    assert_eq!(
        session_journal
            .load_session(&session_id)
            .await
            .expect("Session Journal remains readable")
            .iter()
            .filter(|record| matches!(
                &record.payload,
                AgentSessionEvent::ToolExchangeCommitted { .. }
            ))
            .count(),
        1
    );

    checkpoint_store.release_as_crash();
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        first_controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("simulated boundary process loss reaches the Host")
    .expect_err("uncommitted private loop boundary is not terminal");
    assert!(matches!(
        error,
        AgentControlError::ContinuityUnknown(ref actual) if actual == &run_id
    ));
    drop(first_controller);
    checkpoint_store.allow_recovery_writes();

    let replacement_model = Arc::new(InputRequestModel {
        rounds: AtomicUsize::new(1),
    });
    let replacement_provider = Arc::new(
        InternalGenericAgentProvider::new_with_session_journal(
            replacement_model.clone(),
            config,
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("replacement Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("replacement Provider binds the same private WAL"),
    );
    let replacement_controller = Arc::new(
        AgentController::with_journal_store(
            replacement_provider,
            ProviderBindingRef::new("committed-input-exchange-recovery-binding"),
            host_journal,
        )
        .expect("replacement controller binds"),
    );
    replacement_controller
        .recover(&run_id)
        .await
        .expect("committed Session Tool exchange is recoverable");
    let view = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        replacement_controller.wait_for_terminal(&run_id),
    )
    .await
    .expect("recovered Session exchange advances without another answer")
    .expect("recovered Session exchange reaches Delivery");
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert!(view.pending_requests.is_empty());
    assert_eq!(replacement_model.rounds.load(Ordering::SeqCst), 2);
    assert_eq!(
        session_journal
            .load_session(&session_id)
            .await
            .expect("recovered Session Journal remains readable")
            .iter()
            .filter(|record| matches!(
                &record.payload,
                AgentSessionEvent::ToolExchangeCommitted { .. }
            ))
            .count(),
        1
    );
    let events = replacement_controller
        .events(&run_id, 0)
        .await
        .expect("recovered Host Journal remains readable");
    assert_eq!(
        events
            .iter()
            .filter(|record| matches!(&record.event.payload, AgentEvent::RequestOpened { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|record| matches!(&record.event.payload, AgentEvent::RequestResolved { .. }))
            .count(),
        1
    );
    assert_eq!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("recovered Run remains registered")
            .validate()
            .expect("recovered WAL remains valid")
            .phase,
        GenericCheckpointPhase::Terminal
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_pending_approval_reattaches_to_a_replacement_provider() {
    run_pending_approval_recovery(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_pending_approval_can_be_denied_after_recovery() {
    run_pending_approval_recovery(false).await;
}

async fn run_pending_approval_recovery(allow: bool) {
    const SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
    let run_id = RunId::new("pending-approval-recovery-run");
    let session_id = AgentSessionId::new("pending-approval-recovery-session");
    let checkpoint_store = Arc::new(AckLostAfterApprovalOpenCheckpointStore::default());
    let effect_journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let host_journal = Arc::new(InMemoryAgentJournalStore::default());
    let tool = Arc::new(EchoTool {
        calls: AtomicUsize::new(0),
    });
    let bounds = ToolPolicyBounds {
        allowed_effects: BTreeSet::from([EffectScope::Process]),
        approval: ApprovalPolicy::Required,
        max_timeout_ms: Some(1_000),
        max_output_bytes: Some(1_024),
        ..ToolPolicyBounds::default()
    };
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let first_broker = Arc::new(
        InMemoryHostApprovalBroker::new(SIGNING_KEY).expect("first approval broker is valid"),
    );
    let first_provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools_approval_and_session_journal(
            Arc::new(ApprovalLoopModel {
                rounds: AtomicUsize::new(0),
                expect_allowed: allow,
            }),
            config.clone(),
            durable_approval_runtime(SIGNING_KEY, &bounds, effect_journal.clone(), tool.clone()),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            first_broker,
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("first approval-capable Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let first_controller = Arc::new(
        AgentController::with_journal_store(
            first_provider,
            ProviderBindingRef::new("pending-approval-recovery-binding"),
            host_journal.clone(),
        )
        .expect("first controller binds"),
    );
    let execution = first_controller
        .start(
            AgentRunEnvelope::new(
                AGENT_PROTOCOL_V1,
                session_id.clone(),
                run_id.clone(),
                vec![Content::text("recover this pending approval")],
            )
            .expect("valid approval Run"),
        )
        .await
        .expect("Run starts");
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        first_controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("lost approval RequestOpened acknowledgement reaches the Host")
    .expect_err("durable pending approval is not terminal");
    assert!(matches!(
        error,
        AgentControlError::ContinuityUnknown(ref actual) if actual == &run_id
    ));
    let projection = checkpoint_store
        .load_run(&run_id)
        .expect("private WAL remains readable")
        .expect("private Run remains registered")
        .validate()
        .expect("private WAL remains valid");
    assert!(matches!(
        projection.phase,
        GenericCheckpointPhase::ModelAttemptObserved { round: 1, .. }
    ));
    assert_eq!(
        projection
            .provider_events
            .iter()
            .filter(|event| matches!(
                &event.payload,
                AgentEvent::RequestOpened { request }
                    if matches!(&request.payload, PendingRequestPayload::Approval { .. })
            ))
            .count(),
        1
    );
    assert!(first_controller
        .events(&run_id, 0)
        .await
        .expect("Host Journal remains readable")
        .iter()
        .all(|record| !matches!(&record.event.payload, AgentEvent::RequestOpened { .. })));
    assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
    drop(first_controller);
    checkpoint_store.allow_recovery_writes();

    let replacement_broker = Arc::new(
        InMemoryHostApprovalBroker::new(SIGNING_KEY).expect("replacement approval broker is valid"),
    );
    let replacement_model = Arc::new(ApprovalLoopModel {
        rounds: AtomicUsize::new(1),
        expect_allowed: allow,
    });
    let replacement_provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools_approval_and_session_journal(
            replacement_model.clone(),
            config,
            durable_approval_runtime(SIGNING_KEY, &bounds, effect_journal, tool.clone()),
            RunToolGrant { bounds },
            replacement_broker.clone(),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("replacement approval-capable Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("replacement Provider binds the same private WAL"),
    );
    let replacement_controller = Arc::new(
        AgentController::with_journal_store(
            replacement_provider,
            ProviderBindingRef::new("pending-approval-recovery-binding"),
            host_journal,
        )
        .expect("replacement controller binds"),
    );
    replacement_controller
        .recover(&run_id)
        .await
        .expect("durable pending approval is recoverable");
    let pending = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let view = replacement_controller
                .inspect(&run_id)
                .await
                .expect("recovered Run remains inspectable");
            if let Some(request) = view.pending_requests.first() {
                break request.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("recovered approval request reaches the Host");
    assert_eq!(pending.kind(), PendingRequestKind::Approval);
    let resolution = if allow {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("wall clock follows Unix epoch")
            .as_millis() as i64;
        let grant_ref = replacement_broker
            .approve(&pending.request_id, now_ms + 60_000)
            .expect("replacement Host issues an exact approval grant");
        RequestResolution::Approval {
            decision: ApprovalDecision::Allow,
            grant_ref: Some(grant_ref),
        }
    } else {
        RequestResolution::Approval {
            decision: ApprovalDecision::Deny,
            grant_ref: None,
        }
    };
    let ack = replacement_controller
        .command(
            AgentCommandEnvelope::new(
                CommandId::new("recovered-approval-resolution"),
                run_id.clone(),
                Some(pending.request_id),
                AgentCommand::ResolveRequest {
                    response: resolution,
                },
            )
            .expect("valid recovered approval resolution"),
        )
        .await
        .expect("recovered approval responder accepts the first command");
    assert!(matches!(
        ack.state,
        CommandAckState::Accepted { .. } | CommandAckState::Applied { .. }
    ));
    let view = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        replacement_controller.wait_for_terminal(&run_id),
    )
    .await
    .expect("recovered approval continuation completes promptly")
    .expect("recovered approval continuation reaches Delivery");
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert_eq!(tool.calls.load(Ordering::SeqCst), usize::from(allow));
    assert_eq!(replacement_model.rounds.load(Ordering::SeqCst), 2);
    let events = replacement_controller
        .events(&run_id, 0)
        .await
        .expect("recovered Host Journal remains readable");
    assert_eq!(
        events
            .iter()
            .filter(|record| matches!(&record.event.payload, AgentEvent::RequestOpened { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|record| matches!(&record.event.payload, AgentEvent::RequestResolved { .. }))
            .count(),
        1
    );
    assert_eq!(
        session_journal
            .load_session(&session_id)
            .await
            .expect("recovered Session Journal remains readable")
            .iter()
            .filter(|record| matches!(
                &record.payload,
                AgentSessionEvent::ToolExchangeCommitted { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("recovered Run remains registered")
            .validate()
            .expect("recovered WAL remains valid")
            .phase,
        GenericCheckpointPhase::Terminal
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_approval_recovers_without_a_second_allow() {
    run_accepted_approval_recovery(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_denial_recovers_without_a_second_command() {
    run_accepted_approval_recovery(false).await;
}

async fn run_accepted_approval_recovery(allow: bool) {
    const SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
    let suffix = if allow { "allow" } else { "deny" };
    let run_id = RunId::new(format!("accepted-approval-{suffix}-recovery-run"));
    let command_id = CommandId::new(format!("accepted-approval-{suffix}-resolution"));
    let checkpoint_store = Arc::new(PausingCheckpointStore::at(
        CheckpointCrashCut::ApprovalRequestResolve,
    ));
    let effect_journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let host_journal = Arc::new(InMemoryAgentJournalStore::default());
    let tool = Arc::new(EchoTool {
        calls: AtomicUsize::new(0),
    });
    let bounds = ToolPolicyBounds {
        allowed_effects: BTreeSet::from([EffectScope::Process]),
        approval: ApprovalPolicy::Required,
        max_timeout_ms: Some(1_000),
        max_output_bytes: Some(1_024),
        ..ToolPolicyBounds::default()
    };
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let first_broker = Arc::new(
        InMemoryHostApprovalBroker::new(SIGNING_KEY).expect("first approval broker is valid"),
    );
    let first_provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools_approval_and_session_journal(
            Arc::new(ApprovalLoopModel {
                rounds: AtomicUsize::new(0),
                expect_allowed: allow,
            }),
            config.clone(),
            durable_approval_runtime(SIGNING_KEY, &bounds, effect_journal.clone(), tool.clone()),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            first_broker.clone(),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("first approval-capable Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let first_controller = Arc::new(
        AgentController::with_journal_store(
            first_provider,
            ProviderBindingRef::new("accepted-approval-recovery-binding"),
            host_journal.clone(),
        )
        .expect("first controller binds"),
    );
    let execution = first_controller
        .start(
            AgentRunEnvelope::new(
                AGENT_PROTOCOL_V1,
                AgentSessionId::new(format!("accepted-approval-{suffix}-recovery-session")),
                run_id.clone(),
                vec![Content::text("recover my accepted approval decision")],
            )
            .expect("valid approval Run"),
        )
        .await
        .expect("Run starts");
    let pending = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let view = first_controller
                .inspect(&run_id)
                .await
                .expect("Run remains inspectable");
            if let Some(request) = view.pending_requests.first() {
                break request.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first process opens the approval request");
    let resolution = if allow {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("wall clock follows Unix epoch")
            .as_millis() as i64;
        let grant_ref = first_broker
            .approve(&pending.request_id, now_ms + 60_000)
            .expect("Host issues an exact approval grant");
        RequestResolution::Approval {
            decision: ApprovalDecision::Allow,
            grant_ref: Some(grant_ref),
        }
    } else {
        RequestResolution::Approval {
            decision: ApprovalDecision::Deny,
            grant_ref: None,
        }
    };
    let paused = checkpoint_store.paused.notified();
    let ack = first_controller
        .command(
            AgentCommandEnvelope::new(
                command_id.clone(),
                run_id.clone(),
                Some(pending.request_id),
                AgentCommand::ResolveRequest {
                    response: resolution,
                },
            )
            .expect("valid approval resolution"),
        )
        .await
        .expect("approval decision is accepted");
    assert!(matches!(ack.state, CommandAckState::Accepted { .. }));
    tokio::time::timeout(std::time::Duration::from_secs(1), paused)
        .await
        .expect("RequestResolved reaches the injected crash cut");
    let projection = checkpoint_store
        .load_run(&run_id)
        .expect("private WAL remains readable")
        .expect("private Run remains registered")
        .validate()
        .expect("private WAL remains valid");
    let checkpointed = projection
        .commands
        .get(&command_id)
        .expect("accepted approval command is durable");
    assert_eq!(checkpointed.approval_capability.is_some(), allow);
    assert!(projection
        .provider_events
        .iter()
        .all(|event| !matches!(&event.payload, AgentEvent::RequestResolved { .. })));
    assert_eq!(tool.calls.load(Ordering::SeqCst), 0);

    checkpoint_store.release_as_crash();
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        first_controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("simulated process loss reaches the Host")
    .expect_err("accepted but unapplied approval is not terminal");
    assert!(matches!(
        error,
        AgentControlError::ContinuityUnknown(ref actual) if actual == &run_id
    ));
    drop(first_controller);
    checkpoint_store.allow_recovery_writes();

    let replacement_model = Arc::new(ApprovalLoopModel {
        rounds: AtomicUsize::new(1),
        expect_allowed: allow,
    });
    let replacement_provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools_approval_and_session_journal(
            replacement_model.clone(),
            config,
            durable_approval_runtime(SIGNING_KEY, &bounds, effect_journal, tool.clone()),
            RunToolGrant { bounds },
            Arc::new(
                InMemoryHostApprovalBroker::new(SIGNING_KEY)
                    .expect("replacement approval broker is valid"),
            ),
            session_journal,
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("replacement approval-capable Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("replacement Provider binds the same private WAL"),
    );
    let replacement_controller = Arc::new(
        AgentController::with_journal_store(
            replacement_provider,
            ProviderBindingRef::new("accepted-approval-recovery-binding"),
            host_journal,
        )
        .expect("replacement controller binds"),
    );
    replacement_controller
        .recover(&run_id)
        .await
        .expect("accepted approval decision is recoverable");
    let view = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        replacement_controller.wait_for_terminal(&run_id),
    )
    .await
    .expect("accepted approval continuation completes without another command")
    .expect("accepted approval continuation reaches Delivery");
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert!(view.pending_requests.is_empty());
    assert_eq!(tool.calls.load(Ordering::SeqCst), usize::from(allow));
    assert_eq!(replacement_model.rounds.load(Ordering::SeqCst), 2);
    assert!(matches!(
        replacement_controller
            .command_ack(&run_id, &command_id)
            .await
            .expect("original approval command remains queryable")
            .state,
        CommandAckState::Applied { .. }
    ));
    let events = replacement_controller
        .events(&run_id, 0)
        .await
        .expect("recovered Host Journal remains readable");
    assert_eq!(
        events
            .iter()
            .filter(|record| matches!(&record.event.payload, AgentEvent::RequestOpened { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|record| matches!(&record.event.payload, AgentEvent::RequestResolved { .. }))
            .count(),
        1
    );
    assert_eq!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("recovered Run remains registered")
            .validate()
            .expect("recovered WAL remains valid")
            .phase,
        GenericCheckpointPhase::Terminal
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_approval_resolution_recovers_before_tool_execution() {
    run_durable_approval_resolution_recovery(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_approval_denial_recovers_without_tool_execution() {
    run_durable_approval_resolution_recovery(false).await;
}

async fn run_durable_approval_resolution_recovery(allow: bool) {
    const SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
    let suffix = if allow { "allow" } else { "deny" };
    let run_id = RunId::new(format!("durable-approval-{suffix}-recovery-run"));
    let session_id = AgentSessionId::new(format!("durable-approval-{suffix}-recovery-session"));
    let command_id = CommandId::new(format!("durable-approval-{suffix}-resolution"));
    let checkpoint_store = Arc::new(AckLostAfterApprovalResolveCheckpointStore::default());
    let effect_journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let host_journal = Arc::new(InMemoryAgentJournalStore::default());
    let tool = Arc::new(EchoTool {
        calls: AtomicUsize::new(0),
    });
    let bounds = ToolPolicyBounds {
        allowed_effects: BTreeSet::from([EffectScope::Process]),
        approval: ApprovalPolicy::Required,
        max_timeout_ms: Some(1_000),
        max_output_bytes: Some(1_024),
        ..ToolPolicyBounds::default()
    };
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let first_broker = Arc::new(
        InMemoryHostApprovalBroker::new(SIGNING_KEY).expect("first approval broker is valid"),
    );
    let first_provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools_approval_and_session_journal(
            Arc::new(ApprovalLoopModel {
                rounds: AtomicUsize::new(0),
                expect_allowed: allow,
            }),
            config.clone(),
            durable_approval_runtime(SIGNING_KEY, &bounds, effect_journal.clone(), tool.clone()),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            first_broker.clone(),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("first approval-capable Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let first_controller = Arc::new(
        AgentController::with_journal_store(
            first_provider,
            ProviderBindingRef::new("durable-approval-recovery-binding"),
            host_journal.clone(),
        )
        .expect("first controller binds"),
    );
    let execution = first_controller
        .start(
            AgentRunEnvelope::new(
                AGENT_PROTOCOL_V1,
                session_id.clone(),
                run_id.clone(),
                vec![Content::text("recover my durable approval decision")],
            )
            .expect("valid approval Run"),
        )
        .await
        .expect("Run starts");
    let pending = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let view = first_controller
                .inspect(&run_id)
                .await
                .expect("Run remains inspectable");
            if let Some(request) = view.pending_requests.first() {
                break request.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first process opens the approval request");
    let resolution = if allow {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("wall clock follows Unix epoch")
            .as_millis() as i64;
        let grant_ref = first_broker
            .approve(&pending.request_id, now_ms + 60_000)
            .expect("Host issues an exact approval grant");
        RequestResolution::Approval {
            decision: ApprovalDecision::Allow,
            grant_ref: Some(grant_ref),
        }
    } else {
        RequestResolution::Approval {
            decision: ApprovalDecision::Deny,
            grant_ref: None,
        }
    };
    let ack = first_controller
        .command(
            AgentCommandEnvelope::new(
                command_id.clone(),
                run_id.clone(),
                Some(pending.request_id),
                AgentCommand::ResolveRequest {
                    response: resolution,
                },
            )
            .expect("valid approval resolution"),
        )
        .await
        .expect("approval resolution command is accepted");
    assert!(matches!(ack.state, CommandAckState::Accepted { .. }));
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        first_controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("lost RequestResolved acknowledgement reaches the Host")
    .expect_err("durable but unacknowledged approval resolution is not terminal");
    assert!(matches!(
        error,
        AgentControlError::ContinuityUnknown(ref actual) if actual == &run_id
    ));

    let stored = checkpoint_store
        .load_run(&run_id)
        .expect("private WAL remains readable")
        .expect("private Run remains registered");
    let projection = stored.validate().expect("private WAL remains valid");
    assert!(matches!(
        projection.phase,
        GenericCheckpointPhase::ModelAttemptObserved { round: 1, .. }
    ));
    assert_eq!(
        projection
            .provider_events
            .iter()
            .filter(|event| matches!(&event.payload, AgentEvent::RequestResolved { .. }))
            .count(),
        1
    );
    assert_eq!(
        projection
            .commands
            .get(&command_id)
            .expect("approval command is durable")
            .approval_capability
            .is_some(),
        allow
    );
    assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
    assert!(session_journal
        .load_session(&session_id)
        .await
        .expect("Session Journal remains readable")
        .iter()
        .all(|record| !matches!(
            &record.payload,
            AgentSessionEvent::ToolExchangeCommitted { .. }
        )));
    assert!(first_controller
        .events(&run_id, 0)
        .await
        .expect("Host Journal remains readable")
        .iter()
        .all(|record| !matches!(&record.event.payload, AgentEvent::RequestResolved { .. })));
    drop(first_controller);
    checkpoint_store.allow_recovery_writes();

    let replacement_model = Arc::new(ApprovalLoopModel {
        rounds: AtomicUsize::new(1),
        expect_allowed: allow,
    });
    let replacement_provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools_approval_and_session_journal(
            replacement_model.clone(),
            config,
            durable_approval_runtime(SIGNING_KEY, &bounds, effect_journal, tool.clone()),
            RunToolGrant { bounds },
            Arc::new(
                InMemoryHostApprovalBroker::new(SIGNING_KEY)
                    .expect("replacement approval broker is valid"),
            ),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("replacement approval-capable Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("replacement Provider binds the same private WAL"),
    );
    let replacement_controller = Arc::new(
        AgentController::with_journal_store(
            replacement_provider,
            ProviderBindingRef::new("durable-approval-recovery-binding"),
            host_journal,
        )
        .expect("replacement controller binds"),
    );
    replacement_controller
        .recover(&run_id)
        .await
        .expect("durable approval resolution is recoverable");
    let view = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        replacement_controller.wait_for_terminal(&run_id),
    )
    .await
    .expect("durable approval continuation completes without another command")
    .expect("durable approval continuation reaches Delivery");
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert!(view.pending_requests.is_empty());
    assert_eq!(tool.calls.load(Ordering::SeqCst), usize::from(allow));
    assert_eq!(replacement_model.rounds.load(Ordering::SeqCst), 2);
    assert!(matches!(
        replacement_controller
            .command_ack(&run_id, &command_id)
            .await
            .expect("original approval command remains queryable")
            .state,
        CommandAckState::Applied { .. }
    ));
    let events = replacement_controller
        .events(&run_id, 0)
        .await
        .expect("recovered Host Journal remains readable");
    assert_eq!(
        events
            .iter()
            .filter(|record| matches!(&record.event.payload, AgentEvent::RequestOpened { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|record| matches!(&record.event.payload, AgentEvent::RequestResolved { .. }))
            .count(),
        1
    );
    assert_eq!(
        session_journal
            .load_session(&session_id)
            .await
            .expect("recovered Session Journal remains readable")
            .iter()
            .filter(|record| matches!(
                &record.payload,
                AgentSessionEvent::ToolExchangeCommitted { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("recovered Run remains registered")
            .validate()
            .expect("recovered WAL remains valid")
            .phase,
        GenericCheckpointPhase::Terminal
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_approval_tool_exchange_recovers_without_reexecution() {
    run_committed_approval_exchange_recovery(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_denial_tool_exchange_recovers_without_reexecution() {
    run_committed_approval_exchange_recovery(false).await;
}

async fn run_committed_approval_exchange_recovery(allow: bool) {
    const SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
    let suffix = if allow { "allow" } else { "deny" };
    let run_id = RunId::new(format!("committed-approval-{suffix}-exchange-run"));
    let session_id = AgentSessionId::new(format!("committed-approval-{suffix}-exchange-session"));
    let command_id = CommandId::new(format!("committed-approval-{suffix}-exchange-resolution"));
    let checkpoint_store = Arc::new(PausingCheckpointStore::at(
        CheckpointCrashCut::ApprovalToolExchangeBoundary,
    ));
    let effect_journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let host_journal = Arc::new(InMemoryAgentJournalStore::default());
    let tool = Arc::new(EchoTool {
        calls: AtomicUsize::new(0),
    });
    let bounds = ToolPolicyBounds {
        allowed_effects: BTreeSet::from([EffectScope::Process]),
        approval: ApprovalPolicy::Required,
        max_timeout_ms: Some(1_000),
        max_output_bytes: Some(1_024),
        ..ToolPolicyBounds::default()
    };
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let first_broker = Arc::new(
        InMemoryHostApprovalBroker::new(SIGNING_KEY).expect("first approval broker is valid"),
    );
    let first_provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools_approval_and_session_journal(
            Arc::new(ApprovalLoopModel {
                rounds: AtomicUsize::new(0),
                expect_allowed: allow,
            }),
            config.clone(),
            durable_approval_runtime(SIGNING_KEY, &bounds, effect_journal.clone(), tool.clone()),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            first_broker.clone(),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("first approval-capable Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let first_controller = Arc::new(
        AgentController::with_journal_store(
            first_provider,
            ProviderBindingRef::new("committed-approval-exchange-binding"),
            host_journal.clone(),
        )
        .expect("first controller binds"),
    );
    let execution = first_controller
        .start(
            AgentRunEnvelope::new(
                AGENT_PROTOCOL_V1,
                session_id.clone(),
                run_id.clone(),
                vec![Content::text("recover my committed approval Tool exchange")],
            )
            .expect("valid approval Run"),
        )
        .await
        .expect("Run starts");
    let pending = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let view = first_controller
                .inspect(&run_id)
                .await
                .expect("Run remains inspectable");
            if let Some(request) = view.pending_requests.first() {
                break request.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first process opens the approval request");
    let resolution = if allow {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("wall clock follows Unix epoch")
            .as_millis() as i64;
        let grant_ref = first_broker
            .approve(&pending.request_id, now_ms + 60_000)
            .expect("Host issues an exact approval grant");
        RequestResolution::Approval {
            decision: ApprovalDecision::Allow,
            grant_ref: Some(grant_ref),
        }
    } else {
        RequestResolution::Approval {
            decision: ApprovalDecision::Deny,
            grant_ref: None,
        }
    };
    let paused = checkpoint_store.paused.notified();
    let ack = first_controller
        .command(
            AgentCommandEnvelope::new(
                command_id.clone(),
                run_id.clone(),
                Some(pending.request_id),
                AgentCommand::ResolveRequest {
                    response: resolution,
                },
            )
            .expect("valid approval resolution"),
        )
        .await
        .expect("approval resolution command is accepted");
    assert!(matches!(ack.state, CommandAckState::Accepted { .. }));
    tokio::time::timeout(std::time::Duration::from_secs(1), paused)
        .await
        .expect("Session Tool exchange reaches the next private boundary");

    let projection = checkpoint_store
        .load_run(&run_id)
        .expect("private WAL remains readable")
        .expect("private Run remains registered")
        .validate()
        .expect("private WAL remains valid");
    assert!(matches!(
        projection.phase,
        GenericCheckpointPhase::ModelAttemptObserved { round: 1, .. }
    ));
    assert_eq!(
        projection
            .provider_events
            .iter()
            .filter(|event| matches!(&event.payload, AgentEvent::RequestResolved { .. }))
            .count(),
        1
    );
    assert_eq!(
        session_journal
            .load_session(&session_id)
            .await
            .expect("Session Journal remains readable")
            .iter()
            .filter(|record| matches!(
                &record.payload,
                AgentSessionEvent::ToolExchangeCommitted { .. }
            ))
            .count(),
        1
    );
    assert_eq!(tool.calls.load(Ordering::SeqCst), usize::from(allow));

    checkpoint_store.release_as_crash();
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        first_controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("simulated boundary process loss reaches the Host")
    .expect_err("uncommitted private loop boundary is not terminal");
    assert!(matches!(
        error,
        AgentControlError::ContinuityUnknown(ref actual) if actual == &run_id
    ));
    drop(first_controller);
    checkpoint_store.allow_recovery_writes();

    let replacement_model = Arc::new(ApprovalLoopModel {
        rounds: AtomicUsize::new(1),
        expect_allowed: allow,
    });
    let replacement_provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools_approval_and_session_journal(
            replacement_model.clone(),
            config,
            durable_approval_runtime(SIGNING_KEY, &bounds, effect_journal, tool.clone()),
            RunToolGrant { bounds },
            Arc::new(
                InMemoryHostApprovalBroker::new(SIGNING_KEY)
                    .expect("replacement approval broker is valid"),
            ),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("replacement approval-capable Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("replacement Provider binds the same private WAL"),
    );
    let replacement_controller = Arc::new(
        AgentController::with_journal_store(
            replacement_provider,
            ProviderBindingRef::new("committed-approval-exchange-binding"),
            host_journal,
        )
        .expect("replacement controller binds"),
    );
    replacement_controller
        .recover(&run_id)
        .await
        .expect("committed approval Tool exchange is recoverable");
    let view = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        replacement_controller.wait_for_terminal(&run_id),
    )
    .await
    .expect("recovered Session exchange advances without another approval")
    .expect("recovered Session exchange reaches Delivery");
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert!(view.pending_requests.is_empty());
    assert_eq!(tool.calls.load(Ordering::SeqCst), usize::from(allow));
    assert_eq!(replacement_model.rounds.load(Ordering::SeqCst), 2);
    assert!(matches!(
        replacement_controller
            .command_ack(&run_id, &command_id)
            .await
            .expect("original approval command remains queryable")
            .state,
        CommandAckState::Applied { .. }
    ));
    let events = replacement_controller
        .events(&run_id, 0)
        .await
        .expect("recovered Host Journal remains readable");
    assert_eq!(
        events
            .iter()
            .filter(|record| matches!(&record.event.payload, AgentEvent::RequestOpened { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|record| matches!(&record.event.payload, AgentEvent::RequestResolved { .. }))
            .count(),
        1
    );
    assert_eq!(
        session_journal
            .load_session(&session_id)
            .await
            .expect("recovered Session Journal remains readable")
            .iter()
            .filter(|record| matches!(
                &record.payload,
                AgentSessionEvent::ToolExchangeCommitted { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("recovered Run remains registered")
            .validate()
            .expect("recovered WAL remains valid")
            .phase,
        GenericCheckpointPhase::Terminal
    );
}

#[derive(Clone, Copy)]
enum DirectEffectRecoveryState {
    Fresh,
    Committed,
    Unknown,
}

#[derive(Clone, Copy)]
enum SkillRecoveryState {
    Fresh,
    ActivationCommitted,
    ExchangeCommitted,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observed_workflow_starts_once_after_recovery() {
    let run_id = RunId::new("workflow-observed-recovery-run");
    let session_id = AgentSessionId::new("workflow-observed-recovery-session");
    let checkpoint_store = Arc::new(AckLostAfterModelObservationCheckpointStore::default());
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let host_journal = Arc::new(InMemoryAgentJournalStore::default());
    let bounds = ToolPolicyBounds {
        approval: ApprovalPolicy::NotRequired,
        max_timeout_ms: Some(5_000),
        max_output_bytes: Some(16 * 1024),
        ..ToolPolicyBounds::default()
    };
    let echo = Arc::new(EchoTool {
        calls: AtomicUsize::new(0),
    });
    let runtime = durable_direct_runtime(
        &bounds,
        Arc::new(InMemoryToolEffectJournalStore::default()),
        echo.clone(),
    );
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let first_provider = Arc::new(
        InternalGenericAgentProvider::new_with_workflow_and_session_journal(
            Arc::new(WorkflowLoopModel {
                rounds: AtomicUsize::new(0),
            }),
            config.clone(),
            runtime.clone(),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            workflow_recovery_strategy(runtime.clone()),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("first Workflow-capable Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let first_controller = Arc::new(
        AgentController::with_journal_store(
            first_provider,
            ProviderBindingRef::new("workflow-recovery-binding"),
            host_journal.clone(),
        )
        .expect("first Workflow controller binds"),
    );
    let execution = first_controller
        .start(
            AgentRunEnvelope::new(
                AGENT_PROTOCOL_V1,
                session_id.clone(),
                run_id.clone(),
                vec![Content::text("recover the observed Workflow")],
            )
            .expect("valid Workflow recovery Run"),
        )
        .await
        .expect("Workflow recovery Run starts");
    let error = first_controller
        .wait_for_terminal(&execution.run_id)
        .await
        .expect_err("lost observation acknowledgement leaves continuity unknown");
    assert!(matches!(
        error,
        AgentControlError::ContinuityUnknown(ref actual) if actual == &run_id
    ));
    assert_eq!(echo.calls.load(Ordering::SeqCst), 0);
    drop(first_controller);
    checkpoint_store.allow_recovery_writes();

    let replacement_model = Arc::new(WorkflowLoopModel {
        rounds: AtomicUsize::new(1),
    });
    let replacement_provider = Arc::new(
        InternalGenericAgentProvider::new_with_workflow_and_session_journal(
            replacement_model.clone(),
            config,
            runtime.clone(),
            RunToolGrant { bounds },
            workflow_recovery_strategy(runtime),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("replacement Workflow-capable Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("replacement Provider binds the same private WAL"),
    );
    let replacement_controller = Arc::new(
        AgentController::with_journal_store(
            replacement_provider,
            ProviderBindingRef::new("workflow-recovery-binding"),
            host_journal,
        )
        .expect("replacement Workflow controller binds"),
    );
    replacement_controller
        .recover(&run_id)
        .await
        .expect("unstarted observed Workflow is recoverable");
    let view = replacement_controller
        .wait_for_terminal(&run_id)
        .await
        .expect("recovered Workflow delivers");
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert_eq!(echo.calls.load(Ordering::SeqCst), 2);
    assert_eq!(replacement_model.rounds.load(Ordering::SeqCst), 2);
    assert_eq!(
        session_journal
            .load_session(&session_id)
            .await
            .expect("Workflow Session remains readable")
            .iter()
            .filter(|record| matches!(
                record.payload,
                AgentSessionEvent::ToolExchangeCommitted { .. }
            ))
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_workflow_start_fence_replays_only_after_global_effect_preflight() {
    let run_id = RunId::new("workflow-start-fence-run");
    let checkpoint_store = Arc::new(AckLostAfterWorkflowStartCheckpointStore::default());
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let host_journal = Arc::new(InMemoryAgentJournalStore::default());
    let bounds = ToolPolicyBounds {
        approval: ApprovalPolicy::NotRequired,
        max_timeout_ms: Some(5_000),
        max_output_bytes: Some(16 * 1024),
        ..ToolPolicyBounds::default()
    };
    let echo = Arc::new(EchoTool {
        calls: AtomicUsize::new(0),
    });
    let runtime = durable_direct_runtime(
        &bounds,
        Arc::new(InMemoryToolEffectJournalStore::default()),
        echo.clone(),
    );
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let first_provider = Arc::new(
        InternalGenericAgentProvider::new_with_workflow_and_session_journal(
            Arc::new(WorkflowLoopModel {
                rounds: AtomicUsize::new(0),
            }),
            config.clone(),
            runtime.clone(),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            workflow_recovery_strategy(runtime.clone()),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("first fenced Workflow Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let first_controller = Arc::new(
        AgentController::with_journal_store(
            first_provider,
            ProviderBindingRef::new("workflow-fence-binding"),
            host_journal.clone(),
        )
        .expect("first fenced Workflow controller binds"),
    );
    let execution = first_controller
        .start(
            AgentRunEnvelope::new(
                AGENT_PROTOCOL_V1,
                AgentSessionId::new("workflow-start-fence-session"),
                run_id.clone(),
                vec![Content::text("fence the Workflow before execution")],
            )
            .expect("valid fenced Workflow Run"),
        )
        .await
        .expect("fenced Workflow Run starts");
    let error = first_controller
        .wait_for_terminal(&execution.run_id)
        .await
        .expect_err("lost Workflow fence acknowledgement leaves continuity unknown");
    assert!(matches!(
        error,
        AgentControlError::ContinuityUnknown(ref actual) if actual == &run_id
    ));
    assert_eq!(echo.calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("private Run remains registered")
            .validate()
            .expect("private WAL remains valid")
            .phase,
        GenericCheckpointPhase::WorkflowAttemptOpen { .. }
    ));
    drop(first_controller);
    checkpoint_store.allow_recovery_writes();

    let replacement_model = Arc::new(WorkflowLoopModel {
        rounds: AtomicUsize::new(1),
    });
    let replacement_provider = Arc::new(
        InternalGenericAgentProvider::new_with_workflow_and_session_journal(
            replacement_model.clone(),
            config,
            runtime.clone(),
            RunToolGrant { bounds },
            workflow_recovery_strategy(runtime),
            session_journal,
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("replacement fenced Workflow Agent starts")
        .with_checkpoint_store(checkpoint_store)
        .expect("replacement Provider binds the same private WAL"),
    );
    let replacement_controller = Arc::new(
        AgentController::with_journal_store(
            replacement_provider,
            ProviderBindingRef::new("workflow-fence-binding"),
            host_journal,
        )
        .expect("replacement fenced Workflow controller binds"),
    );
    replacement_controller
        .recover(&run_id)
        .await
        .expect("fenced Workflow with no unresolved effect is recoverable");
    let view = replacement_controller
        .wait_for_terminal(&run_id)
        .await
        .expect("recovered fenced Workflow delivers");
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert_eq!(echo.calls.load(Ordering::SeqCst), 2);
    assert_eq!(replacement_model.rounds.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_workflow_output_recovers_without_rerunning_the_dag() {
    let run_id = RunId::new("workflow-output-recovery-run");
    let session_id = AgentSessionId::new("workflow-output-recovery-session");
    let checkpoint_store = Arc::new(AckLostAfterWorkflowOutputCheckpointStore::default());
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let host_journal = Arc::new(InMemoryAgentJournalStore::default());
    let bounds = ToolPolicyBounds {
        approval: ApprovalPolicy::NotRequired,
        max_timeout_ms: Some(5_000),
        max_output_bytes: Some(16 * 1024),
        ..ToolPolicyBounds::default()
    };
    let first_echo = Arc::new(EchoTool {
        calls: AtomicUsize::new(0),
    });
    let first_runtime = durable_direct_runtime(
        &bounds,
        Arc::new(InMemoryToolEffectJournalStore::default()),
        first_echo.clone(),
    );
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let first_provider = Arc::new(
        InternalGenericAgentProvider::new_with_workflow_and_session_journal(
            Arc::new(WorkflowLoopModel {
                rounds: AtomicUsize::new(0),
            }),
            config.clone(),
            first_runtime.clone(),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            workflow_recovery_strategy(first_runtime),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("first Workflow output Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let first_controller = Arc::new(
        AgentController::with_journal_store(
            first_provider,
            ProviderBindingRef::new("workflow-output-binding"),
            host_journal.clone(),
        )
        .expect("first Workflow output controller binds"),
    );
    let execution = first_controller
        .start(
            AgentRunEnvelope::new(
                AGENT_PROTOCOL_V1,
                session_id.clone(),
                run_id.clone(),
                vec![Content::text("recover the committed Workflow output")],
            )
            .expect("valid Workflow output recovery Run"),
        )
        .await
        .expect("Workflow output recovery Run starts");
    let error = first_controller
        .wait_for_terminal(&execution.run_id)
        .await
        .expect_err("lost Workflow output acknowledgement leaves continuity unknown");
    assert!(matches!(
        error,
        AgentControlError::ContinuityUnknown(ref actual) if actual == &run_id
    ));
    assert_eq!(first_echo.calls.load(Ordering::SeqCst), 2);
    assert!(session_journal
        .load_session(&session_id)
        .await
        .expect("Session remains readable before recovery")
        .iter()
        .all(|record| !matches!(
            record.payload,
            AgentSessionEvent::ToolExchangeCommitted { .. }
        )));
    drop(first_controller);
    checkpoint_store.allow_recovery_writes();

    let replacement_echo = Arc::new(EchoTool {
        calls: AtomicUsize::new(0),
    });
    let replacement_runtime = durable_direct_runtime(
        &bounds,
        Arc::new(InMemoryToolEffectJournalStore::default()),
        replacement_echo.clone(),
    );
    let replacement_model = Arc::new(WorkflowLoopModel {
        rounds: AtomicUsize::new(1),
    });
    let replacement_provider = Arc::new(
        InternalGenericAgentProvider::new_with_workflow_and_session_journal(
            replacement_model.clone(),
            config,
            replacement_runtime.clone(),
            RunToolGrant { bounds },
            workflow_recovery_strategy(replacement_runtime),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("replacement Workflow output Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("replacement Provider binds the same private WAL"),
    );
    let replacement_controller = Arc::new(
        AgentController::with_journal_store(
            replacement_provider,
            ProviderBindingRef::new("workflow-output-binding"),
            host_journal,
        )
        .expect("replacement Workflow output controller binds"),
    );
    replacement_controller
        .recover(&run_id)
        .await
        .expect("durable Workflow output is recoverable");
    let view = replacement_controller
        .wait_for_terminal(&run_id)
        .await
        .expect("recovered Workflow output delivers");
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert_eq!(first_echo.calls.load(Ordering::SeqCst), 2);
    assert_eq!(replacement_echo.calls.load(Ordering::SeqCst), 0);
    assert_eq!(replacement_model.rounds.load(Ordering::SeqCst), 2);
    assert_eq!(
        view.delivery
            .and_then(|delivery| delivery.usage)
            .and_then(|usage| usage.tool_calls),
        Some(3)
    );
    assert_eq!(
        session_journal
            .load_session(&session_id)
            .await
            .expect("recovered Workflow Session remains readable")
            .iter()
            .filter(|record| matches!(
                record.payload,
                AgentSessionEvent::ToolExchangeCommitted { .. }
            ))
            .count(),
        1
    );
}

fn workflow_recovery_strategy(
    runtime: Arc<GuardedToolRuntime<InMemoryApprovalCapabilityStore>>,
) -> Arc<WorkflowExecutionStrategy> {
    let mut normalizer = PlanNormalizer::new();
    normalizer.register_action("echo");
    Arc::new(WorkflowExecutionStrategy::new(
        Arc::new(normalizer),
        Arc::new(Executor::new()),
        runtime,
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observed_skill_activation_recovers_from_each_durable_session_boundary() {
    let uninterrupted = run_uninterrupted_skill_activation().await;
    for state in [
        SkillRecoveryState::Fresh,
        SkillRecoveryState::ActivationCommitted,
        SkillRecoveryState::ExchangeCommitted,
    ] {
        let recovered = run_skill_recovery(state).await;
        assert_eq!(
            recovered, uninterrupted,
            "online and replayed Session projections must be identical"
        );
    }
}

async fn run_uninterrupted_skill_activation() -> Vec<ModelMessage> {
    let skills = Arc::new(recovery_skill_runtime());
    let round_two_messages = Arc::new(Mutex::new(None));
    let model = Arc::new(SkillRecoveryModel {
        rounds: AtomicUsize::new(0),
        digest: skills.catalog().skills[0].digest.to_string(),
        round_two_messages: round_two_messages.clone(),
    });
    let provider = Arc::new(
        InternalGenericAgentProvider::new_with_skills_and_session_journal(
            model,
            GenericAgentConfig::new("internal-provider", "generic-agent"),
            skills.clone(),
            Arc::new(InMemoryAgentSessionJournalStore::default()),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("uninterrupted Skill-capable Generic Agent starts"),
    );
    let controller = Arc::new(
        AgentController::new(provider, ProviderBindingRef::new("skill-recovery-binding"))
            .expect("uninterrupted controller binds"),
    );
    let execution = controller
        .start(bound_skill_recovery_run(
            &skills,
            AgentSessionId::new("skill-uninterrupted-session"),
            RunId::new("skill-uninterrupted-run"),
        ))
        .await
        .expect("uninterrupted Skill Run starts");
    let view = controller
        .wait_for_terminal(&execution.run_id)
        .await
        .expect("uninterrupted Skill Run terminates");
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    let captured = round_two_messages
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        .expect("uninterrupted second model request is captured");
    captured
}

async fn run_skill_recovery(state: SkillRecoveryState) -> Vec<ModelMessage> {
    let suffix = match state {
        SkillRecoveryState::Fresh => "fresh",
        SkillRecoveryState::ActivationCommitted => "activation",
        SkillRecoveryState::ExchangeCommitted => "exchange",
    };
    let run_id = RunId::new(format!("skill-{suffix}-recovery-run"));
    let session_id = AgentSessionId::new(format!("skill-{suffix}-recovery-session"));
    let skills = Arc::new(recovery_skill_runtime());
    let digest = skills.catalog().skills[0].digest.clone();
    let checkpoint_store = Arc::new(AckLostAfterModelObservationCheckpointStore::default());
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let host_journal = Arc::new(InMemoryAgentJournalStore::default());
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let first_provider = Arc::new(
        InternalGenericAgentProvider::new_with_skills_and_session_journal(
            Arc::new(SkillRecoveryModel {
                rounds: AtomicUsize::new(0),
                digest: digest.to_string(),
                round_two_messages: Arc::new(Mutex::new(None)),
            }),
            config.clone(),
            skills.clone(),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("first Skill-capable Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let first_controller = Arc::new(
        AgentController::with_journal_store(
            first_provider,
            ProviderBindingRef::new("skill-recovery-binding"),
            host_journal.clone(),
        )
        .expect("first controller binds"),
    );
    let execution = first_controller
        .start(bound_skill_recovery_run(
            &skills,
            session_id.clone(),
            run_id.clone(),
        ))
        .await
        .expect("Skill recovery Run starts");
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        first_controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("lost model-observation acknowledgement reaches the Host")
    .expect_err("observed Skill call is not terminal");
    assert!(matches!(
        error,
        AgentControlError::ContinuityUnknown(ref actual) if actual == &run_id
    ));
    drop(first_controller);

    if matches!(
        state,
        SkillRecoveryState::ActivationCommitted | SkillRecoveryState::ExchangeCommitted
    ) {
        let activation = match skills
            .activate(
                SkillActivationRequest {
                    name: "recovery-skill".to_owned(),
                    expected_digest: digest.clone(),
                    reason: "recover the exact Skill activation".to_owned(),
                },
                &ActivatedSkillSet::default(),
            )
            .expect("bound Skill remains activatable")
        {
            SkillActivationOutcome::Activated(activation) => activation,
            SkillActivationOutcome::AlreadyActive(_) => {
                panic!("fresh Session cannot already contain the Skill")
            }
        };
        session_journal
            .append(AgentSessionEventDraft {
                event_id: AgentSessionEventId::new(format!(
                    "generic-{}-skill-1-activate-recovery-skill",
                    run_id.as_str()
                )),
                session_id: session_id.clone(),
                run_id: run_id.clone(),
                payload: AgentSessionEvent::SkillActivated {
                    activation: Box::new(activation.clone()),
                },
            })
            .await
            .expect("Skill activation is durably seeded");
        if matches!(state, SkillRecoveryState::ExchangeCommitted) {
            let descriptor = &activation.package.descriptor;
            let arguments = json!({
                "name": "recovery-skill",
                "expected_digest": digest,
                "reason": "recover the exact Skill activation"
            });
            session_journal
                .append(AgentSessionEventDraft {
                    event_id: AgentSessionEventId::new(format!(
                        "generic-{}-tool-exchange-1",
                        run_id.as_str()
                    )),
                    session_id: session_id.clone(),
                    run_id: run_id.clone(),
                    payload: AgentSessionEvent::ToolExchangeCommitted {
                        request_id: ModelRequestId::new(format!("model-{}-1", run_id.as_str())),
                        assistant: ModelMessage {
                            role: ModelRole::Assistant,
                            content: vec![ModelContent::ToolCall {
                                call_id: ModelToolCallId::new("activate-recovery-skill"),
                                name: "orchestral_skill_activate".to_owned(),
                                arguments,
                            }],
                        },
                        tool: ModelMessage {
                            role: ModelRole::Tool,
                            content: vec![ModelContent::ToolResult {
                                call_id: ModelToolCallId::new("activate-recovery-skill"),
                                result: json!({
                                    "status": "activated",
                                    "name": descriptor.name,
                                    "skill_id": descriptor.skill_id,
                                    "version": descriptor.version,
                                    "digest": descriptor.digest,
                                    "source": descriptor.source,
                                    "trust": descriptor.trust,
                                }),
                                is_error: false,
                            }],
                        },
                        usage: None,
                    },
                })
                .await
                .expect("Skill Tool exchange is durably seeded");
        }
    }
    checkpoint_store.allow_recovery_writes();

    let round_two_messages = Arc::new(Mutex::new(None));
    let replacement_model = Arc::new(SkillRecoveryModel {
        rounds: AtomicUsize::new(1),
        digest: skills.catalog().skills[0].digest.to_string(),
        round_two_messages: round_two_messages.clone(),
    });
    let replacement_provider = Arc::new(
        InternalGenericAgentProvider::new_with_skills_and_session_journal(
            replacement_model.clone(),
            config,
            skills,
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("replacement Skill-capable Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("replacement Provider binds the same private WAL"),
    );
    let replacement_controller = Arc::new(
        AgentController::with_journal_store(
            replacement_provider,
            ProviderBindingRef::new("skill-recovery-binding"),
            host_journal,
        )
        .expect("replacement controller binds"),
    );
    replacement_controller
        .recover(&run_id)
        .await
        .expect("observed Skill activation is recoverable");
    let view = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        replacement_controller.wait_for_terminal(&run_id),
    )
    .await
    .expect("Skill recovery reaches a terminal promptly")
    .expect("Skill recovery remains authoritative");
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert_eq!(replacement_model.rounds.load(Ordering::SeqCst), 2);
    let records = session_journal
        .load_session(&session_id)
        .await
        .expect("recovered Skill Session remains readable");
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record.payload, AgentSessionEvent::SkillActivated { .. }))
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(
                record.payload,
                AgentSessionEvent::ToolExchangeCommitted { .. }
            ))
            .count(),
        1
    );
    let captured = round_two_messages
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        .expect("recovered second model request is captured");
    captured
}

fn recovery_skill_runtime() -> SkillRuntime {
    let package = SkillPackage::seal(
        SkillId::new("recovery-skill"),
        "recovery-skill",
        "Recovery Skill",
        Some("1.0.0".to_owned()),
        SkillSource {
            kind: SkillSourceKind::BuiltIn,
            locator: "builtin:recovery-skill".to_owned(),
        },
        SkillTrustLevel::BuiltIn,
        SkillCompatibility::default(),
        SkillDependencies::default(),
        RECOVERY_SKILL_INSTRUCTIONS,
    )
    .expect("valid recovery Skill package");
    SkillRuntime::from_packages(
        ResourceId::new("recovery-skills"),
        vec![package],
        SkillHostProfile::current(),
        SkillActivationPolicy::default(),
    )
    .expect("valid recovery Skill catalog")
}

fn bound_skill_recovery_run(
    skills: &SkillRuntime,
    session_id: AgentSessionId,
    run_id: RunId,
) -> AgentRunEnvelope {
    let mut run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        session_id,
        run_id,
        vec![Content::text("recover this Skill activation")],
    )
    .expect("valid Skill recovery Run");
    run.spec.resources = vec![ResourceBinding {
        binding_id: ResourceBindingId::new("skills"),
        resource: ResourceRef {
            kind: ResourceKind::new(SKILL_CATALOG_RESOURCE_KIND_V1),
            id: skills.catalog().resource_id.clone(),
            revision: ResourceRevision::new(skills.catalog().revision.as_str()),
        },
        requirement: BindingRequirement::Required,
        mode: ResourceBindingMode::Snapshot,
    }];
    AgentRunEnvelope::seal(run.spec).expect("Skill recovery binding reseals")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observed_direct_tool_executes_once_after_recovery() {
    run_direct_tool_recovery(DirectEffectRecoveryState::Fresh).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_direct_tool_effect_replays_without_reexecution() {
    run_direct_tool_recovery(DirectEffectRecoveryState::Committed).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_direct_tool_effect_fails_without_reexecution() {
    run_direct_tool_recovery(DirectEffectRecoveryState::Unknown).await;
}

async fn run_direct_tool_recovery(effect_state: DirectEffectRecoveryState) {
    let suffix = match effect_state {
        DirectEffectRecoveryState::Fresh => "fresh",
        DirectEffectRecoveryState::Committed => "committed",
        DirectEffectRecoveryState::Unknown => "unknown",
    };
    let run_id = RunId::new(format!("direct-tool-{suffix}-recovery-run"));
    let session_id = AgentSessionId::new(format!("direct-tool-{suffix}-recovery-session"));
    let checkpoint_store = Arc::new(AckLostAfterModelObservationCheckpointStore::default());
    let effect_journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let host_journal = Arc::new(InMemoryAgentJournalStore::default());
    let tool = Arc::new(EchoTool {
        calls: AtomicUsize::new(0),
    });
    let bounds = ToolPolicyBounds {
        approval: ApprovalPolicy::NotRequired,
        max_timeout_ms: Some(1_000),
        max_output_bytes: Some(1_024),
        ..ToolPolicyBounds::default()
    };
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let first_provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools_and_session_journal(
            Arc::new(ToolLoopModel {
                rounds: AtomicUsize::new(0),
            }),
            config.clone(),
            durable_direct_runtime(&bounds, effect_journal.clone(), tool.clone()),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("first direct-Tool Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let first_controller = Arc::new(
        AgentController::with_journal_store(
            first_provider,
            ProviderBindingRef::new("direct-tool-recovery-binding"),
            host_journal.clone(),
        )
        .expect("first controller binds"),
    );
    let execution = first_controller
        .start(
            AgentRunEnvelope::new(
                AGENT_PROTOCOL_V1,
                session_id.clone(),
                run_id.clone(),
                vec![Content::text("recover this direct Tool call")],
            )
            .expect("valid direct Tool Run"),
        )
        .await
        .expect("Run starts");
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        first_controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("lost model-observation acknowledgement reaches the Host")
    .expect_err("observed but unfinished direct Tool Run is not terminal");
    assert!(matches!(
        error,
        AgentControlError::ContinuityUnknown(ref actual) if actual == &run_id
    ));
    assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("private Run remains registered")
            .validate()
            .expect("private WAL remains valid")
            .phase,
        GenericCheckpointPhase::ModelAttemptObserved { round: 1, .. }
    ));
    drop(first_controller);
    checkpoint_store.allow_recovery_writes();

    let invocation = ToolInvocation {
        run_id: run_id.clone(),
        call_id: ToolCallId::new("echo-call"),
        tool_id: ToolId::new("test/echo"),
        arguments: json!({ "value": "hello" }),
    };
    match effect_state {
        DirectEffectRecoveryState::Fresh => {}
        DirectEffectRecoveryState::Committed => {
            let runtime = durable_direct_runtime(&bounds, effect_journal.clone(), tool.clone());
            let result = runtime
                .invoke(
                    invocation.clone(),
                    RunToolGrant {
                        bounds: bounds.clone(),
                    },
                    None,
                    CancellationToken::new(),
                )
                .await;
            assert!(matches!(
                result,
                GuardedToolResult::Outcome {
                    outcome: ToolOutcome::Completed { .. },
                    cached: false,
                }
            ));
            assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
        }
        DirectEffectRecoveryState::Unknown => {
            let runtime = durable_direct_runtime(
                &bounds,
                effect_journal.clone(),
                Arc::new(UnknownEffectTool),
            );
            let unknown = runtime
                .invoke(
                    invocation,
                    RunToolGrant {
                        bounds: bounds.clone(),
                    },
                    None,
                    CancellationToken::new(),
                )
                .await;
            assert!(matches!(
                unknown,
                GuardedToolResult::Outcome {
                    outcome: ToolOutcome::UnknownEffect { .. },
                    ..
                }
            ));
            assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
        }
    }

    let replacement_model = Arc::new(ToolLoopModel {
        rounds: AtomicUsize::new(1),
    });
    let replacement_provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools_and_session_journal(
            replacement_model.clone(),
            config,
            durable_direct_runtime(&bounds, effect_journal, tool.clone()),
            RunToolGrant { bounds },
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("replacement direct-Tool Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("replacement Provider binds the same private WAL"),
    );
    let replacement_controller = Arc::new(
        AgentController::with_journal_store(
            replacement_provider,
            ProviderBindingRef::new("direct-tool-recovery-binding"),
            host_journal,
        )
        .expect("replacement controller binds"),
    );
    replacement_controller
        .recover(&run_id)
        .await
        .expect("observed direct Tool call is recoverable");
    let view = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        replacement_controller.wait_for_terminal(&run_id),
    )
    .await
    .expect("direct Tool recovery reaches a terminal promptly")
    .expect("direct Tool recovery remains authoritative");

    match effect_state {
        DirectEffectRecoveryState::Fresh | DirectEffectRecoveryState::Committed => {
            assert_eq!(view.state.status(), AgentRunStatus::Delivered);
            assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
            assert_eq!(replacement_model.rounds.load(Ordering::SeqCst), 2);
            assert_eq!(
                session_journal
                    .load_session(&session_id)
                    .await
                    .expect("recovered Session Journal remains readable")
                    .iter()
                    .filter(|record| matches!(
                        &record.payload,
                        AgentSessionEvent::ToolExchangeCommitted { .. }
                    ))
                    .count(),
                1
            );
        }
        DirectEffectRecoveryState::Unknown => {
            assert_eq!(view.state.status(), AgentRunStatus::Failed);
            assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
            assert_eq!(replacement_model.rounds.load(Ordering::SeqCst), 1);
            assert!(session_journal
                .load_session(&session_id)
                .await
                .expect("failed Session Journal remains readable")
                .iter()
                .all(|record| !matches!(
                    &record.payload,
                    AgentSessionEvent::ToolExchangeCommitted { .. }
                )));
        }
    }
    assert_eq!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("recovered Run remains registered")
            .validate()
            .expect("recovered WAL remains valid")
            .phase,
        GenericCheckpointPhase::Terminal
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_direct_tool_exchange_recovers_without_reexecution() {
    run_committed_direct_exchange_recovery(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_tool_exchange_without_effect_evidence_is_rejected_without_execution() {
    run_committed_direct_exchange_recovery(false).await;
}

async fn run_committed_direct_exchange_recovery(retain_effect: bool) {
    let suffix = if retain_effect { "durable" } else { "missing" };
    let run_id = RunId::new(format!("committed-direct-{suffix}-exchange-run"));
    let session_id = AgentSessionId::new(format!("committed-direct-{suffix}-exchange-session"));
    let checkpoint_store = Arc::new(PausingCheckpointStore::at(
        CheckpointCrashCut::DirectToolExchangeBoundary,
    ));
    let effect_journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let host_journal = Arc::new(InMemoryAgentJournalStore::default());
    let first_tool = Arc::new(EchoTool {
        calls: AtomicUsize::new(0),
    });
    let bounds = ToolPolicyBounds {
        approval: ApprovalPolicy::NotRequired,
        max_timeout_ms: Some(1_000),
        max_output_bytes: Some(1_024),
        ..ToolPolicyBounds::default()
    };
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let first_provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools_and_session_journal(
            Arc::new(ToolLoopModel {
                rounds: AtomicUsize::new(0),
            }),
            config.clone(),
            durable_direct_runtime(&bounds, effect_journal.clone(), first_tool.clone()),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("first direct-Tool Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let first_controller = Arc::new(
        AgentController::with_journal_store(
            first_provider,
            ProviderBindingRef::new("committed-direct-exchange-binding"),
            host_journal.clone(),
        )
        .expect("first controller binds"),
    );
    let execution = first_controller
        .start(
            AgentRunEnvelope::new(
                AGENT_PROTOCOL_V1,
                session_id.clone(),
                run_id.clone(),
                vec![Content::text("recover this committed direct Tool exchange")],
            )
            .expect("valid direct Tool Run"),
        )
        .await
        .expect("Run starts");
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if checkpoint_store
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .paused_once
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Session Tool exchange reaches the next private boundary");
    assert_eq!(first_tool.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        session_journal
            .load_session(&session_id)
            .await
            .expect("Session Journal remains readable")
            .iter()
            .filter(|record| matches!(
                &record.payload,
                AgentSessionEvent::ToolExchangeCommitted { .. }
            ))
            .count(),
        1
    );
    assert!(matches!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("private Run remains registered")
            .validate()
            .expect("private WAL remains valid")
            .phase,
        GenericCheckpointPhase::ModelAttemptObserved { round: 1, .. }
    ));

    checkpoint_store.release_as_crash();
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        first_controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("simulated boundary process loss reaches the Host")
    .expect_err("uncommitted private loop boundary is not terminal");
    assert!(matches!(
        error,
        AgentControlError::ContinuityUnknown(ref actual) if actual == &run_id
    ));
    drop(first_controller);
    checkpoint_store.allow_recovery_writes();

    let replacement_tool = Arc::new(EchoTool {
        calls: AtomicUsize::new(0),
    });
    let replacement_model = Arc::new(ToolLoopModel {
        rounds: AtomicUsize::new(1),
    });
    let recovery_effect_journal = if retain_effect {
        effect_journal
    } else {
        Arc::new(InMemoryToolEffectJournalStore::default())
    };
    let replacement_provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools_and_session_journal(
            replacement_model.clone(),
            config,
            durable_direct_runtime(&bounds, recovery_effect_journal, replacement_tool.clone()),
            RunToolGrant { bounds },
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("replacement direct-Tool Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("replacement Provider binds the same private WAL"),
    );
    let replacement_controller = Arc::new(
        AgentController::with_journal_store(
            replacement_provider,
            ProviderBindingRef::new("committed-direct-exchange-binding"),
            host_journal,
        )
        .expect("replacement controller binds"),
    );
    let recovery = replacement_controller.recover(&run_id).await;
    assert_eq!(replacement_tool.calls.load(Ordering::SeqCst), 0);
    if !retain_effect {
        assert!(recovery.is_err());
        assert_eq!(replacement_model.rounds.load(Ordering::SeqCst), 1);
        assert!(matches!(
            checkpoint_store
                .load_run(&run_id)
                .expect("private WAL remains readable")
                .expect("private Run remains registered")
                .validate()
                .expect("private WAL remains valid")
                .phase,
            GenericCheckpointPhase::ModelAttemptObserved { round: 1, .. }
        ));
        return;
    }
    recovery.expect("committed direct Tool exchange is recoverable");
    let view = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        replacement_controller.wait_for_terminal(&run_id),
    )
    .await
    .expect("recovered direct Tool exchange reaches a terminal promptly")
    .expect("recovered direct Tool exchange remains authoritative");
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert_eq!(first_tool.calls.load(Ordering::SeqCst), 1);
    assert_eq!(replacement_tool.calls.load(Ordering::SeqCst), 0);
    assert_eq!(replacement_model.rounds.load(Ordering::SeqCst), 2);
    assert_eq!(
        session_journal
            .load_session(&session_id)
            .await
            .expect("recovered Session Journal remains readable")
            .iter()
            .filter(|record| matches!(
                &record.payload,
                AgentSessionEvent::ToolExchangeCommitted { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("recovered Run remains registered")
            .validate()
            .expect("recovered WAL remains valid")
            .phase,
        GenericCheckpointPhase::Terminal
    );
}

#[tokio::test]
async fn terminal_private_wal_replays_from_a_new_generic_provider() {
    let run_id = RunId::new("terminal-recovery-run");
    let checkpoint_store = Arc::new(InMemoryGenericAgentCheckpointStore::default());
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let first = InternalGenericAgentProvider::new_with_session_journal(
        Arc::new(ScriptedModel),
        config.clone(),
        session_journal.clone(),
        Arc::new(JsonSizeTokenMeter::default()),
    )
    .expect("first Generic Agent starts")
    .with_checkpoint_store(checkpoint_store.clone())
    .expect("private WAL binds before the Provider is shared");
    let descriptor = first.describe();
    let request = AgentStartRequest::new(
        AgentRunEnvelope::new(
            AGENT_PROTOCOL_V1,
            AgentSessionId::new("terminal-recovery-session"),
            run_id.clone(),
            vec![Content::text("complete before the Provider restarts")],
        )
        .expect("valid text Run"),
        ProviderBindingRef::new("terminal-recovery-binding"),
        &descriptor,
    )
    .expect("valid start request");
    let started = first.start(request.clone()).await.expect("Run starts");
    let execution = started.execution.clone();
    let mut original_stream = started.stream;
    let mut original_events = Vec::new();
    loop {
        let item = tokio::time::timeout(std::time::Duration::from_secs(1), original_stream.next())
            .await
            .expect("first Provider publishes promptly")
            .expect("first Provider reaches a terminal event")
            .expect("first Provider stream remains valid");
        if let AgentProviderStreamItem::Event(draft) = item {
            let terminal = matches!(&draft.payload, AgentEvent::DeliveryCommitted { .. });
            original_events.push(*draft);
            if terminal {
                break;
            }
        }
    }
    drop(first);

    let second = InternalGenericAgentProvider::new_with_session_journal(
        Arc::new(ScriptedModel),
        config,
        session_journal,
        Arc::new(JsonSizeTokenMeter::default()),
    )
    .expect("replacement Generic Agent starts")
    .with_checkpoint_store(checkpoint_store)
    .expect("same private WAL binds to the replacement Provider");
    let recovery = second
        .recover(
            AgentRecoveryRequest::new(request, execution, &descriptor)
                .expect("recovery identity is valid"),
        )
        .await
        .expect("terminal private WAL is recoverable");
    let (mut replay, confirmation) = recovery.into_parts();
    let mut recovered_events = Vec::new();
    while let Some(item) = replay.next().await {
        if let AgentProviderStreamItem::Event(draft) = item.expect("replay remains valid") {
            recovered_events.push(*draft);
        }
    }
    confirmation
        .await
        .expect("terminal replay has no reconstructed work to start");
    assert_eq!(recovered_events, original_events);
}

#[tokio::test]
async fn open_model_attempt_is_never_restarted_from_the_private_wal() {
    let run_id = RunId::new("unsafe-model-recovery-run");
    let checkpoint_store = Arc::new(InMemoryGenericAgentCheckpointStore::default());
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let first = InternalGenericAgentProvider::new_with_session_journal(
        Arc::new(BlockingModel),
        config.clone(),
        session_journal.clone(),
        Arc::new(JsonSizeTokenMeter::default()),
    )
    .expect("first Generic Agent starts")
    .with_checkpoint_store(checkpoint_store.clone())
    .expect("private WAL binds before the Provider is shared");
    let descriptor = first.describe();
    let request = AgentStartRequest::new(
        AgentRunEnvelope::new(
            AGENT_PROTOCOL_V1,
            AgentSessionId::new("unsafe-model-recovery-session"),
            run_id.clone(),
            vec![Content::text("leave one model attempt open")],
        )
        .expect("valid text Run"),
        ProviderBindingRef::new("unsafe-model-recovery-binding"),
        &descriptor,
    )
    .expect("valid start request");
    let started = first.start(request.clone()).await.expect("Run starts");
    let execution = started.execution.clone();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let stored = checkpoint_store
                .load_run(&run_id)
                .expect("private WAL remains readable")
                .expect("private Run remains registered");
            if matches!(
                stored.validate().expect("private WAL replays").phase,
                GenericCheckpointPhase::ModelAttemptOpen { .. }
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("model attempt becomes durably open");

    let second = InternalGenericAgentProvider::new_with_session_journal(
        Arc::new(BlockingModel),
        config,
        session_journal,
        Arc::new(JsonSizeTokenMeter::default()),
    )
    .expect("replacement Generic Agent starts")
    .with_checkpoint_store(checkpoint_store.clone())
    .expect("same private WAL binds to the replacement Provider");
    let error = match second
        .recover(
            AgentRecoveryRequest::new(request, execution.clone(), &descriptor)
                .expect("recovery identity is valid"),
        )
        .await
    {
        Ok(_) => panic!("an open model attempt must not be restarted"),
        Err(error) => error,
    };
    assert_eq!(error.code, AgentProtocolErrorCode::InvalidTransition);
    assert_eq!(error.details["boundary"], "model_attempt_open");

    first
        .command(
            &execution,
            AgentCommandEnvelope::new(
                CommandId::new("cleanup-unsafe-model-recovery"),
                run_id,
                None,
                AgentCommand::Cancel {
                    reason: "test cleanup".to_owned(),
                },
            )
            .expect("cleanup command is valid"),
        )
        .await
        .expect("cleanup cancellation is accepted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_private_wal_recovers_initial_input_after_confirmation() {
    let run_id = RunId::new("prepared-recovery-run");
    let session_id = AgentSessionId::new("prepared-recovery-session");
    let checkpoint_store = Arc::new(PausingCheckpointStore::at(
        CheckpointCrashCut::InitialBoundary,
    ));
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let starts = Arc::new(AtomicUsize::new(0));
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let first = Arc::new(
        InternalGenericAgentProvider::new_with_session_journal(
            Arc::new(PreparedRecoveryModel {
                starts: starts.clone(),
            }),
            config.clone(),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("first Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let descriptor = first.describe();
    let request = AgentStartRequest::new(
        AgentRunEnvelope::new(
            AGENT_PROTOCOL_V1,
            session_id.clone(),
            run_id.clone(),
            vec![Content::text("recover the registered initial input")],
        )
        .expect("valid prepared-recovery Run"),
        ProviderBindingRef::new("prepared-recovery-binding"),
        &descriptor,
    )
    .expect("valid prepared-recovery start request");

    let paused = checkpoint_store.paused.notified();
    let first_for_start = first.clone();
    let request_for_start = request.clone();
    let start_task = tokio::spawn(async move { first_for_start.start(request_for_start).await });
    tokio::time::timeout(std::time::Duration::from_secs(1), paused)
        .await
        .expect("initial loop boundary reaches the injected crash cut");
    assert_eq!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("Run registration is durable")
            .validate()
            .expect("registered WAL is valid")
            .phase,
        GenericCheckpointPhase::Prepared
    );
    assert_eq!(starts.load(Ordering::SeqCst), 0);

    checkpoint_store.release_as_crash();
    let started = start_task
        .await
        .expect("first Provider task joins")
        .expect("admission outcome remains available after the crash cut");
    let execution = started.execution.clone();
    let mut first_stream = started.stream;
    let stream_error = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            match first_stream.next().await {
                Some(Err(error)) => break error,
                Some(Ok(_)) => {}
                None => panic!("simulated Provider loss must be explicit"),
            }
        }
    })
    .await
    .expect("simulated Provider loss reaches the stream");
    assert_eq!(
        stream_error.code,
        AgentProtocolErrorCode::ProviderUnavailable
    );
    drop(first);
    checkpoint_store.allow_recovery_writes();

    let replacement = InternalGenericAgentProvider::new_with_session_journal(
        Arc::new(PreparedRecoveryModel {
            starts: starts.clone(),
        }),
        config,
        session_journal.clone(),
        Arc::new(JsonSizeTokenMeter::default()),
    )
    .expect("replacement Generic Agent starts")
    .with_checkpoint_store(checkpoint_store.clone())
    .expect("replacement Provider binds the same private WAL");
    let recovery = replacement
        .recover(
            AgentRecoveryRequest::new(request, execution, &descriptor)
                .expect("recovery identity is valid"),
        )
        .await
        .expect("Prepared Run is safe to reconstruct");
    let (mut recovered_stream, confirmation) = recovery.into_parts();
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    confirmation
        .await
        .expect("Host confirmation opens the reconstructed execution");

    let terminal = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            match recovered_stream.next().await {
                Some(Ok(AgentProviderStreamItem::Event(event)))
                    if matches!(&event.payload, AgentEvent::DeliveryCommitted { .. }) =>
                {
                    break *event;
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => panic!("recovered stream failed: {error}"),
                None => panic!("recovered stream ended before Delivery"),
            }
        }
    })
    .await
    .expect("Prepared recovery reaches Delivery");
    assert!(matches!(
        terminal.payload,
        AgentEvent::DeliveryCommitted { .. }
    ));
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("recovered Run remains registered")
            .validate()
            .expect("recovered WAL is valid")
            .phase,
        GenericCheckpointPhase::Terminal
    );
    let input_events = session_journal
        .load_session(&session_id)
        .await
        .expect("Session Journal remains readable")
        .into_iter()
        .filter(|record| {
            record.run_id == run_id
                && matches!(&record.payload, AgentSessionEvent::RunInputCommitted { .. })
        })
        .count();
    assert_eq!(input_events, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stable_private_boundary_resumes_only_after_host_restores_continuity() {
    let run_id = RunId::new("stable-recovery-run");
    let checkpoint_store = Arc::new(PausingCheckpointStore::at(CheckpointCrashCut::ModelAttempt));
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let host_journal = Arc::new(InMemoryAgentJournalStore::default());
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let first_provider = Arc::new(
        InternalGenericAgentProvider::new_with_session_journal(
            Arc::new(BlockingModel),
            config.clone(),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("first Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let first_controller = Arc::new(
        AgentController::with_journal_store(
            first_provider,
            ProviderBindingRef::new("stable-recovery-binding"),
            host_journal.clone(),
        )
        .expect("first controller binds"),
    );
    let paused = checkpoint_store.paused.notified();
    let execution = first_controller
        .start(
            AgentRunEnvelope::new(
                AGENT_PROTOCOL_V1,
                AgentSessionId::new("stable-recovery-session"),
                run_id.clone(),
                vec![Content::text("continue from the last stable boundary")],
            )
            .expect("valid text Run"),
        )
        .await
        .expect("Run starts");
    tokio::time::timeout(std::time::Duration::from_secs(1), paused)
        .await
        .expect("first model attempt reaches the injected crash cut");
    assert!(matches!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("private Run remains registered")
            .validate()
            .expect("private WAL replays")
            .phase,
        GenericCheckpointPhase::Stable(_)
    ));

    checkpoint_store.release_as_crash();
    let first_error = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        first_controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("simulated process loss reaches the Host")
    .expect_err("lost Provider continuity is not a terminal outcome");
    assert!(matches!(
        first_error,
        AgentControlError::ContinuityUnknown(ref actual) if actual == &run_id
    ));
    drop(first_controller);
    checkpoint_store.allow_recovery_writes();

    let recovery_model = Arc::new(RecoveryAfterRestoreModel {
        host_journal: host_journal.clone(),
        run_id: run_id.clone(),
        starts: AtomicUsize::new(0),
    });
    let replacement_provider = Arc::new(
        InternalGenericAgentProvider::new_with_session_journal(
            recovery_model.clone(),
            config,
            session_journal,
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("replacement Generic Agent starts")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("same private WAL binds to the replacement Provider"),
    );
    let replacement_controller = Arc::new(
        AgentController::with_journal_store(
            replacement_provider,
            ProviderBindingRef::new("stable-recovery-binding"),
            host_journal,
        )
        .expect("replacement controller binds"),
    );
    replacement_controller
        .recover(&run_id)
        .await
        .expect("stable private boundary reconciles");
    let view = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        replacement_controller.wait_for_terminal(&run_id),
    )
    .await
    .expect("reconstructed execution completes promptly")
    .expect("reconstructed Run reaches an authoritative delivery");

    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert_eq!(
        view.delivery
            .as_ref()
            .and_then(|delivery| match &delivery.final_response.body {
                ContentBody::Inline(serde_json::Value::String(text)) => Some(text.as_str()),
                _ => None,
            }),
        Some("continued after durable recovery")
    );
    assert_eq!(recovery_model.starts.load(Ordering::SeqCst), 1);
    assert_eq!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("recovered Run remains durable")
            .validate()
            .expect("recovered WAL replays")
            .phase,
        GenericCheckpointPhase::Terminal
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_wal_recovery_is_bound_to_model_and_tool_authority() {
    let run_id = RunId::new("recovery-authority-run");
    let checkpoint_store = Arc::new(PausingCheckpointStore::at(CheckpointCrashCut::ModelAttempt));
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let base_bounds = ToolPolicyBounds {
        approval: ApprovalPolicy::NotRequired,
        max_timeout_ms: Some(1_000),
        max_output_bytes: Some(1_024),
        ..ToolPolicyBounds::default()
    };
    let base_grant = RunToolGrant {
        bounds: base_bounds.clone(),
    };
    let base_runtime = recovery_identity_tool_runtime(base_bounds.clone(), base_bounds.clone());
    let starts = Arc::new(AtomicUsize::new(0));
    let first = InternalGenericAgentProvider::new_with_tools_and_session_journal(
        Arc::new(RecoveryIdentityModel {
            revision: "v1",
            starts: starts.clone(),
        }),
        config.clone(),
        base_runtime.clone(),
        base_grant.clone(),
        session_journal.clone(),
        Arc::new(JsonSizeTokenMeter::default()),
    )
    .expect("first authority-bound Generic Agent starts")
    .with_checkpoint_store(checkpoint_store.clone())
    .expect("private WAL binds before the Provider is shared");
    let descriptor = first.describe();
    let request = AgentStartRequest::new(
        AgentRunEnvelope::new(
            AGENT_PROTOCOL_V1,
            AgentSessionId::new("recovery-authority-session"),
            run_id.clone(),
            vec![Content::text("continue only under the same authority")],
        )
        .expect("valid authority-bound Run"),
        ProviderBindingRef::new("recovery-authority-binding"),
        &descriptor,
    )
    .expect("valid authority-bound start request");

    let paused = checkpoint_store.paused.notified();
    let started = first
        .start(request.clone())
        .await
        .expect("first Run is admitted");
    let execution = started.execution.clone();
    tokio::time::timeout(std::time::Duration::from_secs(1), paused)
        .await
        .expect("first model attempt reaches the crash cut");
    checkpoint_store.release_as_crash();

    let mut stream = started.stream;
    let stream_error = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            match stream.next().await {
                Some(Err(error)) => break error,
                Some(Ok(_)) => {}
                None => panic!("simulated process loss must be explicit"),
            }
        }
    })
    .await
    .expect("simulated process loss reaches the Provider stream");
    assert_eq!(
        stream_error.code,
        AgentProtocolErrorCode::ProviderUnavailable
    );
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    assert!(matches!(
        checkpoint_store
            .load_run(&run_id)
            .expect("private WAL remains readable")
            .expect("private Run remains registered")
            .validate()
            .expect("private WAL remains valid")
            .phase,
        GenericCheckpointPhase::Stable(_)
    ));

    let changed_model = InternalGenericAgentProvider::new_with_tools_and_session_journal(
        Arc::new(RecoveryIdentityModel {
            revision: "v2",
            starts: starts.clone(),
        }),
        config.clone(),
        base_runtime.clone(),
        base_grant.clone(),
        session_journal.clone(),
        Arc::new(JsonSizeTokenMeter::default()),
    )
    .expect("replacement model contract is internally valid")
    .with_checkpoint_store(checkpoint_store.clone())
    .expect("replacement model sees the same private WAL");
    let error = match changed_model
        .recover(
            AgentRecoveryRequest::new(request.clone(), execution.clone(), &descriptor)
                .expect("recovery request identity is valid"),
        )
        .await
    {
        Ok(_) => panic!("changed model contract must not resume the Run"),
        Err(error) => error,
    };
    assert_eq!(error.code, AgentProtocolErrorCode::RunIdConflict);

    let mut narrower_grant_bounds = base_bounds.clone();
    narrower_grant_bounds.max_timeout_ms = Some(500);
    let changed_grant = InternalGenericAgentProvider::new_with_tools_and_session_journal(
        Arc::new(RecoveryIdentityModel {
            revision: "v1",
            starts: starts.clone(),
        }),
        config.clone(),
        base_runtime,
        RunToolGrant {
            bounds: narrower_grant_bounds,
        },
        session_journal.clone(),
        Arc::new(JsonSizeTokenMeter::default()),
    )
    .expect("replacement Run grant is internally valid")
    .with_checkpoint_store(checkpoint_store.clone())
    .expect("replacement grant sees the same private WAL");
    let error = match changed_grant
        .recover(
            AgentRecoveryRequest::new(request.clone(), execution.clone(), &descriptor)
                .expect("recovery request identity is valid"),
        )
        .await
    {
        Ok(_) => panic!("changed Run Tool grant must not resume the Run"),
        Err(error) => error,
    };
    assert_eq!(error.code, AgentProtocolErrorCode::RunIdConflict);

    let mut narrower_restriction = base_bounds.clone();
    narrower_restriction.max_output_bytes = Some(512);
    let changed_runtime = InternalGenericAgentProvider::new_with_tools_and_session_journal(
        Arc::new(RecoveryIdentityModel {
            revision: "v1",
            starts: starts.clone(),
        }),
        config,
        recovery_identity_tool_runtime(base_bounds, narrower_restriction),
        base_grant,
        session_journal,
        Arc::new(JsonSizeTokenMeter::default()),
    )
    .expect("replacement Tool Runtime contract is internally valid")
    .with_checkpoint_store(checkpoint_store)
    .expect("replacement Tool Runtime sees the same private WAL");
    let error = match changed_runtime
        .recover(
            AgentRecoveryRequest::new(request, execution, &descriptor)
                .expect("recovery request identity is valid"),
        )
        .await
    {
        Ok(_) => panic!("changed Host Tool contract must not resume the Run"),
        Err(error) => error,
    };
    assert_eq!(error.code, AgentProtocolErrorCode::RunIdConflict);
    assert_eq!(starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn provider_event_wal_failure_is_not_published_and_host_becomes_unknown() {
    let run_id = RunId::new("provider-wal-failure-run");
    let checkpoint_store = Arc::new(FailingProviderEventCheckpointStore::default());
    let provider = Arc::new(
        InternalGenericAgentProvider::new(
            Arc::new(ScriptedModel),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
        )
        .expect("Generic Agent accepts the neutral backend")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("failing private WAL binds before the Provider is shared"),
    );
    let controller = Arc::new(
        AgentController::new(
            provider,
            ProviderBindingRef::new("provider-wal-failure-binding"),
        )
        .expect("controller binds the Generic Agent"),
    );
    let run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        AgentSessionId::new("provider-wal-failure-session"),
        run_id.clone(),
        vec![Content::text("fail closed before publishing RunStarted")],
    )
    .expect("valid text Run");

    let execution = controller.start(run).await.expect("Run admission succeeds");
    let wait_error = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("WAL failure is observed promptly")
    .expect_err("WAL failure cannot become an authoritative terminal");
    assert!(matches!(
        wait_error,
        AgentControlError::ContinuityUnknown(ref actual) if actual == &run_id
    ));
    assert_eq!(
        controller
            .inspect(&run_id)
            .await
            .expect("Unknown Run remains inspectable")
            .state
            .status(),
        AgentRunStatus::Unknown
    );

    let host_journal = controller
        .events(&run_id, 0)
        .await
        .expect("Host journal remains readable");
    assert!(host_journal
        .iter()
        .all(|record| !matches!(&record.authority, AgentEventAuthority::Provider)));
    assert_eq!(
        checkpoint_store
            .provider_event_attempts
            .load(Ordering::SeqCst),
        1
    );
    let stored = checkpoint_store
        .load_run(&run_id)
        .expect("private WAL remains readable")
        .expect("private Run registration remains durable");
    let projection = stored.validate().expect("committed WAL prefix replays");
    assert!(projection.provider_events.is_empty());
    assert!(matches!(
        projection.phase,
        GenericCheckpointPhase::Stable(_)
    ));
}

#[tokio::test]
async fn agent_sdk_uses_the_same_controller_and_durable_run_projection() {
    let provider = Arc::new(
        InternalGenericAgentProvider::new(
            Arc::new(ScriptedModel),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
        )
        .unwrap(),
    );
    let controller =
        Arc::new(AgentController::new(provider, ProviderBindingRef::new("sdk-binding")).unwrap());
    let client = AgentClient::new(controller.clone(), AgentSessionId::new("sdk-session"));
    let turn = client.run_text("say hello through SDK").await.unwrap();

    assert_eq!(turn.status(), AgentRunStatus::Delivered);
    assert_eq!(turn.final_text(), Some("hello from the neutral model"));
    let direct_view = controller.inspect(&turn.run_id).await.unwrap();
    assert_eq!(direct_view, turn.view);
    assert_eq!(controller.events(&turn.run_id, 0).await.unwrap().len(), 4);
}

#[tokio::test]
async fn sdk_and_api_share_the_same_agent_event_semantics() {
    let provider = Arc::new(
        InternalGenericAgentProvider::new(
            Arc::new(ScriptedModel),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
        )
        .unwrap(),
    );
    let controller = Arc::new(
        AgentController::new(provider, ProviderBindingRef::new("shared-binding")).unwrap(),
    );
    let sdk = AgentClient::new(
        controller.clone(),
        AgentSessionId::new("shared-sdk-session"),
    );
    let sdk_turn = sdk.run_text("same request").await.unwrap();
    let sdk_events = controller.events(&sdk_turn.run_id, 0).await.unwrap();

    let api = AgentApi::new(controller.clone());
    let api_session = api
        .create_session(Some(AgentSessionId::new("shared-api-session")))
        .await
        .unwrap();
    let api_handle = api
        .start_text(
            &api_session,
            Some(RunId::new("shared-api-run")),
            "same request",
        )
        .await
        .unwrap();
    let api_turn = api_handle.wait_until_blocked().await.unwrap();
    let api_events = api.events(&api_turn.run_id, 0).await.unwrap();

    let event_types = |records: &[orchestral_core::agent_protocol::wire::AgentJournalRecord]| {
        records
            .iter()
            .map(|record| {
                serde_json::to_value(&record.event.payload).unwrap()["type"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(event_types(&sdk_events), event_types(&api_events));
    assert_eq!(sdk_turn.status(), api_turn.status());
    assert_eq!(sdk_turn.final_text(), api_turn.final_text());
}

fn test_unix_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("test clock is after the Unix epoch")
            .as_millis(),
    )
    .expect("test timestamp fits i64")
}

#[tokio::test]
async fn expired_deadline_commits_incomplete_without_starting_the_model() {
    let starts = Arc::new(AtomicUsize::new(0));
    let checkpoint_store = Arc::new(InMemoryGenericAgentCheckpointStore::default());
    let provider = Arc::new(
        InternalGenericAgentProvider::new(
            Arc::new(CountingBlockingModel {
                starts: starts.clone(),
            }),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
        )
        .expect("Generic Agent accepts the neutral backend")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let controller = Arc::new(
        AgentController::new(provider, ProviderBindingRef::new("deadline-binding"))
            .expect("controller binds the Generic Agent"),
    );
    let run_id = RunId::new("expired-deadline-run");
    let mut run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        AgentSessionId::new("expired-deadline-session"),
        run_id.clone(),
        vec![Content::text("must not reach the model")],
    )
    .expect("valid text Run");
    run.spec.limits.deadline_unix_ms = Some(test_unix_ms().saturating_sub(1));
    let run = AgentRunEnvelope::seal(run.spec).expect("deadline Run reseals");

    let execution = controller.start(run).await.expect("deadline Run starts");
    let view = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("expired deadline terminates promptly")
    .expect("deadline terminal remains authoritative");

    assert_eq!(view.state.status(), AgentRunStatus::Incomplete);
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    let records = controller
        .events(&run_id, 0)
        .await
        .expect("events remain readable");
    assert!(records.iter().any(|record| matches!(
        &record.event.payload,
        AgentEvent::RunIncomplete {
            reason: IncompleteReason::LimitReached {
                limit: RunLimitKind::Deadline
            },
            partial_delivery: None,
        }
    )));
    let stored = checkpoint_store
        .load_run(&run_id)
        .expect("private WAL remains readable")
        .expect("deadline Run remains durable");
    assert_eq!(
        stored.validate().expect("deadline WAL replays").phase,
        GenericCheckpointPhase::Terminal
    );
}

#[tokio::test]
async fn deadline_cancels_a_blocked_model_and_stays_incomplete() {
    let starts = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(
        InternalGenericAgentProvider::new(
            Arc::new(CountingBlockingModel {
                starts: starts.clone(),
            }),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
        )
        .expect("Generic Agent accepts the neutral backend"),
    );
    let controller = Arc::new(
        AgentController::new(provider, ProviderBindingRef::new("live-deadline-binding"))
            .expect("controller binds the Generic Agent"),
    );
    let run_id = RunId::new("live-deadline-run");
    let deadline_unix_ms = test_unix_ms().saturating_add(500);
    let mut run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        AgentSessionId::new("live-deadline-session"),
        run_id.clone(),
        vec![Content::text("wait until the deadline")],
    )
    .expect("valid text Run");
    run.spec.limits.deadline_unix_ms = Some(deadline_unix_ms);
    let run = AgentRunEnvelope::seal(run.spec).expect("deadline Run reseals");

    let execution = controller.start(run).await.expect("deadline Run starts");
    let view = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("deadline reaches the blocked model")
    .expect("deadline terminal remains authoritative");

    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(view.state.status(), AgentRunStatus::Incomplete);
    assert!(test_unix_ms().saturating_sub(deadline_unix_ms) < 1_000);
}

#[tokio::test]
async fn controller_cancel_terminates_a_generic_agent_model_run() {
    let run_id = RunId::new("cancel-run");
    let checkpoint_store = Arc::new(InMemoryGenericAgentCheckpointStore::default());
    let provider = Arc::new(
        InternalGenericAgentProvider::new(
            Arc::new(BlockingModel),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
        )
        .expect("Generic Agent accepts the neutral backend")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("private WAL binds before the Provider is shared"),
    );
    let controller = Arc::new(
        AgentController::new(provider, ProviderBindingRef::new("generic-binding"))
            .expect("controller binds the Generic Agent"),
    );
    let mut run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        AgentSessionId::new("cancel-session"),
        run_id.clone(),
        vec![Content::text("wait")],
    )
    .expect("valid text Run");
    run.spec.limits.deadline_unix_ms = Some(test_unix_ms().saturating_add(5_000));
    let run = AgentRunEnvelope::seal(run.spec).expect("cancel race Run reseals");

    let execution = controller.start(run.clone()).await.expect("Run starts");
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if controller
                .inspect(&execution.run_id)
                .await
                .expect("Run remains inspectable")
                .state
                .status()
                == AgentRunStatus::Running
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Run reaches Running before cancellation");

    let ack = controller
        .cancel(&execution.run_id, "user interrupted the conversation")
        .await
        .expect("cancel command is accepted");
    let view = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("cancellation reaches a terminal promptly")
    .expect("cancelled Run remains authoritative");

    assert_eq!(view.state.status(), AgentRunStatus::Cancelled);
    assert!(view.delivery.is_none());
    let stored = checkpoint_store
        .load_run(&run_id)
        .expect("private WAL remains readable")
        .expect("cancelled Run remains durable");
    let projection = stored.validate().expect("cancelled WAL replays");
    assert_eq!(projection.phase, GenericCheckpointPhase::Terminal);
    let command = projection
        .commands
        .get(&ack.command_id)
        .expect("cancel command is committed before cancellation is applied");
    assert!(matches!(
        &command.command.payload,
        AgentCommand::Cancel { reason } if reason == "user interrupted the conversation"
    ));
    assert_eq!(command.outcome, ProviderCommandOutcome::Accepted);

    let replacement = InternalGenericAgentProvider::new(
        Arc::new(BlockingModel),
        GenericAgentConfig::new("internal-provider", "generic-agent"),
    )
    .expect("replacement Generic Agent starts")
    .with_checkpoint_store(checkpoint_store)
    .expect("same private WAL binds to the replacement Provider");
    let descriptor = replacement.describe();
    let start_request =
        AgentStartRequest::new(run, ProviderBindingRef::new("generic-binding"), &descriptor)
            .expect("original start identity reconstructs");
    let recovery = replacement
        .recover(
            AgentRecoveryRequest::new(start_request, execution, &descriptor)
                .expect("recovery identity is valid"),
        )
        .await
        .expect("cancelled private WAL is recoverable");
    let (mut replay, confirmation) = recovery.into_parts();
    let mut replayed = Vec::new();
    while let Some(item) = replay.next().await {
        if let AgentProviderStreamItem::Event(draft) = item.expect("replay remains valid") {
            replayed.push(*draft);
        }
    }
    confirmation
        .await
        .expect("terminal replay starts no reconstructed work");
    let disposition_index = replayed
        .iter()
        .position(|event| {
            matches!(
                &event.payload,
                AgentEvent::CommandDispositionRecorded { command_id, outcome }
                    if command_id == &ack.command_id && outcome == &ProviderCommandOutcome::Accepted
            )
        })
        .expect("private command reconstructs its Provider disposition");
    let stop_index = replayed
        .iter()
        .position(|event| matches!(&event.payload, AgentEvent::StopRequested { .. }))
        .expect("cancel effect remains replayable");
    assert!(disposition_index < stop_index);
}

#[tokio::test]
async fn command_wal_failure_applies_no_command_effect_and_forces_unknown() {
    let run_id = RunId::new("command-wal-failure-run");
    let checkpoint_store = Arc::new(FailingCommandCheckpointStore::default());
    let provider = Arc::new(
        InternalGenericAgentProvider::new(
            Arc::new(BlockingModel),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
        )
        .expect("Generic Agent accepts the neutral backend")
        .with_checkpoint_store(checkpoint_store.clone())
        .expect("failing private WAL binds before the Provider is shared"),
    );
    let controller = Arc::new(
        AgentController::new(
            provider,
            ProviderBindingRef::new("command-wal-failure-binding"),
        )
        .expect("controller binds the Generic Agent"),
    );
    let run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        AgentSessionId::new("command-wal-failure-session"),
        run_id.clone(),
        vec![Content::text("wait for a cancellation whose WAL will fail")],
    )
    .expect("valid text Run");

    let execution = controller.start(run).await.expect("Run starts");
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if controller
                .inspect(&run_id)
                .await
                .expect("Run remains inspectable")
                .state
                .status()
                == AgentRunStatus::Running
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Run reaches Running before the injected failure");

    assert!(controller
        .cancel(&run_id, "this command must not be applied")
        .await
        .is_err());
    let wait_error = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("command WAL failure is observed promptly")
    .expect_err("an uncommitted command cannot produce a terminal cancellation");
    assert!(matches!(
        wait_error,
        AgentControlError::ContinuityUnknown(ref actual) if actual == &run_id
    ));

    let host_journal = controller
        .events(&run_id, 0)
        .await
        .expect("Host journal remains readable");
    assert!(host_journal.iter().all(|record| !matches!(
        &record.event.payload,
        AgentEvent::CommandDispositionRecorded { .. }
            | AgentEvent::StopRequested { .. }
            | AgentEvent::RunCancelled { .. }
    )));
    assert_eq!(checkpoint_store.command_attempts.load(Ordering::SeqCst), 1);
    let stored = checkpoint_store
        .load_run(&run_id)
        .expect("private WAL remains readable")
        .expect("private Run registration remains durable");
    let projection = stored.validate().expect("committed WAL prefix replays");
    assert!(projection.commands.is_empty());
    assert!(matches!(
        projection.phase,
        GenericCheckpointPhase::ModelAttemptOpen { .. }
    ));
}

#[tokio::test]
async fn one_hundred_steers_are_committed_in_order_without_crossing_the_run() {
    let run_id = RunId::new("steer-run");
    let checkpoint_store = Arc::new(InMemoryGenericAgentCheckpointStore::default());
    let model = Arc::new(SteerAccumulatingModel {
        rounds: AtomicUsize::new(0),
        first_started: Notify::new(),
    });
    let mut config = GenericAgentConfig::new("internal-provider", "generic-agent");
    config.max_model_rounds = 128;
    config.stream_buffer = 128;
    let provider = Arc::new(
        InternalGenericAgentProvider::new(model.clone(), config)
            .expect("steer-capable Generic Agent starts")
            .with_checkpoint_store(checkpoint_store.clone())
            .expect("private WAL binds before the Provider is shared"),
    );
    assert!(provider.describe().descriptor.capabilities.controls.steer);
    let controller = Arc::new(
        AgentController::new(provider, ProviderBindingRef::new("generic-binding"))
            .expect("controller binds the Generic Agent"),
    );
    let client = AgentClient::new(controller.clone(), AgentSessionId::new("steer-session"));
    let handle = client
        .start_with_run_id(run_id.clone(), vec![Content::text("initial input")])
        .await
        .expect("Run starts");
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        model.first_started.notified(),
    )
    .await
    .expect("first model request starts");

    for index in 0..100 {
        let ack = handle
            .steer_text(format!("steer-{index:03}"))
            .await
            .expect("steer command is accepted");
        assert!(matches!(
            ack.state,
            CommandAckState::Accepted { .. } | CommandAckState::Applied { .. }
        ));
    }

    let view = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        controller.wait_for_terminal(handle.run_id()),
    )
    .await
    .expect("steered Run reaches one terminal")
    .expect("steered Run remains inspectable");
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert_eq!(
        view.delivery
            .as_ref()
            .and_then(|delivery| match &delivery.final_response.body {
                ContentBody::Inline(serde_json::Value::String(text)) => Some(text.as_str()),
                _ => None,
            }),
        Some("all steering inputs applied")
    );

    let committed = handle
        .events(0)
        .await
        .expect("steer events remain replayable")
        .into_iter()
        .filter_map(|record| match record.event.payload {
            AgentEvent::InputCommitted { content } => {
                content
                    .into_iter()
                    .next()
                    .and_then(|content| match content.body {
                        ContentBody::Inline(serde_json::Value::String(text)) => Some(text),
                        _ => None,
                    })
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(committed.len(), 100);
    assert_eq!(
        committed,
        (0..100)
            .map(|index| format!("steer-{index:03}"))
            .collect::<Vec<_>>()
    );

    let stored = checkpoint_store
        .load_run(&run_id)
        .expect("private WAL remains readable")
        .expect("steered Run remains durable");
    let projection = stored.validate().expect("steered WAL replays");
    assert_eq!(projection.phase, GenericCheckpointPhase::Terminal);
    assert_eq!(projection.commands.len(), 100);
    assert!(projection
        .commands
        .values()
        .all(|checkpoint| checkpoint.outcome == ProviderCommandOutcome::Accepted));
    let checkpointed_steers = stored
        .records
        .iter()
        .filter_map(|record| match &record.payload {
            GenericCheckpointEvent::CommandCommitted { command, .. } => match &command.payload {
                AgentCommand::Steer { content } => content.first().and_then(|content| {
                    if let ContentBody::Inline(serde_json::Value::String(text)) = &content.body {
                        Some(text.clone())
                    } else {
                        None
                    }
                }),
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(checkpointed_steers, committed);
}

#[tokio::test]
async fn model_input_request_resolves_by_request_id_and_resumes_the_same_run() {
    let model = Arc::new(InputRequestModel {
        rounds: AtomicUsize::new(0),
    });
    let provider = Arc::new(
        InternalGenericAgentProvider::new(
            model.clone(),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
        )
        .expect("input-capable Generic Agent starts"),
    );
    assert!(provider
        .describe()
        .descriptor
        .capabilities
        .pending_request_kinds
        .contains(&PendingRequestKind::Input));
    let controller = Arc::new(
        AgentController::new(provider, ProviderBindingRef::new("generic-binding"))
            .expect("controller binds the Generic Agent"),
    );
    let client = AgentClient::new(controller.clone(), AgentSessionId::new("input-session"));
    let handle = client
        .start_with_run_id(
            RunId::new("input-run"),
            vec![Content::text("prepare a city report")],
        )
        .await
        .expect("Run starts");
    let blocked = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        handle.wait_until_blocked(),
    )
    .await
    .expect("Run opens an input request")
    .expect("blocked Run remains inspectable");
    assert!(blocked.is_waiting());
    assert_eq!(blocked.view.pending_requests.len(), 1);
    let request = &blocked.view.pending_requests[0];
    assert_eq!(request.kind(), PendingRequestKind::Input);
    let ack = handle
        .resolve_input_text(request.request_id.clone(), "Shanghai")
        .await
        .expect("correlated input resolution is accepted");
    assert!(matches!(
        ack.state,
        CommandAckState::Accepted { .. } | CommandAckState::Applied { .. }
    ));

    let view = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        controller.wait_for_terminal(handle.run_id()),
    )
    .await
    .expect("resolved Run reaches terminal")
    .expect("resolved Run remains inspectable");
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert_eq!(model.rounds.load(Ordering::SeqCst), 2);
    assert!(view.pending_requests.is_empty());
    assert!(handle
        .events(0)
        .await
        .unwrap()
        .iter()
        .any(|record| matches!(record.event.payload, AgentEvent::RequestResolved { .. })));
}

#[tokio::test]
async fn generic_agent_executes_model_tools_only_through_the_guarded_runtime() {
    let run_id = RunId::new("tool-run");
    let checkpoint_store = Arc::new(InMemoryGenericAgentCheckpointStore::default());
    let bounds = ToolPolicyBounds {
        approval: ApprovalPolicy::NotRequired,
        max_timeout_ms: Some(1_000),
        max_output_bytes: Some(1_024),
        ..ToolPolicyBounds::default()
    };
    let verifier = HostApprovalVerifier::new(
        b"0123456789abcdef0123456789abcdef",
        InMemoryApprovalCapabilityStore::default(),
    )
    .expect("valid Host signing key");
    let runtime = Arc::new(
        GuardedToolRuntime::new(
            HostToolPolicy {
                bounds: bounds.clone(),
            },
            verifier,
        )
        .expect("valid Host policy"),
    );
    let tool = Arc::new(WalInspectingEchoTool {
        calls: AtomicUsize::new(0),
        checkpoint_store: checkpoint_store.clone(),
        run_id: run_id.clone(),
        observed_before_execute: AtomicUsize::new(0),
    });
    runtime
        .register(
            ToolDescriptor {
                tool_id: ToolId::new("test/echo"),
                model_schema: ModelToolSchema {
                    name: "echo".to_owned(),
                    description: "Echo one string".to_owned(),
                    input_schema: json!({
                        "type": "object",
                        "required": ["value"],
                        "properties": { "value": { "type": "string" } },
                        "additionalProperties": false
                    }),
                },
                output_schema: json!({
                    "type": "object",
                    "required": ["result"],
                    "properties": { "result": { "type": "string" } },
                    "additionalProperties": false
                }),
                effect_scopes: BTreeSet::new(),
                restriction: ToolRestriction {
                    bounds: bounds.clone(),
                },
                idempotency: ToolIdempotency::IdempotentWithKey,
                concurrency: ToolConcurrency::ParallelSafe,
            },
            tool.clone(),
        )
        .expect("Tool registers");
    let model = Arc::new(ToolLoopModel {
        rounds: AtomicUsize::new(0),
    });
    let provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools(
            model.clone(),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
            runtime,
            RunToolGrant { bounds },
        )
        .expect("tool-capable Generic Agent is valid")
        .with_checkpoint_store(checkpoint_store)
        .expect("private WAL binds before the Provider is shared"),
    );
    let controller = Arc::new(
        AgentController::new(provider, ProviderBindingRef::new("generic-binding"))
            .expect("controller binds the Generic Agent"),
    );
    let run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        AgentSessionId::new("tool-session"),
        run_id,
        vec![Content::text("use echo")],
    )
    .expect("valid text Run");

    let execution = controller.start(run).await.expect("Run starts");
    let view = controller
        .wait_for_terminal(&execution.run_id)
        .await
        .expect("Tool loop reaches a terminal delivery");

    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    assert_eq!(tool.observed_before_execute.load(Ordering::SeqCst), 1);
    assert_eq!(model.rounds.load(Ordering::SeqCst), 2);
    let delivery = view.delivery.expect("Delivered Run exposes its delivery");
    assert_eq!(delivery.usage.and_then(|usage| usage.tool_calls), Some(1));
    assert!(matches!(
        delivery.final_response.body,
        ContentBody::Inline(serde_json::Value::String(ref text))
            if text == "tool said hello"
    ));
}

#[tokio::test]
async fn every_model_round_reprojects_context_before_backend_dispatch() {
    const MAX_CONTEXT_TOKENS: u64 = 3_000;
    const RESERVED_OUTPUT_TOKENS: u64 = 500;
    const INPUT_BUDGET_TOKENS: u64 = MAX_CONTEXT_TOKENS - RESERVED_OUTPUT_TOKENS;

    let bounds = ToolPolicyBounds {
        approval: ApprovalPolicy::NotRequired,
        max_timeout_ms: Some(1_000),
        max_output_bytes: Some(10_000),
        ..ToolPolicyBounds::default()
    };
    let verifier = HostApprovalVerifier::new(
        b"0123456789abcdef0123456789abcdef",
        InMemoryApprovalCapabilityStore::default(),
    )
    .unwrap();
    let runtime = Arc::new(
        GuardedToolRuntime::new(
            HostToolPolicy {
                bounds: bounds.clone(),
            },
            verifier,
        )
        .unwrap(),
    );
    runtime
        .register(
            ToolDescriptor {
                tool_id: ToolId::new("test/budget-echo"),
                model_schema: ModelToolSchema {
                    name: "echo".to_owned(),
                    description: "Return a deliberately large result".to_owned(),
                    input_schema: json!({
                        "type": "object",
                        "required": ["value"],
                        "properties": { "value": { "type": "string" } },
                        "additionalProperties": false
                    }),
                },
                output_schema: json!({
                    "type": "object",
                    "required": ["result"],
                    "properties": { "result": { "type": "string" } },
                    "additionalProperties": false
                }),
                effect_scopes: BTreeSet::new(),
                restriction: ToolRestriction {
                    bounds: bounds.clone(),
                },
                idempotency: ToolIdempotency::IdempotentWithKey,
                concurrency: ToolConcurrency::ParallelSafe,
            },
            Arc::new(LargeResultTool {
                value: "large-inline-result/".repeat(300),
            }),
        )
        .unwrap();
    let model = Arc::new(BudgetGuardToolLoopModel {
        rounds: AtomicUsize::new(0),
        oversized_dispatches: AtomicUsize::new(0),
        input_budget: INPUT_BUDGET_TOKENS,
    });
    let mut config = GenericAgentConfig::new("internal-provider", "generic-agent");
    config.max_context_tokens = MAX_CONTEXT_TOKENS;
    config.reserved_output_tokens = RESERVED_OUTPUT_TOKENS;
    config.max_model_rounds = 3;
    let provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools_and_session_journal(
            model.clone(),
            config,
            runtime,
            RunToolGrant { bounds },
            Arc::new(InMemoryAgentSessionJournalStore::default()),
            Arc::new(JsonSizeTokenMeter::new(1).unwrap()),
        )
        .unwrap(),
    );
    let controller = Arc::new(
        AgentController::new(provider, ProviderBindingRef::new("generic-binding")).unwrap(),
    );
    let run_id = RunId::new("context-budget-run");
    let execution = controller
        .start(
            AgentRunEnvelope::new(
                AGENT_PROTOCOL_V1,
                AgentSessionId::new("context-budget-session"),
                run_id.clone(),
                vec![Content::text("call the large echo tool")],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let view = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("context overflow reaches a terminal boundary")
    .unwrap();

    assert_eq!(view.state.status(), AgentRunStatus::Failed);
    assert_eq!(model.rounds.load(Ordering::SeqCst), 1);
    assert_eq!(model.oversized_dispatches.load(Ordering::SeqCst), 0);
    assert!(controller
        .events(&run_id, 0)
        .await
        .unwrap()
        .iter()
        .any(|record| matches!(
            &record.event.payload,
            AgentEvent::RunFailed { failure } if failure.code == "context_overflow"
        )));
}

#[tokio::test]
async fn generic_agent_journals_only_artifact_reference_and_summary_for_large_tool_result() {
    let large_value = "large-result-marker/".repeat(128);
    let bounds = ToolPolicyBounds {
        approval: ApprovalPolicy::NotRequired,
        max_timeout_ms: Some(1_000),
        max_output_bytes: Some(64),
        ..ToolPolicyBounds::default()
    };
    let verifier = HostApprovalVerifier::new(
        b"0123456789abcdef0123456789abcdef",
        InMemoryApprovalCapabilityStore::default(),
    )
    .unwrap();
    let artifacts =
        ToolArtifactStore::new(Arc::new(InMemoryBlobStore::default()), 16 * 1024, 80).unwrap();
    let runtime = Arc::new(
        GuardedToolRuntime::new_with_effect_journal_and_artifacts(
            HostToolPolicy {
                bounds: bounds.clone(),
            },
            verifier,
            Arc::new(InMemoryToolEffectJournalStore::default()),
            artifacts,
        )
        .unwrap(),
    );
    runtime
        .register(
            ToolDescriptor {
                tool_id: ToolId::new("test/echo"),
                model_schema: ModelToolSchema {
                    name: "echo".to_owned(),
                    description: "Echo one string".to_owned(),
                    input_schema: json!({
                        "type": "object",
                        "required": ["value"],
                        "properties": { "value": { "type": "string" } },
                        "additionalProperties": false
                    }),
                },
                output_schema: json!({
                    "type": "object",
                    "required": ["result"],
                    "properties": { "result": { "type": "string" } },
                    "additionalProperties": false
                }),
                effect_scopes: BTreeSet::new(),
                restriction: ToolRestriction {
                    bounds: bounds.clone(),
                },
                idempotency: ToolIdempotency::IdempotentWithKey,
                concurrency: ToolConcurrency::ParallelSafe,
            },
            Arc::new(LargeResultTool {
                value: large_value.clone(),
            }),
        )
        .unwrap();
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let session_id = AgentSessionId::new("artifact-session");
    let model = Arc::new(ArtifactLoopModel {
        rounds: AtomicUsize::new(0),
        large_value: large_value.clone(),
    });
    let provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools_and_session_journal(
            model,
            GenericAgentConfig::new("internal-provider", "generic-agent"),
            runtime,
            RunToolGrant { bounds },
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .unwrap(),
    );
    let controller = Arc::new(
        AgentController::new(provider, ProviderBindingRef::new("artifact-binding")).unwrap(),
    );
    let run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        session_id.clone(),
        RunId::new("artifact-run"),
        vec![Content::text("produce a large result")],
    )
    .unwrap();
    let execution = controller.start(run).await.unwrap();
    let view = controller
        .wait_for_terminal(&execution.run_id)
        .await
        .unwrap();
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);

    let records = session_journal.load_session(&session_id).await.unwrap();
    let encoded = serde_json::to_string(&records).unwrap();
    assert!(!encoded.contains(&large_value));
    assert!(records.iter().any(|record| {
        matches!(
            &record.payload,
            AgentSessionEvent::ToolExchangeCommitted { tool, .. }
                if tool.content.iter().any(|content| matches!(
                    content,
                    ModelContent::ToolResult { result, is_error: false, .. }
                        if result["kind"] == json!("artifact")
                            && result["artifact"]["artifact_ref"].as_str().is_some()
                            && result["summary"].as_str().is_some()
                ))
        )
    }));
}

#[tokio::test]
async fn generic_agent_resumes_the_exact_tool_call_after_host_approval() {
    run_approval_case(true).await;
}

#[tokio::test]
async fn generic_agent_returns_a_denial_observation_without_executing_the_tool() {
    run_approval_case(false).await;
}

async fn run_approval_case(allow: bool) {
    const SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
    let bounds = ToolPolicyBounds {
        allowed_effects: BTreeSet::from([EffectScope::Process]),
        approval: ApprovalPolicy::Required,
        max_timeout_ms: Some(1_000),
        max_output_bytes: Some(1_024),
        ..ToolPolicyBounds::default()
    };
    let verifier =
        HostApprovalVerifier::new(SIGNING_KEY, InMemoryApprovalCapabilityStore::default())
            .expect("valid Host signing key");
    let runtime = Arc::new(
        GuardedToolRuntime::new(
            HostToolPolicy {
                bounds: bounds.clone(),
            },
            verifier,
        )
        .expect("valid Host policy"),
    );
    let tool = Arc::new(EchoTool {
        calls: AtomicUsize::new(0),
    });
    runtime
        .register(
            ToolDescriptor {
                tool_id: ToolId::new("test/approval-echo"),
                model_schema: ModelToolSchema {
                    name: "echo".to_owned(),
                    description: "Echo one string after Host approval".to_owned(),
                    input_schema: json!({
                        "type": "object",
                        "required": ["value"],
                        "properties": { "value": { "type": "string" } },
                        "additionalProperties": false
                    }),
                },
                output_schema: json!({
                    "type": "object",
                    "required": ["result"],
                    "properties": { "result": { "type": "string" } },
                    "additionalProperties": false
                }),
                effect_scopes: BTreeSet::from([EffectScope::Process]),
                restriction: ToolRestriction {
                    bounds: bounds.clone(),
                },
                idempotency: ToolIdempotency::IdempotentWithKey,
                concurrency: ToolConcurrency::ParallelSafe,
            },
            tool.clone(),
        )
        .expect("approval Tool registers");
    let broker = Arc::new(
        InMemoryHostApprovalBroker::new(SIGNING_KEY).expect("Host approval broker is valid"),
    );
    let model = Arc::new(ApprovalLoopModel {
        rounds: AtomicUsize::new(0),
        expect_allowed: allow,
    });
    let provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools_approval_and_session_journal(
            model.clone(),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
            runtime,
            RunToolGrant { bounds },
            broker.clone(),
            Arc::new(InMemoryAgentSessionJournalStore::default()),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("approval-capable Generic Agent is valid"),
    );
    assert!(provider
        .describe()
        .descriptor
        .capabilities
        .pending_request_kinds
        .contains(&PendingRequestKind::Approval));
    let controller = Arc::new(
        AgentController::new(provider, ProviderBindingRef::new("generic-binding"))
            .expect("controller binds the Generic Agent"),
    );
    let suffix = if allow { "allow" } else { "deny" };
    let run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        AgentSessionId::new(format!("approval-{suffix}-session")),
        RunId::new(format!("approval-{suffix}-run")),
        vec![Content::text("use the approval Tool")],
    )
    .expect("valid approval Run");

    let execution = controller.start(run).await.expect("Run starts");
    let pending = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let view = controller
                .inspect(&execution.run_id)
                .await
                .expect("approval Run remains inspectable");
            if let Some(request) = view.pending_requests.into_iter().next() {
                break request;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Tool opens an approval request");
    assert_eq!(pending.kind(), PendingRequestKind::Approval);

    let resolution = if allow {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("wall clock follows Unix epoch")
            .as_millis() as i64;
        let grant_ref = broker
            .approve(&pending.request_id, now_ms + 60_000)
            .expect("Host issues an exact approval grant");
        RequestResolution::Approval {
            decision: ApprovalDecision::Allow,
            grant_ref: Some(grant_ref),
        }
    } else {
        RequestResolution::Approval {
            decision: ApprovalDecision::Deny,
            grant_ref: None,
        }
    };
    let command_id = CommandId::new(format!("approval-{suffix}-command"));
    let command = AgentCommandEnvelope::new(
        command_id.clone(),
        execution.run_id.clone(),
        Some(pending.request_id.clone()),
        AgentCommand::ResolveRequest {
            response: resolution,
        },
    )
    .expect("valid approval resolution command");
    let initial_ack = controller
        .command(command)
        .await
        .expect("Host resolution is accepted");
    assert!(matches!(
        initial_ack.state,
        CommandAckState::Accepted { .. } | CommandAckState::Applied { .. }
    ));

    let view = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .expect("approval resolution resumes the Run")
    .expect("approval Run reaches an authoritative terminal state");
    let final_ack = controller
        .command_ack(&execution.run_id, &command_id)
        .await
        .expect("resolution command remains inspectable");
    let journal = controller
        .events(&execution.run_id, 0)
        .await
        .expect("approval Run journal remains readable");

    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert!(matches!(final_ack.state, CommandAckState::Applied { .. }));
    assert_eq!(tool.calls.load(Ordering::SeqCst), usize::from(allow));
    assert_eq!(model.rounds.load(Ordering::SeqCst), 2);
    assert!(journal
        .iter()
        .any(|record| matches!(record.event.payload, AgentEvent::RequestOpened { .. })));
    assert!(journal
        .iter()
        .any(|record| matches!(record.event.payload, AgentEvent::RequestResolved { .. })));
}

#[tokio::test]
async fn generic_agent_projects_workflow_progress_and_result_into_the_same_agent_run() {
    let bounds = ToolPolicyBounds {
        approval: ApprovalPolicy::NotRequired,
        max_timeout_ms: Some(5_000),
        max_output_bytes: Some(16 * 1024),
        ..ToolPolicyBounds::default()
    };
    let verifier = HostApprovalVerifier::new(
        b"0123456789abcdef0123456789abcdef",
        InMemoryApprovalCapabilityStore::default(),
    )
    .expect("valid Host signing key");
    let runtime = Arc::new(
        GuardedToolRuntime::new(
            HostToolPolicy {
                bounds: bounds.clone(),
            },
            verifier,
        )
        .expect("valid Host policy"),
    );
    let echo = Arc::new(GatedWorkflowEcho {
        calls: AtomicUsize::new(0),
        first_started: Notify::new(),
        release_first: Notify::new(),
    });
    runtime
        .register(
            ToolDescriptor {
                tool_id: ToolId::new("test/echo"),
                model_schema: ModelToolSchema {
                    name: "echo".to_owned(),
                    description: "Echo one string".to_owned(),
                    input_schema: json!({
                        "type": "object",
                        "required": ["value"],
                        "properties": { "value": { "type": "string" } },
                        "additionalProperties": false
                    }),
                },
                output_schema: json!({
                    "type": "object",
                    "required": ["result"],
                    "properties": { "result": { "type": "string" } },
                    "additionalProperties": false
                }),
                effect_scopes: BTreeSet::new(),
                restriction: ToolRestriction {
                    bounds: bounds.clone(),
                },
                idempotency: ToolIdempotency::IdempotentWithKey,
                concurrency: ToolConcurrency::ParallelSafe,
            },
            echo.clone(),
        )
        .expect("Tool registers");
    let mut normalizer = PlanNormalizer::new();
    normalizer.register_action("echo");
    let workflow = Arc::new(WorkflowExecutionStrategy::new(
        Arc::new(normalizer),
        Arc::new(Executor::new()),
        runtime.clone(),
    ));
    let model = Arc::new(WorkflowLoopModel {
        rounds: AtomicUsize::new(0),
    });
    let provider = Arc::new(
        InternalGenericAgentProvider::new_with_workflow_and_session_journal(
            model.clone(),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
            runtime,
            RunToolGrant {
                bounds: bounds.clone(),
            },
            workflow,
            Arc::new(InMemoryAgentSessionJournalStore::default()),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("workflow-capable Generic Agent is valid"),
    );
    let controller = Arc::new(
        AgentController::new(provider, ProviderBindingRef::new("generic-binding"))
            .expect("controller binds the Generic Agent"),
    );
    let run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        AgentSessionId::new("workflow-session"),
        RunId::new("workflow-run"),
        vec![Content::text("run the ordered workflow")],
    )
    .expect("valid workflow Run");

    let execution = controller.start(run).await.expect("Run starts");
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        echo.first_started.notified(),
    )
    .await
    .expect("first workflow Step starts");
    let mut live = controller
        .subscribe(&execution.run_id)
        .await
        .expect("Run supports live progress");
    echo.release_first.notify_one();
    let view = controller
        .wait_for_terminal(&execution.run_id)
        .await
        .expect("workflow Run delivers");
    let journal = controller
        .events(&execution.run_id, 0)
        .await
        .expect("workflow Run journal is readable");

    let mut saw_workflow_progress = false;
    while let Ok(event) = live.try_recv() {
        if matches!(
            event,
            AgentControlEvent::Telemetry(ref telemetry)
                if matches!(
                    telemetry.payload,
                    AgentTelemetry::ProgressReported {
                        fraction: Some(fraction),
                        ..
                    } if (fraction - 1.0).abs() < f64::EPSILON
                )
        ) {
            saw_workflow_progress = true;
        }
    }
    let workflow_record = journal
        .iter()
        .find(|record| {
            matches!(
                &record.event.payload,
                AgentEvent::OutputCommitted { content, .. }
                    if content.iter().any(|content| {
                        content.media_type == "application/json"
                            && matches!(
                                &content.body,
                                ContentBody::Inline(value)
                                    if value["status"] == json!("completed")
                                        && value["tool_calls"] == json!(2)
                            )
                    })
            )
        })
        .expect("workflow result is a durable supporting output");
    let delivery = view.delivery.expect("Run has one Agent delivery");

    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert!(saw_workflow_progress);
    assert_eq!(echo.calls.load(Ordering::SeqCst), 2);
    assert_eq!(model.rounds.load(Ordering::SeqCst), 2);
    assert_eq!(delivery.usage.and_then(|usage| usage.tool_calls), Some(3));
    assert!(delivery
        .provenance
        .supporting_event_ids
        .contains(&workflow_record.event.event_id));
    assert!(matches!(
        delivery.final_response.body,
        ContentBody::Inline(serde_json::Value::String(ref text))
            if text == "workflow complete"
    ));
}

#[tokio::test]
async fn a_new_generic_provider_rebuilds_session_context_from_the_session_journal() {
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let session_id = AgentSessionId::new("restart-session");
    let first_provider = Arc::new(
        InternalGenericAgentProvider::new_with_session_journal(
            Arc::new(RestartSessionModel {
                response: "first answer",
                expect_prior_turn: false,
            }),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("first provider starts"),
    );
    let first_controller = Arc::new(
        AgentController::new(first_provider, ProviderBindingRef::new("generic-binding"))
            .expect("first controller binds"),
    );
    let first_run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        session_id.clone(),
        RunId::new("restart-run-1"),
        vec![Content::text("first question")],
    )
    .unwrap();
    let first_execution = first_controller.start(first_run).await.unwrap();
    first_controller
        .wait_for_terminal(&first_execution.run_id)
        .await
        .unwrap();
    drop(first_controller);

    let second_provider = Arc::new(
        InternalGenericAgentProvider::new_with_session_journal(
            Arc::new(RestartSessionModel {
                response: "second answer",
                expect_prior_turn: true,
            }),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
            session_journal,
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .expect("restarted provider starts"),
    );
    let second_controller = Arc::new(
        AgentController::new(second_provider, ProviderBindingRef::new("generic-binding"))
            .expect("second controller binds"),
    );
    let second_run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        session_id,
        RunId::new("restart-run-2"),
        vec![Content::text("second question")],
    )
    .unwrap();
    let second_execution = second_controller.start(second_run).await.unwrap();
    let second = second_controller
        .wait_for_terminal(&second_execution.run_id)
        .await
        .unwrap();

    assert_eq!(second.state.status(), AgentRunStatus::Delivered);
}

#[tokio::test]
async fn bound_session_compaction_runs_before_a_real_model_round_and_is_durable() {
    let session_journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let summarizer_calls = Arc::new(AtomicUsize::new(0));
    let model = Arc::new(CompactionAwareModel {
        requests: AtomicUsize::new(0),
    });
    let policy = SessionCompactionPolicy {
        minimum_source_records: 2,
        keep_recent_records: 1,
    };
    let expected_policy_digest = policy.digest().unwrap();
    let provider = Arc::new(
        InternalGenericAgentProvider::new_with_session_journal(
            model.clone(),
            GenericAgentConfig::new("internal-provider", "generic-agent"),
            session_journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .unwrap()
        .with_session_compaction(
            Arc::new(RecordingSessionSummarizer {
                calls: summarizer_calls.clone(),
            }),
            policy,
        )
        .expect("Session compaction binds before the Provider is shared"),
    );
    let controller = Arc::new(
        AgentController::new(provider, ProviderBindingRef::new("compaction-binding"))
            .expect("controller binds"),
    );
    let session_id = AgentSessionId::new("compaction-session");

    for (run_id, input) in [
        ("compaction-run-1", "raw first question"),
        ("compaction-run-2", "second question"),
    ] {
        let run = AgentRunEnvelope::new(
            AGENT_PROTOCOL_V1,
            session_id.clone(),
            RunId::new(run_id),
            vec![Content::text(input)],
        )
        .unwrap();
        let execution = controller.start(run).await.unwrap();
        let terminal = controller
            .wait_for_terminal(&execution.run_id)
            .await
            .unwrap();
        assert_eq!(terminal.state.status(), AgentRunStatus::Delivered);
    }

    let records = session_journal.load_session(&session_id).await.unwrap();
    let compaction = records
        .iter()
        .find_map(|record| match &record.payload {
            AgentSessionEvent::CompactionCommitted {
                source,
                policy_digest,
                strategy,
                version,
                ..
            } => Some((source, policy_digest, strategy, version)),
            _ => None,
        })
        .expect("model-round compaction is a durable Session fact");
    assert_eq!(compaction.0.first_session_seq, 1);
    assert_eq!(compaction.0.last_session_seq, 2);
    assert_eq!(*compaction.1, expected_policy_digest);
    assert_eq!(compaction.2, "recording-integration-summary");
    assert_eq!(compaction.3, "1");
    assert_eq!(summarizer_calls.load(Ordering::SeqCst), 1);
    assert_eq!(model.requests.load(Ordering::SeqCst), 2);
}
