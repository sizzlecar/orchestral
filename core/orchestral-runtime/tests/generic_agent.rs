use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{stream, StreamExt};
use orchestral_core::{
    agent_protocol::{
        reference::AgentRunStatus,
        spi::{AgentJournalStore, AgentProvider, AgentRecoveryRequest, InMemoryAgentJournalStore},
        wire::{
            AgentCommand, AgentCommandEnvelope, AgentEvent, AgentEventAuthority,
            AgentProtocolErrorCode, AgentProviderStreamItem, AgentRunEnvelope, AgentSessionId,
            AgentStartRequest, AgentTelemetry, ApprovalDecision, CommandAckState, CommandId,
            Content, ContentBody, PendingRequestKind, PendingRequestPayload, ProviderBindingRef,
            ProviderCommandOutcome, RequestResolution, RunId,
        },
        AGENT_PROTOCOL_V1,
    },
    agent_session::{
        AgentSessionEvent, AgentSessionJournalStore, InMemoryAgentSessionJournalStore,
    },
    executor::Executor,
    model_protocol::{
        ModelBackend, ModelCapabilities, ModelContent, ModelDescriptor, ModelError, ModelEvent,
        ModelEventId, ModelFinishReason, ModelRequest, ModelRole, ModelStream, ModelStreamEvent,
        ModelToolCallId,
    },
    normalizer::PlanNormalizer,
    tool_effect::InMemoryToolEffectJournalStore,
    tool_protocol::{
        ApprovalPolicy, EffectScope, HostApprovalVerifier, HostToolPolicy,
        InMemoryApprovalCapabilityStore, ModelToolSchema, RunToolGrant, ToolConcurrency,
        ToolDescriptor, ToolId, ToolIdempotency, ToolOutcome, ToolPolicyBounds, ToolRestriction,
    },
};
use orchestral_runtime::{
    api::AgentApi, AgentClient, AgentControlError, AgentControlEvent, AgentController,
    AppendGenericCheckpointOutcome, CreateGenericRunOutcome, GenericAgentCheckpointStore,
    GenericAgentConfig, GenericAgentRunRegistration, GenericCheckpointDraft,
    GenericCheckpointError, GenericCheckpointEvent, GenericCheckpointPhase, GuardedToolExecution,
    GuardedToolExecutor, GuardedToolRuntime, InMemoryBlobStore,
    InMemoryGenericAgentCheckpointStore, InMemoryHostApprovalBroker, InternalGenericAgentProvider,
    JsonSizeTokenMeter, StoredGenericAgentRun, ToolArtifactStore, WorkflowExecutionStrategy,
};
use serde_json::json;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

struct ScriptedModel;

struct BlockingModel;

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
                    CheckpointCrashCut::InputToolExchangeBoundary,
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

struct ArtifactLoopModel {
    rounds: AtomicUsize,
    large_value: String,
}

struct ApprovalLoopModel {
    rounds: AtomicUsize,
    expect_allowed: bool,
}

struct EchoTool {
    calls: AtomicUsize,
}

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
    let run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        AgentSessionId::new("cancel-session"),
        run_id.clone(),
        vec![Content::text("wait")],
    )
    .expect("valid text Run");

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
