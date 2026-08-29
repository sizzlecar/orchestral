//! Finite-depth refinement checks for the public Agent Protocol reducer.
//!
//! This is deliberately not a claim of unbounded model checking. The oracle
//! below is a small, literal state machine written independently of the SUT.
//! It enumerates two request identities, two command identities, all four
//! terminal outcomes, and the continuity axis through a fixed committed depth.

use std::collections::{BTreeMap, BTreeSet};

use orchestral_core::agent_protocol::{
    reference::{
        AgentContinuityState, AgentRunReducer, AgentRunStatus, ApplyOutcome,
        ReconciliationProofVerifier,
    },
    wire::{
        AgentAdmission, AgentCapabilities, AgentCommand, AgentCommandEnvelope, AgentDelivery,
        AgentDescriptor, AgentDescriptorEnvelope, AgentEvent, AgentEventDraft, AgentEventId,
        AgentExecutionRef, AgentFailure, AgentId, AgentProtocolError, AgentProtocolErrorCode,
        AgentProviderId, AgentRunEnvelope, AgentRunState, AgentSessionId, AgentStartRequest,
        AgentTerminalState, CancelSupport, CommandAckState, CommandId, Content,
        ControlCapabilities, DeliveryId, Digest, EffectMediation, IncompleteReason, PendingRequest,
        PendingRequestKind, PendingRequestPayload, Provenance, ProviderBindingRef,
        ProviderCommandOutcome, ReconciliationProof, ReconciliationProofRef, RequestId, RunId,
    },
    AGENT_PROTOCOL_V1,
};

const MAX_COMMITTED_DEPTH: u64 = 6;
const REQUEST_COUNT: usize = 2;
const COMMAND_COUNT: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Phase {
    Empty,
    Accepted,
    Running,
    Stopping,
    Delivered,
    Incomplete,
    Cancelled,
    Failed,
}

impl Phase {
    const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Delivered | Self::Incomplete | Self::Cancelled | Self::Failed
        )
    }

    const fn has_run_acceptance(self) -> bool {
        !matches!(self, Self::Empty)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Continuity {
    Confirmed,
    Unknown { last_confirmed_seq: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CommandStage {
    Unused,
    Received { received_seq: u64 },
    Accepted { recorded_seq: u64 },
    Applied { recorded_seq: u64, applied_seq: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Model {
    phase: Phase,
    continuity: Continuity,
    pending_requests: u8,
    used_requests: u8,
    commands: [CommandStage; COMMAND_COUNT],
    last_seq: u64,
}

impl Model {
    const fn initial() -> Self {
        Self {
            phase: Phase::Empty,
            continuity: Continuity::Confirmed,
            pending_requests: 0,
            used_requests: 0,
            commands: [CommandStage::Unused; COMMAND_COUNT],
            last_seq: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    AcceptRun,
    StartRun,
    OpenRequest(usize),
    ReceiveCancel(usize),
    AcceptCommand(usize),
    ApplyCancel(usize),
    Deliver,
    EndIncomplete,
    EndFailed,
    EndCancelled,
    LoseContinuity,
    RestoreContinuity,
}

const ACTIONS: [Action; 16] = [
    Action::AcceptRun,
    Action::StartRun,
    Action::OpenRequest(0),
    Action::OpenRequest(1),
    Action::ReceiveCancel(0),
    Action::ReceiveCancel(1),
    Action::AcceptCommand(0),
    Action::AcceptCommand(1),
    Action::ApplyCancel(0),
    Action::ApplyCancel(1),
    Action::Deliver,
    Action::EndIncomplete,
    Action::EndFailed,
    Action::EndCancelled,
    Action::LoseContinuity,
    Action::RestoreContinuity,
];

impl Action {
    fn label(self) -> String {
        match self {
            Self::AcceptRun => "accept-run".to_owned(),
            Self::StartRun => "start-run".to_owned(),
            Self::OpenRequest(index) => format!("open-request-{index}"),
            Self::ReceiveCancel(index) => format!("receive-cancel-{index}"),
            Self::AcceptCommand(index) => format!("accept-command-{index}"),
            Self::ApplyCancel(index) => format!("apply-cancel-{index}"),
            Self::Deliver => "deliver".to_owned(),
            Self::EndIncomplete => "end-incomplete".to_owned(),
            Self::EndFailed => "end-failed".to_owned(),
            Self::EndCancelled => "end-cancelled".to_owned(),
            Self::LoseContinuity => "lose-continuity".to_owned(),
            Self::RestoreContinuity => "restore-continuity".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpectedDisposition {
    Applied(Model),
    Quarantined(Model),
    Rejected,
}

impl ExpectedDisposition {
    const fn successor(self) -> Option<Model> {
        match self {
            Self::Applied(model) | Self::Quarantined(model) => Some(model),
            Self::Rejected => None,
        }
    }

    const fn kind(self) -> DispositionKind {
        match self {
            Self::Applied(_) => DispositionKind::Applied,
            Self::Quarantined(_) => DispositionKind::Quarantined,
            Self::Rejected => DispositionKind::Rejected,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DispositionKind {
    Applied,
    Quarantined,
    Rejected,
    ExactDuplicate,
}

/// Literal abstract transition relation. It neither calls the reducer nor
/// derives expectations from a wire event. Invalid non-terminal events are
/// journaled as quarantined protocol violations; terminal tails are rejected.
fn reference_transition(model: Model, action: Action) -> ExpectedDisposition {
    transition_with_terminal_guard(model, action, true)
}

/// The `terminal_guard` switch exists only for the mutation self-test below.
/// Production expectations always call `reference_transition` with the guard.
fn transition_with_terminal_guard(
    model: Model,
    action: Action,
    terminal_guard: bool,
) -> ExpectedDisposition {
    if terminal_guard && model.phase.is_terminal() {
        return ExpectedDisposition::Rejected;
    }

    if let Continuity::Unknown { .. } = model.continuity {
        return if action == Action::RestoreContinuity {
            let mut next = model;
            next.last_seq += 1;
            next.continuity = Continuity::Confirmed;
            ExpectedDisposition::Applied(next)
        } else {
            ExpectedDisposition::Quarantined(quarantine(model))
        };
    }

    let legal = match action {
        Action::AcceptRun => model.phase == Phase::Empty && model.last_seq == 0,
        Action::StartRun => model.phase == Phase::Accepted,
        Action::OpenRequest(index) => {
            model.phase == Phase::Running && model.used_requests & bit(index) == 0
        }
        Action::ReceiveCancel(index) => {
            model.phase.has_run_acceptance() && model.commands[index] == CommandStage::Unused
        }
        Action::AcceptCommand(index) => {
            model.phase.has_run_acceptance()
                && matches!(model.commands[index], CommandStage::Received { .. })
        }
        Action::ApplyCancel(index) => {
            matches!(model.phase, Phase::Accepted | Phase::Running)
                && matches!(model.commands[index], CommandStage::Accepted { .. })
        }
        Action::Deliver => {
            matches!(model.phase, Phase::Running | Phase::Stopping) && model.pending_requests == 0
        }
        Action::EndIncomplete | Action::EndFailed | Action::LoseContinuity => {
            model.phase.has_run_acceptance()
        }
        Action::EndCancelled => model.phase == Phase::Stopping,
        Action::RestoreContinuity => false,
    };

    if !legal {
        return ExpectedDisposition::Quarantined(quarantine(model));
    }

    let committed_seq = model.last_seq + 1;
    let mut next = model;
    next.last_seq = committed_seq;
    match action {
        Action::AcceptRun => next.phase = Phase::Accepted,
        Action::StartRun => next.phase = Phase::Running,
        Action::OpenRequest(index) => {
            next.used_requests |= bit(index);
            next.pending_requests |= bit(index);
        }
        Action::ReceiveCancel(index) => {
            next.commands[index] = CommandStage::Received {
                received_seq: committed_seq,
            };
        }
        Action::AcceptCommand(index) => {
            next.commands[index] = CommandStage::Accepted {
                recorded_seq: committed_seq,
            };
        }
        Action::ApplyCancel(index) => {
            let CommandStage::Accepted { recorded_seq } = model.commands[index] else {
                unreachable!("legality table requires an accepted command")
            };
            next.commands[index] = CommandStage::Applied {
                recorded_seq,
                applied_seq: committed_seq,
            };
            next.phase = Phase::Stopping;
        }
        Action::Deliver => next.phase = Phase::Delivered,
        Action::EndIncomplete => next.phase = Phase::Incomplete,
        Action::EndFailed => next.phase = Phase::Failed,
        Action::EndCancelled => next.phase = Phase::Cancelled,
        Action::LoseContinuity => {
            next.continuity = Continuity::Unknown {
                last_confirmed_seq: model.last_seq,
            };
        }
        Action::RestoreContinuity => unreachable!("handled by the Unknown branch"),
    }
    ExpectedDisposition::Applied(next)
}

const fn quarantine(model: Model) -> Model {
    let last_confirmed_seq = match model.continuity {
        Continuity::Confirmed => model.last_seq,
        Continuity::Unknown { last_confirmed_seq } => last_confirmed_seq,
    };
    Model {
        continuity: Continuity::Unknown { last_confirmed_seq },
        last_seq: model.last_seq + 1,
        ..model
    }
}

const fn bit(index: usize) -> u8 {
    1_u8 << index
}

#[derive(Clone)]
struct Fixture {
    request: AgentStartRequest,
    execution: AgentExecutionRef,
}

fn fixture() -> (Fixture, AgentRunReducer) {
    let descriptor = AgentDescriptorEnvelope::seal(AgentDescriptor {
        provider_id: AgentProviderId::new("bounded.provider"),
        agent_id: AgentId::new("bounded-agent-v1"),
        supported_protocol_versions: vec![AGENT_PROTOCOL_V1],
        accepted_content_types: BTreeSet::from(["text/plain".to_owned()]),
        capabilities: AgentCapabilities {
            controls: ControlCapabilities {
                cancel: CancelSupport::Confirmed,
                ..ControlCapabilities::default()
            },
            pending_request_kinds: BTreeSet::from([PendingRequestKind::Input]),
            effect_mediation: EffectMediation::HostMediated,
            ..AgentCapabilities::default()
        },
        extensions: Default::default(),
    })
    .expect("bounded descriptor must seal");
    let run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        AgentSessionId::new("bounded-session"),
        RunId::new("bounded-run"),
        vec![Content::text("bounded transition input")],
    )
    .expect("bounded run must seal");
    assert!(
        run.spec.resources.is_empty(),
        "resource cardinality is fixed at zero"
    );
    let request =
        AgentStartRequest::new(run, ProviderBindingRef::new("bounded-binding"), &descriptor)
            .expect("bounded start request must validate");
    let execution = AgentExecutionRef::for_start(&request, &descriptor)
        .expect("bounded execution identity must bind to start");
    let reducer = AgentRunReducer::new(
        execution.clone(),
        &request,
        &descriptor,
        AgentAdmission::default(),
    )
    .expect("bounded reducer must initialize");
    (Fixture { request, execution }, reducer)
}

#[derive(Debug, Clone)]
struct LossContext {
    last_confirmed_seq: u64,
    loss_event_digest: Digest,
}

#[derive(Debug, Clone, Copy)]
enum EventLane {
    Host,
    Provider,
    VerifiedReconciliation,
}

#[derive(Debug, Clone)]
struct EventAttempt {
    lane: EventLane,
    draft: AgentEventDraft,
}

fn event_attempt(
    fixture: &Fixture,
    model: Model,
    loss: Option<&LossContext>,
    action: Action,
) -> EventAttempt {
    let run_id = fixture.request.run.spec.run_id.clone();
    let event_id = AgentEventId::new(format!(
        "bounded-event-{}-{}",
        model.last_seq + 1,
        action.label()
    ));
    let (lane, causation_id, payload) = match action {
        Action::AcceptRun => (
            EventLane::Host,
            None,
            AgentEvent::RunAccepted {
                session_id: fixture.request.run.spec.session_id.clone(),
                spec_digest: fixture.request.run.spec_digest.clone(),
            },
        ),
        Action::StartRun => (EventLane::Provider, None, AgentEvent::RunStarted),
        Action::OpenRequest(index) => (
            EventLane::Provider,
            None,
            AgentEvent::RequestOpened {
                request: PendingRequest {
                    request_id: request_id(index),
                    blocking: true,
                    payload: PendingRequestPayload::Input {
                        prompt: vec![Content::text(format!("request {index}"))],
                        input_schema: None,
                    },
                },
            },
        ),
        Action::ReceiveCancel(index) => {
            let command = cancel_command(&run_id, index);
            (
                EventLane::Host,
                Some(command.command_id.clone()),
                AgentEvent::CommandReceived { command },
            )
        }
        Action::AcceptCommand(index) => (
            EventLane::Provider,
            Some(command_id(index)),
            AgentEvent::CommandDispositionRecorded {
                command_id: command_id(index),
                outcome: ProviderCommandOutcome::Accepted,
            },
        ),
        Action::ApplyCancel(index) => (
            EventLane::Provider,
            Some(command_id(index)),
            AgentEvent::StopRequested {
                reason: cancel_reason(index),
            },
        ),
        Action::Deliver => (
            EventLane::Provider,
            None,
            AgentEvent::DeliveryCommitted {
                delivery: delivery(fixture),
            },
        ),
        Action::EndIncomplete => (
            EventLane::Provider,
            None,
            AgentEvent::RunIncomplete {
                reason: bounded_incomplete_reason(),
                partial_delivery: None,
            },
        ),
        Action::EndFailed => (
            EventLane::Provider,
            None,
            AgentEvent::RunFailed {
                failure: bounded_failure(),
            },
        ),
        Action::EndCancelled => (
            EventLane::Provider,
            None,
            AgentEvent::RunCancelled {
                reason: bounded_cancelled_reason(),
            },
        ),
        Action::LoseContinuity => (
            EventLane::Host,
            None,
            AgentEvent::ContinuityLost {
                last_confirmed_seq: model.last_seq,
                reason: "bounded continuity loss".to_owned(),
            },
        ),
        Action::RestoreContinuity => {
            let (last_confirmed_seq, loss_event_digest) = loss.map_or_else(
                || (model.last_seq, Digest::sha256(b"no-current-loss")),
                |loss| (loss.last_confirmed_seq, loss.loss_event_digest.clone()),
            );
            let proof = ReconciliationProof::new(
                ReconciliationProofRef::new(format!("bounded-proof-{}", model.last_seq + 1)),
                last_confirmed_seq,
                loss_event_digest,
                Digest::sha256(format!("bounded-snapshot-{}", model.last_seq)),
            )
            .expect("bounded reconciliation proof must seal");
            (
                EventLane::VerifiedReconciliation,
                None,
                AgentEvent::ContinuityRestored {
                    proof,
                    reason: "bounded reconciliation".to_owned(),
                },
            )
        }
    };
    EventAttempt {
        lane,
        draft: AgentEventDraft {
            event_id,
            run_id,
            causation_id,
            source_fingerprint: None,
            payload,
        },
    }
}

fn cancel_command(run_id: &RunId, index: usize) -> AgentCommandEnvelope {
    AgentCommandEnvelope::new(
        command_id(index),
        run_id.clone(),
        None,
        AgentCommand::Cancel {
            reason: cancel_reason(index),
        },
    )
    .expect("bounded cancel command must seal")
}

fn command_id(index: usize) -> CommandId {
    CommandId::new(format!("bounded-command-{index}"))
}

fn request_id(index: usize) -> RequestId {
    RequestId::new(format!("bounded-request-{index}"))
}

fn cancel_reason(index: usize) -> String {
    format!("bounded cancel {index}")
}

fn bounded_cancelled_reason() -> String {
    "bounded cancellation completed".to_owned()
}

fn bounded_incomplete_reason() -> IncompleteReason {
    IncompleteReason::Interrupted {
        reason: "bounded incomplete".to_owned(),
    }
}

fn bounded_failure() -> AgentFailure {
    AgentFailure {
        code: "bounded_failure".to_owned(),
        message: "bounded failure".to_owned(),
        retryable: false,
        details: Default::default(),
    }
}

fn delivery(fixture: &Fixture) -> AgentDelivery {
    AgentDelivery {
        delivery_id: DeliveryId::new("bounded-delivery"),
        run_id: fixture.execution.run_id.clone(),
        spec_digest: fixture.execution.spec_digest.clone(),
        final_response: Content::text("bounded delivery"),
        outputs: Vec::new(),
        artifacts: Vec::new(),
        unresolved_issues: Vec::new(),
        usage: None,
        provenance: Provenance {
            provider_id: fixture.execution.provider_id.clone(),
            agent_id: fixture.execution.agent_id.clone(),
            supporting_event_ids: Vec::new(),
            extensions: Default::default(),
        },
    }
}

struct AcceptAllReconciliationProofs;

impl ReconciliationProofVerifier for AcceptAllReconciliationProofs {
    fn verify(
        &self,
        _execution: &AgentExecutionRef,
        _continuity: &AgentContinuityState,
        _proof: &ReconciliationProof,
    ) -> Result<(), AgentProtocolError> {
        Ok(())
    }
}

#[derive(Debug)]
struct Observation {
    reducer: AgentRunReducer,
    attempt: EventAttempt,
    kind: DispositionKind,
    run_seq: Option<u64>,
    event_digest: Option<Digest>,
    error_code: Option<AgentProtocolErrorCode>,
}

fn observe(mut reducer: AgentRunReducer, attempt: EventAttempt) -> Observation {
    let result = match attempt.lane {
        EventLane::Host => reducer.apply_host_draft(attempt.draft.clone()),
        EventLane::Provider => reducer.apply_provider_draft(attempt.draft.clone()),
        EventLane::VerifiedReconciliation => reducer
            .apply_verified_reconciliation(attempt.draft.clone(), &AcceptAllReconciliationProofs),
    };
    match result {
        Ok(applied) => {
            let run_seq = Some(applied.record.event.run_seq);
            let event_digest = Some(applied.record.event.event_digest.clone());
            let (kind, error_code) = match applied.outcome {
                ApplyOutcome::Applied => (DispositionKind::Applied, None),
                ApplyOutcome::ExactDuplicate => (DispositionKind::ExactDuplicate, None),
                ApplyOutcome::ProtocolViolation { error } => {
                    (DispositionKind::Quarantined, Some(error.code))
                }
                _ => panic!("bounded model does not know a future ApplyOutcome variant"),
            };
            Observation {
                reducer,
                attempt,
                kind,
                run_seq,
                event_digest,
                error_code,
            }
        }
        Err(error) => Observation {
            reducer,
            attempt,
            kind: DispositionKind::Rejected,
            run_seq: None,
            event_digest: None,
            error_code: Some(error.code),
        },
    }
}

#[derive(Clone)]
struct Node {
    model: Model,
    reducer: AgentRunReducer,
    loss: Option<LossContext>,
}

fn checked_successor(
    node: &Node,
    expected: ExpectedDisposition,
    observed: Observation,
) -> Option<Node> {
    assert_eq!(
        observed.kind,
        expected.kind(),
        "acceptance mismatch for {:?} from {:?}; error={:?}",
        observed.attempt.draft.payload,
        node.model,
        observed.error_code
    );

    let Some(next_model) = expected.successor() else {
        assert_eq!(
            observed.error_code,
            Some(AgentProtocolErrorCode::TerminalRun),
            "only terminal-tail rejection is in the bounded oracle"
        );
        assert_eq!(observed.reducer.state(), node.reducer.state());
        let repeated = observe(observed.reducer, observed.attempt);
        assert_eq!(repeated.kind, DispositionKind::Rejected);
        assert_eq!(
            repeated.error_code,
            Some(AgentProtocolErrorCode::TerminalRun)
        );
        return None;
    };

    assert_eq!(observed.run_seq, Some(next_model.last_seq));
    assert_projection(next_model, &observed.reducer);
    assert_exact_duplicate(&observed, next_model);

    let loss = match next_model.continuity {
        Continuity::Confirmed => None,
        Continuity::Unknown { last_confirmed_seq } => Some(LossContext {
            last_confirmed_seq,
            loss_event_digest: observed
                .event_digest
                .clone()
                .expect("a quarantined/loss event has a durable digest"),
        }),
    };
    Some(Node {
        model: next_model,
        reducer: observed.reducer,
        loss,
    })
}

fn assert_exact_duplicate(observed: &Observation, expected: Model) {
    let state_before = observed.reducer.state();
    let duplicate = observe(observed.reducer.clone(), observed.attempt.clone());
    assert_eq!(duplicate.kind, DispositionKind::ExactDuplicate);
    assert_eq!(duplicate.run_seq, observed.run_seq);
    assert_eq!(duplicate.event_digest, observed.event_digest);
    assert_eq!(duplicate.reducer.state(), state_before);
    assert_projection(expected, &duplicate.reducer);
}

fn assert_projection(model: Model, reducer: &AgentRunReducer) {
    let state = reducer.state();
    let expected_status = if model.phase.is_terminal() {
        match model.phase {
            Phase::Delivered => AgentRunStatus::Delivered,
            Phase::Incomplete => AgentRunStatus::Incomplete,
            Phase::Cancelled => AgentRunStatus::Cancelled,
            Phase::Failed => AgentRunStatus::Failed,
            _ => unreachable!("terminal predicate and status table disagree"),
        }
    } else if matches!(model.continuity, Continuity::Unknown { .. }) {
        AgentRunStatus::Unknown
    } else {
        match model.phase {
            Phase::Empty | Phase::Accepted => AgentRunStatus::Accepted,
            Phase::Running if model.pending_requests != 0 => AgentRunStatus::Waiting,
            Phase::Running => AgentRunStatus::Running,
            Phase::Stopping => AgentRunStatus::Stopping,
            _ => unreachable!("non-terminal status table is incomplete"),
        }
    };
    assert_eq!(
        state.status(),
        expected_status,
        "public status diverged for {model:?}"
    );

    match (model.phase, model.continuity, &state) {
        (phase, _, AgentRunState::Terminal { terminal }) if phase.is_terminal() => {
            assert_terminal_projection(phase, terminal);
        }
        (
            _,
            Continuity::Unknown { last_confirmed_seq },
            AgentRunState::Unknown {
                last_confirmed_seq: observed,
                reason,
            },
        ) => {
            assert_eq!(*observed, last_confirmed_seq);
            assert!(!reason.trim().is_empty());
        }
        (Phase::Empty | Phase::Accepted, Continuity::Confirmed, AgentRunState::Accepted) => {}
        (Phase::Running, Continuity::Confirmed, AgentRunState::Running)
            if model.pending_requests == 0 => {}
        (
            Phase::Running,
            Continuity::Confirmed,
            AgentRunState::Waiting {
                pending_request_ids,
            },
        ) => assert_eq!(pending_request_ids, &model_pending_ids(model)),
        (Phase::Stopping, Continuity::Confirmed, AgentRunState::Stopping) => {}
        _ => panic!("public AgentRunState {state:?} diverged from {model:?}"),
    }

    for (index, stage) in model.commands.into_iter().enumerate() {
        let command_id = command_id(index);
        match (stage, reducer.command_ack(&command_id, false)) {
            (CommandStage::Unused, Err(error)) => {
                assert_eq!(error.code, AgentProtocolErrorCode::CommandNotFound);
            }
            (CommandStage::Received { .. }, Err(error)) => {
                assert_eq!(error.code, AgentProtocolErrorCode::InvalidTransition);
            }
            (CommandStage::Accepted { recorded_seq }, Ok(ack)) => {
                assert_eq!(ack.command_id, command_id);
                assert_eq!(ack.state, CommandAckState::Accepted { recorded_seq });
            }
            (
                CommandStage::Applied {
                    recorded_seq,
                    applied_seq,
                },
                Ok(ack),
            ) => {
                assert_eq!(ack.command_id, command_id);
                assert_eq!(
                    ack.state,
                    CommandAckState::Applied {
                        recorded_seq,
                        applied_seq,
                    }
                );
            }
            (expected, actual) => {
                panic!("command projection diverged: expected {expected:?}, got {actual:?}")
            }
        }
    }
}

fn model_pending_ids(model: Model) -> Vec<RequestId> {
    (0..REQUEST_COUNT)
        .filter(|index| model.pending_requests & bit(*index) != 0)
        .map(request_id)
        .collect()
}

fn assert_terminal_projection(phase: Phase, terminal: &AgentTerminalState) {
    match (phase, terminal) {
        (Phase::Delivered, AgentTerminalState::Delivered { delivery_id }) => {
            assert_eq!(delivery_id, &DeliveryId::new("bounded-delivery"));
        }
        (Phase::Incomplete, AgentTerminalState::Incomplete { reason }) => {
            assert_eq!(reason, &bounded_incomplete_reason());
        }
        (Phase::Cancelled, AgentTerminalState::Cancelled { reason }) => {
            assert_eq!(reason, &bounded_cancelled_reason());
        }
        (Phase::Failed, AgentTerminalState::Failed { failure }) => {
            assert_eq!(failure, &bounded_failure());
        }
        _ => panic!("terminal projection {terminal:?} diverged from {phase:?}"),
    }
}

#[derive(Debug, Default)]
struct ExplorationStats {
    expanded_states: usize,
    unique_states: usize,
    transitions: usize,
    applied: usize,
    quarantined: usize,
    rejected_terminal_tails: usize,
}

#[test]
fn bounded_depth_reference_model_refines_public_reducer_not_an_unbounded_proof() {
    let (fixture, reducer) = fixture();
    let initial = Node {
        model: Model::initial(),
        reducer,
        loss: None,
    };
    assert_projection(initial.model, &initial.reducer);

    let mut seen = BTreeSet::from([initial.model]);
    let mut frontier = vec![initial];
    let mut stats = ExplorationStats::default();

    for depth in 0..MAX_COMMITTED_DEPTH {
        let mut next_frontier = BTreeMap::<Model, Node>::new();
        for node in frontier {
            assert_eq!(node.model.last_seq, depth);
            assert_projection(node.model, &node.reducer);
            stats.expanded_states += 1;

            for action in ACTIONS {
                stats.transitions += 1;
                let expected = reference_transition(node.model, action);
                match expected.kind() {
                    DispositionKind::Applied => stats.applied += 1,
                    DispositionKind::Quarantined => stats.quarantined += 1,
                    DispositionKind::Rejected => stats.rejected_terminal_tails += 1,
                    DispositionKind::ExactDuplicate => {
                        unreachable!("oracle has no duplicate action")
                    }
                }
                let attempt = event_attempt(&fixture, node.model, node.loss.as_ref(), action);
                let observed = observe(node.reducer.clone(), attempt);
                if let Some(successor) = checked_successor(&node, expected, observed) {
                    if seen.insert(successor.model) {
                        next_frontier.insert(successor.model, successor);
                    }
                }
            }
        }
        frontier = next_frontier.into_values().collect();
    }
    stats.unique_states = seen.len();

    let phases = seen
        .iter()
        .map(|model| model.phase)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        phases,
        BTreeSet::from([
            Phase::Empty,
            Phase::Accepted,
            Phase::Running,
            Phase::Stopping,
            Phase::Delivered,
            Phase::Incomplete,
            Phase::Cancelled,
            Phase::Failed,
        ])
    );
    assert!(seen
        .iter()
        .any(|model| { matches!(model.continuity, Continuity::Unknown { .. }) }));
    assert!(seen.iter().any(|model| model.used_requests == 0b11));
    assert!(seen.iter().any(|model| {
        model
            .commands
            .iter()
            .all(|stage| !matches!(stage, CommandStage::Unused))
    }));

    // These are intentional lower bounds, not a proof over unbounded traces.
    // A smaller exploration indicates that an abstract state/action silently
    // disappeared; larger counts are allowed when the bounded model expands.
    assert!(stats.unique_states >= 100, "{stats:?}");
    assert!(stats.transitions >= 1_000, "{stats:?}");
    assert!(stats.applied > 0, "{stats:?}");
    assert!(stats.quarantined > 0, "{stats:?}");
    assert!(stats.rejected_terminal_tails > 0, "{stats:?}");
    eprintln!("bounded Agent Protocol refinement exploration: {stats:?}");
}

#[derive(Debug, PartialEq, Eq)]
struct RefinementMismatch {
    step: usize,
    action: Action,
    expected: DispositionKind,
    observed: DispositionKind,
    observed_error: Option<AgentProtocolErrorCode>,
}

#[test]
fn bounded_refinement_mutation_self_test_catches_deleted_terminal_guard() {
    let (fixture, reducer) = fixture();
    let mut node = Node {
        model: Model::initial(),
        reducer,
        loss: None,
    };

    for action in [Action::AcceptRun, Action::StartRun, Action::Deliver] {
        let expected = reference_transition(node.model, action);
        let attempt = event_attempt(&fixture, node.model, node.loss.as_ref(), action);
        node = checked_successor(&node, expected, observe(node.reducer.clone(), attempt))
            .expect("mutation trace prefix must commit");
    }
    assert_eq!(node.model.phase, Phase::Delivered);

    // This deliberately bad oracle models deletion of the terminal guard. Its
    // general `RunFailed` rule therefore claims a second terminal is legal.
    let action = Action::EndFailed;
    let mutant_expected = transition_with_terminal_guard(node.model, action, false);
    let observed = observe(
        node.reducer.clone(),
        event_attempt(&fixture, node.model, node.loss.as_ref(), action),
    );
    let mismatch = RefinementMismatch {
        step: 3,
        action,
        expected: mutant_expected.kind(),
        observed: observed.kind,
        observed_error: observed.error_code,
    };

    assert_eq!(
        mismatch,
        RefinementMismatch {
            step: 3,
            action: Action::EndFailed,
            expected: DispositionKind::Applied,
            observed: DispositionKind::Rejected,
            observed_error: Some(AgentProtocolErrorCode::TerminalRun),
        }
    );
    assert_eq!(observed.reducer.state().status(), AgentRunStatus::Delivered);
}
