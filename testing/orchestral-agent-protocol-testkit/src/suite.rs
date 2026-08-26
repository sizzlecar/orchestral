use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

use futures_util::TryStreamExt;
use orchestral_core::agent_protocol::{
    reference::{AgentRunReducer, AgentRunStatus, ApplyOutcome},
    spi::{AgentProvider, AgentRecoveryRequest, AgentStartError},
    wire::{
        AgentAdmission, AgentDelivery, AgentDescriptorEnvelope, AgentEvent, AgentEventDraft,
        AgentEventId, AgentProtocolError, AgentProtocolErrorCode, AgentProviderStreamItem,
        AgentRejectionCode, AgentRunEnvelope, AgentSessionId, AgentStartRequest, Content,
        DeliveryId, Digest, Provenance, ProviderBindingRef, RunId,
    },
    AGENT_PROTOCOL_V1,
};

use crate::report::{case_ids, CaseId, CaseResult, CaseVerdict, ConformanceReport};

/// Probe storage lives outside the provider under test. A fixture adapter must
/// increment it at the actual native-work boundary, not at Provider `start`.
#[derive(Debug, Clone, Default)]
pub struct TestProbes {
    native_starts: Arc<AtomicUsize>,
    native_start_identities: Arc<Mutex<BTreeMap<(RunId, Digest), usize>>>,
}

impl TestProbes {
    /// Compatibility hook for external fixtures that can only expose a total
    /// native-work counter. In-tree fixtures use `record_native_start_for` so
    /// the fault suite can also prove immutable identity preservation.
    pub fn record_native_start(&self) {
        self.native_starts.fetch_add(1, Ordering::SeqCst);
    }

    pub fn record_native_start_for(&self, request: &AgentStartRequest) {
        self.record_native_start();
        let mut identities = self
            .native_start_identities
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *identities
            .entry((
                request.run.spec.run_id.clone(),
                request.run.spec_digest.clone(),
            ))
            .or_default() += 1;
    }

    pub fn native_start_count(&self) -> usize {
        self.native_starts.load(Ordering::SeqCst)
    }

    pub fn native_start_count_for(&self, request: &AgentStartRequest) -> usize {
        self.native_start_identities
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&(
                request.run.spec.run_id.clone(),
                request.run.spec_digest.clone(),
            ))
            .copied()
            .unwrap_or_default()
    }

    pub fn native_start_identities(&self) -> BTreeMap<(RunId, Digest), usize> {
        self.native_start_identities
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

/// Fully deterministic input a fixture must execute without network or
/// environment prerequisites.
#[derive(Debug, Clone)]
pub struct ProviderScenario {
    pub start_request: AgentStartRequest,
    pub immediate_events: Vec<AgentEventDraft>,
}

impl ProviderScenario {
    pub fn standard(descriptor: &AgentDescriptorEnvelope) -> Result<Self, AgentProtocolError> {
        let run = AgentRunEnvelope::new(
            AGENT_PROTOCOL_V1,
            AgentSessionId::new("conformance-session"),
            RunId::new("conformance-run"),
            vec![Content::text("complete the deterministic fixture")],
        )?;
        let start_request = AgentStartRequest::new(
            run,
            ProviderBindingRef::new("conformance-binding"),
            descriptor,
        )?;
        let immediate_events = Self::completion_events_for(descriptor, &start_request);
        Ok(Self {
            start_request,
            immediate_events,
        })
    }

    pub fn conflicting_request(&self) -> Result<AgentStartRequest, AgentProtocolError> {
        let mut spec = self.start_request.run.spec.clone();
        spec.input = vec![Content::text("different immutable input")];
        Ok(AgentStartRequest {
            run: AgentRunEnvelope::seal(spec)?,
            provider_binding: self.start_request.provider_binding.clone(),
            expected_descriptor_digest: self.start_request.expected_descriptor_digest.clone(),
        })
    }

    pub(crate) fn completion_events_for(
        descriptor: &AgentDescriptorEnvelope,
        request: &AgentStartRequest,
    ) -> Vec<AgentEventDraft> {
        immediate_completion_events(descriptor, request)
    }
}

/// Creates a fresh provider for each case and wires externally-owned probes to
/// its native execution boundary.
pub trait ProviderFixtureFactory: Send + Sync {
    fn descriptor(&self) -> AgentDescriptorEnvelope;

    fn create(&self, scenario: ProviderScenario, probes: TestProbes) -> Arc<dyn AgentProvider>;
}

/// A fixture whose first response for every new immutable start is
/// deliberately lost after native work has begun. This separates mandatory
/// transport-fault assertions from the ordinary base suite.
pub trait OutcomeUnknownFixtureFactory: ProviderFixtureFactory {}

#[derive(Debug, Clone, Copy, Default)]
pub struct ConformanceSuite;

impl ConformanceSuite {
    /// Runs the deterministic Provider base suite. Recovery and session cases
    /// branch on descriptor capabilities, but every branch is a positive
    /// assertion in the report: there are no capability skips.
    pub async fn run(factory: &dyn ProviderFixtureFactory) -> ConformanceReport {
        let descriptor = factory.descriptor();
        let mut expected_cases = Vec::with_capacity(6);
        let mut cases = Vec::with_capacity(6);
        expected_cases.push(case_ids::DESCRIPTOR_START_BINDING);
        cases.push(run_case(
            case_ids::DESCRIPTOR_START_BINDING,
            descriptor_start_binding(factory).await,
        ));
        expected_cases.push(case_ids::SAME_RUN_START_IDEMPOTENT);
        cases.push(run_case(
            case_ids::SAME_RUN_START_IDEMPOTENT,
            same_run_start_idempotent(factory).await,
        ));
        expected_cases.push(case_ids::RUN_ID_DIGEST_CONFLICT);
        cases.push(run_case(
            case_ids::RUN_ID_DIGEST_CONFLICT,
            run_id_digest_conflict(factory).await,
        ));
        expected_cases.push(case_ids::ATOMIC_IMMEDIATE_COMPLETION);
        cases.push(run_case(
            case_ids::ATOMIC_IMMEDIATE_COMPLETION,
            atomic_immediate_completion(factory).await,
        ));

        let (recovery_case_id, recovery_result) =
            if descriptor.descriptor.capabilities.controls.recover {
                (
                    case_ids::RECOVER_STABLE_WHEN_DECLARED,
                    recover_stable_when_declared(factory).await,
                )
            } else {
                (
                    case_ids::RECOVER_UNSUPPORTED_WHEN_UNDECLARED,
                    recover_unsupported_when_undeclared(factory).await,
                )
            };
        expected_cases.push(recovery_case_id);
        cases.push(run_case(recovery_case_id, recovery_result));

        let (session_case_id, session_result) = if descriptor.descriptor.capabilities.session_reuse
        {
            (
                case_ids::SESSION_ISOLATION_WHEN_REUSE_DECLARED,
                session_isolation_when_reuse_declared(factory).await,
            )
        } else {
            (
                case_ids::SESSION_CONFLICT_WHEN_REUSE_UNDECLARED,
                session_conflict_when_reuse_undeclared(factory).await,
            )
        };
        expected_cases.push(session_case_id);
        cases.push(run_case(session_case_id, session_result));

        ConformanceReport::for_cases(expected_cases, cases)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OutcomeUnknownConformanceSuite;

impl OutcomeUnknownConformanceSuite {
    /// Runs the mandatory fault-injection set. It is deliberately distinct
    /// from the base suite because ordinary conformant Providers must not lose
    /// every first start response.
    pub async fn run(factory: &dyn OutcomeUnknownFixtureFactory) -> ConformanceReport {
        let case_id = case_ids::OUTCOME_UNKNOWN_IDENTITY_RETRY_1000;
        ConformanceReport::for_cases(
            [case_id],
            vec![run_case(
                case_id,
                outcome_unknown_identity_retry_1000(factory).await,
            )],
        )
    }
}

enum CaseFailure {
    Failed(String),
    NotProven(String),
}

fn run_case(case_id: CaseId, result: Result<(), CaseFailure>) -> CaseResult {
    let verdict = match result {
        Ok(()) => CaseVerdict::Passed,
        Err(CaseFailure::Failed(reason)) => CaseVerdict::Failed { reason },
        Err(CaseFailure::NotProven(reason)) => CaseVerdict::NotProven { reason },
    };
    CaseResult { case_id, verdict }
}

fn setup(
    factory: &dyn ProviderFixtureFactory,
) -> Result<
    (
        AgentDescriptorEnvelope,
        ProviderScenario,
        TestProbes,
        Arc<dyn AgentProvider>,
    ),
    CaseFailure,
> {
    let expected_descriptor = factory.descriptor();
    expected_descriptor.validate_integrity().map_err(|error| {
        CaseFailure::NotProven(format!("fixture descriptor is invalid: {error}"))
    })?;
    let scenario = ProviderScenario::standard(&expected_descriptor).map_err(|error| {
        CaseFailure::NotProven(format!("could not build deterministic scenario: {error}"))
    })?;
    let probes = TestProbes::default();
    let provider = factory.create(scenario.clone(), probes.clone());
    Ok((expected_descriptor, scenario, probes, provider))
}

async fn descriptor_start_binding(factory: &dyn ProviderFixtureFactory) -> Result<(), CaseFailure> {
    let (expected_descriptor, scenario, probes, provider) = setup(factory)?;
    let observed_descriptor = provider.describe();
    if observed_descriptor != expected_descriptor {
        return Err(CaseFailure::Failed(
            "describe() differs from the immutable fixture descriptor".to_owned(),
        ));
    }
    observed_descriptor.validate_integrity().map_err(|error| {
        CaseFailure::Failed(format!("describe() returned invalid data: {error}"))
    })?;

    let mut mismatched = scenario.start_request.clone();
    mismatched.expected_descriptor_digest = Digest::sha256(b"different-descriptor");
    match provider.start(mismatched).await {
        Err(AgentStartError::Rejected(rejection))
            if rejection.code == AgentRejectionCode::RunIdConflict
                && probes.native_start_count() == 0 => {}
        Err(AgentStartError::Rejected(rejection))
            if rejection.code != AgentRejectionCode::RunIdConflict =>
        {
            return Err(CaseFailure::Failed(format!(
                "descriptor mismatch returned {:?} instead of RunIdConflict",
                rejection.code
            )));
        }
        Err(AgentStartError::Rejected(_)) => {
            return Err(CaseFailure::Failed(
                "descriptor mismatch created native work before rejection".to_owned(),
            ));
        }
        Err(AgentStartError::OutcomeUnknown(_)) => {
            return Err(CaseFailure::Failed(
                "descriptor mismatch returned OutcomeUnknown instead of a no-work rejection"
                    .to_owned(),
            ));
        }
        Err(_) => {
            return Err(CaseFailure::Failed(
                "descriptor mismatch returned an unrecognized start error".to_owned(),
            ));
        }
        Ok(_) => {
            return Err(CaseFailure::Failed(
                "descriptor mismatch was accepted".to_owned(),
            ));
        }
    }

    let started = provider
        .start(scenario.start_request.clone())
        .await
        .map_err(|error| CaseFailure::Failed(format!("valid start was rejected: {error}")))?;
    started
        .execution
        .validate_for(&scenario.start_request, &observed_descriptor)
        .map_err(|error| {
            CaseFailure::Failed(format!(
                "execution is not bound to the start request: {error}"
            ))
        })?;
    let compatibility = observed_descriptor
        .descriptor
        .check_run_compatibility(&scenario.start_request.run)
        .map_err(|error| {
            CaseFailure::NotProven(format!("fixture run is statically incompatible: {error}"))
        })?;
    started
        .admission
        .validate_against(&scenario.start_request.run, &compatibility)
        .map_err(|error| CaseFailure::Failed(format!("invalid start admission: {error}")))?;
    if probes.native_start_count() != 1 {
        return Err(CaseFailure::Failed(format!(
            "valid start created {} native executions instead of one",
            probes.native_start_count()
        )));
    }
    Ok(())
}

async fn same_run_start_idempotent(
    factory: &dyn ProviderFixtureFactory,
) -> Result<(), CaseFailure> {
    let (descriptor, scenario, probes, provider) = setup(factory)?;
    let first = provider
        .start(scenario.start_request.clone())
        .await
        .map_err(|error| CaseFailure::Failed(format!("first start failed: {error}")))?;
    first
        .execution
        .validate_for(&scenario.start_request, &descriptor)
        .map_err(|error| CaseFailure::Failed(format!("first execution is invalid: {error}")))?;
    for replay_index in 1..100 {
        let replay = provider
            .start(scenario.start_request.clone())
            .await
            .map_err(|error| {
                CaseFailure::Failed(format!(
                    "idempotent start replay {replay_index} failed: {error}"
                ))
            })?;
        replay
            .execution
            .validate_for(&scenario.start_request, &descriptor)
            .map_err(|error| {
                CaseFailure::Failed(format!(
                    "replayed execution {replay_index} is invalid: {error}"
                ))
            })?;
        if first.execution != replay.execution || first.admission != replay.admission {
            return Err(CaseFailure::Failed(format!(
                "start replay {replay_index} changed execution or admission"
            )));
        }
    }
    if probes.native_start_count() != 1 {
        return Err(CaseFailure::Failed(format!(
            "same run/spec produced {} native starts",
            probes.native_start_count()
        )));
    }
    Ok(())
}

async fn run_id_digest_conflict(factory: &dyn ProviderFixtureFactory) -> Result<(), CaseFailure> {
    let (_, scenario, probes, provider) = setup(factory)?;
    provider
        .start(scenario.start_request.clone())
        .await
        .map_err(|error| CaseFailure::Failed(format!("first start failed: {error}")))?;
    let conflicting = scenario.conflicting_request().map_err(|error| {
        CaseFailure::NotProven(format!("could not build conflicting run: {error}"))
    })?;
    match provider.start(conflicting).await {
        Err(AgentStartError::Rejected(rejection))
            if rejection.code == AgentRejectionCode::RunIdConflict => {}
        Err(other) => {
            return Err(CaseFailure::Failed(format!(
                "digest conflict returned the wrong failure: {other}"
            )))
        }
        Ok(_) => {
            return Err(CaseFailure::Failed(
                "same run_id with a different digest was accepted".to_owned(),
            ))
        }
    }
    if probes.native_start_count() != 1 {
        return Err(CaseFailure::Failed(format!(
            "digest conflict changed native start count to {}",
            probes.native_start_count()
        )));
    }
    Ok(())
}

async fn atomic_immediate_completion(
    factory: &dyn ProviderFixtureFactory,
) -> Result<(), CaseFailure> {
    let (_, scenario, probes, provider) = setup(factory)?;
    let expected_event_ids = scenario
        .immediate_events
        .iter()
        .map(|draft| draft.event_id.clone())
        .collect::<Vec<_>>();
    let start = provider
        .start(scenario.start_request.clone())
        .await
        .map_err(|error| CaseFailure::Failed(format!("immediate start failed: {error}")))?;
    let mut reducer = AgentRunReducer::new(
        start.execution.clone(),
        &scenario.start_request,
        &factory.descriptor(),
        start.admission.clone(),
    )
    .map_err(|error| CaseFailure::Failed(format!("Host reducer rejected start: {error}")))?;
    reducer
        .apply_host_draft(AgentEventDraft {
            event_id: AgentEventId::new("host-run-accepted"),
            run_id: scenario.start_request.run.spec.run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RunAccepted {
                session_id: scenario.start_request.run.spec.session_id.clone(),
                spec_digest: scenario.start_request.run.spec_digest.clone(),
            },
        })
        .map_err(|error| CaseFailure::Failed(format!("Host could not accept run: {error}")))?;
    apply_admission_skips(&mut reducer, &scenario.start_request, &start.admission)?;
    let items = start
        .stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| {
            CaseFailure::Failed(format!("immediate completion stream failed: {error}"))
        })?;
    let events = items
        .into_iter()
        .map(|item| match item {
            AgentProviderStreamItem::Event(draft) => {
                draft.validate_integrity().map_err(|error| {
                    CaseFailure::Failed(format!("Provider emitted an invalid draft: {error}"))
                })?;
                Ok(*draft)
            }
            AgentProviderStreamItem::Telemetry(_) => Err(CaseFailure::Failed(
                "fixture completion was replaced by telemetry".to_owned(),
            )),
            _ => Err(CaseFailure::Failed(
                "fixture emitted an unsupported stream item".to_owned(),
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let observed_ids = events
        .iter()
        .map(|draft| draft.event_id.clone())
        .collect::<Vec<_>>();
    if observed_ids != expected_event_ids
        || !matches!(
            events.first().map(|event| &event.payload),
            Some(AgentEvent::RunStarted)
        )
        || !matches!(
            events.last().map(|event| &event.payload),
            Some(AgentEvent::DeliveryCommitted { .. })
        )
    {
        return Err(CaseFailure::Failed(
            "atomic start stream lost or reordered immediate terminal events".to_owned(),
        ));
    }
    for event in events {
        let applied = reducer.apply_provider_draft(event).map_err(|error| {
            CaseFailure::Failed(format!("Host rejected immediate Provider event: {error}"))
        })?;
        if !matches!(applied.outcome, ApplyOutcome::Applied) {
            return Err(CaseFailure::Failed(
                "immediate Provider event did not advance the Host projection".to_owned(),
            ));
        }
    }
    if reducer.state().status() != AgentRunStatus::Delivered {
        return Err(CaseFailure::Failed(
            "immediate completion did not reduce to Delivered".to_owned(),
        ));
    }
    if probes.native_start_count() != 1 {
        return Err(CaseFailure::Failed(
            "immediate completion did not correspond to exactly one native start".to_owned(),
        ));
    }
    Ok(())
}

fn apply_admission_skips(
    reducer: &mut AgentRunReducer,
    request: &AgentStartRequest,
    admission: &AgentAdmission,
) -> Result<(), CaseFailure> {
    for (index, skip) in admission.skipped_optional_bindings.iter().enumerate() {
        reducer
            .apply_host_draft(AgentEventDraft {
                event_id: AgentEventId::new(format!("host-resource-skip-{index}")),
                run_id: request.run.spec.run_id.clone(),
                causation_id: None,
                source_fingerprint: None,
                payload: AgentEvent::ResourceBindingSkipped { skip: skip.clone() },
            })
            .map_err(|error| {
                CaseFailure::Failed(format!("Host could not journal start admission: {error}"))
            })?;
    }
    Ok(())
}

async fn recover_unsupported_when_undeclared(
    factory: &dyn ProviderFixtureFactory,
) -> Result<(), CaseFailure> {
    let (descriptor, scenario, _, provider) = setup(factory)?;
    if descriptor.descriptor.capabilities.controls.recover {
        return Err(CaseFailure::Failed(
            "recover=false contract case was selected for a recover=true descriptor".to_owned(),
        ));
    }
    let start_request = scenario.start_request;
    let start = provider
        .start(start_request.clone())
        .await
        .map_err(|error| CaseFailure::Failed(format!("start before recover failed: {error}")))?;
    let recovery = AgentRecoveryRequest::new(start_request, start.execution.clone(), &descriptor)
        .map_err(|error| {
        CaseFailure::Failed(format!("recovery request was invalid: {error}"))
    })?;
    match provider.recover(recovery).await {
        Err(error) if error.code == AgentProtocolErrorCode::Unsupported => Ok(()),
        Err(error) => Err(CaseFailure::Failed(format!(
            "recover=false returned {:?} instead of Unsupported",
            error.code
        ))),
        Ok(_) => Err(CaseFailure::Failed(
            "recover=false returned a stream instead of Unsupported".to_owned(),
        )),
    }
}

async fn recover_stable_when_declared(
    factory: &dyn ProviderFixtureFactory,
) -> Result<(), CaseFailure> {
    let (descriptor, scenario, probes, provider) = setup(factory)?;
    if !descriptor.descriptor.capabilities.controls.recover {
        return Err(CaseFailure::Failed(
            "recover=true contract case was selected for a recover=false descriptor".to_owned(),
        ));
    }
    let start = provider
        .start(scenario.start_request.clone())
        .await
        .map_err(|error| CaseFailure::Failed(format!("start before recover failed: {error}")))?;
    let execution = start.execution.clone();
    let initial = collect_provider_drafts(start.stream, "initial stream").await?;
    let recovery = AgentRecoveryRequest::new(scenario.start_request, execution, &descriptor)
        .map_err(|error| CaseFailure::Failed(format!("recovery request was invalid: {error}")))?;
    let recovered_stream = provider.recover(recovery).await.map_err(|error| {
        CaseFailure::Failed(format!(
            "recover=true returned {:?} instead of a stream: {error}",
            error.code
        ))
    })?;
    let recovered = collect_provider_drafts(recovered_stream, "recovery stream").await?;
    if initial != recovered {
        return Err(CaseFailure::Failed(
            "recovery changed draft IDs, order, or semantic content".to_owned(),
        ));
    }
    if initial.is_empty()
        || initial
            .iter()
            .map(|draft| &draft.event_id)
            .collect::<BTreeSet<_>>()
            .len()
            != initial.len()
    {
        return Err(CaseFailure::Failed(
            "recovery fixture did not expose a non-empty stream of unique stable draft IDs"
                .to_owned(),
        ));
    }
    if probes.native_start_count() != 1 {
        return Err(CaseFailure::Failed(format!(
            "recover created native work; observed {} starts",
            probes.native_start_count()
        )));
    }
    Ok(())
}

async fn session_conflict_when_reuse_undeclared(
    factory: &dyn ProviderFixtureFactory,
) -> Result<(), CaseFailure> {
    let (descriptor, scenario, probes, provider) = setup(factory)?;
    if descriptor.descriptor.capabilities.session_reuse {
        return Err(CaseFailure::Failed(
            "session_reuse=false case was selected for a reusable-session descriptor".to_owned(),
        ));
    }
    let first = provider
        .start(scenario.start_request.clone())
        .await
        .map_err(|error| CaseFailure::Failed(format!("first session start failed: {error}")))?;
    first
        .execution
        .validate_for(&scenario.start_request, &descriptor)
        .map_err(|error| {
            CaseFailure::Failed(format!(
                "first stateless execution crossed identity: {error}"
            ))
        })?;
    let first_drafts = collect_provider_drafts(first.stream, "first stateless stream").await?;
    if first_drafts != ProviderScenario::completion_events_for(&descriptor, &scenario.start_request)
    {
        return Err(CaseFailure::Failed(
            "first stateless Run received a crossed event stream".to_owned(),
        ));
    }
    let second = request_for(
        &descriptor,
        scenario.start_request.run.spec.session_id.clone(),
        RunId::new("conformance-session-second-run"),
    )?;
    match provider.start(second).await {
        Err(AgentStartError::Rejected(rejection))
            if rejection.code == AgentRejectionCode::SessionConflict => {}
        Err(other) => {
            return Err(CaseFailure::Failed(format!(
                "session_reuse=false returned the wrong second-run failure: {other}"
            )))
        }
        Ok(_) => {
            return Err(CaseFailure::Failed(
                "session_reuse=false accepted a second Run in the same session".to_owned(),
            ))
        }
    }
    if probes.native_start_count() != 1 {
        return Err(CaseFailure::Failed(format!(
            "SessionConflict changed native start count to {}",
            probes.native_start_count()
        )));
    }

    let mut execution_run_ids = BTreeSet::from([first.execution.run_id]);
    let mut event_ids = first_drafts
        .iter()
        .map(|draft| draft.event_id.clone())
        .collect::<BTreeSet<_>>();
    let mut delivery_ids = first_drafts
        .iter()
        .filter_map(|draft| match &draft.payload {
            AgentEvent::DeliveryCommitted { delivery } => Some(delivery.delivery_id.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    for run_index in 1..100 {
        let request = request_for(
            &descriptor,
            AgentSessionId::new(format!("conformance-stateless-session-{run_index:03}")),
            RunId::new(format!("conformance-stateless-run-{run_index:03}")),
        )?;
        let expected = ProviderScenario::completion_events_for(&descriptor, &request);
        let start = provider.start(request.clone()).await.map_err(|error| {
            CaseFailure::Failed(format!("stateless Run {run_index} failed: {error}"))
        })?;
        start
            .execution
            .validate_for(&request, &descriptor)
            .map_err(|error| {
                CaseFailure::Failed(format!(
                    "stateless execution {run_index} crossed identity: {error}"
                ))
            })?;
        if !execution_run_ids.insert(start.execution.run_id.clone()) {
            return Err(CaseFailure::Failed(format!(
                "stateless Provider reused a Run identity at {run_index}"
            )));
        }
        let drafts = collect_provider_drafts(start.stream, "stateless isolation stream").await?;
        if drafts != expected {
            return Err(CaseFailure::Failed(format!(
                "stateless Run {run_index} received another Run's event stream"
            )));
        }
        let current_event_ids = drafts
            .iter()
            .map(|draft| draft.event_id.clone())
            .collect::<BTreeSet<_>>();
        for draft in &drafts {
            if draft.run_id != request.run.spec.run_id || !event_ids.insert(draft.event_id.clone())
            {
                return Err(CaseFailure::Failed(format!(
                    "stateless Run {run_index} has a crossed or reused event identity"
                )));
            }
            if let AgentEvent::DeliveryCommitted { delivery } = &draft.payload {
                if delivery.run_id != request.run.spec.run_id
                    || delivery.spec_digest != request.run.spec_digest
                    || !delivery_ids.insert(delivery.delivery_id.clone())
                    || delivery
                        .provenance
                        .supporting_event_ids
                        .iter()
                        .any(|event_id| !current_event_ids.contains(event_id))
                {
                    return Err(CaseFailure::Failed(format!(
                        "stateless Run {run_index} delivery or provenance crossed a Run boundary"
                    )));
                }
            }
        }
    }
    if execution_run_ids.len() != 100 || event_ids.len() != 200 || delivery_ids.len() != 100 {
        return Err(CaseFailure::Failed(
            "100 stateless Runs did not retain isolated executions, events, and deliveries"
                .to_owned(),
        ));
    }
    if probes.native_start_count() != 100 {
        return Err(CaseFailure::Failed(format!(
            "100 stateless Runs created {} native starts",
            probes.native_start_count()
        )));
    }
    Ok(())
}

async fn session_isolation_when_reuse_declared(
    factory: &dyn ProviderFixtureFactory,
) -> Result<(), CaseFailure> {
    let (descriptor, scenario, probes, provider) = setup(factory)?;
    if !descriptor.descriptor.capabilities.session_reuse {
        return Err(CaseFailure::Failed(
            "session_reuse=true case was selected for a single-run-session descriptor".to_owned(),
        ));
    }

    let session_id = scenario.start_request.run.spec.session_id.clone();
    let mut execution_run_ids = BTreeSet::new();
    let mut event_ids = BTreeSet::new();
    let mut delivery_ids = BTreeSet::new();
    for run_index in 0..100 {
        let request = request_for(
            &descriptor,
            session_id.clone(),
            RunId::new(format!("conformance-reused-session-run-{run_index:03}")),
        )?;
        let expected = ProviderScenario::completion_events_for(&descriptor, &request);
        let start = provider.start(request.clone()).await.map_err(|error| {
            CaseFailure::Failed(format!("reused-session Run {run_index} failed: {error}"))
        })?;
        start
            .execution
            .validate_for(&request, &descriptor)
            .map_err(|error| {
                CaseFailure::Failed(format!(
                    "reused-session execution {run_index} crossed identity: {error}"
                ))
            })?;
        if !execution_run_ids.insert(start.execution.run_id.clone()) {
            return Err(CaseFailure::Failed(format!(
                "reused session returned a duplicate execution Run identity at {run_index}"
            )));
        }

        let drafts = collect_provider_drafts(start.stream, "reused-session stream").await?;
        if drafts != expected {
            return Err(CaseFailure::Failed(format!(
                "Run {run_index} received another Run's event stream"
            )));
        }
        let current_event_ids = drafts
            .iter()
            .map(|draft| draft.event_id.clone())
            .collect::<BTreeSet<_>>();
        for draft in &drafts {
            if draft.run_id != request.run.spec.run_id || !event_ids.insert(draft.event_id.clone())
            {
                return Err(CaseFailure::Failed(format!(
                    "Run {run_index} has a crossed or reused event identity"
                )));
            }
            if let AgentEvent::DeliveryCommitted { delivery } = &draft.payload {
                if delivery.run_id != request.run.spec.run_id
                    || delivery.spec_digest != request.run.spec_digest
                    || !delivery_ids.insert(delivery.delivery_id.clone())
                    || delivery
                        .provenance
                        .supporting_event_ids
                        .iter()
                        .any(|event_id| !current_event_ids.contains(event_id))
                {
                    return Err(CaseFailure::Failed(format!(
                        "Run {run_index} delivery or provenance crossed a Run boundary"
                    )));
                }
            }
        }
    }
    if execution_run_ids.len() != 100 || event_ids.len() != 200 || delivery_ids.len() != 100 {
        return Err(CaseFailure::Failed(
            "100 reusable-session Runs did not retain one execution and delivery each".to_owned(),
        ));
    }
    if probes.native_start_count() != 100 {
        return Err(CaseFailure::Failed(format!(
            "100 distinct Runs created {} native starts",
            probes.native_start_count()
        )));
    }
    Ok(())
}

async fn outcome_unknown_identity_retry_1000(
    factory: &dyn OutcomeUnknownFixtureFactory,
) -> Result<(), CaseFailure> {
    let descriptor = factory.descriptor();
    descriptor.validate_integrity().map_err(|error| {
        CaseFailure::NotProven(format!("fault fixture descriptor is invalid: {error}"))
    })?;
    if !descriptor.descriptor.capabilities.session_reuse {
        return Err(CaseFailure::Failed(
            "1000-iteration fault fixture must support reusable sessions".to_owned(),
        ));
    }
    let seed = ProviderScenario::standard(&descriptor).map_err(|error| {
        CaseFailure::NotProven(format!("could not build fault scenario: {error}"))
    })?;
    let probes = TestProbes::default();
    let provider = factory.create(seed.clone(), probes.clone());
    let session_id = AgentSessionId::new("outcome-unknown-session");
    let mut expected_identities = BTreeMap::new();

    for fault_index in 0..1_000 {
        let request = request_for(
            &descriptor,
            session_id.clone(),
            RunId::new(format!("outcome-unknown-run-{fault_index:04}")),
        )?;
        match provider.start(request.clone()).await {
            Err(AgentStartError::OutcomeUnknown(_)) => {}
            Err(other) => {
                return Err(CaseFailure::Failed(format!(
                    "fault {fault_index} returned {other} instead of OutcomeUnknown"
                )))
            }
            Ok(_) => {
                return Err(CaseFailure::Failed(format!(
                    "fault {fault_index} did not lose the first native-start response"
                )))
            }
        }
        if probes.native_start_count_for(&request) != 1 {
            return Err(CaseFailure::Failed(format!(
                "fault {fault_index} created more than one native start before retry"
            )));
        }

        assert_fault_identity_conflicts(&provider, &descriptor, &request, &probes).await?;

        let recovered_start = provider.start(request.clone()).await.map_err(|error| {
            CaseFailure::Failed(format!(
                "same-identity retry {fault_index} did not reconcile: {error}"
            ))
        })?;
        recovered_start
            .execution
            .validate_for(&request, &descriptor)
            .map_err(|error| {
                CaseFailure::Failed(format!(
                    "same-identity retry {fault_index} returned crossed execution: {error}"
                ))
            })?;
        let expected_drafts = ProviderScenario::completion_events_for(&descriptor, &request);
        let observed_drafts =
            collect_provider_drafts(recovered_start.stream, "OutcomeUnknown retry stream").await?;
        if observed_drafts != expected_drafts {
            return Err(CaseFailure::Failed(format!(
                "same-identity retry {fault_index} returned crossed events or delivery"
            )));
        }

        provider.start(request.clone()).await.map_err(|error| {
            CaseFailure::Failed(format!(
                "settled same-identity replay {fault_index} failed: {error}"
            ))
        })?;
        if probes.native_start_count_for(&request) != 1 {
            return Err(CaseFailure::Failed(format!(
                "same immutable identity created duplicate native work at fault {fault_index}"
            )));
        }
        expected_identities.insert(
            (
                request.run.spec.run_id.clone(),
                request.run.spec_digest.clone(),
            ),
            1,
        );
    }

    if probes.native_start_count() != 1_000
        || probes.native_start_identities() != expected_identities
    {
        return Err(CaseFailure::Failed(
            "1000 lost responses did not preserve exactly one native start per immutable Run"
                .to_owned(),
        ));
    }
    Ok(())
}

async fn assert_fault_identity_conflicts(
    provider: &Arc<dyn AgentProvider>,
    descriptor: &AgentDescriptorEnvelope,
    request: &AgentStartRequest,
    probes: &TestProbes,
) -> Result<(), CaseFailure> {
    let mut different_spec = request.run.spec.clone();
    different_spec.input = vec![Content::text("different immutable fault input")];
    let conflicts = [
        AgentStartRequest {
            run: AgentRunEnvelope::seal(different_spec).map_err(|error| {
                CaseFailure::NotProven(format!("could not seal conflicting fault spec: {error}"))
            })?,
            provider_binding: request.provider_binding.clone(),
            expected_descriptor_digest: request.expected_descriptor_digest.clone(),
        },
        AgentStartRequest {
            run: request.run.clone(),
            provider_binding: ProviderBindingRef::new("different-fault-binding"),
            expected_descriptor_digest: request.expected_descriptor_digest.clone(),
        },
        AgentStartRequest {
            run: request.run.clone(),
            provider_binding: request.provider_binding.clone(),
            expected_descriptor_digest: Digest::sha256(b"different-fault-descriptor"),
        },
    ];
    for conflict in conflicts {
        match provider.start(conflict).await {
            Err(AgentStartError::Rejected(rejection))
                if rejection.code == AgentRejectionCode::RunIdConflict => {}
            Err(other) => {
                return Err(CaseFailure::Failed(format!(
                    "OutcomeUnknown accepted only exact retries, but conflict returned {other}"
                )))
            }
            Ok(_) => {
                return Err(CaseFailure::Failed(
                    "OutcomeUnknown reconciliation accepted a changed immutable identity"
                        .to_owned(),
                ))
            }
        }
    }
    if probes.native_start_count_for(request) != 1 {
        return Err(CaseFailure::Failed(
            "conflicting OutcomeUnknown retries changed native work count".to_owned(),
        ));
    }
    descriptor.validate_integrity().map_err(|error| {
        CaseFailure::NotProven(format!(
            "fault descriptor changed during assertion: {error}"
        ))
    })
}

fn request_for(
    descriptor: &AgentDescriptorEnvelope,
    session_id: AgentSessionId,
    run_id: RunId,
) -> Result<AgentStartRequest, CaseFailure> {
    let run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        session_id,
        run_id,
        vec![Content::text("complete the deterministic fixture")],
    )
    .map_err(|error| CaseFailure::NotProven(format!("could not build test Run: {error}")))?;
    AgentStartRequest::new(
        run,
        ProviderBindingRef::new("conformance-binding"),
        descriptor,
    )
    .map_err(|error| CaseFailure::NotProven(format!("could not build test start: {error}")))
}

async fn collect_provider_drafts(
    stream: orchestral_core::agent_protocol::spi::AgentProviderStream,
    label: &str,
) -> Result<Vec<AgentEventDraft>, CaseFailure> {
    let items = stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| CaseFailure::Failed(format!("{label} failed: {error}")))?;
    items
        .into_iter()
        .map(|item| match item {
            AgentProviderStreamItem::Event(draft) => {
                draft.validate_integrity().map_err(|error| {
                    CaseFailure::Failed(format!("{label} emitted an invalid draft: {error}"))
                })?;
                Ok(*draft)
            }
            AgentProviderStreamItem::Telemetry(_) => Err(CaseFailure::Failed(format!(
                "{label} emitted telemetry instead of its durable fixture events"
            ))),
            _ => Err(CaseFailure::Failed(format!(
                "{label} emitted an unsupported stream item"
            ))),
        })
        .collect()
}

fn immediate_completion_events(
    descriptor: &AgentDescriptorEnvelope,
    request: &AgentStartRequest,
) -> Vec<AgentEventDraft> {
    let run_id = request.run.spec.run_id.clone();
    let started_id = AgentEventId::new(format!("fixture-{}-started", run_id.as_str()));
    let delivered_id = AgentEventId::new(format!("fixture-{}-delivered", run_id.as_str()));
    vec![
        AgentEventDraft {
            event_id: started_id.clone(),
            run_id: run_id.clone(),
            causation_id: None,
            source_fingerprint: Some(Digest::sha256(format!(
                "fixture-started:{}:{}",
                run_id.as_str(),
                request.run.spec_digest.as_str()
            ))),
            payload: AgentEvent::RunStarted,
        },
        AgentEventDraft {
            event_id: delivered_id,
            run_id: run_id.clone(),
            causation_id: None,
            source_fingerprint: Some(Digest::sha256(format!(
                "fixture-delivered:{}:{}",
                run_id.as_str(),
                request.run.spec_digest.as_str()
            ))),
            payload: AgentEvent::DeliveryCommitted {
                delivery: AgentDelivery {
                    delivery_id: DeliveryId::new(format!("fixture-delivery-{}", run_id.as_str())),
                    run_id,
                    spec_digest: request.run.spec_digest.clone(),
                    final_response: Content::text(format!(
                        "fixture complete for {}",
                        request.run.spec.run_id.as_str()
                    )),
                    outputs: Vec::new(),
                    artifacts: Vec::new(),
                    unresolved_issues: Vec::new(),
                    usage: None,
                    provenance: Provenance {
                        provider_id: descriptor.descriptor.provider_id.clone(),
                        agent_id: descriptor.descriptor.agent_id.clone(),
                        supporting_event_ids: vec![started_id],
                        extensions: Default::default(),
                    },
                },
            },
        },
    ]
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::{CaseVerdict, ScriptedStatelessFactory};

    #[tokio::test]
    async fn conformant_scripted_provider_passes_first_slice() {
        let factory = ScriptedStatelessFactory::conformant().expect("valid fixture descriptor");
        let report = ConformanceSuite::run(&factory).await;

        assert!(report.is_conformant(), "{:#?}", report.cases());
    }

    #[tokio::test]
    async fn duplicate_work_fake_fails_exact_idempotency_case() {
        let factory = ScriptedStatelessFactory::duplicate_work().expect("valid fixture descriptor");
        let report = ConformanceSuite::run(&factory).await;

        assert!(!report.is_conformant());
        assert!(matches!(
            report.verdict(case_ids::SAME_RUN_START_IDEMPOTENT),
            Some(CaseVerdict::Failed { .. })
        ));
        assert_eq!(
            report.non_passing_case_ids().collect::<BTreeSet<_>>(),
            BTreeSet::from([case_ids::SAME_RUN_START_IDEMPOTENT])
        );
    }
}
