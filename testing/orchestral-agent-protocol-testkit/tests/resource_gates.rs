//! Quantified resource-admission gates over the public Agent Protocol API.
//!
//! The local oracle deliberately uses its own enums and allocation state. It
//! does not call the compatibility or admission implementation under test.

use std::collections::BTreeSet;

use orchestral_core::agent_protocol::{
    reference::{AgentRunReducer, AgentRunStatus, ApplyOutcome},
    wire::{
        AgentAdmission, AgentCapabilities, AgentDescriptor, AgentDescriptorEnvelope, AgentEvent,
        AgentEventDraft, AgentEventId, AgentExecutionRef, AgentId, AgentProtocolErrorCode,
        AgentProviderId, AgentRejectionCode, AgentRunEnvelope, AgentSessionId, AgentStartRequest,
        BindingRequirement, Content, ControlCapabilities, EffectMediation, Extensions,
        ProviderBindingRef, ResourceBinding, ResourceBindingId, ResourceBindingMode,
        ResourceBindingSkip, ResourceBindingSkipCode, ResourceCapability, ResourceId, ResourceKind,
        ResourceRef, ResourceRevision,
    },
    AGENT_PROTOCOL_V1,
};

const ALPHA_KIND: &str = "matrix.alpha/v1";
const BETA_KIND: &str = "matrix.beta/v1";
const UNSUPPORTED_KIND: &str = "matrix.unsupported/v1";
const BINDING_VARIANT_COUNT: usize = 2 * 3 * 2;
const RESOURCE_SHAPE_COUNT: usize =
    1 + BINDING_VARIANT_COUNT + BINDING_VARIANT_COUNT * BINDING_VARIANT_COUNT;
const STATIC_MATRIX_CASES: usize = 3 * 3 * RESOURCE_SHAPE_COUNT;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Requirement {
    Required,
    Optional,
}

impl Requirement {
    const ALL: [Self; 2] = [Self::Required, Self::Optional];

    const fn to_wire(self) -> BindingRequirement {
        match self {
            Self::Required => BindingRequirement::Required,
            Self::Optional => BindingRequirement::Optional,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Alpha,
    Beta,
    Unsupported,
}

impl Kind {
    const ALL: [Self; 3] = [Self::Alpha, Self::Beta, Self::Unsupported];

    const fn name(self) -> &'static str {
        match self {
            Self::Alpha => ALPHA_KIND,
            Self::Beta => BETA_KIND,
            Self::Unsupported => UNSUPPORTED_KIND,
        }
    }

    const fn capacity_index(self) -> Option<usize> {
        match self {
            Self::Alpha => Some(0),
            Self::Beta => Some(1),
            Self::Unsupported => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Snapshot,
    OnDemand,
}

impl Mode {
    const ALL: [Self; 2] = [Self::Snapshot, Self::OnDemand];

    const fn to_wire(self) -> ResourceBindingMode {
        match self {
            Self::Snapshot => ResourceBindingMode::Snapshot,
            Self::OnDemand => ResourceBindingMode::OnDemand,
        }
    }

    const fn wire_name(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::OnDemand => "on_demand",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Capacity {
    One,
    Two,
    Unbounded,
}

impl Capacity {
    const ALL: [Self; 3] = [Self::One, Self::Two, Self::Unbounded];

    const fn oracle_limit(self) -> usize {
        match self {
            Self::One => 1,
            Self::Two => 2,
            Self::Unbounded => usize::MAX,
        }
    }

    const fn wire_limit(self) -> Option<u32> {
        match self {
            Self::One => Some(1),
            Self::Two => Some(2),
            Self::Unbounded => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BindingCase {
    requirement: Requirement,
    kind: Kind,
    mode: Mode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SkipCause {
    UnsupportedKind,
    UnsupportedMode,
    CapacityExceeded,
    ResolutionFailed,
}

impl SkipCause {
    const fn wire_code(self) -> ResourceBindingSkipCode {
        match self {
            Self::UnsupportedKind => ResourceBindingSkipCode::UnsupportedKind,
            Self::UnsupportedMode => ResourceBindingSkipCode::UnsupportedMode,
            Self::CapacityExceeded => ResourceBindingSkipCode::CapacityExceeded,
            Self::ResolutionFailed => ResourceBindingSkipCode::ResolutionFailed,
        }
    }

    const fn wire_name(self) -> &'static str {
        match self {
            Self::UnsupportedKind => "unsupported_kind",
            Self::UnsupportedMode => "unsupported_mode",
            Self::CapacityExceeded => "capacity_exceeded",
            Self::ResolutionFailed => "resolution_failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OracleSkip {
    binding_index: usize,
    cause: SkipCause,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OracleCompatibility {
    static_skips: Vec<OracleSkip>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OracleRejection {
    binding_index: usize,
    cause: SkipCause,
}

#[derive(Debug, Default)]
struct Coverage {
    static_cases: usize,
    zero_binding_cases: usize,
    one_binding_cases: usize,
    two_binding_cases: usize,
    required_bindings: usize,
    optional_bindings: usize,
    supported_kind_bindings: usize,
    unsupported_kind_bindings: usize,
    supported_mode_bindings: usize,
    unsupported_mode_bindings: usize,
    compatible_cases: usize,
    rejected_cases: usize,
    unsupported_kind_rejections: usize,
    unsupported_mode_rejections: usize,
    capacity_rejections: usize,
    unsupported_kind_static_skips: usize,
    unsupported_mode_static_skips: usize,
    capacity_static_skips: usize,
    admission_scenarios: usize,
    resolution_failure_admissions: usize,
    exact_starts_applied: usize,
    missing_skip_start_attempts: usize,
    missing_skip_starts_applied: usize,
    required_skip_attempts: usize,
    required_skips_applied: usize,
    wrong_skip_attempts: usize,
    wrong_skips_applied: usize,
    invalid_admission_attempts: usize,
    invalid_admissions_accepted: usize,
}

impl Coverage {
    fn observe_shape(&mut self, bindings: &[BindingCase]) {
        self.static_cases += 1;
        match bindings.len() {
            0 => self.zero_binding_cases += 1,
            1 => self.one_binding_cases += 1,
            2 => self.two_binding_cases += 1,
            count => panic!("matrix exceeded its two-binding bound: {count}"),
        }
        for binding in bindings {
            match binding.requirement {
                Requirement::Required => self.required_bindings += 1,
                Requirement::Optional => self.optional_bindings += 1,
            }
            match binding.kind {
                Kind::Alpha | Kind::Beta => self.supported_kind_bindings += 1,
                Kind::Unsupported => self.unsupported_kind_bindings += 1,
            }
            match binding.mode {
                Mode::Snapshot => self.supported_mode_bindings += 1,
                Mode::OnDemand => self.unsupported_mode_bindings += 1,
            }
        }
    }

    fn observe_rejection(&mut self, cause: SkipCause) {
        self.rejected_cases += 1;
        match cause {
            SkipCause::UnsupportedKind => self.unsupported_kind_rejections += 1,
            SkipCause::UnsupportedMode => self.unsupported_mode_rejections += 1,
            SkipCause::CapacityExceeded => self.capacity_rejections += 1,
            SkipCause::ResolutionFailed => {
                panic!("static compatibility cannot reject a resolution failure")
            }
        }
    }

    fn observe_static_skip(&mut self, cause: SkipCause) {
        match cause {
            SkipCause::UnsupportedKind => self.unsupported_kind_static_skips += 1,
            SkipCause::UnsupportedMode => self.unsupported_mode_static_skips += 1,
            SkipCause::CapacityExceeded => self.capacity_static_skips += 1,
            SkipCause::ResolutionFailed => {
                panic!("static compatibility cannot produce a resolution failure")
            }
        }
    }
}

fn binding_variants() -> Vec<BindingCase> {
    let mut variants = Vec::with_capacity(BINDING_VARIANT_COUNT);
    for requirement in Requirement::ALL {
        for kind in Kind::ALL {
            for mode in Mode::ALL {
                variants.push(BindingCase {
                    requirement,
                    kind,
                    mode,
                });
            }
        }
    }
    variants
}

fn resource_shapes(variants: &[BindingCase]) -> Vec<Vec<BindingCase>> {
    let mut shapes = vec![Vec::new()];
    shapes.extend(variants.iter().copied().map(|binding| vec![binding]));
    for first in variants {
        for second in variants {
            shapes.push(vec![*first, *second]);
        }
    }
    shapes
}

/// Literal admission oracle: required bindings reserve capacity before any
/// optional binding, then optional bindings are either admitted or skipped in
/// run order.
fn oracle_compatibility(
    bindings: &[BindingCase],
    capacities: [Capacity; 2],
) -> Result<OracleCompatibility, OracleRejection> {
    let mut admitted_per_kind = [0_usize; 2];

    for (binding_index, binding) in bindings.iter().enumerate() {
        if binding.requirement != Requirement::Required {
            continue;
        }
        let Some(kind_index) = binding.kind.capacity_index() else {
            return Err(OracleRejection {
                binding_index,
                cause: SkipCause::UnsupportedKind,
            });
        };
        if binding.mode != Mode::Snapshot {
            return Err(OracleRejection {
                binding_index,
                cause: SkipCause::UnsupportedMode,
            });
        }
        if admitted_per_kind[kind_index] >= capacities[kind_index].oracle_limit() {
            return Err(OracleRejection {
                binding_index,
                cause: SkipCause::CapacityExceeded,
            });
        }
        admitted_per_kind[kind_index] += 1;
    }

    let mut static_skips = Vec::new();
    for (binding_index, binding) in bindings.iter().enumerate() {
        if binding.requirement != Requirement::Optional {
            continue;
        }
        let cause = match binding.kind.capacity_index() {
            None => Some(SkipCause::UnsupportedKind),
            Some(_) if binding.mode != Mode::Snapshot => Some(SkipCause::UnsupportedMode),
            Some(kind_index)
                if admitted_per_kind[kind_index] >= capacities[kind_index].oracle_limit() =>
            {
                Some(SkipCause::CapacityExceeded)
            }
            Some(kind_index) => {
                admitted_per_kind[kind_index] += 1;
                None
            }
        };
        if let Some(cause) = cause {
            static_skips.push(OracleSkip {
                binding_index,
                cause,
            });
        }
    }

    Ok(OracleCompatibility { static_skips })
}

fn descriptor(capacities: [Capacity; 2]) -> AgentDescriptorEnvelope {
    AgentDescriptorEnvelope::seal(AgentDescriptor {
        provider_id: AgentProviderId::new("testkit.resource-matrix"),
        agent_id: AgentId::new("resource-matrix-agent-v1"),
        supported_protocol_versions: vec![AGENT_PROTOCOL_V1],
        accepted_content_types: BTreeSet::from(["text/plain".to_owned()]),
        capabilities: AgentCapabilities {
            controls: ControlCapabilities::default(),
            resources: vec![
                ResourceCapability {
                    kind: ResourceKind::new(ALPHA_KIND),
                    modes: BTreeSet::from([ResourceBindingMode::Snapshot]),
                    max_bindings: capacities[0].wire_limit(),
                },
                ResourceCapability {
                    kind: ResourceKind::new(BETA_KIND),
                    modes: BTreeSet::from([ResourceBindingMode::Snapshot]),
                    max_bindings: capacities[1].wire_limit(),
                },
            ],
            effect_mediation: EffectMediation::HostMediated,
            ..AgentCapabilities::default()
        },
        extensions: Extensions::new(),
    })
    .expect("resource matrix descriptor must seal")
}

fn run(case_index: usize, bindings: &[BindingCase]) -> AgentRunEnvelope {
    let mut run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        AgentSessionId::new(format!("resource-session-{case_index}")),
        orchestral_core::agent_protocol::wire::RunId::new(format!("resource-run-{case_index}")),
        vec![Content::text("exercise deterministic resource admission")],
    )
    .expect("resource matrix run must initially seal");
    run.spec.resources = bindings
        .iter()
        .enumerate()
        .map(|(binding_index, binding)| ResourceBinding {
            binding_id: binding_id(binding_index),
            resource: ResourceRef {
                kind: ResourceKind::new(binding.kind.name()),
                id: ResourceId::new(format!("resource-{case_index}-{binding_index}")),
                revision: ResourceRevision::new(format!("revision-{case_index}-{binding_index}")),
            },
            requirement: binding.requirement.to_wire(),
            mode: binding.mode.to_wire(),
        })
        .collect();
    AgentRunEnvelope::seal(run.spec).expect("resource matrix run with bindings must seal")
}

fn binding_id(binding_index: usize) -> ResourceBindingId {
    ResourceBindingId::new(format!("binding-{binding_index}"))
}

fn start_request(
    case_index: usize,
    run: AgentRunEnvelope,
    descriptor: &AgentDescriptorEnvelope,
) -> AgentStartRequest {
    AgentStartRequest::new(
        run,
        ProviderBindingRef::new(format!("resource-provider-binding-{case_index}")),
        descriptor,
    )
    .expect("resource start request must bind to its descriptor")
}

fn execution(
    request: &AgentStartRequest,
    descriptor: &AgentDescriptorEnvelope,
) -> AgentExecutionRef {
    AgentExecutionRef::for_start(request, descriptor)
        .expect("resource execution must bind to its start request")
}

fn draft(
    request: &AgentStartRequest,
    event_id: impl Into<String>,
    payload: AgentEvent,
) -> AgentEventDraft {
    AgentEventDraft {
        event_id: AgentEventId::new(event_id),
        run_id: request.run.spec.run_id.clone(),
        causation_id: None,
        source_fingerprint: None,
        payload,
    }
}

fn accept_run(reducer: &mut AgentRunReducer, request: &AgentStartRequest, event_suffix: &str) {
    let accepted = reducer
        .apply_host_draft(draft(
            request,
            format!("host-run-accepted-{event_suffix}"),
            AgentEvent::RunAccepted {
                session_id: request.run.spec.session_id.clone(),
                spec_digest: request.run.spec_digest.clone(),
            },
        ))
        .expect("RunAccepted must pass the structural gate");
    assert!(matches!(accepted.outcome, ApplyOutcome::Applied));
    assert_eq!(reducer.state().status(), AgentRunStatus::Accepted);
}

fn skip_cause_from_wire(code: ResourceBindingSkipCode) -> SkipCause {
    match code {
        ResourceBindingSkipCode::UnsupportedKind => SkipCause::UnsupportedKind,
        ResourceBindingSkipCode::UnsupportedMode => SkipCause::UnsupportedMode,
        ResourceBindingSkipCode::CapacityExceeded => SkipCause::CapacityExceeded,
        ResourceBindingSkipCode::ResolutionFailed => SkipCause::ResolutionFailed,
        _ => panic!("matrix observed an unknown resource skip code"),
    }
}

fn oracle_skip_to_wire(skip: OracleSkip) -> ResourceBindingSkip {
    ResourceBindingSkip {
        binding_id: binding_id(skip.binding_index),
        code: skip.cause.wire_code(),
        reason: format!("oracle admission: {}", skip.cause.wire_name()),
    }
}

fn base_admission(oracle: &OracleCompatibility) -> AgentAdmission {
    AgentAdmission {
        skipped_optional_bindings: oracle
            .static_skips
            .iter()
            .copied()
            .map(oracle_skip_to_wire)
            .collect(),
    }
}

fn first_statically_admitted_optional(
    bindings: &[BindingCase],
    oracle: &OracleCompatibility,
) -> Option<usize> {
    bindings.iter().enumerate().find_map(|(index, binding)| {
        (binding.requirement == Requirement::Optional
            && !oracle
                .static_skips
                .iter()
                .any(|skip| skip.binding_index == index))
        .then_some(index)
    })
}

fn resolution_failure_admission(
    oracle: &OracleCompatibility,
    failed_binding_index: usize,
) -> AgentAdmission {
    let mut admission = base_admission(oracle);
    admission
        .skipped_optional_bindings
        .push(oracle_skip_to_wire(OracleSkip {
            binding_index: failed_binding_index,
            cause: SkipCause::ResolutionFailed,
        }));
    admission
        .skipped_optional_bindings
        .sort_by(|left, right| left.binding_id.cmp(&right.binding_id));
    admission
}

fn assert_static_compatibility_matches_oracle(
    case_index: usize,
    actual: &[ResourceBindingSkip],
    oracle: &OracleCompatibility,
) {
    let actual_signature = actual
        .iter()
        .map(|skip| {
            assert!(
                !skip.reason.trim().is_empty(),
                "case {case_index} emitted an empty static skip reason"
            );
            (skip.binding_id.clone(), skip_cause_from_wire(skip.code))
        })
        .collect::<Vec<_>>();
    let expected_signature = oracle
        .static_skips
        .iter()
        .map(|skip| (binding_id(skip.binding_index), skip.cause))
        .collect::<Vec<_>>();
    assert_eq!(
        actual_signature, expected_signature,
        "static compatibility diverged from the independent oracle in case {case_index}"
    );
}

fn new_reducer(
    request: &AgentStartRequest,
    descriptor: &AgentDescriptorEnvelope,
    admission: AgentAdmission,
) -> Result<AgentRunReducer, orchestral_core::agent_protocol::wire::AgentProtocolError> {
    AgentRunReducer::new(
        execution(request, descriptor),
        request,
        descriptor,
        admission,
    )
}

fn assert_invalid_admission(
    coverage: &mut Coverage,
    request: &AgentStartRequest,
    descriptor: &AgentDescriptorEnvelope,
    compatibility: &orchestral_core::agent_protocol::spi::AgentCompatibility,
    admission: AgentAdmission,
) {
    coverage.invalid_admission_attempts += 1;
    let validation = admission.validate_against(&request.run, compatibility);
    if validation.is_ok() {
        coverage.invalid_admissions_accepted += 1;
    }
    assert_eq!(
        validation
            .expect_err("invalid admission unexpectedly passed direct validation")
            .code,
        AgentProtocolErrorCode::InvalidSpec
    );
    let initialization = new_reducer(request, descriptor, admission);
    if initialization.is_ok() {
        coverage.invalid_admissions_accepted += 1;
    }
    assert_eq!(
        initialization
            .expect_err("invalid admission unexpectedly initialized a projection")
            .code,
        AgentProtocolErrorCode::InvalidSpec
    );
}

fn assert_protocol_violation(
    outcome: ApplyOutcome,
    normal_projection_accepts: &mut usize,
    context: &str,
) {
    match outcome {
        ApplyOutcome::Applied => {
            *normal_projection_accepts += 1;
            panic!("{context} entered the normal projection")
        }
        ApplyOutcome::ProtocolViolation { error } => {
            assert_eq!(error.code, AgentProtocolErrorCode::InvalidTransition);
        }
        ApplyOutcome::ExactDuplicate => panic!("{context} was treated as an exact duplicate"),
        _ => panic!("{context} returned an unknown apply outcome"),
    }
}

fn exercise_valid_admission(
    coverage: &mut Coverage,
    case_index: usize,
    request: &AgentStartRequest,
    descriptor: &AgentDescriptorEnvelope,
    compatibility: &orchestral_core::agent_protocol::spi::AgentCompatibility,
    admission: AgentAdmission,
) {
    admission
        .validate_against(&request.run, compatibility)
        .expect("oracle admission must satisfy static compatibility");
    coverage.admission_scenarios += 1;
    if admission
        .skipped_optional_bindings
        .iter()
        .any(|skip| skip.code == ResourceBindingSkipCode::ResolutionFailed)
    {
        coverage.resolution_failure_admissions += 1;
    }

    if !admission.skipped_optional_bindings.is_empty() {
        coverage.missing_skip_start_attempts += 1;
        let mut incomplete = new_reducer(request, descriptor, admission.clone())
            .expect("valid admission must initialize an early-start projection");
        accept_run(&mut incomplete, request, &format!("early-{case_index}"));
        for (skip_index, skip) in admission
            .skipped_optional_bindings
            .iter()
            .take(admission.skipped_optional_bindings.len() - 1)
            .enumerate()
        {
            let applied = incomplete
                .apply_host_draft(draft(
                    request,
                    format!("host-partial-skip-{case_index}-{skip_index}"),
                    AgentEvent::ResourceBindingSkipped { skip: skip.clone() },
                ))
                .expect("a declared partial skip must pass the structural gate");
            assert!(matches!(applied.outcome, ApplyOutcome::Applied));
        }
        let started = incomplete
            .apply_provider_draft(draft(
                request,
                format!("provider-early-start-{case_index}"),
                AgentEvent::RunStarted,
            ))
            .expect("a missing skip is a quarantined semantic violation");
        assert_protocol_violation(
            started.outcome,
            &mut coverage.missing_skip_starts_applied,
            "RunStarted with an incomplete skip set",
        );
        assert_eq!(incomplete.state().status(), AgentRunStatus::Unknown);
    }

    coverage.wrong_skip_attempts += 1;
    let mut wrong_skip_projection = new_reducer(request, descriptor, admission.clone())
        .expect("valid admission must initialize a wrong-skip projection");
    accept_run(
        &mut wrong_skip_projection,
        request,
        &format!("wrong-skip-{case_index}"),
    );
    let wrong_skip = admission
        .skipped_optional_bindings
        .first()
        .cloned()
        .map_or_else(
            || ResourceBindingSkip {
                binding_id: request.run.spec.resources.first().map_or_else(
                    || ResourceBindingId::new("absent-binding"),
                    |binding| binding.binding_id.clone(),
                ),
                code: ResourceBindingSkipCode::ResolutionFailed,
                reason: "undeclared skip".to_owned(),
            },
            |mut skip| {
                skip.reason.push_str(" (mutated)");
                skip
            },
        );
    let wrong = wrong_skip_projection
        .apply_host_draft(draft(
            request,
            format!("host-wrong-skip-{case_index}"),
            AgentEvent::ResourceBindingSkipped { skip: wrong_skip },
        ))
        .expect("a wrong skip is a quarantined semantic violation");
    assert_protocol_violation(
        wrong.outcome,
        &mut coverage.wrong_skips_applied,
        "wrong resource skip",
    );
    assert_eq!(
        wrong_skip_projection.state().status(),
        AgentRunStatus::Unknown
    );

    let mut exact = new_reducer(request, descriptor, admission.clone())
        .expect("valid admission must initialize an exact-start projection");
    accept_run(&mut exact, request, &format!("exact-{case_index}"));
    for (skip_index, skip) in admission.skipped_optional_bindings.iter().rev().enumerate() {
        let skipped = exact
            .apply_host_draft(draft(
                request,
                format!("host-exact-skip-{case_index}-{skip_index}"),
                AgentEvent::ResourceBindingSkipped { skip: skip.clone() },
            ))
            .expect("an exact declared skip must pass the structural gate");
        assert!(matches!(skipped.outcome, ApplyOutcome::Applied));
        assert_eq!(exact.state().status(), AgentRunStatus::Accepted);
    }
    let started = exact
        .apply_provider_draft(draft(
            request,
            format!("provider-exact-start-{case_index}"),
            AgentEvent::RunStarted,
        ))
        .expect("RunStarted with the exact skip set must pass the structural gate");
    assert!(matches!(started.outcome, ApplyOutcome::Applied));
    assert_eq!(exact.state().status(), AgentRunStatus::Running);
    assert_eq!(exact.view().admission, admission);
    assert_eq!(
        exact.view().last_run_seq,
        Some(2 + exact.view().admission.skipped_optional_bindings.len() as u64)
    );
    coverage.exact_starts_applied += 1;
}

fn exercise_compatible_case(
    coverage: &mut Coverage,
    case_index: usize,
    bindings: &[BindingCase],
    descriptor: &AgentDescriptorEnvelope,
    request: &AgentStartRequest,
    compatibility: &orchestral_core::agent_protocol::spi::AgentCompatibility,
    oracle: &OracleCompatibility,
) {
    coverage.compatible_cases += 1;
    assert_static_compatibility_matches_oracle(
        case_index,
        &compatibility.skipped_optional_bindings,
        oracle,
    );
    for skip in &oracle.static_skips {
        coverage.observe_static_skip(skip.cause);
    }

    let admission = base_admission(oracle);

    for (binding_index, binding) in bindings.iter().enumerate() {
        if binding.requirement != Requirement::Required {
            continue;
        }

        let mut invalid = admission.clone();
        invalid
            .skipped_optional_bindings
            .push(oracle_skip_to_wire(OracleSkip {
                binding_index,
                cause: SkipCause::ResolutionFailed,
            }));
        assert_invalid_admission(coverage, request, descriptor, compatibility, invalid);

        coverage.required_skip_attempts += 1;
        let mut reducer = new_reducer(request, descriptor, admission.clone())
            .expect("base admission must initialize a required-skip projection");
        accept_run(
            &mut reducer,
            request,
            &format!("required-skip-{case_index}-{binding_index}"),
        );
        let skipped = reducer
            .apply_host_draft(draft(
                request,
                format!("host-required-skip-{case_index}-{binding_index}"),
                AgentEvent::ResourceBindingSkipped {
                    skip: oracle_skip_to_wire(OracleSkip {
                        binding_index,
                        cause: SkipCause::ResolutionFailed,
                    }),
                },
            ))
            .expect("a required skip is a quarantined semantic violation");
        assert_protocol_violation(
            skipped.outcome,
            &mut coverage.required_skips_applied,
            "required resource skip",
        );
        assert_eq!(reducer.state().status(), AgentRunStatus::Unknown);
    }

    if !oracle.static_skips.is_empty() {
        let mut omitted = admission.clone();
        omitted.skipped_optional_bindings.remove(0);
        assert_invalid_admission(coverage, request, descriptor, compatibility, omitted);

        let mut wrong_code = admission.clone();
        wrong_code.skipped_optional_bindings[0].code = ResourceBindingSkipCode::ResolutionFailed;
        assert_invalid_admission(coverage, request, descriptor, compatibility, wrong_code);
    }

    let admitted_optional = first_statically_admitted_optional(bindings, oracle);
    if let Some(binding_index) = admitted_optional {
        let mut false_static_skip = admission.clone();
        false_static_skip
            .skipped_optional_bindings
            .push(oracle_skip_to_wire(OracleSkip {
                binding_index,
                cause: SkipCause::CapacityExceeded,
            }));
        assert_invalid_admission(
            coverage,
            request,
            descriptor,
            compatibility,
            false_static_skip,
        );
    }

    exercise_valid_admission(
        coverage,
        case_index,
        request,
        descriptor,
        compatibility,
        admission,
    );
    if let Some(binding_index) = admitted_optional {
        exercise_valid_admission(
            coverage,
            case_index + STATIC_MATRIX_CASES,
            request,
            descriptor,
            compatibility,
            resolution_failure_admission(oracle, binding_index),
        );
    }
}

#[test]
fn deterministic_resource_matrix_enforces_static_admission_and_exact_start_gates() {
    let variants = binding_variants();
    let shapes = resource_shapes(&variants);
    assert_eq!(variants.len(), BINDING_VARIANT_COUNT);
    assert_eq!(shapes.len(), RESOURCE_SHAPE_COUNT);

    let mut coverage = Coverage::default();
    for (alpha_index, alpha_capacity) in Capacity::ALL.into_iter().enumerate() {
        for (beta_index, beta_capacity) in Capacity::ALL.into_iter().enumerate() {
            let capacities = [alpha_capacity, beta_capacity];
            let descriptor = descriptor(capacities);
            for (shape_index, bindings) in shapes.iter().enumerate() {
                let case_index =
                    (alpha_index * Capacity::ALL.len() + beta_index) * shapes.len() + shape_index;
                coverage.observe_shape(bindings);
                let run = run(case_index, bindings);
                let request = start_request(case_index, run, &descriptor);
                let expected = oracle_compatibility(bindings, capacities);
                let actual = descriptor.descriptor.check_run_compatibility(&request.run);

                match (expected, actual) {
                    (Ok(oracle), Ok(compatibility)) => exercise_compatible_case(
                        &mut coverage,
                        case_index,
                        bindings,
                        &descriptor,
                        &request,
                        &compatibility,
                        &oracle,
                    ),
                    (Err(expected), Err(actual)) => {
                        coverage.observe_rejection(expected.cause);
                        assert_eq!(
                            actual.code,
                            AgentRejectionCode::UnsupportedResource,
                            "case {case_index} returned the wrong rejection class"
                        );
                        assert_eq!(
                            actual.details["binding_id"],
                            binding_id(expected.binding_index).as_str(),
                            "case {case_index} rejected the wrong binding"
                        );
                        assert_eq!(
                            actual.details["resource_kind"],
                            bindings[expected.binding_index].kind.name(),
                            "case {case_index} reported the wrong resource kind"
                        );
                        assert_eq!(
                            actual.details["binding_mode"],
                            bindings[expected.binding_index].mode.wire_name(),
                            "case {case_index} reported the wrong resource mode"
                        );
                        assert_eq!(
                            actual.details["code"],
                            expected.cause.wire_name(),
                            "case {case_index} reported the wrong resource rejection cause"
                        );
                    }
                    (expected, actual) => panic!(
                        "case {case_index} diverged: oracle={expected:?}, implementation={actual:?}"
                    ),
                }
            }
        }
    }

    assert_eq!(coverage.static_cases, STATIC_MATRIX_CASES);
    assert_eq!(coverage.zero_binding_cases, 9);
    assert_eq!(coverage.one_binding_cases, 108);
    assert_eq!(coverage.two_binding_cases, 1_296);
    assert_eq!(coverage.required_bindings, 1_350);
    assert_eq!(coverage.optional_bindings, 1_350);
    assert_eq!(coverage.supported_kind_bindings, 1_800);
    assert_eq!(coverage.unsupported_kind_bindings, 900);
    assert_eq!(coverage.supported_mode_bindings, 1_350);
    assert_eq!(coverage.unsupported_mode_bindings, 1_350);
    assert_eq!(coverage.compatible_cases, 651);
    assert_eq!(coverage.rejected_cases, 762);
    assert_eq!(coverage.unsupported_kind_rejections, 378);
    assert_eq!(coverage.unsupported_mode_rejections, 378);
    assert_eq!(coverage.capacity_rejections, 6);
    assert_eq!(coverage.unsupported_kind_static_skips, 306);
    assert_eq!(coverage.unsupported_mode_static_skips, 306);
    assert_eq!(coverage.capacity_static_skips, 18);
    assert_eq!(coverage.admission_scenarios, 909);
    assert_eq!(coverage.resolution_failure_admissions, 258);
    assert_eq!(coverage.exact_starts_applied, 909);
    assert_eq!(coverage.missing_skip_start_attempts, 744);
    assert_eq!(coverage.missing_skip_starts_applied, 0);
    assert_eq!(coverage.required_skip_attempts, 294);
    assert_eq!(coverage.required_skips_applied, 0);
    assert_eq!(coverage.wrong_skip_attempts, 909);
    assert_eq!(coverage.wrong_skips_applied, 0);
    assert_eq!(coverage.invalid_admission_attempts, 1_524);
    assert_eq!(coverage.invalid_admissions_accepted, 0);

    eprintln!(
        "resource gates: {} static cases (0/1/2 bindings = {}/{}/{}), {} valid admission projections, {} exact starts; required/wrong/missing-skip normal accepts = {}/{}/{}",
        coverage.static_cases,
        coverage.zero_binding_cases,
        coverage.one_binding_cases,
        coverage.two_binding_cases,
        coverage.admission_scenarios,
        coverage.exact_starts_applied,
        coverage.required_skips_applied,
        coverage.wrong_skips_applied,
        coverage.missing_skip_starts_applied,
    );
}
