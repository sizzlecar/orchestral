//! Delivery and artifact binding gates exercised only through the public API.

use std::collections::BTreeSet;

use orchestral_core::agent_protocol::{
    reference::{AgentRunReducer, AgentRunStatus, ApplyOutcome, SequencedApply},
    wire::{
        AgentAdmission, AgentCapabilities, AgentCommand, AgentCommandEnvelope, AgentDelivery,
        AgentDescriptor, AgentDescriptorEnvelope, AgentEvent, AgentEventDraft, AgentEventId,
        AgentExecutionRef, AgentFailure, AgentId, AgentProtocolError, AgentProviderId,
        AgentRunEnvelope, AgentRunView, AgentSessionId, AgentStartRequest, ArtifactRef,
        ArtifactRefWithDigest, CancelSupport, CommandId, Content, ContentBody, ControlCapabilities,
        DeliveryId, Digest, IncompleteReason, OutputId, PartialDelivery, PartialDeliveryId,
        Provenance, ProviderBindingRef, ProviderCommandOutcome, RunId, RunLimitKind, SchemaRef,
    },
    AGENT_PROTOCOL_V1,
};

const MUTATIONS_PER_FAMILY: usize = 100;
const MUTATION_FAMILY_COUNT: usize = 10;
const EXACT_MUTATION_COUNT: usize = MUTATIONS_PER_FAMILY * MUTATION_FAMILY_COUNT;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RejectionMode {
    Structural,
    Quarantined,
}

#[derive(Debug, Clone, Copy)]
enum DeliveryMutation {
    RunRef,
    SpecDigest,
    ProviderRef,
    AgentRef,
    SchemaRef,
    ArtifactRef,
    ArtifactDigest,
    DanglingSupportingEvent,
    SelfSupportingEvent,
    DuplicateSupportingEvent,
}

impl DeliveryMutation {
    const ALL: [Self; MUTATION_FAMILY_COUNT] = [
        Self::RunRef,
        Self::SpecDigest,
        Self::ProviderRef,
        Self::AgentRef,
        Self::SchemaRef,
        Self::ArtifactRef,
        Self::ArtifactDigest,
        Self::DanglingSupportingEvent,
        Self::SelfSupportingEvent,
        Self::DuplicateSupportingEvent,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::RunRef => "run_ref",
            Self::SpecDigest => "spec_digest",
            Self::ProviderRef => "provider_ref",
            Self::AgentRef => "agent_ref",
            Self::SchemaRef => "schema_ref",
            Self::ArtifactRef => "artifact_ref",
            Self::ArtifactDigest => "artifact_digest",
            Self::DanglingSupportingEvent => "supporting_event_dangling",
            Self::SelfSupportingEvent => "supporting_event_self",
            Self::DuplicateSupportingEvent => "supporting_event_duplicate",
        }
    }

    const fn expected_rejection(self) -> RejectionMode {
        match self {
            Self::ArtifactRef | Self::ArtifactDigest | Self::DuplicateSupportingEvent => {
                RejectionMode::Structural
            }
            Self::RunRef
            | Self::SpecDigest
            | Self::ProviderRef
            | Self::AgentRef
            | Self::SchemaRef
            | Self::DanglingSupportingEvent
            | Self::SelfSupportingEvent => RejectionMode::Quarantined,
        }
    }

    fn apply(self, delivery: &mut AgentDelivery, delivery_event_id: &AgentEventId, case: usize) {
        match self {
            Self::RunRef => {
                delivery.run_id = RunId::new(format!("foreign-run-{case}"));
            }
            Self::SpecDigest => {
                delivery.spec_digest = Digest::sha256(format!("foreign-spec-{case}"));
            }
            Self::ProviderRef => {
                delivery.provenance.provider_id =
                    AgentProviderId::new(format!("foreign-provider-{case}"));
            }
            Self::AgentRef => {
                delivery.provenance.agent_id = AgentId::new(format!("foreign-agent-{case}"));
            }
            Self::SchemaRef => {
                delivery.final_response.schema_id =
                    Some(SchemaRef::new(format!("foreign-schema-{case}")));
            }
            Self::ArtifactRef => {
                delivery.artifacts[0].artifact_ref = ArtifactRef::new("");
            }
            Self::ArtifactDigest => {
                delivery.artifacts[0].digest = Digest::new(format!("not-a-sha256-{case}"));
            }
            Self::DanglingSupportingEvent => {
                delivery.provenance.supporting_event_ids =
                    vec![AgentEventId::new(format!("never-committed-{case}"))];
            }
            Self::SelfSupportingEvent => {
                delivery.provenance.supporting_event_ids = vec![delivery_event_id.clone()];
            }
            Self::DuplicateSupportingEvent => {
                let supporting_event_id = delivery.provenance.supporting_event_ids[0].clone();
                delivery.provenance.supporting_event_ids =
                    vec![supporting_event_id.clone(), supporting_event_id];
            }
        }
    }
}

struct Fixture {
    run: AgentRunEnvelope,
    reducer: AgentRunReducer,
    output_schema: SchemaRef,
    supporting_event_id: AgentEventId,
    event_index: usize,
}

impl Fixture {
    fn running(case: usize) -> Self {
        let output_schema = SchemaRef::new(format!("delivery-schema-{case}"));
        let descriptor = AgentDescriptorEnvelope::seal(AgentDescriptor {
            provider_id: AgentProviderId::new("testkit.delivery"),
            agent_id: AgentId::new("delivery-agent-v1"),
            supported_protocol_versions: vec![AGENT_PROTOCOL_V1],
            accepted_content_types: BTreeSet::from(["text/plain".to_owned()]),
            capabilities: AgentCapabilities {
                structured_output: true,
                controls: ControlCapabilities {
                    cancel: CancelSupport::Confirmed,
                    ..ControlCapabilities::default()
                },
                ..AgentCapabilities::default()
            },
            extensions: Default::default(),
        })
        .expect("fixture descriptor seals");

        let mut run = AgentRunEnvelope::new(
            AGENT_PROTOCOL_V1,
            AgentSessionId::new(format!("delivery-session-{case}")),
            RunId::new(format!("delivery-run-{case}")),
            vec![Content::text("exercise delivery binding gates")],
        )
        .expect("fixture run starts valid");
        run.spec.output_schema = Some(output_schema.clone());
        let run = AgentRunEnvelope::seal(run.spec).expect("structured fixture run seals");
        let request = AgentStartRequest::new(
            run.clone(),
            ProviderBindingRef::new("testkit-delivery-binding"),
            &descriptor,
        )
        .expect("start request binds descriptor");
        let execution =
            AgentExecutionRef::for_start(&request, &descriptor).expect("execution binds start");
        let reducer =
            AgentRunReducer::new(execution, &request, &descriptor, AgentAdmission::default())
                .expect("reference reducer initializes");
        let mut fixture = Self {
            run,
            reducer,
            output_schema,
            supporting_event_id: AgentEventId::new("not-yet-committed"),
            event_index: 0,
        };

        let accepted = fixture.host_applied(
            AgentEvent::RunAccepted {
                session_id: fixture.run.spec.session_id.clone(),
                spec_digest: fixture.run.spec_digest.clone(),
            },
            None,
        );
        assert!(matches!(accepted.outcome, ApplyOutcome::Applied));
        let started = fixture.provider_applied(AgentEvent::RunStarted, None);
        assert!(matches!(started.outcome, ApplyOutcome::Applied));
        let supporting = fixture.provider_applied(
            AgentEvent::OutputCommitted {
                output_id: OutputId::new(format!("supporting-output-{case}")),
                content: vec![Content::text("durable supporting evidence")],
            },
            None,
        );
        fixture.supporting_event_id = supporting.event().event_id.clone();
        assert_eq!(fixture.reducer.state().status(), AgentRunStatus::Running);
        fixture
    }

    fn reserve_event_id(&mut self) -> AgentEventId {
        self.event_index += 1;
        AgentEventId::new(format!(
            "{}-event-{}",
            self.run.spec.run_id.as_str(),
            self.event_index
        ))
    }

    fn draft_with_id(
        &self,
        event_id: AgentEventId,
        payload: AgentEvent,
        causation_id: Option<CommandId>,
    ) -> AgentEventDraft {
        AgentEventDraft {
            event_id,
            run_id: self.run.spec.run_id.clone(),
            causation_id,
            source_fingerprint: None,
            payload,
        }
    }

    fn host_applied(
        &mut self,
        payload: AgentEvent,
        causation_id: Option<CommandId>,
    ) -> SequencedApply {
        let event_id = self.reserve_event_id();
        let draft = self.draft_with_id(event_id, payload, causation_id);
        self.reducer
            .apply_host_draft(draft)
            .expect("Host fixture event sequences")
    }

    fn provider_applied(
        &mut self,
        payload: AgentEvent,
        causation_id: Option<CommandId>,
    ) -> SequencedApply {
        let event_id = self.reserve_event_id();
        let draft = self.draft_with_id(event_id, payload, causation_id);
        self.reducer
            .apply_provider_draft(draft)
            .expect("Provider fixture event sequences")
    }

    fn delivery(&self, case: usize) -> AgentDelivery {
        AgentDelivery {
            delivery_id: DeliveryId::new(format!("complete-delivery-{case}")),
            run_id: self.run.spec.run_id.clone(),
            spec_digest: self.run.spec_digest.clone(),
            final_response: structured_content(&self.output_schema, case),
            outputs: Vec::new(),
            artifacts: vec![artifact_metadata(case)],
            unresolved_issues: Vec::new(),
            usage: None,
            provenance: self.provenance(),
        }
    }

    fn partial_delivery(&self, case: usize) -> PartialDelivery {
        PartialDelivery {
            partial_delivery_id: PartialDeliveryId::new(format!("partial-delivery-{case}")),
            run_id: self.run.spec.run_id.clone(),
            spec_digest: self.run.spec_digest.clone(),
            response: Some(structured_content(&self.output_schema, case)),
            outputs: Vec::new(),
            artifacts: vec![artifact_metadata(case)],
            unresolved_issues: vec!["model-step limit prevented completion".to_owned()],
            usage: None,
            provenance: self.provenance(),
        }
    }

    fn provenance(&self) -> Provenance {
        Provenance {
            provider_id: AgentProviderId::new("testkit.delivery"),
            agent_id: AgentId::new("delivery-agent-v1"),
            supporting_event_ids: vec![self.supporting_event_id.clone()],
            extensions: Default::default(),
        }
    }
}

fn structured_content(schema: &SchemaRef, case: usize) -> Content {
    Content {
        media_type: "application/json".to_owned(),
        schema_id: Some(schema.clone()),
        body: ContentBody::Inline(serde_json::json!({ "case": case, "status": "complete" })),
    }
}

fn artifact_metadata(case: usize) -> ArtifactRefWithDigest {
    ArtifactRefWithDigest {
        artifact_ref: ArtifactRef::new(format!("artifact-{case}")),
        // The public wire object carries no artifact bytes. This is only an
        // opaque, syntactically valid digest value; the test never claims to
        // verify content that is not available through the public contract.
        digest: Digest::new(format!("{case:064x}")),
    }
}

fn observed_rejection(
    result: Result<SequencedApply, AgentProtocolError>,
    label: &str,
) -> RejectionMode {
    match result {
        Err(_) => RejectionMode::Structural,
        Ok(sequenced) => match sequenced.outcome {
            ApplyOutcome::ProtocolViolation { .. } => RejectionMode::Quarantined,
            ApplyOutcome::Applied => panic!("{label} mutation entered the normal projection"),
            ApplyOutcome::ExactDuplicate => {
                panic!("fresh {label} mutation was treated as an exact duplicate")
            }
            _ => panic!("fresh {label} mutation returned an unknown apply outcome"),
        },
    }
}

#[test]
fn valid_delivery_binds_run_schema_artifact_metadata_and_prior_support() {
    let mut fixture = Fixture::running(0);
    let delivery = fixture.delivery(0);
    let expected_artifact = delivery.artifacts[0].clone();
    let event_id = fixture.reserve_event_id();
    let submitted = fixture
        .reducer
        .apply_provider_draft(fixture.draft_with_id(
            event_id,
            AgentEvent::DeliveryCommitted { delivery },
            None,
        ))
        .expect("valid delivery sequences");

    assert!(matches!(submitted.outcome, ApplyOutcome::Applied));
    assert_eq!(fixture.reducer.state().status(), AgentRunStatus::Delivered);
    let view = fixture.reducer.view();
    view.validate_integrity()
        .expect("delivered public view remains valid");
    let projected = view.delivery.expect("complete delivery is projected");
    assert_eq!(projected.artifacts, vec![expected_artifact]);
    assert_eq!(
        projected.provenance.supporting_event_ids,
        vec![fixture.supporting_event_id]
    );
}

#[test]
fn exactly_one_thousand_delivery_binding_mutations_enter_delivered_zero_times() {
    let mut family_counts = [0_usize; MUTATION_FAMILY_COUNT];
    let mut structural_rejections = 0;
    let mut quarantined_rejections = 0;
    let mut delivered_statuses = 0;
    let mut projected_complete_deliveries = 0;

    for (family_index, mutation) in DeliveryMutation::ALL.into_iter().enumerate() {
        for sample in 0..MUTATIONS_PER_FAMILY {
            let case = family_index * MUTATIONS_PER_FAMILY + sample + 1;
            let mut fixture = Fixture::running(case);
            let delivery_event_id = fixture.reserve_event_id();
            let mut delivery = fixture.delivery(case);
            mutation.apply(&mut delivery, &delivery_event_id, case);
            let draft = fixture.draft_with_id(
                delivery_event_id,
                AgentEvent::DeliveryCommitted { delivery },
                None,
            );

            let observed = observed_rejection(
                fixture.reducer.apply_provider_draft(draft),
                mutation.label(),
            );
            assert_eq!(
                observed,
                mutation.expected_rejection(),
                "{} mutation {sample} crossed the wrong gate",
                mutation.label()
            );
            match observed {
                RejectionMode::Structural => {
                    structural_rejections += 1;
                    assert_eq!(fixture.reducer.state().status(), AgentRunStatus::Running);
                }
                RejectionMode::Quarantined => {
                    quarantined_rejections += 1;
                    assert_eq!(fixture.reducer.state().status(), AgentRunStatus::Unknown);
                }
            }

            let view = fixture.reducer.view();
            delivered_statuses +=
                usize::from(fixture.reducer.state().status() == AgentRunStatus::Delivered);
            projected_complete_deliveries += usize::from(view.delivery.is_some());
            assert!(
                view.delivery.is_none(),
                "{} projected a delivery",
                mutation.label()
            );
            family_counts[family_index] += 1;
        }
    }

    assert_eq!(family_counts, [MUTATIONS_PER_FAMILY; MUTATION_FAMILY_COUNT]);
    assert_eq!(family_counts.iter().sum::<usize>(), EXACT_MUTATION_COUNT);
    assert_eq!(structural_rejections, 300);
    assert_eq!(quarantined_rejections, 700);
    assert_eq!(delivered_statuses, 0);
    assert_eq!(projected_complete_deliveries, 0);
}

#[test]
fn incomplete_failed_and_cancelled_count_as_zero_complete_deliveries() {
    let mut incomplete = Fixture::running(20_001);
    let partial_delivery = incomplete.partial_delivery(20_001);
    let incomplete_result = incomplete.provider_applied(
        AgentEvent::RunIncomplete {
            reason: IncompleteReason::LimitReached {
                limit: RunLimitKind::ModelSteps,
            },
            partial_delivery: Some(partial_delivery),
        },
        None,
    );
    assert!(matches!(incomplete_result.outcome, ApplyOutcome::Applied));

    let mut failed = Fixture::running(20_002);
    let failed_result = failed.provider_applied(
        AgentEvent::RunFailed {
            failure: AgentFailure {
                code: "provider_failure".to_owned(),
                message: "provider stopped before a complete delivery".to_owned(),
                retryable: false,
                details: serde_json::Value::Null,
            },
        },
        None,
    );
    assert!(matches!(failed_result.outcome, ApplyOutcome::Applied));

    let mut cancelled = Fixture::running(20_003);
    let command_id = CommandId::new("cancel-before-delivery");
    let reason = "caller cancelled before a complete delivery".to_owned();
    let command = AgentCommandEnvelope::new(
        command_id.clone(),
        cancelled.run.spec.run_id.clone(),
        None,
        AgentCommand::Cancel {
            reason: reason.clone(),
        },
    )
    .expect("cancel command seals");
    cancelled.host_applied(
        AgentEvent::CommandReceived { command },
        Some(command_id.clone()),
    );
    cancelled.provider_applied(
        AgentEvent::CommandDispositionRecorded {
            command_id: command_id.clone(),
            outcome: ProviderCommandOutcome::Accepted,
        },
        Some(command_id.clone()),
    );
    cancelled.provider_applied(
        AgentEvent::StopRequested {
            reason: reason.clone(),
        },
        Some(command_id),
    );
    let cancelled_result = cancelled.provider_applied(AgentEvent::RunCancelled { reason }, None);
    assert!(matches!(cancelled_result.outcome, ApplyOutcome::Applied));

    let terminal_views: [AgentRunView; 3] = [
        incomplete.reducer.view(),
        failed.reducer.view(),
        cancelled.reducer.view(),
    ];
    let terminal_statuses = terminal_views
        .iter()
        .map(|view| view.state.status())
        .collect::<Vec<_>>();
    assert_eq!(
        terminal_statuses,
        [
            AgentRunStatus::Incomplete,
            AgentRunStatus::Failed,
            AgentRunStatus::Cancelled,
        ]
    );
    assert!(terminal_views[0].partial_delivery.is_some());

    let complete_delivery_count = terminal_views
        .iter()
        .filter(|view| {
            view.validate_integrity()
                .expect("non-delivered terminal view remains valid");
            view.delivery.is_some()
        })
        .count();
    assert_eq!(complete_delivery_count, 0);
    assert!(terminal_views
        .iter()
        .all(|view| view.state.status() != AgentRunStatus::Delivered));
}
