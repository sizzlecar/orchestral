//! Deterministic, high-volume protocol invariant checks.
//!
//! These sampled campaigns are regression gates over 10k-class inputs. They
//! deliberately do not claim random or exhaustive coverage of the wire space.

use std::collections::BTreeSet;

use orchestral_core::agent_protocol::{
    reference::{AgentRunReducer, ApplyOutcome},
    wire::{
        AgentAdmission, AgentCapabilities, AgentDescriptor, AgentDescriptorEnvelope, AgentEvent,
        AgentEventDraft, AgentEventId, AgentExecutionRef, AgentId, AgentProtocolErrorCode,
        AgentProviderId, AgentProviderStreamItem, AgentRunEnvelope, AgentSessionId, AgentTelemetry,
        AgentTelemetryEnvelope, Content, ControlCapabilities, Digest, EffectMediation, Extensions,
        OutputId, ProviderBindingRef, RunId, TelemetryId,
    },
    AGENT_PROTOCOL_V1,
};
use serde_json::json;

const DRAFT_SHARDS: usize = 100;
const DRAFTS_PER_SHARD: usize = 100;
const DRAFT_CASES: usize = DRAFT_SHARDS * DRAFTS_PER_SHARD;
const WRONG_LANE_CASES_PER_DIRECTION: usize = 1_000;
const TELEMETRY_GROUPS_PER_MUTATION: usize = 1_000;
const WIRE_CASES: usize = 10_000;

#[derive(Debug, Clone, Copy)]
struct DeterministicLcg(u64);

impl DeterministicLcg {
    const fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }
}

#[derive(Clone)]
struct PreparedRun {
    reducer: AgentRunReducer,
    run_id: RunId,
    session_id: AgentSessionId,
    spec_digest: Digest,
}

fn descriptor() -> AgentDescriptorEnvelope {
    AgentDescriptorEnvelope::seal(AgentDescriptor {
        provider_id: AgentProviderId::new("property.provider"),
        agent_id: AgentId::new("property-agent-v1"),
        supported_protocol_versions: vec![AGENT_PROTOCOL_V1],
        accepted_content_types: BTreeSet::from(["text/plain".to_owned()]),
        capabilities: AgentCapabilities {
            controls: ControlCapabilities::default(),
            effect_mediation: EffectMediation::HostMediated,
            ..AgentCapabilities::default()
        },
        extensions: Extensions::new(),
    })
    .expect("property descriptor must seal")
}

fn new_reducer(label: &str) -> PreparedRun {
    let descriptor = descriptor();
    let session_id = AgentSessionId::new(format!("property-session-{label}"));
    let run_id = RunId::new(format!("property-run-{label}"));
    let run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        session_id.clone(),
        run_id.clone(),
        vec![Content::text(format!("property input {label}"))],
    )
    .expect("property run must seal");
    let spec_digest = run.spec_digest.clone();
    let request = orchestral_core::agent_protocol::wire::AgentStartRequest::new(
        run,
        ProviderBindingRef::new(format!("property-binding-{label}")),
        &descriptor,
    )
    .expect("property start request must validate");
    let execution =
        AgentExecutionRef::for_start(&request, &descriptor).expect("execution must bind to run");
    let reducer = AgentRunReducer::new(execution, &request, &descriptor, AgentAdmission::default())
        .expect("property reducer must initialize");

    PreparedRun {
        reducer,
        run_id,
        session_id,
        spec_digest,
    }
}

fn draft(run_id: &RunId, event_id: impl Into<String>, payload: AgentEvent) -> AgentEventDraft {
    AgentEventDraft {
        event_id: AgentEventId::new(event_id),
        run_id: run_id.clone(),
        causation_id: None,
        source_fingerprint: None,
        payload,
    }
}

fn accept_and_start(prepared: &mut PreparedRun) {
    let accepted = prepared
        .reducer
        .apply_host_draft(draft(
            &prepared.run_id,
            "host-run-accepted",
            AgentEvent::RunAccepted {
                session_id: prepared.session_id.clone(),
                spec_digest: prepared.spec_digest.clone(),
            },
        ))
        .expect("RunAccepted must be admitted");
    assert!(matches!(accepted.outcome, ApplyOutcome::Applied));
    assert_eq!(accepted.event().run_seq, 1);

    let started = prepared
        .reducer
        .apply_provider_draft(draft(
            &prepared.run_id,
            "provider-run-started",
            AgentEvent::RunStarted,
        ))
        .expect("RunStarted must be admitted");
    assert!(matches!(started.outcome, ApplyOutcome::Applied));
    assert_eq!(started.event().run_seq, 2);
}

fn output_draft(
    run_id: &RunId,
    event_id: impl Into<String>,
    output_id: impl Into<String>,
    text: impl Into<String>,
) -> AgentEventDraft {
    draft(
        run_id,
        event_id,
        AgentEvent::OutputCommitted {
            output_id: OutputId::new(output_id),
            content: vec![Content::text(text)],
        },
    )
}

#[test]
fn sampled_10k_provider_drafts_preserve_exact_dedupe_and_reject_equivocation() {
    let mut lcg = DeterministicLcg::new(0x4f52_4348_4553_5452);
    let mut applied = 0_usize;
    let mut exact_duplicates = 0_usize;
    let mut structured_equivocation_rejections = 0_usize;
    let mut erroneously_accepted_equivocations = 0_usize;

    // Sharding bounds the reference reducer's intentionally in-memory output
    // history while retaining exactly 10,000 distinct durable draft cases.
    for shard in 0..DRAFT_SHARDS {
        let mut prepared = new_reducer(&format!("dedupe-{shard}"));
        accept_and_start(&mut prepared);
        let first_output_seq = 3_u64;

        for index in 0..DRAFTS_PER_SHARD {
            let sample = lcg.next_u64();
            let event_id = format!("provider-output-event-{shard}-{index}");
            let original = output_draft(
                &prepared.run_id,
                event_id,
                format!("provider-output-{shard}-{index}"),
                format!("sample-{sample:016x}"),
            );
            let expected_seq = first_output_seq + index as u64;
            let admitted = prepared
                .reducer
                .apply_provider_draft(original.clone())
                .expect("new Provider draft must be admitted");
            assert!(matches!(admitted.outcome, ApplyOutcome::Applied));
            assert_eq!(admitted.event().run_seq, expected_seq);
            applied += 1;

            let projection_after_original = prepared
                .reducer
                .view()
                .projection_digest()
                .expect("projection must digest");
            let duplicate = prepared
                .reducer
                .apply_provider_draft(original.clone())
                .expect("exact duplicate must return its original record");
            assert!(matches!(duplicate.outcome, ApplyOutcome::ExactDuplicate));
            assert_eq!(duplicate.event().run_seq, admitted.event().run_seq);
            assert_eq!(
                duplicate.event().event_digest,
                admitted.event().event_digest
            );
            assert_eq!(duplicate.record.draft_digest, admitted.record.draft_digest);
            assert_eq!(
                prepared
                    .reducer
                    .view()
                    .projection_digest()
                    .expect("projection must digest"),
                projection_after_original
            );
            exact_duplicates += 1;

            let mut equivocation = original;
            equivocation.payload = AgentEvent::OutputCommitted {
                output_id: OutputId::new(format!("equivocated-output-{shard}-{index}")),
                content: vec![Content::text(format!("different-{sample:016x}"))],
            };
            assert_ne!(
                equivocation
                    .computed_digest()
                    .expect("equivocation must itself be a valid draft"),
                admitted.record.draft_digest
            );
            match prepared.reducer.apply_provider_draft(equivocation) {
                Err(error) => {
                    assert_eq!(error.code, AgentProtocolErrorCode::DuplicateConflict);
                    assert!(!error.message.trim().is_empty());
                    structured_equivocation_rejections += 1;
                }
                Ok(_) => erroneously_accepted_equivocations += 1,
            }
            assert_eq!(
                prepared
                    .reducer
                    .view()
                    .projection_digest()
                    .expect("projection must digest"),
                projection_after_original,
                "equivocation must not change the projection"
            );
        }

        // A valid tail proves every rejected equivocation left the sequence
        // cursor untouched, including the last sample in the shard.
        let tail = prepared
            .reducer
            .apply_provider_draft(output_draft(
                &prepared.run_id,
                format!("provider-output-tail-{shard}"),
                format!("provider-output-tail-{shard}"),
                "tail",
            ))
            .expect("valid tail must remain sequence-admissible");
        assert_eq!(
            tail.event().run_seq,
            first_output_seq + DRAFTS_PER_SHARD as u64
        );
    }

    assert_eq!(applied, DRAFT_CASES);
    assert_eq!(exact_duplicates, DRAFT_CASES);
    assert_eq!(structured_equivocation_rejections, DRAFT_CASES);
    assert_eq!(erroneously_accepted_equivocations, 0);
}

#[test]
fn wrong_authority_lanes_reject_1k_each_without_projection_or_sequence_progress() {
    let mut prepared = new_reducer("wrong-authority-lanes");
    let initial_projection = prepared
        .reducer
        .view()
        .projection_digest()
        .expect("initial projection must digest");
    let mut provider_lane_host_events = 0_usize;
    let mut host_lane_provider_events = 0_usize;
    let mut normally_projected = 0_usize;

    for index in 0..WRONG_LANE_CASES_PER_DIRECTION {
        let host_only = draft(
            &prepared.run_id,
            format!("wrong-provider-lane-{index}"),
            AgentEvent::RunAccepted {
                session_id: prepared.session_id.clone(),
                spec_digest: prepared.spec_digest.clone(),
            },
        );
        match prepared.reducer.apply_provider_draft(host_only) {
            Err(error) => {
                assert_eq!(error.code, AgentProtocolErrorCode::InvalidTransition);
                provider_lane_host_events += 1;
            }
            Ok(_) => normally_projected += 1,
        }

        let provider_only = draft(
            &prepared.run_id,
            format!("wrong-host-lane-{index}"),
            AgentEvent::RunStarted,
        );
        match prepared.reducer.apply_host_draft(provider_only) {
            Err(error) => {
                assert_eq!(error.code, AgentProtocolErrorCode::InvalidTransition);
                host_lane_provider_events += 1;
            }
            Ok(_) => normally_projected += 1,
        }
    }

    assert_eq!(provider_lane_host_events, WRONG_LANE_CASES_PER_DIRECTION);
    assert_eq!(host_lane_provider_events, WRONG_LANE_CASES_PER_DIRECTION);
    assert_eq!(normally_projected, 0);
    assert_eq!(
        prepared
            .reducer
            .view()
            .projection_digest()
            .expect("projection must digest"),
        initial_projection
    );

    // Correct-lane events still receive seq 1 and 2: none of the 2,000
    // rejected attempts consumed journal sequence space.
    accept_and_start(&mut prepared);
    assert_eq!(prepared.reducer.view().last_run_seq, Some(2));
}

#[derive(Debug, Default)]
struct StreamClassification {
    durable_events: usize,
    telemetry: usize,
}

fn consume_provider_items(
    reducer: &mut AgentRunReducer,
    items: Vec<AgentProviderStreamItem>,
) -> StreamClassification {
    let mut classification = StreamClassification::default();
    for item in items {
        item.validate_integrity()
            .expect("sampled stream item must validate");
        match item {
            AgentProviderStreamItem::Event(event) => {
                let applied = reducer
                    .apply_provider_draft(*event)
                    .expect("durable stream event must be admitted");
                assert!(matches!(applied.outcome, ApplyOutcome::Applied));
                classification.durable_events += 1;
            }
            AgentProviderStreamItem::Telemetry(_) => {
                // Classification and validation are the entire telemetry
                // path: telemetry is intentionally never passed to reducer.
                classification.telemetry += 1;
            }
            _ => panic!("unknown v1 stream channel in a v1 sampled gate"),
        }
    }
    classification
}

fn telemetry_samples(run_id: &RunId, group: usize) -> Vec<AgentTelemetryEnvelope> {
    vec![
        AgentTelemetryEnvelope {
            telemetry_id: TelemetryId::new(format!("telemetry-progress-{group}")),
            run_id: run_id.clone(),
            provider_seq: Some(1),
            payload: AgentTelemetry::ProgressReported {
                message: format!("progress {group}"),
                fraction: Some(0.25),
            },
        },
        AgentTelemetryEnvelope {
            telemetry_id: TelemetryId::new(format!("telemetry-delta-{group}")),
            run_id: run_id.clone(),
            provider_seq: Some(2),
            payload: AgentTelemetry::OutputDelta {
                output_id: OutputId::new(format!("telemetry-output-{group}")),
                delta: Content::text(format!("delta {group}")),
            },
        },
        AgentTelemetryEnvelope {
            telemetry_id: TelemetryId::new(format!("telemetry-extension-{group}")),
            run_id: run_id.clone(),
            provider_seq: Some(3),
            payload: AgentTelemetry::Extension {
                namespace: "property/progress".to_owned(),
                value: json!({"group": group}),
            },
        },
        AgentTelemetryEnvelope {
            telemetry_id: TelemetryId::new(format!("telemetry-final-{group}")),
            run_id: run_id.clone(),
            provider_seq: Some(4),
            payload: AgentTelemetry::ProgressReported {
                message: format!("final progress {group}"),
                fraction: Some(0.75),
            },
        },
    ]
}

fn stream_with_durable_event(
    telemetry: Vec<AgentTelemetryEnvelope>,
    durable: AgentEventDraft,
) -> Vec<AgentProviderStreamItem> {
    let split = telemetry.len() / 2;
    let mut items = Vec::with_capacity(telemetry.len() + 1);
    items.extend(
        telemetry[..split]
            .iter()
            .cloned()
            .map(AgentProviderStreamItem::Telemetry),
    );
    items.push(AgentProviderStreamItem::Event(Box::new(durable)));
    items.extend(
        telemetry[split..]
            .iter()
            .cloned()
            .map(AgentProviderStreamItem::Telemetry),
    );
    items
}

#[derive(Debug, Clone, Copy)]
enum TelemetryMutation {
    Insert,
    Drop,
    Duplicate,
    Reorder,
}

impl TelemetryMutation {
    const ALL: [Self; 4] = [Self::Insert, Self::Drop, Self::Duplicate, Self::Reorder];

    const fn label(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Drop => "drop",
            Self::Duplicate => "duplicate",
            Self::Reorder => "reorder",
        }
    }

    fn apply(
        self,
        mut telemetry: Vec<AgentTelemetryEnvelope>,
        run_id: &RunId,
        group: usize,
    ) -> Vec<AgentTelemetryEnvelope> {
        match self {
            Self::Insert => telemetry.insert(
                1,
                AgentTelemetryEnvelope {
                    telemetry_id: TelemetryId::new(format!("telemetry-inserted-{group}")),
                    run_id: run_id.clone(),
                    provider_seq: Some(99),
                    payload: AgentTelemetry::ProgressReported {
                        message: format!("inserted {group}"),
                        fraction: Some(0.5),
                    },
                },
            ),
            Self::Drop => {
                telemetry.remove(group % telemetry.len());
            }
            Self::Duplicate => {
                let duplicated = telemetry[group % telemetry.len()].clone();
                telemetry.insert(2, duplicated);
            }
            Self::Reorder => telemetry.reverse(),
        }
        telemetry
    }
}

#[test]
fn telemetry_insert_drop_duplicate_and_reorder_do_not_change_durable_projection() {
    let mut prepared = new_reducer("telemetry-metamorphic");
    accept_and_start(&mut prepared);
    let stable_reducer = prepared.reducer;
    let run_id = prepared.run_id;
    let mut mutation_groups = [0_usize; 4];

    for (mutation_index, mutation) in TelemetryMutation::ALL.into_iter().enumerate() {
        for group in 0..TELEMETRY_GROUPS_PER_MUTATION {
            let global_group = mutation_index * TELEMETRY_GROUPS_PER_MUTATION + group;
            let base_telemetry = telemetry_samples(&run_id, global_group);
            let mutated_telemetry = mutation.apply(base_telemetry.clone(), &run_id, global_group);
            let durable = output_draft(
                &run_id,
                format!("telemetry-trace-event-{}-{group}", mutation.label()),
                format!("telemetry-trace-output-{}-{group}", mutation.label()),
                format!("durable {} {group}", mutation.label()),
            );

            let mut baseline = stable_reducer.clone();
            let mut transformed = stable_reducer.clone();
            let baseline_counts = consume_provider_items(
                &mut baseline,
                stream_with_durable_event(base_telemetry, durable.clone()),
            );
            let transformed_counts = consume_provider_items(
                &mut transformed,
                stream_with_durable_event(mutated_telemetry, durable),
            );
            assert_eq!(baseline_counts.durable_events, 1);
            assert_eq!(transformed_counts.durable_events, 1);
            assert_eq!(baseline_counts.telemetry, 4);
            assert!(transformed_counts.telemetry >= 3);
            assert_eq!(
                baseline
                    .view()
                    .projection_digest()
                    .expect("baseline projection must digest"),
                transformed
                    .view()
                    .projection_digest()
                    .expect("transformed projection must digest")
            );

            let next = output_draft(
                &run_id,
                format!("telemetry-next-event-{}-{group}", mutation.label()),
                format!("telemetry-next-output-{}-{group}", mutation.label()),
                "next durable fact",
            );
            let baseline_next = baseline
                .apply_provider_draft(next.clone())
                .expect("baseline next durable event must apply");
            let transformed_next = transformed
                .apply_provider_draft(next)
                .expect("transformed next durable event must apply");
            assert_eq!(
                baseline_next.event().run_seq,
                transformed_next.event().run_seq
            );
            assert_eq!(
                baseline_next.event().event_digest,
                transformed_next.event().event_digest
            );
            assert_eq!(
                baseline_next.record.draft_digest,
                transformed_next.record.draft_digest
            );
            assert_eq!(baseline_next.event().run_seq, 4);
            mutation_groups[mutation_index] += 1;
        }
    }

    assert_eq!(
        mutation_groups, [TELEMETRY_GROUPS_PER_MUTATION; 4],
        "each telemetry metamorphism must retain its full sampled group count"
    );
    assert!(mutation_groups.iter().sum::<usize>() >= 1_000);
}

#[test]
fn sampled_10k_wire_roundtrips_are_strict_and_extensions_affect_digest() {
    let mut lcg = DeterministicLcg::new(0x5749_5245_5f47_4154);
    let mut roundtrips = 0_usize;
    let mut unknown_core_rejections = 0_usize;
    let mut unknown_core_acceptances = 0_usize;
    let mut extension_digest_changes = 0_usize;

    for index in 0..WIRE_CASES {
        let sample = lcg.next_u64();
        let extension_key = format!("property/sample-{}", index % 31);
        let mut spec = AgentRunEnvelope::new(
            AGENT_PROTOCOL_V1,
            AgentSessionId::new(format!("wire-session-{index}")),
            RunId::new(format!("wire-run-{index}")),
            vec![Content::text(format!("wire-sample-{sample:016x}"))],
        )
        .expect("wire fixture must seal")
        .spec;
        spec.extensions.insert(
            extension_key.clone(),
            json!({"case": index, "sample": sample.to_string()}),
        );
        let run = AgentRunEnvelope::seal(spec).expect("extended wire fixture must seal");
        run.validate_integrity()
            .expect("namespaced extension must validate");

        let encoded = serde_json::to_vec(&run).expect("wire fixture must serialize");
        let decoded: AgentRunEnvelope =
            serde_json::from_slice(&encoded).expect("strict wire roundtrip must deserialize");
        assert_eq!(decoded, run);
        decoded
            .validate_integrity()
            .expect("roundtripped digest must remain valid");
        roundtrips += 1;

        let mut unknown = serde_json::to_value(&run).expect("wire fixture must become JSON");
        let target = if index % 2 == 0 {
            unknown
                .as_object_mut()
                .expect("run envelope must be a JSON object")
        } else {
            unknown["spec"]
                .as_object_mut()
                .expect("run spec must be a JSON object")
        };
        target.insert(
            format!("unknown_core_field_{index}"),
            json!({"sample": sample}),
        );
        match serde_json::from_value::<AgentRunEnvelope>(unknown) {
            Err(_) => unknown_core_rejections += 1,
            Ok(_) => unknown_core_acceptances += 1,
        }

        let mut changed_spec = run.spec.clone();
        changed_spec.extensions.insert(
            extension_key,
            json!({
                "case": index,
                "sample": sample.to_string(),
                "metamorphic_change": true
            }),
        );
        let changed =
            AgentRunEnvelope::seal(changed_spec).expect("changed extension fixture must seal");
        changed
            .validate_integrity()
            .expect("changed namespaced extension must validate");
        assert_ne!(changed.spec_digest, run.spec_digest);
        extension_digest_changes += 1;
    }

    assert_eq!(roundtrips, WIRE_CASES);
    assert_eq!(unknown_core_rejections, WIRE_CASES);
    assert_eq!(unknown_core_acceptances, 0);
    assert_eq!(extension_digest_changes, WIRE_CASES);
}
