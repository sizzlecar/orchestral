use super::*;
use serde_json::Value;
use std::num::NonZeroU64;

#[tokio::test]
async fn model_inline_budget_preserves_full_result_and_paged_recovery() {
    let mut bounds = policy(ApprovalPolicy::NotRequired);
    bounds.max_output_bytes = Some(1024 * 1024);
    let journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let artifacts = ToolArtifactStore::new(Arc::new(InMemoryBlobStore::default()), 128 * 1024, 80)
        .unwrap()
        .with_inline_output_limit(NonZeroU64::new(512).unwrap());
    let runtime = runtime_with_artifacts(bounds.clone(), journal.clone(), artifacts.clone());
    let executor = Arc::new(EchoExecutor {
        calls: AtomicUsize::new(0),
        delay: Duration::ZERO,
    });
    runtime
        .register(
            descriptor(bounds.clone(), ToolConcurrency::ParallelSafe),
            executor.clone(),
        )
        .unwrap();
    let large = (0..200)
        .map(|index| format!("{index:032x} quoted=\"value\" \\ unicode=你好🦀\n"))
        .collect::<String>();
    let invocation = invocation(&large);
    let grant = RunToolGrant { bounds };
    let result = runtime
        .invoke(
            invocation.clone(),
            grant.clone(),
            None,
            CancellationToken::new(),
        )
        .await;
    let GuardedToolResult::Outcome {
        outcome:
            ToolOutcome::Completed {
                output: ToolOutput::Artifact(artifact),
            },
        cached: false,
    } = result
    else {
        panic!("model-inline budget must spill below the executor collection ceiling");
    };
    let canonical = artifacts.resolve(&artifact).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&canonical).unwrap(),
        json!({"result":large})
    );

    let read_bounds = ToolPolicyBounds {
        allowed_effects: effects(&[EffectScope::ArtifactRead]),
        approval: ApprovalPolicy::NotRequired,
        max_timeout_ms: Some(1_000),
        max_output_bytes: Some(1024 * 1024),
        ..ToolPolicyBounds::default()
    };
    let reader = runtime_with_artifacts(
        read_bounds.clone(),
        Arc::new(InMemoryToolEffectJournalStore::default()),
        artifacts.clone(),
    );
    reader
        .register(
            guarded_artifact_read_descriptor(ToolRestriction {
                bounds: read_bounds.clone(),
            }),
            Arc::new(GuardedArtifactReadExecutor::new(artifacts.clone())),
        )
        .unwrap();
    for (index, (field, invalid)) in [
        ("offset", json!(-1)),
        ("max_bytes", json!(-1)),
        ("max_bytes", json!(0)),
        ("byte_size", json!(0)),
    ]
    .into_iter()
    .enumerate()
    {
        let mut arguments = json!({
            "artifact_ref":artifact.artifact.artifact_ref,
            "digest":artifact.artifact.digest,
            "media_type":artifact.media_type, "byte_size":artifact.byte_size,
        });
        arguments[field] = invalid;
        let result = reader
            .invoke(
                ToolInvocation {
                    run_id: RunId::new("paged-artifact-run"),
                    call_id: ToolCallId::new(format!("invalid-page-{index}")),
                    tool_id: ToolId::new("orchestral/artifact_read/v1"),
                    arguments,
                },
                RunToolGrant {
                    bounds: read_bounds.clone(),
                },
                None,
                CancellationToken::new(),
            )
            .await;
        assert!(
            matches!(result, GuardedToolResult::Outcome {
            outcome: ToolOutcome::Rejected { ref code, .. }, ..
            } if matches!(code.as_str(), "input_schema_violation" | "artifact_shape_invalid")),
            "{result:?}"
        );
    }
    let mut offset = 0_u64;
    let mut recovered = Vec::new();
    loop {
        let read = reader
            .invoke(
                ToolInvocation {
                    run_id: RunId::new("paged-artifact-run"),
                    call_id: ToolCallId::new(format!("page-at-{offset}")),
                    tool_id: ToolId::new("orchestral/artifact_read/v1"),
                    arguments: json!({
                        "artifact_ref":artifact.artifact.artifact_ref,
                        "digest":artifact.artifact.digest,
                        "media_type":artifact.media_type,
                        "byte_size":artifact.byte_size,
                        "offset":offset,
                        "max_bytes":u64::MAX,
                    }),
                },
                RunToolGrant {
                    bounds: read_bounds.clone(),
                },
                None,
                CancellationToken::new(),
            )
            .await;
        let GuardedToolResult::Outcome {
            outcome:
                ToolOutcome::Completed {
                    output: ToolOutput::Inline(page),
                },
            ..
        } = read
        else {
            panic!("artifact pages must remain inline instead of recursively spilling: {read:?}");
        };
        assert!(serde_jcs::to_vec(&page).unwrap().len() <= 512);
        assert_eq!(page["offset"], offset);
        let chunk = page["content"].as_str().unwrap();
        recovered.extend_from_slice(chunk.as_bytes());
        let next = page["next_offset"].as_u64().unwrap();
        assert_eq!(next - offset, chunk.len() as u64);
        if page["complete"] == true {
            assert_eq!(next, canonical.len() as u64);
            break;
        }
        assert!(next > offset, "incomplete pages must make UTF-8 progress");
        offset = next;
    }
    assert_eq!(recovered, canonical);

    let replay = runtime
        .invoke(invocation, grant, None, CancellationToken::new())
        .await;
    assert!(matches!(replay, GuardedToolResult::Outcome {
        outcome: ToolOutcome::Completed { output: ToolOutput::Artifact(ref cached) },
        cached: true,
    } if cached == &artifact));
    assert_eq!(
        executor.calls.load(Ordering::SeqCst),
        1,
        "reading or replaying output must not repeat the effect"
    );
}

#[tokio::test]
async fn artifact_reference_and_unicode_summary_fit_the_inline_budget_without_losing_content() {
    let bounds = policy(ApprovalPolicy::NotRequired);
    let artifacts = ToolArtifactStore::new(Arc::new(InMemoryBlobStore::default()), 64 * 1024, 2048)
        .unwrap()
        .with_inline_output_limit(NonZeroU64::new(512).unwrap());
    let runtime = runtime_with_artifacts(
        bounds.clone(),
        Arc::new(InMemoryToolEffectJournalStore::default()),
        artifacts.clone(),
    );
    runtime
        .register(
            descriptor(bounds.clone(), ToolConcurrency::ParallelSafe),
            Arc::new(EchoExecutor {
                calls: AtomicUsize::new(0),
                delay: Duration::ZERO,
            }),
        )
        .unwrap();
    let source = "🦀\"\\\n".repeat(1000);
    let result = runtime
        .invoke(
            invocation(&source),
            RunToolGrant { bounds },
            None,
            CancellationToken::new(),
        )
        .await;
    let GuardedToolResult::Outcome {
        outcome:
            ToolOutcome::Completed {
                output: ToolOutput::Artifact(artifact),
            },
        ..
    } = result
    else {
        panic!("{result:?}")
    };
    let shown = json!({
        "kind":"artifact", "artifact":artifact.artifact,
        "media_type":artifact.media_type, "byte_size":artifact.byte_size,
        "summary":artifact.summary,
    });
    assert!(serde_jcs::to_vec(&shown).unwrap().len() <= 512);
    artifact.validate().unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&artifacts.resolve(&artifact).await.unwrap()).unwrap(),
        json!({"result":source})
    );
}

#[test]
fn inline_budget_is_part_of_the_runtime_recovery_contract() {
    let make = |limit| {
        let artifacts = ToolArtifactStore::new(Arc::new(InMemoryBlobStore::default()), 8192, 80)
            .unwrap()
            .with_inline_output_limit(NonZeroU64::new(limit).unwrap());
        runtime_with_artifacts(
            policy(ApprovalPolicy::NotRequired),
            Arc::new(InMemoryToolEffectJournalStore::default()),
            artifacts,
        )
        .execution_contract_digest()
        .unwrap()
    };
    assert_ne!(make(512), make(1024));
    assert_eq!(make(512), make(512));
}
