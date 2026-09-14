use super::*;
use orchestral_core::tool_protocol::ToolArtifact;
use orchestral_runtime::{InMemoryBlobStore, ToolArtifactStore};
use std::num::NonZeroU64;

fn artifact_value(artifact: &ToolArtifact) -> Value {
    json!({
        "kind": "artifact", "artifact": artifact.artifact,
        "media_type": artifact.media_type, "byte_size": artifact.byte_size,
        "summary": artifact.summary,
    })
}

#[tokio::test]
async fn model_budget_uses_visible_source_without_spilling_hidden_journal_metadata() {
    let mut fixture = Fixture::new(ApprovalPolicy::NotRequired, 64 * 1024);
    fixture
        .policy
        .allowed_effects
        .insert(EffectScope::ArtifactRead);
    fs::write(fixture.file(), "x".repeat(512)).unwrap();
    let initial = fixture.runtime();
    let canonical = fixture
        .read(&initial, "run", "initial", json!({"path":"source.rs"}))
        .await;
    let read = call(
        "run",
        "bounded",
        "orchestral/file_read/v3",
        json!({"path":"source.rs"}),
    );
    let visible = initial.project_model_output(&read, &canonical).unwrap();
    let limit = serde_jcs::to_vec(&visible).unwrap().len() as u64;
    assert!(serde_jcs::to_vec(&canonical).unwrap().len() as u64 > limit);
    let artifacts = ToolArtifactStore::new(Arc::new(InMemoryBlobStore::default()), 128 * 1024, 80)
        .unwrap()
        .with_inline_output_limit(NonZeroU64::new(limit).unwrap());
    let runtime = fixture.runtime_with_artifacts(Some(artifacts));
    let bounded = fixture
        .read(&runtime, "run", "bounded", read.arguments.clone())
        .await;
    assert_eq!(bounded, canonical);
    assert_eq!(
        runtime.project_model_output(&read, &bounded).unwrap(),
        visible
    );
}

#[tokio::test]
async fn complete_artifact_pages_preserve_observed_write_evidence_across_restart() {
    let mut fixture = Fixture::new(ApprovalPolicy::NotRequired, 64 * 1024);
    fixture
        .policy
        .allowed_effects
        .insert(EffectScope::ArtifactRead);
    let artifacts = ToolArtifactStore::new(Arc::new(InMemoryBlobStore::default()), 128 * 1024, 80)
        .unwrap()
        .with_inline_output_limit(NonZeroU64::new(1024).unwrap());
    let runtime = fixture.runtime_with_artifacts(Some(artifacts.clone()));
    let source = "// quoted=\"\\n\" 🦀 你好\r\n".repeat(120);
    fs::write(fixture.file(), &source).unwrap();
    let invocation = call(
        "run",
        "read",
        "orchestral/file_read/v3",
        json!({"path":"source.rs"}),
    );
    let result = runtime
        .invoke(invocation, fixture.grant(), None, CancellationToken::new())
        .await;
    let GuardedToolResult::Outcome {
        outcome:
            ToolOutcome::Completed {
                output: ToolOutput::Artifact(artifact),
            },
        ..
    } = result
    else {
        panic!("expected stored read: {result:?}")
    };
    let mut messages = vec![tool_message("read", artifact_value(&artifact))];
    let mut offset = 0;
    loop {
        let id = format!("page-{offset}");
        let page = completed(
            runtime
                .invoke(
                    call(
                        "run",
                        &id,
                        "orchestral/artifact_read/v1",
                        json!({
                            "artifact_ref":artifact.artifact.artifact_ref,
                            "digest":artifact.artifact.digest,
                            "media_type":artifact.media_type, "byte_size":artifact.byte_size,
                            "offset":offset, "max_bytes":2048,
                        }),
                    ),
                    fixture.grant(),
                    None,
                    CancellationToken::new(),
                )
                .await,
        );
        assert!(serde_jcs::to_vec(&page).unwrap().len() <= 1024);
        let next = page["next_offset"].as_u64().unwrap();
        let done = page["complete"] == true;
        assert!(next > offset);
        messages.push(tool_message(&id, page));
        offset = next;
        if done {
            break;
        }
    }
    assert!(messages.len() > 3, "exercise multiple pages");

    // Every case lacks one part of committed, actually observed content.
    let mut edited = messages.clone();
    if let ModelContent::ToolResult { result, .. } = &mut edited[2].content[0] {
        result["content"] = json!("fabricated source");
    }
    let mut unknown_page = messages.clone();
    if let ModelContent::ToolResult { call_id, .. } = &mut unknown_page[2].content[0] {
        *call_id = ModelToolCallId::new("uncommitted-page");
    }
    let mut failed_page = messages.clone();
    if let ModelContent::ToolResult { is_error, .. } = &mut failed_page[2].content[0] {
        *is_error = true;
    }
    let cases = [
        vec![messages[0].clone()],
        messages[..messages.len() - 1].to_vec(),
        messages[1..].to_vec(),
        edited,
        unknown_page,
        failed_page,
    ];
    for (index, invalid) in cases.iter().enumerate() {
        let observations = freeze(&runtime, "run", invalid, &[]).await;
        rejected_with(
            fixture
                .write(
                    &runtime,
                    replace("run", &format!("invalid-{index}"), "wrong"),
                    &observations,
                )
                .await,
            "file_write_precondition_missing",
        );
        assert_eq!(fs::read_to_string(fixture.file()).unwrap(), source);
    }
    let pending_id = if let ModelContent::ToolResult { call_id, .. } = &messages[2].content[0] {
        call_id.as_str()
    } else {
        unreachable!()
    };
    for (run, pending) in [("other-run", Vec::new()), ("run", vec![pending_id])] {
        let observations = freeze(&runtime, run, &messages, &pending).await;
        rejected_with(
            fixture
                .write(
                    &runtime,
                    replace(run, &format!("invalid-scope-{run}"), "wrong"),
                    &observations,
                )
                .await,
            "file_write_precondition_missing",
        );
    }

    // Recreate the runtime from its durable services, without executing the
    // read or any page again. Page order and duplicate observations are benign.
    drop(runtime);
    let runtime = fixture.runtime_with_artifacts(Some(artifacts));
    let mut visible = messages.clone();
    visible[1..].reverse();
    visible.push(messages[2].clone());
    let observations = freeze(&runtime, "run", &visible, &[]).await;
    let write = replace("run", "write", "updated\r\n");
    completed(fixture.write(&runtime, write.clone(), &observations).await);
    assert_eq!(fs::read(fixture.file()).unwrap(), b"updated\r\n");
    let key = ToolEffectKey::new(RunId::new("run"), ToolCallId::new("write"));
    let effect = runtime.inspect_effect(&key).await.unwrap().unwrap();
    let resolution = effect.prepared.argument_resolution.unwrap();
    assert_eq!(
        resolution.arguments["expected_digest"],
        json!(Digest::sha256(source.as_bytes()))
    );
    assert_eq!(
        resolution.source,
        ToolEffectKey::new(RunId::new("run"), ToolCallId::new("read"))
    );

    fs::write(fixture.file(), "later external change").unwrap();
    let replay = fixture
        .write(&runtime, write, &FrozenToolObservations::default())
        .await;
    assert!(matches!(
        replay,
        GuardedToolResult::Outcome {
            cached: true,
            outcome: ToolOutcome::Completed { .. }
        }
    ));
    assert_eq!(fs::read(fixture.file()).unwrap(), b"later external change");
}
