use super::*;
use orchestral_core::agent_protocol::{
    spi::InMemoryAgentJournalStore,
    wire::{AgentSessionId, Content, ProviderBindingRef},
};
use orchestral_core::agent_session::{
    AgentSessionEvent, AgentSessionEventDraft, AgentSessionEventId, AgentSessionJournalStore,
    InMemoryAgentSessionJournalStore,
};
use orchestral_core::model_protocol::*;
use orchestral_runtime::{
    AgentClient, AgentController, GenericAgentConfig, InternalGenericAgentProvider,
    JsonSizeTokenMeter,
};

struct RegistrationModel;

#[async_trait]
impl ModelBackend for RegistrationModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "artifact-session-test".into(),
            capabilities: Default::default(),
            extensions: Default::default(),
        }
    }
    async fn start(
        &self,
        request: ModelRequest,
        _: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        Ok(Box::pin(futures_util::stream::iter(
            [
                ModelEvent::TextDelta {
                    delta: "registered".into(),
                },
                ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            ]
            .into_iter()
            .enumerate()
            .map(move |(index, payload)| {
                Ok(ModelStreamEvent {
                    request_id: request.request_id.clone(),
                    event_id: ModelEventId::new(format!("event-{index}")),
                    sequence: index as u64 + 1,
                    payload,
                })
            }),
        )))
    }
}

pub(super) struct TestSessionScope {
    pub runs: Arc<InMemoryAgentJournalStore>,
    pub sessions: Arc<InMemoryAgentSessionJournalStore>,
}

impl TestSessionScope {
    pub async fn new() -> Self {
        let runs = Arc::new(InMemoryAgentJournalStore::default());
        let sessions = Arc::new(InMemoryAgentSessionJournalStore::default());
        let provider = InternalGenericAgentProvider::new_with_session_journal(
            Arc::new(RegistrationModel),
            GenericAgentConfig::new("artifact-agent", "test"),
            sessions.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .unwrap();
        let controller = Arc::new(
            AgentController::with_journal_store(
                Arc::new(provider),
                ProviderBindingRef::new("test"),
                runs.clone(),
            )
            .unwrap(),
        );
        for (session, run) in [
            ("session", "run"),
            ("session", "later"),
            ("other-session", "foreign"),
        ] {
            AgentClient::new(controller.clone(), AgentSessionId::new(session))
                .start_with_run_id(RunId::new(run), vec![Content::text("Register this turn.")])
                .await
                .unwrap()
                .wait_until_blocked()
                .await
                .unwrap();
        }
        Self { runs, sessions }
    }

    pub async fn commit(&self, session: &str, run: &str, call: &str, artifact: &ToolArtifact) {
        self.sessions
            .append(AgentSessionEventDraft {
                event_id: AgentSessionEventId::new(format!("artifact-{run}-{call}")),
                session_id: AgentSessionId::new(session),
                run_id: RunId::new(run),
                payload: AgentSessionEvent::ToolExchangeCommitted {
                    request_id: ModelRequestId::new(format!("request-{call}")),
                    assistant: ModelMessage {
                        role: ModelRole::Assistant,
                        content: vec![ModelContent::ToolCall {
                            call_id: ModelToolCallId::new(call),
                            name: "file_read".into(),
                            arguments: json!({"path":"source.rs"}),
                            extensions: Default::default(),
                        }],
                    },
                    tool: tool_message(call, artifact_value(artifact)),
                    retained_artifacts: vec![artifact.artifact.clone()],
                    usage: None,
                },
            })
            .await
            .unwrap();
    }
}

fn fixture() -> Fixture {
    let mut fixture = Fixture::new(ApprovalPolicy::NotRequired, 64 * 1024);
    fixture
        .policy
        .allowed_effects
        .insert(EffectScope::ArtifactRead);
    fs::write(fixture.file(), "// scoped source 你好🦀\n".repeat(150)).unwrap();
    fixture
}

fn store(blobs: Arc<dyn orchestral_core::io::BlobStore>) -> ToolArtifactStore {
    ToolArtifactStore::new(blobs, 128 * 1024, 80)
        .unwrap()
        .with_inline_output_limit(NonZeroU64::new(1024).unwrap())
}

async fn produce(runtime: &Runtime, fixture: &Fixture, run: &str, id: &str) -> ToolArtifact {
    let result = runtime
        .invoke(
            call(
                run,
                id,
                "orchestral/file_read/v3",
                json!({"path":"source.rs"}),
            ),
            fixture.grant(),
            None,
            CancellationToken::new(),
        )
        .await;
    match result {
        GuardedToolResult::Outcome {
            outcome:
                ToolOutcome::Completed {
                    output: ToolOutput::Artifact(artifact),
                },
            ..
        } => artifact,
        other => panic!("expected stored read: {other:?}"),
    }
}

async fn read(
    runtime: &Runtime,
    fixture: &Fixture,
    run: &str,
    id: &str,
    artifact: &ToolArtifact,
) -> GuardedToolResult {
    runtime
        .invoke(
            call(
                run,
                id,
                "orchestral/artifact_read/v2",
                json!({"artifact_ref":artifact.artifact.artifact_ref}),
            ),
            fixture.grant(),
            None,
            CancellationToken::new(),
        )
        .await
}

#[tokio::test]
async fn reference_requires_committed_producer_and_registered_session_before_blob_access() {
    let scope = TestSessionScope::new().await;
    let fixture = fixture();
    let blobs = Arc::new(ProbeBlobStore::default());
    let artifacts = store(blobs.clone());
    let runtime = page_runtime(&fixture, artifacts.clone(), Some(&scope));
    let artifact = produce(&runtime, &fixture, "run", "source").await;
    rejected_with(
        read(&runtime, &fixture, "run", "unpublished", &artifact).await,
        "artifact_reference_invalid",
    );
    scope
        .commit("session", "run", "fake-effect", &artifact)
        .await;
    rejected_with(
        read(&runtime, &fixture, "run", "forged-envelope", &artifact).await,
        "artifact_reference_invalid",
    );
    assert_eq!(blobs.reads.load(Ordering::SeqCst), 0);
    scope.commit("session", "run", "source", &artifact).await;
    for run in ["foreign", "unknown"] {
        rejected_with(
            read(&runtime, &fixture, run, "denied", &artifact).await,
            "artifact_reference_invalid",
        );
    }
    let mut denied = fixture.grant();
    denied
        .bounds
        .allowed_effects
        .remove(&EffectScope::ArtifactRead);
    let denied = runtime
        .invoke(
            call(
                "run",
                "policy-denied",
                "orchestral/artifact_read/v2",
                json!({"artifact_ref":artifact.artifact.artifact_ref}),
            ),
            denied,
            None,
            CancellationToken::new(),
        )
        .await;
    assert!(!matches!(
        denied,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed { .. },
            ..
        }
    ));
    assert_eq!(blobs.reads.load(Ordering::SeqCst), 0);
    let first = completed(read(&runtime, &fixture, "run", "page", &artifact).await);
    assert_eq!(first["digest"], json!(artifact.artifact.digest));
    assert_eq!(
        first,
        completed(read(&runtime, &fixture, "later", "page", &artifact).await)
    );
    let reads = blobs.reads.load(Ordering::SeqCst);
    drop(runtime);
    let restarted = page_runtime(&fixture, artifacts, Some(&scope));
    let replay = read(&restarted, &fixture, "run", "page", &artifact).await;
    assert!(
        matches!(replay, GuardedToolResult::Outcome {
        cached: true, outcome: ToolOutcome::Completed { output: ToolOutput::Inline(ref output) },
    } if output == &first),
        "{replay:?}"
    );
    assert_eq!(blobs.reads.load(Ordering::SeqCst), reads);
}

#[tokio::test]
async fn copied_foreign_producer_does_not_grant_artifact_access() {
    let scope = TestSessionScope::new().await;
    let fixture = fixture();
    let blobs = Arc::new(ProbeBlobStore::default());
    let runtime = page_runtime(&fixture, store(blobs.clone()), Some(&scope));
    let artifact = produce(&runtime, &fixture, "foreign", "private").await;
    scope
        .commit("session", "foreign", "private", &artifact)
        .await;
    rejected_with(
        read(&runtime, &fixture, "run", "copied", &artifact).await,
        "artifact_reference_invalid",
    );
    assert_eq!(blobs.reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn changed_blob_body_and_self_reported_checksum_cannot_replace_journal_digest() {
    let scope = TestSessionScope::new().await;
    let fixture = fixture();
    let blobs = Arc::new(ProbeBlobStore::default());
    let runtime = page_runtime(&fixture, store(blobs.clone()), Some(&scope));
    let artifact = produce(&runtime, &fixture, "run", "source").await;
    scope.commit("session", "run", "source", &artifact).await;
    blobs.corrupt.store(true, Ordering::SeqCst);
    for forge_checksum in [false, true] {
        blobs.forge_checksum.store(forge_checksum, Ordering::SeqCst);
        let id = format!("corrupt-{forge_checksum}");
        let result = read(&runtime, &fixture, "run", &id, &artifact).await;
        assert!(
            matches!(result, GuardedToolResult::Outcome {
            outcome: ToolOutcome::Failed { ref code, .. }, ..
        } if code == "artifact_resolve_failed"),
            "{result:?}"
        );
    }
}

#[tokio::test]
async fn reused_blob_reference_with_conflicting_committed_metadata_is_rejected() {
    let scope = TestSessionScope::new().await;
    let fixture = fixture();
    let blobs = Arc::new(ProbeBlobStore {
        fixed_ref: Some(BlobId::new("reused-reference")),
        ..Default::default()
    });
    let runtime = page_runtime(&fixture, store(blobs.clone()), Some(&scope));
    let first = produce(&runtime, &fixture, "run", "first").await;
    scope.commit("session", "run", "first", &first).await;
    fs::write(fixture.file(), "// different content\n".repeat(150)).unwrap();
    let second = produce(&runtime, &fixture, "run", "second").await;
    assert_eq!(first.artifact.artifact_ref, second.artifact.artifact_ref);
    assert_ne!(first.artifact.digest, second.artifact.digest);
    scope.commit("session", "run", "second", &second).await;
    rejected_with(
        read(&runtime, &fixture, "run", "ambiguous", &first).await,
        "artifact_reference_invalid",
    );
    assert_eq!(blobs.reads.load(Ordering::SeqCst), 0);
}

use orchestral_core::io::{
    BlobHead, BlobId, BlobIoError, BlobMeta, BlobRead, BlobStore, BlobWriteRequest,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default)]
struct ProbeBlobStore {
    inner: InMemoryBlobStore,
    reads: AtomicUsize,
    corrupt: AtomicBool,
    forge_checksum: AtomicBool,
    fixed_ref: Option<BlobId>,
}

#[async_trait]
impl BlobStore for ProbeBlobStore {
    async fn write(&self, request: BlobWriteRequest) -> Result<BlobMeta, BlobIoError> {
        let mut meta = self.inner.write(request).await?;
        if let Some(id) = &self.fixed_ref {
            meta.id = id.clone();
        }
        Ok(meta)
    }
    async fn read(&self, id: &BlobId) -> Result<BlobRead, BlobIoError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let mut read = self.inner.read(id).await?;
        if self.corrupt.load(Ordering::SeqCst) {
            let bytes = vec![b'x'; read.meta.byte_size as usize];
            if self.forge_checksum.load(Ordering::SeqCst) {
                read.meta.checksum_sha256 = Some(Digest::sha256(&bytes).to_string());
            }
            read.body = Box::pin(futures_util::stream::once(async move { Ok(bytes.into()) }));
        }
        Ok(read)
    }
    async fn head(&self, id: &BlobId) -> Result<BlobHead, BlobIoError> {
        self.inner.head(id).await
    }
    async fn delete(&self, id: &BlobId) -> Result<bool, BlobIoError> {
        self.inner.delete(id).await
    }
}
