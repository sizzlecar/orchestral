use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use orchestral_core::agent_protocol::{
    spi::InMemoryAgentJournalStore,
    wire::{AgentSessionId, ProviderBindingRef, RunId},
};
use orchestral_core::agent_session::{
    AgentSessionEvent, AgentSessionEventDraft, AgentSessionEventId, AgentSessionJournalStore,
    InMemoryAgentSessionJournalStore,
};
use orchestral_core::model_protocol::*;
use orchestral_core::session_recall::{SessionReadChunk, SessionReadPage};
use orchestral_core::tool_protocol::*;
use orchestral_runtime::{
    tools::{guarded_session_read_descriptor, GuardedSessionReadExecutor},
    AgentClient, AgentController, GenericAgentConfig, GuardedToolResult, GuardedToolRuntime,
    InternalGenericAgentProvider, JsonSizeTokenMeter,
};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

struct Model;

#[async_trait]
impl ModelBackend for Model {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "recall-test".into(),
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
                    delta: "recorded answer".into(),
                },
                ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            ]
            .into_iter()
            .enumerate()
            .map(move |(i, payload)| {
                Ok(ModelStreamEvent {
                    request_id: request.request_id.clone(),
                    event_id: ModelEventId::new(format!("e-{i}")),
                    sequence: i as u64 + 1,
                    payload,
                })
            }),
        )))
    }
}

struct Fixture {
    runtime: GuardedToolRuntime<InMemoryApprovalCapabilityStore>,
    bounds: ToolPolicyBounds,
    sessions: Arc<InMemoryAgentSessionJournalStore>,
    run: RunId,
}

impl Fixture {
    async fn new() -> Self {
        let runs = Arc::new(InMemoryAgentJournalStore::default());
        let sessions = Arc::new(InMemoryAgentSessionJournalStore::default());
        let provider = InternalGenericAgentProvider::new_with_session_journal(
            Arc::new(Model),
            GenericAgentConfig::new("recall-agent", "test"),
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
        let client = AgentClient::new(controller.clone(), AgentSessionId::new("a"));
        let handle = client
            .start_text("Keep names unchanged. 中文🙂".repeat(60))
            .await
            .unwrap();
        handle.wait_until_blocked().await.unwrap();
        let other = AgentClient::new(controller, AgentSessionId::new("b"))
            .start_text("OTHER_SESSION_PRIVATE_VALUE")
            .await
            .unwrap();
        other.wait_until_blocked().await.unwrap();
        let bounds = ToolPolicyBounds {
            allowed_effects: BTreeSet::from([EffectScope::SessionRead]),
            approval: ApprovalPolicy::NotRequired,
            max_output_bytes: Some(4096),
            ..Default::default()
        };
        let runtime = GuardedToolRuntime::new(
            HostToolPolicy {
                bounds: bounds.clone(),
            },
            HostApprovalVerifier::new(
                b"0123456789abcdef0123456789abcdef",
                InMemoryApprovalCapabilityStore::default(),
            )
            .unwrap(),
        )
        .unwrap();
        runtime
            .register(
                guarded_session_read_descriptor(ToolRestriction {
                    bounds: bounds.clone(),
                }),
                Arc::new(GuardedSessionReadExecutor::new(runs, sessions.clone())),
            )
            .unwrap();
        Self {
            runtime,
            bounds,
            sessions,
            run: handle.run_id().clone(),
        }
    }

    async fn call(&self, id: &str, arguments: Value, allowed: bool) -> GuardedToolResult {
        let mut bounds = self.bounds.clone();
        if !allowed {
            bounds.allowed_effects.clear();
        }
        self.runtime
            .invoke(
                ToolInvocation {
                    run_id: self.run.clone(),
                    call_id: ToolCallId::new(id),
                    tool_id: ToolId::new("orchestral/session_read/v1"),
                    arguments,
                },
                RunToolGrant { bounds },
                None,
                CancellationToken::new(),
            )
            .await
    }

    async fn append(&self) {
        self.sessions
            .append(AgentSessionEventDraft {
                event_id: AgentSessionEventId::new("late-input"),
                session_id: AgentSessionId::new("a"),
                run_id: self.run.clone(),
                payload: AgentSessionEvent::RunInputCommitted {
                    message: ModelMessage::text(ModelRole::User, "newly appended user correction"),
                },
            })
            .await
            .unwrap();
    }
}

fn output(result: GuardedToolResult) -> Value {
    match result {
        GuardedToolResult::Outcome {
            outcome:
                ToolOutcome::Completed {
                    output: ToolOutput::Inline(value),
                },
            ..
        } => value,
        other => panic!("expected inline recall output: {other:?}"),
    }
}

#[tokio::test]
async fn recall_is_session_scoped_guarded_and_effect_replay_is_stable() {
    let f = Fixture::new().await;
    let private = output(
        f.call(
            "private",
            json!({"query": "OTHER_SESSION_PRIVATE_VALUE"}),
            true,
        )
        .await,
    );
    assert!(private["records"].as_array().unwrap().is_empty());
    let spoof = f.call("spoof", json!({"session_id": "b"}), true).await;
    assert!(!matches!(
        spoof,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed { .. },
            ..
        }
    ));
    let denied = f.call("denied", json!({}), false).await;
    assert!(!matches!(
        denied,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed { .. },
            ..
        }
    ));
    let first = output(f.call("cached", json!({}), true).await);
    f.append().await;
    let cached = f.call("cached", json!({}), true).await;
    assert!(matches!(
        cached,
        GuardedToolResult::Outcome { cached: true, .. }
    ));
    assert_eq!(output(cached), first);
}

#[tokio::test]
async fn recall_pages_an_immutable_prefix_and_reassembles_exact_unicode_fields() {
    let f = Fixture::new().await;
    let first: SessionReadPage =
        serde_json::from_value(output(f.call("page-1", json!({"limit": 1}), true).await)).unwrap();
    assert_eq!(first.records.len(), 1);
    assert!(!first.complete);
    f.append().await;
    let second: SessionReadPage = serde_json::from_value(output(f.call("page-2", json!({"after_seq": first.next_after_seq, "through_seq": first.through_seq, "limit": 1}), true).await)).unwrap();
    assert!(second.complete);
    assert_eq!(second.records[0].session_seq, 2);
    let mut offset = 0;
    let mut content = String::new();
    loop {
        let chunk: SessionReadChunk = serde_json::from_value(output(f.call(&format!("chunk-{offset}"), json!({"session_seq": 1, "json_pointer": "/payload/message/content/0/text", "offset": offset, "max_bytes": 37, "through_seq": first.through_seq}), true).await)).unwrap();
        assert_eq!(chunk.digest, first.records[0].digest);
        content.push_str(&chunk.content);
        if chunk.complete {
            break;
        }
        assert!(chunk.next_offset > offset);
        offset = chunk.next_offset;
    }
    assert_eq!(
        serde_json::from_str::<String>(&content).unwrap(),
        "Keep names unchanged. 中文🙂".repeat(60)
    );
    let invalid = f
        .call(
            "bad-path",
            json!({"session_seq": 1, "json_pointer": "/not/a/field"}),
            true,
        )
        .await;
    assert!(matches!(
        invalid,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Rejected { .. },
            ..
        }
    ));
}
