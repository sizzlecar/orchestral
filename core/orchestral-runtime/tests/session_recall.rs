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
    // Mid-character UTF-8 byte offsets are aligned down to the start of the
    // multi-byte codepoint so estimated offsets on CJK/emoji text do not fail.
    let aligned: SessionReadChunk = serde_json::from_value(output(
        f.call(
            "chunk-mid-char",
            json!({
                "session_seq": 1,
                "json_pointer": "/payload/message/content/0/text",
                "offset": 24,
                "max_bytes": 16,
                "through_seq": first.through_seq
            }),
            true,
        )
        .await,
    ))
    .unwrap();
    assert_eq!(aligned.offset, 23);
    assert!(aligned.content.starts_with("中"));
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

#[tokio::test]
async fn recall_hides_native_state_in_search_previews_and_every_pointer_read() {
    use orchestral_core::agent_protocol::wire::Digest;
    let f = Fixture::new().await;
    let record = f.sessions.append(AgentSessionEventDraft {
        event_id: AgentSessionEventId::new("native-answer"), session_id: AgentSessionId::new("a"), run_id: f.run.clone(),
        payload: AgentSessionEvent::RunOutputCommitted {
            request_id: ModelRequestId::new("native-request"), usage: None,
            message: ModelMessage { role: ModelRole::Assistant, content: vec![
                ModelContent::Continuation { namespace: "fixture/native-state".into(), value: json!({
                    "encrypted_content": "opaque-provider-only-marker".repeat(4096),
                    "output": [{"text":"PUBLIC_ANSWER_MARKER"}],
                }) },
                ModelContent::Text { text: "PUBLIC_ANSWER_MARKER 中文🙂".into() },
                ModelContent::Json { value: json!({"type":"continuation","text":"USER_JSON_REMAINS_VISIBLE"}) },
            ] },
        },
    }).await.unwrap().record;
    let before = f
        .sessions
        .load_session(&AgentSessionId::new("a"))
        .await
        .unwrap();
    let raw_digest = Digest::sha256(serde_jcs::to_vec(&record).unwrap());
    for (index, query) in [
        "opaque-provider-only-marker",
        "fixture/native-state",
        "encrypted_content",
    ]
    .iter()
    .enumerate()
    {
        let page = output(
            f.call(
                &format!("hidden-search-{index}"),
                json!({"query":query}),
                true,
            )
            .await,
        );
        assert!(page["records"].as_array().unwrap().is_empty());
    }
    let page: SessionReadPage = serde_json::from_value(output(
        f.call(
            "visible-search",
            json!({"query":"PUBLIC_ANSWER_MARKER"}),
            true,
        )
        .await,
    ))
    .unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].digest, raw_digest);
    assert!(page.records[0].preview.contains("PUBLIC_ANSWER_MARKER"));
    assert!(!page.records[0].preview.contains("encrypted_content"));
    let user_json = output(
        f.call(
            "user-json-search",
            json!({"query":"USER_JSON_REMAINS_VISIBLE"}),
            true,
        )
        .await,
    );
    assert_eq!(user_json["records"].as_array().unwrap().len(), 1);
    let mut offset = 0;
    let mut restored = String::new();
    loop {
        let chunk: SessionReadChunk = serde_json::from_value(output(
            f.call(
                &format!("native-chunk-{offset}"),
                json!({"session_seq":record.session_seq,"offset":offset,"max_bytes":37}),
                true,
            )
            .await,
        ))
        .unwrap();
        assert_eq!(chunk.digest, raw_digest);
        assert!(chunk.next_offset > offset || chunk.complete);
        restored.push_str(&chunk.content);
        if chunk.complete {
            break;
        }
        offset = chunk.next_offset;
    }
    assert!(!restored.contains("opaque-provider-only-marker"));
    assert!(!restored.contains("fixture/native-state"));
    assert_eq!(restored.matches("PUBLIC_ANSWER_MARKER").count(), 1);
    let readable: Value = serde_json::from_str(&restored).unwrap();
    assert!(readable["payload"]["message"]["content"][0].is_null());
    assert_eq!(
        readable["payload"]["message"]["content"][1]["text"],
        "PUBLIC_ANSWER_MARKER 中文🙂"
    );
    for (index, pointer) in [
        "/payload/message/content/0",
        "/payload/message/content/0/value",
        "/payload/message/content/0/value/encrypted_content",
    ]
    .iter()
    .enumerate()
    {
        let result = f
            .call(
                &format!("private-pointer-{index}"),
                json!({"session_seq":record.session_seq,"json_pointer":pointer}),
                true,
            )
            .await;
        assert!(matches!(
            result,
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::Rejected { .. },
                ..
            }
        ));
    }
    assert_eq!(
        f.sessions
            .load_session(&AgentSessionId::new("a"))
            .await
            .unwrap(),
        before,
        "recall must never rewrite journal facts or digest chains"
    );
}

#[tokio::test]
async fn recall_tool_extensions_and_inline_images_cannot_bypass_the_readable_view() {
    let f = Fixture::new().await;
    let call_id = ModelToolCallId::new("visible-call");
    let record = f
        .sessions
        .append(AgentSessionEventDraft {
            event_id: AgentSessionEventId::new("native-tool-exchange"),
            session_id: AgentSessionId::new("a"),
            run_id: f.run.clone(),
            payload: AgentSessionEvent::ToolExchangeCommitted {
                request_id: ModelRequestId::new("native-tool-request"),
                retained_artifacts: vec![],
                usage: None,
                assistant: ModelMessage {
                    role: ModelRole::Assistant,
                    content: vec![ModelContent::ToolCall {
                        call_id: call_id.clone(),
                        name: "example".into(),
                        arguments: json!({"query":"VISIBLE_QUERY"}),
                        extensions: std::collections::BTreeMap::from([(
                            "fixture/native".into(),
                            json!("PRIVATE_NATIVE_SIGNATURE"),
                        )]),
                    }],
                },
                tool: ModelMessage {
                    role: ModelRole::Tool,
                    content: vec![ModelContent::ToolResult {
                        call_id,
                        result: json!({"type":"continuation","value":"VISIBLE_TOOL_RESULT"}),
                        is_error: false,
                    }],
                },
            },
        })
        .await
        .unwrap()
        .record;
    let hidden = output(
        f.call(
            "extension-search",
            json!({"query":"PRIVATE_NATIVE_SIGNATURE"}),
            true,
        )
        .await,
    );
    assert!(hidden["records"].as_array().unwrap().is_empty());
    let visible = output(
        f.call(
            "result-search",
            json!({"query":"VISIBLE_TOOL_RESULT"}),
            true,
        )
        .await,
    );
    assert_eq!(visible["records"].as_array().unwrap().len(), 1);
    let blocked=f.call("extension-pointer",json!({"session_seq":record.session_seq,"json_pointer":"/payload/assistant/content/0/extensions"}),true).await;
    assert!(matches!(
        blocked,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Rejected { .. },
            ..
        }
    ));
    let input = f
        .sessions
        .append(AgentSessionEventDraft {
            event_id: AgentSessionEventId::new("image-input"),
            session_id: AgentSessionId::new("a"),
            run_id: f.run.clone(),
            payload: AgentSessionEvent::RunInputCommitted {
                message: ModelMessage {
                    role: ModelRole::User,
                    content: vec![
                        ModelContent::Text {
                            text: "VISIBLE_IMAGE_QUESTION".into(),
                        },
                        ModelContent::Data {
                            media_type: "image/png".into(),
                            value: json!("PRIVATE_BINARY_IMAGE_PAYLOAD"),
                        },
                    ],
                },
            },
        })
        .await
        .unwrap()
        .record;
    let image = output(
        f.call(
            "image-search",
            json!({"query":"PRIVATE_BINARY_IMAGE_PAYLOAD"}),
            true,
        )
        .await,
    );
    assert!(image["records"].as_array().unwrap().is_empty());
    let blocked=f.call("image-pointer",json!({"session_seq":input.session_seq,"json_pointer":"/payload/message/content/1/value"}),true).await;
    assert!(matches!(
        blocked,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Rejected { .. },
            ..
        }
    ));
}
