use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use orchestral_core::agent_protocol::{
    reference::AgentRunStatus,
    wire::{AgentSessionId, Content, ProviderBindingRef},
};
use orchestral_core::agent_session::{
    AgentSessionEvent, AgentSessionJournalStore, InMemoryAgentSessionJournalStore,
};
use orchestral_core::model_protocol::{
    ModelBackend, ModelCapabilities, ModelDescriptor, ModelError, ModelEvent, ModelEventId,
    ModelFinishReason, ModelRequest, ModelStream, ModelStreamEvent,
};
use orchestral_runtime::{
    AgentClient, AgentController, GenericAgentConfig, InternalGenericAgentProvider,
    JsonSizeTokenMeter,
};

struct ReadThenReplaceModel {
    starts: AtomicUsize,
}

#[async_trait]
impl ModelBackend for ReadThenReplaceModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "read-then-replace".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                tool_calls: true,
                ..Default::default()
            },
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        let round = self.starts.fetch_add(1, Ordering::SeqCst);
        let events = match round {
            0 => tool_call("read", "file_read", json!({"path":"source.rs"})),
            1 => {
                assert!(request.messages.iter().flat_map(|message| &message.content).any(|content| matches!(content, ModelContent::ToolResult {call_id,result,is_error:false} if call_id.as_str()=="read" && result["content"] == "old\n")));
                tool_call(
                    "write",
                    "file_write",
                    replace("run", "write", "new\n").arguments,
                )
            }
            2 => {
                assert!(request.messages.iter().flat_map(|message| &message.content).any(|content| matches!(content, ModelContent::ToolResult {call_id,is_error:false,..} if call_id.as_str()=="write")));
                let call = request
                    .messages
                    .iter()
                    .flat_map(|message| &message.content)
                    .find_map(|content| match content {
                        ModelContent::ToolCall {
                            call_id, arguments, ..
                        } if call_id.as_str() == "write" => Some(arguments),
                        _ => None,
                    })
                    .unwrap();
                assert_eq!(call, &replace("run", "write", "new\n").arguments);
                vec![
                    ModelEvent::TextDelta {
                        delta: "Updated and checked the file.".to_owned(),
                    },
                    ModelEvent::Finish {
                        reason: ModelFinishReason::Stop,
                    },
                ]
            }
            _ => panic!("unexpected model retry"),
        };
        Ok(Box::pin(futures_util::stream::iter(
            events.into_iter().enumerate().map(move |(index, payload)| {
                Ok(ModelStreamEvent {
                    request_id: request.request_id.clone(),
                    event_id: ModelEventId::new(format!("round-{round}-{index}")),
                    sequence: index as u64 + 1,
                    payload,
                })
            }),
        )))
    }
}

fn tool_call(id: &str, name: &str, arguments: Value) -> Vec<ModelEvent> {
    let call_id = ModelToolCallId::new(id);
    vec![
        ModelEvent::ToolCallStart {
            call_id: call_id.clone(),
            name: name.to_owned(),
            extensions: Default::default(),
        },
        ModelEvent::ToolCallArgumentsDelta {
            call_id: call_id.clone(),
            delta: arguments.to_string(),
        },
        ModelEvent::ToolCallEnd { call_id },
        ModelEvent::Finish {
            reason: ModelFinishReason::ToolCalls,
        },
    ]
}

#[tokio::test]
async fn generic_model_tool_model_flow_resolves_read_without_changing_canonical_arguments() {
    let fixture = Fixture::new(ApprovalPolicy::NotRequired, 64 * 1024);
    fs::write(fixture.file(), "old\n").unwrap();
    let runtime = Arc::new(fixture.runtime());
    let model = Arc::new(ReadThenReplaceModel {
        starts: AtomicUsize::new(0),
    });
    let journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools_and_session_journal(
            model.clone(),
            GenericAgentConfig::new("provider", "agent"),
            runtime.clone(),
            fixture.grant(),
            journal.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .unwrap(),
    );
    let controller = Arc::new(
        AgentController::new(provider, ProviderBindingRef::new("observed-write")).unwrap(),
    );
    let session = AgentSessionId::new("session");
    let client = AgentClient::new(controller.clone(), session.clone());
    let handle = client
        .start_with_run_id(
            RunId::new("run"),
            vec![Content::text("Read the file and update it.")],
        )
        .await
        .unwrap();
    let view = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        controller.wait_for_terminal(handle.run_id()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert_eq!(model.starts.load(Ordering::SeqCst), 3);
    assert_eq!(fs::read(fixture.file()).unwrap(), b"new\n");
    let records = journal.load_session(&session).await.unwrap();
    let write = records
        .iter()
        .filter_map(|record| match &record.payload {
            AgentSessionEvent::ToolExchangeCommitted { assistant, .. } => Some(assistant),
            _ => None,
        })
        .flat_map(|assistant| &assistant.content)
        .find_map(|content| match content {
            ModelContent::ToolCall {
                call_id, arguments, ..
            } if call_id.as_str() == "write" => Some(arguments),
            _ => None,
        })
        .unwrap();
    assert_eq!(write, &replace("run", "write", "new\n").arguments);
    let prepared = runtime
        .inspect_effect(&ToolEffectKey::new(
            RunId::new("run"),
            ToolCallId::new("write"),
        ))
        .await
        .unwrap()
        .unwrap()
        .prepared;
    assert_eq!(&prepared.invocation.arguments, write);
    assert_eq!(
        prepared.execution_invocation().arguments["expected_digest"],
        json!(Digest::sha256(b"old\n"))
    );
}
