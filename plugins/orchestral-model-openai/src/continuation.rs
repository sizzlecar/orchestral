//! Message-owned native reasoning. API aliases are normalized to the assistant
//! `reasoning_content` field; no model/template names or visible-text parsing.

use super::*;

pub(super) const NAMESPACE: &str = "openai-compatible/reasoning-content/v1";

#[derive(Default)]
pub(super) struct Reasoning {
    text: String,
    present: bool,
}

impl Reasoning {
    pub(super) fn append(&mut self, message: &Value) -> Result<(), ModelError> {
        let mut fragment = None;
        for field in ["reasoning_content", "reasoning", "reasoning_text"] {
            let Some(value) = message.get(field).filter(|value| !value.is_null()) else {
                continue;
            };
            let text = value
                .as_str()
                .ok_or_else(|| ModelError::protocol("OpenAI reasoning must be a string"))?;
            self.present = true;
            if text.is_empty() {
                continue;
            }
            if fragment.is_some_and(|previous| previous != text) {
                return Err(ModelError::protocol("conflicting OpenAI reasoning aliases"));
            }
            fragment = Some(text);
        }
        if let Some(fragment) = fragment {
            self.text.push_str(fragment);
        }
        Ok(())
    }

    pub(super) fn into_event(self) -> Option<ModelEvent> {
        self.present.then(|| ModelEvent::Continuation {
            namespace: NAMESPACE.to_owned(),
            value: Value::String(self.text),
        })
    }
}

pub(super) fn insert(
    message: &mut Map<String, Value>,
    namespace: &str,
    value: &Value,
) -> Result<(), ModelError> {
    if namespace != NAMESPACE {
        return Err(ModelError::new(
            ModelErrorCode::Unsupported,
            "OpenAI cannot replay this provider's message continuation",
        ));
    }
    if !value.is_string() || message.contains_key("reasoning_content") {
        return Err(ModelError::invalid_request(
            "invalid or duplicate OpenAI continuation",
        ));
    }
    message.insert("reasoning_content".to_owned(), value.clone());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_model_protocol_testkit::{run_live_text_smoke, PacedSseServer};

    fn request() -> ModelRequest {
        super::super::tests::request()
    }

    #[tokio::test]
    async fn native_reasoning_is_not_counted_as_live_smoke_public_text() {
        let server = PacedSseServer::start(vec![
            b"data: {\"choices\":[{\"delta\":{\"reasoning\":\"private native continuation\"}}]}\n\n".to_vec(),
            b"data: {\"choices\":[{\"delta\":{\"content\":\"answer\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n".to_vec(),
        ], Duration::from_millis(1)).await.unwrap();
        let backend = OpenAiCompatibleBackend::new(OpenAiCompatibleConfig {
            backend_id: "reasoning-http".into(),
            endpoint: server.endpoint().into(),
            api_key: String::new(),
            model: "fixture".into(),
            temperature: 0.0,
            default_max_output_tokens: 64,
            max_context_tokens: Some(8192),
            timeout: Duration::from_secs(1),
            structured_output: false,
            max_buffered_events: 128,
        })
        .unwrap();
        let report = run_live_text_smoke(&backend).await.unwrap();
        assert_eq!(report.text_bytes, "answer".len());
        assert_eq!(report.finish_reason, ModelFinishReason::Stop);
        server.finish().await.unwrap();
    }

    #[tokio::test]
    async fn parallel_reasoning_is_one_message_state_in_stream_and_json() {
        let reasoning = "  Ω\n\\n <tool_call>literal</tool_call>\t";
        for finish in ["tool_calls", "length", "content_filter"] {
            let first = json!({"choices": [{"delta": {
                "reasoning": "  Ω\n",
                "tool_calls": [
                    {"index": 0, "id": "a", "function": {"name": "echo", "arguments": "{\"value\":"}},
                    {"index": 1, "id": "b", "function": {"name": "echo", "arguments": "{\"value\":"}}
                ]
            }}]});
            let second = json!({"choices": [{"delta": {
                "reasoning": "\\n <tool_call>literal</tool_call>\t",
                "tool_calls": [
                    {"index": 1, "function": {"arguments": "2}"}},
                    {"index": 0, "function": {"arguments": "1}"}}
                ]
            }, "finish_reason": finish}]});
            let raw = format!("data: {first}\n\ndata: {second}\n\ndata: [DONE]\n\n");
            let events = openai_event_stream(
                request(),
                stream::iter(
                    raw.as_bytes()
                        .chunks(1)
                        .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
                        .collect::<Vec<_>>(),
                )
                .boxed(),
                CancellationToken::new(),
                128,
            )
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
            let json_events = parse_response(
                &request(),
                &json!({"choices": [{
                    "message": {"reasoning_content": reasoning, "tool_calls": [
                        {"id": "a", "function": {"name": "echo", "arguments": "{\"value\":1}"}},
                        {"id": "b", "function": {"name": "echo", "arguments": "{\"value\":2}"}}
                    ]}, "finish_reason": finish
                }]}),
            )
            .unwrap();
            for events in [events, json_events] {
                let mut contents = Vec::new();
                let mut calls = BTreeMap::new();
                for (index, event) in events.iter().enumerate() {
                    event
                        .validate_for(&request().request_id, index as u64 + 1)
                        .unwrap();
                    match &event.payload {
                        ModelEvent::TextDelta { .. } => panic!("reasoning leaked to visible text"),
                        ModelEvent::Continuation { namespace, value } => {
                            contents.push(ModelContent::Continuation {
                                namespace: namespace.clone(),
                                value: value.clone(),
                            })
                        }
                        ModelEvent::ToolCallStart {
                            call_id,
                            name,
                            extensions,
                        } => {
                            calls.insert(
                                call_id.clone(),
                                (name.clone(), extensions.clone(), String::new()),
                            );
                        }
                        ModelEvent::ToolCallArgumentsDelta { call_id, delta } => {
                            calls.get_mut(call_id).unwrap().2.push_str(delta)
                        }
                        _ => {}
                    }
                }
                assert_eq!(contents.len(), 1, "one state covers both parallel calls");
                for (call_id, (name, extensions, arguments)) in calls {
                    contents.push(ModelContent::ToolCall {
                        call_id,
                        name,
                        extensions,
                        arguments: serde_json::from_str(&arguments).unwrap(),
                    });
                }
                let message = ModelMessage {
                    role: ModelRole::Assistant,
                    content: contents,
                };
                message.validate().unwrap();
                let restored: ModelMessage =
                    serde_json::from_slice(&serde_json::to_vec(&message).unwrap()).unwrap();
                let wire = encode_messages(&[restored], OpenAiToolResultFormat::Json).unwrap();
                assert_eq!(wire[0]["reasoning_content"], reasoning);
                assert_eq!(wire[0]["content"], Value::Null);
                for (id, arguments) in [("a", "{\"value\":1}"), ("b", "{\"value\":2}")] {
                    let call = wire[0]["tool_calls"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|call| call["id"] == id)
                        .unwrap();
                    assert_eq!(call["function"]["arguments"], arguments);
                }
                assert!(
                    matches!(&events.last().unwrap().payload, ModelEvent::Finish { reason } if *reason == map_finish_reason(finish))
                );
            }
        }
    }

    #[tokio::test]
    async fn interrupted_reasoning_and_tool_fragments_never_commit_a_finish() {
        let prefix = "data: {\"choices\":[{\"delta\":{\"reasoning\":\"private\",\"tool_calls\":[{\"index\":0,\"id\":\"a\",\"function\":{\"name\":\"echo\",\"arguments\":\"{}\"}}]}}]}\n\n";
        for tail in [
            "",
            "data: [DONE]\n\n",
            "data: {\"error\":{\"message\":\"interrupted\"}}\n\n",
        ] {
            let events = openai_event_stream(
                request(),
                stream::iter([Ok(Bytes::from(format!("{prefix}{tail}")))]).boxed(),
                CancellationToken::new(),
                128,
            )
            .collect::<Vec<_>>()
            .await;
            assert!(events.iter().any(Result::is_err));
            assert!(!events.iter().any(|event| matches!(
                event,
                Ok(ModelStreamEvent {
                    payload: ModelEvent::Finish { .. } | ModelEvent::Continuation { .. },
                    ..
                })
            )));
        }
        let cancellation = CancellationToken::new();
        let mut events = openai_event_stream(
            request(),
            stream::iter([Ok(Bytes::from(prefix))])
                .chain(stream::pending())
                .boxed(),
            cancellation.clone(),
            128,
        );
        assert!(matches!(
            events.next().await.unwrap().unwrap().payload,
            ModelEvent::ToolCallStart { .. }
        ));
        cancellation.cancel();
        let rest = events.collect::<Vec<_>>().await;
        assert!(rest.iter().any(Result::is_err));
        assert!(!rest.iter().any(|event| matches!(
            event,
            Ok(ModelStreamEvent {
                payload: ModelEvent::Finish { .. } | ModelEvent::Continuation { .. },
                ..
            })
        )));
    }

    #[test]
    fn reasoning_only_length_and_foreign_continuation_remain_non_text() {
        let events = parse_response(&request(), &json!({"choices": [{"message": {"reasoning_content": "still considering"}, "finish_reason": "length"}]})).unwrap();
        assert!(matches!(
            &events[0].payload,
            ModelEvent::Continuation { .. }
        ));
        assert!(matches!(
            &events[1].payload,
            ModelEvent::Finish {
                reason: ModelFinishReason::Length
            }
        ));
        let truncated = parse_response(&request(), &json!({"choices": [{"message": {
            "reasoning_content": "not complete", "tool_calls": [{"id": "partial", "function": {"name": "echo", "arguments": "{\"value\":"}}]
        }, "finish_reason": "length"}]})).unwrap();
        assert!(matches!(
            truncated.last().unwrap().payload,
            ModelEvent::Finish {
                reason: ModelFinishReason::Length
            }
        ));
        for (namespace, value) in [
            ("another-provider/state", json!("private")),
            (NAMESPACE, json!({"bad": true})),
        ] {
            assert!(encode_messages(
                &[ModelMessage {
                    role: ModelRole::Assistant,
                    content: vec![ModelContent::Continuation {
                        namespace: namespace.into(),
                        value
                    }]
                }],
                OpenAiToolResultFormat::Json
            )
            .is_err());
        }
        let mut reasoning = Reasoning::default();
        assert!(reasoning
            .append(&json!({"reasoning": "a", "reasoning_content": "b"}))
            .is_err());
    }
}
