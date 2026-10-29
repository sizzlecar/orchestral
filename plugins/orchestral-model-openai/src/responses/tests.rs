use super::*;
use orchestral_model_protocol_testkit::{
    ModelConformanceSuite, ModelFixtureFactory, ModelFixtureResponse, ModelFixtureScenario,
    PacedSseServer,
};
use std::sync::Arc;

fn backend(endpoint: &str) -> OpenAiCompatibleBackend {
    OpenAiCompatibleBackend::new(OpenAiCompatibleConfig {
        backend_id: "responses-fixture".into(),
        endpoint: endpoint.into(),
        api_key: String::new(),
        model: "fixture-model".into(),
        temperature: 0.2,
        default_max_output_tokens: 128,
        max_context_tokens: Some(8192),
        timeout: Duration::from_secs(1),
        structured_output: true,
        max_buffered_events: 128,
    })
    .unwrap()
    .with_api(OpenAiApi::Responses)
}

fn request() -> ModelRequest {
    crate::tests::request()
}
fn text_item(text: &str) -> Value {
    json!({"type":"message","id":"msg-1","role":"assistant","status":"completed","content":[{"type":"output_text","text":text,"annotations":[]}]})
}
fn call_item(id: &str, call_id: &str, value: &str) -> Value {
    json!({"type":"function_call","id":id,"call_id":call_id,"name":"echo","arguments":value,"status":"completed"})
}
fn response(output: Vec<Value>) -> Value {
    json!({"id":"resp-1","status":"completed","output":output,"usage":{"input_tokens":8,"output_tokens":3}})
}
fn frame(value: Value) -> Vec<u8> {
    format!(
        "event: {}\r\ndata: {value}\r\n\r\n",
        value["type"].as_str().unwrap()
    )
    .into_bytes()
}
fn terminal(response: Value) -> Value {
    let kind = format!("response.{}", response["status"].as_str().unwrap());
    json!({"type":kind,"response":response})
}
async fn decode(bytes: Vec<u8>) -> Vec<Result<ModelStreamEvent, ModelError>> {
    event_stream(
        request(),
        stream::iter(
            bytes
                .chunks(1)
                .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
                .collect::<Vec<_>>(),
        )
        .boxed(),
        CancellationToken::new(),
        128,
        OpenAiApi::Responses,
    )
    .collect()
    .await
}

struct Fixture;
impl ModelFixtureFactory for Fixture {
    fn adapter_name(&self) -> &'static str {
        "openai-responses"
    }
    fn backend(
        &self,
        _: ModelFixtureScenario,
        endpoint: &str,
    ) -> Result<Arc<dyn ModelBackend>, ModelError> {
        Ok(Arc::new(backend(endpoint)))
    }
    fn response(&self, scenario: ModelFixtureScenario) -> ModelFixtureResponse {
        let output = match scenario {
            ModelFixtureScenario::Text => vec![text_item("hello world")],
            ModelFixtureScenario::Tool => {
                vec![call_item("fc-1", "call-1", "{\"value\":\"hello\"}")]
            }
            ModelFixtureScenario::Malformed => {
                return ModelFixtureResponse::Complete(b"data: not-json\n\n".to_vec())
            }
            ModelFixtureScenario::Stalled => return ModelFixtureResponse::Stall,
        };
        let mut response = response(output);
        if scenario == ModelFixtureScenario::Text {
            response["usage"] = json!({"input_tokens":7,"output_tokens":2});
        }
        ModelFixtureResponse::Complete(frame(terminal(response)))
    }
}

#[tokio::test]
async fn passes_shared_model_protocol_conformance() {
    let report = ModelConformanceSuite::default().run(&Fixture).await;
    assert!(report.is_conformant(), "{:#?}", report.results());
}

#[test]
fn requests_use_responses_fields_and_preserve_host_schemas() {
    let mut request = request();
    request.output_schema =
        Some(json!({"type":"object","properties":{},"additionalProperties":false}));
    request.tools[0].input_schema = json!({"type":"object","properties":{"value":{"type":"string"}},"oneOf":[{"required":["value"]}]});
    let adapter = backend("http://127.0.0.1:1/v1")
        .with_reasoning_preference(ReasoningPreference::XHigh)
        .unwrap();
    let wire = adapter.build_request_body(&request).unwrap();
    assert_eq!(wire["reasoning"], json!({"effort":"xhigh"}));
    assert_eq!(wire["max_output_tokens"], 128);
    assert_eq!(wire["store"], false);
    assert_eq!(wire["tools"][0]["strict"], false);
    assert_eq!(wire["tools"][0]["name"], "echo");
    assert!(wire["tools"][0]["parameters"].get("oneOf").is_none());
    assert_eq!(
        wire["text"]["format"]["schema"],
        request.output_schema.clone().unwrap()
    );
    for key in [
        "messages",
        "max_tokens",
        "stream_options",
        "reasoning_effort",
        "temperature",
        "response_format",
    ] {
        assert!(wire.get(key).is_none(), "{key}");
    }
    assert!(request.tools[0].input_schema.get("oneOf").is_some());
    assert!(adapter
        .validate_reasoning_preference(&ReasoningPreference::On)
        .is_err());
    let base = adapter.meter_descriptor();
    assert_ne!(
        base,
        backend("http://127.0.0.1:1/v1")
            .with_api(OpenAiApi::ChatCompletions)
            .meter_descriptor()
    );
    let sampled = backend("http://127.0.0.1:1/v1")
        .with_reasoning_preference(ReasoningPreference::from_effort("none").unwrap())
        .unwrap();
    assert_eq!(
        sampled.build_request_body(&request).unwrap()["temperature"],
        json!(0.2_f32)
    );
    let unsupported = sampled
        .with_sampling(OpenAiSamplingConfig {
            top_k: Some(10),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        unsupported.build_request_body(&request).unwrap_err().code,
        ModelErrorCode::Unsupported
    );
}

#[tokio::test]
async fn streamed_text_is_not_duplicated_by_done_or_terminal_snapshots() {
    let item = text_item("你好 world");
    let events = vec![
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"msg-1","type":"message","role":"assistant","content":[]}}),
        json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg-1","delta":"你好 "}),
        json!({"type":"response.output_text.done","output_index":0,"content_index":0,"item_id":"msg-1","text":"你好 world"}),
        json!({"type":"response.output_item.done","output_index":0,"item":item}),
        terminal(response(vec![item.clone()])),
    ];
    let events = decode(events.into_iter().flat_map(frame).collect())
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let text = events
        .iter()
        .filter_map(|event| match &event.payload {
            ModelEvent::TextDelta { delta } => Some(delta.as_str()),
            _ => None,
        })
        .collect::<String>();
    assert_eq!(text, "你好 world");
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event.payload, ModelEvent::Finish { .. }))
            .count(),
        1
    );
    for (index, event) in events.iter().enumerate() {
        event
            .validate_for(&request().request_id, index as u64 + 1)
            .unwrap();
    }
}

#[test]
fn images_and_imported_assistant_history_use_responses_input_types() {
    let mut request = request();
    request.messages = vec![
        ModelMessage::text(ModelRole::System, "Describe attached images"),
        ModelMessage {
            role: ModelRole::User,
            content: vec![
                ModelContent::Text {
                    text: "Inspect".into(),
                },
                ModelContent::Data {
                    media_type: "image/png".into(),
                    value: json!("AQID"),
                },
            ],
        },
        ModelMessage::text(ModelRole::Assistant, "An earlier imported answer"),
    ];
    let wire = backend("http://127.0.0.1:1/v1")
        .build_request_body(&request)
        .unwrap();
    assert_eq!(wire["input"][0]["role"], "system");
    assert_eq!(
        wire["input"][1]["content"][0],
        json!({"type":"input_text","text":"Inspect"})
    );
    assert_eq!(
        wire["input"][1]["content"][1],
        json!({"type":"input_image","image_url":"data:image/png;base64,AQID"})
    );
    assert_eq!(wire["input"][2]["content"][0]["type"], "output_text");
}

#[tokio::test]
async fn parallel_calls_and_encrypted_reasoning_round_trip_without_visible_duplicates() {
    let native = vec![
        json!({"type":"reasoning","id":"rs-1","summary":[],"encrypted_content":"opaque-ciphertext"}),
        call_item("fc-1", "call-a", "{\"value\":1}"),
        call_item("fc-2", "call-b", "{\"value\":2}"),
    ];
    let mut frames = Vec::new();
    for (index, item) in native.iter().enumerate() {
        let mut start = item.clone();
        if start["type"] == "function_call" {
            start["arguments"] = json!("");
        }
        frames.push(json!({"type":"response.output_item.added","output_index":index,"item":start}));
    }
    for (index, id, args) in [(2, "fc-2", "{\"value\":2}"), (1, "fc-1", "{\"value\":1}")] {
        frames.push(json!({"type":"response.function_call_arguments.delta","output_index":index,"item_id":id,"delta":args}));
        frames.push(
            json!({"type":"response.output_item.done","output_index":index,"item":native[index]}),
        );
    }
    frames.push(terminal(response(native.clone())));
    let events = decode(frames.into_iter().flat_map(frame).collect())
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut content = Vec::new();
    let mut calls = BTreeMap::new();
    for event in events {
        match event.payload {
            ModelEvent::ToolCallStart {
                call_id,
                name,
                extensions,
            } => {
                calls.insert(call_id, (name, extensions, String::new()));
            }
            ModelEvent::ToolCallArgumentsDelta { call_id, delta } => {
                calls.get_mut(&call_id).unwrap().2.push_str(&delta)
            }
            ModelEvent::Continuation { namespace, value } => {
                content.push(ModelContent::Continuation { namespace, value })
            }
            ModelEvent::TextDelta { .. } => panic!("native reasoning leaked into public text"),
            ModelEvent::Finish { reason } => assert_eq!(reason, ModelFinishReason::ToolCalls),
            _ => {}
        }
    }
    let mut results = Vec::new();
    for (call_id, (name, extensions, arguments)) in calls {
        results.push(ModelContent::ToolResult {
            call_id: call_id.clone(),
            result: json!({"content":"line1\nline2"}),
            is_error: false,
        });
        content.push(ModelContent::ToolCall {
            call_id,
            name,
            extensions,
            arguments: serde_json::from_str(&arguments).unwrap(),
        });
    }
    let mut request = request();
    request.messages.extend([
        ModelMessage {
            role: ModelRole::Assistant,
            content,
        },
        ModelMessage {
            role: ModelRole::Tool,
            content: results,
        },
    ]);
    let restored: ModelRequest =
        serde_json::from_slice(&serde_json::to_vec(&request).unwrap()).unwrap();
    for format in [
        OpenAiToolResultFormat::Text,
        OpenAiToolResultFormat::Json,
        OpenAiToolResultFormat::Yaml,
        OpenAiToolResultFormat::TextParts,
    ] {
        let wire = backend("http://127.0.0.1:1/v1")
            .with_tool_result_format(format)
            .build_request_body(&restored)
            .unwrap();
        assert_eq!(&wire["input"].as_array().unwrap()[1..4], native.as_slice());
        let mut ids = wire["input"].as_array().unwrap()[4..]
            .iter()
            .map(|item| item["call_id"].as_str().unwrap())
            .collect::<Vec<_>>();
        ids.sort();
        assert_eq!(ids, ["call-a", "call-b"]);
        if format == OpenAiToolResultFormat::TextParts {
            assert_eq!(wire["input"][4]["output"][0]["type"], "input_text");
        }
    }
    let backend = backend("http://127.0.0.1:1/v1");
    let counted = backend
        .count_request_input(&restored.messages, &restored.tools)
        .unwrap();
    let bytes = serde_json::to_vec(&backend.build_request_body(&restored).unwrap())
        .unwrap()
        .len() as u64;
    assert!(counted >= bytes);
    let mut tampered = restored;
    if let ModelContent::ToolCall { arguments, .. } = &mut tampered.messages[1].content[1] {
        *arguments = json!({"value":999});
    }
    assert!(backend.build_request_body(&tampered).is_err());
}

#[tokio::test]
async fn cancellation_truncation_errors_and_conflicts_never_commit_continuation() {
    let prefix = frame(
        json!({"type":"response.output_item.added","output_index":0,"item":call_item("fc-1","call-1", "")}),
    );
    for tail in [
        Vec::new(),
        b"data: [DONE]\n\n".to_vec(),
        frame(json!({"type":"error","code":"server_error","message":"interrupted"})),
        frame(
            json!({"type":"response.function_call_arguments.delta","output_index":0,"item_id":"other","delta":"{}"}),
        ),
    ] {
        let events = decode([prefix.clone(), tail].concat()).await;
        assert!(events.iter().any(Result::is_err));
        assert!(!events.iter().any(|event| matches!(
            event,
            Ok(ModelStreamEvent {
                payload: ModelEvent::Finish { .. } | ModelEvent::Continuation { .. },
                ..
            })
        )));
    }
    let token = CancellationToken::new();
    let mut events = event_stream(
        request(),
        stream::iter([Ok(Bytes::from(prefix))])
            .chain(stream::pending())
            .boxed(),
        token.clone(),
        128,
        OpenAiApi::Responses,
    );
    assert!(matches!(
        events.next().await.unwrap().unwrap().payload,
        ModelEvent::ToolCallStart { .. }
    ));
    token.cancel();
    assert_eq!(
        events.next().await.unwrap().unwrap_err().code,
        ModelErrorCode::Cancelled
    );
    assert!(events.next().await.is_none());
    let mut decoder = Decoder::new(request().request_id, 128);
    decoder
        .handle_frame(
            &json!({"type":"response.output_item.added","output_index":0,"item":text_item("")})
                .to_string(),
        )
        .unwrap();
    decoder.handle_frame(&json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg-1","delta":"before"}).to_string()).unwrap();
    assert!(decoder
        .complete(&response(vec![text_item("after")]))
        .is_err());
}

#[test]
fn json_terminal_mapping_foreign_replay_and_output_limits_are_enforced() {
    for (status, detail, expected) in [
        ("incomplete", "max_output_tokens", ModelFinishReason::Length),
        (
            "incomplete",
            "content_filter",
            ModelFinishReason::ContentFilter,
        ),
    ] {
        let events = Decoder::new(request().request_id, 128)
            .complete(&json!({"status":status,"incomplete_details":{"reason":detail},"output":[]}))
            .unwrap();
        assert!(
            matches!(&events.last().unwrap(), ModelEvent::Finish { reason } if *reason == expected)
        );
    }
    for (code, expected) in [
        (
            "context_length_exceeded",
            ModelErrorCode::ContextLengthExceeded,
        ),
        ("rate_limit_exceeded", ModelErrorCode::RateLimited),
        ("invalid_api_key", ModelErrorCode::Authentication),
    ] {
        let error = Decoder::new(request().request_id, 128)
            .complete(&json!({"status":"failed","error":{"code":code,"message":"fixture"}}))
            .unwrap_err();
        assert_eq!(error.code, expected);
    }
    assert!(Decoder::new(request().request_id, 1)
        .complete(&response(vec![
            text_item("hi"),
            call_item("fc-1", "call-1", "{}")
        ]))
        .is_err());
    let mut request = request();
    request.messages.push(ModelMessage {
        role: ModelRole::Assistant,
        content: vec![ModelContent::Continuation {
            namespace: continuation::NAMESPACE.into(),
            value: json!("private"),
        }],
    });
    assert_eq!(
        backend("http://127.0.0.1:1/v1")
            .build_request_body(&request)
            .unwrap_err()
            .code,
        ModelErrorCode::Unsupported
    );
}

#[tokio::test]
async fn live_http_stream_survives_total_duration_beyond_idle_timeout() {
    let added = json!({"type":"response.output_item.added","output_index":0,"item":text_item("")});
    let delta = |text| json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg-1","delta":text});
    let server = PacedSseServer::start(
        vec![
            frame(added),
            frame(delta("hello ")),
            frame(delta("world")),
            frame(terminal(response(vec![text_item("hello world")]))),
        ],
        Duration::from_millis(80),
    )
    .await
    .unwrap();
    let mut backend = backend(server.endpoint());
    backend.config.timeout = Duration::from_millis(200);
    backend.client = Client::builder()
        .read_timeout(backend.config.timeout)
        .build()
        .unwrap();
    let events = backend
        .start(request(), CancellationToken::new())
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert!(events.into_iter().collect::<Result<Vec<_>, _>>().is_ok());
    server.finish().await.unwrap();
}
