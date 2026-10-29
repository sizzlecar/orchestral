use std::{collections::BTreeMap, sync::Arc, time::Duration};

use futures_util::StreamExt;
use orchestral_core::model_protocol::{
    ModelBackend, ModelEvent, ModelMessage, ModelRequest, ModelRequestId, ModelRole,
    ModelToolDefinition,
};
use orchestral_core::tool_protocol::{ToolPolicyBounds, ToolRestriction};
use orchestral_model_openai::{OpenAiApi, OpenAiCompatibleBackend, OpenAiCompatibleConfig};
use orchestral_runtime::tools::{
    guarded_apply_patch_descriptor, guarded_file_edit_descriptor, guarded_file_read_descriptor,
    guarded_file_write_descriptor,
};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn builtin_tool_schemas_reach_openai_as_objects_and_keep_host_validation() {
    check_builtin_schemas(OpenAiApi::ChatCompletions).await;
    check_builtin_schemas(OpenAiApi::Responses).await;
}

async fn check_builtin_schemas(api: OpenAiApi) {
    let restriction = ToolRestriction {
        bounds: ToolPolicyBounds::default(),
    };
    let descriptors = [
        guarded_file_edit_descriptor(restriction.clone()),
        guarded_apply_patch_descriptor(restriction.clone()),
        guarded_file_read_descriptor(restriction.clone()),
        guarded_file_write_descriptor(restriction),
    ];
    let edit = &descriptors[0].model_schema;
    let single = json!({"path":"example.txt","old_text":"before","new_text":"after"});
    let batch = json!({"path":"example.txt","edits":[{"old_text":"before","new_text":"after"}]});
    edit.validate_arguments(&single).unwrap();
    edit.validate_arguments(&batch).unwrap();
    let mut mixed = single.clone();
    mixed["edits"] = batch["edits"].clone();
    assert!(edit.validate_arguments(&mixed).is_err());
    assert!(edit
        .validate_arguments(&json!({"path":"example.txt"}))
        .is_err());

    let expected = Arc::new(
        descriptors
            .iter()
            .map(|d| d.model_schema.clone())
            .collect::<Vec<_>>(),
    );
    let expected_http = expected.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
    let route = match api {
        OpenAiApi::ChatCompletions => "/v1/chat/completions",
        OpenAiApi::Responses => "/v1/responses",
    };
    let app = axum::Router::new().route(route, axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
        let expected = expected_http.clone();
        async move {
            let tools = body["tools"].as_array().unwrap();
            assert_eq!(tools.len(), expected.len());
            for (tool, canonical) in tools.iter().zip(expected.iter()) {
                let function = match api {
                    OpenAiApi::ChatCompletions => &tool["function"],
                    OpenAiApi::Responses => {
                        assert_eq!(tool["strict"], false);
                        tool
                    }
                };
                let parameters = &function["parameters"];
                assert_eq!(function["name"], canonical.name);
                assert_eq!(parameters["type"], "object");
                for forbidden in ["oneOf","anyOf","allOf","enum","const","not"] {
                    assert!(parameters.get(forbidden).is_none(), "forbidden function root: {forbidden}");
                }
                assert_eq!(parameters["properties"], canonical.input_schema["properties"]);
                assert_eq!(parameters["required"], canonical.input_schema["required"]);
            }
            axum::Json(match api {
                OpenAiApi::ChatCompletions => json!({"choices":[{"message":{"role":"assistant","content":"ready"},"finish_reason":"stop"}]}),
                OpenAiApi::Responses => {
                    assert_eq!(body["store"],false);
                    assert!(body.get("messages").is_none());
                    json!({"status":"completed","output":[{"type":"message","id":"msg-ready","role":"assistant","content":[{"type":"output_text","text":"ready"}]}]})
                }
            })
        }
    }));
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let backend = OpenAiCompatibleBackend::new(OpenAiCompatibleConfig {
        backend_id: "schema-fixture".into(),
        endpoint,
        api_key: String::new(),
        model: "fixture-model".into(),
        temperature: 0.0,
        default_max_output_tokens: 32,
        max_context_tokens: None,
        timeout: Duration::from_secs(2),
        structured_output: true,
        max_buffered_events: 16,
    })
    .unwrap()
    .with_api(api);
    let mut events = backend
        .start(
            ModelRequest {
                request_id: ModelRequestId::new("schema-check"),
                messages: vec![ModelMessage::text(
                    ModelRole::User,
                    "Check the tool declarations",
                )],
                tools: expected
                    .iter()
                    .map(|schema| ModelToolDefinition {
                        name: schema.name.clone(),
                        description: schema.description.clone(),
                        input_schema: schema.input_schema.clone(),
                    })
                    .collect(),
                output_schema: None,
                max_output_tokens: Some(32),
                extensions: BTreeMap::new(),
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let mut finished = false;
    while let Some(event) = events.next().await {
        if matches!(event.unwrap().payload, ModelEvent::Finish { .. }) {
            finished = true;
        }
    }
    assert!(finished);
    assert!(edit.input_schema.get("oneOf").is_some());
    assert!(edit.validate_arguments(&mixed).is_err());
    server.abort();
}
