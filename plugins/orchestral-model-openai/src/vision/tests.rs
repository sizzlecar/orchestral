use super::*;
use crate::{
    ModelTokenAccounting, ModelTokenMeter, OpenAiApi, OpenAiCompatibleBackend,
    OpenAiCompatibleConfig,
};
use std::time::Duration;

fn image(width: u32, height: u32, padding: usize, uri: bool) -> ModelMessage {
    let mut header = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
    header.extend(width.to_be_bytes());
    header.extend(height.to_be_bytes());
    header.resize(24 + padding, 0);
    let data = STANDARD.encode(header);
    ModelMessage {
        role: ModelRole::User,
        content: vec![
            ModelContent::Text {
                text: "Describe this screenshot".into(),
            },
            ModelContent::Data {
                media_type: "image/png".into(),
                value: Value::String(if uri {
                    format!("data:image/png;base64,{data}")
                } else {
                    data
                }),
            },
        ],
    }
}

fn backend(api: OpenAiApi) -> OpenAiCompatibleBackend {
    OpenAiCompatibleBackend::new(OpenAiCompatibleConfig {
        backend_id: "vision-planning".into(),
        endpoint: "http://127.0.0.1:1/v1".into(),
        api_key: String::new(),
        model: "fixture".into(),
        temperature: 0.2,
        default_max_output_tokens: 8192,
        max_context_tokens: None,
        timeout: Duration::from_secs(1),
        structured_output: true,
        max_buffered_events: 128,
    })
    .unwrap()
    .with_api(api)
}

#[test]
fn image_planning_uses_dimensions_instead_of_base64_transport_size_for_both_apis() {
    for api in [OpenAiApi::ChatCompletions, OpenAiApi::Responses] {
        let backend = backend(api);
        for uri in [false, true] {
            let large = vec![image(1600, 1000, 1_200_000, uri)];
            let small = vec![image(1600, 1000, 20_000, uri)];
            let planning = backend.estimate_context_input(&large, &[]).unwrap();
            assert_eq!(planning.accounting, ModelTokenAccounting::Estimated);
            assert!(planning.tokens < 10_000);
            assert_eq!(
                planning,
                backend.estimate_context_input(&small, &[]).unwrap(),
                "compression and transport bytes must not inflate image context"
            );
            let bound = backend.count_request_input(&large, &[]).unwrap();
            assert!(
                bound > 1_000_000,
                "hard wire accounting remains conservative"
            );
            assert!(planning.tokens < bound);
            let body = backend
                .build_request_body(&crate::ModelRequest {
                    messages: large.clone(),
                    ..crate::tests::request()
                })
                .unwrap();
            let data = match &large[0].content[1] {
                ModelContent::Data {
                    value: Value::String(data),
                    ..
                } => data,
                _ => unreachable!(),
            };
            let sent = match api {
                OpenAiApi::Responses => body["input"][0]["content"][1]["image_url"]
                    .as_str()
                    .unwrap(),
                OpenAiApi::ChatCompletions => body["messages"][0]["content"][1]["image_url"]["url"]
                    .as_str()
                    .unwrap(),
            };
            assert!(sent.ends_with(data.strip_prefix("data:image/png;base64,").unwrap_or(data)));
            let as_text = vec![ModelMessage::text(ModelRole::User, data)];
            assert!(
                backend
                    .estimate_context_input(&as_text, &[])
                    .unwrap()
                    .tokens
                    > 300_000,
                "literal text must not receive image discounts"
            );
        }
        let thumbnail = backend
            .estimate_context_input(&[image(128, 128, 20_000, false)], &[])
            .unwrap();
        let screenshot = backend
            .estimate_context_input(&[image(2048, 1600, 20_000, false)], &[])
            .unwrap();
        assert!(screenshot.tokens > thumbnail.tokens);
    }
}

#[test]
fn invalid_encoded_images_fail_with_a_specific_input_error() {
    let message = ModelMessage {
        role: ModelRole::User,
        content: vec![ModelContent::Data {
            media_type: "image/png".into(),
            value: Value::String("not valid base64".into()),
        }],
    };
    let error = planning_size(&[message]).unwrap_err();
    assert_eq!(error.code, crate::ModelErrorCode::InvalidRequest);
}
