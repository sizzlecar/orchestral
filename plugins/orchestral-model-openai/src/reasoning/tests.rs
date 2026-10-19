use super::*;
use crate::{OpenAiCompatibleBackend, OpenAiCompatibleConfig, OpenAiToolResultFormat};
use orchestral_core::model_protocol::{ModelBackend, ModelTokenMeter};
use serde_json::{json, Value};
use std::time::Duration;

fn backend(format: OpenAiToolResultFormat) -> OpenAiCompatibleBackend {
    OpenAiCompatibleBackend::new(OpenAiCompatibleConfig {
        backend_id: "reasoning-wire".to_owned(),
        endpoint: "http://127.0.0.1/v1".to_owned(),
        api_key: String::new(),
        model: "local-model".to_owned(),
        temperature: 0.6,
        default_max_output_tokens: 128,
        max_context_tokens: Some(16_384),
        timeout: Duration::from_secs(1),
        structured_output: true,
        max_buffered_events: 8,
    })
    .unwrap()
    .with_tool_result_format(format)
}

#[test]
fn neutral_capabilities_keep_unknown_support_and_separate_toggles_from_literal_efforts() {
    let unknown =
        ModelReasoningCapabilities::try_from(OpenAiReasoningCapabilities::default()).unwrap();
    assert_eq!(unknown.supported_preferences, None);
    let known_empty = ModelReasoningCapabilities::try_from(OpenAiReasoningCapabilities {
        supported_efforts: Some(Vec::new()),
        thinking: None,
    })
    .unwrap();
    assert_eq!(known_empty.supported_preferences, Some(Vec::new()));
    let capabilities = ModelReasoningCapabilities::try_from(OpenAiReasoningCapabilities {
        supported_efforts: Some(
            ["on", "default", "future-next"]
                .into_iter()
                .map(|v| v.parse().unwrap())
                .collect(),
        ),
        thinking: Some(OpenAiThinkingCapability {
            default_enabled: true,
        }),
    })
    .unwrap();
    assert_eq!(
        capabilities.choices(),
        [
            ReasoningPreference::Default,
            ReasoningPreference::Custom("on".into()),
            ReasoningPreference::Custom("default".into()),
            ReasoningPreference::Custom("future-next".into()),
            ReasoningPreference::On,
            ReasoningPreference::Off,
        ]
    );
    assert_eq!(capabilities.default_thinking_enabled, Some(true));
    assert!(
        ModelReasoningCapabilities::try_from(OpenAiReasoningCapabilities {
            supported_efforts: Some(vec![OpenAiReasoningEffort::Custom(" ".into())]),
            thinking: None,
        })
        .is_err()
    );
}

#[test]
fn neutral_reasoning_preserves_native_wire_controls_and_recovery_identity() {
    for preference in [
        ReasoningPreference::Default,
        ReasoningPreference::None,
        ReasoningPreference::On,
        ReasoningPreference::Off,
        ReasoningPreference::Low,
        ReasoningPreference::High,
        ReasoningPreference::Custom("on".into()),
        ReasoningPreference::Custom("future-next".into()),
    ] {
        let control = OpenAiReasoningControl::from_preference(&preference).unwrap();
        let native = backend(OpenAiToolResultFormat::Json).with_reasoning_control(control);
        let neutral = backend(OpenAiToolResultFormat::Json)
            .with_reasoning_preference(preference.clone())
            .unwrap();
        let request = crate::tests::request();
        assert_eq!(
            neutral.build_request_body(&request).unwrap(),
            native.build_request_body(&request).unwrap()
        );
        assert_eq!(neutral.descriptor(), native.descriptor());
        assert_eq!(neutral.meter_descriptor(), native.meter_descriptor());
        neutral.validate_reasoning_preference(&preference).unwrap();
    }
    assert_eq!(
        OpenAiReasoningControl::from_preference(&ReasoningPreference::None).unwrap(),
        Some(OpenAiReasoningControl::Effort(OpenAiReasoningEffort::None))
    );
    assert_eq!(
        OpenAiReasoningControl::from_preference(&ReasoningPreference::Off).unwrap(),
        Some(OpenAiReasoningControl::Thinking(false))
    );
    assert_eq!(
        OpenAiReasoningControl::from_preference(&ReasoningPreference::Custom("on".into())).unwrap(),
        Some(OpenAiReasoningControl::Effort(
            OpenAiReasoningEffort::Custom("on".into())
        ))
    );
}

#[test]
fn reasoning_effort_vocabulary_is_typed_and_none_is_an_explicit_value() {
    for effort in OpenAiReasoningEffort::ALL.into_iter().chain(
        [
            "ultra",
            "future-next",
            "HIGH",
            "default",
            "on",
            "off",
            "effort:future",
        ]
        .into_iter()
        .map(|name| name.parse().unwrap()),
    ) {
        assert_eq!(
            effort.as_str().parse::<OpenAiReasoningEffort>().unwrap(),
            effort
        );
        assert_eq!(serde_json::to_value(&effort).unwrap(), effort.as_str());
        assert_eq!(
            serde_json::from_value::<OpenAiReasoningEffort>(json!(effort.as_str())).unwrap(),
            effort
        );
    }
    for invalid in [json!(""), json!(" "), json!(false), json!(0)] {
        assert!(serde_json::from_value::<OpenAiReasoningEffort>(invalid).is_err());
    }
    assert_eq!(
        serde_json::from_value::<Option<OpenAiReasoningEffort>>(Value::Null).unwrap(),
        None
    );
    assert_eq!(
        serde_json::from_value::<Option<OpenAiReasoningEffort>>(json!("none")).unwrap(),
        Some(OpenAiReasoningEffort::None)
    );
}

fn controls() -> impl Iterator<Item = OpenAiReasoningControl> {
    OpenAiReasoningEffort::ALL
        .into_iter()
        .chain(
            [
                "ultra",
                "future-next",
                "HIGH",
                "default",
                "on",
                "off",
                "effort:future",
            ]
            .into_iter()
            .map(|name| name.parse().unwrap()),
        )
        .map(OpenAiReasoningControl::Effort)
        .chain([
            OpenAiReasoningControl::Thinking(false),
            OpenAiReasoningControl::Thinking(true),
        ])
}

#[test]
fn explicit_reasoning_changes_only_its_wire_control_and_recovery_identity() {
    let request = crate::tests::request();
    for control in controls() {
        for format in [
            OpenAiToolResultFormat::Text,
            OpenAiToolResultFormat::Json,
            OpenAiToolResultFormat::Yaml,
            OpenAiToolResultFormat::TextParts,
        ] {
            let original = backend(format);
            let default_body = original.build_request_body(&request).unwrap();
            let default_meter = original.meter_descriptor();
            let default_descriptor = original.descriptor();
            assert!(default_body.get("reasoning_effort").is_none());
            assert!(default_body.get("chat_template_kwargs").is_none());

            let configured = original.with_reasoning_control(Some(control.clone()));
            let mut body = configured.build_request_body(&request).unwrap();
            match &control {
                OpenAiReasoningControl::Effort(effort) => {
                    assert_eq!(
                        body.as_object_mut().unwrap().remove("reasoning_effort"),
                        Some(json!(effort))
                    );
                }
                OpenAiReasoningControl::Thinking(enabled) => {
                    assert_eq!(
                        body.as_object_mut().unwrap().remove("chat_template_kwargs"),
                        Some(json!({"enable_thinking": enabled}))
                    );
                }
            }
            assert_eq!(body, default_body, "other request fields changed");
            assert_ne!(
                configured.meter_descriptor().config_digest,
                default_meter.config_digest
            );
            assert_eq!(
                configured.descriptor().extensions["openai-compatible/reasoning-control"],
                json!(control)
            );
            let wire = configured.build_request_body(&request).unwrap();
            assert!(
                configured
                    .count_request_input(&request.messages, &request.tools)
                    .unwrap()
                    >= serde_json::to_vec(&wire).unwrap().len() as u64
            );

            let reset = configured.with_reasoning_control(None);
            assert_eq!(reset.build_request_body(&request).unwrap(), default_body);
            assert_eq!(
                reset.meter_descriptor(),
                default_meter,
                "default must preserve the original identity"
            );
            assert_eq!(reset.descriptor(), default_descriptor);
        }
    }
}

#[test]
fn distinct_explicit_controls_cannot_share_the_same_meter_identity() {
    let mut digests = std::collections::BTreeSet::new();
    for control in controls() {
        let digest = backend(OpenAiToolResultFormat::Yaml)
            .with_reasoning_control(Some(control))
            .meter_descriptor()
            .config_digest;
        assert!(digests.insert(serde_json::to_string(&digest).unwrap()));
    }
}

#[test]
fn empty_programmatic_effort_is_rejected_before_sending_a_request() {
    let backend = backend(OpenAiToolResultFormat::Json).with_reasoning_control(Some(
        OpenAiReasoningControl::Effort(OpenAiReasoningEffort::Custom(" ".into())),
    ));
    assert!(backend
        .build_request_body(&crate::tests::request())
        .is_err());
}
