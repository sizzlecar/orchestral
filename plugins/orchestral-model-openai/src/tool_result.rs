use orchestral_core::model_protocol::ModelError;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

// Bump this identity if YAML rendering changes. It is included in both the
// model descriptor and token-meter configuration bound by Run recovery.
pub(crate) const YAML_ENCODING_IDENTITY: &str = "serde-yaml-0.9/tool-envelope/v1";

/// Text encoding of the complete `{result, is_error}` tool response envelope.
/// Canonical ToolResults and their durable journals retain the original JSON.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OpenAiToolResultFormat {
    /// Preserve the original JSON text encoding and recovery identity.
    #[default]
    Json,
    /// Encode the complete envelope as YAML. Ordinary multiline strings use
    /// literal blocks; special whitespace may require quoted escapes.
    Yaml,
}

impl OpenAiToolResultFormat {
    pub(crate) fn encode(self, result: &Value, is_error: bool) -> Result<String, ModelError> {
        let envelope = json!({"result": result, "is_error": is_error});
        match self {
            Self::Json => Ok(envelope.to_string()),
            Self::Yaml => serde_yaml::to_string(&envelope).map_err(|error| {
                ModelError::invalid_request(format!("cannot encode ToolResult as YAML: {error}"))
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        native_tool_call_extensions, ModelBackend, ModelContent, ModelMessage, ModelRequest,
        ModelRequestId, ModelRole, ModelTokenAccounting, ModelTokenMeter, ModelToolCallId,
        OpenAiCompatibleBackend, OpenAiCompatibleConfig, OpenAiSamplingConfig,
        CONTEXT_ESTIMATE_BYTES_PER_TOKEN,
    };
    use orchestral_core::agent_protocol::wire::Digest;
    use std::{collections::BTreeMap, time::Duration};

    fn backend() -> OpenAiCompatibleBackend {
        OpenAiCompatibleBackend::new(OpenAiCompatibleConfig {
            backend_id: "tool-result-wire".to_owned(),
            endpoint: "http://127.0.0.1/v1".to_owned(),
            api_key: String::new(),
            model: "local-model".to_owned(),
            temperature: 0.6,
            default_max_output_tokens: 128,
            max_context_tokens: Some(8_192),
            timeout: Duration::from_secs(1),
            structured_output: false,
            max_buffered_events: 8,
        })
        .unwrap()
    }

    fn history(result: Value) -> ModelRequest {
        ModelRequest {
            request_id: ModelRequestId::new("tool-result-request"),
            messages: vec![
                ModelMessage::text(ModelRole::User, "Inspect the source"),
                ModelMessage {
                    role: ModelRole::Assistant,
                    content: vec![ModelContent::ToolCall {
                        call_id: ModelToolCallId::new("canonical-call"),
                        name: "inspect".to_owned(),
                        arguments: json!({"path": "src/lib.rs"}),
                        extensions: native_tool_call_extensions("native-call".to_owned()),
                    }],
                },
                ModelMessage {
                    role: ModelRole::Tool,
                    content: vec![ModelContent::ToolResult {
                        call_id: ModelToolCallId::new("canonical-call"),
                        result,
                        is_error: false,
                    }],
                },
            ],
            tools: Vec::new(),
            output_schema: None,
            max_output_tokens: None,
            extensions: BTreeMap::new(),
        }
    }

    #[test]
    fn yaml_tool_result_preserves_multiline_source_and_complete_metadata() {
        let source = "fn quoted(key: &str) -> String {\n    format!(\"\\\"{key}\\\"\")\n}\n";
        let result = json!({
            "content": source, "path": "src/lib.rs", "revision": "r1",
            "start_line": 1, "end_line": 3, "next_offset": null, "eof": true,
            "truncated": false, "truncation_reasons": [], "file_size_bytes": source.len(),
        });
        let encoded = OpenAiToolResultFormat::Yaml.encode(&result, false).unwrap();
        let decoded: Value = serde_yaml::from_str(&encoded).unwrap();
        assert_eq!(decoded, json!({"result": result, "is_error": false}));
        assert_eq!(
            decoded["result"]["content"].as_str().unwrap().as_bytes(),
            source.as_bytes()
        );
        // Inspect model-visible text after the outer HTTP JSON is decoded.
        assert!(encoded.contains("content: |\n"));
        assert!(encoded.contains(r#"format!("\"{key}\"")"#));
        assert!(!encoded.contains(r"\n"));
    }

    #[test]
    fn yaml_tool_result_round_trips_types_special_whitespace_errors_and_artifacts() {
        let values = [
            json!({
                "scalars": [null, true, false, 0, -1, u64::MAX, 0.25],
                "strings": ["null", "true", "01", "0.25", "", "a: b", "---\n..."],
                "source": ["a\r\nb\r\n", "\tindented\n", "space \nlast ", "\0\u{1b}\u{85}\u{2028}", "'\"\\"],
                "nested": [{"key": "value"}, [], {}],
            }),
            json!({"kind": "failed", "code": "io", "message": "line one\nline two", "retryable": true}),
            json!({"kind": "artifact", "artifact": {"id": "blob-1"}, "media_type": "application/json", "byte_size": 512, "summary": "large result"}),
        ];
        for result in values {
            for is_error in [false, true] {
                let encoded = OpenAiToolResultFormat::Yaml
                    .encode(&result, is_error)
                    .unwrap();
                let decoded: Value = serde_yaml::from_str(&encoded).unwrap();
                assert_eq!(decoded, json!({"result": result, "is_error": is_error}));
            }
        }
    }

    #[test]
    fn json_tool_result_keeps_legacy_wire_and_meter_identity() {
        assert_eq!(
            OpenAiToolResultFormat::default(),
            OpenAiToolResultFormat::Json
        );
        assert!(serde_json::from_value::<OpenAiToolResultFormat>(json!("guess")).is_err());
        assert_eq!(
            serde_json::from_value::<OpenAiToolResultFormat>(json!("yaml")).unwrap(),
            OpenAiToolResultFormat::Yaml
        );
        let backend = backend();
        let result = json!({"content": "line\n\\quoted\"", "eof": false});
        let request = history(result.clone());
        let body = backend.build_request_body(&request).unwrap();
        assert_eq!(
            body["messages"][2]["content"],
            json!({"result": result, "is_error": false}).to_string()
        );
        let legacy_config = serde_json::to_vec(&(
            "local-model",
            0.6_f32.to_bits(),
            128_u64,
            false,
            OpenAiSamplingConfig::default(),
            CONTEXT_ESTIMATE_BYTES_PER_TOKEN,
        ))
        .unwrap();
        let meter = backend.meter_descriptor();
        assert_eq!(meter.strategy, "openai-compatible/wire-json-upper-bound");
        assert_eq!(meter.version, "2");
        assert_eq!(meter.config_digest, Digest::sha256(legacy_config));
        let descriptor = backend.descriptor();
        assert_eq!(
            descriptor.extensions,
            BTreeMap::from([("openai-compatible/model".to_owned(), json!("local-model"))])
        );
        let explicit = backend.with_tool_result_format(OpenAiToolResultFormat::Json);
        assert_eq!(explicit.build_request_body(&request).unwrap(), body);
        assert_eq!(explicit.meter_descriptor(), meter);
        assert_eq!(explicit.descriptor(), descriptor);
    }

    #[test]
    fn yaml_tool_result_wire_replays_canonical_history_and_is_metered_with_its_own_identity() {
        let request =
            history(json!({"source": "let quoted = \"\\\"value\\\"\";\n".repeat(8), "eof": true}));
        let saved = serde_json::to_vec(&request).unwrap();
        let restored: ModelRequest = serde_json::from_slice(&saved).unwrap();
        let json_backend = backend();
        let json_body = json_backend.build_request_body(&request).unwrap();
        let yaml_backend = backend().with_tool_result_format(OpenAiToolResultFormat::Yaml);
        let body = yaml_backend.build_request_body(&restored).unwrap();
        assert_eq!(body, yaml_backend.build_request_body(&request).unwrap());
        assert_eq!(serde_json::to_vec(&restored).unwrap(), saved);
        assert_eq!(body["messages"][1], json_body["messages"][1]);
        assert_eq!(body["messages"][2]["tool_call_id"], "native-call");
        let encoded = body["messages"][2]["content"].as_str().unwrap();
        let envelope: Value = serde_yaml::from_str(encoded).unwrap();
        let json_envelope: Value =
            serde_json::from_str(json_body["messages"][2]["content"].as_str().unwrap()).unwrap();
        assert_eq!(envelope, json_envelope);
        let wire_bytes = serde_json::to_vec(&body).unwrap().len() as u64;
        let framing = 64 + request.messages.len() as u64 * 16;
        assert_eq!(
            yaml_backend
                .count_request_input(&request.messages, &[])
                .unwrap(),
            wire_bytes + framing
        );
        let estimate = yaml_backend
            .estimate_context_input(&request.messages, &[])
            .unwrap();
        assert_eq!(
            estimate.tokens,
            wire_bytes.div_ceil(CONTEXT_ESTIMATE_BYTES_PER_TOKEN) + framing
        );
        assert_eq!(estimate.accounting, ModelTokenAccounting::Estimated);
        assert_eq!(
            yaml_backend.meter_descriptor().accounting,
            ModelTokenAccounting::ConservativeUpperBound
        );
        assert_ne!(
            yaml_backend.meter_descriptor().config_digest,
            json_backend.meter_descriptor().config_digest
        );
        assert_ne!(
            yaml_backend.meter_descriptor(),
            json_backend.meter_descriptor()
        );
        assert_ne!(yaml_backend.descriptor(), json_backend.descriptor());
        // A fresh adapter with the same declared encoding reproduces the body
        // after restart. The runtime binds these descriptors for Run recovery.
        let resumed = backend().with_tool_result_format(OpenAiToolResultFormat::Yaml);
        assert_eq!(resumed.build_request_body(&restored).unwrap(), body);
        assert_eq!(resumed.meter_descriptor(), yaml_backend.meter_descriptor());
    }
}
