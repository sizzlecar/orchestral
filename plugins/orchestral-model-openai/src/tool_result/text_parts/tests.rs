use super::*;
use crate::tool_result::{
    tests::{assert_observed_prefix_planning_identity, backend, history},
    OpenAiToolResultFormat,
};
use crate::{
    ModelBackend, ModelRequest, ModelTokenMeter, OpenAiSamplingConfig,
    CONTEXT_ESTIMATE_BYTES_PER_TOKEN,
};
use orchestral_core::agent_protocol::wire::Digest;

// Interpret the published wire framing after a template has joined and trimmed
// the parts. Reconstruct the complete JSON envelope, without source unescaping
// or indentation removal, rather than asserting an implementation part count.
fn restore_envelope(rendered: &str) -> Value {
    let rest = rendered
        .trim()
        .strip_prefix("Tool result metadata:\n")
        .unwrap();
    let (metadata, mut rest) = rest.split_once('\n').unwrap_or((rest, ""));
    let mut envelope: Value = serde_json::from_str(metadata).unwrap();
    while !rest.is_empty() {
        rest = rest.trim_start_matches('\n');
        if rest.is_empty() {
            break;
        }
        let (header, body) = rest.split_once('\n').unwrap();
        let (opening, body) = body.split_once('\n').unwrap();
        let fence = opening.strip_suffix("text").unwrap();
        assert!(fence.len() >= 3 && fence.bytes().all(|byte| byte == b'`'));
        let closing = format!("\n{fence}");
        let (source, following) = body.split_once(closing.as_str()).unwrap();
        assert!(following.is_empty() || following.starts_with('\n'));
        let (label, source) = if let Some(label) = header.strip_suffix(" (final newline: yes)") {
            (label, format!("{source}\n"))
        } else {
            (
                header.strip_suffix(" (final newline: no)").unwrap(),
                source.to_owned(),
            )
        };
        if label == "Result text" {
            assert!(envelope
                .as_object_mut()
                .unwrap()
                .insert("result".to_owned(), json!(source))
                .is_none());
        } else {
            let key: String =
                serde_json::from_str(label.strip_prefix("Text field ").unwrap()).unwrap();
            assert!(envelope["result"]
                .as_object_mut()
                .unwrap()
                .insert(key, json!(source))
                .is_none());
        }
        rest = following;
    }
    envelope
}

fn joined(parts: &Value, separator: &str) -> String {
    parts
        .as_array()
        .unwrap()
        .iter()
        .map(|part| {
            assert_eq!(part["type"], "text");
            part["text"].as_str().unwrap()
        })
        .collect::<Vec<_>>()
        .join(separator)
}

fn assert_round_trip(result: &Value, is_error: bool) -> Value {
    let saved = result.clone();
    let parts = encode(result, is_error);
    for separator in ["", "\n"] {
        assert_eq!(
            restore_envelope(&joined(&parts, separator)),
            json!({"result": result, "is_error": is_error})
        );
    }
    assert_eq!(result, &saved);
    parts
}

#[test]
fn text_parts_preserve_raw_source_boundaries_and_embedded_fences() {
    let sources = [
        "    fn main() {\r\n\tprintln!(\"\\nλ\");\r\n    }\r\n",
        "  ```text\n\tquoted ` value\n````\nlast ",
        "ends in CR\r",
        "\0\u{1b}\u{2028}\n",
        "\n",
        "\r",
    ];
    for source in sources {
        let original = json!({"source": source, "eof": true});
        let parts = assert_round_trip(&original, false);
        assert_eq!(
            parts[0]["text"],
            "Tool result metadata:\n{\"is_error\":false,\"result\":{\"eof\":true}}\n"
        );
        assert_round_trip(&json!(source), true);
    }
    let fenced = encode(&json!({"source": "```\n````\n"}), false);
    assert_eq!(
        fenced[1]["text"],
        "Text field \"source\" (final newline: yes)\n`````text\n```\n````\n`````\n"
    );
}

#[test]
fn text_parts_keep_empty_and_single_line_strings_in_json_metadata() {
    for value in [
        json!(""),
        json!("  leading and trailing  "),
        json!("null"),
        json!("false"),
        json!("01"),
        json!("'\"\\\t\0\u{1b}\u{85}\u{2028}\u{2029}λ"),
        json!("```text"),
        Value::Null,
        json!(false),
        json!(u64::MAX),
        json!(0.25),
        json!([]),
        json!({}),
        json!(["nested\ntext", "", null, {"v": "a\rb"}]),
        json!({"empty": "", "single": "a: b", "null": null, "boolean": false, "nested": {"multiline": "a\r\nb"}}),
    ] {
        for is_error in [false, true] {
            let parts = assert_round_trip(&value, is_error);
            let metadata: Value = serde_json::from_str(
                parts[0]["text"]
                    .as_str()
                    .unwrap()
                    .strip_prefix("Tool result metadata:\n")
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(metadata, json!({"result": value, "is_error": is_error}));
        }
    }
}

#[test]
fn text_parts_keep_typed_metadata_and_sort_keys_independent_of_map_features() {
    let result: Value = serde_json::from_str(
        r#"{"z":"last\r","nested":{"z":1,"a":{"y":2,"b":"nested\nstring"}},"array":[{"z":true,"a":null}],"a":"first\n","n":0,"bool":false,"single":"null","empty":""}"#,
    ).unwrap();
    let parts = assert_round_trip(&result, true);
    assert_eq!(
        parts[0]["text"],
        "Tool result metadata:\n{\"is_error\":true,\"result\":{\"array\":[{\"a\":null,\"z\":true}],\"bool\":false,\"empty\":\"\",\"n\":0,\"nested\":{\"a\":{\"b\":\"nested\\nstring\",\"y\":2},\"z\":1},\"single\":\"null\"}}\n"
    );
    assert!(parts[1]["text"]
        .as_str()
        .unwrap()
        .starts_with("Text field \"a\" "));
    assert!(parts[2]["text"]
        .as_str()
        .unwrap()
        .starts_with("Text field \"z\" "));
    let scalar = assert_round_trip(&json!("null\n"), true);
    assert_eq!(
        scalar[0]["text"],
        "Tool result metadata:\n{\"is_error\":true}\n"
    );
    assert_eq!(
        scalar[1]["text"],
        "Result text (final newline: yes)\n```text\nnull\n```\n"
    );
    let key = "line\n\"key\\";
    let quoted = assert_round_trip(&json!({key: "actual\r"}), false);
    assert!(quoted[1]["text"]
        .as_str()
        .unwrap()
        .starts_with("Text field \"line\\n\\\"key\\\\\" (final newline: no)\n"));
}

#[test]
fn text_parts_close_fence_lines_when_templates_concatenate_parts_directly() {
    let result = json!({"a": "first\n", "b": "second\r", "label": "single"});
    let parts = assert_round_trip(&result, false);
    assert_eq!(
        joined(&parts, ""),
        concat!(
            "Tool result metadata:\n{\"is_error\":false,\"result\":{\"label\":\"single\"}}\n",
            "Text field \"a\" (final newline: yes)\n```text\nfirst\n```\n",
            "Text field \"b\" (final newline: no)\n```text\nsecond\r\n```\n",
        )
    );
}

#[test]
fn text_parts_v2_preserves_canonical_history_and_changes_recovery_identity() {
    assert_eq!(
        serde_json::from_value::<OpenAiToolResultFormat>(json!("text_parts")).unwrap(),
        OpenAiToolResultFormat::TextParts
    );
    assert_eq!(
        serde_json::to_value(OpenAiToolResultFormat::TextParts).unwrap(),
        "text_parts"
    );
    let source = "fn escape(value: &str) -> String {\n    value.replace('\\\\', \"\\\\\\\\\").replace('\\\"', \"\\\\\\\"\")\n}\n";
    let result =
        json!({"content": source, "eof": true, "path": "src/lib.rs", "lines": 3, "stderr": ""});
    let request = history(result.clone());
    let saved = serde_json::to_vec(&request).unwrap();
    let restored: ModelRequest = serde_json::from_slice(&saved).unwrap();
    let parts_backend = backend().with_tool_result_format(OpenAiToolResultFormat::TextParts);
    let body = parts_backend.build_request_body(&restored).unwrap();
    let tool = &body["messages"][2];
    assert_eq!(tool["role"], "tool");
    assert_eq!(tool["tool_call_id"], "native-call");
    assert_eq!(
        restore_envelope(&joined(&tool["content"], "")),
        json!({"result": result, "is_error": false})
    );
    assert_eq!(serde_json::to_vec(&restored).unwrap(), saved);
    let meter = parts_backend.meter_descriptor();
    assert_eq!(
        meter.strategy,
        "openai-compatible/wire-json-text-parts-tool-upper-bound"
    );
    assert_observed_prefix_planning_identity(&parts_backend, OpenAiToolResultFormat::TextParts);
    assert_eq!(
        parts_backend.descriptor().extensions["openai-compatible/tool-result-encoding"],
        crate::tool_result::TEXT_PARTS_ENCODING_IDENTITY
    );
    // Hold the observed-prefix planning version and all settings constant:
    // changing only the wire codec must invalidate the bound recovery identity.
    let mut legacy_descriptor = parts_backend.descriptor();
    legacy_descriptor.extensions.insert(
        "openai-compatible/tool-result-encoding".to_owned(),
        json!("openai-compatible/text-parts-tool-envelope/v1"),
    );
    assert_ne!(legacy_descriptor, parts_backend.descriptor());
    let legacy_config = (
        "local-model",
        0.6_f32.to_bits(),
        128_u64,
        false,
        OpenAiSamplingConfig::default(),
        CONTEXT_ESTIMATE_BYTES_PER_TOKEN,
        "observed-prefix-input-delta/v1",
    );
    let mut legacy_meter = meter.clone();
    legacy_meter.config_digest = Digest::sha256(
        serde_json::to_vec(&(
            legacy_config,
            "openai-compatible/text-parts-tool-envelope/v1",
        ))
        .unwrap(),
    );
    assert_ne!(legacy_meter, meter);
    let resumed = backend().with_tool_result_format(OpenAiToolResultFormat::TextParts);
    assert_eq!(resumed.build_request_body(&restored).unwrap(), body);
    assert_eq!(resumed.descriptor(), parts_backend.descriptor());
    assert_eq!(resumed.meter_descriptor(), meter);

    let mut costs = Vec::new();
    for format in [
        OpenAiToolResultFormat::Json,
        OpenAiToolResultFormat::Yaml,
        OpenAiToolResultFormat::TextParts,
    ] {
        let adapter = backend().with_tool_result_format(format);
        let body = adapter.build_request_body(&request).unwrap();
        let wire_bytes = serde_json::to_vec(&body).unwrap().len() as u64;
        let framing = 64 + request.messages.len() as u64 * 16;
        let count = adapter.count_request_input(&request.messages, &[]).unwrap();
        let estimate = adapter
            .estimate_context_input(&request.messages, &[])
            .unwrap();
        assert_eq!(count, wire_bytes + framing);
        assert_eq!(
            estimate.tokens,
            wire_bytes.div_ceil(CONTEXT_ESTIMATE_BYTES_PER_TOKEN) + framing
        );
        if format != OpenAiToolResultFormat::TextParts {
            assert_ne!(adapter.descriptor(), parts_backend.descriptor());
            assert_ne!(adapter.meter_descriptor(), meter);
        }
        costs.push(json!({"format": format, "http_body_bytes": wire_bytes, "input_upper_bound": count, "estimated_input_tokens": estimate.tokens}));
    }
    // Diagnostic size comparison, not a tokenizer measurement or success gate.
    println!(
        "text_parts_fixture_cost {}",
        json!({"source_bytes": source.len(), "costs": costs})
    );
}
