use super::*;
use crate::tool_result::{tests::backend, tests::history, OpenAiToolResultFormat};
use crate::{ModelBackend, ModelRequest, ModelTokenMeter, CONTEXT_ESTIMATE_BYTES_PER_TOKEN};

// Interpret the published framing, including its explicitly excluded separator
// when the source has no final LF. No text unescaping or indentation removal.
fn source_span(part: &Value) -> &str {
    assert_eq!(part["type"], "text");
    let (header, rest) = part["text"].as_str().unwrap().split_once('\n').unwrap();
    let (opening, rest) = rest.split_once('\n').unwrap();
    let fence = opening.strip_suffix("text").unwrap();
    assert!(fence.len() >= 3 && fence.bytes().all(|byte| byte == b'`'));
    let source = rest
        .strip_suffix('\n')
        .unwrap()
        .strip_suffix(fence)
        .unwrap();
    if header.ends_with("(final newline: no)") {
        source.strip_suffix('\n').unwrap()
    } else {
        assert!(header.ends_with("(final newline: yes)"));
        assert!(source.ends_with('\n'));
        source
    }
}

#[test]
fn text_parts_preserve_raw_source_boundaries_and_embedded_fences() {
    let sources = [
        "    fn main() {\r\n\tprintln!(\"\\nλ\");\r\n    }\r\n",
        "  ```text\n\tquoted ` value\n````\nlast ",
        "  leading and trailing  ",
        "ends in CR\r",
        "\0\u{1b}\u{2028}",
        "\n",
        "",
    ];
    for source in sources {
        let original = json!({"source": source, "eof": true});
        let saved = original.clone();
        let parts = encode(&original, false);
        let parts = parts.as_array().unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(
            parts[0]["text"],
            "Tool result metadata:\n{\"is_error\":false,\"result\":{\"eof\":true}}\n"
        );
        assert_eq!(source_span(&parts[1]).as_bytes(), source.as_bytes());
        assert_eq!(original, saved);
        // Both direct template concatenation and server LF joining keep each
        // complete field intact, even if the whole message is then trimmed.
        for separator in ["", "\n"] {
            let rendered = parts
                .iter()
                .map(|part| part["text"].as_str().unwrap())
                .collect::<Vec<_>>()
                .join(separator);
            assert!(rendered
                .trim()
                .ends_with(parts[1]["text"].as_str().unwrap().trim_end_matches('\n')));
        }
    }
    let fenced = encode(&json!({"source": "```\n````\n"}), false);
    assert_eq!(
        fenced[1]["text"],
        "Text field \"source\" (final newline: yes)\n`````text\n```\n````\n`````\n"
    );
}

#[test]
fn text_parts_keep_typed_metadata_and_sort_keys_independent_of_map_features() {
    let result: Value = serde_json::from_str(
        r#"{"z":"last","nested":{"z":1,"a":{"y":2,"b":"nested string"}},"array":[{"z":true,"a":null}],"a":"first","n":0,"bool":false}"#,
    ).unwrap();
    let parts = encode(&result, true);
    assert_eq!(parts.as_array().unwrap().len(), 3);
    assert_eq!(
        parts[0]["text"],
        "Tool result metadata:\n{\"is_error\":true,\"result\":{\"array\":[{\"a\":null,\"z\":true}],\"bool\":false,\"n\":0,\"nested\":{\"a\":{\"b\":\"nested string\",\"y\":2},\"z\":1}}}\n"
    );
    assert!(parts[1]["text"]
        .as_str()
        .unwrap()
        .starts_with("Text field \"a\" "));
    assert!(parts[2]["text"]
        .as_str()
        .unwrap()
        .starts_with("Text field \"z\" "));
    assert_eq!(source_span(&parts[1]), "first");
    assert_eq!(source_span(&parts[2]), "last");

    for value in [
        Value::Null,
        json!(false),
        json!(u64::MAX),
        json!(["string", null]),
        json!({}),
    ] {
        let parts = encode(&value, false);
        assert_eq!(parts.as_array().unwrap().len(), 1);
        let metadata: Value = serde_json::from_str(
            parts[0]["text"]
                .as_str()
                .unwrap()
                .strip_prefix("Tool result metadata:\n")
                .unwrap(),
        )
        .unwrap();
        assert_eq!(metadata, json!({"is_error": false, "result": value}));
    }
    let scalar = encode(&json!("null\n"), true);
    assert_eq!(
        scalar[0]["text"],
        "Tool result metadata:\n{\"is_error\":true}\n"
    );
    assert_eq!(
        scalar[1]["text"],
        "Result text (final newline: yes)\n```text\nnull\n```\n"
    );
    assert_eq!(source_span(&scalar[1]), "null\n");

    let key = "line\n\"key\\";
    let quoted = encode(&json!({key: "actual"}), false);
    assert!(quoted[1]["text"]
        .as_str()
        .unwrap()
        .starts_with("Text field \"line\\n\\\"key\\\\\" (final newline: no)\n"));
    assert_eq!(source_span(&quoted[1]), "actual");
}

#[test]
fn text_parts_close_fence_lines_when_templates_concatenate_parts_directly() {
    let parts = encode(&json!({"a": "first\n", "b": "second"}), false);
    let texts = parts
        .as_array()
        .unwrap()
        .iter()
        .map(|part| part["text"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        texts.concat(),
        concat!(
            "Tool result metadata:\n{\"is_error\":false,\"result\":{}}\n",
            "Text field \"a\" (final newline: yes)\n```text\nfirst\n```\n",
            "Text field \"b\" (final newline: no)\n```text\nsecond\n```\n",
        )
    );
    for separator in ["", "\n"] {
        let rendered = texts.join(separator);
        let rendered = rendered.trim();
        assert_eq!(rendered.lines().filter(|line| *line == "```").count(), 2);
        assert!(!rendered.contains("```Text field"));
        assert!(rendered.contains("}}\n"));
        // Both the source's final LF and the no-LF framing separator remain
        // distinguishable after either template-level joining convention.
        assert_eq!(source_span(&parts[1]), "first\n");
        assert_eq!(source_span(&parts[2]), "second");
    }
}

#[test]
fn text_parts_bind_actual_wire_meter_and_replay_without_changing_canonical_history() {
    assert_eq!(
        serde_json::from_value::<OpenAiToolResultFormat>(json!("text_parts")).unwrap(),
        OpenAiToolResultFormat::TextParts
    );
    assert_eq!(
        serde_json::to_value(OpenAiToolResultFormat::TextParts).unwrap(),
        "text_parts"
    );
    let source = "fn escape(value: &str) -> String {\n    value.replace('\\\\', \"\\\\\\\\\").replace('\\\"', \"\\\\\\\"\")\n}\n";
    let request =
        history(json!({"content": source, "eof": true, "path": "src/lib.rs", "lines": 3}));
    let saved = serde_json::to_vec(&request).unwrap();
    let restored: ModelRequest = serde_json::from_slice(&saved).unwrap();
    let parts_backend = backend().with_tool_result_format(OpenAiToolResultFormat::TextParts);
    let body = parts_backend.build_request_body(&restored).unwrap();
    let tool = &body["messages"][2];
    assert_eq!(tool["role"], "tool");
    assert_eq!(tool["tool_call_id"], "native-call");
    assert_eq!(source_span(&tool["content"][1]), source);
    assert_eq!(serde_json::to_vec(&restored).unwrap(), saved);
    let meter = parts_backend.meter_descriptor();
    assert_eq!(
        meter.strategy,
        "openai-compatible/wire-json-text-parts-tool-upper-bound"
    );
    assert_eq!(meter.version, "1");
    assert_eq!(
        parts_backend.descriptor().extensions["openai-compatible/tool-result-encoding"],
        crate::tool_result::TEXT_PARTS_ENCODING_IDENTITY
    );
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
