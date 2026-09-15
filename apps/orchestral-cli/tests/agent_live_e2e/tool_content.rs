use super::*;
use orchestral_core::agent_protocol::wire::Digest;
use orchestral_core::tool_effect::{
    replay_tool_effect, ToolEffectJournalRecord, ToolEffectPhase, ToolEffectProjection,
};
use orchestral_core::tool_protocol::{ToolOutcome, ToolOutput};

const SOURCE: &str = "// Preserve Unicode Ω and literal backslashes.\npub fn has_newline(text: &str) -> bool {\n    text.ends_with('\\n')\n}\n";
const UPDATED: &str = "// Preserve Unicode Ω and literal backslashes.\npub fn has_newline(text: &str) -> bool {\n    text.contains('\\n')\n}\n";
const NATIVE_REASONING: &str = " private continuation Ω\n  literal \\n and <tool_call> text\t ";

fn reasoning_tool_response(
    call_id: &str,
    name: &str,
    arguments: Value,
    streaming: bool,
) -> FixtureHttpResponse {
    if !streaming {
        return json_response(json!({"choices": [{
            "message": {"reasoning_content": NATIVE_REASONING, "tool_calls": [{
                "id": call_id, "function": {"name": name, "arguments": arguments.to_string()}
            }]}, "finish_reason": "tool_calls"
        }]}));
    }
    let mut response = openai_tool_response(call_id, name, arguments);
    let reasoning = json!({"choices": [{"delta": {"reasoning": NATIVE_REASONING}}]});
    response.body = [format!("data: {reasoning}\n\n").into_bytes(), response.body].concat();
    response
}

fn assert_native_continuations(request: &CapturedHttpRequest, expected: usize) {
    let assistant = request.body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["role"] == "assistant" && message.get("tool_calls").is_some())
        .collect::<Vec<_>>();
    assert_eq!(assistant.len(), expected);
    for message in assistant {
        assert_eq!(message["reasoning_content"], NATIVE_REASONING);
        assert_eq!(message["content"], Value::Null);
        for call in message["tool_calls"].as_array().unwrap() {
            assert!(!call["function"]["arguments"]
                .as_str()
                .unwrap()
                .contains("private continuation"));
        }
    }
}

// Decode the documented presentation independently of the adapter encoder.
// Source bytes inside fences are never unescaped or dedented.
pub(super) fn decode_tool_envelope(content: &str) -> Value {
    let (metadata, mut rest) = content.split_once('\n').unwrap_or((content, ""));
    let Ok(mut envelope) = serde_json::from_str::<Value>(metadata) else {
        return serde_yaml::from_str(content).expect("explicit YAML tool envelope");
    };
    envelope
        .as_object_mut()
        .unwrap()
        .entry("is_error")
        .or_insert(json!(false));
    while !rest.is_empty() {
        let (header, body) = rest.split_once('\n').expect("field header");
        let (opening, body) = body.split_once('\n').expect("opening fence");
        let fence = opening.strip_suffix("text").expect("text fence");
        assert!(fence.len() >= 3 && fence.bytes().all(|byte| byte == b'`'));
        let closing = format!("\n{fence}\n");
        let (source, following) = body.split_once(&closing).expect("closing fence");
        let (label, source) = if let Some(label) = header.strip_suffix(" (final newline: yes)") {
            (label, format!("{source}\n"))
        } else {
            (
                header
                    .strip_suffix(" (final newline: no)")
                    .expect("newline boundary"),
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
            let key: String = serde_json::from_str(label).expect("field name");
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

fn rendered_tool_messages(request: &CapturedHttpRequest) -> Vec<(&str, Value)> {
    request.body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["role"] == "tool")
        .map(|message| {
            let content = message["content"].as_str().unwrap();
            let envelope = decode_tool_envelope(content);
            (content, envelope)
        })
        .collect()
}

fn assert_source_observation(request: &CapturedHttpRequest) {
    let messages = rendered_tool_messages(request);
    let (text, envelope) = &messages[0];
    assert_eq!(envelope["is_error"], false);
    assert_eq!(envelope["result"]["path"], "src/newline.rs");
    assert_eq!(
        envelope["result"]["content"].as_str().unwrap().as_bytes(),
        SOURCE.as_bytes()
    );
    assert!(envelope["result"].get("revision").is_none());
    assert!(envelope["result"].get("content_digest").is_none());
    // Inspect the actual model-facing text, not only a parsed round trip:
    // normal source lines must retain their original backslashes and quotes.
    for line in SOURCE.lines() {
        assert!(
            text.lines()
                .any(|shown| shown.trim_start() == line.trim_start()),
            "{text}"
        );
    }
    assert!(!text.contains("ends_with('\\\\n')"), "{text}");
    if text.starts_with('{') {
        assert!(
            text.contains(SOURCE),
            "default text must preserve indentation: {text}"
        );
    }
}

#[test]
fn yaml_tool_content_preserves_source_through_patch_and_session_restart() {
    assert_source_edit_and_session_restart("apply_patch", true, Some("yaml"));
}

#[test]
fn file_edit_preserves_source_through_cli_and_session_restart() {
    assert_source_edit_and_session_restart("file_edit", false, None);
}

#[test]
fn file_replace_uses_observed_read_through_cli_and_session_restart() {
    assert_source_edit_and_session_restart("file_write", true, None);
}

#[test]
fn length_with_native_reasoning_never_executes_partial_tool_arguments() {
    let _guard = local_e2e_guard();
    for streaming in [true, false] {
        let workspace = TestWorkspace::new("reasoning-length");
        fs::write(workspace.path("untouched.rs"), SOURCE).unwrap();
        workspace.disable_exec();
        let (endpoint, server) = spawn_fixture_http_server(vec![Box::new(move |_| {
            let call = json!({"id": "truncated", "index": 0, "function": {
                "name": "file_edit", "arguments": "{\"path\":\"untouched.rs\",\"old_text\":"
            }});
            if streaming {
                let frame = json!({"choices": [{"delta": {"reasoning": NATIVE_REASONING, "tool_calls": [call]}, "finish_reason": "length"}]});
                sse_response(format!("data: {frame}\n\ndata: [DONE]\n\n"))
            } else {
                json_response(
                    json!({"choices": [{"message": {"reasoning_content": NATIVE_REASONING, "tool_calls": [call]}, "finish_reason": "length"}]}),
                )
            }
        })]);
        workspace.configure_local_openai(&endpoint);
        let mut command = root_command(&workspace);
        command.env("OPENAI_API_KEY", "fixture-key").args([
            "--session-id",
            "length-session",
            "--no-mcp",
            "--no-skills",
            "Inspect the source.",
        ]);
        let output = run_to_completion(command, LOCAL_PROCESS_TIMEOUT);
        assert!(!output.status.success());
        assert!(!output.stdout_text().contains("private continuation"));
        assert_eq!(
            fs::read_to_string(workspace.path("untouched.rs")).unwrap(),
            SOURCE
        );
        assert_eq!(server.join().unwrap().len(), 1);
        assert!(tool_exchanges(&session_records(&workspace)).is_empty());
    }
}

fn assert_source_edit_and_session_restart(
    edit_tool: &'static str,
    streaming: bool,
    format: Option<&str>,
) {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("source-tool-content");
    fs::create_dir(workspace.path("src")).unwrap();
    fs::write(workspace.path("src/newline.rs"), SOURCE).unwrap();
    workspace.disable_exec();
    workspace.rewrite_config(|config| {
        config["agent"]["model_profile"] = serde_yaml::to_value("source-text").unwrap();
        config["providers"]["default_model"] = serde_yaml::to_value("source-text").unwrap();
        let mut profile_config = json!({});
        if let Some(format) = format {
            profile_config["tool_result_format"] = json!(format);
        }
        config["providers"]["models"] = serde_yaml::to_value(json!([{
            "name": "source-text", "backend": "openai", "model": "fixture-model",
            "config": profile_config,
        }]))
        .unwrap();
    });
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(move |_| {
            reasoning_tool_response(
                "inspect-source",
                "file_read",
                json!({"path": "src/newline.rs"}),
                streaming,
            )
        }),
        Box::new(move |request| {
            assert_source_observation(request);
            assert_native_continuations(request, 1);
            assert!(request.body["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|tool| tool["function"]["name"] == edit_tool));
            let arguments = match edit_tool {
                "file_edit" => json!({
                    "path": "src/newline.rs",
                    "old_text": "    text.ends_with('\\n')",
                    "new_text": "    text.contains('\\n')",
                }),
                "apply_patch" => json!({"patch": concat!(
                    "*** Begin Patch\n*** Update File: src/newline.rs\n@@\n",
                    "-    text.ends_with('\\n')\n+    text.contains('\\n')\n*** End Patch",
                )}),
                "file_write" => json!({
                    "path": "src/newline.rs", "mode": "replace", "content": UPDATED,
                }),
                _ => unreachable!("fixture declares an edit tool"),
            };
            reasoning_tool_response("edit-source", edit_tool, arguments, streaming)
        }),
        Box::new(move |request| {
            assert_native_continuations(request, 2);
            let messages = rendered_tool_messages(request);
            assert_eq!(messages.len(), 2);
            assert_eq!(messages[1].1["is_error"], false);
            if streaming {
                let reasoning = json!({"choices": [{"delta": {"reasoning": NATIVE_REASONING}}]});
                let mut response = openai_text_response("Source updated.");
                response.body =
                    [format!("data: {reasoning}\n\n").into_bytes(), response.body].concat();
                response
            } else {
                json_response(
                    json!({"choices": [{"message": {"content": "Source updated.", "reasoning_content": NATIVE_REASONING}, "finish_reason": "stop"}]}),
                )
            }
        }),
        Box::new(|request| {
            // A fresh process must preserve the recorded model observation
            // in the same explicit format, without repeating either tool.
            assert_source_observation(request);
            assert_native_continuations(request, 2);
            let delivered = request.body["messages"]
                .as_array()
                .unwrap()
                .iter()
                .find(|message| {
                    message["role"] == "assistant" && message["content"] == "Source updated."
                })
                .unwrap();
            assert_eq!(
                delivered["reasoning_content"], NATIVE_REASONING,
                "Stop commits the same message state as a Tool continuation"
            );
            let messages = rendered_tool_messages(request);
            assert_eq!(messages.len(), 2);
            assert_eq!(messages[1].1["is_error"], false);
            openai_text_response("The recorded change is available.")
        }),
    ]);
    workspace.configure_local_openai(&endpoint);
    let mut effects_before_restart = None;
    for (index, prompt) in [
        "Find newlines anywhere in the input.",
        "Recall the recorded change.",
    ]
    .into_iter()
    .enumerate()
    {
        let mut command = root_command(&workspace);
        command.env("OPENAI_API_KEY", "fixture-key").args([
            "--model-profile",
            "source-text",
            "--session-id",
            "source-session",
            "--no-mcp",
            "--no-skills",
            prompt,
        ]);
        let output = run_to_completion(command, LOCAL_PROCESS_TIMEOUT);
        assert!(output.status.success(), "{}", output.stderr_text());
        assert!(!output.stdout_text().contains("private continuation"));
        assert_eq!(
            fs::read_to_string(workspace.path("src/newline.rs")).unwrap(),
            UPDATED
        );
        if index == 0 {
            effects_before_restart = Some(effect_journals(&workspace));
        }
    }
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 4);
    let observed_before = rendered_tool_messages(&requests[1]);
    let observed_after = rendered_tool_messages(&requests[3]);
    assert_eq!(observed_before[0], observed_after[0]);
    let records = session_records(&workspace);
    let exchanges = tool_exchanges(&records);
    assert_eq!(
        exchanges.len(),
        2,
        "restart must not repeat completed effects"
    );
    assert_eq!(tool_name(exchanges[0]), Some("file_read"));
    assert_eq!(tool_name(exchanges[1]), Some(edit_tool));
    assert_eq!(
        tool_result_value(exchanges[0]),
        &observed_before[0].1["result"]
    );
    assert_eq!(
        tool_result_value(exchanges[1]),
        &observed_after[1].1["result"]
    );
    // Model history is compact; the immutable Effect Journal still contains
    // the complete canonical read and mutation outcomes after restart.
    let effects = effect_journals(&workspace);
    assert_eq!(
        effects,
        effects_before_restart.unwrap(),
        "restart must not execute or rewrite completed effects"
    );
    let projections = effects
        .iter()
        .map(|records| {
            let key = &records.first().expect("nonempty effect journal").key;
            replay_tool_effect(key, records)
                .expect("valid effect journal digest chain")
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(projections.len(), 2);
    let read_effect = effect_for_exchange(&projections, exchanges[0]);
    let canonical_read = committed_output(read_effect);
    assert_eq!(
        canonical_read["content"].as_str().unwrap().as_bytes(),
        SOURCE.as_bytes()
    );
    assert_eq!(canonical_read["path"], "src/newline.rs");
    assert_eq!(canonical_read["start_line"], 1);
    assert_eq!(canonical_read["eof"], true);
    assert_eq!(canonical_read["truncated"], false);
    assert_eq!(canonical_read["file_size_bytes"], SOURCE.len());
    assert_eq!(
        canonical_read["content_digest"],
        json!(Digest::sha256(SOURCE.as_bytes()))
    );
    let mutation = effect_for_exchange(&projections, exchanges[1]);
    let changes = committed_output(mutation)["changes"].as_array().unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0]["path"], "src/newline.rs");
    assert_eq!(
        changes[0]["before_digest"],
        json!(Digest::sha256(SOURCE.as_bytes()))
    );
    assert_eq!(
        changes[0]["after_digest"],
        json!(Digest::sha256(UPDATED.as_bytes()))
    );
    if edit_tool == "file_write" {
        assert!(tool_arguments(exchanges[1])
            .get("expected_digest")
            .is_none());
        assert!(mutation
            .prepared
            .invocation
            .arguments
            .get("expected_digest")
            .is_none());
        let resolution = mutation
            .prepared
            .argument_resolution
            .as_ref()
            .expect("runtime binds the observed complete read");
        assert_eq!(resolution.source, read_effect.key);
        assert_eq!(
            resolution.arguments["expected_digest"],
            canonical_read["content_digest"]
        );
    }
}

fn effect_journals(workspace: &TestWorkspace) -> Vec<Vec<ToolEffectJournalRecord>> {
    journal_files(workspace, "effect-")
        .into_iter()
        .map(|path| {
            serde_json::from_slice(&fs::read(path).expect("read Effect Journal"))
                .expect("decode canonical Effect Journal")
        })
        .collect()
}

fn effect_for_exchange<'a>(
    effects: &'a [ToolEffectProjection],
    exchange: &Value,
) -> &'a ToolEffectProjection {
    let call_id = exchange["tool"]["content"]
        .as_array()
        .unwrap()
        .iter()
        .find(|content| content["type"] == "tool_result")
        .unwrap()["call_id"]
        .as_str()
        .unwrap();
    effects
        .iter()
        .find(|effect| effect.key.call_id.as_str() == call_id)
        .expect("model observation has its original committed effect")
}

fn committed_output(effect: &ToolEffectProjection) -> &Value {
    match &effect.phase {
        ToolEffectPhase::Committed {
            outcome:
                ToolOutcome::Completed {
                    output: ToolOutput::Inline(output),
                },
            ..
        } => output,
        _ => panic!("expected a committed inline effect: {effect:?}"),
    }
}
