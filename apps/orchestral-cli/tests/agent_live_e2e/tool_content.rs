use super::*;

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

fn yaml_tool_messages(request: &CapturedHttpRequest) -> Vec<(&str, Value)> {
    request.body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["role"] == "tool")
        .map(|message| {
            let content = message["content"].as_str().unwrap();
            let envelope = serde_yaml::from_str(content).expect("declared YAML tool content");
            (content, envelope)
        })
        .collect()
}

fn assert_source_observation(request: &CapturedHttpRequest) {
    let messages = yaml_tool_messages(request);
    let (text, envelope) = &messages[0];
    assert_eq!(envelope["is_error"], false);
    assert_eq!(envelope["result"]["content"], SOURCE);
    assert_eq!(envelope["result"]["eof"], true);
    assert_eq!(envelope["result"]["truncated"], false);
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
}

#[test]
fn yaml_tool_content_preserves_source_through_patch_and_session_restart() {
    assert_source_edit_and_session_restart("apply_patch", true);
}

#[test]
fn file_edit_preserves_source_through_cli_and_session_restart() {
    assert_source_edit_and_session_restart("file_edit", false);
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

fn assert_source_edit_and_session_restart(edit_tool: &'static str, streaming: bool) {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("yaml-tool-content");
    fs::create_dir(workspace.path("src")).unwrap();
    fs::write(workspace.path("src/newline.rs"), SOURCE).unwrap();
    workspace.disable_exec();
    workspace.rewrite_config(|config| {
        config["agent"]["model_profile"] = serde_yaml::to_value("source-text").unwrap();
        config["providers"]["default_model"] = serde_yaml::to_value("source-text").unwrap();
        config["providers"]["models"] = serde_yaml::to_value(json!([{
            "name": "source-text", "backend": "openai", "model": "fixture-model",
            "config": {"tool_result_format": "yaml"},
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
                _ => unreachable!("fixture declares an edit tool"),
            };
            reasoning_tool_response("edit-source", edit_tool, arguments, streaming)
        }),
        Box::new(move |request| {
            assert_native_continuations(request, 2);
            let messages = yaml_tool_messages(request);
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
            // A fresh process must project the original canonical observation
            // using the same explicit format, without repeating either tool.
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
            let messages = yaml_tool_messages(request);
            assert_eq!(messages.len(), 2);
            assert_eq!(messages[1].1["is_error"], false);
            openai_text_response("The recorded change is available.")
        }),
    ]);
    workspace.configure_local_openai(&endpoint);
    for prompt in [
        "Find newlines anywhere in the input.",
        "Recall the recorded change.",
    ] {
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
    }
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 4);
    let observed_before = yaml_tool_messages(&requests[1]);
    let observed_after = yaml_tool_messages(&requests[3]);
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
}
