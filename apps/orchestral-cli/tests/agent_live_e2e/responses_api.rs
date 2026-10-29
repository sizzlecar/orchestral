use super::*;

fn reasoning(id: &str) -> Value {
    json!({"type":"reasoning","id":format!("rs-{id}"),"summary":[],"encrypted_content":format!("opaque-{id}")})
}

fn call(id: &str, name: &str, arguments: Value) -> Vec<Value> {
    vec![
        reasoning(id),
        json!({"type":"function_call","id":format!("fc-{id}"),"call_id":id,"name":name,"arguments":arguments.to_string(),"status":"completed"}),
    ]
}

fn text(id: &str, text: &str) -> Vec<Value> {
    vec![
        reasoning(id),
        json!({"type":"message","id":format!("msg-{id}"),"role":"assistant","status":"completed","content":[{"type":"output_text","text":text,"annotations":[]}]}),
    ]
}

fn reply(output: Vec<Value>, streaming: bool) -> FixtureHttpResponse {
    let complete = json!({"status":"completed","output":output,"usage":{"input_tokens":512,"output_tokens":48}});
    if !streaming {
        return json_response(complete);
    }
    let mut frames = String::new();
    for (index, item) in output.iter().enumerate() {
        let mut added = item.clone();
        match item["type"].as_str() {
            Some("function_call") => added["arguments"] = json!(""),
            Some("message") => added["content"] = json!([]),
            _ => {}
        }
        frames.push_str(&format!(
            "data: {}\n\n",
            json!({"type":"response.output_item.added","output_index":index,"item":added})
        ));
        let delta = match item["type"].as_str() {
            Some("function_call") => Some(
                json!({"type":"response.function_call_arguments.delta","item_id":item["id"],"output_index":index,"delta":item["arguments"]}),
            ),
            Some("message") => Some(
                json!({"type":"response.output_text.delta","item_id":item["id"],"output_index":index,"content_index":0,"delta":item["content"][0]["text"]}),
            ),
            _ => None,
        };
        if let Some(delta) = delta {
            frames.push_str(&format!("data: {delta}\n\n"));
        }
        frames.push_str(&format!(
            "data: {}\n\n",
            json!({"type":"response.output_item.done","output_index":index,"item":item})
        ));
    }
    frames.push_str(&format!(
        "data: {}\n\n",
        json!({"type":"response.completed","response":complete})
    ));
    sse_response(frames)
}

fn assert_replay(request: &CapturedHttpRequest, expected_calls: usize) {
    assert_eq!(request.path, "/v1/responses");
    assert_eq!(request.body["store"], false);
    assert_eq!(request.body["reasoning"]["effort"], "xhigh");
    assert!(request.body.get("temperature").is_none());
    let input = request.body["input"].as_array().unwrap();
    let calls = input
        .iter()
        .filter(|item| item["type"] == "function_call")
        .collect::<Vec<_>>();
    assert_eq!(calls.len(), expected_calls);
    for item in calls {
        let id = item["call_id"].as_str().unwrap();
        assert_eq!(
            input
                .iter()
                .filter(|part| part["type"] == "reasoning"
                    && part["encrypted_content"] == format!("opaque-{id}"))
                .count(),
            1
        );
        let result = input
            .iter()
            .find(|part| part["type"] == "function_call_output" && part["call_id"] == id)
            .unwrap();
        let envelope = tool_content::decode_tool_envelope(result["output"].as_str().unwrap());
        assert_eq!(envelope["is_error"], false);
    }
}

#[test]
fn responses_cli_edits_through_guarded_tools_and_restores_native_history_after_restart() {
    let _guard = local_e2e_guard();
    for streaming in [true, false] {
        let workspace = TestWorkspace::new("responses-api");
        fs::write(workspace.path("source.txt"), "alpha\nbeta\n").unwrap();
        workspace.disable_exec();
        let (endpoint, server) = spawn_fixture_http_server(vec![
            Box::new(move |request| {
                assert_replay(request, 0);
                let edit = request.body["tools"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|tool| tool["name"] == "file_edit")
                    .unwrap();
                assert_eq!(edit["strict"], false);
                assert!(edit["parameters"].get("oneOf").is_none());
                reply(
                    call("read-source", "file_read", json!({"path":"source.txt"})),
                    streaming,
                )
            }),
            Box::new(move |request| {
                assert_replay(request, 1);
                reply(
                    call(
                        "edit-source",
                        "file_edit",
                        json!({"path":"source.txt","edits":[{"old_text":"alpha","new_text":"first"},{"old_text":"beta","new_text":"second"}]}),
                    ),
                    !streaming,
                )
            }),
            Box::new(move |request| {
                assert_replay(request, 2);
                reply(text("done", "SOURCE_UPDATED"), streaming)
            }),
            Box::new(move |request| {
                assert_replay(request, 2);
                let input = request.body["input"].as_array().unwrap();
                assert_eq!(
                    input.iter().filter(|item| item["id"] == "msg-done").count(),
                    1
                );
                assert_eq!(
                    input
                        .iter()
                        .filter(|item| item["encrypted_content"] == "opaque-done")
                        .count(),
                    1
                );
                reply(text("recall", "RECORDED_CHANGE_AVAILABLE"), !streaming)
            }),
        ]);
        workspace.configure_local_openai(&format!("{endpoint}/v1/responses"));
        for (index, prompt) in [
            "Update both words in the source.",
            "Recall the recorded change.",
        ]
        .into_iter()
        .enumerate()
        {
            let mut command = local_agent_command(
                &workspace,
                "responses-session",
                "Use the available tools.",
                prompt,
                true,
                true,
            );
            command.args(["--reasoning", "xhigh"]);
            let output = run_to_completion(command, LOCAL_PROCESS_TIMEOUT);
            assert!(output.status.success(), "{}", output.stderr_text());
            let expected = if index == 0 {
                "SOURCE_UPDATED"
            } else {
                "RECORDED_CHANGE_AVAILABLE"
            };
            assert_eq!(output.stdout_text().matches(expected).count(), 1);
            assert!(!output.stdout_text().contains("opaque-"));
            assert_eq!(
                fs::read_to_string(workspace.path("source.txt")).unwrap(),
                "first\nsecond\n"
            );
        }
        assert_eq!(server.join().unwrap().len(), 4);
        assert_eq!(
            tool_exchanges(&session_records(&workspace)).len(),
            2,
            "restart must preserve completed effects"
        );
    }
}
