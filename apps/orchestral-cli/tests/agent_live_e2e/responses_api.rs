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

#[test]
fn responses_tui_sends_large_images_and_restores_only_readable_answers() {
    use base64::{engine::general_purpose::STANDARD, Engine};

    let exit = |mut tui: PtyHarness| {
        tui.send(b"\x04");
        assert!(tui.finish(LOCAL_PROCESS_TIMEOUT).status.success());
    };
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("responses-vision-history");
    // Only the image header is inspected by the local transport fixture. Its
    // encoded payload exceeds the entire 128K context window when read as text.
    let mut image = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
    image.extend(1600_u32.to_be_bytes());
    image.extend(1000_u32.to_be_bytes());
    image.resize(900_000, 0);
    let path = workspace.path("screenshot.png");
    fs::write(&path, &image).unwrap();
    let expected = format!("data:image/png;base64,{}", STANDARD.encode(&image));
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(move |request| {
            assert_eq!(request.path, "/v1/responses");
            let input = request.body["input"].as_array().unwrap();
            let user = input.iter().find(|item| item["role"] == "user").unwrap();
            assert_eq!(user["content"][1]["type"], "input_image");
            assert_eq!(user["content"][1]["image_url"], expected);
            reply(text("image-answer", "IMAGE_ANSWER_VISIBLE"), true)
        }),
        Box::new(move |request| {
            let input = request.body["input"].as_array().unwrap();
            assert_eq!(
                input
                    .iter()
                    .filter(|item| item["id"] == "msg-image-answer")
                    .count(),
                1
            );
            assert_eq!(
                input
                    .iter()
                    .filter(|item| item["encrypted_content"] == "opaque-image-answer")
                    .count(),
                1
            );
            reply(text("followup", "CONTINUATION_REPLAY_OK"), false)
        }),
    ]);
    workspace.configure_local_openai(&format!("{endpoint}/v1/responses"));
    workspace.rewrite_config(|config| {
        config["agent"]["max_context_tokens"] = serde_yaml::to_value(128_000).unwrap();
    });
    let command = || local_tui_command(&workspace, "vision-session", "Inspect the user's image.");
    let mut first = PtyHarness::spawn(command());
    first.wait_for_screen(|s| s.contains("Ask Orchestral"), LOCAL_PROCESS_TIMEOUT);
    first.send(format!("\x1b[200~{}\x1b[201~", path.display()).as_bytes());
    first.wait_for_screen(|s| s.contains("Attached [Image #1]"), LOCAL_PROCESS_TIMEOUT);
    first.send(b" Describe this image\r");
    let live = first.wait_for_screen(
        |s| s.contains("IMAGE_ANSWER_VISIBLE") && s.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert_eq!(live.matches("IMAGE_ANSWER_VISIBLE").count(), 1);
    assert!(!live.contains("opaque-image-answer"));
    exit(first);

    let mut resumed = PtyHarness::spawn(command());
    let history = resumed.wait_for_screen(
        |s| s.contains("IMAGE_ANSWER_VISIBLE") && s.contains("Ask Orchestral"),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert_eq!(history.matches("IMAGE_ANSWER_VISIBLE").count(), 1);
    for hidden in [
        "opaque-image-answer",
        "encrypted_content",
        "responses-output/v1",
        "iVBORw0KGgo",
    ] {
        assert!(
            !history.contains(hidden),
            "private state appeared in restored history"
        );
    }
    resumed.send_paste("Continue from the image answer.");
    resumed.wait_for_screen(
        |s| s.contains("CONTINUATION_REPLAY_OK") && s.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    exit(resumed);
    assert_eq!(server.join().unwrap().len(), 2);
    let records = session_records(&workspace);
    let original = records.iter().find(|record| {
        record["payload"]["type"] == "run_output_committed"
            && record["payload"]["message"]["content"]
                .to_string()
                .contains("opaque-image-answer")
    });
    assert!(
        original.is_some(),
        "rendering must preserve native continuation in the journal"
    );
}
