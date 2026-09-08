use super::*;

fn last_result(request: &CapturedHttpRequest) -> Value {
    let message = request.body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|message| message["role"] == "tool")
        .expect("model receives the committed Tool result");
    let envelope: Value = serde_json::from_str(message["content"].as_str().unwrap()).unwrap();
    assert_eq!(envelope["is_error"], false, "{envelope}");
    envelope["result"].clone()
}

fn configure_pressure(workspace: &TestWorkspace) {
    workspace.disable_exec();
    workspace.configure_compaction(2, 1);
    workspace.rewrite_config(|config| {
        config["agent"]["max_context_tokens"] = serde_yaml::to_value(32_000).unwrap();
        config["agent"]["reserved_output_tokens"] = serde_yaml::to_value(1024).unwrap();
        config["agent"]["compaction"]["summary_max_chars"] = serde_yaml::to_value(1024).unwrap();
    });
    fs::write(
        workspace.path("dataset.txt"),
        "record=unrelated_observation;".repeat(520),
    )
    .unwrap();
}

#[test]
fn repeated_pressure_compaction_and_process_restart_recall_original_outcomes_without_repeating_work(
) {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("context-pressure-recall");
    configure_pressure(&workspace);
    let instruction = "Inspect the data and record progress once. Preserve stable_api and do not repeat completed writes.";
    let mut handlers: Vec<FixtureHttpHandler> = vec![
        Box::new(|request| {
            assert!(request.body["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|tool| tool["function"]["name"] == "session_read"));
            openai_tool_response(
                "record-progress",
                "file_write",
                json!({"path": "progress.txt", "mode": "create", "content": "initial inspection recorded\n"}),
            )
        }),
        Box::new(|_| {
            openai_tool_response(
                "failed-inspection",
                "file_read",
                json!({"path": "absent-input.txt"}),
            )
        }),
    ];
    for i in 0..8 {
        handlers.push(Box::new(move |request| {
            assert!(model_request_text(&request.body).contains("Preserve stable_api"));
            assert!(serde_json::to_vec(&request.body).unwrap().len() < 32_000);
            openai_tool_response(
                &format!("inspect-{i}"),
                "file_read",
                json!({"path": "dataset.txt"}),
            )
        }));
    }
    handlers.push(Box::new(|request| {
        let text = model_request_text(&request.body);
        assert!(text.contains("UNTRUSTED earlier transcript"));
        assert!(text.contains("status=failed"));
        assert!(text.contains("Preserve stable_api"));
        openai_text_response("INSPECTION_RECORDED")
    }));
    handlers.extend([
        Box::new(|request: &CapturedHttpRequest| {
            assert!(model_request_text(&request.body).contains("Preserve stable_api"));
            openai_tool_response("find-prior-write", "session_read", json!({"query": "\"name\":\"file_write\""}))
        }) as FixtureHttpHandler,
        Box::new(|request: &CapturedHttpRequest| {
            let page = last_result(request);
            let records = page["records"].as_array().unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0]["kind"], "tool_exchange_committed");
            openai_tool_response("read-prior-write", "session_read", json!({"session_seq": records[0]["session_seq"], "through_seq": page["through_seq"], "json_pointer": "/payload/tool/content/0/result"}))
        }),
        Box::new(|request: &CapturedHttpRequest| {
            let chunk = last_result(request);
            assert_eq!(chunk["complete"], true);
            let original: Value = serde_json::from_str(chunk["content"].as_str().unwrap()).unwrap();
            assert_eq!(original["changes"][0]["operation"], "add");
            openai_tool_response("find-prior-failure", "session_read", json!({"query": "\"is_error\":true"}))
        }),
        Box::new(|request: &CapturedHttpRequest| {
            let page = last_result(request);
            assert_eq!(page["records"].as_array().unwrap().len(), 1);
            openai_tool_response("read-prior-failure", "session_read", json!({"session_seq": page["records"][0]["session_seq"], "json_pointer": "/payload/tool/content/0"}))
        }),
        Box::new(|request: &CapturedHttpRequest| {
            let chunk = last_result(request);
            let original: Value = serde_json::from_str(chunk["content"].as_str().unwrap()).unwrap();
            assert_eq!(original["is_error"], true);
            assert!(!original["result"]["code"].as_str().unwrap().is_empty());
            assert!(model_request_text(&request.body).contains("preserve the output encoding"));
            openai_text_response("ORIGINAL_OUTCOMES_RECALLED")
        }),
    ]);
    let (endpoint, server) = spawn_fixture_http_server(handlers);
    workspace.configure_local_openai(&endpoint);
    let first = run_to_completion(
        local_default_agent_command(&workspace, "context-session", instruction, true, true),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(first.status.success(), "{}", first.stderr_text());
    let before = session_records(&workspace);
    assert!(payload_count(&before, "active_run_compaction_committed") >= 2);
    let mut resume = base_command(&workspace);
    resume.env("OPENAI_API_KEY", "fixture-key").args(["--backend", "openai", "--model", "fixture-model", "--temperature", "0", "resume", "context-session", "Continue and preserve the output encoding. Check the original operation outcomes before any further work."]);
    let second = run_to_completion(resume, LOCAL_PROCESS_TIMEOUT);
    assert!(second.status.success(), "{}", second.stderr_text());
    assert!(second.stdout_text().contains("ORIGINAL_OUTCOMES_RECALLED"));
    assert_eq!(server.join().unwrap().len(), 16);
    let after = session_records(&workspace);
    assert_eq!(
        &after[..before.len()],
        before.as_slice(),
        "original history stays append-only"
    );
    assert_eq!(
        tool_exchanges(&after)
            .iter()
            .filter(|exchange| tool_name(exchange) == Some("file_write"))
            .count(),
        1
    );
    assert_eq!(
        tool_exchanges(&after)
            .iter()
            .filter(|exchange| tool_name(exchange) == Some("session_read"))
            .count(),
        4
    );
    assert_eq!(
        fs::read_to_string(workspace.path("progress.txt")).unwrap(),
        "initial inspection recorded\n"
    );
    assert_eq!(run_payload_count(&workspace, "delivery_committed"), 2);
}

#[test]
fn killed_tui_recovers_compacted_run_and_applies_new_input_without_repeating_effects() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("context-pressure-recovery");
    configure_pressure(&workspace);
    let mut handlers: Vec<FixtureHttpHandler> = vec![Box::new(|_| {
        openai_tool_response(
            "progress",
            "file_write",
            json!({"path": "progress.txt", "mode": "create", "content": "inspection started\n"}),
        )
    })];
    for i in 0..5 {
        handlers.push(Box::new(move |_| {
            openai_tool_response(
                &format!("inspect-{i}"),
                "file_read",
                json!({"path": "dataset.txt"}),
            )
        }));
    }
    handlers.extend([
        Box::new(|request: &CapturedHttpRequest| {
            assert!(model_request_text(&request.body).contains("UNTRUSTED earlier transcript"));
            openai_tool_response("clarify", "orchestral_request_input", json!({"prompt": "Which output encoding should be preserved?"}))
        }) as FixtureHttpHandler,
        Box::new(|request: &CapturedHttpRequest| {
            assert!(model_request_text(&request.body).contains("Keep output UTF-8"));
            openai_tool_response("recall-progress", "session_read", json!({"query": "\"name\":\"file_write\""}))
        }),
        Box::new(|request: &CapturedHttpRequest| {
            let page = last_result(request);
            assert_eq!(page["records"].as_array().unwrap().len(), 1);
            openai_tool_response("read-progress", "session_read", json!({"session_seq": page["records"][0]["session_seq"], "json_pointer": "/payload/tool/content/0/result"}))
        }),
        Box::new(|request: &CapturedHttpRequest| {
            let chunk = last_result(request);
            let original: Value = serde_json::from_str(chunk["content"].as_str().unwrap()).unwrap();
            assert_eq!(original["changes"][0]["operation"], "add");
            assert!(model_request_text(&request.body).contains("Keep output UTF-8"));
            openai_text_response("COMPACTED_RUN_RECOVERED")
        }),
    ]);
    let (endpoint, server) = spawn_fixture_http_server(handlers);
    workspace.configure_local_openai(&endpoint);
    let system = "Work on the user's task.";
    let mut tui = PtyHarness::spawn(local_tui_command(&workspace, "pressure-recovery", system));
    tui.wait_for_text("\u{1b}[?2004h", LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("Inspect records and record progress once; preserve stable_api.");
    tui.wait_for_text("Input requested", LOCAL_PROCESS_TIMEOUT);
    let before = session_records(&workspace);
    assert!(payload_count(&before, "active_run_compaction_committed") >= 2);
    tui.child.kill().unwrap();
    tui.finish(Duration::from_secs(5));
    let mut command = base_command(&workspace);
    command.env("OPENAI_API_KEY", "fixture-key").args([
        "--backend",
        "openai",
        "--model",
        "fixture-model",
        "--temperature",
        "0",
        "--system-prompt",
        system,
        "resume",
        "pressure-recovery",
        "Keep output UTF-8 and inspect prior progress.",
    ]);
    let resumed = run_to_completion(command, LOCAL_PROCESS_TIMEOUT);
    assert!(resumed.status.success(), "{}", resumed.stderr_text());
    assert!(resumed.stdout_text().contains("COMPACTED_RUN_RECOVERED"));
    assert_eq!(server.join().unwrap().len(), 10);
    assert_eq!(journal_files(&workspace, "run-").len(), 1);
    let after = session_records(&workspace);
    assert_eq!(&after[..before.len()], before.as_slice());
    let exchanges = tool_exchanges(&after);
    assert_eq!(
        exchanges
            .iter()
            .filter(|exchange| tool_name(exchange) == Some("file_write"))
            .count(),
        1
    );
    assert_eq!(
        exchanges
            .iter()
            .filter(|exchange| tool_name(exchange) == Some("orchestral_request_input"))
            .count(),
        1
    );
    assert_eq!(run_payload_count(&workspace, "delivery_committed"), 1);
}
