use super::*;
use orchestral_core::model_protocol::ModelTokenAccounting;
use orchestral_runtime::GenericCheckpointEvent;

const PRESSURE_CONTEXT_TOKENS: u64 = 12_000;
const PRESSURE_OUTPUT_TOKENS: u64 = 1024;

fn last_result(request: &CapturedHttpRequest) -> Value {
    let message = request.body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|message| message["role"] == "tool")
        .expect("model receives the committed Tool result");
    let envelope: Value = serde_yaml::from_str(message["content"].as_str().unwrap()).unwrap();
    assert_eq!(envelope["is_error"], false, "{envelope}");
    envelope["result"].clone()
}

fn configure_pressure(workspace: &TestWorkspace) {
    workspace.disable_exec();
    workspace.configure_compaction(2, 1);
    workspace.rewrite_config(|config| {
        // One dataset result fits the native planning estimate, while accumulated
        // results require compaction. This is a token budget, not a JSON byte cap.
        config["agent"]["max_context_tokens"] =
            serde_yaml::to_value(PRESSURE_CONTEXT_TOKENS).unwrap();
        config["agent"]["reserved_output_tokens"] =
            serde_yaml::to_value(PRESSURE_OUTPUT_TOKENS).unwrap();
        config["agent"]["compaction"]["summary_max_chars"] = serde_yaml::to_value(1024).unwrap();
        // This fixture deliberately retains large observations to exercise
        // aggregate context pressure and recovery. Default early Artifact
        // spilling is covered by context_output, and would remove this trigger.
        config["tools"]["max_inline_output_bytes"] = serde_yaml::to_value(64 * 1024).unwrap();
    });
    fs::write(
        workspace.path("dataset.txt"),
        "record=unrelated_observation;".repeat(520),
    )
    .unwrap();
}

#[cfg(unix)]
#[test]
fn real_http_capacity_rejection_compacts_without_repeating_an_exec_effect() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("http-context-rejection");
    workspace.rewrite_config(|config| {
        config["agent"]["max_context_tokens"] =
            serde_yaml::to_value(PRESSURE_CONTEXT_TOKENS).unwrap();
        config["agent"]["reserved_output_tokens"] =
            serde_yaml::to_value(PRESSURE_OUTPUT_TOKENS).unwrap();
        config["agent"]["compaction"]["summary_max_chars"] = serde_yaml::to_value(1024).unwrap();
        config["tools"]["max_inline_output_bytes"] = serde_yaml::to_value(64 * 1024).unwrap();
    });
    fs::write(
        workspace.path("dataset.txt"),
        "record=unrelated_observation;".repeat(520),
    )
    .unwrap();
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(|_| {
            openai_tool_response(
                "append-once",
                "exec_command",
                json!({
                    "cmd":"printf 'once\\n' >> marker.txt", "wait_mode":"completion", "yield_time_ms":10000,
                }),
            )
        }),
        Box::new(|request| {
            assert_eq!(
                model_tool_result_envelopes(&request.body).last().unwrap()["result"]["exit_code"],
                0
            );
            openai_tool_response("read-dataset", "file_read", json!({"path":"dataset.txt"}))
        }),
        Box::new(|request| {
            assert!(model_request_text(&request.body).contains("record=unrelated_observation;"));
            FixtureHttpResponse {
                status: "400 Bad Request",
                content_type: "application/json",
                repeat_handler: false,
                body: serde_json::to_vec(&json!({"error":{
                    "type":"invalid_request_error", "code":"context_length_exceeded",
                    "message":"the input and output reservation exceeds model capacity",
                }}))
                .unwrap(),
            }
        }),
        Box::new(|request| {
            let text = model_request_text(&request.body);
            assert!(text.contains("UNTRUSTED earlier transcript"), "{text}");
            assert!(text.contains("Preserve stable_api"));
            openai_tool_response("verify-once", "file_read", json!({"path":"marker.txt"}))
        }),
        Box::new(|request| {
            assert_eq!(
                model_tool_result_envelopes(&request.body).last().unwrap()["result"]["content"],
                "once\n"
            );
            openai_text_response("The prior effect ran once and its result was verified.")
        }),
    ]);
    workspace.configure_local_openai(&endpoint);
    let output = run_to_completion(
        local_default_agent_command(
            &workspace,
            "http-capacity-session",
            "Record the inspection once and verify it. Preserve stable_api.",
            true,
            true,
        ),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(output.status.success(), "{}", output.stderr_text());
    assert!(!output.stderr_text().contains(APPROVAL_PROMPT));
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 5);
    assert_eq!(
        &requests[2].body["messages"].as_array().unwrap()[..2],
        &requests[3].body["messages"].as_array().unwrap()[..2]
    );
    assert!(
        serde_json::to_vec(&requests[3].body).unwrap().len()
            < serde_json::to_vec(&requests[2].body).unwrap().len()
    );
    assert_eq!(
        checkpoint_event_count(&workspace, "model_context_rejected"),
        1
    );
    assert_eq!(run_payload_count(&workspace, "delivery_committed"), 1);
    assert_eq!(fs::read(workspace.path("marker.txt")).unwrap(), b"once\n");
}

fn assert_planning_boundaries(workspace: &TestWorkspace) {
    let mut saw_attempt = false;
    let mut planning_differs_from_hard_reservation = false;
    for path in journal_files(workspace, "generic-checkpoint-") {
        let journal: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        for record in journal["records"].as_array().unwrap() {
            let event: GenericCheckpointEvent =
                serde_json::from_value(record["payload"].clone()).unwrap();
            if let GenericCheckpointEvent::ModelAttemptStarted { context, .. } = event {
                saw_attempt = true;
                let estimate = context.context_estimate.expect("native planning trace");
                assert_eq!(estimate.accounting, ModelTokenAccounting::Estimated);
                assert_eq!(
                    context.input_budget_tokens,
                    PRESSURE_CONTEXT_TOKENS - PRESSURE_OUTPUT_TOKENS
                );
                assert!(estimate.tokens <= context.input_budget_tokens);
                assert!(context.used_input_tokens >= estimate.tokens);
                planning_differs_from_hard_reservation |=
                    context.used_input_tokens > context.input_budget_tokens;
            }
        }
    }
    assert!(saw_attempt, "read actual model planning boundaries");
    assert!(
        planning_differs_from_hard_reservation,
        "a conservative wire reservation must not become the planning estimate"
    );
}

#[test]
fn cli_capacity_recovery_honors_disable_and_leaves_ordinary_bad_requests_terminal() {
    let _guard = local_e2e_guard();
    for (code, max_retries) in [
        (Some("context_length_exceeded"), 0),
        (Some("invalid_parameter"), 1),
        (None, 1),
    ] {
        let workspace = TestWorkspace::new("capacity-recovery-disabled-or-invalid");
        workspace.disable_exec();
        if max_retries != 1 {
            workspace.rewrite_config(|config| {
                config["agent"]["context_recovery"] =
                    serde_yaml::to_value(json!({"max_retries":max_retries})).unwrap();
            });
        }
        let (endpoint, server) =
            spawn_fixture_http_server(vec![Box::new(move |_| FixtureHttpResponse {
                status: "400 Bad Request",
                content_type: "application/json",
                repeat_handler: false,
                body: serde_json::to_vec(
                    &json!({"error":{"code":code,"message":"request rejected"}}),
                )
                .unwrap(),
            })]);
        workspace.configure_local_openai(&endpoint);
        let output = run_to_completion(
            local_default_agent_command(
                &workspace,
                "rejection-session",
                "Inspect the workspace.",
                true,
                true,
            ),
            LOCAL_PROCESS_TIMEOUT,
        );
        assert!(!output.status.success());
        assert_eq!(server.join().unwrap().len(), 1);
        assert_eq!(
            checkpoint_event_count(&workspace, "model_context_rejected"),
            0
        );
        assert_eq!(run_payload_count(&workspace, "run_failed"), 1);
    }
}

#[test]
fn default_summary_capacity_adapts_to_context_before_the_next_tool_round() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("default-summary-pressure");
    configure_pressure(&workspace);
    workspace.rewrite_config(|config| {
        // Exercise the product default, whose character ceiling is larger
        // than the remaining input space of this small context window.
        config["agent"]["compaction"]
            .as_mapping_mut()
            .unwrap()
            .remove(serde_yaml::Value::String("summary_max_chars".to_owned()));
    });
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(|_| {
            openai_tool_response("inspect-first", "file_read", json!({"path": "dataset.txt"}))
        }),
        Box::new(|_| {
            openai_tool_response("inspect-again", "file_read", json!({"path": "dataset.txt"}))
        }),
        Box::new(|request| {
            let context = model_request_text(&request.body);
            assert!(
                context.contains("UNTRUSTED earlier transcript"),
                "{context}"
            );
            assert!(context.contains("Preserve stable_api"));
            openai_tool_response(
                "record-inspection",
                "file_write",
                json!({"path": "inspection.txt", "mode": "create", "content": "inspection recorded\n"}),
            )
        }),
        Box::new(|_| {
            openai_tool_response(
                "verify-record",
                "file_read",
                json!({"path": "inspection.txt"}),
            )
        }),
        Box::new(|request| {
            assert_eq!(last_result(request)["content"], "inspection recorded\n");
            openai_text_response("Inspection recorded and read back.")
        }),
    ]);
    workspace.configure_local_openai(&endpoint);
    let output = run_to_completion(
        local_default_agent_command(
            &workspace,
            "default-summary-session",
            "Inspect the dataset and record the inspection. Preserve stable_api.",
            true,
            true,
        ),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(output.status.success(), "{}", output.stderr_text());
    assert_planning_boundaries(&workspace);
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 5);
    let initial = requests[0].body["messages"].as_array().unwrap();
    let compacted = requests[2].body["messages"].as_array().unwrap();
    assert_eq!(initial[0]["role"], "system");
    assert_eq!(initial[1]["role"], "user");
    assert_eq!(&compacted[..2], &initial[..2]);
    assert_eq!(compacted[2]["role"], "assistant");
    assert!(compacted[2]["content"]
        .as_str()
        .unwrap()
        .starts_with("UNTRUSTED earlier transcript"));
    assert!(compacted[2].get("tool_calls").is_none());
    assert!(
        payload_count(
            &session_records(&workspace),
            "active_run_compaction_committed"
        ) > 0
    );
    assert_eq!(run_payload_count(&workspace, "delivery_committed"), 1);
    assert_eq!(
        fs::read_to_string(workspace.path("inspection.txt")).unwrap(),
        "inspection recorded\n"
    );
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
    assert_planning_boundaries(&workspace);
    let before = session_records(&workspace);
    assert!(payload_count(&before, "active_run_compaction_committed") >= 2);
    let mut resume = base_command(&workspace);
    resume.env("OPENAI_API_KEY", "fixture-key").args(["--backend", "openai", "--model", "fixture-model", "--temperature", "0", "resume", "context-session", "Continue and preserve the output encoding. Check the original operation outcomes before any further work."]);
    let second = run_to_completion(resume, LOCAL_PROCESS_TIMEOUT);
    assert!(second.status.success(), "{}", second.stderr_text());
    assert_planning_boundaries(&workspace);
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
    // Exercise the kill/accepted-answer boundary with independent processes.
    for _ in 0..3 {
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
        assert_planning_boundaries(&workspace);
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
            "--input-mode",
            "interactive",
            "resume",
            "pressure-recovery",
            "Keep output UTF-8 and inspect prior progress.",
        ]);
        let resumed = run_to_completion(command, LOCAL_PROCESS_TIMEOUT);
        assert!(resumed.status.success(), "{}", resumed.stderr_text());
        assert_planning_boundaries(&workspace);
        assert!(
            !resumed.stderr_text().contains("Input required:"),
            "an already accepted resume answer must not prompt again"
        );
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
}
