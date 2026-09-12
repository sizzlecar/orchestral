use super::*;

fn configure_retry(workspace: &TestWorkspace, max_retries: u32, delay_ms: u64) {
    workspace.rewrite_config(|config| {
        config["agent"]["model_retry"] = serde_yaml::to_value(json!({
            "max_retries": max_retries,
            "base_delay_ms": delay_ms,
            "max_delay_ms": delay_ms,
        }))
        .unwrap();
    });
}

fn http_error(status: &'static str) -> FixtureHttpResponse {
    FixtureHttpResponse {
        status,
        content_type: "application/json",
        repeat_handler: false,
        body: serde_json::to_vec(&json!({"error": {"message": "temporary model failure"}}))
            .unwrap(),
    }
}

#[test]
fn cli_retries_transient_model_failures_and_commits_one_delivery() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("model-retry");
    configure_retry(&workspace, 3, 10);
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(|_| http_error("429 Too Many Requests")),
        Box::new(|_| http_error("503 Service Unavailable")),
        Box::new(|_| openai_text_response("recovered normally")),
    ]);
    workspace.configure_local_openai(&endpoint);
    let output = run_to_completion(
        local_default_agent_command(&workspace, "retry", "Explain this project.", true, true),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(output.status.success(), "{}", output.stderr_text());
    assert_eq!(output.stdout_text(), "recovered normally\n");
    assert!(
        output.stderr_text().contains("retry 1/3"),
        "{}",
        output.stderr_text()
    );
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests.windows(2).all(|pair| pair[0].body == pair[1].body));
    assert_eq!(
        checkpoint_event_count(&workspace, "model_retry_scheduled"),
        2
    );
    assert_eq!(
        checkpoint_event_count(&workspace, "model_attempt_started"),
        1
    );
    assert_eq!(
        payload_count(&session_records(&workspace), "run_input_committed"),
        1
    );
    assert_eq!(run_payload_count(&workspace, "delivery_committed"), 1);
    assert_eq!(run_payload_count(&workspace, "run_failed"), 0);
}

#[test]
fn cli_retry_after_a_tool_result_does_not_repeat_the_file_effect() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("retry-after-effect");
    workspace.disable_exec();
    configure_retry(&workspace, 2, 10);
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(|_| {
            openai_tool_response(
                "create-document",
                "file_write",
                json!({
                    "path": "result.txt", "mode": "create", "content": "written once\n"
                }),
            )
        }),
        Box::new(|request| {
            assert!(model_request_text(&request.body).contains("\"operation\":\"add\""));
            http_error("503 Service Unavailable")
        }),
        Box::new(|request| {
            assert!(model_request_text(&request.body).contains("\"operation\":\"add\""));
            openai_tool_response("read-document", "file_read", json!({"path": "result.txt"}))
        }),
        Box::new(|request| {
            assert!(model_request_text(&request.body).contains("written once"));
            openai_text_response("file verified")
        }),
    ]);
    workspace.configure_local_openai(&endpoint);
    let output = run_to_completion(
        local_default_agent_command(
            &workspace,
            "retry-tool",
            "Create the requested document and verify it.",
            true,
            true,
        ),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(output.status.success(), "{}", output.stderr_text());
    assert_eq!(output.stdout_text().trim(), "file verified");
    assert_eq!(
        fs::read_to_string(workspace.path("result.txt")).unwrap(),
        "written once\n"
    );
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[1].body, requests[2].body);
    let records = session_records(&workspace);
    let exchanges = tool_exchanges(&records);
    assert_eq!(exchanges.len(), 2);
    assert_eq!(
        exchanges
            .iter()
            .filter(|exchange| tool_name(exchange) == Some("file_write"))
            .count(),
        1
    );
    assert_eq!(run_payload_count(&workspace, "delivery_committed"), 1);
    assert_eq!(
        checkpoint_event_count(&workspace, "model_retry_scheduled"),
        1
    );
}

#[test]
fn cli_bounds_retries_and_respects_disabled_retry_policy() {
    let _guard = local_e2e_guard();
    for retries in [0, 2] {
        let workspace = TestWorkspace::new("model-retry-exhausted");
        configure_retry(&workspace, retries, 10);
        let handlers = (0..=retries)
            .map(|_| {
                Box::new(|_: &CapturedHttpRequest| http_error("503 Service Unavailable"))
                    as FixtureHttpHandler
            })
            .collect();
        let (endpoint, server) = spawn_fixture_http_server(handlers);
        workspace.configure_local_openai(&endpoint);
        let output = run_to_completion(
            local_default_agent_command(
                &workspace,
                "exhausted",
                "Inspect the project.",
                true,
                true,
            ),
            LOCAL_PROCESS_TIMEOUT,
        );
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(
            output.stderr_text().contains("model_unavailable"),
            "{}",
            output.stderr_text()
        );
        assert_eq!(server.join().unwrap().len(), retries as usize + 1);
        assert_eq!(
            checkpoint_event_count(&workspace, "model_retry_scheduled"),
            retries as usize
        );
        assert_eq!(run_payload_count(&workspace, "run_failed"), 1);
        assert_eq!(run_payload_count(&workspace, "delivery_committed"), 0);
    }
}

#[test]
fn cli_does_not_retry_authentication_or_partially_observed_model_output() {
    let _guard = local_e2e_guard();
    for partial in [false, true] {
        let workspace = TestWorkspace::new("model-no-retry");
        configure_retry(&workspace, 2, 10);
        let (endpoint, server) = spawn_fixture_http_server(vec![Box::new(move |_| {
            if partial {
                // A tool call was partially generated, but never finished. The
                // Agent must neither execute it nor reissue the model attempt.
                let event = json!({"choices": [{"delta": {"tool_calls": [{
                    "index": 0, "id": "partial-write", "function": {
                        "name": "file_write", "arguments": "{\"path\":\"unsafe.txt\""
                    }
                }]}}]});
                sse_response(format!("data: {event}\n\n"))
            } else {
                http_error("401 Unauthorized")
            }
        })]);
        workspace.configure_local_openai(&endpoint);
        let output = run_to_completion(
            local_default_agent_command(&workspace, "no-retry", "Inspect the project.", true, true),
            LOCAL_PROCESS_TIMEOUT,
        );
        assert!(!output.status.success(), "{}", output.stdout_text());
        assert!(output.stdout.is_empty());
        assert_eq!(server.join().unwrap().len(), 1);
        assert_eq!(
            checkpoint_event_count(&workspace, "model_retry_scheduled"),
            0
        );
        assert!(!workspace.path("unsafe.txt").exists());
        assert_eq!(
            payload_count(&session_records(&workspace), "tool_exchange_committed"),
            0
        );
    }
}

#[test]
fn tui_can_cancel_during_model_retry_backoff() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("retry-cancel");
    configure_retry(&workspace, 3, 30_000);
    let (endpoint, server) =
        spawn_fixture_http_server(vec![Box::new(|_| http_error("429 Too Many Requests"))]);
    workspace.configure_local_openai(&endpoint);
    let mut tui = PtyHarness::spawn(local_tui_command(
        &workspace,
        "retry-cancel",
        "Work on the user's task.",
    ));
    tui.wait_for_text("\u{1b}[?2004h", LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("Inspect the project.");
    tui.wait_for_text("retry 1/3", LOCAL_PROCESS_TIMEOUT);
    tui.send(&[0x03]);
    tui.wait_for_text("cancelled", Duration::from_secs(5));
    tui.send(&[0x04]);
    tui.wait_for_terminal_restore(Duration::from_secs(5));
    let output = tui.finish(Duration::from_secs(5));
    assert!(output.status.success(), "{}", output.text());
    output.assert_terminal_restored();
    assert_eq!(server.join().unwrap().len(), 1);
    assert_eq!(run_payload_count(&workspace, "run_cancelled"), 1);
    assert_eq!(
        checkpoint_event_count(&workspace, "model_retry_scheduled"),
        1
    );
}

#[test]
fn tui_steer_interrupts_retry_backoff_and_rebuilds_the_model_context() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("retry-steer");
    configure_retry(&workspace, 3, 30_000);
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(|_| http_error("429 Too Many Requests")),
        Box::new(|request| {
            assert!(model_request_text(&request.body).contains("Focus on the parser first."));
            openai_text_response("STEER_AFTER_RETRY_OK")
        }),
    ]);
    workspace.configure_local_openai(&endpoint);
    let mut tui = PtyHarness::spawn(local_tui_command(
        &workspace,
        "retry-steer",
        "Work on the user's task.",
    ));
    tui.wait_for_text("\u{1b}[?2004h", LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("Inspect the project.");
    tui.wait_for_text("retry 1/3", LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("Focus on the parser first.");
    tui.wait_for_text("STEER_AFTER_RETRY_OK", Duration::from_secs(5));
    tui.wait_for_text("○ replied", Duration::from_secs(5));
    tui.send(&[0x04]);
    tui.wait_for_terminal_restore(Duration::from_secs(5));
    let output = tui.finish(Duration::from_secs(5));
    assert!(output.status.success(), "{}", output.text());
    assert_eq!(server.join().unwrap().len(), 2);
    assert_eq!(run_payload_count(&workspace, "delivery_committed"), 1);
    assert_eq!(
        checkpoint_event_count(&workspace, "model_retry_scheduled"),
        1
    );
}
