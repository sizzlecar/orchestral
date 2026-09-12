use super::*;

#[test]
fn local_input_auto_with_closed_stdin_delivers_without_advertising_input() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("input-auto");
    let (endpoint, server) = spawn_fixture_http_server(vec![Box::new(|request| {
        assert!(!model_request_has_tool(
            &request.body,
            "orchestral_request_input"
        ));
        openai_text_response("Inspection complete.")
    })]);
    workspace.configure_local_openai(&endpoint);
    let mut command =
        local_default_agent_command(&workspace, "input-auto", "Inspect the project", true, true);
    command.stdin(Stdio::null());
    let output = run_to_completion(command, LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success(), "{}", output.stderr_text());
    assert_eq!(output.stdout_text(), "Inspection complete.\n");
    assert_eq!(server.join().unwrap().len(), 1);
    assert_eq!(run_payload_count(&workspace, "request_opened"), 0);
    assert_eq!(run_payload_count(&workspace, "delivery_committed"), 1);
}

#[test]
fn local_input_interactive_reads_a_piped_reply_and_delivers_the_same_run() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("input-interactive");
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(|request| {
            assert!(model_request_has_tool(
                &request.body,
                "orchestral_request_input"
            ));
            openai_tool_response(
                "destination",
                "orchestral_request_input",
                json!({"prompt": "Which city should the report cover?"}),
            )
        }),
        Box::new(|request| {
            let results: Value =
                serde_json::from_str(&model_tool_results_json(&request.body)).unwrap();
            assert_eq!(
                results,
                json!([{
                    "is_error": false,
                    "result": {"content": [
                        orchestral_core::agent_protocol::wire::Content::text("Oslo")
                    ]}
                }])
            );
            openai_text_response("Report prepared for Oslo.")
        }),
    ]);
    workspace.configure_local_openai(&endpoint);
    let mut command = local_default_agent_command(
        &workspace,
        "input-interactive",
        "Prepare the requested city report",
        true,
        true,
    );
    command.args(["--input-mode", "interactive"]);
    let output = run_with_piped_input(command, b"Oslo\n", LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success(), "{}", output.stderr_text());
    assert_eq!(output.stdout_text(), "Report prepared for Oslo.\n");
    assert_eq!(server.join().unwrap().len(), 2);
    assert_eq!(journal_files(&workspace, "run-").len(), 1);
    assert_eq!(run_payload_count(&workspace, "request_opened"), 1);
    assert_eq!(run_payload_count(&workspace, "request_resolved"), 1);
    assert_eq!(run_payload_count(&workspace, "delivery_committed"), 1);
}

#[test]
fn local_input_config_disabled_remains_a_ceiling_for_interactive_mode() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("input-config-ceiling");
    workspace.rewrite_config(|config| {
        config["agent"]["input_requests_enabled"] = serde_yaml::Value::Bool(false);
    });
    let (endpoint, server) = spawn_fixture_http_server(vec![Box::new(|request| {
        assert!(!model_request_has_tool(
            &request.body,
            "orchestral_request_input"
        ));
        openai_text_response("Available information inspected.")
    })]);
    workspace.configure_local_openai(&endpoint);
    let mut command = local_default_agent_command(
        &workspace,
        "input-config-ceiling",
        "Inspect available information",
        true,
        true,
    );
    command.args(["--input-mode", "interactive"]);
    let output = run_with_piped_input(command, b"unused reply\n", LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success(), "{}", output.stderr_text());
    assert_eq!(server.join().unwrap().len(), 1);
    assert_eq!(run_payload_count(&workspace, "request_opened"), 0);
    assert_eq!(run_payload_count(&workspace, "delivery_committed"), 1);
}

#[test]
fn local_input_interactive_cannot_consume_the_initial_prompt_pipe() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("input-pipe-conflict");
    let mut command = root_command(&workspace);
    command.args(["--input-mode", "interactive", "--no-mcp", "--no-skills"]);
    command.stdin(Stdio::piped());
    let output = run_to_completion(command, LOCAL_PROCESS_TIMEOUT);
    assert!(!output.status.success());
    assert!(output
        .stderr_text()
        .contains("stdin for both the initial prompt and replies"));
    assert!(!workspace.path(".orchestral/agent-journal").exists());
}

#[test]
fn local_input_interactive_eof_cancels_instead_of_delivering() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("input-interactive-eof");
    let (endpoint, server) = spawn_fixture_http_server(vec![Box::new(|request| {
        assert!(model_request_has_tool(
            &request.body,
            "orchestral_request_input"
        ));
        openai_tool_response(
            "destination",
            "orchestral_request_input",
            json!({"prompt": "Which city should the report cover?"}),
        )
    })]);
    workspace.configure_local_openai(&endpoint);
    let mut command = local_default_agent_command(
        &workspace,
        "input-interactive-eof",
        "Prepare the requested city report",
        true,
        true,
    );
    command.args(["--input-mode", "interactive"]);
    command.stdin(Stdio::null());
    let output = run_to_completion(command, LOCAL_PROCESS_TIMEOUT);
    assert!(!output.status.success());
    assert!(output
        .stderr_text()
        .contains("CLI input closed during input request"));
    assert_eq!(server.join().unwrap().len(), 1);
    assert_eq!(run_payload_count(&workspace, "run_cancelled"), 1);
    assert_eq!(run_payload_count(&workspace, "delivery_committed"), 0);
}
