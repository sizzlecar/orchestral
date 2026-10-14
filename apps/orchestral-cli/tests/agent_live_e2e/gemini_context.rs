use super::*;

fn gemini_response(part: Value) -> FixtureHttpResponse {
    gemini_parts_response(vec![part])
}

fn gemini_parts_response(parts: Vec<Value>) -> FixtureHttpResponse {
    let response = json!({
        "candidates":[{"content":{"role":"model","parts":parts},"finishReason":"STOP"}],
        "usageMetadata":{"promptTokenCount":1500,"candidatesTokenCount":32},
    });
    sse_response(format!("data: {response}\n\n"))
}

#[test]
fn gemini_compacts_latest_tool_result_and_continues_without_reexecution() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("gemini-context-continuation");
    fs::write(
        workspace.path("large.txt"),
        "observation=sample\n".repeat(5_000),
    )
    .unwrap();
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(|request| {
            assert_eq!(
                request.body["contents"].as_array().unwrap().last().unwrap()["role"],
                "user"
            );
            gemini_parts_response(vec![
                json!({"functionCall":{
                "id":"inspect-once", "name":"exec_command",
                "args":{"cmd":"printf 'once\\n' >> marker.txt; cat large.txt", "wait_mode":"completion", "yield_time_ms":10000}
            }, "thoughtSignature":"opaque-provider-continuation"}),
                json!({"functionCall":{
                    "id":"invalid-reference", "name":"artifact_read", "args":{"artifact_ref":"unknown-reference"}
                }}),
            ])
        }),
        Box::new(|request| {
            let contents = request.body["contents"].as_array().unwrap();
            assert_eq!(
                contents.last().unwrap()["role"],
                "user",
                "Gemini rejects model-prefill after compaction"
            );
            let text = contents.to_vec();
            let text = serde_json::to_string(&text).unwrap();
            assert!(
                text.contains("historical_context_summary"),
                "pressure must compact the latest completed exchange"
            );
            assert!(text.contains("UNTRUSTED earlier transcript"));
            assert!(
                text.contains("artifact_reference_invalid"),
                "compaction must retain the failed read outcome"
            );
            gemini_response(json!({"functionCall":{
                "id":"verify-marker", "name":"file_read", "args":{"path":"marker.txt"}
            }, "thoughtSignature":"next-opaque-continuation"}))
        }),
        Box::new(|request| {
            let contents = request.body["contents"].as_array().unwrap();
            let last = contents.last().unwrap();
            assert_eq!(last["role"], "user");
            assert_eq!(
                last["parts"][0]["functionResponse"]["response"]["result"]["content"],
                "once\n"
            );
            assert_eq!(
                contents[contents.len() - 2]["parts"][0]["thoughtSignature"],
                "next-opaque-continuation"
            );
            gemini_response(json!({"text":"Completed and verified the single execution."}))
        }),
    ]);
    workspace.rewrite_config(|config| {
        config["agent"]["max_context_tokens"] = serde_yaml::to_value(16_000).unwrap();
        config["agent"]["reserved_output_tokens"] = serde_yaml::to_value(1024).unwrap();
        config["agent"]["compaction"]["summary_max_chars"] = serde_yaml::to_value(1024).unwrap();
        config["tools"]["max_inline_output_bytes"] = serde_yaml::to_value(128 * 1024).unwrap();
        let google = config["providers"]["backends"]
            .as_sequence_mut()
            .unwrap()
            .iter_mut()
            .find(|backend| backend["name"].as_str() == Some("google"))
            .unwrap();
        google["endpoint"] = serde_yaml::Value::String(endpoint);
        google["config"]["discover_model_capabilities"] = serde_yaml::Value::Bool(false);
    });
    let mut command = root_command(&workspace);
    command
        .env("ORCHESTRAL_HOME", workspace.path("user-config"))
        .env("GOOGLE_API_KEY", "fixture-key")
        .args([
            "--backend",
            "google",
            "--model",
            "fixture-model",
            "--session-id",
            "gemini-pressure",
            "--no-mcp",
            "--no-skills",
        ])
        .arg("Inspect the command output once, then verify its marker.");
    let output = run_to_completion(command, LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success(), "{}", output.stderr_text());
    assert_eq!(
        fs::read_to_string(workspace.path("marker.txt")).unwrap(),
        "once\n"
    );
    assert_eq!(server.join().unwrap().len(), 3);
}

// Seed public metadata to test the startup/request boundary without network discovery.
fn configure_gemini_fixture(workspace: &TestWorkspace, endpoint: &str) {
    workspace.rewrite_config(|config| {
        config["agent"]
            .as_mapping_mut()
            .unwrap()
            .remove("reserved_output_tokens");
        config["agent"]
            .as_mapping_mut()
            .unwrap()
            .remove("max_context_tokens");
        let google = config["providers"]["backends"]
            .as_sequence_mut()
            .unwrap()
            .iter_mut()
            .find(|backend| backend["name"].as_str() == Some("google"))
            .unwrap();
        google["endpoint"] = serde_yaml::Value::String(endpoint.to_owned());
        google["config"]["auth"] = serde_yaml::Value::String("api_key".to_owned());
    });
    let identity =
        serde_json::to_vec(&json!({"endpoint":endpoint,"model":"fixture-model","catalog":null}))
            .unwrap();
    let cache = workspace.path("user-config/cache/model-metadata");
    fs::create_dir_all(&cache).unwrap();
    let key = orchestral_core::agent_protocol::wire::Digest::sha256(identity);
    fs::write(cache.join(format!("{key}.json")), serde_json::to_vec(&json!({
        "version":1,"fetched_at":SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        "metadata":{"context_tokens":1_048_576,"output_tokens":65_536,"source":"fixture models API"}
    })).unwrap()).unwrap();
}

fn gemini_fixture_command(workspace: &TestWorkspace, session: &str) -> Command {
    let mut command = root_command(workspace);
    command
        .env("ORCHESTRAL_HOME", workspace.path("user-config"))
        .env("GOOGLE_API_KEY", "fixture-key")
        .args([
            "--backend",
            "google",
            "--model",
            "fixture-model",
            "--session-id",
            session,
            "--no-mcp",
            "--no-skills",
        ]);
    command
}

#[test]
fn gemini_uses_declared_output_capacity_and_reused_native_ids_execute_once_per_request() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("gemini-native-id-reuse");
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(|request| {
            assert_eq!(request.body["generationConfig"]["maxOutputTokens"], 65_536);
            assert_eq!(
                request.body["generationConfig"]["thinkingConfig"],
                json!({"includeThoughts":true})
            );
            gemini_parts_response(vec![
                json!({"text":"Preparing the first operation.","thought":true}),
                json!({"functionCall":{"id":"reused","name":"exec_command","args":{
                    "cmd":"printf 'first\\n' >> marker.txt","wait_mode":"completion","yield_time_ms":10000
                }},"thoughtSignature":"signature-one"}),
            ])
        }),
        Box::new(|request| {
            let contents = request.body["contents"].as_array().unwrap();
            let text = serde_json::to_string(contents).unwrap();
            assert!(
                !text.contains("Preparing the first operation."),
                "summaries must not enter continuation context"
            );
            assert_eq!(
                contents[contents.len() - 1]["parts"][0]["functionResponse"]["id"],
                "reused"
            );
            assert_eq!(
                contents[contents.len() - 2]["parts"][0]["thoughtSignature"],
                "signature-one"
            );
            gemini_parts_response(vec![
                json!({"text":"Preparing the second operation.","thought":true}),
                json!({"functionCall":{"id":"reused","name":"exec_command","args":{
                    "cmd":"printf 'second\\n' >> marker.txt","wait_mode":"completion","yield_time_ms":10000
                }}}),
            ])
        }),
        Box::new(|request| {
            let text = serde_json::to_string(&request.body["contents"]).unwrap();
            assert!(!text.contains("call_identity_conflict"));
            assert!(!text.contains("Preparing the second operation."));
            gemini_response(json!({"text":"Both operations completed."}))
        }),
    ]);
    configure_gemini_fixture(&workspace, &endpoint);
    let mut command = gemini_fixture_command(&workspace, "native-reuse");
    command.arg("Append both markers once.");
    let output = run_to_completion(command, LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success(), "{}", output.stderr_text());
    assert!(output.stdout_text().contains("Both operations completed."));
    assert!(!output.stdout_text().contains("Preparing the"));
    assert_eq!(
        fs::read_to_string(workspace.path("marker.txt")).unwrap(),
        "first\nsecond\n"
    );
    assert_eq!(server.join().unwrap().len(), 3);
    assert_eq!(run_payload_count(&workspace, "delivery_committed"), 1);
}

#[test]
fn gemini_output_exhaustion_is_incomplete_with_usage_and_never_retried() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("gemini-output-exhaustion");
    let (endpoint, server) = spawn_fixture_http_server(vec![Box::new(|request| {
        assert_eq!(request.body["generationConfig"]["maxOutputTokens"], 1024);
        let response = json!({"candidates":[{"content":{"parts":[
            {"thought":true,"text":"Checking the constraints."}
        ]},"finishReason":"MAX_TOKENS"}],
        "usageMetadata":{"promptTokenCount":1500,"thoughtsTokenCount":1024,"candidatesTokenCount":0}});
        sse_response(format!("data: {response}\n\n"))
    })]);
    configure_gemini_fixture(&workspace, &endpoint);
    workspace.rewrite_config(|config| {
        config["agent"]["reserved_output_tokens"] = serde_yaml::to_value(1024).unwrap()
    });
    let mut command = gemini_fixture_command(&workspace, "exhaustion");
    command.arg("Inspect this project.");
    let output = run_to_completion(command, LOCAL_PROCESS_TIMEOUT);
    assert!(!output.status.success());
    assert!(!output.stderr_text().contains("temporarily unavailable"));
    assert_eq!(server.join().unwrap().len(), 1);
    assert_eq!(
        checkpoint_event_count(&workspace, "model_retry_scheduled"),
        0
    );
    assert_eq!(run_payload_count(&workspace, "run_incomplete"), 1);
    assert_eq!(run_payload_count(&workspace, "run_failed"), 0);
    assert_eq!(run_payload_count(&workspace, "delivery_committed"), 0);
    let checkpoint: Value = serde_json::from_slice(
        &fs::read(&journal_files(&workspace, "generic-checkpoint-")[0]).unwrap(),
    )
    .unwrap();
    let observation = checkpoint["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|record| record["payload"]["type"] == "model_attempt_observed")
        .unwrap();
    assert_eq!(
        observation["payload"]["observation"]["usage"]["output_tokens"],
        1024
    );
}

#[test]
fn gemini_tui_shows_the_summary_while_waiting_for_the_next_model_response() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("gemini-tui-summary");
    fs::write(workspace.path("note.txt"), "fixture evidence").unwrap();
    let (waiting_tx, waiting_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(|_| {
            gemini_parts_response(vec![
                json!({"text":"Inspecting the interface before editing.","thought":true}),
                json!({"functionCall":{"id":"read","name":"file_read","args":{"path":"note.txt"}}}),
            ])
        }),
        Box::new(move |_| {
            waiting_tx.send(()).unwrap();
            release_rx.recv_timeout(LOCAL_PROCESS_TIMEOUT).unwrap();
            gemini_response(json!({"text":"The interface check is complete."}))
        }),
    ]);
    configure_gemini_fixture(&workspace, &endpoint);
    let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_orchestral"));
    command.cwd(&workspace.root);
    command.env("ORCHESTRAL_HOME", workspace.path("user-config"));
    command.env("GOOGLE_API_KEY", "fixture-key");
    command.args([
        "--config",
        workspace.path("orchestral.yaml").to_str().unwrap(),
        "--backend",
        "google",
        "--model",
        "fixture-model",
        "--session-id",
        "summary-tui",
        "--no-mcp",
        "--no-skills",
    ]);
    let mut tui = PtyHarness::spawn_with_size(command, 120, 32);
    tui.wait_for_screen(
        |screen| screen.contains("fixture-model"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("Read note.txt and report the result.");
    waiting_rx.recv_timeout(LOCAL_PROCESS_TIMEOUT).unwrap();
    tui.wait_for_screen(
        |screen| {
            screen.contains("Thinking summary")
                && screen.contains("Inspecting the interface before editing.")
        },
        LOCAL_PROCESS_TIMEOUT,
    );
    release_tx.send(()).unwrap();
    tui.wait_for_screen(
        |screen| screen.contains("replied") && screen.contains("The interface check is complete."),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x04");
    let output = tui.finish(LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success(), "{}", output.text());
    output.assert_terminal_restored();
    assert_eq!(server.join().unwrap().len(), 2);
}
