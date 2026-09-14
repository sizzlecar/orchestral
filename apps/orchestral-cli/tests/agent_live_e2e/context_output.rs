use super::*;

#[derive(Default)]
struct OutputObservation {
    artifact: Option<(Value, Vec<u8>)>,
    stdout: String,
    stderr: String,
    completed: bool,
    call_sequence: usize,
}

fn read_page(artifact: &Value, offset: usize, ordinal: usize) -> FixtureHttpResponse {
    let mut response = openai_tool_response(
        &format!("read-output-{ordinal}"),
        "artifact_read",
        json!({
            "artifact_ref":artifact["artifact"]["artifact_ref"],
            "digest":artifact["artifact"]["digest"],
            "media_type":artifact["media_type"], "byte_size":artifact["byte_size"],
            "offset":offset, "max_bytes":64 * 1024,
        }),
    );
    response.repeat_handler = true;
    response
}

#[test]
fn cli_large_exec_output_preserves_nonzero_status_and_reads_without_reexecution() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("stored-command-output");
    let expected_stdout = (0..90)
        .map(|index| format!("{index:032x} quoted=\"\\n\" unicode=你好🦀\n"))
        .collect::<String>();
    fs::write(workspace.path("output.txt"), &expected_stdout).unwrap();
    workspace.rewrite_config(|config| {
        // Use the product default, including its declared 2 KiB model view
        // budget for this window. Do not override the tool inline threshold.
        config["agent"]["max_context_tokens"] = serde_yaml::to_value(12_288).unwrap();
        config["agent"]["reserved_output_tokens"] = serde_yaml::to_value(4_096).unwrap();
    });
    let observation = Arc::new(Mutex::new(OutputObservation::default()));
    let handler_observation = observation.clone();
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(|_| {
            openai_tool_response(
                "execute-once",
                "exec_command",
                json!({
                    "cmd":"cat output.txt; printf 'compiler diagnostic\\n' >&2; printf 'run\\n' >> runs.txt; exit 17",
                    "wait_mode":"completion", "yield_time_ms":10000,
                }),
            )
        }),
        Box::new(move |request| {
            let envelopes = model_tool_result_envelopes(&request.body);
            let envelope = envelopes.last().unwrap();
            assert_eq!(envelope["is_error"], false, "{envelope}");
            let mut state = handler_observation.lock().unwrap();
            state.call_sequence += 1;
            let ordinal = state.call_sequence;
            let result = &envelope["result"];
            assert!(
                serde_json::to_vec(result).unwrap().len() <= 2048,
                "{result}"
            );
            if result["kind"] == "artifact" {
                assert!(state.artifact.is_none());
                state.artifact = Some((result.clone(), Vec::new()));
                return read_page(result, 0, ordinal);
            }
            let result = if let Some((artifact, bytes)) = &mut state.artifact {
                assert_eq!(result["artifact_ref"], artifact["artifact"]["artifact_ref"]);
                assert_eq!(result["digest"], artifact["artifact"]["digest"]);
                assert_eq!(result["offset"], bytes.len());
                let content = result["content"].as_str().unwrap();
                bytes.extend_from_slice(content.as_bytes());
                assert_eq!(result["next_offset"], bytes.len());
                if result["complete"] != true {
                    assert!(!content.is_empty());
                    return read_page(artifact, bytes.len(), ordinal);
                }
                assert_eq!(artifact["byte_size"], bytes.len());
                let output = serde_json::from_slice::<Value>(bytes).unwrap();
                assert_eq!(
                    artifact["artifact"]["digest"],
                    json!(orchestral_core::agent_protocol::wire::Digest::sha256(bytes)),
                );
                state.artifact = None;
                output
            } else {
                result.clone()
            };
            state.stdout.push_str(result["stdout"].as_str().unwrap());
            state.stderr.push_str(result["stderr"].as_str().unwrap());
            assert_ne!(result["truncated"], true);
            if result["alive"] == true {
                let mut response = openai_tool_response(
                    &format!("poll-output-{ordinal}"),
                    "write_stdin",
                    json!({"session_id":result["session_id"], "wait_mode":"completion", "yield_time_ms":10000}),
                );
                response.repeat_handler = true;
                return response;
            }
            assert_eq!(result["exit_code"], 17, "{result}");
            assert!(result.get("session_id").is_none());
            state.completed = true;
            openai_text_response("The command failed with exit status 17; its complete diagnostic output was recovered.")
        }),
    ]);
    workspace.configure_local_openai(&endpoint);
    let output = run_to_completion(
        local_default_agent_command(
            &workspace,
            "stored-output-session",
            "Inspect the command result and report its exit status.",
            true,
            true,
        ),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(output.status.success(), "{}", output.stderr_text());
    assert!(!output.stderr_text().contains(APPROVAL_PROMPT));
    let requests = server.join().unwrap();
    assert!(requests
        .iter()
        .any(|request| model_tool_result_envelopes(&request.body)
            .iter()
            .any(|envelope| envelope["result"]["kind"] == "artifact")));
    let state = observation.lock().unwrap();
    assert!(state.completed);
    assert!(state.artifact.is_none());
    assert_eq!(state.stdout, expected_stdout);
    assert_eq!(state.stderr, "compiler diagnostic\n");
    assert_eq!(fs::read(workspace.path("runs.txt")).unwrap(), b"run\n");
}
