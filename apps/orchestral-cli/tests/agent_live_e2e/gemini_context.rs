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
