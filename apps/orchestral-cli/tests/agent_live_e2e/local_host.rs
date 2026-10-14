use super::*;

/// Real terminal + authenticated HTTP clients share the same controller,
/// transcript, approval broker, and journal. All model replies are local fixtures.
#[test]
fn local_host_pty_accepts_remote_turns_approvals_and_cancel_without_a_second_writer() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("local-host-pwa");
    fs::write(
        workspace.path("approved.marker"),
        "remove only after approval",
    )
    .unwrap();
    let (endpoint, model_server) = spawn_fixture_http_server(vec![
        Box::new(|_| {
            openai_tool_response(
                "remove-marker",
                "exec_command",
                if cfg!(windows) {
                    json!({"cmd":"[System.IO.File]::Delete([System.IO.Path]::Combine($PWD.Path, 'approved.marker'))", "sandbox_permissions":"require_escalated", "justification":"Delete requested marker", "yield_time_ms":1, "wait_mode":"completion"})
                } else {
                    json!({"cmd":"rm approved.marker", "yield_time_ms":1, "wait_mode":"completion"})
                },
            )
        }),
        Box::new(|request| {
            continue_exec_until_exit(request)
                .unwrap_or_else(|| openai_text_response("TERMINAL_APPROVAL_DONE"))
        }),
        Box::new(|request| {
            let context = model_request_text(&request.body);
            assert!(
                context.contains("TERMINAL_APPROVAL_DONE"),
                "remote turn lost terminal history"
            );
            assert!(context.contains("Message from the PWA"));
            openai_text_response("REMOTE_TURN_DONE")
        }),
        Box::new(|request| {
            assert!(model_request_text(&request.body).contains("REMOTE_TURN_DONE"));
            openai_tool_response(
                "waiting-question",
                "orchestral_request_input",
                json!({"prompt":"REMOTE_CANCEL_TARGET"}),
            )
        }),
    ]);
    workspace.configure_local_openai(&endpoint);
    let mut tui = PtyHarness::spawn_with_size(
        local_tui_command(&workspace, "local-host-session", "Follow the user's task."),
        120,
        40,
    );
    tui.wait_for_screen(
        |screen| screen.contains("Ask Orchestral to do anything"),
        LOCAL_PROCESS_TIMEOUT,
    );
    let registration = wait_registration(&workspace);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let api = |method: reqwest::Method, path: &str, body: Value| -> Value {
        runtime.block_on(async {
            let response = client
                .request(
                    method,
                    format!("http://{}{path}", registration["address"].as_str().unwrap()),
                )
                .bearer_auth(registration["token"].as_str().unwrap())
                .json(&body)
                .send()
                .await
                .unwrap();
            let status = response.status();
            let body: Value = response.json().await.unwrap();
            assert!(status.is_success(), "HTTP {status}: {body}");
            body
        })
    };
    let sessions = api(reqwest::Method::GET, "/sessions", Value::Null);
    assert_eq!(sessions[0]["id"], "local-host-session");
    assert!(
        sessions[0]["run_ids"].as_array().unwrap().is_empty(),
        "idle terminals must be discoverable before their first turn"
    );
    tui.send_paste("Remove the marker after approval");
    tui.wait_for_text("Allow once", LOCAL_PROCESS_TIMEOUT);
    assert!(workspace.path("approved.marker").exists());
    let session = api(
        reqwest::Method::GET,
        "/sessions/local-host-session",
        Value::Null,
    );
    let run = session["run_ids"][0].as_str().unwrap();
    let view = api(reqwest::Method::GET, &format!("/runs/{run}"), Value::Null);
    let pending = &view["pending_requests"][0];
    let pending = if pending.is_null() {
        &view["view"]["pending_requests"][0]
    } else {
        pending
    };
    let request_id = pending["request_id"]
        .as_str()
        .expect("approval request has id");
    api(
        reqwest::Method::POST,
        &format!("/runs/{run}/requests/{request_id}/approval"),
        json!({"command_id":"pwa-approve", "decision":"allow_once"}),
    );
    tui.wait_for_text("TERMINAL_APPROVAL_DONE", LOCAL_PROCESS_TIMEOUT);
    tui.wait_for_text_count("○ replied", 1, LOCAL_PROCESS_TIMEOUT);
    assert!(!workspace.path("approved.marker").exists());
    let turn = json!({"run_id":"pwa-run", "input":"Message from the PWA"});
    api(
        reqwest::Method::POST,
        "/sessions/local-host-session/runs",
        turn.clone(),
    );
    // A retried POST must acknowledge the same Run, without another model call.
    api(
        reqwest::Method::POST,
        "/sessions/local-host-session/runs",
        turn,
    );
    tui.wait_for_text("REMOTE_TURN_DONE", LOCAL_PROCESS_TIMEOUT);
    tui.wait_for_screen(
        |screen| screen.contains("Message from the PWA") && screen.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.wait_for_screen(
        |screen| screen.contains("Ask Orchestral to do anything"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("Ask a question and wait");
    tui.wait_for_text("REMOTE_CANCEL_TARGET", LOCAL_PROCESS_TIMEOUT);
    let session = api(
        reqwest::Method::GET,
        "/sessions/local-host-session",
        Value::Null,
    );
    assert_eq!(session["run_ids"].as_array().unwrap().len(), 3);
    let run = session["run_ids"]
        .as_array()
        .unwrap()
        .last()
        .unwrap()
        .as_str()
        .unwrap();
    api(
        reqwest::Method::POST,
        &format!("/runs/{run}/cancel"),
        json!({"command_id":"pwa-cancel", "reason":"Cancel from PWA"}),
    );
    tui.wait_for_text("cancelled", LOCAL_PROCESS_TIMEOUT);
    tui.send(b"\x04");
    let output = tui.finish(LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success(), "{}", output.text());
    output.assert_terminal_restored();
    assert!(
        !output.text().contains("HTTP request completed"),
        "local HTTP logs must not corrupt the terminal"
    );
    let inactive = wait_registration(&workspace);
    assert_eq!(
        inactive["active"], false,
        "exited terminals must retain an unavailable route"
    );
    let requests = model_server.join().unwrap();
    let polls = assert_successful_exec_observations(&session_records(&workspace));
    assert_eq!(requests.len(), 4 + polls);
    assert_eq!(run_payload_count(&workspace, "run_cancelled"), 1);
}

fn wait_registration(workspace: &TestWorkspace) -> Value {
    let deadline = Instant::now() + LOCAL_PROCESS_TIMEOUT;
    loop {
        if let Ok(entries) = fs::read_dir(workspace.path("user-config/local-hosts")) {
            for entry in entries.flatten() {
                if entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "json")
                {
                    if let Ok(bytes) = fs::read(entry.path()) {
                        if let Ok(record) = serde_json::from_slice::<Value>(&bytes) {
                            return record;
                        }
                    }
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "terminal did not publish its local control endpoint"
        );
        thread::sleep(Duration::from_millis(20));
    }
}
