use super::*;

#[test]
fn tui_welcome_and_editable_queue_follow_real_runtime_consumption() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("tui-input-queue");
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(move |_| {
            started_tx.send(()).unwrap();
            release_rx.recv_timeout(PROCESS_TIMEOUT).unwrap();
            openai_text_response("Original response completed")
        }),
        Box::new(move |request| {
            let text = model_request_text(&request.body);
            assert!(text.contains("Revised queued requirement"), "{text}");
            assert!(!text.contains("Original queued requirement"), "{text}");
            assert!(text.contains("Original response completed"), "{text}");
            let usage = json!({"choices": [], "usage": {"prompt_tokens": 123, "completion_tokens": 2, "total_tokens": 125}});
            let answer = json!({"choices": [{"delta": {"content": "Queued requirement received"}, "finish_reason": "stop"}]});
            sse_response(format!(
                "data: {usage}\n\ndata: {answer}\n\ndata: [DONE]\n\n"
            ))
        }),
    ]);
    workspace.configure_local_openai(&endpoint);
    // Match the emulator and native console before either can emit a frame.
    // Resizing immediately after spawning can decode ConPTY's queued 80-column
    // startup output at 120 columns and leave stale cells in later assertions.
    let mut tui = PtyHarness::spawn_with_size(
        local_tui_command(&workspace, "queue-session", "Follow the user's task."),
        120,
        32,
    );
    tui.wait_for_screen(
        |screen| {
            screen.contains("A runtime for reliable, interactive AI agents.")
                && screen.contains(env!("CARGO_PKG_VERSION"))
                && screen.contains("fixture-model")
        },
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("Inspect this project");
    started_rx.recv_timeout(LOCAL_PROCESS_TIMEOUT).unwrap();
    tui.send_paste("Original queued requirement");
    tui.wait_for_screen(
        |screen| screen.contains("Queued 1") && screen.contains("Original queued requirement"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("/queue");
    tui.wait_for_screen(
        |screen| screen.contains("Pending messages"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\r");
    tui.wait_for_screen(
        |screen| screen.contains("esc discard edit"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x01\x0bRevised queued requirement\r");
    tui.wait_for_screen(
        |screen| {
            screen.contains("Queued 1")
                && screen.contains("Revised queued requirement")
                && !screen.contains("Original queued requirement")
        },
        LOCAL_PROCESS_TIMEOUT,
    );
    release_tx.send(()).unwrap();
    tui.wait_for_screen(
        |screen| {
            screen.contains("replied")
                && screen.contains("Queued requirement received")
                && screen.contains("last input: 123")
                && !screen.contains("Queued 1")
        },
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x04");
    let output = tui.finish(LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success(), "{}", output.text());
    output.assert_terminal_restored();
    assert_eq!(server.join().unwrap().len(), 2);
    assert_eq!(run_payload_count(&workspace, "input_committed"), 1);
    assert_eq!(run_payload_count(&workspace, "delivery_committed"), 1);
}

#[test]
#[ignore = "spends real Google Vertex quota; requires ADC or a service-account credential"]
fn live_tui_repairs_rust_and_continues_after_session_selection() {
    let _guard = live_test_guard();
    let workspace = TestWorkspace::new("live-tui-coding");
    fs::create_dir_all(workspace.path("src")).unwrap();
    fs::write(
        workspace.path("Cargo.toml"),
        "[package]\nname = \"tui_calculator\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    )
    .unwrap();
    fs::write(workspace.path("src/lib.rs"), "pub fn product(a: i64, b: i64) -> i64 { a + b }\n#[cfg(test)] mod tests { #[test] fn multiplies() { assert_eq!(super::product(6, 7), 42); } }\n").unwrap();
    let command = live_default_command(&workspace, "live-tui-session", true, true);
    let mut pty_command = CommandBuilder::new(command.get_program());
    pty_command.args(command.get_args());
    pty_command.cwd(&workspace.root);
    for (name, value) in command.get_envs() {
        if let Some(value) = value {
            pty_command.env(name, value);
        } else {
            pty_command.env_remove(name);
        }
    }
    let mut tui = PtyHarness::spawn(pty_command);
    tui.wait_for_screen(|s| s.contains("Ask Orchestral"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"Fix the bug in this Rust project. Run its tests and explain the change. @src/lib");
    tui.wait_for_screen(
        |s| s.contains("Files ·") && s.contains("src/lib.rs"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\t\r");
    tui.wait_for_screen(|s| s.contains("replied"), LIVE_CODING_PROCESS_TIMEOUT);
    fs::create_dir_all(workspace.path("tests")).unwrap();
    fs::write(workspace.path("tests/independent.rs"), "#[test] fn independent_product() { for (a,b) in [(3,9),(-4,7),(0,19),(1,31)] { assert_eq!(tui_calculator::product(a,b), a*b); } }\n").unwrap();
    let verification = Command::new("cargo")
        .arg("test")
        .current_dir(&workspace.root)
        .output()
        .unwrap();
    assert!(
        verification.status.success(),
        "{}",
        String::from_utf8_lossy(&verification.stderr)
    );
    tui.send(b"\x0f"); // Inspect tool output in its conversation position.
    tui.send(b"\x1b[5~");
    tui.wait_for_screen(|s| s.contains("end to follow"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"\x1b[F\x0f");
    tui.send_paste("/resume");
    tui.wait_for_screen(
        |s| s.contains("Sessions · this workspace"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\r");
    tui.wait_for_screen(|s| s.contains("Resumed ·"), LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("Explain how the corrected function behaves for zero and negative inputs.");
    tui.wait_for_screen(|s| s.contains("replied"), LIVE_CODING_PROCESS_TIMEOUT);
    tui.send(b"\x04");
    let output = tui.finish(LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success());
    output.assert_terminal_restored();
    assert_eq!(run_payload_count(&workspace, "delivery_committed"), 2);
}

#[test]
#[cfg(unix)] // This fixture supplies Unix clipboard executables and a POSIX PATH.
fn tui_pty_long_paste_context_and_clipboard_preserve_original_content() {
    use std::os::unix::fs::PermissionsExt;
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("tui-long-copy");
    let input = (0..200)
        .map(|i| format!("原始文本 {i} 👩🏽‍💻\n"))
        .collect::<String>();
    let expected = input.clone();
    let (endpoint, server) = spawn_fixture_http_server(vec![Box::new(move |request| {
        assert!(model_request_text(&request.body).contains(&expected));
        openai_text_response("COPY_START\n\u{1b}[31m中文 response\u{1b}[0m\nCOPY_END")
    })]);
    workspace.configure_local_openai(&endpoint);
    let bin = workspace.path("clipboard-bin");
    fs::create_dir_all(&bin).unwrap();
    for program in ["pbcopy", "wl-copy", "xclip"] {
        let path = bin.join(program);
        fs::write(&path, "#!/bin/sh\nif [ -f \"$ORCHESTRAL_TEST_CLIPBOARD.fail\" ]; then exit 1; fi\n/bin/cat > \"$ORCHESTRAL_TEST_CLIPBOARD\"\n").unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }
    let clipboard = workspace.path("copied.txt");
    let mut command = local_tui_command(
        &workspace,
        "paste-copy-session",
        "Follow the user's request.",
    );
    command.env(
        "PATH",
        format!(
            "{}:{}",
            bin.display(),
            std::env::var("PATH").unwrap_or_default()
        ),
    );
    command.env("ORCHESTRAL_TEST_CLIPBOARD", &clipboard);
    let mut tui = PtyHarness::spawn(command);
    tui.wait_for_screen(|s| s.contains("Ask Orchestral"), LOCAL_PROCESS_TIMEOUT);
    tui.send(format!("\x1b[200~{input}\x1b[201~").as_bytes());
    tui.wait_for_screen(
        |s| s.contains("200 lines") && s.contains("ctrl+p expand"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x10");
    tui.wait_for_screen(|s| s.contains("ctrl+p collapse"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"\r");
    tui.wait_for_screen(
        |s| s.contains("COPY_END") && s.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("/copy");
    tui.wait_for_screen(|s| s.contains("Answer copied"), LOCAL_PROCESS_TIMEOUT);
    assert_eq!(
        fs::read_to_string(&clipboard).unwrap(),
        "COPY_START\n中文 response\nCOPY_END"
    );
    fs::write(workspace.path("copied.txt.fail"), "fail").unwrap();
    tui.send_paste("/copy");
    tui.wait_for_screen(
        |s| s.contains("Clipboard unavailable"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("/resume");
    tui.wait_for_screen(
        |s| s.contains("Sessions · this workspace"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"Current session details\r");
    tui.wait_for_screen(
        |s| s.contains("Storage:") && s.contains("paste-copy-session"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x1b");
    tui.wait_for_screen(
        |s| s.contains("Sessions · this workspace"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x1b");
    tui.wait_for_screen(
        |s| !s.contains("Sessions · this workspace"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("/context");
    tui.wait_for_screen(|s| s.contains("Latest request:"), LOCAL_PROCESS_TIMEOUT);
    read_panel_until(&mut tui, "Reserved output:");
    tui.send(b"\x1b");
    tui.wait_for_screen(|s| !s.contains("Context ·"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"\x04");
    let output = tui.finish(LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success());
    output.assert_terminal_restored();
    assert_eq!(server.join().unwrap().len(), 1);
}

#[test]
fn tui_pty_completes_paths_and_rejects_deleted_references_without_submitting() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("tui-files");
    fs::write(workspace.path("规则 file.rs"), "fn example() {}\n").unwrap();
    fs::create_dir_all(workspace.path("target")).unwrap();
    fs::write(workspace.path("target/hidden.rs"), "ignored").unwrap();
    let expected_path = workspace.path("规则 file.rs").canonicalize().unwrap();
    let (endpoint, server) = spawn_fixture_http_server(vec![Box::new(move |request| {
        let text = model_request_text(&request.body);
        let reference = serde_json::to_string(&expected_path.canonicalize().unwrap()).unwrap();
        assert!(text.contains(&reference), "{text}");
        assert!(
            !text.contains("fn example"),
            "path selection must not eagerly read contents"
        );
        assert!(!text.contains("/unknown-command"));
        openai_text_response("REFERENCE_ACCEPTED")
    })]);
    workspace.configure_local_openai(&endpoint);
    let mut tui = PtyHarness::spawn(local_tui_command(
        &workspace,
        "files-session",
        "Follow the user's request.",
    ));
    tui.wait_for_screen(|s| s.contains("Ask Orchestral"), LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("/unknown-command");
    tui.wait_for_screen(|s| s.contains("Unknown command"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"\x03");
    tui.send("inspect @规则".as_bytes());
    tui.wait_for_screen(
        |s| s.contains("Files · path reference") && s.contains("规则 file.rs"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\t");
    tui.wait_for_screen(
        |s| s.contains("file.rs\"") && !s.contains("Filter:"),
        LOCAL_PROCESS_TIMEOUT,
    );
    fs::remove_file(workspace.path("规则 file.rs")).unwrap();
    tui.send(b"\r");
    tui.wait_for_screen(
        |s| s.contains("Referenced file is unavailable"),
        LOCAL_PROCESS_TIMEOUT,
    );
    fs::write(workspace.path("规则 file.rs"), "fn example() {}\n").unwrap();
    tui.send(b"\r");
    tui.wait_for_screen(
        |s| s.contains("REFERENCE_ACCEPTED") && s.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x04");
    assert!(tui.finish(LOCAL_PROCESS_TIMEOUT).status.success());
    assert_eq!(server.join().unwrap().len(), 1);
}

#[test]
fn tui_pty_extra_enter_after_answer_cannot_submit_a_restored_draft() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("tui-answer-submission");
    let (release, wait) = mpsc::channel();
    let draft = "UNSENT_FUTURE_GUIDANCE";
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(move |_| {
            wait.recv_timeout(LOCAL_PROCESS_TIMEOUT)
                .expect("draft entered before question");
            openai_tool_response(
                "question",
                "orchestral_request_input",
                json!({"prompt": "CHOOSE_OPTION"}),
            )
        }),
        Box::new(move |request| {
            let text = model_request_text(&request.body);
            assert!(text.contains("selected"), "{text}");
            assert!(
                !text.contains(draft),
                "unsubmitted draft reached the model: {text}"
            );
            openai_text_response("ANSWER_CONFIRMED")
        }),
        Box::new(move |request| {
            assert!(model_request_text(&request.body).contains(draft));
            openai_text_response("EXPLICIT_DRAFT_CONFIRMED")
        }),
    ]);
    workspace.configure_local_openai(&endpoint);
    let mut tui = PtyHarness::spawn(local_tui_command(
        &workspace,
        "answer-submission-session",
        "Follow the user's request.",
    ));
    tui.wait_for_screen(|s| s.contains("Ask Orchestral"), LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("Ask me to choose an option");
    tui.wait_for_screen(|s| s.contains("running"), LOCAL_PROCESS_TIMEOUT);
    tui.send(draft.as_bytes());
    tui.wait_for_screen(|s| s.contains(&format!("› {draft}")), LOCAL_PROCESS_TIMEOUT);
    release.send(()).unwrap();
    tui.wait_for_screen(|s| s.contains("CHOOSE_OPTION"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"selected\r\r");
    tui.wait_for_screen(
        |s| {
            s.contains("ANSWER_CONFIRMED")
                && s.contains("replied")
                && s.contains(&format!("› {draft}"))
        },
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\r");
    tui.wait_for_screen(
        |s| s.contains("Previous draft restored"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x05\r");
    tui.wait_for_screen(
        |s| s.contains("EXPLICIT_DRAFT_CONFIRMED") && s.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x04");
    let output = tui.finish(LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success());
    output.assert_terminal_restored();
    assert_eq!(server.join().unwrap().len(), 3);
}

#[test]
fn tui_pty_refreshes_open_file_completion_after_create_rename_and_delete() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("tui-file-refresh");
    let expected_path = workspace.path("fresh-renamed.rs");
    let (endpoint, server) = spawn_fixture_http_server(vec![Box::new(move |request| {
        let text = model_request_text(&request.body);
        let reference = serde_json::to_string(&expected_path.canonicalize().unwrap()).unwrap();
        assert!(text.contains(&reference), "{text}");
        openai_text_response("FRESH_REFERENCE_ACCEPTED")
    })]);
    workspace.configure_local_openai(&endpoint);
    let mut tui = PtyHarness::spawn(local_tui_command(
        &workspace,
        "file-refresh-session",
        "Follow the user's request.",
    ));
    tui.wait_for_screen(|s| s.contains("Ask Orchestral"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"inspect @fresh");
    tui.wait_for_screen(
        |s| s.contains("Files · path reference") && s.contains("No matches"),
        LOCAL_PROCESS_TIMEOUT,
    );
    fs::write(workspace.path("fresh-original.rs"), "original").unwrap();
    tui.wait_for_screen(|s| s.contains("fresh-original.rs"), LOCAL_PROCESS_TIMEOUT);
    fs::rename(
        workspace.path("fresh-original.rs"),
        workspace.path("fresh-renamed.rs"),
    )
    .unwrap();
    tui.wait_for_screen(
        |s| s.contains("fresh-renamed.rs") && !s.contains("fresh-original.rs"),
        LOCAL_PROCESS_TIMEOUT,
    );
    fs::remove_file(workspace.path("fresh-renamed.rs")).unwrap();
    tui.wait_for_screen(|s| s.contains("No matches"), LOCAL_PROCESS_TIMEOUT);
    fs::write(workspace.path("fresh-renamed.rs"), "restored").unwrap();
    tui.wait_for_screen(|s| s.contains("fresh-renamed.rs"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"\t");
    tui.wait_for_screen(
        |s| s.contains("fresh-renamed.rs\"") && !s.contains("Filter:"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\r");
    tui.wait_for_screen(
        |s| s.contains("FRESH_REFERENCE_ACCEPTED") && s.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x04");
    let output = tui.finish(LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success());
    output.assert_terminal_restored();
    assert_eq!(server.join().unwrap().len(), 1);
}

#[test]
fn tui_pty_switches_models_without_losing_memory_journal_and_resumes_session_draft() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("tui-model-session");
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(|request| {
            assert_eq!(request.body["model"], "fixture-model");
            openai_text_response("FIRST_MODEL_ANSWER")
        }),
        Box::new(|request| {
            assert_eq!(request.body["model"], "alternate-fixture");
            assert!(model_request_text(&request.body).contains("FIRST_MODEL_ANSWER"));
            openai_text_response("SECOND_MODEL_ANSWER")
        }),
    ]);
    workspace.configure_local_openai(&endpoint);
    workspace.rewrite_config(|config| {
        config["journal"]["backend"] = serde_yaml::Value::String("memory".to_owned());
        config["agent"]["model_profile"] = serde_yaml::Value::String("alternate".to_owned());
        config["providers"]["default_model"] = serde_yaml::Value::String("alternate".to_owned());
        config["providers"]["models"] = serde_yaml::to_value(json!([
            {"name":"alternate", "backend":"openai", "model":"alternate-fixture", "max_tokens":8192}
        ]))
        .unwrap();
    });
    let mut tui = PtyHarness::spawn(local_tui_command(
        &workspace,
        "model-session",
        "Follow the user's request.",
    ));
    tui.wait_for_screen(|s| s.contains("Ask Orchestral"), LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("remember the current project");
    tui.wait_for_screen(
        |s| s.contains("FIRST_MODEL_ANSWER") && s.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("/model profiles");
    tui.wait_for_screen(
        |s| s.contains("Models · configured profiles") && s.contains("alternate"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\r");
    tui.wait_for_screen(
        |s| s.contains("Model selected for the next request"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("continue the same project");
    tui.wait_for_screen(
        |s| s.contains("SECOND_MODEL_ANSWER") && s.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"keep this draft");
    tui.send(b"\x1d");
    tui.wait_for_screen(|s| s.contains("Help · Esc"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"new\r");
    tui.wait_for_screen(
        |s| s.contains("Ask Orchestral") && !s.contains("FIRST_MODEL_ANSWER"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("/resume");
    tui.wait_for_screen(
        |s| s.contains("Sessions · this workspace") && s.contains("remember the current project"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\r");
    tui.wait_for_screen(
        |s| s.contains("keep this draft") && s.contains("SECOND_MODEL_ANSWER"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x03");
    tui.wait_for_screen(|s| s.contains("Ask Orchestral"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"\x04");
    let output = tui.finish(LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success());
    output.assert_terminal_restored();
    assert_eq!(server.join().unwrap().len(), 2);
}

#[test]
fn tui_pty_discovers_models_and_preserves_reasoning_when_resuming_a_session() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("tui-api-model-reasoning");
    let models: FixtureHttpHandler = Box::new(|request| {
        assert_eq!(request.method, "GET");
        assert_eq!(request.path, "/v1/models");
        assert_eq!(
            request.headers.get("authorization").map(String::as_str),
            Some("Bearer fixture-key")
        );
        FixtureHttpResponse {
            status: "200 OK",
            content_type: "application/json",
            repeat_handler: false,
            body: serde_json::to_vec(&json!({"data":[
                {"id":"fixture-model", "max_model_len":32768},
                {"id":"api-alternate", "max_model_len":32768, "reasoning":{
                    "thinking":{"default_enabled":true}, "supported_efforts":["future-next", "on"]}}
            ]}))
            .unwrap(),
        }
    });
    let (endpoint, server) = spawn_fixture_http_server_with_models(
        vec![
            Box::new(|request| {
                assert_eq!(request.body["model"], "fixture-model");
                assert!(request.body.get("reasoning_effort").is_none());
                assert!(request.body.get("chat_template_kwargs").is_none());
                openai_text_response("INITIAL_SESSION_MEMORY")
            }),
            Box::new(|request| {
                assert_eq!(request.body["model"], "api-alternate");
                assert_eq!(
                    request.body["chat_template_kwargs"]["enable_thinking"],
                    false
                );
                assert!(request.body.get("reasoning_effort").is_none());
                assert!(model_request_text(&request.body).contains("INITIAL_SESSION_MEMORY"));
                openai_text_response("API_SELECTION_CONFIRMED")
            }),
            Box::new(|request| {
                assert_eq!(request.body["model"], "api-alternate");
                assert_eq!(
                    request.body["chat_template_kwargs"]["enable_thinking"],
                    false
                );
                assert!(model_request_text(&request.body).contains("API_SELECTION_CONFIRMED"));
                openai_text_response("RESUMED_SELECTION_CONFIRMED")
            }),
            Box::new(|request| {
                assert_eq!(request.body["model"], "api-alternate");
                assert_eq!(request.body["reasoning_effort"], "future-next");
                assert!(request.body.get("chat_template_kwargs").is_none());
                assert!(model_request_text(&request.body).contains("RESUMED_SELECTION_CONFIRMED"));
                openai_text_response("FUTURE_EFFORT_CONFIRMED")
            }),
            Box::new(|request| {
                assert_eq!(request.body["model"], "api-alternate");
                assert_eq!(request.body["reasoning_effort"], "on");
                assert!(request.body.get("chat_template_kwargs").is_none());
                assert!(model_request_text(&request.body).contains("FUTURE_EFFORT_CONFIRMED"));
                openai_text_response("LITERAL_EFFORT_CONFIRMED")
            }),
        ],
        Some(models),
    );
    workspace.configure_local_openai(&endpoint);
    workspace.rewrite_config(|config| {
        config["journal"]["backend"] = serde_yaml::Value::String("memory".into());
    });
    let mut tui = PtyHarness::spawn(local_tui_command(
        &workspace,
        "api-selection-session",
        "Follow the user's request.",
    ));
    tui.wait_for_screen(|s| s.contains("Ask Orchestral"), LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("remember the connection selection");
    tui.wait_for_screen(
        |s| s.contains("INITIAL_SESSION_MEMORY") && s.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("/model");
    tui.wait_for_screen(
        |s| s.contains("refreshed from service") && s.contains("api-alternate"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"api-alternate\r");
    tui.wait_for_screen(
        |s| s.contains("Model selected for the next request"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("/reasoning");
    tui.wait_for_screen(
        |s| s.contains("Reasoning") && s.contains("Use service default (thinking on)"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"off\r");
    tui.wait_for_screen(
        |s| s.contains("Reasoning off selected"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("continue with the selected model");
    tui.wait_for_screen(
        |s| s.contains("API_SELECTION_CONFIRMED") && s.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("/new");
    tui.wait_for_screen(
        |s| s.contains("Ask Orchestral") && !s.contains("API_SELECTION_CONFIRMED"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("/resume");
    tui.wait_for_screen(
        |s| {
            s.contains("Sessions · this workspace")
                && s.contains("remember the connection selection")
        },
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\r");
    tui.wait_for_screen(
        |s| s.contains("API_SELECTION_CONFIRMED") && s.contains("Resumed ·"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("continue after resuming");
    tui.wait_for_screen(
        |s| s.contains("RESUMED_SELECTION_CONFIRMED") && s.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("/reasoning");
    tui.wait_for_screen(
        |s| s.contains("Reasoning") && s.contains("future-next"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"future-next\r");
    tui.wait_for_screen(
        |s| s.contains("Reasoning future-next selected"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("continue with the declared future effort");
    tui.wait_for_screen(
        |s| s.contains("FUTURE_EFFORT_CONFIRMED") && s.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("/reasoning effort:on");
    tui.wait_for_screen(
        |s| s.contains("Reasoning effort:on selected"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("continue with the literal provider effort");
    tui.wait_for_screen(
        |s| s.contains("LITERAL_EFFORT_CONFIRMED") && s.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x04");
    let output = tui.finish(LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success());
    output.assert_terminal_restored();
    assert_eq!(server.join().unwrap().len(), 5);
}

#[cfg(windows)]
#[test]
fn native_console_accepts_unicode_and_restores_terminal_on_exit() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("native-unicode-console");
    let (endpoint, server) = spawn_fixture_http_server(vec![Box::new(|request| {
        assert!(model_request_text(&request.body).contains("检查 workspace 中文"));
        openai_text_response("NATIVE_UNICODE_ACCEPTED")
    })]);
    workspace.configure_local_openai(&endpoint);
    let mut tui = PtyHarness::spawn(local_tui_command(
        &workspace,
        "native-unicode",
        "Follow the user's request.",
    ));
    tui.wait_for_screen(
        |screen| screen.contains("Ask Orchestral"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send("检查 workspace 中文\r".as_bytes());
    tui.wait_for_screen(
        |screen| screen.contains("NATIVE_UNICODE_ACCEPTED") && screen.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x04");
    let output = tui.finish(LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success(), "{}", output.text());
    output.assert_terminal_restored();
    assert_eq!(server.join().unwrap().len(), 1);
}

#[test]
#[cfg(unix)] // Exercises POSIX LF/Ctrl-J and VT bracketed-paste input bytes.
fn tui_pty_preserves_graphemes_draft_and_focus_through_help_and_history() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("tui-editor");
    let (endpoint, server) = spawn_fixture_http_server(vec![Box::new(|request| {
        let text = model_request_text(&request.body);
        assert!(text.contains("中文e\u{301}\nsecond line"), "{text}");
        assert!(!text.contains("👩🏽‍💻"), "deleted emoji must not be submitted");
        openai_text_response("EDITOR_ACCEPTED")
    })]);
    workspace.configure_local_openai(&endpoint);
    let mut tui = PtyHarness::spawn(local_tui_command(
        &workspace,
        "editor-session",
        "Follow the user's request.",
    ));
    tui.wait_for_screen(|s| s.contains("Ask Orchestral"), LOCAL_PROCESS_TIMEOUT);
    tui.send("\x1b[200~中文e\u{301}👩🏽‍💻\x1b[201~".as_bytes());
    tui.send(b"\x7f\x0asecond line");
    tui.send(b"\x1d");
    tui.wait_for_screen(|s| s.contains("Help · Esc"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"\x1b");
    tui.wait_for_screen(
        |s| !s.contains("Help · Esc") && s.contains("second line"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\r");
    tui.wait_for_screen(
        |s| s.contains("EDITOR_ACCEPTED") && s.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"draft preserved");
    tui.send(b"\x1b[A");
    tui.wait_for_screen(
        |s| s.contains("second line") && !s.contains("draft preserved"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x1b[B");
    tui.wait_for_screen(|s| s.contains("draft preserved"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"\x1b"); // Escape cannot discard the draft or quit an idle session.
    tui.send(b"\x1d");
    tui.wait_for_screen(|s| s.contains("Help · Esc"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"\x1b");
    tui.wait_for_screen(|s| !s.contains("Help · Esc"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"\x03");
    tui.wait_for_screen(
        |s| s.contains("Ask Orchestral") && !s.contains("draft preserved"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x04");
    let output = tui.finish(LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success());
    output.assert_terminal_restored();
    assert_eq!(server.join().unwrap().len(), 1);
}

#[test]
fn tui_pty_keeps_reading_anchor_when_later_output_commits() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("tui-reading");
    let (release, wait) = mpsc::channel();
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(|_| {
            openai_text_response(
                &(0..80)
                    .map(|i| format!("history row {i:03}\n"))
                    .collect::<String>(),
            )
        }),
        Box::new(move |_| {
            wait.recv_timeout(LOCAL_PROCESS_TIMEOUT)
                .expect("release later answer");
            openai_text_response(
                &(0..80)
                    .map(|i| format!("new answer row {i:03}\n"))
                    .collect::<String>(),
            )
        }),
    ]);
    workspace.configure_local_openai(&endpoint);
    let mut tui = PtyHarness::spawn(local_tui_command(
        &workspace,
        "reading-session",
        "Follow the user's request.",
    ));
    tui.wait_for_screen(|s| s.contains("Ask Orchestral"), LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("first request");
    tui.wait_for_screen(
        |s| s.contains("history row 079") && s.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    let native_scrollback = full_scrollback_contents(tui.screen.screen_mut());
    for i in 0..80 {
        let expected = format!("history row {i:03}");
        assert!(
            native_scrollback.contains(&expected),
            "missing {expected} across native scrollback + screen:\n{native_scrollback}"
        );
    }
    tui.send_paste("second request");
    let mut before = tui.wait_for_screen(|s| s.contains("running"), LOCAL_PROCESS_TIMEOUT);
    for _ in 0..2 {
        let previous_top = before.lines().next().unwrap().trim().to_owned();
        tui.send(b"\x1b[5~");
        before = tui.wait_for_screen(
            |s| {
                s.contains("history · end to follow")
                    && s.lines()
                        .next()
                        .is_some_and(|line| line.trim() != previous_top)
            },
            LOCAL_PROCESS_TIMEOUT,
        );
    }
    let anchor = before.lines().next().unwrap().trim().to_owned();
    assert!(anchor.contains("history row"), "{before}");
    release.send(()).unwrap();
    let after = tui.wait_for_screen(
        |s| s.contains("replied") && s.contains("new output"),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert_eq!(after.lines().next().unwrap().trim(), anchor);
    tui.resize(100, 30);
    let resized = tui.wait_for_screen(
        |s| s.lines().next().is_some_and(|line| line.trim() == anchor),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(!resized.contains("new answer row 079"));
    tui.send(b"\x1b[F");
    tui.wait_for_screen(
        |s| s.contains("new answer row 079") && !s.contains("end to follow"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x04");
    let output = tui.finish(LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success());
    output.assert_terminal_restored();
    assert_eq!(server.join().unwrap().len(), 2);
}

#[test]
fn tui_pty_pastes_images_from_paths_and_clipboard_as_multimodal_content() {
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use base64::Engine as _;

    const ONE_BY_ONE_PNG: &[u8] = &[
        0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, b'I', b'H', b'D',
        b'R', 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1f,
        0x15, 0xc4, 0x89, 0x00, 0x00, 0x00, 0x0a, b'I', b'D', b'A', b'T', 0x78, 0x9c, 0x63, 0x00,
        0x01, 0x00, 0x00, 0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00, 0x00, 0x00, 0x00, b'I',
        b'E', b'N', b'D', 0xae, 0x42, 0x60, 0x82,
    ];

    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("tui-image-paste");
    let diagram_path = workspace.path("Screen Shot 1.png");
    let clipboard_path = workspace.path("clipboard-shot.png");
    fs::write(&diagram_path, ONE_BY_ONE_PNG).unwrap();
    fs::write(&clipboard_path, ONE_BY_ONE_PNG).unwrap();
    let expected_data_url = format!(
        "data:image/png;base64,{}",
        BASE64_STANDARD.encode(ONE_BY_ONE_PNG)
    );

    let (endpoint, server) = spawn_fixture_http_server(vec![Box::new(move |request| {
        let user_msg = request.body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["role"] == "user")
            .expect("user message");
        let parts = user_msg["content"]
            .as_array()
            .expect("multimodal user content array");
        assert_eq!(parts.len(), 2, "only [Image #1] should be sent: {parts:?}");
        assert_eq!(parts[0]["type"], "text");
        assert_eq!(parts[0]["text"], "[Image #1] Describe this diagram");
        assert_eq!(parts[1]["type"], "image_url");
        assert_eq!(parts[1]["image_url"]["url"], expected_data_url);
        openai_text_response("IMAGE_INSPECTED_OK")
    })]);
    workspace.configure_local_openai(&endpoint);

    let mut command = local_tui_command(
        &workspace,
        "image-paste-session",
        "Follow the user's request.",
    );
    command.env("ORCHESTRAL_TUI_CLIPBOARD_IMAGE", &clipboard_path);
    let mut tui = PtyHarness::spawn(command);
    tui.wait_for_screen(|s| s.contains("Ask Orchestral"), LOCAL_PROCESS_TIMEOUT);

    // 1. Bracketed paste of a local image file path attaches [Image #1].
    let pasted_path = diagram_path.to_string_lossy().into_owned();
    tui.send(format!("\x1b[200~{pasted_path}\x1b[201~").as_bytes());
    tui.wait_for_screen(
        |s| s.contains("› [Image #1]") && s.contains("Attached [Image #1]"),
        LOCAL_PROCESS_TIMEOUT,
    );

    // 2. Ctrl+V pastes from the system clipboard as [Image #2].
    tui.send(b"\x16");
    tui.wait_for_screen(
        |s| s.contains("› [Image #1] [Image #2]") && s.contains("Attached [Image #2]"),
        LOCAL_PROCESS_TIMEOUT,
    );

    // 3. Backspace deletes [Image #2] atomically, then one more backspace deletes the space.
    tui.send(b"\x7f\x7f");
    tui.wait_for_screen(
        |s| s.contains("› [Image #1]") && !s.contains("[Image #2]"),
        LOCAL_PROCESS_TIMEOUT,
    );

    // 4. Add text and submit.
    tui.send(b" Describe this diagram\r");
    tui.wait_for_screen(
        |s| s.contains("IMAGE_INSPECTED_OK") && s.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );

    // 5. Verify /resume shows the clean placeholder and never leaks raw base64.
    tui.send_paste("/resume");
    let resume_screen = tui.wait_for_screen(
        |s| {
            s.contains("Sessions · this workspace")
                && s.contains("[Image #1] Describe this diagram")
        },
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(!resume_screen.contains("iVBORw0KGgo"));
    tui.send(b"\r");
    let resumed_transcript = tui.wait_for_screen(
        |s| s.contains("Resumed ·") && s.contains("IMAGE_INSPECTED_OK"),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(resumed_transcript.contains("[Image #1] Describe this diagram"));
    assert!(!resumed_transcript.contains("iVBORw0KGgo"));

    tui.send(b"\x04");
    let output = tui.finish(LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success());
    output.assert_terminal_restored();
    assert_eq!(server.join().unwrap().len(), 1);
}

// Inspect long read-only panels through the same paging keys used by a person.
fn read_panel_until(tui: &mut PtyHarness, marker: &str) -> String {
    for _ in 0..100 {
        let before = physical_screen_contents(tui.screen.screen());
        if before.contains(marker) {
            return before;
        }
        tui.send(b"\x1b[6~");
        tui.wait_for_screen(|screen| screen != before, LOCAL_PROCESS_TIMEOUT);
    }
    panic!("Panel did not contain {marker}");
}

fn skill_tui_command(workspace: &TestWorkspace, session: &str) -> CommandBuilder {
    let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_orchestral"));
    command.cwd(&workspace.root);
    command.env("OPENAI_API_KEY", "fixture-key");
    command.env("ORCHESTRAL_HOME", workspace.path("user-config"));
    command.args([
        "--config",
        workspace.path("orchestral.yaml").to_str().unwrap(),
        "--backend",
        "openai",
        "--model",
        "fixture-model",
        "--temperature",
        "0",
        "--session-id",
        session,
        "--no-mcp",
    ]);
    command
}

fn write_tui_skill(workspace: &TestWorkspace, name: &str, description: &str, instructions: &str) {
    let directory = workspace.path(&format!("skills/{name}"));
    fs::create_dir_all(&directory).unwrap();
    fs::write(
        directory.join("SKILL.md"),
        format!("---\nname: {name}\ndescription: {description}\n---\n{instructions}\n"),
    )
    .unwrap();
}

#[test]
fn tui_pty_skill_catalog_keeps_details_and_preferences_out_of_the_conversation() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("tui-skill-catalog");
    write_tui_skill(
        &workspace,
        "report-builder",
        &format!(
            "Prepare reports. {} DESCRIPTION_END",
            (0..80)
                .map(|index| format!("Description detail {index:03}. "))
                .collect::<String>()
        ),
        "PRIVATE_SKILL_INSTRUCTIONS",
    );
    write_tui_skill(
        &workspace,
        "rust-review",
        "Review Rust changes.",
        "ANOTHER_PRIVATE_SKILL",
    );
    let (endpoint, server) = spawn_fixture_http_server(vec![Box::new(|request| {
        let text = model_request_text(&request.body);
        assert!(!text.contains("PRIVATE_SKILL_INSTRUCTIONS"));
        assert!(!text.contains("ANOTHER_PRIVATE_SKILL"));
        for message in request.body["messages"].as_array().unwrap() {
            if message["role"] == "user" {
                assert!(!message.to_string().contains("/skills"));
            }
        }
        openai_text_response("CATALOG_BROWSING_COMPLETE")
    })]);
    workspace.configure_local_openai(&endpoint);
    // This test inspects the dark palette; do not inherit a CI user's opt-out.
    let mut command = skill_tui_command(&workspace, "skill-catalog-session");
    command.env_remove("NO_COLOR");
    let mut tui = PtyHarness::spawn(command);
    tui.wait_for_screen(|s| s.contains("Ask Orchestral"), LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("/skills");
    let list = tui.wait_for_screen(
        |s| s.contains("Skills · this workspace") && s.contains("report-builder"),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(!list.contains("DESCRIPTION_END"));
    assert!(!list.contains("SKILL.md"));
    // The welcome view above the menu intentionally shows the workspace.
    // Skill source paths belong in details, not in the catalog itself.
    let catalog = list.split_once("Skills · this workspace").unwrap().1;
    assert!(!catalog.contains(workspace.root.to_str().unwrap()));
    let description_row = list
        .lines()
        .position(|line| line.contains("Prepare reports."))
        .unwrap();
    assert_eq!(
        tui.screen
            .screen()
            .cell(description_row as u16, 4)
            .unwrap()
            .fgcolor(),
        vt100::Color::Rgb(163, 168, 181)
    );
    tui.send(b"report");
    tui.wait_for_screen(
        |s| s.contains("Filter: report") && !s.contains("rust-review"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.resize(40, 18);
    tui.wait_for_screen(
        |s| {
            s.contains("report-builder")
                && s.contains('…')
                && s.lines()
                    .rev()
                    .nth(1)
                    .is_some_and(|line| line.contains("ready"))
        },
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.resize(80, 24);
    tui.wait_for_screen(
        |s| {
            s.contains("Skills · this workspace")
                && s.lines()
                    .last()
                    .is_some_and(|line| line.contains("fixture-model"))
        },
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\r");
    tui.wait_for_screen(
        |s| s.contains("Skill · report-builder") && s.contains("This process: enabled"),
        LOCAL_PROCESS_TIMEOUT,
    );
    read_panel_until(&mut tui, "DESCRIPTION_END");
    tui.send(b" ");
    tui.wait_for_screen(
        |s| s.contains("After restart: disabled") && s.contains("This process: enabled"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x1b");
    tui.wait_for_screen(
        |s| s.contains("Filter: report") && s.contains("restart pending"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\r");
    tui.wait_for_screen(
        |s| s.contains("Skill · report-builder") && s.contains("After restart: disabled"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b" ");
    tui.wait_for_screen(
        |s| {
            s.contains("This process: enabled")
                && s.contains("Space: disable for future launches")
                && !s.contains("After restart: disabled")
        },
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x1b");
    tui.wait_for_screen(|s| s.contains("Filter: report"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"\x1b");
    let closed = tui.wait_for_screen(
        // PTY reads can split the redraw after the filter has cleared but
        // before the remaining catalog rows are erased.
        |s| {
            s.contains("Ask Orchestral")
                && !s.contains("Filter:")
                && !s.contains("Prepare reports.")
                && !s.contains("report-builder")
        },
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(!closed.contains("Prepare reports."));
    assert!(!closed.contains("report-builder"));
    tui.send_paste("Say that catalog browsing is complete.");
    tui.wait_for_screen(
        |s| s.contains("CATALOG_BROWSING_COMPLETE") && s.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x04");
    let output = tui.finish(LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success());
    output.assert_terminal_restored();
    assert_eq!(server.join().unwrap().len(), 1);
    assert_eq!(run_payload_count(&workspace, "delivery_committed"), 1);
}

#[test]
fn tui_pty_context_reports_skill_loads_for_one_request_after_switching_and_resuming() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("tui-skill-scope");
    write_tui_skill(
        &workspace,
        "report-builder",
        "Prepare reports.",
        "REPORT_SKILL_BODY",
    );
    let (release, wait) = mpsc::channel();
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(|_| {
            openai_tool_response(
                "read-report",
                "skill_read",
                json!({"name":"report-builder"}),
            )
        }),
        Box::new(|request| {
            assert!(model_request_text(&request.body).contains("REPORT_SKILL_BODY"));
            openai_text_response("REPORT_TASK_COMPLETE")
        }),
        Box::new(move |request| {
            // Historical tool exchanges may remain, but full skill instructions
            // from the previous request must not become instructions for this one.
            let messages = request.body["messages"].as_array().unwrap();
            assert!(!messages
                .iter()
                .any(|m| m["role"] == "system" && m.to_string().contains("REPORT_SKILL_BODY")));
            wait.recv_timeout(LOCAL_PROCESS_TIMEOUT).unwrap();
            openai_text_response("UNRELATED_TASK_COMPLETE")
        }),
    ]);
    workspace.configure_local_openai(&endpoint);
    let mut tui = PtyHarness::spawn(skill_tui_command(&workspace, "skill-scope-session"));
    tui.wait_for_screen(|s| s.contains("Ask Orchestral"), LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("Prepare the project report.");
    tui.wait_for_screen(
        |s| s.contains("REPORT_TASK_COMPLETE") && s.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("/context");
    let first = tui.wait_for_screen(
        // PTY reads can end after the panel heading but partway through a
        // skill name. Wait for the panel's content, not the earlier transcript.
        |s| {
            s.split_once("Context ·").is_some_and(|(_, context)| {
                context.contains("Latest request:")
                    && context.contains("Skills loaded for this request:")
                    && context.contains("report-builder")
            })
        },
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(
        first
            .split("Context ·")
            .nth(1)
            .unwrap()
            .contains("report-builder"),
        "{first}"
    );
    tui.send(b"\x1b");
    tui.wait_for_screen(|s| !s.contains("Context ·"), LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("Now answer an unrelated question.");
    tui.wait_for_screen(|s| s.contains("running"), LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("/context");
    let second = tui.wait_for_screen(
        |s| s.contains("Current request:") && s.contains("None recorded for this request."),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(
        !second
            .split("Context ·")
            .nth(1)
            .unwrap()
            .contains("report-builder"),
        "{second}"
    );
    tui.send(b"\x1b");
    tui.wait_for_screen(
        |s| !s.contains("Context ·") && s.contains("running"),
        LOCAL_PROCESS_TIMEOUT,
    );
    release.send(()).unwrap();
    tui.wait_for_screen(
        |s| s.contains("UNRELATED_TASK_COMPLETE") && s.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("/new");
    tui.wait_for_screen(
        |s| s.contains("Ask Orchestral") && !s.contains("REPORT_TASK_COMPLETE"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("/context");
    tui.wait_for_screen(
        |s| s.contains("No requests recorded.") && s.contains("None recorded for this request."),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x1b");
    tui.wait_for_screen(|s| !s.contains("Context ·"), LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("/resume");
    tui.wait_for_screen(
        |s| s.contains("Sessions · this workspace"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"Prepare the project report\r");
    tui.wait_for_screen(
        |s| s.contains("Resumed ·") && s.contains("UNRELATED_TASK_COMPLETE"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("/context");
    tui.wait_for_screen(
        |s| s.contains("Latest request:") && s.contains("None recorded for this request."),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x1b");
    tui.wait_for_screen(|s| !s.contains("Context ·"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"\x04");
    let output = tui.finish(LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success());
    output.assert_terminal_restored();
    assert_eq!(server.join().unwrap().len(), 3);
    assert_eq!(run_payload_count(&workspace, "delivery_committed"), 2);
}

#[test]
fn tui_pty_help_actions_preserve_drafts_and_do_not_interrupt_a_running_request() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("tui-help-actions");
    let (release, wait) = mpsc::channel();
    let (endpoint, server) = spawn_fixture_http_server(vec![Box::new(move |_| {
        wait.recv_timeout(LOCAL_PROCESS_TIMEOUT).unwrap();
        openai_text_response("TASK_NOT_INTERRUPTED")
    })]);
    workspace.configure_local_openai(&endpoint);
    let mut tui = PtyHarness::spawn(local_tui_command(
        &workspace,
        "help-actions-session",
        "Follow the user's request.",
    ));
    tui.wait_for_screen(|s| s.contains("Ask Orchestral"), LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("Complete the current task.");
    tui.wait_for_screen(|s| s.contains("running"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"guidance draft\x1d");
    tui.wait_for_screen(|s| s.contains("Help · Esc"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"keyboard\r");
    tui.wait_for_screen(
        |s| s.contains("Keyboard shortcuts ·"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x1b");
    tui.wait_for_screen(|s| s.contains("Filter: keyboard"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"\x1b");
    tui.wait_for_screen(
        |s| s.contains("guidance draft") && !s.contains("Help · Esc") && s.contains("running"),
        LOCAL_PROCESS_TIMEOUT,
    );
    // The slash entry and F1 expose the same actions. Clear the draft with
    // editor keys; Ctrl+C would correctly interrupt the active task here.
    tui.send(b"\x01\x0b/");
    tui.wait_for_screen(|s| s.contains("Help · Esc"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"\x1b");
    tui.send(b"\x7f\x1d");
    tui.wait_for_screen(|s| s.contains("Help · Esc"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"appearance\r");
    tui.wait_for_screen(|s| s.contains("Appearance · choose"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"light\r");
    tui.wait_for_screen(|s| s.contains("Filter: appearance"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"\x1b");
    tui.wait_for_screen(
        |s| !s.contains("Help · Esc") && s.contains("running"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("/tools");
    tui.wait_for_screen(
        |s| s.contains("Unknown command: /tools"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x01\x0b");
    release.send(()).unwrap();
    tui.wait_for_screen(
        |s| s.contains("TASK_NOT_INTERRUPTED") && s.contains("replied"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x04");
    let output = tui.finish(LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success());
    output.assert_terminal_restored();
    assert_eq!(server.join().unwrap().len(), 1);
    assert_eq!(run_payload_count(&workspace, "run_cancelled"), 0);
}
