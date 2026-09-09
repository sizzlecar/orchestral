use super::*;

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
        assert!(text.contains(expected_path.to_str().unwrap()), "{text}");
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
        assert!(text.contains(expected_path.to_str().unwrap()), "{text}");
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
    tui.send_paste("/model");
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
    tui.send_paste("second request");
    let mut before = tui.wait_for_screen(|s| s.contains("running"), LOCAL_PROCESS_TIMEOUT);
    for _ in 0..2 {
        let previous_top = before.lines().nth(1).unwrap().trim().to_owned();
        tui.send(b"\x1b[5~");
        before = tui.wait_for_screen(
            |s| {
                s.contains("history · end to follow")
                    && s.lines()
                        .nth(1)
                        .is_some_and(|line| line.trim() != previous_top)
            },
            LOCAL_PROCESS_TIMEOUT,
        );
    }
    let anchor = before.lines().nth(1).unwrap().trim().to_owned();
    assert!(anchor.contains("history row"), "{before}");
    release.send(()).unwrap();
    let after = tui.wait_for_screen(
        |s| s.contains("replied") && s.contains("new output"),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert_eq!(after.lines().nth(1).unwrap().trim(), anchor);
    tui.resize(100, 30);
    let resized = tui.wait_for_screen(
        |s| s.lines().nth(1).is_some_and(|line| line.trim() == anchor),
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

// Inspect long read-only panels through the same paging keys used by a person.
fn read_panel_until(tui: &mut PtyHarness, marker: &str) -> String {
    for _ in 0..100 {
        let before = tui.screen.screen().contents();
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
            "Long description. ".repeat(80)
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
    let mut tui = PtyHarness::spawn(skill_tui_command(&workspace, "skill-catalog-session"));
    tui.wait_for_screen(|s| s.contains("Ask Orchestral"), LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("/skills");
    let list = tui.wait_for_screen(
        |s| s.contains("Skills · this workspace") && s.contains("report-builder"),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(!list.contains("DESCRIPTION_END"));
    assert!(!list.contains("SKILL.md"));
    assert!(!list.contains(workspace.root.to_str().unwrap()));
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
        vt100::Color::Default
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
                && s.lines().next().is_some_and(|line| line.contains("ready"))
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
        |s| s.contains("Skill · report-builder"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b" ");
    tui.wait_for_screen(
        |s| s.contains("This process: enabled") && !s.contains("restart pending"),
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send(b"\x1b");
    tui.wait_for_screen(|s| s.contains("Filter: report"), LOCAL_PROCESS_TIMEOUT);
    tui.send(b"\x1b");
    let closed = tui.wait_for_screen(
        |s| s.contains("Ask Orchestral") && !s.contains("Filter:"),
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
        |s| s.contains("Latest request:") && s.contains("Skills loaded for this request:"),
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
