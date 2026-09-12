use super::*;

struct LegacyModel;

#[async_trait::async_trait]
impl orchestral_core::model_protocol::ModelBackend for LegacyModel {
    fn descriptor(&self) -> orchestral_core::model_protocol::ModelDescriptor {
        orchestral_core::model_protocol::ModelDescriptor {
            backend_id: "legacy-fixture".to_owned(),
            capabilities: Default::default(),
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: orchestral_core::model_protocol::ModelRequest,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<
        orchestral_core::model_protocol::ModelStream,
        orchestral_core::model_protocol::ModelError,
    > {
        use futures_util::StreamExt;
        use orchestral_core::model_protocol::{
            ModelEvent, ModelEventId, ModelFinishReason, ModelStreamEvent,
        };
        Ok(futures_util::stream::iter(
            [
                ModelEvent::TextDelta {
                    delta: "LEGACY_ASSISTANT_HISTORY".to_owned(),
                },
                ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            ]
            .into_iter()
            .enumerate()
            .map(move |(index, payload)| {
                Ok(ModelStreamEvent {
                    request_id: request.request_id.clone(),
                    event_id: ModelEventId::new(format!("legacy-event-{index}")),
                    sequence: index as u64 + 1,
                    payload,
                })
            }),
        )
        .boxed())
    }
}

fn seed_legacy_session(workspace: &TestWorkspace) {
    use orchestral_core::agent_protocol::wire::{AgentSessionId, ProviderBindingRef};
    use orchestral_runtime::{
        AgentClient, AgentController, GenericAgentConfig, InternalGenericAgentProvider,
        JsonSizeTokenMeter,
    };
    use std::sync::Arc;
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let store = Arc::new(
            orchestral_agent_journal_fs::FileAgentJournalStore::open_single_writer(
                workspace.path(".orchestral/agent-journal"),
            )
            .unwrap(),
        );
        let provider = InternalGenericAgentProvider::new_with_session_journal(
            Arc::new(LegacyModel),
            GenericAgentConfig::new("orchestral/internal", "generic-agent"),
            store.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .unwrap()
        .with_checkpoint_store(store.clone())
        .unwrap();
        let controller = Arc::new(
            AgentController::with_journal_store(
                Arc::new(provider),
                ProviderBindingRef::new("orchestral/generic-agent"),
                store,
            )
            .unwrap(),
        );
        let client = AgentClient::new(controller, AgentSessionId::new("legacy-session"));
        let handle = client
            .start_text("Legacy project conversation")
            .await
            .unwrap();
        handle.wait_until_blocked().await.unwrap();
    });
}

#[test]
fn legacy_sessions_without_metadata_remain_readable_and_explicitly_resumable() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("session-legacy");
    seed_legacy_session(&workspace);
    assert!(browse(&workspace, &["list"])["sessions"]
        .as_array()
        .unwrap()
        .is_empty());
    let page = browse(&workspace, &["list", "--all"]);
    assert_eq!(page["sessions"][0]["session_id"], "legacy-session");
    assert!(page["sessions"][0]["origin"].is_null());
    assert!(browse(&workspace, &["show", "legacy-session"])
        .to_string()
        .contains("LEGACY_ASSISTANT_HISTORY"));
    let absent = run_to_completion(
        resume_command(&workspace, "--last", "Continue"),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(!absent.status.success());
    assert!(absent
        .stderr_text()
        .contains("no built-in Agent sessions in this workspace"));
    let (endpoint, server) = spawn_fixture_http_server(vec![Box::new(|request| {
        let text = model_request_text(&request.body);
        assert!(text.contains("Legacy project conversation"));
        assert!(text.contains("LEGACY_ASSISTANT_HISTORY"));
        openai_text_response("LEGACY_SESSION_CONTINUED")
    })]);
    workspace.configure_local_openai(&endpoint);
    let mut tui = PtyHarness::spawn(resume_tui(&workspace, "legacy-session", None));
    tui.wait_for_text("LEGACY_ASSISTANT_HISTORY", LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("Continue the legacy conversation");
    tui.wait_for_text("LEGACY_SESSION_CONTINUED", LOCAL_PROCESS_TIMEOUT);
    tui.wait_for_text("○ replied", LOCAL_PROCESS_TIMEOUT);
    exit_tui(tui);
    assert_eq!(server.join().unwrap().len(), 1);
    assert_eq!(
        browse(&workspace, &["list"])["sessions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

fn resume_command(workspace: &TestWorkspace, selector: &str, prompt: &str) -> Command {
    let mut command = base_command(workspace);
    command.env("OPENAI_API_KEY", "fixture-key").args([
        "--backend",
        "openai",
        "--model",
        "fixture-model",
        "--temperature",
        "0",
        "resume",
        selector,
        prompt,
    ]);
    command
}

fn browse(workspace: &TestWorkspace, args: &[&str]) -> Value {
    let mut command = root_command(workspace);
    command
        .env_remove("OPENAI_API_KEY")
        .args([
            "--credential-file",
            "/nonexistent/credential.json",
            "sessions",
        ])
        .args(args)
        .arg("--json");
    let output = run_to_completion(command, LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success(), "{}", output.stderr_text());
    serde_json::from_slice(&output.stdout).unwrap()
}

fn resume_tui(workspace: &TestWorkspace, session: &str, system: Option<&str>) -> CommandBuilder {
    let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_orchestral"));
    command.cwd(&workspace.root);
    command.env("OPENAI_API_KEY", "fixture-key");
    command.args([
        "--config",
        workspace.path("orchestral.yaml").to_str().unwrap(),
        "--backend",
        "openai",
        "--model",
        "fixture-model",
        "--temperature",
        "0",
        "--no-mcp",
        "--no-skills",
    ]);
    if let Some(system) = system {
        command.args(["--system-prompt", system]);
    }
    command.args(["resume", session]);
    command
}

fn exit_tui(mut tui: PtyHarness) {
    tui.send(&[0x04]);
    tui.wait_for_terminal_restore(Duration::from_secs(5));
    let output = tui.finish(Duration::from_secs(5));
    assert!(output.status.success(), "{}", output.text());
    output.assert_terminal_restored();
}

#[test]
fn builtin_sessions_browse_without_credentials_or_a_provider_and_do_not_mutate_journals() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("session-browse");
    let (endpoint, server) = spawn_fixture_http_server(vec![Box::new(|_| {
        openai_text_response("PARSER_HISTORY_ANSWER")
    })]);
    workspace.configure_local_openai(&endpoint);
    let output = run_to_completion(
        local_default_agent_command(
            &workspace,
            "parser-session",
            "Investigate the parser regression",
            true,
            true,
        ),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(output.status.success(), "{}", output.stderr_text());
    server.join().unwrap(); // No server remains for browsing to contact.
    let before = journal_files(&workspace, "")
        .iter()
        .map(|path| (path.clone(), fs::read(path).unwrap()))
        .collect::<Vec<_>>();
    let page = browse(&workspace, &["list", "--search", "PARSER"]);
    assert_eq!(page["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(page["sessions"][0]["session_id"], "parser-session");
    assert_eq!(page["sessions"][0]["status"], "delivered");
    assert_eq!(
        page["sessions"][0]["origin"]["workspace"],
        fs::canonicalize(&workspace.root).unwrap().to_str().unwrap()
    );
    assert!(
        browse(&workspace, &["list", "--search", "unrelated"])["sessions"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let history = browse(&workspace, &["show", "parser-session"]);
    assert_eq!(history["records"].as_array().unwrap().len(), 2);
    assert!(history.to_string().contains("PARSER_HISTORY_ANSWER"));
    for (path, bytes) in before {
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
}

#[test]
fn resume_by_id_and_last_preserves_context_across_processes_with_one_new_input_per_turn() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("session-resume");
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(|_| openai_text_response("First investigation is recorded.")),
        Box::new(|request| {
            let text = model_request_text(&request.body);
            assert!(text.contains("Investigate the parser regression"));
            assert!(text.contains("First investigation is recorded."));
            assert!(text.contains("Continue with the parser fix"));
            openai_text_response("The parser fix is recorded.")
        }),
        Box::new(|request| {
            let text = model_request_text(&request.body);
            assert!(text.contains("The parser fix is recorded."));
            assert!(text.contains("Verify the completed fix"));
            openai_text_response("RESUME_LAST_VERIFIED")
        }),
    ]);
    workspace.configure_local_openai(&endpoint);
    for command in [
        local_default_agent_command(
            &workspace,
            "parser-session",
            "Investigate the parser regression",
            true,
            true,
        ),
        resume_command(&workspace, "parser-session", "Continue with the parser fix"),
        resume_command(&workspace, "--last", "Verify the completed fix"),
    ] {
        let output = run_to_completion(command, LOCAL_PROCESS_TIMEOUT);
        assert!(output.status.success(), "{}", output.stderr_text());
    }
    assert_eq!(server.join().unwrap().len(), 3);
    let history = browse(&workspace, &["show", "parser-session"]);
    assert_eq!(history["summary"]["run_count"], 3);
    assert_eq!(
        payload_count(&session_records(&workspace), "run_input_committed"),
        3
    );
    assert_eq!(run_payload_count(&workspace, "delivery_committed"), 3);
}

#[test]
fn session_listing_and_last_resume_are_scoped_to_the_selected_project() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("session-workspaces");
    fs::create_dir(workspace.path("other-project")).unwrap();
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(|_| openai_text_response("Primary project history")),
        Box::new(|_| openai_text_response("Other project history")),
        Box::new(|request| {
            let text = model_request_text(&request.body);
            assert!(text.contains("Primary project history"));
            assert!(!text.contains("Other project history"));
            openai_text_response("Correct project resumed")
        }),
    ]);
    workspace.configure_local_openai(&endpoint);
    let primary = local_default_agent_command(&workspace, "primary", "Work on primary", true, true);
    let mut other = local_default_agent_command(&workspace, "other", "Work on other", true, true);
    other.arg("-C").arg(workspace.path("other-project"));
    for command in [primary, other] {
        let output = run_to_completion(command, LOCAL_PROCESS_TIMEOUT);
        assert!(output.status.success(), "{}", output.stderr_text());
    }
    assert_eq!(
        browse(&workspace, &["list"])["sessions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let page = browse(&workspace, &["list", "--all", "--limit", "1"]);
    let cursor = page["next_cursor"].as_str().unwrap();
    let second = browse(
        &workspace,
        &["list", "--all", "--limit", "1", "--cursor", cursor],
    );
    assert_ne!(
        page["sessions"][0]["session_id"],
        second["sessions"][0]["session_id"]
    );
    let rejected = run_to_completion(
        resume_command(&workspace, "other", "Continue"),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(!rejected.status.success());
    assert!(rejected.stderr_text().contains("belongs to workspace"));
    let output = run_to_completion(
        resume_command(&workspace, "--last", "Continue primary"),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(output.status.success(), "{}", output.stderr_text());
    assert_eq!(server.join().unwrap().len(), 3);
}

#[test]
fn tui_resume_replays_user_tool_and_assistant_history_without_repeating_file_effects() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("session-tui");
    workspace.disable_exec();
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(|_| {
            openai_tool_response(
                "create-note",
                "file_write",
                json!({"path":"note.txt", "mode":"create", "content":"note written once"}),
            )
        }),
        Box::new(|_| openai_text_response("NOTE_CREATION_FINISHED")),
        Box::new(|request| {
            let text = model_request_text(&request.body);
            assert!(request.body["messages"]
                .to_string()
                .contains("note written once"));
            assert!(text.contains("Continue reviewing the note"));
            openai_text_response("NOTE_REVIEW_FINISHED")
        }),
    ]);
    workspace.configure_local_openai(&endpoint);
    let output = run_to_completion(
        local_default_agent_command(&workspace, "notes", "Create the requested note", true, true),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(output.status.success(), "{}", output.stderr_text());
    let mut tui = PtyHarness::spawn(resume_tui(&workspace, "notes", None));
    tui.resize(140, 50);
    for text in [
        "Resumed session notes",
        "Create the requested note",
        "file_write",
        "NOTE_CREATION_FINISHED",
    ] {
        tui.wait_for_text(text, LOCAL_PROCESS_TIMEOUT);
    }
    assert_eq!(
        payload_count(&session_records(&workspace), "run_input_committed"),
        1
    );
    tui.send_paste("Continue reviewing the note");
    tui.wait_for_text("NOTE_REVIEW_FINISHED", LOCAL_PROCESS_TIMEOUT);
    tui.wait_for_text("○ replied", LOCAL_PROCESS_TIMEOUT);
    exit_tui(tui);
    assert_eq!(server.join().unwrap().len(), 3);
    assert_eq!(
        payload_count(&session_records(&workspace), "tool_exchange_committed"),
        1
    );
    assert_eq!(
        fs::read_to_string(workspace.path("note.txt")).unwrap(),
        "note written once"
    );
}

#[test]
fn interrupted_session_reconciles_open_model_attempt_and_continues_without_repeating_tool() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("session-interrupted");
    workspace.disable_exec();
    workspace.rewrite_config(|config| {
        config["agent"]["model_retry"] = serde_yaml::to_value(
            json!({"max_retries": 3, "base_delay_ms": 30000, "max_delay_ms": 30000}),
        )
        .unwrap();
    });
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(|_| {
            openai_tool_response(
                "write-once",
                "file_write",
                json!({"path":"completed.txt", "mode":"create", "content":"completed before interruption"}),
            )
        }),
        Box::new(|_| FixtureHttpResponse {
            status: "429 Too Many Requests",
            repeat_handler: false,
            content_type: "application/json",
            body: br#"{"error":{"message":"temporary rate limit"}}"#.to_vec(),
        }),
        Box::new(|request| {
            assert!(request.body["messages"]
                .to_string()
                .contains("completed before interruption"));
            openai_text_response("INTERRUPTED_SESSION_CONTINUED")
        }),
    ]);
    workspace.configure_local_openai(&endpoint);
    let mut first = PtyHarness::spawn(local_tui_command(
        &workspace,
        "interrupted",
        "Work on the user's task.",
    ));
    first.wait_for_text("\u{1b}[?2004h", LOCAL_PROCESS_TIMEOUT);
    first.send_paste("Write the requested document");
    first.wait_for_text("retry 1/3", LOCAL_PROCESS_TIMEOUT);
    // A read-only browser may coexist with the writer, but a second Host may not.
    assert_eq!(
        browse(&workspace, &["list"])["sessions"][0]["status"],
        "unfinished"
    );
    let blocked = run_to_completion(
        resume_command(&workspace, "interrupted", "Continue"),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(!blocked.status.success());
    assert!(blocked.stderr_text().contains("active control writer"));
    first.child.kill().unwrap();
    first.finish(Duration::from_secs(5));
    let mut resumed = PtyHarness::spawn(resume_tui(
        &workspace,
        "interrupted",
        Some("Work on the user's task."),
    ));
    resumed.resize(140, 50);
    resumed.wait_for_text("Run incomplete", LOCAL_PROCESS_TIMEOUT);
    resumed.send_paste("Continue from the completed document");
    resumed.wait_for_text("INTERRUPTED_SESSION_CONTINUED", LOCAL_PROCESS_TIMEOUT);
    resumed.wait_for_text("○ replied", LOCAL_PROCESS_TIMEOUT);
    exit_tui(resumed);
    assert_eq!(server.join().unwrap().len(), 3);
    assert_eq!(
        payload_count(&session_records(&workspace), "tool_exchange_committed"),
        1
    );
    assert_eq!(run_payload_count(&workspace, "run_incomplete"), 1);
    assert_eq!(run_payload_count(&workspace, "delivery_committed"), 1);
}

#[test]
fn resume_answers_a_recovered_input_request_in_the_original_run() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("session-input");
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(|_| {
            openai_tool_response(
                "choose-scope",
                "orchestral_request_input",
                json!({"prompt":"Which package should be inspected?"}),
            )
        }),
        Box::new(|request| {
            assert!(request.body["messages"]
                .to_string()
                .contains("Inspect the parser package"));
            openai_text_response("RECOVERED_INPUT_APPLIED")
        }),
    ]);
    workspace.configure_local_openai(&endpoint);
    let system = "Work on the user's task.";
    let mut tui = PtyHarness::spawn(local_tui_command(&workspace, "input-session", system));
    tui.wait_for_text("\u{1b}[?2004h", LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("Inspect the requested package");
    tui.wait_for_text("Input requested", LOCAL_PROCESS_TIMEOUT);
    tui.child.kill().unwrap();
    tui.finish(Duration::from_secs(5));
    let mut command = resume_command(&workspace, "input-session", "Inspect the parser package");
    command.args(["--system-prompt", system]);
    let output = run_to_completion(command, LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success(), "{}", output.stderr_text());
    assert_eq!(output.stdout_text().trim(), "RECOVERED_INPUT_APPLIED");
    assert_eq!(server.join().unwrap().len(), 2);
    assert_eq!(
        browse(&workspace, &["show", "input-session"])["summary"]["run_count"],
        1
    );
    assert_eq!(run_payload_count(&workspace, "delivery_committed"), 1);
    assert_eq!(
        payload_count(&session_records(&workspace), "tool_exchange_committed"),
        1
    );
}
