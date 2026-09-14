use super::*;

#[test]
#[ignore = "spends real Google Vertex quota; requires ADC or a service-account credential"]
fn live_agent_uses_existing_project_instructions_for_coding() {
    let _guard = live_test_guard();
    for filename in ["AGENTS.md", "CLAUDE.md"] {
        let workspace = TestWorkspace::new("live-project-instructions");
        fs::create_dir_all(workspace.path("src")).unwrap();
        fs::create_dir_all(workspace.path("scripts")).unwrap();
        fs::create_dir_all(workspace.path("tests")).unwrap();
        fs::write(workspace.path("Cargo.toml"), "[package]\nname = \"project-instructions-fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n").unwrap();
        fs::write(workspace.path("src/lib.rs"),
            "pub fn total(values: &[u32]) -> u32 { values.iter().sum::<u32>().saturating_sub(1) }\n",
        ).unwrap();
        let assertions = concat!(
            "use project_instructions_fixture::total;\n",
            "#[test] fn totals_every_value() {\n",
            "    for values in [&[][..], &[2, 7, 13], &[1], &[4, 8, 16, 32]] {\n",
            "        assert_eq!(total(values), values.iter().copied().sum::<u32>());\n",
            "    }\n",
            "}\n",
        );
        let verification_script =
            "#!/bin/sh\nset -eu\ncargo test --quiet\nprintf 'ok\\n' > verification-ran\n";
        fs::write(workspace.path("tests/total.rs"), assertions).unwrap();
        fs::write(workspace.path("scripts/check.sh"), verification_script).unwrap();
        fs::write(workspace.path(filename), concat!(
            "Project verification: after changing Rust code, run `sh scripts/check.sh`. ",
            "This is the repository's verification entry point; direct cargo invocations alone are insufficient. ",
            "Keep the existing test assertions and verification script unchanged.\n",
        )).unwrap();
        let mut command = live_default_command(&workspace, "live-project-instructions", true, true);
        command.arg(
            "Repair the failing Rust project in this workspace and report the verified result.",
        );
        let output = run_to_completion(command, LIVE_CODING_PROCESS_TIMEOUT);
        assert!(output.status.success(), "{}", output.stderr_text());
        assert_eq!(
            fs::read_to_string(workspace.path("tests/total.rs")).unwrap(),
            assertions
        );
        assert_eq!(
            fs::read_to_string(workspace.path("scripts/check.sh")).unwrap(),
            verification_script
        );
        assert_eq!(
            fs::read_to_string(workspace.path("verification-ran")).unwrap(),
            "ok\n"
        );
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
        let records = session_records(&workspace);
        assert!(
            tool_exchanges(&records).iter().any(|exchange| {
                tool_name(exchange) == Some("exec_command")
                    && tool_result_is_error(exchange) == Some(false)
                    && tool_arguments(exchange)["cmd"]
                        .as_str()
                        .is_some_and(|cmd| cmd.contains("scripts/check.sh"))
            }),
            "the Agent must execute the verification prescribed by {filename}"
        );
    }
}

#[test]
fn cli_loads_ancestor_rules_and_claude_fallback_with_directory_scopes() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("project-rules");
    fs::create_dir(workspace.path(".git")).unwrap();
    fs::create_dir_all(workspace.path("packages/service")).unwrap();
    fs::create_dir(workspace.path("unrelated")).unwrap();
    fs::write(
        workspace.path("AGENTS.md"),
        "Use the repository's focused tests.\n",
    )
    .unwrap();
    fs::write(workspace.path("CLAUDE.md"), "IGNORED_ROOT_FALLBACK").unwrap();
    fs::write(
        workspace.path("packages/AGENTS.override.md"),
        "Keep public APIs documented.\n",
    )
    .unwrap();
    fs::write(workspace.path("packages/AGENTS.md"), "IGNORED_PACKAGE_BASE").unwrap();
    fs::write(workspace.path("packages/service/AGENTS.md"), " \n").unwrap();
    fs::write(
        workspace.path("packages/service/CLAUDE.md"),
        "Use service-local verification.\n",
    )
    .unwrap();
    fs::write(
        workspace.path("unrelated/AGENTS.md"),
        "UNRELATED_SIBLING_RULES",
    )
    .unwrap();
    let root = fs::canonicalize(&workspace.root).unwrap();
    let (endpoint, server) = spawn_fixture_http_server(vec![Box::new(move |request| {
        let text = model_request_text(&request.body);
        let root_rule = text.find("Use the repository's focused tests.").unwrap();
        let package_rule = text.find("Keep public APIs documented.").unwrap();
        let local_rule = text.find("Use service-local verification.").unwrap();
        assert!(
            root_rule < package_rule && package_rule < local_rule,
            "{text}"
        );
        assert!(!text.contains("IGNORED_"));
        assert!(!text.contains("UNRELATED_SIBLING_RULES"));
        assert!(text.contains(&format!(
            "\"scope\":{}",
            serde_json::to_string(&root).unwrap()
        )));
        assert!(text.contains(&format!(
            "\"scope\":{}",
            serde_json::to_string(&root.join("packages/service")).unwrap()
        )));
        assert!(text.contains("CLAUDE.md"));
        openai_text_response("project context received")
    })]);
    workspace.configure_local_openai(&endpoint);
    let mut command = local_default_agent_command(
        &workspace,
        "rules",
        "Inspect the project conventions.",
        true,
        true,
    );
    command.arg("-C").arg(workspace.path("packages/service"));
    let output = run_to_completion(command, LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success(), "{}", output.stderr_text());
    assert_eq!(output.stdout_text().trim(), "project context received");
    assert_eq!(server.join().unwrap().len(), 1);
}

#[test]
fn cli_preserves_instruction_snapshot_through_tools_and_reloads_on_next_host() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("project-rule-snapshot");
    workspace.disable_exec();
    fs::write(workspace.path("AGENTS.md"), "ORIGINAL_PROJECT_RULE").unwrap();
    fs::write(workspace.path("source.txt"), "source content").unwrap();
    let path = workspace.path("AGENTS.md");
    let (endpoint, server) = spawn_fixture_http_server(vec![
        Box::new(move |request| {
            assert!(model_request_text(&request.body).contains("ORIGINAL_PROJECT_RULE"));
            fs::write(&path, "UPDATED_PROJECT_RULE").unwrap();
            openai_tool_response("read-source", "file_read", json!({"path": "source.txt"}))
        }),
        Box::new(|request| {
            let text = model_request_text(&request.body);
            assert!(text.contains("ORIGINAL_PROJECT_RULE"));
            assert!(!text.contains("UPDATED_PROJECT_RULE"));
            openai_text_response("first host finished")
        }),
        Box::new(|request| {
            let system = request.body["messages"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|message| message["role"] == "system")
                .map(|message| message["content"].as_str().unwrap_or_default())
                .collect::<Vec<_>>()
                .join("\n");
            assert!(system.contains("UPDATED_PROJECT_RULE"));
            assert!(!system.contains("ORIGINAL_PROJECT_RULE"));
            openai_text_response("second host finished")
        }),
    ]);
    workspace.configure_local_openai(&endpoint);
    for prompt in [
        "Inspect the source file.",
        "Continue with the current project rules.",
    ] {
        let output = run_to_completion(
            local_default_agent_command(&workspace, "snapshot", prompt, true, true),
            LOCAL_PROCESS_TIMEOUT,
        );
        assert!(output.status.success(), "{}", output.stderr_text());
    }
    assert_eq!(server.join().unwrap().len(), 3);
    assert_eq!(
        payload_count(&session_records(&workspace), "run_input_committed"),
        2
    );
}

#[test]
fn cli_keeps_added_workspace_rules_scoped_and_deduplicates_ancestors() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("project-added-rules");
    fs::create_dir(workspace.path(".git")).unwrap();
    fs::create_dir_all(workspace.path("library")).unwrap();
    fs::write(workspace.path("AGENTS.md"), "COMMON_REPOSITORY_RULE").unwrap();
    fs::write(workspace.path("library/CLAUDE.md"), "LIBRARY_ONLY_RULE").unwrap();
    let library = fs::canonicalize(workspace.path("library")).unwrap();
    let (endpoint, server) = spawn_fixture_http_server(vec![Box::new(move |request| {
        let text = model_request_text(&request.body);
        assert_eq!(text.matches("COMMON_REPOSITORY_RULE").count(), 1);
        assert_eq!(text.matches("LIBRARY_ONLY_RULE").count(), 1);
        assert!(text.contains(&format!(
            "\"scope\":{}",
            serde_json::to_string(&library).unwrap()
        )));
        openai_text_response("both workspaces loaded")
    })]);
    workspace.configure_local_openai(&endpoint);
    let mut command = local_default_agent_command(
        &workspace,
        "added-rules",
        "Inspect both workspaces.",
        true,
        true,
    );
    command.arg("--add-dir").arg("library");
    let output = run_to_completion(command, LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success(), "{}", output.stderr_text());
    assert_eq!(server.join().unwrap().len(), 1);
}

#[test]
fn cli_allows_disabling_automatic_instructions_and_configuring_fallback_names() {
    let _guard = local_e2e_guard();
    for enabled in [true, false] {
        let workspace = TestWorkspace::new("project-custom-rules");
        fs::write(workspace.path("TEAM_GUIDE.md"), "CUSTOM_TEAM_CONVENTIONS").unwrap();
        workspace.rewrite_config(|config| {
            config["agent"]["project_instructions"] = serde_yaml::to_value(json!({
                "enabled": enabled, "fallback_filenames": ["CLAUDE.md", "TEAM_GUIDE.md"]
            }))
            .unwrap();
        });
        let (endpoint, server) = spawn_fixture_http_server(vec![Box::new(move |request| {
            assert_eq!(
                model_request_text(&request.body).contains("CUSTOM_TEAM_CONVENTIONS"),
                enabled
            );
            openai_text_response("configuration respected")
        })]);
        workspace.configure_local_openai(&endpoint);
        let output = run_to_completion(
            local_default_agent_command(&workspace, "custom", "Inspect the project.", true, true),
            LOCAL_PROCESS_TIMEOUT,
        );
        assert!(output.status.success(), "{}", output.stderr_text());
        assert_eq!(server.join().unwrap().len(), 1);
    }
}

#[test]
fn cli_reports_oversized_instructions_before_dispatching_a_model() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("project-rules-limit");
    fs::write(workspace.path("AGENTS.md"), "规则".repeat(32)).unwrap();
    workspace.rewrite_config(|config| {
        config["agent"]["project_instructions"] =
            serde_yaml::to_value(json!({"max_bytes": 16})).unwrap();
    });
    let output = run_to_completion(
        local_default_agent_command(&workspace, "limit", "Inspect the project.", true, true),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        output
            .stderr_text()
            .contains("project_instructions.max_bytes"),
        "{}",
        output.stderr_text()
    );
}
