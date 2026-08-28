//! Product-boundary E2E tests for the installed `orchestral` command.
//!
//! The live cases are ignored in ordinary CI because they spend real Vertex
//! quota. Run them explicitly with `--ignored --test-threads=1`. Once enabled,
//! missing credentials are a hard failure rather than a silent skip.

use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{mpsc, Mutex, OnceLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use portable_pty::{
    native_pty_system, Child as PtyChild, CommandBuilder, ExitStatus as PtyExitStatus, MasterPty,
    PtySize,
};
use serde_json::{json, Value};

const PROCESS_TIMEOUT: Duration = Duration::from_secs(120);
const LIVE_CODING_PROCESS_TIMEOUT: Duration = Duration::from_secs(240);
const LOCAL_PROCESS_TIMEOUT: Duration = Duration::from_secs(30);
const APPROVAL_PROMPT: &str = "Allow this exact operation? [y/N]";

static LIVE_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static LOCAL_E2E_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

struct TestWorkspace {
    root: PathBuf,
}

impl TestWorkspace {
    fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "orchestral-agent-e2e-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create isolated E2E workspace");
        let config = fs::read_to_string(repository_root().join("configs/orchestral.cli.yaml"))
            .expect("read canonical CLI config");
        fs::write(root.join("orchestral.yaml"), config).expect("write isolated CLI config");
        Self { root }
    }

    fn path(&self, relative: &str) -> PathBuf {
        self.root.join(relative)
    }

    fn configure_compaction(&self, minimum_source_records: usize, keep_recent_records: usize) {
        self.rewrite_config(|config| {
            config
                .replace(
                    "minimum_source_records: 32",
                    &format!("minimum_source_records: {minimum_source_records}"),
                )
                .replace(
                    "keep_recent_records: 16",
                    &format!("keep_recent_records: {keep_recent_records}"),
                )
        });
    }

    fn configure_local_openai(&self, endpoint: &str) {
        self.rewrite_config(|config| {
            config.replace(
                "kind: openai\n      api_key_env: OPENAI_API_KEY",
                &format!(
                    "kind: openai\n      endpoint: {endpoint}\n      api_key_env: OPENAI_API_KEY"
                ),
            )
        });
    }

    fn disable_exec(&self) {
        self.rewrite_config(|config| {
            config.replace(
                "tools:\n  max_timeout_ms: 30000\n  max_output_bytes: 1048576\n  exec:\n    enabled: true",
                "tools:\n  max_timeout_ms: 30000\n  max_output_bytes: 1048576\n  exec:\n    enabled: false",
            )
        });
    }

    fn configure_mcp_server(&self, endpoint: &str) {
        self.rewrite_config(|config| {
            config.replace(
                "mcp:\n  enabled: true\n  servers: []",
                &format!(
                    "mcp:\n  enabled: true\n  servers:\n    - name: fixture\n      required: true\n      transport:\n        type: streamable_http\n        endpoint: {endpoint}\n      startup_timeout_ms: 5000\n      tool_timeout_ms: 5000"
                ),
            )
        });
    }

    fn rewrite_config(&self, update: impl FnOnce(String) -> String) {
        let path = self.path("orchestral.yaml");
        let before = fs::read_to_string(&path).expect("read E2E config");
        let after = update(before.clone());
        assert_ne!(after, before, "E2E config rewrite did not match its target");
        fs::write(path, after).expect("write E2E config");
    }
}

impl Drop for TestWorkspace {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "preserving failed Agent E2E workspace at {}",
                self.root.display()
            );
            return;
        }
        let _ = fs::remove_dir_all(&self.root);
    }
}

struct ProcessOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl ProcessOutput {
    fn stdout_text(&self) -> String {
        String::from_utf8(self.stdout.clone()).expect("stdout must be UTF-8")
    }

    fn stderr_text(&self) -> String {
        String::from_utf8(self.stderr.clone()).expect("stderr must be UTF-8")
    }

    fn assert_no_ansi(&self) {
        assert!(!self.stdout.contains(&0x1b), "stdout leaked ANSI escapes");
        assert!(!self.stderr.contains(&0x1b), "stderr leaked ANSI escapes");
    }
}

#[test]
fn missing_google_credential_is_non_zero_and_actionable() {
    let workspace = TestWorkspace::new("missing-credential");
    let missing = workspace.path("does-not-exist.json");
    let mut command = base_command(&workspace);
    command
        .arg("--backend")
        .arg("google")
        .arg("--model")
        .arg("gemini-2.5-flash")
        .arg("--credential-file")
        .arg(&missing)
        .arg("do not run");

    let output = run_to_completion(command, Duration::from_secs(30));
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = output.stderr_text();
    assert!(
        stderr.contains("Google credential file not found"),
        "{stderr}"
    );
    assert!(
        stderr.contains(missing.to_string_lossy().as_ref()),
        "{stderr}"
    );
    output.assert_no_ansi();
}

#[test]
fn terminal_model_failure_is_non_zero_and_preserves_the_reason() {
    let workspace = TestWorkspace::new("terminal-failure");
    let config_path = workspace.path("orchestral.yaml");
    let config = fs::read_to_string(&config_path)
        .expect("read config")
        .replace(
            "kind: gemini\n      api_key_env: GOOGLE_API_KEY",
            "kind: gemini\n      endpoint: http://127.0.0.1:1\n      api_key_env: GOOGLE_API_KEY",
        );
    fs::write(&config_path, config).expect("write unreachable model endpoint");

    let mut command = base_command(&workspace);
    command
        .env("GOOGLE_API_KEY", "fixture-key")
        .arg("--backend")
        .arg("google")
        .arg("--model")
        .arg("fixture-model")
        .arg("hello");

    let output = run_to_completion(command, Duration::from_secs(30));
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = output.stderr_text();
    assert!(
        stderr.contains("Agent Run failed [model_unavailable]"),
        "{stderr}"
    );
    assert!(
        stderr.contains("Connection refused") || stderr.contains("error sending request"),
        "{stderr}"
    );
    output.assert_no_ansi();
}

#[test]
fn tui_pty_resolves_input_and_approval_then_cancels_another_run() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("tui-control");
    let (model_endpoint, model_server) = spawn_fixture_http_server(vec![
        Box::new(|request| {
            assert!(model_request_has_tool(
                &request.body,
                "orchestral_request_input"
            ));
            openai_tool_response(
                "ask-target",
                "orchestral_request_input",
                json!({"prompt": "INPUT_REQUEST_MARKER_7319"}),
            )
        }),
        Box::new(|request| {
            assert!(model_request_text(&request.body).contains("runtime-core"));
            openai_text_response("INPUT_RESOLVED_OK")
        }),
        Box::new(|_request| {
            openai_tool_response(
                "approval-write",
                "exec_command",
                json!({ "cmd": "touch tui-approved.marker" }),
            )
        }),
        Box::new(|request| {
            assert!(model_request_text(&request.body).contains("\"exit_code\":0"));
            openai_text_response("APPROVAL_RESOLVED_OK")
        }),
        Box::new(|_| {
            openai_tool_response(
                "cancel-input",
                "orchestral_request_input",
                json!({"prompt": "CANCEL_REQUEST_MARKER_4827"}),
            )
        }),
    ]);
    workspace.configure_local_openai(&model_endpoint);

    let mut tui = PtyHarness::spawn(local_tui_command(
        &workspace,
        "tui-control-session",
        "Follow the requested interaction exactly and keep final markers unchanged.",
    ));
    tui.wait_for_text("\u{1b}[?2004h", LOCAL_PROCESS_TIMEOUT);
    let before_resize = tui.latest.len();
    tui.resize(100, 30);
    tui.wait_for_text_after(
        "Ask Orchestral to do anything",
        before_resize,
        LOCAL_PROCESS_TIMEOUT,
    );
    tui.send_paste("start the input flow");
    tui.wait_for_text("INPUT_REQUEST_MARKER_7319", LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("runtime-core");
    tui.wait_for_text("INPUT_RESOLVED_OK", LOCAL_PROCESS_TIMEOUT);
    tui.wait_for_text_count("✓ done", 1, LOCAL_PROCESS_TIMEOUT);

    tui.send_paste("start the approval flow");
    tui.wait_for_text("Effects:", LOCAL_PROCESS_TIMEOUT);
    tui.send(b"a");
    tui.wait_for_text("APPROVAL_RESOLVED_OK", LOCAL_PROCESS_TIMEOUT);
    tui.wait_for_text_count("✓ done", 2, LOCAL_PROCESS_TIMEOUT);

    tui.send_paste("start the cancellation flow");
    tui.wait_for_text("CANCEL_REQUEST_MARKER_4827", LOCAL_PROCESS_TIMEOUT);
    tui.send(&[0x03]);
    tui.wait_for_text("cancelled", LOCAL_PROCESS_TIMEOUT);
    tui.send(&[0x1b]);
    tui.wait_for_text("\u{1b}[?1049l", LOCAL_PROCESS_TIMEOUT);

    let output = tui.finish(LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success(), "{}", output.text());
    assert!(output.text().contains("Ran 1 command"), "{}", output.text());
    assert!(workspace.path("tui-approved.marker").is_file());
    output.assert_terminal_restored();
    let requests = model_server.join().expect("join TUI model server");
    assert_eq!(requests.len(), 5);
    assert_eq!(run_payload_count(&workspace, "request_opened"), 3);
    assert_eq!(run_payload_count(&workspace, "request_resolved"), 2);
    assert_eq!(run_payload_count(&workspace, "run_cancelled"), 1);
}

#[test]
fn tui_pty_restores_terminal_after_agent_failure() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("tui-failure");
    let (model_endpoint, model_server) = spawn_fixture_http_server(vec![Box::new(|_| {
        openai_tool_response("unknown-call", "not_a_registered_tool", json!({}))
    })]);
    workspace.configure_local_openai(&model_endpoint);

    let mut tui = PtyHarness::spawn(local_tui_command(
        &workspace,
        "tui-failure-session",
        "Exercise the model response without changing its Tool call.",
    ));
    tui.wait_for_text("\u{1b}[?2004h", LOCAL_PROCESS_TIMEOUT);
    tui.send_paste("trigger the fixture failure");
    tui.wait_for_text("tool_not_found", LOCAL_PROCESS_TIMEOUT);
    tui.send(&[0x1b]);
    tui.wait_for_text("\u{1b}[?1049l", LOCAL_PROCESS_TIMEOUT);

    let output = tui.finish(LOCAL_PROCESS_TIMEOUT);
    assert!(output.status.success(), "{}", output.text());
    output.assert_terminal_restored();
    assert_eq!(
        model_server
            .join()
            .expect("join failure model server")
            .len(),
        1
    );
}

#[test]
fn piped_prompt_is_headless_and_stdout_contains_only_final_delivery() {
    let _guard = local_e2e_guard();
    const PIPE_PROMPT: &str = "PIPE_INPUT_MARKER_雪豹_7319";
    let workspace = TestWorkspace::new("headless-pipe");
    let (model_endpoint, model_server) = spawn_fixture_http_server(vec![Box::new(|request| {
        assert!(model_request_text(&request.body).contains(PIPE_PROMPT));
        openai_text_response("PIPE_FINAL_ONLY")
    })]);
    workspace.configure_local_openai(&model_endpoint);

    let mut command = root_command(&workspace);
    command
        .env("OPENAI_API_KEY", "fixture-key")
        .arg("--backend")
        .arg("openai")
        .arg("--model")
        .arg("fixture-model")
        .arg("--temperature")
        .arg("0")
        .arg("--session-id")
        .arg("headless-pipe-session")
        .arg("--no-mcp")
        .arg("--no-skills");
    let output = run_with_piped_input(command, PIPE_PROMPT.as_bytes(), LOCAL_PROCESS_TIMEOUT);

    assert!(output.status.success(), "{}", output.stderr_text());
    assert_eq!(output.stdout_text(), "PIPE_FINAL_ONLY\n");
    output.assert_no_ansi();
    assert_eq!(
        model_server.join().expect("join piped model server").len(),
        1
    );
}

#[test]
fn local_cli_creates_and_verifies_a_file_with_exec_disabled() {
    let _guard = local_e2e_guard();
    const CONTEXT_MARKER: &str = "需求上下文=雪豹-7319🧩";
    const GENERATED_CONTENT: &str = "generated from 雪豹-7319🧩\n";

    let workspace = TestWorkspace::new("patch-without-shell");
    fs::write(
        workspace.path("request.txt"),
        format!("{CONTEXT_MARKER}\nCreate generated.txt from this request.\n"),
    )
    .expect("write patch request fixture");
    workspace.disable_exec();

    let (model_endpoint, model_server) = spawn_fixture_http_server(vec![
        Box::new(|request| {
            assert_eq!(
                model_request_tool_names(&request.body),
                vec![
                    "apply_patch",
                    "artifact_read",
                    "file_read",
                    "orchestral_request_input"
                ]
            );
            openai_tool_response("read-request", "file_read", json!({"path": "request.txt"}))
        }),
        Box::new(|request| {
            assert!(model_request_text(&request.body).contains(CONTEXT_MARKER));
            openai_tool_response(
                "create-generated",
                "apply_patch",
                json!({
                    "patch": concat!(
                        "*** Begin Patch\n",
                        "*** Add File: generated.txt\n",
                        "+generated from 雪豹-7319🧩\n",
                        "*** End Patch"
                    )
                }),
            )
        }),
        Box::new(|request| {
            let context = model_request_text(&request.body);
            assert!(context.contains("\"operation\":\"add\""), "{context}");
            openai_tool_response(
                "verify-generated",
                "file_read",
                json!({"path": "generated.txt"}),
            )
        }),
        Box::new(|request| {
            assert!(model_request_text(&request.body).contains(GENERATED_CONTENT.trim_end()));
            openai_text_response("PATCH_WITHOUT_SHELL_OK")
        }),
    ]);
    workspace.configure_local_openai(&model_endpoint);

    let output = run_to_completion(
        local_agent_command(
            &workspace,
            "patch-without-shell-session",
            concat!(
                "Use workspace Tools to inspect the request, make the requested file change, ",
                "verify the resulting file, then report only the success marker."
            ),
            "Read request.txt, implement its request, and verify the result.",
            true,
            true,
        ),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(output.status.success(), "{}", output.stderr_text());
    assert_eq!(output.stdout_text().trim(), "PATCH_WITHOUT_SHELL_OK");
    assert_eq!(
        fs::read_to_string(workspace.path("generated.txt")).unwrap(),
        GENERATED_CONTENT
    );
    output.assert_no_ansi();

    let requests = model_server.join().expect("join local model server");
    assert_eq!(requests.len(), 4);
    let records = session_records(&workspace);
    let exchanges = tool_exchanges(&records);
    assert_eq!(exchanges.len(), 3);
    assert_eq!(tool_name(exchanges[0]), Some("file_read"));
    assert_eq!(tool_name(exchanges[1]), Some("apply_patch"));
    assert_eq!(tool_name(exchanges[2]), Some("file_read"));
    assert!(exchanges
        .iter()
        .all(|exchange| tool_result_is_error(exchange) == Some(false)));
}

#[test]
fn local_cli_reads_patches_and_runs_a_guarded_verification() {
    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("patch-and-verify");
    fs::create_dir_all(workspace.path("src")).expect("create source directory");
    fs::write(
        workspace.path("Cargo.toml"),
        concat!(
            "[package]\n",
            "name = \"agent-e1-deterministic-fixture\"\n",
            "version = \"0.1.0\"\n",
            "edition = \"2021\"\n",
        ),
    )
    .expect("write deterministic Cargo manifest");
    fs::write(
        workspace.path("src/lib.rs"),
        concat!(
            "pub fn answer() -> u32 { 41 }\n\n",
            "#[cfg(test)]\n",
            "mod tests {\n",
            "    use super::*;\n\n",
            "    #[test]\n",
            "    fn returns_the_documented_answer() {\n",
            "        assert_eq!(answer(), 42);\n",
            "    }\n",
            "}\n",
        ),
    )
    .expect("write buggy source fixture");

    let (model_endpoint, model_server) = spawn_fixture_http_server(vec![
        Box::new(|request| {
            assert!(model_request_has_tool(&request.body, "file_read"));
            assert!(model_request_has_tool(&request.body, "apply_patch"));
            assert!(model_request_has_tool(&request.body, "exec_command"));
            assert!(model_request_has_tool(&request.body, "write_stdin"));
            assert!(!model_request_has_tool(&request.body, "shell"));
            for old_name in [
                "pty_create",
                "pty_write",
                "pty_read",
                "pty_close",
                "pty_list",
            ] {
                assert!(!model_request_has_tool(&request.body, old_name));
            }
            openai_tool_response(
                "read-buggy-source",
                "file_read",
                json!({"path": "src/lib.rs"}),
            )
        }),
        Box::new(|request| {
            assert!(model_request_text(&request.body).contains("answer() -> u32 { 41 }"));
            openai_tool_response(
                "fix-answer",
                "apply_patch",
                json!({
                    "patch": concat!(
                        "*** Begin Patch\n",
                        "*** Update File: src/lib.rs\n",
                        "@@\n",
                        "-pub fn answer() -> u32 { 41 }\n",
                        "+pub fn answer() -> u32 { 42 }\n",
                        "*** End Patch"
                    )
                }),
            )
        }),
        Box::new(|request| {
            let context = model_request_text(&request.body);
            assert!(context.contains("\"operation\":\"update\""), "{context}");
            openai_tool_response(
                "verify-fixed-source",
                "exec_command",
                json!({
                    "cmd": "cargo test --offline --quiet >/dev/null 2>&1",
                    "yield_time_ms": 30_000
                }),
            )
        }),
        Box::new(|request| {
            let context = model_request_text(&request.body);
            assert!(context.contains("\"exit_code\":0"), "{context}");
            openai_text_response("PATCH_AND_VERIFY_OK")
        }),
    ]);
    workspace.configure_local_openai(&model_endpoint);

    let output = run_with_approval(
        local_default_agent_command(
            &workspace,
            "patch-and-verify-session",
            "Fix answer() so it returns the documented answer 42, then verify the change.",
            true,
            true,
        ),
        true,
        None,
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(output.status.success(), "{}", output.stderr_text());
    assert_eq!(output.stdout_text().trim(), "PATCH_AND_VERIFY_OK");
    assert!(fs::read_to_string(workspace.path("src/lib.rs"))
        .unwrap()
        .contains("pub fn answer() -> u32 { 42 }"));
    assert!(output.stderr_text().contains(APPROVAL_PROMPT));
    output.assert_no_ansi();

    let requests = model_server.join().expect("join local model server");
    assert_eq!(requests.len(), 4);
    let records = session_records(&workspace);
    let exchanges = tool_exchanges(&records);
    assert_eq!(exchanges.len(), 3);
    assert_eq!(tool_name(exchanges[0]), Some("file_read"));
    assert_eq!(tool_name(exchanges[1]), Some("apply_patch"));
    assert_eq!(tool_name(exchanges[2]), Some("exec_command"));
    assert!(exchanges
        .iter()
        .all(|exchange| tool_result_is_error(exchange) == Some(false)));
}

#[cfg(target_os = "macos")]
#[test]
fn local_exec_runs_toolchains_and_a_child_script_without_program_enumeration() {
    use std::os::unix::fs::PermissionsExt;

    let _guard = local_e2e_guard();
    let workspace = TestWorkspace::new("unified-exec-toolchains");
    let script = workspace.path("child-check.sh");
    fs::write(
        &script,
        "#!/bin/sh\npython3 -c 'print(\"CHILD_SCRIPT_OK\")'\n",
    )
    .expect("write child script");
    let mut permissions = fs::metadata(&script).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&script, permissions).unwrap();

    let (model_endpoint, model_server) = spawn_fixture_http_server(vec![
        Box::new(|request| {
            let names = model_request_tool_names(&request.body);
            assert!(names.contains(&"exec_command"));
            assert!(names.contains(&"write_stdin"));
            for removed in [
                "shell",
                "pty_create",
                "pty_write",
                "pty_read",
                "pty_close",
                "pty_list",
            ] {
                assert!(!names.contains(&removed));
            }
            openai_tool_response(
                "toolchain-check",
                "exec_command",
                json!({
                    "cmd": "cargo --version && python3 --version && ./child-check.sh",
                    "yield_time_ms": 5000
                }),
            )
        }),
        Box::new(|request| {
            let context = model_request_text(&request.body);
            assert!(context.contains("cargo 1."), "{context}");
            assert!(context.contains("Python 3."), "{context}");
            assert!(context.contains("\"alive\":true"), "{context}");
            openai_tool_response(
                "toolchain-poll",
                "write_stdin",
                json!({ "session_id": 1, "yield_time_ms": 5000 }),
            )
        }),
        Box::new(|request| {
            let context = model_request_text(&request.body);
            assert!(context.contains("CHILD_SCRIPT_OK"), "{context}");
            assert!(context.contains("\"exit_code\":0"), "{context}");
            openai_text_response("UNIFIED_EXEC_TOOLCHAINS_OK")
        }),
    ]);
    workspace.configure_local_openai(&model_endpoint);

    let output = run_with_approval(
        local_agent_command(
            &workspace,
            "unified-exec-toolchains-session",
            "Execute the requested local verification, use its observation, then report the marker.",
            "Verify the installed Rust and Python toolchains and run child-check.sh.",
            true,
            true,
        ),
        true,
        None,
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(output.status.success(), "{}", output.stderr_text());
    assert_eq!(output.stdout_text().trim(), "UNIFIED_EXEC_TOOLCHAINS_OK");
    output.assert_no_ansi();
    assert_eq!(model_server.join().unwrap().len(), 3);

    let records = session_records(&workspace);
    let exchanges = tool_exchanges(&records);
    assert_eq!(exchanges.len(), 2);
    assert_eq!(tool_name(exchanges[0]), Some("exec_command"));
    assert_eq!(tool_name(exchanges[1]), Some("write_stdin"));
    assert!(exchanges
        .iter()
        .all(|exchange| tool_result_is_error(exchange) == Some(false)));
}

#[test]
fn local_cli_skill_read_injects_instructions_and_journals_load() {
    let _guard = local_e2e_guard();
    const DESCRIPTOR_MARKER: &str = "E2E skill descriptor marker";
    const INSTRUCTION_MARKER: &str = "SKILL_E2E_INSTRUCTION_雪豹_7319";
    const RESULT_MARKER: &str = "SKILL_E2E_RESULT_云鲸_4827";

    let workspace = TestWorkspace::new("skill-entrypoint");
    let skill_directory = workspace.path("skills/e2e-skill");
    fs::create_dir_all(skill_directory.join("scripts")).expect("create Skill fixture resources");
    fs::write(
        skill_directory.join("SKILL.md"),
        format!(
            "---\nname: e2e-skill\ndescription: {DESCRIPTOR_MARKER}\nversion: 1.0.0\n---\nPrivate instruction {INSTRUCTION_MARKER}: run `sh scripts/collect.sh` with exec_command using this Skill's resource base as workdir, then report its exact stdout.\n"
        ),
    )
    .expect("write Skill fixture");
    fs::write(
        skill_directory.join("scripts/collect.sh"),
        format!("#!/bin/sh\nprintf '%s\\n' '{RESULT_MARKER}'\n"),
    )
    .expect("write Skill verification script");
    let skill_path = skill_directory
        .join("SKILL.md")
        .canonicalize()
        .expect("canonical Skill fixture path")
        .to_string_lossy()
        .to_string();
    let skill_resource_base = skill_directory
        .canonicalize()
        .expect("canonical Skill resource base")
        .to_string_lossy()
        .to_string();

    let (model_endpoint, model_server) = spawn_fixture_http_server(vec![
        Box::new(move |request| {
            let context = model_request_text(&request.body);
            assert!(context.contains("A Skill is a set of local instructions"));
            assert!(context.contains(&skill_path));
            assert!(model_request_has_tool(&request.body, "skill_read"));
            openai_tool_response(
                "read-e2e-skill",
                "skill_read",
                json!({ "name": "e2e-skill" }),
            )
        }),
        Box::new(move |request| {
            let context = model_request_text(&request.body);
            assert!(context.contains(INSTRUCTION_MARKER));
            assert!(context.contains("\"status\":\"loaded\""));
            assert!(context.contains(&skill_resource_base));
            assert!(context.contains("Relative paths in this Skill's instructions"));
            openai_tool_response(
                "run-skill-resource",
                "exec_command",
                json!({
                    "cmd": "sh scripts/collect.sh",
                    "workdir": skill_resource_base,
                    "yield_time_ms": 5_000
                }),
            )
        }),
        Box::new(|request| {
            assert!(model_request_text(&request.body).contains(RESULT_MARKER));
            openai_text_response(RESULT_MARKER)
        }),
    ]);
    workspace.configure_local_openai(&model_endpoint);

    let output = run_with_approval(
        local_default_agent_command(
            &workspace,
            "skill-entrypoint-session",
            "Use e2e-skill and complete its instructions without asking me for a command.",
            true,
            false,
        ),
        true,
        None,
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(output.status.success(), "{}", output.stderr_text());
    assert_eq!(output.stdout_text().trim(), RESULT_MARKER);
    output.assert_no_ansi();

    let requests = model_server.join().expect("join local model server");
    assert_eq!(requests.len(), 3);
    assert!(model_request_has_tool(&requests[0].body, "skill_read"));
    assert_single_apply_patch_tool(&requests[0].body);
    let first_context = model_request_text(&requests[0].body);
    assert!(first_context.contains(DESCRIPTOR_MARKER));
    assert!(!first_context.contains(INSTRUCTION_MARKER));
    let second_context = model_request_text(&requests[1].body);
    assert!(second_context.contains(INSTRUCTION_MARKER));
    assert!(second_context.contains("\"status\":\"loaded\""));

    let records = session_records(&workspace);
    assert_eq!(payload_count(&records, "skill_loaded"), 1);
    let exchanges = tool_exchanges(&records);
    assert_eq!(exchanges.len(), 2);
    assert_eq!(tool_name(exchanges[0]), Some("skill_read"));
    assert_eq!(tool_name(exchanges[1]), Some("exec_command"));
    assert_eq!(tool_result_is_error(exchanges[0]), Some(false));
    assert_eq!(tool_result_is_error(exchanges[1]), Some(false));
    assert!(
        tool_result_value(exchanges[1])
            .to_string()
            .contains(RESULT_MARKER),
        "Skill script output did not enter the Tool observation"
    );
}

#[test]
fn local_cli_discovers_calls_and_journals_an_mcp_tool() {
    let _guard = local_e2e_guard();
    const MCP_RESULT_MARKER: &str = "MCP_RESULT_云鲸_4827🔌";

    let workspace = TestWorkspace::new("mcp-entrypoint");
    let (mcp_base, mcp_server) = spawn_fixture_http_server(vec![
        Box::new(|request| {
            mcp_json_response(
                request,
                json!({
                    "supportedVersions": ["2026-07-28"],
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "fixture", "version": "1"}
                }),
            )
        }),
        Box::new(|request| {
            mcp_sse_response(
                request,
                json!({
                    "resultType": "complete",
                    "ttlMs": 1000,
                    "cacheScope": "private",
                    "tools": [{
                        "name": "lookup_marker",
                        "description": "Return the configured E2E marker",
                        "inputSchema": {
                            "type": "object",
                            "required": ["key"],
                            "properties": {"key": {"type": "string"}},
                            "additionalProperties": false
                        }
                    }]
                }),
            )
        }),
        Box::new(|request| {
            mcp_json_response(
                request,
                json!({
                    "resultType": "complete",
                    "content": [{"type": "text", "text": MCP_RESULT_MARKER}],
                    "isError": false
                }),
            )
        }),
    ]);
    workspace.configure_mcp_server(&format!("{mcp_base}/mcp"));

    let (model_endpoint, model_server) = spawn_fixture_http_server(vec![
        Box::new(|_| {
            openai_tool_response(
                "mcp-e2e-call",
                "mcp__fixture__lookup_marker",
                json!({"key": "能力"}),
            )
        }),
        Box::new(|_| openai_text_response("MCP_E2E_OK")),
    ]);
    workspace.configure_local_openai(&model_endpoint);

    let output = run_with_approval(
        local_default_agent_command(
            &workspace,
            "mcp-entrypoint-session",
            "Use the fixture MCP lookup_marker tool with key 能力.",
            false,
            true,
        ),
        true,
        None,
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(output.status.success(), "{}", output.stderr_text());
    assert_eq!(output.stdout_text().trim(), "MCP_E2E_OK");
    assert!(output.stderr_text().contains(APPROVAL_PROMPT));
    assert!(!output.stdout_text().contains(APPROVAL_PROMPT));
    output.assert_no_ansi();

    let model_requests = model_server.join().expect("join local model server");
    assert_eq!(model_requests.len(), 2);
    assert!(model_request_has_tool(
        &model_requests[0].body,
        "mcp__fixture__lookup_marker"
    ));
    assert_single_apply_patch_tool(&model_requests[0].body);
    assert!(model_request_text(&model_requests[1].body).contains(MCP_RESULT_MARKER));

    let mcp_requests = mcp_server.join().expect("join local MCP server");
    assert_eq!(mcp_requests.len(), 3);
    assert_eq!(mcp_requests[0].body["method"], "server/discover");
    assert_eq!(mcp_requests[1].body["method"], "tools/list");
    assert_eq!(mcp_requests[2].body["method"], "tools/call");
    assert_eq!(mcp_requests[2].body["params"]["name"], "lookup_marker");
    assert_eq!(mcp_requests[2].body["params"]["arguments"]["key"], "能力");
    assert_eq!(
        mcp_requests[2].headers.get("mcp-name").map(String::as_str),
        Some("lookup_marker")
    );

    let records = session_records(&workspace);
    let exchanges = tool_exchanges(&records);
    assert_eq!(exchanges.len(), 1);
    assert_eq!(tool_name(exchanges[0]), Some("mcp__fixture__lookup_marker"));
    assert_eq!(tool_result_is_error(exchanges[0]), Some(false));
    assert_eq!(run_payload_count(&workspace, "request_opened"), 1);
    assert_eq!(run_payload_count(&workspace, "request_resolved"), 1);
}

#[test]
#[ignore = "spends real Google Vertex quota; requires ADC or a service-account credential"]
fn live_vertex_stream_obeys_the_terminal_contract() {
    let _guard = live_test_guard();
    let workspace = TestWorkspace::new("terminal-contract");
    let output = run_live_agent(
        &workspace,
        "terminal-contract-session",
        "Do not call tools. Output exactly the text requested by the user, with no explanation.",
        "只回复：你好，Orchestral 👋",
    );

    assert!(output.status.success(), "{}", output.stderr_text());
    assert_eq!(output.stdout_text().trim(), "你好，Orchestral 👋");
    let stderr = output.stderr_text();
    assert!(stderr.contains("Generic Agent: backend=google model="));
    assert!(!stderr.contains("你好，Orchestral"));
    output.assert_no_ansi();

    let records = session_records(&workspace);
    assert_eq!(payload_count(&records, "tool_exchange_committed"), 0);
    assert_eq!(payload_count(&records, "run_output_committed"), 1);
}

#[test]
#[ignore = "spends real Google Vertex quota; requires ADC or a service-account credential"]
fn live_agent_recovers_after_a_failed_file_read() {
    let _guard = live_test_guard();
    let workspace = TestWorkspace::new("tool-recovery");
    let nonce = "海豚-7319-星图🧪";
    fs::write(workspace.path("fallback.txt"), format!("{nonce}\n"))
        .expect("write hidden fallback fact");

    let output = run_live_agent(
        &workspace,
        "tool-recovery-session",
        concat!(
            "Follow the requested tool order exactly. Never guess file contents. ",
            "After tool observations, output only the requested nonce with no explanation."
        ),
        concat!(
            "First call file_read for missing.txt. It does not exist and you must observe that ",
            "failure. Only after that failure, call file_read for fallback.txt and output the ",
            "complete recovery token found there."
        ),
    );

    assert!(output.status.success(), "{}", output.stderr_text());
    assert_eq!(output.stdout_text().trim(), nonce);
    output.assert_no_ansi();

    let records = session_records(&workspace);
    let exchanges = tool_exchanges(&records);
    assert_eq!(exchanges.len(), 2, "records={records:#?}");
    assert_eq!(tool_name(exchanges[0]), Some("file_read"));
    assert_eq!(tool_name(exchanges[1]), Some("file_read"));
    assert_eq!(tool_result_is_error(exchanges[0]), Some(true));
    assert_eq!(tool_result_is_error(exchanges[1]), Some(false));
    assert_ne!(exchanges[0]["request_id"], exchanges[1]["request_id"]);
    assert!(checkpoint_event_count(&workspace, "model_attempt_started") >= 3);
}

#[test]
#[ignore = "spends real Google Vertex quota; requires ADC or a service-account credential"]
fn live_e1_coding_closes_the_inspect_patch_verify_loop_three_times() {
    let _guard = live_test_guard();
    for attempt in 1..=3 {
        let workspace = TestWorkspace::new(&format!("live-e1-coding-{attempt}"));
        fs::create_dir_all(workspace.path("src")).expect("create live source directory");
        fs::write(
            workspace.path("Cargo.toml"),
            concat!(
                "[package]\n",
                "name = \"agent-e1-fixture\"\n",
                "version = \"0.1.0\"\n",
                "edition = \"2021\"\n",
            ),
        )
        .expect("write live Cargo manifest");
        fs::write(
            workspace.path("src/lib.rs"),
            concat!(
                "pub fn total(values: &[u32]) -> u32 {\n",
                "    values.iter().sum::<u32>().saturating_sub(1)\n",
                "}\n\n",
                "#[cfg(test)]\n",
                "mod tests {\n",
                "    use super::*;\n\n",
                "    #[test]\n",
                "    fn totals_every_value() {\n",
                "        assert_eq!(total(&[10, 20, 12]), 42);\n",
                "    }\n",
                "}\n",
            ),
        )
        .expect("write live failing source");

        let mut command = live_default_command(
            &workspace,
            &format!("live-e1-coding-session-{attempt}"),
            true,
            true,
        );
        command.arg(
            "Repair the failing Rust project in this workspace. Run its test suite and report the verified result.",
        );
        let output = run_with_approval(command, true, None, LIVE_CODING_PROCESS_TIMEOUT);
        assert!(output.status.success(), "{}", output.stderr_text());
        assert!(!output.stdout_text().trim().is_empty());
        output.assert_no_ansi();

        let verification = Command::new("cargo")
            .arg("test")
            .current_dir(&workspace.root)
            .output()
            .expect("independently verify the live coding fixture");
        assert!(
            verification.status.success(),
            "live coding attempt {attempt} did not fix the project:\n{}",
            String::from_utf8_lossy(&verification.stderr)
        );

        let records = session_records(&workspace);
        let exchanges = tool_exchanges(&records);
        let patch = exchanges
            .iter()
            .position(|exchange| {
                tool_name(exchange) == Some("apply_patch")
                    && tool_result_is_error(exchange) == Some(false)
            })
            .expect("live coding run omitted apply_patch");
        assert!(
            exchanges[..patch].iter().any(|exchange| {
                matches!(tool_name(exchange), Some("file_read" | "exec_command"))
                    && tool_result_is_error(exchange) == Some(false)
            }),
            "live coding run did not inspect before editing"
        );
        assert!(
            exchanges[patch + 1..].iter().any(|exchange| {
                matches!(tool_name(exchange), Some("exec_command" | "write_stdin"))
                    && tool_result_is_error(exchange) == Some(false)
                    && tool_result_value(exchange)["exit_code"] == json!(0)
            }),
            "live coding run did not verify successfully after editing"
        );
    }
}

#[test]
#[ignore = "spends real Google Vertex quota; requires ADC or a service-account credential"]
fn live_e2_skill_discovers_relative_resources_three_times() {
    let _guard = live_test_guard();
    for attempt in 1..=3 {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let marker = format!("LIVE_SKILL_EVIDENCE_海豚_{attempt}_{nonce}");
        let workspace = TestWorkspace::new(&format!("live-e2-skill-{attempt}"));
        let skill_directory = workspace.path("skills/release-evidence");
        fs::create_dir_all(skill_directory.join("scripts")).expect("create live Skill resources");
        fs::write(
            skill_directory.join("SKILL.md"),
            concat!(
                "---\n",
                "name: release-evidence\n",
                "description: Collect release evidence by following this workspace's local procedure\n",
                "version: 1.0.0\n",
                "---\n",
                "Run `sh scripts/collect.sh` with exec_command using this Skill's resource base as workdir. ",
                "Report the exact stdout as the evidence.\n",
            ),
        )
        .expect("write live Skill");
        fs::write(
            skill_directory.join("scripts/collect.sh"),
            format!("#!/bin/sh\nprintf '%s\\n' '{marker}'\n"),
        )
        .expect("write live Skill script");

        let mut command = live_default_command(
            &workspace,
            &format!("live-e2-skill-session-{attempt}"),
            true,
            false,
        );
        command.arg(
            "Follow the workspace procedure for collecting release evidence and report what it produces.",
        );
        let output = run_with_approval(command, true, None, PROCESS_TIMEOUT);
        assert!(output.status.success(), "{}", output.stderr_text());
        let final_output = output.stdout_text();
        assert!(final_output.contains(&marker), "{final_output}");
        output.assert_no_ansi();

        let records = session_records(&workspace);
        assert_eq!(payload_count(&records, "skill_loaded"), 1);
        let exchanges = tool_exchanges(&records);
        let skill_read = exchanges
            .iter()
            .position(|exchange| {
                tool_name(exchange) == Some("skill_read")
                    && tool_result_is_error(exchange) == Some(false)
            })
            .expect("live Skill run omitted skill_read");
        let script = exchanges
            .iter()
            .position(|exchange| {
                tool_name(exchange) == Some("exec_command")
                    && tool_result_is_error(exchange) == Some(false)
            })
            .expect("live Skill run omitted relative script execution");
        assert!(
            skill_read < script,
            "Skill instructions were not loaded first"
        );
        assert!(
            tool_result_value(exchanges[script])
                .to_string()
                .contains(&marker),
            "Skill script output was not returned through the Tool observation"
        );
    }
}

#[test]
#[ignore = "spends real Google Vertex quota; requires ADC or a service-account credential"]
fn live_e3_mcp_discovers_and_uses_the_configured_service_three_times() {
    let _guard = live_test_guard();
    for attempt in 1..=3 {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let color = format!("blue-{attempt}-{nonce}");
        let marker = format!("checkout={color}");
        let mcp_marker = marker.clone();
        let workspace = TestWorkspace::new(&format!("live-e3-mcp-{attempt}"));
        let (mcp_base, mcp_server) = spawn_fixture_http_server(vec![
            Box::new(|request| {
                mcp_json_response(
                    request,
                    json!({
                        "supportedVersions": ["2026-07-28"],
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "deployment-inventory", "version": "1"}
                    }),
                )
            }),
            Box::new(|request| {
                mcp_sse_response(
                    request,
                    json!({
                        "resultType": "complete",
                        "ttlMs": 1000,
                        "cacheScope": "private",
                        "tools": [{
                            "name": "deployment_color",
                            "description": "Look up the current deployment color for a service",
                            "inputSchema": {
                                "type": "object",
                                "required": ["service"],
                                "properties": {"service": {"type": "string"}},
                                "additionalProperties": false
                            }
                        }]
                    }),
                )
            }),
            Box::new(move |request| {
                assert_eq!(
                    request.body["params"]["arguments"]["service"],
                    json!("checkout")
                );
                mcp_json_response(
                    request,
                    json!({
                        "resultType": "complete",
                        "content": [{"type": "text", "text": mcp_marker}],
                        "isError": false
                    }),
                )
            }),
        ]);
        workspace.configure_mcp_server(&format!("{mcp_base}/mcp"));

        let mut command = live_default_command(
            &workspace,
            &format!("live-e3-mcp-session-{attempt}"),
            false,
            true,
        );
        command.arg(
            "Consult the configured deployment inventory and report the current color for service checkout.",
        );
        let output = run_with_approval(command, true, None, PROCESS_TIMEOUT);
        assert!(output.status.success(), "{}", output.stderr_text());
        assert!(
            output.stdout_text().contains(&color),
            "{}",
            output.stdout_text()
        );
        output.assert_no_ansi();

        let requests = mcp_server.join().expect("join live MCP fixture");
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].body["method"], "server/discover");
        assert_eq!(requests[1].body["method"], "tools/list");
        assert_eq!(requests[2].body["method"], "tools/call");
        let records = session_records(&workspace);
        let exchanges = tool_exchanges(&records);
        assert_eq!(exchanges.len(), 1);
        assert_eq!(
            tool_name(exchanges[0]),
            Some("mcp__fixture__deployment_color")
        );
        assert_eq!(tool_result_is_error(exchanges[0]), Some(false));
        assert!(
            tool_result_value(exchanges[0])
                .to_string()
                .contains(&marker),
            "MCP Tool observation did not preserve the service result"
        );
    }
}

#[test]
#[ignore = "spends real Google Vertex quota; requires ADC or a service-account credential"]
fn live_shell_effect_happens_only_after_exact_approval() {
    let _guard = live_test_guard();

    let denied = TestWorkspace::new("approval-deny");
    fs::write(denied.path("source.txt"), "审批边界-拒绝🧪\n").expect("write source");
    let denied_target = denied.path("approved.txt");
    let denied_output = run_live_shell_copy(&denied, false, &denied_target);
    assert!(
        denied_output.status.success(),
        "{}",
        denied_output.stderr_text()
    );
    assert!(!denied_target.exists(), "denied effect changed the world");
    assert!(denied_output.stderr_text().contains(APPROVAL_PROMPT));
    assert!(!denied_output.stdout_text().contains(APPROVAL_PROMPT));
    denied_output.assert_no_ansi();

    let allowed = TestWorkspace::new("approval-allow");
    let source_bytes = "审批边界-允许🧪\n".as_bytes();
    fs::write(allowed.path("source.txt"), source_bytes).expect("write source");
    let allowed_target = allowed.path("approved.txt");
    let allowed_output = run_live_shell_copy(&allowed, true, &allowed_target);
    assert!(
        allowed_output.status.success(),
        "{}",
        allowed_output.stderr_text()
    );
    assert_eq!(
        fs::read(&allowed_target).expect("approved target"),
        source_bytes
    );
    assert_eq!(fs::read(allowed.path("source.txt")).unwrap(), source_bytes);
    assert!(allowed_output.stderr_text().contains(APPROVAL_PROMPT));
    assert!(!allowed_output.stdout_text().contains(APPROVAL_PROMPT));
    allowed_output.assert_no_ansi();

    assert_eq!(run_payload_count(&denied, "request_opened"), 1);
    assert_eq!(run_payload_count(&allowed, "request_opened"), 1);
    assert_eq!(run_payload_count(&denied, "request_resolved"), 1);
    assert_eq!(run_payload_count(&allowed, "request_resolved"), 1);
}

#[test]
#[ignore = "spends real Google Vertex quota; requires ADC or a service-account credential"]
fn live_session_survives_process_restart_and_compaction() {
    let _guard = live_test_guard();
    let workspace = TestWorkspace::new("session-compaction");
    workspace.configure_compaction(2, 1);
    let session_id = "persistent-session-a9f4";
    let nonce = "会话令牌=星鲸-4827🧠";
    let system = concat!(
        "Conversation history is authoritative. Retain user-provided tokens across turns. ",
        "Do not call tools. Output only the requested value."
    );

    let first = run_live_agent(
        &workspace,
        session_id,
        system,
        &format!("Remember this exact token for the next turn: {nonce}. Reply only ACK-1."),
    );
    assert!(first.status.success(), "{}", first.stderr_text());
    assert_eq!(first.stdout_text().trim(), "ACK-1");

    let second = run_live_agent(
        &workspace,
        session_id,
        system,
        "Output the exact token I asked you to remember in the previous turn.",
    );
    assert!(second.status.success(), "{}", second.stderr_text());
    assert_eq!(second.stdout_text().trim(), nonce);

    let records = session_records(&workspace);
    assert_eq!(payload_count(&records, "compaction_committed"), 1);
    assert_eq!(payload_count(&records, "run_input_committed"), 2);
    assert_eq!(payload_count(&records, "run_output_committed"), 2);
    assert_eq!(payload_count(&records, "tool_exchange_committed"), 0);
}

fn live_test_guard() -> std::sync::MutexGuard<'static, ()> {
    LIVE_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn local_e2e_guard() -> std::sync::MutexGuard<'static, ()> {
    LOCAL_E2E_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn base_command(workspace: &TestWorkspace) -> Command {
    let mut command = root_command(workspace);
    command.arg("--no-mcp").arg("--no-skills");
    command
}

fn local_tui_command(
    workspace: &TestWorkspace,
    session_id: &str,
    system_prompt: &str,
) -> CommandBuilder {
    let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_orchestral"));
    command.cwd(&workspace.root);
    command.env("OPENAI_API_KEY", "fixture-key");
    command.args([
        "--config",
        workspace
            .path("orchestral.yaml")
            .to_str()
            .expect("TUI config path is UTF-8"),
        "--backend",
        "openai",
        "--model",
        "fixture-model",
        "--temperature",
        "0",
        "--session-id",
        session_id,
        "--system-prompt",
        system_prompt,
        "--no-mcp",
        "--no-skills",
    ]);
    command
}

struct PtyHarness {
    master: Box<dyn MasterPty + Send>,
    child: Box<dyn PtyChild + Send + Sync>,
    writer: Option<Box<dyn Write + Send>>,
    updates: mpsc::Receiver<Vec<u8>>,
    reader: JoinHandle<Vec<u8>>,
    latest: Vec<u8>,
}

struct PtyOutput {
    status: PtyExitStatus,
    bytes: Vec<u8>,
}

impl PtyOutput {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.bytes).into_owned()
    }

    fn assert_terminal_restored(&self) {
        for sequence in [
            b"\x1b[?1049h".as_slice(),
            b"\x1b[?1049l".as_slice(),
            b"\x1b[?2004h".as_slice(),
            b"\x1b[?2004l".as_slice(),
            b"\x1b[?25l".as_slice(),
            b"\x1b[?25h".as_slice(),
        ] {
            assert!(
                self.bytes
                    .windows(sequence.len())
                    .any(|part| part == sequence),
                "terminal sequence {:?} was missing from PTY output:\n{}",
                String::from_utf8_lossy(sequence),
                self.text()
            );
        }
        let leave = self
            .bytes
            .windows(b"\x1b[?1049l".len())
            .rposition(|part| part == b"\x1b[?1049l")
            .expect("alternate-screen leave sequence");
        let enter = self
            .bytes
            .windows(b"\x1b[?1049h".len())
            .rposition(|part| part == b"\x1b[?1049h")
            .expect("alternate-screen enter sequence");
        assert!(leave > enter, "alternate screen was not left after entry");
    }
}

impl PtyHarness {
    fn spawn(command: CommandBuilder) -> Self {
        let pair = native_pty_system()
            .openpty(PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            })
            .expect("open TUI PTY");
        let reader = pair
            .master
            .try_clone_reader()
            .expect("clone TUI PTY reader");
        let writer = pair.master.take_writer().expect("take TUI PTY writer");
        let child = pair
            .slave
            .spawn_command(command)
            .expect("spawn orchestral in TUI PTY");
        drop(pair.slave);
        let (updates, receiver) = mpsc::channel();
        let reader = thread::spawn(move || read_pty_with_updates(reader, updates));
        Self {
            master: pair.master,
            child,
            writer: Some(writer),
            updates: receiver,
            reader,
            latest: Vec::new(),
        }
    }

    fn resize(&self, cols: u16, rows: u16) {
        self.master
            .resize(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .expect("resize TUI PTY");
    }

    fn send(&mut self, bytes: &[u8]) {
        let writer = self.writer.as_mut().expect("TUI writer remains open");
        writer.write_all(bytes).expect("write TUI input");
        writer.flush().expect("flush TUI input");
    }

    fn send_paste(&mut self, text: &str) {
        self.send(format!("\x1b[200~{text}\x1b[201~\r").as_bytes());
    }

    fn wait_for_text(&mut self, marker: &str, timeout: Duration) {
        self.wait_for_text_count(marker, 1, timeout);
    }

    fn wait_for_text_after(&mut self, marker: &str, offset: usize, timeout: Duration) {
        let started = Instant::now();
        while started.elapsed() < timeout {
            if String::from_utf8_lossy(&self.latest[offset.min(self.latest.len())..])
                .contains(marker)
            {
                return;
            }
            match self.updates.recv_timeout(Duration::from_millis(100)) {
                Ok(bytes) => self.latest = bytes,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
            if let Some(status) = self.child.try_wait().expect("poll TUI child") {
                panic!(
                    "TUI exited before fresh marker {marker:?} with {status:?}:\n{}",
                    String::from_utf8_lossy(&self.latest)
                );
            }
        }
        panic!(
            "TUI did not render fresh marker {marker:?} within {timeout:?}:\n{}",
            String::from_utf8_lossy(&self.latest)
        );
    }

    fn wait_for_text_count(&mut self, marker: &str, count: usize, timeout: Duration) {
        let started = Instant::now();
        while started.elapsed() < timeout {
            if String::from_utf8_lossy(&self.latest)
                .matches(marker)
                .count()
                >= count
            {
                return;
            }
            match self.updates.recv_timeout(Duration::from_millis(100)) {
                Ok(bytes) => self.latest = bytes,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
            if let Some(status) = self.child.try_wait().expect("poll TUI child") {
                panic!(
                    "TUI exited before marker {marker:?} with {status:?}:\n{}",
                    String::from_utf8_lossy(&self.latest)
                );
            }
        }
        panic!(
            "TUI did not render marker {marker:?} {count} time(s) within {timeout:?}:\n{}",
            String::from_utf8_lossy(&self.latest)
        );
    }

    fn finish(mut self, timeout: Duration) -> PtyOutput {
        self.writer.take();
        let started = Instant::now();
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("poll TUI child") {
                break status;
            }
            if started.elapsed() >= timeout {
                let _ = self.child.kill();
                panic!(
                    "TUI did not exit within {timeout:?}:\n{}",
                    String::from_utf8_lossy(&self.latest)
                );
            }
            if let Ok(bytes) = self.updates.recv_timeout(Duration::from_millis(50)) {
                self.latest = bytes;
            }
        };
        drop(self.master);
        let bytes = self.reader.join().expect("join TUI PTY reader");
        PtyOutput { status, bytes }
    }
}

fn read_pty_with_updates(
    mut reader: Box<dyn Read + Send>,
    updates: mpsc::Sender<Vec<u8>>,
) -> Vec<u8> {
    let mut all = Vec::new();
    let mut buffer = [0_u8; 1024];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) | Err(_) => return all,
            Ok(count) => {
                all.extend_from_slice(&buffer[..count]);
                let _ = updates.send(all.clone());
            }
        }
    }
}

fn root_command(workspace: &TestWorkspace) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_orchestral"));
    command
        .current_dir(&workspace.root)
        .arg("--config")
        .arg(workspace.path("orchestral.yaml"));
    command
}

fn local_agent_command(
    workspace: &TestWorkspace,
    session_id: &str,
    system_prompt: &str,
    prompt: &str,
    disable_mcp: bool,
    disable_skills: bool,
) -> Command {
    let mut command = root_command(workspace);
    command
        .env("OPENAI_API_KEY", "fixture-key")
        .arg("--backend")
        .arg("openai")
        .arg("--model")
        .arg("fixture-model")
        .arg("--temperature")
        .arg("0")
        .arg("--session-id")
        .arg(session_id)
        .arg("--system-prompt")
        .arg(system_prompt);
    if disable_mcp {
        command.arg("--no-mcp");
    }
    if disable_skills {
        command.arg("--no-skills");
    }
    command.arg(prompt);
    command
}

fn local_default_agent_command(
    workspace: &TestWorkspace,
    session_id: &str,
    prompt: &str,
    disable_mcp: bool,
    disable_skills: bool,
) -> Command {
    let mut command = root_command(workspace);
    command
        .env("OPENAI_API_KEY", "fixture-key")
        .arg("--backend")
        .arg("openai")
        .arg("--model")
        .arg("fixture-model")
        .arg("--temperature")
        .arg("0")
        .arg("--session-id")
        .arg(session_id);
    if disable_mcp {
        command.arg("--no-mcp");
    }
    if disable_skills {
        command.arg("--no-skills");
    }
    command.arg(prompt);
    command
}

fn live_command(
    workspace: &TestWorkspace,
    session_id: &str,
    system_prompt: &str,
    prompt: &str,
) -> Command {
    let mut command = live_default_command(workspace, session_id, true, true);
    command
        .arg("--system-prompt")
        .arg(system_prompt)
        .arg(prompt);
    command
}

fn live_default_command(
    workspace: &TestWorkspace,
    session_id: &str,
    disable_mcp: bool,
    disable_skills: bool,
) -> Command {
    let mut command = root_command(workspace);
    command
        .arg("--backend")
        .arg("google")
        .arg("--model")
        .arg(live_model())
        .arg("--temperature")
        .arg("0")
        .arg("--session-id")
        .arg(session_id);
    if disable_mcp {
        command.arg("--no-mcp");
    }
    if disable_skills {
        command.arg("--no-skills");
    }
    if let Some(path) = explicit_live_credential() {
        command.arg("--credential-file").arg(path);
    } else {
        assert!(
            standard_adc_is_available(),
            "live E2E requires ORCHESTRAL_E2E_CREDENTIAL_FILE, \
             GOOGLE_APPLICATION_CREDENTIALS, repository credential.json, or standard ADC"
        );
    }
    command
}

fn run_live_agent(
    workspace: &TestWorkspace,
    session_id: &str,
    system_prompt: &str,
    prompt: &str,
) -> ProcessOutput {
    run_to_completion(
        live_command(workspace, session_id, system_prompt, prompt),
        PROCESS_TIMEOUT,
    )
}

fn run_live_shell_copy(workspace: &TestWorkspace, allow: bool, target: &Path) -> ProcessOutput {
    let command = live_command(
        workspace,
        if allow {
            "approval-allow-session"
        } else {
            "approval-deny-session"
        },
        concat!(
            "Use exactly one exec_command call with `cp source.txt approved.txt`. ",
            "Do not call other tools and do not retry a ",
            "denied operation. After the observation, briefly report the actual outcome."
        ),
        "Copy source.txt to approved.txt now.",
    );
    run_with_approval(command, allow, Some(target), PROCESS_TIMEOUT)
}

fn live_model() -> String {
    std::env::var("ORCHESTRAL_E2E_MODEL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "gemini-3.1-pro-preview".to_owned())
}

fn explicit_live_credential() -> Option<PathBuf> {
    std::env::var_os("ORCHESTRAL_E2E_CREDENTIAL_FILE")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            let path = repository_root().join("credential.json");
            path.is_file().then_some(path)
        })
}

fn standard_adc_is_available() -> bool {
    std::env::var_os("GOOGLE_APPLICATION_CREDENTIALS").is_some_and(|value| !value.is_empty())
        || well_known_adc_path().is_some_and(|path| path.is_file())
}

fn well_known_adc_path() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        return std::env::var_os("APPDATA")
            .map(PathBuf::from)
            .map(|root| root.join("gcloud/application_default_credentials.json"));
    }
    #[cfg(not(target_os = "windows"))]
    {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .map(|root| root.join(".config/gcloud/application_default_credentials.json"))
    }
}

fn run_to_completion(mut command: Command, timeout: Duration) -> ProcessOutput {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn orchestral CLI");
    let stdout = child.stdout.take().expect("capture stdout");
    let stderr = child.stderr.take().expect("capture stderr");
    let stdout_reader = thread::spawn(move || read_all(stdout));
    let stderr_reader = thread::spawn(move || read_all(stderr));
    let status = wait_for_child(&mut child, timeout);
    ProcessOutput {
        status,
        stdout: stdout_reader.join().expect("join stdout reader"),
        stderr: stderr_reader.join().expect("join stderr reader"),
    }
}

fn run_with_piped_input(mut command: Command, input: &[u8], timeout: Duration) -> ProcessOutput {
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn piped orchestral CLI");
    let mut stdin = child.stdin.take().expect("capture piped stdin");
    stdin.write_all(input).expect("write piped Agent input");
    drop(stdin);
    let stdout = child.stdout.take().expect("capture stdout");
    let stderr = child.stderr.take().expect("capture stderr");
    let stdout_reader = thread::spawn(move || read_all(stdout));
    let stderr_reader = thread::spawn(move || read_all(stderr));
    let status = wait_for_child(&mut child, timeout);
    ProcessOutput {
        status,
        stdout: stdout_reader.join().expect("join stdout reader"),
        stderr: stderr_reader.join().expect("join stderr reader"),
    }
}

fn run_with_approval(
    mut command: Command,
    allow: bool,
    target: Option<&Path>,
    timeout: Duration,
) -> ProcessOutput {
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn approval E2E");
    let mut stdin = child.stdin.take().expect("capture stdin");
    let stdout = child.stdout.take().expect("capture stdout");
    let stderr = child.stderr.take().expect("capture stderr");
    let stdout_reader = thread::spawn(move || read_all(stdout));
    let (stderr_updates, receiver) = mpsc::channel();
    let stderr_reader = thread::spawn(move || read_with_updates(stderr, stderr_updates));

    let started = Instant::now();
    let mut approvals_sent = 0_usize;
    let mut exited = None;
    while started.elapsed() < timeout {
        if let Ok(bytes) = receiver.recv_timeout(Duration::from_millis(100)) {
            let text = String::from_utf8_lossy(&bytes);
            let observed_prompts = text.matches(APPROVAL_PROMPT).count();
            while approvals_sent < observed_prompts {
                if approvals_sent == 0 {
                    if let Some(target) = target {
                        assert!(!target.exists(), "effect happened before exact approval");
                    }
                }
                stdin
                    .write_all(if allow { b"y\n" } else { b"n\n" })
                    .expect("answer approval prompt");
                stdin.flush().expect("flush approval answer");
                approvals_sent += 1;
            }
        }
        if let Some(status) = child.try_wait().expect("poll approval child") {
            exited = Some(status);
            break;
        }
    }
    let timed_out = exited.is_none() && started.elapsed() >= timeout;
    if timed_out {
        let _ = child.kill();
    }
    drop(stdin);
    let status = exited.unwrap_or_else(|| child.wait().expect("wait for approval child"));
    let output = ProcessOutput {
        status,
        stdout: stdout_reader.join().expect("join stdout reader"),
        stderr: stderr_reader.join().expect("join stderr reader"),
    };
    assert!(
        approvals_sent > 0,
        "CLI never opened an approval request (status={}):\nstdout:\n{}\nstderr:\n{}",
        output.status,
        output.stdout_text(),
        output.stderr_text()
    );
    assert!(
        !timed_out,
        "approval E2E exceeded {timeout:?}:\nstdout:\n{}\nstderr:\n{}",
        output.stdout_text(),
        output.stderr_text()
    );
    output
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> ExitStatus {
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("poll orchestral CLI") {
            return status;
        }
        if started.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            panic!("orchestral CLI exceeded {timeout:?}");
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn read_all(mut stream: impl Read) -> Vec<u8> {
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).expect("read process stream");
    bytes
}

fn read_with_updates(mut stream: impl Read, updates: mpsc::Sender<Vec<u8>>) -> Vec<u8> {
    let mut all = Vec::new();
    let mut buffer = [0_u8; 256];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => return all,
            Ok(count) => {
                all.extend_from_slice(&buffer[..count]);
                let _ = updates.send(all.clone());
            }
            Err(error) => panic!("read process stderr: {error}"),
        }
    }
}

fn session_records(workspace: &TestWorkspace) -> Vec<Value> {
    let files = journal_files(workspace, "session-");
    assert_eq!(files.len(), 1, "expected one Session journal: {files:?}");
    serde_json::from_slice(&fs::read(&files[0]).expect("read Session journal"))
        .expect("parse Session journal")
}

fn payload_count(records: &[Value], kind: &str) -> usize {
    records
        .iter()
        .filter(|record| record["payload"]["type"].as_str() == Some(kind))
        .count()
}

fn tool_exchanges(records: &[Value]) -> Vec<&Value> {
    records
        .iter()
        .filter_map(|record| {
            (record["payload"]["type"].as_str() == Some("tool_exchange_committed"))
                .then_some(&record["payload"])
        })
        .collect()
}

fn tool_name(exchange: &Value) -> Option<&str> {
    exchange["assistant"]["content"]
        .as_array()?
        .iter()
        .find(|item| item["type"].as_str() == Some("tool_call"))?["name"]
        .as_str()
}

fn tool_result_is_error(exchange: &Value) -> Option<bool> {
    exchange["tool"]["content"]
        .as_array()?
        .iter()
        .find(|item| item["type"].as_str() == Some("tool_result"))?["is_error"]
        .as_bool()
}

fn tool_result_value(exchange: &Value) -> &Value {
    exchange["tool"]["content"]
        .as_array()
        .and_then(|content| {
            content
                .iter()
                .find(|item| item["type"].as_str() == Some("tool_result"))
        })
        .map(|item| &item["result"])
        .expect("Tool exchange contains one Tool result")
}

fn checkpoint_event_count(workspace: &TestWorkspace, kind: &str) -> usize {
    journal_files(workspace, "generic-checkpoint-")
        .into_iter()
        .map(|path| {
            serde_json::from_slice::<Value>(&fs::read(path).expect("read checkpoint journal"))
                .expect("parse checkpoint journal")
        })
        .flat_map(|value| value["records"].as_array().cloned().unwrap_or_default())
        .filter(|record| record["payload"]["type"].as_str() == Some(kind))
        .count()
}

fn run_payload_count(workspace: &TestWorkspace, kind: &str) -> usize {
    journal_files(workspace, "run-")
        .into_iter()
        .map(|path| {
            serde_json::from_slice::<Value>(&fs::read(path).expect("read Run journal"))
                .expect("parse Run journal")
        })
        .flat_map(|value| value["records"].as_array().cloned().unwrap_or_default())
        .filter(|record| record["event"]["payload"]["type"].as_str() == Some(kind))
        .count()
}

fn journal_files(workspace: &TestWorkspace, prefix: &str) -> Vec<PathBuf> {
    let directory = workspace.path(".orchestral/agent-journal");
    let mut files = fs::read_dir(&directory)
        .unwrap_or_else(|error| panic!("read journal directory '{}': {error}", directory.display()))
        .map(|entry| entry.expect("journal entry").path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(prefix) && name.ends_with(".json"))
        })
        .collect::<Vec<_>>();
    files.sort();
    files
}

struct CapturedHttpRequest {
    headers: std::collections::BTreeMap<String, String>,
    body: Value,
}

struct FixtureHttpResponse {
    content_type: &'static str,
    body: Vec<u8>,
}

type FixtureHttpHandler = Box<dyn Fn(&CapturedHttpRequest) -> FixtureHttpResponse + Send + 'static>;

fn spawn_fixture_http_server(
    handlers: Vec<FixtureHttpHandler>,
) -> (String, JoinHandle<Vec<CapturedHttpRequest>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind local HTTP fixture");
    listener
        .set_nonblocking(true)
        .expect("configure local HTTP fixture");
    let address = listener.local_addr().expect("local HTTP fixture address");
    let server = thread::spawn(move || {
        let started = Instant::now();
        let mut captured = Vec::with_capacity(handlers.len());
        for handler in handlers {
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            started.elapsed() < Duration::from_secs(35),
                            "local HTTP fixture did not receive every expected request"
                        );
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("accept local HTTP fixture request: {error}"),
                }
            };
            stream
                .set_nonblocking(false)
                .expect("use blocking fixture connection");
            stream
                .set_read_timeout(Some(Duration::from_secs(20)))
                .expect("bound fixture read timeout");
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .expect("bound fixture write timeout");
            let request = read_http_fixture_request(&mut stream);
            let response = handler(&request);
            write_http_fixture_response(&mut stream, response);
            captured.push(request);
        }
        captured
    });
    (format!("http://{address}"), server)
}

fn read_http_fixture_request(stream: &mut TcpStream) -> CapturedHttpRequest {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    let deadline = Instant::now() + Duration::from_secs(35);
    let header_end = loop {
        let count = read_http_fixture_chunk(stream, &mut buffer, deadline);
        assert!(count > 0, "HTTP fixture request ended before its headers");
        bytes.extend_from_slice(&buffer[..count]);
        assert!(
            bytes.len() <= 1024 * 1024,
            "HTTP fixture request is oversized"
        );
        if let Some(index) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let header_text = std::str::from_utf8(&bytes[..header_end]).expect("HTTP headers are UTF-8");
    let headers = header_text
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let content_length = headers
        .get("content-length")
        .expect("HTTP fixture request has Content-Length")
        .parse::<usize>()
        .expect("HTTP fixture Content-Length is valid");
    while bytes.len() < header_end + content_length {
        let count = read_http_fixture_chunk(stream, &mut buffer, deadline);
        assert!(count > 0, "HTTP fixture request body ended early");
        bytes.extend_from_slice(&buffer[..count]);
    }
    CapturedHttpRequest {
        headers,
        body: serde_json::from_slice(&bytes[header_end..header_end + content_length])
            .expect("HTTP fixture request body is JSON"),
    }
}

fn read_http_fixture_chunk(stream: &mut TcpStream, buffer: &mut [u8], deadline: Instant) -> usize {
    loop {
        match stream.read(buffer) {
            Ok(count) => return count,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) && Instant::now() < deadline => {}
            Err(error) => panic!("read HTTP fixture request: {error}"),
        }
    }
}

fn write_http_fixture_response(stream: &mut TcpStream, response: FixtureHttpResponse) {
    let headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        response.content_type,
        response.body.len()
    );
    stream
        .write_all(headers.as_bytes())
        .expect("write HTTP fixture headers");
    stream
        .write_all(&response.body)
        .expect("write HTTP fixture body");
    stream.flush().expect("flush HTTP fixture response");
}

fn openai_tool_response(call_id: &str, name: &str, arguments: Value) -> FixtureHttpResponse {
    let start = json!({
        "choices": [{
            "delta": {
                "tool_calls": [{
                    "index": 0,
                    "id": call_id,
                    "function": {
                        "name": name,
                        "arguments": arguments.to_string()
                    }
                }]
            }
        }]
    });
    let finish = json!({"choices": [{"delta": {}, "finish_reason": "tool_calls"}]});
    sse_response(format!(
        "data: {start}\n\ndata: {finish}\n\ndata: [DONE]\n\n"
    ))
}

fn openai_text_response(text: &str) -> FixtureHttpResponse {
    let message = json!({
        "choices": [{
            "delta": {"content": text},
            "finish_reason": "stop"
        }]
    });
    sse_response(format!("data: {message}\n\ndata: [DONE]\n\n"))
}

fn mcp_json_response(request: &CapturedHttpRequest, result: Value) -> FixtureHttpResponse {
    json_response(json!({
        "jsonrpc": "2.0",
        "id": request.body["id"].clone(),
        "result": result
    }))
}

fn mcp_sse_response(request: &CapturedHttpRequest, result: Value) -> FixtureHttpResponse {
    let response = json!({
        "jsonrpc": "2.0",
        "id": request.body["id"].clone(),
        "result": result
    });
    sse_response(format!(": fixture\r\ndata: {response}\r\n\r\n"))
}

fn json_response(body: Value) -> FixtureHttpResponse {
    FixtureHttpResponse {
        content_type: "application/json",
        body: serde_json::to_vec(&body).expect("serialize HTTP fixture JSON"),
    }
}

fn sse_response(body: String) -> FixtureHttpResponse {
    FixtureHttpResponse {
        content_type: "text/event-stream",
        body: body.into_bytes(),
    }
}

fn model_request_has_tool(request: &Value, name: &str) -> bool {
    request["tools"].as_array().is_some_and(|tools| {
        tools
            .iter()
            .any(|tool| tool["function"]["name"].as_str() == Some(name))
    })
}

fn model_request_tool_names(request: &Value) -> Vec<&str> {
    let mut names = request["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|tool| tool["function"]["name"].as_str())
        .collect::<Vec<_>>();
    names.sort_unstable();
    names
}

fn assert_single_apply_patch_tool(request: &Value) {
    assert_eq!(
        model_request_tool_names(request)
            .into_iter()
            .filter(|name| *name == "apply_patch")
            .count(),
        1,
        "apply_patch must survive composition exactly once"
    );
}

fn model_request_text(request: &Value) -> String {
    request["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|message| message["content"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("CLI crate must live under apps/")
        .to_path_buf()
}
