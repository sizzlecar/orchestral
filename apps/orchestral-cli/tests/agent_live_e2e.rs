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

use serde_json::{json, Value};

const PROCESS_TIMEOUT: Duration = Duration::from_secs(120);
const LOCAL_PROCESS_TIMEOUT: Duration = Duration::from_secs(30);
const APPROVAL_PROMPT: &str = "Allow this exact operation? [y/N]";

static LIVE_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

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

    fn configure_skill_directory(&self, relative: &str) {
        self.rewrite_config(|config| {
            config
                .replace("auto_discover: true", "auto_discover: false")
                .replace(
                    "  directories: []",
                    &format!("  directories:\n    - {relative}"),
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
fn local_cli_activates_a_skill_and_reprojects_its_instructions() {
    const DESCRIPTOR_MARKER: &str = "E2E skill descriptor marker";
    const INSTRUCTION_MARKER: &str = "SKILL_E2E_INSTRUCTION_雪豹_7319";

    let workspace = TestWorkspace::new("skill-entrypoint");
    let skill_directory = workspace.path("fixture-skills/e2e-skill");
    fs::create_dir_all(&skill_directory).expect("create Skill fixture directory");
    fs::write(
        skill_directory.join("SKILL.md"),
        format!(
            "---\nname: e2e-skill\ndescription: {DESCRIPTOR_MARKER}\nversion: 1.0.0\n---\nFollow this private instruction marker: {INSTRUCTION_MARKER}\n"
        ),
    )
    .expect("write Skill fixture");
    workspace.configure_skill_directory("fixture-skills");

    let (model_endpoint, model_server) = spawn_fixture_http_server(vec![
        Box::new(|request| {
            let digest = skill_digest_from_model_request(&request.body, "e2e-skill");
            openai_tool_response(
                "activate-e2e-skill",
                "orchestral_skill_activate",
                json!({
                    "name": "e2e-skill",
                    "expected_digest": digest,
                    "reason": "the user explicitly requested the E2E Skill"
                }),
            )
        }),
        Box::new(|_| openai_text_response("SKILL_E2E_OK")),
    ]);
    workspace.configure_local_openai(&model_endpoint);

    let output = run_to_completion(
        local_agent_command(
            &workspace,
            "skill-entrypoint-session",
            "Use the requested Skill, then return only the exact success marker.",
            "Activate e2e-skill and complete its instructions.",
            true,
            false,
        ),
        LOCAL_PROCESS_TIMEOUT,
    );
    assert!(output.status.success(), "{}", output.stderr_text());
    assert_eq!(output.stdout_text().trim(), "SKILL_E2E_OK");
    output.assert_no_ansi();

    let requests = model_server.join().expect("join local model server");
    assert_eq!(requests.len(), 2);
    assert!(model_request_has_tool(
        &requests[0].body,
        "orchestral_skill_activate"
    ));
    let first_context = model_request_text(&requests[0].body);
    assert!(first_context.contains(DESCRIPTOR_MARKER));
    assert!(!first_context.contains(INSTRUCTION_MARKER));
    let second_context = model_request_text(&requests[1].body);
    assert!(second_context.contains(INSTRUCTION_MARKER));
    assert!(second_context.contains("\"status\":\"activated\""));

    let records = session_records(&workspace);
    assert_eq!(payload_count(&records, "skill_activated"), 1);
    let exchanges = tool_exchanges(&records);
    assert_eq!(exchanges.len(), 1);
    assert_eq!(tool_name(exchanges[0]), Some("orchestral_skill_activate"));
    assert_eq!(tool_result_is_error(exchanges[0]), Some(false));
}

#[test]
fn local_cli_discovers_calls_and_journals_an_mcp_tool() {
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
        local_agent_command(
            &workspace,
            "mcp-entrypoint-session",
            "Call the requested MCP Tool exactly once, then return only the success marker.",
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

fn base_command(workspace: &TestWorkspace) -> Command {
    let mut command = root_command(workspace);
    command.arg("--no-mcp").arg("--no-skills");
    command
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

fn live_command(
    workspace: &TestWorkspace,
    session_id: &str,
    system_prompt: &str,
    prompt: &str,
) -> Command {
    let mut command = base_command(workspace);
    command
        .arg("--backend")
        .arg("google")
        .arg("--model")
        .arg(live_model())
        .arg("--temperature")
        .arg("0")
        .arg("--session-id")
        .arg(session_id)
        .arg("--system-prompt")
        .arg(system_prompt);
    if let Some(path) = explicit_live_credential() {
        command.arg("--credential-file").arg(path);
    } else {
        assert!(
            standard_adc_is_available(),
            "live E2E requires ORCHESTRAL_E2E_CREDENTIAL_FILE, \
             GOOGLE_APPLICATION_CREDENTIALS, repository credential.json, or standard ADC"
        );
    }
    command.arg(prompt);
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
            "Use exactly one shell tool call to run the allowed cp program with the argument ",
            "vector [source.txt, approved.txt]. Do not call other tools and do not retry a ",
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
    let mut approval_sent = false;
    let mut exited = None;
    while started.elapsed() < timeout {
        if let Ok(bytes) = receiver.recv_timeout(Duration::from_millis(100)) {
            let text = String::from_utf8_lossy(&bytes);
            if !approval_sent && text.contains(APPROVAL_PROMPT) {
                if let Some(target) = target {
                    assert!(!target.exists(), "effect happened before exact approval");
                }
                stdin
                    .write_all(if allow { b"y\n" } else { b"n\n" })
                    .expect("answer approval prompt");
                stdin.flush().expect("flush approval answer");
                approval_sent = true;
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
        approval_sent,
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
                .set_read_timeout(Some(Duration::from_secs(5)))
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
    let header_end = loop {
        let count = stream.read(&mut buffer).expect("read HTTP fixture request");
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
        let count = stream
            .read(&mut buffer)
            .expect("read HTTP fixture request body");
        assert!(count > 0, "HTTP fixture request body ended early");
        bytes.extend_from_slice(&buffer[..count]);
    }
    CapturedHttpRequest {
        headers,
        body: serde_json::from_slice(&bytes[header_end..header_end + content_length])
            .expect("HTTP fixture request body is JSON"),
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

fn model_request_text(request: &Value) -> String {
    request["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|message| message["content"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

fn skill_digest_from_model_request(request: &Value, name: &str) -> String {
    let context = model_request_text(request);
    let descriptor = context
        .lines()
        .find(|line| line.contains(&format!("- name={name} ")))
        .unwrap_or_else(|| panic!("model request omitted Skill descriptor '{name}': {context}"));
    let digest = descriptor
        .split_once(" digest=")
        .and_then(|(_, rest)| rest.split_whitespace().next())
        .unwrap_or_else(|| panic!("Skill descriptor omitted its digest: {descriptor}"));
    assert_eq!(digest.len(), 64, "Skill digest is not SHA-256");
    assert!(
        digest
            .chars()
            .all(|character| character.is_ascii_hexdigit()),
        "Skill digest is not hexadecimal"
    );
    digest.to_owned()
}

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("CLI crate must live under apps/")
        .to_path_buf()
}
