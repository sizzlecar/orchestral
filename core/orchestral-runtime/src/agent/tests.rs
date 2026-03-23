use super::types::{AgentCaptureMode, AgentMode, AgentOutputCandidate, AgentOutputRule};
use super::*;
use std::collections::{HashSet, VecDeque};
use std::sync::Mutex;

use orchestral_core::action::{Action, ActionMeta};
use orchestral_core::store::WorkingSet;
use orchestral_core::types::{Step, TaskId};

struct SequenceLlmClient {
    responses: Mutex<VecDeque<String>>,
}

#[async_trait]
impl LlmClient for SequenceLlmClient {
    async fn complete(&self, _request: LlmRequest) -> Result<String, crate::planner::LlmError> {
        let mut locked = self.responses.lock().expect("lock");
        Ok(locked.pop_front().unwrap_or_else(|| "{}".to_string()))
    }
}

struct ToolCallLlmClient {
    responses: Mutex<VecDeque<LlmResponse>>,
}

#[async_trait]
impl LlmClient for ToolCallLlmClient {
    async fn complete(&self, _request: LlmRequest) -> Result<String, crate::planner::LlmError> {
        Ok("{}".to_string())
    }

    async fn complete_with_tools(
        &self,
        _request: LlmRequest,
        _tools: &[ToolDefinition],
    ) -> Result<LlmResponse, crate::planner::LlmError> {
        let mut locked = self.responses.lock().expect("lock");
        Ok(locked
            .pop_front()
            .unwrap_or_else(|| LlmResponse::Text("{}".to_string())))
    }
}

struct EchoAction;

#[async_trait]
impl Action for EchoAction {
    fn name(&self) -> &str {
        "echo"
    }

    fn description(&self) -> &str {
        "echo"
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new("echo", "echo")
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let message = input
            .params
            .get("message")
            .cloned()
            .unwrap_or(Value::String(String::new()));
        let mut exports = HashMap::new();
        exports.insert("observation".to_string(), message.clone());
        exports.insert("result".to_string(), message);
        ActionResult::success_with(exports)
    }
}

struct JsonStdoutAction;

#[async_trait]
impl Action for JsonStdoutAction {
    fn name(&self) -> &str {
        "json_stdout"
    }

    fn description(&self) -> &str {
        "json_stdout"
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new("json_stdout", "json_stdout")
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let payload = input
            .params
            .get("payload")
            .cloned()
            .unwrap_or(Value::Object(Default::default()));
        let stdout =
            serde_json::to_string(&payload).expect("payload should serialize to JSON string");
        let mut exports = HashMap::new();
        exports.insert("stdout".to_string(), Value::String(stdout));
        exports.insert("stderr".to_string(), Value::String(String::new()));
        exports.insert("status".to_string(), Value::Number(0.into()));
        ActionResult::success_with(exports)
    }
}

fn test_ctx() -> ExecutorContext {
    ExecutorContext::new(
        TaskId::from("task-1"),
        Arc::new(RwLock::new(WorkingSet::new())),
    )
}

#[tokio::test]
async fn test_agent_executor_runs_action_then_returns_final() {
    let client = SequenceLlmClient {
        responses: Mutex::new(VecDeque::from(vec![
            r#"{"type":"action","action":"echo","params":{"message":"ok"}}"#.to_string(),
            r#"{"type":"final","exports":{"summary":"done"}}"#.to_string(),
        ])),
    };
    let executor = LlmAgentExecutor::new(client, LlmAgentExecutorConfig::default());

    let step = Step::agent("s1").with_params(serde_json::json!({
        "goal":"inspect",
        "allowed_actions":["echo"],
        "max_iterations":3,
        "output_keys":["summary"]
    }));
    let mut registry = ActionRegistry::new();
    registry.register(Arc::new(EchoAction));
    let result = executor
        .execute_agent_step(
            &step,
            step.params.clone(),
            "exec-1",
            &test_ctx(),
            Arc::new(RwLock::new(registry)),
        )
        .await;

    match result {
        ActionResult::Success { exports } => {
            assert_eq!(
                exports.get("summary"),
                Some(&Value::String("ok".to_string()))
            );
        }
        other => panic!("unexpected result: {other:?}"),
    }
}

#[tokio::test]
async fn test_agent_executor_returns_error_when_final_has_no_materialized_evidence() {
    let client = SequenceLlmClient {
        responses: Mutex::new(VecDeque::from(vec![
            r#"{"type":"final","exports":{"summary":"x"}}"#.to_string(),
        ])),
    };
    let executor = LlmAgentExecutor::new(client, LlmAgentExecutorConfig::default());
    let step = Step::agent("s1").with_params(serde_json::json!({
        "goal":"inspect",
        "allowed_actions":["echo"],
        "max_iterations":1,
        "output_keys":["summary"]
    }));
    let result = executor
        .execute_agent_step(
            &step,
            step.params.clone(),
            "exec-1",
            &test_ctx(),
            Arc::new(RwLock::new(ActionRegistry::new())),
        )
        .await;

    assert!(matches!(result, ActionResult::Error { .. }));
}

#[test]
fn test_parse_agent_params_retains_bound_inputs() {
    let params = serde_json::json!({
        "goal": "inspect",
        "allowed_actions": ["echo"],
        "max_iterations": 3,
        "output_keys": ["summary"],
        "input_candidates": "docs/sample.dat",
        "skill_path": ".claude/skills/tabular/SKILL.md"
    });

    let parsed = parse_agent_params("s1", &params).expect("parse agent params");
    assert_eq!(
        parsed.bound_inputs.get("input_candidates"),
        Some(&Value::String("docs/sample.dat".to_string()))
    );
    assert_eq!(
        parsed.bound_inputs.get("skill_path"),
        Some(&Value::String(
            ".claude/skills/tabular/SKILL.md".to_string()
        ))
    );

    let prompt = build_agent_system_prompt(&parsed, &test_ctx());
    assert!(prompt.contains("Bound upstream inputs (from io_bindings):"));
    assert!(prompt.contains("input_candidates"));
    assert!(prompt.contains("skill_path"));
}

#[test]
fn test_parse_agent_params_parses_output_rules() {
    let params = serde_json::json!({
        "goal": "inspect",
        "allowed_actions": ["echo"],
        "max_iterations": 3,
        "output_keys": ["updated_file_path", "summary"],
        "output_rules": {
            "updated_file_path": {
                "requires": { "action": "file_write" },
                "candidates": [
                    { "slot": "fill_result", "path": "updated_file_path", "requires_action": "file_write" }
                ]
            },
            "summary": {
                "candidates": [
                    { "slot": "fill_result", "path": "summary" }
                ],
                "fallback_aliases": ["result"]
            }
        }
    });

    let parsed = parse_agent_params("s1", &params).expect("parse agent params");
    assert_eq!(
        parsed
            .output_rules
            .get("updated_file_path")
            .and_then(|v| v.candidates.first())
            .map(|v| v.slot.as_str()),
        Some("fill_result")
    );
    assert_eq!(
        parsed
            .output_rules
            .get("updated_file_path")
            .and_then(|v| v.required_action.as_deref()),
        Some("file_write")
    );
    assert_eq!(
        parsed
            .output_rules
            .get("updated_file_path")
            .and_then(|v| v.candidates.first())
            .and_then(|v| v.required_action.as_deref()),
        Some("file_write")
    );
    assert_eq!(
        parsed
            .output_rules
            .get("summary")
            .map(|v| v.fallback_aliases.clone()),
        Some(vec!["result".to_string()])
    );
}

#[test]
fn test_parse_leaf_agent_params_and_prompt() {
    let params = serde_json::json!({
        "mode": "leaf",
        "goal": "derive a structured patch",
        "allowed_actions": ["json_stdout"],
        "max_iterations": 1,
        "result_slot": "leaf_result",
        "output_keys": ["change_spec"]
    });

    let parsed = parse_agent_params("s1", &params).expect("parse agent params");
    assert_eq!(parsed.mode, AgentMode::Leaf);
    assert_eq!(parsed.result_slot.as_deref(), Some("leaf_result"));

    let prompt = build_agent_system_prompt(&parsed, &test_ctx());
    assert!(prompt.contains("Mode: leaf"));
    assert!(prompt.contains("Leaf Mode:"));
    assert!(prompt.contains("The only allowed action is json_stdout."));
    assert!(prompt.contains("Empty objects, missing required keys"));
    assert!(prompt.contains("do not return an empty list unless"));
    assert!(prompt.contains("save_as=leaf_result"));
}

#[test]
fn test_leaf_execute_action_schema_requires_payload_and_result_slot() {
    let params = AgentStepParams {
        mode: AgentMode::Leaf,
        goal: "derive".to_string(),
        allowed_actions: HashSet::from(["json_stdout".to_string()]),
        max_iterations: 1,
        output_keys: vec!["fills".to_string(), "file_path".to_string()],
        output_rules: HashMap::new(),
        result_slot: Some("leaf_result".to_string()),
        bound_inputs: HashMap::new(),
    };
    let schema = leaf_execute_action_schema(&params);
    assert_eq!(
        schema["required"],
        serde_json::json!(["action", "params", "save_as", "capture"])
    );
    assert_eq!(
        schema["properties"]["save_as"]["enum"],
        serde_json::json!(["leaf_result"])
    );
    assert_eq!(
        schema["properties"]["params"]["properties"]["payload"]["required"],
        serde_json::json!(["fills", "file_path"])
    );
}

#[test]
fn test_parse_agent_params_supports_legacy_output_sources() {
    let params = serde_json::json!({
        "goal": "inspect",
        "allowed_actions": ["echo"],
        "max_iterations": 3,
        "output_keys": ["updated_file_path"],
        "output_sources": {
            "updated_file_path": { "slot": "fill_result", "field": "updated_file_path" }
        }
    });

    let parsed = parse_agent_params("s1", &params).expect("parse agent params");
    let candidate = parsed
        .output_rules
        .get("updated_file_path")
        .and_then(|rule| rule.candidates.first())
        .expect("candidate should be parsed");
    assert_eq!(candidate.slot, "fill_result");
    assert_eq!(render_path_segments(&candidate.path), "updated_file_path");
}

#[test]
fn test_agent_prompt_includes_execution_environment() {
    use orchestral_core::planner::PlannerRuntimeInfo;

    let params = parse_agent_params(
        "s1",
        &serde_json::json!({
            "goal": "do stuff",
            "allowed_actions": ["shell"],
            "max_iterations": 3,
            "output_keys": ["result"]
        }),
    )
    .unwrap();

    let ctx = ExecutorContext::new(
        TaskId::from("task-1"),
        Arc::new(RwLock::new(WorkingSet::new())),
    )
    .with_runtime_info(PlannerRuntimeInfo {
        os: "macos".to_string(),
        os_family: "unix".to_string(),
        arch: "aarch64".to_string(),
        shell: Some("/bin/zsh".to_string()),
    });

    let prompt = build_agent_system_prompt(&params, &ctx);
    assert!(prompt.contains("Execution Environment:"));
    assert!(prompt.contains("os: macos"));
    assert!(prompt.contains("arch: aarch64"));
    assert!(prompt.contains("shell: /bin/zsh"));
}

#[test]
fn test_agent_prompt_includes_skill_knowledge() {
    let params = parse_agent_params(
        "s1",
        &serde_json::json!({
            "goal": "update artifact",
            "allowed_actions": ["shell", "file_read"],
            "max_iterations": 5,
            "output_keys": ["path"]
        }),
    )
    .unwrap();

    let ctx = ExecutorContext::new(
        TaskId::from("task-1"),
        Arc::new(RwLock::new(WorkingSet::new())),
    )
    .with_skill_instructions(vec![SkillInstruction {
        skill_name: "tabular".to_string(),
        instructions: "data processing and formatting".to_string(),
        skill_path: Some("/skills/tabular/SKILL.md".to_string()),
        scripts_dir: Some("/skills/tabular/scripts".to_string()),
        venv_python: None,
    }]);

    let prompt = build_agent_system_prompt(&params, &ctx);
    assert!(prompt.contains("Activated Skills:"));
    assert!(prompt.contains("- tabular"));
    assert!(prompt.contains("data processing and formatting"));
    assert!(prompt.contains("[skill file: /skills/tabular/SKILL.md]"));
    assert!(prompt.contains("[scripts: /skills/tabular/scripts]"));
}

#[test]
fn test_agent_prompt_adds_spreadsheet_binary_rules() {
    let params = parse_agent_params(
        "s1",
        &serde_json::json!({
            "goal": "update workbook",
            "allowed_actions": ["shell", "file_read"],
            "max_iterations": 5,
            "output_keys": ["summary"]
        }),
    )
    .unwrap();

    let ctx = ExecutorContext::new(
        TaskId::from("task-1"),
        Arc::new(RwLock::new(WorkingSet::new())),
    )
    .with_skill_instructions(vec![SkillInstruction {
        skill_name: "xlsx".to_string(),
        instructions:
            "summary: spreadsheet workbook editing\nkeywords: excel, spreadsheet, workbook"
                .to_string(),
        skill_path: Some("/skills/xlsx/SKILL.md".to_string()),
        scripts_dir: Some("/skills/xlsx/scripts".to_string()),
        venv_python: Some("/skills/xlsx/.venv/bin/python3".to_string()),
    }]);

    let prompt = build_agent_system_prompt(&params, &ctx);
    assert!(prompt.contains("Skill-Specific Rules:"));
    assert!(prompt.contains("never call file_read on .xlsx"));
    assert!(prompt.contains("inspect or modify the workbook with shell"));
    assert!(prompt.contains("avoid recalc.py unless formulas were added/changed"));
}

#[test]
fn test_summarize_bound_input_value_compacts_multiline_skill_text() {
    let skill_text = (1..=20)
        .map(|i| format!("line-{i}"))
        .collect::<Vec<_>>()
        .join("\n");
    let summarized = summarize_bound_input_value("skill_instructions", &Value::String(skill_text));
    assert!(summarized.contains("line-1\nline-2\nline-3"));
    assert!(summarized.contains('\n'));
}

#[test]
fn test_build_bound_inputs_block_omits_skill_doc_in_leaf_mode_when_other_inputs_exist() {
    let mut bound_inputs = HashMap::new();
    bound_inputs.insert(
        "skill_doc".to_string(),
        Value::String("line-1\nline-2".to_string()),
    );
    bound_inputs.insert(
        "input_file".to_string(),
        Value::String("docs/a.dat".to_string()),
    );
    let params = AgentStepParams {
        mode: AgentMode::Leaf,
        goal: "derive".to_string(),
        allowed_actions: HashSet::from(["json_stdout".to_string()]),
        max_iterations: 1,
        output_keys: vec!["fills".to_string()],
        output_rules: HashMap::new(),
        result_slot: Some("leaf_result".to_string()),
        bound_inputs,
    };
    let block = build_bound_inputs_block(&params);
    assert!(!block.contains("- skill_doc:"));
    assert!(block.contains("- input_file = docs/a.dat"));
    assert!(block.contains("skill_* bound inputs omitted in leaf mode"));
}

#[test]
fn test_parse_tool_call_execute_action() {
    let args = serde_json::json!({
        "action": "shell",
        "params": {"command": "ls", "args": ["-la"]},
        "save_as": "listing",
        "capture": "json_stdout"
    });
    let decision = parse_tool_call_decision("execute_action", args).unwrap();
    match decision {
        AgentDecision::Action {
            name,
            params,
            save_as,
            capture,
        } => {
            assert_eq!(name, "shell");
            assert_eq!(params.get("command").unwrap(), "ls");
            assert_eq!(save_as.as_deref(), Some("listing"));
            assert_eq!(capture, Some(AgentCaptureMode::JsonStdout));
        }
        other => panic!("expected Action, got {:?}", other),
    }
}

#[test]
fn test_parse_tool_call_finish() {
    let args = serde_json::json!({});
    let decision = parse_tool_call_decision("finish", args).unwrap();
    assert!(matches!(decision, AgentDecision::Finish));
}

#[test]
fn test_parse_tool_call_return_final() {
    let args = serde_json::json!({
        "exports": {"path": "/tmp/output.dat", "summary": "done"}
    });
    let decision = parse_tool_call_decision("return_final", args).unwrap();
    match decision {
        AgentDecision::Final { exports } => {
            assert_eq!(
                exports.get("path"),
                Some(&Value::String("/tmp/output.dat".to_string()))
            );
            assert_eq!(
                exports.get("summary"),
                Some(&Value::String("done".to_string()))
            );
        }
        other => panic!("expected Final, got {:?}", other),
    }
}

#[test]
fn test_parse_tool_call_unknown_tool() {
    let args = serde_json::json!({"foo": "bar"});
    let result = parse_tool_call_decision("unknown_tool", args);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("unknown tool"));
}

#[test]
fn test_collect_agent_debug_text_payloads_finds_multiline_script() {
    let payload = serde_json::json!({
        "action": "json_stdout",
        "params": {
            "payload": {
                "script": "import openpyxl\nprint('fill')\nprint('done')",
                "note": "short"
            }
        }
    });
    let matches = collect_agent_debug_text_payloads(&payload);
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].path, "$.params.payload.script");
    assert!(matches[0].text.contains("import openpyxl"));
}

#[test]
fn test_action_preflight_allows_shell_expression_command_with_existing_binary_token() {
    let python = std::env::current_exe().expect("current exe path");
    let params = serde_json::json!({
        "command": format!("{} -c \"print('ok')\"", python.display())
    });
    let error = action_preflight_error("shell", &params);
    assert!(error.is_none(), "unexpected preflight error: {:?}", error);
}

#[test]
fn test_action_preflight_resolves_python_script_relative_to_workdir() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    let temp_root = std::env::temp_dir().join(format!(
        "orchestral-agent-preflight-{}-{}",
        std::process::id(),
        nonce
    ));
    let script_dir = temp_root.join("scripts");
    std::fs::create_dir_all(&script_dir).expect("create script dir");
    let script_path = script_dir.join("task.py");
    std::fs::write(&script_path, "print('ok')").expect("write script");

    let params = serde_json::json!({
        "command": "python3",
        "args": ["scripts/task.py"],
        "workdir": temp_root.to_string_lossy().to_string()
    });
    let error = action_preflight_error("shell", &params);

    let _ = std::fs::remove_dir_all(&temp_root);
    assert!(error.is_none(), "unexpected preflight error: {:?}", error);
}

#[tokio::test]
async fn test_agent_executor_with_tool_call_client() {
    let client = ToolCallLlmClient {
        responses: Mutex::new(VecDeque::from(vec![
            LlmResponse::ToolCall {
                id: "call-1".to_string(),
                name: "execute_action".to_string(),
                arguments: serde_json::json!({
                    "action": "echo",
                    "params": {"message": "completed via tool_use"}
                }),
            },
            LlmResponse::ToolCall {
                id: "call-2".to_string(),
                name: "return_final".to_string(),
                arguments: serde_json::json!({
                    "exports": {"summary": "completed via tool_use"}
                }),
            },
        ])),
    };
    let executor = LlmAgentExecutor::new(client, LlmAgentExecutorConfig::default());

    let step = Step::agent("s1").with_params(serde_json::json!({
        "goal": "test tool_use",
        "allowed_actions": ["echo"],
        "max_iterations": 3,
        "output_keys": ["summary"]
    }));
    let mut registry = ActionRegistry::new();
    registry.register(Arc::new(EchoAction));
    let result = executor
        .execute_agent_step(
            &step,
            step.params.clone(),
            "exec-1",
            &test_ctx(),
            Arc::new(RwLock::new(registry)),
        )
        .await;

    match result {
        ActionResult::Success { exports } => {
            assert_eq!(
                exports.get("summary"),
                Some(&Value::String("completed via tool_use".to_string()))
            );
        }
        other => panic!("unexpected result: {other:?}"),
    }
}

#[tokio::test]
async fn test_agent_executor_forces_finalization_after_last_action_iteration() {
    let client = ToolCallLlmClient {
        responses: Mutex::new(VecDeque::from(vec![
            LlmResponse::ToolCall {
                id: "call-1".to_string(),
                name: "execute_action".to_string(),
                arguments: serde_json::json!({
                    "action": "echo",
                    "params": {"message": "completed after last action"}
                }),
            },
            LlmResponse::ToolCall {
                id: "call-2".to_string(),
                name: "return_final".to_string(),
                arguments: serde_json::json!({
                    "exports": {"summary": "completed after last action"}
                }),
            },
        ])),
    };
    let executor = LlmAgentExecutor::new(client, LlmAgentExecutorConfig::default());

    let step = Step::agent("s1").with_params(serde_json::json!({
        "goal": "test forced finalization",
        "allowed_actions": ["echo"],
        "max_iterations": 1,
        "output_keys": ["summary"]
    }));
    let mut registry = ActionRegistry::new();
    registry.register(Arc::new(EchoAction));
    let result = executor
        .execute_agent_step(
            &step,
            step.params.clone(),
            "exec-1",
            &test_ctx(),
            Arc::new(RwLock::new(registry)),
        )
        .await;

    match result {
        ActionResult::Success { exports } => {
            assert_eq!(
                exports.get("summary"),
                Some(&Value::String("completed after last action".to_string()))
            );
        }
        other => panic!("unexpected result: {other:?}"),
    }
}

#[tokio::test]
async fn test_agent_executor_fast_fails_when_shell_python_script_is_missing() {
    let client = ToolCallLlmClient {
        responses: Mutex::new(VecDeque::from(vec![LlmResponse::ToolCall {
            id: "call-1".to_string(),
            name: "execute_action".to_string(),
            arguments: serde_json::json!({
                "action": "shell",
                "params": {
                    "command": ".venv/bin/python3",
                    "args": [".claude/skills/xlsx/scripts/read_xlsx.py", "docs/a.xlsx"]
                }
            }),
        }])),
    };
    let executor = LlmAgentExecutor::new(client, LlmAgentExecutorConfig::default());

    let step = Step::agent("s1").with_params(serde_json::json!({
        "goal":"inspect",
        "allowed_actions":["shell"],
        "max_iterations":8,
        "output_keys":["summary"]
    }));
    let result = executor
        .execute_agent_step(
            &step,
            step.params.clone(),
            "exec-1",
            &test_ctx(),
            Arc::new(RwLock::new(ActionRegistry::new())),
        )
        .await;

    match result {
        ActionResult::Error { message } => {
            assert!(message.contains("failed preflight"));
            assert!(message.contains("does not exist"));
        }
        other => panic!("expected Error, got {:?}", other),
    }
}

#[tokio::test]
async fn test_agent_executor_materializes_outputs_from_saved_slot() {
    let client = ToolCallLlmClient {
        responses: Mutex::new(VecDeque::from(vec![LlmResponse::ToolCall {
            id: "call-1".to_string(),
            name: "execute_action".to_string(),
            arguments: serde_json::json!({
                "action": "json_stdout",
                "params": {
                    "payload": {
                        "updated_file_path": "/tmp/mock.xlsx",
                        "summary": "filled workbook"
                    }
                },
                "save_as": "fill_result",
                "capture": "json_stdout"
            }),
        }])),
    };
    let executor = LlmAgentExecutor::new(client, LlmAgentExecutorConfig::default());

    let step = Step::agent("s1").with_params(serde_json::json!({
        "goal": "fill workbook",
        "allowed_actions": ["json_stdout"],
        "max_iterations": 1,
        "output_keys": ["updated_file_path", "summary"],
        "output_rules": {
            "updated_file_path": {
                "candidates": [
                    { "slot": "fill_result", "path": "updated_file_path" }
                ]
            },
            "summary": {
                "candidates": [
                    { "slot": "fill_result", "path": "summary" }
                ]
            }
        }
    }));
    let mut registry = ActionRegistry::new();
    registry.register(Arc::new(JsonStdoutAction));
    let result = executor
        .execute_agent_step(
            &step,
            step.params.clone(),
            "exec-1",
            &test_ctx(),
            Arc::new(RwLock::new(registry)),
        )
        .await;

    match result {
        ActionResult::Success { exports } => {
            assert_eq!(
                exports.get("updated_file_path"),
                Some(&Value::String("/tmp/mock.xlsx".to_string()))
            );
            assert_eq!(
                exports.get("summary"),
                Some(&Value::String("filled workbook".to_string()))
            );
        }
        other => panic!("unexpected result: {other:?}"),
    }
}

#[tokio::test]
async fn test_agent_executor_wraps_json_stdout_payload_for_leaf_outputs() {
    let client = ToolCallLlmClient {
        responses: Mutex::new(VecDeque::from(vec![LlmResponse::ToolCall {
            id: "call-1".to_string(),
            name: "execute_action".to_string(),
            arguments: serde_json::json!({
                "action": "json_stdout",
                "params": {
                    "updated_file_path": "/tmp/mock.xlsx",
                    "summary": "filled workbook"
                },
                "save_as": "fill_result",
                "capture": "json_stdout"
            }),
        }])),
    };
    let executor = LlmAgentExecutor::new(client, LlmAgentExecutorConfig::default());

    let step = Step::agent("s1").with_params(serde_json::json!({
        "goal": "fill workbook",
        "allowed_actions": ["json_stdout"],
        "max_iterations": 1,
        "output_keys": ["updated_file_path", "summary"],
        "output_rules": {
            "updated_file_path": {
                "candidates": [
                    { "slot": "fill_result", "path": "updated_file_path" }
                ]
            },
            "summary": {
                "candidates": [
                    { "slot": "fill_result", "path": "summary" }
                ]
            }
        }
    }));
    let mut registry = ActionRegistry::new();
    registry.register(Arc::new(JsonStdoutAction));
    let result = executor
        .execute_agent_step(
            &step,
            step.params.clone(),
            "exec-1",
            &test_ctx(),
            Arc::new(RwLock::new(registry)),
        )
        .await;

    match result {
        ActionResult::Success { exports } => {
            assert_eq!(
                exports.get("updated_file_path"),
                Some(&Value::String("/tmp/mock.xlsx".to_string()))
            );
            assert_eq!(
                exports.get("summary"),
                Some(&Value::String("filled workbook".to_string()))
            );
        }
        other => panic!("unexpected result: {other:?}"),
    }
}

#[test]
fn test_materialize_output_rules_do_not_fallback_to_generic_aliases() {
    let params = AgentStepParams {
        mode: AgentMode::Explore,
        goal: "fill workbook".to_string(),
        allowed_actions: HashSet::new(),
        max_iterations: 1,
        output_keys: vec!["updated_file_path".to_string()],
        output_rules: HashMap::from([(
            "updated_file_path".to_string(),
            AgentOutputRule {
                candidates: vec![AgentOutputCandidate {
                    slot: "fill_result".to_string(),
                    path: parse_path_segments("updated_file_path").expect("valid path"),
                    required_action: None,
                }],
                template: None,
                fallback_aliases: vec![],
                required_action: None,
            },
        )]),
        result_slot: None,
        bound_inputs: HashMap::new(),
    };

    let mut evidence = AgentEvidenceStore::default();
    let mut raw_exports = HashMap::new();
    raw_exports.insert(
        "path".to_string(),
        Value::String("/tmp/wrong.md".to_string()),
    );
    evidence.record_success(
        "file_read",
        raw_exports,
        serde_json::json!({"path": "/tmp/wrong.md"}),
        Some("xlsx_skill"),
    );

    let result = materialize_final_exports(&params, &evidence);
    let err = result.expect_err("expected materialize error");
    assert_eq!(err.key, "updated_file_path");
    assert_eq!(err.reason_code, "rule_unresolved");
}

#[test]
fn test_validate_agent_action_success_rejects_empty_leaf_payload() {
    let params = AgentStepParams {
        mode: AgentMode::Leaf,
        goal: "fill workbook".to_string(),
        allowed_actions: HashSet::from(["json_stdout".to_string()]),
        max_iterations: 1,
        output_keys: vec!["fills".to_string(), "file_path".to_string()],
        output_rules: HashMap::from([
            (
                "fills".to_string(),
                AgentOutputRule {
                    candidates: vec![AgentOutputCandidate {
                        slot: "leaf_result".to_string(),
                        path: parse_path_segments("fills").expect("valid path"),
                        required_action: Some("json_stdout".to_string()),
                    }],
                    template: None,
                    fallback_aliases: vec![],
                    required_action: Some("json_stdout".to_string()),
                },
            ),
            (
                "file_path".to_string(),
                AgentOutputRule {
                    candidates: vec![AgentOutputCandidate {
                        slot: "leaf_result".to_string(),
                        path: parse_path_segments("file_path").expect("valid path"),
                        required_action: Some("json_stdout".to_string()),
                    }],
                    template: None,
                    fallback_aliases: vec![],
                    required_action: Some("json_stdout".to_string()),
                },
            ),
        ]),
        result_slot: Some("leaf_result".to_string()),
        bound_inputs: HashMap::new(),
    };
    let mut exports = HashMap::new();
    exports.insert("stdout".to_string(), serde_json::json!("{}"));
    exports.insert("stderr".to_string(), serde_json::json!(""));
    exports.insert("status".to_string(), serde_json::json!(0));
    let err = validate_agent_action_success(
        &params,
        &AgentEvidenceStore::default(),
        "json_stdout",
        &exports,
        &serde_json::json!({}),
        Some("leaf_result"),
    )
    .expect_err("empty payload should be rejected");
    assert_eq!(err.key, "fills");
    assert_eq!(err.reason_code, "rule_unresolved");
}

#[test]
fn test_materialize_output_rules_allow_explicit_fallback_aliases() {
    let params = AgentStepParams {
        mode: AgentMode::Explore,
        goal: "fill workbook".to_string(),
        allowed_actions: HashSet::new(),
        max_iterations: 1,
        output_keys: vec!["updated_file_path".to_string()],
        output_rules: HashMap::from([(
            "updated_file_path".to_string(),
            AgentOutputRule {
                candidates: vec![AgentOutputCandidate {
                    slot: "fill_result".to_string(),
                    path: parse_path_segments("updated_file_path").expect("valid path"),
                    required_action: None,
                }],
                template: None,
                fallback_aliases: vec!["path".to_string()],
                required_action: None,
            },
        )]),
        result_slot: None,
        bound_inputs: HashMap::new(),
    };

    let mut evidence = AgentEvidenceStore::default();
    let mut raw_exports = HashMap::new();
    raw_exports.insert(
        "path".to_string(),
        Value::String("/tmp/fallback.md".to_string()),
    );
    evidence.record_success(
        "file_read",
        raw_exports,
        serde_json::json!({"path": "/tmp/fallback.md"}),
        Some("xlsx_skill"),
    );

    let result = materialize_final_exports(&params, &evidence).expect("materialized");
    assert_eq!(
        result.get("updated_file_path"),
        Some(&Value::String("/tmp/fallback.md".to_string()))
    );
}

#[test]
fn test_materialize_output_rules_reject_spoofed_action_named_slot() {
    let params = AgentStepParams {
        mode: AgentMode::Explore,
        goal: "fill workbook".to_string(),
        allowed_actions: HashSet::from([
            "shell".to_string(),
            "file_write".to_string(),
            "file_read".to_string(),
        ]),
        max_iterations: 1,
        output_keys: vec!["updated_file_path".to_string()],
        output_rules: HashMap::from([(
            "updated_file_path".to_string(),
            AgentOutputRule {
                candidates: vec![AgentOutputCandidate {
                    slot: "file_write".to_string(),
                    path: parse_path_segments("path").expect("valid path"),
                    required_action: None,
                }],
                template: None,
                fallback_aliases: vec![],
                required_action: None,
            },
        )]),
        result_slot: None,
        bound_inputs: HashMap::new(),
    };

    let mut evidence = AgentEvidenceStore::default();
    evidence.record_success(
        "shell",
        HashMap::new(),
        serde_json::json!({"path": "/tmp/spoofed.xlsx"}),
        Some("file_write"),
    );

    let result = materialize_final_exports(&params, &evidence);
    let err = result.expect_err("expected materialize error");
    assert_eq!(err.key, "updated_file_path");
    assert_eq!(err.reason_code, "slot_provenance_mismatch");
}

#[test]
fn test_materialize_output_rules_accept_requires_action_evidence() {
    let params = AgentStepParams {
        mode: AgentMode::Explore,
        goal: "fill workbook".to_string(),
        allowed_actions: HashSet::from(["file_write".to_string(), "shell".to_string()]),
        max_iterations: 1,
        output_keys: vec!["updated_file_path".to_string()],
        output_rules: HashMap::from([(
            "updated_file_path".to_string(),
            AgentOutputRule {
                candidates: vec![AgentOutputCandidate {
                    slot: "artifact_slot".to_string(),
                    path: parse_path_segments("path").expect("valid path"),
                    required_action: Some("file_write".to_string()),
                }],
                template: None,
                fallback_aliases: vec![],
                required_action: None,
            },
        )]),
        result_slot: None,
        bound_inputs: HashMap::new(),
    };

    let mut evidence = AgentEvidenceStore::default();
    evidence.record_success(
        "shell",
        HashMap::new(),
        serde_json::json!({"path": "/tmp/wrong.xlsx"}),
        Some("artifact_slot"),
    );
    evidence.record_success(
        "file_write",
        HashMap::new(),
        serde_json::json!({"path": "/tmp/right.xlsx"}),
        Some("artifact_slot"),
    );

    let result = materialize_final_exports(&params, &evidence).expect("materialized");
    assert_eq!(
        result.get("updated_file_path"),
        Some(&Value::String("/tmp/right.xlsx".to_string()))
    );
}

#[tokio::test]
async fn test_agent_executor_materializes_outputs_from_stdout_json_without_capture() {
    let client = ToolCallLlmClient {
        responses: Mutex::new(VecDeque::from(vec![
            LlmResponse::ToolCall {
                id: "call-1".to_string(),
                name: "execute_action".to_string(),
                arguments: serde_json::json!({
                    "action": "json_stdout",
                    "params": {
                        "payload": {
                            "updated_file_path": "/tmp/auto-mock.xlsx",
                            "summary": "auto extracted from stdout json"
                        }
                    }
                }),
            },
            LlmResponse::ToolCall {
                id: "call-2".to_string(),
                name: "finish".to_string(),
                arguments: serde_json::json!({}),
            },
        ])),
    };
    let executor = LlmAgentExecutor::new(client, LlmAgentExecutorConfig::default());

    let step = Step::agent("s1").with_params(serde_json::json!({
        "goal": "fill workbook",
        "allowed_actions": ["json_stdout"],
        "max_iterations": 2,
        "output_keys": ["updated_file_path", "summary"]
    }));
    let mut registry = ActionRegistry::new();
    registry.register(Arc::new(JsonStdoutAction));
    let result = executor
        .execute_agent_step(
            &step,
            step.params.clone(),
            "exec-1",
            &test_ctx(),
            Arc::new(RwLock::new(registry)),
        )
        .await;

    match result {
        ActionResult::Success { exports } => {
            assert_eq!(
                exports.get("updated_file_path"),
                Some(&Value::String("/tmp/auto-mock.xlsx".to_string()))
            );
            assert_eq!(
                exports.get("summary"),
                Some(&Value::String(
                    "auto extracted from stdout json".to_string()
                ))
            );
        }
        other => panic!("unexpected result: {other:?}"),
    }
}

#[tokio::test]
async fn test_agent_executor_materializes_outputs_from_auto_stdout_slot_via_rules() {
    let client = ToolCallLlmClient {
        responses: Mutex::new(VecDeque::from(vec![
            LlmResponse::ToolCall {
                id: "call-1".to_string(),
                name: "execute_action".to_string(),
                arguments: serde_json::json!({
                    "action": "json_stdout",
                    "params": {
                        "payload": {
                            "result": {
                                "path": "/tmp/rule-mock.xlsx"
                            }
                        }
                    }
                }),
            },
            LlmResponse::ToolCall {
                id: "call-2".to_string(),
                name: "finish".to_string(),
                arguments: serde_json::json!({}),
            },
        ])),
    };
    let executor = LlmAgentExecutor::new(client, LlmAgentExecutorConfig::default());

    let step = Step::agent("s1").with_params(serde_json::json!({
        "goal": "fill workbook",
        "allowed_actions": ["json_stdout"],
        "max_iterations": 2,
        "output_keys": ["updated_file_path", "summary"],
        "output_rules": {
            "updated_file_path": {
                "candidates": [
                    { "slot": "action:json_stdout:stdout_json", "path": "result.path" }
                ]
            },
            "summary": {
                "template": "runtime extracted {{updated_file_path}}"
            }
        }
    }));
    let mut registry = ActionRegistry::new();
    registry.register(Arc::new(JsonStdoutAction));
    let result = executor
        .execute_agent_step(
            &step,
            step.params.clone(),
            "exec-1",
            &test_ctx(),
            Arc::new(RwLock::new(registry)),
        )
        .await;

    match result {
        ActionResult::Success { exports } => {
            assert_eq!(
                exports.get("updated_file_path"),
                Some(&Value::String("/tmp/rule-mock.xlsx".to_string()))
            );
            assert_eq!(
                exports.get("summary"),
                Some(&Value::String(
                    "runtime extracted /tmp/rule-mock.xlsx".to_string()
                ))
            );
        }
        other => panic!("unexpected result: {other:?}"),
    }
}

#[tokio::test]
async fn test_agent_executor_materializes_template_summary_from_outputs() {
    let client = ToolCallLlmClient {
        responses: Mutex::new(VecDeque::from(vec![
            LlmResponse::ToolCall {
                id: "call-1".to_string(),
                name: "execute_action".to_string(),
                arguments: serde_json::json!({
                    "action": "json_stdout",
                    "params": {
                        "payload": {
                            "updated_file_path": "/tmp/mock2.xlsx"
                        }
                    },
                    "save_as": "fill_result",
                    "capture": "json_stdout"
                }),
            },
            LlmResponse::ToolCall {
                id: "call-2".to_string(),
                name: "finish".to_string(),
                arguments: serde_json::json!({}),
            },
        ])),
    };
    let executor = LlmAgentExecutor::new(client, LlmAgentExecutorConfig::default());

    let step = Step::agent("s1").with_params(serde_json::json!({
        "goal": "fill workbook",
        "allowed_actions": ["json_stdout"],
        "max_iterations": 2,
        "output_keys": ["updated_file_path", "summary"],
        "output_rules": {
            "updated_file_path": {
                "candidates": [
                    { "slot": "fill_result", "path": "updated_file_path" }
                ]
            },
            "summary": {
                "template": "updated file => {{updated_file_path}}"
            }
        }
    }));
    let mut registry = ActionRegistry::new();
    registry.register(Arc::new(JsonStdoutAction));
    let result = executor
        .execute_agent_step(
            &step,
            step.params.clone(),
            "exec-1",
            &test_ctx(),
            Arc::new(RwLock::new(registry)),
        )
        .await;

    match result {
        ActionResult::Success { exports } => {
            assert_eq!(
                exports.get("updated_file_path"),
                Some(&Value::String("/tmp/mock2.xlsx".to_string()))
            );
            assert_eq!(
                exports.get("summary"),
                Some(&Value::String(
                    "updated file => /tmp/mock2.xlsx".to_string()
                ))
            );
        }
        other => panic!("unexpected result: {other:?}"),
    }
}

#[test]
fn test_agent_tool_definitions_has_three_tools() {
    let params = AgentStepParams {
        mode: AgentMode::Explore,
        goal: "inspect".to_string(),
        allowed_actions: HashSet::from(["echo".to_string()]),
        max_iterations: 1,
        output_keys: vec!["summary".to_string()],
        output_rules: HashMap::new(),
        result_slot: None,
        bound_inputs: HashMap::new(),
    };
    let tools = agent_tool_definitions(&params);
    assert_eq!(tools.len(), 3);
    assert_eq!(tools[0].name, "execute_action");
    assert_eq!(tools[1].name, "finish");
    assert_eq!(tools[2].name, "return_final");
}
