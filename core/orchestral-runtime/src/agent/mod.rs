use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::sync::Arc;

use async_trait::async_trait;
use orchestral_core::action::{ActionContext, ActionInput, ActionResult};
use orchestral_core::executor::{
    ActionRegistry, AgentStepExecutor, ExecutionProgressEvent, ExecutorContext,
};
use orchestral_core::planner::SkillInstruction;
use orchestral_core::types::Step;
use serde_json::Value;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::planner::{LlmClient, LlmRequest, LlmResponse, ToolDefinition};

const MAX_PROMPT_OBSERVATION_CHARS: usize = 600;
const MAX_AGENT_LOG_TEXT_CHARS: usize = 1_200;
const MAX_BOUND_INPUT_VALUE_CHARS: usize = 1_200;
const MAX_BOUND_SKILL_VALUE_CHARS: usize = 12_000;
const MAX_BOUND_INPUT_KEYS: usize = 4;
const MAX_RECENT_OBSERVATIONS: usize = 4;

#[derive(Debug, Clone)]
pub struct LlmAgentExecutorConfig {
    pub model: String,
    pub temperature: f32,
}

impl Default for LlmAgentExecutorConfig {
    fn default() -> Self {
        Self {
            model: "anthropic/claude-sonnet-4.5".to_string(),
            temperature: 0.2,
        }
    }
}

pub struct LlmAgentExecutor<C: LlmClient> {
    client: C,
    config: LlmAgentExecutorConfig,
}

impl<C: LlmClient> LlmAgentExecutor<C> {
    pub fn new(client: C, config: LlmAgentExecutorConfig) -> Self {
        Self { client, config }
    }
}

#[derive(Debug)]
struct AgentStepParams {
    goal: String,
    allowed_actions: HashSet<String>,
    max_iterations: u64,
    output_keys: Vec<String>,
    bound_inputs: HashMap<String, Value>,
}

#[derive(Debug)]
enum AgentDecision {
    Action { name: String, params: Value },
    Final { exports: HashMap<String, Value> },
}

#[async_trait]
impl<C: LlmClient> AgentStepExecutor for LlmAgentExecutor<C> {
    async fn execute_agent_step(
        &self,
        step: &Step,
        resolved_params: Value,
        execution_id: &str,
        ctx: &ExecutorContext,
        action_registry: Arc<RwLock<ActionRegistry>>,
    ) -> ActionResult {
        let params = match parse_agent_params(step.id.as_ref(), &resolved_params) {
            Ok(v) => v,
            Err(err) => {
                warn!(
                    step_id = %step.id,
                    error = %err,
                    "agent step params invalid"
                );
                return ActionResult::error(err);
            }
        };
        let mut allowed_actions = params.allowed_actions.iter().cloned().collect::<Vec<_>>();
        allowed_actions.sort_unstable();
        info!(
            step_id = %step.id,
            execution_id = %execution_id,
            max_iterations = params.max_iterations,
            output_keys = ?params.output_keys,
            allowed_actions = ?allowed_actions,
            "agent step started"
        );

        let mut observations: Vec<String> = Vec::new();
        for iteration in 1..=params.max_iterations {
            report_agent_progress(
                ctx,
                step,
                "iteration",
                None,
                format!("iteration {}/{}", iteration, params.max_iterations),
            )
            .await;
            info!(
                step_id = %step.id,
                execution_id = %execution_id,
                iteration = iteration,
                max_iterations = params.max_iterations,
                observation_count = observations.len(),
                "agent iteration started"
            );
            let request = LlmRequest {
                system: build_agent_system_prompt(&params, ctx),
                user: build_agent_user_prompt(iteration, &observations),
                model: self.config.model.clone(),
                temperature: self.config.temperature,
            };
            debug!(
                step_id = %step.id,
                iteration = iteration,
                model = %self.config.model,
                temperature = self.config.temperature,
                system_prompt = %truncate_log_text(&request.system),
                user_prompt = %truncate_log_text(&request.user),
                "agent llm request"
            );

            let tools = agent_tool_definitions();
            let llm_response =
                match self.client.complete_with_tools(request, &tools).await {
                    Ok(v) => v,
                    Err(err) => {
                        warn!(
                            step_id = %step.id,
                            iteration = iteration,
                            error = %err,
                            "agent llm call failed"
                        );
                        report_agent_progress(
                            ctx,
                            step,
                            "note",
                            None,
                            format!("llm call failed: {}", truncate_text(&err.to_string())),
                        )
                        .await;
                        return ActionResult::error(format!("agent llm call failed: {}", err));
                    }
                };

            let decision = match &llm_response {
                LlmResponse::ToolCall {
                    name, arguments, ..
                } => {
                    debug!(
                        step_id = %step.id,
                        iteration = iteration,
                        tool_name = %name,
                        tool_args = %truncate_log_text(&arguments.to_string()),
                        "agent llm tool_call response"
                    );
                    match parse_tool_call_decision(name, arguments.clone()) {
                        Ok(v) => v,
                        Err(err) => {
                            warn!(
                                step_id = %step.id,
                                iteration = iteration,
                                error = %err,
                                tool_name = %name,
                                "agent tool_call parse failed"
                            );
                            observations.push(format!(
                                "invalid_tool_call: {}",
                                truncate_text(&err)
                            ));
                            continue;
                        }
                    }
                }
                LlmResponse::Text(raw) => {
                    debug!(
                        step_id = %step.id,
                        iteration = iteration,
                        llm_output = %truncate_log_text(raw),
                        "agent llm text response (fallback)"
                    );
                    let json = extract_json(raw).unwrap_or_else(|| raw.clone());
                    match parse_agent_decision(&json) {
                        Ok(v) => v,
                        Err(err) => {
                            warn!(
                                step_id = %step.id,
                                iteration = iteration,
                                error = %err,
                                response = %truncate_log_text(&json),
                                "agent decision parse failed"
                            );
                            report_agent_progress(
                                ctx,
                                step,
                                "note",
                                None,
                                format!("invalid agent response: {}", truncate_text(&err)),
                            )
                            .await;
                            observations.push(format!(
                                "invalid_decision: {}",
                                truncate_text(&err)
                            ));
                            continue;
                        }
                    }
                }
            };

            match decision {
                AgentDecision::Action {
                    name: action_name,
                    params: action_params,
                } => {
                    info!(
                        step_id = %step.id,
                        iteration = iteration,
                        action = %action_name,
                        "agent decided action"
                    );
                    report_agent_progress(
                        ctx,
                        step,
                        "action_started",
                        Some(action_name.as_str()),
                        format!("run {}", action_name),
                    )
                    .await;
                    if !params.allowed_actions.contains(action_name.as_str()) {
                        warn!(
                            step_id = %step.id,
                            iteration = iteration,
                            action = %action_name,
                            "agent action blocked by allowed_actions"
                        );
                        report_agent_progress(
                            ctx,
                            step,
                            "action_failed",
                            Some(action_name.as_str()),
                            format!("blocked {}", action_name),
                        )
                        .await;
                        observations.push(format!(
                            "blocked_action: '{}' is not in allowed_actions",
                            action_name
                        ));
                        continue;
                    }

                    let action = {
                        let registry = action_registry.read().await;
                        registry.get(&action_name)
                    };
                    let Some(action) = action else {
                        warn!(
                            step_id = %step.id,
                            iteration = iteration,
                            action = %action_name,
                            "agent action missing in registry"
                        );
                        report_agent_progress(
                            ctx,
                            step,
                            "action_failed",
                            Some(action_name.as_str()),
                            format!("missing action {}", action_name),
                        )
                        .await;
                        observations.push(format!("missing_action: '{}'", action_name));
                        continue;
                    };

                    let action_ctx = ActionContext::new(
                        ctx.task_id.clone(),
                        step.id.clone(),
                        format!("{}-agent-{}", execution_id, iteration),
                        ctx.working_set.clone(),
                        ctx.reference_store.clone(),
                    );
                    let action_result = action
                        .run(ActionInput::with_params(action_params), action_ctx)
                        .await;
                    match action_result {
                        ActionResult::Success { exports } => {
                            info!(
                                step_id = %step.id,
                                iteration = iteration,
                                action = %action_name,
                                export_keys = ?exports.keys().collect::<Vec<_>>(),
                                "agent action succeeded"
                            );
                            let status = exports.get("status").and_then(|v| v.as_i64());
                            let progress_line = status
                                .map(|code| format!("{} completed status={}", action_name, code))
                                .unwrap_or_else(|| format!("{} completed", action_name));
                            report_agent_progress(
                                ctx,
                                step,
                                "action_completed",
                                Some(action_name.as_str()),
                                progress_line,
                            )
                            .await;
                            observations.push(format!(
                                "action_success {} => {}",
                                action_name,
                                truncate_text(
                                    &Value::Object(
                                        exports
                                            .iter()
                                            .map(|(k, v)| (k.clone(), v.clone()))
                                            .collect()
                                    )
                                    .to_string()
                                )
                            ));
                        }
                        ActionResult::NeedClarification { .. }
                        | ActionResult::NeedApproval { .. } => {
                            info!(
                                step_id = %step.id,
                                iteration = iteration,
                                action = %action_name,
                                "agent action requested user interaction"
                            );
                            report_agent_progress(
                                ctx,
                                step,
                                "action_completed",
                                Some(action_name.as_str()),
                                format!("{} requires user input", action_name),
                            )
                            .await;
                            return action_result;
                        }
                        ActionResult::RetryableError { message, .. } => {
                            warn!(
                                step_id = %step.id,
                                iteration = iteration,
                                action = %action_name,
                                error = %truncate_log_text(&message),
                                "agent action retryable error"
                            );
                            report_agent_progress(
                                ctx,
                                step,
                                "action_failed",
                                Some(action_name.as_str()),
                                format!(
                                    "{} retryable error: {}",
                                    action_name,
                                    truncate_text(&message)
                                ),
                            )
                            .await;
                            observations.push(format!(
                                "action_retryable_error {} => {}",
                                action_name,
                                truncate_text(&message)
                            ));
                        }
                        ActionResult::Error { message } => {
                            warn!(
                                step_id = %step.id,
                                iteration = iteration,
                                action = %action_name,
                                error = %truncate_log_text(&message),
                                "agent action failed"
                            );
                            report_agent_progress(
                                ctx,
                                step,
                                "action_failed",
                                Some(action_name.as_str()),
                                format!("{} failed: {}", action_name, truncate_text(&message)),
                            )
                            .await;
                            observations.push(format!(
                                "action_error {} => {}",
                                action_name,
                                truncate_text(&message)
                            ));
                        }
                    }
                }
                AgentDecision::Final { exports } => {
                    if let Some(missing) = params
                        .output_keys
                        .iter()
                        .find(|key| !exports.contains_key(key.as_str()))
                    {
                        warn!(
                            step_id = %step.id,
                            iteration = iteration,
                            missing_output_key = %missing,
                            available_keys = ?exports.keys().collect::<Vec<_>>(),
                            "agent final result missing required output key"
                        );
                        report_agent_progress(
                            ctx,
                            step,
                            "note",
                            None,
                            format!("final result missing '{}'; retrying", missing),
                        )
                        .await;
                        observations.push(format!(
                            "invalid_final: missing required output_key '{}'",
                            missing
                        ));
                        continue;
                    }

                    let filtered = params
                        .output_keys
                        .iter()
                        .filter_map(|k| exports.get(k).map(|v| (k.clone(), v.clone())))
                        .collect::<HashMap<_, _>>();
                    info!(
                        step_id = %step.id,
                        iteration = iteration,
                        output_keys = ?params.output_keys,
                        "agent step completed"
                    );
                    report_agent_progress(ctx, step, "note", None, "agent completed").await;
                    return ActionResult::success_with(filtered);
                }
            }
        }

        let observation_tail =
            summarize_recent_observations(&observations, MAX_RECENT_OBSERVATIONS);
        warn!(
            step_id = %step.id,
            max_iterations = params.max_iterations,
            recent_observations = %observation_tail,
            "agent step exhausted max_iterations"
        );
        report_agent_progress(
            ctx,
            step,
            "note",
            None,
            format!("agent exhausted after {} iterations", params.max_iterations),
        )
        .await;
        ActionResult::error(format!(
            "agent step '{}' reached max_iterations without a valid final result; recent_observations={}",
            step.id,
            observation_tail
        ))
    }
}

fn parse_agent_params(step_id: &str, params: &Value) -> Result<AgentStepParams, String> {
    let Some(obj) = params.as_object() else {
        return Err(format!(
            "invalid agent params in step '{}': params must be an object",
            step_id
        ));
    };

    let goal = obj
        .get("goal")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            format!(
                "invalid agent params in step '{}': goal is required",
                step_id
            )
        })?
        .to_string();

    let allowed_actions = obj
        .get("allowed_actions")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            format!(
                "invalid agent params in step '{}': allowed_actions must be a non-empty array",
                step_id
            )
        })?
        .iter()
        .filter_map(|v| v.as_str().map(str::trim))
        .filter(|v| !v.is_empty())
        .map(str::to_string)
        .collect::<HashSet<_>>();
    if allowed_actions.is_empty() {
        return Err(format!(
            "invalid agent params in step '{}': allowed_actions must be a non-empty array",
            step_id
        ));
    }

    let max_iterations = obj
        .get("max_iterations")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| {
            format!(
                "invalid agent params in step '{}': max_iterations is required",
                step_id
            )
        })?;
    if !(1..=10).contains(&max_iterations) {
        return Err(format!(
            "invalid agent params in step '{}': max_iterations must be in [1,10]",
            step_id
        ));
    }

    let output_keys = obj
        .get("output_keys")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            format!(
                "invalid agent params in step '{}': output_keys must be a non-empty array",
                step_id
            )
        })?
        .iter()
        .filter_map(|v| v.as_str().map(str::trim))
        .filter(|v| !v.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    if output_keys.is_empty() {
        return Err(format!(
            "invalid agent params in step '{}': output_keys must be a non-empty array",
            step_id
        ));
    }

    let bound_inputs = obj
        .iter()
        .filter_map(|(key, value)| {
            if matches!(
                key.as_str(),
                "goal" | "allowed_actions" | "max_iterations" | "output_keys"
            ) {
                None
            } else {
                Some((key.clone(), value.clone()))
            }
        })
        .collect::<HashMap<_, _>>();

    Ok(AgentStepParams {
        goal,
        allowed_actions,
        max_iterations,
        output_keys,
        bound_inputs,
    })
}

async fn report_agent_progress(
    ctx: &ExecutorContext,
    step: &Step,
    kind: &str,
    action: Option<&str>,
    message: impl Into<String>,
) {
    if let Some(reporter) = &ctx.progress_reporter {
        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "agent_progress_kind".to_string(),
            Value::String(kind.to_string()),
        );
        if let Some(action_name) = action.filter(|s| !s.trim().is_empty()) {
            metadata.insert(
                "agent_action".to_string(),
                Value::String(action_name.to_string()),
            );
        }
        let event = ExecutionProgressEvent::new(
            ctx.task_id.clone(),
            Some(step.id.clone()),
            Some("agent".to_string()),
            "agent_progress",
        )
        .with_message(message.into())
        .with_metadata(Value::Object(metadata));
        if let Err(err) = reporter.report(event).await {
            warn!(
                step_id = %step.id,
                error = %err,
                "failed to report agent progress"
            );
        }
    }
}

fn build_agent_system_prompt(params: &AgentStepParams, ctx: &ExecutorContext) -> String {
    let mut actions: Vec<&str> = params.allowed_actions.iter().map(String::as_str).collect();
    actions.sort_unstable();
    let bound_inputs_block = build_bound_inputs_block(&params.bound_inputs);
    let env_block = build_execution_environment_block(ctx);
    let skill_block = build_skill_knowledge_block(&ctx.skill_instructions);
    format!(
        "You are a constrained execution agent.\n\
Goal: {goal}\n\
Allowed actions: {actions}\n\
Required output keys: {output_keys}.\n\
{env}{skill}{bound}\n\
You have two tools:\n\
1) execute_action — run one of the allowed actions. Example: execute_action({{\"action\":\"shell\",\"params\":{{\"command\":\"ls\"}}}})\n\
2) return_final — submit results when the goal is achieved. You MUST call this when done. Example: return_final({{\"exports\":{{{output_keys_example}}}}})\n\
\n\
Rules:\n\
- Act first, analyze minimally. Do not spend iterations only reading/analyzing.\n\
- Never call actions outside the allowed list.\n\
- You MUST call return_final before iterations run out. If the main task is done, call return_final immediately.",
        goal = params.goal,
        actions = actions.join(", "),
        output_keys = params.output_keys.join(", "),
        env = env_block,
        skill = skill_block,
        bound = bound_inputs_block,
        output_keys_example = params.output_keys.iter()
            .map(|k| format!("\"{}\":\"...\"", k))
            .collect::<Vec<_>>()
            .join(","),
    )
}

fn build_execution_environment_block(ctx: &ExecutorContext) -> String {
    let Some(info) = &ctx.runtime_info else {
        return String::new();
    };
    if info.os.is_empty() {
        return String::new();
    }
    let mut out = String::from("Execution Environment:\n");
    let _ = writeln!(out, "- os: {}", info.os);
    let _ = writeln!(out, "- os_family: {}", info.os_family);
    let _ = writeln!(out, "- arch: {}", info.arch);
    if let Some(shell) = &info.shell {
        let _ = writeln!(out, "- shell: {}", shell);
    }
    if let Some(python) = resolve_python_path(&ctx.skill_instructions) {
        let _ = writeln!(
            out,
            "- python: {} (preferred managed runtime when Python execution is needed)",
            python
        );
    }
    out
}

/// Resolve the best python path: skill venv > project root venv > None.
fn resolve_python_path(skills: &[SkillInstruction]) -> Option<String> {
    for skill in skills {
        if let Some(venv) = &skill.venv_python {
            if std::path::Path::new(venv).exists() {
                return Some(venv.clone());
            }
        }
    }
    let project_venv = std::path::Path::new(".venv/bin/python3");
    if project_venv.exists() {
        return Some(".venv/bin/python3".to_string());
    }
    None
}

fn build_skill_knowledge_block(skills: &[SkillInstruction]) -> String {
    if skills.is_empty() {
        return String::new();
    }
    let mut out = String::from("Activated Skills:\n");
    out.push_str(
        "If the skill summary is insufficient for concrete execution details, read its skill file via file_read before acting.\n",
    );
    for skill in skills {
        let _ = writeln!(out, "- {}: {}", skill.skill_name, skill.instructions.trim());
        if let Some(path) = &skill.skill_path {
            let _ = writeln!(out, "  [skill file: {}]", path);
        }
        if let Some(dir) = &skill.scripts_dir {
            let _ = writeln!(out, "  [scripts: {}]", dir);
        }
    }
    out
}

fn build_bound_inputs_block(bound_inputs: &HashMap<String, Value>) -> String {
    if bound_inputs.is_empty() {
        return "Bound upstream inputs: none".to_string();
    }

    let mut keys = bound_inputs.keys().cloned().collect::<Vec<_>>();
    keys.sort_unstable();

    let mut lines = Vec::with_capacity(keys.len() + 2);
    lines.push("Bound upstream inputs (from io_bindings):".to_string());
    for key in keys.iter().take(MAX_BOUND_INPUT_KEYS) {
        if let Some(value) = bound_inputs.get(key) {
            let summarized = summarize_bound_input_value(key, value);
            if summarized.contains('\n') {
                lines.push(format!("- {}:", key));
                for line in summarized.lines() {
                    lines.push(format!("  {}", line));
                }
            } else {
                lines.push(format!("- {} = {}", key, summarized));
            }
        }
    }
    if keys.len() > MAX_BOUND_INPUT_KEYS {
        lines.push(format!(
            "- ... {} more bound input key(s) omitted",
            keys.len() - MAX_BOUND_INPUT_KEYS
        ));
    }
    lines.push(
        "Treat bound upstream inputs as authoritative context; do not rediscover unless verification is necessary."
            .to_string(),
    );
    lines.join("\n")
}

fn summarize_bound_input_value(key: &str, value: &Value) -> String {
    let key_lower = key.to_ascii_lowercase();
    match value {
        Value::String(text) => {
            let normalized = if key_lower.contains("skill") {
                // Keep far more skill detail for explicit skill-driven tasks.
                summarize_multiline_text(text, 120, "\n")
            } else {
                summarize_multiline_text(text, 4, " | ")
            };
            if key_lower.contains("skill") {
                truncate_with_limit(&normalized, MAX_BOUND_SKILL_VALUE_CHARS)
            } else {
                truncate_with_limit(&normalized, MAX_BOUND_INPUT_VALUE_CHARS)
            }
        }
        _ => truncate_with_limit(&value.to_string(), MAX_BOUND_INPUT_VALUE_CHARS),
    }
}

fn summarize_multiline_text(text: &str, max_lines: usize, separator: &str) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let lines = trimmed
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .take(max_lines)
        .collect::<Vec<_>>();
    if lines.is_empty() {
        trimmed.to_string()
    } else {
        lines.join(separator)
    }
}

fn build_agent_user_prompt(iteration: u64, observations: &[String]) -> String {
    if observations.is_empty() {
        return format!("iteration={} no prior observations", iteration);
    }
    let tail = observations
        .iter()
        .rev()
        .take(MAX_RECENT_OBSERVATIONS)
        .cloned()
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>();
    format!(
        "iteration={}\nrecent_observations:\n- {}",
        iteration,
        tail.join("\n- ")
    )
}

fn agent_tool_definitions() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "execute_action".to_string(),
            description: "Execute one of the allowed actions".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "Action name from the allowed list"
                    },
                    "params": {
                        "type": "object",
                        "description": "Action parameters"
                    }
                },
                "required": ["action", "params"]
            }),
        },
        ToolDefinition {
            name: "return_final".to_string(),
            description: "Return final results when the goal is achieved".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "exports": {
                        "type": "object",
                        "description": "Key-value map of required output_keys"
                    }
                },
                "required": ["exports"]
            }),
        },
    ]
}

fn parse_tool_call_decision(name: &str, args: Value) -> Result<AgentDecision, String> {
    match name {
        "execute_action" => {
            let action = args
                .get("action")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| "execute_action: missing 'action' field".to_string())?
                .to_string();
            let params = args
                .get("params")
                .cloned()
                .unwrap_or(Value::Object(Default::default()));
            Ok(AgentDecision::Action {
                name: action,
                params,
            })
        }
        "return_final" => {
            let exports = args
                .get("exports")
                .and_then(|v| v.as_object())
                .ok_or_else(|| "return_final: missing object field 'exports'".to_string())?
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<HashMap<_, _>>();
            Ok(AgentDecision::Final { exports })
        }
        other => Err(format!("unknown tool: '{}'", other)),
    }
}

fn parse_agent_decision(json: &str) -> Result<AgentDecision, String> {
    let value: Value = serde_json::from_str(json).map_err(|e| format!("invalid json: {}", e))?;
    let Some(obj) = value.as_object() else {
        return Err("decision must be a JSON object".to_string());
    };

    let decision_type = obj
        .get("type")
        .and_then(|v| v.as_str())
        .map(|v| v.to_ascii_lowercase())
        .ok_or_else(|| "missing decision field 'type'".to_string())?;
    match decision_type.as_str() {
        "action" => {
            let name = obj
                .get("action")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| "action decision missing 'action'".to_string())?
                .to_string();
            let params = obj
                .get("params")
                .cloned()
                .unwrap_or(Value::Object(Default::default()));
            Ok(AgentDecision::Action { name, params })
        }
        "final" => {
            let exports = obj
                .get("exports")
                .and_then(|v| v.as_object())
                .ok_or_else(|| "final decision missing object field 'exports'".to_string())?
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<HashMap<_, _>>();
            Ok(AgentDecision::Final { exports })
        }
        other => Err(format!("unsupported decision type '{}'", other)),
    }
}

fn extract_json(text: &str) -> Option<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut start = None;
    let mut depth = 0_u32;
    let mut in_string = false;
    let mut escaped = false;
    for (idx, ch) in chars.iter().enumerate() {
        if in_string {
            if escaped {
                escaped = false;
            } else if *ch == '\\' {
                escaped = true;
            } else if *ch == '"' {
                in_string = false;
            }
            continue;
        }
        if *ch == '"' {
            in_string = true;
            continue;
        }
        if *ch == '{' {
            if start.is_none() {
                start = Some(idx);
            }
            depth = depth.saturating_add(1);
        } else if *ch == '}' {
            if depth == 0 {
                continue;
            }
            depth -= 1;
            if depth == 0 {
                if let Some(start_idx) = start {
                    let candidate: String = chars[start_idx..=idx].iter().collect();
                    if serde_json::from_str::<Value>(&candidate).is_ok() {
                        return Some(candidate);
                    }
                }
                start = None;
            }
        }
    }
    None
}

fn truncate_text(input: &str) -> String {
    truncate_with_limit(input, MAX_PROMPT_OBSERVATION_CHARS)
}

fn truncate_log_text(input: &str) -> String {
    let char_count = input.chars().count();
    if char_count <= MAX_AGENT_LOG_TEXT_CHARS {
        return input.to_string();
    }
    let mut preview: String = input.chars().take(MAX_AGENT_LOG_TEXT_CHARS).collect();
    preview.push_str(&format!("... [truncated, total_chars={}]", char_count));
    preview
}

fn truncate_with_limit(input: &str, max_chars: usize) -> String {
    let char_count = input.chars().count();
    if char_count <= max_chars {
        return input.to_string();
    }
    let mut preview: String = input.chars().take(max_chars).collect();
    preview.push_str(&format!("... [truncated, total_chars={}]", char_count));
    preview
}

fn summarize_recent_observations(observations: &[String], take_last: usize) -> String {
    if observations.is_empty() {
        return "none".to_string();
    }
    let tail = observations
        .iter()
        .rev()
        .take(take_last)
        .cloned()
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join(" | ");
    truncate_log_text(&tail)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use orchestral_core::action::{Action, ActionMeta};
    use orchestral_core::store::{InMemoryReferenceStore, WorkingSet};
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
        async fn complete(
            &self,
            _request: LlmRequest,
        ) -> Result<String, crate::planner::LlmError> {
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
            let mut exports = HashMap::new();
            exports.insert(
                "observation".to_string(),
                input
                    .params
                    .get("message")
                    .cloned()
                    .unwrap_or(Value::String(String::new())),
            );
            ActionResult::success_with(exports)
        }
    }

    fn test_ctx() -> ExecutorContext {
        ExecutorContext::new(
            TaskId::from("task-1"),
            Arc::new(RwLock::new(WorkingSet::new())),
            Arc::new(InMemoryReferenceStore::new()),
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
                    Some(&Value::String("done".to_string()))
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_agent_executor_returns_error_when_final_missing_output_key() {
        let client = SequenceLlmClient {
            responses: Mutex::new(VecDeque::from(vec![
                r#"{"type":"final","exports":{"other":"x"}}"#.to_string(),
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
            Some(&Value::String(".claude/skills/tabular/SKILL.md".to_string()))
        );

        let prompt = build_agent_system_prompt(&parsed, &test_ctx());
        assert!(prompt.contains("Bound upstream inputs (from io_bindings):"));
        assert!(prompt.contains("input_candidates"));
        assert!(prompt.contains("skill_path"));
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
            Arc::new(InMemoryReferenceStore::new()),
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
            Arc::new(InMemoryReferenceStore::new()),
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
        assert!(prompt.contains("- tabular: data processing and formatting"));
        assert!(prompt.contains("[skill file: /skills/tabular/SKILL.md]"));
        assert!(prompt.contains("[scripts: /skills/tabular/scripts]"));
    }

    #[test]
    fn test_summarize_bound_input_value_compacts_multiline_skill_text() {
        let skill_text = (1..=20)
            .map(|i| format!("line-{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let summarized =
            summarize_bound_input_value("skill_instructions", &Value::String(skill_text));
        assert!(summarized.contains("line-1\nline-2\nline-3"));
        assert!(summarized.contains('\n'));
    }

    #[test]
    fn test_build_bound_inputs_block_uses_multiline_block_for_skill() {
        let mut bound_inputs = HashMap::new();
        bound_inputs.insert(
            "skill_doc".to_string(),
            Value::String("line-1\nline-2".to_string()),
        );
        bound_inputs.insert(
            "input_file".to_string(),
            Value::String("docs/a.dat".to_string()),
        );
        let block = build_bound_inputs_block(&bound_inputs);
        assert!(block.contains("- skill_doc:"));
        assert!(block.contains("  line-1"));
        assert!(block.contains("  line-2"));
        assert!(block.contains("- input_file = docs/a.dat"));
    }

    #[test]
    fn test_parse_tool_call_execute_action() {
        let args = serde_json::json!({
            "action": "shell",
            "params": {"command": "ls", "args": ["-la"]}
        });
        let decision = parse_tool_call_decision("execute_action", args).unwrap();
        match decision {
            AgentDecision::Action { name, params } => {
                assert_eq!(name, "shell");
                assert_eq!(params.get("command").unwrap(), "ls");
            }
            other => panic!("expected Action, got {:?}", other),
        }
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

    #[tokio::test]
    async fn test_agent_executor_with_tool_call_client() {
        let client = ToolCallLlmClient {
            responses: Mutex::new(VecDeque::from(vec![
                LlmResponse::ToolCall {
                    id: "call-1".to_string(),
                    name: "execute_action".to_string(),
                    arguments: serde_json::json!({
                        "action": "echo",
                        "params": {"message": "hello"}
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

    #[test]
    fn test_agent_tool_definitions_has_two_tools() {
        let tools = agent_tool_definitions();
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0].name, "execute_action");
        assert_eq!(tools[1].name, "return_final");
    }
}
