use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use orchestral_core::action::{ActionContext, ActionInput, ActionResult};
use orchestral_core::executor::{ActionRegistry, AgentStepExecutor, ExecutorContext};
use orchestral_core::types::Step;
use serde_json::Value;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::planner::{LlmClient, LlmRequest};

const MAX_PROMPT_OBSERVATION_CHARS: usize = 600;
const MAX_AGENT_LOG_TEXT_CHARS: usize = 1_200;
const MAX_BOUND_INPUT_VALUE_CHARS: usize = 500;
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
            model: "gpt-4o-mini".to_string(),
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
            info!(
                step_id = %step.id,
                execution_id = %execution_id,
                iteration = iteration,
                max_iterations = params.max_iterations,
                observation_count = observations.len(),
                "agent iteration started"
            );
            let request = LlmRequest {
                system: build_agent_system_prompt(&params),
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

            let raw = match self.client.complete(request).await {
                Ok(v) => v,
                Err(err) => {
                    warn!(
                        step_id = %step.id,
                        iteration = iteration,
                        error = %err,
                        "agent llm call failed"
                    );
                    return ActionResult::error(format!("agent llm call failed: {}", err));
                }
            };
            debug!(
                step_id = %step.id,
                iteration = iteration,
                llm_output = %truncate_log_text(&raw),
                "agent llm response"
            );
            let json = extract_json(&raw).unwrap_or(raw);
            let decision = match parse_agent_decision(&json) {
                Ok(v) => v,
                Err(err) => {
                    warn!(
                        step_id = %step.id,
                        iteration = iteration,
                        error = %err,
                        response = %truncate_log_text(&json),
                        "agent decision parse failed"
                    );
                    observations.push(format!("invalid_decision: {}", truncate_text(&err)));
                    continue;
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
                    if !params.allowed_actions.contains(action_name.as_str()) {
                        warn!(
                            step_id = %step.id,
                            iteration = iteration,
                            action = %action_name,
                            "agent action blocked by allowed_actions"
                        );
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

fn build_agent_system_prompt(params: &AgentStepParams) -> String {
    let mut actions: Vec<&str> = params.allowed_actions.iter().map(String::as_str).collect();
    actions.sort_unstable();
    let bound_inputs_block = build_bound_inputs_block(&params.bound_inputs);
    format!(
        "You are a constrained execution agent.\nGoal: {}\nAllowed actions: {}\nRequired output keys: {}.\n{}\n\
Return strict JSON only in one of the two shapes:\n\
1) {{\"type\":\"action\",\"action\":\"<name>\",\"params\":{{...}}}}\n\
2) {{\"type\":\"final\",\"exports\":{{\"key\":\"value\"}}}}\n\
Never call actions outside the allowed list.",
        params.goal,
        actions.join(", "),
        params.output_keys.join(", "),
        bound_inputs_block
    )
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
            lines.push(format!(
                "- {} = {}",
                key,
                summarize_bound_input_value(key, value)
            ));
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
                summarize_multiline_text(text, 10)
            } else {
                summarize_multiline_text(text, 4)
            };
            truncate_with_limit(&normalized, MAX_BOUND_INPUT_VALUE_CHARS)
        }
        _ => truncate_with_limit(&value.to_string(), MAX_BOUND_INPUT_VALUE_CHARS),
    }
}

fn summarize_multiline_text(text: &str, max_lines: usize) -> String {
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
        lines.join(" | ")
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
            "xlsx_candidates": "docs/sample.xlsx",
            "skill_path": ".claude/skills/xlsx/SKILL.md"
        });

        let parsed = parse_agent_params("s1", &params).expect("parse agent params");
        assert_eq!(
            parsed.bound_inputs.get("xlsx_candidates"),
            Some(&Value::String("docs/sample.xlsx".to_string()))
        );
        assert_eq!(
            parsed.bound_inputs.get("skill_path"),
            Some(&Value::String(".claude/skills/xlsx/SKILL.md".to_string()))
        );

        let prompt = build_agent_system_prompt(&parsed);
        assert!(prompt.contains("Bound upstream inputs (from io_bindings):"));
        assert!(prompt.contains("xlsx_candidates"));
        assert!(prompt.contains("skill_path"));
    }

    #[test]
    fn test_summarize_bound_input_value_compacts_multiline_skill_text() {
        let skill_text = (1..=20)
            .map(|i| format!("line-{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let summarized =
            summarize_bound_input_value("skill_instructions", &Value::String(skill_text));
        assert!(summarized.contains("line-1 | line-2 | line-3"));
        assert!(!summarized.contains('\n'));
    }
}
