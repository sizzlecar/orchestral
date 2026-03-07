use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::sync::Arc;

use async_trait::async_trait;
use orchestral_core::action::ActionResult;
#[cfg(test)]
use orchestral_core::action::{ActionContext, ActionInput};
use orchestral_core::executor::{
    execute_action_with_registry_with_options, ActionExecutionOptions, ActionPreflightHook,
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
const MAX_DISCOVERED_SKILL_SCRIPTS: usize = 12;

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
    output_rules: HashMap<String, AgentOutputRule>,
    bound_inputs: HashMap<String, Value>,
}

#[derive(Debug)]
enum AgentDecision {
    Action {
        name: String,
        params: Value,
        save_as: Option<String>,
        capture: Option<AgentCaptureMode>,
    },
    Final {
        exports: HashMap<String, Value>,
    },
    Finish,
}

#[derive(Debug, Clone, Default)]
struct AgentOutputRule {
    candidates: Vec<AgentOutputCandidate>,
    template: Option<String>,
    fallback_aliases: Vec<String>,
    required_action: Option<String>,
}

#[derive(Debug, Clone)]
struct AgentOutputCandidate {
    slot: String,
    path: Vec<AgentPathSegment>,
    required_action: Option<String>,
}

#[derive(Debug, Clone)]
enum AgentPathSegment {
    Key(String),
    Index(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentCaptureMode {
    JsonStdout,
}

#[derive(Debug, Clone)]
struct AgentEvidenceRecord {
    action_name: String,
    raw_exports: HashMap<String, Value>,
    value: Value,
}

#[derive(Debug, Default, Clone)]
struct AgentEvidenceStore {
    slots: HashMap<String, Value>,
    slot_actions: HashMap<String, String>,
    records: Vec<AgentEvidenceRecord>,
}

struct ForcedFinalizationContext<'a> {
    step: &'a Step,
    params: &'a AgentStepParams,
    ctx: &'a ExecutorContext,
    observations: &'a [String],
    last_success_exports: Option<&'a HashMap<String, Value>>,
    evidence: &'a AgentEvidenceStore,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MaterializeError {
    key: String,
    reason_code: &'static str,
    detail: String,
}

impl MaterializeError {
    fn new(key: &str, reason_code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            key: key.to_string(),
            reason_code,
            detail: detail.into(),
        }
    }

    fn summary(&self) -> String {
        format!("{} [{}]: {}", self.key, self.reason_code, self.detail)
    }
}

impl AgentEvidenceStore {
    fn set_slot(&mut self, key: String, value: Value, action_name: Option<&str>) {
        self.slots.insert(key.clone(), value);
        if let Some(action_name) = action_name {
            self.slot_actions.insert(key, action_name.to_string());
        } else {
            self.slot_actions.remove(&key);
        }
    }

    fn slot_value_with_requirement(
        &self,
        key: &str,
        required_action: Option<&str>,
    ) -> Option<Value> {
        let value = self.slots.get(key)?;
        if let Some(required_action) = required_action {
            let actual = self.slot_actions.get(key).map(String::as_str)?;
            if actual != required_action {
                return None;
            }
        }
        Some(value.clone())
    }

    fn record_success(
        &mut self,
        action_name: &str,
        raw_exports: HashMap<String, Value>,
        value: Value,
        save_as: Option<&str>,
    ) {
        let raw_object = exports_to_value(&raw_exports);
        self.set_slot("last".to_string(), value.clone(), Some(action_name));
        self.set_slot(
            "last_raw".to_string(),
            raw_object.clone(),
            Some(action_name),
        );
        self.set_slot(
            format!("action:{}", action_name),
            value.clone(),
            Some(action_name),
        );
        self.set_slot(
            format!("action:{}:raw", action_name),
            raw_object,
            Some(action_name),
        );
        self.set_slot(
            "last_action".to_string(),
            Value::String(action_name.to_string()),
            Some(action_name),
        );

        if let Some(parsed) = parse_json_export_field(&raw_exports, "stdout") {
            self.set_slot(
                "last_stdout_json".to_string(),
                parsed.clone(),
                Some(action_name),
            );
            self.set_slot(
                format!("action:{}:stdout_json", action_name),
                parsed,
                Some(action_name),
            );
        }
        if let Some(parsed) = parse_json_export_field(&raw_exports, "body") {
            self.set_slot(
                "last_body_json".to_string(),
                parsed.clone(),
                Some(action_name),
            );
            self.set_slot(
                format!("action:{}:body_json", action_name),
                parsed,
                Some(action_name),
            );
        }

        if let Some(slot) = save_as {
            self.set_slot(slot.to_string(), value.clone(), Some(action_name));
        }
        self.records.push(AgentEvidenceRecord {
            action_name: action_name.to_string(),
            raw_exports,
            value,
        });
    }
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
        let mut last_success_exports: Option<HashMap<String, Value>> = None;
        let mut evidence = AgentEvidenceStore::default();
        let action_execution_options =
            ActionExecutionOptions::default().with_preflight_hook(default_action_preflight_hook());
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
            let llm_response = match self.client.complete_with_tools(request, &tools).await {
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
                            observations
                                .push(format!("invalid_tool_call: {}", truncate_text(&err)));
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
                            observations.push(format!("invalid_decision: {}", truncate_text(&err)));
                            continue;
                        }
                    }
                }
            };

            match decision {
                AgentDecision::Action {
                    name: action_name,
                    params: action_params,
                    save_as,
                    capture,
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

                    let action_result = execute_action_with_registry_with_options(
                        action_registry.clone(),
                        ctx,
                        &step.id,
                        &action_name,
                        &format!("{}-agent-{}", execution_id, iteration),
                        action_params,
                        &action_execution_options,
                    )
                    .await;
                    match action_result {
                        ActionResult::Success { exports } => {
                            let captured_value =
                                match derive_evidence_value(&exports, capture.as_ref()) {
                                    Ok(value) => value,
                                    Err(err) => {
                                        observations.push(format!(
                                            "capture_error {} => {}",
                                            action_name,
                                            truncate_text(&err)
                                        ));
                                        Value::Object(
                                            exports
                                                .iter()
                                                .map(|(k, v)| (k.clone(), v.clone()))
                                                .collect(),
                                        )
                                    }
                                };
                            evidence.record_success(
                                &action_name,
                                exports.clone(),
                                captured_value,
                                save_as.as_deref(),
                            );
                            last_success_exports = Some(exports.clone());
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
                            if let Some(slot) = save_as.as_deref() {
                                observations
                                    .push(format!("saved_slot {} <= {}", slot, action_name));
                            }
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
                            if let Some(reason) = deterministic_action_error_reason(&message) {
                                warn!(
                                    step_id = %step.id,
                                    iteration = iteration,
                                    action = %action_name,
                                    reason = reason,
                                    error = %truncate_log_text(&message),
                                    "agent deterministic action error detected (fast fail)"
                                );
                                report_agent_progress(
                                    ctx,
                                    step,
                                    "action_failed",
                                    Some(action_name.as_str()),
                                    format!(
                                        "{} deterministic error ({}): {}",
                                        action_name,
                                        reason,
                                        truncate_text(&message)
                                    ),
                                )
                                .await;
                                return ActionResult::error(format!(
                                    "agent action '{}' deterministic error ({}): {}",
                                    action_name, reason, message
                                ));
                            }
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
                            if let Some(reason) = deterministic_action_error_reason(&message) {
                                warn!(
                                    step_id = %step.id,
                                    iteration = iteration,
                                    action = %action_name,
                                    reason = reason,
                                    error = %truncate_log_text(&message),
                                    "agent deterministic action error detected (fast fail)"
                                );
                                report_agent_progress(
                                    ctx,
                                    step,
                                    "action_failed",
                                    Some(action_name.as_str()),
                                    format!(
                                        "{} deterministic error ({}): {}",
                                        action_name,
                                        reason,
                                        truncate_text(&message)
                                    ),
                                )
                                .await;
                                return ActionResult::error(format!(
                                    "agent action '{}' deterministic error ({}): {}",
                                    action_name, reason, message
                                ));
                            }
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
                    let filtered = match materialize_final_exports(&params, &evidence) {
                        Ok(filtered) => filtered,
                        Err(err) => {
                            warn!(
                                step_id = %step.id,
                                iteration = iteration,
                                missing_output_key = %err.key,
                                reason_code = err.reason_code,
                                reason_detail = %truncate_log_text(&err.detail),
                                available_slots = ?evidence.slots.keys().collect::<Vec<_>>(),
                                available_keys = ?exports.keys().collect::<Vec<_>>(),
                                "agent return_final missing materialized output key"
                            );
                            report_agent_progress(
                                ctx,
                                step,
                                "note",
                                None,
                                format!(
                                    "return_final materialize failed '{}' ({}); retrying",
                                    err.key, err.reason_code
                                ),
                            )
                            .await;
                            observations.push(format!(
                                "invalid_final: output_key='{}' reason='{}' detail='{}'",
                                err.key,
                                err.reason_code,
                                truncate_text(&err.detail)
                            ));
                            continue;
                        }
                    };
                    info!(
                        step_id = %step.id,
                        iteration = iteration,
                        output_keys = ?params.output_keys,
                        "agent step completed"
                    );
                    report_agent_progress(ctx, step, "note", None, "agent completed").await;
                    return ActionResult::success_with(filtered);
                }
                AgentDecision::Finish => {
                    let filtered = match materialize_final_exports(&params, &evidence) {
                        Ok(filtered) => filtered,
                        Err(err) => {
                            warn!(
                                step_id = %step.id,
                                iteration = iteration,
                                missing_output_key = %err.key,
                                reason_code = err.reason_code,
                                reason_detail = %truncate_log_text(&err.detail),
                                available_slots = ?evidence.slots.keys().collect::<Vec<_>>(),
                                "agent finish missing materialized output key"
                            );
                            report_agent_progress(
                                ctx,
                                step,
                                "note",
                                None,
                                format!(
                                    "finish materialize failed '{}' ({}); retrying",
                                    err.key, err.reason_code
                                ),
                            )
                            .await;
                            observations.push(format!(
                                "invalid_finish: output_key='{}' reason='{}' detail='{}'",
                                err.key,
                                err.reason_code,
                                truncate_text(&err.detail)
                            ));
                            continue;
                        }
                    };
                    info!(
                        step_id = %step.id,
                        iteration = iteration,
                        output_keys = ?params.output_keys,
                        "agent step completed via materialized finish"
                    );
                    report_agent_progress(ctx, step, "note", None, "agent completed").await;
                    return ActionResult::success_with(filtered);
                }
            }
        }

        if let Ok(filtered) = materialize_final_exports(&params, &evidence) {
            info!(
                step_id = %step.id,
                output_keys = ?params.output_keys,
                "agent step completed via deterministic materialization"
            );
            report_agent_progress(
                ctx,
                step,
                "note",
                None,
                "agent completed via deterministic materialization",
            )
            .await;
            return ActionResult::success_with(filtered);
        }

        let mut forced_finalization_failure: Option<String> = None;
        if !observations.is_empty() {
            match attempt_forced_finalization(
                &self.client,
                &self.config,
                ForcedFinalizationContext {
                    step,
                    params: &params,
                    ctx,
                    observations: &observations,
                    last_success_exports: last_success_exports.as_ref(),
                    evidence: &evidence,
                },
            )
            .await
            {
                Ok(filtered) => {
                    info!(
                        step_id = %step.id,
                        output_keys = ?params.output_keys,
                        "agent step completed via forced finalization"
                    );
                    report_agent_progress(
                        ctx,
                        step,
                        "note",
                        None,
                        "agent completed via forced finalization",
                    )
                    .await;
                    return ActionResult::success_with(filtered);
                }
                Err(err) => {
                    warn!(
                        step_id = %step.id,
                        error = %err,
                        "forced agent finalization failed"
                    );
                    report_agent_progress(
                        ctx,
                        step,
                        "note",
                        None,
                        format!("forced finalization failed: {}", truncate_text(&err)),
                    )
                    .await;
                    forced_finalization_failure = Some(err);
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
        let mut error_message = format!(
            "agent step '{}' reached max_iterations without a valid final result; recent_observations={}",
            step.id,
            observation_tail
        );
        if let Some(reason) = forced_finalization_failure {
            let _ = write!(
                error_message,
                "; forced_finalization={}",
                truncate_text(&reason)
            );
        }
        ActionResult::error(error_message)
    }
}

async fn attempt_forced_finalization<C: LlmClient>(
    client: &C,
    config: &LlmAgentExecutorConfig,
    req: ForcedFinalizationContext<'_>,
) -> Result<HashMap<String, Value>, String> {
    report_agent_progress(
        req.ctx,
        req.step,
        "note",
        None,
        "attempting forced finalization",
    )
    .await;

    let request = LlmRequest {
        system: build_agent_finalization_system_prompt(req.params, req.ctx),
        user: build_agent_finalization_user_prompt(
            req.observations,
            req.last_success_exports,
            req.evidence,
        ),
        model: config.model.clone(),
        temperature: config.temperature,
    };
    debug!(
        step_id = %req.step.id,
        model = %config.model,
        temperature = config.temperature,
        system_prompt = %truncate_log_text(&request.system),
        user_prompt = %truncate_log_text(&request.user),
        "agent forced finalization request"
    );

    let response = client
        .complete_with_tools(request, &agent_final_tool_definitions())
        .await
        .map_err(|err| format!("llm call failed: {}", err))?;
    let decision = match response {
        LlmResponse::ToolCall {
            name, arguments, ..
        } => parse_tool_call_decision(&name, arguments)
            .map_err(|err| format!("tool call parse failed: {}", err))?,
        LlmResponse::Text(raw) => {
            let json = extract_json(&raw).unwrap_or(raw);
            parse_agent_decision(&json).map_err(|err| format!("decision parse failed: {}", err))?
        }
    };

    match decision {
        AgentDecision::Finish => {
            materialize_final_exports(req.params, req.evidence).map_err(|err| err.summary())
        }
        AgentDecision::Final { .. } => {
            materialize_final_exports(req.params, req.evidence).map_err(|err| err.summary())
        }
        AgentDecision::Action { name, .. } => Err(format!(
            "finalization returned action '{}' instead of finish/return_final",
            name
        )),
    }
}

fn parse_output_rules(
    step_id: &str,
    output_rules: Option<&Value>,
    output_sources_legacy: Option<&Value>,
    output_keys: &[String],
) -> Result<HashMap<String, AgentOutputRule>, String> {
    let mut out = HashMap::new();
    if let Some(value) = output_rules {
        let parsed = parse_output_rules_object(step_id, "output_rules", value, output_keys)?;
        out.extend(parsed);
    }

    if let Some(value) = output_sources_legacy {
        let parsed = parse_legacy_output_sources(step_id, value, output_keys)?;
        for (key, rule) in parsed {
            out.entry(key).or_insert(rule);
        }
    }

    Ok(out)
}

fn parse_output_rules_object(
    step_id: &str,
    field_name: &str,
    value: &Value,
    output_keys: &[String],
) -> Result<HashMap<String, AgentOutputRule>, String> {
    let Some(obj) = value.as_object() else {
        return Err(format!(
            "invalid agent params in step '{}': {} must be an object",
            step_id, field_name
        ));
    };

    let allowed = output_keys
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let mut out = HashMap::with_capacity(obj.len());
    for (key, raw_rule) in obj {
        if !allowed.contains(key.as_str()) {
            return Err(format!(
                "invalid agent params in step '{}': {} key '{}' is not declared in output_keys",
                step_id, field_name, key
            ));
        }
        let rule = parse_output_rule_entry(step_id, key, raw_rule)?;
        out.insert(key.clone(), rule);
    }
    Ok(out)
}

fn parse_output_rule_entry(
    step_id: &str,
    output_key: &str,
    value: &Value,
) -> Result<AgentOutputRule, String> {
    let Some(obj) = value.as_object() else {
        return Err(format!(
            "invalid agent params in step '{}': output_rules.{} must be an object",
            step_id, output_key
        ));
    };

    let mut candidates = Vec::new();
    let required_action =
        parse_required_action_entry(step_id, &format!("output_rules.{}", output_key), obj)?;
    if let Some(slot) = parse_rule_slot(step_id, output_key, obj.get("slot"))? {
        let path = parse_rule_path(
            step_id,
            output_key,
            obj.get("path").or_else(|| obj.get("field")),
        )?;
        candidates.push(AgentOutputCandidate {
            slot,
            path,
            required_action: None,
        });
    }

    if let Some(raw_candidates) = obj.get("candidates") {
        let arr = raw_candidates.as_array().ok_or_else(|| {
            format!(
                "invalid agent params in step '{}': output_rules.{}.candidates must be an array",
                step_id, output_key
            )
        })?;
        for raw_candidate in arr {
            candidates.push(parse_output_candidate_entry(
                step_id,
                output_key,
                raw_candidate,
            )?);
        }
    }

    let template = obj
        .get("template")
        .map(|v| {
            v.as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    format!(
                        "invalid agent params in step '{}': output_rules.{}.template must be a non-empty string",
                        step_id, output_key
                    )
                })
        })
        .transpose()?;

    let fallback_aliases = obj
        .get("fallback_aliases")
        .map(|v| {
            v.as_array()
                .ok_or_else(|| {
                    format!(
                        "invalid agent params in step '{}': output_rules.{}.fallback_aliases must be an array",
                        step_id, output_key
                    )
                })?
                .iter()
                .map(|item| {
                    item.as_str()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string)
                        .ok_or_else(|| {
                            format!(
                                "invalid agent params in step '{}': output_rules.{}.fallback_aliases must contain non-empty strings",
                                step_id, output_key
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();

    if candidates.is_empty() && template.is_none() && fallback_aliases.is_empty() {
        return Err(format!(
            "invalid agent params in step '{}': output_rules.{} must provide slot/candidates/template/fallback_aliases",
            step_id, output_key
        ));
    }

    Ok(AgentOutputRule {
        candidates,
        template,
        fallback_aliases,
        required_action,
    })
}

fn parse_output_candidate_entry(
    step_id: &str,
    output_key: &str,
    value: &Value,
) -> Result<AgentOutputCandidate, String> {
    let Some(obj) = value.as_object() else {
        return Err(format!(
            "invalid agent params in step '{}': output_rules.{}.candidates[] must be an object",
            step_id, output_key
        ));
    };
    let slot = parse_rule_slot(step_id, output_key, obj.get("slot"))?.ok_or_else(|| {
        format!(
            "invalid agent params in step '{}': output_rules.{}.candidates[].slot is required",
            step_id, output_key
        )
    })?;
    let path = parse_rule_path(step_id, output_key, obj.get("path"))?;
    let required_action = parse_required_action_entry(
        step_id,
        &format!("output_rules.{}.candidates[]", output_key),
        obj,
    )?;
    if slot == "outputs" && required_action.is_some() {
        return Err(format!(
            "invalid agent params in step '{}': output_rules.{}.candidates[].requires.action cannot target slot 'outputs'",
            step_id, output_key
        ));
    }
    Ok(AgentOutputCandidate {
        slot,
        path,
        required_action,
    })
}

fn parse_rule_slot(
    step_id: &str,
    output_key: &str,
    value: Option<&Value>,
) -> Result<Option<String>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let slot = value
        .as_str()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            format!(
                "invalid agent params in step '{}': output_rules.{}.slot must be a non-empty string",
                step_id, output_key
            )
        })?;
    Ok(Some(slot.to_string()))
}

fn parse_rule_path(
    step_id: &str,
    output_key: &str,
    value: Option<&Value>,
) -> Result<Vec<AgentPathSegment>, String> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let raw = value.as_str().map(str::trim).ok_or_else(|| {
        format!(
            "invalid agent params in step '{}': output_rules.{}.path must be a string",
            step_id, output_key
        )
    })?;
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    parse_path_segments(raw).ok_or_else(|| {
        format!(
            "invalid agent params in step '{}': output_rules.{}.path has invalid syntax",
            step_id, output_key
        )
    })
}

fn parse_required_action_entry(
    step_id: &str,
    scope: &str,
    obj: &serde_json::Map<String, Value>,
) -> Result<Option<String>, String> {
    let direct = obj.get("requires_action");
    let structured = obj.get("requires");
    if direct.is_some() && structured.is_some() {
        return Err(format!(
            "invalid agent params in step '{}': {} cannot include both requires and requires_action",
            step_id, scope
        ));
    }

    if let Some(value) = direct {
        let action = value
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                format!(
                    "invalid agent params in step '{}': {}.requires_action must be a non-empty string",
                    step_id, scope
                )
            })?;
        return Ok(Some(action.to_string()));
    }

    let Some(value) = structured else {
        return Ok(None);
    };
    let requires = value.as_object().ok_or_else(|| {
        format!(
            "invalid agent params in step '{}': {}.requires must be an object",
            step_id, scope
        )
    })?;
    let action = requires.get("action").ok_or_else(|| {
        format!(
            "invalid agent params in step '{}': {}.requires.action is required",
            step_id, scope
        )
    })?;
    let action = action
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            format!(
                "invalid agent params in step '{}': {}.requires.action must be a non-empty string",
                step_id, scope
            )
        })?;
    Ok(Some(action.to_string()))
}

fn parse_legacy_output_sources(
    step_id: &str,
    value: &Value,
    output_keys: &[String],
) -> Result<HashMap<String, AgentOutputRule>, String> {
    parse_output_rules_object(step_id, "output_sources", value, output_keys)
}

fn parse_optional_slot_name(scope: &str, value: Option<&Value>) -> Result<Option<String>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let name = value
        .as_str()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| format!("{}: save_as must be a non-empty string", scope))?;
    Ok(Some(name.to_string()))
}

fn parse_optional_capture_mode(
    scope: &str,
    value: Option<&Value>,
) -> Result<Option<AgentCaptureMode>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let mode = value
        .as_str()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| format!("{}: capture must be a non-empty string", scope))?;
    match mode.to_ascii_lowercase().as_str() {
        "json_stdout" => Ok(Some(AgentCaptureMode::JsonStdout)),
        other => Err(format!("{}: unsupported capture mode '{}'", scope, other)),
    }
}

fn derive_evidence_value(
    exports: &HashMap<String, Value>,
    capture: Option<&AgentCaptureMode>,
) -> Result<Value, String> {
    match capture {
        Some(AgentCaptureMode::JsonStdout) => {
            let stdout = exports
                .get("stdout")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "capture json_stdout requires string stdout".to_string())?;
            serde_json::from_str::<Value>(stdout)
                .map_err(|e| format!("capture json_stdout invalid JSON: {}", e))
        }
        None => Ok(exports_to_value(exports)),
    }
}

pub(crate) fn default_action_preflight_hook() -> ActionPreflightHook {
    Arc::new(|action_name: &str, params: &Value| action_preflight_error(action_name, params))
}

fn action_preflight_error(action_name: &str, params: &Value) -> Option<String> {
    if action_name != "shell" {
        return None;
    }
    let obj = params.as_object()?;
    let command = obj.get("command")?.as_str()?.trim();
    if command.is_empty() {
        return None;
    }
    let command_token = shell_command_first_token(command);
    let command_for_path_check = command_token.unwrap_or(command);

    // Shell command explicitly points to a non-existing executable path.
    if command_for_path_check.contains('/')
        && !std::path::Path::new(command_for_path_check).exists()
    {
        return Some(format!(
            "command path '{}' does not exist",
            command_for_path_check
        ));
    }

    let command_name = std::path::Path::new(command_for_path_check)
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or(command_for_path_check);
    let is_python = command_name.starts_with("python");
    if !is_python {
        return None;
    }

    let args = obj.get("args").and_then(|v| v.as_array())?;
    let script = args.iter().find_map(|value| {
        value
            .as_str()
            .map(str::trim)
            .filter(|arg| !arg.is_empty() && arg.ends_with(".py"))
    })?;

    let path = std::path::Path::new(script);
    let base_dir = resolve_preflight_base_dir(obj);
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    };
    if abs.exists() {
        None
    } else {
        Some(format!("python script '{}' does not exist", abs.display()))
    }
}

fn shell_command_first_token(command: &str) -> Option<&str> {
    command.split_whitespace().next().map(str::trim)
}

fn resolve_preflight_base_dir(obj: &serde_json::Map<String, Value>) -> std::path::PathBuf {
    if let Some(workdir) = obj.get("workdir").and_then(|v| v.as_str()) {
        let workdir = workdir.trim();
        if !workdir.is_empty() {
            let workdir_path = std::path::Path::new(workdir);
            if workdir_path.is_absolute() {
                return workdir_path.to_path_buf();
            }
            if let Ok(cwd) = std::env::current_dir() {
                return cwd.join(workdir_path);
            }
        }
    }
    std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."))
}

fn deterministic_action_error_reason(message: &str) -> Option<&'static str> {
    let lower = message.to_ascii_lowercase();
    if lower.contains("failed preflight") {
        return Some("preflight_rejected");
    }
    if lower.contains("no such file or directory") || lower.contains("can't open file") {
        return Some("missing_file_or_path");
    }
    if lower.contains("does not exist") {
        return Some("missing_file_or_path");
    }
    if lower.contains("command not found") {
        return Some("missing_command");
    }
    None
}

fn exports_to_value(exports: &HashMap<String, Value>) -> Value {
    Value::Object(
        exports
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<serde_json::Map<_, _>>(),
    )
}

fn parse_json_export_field(exports: &HashMap<String, Value>, field: &str) -> Option<Value> {
    let raw = exports.get(field)?.as_str()?;
    serde_json::from_str(raw).ok()
}

fn materialize_final_exports(
    params: &AgentStepParams,
    evidence: &AgentEvidenceStore,
) -> Result<HashMap<String, Value>, MaterializeError> {
    let mut out = HashMap::with_capacity(params.output_keys.len());
    for key in &params.output_keys {
        let value = materialize_output_key(key, params, evidence, &out)?;
        out.insert(key.clone(), value);
    }
    Ok(out)
}

fn materialize_output_key(
    key: &str,
    params: &AgentStepParams,
    evidence: &AgentEvidenceStore,
    resolved_outputs: &HashMap<String, Value>,
) -> Result<Value, MaterializeError> {
    if let Some(rule) = params.output_rules.get(key) {
        if let Some(value) =
            resolve_output_from_rule(key, rule, params, evidence, resolved_outputs)?
        {
            return Ok(value);
        }
        // When explicit output_rules exist for a key, avoid generic heuristic fallback.
        // This prevents unrelated later actions (for example file_read.path) from being
        // materialized as required outputs like `updated_file_path`.
        let required_action = rule_required_action(rule);
        if let Some(value) =
            resolve_slot_with_requirement(key, key, evidence, required_action.as_deref())?
        {
            return Ok(value);
        }
        if !rule.fallback_aliases.is_empty() {
            let aliases = output_rule_fallback_aliases(key, rule);
            if let Some(value) =
                find_candidate_in_evidence(evidence, &aliases, required_action.as_deref())
            {
                return Ok(value);
            }
            let code = if required_action.is_some() {
                "missing_requires"
            } else {
                "rule_unresolved"
            };
            return Err(MaterializeError::new(
                key,
                code,
                format!(
                    "fallback aliases {:?} produced no value{}",
                    aliases,
                    required_action_suffix(required_action.as_deref())
                ),
            ));
        }
        let code = if required_action.is_some() {
            "missing_requires"
        } else {
            "rule_unresolved"
        };
        return Err(MaterializeError::new(
            key,
            code,
            format!(
                "output_rules could not materialize value{}",
                required_action_suffix(required_action.as_deref())
            ),
        ));
    }

    if let Some(value) = resolve_slot_with_requirement(key, key, evidence, None)? {
        return Ok(value);
    }

    let aliases = output_key_aliases(key, None);
    if let Some(value) = find_candidate_in_evidence(evidence, &aliases, None) {
        return Ok(value);
    }
    Err(MaterializeError::new(
        key,
        "missing_evidence",
        format!("no evidence found for aliases {:?}", aliases),
    ))
}

fn resolve_slot_with_requirement(
    output_key: &str,
    slot: &str,
    evidence: &AgentEvidenceStore,
    required_action: Option<&str>,
) -> Result<Option<Value>, MaterializeError> {
    let Some(value) = evidence.slots.get(slot) else {
        return Ok(None);
    };
    if let Some(required_action) = required_action {
        let actual = evidence.slot_actions.get(slot).map(String::as_str);
        if actual != Some(required_action) {
            return Err(MaterializeError::new(
                output_key,
                "slot_provenance_mismatch",
                format!(
                    "slot '{}' was produced by {:?}, expected '{}'{}",
                    slot,
                    actual,
                    required_action,
                    required_action_suffix(Some(required_action))
                ),
            ));
        }
    }
    Ok(Some(value.clone()))
}

fn required_action_suffix(required_action: Option<&str>) -> String {
    match required_action {
        Some(action) => format!(" (requires.action='{}')", action),
        None => String::new(),
    }
}

fn find_candidate_in_record(
    record: &AgentEvidenceRecord,
    aliases: &[String],
    required_action: Option<&str>,
) -> Option<Value> {
    if let Some(required_action) = required_action {
        if record.action_name != required_action {
            return None;
        }
    }
    for field in ["stdout", "body", "result"] {
        if let Some(parsed) = parse_json_field_from_value(&record.value, field) {
            if let Some(value) = find_candidate_in_value_or_wrapped(&parsed, aliases) {
                return Some(value);
            }
        }
    }
    if let Some(value) = find_candidate_in_value_or_wrapped(&record.value, aliases) {
        return Some(value);
    }
    for alias in aliases {
        if let Some(value) = record.raw_exports.get(alias.as_str()) {
            return Some(value.clone());
        }
    }
    for field in ["stdout", "body", "result"] {
        if let Some(parsed) = parse_json_export_field(&record.raw_exports, field) {
            if let Some(value) = find_candidate_in_value_or_wrapped(&parsed, aliases) {
                return Some(value);
            }
        }
    }
    None
}

fn find_candidate_in_evidence(
    evidence: &AgentEvidenceStore,
    aliases: &[String],
    required_action: Option<&str>,
) -> Option<Value> {
    for record in evidence.records.iter().rev() {
        if let Some(value) = find_candidate_in_record(record, aliases, required_action) {
            return Some(value);
        }
    }

    if let Some(last_raw) = evidence.slot_value_with_requirement("last_raw", required_action) {
        if let Some(value) = find_candidate_in_value_or_wrapped(&last_raw, aliases) {
            return Some(value);
        }
    }
    if let Some(last_stdout_json) =
        evidence.slot_value_with_requirement("last_stdout_json", required_action)
    {
        if let Some(value) = find_candidate_in_value_or_wrapped(&last_stdout_json, aliases) {
            return Some(value);
        }
    }
    if let Some(last_body_json) =
        evidence.slot_value_with_requirement("last_body_json", required_action)
    {
        if let Some(value) = find_candidate_in_value_or_wrapped(&last_body_json, aliases) {
            return Some(value);
        }
    }

    None
}

fn resolve_output_from_rule(
    key: &str,
    rule: &AgentOutputRule,
    params: &AgentStepParams,
    evidence: &AgentEvidenceStore,
    resolved_outputs: &HashMap<String, Value>,
) -> Result<Option<Value>, MaterializeError> {
    let mut first_error: Option<MaterializeError> = None;
    for candidate in &rule.candidates {
        let required_action = candidate_required_action(rule, candidate, params);
        match resolve_output_candidate(key, candidate, evidence, resolved_outputs, required_action)
        {
            Ok(Some(value)) => return Ok(Some(value)),
            Ok(None) => {}
            Err(err) => {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
    }
    if let Some(template) = &rule.template {
        if let Some(rendered) = render_output_template(template, evidence, resolved_outputs) {
            return Ok(Some(Value::String(rendered)));
        }
    }
    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(None)
}

fn resolve_output_candidate(
    output_key: &str,
    candidate: &AgentOutputCandidate,
    evidence: &AgentEvidenceStore,
    resolved_outputs: &HashMap<String, Value>,
    required_action: Option<&str>,
) -> Result<Option<Value>, MaterializeError> {
    let root = if candidate.slot == "outputs" {
        if required_action.is_some() {
            return Err(MaterializeError::new(
                output_key,
                "missing_requires",
                format!(
                    "slot 'outputs' cannot satisfy{}",
                    required_action_suffix(required_action)
                ),
            ));
        }
        Some(Value::Object(
            resolved_outputs
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        ))
    } else {
        resolve_slot_with_requirement(output_key, &candidate.slot, evidence, required_action)?
    };
    let Some(root) = root else {
        return Ok(None);
    };
    if candidate.path.is_empty() {
        Ok(Some(root))
    } else {
        match resolve_path_value(&root, &candidate.path) {
            Some(value) => Ok(Some(value)),
            None => Err(MaterializeError::new(
                output_key,
                "rule_unresolved",
                format!(
                    "slot '{}' does not contain path '{}'{}",
                    candidate.slot,
                    render_path_segments(&candidate.path),
                    required_action_suffix(required_action)
                ),
            )),
        }
    }
}

fn rule_required_action(rule: &AgentOutputRule) -> Option<String> {
    rule.required_action
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
}

fn candidate_required_action<'a>(
    rule: &'a AgentOutputRule,
    candidate: &'a AgentOutputCandidate,
    params: &'a AgentStepParams,
) -> Option<&'a str> {
    candidate
        .required_action
        .as_deref()
        .or(rule.required_action.as_deref())
        .or_else(|| {
            if candidate.slot != "outputs"
                && params.allowed_actions.contains(candidate.slot.as_str())
            {
                Some(candidate.slot.as_str())
            } else {
                None
            }
        })
}

fn resolve_path_value(value: &Value, path: &[AgentPathSegment]) -> Option<Value> {
    if path.is_empty() {
        return Some(value.clone());
    }
    let mut current = value;
    for segment in path {
        current = match segment {
            AgentPathSegment::Key(key) => current.get(key)?,
            AgentPathSegment::Index(idx) => current.as_array()?.get(*idx)?,
        };
    }
    Some(current.clone())
}

fn find_candidate_in_value(value: &Value, aliases: &[String]) -> Option<Value> {
    let obj = value.as_object()?;
    for alias in aliases {
        if let Some(found) = obj.get(alias.as_str()) {
            return Some(found.clone());
        }
    }
    None
}

fn find_candidate_in_value_or_wrapped(value: &Value, aliases: &[String]) -> Option<Value> {
    if let Some(value) = find_candidate_in_value(value, aliases) {
        return Some(value);
    }
    let obj = value.as_object()?;
    for wrapper in ["data", "result", "payload", "output", "response"] {
        if let Some(nested) = obj.get(wrapper) {
            if let Some(value) = find_candidate_in_value(nested, aliases) {
                return Some(value);
            }
        }
    }
    None
}

fn parse_json_field_from_value(value: &Value, field: &str) -> Option<Value> {
    let obj = value.as_object()?;
    let raw = obj.get(field)?.as_str()?;
    serde_json::from_str(raw).ok()
}

fn output_key_aliases(key: &str, rule: Option<&AgentOutputRule>) -> Vec<String> {
    let mut aliases = vec![key.to_string()];
    if key.ends_with("_path") {
        aliases.extend(["path", "output_path", "file_path"].map(str::to_string));
    }
    if key == "summary" {
        aliases.extend(["result", "message", "stdout"].map(str::to_string));
    }
    if key == "content" {
        aliases.extend(["body", "stdout", "result"].map(str::to_string));
    }
    if let Some(rule) = rule {
        aliases.extend(rule.fallback_aliases.iter().map(|alias| alias.to_string()));
    }
    dedupe_aliases(aliases)
}

fn output_rule_fallback_aliases(key: &str, rule: &AgentOutputRule) -> Vec<String> {
    let mut aliases = vec![key.to_string()];
    aliases.extend(rule.fallback_aliases.iter().map(|alias| alias.to_string()));
    dedupe_aliases(aliases)
}

fn dedupe_aliases(aliases: Vec<String>) -> Vec<String> {
    let mut deduped = Vec::with_capacity(aliases.len());
    let mut seen = HashSet::with_capacity(aliases.len());
    for alias in aliases {
        if seen.insert(alias.clone()) {
            deduped.push(alias);
        }
    }
    deduped
}

fn parse_path_segments(raw: &str) -> Option<Vec<AgentPathSegment>> {
    raw.split('.')
        .map(str::trim)
        .map(|token| {
            if token.is_empty() {
                return None;
            }
            if let Ok(index) = token.parse::<usize>() {
                Some(AgentPathSegment::Index(index))
            } else {
                Some(AgentPathSegment::Key(token.to_string()))
            }
        })
        .collect::<Option<Vec<_>>>()
}

fn render_path_segments(path: &[AgentPathSegment]) -> String {
    path.iter()
        .map(|segment| match segment {
            AgentPathSegment::Key(key) => key.clone(),
            AgentPathSegment::Index(index) => index.to_string(),
        })
        .collect::<Vec<_>>()
        .join(".")
}

fn render_output_template(
    template: &str,
    evidence: &AgentEvidenceStore,
    resolved_outputs: &HashMap<String, Value>,
) -> Option<String> {
    let mut out = String::new();
    let mut cursor = 0usize;
    let mut unresolved = false;

    while let Some(start_rel) = template[cursor..].find("{{") {
        let start = cursor + start_rel;
        out.push_str(&template[cursor..start]);
        let open_end = start + 2;
        let Some(end_rel) = template[open_end..].find("}}") else {
            unresolved = true;
            break;
        };
        let end = open_end + end_rel;
        let token = template[open_end..end].trim();
        if let Some(value) = resolve_template_token(token, evidence, resolved_outputs) {
            out.push_str(&template_value_to_text(&value));
        } else {
            unresolved = true;
        }
        cursor = end + 2;
    }
    out.push_str(&template[cursor..]);

    if unresolved {
        None
    } else {
        Some(out)
    }
}

fn resolve_template_token(
    token: &str,
    evidence: &AgentEvidenceStore,
    resolved_outputs: &HashMap<String, Value>,
) -> Option<Value> {
    if let Some(raw) = token.strip_prefix("slot:") {
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }
        let mut parts = raw.splitn(2, '.');
        let slot = parts.next()?.trim();
        if slot.is_empty() {
            return None;
        }
        let root = evidence.slots.get(slot)?;
        if let Some(path_text) = parts.next() {
            let path = parse_path_segments(path_text.trim())?;
            return resolve_path_value(root, &path);
        }
        return Some(root.clone());
    }

    if let Some(value) = resolved_outputs.get(token) {
        return Some(value.clone());
    }
    evidence.slots.get(token).cloned()
}

fn template_value_to_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        _ => value.to_string(),
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

    let output_rules = parse_output_rules(
        step_id,
        obj.get("output_rules"),
        obj.get("output_sources"),
        &output_keys,
    )?;

    let bound_inputs = obj
        .iter()
        .filter_map(|(key, value)| {
            if matches!(
                key.as_str(),
                "goal"
                    | "allowed_actions"
                    | "max_iterations"
                    | "output_keys"
                    | "output_rules"
                    | "output_sources"
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
        output_rules,
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
    let output_rules_block = build_output_rules_block(&params.output_rules);
    let env_block = build_execution_environment_block(ctx);
    let skill_block = build_skill_knowledge_block(&ctx.skill_instructions);
    format!(
        "You are a constrained execution agent.\n\
Goal: {goal}\n\
Allowed actions: {actions}\n\
Required output keys: {output_keys}.\n\
{sources}{env}{skill}{bound}\n\
You have three tools:\n\
1) execute_action — run one of the allowed actions. Optional save_as stores the result in a named slot; capture=json_stdout parses stdout as JSON before storing. Example: execute_action({{\"action\":\"shell\",\"params\":{{\"command\":\"ls\"}},\"save_as\":\"listing\"}})\n\
2) finish — ask runtime to materialize required output keys from saved evidence. Prefer this when saved slots already contain the needed values. Example: finish({{}})\n\
3) return_final — completion signal when you believe runtime can now materialize outputs from evidence. Runtime does not trust explicit exports without evidence. Example: return_final({{\"exports\":{{{output_keys_example}}}}})\n\
\n\
Rules:\n\
- Act first, analyze minimally. Do not spend iterations only reading/analyzing.\n\
- Never call actions outside the allowed list.\n\
- Runtime always stores evidence slots: last, last_raw, last_action, action:<name>, action:<name>:raw. If stdout/body is JSON, runtime also stores action:<name>:stdout_json/body_json.\n\
- Save useful structured results with save_as so runtime can materialize outputs deterministically.\n\
- If output_rules specify requires.action, that slot evidence must be produced by the required action.\n\
- Action-named slots (for example slot 'file_write') are provenance-checked against that action.\n\
- If a command prints one JSON object to stdout, use capture=json_stdout.\n\
- Never use file_write with empty content as a marker step. If you only need to expose a path or metadata, save structured evidence with save_as and call finish.\n\
- Never invent script file names. Use script paths discovered in Activated Skills, or inspect the scripts directory first.\n\
- Missing file/command shell errors are treated as deterministic and may abort the step early.\n\
- When the required outputs can be derived from saved evidence, call finish immediately.\n\
- You MUST call finish or return_final before iterations run out.",
        goal = params.goal,
        actions = actions.join(", "),
        output_keys = params.output_keys.join(", "),
        sources = output_rules_block,
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
    out.push_str(
        "Only execute scripts that are explicitly listed below or verified at runtime; do not guess script filenames.\n",
    );
    for skill in skills {
        let _ = writeln!(out, "- {}: {}", skill.skill_name, skill.instructions.trim());
        if let Some(path) = &skill.skill_path {
            let _ = writeln!(out, "  [skill file: {}]", path);
        }
        if let Some(dir) = &skill.scripts_dir {
            let _ = writeln!(out, "  [scripts: {}]", dir);
            let discovered = discover_skill_scripts(dir, MAX_DISCOVERED_SKILL_SCRIPTS);
            if discovered.is_empty() {
                let _ = writeln!(out, "  [scripts discovered: none]");
            } else {
                let _ = writeln!(out, "  [scripts discovered: {}]", discovered.join(", "));
            }
        }
    }
    out
}

fn discover_skill_scripts(dir: &str, limit: usize) -> Vec<String> {
    let root = std::path::Path::new(dir);
    if !root.is_dir() || limit == 0 {
        return Vec::new();
    }

    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(current) = stack.pop() {
        let Ok(read_dir) = std::fs::read_dir(&current) else {
            continue;
        };
        let mut entries = read_dir
            .filter_map(|entry| entry.ok().map(|v| v.path()))
            .collect::<Vec<_>>();
        entries.sort();
        for path in entries {
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if !path.is_file() {
                continue;
            }
            let extension = path
                .extension()
                .and_then(|v| v.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();
            if !matches!(extension.as_str(), "py" | "sh" | "js" | "ts" | "rb") {
                continue;
            }
            let display = path
                .strip_prefix(root)
                .unwrap_or(path.as_path())
                .display()
                .to_string();
            out.push(display);
            if out.len() >= limit {
                out.sort_unstable();
                return out;
            }
        }
    }

    out.sort_unstable();
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

fn build_output_rules_block(output_rules: &HashMap<String, AgentOutputRule>) -> String {
    if output_rules.is_empty() {
        return String::new();
    }

    let mut keys = output_rules.keys().cloned().collect::<Vec<_>>();
    keys.sort_unstable();
    let mut lines = Vec::with_capacity(keys.len() + 1);
    lines.push("Runtime output materialization rules:".to_string());
    for key in keys {
        let Some(rule) = output_rules.get(&key) else {
            continue;
        };
        let mut parts = Vec::new();
        for candidate in &rule.candidates {
            let mut repr = format!("slot '{}'", candidate.slot);
            if !candidate.path.is_empty() {
                repr.push_str(" path '");
                repr.push_str(&render_path_segments(&candidate.path));
                repr.push('\'');
            }
            if let Some(required_action) = &candidate.required_action {
                repr.push_str(" requires.action '");
                repr.push_str(required_action);
                repr.push('\'');
            }
            parts.push(repr);
        }
        if let Some(template) = &rule.template {
            parts.push(format!("template {:?}", truncate_text(template)));
        }
        if !rule.fallback_aliases.is_empty() {
            parts.push(format!("fallback_aliases={:?}", rule.fallback_aliases));
        }
        if let Some(required_action) = &rule.required_action {
            parts.push(format!("requires.action='{}'", required_action));
        }
        if parts.is_empty() {
            lines.push(format!("- {} <= <empty rule>", key));
        } else {
            lines.push(format!("- {} <= {}", key, parts.join(" | ")));
        }
    }
    lines.push(
        "Save evidence into the referenced slots and prefer finish once rules can materialize required outputs."
            .to_string(),
    );
    format!("{}\n", lines.join("\n"))
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

fn build_agent_finalization_system_prompt(
    params: &AgentStepParams,
    ctx: &ExecutorContext,
) -> String {
    let mut base = build_agent_system_prompt(params, ctx);
    base.push_str(
        "\n\nFinalization Mode:\n- Action iterations are exhausted.\n- execute_action is no longer available.\n- Prefer finish if runtime can materialize outputs from saved evidence.\n- Use return_final only when deterministic materialization is insufficient.\n",
    );
    base
}

fn build_agent_finalization_user_prompt(
    observations: &[String],
    last_success_exports: Option<&HashMap<String, Value>>,
    evidence: &AgentEvidenceStore,
) -> String {
    let mut lines = vec![
        "All action iterations are exhausted. No more execute_action calls are allowed."
            .to_string(),
    ];

    if let Some(exports) = last_success_exports {
        let serialized = Value::Object(
            exports
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<serde_json::Map<_, _>>(),
        )
        .to_string();
        lines.push(format!(
            "last_success_exports={}",
            truncate_with_limit(&serialized, MAX_BOUND_INPUT_VALUE_CHARS)
        ));
    }

    let mut slot_names = evidence.slots.keys().cloned().collect::<Vec<_>>();
    slot_names.sort_unstable();
    if slot_names.is_empty() {
        lines.push("saved_slots: none".to_string());
    } else {
        lines.push(format!("saved_slots: {}", slot_names.join(", ")));
    }

    if observations.is_empty() {
        lines.push("recent_observations: none".to_string());
    } else {
        let tail = observations
            .iter()
            .rev()
            .take(MAX_RECENT_OBSERVATIONS)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>();
        lines.push("recent_observations:".to_string());
        for item in tail {
            lines.push(format!("- {}", item));
        }
    }
    lines.push(
        "Call finish now if saved evidence is sufficient; otherwise call return_final.".to_string(),
    );
    lines.join("\n")
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
                    },
                    "save_as": {
                        "type": "string",
                        "description": "Optional evidence slot name for storing this action result"
                    },
                    "capture": {
                        "type": "string",
                        "description": "Optional capture mode. Use 'json_stdout' when stdout is exactly one JSON object."
                    }
                },
                "required": ["action", "params"]
            }),
        },
        ToolDefinition {
            name: "finish".to_string(),
            description:
                "Finish by asking runtime to materialize required output keys from saved evidence"
                    .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {}
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

fn agent_final_tool_definitions() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "finish".to_string(),
            description:
                "Finish by asking runtime to materialize required output keys from saved evidence"
                    .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {}
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
            let save_as = parse_optional_slot_name("execute_action", args.get("save_as"))?;
            let capture = parse_optional_capture_mode("execute_action", args.get("capture"))?;
            Ok(AgentDecision::Action {
                name: action,
                params,
                save_as,
                capture,
            })
        }
        "finish" => Ok(AgentDecision::Finish),
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
            let save_as = parse_optional_slot_name("action", obj.get("save_as"))?;
            let capture = parse_optional_capture_mode("action", obj.get("capture"))?;
            Ok(AgentDecision::Action {
                name,
                params,
                save_as,
                capture,
            })
        }
        "finish" => Ok(AgentDecision::Finish),
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

    #[test]
    fn test_materialize_output_rules_do_not_fallback_to_generic_aliases() {
        let params = AgentStepParams {
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
    fn test_materialize_output_rules_allow_explicit_fallback_aliases() {
        let params = AgentStepParams {
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
        let tools = agent_tool_definitions();
        assert_eq!(tools.len(), 3);
        assert_eq!(tools[0].name, "execute_action");
        assert_eq!(tools[1].name, "finish");
        assert_eq!(tools[2].name, "return_final");
    }
}
