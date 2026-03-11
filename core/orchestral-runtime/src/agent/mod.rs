use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

use async_trait::async_trait;
use orchestral_core::action::ActionResult;
#[cfg(test)]
use orchestral_core::action::{ActionContext, ActionInput};
use orchestral_core::executor::{
    execute_action_with_registry_with_options, ActionExecutionOptions, ActionRegistry,
    AgentStepExecutor, ExecutorContext,
};
use orchestral_core::planner::SkillInstruction;
use orchestral_core::types::Step;
use serde_json::Value;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::planner::{LlmClient, LlmRequest, LlmResponse, ToolDefinition};

mod finalize;
mod materialize;
mod parsing;
mod preflight;
mod progress;
mod prompt;
mod types;

use self::finalize::{attempt_forced_finalization, ForcedFinalizationContext};
use self::materialize::{
    derive_evidence_value, materialize_final_exports, validate_agent_action_success,
};
#[cfg(test)]
use self::materialize::{parse_path_segments, render_path_segments};
use self::parsing::{
    extract_json, normalize_agent_action_params, parse_agent_decision, parse_agent_params,
    parse_tool_call_decision,
};
#[cfg(test)]
use self::preflight::action_preflight_error;
pub(crate) use self::preflight::default_action_preflight_hook;
use self::preflight::deterministic_action_error_reason;
use self::progress::report_agent_progress;
use self::prompt::{
    agent_tool_definitions, build_agent_system_prompt, build_agent_user_prompt,
    log_agent_debug_text_payloads, summarize_recent_observations, truncate_log_text, truncate_text,
};
#[cfg(test)]
use self::prompt::{
    build_bound_inputs_block, collect_agent_debug_text_payloads, leaf_execute_action_schema,
    summarize_bound_input_value,
};
use self::types::{AgentDecision, AgentEvidenceStore, AgentStepParams};

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
            mode = %params.mode.as_str(),
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

            let tools = agent_tool_definitions(&params);
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
                    log_agent_debug_text_payloads(
                        step.id.as_ref(),
                        iteration,
                        "tool_call_arguments",
                        arguments,
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
                    params: mut action_params,
                    save_as,
                    capture,
                } => {
                    info!(
                        step_id = %step.id,
                        iteration = iteration,
                        action = %action_name,
                        "agent decided action"
                    );
                    log_agent_debug_text_payloads(
                        step.id.as_ref(),
                        iteration,
                        "decision_action_params",
                        &action_params,
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

                    action_params = normalize_agent_action_params(&action_name, action_params);

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
                            if let Err(err) = validate_agent_action_success(
                                &params,
                                &evidence,
                                &action_name,
                                &exports,
                                &captured_value,
                                save_as.as_deref(),
                            ) {
                                let summary = err.summary();
                                warn!(
                                    step_id = %step.id,
                                    iteration = iteration,
                                    action = %action_name,
                                    reason = %summary,
                                    "agent action produced invalid materialized output"
                                );
                                report_agent_progress(
                                    ctx,
                                    step,
                                    "action_failed",
                                    Some(action_name.as_str()),
                                    format!(
                                        "{} invalid structured output: {}",
                                        action_name,
                                        truncate_text(&summary)
                                    ),
                                )
                                .await;
                                observations.push(format!(
                                    "invalid_action_output {} => {}",
                                    action_name,
                                    truncate_text(&summary)
                                ));
                                continue;
                            }
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

#[cfg(test)]
mod tests;
