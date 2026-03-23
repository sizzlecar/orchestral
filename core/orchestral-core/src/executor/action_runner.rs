use std::sync::Arc;

use serde_json::Value;
use tokio::sync::RwLock;

use crate::action::{ActionContext, ActionInput, ActionResult};

use super::logging::{
    truncate_for_log, truncate_json_for_log, truncate_json_map_for_log, MAX_LOG_JSON_CHARS,
    MAX_LOG_TEXT_CHARS,
};
use super::schema::validate_schema;
use super::{ActionExecutionOptions, ActionRegistry, ExecutorContext};

/// Unified action execution entrypoint used by both step execution and agent subloops.
/// This centralizes registry lookup, schema validation, action run, and result logging.
pub async fn execute_action_with_registry(
    action_registry: Arc<RwLock<ActionRegistry>>,
    ctx: &ExecutorContext,
    step_id: &crate::types::StepId,
    action_name: &str,
    execution_id: &str,
    resolved_params: Value,
) -> ActionResult {
    let options = ActionExecutionOptions::default();
    execute_action_with_registry_with_options(
        action_registry,
        ctx,
        step_id,
        action_name,
        execution_id,
        resolved_params,
        &options,
    )
    .await
}

pub async fn execute_action_with_registry_with_options(
    action_registry: Arc<RwLock<ActionRegistry>>,
    ctx: &ExecutorContext,
    step_id: &crate::types::StepId,
    action_name: &str,
    execution_id: &str,
    resolved_params: Value,
    options: &ActionExecutionOptions,
) -> ActionResult {
    if let Some(preflight_hook) = options.preflight_hook.as_ref() {
        if let Some(reason) = preflight_hook(action_name, &resolved_params) {
            tracing::warn!(
                task_id = %ctx.task_id,
                step_id = %step_id,
                action = %action_name,
                reason = %truncate_for_log(&reason, MAX_LOG_TEXT_CHARS),
                "action preflight rejected"
            );
            return ActionResult::error(format!(
                "action '{}' failed preflight: {}",
                action_name, reason
            ));
        }
    }

    let action = {
        let registry = action_registry.read().await;
        registry.get(action_name)
    };

    let action = match action {
        Some(a) => a,
        None => return ActionResult::error(format!("Action '{}' not found", action_name)),
    };
    let action_meta = action.metadata();

    if let Err(error) = validate_schema(
        &resolved_params,
        &action_meta.input_schema,
        "input",
        step_id,
        action_name,
    ) {
        return ActionResult::error(error);
    }

    let input = ActionInput::with_params(resolved_params);
    if tracing::enabled!(tracing::Level::DEBUG) {
        tracing::debug!(
            task_id = %ctx.task_id,
            step_id = %step_id,
            action = %action_name,
            resolved_params = %truncate_json_for_log(&input.params, MAX_LOG_JSON_CHARS),
            "action input resolved"
        );
    }

    let action_ctx = ActionContext::new(
        ctx.task_id.clone(),
        step_id.clone(),
        execution_id.to_string(),
        ctx.working_set.clone(),
    );

    let result = action.run(input, action_ctx).await;
    match result {
        ActionResult::Success { exports } => {
            if tracing::enabled!(tracing::Level::DEBUG) {
                tracing::debug!(
                    task_id = %ctx.task_id,
                    step_id = %step_id,
                    action = %action_name,
                    exports = %truncate_json_map_for_log(&exports, MAX_LOG_JSON_CHARS),
                    "action returned success"
                );
            }
            let export_value = Value::Object(
                exports
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            );
            if let Err(error) = validate_schema(
                &export_value,
                &action_meta.output_schema,
                "output",
                step_id,
                action_name,
            ) {
                return ActionResult::error(error);
            }
            ActionResult::Success { exports }
        }
        ActionResult::NeedClarification { question } => {
            tracing::info!(
                task_id = %ctx.task_id,
                step_id = %step_id,
                action = %action_name,
                question = %truncate_for_log(&question, MAX_LOG_TEXT_CHARS),
                "action requested clarification"
            );
            ActionResult::NeedClarification { question }
        }
        ActionResult::NeedApproval { request } => {
            tracing::info!(
                task_id = %ctx.task_id,
                step_id = %step_id,
                action = %action_name,
                reason = %truncate_for_log(&request.reason, MAX_LOG_TEXT_CHARS),
                command = ?request.command,
                "action requested approval"
            );
            ActionResult::NeedApproval { request }
        }
        ActionResult::RetryableError {
            message,
            retry_after,
            attempt,
        } => {
            tracing::warn!(
                task_id = %ctx.task_id,
                step_id = %step_id,
                action = %action_name,
                message = %truncate_for_log(&message, MAX_LOG_TEXT_CHARS),
                retry_after_ms = retry_after.map(|d| d.as_millis() as u64),
                attempt = attempt,
                "action returned retryable error"
            );
            ActionResult::RetryableError {
                message,
                retry_after,
                attempt,
            }
        }
        ActionResult::Error { message } => {
            tracing::error!(
                task_id = %ctx.task_id,
                step_id = %step_id,
                action = %action_name,
                error = %truncate_for_log(&message, MAX_LOG_TEXT_CHARS),
                "action returned terminal error"
            );
            ActionResult::Error { message }
        }
    }
}
