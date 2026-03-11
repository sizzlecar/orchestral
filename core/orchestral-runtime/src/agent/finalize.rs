use std::collections::HashMap;

use orchestral_core::executor::ExecutorContext;
use orchestral_core::types::Step;
use serde_json::Value;
use tracing::debug;

use crate::planner::{LlmClient, LlmRequest, LlmResponse};

use super::materialize::materialize_final_exports;
use super::parsing::{extract_json, parse_agent_decision, parse_tool_call_decision};
use super::progress::report_agent_progress;
use super::prompt::{
    agent_final_tool_definitions, build_agent_finalization_system_prompt,
    build_agent_finalization_user_prompt, truncate_log_text,
};
use super::{AgentDecision, AgentEvidenceStore, AgentStepParams, LlmAgentExecutorConfig};

pub(super) struct ForcedFinalizationContext<'a> {
    pub(super) step: &'a Step,
    pub(super) params: &'a AgentStepParams,
    pub(super) ctx: &'a ExecutorContext,
    pub(super) observations: &'a [String],
    pub(super) last_success_exports: Option<&'a HashMap<String, Value>>,
    pub(super) evidence: &'a AgentEvidenceStore,
}

pub(super) async fn attempt_forced_finalization<C: LlmClient>(
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
