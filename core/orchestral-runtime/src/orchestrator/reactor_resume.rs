use orchestral_core::planner::{PlanError, PlannerContext, PlannerOutput, PlannerRuntimeInfo};
use orchestral_core::types::{StageChoice, StageKind, Task};

use super::{
    build_reactor_stage_choice, normalize_reactor_stage_choice, payload_to_string,
    resolve_stage_choice_from_skeleton_choice, summarize_working_set, truncate_for_log, Event,
    Orchestrator, OrchestratorError,
};
use crate::Intent;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReactorResumeDecision {
    Approve,
    Deny,
    Unknown,
}

fn parse_reactor_resume_decision(message: &str) -> ReactorResumeDecision {
    let normalized = message.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return ReactorResumeDecision::Unknown;
    }

    let deny_markers = ["拒绝", "不同意", "取消", "不要执行", "deny", "cancel"];
    if deny_markers
        .iter()
        .any(|marker| normalized.contains(&marker.to_ascii_lowercase()))
    {
        return ReactorResumeDecision::Deny;
    }

    let approve_markers = [
        "确认",
        "同意",
        "批准",
        "继续",
        "重试",
        "再试",
        "按计划",
        "写回",
        "执行",
        "approve",
        "approved",
        "proceed",
        "retry",
    ];
    if approve_markers
        .iter()
        .any(|marker| normalized.contains(&marker.to_ascii_lowercase()))
    {
        return ReactorResumeDecision::Approve;
    }

    ReactorResumeDecision::Unknown
}

pub(super) async fn build_reactor_resume_choice(
    orchestrator: &Orchestrator,
    task: &Task,
    event: &Event,
    interaction_id: &str,
) -> Result<StageChoice, OrchestratorError> {
    let reactor_state = task.reactor.clone().ok_or_else(|| {
        OrchestratorError::ResumeError("reactor task state missing during resume".to_string())
    })?;
    let current_choice = build_reactor_stage_choice(
        reactor_state.skeleton,
        reactor_state.artifact_family,
        reactor_state.current_stage,
        reactor_state.derivation_policy,
        reactor_state
            .last_continuation
            .as_ref()
            .and_then(|continuation| continuation.next_stage_hint),
        reactor_state
            .last_continuation
            .as_ref()
            .map(|continuation| continuation.reason.clone()),
    )?;
    let resume_text = match event {
        Event::UserInput { payload, .. } | Event::ExternalEvent { payload, .. } => {
            payload_to_string(payload)
        }
        _ => String::new(),
    };

    match parse_reactor_resume_decision(&resume_text) {
        ReactorResumeDecision::Approve => {
            if let Some(last_failure) = reactor_state.last_failure.as_ref() {
                return build_reactor_stage_choice(
                    reactor_state.skeleton,
                    reactor_state.artifact_family,
                    reactor_state.current_stage,
                    reactor_state.derivation_policy,
                    Some(reactor_state.current_stage),
                    Some(format!(
                        "retrying failed reactor step '{}' after user approval",
                        last_failure.step_id
                    )),
                );
            }
            if let Some(next_stage) = reactor_state
                .last_continuation
                .as_ref()
                .and_then(|continuation| continuation.next_stage_hint)
            {
                return build_reactor_stage_choice(
                    reactor_state.skeleton,
                    reactor_state.artifact_family,
                    next_stage,
                    reactor_state.derivation_policy,
                    Some(next_stage),
                    Some(format!(
                        "reactor resume approved by user: {}",
                        truncate_for_log(&resume_text, 300)
                    )),
                );
            }
        }
        ReactorResumeDecision::Deny => {
            if reactor_state.last_failure.is_some() {
                return build_reactor_stage_choice(
                    reactor_state.skeleton,
                    reactor_state.artifact_family,
                    StageKind::WaitUser,
                    reactor_state.derivation_policy,
                    Some(StageKind::WaitUser),
                    Some(
                        "User declined retrying the failed reactor step. Awaiting updated instructions."
                            .to_string(),
                    ),
                );
            }
            return build_reactor_stage_choice(
                reactor_state.skeleton,
                reactor_state.artifact_family,
                StageKind::WaitUser,
                reactor_state.derivation_policy,
                Some(StageKind::WaitUser),
                Some(
                    "User declined the pending reactor change. Awaiting updated instructions."
                        .to_string(),
                ),
            );
        }
        ReactorResumeDecision::Unknown => {}
    }

    let prompt = format!(
        "{}: {}",
        if reactor_state.last_failure.is_some() {
            "User responded after a recoverable reactor failure"
        } else {
            "User resumed the waiting reactor task"
        },
        truncate_for_log(&resume_text, 600)
    );
    build_reactor_replan_choice(orchestrator, task, &current_choice, &prompt, interaction_id).await
}

pub(super) async fn build_reactor_replan_choice(
    orchestrator: &Orchestrator,
    task: &Task,
    current_choice: &StageChoice,
    replan_prompt: &str,
    interaction_id: &str,
) -> Result<StageChoice, OrchestratorError> {
    tracing::info!(
        interaction_id = %interaction_id,
        task_id = %task.id,
        current_stage = ?current_choice.current_stage,
        skeleton = ?current_choice.skeleton,
        artifact_family = ?current_choice.artifact_family,
        replan_prompt = %truncate_for_log(replan_prompt, 400),
        "build_reactor_replan_choice started"
    );

    let actions = orchestrator.available_actions().await;
    let history = orchestrator
        .history_for_planner(interaction_id, task.id.as_str())
        .await?;
    let ws_summary = summarize_working_set(&task.working_set_snapshot);
    let content = format!(
        "Reactor stage replanning request.\n\
        Original user intent: {}\n\
        Current skeleton: {:?}\n\
        Current artifact family: {:?}\n\
        Current stage: {:?}\n\
        Current working set summary:\n{}\n\
        Replan reason: {}\n\
        Return the next STAGE_CHOICE for the current reactor task. \
        Prefer staying within the current skeleton/artifact family unless current evidence requires otherwise. \
        Do not return a full workflow DAG.",
        task.intent.content,
        current_choice.skeleton,
        current_choice.artifact_family,
        current_choice.current_stage,
        ws_summary,
        replan_prompt,
    );
    let intent = Intent::new(content);

    let runtime_info = PlannerRuntimeInfo::detect();
    let skill_instructions = orchestrator
        .skill_catalog
        .build_instructions(&task.intent.content);
    let context =
        PlannerContext::with_history(actions, history, orchestrator.reference_store.clone())
            .with_runtime_info(runtime_info)
            .with_skill_instructions(skill_instructions);

    let output = orchestrator.planner.plan(&intent, &context).await?;
    match output {
        PlannerOutput::SkeletonChoice(choice) => resolve_stage_choice_from_skeleton_choice(
            choice,
            Some(current_choice.artifact_family),
            current_choice.derivation_policy,
            false,
        ),
        PlannerOutput::StageChoice(choice) => normalize_reactor_stage_choice(choice),
        PlannerOutput::ActionCall(call) => {
            Err(OrchestratorError::Planner(PlanError::Generation(format!(
                "reactor replan returned action_call unexpectedly: {}",
                truncate_for_log(&call.action, 200)
            ))))
        }
        PlannerOutput::Clarification(question) => build_reactor_stage_choice(
            current_choice.skeleton,
            current_choice.artifact_family,
            StageKind::WaitUser,
            current_choice.derivation_policy,
            Some(StageKind::WaitUser),
            Some(question),
        ),
        PlannerOutput::DirectResponse(message) => {
            Err(OrchestratorError::Planner(PlanError::Generation(format!(
                "reactor replan returned direct_response unexpectedly: {}",
                truncate_for_log(&message, 200)
            ))))
        }
    }
}
