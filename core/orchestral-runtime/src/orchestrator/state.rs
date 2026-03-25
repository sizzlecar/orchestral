use super::*;

pub(super) fn task_state_from_execution(result: &ExecutionResult) -> TaskState {
    match result {
        ExecutionResult::Completed => TaskState::Done,
        ExecutionResult::Failed { error, .. } => TaskState::Failed {
            reason: error.clone(),
            recoverable: false,
        },
        ExecutionResult::WaitingUser {
            prompt, approval, ..
        } => TaskState::WaitingUser {
            prompt: prompt.clone(),
            reason: approval
                .as_ref()
                .map(|a| WaitUserReason::Approval {
                    reason: a.reason.clone(),
                    command: a.command.clone(),
                })
                .unwrap_or(WaitUserReason::Input),
        },
        ExecutionResult::WaitingEvent { event_type, .. } => TaskState::WaitingEvent {
            event_type: event_type.clone(),
        },
    }
}

pub(super) fn restore_checkpoint(dag: &mut orchestral_core::executor::ExecutionDag, task: &Task) {
    for step_id in &task.completed_step_ids {
        dag.mark_completed(step_id.as_str());
    }
}

pub(super) fn complete_wait_step_for_resume(
    dag: &mut orchestral_core::executor::ExecutionDag,
    plan: &orchestral_core::types::Plan,
    completed_steps: &[StepId],
    event: &Event,
) {
    let target_kind = match event {
        Event::UserInput { .. } => StepKind::WaitUser,
        Event::ExternalEvent { .. } => StepKind::WaitEvent,
        _ => return,
    };

    let completed: std::collections::HashSet<&str> =
        completed_steps.iter().map(|id| id.as_str()).collect();
    let candidates = plan
        .steps
        .iter()
        .filter(|s| {
            s.kind == target_kind
                && !completed.contains(s.id.as_str())
                && wait_step_matches_resume_event(s, event)
                && s.depends_on
                    .iter()
                    .all(|dep| completed.contains(dep.as_str()))
        })
        .collect::<Vec<_>>();
    if candidates.len() == 1 {
        dag.mark_completed(candidates[0].id.as_str());
    }
}

fn wait_step_matches_resume_event(step: &orchestral_core::types::Step, event: &Event) -> bool {
    match event {
        Event::ExternalEvent { kind, .. } if step.kind == StepKind::WaitEvent => step
            .params
            .get("event_type")
            .and_then(|v| v.as_str())
            .map(|expected| expected == "*" || expected.eq_ignore_ascii_case(kind))
            .unwrap_or(true),
        Event::UserInput { .. } if step.kind == StepKind::WaitUser => true,
        _ => false,
    }
}

pub(super) fn apply_resume_event_to_working_set(ws: &mut WorkingSet, event: &Event) {
    match event {
        Event::UserInput { payload, .. } => {
            ws.set_task("resume_user_input", payload.clone());
            ws.set_task("last_event_payload", payload.clone());
        }
        Event::ExternalEvent { payload, kind, .. } => {
            ws.set_task("resume_external_event", payload.clone());
            ws.set_task("last_event_payload", payload.clone());
            ws.set_task("last_event_kind", Value::String(kind.clone()));
        }
        _ => {}
    }
}

pub(super) fn interaction_state_from_task(state: &TaskState) -> InteractionState {
    match state {
        TaskState::Done => InteractionState::Completed,
        TaskState::Failed { .. } => InteractionState::Failed,
        TaskState::WaitingUser { .. } => InteractionState::WaitingUser,
        TaskState::WaitingEvent { .. } => InteractionState::WaitingEvent,
        TaskState::Paused => InteractionState::Paused,
        TaskState::Planning | TaskState::Executing => InteractionState::Active,
    }
}
