use super::*;
use crate::orchestrator::support::{summarize_available_bindings, summarize_binding_shapes};
use orchestral_core::types::Plan;

const MAX_RECENT_OBSERVATIONS: usize = 4;

#[derive(Debug, Default, Clone)]
pub(super) struct AgentLoopState {
    recent_observations: Vec<String>,
    completed_step_ids: Vec<String>,
    available_bindings: Vec<String>,
    binding_shapes: Vec<String>,
    working_set_preview: Option<String>,
}

impl AgentLoopState {
    pub(super) fn planner_loop_context(
        &self,
        iteration: usize,
        max_iterations: usize,
    ) -> Option<PlannerLoopContext> {
        if self.recent_observations.is_empty()
            && self.completed_step_ids.is_empty()
            && self.available_bindings.is_empty()
            && self.binding_shapes.is_empty()
            && self.working_set_preview.is_none()
        {
            return None;
        }

        Some(PlannerLoopContext {
            iteration,
            max_iterations,
            recent_observations: self.recent_observations.clone(),
            completed_step_ids: self.completed_step_ids.clone(),
            available_bindings: self.available_bindings.clone(),
            binding_shapes: self.binding_shapes.clone(),
            working_set_preview: self.working_set_preview.clone(),
        })
    }

    pub(super) fn record_iteration(
        &mut self,
        iteration: usize,
        execution_mode: &str,
        plan: &Plan,
        result: &ExecutionResult,
        task: &Task,
    ) {
        let observation =
            build_iteration_observation(iteration, execution_mode, plan, result, task);
        self.recent_observations.push(observation);
        if self.recent_observations.len() > MAX_RECENT_OBSERVATIONS {
            let drain = self.recent_observations.len() - MAX_RECENT_OBSERVATIONS;
            self.recent_observations.drain(0..drain);
        }

        self.completed_step_ids = task
            .completed_step_ids
            .iter()
            .map(|id| id.to_string())
            .collect();
        self.available_bindings = summarize_available_bindings(&task.working_set_snapshot);
        self.binding_shapes = summarize_binding_shapes(&task.working_set_snapshot);
        let preview = summarize_working_set(&task.working_set_snapshot);
        self.working_set_preview = if preview.trim().is_empty() || preview == "(empty)" {
            None
        } else {
            Some(preview)
        };
    }

    pub(super) fn observation_count(&self) -> usize {
        self.recent_observations.len()
    }
}

pub(super) fn should_continue_agent_loop(
    result: &ExecutionResult,
    iteration: usize,
    max_iterations: usize,
) -> bool {
    iteration < max_iterations
        && matches!(
            result,
            ExecutionResult::Completed | ExecutionResult::Failed { .. }
        )
}

pub(super) fn agent_loop_iteration_limit_error(max_iterations: usize) -> String {
    format!(
        "planner iteration limit reached after {} rounds without a terminal DONE/NEED_INPUT decision",
        max_iterations
    )
}

fn build_iteration_observation(
    iteration: usize,
    execution_mode: &str,
    plan: &Plan,
    result: &ExecutionResult,
    task: &Task,
) -> String {
    let mut observation = format!(
        "iteration {} ran {} plan '{}' ({} step(s): {})",
        iteration,
        execution_mode,
        plan.goal,
        plan.steps.len(),
        summarize_plan_actions(plan)
    );
    match result {
        ExecutionResult::Completed => {
            observation.push_str("; result=completed");
            if let Some(summary) = task
                .working_set_snapshot
                .get("summary")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
            {
                observation.push_str(&format!("; summary={}", truncate_for_log(summary, 240)));
            }
        }
        ExecutionResult::Failed { step_id, error } => {
            observation.push_str(&format!(
                "; result=failed at {}: {}",
                step_id,
                truncate_for_log(error, 240)
            ));
        }
        ExecutionResult::WaitingUser { prompt, .. } => {
            observation.push_str(&format!(
                "; result=waiting_user: {}",
                truncate_for_log(prompt, 240)
            ));
        }
        ExecutionResult::WaitingEvent {
            step_id,
            event_type,
        } => {
            observation.push_str(&format!(
                "; result=waiting_event at {} for {}",
                step_id, event_type
            ));
        }
    }
    observation
}

fn summarize_plan_actions(plan: &Plan) -> String {
    const MAX_STEPS: usize = 6;
    let mut parts = plan
        .steps
        .iter()
        .take(MAX_STEPS)
        .map(|step| format!("{}:{}", step.id, step.action))
        .collect::<Vec<_>>();
    if plan.steps.len() > MAX_STEPS {
        parts.push(format!("... +{} more", plan.steps.len() - MAX_STEPS));
    }
    parts.join(", ")
}
