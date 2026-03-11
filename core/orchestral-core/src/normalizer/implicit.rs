use crate::types::{Plan, Step, StepId, StepKind};

use super::{agent::agent_output_keys, PlanNormalizer};

#[derive(Debug, Default, Clone)]
pub(super) struct ActionContract {
    pub(super) output_keys: Vec<String>,
}

impl PlanNormalizer {
    pub(super) fn apply_implicit_contracts(&self, plan: &mut Plan) {
        for step in &mut plan.steps {
            let original_kind = step.kind.clone();
            let original_depends_on = step.depends_on.clone();
            let original_exports = step.exports.clone();
            let original_params = step.params.clone();
            infer_special_step_kind(step);
            super::agent::apply_agent_defaults(step);
            derive_depends_on_from_bindings(step);

            if step.kind == StepKind::Agent {
                if let Some(keys) = agent_output_keys(step) {
                    step.exports = keys;
                }
            }

            if let Some(contract) = self.known_actions.get(&step.action) {
                if !contract.output_keys.is_empty() {
                    step.exports = contract.output_keys.clone();
                }
            }

            if step.kind != original_kind {
                tracing::debug!(
                    step_id = %step.id,
                    action = %step.action,
                    from_kind = ?original_kind,
                    to_kind = ?step.kind,
                    "normalizer inferred step kind"
                );
            }
            if step.depends_on != original_depends_on {
                tracing::debug!(
                    step_id = %step.id,
                    action = %step.action,
                    depends_on = ?step.depends_on,
                    "normalizer derived depends_on from io_bindings"
                );
            }
            if step.exports != original_exports {
                tracing::debug!(
                    step_id = %step.id,
                    action = %step.action,
                    exports = ?step.exports,
                    "normalizer populated exports from action output schema"
                );
            }
            if step.params != original_params {
                tracing::debug!(
                    step_id = %step.id,
                    action = %step.action,
                    params = ?step.params,
                    "normalizer updated step params"
                );
            }
        }
    }
}

pub(super) fn fix_control_flow_dependencies(plan: &mut Plan) {
    let step_ids: Vec<StepId> = plan.steps.iter().map(|s| s.id.clone()).collect();
    for i in 0..plan.steps.len() {
        let step = &plan.steps[i];
        let is_control = matches!(
            step.kind,
            StepKind::Replan | StepKind::WaitUser | StepKind::WaitEvent
        );
        if !is_control || !step.depends_on.is_empty() {
            continue;
        }
        let preceding: Vec<StepId> = step_ids[..i].to_vec();
        if !preceding.is_empty() {
            tracing::debug!(
                step_id = %plan.steps[i].id,
                kind = ?plan.steps[i].kind,
                deps = ?preceding,
                "normalizer auto-assigned depends_on for control-flow step"
            );
            plan.steps[i].depends_on = preceding;
        }
    }
}

fn infer_special_step_kind(step: &mut Step) {
    if step.kind != StepKind::Action {
        return;
    }

    match step.action.as_str() {
        "wait_user" => step.kind = StepKind::WaitUser,
        "wait_event" => step.kind = StepKind::WaitEvent,
        "agent" => step.kind = StepKind::Agent,
        _ => {}
    }
}

fn derive_depends_on_from_bindings(step: &mut Step) {
    for binding in &step.io_bindings {
        if let Some((source_step, _)) = parse_step_binding_source(&binding.from) {
            if source_step.as_str() != step.id.as_str()
                && !step
                    .depends_on
                    .iter()
                    .any(|dep| dep.as_str() == source_step.as_str())
            {
                step.depends_on.push(source_step.into());
            }
        }
    }
}

pub(super) fn parse_step_binding_source(value: &str) -> Option<(String, &str)> {
    let (source_step, source_key) = value.split_once('.')?;
    if source_step.is_empty() || source_key.is_empty() {
        return None;
    }
    Some((source_step.to_string(), source_key))
}

pub(super) fn output_keys_from_schema(schema: &serde_json::Value) -> Vec<String> {
    let Some(schema_obj) = schema.as_object() else {
        return Vec::new();
    };

    let required: Vec<String> = schema_obj
        .get("required")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|item| item.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    if !required.is_empty() {
        return required;
    }

    schema_obj
        .get("properties")
        .and_then(|v| v.as_object())
        .map(|props| props.keys().cloned().collect())
        .unwrap_or_default()
}
