use crate::types::{Plan, Step, StepId, StepKind};

use super::{agent::agent_output_keys, PlanNormalizer};

#[derive(Debug, Default, Clone)]
pub(super) struct ActionContract {
    pub(super) output_keys: Vec<String>,
}

impl PlanNormalizer {
    pub(super) fn apply_implicit_contracts(&self, plan: &mut Plan) {
        let known_step_ids = plan
            .steps
            .iter()
            .map(|step| step.id.to_string())
            .collect::<std::collections::HashSet<_>>();
        for step in &mut plan.steps {
            let original_kind = step.kind.clone();
            let original_depends_on = step.depends_on.clone();
            let original_exports = step.exports.clone();
            let original_params = step.params.clone();
            infer_special_step_kind(step);
            super::agent::apply_agent_defaults(step);
            derive_depends_on_from_bindings(step);
            derive_depends_on_from_param_templates(step, &known_step_ids);

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
                    "normalizer derived depends_on from io_bindings/templates"
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
    let refs = step
        .io_bindings
        .iter()
        .filter_map(|binding| {
            parse_step_binding_source(&binding.from).map(|(source_step, _)| source_step)
        })
        .collect::<Vec<_>>();
    for source_step in refs {
        add_dependency(step, source_step);
    }
}

fn derive_depends_on_from_param_templates(
    step: &mut Step,
    known_step_ids: &std::collections::HashSet<String>,
) {
    for reference in template_step_references(&step.params, known_step_ids) {
        add_dependency(step, reference);
    }
}

fn add_dependency(step: &mut Step, source_step: String) {
    if source_step.as_str() != step.id.as_str()
        && !step
            .depends_on
            .iter()
            .any(|dep| dep.as_str() == source_step.as_str())
    {
        step.depends_on.push(source_step.into());
    }
}

fn template_step_references(
    value: &serde_json::Value,
    known_step_ids: &std::collections::HashSet<String>,
) -> Vec<String> {
    let mut refs = Vec::new();
    collect_template_step_references(value, known_step_ids, &mut refs);
    refs
}

fn collect_template_step_references(
    value: &serde_json::Value,
    known_step_ids: &std::collections::HashSet<String>,
    refs: &mut Vec<String>,
) {
    match value {
        serde_json::Value::Object(map) => {
            for value in map.values() {
                collect_template_step_references(value, known_step_ids, refs);
            }
        }
        serde_json::Value::Array(items) => {
            for value in items {
                collect_template_step_references(value, known_step_ids, refs);
            }
        }
        serde_json::Value::String(text) => {
            collect_template_refs_from_string(text, known_step_ids, refs);
        }
        _ => {}
    }
}

fn collect_template_refs_from_string(
    text: &str,
    known_step_ids: &std::collections::HashSet<String>,
    refs: &mut Vec<String>,
) {
    let mut rest = text;
    while let Some(start) = rest.find("{{") {
        let after_start = &rest[start + 2..];
        let Some(end) = after_start.find("}}") else {
            return;
        };
        let key = after_start[..end].trim();
        if let Some((source_step, _)) = parse_step_binding_source(key) {
            if known_step_ids.contains(&source_step)
                && !refs.iter().any(|existing| existing == &source_step)
            {
                refs.push(source_step);
            } else if !known_step_ids.contains(&source_step) {
                tracing::debug!(
                    template_ref = key,
                    source_step = %source_step,
                    "normalizer treated template reference as external working-set binding"
                );
            }
        }
        rest = &after_start[end + 2..];
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
