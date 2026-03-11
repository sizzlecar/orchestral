use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::types::{Plan, Step, StepId, StepIoBinding};

use super::{RecipeAlias, RecipeCompileError, RecipeInputAlias};

pub(super) fn rewrite_local_dep(
    dep: &StepId,
    recipe_step: &Step,
    local_ids: &HashSet<String>,
) -> StepId {
    if local_ids.contains(dep.as_str()) {
        StepId::new(format!("{}__{}", recipe_step.id, dep))
    } else {
        dep.clone()
    }
}

pub(super) fn rewrite_local_io_bindings_with_aliases(
    bindings: &mut [StepIoBinding],
    recipe_step: &Step,
    local_ids: &HashSet<String>,
    recipe_inputs: &HashMap<String, RecipeInputAlias>,
) -> Vec<StepId> {
    let mut deps = Vec::new();
    for binding in bindings {
        if let Some(alias) = recipe_inputs.get(binding.from.as_str()) {
            binding.from = alias.source.clone();
            if let Some(dep) = &alias.dependency {
                deps.push(dep.clone());
            }
            continue;
        }
        let Some((source_step, source_key)) = parse_binding_source(&binding.from) else {
            continue;
        };
        if local_ids.contains(source_step.as_str()) {
            binding.from = format!("{}__{}.{}", recipe_step.id, source_step, source_key);
        }
    }
    deps
}

pub(super) fn parse_export_from_object(
    recipe_step: &Step,
    raw_export_from: &Value,
) -> Result<HashMap<String, String>, RecipeCompileError> {
    let Some(obj) = raw_export_from.as_object() else {
        return Err(RecipeCompileError::InvalidRecipe {
            step_id: recipe_step.id.to_string(),
            reason: "params.export_from must be an object of output_key -> stage.output"
                .to_string(),
        });
    };
    let mut export_from = HashMap::new();
    for (output_key, reference) in obj {
        let reference = reference
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| RecipeCompileError::InvalidRecipe {
                step_id: recipe_step.id.to_string(),
                reason: format!(
                    "params.export_from.{} must be a non-empty string",
                    output_key
                ),
            })?;
        export_from.insert(output_key.clone(), reference.to_string());
    }
    Ok(export_from)
}

pub(super) fn parse_export_bindings(
    recipe_step: &Step,
    export_from: &HashMap<String, String>,
    terminal_step_id: &StepId,
    local_ids: &HashSet<String>,
) -> Result<HashMap<String, String>, RecipeCompileError> {
    let mut bindings = HashMap::new();
    for (output_key, reference) in export_from {
        let resolved = if let Some((local_stage, local_key)) = parse_binding_source(reference) {
            if !local_ids.contains(local_stage.as_str()) {
                return Err(RecipeCompileError::InvalidRecipe {
                    step_id: recipe_step.id.to_string(),
                    reason: format!(
                        "params.export_from.{} references unknown stage '{}'",
                        output_key, local_stage
                    ),
                });
            }
            format!("{}__{}.{}", recipe_step.id, local_stage, local_key)
        } else {
            format!("{}.{}", terminal_step_id, reference)
        };
        bindings.insert(output_key.clone(), resolved);
    }

    Ok(bindings)
}

pub(super) fn rewrite_step_references(steps: &mut [Step], aliases: &HashMap<String, RecipeAlias>) {
    for step in steps {
        for dep in &mut step.depends_on {
            if let Some(alias) = aliases.get(dep.as_str()) {
                *dep = alias.terminal_step_id.clone();
            }
        }
        dedup_step_ids(&mut step.depends_on);

        for binding in &mut step.io_bindings {
            let Some((source_step, source_key)) = parse_binding_source(&binding.from) else {
                continue;
            };
            if let Some(alias) = aliases.get(source_step.as_str()) {
                binding.from = alias.resolve_binding(source_key);
            }
        }
    }
}

pub(super) fn rewrite_plan_templates(plan: &mut Plan, aliases: &HashMap<String, RecipeAlias>) {
    if let Some(template) = &mut plan.on_complete {
        rewrite_template(template, aliases);
    }
    if let Some(template) = &mut plan.on_failure {
        rewrite_template(template, aliases);
    }
}

pub(super) fn rewrite_stage_param_templates(
    params: &mut Value,
    recipe_step: &Step,
    local_ids: &HashSet<String>,
    recipe_inputs: &HashMap<String, RecipeInputAlias>,
) -> Vec<StepId> {
    let mut deps = Vec::new();
    rewrite_value_templates(params, recipe_step, local_ids, recipe_inputs, &mut deps);
    deps
}

pub(super) fn dedup_step_ids(values: &mut Vec<StepId>) {
    let mut seen = HashSet::new();
    values.retain(|value| seen.insert(value.to_string()));
}

pub(super) fn parse_binding_source(value: &str) -> Option<(String, &str)> {
    let (source_step, source_key) = value.split_once('.')?;
    if source_step.is_empty() || source_key.is_empty() {
        return None;
    }
    Some((source_step.to_string(), source_key))
}

fn rewrite_template(template: &mut String, aliases: &HashMap<String, RecipeAlias>) {
    for (recipe_id, alias) in aliases {
        for (output_key, binding) in &alias.export_bindings {
            let from = format!("{{{{{}.{}}}}}", recipe_id, output_key);
            let to = format!("{{{{{}}}}}", binding);
            *template = template.replace(&from, &to);
        }

        let from_prefix = format!("{{{{{}.", recipe_id);
        let to_prefix = format!("{{{{{}.", alias.terminal_step_id);
        *template = template.replace(&from_prefix, &to_prefix);
    }
}

fn rewrite_value_templates(
    value: &mut Value,
    recipe_step: &Step,
    local_ids: &HashSet<String>,
    recipe_inputs: &HashMap<String, RecipeInputAlias>,
    deps: &mut Vec<StepId>,
) {
    match value {
        Value::String(text) => {
            for (alias, binding) in recipe_inputs {
                let placeholder = format!("{{{{{}}}}}", alias);
                if text.contains(&placeholder) {
                    *text = text.replace(&placeholder, &format!("{{{{{}}}}}", binding.source));
                    if let Some(dep) = &binding.dependency {
                        deps.push(dep.clone());
                    }
                }
            }
            for local_id in local_ids {
                let from = format!("{{{{{}.", local_id);
                let to = format!("{{{{{}__{}.", recipe_step.id, local_id);
                if text.contains(&from) {
                    *text = text.replace(&from, &to);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                rewrite_value_templates(item, recipe_step, local_ids, recipe_inputs, deps);
            }
        }
        Value::Object(map) => {
            for item in map.values_mut() {
                rewrite_value_templates(item, recipe_step, local_ids, recipe_inputs, deps);
            }
        }
        _ => {}
    }
}
