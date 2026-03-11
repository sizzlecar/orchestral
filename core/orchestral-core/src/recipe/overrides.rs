use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::types::{Step, StepId, StepIoBinding, StepKind};

use super::{RecipeCompileError, RecipeStageOverride, RecipeStageTemplate};

pub(super) fn parse_stage_overrides(
    recipe_step: &Step,
    raw_overrides: Option<&Value>,
) -> Result<HashMap<String, RecipeStageOverride>, RecipeCompileError> {
    let Some(raw_overrides) = raw_overrides else {
        return Ok(HashMap::new());
    };
    let Some(obj) = raw_overrides.as_object() else {
        return Err(RecipeCompileError::InvalidRecipe {
            step_id: recipe_step.id.to_string(),
            reason: "params.stage_overrides must be an object keyed by stage id".to_string(),
        });
    };
    let mut overrides = HashMap::new();
    for (stage_id, raw_override) in obj {
        let override_spec: RecipeStageOverride = serde_json::from_value(raw_override.clone())
            .map_err(|error| RecipeCompileError::InvalidRecipe {
                step_id: recipe_step.id.to_string(),
                reason: format!(
                    "params.stage_overrides.{} must deserialize to RecipeStageOverride: {}",
                    stage_id, error
                ),
            })?;
        overrides.insert(stage_id.clone(), override_spec);
    }
    Ok(overrides)
}

pub(super) fn apply_stage_overrides_map(
    stages: &mut [RecipeStageTemplate],
    overrides: &HashMap<String, RecipeStageOverride>,
) -> HashSet<String> {
    let mut applied = HashSet::new();
    for stage in stages {
        if let Some(override_spec) = overrides.get(stage.id.as_str()) {
            merge_stage_override(stage, override_spec.clone());
            applied.insert(stage.id.to_string());
        }
    }
    applied
}

pub(super) fn merge_stage_override(
    stage: &mut RecipeStageTemplate,
    override_spec: RecipeStageOverride,
) {
    if let Some(action) = override_spec.action {
        stage.action = Some(action);
    }
    if let Some(selector) = override_spec.selector {
        stage.selector = Some(selector);
    }
    if let Some(depends_on) = override_spec.depends_on {
        stage.depends_on = depends_on;
    }
    if let Some(exports) = override_spec.exports {
        stage.exports = exports;
    }
    if let Some(io_bindings) = override_spec.io_bindings {
        stage.io_bindings = io_bindings;
    }
    if let Some(params) = override_spec.params {
        stage.params = merge_json(stage.params.clone(), params);
    }
    if let Some(verify_with) = override_spec.verify_with {
        stage.verify_with = Some(verify_with);
    }
}

pub(super) fn expand_verification_contracts(
    recipe_step: &Step,
    stages: Vec<RecipeStageTemplate>,
) -> Result<Vec<RecipeStageTemplate>, RecipeCompileError> {
    let mut expanded = Vec::with_capacity(stages.len());
    let mut reserved_ids = stages
        .iter()
        .map(|stage| stage.id.to_string())
        .collect::<HashSet<_>>();
    let mut emitted_ids = HashSet::new();

    for stage in stages {
        if !emitted_ids.insert(stage.id.to_string()) {
            return Err(RecipeCompileError::InvalidRecipe {
                step_id: recipe_step.id.to_string(),
                reason: format!("duplicate local stage id '{}'", stage.id),
            });
        }

        let verify_with = stage.verify_with.clone();
        let stage_id = stage.id.clone();
        let stage_exports = stage.exports.clone();
        expanded.push(RecipeStageTemplate {
            verify_with: None,
            ..stage
        });

        if let Some(verify_with) = verify_with {
            let verify_id = verify_with
                .id
                .clone()
                .unwrap_or_else(|| StepId::new(format!("{}_verify", stage_id)));
            if verify_id.as_str().trim().is_empty() {
                return Err(RecipeCompileError::InvalidRecipe {
                    step_id: recipe_step.id.to_string(),
                    reason: format!("stage '{}' verify_with.id must not be empty", stage_id),
                });
            }
            if verify_id == stage_id {
                return Err(RecipeCompileError::InvalidRecipe {
                    step_id: recipe_step.id.to_string(),
                    reason: format!(
                        "stage '{}' verify_with.id must differ from the source stage id",
                        stage_id
                    ),
                });
            }
            if reserved_ids.contains(verify_id.as_str())
                || !emitted_ids.insert(verify_id.to_string())
            {
                return Err(RecipeCompileError::InvalidRecipe {
                    step_id: recipe_step.id.to_string(),
                    reason: format!("duplicate verification stage id '{}'", verify_id),
                });
            }
            reserved_ids.insert(verify_id.to_string());

            let mut io_bindings = verify_with.io_bindings.clone();
            if io_bindings.is_empty() && stage_exports.iter().any(|key| key == "path") {
                io_bindings.push(StepIoBinding::required(
                    format!("{}.path", stage_id),
                    "path",
                ));
            }
            expanded.push(RecipeStageTemplate {
                id: verify_id,
                kind: StepKind::Action,
                action: verify_with.action.clone(),
                selector: verify_with.selector.clone(),
                depends_on: vec![stage_id],
                exports: verify_with.exports.clone(),
                io_bindings,
                params: verify_with.params.clone(),
                verify_with: None,
            });
        }
    }

    Ok(expanded)
}

pub(super) fn merge_json(base: Value, overlay: Value) -> Value {
    match (base, overlay) {
        (Value::Object(mut base_obj), Value::Object(overlay_obj)) => {
            for (key, value) in overlay_obj {
                let next = match base_obj.remove(&key) {
                    Some(existing) => merge_json(existing, value),
                    None => value,
                };
                base_obj.insert(key, next);
            }
            Value::Object(base_obj)
        }
        (_, overlay) => overlay,
    }
}
