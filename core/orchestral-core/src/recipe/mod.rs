mod builtins;
mod overrides;
mod rewrite;

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::action::ActionMeta;
use crate::types::{Plan, Step, StepId, StepIoBinding, StepKind};

use self::builtins::builtin_recipe_templates;
use self::overrides::{
    apply_stage_overrides_map, expand_verification_contracts, parse_stage_overrides,
};
use self::rewrite::{
    dedup_step_ids, parse_binding_source, parse_export_bindings, parse_export_from_object,
    rewrite_local_dep, rewrite_local_io_bindings_with_aliases, rewrite_plan_templates,
    rewrite_stage_param_templates, rewrite_step_references,
};

pub const RECIPE_REGISTRY_COMPONENT_KEY: &str = "recipe_registry";

#[derive(Debug, Error)]
pub enum RecipeCompileError {
    #[error("recipe step '{step_id}' is invalid: {reason}")]
    InvalidRecipe { step_id: String, reason: String },
    #[error("recipe step '{step_id}' stage '{stage_id}' cannot resolve action: {reason}")]
    ActionResolution {
        step_id: String,
        stage_id: String,
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ActionSelector {
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub all_of: Vec<String>,
    #[serde(default)]
    pub any_of: Vec<String>,
    #[serde(default)]
    pub none_of: Vec<String>,
    #[serde(default)]
    pub roles_all_of: Vec<String>,
    #[serde(default)]
    pub roles_any_of: Vec<String>,
    #[serde(default)]
    pub input_kinds_any_of: Vec<String>,
    #[serde(default)]
    pub output_kinds_any_of: Vec<String>,
}

impl ActionSelector {
    pub fn with_action(action: impl Into<String>) -> Self {
        Self {
            action: Some(action.into()),
            all_of: Vec::new(),
            any_of: Vec::new(),
            none_of: Vec::new(),
            roles_all_of: Vec::new(),
            roles_any_of: Vec::new(),
            input_kinds_any_of: Vec::new(),
            output_kinds_any_of: Vec::new(),
        }
    }

    pub fn with_all_of<I, S>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.all_of = values.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_any_of<I, S>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.any_of = values.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_none_of<I, S>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.none_of = values.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_roles_all_of<I, S>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.roles_all_of = values.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_roles_any_of<I, S>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.roles_any_of = values.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_input_kinds_any_of<I, S>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.input_kinds_any_of = values.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_output_kinds_any_of<I, S>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.output_kinds_any_of = values.into_iter().map(Into::into).collect();
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecipeStageTemplate {
    pub id: StepId,
    #[serde(default)]
    pub kind: StepKind,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub selector: Option<ActionSelector>,
    #[serde(default)]
    pub depends_on: Vec<StepId>,
    #[serde(default)]
    pub exports: Vec<String>,
    #[serde(default)]
    pub io_bindings: Vec<StepIoBinding>,
    #[serde(default)]
    pub params: Value,
    #[serde(default)]
    pub verify_with: Option<RecipeVerificationTemplate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecipeVerificationTemplate {
    #[serde(default)]
    pub id: Option<StepId>,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub selector: Option<ActionSelector>,
    #[serde(default)]
    pub exports: Vec<String>,
    #[serde(default)]
    pub io_bindings: Vec<StepIoBinding>,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RecipeStageOverride {
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub selector: Option<ActionSelector>,
    #[serde(default)]
    pub depends_on: Option<Vec<StepId>>,
    #[serde(default)]
    pub exports: Option<Vec<String>>,
    #[serde(default)]
    pub io_bindings: Option<Vec<StepIoBinding>>,
    #[serde(default)]
    pub params: Option<Value>,
    #[serde(default)]
    pub verify_with: Option<RecipeVerificationTemplate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecipeTemplate {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub stages: Vec<RecipeStageTemplate>,
    #[serde(default)]
    pub export_from: HashMap<String, String>,
}

impl RecipeTemplate {
    pub fn new(name: impl Into<String>, stages: Vec<RecipeStageTemplate>) -> Self {
        Self {
            name: name.into(),
            description: None,
            stages,
            export_from: HashMap::new(),
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_export_from(mut self, export_from: HashMap<String, String>) -> Self {
        self.export_from = export_from;
        self
    }
}

#[derive(Debug, Clone)]
pub struct RecipeRegistry {
    templates: HashMap<String, RecipeTemplate>,
}

impl RecipeRegistry {
    pub fn new() -> Self {
        Self {
            templates: HashMap::new(),
        }
    }

    pub fn with_builtin_templates() -> Self {
        let mut registry = Self::new();
        for template in builtin_recipe_templates() {
            registry.register(template);
        }
        registry
    }

    pub fn register(&mut self, template: RecipeTemplate) {
        self.templates.insert(template.name.clone(), template);
    }

    pub fn get(&self, name: &str) -> Option<&RecipeTemplate> {
        self.templates.get(name)
    }

    pub fn templates(&self) -> impl Iterator<Item = &RecipeTemplate> {
        self.templates.values()
    }
}

impl Default for RecipeRegistry {
    fn default() -> Self {
        Self::with_builtin_templates()
    }
}

#[derive(Debug, Clone)]
pub struct RecipeCompiler {
    action_catalog: HashMap<String, ActionMeta>,
    registry: RecipeRegistry,
}

#[derive(Debug, Clone)]
struct RecipeAlias {
    terminal_step_id: StepId,
    export_bindings: HashMap<String, String>,
}

impl RecipeAlias {
    fn resolve_binding(&self, key: &str) -> String {
        self.export_bindings
            .get(key)
            .cloned()
            .unwrap_or_else(|| format!("{}.{}", self.terminal_step_id, key))
    }
}

#[derive(Debug, Clone)]
struct ResolvedRecipeSpec {
    stages: Vec<RecipeStageTemplate>,
    export_from: HashMap<String, String>,
}

#[derive(Debug, Clone)]
struct RecipeInputAlias {
    source: String,
    dependency: Option<StepId>,
}

#[derive(Debug, Clone)]
struct LoweredRecipe {
    steps: Vec<Step>,
    alias: RecipeAlias,
}

impl RecipeCompiler {
    pub fn new() -> Self {
        Self {
            action_catalog: HashMap::new(),
            registry: RecipeRegistry::default(),
        }
    }

    pub fn register_action(&mut self, name: impl Into<String>) {
        let name = name.into();
        self.action_catalog
            .entry(name.clone())
            .or_insert_with(|| ActionMeta::new(name, ""));
    }

    pub fn register_action_meta(&mut self, meta: &ActionMeta) {
        self.action_catalog.insert(meta.name.clone(), meta.clone());
    }

    pub fn register_template(&mut self, template: RecipeTemplate) {
        self.registry.register(template);
    }

    pub fn compile(&self, plan: &mut Plan) -> Result<bool, RecipeCompileError> {
        let mut aliases = HashMap::new();
        let mut lowered = Vec::with_capacity(plan.steps.len());
        let mut compiled_any = false;

        for step in std::mem::take(&mut plan.steps) {
            if step.kind != StepKind::Recipe {
                lowered.push(step);
                continue;
            }

            let lowered_recipe = self.lower_recipe_step(&step)?;
            aliases.insert(step.id.to_string(), lowered_recipe.alias);
            lowered.extend(lowered_recipe.steps);
            compiled_any = true;
        }

        if compiled_any {
            rewrite_step_references(&mut lowered, &aliases);
            rewrite_plan_templates(plan, &aliases);
        }

        plan.steps = lowered;
        Ok(compiled_any)
    }

    fn lower_recipe_step(&self, step: &Step) -> Result<LoweredRecipe, RecipeCompileError> {
        let resolved = self.resolve_recipe_spec(step)?;
        if resolved.stages.is_empty() {
            return Err(RecipeCompileError::InvalidRecipe {
                step_id: step.id.to_string(),
                reason: "recipe must expand to at least one stage".to_string(),
            });
        }

        let mut local_ids = HashSet::new();
        for local in &resolved.stages {
            if local.id.as_str().trim().is_empty() {
                return Err(RecipeCompileError::InvalidRecipe {
                    step_id: step.id.to_string(),
                    reason: "each recipe stage must define a non-empty id".to_string(),
                });
            }
            if local.kind == StepKind::Recipe {
                return Err(RecipeCompileError::InvalidRecipe {
                    step_id: step.id.to_string(),
                    reason: format!("nested recipe stage '{}' is not supported", local.id),
                });
            }
            if !local_ids.insert(local.id.to_string()) {
                return Err(RecipeCompileError::InvalidRecipe {
                    step_id: step.id.to_string(),
                    reason: format!("duplicate local stage id '{}'", local.id),
                });
            }
        }

        let mut lowered = Vec::with_capacity(resolved.stages.len());
        let mut previous_stage_id: Option<StepId> = None;
        let recipe_inputs = collect_recipe_input_aliases(step);
        for (index, local_template) in resolved.stages.into_iter().enumerate() {
            let original_local_id = local_template.id.to_string();
            let global_id = StepId::new(format!("{}__{}", step.id, original_local_id));
            let had_explicit_local_deps = !local_template.depends_on.is_empty();
            let mut local = self.materialize_stage(step, local_template)?;
            local.id = global_id.clone();
            let template_deps =
                rewrite_stage_param_templates(&mut local.params, step, &local_ids, &recipe_inputs);

            local.depends_on = local
                .depends_on
                .into_iter()
                .map(|dep| rewrite_local_dep(&dep, step, &local_ids))
                .collect();
            dedup_step_ids(&mut local.depends_on);

            if !had_explicit_local_deps {
                if index == 0 {
                    local.depends_on.extend(step.depends_on.clone());
                    local.io_bindings.extend(step.io_bindings.clone());
                } else if let Some(previous) = &previous_stage_id {
                    local.depends_on.push(previous.clone());
                }
            }

            let binding_deps = rewrite_local_io_bindings_with_aliases(
                &mut local.io_bindings,
                step,
                &local_ids,
                &recipe_inputs,
            );
            local.depends_on.extend(template_deps);
            local.depends_on.extend(binding_deps);

            dedup_step_ids(&mut local.depends_on);
            previous_stage_id = Some(global_id);
            lowered.push(local);
        }

        let terminal_step_id =
            previous_stage_id.ok_or_else(|| RecipeCompileError::InvalidRecipe {
                step_id: step.id.to_string(),
                reason: "recipe must expand to at least one stage".to_string(),
            })?;
        let export_bindings =
            parse_export_bindings(step, &resolved.export_from, &terminal_step_id, &local_ids)?;
        Ok(LoweredRecipe {
            steps: lowered,
            alias: RecipeAlias {
                terminal_step_id,
                export_bindings,
            },
        })
    }

    fn resolve_recipe_spec(&self, step: &Step) -> Result<ResolvedRecipeSpec, RecipeCompileError> {
        let params = step
            .params
            .as_object()
            .ok_or_else(|| RecipeCompileError::InvalidRecipe {
                step_id: step.id.to_string(),
                reason: "params must be an object".to_string(),
            })?;

        if params.contains_key("template") && params.contains_key("stages") {
            return Err(RecipeCompileError::InvalidRecipe {
                step_id: step.id.to_string(),
                reason: "recipe params cannot contain both template and stages".to_string(),
            });
        }

        if let Some(template_name) = params.get("template") {
            let template_name = template_name
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| RecipeCompileError::InvalidRecipe {
                    step_id: step.id.to_string(),
                    reason: "params.template must be a non-empty string".to_string(),
                })?;
            let template = self.registry.get(template_name).ok_or_else(|| {
                RecipeCompileError::InvalidRecipe {
                    step_id: step.id.to_string(),
                    reason: format!("unknown recipe template '{}'", template_name),
                }
            })?;
            let overrides = parse_stage_overrides(step, params.get("stage_overrides"))?;
            let mut stages = template.stages.clone();
            let mut applied = apply_stage_overrides_map(&mut stages, &overrides);
            stages = expand_verification_contracts(step, stages)?;
            let remaining = overrides
                .iter()
                .filter(|(stage_id, _)| !applied.contains(stage_id.as_str()))
                .map(|(stage_id, override_spec)| (stage_id.clone(), override_spec.clone()))
                .collect::<HashMap<_, _>>();
            let generated = apply_stage_overrides_map(&mut stages, &remaining);
            applied.extend(generated);
            if applied.len() != overrides.len() {
                let unknown = overrides
                    .keys()
                    .find(|stage_id| !applied.contains(stage_id.as_str()))
                    .cloned()
                    .unwrap_or_else(|| "<unknown>".to_string());
                return Err(RecipeCompileError::InvalidRecipe {
                    step_id: step.id.to_string(),
                    reason: format!(
                        "params.stage_overrides references unknown stage '{}'",
                        unknown
                    ),
                });
            }
            let mut export_from = template.export_from.clone();
            if let Some(raw_export_from) = params.get("export_from") {
                export_from = parse_export_from_object(step, raw_export_from)?;
            }
            return Ok(ResolvedRecipeSpec {
                stages,
                export_from,
            });
        }

        let raw_stages = params
            .get("stages")
            .and_then(Value::as_array)
            .ok_or_else(|| RecipeCompileError::InvalidRecipe {
                step_id: step.id.to_string(),
                reason: "recipe params must contain either template or non-empty stages"
                    .to_string(),
            })?;
        if raw_stages.is_empty() {
            return Err(RecipeCompileError::InvalidRecipe {
                step_id: step.id.to_string(),
                reason: "params.stages must be a non-empty array".to_string(),
            });
        }

        let mut stages = Vec::with_capacity(raw_stages.len());
        for raw in raw_stages {
            let stage: RecipeStageTemplate =
                serde_json::from_value(raw.clone()).map_err(|error| {
                    RecipeCompileError::InvalidRecipe {
                        step_id: step.id.to_string(),
                        reason: format!(
                            "params.stages[] must deserialize to RecipeStageTemplate: {}",
                            error
                        ),
                    }
                })?;
            stages.push(stage);
        }
        stages = expand_verification_contracts(step, stages)?;
        let export_from = if let Some(raw_export_from) = params.get("export_from") {
            parse_export_from_object(step, raw_export_from)?
        } else {
            HashMap::new()
        };
        Ok(ResolvedRecipeSpec {
            stages,
            export_from,
        })
    }

    fn materialize_stage(
        &self,
        recipe_step: &Step,
        stage: RecipeStageTemplate,
    ) -> Result<Step, RecipeCompileError> {
        if stage.kind != StepKind::Action && stage.selector.is_some() {
            return Err(RecipeCompileError::InvalidRecipe {
                step_id: recipe_step.id.to_string(),
                reason: format!("stage '{}' uses selector but is not kind=action", stage.id),
            });
        }

        let action = match stage.kind {
            StepKind::Action => self.resolve_action_name(recipe_step, &stage)?,
            StepKind::Agent => stage.action.unwrap_or_else(|| "agent".to_string()),
            StepKind::WaitUser => stage.action.unwrap_or_else(|| "wait_user".to_string()),
            StepKind::WaitEvent => stage.action.unwrap_or_else(|| "wait_event".to_string()),
            StepKind::Replan => stage.action.unwrap_or_else(|| "replan".to_string()),
            StepKind::System => stage.action.unwrap_or_default(),
            StepKind::Recipe => {
                return Err(RecipeCompileError::InvalidRecipe {
                    step_id: recipe_step.id.to_string(),
                    reason: format!("nested recipe stage '{}' is not supported", stage.id),
                });
            }
        };

        Ok(Step {
            id: stage.id,
            action,
            kind: stage.kind,
            depends_on: stage.depends_on,
            exports: stage.exports,
            io_bindings: stage.io_bindings,
            params: stage.params,
        })
    }

    fn resolve_action_name(
        &self,
        recipe_step: &Step,
        stage: &RecipeStageTemplate,
    ) -> Result<String, RecipeCompileError> {
        if let Some(action) = &stage.action {
            if let Some(selector) = &stage.selector {
                self.validate_explicit_action_against_selector(
                    recipe_step,
                    stage,
                    action,
                    selector,
                )?;
            }
            return Ok(action.clone());
        }

        let selector =
            stage
                .selector
                .as_ref()
                .ok_or_else(|| RecipeCompileError::InvalidRecipe {
                    step_id: recipe_step.id.to_string(),
                    reason: format!(
                        "action stage '{}' must declare either action or selector",
                        stage.id
                    ),
                })?;

        if self.action_catalog.is_empty() {
            if let Some(action) = &selector.action {
                return Ok(action.clone());
            }
            return Err(RecipeCompileError::ActionResolution {
                step_id: recipe_step.id.to_string(),
                stage_id: stage.id.to_string(),
                reason: "selector requires action metadata, but action catalog is empty"
                    .to_string(),
            });
        }

        let mut candidates = self
            .action_catalog
            .values()
            .filter(|meta| selector_matches(meta, selector))
            .map(|meta| (selector_score(meta, selector), meta.name.clone()))
            .collect::<Vec<_>>();
        candidates.sort_by(|(score_a, name_a), (score_b, name_b)| {
            score_b.cmp(score_a).then_with(|| name_a.cmp(name_b))
        });
        candidates.dedup_by(|left, right| left.1 == right.1);

        candidates
            .into_iter()
            .map(|(_, name)| name)
            .next()
            .ok_or_else(|| RecipeCompileError::ActionResolution {
                step_id: recipe_step.id.to_string(),
                stage_id: stage.id.to_string(),
                reason: format!(
                    "no action matched selector action={:?} all_of={:?} any_of={:?} none_of={:?} roles_all_of={:?} roles_any_of={:?} input_kinds_any_of={:?} output_kinds_any_of={:?}",
                    selector.action,
                    selector.all_of,
                    selector.any_of,
                    selector.none_of,
                    selector.roles_all_of,
                    selector.roles_any_of,
                    selector.input_kinds_any_of,
                    selector.output_kinds_any_of
                ),
            })
    }

    fn validate_explicit_action_against_selector(
        &self,
        recipe_step: &Step,
        stage: &RecipeStageTemplate,
        action: &str,
        selector: &ActionSelector,
    ) -> Result<(), RecipeCompileError> {
        if let Some(expected) = &selector.action {
            if expected != action {
                return Err(RecipeCompileError::ActionResolution {
                    step_id: recipe_step.id.to_string(),
                    stage_id: stage.id.to_string(),
                    reason: format!(
                        "explicit action '{}' does not match selector.action '{}'",
                        action, expected
                    ),
                });
            }
        }

        if selector.all_of.is_empty()
            && selector.any_of.is_empty()
            && selector.none_of.is_empty()
            && selector.roles_all_of.is_empty()
            && selector.roles_any_of.is_empty()
            && selector.input_kinds_any_of.is_empty()
            && selector.output_kinds_any_of.is_empty()
        {
            return Ok(());
        }

        let Some(meta) = self.action_catalog.get(action) else {
            return Ok(());
        };
        if !selector_matches(meta, selector) {
            return Err(RecipeCompileError::ActionResolution {
                step_id: recipe_step.id.to_string(),
                stage_id: stage.id.to_string(),
                reason: format!(
                    "explicit action '{}' does not satisfy selector all_of={:?} any_of={:?} none_of={:?} roles_all_of={:?} roles_any_of={:?} input_kinds_any_of={:?} output_kinds_any_of={:?}",
                    action,
                    selector.all_of,
                    selector.any_of,
                    selector.none_of,
                    selector.roles_all_of,
                    selector.roles_any_of,
                    selector.input_kinds_any_of,
                    selector.output_kinds_any_of
                ),
            });
        }
        Ok(())
    }
}

impl Default for RecipeCompiler {
    fn default() -> Self {
        Self::new()
    }
}

fn selector_matches(meta: &ActionMeta, selector: &ActionSelector) -> bool {
    if let Some(action) = &selector.action {
        if meta.name != *action {
            return false;
        }
    }
    if !selector.all_of.is_empty()
        && !selector
            .all_of
            .iter()
            .all(|capability| meta.has_capability(capability))
    {
        return false;
    }
    if !selector.none_of.is_empty()
        && selector
            .none_of
            .iter()
            .any(|capability| meta.has_capability(capability))
    {
        return false;
    }
    if !selector.any_of.is_empty()
        && !selector
            .any_of
            .iter()
            .any(|capability| meta.has_capability(capability))
    {
        return false;
    }
    if !selector.roles_all_of.is_empty()
        && !selector.roles_all_of.iter().all(|role| meta.has_role(role))
    {
        return false;
    }
    if !selector.roles_any_of.is_empty()
        && !selector.roles_any_of.iter().any(|role| meta.has_role(role))
    {
        return false;
    }
    if !selector.input_kinds_any_of.is_empty()
        && !selector
            .input_kinds_any_of
            .iter()
            .any(|kind| meta.supports_input_kind(kind))
    {
        return false;
    }
    if !selector.output_kinds_any_of.is_empty()
        && !selector
            .output_kinds_any_of
            .iter()
            .any(|kind| meta.supports_output_kind(kind))
    {
        return false;
    }
    true
}

fn selector_score(meta: &ActionMeta, selector: &ActionSelector) -> usize {
    let mut score = 0usize;
    if let Some(expected) = &selector.action {
        if meta.name == *expected {
            score += 100;
        }
    }
    score += selector.all_of.len() * 10;
    score += selector
        .any_of
        .iter()
        .filter(|item| meta.has_capability(item))
        .count()
        * 5;
    score += selector
        .roles_all_of
        .iter()
        .filter(|item| meta.has_role(item))
        .count()
        * 20;
    score += selector
        .roles_any_of
        .iter()
        .filter(|item| meta.has_role(item))
        .count()
        * 10;
    score += selector
        .input_kinds_any_of
        .iter()
        .filter(|item| meta.supports_input_kind(item))
        .count()
        * 4;
    score += selector
        .output_kinds_any_of
        .iter()
        .filter(|item| meta.supports_output_kind(item))
        .count()
        * 4;
    if meta.has_capability("fallback") {
        score = score.saturating_sub(25);
    }
    score
}

fn collect_recipe_input_aliases(step: &Step) -> HashMap<String, RecipeInputAlias> {
    step.io_bindings
        .iter()
        .map(|binding| {
            let dependency =
                parse_binding_source(&binding.from).map(|(source_step, _)| source_step.into());
            (
                binding.to.clone(),
                RecipeInputAlias {
                    source: binding.from.clone(),
                    dependency,
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests;
