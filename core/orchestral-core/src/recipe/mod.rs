use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::action::ActionMeta;
use crate::types::{Plan, Step, StepId, StepIoBinding, StepKind};

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

fn builtin_recipe_templates() -> Vec<RecipeTemplate> {
    vec![
        builtin_inspect_derive_apply_verify_template(),
        builtin_collect_derive_emit_template(),
        builtin_resolve_execute_verify_template(),
    ]
}

fn builtin_inspect_derive_apply_verify_template() -> RecipeTemplate {
    RecipeTemplate::new(
        "inspect_derive_apply_verify",
        vec![
            RecipeStageTemplate {
                id: StepId::from("inspect"),
                kind: StepKind::Action,
                action: None,
                selector: Some(
                    ActionSelector::default()
                        .with_roles_all_of(["inspect"])
                        .with_none_of(["fallback"]),
                ),
                depends_on: Vec::new(),
                exports: Vec::new(),
                io_bindings: Vec::new(),
                params: Value::Null,
                verify_with: None,
            },
            RecipeStageTemplate {
                id: StepId::from("derive"),
                kind: StepKind::Agent,
                action: None,
                selector: None,
                depends_on: vec![StepId::from("inspect")],
                exports: Vec::new(),
                io_bindings: Vec::new(),
                params: serde_json::json!({
                    "mode": "leaf"
                }),
                verify_with: None,
            },
            RecipeStageTemplate {
                id: StepId::from("apply"),
                kind: StepKind::Action,
                action: None,
                selector: Some(
                    ActionSelector::default()
                        .with_roles_all_of(["apply"])
                        .with_none_of(["fallback"]),
                ),
                depends_on: vec![StepId::from("derive")],
                exports: vec!["path".to_string()],
                io_bindings: Vec::new(),
                params: Value::Null,
                verify_with: Some(RecipeVerificationTemplate {
                    id: Some(StepId::from("verify")),
                    action: None,
                    selector: Some(
                        ActionSelector::default()
                            .with_roles_any_of(["verify"])
                            .with_none_of(["fallback"]),
                    ),
                    exports: Vec::new(),
                    io_bindings: vec![StepIoBinding::required("apply.path", "path")],
                    params: Value::Null,
                }),
            },
        ],
    )
    .with_description("Generic mutable workflow: inspect current state, derive a structured delta, apply it, then verify.")
}

fn builtin_collect_derive_emit_template() -> RecipeTemplate {
    RecipeTemplate::new(
        "collect_derive_emit",
        vec![
            RecipeStageTemplate {
                id: StepId::from("collect"),
                kind: StepKind::Action,
                action: None,
                selector: Some(
                    ActionSelector::default()
                        .with_roles_all_of(["collect"])
                        .with_none_of(["fallback"]),
                ),
                depends_on: Vec::new(),
                exports: Vec::new(),
                io_bindings: Vec::new(),
                params: Value::Null,
                verify_with: None,
            },
            RecipeStageTemplate {
                id: StepId::from("derive"),
                kind: StepKind::Agent,
                action: None,
                selector: None,
                depends_on: vec![StepId::from("collect")],
                exports: Vec::new(),
                io_bindings: Vec::new(),
                params: serde_json::json!({
                    "mode": "leaf"
                }),
                verify_with: None,
            },
            RecipeStageTemplate {
                id: StepId::from("emit"),
                kind: StepKind::Action,
                action: None,
                selector: Some(
                    ActionSelector::default()
                        .with_roles_all_of(["emit"])
                        .with_none_of(["fallback"]),
                ),
                depends_on: vec![StepId::from("derive")],
                exports: Vec::new(),
                io_bindings: Vec::new(),
                params: Value::Null,
                verify_with: None,
            },
        ],
    )
    .with_description("Generic read-only workflow: collect upstream facts, derive a structured result, then emit an artifact or response.")
}

fn builtin_resolve_execute_verify_template() -> RecipeTemplate {
    RecipeTemplate::new(
        "resolve_execute_verify",
        vec![
            RecipeStageTemplate {
                id: StepId::from("resolve"),
                kind: StepKind::Action,
                action: None,
                selector: Some(
                    ActionSelector::default()
                        .with_roles_all_of(["resolve"])
                        .with_none_of(["fallback"]),
                ),
                depends_on: Vec::new(),
                exports: Vec::new(),
                io_bindings: Vec::new(),
                params: Value::Null,
                verify_with: None,
            },
            RecipeStageTemplate {
                id: StepId::from("execute"),
                kind: StepKind::Action,
                action: None,
                selector: Some(
                    ActionSelector::default()
                        .with_roles_all_of(["execute"])
                        .with_none_of(["fallback"]),
                ),
                depends_on: vec![StepId::from("resolve")],
                exports: Vec::new(),
                io_bindings: Vec::new(),
                params: Value::Null,
                verify_with: Some(RecipeVerificationTemplate {
                    id: Some(StepId::from("verify")),
                    action: None,
                    selector: Some(
                        ActionSelector::default()
                            .with_roles_any_of(["verify"])
                            .with_none_of(["fallback"]),
                    ),
                    exports: Vec::new(),
                    io_bindings: Vec::new(),
                    params: Value::Null,
                }),
            },
        ],
    )
    .with_description("Generic side-effect workflow: resolve a target, execute the effect deterministically, then verify.")
}

fn parse_stage_overrides(
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

fn apply_stage_overrides_map(
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

fn merge_stage_override(stage: &mut RecipeStageTemplate, override_spec: RecipeStageOverride) {
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

fn expand_verification_contracts(
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

fn merge_json(base: Value, overlay: Value) -> Value {
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

fn rewrite_local_dep(dep: &StepId, recipe_step: &Step, local_ids: &HashSet<String>) -> StepId {
    if local_ids.contains(dep.as_str()) {
        StepId::new(format!("{}__{}", recipe_step.id, dep))
    } else {
        dep.clone()
    }
}

fn rewrite_local_io_bindings_with_aliases(
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

fn parse_export_from_object(
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

fn parse_export_bindings(
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

fn rewrite_step_references(steps: &mut [Step], aliases: &HashMap<String, RecipeAlias>) {
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

fn rewrite_plan_templates(plan: &mut Plan, aliases: &HashMap<String, RecipeAlias>) {
    if let Some(template) = &mut plan.on_complete {
        rewrite_template(template, aliases);
    }
    if let Some(template) = &mut plan.on_failure {
        rewrite_template(template, aliases);
    }
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

fn rewrite_stage_param_templates(
    params: &mut Value,
    recipe_step: &Step,
    local_ids: &HashSet<String>,
    recipe_inputs: &HashMap<String, RecipeInputAlias>,
) -> Vec<StepId> {
    let mut deps = Vec::new();
    rewrite_value_templates(params, recipe_step, local_ids, recipe_inputs, &mut deps);
    deps
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

fn dedup_step_ids(values: &mut Vec<StepId>) {
    let mut seen = HashSet::new();
    values.retain(|value| seen.insert(value.to_string()));
}

fn parse_binding_source(value: &str) -> Option<(String, &str)> {
    let (source_step, source_key) = value.split_once('.')?;
    if source_step.is_empty() || source_key.is_empty() {
        return None;
    }
    Some((source_step.to_string(), source_key))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::types::{Step, StepIoBinding};

    #[test]
    fn test_recipe_compiler_lowers_local_stages_and_rewrites_exports() {
        let mut plan = Plan::new(
            "compile recipe",
            vec![
                Step::recipe("r1")
                    .with_exports(vec!["change_spec".to_string(), "updated_path".to_string()])
                    .with_params(json!({
                        "stages": [
                            {
                                "id": "inspect",
                                "kind": "action",
                                "action": "inspect_doc",
                                "exports": ["view"]
                            },
                            {
                                "id": "derive",
                                "kind": "agent",
                                "params": {
                                    "mode": "leaf",
                                    "goal": "derive patch",
                                    "output_keys": ["change_spec"]
                                }
                            },
                            {
                                "id": "apply",
                                "kind": "action",
                                "action": "apply_patch",
                                "io_bindings": [
                                    {"from": "derive.change_spec", "to": "patch", "required": true}
                                ],
                                "exports": ["updated_path"]
                            }
                        ],
                        "export_from": {
                            "change_spec": "derive.change_spec",
                            "updated_path": "apply.updated_path"
                        }
                    })),
                Step::action("s2", "finalize").with_io_bindings(vec![
                    StepIoBinding::required("r1.change_spec", "patch"),
                    StepIoBinding::required("r1.updated_path", "path"),
                ]),
            ],
        );

        let compiled = RecipeCompiler::new().compile(&mut plan).expect("compile");
        assert!(compiled);
        assert_eq!(plan.steps.len(), 4);
        assert_eq!(plan.steps[0].id.as_str(), "r1__inspect");
        assert_eq!(plan.steps[1].id.as_str(), "r1__derive");
        assert_eq!(plan.steps[2].id.as_str(), "r1__apply");
        let final_step = plan.get_step("s2").expect("s2");
        assert_eq!(final_step.io_bindings[0].from, "r1__derive.change_spec");
        assert_eq!(final_step.io_bindings[1].from, "r1__apply.updated_path");
        assert_eq!(plan.steps[2].depends_on, vec![StepId::from("r1__derive")]);
    }

    #[test]
    fn test_recipe_compiler_attaches_recipe_inputs_to_first_stage() {
        let mut plan = Plan::new(
            "attach inputs",
            vec![Step::recipe("r1")
                .with_depends_on(vec![StepId::from("s0")])
                .with_io_bindings(vec![StepIoBinding::required("s0.path", "path")])
                .with_params(json!({
                    "stages": [
                        {
                            "id": "inspect",
                            "kind": "action",
                            "action": "file_read"
                        }
                    ]
                }))],
        );

        RecipeCompiler::new().compile(&mut plan).expect("compile");
        let first = plan.get_step("r1__inspect").expect("first stage");
        assert_eq!(first.depends_on, vec![StepId::from("s0")]);
        assert_eq!(first.io_bindings.len(), 1);
        assert_eq!(first.io_bindings[0].from, "s0.path");
    }

    #[test]
    fn test_recipe_compiler_rewrites_recipe_input_placeholders_in_stage_params() {
        let mut plan = Plan::new(
            "rewrite recipe inputs",
            vec![Step::recipe("r1")
                .with_depends_on(vec![StepId::from("s0")])
                .with_io_bindings(vec![StepIoBinding::required("s0.stdout", "excel_path")])
                .with_params(json!({
                    "stages": [
                        {
                            "id": "unpack",
                            "kind": "action",
                            "action": "shell",
                            "params": {
                                "command": "python",
                                "args": ["scripts/unpack.py", "{{excel_path}}"]
                            }
                        }
                    ]
                }))],
        );

        RecipeCompiler::new().compile(&mut plan).expect("compile");
        let unpack = plan.get_step("r1__unpack").expect("unpack stage");
        assert_eq!(unpack.depends_on, vec![StepId::from("s0")]);
        assert_eq!(
            unpack.params.pointer("/args/1"),
            Some(&json!("{{s0.stdout}}"))
        );
    }

    #[test]
    fn test_recipe_compiler_selects_actions_by_role() {
        let mut compiler = RecipeCompiler::new();
        compiler.register_action_meta(
            &ActionMeta::new("inspect_doc", "inspect")
                .with_capabilities(["filesystem_read"])
                .with_roles(["inspect"]),
        );
        compiler.register_action_meta(
            &ActionMeta::new("persist_doc", "persist")
                .with_capabilities(["filesystem_write"])
                .with_roles(["apply"]),
        );
        compiler.register_action_meta(
            &ActionMeta::new("verify_doc", "verify")
                .with_capabilities(["verification"])
                .with_roles(["verify"]),
        );

        let mut plan = Plan::new(
            "template recipe",
            vec![Step::recipe("r1").with_params(json!({
                "template": "inspect_derive_apply_verify",
                "stage_overrides": {
                    "inspect": {
                        "exports": ["content"]
                    },
                    "derive": {
                        "params": {
                            "goal": "derive change spec",
                            "output_keys": ["change_spec"]
                        },
                        "io_bindings": [
                            {"from": "inspect.content", "to": "source_content", "required": true}
                        ]
                    },
                    "apply": {
                        "exports": ["path"],
                        "io_bindings": [
                            {"from": "derive.change_spec", "to": "content", "required": true}
                        ]
                    },
                    "verify": {
                        "exports": ["verified"],
                        "io_bindings": [
                            {"from": "apply.path", "to": "path", "required": true}
                        ]
                    }
                },
                "export_from": {
                    "updated_path": "apply.path"
                }
            }))],
        );

        compiler.compile(&mut plan).expect("compile");
        assert_eq!(plan.steps.len(), 4);
        assert_eq!(plan.steps[0].action, "inspect_doc");
        assert_eq!(plan.steps[1].kind, StepKind::Agent);
        assert_eq!(plan.steps[2].action, "persist_doc");
        assert_eq!(plan.steps[3].action, "verify_doc");
    }

    #[test]
    fn test_recipe_compiler_prefers_non_fallback_action_for_builtin_template() {
        let mut compiler = RecipeCompiler::new();
        compiler.register_action_meta(
            &ActionMeta::new("file_read", "read file")
                .with_roles(["inspect"])
                .with_capabilities(["filesystem_read"]),
        );
        compiler.register_action_meta(
            &ActionMeta::new("file_write", "write file")
                .with_roles(["apply"])
                .with_capabilities(["filesystem_write"]),
        );
        compiler.register_action_meta(
            &ActionMeta::new("file_verify", "verify file")
                .with_roles(["verify"])
                .with_capabilities(["verification"]),
        );
        compiler.register_action_meta(
            &ActionMeta::new("shell", "fallback shell")
                .with_roles(["inspect", "apply", "verify"])
                .with_capabilities(["shell", "fallback", "filesystem_write", "verification"]),
        );

        let mut plan = Plan::new(
            "prefer specific action",
            vec![Step::recipe("r1").with_params(json!({
                "template": "inspect_derive_apply_verify"
            }))],
        );

        compiler.compile(&mut plan).expect("compile");
        assert_eq!(
            plan.get_step("r1__inspect")
                .map(|step| step.action.as_str()),
            Some("file_read")
        );
        assert_eq!(
            plan.get_step("r1__apply").map(|step| step.action.as_str()),
            Some("file_write")
        );
        assert_eq!(
            plan.get_step("r1__verify").map(|step| step.action.as_str()),
            Some("file_verify")
        );
    }

    #[test]
    fn test_recipe_compiler_rejects_unknown_stage_override() {
        let mut plan = Plan::new(
            "bad override",
            vec![Step::recipe("r1").with_params(json!({
                "template": "inspect_derive_apply_verify",
                "stage_overrides": {
                    "unknown": {
                        "action": "echo"
                    }
                }
            }))],
        );

        let err = RecipeCompiler::new()
            .compile(&mut plan)
            .expect_err("expected compile error");
        match err {
            RecipeCompileError::InvalidRecipe { reason, .. } => {
                assert!(reason.contains("unknown stage"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_recipe_compiler_expands_verify_with_contract() {
        let mut compiler = RecipeCompiler::new();
        compiler.register_action_meta(
            &ActionMeta::new("write_doc", "write").with_capabilities(["filesystem_write"]),
        );
        compiler.register_action_meta(
            &ActionMeta::new("verify_doc", "verify").with_capabilities(["verification"]),
        );

        let mut plan = Plan::new(
            "verify contract",
            vec![Step::recipe("r1").with_params(json!({
                "stages": [
                    {
                        "id": "apply",
                        "kind": "action",
                        "selector": { "all_of": ["filesystem_write"] },
                        "exports": ["path"],
                        "verify_with": {
                            "selector": { "any_of": ["verification"] },
                            "exports": ["verified"]
                        }
                    }
                ],
                "export_from": {
                    "verified": "apply_verify.verified"
                }
            }))],
        );

        compiler.compile(&mut plan).expect("compile");
        assert_eq!(plan.steps.len(), 2);
        assert_eq!(plan.steps[0].id.as_str(), "r1__apply");
        assert_eq!(plan.steps[0].action, "write_doc");
        assert_eq!(plan.steps[1].id.as_str(), "r1__apply_verify");
        assert_eq!(plan.steps[1].action, "verify_doc");
        assert_eq!(plan.steps[1].depends_on, vec![StepId::from("r1__apply")]);
        assert_eq!(plan.steps[1].io_bindings[0].from, "r1__apply.path");
    }
}
