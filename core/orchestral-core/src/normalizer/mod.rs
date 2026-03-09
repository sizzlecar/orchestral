//! Plan Normalizer module
//!
//! The Plan Normalizer is the stability core of Orchestral.
//! It is NOT an optional component.
//!
//! Responsibilities:
//! - Validate plan correctness
//! - Inject implicit steps when necessary
//! - Repair common LLM planning errors
//! - Produce an executable DAG for the executor

use serde_json::Value;
use thiserror::Error;

use crate::action::ActionMeta;
use crate::executor::ExecutionDag;
use crate::recipe::{RecipeCompileError, RecipeCompiler, RecipeTemplate};
use crate::types::{Plan, StepId, StepKind};

/// Validation errors
#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("DAG contains cycle involving step: {0}")]
    CycleDetected(String),

    #[error("Unknown action: {0}")]
    UnknownAction(String),

    #[error("Missing dependency: step '{0}' depends on undefined step '{1}'")]
    MissingDependency(String, String),

    #[error("Duplicate step ID: {0}")]
    DuplicateStepId(String),

    #[error("Empty plan")]
    EmptyPlan,

    #[error("Invalid io binding in step '{0}': {1}")]
    InvalidIoBinding(String, String),

    #[error("Step '{0}' binds from '{1}' but does not depend on that step")]
    IoBindingMissingDependency(String, String),

    #[error("Invalid agent params in step '{0}': {1}")]
    InvalidAgentParams(String, String),
}

/// Fix errors
#[derive(Debug, Error)]
pub enum FixError {
    #[error("Unable to fix: {0}")]
    UnableToFix(String),

    #[error("Fix caused new error: {0}")]
    FixCausedError(String),
}

/// Normalization errors
#[derive(Debug, Error)]
pub enum NormalizeError {
    #[error("recipe compile failed: {0}")]
    RecipeCompile(#[from] RecipeCompileError),

    #[error("Validation failed: {0}")]
    Validation(#[from] ValidationError),

    #[error("Fix failed: {0}")]
    Fix(#[from] FixError),

    #[error("Failed to build DAG: {0}")]
    DagBuild(String),
}

/// Plan validator trait
pub trait PlanValidator: Send + Sync {
    /// Validate a plan
    fn validate(&self, plan: &Plan) -> Result<(), ValidationError>;
}

/// Plan fixer trait
pub trait PlanFixer: Send + Sync {
    /// Attempt to fix a plan
    /// Returns true if any fixes were applied
    fn fix(&self, plan: &mut Plan) -> Result<bool, FixError>;
}

/// Normalized plan ready for execution
#[derive(Debug)]
pub struct NormalizedPlan {
    /// The validated and potentially fixed plan
    pub plan: Plan,
    /// The executable DAG
    pub dag: ExecutionDag,
}

/// Plan normalizer - the stability core
pub struct PlanNormalizer {
    validators: Vec<Box<dyn PlanValidator>>,
    fixers: Vec<Box<dyn PlanFixer>>,
    recipe_compiler: RecipeCompiler,
    /// Known action contracts keyed by action name.
    known_actions: std::collections::HashMap<String, ActionContract>,
}

impl PlanNormalizer {
    /// Create a new normalizer
    pub fn new() -> Self {
        Self {
            validators: Vec::new(),
            fixers: Vec::new(),
            recipe_compiler: RecipeCompiler::new(),
            known_actions: std::collections::HashMap::new(),
        }
    }

    /// Add a validator
    pub fn add_validator(&mut self, validator: Box<dyn PlanValidator>) {
        self.validators.push(validator);
    }

    /// Add a fixer
    pub fn add_fixer(&mut self, fixer: Box<dyn PlanFixer>) {
        self.fixers.push(fixer);
    }

    /// Register a known action
    pub fn register_action(&mut self, name: impl Into<String>) {
        let name = name.into();
        self.recipe_compiler.register_action(name.clone());
        self.known_actions.entry(name).or_default();
    }

    /// Register a known action with metadata-derived contract.
    pub fn register_action_meta(&mut self, meta: &ActionMeta) {
        self.recipe_compiler.register_action_meta(meta);
        let output_keys = output_keys_from_schema(&meta.output_schema);
        self.known_actions
            .insert(meta.name.clone(), ActionContract { output_keys });
    }

    /// Register a reusable recipe template.
    pub fn register_recipe_template(&mut self, template: RecipeTemplate) {
        self.recipe_compiler.register_template(template);
    }

    /// Normalize a plan
    pub fn normalize(&self, mut plan: Plan) -> Result<NormalizedPlan, NormalizeError> {
        self.recipe_compiler.compile(&mut plan)?;

        // Step 1: Run fixers (they may fix issues before validation)
        for fixer in &self.fixers {
            fixer.fix(&mut plan)?;
        }

        // Step 2: Apply built-in normalization derived from action contracts.
        self.apply_implicit_contracts(&mut plan);

        // Step 2.5: Control-flow steps (replan/wait) without explicit depends_on
        // must depend on all preceding steps to avoid premature execution.
        fix_control_flow_dependencies(&mut plan);

        // Step 3: Run built-in validations
        self.validate_basic(&plan)?;

        // Step 4: Run custom validators
        for validator in &self.validators {
            validator.validate(&plan)?;
        }

        // Step 5: Build the execution DAG
        let dag = self.build_dag(&plan)?;

        Ok(NormalizedPlan { plan, dag })
    }

    /// Basic built-in validations
    fn validate_basic(&self, plan: &Plan) -> Result<(), ValidationError> {
        // Check for empty plan
        if plan.steps.is_empty() {
            return Err(ValidationError::EmptyPlan);
        }

        // Check for duplicate step IDs
        let mut seen_ids = std::collections::HashSet::new();
        for step in &plan.steps {
            if !seen_ids.insert(&step.id) {
                return Err(ValidationError::DuplicateStepId(step.id.to_string()));
            }
        }

        // Check that all dependencies exist
        let step_ids: std::collections::HashSet<_> =
            plan.steps.iter().map(|s| s.id.as_str()).collect();
        for step in &plan.steps {
            for dep in &step.depends_on {
                if !step_ids.contains(dep.as_str()) {
                    return Err(ValidationError::MissingDependency(
                        step.id.to_string(),
                        dep.to_string(),
                    ));
                }
            }

            for binding in &step.io_bindings {
                let (source_step, source_key) = match parse_step_binding_source(&binding.from) {
                    Some(parts) => parts,
                    None => continue,
                };

                if !step_ids.contains(source_step.as_str()) {
                    return Err(ValidationError::InvalidIoBinding(
                        step.id.to_string(),
                        format!("unknown source step '{}'", source_step),
                    ));
                }
                if source_step.as_str() != step.id.as_str()
                    && !step
                        .depends_on
                        .iter()
                        .any(|dep| dep.as_str() == source_step.as_str())
                {
                    return Err(ValidationError::IoBindingMissingDependency(
                        step.id.to_string(),
                        source_step,
                    ));
                }

                if let Some(source) = plan.get_step(&source_step) {
                    if !source.exports.is_empty() && !source.exports.iter().any(|k| k == source_key)
                    {
                        return Err(ValidationError::InvalidIoBinding(
                            step.id.to_string(),
                            format!(
                                "source key '{}' not declared in step '{}' exports",
                                source_key, source.id
                            ),
                        ));
                    }
                }
            }

            validate_agent_params(step)?;
        }

        // Check for unknown actions (if we have a registry)
        if !self.known_actions.is_empty() {
            for step in &plan.steps {
                if step.kind != StepKind::Action {
                    continue;
                }
                if !self.known_actions.contains_key(&step.action) {
                    return Err(ValidationError::UnknownAction(step.action.clone()));
                }
            }
        }

        // Check for cycles using DFS
        self.detect_cycles(plan)?;

        Ok(())
    }

    /// Detect cycles in the dependency graph
    fn detect_cycles(&self, plan: &Plan) -> Result<(), ValidationError> {
        use std::collections::{HashMap, HashSet};

        // Build adjacency list
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        for step in &plan.steps {
            adj.entry(step.id.as_str()).or_default();
            for dep in &step.depends_on {
                adj.entry(dep.as_str()).or_default().push(step.id.as_str());
            }
        }

        // DFS for cycle detection
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();

        fn dfs<'a>(
            node: &'a str,
            adj: &HashMap<&'a str, Vec<&'a str>>,
            visited: &mut HashSet<&'a str>,
            rec_stack: &mut HashSet<&'a str>,
        ) -> Option<&'a str> {
            visited.insert(node);
            rec_stack.insert(node);

            if let Some(neighbors) = adj.get(node) {
                for &neighbor in neighbors {
                    if !visited.contains(neighbor) {
                        if let Some(cycle_node) = dfs(neighbor, adj, visited, rec_stack) {
                            return Some(cycle_node);
                        }
                    } else if rec_stack.contains(neighbor) {
                        return Some(neighbor);
                    }
                }
            }

            rec_stack.remove(node);
            None
        }

        for step in &plan.steps {
            if !visited.contains(step.id.as_str()) {
                if let Some(cycle_node) = dfs(step.id.as_str(), &adj, &mut visited, &mut rec_stack)
                {
                    return Err(ValidationError::CycleDetected(cycle_node.to_string()));
                }
            }
        }

        Ok(())
    }

    /// Build an execution DAG from a validated plan
    fn build_dag(&self, plan: &Plan) -> Result<ExecutionDag, NormalizeError> {
        ExecutionDag::from_plan(plan).map_err(NormalizeError::DagBuild)
    }
}

#[derive(Debug, Default, Clone)]
struct ActionContract {
    output_keys: Vec<String>,
}

impl PlanNormalizer {
    fn apply_implicit_contracts(&self, plan: &mut Plan) {
        for step in &mut plan.steps {
            let original_kind = step.kind.clone();
            let original_depends_on = step.depends_on.clone();
            let original_exports = step.exports.clone();
            let original_params = step.params.clone();
            infer_special_step_kind(step);
            apply_agent_defaults(step);
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

fn fix_control_flow_dependencies(plan: &mut Plan) {
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

fn infer_special_step_kind(step: &mut crate::types::Step) {
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

fn apply_agent_defaults(step: &mut crate::types::Step) {
    if step.kind != StepKind::Agent {
        return;
    }

    if step.params.is_null() {
        step.params = Value::Object(serde_json::Map::new());
    }

    if let Some(params) = step.params.as_object_mut() {
        normalize_agent_output_rules(params);
        repair_leaf_mode_for_read_only_inputs(params);
        if agent_mode_from_params(params) == Some(AgentMode::Leaf) {
            append_leaf_goal_guard(params);
            let result_slot = params
                .entry("result_slot".to_string())
                .or_insert(Value::String("leaf_result".to_string()))
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or("leaf_result")
                .to_string();
            params
                .entry("allowed_actions".to_string())
                .or_insert_with(|| serde_json::json!(["json_stdout"]));
            params
                .entry("max_iterations".to_string())
                .or_insert(Value::from(1_u64));
            let output_keys = params
                .get("output_keys")
                .and_then(|value| value.as_array())
                .map(|keys| {
                    keys.iter()
                        .filter_map(|value| value.as_str().map(str::to_string))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if !output_keys.is_empty() {
                ensure_leaf_output_rules(params, &result_slot, &output_keys);
            }
        } else {
            params
                .entry("max_iterations".to_string())
                .or_insert(Value::from(5_u64));
        }
    }
}

fn repair_leaf_mode_for_read_only_inputs(params: &mut serde_json::Map<String, Value>) {
    if agent_mode_from_params(params) != Some(AgentMode::Leaf) {
        return;
    }

    let Some(actions) = params.get("allowed_actions").and_then(Value::as_array) else {
        return;
    };
    let extra_actions = actions
        .iter()
        .filter_map(Value::as_str)
        .filter(|action| *action != "json_stdout")
        .collect::<Vec<_>>();
    if extra_actions.is_empty() {
        return;
    }
    if extra_actions.iter().all(|action| *action == "file_read")
        && !output_rules_require_explicit_action(params)
    {
        params.insert("mode".to_string(), Value::String("explore".to_string()));
        params.remove("result_slot");
    }
}

fn normalize_agent_output_rules(params: &mut serde_json::Map<String, Value>) {
    let Some(raw_rules) = params.get("output_rules").cloned() else {
        return;
    };
    let Some(items) = raw_rules.as_array() else {
        return;
    };

    let mut normalized = serde_json::Map::new();
    for item in items {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let Some(slot) = obj
            .get("slot")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let mut rule = obj.clone();
        rule.remove("slot");
        normalized.insert(slot.to_string(), Value::Object(rule));
    }

    if !normalized.is_empty() {
        params.insert("output_rules".to_string(), Value::Object(normalized));
    }
}

fn append_leaf_goal_guard(params: &mut serde_json::Map<String, Value>) {
    let Some(goal) = params.get("goal").and_then(Value::as_str) else {
        return;
    };
    if goal.contains("Use only the provided bound inputs.")
        || goal.contains("Do not run commands, read files, or inspect external tools.")
    {
        return;
    }

    params.insert(
        "goal".to_string(),
        Value::String(format!(
            "{} Use only the provided bound inputs. Do not run commands, read files, or inspect external tools. Return only the final structured result for the declared output keys.",
            goal.trim()
        )),
    );
}

fn ensure_leaf_output_rules(
    params: &mut serde_json::Map<String, Value>,
    result_slot: &str,
    output_keys: &[String],
) {
    let default_rules = build_leaf_output_rules(result_slot, output_keys);
    let Some(default_obj) = default_rules.as_object() else {
        return;
    };

    let mut normalized = params
        .get("output_rules")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut changed = !params.get("output_rules").is_some_and(Value::is_object);

    for output_key in output_keys {
        let valid = normalized
            .get(output_key)
            .is_some_and(is_valid_leaf_output_rule);
        if valid {
            continue;
        }
        if let Some(default_rule) = default_obj.get(output_key) {
            normalized.insert(output_key.clone(), default_rule.clone());
            changed = true;
        }
    }

    if changed {
        params.insert("output_rules".to_string(), Value::Object(normalized));
    }
}

fn is_valid_leaf_output_rule(rule: &Value) -> bool {
    let Some(obj) = rule.as_object() else {
        return false;
    };
    if obj
        .get("slot")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty())
    {
        return true;
    }
    if obj
        .get("template")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty())
    {
        return true;
    }
    if obj
        .get("fallback_aliases")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            !items.is_empty()
                && items
                    .iter()
                    .all(|value| value.as_str().is_some_and(|entry| !entry.trim().is_empty()))
        })
    {
        return true;
    }
    obj.get("candidates")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            !items.is_empty()
                && items.iter().all(|candidate| {
                    candidate
                        .as_object()
                        .and_then(|obj| obj.get("slot"))
                        .and_then(Value::as_str)
                        .is_some_and(|slot| !slot.trim().is_empty())
                })
        })
}

fn validate_agent_params(step: &crate::types::Step) -> Result<(), ValidationError> {
    if step.kind != StepKind::Agent {
        return Ok(());
    }

    let Some(params) = step.params.as_object() else {
        return Err(ValidationError::InvalidAgentParams(
            step.id.to_string(),
            "params must be an object".to_string(),
        ));
    };

    let goal = params.get("goal").and_then(|v| v.as_str()).map(str::trim);
    if goal.is_none() || goal == Some("") {
        return Err(ValidationError::InvalidAgentParams(
            step.id.to_string(),
            "goal must be a non-empty string".to_string(),
        ));
    }

    let mode = agent_mode_from_params(params).ok_or_else(|| {
        ValidationError::InvalidAgentParams(
            step.id.to_string(),
            "mode must be 'explore' or 'leaf' when provided".to_string(),
        )
    })?;

    let Some(allowed_actions) = params.get("allowed_actions").and_then(|v| v.as_array()) else {
        return Err(ValidationError::InvalidAgentParams(
            step.id.to_string(),
            "allowed_actions must be a non-empty string array".to_string(),
        ));
    };
    if allowed_actions.is_empty()
        || allowed_actions.iter().any(|v| {
            v.as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .is_none()
        })
    {
        return Err(ValidationError::InvalidAgentParams(
            step.id.to_string(),
            "allowed_actions must be a non-empty string array".to_string(),
        ));
    }

    let max_iterations = params.get("max_iterations").and_then(|v| v.as_u64());
    if !(Some(1)..=Some(10)).contains(&max_iterations) {
        return Err(ValidationError::InvalidAgentParams(
            step.id.to_string(),
            "max_iterations must be an integer between 1 and 10".to_string(),
        ));
    }

    let Some(output_keys) = params.get("output_keys").and_then(|v| v.as_array()) else {
        return Err(ValidationError::InvalidAgentParams(
            step.id.to_string(),
            "output_keys must be a non-empty string array".to_string(),
        ));
    };
    if output_keys.is_empty()
        || output_keys.iter().any(|v| {
            v.as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .is_none()
        })
    {
        return Err(ValidationError::InvalidAgentParams(
            step.id.to_string(),
            "output_keys must be a non-empty string array".to_string(),
        ));
    }

    if mode == AgentMode::Leaf {
        if !(Some(1)..=Some(3)).contains(&max_iterations) {
            return Err(ValidationError::InvalidAgentParams(
                step.id.to_string(),
                "leaf agent max_iterations must be an integer between 1 and 3".to_string(),
            ));
        }
        if allowed_actions
            .iter()
            .any(|value| value.as_str() != Some("json_stdout"))
        {
            return Err(ValidationError::InvalidAgentParams(
                step.id.to_string(),
                "leaf agent allowed_actions may only contain 'json_stdout'".to_string(),
            ));
        }
        let result_slot = params.get("result_slot").and_then(|value| value.as_str());
        if result_slot
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_none()
        {
            return Err(ValidationError::InvalidAgentParams(
                step.id.to_string(),
                "leaf agent result_slot must be a non-empty string".to_string(),
            ));
        }
    }

    if mode == AgentMode::Explore && output_rules_require_explicit_action(params) {
        return Err(ValidationError::InvalidAgentParams(
            step.id.to_string(),
            "explore agent may not own side-effect-sensitive outputs with requires.action; split the workflow into explicit action/recipe stages and keep only local derivation in a leaf agent".to_string(),
        ));
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentMode {
    Explore,
    Leaf,
}

fn agent_mode_from_params(params: &serde_json::Map<String, Value>) -> Option<AgentMode> {
    match params.get("mode").and_then(|value| value.as_str()) {
        None | Some("explore") => Some(AgentMode::Explore),
        Some("leaf") => Some(AgentMode::Leaf),
        Some(_) => None,
    }
}

fn build_leaf_output_rules(result_slot: &str, output_keys: &[String]) -> Value {
    let mut output_rules = serde_json::Map::new();
    for output_key in output_keys {
        output_rules.insert(
            output_key.clone(),
            serde_json::json!({
                "candidates": [
                    {
                        "slot": result_slot,
                        "path": output_key,
                        "requires": {
                            "action": "json_stdout"
                        }
                    }
                ]
            }),
        );
    }
    Value::Object(output_rules)
}

fn output_rules_require_explicit_action(params: &serde_json::Map<String, Value>) -> bool {
    let Some(output_rules) = params
        .get("output_rules")
        .and_then(|value| value.as_object())
    else {
        return false;
    };

    output_rules.values().any(output_rule_requires_action)
}

fn output_rule_requires_action(rule: &Value) -> bool {
    let Some(rule_obj) = rule.as_object() else {
        return false;
    };

    if requires_action(rule_obj.get("requires")) || requires_action(rule_obj.get("requires_action"))
    {
        return true;
    }

    rule_obj
        .get("candidates")
        .and_then(|value| value.as_array())
        .map(|candidates| {
            candidates.iter().any(|candidate| {
                let Some(candidate_obj) = candidate.as_object() else {
                    return false;
                };
                requires_action(candidate_obj.get("requires"))
                    || requires_action(candidate_obj.get("requires_action"))
            })
        })
        .unwrap_or(false)
}

fn requires_action(value: Option<&Value>) -> bool {
    match value {
        Some(Value::String(action)) => !action.trim().is_empty(),
        Some(Value::Object(obj)) => obj
            .get("action")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some(),
        _ => false,
    }
}

fn agent_output_keys(step: &crate::types::Step) -> Option<Vec<String>> {
    if step.kind != StepKind::Agent {
        return None;
    }
    step.params
        .get("output_keys")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect::<Vec<_>>()
        })
}

fn derive_depends_on_from_bindings(step: &mut crate::types::Step) {
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

fn parse_step_binding_source(value: &str) -> Option<(String, &str)> {
    let (source_step, source_key) = value.split_once('.')?;
    if source_step.is_empty() || source_key.is_empty() {
        return None;
    }
    Some((source_step.to_string(), source_key))
}

fn output_keys_from_schema(schema: &serde_json::Value) -> Vec<String> {
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

impl Default for PlanNormalizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Step, StepIoBinding};
    use serde_json::json;

    #[test]
    fn test_normalizer_derives_depends_on_from_io_bindings() {
        let mut normalizer = PlanNormalizer::new();
        normalizer.register_action("produce");
        normalizer.register_action("consume");

        let plan = Plan::new(
            "derive deps",
            vec![
                Step::action("s1", "produce"),
                Step::action("s2", "consume")
                    .with_io_bindings(vec![StepIoBinding::required("s1.result", "content")]),
            ],
        );

        let normalized = normalizer.normalize(plan).expect("normalize");
        let step2 = normalized.plan.get_step("s2").expect("s2");
        assert_eq!(step2.depends_on, vec![crate::types::StepId::from("s1")]);
    }

    #[test]
    fn test_normalizer_populates_exports_from_action_output_schema() {
        let mut normalizer = PlanNormalizer::new();
        normalizer.register_action_meta(&ActionMeta::new("write_doc", "write").with_output_schema(
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "bytes": { "type": "integer" }
                },
                "required": ["path", "bytes"]
            }),
        ));

        let plan = Plan::new("exports", vec![Step::action("s1", "write_doc")]);
        let normalized = normalizer.normalize(plan).expect("normalize");
        let step = normalized.plan.get_step("s1").expect("s1");
        assert_eq!(step.exports, vec!["path".to_string(), "bytes".to_string()]);
    }

    #[test]
    fn test_normalizer_infers_wait_user_kind_from_action_name() {
        let normalizer = PlanNormalizer::new();
        let plan = Plan::new("wait", vec![Step::action("s1", "wait_user")]);
        let normalized = normalizer.normalize(plan).expect("normalize");
        let step = normalized.plan.get_step("s1").expect("s1");
        assert_eq!(step.kind, StepKind::WaitUser);
    }

    #[test]
    fn test_normalizer_infers_agent_kind_and_applies_default_iterations() {
        let normalizer = PlanNormalizer::new();
        let plan = Plan::new(
            "agent",
            vec![Step::action("s1", "agent").with_params(json!({
                "goal": "inspect workbook structure",
                "allowed_actions": ["file_read", "shell"],
                "output_keys": ["inspection"]
            }))],
        );

        let normalized = normalizer.normalize(plan).expect("normalize");
        let step = normalized.plan.get_step("s1").expect("s1");
        assert_eq!(step.kind, StepKind::Agent);
        assert_eq!(
            step.params
                .get("max_iterations")
                .and_then(|v| v.as_u64())
                .expect("max_iterations"),
            5
        );
    }

    #[test]
    fn test_normalizer_rejects_agent_params_without_goal() {
        let normalizer = PlanNormalizer::new();
        let plan = Plan::new(
            "agent-invalid",
            vec![Step::agent("s1").with_params(json!({
                "allowed_actions": ["file_read"],
                "max_iterations": 3,
                "output_keys": ["summary"]
            }))],
        );

        let err = normalizer
            .normalize(plan)
            .expect_err("expected validation error");
        match err {
            NormalizeError::Validation(ValidationError::InvalidAgentParams(step_id, reason)) => {
                assert_eq!(step_id, "s1");
                assert!(reason.contains("goal"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn test_normalizer_rejects_agent_params_when_iterations_out_of_range() {
        let normalizer = PlanNormalizer::new();
        let plan = Plan::new(
            "agent-invalid",
            vec![Step::agent("s1").with_params(json!({
                "goal": "inspect",
                "allowed_actions": ["file_read"],
                "max_iterations": 99,
                "output_keys": ["summary"]
            }))],
        );

        let err = normalizer
            .normalize(plan)
            .expect_err("expected validation error");
        match err {
            NormalizeError::Validation(ValidationError::InvalidAgentParams(step_id, reason)) => {
                assert_eq!(step_id, "s1");
                assert!(reason.contains("max_iterations"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn test_normalizer_populates_agent_exports_from_output_keys() {
        let normalizer = PlanNormalizer::new();
        let plan = Plan::new(
            "agent-exports",
            vec![Step::agent("s1").with_params(json!({
                "goal": "inspect",
                "allowed_actions": ["file_read"],
                "max_iterations": 3,
                "output_keys": ["summary", "next_step"]
            }))],
        );

        let normalized = normalizer.normalize(plan).expect("normalize");
        let step = normalized.plan.get_step("s1").expect("s1");
        assert_eq!(
            step.exports,
            vec!["summary".to_string(), "next_step".to_string()]
        );
    }

    #[test]
    fn test_normalizer_applies_leaf_agent_defaults() {
        let normalizer = PlanNormalizer::new();
        let plan = Plan::new(
            "leaf-defaults",
            vec![Step::leaf_agent("s1").with_params(json!({
                "mode": "leaf",
                "goal": "derive a patch",
                "output_keys": ["change_spec"]
            }))],
        );

        let normalized = normalizer.normalize(plan).expect("normalize");
        let step = normalized.plan.get_step("s1").expect("s1");
        assert_eq!(step.kind, StepKind::Agent);
        assert_eq!(
            step.params.get("allowed_actions"),
            Some(&json!(["json_stdout"]))
        );
        assert_eq!(step.params.get("max_iterations"), Some(&json!(1)));
        assert_eq!(step.params.get("result_slot"), Some(&json!("leaf_result")));
        assert_eq!(
            step.params
                .pointer("/output_rules/change_spec/candidates/0/slot"),
            Some(&json!("leaf_result"))
        );
        assert!(step
            .params
            .get("goal")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .contains("Use only the provided bound inputs."));
    }

    #[test]
    fn test_normalizer_converts_legacy_output_rules_array_for_leaf_agents() {
        let normalizer = PlanNormalizer::new();
        let plan = Plan::new(
            "legacy-output-rules",
            vec![Step::leaf_agent("s1").with_params(json!({
                "mode": "leaf",
                "goal": "derive fill specification",
                "output_keys": ["fill_specification"],
                "output_rules": [
                    {
                        "slot": "fill_specification",
                        "candidates": [
                            {
                                "slot": "leaf_result",
                                "path": "fill_specification",
                                "requires": { "action": "json_stdout" }
                            }
                        ]
                    }
                ]
            }))],
        );

        let normalized = normalizer.normalize(plan).expect("normalize");
        let step = normalized.plan.get_step("s1").expect("s1");
        assert_eq!(
            step.params
                .pointer("/output_rules/fill_specification/candidates/0/path"),
            Some(&json!("fill_specification"))
        );
    }

    #[test]
    fn test_normalizer_repairs_malformed_leaf_output_rules_entries() {
        let normalizer = PlanNormalizer::new();
        let plan = Plan::new(
            "malformed-leaf-output-rules",
            vec![Step::leaf_agent("s1").with_params(json!({
                "mode": "leaf",
                "goal": "derive fill specification",
                "output_keys": ["fill_specification", "excel_path"],
                "output_rules": {
                    "fill_specification": "fill_specification",
                    "excel_path": {
                        "candidates": [
                            {
                                "path": "excel_path"
                            }
                        ]
                    }
                }
            }))],
        );

        let normalized = normalizer.normalize(plan).expect("normalize");
        let step = normalized.plan.get_step("s1").expect("s1");
        assert_eq!(
            step.params
                .pointer("/output_rules/fill_specification/candidates/0/slot"),
            Some(&json!("leaf_result"))
        );
        assert_eq!(
            step.params
                .pointer("/output_rules/excel_path/candidates/0/slot"),
            Some(&json!("leaf_result"))
        );
    }

    #[test]
    fn test_normalizer_compiles_recipe_steps_before_validation() {
        let mut normalizer = PlanNormalizer::new();
        normalizer.register_action("inspect_doc");
        normalizer.register_action("apply_patch");
        normalizer.register_action("finalize");

        let plan = Plan::new(
            "recipe-flow",
            vec![
                Step::recipe("r1").with_params(json!({
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
                                "goal": "derive a patch",
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
                        "updated_path": "apply.updated_path"
                    }
                })),
                Step::action("s2", "finalize")
                    .with_io_bindings(vec![StepIoBinding::required("r1.updated_path", "path")]),
            ],
        );

        let normalized = normalizer.normalize(plan).expect("normalize");
        assert!(normalized.plan.get_step("r1").is_none());
        let apply = normalized.plan.get_step("r1__apply").expect("apply");
        assert_eq!(apply.depends_on, vec![StepId::from("r1__derive")]);
        let finalize = normalized.plan.get_step("s2").expect("s2");
        assert_eq!(finalize.io_bindings[0].from, "r1__apply.updated_path");
    }

    #[test]
    fn test_normalizer_repairs_leaf_agent_with_file_read_actions_to_explore() {
        let normalizer = PlanNormalizer::new();
        let plan = Plan::new(
            "repair-leaf-agent",
            vec![Step::agent("s1").with_params(json!({
                "mode": "leaf",
                "goal": "derive a patch",
                "allowed_actions": ["file_read", "json_stdout"],
                "max_iterations": 3,
                "output_keys": ["change_spec"]
            }))],
        );

        let normalized = normalizer.normalize(plan).expect("normalize");
        let step = normalized.plan.get_step("s1").expect("s1");
        assert_eq!(step.params.get("mode"), Some(&json!("explore")));
        assert_eq!(step.params.get("max_iterations"), Some(&json!(3)));
        assert!(step.params.get("result_slot").is_none());
    }

    #[test]
    fn test_normalizer_rejects_leaf_agent_with_shell_actions() {
        let normalizer = PlanNormalizer::new();
        let plan = Plan::new(
            "invalid-leaf-agent",
            vec![Step::agent("s1").with_params(json!({
                "mode": "leaf",
                "goal": "derive a patch",
                "allowed_actions": ["shell"],
                "max_iterations": 1,
                "result_slot": "leaf_result",
                "output_keys": ["change_spec"]
            }))],
        );

        let err = normalizer
            .normalize(plan)
            .expect_err("expected validation error");
        match err {
            NormalizeError::Validation(ValidationError::InvalidAgentParams(step_id, reason)) => {
                assert_eq!(step_id, "s1");
                assert!(reason.contains("json_stdout"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_normalizer_rejects_explore_agent_with_side_effect_sensitive_outputs() {
        let normalizer = PlanNormalizer::new();
        let plan = Plan::new(
            "invalid-explore-agent",
            vec![Step::agent("s1").with_params(json!({
                "goal": "inspect and update workbook",
                "allowed_actions": ["shell", "file_write"],
                "max_iterations": 5,
                "output_keys": ["updated_file_path", "summary"],
                "output_rules": {
                    "updated_file_path": {
                        "candidates": [
                            {
                                "slot": "fill_result",
                                "path": "updated_file_path",
                                "requires": {
                                    "action": "file_write"
                                }
                            }
                        ]
                    },
                    "summary": {
                        "candidates": [
                            {
                                "slot": "fill_result",
                                "path": "summary"
                            }
                        ]
                    }
                }
            }))],
        );

        let err = normalizer
            .normalize(plan)
            .expect_err("expected validation error");
        match err {
            NormalizeError::Validation(ValidationError::InvalidAgentParams(step_id, reason)) => {
                assert_eq!(step_id, "s1");
                assert!(reason.contains("side-effect-sensitive outputs"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
