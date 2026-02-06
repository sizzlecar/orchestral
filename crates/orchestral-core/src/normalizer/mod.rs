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

use thiserror::Error;

use crate::action::ActionMeta;
use crate::executor::ExecutionDag;
use crate::types::{Plan, StepKind};

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
    /// Known action contracts keyed by action name.
    known_actions: std::collections::HashMap<String, ActionContract>,
}

impl PlanNormalizer {
    /// Create a new normalizer
    pub fn new() -> Self {
        Self {
            validators: Vec::new(),
            fixers: Vec::new(),
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
        self.known_actions.entry(name.into()).or_default();
    }

    /// Register a known action with metadata-derived contract.
    pub fn register_action_meta(&mut self, meta: &ActionMeta) {
        let output_keys = output_keys_from_schema(&meta.output_schema);
        self.known_actions
            .insert(meta.name.clone(), ActionContract { output_keys });
    }

    /// Normalize a plan
    pub fn normalize(&self, mut plan: Plan) -> Result<NormalizedPlan, NormalizeError> {
        // Step 1: Run fixers (they may fix issues before validation)
        for fixer in &self.fixers {
            fixer.fix(&mut plan)?;
        }

        // Step 2: Apply built-in normalization derived from action contracts.
        self.apply_implicit_contracts(&mut plan);

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
                return Err(ValidationError::DuplicateStepId(step.id.clone()));
            }
        }

        // Check that all dependencies exist
        let step_ids: std::collections::HashSet<_> =
            plan.steps.iter().map(|s| s.id.as_str()).collect();
        for step in &plan.steps {
            for dep in &step.depends_on {
                if !step_ids.contains(dep.as_str()) {
                    return Err(ValidationError::MissingDependency(
                        step.id.clone(),
                        dep.clone(),
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
                        step.id.clone(),
                        format!("unknown source step '{}'", source_step),
                    ));
                }
                if source_step != step.id && !step.depends_on.iter().any(|dep| dep == &source_step)
                {
                    return Err(ValidationError::IoBindingMissingDependency(
                        step.id.clone(),
                        source_step,
                    ));
                }

                if let Some(source) = plan.get_step(&source_step) {
                    if !source.exports.is_empty() && !source.exports.iter().any(|k| k == source_key)
                    {
                        return Err(ValidationError::InvalidIoBinding(
                            step.id.clone(),
                            format!(
                                "source key '{}' not declared in step '{}' exports",
                                source_key, source.id
                            ),
                        ));
                    }
                }
            }
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
            infer_special_step_kind(step);
            derive_depends_on_from_bindings(step);

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
        _ => {}
    }
}

fn derive_depends_on_from_bindings(step: &mut crate::types::Step) {
    for binding in &step.io_bindings {
        if let Some((source_step, _)) = parse_step_binding_source(&binding.from) {
            if source_step != step.id && !step.depends_on.iter().any(|dep| dep == &source_step) {
                step.depends_on.push(source_step);
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
        assert_eq!(step2.depends_on, vec!["s1".to_string()]);
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
}
