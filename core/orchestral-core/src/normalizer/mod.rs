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

mod agent;
mod implicit;
mod validation;

use thiserror::Error;

use crate::action::ActionMeta;
use crate::executor::ExecutionDag;
use crate::recipe::{RecipeCompileError, RecipeCompiler, RecipeTemplate};
use crate::types::Plan;

use self::implicit::{fix_control_flow_dependencies, output_keys_from_schema, ActionContract};

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
