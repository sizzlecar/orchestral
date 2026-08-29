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

mod implicit;
mod validation;

use thiserror::Error;

use crate::executor::ExecutionDag;
use crate::types::Plan;

use self::implicit::{fix_control_flow_dependencies, ActionContract};

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
    /// Human-readable summary of fixes applied during normalization (empty if none).
    pub fix_summary: Vec<String>,
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
        let name = name.into();
        self.known_actions.entry(name).or_default();
    }

    /// Returns the immutable contract required to replay normalization after
    /// a process restart.
    ///
    /// Custom trait-object validators and fixers have no stable identity in
    /// v1, so a normalizer containing either is intentionally not replayable.
    pub fn deterministic_contract(&self) -> Option<serde_json::Value> {
        if !self.validators.is_empty() || !self.fixers.is_empty() {
            return None;
        }
        let actions = self
            .known_actions
            .iter()
            .map(|(name, contract)| (name.clone(), contract.output_keys.clone()))
            .collect::<std::collections::BTreeMap<_, _>>();
        Some(serde_json::json!({
            "version": "plan-normalizer/v1",
            "known_actions": actions,
        }))
    }

    /// Normalize a plan
    pub fn normalize(&self, mut plan: Plan) -> Result<NormalizedPlan, NormalizeError> {
        let mut fix_summary: Vec<String> = Vec::new();

        // Step 1: Run fixers (they may fix issues before validation)
        for fixer in &self.fixers {
            fixer.fix(&mut plan)?;
        }

        // Step 2: Apply built-in normalization derived from action contracts.
        self.apply_implicit_contracts(&mut plan, &mut fix_summary);

        // Step 2.5: Control-flow steps (replan/wait) without explicit depends_on
        // must depend on all preceding steps to avoid premature execution.
        fix_control_flow_dependencies(&mut plan, &mut fix_summary);

        // Step 3: Run built-in validations
        self.validate_basic(&plan)?;

        // Step 4: Run custom validators
        for validator in &self.validators {
            validator.validate(&plan)?;
        }

        // Step 5: Build the execution DAG
        let dag = self.build_dag(&plan)?;

        Ok(NormalizedPlan {
            plan,
            dag,
            fix_summary,
        })
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
    use crate::types::{Step, StepIoBinding, StepKind};
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
    fn test_normalizer_derives_depends_on_from_param_templates() {
        let mut normalizer = PlanNormalizer::new();
        normalizer.register_action("produce");
        normalizer.register_action("consume");

        let plan = Plan::new(
            "derive deps from templates",
            vec![
                Step::action("s1", "produce"),
                Step::action("s2", "consume").with_params(json!({
                    "source_paths": "{{s1.source_paths}}",
                    "message": "process {{s1.summary}}"
                })),
            ],
        );

        let normalized = normalizer.normalize(plan).expect("normalize");
        let step2 = normalized.plan.get_step("s2").expect("s2");
        assert_eq!(step2.depends_on, vec![crate::types::StepId::from("s1")]);
    }

    #[test]
    fn test_normalizer_allows_external_working_set_template_refs_without_dependencies() {
        let mut normalizer = PlanNormalizer::new();
        normalizer.register_action("spreadsheet_apply_patch");

        let plan = Plan::new(
            "apply spreadsheet patch from prior iteration state",
            vec![
                Step::action("single_action", "spreadsheet_apply_patch").with_params(json!({
                    "path": "{{locate.source_path}}",
                    "patch_spec": "{{assess.continuation.patch_spec}}"
                })),
            ],
        );

        let normalized = normalizer.normalize(plan).expect("normalize");
        let step = normalized
            .plan
            .get_step("single_action")
            .expect("single_action");
        assert!(step.depends_on.is_empty());
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
