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

use crate::executor::ExecutionDag;
use crate::types::Plan;

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
    /// Set of known action names
    known_actions: std::collections::HashSet<String>,
}

impl PlanNormalizer {
    /// Create a new normalizer
    pub fn new() -> Self {
        Self {
            validators: Vec::new(),
            fixers: Vec::new(),
            known_actions: std::collections::HashSet::new(),
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
        self.known_actions.insert(name.into());
    }

    /// Normalize a plan
    pub fn normalize(&self, mut plan: Plan) -> Result<NormalizedPlan, NormalizeError> {
        // Step 1: Run fixers (they may fix issues before validation)
        for fixer in &self.fixers {
            fixer.fix(&mut plan)?;
        }

        // Step 2: Run built-in validations
        self.validate_basic(&plan)?;

        // Step 3: Run custom validators
        for validator in &self.validators {
            validator.validate(&plan)?;
        }

        // Step 4: Build the execution DAG
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
        }

        // Check for unknown actions (if we have a registry)
        if !self.known_actions.is_empty() {
            for step in &plan.steps {
                if !self.known_actions.contains(&step.action) {
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

impl Default for PlanNormalizer {
    fn default() -> Self {
        Self::new()
    }
}
