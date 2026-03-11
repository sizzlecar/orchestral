use crate::executor::ExecutionDag;
use crate::types::{Plan, StepKind};

use super::{
    agent::validate_agent_params, implicit::parse_step_binding_source, NormalizeError,
    PlanNormalizer, ValidationError,
};

impl PlanNormalizer {
    pub(super) fn validate_basic(&self, plan: &Plan) -> Result<(), ValidationError> {
        if plan.steps.is_empty() {
            return Err(ValidationError::EmptyPlan);
        }

        let mut seen_ids = std::collections::HashSet::new();
        for step in &plan.steps {
            if !seen_ids.insert(&step.id) {
                return Err(ValidationError::DuplicateStepId(step.id.to_string()));
            }
        }

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

        self.detect_cycles(plan)?;
        Ok(())
    }

    fn detect_cycles(&self, plan: &Plan) -> Result<(), ValidationError> {
        use std::collections::{HashMap, HashSet};

        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        for step in &plan.steps {
            adj.entry(step.id.as_str()).or_default();
            for dep in &step.depends_on {
                adj.entry(dep.as_str()).or_default().push(step.id.as_str());
            }
        }

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

        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
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

    pub(super) fn build_dag(&self, plan: &Plan) -> Result<ExecutionDag, NormalizeError> {
        ExecutionDag::from_plan(plan).map_err(NormalizeError::DagBuild)
    }
}
