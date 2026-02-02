//! Executor module
//!
//! The Executor is responsible for:
//! - DAG-based topological scheduling
//! - Parallel execution of ready nodes
//! - State-driven execution

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::action::{Action, ActionContext, ActionInput, ActionResult};
use crate::store::{ReferenceStore, WorkingSet};
use crate::types::{Plan, Step, StepKind};

/// Node state in the execution DAG
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeState {
    /// Not yet ready to execute
    Pending,
    /// Ready to execute (all dependencies completed)
    Ready,
    /// Currently executing
    Running,
    /// Execution completed successfully
    Completed,
    /// Execution failed
    Failed,
    /// Skipped (e.g., due to conditional logic)
    Skipped,
}

/// A node in the execution DAG
#[derive(Debug, Clone)]
pub struct DagNode {
    /// The step definition
    pub step: Step,
    /// Current state
    pub state: NodeState,
    /// Steps this node depends on
    pub depends_on: Vec<String>,
    /// Steps that depend on this node (reverse dependencies)
    pub dependents: Vec<String>,
    /// Execution ID for this run (distinguishes retry/resume)
    pub execution_id: String,
}

impl DagNode {
    /// Create a new DAG node from a step
    pub fn new(step: Step) -> Self {
        let depends_on = step.depends_on.clone();
        Self {
            step,
            state: NodeState::Pending,
            depends_on,
            dependents: Vec::new(),
            execution_id: uuid::Uuid::new_v4().to_string(),
        }
    }

    /// Check if all dependencies are completed
    pub fn dependencies_satisfied(&self, nodes: &HashMap<String, DagNode>) -> bool {
        self.depends_on.iter().all(|dep_id| {
            nodes
                .get(dep_id)
                .map(|n| n.state == NodeState::Completed)
                .unwrap_or(false)
        })
    }

    /// Generate a new execution ID (for retry)
    pub fn new_execution_id(&mut self) {
        self.execution_id = uuid::Uuid::new_v4().to_string();
    }
}

/// Execution DAG - supports dynamic modification
#[derive(Debug, Clone)]
pub struct ExecutionDag {
    /// All nodes in the DAG
    pub nodes: HashMap<String, DagNode>,
    /// Currently ready nodes (in-degree = 0 among non-completed)
    pub ready_nodes: Vec<String>,
    /// Whether dynamic modification is allowed
    pub dynamic: bool,
}

impl ExecutionDag {
    /// Create a new empty DAG
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            ready_nodes: Vec::new(),
            dynamic: false,
        }
    }

    /// Create a dynamic DAG (allows modification during execution)
    pub fn new_dynamic() -> Self {
        Self {
            nodes: HashMap::new(),
            ready_nodes: Vec::new(),
            dynamic: true,
        }
    }

    /// Build a DAG from a plan
    pub fn from_plan(plan: &Plan) -> Result<Self, String> {
        let mut dag = Self::new();

        // Create all nodes
        for step in &plan.steps {
            let node = DagNode::new(step.clone());
            dag.nodes.insert(step.id.clone(), node);
        }

        // Build reverse dependencies
        for step in &plan.steps {
            for dep_id in &step.depends_on {
                if let Some(dep_node) = dag.nodes.get_mut(dep_id) {
                    dep_node.dependents.push(step.id.clone());
                } else {
                    return Err(format!("Dependency '{}' not found", dep_id));
                }
            }
        }

        // Find initial ready nodes
        dag.update_ready_nodes();

        Ok(dag)
    }

    /// Update the list of ready nodes
    pub fn update_ready_nodes(&mut self) {
        self.ready_nodes = self
            .nodes
            .iter()
            .filter(|(_, node)| {
                node.state == NodeState::Pending && node.dependencies_satisfied(&self.nodes)
            })
            .map(|(id, _)| id.clone())
            .collect();

        // Mark ready nodes
        for id in &self.ready_nodes {
            if let Some(node) = self.nodes.get_mut(id) {
                node.state = NodeState::Ready;
            }
        }
    }

    /// Get a node by ID
    pub fn get_node(&self, id: &str) -> Option<&DagNode> {
        self.nodes.get(id)
    }

    /// Get a mutable node by ID
    pub fn get_node_mut(&mut self, id: &str) -> Option<&mut DagNode> {
        self.nodes.get_mut(id)
    }

    /// Mark a node as running
    pub fn mark_running(&mut self, id: &str) {
        if let Some(node) = self.nodes.get_mut(id) {
            node.state = NodeState::Running;
            self.ready_nodes.retain(|n| n != id);
        }
    }

    /// Mark a node as completed and update ready nodes
    pub fn mark_completed(&mut self, id: &str) {
        if let Some(node) = self.nodes.get_mut(id) {
            node.state = NodeState::Completed;
        }
        self.update_ready_nodes();
    }

    /// Mark a node as failed
    pub fn mark_failed(&mut self, id: &str) {
        if let Some(node) = self.nodes.get_mut(id) {
            node.state = NodeState::Failed;
        }
    }

    /// Check if all nodes are completed
    pub fn is_completed(&self) -> bool {
        self.nodes.values().all(|n| n.state == NodeState::Completed)
    }

    /// Check if any node has failed
    pub fn has_failed(&self) -> bool {
        self.nodes.values().any(|n| n.state == NodeState::Failed)
    }

    /// Get all completed node IDs
    pub fn completed_nodes(&self) -> Vec<&str> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.state == NodeState::Completed)
            .map(|(id, _)| id.as_str())
            .collect()
    }

    /// Get all failed node IDs
    pub fn failed_nodes(&self) -> Vec<&str> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.state == NodeState::Failed)
            .map(|(id, _)| id.as_str())
            .collect()
    }

    /// Add a new node dynamically (only if dynamic is enabled)
    pub fn add_node(&mut self, step: Step) -> Result<(), String> {
        if !self.dynamic {
            return Err("DAG is not dynamic".to_string());
        }

        // Validate dependencies exist
        for dep_id in &step.depends_on {
            if !self.nodes.contains_key(dep_id) {
                return Err(format!("Dependency '{}' not found", dep_id));
            }
        }

        let id = step.id.clone();
        let node = DagNode::new(step);

        // Update reverse dependencies
        for dep_id in &node.depends_on {
            if let Some(dep_node) = self.nodes.get_mut(dep_id) {
                dep_node.dependents.push(id.clone());
            }
        }

        self.nodes.insert(id, node);
        self.update_ready_nodes();

        Ok(())
    }
}

impl Default for ExecutionDag {
    fn default() -> Self {
        Self::new()
    }
}

/// Action registry for looking up actions by name
pub struct ActionRegistry {
    actions: HashMap<String, Arc<dyn Action>>,
}

impl ActionRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            actions: HashMap::new(),
        }
    }

    /// Register an action
    pub fn register(&mut self, action: Arc<dyn Action>) {
        self.actions.insert(action.name().to_string(), action);
    }

    /// Get an action by name
    pub fn get(&self, name: &str) -> Option<Arc<dyn Action>> {
        self.actions.get(name).cloned()
    }

    /// Get all action names
    pub fn names(&self) -> Vec<String> {
        self.actions.keys().cloned().collect()
    }
}

impl Default for ActionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Executor context
pub struct ExecutorContext {
    /// Working set for inter-step communication
    pub working_set: Arc<RwLock<WorkingSet>>,
    /// Reference store for historical artifacts
    pub reference_store: Arc<dyn ReferenceStore>,
    /// Task ID
    pub task_id: String,
}

impl ExecutorContext {
    /// Create a new executor context
    pub fn new(
        task_id: impl Into<String>,
        working_set: Arc<RwLock<WorkingSet>>,
        reference_store: Arc<dyn ReferenceStore>,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            working_set,
            reference_store,
        }
    }
}

/// Execution result
#[derive(Debug)]
pub enum ExecutionResult {
    /// All steps completed successfully
    Completed,
    /// Execution failed
    Failed { step_id: String, error: String },
    /// Waiting for user input
    WaitingUser { step_id: String, prompt: String },
    /// Waiting for external event
    WaitingEvent { step_id: String, event_type: String },
}

/// The executor - orchestrates DAG execution
pub struct Executor {
    /// Action registry
    pub action_registry: Arc<RwLock<ActionRegistry>>,
    /// Maximum parallel executions
    pub max_parallel: usize,
    /// Whether declared imports are required at runtime.
    pub strict_imports: bool,
    /// Whether declared exports are required at runtime.
    pub strict_exports: bool,
}

impl Executor {
    /// Create a new executor
    pub fn new(action_registry: ActionRegistry) -> Self {
        Self::with_registry(Arc::new(RwLock::new(action_registry)))
    }

    /// Create a new executor with a shared registry
    pub fn with_registry(action_registry: Arc<RwLock<ActionRegistry>>) -> Self {
        Self {
            action_registry,
            max_parallel: 4,
            strict_imports: true,
            strict_exports: true,
        }
    }

    /// Set maximum parallel executions
    pub fn with_max_parallel(mut self, max: usize) -> Self {
        self.max_parallel = max;
        self
    }

    /// Configure strict runtime checks for step imports/exports.
    pub fn with_io_contract(mut self, strict_imports: bool, strict_exports: bool) -> Self {
        self.strict_imports = strict_imports;
        self.strict_exports = strict_exports;
        self
    }

    /// Execute a DAG
    pub async fn execute(&self, dag: &mut ExecutionDag, ctx: &ExecutorContext) -> ExecutionResult {
        loop {
            // Get ready nodes
            let ready: Vec<_> = dag.ready_nodes.clone();

            if ready.is_empty() {
                // No more ready nodes
                if dag.is_completed() {
                    return ExecutionResult::Completed;
                } else if dag.has_failed() {
                    let failed = dag.failed_nodes();
                    return ExecutionResult::Failed {
                        step_id: failed.first().map(|s| s.to_string()).unwrap_or_default(),
                        error: "Execution failed".to_string(),
                    };
                } else {
                    // Deadlock or waiting
                    return ExecutionResult::Failed {
                        step_id: String::new(),
                        error: "No ready nodes but DAG not completed".to_string(),
                    };
                }
            }

            // Execute ready nodes (up to max_parallel)
            let batch: Vec<_> = ready.into_iter().take(self.max_parallel).collect();

            for step_id in batch {
                // Extract node data before mutating dag
                let node_data = dag.get_node(&step_id).map(|node| {
                    (
                        node.step.kind.clone(),
                        node.step.params.clone(),
                        node.step.clone(),
                        node.execution_id.clone(),
                    )
                });

                if let Some((kind, params, step, execution_id)) = node_data {
                    // Check for special step kinds
                    match kind {
                        StepKind::WaitUser => {
                            dag.mark_running(&step_id);
                            return ExecutionResult::WaitingUser {
                                step_id,
                                prompt: params
                                    .get("prompt")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("Please provide input")
                                    .to_string(),
                            };
                        }
                        StepKind::WaitEvent => {
                            dag.mark_running(&step_id);
                            return ExecutionResult::WaitingEvent {
                                step_id,
                                event_type: params
                                    .get("event_type")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown")
                                    .to_string(),
                            };
                        }
                        _ => {}
                    }

                    // Execute the action
                    dag.mark_running(&step_id);

                    let result = self.execute_step_data(&step, &execution_id, ctx).await;

                    match result {
                        ActionResult::Success { exports } => {
                            if let Err(error) =
                                validate_declared_exports(&step, &exports, self.strict_exports)
                            {
                                dag.mark_failed(&step_id);
                                return ExecutionResult::Failed { step_id, error };
                            }

                            // Write exports to working set
                            let mut ws = ctx.working_set.write().await;
                            for (key, value) in exports {
                                ws.set_task(key.clone(), value.clone());
                                ws.set_task(format!("{}.{}", step.id, key), value);
                            }
                            dag.mark_completed(&step_id);
                        }
                        ActionResult::NeedClarification { question } => {
                            return ExecutionResult::WaitingUser {
                                step_id,
                                prompt: question,
                            };
                        }
                        ActionResult::RetryableError { message, .. } => {
                            // For now, mark as failed (retry logic can be added later)
                            dag.mark_failed(&step_id);
                            return ExecutionResult::Failed {
                                step_id,
                                error: message,
                            };
                        }
                        ActionResult::Error { message } => {
                            dag.mark_failed(&step_id);
                            return ExecutionResult::Failed {
                                step_id,
                                error: message,
                            };
                        }
                    }
                }
            }
        }
    }

    /// Execute a single step using extracted data (avoids borrow conflicts)
    async fn execute_step_data(
        &self,
        step: &Step,
        execution_id: &str,
        ctx: &ExecutorContext,
    ) -> ActionResult {
        let action = {
            let registry = self.action_registry.read().await;
            registry.get(&step.action)
        };

        let action = match action {
            Some(a) => a,
            None => return ActionResult::error(format!("Action '{}' not found", step.action)),
        };

        // Build input from imports
        let mut imports = HashMap::new();
        let mut resolved_params = step.params.clone();
        {
            let ws = ctx.working_set.read().await;
            for key in &step.imports {
                if let Some(value) = ws.get_task(key) {
                    imports.insert(key.clone(), value.clone());
                } else if self.strict_imports {
                    return ActionResult::error(format!(
                        "Missing declared import '{}' for step '{}'",
                        key, step.id
                    ));
                }
            }
            for binding in &step.io_bindings {
                if let Some(value) = ws.get_task(&binding.from) {
                    imports.insert(binding.to.clone(), value.clone());
                    if let Err(error) = bind_param_value(&mut resolved_params, &binding.to, value) {
                        return ActionResult::error(format!(
                            "Invalid io binding for step '{}': {}",
                            step.id, error
                        ));
                    }
                } else if binding.required {
                    return ActionResult::error(format!(
                        "Missing required io binding '{}' from '{}' for step '{}'",
                        binding.to, binding.from, step.id
                    ));
                }
            }
        }

        let input = ActionInput {
            params: resolved_params,
            imports,
        };

        let action_ctx = ActionContext::new(
            ctx.task_id.clone(),
            step.id.clone(),
            execution_id.to_string(),
            ctx.working_set.clone(),
            ctx.reference_store.clone(),
        );

        action.run(input, action_ctx).await
    }
}

fn bind_param_value(
    params: &mut serde_json::Value,
    key: &str,
    value: &serde_json::Value,
) -> Result<(), String> {
    match params {
        serde_json::Value::Object(map) => {
            map.insert(key.to_string(), value.clone());
            Ok(())
        }
        serde_json::Value::Null => {
            let mut map = serde_json::Map::new();
            map.insert(key.to_string(), value.clone());
            *params = serde_json::Value::Object(map);
            Ok(())
        }
        _ => Err("step.params must be an object (or null) when using io_bindings".to_string()),
    }
}

fn validate_declared_exports(
    step: &Step,
    exports: &HashMap<String, serde_json::Value>,
    strict_exports: bool,
) -> Result<(), String> {
    if !strict_exports || step.exports.is_empty() {
        return Ok(());
    }

    for key in &step.exports {
        match exports.get(key) {
            Some(value) if !value.is_null() => {}
            Some(_) => return Err(format!("Step '{}' export '{}' is null", step.id, key)),
            None => {
                return Err(format!(
                    "Step '{}' missing declared export '{}'",
                    step.id, key
                ))
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::{json, Value};

    use crate::store::{Reference, ReferenceStore, ReferenceType, StoreError};
    use crate::types::{Plan, StepIoBinding};

    struct NoopReferenceStore;

    #[async_trait]
    impl ReferenceStore for NoopReferenceStore {
        async fn add(&self, _reference: Reference) -> Result<(), StoreError> {
            Ok(())
        }

        async fn get(&self, _id: &str) -> Result<Option<Reference>, StoreError> {
            Ok(None)
        }

        async fn query_by_type(
            &self,
            _ref_type: &ReferenceType,
        ) -> Result<Vec<Reference>, StoreError> {
            Ok(Vec::new())
        }

        async fn query_recent(&self, _limit: usize) -> Result<Vec<Reference>, StoreError> {
            Ok(Vec::new())
        }

        async fn delete(&self, _id: &str) -> Result<bool, StoreError> {
            Ok(false)
        }
    }

    struct StaticAction {
        name: String,
        result: ActionResult,
    }

    impl StaticAction {
        fn new(name: &str, result: ActionResult) -> Self {
            Self {
                name: name.to_string(),
                result,
            }
        }
    }

    #[async_trait]
    impl Action for StaticAction {
        fn name(&self) -> &str {
            &self.name
        }

        fn description(&self) -> &str {
            "test action"
        }

        async fn run(&self, _input: ActionInput, _ctx: ActionContext) -> ActionResult {
            self.result.clone()
        }
    }

    struct ConsumeContentAction;

    #[async_trait]
    impl Action for ConsumeContentAction {
        fn name(&self) -> &str {
            "consume_content"
        }

        fn description(&self) -> &str {
            "consume imported content"
        }

        async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
            let from_params = input.params.get("content").and_then(|v| v.as_str());
            let from_imports = input.imports.get("content").and_then(|v| v.as_str());
            match (from_params, from_imports) {
                (Some(a), Some(b)) if a == b => {
                    ActionResult::success_with_one("written", Value::String(a.to_string()))
                }
                _ => ActionResult::error("content binding not applied"),
            }
        }
    }

    #[test]
    fn test_io_binding_injects_input_for_downstream_step() {
        tokio_test::block_on(async {
            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(StaticAction::new(
                "produce",
                ActionResult::success_with_one("guide_markdown", json!("hello world")),
            )));
            registry.register(Arc::new(ConsumeContentAction));
            let executor = Executor::new(registry);

            let plan = Plan::new(
                "binding flow",
                vec![
                    Step::action("s1", "produce").with_exports(vec!["guide_markdown".to_string()]),
                    Step::action("s2", "consume_content")
                        .with_depends_on(vec!["s1".to_string()])
                        .with_io_bindings(vec![StepIoBinding::required(
                            "s1.guide_markdown",
                            "content",
                        )])
                        .with_exports(vec!["written".to_string()]),
                ],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let working_set = Arc::new(RwLock::new(WorkingSet::new()));
            let ctx =
                ExecutorContext::new("task-1", working_set.clone(), Arc::new(NoopReferenceStore));

            let result = executor.execute(&mut dag, &ctx).await;
            assert!(matches!(result, ExecutionResult::Completed));

            let ws = working_set.read().await;
            assert_eq!(ws.get_task("s2.written"), Some(&json!("hello world")));
        });
    }

    #[test]
    fn test_missing_required_binding_fails_step() {
        tokio_test::block_on(async {
            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(ConsumeContentAction));
            let executor = Executor::new(registry);

            let plan = Plan::new(
                "missing binding",
                vec![Step::action("s1", "consume_content")
                    .with_io_bindings(vec![StepIoBinding::required(
                        "missing.step_output",
                        "content",
                    )])
                    .with_exports(vec!["written".to_string()])],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ctx = ExecutorContext::new(
                "task-1",
                Arc::new(RwLock::new(WorkingSet::new())),
                Arc::new(NoopReferenceStore),
            );

            let result = executor.execute(&mut dag, &ctx).await;
            match result {
                ExecutionResult::Failed { error, .. } => {
                    assert!(error.contains("Missing required io binding"));
                }
                _ => panic!("expected failed result"),
            }
        });
    }

    #[test]
    fn test_missing_declared_export_fails_step_when_strict() {
        tokio_test::block_on(async {
            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(StaticAction::new(
                "produce",
                ActionResult::success(),
            )));
            let executor = Executor::new(registry).with_io_contract(true, true);

            let plan = Plan::new(
                "strict exports",
                vec![Step::action("s1", "produce").with_exports(vec!["result".to_string()])],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ctx = ExecutorContext::new(
                "task-1",
                Arc::new(RwLock::new(WorkingSet::new())),
                Arc::new(NoopReferenceStore),
            );

            let result = executor.execute(&mut dag, &ctx).await;
            match result {
                ExecutionResult::Failed { error, .. } => {
                    assert!(error.contains("missing declared export"));
                }
                _ => panic!("expected failed result"),
            }
        });
    }
}
