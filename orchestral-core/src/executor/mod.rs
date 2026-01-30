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
        }
    }

    /// Set maximum parallel executions
    pub fn with_max_parallel(mut self, max: usize) -> Self {
        self.max_parallel = max;
        self
    }

    /// Execute a DAG
    pub async fn execute(
        &self,
        dag: &mut ExecutionDag,
        ctx: &ExecutorContext,
    ) -> ExecutionResult {
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
                            // Write exports to working set
                            let mut ws = ctx.working_set.write().await;
                            for (key, value) in exports {
                                ws.set_task(key, value);
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
        {
            let ws = ctx.working_set.read().await;
            for key in &step.imports {
                if let Some(value) = ws.get_task(key) {
                    imports.insert(key.clone(), value.clone());
                }
            }
        }

        let input = ActionInput {
            params: step.params.clone(),
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
