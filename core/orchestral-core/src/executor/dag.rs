use std::collections::HashMap;

use crate::types::{Plan, Step, StepId};

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
    pub depends_on: Vec<StepId>,
    /// Steps that depend on this node (reverse dependencies)
    pub dependents: Vec<StepId>,
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
                .get(dep_id.as_str())
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

        for step in &plan.steps {
            let node = DagNode::new(step.clone());
            dag.nodes.insert(step.id.to_string(), node);
        }

        for step in &plan.steps {
            for dep_id in &step.depends_on {
                if let Some(dep_node) = dag.nodes.get_mut(dep_id.as_str()) {
                    dep_node.dependents.push(step.id.clone());
                } else {
                    return Err(format!("Dependency '{}' not found", dep_id));
                }
            }
        }

        dag.update_ready_nodes();
        Ok(dag)
    }

    /// Update the list of ready nodes
    pub fn update_ready_nodes(&mut self) {
        self.ready_nodes = self
            .nodes
            .iter()
            .filter(|(_, node)| {
                matches!(node.state, NodeState::Pending | NodeState::Ready)
                    && node.dependencies_satisfied(&self.nodes)
            })
            .map(|(id, _)| id.clone())
            .collect();

        for id in &self.ready_nodes {
            if let Some(node) = self.nodes.get_mut(id) {
                if node.state == NodeState::Pending {
                    node.state = NodeState::Ready;
                }
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

        for dep_id in &step.depends_on {
            if !self.nodes.contains_key(dep_id.as_str()) {
                return Err(format!("Dependency '{}' not found", dep_id));
            }
        }

        let id = step.id.clone();
        let node = DagNode::new(step);

        for dep_id in &node.depends_on {
            if let Some(dep_node) = self.nodes.get_mut(dep_id.as_str()) {
                dep_node.dependents.push(id.clone());
            }
        }

        self.nodes.insert(id.to_string(), node);
        self.update_ready_nodes();

        Ok(())
    }
}

impl Default for ExecutionDag {
    fn default() -> Self {
        Self::new()
    }
}
