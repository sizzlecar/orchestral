use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::RwLock;

use crate::action::{Action, ActionResult, ApprovalRequest};
use crate::planner::{PlannerRuntimeInfo, SkillInstruction};
use crate::store::WorkingSet;
use crate::types::{Step, StepId, TaskId};

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
    /// Task ID
    pub task_id: TaskId,
    /// Optional execution progress reporter.
    pub progress_reporter: Option<Arc<dyn ExecutionProgressReporter>>,
    /// Runtime host information (OS, arch, shell, python) for platform-aware execution.
    pub runtime_info: Option<PlannerRuntimeInfo>,
    /// Activated skill instructions for this execution turn.
    pub skill_instructions: Vec<SkillInstruction>,
}

impl ExecutorContext {
    /// Create a new executor context
    pub fn new(task_id: impl Into<TaskId>, working_set: Arc<RwLock<WorkingSet>>) -> Self {
        Self {
            task_id: task_id.into(),
            working_set,
            progress_reporter: None,
            runtime_info: None,
            skill_instructions: Vec::new(),
        }
    }

    /// Attach a realtime execution progress reporter.
    pub fn with_progress_reporter(mut self, reporter: Arc<dyn ExecutionProgressReporter>) -> Self {
        self.progress_reporter = Some(reporter);
        self
    }

    /// Attach runtime host information for platform-aware execution.
    pub fn with_runtime_info(mut self, info: PlannerRuntimeInfo) -> Self {
        self.runtime_info = Some(info);
        self
    }

    /// Attach activated skill instructions for this execution turn.
    pub fn with_skill_instructions(mut self, skills: Vec<SkillInstruction>) -> Self {
        self.skill_instructions = skills;
        self
    }
}

/// Execution result
#[derive(Debug, Clone)]
pub enum ExecutionResult {
    /// All steps completed successfully
    Completed,
    /// Execution failed
    Failed { step_id: StepId, error: String },
    /// Waiting for user input
    WaitingUser {
        step_id: StepId,
        prompt: String,
        approval: Option<ApprovalRequest>,
    },
    /// Waiting for external event
    WaitingEvent { step_id: StepId, event_type: String },
}

/// Realtime execution progress event.
#[derive(Debug, Clone)]
pub struct ExecutionProgressEvent {
    pub task_id: TaskId,
    pub step_id: Option<StepId>,
    pub action: Option<String>,
    /// Phase label, e.g. step_started/step_completed/task_completed.
    pub phase: String,
    /// Optional human-readable message.
    pub message: Option<String>,
    /// Extra structured metadata.
    pub metadata: serde_json::Value,
}

impl ExecutionProgressEvent {
    pub fn new(
        task_id: impl Into<TaskId>,
        step_id: Option<StepId>,
        action: Option<String>,
        phase: impl Into<String>,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            step_id,
            action,
            phase: phase.into(),
            message: None,
            metadata: serde_json::Value::Null,
        }
    }

    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }
}

/// Sink interface for execution progress reporting.
#[async_trait]
pub trait ExecutionProgressReporter: Send + Sync {
    async fn report(&self, event: ExecutionProgressEvent) -> Result<(), String>;
}

/// Runtime-provided executor for `StepKind::Agent`.
#[async_trait]
pub trait AgentStepExecutor: Send + Sync {
    async fn execute_agent_step(
        &self,
        step: &Step,
        resolved_params: serde_json::Value,
        execution_id: &str,
        ctx: &ExecutorContext,
        action_registry: Arc<RwLock<ActionRegistry>>,
    ) -> ActionResult;
}

pub type ActionPreflightHook = Arc<dyn Fn(&str, &Value) -> Option<String> + Send + Sync>;

#[derive(Clone, Default)]
pub struct ActionExecutionOptions {
    pub preflight_hook: Option<ActionPreflightHook>,
}

impl ActionExecutionOptions {
    pub fn with_preflight_hook(mut self, hook: ActionPreflightHook) -> Self {
        self.preflight_hook = Some(hook);
        self
    }
}
