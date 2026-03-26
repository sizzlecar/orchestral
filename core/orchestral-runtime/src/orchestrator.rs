//! Orchestrator - minimal intent → plan → normalize → execute pipeline
//!
//! This bridges the ThreadRuntime (events + concurrency) with core planning/execution.

mod agent_loop;
mod entry;
mod execution;
mod output;
mod planning;
mod progress;
#[cfg(test)]
mod recovery;
mod state;
mod support;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use serde_json::Value;
use tokio::sync::RwLock;

use crate::context::{ContextBuilder, ContextError, ContextRequest, ContextWindow, TokenBudget};
use crate::skill::SkillCatalog;
use orchestral_core::action::{extract_meta, ActionMeta};
use orchestral_core::executor::{ExecutionResult, Executor, ExecutorContext};
use orchestral_core::interpreter::InterpretRequest;
use orchestral_core::normalizer::{NormalizeError, PlanNormalizer};
use orchestral_core::planner::{
    HistoryItem, PlanError, Planner, PlannerContext, PlannerLoopContext, PlannerOutput,
    PlannerRuntimeInfo,
};
use orchestral_core::spi::lifecycle::LifecycleHookRegistry;
use orchestral_core::spi::HookRegistry;
use orchestral_core::store::{Event, InteractionId, StoreError, TaskStore, WorkingSet};
use orchestral_core::types::{
    Intent, IntentContext, StepId, StepKind, Task, TaskId, TaskState, WaitUserReason,
};
use output::{execution_result_metadata, summarize_plan_steps};
use progress::RuntimeProgressReporter;
use state::{
    apply_resume_event_to_working_set, complete_wait_step_for_resume, interaction_state_from_task,
    restore_checkpoint, task_state_from_execution,
};
use support::{
    context_window_to_history, drop_current_turn_user_input, event_to_history_item,
    event_type_label, intent_from_event, summarize_working_set,
};

use crate::{HandleEventResult, InteractionState, RuntimeError, ThreadRuntime};

const MAX_LOG_CHARS: usize = 8_000;

fn truncate_for_log(input: &str, max_chars: usize) -> String {
    let char_count = input.chars().count();
    if char_count <= max_chars {
        return input.to_string();
    }
    let mut preview: String = input.chars().take(max_chars).collect();
    preview.push_str(&format!("... [truncated, total_chars={}]", char_count));
    preview
}

fn truncate_debug_for_log(value: &impl std::fmt::Debug, max_chars: usize) -> String {
    truncate_for_log(&format!("{:?}", value), max_chars)
}

/// Orchestrator result for a handled event
#[derive(Debug)]
pub enum OrchestratorResult {
    /// A new interaction was started and executed
    Started {
        interaction_id: InteractionId,
        task_id: TaskId,
        result: ExecutionResult,
    },
    /// The event was merged into an existing interaction and executed
    Merged {
        interaction_id: InteractionId,
        task_id: TaskId,
        result: ExecutionResult,
    },
    /// The event was rejected
    Rejected { reason: String },
    /// The event was queued
    Queued,
}

/// Orchestrator errors
#[derive(Debug, thiserror::Error)]
pub enum OrchestratorError {
    #[error("runtime error: {0}")]
    Runtime(#[from] RuntimeError),
    #[error("planner error: {0}")]
    Planner(#[from] PlanError),
    #[error("normalize error: {0}")]
    Normalize(#[from] NormalizeError),
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    #[error("context error: {0}")]
    Context(#[from] ContextError),
    #[error("task not found: {0}")]
    TaskNotFound(String),
    #[error("task has no plan: {0}")]
    MissingPlan(String),
    #[error("resume error: {0}")]
    ResumeError(String),
    #[error("unsupported event: {0}")]
    UnsupportedEvent(String),
}

/// Orchestrator - wires runtime + planner + executor for a minimal pipeline
pub struct Orchestrator {
    pub thread_runtime: ThreadRuntime,
    pub planner: Arc<dyn Planner>,
    pub normalizer: PlanNormalizer,
    pub executor: Executor,
    pub task_store: Arc<dyn TaskStore>,
    pub context_builder: Option<Arc<dyn ContextBuilder>>,
    pub config: OrchestratorConfig,
    pub hook_registry: Arc<HookRegistry>,
    pub lifecycle_hooks: Arc<LifecycleHookRegistry>,
    pub skill_catalog: Arc<RwLock<SkillCatalog>>,
    /// Config path used for skill re-discovery (hot reload).
    pub skill_config_path: Option<std::path::PathBuf>,
}

/// Orchestrator configuration
#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    /// Max events to include in planner history (0 = all)
    pub history_limit: usize,
    /// Token budget for context assembly
    pub context_budget: TokenBudget,
    /// Whether to include history when building context
    pub include_history: bool,
    /// Retry once by replanning only the failed subgraph.
    pub auto_replan_once: bool,
    /// Retry once by asking the planner to repair a plan rejected by normalization.
    pub auto_repair_plan_once: bool,
    /// Max planner iterations inside one outer agent loop turn.
    pub max_planner_iterations: usize,
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            history_limit: 50,
            context_budget: TokenBudget::default(),
            include_history: true,
            auto_replan_once: true,
            auto_repair_plan_once: true,
            max_planner_iterations: 6,
        }
    }
}

impl Orchestrator {
    /// Create a new orchestrator
    pub fn new(
        thread_runtime: ThreadRuntime,
        planner: Arc<dyn Planner>,
        normalizer: PlanNormalizer,
        executor: Executor,
        task_store: Arc<dyn TaskStore>,
    ) -> Self {
        Self::with_config(
            thread_runtime,
            planner,
            normalizer,
            executor,
            task_store,
            OrchestratorConfig::default(),
        )
    }

    /// Create a new orchestrator with config
    pub fn with_config(
        thread_runtime: ThreadRuntime,
        planner: Arc<dyn Planner>,
        normalizer: PlanNormalizer,
        executor: Executor,
        task_store: Arc<dyn TaskStore>,
        config: OrchestratorConfig,
    ) -> Self {
        Self {
            thread_runtime,
            planner,
            normalizer,
            executor,
            task_store,
            context_builder: None,
            config,
            hook_registry: Arc::new(HookRegistry::new()),
            lifecycle_hooks: Arc::new(LifecycleHookRegistry::new()),
            skill_catalog: Arc::new(RwLock::new(SkillCatalog::new(Vec::new(), 0))),
            skill_config_path: None,
        }
    }

    /// Attach a context builder (optional)
    pub fn with_context_builder(mut self, builder: Arc<dyn ContextBuilder>) -> Self {
        self.context_builder = Some(builder);
        self
    }

    /// Attach runtime hook registry.
    pub fn with_hook_registry(mut self, hook_registry: Arc<HookRegistry>) -> Self {
        self.hook_registry = hook_registry;
        self
    }

    /// Attach lifecycle hook registry for SDK pipeline hooks.
    pub fn with_lifecycle_hooks(mut self, hooks: Arc<LifecycleHookRegistry>) -> Self {
        self.lifecycle_hooks = hooks;
        self
    }

    /// Attach skill catalog for planner-time instruction injection.
    pub fn with_skill_catalog(mut self, skill_catalog: Arc<RwLock<SkillCatalog>>) -> Self {
        self.skill_catalog = skill_catalog;
        self
    }

    /// Set config path for skill hot-reload discovery.
    pub fn with_skill_config_path(mut self, path: std::path::PathBuf) -> Self {
        self.skill_config_path = Some(path);
        self
    }
}
