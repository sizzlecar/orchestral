//! Orchestrator - minimal intent → plan → normalize → execute pipeline
//!
//! This bridges the ThreadRuntime (events + concurrency) with core planning/execution.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::RwLock;

use orchestral_context::{
    ContextBuilder, ContextError, ContextRequest, ContextWindow, TokenBudget,
};
use orchestral_core::action::{extract_meta, ActionMeta};
use orchestral_core::executor::{ExecutionResult, Executor, ExecutorContext};
use orchestral_core::normalizer::{NormalizeError, PlanNormalizer};
use orchestral_core::planner::{HistoryItem, PlanError, Planner, PlannerContext};
use orchestral_core::store::{ReferenceStore, StoreError, TaskStore, WorkingSet};
use orchestral_core::types::{Intent, IntentContext, Task, TaskId, TaskState};
use orchestral_stores::Event;

use crate::{HandleEventResult, InteractionState, RuntimeError, ThreadRuntime};

/// Orchestrator result for a handled event
#[derive(Debug)]
pub enum OrchestratorResult {
    /// A new interaction was started and executed
    Started {
        interaction_id: String,
        task_id: TaskId,
        result: ExecutionResult,
    },
    /// The event was merged into an existing interaction and executed
    Merged {
        interaction_id: String,
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
    pub reference_store: Arc<dyn ReferenceStore>,
    pub context_builder: Option<Arc<dyn ContextBuilder>>,
    pub config: OrchestratorConfig,
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
    /// Whether to include references when building context
    pub include_references: bool,
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            history_limit: 50,
            context_budget: TokenBudget::default(),
            include_history: true,
            include_references: true,
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
        reference_store: Arc<dyn ReferenceStore>,
    ) -> Self {
        Self::with_config(
            thread_runtime,
            planner,
            normalizer,
            executor,
            task_store,
            reference_store,
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
        reference_store: Arc<dyn ReferenceStore>,
        config: OrchestratorConfig,
    ) -> Self {
        Self {
            thread_runtime,
            planner,
            normalizer,
            executor,
            task_store,
            reference_store,
            context_builder: None,
            config,
        }
    }

    /// Attach a context builder (optional)
    pub fn with_context_builder(mut self, builder: Arc<dyn ContextBuilder>) -> Self {
        self.context_builder = Some(builder);
        self
    }

    /// Handle an event end-to-end (intent → plan → normalize → execute)
    pub async fn handle_event(
        &self,
        event: Event,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        let event_clone = event.clone();
        let decision = self.thread_runtime.handle_event(event).await?;

        let (interaction_id, started_kind) = match &decision {
            HandleEventResult::Started { interaction_id } => (interaction_id.clone(), "started"),
            HandleEventResult::Merged { interaction_id } => (interaction_id.clone(), "merged"),
            HandleEventResult::Rejected { reason } => {
                return Ok(OrchestratorResult::Rejected {
                    reason: reason.clone(),
                });
            }
            HandleEventResult::Queued => return Ok(OrchestratorResult::Queued),
        };

        // Build intent from event
        let intent = intent_from_event(&event_clone, Some(interaction_id.clone()))?;

        // Create and persist task
        let mut task = Task::new(intent);
        self.task_store.save(&task).await?;
        self.thread_runtime
            .add_task_to_interaction(&interaction_id, task.id.clone())
            .await?;

        // Plan
        let actions = self.available_actions().await;
        let history = self.history_for_planner(&interaction_id, &task.id).await?;
        let context = PlannerContext::with_history(actions, history, self.reference_store.clone());
        let plan = self.planner.plan(&task.intent, &context).await?;
        task.set_plan(plan.clone());
        task.start_executing();
        self.task_store.save(&task).await?;

        // Normalize
        let normalized = self.normalizer.normalize(plan)?;

        // Execute
        let working_set = Arc::new(RwLock::new(WorkingSet::new()));
        let exec_ctx =
            ExecutorContext::new(task.id.clone(), working_set, self.reference_store.clone());
        let mut dag = normalized.dag;
        let result = self.executor.execute(&mut dag, &exec_ctx).await;

        // Update task + interaction state
        let new_state = task_state_from_execution(&result);
        task.set_state(new_state.clone());
        self.task_store.save(&task).await?;
        self.thread_runtime
            .update_interaction_state(&interaction_id, interaction_state_from_task(&new_state))
            .await?;

        let response = match started_kind {
            "started" => OrchestratorResult::Started {
                interaction_id,
                task_id: task.id,
                result,
            },
            _ => OrchestratorResult::Merged {
                interaction_id,
                task_id: task.id,
                result,
            },
        };

        Ok(response)
    }

    async fn available_actions(&self) -> Vec<ActionMeta> {
        let mut actions = Vec::new();
        let registry = self.executor.action_registry.read().await;
        for name in registry.names() {
            if let Some(action) = registry.get(&name) {
                actions.push(extract_meta(action.as_ref()));
            }
        }
        actions
    }

    async fn history_for_planner(
        &self,
        interaction_id: &str,
        task_id: &str,
    ) -> Result<Vec<HistoryItem>, OrchestratorError> {
        if let Some(builder) = &self.context_builder {
            let request = ContextRequest {
                thread_id: self.thread_runtime.thread_id().await,
                task_id: Some(task_id.to_string()),
                interaction_id: Some(interaction_id.to_string()),
                query: None,
                budget: self.config.context_budget.clone(),
                include_history: self.config.include_history,
                include_references: self.config.include_references,
                ref_type_filter: None,
                tags: Vec::new(),
            };
            let window = builder.build(&request).await?;
            return Ok(context_window_to_history(&window));
        }

        let mut events = self
            .thread_runtime
            .query_history(self.config.history_limit)
            .await?;
        events.sort_by(|a, b| a.timestamp().cmp(&b.timestamp()));
        Ok(events.iter().filter_map(event_to_history_item).collect())
    }
}

fn intent_from_event(
    event: &Event,
    interaction_id: Option<String>,
) -> Result<Intent, OrchestratorError> {
    let mut metadata = HashMap::new();
    metadata.insert(
        "event_type".to_string(),
        Value::String(event_type_label(event).to_string()),
    );
    if let Some(id) = interaction_id {
        metadata.insert("interaction_id".to_string(), Value::String(id));
    }

    let context = IntentContext {
        thread_id: Some(event.thread_id().to_string()),
        previous_task_id: None,
        metadata,
    };

    match event {
        Event::UserInput { payload, .. } => {
            Ok(Intent::with_context(payload_to_string(payload), context))
        }
        Event::ExternalEvent { kind, payload, .. } => {
            let content = format!("external:{} {}", kind, payload_to_string(payload));
            Ok(Intent::with_context(content, context))
        }
        Event::AssistantOutput { payload, .. } => {
            Ok(Intent::with_context(payload_to_string(payload), context))
        }
        Event::SystemTrace { level, payload, .. } => Ok(Intent::with_context(
            format!("trace:{} {}", level, payload_to_string(payload)),
            context,
        )),
        Event::Artifact { reference_id, .. } => Ok(Intent::with_context(
            format!("artifact:{}", reference_id),
            context,
        )),
    }
}

fn payload_to_string(payload: &Value) -> String {
    if let Some(s) = payload.as_str() {
        return s.to_string();
    }
    for key in ["content", "message", "text"] {
        if let Some(s) = payload.get(key).and_then(|v| v.as_str()) {
            return s.to_string();
        }
    }
    payload.to_string()
}

fn event_to_history_item(event: &Event) -> Option<HistoryItem> {
    let timestamp = event.timestamp();
    match event {
        Event::UserInput { payload, .. } => Some(HistoryItem {
            role: "user".to_string(),
            content: payload_to_string(payload),
            timestamp,
        }),
        Event::AssistantOutput { payload, .. } => Some(HistoryItem {
            role: "assistant".to_string(),
            content: payload_to_string(payload),
            timestamp,
        }),
        Event::ExternalEvent { kind, payload, .. } => Some(HistoryItem {
            role: "system".to_string(),
            content: format!("external:{} {}", kind, payload_to_string(payload)),
            timestamp,
        }),
        Event::SystemTrace { level, payload, .. } => Some(HistoryItem {
            role: "system".to_string(),
            content: format!("trace:{} {}", level, payload_to_string(payload)),
            timestamp,
        }),
        Event::Artifact { reference_id, .. } => Some(HistoryItem {
            role: "system".to_string(),
            content: format!("artifact:{}", reference_id),
            timestamp,
        }),
    }
}

fn context_window_to_history(window: &ContextWindow) -> Vec<HistoryItem> {
    let mut items = Vec::new();
    for slice in window.core.iter().chain(window.optional.iter()) {
        items.push(HistoryItem {
            role: slice.role.clone(),
            content: slice.content.clone(),
            timestamp: slice.timestamp.unwrap_or_else(chrono::Utc::now),
        });
    }
    items
}

fn event_type_label(event: &Event) -> &'static str {
    match event {
        Event::UserInput { .. } => "user_input",
        Event::AssistantOutput { .. } => "assistant_output",
        Event::Artifact { .. } => "artifact",
        Event::ExternalEvent { .. } => "external_event",
        Event::SystemTrace { .. } => "system_trace",
    }
}

fn task_state_from_execution(result: &ExecutionResult) -> TaskState {
    match result {
        ExecutionResult::Completed => TaskState::Done,
        ExecutionResult::Failed { error, .. } => TaskState::Failed {
            reason: error.clone(),
            recoverable: false,
        },
        ExecutionResult::WaitingUser { prompt, .. } => TaskState::WaitingUser {
            prompt: prompt.clone(),
        },
        ExecutionResult::WaitingEvent { event_type, .. } => TaskState::WaitingEvent {
            event_type: event_type.clone(),
        },
    }
}

fn interaction_state_from_task(state: &TaskState) -> InteractionState {
    match state {
        TaskState::Done => InteractionState::Completed,
        TaskState::Failed { .. } => InteractionState::Failed,
        TaskState::WaitingUser { .. } => InteractionState::WaitingUser,
        TaskState::WaitingEvent { .. } => InteractionState::WaitingEvent,
        TaskState::Paused => InteractionState::Paused,
        TaskState::Planning | TaskState::Executing => InteractionState::Active,
    }
}
