//! SDK builder for programmatic Orchestral setup.
//!
//! ```ignore
//! use orchestral::prelude::*;
//! use orchestral::sdk::{Orchestral, RunResult};
//!
//! let app = Orchestral::builder()
//!     .action(MyAction::new())
//!     .planner_backend("openrouter")
//!     .planner_model("anthropic/claude-sonnet-4.5")
//!     .planner_api_key_env("OPENROUTER_API_KEY")
//!     .build()
//!     .await
//!     .unwrap();
//!
//! let result = app.run("read README.md").await.unwrap();
//! println!("{}", result.message);
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use serde_json::Value;
use thiserror::Error;

use orchestral_core::action::Action;
use orchestral_core::config::BackendSpec;
use orchestral_core::executor::{ExecutionResult, Executor};
use orchestral_core::normalizer::PlanNormalizer;
use orchestral_core::planner::Planner;
use orchestral_core::spi::lifecycle::{LifecycleHook, LifecycleHookRegistry};
use orchestral_core::store::{Event, InMemoryEventStore, InMemoryTaskStore};

use crate::action::{ActionRegistryManager, DefaultActionFactory};
use crate::concurrency::DefaultConcurrencyPolicy;
use crate::context::{BasicContextBuilder, TokenBudget};
use crate::orchestrator::OrchestratorConfig;
use crate::planner::{
    build_client_from_backend, LlmInvocationConfig, LlmPlanner, LlmPlannerConfig,
};
use crate::thread::Thread;
use crate::thread_runtime::{ThreadRuntime, ThreadRuntimeConfig};
use crate::{Orchestrator, OrchestratorResult};

/// Entry point for the SDK.
pub struct Orchestral;

impl Orchestral {
    /// Create a new builder for programmatic setup.
    pub fn builder() -> OrchestralBuilder {
        OrchestralBuilder::default()
    }
}

/// Builder for constructing an OrchestralApp without YAML config.
pub struct OrchestralBuilder {
    custom_actions: Vec<Arc<dyn Action>>,
    lifecycle_hooks: Vec<Arc<dyn LifecycleHook>>,
    planner_backend: Option<String>,
    planner_model: Option<String>,
    planner_api_key_env: Option<String>,
    planner_temperature: Option<f32>,
    max_planner_iterations: usize,
    config_path: Option<PathBuf>,
}

impl Default for OrchestralBuilder {
    fn default() -> Self {
        Self {
            custom_actions: Vec::new(),
            lifecycle_hooks: Vec::new(),
            planner_backend: None,
            planner_model: None,
            planner_api_key_env: None,
            planner_temperature: None,
            max_planner_iterations: 6,
            config_path: None,
        }
    }
}

impl OrchestralBuilder {
    /// Register a custom action.
    pub fn action(mut self, action: impl Action + 'static) -> Self {
        self.custom_actions.push(Arc::new(action));
        self
    }

    /// Register a lifecycle hook.
    pub fn hook(mut self, hook: impl LifecycleHook + 'static) -> Self {
        self.lifecycle_hooks.push(Arc::new(hook));
        self
    }

    /// Register a lifecycle hook from an existing Arc (for shared ownership).
    pub fn hook_arc(mut self, hook: Arc<dyn LifecycleHook>) -> Self {
        self.lifecycle_hooks.push(hook);
        self
    }

    /// Set the LLM backend (e.g., "openrouter", "openai", "anthropic").
    pub fn planner_backend(mut self, backend: impl Into<String>) -> Self {
        self.planner_backend = Some(backend.into());
        self
    }

    /// Set the LLM model (e.g., "anthropic/claude-sonnet-4.5").
    pub fn planner_model(mut self, model: impl Into<String>) -> Self {
        self.planner_model = Some(model.into());
        self
    }

    /// Set the environment variable name for the API key.
    pub fn planner_api_key_env(mut self, env_var: impl Into<String>) -> Self {
        self.planner_api_key_env = Some(env_var.into());
        self
    }

    /// Set the planner temperature (0.0 - 1.0).
    pub fn planner_temperature(mut self, temperature: f32) -> Self {
        self.planner_temperature = Some(temperature);
        self
    }

    /// Set the maximum planner iterations per turn (default: 6).
    pub fn max_planner_iterations(mut self, max: usize) -> Self {
        self.max_planner_iterations = max;
        self
    }

    /// Optionally load base config from a YAML file.
    /// Builder settings override YAML values.
    pub fn config_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.config_path = Some(path.into());
        self
    }

    /// Build the OrchestralApp.
    pub async fn build(self) -> Result<OrchestralApp, SdkError> {
        // Stores
        let event_store = Arc::new(InMemoryEventStore::new());
        let task_store = Arc::new(InMemoryTaskStore::new());

        // Action registry: load from config if available, then add custom actions
        let factory = Arc::new(DefaultActionFactory::new());
        let config_path = self
            .config_path
            .clone()
            .unwrap_or_else(|| PathBuf::from("orchestral.yaml"));
        let registry_manager = ActionRegistryManager::new(config_path.clone(), factory);
        // Try to load from config — ignore errors (config may not exist)
        let _ = registry_manager.load().await;
        // Register custom actions
        let registry = registry_manager.registry();
        {
            let mut reg = registry.write().await;
            for action in &self.custom_actions {
                reg.register(action.clone());
            }
        }

        // Normalizer — sync from registry
        let mut normalizer = PlanNormalizer::new();
        {
            let reg = registry.read().await;
            for name in reg.names() {
                if let Some(action) = reg.get(&name) {
                    normalizer.register_action_meta(&orchestral_core::action::extract_meta(
                        action.as_ref(),
                    ));
                }
            }
        }

        // Planner
        let planner: Arc<dyn Planner> = self.build_planner()?;

        // Executor
        let executor = Executor::with_registry(registry.clone());

        // Thread runtime
        let thread_runtime = ThreadRuntime::with_policy_and_config(
            Thread::new(),
            event_store.clone(),
            Arc::new(DefaultConcurrencyPolicy),
            ThreadRuntimeConfig::default(),
        );

        // Lifecycle hooks
        let lifecycle_hooks = Arc::new(LifecycleHookRegistry::new());
        for hook in self.lifecycle_hooks {
            lifecycle_hooks.register(hook).await;
        }

        // Context builder
        let context_builder = Arc::new(BasicContextBuilder::new(event_store.clone()));

        // Orchestrator config
        let config = OrchestratorConfig {
            history_limit: 50,
            context_budget: TokenBudget::new(4096),
            include_history: true,
            auto_replan_once: true,
            auto_repair_plan_once: true,
            max_planner_iterations: self.max_planner_iterations,
        };

        let mut orchestrator = Orchestrator::with_config(
            thread_runtime,
            planner,
            normalizer,
            executor,
            task_store,
            config,
        )
        .with_context_builder(context_builder)
        .with_lifecycle_hooks(lifecycle_hooks);

        if let Some(path) = &self.config_path {
            orchestrator = orchestrator.with_skill_config_path(path.clone());
        }

        Ok(OrchestralApp { orchestrator })
    }

    fn build_planner(&self) -> Result<Arc<dyn Planner>, SdkError> {
        let model = self
            .planner_model
            .clone()
            .unwrap_or_else(|| "anthropic/claude-sonnet-4.5".to_string());
        let api_key_env = self.planner_api_key_env.clone().unwrap_or_else(|| {
            // Auto-detect from backend
            match self.planner_backend.as_deref() {
                Some("openai") => "OPENAI_API_KEY",
                Some("anthropic") | Some("claude") => "ANTHROPIC_API_KEY",
                Some("google") | Some("gemini") => "GOOGLE_API_KEY",
                _ => "OPENROUTER_API_KEY",
            }
            .to_string()
        });

        let backend_kind = self
            .planner_backend
            .clone()
            .unwrap_or_else(|| "openrouter".to_string());
        let backend_spec = BackendSpec {
            name: backend_kind.clone(),
            kind: backend_kind,
            endpoint: None,
            api_key_env: Some(api_key_env),
            config: Value::Null,
        };
        let invocation = LlmInvocationConfig {
            model: model.clone(),
            temperature: self.planner_temperature.unwrap_or(0.2),
            ..LlmInvocationConfig::default()
        };
        let client = build_client_from_backend(&backend_spec, &invocation)
            .map_err(|e| SdkError::Config(format!("failed to build LLM client: {}", e)))?;

        let config = LlmPlannerConfig {
            model,
            temperature: self.planner_temperature.unwrap_or(0.2),
            ..LlmPlannerConfig::default()
        };

        Ok(Arc::new(LlmPlanner::new(client, config)))
    }
}

/// The running Orchestral application.
pub struct OrchestralApp {
    pub orchestrator: Orchestrator,
}

impl std::fmt::Debug for OrchestralApp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrchestralApp").finish()
    }
}

impl OrchestralApp {
    /// Run a single user input and return the result.
    /// The message field contains the assistant's actual output text.
    pub async fn run(&self, input: &str) -> Result<RunResult, SdkError> {
        let thread_id = self
            .orchestrator
            .thread_runtime
            .thread_id()
            .await
            .to_string();
        let event = Event::user_input(thread_id.as_str(), "", Value::String(input.to_string()));

        let result = self
            .orchestrator
            .handle_event(event)
            .await
            .map_err(|e| SdkError::Runtime(e.to_string()))?;

        // Extract the actual assistant output from event store
        let assistant_message = self.last_assistant_output(&thread_id).await;

        Ok(RunResult::from_orchestrator_result(
            result,
            assistant_message,
        ))
    }

    /// Query the latest AssistantOutput event from the thread's event store.
    async fn last_assistant_output(&self, _thread_id: &str) -> Option<String> {
        let events = self
            .orchestrator
            .thread_runtime
            .query_history(20)
            .await
            .ok()?;

        events
            .iter()
            .rev()
            .filter_map(|event| match event {
                Event::AssistantOutput { payload, .. } => {
                    let text = payload
                        .as_str()
                        .map(String::from)
                        .or_else(|| {
                            payload
                                .get("content")
                                .or_else(|| payload.get("message"))
                                .or_else(|| payload.get("text"))
                                .and_then(|v| v.as_str())
                                .map(String::from)
                        })
                        .unwrap_or_else(|| payload.to_string());
                    if text.is_empty() {
                        None
                    } else {
                        Some(text)
                    }
                }
                _ => None,
            })
            .next()
    }
}

/// Result of a single `run()` call.
#[derive(Debug, Clone)]
pub struct RunResult {
    pub status: String,
    pub interaction_id: String,
    pub task_id: String,
    pub message: String,
}

impl RunResult {
    fn from_orchestrator_result(
        result: OrchestratorResult,
        assistant_message: Option<String>,
    ) -> Self {
        match result {
            OrchestratorResult::Started {
                interaction_id,
                task_id,
                result,
            }
            | OrchestratorResult::Merged {
                interaction_id,
                task_id,
                result,
            } => Self {
                status: execution_result_status(&result),
                interaction_id: interaction_id.to_string(),
                task_id: task_id.to_string(),
                message: assistant_message.unwrap_or_else(|| execution_result_message(&result)),
            },
            OrchestratorResult::Rejected { reason } => Self {
                status: "rejected".to_string(),
                interaction_id: String::new(),
                task_id: String::new(),
                message: reason,
            },
            OrchestratorResult::Queued => Self {
                status: "queued".to_string(),
                interaction_id: String::new(),
                task_id: String::new(),
                message: "Request queued".to_string(),
            },
        }
    }
}

fn execution_result_status(result: &ExecutionResult) -> String {
    match result {
        ExecutionResult::Completed => "completed".to_string(),
        ExecutionResult::Failed { .. } => "failed".to_string(),
        ExecutionResult::WaitingUser { .. } => "waiting_user".to_string(),
        ExecutionResult::WaitingEvent { .. } => "waiting_event".to_string(),
    }
}

fn execution_result_message(result: &ExecutionResult) -> String {
    match result {
        ExecutionResult::Completed => "Completed".to_string(),
        ExecutionResult::Failed { error, .. } => error.clone(),
        ExecutionResult::WaitingUser { prompt, .. } => prompt.clone(),
        ExecutionResult::WaitingEvent { event_type, .. } => {
            format!("Waiting for event: {}", event_type)
        }
    }
}

/// SDK errors.
#[derive(Debug, Error)]
pub enum SdkError {
    #[error("config error: {0}")]
    Config(String),
    #[error("runtime error: {0}")]
    Runtime(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
    use orchestral_core::spi::lifecycle::LifecycleHook;

    /// A deterministic action for testing — no LLM needed.
    struct EchoAction;

    #[async_trait::async_trait]
    impl Action for EchoAction {
        fn name(&self) -> &str {
            "echo"
        }
        fn description(&self) -> &str {
            "Echo input back"
        }
        fn metadata(&self) -> ActionMeta {
            ActionMeta::new("echo", "Echo input back")
                .with_input_schema(serde_json::json!({
                    "type": "object",
                    "properties": { "message": { "type": "string" } },
                    "required": ["message"]
                }))
                .with_output_schema(serde_json::json!({
                    "type": "object",
                    "properties": { "result": { "type": "string" } },
                    "required": ["result"]
                }))
        }
        async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
            let msg = input
                .params
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("no message");
            ActionResult::success_with(std::collections::HashMap::from([(
                "result".to_string(),
                Value::String(msg.to_string()),
            )]))
        }
    }

    #[test]
    fn test_builder_defaults() {
        let builder = Orchestral::builder();
        assert_eq!(builder.max_planner_iterations, 6);
        assert!(builder.custom_actions.is_empty());
        assert!(builder.lifecycle_hooks.is_empty());
        assert!(builder.planner_backend.is_none());
    }

    #[test]
    fn test_builder_fluent_api() {
        let builder = Orchestral::builder()
            .action(EchoAction)
            .planner_backend("openai")
            .planner_model("gpt-4o-mini")
            .planner_api_key_env("OPENAI_API_KEY")
            .planner_temperature(0.5)
            .max_planner_iterations(3);

        assert_eq!(builder.custom_actions.len(), 1);
        assert_eq!(builder.planner_backend.as_deref(), Some("openai"));
        assert_eq!(builder.planner_model.as_deref(), Some("gpt-4o-mini"));
        assert_eq!(
            builder.planner_api_key_env.as_deref(),
            Some("OPENAI_API_KEY")
        );
        assert_eq!(builder.planner_temperature, Some(0.5));
        assert_eq!(builder.max_planner_iterations, 3);
    }

    #[test]
    fn test_builder_with_hooks() {
        struct TestHook;

        #[async_trait::async_trait]
        impl LifecycleHook for TestHook {}

        let builder = Orchestral::builder().hook(TestHook).hook(TestHook);
        assert_eq!(builder.lifecycle_hooks.len(), 2);
    }

    #[test]
    fn test_builder_build_fails_with_invalid_backend() {
        tokio_test::block_on(async {
            // Use a nonexistent backend kind — should fail
            let result = Orchestral::builder()
                .planner_backend("nonexistent_backend_xyz")
                .planner_api_key_env("__ORCHESTRAL_TEST_NONEXISTENT_KEY__")
                .build()
                .await;

            assert!(result.is_err(), "expected error, got Ok");
        });
    }

    #[test]
    fn test_run_result_from_completed() {
        let result = RunResult::from_orchestrator_result(
            OrchestratorResult::Started {
                interaction_id: "i1".into(),
                task_id: "t1".into(),
                result: ExecutionResult::Completed,
            },
            Some("Hello from assistant".to_string()),
        );
        assert_eq!(result.status, "completed");
        assert_eq!(result.interaction_id, "i1");
        assert_eq!(result.message, "Hello from assistant");
    }

    #[test]
    fn test_run_result_from_completed_no_assistant_output() {
        let result = RunResult::from_orchestrator_result(
            OrchestratorResult::Started {
                interaction_id: "i1".into(),
                task_id: "t1".into(),
                result: ExecutionResult::Completed,
            },
            None,
        );
        assert_eq!(result.message, "Completed");
    }

    #[test]
    fn test_run_result_from_failed() {
        let result = RunResult::from_orchestrator_result(
            OrchestratorResult::Started {
                interaction_id: "i1".into(),
                task_id: "t1".into(),
                result: ExecutionResult::Failed {
                    step_id: "s1".into(),
                    error: "boom".to_string(),
                },
            },
            None,
        );
        assert_eq!(result.status, "failed");
        assert_eq!(result.message, "boom");
    }

    #[test]
    fn test_run_result_from_rejected() {
        let result = RunResult::from_orchestrator_result(
            OrchestratorResult::Rejected {
                reason: "too busy".to_string(),
            },
            None,
        );
        assert_eq!(result.status, "rejected");
        assert_eq!(result.message, "too busy");
    }

    #[test]
    fn test_run_result_from_queued() {
        let result = RunResult::from_orchestrator_result(OrchestratorResult::Queued, None);
        assert_eq!(result.status, "queued");
    }
}
