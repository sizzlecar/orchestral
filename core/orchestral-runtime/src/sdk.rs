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

impl OrchestralApp {
    /// Run a single user input and return the result.
    pub async fn run(&self, input: &str) -> Result<RunResult, SdkError> {
        let event = Event::user_input(
            self.orchestrator.thread_runtime.thread_id().await.as_str(),
            "",
            Value::String(input.to_string()),
        );

        let result = self
            .orchestrator
            .handle_event(event)
            .await
            .map_err(|e| SdkError::Runtime(e.to_string()))?;

        Ok(RunResult::from_orchestrator_result(result))
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
    fn from_orchestrator_result(result: OrchestratorResult) -> Self {
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
                message: execution_result_message(&result),
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
