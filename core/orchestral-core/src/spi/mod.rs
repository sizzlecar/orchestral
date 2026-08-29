use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::sync::RwLock;

use crate::agent_protocol::wire::{AgentSessionId, RunId};
use crate::io::BlobStore;
use crate::types::{StepId, WorkflowId};

pub type SharedComponent = Arc<dyn Any + Send + Sync>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpiMeta {
    pub spi_version: String,
    pub runtime_version: String,
    pub spi_version_range: String,
    pub capabilities: Vec<String>,
    pub extensions: Map<String, Value>,
}

impl SpiMeta {
    pub fn runtime_defaults(runtime_version: impl Into<String>) -> Self {
        Self {
            spi_version: "1.0.0".to_string(),
            runtime_version: runtime_version.into(),
            spi_version_range: ">=1.0,<2.0".to_string(),
            capabilities: vec!["component_factory".to_string(), "runtime_hook".to_string()],
            extensions: Map::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeBuildRequest {
    pub meta: SpiMeta,
    pub config_path: String,
    pub profile: Option<String>,
    pub options: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeHookEventEnvelope {
    pub meta: SpiMeta,
    pub event_type: String,
    pub event_version: String,
    pub occurred_at_unix_ms: i64,
    pub payload: Value,
    pub extensions: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeHookContext {
    pub session_id: Option<AgentSessionId>,
    pub run_id: Option<RunId>,
    pub workflow_id: Option<WorkflowId>,
    pub step_id: Option<StepId>,
    pub tool_name: Option<String>,
    pub message: Option<String>,
    pub metadata: Value,
    pub extensions: Map<String, Value>,
}

#[derive(Default)]
pub struct ComponentRegistry {
    pub blob_store: Option<Arc<dyn BlobStore>>,
    named_components: HashMap<String, SharedComponent>,
}

impl ComponentRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_blob_store(mut self, blob_store: Arc<dyn BlobStore>) -> Self {
        self.blob_store = Some(blob_store);
        self
    }

    pub fn insert_named_component(
        &mut self,
        key: impl Into<String>,
        component: SharedComponent,
    ) -> Option<SharedComponent> {
        self.named_components.insert(key.into(), component)
    }

    pub fn get_named_component(&self, key: &str) -> Option<SharedComponent> {
        self.named_components.get(key).cloned()
    }

    pub fn into_named_components(self) -> HashMap<String, SharedComponent> {
        self.named_components
    }
}

#[derive(Debug, Error)]
pub enum SpiError {
    #[error("invalid build request: {0}")]
    InvalidBuildRequest(String),
    #[error("unsupported backend for {component}: {backend}")]
    UnsupportedBackend { component: String, backend: String },
    #[error("missing required setting for {component}: {setting}")]
    MissingSetting { component: String, setting: String },
    #[error("io error: {0}")]
    Io(String),
    #[error("internal error: {0}")]
    Internal(String),
}

#[async_trait]
pub trait RuntimeComponentFactory: Send + Sync {
    async fn build(&self, request: &RuntimeBuildRequest) -> Result<ComponentRegistry, SpiError>;
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct HookError {
    pub message: String,
}

#[derive(Debug, Error)]
#[error("runtime hook '{hook_id}' rejected {event_type}: {message}")]
pub struct HookDispatchError {
    pub hook_id: String,
    pub event_type: String,
    pub message: String,
}

impl HookError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[async_trait]
pub trait RuntimeHook: Send + Sync {
    fn id(&self) -> &'static str {
        "runtime_hook"
    }

    async fn on_event(
        &self,
        event: &RuntimeHookEventEnvelope,
        context: &RuntimeHookContext,
    ) -> Result<(), HookError>;
}

#[derive(Debug, Clone, Copy)]
pub enum HookDispatchMode {
    Sequential,
    Parallel,
}

#[derive(Debug, Clone, Copy)]
pub enum HookFailurePolicy {
    FailOpen,
    FailClosed,
}

#[derive(Debug, Clone, Copy)]
pub struct HookExecutionPolicy {
    pub mode: HookDispatchMode,
    pub failure_policy: HookFailurePolicy,
    pub timeout: Option<Duration>,
}

impl Default for HookExecutionPolicy {
    fn default() -> Self {
        Self {
            mode: HookDispatchMode::Sequential,
            failure_policy: HookFailurePolicy::FailOpen,
            timeout: Some(Duration::from_secs(3)),
        }
    }
}

#[derive(Default)]
pub struct HookRegistry {
    hooks: RwLock<Vec<Arc<dyn RuntimeHook>>>,
    policy: RwLock<HookExecutionPolicy>,
}

impl HookRegistry {
    pub fn new() -> Self {
        Self {
            hooks: RwLock::new(Vec::new()),
            policy: RwLock::new(HookExecutionPolicy::default()),
        }
    }

    pub async fn set_policy(&self, policy: HookExecutionPolicy) {
        *self.policy.write().await = policy;
    }

    pub async fn register(&self, hook: Arc<dyn RuntimeHook>) {
        self.hooks.write().await.push(hook);
    }

    pub async fn register_many(&self, hooks: Vec<Arc<dyn RuntimeHook>>) {
        self.hooks.write().await.extend(hooks);
    }

    async fn snapshot_hooks(&self) -> Vec<Arc<dyn RuntimeHook>> {
        self.hooks.read().await.clone()
    }

    pub async fn dispatch(&self, event: &RuntimeHookEventEnvelope, context: &RuntimeHookContext) {
        let _ = self.dispatch_checked(event, context).await;
    }

    /// Dispatches hooks and surfaces fail-closed rejection to the lifecycle
    /// owner. Fail-open hook errors are logged and do not fail this call.
    pub async fn dispatch_checked(
        &self,
        event: &RuntimeHookEventEnvelope,
        context: &RuntimeHookContext,
    ) -> Result<(), HookDispatchError> {
        let policy = *self.policy.read().await;
        let hooks = self.snapshot_hooks().await;

        match policy.mode {
            HookDispatchMode::Sequential => {
                for hook in hooks {
                    if let Err(error) = Self::run_hook(policy, hook, event, context).await {
                        if matches!(policy.failure_policy, HookFailurePolicy::FailClosed) {
                            return Err(error);
                        }
                    }
                }
            }
            HookDispatchMode::Parallel => {
                let futures = hooks
                    .into_iter()
                    .map(|hook| Self::run_hook(policy, hook, event, context));
                let results = join_all(futures).await;
                if matches!(policy.failure_policy, HookFailurePolicy::FailClosed) {
                    if let Some(error) = results.into_iter().find_map(Result::err) {
                        return Err(error);
                    }
                }
            }
        }
        Ok(())
    }

    async fn run_hook(
        policy: HookExecutionPolicy,
        hook: Arc<dyn RuntimeHook>,
        event: &RuntimeHookEventEnvelope,
        context: &RuntimeHookContext,
    ) -> Result<(), HookDispatchError> {
        let hook_id = hook.id();
        let call = hook.on_event(event, context);
        let result = if let Some(timeout) = policy.timeout {
            match tokio::time::timeout(timeout, call).await {
                Ok(res) => res,
                Err(_) => Err(HookError::new(format!("hook '{}' timed out", hook_id))),
            }
        } else {
            call.await
        };

        match result {
            Ok(()) => Ok(()),
            Err(err) => {
                tracing::warn!(
                    hook_id,
                    event_type = %event.event_type,
                    error = %err,
                    "runtime hook execution failed"
                );
                Err(HookDispatchError {
                    hook_id: hook_id.to_owned(),
                    event_type: event.event_type.clone(),
                    message: err.message,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct RecordingHook {
        id: &'static str,
        calls: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl RuntimeHook for RecordingHook {
        fn id(&self) -> &'static str {
            self.id
        }

        async fn on_event(
            &self,
            event: &RuntimeHookEventEnvelope,
            _context: &RuntimeHookContext,
        ) -> Result<(), HookError> {
            self.calls
                .lock()
                .expect("lock")
                .push(format!("{}:{}", self.id, event.event_type));
            Ok(())
        }
    }

    struct FailingHook;

    #[async_trait]
    impl RuntimeHook for FailingHook {
        fn id(&self) -> &'static str {
            "failing"
        }

        async fn on_event(
            &self,
            _event: &RuntimeHookEventEnvelope,
            _context: &RuntimeHookContext,
        ) -> Result<(), HookError> {
            Err(HookError::new("boom"))
        }
    }

    fn sample_event() -> RuntimeHookEventEnvelope {
        RuntimeHookEventEnvelope {
            meta: SpiMeta::runtime_defaults("0.1.0"),
            event_type: "step.started".to_string(),
            event_version: "1.0.0".to_string(),
            occurred_at_unix_ms: 1,
            payload: serde_json::json!({"k":"v"}),
            extensions: Map::new(),
        }
    }

    fn sample_context() -> RuntimeHookContext {
        RuntimeHookContext {
            session_id: Some(AgentSessionId::new("session-1")),
            run_id: Some(RunId::new("run-1")),
            workflow_id: Some(WorkflowId::new("workflow-1")),
            step_id: Some("step-1".into()),
            tool_name: Some("echo".to_string()),
            message: None,
            metadata: Value::Null,
            extensions: Map::new(),
        }
    }

    #[tokio::test]
    async fn test_dispatch_sequential_order() {
        let registry = HookRegistry::new();
        registry
            .set_policy(HookExecutionPolicy {
                mode: HookDispatchMode::Sequential,
                failure_policy: HookFailurePolicy::FailOpen,
                timeout: None,
            })
            .await;
        let calls = Arc::new(Mutex::new(Vec::new()));
        registry
            .register(Arc::new(RecordingHook {
                id: "h1",
                calls: calls.clone(),
            }))
            .await;
        registry
            .register(Arc::new(RecordingHook {
                id: "h2",
                calls: calls.clone(),
            }))
            .await;

        registry
            .dispatch_checked(&sample_event(), &sample_context())
            .await
            .expect("fail-open hook error must not reject lifecycle");

        assert_eq!(
            calls.lock().expect("lock").clone(),
            vec!["h1:step.started".to_string(), "h2:step.started".to_string()]
        );
    }

    #[tokio::test]
    async fn test_dispatch_fail_open_continues() {
        let registry = HookRegistry::new();
        registry
            .set_policy(HookExecutionPolicy {
                mode: HookDispatchMode::Sequential,
                failure_policy: HookFailurePolicy::FailOpen,
                timeout: None,
            })
            .await;
        let calls = Arc::new(Mutex::new(Vec::new()));
        registry.register(Arc::new(FailingHook)).await;
        registry
            .register(Arc::new(RecordingHook {
                id: "h2",
                calls: calls.clone(),
            }))
            .await;

        registry.dispatch(&sample_event(), &sample_context()).await;

        assert_eq!(
            calls.lock().expect("lock").clone(),
            vec!["h2:step.started".to_string()]
        );
    }

    #[tokio::test]
    async fn test_dispatch_fail_closed_stops() {
        let registry = HookRegistry::new();
        registry
            .set_policy(HookExecutionPolicy {
                mode: HookDispatchMode::Sequential,
                failure_policy: HookFailurePolicy::FailClosed,
                timeout: None,
            })
            .await;
        let calls = Arc::new(Mutex::new(Vec::new()));
        registry.register(Arc::new(FailingHook)).await;
        registry
            .register(Arc::new(RecordingHook {
                id: "h2",
                calls: calls.clone(),
            }))
            .await;

        let error = registry
            .dispatch_checked(&sample_event(), &sample_context())
            .await
            .expect_err("fail-closed hook error must reject lifecycle");

        assert!(calls.lock().expect("lock").is_empty());
        assert_eq!(error.hook_id, "failing");
    }
}
