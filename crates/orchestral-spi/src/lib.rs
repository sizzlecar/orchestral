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

use orchestral_core::io::BlobStore;
use orchestral_core::store::{EventStore, InteractionId, ReferenceStore, TaskStore, ThreadId};
use orchestral_core::types::{StepId, TaskId};

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
    pub thread_id: ThreadId,
    pub interaction_id: InteractionId,
    pub task_id: Option<TaskId>,
    pub step_id: Option<StepId>,
    pub action: Option<String>,
    pub message: Option<String>,
    pub metadata: Value,
    pub extensions: Map<String, Value>,
}

#[derive(Clone)]
pub struct StoreBundle {
    pub event_store: Arc<dyn EventStore>,
    pub task_store: Arc<dyn TaskStore>,
    pub reference_store: Arc<dyn ReferenceStore>,
}

#[derive(Default)]
pub struct ComponentRegistry {
    pub stores: Option<StoreBundle>,
    pub blob_store: Option<Arc<dyn BlobStore>>,
    named_components: HashMap<String, SharedComponent>,
}

impl ComponentRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_stores(mut self, stores: StoreBundle) -> Self {
        self.stores = Some(stores);
        self
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
        let policy = *self.policy.read().await;
        let hooks = self.snapshot_hooks().await;

        match policy.mode {
            HookDispatchMode::Sequential => {
                for hook in hooks {
                    if !Self::run_hook(policy, hook, event, context).await {
                        break;
                    }
                }
            }
            HookDispatchMode::Parallel => {
                let futures = hooks
                    .into_iter()
                    .map(|hook| Self::run_hook(policy, hook, event, context));
                let _ = join_all(futures).await;
            }
        }
    }

    async fn run_hook(
        policy: HookExecutionPolicy,
        hook: Arc<dyn RuntimeHook>,
        event: &RuntimeHookEventEnvelope,
        context: &RuntimeHookContext,
    ) -> bool {
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
            Ok(()) => true,
            Err(err) => {
                tracing::warn!(
                    hook_id,
                    event_type = %event.event_type,
                    error = %err,
                    "runtime hook execution failed"
                );
                matches!(policy.failure_policy, HookFailurePolicy::FailOpen)
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
            thread_id: "thread-1".into(),
            interaction_id: "interaction-1".into(),
            task_id: Some("task-1".into()),
            step_id: Some("step-1".into()),
            action: Some("echo".to_string()),
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

        registry.dispatch(&sample_event(), &sample_context()).await;

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

        registry.dispatch(&sample_event(), &sample_context()).await;

        assert!(calls.lock().expect("lock").is_empty());
    }
}
