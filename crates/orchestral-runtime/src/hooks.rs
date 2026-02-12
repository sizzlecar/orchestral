use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::RwLock;

use orchestral_core::store::{InteractionId, ThreadId};
use orchestral_core::types::{StepId, TaskId};

/// Step-level hook context exposed to runtime extensions.
#[derive(Debug, Clone)]
pub struct StepHookContext {
    pub thread_id: ThreadId,
    pub interaction_id: InteractionId,
    pub task_id: TaskId,
    pub step_id: Option<StepId>,
    pub action: Option<String>,
    pub phase: String,
    pub message: Option<String>,
    pub metadata: Value,
}

/// Runtime hook extension point.
#[async_trait]
pub trait RuntimeHook: Send + Sync {
    async fn on_before_step(&self, _ctx: &StepHookContext) -> Result<(), String> {
        Ok(())
    }

    async fn on_after_step(&self, _ctx: &StepHookContext) -> Result<(), String> {
        Ok(())
    }

    async fn on_step_error(&self, _ctx: &StepHookContext) -> Result<(), String> {
        Ok(())
    }

    async fn on_execution_progress(&self, _ctx: &StepHookContext) -> Result<(), String> {
        Ok(())
    }
}

/// Registry for runtime hooks.
#[derive(Default)]
pub struct HookRegistry {
    hooks: RwLock<Vec<Arc<dyn RuntimeHook>>>,
}

impl HookRegistry {
    pub fn new() -> Self {
        Self {
            hooks: RwLock::new(Vec::new()),
        }
    }

    pub async fn register(&self, hook: Arc<dyn RuntimeHook>) {
        self.hooks.write().await.push(hook);
    }

    pub async fn register_many(&self, hooks: Vec<Arc<dyn RuntimeHook>>) {
        self.hooks.write().await.extend(hooks);
    }

    async fn snapshot(&self) -> Vec<Arc<dyn RuntimeHook>> {
        self.hooks.read().await.clone()
    }

    pub async fn on_before_step(&self, ctx: &StepHookContext) {
        for hook in self.snapshot().await {
            if let Err(err) = hook.on_before_step(ctx).await {
                tracing::warn!(error = %err, phase = %ctx.phase, "runtime hook on_before_step failed");
            }
        }
    }

    pub async fn on_after_step(&self, ctx: &StepHookContext) {
        for hook in self.snapshot().await {
            if let Err(err) = hook.on_after_step(ctx).await {
                tracing::warn!(error = %err, phase = %ctx.phase, "runtime hook on_after_step failed");
            }
        }
    }

    pub async fn on_step_error(&self, ctx: &StepHookContext) {
        for hook in self.snapshot().await {
            if let Err(err) = hook.on_step_error(ctx).await {
                tracing::warn!(error = %err, phase = %ctx.phase, "runtime hook on_step_error failed");
            }
        }
    }

    pub async fn on_execution_progress(&self, ctx: &StepHookContext) {
        for hook in self.snapshot().await {
            if let Err(err) = hook.on_execution_progress(ctx).await {
                tracing::warn!(error = %err, phase = %ctx.phase, "runtime hook on_execution_progress failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct RecordingHook {
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl RuntimeHook for RecordingHook {
        async fn on_before_step(&self, _ctx: &StepHookContext) -> Result<(), String> {
            self.calls.lock().expect("lock").push("before");
            Ok(())
        }

        async fn on_after_step(&self, _ctx: &StepHookContext) -> Result<(), String> {
            self.calls.lock().expect("lock").push("after");
            Ok(())
        }

        async fn on_step_error(&self, _ctx: &StepHookContext) -> Result<(), String> {
            self.calls.lock().expect("lock").push("error");
            Ok(())
        }

        async fn on_execution_progress(&self, _ctx: &StepHookContext) -> Result<(), String> {
            self.calls.lock().expect("lock").push("progress");
            Ok(())
        }
    }

    fn sample_ctx(phase: &str) -> StepHookContext {
        StepHookContext {
            thread_id: "thread-1".into(),
            interaction_id: "int-1".into(),
            task_id: "task-1".into(),
            step_id: Some("step-1".into()),
            action: Some("echo".to_string()),
            phase: phase.to_string(),
            message: Some("ok".to_string()),
            metadata: serde_json::json!({"n":1}),
        }
    }

    #[tokio::test]
    async fn test_hook_registry_dispatches_callbacks() {
        let registry = HookRegistry::new();
        let calls = Arc::new(Mutex::new(Vec::new()));
        registry
            .register(Arc::new(RecordingHook {
                calls: calls.clone(),
            }))
            .await;

        registry.on_before_step(&sample_ctx("step_started")).await;
        registry.on_after_step(&sample_ctx("step_completed")).await;
        registry.on_step_error(&sample_ctx("step_failed")).await;
        registry
            .on_execution_progress(&sample_ctx("step_started"))
            .await;

        let got = calls.lock().expect("lock").clone();
        assert_eq!(got, vec!["before", "after", "error", "progress"]);
    }

    struct FailingHook;

    #[async_trait]
    impl RuntimeHook for FailingHook {
        async fn on_before_step(&self, _ctx: &StepHookContext) -> Result<(), String> {
            Err("boom".to_string())
        }
    }

    #[tokio::test]
    async fn test_hook_registry_tolerates_hook_failures() {
        let registry = HookRegistry::new();
        registry.register(Arc::new(FailingHook)).await;
        registry.on_before_step(&sample_ctx("step_started")).await;
    }
}
