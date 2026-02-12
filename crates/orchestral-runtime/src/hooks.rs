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
