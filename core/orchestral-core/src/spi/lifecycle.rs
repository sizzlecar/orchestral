//! Typed lifecycle hooks for the orchestration pipeline.
//!
//! SDK users implement `LifecycleHook` to observe and intercept key points
//! in the plan → normalize → execute cycle. All methods have default
//! empty implementations — implement only the ones you need.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::action::ActionResult;
use crate::executor::ExecutionResult;
use crate::store::WorkingSet;
use crate::types::{Plan, Step};

/// Decision returned by `before_step` to control step execution.
#[derive(Debug, Clone)]
pub enum StepDecision {
    /// Proceed with step execution normally.
    Continue,
    /// Skip this step entirely.
    Skip { reason: String },
}

/// Context for turn-level hooks (on_turn_start, on_plan_created, etc.).
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct TurnContext {
    pub thread_id: String,
    pub interaction_id: String,
    pub task_id: String,
    pub intent: String,
    pub iteration: usize,
}

/// Context for step-level hooks (before_step, after_step).
#[non_exhaustive]
pub struct StepContext {
    pub thread_id: String,
    pub task_id: String,
    pub step_id: String,
    pub action: String,
    pub working_set: Arc<RwLock<WorkingSet>>,
}

impl std::fmt::Debug for StepContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StepContext")
            .field("thread_id", &self.thread_id)
            .field("task_id", &self.task_id)
            .field("step_id", &self.step_id)
            .field("action", &self.action)
            .finish()
    }
}

/// Typed lifecycle hooks for the orchestration pipeline.
///
/// All methods have default empty implementations. Override only the
/// hooks you need. New hooks may be added in future versions without
/// breaking existing implementations.
///
/// # Example
///
/// ```ignore
/// use orchestral_core::spi::lifecycle::{LifecycleHook, StepContext, StepDecision};
///
/// struct LoggingHook;
///
/// #[async_trait::async_trait]
/// impl LifecycleHook for LoggingHook {
///     async fn before_step(&self, step: &Step, ctx: &StepContext) -> StepDecision {
///         println!("About to run step {} (action: {})", ctx.step_id, ctx.action);
///         StepDecision::Continue
///     }
///
///     async fn after_step(&self, step: &Step, result: &ActionResult, ctx: &StepContext) {
///         println!("Step {} finished", ctx.step_id);
///     }
/// }
/// ```
#[async_trait]
pub trait LifecycleHook: Send + Sync {
    /// Called when a planning turn starts (before the planner runs).
    async fn on_turn_start(&self, _ctx: &TurnContext) {}

    /// Called after the planner produces a plan, before normalization.
    /// The plan can be mutated (e.g., inject extra steps, modify params).
    async fn on_plan_created(&self, _plan: &mut Plan, _ctx: &TurnContext) {}

    /// Called before a single step executes. Return `Skip` to bypass the step.
    async fn before_step(&self, _step: &Step, _ctx: &StepContext) -> StepDecision {
        StepDecision::Continue
    }

    /// Called after a step executes (success or failure).
    async fn after_step(&self, _step: &Step, _result: &ActionResult, _ctx: &StepContext) {}

    /// Called after the DAG executor finishes (all steps done or failed).
    async fn on_execution_complete(&self, _result: &ExecutionResult, _ctx: &TurnContext) {}

    /// Called when the planning turn ends (final output determined).
    async fn on_turn_end(&self, _output: &str, _ctx: &TurnContext) {}
}

/// Registry extension for lifecycle hooks.
pub struct LifecycleHookRegistry {
    hooks: tokio::sync::RwLock<Vec<Arc<dyn LifecycleHook>>>,
}

impl LifecycleHookRegistry {
    pub fn new() -> Self {
        Self {
            hooks: tokio::sync::RwLock::new(Vec::new()),
        }
    }

    pub async fn register(&self, hook: Arc<dyn LifecycleHook>) {
        self.hooks.write().await.push(hook);
    }

    pub async fn on_turn_start(&self, ctx: &TurnContext) {
        let hooks = self.hooks.read().await;
        for hook in hooks.iter() {
            hook.on_turn_start(ctx).await;
        }
    }

    pub async fn on_plan_created(&self, plan: &mut Plan, ctx: &TurnContext) {
        let hooks = self.hooks.read().await;
        for hook in hooks.iter() {
            hook.on_plan_created(plan, ctx).await;
        }
    }

    pub async fn before_step(&self, step: &Step, ctx: &StepContext) -> StepDecision {
        let hooks = self.hooks.read().await;
        for hook in hooks.iter() {
            match hook.before_step(step, ctx).await {
                StepDecision::Skip { reason } => return StepDecision::Skip { reason },
                StepDecision::Continue => {}
            }
        }
        StepDecision::Continue
    }

    pub async fn after_step(&self, step: &Step, result: &ActionResult, ctx: &StepContext) {
        let hooks = self.hooks.read().await;
        for hook in hooks.iter() {
            hook.after_step(step, result, ctx).await;
        }
    }

    pub async fn on_execution_complete(&self, result: &ExecutionResult, ctx: &TurnContext) {
        let hooks = self.hooks.read().await;
        for hook in hooks.iter() {
            hook.on_execution_complete(result, ctx).await;
        }
    }

    pub async fn on_turn_end(&self, output: &str, ctx: &TurnContext) {
        let hooks = self.hooks.read().await;
        for hook in hooks.iter() {
            hook.on_turn_end(output, ctx).await;
        }
    }
}

impl Default for LifecycleHookRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingHook {
        before_count: AtomicUsize,
        after_count: AtomicUsize,
    }

    impl CountingHook {
        fn new() -> Self {
            Self {
                before_count: AtomicUsize::new(0),
                after_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl LifecycleHook for CountingHook {
        async fn before_step(&self, _step: &Step, _ctx: &StepContext) -> StepDecision {
            self.before_count.fetch_add(1, Ordering::SeqCst);
            StepDecision::Continue
        }

        async fn after_step(&self, _step: &Step, _result: &ActionResult, _ctx: &StepContext) {
            self.after_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn test_step_ctx() -> StepContext {
        StepContext {
            thread_id: "t1".to_string(),
            task_id: "task1".to_string(),
            step_id: "s1".to_string(),
            action: "shell".to_string(),
            working_set: Arc::new(RwLock::new(WorkingSet::new())),
        }
    }

    #[tokio::test]
    async fn test_lifecycle_registry_dispatches_hooks() {
        let registry = LifecycleHookRegistry::new();
        let hook = Arc::new(CountingHook::new());
        registry.register(hook.clone()).await;

        let step = Step::action("s1", "shell");
        let ctx = test_step_ctx();
        let result = ActionResult::success();

        let decision = registry.before_step(&step, &ctx).await;
        assert!(matches!(decision, StepDecision::Continue));
        registry.after_step(&step, &result, &ctx).await;

        assert_eq!(hook.before_count.load(Ordering::SeqCst), 1);
        assert_eq!(hook.after_count.load(Ordering::SeqCst), 1);
    }

    struct SkippingHook;

    #[async_trait]
    impl LifecycleHook for SkippingHook {
        async fn before_step(&self, _step: &Step, _ctx: &StepContext) -> StepDecision {
            StepDecision::Skip {
                reason: "blocked by policy".to_string(),
            }
        }
    }

    #[tokio::test]
    async fn test_before_step_skip_short_circuits() {
        let registry = LifecycleHookRegistry::new();
        let counting = Arc::new(CountingHook::new());
        let skipping = Arc::new(SkippingHook);

        // Register skipping hook first, then counting — counting should never fire
        registry.register(skipping).await;
        registry.register(counting.clone()).await;

        let step = Step::action("s1", "shell");
        let ctx = test_step_ctx();

        let decision = registry.before_step(&step, &ctx).await;
        assert!(matches!(decision, StepDecision::Skip { .. }));
        // Counting hook's before_step was never called because skipping hook short-circuited
        assert_eq!(counting.before_count.load(Ordering::SeqCst), 0);
    }
}
