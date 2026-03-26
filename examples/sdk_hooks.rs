//! SDK Hooks: Observe and intercept the orchestration pipeline.
//!
//! ```bash
//! export OPENROUTER_API_KEY="sk-or-..."
//! cargo run --example sdk_hooks
//! ```

use async_trait::async_trait;

use orchestral::core::action::{ActionResult};
use orchestral::core::executor::ExecutionResult;
use orchestral::core::spi::lifecycle::{
    LifecycleHook, StepContext, StepDecision, TurnContext,
};
use orchestral::core::types::{Plan, Step};
use orchestral::{Orchestral, SdkError};

/// A hook that logs every step execution.
struct LoggingHook;

#[async_trait]
impl LifecycleHook for LoggingHook {
    async fn on_turn_start(&self, ctx: &TurnContext) {
        println!("[hook] Turn started: intent={}", ctx.intent);
    }

    async fn on_plan_created(&self, plan: &mut Plan, ctx: &TurnContext) {
        println!(
            "[hook] Plan created: {} steps, goal={}",
            plan.steps.len(),
            plan.goal
        );
    }

    async fn before_step(&self, step: &Step, ctx: &StepContext) -> StepDecision {
        println!(
            "[hook] Before step: id={}, action={}",
            ctx.step_id, ctx.action
        );
        StepDecision::Continue
    }

    async fn after_step(&self, _step: &Step, result: &ActionResult, ctx: &StepContext) {
        let status = match result {
            ActionResult::Success { .. } => "success",
            ActionResult::Error { .. } => "error",
            _ => "other",
        };
        println!(
            "[hook] After step: id={}, action={}, status={}",
            ctx.step_id, ctx.action, status
        );
    }

    async fn on_execution_complete(&self, result: &ExecutionResult, _ctx: &TurnContext) {
        println!("[hook] Execution complete: {:?}", result);
    }

    async fn on_turn_end(&self, output: &str, _ctx: &TurnContext) {
        println!("[hook] Turn ended: {}", &output[..80.min(output.len())]);
    }
}

#[tokio::main]
async fn main() -> Result<(), SdkError> {
    let app = Orchestral::builder()
        .hook(LoggingHook)
        .planner_backend("openrouter")
        .planner_model("anthropic/claude-sonnet-4.5")
        .build()
        .await?;

    let result = app.run("What is 2 + 2?").await?;
    println!("\nFinal: {} — {}", result.status, result.message);

    Ok(())
}
