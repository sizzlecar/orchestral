//! Internal DAG scheduler for Agent-selected workflows.
//!
//! Every executable Step is dispatched through the mandatory, run-scoped
//! [`StepExecutionPort`]. The scheduler has no Action registry and no nested
//! Agent fallback, so it cannot create a second execution authority.

use std::time::Duration;

mod dag;
mod logging;
mod outcome;
mod progress;
mod run;
mod step_support;
mod types;

pub use self::dag::{DagNode, ExecutionDag, NodeState};
pub use self::outcome::StepOutcome;
pub use self::types::{
    ExecutionProgressEvent, ExecutionProgressReporter, ExecutionResult, ExecutorContext,
    StepExecutionPort, StepExecutionRequest,
};

use self::logging::{
    truncate_for_log, truncate_json_for_log, MAX_LOG_JSON_CHARS, MAX_LOG_TEXT_CHARS,
};
use self::progress::{
    build_step_completion_metadata, build_step_start_metadata, choose_terminal_result,
    report_progress,
};
pub use self::step_support::render_working_set_template;
use self::step_support::{bind_param_value, resolve_param_templates, validate_declared_exports};

const DEFAULT_MAX_RETRY_ATTEMPTS: u32 = 3;
const DEFAULT_RETRY_BASE_DELAY: Duration = Duration::from_millis(200);
const DEFAULT_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);

/// Deterministic DAG scheduler. Side effects remain owned by the injected port.
pub struct Executor {
    pub max_parallel: usize,
    pub max_retry_attempts: u32,
    pub retry_base_delay: Duration,
    pub retry_max_delay: Duration,
    pub strict_exports: bool,
}

impl Executor {
    pub fn new() -> Self {
        Self {
            max_parallel: 4,
            max_retry_attempts: DEFAULT_MAX_RETRY_ATTEMPTS,
            retry_base_delay: DEFAULT_RETRY_BASE_DELAY,
            retry_max_delay: DEFAULT_RETRY_MAX_DELAY,
            strict_exports: true,
        }
    }

    pub fn with_max_parallel(mut self, max: usize) -> Self {
        self.max_parallel = max.max(1);
        self
    }

    pub fn with_retry_policy(
        mut self,
        max_retry_attempts: u32,
        retry_base_delay: Duration,
        retry_max_delay: Duration,
    ) -> Self {
        self.max_retry_attempts = max_retry_attempts;
        self.retry_base_delay = retry_base_delay;
        self.retry_max_delay = retry_max_delay.max(retry_base_delay);
        self
    }

    pub fn with_export_contract(mut self, strict_exports: bool) -> Self {
        self.strict_exports = strict_exports;
        self
    }

    pub async fn execute(&self, dag: &mut ExecutionDag, ctx: &ExecutorContext) -> ExecutionResult {
        loop {
            let ready = dag.ready_nodes.clone();
            if ready.is_empty() {
                return self.resolve_no_ready_nodes(dag, ctx).await;
            }
            let batch: Vec<String> = ready.into_iter().take(self.max_parallel).collect();
            if let Some(waiting_result) = self.handle_wait_steps(dag, &batch, ctx).await {
                return waiting_result;
            }
            if let Some(result) = self.execute_batch(dag, batch, ctx).await {
                return result;
            }
        }
    }
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}
