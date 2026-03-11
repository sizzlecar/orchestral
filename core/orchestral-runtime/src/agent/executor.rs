use crate::planner::LlmClient;

#[derive(Debug, Clone)]
pub struct LlmAgentExecutorConfig {
    pub model: String,
    pub temperature: f32,
}

impl Default for LlmAgentExecutorConfig {
    fn default() -> Self {
        Self {
            model: "anthropic/claude-sonnet-4.5".to_string(),
            temperature: 0.2,
        }
    }
}

pub struct LlmAgentExecutor<C: LlmClient> {
    pub(super) client: C,
    pub(super) config: LlmAgentExecutorConfig,
}

impl<C: LlmClient> LlmAgentExecutor<C> {
    pub fn new(client: C, config: LlmAgentExecutorConfig) -> Self {
        Self { client, config }
    }
}
