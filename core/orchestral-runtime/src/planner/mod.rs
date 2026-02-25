//! Planner implementations for Orchestral.
//!
//! This crate provides LLM client implementations for different providers:
//! - OpenAI (and OpenAI-compatible APIs)
//! - Google Gemini
//!
//! Use `LlmClientFactory` to create clients from configuration.

mod factory;
mod gemini;
mod llm;

pub use factory::{
    build_client_from_backend, DefaultLlmClientFactory, LlmBuildError, LlmClientFactory,
    LlmInvocationConfig,
};
pub use gemini::{GeminiClient, GeminiClientConfig};
pub use llm::{
    HttpLlmClient, HttpLlmClientConfig, LlmClient, LlmError, LlmPlanner, LlmPlannerConfig,
    LlmRequest, MockLlmClient, StreamChunkCallback,
};
