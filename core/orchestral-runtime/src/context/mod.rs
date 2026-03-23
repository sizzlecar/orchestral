//! # Orchestral Context
//!
//! Context abstraction and assembly layer for Orchestral.
//! Provides a unified view over events and artifacts (text/image/file/etc.).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use orchestral_core::store::{Event, EventStore, StoreError};

/// A single context slice included in a context window
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextSlice {
    pub role: String,
    pub content: String,
    #[serde(default)]
    pub attachments: Vec<String>,
    #[serde(default)]
    pub weight: f32,
    #[serde(default)]
    pub metadata: HashMap<String, Value>,
    #[serde(default)]
    pub timestamp: Option<DateTime<Utc>>,
}

/// Token budget for context assembly
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenBudget {
    pub max_tokens: usize,
    pub used_tokens: usize,
}

impl TokenBudget {
    pub fn new(max_tokens: usize) -> Self {
        Self {
            max_tokens,
            used_tokens: 0,
        }
    }

    pub fn remaining(&self) -> usize {
        self.max_tokens.saturating_sub(self.used_tokens)
    }
}

impl Default for TokenBudget {
    fn default() -> Self {
        Self::new(4096)
    }
}

/// Assembled context window
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextWindow {
    pub core: Vec<ContextSlice>,
    pub optional: Vec<ContextSlice>,
    pub deferred: Vec<ContextSlice>,
    pub budget: TokenBudget,
}

impl ContextWindow {
    pub fn new(budget: TokenBudget) -> Self {
        Self {
            core: Vec::new(),
            optional: Vec::new(),
            deferred: Vec::new(),
            budget,
        }
    }
}

/// Context request input
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextRequest {
    pub thread_id: String,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub interaction_id: Option<String>,
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    pub budget: TokenBudget,
    #[serde(default)]
    pub include_history: bool,
    #[serde(default)]
    pub tags: Vec<String>,
}

impl ContextRequest {
    pub fn new(thread_id: impl Into<String>) -> Self {
        Self {
            thread_id: thread_id.into(),
            task_id: None,
            interaction_id: None,
            query: None,
            budget: TokenBudget::default(),
            include_history: true,
            tags: Vec::new(),
        }
    }
}

/// Context builder configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextBuilderConfig {
    /// Max events to include (0 = all)
    pub history_limit: usize,
}

impl Default for ContextBuilderConfig {
    fn default() -> Self {
        Self { history_limit: 50 }
    }
}

/// Context builder errors
#[derive(Debug, Error)]
pub enum ContextError {
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
}

/// Thread-level summary artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadSummary {
    pub thread_id: String,
    pub summary_text: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    pub last_event_id: i64,
    #[serde(default)]
    pub metadata: HashMap<String, Value>,
    pub updated_at: DateTime<Utc>,
}

/// Pluggable thread summary strategy.
#[async_trait]
pub trait ThreadSummarizer: Send + Sync {
    async fn summarize_thread(
        &self,
        request: &ContextRequest,
    ) -> Result<ThreadSummary, ContextError>;
}

/// Context builder trait
#[async_trait]
pub trait ContextBuilder: Send + Sync {
    async fn build(&self, request: &ContextRequest) -> Result<ContextWindow, ContextError>;
}

/// Basic context builder (rule-based, no LLM summarization)
pub struct BasicContextBuilder {
    event_store: Arc<dyn EventStore>,
    config: ContextBuilderConfig,
}

impl BasicContextBuilder {
    pub fn new(event_store: Arc<dyn EventStore>) -> Self {
        Self {
            event_store,
            config: ContextBuilderConfig::default(),
        }
    }

    pub fn with_config(event_store: Arc<dyn EventStore>, config: ContextBuilderConfig) -> Self {
        Self {
            event_store,
            config,
        }
    }
}

#[async_trait]
impl ContextBuilder for BasicContextBuilder {
    async fn build(&self, request: &ContextRequest) -> Result<ContextWindow, ContextError> {
        if request.thread_id.is_empty() {
            return Err(ContextError::InvalidRequest(
                "thread_id must not be empty".to_string(),
            ));
        }

        let mut window = ContextWindow::new(request.budget.clone());

        if request.include_history {
            let mut events = if self.config.history_limit == 0 {
                self.event_store.query_by_thread(&request.thread_id).await?
            } else {
                self.event_store
                    .query_by_thread_with_limit(&request.thread_id, self.config.history_limit)
                    .await?
            };

            events.sort_by_key(|a| a.timestamp());
            for event in events {
                if let Some(slice) = event_to_slice(&event) {
                    push_slice_with_budget(&mut window, slice, SliceBucket::Core);
                }
            }
        }

        Ok(window)
    }
}

fn event_to_slice(event: &Event) -> Option<ContextSlice> {
    let timestamp = Some(event.timestamp());
    match event {
        Event::UserInput { payload, .. } => Some(ContextSlice {
            role: "user".to_string(),
            content: payload_to_string(payload),
            attachments: Vec::new(),
            weight: 1.0,
            metadata: HashMap::new(),
            timestamp,
        }),
        Event::AssistantOutput { payload, .. } => Some(ContextSlice {
            role: "assistant".to_string(),
            content: payload_to_string(payload),
            attachments: Vec::new(),
            weight: 1.0,
            metadata: HashMap::new(),
            timestamp,
        }),
        Event::ExternalEvent { kind, payload, .. } => Some(ContextSlice {
            role: "system".to_string(),
            content: format!("external:{} {}", kind, payload_to_string(payload)),
            attachments: Vec::new(),
            weight: 0.6,
            metadata: HashMap::new(),
            timestamp,
        }),
        Event::SystemTrace { level, payload, .. } => Some(ContextSlice {
            role: "system".to_string(),
            content: format!("trace:{} {}", level, payload_to_string(payload)),
            attachments: Vec::new(),
            weight: 0.4,
            metadata: HashMap::new(),
            timestamp,
        }),
        Event::Artifact { reference_id, .. } => Some(ContextSlice {
            role: "system".to_string(),
            content: format!("artifact:{}", reference_id),
            attachments: vec![reference_id.clone()],
            weight: 0.5,
            metadata: HashMap::new(),
            timestamp,
        }),
    }
}

fn payload_to_string(payload: &Value) -> String {
    if let Some(s) = payload.as_str() {
        return s.to_string();
    }
    for key in ["content", "message", "text"] {
        if let Some(s) = payload.get(key).and_then(|v| v.as_str()) {
            return s.to_string();
        }
    }
    payload.to_string()
}

#[derive(Copy, Clone)]
enum SliceBucket {
    Core,
    #[allow(dead_code)]
    Optional,
}

fn push_slice_with_budget(window: &mut ContextWindow, slice: ContextSlice, bucket: SliceBucket) {
    let tokens = estimate_slice_tokens(&slice);
    let remaining = window.budget.remaining();
    if tokens <= remaining {
        window.budget.used_tokens = window.budget.used_tokens.saturating_add(tokens);
        match bucket {
            SliceBucket::Core => window.core.push(slice),
            SliceBucket::Optional => window.optional.push(slice),
        }
    } else {
        window.deferred.push(slice);
    }
}

fn estimate_slice_tokens(slice: &ContextSlice) -> usize {
    let attachment_chars: usize = slice.attachments.iter().map(|s| s.chars().count()).sum();
    // Rough estimate for budget packing; avoids pulling in tokenizer deps.
    let total_chars = slice.content.chars().count() + attachment_chars + slice.role.chars().count();
    std::cmp::max(1, total_chars.div_ceil(4))
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::store::{Event, InMemoryEventStore};
    use serde_json::json;

    fn make_builder() -> BasicContextBuilder {
        let event_store = Arc::new(InMemoryEventStore::new());
        BasicContextBuilder::new(event_store)
    }

    #[test]
    fn test_context_budget_keeps_core_when_fit() {
        tokio_test::block_on(async {
            let builder = make_builder();
            builder
                .event_store
                .append(Event::user_input(
                    "thread-1",
                    "i-1",
                    json!("12345678901234567890"),
                ))
                .await
                .expect("append event 1");
            builder
                .event_store
                .append(Event::user_input(
                    "thread-1",
                    "i-1",
                    json!("abcdefghijabcdefghij"),
                ))
                .await
                .expect("append event 2");

            let mut request = ContextRequest::new("thread-1");
            request.budget = TokenBudget::new(12);
            let window = builder.build(&request).await.expect("build context");
            assert_eq!(window.core.len(), 2);
            assert_eq!(window.deferred.len(), 0);
            assert_eq!(window.budget.used_tokens, 12);
        });
    }

    #[test]
    fn test_context_budget_defers_core_when_overflow() {
        tokio_test::block_on(async {
            let builder = make_builder();
            builder
                .event_store
                .append(Event::user_input(
                    "thread-2",
                    "i-2",
                    json!("12345678901234567890"),
                ))
                .await
                .expect("append event 1");
            builder
                .event_store
                .append(Event::user_input(
                    "thread-2",
                    "i-2",
                    json!("abcdefghijklmnopqrstuvwxyz"),
                ))
                .await
                .expect("append event 2");

            let mut request = ContextRequest::new("thread-2");
            request.budget = TokenBudget::new(8);

            let window = builder.build(&request).await.expect("build context");
            assert_eq!(window.core.len(), 1);
            assert_eq!(window.deferred.len(), 1);
        });
    }
}
