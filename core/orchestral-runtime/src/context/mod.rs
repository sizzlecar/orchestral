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
    /// Characters per token estimate. Varies by backend and language mix.
    /// English/code ~3.5, Chinese ~1.8, mixed ~2.5. Default: 3.0.
    #[serde(default = "default_chars_per_token")]
    pub chars_per_token: f32,
    /// Max tokens for a single tool output before truncation. Default: 2000.
    #[serde(default = "default_max_tool_output_tokens")]
    pub max_tool_output_tokens: usize,
}

fn default_chars_per_token() -> f32 {
    3.0
}
fn default_max_tool_output_tokens() -> usize {
    2000
}

impl TokenBudget {
    pub fn new(max_tokens: usize) -> Self {
        Self {
            max_tokens,
            used_tokens: 0,
            chars_per_token: default_chars_per_token(),
            max_tool_output_tokens: default_max_tool_output_tokens(),
        }
    }

    pub fn with_chars_per_token(mut self, chars_per_token: f32) -> Self {
        self.chars_per_token = chars_per_token.max(0.5);
        self
    }

    pub fn remaining(&self) -> usize {
        self.max_tokens.saturating_sub(self.used_tokens)
    }

    /// Estimate tokens for a string using the configured chars_per_token ratio.
    pub fn estimate_tokens(&self, text: &str) -> usize {
        let chars = text.chars().count();
        std::cmp::max(1, (chars as f32 / self.chars_per_token).ceil() as usize)
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
            let mut slices: Vec<ContextSlice> = events.iter().filter_map(event_to_slice).collect();

            // Layer 1: Truncate large individual tool outputs.
            let max_tool_tokens = request.budget.max_tool_output_tokens;
            let cpt = request.budget.chars_per_token;
            for slice in &mut slices {
                if slice.role == "system" {
                    slice.content = truncate_tool_output(&slice.content, max_tool_tokens, cpt);
                }
            }

            // Layer 2: FIFO eviction of old system/tool slices when over budget.
            // Keep first 2 + last N slices intact, replace old system slices with traces.
            let total_tokens: usize = slices
                .iter()
                .map(|s| estimate_slice_tokens(s, &request.budget))
                .sum();
            if total_tokens > request.budget.max_tokens && slices.len() > 4 {
                let head_keep = 2.min(slices.len());
                let tail_keep = (slices.len() / 3).max(2).min(slices.len() - head_keep);
                let middle_start = head_keep;
                let middle_end = slices.len() - tail_keep;

                for slice in slices.iter_mut().take(middle_end).skip(middle_start) {
                    if slice.role == "system" {
                        let trace = compact_system_trace(&slice.content);
                        slice.content = trace;
                        slice.weight = 0.1;
                    } else if slice.role == "assistant" && slice.content.chars().count() > 200 {
                        let chars: String = slice.content.chars().take(120).collect();
                        slice.content = format!("{}... (truncated)", chars);
                        slice.weight = 0.3;
                    }
                }
            }

            // Push slices with budget enforcement.
            for slice in slices {
                push_slice_with_budget(&mut window, slice, SliceBucket::Core);
            }
        }

        Ok(window)
    }
}

/// Compress a system trace/tool output to a one-line summary for FIFO eviction.
fn compact_system_trace(content: &str) -> String {
    let first_line = content.lines().next().unwrap_or(content);
    let total_chars = content.chars().count();
    if total_chars <= 80 {
        return content.to_string();
    }
    let prefix: String = first_line.chars().take(60).collect();
    format!("[{}... ({} chars)]", prefix, total_chars)
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
    let tokens = estimate_slice_tokens(&slice, &window.budget);
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

fn estimate_slice_tokens(slice: &ContextSlice, budget: &TokenBudget) -> usize {
    let attachment_chars: usize = slice.attachments.iter().map(|s| s.chars().count()).sum();
    let total_chars = slice.content.chars().count() + attachment_chars + slice.role.chars().count();
    std::cmp::max(
        1,
        (total_chars as f32 / budget.chars_per_token).ceil() as usize,
    )
}

/// Truncate a tool output string to fit within max_tokens, preserving head + tail.
fn truncate_tool_output(content: &str, max_tokens: usize, chars_per_token: f32) -> String {
    let max_chars = (max_tokens as f32 * chars_per_token) as usize;
    let total = content.chars().count();
    if total <= max_chars {
        return content.to_string();
    }
    let keep = max_chars.saturating_sub(60); // reserve room for truncation marker
    let head_chars = keep * 2 / 5;
    let tail_chars = keep - head_chars;
    let head: String = content.chars().take(head_chars).collect();
    let tail: String = content.chars().skip(total - tail_chars).collect();
    let dropped = total - head_chars - tail_chars;
    format!(
        "{}\n... ({} chars truncated, {} total) ...\n{}",
        head, dropped, total, tail
    )
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
                .append(Event::user_input("thread-1", "i-1", json!("hello world")))
                .await
                .expect("append event 1");
            builder
                .event_store
                .append(Event::user_input("thread-1", "i-1", json!("second msg")))
                .await
                .expect("append event 2");

            let mut request = ContextRequest::new("thread-1");
            // Give enough budget to hold both (each ~5-6 tokens at 3.0 cpt)
            request.budget = TokenBudget::new(20);
            let window = builder.build(&request).await.expect("build context");
            assert_eq!(window.core.len(), 2);
            assert_eq!(window.deferred.len(), 0);
        });
    }

    #[test]
    fn test_context_budget_defers_core_when_overflow() {
        tokio_test::block_on(async {
            let builder = make_builder();
            builder
                .event_store
                .append(Event::user_input("thread-2", "i-2", json!("short")))
                .await
                .expect("append event 1");
            builder
                .event_store
                .append(Event::user_input(
                    "thread-2",
                    "i-2",
                    json!("this is a much longer message that should exceed budget"),
                ))
                .await
                .expect("append event 2");

            let mut request = ContextRequest::new("thread-2");
            // Budget for ~5 tokens — first event fits, second overflows
            request.budget = TokenBudget::new(5);
            let window = builder.build(&request).await.expect("build context");
            assert_eq!(window.core.len(), 1);
            assert_eq!(window.deferred.len(), 1);
        });
    }

    #[test]
    fn test_token_budget_estimate_tokens() {
        let budget = TokenBudget::new(100).with_chars_per_token(3.0);
        assert_eq!(budget.estimate_tokens("hello"), 2); // 5 chars / 3.0 = 1.67 → 2
        assert_eq!(budget.estimate_tokens("你好世界"), 2); // 4 chars / 3.0 = 1.33 → 2
        assert_eq!(budget.estimate_tokens(""), 1); // min 1
    }

    #[test]
    fn test_truncate_tool_output_preserves_short() {
        let short = "hello world";
        assert_eq!(truncate_tool_output(short, 100, 3.0), short);
    }

    #[test]
    fn test_truncate_tool_output_head_tail() {
        let long: String = (0..1000).map(|i| format!("line{}\n", i)).collect();
        let truncated = truncate_tool_output(&long, 50, 3.0); // 50*3=150 chars max
        assert!(truncated.contains("... ("));
        assert!(truncated.contains("chars truncated"));
        assert!(truncated.chars().count() < long.chars().count());
        // Head should contain "line0"
        assert!(truncated.starts_with("line0"));
    }

    #[test]
    fn test_compact_system_trace() {
        let short = "trace:info ok";
        assert_eq!(compact_system_trace(short), short);

        let long = "trace:info ".to_string() + &"x".repeat(200);
        let compacted = compact_system_trace(&long);
        assert!(compacted.starts_with("[trace:info"));
        assert!(compacted.contains("chars)"));
        assert!(compacted.len() < long.len());
    }

    #[test]
    fn test_fifo_eviction_compresses_old_system_slices() {
        tokio_test::block_on(async {
            let builder = make_builder();
            // Create a conversation: user, system(large), user, system(large), user, assistant
            let large_output = "x".repeat(500);
            builder
                .event_store
                .append(Event::user_input("t", "i", json!("first question")))
                .await
                .unwrap();
            builder
                .event_store
                .append(Event::trace("t", "info", json!(large_output)))
                .await
                .unwrap();
            builder
                .event_store
                .append(Event::user_input("t", "i", json!("second question")))
                .await
                .unwrap();
            builder
                .event_store
                .append(Event::trace("t", "info", json!(large_output)))
                .await
                .unwrap();
            builder
                .event_store
                .append(Event::user_input("t", "i", json!("third question")))
                .await
                .unwrap();
            builder
                .event_store
                .append(Event::user_input("t", "i", json!("fourth question")))
                .await
                .unwrap();

            let mut request = ContextRequest::new("t");
            // Budget that can't hold all content but can hold compressed version
            request.budget = TokenBudget::new(100);
            let window = builder.build(&request).await.expect("build context");

            // Should have compressed the middle system traces
            let system_slices: Vec<_> = window.core.iter().filter(|s| s.role == "system").collect();
            for s in &system_slices {
                // Compressed system slices should be short
                assert!(
                    s.content.chars().count() < 200,
                    "system slice should be compacted: {}",
                    &s.content[..80.min(s.content.len())]
                );
            }
        });
    }
}
