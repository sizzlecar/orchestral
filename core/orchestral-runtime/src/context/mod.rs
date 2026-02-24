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

use orchestral_core::store::{
    Event, EventStore, Reference, ReferenceStore, ReferenceType, StoreError,
};

/// Metadata key for summary reference ID
pub const META_SUMMARY_REF: &str = "orchestral.summary_ref";
/// Metadata key for source reference IDs
pub const META_SOURCE_IDS: &str = "orchestral.source_ids";
/// Metadata key for tags
pub const META_TAGS: &str = "orchestral.tags";

/// How to reference artifact content
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentSource {
    /// Inline text content
    InlineText { text: String },
    /// Reference by ID (stored in ReferenceStore)
    Reference { ref_id: String },
    /// External URL
    ExternalUrl { url: String },
}

/// Unified context artifact abstraction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextArtifact {
    pub id: String,
    pub ref_type: ReferenceType,
    pub source: ContentSource,
    #[serde(default)]
    pub summary_id: Option<String>,
    #[serde(default)]
    pub source_ids: Vec<String>,
    #[serde(default)]
    pub metadata: HashMap<String, Value>,
}

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
    pub include_references: bool,
    #[serde(default)]
    pub ref_type_filter: Option<Vec<ReferenceType>>,
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
            include_references: true,
            ref_type_filter: None,
            tags: Vec::new(),
        }
    }
}

/// Context builder configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextBuilderConfig {
    /// Max events to include (0 = all)
    pub history_limit: usize,
    /// Max references to include (0 = all)
    pub reference_limit: usize,
    /// Whether to include summary references when available
    pub include_summaries: bool,
}

impl Default for ContextBuilderConfig {
    fn default() -> Self {
        Self {
            history_limit: 50,
            reference_limit: 20,
            include_summaries: true,
        }
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

/// Result of embedding a reference artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddedReference {
    pub reference_id: String,
    pub model: String,
    pub vector: Vec<f32>,
}

/// Pluggable artifact embedding strategy.
#[async_trait]
pub trait ArtifactEmbedder: Send + Sync {
    async fn embed_reference(
        &self,
        reference: &Reference,
    ) -> Result<EmbeddedReference, ContextError>;
}

/// Context builder trait
#[async_trait]
pub trait ContextBuilder: Send + Sync {
    async fn build(&self, request: &ContextRequest) -> Result<ContextWindow, ContextError>;
}

/// Basic context builder (rule-based, no LLM summarization)
pub struct BasicContextBuilder {
    event_store: Arc<dyn EventStore>,
    reference_store: Arc<dyn ReferenceStore>,
    config: ContextBuilderConfig,
}

impl BasicContextBuilder {
    pub fn new(event_store: Arc<dyn EventStore>, reference_store: Arc<dyn ReferenceStore>) -> Self {
        Self {
            event_store,
            reference_store,
            config: ContextBuilderConfig::default(),
        }
    }

    pub fn with_config(
        event_store: Arc<dyn EventStore>,
        reference_store: Arc<dyn ReferenceStore>,
        config: ContextBuilderConfig,
    ) -> Self {
        Self {
            event_store,
            reference_store,
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
                    window.core.push(slice);
                }
            }
        }

        if request.include_references {
            let references = if self.config.reference_limit == 0 {
                self.reference_store
                    .query_recent_by_thread(&request.thread_id, usize::MAX)
                    .await?
            } else {
                self.reference_store
                    .query_recent_by_thread(&request.thread_id, self.config.reference_limit)
                    .await?
            };

            for reference in references {
                if !matches_ref_type(&reference, request.ref_type_filter.as_ref()) {
                    continue;
                }
                if !matches_tags(&reference, &request.tags) {
                    continue;
                }
                if let Some(slice) = reference_to_slice(&reference, self.config.include_summaries) {
                    window.optional.push(slice);
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

fn reference_to_slice(reference: &Reference, include_summary: bool) -> Option<ContextSlice> {
    let mut attachments = Vec::new();
    let content = if include_summary {
        if let Some(summary) = reference
            .metadata
            .get(META_SUMMARY_REF)
            .and_then(|v| v.as_str())
        {
            format!("summary_ref:{}", summary)
        } else {
            reference_content_as_string(reference)
        }
    } else {
        reference_content_as_string(reference)
    };

    match reference.ref_type {
        ReferenceType::Image
        | ReferenceType::Document
        | ReferenceType::Binary
        | ReferenceType::Code
        | ReferenceType::Table
        | ReferenceType::Audio
        | ReferenceType::Video
        | ReferenceType::Custom(_)
        | ReferenceType::Text => {
            attachments.push(reference.id.clone());
        }
    }

    Some(ContextSlice {
        role: "system".to_string(),
        content,
        attachments,
        weight: 0.5,
        metadata: reference.metadata.clone(),
        timestamp: Some(reference.created_at),
    })
}

fn reference_content_as_string(reference: &Reference) -> String {
    match reference.content.as_str() {
        Some(s) => s.to_string(),
        None => reference.content.to_string(),
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

fn matches_ref_type(reference: &Reference, filter: Option<&Vec<ReferenceType>>) -> bool {
    match filter {
        None => true,
        Some(list) => list.iter().any(|t| t == &reference.ref_type),
    }
}

fn matches_tags(reference: &Reference, tags: &[String]) -> bool {
    if tags.is_empty() {
        return true;
    }
    if !reference.tags.is_empty() {
        return tags.iter().all(|t| reference.tags.contains(t));
    }
    let ref_tags = reference
        .metadata
        .get(META_TAGS)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    tags.iter().all(|t| ref_tags.contains(t))
}
