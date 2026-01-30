//! Planner module
//!
//! The Planner is responsible for:
//! - Understanding user intent
//! - Selecting appropriate actions
//! - Building dependency relationships
//!
//! The Planner does NOT handle:
//! - Parameter expansion details
//! - Parallel/retry strategies
//! - Runtime error recovery

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use thiserror::Error;

use crate::action::ActionMeta;
use crate::store::ReferenceStore;
use crate::types::{Intent, Plan};

/// Planner errors
#[derive(Debug, Error)]
pub enum PlanError {
    #[error("Failed to understand intent: {0}")]
    IntentUnderstanding(String),

    #[error("No suitable actions found for intent")]
    NoSuitableActions,

    #[error("Failed to generate plan: {0}")]
    Generation(String),

    #[error("LLM error: {0}")]
    LlmError(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

/// Planner trait - generates execution plans from user intent
///
/// Implementations can use different LLM backends or planning strategies.
#[async_trait]
pub trait Planner: Send + Sync {
    /// Generate a plan from user intent
    async fn plan(&self, intent: &Intent, context: &PlannerContext) -> Result<Plan, PlanError>;
}

/// Context provided to the planner
pub struct PlannerContext {
    /// Available actions and their metadata
    pub available_actions: Vec<ActionMeta>,
    /// Conversation/interaction history
    pub history: Vec<HistoryItem>,
    /// Reference store for querying historical artifacts and user preferences
    /// Read-only access
    pub reference_store: Arc<dyn ReferenceStore>,
}

impl PlannerContext {
    /// Create a new planner context
    pub fn new(
        available_actions: Vec<ActionMeta>,
        reference_store: Arc<dyn ReferenceStore>,
    ) -> Self {
        Self {
            available_actions,
            history: Vec::new(),
            reference_store,
        }
    }

    /// Create context with history
    pub fn with_history(
        available_actions: Vec<ActionMeta>,
        history: Vec<HistoryItem>,
        reference_store: Arc<dyn ReferenceStore>,
    ) -> Self {
        Self {
            available_actions,
            history,
            reference_store,
        }
    }

    /// Add a history item
    pub fn add_history(&mut self, item: HistoryItem) {
        self.history.push(item);
    }

    /// Get action by name
    pub fn get_action(&self, name: &str) -> Option<&ActionMeta> {
        self.available_actions.iter().find(|a| a.name == name)
    }
}

/// A single item in the conversation history
#[derive(Debug, Clone)]
pub struct HistoryItem {
    /// Role (e.g., "user", "assistant", "system")
    pub role: String,
    /// Content of the message
    pub content: String,
    /// Timestamp
    pub timestamp: DateTime<Utc>,
}

impl HistoryItem {
    /// Create a new history item
    pub fn new(role: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: role.into(),
            content: content.into(),
            timestamp: Utc::now(),
        }
    }

    /// Create a user message
    pub fn user(content: impl Into<String>) -> Self {
        Self::new("user", content)
    }

    /// Create an assistant message
    pub fn assistant(content: impl Into<String>) -> Self {
        Self::new("assistant", content)
    }

    /// Create a system message
    pub fn system(content: impl Into<String>) -> Self {
        Self::new("system", content)
    }
}
