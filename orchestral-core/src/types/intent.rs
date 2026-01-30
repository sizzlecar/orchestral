//! Intent type definitions
//!
//! Intent represents a user's high-level goal description in the current context.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// User intent - the first-class input of the system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Intent {
    /// Unique identifier for this intent
    pub id: String,
    /// The actual content/description of what the user wants to achieve
    pub content: String,
    /// Optional context information
    #[serde(default)]
    pub context: Option<IntentContext>,
}

/// Additional context for an intent
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IntentContext {
    /// Associated conversation ID (for multi-turn interactions)
    #[serde(default)]
    pub conversation_id: Option<String>,
    /// Previous task ID (for continuation/follow-up)
    #[serde(default)]
    pub previous_task_id: Option<String>,
    /// Arbitrary metadata
    #[serde(default)]
    pub metadata: HashMap<String, Value>,
}

impl Intent {
    /// Create a new intent with just content
    pub fn new(content: impl Into<String>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            content: content.into(),
            context: None,
        }
    }

    /// Create a new intent with context
    pub fn with_context(content: impl Into<String>, context: IntentContext) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            content: content.into(),
            context: Some(context),
        }
    }
}
