//! Thread - Context world
//!
//! Thread represents a long-lived context where interactions take place.
//! It replaces the concept of "Session" with a more general abstraction.
//!
//! Use cases:
//! - Chat session
//! - Ticket/Issue
//! - IDE Workspace
//! - Automation flow instance

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Type alias for Thread ID
pub type ThreadId = String;

/// Thread - a long-lived context where interactions take place
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Thread {
    /// Unique identifier
    pub id: ThreadId,
    /// Arbitrary metadata
    #[serde(default)]
    pub metadata: HashMap<String, Value>,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Last activity timestamp
    pub updated_at: DateTime<Utc>,
}

impl Thread {
    /// Create a new thread
    pub fn new() -> Self {
        let now = Utc::now();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            metadata: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Create a new thread with specific ID
    pub fn with_id(id: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: id.into(),
            metadata: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Create a new thread with metadata
    pub fn with_metadata(metadata: HashMap<String, Value>) -> Self {
        let now = Utc::now();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            metadata,
            created_at: now,
            updated_at: now,
        }
    }

    /// Set metadata value
    pub fn set_metadata(&mut self, key: impl Into<String>, value: Value) {
        self.metadata.insert(key.into(), value);
        self.updated_at = Utc::now();
    }

    /// Get metadata value
    pub fn get_metadata(&self, key: &str) -> Option<&Value> {
        self.metadata.get(key)
    }

    /// Remove metadata value
    pub fn remove_metadata(&mut self, key: &str) -> Option<Value> {
        let result = self.metadata.remove(key);
        if result.is_some() {
            self.updated_at = Utc::now();
        }
        result
    }

    /// Touch the thread (update last activity timestamp)
    pub fn touch(&mut self) {
        self.updated_at = Utc::now();
    }
}

impl Default for Thread {
    fn default() -> Self {
        Self::new()
    }
}
