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

pub use orchestral_core::store::ThreadId;

fn default_scope() -> String {
    "user:anonymous".to_string()
}

/// Thread - a long-lived context where interactions take place
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Thread {
    /// Unique identifier
    pub id: ThreadId,
    /// Abstract ownership scope for access partitioning.
    #[serde(default = "default_scope")]
    pub scope: String,
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
            id: ThreadId::from(uuid::Uuid::new_v4().to_string()),
            scope: default_scope(),
            metadata: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Create a new thread with specific ID
    pub fn with_id(id: impl Into<ThreadId>) -> Self {
        let now = Utc::now();
        Self {
            id: id.into(),
            scope: default_scope(),
            metadata: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Create a new thread with metadata
    pub fn with_metadata(metadata: HashMap<String, Value>) -> Self {
        let now = Utc::now();
        Self {
            id: ThreadId::from(uuid::Uuid::new_v4().to_string()),
            scope: default_scope(),
            metadata,
            created_at: now,
            updated_at: now,
        }
    }

    /// Create a new thread in a specific scope.
    pub fn with_scope(scope: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: ThreadId::from(uuid::Uuid::new_v4().to_string()),
            scope: scope.into(),
            metadata: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Set thread scope.
    pub fn set_scope(&mut self, scope: impl Into<String>) {
        self.scope = scope.into();
        self.updated_at = Utc::now();
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
