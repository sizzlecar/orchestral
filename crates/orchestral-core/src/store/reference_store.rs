//! ReferenceStore - Historical artifact storage trait

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use super::StoreError;

/// Reference type for categorization
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceType {
    Image,
    Document,
    File,
    Text,
    Code,
    Url,
    Summary,
    Embedding,
    Custom(String),
}

/// A reference to a historical artifact
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reference {
    /// Unique identifier
    pub id: String,
    /// Type of reference
    pub ref_type: ReferenceType,
    /// Content (can be actual data or a pointer/URL)
    pub content: Value,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Arbitrary metadata
    #[serde(default)]
    pub metadata: HashMap<String, Value>,
}

impl Reference {
    /// Create a new reference
    pub fn new(ref_type: ReferenceType, content: Value) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            ref_type,
            content,
            created_at: Utc::now(),
            metadata: HashMap::new(),
        }
    }

    /// Create a new reference with metadata
    pub fn with_metadata(
        ref_type: ReferenceType,
        content: Value,
        metadata: HashMap<String, Value>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            ref_type,
            content,
            created_at: Utc::now(),
            metadata,
        }
    }
}

/// ReferenceStore trait - async interface for multiple backend implementations
///
/// Designed as async trait to support:
/// - Vector databases
/// - Relational databases
/// - File systems
/// - Remote services
#[async_trait]
pub trait ReferenceStore: Send + Sync {
    /// Add a new reference
    async fn add(&self, reference: Reference) -> Result<(), StoreError>;

    /// Get a reference by ID
    async fn get(&self, id: &str) -> Result<Option<Reference>, StoreError>;

    /// Query references by type
    async fn query_by_type(&self, ref_type: &ReferenceType) -> Result<Vec<Reference>, StoreError>;

    /// Query most recent references
    async fn query_recent(&self, limit: usize) -> Result<Vec<Reference>, StoreError>;

    /// Delete a reference by ID
    async fn delete(&self, id: &str) -> Result<bool, StoreError>;
}
