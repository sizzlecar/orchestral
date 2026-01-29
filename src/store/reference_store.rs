//! ReferenceStore - Historical artifact storage

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::RwLock;

use super::StoreError;

/// Reference type for categorization
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceType {
    Image,
    Document,
    Text,
    Code,
    Url,
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

/// In-memory implementation for development and testing
pub struct InMemoryReferenceStore {
    references: RwLock<Vec<Reference>>,
}

impl InMemoryReferenceStore {
    /// Create a new in-memory store
    pub fn new() -> Self {
        Self {
            references: RwLock::new(Vec::new()),
        }
    }
}

impl Default for InMemoryReferenceStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ReferenceStore for InMemoryReferenceStore {
    async fn add(&self, reference: Reference) -> Result<(), StoreError> {
        let mut refs = self
            .references
            .write()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        refs.push(reference);
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<Reference>, StoreError> {
        let refs = self
            .references
            .read()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(refs.iter().find(|r| r.id == id).cloned())
    }

    async fn query_by_type(&self, ref_type: &ReferenceType) -> Result<Vec<Reference>, StoreError> {
        let refs = self
            .references
            .read()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(refs
            .iter()
            .filter(|r| &r.ref_type == ref_type)
            .cloned()
            .collect())
    }

    async fn query_recent(&self, limit: usize) -> Result<Vec<Reference>, StoreError> {
        let refs = self
            .references
            .read()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let mut sorted: Vec<_> = refs.iter().cloned().collect();
        sorted.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(sorted.into_iter().take(limit).collect())
    }

    async fn delete(&self, id: &str) -> Result<bool, StoreError> {
        let mut refs = self
            .references
            .write()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let len_before = refs.len();
        refs.retain(|r| r.id != id);
        Ok(refs.len() < len_before)
    }
}
