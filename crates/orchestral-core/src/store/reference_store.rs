//! ReferenceStore - Historical artifact index trait

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use super::StoreError;
use crate::types::{StepId, TaskId};

/// Reference type for categorization.
///
/// This only describes content shape and must not encode storage location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceType {
    Document,
    Table,
    Code,
    Text,
    Image,
    Audio,
    Video,
    Binary,
    Custom(String),
}

/// Embedding index status for reference retrieval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingStatus {
    None,
    Queued,
    Indexed,
    Failed,
}

/// A reference to a historical artifact.
///
/// Reference is an index row (metadata + linkage), not the artifact body itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reference {
    /// Unique identifier
    pub id: String,
    /// Owning thread id.
    pub thread_id: String,
    /// Optional owning interaction id.
    #[serde(default)]
    pub interaction_id: Option<String>,
    /// Optional owning task id.
    #[serde(default)]
    pub task_id: Option<TaskId>,
    /// Optional owning step id.
    #[serde(default)]
    pub step_id: Option<StepId>,
    /// Optional managed file identifier. When set, file IO must go through file service.
    #[serde(default)]
    pub file_id: Option<String>,
    /// Type of reference
    pub ref_type: ReferenceType,
    /// Inline content payload (small previews, snippets, extracted text).
    #[serde(default)]
    pub content: Value,
    /// Optional media type (RFC 6838), e.g. application/pdf, image/png.
    #[serde(default)]
    pub mime_type: Option<String>,
    /// Optional user-facing file name.
    #[serde(default)]
    pub file_name: Option<String>,
    /// Optional byte size for file-like references.
    #[serde(default)]
    pub byte_size: Option<u64>,
    /// Source reference ids this artifact derives from.
    #[serde(default)]
    pub derived_from: Vec<String>,
    /// Searchable tag labels.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Arbitrary metadata
    #[serde(default)]
    pub metadata: HashMap<String, Value>,
    /// Embedding backend (e.g. qdrant, milvus, pgvector-service).
    #[serde(default)]
    pub embedding_backend: Option<String>,
    /// Embedding collection/index name.
    #[serde(default)]
    pub embedding_collection: Option<String>,
    /// Embedding key/document id in vector backend.
    #[serde(default)]
    pub embedding_key: Option<String>,
    /// Embedding model identifier.
    #[serde(default)]
    pub embedding_model: Option<String>,
    /// Embedding indexing status.
    #[serde(default)]
    pub embedding_status: Option<EmbeddingStatus>,
    /// Embedding version tag.
    #[serde(default)]
    pub embedding_version: Option<String>,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
}

impl Reference {
    /// Create a new reference.
    pub fn new(thread_id: impl Into<String>, ref_type: ReferenceType, content: Value) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            thread_id: thread_id.into(),
            interaction_id: None,
            task_id: None,
            step_id: None,
            file_id: None,
            ref_type,
            content,
            mime_type: None,
            file_name: None,
            byte_size: None,
            derived_from: Vec::new(),
            tags: Vec::new(),
            metadata: HashMap::new(),
            embedding_backend: None,
            embedding_collection: None,
            embedding_key: None,
            embedding_model: None,
            embedding_status: None,
            embedding_version: None,
            created_at: Utc::now(),
        }
    }

    /// Create a new reference with metadata.
    pub fn with_metadata(
        thread_id: impl Into<String>,
        ref_type: ReferenceType,
        content: Value,
        metadata: HashMap<String, Value>,
    ) -> Self {
        Self {
            metadata,
            ..Self::new(thread_id, ref_type, content)
        }
    }

    /// Attach linkage information.
    pub fn with_lineage(
        mut self,
        interaction_id: Option<String>,
        task_id: Option<TaskId>,
        step_id: Option<StepId>,
    ) -> Self {
        self.interaction_id = interaction_id;
        self.task_id = task_id;
        self.step_id = step_id;
        self
    }

    /// Attach managed file id.
    pub fn with_file_id(mut self, file_id: Option<String>) -> Self {
        self.file_id = file_id;
        self
    }

    /// Attach file/media metadata.
    pub fn with_media(
        mut self,
        mime_type: Option<String>,
        file_name: Option<String>,
        byte_size: Option<u64>,
    ) -> Self {
        self.mime_type = mime_type;
        self.file_name = file_name;
        self.byte_size = byte_size;
        self
    }

    /// Attach embedding index pointer.
    pub fn with_embedding_pointer(
        mut self,
        backend: Option<String>,
        collection: Option<String>,
        key: Option<String>,
        model: Option<String>,
        status: Option<EmbeddingStatus>,
        version: Option<String>,
    ) -> Self {
        self.embedding_backend = backend;
        self.embedding_collection = collection;
        self.embedding_key = key;
        self.embedding_model = model;
        self.embedding_status = status;
        self.embedding_version = version;
        self
    }

    /// Attach source links.
    pub fn with_derived_from(mut self, source_ids: Vec<String>) -> Self {
        self.derived_from = source_ids;
        self
    }

    /// Attach tags.
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }
}

/// Similarity hit for retrieval.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferenceMatch {
    pub reference: Reference,
    /// Similarity score in range [0, 1], higher is better.
    pub score: f32,
}

/// ReferenceStore trait - async interface for multiple backend implementations.
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

    /// Query most recent references scoped to a thread.
    async fn query_recent_by_thread(
        &self,
        thread_id: &str,
        limit: usize,
    ) -> Result<Vec<Reference>, StoreError> {
        let refs = self
            .query_recent(limit.saturating_mul(4).max(limit))
            .await?;
        Ok(refs
            .into_iter()
            .filter(|r| r.thread_id == thread_id)
            .take(limit)
            .collect())
    }

    /// Query references by type within a thread.
    async fn query_by_type_in_thread(
        &self,
        thread_id: &str,
        ref_type: &ReferenceType,
        limit: usize,
    ) -> Result<Vec<Reference>, StoreError> {
        let refs = self.query_by_type(ref_type).await?;
        Ok(refs
            .into_iter()
            .filter(|r| r.thread_id == thread_id)
            .take(limit)
            .collect())
    }

    /// Vector-search hook.
    ///
    /// Vector payload is expected to live outside Orchestral primary DB.
    async fn search_by_embedding(
        &self,
        _thread_id: &str,
        _embedding: &[f32],
        _limit: usize,
    ) -> Result<Vec<ReferenceMatch>, StoreError> {
        Ok(Vec::new())
    }

    /// Delete a reference by ID
    async fn delete(&self, id: &str) -> Result<bool, StoreError>;
}
