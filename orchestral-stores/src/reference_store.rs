//! ReferenceStore implementations

use async_trait::async_trait;
use std::sync::RwLock;

use orchestral_core::store::{Reference, ReferenceStore, ReferenceType, StoreError};

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
