//! ReferenceStore in-memory implementation.

use async_trait::async_trait;
use std::sync::RwLock;

use orchestral_core::store::{
    Reference, ReferenceMatch, ReferenceStore, ReferenceType, StoreError,
};

const DEFAULT_IN_MEMORY_REFERENCE_LIMIT: usize = 10_000;

/// In-memory implementation for development and testing.
pub struct InMemoryReferenceStore {
    references: RwLock<Vec<Reference>>,
    max_references: usize,
}

impl InMemoryReferenceStore {
    /// Create a new in-memory store.
    pub fn new() -> Self {
        Self::with_max_references(DEFAULT_IN_MEMORY_REFERENCE_LIMIT)
    }

    /// Create a new in-memory store with a hard capacity limit.
    pub fn with_max_references(max_references: usize) -> Self {
        Self {
            references: RwLock::new(Vec::new()),
            max_references: max_references.max(1),
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
        if refs.len() >= self.max_references {
            let overflow = refs
                .len()
                .saturating_add(1)
                .saturating_sub(self.max_references);
            if overflow > 0 {
                refs.drain(0..overflow);
            }
        }
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

    async fn query_recent_by_thread(
        &self,
        thread_id: &str,
        limit: usize,
    ) -> Result<Vec<Reference>, StoreError> {
        let refs = self
            .references
            .read()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let mut sorted: Vec<_> = refs
            .iter()
            .filter(|r| r.thread_id == thread_id)
            .cloned()
            .collect();
        sorted.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(sorted.into_iter().take(limit).collect())
    }

    async fn query_by_type_in_thread(
        &self,
        thread_id: &str,
        ref_type: &ReferenceType,
        limit: usize,
    ) -> Result<Vec<Reference>, StoreError> {
        let refs = self
            .references
            .read()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let mut sorted: Vec<_> = refs
            .iter()
            .filter(|r| r.thread_id == thread_id && &r.ref_type == ref_type)
            .cloned()
            .collect();
        sorted.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(sorted.into_iter().take(limit).collect())
    }

    async fn search_by_embedding(
        &self,
        _thread_id: &str,
        _embedding: &[f32],
        _limit: usize,
    ) -> Result<Vec<ReferenceMatch>, StoreError> {
        Ok(Vec::new())
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_in_memory_reference_store_limit() {
        tokio_test::block_on(async {
            let store = InMemoryReferenceStore::with_max_references(2);

            let mut r1 = Reference::new("thread-1", ReferenceType::Text, json!("r1"));
            r1.id = "r1".to_string();
            let mut r2 = Reference::new("thread-1", ReferenceType::Text, json!("r2"));
            r2.id = "r2".to_string();
            let mut r3 = Reference::new("thread-1", ReferenceType::Text, json!("r3"));
            r3.id = "r3".to_string();

            store.add(r1).await.unwrap();
            store.add(r2).await.unwrap();
            store.add(r3).await.unwrap();

            assert!(store.get("r1").await.unwrap().is_none());
            assert!(store.get("r2").await.unwrap().is_some());
            assert!(store.get("r3").await.unwrap().is_some());
        });
    }
}
