//! ReferenceStore implementations

use async_trait::async_trait;
use redis::AsyncCommands;
use std::sync::RwLock;

use orchestral_core::store::{Reference, ReferenceStore, ReferenceType, StoreError};

const DEFAULT_IN_MEMORY_REFERENCE_LIMIT: usize = 10_000;

/// In-memory implementation for development and testing
pub struct InMemoryReferenceStore {
    references: RwLock<Vec<Reference>>,
    max_references: usize,
}

impl InMemoryReferenceStore {
    /// Create a new in-memory store
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

/// Redis implementation for durable reference storage.
pub struct RedisReferenceStore {
    client: redis::Client,
    key_prefix: String,
}

impl RedisReferenceStore {
    /// Create a new Redis reference store from a connection URL.
    pub fn new(connection_url: &str, key_prefix: impl Into<String>) -> Result<Self, StoreError> {
        let client = redis::Client::open(connection_url)
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        Ok(Self {
            client,
            key_prefix: key_prefix.into(),
        })
    }

    fn reference_key(&self, id: &str) -> String {
        format!("{}:reference:{}", self.key_prefix, id)
    }

    fn reference_ids_key(&self) -> String {
        format!("{}:reference:ids", self.key_prefix)
    }

    fn reference_type_key(&self, ref_type: &ReferenceType) -> String {
        format!(
            "{}:reference:type:{}",
            self.key_prefix,
            ref_type_label(ref_type)
        )
    }

    fn reference_recent_key(&self) -> String {
        format!("{}:reference:recent", self.key_prefix)
    }

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection, StoreError> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))
    }
}

#[async_trait]
impl ReferenceStore for RedisReferenceStore {
    async fn add(&self, reference: Reference) -> Result<(), StoreError> {
        let mut conn = self.connection().await?;
        let payload = serde_json::to_string(&reference)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        let id = reference.id.clone();
        let ref_key = self.reference_key(&id);
        let ids_key = self.reference_ids_key();
        let type_key = self.reference_type_key(&reference.ref_type);
        let recent_key = self.reference_recent_key();
        let score = reference.created_at.timestamp_millis();

        conn.set::<_, _, ()>(ref_key, payload)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        conn.sadd::<_, _, ()>(ids_key, &id)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        conn.sadd::<_, _, ()>(type_key, &id)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        conn.zadd::<_, _, _, ()>(recent_key, &id, score)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<Reference>, StoreError> {
        let mut conn = self.connection().await?;
        let payload: Option<String> = conn
            .get(self.reference_key(id))
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        payload
            .map(|s| serde_json::from_str(&s).map_err(|e| StoreError::Serialization(e.to_string())))
            .transpose()
    }

    async fn query_by_type(&self, ref_type: &ReferenceType) -> Result<Vec<Reference>, StoreError> {
        let mut conn = self.connection().await?;
        let ids: Vec<String> = conn
            .smembers(self.reference_type_key(ref_type))
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;

        let mut refs = Vec::new();
        for id in ids {
            if let Some(reference) = self.get(&id).await? {
                refs.push(reference);
            }
        }
        Ok(refs)
    }

    async fn query_recent(&self, limit: usize) -> Result<Vec<Reference>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut conn = self.connection().await?;
        let stop = (limit as isize) - 1;
        let ids: Vec<String> = conn
            .zrevrange(self.reference_recent_key(), 0, stop)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;

        let mut refs = Vec::new();
        for id in ids {
            if let Some(reference) = self.get(&id).await? {
                refs.push(reference);
            }
        }
        Ok(refs)
    }

    async fn delete(&self, id: &str) -> Result<bool, StoreError> {
        let existing = self.get(id).await?;
        let Some(reference) = existing else {
            return Ok(false);
        };

        let mut conn = self.connection().await?;
        let key = self.reference_key(id);
        let ids_key = self.reference_ids_key();
        let type_key = self.reference_type_key(&reference.ref_type);
        let recent_key = self.reference_recent_key();

        let deleted: u64 = conn
            .del(key)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        let _ids_removed: u64 = conn
            .srem(ids_key, id)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        let _type_removed: u64 = conn
            .srem(type_key, id)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        let _recent_removed: u64 = conn
            .zrem(recent_key, id)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        Ok(deleted > 0)
    }
}

fn ref_type_label(ref_type: &ReferenceType) -> String {
    match ref_type {
        ReferenceType::Image => "image".to_string(),
        ReferenceType::Document => "document".to_string(),
        ReferenceType::File => "file".to_string(),
        ReferenceType::Text => "text".to_string(),
        ReferenceType::Code => "code".to_string(),
        ReferenceType::Url => "url".to_string(),
        ReferenceType::Summary => "summary".to_string(),
        ReferenceType::Embedding => "embedding".to_string(),
        ReferenceType::Custom(label) => format!("custom:{}", label),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_in_memory_reference_store_evicts_oldest_when_limit_exceeded() {
        tokio_test::block_on(async {
            let store = InMemoryReferenceStore::with_max_references(2);

            let mut r1 = Reference::new(ReferenceType::Text, json!("r1"));
            r1.id = "r1".to_string();
            let mut r2 = Reference::new(ReferenceType::Text, json!("r2"));
            r2.id = "r2".to_string();
            let mut r3 = Reference::new(ReferenceType::Text, json!("r3"));
            r3.id = "r3".to_string();

            store.add(r1).await.expect("add r1");
            store.add(r2).await.expect("add r2");
            store.add(r3).await.expect("add r3");

            assert!(store.get("r1").await.expect("get r1").is_none());
            assert!(store.get("r2").await.expect("get r2").is_some());
            assert!(store.get("r3").await.expect("get r3").is_some());
        });
    }
}
