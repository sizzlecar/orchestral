//! ReferenceStore implementations

use async_trait::async_trait;
use redis::AsyncCommands;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::sync::RwLock;

use orchestral_core::store::{
    EmbeddingStatus, Reference, ReferenceMatch, ReferenceStore, ReferenceType, StoreError,
};

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
        // Vector payloads are intentionally not stored in the primary DB/store.
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

    fn thread_recent_key(&self, thread_id: &str) -> String {
        format!("{}:reference:thread:{}:recent", self.key_prefix, thread_id)
    }

    fn thread_type_key(&self, thread_id: &str, ref_type: &ReferenceType) -> String {
        format!(
            "{}:reference:thread:{}:type:{}",
            self.key_prefix,
            thread_id,
            ref_type_label(ref_type)
        )
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
        let thread_recent_key = self.thread_recent_key(&reference.thread_id);
        let thread_type_key = self.thread_type_key(&reference.thread_id, &reference.ref_type);
        let score = reference.created_at.timestamp_millis();

        conn.set::<_, _, ()>(ref_key, payload)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        conn.sadd::<_, _, ()>(ids_key, &id)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        conn.zadd::<_, _, _, ()>(type_key, &id, score)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        conn.zadd::<_, _, _, ()>(recent_key, &id, score)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        conn.zadd::<_, _, _, ()>(thread_recent_key, &id, score)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        conn.zadd::<_, _, _, ()>(thread_type_key, &id, score)
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
            .zrevrange(self.reference_type_key(ref_type), 0, -1)
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

    async fn query_recent_by_thread(
        &self,
        thread_id: &str,
        limit: usize,
    ) -> Result<Vec<Reference>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut conn = self.connection().await?;
        let stop = (limit as isize) - 1;
        let ids: Vec<String> = conn
            .zrevrange(self.thread_recent_key(thread_id), 0, stop)
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

    async fn query_by_type_in_thread(
        &self,
        thread_id: &str,
        ref_type: &ReferenceType,
        limit: usize,
    ) -> Result<Vec<Reference>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut conn = self.connection().await?;
        let stop = (limit as isize) - 1;
        let ids: Vec<String> = conn
            .zrevrange(self.thread_type_key(thread_id, ref_type), 0, stop)
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

    async fn search_by_embedding(
        &self,
        _thread_id: &str,
        _embedding: &[f32],
        _limit: usize,
    ) -> Result<Vec<ReferenceMatch>, StoreError> {
        Ok(Vec::new())
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
        let thread_recent_key = self.thread_recent_key(&reference.thread_id);
        let thread_type_key = self.thread_type_key(&reference.thread_id, &reference.ref_type);

        let deleted: u64 = conn
            .del(key)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        let _ids_removed: u64 = conn
            .srem(ids_key, id)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        let _type_removed: u64 = conn
            .zrem(type_key, id)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        let _recent_removed: u64 = conn
            .zrem(recent_key, id)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        let _thread_recent_removed: u64 = conn
            .zrem(thread_recent_key, id)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        let _thread_type_removed: u64 = conn
            .zrem(thread_type_key, id)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        Ok(deleted > 0)
    }
}

/// PostgreSQL implementation for durable reference index persistence.
pub struct PostgresReferenceStore {
    pool: PgPool,
    table_name: String,
}

impl PostgresReferenceStore {
    /// Create a new PostgreSQL reference store from a connection URL.
    pub async fn new(
        connection_url: &str,
        table_prefix: impl Into<String>,
    ) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(connection_url)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        let prefix = normalize_table_prefix(&table_prefix.into());
        let table_name = format!("{}_references", prefix);
        let this = Self { pool, table_name };
        this.init_schema().await?;
        Ok(this)
    }

    async fn init_schema(&self) -> Result<(), StoreError> {
        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                interaction_id TEXT NULL,
                task_id TEXT NULL,
                step_id TEXT NULL,
                file_id TEXT NULL,
                ref_type TEXT NOT NULL,
                content JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                mime_type TEXT NULL,
                file_name TEXT NULL,
                byte_size BIGINT NULL,
                derived_from JSONB NOT NULL DEFAULT '[]'::jsonb,
                tags JSONB NOT NULL DEFAULT '[]'::jsonb,
                metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                embedding_backend TEXT NULL,
                embedding_collection TEXT NULL,
                embedding_key TEXT NULL,
                embedding_model TEXT NULL,
                embedding_status TEXT NULL,
                embedding_version TEXT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                reference_json JSONB NOT NULL
            )",
            self.table_name
        );
        sqlx::query(&create_table)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        let alter_file_id = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS file_id TEXT NULL",
            self.table_name
        );
        sqlx::query(&alter_file_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;

        let idx_thread_created = format!(
            "CREATE INDEX IF NOT EXISTS {0}_thread_created_idx ON {1} (thread_id, created_at DESC)",
            self.table_name, self.table_name
        );
        let idx_thread_type_created = format!(
            "CREATE INDEX IF NOT EXISTS {0}_thread_type_created_idx ON {1} (thread_id, ref_type, created_at DESC)",
            self.table_name, self.table_name
        );
        let idx_task_step = format!(
            "CREATE INDEX IF NOT EXISTS {0}_task_step_idx ON {1} (task_id, step_id)",
            self.table_name, self.table_name
        );
        let idx_file_id = format!(
            "CREATE INDEX IF NOT EXISTS {0}_file_id_idx ON {1} (file_id)",
            self.table_name, self.table_name
        );
        let idx_tags = format!(
            "CREATE INDEX IF NOT EXISTS {0}_tags_idx ON {1} USING GIN (tags)",
            self.table_name, self.table_name
        );
        let idx_metadata = format!(
            "CREATE INDEX IF NOT EXISTS {0}_metadata_idx ON {1} USING GIN (metadata)",
            self.table_name, self.table_name
        );
        sqlx::query(&idx_thread_created)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        sqlx::query(&idx_thread_type_created)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        sqlx::query(&idx_task_step)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        sqlx::query(&idx_file_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        sqlx::query(&idx_tags)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        sqlx::query(&idx_metadata)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl ReferenceStore for PostgresReferenceStore {
    async fn add(&self, reference: Reference) -> Result<(), StoreError> {
        let ref_json = serde_json::to_value(&reference)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        let ref_type = ref_type_label(&reference.ref_type);
        let derived_from = serde_json::to_value(&reference.derived_from)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        let tags = serde_json::to_value(&reference.tags)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        let metadata = serde_json::to_value(&reference.metadata)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        let status = reference
            .embedding_status
            .as_ref()
            .map(embedding_status_label);
        let byte_size = match reference.byte_size {
            Some(v) => Some(i64::try_from(v).map_err(|_| {
                StoreError::Serialization("byte_size exceeds i64 range".to_string())
            })?),
            None => None,
        };

        let sql = format!(
            "INSERT INTO {} (
                id, thread_id, interaction_id, task_id, step_id, file_id,
                ref_type, content,
                mime_type, file_name, byte_size,
                derived_from, tags, metadata,
                embedding_backend, embedding_collection, embedding_key, embedding_model, embedding_status, embedding_version,
                created_at, reference_json
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8,
                $9, $10, $11,
                $12, $13, $14,
                $15, $16, $17, $18, $19, $20,
                $21, $22
            )
            ON CONFLICT (id) DO UPDATE SET
                thread_id = EXCLUDED.thread_id,
                interaction_id = EXCLUDED.interaction_id,
                task_id = EXCLUDED.task_id,
                step_id = EXCLUDED.step_id,
                file_id = EXCLUDED.file_id,
                ref_type = EXCLUDED.ref_type,
                content = EXCLUDED.content,
                mime_type = EXCLUDED.mime_type,
                file_name = EXCLUDED.file_name,
                byte_size = EXCLUDED.byte_size,
                derived_from = EXCLUDED.derived_from,
                tags = EXCLUDED.tags,
                metadata = EXCLUDED.metadata,
                embedding_backend = EXCLUDED.embedding_backend,
                embedding_collection = EXCLUDED.embedding_collection,
                embedding_key = EXCLUDED.embedding_key,
                embedding_model = EXCLUDED.embedding_model,
                embedding_status = EXCLUDED.embedding_status,
                embedding_version = EXCLUDED.embedding_version,
                created_at = EXCLUDED.created_at,
                reference_json = EXCLUDED.reference_json",
            self.table_name
        );
        sqlx::query(&sql)
            .bind(&reference.id)
            .bind(&reference.thread_id)
            .bind(&reference.interaction_id)
            .bind(reference.task_id.as_ref().map(|id| id.to_string()))
            .bind(reference.step_id.as_ref().map(|id| id.to_string()))
            .bind(&reference.file_id)
            .bind(ref_type)
            .bind(&reference.content)
            .bind(&reference.mime_type)
            .bind(&reference.file_name)
            .bind(byte_size)
            .bind(derived_from)
            .bind(tags)
            .bind(metadata)
            .bind(&reference.embedding_backend)
            .bind(&reference.embedding_collection)
            .bind(&reference.embedding_key)
            .bind(&reference.embedding_model)
            .bind(status)
            .bind(&reference.embedding_version)
            .bind(reference.created_at)
            .bind(ref_json)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<Reference>, StoreError> {
        let sql = format!(
            "SELECT reference_json FROM {} WHERE id = $1",
            self.table_name
        );
        let row = sqlx::query(&sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let value: serde_json::Value = row
            .try_get("reference_json")
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        let reference: Reference =
            serde_json::from_value(value).map_err(|e| StoreError::Serialization(e.to_string()))?;
        Ok(Some(reference))
    }

    async fn query_by_type(&self, ref_type: &ReferenceType) -> Result<Vec<Reference>, StoreError> {
        let sql = format!(
            "SELECT reference_json FROM {} WHERE ref_type = $1 ORDER BY created_at DESC",
            self.table_name
        );
        let rows = sqlx::query(&sql)
            .bind(ref_type_label(ref_type))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        decode_reference_rows(rows)
    }

    async fn query_recent(&self, limit: usize) -> Result<Vec<Reference>, StoreError> {
        let limit_i64 = usize_to_i64(limit)?;
        let sql = format!(
            "SELECT reference_json FROM {} ORDER BY created_at DESC LIMIT $1",
            self.table_name
        );
        let rows = sqlx::query(&sql)
            .bind(limit_i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        decode_reference_rows(rows)
    }

    async fn query_recent_by_thread(
        &self,
        thread_id: &str,
        limit: usize,
    ) -> Result<Vec<Reference>, StoreError> {
        let limit_i64 = usize_to_i64(limit)?;
        let sql = format!(
            "SELECT reference_json FROM {} WHERE thread_id = $1 ORDER BY created_at DESC LIMIT $2",
            self.table_name
        );
        let rows = sqlx::query(&sql)
            .bind(thread_id)
            .bind(limit_i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        decode_reference_rows(rows)
    }

    async fn query_by_type_in_thread(
        &self,
        thread_id: &str,
        ref_type: &ReferenceType,
        limit: usize,
    ) -> Result<Vec<Reference>, StoreError> {
        let limit_i64 = usize_to_i64(limit)?;
        let sql = format!(
            "SELECT reference_json FROM {} WHERE thread_id = $1 AND ref_type = $2 ORDER BY created_at DESC LIMIT $3",
            self.table_name
        );
        let rows = sqlx::query(&sql)
            .bind(thread_id)
            .bind(ref_type_label(ref_type))
            .bind(limit_i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        decode_reference_rows(rows)
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
        let sql = format!("DELETE FROM {} WHERE id = $1", self.table_name);
        let result = sqlx::query(&sql)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        Ok(result.rows_affected() > 0)
    }
}

fn decode_reference_rows(rows: Vec<sqlx::postgres::PgRow>) -> Result<Vec<Reference>, StoreError> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let value: serde_json::Value = row
            .try_get("reference_json")
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        let reference: Reference =
            serde_json::from_value(value).map_err(|e| StoreError::Serialization(e.to_string()))?;
        out.push(reference);
    }
    Ok(out)
}

fn normalize_table_prefix(raw: &str) -> String {
    let candidate = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string();
    if candidate.is_empty() {
        "orchestral".to_string()
    } else {
        candidate
    }
}

fn usize_to_i64(value: usize) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::Internal("limit exceeds i64 range".to_string()))
}

fn ref_type_label(ref_type: &ReferenceType) -> String {
    match ref_type {
        ReferenceType::Document => "document".to_string(),
        ReferenceType::Table => "table".to_string(),
        ReferenceType::Code => "code".to_string(),
        ReferenceType::Text => "text".to_string(),
        ReferenceType::Image => "image".to_string(),
        ReferenceType::Audio => "audio".to_string(),
        ReferenceType::Video => "video".to_string(),
        ReferenceType::Binary => "binary".to_string(),
        ReferenceType::Custom(label) => format!("custom:{}", label),
    }
}

fn embedding_status_label(status: &EmbeddingStatus) -> String {
    match status {
        EmbeddingStatus::None => "none".to_string(),
        EmbeddingStatus::Queued => "queued".to_string(),
        EmbeddingStatus::Indexed => "indexed".to_string(),
        EmbeddingStatus::Failed => "failed".to_string(),
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

            let mut r1 = Reference::new("thread-1", ReferenceType::Text, json!("r1"));
            r1.id = "r1".to_string();
            let mut r2 = Reference::new("thread-1", ReferenceType::Text, json!("r2"));
            r2.id = "r2".to_string();
            let mut r3 = Reference::new("thread-1", ReferenceType::Text, json!("r3"));
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
