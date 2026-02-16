use std::any::Any;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use orchestral_actions::ActionFactory;
use orchestral_config::{OrchestralConfig, RuntimeExtensionSpec};
use orchestral_spi::{
    ComponentRegistry, HookError, RuntimeBuildRequest, RuntimeHook, RuntimeHookContext,
    RuntimeHookEventEnvelope, SpiError,
};
use serde::Deserialize;
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

mod document_actions;

pub use document_actions::DocsAssistantActionFactory;

pub const EXTENSION_NAME: &str = "builtin.docs_assistant";
pub const CATALOG_COMPONENT_KEY: &str = "docs_assistant.catalog";

// SQL schema reserved for future Postgres-backed docs catalog support.
pub const POSTGRES_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS doc_sources (
  source_id TEXT PRIMARY KEY,
  thread_id TEXT NOT NULL,
  interaction_id TEXT NOT NULL,
  task_id TEXT,
  step_id TEXT,
  action TEXT,
  source_path TEXT NOT NULL,
  mime_type TEXT NOT NULL,
  byte_size BIGINT NOT NULL,
  char_count BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS doc_chunks (
  chunk_id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL REFERENCES doc_sources(source_id),
  chunk_index INT NOT NULL,
  content TEXT NOT NULL,
  token_hint INT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS doc_embeddings (
  embedding_id TEXT PRIMARY KEY,
  chunk_id TEXT NOT NULL REFERENCES doc_chunks(chunk_id),
  dim INT NOT NULL,
  vector_json JSONB NOT NULL,
  model TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"#;

#[derive(Debug, Clone, Deserialize)]
pub struct DocsAssistantOptions {
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,
    #[serde(default = "default_max_chunk_chars")]
    pub max_chunk_chars: usize,
    #[serde(default = "default_chunk_overlap")]
    pub chunk_overlap: usize,
    #[serde(default = "default_embedding_dim")]
    pub embedding_dim: usize,
    #[serde(default)]
    pub action_allowlist: Vec<String>,
    #[serde(default = "default_path_suffix_allowlist")]
    pub path_suffix_allowlist: Vec<String>,
}

fn default_queue_capacity() -> usize {
    64
}

fn default_max_chunk_chars() -> usize {
    900
}

fn default_chunk_overlap() -> usize {
    120
}

fn default_embedding_dim() -> usize {
    24
}

fn default_path_suffix_allowlist() -> Vec<String> {
    vec![
        ".md".to_string(),
        ".markdown".to_string(),
        ".txt".to_string(),
        ".pdf".to_string(),
        ".docx".to_string(),
        ".html".to_string(),
    ]
}

impl Default for DocsAssistantOptions {
    fn default() -> Self {
        Self {
            queue_capacity: default_queue_capacity(),
            max_chunk_chars: default_max_chunk_chars(),
            chunk_overlap: default_chunk_overlap(),
            embedding_dim: default_embedding_dim(),
            action_allowlist: Vec::new(),
            path_suffix_allowlist: default_path_suffix_allowlist(),
        }
    }
}

impl DocsAssistantOptions {
    fn normalize(mut self) -> Self {
        self.queue_capacity = self.queue_capacity.max(1);
        self.max_chunk_chars = self.max_chunk_chars.max(64);
        self.chunk_overlap = self
            .chunk_overlap
            .min(self.max_chunk_chars.saturating_sub(1));
        self.embedding_dim = self.embedding_dim.max(4);
        self.action_allowlist = self
            .action_allowlist
            .into_iter()
            .map(|v| v.trim().to_ascii_lowercase())
            .filter(|v| !v.is_empty())
            .collect();
        self.path_suffix_allowlist = self
            .path_suffix_allowlist
            .into_iter()
            .map(|v| v.trim().to_ascii_lowercase())
            .filter(|v| !v.is_empty())
            .collect();
        self
    }
}

#[derive(Debug, Clone)]
pub struct DocSourceRecord {
    pub source_id: String,
    pub thread_id: String,
    pub interaction_id: String,
    pub task_id: Option<String>,
    pub step_id: Option<String>,
    pub action: Option<String>,
    pub source_path: String,
    pub mime_type: String,
    pub byte_size: u64,
    pub char_count: usize,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct DocChunkRecord {
    pub chunk_id: String,
    pub source_id: String,
    pub chunk_index: usize,
    pub content: String,
    pub token_hint: usize,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct DocEmbeddingRecord {
    pub embedding_id: String,
    pub chunk_id: String,
    pub model: String,
    pub vector: Vec<f32>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct IngestionStats {
    pub sources: usize,
    pub chunks: usize,
    pub embeddings: usize,
}

#[derive(Default)]
pub struct InMemoryDocsCatalog {
    sources: RwLock<Vec<DocSourceRecord>>,
    chunks: RwLock<Vec<DocChunkRecord>>,
    embeddings: RwLock<Vec<DocEmbeddingRecord>>,
}

impl InMemoryDocsCatalog {
    pub async fn save_source(&self, source: DocSourceRecord) {
        self.sources.write().await.push(source);
    }

    pub async fn save_chunk(&self, chunk: DocChunkRecord) {
        self.chunks.write().await.push(chunk);
    }

    pub async fn save_embedding(&self, embedding: DocEmbeddingRecord) {
        self.embeddings.write().await.push(embedding);
    }

    pub async fn stats(&self) -> IngestionStats {
        IngestionStats {
            sources: self.sources.read().await.len(),
            chunks: self.chunks.read().await.len(),
            embeddings: self.embeddings.read().await.len(),
        }
    }
}

#[derive(Clone)]
struct WorkerHandle {
    sender: mpsc::Sender<IngestionJob>,
}

#[derive(Debug, Clone)]
struct IngestionJob {
    thread_id: String,
    interaction_id: String,
    task_id: Option<String>,
    step_id: Option<String>,
    action: Option<String>,
    source_path: String,
}

pub struct DocsAssistantExtension {
    catalog: OnceLock<Arc<InMemoryDocsCatalog>>,
    worker: OnceLock<WorkerHandle>,
}

impl DocsAssistantExtension {
    pub fn new() -> Self {
        Self {
            catalog: OnceLock::new(),
            worker: OnceLock::new(),
        }
    }

    fn catalog(&self) -> Arc<InMemoryDocsCatalog> {
        self.catalog
            .get_or_init(|| Arc::new(InMemoryDocsCatalog::default()))
            .clone()
    }

    fn ensure_worker(&self, options: DocsAssistantOptions) -> mpsc::Sender<IngestionJob> {
        let handle = self.worker.get_or_init(|| {
            let (sender, receiver) = mpsc::channel(options.queue_capacity);
            let catalog = self.catalog();
            tokio::spawn(async move {
                run_ingestion_worker(receiver, catalog, options).await;
            });
            WorkerHandle { sender }
        });
        handle.sender.clone()
    }

    pub async fn build_components(
        &self,
        _request: &RuntimeBuildRequest,
        _config: &OrchestralConfig,
        _spec: &RuntimeExtensionSpec,
    ) -> Result<ComponentRegistry, SpiError> {
        let mut registry = ComponentRegistry::new();
        let catalog = self.catalog();
        let component: Arc<dyn Any + Send + Sync> = catalog;
        registry.insert_named_component(CATALOG_COMPONENT_KEY, component);
        Ok(registry)
    }

    pub async fn build_hooks(
        &self,
        _request: &RuntimeBuildRequest,
        _config: &OrchestralConfig,
        spec: &RuntimeExtensionSpec,
    ) -> Result<Vec<Arc<dyn RuntimeHook>>, SpiError> {
        let options = parse_options(spec)?;
        let sender = self.ensure_worker(options.clone());
        Ok(vec![Arc::new(DocsAssistantHook { sender, options })])
    }

    pub async fn build_action_factories(
        &self,
        _request: &RuntimeBuildRequest,
        _config: &OrchestralConfig,
        _spec: &RuntimeExtensionSpec,
    ) -> Result<Vec<Arc<dyn ActionFactory>>, SpiError> {
        Ok(vec![Arc::new(DocsAssistantActionFactory::new())])
    }

    #[cfg(test)]
    async fn stats(&self) -> IngestionStats {
        self.catalog().stats().await
    }
}

impl Default for DocsAssistantExtension {
    fn default() -> Self {
        Self::new()
    }
}

fn parse_options(spec: &RuntimeExtensionSpec) -> Result<DocsAssistantOptions, SpiError> {
    if spec.options.is_null() {
        return Ok(DocsAssistantOptions::default().normalize());
    }
    serde_json::from_value::<DocsAssistantOptions>(spec.options.clone())
        .map(|v| v.normalize())
        .map_err(|err| {
            SpiError::InvalidBuildRequest(format!(
                "extension '{}' options decode failed: {}",
                spec.name, err
            ))
        })
}

struct DocsAssistantHook {
    sender: mpsc::Sender<IngestionJob>,
    options: DocsAssistantOptions,
}

#[async_trait]
impl RuntimeHook for DocsAssistantHook {
    fn id(&self) -> &'static str {
        "docs_assistant_hook"
    }

    async fn on_event(
        &self,
        event: &RuntimeHookEventEnvelope,
        context: &RuntimeHookContext,
    ) -> Result<(), HookError> {
        if event.event_type != "step.completed" {
            return Ok(());
        }

        let action = event
            .payload
            .get("action")
            .and_then(|v| v.as_str())
            .map(|v| v.to_ascii_lowercase());
        if !self.options.action_allowlist.is_empty() {
            let allowed = action
                .as_ref()
                .map(|a| self.options.action_allowlist.iter().any(|x| x == a))
                .unwrap_or(false);
            if !allowed {
                return Ok(());
            }
        }

        let Some(source_path) = extract_source_path(event) else {
            return Ok(());
        };
        if !is_supported_suffix(&source_path, &self.options.path_suffix_allowlist) {
            return Ok(());
        }

        let job = IngestionJob {
            thread_id: context.thread_id.to_string(),
            interaction_id: context.interaction_id.to_string(),
            task_id: context.task_id.as_ref().map(ToString::to_string),
            step_id: context.step_id.as_ref().map(ToString::to_string),
            action,
            source_path,
        };

        if let Err(err) = self.sender.try_send(job) {
            tracing::warn!(error = %err, "docs assistant queue is full; dropping ingestion job");
        }
        Ok(())
    }
}

fn extract_source_path(event: &RuntimeHookEventEnvelope) -> Option<String> {
    let metadata = event.payload.get("metadata")?;
    for key in [
        "path",
        "target_path",
        "output_path",
        "file_path",
        "source_path",
    ] {
        if let Some(path) = metadata.get(key).and_then(|v| v.as_str()) {
            let trimmed = path.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn is_supported_suffix(path: &str, allowlist: &[String]) -> bool {
    if allowlist.is_empty() {
        return true;
    }
    let lowercase = path.to_ascii_lowercase();
    allowlist.iter().any(|suffix| lowercase.ends_with(suffix))
}

async fn run_ingestion_worker(
    mut receiver: mpsc::Receiver<IngestionJob>,
    catalog: Arc<InMemoryDocsCatalog>,
    options: DocsAssistantOptions,
) {
    while let Some(job) = receiver.recv().await {
        if let Err(err) = process_job(job, catalog.clone(), &options).await {
            tracing::warn!(error = %err, "docs assistant ingestion job failed");
        }
    }
}

async fn process_job(
    job: IngestionJob,
    catalog: Arc<InMemoryDocsCatalog>,
    options: &DocsAssistantOptions,
) -> Result<(), String> {
    let parsed = parse_document(&job.source_path).await?;
    let chunks = chunk_document(
        &parsed.content,
        options.max_chunk_chars,
        options.chunk_overlap,
    );
    if chunks.is_empty() {
        return Ok(());
    }

    let source_id = Uuid::new_v4().to_string();
    catalog
        .save_source(DocSourceRecord {
            source_id: source_id.clone(),
            thread_id: job.thread_id,
            interaction_id: job.interaction_id,
            task_id: job.task_id,
            step_id: job.step_id,
            action: job.action,
            source_path: job.source_path,
            mime_type: parsed.mime_type,
            byte_size: parsed.byte_size,
            char_count: parsed.content.chars().count(),
            created_at: Utc::now(),
        })
        .await;

    for (idx, content) in chunks.into_iter().enumerate() {
        let chunk_id = Uuid::new_v4().to_string();
        catalog
            .save_chunk(DocChunkRecord {
                chunk_id: chunk_id.clone(),
                source_id: source_id.clone(),
                chunk_index: idx,
                token_hint: content.chars().count() / 4,
                content: content.clone(),
                created_at: Utc::now(),
            })
            .await;

        catalog
            .save_embedding(DocEmbeddingRecord {
                embedding_id: Uuid::new_v4().to_string(),
                chunk_id,
                model: "placeholder-hash-v1".to_string(),
                vector: placeholder_embed(&content, options.embedding_dim),
                created_at: Utc::now(),
            })
            .await;
    }

    Ok(())
}

struct ParsedDocument {
    content: String,
    mime_type: String,
    byte_size: u64,
}

async fn parse_document(path: &str) -> Result<ParsedDocument, String> {
    let file_meta = tokio::fs::metadata(path)
        .await
        .map_err(|err| format!("read source metadata '{}' failed: {}", path, err))?;
    let byte_size = file_meta.len();
    let (content, mime_type) = document_actions::parse_document_markdown(path, false).await?;
    let content = if content.trim().is_empty() {
        format!("(empty-or-binary) source={}", path)
    } else {
        content
    };
    Ok(ParsedDocument {
        content,
        mime_type,
        byte_size,
    })
}

fn chunk_document(content: &str, max_chars: usize, overlap: usize) -> Vec<String> {
    if content.trim().is_empty() {
        return Vec::new();
    }

    let chars: Vec<char> = content.chars().collect();
    if chars.is_empty() {
        return Vec::new();
    }
    if chars.len() <= max_chars {
        return vec![content.to_string()];
    }

    let mut chunks = Vec::new();
    let step = max_chars.saturating_sub(overlap).max(1);
    let mut start = 0usize;
    while start < chars.len() {
        let end = (start + max_chars).min(chars.len());
        let chunk: String = chars[start..end].iter().collect();
        let trimmed = chunk.trim();
        if !trimmed.is_empty() {
            chunks.push(trimmed.to_string());
        }
        if end == chars.len() {
            break;
        }
        start = start.saturating_add(step);
    }
    chunks
}

fn placeholder_embed(text: &str, dim: usize) -> Vec<f32> {
    let mut vec = vec![0.0_f32; dim];
    if text.is_empty() {
        return vec;
    }

    for (idx, byte) in text.as_bytes().iter().enumerate() {
        let slot = idx % dim;
        vec[slot] += (*byte as f32) / 255.0;
    }
    let norm = text.len() as f32;
    for value in &mut vec {
        *value /= norm.max(1.0);
    }
    vec
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_spec(options: serde_json::Value) -> RuntimeExtensionSpec {
        RuntimeExtensionSpec {
            name: EXTENSION_NAME.to_string(),
            enabled: true,
            targets: vec![],
            options,
        }
    }

    fn sample_request() -> RuntimeBuildRequest {
        RuntimeBuildRequest {
            meta: orchestral_spi::SpiMeta::runtime_defaults("0.1.0"),
            config_path: "configs/orchestral.yaml".to_string(),
            profile: Some("test".to_string()),
            options: serde_json::Map::new(),
        }
    }

    fn sample_context() -> RuntimeHookContext {
        RuntimeHookContext {
            thread_id: "thread-1".into(),
            interaction_id: "interaction-1".into(),
            task_id: Some("task-1".into()),
            step_id: Some("step-1".into()),
            action: Some("file_write".to_string()),
            message: None,
            metadata: serde_json::Value::Null,
            extensions: serde_json::Map::new(),
        }
    }

    fn sample_completed_event(path: &str, action: &str) -> RuntimeHookEventEnvelope {
        RuntimeHookEventEnvelope {
            meta: orchestral_spi::SpiMeta::runtime_defaults("0.1.0"),
            event_type: "step.completed".to_string(),
            event_version: "1.0.0".to_string(),
            occurred_at_unix_ms: Utc::now().timestamp_millis(),
            payload: serde_json::json!({
                "action": action,
                "metadata": {
                    "path": path
                }
            }),
            extensions: serde_json::Map::new(),
        }
    }

    #[tokio::test]
    async fn test_docs_hook_ingests_step_completed_path() {
        let extension = DocsAssistantExtension::new();
        let file_path = std::env::temp_dir().join(format!("docs-assistant-{}.md", Uuid::new_v4()));
        std::fs::write(&file_path, "hello\n\nworld\n\nfrom docs assistant").expect("write");

        let hooks = extension
            .build_hooks(
                &sample_request(),
                &OrchestralConfig::default(),
                &test_spec(serde_json::json!({
                    "max_chunk_chars": 10,
                    "chunk_overlap": 2,
                    "embedding_dim": 8
                })),
            )
            .await
            .expect("hooks");
        hooks[0]
            .on_event(
                &sample_completed_event(file_path.to_string_lossy().as_ref(), "file_write"),
                &sample_context(),
            )
            .await
            .expect("dispatch");

        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        let stats = extension.stats().await;
        assert_eq!(stats.sources, 1);
        assert!(stats.chunks >= 1);
        assert_eq!(stats.chunks, stats.embeddings);

        let _ = std::fs::remove_file(file_path);
    }

    #[tokio::test]
    async fn test_docs_hook_ignores_event_without_path() {
        let extension = DocsAssistantExtension::new();
        let hooks = extension
            .build_hooks(
                &sample_request(),
                &OrchestralConfig::default(),
                &test_spec(serde_json::Value::Null),
            )
            .await
            .expect("hooks");
        let event = RuntimeHookEventEnvelope {
            meta: orchestral_spi::SpiMeta::runtime_defaults("0.1.0"),
            event_type: "step.completed".to_string(),
            event_version: "1.0.0".to_string(),
            occurred_at_unix_ms: Utc::now().timestamp_millis(),
            payload: serde_json::json!({ "action": "file_write", "metadata": {} }),
            extensions: serde_json::Map::new(),
        };
        hooks[0]
            .on_event(&event, &sample_context())
            .await
            .expect("dispatch");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let stats = extension.stats().await;
        assert_eq!(stats.sources, 0);
    }

    #[tokio::test]
    async fn test_docs_hook_respects_action_allowlist() {
        let extension = DocsAssistantExtension::new();
        let file_path = std::env::temp_dir().join(format!("docs-assistant-{}.md", Uuid::new_v4()));
        std::fs::write(&file_path, "allowlist").expect("write");

        let hooks = extension
            .build_hooks(
                &sample_request(),
                &OrchestralConfig::default(),
                &test_spec(serde_json::json!({
                    "action_allowlist": ["doc_convert"]
                })),
            )
            .await
            .expect("hooks");
        hooks[0]
            .on_event(
                &sample_completed_event(file_path.to_string_lossy().as_ref(), "file_write"),
                &sample_context(),
            )
            .await
            .expect("dispatch");

        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        let stats = extension.stats().await;
        assert_eq!(stats.sources, 0);

        let _ = std::fs::remove_file(file_path);
    }
}
