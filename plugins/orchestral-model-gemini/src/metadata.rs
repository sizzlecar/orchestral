//! Optional capacity discovery. Vertex PublisherModel currently omits token
//! limits, so public Google endpoints may fall back to an exact catalog entry.

use super::*;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const NATIVE_TIMEOUT: Duration = Duration::from_secs(5);
// Catalogs are larger than a single model record. This work runs in the
// background for the CLI and must tolerate ordinary download latency.
const CATALOG_TIMEOUT: Duration = Duration::from_secs(30);
const CACHE_TTL_SECONDS: u64 = 300;
const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(super) struct ResolvedMetadata {
    pub context_tokens: u64,
    pub output_tokens: Option<u64>,
    pub source: String,
}

impl ResolvedMetadata {
    pub fn description(&self) -> Value {
        json!({
            "context_tokens": self.context_tokens,
            "output_tokens": self.output_tokens,
            "source": self.source,
        })
    }
}

pub(super) fn public_google_endpoint(endpoint: &str) -> bool {
    reqwest::Url::parse(endpoint).is_ok_and(|url| {
        url.scheme() == "https"
            && url.host_str().is_some_and(|host| {
                host == "generativelanguage.googleapis.com"
                    || host == "aiplatform.googleapis.com"
                    || host.ends_with("-aiplatform.googleapis.com")
            })
    })
}

fn native_url(backend: &GeminiModelBackend) -> Option<String> {
    let mut url = reqwest::Url::parse(&backend.config.endpoint).ok()?;
    let model = backend
        .config
        .model
        .strip_prefix("models/")
        .unwrap_or(&backend.config.model);
    if public_google_endpoint(&backend.config.endpoint) && url.path().contains("/publishers/google")
    {
        let version = url.path().trim_start_matches('/').split('/').next()?;
        url.set_path(&format!("/{version}/publishers/google/models/{model}"));
        Some(url.to_string())
    } else {
        Some(format!(
            "{}/models/{model}",
            backend.config.endpoint.trim_end_matches('/')
        ))
    }
}

fn positive(value: Option<&Value>) -> Option<u64> {
    value.and_then(Value::as_u64).filter(|value| *value > 0)
}

fn native_metadata(value: &Value, source: String) -> Option<ResolvedMetadata> {
    Some(ResolvedMetadata {
        context_tokens: positive(value.get("inputTokenLimit"))?,
        output_tokens: positive(value.get("outputTokenLimit")),
        source,
    })
}

fn catalog_metadata(
    value: &Value,
    provider: &str,
    model: &str,
    source: &str,
) -> Option<ResolvedMetadata> {
    let limit = value
        .get(provider)?
        .get("models")?
        .get(model)?
        .get("limit")?;
    let context = positive(limit.get("context"));
    let input = positive(limit.get("input"));
    Some(ResolvedMetadata {
        // An input ceiling may be smaller than a shared context window. A Host
        // reservation must fit both; never add input/output limits together.
        context_tokens: match (context, input) {
            (Some(context), Some(input)) => context.min(input),
            (Some(limit), None) | (None, Some(limit)) => limit,
            (None, None) => return None,
        },
        output_tokens: positive(limit.get("output")),
        source: format!("{source} ({provider})"),
    })
}

async fn response_json(request: RequestBuilder) -> Option<Value> {
    let mut response = request.send().await.ok()?.error_for_status().ok()?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        return None;
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.ok()? {
        if bytes.len().checked_add(chunk.len())? > MAX_RESPONSE_BYTES {
            return None;
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).ok()
}

fn cache_path(backend: &GeminiModelBackend) -> Option<PathBuf> {
    let identity = serde_json::to_vec(&json!({
        "endpoint": backend.config.endpoint,
        "model": backend.config.model,
        "catalog": backend.catalog_url,
    }))
    .ok()?;
    Some(
        backend
            .metadata_cache
            .as_ref()?
            .join(format!("{}.json", Digest::sha256(identity))),
    )
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

async fn cached(path: &std::path::Path) -> Option<(ResolvedMetadata, bool)> {
    let bytes = tokio::fs::read(path).await.ok()?;
    if bytes.len() > 8192 {
        return None;
    }
    let value: Value = serde_json::from_slice(&bytes).ok()?;
    if value.get("version")?.as_u64()? != 1 {
        return None;
    }
    let metadata = value.get("metadata")?;
    let metadata = ResolvedMetadata {
        context_tokens: positive(metadata.get("context_tokens"))?,
        output_tokens: positive(metadata.get("output_tokens")),
        source: metadata.get("source")?.as_str()?.to_owned(),
    };
    let fresh = now().checked_sub(value.get("fetched_at")?.as_u64()?)? < CACHE_TTL_SECONDS;
    Some((metadata, fresh))
}

async fn persist(path: &std::path::Path, metadata: &ResolvedMetadata) -> Option<()> {
    tokio::fs::create_dir_all(path.parent()?).await.ok()?;
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_nanos();
    let temporary = path.with_extension(format!("{}-{suffix}.tmp", std::process::id()));
    let bytes = serde_json::to_vec(&json!({
        "version": 1, "fetched_at": now(), "metadata": metadata.description(),
    }))
    .ok()?;
    tokio::fs::write(&temporary, bytes).await.ok()?;
    if tokio::fs::rename(&temporary, path).await.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
        return None;
    }
    Some(())
}

pub(super) async fn discover(backend: &GeminiModelBackend) -> Option<ResolvedMetadata> {
    let path = cache_path(backend);
    let cached = match &path {
        Some(path) => cached(path).await,
        None => None,
    };
    if let Some((metadata, true)) = &cached {
        return Some(metadata.clone());
    }
    let native = tokio::time::timeout(NATIVE_TIMEOUT, async {
        let url = native_url(backend)?;
        let request = backend
            .config
            .authentication
            .apply(backend.client.get(&url), &CancellationToken::new())
            .await
            .ok()?;
        native_metadata(
            &response_json(request).await?,
            "Google models API".to_owned(),
        )
    })
    .await
    .ok()
    .flatten();
    let resolved = match native {
        Some(value) => Some(value),
        None => tokio::time::timeout(CATALOG_TIMEOUT, async {
            let url = backend.catalog_url.as_ref()?;
            // This request deliberately has no inference-service credentials.
            let catalog = response_json(
                backend
                    .client
                    .get(url)
                    .header("User-Agent", "orchestral-model-metadata/1"),
            )
            .await?;
            let provider = if backend.config.endpoint.contains("/publishers/google") {
                "google-vertex"
            } else {
                "google"
            };
            let model = backend
                .config
                .model
                .strip_prefix("models/")
                .unwrap_or(&backend.config.model);
            catalog_metadata(&catalog, provider, model, url)
        })
        .await
        .ok()
        .flatten(),
    };
    if let Some(metadata) = &resolved {
        if let Some(path) = &path {
            persist(path, metadata).await;
        }
    }
    resolved.or_else(|| cached.map(|(metadata, _)| metadata))
}

fn bundled(backend: &GeminiModelBackend) -> Option<ResolvedMetadata> {
    if !public_google_endpoint(&backend.config.endpoint)
        || backend.catalog_url.as_deref() != Some("https://models.dev/api.json")
    {
        return None;
    }
    static CATALOG: std::sync::OnceLock<Value> = std::sync::OnceLock::new();
    let catalog = CATALOG.get_or_init(|| {
        serde_json::from_str(include_str!("metadata/catalog.json"))
            .expect("bundled model catalog must be valid JSON")
    });
    let provider = if backend.config.endpoint.contains("/publishers/google") {
        "google-vertex"
    } else {
        "google"
    };
    let model = backend
        .config
        .model
        .strip_prefix("models/")
        .unwrap_or(&backend.config.model);
    let source = format!(
        "bundled models.dev snapshot {}",
        catalog.pointer("/_meta/fetched_at")?.as_str()?
    );
    catalog_metadata(catalog, provider, model, &source)
}

pub(super) async fn local_snapshot_and_refresh(
    backend: &GeminiModelBackend,
) -> Option<ResolvedMetadata> {
    let existing = match cache_path(backend) {
        Some(path) => cached(&path).await,
        None => None,
    };
    let snapshot = existing
        .as_ref()
        .map(|(value, _)| value.clone())
        .or_else(|| bundled(backend));
    // Without a persistent cache there is no future consumer of a refresh.
    if backend.metadata_cache.is_some() && !matches!(existing, Some((_, true))) {
        let worker = GeminiModelBackend {
            client: backend.client.clone(),
            config: backend.config.clone(),
            metadata: tokio::sync::OnceCell::new(),
            metadata_cache: backend.metadata_cache.clone(),
            metadata_discovery: true,
            background_metadata_refresh: false,
            include_thoughts: backend.include_thoughts,
            catalog_url: backend.catalog_url.clone(),
        };
        tokio::spawn(async move {
            discover(&worker).await;
        });
    }
    snapshot
}

#[cfg(test)]
mod tests;
