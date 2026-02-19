use std::sync::Arc;

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use aws_types::region::Region;
use bytes::Bytes;
use futures_util::StreamExt;

use orchestral_config::{OrchestralConfig, StoreSpec};
use orchestral_core::io::{BlobHead, BlobIoError, BlobStore, BlobStream};
use orchestral_files::{
    BlobMode, BlobServiceConfig, FileService, LocalBlobStoreConfig, S3BlobClient,
    S3BlobStoreConfig, S3GetObjectResult, S3PutObjectResult,
};
use orchestral_spi::{
    ComponentRegistry, RuntimeBuildRequest, RuntimeComponentFactory, SpiError, StoreBundle,
};
use orchestral_stores::{InMemoryEventStore, InMemoryReferenceStore, InMemoryTaskStore};
use orchestral_stores_backends::{
    PostgresEventStore, PostgresReferenceStore, PostgresTaskStore, RedisEventStore,
    RedisReferenceStore, RedisTaskStore, SqliteEventStore, SqliteReferenceStore, SqliteTaskStore,
};

const DEFAULT_SQLITE_STORES_URL: &str = "sqlite://orchestral-runtime.db";

#[derive(Debug, Clone, Default)]
pub struct InfraFactoryOptions {
    pub skip_blob: bool,
}

/// Config-driven infrastructure component factory.
///
/// This factory is responsible for provisioning stores/blob backends only.
/// Runtime extensions should not override these infra components.
pub struct InfraComponentFactory {
    config: Arc<OrchestralConfig>,
    options: InfraFactoryOptions,
}

impl InfraComponentFactory {
    pub fn new(config: Arc<OrchestralConfig>) -> Self {
        Self {
            config,
            options: InfraFactoryOptions::default(),
        }
    }

    pub fn with_options(config: Arc<OrchestralConfig>, options: InfraFactoryOptions) -> Self {
        Self { config, options }
    }
}

#[async_trait]
impl RuntimeComponentFactory for InfraComponentFactory {
    async fn build(&self, _request: &RuntimeBuildRequest) -> Result<ComponentRegistry, SpiError> {
        let event_store = build_event_store(&self.config.stores.event).await?;
        let task_store = build_task_store(&self.config.stores.task).await?;
        let reference_store = build_reference_store(&self.config.stores.reference).await?;

        let mut registry = ComponentRegistry::new().with_stores(StoreBundle {
            event_store,
            task_store,
            reference_store,
        });

        if !self.options.skip_blob {
            let blob_store = build_blob_store(&self.config).await?;
            registry = registry.with_blob_store(blob_store);
        }

        Ok(registry)
    }
}

async fn build_event_store(
    spec: &StoreSpec,
) -> Result<Arc<dyn orchestral_core::store::EventStore>, SpiError> {
    match spec.backend.trim().to_ascii_lowercase().as_str() {
        "in_memory" | "memory" => Ok(Arc::new(InMemoryEventStore::new())),
        "redis" => {
            let url = require_setting(
                "event_store",
                "connection_url",
                spec.connection_url.as_ref(),
            )?;
            let prefix = spec
                .key_prefix
                .clone()
                .unwrap_or_else(|| "orchestral:event".to_string());
            let store = RedisEventStore::new(url, prefix)
                .map_err(|err| SpiError::Internal(err.to_string()))?;
            Ok(Arc::new(store))
        }
        "postgres" | "postgresql" | "pgsql" => {
            let url = require_setting(
                "event_store",
                "connection_url",
                spec.connection_url.as_ref(),
            )?;
            let prefix = spec
                .key_prefix
                .clone()
                .unwrap_or_else(|| "orchestral".to_string());
            let store = PostgresEventStore::new(url, prefix)
                .await
                .map_err(|err| SpiError::Internal(err.to_string()))?;
            Ok(Arc::new(store))
        }
        "sqlite" => {
            let url = spec
                .connection_url
                .as_deref()
                .unwrap_or(DEFAULT_SQLITE_STORES_URL);
            let prefix = spec
                .key_prefix
                .clone()
                .unwrap_or_else(|| "orchestral".to_string());
            let store = SqliteEventStore::new(url, prefix)
                .await
                .map_err(|err| SpiError::Internal(err.to_string()))?;
            Ok(Arc::new(store))
        }
        backend => Err(SpiError::UnsupportedBackend {
            component: "event_store".to_string(),
            backend: backend.to_string(),
        }),
    }
}

async fn build_task_store(
    spec: &StoreSpec,
) -> Result<Arc<dyn orchestral_core::store::TaskStore>, SpiError> {
    match spec.backend.trim().to_ascii_lowercase().as_str() {
        "in_memory" | "memory" => Ok(Arc::new(InMemoryTaskStore::new())),
        "redis" => {
            let url =
                require_setting("task_store", "connection_url", spec.connection_url.as_ref())?;
            let prefix = spec
                .key_prefix
                .clone()
                .unwrap_or_else(|| "orchestral:task".to_string());
            let store = RedisTaskStore::new(url, prefix)
                .map_err(|err| SpiError::Internal(err.to_string()))?;
            Ok(Arc::new(store))
        }
        "postgres" | "postgresql" | "pgsql" => {
            let url =
                require_setting("task_store", "connection_url", spec.connection_url.as_ref())?;
            let prefix = spec
                .key_prefix
                .clone()
                .unwrap_or_else(|| "orchestral".to_string());
            let store = PostgresTaskStore::new(url, prefix)
                .await
                .map_err(|err| SpiError::Internal(err.to_string()))?;
            Ok(Arc::new(store))
        }
        "sqlite" => {
            let url = spec
                .connection_url
                .as_deref()
                .unwrap_or(DEFAULT_SQLITE_STORES_URL);
            let prefix = spec
                .key_prefix
                .clone()
                .unwrap_or_else(|| "orchestral".to_string());
            let store = SqliteTaskStore::new(url, prefix)
                .await
                .map_err(|err| SpiError::Internal(err.to_string()))?;
            Ok(Arc::new(store))
        }
        backend => Err(SpiError::UnsupportedBackend {
            component: "task_store".to_string(),
            backend: backend.to_string(),
        }),
    }
}

async fn build_reference_store(
    spec: &StoreSpec,
) -> Result<Arc<dyn orchestral_core::store::ReferenceStore>, SpiError> {
    match spec.backend.trim().to_ascii_lowercase().as_str() {
        "in_memory" | "memory" => Ok(Arc::new(InMemoryReferenceStore::new())),
        "redis" => {
            let url = require_setting(
                "reference_store",
                "connection_url",
                spec.connection_url.as_ref(),
            )?;
            let prefix = spec
                .key_prefix
                .clone()
                .unwrap_or_else(|| "orchestral:reference".to_string());
            let store = RedisReferenceStore::new(url, prefix)
                .map_err(|err| SpiError::Internal(err.to_string()))?;
            Ok(Arc::new(store))
        }
        "postgres" | "postgresql" | "pgsql" => {
            let url = require_setting(
                "reference_store",
                "connection_url",
                spec.connection_url.as_ref(),
            )?;
            let prefix = spec
                .key_prefix
                .clone()
                .unwrap_or_else(|| "orchestral".to_string());
            let store = PostgresReferenceStore::new(url, prefix)
                .await
                .map_err(|err| SpiError::Internal(err.to_string()))?;
            Ok(Arc::new(store))
        }
        "sqlite" => {
            let url = spec
                .connection_url
                .as_deref()
                .unwrap_or(DEFAULT_SQLITE_STORES_URL);
            let prefix = spec
                .key_prefix
                .clone()
                .unwrap_or_else(|| "orchestral".to_string());
            let store = SqliteReferenceStore::new(url, prefix)
                .await
                .map_err(|err| SpiError::Internal(err.to_string()))?;
            Ok(Arc::new(store))
        }
        "sqlite_vector" | "sqlite-vector" => {
            let url = spec
                .connection_url
                .as_deref()
                .unwrap_or(DEFAULT_SQLITE_STORES_URL);
            let prefix = spec
                .key_prefix
                .clone()
                .unwrap_or_else(|| "orchestral".to_string());
            let store = SqliteReferenceStore::new_vector(url, prefix)
                .await
                .map_err(|err| SpiError::Internal(err.to_string()))?;
            Ok(Arc::new(store))
        }
        backend => Err(SpiError::UnsupportedBackend {
            component: "reference_store".to_string(),
            backend: backend.to_string(),
        }),
    }
}

async fn build_blob_store(config: &OrchestralConfig) -> Result<Arc<dyn BlobStore>, SpiError> {
    let mode = match config.blobs.mode.trim().to_ascii_lowercase().as_str() {
        "local" => BlobMode::Local,
        "s3" => BlobMode::S3,
        "hybrid" => BlobMode::Hybrid,
        backend => {
            return Err(SpiError::UnsupportedBackend {
                component: "blob_store".to_string(),
                backend: backend.to_string(),
            });
        }
    };

    let service_config = BlobServiceConfig {
        mode: mode.clone(),
        hybrid_write_to: if config.blobs.hybrid.write_to.trim().is_empty() {
            None
        } else {
            Some(config.blobs.hybrid.write_to.clone())
        },
    };

    let mut service = match config
        .blobs
        .catalog
        .backend
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "in_memory" | "memory" => FileService::with_in_memory_catalog(service_config),
        "postgres" | "postgresql" | "pgsql" => {
            let url = require_setting(
                "blob_catalog",
                "connection_url",
                config.blobs.catalog.connection_url.as_ref(),
            )?;
            FileService::with_postgres_catalog(
                service_config,
                url,
                config.blobs.catalog.table_prefix.clone(),
            )
            .await
            .map_err(|err| SpiError::Io(err.to_string()))?
        }
        backend => {
            return Err(SpiError::UnsupportedBackend {
                component: "blob_catalog".to_string(),
                backend: backend.to_string(),
            });
        }
    };

    service = service.with_local_defaults(LocalBlobStoreConfig {
        root_dir: config.blobs.local.root_dir.clone(),
    });

    let needs_s3 = matches!(mode, BlobMode::S3)
        || (matches!(mode, BlobMode::Hybrid)
            && config
                .blobs
                .hybrid
                .write_to
                .trim()
                .eq_ignore_ascii_case("s3"));
    if needs_s3 {
        let bucket = require_setting("blob_store.s3", "bucket", config.blobs.s3.bucket.as_ref())?;
        let s3_client = build_s3_blob_client(config).await?;
        service = service.with_s3_client(
            S3BlobStoreConfig {
                bucket: bucket.to_string(),
                key_prefix: config
                    .blobs
                    .s3
                    .key_prefix
                    .clone()
                    .unwrap_or_else(|| "orchestral/blobs".to_string()),
            },
            s3_client,
        );
    }

    Ok(Arc::new(service))
}

async fn build_s3_blob_client(
    config: &OrchestralConfig,
) -> Result<Arc<dyn S3BlobClient>, SpiError> {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());
    if let Some(region) = config
        .blobs
        .s3
        .region
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        loader = loader.region(Region::new(region));
    }
    if let Some(provider) = resolve_s3_credentials_provider(config)? {
        loader = loader.credentials_provider(provider);
    }
    let shared_config = loader.load().await;
    let mut s3_config_builder = S3ConfigBuilder::from(&shared_config);
    if let Some(endpoint) = config
        .blobs
        .s3
        .endpoint
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        s3_config_builder = s3_config_builder.endpoint_url(endpoint);
    }
    if let Some(force_path_style) = config.blobs.s3.force_path_style {
        s3_config_builder = s3_config_builder.force_path_style(force_path_style);
    }
    Ok(Arc::new(AwsSdkS3BlobClient {
        client: S3Client::from_conf(s3_config_builder.build()),
    }))
}

fn resolve_s3_credentials_provider(
    config: &OrchestralConfig,
) -> Result<Option<SharedCredentialsProvider>, SpiError> {
    let access_key = resolve_env_value(config.blobs.s3.access_key_env.as_ref(), "blob_store.s3")?;
    let secret_key = resolve_env_value(config.blobs.s3.secret_key_env.as_ref(), "blob_store.s3")?;
    match (access_key, secret_key) {
        (Some(access_key), Some(secret_key)) => Ok(Some(SharedCredentialsProvider::new(
            Credentials::new(access_key, secret_key, None, None, "orchestral-config"),
        ))),
        (None, None) => Ok(None),
        _ => Err(SpiError::MissingSetting {
            component: "blob_store.s3".to_string(),
            setting: "access_key_env and secret_key_env must be configured together".to_string(),
        }),
    }
}

fn resolve_env_value(
    env_name: Option<&String>,
    component: &str,
) -> Result<Option<String>, SpiError> {
    let Some(name) = env_name
        .map(String::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return Ok(None);
    };
    let value = std::env::var(name).map_err(|_| SpiError::MissingSetting {
        component: component.to_string(),
        setting: format!("env '{}' is not set", name),
    })?;
    Ok(Some(value))
}

struct AwsSdkS3BlobClient {
    client: S3Client,
}

#[async_trait]
impl S3BlobClient for AwsSdkS3BlobClient {
    async fn put_object_stream(
        &self,
        bucket: &str,
        key: &str,
        mut body: BlobStream,
        content_type: Option<&str>,
    ) -> Result<S3PutObjectResult, BlobIoError> {
        let mut data: Vec<u8> = Vec::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            data.extend_from_slice(&chunk);
        }
        let byte_size = data.len() as u64;
        if byte_size == 0 {
            return Err(BlobIoError::Invalid("empty blob payload".to_string()));
        }
        let mut request = self
            .client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(data));
        if let Some(content_type) = content_type.filter(|s| !s.trim().is_empty()) {
            request = request.content_type(content_type);
        }
        let output = request
            .send()
            .await
            .map_err(|err| BlobIoError::Io(err.to_string()))?;
        Ok(S3PutObjectResult {
            byte_size,
            etag: output.e_tag.map(|s| s.trim_matches('"').to_string()),
        })
    }

    async fn get_object_stream(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<S3GetObjectResult, BlobIoError> {
        let output = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| BlobIoError::Io(err.to_string()))?;
        let aggregated = output
            .body
            .collect()
            .await
            .map_err(|err| BlobIoError::Io(err.to_string()))?;
        let bytes = Bytes::from(aggregated.into_bytes().to_vec());
        let byte_size = bytes.len() as u64;
        let body_stream =
            futures_util::stream::once(async move { Ok::<Bytes, BlobIoError>(bytes) });
        Ok(S3GetObjectResult {
            body: Box::pin(body_stream),
            byte_size,
            etag: output.e_tag.map(|s| s.trim_matches('"').to_string()),
            last_modified: None,
        })
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), BlobIoError> {
        self.client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| BlobIoError::Io(err.to_string()))?;
        Ok(())
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<BlobHead, BlobIoError> {
        let output = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| BlobIoError::Io(err.to_string()))?;
        Ok(BlobHead {
            byte_size: output.content_length().map(|v| v as u64).unwrap_or(0),
            etag: output.e_tag.map(|s| s.trim_matches('"').to_string()),
            last_modified: None,
        })
    }
}

fn require_setting<'a>(
    component: &str,
    setting: &str,
    value: Option<&'a String>,
) -> Result<&'a str, SpiError> {
    value
        .map(String::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| SpiError::MissingSetting {
            component: component.to_string(),
            setting: setting.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_spi::SpiMeta;

    #[tokio::test]
    async fn test_infra_factory_skip_blob() {
        let mut config = OrchestralConfig::default();
        config.stores.event.backend = "in_memory".to_string();
        config.stores.task.backend = "in_memory".to_string();
        config.stores.reference.backend = "in_memory".to_string();
        let factory = InfraComponentFactory::with_options(
            Arc::new(config),
            InfraFactoryOptions { skip_blob: true },
        );
        let components = factory
            .build(&RuntimeBuildRequest {
                meta: SpiMeta::runtime_defaults("test"),
                config_path: "configs/orchestral.yaml".to_string(),
                profile: Some("cli".to_string()),
                options: serde_json::Map::new(),
            })
            .await
            .expect("build should succeed");
        assert!(components.stores.is_some());
        assert!(components.blob_store.is_none());
    }
}
