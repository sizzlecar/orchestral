use super::*;
use axum::{extract::Request, response::IntoResponse, routing::get, Router};
use std::sync::atomic::{AtomicUsize, Ordering};

fn backend(endpoint: &str) -> GeminiModelBackend {
    GeminiModelBackend::new(GeminiModelConfig {
        backend_id: "metadata-fixture".to_owned(),
        endpoint: endpoint.to_owned(),
        authentication: GeminiAuthentication::ApiKey("fixture-secret".to_owned()),
        model: "exact-model".to_owned(),
        temperature: 0.2,
        thinking_level: None,
        default_max_output_tokens: 1024,
        max_context_tokens: None,
        timeout: Duration::from_secs(1),
        max_buffered_events: 8,
    })
    .unwrap()
}

async fn server(native: Value) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let app = Router::new().route(
        "/{*path}",
        get(move |request: Request| {
            let native = native.clone();
            let calls = count.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                if request.uri().path() == "/catalog" {
                    assert!(!request.headers().contains_key("authorization"));
                    assert!(!request.headers().contains_key("x-goog-api-key"));
                    axum::Json(json!({"google-vertex":{"models":{
                        "exact-model":{"limit":{"context":1_048_576,"output":65_536}},
                        "other-model":{"limit":{"context":8_192,"output":1024}}
                    }}}))
                    .into_response()
                } else {
                    assert_eq!(
                        request.headers().get("x-goog-api-key").unwrap(),
                        "fixture-secret"
                    );
                    axum::Json(native).into_response()
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (endpoint, calls, task)
}

#[tokio::test]
async fn native_capacity_is_resolved_once_and_wins_over_catalog() {
    let (endpoint, calls, task) =
        server(json!({"inputTokenLimit":600_000,"outputTokenLimit":16_384})).await;
    let model: Arc<dyn ModelBackend> =
        Arc::new(backend(&endpoint).with_model_catalog(Some(format!("{endpoint}/catalog"))));
    assert_eq!(model.descriptor().capabilities.max_context_tokens, None);
    let resolved = model.resolve_descriptor().await.unwrap();
    assert_eq!(resolved.capabilities.max_context_tokens, Some(600_000));
    assert_eq!(model.resolve_descriptor().await.unwrap(), resolved);
    assert_eq!(model.descriptor(), resolved);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    task.abort();
}

#[tokio::test]
async fn vertex_catalog_fallback_is_exact_scoped_cached_and_credential_free() {
    let (endpoint, calls, task) = server(json!({"name":"publisher-model","versionId":"1"})).await;
    let root = std::env::temp_dir().join(format!(
        "orchestral-model-metadata-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let make = || {
        backend(&format!("{endpoint}/v1/publishers/google"))
            .with_model_catalog(Some(format!("{endpoint}/catalog")))
            .with_metadata_cache(&root)
    };
    let model = make();
    let resolved = model.resolve_descriptor().await.unwrap();
    assert_eq!(resolved.capabilities.max_context_tokens, Some(1_048_576));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(make().resolve_descriptor().await.unwrap(), resolved);
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    let mut other = make();
    other.config.model = "unlisted-model".to_owned();
    assert_eq!(
        other
            .resolve_descriptor()
            .await
            .unwrap()
            .capabilities
            .max_context_tokens,
        None
    );
    assert_eq!(calls.load(Ordering::SeqCst), 4);

    let path = cache_path(&model).unwrap();
    let mut cached: Value = serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
    cached["fetched_at"] = json!(1);
    tokio::fs::write(&path, serde_json::to_vec(&cached).unwrap())
        .await
        .unwrap();
    task.abort();
    let _ = task.await;
    assert_eq!(make().resolve_descriptor().await.unwrap(), resolved);
    tokio::fs::remove_dir_all(root).await.unwrap();
}

#[tokio::test]
async fn configured_or_disabled_discovery_makes_no_network_requests() {
    let (endpoint, calls, task) = server(json!({"inputTokenLimit":1_000_000})).await;
    let mut configured = backend(&endpoint);
    configured.config.max_context_tokens = Some(65_536);
    assert_eq!(
        configured
            .resolve_descriptor()
            .await
            .unwrap()
            .capabilities
            .max_context_tokens,
        Some(65_536)
    );
    assert_eq!(
        backend(&endpoint)
            .with_metadata_discovery(false)
            .resolve_descriptor()
            .await
            .unwrap()
            .capabilities
            .max_context_tokens,
        None
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    task.abort();
}

#[test]
fn metadata_never_guesses_missing_model_limits_or_combines_input_and_output() {
    assert!(native_metadata(&json!({"inputTokenLimit":0}), "native".into()).is_none());
    let value = json!({"google":{"models":{"m":{"limit":{"context":1_000_000,"input":900_000,"output":64_000}}}}});
    assert_eq!(
        catalog_metadata(&value, "google", "m", "catalog")
            .unwrap()
            .context_tokens,
        900_000
    );
    assert!(catalog_metadata(&value, "google-vertex", "m", "catalog").is_none());
    assert!(catalog_metadata(&value, "google", "m-preview", "catalog").is_none());
    assert!(!public_google_endpoint(
        "https://aiplatform.googleapis.com.example.org/v1"
    ));
    assert!(backend("https://generativelanguage.googleapis.com/v1beta")
        .catalog_url
        .is_some());
    assert!(backend("http://127.0.0.1/v1beta").catalog_url.is_none());
}

#[tokio::test]
async fn bundled_capacity_is_immediate_exact_and_not_applied_to_custom_gateways() {
    let mut model = backend(
        "https://aiplatform.googleapis.com/v1/projects/p/locations/global/publishers/google",
    )
    .with_background_metadata_refresh();
    model.config.model = "gemini-4-argon".to_owned();
    let resolved = tokio::time::timeout(Duration::from_secs(1), model.resolve_descriptor())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resolved.capabilities.max_context_tokens, Some(1_048_576));
    assert!(resolved.extensions["orchestral/context-capacity-source"]
        .as_str()
        .unwrap()
        .starts_with("bundled models.dev snapshot"));
    let mut gateway = backend("http://127.0.0.1:1").with_background_metadata_refresh();
    gateway.config.model = "gemini-4-argon".to_owned();
    assert_eq!(
        gateway
            .resolve_descriptor()
            .await
            .unwrap()
            .capabilities
            .max_context_tokens,
        None
    );
    let mut unknown = backend("https://generativelanguage.googleapis.com/v1beta")
        .with_background_metadata_refresh();
    unknown.config.model = "not-in-the-bundled-catalog".to_owned();
    assert_eq!(
        unknown
            .resolve_descriptor()
            .await
            .unwrap()
            .capabilities
            .max_context_tokens,
        None
    );
}

#[tokio::test]
async fn pending_refresh_never_blocks_startup_or_changes_an_active_backend() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let app = Router::new().route(
        "/{*path}",
        get({
            let entered = entered.clone();
            let release = release.clone();
            move || {
                let entered = entered.clone();
                let release = release.clone();
                async move {
                    entered.notify_one();
                    release.notified().await;
                    axum::Json(json!({"inputTokenLimit":600_000,"outputTokenLimit":1024}))
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let root = std::env::temp_dir().join(format!(
        "orchestral-background-metadata-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let make = || {
        backend(&endpoint)
            .with_metadata_cache(&root)
            .with_background_metadata_refresh()
    };
    let active = make();
    let snapshot = tokio::time::timeout(Duration::from_secs(1), active.resolve_descriptor())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.capabilities.max_context_tokens, None);
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .unwrap();
    assert_eq!(active.resolve_descriptor().await.unwrap(), snapshot);
    release.notify_one();
    let path = cache_path(&active).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while cached(&path).await.is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        active.descriptor(),
        snapshot,
        "refresh must not change an active Run's identity"
    );
    assert_eq!(
        make()
            .resolve_descriptor()
            .await
            .unwrap()
            .capabilities
            .max_context_tokens,
        Some(600_000)
    );

    let mut value: Value = serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
    value["fetched_at"] = json!(1);
    tokio::fs::write(&path, serde_json::to_vec(&value).unwrap())
        .await
        .unwrap();
    let stale = make();
    let resolved = tokio::time::timeout(Duration::from_secs(1), stale.resolve_descriptor())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resolved.capabilities.max_context_tokens, Some(600_000));
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .unwrap();
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !matches!(cached(&path).await, Some((_, true))) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(stale.descriptor(), resolved);
    task.abort();
    tokio::fs::remove_dir_all(root).await.unwrap();
}
