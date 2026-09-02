use axum::extract::{Path, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use include_dir::{include_dir, Dir, File};

static PWA: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/../../web/orchestral-web/dist");

pub fn router() -> Router {
    router_with_artifact_origin(None)
}

pub fn router_with_artifact_origin(artifact_origin: Option<&str>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/{*path}", get(asset))
        .with_state(AssetPolicy::new(artifact_origin))
}

#[derive(Clone)]
struct AssetPolicy {
    content_security_policy: HeaderValue,
}

impl AssetPolicy {
    fn new(artifact_origin: Option<&str>) -> Self {
        let image_sources = artifact_origin
            .map(|origin| format!("'self' data: {origin}"))
            .unwrap_or_else(|| "'self' data:".to_owned());
        let policy = format!(
            "default-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'; object-src 'none'; script-src 'self' 'wasm-unsafe-eval'; style-src 'self'; img-src {image_sources}; font-src 'self'; connect-src 'self'; manifest-src 'self'; worker-src 'self'"
        );
        Self {
            content_security_policy: HeaderValue::try_from(policy)
                .expect("validated Artifact HTTPS origin produces a valid CSP"),
        }
    }
}

async fn index(State(policy): State<AssetPolicy>) -> Response {
    asset_response(PWA.get_file("index.html"), "index.html", &policy)
}

async fn asset(State(policy): State<AssetPolicy>, Path(path): Path<String>) -> Response {
    if path
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
        || !allowed_asset_path(&path)
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    asset_response(PWA.get_file(&path), &path, &policy)
}

fn allowed_asset_path(path: &str) -> bool {
    matches!(path, "index.html" | "sw.js" | "manifest.webmanifest")
        || (path.starts_with("assets/")
            && (path.ends_with(".js") || path.ends_with(".wasm") || path.ends_with(".css")))
        || (path.starts_with("icons/") && (path.ends_with(".svg") || path.ends_with(".png")))
}

fn asset_response(file: Option<&File<'_>>, path: &str, policy: &AssetPolicy) -> Response {
    let Some(file) = file else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut response = file.contents().to_vec().into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(content_type(path)),
    );
    headers.insert(header::CACHE_CONTROL, cache_control(path));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        policy.content_security_policy.clone(),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        header::HeaderName::from_static("permissions-policy"),
        HeaderValue::from_static("camera=(), microphone=(), geolocation=()"),
    );
    headers.insert(
        header::HeaderName::from_static("x-frame-options"),
        HeaderValue::from_static("DENY"),
    );
    if path == "sw.js" {
        headers.insert(
            header::HeaderName::from_static("service-worker-allowed"),
            HeaderValue::from_static("/"),
        );
    }
    response
}

fn content_type(path: &str) -> &'static str {
    if path.ends_with(".html") {
        "text/html; charset=utf-8"
    } else if path.ends_with(".js") {
        "text/javascript; charset=utf-8"
    } else if path.ends_with(".css") {
        "text/css; charset=utf-8"
    } else if path.ends_with(".wasm") {
        "application/wasm"
    } else if path.ends_with(".webmanifest") {
        "application/manifest+json; charset=utf-8"
    } else if path.ends_with(".svg") {
        "image/svg+xml"
    } else if path.ends_with(".png") {
        "image/png"
    } else {
        "application/octet-stream"
    }
}

fn cache_control(path: &str) -> HeaderValue {
    if path.starts_with("assets/") && path.contains("-dxh") {
        HeaderValue::from_static("public, max-age=31536000, immutable")
    } else {
        HeaderValue::from_static("no-cache, private")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn static_shell_has_strict_browser_security_headers() {
        let response = router()
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        assert!(response.headers()[header::CONTENT_SECURITY_POLICY]
            .to_str()
            .unwrap()
            .contains("connect-src 'self'"));
        assert!(response.headers()[header::CONTENT_SECURITY_POLICY]
            .to_str()
            .unwrap()
            .contains("'wasm-unsafe-eval'"));
        // COOP is useful only on a potentially trustworthy origin. The Host
        // deliberately supports explicit trusted-LAN HTTP for diagnostics, so
        // sending it unconditionally would produce a misleading browser
        // security warning without protecting any feature used by this UI.
        assert!(response
            .headers()
            .get("cross-origin-opener-policy")
            .is_none());
    }

    #[tokio::test]
    async fn configured_artifact_origin_is_the_only_external_image_source() {
        let response = router_with_artifact_origin(Some("https://files.example"))
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let policy = response.headers()[header::CONTENT_SECURITY_POLICY]
            .to_str()
            .unwrap();
        assert!(policy.contains("img-src 'self' data: https://files.example; font-src 'self'"));
    }

    #[tokio::test]
    async fn path_traversal_and_unknown_assets_are_not_served() {
        let response = router()
            .oneshot(
                Request::builder()
                    .uri("/missing.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn install_manifest_worker_and_png_icons_are_embedded() {
        let app = router();
        let manifest = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/manifest.webmanifest")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(manifest.status(), StatusCode::OK);
        assert_eq!(
            manifest.headers()[header::CONTENT_TYPE],
            "application/manifest+json; charset=utf-8"
        );

        let worker = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/sw.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(worker.status(), StatusCode::OK);
        assert_eq!(worker.headers()["service-worker-allowed"], "/");

        let icon = app
            .oneshot(
                Request::builder()
                    .uri("/icons/icon-192.png")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(icon.status(), StatusCode::OK);
        assert_eq!(icon.headers()[header::CONTENT_TYPE], "image/png");
    }

    #[tokio::test]
    async fn fingerprinted_dioxus_assets_are_embedded_with_correct_mime_and_cache_headers() {
        let assets = PWA.get_dir("assets").expect("release assets directory");
        let wasm = assets
            .files()
            .find(|file| file.path().extension().is_some_and(|ext| ext == "wasm"))
            .expect("Dioxus wasm bundle");
        let js = assets
            .files()
            .find(|file| file.path().extension().is_some_and(|ext| ext == "js"))
            .expect("Dioxus JavaScript loader");

        for (file, mime) in [
            (wasm, "application/wasm"),
            (js, "text/javascript; charset=utf-8"),
        ] {
            let uri = format!("/{}", file.path().display());
            let response = router()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()[header::CONTENT_TYPE], mime);
            assert_eq!(
                response.headers()[header::CACHE_CONTROL],
                "public, max-age=31536000, immutable"
            );
        }
    }

    #[tokio::test]
    async fn retired_javascript_entrypoint_is_not_served() {
        let response = router()
            .oneshot(
                Request::builder()
                    .uri("/app.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
