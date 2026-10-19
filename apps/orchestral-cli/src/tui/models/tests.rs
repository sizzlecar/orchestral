use super::*;
use orchestral_core::model_protocol::{
    ModelBackend, ModelDescriptor, ModelError, ModelRequest, ModelStream,
};
use orchestral_model_gemini::{GeminiAuthentication, GeminiModelBackend, GeminiModelConfig};
use orchestral_model_openai::{OpenAiCompatibleBackend, OpenAiCompatibleConfig};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn openai_backend(endpoint: &str) -> Arc<dyn ModelBackend> {
    Arc::new(
        OpenAiCompatibleBackend::new(OpenAiCompatibleConfig {
            backend_id: "menu-service".into(),
            endpoint: endpoint.into(),
            api_key: String::new(),
            model: "model".into(),
            temperature: 0.0,
            default_max_output_tokens: 128,
            max_context_tokens: None,
            timeout: Duration::from_secs(1),
            structured_output: true,
            max_buffered_events: 16,
        })
        .unwrap(),
    )
}

fn gemini_backend(model: &str) -> Arc<dyn ModelBackend> {
    Arc::new(
        GeminiModelBackend::new(GeminiModelConfig {
            backend_id: "menu-native".into(),
            endpoint: "http://127.0.0.1:1".into(),
            authentication: GeminiAuthentication::BearerToken("fixture".into()),
            model: model.into(),
            temperature: 0.0,
            thinking_level: None,
            default_max_output_tokens: 128,
            max_context_tokens: None,
            timeout: Duration::from_secs(1),
            max_buffered_events: 16,
        })
        .unwrap(),
    )
}

struct ExternalBackend(ModelReasoningCapabilities, Option<Vec<String>>);

#[async_trait::async_trait]
impl ModelBackend for ExternalBackend {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "external-adapter".into(),
            capabilities: Default::default(),
            extensions: Default::default(),
        }
    }

    fn reasoning_capabilities(&self) -> ModelReasoningCapabilities {
        self.0.clone()
    }

    async fn list_models(&self) -> Result<Option<Vec<String>>, ModelError> {
        Ok(self.1.clone())
    }

    async fn resolve_reasoning_capabilities(
        &self,
    ) -> Result<ModelReasoningCapabilities, ModelError> {
        let mut capabilities = self.0.clone();
        capabilities.default_thinking_enabled = Some(true);
        Ok(capabilities)
    }

    async fn start(
        &self,
        _: ModelRequest,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        panic!("reasoning inspection must not generate model output")
    }
}

#[tokio::test]
async fn external_model_adapters_drive_reasoning_without_provider_registration_in_the_ui() {
    let mut meta = metadata("http://127.0.0.1:1/v1");
    meta.model_backend.kind = "third-party-native-api".into();
    let custom = ReasoningPreference::from_effort("adaptive-depth").unwrap();
    meta.model_controls = Arc::new(ExternalBackend(
        ModelReasoningCapabilities {
            supported_preferences: Some(vec![custom.clone()]),
            default_thinking_enabled: None,
        },
        Some(vec!["model".into(), "other-model".into()]),
    ));
    let models = discover(&meta, "model").await.unwrap();
    assert!(
        matches!(serde_json::from_str(&models.choices[1].value).unwrap(), Selection::ApiModel { id, .. } if id == "other-model")
    );
    let menu = reasoning(&meta, "model").await.unwrap();
    assert_eq!(
        menu.choices
            .iter()
            .map(|c| c.label.as_str())
            .collect::<Vec<_>>(),
        ["default", "adaptive-depth"]
    );
    explicit_reasoning(&meta, "model", "adaptive-depth").unwrap();
    assert!(explicit_reasoning(&meta, "model", "high").is_err());

    // Arc<dyn ModelBackend> forwarding preserves capabilities and validation.
    meta.model_controls = Arc::new(meta.model_controls.clone());
    assert_eq!(reasoning(&meta, "model").await.unwrap(), menu);
    assert_eq!(discover(&meta, "model").await.unwrap(), models);
    explicit_reasoning(&meta, "model", "adaptive-depth").unwrap();
    assert!(explicit_reasoning(&meta, "model", "high").is_err());

    meta.model_controls = Arc::new(ExternalBackend(ModelReasoningCapabilities::default(), None));
    assert_eq!(reasoning(&meta, "model").await.unwrap().choices.len(), 1);
    assert!(matches!(
        serde_json::from_str(&discover(&meta, "model").await.unwrap().choices[0].value).unwrap(),
        Selection::Profile { .. }
    ));
    explicit_reasoning(&meta, "model", "default").unwrap();
    assert!(explicit_reasoning(&meta, "model", "adaptive-depth").is_err());

    meta.model_controls = Arc::new(ExternalBackend(
        ModelReasoningCapabilities::default(),
        Some(Vec::new()),
    ));
    assert!(discover(&meta, "model").await.is_err());
}

#[tokio::test]
async fn reasoning_discovery_errors_remain_distinct_from_unknown_capabilities() {
    for (status, body, expected) in [
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            json!({"error":"offline"}),
            "503",
        ),
        (
            axum::http::StatusCode::OK,
            json!({"data":[{"id":"other-model"}]}),
            "absent",
        ),
    ] {
        let (endpoint, _server) = server(vec![(status, body)]).await;
        let meta = metadata(&endpoint);
        assert!(format!("{:#}", reasoning(&meta, "model").await.unwrap_err()).contains(expected));
    }
    let (endpoint, _server) = server(vec![(
        axum::http::StatusCode::OK,
        json!({"data":[{"id":"model"}]}),
    )])
    .await;
    assert_eq!(
        reasoning(&metadata(&endpoint), "model")
            .await
            .unwrap()
            .choices
            .len(),
        1
    );
}

#[tokio::test]
async fn compatible_services_supply_reasoning_metadata_through_the_shared_adapter_interface() {
    for kind in ["openai", "openrouter", "deepseek", "groq", "xai", "mistral"] {
        let (endpoint, _server) = server(vec![(axum::http::StatusCode::OK,
            json!({"data":[{"id":"model", "reasoning":{"supported_efforts":["low","future-depth"]}}]})
        )]).await;
        let mut meta = metadata(&endpoint);
        meta.model_backend.kind = kind.into();
        let menu = reasoning(&meta, "model").await.unwrap();
        assert_eq!(
            menu.choices
                .iter()
                .map(|c| c.label.as_str())
                .collect::<Vec<_>>(),
            ["default", "low", "future-depth"]
        );
    }
}

fn metadata(endpoint: &str) -> HostMetadata {
    HostMetadata {
        workspaces: Vec::new(), journal_location: String::new(), context: String::new(), context_budget: Some(8192),
        models: vec![serde_json::from_value(json!({"name":"same-name", "backend":"configured", "model":"profile-id"})).unwrap()],
        model_backend: serde_json::from_value(json!({"name":"cli-openai", "kind":"openai", "endpoint":endpoint,"config":{"auth":"none"}})).unwrap(),
        model_controls: openai_backend(endpoint),
        reasoning: ReasoningPreference::Default,
        permissions: PermissionPreset::Auto,
    }
}

struct Server(tokio::task::JoinHandle<()>);

#[tokio::test]
async fn gemini_reasoning_menu_uses_native_capabilities_without_model_discovery() {
    for kind in ["google", "gemini", " Gemini "] {
        let mut meta = metadata("http://127.0.0.1:1/v1");
        meta.model_backend.kind = kind.into();
        // Menus need neither the credential file nor a token/discovery request.
        meta.model_backend.config = json!({"credential_file":"/missing/credential.json"});
        for (model, expected) in [
            (
                "gemini-3.1-pro-preview",
                vec!["default", "low", "medium", "high"],
            ),
            (
                "models/gemini-3-flash-preview",
                vec!["default", "minimal", "low", "medium", "high"],
            ),
            ("gemini-2.5-flash", vec!["default", "on", "off"]),
            ("gemini-2.5-pro", vec!["default", "on"]),
            ("unknown-model", vec!["default"]),
        ] {
            meta.model_controls = gemini_backend(model);
            let menu = reasoning(&meta, model).await.unwrap();
            assert_eq!(
                menu.choices
                    .iter()
                    .map(|c| c.label.as_str())
                    .collect::<Vec<_>>(),
                expected
            );
            for entry in menu.choices.iter() {
                let selection = serde_json::from_str(&entry.value).unwrap();
                let mut overrides = ModelOverrides::default();
                apply_selection(&meta, model, &mut overrides, selection).unwrap();
                meta.model_controls
                    .validate_reasoning_preference(overrides.reasoning.as_ref().unwrap())
                    .unwrap();
                assert_eq!(overrides.model.as_deref(), Some(model));
                assert!(overrides.backend.is_none() && overrides.base_url.is_none());
            }
        }
    }
}

#[test]
fn gemini_reasoning_explicit_values_validate_against_native_model_controls() {
    let mut meta = metadata("http://127.0.0.1:1/v1");
    meta.model_backend.kind = "gemini".into();
    for (model, value) in [
        ("gemini-3.1-pro-preview", "high"),
        ("gemini-2.5-flash", "off"),
        ("unknown-model", "low"),
    ] {
        meta.model_controls = gemini_backend(model);
        explicit_reasoning(&meta, model, value).unwrap();
    }
    for (model, value) in [
        ("gemini-2.5-pro", "off"),
        ("gemini-2.5-flash", "high"),
        ("gemini-3-pro-preview", "medium"),
        ("gemini-3.1-pro-preview", "xhigh"),
    ] {
        meta.model_controls = gemini_backend(model);
        let error = explicit_reasoning(&meta, model, value).unwrap_err();
        assert!(error.to_string().contains("not supported"), "{error:#}");
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn server(responses: Vec<(axum::http::StatusCode, serde_json::Value)>) -> (String, Server) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let responses = Arc::new(Mutex::new(std::collections::VecDeque::from(responses)));
    let app = axum::Router::new().route(
        "/proxy/v1/models",
        axum::routing::get(move |headers: axum::http::HeaderMap| {
            let responses = responses.clone();
            async move {
                assert!(!headers.contains_key("authorization"));
                let (status, body) = responses
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("expected discovery call");
                (status, axum::Json(body))
            }
        }),
    );
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (
        format!("http://{address}/proxy/v1/chat/completions"),
        Server(task),
    )
}

#[tokio::test]
async fn model_menu_refreshes_api_ids_and_keeps_profiles_explicit() {
    let (endpoint, _server) = server(vec![
        (
            axum::http::StatusCode::OK,
            json!({"data":[{"id":"same-name"},{"id":"other-api"}]}),
        ),
        (
            axum::http::StatusCode::OK,
            json!({"data":[{"id":"new-api"}]}),
        ),
    ])
    .await;
    let metadata = metadata(&endpoint);
    let first = discover(&metadata, "same-name").await.unwrap();
    assert_eq!(first.choices.len(), 3);
    assert!(first.choices.iter().any(|choice| matches!(serde_json::from_str::<Selection>(&choice.value).unwrap(), Selection::ApiModel { id, .. } if id == "same-name")));
    assert!(first.choices.iter().all(|choice| !matches!(
        serde_json::from_str::<Selection>(&choice.value).unwrap(),
        Selection::Profile { .. }
    )));
    let second = discover(&metadata, "same-name").await.unwrap();
    assert_eq!(second.choices[0].label, "new-api");
    assert!(!second
        .choices
        .iter()
        .any(|choice| choice.label == "same-name"));
}

#[tokio::test]
async fn empty_and_failed_discovery_do_not_masquerade_as_configured_models() {
    let (endpoint, _server) = server(vec![
        (axum::http::StatusCode::OK, json!({"data":[]})),
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            json!({"error":"offline"}),
        ),
    ])
    .await;
    let metadata = metadata(&endpoint);
    assert!(
        format!("{:#}", discover(&metadata, "current").await.unwrap_err()).contains("no models")
    );
    assert!(format!("{:#}", discover(&metadata, "current").await.unwrap_err()).contains("503"));
    assert_eq!(profiles(&metadata, "current").choices[0].label, "same-name");
}

#[test]
fn api_selection_retains_connection_profile_and_reasoning_without_synthetic_backend_override() {
    let metadata = metadata("http://127.0.0.1:1/v1");
    let mut overrides = ModelOverrides {
        base_url: Some(metadata.model_backend.endpoint.clone().unwrap()),
        api_key_env: Some("EXPLICIT_KEY".into()),
        model_profile: Some("kept-sampling".into()),
        reasoning: Some(ReasoningPreference::Off),
        ..Default::default()
    };
    apply_selection(
        &metadata,
        "old",
        &mut overrides,
        Selection::ApiModel {
            backend: "cli-openai".into(),
            id: "same-name".into(),
        },
    )
    .unwrap();
    assert_eq!(overrides.model.as_deref(), Some("same-name"));
    assert_eq!(overrides.backend, None);
    assert_eq!(overrides.api_key_env.as_deref(), Some("EXPLICIT_KEY"));
    assert_eq!(overrides.model_profile.as_deref(), Some("kept-sampling"));
    assert!(overrides.base_url.is_some());
    assert_eq!(overrides.reasoning, Some(ReasoningPreference::Off));
    apply_selection(
        &metadata,
        "same-name",
        &mut overrides,
        Selection::Profile {
            name: "same-name".into(),
        },
    )
    .unwrap();
    assert_eq!(overrides.model.as_deref(), Some("profile-id"));
    assert_eq!(overrides.backend.as_deref(), Some("configured"));
    assert!(overrides.base_url.is_none() && overrides.api_key_env.is_none() && !overrides.no_auth);
}

#[test]
fn reasoning_selection_pins_api_model_and_rejects_stale_selection() {
    let metadata = metadata("http://127.0.0.1:1/v1");
    let mut overrides = ModelOverrides::default();
    apply_selection(
        &metadata,
        "api-model",
        &mut overrides,
        Selection::Reasoning {
            backend: "cli-openai".into(),
            model: "api-model".into(),
            value: ReasoningPreference::Off,
        },
    )
    .unwrap();
    assert_eq!(overrides.model.as_deref(), Some("api-model"));
    assert_eq!(overrides.reasoning, Some(ReasoningPreference::Off));
    assert!(apply_selection(
        &metadata,
        "new-api-model",
        &mut overrides,
        Selection::Reasoning {
            backend: "cli-openai".into(),
            model: "api-model".into(),
            value: ReasoningPreference::High,
        }
    )
    .is_err());
}

#[test]
fn binary_reasoning_menu_lists_only_default_on_off_and_marks_unknown_explicitly() {
    let metadata = metadata("http://127.0.0.1:1/v1");
    let mut capabilities = ModelReasoningCapabilities {
        supported_preferences: Some(vec![ReasoningPreference::On, ReasoningPreference::Off]),
        default_thinking_enabled: Some(true),
    };
    let menu = reasoning_menu(&metadata, "model", &capabilities);
    assert_eq!(
        menu.choices
            .iter()
            .map(|choice| choice.label.as_str())
            .collect::<Vec<_>>(),
        ["default", "on", "off"]
    );
    capabilities.supported_preferences = None;
    capabilities.default_thinking_enabled = None;
    let menu = reasoning_menu(&metadata, "model", &capabilities);
    assert!(
        menu.detail.is_none(),
        "selectable choices must not become a read-only detail panel"
    );
    assert!(menu.choices[0].description.contains("not declared"));
    assert_eq!(menu.choices.len(), 1);
    assert!(menu.choices[0].description.contains("/reasoning <value>"));
}

#[test]
fn declared_efforts_round_trip_through_menu_and_selection_without_local_control_aliases() {
    let metadata = metadata("http://127.0.0.1:1/v1");
    for (name, label) in [
        ("ultra", "ultra"),
        ("future-next", "future-next"),
        ("HIGH", "HIGH"),
        ("on", "effort:on"),
        ("off", "effort:off"),
        ("default", "effort:default"),
        ("effort:on", "effort:effort:on"),
    ] {
        let capabilities = ModelReasoningCapabilities {
            supported_preferences: Some(vec![ReasoningPreference::from_effort(name).unwrap()]),
            default_thinking_enabled: None,
        };
        let menu = reasoning_menu(&metadata, "model", &capabilities);
        assert_eq!(menu.choices.len(), 2);
        assert_eq!(menu.choices[1].label, label);
        let selection = serde_json::from_str(&menu.choices[1].value).unwrap();
        let mut overrides = ModelOverrides::default();
        apply_selection(&metadata, "model", &mut overrides, selection).unwrap();
        let preference = overrides.reasoning.unwrap();
        metadata
            .model_controls
            .validate_reasoning_preference(&preference)
            .unwrap();
        assert_eq!(preference, ReasoningPreference::from_effort(name).unwrap());
    }
}

#[test]
fn explicit_reasoning_command_needs_no_discovery_and_preserves_custom_effort() {
    let metadata = metadata("http://127.0.0.1:1/v1");
    for (command, expected) in [
        ("/reasoning future-next", "future-next"),
        ("/reasoning\tHIGH", "HIGH"),
        ("/reasoning effort:on", "on"),
    ] {
        let argument = reasoning_argument(command).unwrap();
        let selection = explicit_reasoning(&metadata, "model", argument).unwrap();
        let mut overrides = ModelOverrides::default();
        apply_selection(&metadata, "model", &mut overrides, selection).unwrap();
        assert_eq!(overrides.model.as_deref(), Some("model"));
        assert_eq!(
            overrides.reasoning,
            Some(ReasoningPreference::Custom(expected.into()))
        );
    }
    assert!(reasoning_argument("/reasoning").is_none());
    assert!(reasoning_argument("/reasoningful high").is_none());
    assert!(explicit_reasoning(&metadata, "model", "effort:").is_err());
}

#[test]
fn permissions_menu_and_explicit_command_select_all_presets() {
    let mut meta = metadata("http://127.0.0.1:1/v1");
    meta.permissions = PermissionPreset::Auto;
    let menu = permissions(&meta);
    assert_eq!(menu.choices.len(), 3);
    assert!(menu.choices[1].description.starts_with("Current · "));
    for (command, expected) in [
        ("/permissions read-only", PermissionPreset::ReadOnly),
        ("/permissions auto", PermissionPreset::Auto),
        ("/permissions default", PermissionPreset::Auto),
        ("/permissions full-access", PermissionPreset::FullAccess),
    ] {
        let argument = permissions_argument(command).unwrap();
        let selection = explicit_permissions(argument).unwrap();
        let mut overrides = ModelOverrides::default();
        apply_selection(&meta, "model", &mut overrides, selection).unwrap();
        assert_eq!(overrides.permissions, Some(expected));
        assert_eq!(overrides.model.as_deref(), Some("model"));
    }
    assert!(permissions_argument("/permissions").is_none());
    assert!(explicit_permissions("unknown").is_err());
}
