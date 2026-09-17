use super::*;
use serde_json::json;
use std::sync::{Arc, Mutex};

fn metadata(endpoint: &str) -> HostMetadata {
    HostMetadata {
        workspaces: Vec::new(), journal_location: String::new(), context: String::new(), context_budget: Some(8192),
        models: vec![serde_json::from_value(json!({"name":"same-name", "backend":"configured", "model":"profile-id"})).unwrap()],
        model_backend: serde_json::from_value(json!({"name":"cli-openai", "kind":"openai", "endpoint":endpoint,"config":{"auth":"none"}})).unwrap(),
        reasoning: ReasoningPreference::Default,
    }
}

struct Server(tokio::task::JoinHandle<()>);
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
    let mut model = DiscoveredModel {
        id: "model".into(),
        reasoning: Some(vec![
            ReasoningPreference::Default,
            ReasoningPreference::On,
            ReasoningPreference::Off,
        ]),
        thinking_default_enabled: Some(true),
    };
    let menu = reasoning_menu(&metadata, &model);
    assert_eq!(
        menu.choices
            .iter()
            .map(|choice| choice.label.as_str())
            .collect::<Vec<_>>(),
        ["default", "on", "off"]
    );
    model.reasoning = None;
    model.thinking_default_enabled = None;
    let menu = reasoning_menu(&metadata, &model);
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
        let model =
            crate::model_controls::discovered_model(orchestral_model_openai::DiscoveredModel {
                id: "model".into(),
                max_context_tokens: None,
                reasoning: Some(orchestral_model_openai::OpenAiReasoningCapabilities {
                    supported_efforts: Some(vec![name.parse().unwrap()]),
                    thinking: None,
                }),
            });
        let menu = reasoning_menu(&metadata, &model);
        assert_eq!(menu.choices.len(), 2);
        assert_eq!(menu.choices[1].label, label);
        let selection = serde_json::from_str(&menu.choices[1].value).unwrap();
        let mut overrides = ModelOverrides::default();
        apply_selection(&metadata, "model", &mut overrides, selection).unwrap();
        let control = crate::model_controls::openai_reasoning(
            &metadata.model_backend,
            overrides.reasoning.unwrap(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            control,
            orchestral_model_openai::OpenAiReasoningControl::Effort(name.parse().unwrap())
        );
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
