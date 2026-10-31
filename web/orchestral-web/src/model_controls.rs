//! Provider-neutral presentation of session model settings.

use serde::{Deserialize, Serialize};

use crate::model::{AgentConnectorView, AgentSessionExecutionProfile, SessionView};

pub const SET_MODEL: &str = "session.set_model";
pub const SET_REASONING: &str = "session.set_reasoning";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelChoice {
    pub value: String,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelSettingsView {
    pub session_id: String,
    pub profile: AgentSessionExecutionProfile,
    pub models: Vec<ModelChoice>,
    pub reasoning: Vec<ModelChoice>,
    pub reasoning_declared: bool,
    pub notice: Option<String>,
    pub model_supported: bool,
    pub reasoning_supported: bool,
    pub allow_custom_model: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelControlsState {
    pub open: bool,
    pub session_key: String,
    pub loading: bool,
    pub busy: bool,
    pub view: Option<ModelSettingsView>,
    pub error: Option<String>,
    pub generation: u64,
}

impl ModelControlsState {
    pub fn begin(&mut self, session_key: String) -> u64 {
        let generation = self.generation.wrapping_add(1);
        *self = Self {
            open: true,
            loading: true,
            session_key,
            generation,
            ..Default::default()
        };
        generation
    }
    pub fn accepts(&self, session_key: &str, generation: u64) -> bool {
        self.open && self.session_key == session_key && self.generation == generation
    }
    pub fn close(&mut self) {
        let generation = self.generation.wrapping_add(1);
        *self = Self {
            generation,
            ..Default::default()
        };
    }
}

pub fn from_connector(session: &SessionView, connector: &AgentConnectorView) -> ModelSettingsView {
    let field = |id: &str, key: &str| {
        connector
            .actions
            .iter()
            .find(|action| {
                action.action_id == id
                    && action.execution == crate::model::AgentSessionActionExecutionView::Immediate
            })
            .and_then(|action| action.input_schema.as_ref())
            .and_then(|schema| schema.get("properties"))
            .and_then(|properties| properties.get(key))
    };
    let model = field(SET_MODEL, "model");
    let reasoning = field(SET_REASONING, "reasoning_effort");
    let options = |schema: Option<&serde_json::Value>| {
        schema
            .and_then(|schema| schema.get("enum"))
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|value| value.as_str())
            .map(|value| ModelChoice {
                value: value.into(),
                label: value.into(),
            })
            .collect::<Vec<_>>()
    };
    let models = options(model);
    let reasoning_options = options(reasoning);
    ModelSettingsView {
        session_id: session.id.clone(),
        profile: session.execution_profile.clone(),
        allow_custom_model: model.is_some() && models.is_empty(),
        model_supported: model.is_some(),
        reasoning_supported: reasoning.is_some(),
        reasoning_declared: !reasoning_options.is_empty(),
        models,
        reasoning: reasoning_options,
        notice: (model.is_none() && reasoning.is_none())
            .then(|| "此 Agent 尚未提供模型和推理强度切换接口。".into()),
    }
}

pub fn initial_choice(options: &[ModelChoice], current: Option<&str>) -> String {
    current
        .and_then(|current| {
            options
                .iter()
                .find(|choice| choice.value == current || choice.label == current)
        })
        .map(|choice| choice.value.clone())
        .or_else(|| current.map(str::to_owned))
        .or_else(|| options.first().map(|choice| choice.value.clone()))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn late_catalog_results_cannot_replace_another_session_or_a_newer_refresh() {
        let mut controls = ModelControlsState::default();
        let first = controls.begin("first".into());
        assert!(controls.accepts("first", first));
        let second = controls.begin("first".into());
        assert!(!controls.accepts("first", first));
        controls.close();
        assert!(!controls.accepts("first", second));
        let third = controls.begin("second".into());
        assert!(!controls.accepts("first", third));
        assert!(controls.accepts("second", third));
    }

    #[test]
    fn connector_options_are_declared_actions_and_preserve_unknown_current_values() {
        let session: SessionView = serde_json::from_value(json!({"id":"session","created_at_unix_ms":0,"updated_at_unix_ms":0,"execution_profile":{"model":"old-model","reasoning_effort":"future-effort"}})).unwrap();
        let mut connector: AgentConnectorView = serde_json::from_value(json!({"connector_id":"opaque","display_name":"Agent","agent_family":"coding-agent","capabilities":{"list":true,"read":true,"create":false},"actions":[{"action_id":SET_MODEL,"title":"Model","description":"Choose a model","input_schema":{"type":"object","properties":{"model":{"type":"string","enum":["available-model"]}}}}]})).unwrap();
        let view = from_connector(&session, &connector);
        assert!(view.model_supported);
        assert!(!view.reasoning_supported);
        assert_eq!(
            view.models,
            vec![ModelChoice {
                value: "available-model".into(),
                label: "available-model".into()
            }]
        );
        assert_eq!(
            initial_choice(&view.models, view.profile.model.as_deref()),
            "old-model"
        );
        connector.actions.clear();
        let view = from_connector(&session, &connector);
        assert!(!view.model_supported);
        assert!(!view.reasoning_supported);
        assert!(view.notice.is_some());
    }

    #[test]
    fn opaque_model_choices_resolve_by_label_without_exposing_encoding_in_the_ui() {
        let choices = vec![ModelChoice {
            value: "opaque-selection".into(),
            label: "model-name".into(),
        }];
        assert_eq!(
            initial_choice(&choices, Some("model-name")),
            "opaque-selection"
        );
    }
}
