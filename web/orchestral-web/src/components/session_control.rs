use std::collections::BTreeMap;

use dioxus::prelude::*;
use serde_json::{Map, Value};

use crate::browser::controller::AppController;
use crate::model::{AgentSessionActionExecutionView, AgentSessionActionView};

#[component]
pub fn NewSessionPanel() -> Element {
    let mut controller = consume_context::<AppController>();
    let state = controller.state.read().clone();
    if !state.ui.new_session_open {
        return rsx! {};
    }
    let disabled = state.ui.composer_busy || !state.connection.online;
    let create_connectors = state
        .connectors
        .items
        .into_iter()
        .filter(|connector| connector.capabilities.create)
        .collect::<Vec<_>>();

    rsx! {
        div {
            class: "settings-backdrop",
            role: "presentation",
            onclick: move |_| controller.state.write().ui.new_session_open = false,
        }
        dialog { class: "settings-dialog session-control-dialog", open: true,
            header { class: "settings-header",
                div {
                    p { class: "eyebrow", "选择执行者" }
                    h2 { "新建会话" }
                }
                button {
                    class: "icon-button",
                    r#type: "button",
                    aria_label: "关闭新建会话",
                    onclick: move |_| controller.state.write().ui.new_session_open = false,
                    "×"
                }
            }
            div { class: "settings-content session-source-list",
                button {
                    class: "session-source-card",
                    r#type: "button",
                    disabled,
                    onclick: move |_| {
                        spawn(async move {
                            if controller.create_session().await.is_some() {
                                controller.state.write().ui.new_session_open = false;
                            }
                        });
                    },
                    span { class: "session-source-card__mark", "O" }
                    span { class: "session-source-card__copy",
                        strong { "Orchestral" }
                        small { "内置通用 Agent" }
                    }
                    span { class: "session-source-card__action", "创建" }
                }
                for connector in create_connectors {
                    {
                        let connector_for_click = connector.clone();
                        rsx! {
                            button {
                                class: "session-source-card",
                                key: "{connector.connector_id}",
                                r#type: "button",
                                disabled,
                                onclick: move |_| {
                                    let connector = connector_for_click.clone();
                                    spawn(async move {
                                        controller.create_agent_session(connector).await;
                                    });
                                },
                                span { class: "session-source-card__mark", "A" }
                                span { class: "session-source-card__copy",
                                    strong { "{connector.display_name}" }
                                    small { "{connector.agent_family}" }
                                }
                                span { class: "session-source-card__action", "创建" }
                            }
                        }
                    }
                }
                if let Some(error) = state.connectors.error {
                    p { class: "settings-error", "外部 Agent 暂不可用：{error}" }
                }
            }
        }
    }
}

#[component]
pub fn SessionActionsPanel() -> Element {
    let mut controller = consume_context::<AppController>();
    let state = controller.state.read().clone();
    if !state.ui.session_actions_open {
        return rsx! {};
    }
    let Some(session) = state.selected_session().cloned() else {
        return rsx! {};
    };
    let Some(connector) = state.selected_connector().cloned() else {
        return rsx! {};
    };
    let connector_id = connector.connector_id.clone();
    let session_id = session.id.clone();
    let disabled = state.ui.composer_busy || !state.connection.online;

    rsx! {
        div {
            class: "settings-backdrop",
            role: "presentation",
            onclick: move |_| controller.state.write().ui.session_actions_open = false,
        }
        dialog { class: "settings-dialog session-control-dialog", open: true,
            header { class: "settings-header",
                div {
                    p { class: "eyebrow", "{connector.display_name}" }
                    h2 { "会话操作" }
                }
                button {
                    class: "icon-button",
                    r#type: "button",
                    aria_label: "关闭会话操作",
                    onclick: move |_| controller.state.write().ui.session_actions_open = false,
                    "×"
                }
            }
            div { class: "settings-content session-action-list",
                for action in connector.actions {
                    ActionCard {
                        key: "{action.action_id}",
                        connector_id: connector_id.clone(),
                        session_id: session_id.clone(),
                        action,
                        disabled,
                    }
                }
            }
        }
    }
}

#[component]
fn ActionCard(
    connector_id: String,
    session_id: String,
    action: AgentSessionActionView,
    disabled: bool,
) -> Element {
    let controller = consume_context::<AppController>();
    let form = action_form(&action.input_schema);
    let mut values = use_signal(BTreeMap::<String, String>::new);
    let mut raw_json = use_signal(|| "{}".to_owned());
    let mut error = use_signal(|| None::<String>);
    let action_id = action.action_id.clone();
    let run_action = action.execution == AgentSessionActionExecutionView::Run;

    rsx! {
        form {
            class: "session-action-card",
            onsubmit: move |event| {
                event.prevent_default();
                let arguments = match action_arguments(&form, &values.read(), &raw_json.read()) {
                    Ok(arguments) => arguments,
                    Err(message) => {
                        error.set(Some(message));
                        return;
                    }
                };
                error.set(None);
                let connector_id = connector_id.clone();
                let session_id = session_id.clone();
                let action_id = action_id.clone();
                spawn(async move {
                    controller
                        .invoke_session_action(
                            connector_id,
                            session_id,
                            action_id,
                            arguments,
                            run_action,
                        )
                        .await;
                });
            },
            div { class: "session-action-card__head",
                div {
                    h3 { "{action.title}" }
                    p { "{action.description}" }
                }
                if matches!(form, ActionForm::NoArguments) {
                    button { class: "dialog-button is-primary", r#type: "submit", disabled, "执行" }
                }
            }
            match &form {
                ActionForm::NoArguments => rsx! {},
                ActionForm::Fields(fields) => rsx! {
                    div { class: "session-action-fields",
                        for field in fields {
                            label { class: "session-action-field", key: "{field.name}",
                                span { "{field.label}" }
                                input {
                                    r#type: "text",
                                    required: field.required,
                                    disabled,
                                    value: values.read().get(&field.name).cloned().unwrap_or_default(),
                                    oninput: {
                                        let name = field.name.clone();
                                        move |event| {
                                            values.write().insert(name.clone(), event.value());
                                        }
                                    },
                                }
                            }
                        }
                        button { class: "dialog-button is-primary", r#type: "submit", disabled, "执行" }
                    }
                },
                ActionForm::RawJson => rsx! {
                    div { class: "session-action-fields",
                        label { class: "session-action-field",
                            span { "JSON 参数" }
                            textarea {
                                disabled,
                                value: raw_json,
                                oninput: move |event| raw_json.set(event.value()),
                            }
                        }
                        button { class: "dialog-button is-primary", r#type: "submit", disabled, "执行" }
                    }
                },
            }
            if let Some(message) = error() {
                p { class: "settings-error", "{message}" }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ActionForm {
    NoArguments,
    Fields(Vec<ActionField>),
    RawJson,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActionField {
    name: String,
    label: String,
    required: bool,
}

fn action_form(schema: &Option<Value>) -> ActionForm {
    let Some(schema) = schema else {
        return ActionForm::NoArguments;
    };
    if schema.get("type").and_then(Value::as_str) != Some("object") {
        return ActionForm::RawJson;
    }
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        return ActionForm::RawJson;
    };
    let required = schema
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    let mut fields = Vec::new();
    for (name, property) in properties {
        if property.get("type").and_then(Value::as_str) != Some("string") {
            return ActionForm::RawJson;
        }
        fields.push(ActionField {
            name: name.clone(),
            label: property
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or(name)
                .to_owned(),
            required: required.contains(&name.as_str()),
        });
    }
    ActionForm::Fields(fields)
}

fn action_arguments(
    form: &ActionForm,
    values: &BTreeMap<String, String>,
    raw_json: &str,
) -> Result<Value, String> {
    match form {
        ActionForm::NoArguments => Ok(Value::Null),
        ActionForm::RawJson => {
            serde_json::from_str(raw_json).map_err(|error| format!("JSON 参数无效：{error}"))
        }
        ActionForm::Fields(fields) => {
            let mut arguments = Map::new();
            for field in fields {
                let value = values
                    .get(&field.name)
                    .map(String::as_str)
                    .map(str::trim)
                    .unwrap_or_default();
                if field.required && value.is_empty() {
                    return Err(format!("请填写 {}", field.label));
                }
                if !value.is_empty() {
                    arguments.insert(field.name.clone(), Value::String(value.to_owned()));
                }
            }
            Ok(Value::Object(arguments))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_schema_string_fields_become_a_provider_neutral_form() {
        let form = action_form(&Some(serde_json::json!({
            "type": "object",
            "required": ["name"],
            "properties": {
                "name": {"type": "string", "title": "New name"}
            }
        })));
        assert_eq!(
            form,
            ActionForm::Fields(vec![ActionField {
                name: "name".to_owned(),
                label: "New name".to_owned(),
                required: true,
            }])
        );

        let arguments = action_arguments(
            &form,
            &BTreeMap::from([("name".to_owned(), "renamed".to_owned())]),
            "",
        )
        .unwrap();
        assert_eq!(arguments, serde_json::json!({"name": "renamed"}));
    }

    #[test]
    fn unsupported_schema_uses_validated_raw_json_instead_of_guessing() {
        let form = action_form(&Some(serde_json::json!({
            "type": "object",
            "properties": {"depth": {"type": "integer"}}
        })));
        assert_eq!(form, ActionForm::RawJson);
        assert!(action_arguments(&form, &BTreeMap::new(), "not-json").is_err());
    }
}
