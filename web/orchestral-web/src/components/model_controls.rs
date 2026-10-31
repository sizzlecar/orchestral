use dioxus::prelude::*;

use crate::browser::controller::AppController;
use crate::model_controls::{initial_choice, ModelSettingsView};

#[component]
pub fn ModelSettingsPanel() -> Element {
    let mut controller = consume_context::<AppController>();
    let state = controller.state.read().clone();
    let controls = state.ui.model_controls.clone();
    if !controls.open
        || state.sessions.selected_id.as_deref() != Some(controls.session_key.as_str())
    {
        return rsx! {};
    }
    let active = state.active_run().is_some()
        || state
            .selected_session()
            .and_then(|session| session.state.as_deref())
            .is_some_and(|state| {
                matches!(
                    state,
                    "active" | "busy_elsewhere" | "waiting_input" | "waiting_approval"
                )
            });
    let disabled = controls.busy || state.ui.composer_busy || !state.connection.online || active;
    let external_owner = state
        .selected_session()
        .and_then(|session| session.state.as_deref())
        == Some("busy_elsewhere");
    rsx! {
        div { class: "settings-backdrop", role: "presentation",
            onclick: move |_| { if !controller.state.read().ui.model_controls.busy { controller.state.write().ui.model_controls.close(); } },
        }
        dialog { class: "settings-dialog session-control-dialog model-settings-dialog", open: true, aria_label: "模型与推理强度",
            header { class: "settings-header",
                div { p { class: "eyebrow", "当前会话" } h2 { "模型与推理强度" } }
                button { class: "icon-button", r#type: "button", disabled: controls.busy, aria_label: "关闭模型设置",
                    onclick: move |_| controller.state.write().ui.model_controls.close(), "×"
                }
            }
            div { class: "settings-content session-action-list",
                if external_owner { p { role: "status", "会话由现有终端控制，请在该终端中修改模型设置。" } }
                else if active { p { role: "status", "当前任务正在执行，结束后可修改设置。" } }
                if controls.loading { p { role: "status", "正在读取可用模型和推理选项…" } }
                if let Some(error) = controls.error { p { class: "settings-error", role: "alert", "{error}" } }
                if let Some(view) = controls.view {
                    ModelSettingsFields { key: "{controls.generation}", view, disabled, busy: controls.busy }
                }
                if !controls.loading && !controls.busy {
                    button { class: "dialog-button", r#type: "button",
                        onclick: move |_| { controller.spawn(async move { controller.open_model_controls().await }); }, "刷新可用选项"
                    }
                }
            }
        }
    }
}

#[component]
fn ModelSettingsFields(view: ModelSettingsView, disabled: bool, busy: bool) -> Element {
    let controller = consume_context::<AppController>();
    let initial_model = initial_choice(&view.models, view.profile.model.as_deref());
    let initial_reasoning =
        initial_choice(&view.reasoning, view.profile.reasoning_effort.as_deref());
    let current_model = view
        .profile
        .model
        .clone()
        .unwrap_or_else(|| "服务默认".into());
    let current_reasoning = view
        .profile
        .reasoning_effort
        .clone()
        .unwrap_or_else(|| "default".into());
    let model_known = view
        .models
        .iter()
        .any(|choice| choice.value == current_model || choice.label == current_model);
    let reasoning_known = view
        .reasoning
        .iter()
        .any(|choice| choice.value == current_reasoning);
    let mut model = use_signal(move || initial_model);
    let mut reasoning = use_signal(move || initial_reasoning);
    let mut custom = use_signal(|| false);
    let initial_custom = view.profile.model.clone().unwrap_or_default();
    let mut custom_model = use_signal(move || initial_custom);
    rsx! {
        dl { class: "model-settings-current", aria_label: "当前模型设置",
            dt { "模型" } dd { "{current_model}" }
            dt { "推理强度" } dd { "{current_reasoning}" }
        }
        if let Some(notice) = &view.notice { p { class: "model-settings-notice", "{notice}" } }
        if view.model_supported {
            form { class: "session-action-card",
                onsubmit: move |event| {
                    event.prevent_default();
                    let value = if custom() || model().is_empty() { custom_model().trim().to_owned() } else { model() };
                    if !disabled && !value.is_empty() { controller.spawn(async move { controller.change_model_setting(false, value).await }); }
                },
                label { class: "session-action-field",
                    span { "选择模型" }
                    if !custom() && !view.models.is_empty() {
                        select { aria_label: "选择模型", disabled, value: model, onchange: move |event| model.set(event.value()),
                            if !model_known { option { value: "{current_model}", selected: model() == current_model, disabled: true, "当前：{current_model}" } }
                            for choice in &view.models { option { value: "{choice.value}", selected: model() == choice.value, "{choice.label}" } }
                        }
                    } else {
                        input { aria_label: "输入模型 ID", disabled, required: true, value: custom_model,
                            placeholder: "输入服务支持的模型 ID", oninput: move |event| custom_model.set(event.value()),
                        }
                    }
                }
                if view.allow_custom_model && !view.models.is_empty() {
                    label { class: "model-settings-custom",
                        input { r#type: "checkbox", disabled, checked: custom, onchange: move |event| custom.set(event.checked()) }
                        "手动输入模型 ID"
                    }
                }
                button { class: "dialog-button is-primary", r#type: "submit", disabled,
                    if busy { "正在应用…" } else { "应用模型" }
                }
            }
        }
        if view.reasoning_supported {
            form { class: "session-action-card",
                onsubmit: move |event| {
                    event.prevent_default();
                    let value = reasoning().trim().to_owned();
                    if !disabled && !value.is_empty() { controller.spawn(async move { controller.change_model_setting(true, value).await }); }
                },
                label { class: "session-action-field",
                    span { "推理强度" }
                    if view.reasoning.is_empty() {
                        input { aria_label: "推理强度", disabled, required: true, value: reasoning,
                            placeholder: "输入 Agent 支持的推理设置", oninput: move |event| reasoning.set(event.value()),
                        }
                    } else {
                        select { aria_label: "推理强度", disabled, value: reasoning, onchange: move |event| reasoning.set(event.value()),
                            if !reasoning_known { option { value: "{current_reasoning}", selected: reasoning() == current_reasoning, disabled: true, "当前：{current_reasoning}" } }
                            for choice in &view.reasoning { option { value: "{choice.value}", selected: reasoning() == choice.value, "{choice.label}" } }
                        }
                    }
                }
                p { class: "model-settings-notice", "default 使用服务默认设置。修改将在后续请求中生效。" }
                button { class: "dialog-button is-primary", r#type: "submit", disabled,
                    if busy { "正在应用…" } else { "应用推理强度" }
                }
            }
        }
    }
}
