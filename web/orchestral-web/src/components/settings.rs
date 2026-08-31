use dioxus::prelude::*;

use crate::browser::controller::AppController;
use crate::state::LoadStatus;

#[component]
pub fn SettingsPanel() -> Element {
    let mut controller = consume_context::<AppController>();
    let state = controller.state.read().clone();
    if !state.ui.settings_open {
        return rsx! {};
    }
    let theme = controller.preferences.read().theme.clone();
    let gateway_identity = state.auth.me.as_ref().is_some_and(|me| {
        me.get("auth_mode").and_then(|value| value.as_str()) == Some("gateway_jwt")
    });
    let gateway_label = state
        .auth
        .me
        .as_ref()
        .and_then(|me| me.get("attributes"))
        .and_then(|claims| claims.get("email"))
        .and_then(|email| email.as_str())
        .unwrap_or("由访问网关验证")
        .to_owned();
    rsx! {
        div {
            class: "settings-backdrop",
            role: "presentation",
            onclick: move |_| controller.state.write().ui.settings_open = false,
        }
        dialog { class: "settings-dialog", open: true,
            header { class: "settings-header",
                div {
                    p { class: "eyebrow", "设备与偏好" }
                    h2 { "设置" }
                }
                button {
                    class: "icon-button",
                    r#type: "button",
                    aria_label: "关闭设置",
                    onclick: move |_| controller.state.write().ui.settings_open = false,
                    "×"
                }
            }
            div { class: "settings-content",
                section { class: "settings-section",
                    h3 { "外观" }
                    select {
                        class: "settings-select",
                        value: theme,
                        onchange: move |event| controller.set_theme(event.value()),
                        option { value: "auto", "跟随系统" }
                        option { value: "light", "浅色" }
                        option { value: "dark", "深色" }
                    }
                }
                section { class: "settings-section",
                    h3 { if gateway_identity { "登录身份" } else { "已配对设备" } }
                    if gateway_identity {
                        p { "{gateway_label}" }
                    } else if state.devices.status == LoadStatus::Loading {
                        p { "正在载入设备…" }
                    } else if let Some(error) = state.devices.error.as_ref() {
                        p { class: "settings-error", "{error}" }
                    } else {
                        ul { class: "device-list",
                            for device in state.devices.items {
                                {
                                    let id = device.id.clone();
                                    let current = device.current;
                                    rsx! {
                                        li { class: "device-item", key: "{device.id}",
                                            span { class: "device-item__copy",
                                                strong { "{device.name}" }
                                                small { if current { "当前设备" } else { "已配对" } }
                                            }
                                            button {
                                                class: "device-item__revoke",
                                                r#type: "button",
                                                onclick: move |_| {
                                                    let device_id = id.clone();
                                                    spawn(async move {
                                                        controller.revoke_device(device_id, current).await;
                                                    });
                                                },
                                                if current { "撤销当前设备" } else { "撤销" }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
