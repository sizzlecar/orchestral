use dioxus::prelude::*;

use crate::browser::controller::AppController;
use crate::components::pending::PendingPanel;
use crate::components::settings::SettingsPanel;
use crate::components::timeline::ConversationTimeline;
use crate::state::{AuthStatus, LoadStatus, RunState};

#[component]
pub fn AuthScreen() -> Element {
    let controller = consume_context::<AppController>();
    let auth = controller.state.read().auth.clone();
    let (eyebrow, title, body) = match auth.status {
        AuthStatus::Booting => (
            "正在准备",
            "连接 Orchestral",
            "正在恢复这台设备的安全会话…".to_owned(),
        ),
        AuthStatus::Pairing => (
            "安全配对",
            "正在连接这台设备…",
            "请保持此页面打开。配对密钥只会使用一次。".to_owned(),
        ),
        AuthStatus::Error => (
            "无法连接",
            "安全登录没有完成",
            auth.error
                .unwrap_or_else(|| "请检查登录状态后重试。".to_owned()),
        ),
        _ => (
            "尚未配对",
            "从 Orchestral 主机开始",
            auth.error.unwrap_or_else(|| {
                "在主机运行带有 --pair 的 serve 命令，然后用此设备扫描二维码。".to_owned()
            }),
        ),
    };
    let can_retry = matches!(auth.status, AuthStatus::Error);

    rsx! {
        main { class: "auth-screen", aria_live: "polite",
            img {
                class: "auth-screen__mark",
                src: "/icons/icon-192.svg",
                alt: "",
                width: "72",
                height: "72"
            }
            p { class: "eyebrow", "{eyebrow}" }
            h1 { "{title}" }
            p { class: "auth-screen__copy", "{body}" }
            if can_retry {
                button {
                    class: "auth-screen__button",
                    r#type: "button",
                    onclick: move |_| {
                        spawn(async move { controller.bootstrap().await });
                    },
                    "重试"
                }
            }
        }
    }
}

#[component]
pub fn Workspace() -> Element {
    let mut controller = consume_context::<AppController>();
    let state = controller.state.read().clone();
    let drawer_class = if state.ui.drawer_open {
        "sidebar sidebar--open"
    } else {
        "sidebar"
    };
    let selected = state.selected_session().cloned();
    let title = selected
        .as_ref()
        .map(|session| session_title(&state, session))
        .unwrap_or_else(|| "新会话".to_owned());
    let run = state.current_run().cloned();
    let status = run_label(run.as_ref());
    let install_available = state.ui.install_available;

    rsx! {
        a { class: "skip-link", href: "#main-content", "跳到对话" }
        div { class: "app-shell", "data-state": "ready",
            header { class: "mobile-topbar",
                button {
                    class: "icon-button",
                    r#type: "button",
                    aria_label: "打开会话列表",
                    aria_expanded: state.ui.drawer_open,
                    onclick: move |_| {
                        let open = controller.state.read().ui.drawer_open;
                        controller.state.write().ui.drawer_open = !open;
                    },
                    svg { view_box: "0 0 24 24", width: "21", height: "21",
                        path { d: "M4 7h16M4 12h16M4 17h11" }
                    }
                }
                a { class: "mobile-brand", href: "./", aria_label: "Orchestral 首页",
                    img { src: "/icons/favicon.svg", alt: "", width: "28", height: "28" }
                    span { "Orchestral" }
                }
                button {
                    class: "icon-button",
                    r#type: "button",
                    aria_label: "新建会话",
                    onclick: move |_| {
                        spawn(async move { controller.create_session().await; });
                    },
                    svg { view_box: "0 0 24 24", width: "21", height: "21",
                        path { d: "M12 5v14M5 12h14" }
                    }
                }
            }

            div { class: "app-layout",
                if state.ui.drawer_open {
                    button {
                        class: "sidebar-backdrop",
                        r#type: "button",
                        aria_label: "关闭会话列表",
                        onclick: move |_| controller.state.write().ui.drawer_open = false,
                    }
                }
                aside { class: drawer_class, aria_label: "会话导航",
                    div { class: "sidebar__head",
                        a { class: "brand", href: "./",
                            img { src: "/icons/favicon.svg", alt: "", width: "34", height: "34" }
                            span { class: "brand__copy",
                                strong { "Orchestral" }
                                span { "agent workspace" }
                            }
                        }
                        button {
                            class: "new-thread-button",
                            r#type: "button",
                            onclick: move |_| {
                                spawn(async move { controller.create_session().await; });
                            },
                            span { "+" }
                            span { "新建会话" }
                            kbd { "⌘ K" }
                        }
                    }
                    nav { class: "thread-nav", aria_label: "最近会话",
                        div { class: "section-label",
                            span { "最近" }
                            span { class: "section-label__count", "{state.sessions.items.len()}" }
                        }
                        ul { class: "thread-list",
                            for session in state.sessions.items.iter() {
                                {
                                    let session_id = session.id.clone();
                                    let selected = state.sessions.selected_id.as_deref() == Some(&session.id);
                                    let title = session_title(&state, session);
                                    let updated = format_date(session.updated_at_unix_ms);
                                    rsx! {
                                        li { class: "thread-item", key: "{session.id}",
                                            button {
                                                class: "thread-button",
                                                r#type: "button",
                                                aria_current: if selected { "page" } else { "false" },
                                                onclick: move |_| {
                                                    let selected = session_id.clone();
                                                    spawn(async move { controller.load_session(selected).await });
                                                },
                                                span { class: "thread-button__title", "{title}" }
                                                span { class: "thread-button__meta", "{updated}" }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if state.sessions.items.is_empty() && state.sessions.status != LoadStatus::Loading {
                            p { class: "thread-list-empty", "还没有会话。开始一个新任务吧。" }
                        }
                    }
                    footer { class: "sidebar__footer",
                        if install_available {
                            button {
                                class: "sidebar-action",
                                r#type: "button",
                                onclick: move |_| controller.install(),
                                span { "安装到设备" }
                            }
                        }
                        button {
                            class: "sidebar-action",
                            r#type: "button",
                            onclick: move |_| {
                                controller.state.write().ui.settings_open = true;
                                spawn(async move { controller.refresh_devices().await });
                            },
                            span { "设置" }
                        }
                        ConnectionStatus {}
                    }
                }

                main { class: "conversation", id: "main-content", tabindex: "-1",
                    header { class: "conversation-header",
                        div { class: "conversation-header__title",
                            p { class: "eyebrow", "当前会话" }
                            h1 { "{title}" }
                        }
                        output { class: "run-status", "data-state": status.1,
                            span { class: "run-status__pulse", aria_hidden: "true" }
                            span { "{status.0}" }
                        }
                    }
                    ConversationTimeline {}
                    PendingPanel {}
                    Composer {}
                }
            }
        }
        SettingsPanel {}
        if let Some(notice) = state.ui.notice {
            div { class: "toast-region", aria_live: "polite",
                div { class: "toast toast--{notice.tone}", "{notice.message}" }
            }
        }
    }
}

#[component]
fn ConnectionStatus() -> Element {
    let controller = consume_context::<AppController>();
    let connection = controller.state.read().connection.clone();
    let status = if connection.online {
        connection.stream
    } else {
        "offline".to_owned()
    };
    let label = match status.as_str() {
        "offline" => "离线 · 将自动重连".to_owned(),
        "connecting" => "正在连接".to_owned(),
        "reconnecting" => format!("正在重连 · {}", connection.attempt),
        "live" => "实时连接".to_owned(),
        "idle" => "已连接".to_owned(),
        "error" => "连接中断".to_owned(),
        value => value.to_owned(),
    };
    rsx! {
        div { class: "connection-status", "data-state": status, role: "status",
            span { class: "connection-status__dot", aria_hidden: "true" }
            span { class: "connection-status__label", "{label}" }
        }
    }
}

#[component]
fn Composer() -> Element {
    let controller = consume_context::<AppController>();
    let mut draft = use_signal(String::new);
    let state = controller.state.read();
    let active = state.active_run().is_some();
    let disabled = state.ui.composer_busy
        || !state.connection.online
        || state.auth.status != AuthStatus::Authenticated;
    let stopping = state
        .active_run()
        .is_some_and(|run| run.status == "stopping");
    drop(state);
    let placeholder = if active {
        "补充指令（steer）…"
    } else {
        "告诉 Orchestral 你想完成什么…"
    };
    let hint = if active {
        "当前发送会引导正在运行的任务"
    } else {
        "Enter 发送 · Shift + Enter 换行"
    };

    rsx! {
        div { class: "composer-dock",
            form {
                class: "composer-form",
                onsubmit: move |event| {
                    event.prevent_default();
                    let text = draft();
                    if text.trim().is_empty() { return; }
                    draft.set(String::new());
                    spawn(async move { controller.submit(text).await });
                },
                textarea {
                    class: "message-input",
                    rows: "1",
                    maxlength: "20000",
                    value: draft,
                    disabled,
                    placeholder,
                    autocomplete: "off",
                    oninput: move |event| draft.set(event.value()),
                    onkeydown: move |event| {
                        if event.key() == Key::Enter && !event.modifiers().shift() {
                            event.prevent_default();
                            let text = draft();
                            if text.trim().is_empty() { return; }
                            draft.set(String::new());
                            spawn(async move { controller.submit(text).await });
                        }
                    }
                }
                div { class: "composer-actions",
                    if active && !stopping {
                        button {
                            class: "cancel-button",
                            r#type: "button",
                            disabled,
                            onclick: move |_| {
                                spawn(async move { controller.cancel().await });
                            },
                            "停止"
                        }
                    }
                    button { class: "send-button", r#type: "submit", disabled,
                        span { "发送" }
                    }
                }
            }
            p { class: "composer-hint", "{hint}" }
        }
    }
}

fn session_title(state: &crate::state::AppState, session: &crate::model::SessionView) -> String {
    for run_id in &session.run_ids {
        if let Some(text) = state
            .runs
            .get(run_id)
            .and_then(|run| run.messages.iter().find(|message| message.role == "user"))
            .map(|message| {
                message
                    .text
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .filter(|text| !text.is_empty())
        {
            let mut chars = text.chars();
            let short = chars.by_ref().take(42).collect::<String>();
            return if chars.next().is_some() {
                format!("{short}…")
            } else {
                short
            };
        }
    }
    format!("会话 {}", short_id(&session.id))
}

fn short_id(value: &str) -> String {
    value.chars().take(8).collect()
}

fn format_date(milliseconds: i64) -> String {
    let date = js_sys::Date::new(&wasm_bindgen::JsValue::from_f64(milliseconds as f64));
    date.to_locale_string("zh-CN", &wasm_bindgen::JsValue::UNDEFINED)
        .as_string()
        .unwrap_or_default()
}

fn run_label(run: Option<&RunState>) -> (String, &'static str) {
    let Some(run) = run else {
        return ("就绪".to_owned(), "idle");
    };
    let elapsed = run
        .started_at
        .map(|started| ((js_sys::Date::now() - started).max(0.0) / 1_000.0) as u64)
        .map(|seconds| format!(" · {}:{:02}", seconds / 60, seconds % 60))
        .unwrap_or_default();
    match run.status.as_str() {
        "accepted" => (format!("Starting{elapsed}"), "working"),
        "running" => (format!("Working{elapsed}"), "working"),
        "waiting" => (format!("Waiting{elapsed}"), "waiting"),
        "stopping" => (format!("Stopping{elapsed}"), "working"),
        "delivered" => ("完成".to_owned(), "complete"),
        "incomplete" => ("未完整结束".to_owned(), "warning"),
        "cancelled" => ("已取消".to_owned(), "idle"),
        "failed" => ("失败".to_owned(), "error"),
        "loading" => ("正在载入".to_owned(), "working"),
        _ => ("状态待确认".to_owned(), "warning"),
    }
}
