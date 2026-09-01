use std::collections::BTreeMap;

use dioxus::prelude::*;
use gloo_timers::future::TimeoutFuture;

use crate::browser::controller::AppController;
use crate::components::pending::PendingPanel;
use crate::components::session_control::{NewSessionPanel, SessionActionsPanel};
use crate::components::settings::SettingsPanel;
use crate::components::timeline::ConversationTimeline;
use crate::model::SessionView;
use crate::state::{AppState, AuthStatus, LoadStatus, RunState};

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
    let session_source = selected
        .as_ref()
        .and_then(|session| session.connector_id.as_deref())
        .map(|connector_id| connector_display_name(&state, connector_id))
        .unwrap_or_else(|| "Orchestral".to_owned());
    let run = state.current_run().cloned();
    let has_session_actions = state
        .selected_connector()
        .is_some_and(|connector| !connector.actions.is_empty());
    let install_available = state.ui.install_available;
    let session_groups = group_sessions(&state);

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
                        let mut state = controller.state.write();
                        state.ui.drawer_open = false;
                        state.ui.new_session_open = true;
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
                                let mut state = controller.state.write();
                                state.ui.drawer_open = false;
                                state.ui.new_session_open = true;
                            },
                            span { "+" }
                            span { "新建会话" }
                            kbd { "⌘ K" }
                        }
                    }
                    nav { class: "thread-nav", aria_label: "最近会话",
                        div { class: "section-label",
                            span { "会话" }
                            span { class: "section-label__count", "{state.sessions.items.len()}" }
                        }
                        div { class: "thread-groups",
                            for group in session_groups {
                                section {
                                    class: "thread-group",
                                    key: "{group.key}",
                                    aria_label: "{group.label} 会话",
                                    div { class: "thread-group__label",
                                        span { "{group.label}" }
                                        span { "{group.sessions.len()}" }
                                    }
                                    ul { class: "thread-list",
                                        for session in group.sessions {
                                            {
                                                let session_key = session.key();
                                                let selected = state.sessions.selected_id.as_deref() == Some(session_key.as_str());
                                                let title = session_title(&state, &session);
                                                let updated = format_date(session.updated_at_unix_ms);
                                                rsx! {
                                                    li { class: "thread-item", key: "{session_key}",
                                                        button {
                                                            class: "thread-button",
                                                            r#type: "button",
                                                            aria_current: if selected { "page" } else { "false" },
                                                            onclick: move |_| {
                                                                let selected = session_key.clone();
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
                                    if group.has_more || group.load_error.is_some() {
                                        if let Some(connector_id) = group.connector_id.clone() {
                                            button {
                                                class: "thread-group__more",
                                                r#type: "button",
                                                disabled: group.loading_more || !state.connection.online,
                                                onclick: move |_| {
                                                    let connector_id = connector_id.clone();
                                                    spawn(async move {
                                                        controller.load_more_agent_sessions(connector_id).await;
                                                    });
                                                },
                                                if group.loading_more {
                                                    "正在加载…"
                                                } else if group.load_error.is_some() {
                                                    "重试加载"
                                                } else {
                                                    "加载更多"
                                                }
                                            }
                                        }
                                    }
                                    if let Some(error) = group.load_error.as_ref() {
                                        p { class: "thread-group__error", "{error}" }
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
                            p { class: "eyebrow", "{session_source} 会话" }
                            h1 { "{title}" }
                        }
                        div { class: "conversation-header__controls",
                            if has_session_actions {
                                button {
                                    class: "session-actions-button",
                                    r#type: "button",
                                    aria_label: "打开会话操作",
                                    onclick: move |_| controller.state.write().ui.session_actions_open = true,
                                    "操作"
                                }
                            }
                            RunStatusBadge { run }
                        }
                    }
                    ConversationTimeline {}
                    PendingPanel {}
                    Composer {}
                }
            }
        }
        SettingsPanel {}
        NewSessionPanel {}
        SessionActionsPanel {}
        if let Some(notice) = state.ui.notice {
            div { class: "toast-region", aria_live: "polite",
                div { class: "toast toast--{notice.tone}",
                    span { class: "toast__message", "{notice.message}" }
                    button {
                        class: "toast__dismiss",
                        r#type: "button",
                        aria_label: "关闭提示",
                        onclick: move |_| {
                            controller.state.write().ui.dismiss_notice(notice.id);
                        },
                        "×"
                    }
                }
            }
        }
    }
}

#[component]
fn RunStatusBadge(run: Option<RunState>) -> Element {
    let mut now = use_signal(js_sys::Date::now);
    use_future(move || async move {
        loop {
            TimeoutFuture::new(1_000).await;
            now.set(js_sys::Date::now());
        }
    });
    let status = run_label(run.as_ref(), now());

    rsx! {
        output { class: "run-status", "data-state": status.1,
            span { class: "run-status__pulse", aria_hidden: "true" }
            span { "{status.0}" }
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
        "observing" => "正在自动刷新 Agent".to_owned(),
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
    let recoverable = state.recoverable_run().is_some();
    let input_disabled =
        !state.connection.online || state.auth.status != AuthStatus::Authenticated || recoverable;
    let control_disabled = state.ui.composer_busy
        || !state.connection.online
        || state.auth.status != AuthStatus::Authenticated;
    let action_disabled = control_disabled || input_disabled;
    let stopping = state
        .active_run()
        .is_some_and(|run| run.status == "stopping");
    drop(state);
    let placeholder = if recoverable {
        "连接中断，恢复后可继续…"
    } else if active {
        "补充指令（steer）…"
    } else {
        "告诉 Orchestral 你想完成什么…"
    };
    let hint = if recoverable {
        "原生 Agent 状态未知，恢复前不会重复执行"
    } else if active {
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
                    disabled: input_disabled,
                    placeholder,
                    autocomplete: "off",
                    autocapitalize: "sentences",
                    inputmode: "text",
                    enterkeyhint: "send",
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
                div { class: "composer-form__actions",
                    if recoverable {
                        button {
                            class: "send-button recover-button",
                            r#type: "button",
                            disabled: control_disabled,
                            onclick: move |_| {
                                spawn(async move { controller.recover_current_run().await });
                            },
                            "恢复连接"
                        }
                    }
                    if active && !stopping {
                        button {
                            class: "cancel-button",
                            r#type: "button",
                            disabled: action_disabled,
                            onclick: move |_| {
                                spawn(async move { controller.cancel().await });
                            },
                            "停止"
                        }
                    }
                    button { class: "send-button", r#type: "submit", disabled: action_disabled,
                        span { "发送" }
                    }
                }
            }
            p { class: "composer-hint", "{hint}" }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionGroup {
    key: String,
    label: String,
    connector_id: Option<String>,
    sessions: Vec<SessionView>,
    has_more: bool,
    loading_more: bool,
    load_error: Option<String>,
}

fn group_sessions(state: &AppState) -> Vec<SessionGroup> {
    let mut grouped = BTreeMap::<Option<String>, Vec<SessionView>>::new();
    grouped.entry(None).or_default();
    for connector in &state.connectors.items {
        if connector.capabilities.list {
            grouped
                .entry(Some(connector.connector_id.clone()))
                .or_default();
        }
    }
    for session in &state.sessions.items {
        grouped
            .entry(session.connector_id.clone())
            .or_default()
            .push(session.clone());
    }

    let mut groups = grouped
        .into_iter()
        .map(|(connector_id, mut sessions)| {
            sessions.sort_by(|left, right| {
                right
                    .updated_at_unix_ms
                    .cmp(&left.updated_at_unix_ms)
                    .then_with(|| left.key().cmp(&right.key()))
            });
            let (key, label, page) = match connector_id.as_deref() {
                Some(connector_id) => (
                    connector_id.to_owned(),
                    connector_display_name(state, connector_id),
                    state.sessions.connector_pages.get(connector_id),
                ),
                None => ("orchestral".to_owned(), "Orchestral".to_owned(), None),
            };
            SessionGroup {
                key,
                label,
                connector_id,
                sessions,
                has_more: page.is_some_and(|page| page.next_cursor.is_some()),
                loading_more: page.is_some_and(|page| page.loading_more),
                load_error: page.and_then(|page| page.error.clone()),
            }
        })
        .collect::<Vec<_>>();
    groups.sort_by(|left, right| {
        let left_native = left.key == "orchestral";
        let right_native = right.key == "orchestral";
        right_native
            .cmp(&left_native)
            .then_with(|| left.label.to_lowercase().cmp(&right.label.to_lowercase()))
            .then_with(|| left.key.cmp(&right.key))
    });
    groups
}

fn session_title(state: &AppState, session: &SessionView) -> String {
    if let Some(title) = session
        .title
        .as_ref()
        .filter(|title| !title.trim().is_empty())
    {
        return title.clone();
    }
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

fn connector_display_name(state: &AppState, connector_id: &str) -> String {
    state
        .connectors
        .items
        .iter()
        .find(|connector| connector.connector_id == connector_id)
        .map(|connector| connector.display_name.trim())
        .filter(|label| !label.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| {
            let fallback = connector_id.split('/').next().unwrap_or(connector_id);
            let mut chars = fallback.chars();
            chars
                .next()
                .map(|first| first.to_uppercase().chain(chars).collect())
                .unwrap_or_else(|| "Agent".to_owned())
        })
}

fn format_date(milliseconds: i64) -> String {
    let date = js_sys::Date::new(&wasm_bindgen::JsValue::from_f64(milliseconds as f64));
    date.to_locale_string("zh-CN", &wasm_bindgen::JsValue::UNDEFINED)
        .as_string()
        .unwrap_or_default()
}

fn run_label(run: Option<&RunState>, now: f64) -> (String, &'static str) {
    let Some(run) = run else {
        return ("就绪".to_owned(), "idle");
    };
    let elapsed = run
        .started_at
        .map(|started| ((now - started).max(0.0) / 1_000.0) as u64)
        .map(|seconds| format!(" · {}:{:02}", seconds / 60, seconds % 60))
        .unwrap_or_default();
    match run.status.as_str() {
        "submitting" => (format!("发送中{elapsed}"), "working"),
        "accepted" => (format!("Starting{elapsed}"), "working"),
        "running" => (format!("Working{elapsed}"), "working"),
        "waiting" => (format!("Waiting{elapsed}"), "waiting"),
        "stopping" => (format!("Stopping{elapsed}"), "working"),
        "delivered" => ("完成".to_owned(), "complete"),
        "incomplete" => ("未完整结束".to_owned(), "warning"),
        "cancelled" => ("已取消".to_owned(), "idle"),
        "failed" => ("失败".to_owned(), "error"),
        "loading" => ("正在载入".to_owned(), "working"),
        "unknown" => ("连接待恢复".to_owned(), "warning"),
        _ => ("状态待确认".to_owned(), "warning"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{AgentConnectorView, AgentSessionCapabilitiesView};
    use crate::state::AgentSessionListState;

    fn session(id: &str, connector_id: Option<&str>, updated_at_unix_ms: i64) -> SessionView {
        SessionView {
            id: id.to_owned(),
            created_at_unix_ms: updated_at_unix_ms,
            updated_at_unix_ms,
            run_ids: Vec::new(),
            connector_id: connector_id.map(str::to_owned),
            title: None,
            preview: None,
            cwd: None,
            state: None,
        }
    }

    fn connector(connector_id: &str, display_name: &str) -> AgentConnectorView {
        AgentConnectorView {
            connector_id: connector_id.to_owned(),
            display_name: display_name.to_owned(),
            agent_family: "coding-agent".to_owned(),
            capabilities: AgentSessionCapabilitiesView {
                list: true,
                read: true,
                create: true,
            },
            actions: Vec::new(),
        }
    }

    #[test]
    fn sidebar_groups_native_and_agent_sessions_and_sorts_each_group_by_recency() {
        let mut state = AppState::new(true);
        state.connectors.items = vec![
            connector("codex/local", "Codex"),
            connector("claude/local", "Claude"),
        ];
        state.sessions.items = vec![
            session("native-old", None, 10),
            session("same-id", Some("codex/local"), 20),
            session("same-id", Some("claude/local"), 50),
            session("codex-new", Some("codex/local"), 40),
            session("native-new", None, 30),
        ];
        state.sessions.connector_pages.insert(
            "codex/local".to_owned(),
            AgentSessionListState {
                next_cursor: Some("next-codex-page".to_owned()),
                loading_more: false,
                error: None,
            },
        );

        let groups = group_sessions(&state);

        assert_eq!(
            groups
                .iter()
                .map(|group| group.label.as_str())
                .collect::<Vec<_>>(),
            vec!["Orchestral", "Claude", "Codex"]
        );
        assert_eq!(
            groups[0]
                .sessions
                .iter()
                .map(|session| session.id.as_str())
                .collect::<Vec<_>>(),
            vec!["native-new", "native-old"]
        );
        assert_eq!(
            groups[2]
                .sessions
                .iter()
                .map(|session| session.id.as_str())
                .collect::<Vec<_>>(),
            vec!["codex-new", "same-id"]
        );
        assert_ne!(groups[1].sessions[0].key(), groups[2].sessions[1].key());
        assert!(groups[2].has_more);
        assert_eq!(groups[2].connector_id.as_deref(), Some("codex/local"));
        assert!(!groups[1].has_more);
        assert!(groups[0].connector_id.is_none());
    }

    #[test]
    fn empty_connector_group_keeps_its_initial_load_error_and_retry_state_visible() {
        let mut state = AppState::new(true);
        state.connectors.items = vec![connector("codex/local", "Codex")];
        state.sessions.connector_pages.insert(
            "codex/local".to_owned(),
            AgentSessionListState {
                next_cursor: None,
                loading_more: false,
                error: Some("Codex 暂时不可用".to_owned()),
            },
        );

        let groups = group_sessions(&state);
        let codex = groups.iter().find(|group| group.label == "Codex").unwrap();

        assert!(codex.sessions.is_empty());
        assert_eq!(codex.load_error.as_deref(), Some("Codex 暂时不可用"));
        assert!(!codex.has_more);
        assert_eq!(codex.connector_id.as_deref(), Some("codex/local"));
    }
}
