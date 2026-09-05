use std::collections::BTreeMap;

use dioxus::prelude::*;
use gloo_timers::future::TimeoutFuture;

use crate::browser::controller::AppController;
use crate::components::pending::PendingPanel;
use crate::components::session_control::{NewSessionPanel, SessionActionsPanel};
use crate::components::settings::SettingsPanel;
use crate::components::timeline::ConversationTimeline;
use crate::model::{
    AgentApprovalMode, AgentFilesystemAccess, AgentNetworkAccess, AgentSessionPermissions,
    SessionView,
};
use crate::state::{
    is_terminal, timeline_blocks_for_session, AppState, AuthStatus, LoadStatus, RunState,
    TimelineBlock, TimelineItem,
};

const SIDEBAR_SESSIONS_PER_PAGE: usize = 10;

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
    let session_metadata = selected.as_ref().map(session_metadata).unwrap_or_default();
    let run = state.current_run().cloned();
    let native_session_state = selected.as_ref().and_then(|session| {
        session
            .connector_id
            .as_ref()
            .and_then(|_| session.state.clone())
    });
    let native_session_updated_at = selected.as_ref().and_then(|session| {
        session
            .connector_id
            .as_ref()
            .map(|_| session.updated_at_unix_ms)
    });
    let native_turn_status = selected.as_ref().and_then(|session| {
        let history_id = session.history_run_id()?;
        state
            .runs
            .get(&history_id)
            .and_then(|history| history.history_latest_turn_status.clone())
    });
    let has_session_actions = state
        .selected_connector()
        .is_some_and(|connector| !connector.actions.is_empty());
    let install_available = state.ui.install_available;
    let session_groups = group_sessions(&state);
    let selected_tab = selected
        .as_ref()
        .and_then(|session| session.connector_id.clone())
        .unwrap_or_else(|| "orchestral".to_owned());
    let active_tab = state
        .ui
        .session_tab
        .as_ref()
        .filter(|tab| session_groups.iter().any(|group| group.key == **tab))
        .cloned()
        .filter(|_| state.ui.session_tab.is_some())
        .unwrap_or(selected_tab);
    let active_group = session_groups
        .iter()
        .find(|group| group.key == active_tab)
        .cloned()
        .or_else(|| session_groups.first().cloned());
    let (visible_sessions, session_page, loaded_page_count) = active_group
        .as_ref()
        .map(|group| paginated_sessions(group, state.ui.session_page))
        .unwrap_or_else(|| (Vec::new(), 0, 1));

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
                        div { class: "thread-tabs", role: "tablist", aria_label: "会话来源",
                            for group in session_groups.clone() {
                                {
                                    let tab_key = group.key.clone();
                                    let tab_active = active_group
                                        .as_ref()
                                        .is_some_and(|active| active.key == group.key);
                                    rsx! {
                                        button {
                                            class: if tab_active { "thread-tab is-active" } else { "thread-tab" },
                                            key: "{group.key}",
                                            r#type: "button",
                                            role: "tab",
                                            aria_selected: tab_active,
                                            onclick: move |_| {
                                                let mut state = controller.state.write();
                                                state.ui.session_tab = Some(tab_key.clone());
                                                state.ui.session_page = 0;
                                            },
                                            span { "{group.label}" }
                                            span { class: "thread-tab__count", "{group.sessions.len()}" }
                                        }
                                    }
                                }
                            }
                        }
                        div { class: "thread-groups",
                            if let Some(group) = active_group.clone() {
                                section {
                                    class: "thread-group",
                                    key: "{group.key}",
                                    aria_label: "{group.label} 会话",
                                    div { class: "thread-group__toolbar",
                                        span { "最近会话" }
                                        {
                                            let refresh_connector_id = group.connector_id.clone();
                                            rsx! {
                                                button {
                                                    class: "thread-group__refresh",
                                                    r#type: "button",
                                                    aria_label: "刷新 {group.label} 会话",
                                                    disabled: group.loading_more || state.sessions.status == LoadStatus::Loading,
                                                    onclick: move |_| {
                                                        let connector_id = refresh_connector_id.clone();
                                                        spawn(async move {
                                                            controller.refresh_session_group(connector_id).await;
                                                        });
                                                    },
                                                    if group.loading_more || state.sessions.status == LoadStatus::Loading {
                                                        "刷新中…"
                                                    } else {
                                                        "刷新"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    ul { class: "thread-list",
                                        for session in visible_sessions.clone() {
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
                                    if group.sessions.is_empty() && group.load_error.is_none() {
                                        p { class: "thread-list-empty", "这个分类还没有会话。" }
                                    }
                                    if !group.sessions.is_empty() || group.has_more || group.load_error.is_some() {
                                        div { class: "thread-pagination", aria_label: "会话分页",
                                            button {
                                                class: "thread-pagination__button",
                                                r#type: "button",
                                                disabled: session_page == 0,
                                                onclick: move |_| {
                                                    let page = controller.state.read().ui.session_page;
                                                    controller.state.write().ui.session_page = page.saturating_sub(1);
                                                },
                                                "上一页"
                                            }
                                            span { class: "thread-pagination__status",
                                                if group.has_more { "{session_page + 1} / {loaded_page_count}+" }
                                                else { "{session_page + 1} / {loaded_page_count}" }
                                            }
                                            button {
                                                class: "thread-pagination__button",
                                                r#type: "button",
                                                disabled: group.loading_more
                                                    || (session_page + 1 >= loaded_page_count && !group.has_more && group.load_error.is_none()),
                                                onclick: move |_| {
                                                    if session_page + 1 < loaded_page_count {
                                                        controller.state.write().ui.session_page = session_page + 1;
                                                        return;
                                                    }
                                                    let Some(connector_id) = group.connector_id.clone() else { return; };
                                                    let tab_key = group.key.clone();
                                                    let prior_count = group.sessions.len();
                                                    spawn(async move {
                                                        controller.load_more_agent_sessions(connector_id.clone()).await;
                                                        let mut state = controller.state.write();
                                                        let new_count = state.sessions.items.iter().filter(|session| {
                                                            session.connector_id.as_deref() == Some(connector_id.as_str())
                                                        }).count();
                                                        if state.ui.session_tab.as_deref() == Some(tab_key.as_str())
                                                            && new_count > prior_count
                                                        {
                                                            state.ui.session_page = session_page + 1;
                                                        }
                                                    });
                                                },
                                                if group.loading_more { "加载中…" }
                                                else { "下一页" }
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
                            RunStatusBadge {
                                run,
                                native_session_state,
                                native_session_updated_at,
                                native_turn_status,
                            }
                        }
                        if !session_metadata.is_empty() {
                            div { class: "conversation-header__meta", aria_label: "Agent 会话配置",
                                for (label, value) in session_metadata {
                                    div {
                                        class: "session-meta-chip",
                                        title: "{label}：{value}",
                                        span { class: "session-meta-chip__label", "{label}" }
                                        span { class: "session-meta-chip__value", "{value}" }
                                    }
                                }
                            }
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
fn RunStatusBadge(
    run: Option<RunState>,
    native_session_state: Option<String>,
    native_session_updated_at: Option<i64>,
    native_turn_status: Option<String>,
) -> Element {
    let mut now = use_signal(js_sys::Date::now);
    use_future(move || async move {
        loop {
            TimeoutFuture::new(1_000).await;
            now.set(js_sys::Date::now());
        }
    });
    let status = session_run_label(
        run.as_ref(),
        native_session_state.as_deref(),
        native_session_updated_at,
        native_turn_status.as_deref(),
        now(),
    );

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
    let mut attachments = use_signal(Vec::<crate::model::UploadedArtifact>::new);
    let mut uploads_in_flight = use_signal(|| 0_usize);
    let mut upload_error = use_signal(|| None::<String>);
    let mut saved_drafts =
        use_signal(BTreeMap::<String, (String, Vec<crate::model::UploadedArtifact>)>::new);
    let mut draft_session = use_signal(|| {
        controller
            .state
            .read()
            .sessions
            .selected_id
            .clone()
            .unwrap_or_default()
    });
    use_effect(move || {
        let selected = controller
            .state
            .read()
            .sessions
            .selected_id
            .clone()
            .unwrap_or_default();
        let previous = draft_session.peek().clone();
        if selected != previous {
            saved_drafts
                .write()
                .insert(previous, (draft.peek().clone(), attachments.peek().clone()));
            let (text, files) = saved_drafts.write().remove(&selected).unwrap_or_default();
            draft.set(text);
            attachments.set(files);
            draft_session.set(selected);
            upload_error.set(None);
        }
    });
    let state = controller.state.read();
    let sending = state.ui.composer_busy;
    let confirming = state.ui.outbox_flushing;
    let online = state.connection.online;
    let pending_count = state.pending_requests().len();
    let active = state.active_run().is_some();
    let manual_recovery = state
        .current_run()
        .filter(|run| run.recovery_is_manual())
        .map(|run| run.recovery_allows_new_run());
    let recoverable = state.recoverable_run().is_some();
    let supervision = state
        .current_run()
        .and_then(|run| run.supervision.as_ref())
        .cloned();
    let supervision_blocked = supervision.is_some();
    let input_disabled = !state.connection.online
        || state.auth.status != AuthStatus::Authenticated
        || recoverable
        || supervision_blocked;
    let control_disabled = state.ui.composer_busy
        || !state.connection.online
        || state.auth.status != AuthStatus::Authenticated;
    let action_disabled =
        control_disabled || input_disabled || confirming || uploads_in_flight() > 0;
    let stopping = state
        .active_run()
        .is_some_and(|run| run.status == "stopping");
    drop(state);
    let placeholder = if !online {
        "离线时也可以先写好草稿…"
    } else if supervision_blocked {
        "任务已停滞，Host 正在安全终止…"
    } else if manual_recovery == Some(true) {
        "上次任务已中断，可发送新消息继续…"
    } else if manual_recovery == Some(false) {
        "该任务需要在 Host 端人工恢复…"
    } else if recoverable {
        "正在自动恢复，恢复后可继续…"
    } else if active {
        "补充说明或调整方向…"
    } else {
        "告诉 Orchestral 你想完成什么…"
    };
    let hint = if sending {
        "正在发送，请稍候…".to_owned()
    } else if !online {
        "当前离线，草稿会保留；恢复连接后可发送".to_owned()
    } else if confirming {
        "正在确认上一条消息的发送状态，可以继续编辑草稿".to_owned()
    } else if pending_count > 0 {
        format!("有 {pending_count} 项待处理，请在上方回复或批准")
    } else if let Some(supervision) = supervision {
        supervision.reason
    } else if manual_recovery == Some(true) {
        "上次任务不会自动重试；本次发送将创建新任务".to_owned()
    } else if manual_recovery == Some(false) {
        "自动恢复已停止，不会重复执行".to_owned()
    } else if recoverable {
        "正在自动恢复 Agent 状态，期间不会重复执行".to_owned()
    } else if active {
        "当前发送会引导正在运行的任务".to_owned()
    } else {
        "Enter 发送 · Shift + Enter 换行".to_owned()
    };

    let mut send_draft = move || {
        if action_disabled {
            return;
        }
        let text = draft();
        let selected = attachments();
        let submission_session = draft_session();
        if text.trim().is_empty() && selected.is_empty() {
            return;
        }
        upload_error.set(None);
        // Commit the composer locally before the Host resolves R2
        // content and starts the Agent. That remote acknowledgement
        // can take seconds and must not make the UI look frozen.
        draft.set(String::new());
        attachments.set(Vec::new());
        spawn(async move {
            if !controller.submit(text.clone(), selected.clone()).await {
                if draft_session() != submission_session {
                    saved_drafts
                        .write()
                        .insert(submission_session, (text, selected));
                    return;
                }
                // Restore a definitively rejected submission only
                // when no newer draft would be overwritten.
                if draft().is_empty() {
                    draft.set(text);
                }
                if attachments().is_empty() {
                    attachments.set(selected);
                }
            }
        });
    };

    rsx! {
        div { class: "composer-dock",
            if !attachments().is_empty() || uploads_in_flight() > 0 || upload_error().is_some() {
                div { class: "composer-attachments", aria_live: "polite",
                    for (index, attachment) in attachments().iter().enumerate() {
                        div {
                            class: "composer-attachment",
                            key: "{attachment.artifact_ref}-{index}",
                            a {
                                href: attachment.download_url.clone(),
                                target: "_blank",
                                rel: "noopener",
                                span { class: "composer-attachment__name", "{attachment.file_name}" }
                                span { class: "composer-attachment__meta", "{format_bytes(attachment.byte_size)}" }
                            }
                            button {
                                class: "composer-attachment__remove",
                                r#type: "button",
                                aria_label: "移除附件",
                                onclick: move |_| {
                                    if index < attachments.read().len() {
                                        attachments.write().remove(index);
                                    }
                                },
                                "×"
                            }
                        }
                    }
                    if uploads_in_flight() > 0 {
                        span { class: "composer-uploading", "正在上传 {uploads_in_flight()} 个文件…" }
                    }
                    if let Some(error) = upload_error() {
                        span { class: "composer-upload-error", "{error}" }
                    }
                }
            }
            form {
                class: "composer-form",
                onsubmit: move |event| {
                    event.prevent_default();
                    send_draft();
                },
                input {
                    id: "composer-file-input",
                    class: "composer-file-input",
                    r#type: "file",
                    multiple: true,
                    disabled: action_disabled,
                    onchange: move |event| {
                        let files = event.files();
                        if files.is_empty() { return; }
                        upload_error.set(None);
                        let available = 10_usize.saturating_sub(attachments.read().len());
                        if files.len() > available {
                            upload_error.set(Some("每条消息最多 10 个附件".to_owned()));
                        }
                        for file in files.into_iter().take(available) {
                            let upload_session = draft_session();
                            uploads_in_flight += 1;
                            spawn(async move {
                                match controller.upload_artifact(file).await {
                                    Ok(artifact) if draft_session() != upload_session => {
                                        saved_drafts.write().entry(upload_session).or_default().1.push(artifact);
                                    }
                                    Ok(artifact) => {
                                        if !attachments
                                            .read()
                                            .iter()
                                            .any(|item| item.artifact_ref == artifact.artifact_ref)
                                        {
                                            attachments.write().push(artifact);
                                        }
                                    }
                                    Err(error) => upload_error.set(Some(error)),
                                }
                                uploads_in_flight -= 1;
                            });
                        }
                    }
                }
                label {
                    class: "attach-button",
                    r#for: "composer-file-input",
                    title: "添加文件",
                    aria_label: "添加文件",
                    "＋"
                }
                textarea {
                    class: "message-input",
                    rows: "1",
                    maxlength: "20000",
                    value: draft,
                    disabled: sending,
                    aria_label: "消息草稿",
                    placeholder,
                    autocomplete: "off",
                    autocapitalize: "sentences",
                    inputmode: "text",
                    enterkeyhint: "send",
                    oninput: move |event| draft.set(event.value()),
                    onkeydown: move |event| {
                        if event.key() == Key::Enter && !event.modifiers().shift() && !event.is_composing() {
                            event.prevent_default();
                            send_draft();
                        }
                    }
                }
                div { class: "composer-form__actions",
                    if active && !stopping {
                        button {
                            class: "cancel-button",
                            r#type: "button",
                            disabled: control_disabled,
                            onclick: move |_| {
                                spawn(async move { controller.cancel().await });
                            },
                            "停止"
                        }
                    }
                    button { class: "send-button", r#type: "submit", disabled: action_disabled || (draft.read().trim().is_empty() && attachments.read().is_empty()),
                        aria_label: if sending { "正在发送" } else { "发送消息" },
                        span { if sending { "发送中" } else { "发送" } }
                    }
                }
            }
            p { class: "composer-hint", "{hint}" }
        }
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
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

fn paginated_sessions(
    group: &SessionGroup,
    requested_page: usize,
) -> (Vec<SessionView>, usize, usize) {
    let page_count = group
        .sessions
        .len()
        .div_ceil(SIDEBAR_SESSIONS_PER_PAGE)
        .max(1);
    let page = requested_page.min(page_count.saturating_sub(1));
    let start = page.saturating_mul(SIDEBAR_SESSIONS_PER_PAGE);
    let end = (start + SIDEBAR_SESSIONS_PER_PAGE).min(group.sessions.len());
    (group.sessions[start..end].to_vec(), page, page_count)
}

fn session_title(state: &AppState, session: &SessionView) -> String {
    if let Some(text) = timeline_blocks_for_session(state, session)
        .into_iter()
        .rev()
        .find_map(|entry| match entry.block {
            TimelineBlock::Entry(TimelineItem::Message(message)) if message.role == "user" => {
                Some(compact_session_title(&message.text))
            }
            _ => None,
        })
        .filter(|text| !text.is_empty())
    {
        return text;
    }
    if let Some(preview) = session
        .preview
        .as_ref()
        .map(|preview| compact_session_title(preview))
        .filter(|preview| !preview.is_empty())
    {
        return preview;
    }
    if let Some(title) = session
        .title
        .as_ref()
        .map(|title| compact_session_title(title))
        .filter(|title| !title.is_empty())
    {
        return title;
    }
    format!("会话 {}", short_id(&session.id))
}

fn compact_session_title(value: &str) -> String {
    let compact = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = compact.chars();
    let short = chars.by_ref().take(36).collect::<String>();
    if chars.next().is_some() {
        format!("{short}…")
    } else {
        short
    }
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

fn session_metadata(session: &SessionView) -> Vec<(&'static str, String)> {
    let mut metadata = Vec::new();
    if let Some(cwd) = session
        .cwd
        .as_deref()
        .map(str::trim)
        .filter(|cwd| !cwd.is_empty())
    {
        metadata.push(("目录", cwd.to_owned()));
    }
    if let Some(model) = session
        .execution_profile
        .model
        .as_deref()
        .map(str::trim)
        .filter(|model| !model.is_empty())
    {
        metadata.push(("模型", model.to_owned()));
    }
    if let Some(effort) = session
        .execution_profile
        .reasoning_effort
        .as_deref()
        .map(str::trim)
        .filter(|effort| !effort.is_empty())
    {
        metadata.push(("推理", reasoning_effort_label(effort)));
    }
    if let Some(permissions) = permissions_label(&session.execution_profile.permissions) {
        metadata.push(("权限", permissions));
    }
    metadata
}

fn reasoning_effort_label(effort: &str) -> String {
    match effort {
        "default" => "默认".to_owned(),
        "low" => "低".to_owned(),
        "medium" => "中".to_owned(),
        "high" => "高".to_owned(),
        "xhigh" => "很高".to_owned(),
        "max" => "最高".to_owned(),
        "ultra" => "极高".to_owned(),
        _ => effort.to_owned(),
    }
}

fn permissions_label(permissions: &AgentSessionPermissions) -> Option<String> {
    let mut labels = Vec::new();
    if let Some(filesystem) = permissions.filesystem {
        labels.push(match filesystem {
            AgentFilesystemAccess::ReadOnly => "只读",
            AgentFilesystemAccess::WorkspaceWrite => "工作区可写",
            AgentFilesystemAccess::FullAccess => "完全访问",
            AgentFilesystemAccess::External => "文件权限由 Agent 控制",
        });
    }
    if let Some(network) = permissions.network {
        labels.push(match network {
            AgentNetworkAccess::Disabled => "网络关闭",
            AgentNetworkAccess::Restricted => "网络受限",
            AgentNetworkAccess::Enabled => "网络开启",
            AgentNetworkAccess::External => "网络由 Agent 控制",
        });
    }
    if let Some(approvals) = permissions.approvals {
        labels.push(match approvals {
            AgentApprovalMode::Restricted => "受限操作需审批",
            AgentApprovalMode::OnRequest => "按需审批",
            AgentApprovalMode::Never => "无需审批",
            AgentApprovalMode::Granular => "细粒度审批",
            AgentApprovalMode::External => "审批由 Agent 控制",
        });
    }
    (!labels.is_empty()).then(|| labels.join(" · "))
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
    if let Some(supervision) = &run.supervision {
        return match supervision.state.as_str() {
            "interrupting" => ("无进展，正在安全终止".to_owned(), "warning"),
            _ => ("任务已停滞".to_owned(), "error"),
        };
    }
    match run.status.as_str() {
        "submitting" => (format!("发送中{elapsed}"), "working"),
        "accepted" => (format!("已接收，等待 Agent 开始{elapsed}"), "working"),
        "running" => (format!("Working{elapsed}"), "working"),
        "waiting" => (format!("Waiting{elapsed}"), "waiting"),
        "stopping" => (format!("Stopping{elapsed}"), "working"),
        "delivered" => ("完成".to_owned(), "complete"),
        "incomplete" => ("未完整结束".to_owned(), "warning"),
        "cancelled" => ("已取消".to_owned(), "idle"),
        "failed" => ("失败".to_owned(), "error"),
        "loading" => ("正在载入".to_owned(), "working"),
        "unknown" if run.recovery_is_manual() => ("自动恢复已停止".to_owned(), "warning"),
        "unknown" => ("正在自动恢复".to_owned(), "warning"),
        _ => ("状态待确认".to_owned(), "warning"),
    }
}

/// Chooses the status of a connector-owned Session without changing which
/// concrete Run receives controls. A terminal Host mirror may be older than a
/// newer native turn, so native state owns the badge once no controlled Run is
/// active. The mirror remains in the timeline as durable failure evidence.
fn session_run_label(
    run: Option<&RunState>,
    native_session_state: Option<&str>,
    native_session_updated_at: Option<i64>,
    native_turn_status: Option<&str>,
    now: f64,
) -> (String, &'static str) {
    if run.is_some_and(|run| !is_terminal(&run.status)) {
        return run_label(run, now);
    }

    match native_session_state {
        Some("active" | "busy_elsewhere") => return ("Working".to_owned(), "working"),
        Some("waiting_input" | "waiting_approval") => {
            return ("Waiting".to_owned(), "waiting");
        }
        _ => {}
    }

    let native_turn_is_newer = run.is_none_or(|run| {
        run.history_latest_turn_status.is_some()
            || native_session_updated_at
                .zip(run.updated_at_unix_ms)
                .is_some_and(|(native_updated_at, controlled_updated_at)| {
                    native_updated_at > controlled_updated_at
                })
    });
    if native_turn_is_newer {
        match native_turn_status {
            Some("pending") => return ("已接收，等待 Agent 开始".to_owned(), "working"),
            Some("active" | "running" | "in_progress") => {
                return ("Working".to_owned(), "working");
            }
            Some("completed") => return ("完成".to_owned(), "complete"),
            Some("interrupted") => return ("未完整结束".to_owned(), "warning"),
            Some("failed") => return ("失败".to_owned(), "error"),
            _ => {}
        }
    }
    run_label(run, now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        AgentConnectorView, AgentSessionCapabilitiesView, AgentSessionDetail,
        AgentSessionExecutionProfile,
    };
    use crate::state::{AgentSessionListState, Message};

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
            execution_profile: Default::default(),
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
                resolve_requests: false,
            },
            creation: None,
            actions: Vec::new(),
        }
    }

    #[test]
    fn session_metadata_is_provider_neutral_and_omits_unknown_fields() {
        let mut view = session("thread-1", Some("fixture/local"), 1);
        view.cwd = Some("/workspace/project".to_owned());
        view.execution_profile = AgentSessionExecutionProfile {
            model: Some("fixture-large".to_owned()),
            reasoning_effort: Some("high".to_owned()),
            permissions: AgentSessionPermissions {
                filesystem: Some(AgentFilesystemAccess::WorkspaceWrite),
                network: Some(AgentNetworkAccess::Restricted),
                approvals: Some(AgentApprovalMode::OnRequest),
            },
        };

        assert_eq!(
            session_metadata(&view),
            vec![
                ("目录", "/workspace/project".to_owned()),
                ("模型", "fixture-large".to_owned()),
                ("推理", "高".to_owned()),
                ("权限", "工作区可写 · 网络受限 · 按需审批".to_owned()),
            ]
        );
        view.execution_profile.reasoning_effort = Some("default".to_owned());
        assert_eq!(session_metadata(&view)[2], ("推理", "默认".to_owned()));
        assert!(session_metadata(&session("empty", Some("other/local"), 1)).is_empty());
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

    #[test]
    fn sidebar_pages_each_source_without_losing_recency_order() {
        let group = SessionGroup {
            key: "orchestral".to_owned(),
            label: "Orchestral".to_owned(),
            connector_id: None,
            sessions: (0..23)
                .map(|index| session(&format!("session-{index}"), None, 23 - index))
                .collect(),
            has_more: false,
            loading_more: false,
            load_error: None,
        };

        let (first, first_page, page_count) = paginated_sessions(&group, 0);
        let (last, last_page, _) = paginated_sessions(&group, usize::MAX);

        assert_eq!(first_page, 0);
        assert_eq!(page_count, 3);
        assert_eq!(first.len(), 10);
        assert_eq!(first[0].id, "session-0");
        assert_eq!(last_page, 2);
        assert_eq!(last.len(), 3);
        assert_eq!(last[0].id, "session-20");
    }

    #[test]
    fn sidebar_title_prefers_recent_content_over_generated_name() {
        let mut state = AppState::new(true);
        let mut item = session("thread-1", Some("codex/local"), 10);
        item.title = Some("回应问候".to_owned());
        item.preview = Some("来自列表的最近内容".to_owned());
        item.run_ids = vec!["history".to_owned()];
        let run = state.ensure_run_source(
            "history",
            Some("thread-1".to_owned()),
            Some("codex/local".to_owned()),
        );
        run.messages.push(Message {
            id: "user-1".to_owned(),
            client_id: None,
            role: "user".to_owned(),
            text: "更早的内容".to_owned(),
            order: 1,
            occurred_at_unix_ms: None,
            native_anchor_id: None,
            optimistic: false,
            deferred: false,
            partial: false,
            steering: false,
        });
        run.messages.push(Message {
            id: "user-2".to_owned(),
            client_id: None,
            role: "user".to_owned(),
            text: "这是最近发送、用来快速定位会话的内容".to_owned(),
            order: 2,
            occurred_at_unix_ms: None,
            native_anchor_id: None,
            optimistic: false,
            deferred: false,
            partial: false,
            steering: false,
        });

        assert_eq!(
            session_title(&state, &item),
            "这是最近发送、用来快速定位会话的内容"
        );

        state.runs.clear();
        assert_eq!(session_title(&state, &item), "来自列表的最近内容");
    }

    #[test]
    fn detail_title_uses_the_last_message_in_the_merged_timeline() {
        let detail: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "codex/local",
                "session_id": "thread-1",
                "title": "stale generated title",
                "state": "active"
            },
            "turns": [{
                "turn_id": "turn-latest",
                "status": "active",
                "activities": [{
                    "activity_id": "native-latest-user",
                    "kind": "user_message",
                    "status": "completed",
                    "content": [{"body": {"kind": "inline", "value": "真正的最新消息"}}],
                    "details": {
                        "clientId": "orchestral-command:run-1:command-1:digest"
                    }
                }]
            }],
            "controlled_runs": [{
                "created_at_unix_ms": 1000,
                "execution": {"session_id": "thread-1", "run_id": "run-1"},
                "state": {"state": "running"},
                "last_run_seq": 2,
                "input": [{"body": {"kind": "inline", "value": "已经滑出最近页的旧消息"}}]
            }]
        }))
        .unwrap();
        let mut state = AppState::new(true);
        state.project_agent_session(detail);

        assert_eq!(
            session_title(&state, &state.sessions.items[0]),
            "真正的最新消息"
        );
    }

    #[test]
    fn run_labels_distinguish_host_acceptance_from_native_execution() {
        let mut run = RunState::new("run-1", None);
        run.status = "accepted".to_owned();
        assert!(run_label(Some(&run), 0.0)
            .0
            .starts_with("已接收，等待 Agent 开始"));

        run.status = "unknown".to_owned();
        assert_eq!(run_label(Some(&run), 0.0).0, "正在自动恢复");

        run.recovery = Some(crate::state::RunRecoveryState {
            mode: "manual".to_owned(),
            can_start_new_run: false,
            reason: Some("unsafe boundary".to_owned()),
        });
        assert_eq!(run_label(Some(&run), 0.0).0, "自动恢复已停止");
    }

    #[test]
    fn newer_native_turn_owns_badge_over_stale_controlled_failure() {
        let mut state = AppState::new(true);
        state.sessions.selected_id = Some("codex/local\0thread-1".to_owned());

        for (session_state, turn_status, expected_label, expected_tone) in [
            ("active", "active", "Working", "working"),
            ("idle", "completed", "完成", "complete"),
        ] {
            let detail: AgentSessionDetail = serde_json::from_value(serde_json::json!({
                "summary": {
                    "connector_id": "codex/local",
                    "session_id": "thread-1",
                    "state": session_state,
                    "updated_at_unix_ms": 3000
                },
                "turns": [{
                    "turn_id": "native-newer",
                    "status": turn_status,
                    "activities": []
                }],
                "controlled_runs": [{
                    "created_at_unix_ms": 1000,
                    "updated_at_unix_ms": 2000,
                    "execution": {"session_id": "thread-1", "run_id": "controlled-old"},
                    "state": {"state": "terminal", "terminal": {"type": "failed"}},
                    "last_run_seq": 3,
                    "input": [{"body": {"kind": "inline", "value": "old input"}}]
                }]
            }))
            .unwrap();
            state.project_agent_session(detail);

            let session = state.selected_session().expect("session is selected");
            let current_run = state.current_run().expect("controlled Run remains visible");
            assert_eq!(current_run.id, "controlled-old");
            assert_eq!(current_run.status, "failed");

            let history_id = session.history_run_id().expect("native history Run");
            let history = state
                .runs
                .get(&history_id)
                .expect("projected native history");
            assert_eq!(history.status, "delivered");
            assert_eq!(
                history.history_latest_turn_status.as_deref(),
                Some(turn_status)
            );
            assert_eq!(
                session_run_label(
                    Some(current_run),
                    session.state.as_deref(),
                    Some(session.updated_at_unix_ms),
                    history.history_latest_turn_status.as_deref(),
                    0.0,
                ),
                (expected_label.to_owned(), expected_tone)
            );
        }
    }

    #[test]
    fn newer_controlled_failure_is_not_hidden_by_older_native_completion() {
        let mut run = RunState::new("controlled-new", Some("thread-1".to_owned()));
        run.status = "failed".to_owned();
        run.updated_at_unix_ms = Some(3_000);

        assert_eq!(
            session_run_label(
                Some(&run),
                Some("idle"),
                Some(2_000),
                Some("completed"),
                0.0,
            ),
            ("失败".to_owned(), "error")
        );
    }
}
