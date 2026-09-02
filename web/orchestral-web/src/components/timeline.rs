use dioxus::prelude::*;
use serde_json::Value;

use crate::browser::{controller::AppController, platform};
use crate::markdown;
use crate::presentation::{
    activity_group_expansion_for_status, operation_details_expanded_by_default,
    operation_is_failure, operation_is_running,
};
use crate::state::{
    timeline_blocks_for_session, timeline_run_ids_for_session, CommandActivity, TimelineBlock,
    TimelineItem, ToolActivity,
};

#[component]
pub fn ConversationTimeline() -> Element {
    let controller = consume_context::<AppController>();
    let state = controller.state.read();
    let loading_session = state
        .selected_session()
        .is_some_and(|session| state.ui.loading_session.as_deref() == Some(session.key().as_str()));
    let scrolled_away = state.ui.timeline_scrolled_away;
    let (blocks, runs) = state
        .selected_session()
        .map(|session| {
            let blocks = timeline_blocks_for_session(&state, session)
                .into_iter()
                .map(|entry| (entry.key(), entry))
                .collect::<Vec<_>>();
            let runs = timeline_run_ids_for_session(&state, session)
                .iter()
                .filter_map(|run_id| state.runs.get(run_id).cloned())
                .collect::<Vec<_>>();
            (blocks, runs)
        })
        .unwrap_or_default();
    let empty = blocks.is_empty();
    let history_pagination = state.selected_session().and_then(|session| {
        let run_id = session.history_run_id()?;
        let run = state.runs.get(&run_id)?;
        run.history_next_cursor
            .as_ref()
            .map(|_| (run.history_loading_earlier, state.connection.online))
    });
    drop(state);

    rsx! {
        div { class: "timeline-stage",
            section {
                class: "message-list",
                role: "log",
                aria_label: "对话内容",
                aria_live: "polite",
                onscroll: move |_| controller.update_timeline_scroll_state(),
                if let Some((loading, online)) = history_pagination {
                    div { class: "history-pagination",
                        button {
                            class: "history-pagination__button",
                            r#type: "button",
                            disabled: loading || !online,
                            aria_label: "加载更早的会话记录",
                            onclick: move |_| {
                                spawn(async move { controller.load_earlier_agent_history().await });
                            },
                            if loading { "正在加载更早记录…" } else { "加载更早记录" }
                        }
                    }
                }
                if empty && loading_session {
                    div { class: "timeline-skeleton", role: "status", aria_label: "正在加载会话内容",
                        span { class: "timeline-skeleton__line timeline-skeleton__line--short" }
                        span { class: "timeline-skeleton__bubble" }
                        span { class: "timeline-skeleton__line" }
                        span { class: "timeline-skeleton__line timeline-skeleton__line--medium" }
                    }
                } else if empty {
                    div { class: "empty-state",
                        div { class: "empty-state__mark", aria_hidden: "true", "✦" }
                        p { class: "eyebrow", "随时可以开始" }
                        h2 { "今天想推进什么？" }
                        p { class: "empty-state__intro",
                            "描述目标、贴一段错误，或者交给我一项完整任务。过程与结果都会保留在这个会话里。"
                        }
                    }
                } else {
                    for (key, entry) in blocks {
                        TimelineBlockView {
                            key: "{key}",
                            run_id: entry.run_id,
                            block: entry.block
                        }
                    }
                    for run in runs {
                        if let Some(failure) = run.failure.as_ref() {
                            RunFailure { value: failure.clone() }
                        } else if let Some(error) = run.error.as_ref() {
                            div { class: "run-failure", "{error}" }
                        }
                    }
                }
            }
            if scrolled_away {
                button {
                    class: "timeline-jump",
                    r#type: "button",
                    aria_label: "跳到最新消息",
                    onclick: move |_| controller.jump_timeline_to_latest(),
                    "↓ 最新消息"
                }
            }
        }
    }
}

#[component]
fn TimelineBlockView(run_id: String, block: TimelineBlock) -> Element {
    match block {
        TimelineBlock::Entry(item) => rsx! { TimelineEntry { run_id, item } },
        TimelineBlock::ActivityGroup(items) => rsx! { ActivityGroupView { items } },
    }
}

#[component]
fn TimelineEntry(run_id: String, item: TimelineItem) -> Element {
    match item {
        TimelineItem::Message(message) => {
            let controller = consume_context::<AppController>();
            let accepted = message.role == "user"
                && !message.optimistic
                && !message.deferred
                && controller
                    .state
                    .read()
                    .runs
                    .get(&run_id)
                    .is_some_and(|run| run.status == "accepted");
            rsx! {
                MessageView {
                    id: message.id,
                    role: message.role,
                    text: message.text,
                    optimistic: message.optimistic,
                    accepted,
                    deferred: message.deferred,
                    partial: message.partial,
                    streaming: false
                }
            }
        }
        TimelineItem::Stream(output) => rsx! {
            MessageView {
                id: format!("stream-{run_id}-{}", output.output_id),
                role: "assistant".to_owned(),
                text: output.text,
                optimistic: false,
                accepted: false,
                deferred: false,
                partial: false,
                streaming: true
            }
        },
        TimelineItem::Activity(activity) => rsx! { ToolActivityView { activity } },
        TimelineItem::Command(command) => rsx! { CommandActivityView { command } },
        TimelineItem::Progress(progress) => {
            let label = progress
                .fraction
                .map(|fraction| format!("{}% · {}", (fraction * 100.0).round(), progress.message))
                .unwrap_or(progress.message);
            rsx! {
                section { class: "run-activity", aria_label: "运行活动",
                    div { class: "progress-card",
                        span { class: "progress-card__label", "{label}" }
                        if let Some(fraction) = progress.fraction {
                            progress { max: "1", value: "{fraction}" }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn ActivityGroupView(items: Vec<TimelineItem>) -> Element {
    let failures = items
        .iter()
        .filter(|item| operation_state(item).is_some_and(operation_is_failure))
        .count();
    let running = items
        .iter()
        .filter(|item| operation_state(item).is_some_and(operation_is_running))
        .count();
    let state = if failures > 0 {
        "failed"
    } else if running > 0 {
        "running"
    } else {
        "succeeded"
    };
    let status = if failures > 0 {
        format!("{failures} 项失败")
    } else if running > 0 {
        format!("{running} 项进行中")
    } else {
        "已完成".to_owned()
    };
    let latest = items
        .last()
        .map(operation_name)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "Agent 操作".to_owned());
    let count = items.len();
    let mut expanded = use_signal(|| running > 0);

    use_effect(use_reactive(
        (&failures, &running),
        move |(failures, running)| {
            if let Some(next) = activity_group_expansion_for_status(failures, running) {
                expanded.set(next);
            }
        },
    ));

    rsx! {
        section { class: "run-activity", aria_label: "运行活动",
            div { class: "activity-group", "data-state": state,
                button {
                    class: "activity-group__summary",
                    r#type: "button",
                    aria_expanded: expanded(),
                    onclick: move |_| expanded.toggle(),
                    span { class: "activity-state activity-state--{state}", aria_hidden: "true" }
                    span { class: "activity-group__copy",
                        strong { "{count} 项操作" }
                        span { "最近：{latest}" }
                    }
                    span { class: "activity-group__status", "{status}" }
                    span {
                        class: if expanded() { "activity-group__chevron is-open" } else { "activity-group__chevron" },
                        aria_hidden: "true"
                    }
                }
                if expanded() {
                    div { class: "activity-group__body",
                        for (index, item) in items.into_iter().enumerate() {
                            ActivityGroupEntry { key: "{index}", item }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn ActivityGroupEntry(item: TimelineItem) -> Element {
    match item {
        TimelineItem::Activity(activity) => rsx! { ToolActivityView { activity } },
        TimelineItem::Command(command) => rsx! { CommandActivityView { command } },
        _ => rsx! {},
    }
}

#[component]
fn MessageView(
    id: String,
    role: String,
    text: String,
    optimistic: bool,
    accepted: bool,
    deferred: bool,
    partial: bool,
    streaming: bool,
) -> Element {
    let controller = consume_context::<AppController>();
    let class = format!(
        "message message--{role}{}{}{}{}",
        if partial { " message--partial" } else { "" },
        if streaming { " is-streaming" } else { "" },
        if accepted { " message--accepted" } else { "" },
        if deferred { " message--deferred" } else { "" }
    );
    let copy = text.clone();
    let rendered = (role == "assistant" || text.contains("](/api/v1/attachments/"))
        .then(|| markdown::render(&text));
    rsx! {
        article { class, "data-message-id": id,
            span { class: "message__role", if role == "user" { "你" } else { "Orchestral" } }
            if let Some(rendered) = rendered {
                div { class: "message__content", dangerous_inner_html: rendered }
            } else {
                div { class: "message__content", "{text}" }
            }
            if role == "assistant" && !text.is_empty() {
                button {
                    class: "message__copy",
                    r#type: "button",
                    onclick: move |_| {
                        let value = copy.clone();
                        spawn(async move {
                            match platform::copy_text(&value).await {
                                Ok(()) => controller.notice("已复制", "success"),
                                Err(error) => controller.notice(&error, "error"),
                            }
                        });
                    },
                    "复制"
                }
            }
            if optimistic { span { class: "message__meta", "发送中…" } }
            if accepted { span { class: "message__meta", "已接收，等待 Agent 开始" } }
            if deferred { span { class: "message__meta", "已排队，等待原 Agent 接收" } }
        }
    }
}

#[component]
fn ToolActivityView(activity: ToolActivity) -> Element {
    let open = operation_details_expanded_by_default(&activity.state);
    rsx! {
        details { class: "activity-item", open,
            summary { class: "activity-item__summary",
                span { class: "activity-state activity-state--{activity.state}", aria_hidden: "true" }
                span { class: "activity-item__name", "{activity.tool_name}" }
                span { class: "activity-item__status", "{activity.state}" }
            }
            div { class: "activity-item__body",
                if activity.evidence.is_empty() {
                    p { class: "evidence", "暂无可展示的活动细节" }
                } else {
                    for (index, evidence) in activity.evidence.into_iter().enumerate() {
                        EvidenceView { key: "{index}", evidence }
                    }
                }
            }
        }
    }
}

#[component]
fn CommandActivityView(command: CommandActivity) -> Element {
    let open = operation_details_expanded_by_default(&command.state);
    rsx! {
        details { class: "activity-item activity-item--command", open,
            summary { class: "activity-item__summary",
                span { class: "activity-state activity-state--{command.state}", aria_hidden: "true" }
                span { class: "activity-item__name", {command.kind.replace('_', " ")} }
                span { class: "activity-item__status", "{command.state}" }
            }
            div { class: "activity-item__body",
                if !command.summary.is_empty() {
                    pre { class: "command-block", tabindex: "0", "{command.summary}" }
                }
                small { "command_id: {command.id}" }
            }
        }
    }
}

fn operation_state(item: &TimelineItem) -> Option<&str> {
    match item {
        TimelineItem::Activity(activity) => Some(&activity.state),
        TimelineItem::Command(command) => Some(&command.state),
        _ => None,
    }
}

fn operation_name(item: &TimelineItem) -> String {
    match item {
        TimelineItem::Activity(activity) => activity.tool_name.replace('_', " "),
        TimelineItem::Command(command) => command.kind.replace('_', " "),
        _ => String::new(),
    }
}

#[component]
fn EvidenceView(evidence: Value) -> Element {
    let kind = evidence
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("note");
    match kind {
        "command" => {
            let command = string_field(&evidence, "command");
            rsx! {
                div { class: "evidence evidence--command",
                    span { class: "evidence__label", "命令" }
                    pre { class: "command-block", tabindex: "0", "{command}" }
                }
            }
        }
        "file" => {
            let operation = string_field(&evidence, "operation");
            let path = string_field(&evidence, "path");
            let lines = evidence
                .get("diff")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            rsx! {
                div { class: "evidence evidence--file",
                    div { class: "evidence__heading",
                        span { class: "evidence__label", "{operation}" }
                        code { "{path}" }
                    }
                    if !lines.is_empty() {
                        pre { class: "tool-diff", tabindex: "0",
                            for line in lines {
                                {
                                    let line_kind = string_field(&line, "kind");
                                    let prefix = match line_kind.as_str() {
                                        "addition" => "+",
                                        "deletion" => "−",
                                        _ => " ",
                                    };
                                    let text = string_field(&line, "text");
                                    rsx! {
                                        span { class: "tool-diff__line tool-diff__line--{line_kind}",
                                            "{prefix} {text}"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        "error" => {
            let code = string_field(&evidence, "code");
            let message = string_field(&evidence, "message");
            let details = evidence
                .get("details")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let guidance = details
                .iter()
                .filter(|detail| {
                    matches!(
                        detail.get("label").and_then(Value::as_str),
                        Some("how_to_get" | "hint")
                    )
                })
                .cloned()
                .collect::<Vec<_>>();
            let metadata = details
                .iter()
                .filter(|detail| {
                    !matches!(
                        detail.get("label").and_then(Value::as_str),
                        Some("how_to_get" | "hint")
                    )
                })
                .cloned()
                .collect::<Vec<_>>();
            rsx! {
                div { class: "evidence evidence--error",
                    strong { class: "evidence-error__code", "{code}" }
                    p { class: "evidence-error__message", "{message}" }
                    if !guidance.is_empty() {
                        div { class: "evidence-error__guidance",
                            span { class: "evidence-error__label", "建议" }
                            ul {
                                for detail in guidance {
                                    li { {string_field(&detail, "value")} }
                                }
                            }
                        }
                    }
                    if !metadata.is_empty() {
                        details { class: "evidence-error__details",
                            summary { "更多错误信息" }
                            dl {
                                for detail in metadata {
                                    dt { {string_field(&detail, "label")} }
                                    dd { {string_field(&detail, "value")} }
                                }
                            }
                        }
                    }
                }
            }
        }
        "omitted" => {
            let count = evidence
                .get("count")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            rsx! { p { class: "evidence", "{count} 项活动未显示" } }
        }
        _ => {
            let text = string_field(&evidence, "text");
            rsx! { p { class: "evidence", "{text}" } }
        }
    }
}

#[component]
fn RunFailure(value: Value) -> Element {
    let code = string_field(&value, "code");
    let message = string_field(&value, "message");
    rsx! {
        div { class: "run-failure",
            strong { "{code}" }
            span { "{message}" }
        }
    }
}

fn string_field(value: &Value, field: &str) -> String {
    value
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}
