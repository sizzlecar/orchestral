use dioxus::prelude::*;
use serde_json::Value;

use crate::browser::controller::AppController;
use crate::state::{content_text, RunState};

#[component]
pub fn PendingPanel() -> Element {
    let controller = consume_context::<AppController>();
    let (run, can_resolve) = {
        let state = controller.state.read();
        let run = state.pending_run().cloned();
        let can_resolve = run.as_ref().is_none_or(|run| {
            !run.id.starts_with("agent-history:")
                || state
                    .connectors
                    .items
                    .iter()
                    .find(|connector| {
                        run.connector_id.as_deref() == Some(connector.connector_id.as_str())
                    })
                    .is_some_and(|connector| connector.capabilities.resolve_requests)
        });
        (run, can_resolve)
    };
    let Some(run) = run.filter(|run| !run.pending.is_empty()) else {
        return rsx! {};
    };
    rsx! {
        section { class: "pending-panel", aria_label: "待处理请求", aria_live: "assertive",
            for request in run.pending.clone() {
                PendingCard { key: "{request_id(&request)}", run: run.clone(), request, can_resolve }
            }
        }
    }
}

#[component]
fn PendingCard(run: RunState, request: Value, can_resolve: bool) -> Element {
    let controller = consume_context::<AppController>();
    let payload = request.get("payload").cloned().unwrap_or(Value::Null);
    let kind = payload
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("external");
    let request_id = request_id(&request);
    let run_id = run.id.clone();
    let resolving = controller
        .state
        .read()
        .ui
        .request_is_resolving(&run_id, &request_id);
    if !can_resolve {
        return rsx! {
            article { class: "pending-card",
                span { class: "pending-card__badge", "外部操作" }
                h2 { "需要在主机上继续" }
                p { "当前 Agent 只能展示这项请求，尚未提供远程处理能力。" }
            }
        };
    }
    match kind {
        "input" => {
            let mut response = use_signal(String::new);
            let prompt = request_prompt(&request);
            let submit_run = run_id.clone();
            let submit_request = request_id.clone();
            rsx! {
                article { class: "pending-card", "data-request-id": request_id, "data-state": if resolving { "resolving" } else { "pending" },
                    form {
                        class: "pending-card__form",
                        onsubmit: move |event| {
                            event.prevent_default();
                            let text = response();
                            if text.trim().is_empty() { return; }
                            let run_id = submit_run.clone();
                            let request_id = submit_request.clone();
                            spawn(async move {
                                controller.resolve_input(run_id, request_id, text).await;
                            });
                        },
                        div { class: "pending-card__heading",
                            span { class: "pending-card__badge", "需要输入" }
                            h2 { if prompt.is_empty() { "Orchestral 需要更多信息" } else { "{prompt}" } }
                        }
                        textarea {
                            rows: "2",
                            maxlength: "20000",
                            required: true,
                            disabled: resolving,
                            placeholder: "输入回复…",
                            value: response,
                            oninput: move |event| response.set(event.value())
                        }
                        button { class: "pending-card__primary", r#type: "submit", disabled: resolving,
                            if resolving { "处理中…" } else { "继续" }
                        }
                    }
                }
            }
        }
        "approval" => {
            let reason = payload
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let (headline, operation) = approval_presentation(reason);
            let scopes = payload
                .get("requested_scope")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let allow_session = !payload
                .get("session_approval_scope")
                .unwrap_or(&Value::Null)
                .is_null();
            rsx! {
                article { class: "pending-card pending-card--approval", "data-request-id": request_id.clone(), "data-state": if resolving { "resolving" } else { "pending" },
                    div { class: "pending-card__heading",
                        span { class: "pending-card__badge pending-card__badge--warning", "需要批准" }
                        h2 { "{headline}" }
                    }
                    if !scopes.is_empty() {
                        div { class: "pending-card__scope-row", aria_label: "请求的权限范围",
                            for scope in scopes.into_iter().take(4) {
                                span { class: "pending-card__scope-chip", "{approval_scope_label(scope.as_str().unwrap_or_default())}" }
                            }
                        }
                    }
                    details { class: "pending-card__details",
                        summary { class: "pending-card__details-summary",
                            span { class: "pending-card__details-label", "操作详情" }
                            span { class: "pending-card__details-preview", "{operation}" }
                            span { class: "pending-card__details-chevron", aria_hidden: "true" }
                        }
                        pre { class: "pending-card__operation", tabindex: "0", "{operation}" }
                    }
                    div { class: "pending-card__actions",
                        ApprovalButton {
                            label: "允许一次",
                            class: "pending-card__primary",
                            run_id: run_id.clone(),
                            request_id: request_id.clone(),
                            decision: "allow_once"
                        }
                        if allow_session {
                            ApprovalButton {
                                label: "本会话允许",
                                class: "pending-card__secondary",
                                run_id: run_id.clone(),
                                request_id: request_id.clone(),
                                decision: "allow_session"
                            }
                        }
                        ApprovalButton {
                            label: "拒绝",
                            class: "pending-card__danger",
                            run_id: run_id.clone(),
                            request_id: request_id.clone(),
                            decision: "deny"
                        }
                        if resolving {
                            span { class: "pending-card__resolving", role: "status", "处理中…" }
                        }
                    }
                }
            }
        }
        _ => rsx! {
            article { class: "pending-card",
                span { class: "pending-card__badge", "外部操作" }
                h2 { "需要在主机上继续" }
                p { "此请求暂时需要在 Orchestral 主机端处理。" }
            }
        },
    }
}

#[component]
fn ApprovalButton(
    label: &'static str,
    class: &'static str,
    run_id: String,
    request_id: String,
    decision: &'static str,
) -> Element {
    let controller = consume_context::<AppController>();
    let resolving = controller
        .state
        .read()
        .ui
        .request_is_resolving(&run_id, &request_id);
    rsx! {
        button {
            class,
            r#type: "button",
            disabled: resolving,
            onclick: move |_| {
                let run_id = run_id.clone();
                let request_id = request_id.clone();
                spawn(async move {
                    controller
                        .resolve_approval(run_id, request_id, decision.to_owned())
                        .await;
                });
            },
            "{label}"
        }
    }
}

fn request_id(request: &Value) -> String {
    request
        .get("request_id")
        .and_then(Value::as_str)
        .unwrap_or("request")
        .to_owned()
}

fn request_prompt(request: &Value) -> String {
    request
        .get("payload")
        .and_then(|payload| payload.get("prompt"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(content_text)
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn approval_presentation(reason: &str) -> (String, String) {
    let summary = reason.trim();
    if let Some((operation, rationale)) = summary.rsplit_once("; Reason: ") {
        if !operation.trim().is_empty() && !rationale.trim().is_empty() {
            return (rationale.trim().to_owned(), operation.trim().to_owned());
        }
    }
    (
        "允许 Orchestral 执行此操作？".to_owned(),
        if summary.is_empty() {
            "主机请求执行一项受保护的操作".to_owned()
        } else {
            summary.to_owned()
        },
    )
}

pub fn approval_scope_label(scope: &str) -> String {
    let (effect, qualifier) = scope.split_once(':').unwrap_or((scope, ""));
    let label = match effect {
        "process" => "运行命令",
        "network" => "访问网络",
        "filesystem_read" => "读取文件",
        "filesystem_write" => "修改文件",
        "environment_read" => "读取环境",
        "external_side_effect" => "外部影响",
        "host_execution" => "主机执行",
        value => value,
    };
    if qualifier.is_empty() || qualifier == "unrestricted" {
        label.to_owned()
    } else {
        format!("{label} · {qualifier}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn approval_reason_is_split_into_rationale_and_operation() {
        let (headline, operation) = approval_presentation(
            "Execute outside sandbox: git pull; Reason: User asked to update the repository",
        );
        assert_eq!(headline, "User asked to update the repository");
        assert_eq!(operation, "Execute outside sandbox: git pull");
    }

    #[test]
    fn approval_scopes_are_human_readable() {
        assert_eq!(approval_scope_label("network:unrestricted"), "访问网络");
        assert_eq!(
            approval_scope_label("filesystem_read:workspace"),
            "读取文件 · workspace"
        );
    }
}
