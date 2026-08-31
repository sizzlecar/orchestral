import {
    activeRun,
    contentText,
    currentRun,
    isTerminalStatus,
    runsForSelectedSession,
    selectedSession,
    timelineForRun,
} from "./state.js";

function element(tag, { className, text, attrs, dataset } = {}, children = []) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    for (const [name, value] of Object.entries(attrs ?? {})) {
        if (value !== false && value !== null && value !== undefined) {
            node.setAttribute(name, value === true ? "" : String(value));
        }
    }
    for (const [name, value] of Object.entries(dataset ?? {})) {
        node.dataset[name] = String(value);
    }
    for (const child of Array.isArray(children) ? children : [children]) {
        if (child) node.append(child);
    }
    return node;
}

function button(text, action, dataset = {}, className = "button") {
    return element("button", {
        className,
        text,
        attrs: { type: "button" },
        dataset: { action, ...dataset },
    });
}

function shortId(value) {
    if (!value) return "";
    return value.length > 18 ? `${value.slice(0, 8)}…${value.slice(-5)}` : value;
}

function formatDate(milliseconds) {
    if (!Number.isFinite(milliseconds)) return "";
    const value = new Date(milliseconds);
    const today = new Date();
    const sameDay = value.toDateString() === today.toDateString();
    return new Intl.DateTimeFormat(undefined, sameDay
        ? { hour: "2-digit", minute: "2-digit" }
        : { month: "short", day: "numeric" }).format(value);
}

function formatElapsed(startedAt, completedAt, now = Date.now()) {
    if (!startedAt) return "";
    const seconds = Math.max(0, Math.floor(((completedAt ?? now) - startedAt) / 1000));
    const minutes = Math.floor(seconds / 60);
    const remainder = seconds % 60;
    if (minutes >= 60) {
        const hours = Math.floor(minutes / 60);
        return `${hours}:${String(minutes % 60).padStart(2, "0")}:${String(remainder).padStart(2, "0")}`;
    }
    return `${minutes}:${String(remainder).padStart(2, "0")}`;
}

function runLabel(run, now) {
    if (!run) return { text: "就绪", state: "idle" };
    const elapsed = formatElapsed(run.startedAt, run.completedAt, now);
    const suffix = elapsed ? ` · ${elapsed}` : "";
    switch (run.status) {
        case "accepted": return { text: `Starting${suffix}`, state: "working" };
        case "running": return { text: `Working${suffix}`, state: "working" };
        case "waiting": return { text: `Waiting${suffix}`, state: "waiting" };
        case "stopping": return { text: `Stopping${suffix}`, state: "working" };
        case "unknown": return { text: "状态待确认", state: "warning" };
        case "delivered": return { text: elapsed ? `完成 · ${elapsed}` : "完成", state: "complete" };
        case "incomplete": return { text: "未完整结束", state: "warning" };
        case "cancelled": return { text: "已取消", state: "idle" };
        case "failed": return { text: "失败", state: "error" };
        case "loading": return { text: "正在载入", state: "working" };
        default: return { text: run.status || "未知", state: "warning" };
    }
}

function firstUserMessage(state, session) {
    for (const runId of session.run_ids ?? []) {
        const message = state.runs.byId[runId]?.messages.find((item) => item.role === "user");
        if (message?.text) return message.text;
    }
    return "";
}

function sessionTitle(state, session) {
    const prompt = firstUserMessage(state, session).replace(/\s+/g, " ").trim();
    if (prompt) return prompt.length > 42 ? `${prompt.slice(0, 42)}…` : prompt;
    return `会话 ${shortId(session.id)}`;
}

function createMessage(message) {
    const article = element("article", {
        className: `message message--${message.role}${message.partial ? " message--partial" : ""}${message.streaming ? " is-streaming" : ""}`,
        dataset: { messageId: message.id },
    });
    const role = element("span", {
        className: "message__role",
        text: message.role === "user" ? "你" : "Orchestral",
    });
    const content = element("div", { className: "message__content", text: message.text });
    article.append(role, content);
    if (message.role === "assistant" && message.text) {
        article.append(button("复制", "copy-message", { messageId: message.id }, "message__copy"));
    }
    if (message.optimistic) {
        article.append(element("span", { className: "message__meta", text: "发送中…" }));
    }
    return article;
}

function createDiff(lines) {
    const pre = element("pre", { className: "tool-diff", attrs: { tabindex: "0" } });
    for (const line of lines ?? []) {
        const prefix = line.kind === "addition" ? "+" : line.kind === "deletion" ? "−" : " ";
        pre.append(element("span", {
            className: `tool-diff__line tool-diff__line--${line.kind ?? "context"}`,
            text: `${prefix} ${line.text}`,
        }));
    }
    return pre;
}

function createEvidence(item) {
    switch (item?.type) {
        case "command":
            return element("div", { className: "evidence evidence--command" }, [
                element("span", { className: "evidence__label", text: "命令" }),
                element("pre", { className: "command-block", text: item.command, attrs: { tabindex: "0" } }),
            ]);
        case "file": {
            const children = [
                element("div", { className: "evidence__heading" }, [
                    element("span", { className: "evidence__label", text: item.operation || "file" }),
                    element("code", { text: item.path }),
                ]),
            ];
            if (item.diff?.length) children.push(createDiff(item.diff));
            if (item.diff_omitted) {
                children.push(element("small", { text: `另有 ${item.diff_omitted} 行未显示` }));
            }
            return element("div", { className: "evidence evidence--file" }, children);
        }
        case "error":
            return element("div", { className: "evidence evidence--error" }, [
                element("strong", { text: item.code || "Error" }),
                element("p", { text: item.message }),
            ]);
        case "omitted":
            return element("p", { className: "evidence", text: `${item.count} 项活动未显示` });
        case "note":
        default:
            return element("p", { className: "evidence", text: item?.text || "" });
    }
}

function createActivity(activity) {
    const running = activity.state === "running";
    const details = element("details", {
        className: "activity-item",
        dataset: { detailKey: `tool-${activity.id}` },
    });
    if (running) details.open = true;
    const summary = element("summary", { className: "activity-item__summary" }, [
        element("span", { className: `activity-state activity-state--${activity.state}`, attrs: { "aria-hidden": "true" } }),
        element("span", { className: "activity-item__name", text: activity.toolName }),
        element("span", { className: "activity-item__status", text: activity.state }),
    ]);
    const body = element("div", { className: "activity-item__body" });
    if (activity.evidence.length === 0) {
        body.append(element("p", { className: "evidence", text: "暂无可展示的活动细节" }));
    } else {
        for (const evidence of activity.evidence) body.append(createEvidence(evidence));
    }
    details.append(summary, body);
    return details;
}

function createCommand(command) {
    const details = element("details", {
        className: "activity-item activity-item--command",
        dataset: { detailKey: `command-${command.id}` },
    });
    details.append(
        element("summary", { className: "activity-item__summary" }, [
            element("span", { className: `activity-state activity-state--${command.state}`, attrs: { "aria-hidden": "true" } }),
            element("span", { className: "activity-item__name", text: command.type.replaceAll("_", " ") }),
            element("span", { className: "activity-item__status", text: command.state }),
        ]),
        element("div", { className: "activity-item__body" }, [
            command.summary
                ? element("pre", { className: "command-block", text: command.summary, attrs: { tabindex: "0" } })
                : null,
            element("small", { text: `command_id: ${command.id}` }),
        ]),
    );
    return details;
}

function wrapRunActivity(child) {
    const section = element("section", { className: "run-activity", attrs: { "aria-label": "运行活动" } });
    section.append(child);
    return section;
}

function createProgress(progressState) {
    const fraction = progressState.fraction;
    const text = fraction === null
        ? progressState.message
        : `${Math.round(fraction * 100)}% · ${progressState.message}`;
    const progress = element("div", { className: "progress-card" }, [
        element("span", { className: "progress-card__label", text }),
    ]);
    if (fraction !== null) {
        progress.append(element("progress", {
            attrs: { max: "1", value: String(fraction), "aria-label": text },
        }));
    }
    return progress;
}

function createTimelineNode(run, entry) {
    switch (entry.kind) {
        case "message":
            return createMessage(entry.value);
        case "stream":
            return createMessage({
                id: `stream-${run.id}-${entry.value.outputId}`,
                role: "assistant",
                text: entry.value.text,
                streaming: true,
            });
        case "activity":
            return wrapRunActivity(createActivity(entry.value));
        case "command":
            return wrapRunActivity(createCommand(entry.value));
        case "progress":
            return wrapRunActivity(createProgress(entry.value));
        default:
            return null;
    }
}

function requestPrompt(request) {
    const prompt = request.payload?.prompt;
    return Array.isArray(prompt) ? prompt.map(contentText).filter(Boolean).join("\n") : "";
}

function createPendingCard(run, request) {
    const card = element("article", {
        className: "pending-card",
        dataset: { requestId: request.request_id },
    });
    const payload = request.payload ?? {};
    if (payload.type === "input") {
        const form = element("form", {
            className: "pending-card__form",
            dataset: { action: "resolve-input", runId: run.id, requestId: request.request_id },
        });
        form.append(
            element("div", { className: "pending-card__heading" }, [
                element("span", { className: "pending-card__badge", text: "需要输入" }),
                element("h2", { text: requestPrompt(request) || "Orchestral 需要更多信息" }),
            ]),
            element("textarea", {
                attrs: {
                    name: "response",
                    rows: "2",
                    maxlength: "20000",
                    required: true,
                    placeholder: "输入回复…",
                    "aria-label": "回复待处理问题",
                },
            }),
            element("button", {
                className: "pending-card__primary",
                text: "继续",
                attrs: { type: "submit" },
            }),
        );
        card.append(form);
        return card;
    }

    if (payload.type === "approval") {
        card.append(
            element("div", { className: "pending-card__heading" }, [
                element("span", { className: "pending-card__badge pending-card__badge--warning", text: "需要批准" }),
                element("h2", { text: payload.reason || "是否允许此操作？" }),
            ]),
        );
        if (payload.requested_scope?.length) {
            card.append(element("p", {
                className: "pending-card__scope",
                text: `范围：${payload.requested_scope.join(" · ")}`,
            }));
        }
        const actions = element("div", { className: "pending-card__actions" });
        actions.append(
            button("仅允许一次", "resolve-approval", {
                runId: run.id,
                requestId: request.request_id,
                decision: "allow_once",
            }, "pending-card__primary"),
        );
        if (payload.session_approval_scope) {
            actions.append(button("本会话允许", "resolve-approval", {
                runId: run.id,
                requestId: request.request_id,
                decision: "allow_session",
            }, "pending-card__secondary"));
        }
        actions.append(button("拒绝", "resolve-approval", {
            runId: run.id,
            requestId: request.request_id,
            decision: "deny",
        }, "pending-card__danger"));
        card.append(actions);
        return card;
    }

    card.append(
        element("span", { className: "pending-card__badge", text: "外部操作" }),
        element("h2", { text: payload.name || "需要在主机上继续" }),
        element("p", { text: "此请求暂时需要在 Orchestral 主机端处理。" }),
    );
    return card;
}

function createAuthScreen() {
    const existing = document.querySelector("#auth-screen");
    if (existing) return existing;
    const screen = element("main", {
        className: "auth-screen",
        attrs: { id: "auth-screen", "aria-live": "polite" },
    });
    document.body.append(screen);
    return screen;
}

export function createView() {
    const refs = {
        shell: document.querySelector("#app-shell"),
        splash: document.querySelector("#splash"),
        sidebar: document.querySelector("#sidebar"),
        backdrop: document.querySelector("#sidebar-backdrop"),
        sidebarToggle: document.querySelector("#mobile-sidebar-toggle"),
        threadList: document.querySelector("#thread-list"),
        threadEmpty: document.querySelector("#thread-list-empty"),
        threadCount: document.querySelector("#thread-count"),
        title: document.querySelector("#thread-title"),
        messageList: document.querySelector("#message-list"),
        emptyState: document.querySelector("#empty-state"),
        runStatus: document.querySelector("#run-status"),
        pending: document.querySelector("#pending-panel"),
        composer: document.querySelector("#composer-form"),
        input: document.querySelector("#message-input"),
        send: document.querySelector("#send-button"),
        cancel: document.querySelector("#cancel-button"),
        hint: document.querySelector("#composer-hint"),
        connection: document.querySelector("#connection-status"),
        install: document.querySelector("#install-button"),
        settings: document.querySelector("#settings-dialog"),
        settingsContent: document.querySelector("#settings-content"),
        toast: document.querySelector("#toast-region"),
    };
    const emptyTemplate = refs.emptyState.cloneNode(true);
    const authScreen = createAuthScreen();
    let messageStructureSignature = "";
    let pendingSignature = "";

    function renderAuth(state) {
        const ready = state.auth.status === "authenticated";
        refs.shell.hidden = !ready;
        authScreen.hidden = ready || state.auth.status === "booting";
        refs.splash.hidden = state.auth.status !== "booting";
        if (ready || state.auth.status === "booting") return;

        authScreen.replaceChildren();
        const mark = element("img", {
            className: "auth-screen__mark",
            attrs: { src: "./icons/icon-192.svg", alt: "", width: "72", height: "72" },
        });
        if (state.auth.status === "pairing") {
            authScreen.append(mark, element("p", { className: "eyebrow", text: "安全配对" }), element("h1", { text: "正在连接这台设备…" }), element("p", { text: "请保持此页面打开。配对密钥只会使用一次。" }));
            return;
        }
        const error = state.auth.error;
        authScreen.append(
            mark,
            element("p", { className: "eyebrow", text: error ? "无法连接" : "尚未配对" }),
            element("h1", { text: error ? "配对没有完成" : "从 Orchestral 主机开始" }),
            element("p", { className: "auth-screen__copy", text: error || "在主机运行带有 --pair 的 serve 命令，然后用此设备扫描二维码。" }),
        );
        if (error) authScreen.append(button("重试配对", "retry-pair", {}, "auth-screen__button"));
    }

    function renderThreads(state) {
        refs.threadList.replaceChildren();
        for (const session of state.sessions.items) {
            const selected = session.id === state.sessions.selectedId;
            const item = element("li", { className: "thread-item" });
            const threadButton = element("button", {
                className: "thread-button",
                attrs: { type: "button", "aria-current": selected ? "page" : null },
                dataset: { action: "select-session", sessionId: session.id },
            }, [
                element("span", { className: "thread-button__title", text: sessionTitle(state, session) }),
                element("span", { className: "thread-button__meta", text: formatDate(session.updated_at_unix_ms) }),
            ]);
            item.append(threadButton);
            refs.threadList.append(item);
        }
        refs.threadCount.textContent = String(state.sessions.items.length || "");
        refs.threadEmpty.hidden = state.sessions.items.length > 0 || state.sessions.status === "loading";
    }

    function renderMessages(state) {
        const runs = runsForSelectedSession(state);
        const structureSignature = JSON.stringify(runs.map((run) => ({
            id: run.id,
            messages: run.messages,
            streamedOutputIds: Object.keys(run.streamedOutputs),
            activities: run.activities,
            commands: run.commands,
            progress: run.progress,
            status: run.status,
            failure: run.failure,
            error: run.error,
        })));
        if (structureSignature === messageStructureSignature) {
            const nearBottom = refs.messageList.scrollHeight - refs.messageList.scrollTop - refs.messageList.clientHeight < 120;
            for (const run of runs) {
                for (const output of Object.values(run.streamedOutputs)) {
                    const id = `stream-${run.id}-${output.outputId}`;
                    const content = refs.messageList.querySelector(
                        `[data-message-id="${CSS.escape(id)}"] .message__content`,
                    );
                    if (content && content.textContent !== output.text) content.textContent = output.text;
                }
            }
            if (nearBottom) refs.messageList.scrollTop = refs.messageList.scrollHeight;
            return;
        }
        messageStructureSignature = structureSignature;

        const openDetails = new Set(
            [...refs.messageList.querySelectorAll("details[open][data-detail-key]")]
                .map((node) => node.dataset.detailKey),
        );
        const nearBottom = refs.messageList.scrollHeight - refs.messageList.scrollTop - refs.messageList.clientHeight < 120;
        refs.messageList.replaceChildren();

        const hasContent = runs.some((run) => run.messages.length
            || Object.keys(run.streamedOutputs).length
            || run.activities.length
            || run.commands.length
            || run.progress);
        if (!hasContent) {
            refs.messageList.append(emptyTemplate.cloneNode(true));
            return;
        }

        for (const run of runs) {
            const group = element("div", { className: "run-group", dataset: { runId: run.id } });
            for (const entry of timelineForRun(run)) {
                const node = createTimelineNode(run, entry);
                if (node) group.append(node);
            }
            if (run.failure?.message) {
                group.append(element("div", {
                    className: "run-notice run-notice--error",
                    text: run.failure.message,
                    attrs: { role: "alert" },
                }));
            } else if (run.error && !run.gap) {
                group.append(element("div", { className: "run-notice", text: run.error }));
            }
            refs.messageList.append(group);
        }
        for (const details of refs.messageList.querySelectorAll("details[data-detail-key]")) {
            if (openDetails.has(details.dataset.detailKey)) details.open = true;
        }
        if (nearBottom) requestAnimationFrame(() => { refs.messageList.scrollTop = refs.messageList.scrollHeight; });
    }

    function renderPending(state) {
        const run = activeRun(state);
        const signature = JSON.stringify({ runId: run?.id ?? null, pending: run?.pending ?? [] });
        if (signature === pendingSignature) {
            for (const control of refs.pending.querySelectorAll("button, textarea")) {
                control.disabled = state.ui.composerBusy;
            }
            return;
        }
        pendingSignature = signature;
        refs.pending.replaceChildren();
        if (!run?.pending.length) {
            refs.pending.hidden = true;
            return;
        }
        for (const request of run.pending) refs.pending.append(createPendingCard(run, request));
        for (const control of refs.pending.querySelectorAll("button, textarea")) {
            control.disabled = state.ui.composerBusy;
        }
        refs.pending.hidden = false;
    }

    function renderConnection(state) {
        const label = refs.connection.querySelector(".connection-status__label");
        const status = !state.connection.online ? "offline" : state.connection.stream;
        refs.connection.dataset.state = status;
        const labels = {
            offline: "离线 · 将自动重连",
            connecting: "正在连接",
            reconnecting: `正在重连${state.connection.attempt ? ` · ${state.connection.attempt}` : ""}`,
            live: "实时连接",
            idle: "已连接",
            error: "连接中断",
        };
        label.textContent = labels[status] ?? status;
    }

    function renderComposer(state) {
        const active = activeRun(state);
        const loadingRun = currentRun(state)?.status === "loading";
        const disabled = state.ui.composerBusy || loadingRun || !state.connection.online || state.auth.status !== "authenticated";
        refs.input.disabled = disabled;
        refs.send.disabled = disabled;
        refs.cancel.hidden = !active || active.status === "stopping";
        refs.cancel.disabled = disabled;
        refs.input.placeholder = active ? "补充指令（steer）…" : "告诉 Orchestral 你想完成什么…";
        refs.hint.firstElementChild.textContent = active
            ? "当前发送会引导正在运行的任务"
            : "Enter 发送 · Shift + Enter 换行";
    }

    function renderSettings(state, context) {
        if (state.ui.settingsOpen && !refs.settings.open) {
            if (typeof refs.settings.showModal === "function") refs.settings.showModal();
            else refs.settings.setAttribute("open", "");
        }
        if (!state.ui.settingsOpen && refs.settings.open) {
            if (typeof refs.settings.close === "function") refs.settings.close();
            else refs.settings.removeAttribute("open");
        }
        refs.settingsContent.replaceChildren();

        const themeSection = element("section", { className: "settings-section" }, [
            element("h3", { text: "外观" }),
        ]);
        const select = element("select", {
            className: "settings-select",
            attrs: { "aria-label": "颜色主题" },
            dataset: { action: "theme" },
        });
        for (const [value, label] of [["auto", "跟随系统"], ["light", "浅色"], ["dark", "深色"]]) {
            const option = element("option", { text: label, attrs: { value } });
            option.selected = context.preferences.theme === value;
            select.append(option);
        }
        themeSection.append(select);

        const notificationSection = element("section", { className: "settings-section" }, [
            element("h3", { text: "通知" }),
            element("p", { text: context.notificationPermission === "granted"
                ? "页面在后台时，会在需要输入或任务结束后提醒你。"
                : "允许在任务需要输入或完成时提醒你。" }),
            button(
                context.notificationPermission === "granted" ? "通知已开启" : "开启通知",
                "enable-notifications",
                {},
                "settings-button",
            ),
        ]);
        if (context.notificationPermission === "granted") notificationSection.lastElementChild.disabled = true;

        const devices = element("section", { className: "settings-section" }, [
            element("h3", { text: "已配对设备" }),
        ]);
        if (state.devices.status === "loading") {
            devices.append(element("p", { text: "正在载入设备…" }));
        } else if (state.devices.error) {
            devices.append(element("p", { className: "settings-error", text: state.devices.error }));
        } else {
            const list = element("ul", { className: "device-list" });
            for (const device of state.devices.items) {
                const details = `${device.current ? "当前设备 · " : ""}最近使用 ${formatDate(device.last_seen_at_unix_ms)}`;
                list.append(element("li", { className: "device-item" }, [
                    element("span", { className: "device-item__copy" }, [
                        element("strong", { text: device.name }),
                        element("small", { text: details }),
                    ]),
                    button(device.current ? "撤销当前设备" : "撤销", "revoke-device", {
                        deviceId: device.id,
                        current: String(device.current),
                        deviceName: device.name,
                    }, "device-item__revoke"),
                ]));
            }
            devices.append(list);
        }
        refs.settingsContent.append(themeSection, notificationSection, devices);
    }

    function updateElapsed(state, now = Date.now()) {
        const run = activeRun(state) ?? currentRun(state);
        const label = runLabel(run, now);
        refs.runStatus.dataset.state = label.state;
        refs.runStatus.querySelector("span:last-child").textContent = label.text;
    }

    function render(state, context = {}) {
        renderAuth(state);
        if (state.auth.status !== "authenticated") return;
        refs.shell.dataset.state = "ready";
        refs.sidebar.classList.toggle("sidebar--open", state.ui.drawerOpen);
        refs.backdrop.hidden = !state.ui.drawerOpen;
        refs.sidebarToggle.setAttribute("aria-expanded", String(state.ui.drawerOpen));
        refs.install.hidden = !state.ui.installAvailable;

        renderThreads(state);
        const session = selectedSession(state);
        refs.title.textContent = session ? sessionTitle(state, session) : "新会话";
        renderMessages(state);
        renderPending(state);
        renderConnection(state);
        renderComposer(state);
        renderSettings(state, {
            preferences: { theme: "auto", ...(context.preferences ?? {}) },
            notificationPermission: context.notificationPermission ?? "default",
        });
        updateElapsed(state);

        refs.toast.replaceChildren();
        if (state.ui.notice) {
            refs.toast.append(element("div", {
                className: `toast toast--${state.ui.notice.tone}`,
                text: state.ui.notice.message,
            }));
        }
    }

    return { refs, render, updateElapsed };
}
