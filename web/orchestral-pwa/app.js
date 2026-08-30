import { ApiError, createApiClient } from "./modules/api.js";
import {
    activeRun,
    createInitialState,
    currentRun,
    isTerminalStatus,
    reducer,
    selectedSession,
} from "./modules/state.js";
import { createTokenStore, loadPreferences, savePreferences } from "./modules/storage.js";
import { createView } from "./modules/view.js";

const view = createView();
const tokenStore = createTokenStore();
const preferences = {
    theme: "auto",
    notifications: false,
    deviceName: "",
    ...loadPreferences(),
};

let state = createInitialState({ online: navigator.onLine });
let deviceToken = null;
let pairingSecret = takePairingSecret();
let deferredInstallPrompt = null;
let streamController = null;
let streamRunId = null;
let loadGeneration = 0;
let noticeSequence = 0;
let unauthorizedInProgress = false;
const reconcilingRuns = new Set();

applyTheme();

const api = createApiClient({
    getToken: () => deviceToken,
    onUnauthorized: (error) => void clearAuthentication(error.message),
});

function notificationPermission() {
    return "Notification" in window ? Notification.permission : "unsupported";
}

function render() {
    view.render(state, { preferences, notificationPermission: notificationPermission() });
}

function dispatch(action) {
    state = reducer(state, action);
    render();
}

function showNotice(message, tone = "info", timeout = 4200) {
    const id = ++noticeSequence;
    dispatch({ type: "NOTICE", message, tone, id });
    if (timeout > 0) {
        window.setTimeout(() => {
            if (state.ui.notice?.id === id) dispatch({ type: "NOTICE", message: null });
        }, timeout);
    }
}

function errorMessage(error) {
    if (error instanceof ApiError) return error.message;
    return error?.message || "发生了未知错误";
}

function takePairingSecret() {
    if (!location.hash.startsWith("#")) return null;
    const values = new URLSearchParams(location.hash.slice(1));
    const secret = values.get("pair");
    if (!secret) return null;

    // Remove the one-time credential from the visible URL and browser history
    // immediately; the in-memory value survives long enough to finish claiming.
    values.delete("pair");
    const remainder = values.toString();
    history.replaceState(history.state, "", `${location.pathname}${location.search}${remainder ? `#${remainder}` : ""}`);
    return secret;
}

function defaultDeviceName() {
    if (preferences.deviceName) return preferences.deviceName;
    const ua = navigator.userAgent;
    const family = /iPad/i.test(ua)
        ? "iPad"
        : /iPhone/i.test(ua)
            ? "iPhone"
            : /Android/i.test(ua)
                ? "Android device"
                : navigator.userAgentData?.platform || navigator.platform || "Browser";
    return `${family} · Orchestral`.slice(0, 80);
}

async function clearAuthentication(message = null) {
    if (unauthorizedInProgress) return;
    unauthorizedInProgress = true;
    stopStream();
    deviceToken = null;
    await tokenStore.clear();
    dispatch({ type: "AUTH_CLEARED", error: message });
    unauthorizedInProgress = false;
}

async function claimPairing() {
    if (!pairingSecret) {
        dispatch({ type: "AUTH_CLEARED", error: "配对链接已失效，请从主机生成新的二维码。" });
        return;
    }
    dispatch({ type: "AUTH_STATUS", status: "pairing", error: null });
    try {
        const deviceName = defaultDeviceName();
        const claim = await api.claimPairing(pairingSecret, deviceName);
        await tokenStore.set(claim.token);
        deviceToken = claim.token;
        preferences.deviceName = claim.device?.name || deviceName;
        savePreferences(preferences);
        pairingSecret = null;
        dispatch({
            type: "AUTH_STATUS",
            status: "authenticated",
            device: claim.device,
            me: claim.device ? { device_id: claim.device.id } : null,
        });
        showNotice("设备配对成功", "success");
        await loadWorkspace();
    } catch (error) {
        dispatch({ type: "AUTH_STATUS", status: "error", error: errorMessage(error) });
    }
}

async function bootstrapAuthentication() {
    if (pairingSecret) {
        await claimPairing();
        return;
    }

    deviceToken = await tokenStore.get();
    if (!deviceToken) {
        dispatch({ type: "AUTH_CLEARED" });
        return;
    }

    try {
        const me = await api.me();
        dispatch({ type: "AUTH_STATUS", status: "authenticated", me });
    } catch (error) {
        if (error instanceof ApiError && error.status === 401) return;
        // A retained token still lets the installed shell open offline. It is
        // verified as soon as connectivity comes back.
        dispatch({ type: "AUTH_STATUS", status: "authenticated", error: null });
        dispatch({ type: "STREAM_STATUS", status: navigator.onLine ? "error" : "offline", error: errorMessage(error) });
    }
    await loadWorkspace();
}

async function loadWorkspace() {
    if (!deviceToken || state.auth.status !== "authenticated") return;
    await Promise.allSettled([refreshDevices(), refreshSessions({ loadSelection: true })]);
}

async function refreshDevices() {
    dispatch({ type: "DEVICES_LOADING" });
    try {
        const devices = await api.listDevices();
        dispatch({ type: "DEVICES_LOADED", devices });
    } catch (error) {
        if (error?.name === "AbortError" || error?.status === 401) return;
        dispatch({ type: "DEVICES_ERROR", error: errorMessage(error) });
    }
}

async function refreshSessions({ loadSelection = false } = {}) {
    const previousSelectedId = state.sessions.selectedId;
    const previousRunIds = state.sessions.items.find(
        (item) => item.id === previousSelectedId,
    )?.run_ids ?? [];
    dispatch({ type: "SESSIONS_LOADING" });
    try {
        const sessions = await api.listSessions();
        dispatch({ type: "SESSIONS_LOADED", sessions });
        if (loadSelection && state.sessions.selectedId) {
            const selected = state.sessions.items.find(
                (item) => item.id === state.sessions.selectedId,
            );
            const runIds = selected?.run_ids ?? [];
            const selectionChanged = previousSelectedId !== state.sessions.selectedId;
            const runSetChanged = previousRunIds.length !== runIds.length
                || previousRunIds.some((runId, index) => runId !== runIds[index]);
            const hasMissingRun = runIds.some((runId) => !state.runs.byId[runId]);
            if (selectionChanged || runSetChanged || hasMissingRun) {
                await loadSession(state.sessions.selectedId);
            }
        }
    } catch (error) {
        if (error?.name === "AbortError" || error?.status === 401) return;
        dispatch({ type: "SESSIONS_ERROR", error: errorMessage(error) });
        dispatch({ type: "STREAM_STATUS", status: navigator.onLine ? "error" : "offline", error: errorMessage(error) });
    }
}

function applyRunRead(runId, sessionId, viewResult, eventsResult) {
    const applyEvents = () => {
        if (eventsResult.status !== "fulfilled") return;
        for (const record of eventsResult.value.records ?? []) {
            dispatch({ type: "RUN_DURABLE", runId, sessionId, record });
        }
    };
    const applyView = () => {
        if (viewResult.status === "fulfilled") {
            dispatch({ type: "RUN_VIEW", runId, sessionId, view: viewResult.value });
        }
    };

    if (viewResult.status === "fulfilled" && eventsResult.status === "fulfilled") {
        const viewCursor = Number(viewResult.value.last_run_seq ?? 0);
        const eventsCursor = Number(eventsResult.value.next ?? 0);
        if (viewCursor >= eventsCursor) {
            applyEvents();
            applyView();
        } else {
            applyView();
            applyEvents();
        }
        return;
    }
    applyEvents();
    applyView();
}

async function loadRunSnapshot(runId, sessionId, generation) {
    dispatch({ type: "RUN_LOADING", runId, sessionId });
    const [viewResult, eventsResult] = await Promise.allSettled([
        api.getRun(runId),
        api.getEvents(runId, state.runs.byId[runId]?.cursor ?? 0),
    ]);
    if (generation !== loadGeneration) return;
    applyRunRead(runId, sessionId, viewResult, eventsResult);
    const failure = [viewResult, eventsResult]
        .find((result) => result.status === "rejected")?.reason;
    if (failure && failure?.name !== "AbortError" && failure?.status !== 401) {
        dispatch({ type: "RUN_ERROR", runId, sessionId, error: errorMessage(failure) });
    }
}

async function loadSession(sessionId) {
    const generation = ++loadGeneration;
    stopStream();
    const session = state.sessions.items.find((item) => item.id === sessionId);
    if (!session) return;
    await Promise.all((session.run_ids ?? []).map(
        (runId) => loadRunSnapshot(runId, sessionId, generation),
    ));
    if (generation !== loadGeneration || state.sessions.selectedId !== sessionId) return;
    const run = activeRun(state);
    if (run) startStream(run.id);
    else dispatch({ type: "STREAM_STATUS", status: navigator.onLine ? "idle" : "offline", attempt: 0 });
}

async function refreshRun(runId) {
    const run = state.runs.byId[runId];
    if (!run) return;
    const [eventsResult, viewResult] = await Promise.allSettled([
        api.getEvents(runId, run.cursor),
        api.getRun(runId),
    ]);
    applyRunRead(runId, run.sessionId, viewResult, eventsResult);
    const failure = [eventsResult, viewResult]
        .find((result) => result.status === "rejected")?.reason;
    if (failure && failure?.name !== "AbortError" && failure?.status !== 401) {
        dispatch({ type: "RUN_ERROR", runId, error: errorMessage(failure) });
    }
}

async function reconcileRun(runId) {
    if (reconcilingRuns.has(runId)) return;
    reconcilingRuns.add(runId);
    try {
        await refreshRun(runId);
    } finally {
        reconcilingRuns.delete(runId);
    }
}

function wait(milliseconds, signal) {
    return new Promise((resolve, reject) => {
        const timer = window.setTimeout(resolve, milliseconds);
        signal?.addEventListener("abort", () => {
            window.clearTimeout(timer);
            reject(signal.reason ?? new DOMException("Aborted", "AbortError"));
        }, { once: true });
    });
}

function waitUntilOnline(signal) {
    if (navigator.onLine) return Promise.resolve();
    return new Promise((resolve, reject) => {
        const online = () => {
            cleanup();
            resolve();
        };
        const aborted = () => {
            cleanup();
            reject(signal.reason ?? new DOMException("Aborted", "AbortError"));
        };
        const cleanup = () => {
            window.removeEventListener("online", online);
            signal.removeEventListener("abort", aborted);
        };
        window.addEventListener("online", online, { once: true });
        signal.addEventListener("abort", aborted, { once: true });
    });
}

function parseStreamData(event) {
    try {
        return JSON.parse(event.data);
    } catch {
        throw new ApiError("Host sent an invalid stream event", { code: "stream_decode_failed" });
    }
}

async function notify(title, body, tag) {
    if (!document.hidden || !preferences.notifications || notificationPermission() !== "granted") return;
    const options = {
        body,
        tag,
        icon: "./icons/icon-192.png",
        badge: "./icons/favicon.svg",
        renotify: true,
        data: { url: location.href },
    };
    try {
        const registration = await navigator.serviceWorker?.ready;
        if (registration?.showNotification) {
            await registration.showNotification(title, options);
        } else {
            new Notification(title, options);
        }
    } catch {
        // Notification delivery is best effort and never affects a Run.
    }
}

function notifyForDurable(previous, next) {
    if (!document.hidden) return;
    const priorPending = new Set(previous?.pending.map((request) => request.request_id) ?? []);
    const opened = next.pending.find((request) => request.blocking && !priorPending.has(request.request_id));
    if (opened) {
        const kind = opened.payload?.type === "approval" ? "批准" : "输入";
        void notify("Orchestral 正在等你", `任务需要你的${kind}才能继续。`, `waiting-${next.id}`);
    }
    if (previous && !isTerminalStatus(previous.status) && isTerminalStatus(next.status)) {
        const successful = next.status === "delivered";
        void notify(
            successful ? "任务已完成" : "任务已结束",
            successful ? "Orchestral 已准备好结果。" : `运行状态：${next.status}`,
            `complete-${next.id}`,
        );
    }
}

function handleStreamEvent(runId, event) {
    if (event.type === "durable") {
        const record = parseStreamData(event);
        if (record?.event?.run_id !== runId) {
            throw new ApiError("Stream event belongs to a different run", { code: "stream_run_mismatch" });
        }
        const previous = state.runs.byId[runId];
        dispatch({ type: "RUN_DURABLE", runId, sessionId: previous?.sessionId, record });
        const next = state.runs.byId[runId];
        notifyForDurable(previous, next);
        if (next.gap) void reconcileRun(runId);
        if (isTerminalStatus(next.status)) {
            queueMicrotask(() => {
                if (streamRunId === runId) stopStream();
                void refreshSessions();
            });
        }
        return;
    }
    if (event.type === "telemetry") {
        const telemetry = parseStreamData(event);
        if (telemetry?.run_id === runId) {
            dispatch({ type: "RUN_TELEMETRY", runId, telemetry });
        }
        return;
    }
    if (event.type === "error") {
        const detail = parseStreamData(event);
        throw new ApiError(detail.message || "Live stream failed", {
            code: detail.code || "stream_failed",
        });
    }
}

async function followRun(runId, controller) {
    let attempt = 0;
    while (!controller.signal.aborted && streamRunId === runId) {
        const run = state.runs.byId[runId];
        if (!run || isTerminalStatus(run.status)) break;
        try {
            if (!navigator.onLine) {
                dispatch({ type: "STREAM_STATUS", status: "offline", attempt });
                await waitUntilOnline(controller.signal);
            }
            dispatch({
                type: "STREAM_STATUS",
                status: attempt === 0 ? "connecting" : "reconnecting",
                attempt,
            });
            const cursor = state.runs.byId[runId]?.cursor ?? 0;
            await api.openRunStream(runId, cursor, {
                signal: controller.signal,
                onOpen: () => {
                    attempt = 0;
                    dispatch({ type: "STREAM_STATUS", status: "live", attempt: 0 });
                },
                onEvent: (event) => handleStreamEvent(runId, event),
            });
            if (controller.signal.aborted || isTerminalStatus(state.runs.byId[runId]?.status)) break;
            throw new ApiError("Live stream closed", { code: "stream_closed" });
        } catch (error) {
            if (controller.signal.aborted || error?.name === "AbortError" || error?.status === 401) break;
            attempt += 1;
            dispatch({
                type: "STREAM_STATUS",
                status: navigator.onLine ? "reconnecting" : "offline",
                attempt,
                error: errorMessage(error),
            });
            const backoff = Math.min(15_000, 700 * (2 ** Math.min(attempt - 1, 5)));
            const jitter = Math.floor(Math.random() * 350);
            try {
                await wait(backoff + jitter, controller.signal);
            } catch {
                break;
            }
        }
    }
}

function startStream(runId) {
    if (streamRunId === runId && streamController && !streamController.signal.aborted) return;
    stopStream();
    streamController = new AbortController();
    streamRunId = runId;
    void followRun(runId, streamController);
}

function stopStream() {
    const hadStream = Boolean(streamController);
    streamController?.abort();
    streamController = null;
    streamRunId = null;
    if (hadStream && state.auth.status === "authenticated") {
        dispatch({
            type: "STREAM_STATUS",
            status: navigator.onLine ? "idle" : "offline",
            attempt: 0,
        });
    }
}

function checkAck(ack, operation) {
    switch (ack?.state?.state) {
        case "accepted":
        case "applied":
            return ack;
        case "rejected":
            throw new ApiError(ack.state.message || `${operation}被拒绝`, { code: ack.state.code });
        case "unsupported":
            throw new ApiError(`主机不支持此操作：${ack.state.feature}`, { code: "unsupported" });
        default:
            throw new ApiError(`${operation}没有返回有效确认`, { code: "invalid_ack" });
    }
}

async function createSession() {
    if (!navigator.onLine) {
        showNotice("离线时无法创建会话", "warning");
        return null;
    }
    dispatch({ type: "COMPOSER_BUSY", busy: true });
    try {
        const session = await api.createSession();
        dispatch({ type: "SESSION_UPSERT", session });
        dispatch({ type: "UI_PATCH", patch: { drawerOpen: false } });
        await loadSession(session.id);
        view.refs.input.focus();
        return session;
    } catch (error) {
        if (error?.status !== 401) showNotice(errorMessage(error), "error");
        return null;
    } finally {
        dispatch({ type: "COMPOSER_BUSY", busy: false });
    }
}

async function submitComposer(text) {
    const input = text.trim();
    if (!input || state.ui.composerBusy) return;
    if (!navigator.onLine) {
        showNotice("当前离线，恢复连接后再发送", "warning");
        return;
    }
    if (currentRun(state)?.status === "loading") {
        showNotice("会话仍在载入，请稍候", "info");
        return;
    }

    dispatch({ type: "COMPOSER_BUSY", busy: true });
    try {
        let session = selectedSession(state);
        if (!session) {
            session = await api.createSession();
            dispatch({ type: "SESSION_UPSERT", session });
        }

        const running = activeRun(state);
        if (running) {
            const ack = checkAck(await api.steer(running.id, input), "引导");
            dispatch({
                type: "RUN_OPTIMISTIC_MESSAGE",
                runId: running.id,
                sessionId: session.id,
                id: `steer-${ack.command_id}`,
                text: input,
                steering: true,
            });
        } else {
            const requestedRunId = crypto.randomUUID();
            const response = await api.startRun(session.id, { runId: requestedRunId, input });
            const runId = String(response.run_id ?? requestedRunId);
            const updatedSession = {
                ...session,
                updated_at_unix_ms: Date.now(),
                run_ids: [...new Set([...(session.run_ids ?? []), runId])],
            };
            dispatch({ type: "SESSION_UPSERT", session: updatedSession });
            dispatch({ type: "RUN_OPTIMISTIC_START", runId, sessionId: session.id, input });
            dispatch({ type: "RUN_VIEW", runId, sessionId: session.id, view: response.view });
            try {
                const page = await api.getEvents(runId, 0);
                for (const record of page.records ?? []) {
                    dispatch({ type: "RUN_DURABLE", runId, sessionId: session.id, record });
                }
                dispatch({ type: "RUN_VIEW", runId, sessionId: session.id, view: response.view });
            } catch (replayError) {
                if (replayError?.status !== 401 && replayError?.name !== "AbortError") {
                    dispatch({ type: "RUN_ERROR", runId, sessionId: session.id, error: errorMessage(replayError) });
                }
            }
            if (!isTerminalStatus(state.runs.byId[runId]?.status)) startStream(runId);
        }
        view.refs.input.value = "";
        resizeComposer();
    } catch (error) {
        if (error?.status !== 401) showNotice(errorMessage(error), "error", 6500);
    } finally {
        dispatch({ type: "COMPOSER_BUSY", busy: false });
    }
}

async function cancelActiveRun() {
    const run = activeRun(state);
    if (!run || state.ui.composerBusy) return;
    dispatch({ type: "COMPOSER_BUSY", busy: true });
    try {
        checkAck(await api.cancel(run.id, "Cancelled from paired device"), "取消");
        showNotice("已请求停止任务", "info");
    } catch (error) {
        if (error?.status !== 401) showNotice(errorMessage(error), "error");
    } finally {
        dispatch({ type: "COMPOSER_BUSY", busy: false });
    }
}

async function resolveInput(form) {
    const text = new FormData(form).get("response")?.toString().trim();
    if (!text) return;
    const { runId, requestId } = form.dataset;
    dispatch({ type: "COMPOSER_BUSY", busy: true });
    try {
        checkAck(await api.resolveInput(runId, requestId, text), "回复");
        await refreshRun(runId);
    } catch (error) {
        if (error?.status !== 401) showNotice(errorMessage(error), "error");
    } finally {
        dispatch({ type: "COMPOSER_BUSY", busy: false });
    }
}

async function resolveApproval(target) {
    const { runId, requestId, decision } = target.dataset;
    dispatch({ type: "COMPOSER_BUSY", busy: true });
    try {
        checkAck(await api.resolveApproval(runId, requestId, decision), "批准");
        await refreshRun(runId);
    } catch (error) {
        if (error?.status !== 401) showNotice(errorMessage(error), "error");
    } finally {
        dispatch({ type: "COMPOSER_BUSY", busy: false });
    }
}

async function revokeDevice(target) {
    const { deviceId, deviceName, current } = target.dataset;
    const warning = current === "true"
        ? "撤销当前设备后需要重新配对。确定继续吗？"
        : `确定撤销“${deviceName}”吗？该设备会立即失去访问权限。`;
    if (!window.confirm(warning)) return;
    target.disabled = true;
    try {
        await api.revokeDevice(deviceId);
        dispatch({ type: "DEVICE_REMOVED", deviceId });
        if (current === "true") await clearAuthentication("当前设备已撤销，请重新配对。 ");
        else showNotice("设备访问权已撤销", "success");
    } catch (error) {
        target.disabled = false;
        if (error?.status !== 401) showNotice(errorMessage(error), "error");
    }
}

async function copyText(value) {
    try {
        await navigator.clipboard.writeText(value);
    } catch {
        const area = document.createElement("textarea");
        area.value = value;
        area.style.position = "fixed";
        area.style.opacity = "0";
        document.body.append(area);
        area.select();
        document.execCommand("copy");
        area.remove();
    }
    showNotice("已复制", "success", 1800);
}

async function requestNotifications() {
    if (!("Notification" in window)) {
        showNotice("此浏览器不支持通知", "warning");
        return;
    }
    const permission = await Notification.requestPermission();
    preferences.notifications = permission === "granted";
    savePreferences(preferences);
    render();
    if (permission !== "granted") showNotice("通知权限未开启", "warning");
}

async function installApp() {
    if (deferredInstallPrompt) {
        deferredInstallPrompt.prompt();
        await deferredInstallPrompt.userChoice;
        deferredInstallPrompt = null;
        dispatch({ type: "UI_PATCH", patch: { installAvailable: false } });
        return;
    }
    const ios = /iPad|iPhone|iPod/.test(navigator.userAgent);
    showNotice(
        ios ? "在 Safari 分享菜单中选择“添加到主屏幕”" : "请在浏览器菜单中选择“安装应用”",
        "info",
        7000,
    );
}

function applyTheme() {
    document.documentElement.dataset.theme = preferences.theme;
}

function resizeComposer() {
    const input = view.refs.input;
    input.style.height = "auto";
    input.style.height = `${Math.min(input.scrollHeight, 180)}px`;
}

async function handleAction(target) {
    switch (target.dataset.action) {
        case "new-thread":
            await createSession();
            break;
        case "select-session":
            dispatch({ type: "SESSION_SELECTED", sessionId: target.dataset.sessionId });
            await loadSession(target.dataset.sessionId);
            break;
        case "settings":
            dispatch({ type: "UI_PATCH", patch: { settingsOpen: true, drawerOpen: false } });
            void refreshDevices();
            break;
        case "install":
            await installApp();
            break;
        case "cancel":
            await cancelActiveRun();
            break;
        case "resolve-approval":
            await resolveApproval(target);
            break;
        case "revoke-device":
            await revokeDevice(target);
            break;
        case "enable-notifications":
            await requestNotifications();
            break;
        case "copy-message": {
            const node = document.querySelector(`[data-message-id="${CSS.escape(target.dataset.messageId)}"] .message__content`);
            if (node) await copyText(node.textContent);
            break;
        }
        case "retry-pair":
            await claimPairing();
            break;
        default:
            break;
    }
}

function bindEvents() {
    document.querySelector("#new-thread-button").dataset.action = "new-thread";
    document.querySelector("#settings-button").dataset.action = "settings";
    view.refs.install.dataset.action = "install";
    view.refs.cancel.dataset.action = "cancel";

    document.addEventListener("click", (event) => {
        const prompt = event.target.closest("[data-prompt]");
        if (prompt) {
            view.refs.input.value = prompt.dataset.prompt;
            resizeComposer();
            view.refs.input.focus();
            return;
        }
        const target = event.target.closest("[data-action]");
        if (target) void handleAction(target);
    });

    document.addEventListener("submit", (event) => {
        if (event.target === view.refs.composer) {
            event.preventDefault();
            void submitComposer(view.refs.input.value);
        } else if (event.target.matches('[data-action="resolve-input"]')) {
            event.preventDefault();
            void resolveInput(event.target);
        }
    });

    document.addEventListener("change", (event) => {
        if (event.target.matches('[data-action="theme"]')) {
            preferences.theme = event.target.value;
            savePreferences(preferences);
            applyTheme();
        }
    });

    view.refs.input.addEventListener("input", resizeComposer);
    view.refs.input.addEventListener("keydown", (event) => {
        if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
            event.preventDefault();
            view.refs.composer.requestSubmit();
        }
    });
    document.querySelector("#mobile-sidebar-toggle").addEventListener("click", () => {
        dispatch({ type: "UI_PATCH", patch: { drawerOpen: !state.ui.drawerOpen } });
    });
    view.refs.backdrop.addEventListener("click", () => {
        dispatch({ type: "UI_PATCH", patch: { drawerOpen: false } });
    });
    view.refs.settings.addEventListener("close", () => {
        if (state.ui.settingsOpen) dispatch({ type: "UI_PATCH", patch: { settingsOpen: false } });
    });

    window.addEventListener("online", () => {
        dispatch({ type: "ONLINE_CHANGED", online: true });
        if (deviceToken) void loadWorkspace();
    });
    window.addEventListener("offline", () => dispatch({ type: "ONLINE_CHANGED", online: false }));
    const refreshVisibleSessions = () => {
        if (
            document.visibilityState !== "hidden"
            && navigator.onLine
            && deviceToken
            && state.auth.status === "authenticated"
        ) {
            void refreshSessions({ loadSelection: true });
        }
    };
    document.addEventListener("visibilitychange", refreshVisibleSessions);
    window.addEventListener("focus", refreshVisibleSessions);
    window.addEventListener("beforeinstallprompt", (event) => {
        event.preventDefault();
        deferredInstallPrompt = event;
        dispatch({ type: "UI_PATCH", patch: { installAvailable: true } });
    });
    window.addEventListener("appinstalled", () => {
        deferredInstallPrompt = null;
        dispatch({ type: "UI_PATCH", patch: { installAvailable: false } });
        showNotice("Orchestral 已安装", "success");
    });
    window.addEventListener("keydown", (event) => {
        if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
            event.preventDefault();
            void createSession();
        }
    });
}

async function registerServiceWorker() {
    if (!("serviceWorker" in navigator)) return;
    try {
        await navigator.serviceWorker.register("./sw.js", { scope: "./" });
    } catch (error) {
        console.warn("Service worker registration failed", error);
    }
}

bindEvents();
const standalone = matchMedia("(display-mode: standalone)").matches || navigator.standalone === true;
state = reducer(state, { type: "UI_PATCH", patch: { installAvailable: !standalone } });
render();
window.setInterval(() => view.updateElapsed(state), 1000);
window.setInterval(() => {
    if (document.visibilityState !== "hidden" && navigator.onLine && deviceToken) {
        void refreshSessions({ loadSelection: true });
    }
}, 30_000);
void registerServiceWorker();
void bootstrapAuthentication();
