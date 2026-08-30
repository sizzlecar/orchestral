const TERMINAL = new Set(["delivered", "incomplete", "cancelled", "failed"]);
const CONTROLLABLE = new Set(["accepted", "running", "waiting", "stopping", "unknown"]);
const MAX_TELEMETRY_IDS = 800;

export function createInitialState({ online = true } = {}) {
    return {
        auth: {
            status: "booting",
            me: null,
            device: null,
            error: null,
        },
        sessions: {
            status: "idle",
            items: [],
            selectedId: null,
            error: null,
        },
        devices: {
            status: "idle",
            items: [],
            error: null,
        },
        runs: {
            byId: {},
            order: [],
        },
        connection: {
            online,
            stream: online ? "idle" : "offline",
            attempt: 0,
            error: null,
            lastConnectedAt: null,
        },
        ui: {
            drawerOpen: false,
            settingsOpen: false,
            composerBusy: false,
            installing: false,
            installAvailable: false,
            notice: null,
        },
    };
}

export function isTerminalStatus(status) {
    return TERMINAL.has(status);
}

export function contentText(content) {
    if (!content || typeof content !== "object") {
        return "";
    }
    const body = content.body;
    if (body?.kind === "inline") {
        if (typeof body.value === "string") {
            return body.value;
        }
        try {
            return JSON.stringify(body.value, null, 2);
        } catch {
            return String(body.value ?? "");
        }
    }
    if (body?.kind === "artifact") {
        const reference = body.value?.artifact_ref;
        return reference ? `[Artifact: ${reference}]` : "[Artifact]";
    }
    return "";
}

export function contentsText(contents) {
    return (Array.isArray(contents) ? contents : [])
        .map(contentText)
        .filter(Boolean)
        .join("\n");
}

export function statusFromView(view) {
    const state = view?.state;
    if (!state || typeof state.state !== "string") {
        return "unknown";
    }
    if (state.state !== "terminal") {
        return state.state;
    }
    return state.terminal?.type || "unknown";
}

function emptyRun(id, sessionId = null) {
    return {
        id,
        sessionId,
        status: "loading",
        view: null,
        cursor: 0,
        serverCursor: 0,
        eventIds: [],
        sequenceIds: {},
        messages: [],
        streamedOutputs: {},
        committedOutputIds: [],
        telemetryIds: [],
        activities: [],
        commands: [],
        pending: [],
        progress: null,
        delivery: null,
        partialDelivery: null,
        failure: null,
        gap: null,
        startedAt: null,
        completedAt: null,
        error: null,
    };
}

function ensureRun(state, runId, sessionId = null) {
    return state.runs.byId[runId] ?? emptyRun(runId, sessionId);
}

function putRun(state, run) {
    const exists = Boolean(state.runs.byId[run.id]);
    return {
        ...state,
        runs: {
            byId: { ...state.runs.byId, [run.id]: run },
            order: exists ? state.runs.order : [...state.runs.order, run.id],
        },
    };
}

function withRun(state, runId, sessionId, update) {
    return putRun(state, update(ensureRun(state, runId, sessionId)));
}

function replaceOrAppendOptimistic(messages, message) {
    const optimisticIndex = messages.findIndex(
        (item) => item.role === message.role && item.optimistic,
    );
    if (optimisticIndex < 0) {
        return [...messages, message];
    }
    const next = [...messages];
    next[optimisticIndex] = message;
    return next;
}

function appendIfNewText(messages, message) {
    if (!message.text) {
        return messages;
    }
    const duplicate = messages.some(
        (item) => item.role === message.role && item.text === message.text && !item.optimistic,
    );
    return duplicate ? messages : [...messages, message];
}

function terminalTime(status, previous, now) {
    return isTerminalStatus(status) ? (previous ?? now) : null;
}

function commandSummary(command) {
    const payload = command?.payload ?? {};
    switch (payload.type) {
        case "steer":
            return contentsText(payload.content);
        case "cancel":
            return payload.reason || "Cancel";
        case "resolve_request":
            return `Resolve ${command.request_id || "request"}`;
        default:
            return payload.type || "Command";
    }
}

export function projectDurableRecord(run, record, now = Date.now()) {
    const event = record?.event ?? record;
    const sequence = Number(event?.run_seq);
    const eventId = event?.event_id;
    const payload = event?.payload;

    if (!Number.isSafeInteger(sequence) || sequence <= 0 || !eventId || !payload?.type) {
        return { ...run, error: "Received a malformed durable event" };
    }

    if (sequence <= run.cursor) {
        const priorId = run.sequenceIds[sequence];
        if (priorId && priorId !== eventId) {
            return {
                ...run,
                error: `Journal conflict at run_seq ${sequence}`,
                gap: { expected: sequence, received: sequence, conflict: true },
            };
        }
        return run;
    }

    if (sequence !== run.cursor + 1) {
        return {
            ...run,
            gap: { expected: run.cursor + 1, received: sequence, conflict: false },
            error: `Waiting for durable event ${run.cursor + 1}`,
        };
    }

    let next = {
        ...run,
        cursor: sequence,
        serverCursor: Math.max(run.serverCursor, sequence),
        eventIds: [...run.eventIds, eventId],
        sequenceIds: { ...run.sequenceIds, [sequence]: eventId },
        gap: null,
        error: null,
    };

    switch (payload.type) {
        case "run_accepted":
            next.status = "accepted";
            break;
        case "run_started":
            next.status = "running";
            next.startedAt ??= now;
            break;
        case "input_committed": {
            const text = contentsText(payload.content);
            next.messages = replaceOrAppendOptimistic(next.messages, {
                id: eventId,
                role: "user",
                text,
                seq: sequence,
                optimistic: false,
            });
            break;
        }
        case "output_committed": {
            const text = contentsText(payload.content);
            next.messages = appendIfNewText(next.messages, {
                id: eventId,
                role: "assistant",
                text,
                seq: sequence,
                outputId: payload.output_id,
                optimistic: false,
            });
            next.committedOutputIds = next.committedOutputIds.includes(payload.output_id)
                ? next.committedOutputIds
                : [...next.committedOutputIds, payload.output_id];
            if (Object.hasOwn(next.streamedOutputs, payload.output_id)) {
                const streamedOutputs = { ...next.streamedOutputs };
                delete streamedOutputs[payload.output_id];
                next.streamedOutputs = streamedOutputs;
            }
            break;
        }
        case "request_opened":
            next.pending = [
                ...next.pending.filter((request) => request.request_id !== payload.request.request_id),
                payload.request,
            ];
            if (payload.request.blocking) {
                next.status = "waiting";
            }
            break;
        case "request_resolved":
            next.pending = next.pending.filter(
                (request) => request.request_id !== payload.request_id,
            );
            if (next.status === "waiting" && !next.pending.some((request) => request.blocking)) {
                next.status = "running";
            }
            break;
        case "command_received": {
            const command = payload.command;
            next.commands = [
                ...next.commands.filter((item) => item.id !== command.command_id),
                {
                    id: command.command_id,
                    type: command.payload?.type ?? "command",
                    summary: commandSummary(command),
                    requestId: command.request_id ?? null,
                    state: "received",
                    seq: sequence,
                },
            ];
            break;
        }
        case "command_disposition_recorded": {
            const existing = next.commands.find((item) => item.id === payload.command_id);
            const item = {
                ...(existing ?? {
                    id: payload.command_id,
                    type: "command",
                    summary: "",
                    requestId: null,
                }),
                state: payload.outcome?.outcome ?? "recorded",
                outcome: payload.outcome,
                dispositionSeq: sequence,
            };
            next.commands = [
                ...next.commands.filter((command) => command.id !== payload.command_id),
                item,
            ];
            break;
        }
        case "stop_requested":
            next.status = "stopping";
            break;
        case "delivery_committed": {
            next.status = "delivered";
            next.delivery = payload.delivery;
            next.completedAt = terminalTime(next.status, next.completedAt, now);
            next.pending = [];
            const text = contentText(payload.delivery?.final_response);
            next.messages = appendIfNewText(next.messages, {
                id: `${eventId}-delivery`,
                role: "assistant",
                text,
                seq: sequence,
                optimistic: false,
            });
            break;
        }
        case "run_incomplete": {
            next.status = "incomplete";
            next.partialDelivery = payload.partial_delivery ?? null;
            next.completedAt = terminalTime(next.status, next.completedAt, now);
            next.pending = [];
            const text = contentText(payload.partial_delivery?.response);
            next.messages = appendIfNewText(next.messages, {
                id: `${eventId}-partial`,
                role: "assistant",
                text,
                seq: sequence,
                partial: true,
                optimistic: false,
            });
            break;
        }
        case "run_failed":
            next.status = "failed";
            next.failure = payload.failure;
            next.completedAt = terminalTime(next.status, next.completedAt, now);
            next.pending = [];
            break;
        case "run_cancelled":
            next.status = "cancelled";
            next.failure = { code: "cancelled", message: payload.reason };
            next.completedAt = terminalTime(next.status, next.completedAt, now);
            next.pending = [];
            break;
        case "continuity_lost":
            next.status = "unknown";
            next.error = payload.reason;
            break;
        case "continuity_restored":
            next.status = next.pending.some((request) => request.blocking) ? "waiting" : "running";
            next.error = null;
            break;
        default:
            break;
    }

    return next;
}

export function projectTelemetry(run, telemetry) {
    const telemetryId = telemetry?.telemetry_id;
    const payload = telemetry?.payload;
    if (!telemetryId || !payload?.type || run.telemetryIds.includes(telemetryId)) {
        return run;
    }

    const telemetryIds = [...run.telemetryIds, telemetryId].slice(-MAX_TELEMETRY_IDS);
    let next = { ...run, telemetryIds };
    switch (payload.type) {
        case "output_delta": {
            if (run.committedOutputIds.includes(payload.output_id)) {
                break;
            }
            const text = contentText(payload.delta);
            if (!text) {
                break;
            }
            const previous = run.streamedOutputs[payload.output_id];
            next.streamedOutputs = {
                ...run.streamedOutputs,
                [payload.output_id]: {
                    outputId: payload.output_id,
                    text: `${previous?.text ?? ""}${text}`,
                    telemetryId,
                },
            };
            break;
        }
        case "progress_reported":
            next.progress = {
                message: payload.message,
                fraction: payload.fraction ?? null,
                telemetryId,
            };
            break;
        case "tool_activity": {
            const activity = {
                id: payload.activity_id,
                toolName: payload.tool_name,
                state: payload.state,
                evidence: Array.isArray(payload.evidence) ? payload.evidence : [],
                telemetryId,
            };
            const existingIndex = run.activities.findIndex((item) => item.id === activity.id);
            if (existingIndex < 0) {
                next.activities = [...run.activities, activity];
            } else {
                next.activities = [...run.activities];
                next.activities[existingIndex] = activity;
            }
            break;
        }
        default:
            break;
    }
    return next;
}

function mergeSession(items, session) {
    return [session, ...items.filter((item) => item.id !== session.id)]
        .sort((left, right) => right.updated_at_unix_ms - left.updated_at_unix_ms);
}

export function reducer(state, action) {
    switch (action.type) {
        case "AUTH_STATUS":
            return {
                ...state,
                auth: {
                    ...state.auth,
                    status: action.status,
                    error: action.error ?? null,
                    me: action.me ?? state.auth.me,
                    device: action.device ?? state.auth.device,
                },
            };
        case "AUTH_CLEARED":
            return {
                ...createInitialState({ online: state.connection.online }),
                auth: { status: "unpaired", me: null, device: null, error: action.error ?? null },
            };
        case "ONLINE_CHANGED":
            return {
                ...state,
                connection: {
                    ...state.connection,
                    online: action.online,
                    stream: action.online
                        ? (state.connection.stream === "offline" ? "reconnecting" : state.connection.stream)
                        : "offline",
                    error: action.online ? state.connection.error : null,
                },
            };
        case "STREAM_STATUS":
            return {
                ...state,
                connection: {
                    ...state.connection,
                    stream: action.status,
                    attempt: action.attempt ?? state.connection.attempt,
                    error: action.error ?? null,
                    lastConnectedAt: action.status === "live"
                        ? (action.now ?? Date.now())
                        : state.connection.lastConnectedAt,
                },
            };
        case "SESSIONS_LOADING":
            return { ...state, sessions: { ...state.sessions, status: "loading", error: null } };
        case "SESSIONS_LOADED": {
            const items = [...action.sessions].sort(
                (left, right) => right.updated_at_unix_ms - left.updated_at_unix_ms,
            );
            const selectedExists = items.some((item) => item.id === state.sessions.selectedId);
            return {
                ...state,
                sessions: {
                    status: "ready",
                    items,
                    selectedId: selectedExists ? state.sessions.selectedId : (items[0]?.id ?? null),
                    error: null,
                },
            };
        }
        case "SESSIONS_ERROR":
            return {
                ...state,
                sessions: { ...state.sessions, status: "error", error: action.error },
            };
        case "SESSION_UPSERT":
            return {
                ...state,
                sessions: {
                    ...state.sessions,
                    status: "ready",
                    items: mergeSession(state.sessions.items, action.session),
                    selectedId: action.select === false
                        ? state.sessions.selectedId
                        : action.session.id,
                    error: null,
                },
            };
        case "SESSION_SELECTED":
            return {
                ...state,
                sessions: { ...state.sessions, selectedId: action.sessionId },
                ui: { ...state.ui, drawerOpen: false },
            };
        case "DEVICES_LOADING":
            return { ...state, devices: { ...state.devices, status: "loading", error: null } };
        case "DEVICES_LOADED":
            return {
                ...state,
                devices: { status: "ready", items: action.devices, error: null },
            };
        case "DEVICES_ERROR":
            return {
                ...state,
                devices: { ...state.devices, status: "error", error: action.error },
            };
        case "DEVICE_REMOVED":
            return {
                ...state,
                devices: {
                    ...state.devices,
                    items: state.devices.items.filter((device) => device.id !== action.deviceId),
                },
            };
        case "RUN_LOADING":
            return withRun(state, action.runId, action.sessionId, (run) => ({
                ...run,
                sessionId: action.sessionId ?? run.sessionId,
                error: null,
            }));
        case "RUN_VIEW":
            return withRun(state, action.runId, action.sessionId, (run) => {
                const viewCursor = Number(action.view?.last_run_seq ?? 0);
                if (viewCursor < run.cursor) return run;
                const status = statusFromView(action.view);
                return {
                    ...run,
                    sessionId: action.sessionId ?? action.view?.execution?.session_id ?? run.sessionId,
                    view: action.view,
                    status,
                    serverCursor: Math.max(run.serverCursor, action.view?.last_run_seq ?? 0),
                    pending: action.view?.pending_requests ?? run.pending,
                    delivery: action.view?.delivery ?? run.delivery,
                    partialDelivery: action.view?.partial_delivery ?? run.partialDelivery,
                    startedAt: !isTerminalStatus(status) && status !== "accepted"
                        ? (run.startedAt ?? action.now ?? Date.now())
                        : run.startedAt,
                    completedAt: terminalTime(status, run.completedAt, action.now ?? Date.now()),
                    error: status === "unknown" ? action.view?.state?.reason ?? run.error : null,
                };
            });
        case "RUN_OPTIMISTIC_START":
            return withRun(state, action.runId, action.sessionId, (run) => ({
                ...run,
                sessionId: action.sessionId,
                status: "running",
                startedAt: action.now ?? Date.now(),
                messages: appendIfNewText(run.messages, {
                    id: `optimistic-${action.runId}`,
                    role: "user",
                    text: action.input,
                    optimistic: true,
                }),
            }));
        case "RUN_OPTIMISTIC_MESSAGE":
            return withRun(state, action.runId, action.sessionId, (run) => ({
                ...run,
                messages: [...run.messages, {
                    id: action.id,
                    role: action.role ?? "user",
                    text: action.text,
                    optimistic: false,
                    steering: Boolean(action.steering),
                }],
            }));
        case "RUN_DURABLE":
            return withRun(state, action.runId, action.sessionId, (run) => (
                projectDurableRecord(run, action.record, action.now)
            ));
        case "RUN_TELEMETRY":
            return withRun(state, action.runId, action.sessionId, (run) => (
                projectTelemetry(run, action.telemetry)
            ));
        case "RUN_ERROR":
            return withRun(state, action.runId, action.sessionId, (run) => ({
                ...run,
                status: run.status === "loading" ? "unknown" : run.status,
                error: action.error,
            }));
        case "COMPOSER_BUSY":
            return { ...state, ui: { ...state.ui, composerBusy: action.busy } };
        case "UI_PATCH":
            return { ...state, ui: { ...state.ui, ...action.patch } };
        case "NOTICE":
            return {
                ...state,
                ui: {
                    ...state.ui,
                    notice: action.message
                        ? { message: action.message, tone: action.tone ?? "info", id: action.id ?? Date.now() }
                        : null,
                },
            };
        default:
            return state;
    }
}

export function selectedSession(state) {
    return state.sessions.items.find((item) => item.id === state.sessions.selectedId) ?? null;
}

export function runsForSelectedSession(state) {
    const session = selectedSession(state);
    if (!session) {
        return [];
    }
    return session.run_ids
        .map((runId) => state.runs.byId[runId])
        .filter(Boolean);
}

export function currentRun(state) {
    const runs = runsForSelectedSession(state);
    for (let index = runs.length - 1; index >= 0; index -= 1) {
        if (!isTerminalStatus(runs[index].status)) {
            return runs[index];
        }
    }
    return runs.at(-1) ?? null;
}

export function activeRun(state) {
    const run = currentRun(state);
    return run && CONTROLLABLE.has(run.status) ? run : null;
}
