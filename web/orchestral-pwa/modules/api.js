import { consumeSseStream } from "./sse.js";

export const API_BASE = "/api/v1";

export class ApiError extends Error {
    constructor(message, { status = 0, code = "request_failed", details = null } = {}) {
        super(message);
        this.name = "ApiError";
        this.status = status;
        this.code = code;
        this.details = details;
    }
}

async function errorFromResponse(response) {
    let body = null;
    try {
        body = await response.json();
    } catch {
        // Some reverse proxies return plain text or an empty body.
    }
    return new ApiError(
        body?.message || `Request failed (${response.status})`,
        {
            status: response.status,
            code: body?.code || `http_${response.status}`,
            details: body?.details ?? null,
        },
    );
}

function encode(value) {
    return encodeURIComponent(String(value));
}

function newCommandId() {
    if (typeof globalThis.crypto?.randomUUID === "function") {
        return globalThis.crypto.randomUUID();
    }
    if (typeof globalThis.crypto?.getRandomValues === "function") {
        const bytes = globalThis.crypto.getRandomValues(new Uint8Array(16));
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        const hex = [...bytes].map((value) => value.toString(16).padStart(2, "0"));
        return [
            hex.slice(0, 4).join(""),
            hex.slice(4, 6).join(""),
            hex.slice(6, 8).join(""),
            hex.slice(8, 10).join(""),
            hex.slice(10).join(""),
        ].join("-");
    }
    throw new Error("Web Crypto is required for stable command identity");
}

function retryableTransportFailure(error) {
    return error instanceof ApiError
        && (error.code === "network_error" || [502, 503, 504].includes(error.status));
}

export function createApiClient({
    base = API_BASE,
    getToken,
    fetchImpl = globalThis.fetch?.bind(globalThis),
    onUnauthorized = () => {},
} = {}) {
    if (typeof fetchImpl !== "function") {
        throw new TypeError("A fetch implementation is required");
    }
    if (typeof getToken !== "function") {
        throw new TypeError("getToken must be a function");
    }

    async function request(path, {
        method = "GET",
        body,
        signal,
        authenticated = true,
    } = {}) {
        const headers = new Headers({ Accept: "application/json" });
        if (authenticated) {
            const token = await getToken();
            if (!token) {
                throw new ApiError("This device is not paired", {
                    status: 401,
                    code: "authentication_required",
                });
            }
            headers.set("Authorization", `Bearer ${token}`);
        }
        if (body !== undefined) {
            headers.set("Content-Type", "application/json");
        }

        let response;
        try {
            response = await fetchImpl(`${base}${path}`, {
                method,
                headers,
                body: body === undefined ? undefined : JSON.stringify(body),
                signal,
                cache: "no-store",
                credentials: "same-origin",
                referrerPolicy: "no-referrer",
            });
        } catch (error) {
            if (error?.name === "AbortError") {
                throw error;
            }
            throw new ApiError(error?.message || "Unable to reach the Orchestral host", {
                code: "network_error",
            });
        }

        if (!response.ok) {
            const error = await errorFromResponse(response);
            if (response.status === 401) {
                onUnauthorized(error);
            }
            throw error;
        }
        if (response.status === 204) {
            return null;
        }
        return response.json();
    }

    async function openRunStream(runId, after, { signal, onEvent, onOpen } = {}) {
        const token = await getToken();
        if (!token) {
            throw new ApiError("This device is not paired", {
                status: 401,
                code: "authentication_required",
            });
        }

        const cursor = Math.max(0, Number(after) || 0);
        let response;
        try {
            const headers = new Headers({
                Accept: "text/event-stream",
                Authorization: `Bearer ${token}`,
                "Cache-Control": "no-cache",
            });
            if (cursor > 0) {
                headers.set("Last-Event-ID", String(cursor));
            }
            response = await fetchImpl(
                `${base}/runs/${encode(runId)}/stream?after=${cursor}`,
                {
                    method: "GET",
                    headers,
                    signal,
                    cache: "no-store",
                    credentials: "same-origin",
                    referrerPolicy: "no-referrer",
                },
            );
        } catch (error) {
            if (error?.name === "AbortError") {
                throw error;
            }
            throw new ApiError(error?.message || "The live connection was interrupted", {
                code: "network_error",
            });
        }

        if (!response.ok) {
            const error = await errorFromResponse(response);
            if (response.status === 401) {
                onUnauthorized(error);
            }
            throw error;
        }
        if (!response.body) {
            throw new ApiError("Streaming is not supported by this browser or proxy", {
                code: "stream_unavailable",
            });
        }

        onOpen?.(response);
        await consumeSseStream(response.body, { onEvent, signal });
    }

    async function retryIdempotent(operation, signal) {
        try {
            return await operation();
        } catch (error) {
            if (signal?.aborted || !retryableTransportFailure(error)) throw error;
            return operation();
        }
    }

    const command = (path, payload, signal) => {
        // Allocate exactly once so an ambiguous transport failure can be
        // retried without applying the command twice on the Host.
        const body = { command_id: newCommandId(), ...payload };
        return retryIdempotent(
            () => request(path, { method: "POST", body, signal }),
            signal,
        );
    };

    return Object.freeze({
        claimPairing: (secret, deviceName, signal) => request("/pairing/claim", {
            method: "POST",
            body: { secret, device_name: deviceName },
            signal,
            authenticated: false,
        }),
        me: (signal) => request("/me", { signal }),
        listDevices: (signal) => request("/devices", { signal }),
        revokeDevice: (deviceId, signal) => request(`/devices/${encode(deviceId)}`, {
            method: "DELETE",
            signal,
        }),
        listSessions: (signal) => request("/sessions", { signal }),
        createSession: (sessionId = newCommandId(), signal) => retryIdempotent(
            () => request("/sessions", {
                method: "POST",
                body: { session_id: sessionId },
                signal,
            }),
            signal,
        ),
        getSession: (sessionId, signal) => request(`/sessions/${encode(sessionId)}`, { signal }),
        startRun: (sessionId, { runId, input }, signal) => retryIdempotent(
            () => request(
                `/sessions/${encode(sessionId)}/runs`,
                { method: "POST", body: { run_id: runId, input }, signal },
            ),
            signal,
        ),
        getRun: (runId, signal) => request(`/runs/${encode(runId)}`, { signal }),
        getEvents: (runId, after = 0, signal) => request(
            `/runs/${encode(runId)}/events?after=${Math.max(0, Number(after) || 0)}`,
            { signal },
        ),
        openRunStream,
        steer: (runId, text, signal) => command(
            `/runs/${encode(runId)}/steer`,
            { text },
            signal,
        ),
        cancel: (runId, reason, signal) => command(
            `/runs/${encode(runId)}/cancel`,
            { reason },
            signal,
        ),
        resolveInput: (runId, requestId, text, signal) => command(
            `/runs/${encode(runId)}/requests/${encode(requestId)}/input`,
            { text },
            signal,
        ),
        resolveApproval: (runId, requestId, decision, signal) => command(
            `/runs/${encode(runId)}/requests/${encode(requestId)}/approval`,
            { decision },
            signal,
        ),
    });
}
