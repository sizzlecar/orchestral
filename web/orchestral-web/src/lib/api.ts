export type RuntimeEvent = {
  type: string;
  timestamp?: string;
  thread_id?: string;
  interaction_id?: string;
  payload?: Record<string, unknown>;
  [key: string]: unknown;
};

export type SubmitMessageRequest = {
  request_id?: string;
  input: string;
};

export type SubmitMessageResponse = {
  status: string;
  interaction_id?: string;
  task_id?: string;
  message?: string | null;
};

export type HealthResponse = {
  status: string;
};

const API_BASE = (import.meta.env.VITE_API_BASE as string | undefined)?.replace(/\/$/, "") ?? "";

function apiUrl(path: string): string {
  return `${API_BASE}${path}`;
}

export async function fetchHealth(): Promise<HealthResponse> {
  const resp = await fetch(apiUrl("/health"));
  if (!resp.ok) {
    throw new Error(`Health check failed: ${resp.status}`);
  }
  return (await resp.json()) as HealthResponse;
}

export async function postMessage(
  session: string,
  payload: SubmitMessageRequest
): Promise<SubmitMessageResponse> {
  const resp = await fetch(apiUrl(`/threads/${encodeURIComponent(session)}/messages`), {
    method: "POST",
    headers: {
      "content-type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Submit failed (${resp.status}): ${text}`);
  }

  return (await resp.json()) as SubmitMessageResponse;
}

export function openEventStream(
  session: string,
  onEvent: (event: RuntimeEvent) => void,
  onError: (error: Event) => void
): EventSource {
  const es = new EventSource(apiUrl(`/threads/${encodeURIComponent(session)}/events`));

  es.addEventListener("runtime_event", (evt: MessageEvent<string>) => {
    try {
      onEvent(JSON.parse(evt.data) as RuntimeEvent);
    } catch {
      // Ignore malformed SSE payloads from transient server errors.
    }
  });

  es.onerror = onError;
  return es;
}

export function eventText(event: RuntimeEvent): string {
  const payload = event.payload;
  if (!payload || typeof payload !== "object") {
    return "";
  }

  const candidates = ["message", "content", "text", "input", "summary"] as const;
  for (const key of candidates) {
    const v = payload[key];
    if (typeof v === "string" && v.trim()) {
      return v;
    }
  }

  return "";
}
