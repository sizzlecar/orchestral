import { FormEvent, KeyboardEvent, useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { eventText, fetchHealth, postMessage } from "./lib/api";
import { useEventStream } from "./hooks/useEventStream";

type NoteLevel = "info" | "warn" | "error";
type RunPhase = "idle" | "planning" | "executing" | "waiting_input" | "failed";
type ActivityKind = "Ran" | "Edited" | "Explored";

type ActivityGroup = {
  key: string;
  kind: ActivityKind;
  title: string;
  items: string[];
  failed: boolean;
  active: boolean;
};

type TurnNote = {
  id: string;
  text: string;
  level: NoteLevel;
  timestamp?: string;
};

type TurnMessage = {
  id: string;
  text: string;
  timestamp?: string;
};

type Turn = {
  id: string;
  prompt: string;
  promptTimestamp?: string;
  phase: RunPhase;
  statusText: string;
  notes: TurnNote[];
  activities: ActivityGroup[];
  assistantMessages: TurnMessage[];
  assistantDraft: string;
  startedAtMs?: number;
  elapsedReported: boolean;
};

const nowId = () => `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  return value as Record<string, unknown>;
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function asNumber(value: unknown): number | undefined {
  return typeof value === "number" ? value : undefined;
}

function classifyActivityKind(action: string): ActivityKind {
  if (["file_write", "edit", "patch", "write"].includes(action)) {
    return "Edited";
  }
  if (["http", "search", "read_file", "list_files", "find", "grep"].includes(action)) {
    return "Explored";
  }
  return "Ran";
}

function previewToActivityLines(preview: string): string[] {
  const rawLines = preview.split("\n");
  const maxLines = 8;
  const out = rawLines
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .slice(0, maxLines)
    .map((line) => {
      if (line.length <= 180) {
        return line;
      }
      return `${line.slice(0, 180)}...`;
    });

  if (rawLines.length > maxLines) {
    out.push(`... +${rawLines.length - maxLines} lines`);
  }

  return out.length > 0 ? out : ["(empty)"];
}

function formatElapsed(ms: number): string {
  if (ms < 1000) {
    return `${Math.max(1, Math.round(ms))}ms`;
  }
  const secs = Math.floor(ms / 1000);
  if (secs < 60) {
    return `${secs}s`;
  }
  const mins = Math.floor(secs / 60);
  const rem = secs % 60;
  return rem === 0 ? `${mins}m` : `${mins}m ${rem}s`;
}

function normalizeMessage(text: string): string {
  return text.trim();
}

function humanizeError(error: unknown): string {
  const raw = error instanceof Error ? error.message : String(error);
  const submitMatch = raw.match(/^Submit failed \((\d+)\):\s*([\s\S]*)$/);
  if (!submitMatch) {
    return raw;
  }

  const code = submitMatch[1];
  const body = submitMatch[2].trim();
  try {
    const parsed = JSON.parse(body) as Record<string, unknown>;
    const msg = typeof parsed.message === "string" ? parsed.message : "";
    if (msg.trim()) {
      return `Submit failed (${code}): ${msg.trim()}`;
    }
  } catch {
    // Fallback to first line of raw body when backend doesn't return JSON.
  }

  const oneLine = body.split("\n")[0]?.trim() ?? "";
  if (oneLine) {
    return `Submit failed (${code}): ${oneLine}`;
  }
  return `Submit failed (${code})`;
}

function ts(input?: string): string {
  if (!input) {
    return "";
  }
  const date = new Date(input);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  return date.toLocaleTimeString();
}

function createTurn(id: string, prompt: string, timestamp?: string): Turn {
  return {
    id,
    prompt,
    promptTimestamp: timestamp,
    phase: "idle",
    statusText: "Idle",
    notes: [],
    activities: [],
    assistantMessages: [],
    assistantDraft: "",
    elapsedReported: false
  };
}

export default function App() {
  const [session, setSession] = useState("demo");
  const [input, setInput] = useState("");
  const [turns, setTurns] = useState<Turn[]>([]);
  const activeTurnIdRef = useRef<string | null>(null);
  const typingTimersRef = useRef<Record<string, ReturnType<typeof window.setInterval>>>({});
  const lastAssistantByTurnRef = useRef<Record<string, string>>({});
  const streamDraftByTurnRef = useRef<Record<string, string>>({});
  const lastStreamedByTurnRef = useRef<Record<string, string>>({});

  const health = useQuery({
    queryKey: ["health"],
    queryFn: fetchHealth,
    refetchInterval: 10_000
  });

  const sendMutation = useMutation({
    mutationFn: async (prompt: string) => {
      return postMessage(session, { input: prompt, request_id: nowId() });
    },
    onError: (error) => {
      const turnId = activeTurnIdRef.current ?? ensureActiveTurn("(submit failed)");
      updateTurn(turnId, (turn) => {
        const next = appendNote(turn, `Error: ${humanizeError(error)}`, "error");
        return {
          ...next,
          phase: "failed",
          statusText: "Failed"
        };
      });
    }
  });

  useEffect(() => {
    return () => {
      for (const timer of Object.values(typingTimersRef.current)) {
        window.clearInterval(timer);
      }
      typingTimersRef.current = {};
    };
  }, []);

  function ensureActiveTurn(promptFallback = "(continued)", timestamp?: string): string {
    const currentId = activeTurnIdRef.current;
    if (currentId) {
      return currentId;
    }

    const nextId = nowId();
    activeTurnIdRef.current = nextId;
    setTurns((prev) => [...prev, createTurn(nextId, promptFallback, timestamp)]);
    return nextId;
  }

  function updateTurn(turnId: string, updater: (turn: Turn) => Turn) {
    setTurns((prev) => prev.map((turn) => (turn.id === turnId ? updater(turn) : turn)));
  }

  function stopTyping(turnId: string) {
    const timer = typingTimersRef.current[turnId];
    if (timer) {
      window.clearInterval(timer);
      delete typingTimersRef.current[turnId];
    }
  }

  function animateAssistantOutput(turnId: string, text: string, timestamp?: string) {
    stopTyping(turnId);
    const chars = Array.from(text);
    if (chars.length === 0) {
      return;
    }

    let cursor = 0;
    const chunkSize = Math.max(1, Math.ceil(chars.length / 70));

    updateTurn(turnId, (turn) => ({ ...turn, assistantDraft: "" }));

    const timer = window.setInterval(() => {
      cursor = Math.min(chars.length, cursor + chunkSize);
      const draft = chars.slice(0, cursor).join("");
      updateTurn(turnId, (turn) => ({ ...turn, assistantDraft: draft }));

      if (cursor < chars.length) {
        return;
      }

      stopTyping(turnId);
      updateTurn(turnId, (turn) => {
        const normalized = normalizeMessage(text);
        const last = turn.assistantMessages[turn.assistantMessages.length - 1];
        if (last?.text.trim() === normalized || lastAssistantByTurnRef.current[turnId] === normalized) {
          return { ...turn, assistantDraft: "" };
        }
        lastAssistantByTurnRef.current[turnId] = normalized;
        streamDraftByTurnRef.current[turnId] = "";
        return {
          ...turn,
          assistantDraft: "",
          assistantMessages: [...turn.assistantMessages, { id: nowId(), text, timestamp }]
        };
      });
    }, 22);

    typingTimersRef.current[turnId] = timer;
  }

  function appendNote(turn: Turn, text: string, level: NoteLevel, timestamp?: string): Turn {
    if (!text.trim()) {
      return turn;
    }
    if (turn.notes.some((n) => n.text.trim() === text.trim())) {
      return turn;
    }

    return {
      ...turn,
      notes: [...turn.notes, { id: nowId(), text, level, timestamp }]
    };
  }

  function upsertActivityStart(turn: Turn, stepId: string, action: string, inputSummary?: string): Turn {
    const key = `${stepId}::${action}`;
    const existingIdx = turn.activities.findIndex((g) => g.key === key);
    const initialItems = inputSummary?.trim() ? [`input: ${inputSummary.trim()}`] : [];

    let activities: ActivityGroup[];
    if (existingIdx >= 0) {
      activities = turn.activities.map((group, idx) =>
        idx === existingIdx
          ? {
              ...group,
              active: true,
              failed: false,
              items: group.items.length === 0 ? initialItems : group.items
            }
          : group
      );
    } else {
      activities = [
        ...turn.activities,
        {
          key,
          kind: classifyActivityKind(action),
          title: action,
          items: initialItems,
          failed: false,
          active: true
        }
      ];
    }

    return {
      ...turn,
      phase: "executing",
      statusText: `Running ${stepId} (${action})`,
      activities
    };
  }

  function appendActivityItem(turn: Turn, stepId: string, action: string, line: string): Turn {
    const key = `${stepId}::${action}`;
    const idx = turn.activities.findIndex((g) => g.key === key);
    if (idx < 0) {
      return turn;
    }

    const group = turn.activities[idx];
    if (group.items.includes(line)) {
      return turn;
    }

    const next = [...turn.activities];
    next[idx] = {
      ...group,
      items: [...group.items, line]
    };
    return { ...turn, activities: next };
  }

  function endActivity(turn: Turn, stepId: string, action: string, failed: boolean): Turn {
    const key = `${stepId}::${action}`;
    return {
      ...turn,
      activities: turn.activities.map((group) =>
        group.key === key
          ? {
              ...group,
              active: false,
              failed: group.failed || failed
            }
          : group
      )
    };
  }

  function markTurnElapsed(turn: Turn): Turn {
    if (turn.elapsedReported || !turn.startedAtMs) {
      return turn;
    }
    const worked = formatElapsed(Date.now() - turn.startedAtMs);
    return {
      ...appendNote(turn, `Worked for ${worked}`, "info"),
      elapsedReported: true
    };
  }

  const streamState = useEventStream(session, (event) => {
    if (event.type === "user_input") {
      const text = eventText(event);
      if (!text) {
        return;
      }

      const turnId = activeTurnIdRef.current;
      if (!turnId) {
        const nextId = nowId();
        activeTurnIdRef.current = nextId;
        setTurns((prev) => [...prev, createTurn(nextId, text, event.timestamp)]);
        return;
      }

      updateTurn(turnId, (turn) => {
        if (turn.prompt.trim() === text.trim()) {
          return { ...turn, promptTimestamp: turn.promptTimestamp ?? event.timestamp };
        }
        return turn;
      });
      return;
    }

    if (event.type === "assistant_output") {
      const text = eventText(event);
      if (!text) {
        return;
      }

      const turnId = ensureActiveTurn("(assistant message)", event.timestamp);
      const normalized = normalizeMessage(text);
      if (!normalized) {
        return;
      }
      const streamDraft = normalizeMessage(streamDraftByTurnRef.current[turnId] ?? "");
      const lastStreamed = normalizeMessage(lastStreamedByTurnRef.current[turnId] ?? "");
      if ((streamDraft && streamDraft === normalized) || (lastStreamed && lastStreamed === normalized)) {
        updateTurn(turnId, (turn) => ({ ...turn, assistantDraft: "" }));
        return;
      }
      if (lastAssistantByTurnRef.current[turnId] === normalized) {
        updateTurn(turnId, (turn) => ({ ...turn, assistantDraft: "" }));
        return;
      }
      animateAssistantOutput(turnId, text, event.timestamp);
      return;
    }

    if (event.type !== "system_trace") {
      return;
    }

    const payload = asRecord(event.payload);
    if (!payload) {
      return;
    }
    const category = asString(payload.category);

    if (category === "assistant_stream") {
      const delta = asString(payload.delta) ?? "";
      const done = Boolean(payload.done);
      const turnId = ensureActiveTurn();
      stopTyping(turnId);
      updateTurn(turnId, (turn) => {
        const draft = `${turn.assistantDraft}${delta}`;
        streamDraftByTurnRef.current[turnId] = draft;
        if (!done) {
          return { ...turn, assistantDraft: draft };
        }

        const finalText = draft.trim();
        streamDraftByTurnRef.current[turnId] = "";
        if (!finalText) {
          return { ...turn, assistantDraft: "" };
        }

        const last = turn.assistantMessages[turn.assistantMessages.length - 1];
        if (last?.text.trim() === finalText) {
          lastStreamedByTurnRef.current[turnId] = finalText;
          return { ...turn, assistantDraft: "" };
        }

        lastAssistantByTurnRef.current[turnId] = finalText;
        lastStreamedByTurnRef.current[turnId] = finalText;
        return {
          ...turn,
          assistantDraft: "",
          assistantMessages: [...turn.assistantMessages, { id: nowId(), text: finalText, timestamp: event.timestamp }]
        };
      });
      return;
    }

    if (category === "runtime_lifecycle") {
      const eventType = asString(payload.event_type);
      const message = asString(payload.message);
      const metadata = asRecord(payload.metadata);
      const turnId = ensureActiveTurn();

      if (eventType === "planning_started") {
        updateTurn(turnId, (turn) => ({
          ...turn,
          phase: "planning",
          statusText: "Planning...",
          startedAtMs: turn.startedAtMs ?? Date.now()
        }));
        return;
      }

      if (eventType === "planning_completed") {
        const stepCount = asNumber(metadata?.step_count);
        if (typeof stepCount === "number") {
          updateTurn(turnId, (turn) => appendNote(turn, `${stepCount} step(s) queued`, "info", event.timestamp));
        }
        return;
      }

      if (eventType === "execution_started") {
        updateTurn(turnId, (turn) => ({ ...turn, phase: "executing", statusText: "Executing..." }));
        return;
      }

      if (eventType === "execution_completed") {
        const status = asString(metadata?.status);
        if (status === "waiting_user" || status === "waiting_event") {
          const prompt = asString(metadata?.prompt) ?? asString(metadata?.event_type) ?? "input required";
          updateTurn(turnId, (turn) => {
            const waited = appendNote(turn, `Waiting: ${prompt}`, "warn", event.timestamp);
            return {
              ...waited,
              phase: "waiting_input",
              statusText: "Waiting input"
            };
          });
          return;
        }

        if (status === "failed") {
          updateTurn(turnId, (turn) => {
            const next = appendNote(turn, "Execution failed", "error", event.timestamp);
            return {
              ...next,
              phase: "failed",
              statusText: "Failed"
            };
          });
          return;
        }

        updateTurn(turnId, (turn) => {
          const elapsed = markTurnElapsed(turn);
          return {
            ...elapsed,
            phase: "waiting_input",
            statusText: "Idle"
          };
        });
        return;
      }

      if (eventType === "replanning_started") {
        updateTurn(turnId, (turn) =>
          appendNote(turn, message ?? "Execution failed, retrying with recovery plan...", "warn", event.timestamp)
        );
        return;
      }

      if (eventType === "replanning_completed") {
        updateTurn(turnId, (turn) =>
          appendNote(turn, message ?? "Recovery plan ready, resuming execution.", "info", event.timestamp)
        );
        return;
      }

      if (eventType === "replanning_failed") {
        updateTurn(turnId, (turn) =>
          appendNote(turn, message ?? "Recovery planning failed", "error", event.timestamp)
        );
        return;
      }

      if (eventType === "turn_rejected") {
        updateTurn(turnId, (turn) => {
          const next = appendNote(turn, `Waiting: ${message ?? "turn rejected"}`, "warn", event.timestamp);
          return {
            ...next,
            phase: "waiting_input",
            statusText: "Waiting input"
          };
        });
      }
      return;
    }

    if (category === "execution_progress") {
      const phaseName = asString(payload.phase);
      const stepId = asString(payload.step_id);
      const action = asString(payload.action) ?? "-";
      const message = asString(payload.message);
      const metadata = asRecord(payload.metadata);
      const turnId = ensureActiveTurn();

      if (phaseName === "step_started" && stepId) {
        const inputSummary = asString(metadata?.input_summary);
        updateTurn(turnId, (turn) => upsertActivityStart(turn, stepId, action, inputSummary));
        return;
      }

      if (phaseName === "step_completed" && stepId) {
        updateTurn(turnId, (turn) => {
          let next = appendActivityItem(turn, stepId, action, "completed");
          const status = asNumber(metadata?.status);
          const bytes = asNumber(metadata?.bytes);
          const path = asString(metadata?.path);
          const preview = asString(metadata?.output_preview);

          if (typeof status === "number") {
            next = appendActivityItem(next, stepId, action, `status=${status}`);
          }
          if (typeof bytes === "number") {
            next = appendActivityItem(next, stepId, action, `bytes=${bytes}`);
          }
          if (path) {
            next = appendActivityItem(next, stepId, action, `file: ${path}`);
          }
          if (preview) {
            for (const line of previewToActivityLines(preview)) {
              next = appendActivityItem(next, stepId, action, line);
            }
          }
          return endActivity(next, stepId, action, false);
        });
        return;
      }

      if (phaseName === "step_failed" && stepId) {
        updateTurn(turnId, (turn) => {
          let next = appendActivityItem(turn, stepId, action, `failed ${message ?? "unknown error"}`);
          next = endActivity(next, stepId, action, true);
          return {
            ...next,
            phase: "failed",
            statusText: "Failed"
          };
        });
        return;
      }

      if ((phaseName === "step_waiting_user" || phaseName === "step_waiting_event") && message) {
        updateTurn(turnId, (turn) => {
          const next = appendNote(turn, `Waiting: ${message}`, "warn", event.timestamp);
          return {
            ...next,
            phase: "waiting_input",
            statusText: "Waiting input"
          };
        });
        return;
      }

      if (phaseName === "task_completed") {
        updateTurn(turnId, (turn) => {
          const elapsed = markTurnElapsed(turn);
          return {
            ...elapsed,
            phase: "waiting_input",
            statusText: "Idle"
          };
        });
        return;
      }

      if (phaseName === "task_failed") {
        updateTurn(turnId, (turn) => {
          const next = appendNote(turn, `Task failed: ${message ?? "unknown error"}`, "error", event.timestamp);
          return {
            ...next,
            phase: "failed",
            statusText: "Failed"
          };
        });
      }
    }
  });

  const canSubmit = input.trim().length > 0 && !sendMutation.isPending;

  async function onSubmit(evt: FormEvent) {
    evt.preventDefault();
    const prompt = input.trim();
    if (!prompt) {
      return;
    }

    const turnId = nowId();
    activeTurnIdRef.current = turnId;
    streamDraftByTurnRef.current[turnId] = "";
    lastStreamedByTurnRef.current[turnId] = "";
    lastAssistantByTurnRef.current[turnId] = "";
    setTurns((prev) => [...prev, createTurn(turnId, prompt)]);
    setInput("");
    await sendMutation.mutateAsync(prompt);
  }

  function onComposerKeyDown(evt: KeyboardEvent<HTMLTextAreaElement>) {
    if (evt.key !== "Enter" || evt.shiftKey) {
      return;
    }
    evt.preventDefault();
    if (canSubmit) {
      void onSubmit(evt as unknown as FormEvent);
    }
  }

  const streamLabel = useMemo(() => {
    if (streamState === "open") {
      return "SSE connected";
    }
    if (streamState === "connecting") {
      return "SSE connecting";
    }
    if (streamState === "error") {
      return "SSE error";
    }
    return "SSE idle";
  }, [streamState]);

  return (
    <div className="page">
      <header className="topbar">
        <h1>Orchestral Web</h1>
        <div className="badges">
          <span className={`badge ${health.data?.status === "ok" ? "ok" : "warn"}`}>
            API {health.data?.status ?? "down"}
          </span>
          <span className={`badge ${streamState === "open" ? "ok" : "warn"}`}>{streamLabel}</span>
        </div>
      </header>

      <section className="session-bar">
        <label htmlFor="session">Thread ID</label>
        <input
          id="session"
          value={session}
          onChange={(e) => setSession(e.target.value)}
          placeholder="demo"
        />
      </section>

      <section className="panel terminal">
        <div className="timeline">
          {turns.length === 0 ? (
            <p className="empty">Ready. Enter prompt to start.</p>
          ) : (
            turns.map((turn) => {
              const kindOrder: ActivityKind[] = [];
              for (const group of turn.activities) {
                if (!kindOrder.includes(group.kind)) {
                  kindOrder.push(group.kind);
                }
              }

              return (
                <article key={turn.id} className="turn">
                  <div className="line prompt">
                    <span className="prefix">›</span>
                    <span>{turn.prompt}</span>
                    {turn.promptTimestamp ? <span className="line-time">{ts(turn.promptTimestamp)}</span> : null}
                  </div>

                  {(turn.phase === "planning" || turn.phase === "executing") && turn.statusText ? (
                    <div className={`line status ${turn.phase === "planning" || turn.phase === "executing" ? "working" : ""}`}>
                      <span className="prefix">•</span>
                      <span className="spinner" aria-hidden />
                      <span className="shimmer">{turn.statusText}</span>
                    </div>
                  ) : null}

                  {kindOrder.map((kind) => {
                    const groups = turn.activities.filter((group) => group.kind === kind);
                    const isActive = groups.some((group) => group.active);

                    return (
                      <div key={`${turn.id}-${kind}`} className="activity-kind-block">
                        <div className={`line kind ${isActive ? "active shimmer" : ""}`}>
                          <span className="prefix">•</span>
                          <span>{kind}</span>
                        </div>

                        {groups.map((group) => (
                          <div key={group.key} className="activity-group">
                            <div className={`line step ${group.failed ? "failed" : ""}`}>
                              <span className="branch">└</span>
                              <span>{group.title}</span>
                            </div>
                            {group.items.map((item, idx) => (
                              <div key={`${group.key}-${idx}`} className="line item">
                                <span className="bullet">·</span>
                                <span>{item}</span>
                              </div>
                            ))}
                          </div>
                        ))}
                      </div>
                    );
                  })}

                  {turn.notes.map((note) => (
                    <div key={note.id} className={`line note ${note.level}`}>
                      <span>{note.text}</span>
                      {note.timestamp ? <span className="line-time">{ts(note.timestamp)}</span> : null}
                    </div>
                  ))}

                  {turn.assistantMessages.map((msg) => (
                    <div key={msg.id} className="line assistant">
                      <span>{msg.text}</span>
                      {msg.timestamp ? <span className="line-time">{ts(msg.timestamp)}</span> : null}
                    </div>
                  ))}

                  {turn.assistantDraft ? (
                    <div className="line assistant draft">
                      <span>{turn.assistantDraft}</span>
                      <span className="caret" aria-hidden />
                    </div>
                  ) : null}
                </article>
              );
            })
          )}
        </div>
      </section>

      <form onSubmit={onSubmit} className="composer">
        <textarea
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={onComposerKeyDown}
          placeholder="Enter submit (Shift+Enter newline)"
          rows={3}
        />
        <button disabled={!canSubmit} type="submit">
          {sendMutation.isPending ? "Sending..." : "Submit"}
        </button>
      </form>
    </div>
  );
}
