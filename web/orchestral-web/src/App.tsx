import { FormEvent, useMemo, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { eventText, fetchHealth, postMessage, type RuntimeEvent } from "./lib/api";
import { useEventStream } from "./hooks/useEventStream";

type ChatMessage = {
  id: string;
  role: "user" | "assistant";
  text: string;
  timestamp?: string;
};

const nowId = () => `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;

export default function App() {
  const [session, setSession] = useState("demo");
  const [input, setInput] = useState("");
  const [events, setEvents] = useState<RuntimeEvent[]>([]);
  const [messages, setMessages] = useState<ChatMessage[]>([]);

  const health = useQuery({
    queryKey: ["health"],
    queryFn: fetchHealth,
    refetchInterval: 10_000
  });

  const sendMutation = useMutation({
    mutationFn: async (prompt: string) => {
      return postMessage(session, { input: prompt, request_id: nowId() });
    }
  });

  const streamState = useEventStream(session, (event) => {
    setEvents((prev) => [...prev.slice(-199), event]);

    if (event.type === "user_input") {
      const text = eventText(event);
      if (text) {
        setMessages((prev) => [
          ...prev,
          { id: nowId(), role: "user", text, timestamp: event.timestamp }
        ]);
      }
      return;
    }

    if (event.type === "assistant_output") {
      const text = eventText(event);
      if (text) {
        setMessages((prev) => [
          ...prev,
          { id: nowId(), role: "assistant", text, timestamp: event.timestamp }
        ]);
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

    setInput("");
    await sendMutation.mutateAsync(prompt);
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

      <main className="layout">
        <section className="panel chat">
          <div className="panel-head">
            <label htmlFor="session">Session</label>
            <input
              id="session"
              value={session}
              onChange={(e) => setSession(e.target.value)}
              placeholder="demo"
            />
          </div>

          <div className="messages">
            {messages.length === 0 ? (
              <p className="empty">No messages yet. Send a prompt to start.</p>
            ) : (
              messages.map((m) => (
                <article key={m.id} className={`bubble ${m.role}`}>
                  <div className="bubble-role">{m.role}</div>
                  <div>{m.text}</div>
                </article>
              ))
            )}
          </div>

          <form onSubmit={onSubmit} className="composer">
            <textarea
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder="Ask Orchestral..."
              rows={3}
            />
            <button disabled={!canSubmit} type="submit">
              {sendMutation.isPending ? "Sending..." : "Send"}
            </button>
          </form>

          {sendMutation.error ? <p className="error">{String(sendMutation.error)}</p> : null}
        </section>

        <section className="panel events">
          <div className="panel-head">
            <h2>Runtime Events</h2>
            <span>{events.length}</span>
          </div>
          <div className="event-list">
            {events.length === 0 ? (
              <p className="empty">Waiting for events...</p>
            ) : (
              [...events].reverse().map((event, idx) => (
                <pre key={`${event.timestamp ?? "ts"}-${idx}`}>{JSON.stringify(event, null, 2)}</pre>
              ))
            )}
          </div>
        </section>
      </main>
    </div>
  );
}
