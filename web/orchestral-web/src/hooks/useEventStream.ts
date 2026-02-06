import { useEffect, useRef, useState } from "react";
import { openEventStream, type RuntimeEvent } from "../lib/api";

export type StreamState = "idle" | "connecting" | "open" | "error";

export function useEventStream(session: string, onEvent: (event: RuntimeEvent) => void) {
  const onEventRef = useRef(onEvent);
  const [state, setState] = useState<StreamState>("idle");

  onEventRef.current = onEvent;

  useEffect(() => {
    if (!session.trim()) {
      setState("idle");
      return;
    }

    setState("connecting");
    const es = openEventStream(
      session,
      (event) => {
        setState("open");
        onEventRef.current(event);
      },
      () => {
        setState("error");
      }
    );

    return () => {
      es.close();
      setState("idle");
    };
  }, [session]);

  return state;
}
