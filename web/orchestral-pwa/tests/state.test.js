import test from "node:test";
import assert from "node:assert/strict";

import {
    activeRun,
    contentText,
    createInitialState,
    currentRun,
    reducer,
    timelineForRun,
} from "../modules/state.js";

const inline = (value) => ({
    media_type: "text/plain",
    schema_id: null,
    body: { kind: "inline", value },
});

const record = (runSeq, type, fields = {}) => ({
    authority: { authority: "provider" },
    event: {
        event_id: `event-${runSeq}`,
        run_id: "run-1",
        run_seq: runSeq,
        payload: { type, ...fields },
    },
});

function withRun() {
    let state = createInitialState();
    state = reducer(state, {
        type: "SESSIONS_LOADED",
        sessions: [{
            id: "session-1",
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
            run_ids: ["run-1"],
        }],
    });
    return reducer(state, { type: "RUN_LOADING", runId: "run-1", sessionId: "session-1" });
}

test("contentText presents inline strings and JSON without leaking object coercion", () => {
    assert.equal(contentText(inline("hello")), "hello");
    assert.equal(contentText(inline({ answer: 42 })), '{\n  "answer": 42\n}');
    assert.equal(contentText({ body: { kind: "artifact", value: { artifact_ref: "blob:one" } } }), "[Artifact: blob:one]");
});

test("durable events advance only a contiguous run_seq and remain idempotent", () => {
    let state = withRun();
    const accepted = record(1, "run_accepted", { session_id: "session-1" });
    state = reducer(state, { type: "RUN_DURABLE", runId: "run-1", record: accepted });
    state = reducer(state, { type: "RUN_DURABLE", runId: "run-1", record: accepted });
    state = reducer(state, {
        type: "RUN_DURABLE",
        runId: "run-1",
        record: record(2, "input_committed", { content: [inline("Do the work")] }),
    });

    const run = state.runs.byId["run-1"];
    assert.equal(run.cursor, 2);
    assert.equal(run.eventIds.length, 2);
    assert.deepEqual(run.messages.map(({ role, text }) => ({ role, text })), [
        { role: "user", text: "Do the work" },
    ]);
});

test("a journal gap is surfaced without skipping the durable reconnect cursor", () => {
    let state = withRun();
    state = reducer(state, {
        type: "RUN_DURABLE",
        runId: "run-1",
        record: record(2, "run_started"),
    });
    const run = state.runs.byId["run-1"];
    assert.equal(run.cursor, 0);
    assert.deepEqual(run.gap, { expected: 1, received: 2, conflict: false });
});

test("telemetry deduplicates deltas but never advances the durable cursor", () => {
    let state = withRun();
    state = reducer(state, {
        type: "RUN_TELEMETRY",
        runId: "run-1",
        telemetry: {
            telemetry_id: "delta-1",
            run_id: "run-1",
            payload: { type: "output_delta", output_id: "answer", delta: inline("Hel") },
        },
    });
    state = reducer(state, {
        type: "RUN_TELEMETRY",
        runId: "run-1",
        telemetry: {
            telemetry_id: "delta-1",
            run_id: "run-1",
            payload: { type: "output_delta", output_id: "answer", delta: inline("Hel") },
        },
    });
    state = reducer(state, {
        type: "RUN_TELEMETRY",
        runId: "run-1",
        telemetry: {
            telemetry_id: "delta-2",
            run_id: "run-1",
            payload: { type: "output_delta", output_id: "answer", delta: inline("lo") },
        },
    });

    const run = state.runs.byId["run-1"];
    assert.equal(run.cursor, 0);
    assert.equal(run.streamedOutputs.answer.text, "Hello");
});

test("the presentation timeline preserves live tool order through terminal updates", () => {
    let state = withRun();
    for (const item of [
        record(1, "run_accepted"),
        record(2, "input_committed", { content: [inline("Inspect the repository")] }),
    ]) {
        state = reducer(state, { type: "RUN_DURABLE", runId: "run-1", record: item });
    }
    state = reducer(state, {
        type: "RUN_TELEMETRY",
        runId: "run-1",
        telemetry: {
            telemetry_id: "tool-running",
            run_id: "run-1",
            payload: {
                type: "tool_activity",
                activity_id: "tool-1",
                tool_name: "exec_command",
                state: "running",
                evidence: [],
            },
        },
    });
    const originalToolOrder = state.runs.byId["run-1"].activities[0].order;
    state = reducer(state, {
        type: "RUN_TELEMETRY",
        runId: "run-1",
        telemetry: {
            telemetry_id: "tool-succeeded",
            run_id: "run-1",
            payload: {
                type: "tool_activity",
                activity_id: "tool-1",
                tool_name: "exec_command",
                state: "succeeded",
                evidence: [{ type: "command", command: "git log -n 5" }],
            },
        },
    });
    state = reducer(state, {
        type: "RUN_TELEMETRY",
        runId: "run-1",
        telemetry: {
            telemetry_id: "answer-delta",
            run_id: "run-1",
            payload: { type: "output_delta", output_id: "answer", delta: inline("Finished") },
        },
    });
    state = reducer(state, {
        type: "RUN_DURABLE",
        runId: "run-1",
        record: record(3, "output_committed", {
            output_id: "answer",
            content: [inline("Finished")],
        }),
    });

    const run = state.runs.byId["run-1"];
    assert.equal(run.activities[0].order, originalToolOrder);
    assert.deepEqual(
        timelineForRun(run).map(({ kind, value }) => [
            kind,
            value.text ?? value.toolName,
        ]),
        [
            ["message", "Inspect the repository"],
            ["activity", "exec_command"],
            ["message", "Finished"],
        ],
    );
});

test("committed output replaces its lossy stream and terminal delivery closes the run", () => {
    let state = withRun();
    for (const item of [
        record(1, "run_accepted"),
        record(2, "run_started"),
        record(3, "request_opened", {
            request: {
                request_id: "request-1",
                blocking: true,
                payload: { type: "input", prompt: [inline("Which target?")] },
            },
        }),
    ]) {
        state = reducer(state, { type: "RUN_DURABLE", runId: "run-1", record: item });
    }
    assert.equal(activeRun(state)?.status, "waiting");

    state = reducer(state, {
        type: "RUN_DURABLE",
        runId: "run-1",
        record: record(4, "request_resolved", { request_id: "request-1" }),
    });
    state = reducer(state, {
        type: "RUN_DURABLE",
        runId: "run-1",
        record: record(5, "output_committed", {
            output_id: "answer",
            content: [inline("Finished")],
        }),
    });
    state = reducer(state, {
        type: "RUN_DURABLE",
        runId: "run-1",
        now: 1234,
        record: record(6, "delivery_committed", {
            delivery: { final_response: inline("Finished") },
        }),
    });

    assert.equal(activeRun(state), null);
    assert.equal(currentRun(state).status, "delivered");
    assert.equal(currentRun(state).completedAt, 1234);
    assert.equal(currentRun(state).messages.filter((item) => item.text === "Finished").length, 1);
});

test("inspect view records a consistency bound without moving the replay cursor", () => {
    let state = withRun();
    state = reducer(state, {
        type: "RUN_VIEW",
        runId: "run-1",
        sessionId: "session-1",
        view: {
            execution: { session_id: "session-1" },
            state: { state: "running" },
            last_run_seq: 9,
            pending_requests: [],
        },
    });
    assert.equal(state.runs.byId["run-1"].cursor, 0);
    assert.equal(state.runs.byId["run-1"].serverCursor, 9);
});

test("an inspect view older than the durable cursor cannot reopen resolved UI state", () => {
    let state = withRun();
    for (const item of [
        record(1, "run_accepted"),
        record(2, "run_started"),
        record(3, "request_opened", {
            request: {
                request_id: "request-1",
                blocking: true,
                payload: { type: "input", prompt: [inline("Question")] },
            },
        }),
        record(4, "request_resolved", { request_id: "request-1" }),
    ]) {
        state = reducer(state, { type: "RUN_DURABLE", runId: "run-1", record: item });
    }
    state = reducer(state, {
        type: "RUN_VIEW",
        runId: "run-1",
        view: {
            state: { state: "waiting", pending_request_ids: ["request-1"] },
            last_run_seq: 3,
            pending_requests: [{
                request_id: "request-1",
                blocking: true,
                payload: { type: "input", prompt: [inline("Question")] },
            }],
        },
    });
    assert.equal(state.runs.byId["run-1"].status, "running");
    assert.deepEqual(state.runs.byId["run-1"].pending, []);
    assert.equal(state.runs.byId["run-1"].cursor, 4);
});
