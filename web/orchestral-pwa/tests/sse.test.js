import test from "node:test";
import assert from "node:assert/strict";

import { createSseParser } from "../modules/sse.js";

test("SSE parser handles split CRLF, comments, ids and multiline data", () => {
    const events = [];
    const parser = createSseParser((event) => events.push(event));
    parser.push(": keep alive\r");
    parser.push("\nevent: durable\r\nid: 7\r\ndata: {\"one\":\r\n");
    parser.push("data: 1}\r\n\r");
    parser.push("\n");
    parser.finish();

    assert.deepEqual(events, [{
        type: "durable",
        id: "7",
        retry: undefined,
        data: '{"one":\n1}',
    }]);
});

test("SSE parser ignores empty events and exposes numeric retry", () => {
    const events = [];
    const parser = createSseParser((event) => events.push(event));
    parser.push("event: telemetry\nretry: 2500\ndata: {}\n\n\n");
    parser.finish();
    assert.equal(events.length, 1);
    assert.equal(events[0].type, "telemetry");
    assert.equal(events[0].retry, 2500);
});
