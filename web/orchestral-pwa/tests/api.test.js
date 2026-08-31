import test from "node:test";
import assert from "node:assert/strict";

import { createApiClient, newUuid } from "../modules/api.js";

test("UUID generation works when randomUUID is unavailable on an HTTP origin", () => {
    const cryptoWithoutRandomUuid = {
        getRandomValues(bytes) {
            for (let index = 0; index < bytes.length; index += 1) bytes[index] = index;
            return bytes;
        },
    };

    assert.equal(
        newUuid(cryptoWithoutRandomUuid),
        "00010203-0405-4607-8809-0a0b0c0d0e0f",
    );
});

test("commands carry a UUID identity, bearer token and no-store request policy", async () => {
    const calls = [];
    const client = createApiClient({
        getToken: () => "orch_device_test.secret",
        fetchImpl: async (url, init) => {
            calls.push({ url, init });
            const request = JSON.parse(init.body);
            return new Response(JSON.stringify({
                command_id: request.command_id,
                run_id: "run / one",
                duplicate: false,
                state: { state: "accepted", recorded_seq: 3 },
            }), { status: 200, headers: { "Content-Type": "application/json" } });
        },
    });

    const ack = await client.steer("run / one", "keep going");
    assert.equal(calls.length, 1);
    assert.equal(calls[0].url, "/api/v1/runs/run%20%2F%20one/steer");
    assert.equal(calls[0].init.cache, "no-store");
    assert.equal(calls[0].init.headers.get("Authorization"), "Bearer orch_device_test.secret");
    const body = JSON.parse(calls[0].init.body);
    assert.match(body.command_id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
    assert.equal(body.text, "keep going");
    assert.equal(ack.command_id, body.command_id);
});

test("an ambiguous command transport failure retries with the same command identity", async () => {
    const commandIds = [];
    const client = createApiClient({
        getToken: () => "stream-token",
        fetchImpl: async (_url, init) => {
            const body = JSON.parse(init.body);
            commandIds.push(body.command_id);
            if (commandIds.length === 1) throw new TypeError("connection reset after write");
            return new Response(JSON.stringify({
                command_id: body.command_id,
                run_id: "run-1",
                duplicate: true,
                state: { state: "accepted", recorded_seq: 4 },
            }), { status: 200, headers: { "Content-Type": "application/json" } });
        },
    });

    const ack = await client.cancel("run-1", "stop");
    assert.equal(commandIds.length, 2);
    assert.equal(commandIds[0], commandIds[1]);
    assert.equal(ack.duplicate, true);
});

test("pairing claim never adds the authorization header", async () => {
    let authorization = "not-called";
    const client = createApiClient({
        getToken: () => "must-not-be-read",
        fetchImpl: async (_url, init) => {
            authorization = init.headers.get("Authorization");
            return new Response(JSON.stringify({ token: "new-token", device: { id: "device-1" } }), {
                status: 200,
                headers: { "Content-Type": "application/json" },
            });
        },
    });
    await client.claimPairing("once", "Phone");
    assert.equal(authorization, null);
});

test("fetch streaming SSE sends auth and resumes from the durable cursor", async () => {
    let captured;
    const source = new ReadableStream({
        start(controller) {
            controller.enqueue(new TextEncoder().encode('event: telemetry\ndata: {"telemetry_id":"t1"}\n\n'));
            controller.close();
        },
    });
    const client = createApiClient({
        getToken: () => "stream-token",
        fetchImpl: async (url, init) => {
            captured = { url, init };
            return new Response(source, {
                status: 200,
                headers: { "Content-Type": "text/event-stream" },
            });
        },
    });
    const events = [];
    let opened = false;
    await client.openRunStream("run-1", 17, {
        onOpen: () => { opened = true; },
        onEvent: (event) => events.push(event),
    });

    assert.equal(captured.url, "/api/v1/runs/run-1/stream?after=17");
    assert.equal(captured.init.headers.get("Authorization"), "Bearer stream-token");
    assert.equal(captured.init.headers.get("Last-Event-ID"), "17");
    assert.equal(captured.init.cache, "no-store");
    assert.equal(opened, true);
    assert.equal(events[0].type, "telemetry");
});
