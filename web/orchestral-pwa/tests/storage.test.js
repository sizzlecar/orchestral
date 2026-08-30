import test from "node:test";
import assert from "node:assert/strict";

import { createTokenStore, storageKeys } from "../modules/storage.js";

function memoryStorage() {
    const values = new Map();
    return {
        getItem: (key) => values.get(key) ?? null,
        setItem: (key, value) => values.set(key, String(value)),
        removeItem: (key) => values.delete(key),
        values,
    };
}

test("token storage falls back cleanly when IndexedDB is unavailable", async () => {
    const local = memoryStorage();
    const store = createTokenStore({ indexedDb: undefined, local });
    assert.equal(await store.get(), null);
    assert.equal(await store.set("orch_device_one.secret"), "localstorage");
    assert.equal(await store.get(), "orch_device_one.secret");
    assert.equal(local.values.has(storageKeys.preferences), false);
    await store.clear();
    assert.equal(await store.get(), null);
});

test("empty bearer tokens are rejected before touching persistence", async () => {
    const local = memoryStorage();
    const store = createTokenStore({ indexedDb: undefined, local });
    await assert.rejects(() => store.set("  "), /non-empty device token/);
    assert.equal(local.values.has(storageKeys.tokenFallback), false);
});
