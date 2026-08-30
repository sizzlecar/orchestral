const DATABASE_NAME = "orchestral-pwa";
const DATABASE_VERSION = 1;
const STORE_NAME = "private-values";
const TOKEN_KEY = "device-token";
const TOKEN_FALLBACK_KEY = "orchestral.device-token.v1";
const PREFERENCES_KEY = "orchestral.preferences.v1";

function openDatabase(indexedDb) {
    return new Promise((resolve, reject) => {
        if (!indexedDb) {
            reject(new Error("IndexedDB is unavailable"));
            return;
        }

        const request = indexedDb.open(DATABASE_NAME, DATABASE_VERSION);
        request.onupgradeneeded = () => {
            const database = request.result;
            if (!database.objectStoreNames.contains(STORE_NAME)) {
                database.createObjectStore(STORE_NAME);
            }
        };
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error ?? new Error("Unable to open IndexedDB"));
        request.onblocked = () => reject(new Error("IndexedDB upgrade is blocked"));
    });
}

async function useStore(indexedDb, mode, operation) {
    const database = await openDatabase(indexedDb);
    try {
        return await new Promise((resolve, reject) => {
            const transaction = database.transaction(STORE_NAME, mode);
            const store = transaction.objectStore(STORE_NAME);
            let result;

            try {
                result = operation(store);
            } catch (error) {
                reject(error);
                return;
            }

            transaction.oncomplete = () => resolve(result?.result);
            transaction.onerror = () => reject(
                transaction.error ?? result?.error ?? new Error("IndexedDB transaction failed"),
            );
            transaction.onabort = () => reject(
                transaction.error ?? new Error("IndexedDB transaction was aborted"),
            );
        });
    } finally {
        database.close();
    }
}

function usableStorage(storage) {
    return storage && typeof storage.getItem === "function" && typeof storage.setItem === "function";
}

/**
 * Browser storage cannot make a bearer token inaccessible to same-origin JS.
 * This wrapper keeps it out of URLs, Cache Storage and ordinary app state,
 * preferring a dedicated IndexedDB store with localStorage only as a fallback.
 */
function browserIndexedDb() {
    try {
        return globalThis.indexedDB;
    } catch {
        return undefined;
    }
}

function browserLocalStorage() {
    try {
        return globalThis.localStorage;
    } catch {
        return undefined;
    }
}

export function createTokenStore(options = {}) {
    const indexedDb = Object.hasOwn(options, "indexedDb") ? options.indexedDb : browserIndexedDb();
    const local = Object.hasOwn(options, "local") ? options.local : browserLocalStorage();
    return {
        async get() {
            try {
                const value = await useStore(indexedDb, "readonly", (store) => store.get(TOKEN_KEY));
                if (typeof value === "string" && value.length > 0) {
                    return value;
                }
            } catch {
                // Private browsing and hardened browsers may disable IndexedDB.
            }

            try {
                return usableStorage(local) ? local.getItem(TOKEN_FALLBACK_KEY) : null;
            } catch {
                return null;
            }
        },

        async set(token) {
            if (typeof token !== "string" || token.trim().length === 0) {
                throw new TypeError("A non-empty device token is required");
            }

            try {
                await useStore(indexedDb, "readwrite", (store) => store.put(token, TOKEN_KEY));
                try {
                    local?.removeItem(TOKEN_FALLBACK_KEY);
                } catch {
                    // The durable IndexedDB write already succeeded.
                }
                return "indexeddb";
            } catch (indexedDbError) {
                try {
                    if (!usableStorage(local)) {
                        throw indexedDbError;
                    }
                    local.setItem(TOKEN_FALLBACK_KEY, token);
                    return "localstorage";
                } catch {
                    throw new Error("This browser could not securely retain the device token", {
                        cause: indexedDbError,
                    });
                }
            }
        },

        async clear() {
            try {
                await useStore(indexedDb, "readwrite", (store) => store.delete(TOKEN_KEY));
            } catch {
                // Clear both stores independently so one blocked backend is harmless.
            }
            try {
                local?.removeItem(TOKEN_FALLBACK_KEY);
            } catch {
                // Nothing else can be done when browser storage is disabled.
            }
        },
    };
}

export function loadPreferences(local = browserLocalStorage()) {
    try {
        const value = JSON.parse(local?.getItem(PREFERENCES_KEY) ?? "null");
        return value && typeof value === "object" ? value : {};
    } catch {
        return {};
    }
}

export function savePreferences(value, local = browserLocalStorage()) {
    try {
        local?.setItem(PREFERENCES_KEY, JSON.stringify(value));
        return true;
    } catch {
        return false;
    }
}

export const storageKeys = Object.freeze({
    tokenFallback: TOKEN_FALLBACK_KEY,
    preferences: PREFERENCES_KEY,
});
