const CACHE_PREFIX = "orchestral-shell-";
const CACHE_NAME = `${CACHE_PREFIX}v4`;
const SHELL_FILES = [
    "./",
    "./index.html",
    "./styles.css",
    "./app.js",
    "./manifest.webmanifest",
    "./modules/api.js",
    "./modules/sse.js",
    "./modules/state.js",
    "./modules/storage.js",
    "./modules/view.js",
    "./icons/favicon.svg",
    "./icons/icon-192.svg",
    "./icons/icon-512.svg",
    "./icons/maskable-icon.svg",
    "./icons/apple-touch-icon.svg",
    "./icons/icon-192.png",
    "./icons/icon-512.png",
    "./icons/maskable-icon.png",
    "./icons/apple-touch-icon.png",
];

const shellUrls = new Set(SHELL_FILES.map((path) => new URL(path, self.location.href).href));
const offlineDocument = new URL("./index.html", self.location.href).href;

function isSensitive(request, url) {
    if (request.headers.has("Authorization")) return true;
    const value = `${url.pathname}${url.search}`.toLowerCase();
    return /(?:^|\/)api(?:\/|$)/.test(url.pathname.toLowerCase())
        || /(?:^|\/)(?:stream|transcript|token)(?:\/|$|[?&])/.test(value)
        || /[?&](?:access_?token|token|secret|transcript)=/.test(value);
}

self.addEventListener("install", (event) => {
    event.waitUntil((async () => {
        const cache = await caches.open(CACHE_NAME);
        await cache.addAll(SHELL_FILES);
        await self.skipWaiting();
    })());
});

self.addEventListener("activate", (event) => {
    event.waitUntil((async () => {
        const names = await caches.keys();
        await Promise.all(names
            .filter((name) => name.startsWith(CACHE_PREFIX) && name !== CACHE_NAME)
            .map((name) => caches.delete(name)));

        // Defense in depth: an older worker must never leave authenticated or
        // transcript-like requests behind in a shell cache.
        const cache = await caches.open(CACHE_NAME);
        const keys = await cache.keys();
        await Promise.all(keys.map((request) => {
            const url = new URL(request.url);
            return isSensitive(request, url) ? cache.delete(request) : Promise.resolve(false);
        }));
        await self.clients.claim();
    })());
});

self.addEventListener("fetch", (event) => {
    const { request } = event;
    if (request.method !== "GET") return;
    const url = new URL(request.url);
    if (url.origin !== self.location.origin) return;

    if (isSensitive(request, url)) {
        event.respondWith(fetch(request, { cache: "no-store" }));
        return;
    }

    if (request.mode === "navigate") {
        event.respondWith((async () => {
            try {
                return await fetch(request, { cache: "no-store" });
            } catch {
                return (await caches.match(offlineDocument))
                    ?? new Response("Orchestral is offline", {
                        status: 503,
                        headers: { "Content-Type": "text/plain; charset=utf-8" },
                    });
            }
        })());
        return;
    }

    if (!shellUrls.has(url.href)) return;
    event.respondWith((async () => {
        const cached = await caches.match(request);
        if (cached) return cached;
        const response = await fetch(request);
        if (response.ok && response.type === "basic") {
            const cache = await caches.open(CACHE_NAME);
            await cache.put(request, response.clone());
        }
        return response;
    })());
});

self.addEventListener("message", (event) => {
    if (event.data?.type === "SKIP_WAITING") void self.skipWaiting();
});

self.addEventListener("notificationclick", (event) => {
    event.notification.close();
    event.waitUntil((async () => {
        const requested = new URL(event.notification.data?.url || "./", self.location.origin);
        const targetUrl = requested.origin === self.location.origin
            ? requested.href
            : new URL("./", self.location.href).href;
        const windows = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
        const existing = windows.find((client) => new URL(client.url).origin === self.location.origin);
        if (existing) {
            await existing.focus();
            if ("navigate" in existing) await existing.navigate(targetUrl);
            return;
        }
        await self.clients.openWindow(targetUrl);
    })());
});
