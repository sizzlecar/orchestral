# Orchestral Web

The mobile control surface is a Dioxus 0.7 WebAssembly application. It consumes
the existing `/api/v1` Host API; browser rendering is not coupled to runtime or
model-provider code.

## Boundaries

- `state.rs` is the deterministic reducer and timeline projection.
- `sse.rs` is a byte-oriented, platform-neutral SSE parser.
- `browser/` owns HTTP, IndexedDB, browser APIs, and stream lifecycle effects.
- `components/` owns Dioxus presentation components.
- `public/` contains PWA metadata and the service worker.
- `dist/` is the release bundle embedded by `orchestral-cli`; do not edit it by
  hand.

## Development

```sh
cargo test -p orchestral-web
cargo check -p orchestral-web --target wasm32-unknown-unknown --features web
cd web/orchestral-web && dx serve --web
```

Refresh the release bundle after changing Rust, CSS, or public assets:

```sh
scripts/build_web.sh
```

The build script uses `Cargo.lock`, disables release DWARF before `wasm-opt`,
and replaces stale hashed assets in `dist/`.

## Browser regression smoke

After rebuilding, run from the repository root with Node.js, Chrome, and
`playwright` installed in a local or external Node module directory:

```sh
node scripts/pwa_browser_smoke.cjs
PWA_SMOKE_WIDTH=320 node scripts/pwa_browser_smoke.cjs
```

Set `NODE_PATH` if Playwright is installed outside the repository, or
`PWA_SMOKE_CHANNEL` to use another installed Chromium channel. The smoke serves
the real release bundle with an isolated HTTP fixture on a random loopback
port. It checks ambiguous submission retry, stable identity, message ordering,
approval visibility and retry, per-session drafts, IME Enter, offline editing,
and mobile layout. Screenshots are written to `target/pwa-smoke/`.

The fixture never connects to a real Host or model. Service workers are disabled
for this smoke so it always tests the bundle just built; installed PWA updates
and device-specific suspend/resume still need device testing.
