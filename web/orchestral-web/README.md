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
