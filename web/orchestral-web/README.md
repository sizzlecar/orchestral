# Orchestral Web

The mobile control surface is a Dioxus 0.7 WebAssembly application. It consumes
the existing `/api/v1` Host API; browser rendering is not coupled to runtime or
model-provider code.

The workspace starts on Codex. The Codex, Claude Code, and Orch tabs open the
two most recently updated sessions in vertically stacked panes. Each pane owns
its selection, transcript, controls, draft, and live connection. Background
refresh preserves manually selected sessions. Open the arrow beside a pane's title to choose
an older session; a session already shown in the other pane is excluded.

On phones, compact single-line headers leave the conversation visible and both
panes share one bottom composer. Tap a header to choose its sending target;
the composer names that target and retains each session's own draft. Directory
metadata is available in the session drawer. A software keyboard temporarily
expands the editing pane, with a switch beside the target label; closing the
keyboard restores both panes without restarting either live stream.

Opening a conversation follows the latest message after layout commits. Built-in
Orch sessions initially load their latest 20 Runs, newest first, while displaying
them chronologically. Earlier Runs are loaded on demand with the reading position
preserved. Run event streams remain complete for correct protocol reduction.

When the page is hidden or leaves the foreground, browser session and Run
streams pause. Host-owned execution continues. Returning to the page refreshes
native history and resumes observation without clearing drafts or loaded history.
Idle status badges are static, and unchanged heartbeat snapshots use HTTP ETags.

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
cargo test --locked -p orchestral-web
cargo check --locked -p orchestral-web --target wasm32-unknown-unknown --features web
cd web/orchestral-web && dx serve --web
```

Refresh the release bundle after changing Rust, CSS, or public assets:

```sh
cargo install dioxus-cli --version 0.7.9 --locked
rustup target add wasm32-unknown-unknown
scripts/build_web.sh
```

The build script uses `Cargo.lock`, disables release DWARF before `wasm-opt`,
and replaces stale hashed assets in `dist/`.

## Browser regression smoke

After rebuilding, install the pinned browser test dependencies with Node.js 22+:

```sh
npm ci --prefix scripts --ignore-scripts
(cd scripts && npx playwright install chromium)
export NODE_PATH="$PWD/scripts/node_modules"
export PWA_SMOKE_CHANNEL=chromium
```

Run from the repository root:

```sh
node scripts/pwa_browser_smoke.cjs
node scripts/pwa_local_host_smoke.cjs
node scripts/pwa_dual_session_smoke.cjs
node scripts/pwa_history_sync_smoke.cjs
node scripts/pwa_power_smoke.cjs
node scripts/pwa_draft_smoke.cjs
node scripts/pwa_mobile_layout_smoke.cjs
PWA_SMOKE_ENGINE=webkit node scripts/pwa_mobile_layout_smoke.cjs
PWA_SMOKE_WIDTH=320 node scripts/pwa_browser_smoke.cjs
PWA_SMOKE_SW=1 node scripts/pwa_browser_smoke.cjs
PWA_SMOKE_SW=1 node scripts/pwa_image_message_smoke.cjs
PWA_SMOKE_SW=1 PWA_SMOKE_WIDTH=320 node scripts/pwa_image_message_smoke.cjs
```

Set `NODE_PATH` if Playwright is installed outside the repository, or
`PWA_SMOKE_CHANNEL` to use another installed Chromium channel. The smoke serves
the real release bundle with an isolated HTTP fixture on a random loopback
port. It checks ambiguous submission retry, stable identity, message ordering,
approval visibility and retry, per-session drafts, IME Enter, offline editing,
and mobile layout. Screenshots are written to `target/pwa-smoke/`.

The draft smoke verifies that storage failures preserve in-memory drafts and
that uploads completing after a session moves between panes remain visible.
The mobile layout fixture checks measured reading space, one shared composer,
keyboard viewport resize/scroll, sending-target transfer, and rotation in
Chromium and WebKit. It simulates viewport events rather than a physical keyboard.

The history-sync smoke advances the native transcript without sending a session
event, while the SSE connection continues receiving heartbeats. It checks that
bounded snapshot reconciliation restores intermediate and final replies, retries
a failed read, preserves loaded older pages, and backfills after reconnection.

The power smoke checks that an idle mobile layout stops continuous style
recalculation, unchanged snapshots return 304, hidden panes detach their streams
and stop polling, and foreground restoration preserves history and drafts.

The image-message smoke uploads real image bytes and reconciles the local
preview, HTTP acknowledgement, durable input event, refreshed signed attachment
links, and native transcript echo. It verifies one bubble per submission,
distinct identities for intentionally repeated content, and working drawer
interaction after sending and refreshing the session. Duplicate timeline keys
previously caused a Dioxus `invalid key` panic that disabled page interaction.

The fixture never connects to a real Host or model. By default service workers
are disabled so the smoke tests the bundle just built. `PWA_SMOKE_SW=1` enables
the real worker, simulates a release while the page stays open, and checks
foreground update detection, the explicit refresh action, and preservation of
drafts in multiple sessions. Device-specific suspend/resume still needs device
testing.
