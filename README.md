# Orchestral

Orchestral is a Rust-native runtime for long-lived, resumable interactions and task orchestration.

This README focuses on **how to run and use it**.

## What You Can Run

- `orchestral-cli`: terminal UI for interactive runs (only depends on core, no platform dependencies)
- `orchestral-server`: HTTP + SSE runtime server (includes all backends and extensions)
- `orchestral-web`: browser UI that mirrors CLI conversation and step activity

## Prerequisites

- Rust toolchain (stable, 1.91.0+)
- Node.js 18+ (for web UI)
- API key for your configured planner/interpreter backend

CLI supports zero-manual-config startup. If no config file is provided/found, it generates a minimal runtime config automatically and reads provider keys from environment variables.
Set at least:

```bash
export OPENAI_API_KEY=your_key
```

If you use a different backend/provider, set the corresponding key (for example `GEMINI_API_KEY`, `ANTHROPIC_API_KEY`).

## Quick Start (CLI)

### 1. Build

```bash
cargo build -p orchestral-cli
```

### 2. Start interactive CLI (TUI)

```bash
cargo run -p orchestral-cli
```

You will get a chat-like terminal session.

Optional flags:

```bash
# Disable MCP auto-discovery/actions
cargo run -p orchestral-cli -- run --no-mcp "hello"

# Disable Skill auto-discovery/actions
cargo run -p orchestral-cli -- run --no-skills "hello"

# Use an explicit config file
cargo run -p orchestral-cli -- run --config configs/orchestral.cli.yaml
```

### 3. Run one turn directly

```bash
cargo run -p orchestral-cli -- run "Plan a 3-day Tokyo trip"
```

### 4. Run from script file (multi-turn)

```bash
cargo run -p orchestral-cli -- run --script scripts/cli_multiturn_regression.sh
```

## Run Server + Web UI

### 1. Start server

```bash
cargo run -p orchestral-server -- --config configs/orchestral.cli.yaml --listen 127.0.0.1:8080
```

Server endpoints:

- `GET /health`
- `POST /threads/{session}/messages`
- `GET /threads/{session}/events` (SSE)

### 2. Start web app

```bash
cd web/orchestral-web
npm install
npm run dev
```

Open: `http://127.0.0.1:5173`

### 3. Use web app

- Enter a `Thread ID` to start or resume a conversation
- Submit input from the bottom composer
- Watch planning/execution activity in a CLI-like timeline
- Assistant output supports streaming and deduped rendering

## Common Commands

```bash
# Run tests
cargo test --workspace --all-targets

# Format
cargo fmt --all

# Lint
cargo clippy --workspace --all-targets -- -D warnings

# Example program
cargo run --example basic_usage
```

## Architecture Overview

The project follows a **two-tier architecture** with strict one-way dependencies:

```
Core (Tier 1)  ←  Platform (Tier 2)  ←  Apps
```

- **Core**: types, traits, SPI contracts, config, in-memory stores, runtime, planners, actions, API — everything needed for a minimal standalone binary.
- **Platform**: optional layer providing persistent store backends, blob storage, HTTP server, extension host, and business extensions.
- **Apps**: composition roots that wire core and (optionally) platform together.

CI enforces that `core/` crates never depend on `platform/`.

## Workspace Layout

```text
orchestral/
├── core/                              # Tier 1: publishable as library
│   ├── orchestral-core/               #   types + traits + SPI + config + in-memory stores
│   ├── orchestral-runtime/            #   runtime + context + planners + actions + API
│   └── orchestral/                    #   facade crate (re-exports core + runtime)
├── platform/                          # Tier 2: optional, depends on core
│   ├── orchestral-stores-backends/    #   SQLite / Redis / Postgres (feature-gated)
│   ├── orchestral-files/              #   blob storage (local / S3 / hybrid)
│   ├── orchestral-infra/              #   infrastructure factory
│   ├── orchestral-composition/        #   composition root (wires infra + extensions)
│   ├── orchestral-extension-host/     #   extension host system
│   ├── orchestral-docs-assistant/     #   document assistant extension
│   └── orchestral-server/             #   HTTP/SSE server (axum)
├── apps/
│   └── orchestral-cli/                # minimal CLI, depends only on core
├── configs/
├── web/
│   └── orchestral-web/                # browser UI
├── scripts/
└── examples/
```

## Store Backends (Feature Flags)

`orchestral-stores-backends` uses feature flags to control which backends are compiled:

| Feature    | Backends                 | Default |
|------------|--------------------------|---------|
| `sqlite`   | SQLite + SQLite-Vector   | yes     |
| `external` | Redis + Postgres         | no      |
| `all`      | All backends             | no      |

Infra selection is configured via `stores.*.backend` and `blobs.*` in config:
- Supported store backends: `sqlite | sqlite_vector | in_memory | redis | postgres`
- Supported blob backends: `local | s3 | hybrid`
- Default store profile: local SQLite (`event/task=sqlite`, `reference=sqlite_vector`)
- `sqlite_vector` reads vectors from `reference.metadata.embedding_vector` (or `metadata.vector`) as `f32[]`

## Extension Model

- **Extension Point**: trait-level abstraction in core (`Action`, hooks, SPI components).
- **Adapter**: infra implementation for storage/blob backends.
- **Extension Package**: business/runtime extension that contributes actions/hooks/components.
- Layering rule:
  - `core/*` keeps abstractions, orchestration, and in-memory defaults only.
  - `platform/*` holds concrete infra backends and runtime extension packages.
  - `apps/*` are composition roots that wire implementations.
- Config naming:
  - `extensions.runtime` is preferred.
  - `plugins.runtime` remains a backward-compatible alias.
- Runtime extension registration (custom package):

```rust
use std::sync::Arc;
use orchestral_composition::{ComposedRuntimeAppBuilder, RuntimeTarget};
use orchestral_extension_host::{RuntimeExtensionCatalog, RuntimeExtension};

let catalog = RuntimeExtensionCatalog::with_builtin_extensions()
    .with_extension(Arc::new(MyRuntimeExtension), &["my_extension_alias"]);
let builder = ComposedRuntimeAppBuilder::with_extension_catalog(RuntimeTarget::Server, catalog);
```

## Document Assistant Actions (Rust SDK)

- Parser pipeline: `source -> kreuzberg markdown -> downstream`.
- Conversion uses `pandoc`; input is always the markdown emitted by `kreuzberg`.
- Provided by runtime extension `builtin.docs_assistant`, split into dedicated action kinds:
  - `doc_parse`: parse document to normalized markdown + metadata
  - `doc_convert`: convert with pandoc from markdown intermediate
  - `doc_summarize`: summary/outline/overview generation
  - `doc_generate`: markdown generation with optional format conversion
  - `doc_qa`: document QA with evidence snippets
  - `doc_merge`: merge multi-source docs with optional format conversion
- Runtime config reference: `configs/orchestral.cli.yaml`.
- Placeholder embeddings disabled by default (`enable_placeholder_embeddings: false`).

### Document Runtime Dependencies

- `pandoc` must be installed and available in `PATH`.
- PDF parsing is optional and disabled by default. To enable: build with `--features orchestral-docs-assistant/pdf`.
- When `kreuzberg` is built with `pdf` support, PDFium is required at build time:
  - either allow network download of prebuilt PDFium binaries, or
  - set `KREUZBERG_PDFIUM_PREBUILT` to a local PDFium directory.

## Troubleshooting

- Missing model key: verify `GEMINI_API_KEY` (or provider-specific key) is exported.
- No web updates: ensure server is running and SSE endpoint is reachable.
- Empty/failed runs: check runtime logs at `logs/orchestral-runtime.log`.
- PDF-related build failures: verify PDFium availability (`KREUZBERG_PDFIUM_PREBUILT`) or network access for `kreuzberg` prebuilt download.
- Runtime extension actions (`doc_*`) require composition builder wiring (`ComposedRuntimeAppBuilder`), which CLI now uses by default.
- `runtime.concurrency_policy=queue` is intentionally unsupported in bootstrap (use `interrupt`, `parallel`, or `reject`).

## License

MIT. See `LICENSE`.
