# Orchestral

Orchestral is a Rust-native runtime for long-lived, resumable interactions and task orchestration.

This README focuses on **how to run and use it**.

## What You Can Run

- `orchestral-cli`: terminal UI for interactive runs
- `orchestral-server`: HTTP + SSE runtime server
- `orchestral-web`: browser UI that mirrors CLI conversation and step activity

## Prerequisites

- Rust toolchain (stable)
- Node.js 18+ (for web UI)
- API key for your configured planner/interpreter backend

Default config is `configs/orchestral.cli.yaml` and currently uses Google Gemini models for planning/interpreting.
Set at least:

```bash
export GEMINI_API_KEY=your_key
```

If you switch models/providers in config, set the corresponding key (for example `OPENAI_API_KEY`).

## Quick Start (CLI)

### 1. Build

```bash
cargo build
```

### 2. Start interactive CLI (TUI)

```bash
cargo run -p orchestral-cli
```

You will get a chat-like terminal session.

### 3. Run one turn directly

```bash
cargo run -p orchestral-cli -- run "Plan a 3-day Tokyo trip"
```

## Run Server + Web UI

### 1. Start server

```bash
cargo run -p orchestral-cli -- server --config configs/orchestral.cli.yaml --listen 127.0.0.1:8080
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
cargo test

# Format
cargo fmt

# Lint
cargo clippy --all-targets --all-features

# Example program
cargo run --example basic_usage
```

## Workspace Layout

```text
orchestral/
├── crates/
│   ├── orchestral-core/
│   ├── orchestral-runtime/
│   ├── orchestral-stores/
│   ├── orchestral-actions/
│   └── orchestral-composition/
├── adapters/
│   ├── orchestral-infra/
│   ├── orchestral-files/
│   └── orchestral-stores-backends/
├── extensions/
│   ├── orchestral-extension-host/
│   └── orchestral-docs-assistant/
├── apps/
│   ├── orchestral-cli/
│   └── orchestral-server/
├── configs/
├── web/
│   └── orchestral-web/
└── examples/
```

## Extension Model

- `Extension Point`: trait-level abstraction in core/runtime (`Action`, hooks, SPI components).
- `Adapter`: infra implementation for storage/blob backends (Redis/Postgres/S3, etc.).
- `Extension Package`: business/runtime extension that contributes actions/hooks/components.
- Layering rule:
  - `crates/*` keep abstractions and orchestration only.
  - `adapters/*` hold concrete infra backends.
  - `extensions/*` hold runtime extension packages.
  - `apps/*` are composition roots; they wire `infra + extensions`.
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

- Infra selection is **not** done via runtime extensions:
  - stores are configured by `stores.*.backend`
  - blob storage is configured by `blobs.*`
  - supported backends include `in_memory | redis | postgres` for stores and
    `local | s3 | hybrid` for blobs.

## Document Assistant Actions (Rust SDK)

- Parser pipeline is standardized as `source -> kreuzberg markdown -> downstream`.
- Conversion still uses `pandoc`, but conversion input is always the markdown emitted by `kreuzberg`.
- Document capabilities are provided by runtime extension `builtin.docs_assistant` and split into dedicated action kinds:
  - `doc_parse`: parse document to normalized markdown + metadata
  - `doc_convert`: convert with pandoc from markdown intermediate
  - `doc_summarize`: summary/outline/overview generation
  - `doc_generate`: markdown generation with optional format conversion
  - `doc_qa`: document QA with evidence snippets
  - `doc_merge`: merge multi-source docs with optional format conversion
- Runtime config reference: `configs/orchestral.cli.yaml`.
- Docs ingestion placeholder embeddings are disabled by default (`enable_placeholder_embeddings: false`).

### Document Runtime Dependencies

- `pandoc` must be installed and available in `PATH`.
- PDF parsing is optional and disabled by default in the docs extension. To enable it, build with `--features orchestral-docs-assistant/pdf`.
- When `kreuzberg` is built with `pdf` support, PDFium is required at build time:
  - either allow network download of prebuilt PDFium binaries, or
  - set `KREUZBERG_PDFIUM_PREBUILT` to a local PDFium directory.

## Troubleshooting

- Missing model key: verify `GEMINI_API_KEY` (or provider-specific key) is exported.
- No web updates: ensure server is running and SSE endpoint is reachable.
- Empty/failed runs: check runtime logs at `logs/orchestral-runtime.log`.
- PDF-related build failures: verify PDFium availability (`KREUZBERG_PDFIUM_PREBUILT`) or network access for `kreuzberg` prebuilt download.
- Runtime extension actions (`doc_*`) require composition builder wiring (`ComposedRuntimeAppBuilder`). `RuntimeApi::from_config_path` is minimal-runtime only.
- `runtime.concurrency_policy=queue` is intentionally unsupported in bootstrap (use `interrupt`, `parallel`, or `reject`).

## License

See repository license information.
