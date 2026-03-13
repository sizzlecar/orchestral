# Orchestral

Orchestral is a Rust-native runtime for long-lived, resumable interactions and task orchestration.

This README focuses on **how to run and use it**.

## What You Can Run

- `orchestral-cli`: terminal UI for interactive runs
- `examples/*`: runnable examples for core/runtime flows

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

# Load env vars from a file before boot (KEY=VALUE or export KEY=VALUE)
cargo run -p orchestral-cli -- run --env-file .orchestral/env/openrouter.env "hello"
```

### 3. Run one turn directly

```bash
cargo run -p orchestral-cli -- run "Plan a 3-day Tokyo trip"
```

### 4. Run from script file (multi-turn)

```bash
cargo run -p orchestral-cli -- run --script scripts/cli_multiturn_regression.sh
```

### 5. Run a repeatable scenario

```bash
# Ad-hoc single-turn scenario with structured report and assertions
cargo run -p orchestral-cli -- scenario \
  --env-file .orchestral/env/openrouter.env \
  --config configs/orchestral.cli.yaml \
  --timeout-secs 300 \
  --max-approvals 0 \
  --max-errors 0 \
  "docs 目前下有一个excel 你看下内容 把需要填的都填了"

# YAML-driven scenario spec
cargo run -p orchestral-cli -- scenario \
  --env-file .orchestral/env/openrouter.env \
  --spec configs/scenarios/one_turn.example.yaml
```

Scenario reports are written to `.orchestral/scenario-runs/*.json` by default.

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

The slim 1.0 workspace centers on a minimal core/runtime + CLI shape:

```
Core  ←  Apps
```

- **Core**: types, traits, SPI contracts, config, in-memory stores, runtime, planners, actions, API
- **Apps**: composition roots that wire the minimal runtime into runnable binaries

## Workspace Layout

```text
orchestral/
├── core/                              # Tier 1: publishable as library
│   ├── orchestral-core/               #   types + traits + SPI + config + in-memory stores
│   ├── orchestral-runtime/            #   runtime + context + planners + actions + API
│   └── orchestral/                    #   facade crate (re-exports core + runtime)
├── apps/
│   └── orchestral-cli/                # interactive CLI
├── configs/
├── web/
│   └── orchestral-web/                # browser UI assets (not part of slim workspace build)
├── scripts/
└── examples/
```

## Troubleshooting

- Missing model key: verify `GEMINI_API_KEY` (or provider-specific key) is exported.
- Empty/failed runs: check runtime logs at `logs/orchestral-runtime.log`.
- `runtime.concurrency_policy=queue` is intentionally unsupported in bootstrap (use `interrupt`, `parallel`, or `reject`).

## License

MIT. See `LICENSE`.
