# Orchestral

Orchestral is a Rust-first orchestration runtime for long-lived interactions and stage-based task execution.

This README is intentionally user-facing. It describes the repository as it exists today: what currently works, what is still incomplete, what you can do with it now, and how to start using the CLI.

Developer validation flows such as `scenario`, release smokes, and CI commands are intentionally not part of this top-level guide.

## Current Status

The repository is in an active 1.0 convergence phase.

What is already true:

- The runtime is `reactor`-first for supported task shapes.
- Planner output is centered on `SkeletonChoice`, `StageChoice`, `DirectResponse`, and `Clarification`.
- Stage execution is lowered by the runtime into small DAGs instead of letting the planner emit one large workflow.
- `verify` is the completion gate for supported reactor paths.
- The checked-in application is the CLI.

What is not yet complete:

- Not every declared skeleton has full runtime coverage.
- Some families and task shapes are still narrower than the target RFC shape.
- There is no checked-in web frontend.

## What Works Today

Current executable reactor coverage is:

- `locate_and_patch + spreadsheet`
  Can inspect, derive, apply, and verify spreadsheet edits.
- `locate_and_patch + document`
  Can inspect, derive, apply, and verify document and text patching flows.
- `locate_and_patch + structured`
  Can inspect, derive, apply, and verify structured file edits such as JSON, YAML, and TOML.
- `inspect_and_extract + spreadsheet|document|structured`
  Can inspect local artifacts and produce a grounded extracted answer.
- `run_and_verify + codebase`
  Initial codebase-oriented run-and-check flow is implemented.

Direct action paths also exist for built-in actions such as:

- `shell`
- `file_read`
- `file_write`
- `http`

## What You Can Do With It Right Now

Today the repository is best suited for:

- Local artifact patching through the CLI
- Grounded local inspection and extraction tasks
- Structured file changes with verification
- Spreadsheet and document patching workflows
- Codebase-oriented run-and-verify flows

Concrete examples already covered by the current implementation:

- Fill or patch spreadsheets
- Patch markdown and text documents
- Patch structured config files
- Inspect local files and return a structured answer

## Requirements

- Rust stable toolchain
- One usable LLM provider key

Supported provider environment variables:

- `OPENAI_API_KEY`
- `GOOGLE_API_KEY`
- `GEMINI_API_KEY`
- `ANTHROPIC_API_KEY`
- `CLAUDE_API_KEY`
- `OPENROUTER_API_KEY`

The checked-in CLI config prefers direct providers in this order:

1. `OpenAI`
2. `Google`
3. `Anthropic`
4. `OpenRouter`

If the configured default backend has no usable key, runtime will try another configured backend that does.

## How Config Selection Works

If you do not pass `--config`, the CLI looks for config files in this order:

1. `.orchestral/config.yaml`
2. `.orchestral/config.yml`
3. `configs/orchestral.cli.yaml`
4. `configs/orchestral.yaml`
5. `orchestral.yaml`

If none exist, it generates:

- `.orchestral/generated/default.cli.yaml`

You can also load env vars from a file with `--env-file`. The parser accepts:

- `KEY=VALUE`
- `export KEY=VALUE`

## Quick Start

### Build the CLI

```bash
cargo build -p orchestral-cli
```

### Start an interactive session

```bash
cargo run -p orchestral-cli -- run
```

### Run a single turn

```bash
cargo run -p orchestral-cli -- run --once "直接修改 fixtures/scenarios/structured_patch/config/app.json"
```

### Override planner backend or model at runtime

```bash
cargo run -p orchestral-cli -- run \
  --planner-backend google \
  --planner-model-profile gemini-2.5-flash \
  --once "列出 docs 下有哪些文档"
```

### Load provider keys from a file

```bash
cargo run -p orchestral-cli -- run \
  --env-file .env.local \
  --once "读取 docs 下的内容并总结"
```

### Disable MCP or skills when needed

```bash
cargo run -p orchestral-cli -- run --no-mcp --no-skills --once "hello"
```

## Repository Layout

```text
orchestral/
├── core/
│   ├── orchestral-core/       # contracts, config, types, executor, normalizer, recipe
│   ├── orchestral-runtime/    # runtime, planner, reactor lowering, actions, bootstrap
│   └── orchestral/            # facade crate
├── apps/
│   └── orchestral-cli/        # interactive CLI
├── configs/                   # checked-in runtime and scenario configs
├── docs/                      # RFCs, dev notes, implementation docs
├── examples/                  # runnable examples and focused demos
├── fixtures/                  # scenario fixtures
├── scripts/                   # helper scripts
└── logs/                      # runtime logs when enabled
```

## Architecture Snapshot

The current supported runtime path is:

```text
User Input
-> Planner
-> SkeletonChoice / StageChoice
-> Runtime lowers a stage-local DAG
-> Typed actions + bounded derivation
-> continuation / verify
-> next stage or completion
```

This is not yet full coverage for every declared skeleton, but it is the direction the codebase is already executing in.

## Next Steps

Based on the current codebase, the next work is:

- Expand runtime coverage for the remaining declared skeletons
- Continue tightening family contracts and verify behavior
- Keep improving user-facing CLI usability and runtime stability
- Continue splitting oversized modules while preserving behavior

The current focus is not “more prompt text”, but clearer runtime contracts:

- skeleton selection
- stage lowering
- explicit continuation
- verify-gated completion

## Troubleshooting

- If a run fails early, check `logs/orchestral-runtime.log`.
- If the configured backend has no usable key, runtime may fall back to another configured backend with an available key.
- If you are working on repository validation rather than normal CLI usage, use the repository's maintainer workflows instead of the end-user CLI path described here.

## License

See `LICENSE`.
