# Orchestral

Orchestral is workflow orchestration for grounded agents.

[中文版本](./README.zh-CN.md)

## What It Does

- Orchestrates stateful workflows, not one-off tool calls
- Executes typed actions with an `agent loop` and `mini-DAG`
- Replans from real state and verifies before finishing

## Quick Start

- Rust stable
- Export one provider key, such as `OPENAI_API_KEY`, `GOOGLE_API_KEY`, `ANTHROPIC_API_KEY`, or `OPENROUTER_API_KEY`

```bash
cargo build -p orchestral-cli
```

```bash
cargo run -p orchestral-cli -- run
```

## Current Status

- Usable pre-1.0 CLI
- Core orchestration loop is working
- Typed actions and scenario smokes are in place
- MCP, skills, and release hardening are still in progress

## Next Steps

- Expand action, MCP, and skill coverage
- Tighten action and runtime contracts
- Harden defaults and finish release cleanup

## License

See [LICENSE](./LICENSE).
