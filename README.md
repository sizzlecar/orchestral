# Orchestral

Provider-neutral runtime for running one Agent safely, durably, and interactively.

[中文版本](./README.zh-CN.md)

> Status: the Agent Foundation is under active development. The current scope is a complete
> single-Agent runtime contract and implementation—not goal compilation, task brokering, or
> multi-Agent orchestration.

## What exists today

- **Agent Protocol v1** — versioned Run/Session contracts, commands, durable events,
  inspection, cancellation, recovery, and exactly one terminal projection.
- **Generic Agent** — one provider-neutral `Model → Tool/Workflow → Model` loop shared by
  CLI, SDK, and API surfaces.
- **Model adapters** — OpenAI-compatible and Gemini-native protocols behind the same
  [`ModelBackend` contract](testing/orchestral-model-protocol-testkit/README.md) and conformance
  suite.
- **Guarded Tool Runtime** — Host-owned policy, approval capabilities, cancellation, effect
  journaling, artifact spill, and conservative `UnknownEffect` handling.
- **Two distinct extension planes** — Skills add trusted instructions to Context; MCP tools
  enter the Action plane and always pass through the guarded runtime.
- **Optional Workflow strategy** — complex calls reuse the typed Plan normalizer, DAG, and
  executor. A Workflow is subordinate to its Agent Run and cannot create a second terminal.
- **Durable context** — Run, Session, Tool Effect, and Generic Agent checkpoint journals can
  use filesystem-backed plugins and recover across process replacement.

```text
CLI / SDK / API
      │
      ▼
AgentController ── Agent Protocol + durable Run journal
      │
      ▼
Generic Agent ─── ModelBackend + durable Session context
      │
      ├── direct Tool ───────────────┐
      └── Workflow → Plan/DAG/Step ──┤
                                     ▼
                         GuardedToolRuntime
                           ├── built-in tools
                           └── MCP stdio tools
```

## Quick start

Export one configured provider key:

```bash
export OPENAI_API_KEY="..."
# or GOOGLE_API_KEY / OPENROUTER_API_KEY / DEEPSEEK_API_KEY
```

Run one turn:

```bash
cargo run -p orchestral-cli -- agent "Summarize the public API of this repository"
```

Start an interactive Agent Session:

```bash
cargo run -p orchestral-cli -- agent
```

The CLI discovers `configs/orchestral.cli.yaml` by default. Use `--config`, `--backend`,
`--model-profile`, or `--model` for explicit selection. `--session-id` gives multiple turns a
stable durable Session identity; `--no-mcp` and `--no-skills` disable those planes.

The default Host policy uses an explicit process allowlist, does not inherit the ambient
environment into tools, and keeps network access disabled. A model-visible tool call cannot
expand those permissions.

## SDK

The public SDK is the Agent control plane: `AgentClient` starts Runs and `AgentRunHandle`
provides events, inspection, commands, input resolution, steering, cancellation, and terminal
waiting. It does not expose the retired Planner loop.

Run the complete provider-neutral example:

```bash
cargo run -p orchestral-examples --example agent_session
```

See [`examples/agent_session.rs`](examples/agent_session.rs) for the minimal composition of a
`ModelBackend`, `InternalGenericAgentProvider`, `AgentController`, and `AgentClient`.

## Project structure

```text
core/orchestral-core      Agent/Model/Tool/Skill/MCP contracts and deterministic Plan/DAG core
core/orchestral-runtime   Agent controller, Generic Agent, context, guarded tools, Workflow bridge
core/orchestral           facade re-exporting the public core and runtime APIs
plugins/                  filesystem journals/blob store and concrete model adapters
apps/orchestral-cli       conversational CLI composition root
examples/                 runnable Agent Session example
testing/                  protocol conformance and property-test harnesses
```

Concrete infrastructure belongs in `plugins/` and is wired by an application composition root;
core/runtime crates depend only on contracts.

## Development

```bash
cargo build --workspace
cargo test --workspace --all-targets
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## Current boundaries

- This is not yet a Goal Compiler, Task Broker, or multi-Agent scheduler.
- `DeliveryCommitted` means the Agent delivered an output; it does not mean an external goal was
  independently satisfied or verified.
- MCP stdio tools use the guarded Action path. Streamable HTTP and the remaining quantitative
  security/recovery gates are not yet release-complete.
- The typed Plan/DAG implementation is an optional execution strategy inside one Agent, not the
  top-level product entry point.

## License

See [LICENSE](./LICENSE).
