# Developer guide

[Back to README](../../README.md)

## Architecture

- **Agent Protocol v1** — versioned Run/Session contracts, commands, durable events,
  inspection, cancellation, recovery, and exactly one terminal projection.
- **Generic Agent** — one provider-neutral `Model → Tool/Workflow → Model` loop shared by
  CLI, SDK, and API surfaces.
- **Model adapters** — OpenAI-compatible and Gemini-native protocols behind the same
  [`ModelBackend` contract](../../testing/orchestral-model-protocol-testkit/README.md) and conformance
  suite.
- **Guarded Tool Runtime** — Host-owned policy, approval capabilities, cancellation, effect
  journaling, artifact spill, and conservative `UnknownEffect` handling.
- **Two distinct extension planes** — Skills add trusted instructions to Context; MCP tools
  enter the Action plane and always pass through the guarded runtime.
- **Optional Workflow strategy** — complex calls reuse the typed Plan normalizer, DAG, and
  executor. A Workflow is subordinate to its Agent Run and cannot create a second terminal.
- **Durable context** — Run, Session, Tool Effect, and Generic Agent checkpoint journals can
  use filesystem-backed plugins and recover across process replacement.
- **Interactive clients** — a terminal UI and an embedded Dioxus/WASM mobile PWA, with
  session history, input requests, approvals, steering, and cancellation.
- **External Agent integration** — the application wires a Codex connector; the ACP plugin
  is available for SDK hosts to register explicitly.

```text
CLI / SDK / API
      │
      ▼
AgentController ── Agent Protocol + durable Run journal
      │
      ▼
Generic Agent ─── ModelBackend + durable Session context
      │
      ├── direct Tool ─────────────────────────┐
      └── optional injected Workflow → Plan/DAG┤
                                     ▼
                         GuardedToolRuntime
                           ├── built-in tools
                           └── MCP tools (stdio / Streamable HTTP)
```

## SDK

The public SDK is the Agent control plane: `AgentClient` starts Runs and `AgentRunHandle`
provides events, inspection, commands, input resolution, steering, cancellation, and terminal
waiting. It does not expose the retired Planner loop.

Run the complete provider-neutral example:

```bash
cargo run -p orchestral-examples --example agent_session
```

See [`examples/agent_session.rs`](../../examples/agent_session.rs) for the minimal composition of a
`ModelBackend`, `InternalGenericAgentProvider`, `AgentController`, and `AgentClient`.

## Versioned contracts

- [Agent Protocol v1](../../docs/agent-foundation/agent-protocol-v1.md)
- [Model Protocol v1](../../docs/agent-foundation/model-protocol-v1.md)
- [Guarded Tool Runtime v1](../../docs/agent-foundation/tool-runtime-v1.md) and
  [Tool Artifact v1](../../docs/agent-foundation/tool-artifact-v1.md)
- [Skill Runtime v1](../../docs/agent-foundation/skill-runtime-v1.md)
- [MCP Tools Adapter v1](../../docs/agent-foundation/mcp-tools-adapter-v1.md)

## Project structure

```text
core/orchestral-core      Agent/Model/Tool/Skill/MCP contracts and deterministic Plan/DAG core
core/orchestral-runtime   Agent controller, Generic Agent, context, guarded tools, Workflow bridge
core/orchestral           facade re-exporting the public core and runtime APIs
plugins/                  filesystem journals/blob store and concrete model adapters
apps/orchestral-cli       CLI/TUI composition root and HTTP/SSE Host gateway
web/orchestral-web        Dioxus/WASM PWA, including the embedded distribution
examples/                 runnable Agent Session example
testing/                  protocol tests, coding-task evaluation, and Harbor adapter
```

Concrete infrastructure belongs in `plugins/` and is wired by an application composition root;
core/runtime crates depend only on contracts.

## Development

```bash
cargo build --locked --workspace
cargo test --locked --workspace --all-targets
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
bash scripts/check_workspace.sh
bash scripts/check_agent_surface.sh
```

CI checks Linux, macOS and Windows, SDK doctests, the WASM target, the rebuilt PWA in Chromium, and the
Harbor adapter without model calls. Rebuild changed web sources with `scripts/build_web.sh`;
see the [web development guide](../../web/orchestral-web/README.md). The
[release process](../../RELEASING.md) describes packaging, upgrade checks, and optional live tests.

## Coding task evaluation

For standard terminal-agent tasks, the [Harbor adapter](../../testing/orchestral-harbor/README.md)
runs the native CLI in isolated Docker environments with official task verifiers.
Start with an oracle preflight before model runs; small subsets validate the integration
and do not constitute a full benchmark score. Unattended hosts can set
`agent.input_requests_enabled: false` to omit the input-request capability and tool.

`orchestral-coding-eval` runs 20 controlled repair tasks against pinned Orchestral source.
These are seeded regressions in a real repository, not historical issue or cross-repository
benchmarks. Tasks cover retry policy, project instructions, Unicode editing, interaction,
file mutation, and two conversations continued in a fresh CLI process. This first suite does
not measure feature development, forced compaction, or recovery from a killed active Run.

```bash
cargo run -p orchestral-coding-eval -- list
cargo run -p orchestral-coding-eval -- validate --repo .
```

Validation makes no model calls: the reference must pass and the seeded version must fail a
named assertion. Agent grading uses a separate checkout with fixed tests and checks protected
files, existing uncommitted work, and unauthorized staging/commits. A zero-test selection,
compiler error, or an Agent claiming success cannot satisfy verification. The verifier runs
candidate Rust code locally; this is a correctness harness, not isolation for hostile code.

For actual model runs, copy and configure
[`orchestral.example.json`](../../testing/orchestral-coding-eval/agents/orchestral.example.json), including
the executable path, provider/model, and credentials through the provider's usual environment
or an explicit `--credential-file` argument. `{prompt}`, `{workspace}`, `{config}`, and
`{session_id}` are substituted as literal argv values; no shell interpolation is used.
Other agents can supply their own invocation file; those adapters are not prevalidated.
The generated Orchestral config allows 32 model steps and 96 tool calls per Run. Agent turns
default to a 300-second deadline (`--timeout-secs`); each verification command gets 600 seconds
(`--verify-timeout-secs`). The generated config and binary/config digests accompany the report.

```bash
# Explicitly invokes the configured model and may incur provider charges.
cargo run -p orchestral-coding-eval -- run --repo . \
  --agent-config /absolute/path/eval-agent.json \
  --task retry-backoff-cap --repetitions 1
# Omit --task for all 20 tasks; the default is 3 repetitions per task.
```

JSON reports, prompts, patches, process logs and available session journals go to a fresh OS
temporary directory, or `--output PATH`. Validation and actual Agent results are separate;
unrun attempts, timeouts, constraints and infrastructure errors remain visible. Repairs
are independently checked even after an Agent error or timeout; those attempts still
fail the overall completion criterion. Reported
committed-request token usage is deduplicated; unmeasured cost and human corrections are
`null`. Workspaces/build outputs are removed after use unless `--keep-workspaces` is selected
for candidate workspaces. Reports and acceptance records are local artifacts, not source files.

## Current boundaries

- This is not yet a Goal Compiler, Task Broker, or multi-Agent scheduler.
- `DeliveryCommitted` means the Agent delivered an output; it does not mean an external goal was
  independently satisfied or verified.
- MCP is intentionally Tools-only in Foundation v1; Resources, Prompts, subscriptions, and other
  MCP surfaces are outside this contract.
- The typed Plan/DAG implementation is an optional execution strategy inside one Agent, not the
  top-level product entry point.
