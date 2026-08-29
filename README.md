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
      ├── direct Tool ─────────────────────────┐
      └── optional injected Workflow → Plan/DAG┤
                                     ▼
                         GuardedToolRuntime
                           ├── built-in tools
                           └── MCP tools (stdio / Streamable HTTP)
```

## Quick start

Export one configured provider key:

```bash
export OPENAI_API_KEY="..."
# or GOOGLE_API_KEY / OPENROUTER_API_KEY / DEEPSEEK_API_KEY
```

Run one turn:

```bash
cargo run -p orchestral-cli -- "Summarize the public API of this repository"
```

Start a full-screen interactive Agent Session:

```bash
cargo run -p orchestral-cli --
```

The root command is the Agent entry point; there is no `agent` subcommand. Entry mode is
deterministic:

| Invocation | Mode |
| --- | --- |
| `orchestral` with terminal stdin and stdout | Multi-turn TUI |
| `orchestral "fix the bug"` | One-turn Headless |
| `printf 'fix the bug' \| orchestral` | One-turn Headless |

Headless stdout contains only the final Delivery, so it is safe to pipe into another command;
progress and errors use stderr. In the TUI, Enter sends or steers, Shift/Alt+Enter inserts a
newline, `a`/`d` resolves an approval, Ctrl-C cancels the active Run, PageUp/PageDown or the mouse
wheel scrolls, and Esc exits. Paste, resize, CJK, and emoji are supported.
`completed` means that the current turn settled and committed its output; it is not an independent
claim that the user's external goal was achieved.

The CLI discovers `.orchestral/config.yaml`, `.orchestral/config.yml`,
`configs/orchestral.cli.yaml`, then `orchestral.yaml`; if none exists it creates
`.orchestral/config.yaml`. Use `--config`, `--backend`, `--model-profile`, or `--model` for
explicit selection. For example:

```bash
orchestral --backend deepseek --model deepseek-chat "inspect this crate"
orchestral --backend google --model gemini-3.1-pro-preview "inspect this crate"
```

OpenAI-compatible providers use their configured key environment variable. Google supports
`GOOGLE_API_KEY` for the Gemini API and the standard Application Default Credentials chain for
Vertex AI: `GOOGLE_APPLICATION_CREDENTIALS`, the file created by
`gcloud auth application-default login` (`~/.config/gcloud/application_default_credentials.json`
on Unix), or an attached Google Cloud service account. `--credential-file PATH` is a convenience
override for a service-account JSON key; a Vertex project must resolve from the credential or
`GOOGLE_CLOUD_PROJECT`.

`--session-id` gives multiple turns a stable durable Session identity; `--no-mcp` and
`--no-skills` disable those planes.

Minimal coding task:

```bash
orchestral "Repair the failing project in this workspace, run its tests, and report the verified result."
```

The model sees one structured file-mutation tool, `apply_patch`, for Add/Update/Delete. It cannot
choose workspace roots or approval authority. `file_read`, `apply_patch`, `exec_command` /
`write_stdin`, and MCP calls all remain behind Host policy and effect journaling.

`exec_command` launches one Host-resolved shell and may run ordinary child programs and local
toolchains inside the OS sandbox; it does not require a per-program allowlist. The actual boundary
is the Host-approved read/write roots, exact network targets, captured environment, time/output
limits, exact approval, and effect journal. Ambient environment is not inherited wholesale and
network access is disabled by default. MCP stdio launch identities remain explicitly configured by
the Host. Model-visible arguments cannot expand any of these permissions.

With `skills.auto_discover: true`, the CLI discovers `SKILL.md` packages under workspace
`.claude/skills`, `.codex/skills`, and `skills`, plus any explicit `skills.directories`. Only Skill
descriptors enter initial Context; `skill_read` loads the selected instructions, and relative
resources resolve from that Skill's directory. MCP stays separate: `mcp.servers` accepts
Host-configured stdio or Streamable HTTP transports, and discovered MCP methods become guarded,
namespaced Tools rather than prompt text.

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

## Versioned contracts

- [Agent Protocol v1](docs/agent-foundation/agent-protocol-v1.md)
- [Model Protocol v1](docs/agent-foundation/model-protocol-v1.md)
- [Guarded Tool Runtime v1](docs/agent-foundation/tool-runtime-v1.md) and
  [Tool Artifact v1](docs/agent-foundation/tool-artifact-v1.md)
- [Skill Runtime v1](docs/agent-foundation/skill-runtime-v1.md)
- [MCP Tools Adapter v1](docs/agent-foundation/mcp-tools-adapter-v1.md)

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
- MCP is intentionally Tools-only in Foundation v1; Resources, Prompts, subscriptions, and other
  MCP surfaces are outside this contract.
- The typed Plan/DAG implementation is an optional execution strategy inside one Agent, not the
  top-level product entry point.

## License

See [LICENSE](./LICENSE).
