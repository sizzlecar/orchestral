# CLAUDE.md

Guidance for Claude Code when working in this repository.

## Build and test

```bash
cargo build --workspace
cargo test --workspace --all-targets
cargo test -p orchestral-core
cargo test -p orchestral-runtime -- test_name
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Run one Agent turn:

```bash
cargo run -p orchestral-cli -- run "your request"
```

Omit the request to start an interactive Session. A provider key such as
`OPENAI_API_KEY`, `GOOGLE_API_KEY`, `OPENROUTER_API_KEY`, or `DEEPSEEK_API_KEY` is required.

Toolchain: Rust 1.91.0 stable, pinned in `rust-toolchain.toml`.

## Architecture

Orchestral currently implements the foundation for one provider-neutral Agent. It is not yet a
Goal Compiler, Task Broker, or multi-Agent scheduler.

```text
CLI / SDK / API
      │
      ▼
AgentController ── versioned Agent Protocol + durable Run journal
      │
      ▼
InternalGenericAgentProvider ── ModelBackend + Session context
      │
      ├── direct Tool ───────────────┐
      └── Workflow → Plan/DAG/Step ──┤
                                     ▼
                         GuardedToolRuntime
```

### Crates

```text
core/orchestral-core      deterministic contracts, reducers, Plan/Step/DAG executor
core/orchestral-runtime   Agent control plane, Generic Agent, context, guarded Tool runtime
core/orchestral           public facade re-exporting core and runtime APIs
plugins/                  concrete journals, blob stores, and model adapters
apps/orchestral-cli       conversational CLI composition root
examples/                 provider-neutral Agent Session example
testing/                  Agent Protocol conformance/property harness
```

Dependency direction is strict: runtime depends on contracts only and never on concrete plugins.
Applications wire plugins at their composition root.

### Runtime boundaries

- `AgentController` is the only production start/command/inspect/events control surface.
- `InternalGenericAgentProvider` owns the single `Model → Tool/Workflow → Model` loop.
- `AgentSessionJournal` is the source for context projection and traceable compaction.
- Direct tools and Workflow DAG steps share `GuardedToolRuntime`; there is no ActionRegistry
  fallback after a run-scoped execution port is installed.
- Host policy, approval, credentials, sandbox, cancellation, effect fencing, and artifact spill
  are not model-visible authority.
- A Workflow is subordinate to one Agent Run. `DeliveryCommitted` is the sole delivery terminal;
  it must never be mapped to goal satisfaction or verification.

### Extension planes

- **Skill Runtime / Context Plane:** descriptors may be listed for a bound catalog; full
  instructions enter context only after trust, compatibility, dependency, and digest checks.
  Skill activation cannot expand Tool grants.
- **MCP Runtime / Action Plane:** discovered tools are namespaced and invoked only through
  `GuardedToolRuntime`, approval, cancellation, and the Tool Effect Journal. CLI configuration is
  Host-owned; do not scan arbitrary workspace MCP configuration.
- **Runtime hooks:** contracts belong in core/runtime; concrete implementations belong in
  plugins. Heavy hook work must enqueue rather than block step execution.

## Configuration

The main development config is `configs/orchestral.cli.yaml`. Without `--config`, the CLI searches
`.orchestral/config.yaml`, `.orchestral/config.yml`, the development config, and
`configs/orchestral.yaml`, then creates a safe default if none exists.

The default Tool policy uses an executable allowlist, minimal environment, disabled network, and
workspace-bounded filesystem access. Permission expansion must come from explicit Host policy or
an exact approval capability—never from model arguments, prompts, Skills, or MCP metadata.

## Development principles

- Follow Conventional Commits: `feat(scope):`, `fix(scope):`, `refactor(scope):`,
  `test(scope):`, `docs(scope):`, `style(scope):`.
- Fix failures at the contract, typed model, reducer, adapter, or runtime state boundary.
- Do not hardcode fixture details, expected values, or scenario wording into prompts or runtime
  heuristics.
- Tests prove general invariants. Property/model checks cover state spaces; smoke tests only prove
  wiring.
- Preserve the separation between durable events and lossy telemetry.
- A crash boundary that cannot prove whether an effect happened must become `UnknownEffect`; it
  must not be silently replayed.
- Keep public contracts versioned and explicit. Do not introduce Goal/Broker/multi-Agent domain
  types during the Agent Foundation phase.
