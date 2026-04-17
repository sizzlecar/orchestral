# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
cargo build --workspace              # Build all crates
cargo test --workspace --all-targets # Run all tests (CI runs this)
cargo test -p orchestral-core        # Test a single crate
cargo test -p orchestral-runtime -- test_name  # Run a single test
cargo fmt --all                      # Format
cargo clippy --workspace --all-targets -- -D warnings  # Lint (CI enforces -D warnings)
```

Run the CLI: `cargo run -p orchestral-cli -- run` (requires a provider key like `OPENROUTER_API_KEY`).

Run a scenario smoke test: `cargo run -p orchestral-cli -- scenario --spec configs/scenarios/tier1/<name>.yaml --env-file .env.local`

Toolchain: Rust 1.91.0 stable (pinned in `rust-toolchain.toml`).

## Architecture

Orchestral is a Rust-native intent-first runtime for orchestrating long-lived, resumable tasks as dynamic DAGs. Not a chatbot framework — a general-purpose interaction runtime.

### Crate Hierarchy

```
core/orchestral-core     — Pure abstractions: Intent/Plan/Step/Task, Planner/Normalizer/Executor traits, stores, DAG. No I/O.
core/orchestral-runtime  — Stateful runtime: LLM planners, built-in actions, ThreadRuntime, Orchestrator, agent loop. I/O-heavy.
core/orchestral          — Facade: re-exports core + runtime.
apps/orchestral-cli      — CLI + TUI (ratatui/clap), scenario runner.
examples/                — Runnable demos.
```

**Dependency rule enforced by CI:** core crates must never depend on `platform/` or `apps/`. The architecture guard in `.github/workflows/ci.yml` checks this.

### Core Pipeline: Intent → Plan → Normalize → Execute

1. **Event arrives** → ThreadRuntime routes it, applies ConcurrencyPolicy (Interrupt/Queue/Parallel/Merge)
2. **Orchestrator** parses event into an Intent, builds PlannerContext (available actions, history, skill instructions, skill summaries, loop context)
3. **Agent Loop** (multi-iteration): Planner generates `PlannerOutput` (SingleAction | MiniPlan | Done | NeedInput)
   - If SingleAction/MiniPlan → Normalizer validates DAG + injects implicit steps → Executor runs topologically with parallel scheduling, retry, approval gates
   - Execution result becomes an observation; loop continues if not terminal
   - Done/NeedInput → return to user
4. **WorkingSet** carries per-step outputs and inter-step bindings through the DAG

### Key Traits (extension points)

| Trait | Location | Purpose |
|-------|----------|---------|
| `Planner` | `orchestral-core/src/planner/mod.rs` | Intent → PlannerOutput |
| `Action` | `orchestral-core/src/action/mod.rs` | Atomic execution unit with typed I/O |
| `EventStore` | `orchestral-core/src/store/event_store.rs` | Append-only event journal |
| `TaskStore` | `orchestral-core/src/store/task_store.rs` | Mutable task state |
| `ConcurrencyPolicy` | `orchestral-runtime/src/concurrency.rs` | Decide interrupt/queue/parallel |
| `ContextBuilder` | `orchestral-runtime/src/context/mod.rs` | Assemble context window with token budget |
| `PlanValidator` / `PlanFixer` | `orchestral-core/src/normalizer/mod.rs` | DAG validation and auto-repair |

### Actions

Built-in actions in `orchestral-runtime/src/action/`:

- **builtin** — `shell`, `file_read`, `file_write`, `echo`, `http`, `json_stdout`, `tool_lookup`, `skill_activate`
- **mcp** — `McpToolAction` per MCP tool, discovered at startup via `tools/list` probe

Document and structured operations are handled by skills (e.g. `xlsx` skill uses shell + openpyxl), not built-in actions.

### MCP Integration

MCP servers are auto-discovered from `.mcp.json` files. At startup, each server is probed via `tools/list` and every tool is registered as an independent action (`mcp__<server>__<tool>`). The planner catalog shows tool names and descriptions only; the planner calls `tool_lookup` to get the full input schema before invoking an MCP tool.

### Skill System

Skills are SKILL.md files auto-discovered from `.claude/skills/`, `.codex/skills/`, and `skills/` directories (relative to config path and CWD). Discovery uses `canonicalize()` on the config path for stable resolution.

- **Auto-injection** — keyword matching scores skills against the user intent; top matches are injected into the planner prompt
- **Catalog listing** — all skill names + descriptions appear in the planner's capability catalog
- **On-demand activation** — planner can call `skill_activate` to load full instructions for any skill not auto-matched

### Runtime Model

- **Thread** — persistent context (chat session, workflow instance)
- **Interaction** — one request/response cycle within a thread
- **Event** — UserInput, AssistantOutput, Artifact, ExternalEvent, SystemTrace, ResumeAfterWait

## Conventions

### Commits

Follow Conventional Commits: `feat(scope):`, `fix(scope):`, `refactor(scope):`, `test(scope):`, `docs(scope):`, `style(scope):`.

### Development Principles (from AGENTS.md)

- Fix failures at the right abstraction level — improve contracts and typed models, not narrow test-specific hacks.
- Never hardcode fixture details into prompts/planner rules to make a scenario pass. Tests validate general capability.
- Prompt changes must stay generic — no scenario filenames, expected values, or fixture paths.
- Prefer durable fixes that improve multiple scenarios. A change helping only one fixture is likely wrong direction.
- Keep core crates clean: domain models, traits, lifecycle contracts, minimal in-memory defaults only.
- Concrete infra implementations (S3/PG/Redis) go in `plugins/`, wired at app-level composition root.

## Configuration

Main config: `configs/orchestral.cli.yaml`. Runtime override: `configs/orchestral.cli.runtime.override.yaml`. Scenario smoke tests organized in three tiers under `configs/scenarios/{tier1,tier2,tier3}/`: Tier 1 = capability contracts (one per dimension), Tier 2 = integration paths (end-to-end), Tier 3 = robustness (errors/pressure).

Scenario layer has a deliberate scope: observable user-facing behavior and LLM decision correctness. Purely internal mechanics (ConcurrencyPolicy interrupt/queue/parallel timing, HTTP action I/O, MCP error propagation) live in unit/integration tests under the owning crate — scenario layer would make those tests unstable and slow without adding signal.

Skills are auto-discovered from `.claude/skills/` directories with `SKILL.md` instruction files. MCP servers from `.mcp.json` files.

LLM planner prompt templates live in `core/orchestral-runtime/src/prompts/` (constitution, execution rules, JSON examples).
