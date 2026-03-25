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

Run the CLI: `cargo run -p orchestral-cli -- run` (requires a provider key like `OPENAI_API_KEY`).

Run a scenario smoke test: `cargo run -p orchestral-cli -- scenario configs/scenarios/<name>.smoke.yaml`

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
2. **Orchestrator** parses event into an Intent, builds PlannerContext (available actions, history, skill instructions, loop context)
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

### Built-in Action Families (orchestral-runtime/src/action/)

`builtin` (shell, file_read, file_write, json_extract, wait), `document`, `spreadsheet`, `structured`, `codebase`, `mcp`, `skill` — each family has inspect/locate/derive/apply/verify/assess variants.

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

Main config: `configs/orchestral.yaml`. Action config: `configs/orchestral.cli.yaml`. Scenario smoke tests: `configs/scenarios/*.smoke.yaml`.

Skills are auto-discovered from `.claude/skills/` directories with `SKILL.md` instruction files.

LLM planner prompt templates live in `core/orchestral-runtime/src/planner/llm/` (constitution, rules, JSON examples).
