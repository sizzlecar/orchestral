# Repository Guidelines

## Project Structure & Module Organization

- Workspace root: `Cargo.toml`, `Cargo.lock`, `README.md`, `README.zh-CN.md`.
- `core/orchestral-core/` — Agent/Model/Tool/Session contracts, event reducer, Plan Normalizer, and DAG Executor.
- `core/orchestral-runtime/` — AgentController, Generic Agent loop, session context, guarded tools, recovery, and workflow integration.
- `core/orchestral/` — public SDK facade re-exporting core/runtime APIs.
- `plugins/` — concrete model and Agent adapters, filesystem journals/blob storage, R2 artifacts, and HTTP MCP transport.
- `apps/orchestral-cli/` — application composition root, headless CLI, TUI, and `serve` HTTP/SSE gateway.
- `web/orchestral-web/` — Dioxus/WASM PWA; `state.rs` owns deterministic projection, `browser/` owns effects, and `components/` owns presentation.
- `testing/` — Agent/Model protocol conformance and property-test harnesses.
- `examples/agent_session.rs` — minimal model/provider/controller/client composition.
- `configs/` — runtime configuration; `docs/agent-foundation/` — versioned protocol contracts.
- `deploy/` and `DEPLOYMENT.md` — deployment assets and instructions.
- `target/` is generated; `web/orchestral-web/dist/` is the generated release bundle embedded by the CLI.

## Architecture Overview

- Public entry points share `AgentController` and Agent Protocol Run/Session contracts. Session holds conversation continuity; Run identifies one execution.
- The built-in Generic Agent executes `Model → Tool/Workflow → Model`, with context projection, budgets, input, approval, Steer, and cancellation.
- Workflow is an optional Run-owned strategy: `Plan → Normalize → DAG → Execute`. Its steps use the same guarded tool runtime and cannot introduce a second top-level terminal state.
- `GuardedToolRuntime` enforces Host policy, Run grants, approval capabilities, and Tool Effect journaling. Preserve conservative handling of `UnknownEffect` during execution and recovery.
- Skills supply context instructions; MCP tools enter the guarded action path. Neither can expand Host-granted permissions.
- Run Journal + Reducer own durable execution state; Session Journal, Tool Effect Journal, and Generic checkpoints serve distinct context and recovery roles. `DeliveryCommitted` means output was delivered, not that an external goal was independently verified.
- External Agents enter through `AgentDirectory` and `AgentConnector`/`AgentProvider`; adapters translate native commands and events into the shared protocol. The application currently wires Codex.
- PWA clients control the Host over HTTP/SSE. The Host owns shared session coordination; native history and controlled Runs are merged using stable identities and causal anchors.
- Current scope is the single-Agent foundation; Goal Compiler, Task Broker, and multi-Agent scheduling are outside it.

## Build, Test, and Development Commands

- `cargo build --workspace` — build the workspace.
- `cargo test --workspace --all-targets` — run workspace tests.
- `cargo test -p orchestral-runtime` — run a targeted crate's tests; substitute the affected package.
- `cargo run -p orchestral-examples --example agent_session` — run the self-contained SDK example.
- `cargo run -p orchestral-cli -- "Inspect this workspace"` — run a headless turn; no prompt in a terminal enters the TUI.
- `cargo fmt --all -- --check` — check formatting; use `cargo fmt --all` to apply it.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — lint the workspace.
- `cargo check -p orchestral-web --target wasm32-unknown-unknown --features web` — check the browser target.
- `scripts/build_web.sh` — regenerate the embedded PWA bundle after web Rust, CSS, or public asset changes; requires Dioxus CLI and the WASM target. Do not hand-edit `dist/`. See `web/orchestral-web/README.md`.

## Coding Style & Naming Conventions

- Rust 2021 edition; 4-space indentation.
- Use rustfmt defaults; keep modules small and well-scoped.
- Naming: `snake_case` for modules/functions, `CamelCase` for types/traits, `SCREAMING_SNAKE_CASE` for constants, crate names in kebab-case (e.g., `orchestral-core`).
- Prefer explicit types at public boundaries; keep public APIs documented.

## Testing Guidelines

- Use Rust's built-in test harness: unit tests under `mod tests`, integration tests in crate-level `tests/`, and descriptive `snake_case` names.
- For protocol changes, cover the public contract and relevant conformance tests under `testing/`; for recovery changes, cover replay, restart, and duplicate-effect boundaries.
- Keep regression tests focused on general behavior. Frontend projection and SSE parsing can be tested with `cargo test -p orchestral-web` without a browser.
- Some live-provider and native Codex compatibility tests are opt-in (`#[ignore]`) and require credentials or installed tools. Report which checks ran and which were skipped.

## Commit & Pull Request Guidelines

- Git history follows Conventional Commits (e.g., `feat(workspace): …`, `feat(init): …`). Please keep using this pattern.
- PRs should include: a concise summary, rationale, linked issues (if any), and tests run. For behavior changes, include example output or reproduction steps.

## Development Principles

- Fix failures at the relevant contract, typed model, adapter, or runtime state boundary. Improve shared behavior instead of adding test-specific workarounds or special-casing user wording.
- Keep prompts and runtime rules generic. Never embed fixture filenames, paths, expected values, or smoke-specific examples to teach the system a test's answer.
- Strengthen typed inspection, canonical field/path resolution, continuation semantics, and verification contracts before adding heuristics. Tests should validate general capability across scenarios.

## Extension & Dependency Rules

- Keep core/runtime production dependencies independent of `plugins/`. Contracts belong in core; orchestration and guarded execution belong in runtime. Minimal in-memory defaults may live alongside the contracts or runtime services they implement.
- Put concrete model/Agent integrations and infrastructure implementations in `plugins/`; wire them through the application composition root in `apps/*`.
- Keep provider-native wire details inside adapters. Shared control and UI code should consume provider-neutral contracts and opaque identities.
- Expose stable extension SPI for external developers (component factory + runtime hooks), so plugins can be added without modifying runtime internals.
- Define runtime hook points at least for: `before_step`, `after_step`, `on_step_error`, and artifact/blob-related lifecycle events.
- For heavy workflows (upload/parse/chunk/embedding), hooks should enqueue async jobs and avoid blocking the main step execution path.
