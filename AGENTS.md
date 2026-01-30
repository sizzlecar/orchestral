# Repository Guidelines

## Project Structure & Module Organization
- Workspace root: `Cargo.toml`, `Cargo.lock`, `readme.md`.
- Core crates:
  - `orchestral-core/` — deterministic abstractions (Intent/Plan/Step/Task), planner/normalizer/executor.
  - `orchestral-runtime/` — Thread/Interaction model, concurrency policies, runtime orchestration.
  - `orchestral-stores/` — Event/Task/Reference store traits + in-memory implementations.
  - `orchestral-actions/` — placeholder for built-in Actions.
- `examples/` — runnable demos (e.g., `examples/basic_usage.rs`).
- `docs/` — developer notes (`docs/dev.md`).
- `target/` — build artifacts (generated).

## Build, Test, and Development Commands
- `cargo build` — build all workspace crates.
- `cargo test` — run tests (currently minimal/no tests).
- `cargo run --example basic_usage` — run the end-to-end example.
- `cargo fmt` — format Rust code with rustfmt.
- `cargo clippy --all-targets --all-features` — lint with Clippy.

## Coding Style & Naming Conventions
- Rust 2021 edition; 4-space indentation.
- Use rustfmt defaults; keep modules small and well-scoped.
- Naming: `snake_case` for modules/functions, `CamelCase` for types/traits, `SCREAMING_SNAKE_CASE` for constants, crate names in kebab-case (e.g., `orchestral-core`).
- Prefer explicit types at public boundaries; keep public APIs documented.

## Testing Guidelines
- Uses Rust’s built-in test harness.
- Add unit tests under `mod tests` in the same file or create crate-level `tests/` directories.
- Naming: `test_*` functions, files like `*_test.rs` if using integration tests.
- Run per-crate: `cargo test -p orchestral-core` (or other crate).

## Commit & Pull Request Guidelines
- Git history follows Conventional Commits (e.g., `feat(workspace): …`, `feat(init): …`). Please keep using this pattern.
- PRs should include: a concise summary, rationale, linked issues (if any), and tests run. For behavior changes, include example output or reproduction steps.

## Architecture Overview
- Flow: Intent → Plan → Normalize → Execute (core).
- Runtime manages Thread/Interaction lifecycle and concurrency decisions.
- Stores persist events and task/reference state; Actions encapsulate side effects.
