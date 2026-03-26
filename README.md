# Orchestral

Workflow orchestration for grounded agents.

[中文版本](./README.zh-CN.md)

## What It Does

- Orchestrates stateful workflows, not one-off tool calls
- Executes typed actions with an `agent loop` and `mini-DAG`
- Replans from real state and verifies before finishing

## See It Work

One user command. Orchestral coordinates an MCP data source, a domain skill, and shell execution into a multi-step pipeline:

```
You:  "Query Q4 sales from the API, fill the Excel template with actuals
       and formulas, write a markdown summary comparing to budget."

Orchestral (3 rounds, 5 steps):
  ├─ mcp__sales-api__query_sales_data  → fetch actuals from external API
  ├─ file_read budget.yaml             → load budget targets
  ├─ mcp__sales-api__query_budget_variance → compute variance
  ├─ shell (venv python + openpyxl)    → fill Excel: values, formulas, status
  └─ file_write report.md              → generate comparison report
```

The planner discovers MCP tools at startup (`tool_lookup`), activates the `xlsx` skill for openpyxl guidance, and uses the skill's virtual environment to run Python — all without manual wiring.

**Run it yourself:**

```bash
export OPENROUTER_API_KEY="sk-or-..."
cargo run -p orchestral-cli -- scenario \
  --spec configs/scenarios/sales_report_pipeline.smoke.yaml \
  --env-file .env.local
```

## What Makes This Different

| Feature | Claude Code / Codex | Orchestral |
|---------|-------------------|------------|
| Execution model | Single tool call per turn | Multi-step DAG with dependencies |
| Error handling | Fail and report | Agent loop: observe → replan → retry |
| External tools | Built-in set | MCP servers auto-discovered, per-tool registration |
| Domain knowledge | System prompt | Skills: auto-matched, on-demand activation |
| Verification | None | Typed inspect → patch → verify pipelines |
| State | Stateless | Checkpoint/resume across turns |

## Architecture

```
Intent → Planner (LLM) → Normalizer (DAG validation) → Executor (parallel + retry)
            ↑                                                    ↓
            └──── agent loop: observe execution result ──────────┘
```

- **Agent loop** — planner iterates up to 6 rounds, observing results and replanning
- **MCP integration** — servers probed at startup, each tool registered as a callable action with deferred schema loading via `tool_lookup`
- **Skills** — domain knowledge (SKILL.md files) auto-discovered and injected into planner context; `skill_activate` for on-demand loading
- **Typed actions** — document inspect/patch/verify, structured config patch/verify, shell, file I/O, HTTP

## Quick Start

Rust stable (1.91.0+). Export one provider key:

```bash
export OPENROUTER_API_KEY="sk-or-..."  # or OPENAI_API_KEY, ANTHROPIC_API_KEY, etc.
```

```bash
cargo build -p orchestral-cli
cargo run -p orchestral-cli -- run
```

## Project Structure

```
core/orchestral-core     — Pure abstractions: Intent/Plan/Step, traits, DAG executor
core/orchestral-runtime  — LLM planners, actions, MCP bridge, skill system
core/orchestral          — Facade re-exporting core + runtime
apps/orchestral-cli      — CLI + TUI (ratatui)
```

## Current Status

- Core orchestration loop working with agent loop + mini-DAG
- MCP per-tool registration with deferred schema loading
- Skill auto-discovery and on-demand activation
- Document and structured config typed pipelines
- Spreadsheet operations via xlsx skill + openpyxl
- 18 scenario smoke tests passing

## License

See [LICENSE](./LICENSE).
