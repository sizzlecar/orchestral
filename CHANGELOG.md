# Changelog

## [0.3.0]

Pre-1.0 Agent Foundation release, replacing the workflow-first architecture in 0.2.

### Breaking changes and upgrade notes

- The `orchestral` root command starts an Agent turn or an interactive TUI. The old
  `run` and `scenario` entry points and their configuration formats have been removed.
- SDK integrations use `AgentController`, `AgentClient`, and `AgentRunHandle` with
  Agent Protocol Run/Session contracts. Rework integrations using the old application
  builder, Planner loop, or Action extension protocol; see `examples/agent_session.rs`.
- Start from `configs/orchestral.cli.yaml` and transfer the intended provider, workspace,
  MCP, and Skill settings. Select a provider/model profile explicitly when a particular
  configuration is required. Project instructions and Skills do not expand Host permissions.
- Retain existing journals before upgrading. The old 0.2 Planner state has no automatic
  migration to Agent Foundation journals. Foundation recovery checks runtime/configuration
  identity and rejects incompatible unfinished Runs; it does not silently rerun them.
- Repository-local personal MCP settings, business-specific Skills, the vendored spreadsheet
  bundle, obsolete Python extensions, and one-off development prompts/verifiers were removed.
  Install project-specific resources in your own workspace when needed.

### Added

- Provider-neutral Model → Tool/Workflow → Model execution with versioned Agent, Model,
  Tool, Skill, and MCP contracts and an optional Run-owned Plan/DAG strategy.
- Host-owned permissions, exact approval capabilities, Tool Effect journaling, bounded
  process execution, artifacts, and conservative recovery of uncertain effects.
- Filesystem Run/Session/checkpoint storage, session discovery and continuation, traceable
  context compaction, original-record recall, and project instruction discovery.
- OpenAI-compatible and Gemini-native model adapters; native Codex integration and an ACP
  connector for SDK hosts.
- TUI session/model selection, Unicode editing, file completion, approval/input handling,
  tool evidence, and session/context inspection.
- Embedded Dioxus/WASM PWA with HTTP/SSE control, device pairing or gateway JWT authentication,
  shared session coordination, and optional private R2 attachments.
- A pinned 20-task coding repair suite with independent grading, plus a Harbor adapter for
  native unattended CLI runs in Docker task environments.
- Linux/macOS/Windows CI, WASM and browser checks, and a tag-triggered workflow that prepares
  release archives, SHA-256 files, and a GitHub Release draft.

- `--base-url`, `OPENAI_BASE_URL`, explicit keyless local connections, optional model
  discovery, and `doctor` configuration/connection diagnostics without generation calls.
- Shell and PowerShell installers with version selection, SHA-256 verification, user
  installation paths, and upgrade support; public website at `orch.pandaailabs.com`.
- Native Windows x64 packaging, PowerShell shell selection, and process-tree supervision.
  Native shell commands use exact Host approval; unsupported sandbox requests fail before launch.

### Reliability fixes

- Retry transient failures before model content arrives, retaining reported usage from
  usage-only retry attempts while respecting strict cumulative budget constraints.
- Preserve recent tool outcomes and process state during compaction, including results
  that would otherwise disappear behind long output or repeated inspection.
- Isolate Run temporary storage, aggregate process waits, yield to Steer, and finish process
  cleanup before completion or cancellation. Harbor aborts verification when timeout cleanup
  cannot be confirmed.
- Honor container command settings and proxy environment variables; account for Gemini
  thinking and server-side tool token usage.
- Reconcile native and controlled session history using stable identities and causal anchors;
  avoid duplicate image messages and stale or misrouted approval requests.

### Current limits

- Single-Agent foundation; goal compilation, task brokering, and multi-Agent scheduling
  remain outside the runtime scope.
- `DeliveryCommitted` records an output delivery. Independent task verification remains a
  separate concern. Integration tests and small benchmark subsets are not coding quality scores.
- Live model/native Agent checks are opt-in. TUI cross-terminal manual acceptance and full
  end-to-end performance characterization are still incomplete.
- Release archives target Linux x86_64 (glibc 2.35+), macOS Apple Silicon/Intel, and Windows x64.
  Native Windows has no filesystem/network sandbox; use WSL for sandboxed commands.
  Native Windows MCP connections use Streamable HTTP; local stdio MCP requires WSL.
  macOS archives are not Developer ID notarized; Windows binaries are not Authenticode signed. This workflow does not publish to crates.io.

## [0.2.0]

Previous workflow-first release tag. See the repository history for its implementation.
