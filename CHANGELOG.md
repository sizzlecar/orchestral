# Changelog

## [0.4.1]

### Fixed

- `mcp add` rejects an existing server name without changing its registration.
  Use `--replace` to replace the entire entry; omitted settings reset to defaults
  instead of retaining the previous environment or permissions.

## [0.4.0]

### Model selection and reasoning controls

- `/model` discovers model IDs from the configured service API while preserving its
  endpoint, authentication, and protocol settings. Explicit profile selection remains
  available through `/model profiles`; model and reasoning switches retain session history.
- `/reasoning`, `--reasoning`, and model profiles support provider-defined effort names
  without a client release, preserving their spelling through discovery and requests.
  Missing capability metadata no longer produces an invented menu of supported efforts;
  explicit values can still be submitted for the service to validate.
- Default omits reasoning controls, `none` requests an explicit effort, and `on`/`off`
  control binary thinking. Use `effort:on`, `effort:off`, or `effort:default` for literal
  provider efforts that share those names.
- Native Codex choices follow its model catalog and configured defaults. Unknown effort
  names remain intact, and missing catalog metadata leaves optional controls for native
  validation rather than selecting fixed fallback models or effort levels.
- Web forms can clear optional model and reasoning selections back to the provider default
  without weakening required sandbox or approval settings.

### Runtime and Web reliability

- Live session updates and reconciliation continue after the dialog that started them
  closes. A recovered session clears superseded error messages while retaining genuinely
  new failures and the original history.
- Process sessions wait for both stdout and stderr readers to finish before reporting
  completion. Poll deadlines preserve unread output, and session cleanup stops its readers.

### Distribution and compatibility

- Published-release checks exercise public installers and startup of the installed CLI,
  including onboarding against a local fixture service without paid model calls.
- The SDK adds optional `AgentConfig.reasoning` and extensible reasoning preference types.
  Direct `AgentConfig` struct literals need the new field or a `Default` update; existing
  serialized configuration remains valid. Explicit reasoning controls participate in request
  and unfinished-run recovery identity; an omitted control preserves the default identity.
- OpenAI discovery adds `DiscoveredModel.reasoning`. Downstream code constructing this public
  struct directly must supply `reasoning: None` when capabilities are unknown, or provide the
  declared capabilities. Existing model IDs and context-capacity metadata retain their meanings.
- Agent Protocol remains v1. Native Codex validates model/effort combinations; an HTTP effort
  named `ultra` does not imply Codex's native preset or multi-agent behavior.

## [0.3.1]

The v0.3.0 tag remains an unpublished candidate; v0.3.1 includes the changes below.

Pre-1.0 Agent Foundation release, replacing the workflow-first architecture in 0.2.

### Compatibility

- Updated the transitive ChaCha20 dependency to 0.10.2 so the Google Cloud authentication RNG's SSE2 path does not require SSE4.1 instructions on older x86 processors.

### Local startup and interaction

- Independent CLI sessions can run in the same workspace while retaining exclusive control of each conversation and read-only access to legacy history.
- Context-capacity recovery can reduce output reservation without discarding required first-turn instructions; the reduced budget survives checkpoint recovery.
- After an output-only capacity retry succeeds, new tool observations can use the released context space. Input ceilings learned from input reduction and cumulative Run limits remain enforced.
- Accepted Host cancellation takes precedence when a model stream concurrently returns an error, EOF, or completion.
- `doctor` validates model profile options through the same adapter construction as startup.
- `file_edit` supports atomic batches of independent edits to one file.
- OpenAI-compatible tool results default to a single string with typed metadata and verbatim fenced multiline fields, avoiding YAML presentation indentation in source text. Explicit JSON, YAML, and text-part array encodings remain available; encoding changes invalidate unfinished Run recovery identity.
- Generated configuration is published atomically and isolated by content, preventing concurrent terminals from reading partial configuration or overwriting each other's model connection.
- Nested context compaction records the live source intervals it consumed, keeping checkpoint provenance disjoint when newer summaries have replaced older records.
- Model discovery uses declared serving capacity to bound context planning. The TUI distinguishes estimated and reported input tokens, preserves unknown limits, and clears stale recovery and acceptance messages on progress.
- Empty TUI sessions show the Orchestral logo, running version, selected model, and workspace.
- During a run, Enter queues input for the next model call. `/queue` edits or withdraws pending messages; Alt+Enter explicitly interrupts generation to steer. Queue commands and consumption survive journal replay.

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
- A separate distribution workflow for published stable releases, with verified Cargo packages,
  Homebrew formula generation, and public installation checks. See `RELEASING.md` for the
  registry and tap prerequisites; preparing a draft does not publish these channels.

- `--base-url`, `OPENAI_BASE_URL`, explicit keyless local connections, optional model
  discovery, and `doctor` configuration/connection diagnostics without generation calls.
- Shell and PowerShell installers with version selection, SHA-256 verification, user
  installation paths, and upgrade support; public website at `orch.pandaailabs.com`.
- Native Windows x64 packaging, PowerShell shell selection, and process-tree supervision.
  Native shell commands use exact Host approval; unsupported sandbox requests fail before launch.

### Reliability fixes

- Retry transient failures before model content arrives, retaining reported usage from
  usage-only retry attempts while respecting strict cumulative budget constraints.
- Handle structured model context-capacity rejections before content arrives with bounded
  retries and a smaller, durable input budget. Recovery reprojects committed context without
  repeating completed tool effects; `agent.context_recovery.max_retries` controls retries.
- Preserve recent tool outcomes and process state during compaction, including results
  that would otherwise disappear behind long output or repeated inspection.
- Merge live summaries across superseded journal records while retaining original-source
  identity, historical replay, user corrections, and protected effect/artifact boundaries.
- Separate model-visible tool output allowances from execution capture limits. Large collected
  results can be read through `artifact_read` using a reference and byte cursor; the Host resolves
  and verifies their Session-scoped metadata. Complete observed pages preserve file-read version
  evidence. The explicit-metadata Artifact reader remains available to SDK Hosts.
- Preserve failing upstream pipeline status by default with Bash/Zsh `pipefail`. Select
  `tools.exec.pipeline_exit_status: native` when native shell pipeline semantics are intended.
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
- A per-result output allowance does not guarantee that the full model request fits. Recovery
  can still stop when required context cannot fit; process output discarded beyond capture
  limits cannot be reconstructed through Artifact reads.
- Release archives target Linux x86_64 (glibc 2.35+), macOS Apple Silicon/Intel, and Windows x64.
  Native Windows has no filesystem/network sandbox; use WSL for sandboxed commands.
  Native Windows MCP connections use Streamable HTTP; local stdio MCP requires WSL.
  macOS archives are not Developer ID notarized; Windows binaries are not Authenticode signed.

## [0.2.0]

Previous workflow-first release tag. See the repository history for its implementation.
