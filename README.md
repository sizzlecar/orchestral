# Orchestral

A coding agent for your terminal and workspace. Read code, make changes, and continue the conversation with cloud models or your own OpenAI-compatible server.

Use the terminal UI, run a headless command, control a host from your browser, or embed the Rust SDK.

[Website](https://orch.pandaailabs.com) · [中文版本](./README.zh-CN.md)

> The 0.3 series is a pre-1.0 Agent Foundation release. Public APIs and recovery identities
> can change between releases. Upgrading from 0.2 requires changes to CLI invocations,
> configuration, and SDK integrations; see the [release notes](CHANGELOG.md).

[Install](#install) · [Quick start](#quick-start) · [Mobile control](#mobile-control-pwa) ·
[SDK](#sdk) · [Evaluation](#coding-task-evaluation) · [Release process](RELEASING.md)

## Install

**v0.3.0 is in preparation.** This checkout contains the new installation and local API
features. Binary installers become usable once the first public release is published.
Until then, build from this checkout:

```sh
cargo install --locked --path apps/orchestral-cli
orchestral --version
```

The repository pins Rust 1.91.0. The executable is named `orchestral`; the web client is
embedded, so normal builds do not require Node.js or Dioxus CLI.

Once a release is available, use the installer for your platform:

**macOS / Linux**

```sh
curl -fsSL https://orch.pandaailabs.com/install.sh | sh
```

**Windows PowerShell**

```powershell
irm https://orch.pandaailabs.com/install.ps1 | iex
```

The installers verify SHA-256 checksums, install into a user directory, and configure PATH.
Run the same installer again to upgrade; your configuration and conversations are preserved.
See [installation options, rollback and uninstall](RELEASING.md#installers).

| Platform | Release archive | Command execution |
| --- | --- | --- |
| macOS Apple Silicon / Intel | `.tar.gz` | Native Seatbelt sandbox |
| Linux x64, glibc 2.35+ | `.tar.gz` | Requires `bubblewrap` and unprivileged namespaces |
| Windows x64 | `.zip` | Native commands require explicit approval; use WSL for sandboxed commands |

On native Windows, use Streamable HTTP for MCP servers. Local stdio MCP requires WSL
because its process sandbox is not implemented on native Windows.

On Ubuntu, install the sandbox backend with `sudo apt-get install bubblewrap`.
For manual or offline installation, download the archive and matching `.sha256` file from
[GitHub Releases](https://github.com/sizzlecar/orchestral/releases). Platform validation is
tracked in the [release checklist](docs/product/user-onboarding-release-v0.3.0.md).

## Quick start

Open a terminal in your project directory. For an OpenAI cloud model, set your key:

```bash
export OPENAI_API_KEY="your-api-key"
```

In PowerShell, use `$env:OPENAI_API_KEY="your-api-key"`.

Run one turn:

```bash
orchestral "Summarize the public API of this repository"
```

Start a full-screen interactive Agent Session:

```bash
orchestral
```

### Your own OpenAI-compatible server

No YAML or placeholder key is needed for a server without authentication:

```sh
orchestral --base-url http://127.0.0.1:8000/v1
```

A server root, `/v1` base, or full `/chat/completions` endpoint is accepted. If the server
lists exactly one model, Orchestral selects it. Otherwise choose a model explicitly:

```sh
orchestral --base-url http://127.0.0.1:8000/v1 --model your-model-id
```

`OPENAI_BASE_URL` and `OPENAI_MODEL` provide the same shortcut through environment variables.
Explicit command-line values take precedence. An explicit `--backend` or `--model-profile`
takes precedence over the URL environment variable.

Custom URLs default to **no authentication** and do not inherit your cloud key. For an
authenticated local service or gateway, select its credential environment variable:

```sh
orchestral --base-url https://your-gateway.example/v1 --api-key-env LOCAL_MODEL_API_KEY --model your-model-id
```

Set `LOCAL_MODEL_API_KEY` in your environment first. Orchestral sends it only when explicitly
selected. In YAML, use a provider `endpoint` with `config: { auth: none }` for a keyless service;
the default authentication mode is `api_key`. The model needs tool-calling support to perform
coding actions; OpenAI-compatible HTTP alone does not guarantee a model has that capability.

Check configuration without starting a task, or query the model list without generating text:

```sh
orchestral doctor
orchestral --base-url http://127.0.0.1:8000/v1 doctor --check-connection
```

`doctor --json` prints a report with credential values omitted. Plain `doctor` does not make
network requests or create configuration files. Reuse the same connection options when
starting a conversation.

### More providers and everyday use

Select another provider explicitly when needed:

```bash
export GOOGLE_API_KEY="..."
orchestral --model-profile gemini-2.5-flash "Inspect this workspace"
```

From a source checkout, `cargo run --locked -p orchestral-cli -- "Inspect this workspace"`
is equivalent to invoking the installed executable.

The root command is the Agent entry point; there is no `agent` subcommand. Entry mode is
deterministic:

| Invocation | Mode |
| --- | --- |
| `orchestral` with terminal stdin and stdout | Multi-turn TUI |
| `orchestral "fix the bug"` | One-turn Headless |
| `printf 'fix the bug' \| orchestral` | One-turn Headless |

Headless stdout contains only the final Delivery, so it is safe to pipe into another command;
progress and errors use stderr. In the TUI, Enter sends, steers, or answers the current question.
Ctrl+J inserts a newline (Shift+Enter also works in supporting terminals). Up/Down edits lines,
then navigates session input history while preserving your draft. Paste supports CJK, combining
characters, and emoji; input over 20 lines shows a bounded preview with Ctrl+P to expand.

F1 or Ctrl+] opens commands without replacing your draft. `/` discovers commands; `//` sends a
literal leading slash. `@` completes workspace paths, with ignored/build directories excluded;
selecting a path does not read its contents. Candidates refresh in the background while the
file menu is open, including newly created and renamed files. `/model`, `/new`, and `/resume` switch configured
models or sessions while idle; session drafts survive switching within the current process.
The `/resume` panel includes “Current session details” for storage and reported usage.
`/context` scopes skill load records to the current or latest request and shows instruction
sources loaded for the process and session compaction records. Unknown context usage remains `—`.

Ctrl+O expands tool records in the conversation; PgUp/PgDn reads history or the focused panel,
and End follows new output. Native terminal text selection is retained; `/copy` copies the last
committed answer when a local clipboard utility is available. `/`, F1 and `/help` expose the
same action menu, including keyboard shortcuts and appearance settings; `NO_COLOR` disables styling.
`/skills` searches discovered workspace skills. Enter opens full descriptions and sources; Space
toggles enablement preferences, with pending restart changes distinguished from the current process.
Browsing skills neither loads model instructions nor adds conversation entries.

Esc/Ctrl+C closes the focused panel first, otherwise interrupts an active Run. While idle,
Ctrl+C clears a draft and Ctrl+D exits with empty input; `/quit` stops work and exits. Approvals
require `a`/`d` or an explicit arrow selection before Enter. `replied` means output was delivered;
it does not independently verify the user's goal. Waiting questions accept literal `/` answers;
use F1 to access commands without submitting an answer. Submitted answers and approvals wait
for confirmation before another response is allowed. When a question ends, its previous draft
is restored; edit it or move the cursor before sending so an extra Enter cannot send it accidentally.

The CLI discovers `.orchestral/config.yaml`, `.orchestral/config.yml`,
`configs/orchestral.cli.yaml`, then `orchestral.yaml`; if none exists it creates
`.orchestral/generated/default.agent.yaml`. Use `--config`, `--backend`, `--model-profile`, or `--model` for
explicit selection. For example:

```bash
orchestral --backend deepseek --model deepseek-chat "inspect this crate"
orchestral --backend google --model gemini-2.5-flash "inspect this crate"
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

Find and resume built-in Agent conversations:

```bash
orchestral sessions list                         # Current workspace, most recent first
orchestral sessions list --search parser         # Search titles or Session IDs
orchestral sessions list --all --limit 20 --json  # All workspaces, including legacy sessions
orchestral sessions show SESSION_ID              # Original conversation and Tool results
orchestral resume SESSION_ID                     # Restore history in the interactive terminal
orchestral resume --last                         # Most recent session in this workspace
orchestral resume --last "Continue verification"  # Headless follow-up; stdin pipes also work
```

`sessions list/show` default to the built-in Agent; select external Codex history
with `--connector codex`. Lists support `--cursor` pagination. Browsing needs no
model credentials and starts no model, Tool, or recovery execution. Discovery is
rebuilt from Run/Session journals. Context compaction does not replace the original
TUI transcript; Tool results have bounded display excerpts, with full records
available through `sessions show SESSION_ID --json`.

New Runs record workspace and model provenance. `--last` selects only the current
workspace; resuming another workspace by ID reports the required `-C`. Provenance
never grants Tool permissions. Legacy sessions without metadata remain available
through `--all` and explicit IDs, and are excluded from automatic `--last` selection.
After a completed conversation, new input creates a new Run. Unfinished Runs first
use the Controller's existing checkpoint recovery contract: pending input and
approval remain interactive, unobserved model attempts become `Incomplete`, and
committed Tool effects are not repeated. Uncertain effects remain `UnknownEffect`;
incompatible recovery identities fail explicitly. Each filesystem journal permits
one Host writer, while read-only browsing can run concurrently.

Long conversations retain their original records through repeated compaction.
The compactor follows journal references back to original exchanges, avoiding
repeated summarization of lossy summaries. A follow-up can restore the most
recent compacted user request verbatim within the history/token budget. Bounded
summaries retain typed failure outcomes; successful Tool execution alone is not
evidence that the task has been verified.

The built-in `session_read` Tool lets the Agent search its own original Session
history, then read exact JSON fields or paginated chunks. It cannot select another
Session, and uses the same Host grants, cancellation and effect journal as other
Tools. See [Session Context and Recall](docs/agent-foundation/session-context-v1.md)
for snapshot cursors, SDK registration and recovery compatibility.

The built-in Agent reuses existing project instructions. For `-C` and each
`--add-dir`, it discovers documents along the path from the nearest Git root to
the selected directory. Git worktree `.git` files are supported; outside Git,
only the selected directory is checked. Each directory contributes its first
nonempty file in this order: `AGENTS.override.md`, `AGENTS.md`, `CLAUDE.md`.
Ancestor instructions precede more specific ones; shared sources are deduplicated
across overlapping workspaces. Every document retains its source and directory
scope. Unrelated descendants and other agents' global profiles are not imported
automatically; select additional directories with `--add-dir`.

Instructions are a Host-startup snapshot, retained through compaction and model
retries. Restart the Host to reload changes. Recovery rejects an old Run if its
instruction snapshot has changed. Instructions cannot grant tool permissions.
Same-directory aliases such as `AGENTS.md -> CLAUDE.md` work; escaping symlinks,
invalid UTF-8, and oversized documents produce explicit errors rather than
silently dropping project rules.

Transient model failures before any text, tool call, or Finish automatically retry within
the current model step, with exponential backoff and at most three retries by
default. CLI/TUI progress shows the wait, which cancellation and Steer can
interrupt. A request that only reported usage can retry; its latest usage snapshot
is retained in checkpoints and added once to the Run's reported usage. Requests
that already produced text, tool calls, or Finish are not automatically reissued;
model retries never repeat previously executed tools.
With explicit cumulative Run token/cost limits, only rate-limit rejections without
usage retry, since failed requests may have consumed more than their last reported usage.
Uncertain model attempts after process loss retain the existing recovery contract.

Configure discovery, compatibility filenames, and retries as follows:

```yaml
agent:
  project_instructions:
    enabled: true
    max_bytes: 65536
    fallback_filenames: [CLAUDE.md, TEAM_GUIDE.md]
  model_retry:
    max_retries: 3 # 0 disables automatic retries
    base_delay_ms: 500
    max_delay_ms: 8000
```

Credential-free CLI/PTY E2E tests cover instruction precedence, scope, snapshots,
retries, cancellation, Steer, and preservation of tool effects:

```bash
cargo test -p orchestral-cli --test agent_live_e2e
```

The opt-in live coding test checks both existing instruction filenames using
Vertex credentials and spends real model quota:

```bash
cargo test -p orchestral-cli --test agent_live_e2e live_agent_uses_existing_project_instructions_for_coding -- --ignored --test-threads=1
```

## Mobile control PWA

`orchestral serve` starts the same Agent Host used by the TUI and serves an embedded,
installable mobile web app. The phone is a control client—not a second Agent runtime—so model,
Skill, MCP, workspace policy, approvals, and journals remain on the Host.

For local browser development:

```bash
orchestral serve --pair --backend google --model gemini-2.5-flash -C /path/to/workspace
```

For a phone, terminate HTTPS with a trusted reverse proxy or private-network relay and tell the
Host the browser-visible URL:

```bash
orchestral serve --pair \
  --public-url https://agent.example.com \
  --backend google --model gemini-2.5-flash \
  -C /path/to/workspace
```

Scan the printed QR code. Its fragment contains a one-time, short-lived pairing secret; after the
claim, the browser retains a device credential and the Host stores only its digest. The PWA can
start and continue Sessions, stream durable Run events with cursor-based reconnect, show bounded
Tool/file evidence and progress, resolve input and approval requests, steer or cancel a Run, and
revoke paired devices. API responses, credentials, and transcripts are excluded from the service
worker cache.

For an identity-aware reverse proxy, the Host can instead require and verify a signed RS256 JWT.
This mode does not use browser device pairing: the PWA restores the proxy session from its secure
cookie, while the Host independently verifies the assertion signature, issuer, audience, expiry,
and configured identity claims on every API request.

```bash
orchestral serve \
  --public-url https://agent.example.com \
  --access-jwt-issuer https://access.example.com \
  --access-jwt-jwks-url https://access.example.com/.well-known/jwks.json \
  --access-jwt-audience orchestral \
  --access-jwt-header X-Access-JWT \
  --access-jwt-required-claim email=owner@example.com \
  --backend google --model gemini-2.5-flash \
  -C /path/to/workspace
```

The contract is proxy-neutral: header name, issuer, JWKS endpoint, audience, and repeatable
`NAME=VALUE` claim constraints are deployment configuration. Cloudflare Access, oauth2-proxy, or
another gateway can provide the assertion. Once JWT mode is enabled, protected Host routes accept
only a valid gateway assertion; a stale device token cannot bypass the proxy identity policy.
The proxy must strip or overwrite the configured identity header, and the Host origin should stay
private or loopback-only so clients cannot bypass the proxy and inject their own assertion header.

The default listener is loopback-only. A direct trusted-LAN test can opt into cleartext explicitly
with `--listen 0.0.0.0:8765 --public-url http://HOST:8765 --allow-insecure-http`, but browsers
normally require trusted HTTPS for installation, service workers, notifications, and other PWA
features. Orchestral does not silently publish the Host or upload Agent state; the HTTPS proxy or
relay remains an explicit deployment choice. Device and Session metadata defaults to
`~/.config/orchestral/remote-control.json` on Unix.

Minimal coding task:

```bash
orchestral "Repair the failing project in this workspace, run its tests, and report the verified result."
```

The model sees one structured file-mutation tool, `apply_patch`, for Add/Update/Delete. It cannot
choose workspace roots or approval authority. `file_read`, `apply_patch`, `exec_command` /
`write_stdin`, and MCP calls all remain behind Host policy and effect journaling.

On macOS and Linux, `exec_command` launches one Host-resolved shell and may run ordinary child
programs and local toolchains inside the OS sandbox; it does not require a per-program allowlist. The actual boundary
is the Host-approved read/write roots, exact network targets, captured environment, time/output
limits, exact approval, and effect journal. Ambient environment is not inherited wholesale and
network access is disabled by default. MCP stdio launch identities remain explicitly configured by
the Host. Model-visible arguments cannot expand any of these permissions.

Native Windows commands run with the current user's OS permissions after exact Host approval;
they have process supervision and output/time limits, but no filesystem or network isolation.

Command temporary files live outside the workspace in a private Host-managed directory.
`TMPDIR`, `TMP`, and `TEMP` point to one Run-specific child: commands in the same Run share it,
and the sandbox cannot access another Run's child. The directory stays alive while processes
use it and is reclaimed when the Run ends. The CLI keeps the Host root identity stable across
restarts; existing Run directories from another Host or a crash are never silently reused.
SDK Hosts must include `ProcessSupervisor::runtime_temp_root()` in the Host/Run/exec Tool
filesystem read/write grants. File Tools retain their workspace-only restrictions.

Process waits support `wait_mode: "completion"` to collect output until exit or the observation
deadline, and `wait_mode: "output"` to return after a short pause in output. Non-TTY commands
default to completion mode with an initial 10-second window; empty non-TTY `write_stdin` polls
default to 30 seconds. TTY sessions and input writes default to output mode. `yield_time_ms`
overrides the window within the Host execution limit; reaching that window leaves the process
running and returns its session ID. New Steer input can yield either wait without stopping or
replaying the process, so the Agent can consider the instruction alongside the collected output.

With `skills.auto_discover: true`, the CLI discovers `SKILL.md` packages under workspace
`.claude/skills`, `.codex/skills`, and `skills`, plus any explicit `skills.directories`. Only Skill
descriptors enter initial Context; `skill_read` loads the selected instructions, and relative
resources resolve from that Skill's directory. MCP stays separate: `mcp.servers` accepts
Host-configured stdio or Streamable HTTP transports, and discovered MCP methods become guarded,
namespaced Tools rather than prompt text.

Local MCP servers use the same Host registry as remote servers. An explicit `.mcp.json` can be
loaded with `orchestral --mcp-config PATH`; it is never auto-executed merely because a repository
contains one. Orchestral resolves the exact executable, gives it an isolated private HOME, and
limits its cwd, read/write roots, environment, and network independently from generic shell Tools.
Registered launchers may form a process tree by default (for `npx`, `uvx`, or shell wrappers), but
every descendant remains inside that MCP server's sandbox; set `allowChildProcesses: false` for a
single-process server.
The same manifest can be pinned in the main config with `mcp.import_files`:

```yaml
mcp:
  import_files: [.mcp.json]
```

```json
{
  "mcpServers": {
    "local": {
      "type": "stdio",
      "command": "/absolute/path/to/server",
      "args": [],
      "allowChildProcesses": true,
      "cwd": ".",
      "readableRoots": ["/absolute/path/to/server-bundle"],
      "networkTargets": ["localhost:4317"]
    }
  }
}
```

## Architecture

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
see the [web development guide](web/orchestral-web/README.md). The
[release process](RELEASING.md) describes packaging, upgrade checks, and optional live tests.

## Coding task evaluation

For standard terminal-agent tasks, the [Harbor adapter](testing/orchestral-harbor/README.md)
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
[`orchestral.example.json`](testing/orchestral-coding-eval/agents/orchestral.example.json), including
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

## License

See [LICENSE](./LICENSE).
