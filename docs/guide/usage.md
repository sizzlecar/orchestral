# User guide

[Back to README](../../README.md)

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
Tools. See [Session Context and Recall](../../docs/agent-foundation/session-context-v1.md)
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


## Tools and permissions

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
