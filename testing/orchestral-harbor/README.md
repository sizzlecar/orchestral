# Harbor evaluation

This package runs the native Orchestral CLI in Harbor Docker task environments.
Harbor supplies the instruction, task environment, execution deadline, verifier,
and reward. The adapter does not read solutions or implement its own grader.
It requires Python 3.12+, Docker, and a Linux Orchestral executable matching the
task image architecture. Harbor is pinned to 0.22.0.

## Build and install

From the repository root:

```sh
uv sync --project testing/orchestral-harbor
```

Build a static Linux executable outside the task containers. On macOS, for the
amd64 Terminal-Bench images:

```sh
rustup target add x86_64-unknown-linux-musl
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  uv run --project testing/orchestral-harbor \
  --with cargo-zigbuild==0.23.4 --with ziglang==0.14.1 \
  cargo zigbuild --locked -p orchestral-cli --target x86_64-unknown-linux-musl
```

The executable is `target/x86_64-unknown-linux-musl/debug/orchestral`.
An ordinary Linux build also works when its libc and architecture match the task
image. A macOS executable is rejected before installation. The adapter records
the executable SHA-256; retain the source revision alongside the run.

## Validate the environment before spending model quota

Run a small official subset with its reference solution first:

```sh
uv run --project testing/orchestral-harbor harbor run \
  -d terminal-bench@2.0 -i log-summary-date-ranges \
  -a oracle -n 1 -k 1 --max-retries 0 \
  --jobs-dir /tmp/orchestral-harbor-jobs --job-name oracle-preflight
```

Inspect the per-trial `result.json`, verifier logs, and reward. A missing reward,
image pull failure, or verifier exception is an environment failure; fix it
before evaluating the agent. `--install-only` provides a separate CLI installation
check without model calls or verification.

## Run Orchestral

After the oracle passes, run the same task selection with a configured model:

```sh
uv run --project testing/orchestral-harbor harbor run \
  -d terminal-bench@2.0 -i log-summary-date-ranges \
  -a orchestral_harbor:Orchestral -m google/gemini-3.1-pro-preview \
  --ak binary_path=/absolute/path/to/orchestral \
  --ak credential_file=/absolute/path/to/google-service-account.json \
  -n 1 -k 1 --max-retries 0 \
  --jobs-dir /tmp/orchestral-harbor-jobs --job-name orchestral-preflight
```

The credential file is used only for Google service-account authentication and
is copied into the disposable container outside the logs. Alternatively, export
the selected provider's API key (`GOOGLE_API_KEY`, `OPENAI_API_KEY`,
`OPENROUTER_API_KEY`, or `DEEPSEEK_API_KEY`) and omit `credential_file`.
Supported model prefixes are `google/`, `openai/`, `openrouter/`, and `deepseek/`.
For container access through a host proxy, pass a reachable container-side URL
using `--ae HTTPS_PROXY=http://host.docker.internal:PORT`; host loopback addresses
do not refer to the host from inside a container. Standard `HTTP_PROXY`,
`HTTPS_PROXY`, `ALL_PROXY`, and `NO_PROXY` variables, including lowercase forms,
are forwarded to the CLI and its commands. This does not configure Harbor's
image downloads or the verifier's environment.

To use a pinned local checkout of official tasks, replace `-d` with
`-p /absolute/path/to/terminal-bench-2` and keep the same `-i` filter. Record its
commit and Harbor's task/image identities. Do not change the task instructions,
tests, or scoring rules between agents.

## Execution and result contract

- Each trial installs the same binary, preserving the task image's working
  directory. No host repository or user configuration is mounted into the task.
- `agent.input_requests_enabled: false` removes the input-request capability
  and tool. Unsolicited input calls fail without opening a pending request.
  Interactive applications retain the default `true` setting.
- `tools.exec.sandboxed_execution_enabled: false` exposes only explicit approved
  command execution; official images do not need a nested bubblewrap sandbox.
  Each command still supplies an escalation request and justification, and the
  native Host checks an exact approval capability before execution. The adapter
  supplies approval inside the disposable Docker container.
  This adapter accepts Docker environments only. Do not expose host workspace
  mounts or the Docker socket to benchmark containers.
- MCP and skills are disabled. Task-provided MCP/skills are rejected, rather
  than silently omitted. This adapter currently supports single-turn tasks;
  native resume, ATIF conversion, and automatic cost calculation are not exposed.
- Default ceilings are 128 model steps and 512 tool calls, with temperature 0 and
  8192 output tokens per model response. `--ak max_model_steps=...`,
  `--ak max_tool_calls=...`, `--ak max_output_tokens=...`, and `--ak temperature=...`
  override these settings. Reasoning models may consume the output allowance
  before producing visible content; record the allowance alongside their results.
  Harbor retains the task's timeout. Declare identical budgets when comparing
  agents, and report any remaining differences.
- `agent/adapter.json` records the binary digest and generated configuration;
  `stdout.txt`, `stderr.txt`, `journal/`, and `artifacts/` retain native evidence.
  A nonzero CLI exit remains an execution error, independent of verifier reward.
  Harbor also preserves logs when the agent times out.
- Token totals count each committed model request once, even when it emits
  several tool calls. Gemini input includes server-side tool input and output
  includes thinking tokens; cached prompt tokens are not counted twice. Missing
  usage and unknown cost remain unknown. Interrupted, uncommitted provider
  requests are not included in those totals.

Small subsets validate integration; they are not full Terminal-Bench scores.
Separate reference failures, agent execution errors, incorrect solutions, and
successful solutions. Run logs and acceptance records belong outside Git.

## Adapter tests

```sh
uv run --project testing/orchestral-harbor \
  python -m unittest discover -s testing/orchestral-harbor/tests -v
cargo test -p orchestral-runtime --test generic_agent unattended_host_
```
