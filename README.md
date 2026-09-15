<p align="center"><img src="assets/brand/logo-mark.svg" alt="Orchestral" width="80"></p>

# Orchestral

**A runtime for reliable, interactive AI agents.**

Agents are the new processes. Orchestral is the runtime.

[Website](https://orch.pandaailabs.com) · [中文](README.zh-CN.md)

## See it in action

One local model. Three Orchestral terminals inspecting code, fixing bugs, and
running tests concurrently.

[![Watch the Ferrum + Orchestral demo](https://ferrum-downloads.pandaailabs.com/v0.3.1/ferrum-orch-three-agents.png)](https://ferrum-downloads.pandaailabs.com/v0.3.1/ferrum-orch-three-agents.mp4)

[Watch the English demo](https://ferrum-downloads.pandaailabs.com/v0.3.1/ferrum-orch-three-agents.mp4) · 50 seconds · 8× speed.

<details>
<summary><strong>Try it locally</strong></summary>

Ferrum is an optional local model server for this example, not an Orchestral
dependency. Orchestral also connects to other OpenAI-compatible providers.

On macOS (Apple Silicon) or Linux (x86_64), install the latest releases:

```sh
curl -fsSL https://ferrum.pandaailabs.com/install.sh | sh
curl -fsSL https://orch.pandaailabs.com/install.sh | sh
```

On Windows (x86_64), use PowerShell:

```powershell
irm https://ferrum.pandaailabs.com/install.ps1 | iex
irm https://orch.pandaailabs.com/install.ps1 | iex
```

Open a new terminal after installation, then start Ferrum:

```sh
ferrum serve --model unsloth/Qwen3.5-9B-GGUF
```

Ferrum automatically selects an available backend and downloads the model and
metadata as needed, reusing its cache on later starts. Wait for the server to be
ready, then open another terminal in your project directory and run:

```sh
orchestral --base-url http://127.0.0.1:8000/v1 --no-auth
```

Type a task and press Enter. These defaults are a quick starting point, not a
reproduction of the recording's three-session configuration or performance.

</details>

<details>
<summary><strong>Advanced: reproduce the recording configuration</strong></summary>

The recording uses an **M1 Max Mac with 32 GB unified memory**, Metal, and
**Qwen3.5-9B Q4_K_M**. The commands below reproduce its serving settings:
24,576 tokens per context, three active sequences, a 20 GiB runtime memory budget,
and the model's default thinking behavior. Use **Ferrum 0.10.0** and
**Orchestral 0.3.1**. No JSON configuration or API key is required.

Install both programs once, then open four terminal panes:

```sh
curl -fsSL https://ferrum.pandaailabs.com/install.sh | sh -s -- --version 0.10.0 --backend metal
curl -fsSL https://orch.pandaailabs.com/install.sh | sh -s -- --version 0.3.1
export PATH="$HOME/.local/bin:$PATH"
ferrum --version
orchestral --version
```

**Terminal 1 — upper left: start Ferrum.** The first start downloads the selected
GGUF and its model/tokenizer metadata from Hugging Face; subsequent starts reuse
the cache. The repository revision and filename select the weights used in the video.

```sh
ferrum serve \
  --model unsloth/Qwen3.5-9B-GGUF@3885219b6810b007914f3a7950a8d1b469d598a5 \
  --gguf-file Qwen3.5-9B-Q4_K_M.gguf \
  --served-model-name Qwen3.5-9B \
  --backend metal \
  --numerical-profile qwen3_5.f32-master \
  --host 127.0.0.1 --port 8001 \
  --max-model-len 24576 \
  --max-num-seqs 3 \
  --max-num-batched-tokens 3072 \
  --scheduler-prefill-step-chunk 1024 \
  --scheduler-active-decode-prefill-chunk 256 \
  --enable-prefix-cache \
  --runtime-memory-budget-bytes 21474836480 \
  --prefix-rendezvous-max-wait-ms 180000
```

Leave Ferrum running. In another terminal, check that it is ready before starting
the agents. This discovers the served model without generating a response:

```sh
orchestral --base-url http://127.0.0.1:8001/v1 --no-auth doctor --check-connection
```

**Terminal 2 — upper right:** replace the path with your first project directory.

```sh
cd /path/to/project-a
orchestral --base-url http://127.0.0.1:8001/v1 --no-auth
```

**Terminal 3 — lower left:** open your second project.

```sh
cd /path/to/project-b
orchestral --base-url http://127.0.0.1:8001/v1 --no-auth
```

**Terminal 4 — lower right:** open your third project.

```sh
cd /path/to/project-c
orchestral --base-url http://127.0.0.1:8001/v1 --no-auth
```

Type a task in each Orchestral terminal and press Enter. Each session uses the
same Ferrum server. The video uses three separate Rust projects with Cargo
installed, and asks each agent to fix failing tests, preserve the public API,
run `cargo test`, and explain the fix in English.

</details>

## What it does

- Read code, edit files, and run commands.
- Answer questions, approve actions, steer, or cancel as it works.
- Resume recorded conversations and unfinished work.
- Use it in your terminal, control it from a browser, or embed it in Rust.

## Install

Install the latest release on macOS or Linux:

```sh
curl -fsSL https://orch.pandaailabs.com/install.sh | sh
```

Open a new terminal after installation. Homebrew is also available:

```sh
brew install sizzlecar/orchestral/orchestral
```

On Windows, use PowerShell:

```powershell
irm https://orch.pandaailabs.com/install.ps1 | iex
```

See [Releases](https://github.com/sizzlecar/orchestral/releases) for downloadable
archives, or build from source:

```sh
git clone https://github.com/sizzlecar/orchestral.git
cd orchestral
cargo install --locked --path apps/orchestral-cli
```

## Start

In your project directory:

```sh
export OPENAI_API_KEY="your-api-key"
orchestral
```

Or run a task directly:

```sh
orchestral "Find the bug, fix it, and run the relevant tests."
orchestral resume --last
```

Use `orchestral serve --pair` for browser access.

## Configure

Connect models, MCP tools, and skills through the [configuration](configs/orchestral.cli.yaml).
For Rust integration, use the [SDK](core/orchestral). Run `orchestral --help` for commands.
