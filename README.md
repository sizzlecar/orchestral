<p align="center"><img src="assets/brand/logo-mark.svg" alt="Orchestral" width="80"></p>

# Orchestral

**A runtime for reliable, interactive AI agents.**

Agents are the new processes. Orchestral is the runtime.

[Website](https://orch.pandaailabs.com) · [中文](README.zh-CN.md)

## What it does

- Read code, edit files, and run commands.
- Answer questions, approve actions, steer, or cancel as it works.
- Resume recorded conversations and unfinished work.
- Use it in your terminal, control it from a browser, or embed it in Rust.

## Install

Build from source while the first release is being prepared:

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
