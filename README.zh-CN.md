<p align="center"><img src="assets/brand/logo-mark.svg" alt="Orchestral" width="80"></p>

# Orchestral

**A runtime for reliable, interactive AI agents.**

Agents are the new processes. Orchestral is the runtime.

[官网](https://orch.pandaailabs.com) · [English](README.md)

## 看它如何工作

一个本地模型，三个 Orchestral 终端同时阅读代码、修复问题并运行测试。

[![观看 Ferrum + Orchestral 演示](https://ferrum-downloads.pandaailabs.com/v0.3.1/ferrum-orch-three-agents.png)](https://ferrum-downloads.pandaailabs.com/v0.3.1/ferrum-orch-three-agents.mp4)

[观看英文演示](https://ferrum-downloads.pandaailabs.com/v0.3.1/ferrum-orch-three-agents.mp4) · 50 秒 · 8 倍速。

<details>
<summary><strong>快速试用</strong></summary>

Ferrum 只是这个示例可选的本地模型服务，不是 Orchestral 的依赖。
Orchestral 也可以连接其他 OpenAI 兼容服务。

在 macOS（Apple Silicon）或 Linux（x86_64）上安装最新版本：

```sh
curl -fsSL https://ferrum.pandaailabs.com/install.sh | sh
curl -fsSL https://orch.pandaailabs.com/install.sh | sh
```

Windows（x86_64）使用 PowerShell：

```powershell
irm https://ferrum.pandaailabs.com/install.ps1 | iex
irm https://orch.pandaailabs.com/install.ps1 | iex
```

安装后打开新终端，启动 Ferrum：

```sh
ferrum serve --model unsloth/Qwen3.5-9B-GGUF
```

Ferrum 会自动选择可用的后端，按需下载模型和元数据，后续启动复用缓存。
等待服务就绪后，在另一个终端打开你的项目目录，运行：

```sh
orchestral --base-url http://127.0.0.1:8000/v1 --no-auth
```

输入任务并按 Enter 即可开始。这些默认参数用于快速试用，不代表复现录像中的三会话配置或性能。

</details>

<details>
<summary><strong>高级：复现录像配置</strong></summary>

视频使用 **M1 Max、32 GB 统一内存的 Mac**，通过 Metal 运行 **Qwen3.5-9B Q4_K_M**。
以下命令采用相同的服务参数：每个上下文 24,576 token、三个活跃序列、20 GiB 运行时内存预算，
并保留模型默认的思考行为。使用 **Ferrum 0.10.0** 和 **Orchestral 0.3.1**，无需 JSON 配置或 API key。

先安装两个程序，再打开四个终端格子：

```sh
curl -fsSL https://ferrum.pandaailabs.com/install.sh | sh -s -- --version 0.10.0 --backend metal
curl -fsSL https://orch.pandaailabs.com/install.sh | sh -s -- --version 0.3.1
export PATH="$HOME/.local/bin:$PATH"
ferrum --version
orchestral --version
```

**终端 1（左上）：启动 Ferrum。** 首次启动会从 Hugging Face 下载指定的 GGUF 和模型、分词器元数据，
以后启动复用缓存。仓库版本和文件名对应视频使用的模型权重。

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

保持 Ferrum 运行。在另一个终端检查服务是否就绪，再启动 Agent。
这条命令会发现服务中的模型，不会生成回答：

```sh
orchestral --base-url http://127.0.0.1:8001/v1 --no-auth doctor --check-connection
```

**终端 2（右上）：** 将路径替换为你的第一个项目目录。

```sh
cd /path/to/project-a
orchestral --base-url http://127.0.0.1:8001/v1 --no-auth
```

**终端 3（左下）：** 打开第二个项目。

```sh
cd /path/to/project-b
orchestral --base-url http://127.0.0.1:8001/v1 --no-auth
```

**终端 4（右下）：** 打开第三个项目。

```sh
cd /path/to/project-c
orchestral --base-url http://127.0.0.1:8001/v1 --no-auth
```

在每个 Orchestral 终端输入任务并按 Enter，三个会话共用同一个 Ferrum 服务。
视频使用三个独立的 Rust 项目，机器上已安装 Cargo；每个 Agent 的任务都是修复失败测试、
保留公开接口、运行 `cargo test`，并用英文解释修改。

</details>

## 能做什么

- 阅读代码、修改文件、执行命令。
- 工作中回答问题、审批操作、调整方向或取消。
- 恢复会话，继续未完成的工作。
- 在终端使用，通过浏览器控制，或嵌入 Rust 应用。

## 安装

在 macOS 或 Linux 上安装最新版本：

```sh
curl -fsSL https://orch.pandaailabs.com/install.sh | sh
```

安装后打开新终端，也可以使用 Homebrew：

```sh
brew install sizzlecar/orchestral/orchestral
```

Windows 使用 PowerShell：

```powershell
irm https://orch.pandaailabs.com/install.ps1 | iex
```

可以在 [Releases](https://github.com/sizzlecar/orchestral/releases) 下载发布包，或从源码构建：

```sh
git clone https://github.com/sizzlecar/orchestral.git
cd orchestral
cargo install --locked --path apps/orchestral-cli
```

## 启动

在项目目录中运行：

```sh
export OPENAI_API_KEY="your-api-key"
orchestral
```

也可以直接交给它一个任务：

```sh
orchestral "定位并修复这个问题，运行相关测试。"
orchestral resume --last
```

需要浏览器访问时，运行 `orchestral serve --pair`。

## 配置

模型、MCP 工具和 skills 见[配置示例](configs/orchestral.cli.yaml)。
Rust 集成见 [SDK](core/orchestral)，完整命令见 `orchestral --help`。
