<p align="center"><img src="assets/brand/logo-mark.svg" alt="Orchestral" width="80"></p>

# Orchestral

**A runtime for reliable, interactive AI agents.**

Agents are the new processes. Orchestral is the runtime.

[官网](https://orch.pandaailabs.com) · [English](README.md)

## 能做什么

- 阅读代码、修改文件、执行命令。
- 工作中回答问题、审批操作、调整方向或取消。
- 恢复会话，继续未完成的工作。
- 在终端使用，通过浏览器控制，或嵌入 Rust 应用。

## 安装

首个版本正在准备发布，目前从源码安装：

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
