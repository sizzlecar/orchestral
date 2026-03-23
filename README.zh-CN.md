# Orchestral

Orchestral 是面向 grounded agent 的流程编排运行时。

[English Version](./README.md)

## 它能做什么

- 重点是有状态的流程编排，不是单次 tool calling
- 用 `agent loop` + `mini-DAG` 执行 typed actions
- 基于真实状态重规划，并在结束前校验结果

## 快速开始

- Rust stable
- 导出一个可用的模型密钥，例如 `OPENAI_API_KEY`、`GOOGLE_API_KEY`、`ANTHROPIC_API_KEY`、`OPENROUTER_API_KEY`

```bash
cargo build -p orchestral-cli
```

```bash
cargo run -p orchestral-cli -- run
```

## 当前项目情况

- 现在是可用的 pre-1.0 CLI
- 核心编排循环已跑通
- typed actions 和 scenario smoke 已具备
- MCP、skills 和发布收敛仍在推进

## 下一步计划

- 扩大 action、MCP 和 skill 覆盖
- 继续收紧 action 和 runtime contract
- 稳定默认行为并完成发布清理

## 许可证

见 [LICENSE](./LICENSE)。
