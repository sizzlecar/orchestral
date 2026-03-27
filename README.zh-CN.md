# Orchestral

面向 grounded agent 的流程编排运行时。

[English Version](./README.md)

## 它能做什么

- 重点是有状态的流程编排，不是单次 tool calling
- 用 `agent loop` + `mini-DAG` 执行 typed actions
- 基于真实状态重规划，并在结束前校验结果

## 看它怎么跑

一条用户指令，Orchestral 协调 MCP 数据源、领域 Skill 和 shell 执行，串成多步管道：

```
用户: "从 API 查询 Q4 销售数据，用实际数字和公式填充 Excel 模板，
       再写一份 markdown 摘要对比预算。"

Orchestral 自动执行:
  ├─ mcp__sales-api__query_sales_data  → 从外部 API 获取实际销售额
  ├─ file_read budget.yaml             → 读取预算目标
  ├─ shell (venv python + openpyxl)    → 填充 Excel：数值、公式、状态
  └─ file_write report.md              → 生成对比报告
```

Planner 在启动时通过 `tool_lookup` 自动发现 MCP tools，激活 `xlsx` skill 获取 openpyxl 操作指引，并使用 skill 的虚拟环境运行 Python。当某一步失败时，agent loop 观察错误并重新规划 — 无需人工介入。

**试一下：**

```bash
export OPENROUTER_API_KEY="sk-or-..."
cargo build -p orchestral-cli
cargo run -p orchestral-cli -- scenario \
  --spec configs/scenarios/sales_report_pipeline.smoke.yaml
```

## 架构

```
Intent → Planner (LLM) → Normalizer (DAG 校验) → Executor (并行 + 重试)
            ↑                                              ↓
            └──── agent loop: 观察执行结果 ────────────────┘
```

- **Agent loop** — planner 最多迭代 6 轮，观察结果后重新规划
- **MCP 集成** — 启动时探测 server，每个 tool 注册为独立 action，通过 `tool_lookup` 延迟加载 schema
- **Skill 系统** — SKILL.md 自动发现并注入 planner 上下文；`skill_activate` 支持按需加载
- **类型化 action** — 文档 inspect/patch/verify、结构化配置 patch/verify、shell、文件读写、HTTP

## SDK

把 Orchestral 当库用 — 注册自定义 action 和生命周期钩子：

```rust
use orchestral::{Orchestral, core::action::*};

let app = Orchestral::builder()
    .action(MyCustomAction::new())
    .hook(MyLoggingHook::new())
    .planner_backend("openrouter")
    .planner_model("anthropic/claude-sonnet-4.5")
    .build()
    .await?;

let result = app.run("分析数据并生成报告").await?;
println!("{}", result.message);
```

参考 [`examples/sdk_quickstart.rs`](examples/sdk_quickstart.rs) 和 [`examples/sdk_hooks.rs`](examples/sdk_hooks.rs)。

## 快速开始

Rust stable (1.91.0+)，导出一个模型密钥：

```bash
export OPENROUTER_API_KEY="sk-or-..."  # 或 OPENAI_API_KEY、ANTHROPIC_API_KEY 等
```

```bash
cargo build -p orchestral-cli
cargo run -p orchestral-cli -- run
```

## 项目结构

```
core/orchestral-core     — 纯抽象：Intent/Plan/Step、trait 定义、DAG 执行器
core/orchestral-runtime  — LLM planner、action 实现、MCP 桥接、skill 系统
core/orchestral          — 对外门面，re-export core + runtime
apps/orchestral-cli      — CLI + TUI (ratatui)
apps/orchestral-telegram — Telegram 机器人适配器
```

## Telegram 机器人

把 Orchestral 作为 Telegram 机器人运行 — 每条消息都经过完整的编排管道：

```bash
export TELEGRAM_BOT_TOKEN="你的 bot token"
export GOOGLE_API_KEY="你的 key"  # 或 OPENAI_API_KEY、ANTHROPIC_API_KEY 等
cargo run -p orchestral-telegram
```

LLM 后端可在代码中配置（默认 Google Gemini）。支持文件操作、shell 命令和多轮对话。

## 当前状态

- 核心编排循环已跑通：agent loop + mini-DAG
- MCP per-tool 注册 + 延迟 schema 加载
- Skill 自动发现 + 按需激活
- SDK：builder API + 生命周期钩子 + 程序化执行
- Telegram 机器人适配器（可配置 LLM 后端）
- 文档和结构化配置的类型化管道
- 场景 smoke 测试覆盖核心工作流

## 许可证

见 [LICENSE](./LICENSE)。
