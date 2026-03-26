# Orchestral

面向 grounded agent 的流程编排运行时 — 不是单次 tool calling，而是能自动获取、转换、校验、恢复的有状态多步管道。

[English Version](./README.md)

## 看它怎么跑

一条用户指令，Orchestral 协调 MCP 数据源、领域 Skill 和 shell 执行，串成多步管道：

```
用户: "从 API 查询 Q4 销售数据，用实际数字和公式填充 Excel 模板，
       再写一份 markdown 摘要对比预算。"

Orchestral (3 轮 agent loop, 5 步):
  ├─ mcp__sales-api__query_sales_data  → 从外部 API 获取实际销售额
  ├─ file_read budget.yaml             → 读取预算目标
  ├─ mcp__sales-api__query_budget_variance → 计算差异
  ├─ shell (venv python + openpyxl)    → 填充 Excel：数值、公式、状态
  └─ file_write report.md              → 生成对比报告
```

Planner 在启动时自动发现 MCP tools（`tool_lookup`），激活 `xlsx` skill 获取 openpyxl 操作指引，并使用 skill 的虚拟环境运行 Python — 全程无需手动配置。

**自己跑一下：**

```bash
export OPENROUTER_API_KEY="sk-or-..."
cargo run -p orchestral-cli -- scenario \
  --spec configs/scenarios/sales_report_pipeline.smoke.yaml \
  --env-file .env.local
```

## 和 Claude Code / Codex 的区别

| 能力 | Claude Code / Codex | Orchestral |
|------|-------------------|------------|
| 执行模型 | 每轮单次 tool 调用 | 多步 DAG，带依赖关系 |
| 错误处理 | 失败即报错 | Agent loop：观察 → 重规划 → 重试 |
| 外部工具 | 内置固定集合 | MCP server 自动发现，per-tool 注册 |
| 领域知识 | 系统提示词 | Skill：自动匹配 + 按需激活 |
| 结果校验 | 无 | 类型化 inspect → patch → verify 管道 |
| 状态 | 无状态 | 跨轮次 checkpoint/resume |

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
```

## 当前状态

- 核心编排循环已跑通：agent loop + mini-DAG
- MCP per-tool 注册 + 延迟 schema 加载
- Skill 自动发现 + 按需激活
- 文档和结构化配置的类型化管道
- 表格操作走 xlsx skill + openpyxl
- 18 个场景 smoke 测试通过

## 许可证

见 [LICENSE](./LICENSE)。
