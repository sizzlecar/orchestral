# Orchestral

一个 AI 中立、可安全执行、可持久恢复、可交互的单 Agent 运行时。

[English Version](./README.md)

> 当前状态：Agent Foundation 仍在持续实现。本阶段只完成完整的单 Agent 协议与运行时，
> 不包含 Goal Compiler、Task Broker 或多 Agent 编排。

## 当前已经具备什么

- **Agent Protocol v1**：版本化 Run/Session 合同、Command、持久事件、Inspect、Cancel、
  Recovery，以及唯一终态投影。
- **Generic Agent**：CLI、SDK、API 共用同一套 AI 中立的
  `Model → Tool/Workflow → Model` 循环。
- **模型适配器**：OpenAI-compatible 与 Gemini Native 统一实现
  [`ModelBackend` 合同](testing/orchestral-model-protocol-testkit/README.md)并通过同一
  conformance suite。
- **Guarded Tool Runtime**：Host 持有权限策略、审批 capability、取消、Effect Journal、
  Artifact spill，并对 `UnknownEffect` 保守停机。
- **两套独立扩展面**：Skill 只把受信任指令加入 Context；MCP Tool 只进入 Action Plane，
  且必须经过统一 Guarded Runtime。
- **可选 Workflow 策略**：复杂调用复用类型化 Plan Normalizer、DAG 和 Executor；Workflow
  从属于 Agent Run，不能产生第二个顶层终态。
- **持久上下文**：Run、Session、Tool Effect、Generic Agent checkpoint 都可使用文件插件，
  并支持进程替换后的恢复。

```text
CLI / SDK / API
      │
      ▼
AgentController ── Agent Protocol + 持久 Run Journal
      │
      ▼
Generic Agent ─── ModelBackend + 持久 Session Context
      │
      ├── 直接 Tool ────────────────┐
      └── Workflow → Plan/DAG/Step ─┤
                                    ▼
                         GuardedToolRuntime
                           ├── 内置 Tools
                           └── MCP stdio Tools
```

## 快速开始

导出任意一个已配置模型的密钥：

```bash
export OPENAI_API_KEY="..."
# 或 GOOGLE_API_KEY / OPENROUTER_API_KEY / DEEPSEEK_API_KEY
```

执行单轮任务：

```bash
cargo run -p orchestral-cli -- agent "总结这个仓库的公共 API"
```

进入交互式 Agent Session：

```bash
cargo run -p orchestral-cli -- agent
```

CLI 默认发现 `configs/orchestral.cli.yaml`。可以用 `--config`、`--backend`、
`--model-profile` 或 `--model` 显式选择；`--session-id` 为多轮对话提供稳定、持久的
Session 身份；`--no-mcp` 和 `--no-skills` 可分别关闭两套扩展面。

默认 Host 策略使用显式进程白名单，不把宿主环境变量自动传给 Tool，并关闭网络访问；
模型可见参数无法扩大这些权限。

## SDK

公共 SDK 就是 Agent 控制面：`AgentClient` 启动 Run，`AgentRunHandle` 提供事件订阅、
Inspect、Command、输入恢复、Steer、Cancel 和终态等待，不再暴露旧 Planner Loop。

运行完整的 AI 中立示例：

```bash
cargo run -p orchestral-examples --example agent_session
```

最小组合方式见 [`examples/agent_session.rs`](examples/agent_session.rs)：它把
`ModelBackend`、`InternalGenericAgentProvider`、`AgentController` 与 `AgentClient` 连接起来。

## 项目结构

```text
core/orchestral-core      Agent/Model/Tool/Skill/MCP 合同与确定性 Plan/DAG 内核
core/orchestral-runtime   Agent 控制面、Generic Agent、Context、Guarded Tool、Workflow 桥接
core/orchestral           对外 re-export core/runtime 公共 API 的 facade
plugins/                  文件 Journal/Blob Store 与具体模型 Adapter
apps/orchestral-cli       对话式 CLI composition root
examples/                 可运行的 Agent Session 示例
testing/                  协议一致性与属性测试 harness
```

具体基础设施实现放在 `plugins/`，由应用层 composition root 装配；core/runtime 只依赖合同。

## 开发

```bash
cargo build --workspace
cargo test --workspace --all-targets
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## 当前边界

- 当前不是 Goal Compiler、Task Broker 或多 Agent Scheduler。
- `DeliveryCommitted` 只表示 Agent 已交付输出，不代表外部目标已经被独立满足或验证。
- MCP stdio Tool 已走统一 Guarded Action 路径；Streamable HTTP 与剩余量化安全/恢复 gate
  尚未达到发布完成标准。
- 类型化 Plan/DAG 是单 Agent 内部的可选执行策略，不是产品顶层入口。

## 许可证

见 [LICENSE](./LICENSE)。
