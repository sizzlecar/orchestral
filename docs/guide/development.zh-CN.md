# 开发指南

[返回 README](../../README.zh-CN.md)

## 架构

- **Agent Protocol v1**：版本化 Run/Session 合同、Command、持久事件、Inspect、Cancel、
  Recovery，以及唯一终态投影。
- **Generic Agent**：CLI、SDK、API 共用同一套 AI 中立的
  `Model → Tool/Workflow → Model` 循环。
- **模型适配器**：OpenAI-compatible 与 Gemini Native 统一实现
  [`ModelBackend` 合同](../../testing/orchestral-model-protocol-testkit/README.md)并通过同一
  conformance suite。
- **Guarded Tool Runtime**：Host 持有权限策略、审批 capability、取消、Effect Journal、
  Artifact spill，并对 `UnknownEffect` 保守停机。
- **两套独立扩展面**：Skill 只把受信任指令加入 Context；MCP Tool 只进入 Action Plane，
  且必须经过统一 Guarded Runtime。
- **可选 Workflow 策略**：复杂调用复用类型化 Plan Normalizer、DAG 和 Executor；Workflow
  从属于 Agent Run，不能产生第二个顶层终态。
- **持久上下文**：Run、Session、Tool Effect、Generic Agent checkpoint 都可使用文件插件，
  并支持进程替换后的恢复。
- **交互客户端**：终端 TUI 和内嵌 Dioxus/WASM 手机 PWA，支持历史会话、提问、审批、
  Steer 和取消。
- **外部 Agent 接入**：应用已装配 Codex connector；SDK Host 可显式注册 ACP 插件。

```text
CLI / SDK / API
      │
      ▼
AgentController ── Agent Protocol + 持久 Run Journal
      │
      ▼
Generic Agent ─── ModelBackend + 持久 Session Context
      │
      ├── 直接 Tool ───────────────────────────┐
      └── 可选注入的 Workflow → Plan/DAG ──────┤
                                    ▼
                         GuardedToolRuntime
                           ├── 内置 Tools
                           └── MCP Tools（stdio / Streamable HTTP）
```

## SDK

公共 SDK 就是 Agent 控制面：`AgentClient` 启动 Run，`AgentRunHandle` 提供事件订阅、
Inspect、Command、输入恢复、Steer、Cancel 和终态等待，不再暴露旧 Planner Loop。

运行完整的 AI 中立示例：

```bash
cargo run -p orchestral-examples --example agent_session
```

最小组合方式见 [`examples/agent_session.rs`](../../examples/agent_session.rs)：它把
`ModelBackend`、`InternalGenericAgentProvider`、`AgentController` 与 `AgentClient` 连接起来。

## 版本化合同

- [Agent Protocol v1](../../docs/agent-foundation/agent-protocol-v1.md)
- [Model Protocol v1](../../docs/agent-foundation/model-protocol-v1.md)
- [Guarded Tool Runtime v1](../../docs/agent-foundation/tool-runtime-v1.md) 与
  [Tool Artifact v1](../../docs/agent-foundation/tool-artifact-v1.md)
- [Skill Runtime v1](../../docs/agent-foundation/skill-runtime-v1.md)
- [MCP Tools Adapter v1](../../docs/agent-foundation/mcp-tools-adapter-v1.md)

## 项目结构

```text
core/orchestral-core      Agent/Model/Tool/Skill/MCP 合同与确定性 Plan/DAG 内核
core/orchestral-runtime   Agent 控制面、Generic Agent、Context、Guarded Tool、Workflow 桥接
core/orchestral           对外 re-export core/runtime 公共 API 的 facade
plugins/                  文件 Journal/Blob Store 与具体模型 Adapter
apps/orchestral-cli       CLI/TUI 装配与 HTTP/SSE Host 网关
web/orchestral-web        Dioxus/WASM PWA 及内嵌构建产物
examples/                 可运行的 Agent Session 示例
testing/                  协议测试、编码任务评测与 Harbor 适配器
```

具体基础设施实现放在 `plugins/`，由应用层 composition root 装配；core/runtime 只依赖合同。

## 开发

```bash
cargo build --locked --workspace
cargo test --locked --workspace --all-targets
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
bash scripts/check_workspace.sh
bash scripts/check_agent_surface.sh
```

CI 覆盖 Linux、macOS、Windows、SDK 文档示例、WASM 目标、重新构建的 PWA Chromium 测试和无需
模型调用的 Harbor 测试。修改 Web 源码后运行 `scripts/build_web.sh`，详见
[Web 开发说明](../../web/orchestral-web/README.md)。打包、升级检查和可选真实模型验证见
[发布流程](../../RELEASING.md)。

## 编码任务评测

[Harbor 适配器](../../testing/orchestral-harbor/README.md) 可在独立 Docker 容器中运行原生 CLI，
使用标准终端任务及其官方验收脚本。先运行参考答案校验环境，再调用模型；小规模任务集
用于验证接入，不代表完整榜单成绩。无人值守的 Host 可设置
`agent.input_requests_enabled: false`，同时关闭输入请求能力和对应工具。

`orchestral-coding-eval` 提供 20 个基于固定版本 Orchestral 源码的可控修复任务。
题目通过注入回归构造，不冒充真实历史 issue 或跨仓库基准。覆盖重试策略、项目规则、
Unicode 编辑、交互、文件修改，以及两项分两次启动 CLI 的续聊任务；本版不覆盖功能开发、
强制压缩或运行中被杀后的恢复。

```bash
cargo run -p orchestral-coding-eval -- list
cargo run -p orchestral-coding-eval -- validate --repo .
```

`validate` 不调用模型：原版须通过、故障版须触发指定测试断言。实际评测在独立副本中使用
固定测试判分，并检查受保护文件、用户未提交内容和未经允许的暂存/提交。零个测试、
编译错误或 Agent 声称完成都不算通过。验证器会在本机执行候选 Rust 代码，因此这套工具
用于正确性评测，不提供恶意代码隔离。

实跑前复制并配置 [调用示例](../../testing/orchestral-coding-eval/agents/orchestral.example.json)，
填写可执行文件、模型和对应凭据；凭据使用模型已有环境变量或显式 `--credential-file`。
`{prompt}`、`{workspace}`、`{config}`、`{session_id}` 按 argv 字面量替换，不经过 shell。
其他 Agent 可以提供自己的调用文件，但这些适配尚未经实测。
生成的 Orchestral 配置为每个 Run 提供 32 次模型调用、96 次工具调用；每轮进程默认限时
300 秒（`--timeout-secs`），每条验证命令限时 600 秒（`--verify-timeout-secs`）。
配置文件及二进制、配置摘要随报告保留。

```bash
# 明确调用配置中的真实模型，可能产生模型费用。
cargo run -p orchestral-coding-eval -- run --repo . \
  --agent-config /absolute/path/eval-agent.json \
  --task retry-backoff-cap --repetitions 1
# 省略 --task 运行全部 20 项；默认每项重复 3 次。
```

JSON 结果、提示、补丁、进程日志和可用的会话日志默认保存在新的系统临时目录，也可指定
`--output PATH`。题目校验与 Agent 成绩分别记录，未执行、超时、约束失败和基础设施错误
不会被算成通过。已报告的持久请求用量按请求去重；未测费用和人工纠正次数保留为 `null`。
Agent 报错或超时后仍会独立验证修复结果，但不会因此将未完成的任务计为成功。
默认清理工作副本和构建产物，`--keep-workspaces` 可保留候选副本。评测结果和验收记录
都是本地产物，不属于提交内容。

## 当前边界

- 当前不是 Goal Compiler、Task Broker 或多 Agent Scheduler。
- `DeliveryCommitted` 只表示 Agent 已交付输出，不代表外部目标已经被独立满足或验证。
- Foundation v1 的 MCP 范围有意限定为 Tools；Resources、Prompts、订阅等表面不属于该合同。
- 类型化 Plan/DAG 是单 Agent 内部的可选执行策略，不是产品顶层入口。
