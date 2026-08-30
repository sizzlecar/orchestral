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
      ├── 直接 Tool ───────────────────────────┐
      └── 可选注入的 Workflow → Plan/DAG ──────┤
                                    ▼
                         GuardedToolRuntime
                           ├── 内置 Tools
                           └── MCP Tools（stdio / Streamable HTTP）
```

## 快速开始

导出任意一个已配置模型的密钥：

```bash
export OPENAI_API_KEY="..."
# 或 GOOGLE_API_KEY / OPENROUTER_API_KEY / DEEPSEEK_API_KEY
```

执行单轮任务：

```bash
cargo run -p orchestral-cli -- "总结这个仓库的公共 API"
```

进入全屏交互式 Agent Session：

```bash
cargo run -p orchestral-cli --
```

根命令本身就是 Agent 入口，不存在 `agent` 子命令。入口选择是确定性的：

| 调用方式 | 模式 |
| --- | --- |
| stdin/stdout 都是终端时执行 `orchestral` | 多轮 TUI |
| `orchestral "修复这个 bug"` | Headless 单轮 |
| `printf '修复这个 bug' \| orchestral` | Headless 单轮 |

Headless stdout 只输出最终 Delivery，进度和错误进入 stderr，适合管道消费。TUI 中 Enter
发送消息或 Steer，Shift/Alt+Enter 换行，`a`/`d` 处理审批，Ctrl-C 取消当前 Run，
PageUp/PageDown 或鼠标滚轮滚动，Esc 退出；支持 paste、resize、中文与 emoji。
`completed` 只表示当前 Turn 已收敛并提交输出，不表示用户的外部目标已经被独立证明完成。

CLI 依次发现 `.orchestral/config.yaml`、`.orchestral/config.yml`、
`configs/orchestral.cli.yaml`、`orchestral.yaml`；都不存在时会生成
`.orchestral/config.yaml`。可以用 `--config`、`--backend`、`--model-profile` 或
`--model` 显式选择，例如：

```bash
orchestral --backend deepseek --model deepseek-chat "检查这个 crate"
orchestral --backend google --model gemini-3.1-pro-preview "检查这个 crate"
```

OpenAI-compatible 厂商读取配置中对应的密钥环境变量。Google 可通过 `GOOGLE_API_KEY`
调用 Gemini API，也支持 Vertex AI 的标准 Application Default Credentials 链：
`GOOGLE_APPLICATION_CREDENTIALS`、`gcloud auth application-default login` 生成的文件
（Unix 默认 `~/.config/gcloud/application_default_credentials.json`），或 Google Cloud
挂载的服务账号。`--credential-file PATH` 是 service-account JSON key 的便捷覆盖；Vertex
project 必须能从凭据或 `GOOGLE_CLOUD_PROJECT` 解析。

`--session-id` 为多轮对话提供稳定、持久的 Session 身份；`--no-mcp` 和 `--no-skills`
可分别关闭两套扩展面。

最小 coding 任务：

```bash
orchestral "修复当前 workspace 中失败的项目，运行测试，并报告经过验证的结果。"
```

模型只看到一个结构化文件修改 Tool：`apply_patch`，支持 Add/Update/Delete，不能自行选择
workspace root 或审批权限。`file_read`、`apply_patch`、`exec_command` / `write_stdin` 和
MCP 调用都继续经过 Host policy 与 Effect Journal。

`exec_command` 只启动 Host 解析并批准的 shell，但允许它在 OS sandbox 内运行普通子进程和
本地工具链，不要求逐个配置程序白名单。真正的边界是 Host 批准的读写根目录、精确网络目标、
捕获的环境变量、时间/输出上限、逐次审批与 Effect Journal。默认不继承完整宿主环境，并关闭
网络；MCP stdio 的启动程序仍必须由 Host 明确配置。模型可见参数不能扩大任何权限。

启用 `skills.auto_discover` 后，CLI 会从 workspace 的 `.claude/skills`、`.codex/skills`、
`skills` 以及显式 `skills.directories` 发现 `SKILL.md` 包。初始 Context 只包含 Skill descriptor；
选中后由 `skill_read` 载入完整指令，相对资源从该 Skill 目录解析。MCP 与 Skill 保持独立：
`mcp.servers` 支持 Host 配置的 stdio 与 Streamable HTTP transport，发现的方法会成为经过统一
Guarded Runtime 的命名空间 Tool，而不是提示词。

本地 MCP 与远程 MCP 使用同一个 Host Registry。可通过
`orchestral --mcp-config PATH` 显式加载 `.mcp.json`；仅仅因为仓库中存在该文件，
Orchestral 不会自动执行它。本地进程使用精确可执行文件和隔离的私有 HOME，其 cwd、
读写目录、环境变量及网络权限均与通用 Shell 分离。为兼容 `npx`、`uvx` 和 shell wrapper，
注册的启动器默认可形成进程树，但所有子进程仍受该 MCP 沙箱约束；单进程服务可设置
`allowChildProcesses: false`。也可以在主配置中固定 Manifest：

```yaml
mcp:
  import_files: [.mcp.json]
```

## SDK

公共 SDK 就是 Agent 控制面：`AgentClient` 启动 Run，`AgentRunHandle` 提供事件订阅、
Inspect、Command、输入恢复、Steer、Cancel 和终态等待，不再暴露旧 Planner Loop。

运行完整的 AI 中立示例：

```bash
cargo run -p orchestral-examples --example agent_session
```

最小组合方式见 [`examples/agent_session.rs`](examples/agent_session.rs)：它把
`ModelBackend`、`InternalGenericAgentProvider`、`AgentController` 与 `AgentClient` 连接起来。

## 版本化合同

- [Agent Protocol v1](docs/agent-foundation/agent-protocol-v1.md)
- [Model Protocol v1](docs/agent-foundation/model-protocol-v1.md)
- [Guarded Tool Runtime v1](docs/agent-foundation/tool-runtime-v1.md) 与
  [Tool Artifact v1](docs/agent-foundation/tool-artifact-v1.md)
- [Skill Runtime v1](docs/agent-foundation/skill-runtime-v1.md)
- [MCP Tools Adapter v1](docs/agent-foundation/mcp-tools-adapter-v1.md)

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
- Foundation v1 的 MCP 范围有意限定为 Tools；Resources、Prompts、订阅等表面不属于该合同。
- 类型化 Plan/DAG 是单 Agent 内部的可选执行策略，不是产品顶层入口。

## 许可证

见 [LICENSE](./LICENSE)。
