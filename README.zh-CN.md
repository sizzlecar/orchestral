<p align="center"><img src="assets/brand/readme-banner.svg" alt="Orchestral — A runtime for reliable, interactive AI agents." width="1280"></p>

**A runtime for reliable, interactive AI agents.**

为可靠、可交互的 AI Agent 提供运行时。阅读代码、修改文件、调用工具，并在同一会话中继续工作。
你可以连接云模型，也可以在本地使用 Ferrum；入口包括终端 UI、无界面 CLI、浏览器控制端和 Rust SDK。

**Agents are the new processes. Orchestral is the runtime.**

[官网](https://orch.pandaailabs.com) · [English](README.md) · [安装](#安装) · [快速开始](#快速开始) · [SDK](#sdk)

## 现在可以做什么

- **在项目里工作。** 阅读和编辑文件、执行命令，并加载项目指令。
- **随时参与。** 查看工具活动、回答问题、审批操作、调整方向或取消运行。
- **稍后继续。** 查找和恢复会话；上下文压缩后仍保留原始记录。
- **选择模型。** 使用支持的云服务，或连接自己的 OpenAI 兼容端点。
- **扩展上下文和工具。** 加载工作区 skills，通过统一的工具执行路径连接 MCP。
- **选择交互入口。** 使用终端、多轮会话、可通过管道调用的 CLI，或与 Host 配对的浏览器。

## 安装

**v0.3.0 正在准备发布。** 正式二进制发布前，请从源码安装：

```sh
git clone https://github.com/sizzlecar/orchestral.git
cd orchestral
cargo install --locked --path apps/orchestral-cli
orchestral --version
```

仓库固定使用 Rust 1.91.0。可执行文件名为 `orchestral`；浏览器客户端已内嵌，
普通构建不需要 Node.js 或 Dioxus CLI。

正式发布后，可以使用二进制安装器：

```sh
# macOS / Linux
curl -fsSL https://orch.pandaailabs.com/install.sh | sh
```

```powershell
# Windows PowerShell
irm https://orch.pandaailabs.com/install.ps1 | iex
```

安装器校验 SHA-256，并安装到用户目录。再次运行同一命令即可升级。
参见[发布下载](https://github.com/sizzlecar/orchestral/releases)及[安装、回退与卸载](RELEASING.md#installers)。

| 平台 | 命令执行方式 |
| --- | --- |
| macOS Apple Silicon / Intel | 系统 Seatbelt 沙箱 |
| Linux x64，glibc 2.35+ | 需要 `bubblewrap`，并允许非特权用户命名空间 |
| Windows x64 | 原生命令需要审批；沙箱命令请使用 WSL |

Ubuntu 可执行 `sudo apt-get install bubblewrap`。原生 Windows 支持 Streamable HTTP MCP；
本地 stdio MCP 需要 WSL。

0.3 系列尚未达到 1.0，API 和恢复标识可能发生变化。升级前请查看[版本说明](CHANGELOG.md)。

## 快速开始

在项目目录打开终端。使用 OpenAI 云模型时：

```sh
export OPENAI_API_KEY="your-api-key"
orchestral
```

PowerShell 使用 `$env:OPENAI_API_KEY="your-api-key"`。也可以只运行一轮：

```sh
orchestral "梳理这个项目中请求到数据库的调用路径"
```

### 连接本地 Ferrum

当 Ferrum 已在 `http://127.0.0.1:8000` 提供支持工具调用的模型服务时，直接连接：

```sh
orchestral --base-url http://127.0.0.1:8000/v1
```

同一选项也支持其他 OpenAI 兼容服务，可以传服务根地址、`/v1` 或完整的 `/chat/completions` 地址。
服务只列出一个模型时会自动选择；有多个模型时，添加 `--model your-model-id`。

自定义地址默认不带认证，也不会继承云服务的密钥。网关需要认证时，请先设置专用密钥，
再明确选择环境变量：

```sh
orchestral --base-url https://gateway.example/v1 --model your-model-id --api-key-env LOCAL_MODEL_API_KEY
```

也可以通过 `OPENAI_BASE_URL` 和 `OPENAI_MODEL` 设置连接，显式 CLI 参数优先。
要执行编程操作，服务与模型都需要支持工具调用。

不生成模型回复，只检查配置或连接：

```sh
orchestral doctor
orchestral --base-url http://127.0.0.1:8000/v1 doctor --check-connection
```

普通 `doctor` 离线运行；`doctor --json` 不输出凭据值。
模型配置、MCP、skills 和工作区策略见[配置示例](configs/orchestral.cli.yaml)。

## 日常使用

| 命令 | 用途 |
| --- | --- |
| `orchestral` | stdin、stdout 均为终端时进入交互式 TUI |
| `orchestral "你的请求"` | 无界面执行一轮 |
| `orchestral sessions list` | 查找当前工作区的会话 |
| `orchestral sessions show SESSION_ID` | 查看原始对话和工具结果 |
| `orchestral resume --last` | 继续当前工作区最近的会话 |
| `orchestral resume --last "继续验证"` | 无界面追加一轮请求 |

Headless 模式的 stdout 只输出最终答复，进度和错误写入 stderr。
使用 `-C /path/to/project` 选择工作区，`--add-dir` 添加其他目录。

TUI 中，Enter 发送或回答，Ctrl+J 换行；`/` 或 F1 打开命令菜单，`@` 补全工作区路径，
Ctrl+O 展开工具记录。空闲时可以通过 `/model`、`/new`、`/resume` 切换模型或会话。
`/context` 查看已加载的指令和上下文记录，`/skills` 列出已发现的 skills。
详见 [TUI 使用说明](docs/product/tui-experience-v1.md)。

Orchestral 沿工作区路径发现 `AGENTS.override.md`、`AGENTS.md` 或 `CLAUDE.md`，
先加载上级指令，再加载更具体的目录指令。Skills 补充上下文，工具权限仍由 Host 控制。

会话日志在压缩后保留原始消息和工具记录。恢复时避免重复已经提交的工具效果，
不能确认的效果保持为不确定状态。[会话上下文与回溯合同](docs/agent-foundation/session-context-v1.md)
说明了恢复行为和兼容边界。

## 手机控制 PWA

`orchestral serve` 使用同一个 Agent runtime，并提供内嵌浏览器客户端。
与本地 Host 配对：

```sh
orchestral serve --pair -C /path/to/project
```

可以沿用现有模型配置，也可以传入与 CLI 相同的连接选项。手机访问需要可信的 HTTPS 入口，
并设置 `--public-url https://agent.example.com`。扫描配对二维码后，可以打开会话、查看工具、
处理审批、调整方向或取消运行。模型凭据和工作区访问保留在 Host 上。

<details>
<summary>使用认证网关</summary>

如果反向代理提供身份认证，Host 可以验证签名的 RS256 JWT 断言，使用网关会话代替浏览器配对凭据：

```sh
orchestral serve --public-url https://agent.example.com \
  --access-jwt-issuer https://access.example.com \
  --access-jwt-jwks-url https://access.example.com/.well-known/jwks.json \
  --access-jwt-audience orchestral \
  --access-jwt-header X-Access-JWT \
  --access-jwt-required-claim email=owner@example.com \
  -C /path/to/project
```

请按自己的网关配置这些值。Host 对受保护请求检查签名、issuer、audience、有效期和必要 claims。
源站应放在可信 HTTPS 代理后；此模式使用代理的浏览器会话，不使用 `--pair`。

</details>

可选基础设施见[部署文件](deploy/README.md)，浏览器开发见 [Web 指南](web/orchestral-web/README.md)。

## SDK

在 Rust 应用中嵌入同一个运行时。`AgentClient` 启动 Run，`AgentRunHandle` 提供事件、
状态检查、输入、方向调整、取消和终态结果。

```sh
cargo run -p orchestral-examples --example agent_session
```

[完整示例](examples/agent_session.rs) 组合模型后端、Agent provider、controller 和 client，
应用层无需依赖模型服务的具体传输格式。

## 运行时

当前运行时面向单 Agent，提供统一的模型与工具循环、持久会话、受控工具和交互入口。
已有的可选 Plan/DAG 工作流在单 Agent 内执行，尚未实现多 Agent 调度。

记录一次交付表示 Agent 已经输出答复。任务是否正确，仍需通过测试或其他证据验证。

```text
终端 / Headless / 浏览器 / SDK
                 │
          AgentController
                 │
          模型 ↔ 受控工具
                 │
         持久会话与运行日志
```

接口合同：[Agent](docs/agent-foundation/agent-protocol-v1.md) ·
[Model](docs/agent-foundation/model-protocol-v1.md) ·
[工具](docs/agent-foundation/tool-runtime-v1.md) ·
[产物](docs/agent-foundation/tool-artifact-v1.md) ·
[Skills](docs/agent-foundation/skill-runtime-v1.md) ·
[MCP tools](docs/agent-foundation/mcp-tools-adapter-v1.md)。
当前 MCP 支持工具，不包括 Resources 和 Prompts。

## 开发

`core/` 包含合同和运行时，`plugins/` 提供具体集成；CLI 位于 `apps/orchestral-cli/`，
浏览器客户端位于 `web/orchestral-web/`，协议与任务评测工具位于 `testing/`。

```sh
cargo build --locked --workspace
cargo test --locked --workspace --all-targets
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
```

仓库约定见 [AGENTS.md](AGENTS.md)，打包和发布检查见 [RELEASING.md](RELEASING.md)。

## 编码任务评测

[Harbor 适配器](testing/orchestral-harbor/README.md) 使用标准终端任务及其验收器。
本地编码评测工具提供可控的回归修复任务：

```sh
cargo run -p orchestral-coding-eval -- list
cargo run -p orchestral-coding-eval -- validate --repo .
```

`validate` 只检查题目，不调用模型。实跑前配置 [Agent 调用文件](testing/orchestral-coding-eval/agents/orchestral.example.json)。
报告分别记录任务验证、Agent 是否正常完成、超时和基础设施错误。
Agent 自称完成或零测试运行都不算验证通过。

## 许可证

[MIT](LICENSE)。
