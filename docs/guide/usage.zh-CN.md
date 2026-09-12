# 使用指南

[返回 README](../../README.zh-CN.md)

### 使用自己的 OpenAI 兼容服务

无鉴权服务不需要手写 YAML，也不需要占位 API Key：

```sh
orchestral --base-url http://127.0.0.1:8000/v1
```

支持服务根 URL、`/v1` 基址或完整 `/chat/completions` 地址。服务只提供一个模型时自动
选用；有多个模型时会列出可选名称，由你明确指定：

```sh
orchestral --base-url http://127.0.0.1:8000/v1 --model your-model-id
```

也可以设置 `OPENAI_BASE_URL` 和 `OPENAI_MODEL` 环境变量。命令行参数优先；显式指定
`--backend` 或 `--model-profile` 时，不使用 URL 环境变量。

自定义 URL 默认**不发送鉴权信息**，也不会继承已有云端密钥。服务或网关需要鉴权时，
先在环境中设置密钥，再指定它的变量名：

```sh
orchestral --base-url https://your-gateway.example/v1 --api-key-env LOCAL_MODEL_API_KEY --model your-model-id
```

在 YAML 中，无鉴权 provider 使用 `endpoint` 和 `config: { auth: none }`；默认鉴权模式
是 `api_key`。编程操作还需要模型具备工具调用能力，仅接口兼容不代表模型一定支持。

检查配置，或在不生成回答的情况下查询模型列表：

```sh
orchestral doctor
orchestral --base-url http://127.0.0.1:8000/v1 doctor --check-connection
```

`doctor --json` 输出隐藏密钥值的诊断报告。普通 `doctor` 不发送网络请求、不创建配置
文件。开始会话时，沿用检查时的连接参数。

### 其他模型与日常操作


也可显式选择其他模型：

```bash
export GOOGLE_API_KEY="..."
orchestral --model-profile gemini-2.5-flash "检查这个工作区"
```

在源码仓库中也可运行 `cargo run --locked -p orchestral-cli -- "检查这个工作区"`。

根命令本身就是 Agent 入口，不存在 `agent` 子命令。入口选择是确定性的：

| 调用方式 | 模式 |
| --- | --- |
| stdin/stdout 都是终端时执行 `orchestral` | 多轮 TUI |
| `orchestral "修复这个 bug"` | Headless 单轮 |
| `printf '修复这个 bug' \| orchestral` | Headless 单轮 |

Headless stdout 只输出最终 Delivery，进度和错误进入 stderr，适合管道消费。TUI 中 Enter
发送消息、Steer 或回答当前问题；Ctrl+J 换行（终端支持时也可用 Shift+Enter）。上下键先移动
多行光标，再访问本会话输入历史，返回时恢复草稿。粘贴支持中文、组合字符与 emoji；超过
20 行显示有界预览，Ctrl+P 展开。

F1 / Ctrl+] 打开命令并保留草稿。`/` 发现命令，`//` 发送以斜杠开头的普通文本。`@` 补全
工作区路径，排除忽略目录和构建输出；选择路径不会读取文件正文。候选面板打开时会后台刷新，
包含新建、重命名后的文件。空闲时可通过 `/model`、
`/new`、`/resume` 切换配置中的模型或会话；草稿在当前进程内按会话保留。`/resume` 面板中的
“Current session details” 查看实际存储位置和已报告用量。`/context` 区分当前或最近一轮的
技能加载记录，并展示进程加载的规则来源和会话压缩记录。没有依据的上下文占用显示 `—`。

Ctrl+O 在对话中展开工具记录；PgUp/PgDn 阅读历史或当前面板，End 跟随新输出。保留终端原生
文本选择；存在本地剪贴板工具时，`/copy` 复制最近的已提交回答。`/`、F1 和 `/help` 提供统一
操作菜单，其中包含快捷键说明和外观设置；`NO_COLOR` 禁用样式。`/skills` 搜索工作区发现的
技能，Enter 查看完整说明和来源，空格切换启用偏好；待重启的修改与当前进程状态分别显示。
浏览技能不会加载模型指令，也不会写入对话。

Esc/Ctrl+C 优先关闭当前面板，否则中断活动 Run。空闲时 Ctrl+C 清空草稿；输入为空时
Ctrl+D 退出，或使用 `/quit` 停止并退出。审批须用 `a`/`d`，或先用方向键明确选择再按 Enter。
`replied` 表示回答已投递，不代表外部目标已被独立验证。等待回答时，`/` 开头的文字也作为
回答提交；需要命令时使用 F1。回答和审批提交后等待确认，期间不重复发送。
问题结束后恢复之前的草稿；先编辑或移动光标再发送，避免多按一次 Enter 意外提交旧草稿。

CLI 依次发现 `.orchestral/config.yaml`、`.orchestral/config.yml`、
`configs/orchestral.cli.yaml`、`orchestral.yaml`；都不存在时会生成
`.orchestral/generated/default.agent.yaml`。可以用 `--config`、`--backend`、`--model-profile` 或
`--model` 显式选择，例如：

```bash
orchestral --backend deepseek --model deepseek-chat "检查这个 crate"
orchestral --backend google --model gemini-2.5-flash "检查这个 crate"
```

OpenAI-compatible 厂商读取配置中对应的密钥环境变量。Google 可通过 `GOOGLE_API_KEY`
调用 Gemini API，也支持 Vertex AI 的标准 Application Default Credentials 链：
`GOOGLE_APPLICATION_CREDENTIALS`、`gcloud auth application-default login` 生成的文件
（Unix 默认 `~/.config/gcloud/application_default_credentials.json`），或 Google Cloud
挂载的服务账号。`--credential-file PATH` 是 service-account JSON key 的便捷覆盖；Vertex
project 必须能从凭据或 `GOOGLE_CLOUD_PROJECT` 解析。

`--session-id` 为多轮对话提供稳定、持久的 Session 身份；`--no-mcp` 和 `--no-skills`
可分别关闭两套扩展面。

内置 Agent 的历史会话可以直接查找和恢复：

```bash
orchestral sessions list                         # 当前工作区，最近更新在前
orchestral sessions list --search parser         # 按标题或 Session ID 搜索
orchestral sessions list --all --limit 20 --json  # 所有工作区，含旧会话
orchestral sessions show SESSION_ID              # 查看原始对话与工具结果
orchestral resume SESSION_ID                     # 在终端中回放历史并继续交互
orchestral resume --last                         # 当前工作区的最近会话
orchestral resume --last "继续验证刚才的修改"      # 单轮续接，也支持 stdin 管道
```

`sessions list/show` 默认读取内置 Agent；外部 Codex 会话使用 `--connector codex`。
列表支持 `--cursor` 翻页，查询无需模型凭据，也不启动模型、工具或恢复执行。会话目录由
Run/Session 日志重建；压缩影响模型上下文，TUI 仍回放原始对话。工具结果在 TUI 中显示
有界摘录，完整内容可通过 `sessions show SESSION_ID --json` 查看。

新 Run 记录工作区和模型来源。`--last` 只选择当前工作区，按 ID 恢复其他工作区时会提示
使用对应 `-C`；来源元数据不会自动扩大工具权限。没有来源元数据的旧会话仍可通过
`--all` 找到并按 ID 恢复，但不会被 `--last` 自动选中。恢复已完成的会话后，新输入创建新
Run；未完成的 Run 先由 Controller 按现有 checkpoint 合同恢复，等待输入或审批时继续
原交互，未观察到结果的模型尝试收束为 `Incomplete`，已提交工具效果不会重复执行。
无法确认的效果保留 `UnknownEffect`；恢复身份不兼容时明确报错，不另起 Run 掩盖问题。
同一文件日志目录同时只允许一个 Host 写入，浏览命令可以并行只读访问。

长会话多次压缩时，runtime 会沿日志引用重新读取原始记录，避免反复压缩旧摘要。
跨 Run 续聊会在历史条数和 token 预算内优先恢复最近一条被压缩的用户输入；有界摘要
保留类型化的工具失败状态，但工具执行成功不等于任务已通过验证。

内置 `session_read` 工具允许 Agent 搜索当前会话的原始记录，再按 JSON 字段或分块读取
完整结果。它不能选择其他会话，且沿用 Host 权限、取消和 Tool Effect 日志。
来源、分页与恢复兼容性详见 [Session Context and Recall](../../docs/agent-foundation/session-context-v1.md)。

内置 Agent 自动复用现有项目指令。对 `-C` 和每个 `--add-dir` 工作区，从最近的 Git 根目录
到所选目录逐层发现；Git worktree 的 `.git` 文件同样支持，非 Git 目录只检查所选目录。
每层采用第一个非空文件：`AGENTS.override.md` → `AGENTS.md` → `CLAUDE.md`，祖先规则在前，
具体目录规则在后。重叠工作区共享的来源只加载一次，每份指令保留来源和目录作用域。
默认不会扫描无关子目录或导入其他 Agent 的全局个人配置；可用 `--add-dir` 选择额外目录。

指令是 Host 启动时的固定快照，压缩与重试继续使用该快照；重启 Host 后重新加载。
恢复旧 Run 时，如果指令内容发生变化，恢复身份校验会拒绝继续该 Run。
项目指令不会扩大 Host 授权。同目录的 `AGENTS.md → CLAUDE.md` 符号链接可复用；越出来源
目录的链接、非 UTF-8 或超出总字节上限的指令会明确报错，避免
静默丢失项目规则。

模型在返回文字、工具调用或 Finish 之前遇到可重试的限流或临时不可用错误时，会在当前
模型步骤内自动退避重试，默认最多 3 次；CLI/TUI 会显示等待进度，取消和 Steer 可立即
打断等待。只返回用量的失败请求也可以重试，其最后一份用量快照会持久化，并计入 Run
已报告用量一次。已经出现文字、工具调用或 Finish 的请求不自动重发，已执行工具也不会
因模型重试而重新执行。配置了 Run 累计 Token/费用上限时，只自动重试未报告用量的明确
限流拒绝，因为失败请求的最后一份快照不一定包含全部消耗。进程重启后的不确定模型调用
仍按原恢复合同处理。

可在配置中关闭发现、扩展兼容文件名或调整重试：

```yaml
agent:
  project_instructions:
    enabled: true
    max_bytes: 65536
    fallback_filenames: [CLAUDE.md, TEAM_GUIDE.md]
  model_retry:
    max_retries: 3 # 0 表示禁用自动重试
    base_delay_ms: 500
    max_delay_ms: 8000
```

无需凭据的 CLI/PTY E2E 覆盖规则优先级、作用域、快照、重试、取消、Steer 和工具去重：

```bash
cargo test -p orchestral-cli --test agent_live_e2e
```

真实模型的项目指令 coding 验收使用已有 Vertex 凭据，明确启用后会产生模型费用：

```bash
cargo test -p orchestral-cli --test agent_live_e2e live_agent_uses_existing_project_instructions_for_coding -- --ignored --test-threads=1
```


## 工具与权限

最小 coding 任务：

```bash
orchestral "修复当前 workspace 中失败的项目，运行测试，并报告经过验证的结果。"
```

模型只看到一个结构化文件修改 Tool：`apply_patch`，支持 Add/Update/Delete，不能自行选择
workspace root 或审批权限。`file_read`、`apply_patch`、`exec_command` / `write_stdin` 和
MCP 调用都继续经过 Host policy 与 Effect Journal。

在 macOS 和 Linux 上，`exec_command` 只启动 Host 解析并批准的 shell，但允许它在 OS sandbox 内运行普通子进程和
本地工具链，不要求逐个配置程序白名单。真正的边界是 Host 批准的读写根目录、精确网络目标、
捕获的环境变量、时间/输出上限、逐次审批与 Effect Journal。默认不继承完整宿主环境，并关闭
网络；MCP stdio 的启动程序仍必须由 Host 明确配置。模型可见参数不能扩大任何权限。

原生 Windows 命令在逐次审批后以当前用户的系统权限运行；具备进程管理和时间/输出限制，
但没有文件系统或网络隔离。

命令的临时文件位于仓库外、由 Host 管理的私有目录。`TMPDIR`、`TMP`、`TEMP` 指向当前
Run 的专用子目录，同一 Run 内可共享，沙箱不能访问其他 Run 的子目录。进程使用期间
目录持续保留，Run 结束后回收。CLI 在重启后保持 Host 根目录身份稳定，不会静默复用
其他 Host 或崩溃遗留的 Run 目录。SDK Host 需将 `ProcessSupervisor::runtime_temp_root()`
加入 Host、Run 和 exec Tool 的文件系统读写授权；文件 Tool 仍只开放工作区。

进程等待支持 `wait_mode: "completion"`，将输出汇总到进程退出或本次等待到期；
`wait_mode: "output"` 则在输出短暂停顿后返回。非 TTY 命令默认使用 completion 模式，
首次等待 10 秒，无输入的非 TTY `write_stdin` 默认等待 30 秒；TTY 会话和发送输入默认
使用 output 模式。`yield_time_ms` 可在 Host 执行上限内调整等待时长，等待到期会返回
会话 ID，进程继续运行。收到新的 Steer 指令时，两种等待均可提前返回当前输出，让 Agent
处理追加指令，同时保留原进程，不终止或重跑它。

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
