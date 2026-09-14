# TUI v1.0 实现与验收记录

日期：2026-09-08。分支：`feat/tui-experience-v1`。对应 [需求](tui-experience-v1.md)。

当前是可运行、可审查的实现。M1–M3 主体已经接通，M4 已完成下述自动化与真实 provider 验证；**尚未宣布完成全部 v1.0 发布验收**。跨终端人工检查、完整性能指标和部分竞态组合仍需补齐。

## 已实现

- 项目与会话标题、按请求顺序展示的正文、缩进和默认折叠的工具块、当前动作、模型和上下文预算。普通交付显示 `replied`，不冒充独立验证。
- 字素编辑、按词编辑、多行移动与会话输入历史；200 行粘贴保留原文，超过 20 行显示行数和有界窗口，Ctrl+P 展开；1 MiB 上限拒绝时保留原草稿。
- `/` 命令发现、F1 / Ctrl+] 独立帮助、`/hotkeys`、`@` 文件引用。索引遵守工作区和忽略规则，排除构建目录；只插入路径，提交时检查已选择的引用。
- Ctrl+O 切换工具展开，`/tools` 查看单个已记录工具；PgUp/PgDn 阅读、End 跟随；流式与最终输出使用稳定身份合并。历史缓存复用已排版内容，工具更新从实际变化的条目开始重排。
- `/model` 通过 Host 重建实际模型组合；沿用同一组 journal 实例，包括内存 journal，避免重新打开文件写入者或丢失会话上下文。
- `/new`、`/resume`、会话搜索和进程内草稿隔离；沿用已有恢复、工作区兼容性和权限合同。运行期间阻止切换，不自动取消工作。
- `/session` 展示实际存储位置、每次请求和会话已报告用量；`/context` 展示 Host 已加载规则、已激活 Skills、预算、最近压缩范围。当前上下文占用没有实时估计时显示 `—`；日志没有压缩时钟信息时明确说明。
- `/copy` 复制已提交回答，清除终端控制序列；系统剪贴板失败时保留正文并显示原因。内置 terminal/dark/light 和 `NO_COLOR`。
- 帮助与选择器优先处理焦点；审批须明确选择；等待回答时允许以 `/` 开头的普通答案。退出等待取消，后台读取可取消，过期面板结果不重新打开面板。

没有加入插件执行器、LSP、follow-up 队列或 PWA 功能。

## 本轮发现并修复的问题

1. 窗口缩放与输入同时到达时，原终端事件源会间歇性停止处理已到达输入。切换到 crossterm 的 `use-dev-tty` 轮询路径后，原 PTY 流程通过。
2. 恢复命令已经接受问题答案，但 durable projection 尚未清除旧请求时，headless CLI 可能重复提示，并因 stdin 已关闭而取消。CLI 现在把恢复时已提交的请求 ID 带入已处理集合；复用与后续回答相同的处理规则，不修改协议或用 sleep 掩盖竞态。恢复 E2E 在三个独立进程场景中检查不重复提问、不重复文件副作用。

## 自动化记录

最终结果：`cargo test --workspace --all-targets` **732 passed / 0 failed / 18 ignored**，共 49 个测试二进制。TUI 单元/快照子集 **39 passed / 1 ignored**；Agent 进程 E2E **43 passed / 10 ignored**。性能用例和下述两个 live 用例另行显式运行并通过。`cargo fmt --all -- --check`、全 workspace/all-features clippy（`-D warnings`）及 `git diff --check` 通过。

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets

# 保存真实 CLI 进程的屏幕投影和 asciicast 回放
ORCHESTRAL_TUI_ARTIFACT_DIR=/tmp/orchestral-tui-review \
  cargo test -p orchestral-cli --test agent_live_e2e tui_pty

# 独立、可选的本地性能测量
cargo test -p orchestral-cli --lib \
  large_history_stream_and_file_completion_latency -- --ignored --nocapture
```

本机为 Apple M1 Max / arm64、macOS 15.1.1、Rust 1.91，使用 debug 构建。磁盘空间曾不足，清理项目 `target/debug/incremental` 后，后续检查使用 `CARGO_INCREMENTAL=0`。

PTY harness 启动构建出的 `orchestral`，以 vt100 解析当前画面，真实发送按键、括号粘贴和 resize。新增 5 条 PTY 用例，与原 2 条核心 PTY 用例一起覆盖：

| 流程 | 关键证据 |
| --- | --- |
| 编辑、帮助与历史 | 中文、组合字符和完整 emoji 删除；帮助关闭、历史返回均保留草稿；HTTP 请求断言原文 |
| 文件引用 | 中文/空格路径补全；删除后拒绝提交；恢复文件后只发送路径；未知命令不进入模型 |
| 模型与会话 | 下一次 HTTP 请求实际使用新模型，并包含前一轮回答；内存 journal 连续；切换会话恢复草稿 |
| 长输出与滚动 | 阅读锚点在后续输出提交后保留，resize 后仍能阅读；End 返回最新输出 |
| 长粘贴与复制 | 200 行完整提交，展开窗口；可控本地剪贴板成功/失败；ANSI 清除；会话与上下文详情 |
| 提问、审批与取消 | 问题回答、明确审批、重复投影后选择不变；取消记录、文件实际结果、终端恢复 |
| Agent 失败退出 | 可见错误、可退出、备用屏/光标/括号粘贴模式恢复 |

既有 PTY 与进程测试同时覆盖：模型重试期间取消/Steer、恢复提问、压缩后的进程重启、不重复文件副作用、旧会话兼容性等。

## 真实 provider

已显式运行以下 opt-in 测试，均通过：

```sh
cargo test -p orchestral-cli --test agent_live_e2e \
  live_vertex_stream_obeys_the_terminal_contract -- --ignored --nocapture

ORCHESTRAL_TUI_ARTIFACT_DIR=/tmp/orchestral-tui-review \
  cargo test -p orchestral-cli --test agent_live_e2e \
  live_tui_repairs_rust_and_continues_after_session_selection -- --ignored --nocapture
```

第一项验证真实 Vertex 流式输出与 headless 终态合同，约 7 秒。第二项约 30 秒：实际启动 TUI，通过 `@` 引用源码，修复 Rust 项目；随后添加独立测试并运行 `cargo test`，覆盖正数、负数和零；打开工具列表、选择原会话，再完成第二轮追问。检查两次持久交付及终端恢复。其余有凭据要求的 live/native 测试保持 opt-in，未全部运行。

这些测试证明本次运行和连接可用，不代表对模型任务质量做了统计评测。

## 性能与回放

测量使用 `TestBackend(100×30)`，10,000 条历史、10,000 个文件候选、每秒安排 20 次文本与工具更新，共 100 个样本。首次排版约 **850 ms**；编辑、候选过滤和渲染的 **p95 约 88 ms**，最大约 **117 ms**。取消在 UI reducer 中产生控制效果约 **15 ms**。

这不是实际终端输入到 Host 的完整延迟测量；尚未测量控制请求发出 p95、进程实际停止耗时和 RSS 峰值。初次加载仍排版完整历史，进一步虚拟化与首次加载优化未完成。不能据此宣布 R08 全部达标。

本机回放在 `/tmp/orchestral-tui-review/`，包含 `.cast` 与逐帧 `.txt`。真实编码流程文件名为：

`tui_experience-live_tui_repairs_rust_and_continues_after_session_selection.cast`

可用支持 asciicast v2 的播放器查看。回放由实际 PTY 输出解析得到；它不替代 GUI 终端的字体、输入法和原生文本选择验收。该目录是临时测试产物，可用上面的命令重建。

## 验收矩阵覆盖及剩余项

| 条目 | 当前覆盖与剩余项 |
| --- | --- |
| T01 | 四种尺寸的快照和实际 PTY；三种配色、单色及 GUI 光标/字体仍需人工检查 |
| T02 | 编辑单元与 200 行 PTY 通过；实际中文输入法待人工检查 |
| T03 | 过滤、帮助、关闭、未知命令和模型请求隔离有 PTY；`//` 的独立进程场景待补 |
| T04 | 路径引用、中文/空格、删除与恢复有 PTY；同名、多根、忽略目录和符号链接的组合场景待补 |
| T05 | 新输出、resize、End、去重有单元/PTY；所有展开与折叠组合尚未穷举 |
| T06 | 工具聚合、diff、截断有现有投影/快照；超大单工具详情和原始 artifact 取回入口需继续完善 |
| T07 | Steer 接受及重试中 Steer 有 PTY；拒绝、超时与终态并发组合待补 |
| T08 | 问题草稿与斜杠答案有单元，审批有 PTY；所有重复 Enter 和面板切换组合待补 |
| T09 | 模型成功切换和历史连续有 PTY；配置/鉴权失败、忙时操作的独立进程覆盖待补 |
| T10 | 会话选择、搜索和草稿隔离有 PTY；占用/兼容性沿用 Host 合同，面板中的组合场景待补 |
| T11 | 未知数据明确标记，压缩恢复有 E2E；用量详情与重复事件的专项场景待补 |
| T12 | 可控剪贴板成功/失败及 ANSI 清除有 PTY/单元；SSH 原生选择待人工检查 |
| T13 | 取消、失败、退出、重启恢复有实际进程覆盖；SIGKILL 不承诺自行恢复终端 |
| T14 | 本地渲染测量已记录；完整链路、首次加载和内存指标未完成 |
| T15 | 请求/补充输入标识与 durable 去重有单元及多轮 PTY；缺失归属的全部旧历史组合待补 |
| T16 | 已结束和等待回答的恢复有 PTY；等待审批、不可恢复状态在选择器中的完整组合待补 |

Terminal.app、iTerm2、Ghostty、Linux 终端及 tmux/SSH 的人工验收尚未完成。当前不以这份记录宣布正式 v1.0 发布。

## 2026-09-09：交互边界修复与复测

- 回答和审批增加提交中/已接受状态；等待 durable resolution 时不重复提交，已解决请求的旧投影被忽略。被拒的回答保留为可编辑内容，仅已接受回答进入对话投影。
- 问题结束后恢复旧草稿，但额外 Enter 不发送它；编辑或移动光标后可显式发送。覆盖普通草稿及以斜杠开头的命令草稿。
- 文件候选在打开补全及 Run 结束后异步刷新；候选面板持续打开时每两秒请求刷新。最多一个扫描和一个合并后的待刷新请求，退出可取消；刷新按路径身份保留选择。
- 滚动 PTY 用例逐次等待翻页后的画面，再记录锚点并释放后续回答，消除两个 PgUp 尚未全部处理时提前取样的竞态。

本次执行：

```sh
CARGO_INCREMENTAL=0 cargo test -p orchestral-cli --all-targets
CARGO_INCREMENTAL=0 cargo test -p orchestral-cli --lib \
  large_history_stream_and_file_completion_latency -- --ignored --nocapture
cargo fmt --all -- --check
CARGO_INCREMENTAL=0 cargo clippy -p orchestral-cli --all-targets --all-features -- -D warnings
git diff --check
```

CLI 全目标 **167 passed / 0 failed / 11 ignored**：库测试 118 通过，Agent 进程 E2E 48 通过，HTTP Host E2E 1 通过。11 个默认跳过项中的性能测试随后单独通过；其余 10 项真实模型测试本次未运行。未重新运行全 workspace 测试。

新增 PTY 用例实际验证：回答后的连续 Enter 保留旧草稿，显式编辑操作后才发出下一轮请求；面板保持打开时，文件创建、重命名、删除和重新创建会更新候选并可正确提交。已修复的滚动用例另行连续运行 **5 次，全部通过**。

同一本地 TestBackend 性能场景：首次排版 **720.8 ms**，编辑与渲染 **p95 47.0 ms / 最大 61.9 ms**，取消 reducer **14.3 ms**。这些数据仍不包含后台目录遍历、实际终端到 Host 的完整延迟或 RSS，前述完整性能和人工验收剩余项仍然保留。
