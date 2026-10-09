# Agent Protocol v1

状态：stable foundation contract。版本常量是 `AGENT_PROTOCOL_V1`。

Agent Protocol 管理一个不透明 Agent Run 的启动、控制、观察和恢复。它不定义模型厂商、
Prompt、Tool 实现、Goal Compiler 或多 Agent 调度。

## 分层

- `wire`：可持久化的 Run、Command、Event、Delivery、ResourceBinding 和错误类型。
- `spi`：`AgentProvider`、`AgentJournalStore` 与恢复接口。
- `reference`：Host 侧 reducer、Run 投影和协议状态机。

```text
AgentRunEnvelope
  → AgentController.start
  → AgentProvider.start
  → Host 分配 run_seq 并提交 AgentJournalRecord
  → inspect / events(after_seq) / command / recover
  → 恰好一个 terminal projection
```

## 核心不变量

会话目录中的 `AgentSessionActivity.occurred_at_unix_ms` 是可选的原生事件时间
（非负 Unix 毫秒）。客户端优先使用这个字段排列原生历史和 Host 控制的 Run；
活动 ID 始终作为不透明身份处理。旧适配器可以不提供时间字段。
Claude Code 适配器通过本机历史观察既有会话，将原生跨会话消息作为独立动作，
并通过 `stream-json` 控制由 Host 启动的进程；发送到已有终端的消息保留 peer 来源，
不得宣告直接用户输入或审批授权。同一会话保留
一个执行进程。原生控制细节只在 `plugins/orchestral-agent-claude` 中解释。
启用原生审批桥后，Claude 的 `PermissionRequest` hook 进入会话级 `PendingRequest`，
由现有 session request API 返回用户决定。终端先处理会取消 hook 并关闭请求；
Host 控制的 SDK Run 仍使用自己的 durable request 路径，不产生第二个审批来源。

1. Host 是 `run_seq` 和 durable Journal 的唯一权威；Provider 只能提交无序号 Draft。
2. `run_id + spec_digest + binding + descriptor_digest` 构成不可变启动身份。
3. Command 使用 `command_id + digest` 幂等；相同 ID 的不同内容必须拒绝。
4. Stream EOF、sequence gap 或无法证明的恢复进入 `Unknown`，不能伪造成功或取消。
5. 一个 Run 最多一个 `DeliveryCommitted / RunIncomplete / RunFailed / RunCancelled` 终态；
   终态后的 Draft 和 telemetry 不改变 durable 投影。
6. `DeliveryCommitted` 只证明 Agent 已交付，不等于 `GoalSatisfied` 或 `Verified`。
7. ResourceBinding 只授予可见性，不授予 Tool、文件、网络或 Secret 权限。

## 最小 Host 调用

```rust
use orchestral_core::agent_protocol::{wire::*, AGENT_PROTOCOL_V1};

let run = AgentRunEnvelope::new(
    AGENT_PROTOCOL_V1,
    AgentSessionId::new("session-1"),
    RunId::new("run-1"),
    vec![Content::text("summarize this repository")],
)?;
let execution = controller.start(run).await?;
let terminal = controller.wait_for_terminal(&execution.run_id).await?;
let durable = controller.events(&execution.run_id, 0).await?;
```

完整 composition 见 [`examples/agent_session.rs`](../../examples/agent_session.rs)，Provider
一致性入口见 `testing/orchestral-agent-protocol-testkit`。

## 兼容规则

- v1 reader 拒绝未知 core 字段；扩展只能放在 namespaced `extensions` 中。
- Provider 必须先声明 capability；不支持的 limit、resource、control 或 output schema 返回
  结构化 `UnsupportedCapability`，不能静默忽略。
- recovery 必须继续同一 Execution；`OutcomeUnknown` 不能通过创建新 Run 绕过。

HTTP Host 遇到不可重试的恢复错误时暂停自动恢复，保留 `unknown` 和原 Session 的
执行权。排除故障后，用户可点击 PWA 的“重试恢复”，或显式调用
`POST /api/v1/runs/{run_id}/recover?retry_manual=true`（外部 Agent 还需原
`connector_id`）。默认 `retry_manual=false`，浏览器的自动恢复不越过手动暂停。
此操作只重新核对并连接同一 Execution，不重发初始输入、不创建新 Run，也不将
`unknown` 猜测为已完成。仍无法证明连续性时继续保留 `unknown`。

Codex 原生文件描述符耗尽（`EMFILE`/`ENFILE`）按临时不可用处理，复用 Host 的退避
恢复。它不会自动重启共享 app-server；该进程可能同时承载其他活跃会话。

## 可协商的输入队列与上下文用量扩展

Generic Provider 在 descriptor 的 `extensions["orchestral/input-queue.v1"]` 声明
`{"version":1}`。Host 只向声明支持的 Provider 发送带同名命名空间的 Steer command；
扩展值为 `{"operation":"enqueue"}`、`{"operation":"replace","target":"原 command_id"}`
或 `{"operation":"withdraw","target":"原 command_id"}`。普通 Steer 的立即控制语义保留。
排队消息在下一次模型调用边界接收；修改保留队列位置，以新 command_id 取代旧身份。
修改与撤回仅适用于仍在队列中的消息，和消费使用同一个运行时锁决定先后。
`Accepted` 表示操作已记入 WAL，`InputCommitted` 才表示输入已接收。
重放接受、修改、撤回和消费记录恢复队列，不重发已接收内容。

上下文显示使用 `AgentTelemetry::Extension`，namespace 为
`orchestral/context-usage.v1`，payload 为 `ContextUsageReport`。其中 `request_id` 绑定最近
模型请求，`input_tokens` 表示本次输入，`input_tokens_estimated` 区分估算与实报，
`max_context_tokens` 缺省表示未声明上限。它不代表累计计费量或实时 KV 占用。
以上扩展沿用 v1 的命令、遥测和摘要规则，不增加未知 core variant。

会话摘要可提供 `input_action`，指向目录中声明了 `input_channel: true`、输入 schema
和 Immediate 执行方式的动作。标准参数为 `AgentSessionTextInput { submission_id, text }`。
客户端通过此动作向现有外部进程提交文字，不分配 Host Run，也不声明审批或取消控制权。
适配器须说明原生输入的来源语义；`input_channel` 只声明输入框的发送动作，
不赋予客户端原进程的用户身份或审批权限。
适配器负责持久化发送去重；不确定的结果报告为 `OutcomeUnknown`，客户端不能自动
换一个身份重发。
