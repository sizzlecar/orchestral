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
