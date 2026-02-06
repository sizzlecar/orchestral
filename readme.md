# Orchestral

> **Orchestral** 是一个 Rust-native、Intent-first 的通用交互与任务编排 Runtime。
>
> **Orchestral** is a Rust-native, intent-first runtime for orchestrating long-lived interactions and tasks as dynamic, resumable DAGs.

---

## 设计定位（Design Positioning）

### Orchestral 是什么

* 一个 **通用交互运行时（Interaction Runtime）**
* 一个 **任务编排内核（Task Orchestration Kernel）**
* 一个 **可恢复、可中断、可并发的 Agent 执行系统**

### Orchestral 不是什么

* ❌ 不是 Chat Loop Framework
* ❌ 不是 ReAct Prompt 模板
* ❌ 不是 Workflow DSL / BPM Engine

> Chatbot 只是 Orchestral 的一个**上层应用形态**。

---

## 多 Crate 结构（Workspace Layout）

```
orchestral/                  # Cargo workspace root
├── Cargo.toml
├── README.md
├── crates/
│   ├── orchestral-core/     # 核心抽象与确定性逻辑
│   ├── orchestral-runtime/  # Thread Runtime + 并发/中断/调度
│   ├── orchestral-stores/   # 各类存储实现（InMemory / Redis / DB）
│   └── orchestral-actions/  # 官方 Action 集（可选）
├── apps/
│   ├── orchestral-cli/
│   └── orchestral-server/
├── configs/
├── web/
│   └── orchestral-web/
└── examples/                # 完整示例
```

### Crate 职责划分

| Crate              | 职责                                           | 是否依赖 I/O |
| ------------------ | -------------------------------------------- | -------- |
| orchestral-core    | 抽象、状态机、DAG、Executor                        | 否        |
| orchestral-runtime | Thread / Interaction / 并发策略 / 流管理            | 是        |
| orchestral-stores  | TaskStore / ReferenceStore / EventStore 实现   | 是        |
| orchestral-actions | 内置 Action（图片、文档、对话等）                       | 是        |

---

## 核心概念

### Intent（意图）

用户在当前上下文中的目标描述。

### Plan（计划）

LLM 生成的 **粗粒度 DAG 描述**。

### Action（动作）

原子执行单元，对 Executor 是黑盒。

### Thread（上下文世界）

长期存在的上下文，代表"一件事正在被处理"。

适用场景：聊天会话、工单、IDE Workspace、自动化流程实例。

### Interaction（一次介入）

一次 **触发 → 响应** 的尝试，同一个 Thread 可以同时存在多个 Interaction。

### Event（事实记录）

append-only 的事实，Message 只是 Event 的一种。

---

## 快速开始（Quick Start）

### 添加依赖

```toml
[dependencies]
orchestral-core = { path = "crates/orchestral-core" }
orchestral-runtime = { path = "crates/orchestral-runtime" }
orchestral-stores = { path = "crates/orchestral-stores" }
```

### 基本用法

```rust
use std::sync::Arc;
use orchestral_core::prelude::*;
use orchestral_runtime::{Thread, ThreadRuntime};
use orchestral_stores::{Event, InMemoryEventStore, InMemoryReferenceStore};

#[tokio::main]
async fn main() {
    // Create stores
    let event_store = Arc::new(InMemoryEventStore::new());
    let reference_store = Arc::new(InMemoryReferenceStore::new());

    // Create a Thread (context world)
    let thread = Thread::new();

    // Create ThreadRuntime
    let runtime = ThreadRuntime::new(thread, event_store);

    // Handle events...
}
```

### 运行示例

```bash
cargo run --example basic_usage
```

### CLI 运行（MVP）

```bash
# 单次执行（触发 user_input）
cargo run -p orchestral-cli -- run "帮我生成一份日本旅行攻略"

# 指定 thread_id 继续同一条链路（配合 redis store）
cargo run -p orchestral-cli -- run --thread-id demo-thread "继续"

# 交互模式
cargo run -p orchestral-cli -- repl --thread-id demo-thread

# 注入 external event（恢复 wait_event）
cargo run -p orchestral-cli -- event emit --thread-id demo-thread --kind timer --payload '{"tick":1}'
```

`actions.actions[]` supports an optional `interface` section (`input_schema`, `output_schema`), and the planner will automatically receive these typed contracts in its prompt context.

### 多语言 Action（external_process）

可通过 `kind: external_process` 把 Action 委托给任意语言进程（Python/Node/Go 等），
Orchestral 使用 stdin/stdout 传递 JSON：

- 请求：`{ protocol, action, input, context }`
- 响应：`ActionResult` JSON（推荐），或简写 `{ "exports": {...} }`

示例插件：`examples/plugins/echo_plugin.py`（读取 stdin，返回 success exports）。
配置示例见：`configs/orchestral.cli.yaml` 中的 `external_process` 注释块。

### 安全内置 Action（shell/file_read/file_write）

- `shell` 默认禁用隐式 `sh -c`，如需表达式执行必须显式 `shell=true` 或配置 `allow_shell_expression=true`。
- `shell` 支持 `blocked_commands` / `allowed_commands` 策略以及 `max_output_bytes` 输出截断，防止危险命令与日志爆量。
- `shell` 支持 `sandbox_mode`（`none/read_only/workspace_write`）、`sandbox_backend`（`auto/macos_seatbelt/linux_seccomp/...`）与 `sandbox_writable_roots`；macOS 使用 seatbelt，Linux 使用 bubblewrap（可通过 `sandbox_linux_bwrap_path` 指定）。
- `shell` 默认使用 `env_policy=minimal`，避免把宿主机全部环境变量暴露给命令；可通过 `env_allowlist/env_denylist` 细化。
- `file_read` 默认有 `max_read_bytes` 上限，并支持请求级 `max_bytes` + `truncate` 控制。
- `file_write` 默认有 `max_write_bytes` 上限，校验 `sandbox_writable_roots` 越权路径，并拒绝符号链接写入。

---

## 日志与排障（Logging）

- 进程日志基于 `tracing`，启动时会读取配置中的 `observability.log_level`（如 `info/debug/trace`）。
- 可配置写入文件：`observability.log_file`，或通过环境变量 `ORCHESTRAL_LOG_FILE` 覆盖。
- 可用环境变量 `RUST_LOG` 覆盖配置（例如 `RUST_LOG=orchestral_runtime=debug,orchestral_core=debug`）。
- 运行 CLI 时建议：

```bash
RUST_LOG=debug cargo run -p orchestral-cli -- run "你好"
```

```bash
ORCHESTRAL_LOG_FILE=logs/orchestral-runtime.log cargo run -p orchestral-cli -- run "你好"
```

- 执行链路进度会作为 `system_trace` 事件写入 EventStore，`category=execution_progress` 可用于历史回放与实时展示。
- 每轮执行结束会额外产出 `AssistantOutput` 事件（由 Result Interpreter 生成），用于给用户展示智能化结果摘要。
- 当一次执行在某个 step 失败时，Runtime 会触发一次自动重规划（仅替换失败子图），默认不会从已完成步骤重跑。
- `interpreter.mode` 支持 `auto/llm/noop`：`auto` 会在 `planner.mode=llm` 时启用 `LlmResultInterpreter`，否则回退规则解释。

---

## 并发策略（Concurrency Policy）

ThreadRuntime 支持多种并发策略：

| 策略                  | 行为               |
| ------------------- | ---------------- |
| InterruptAndStartNew | 中断当前 Interaction |
| Reject              | 拒绝新输入            |
| Queue               | 排队等待             |
| Parallel            | 并行执行             |
| MergeIntoRunning    | 合并为同一任务          |

```rust
use orchestral_runtime::{ConcurrencyPolicy, ConcurrencyDecision, RunningState};
use orchestral_stores::Event;

struct CustomPolicy;

impl ConcurrencyPolicy for CustomPolicy {
    fn decide(&self, running: &RunningState, _event: &Event) -> ConcurrencyDecision {
        if running.active_count >= 2 {
            ConcurrencyDecision::reject("Too many active interactions")
        } else {
            ConcurrencyDecision::Parallel
        }
    }
}
```

---

## 中断支持（Cancellation）

ActionContext 携带 `CancellationToken`，支持协作式取消：

```rust
use orchestral_core::prelude::*;

async fn run(&self, input: ActionInput, ctx: ActionContext) -> ActionResult {
    // Check if cancelled
    if ctx.is_cancelled() {
        return ActionResult::error("Action was cancelled");
    }

    // Or await cancellation
    tokio::select! {
        _ = ctx.cancelled() => {
            return ActionResult::error("Action was cancelled");
        }
        result = do_work() => {
            return result;
        }
    }
}
```

---

## 系统分层总览

```
[ UI / API ]
      ↓
[ Runtime (Thread / Interaction / Concurrency) ]
      ↓
[ Core (Planner / Normalizer / Executor) ]
      ↓
[ Stores / Actions ]
```

---

## 一句话总结（One-liner）

> **Orchestral is not a chatbot framework.**
> **It is a general-purpose interaction runtime that turns intent into durable, resumable execution.**
