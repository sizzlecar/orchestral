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
├── orchestral-core/         # 核心抽象与确定性逻辑
├── orchestral-runtime/      # Thread Runtime + 并发/中断/调度
├── orchestral-stores/       # 各类存储实现（InMemory / Redis / DB）
├── orchestral-actions/      # 官方 Action 集（可选）
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
orchestral-core = { path = "orchestral-core" }
orchestral-runtime = { path = "orchestral-runtime" }
orchestral-stores = { path = "orchestral-stores" }
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
