# Guarded Tool Runtime v1

状态：stable Host effect boundary。

Tool Runtime 是所有副作用的 reference monitor。内置 Tool、Shell、PTY、Workflow Step 与
MCP Tool 都必须经过同一条路径；模型输出只是一份调用申请，不携带权限。

```text
Model ToolCall
  → resolve immutable ToolDescriptor
  → schema validation
  → HostToolPolicy ∩ RunToolGrant ∩ ToolRestriction
  → exact approval capability（如需要）
  → ToolEffectJournal fence
  → executor
  → schema validation / artifact spill
  → committed ToolOutcome
```

## 核心合同

- `ToolDescriptor`：稳定 Tool ID、模型可见 schema、effect scopes、restriction、幂等性。
- `HostToolPolicy`：进程级不可变权限上限。
- `RunToolGrant`：Host 为单 Run 分配的权限；不能超过 Host policy。
- `ApprovalCapability`：Host 签名并绑定 run/call/tool/args/scope/expiry 的一次性授权。
- `ToolEffectJournal`：`Prepared → Invoked → Observed → Committed | UnknownEffect`。
- `AgentToolRuntime`：Generic Agent 唯一可调用的 Tool 端口。

## 不变量

1. 模型参数不能包含或修改 policy、approval、sandbox、environment 或 credential authority。
2. schema 校验发生在执行前，输出 schema 校验发生在 commit 前。
3. `Invoked` 后丢失结果必须成为 `UnknownEffect`；未经 reconcile 不自动重放。
4. Cancel/timeout 必须贯穿 executor；Shell、PTY、MCP 子进程需要 kill + reap。
5. 大结果统一进入 [Tool Artifact v1](./tool-artifact-v1.md)，不能由 Adapter 绕过。
6. Hook failure policy 显式选择 fail-open/fail-closed；hook 本身不能扩大权限。

## 最小注册与调用

```rust
runtime.register(tool_descriptor, Arc::new(my_executor))?;

let result = runtime.invoke(
    ToolInvocation { run_id, call_id, tool_id, arguments },
    run_grant,
    None, // 只有 Host 可以在审批后放入 capability
    cancellation,
).await;
```

完整构造见 `core/orchestral-runtime/tests/tool_runtime.rs`；Generic Agent 接线见
`generic_agent_executes_model_tools_only_through_the_guarded_runtime`。
