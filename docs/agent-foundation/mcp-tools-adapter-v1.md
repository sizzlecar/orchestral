# Orchestral MCP Tools Adapter v1

状态：stable Tools-only adapter contract；不是完整 MCP client。

`orchestral.mcp-tools/v1` 是 Orchestral 的 Tool Provider Adapter 版本，不是 MCP 官方协议版本。它把 MCP Tool 转成普通 `ToolDescriptor`，所有调用必须经过同一个 `GuardedToolRuntime → Host approval → ToolEffectJournal`。

## 兼容范围

| 能力 | v1 状态 |
| --- | --- |
| MCP `2026-07-28` stdio | 支持：`server/discover`、每请求 `_meta`、`tools/list` 分页、`tools/call`、取消通知 |
| legacy stdio | 支持：现代 discovery 失败后回退 `initialize/initialized`；接受 `2024-11-05` 至 `2025-11-25` |
| MCP `2026-07-28` Streamable HTTP | 支持：独立 plugin、单 POST、JSON/请求级 SSE、标准 routing headers、`x-mcp-header`、请求级取消 |
| Tool catalog | server-scoped 单进程、enabled/disabled 双重校验、namespace、schema/revision pin |
| Tool result | complete 与 `isError`；`input_required` 返回结构化 Unsupported，不伪装成功 |
| MCP Resources / Prompts / extensions | 不属于 v1 |
| `subscriptions/listen` / list-changed | 不属于 v1；当前 Run 使用固定 snapshot |
| 大结果 artifact spill | 支持；统一进入 Tool Artifact v1，模型上下文只保留 digest 引用与摘要 |

官方 `2026-07-28` 已移除 `initialize/initialized` 和协议 session，改为 stateless、self-contained request。runtime 通过同一个 transport SPI 驱动两条显式路径：

```text
Host-composed stdio or Streamable HTTP transport
  → server/discover (2026-07-28 metadata)
      ├─ supported → stateless requests (stdio or HTTP)
      └─ unsupported → legacy initialize (stdio only)
  → paginated tools/list
  → immutable snapshot + revision
  → namespaced ToolDescriptor
  → GuardedToolRuntime
  → exact Host approval
  → tools/call
```

## 不变量

1. workspace MCP 文件不会在新 Agent 路径中被隐式执行；v1 只接受显式 Host 配置。
2. 模型看不到 command、环境、approval 或 policy 字段，也不能扩大 ToolGrant。
3. 一个 stdio server 的多个 Tool 共用一个受控进程；异常连接被杀死并 reap。HTTP 没有协议 session，每个请求独立 POST。
4. 重连必须重新 discovery；transport binding、协议版本或任一 input/output schema digest 改变时拒绝调用。
5. 调用 dispatch 后取消、超时或断线记为 `UnknownEffect`，不自动 replay，不接受迟到结果；stdio 发送取消通知，HTTP 关闭当前 response stream。
6. stdio 子进程清空继承环境，只注入 Host 明确配置的键值。
7. HTTP endpoint、credential reference 和 `Network/SecretRead` effect 必须同时落入 Host policy；redirect 与环境代理关闭，credential 明文不进入 authority/debug/snapshot。
8. HTTP `x-mcp-header` 只接受沿 `properties` 静态可达的 string/integer/boolean；无效 annotation 只剔除对应 Tool，不能生成任意 header。
9. 超过 Host inline 阈值的结果由统一 Tool Runtime spill；MCP adapter 不能自行生成或绕过 Artifact 引用。

最小调用不直接使用 MCP client API：Host 按下方配置构建 transport，发现出的 Tool 会注册为
普通 `ToolDescriptor`；Agent 仍只调用 `AgentToolRuntime::invoke`。这保证 stdio 与 HTTP 不会
形成第二条执行入口。

## 最小配置

```yaml
mcp:
  enabled: true
  servers:
    - name: local-search
      required: true
      transport:
        type: stdio
        command: /absolute/path/to/server
        args: ["--stdio"]
        env: {}
      enabled_tools: [search]
      startup_timeout_ms: 15000
      tool_timeout_ms: 20000
    - name: remote-search
      required: true
      transport:
        type: streamable_http
        endpoint: https://mcp.example.com/mcp
        credential_headers:
          Authorization:
            env: MCP_AUTHORIZATION
        max_frame_bytes: 8388608
      enabled_tools: [search]
      startup_timeout_ms: 15000
      tool_timeout_ms: 20000
```

参考：[MCP 2026-07-28 Specification](https://modelcontextprotocol.io/specification/2026-07-28)、[Tools](https://modelcontextprotocol.io/specification/2026-07-28/server/tools)、[SEP-2575](https://modelcontextprotocol.io/seps/2575-stateless-mcp)。
