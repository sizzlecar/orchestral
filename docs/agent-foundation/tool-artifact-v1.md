# Tool Artifact v1

状态：stable Tool Runtime v1 contract。

Tool Artifact 是 `GuardedToolRuntime` 的统一大结果边界，不属于 MCP transport。内置 Tool、Workflow Tool 和 MCP Tool 使用同一条路径。

```text
GuardedToolExecutor returns ToolOutput::Inline
  → validate original Tool output schema
  → canonical JSON serialization
      ├─ canonical result fits policy AND model view fits Host budget → keep Inline
      └─ either boundary exceeded
           → enforce independent Artifact hard limit
           → BlobStore write
           → verify size/media-type/checksum metadata
           → ToolOutput::Artifact(ref + SHA-256 + size + summary)
           → Effect Journal commit
           → Session Journal ToolResult
           → model context sees only reference + summary
```

## 合同

```rust
pub enum ToolOutput {
    Inline(serde_json::Value),
    Artifact(ToolArtifact),
}

pub struct ToolArtifact {
    pub artifact: ArtifactRefWithDigest,
    pub media_type: String,
    pub byte_size: u64,
    pub summary: String,
}
```

不变量：

1. Executor 不能直接提交 Artifact；伪造引用会被 Runtime 拒绝。
2. 原始 Inline 结果必须先通过 Tool 的 `output_schema`，spill 不能绕过 schema。
3. Artifact bytes 是原始结果的 canonical JSON；引用同时绑定 SHA-256 和 byte size。
4. `ToolEffectJournal`、`AgentSessionJournal` 和模型上下文只持久化引用与有界摘要，不复制大 payload。
5. Resolver 不信任 BlobStore metadata；读取后重新校验 id、media type、size 和 SHA-256。
6. 非幂等 Tool 已完成、但 Artifact 无法持久化时返回 `UnknownEffect`，不会自动重放副作用。
7. Tool policy 的 `max_output_bytes` 检查 canonical 结果；Host 配置的 `ToolArtifactStore::inline_output_limit()` 检查执行器声明的模型投影。两者分别满足时保留 Inline，避免仅因模型不可见的审计字段而转存。模型限额不缩小 Executor 的采集范围；`ToolArtifactStore::max_artifact_bytes` 是独立存储硬上限。
8. Artifact 页按完整 canonical JSON 的字节数计入 inline 限额，包括转义和续读元数据。非末页必须在 UTF-8 边界前进，不能把读取结果再次 spill 成另一层 Artifact。预算连元数据与一个字符都放不下时明确拒绝。
9. 配置了模型限额时，Artifact 引用加摘要的完整模型结果也必须满足较小的 policy/model 限额；仅缩短摘要，引用、摘要校验值和原始字节不变。连最小引用都无法容纳时明确失败，非幂等工具仍按 UnknownEffect 处理。

## 应用默认预算

CLI、TUI 和 `serve` 共用的 Host 根据模型声明及用户配置中较小的 context window，减去预留输出 token，取剩余数值的四分之一作为单个 inline 结果的**字节**限额。12,288 context / 4,096 reserved output 对应 2,048 bytes。它给请求、历史和其他工具结果留出空间，是单结果策略，不是整段会话的精确 token 计数或容量保证。

`tools.max_inline_output_bytes` 可设置显式正整数；省略或 `null` 使用上述默认策略。它与 `tools.max_output_bytes` 分离：后者仍约束命令采集等执行行为。超过 inline 限额的完整已采集结果进入 Artifact，原始字段和退出码保留，模型先得到引用及摘要，再按需读取。

## 存储与读取

`orchestral-blob-fs` 是 durable、content-addressed 文件插件，Blob ID 等于内容 SHA-256。data 与 metadata 使用同目录临时文件、fsync 和原子 rename 提交；应用的 `artifacts.backend: filesystem` 默认接入该插件。

CLI、TUI 和 `serve` 共用的 `artifact_read` 使用 `orchestral/artifact_read/v2`：模型只需提供 `artifact_ref`，可选 `offset` 和 `max_bytes`，按 UTF-8 byte offset 分块读取。Host 根据调用 Run 的已注册 Session，从该会话的已提交 Tool Exchange 和对应 Effect Journal 中取得原始 digest、media_type、byte_size，再验证实际存储字节。允许读取同一 Session 中先前 Run 的结果；其他 Session、未注册 Run、只有 JSON 外形或用户文本的引用不能授予读取权限。Blob 自报的摘要不能替代日志中的原始摘要，同一引用的已提交元数据冲突会拒绝读取。

SDK 仍保留 `guarded_artifact_read_descriptor` 和 `GuardedArtifactReadExecutor::new` 的 v1 显式元数据接口。v2 必须将 `guarded_artifact_read_v2_descriptor` 与 `GuardedArtifactReadExecutor::new_session_scoped` 配对注册，并传入与生产者共用的 Run、Session、Effect Journal。元数据来源和读取证据契约纳入规划身份；旧版本未完成 Run 不会跨契约静默恢复。每次新读取仍经过 Tool policy，并重新验证完整 Artifact；已提交的同一调用按 Effect Journal 重放。

`artifact_read` 返回的 `next_offset` 用于下一页，`complete=true` 表示读到末尾；负数 offset 或非正数 max_bytes 被拒绝，不能静默重置到起点。模型不必重跑产生结果的原始工具。

文件完整读取结果被转存后，运行时仅在当前模型请求中同时存在原始引用和覆盖完整内容的已提交页时，才能恢复该次 file_read 的版本证据。每页必须由显式实现 `GuardedToolExecutor::artifact_read_observation` 的执行器声明，且与 Effect Journal 中的结果及模型可见投影匹配；重组字节必须符合原引用的大小、SHA-256 和 canonical JSON，并再次通过原生产者的输出 schema/完整读取判定。缺页、改写、错误结果、其他 Run 的记录或当前批次尚未观察的结果不能授权替换。页的重复和乱序不改变已证明的内容。

这项证据不会扩大写权限，也不会刷新已经准备或执行的 file_write 版本前提；重启后从原有日志重建，重放不重新执行原命令或文件写入。

```yaml
tools:
  max_output_bytes: 1048576
  max_inline_output_bytes: null
artifacts:
  backend: filesystem
  root_dir: .orchestral/artifacts
  max_bytes: 67108864
  summary_max_chars: 512
```

当前限制：只 spill `application/json` Tool result；远程 BlobStore 由外部 plugin 实现，不属于
Foundation 内置交付。
