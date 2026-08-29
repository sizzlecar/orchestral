# Tool Artifact v1

状态：stable Tool Runtime v1 contract。

Tool Artifact 是 `GuardedToolRuntime` 的统一大结果边界，不属于 MCP transport。内置 Tool、Workflow Tool 和 MCP Tool 使用同一条路径。

```text
GuardedToolExecutor returns ToolOutput::Inline
  → validate original Tool output schema
  → canonical JSON serialization
      ├─ <= Host inline limit → keep Inline
      └─ >  Host inline limit
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
7. `max_output_bytes` 是模型上下文的 inline 阈值；`ToolArtifactStore::max_artifact_bytes` 是独立硬上限。

## 存储与读取

`orchestral-blob-fs` 是 durable、content-addressed 文件插件，Blob ID 等于内容 SHA-256。data 与 metadata 使用同目录临时文件、fsync 和原子 rename 提交；CLI 的 `blobs.mode: local` 默认接入该插件。

模型可调用只读 `artifact_read` Tool，携带返回结果中的 `artifact_ref/digest/media_type/byte_size`，按 UTF-8 byte offset 分块读取。每次读取仍经过 Tool policy，并重新验证完整 Artifact。

```yaml
blobs:
  mode: local
  local:
    root_dir: .orchestral/blobs
```

当前限制：只 spill `application/json` Tool result；远程 BlobStore 由外部 plugin 实现，不属于
Foundation 内置交付。
