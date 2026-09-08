# Session Context and Recall v1

状态：Session journal wire schema 保持兼容；当前 projection / compaction binding 为 v2。

## 原始记录与压缩

Session journal 保存原始用户输入、完整 Tool call/result 对和输出。压缩追加摘要事实，
不覆盖原始记录。摘要的 `source` 与 `source_digest` 继续绑定被替代的直接生产记录。
再次压缩时，runtime 沿已验证的压缩引用回到原始记录，按 `session_seq` 去重、排序，
再交给 summarizer；不会把摘要文本当作新的原始事实。

`SessionCompactionInput.source` 表示本次替代的生产记录范围。`groups` 可以引用该范围
之前的原始记录，来源必须能通过 journal 的向后引用到达。每个 Tool exchange 仍是
一个完整 group。已有 summarizer SPI 消费者应遵守这一来源语义。

当前 Run 的用户输入、有效 Skill、安全事实和 artifact-bearing exchanges 继续遵守
原有保留规则。跨 Run 续聊时，如果最近一条历史用户输入已被压缩，projection 会在
history/token 预算内优先恢复它的原文及 User 角色；不能容纳时仍可回查原始记录。
这一规则不把所有旧用户要求永久固定，也不承诺有损摘要保存全部细节。

内置 extractive summarizer v2 从类型化 ToolResult 保留失败状态，并在大参数之前呈现
有界错误结果。Tool 成功只表示该调用的结果，不代表用户任务已经验证完成。摘要明确
标注为历史材料，不能提升为系统权限。修改 projection、压缩策略或 summarizer 的合同
会改变恢复配置 digest；已有不兼容的未完成 Run 明确拒绝恢复，已完成历史仍可读取。

## 受控历史回查

CLI Host 注册 `orchestral/session_read/v1`，模型可见名称为 `session_read`。SDK Host 可使用
`GuardedSessionReadExecutor` 自行注册，并显式授予 `EffectScope::SessionRead`。

- Session 身份从调用 Run 的 Host registration 解析，不接受模型指定 Session 或日志路径。
- 查询与读取经过统一 GuardedToolRuntime、Run grant、取消、输出预算和 Tool Effect journal。
- `query` 是对原始记录 canonical JSON 的大小写不敏感字面搜索。摘要不参与搜索，避免
  同一来源以摘要形式重复命中；按 `session_seq` 仍可读取任何记录。
- `after_seq` 与返回的 `next_after_seq` 是排他分页游标。首次返回 `through_seq`；后续
  复用它固定历史前缀，新追加记录不会改变这一查询范围。
- 搜索结果包含原始序号、Run、事件类型、完整记录 digest 和明确标记截断的 preview。
- 使用 `session_seq` 读取完整原始记录，或用 RFC 6901 `json_pointer` 精确选择字段。
  `content` 是 canonical JSON 的 UTF-8 分块；按 `next_offset` 重组后再解析 JSON。
- 单页最多 20 条，输出上限取 Host 预算与 16 KiB 的较小值。单记录内容可持续分块读取。
- 已提交的回查结果沿用 effect journal 缓存；恢复不会悄悄用更新后的历史替换已观察结果。

```json
{"query":"verification"}
```

```json
{"session_seq":42,"json_pointer":"/payload/tool/content/0/result","offset":0,"max_bytes":1024}
```

## 验证

`core/orchestral-runtime/tests/session_recall.rs` 验证权限、会话隔离、固定前缀分页、精确
字段读取、Unicode 分块和提交结果重放。Session context 单元与属性测试验证原始来源、
预算、用户修正、失败状态，以及 Tool exchange 原子性。

`apps/orchestral-cli/tests/agent_live_e2e/context_reliability.rs` 实际启动 CLI/TUI，使用受控
HTTP 模型连续触发小上下文压缩，再验证跨进程续聊、待输入时 kill/restart、原始结果回查、
新约束传递和文件操作不重复。它验证运行时边界；真实模型语义另由 opt-in live tests 验证。
