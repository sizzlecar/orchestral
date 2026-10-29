# Session Context and Recall v1

状态：Session journal wire schema 保持兼容；projection、压缩与计量策略由恢复配置 digest 绑定。

## 原始记录与压缩

Session journal 保存原始用户输入、完整 Tool call/result 对和输出。压缩追加摘要事实，
不覆盖原始记录。摘要的 `source` 与 `source_digest` 继续绑定被替代的直接生产记录。
再次压缩时，runtime 沿已验证的压缩引用回到原始记录，按 `session_seq` 去重、排序，
再交给 summarizer；不会把摘要文本当作新的原始事实。

`SessionCompactionInput.source` 表示本次替代的生产记录范围。`groups` 可以引用该范围
之前的原始记录，来源必须能通过 journal 的向后引用到达。每个 Tool exchange 仍是
一个完整 group。已有 summarizer SPI 消费者应遵守这一来源语义。

当前 Run 的用户输入、有效 Skill 和安全事实继续遵守原有保留规则。Artifact 所在的完整
Tool exchange 可以参与压缩，不再仅因存在引用而永久固定在上下文中；访问权限仍由原始
Session journal 与 Tool Effect journal 校验，摘要中的引用不能授予访问权。
跨 Run 续聊时，如果最近一条历史用户输入已被压缩，projection 会在
history/token 预算内优先恢复它的原文及 User 角色；不能容纳时仍可回查原始记录。
这一规则不把所有旧用户要求永久固定，也不承诺有损摘要保存全部细节。

内置 extractive summarizer 从类型化 ToolResult 保留失败状态，并在大参数之前呈现
有界错误结果。Tool 成功只表示该调用的结果，不代表用户任务已经验证完成。摘要明确
标注为历史材料，不能提升为系统权限。修改 projection、压缩策略或 summarizer 的合同
会改变恢复配置 digest；已有不兼容的未完成 Run 明确拒绝恢复，已完成历史仍可读取。

压力压缩使用与 projection 相同的完整请求计量，包含系统上下文和工具定义。严格预算
继续遵守 `UpperBound`，不得通过压缩丢弃必需事实。使用 `Planning` 时，删去可压缩记录
也可能删去已观测的请求前缀，使估算退回更大的原始值；这个子集估算不能作为所有候选的
下界来提前拒绝压缩。runtime 会计量保留前缀的完整候选，只提交严格变小的结果，且不会
拆开 Tool call/result 对。Artifact 原始生产记录仍保留在 journal 中。若预算仍不能满足，
继续有限压缩或明确失败。
此行为通过 `context_pressure_contract = capacity-aware-preserved-tail/v4` 绑定恢复配置。

软工作集水位默认来自模型的有效输入预算：高水位为其 90%，低水位为高水位的 80%。
`agent.compaction.working_set_tokens` 可指定更小的软偏好，不改变硬模型容量或 Run 限额。
仅在实际选中的工作集超水位时同步维护历史。`deferred_ranges` 表示因历史记录数或历史
token 分配而未选中的来源，不代表容量压力，不能单独触发模型摘要。恢复长会话后的第一轮
请求也遵守这一条件；容量充足时直接调用回复模型，不先遍历整理旧历史。维护进度展示
本轮计量值与触发水位。
软压缩保留 `keep_recent_records` 个最近原始交换；跨 Run 历史的保留尾部不因当前 Run
新增记录、摘要记录或大输出而缩短。硬溢出才允许尝试缩小该尾部，并对完整候选重新计量。
历史投影按剩余空间和输入预算的四分之一分配，不再有固定 24K 上限；不能容纳的记录仍
以 `deferred_ranges` 标识并可回查。

CLI 默认使用 `model_assisted` 生成语义工作摘要，`deterministic_extractive` 仍可显式选择。
语义摘要使用完整原始文字与任务焦点、模型容量和 provider token meter 进行预检；图片数据
与不透明续接状态不会被当成文字证据。模型必须返回完整、非空且在字数预算内的 Stop 结果；
超长、Length、缺失 Finish、错误序列或超时不能伪装为成功摘要。完整请求与收流受
`agent.compaction.model_timeout_secs` 约束，默认 120 秒。

`SessionSummaryGeneration` 报告实际摘要策略、版本、配置摘要和可选的稳定 fallback 原因。
软维护不能得到合格语义摘要时保留原始记录，不提交 extractive 回退；有界的进程内缓存按
Run 和来源抑制重复软尝试，避免每轮反复等待同一失败请求。硬压力可使用明确标记的 extractive 回退，
提交的策略使用实际 descriptor，并附加 `/fallback:<reason>`；不把它标成模型语义摘要。
Run 设置累计 input/output token 或 cost 上限时，Generic Agent 不调用可能另发模型请求的
摘要器，以免绕过用量记账；确定性摘要仍可用，无法满足硬预算时明确结束。

已被摘要替代的中间日志记录不会阻断相邻有效片段的再次合并。`source` 仍是连续的原始日志
范围，`source_digest` 绑定该范围的全部记录；两端必须是当前有效的生产记录，范围内的有效
group 必须来自同一 Run 的完整、可压缩工具交换或其摘要。展开原始来源时沿向后引用去重，
不会把旧记录再次计入模型上下文。用户修正、Skill、安全事实，以及范围外
仍有效片段的逻辑位置都不能被跨越。合并前的历史游标仍重放原有上下文；提交和重启回放采用
相同检查。该投影规则通过 `context_projection_contract = source-positioned-summary/v6` 绑定。

Journal 中的 Assistant 文本摘要在模型投影时转换为 User 角色的历史上下文，携带
`application/vnd.orchestral.session-summary+json` 类型标记及“不是新指令”的说明。
这同样适用于旧 Session 的摘要回放，避免压缩最新 Tool exchange 后留下模型续写前缀。
摘要仍在原始历史位置，不改写 journal，不伪造新的用户请求或 Tool 执行结果。
已有 System 摘要保持兼容。压力计量包含完整投影包装；重复压缩仍读取原始交换。

前缀及工具定义保持不变时，类型化历史摘要可以作为新观察参与实测用量增量估算；普通
User/Steer、System 更新仍使该估算退回全量。此规则仅用于规划，不改变硬预算和权限，
通过 `observed_prefix_planning_contract = completed-input-prefix/v2` 绑定恢复配置。

内置 `file_read` 的模型输出 v2 保留 workspace、path、revision、行范围、EOF、next_offset
和截断事实，包括完整文件与空文件。summarizer 从原始交换生成完整 JSON 检查记录，按
workspace/path/revision 合并已完整返回的行区间；被截断的行不算完整读过。不同 revision
不得合并，失败读取保留错误码和请求 offset。记录预算不足时保留来源序号的省略索引，
不截断路径、引用、游标或 JSON，也不把“读过”当成当前文件内容仍然有效的保证。

extractive summarizer v8 在小预算下优先保留符合 Artifact 分页输出合同的完整引用、
`next_offset` 和 `complete`，与来源序号和工具状态一起组成不可拆分的观察记录。该观察
不能容纳时，不将它截成残缺游标或引用；原始记录仍可回查。错误交换不会被一个成功分页观察遮蔽。
摘要是历史数据，不能当作新权限或任务验收结论。

## 受控历史回查

CLI Host 注册 `orchestral/session_read/v1`，模型可见名称为 `session_read`。SDK Host 可使用
`GuardedSessionReadExecutor` 自行注册，并显式授予 `EffectScope::SessionRead`。

- Session 身份从调用 Run 的 Host registration 解析，不接受模型指定 Session 或日志路径。
- 查询与读取经过统一 GuardedToolRuntime、Run grant、取消、输出预算和 Tool Effect journal。
- `query` 是对记录的可读投影 canonical JSON 的大小写不敏感字面搜索。摘要不参与搜索，避免
  同一来源以摘要形式重复命中；按 `session_seq` 仍可读取任何记录。
- `after_seq` 与返回的 `next_after_seq` 是排他分页游标。首次返回 `through_seq`；后续
  复用它固定历史前缀，新追加记录不会改变这一查询范围。
- 搜索结果包含原始序号、Run、事件类型、完整记录 digest 和明确标记截断的 preview。
- 使用 `session_seq` 读取记录的可读投影，或用 RFC 6901 `json_pointer` 精确选择字段。
  `content` 是该投影 canonical JSON 的 UTF-8 分块；按 `next_offset` 重组后再解析 JSON。
- 搜索、preview、整条读取与字段读取使用同一投影。Provider `Continuation`、Tool call 的
  私有 `extensions` 和图片二进制数据不向模型开放。隐藏的内容槽位保留为 `null`，不改变
  可读字段的数组索引；直接选择隐藏字段或其子字段会拒绝，读取父字段也不会泄漏它们。
  普通用户 JSON 与 Tool result 不按字段名称猜测过滤。原始 journal 不被改写，返回的
  digest 仍标识完整原始记录，不是可读投影的哈希。
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
字段读取、Unicode 分块、私有续接隔离和提交结果重放。Session context 单元与属性测试验证原始来源、
预算、用户修正、失败状态，以及 Tool exchange 原子性。

`apps/orchestral-cli/tests/agent_live_e2e/context_reliability.rs` 实际启动 CLI/TUI，使用受控
HTTP 模型连续触发小上下文压缩，再验证跨进程续聊、待输入时 kill/restart、原始结果回查、
新约束传递和文件操作不重复。它验证运行时边界；真实模型语义另由 opt-in live tests 验证。
