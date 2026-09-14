# Model Protocol v1

状态：stable provider-neutral contract。

Model Protocol 是一次模型请求的最小边界。`ModelBackend` 不拥有 Agent Run、Session、Tool
执行、审批或目标语义；这些职责属于 Generic Agent 与 Host。

## 合同

```text
ModelDescriptor
  + ModelRequest(messages, tools, output limit, cancellation)
  → ModelStreamEvent(sequence)
      TextDelta | ToolCall* | Usage | Finish
```

核心类型位于 `orchestral_core::model_protocol`：

- `ModelBackend::descriptor/start`：模型族适配 SPI。
- `ModelRequest`：AI 中立消息、Tool schema、结构化输出与单请求输出上限。
- `ModelStreamEvent`：绑定 request ID 的严格递增序列。
- `ModelTokenMeter`：Provider-exact tokenizer 或保守的 Provider wire 上界。
- `ModelUsage`：跨模型族归一化的 input/output token 统计。

## 不变量

1. core contract 不出现厂商 SDK 类型、endpoint 参数或私有 stream chunk。
2. 每个 event 必须属于同一 request，sequence 从 1 严格递增，恰好一个 `Finish`。
3. Tool call 必须按 start → arguments delta → end 完整闭合。
4. cancellation token 必须停止底层 HTTP/stream；取消后的迟到 callback 不再被接受。
5. bounded stream queue 不能因慢消费者无限增长，也不能丢 terminal。
6. Adapter 的 token meter identity 进入 Generic Agent config digest，恢复时不能静默更换。
7. Run 级 token/cost 由 Generic Agent 累计；Adapter 只执行 Host 给出的本次请求上限。

## Generic Agent 的请求重试

Host 可通过 `agent.model_retry` 配置有限退避，默认最多重试 3 次。重试发生在同一个逻辑
模型步骤内，保留 request ID 和消息快照，不重复提交 Session 输入或执行已完成的 Tool。
Adapter 继续负责将厂商错误归一化为 `ModelError`，无需理解重试次数或 Run 状态。

- 仅 `retryable = true` 的 `RateLimited` / `Unavailable` 可以重试；首次事件之前的空流
  按临时不可用处理。文字、ToolCallStart 或 Finish 会关闭自动重试窗口。只报告 Usage
  的失败尝试可以重试，最后一份用量快照持久化并计入 Run 一次。
- 取消、Steer 和 Run deadline 可以打断请求或退避等待；被丢弃的尝试会取消底层 stream。
- Run 设置累计 token/cost 上限时，只自动重试未报告用量的明确限流拒绝。其他失败请求
  可能已消耗额度或超出最后一份快照的用量，不能通过自动重试绕过累计预算。
- 每次退避前持久化 `GenericCheckpointEvent::ModelRetryScheduled`。它必须关联当前
  `ModelAttemptOpen`，重试序号从 1 连续递增，不产生第二个顶层终态。
- 该 checkpoint 只记录调度事实，不授权进程重启后重发请求。恢复仍将未闭合模型尝试
  收束为 `RunIncomplete(Interrupted)`；重试策略和项目指令快照均进入恢复配置摘要。

## 上下文容量拒绝的恢复

`ModelErrorCode::ContextLengthExceeded` 表示后端在生成之前明确拒绝了输入与输出的容量
预留，不是普通参数错误，也不允许原样重发。OpenAI 适配器仅将 HTTP 400 且
`error.code = context_length_exceeded` 的响应归入此类，不解析错误消息中的关键词。
Ferrum 的上下文容量拒绝提供该结构化 code。

Host 可配置 `agent.context_recovery.max_retries`，默认 1，0 禁用。仅在没有收到任何
Usage、文本、工具调用或 Finish 时恢复。每次持久化一个严格小于被拒绝输入规划量的
预算上限，以原规划量的一半作为优先压缩目标。用已有上下文投影和压缩机制保留任务、
权限、技能和完整工具交换的边界；目标无法容纳必须保留的事实时，可在该上限内再尝试
压缩一次，不扩大 Run 累计预算。不可压缩内容仍超限时明确结束，不删除约束来制造空间。
它占用新的模型步骤，仍受 Run 的步骤、token、cost
和 deadline 限制；成功观察一轮生成后，仅重置连续容量拒绝计数和优先压缩目标，更小的
输入规划上限继续约束同一 Run 的后续请求与重启恢复。它不是服务端物理容量的精确测量，
不能扩大累计预算；新 Run 使用自己的模型配置，不继承其他 Run 的经验值。

在重新投影前持久化 `ModelContextRejected`，记录请求身份、连续次数和更小的输入预算。
该事件把明确被拒绝的尝试关闭为稳定边界，重启可从新边界继续，不能重复之前的工具。
没有此记录的开放请求仍属于结果未知；收到 Usage/内容后发生的错误也不能走这条恢复路径。

## 最小 Adapter

```rust
#[async_trait::async_trait]
impl ModelBackend for MyBackend {
    fn descriptor(&self) -> ModelDescriptor { self.descriptor.clone() }

    async fn start(
        &self,
        request: ModelRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        // 将厂商 stream 归一化为有序 ModelStreamEvent，并监听 cancellation。
        Ok(self.open_normalized_stream(request, cancellation).await?)
    }
}
```

可运行 fake 见 [`examples/agent_session.rs`](../../examples/agent_session.rs)；Adapter 必须通过
`testing/orchestral-model-protocol-testkit`。当前 production family 是 OpenAI-compatible 与
Gemini Native。

## Opt-in live smoke

默认测试不访问网络。显式提供 endpoint、model 和 credential 后可验证两个 production
Adapter 的真实协议/wiring；任务质量单独观察，不作为形式化正确性证明。Gemini Native
支持 Developer API 的 API key，也支持 Vertex AI 的短期 OAuth Bearer token。

```bash
ORCHESTRAL_LIVE_MODEL_SMOKE=1 \
ORCHESTRAL_OPENAI_LIVE_ENDPOINT=https://your-endpoint/v1 \
ORCHESTRAL_OPENAI_LIVE_MODEL=your-model \
OPENAI_API_KEY=... \
cargo test -p orchestral-model-openai --test live_smoke -- --ignored --nocapture

ORCHESTRAL_LIVE_MODEL_SMOKE=1 \
ORCHESTRAL_GEMINI_LIVE_ENDPOINT=https://your-gemini-endpoint/v1beta \
ORCHESTRAL_GEMINI_LIVE_MODEL=your-model \
GOOGLE_API_KEY=... \
cargo test -p orchestral-model-gemini --test live_smoke -- --ignored --nocapture
```

使用 Google Cloud service account 时，不把 credential 文件交给 Adapter；先在调用进程中
换取短期 token，再分别验证原生 Gemini 与 Google 的 OpenAI-compatible endpoint：

```bash
export GOOGLE_OAUTH_ACCESS_TOKEN="$(
  CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE=/path/to/service-account.json \
  gcloud auth print-access-token --quiet
)"

ORCHESTRAL_LIVE_MODEL_SMOKE=1 \
ORCHESTRAL_GEMINI_LIVE_ENDPOINT="https://aiplatform.googleapis.com/v1/projects/$PROJECT_ID/locations/global/publishers/google" \
ORCHESTRAL_GEMINI_LIVE_MODEL=gemini-3.1-pro-preview \
cargo test -p orchestral-model-gemini --test live_smoke -- --ignored --nocapture

ORCHESTRAL_LIVE_MODEL_SMOKE=1 \
ORCHESTRAL_OPENAI_LIVE_ENDPOINT="https://aiplatform.googleapis.com/v1/projects/$PROJECT_ID/locations/global/endpoints/openapi" \
ORCHESTRAL_OPENAI_LIVE_MODEL=google/gemini-3.5-flash \
OPENAI_API_KEY="$GOOGLE_OAUTH_ACCESS_TOKEN" \
cargo test -p orchestral-model-openai --test live_smoke -- --ignored --nocapture

unset GOOGLE_OAUTH_ACCESS_TOKEN
```

2026-08-27 的 M5.4 验收已实际执行上述两条协议链路：Gemini Native 使用
`gemini-3.1-pro-preview`，OpenAI-compatible 使用 `google/gemini-3.5-flash`；两项均为
`1 passed, 0 failed`。credential、私钥和 token 均未进入仓库或测试输出。

仓库的 `Live Model Smoke` 手动 workflow 运行同一组 ignored tests；触发前必须配置
`OPENAI_API_KEY` 与 `GOOGLE_API_KEY` repository secrets，并在 dispatch 输入中显式指定
两个 endpoint 与 model。缺少任一配置时 workflow 必须失败，不能将 skip 记作通过。
