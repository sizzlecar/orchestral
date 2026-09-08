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
  按临时不可用处理。任何已观察事件（包括 Usage 或 ToolCallStart）都会关闭自动重试窗口。
- 取消、Steer 和 Run deadline 可以打断请求或退避等待；被丢弃的尝试会取消底层 stream。
- Run 设置累计 token/cost 上限时，只自动重试明确的限流拒绝。其他未观察到用量的失败
  请求可能已消耗额度，不能通过自动重试绕过累计预算。
- 每次退避前持久化 `GenericCheckpointEvent::ModelRetryScheduled`。它必须关联当前
  `ModelAttemptOpen`，重试序号从 1 连续递增，不产生第二个顶层终态。
- 该 checkpoint 只记录调度事实，不授权进程重启后重发请求。恢复仍将未闭合模型尝试
  收束为 `RunIncomplete(Interrupted)`；重试策略和项目指令快照均进入恢复配置摘要。

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
