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

默认测试不访问网络。显式提供 endpoint、model 和 key 后可验证两个 production Adapter 的
真实协议/wiring；任务质量单独观察，不作为形式化正确性证明。

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

仓库的 `Live Model Smoke` 手动 workflow 运行同一组 ignored tests；触发前必须配置
`OPENAI_API_KEY` 与 `GOOGLE_API_KEY` repository secrets，并在 dispatch 输入中显式指定
两个 endpoint 与 model。缺少任一配置时 workflow 必须失败，不能将 skip 记作通过。
