# Model Protocol v1

状态：stable provider-neutral contract。

Model Protocol 是一次模型请求的最小边界。`ModelBackend` 不拥有 Agent Run、Session、Tool
执行、审批或目标语义；这些职责属于 Generic Agent 与 Host。

## 合同

```text
ModelDescriptor
  + ModelRequest(messages, tools, output limit, cancellation)
  → ModelStreamEvent(sequence)
      TextDelta | ReasoningSummaryDelta | ToolCall* | Usage | Finish
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
  按临时不可用处理。文字、思考摘要、ToolCallStart 或 Finish 会关闭自动重试窗口。只报告 Usage
  的失败尝试可以重试，最后一份用量快照持久化并计入 Run 一次。
- 取消、Steer 和 Run deadline 可以打断请求或退避等待；被丢弃的尝试会取消底层 stream。
- Run 设置累计 token/cost 上限时，只自动重试未报告用量的明确限流拒绝。其他失败请求
  可能已消耗额度或超出最后一份快照的用量，不能通过自动重试绕过累计预算。
- 每次退避前持久化 `GenericCheckpointEvent::ModelRetryScheduled`。它必须关联当前
  `ModelAttemptOpen`，重试序号从 1 连续递增，不产生第二个顶层终态。
- 该 checkpoint 只记录调度事实，不授权进程重启后重发请求。恢复仍将未闭合模型尝试
  收束为 `RunIncomplete(Interrupted)`；重试策略和项目指令快照均进入恢复配置摘要。
- 等待提示显示当前 attempt 与整个逻辑请求累计时间，累计时间包含之前的失败尝试及退避，
  不会在重试、收到 Usage 或进入流读取阶段后归零。错误详情保留 `attempt_number`、
  `max_attempts`、`attempt_elapsed_ms` 和 `logical_request_elapsed_ms`，随重试 checkpoint
  或最终失败持久化。开始输出后仍可报告等待，但不会重新执行推理。

## 模型发现与推理控制能力

`ReasoningPreference` 是模型无关的用户设置。`default` 表示省略原生控制字段；`on/off`
是思考开关，其他值是推理强度。厂商字面量名称通过 `Custom` 保留，不能把名称为 `on`
的 effort 误当作开关。

`ModelBackend::reasoning_capabilities()` 返回本地可用的 `ModelReasoningCapabilities`；
`resolve_reasoning_capabilities()` 可查询服务声明，但不得生成模型输出或改变 execution
descriptor、token meter 和 Run 的恢复身份。`supported_preferences: None` 表示未知，
空列表表示明确没有额外选项；`default` 始终隐含可用，不在列表中重复。

`validate_reasoning_preference()` 无需联网，检查适配器能否编码显式设置。缺省实现接受
`default` 和本地已声明选项；支持未声明设置的适配器可交给服务最终校验。能力查询、
认证与原生参数转换均归适配器所有。CLI/TUI 只消费中立合同，不按厂商选择推理路径。
新增适配器可以保留未知能力和默认设置，按需实现能力查询、校验和原生参数编码。

`ModelBackend::list_models()` 可返回可选择的模型 ID；`None` 表示不提供发现能力，空列表
表示服务当前没有模型。Host 可在不支持发现时展示配置中的 profiles，服务查询失败仍显式
报错。模型发现与推理能力查询都不得触发生成请求。

## OpenAI Responses 与兼容服务

OpenAI 适配器支持 `OpenAiApi::{Responses, ChatCompletions}`；原生 wire 格式仍留在
plugin 内。官方服务默认走 Responses，其他服务默认走 Chat Completions。完整 resource
URL 或 backend `config.api` 显式选择协议；客户端规范化 URL 时保留该选择。

Responses 使用 `store: false`，将完整原生 output 数组作为 Assistant 的
`openai-compatible/responses-output/v1` Continuation 保留。重放原始 reasoning、message、
function_call 顺序及加密内容，不重复编码同一条 canonical 文本或工具调用。canonical 工具
参数与原生调用必须一致，结果使用原始 `call_id`；执行身份仍按 request ID 稳定隔离。
只有 completed/incomplete terminal response 才提交 Continuation 和 Finish；截断、取消和
失败不能将流中已出现的工具片段提交为完成。usage 与推理摘要分别映射为标准事件。

Continuation 仅用于适配器续接。TUI 恢复历史只显示 canonical 可读内容，不把原生 output
数组再次渲染为回答；Session 回查也不开放其私有字段。完整续接仍保留在 journal 中。

两种 OpenAI API 的软上下文规划都将图片 Base64 视为传输数据，按解码后的图片尺寸估算
图像开销（32×32 patches，每 patch 3 个规划 token），而不把编码字符串当正文。该估算不
声明任何模型容量或精确视觉 token 数，不按模型名称选择算法；服务的缩放与计量仍可能不同。
估算最多采用现有保守 wire 上界，实测 usage 可继续校准未改变的请求前缀。严格 input/cost
预留仍使用保守 wire 计数，发送的原图不变；计量身份包含算法版本以保护恢复一致性。

工具声明在 adapter 内转换为 API 接受的对象投影，不改变 canonical schema 或 Host 权限。
Host 的组合约束校验保留 `allOf`、`anyOf`、`oneOf`、`not`；具体工具仍执行自己的参数与
效果校验。协议、schema 编码版本和工具结果编码均参与恢复身份与输入计量。

`ModelBackend::reasoning_preference_candidates()` 只声明适配器能发送的候选值，不声明
模型支持。能力元数据缺失时，客户端可显示这些候选并明确标注未验证；服务明确声明的列表
优先，包括显式空列表。该 SPI 默认返回空列表，不影响其他适配器。

## 模型容量发现与 Host 预算

`ModelBackend::resolve_descriptor()` 是可选的异步启动阶段：静态适配器直接返回
`descriptor()`；远端适配器发现元数据后，在该 Backend 生命周期内固定 descriptor，
再构建 Agent 和恢复配置 digest。推理请求不反复刷新模型能力。

CLI 的 `agent.max_context_tokens` 是可选 Host 上限。省略时采用模型声明容量；显式
设置时取 Host 与模型上限的较小值。两者均未知时仍采用 128K Host 回退预算，并明确
显示模型容量未知，不能将回退预算报告为模型原生容量。

`ModelCapabilities.max_output_tokens` 声明单次生成的输出上限，未知时省略。CLI 的
`agent.reserved_output_tokens` 和匹配当前模型的 profile `max_tokens` 都是可选上限；
省略时采用模型元数据中的输出容量，显式限制则取较小值。两者均未知时使用标明来源的
8192 Host 回退值。直接切换 backend/model 不继承其他模型 profile 的限额或策略。
Gemini 的输出用量包含回答与思考 token；`MAX_TOKENS` 即使没有可见回答也转换为
`Finish(Length)`，保留 Usage，由运行时收束为 `RunIncomplete(OutputTokens)`，不重试。

Gemini CLI 默认请求 `includeThoughts`，不覆盖模型默认思考强度。Provider 明确返回的
`thought: true` 文字转换为 `ReasoningSummaryDelta`，不进入回答、Session 消息或模型
续接上下文；`thoughtSignature` 仍作为不透明的 Provider 扩展保留。SDK 使用
`with_thought_summaries(true)` 显式启用，CLI 自定义网关可设置 `include_thoughts: false`。

Gemini 原始 function-call ID 仅在单次请求内有效。Adapter 以 request ID 和原始 ID
生成稳定执行身份，防止跨轮冲突；相同请求的重放与同轮重复 ID 仍保留相同身份。
原始 ID 与签名保存在扩展中，并原样回传 functionCall/functionResponse，不改变 Provider
续接协议，也不放宽 Tool Effect Journal 的冲突和未知效果保护。

CLI 中 Gemini 的容量解析只读取本地缓存或随包附带的目录快照，不等待联网。缓存位于
Host 配置目录的 `cache/model-metadata`；超过 5 分钟或缺失时，后台刷新先查询原生
模型信息（最多 5 秒），未返回 token 上限再查询目录（最多 30 秒）。刷新失败不阻塞
操作，过期缓存仍可使用。新数据写入缓存，供下次构建 Backend 使用；不会改变活动
Backend 的容量或恢复身份。没有缓存和目录记录时容量仍为 unknown，不猜测模型上限。

公开 Google/Vertex endpoint 按 Provider 和完整模型 ID 精确匹配 `models.dev`。
随包快照由 `python3 scripts/update_gemini_catalog.py` 从该 API 生成，只包含 Google
模型限制及来源、更新时间；自定义网关和自定义目录不套用这个公开快照。目录请求不携带
Google 凭证。`discover_model_capabilities: false` 或显式 backend `max_context_tokens`
可避免自动发现。诊断上下文显示容量来源。SDK 默认仍提供显式等待发现的模式；通过
`with_background_metadata_refresh()` 和持久缓存启用与 CLI 相同的非阻塞策略。

Gemini CLI 默认将 HTTP send 到收到响应头的总时间限制为 45 秒，可通过 Google backend 的
`response_header_timeout_secs` 调整。该范围包含连接建立、上传和响应头等待，不是纯服务端
排队或推理耗时；身份认证耗时单独记录。收到响应头后，`stream_idle_timeout_secs`（默认
300 秒）独立约束流中两次读取之间的等待，不限制生成总时长。首响应超时作为
`Unavailable` 进入现有有界重试策略，不重放已经执行的工具；它只能缩短挂起请求的
恢复等待，不能保证上游推理延迟。SDK 可显式调用 `with_response_header_timeout`。

模型请求、首个内容事件或流中后续事件等待超过 10 秒时，运行时以 ProgressReported
报告等待状态；收到后续内容时 UI 恢复内容展示。这一提示保留原有待完成读取，
不重发请求，不改变重试及取消语义。Gemini 错误详情记录 authentication、response_headers、
error_body 或 response_body 阶段及耗时；`response_headers` 名称保持兼容，
`send_scope = connect_upload_response_headers` 明确它覆盖整个 send。正文耗时从收到响应头起算。
正文错误另记录已收到的
字节数、是否收到正文内容及最后一次收到字节后的空闲时长，以区分整体耗时和流停顿。
诊断另记录 `request_id`、序列化后实际发送的 `request_bytes`、`inline_image_base64_bytes`
（图片 Base64 数据长度，不是解码大小）、消息和工具定义数量、`auth_elapsed_ms`、
`send_elapsed_ms`、`request_elapsed_ms` 与 `client_generation`。HTTP send 超时或失败后沿用
原有连接池重建策略，重建成功时 generation 增加；它提供关联证据，不证明故障由连接池或代理引起。
这些计数与计时进入错误详情和调试日志，不新增提示词、图片内容、工具参数或凭据记录。

Gemini 规划计数使用字节估算与运行时的实测 input usage 前缀校准；请求前缀、模型配置
或工具定义改变时放弃该校准。保守 JSON 字节上界继续用于严格 input/cost 预算。

## 上下文容量拒绝的恢复

`ModelErrorCode::ContextLengthExceeded` 表示后端在生成之前明确拒绝了输入与输出的容量
预留，不是普通参数错误，也不允许原样重发。OpenAI 适配器仅将 HTTP 400 且
`error.code = context_length_exceeded` 的响应归入此类，不解析错误消息中的关键词。
Ferrum 的上下文容量拒绝提供该结构化 code。

Host 可配置 `agent.context_recovery.max_retries`，默认 1，0 禁用。仅在没有收到任何
Usage、文本、工具调用或 Finish 时恢复。先将输出额度减半，最低为
`agent.context_recovery.minimum_output_tokens`（默认 1024，设为 null 可禁用输出缩减），
并保留必要输入；显式配置的 `minimum_output_reserve_tokens` 同样作为下限。
达到下限后，每次持久化一个严格小于被拒绝输入规划量的
预算上限，以原规划量的一半作为优先压缩目标。用已有上下文投影和压缩机制保留任务、
权限、技能和完整工具交换的边界；目标无法容纳必须保留的事实时，可在该上限内再尝试
压缩一次，不扩大 Run 累计预算。不可压缩内容仍超限时明确结束，不删除约束来制造空间。
它占用新的模型步骤，仍受 Run 的步骤、token、cost
和 deadline 限制；成功观察一轮生成后，仅重置连续容量拒绝计数和优先压缩目标，更小的
输入规划上限和输出额度继续约束同一 Run 的后续请求与重启恢复。它不是服务端物理容量的精确测量，
不能扩大累计预算；新 Run 使用自己的模型配置，不继承其他 Run 的经验值。

在重新投影前持久化 `ModelContextRejected`，记录请求身份、连续次数和输入/输出预算。
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

默认测试使用本地 fixture，不调用真实模型。显式提供 endpoint、model 和 credential 后可验证两个 production
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
