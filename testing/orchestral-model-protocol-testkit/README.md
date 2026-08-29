# Model Protocol v1 Conformance

`ModelBackend` is Orchestral's provider-neutral boundary for one model request. It is deliberately
smaller than an Agent: it has no Run lifecycle, Session ownership, Tool authority, approvals,
goal semantics, or recovery policy.

## Contract

A backend exposes an immutable `ModelDescriptor` and implements:

```rust
async fn start(
    &self,
    request: ModelRequest,
    cancellation: CancellationToken,
) -> Result<ModelStream, ModelError>;
```

The canonical request owns model messages, Tool definitions, optional output schema, output-token
limit, and namespaced extensions. Provider credentials, endpoints, pricing, and transport settings
remain Host configuration and never enter model-visible Tool arguments.

Every successful stream must satisfy all of these invariants:

1. `request_id` is unchanged and `sequence` starts at 1 with no gaps or duplicates.
2. Text and Tool-call fragments preserve their original order and identity.
3. A Tool call has one start, zero or more argument deltas, and one end.
4. `Finish` occurs exactly once and is the final successful event.
5. A malformed provider stream returns a structured `ModelError`; it is not repaired into a
   successful finish.
6. Cancellation before dispatch performs no request. Cancellation during connect/read terminates
   the stream with `ModelErrorCode::Cancelled`; late provider data is not accepted.
7. The stream is pull-based. Dropping it does not replace cancellation; adapters must observe the
   supplied root token and stop their underlying request.

`ModelFinishReason::ToolCalls` is the canonical reason when a normally completed response contains
Tool calls, even when a provider family reports its generic stop reason.

## Running the shared suite

Concrete adapters supply only family-specific wire fixtures and backend construction. The suite
runs the same seven cases for descriptor validity, invalid-request fail-fast, text reconstruction,
Tool lifecycle and JSON arguments, malformed-stream fail-closed, cancellation before start, and
cancellation during a live stream.

```rust
let report = ModelConformanceSuite::default().run(&fixture).await;
assert!(report.is_conformant(), "{:#?}", report.results());
```

OpenAI-compatible, Gemini Native, and the deterministic Scripted Fake all execute this suite.
Adapter-specific fragmentation and 10,000-delta tests remain additional transport gates; they do
not replace the shared contract.
