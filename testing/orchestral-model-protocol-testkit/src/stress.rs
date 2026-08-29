use futures_util::StreamExt;
use orchestral_core::model_protocol::{
    ModelError, ModelErrorCode, ModelEvent, ModelRequestId, ModelStream,
};

pub const DEFAULT_MODEL_STREAM_STRESS_CASES: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelStreamStressFault {
    None,
    MalformedFrame,
    ExtraAfterTerminal,
    BufferedBurst,
    CancelBeforePoll,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelStreamStressCase {
    index: usize,
    fault: ModelStreamStressFault,
}

impl ModelStreamStressCase {
    fn new(index: usize) -> Self {
        let fault = match index % 5 {
            0 => ModelStreamStressFault::None,
            1 => ModelStreamStressFault::MalformedFrame,
            2 => ModelStreamStressFault::ExtraAfterTerminal,
            3 => ModelStreamStressFault::BufferedBurst,
            _ => ModelStreamStressFault::CancelBeforePoll,
        };
        Self { index, fault }
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn fault(&self) -> ModelStreamStressFault {
        self.fault
    }

    pub fn request_id(&self) -> ModelRequestId {
        ModelRequestId::new(format!("model-stream-stress-{}", self.index))
    }

    pub fn text_fragments(&self) -> [String; 2] {
        [format!("stress-{}-alpha", self.index), "|omega".to_owned()]
    }

    pub fn expected_text(&self) -> String {
        self.text_fragments().concat()
    }

    pub fn max_buffered_events(&self) -> usize {
        if self.fault == ModelStreamStressFault::BufferedBurst {
            2
        } else {
            16
        }
    }

    pub fn cancel_before_poll(&self) -> bool {
        self.fault == ModelStreamStressFault::CancelBeforePoll
    }

    /// Deterministically splits one family-specific wire body across arbitrary
    /// byte boundaries, including inside UTF-8/JSON/SSE delimiters.
    pub fn split_wire(&self, wire: &[u8]) -> Vec<Vec<u8>> {
        if self.fault == ModelStreamStressFault::BufferedBurst {
            return vec![wire.to_vec()];
        }
        let mut seed = (self.index as u64)
            .wrapping_add(0x5EED_5EED_D15C_A11D)
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let mut chunks = Vec::new();
        let mut offset = 0usize;
        while offset < wire.len() {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let width = 1 + (seed % 97) as usize;
            let end = offset.saturating_add(width).min(wire.len());
            chunks.push(wire[offset..end].to_vec());
            offset = end;
        }
        chunks
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelStreamStressReport {
    adapter_name: String,
    total_cases: usize,
    successful_cases: usize,
    protocol_failures: usize,
    cancellations: usize,
    violations: usize,
    first_violation: Option<String>,
}

impl ModelStreamStressReport {
    pub fn adapter_name(&self) -> &str {
        &self.adapter_name
    }

    pub fn total_cases(&self) -> usize {
        self.total_cases
    }

    pub fn successful_cases(&self) -> usize {
        self.successful_cases
    }

    pub fn protocol_failures(&self) -> usize {
        self.protocol_failures
    }

    pub fn cancellations(&self) -> usize {
        self.cancellations
    }

    pub fn violations(&self) -> usize {
        self.violations
    }

    pub fn first_violation(&self) -> Option<&str> {
        self.first_violation.as_deref()
    }

    pub fn is_conformant(&self) -> bool {
        self.total_cases > 0
            && self.successful_cases + self.protocol_failures + self.cancellations
                == self.total_cases
            && self.violations == 0
    }
}

pub struct ModelStreamStressSuite {
    cases: usize,
}

impl Default for ModelStreamStressSuite {
    fn default() -> Self {
        Self {
            cases: DEFAULT_MODEL_STREAM_STRESS_CASES,
        }
    }
}

impl ModelStreamStressSuite {
    pub fn with_cases(cases: usize) -> Result<Self, String> {
        if cases == 0 {
            return Err("Model stream stress cases must be positive".to_owned());
        }
        Ok(Self { cases })
    }

    pub async fn run<F>(&self, adapter_name: &str, mut stream: F) -> ModelStreamStressReport
    where
        F: FnMut(&ModelStreamStressCase) -> Result<ModelStream, ModelError>,
    {
        let mut report = ModelStreamStressReport {
            adapter_name: adapter_name.to_owned(),
            total_cases: self.cases,
            successful_cases: 0,
            protocol_failures: 0,
            cancellations: 0,
            violations: 0,
            first_violation: None,
        };
        for index in 0..self.cases {
            let case = ModelStreamStressCase::new(index);
            let outcome = match stream(&case) {
                Ok(stream) => consume_case(&case, stream).await,
                Err(error) => Err(format!("stream factory rejected the case: {error}")),
            };
            match outcome {
                Ok(StressOutcome::Success) => report.successful_cases += 1,
                Ok(StressOutcome::ProtocolFailure) => report.protocol_failures += 1,
                Ok(StressOutcome::Cancelled) => report.cancellations += 1,
                Err(failure) => {
                    report.violations += 1;
                    if report.first_violation.is_none() {
                        report.first_violation = Some(format!("case {index}: {failure}"));
                    }
                }
            }
        }
        report
    }
}

enum StressOutcome {
    Success,
    ProtocolFailure,
    Cancelled,
}

async fn consume_case(
    case: &ModelStreamStressCase,
    mut stream: ModelStream,
) -> Result<StressOutcome, String> {
    let mut events = Vec::new();
    let mut errors = Vec::new();
    let mut terminal_seen = false;
    let mut item_after_terminal = 0usize;
    if case.index.is_multiple_of(7) {
        tokio::task::yield_now().await;
    }
    while let Some(item) = stream.next().await {
        if terminal_seen {
            item_after_terminal += 1;
        }
        match item {
            Ok(event) => {
                event
                    .validate_for(&case.request_id(), events.len() as u64 + 1)
                    .map_err(|error| error.to_string())?;
                terminal_seen = matches!(event.payload, ModelEvent::Finish { .. });
                events.push(event);
            }
            Err(error) => {
                terminal_seen = true;
                errors.push(error);
            }
        }
        if case.index.is_multiple_of(11) {
            tokio::task::yield_now().await;
        }
    }
    if item_after_terminal != 0 {
        return Err(format!(
            "{item_after_terminal} callback(s) arrived after Finish/error"
        ));
    }
    let finish_positions = events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| {
            matches!(event.payload, ModelEvent::Finish { .. }).then_some(index)
        })
        .collect::<Vec<_>>();

    match case.fault {
        ModelStreamStressFault::None | ModelStreamStressFault::ExtraAfterTerminal => {
            if !errors.is_empty() || finish_positions != vec![events.len().saturating_sub(1)] {
                return Err(format!(
                    "successful stream had errors={errors:?}, finishes={finish_positions:?}"
                ));
            }
            let text = events
                .iter()
                .filter_map(|event| match &event.payload {
                    ModelEvent::TextDelta { delta } => Some(delta.as_str()),
                    _ => None,
                })
                .collect::<String>();
            if text != case.expected_text() {
                return Err(format!("delta reconstruction diverged: {text:?}"));
            }
            Ok(StressOutcome::Success)
        }
        ModelStreamStressFault::MalformedFrame | ModelStreamStressFault::BufferedBurst => {
            if errors.len() != 1
                || errors[0].code != ModelErrorCode::Protocol
                || !finish_positions.is_empty()
            {
                return Err(format!(
                    "protocol fault produced errors={errors:?}, finishes={finish_positions:?}"
                ));
            }
            Ok(StressOutcome::ProtocolFailure)
        }
        ModelStreamStressFault::CancelBeforePoll => {
            if errors.len() != 1
                || errors[0].code != ModelErrorCode::Cancelled
                || !events.is_empty()
            {
                return Err(format!(
                    "cancellation produced events={}, errors={errors:?}",
                    events.len()
                ));
            }
            Ok(StressOutcome::Cancelled)
        }
    }
}
