use crate::model::StreamEvent;

/// Browser-side guard for one Agent-session subscription.
///
/// Sequence zero is an explicit snapshot barrier emitted by the Host. Native
/// sequences may restart after that barrier, while duplicates and forward
/// gaps within the same epoch must never be applied as if they were complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionSequenceDisposition {
    Apply,
    IgnoreDuplicate,
    RefreshSnapshot,
}

#[derive(Debug, Default)]
pub struct SessionSequenceGuard {
    baseline_established: bool,
    last: Option<u64>,
}

impl SessionSequenceGuard {
    pub fn with_cursor(cursor: u64) -> Self {
        Self {
            baseline_established: true,
            last: Some(cursor),
        }
    }

    pub fn observe(&mut self, sequence: u64) -> SessionSequenceDisposition {
        if sequence == 0 {
            self.baseline_established = true;
            self.last = None;
            return SessionSequenceDisposition::Apply;
        }

        let Some(last) = self.last else {
            self.last = Some(sequence);
            if self.baseline_established {
                return SessionSequenceDisposition::Apply;
            }
            self.baseline_established = true;
            return SessionSequenceDisposition::RefreshSnapshot;
        };
        if sequence <= last {
            return SessionSequenceDisposition::IgnoreDuplicate;
        }
        self.last = Some(sequence);
        if sequence == last.saturating_add(1) {
            SessionSequenceDisposition::Apply
        } else {
            SessionSequenceDisposition::RefreshSnapshot
        }
    }
}

/// Incrementally frames an SSE byte stream without assuming that UTF-8 code
/// points or records align with network chunks.
#[derive(Debug, Default)]
pub struct SseParser {
    buffer: Vec<u8>,
}

impl SseParser {
    pub fn push(&mut self, bytes: &[u8]) -> Vec<StreamEvent> {
        self.buffer.extend_from_slice(bytes);
        let mut events = Vec::new();
        while let Some((end, separator_len)) = find_record_end(&self.buffer) {
            let record = self.buffer.drain(..end).collect::<Vec<_>>();
            self.buffer.drain(..separator_len);
            if let Some(event) = parse_record(&record) {
                events.push(event);
            }
        }
        events
    }

    pub fn finish(&mut self) -> Option<StreamEvent> {
        if self.buffer.is_empty() {
            return None;
        }
        let record = std::mem::take(&mut self.buffer);
        parse_record(&record)
    }
}

fn find_record_end(bytes: &[u8]) -> Option<(usize, usize)> {
    bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| (index, 4))
        .or_else(|| {
            bytes
                .windows(2)
                .position(|window| window == b"\n\n")
                .map(|index| (index, 2))
        })
}

fn parse_record(bytes: &[u8]) -> Option<StreamEvent> {
    let text = String::from_utf8(bytes.to_vec()).ok()?;
    let mut kind = "message";
    let mut id = None;
    let mut data = Vec::new();
    for line in text.lines() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.starts_with(':') {
            continue;
        }
        let (field, value) = line
            .split_once(':')
            .map(|(field, value)| (field, value.strip_prefix(' ').unwrap_or(value)))
            .unwrap_or((line, ""));
        match field {
            "event" => kind = value,
            "id" => id = Some(value.to_owned()),
            "data" => data.push(value),
            _ => {}
        }
    }
    let data = data.join("\n");
    match kind {
        "durable" => Some(StreamEvent::Durable { id, data }),
        "telemetry" => Some(StreamEvent::Telemetry { data }),
        "session_changed" => Some(StreamEvent::SessionChanged { id, data }),
        "error" => Some(StreamEvent::Error { data }),
        _ if data.is_empty() => Some(StreamEvent::KeepAlive),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_chunked_utf8_and_crlf_records() {
        let text = "event: durable\r\nid: 2\r\ndata: {\"message\":\"你好\"}\r\n\r\n";
        let split = text.find('好').unwrap() + 1;
        let mut parser = SseParser::default();
        assert!(parser.push(&text.as_bytes()[..split]).is_empty());
        let events = parser.push(&text.as_bytes()[split..]);
        assert_eq!(
            events,
            vec![StreamEvent::Durable {
                id: Some("2".to_owned()),
                data: "{\"message\":\"你好\"}".to_owned(),
            }]
        );
    }

    #[test]
    fn joins_multiline_data() {
        let mut parser = SseParser::default();
        let events = parser.push(b"event: error\ndata: first\ndata: second\n\n");
        assert_eq!(
            events,
            vec![StreamEvent::Error {
                data: "first\nsecond".to_owned()
            }]
        );
    }

    #[test]
    fn parses_session_change_invalidation() {
        let mut parser = SseParser::default();
        let events = parser.push(b"event: session_changed\nid: 7\ndata: {}\n\n");
        assert_eq!(
            events,
            vec![StreamEvent::SessionChanged {
                id: Some("7".to_owned()),
                data: "{}".to_owned(),
            }]
        );
    }

    #[test]
    fn session_sequence_guard_detects_duplicates_and_forward_gaps() {
        let mut guard = SessionSequenceGuard::default();
        assert_eq!(
            guard.observe(0),
            SessionSequenceDisposition::Apply,
            "the Host snapshot barrier is always applied"
        );
        assert_eq!(guard.observe(1), SessionSequenceDisposition::Apply);
        assert_eq!(guard.observe(2), SessionSequenceDisposition::Apply);
        assert_eq!(
            guard.observe(2),
            SessionSequenceDisposition::IgnoreDuplicate
        );
        assert_eq!(
            guard.observe(5),
            SessionSequenceDisposition::RefreshSnapshot
        );
        assert_eq!(guard.observe(6), SessionSequenceDisposition::Apply);
    }

    #[test]
    fn explicit_gap_barrier_allows_the_native_sequence_to_rebase() {
        let mut guard = SessionSequenceGuard::default();
        assert_eq!(
            guard.observe(9),
            SessionSequenceDisposition::RefreshSnapshot,
            "a stream missing its initial barrier must reconcile first"
        );
        assert_eq!(guard.observe(0), SessionSequenceDisposition::Apply);
        assert_eq!(guard.observe(42), SessionSequenceDisposition::Apply);
    }

    #[test]
    fn canonical_cursor_resumes_without_an_initial_snapshot_barrier() {
        let mut guard = SessionSequenceGuard::with_cursor(41);
        assert_eq!(guard.observe(42), SessionSequenceDisposition::Apply);
        assert_eq!(
            guard.observe(42),
            SessionSequenceDisposition::IgnoreDuplicate
        );
        assert_eq!(
            guard.observe(44),
            SessionSequenceDisposition::RefreshSnapshot
        );
    }
}
