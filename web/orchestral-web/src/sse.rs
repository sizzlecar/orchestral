use crate::model::StreamEvent;

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
}
