#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    pub event: Option<String>,
    pub data: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StreamParseError {
    #[error("SSE buffer exceeded {0} bytes")]
    BufferLimit(usize),
    #[error("stream ended with an incomplete SSE event")]
    Incomplete,
}

pub struct SseDecoder {
    buffer: String,
    max_buffer_bytes: usize,
}

impl Default for SseDecoder {
    fn default() -> Self {
        Self::new(1024 * 1024)
    }
}

impl SseDecoder {
    pub fn new(max_buffer_bytes: usize) -> Self {
        Self {
            buffer: String::new(),
            max_buffer_bytes,
        }
    }

    pub fn push(&mut self, chunk: &str) -> Result<Vec<SseEvent>, StreamParseError> {
        self.buffer.push_str(chunk);
        if self.buffer.len() > self.max_buffer_bytes {
            return Err(StreamParseError::BufferLimit(self.max_buffer_bytes));
        }
        let mut events = Vec::new();
        while let Some(end) = event_end(&self.buffer) {
            let block = self.buffer[..end].replace('\r', "");
            let delimiter_len = if self.buffer[end..].starts_with("\r\n\r\n") {
                4
            } else {
                2
            };
            self.buffer.drain(..end + delimiter_len);
            if let Some(event) = parse_event(&block) {
                events.push(event);
            }
        }
        Ok(events)
    }

    pub fn finish(self) -> Result<(), StreamParseError> {
        if self.buffer.trim().is_empty() {
            Ok(())
        } else {
            Err(StreamParseError::Incomplete)
        }
    }
}

fn event_end(buffer: &str) -> Option<usize> {
    match (buffer.find("\n\n"), buffer.find("\r\n\r\n")) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn parse_event(block: &str) -> Option<SseEvent> {
    let mut event = None;
    let mut data = Vec::new();
    for line in block.lines() {
        if line.starts_with(':') {
            continue;
        }
        if let Some(value) = line.strip_prefix("event:") {
            event = Some(value.trim_start().to_owned());
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push(value.trim_start());
        }
    }
    (!data.is_empty()).then(|| SseEvent {
        event,
        data: data.join("\n"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_fragmented_and_multiline_events() {
        let mut decoder = SseDecoder::default();
        assert!(
            decoder
                .push("event: delta\r\ndata: {\"a\":")
                .unwrap()
                .is_empty()
        );
        let events = decoder.push("1}\r\ndata: tail\r\n\r\n").unwrap();
        assert_eq!(events[0].event.as_deref(), Some("delta"));
        assert_eq!(events[0].data, "{\"a\":1}\ntail");
        decoder.finish().unwrap();
    }
}
