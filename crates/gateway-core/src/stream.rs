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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StrictStreamParseError {
    #[error(transparent)]
    Parse(#[from] StreamParseError),
    #[error("SSE block contains fields that cannot be policy-validated")]
    UnvalidatedBlock,
}

pub struct SseDecoder {
    buffer: String,
    /// How much of `buffer` is already known to hold no event delimiter, so a
    /// partial event is never rescanned once per chunk that extends it.
    scanned: usize,
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
            scanned: 0,
            max_buffer_bytes,
        }
    }

    pub fn push(&mut self, chunk: &str) -> Result<Vec<SseEvent>, StreamParseError> {
        match self.push_inner(chunk, false) {
            Ok(events) => Ok(events),
            Err(StrictStreamParseError::Parse(error)) => Err(error),
            Err(StrictStreamParseError::UnvalidatedBlock) => {
                unreachable!("ordinary SSE decoding does not apply strict validation")
            }
        }
    }

    /// Decode only SSE blocks that can be presented to policy middleware.
    ///
    /// Comments and blocks without `data:` are valid SSE, but a byte-faithful
    /// relay cannot release them after validating only data events: their raw
    /// bytes would never receive a policy verdict. Strict decoding therefore
    /// refuses such a block instead of silently discarding it.
    pub fn push_strict(&mut self, chunk: &str) -> Result<Vec<SseEvent>, StrictStreamParseError> {
        self.push_inner(chunk, true)
    }

    fn push_inner(
        &mut self,
        chunk: &str,
        reject_unvalidated_blocks: bool,
    ) -> Result<Vec<SseEvent>, StrictStreamParseError> {
        self.buffer.push_str(chunk);
        if self.buffer.len() > self.max_buffer_bytes {
            return Err(StreamParseError::BufferLimit(self.max_buffer_bytes).into());
        }
        let mut events = Vec::new();
        // Scanning and draining per event would reread and reshuffle the whole
        // buffer once per event, which is quadratic in what an upstream sends:
        // a chunk of nothing but delimiters costs the gateway far more than it
        // costs the provider. A cursor plus a single drain keeps it linear.
        let mut search = self.scanned;
        let mut consumed = 0;
        while let Some(offset) = event_end(&self.buffer[search..]) {
            let end = search + offset;
            let block = self.buffer[consumed..end].replace('\r', "");
            let delimiter_len = if self.buffer[end..].starts_with("\r\n\r\n") {
                4
            } else {
                2
            };
            consumed = end + delimiter_len;
            search = consumed;
            if reject_unvalidated_blocks && !strict_block_is_valid(&block) {
                return Err(StrictStreamParseError::UnvalidatedBlock);
            }
            if let Some(event) = parse_event(&block) {
                events.push(event);
            }
        }
        self.buffer.drain(..consumed);
        // A delimiter can straddle the next chunk, so the tail stays unscanned.
        let mut scanned = self.buffer.len().saturating_sub(DELIMITER_OVERLAP);
        while scanned > 0 && !self.buffer.is_char_boundary(scanned) {
            scanned -= 1;
        }
        self.scanned = scanned;
        Ok(events)
    }

    /// What the decoder is still holding between events, for the out-of-tree
    /// fuzz project to assert the buffer stays bounded and never retains a
    /// complete event. Compiled only under `--cfg fuzzing`, which nothing but
    /// [`fuzz/`](https://github.com/Litvue/axond/tree/main/fuzz) sets, so this
    /// widens no published API.
    #[cfg(fuzzing)]
    pub fn fuzz_buffered(&self) -> &str {
        &self.buffer
    }

    pub fn finish(self) -> Result<(), StreamParseError> {
        if self.buffer.trim().is_empty() {
            Ok(())
        } else {
            Err(StreamParseError::Incomplete)
        }
    }
}

/// The longest prefix of an event delimiter that can end a chunk: `\r\n\r`.
const DELIMITER_OVERLAP: usize = 3;

/// Where the first event delimiter starts, whichever of the two it is.
///
/// One left-to-right pass rather than a search for each delimiter: searching
/// separately costs a scan of the whole buffer for the delimiter that is not
/// there, on every event, which a stream of LF-delimited events pays in full.
fn event_end(buffer: &str) -> Option<usize> {
    let bytes = buffer.as_bytes();
    for index in 0..bytes.len().saturating_sub(1) {
        if bytes[index] == b'\n' && bytes[index + 1] == b'\n' {
            return Some(index);
        }
        if bytes[index] == b'\r' && bytes[index..].starts_with(b"\r\n\r\n") {
            return Some(index);
        }
    }
    None
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

/// Whether every byte-bearing SSE field in a block reaches the policy callback.
/// `data:` values are joined and exposed, and one `event:` value is exposed as
/// the event name. Everything else—including comments, metadata, unknown
/// fields, and a shadowed event name—would disappear in `parse_event` and is
/// therefore refused by strict byte-faithful validation.
fn strict_block_is_valid(block: &str) -> bool {
    let mut event_seen = false;
    let mut data_seen = false;
    for line in block.lines() {
        if line.starts_with(':') {
            return false;
        }
        let Some((field, _)) = line.split_once(':') else {
            return false;
        };
        match field {
            "data" => data_seen = true,
            "event" if !event_seen => event_seen = true,
            "event" | "id" | "retry" => return false,
            _ => return false,
        }
    }
    data_seen
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

    /// What the `sse_decode` fuzz target asserts over arbitrary bodies, pinned
    /// here for the cases the corpus is built from: where a chunk boundary
    /// falls cannot change what a stream decodes to.
    #[test]
    fn every_chunk_boundary_decodes_a_body_identically() {
        for body in [
            "data: one\n\ndata: two\n\n",
            "event: delta\r\ndata: {\"a\":1}\r\n\r\n: keep-alive\r\n\r\ndata: [DONE]\r\n\r\n",
            "data: first\ndata: second\n\ndata: tail",
            ": comment only\n\n\n\ndata: \n\n",
        ] {
            let mut whole = SseDecoder::default();
            let expected = whole.push(body).unwrap();
            for cut in 1..body.len() {
                if !body.is_char_boundary(cut) {
                    continue;
                }
                let mut split = SseDecoder::default();
                let mut events = split.push(&body[..cut]).unwrap();
                events.extend(split.push(&body[cut..]).unwrap());
                assert_eq!(events, expected, "{body:?} split at {cut}");
            }
        }
    }

    /// A stream that ends mid-event is a controlled error, not a panic and not
    /// a silently accepted truncation.
    #[test]
    fn a_truncated_final_event_is_refused_by_finish() {
        let mut decoder = SseDecoder::default();
        assert!(decoder.push("data: complete\n\ndata: trunc").unwrap().len() == 1);
        assert_eq!(decoder.finish(), Err(StreamParseError::Incomplete));
    }

    #[test]
    fn strict_decode_refuses_blocks_middleware_cannot_validate() {
        for block in [
            ": secret comment\n\n",
            "event: opaque\nid: secret\n\n",
            ": mixed secret\ndata: {\"safe\":true}\n\n",
            "event: first\nevent: second\ndata: {\"safe\":true}\n\n",
            "retry: 1000\ndata: {\"safe\":true}\n\n",
            "future-field: secret\ndata: {\"safe\":true}\n\n",
        ] {
            let mut decoder = SseDecoder::default();
            assert_eq!(
                decoder.push_strict(block),
                Err(StrictStreamParseError::UnvalidatedBlock)
            );
        }

        let mut decoder = SseDecoder::default();
        assert_eq!(
            decoder
                .push_strict("event: delta\ndata: first\ndata: second\n\n")
                .unwrap(),
            vec![SseEvent {
                event: Some("delta".to_owned()),
                data: "first\nsecond".to_owned(),
            }]
        );
    }

    /// The scan cursor must not skip a delimiter that arrives one byte at a
    /// time, which is the boundary case it exists to avoid rescanning.
    #[test]
    fn a_delimiter_split_byte_by_byte_still_terminates_an_event() {
        let body = "event: delta\r\ndata: one\r\n\r\ndata: two\n\n";
        let mut decoder = SseDecoder::default();
        let mut events = Vec::new();
        for byte in 0..body.len() {
            events.extend(decoder.push(&body[byte..=byte]).unwrap());
        }
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event.as_deref(), Some("delta"));
        assert_eq!(events[0].data, "one");
        assert_eq!(events[1].data, "two");
        decoder.finish().unwrap();
    }

    /// A stream of nothing but delimiters is the cheapest thing an upstream can
    /// send and used to be the most expensive thing to parse: scanning and
    /// draining per event made the cost quadratic in the chunk.
    #[test]
    fn a_chunk_of_many_tiny_events_is_parsed_in_one_pass() {
        let events = 200_000;
        let mut decoder = SseDecoder::new(8 * 1024 * 1024);
        let decoded = decoder.push(&"data: x\n\n".repeat(events)).unwrap();
        assert_eq!(decoded.len(), events);
        decoder.finish().unwrap();
    }

    /// The buffer limit is the bound on what an upstream can make the gateway
    /// hold for a stream that never terminates an event.
    #[test]
    fn an_unterminated_event_trips_the_buffer_limit() {
        let mut decoder = SseDecoder::new(64);
        assert_eq!(
            decoder.push(&"data: ".repeat(64)),
            Err(StreamParseError::BufferLimit(64))
        );
    }
}
