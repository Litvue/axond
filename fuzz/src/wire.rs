//! The wire-decoding fuzz targets: `gateway-core`'s SSE decoder, the provider
//! stream decoders behind it, and the provider-error classification an upstream
//! failure lands in (issue #224).
//!
//! These parse what a *provider* sends, which is untrusted for the same reason
//! a caller's token is: a compromised or merely broken upstream is on the other
//! end of the socket, and a stream is relayed byte for byte to a tenant. The
//! properties asserted here are the ones the relay depends on:
//!
//! 1. **No panic and no hang.** An unwind inside a stream decoder aborts a live
//!    response mid-flight, which no retry recovers.
//! 2. **No unbounded buffer.** The SSE decoder must never hold more than its
//!    configured limit, must never *retain* a complete event, and must never
//!    decode to more bytes than it was given. A decoder amplifying its input is
//!    a memory-exhaustion vector an upstream controls for free.
//! 3. **Controlled errors.** `finish` and every malformed event return a typed
//!    [`StreamParseError`] or [`ProviderError`], never a surprise variant.
//! 4. **Stability.** Where a chunk boundary happens to fall cannot change what
//!    a body decodes to, and a valid fixture decodes to the same events under
//!    every possible split.
//! 5. **Bounded, non-disclosing diagnostics.** Every provider diagnostic fits
//!    inside [`MAX_DIAGNOSTIC_BYTES`], and none of them carries a value the
//!    input did not — the gateway's own credential and upstream URL are held by
//!    the harness in [`GATEWAY_CREDENTIAL_CANARY`] and [`PROVIDER_URL_CANARY`]
//!    and must never appear in an error the decoders build.
//!
//! Catalogue parsing is deliberately absent: it is issue #222's target.

use arbitrary::Arbitrary;
use gateway_core::{
    AnthropicAdapter, DIAGNOSTIC_TRUNCATION_MARKER, MAX_DIAGNOSTIC_BYTES, NativeMessagesDecoder,
    OpenAiCompatibleAdapter, ProviderAdapter, ProviderError, ProviderStreamDecoder,
    ProviderStreamEvent, SseDecoder, SseEvent, StreamParseError, Surface,
};

/// A value the gateway holds and an upstream never sends: a credential this
/// process would authenticate with. It is passed to nothing below, so finding
/// it in a diagnostic means a decoder disclosed gateway state.
///
/// Synthetic and committed, like every other value in this project.
pub const GATEWAY_CREDENTIAL_CANARY: &str = "axond-fuzz-gateway-credential-not-a-secret";

/// The upstream URL the gateway would have called. Provider errors name the
/// *provider*, never the endpoint, because an endpoint carries deployment
/// topology and, for some providers, the key in its query string.
pub const PROVIDER_URL_CANARY: &str = "https://fuzz-upstream.provider.invalid/v1/messages?key=live";

/// The longest a diagnostic may be once bounding has been applied.
const DIAGNOSTIC_CEILING: usize = MAX_DIAGNOSTIC_BYTES + DIAGNOSTIC_TRUNCATION_MARKER.len();

/// How far back a scan must reach to catch a delimiter that straddles the join
/// between what a push appended and what was already there: `\r\n\r\n` less its
/// last byte.
const DELIMITER_OVERLAP: usize = 3;

/// Every code [`ProviderError::code`] may answer with. A code outside this set
/// would reach a response body and a metric label unrecognized.
const PROVIDER_ERROR_CODES: &[&str] = &[
    "invalid_request",
    "context_window_exceeded",
    "unsupported",
    "model_unavailable",
    "provider_dependency_failed",
    "invalid_stream",
    "provider_rate_limited",
    "all_provider_circuits_open",
];

/// An untrusted SSE body together with the chunk boundaries it arrives on.
///
/// The boundaries are the point of the target: the transport hands the decoder
/// whatever a TCP read returned, so an event, a `\r\n\r\n` delimiter, or a
/// multi-byte character can be split across two calls.
#[derive(Debug, Arbitrary)]
pub struct SseInput<'a> {
    pub body: &'a str,
    /// Where to cut, as offsets into the body modulo its length.
    pub cuts: Vec<u16>,
    /// The decoder's buffer limit, small enough that the limit is reachable.
    pub max_buffer_bytes: u16,
}

/// What one drive of the decoder produced.
struct Run {
    events: Vec<SseEvent>,
    push_error: Option<StreamParseError>,
    finish: Result<(), StreamParseError>,
}

/// SSE decoding: arbitrary bodies, arbitrary chunk boundaries, truncated final
/// events, and a reachable buffer limit.
pub fn sse_decode(input: &SseInput<'_>) -> Vec<&'static str> {
    let body = input.body;
    let boundaries = boundaries(body, &input.cuts);

    // A limit the body cannot trip, so this run is about parsing rather than
    // refusal: the buffer never holds more than the body itself.
    let generous = body.len().max(1);
    let whole = drive(body, &[], generous);
    let split = drive(body, &boundaries, generous);
    assert!(
        whole.push_error.is_none() && split.push_error.is_none(),
        "a body decoded under a limit its own length cannot exceed still hit one"
    );
    // The stability property: a chunk boundary is an artefact of the network,
    // so it must not be observable in what the stream decodes to.
    assert_eq!(
        whole.events, split.events,
        "decoding {body:?} split at {boundaries:?} differed from decoding it whole"
    );
    assert_eq!(
        whole.finish.is_ok(),
        split.finish.is_ok(),
        "a chunk boundary changed whether {body:?} ended cleanly"
    );

    // Decoding strips `data:` prefixes and `\r`, so it can only ever shrink: an
    // upstream cannot make the gateway hold more than it sent.
    let decoded: usize = whole
        .events
        .iter()
        .map(|event| event.data.len() + event.event.as_ref().map_or(0, String::len))
        .sum();
    assert!(
        decoded <= body.len(),
        "decoding expanded a {}-byte body into {decoded} bytes of events",
        body.len()
    );

    // The same body under a limit it can trip. Everything decoded before the
    // refusal has to match what the unbounded run decoded, or a limit would be
    // rewriting a stream rather than ending it.
    let limited = drive(body, &boundaries, usize::from(input.max_buffer_bytes));
    assert!(
        limited.events.len() <= whole.events.len()
            && limited.events[..] == whole.events[..limited.events.len()],
        "the buffer limit changed what was decoded before it was reached"
    );

    let mut classes = Vec::new();
    classes.push(if whole.events.is_empty() {
        "no_events"
    } else {
        "events"
    });
    classes.push(match &limited.push_error {
        Some(StreamParseError::BufferLimit(limit)) => {
            assert_eq!(
                *limit,
                usize::from(input.max_buffer_bytes),
                "the refusal reported a limit the decoder was not configured with"
            );
            "buffer_limit"
        }
        // `push` has no other way to refuse; a new one must not arrive silently.
        Some(StreamParseError::Incomplete) => {
            panic!("push reported an incomplete stream, which only finish may")
        }
        None => "pushed",
    });
    classes.push(match limited.finish {
        Ok(()) => "complete",
        Err(StreamParseError::Incomplete) => "incomplete",
        Err(StreamParseError::BufferLimit(limit)) => {
            panic!("finish enforced a buffer limit of {limit}, which is push's job")
        }
    });
    classes
}

/// Feed `body` to a decoder in the chunks `boundaries` describes, asserting the
/// buffer invariants after every accepted push.
fn drive(body: &str, boundaries: &[usize], max_buffer_bytes: usize) -> Run {
    let mut decoder = SseDecoder::new(max_buffer_bytes);
    let mut events = Vec::new();
    let mut push_error = None;
    for chunk in chunks(body, boundaries) {
        match decoder.push(chunk) {
            Ok(decoded) => {
                let buffered = decoder.fuzz_buffered();
                assert!(
                    buffered.len() <= max_buffer_bytes,
                    "the decoder holds {} bytes, over its {max_buffer_bytes}-byte limit",
                    buffered.len()
                );
                // Anything terminated has been emitted, so what is held is a
                // partial event. A decoder retaining a complete one would grow
                // without bound on a stream that never stops.
                //
                // Only the part of the buffer this push could have changed is
                // scanned. The buffer grew by the chunk and shrank by whatever
                // was drained, so everything below `len - chunk` was already
                // scanned by an earlier push and cannot have been rewritten —
                // `push` only appends and drains a prefix. That keeps the whole
                // drive linear in the body rather than quadratic in the chunk
                // count, so a slow input means a slow *decoder*.
                let unscanned = tail(buffered, chunk.len() + DELIMITER_OVERLAP);
                assert!(
                    !unscanned.contains("\n\n") && !unscanned.contains("\r\n\r\n"),
                    "the decoder retained a complete event: {buffered:?}"
                );
                events.extend(decoded);
            }
            Err(error) => {
                push_error = Some(error);
                break;
            }
        }
    }
    // One full scan of what survived the whole drive, so the incremental scans
    // cannot hide a retained event behind a wrong assumption about what a push
    // may rewrite. Once per drive, over a buffer the limit already caps — after
    // a refusal the decoder is entitled to still hold the chunk it rejected.
    if push_error.is_none() {
        let buffered = decoder.fuzz_buffered();
        assert!(
            !buffered.contains("\n\n") && !buffered.contains("\r\n\r\n"),
            "the decoder retained a complete event: {buffered:?}"
        );
    }
    let finish = decoder.finish();
    Run {
        events,
        push_error,
        finish,
    }
}

/// The last `bytes` of `text`, widened to the nearest character boundary below.
fn tail(text: &str, bytes: usize) -> &str {
    let mut start = text.len().saturating_sub(bytes);
    while start > 0 && !text.is_char_boundary(start) {
        start -= 1;
    }
    &text[start..]
}

/// Turn arbitrary integers into sorted, deduplicated, character-aligned offsets
/// into `body`.
fn boundaries(body: &str, cuts: &[u16]) -> Vec<usize> {
    if body.is_empty() {
        return Vec::new();
    }
    let mut offsets: Vec<usize> = cuts
        .iter()
        .map(|cut| {
            let mut offset = usize::from(*cut) % body.len();
            while offset > 0 && !body.is_char_boundary(offset) {
                offset -= 1;
            }
            offset
        })
        .filter(|offset| *offset > 0)
        .collect();
    offsets.sort_unstable();
    offsets.dedup();
    offsets
}

fn chunks<'a>(body: &'a str, boundaries: &[usize]) -> Vec<&'a str> {
    let mut chunks = Vec::with_capacity(boundaries.len() + 1);
    let mut start = 0;
    for boundary in boundaries {
        chunks.push(&body[start..*boundary]);
        start = *boundary;
    }
    chunks.push(&body[start..]);
    chunks
}

/// Decode a wire capture into the `(event name, data)` pairs a provider stream
/// decoder sees, so a readable seed file can drive the stream targets.
///
/// Whatever the capture decodes to before it stops is what the decoders get: a
/// truncated capture is exactly the case worth feeding them.
#[must_use]
pub fn sse_events(body: &str) -> Vec<(Option<String>, String)> {
    let mut decoder = SseDecoder::default();
    decoder
        .push(body)
        .unwrap_or_default()
        .into_iter()
        .map(|event| (event.event, event.data))
        .collect()
}

/// Which decoder an event stream is fed to.
#[derive(Debug, Arbitrary)]
pub enum StreamShape {
    /// OpenAI chat completions: chunks forwarded verbatim, usage folded.
    OpenAiChat,
    /// The OpenAI Responses surface, whose event names come from the payload.
    OpenAiResponses,
    /// Azure AI Foundry, which is OpenAI-compatible with a rewritten endpoint.
    FoundryChat,
    /// Anthropic Messages translated into OpenAI chat chunks: the decoder with
    /// state — thinking blocks, tool indices, a terminal chunk.
    AnthropicTranslated,
    /// A native Anthropic stream relayed unchanged, with usage folded.
    AnthropicNative,
}

/// An untrusted provider stream: the events an [`SseDecoder`] would have
/// produced, handed to the decoder that interprets them.
#[derive(Debug, Arbitrary)]
pub struct ProviderStreamInput<'a> {
    pub shape: StreamShape,
    /// `(event name, data)` pairs, the shape [`SseEvent`] carries.
    pub events: Vec<(Option<&'a str>, &'a str)>,
}

/// How much output one event may produce beyond a multiple of its input.
///
/// A translated Anthropic delta is wrapped in an OpenAI chat chunk, and a
/// terminal chunk carries a usage block, so a tiny event legitimately produces
/// a few hundred bytes. What this rules out is amplification that *scales*:
/// re-emitting accumulated state on every event.
const PER_EVENT_OVERHEAD: usize = 1024;

/// The multiple of its input a stream may decode to. Generous, because
/// Anthropic's tool-call translation base64s the thinking blocks it accumulated
/// (about 1.4x) on top of having streamed them as deltas.
const EMISSION_FACTOR: usize = 8;

/// Provider stream decoding: malformed JSON, unknown event types, out-of-order
/// blocks, rate-limit payloads, and terminal handling.
pub fn provider_stream(input: &ProviderStreamInput<'_>) -> Vec<&'static str> {
    let mut decoder = decoder_for(&input.shape);
    let mut classes = Vec::new();
    let mut input_bytes = 0_usize;
    let mut emitted_bytes = 0_usize;

    for (name, data) in &input.events {
        input_bytes += data.len() + name.map_or(0, |name| name.len());
        let event = SseEvent {
            event: name.map(str::to_owned),
            data: (*data).to_owned(),
        };
        match decoder.decode(event) {
            Ok(events) => {
                emitted_bytes += events.iter().map(measure).sum::<usize>();
                classes.push(if events.is_empty() {
                    "absorbed"
                } else {
                    "emitted"
                });
            }
            Err(error) => {
                classes.push(assert_typed_provider_error(&error, &[data]));
                // The relay stops at the first error, so the decoder is never
                // driven past one. Fuzzing past it would assert on a state the
                // gateway cannot reach.
                break;
            }
        }
    }

    match decoder.finish() {
        Ok(events) => {
            emitted_bytes += events.iter().map(measure).sum::<usize>();
            classes.push(if events.is_empty() {
                "finished_silently"
            } else {
                "finished_terminal"
            });
        }
        Err(error) => classes.push(assert_typed_provider_error(&error, &[])),
    }
    // Finishing twice is what a cancelled stream does: the relay's cleanup runs
    // after the terminal event it already emitted. A second terminal chunk
    // would be a duplicate `finish_reason` on the wire.
    let repeated = decoder.finish();
    assert!(
        matches!(&repeated, Ok(events) if events.is_empty()),
        "finishing an already-finished stream emitted {repeated:?}"
    );

    let allowance = PER_EVENT_OVERHEAD * (input.events.len() + 2) + EMISSION_FACTOR * input_bytes;
    assert!(
        emitted_bytes <= allowance,
        "a {input_bytes}-byte stream of {} events decoded into {emitted_bytes} bytes, over the \
         {allowance}-byte allowance",
        input.events.len()
    );
    classes
}

fn decoder_for(shape: &StreamShape) -> Box<dyn ProviderStreamDecoder> {
    let surface = match shape {
        StreamShape::OpenAiResponses => Surface::Responses,
        _ => Surface::ChatCompletions,
    };
    match shape {
        StreamShape::OpenAiChat | StreamShape::OpenAiResponses => OpenAiCompatibleAdapter::openai()
            .stream_decoder(surface)
            .expect("the OpenAI adapter decodes both of its surfaces"),
        StreamShape::FoundryChat => OpenAiCompatibleAdapter::foundry()
            .stream_decoder(surface)
            .expect("the Foundry adapter decodes chat completions"),
        StreamShape::AnthropicTranslated => AnthropicAdapter::new()
            .stream_decoder(surface)
            .expect("the Anthropic adapter decodes chat completions"),
        StreamShape::AnthropicNative => Box::new(NativeMessagesDecoder::new()),
    }
}

/// The bytes an emitted event puts on the wire.
fn measure(event: &ProviderStreamEvent) -> usize {
    match event {
        ProviderStreamEvent::Data { event, data } => {
            event.as_ref().map_or(0, String::len) + data.to_string().len()
        }
        // A `[DONE]` sentinel plus the usage line the relay renders from it.
        ProviderStreamEvent::Done(_) => 256,
    }
}

/// An untrusted upstream failure: the three things `from_upstream` classifies.
#[derive(Debug, Arbitrary)]
pub struct UpstreamFailure<'a> {
    /// The provider *name* an adapter reports — never a URL, which is the
    /// property the leakage assertion pins.
    pub provider: &'a str,
    pub status: u16,
    pub body: &'a str,
}

/// Upstream failure classification: which typed error a status and body become,
/// and what the diagnostic that comes with it may contain.
pub fn provider_error(input: &UpstreamFailure<'_>) -> Vec<&'static str> {
    let error = ProviderError::from_upstream(input.provider, input.status, input.body);
    // Classification is a pure function of its inputs: two replicas seeing the
    // same upstream failure must fail over the same way.
    assert_eq!(
        error,
        ProviderError::from_upstream(input.provider, input.status, input.body),
        "the same upstream failure classified two ways"
    );

    let code = assert_typed_provider_error(&error, &[input.provider, input.body]);
    assert_classification_is_consistent(&error, input);

    // `transport` is the other constructor an untrusted value reaches, through
    // a redacted client-error string.
    let transport = ProviderError::transport(input.provider, input.body);
    assert_typed_provider_error(&transport, &[input.provider, input.body]);
    assert!(
        transport.is_retryable() && transport.affects_provider_health(),
        "a transport failure stopped being retryable, so a dead upstream would not fail over"
    );

    vec![code]
}

/// The properties every classification must satisfy, whatever the input.
fn assert_classification_is_consistent(error: &ProviderError, input: &UpstreamFailure<'_>) {
    let status = input.status;
    match error {
        // A context-window refusal is the one classification that ignores the
        // status, because providers report it under several.
        ProviderError::ContextWindowExceeded(_) => {
            assert!(
                !error.is_retryable(),
                "retrying a prompt too long for the model cannot help"
            );
            assert!(!error.affects_provider_health());
        }
        ProviderError::ModelUnavailable(failures) => {
            assert_eq!(status, 404, "a non-404 became a missing model");
            assert_providers(failures, input);
            // Failing over to another provider is the point; the provider that
            // lacks the model is not unhealthy.
            assert!(error.is_retryable());
            assert!(!error.affects_provider_health());
        }
        ProviderError::InvalidRequest(_) => {
            assert!(
                (400..500).contains(&status) && status != 429 && status != 404,
                "status {status} became a client error"
            );
            assert!(
                !error.is_retryable(),
                "replaying a refused request would only refuse again"
            );
        }
        ProviderError::Dependency(failures) => {
            assert!(
                status >= 500 || status == 429 || status < 400,
                "status {status} became a dependency failure"
            );
            assert_providers(failures, input);
            // Only a rate limit or a server-side failure is worth another
            // attempt; anything else here is a status the gateway does not
            // treat as transient.
            let transient = status == 429 || status >= 500;
            assert_eq!(error.is_retryable(), transient, "status {status}");
            assert_eq!(
                error.affects_provider_health(),
                transient,
                "status {status}"
            );
            assert_eq!(
                error.is_credential_rate_limited(),
                status == 429,
                "credential rate limiting disagreed with status {status}"
            );
        }
        other => panic!("from_upstream produced {other:?}, which it has no arm for"),
    }
    // Health is a strictly narrower judgement than retryability: marking a
    // provider unhealthy while refusing to retry would open a circuit nothing
    // ever closes.
    assert!(!error.affects_provider_health() || error.is_retryable());
    assert!(
        !error.is_stream_rate_limited(),
        "an HTTP failure is not a stream failure"
    );
}

/// The failure must name the provider it was told about — exactly, with nothing
/// appended. An endpoint reaching this field would put deployment topology into
/// every log line and status response the failure appears in.
fn assert_providers(failures: &[gateway_core::DependencyFailure], input: &UpstreamFailure<'_>) {
    for failure in failures {
        assert_eq!(
            failure.provider, input.provider,
            "the failure named a provider the caller did not"
        );
        assert_eq!(failure.status, Some(input.status));
    }
}

/// A provider error must be answerable: a known code, a bounded diagnostic, and
/// nothing in it the input did not carry.
///
/// Returns the error's stable code as the outcome class.
fn assert_typed_provider_error(error: &ProviderError, sources: &[&str]) -> &'static str {
    let code = error.code();
    assert!(
        PROVIDER_ERROR_CODES.contains(&code),
        "provider error carries unknown code {code:?}"
    );
    let rendered = error.to_string();
    for diagnostic in diagnostics(error) {
        assert!(
            diagnostic.len() <= DIAGNOSTIC_CEILING,
            "a {}-byte diagnostic reached a log line and a response body, over the \
             {DIAGNOSTIC_CEILING}-byte ceiling",
            diagnostic.len()
        );
        assert_no_disclosure(diagnostic, sources);
    }
    assert_no_disclosure(&rendered, sources);
    assert_no_disclosure(&format!("{error:?}"), sources);
    code
}

/// Every diagnostic a provider error carries outwards.
///
/// Provider *names* are excluded: they come from the gateway's own
/// configuration rather than from an upstream, so they are bounded by what an
/// operator wrote. What must be bounded is everything derived from a response.
fn diagnostics(error: &ProviderError) -> Vec<&str> {
    match error {
        ProviderError::InvalidRequest(message)
        | ProviderError::ContextWindowExceeded(message)
        | ProviderError::Unsupported(message)
        | ProviderError::InvalidStream(message)
        | ProviderError::RateLimitedStream(message) => vec![message.as_str()],
        ProviderError::ModelUnavailable(failures) | ProviderError::Dependency(failures) => failures
            .iter()
            .map(|failure| failure.message.as_str())
            .collect(),
        ProviderError::AllCircuitsOpen(providers) => providers.iter().map(String::as_str).collect(),
    }
}

/// A diagnostic may repeat what it was given and nothing else.
///
/// The canaries stand for the two classes of value the gateway holds and a
/// provider error must never carry: the credential this process authenticates
/// upstream with, and the endpoint it called. Neither is passed to anything in
/// this module, so an occurrence that no input explains is a disclosure — and
/// the seeds that *do* carry a canary keep the other side of the branch live.
fn assert_no_disclosure(rendered: &str, sources: &[&str]) {
    for canary in [GATEWAY_CREDENTIAL_CANARY, PROVIDER_URL_CANARY] {
        if rendered.contains(canary) {
            assert!(
                sources.iter().any(|source| source.contains(canary)),
                "a diagnostic disclosed {canary:?}, which no input carried"
            );
        }
    }
}

/// Prove a valid stream decodes identically however it is chunked, and to
/// exactly the events it is supposed to.
///
/// The fuzz targets assert *relative* properties — two drives agree, nothing
/// grew — which a decoder that returned nothing at all would satisfy. This is
/// the absolute one: named fixtures with their expected events, replayed under
/// every character boundary the body has.
///
/// # Panics
///
/// If a fixture stops decoding to the events it is pinned to.
pub fn assert_valid_fixtures_are_stable() {
    for (name, body, expected) in FIXTURES {
        let mut whole = SseDecoder::default();
        let decoded = whole
            .push(body)
            .unwrap_or_else(|error| panic!("valid fixture {name} was refused: {error}"));
        let actual: Vec<(Option<&str>, &str)> = decoded
            .iter()
            .map(|event| (event.event.as_deref(), event.data.as_str()))
            .collect();
        assert_eq!(actual, *expected, "fixture {name} decoded differently");
        whole
            .finish()
            .unwrap_or_else(|error| panic!("valid fixture {name} did not end cleanly: {error}"));

        for cut in 1..body.len() {
            if !body.is_char_boundary(cut) {
                continue;
            }
            let split = drive(body, &[cut], body.len().max(1));
            assert_eq!(
                split.events, decoded,
                "fixture {name} decoded differently when split at {cut}"
            );
            assert!(
                split.finish.is_ok(),
                "fixture {name} did not end cleanly when split at {cut}"
            );
        }
    }
}

/// Valid streams and what they must decode to, forever.
type Fixture = (
    &'static str,
    &'static str,
    &'static [(Option<&'static str>, &'static str)],
);

const FIXTURES: &[Fixture] = &[
    (
        "openai-chat",
        "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n",
        &[
            (None, "{\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}"),
            (None, "[DONE]"),
        ],
    ),
    (
        "anthropic-crlf-named-events",
        "event: message_start\r\ndata: {\"type\":\"message_start\"}\r\n\r\nevent: message_stop\r\n\
         data: {\"type\":\"message_stop\"}\r\n\r\n",
        &[
            (Some("message_start"), "{\"type\":\"message_start\"}"),
            (Some("message_stop"), "{\"type\":\"message_stop\"}"),
        ],
    ),
    (
        "multiline-data-and-comments",
        ": ping\n\ndata: first\ndata: second\n\n",
        &[(None, "first\nsecond")],
    ),
    (
        "utf8-payload",
        "data: {\"content\":\"héllo — 🌍\"}\n\n",
        &[(None, "{\"content\":\"héllo — 🌍\"}")],
    ),
];
