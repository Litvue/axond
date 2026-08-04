//! Server-sent-events relay for streamed completions.
//!
//! The relay decodes the upstream stream with `gateway-core` (`SseDecoder` +
//! the provider's `ProviderStreamDecoder`) and re-emits OpenAI-shaped chunks,
//! so a target reaches the caller in the OpenAI chunk shape whichever wire it
//! spoke upstream. Bytes are forwarded event-by-event as they decode:
//! nothing buffers a whole response, and the outbound body inherits the
//! client's backpressure because axum only polls it as the socket drains.
//!
//! Accounting is attached to the body, not to the handler: a client that hangs
//! up mid-stream drops the body, which drops the upstream response (cancelling
//! the in-flight request) and commits the spend accrued so far. See ADR 0005.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::time::Instant;

use axum::body::Body;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::Response;
use bytes::Bytes;
use futures::StreamExt;
use gateway_core::{
    ModelPrice, ModelUsage, ProviderAdapter, ProviderRequest, ProviderStreamDecoder,
    ProviderStreamEvent, SseDecoder, Surface,
};
use gateway_transport::{ByteStream, TransportError, Upstream};
use serde_json::{Value, json};
use tracing::Instrument;

use crate::budget::BudgetKey;
use crate::credentials::CredentialSource;
use crate::routes::next_request_id;
use crate::state::AppState;
use crate::telemetry;
use crate::usage::{Status, UsageRecord};

/// Everything the relay needs to attribute a streamed request once it ends.
pub struct StreamContext {
    pub namespace: String,
    pub subject: String,
    pub alias: String,
    pub target_provider: String,
    pub target_model: String,
    pub source: CredentialSource,
    pub credential_id: String,
    /// Captured in the handler while the server span is live; settlement may run
    /// in a detached task where the span context is no longer current.
    pub trace_id: Option<String>,
    pub price: ModelPrice,
    pub budget_key: BudgetKey,
    /// Upstream target attempts made before this stream opened (or the walk
    /// gave up), so the settled record matches the buffered path's attribution.
    pub attempts: u32,
}

/// Attempt to open one target's upstream stream, wrapped in its per-attempt
/// child span. Returns the undecoded byte stream on success, or the transport
/// error so the failover loop can decide whether to advance to the next target
/// — failover is only possible here, *before* the first byte is relayed.
///
/// A non-success upstream status is reported by `dispatch_stream` before any
/// bytes flow, so a failed open never yields a partially-consumed stream.
pub async fn open_stream(
    state: &AppState,
    ctx: &StreamContext,
    adapter: &dyn ProviderAdapter,
    upstream: &Upstream,
    surface: Surface,
    request: ProviderRequest,
    attempt: u32,
) -> Result<ByteStream, TransportError> {
    // The attempt span covers opening the stream, which is where a failed
    // stream fails; the relayed body outlives it, so its TTFT is reported
    // through the metrics at settlement instead.
    let started = Instant::now();
    let attempt_span = telemetry::upstream_attempt_span(
        attempt,
        &ctx.target_provider,
        &ctx.target_model,
        UsageRecord::credential_source_str(ctx.source),
    );
    let opened = state
        .0
        .dispatcher
        .dispatch_stream(adapter, upstream, surface, request)
        .instrument(attempt_span.clone())
        .await;
    telemetry::finish_upstream_attempt(
        &attempt_span,
        if opened.is_ok() {
            telemetry::ATTEMPT_OK
        } else {
            telemetry::ATTEMPT_ERROR
        },
        started.elapsed().as_millis() as u64,
        None,
    );
    opened
}

/// Build the client-facing SSE response from an already-opened upstream stream.
///
/// The caller has committed to this target (the first byte is about to flow),
/// so there is no more failover: an upstream that fails mid-stream is surfaced
/// as a terminal `error` event followed by `[DONE]`.
pub fn relay_opened(
    state: AppState,
    ctx: StreamContext,
    decoder: Box<dyn ProviderStreamDecoder>,
    bytes: ByteStream,
    started: Instant,
) -> Response {
    let relay = Relay {
        bytes,
        carry: Vec::new(),
        sse: Some(SseDecoder::default()),
        decoder,
        pending: VecDeque::new(),
        phase: Phase::Streaming,
        accounting: Accounting::new(state, ctx, started),
    };

    let body = Body::from_stream(futures::stream::unfold(relay, |mut relay| async move {
        relay.next_chunk().await.map(|chunk| (chunk, relay))
    }));

    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    response
}

/// Settle a streamed request that never opened a stream (every target failed or
/// was skipped) as a single upstream-error usage record, so a failed stream
/// still reconciles exactly one record like the buffered path.
pub fn settle_upstream_error(state: AppState, ctx: StreamContext, started: Instant) {
    let mut accounting = Accounting::new(state, ctx, started);
    accounting.settle(Status::UpstreamError);
}

enum Phase {
    Streaming,
    Failed(String),
    Finished,
    Ended,
}

struct Relay {
    bytes: ByteStream,
    /// Trailing bytes of a UTF-8 sequence split across upstream chunks.
    carry: Vec<u8>,
    /// Taken at end of stream, where `finish` reports a truncated event.
    sse: Option<SseDecoder>,
    decoder: Box<dyn ProviderStreamDecoder>,
    pending: VecDeque<Bytes>,
    phase: Phase,
    accounting: Accounting,
}

impl Relay {
    async fn next_chunk(&mut self) -> Option<Result<Bytes, Infallible>> {
        loop {
            if let Some(chunk) = self.pending.pop_front() {
                return Some(Ok(chunk));
            }
            match &self.phase {
                Phase::Streaming => self.poll_upstream().await,
                Phase::Failed(message) => {
                    let message = message.clone();
                    self.pending.push_back(error_event(&message));
                    self.pending.push_back(done_event());
                    self.accounting.settle(Status::UpstreamError);
                    self.phase = Phase::Ended;
                }
                Phase::Finished => {
                    self.pending.push_back(done_event());
                    self.accounting.settle(Status::Ok);
                    self.phase = Phase::Ended;
                }
                Phase::Ended => return None,
            }
        }
    }

    async fn poll_upstream(&mut self) {
        match self.bytes.next().await {
            Some(Ok(chunk)) => {
                let text = self.decode_utf8(&chunk);
                let pushed = match self.sse.as_mut() {
                    Some(sse) => sse.push(&text),
                    None => Ok(Vec::new()),
                };
                match pushed {
                    Ok(events) => {
                        for event in events {
                            match self.decoder.decode(event) {
                                Ok(decoded) => self.emit(decoded),
                                Err(err) => {
                                    self.phase = Phase::Failed(err.to_string());
                                    return;
                                }
                            }
                        }
                    }
                    Err(err) => self.phase = Phase::Failed(err.to_string()),
                }
            }
            Some(Err(err)) => self.phase = Phase::Failed(err.to_string()),
            None => self.finish_upstream(),
        }
    }

    /// Chunk boundaries fall wherever the socket puts them, so a multi-byte
    /// character can straddle two chunks: only the valid prefix is decoded and
    /// the remainder waits for the next chunk. Genuinely invalid bytes are
    /// replaced rather than stalling the stream.
    fn decode_utf8(&mut self, chunk: &[u8]) -> String {
        self.carry.extend_from_slice(chunk);
        match std::str::from_utf8(&self.carry) {
            Ok(_) => {
                let text = String::from_utf8_lossy(&self.carry).into_owned();
                self.carry.clear();
                text
            }
            Err(err) if err.error_len().is_none() => {
                let rest = self.carry.split_off(err.valid_up_to());
                let text = String::from_utf8_lossy(&self.carry).into_owned();
                self.carry = rest;
                text
            }
            Err(_) => {
                let text = String::from_utf8_lossy(&self.carry).into_owned();
                self.carry.clear();
                text
            }
        }
    }

    /// An upstream that ends mid-event is a truncated answer, not a complete
    /// one: `SseDecoder::finish` reports the leftover so the caller gets an
    /// error rather than a `[DONE]` it would read as success.
    fn finish_upstream(&mut self) {
        if let Some(sse) = self.sse.take()
            && let Err(err) = sse.finish()
        {
            self.phase = Phase::Failed(err.to_string());
            return;
        }
        if !self.carry.is_empty() {
            self.phase = Phase::Failed("stream ended mid-character".to_owned());
            return;
        }
        match self.decoder.finish() {
            Ok(decoded) => {
                self.emit(decoded);
                self.phase = Phase::Finished;
            }
            Err(err) => self.phase = Phase::Failed(err.to_string()),
        }
    }

    /// Frame decoded events for the client. `Done` carries the stream's
    /// authoritative usage; the `[DONE]` sentinel itself is written once, from
    /// the terminal phase, so a provider that ends the connection without one
    /// still gets a well-formed close.
    fn emit(&mut self, events: Vec<ProviderStreamEvent>) {
        for event in events {
            match event {
                ProviderStreamEvent::Data { event, data } => {
                    self.accounting.mark_first_token();
                    self.pending.push_back(data_event(event.as_deref(), &data));
                }
                ProviderStreamEvent::Done(usage) => {
                    self.accounting.usage = usage;
                    self.phase = Phase::Finished;
                }
            }
        }
    }
}

fn data_event(name: Option<&str>, data: &Value) -> Bytes {
    let payload = serde_json::to_string(data).unwrap_or_else(|_| "{}".to_owned());
    match name {
        Some(name) => Bytes::from(format!("event: {name}\ndata: {payload}\n\n")),
        None => Bytes::from(format!("data: {payload}\n\n")),
    }
}

fn done_event() -> Bytes {
    Bytes::from_static(b"data: [DONE]\n\n")
}

fn error_event(message: &str) -> Bytes {
    let payload = json!({ "error": { "type": "upstream_stream_error", "message": message } });
    Bytes::from(format!(
        "event: error\ndata: {}\n\n",
        serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_owned())
    ))
}

/// Terminal accounting for one streamed request: exactly one usage record and
/// exactly one budget commit, whichever way the stream ends.
///
/// The `Drop` arm is the cancellation path — a dropped body means the client
/// went away, so the spend accrued up to that point is still charged. Partial
/// spend is committed through the existing `BudgetStore::commit`; a dedicated
/// reserve/release lifecycle belongs with the durable backends (#6).
struct Accounting {
    state: AppState,
    ctx: StreamContext,
    started: Instant,
    usage: ModelUsage,
    /// Time to the first relayed token, which for a stream is the number a
    /// caller actually feels.
    ttft_ms: Option<u64>,
    settled: bool,
}

impl Accounting {
    fn new(state: AppState, ctx: StreamContext, started: Instant) -> Self {
        Self {
            state,
            ctx,
            started,
            usage: ModelUsage::default(),
            ttft_ms: None,
            settled: false,
        }
    }

    fn mark_first_token(&mut self) {
        self.ttft_ms
            .get_or_insert_with(|| self.started.elapsed().as_millis() as u64);
    }

    fn settle(&mut self, status: Status) {
        if self.settled {
            return;
        }
        self.settled = true;
        let state = self.state.clone();
        let usage = self.usage;
        let latency_ms = self.started.elapsed().as_millis() as u64;
        let cost = self.ctx.price.cost_microdollars(gateway_core::Usage {
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            reasoning_tokens: usage.reasoning_tokens,
            cache_read_tokens: usage.cache_read_tokens,
            cache_write_tokens: usage.cache_write_tokens,
        });
        let record = UsageRecord {
            schema_version: UsageRecord::SCHEMA_VERSION,
            request_id: next_request_id(),
            namespace: self.ctx.namespace.clone(),
            subject: self.ctx.subject.clone(),
            model: self.ctx.alias.clone(),
            target_provider: self.ctx.target_provider.clone(),
            target_model: self.ctx.target_model.clone(),
            credential_source: UsageRecord::credential_source_str(self.ctx.source),
            credential_id: self.ctx.credential_id.clone(),
            trace_id: self.ctx.trace_id.clone(),
            status,
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            cost_microdollars: cost,
            catalog_version: 0,
            latency_ms,
            attempts: self.ctx.attempts,
        };
        telemetry::record_streamed(&record, self.ttft_ms);
        let budget_key = self.ctx.budget_key.clone();
        spawn_settlement(async move {
            state.0.budget.commit(&budget_key, cost).await;
            state.0.usage.record(&record).await;
        });
    }
}

impl Drop for Accounting {
    fn drop(&mut self) {
        self.settle(Status::ClientCancelled);
    }
}

/// Settlement outlives the request body, so it runs detached. Outside a
/// runtime (process teardown) there is nothing left to settle onto.
fn spawn_settlement<F>(future: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(future);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::post;
    use futures::StreamExt;
    use serde_json::json;
    use tower::util::ServiceExt;

    use crate::budget::{Admission, BudgetStore};
    use crate::config::Config;
    use crate::routes::router;
    use crate::usage::{UsageFanout, UsageSink};

    use super::*;

    #[derive(Default)]
    struct Ledger {
        records: Mutex<Vec<Value>>,
        commits: Mutex<Vec<u64>>,
    }

    struct LedgerSink(Arc<Ledger>);

    #[async_trait]
    impl UsageSink for LedgerSink {
        fn name(&self) -> &'static str {
            "ledger"
        }
        async fn record(&self, record: &UsageRecord) {
            let value = serde_json::to_value(record).expect("record serializes");
            self.0.records.lock().expect("ledger").push(value);
        }
    }

    struct LedgerBudget(Arc<Ledger>);

    #[async_trait]
    impl BudgetStore for LedgerBudget {
        fn name(&self) -> &'static str {
            "ledger"
        }
        async fn reserve(&self, _key: &BudgetKey, _estimated_microdollars: u64) -> Admission {
            Admission::Allowed
        }
        async fn commit(&self, _key: &BudgetKey, actual_microdollars: u64) {
            self.0
                .commits
                .lock()
                .expect("ledger")
                .push(actual_microdollars);
        }
    }

    /// A fake upstream that replies with a fixed SSE body.
    async fn upstream_serving(body: &'static str) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let app = axum::Router::new().route(
            "/chat/completions",
            post(move || async move {
                (
                    [(header::CONTENT_TYPE, "text/event-stream")],
                    Body::from(body),
                )
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    fn state_for(base_url: &str, ledger: Arc<Ledger>) -> AppState {
        let cfg = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "{base_url}"

[[credential]]
namespace = "platform"
provider = "openai"
env = "GW_TEST_OPENAI_KEY"

[[model]]
name = "gpt-4o"
targets = [{{ provider = "openai", model = "gpt-4o", price = {{ input_microdollars_per_million = 1000000, output_microdollars_per_million = 2000000 }} }}]
"#
        ))
        .expect("config");
        let env = HashMap::from([("GW_TEST_OPENAI_KEY".to_owned(), "sk-test".to_owned())]);
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(LedgerSink(ledger.clone()))];
        AppState::new(
            cfg,
            &env,
            UsageFanout::new(sinks),
            Box::new(LedgerBudget(ledger)),
        )
        .expect("state")
    }

    fn stream_request() -> Request<Body> {
        let body = json!({
            "model": "gpt-4o",
            "stream": true,
            "stream_options": { "include_usage": true },
            "messages": [{ "role": "user", "content": "hi" }]
        });
        Request::post("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).expect("body")))
            .expect("request")
    }

    fn context() -> StreamContext {
        StreamContext {
            namespace: "platform".to_owned(),
            subject: "anonymous".to_owned(),
            alias: "gpt-4o".to_owned(),
            target_provider: "openai".to_owned(),
            target_model: "gpt-4o".to_owned(),
            source: CredentialSource::Platform,
            credential_id: "openai-primary".to_owned(),
            trace_id: None,
            price: ModelPrice {
                input_microdollars_per_million: 1_000_000,
                output_microdollars_per_million: 2_000_000,
                reasoning_microdollars_per_million: None,
                cache_read_microdollars_per_million: None,
                cache_write_microdollars_per_million: None,
            },
            budget_key: BudgetKey {
                namespace: "platform".to_owned(),
                subject: "anonymous".to_owned(),
            },
            attempts: 1,
        }
    }

    /// The ledger is written from a detached settlement task; poll briefly
    /// rather than racing it.
    async fn settled(ledger: &Ledger) -> Value {
        for _ in 0..100 {
            if let Some(record) = ledger.records.lock().expect("ledger").first() {
                return record.clone();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("no usage record was settled");
    }

    const OPENAI_STREAM: &str = concat!(
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hel\"}}]}\n\n",
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"}}]}\n\n",
        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":3}}\n\n",
        "data: [DONE]\n\n",
    );

    #[tokio::test]
    async fn relays_events_and_settles_one_usage_record() {
        let ledger = Arc::new(Ledger::default());
        let base_url = upstream_serving(OPENAI_STREAM).await;
        let resp = router(state_for(&base_url, ledger.clone()))
            .oneshot(stream_request())
            .await
            .expect("response");

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("text/event-stream")
        );

        let mut body = resp.into_body().into_data_stream();
        let mut relayed = String::new();
        while let Some(chunk) = body.next().await {
            relayed.push_str(&String::from_utf8_lossy(&chunk.expect("chunk")));
        }
        assert_eq!(relayed.matches("data: ").count(), 4);
        assert!(relayed.contains("\"content\":\"hel\""));
        assert!(relayed.ends_with("data: [DONE]\n\n"));

        let record = settled(&ledger).await;
        assert_eq!(ledger.records.lock().expect("ledger").len(), 1);
        assert_eq!(record["status"], "ok");
        assert_eq!(record["input_tokens"], 11);
        assert_eq!(record["output_tokens"], 3);
        // 11 input @ 1 µ$/token + 3 output @ 2 µ$/token.
        assert_eq!(record["cost_microdollars"], 17);
        assert_eq!(*ledger.commits.lock().expect("ledger"), vec![17]);
    }

    #[tokio::test]
    async fn mid_stream_upstream_failure_becomes_a_terminal_error_event() {
        let ledger = Arc::new(Ledger::default());
        let base_url = upstream_serving(concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n",
            "data: {not json}\n\n",
        ))
        .await;
        let resp = router(state_for(&base_url, ledger.clone()))
            .oneshot(stream_request())
            .await
            .expect("response");
        assert_eq!(resp.status(), StatusCode::OK);

        let mut body = resp.into_body().into_data_stream();
        let mut relayed = String::new();
        while let Some(chunk) = body.next().await {
            relayed.push_str(&String::from_utf8_lossy(&chunk.expect("chunk")));
        }
        assert!(relayed.contains("event: error"));
        assert!(relayed.ends_with("data: [DONE]\n\n"));

        let record = settled(&ledger).await;
        assert_eq!(record["status"], "upstream_error");
    }

    #[tokio::test]
    async fn multibyte_characters_survive_a_chunk_boundary() {
        let mut relay = Relay {
            bytes: Box::pin(futures::stream::empty()),
            carry: Vec::new(),
            sse: Some(SseDecoder::default()),
            decoder: gateway_core::OpenAiCompatibleAdapter::openai()
                .stream_decoder(Surface::ChatCompletions)
                .expect("decoder"),
            pending: VecDeque::new(),
            phase: Phase::Streaming,
            accounting: Accounting::new(
                state_for("http://127.0.0.1:1", Arc::new(Ledger::default())),
                context(),
                Instant::now(),
            ),
        };
        let text = "héllo — 日本語";
        let bytes = text.as_bytes();
        let mut decoded = String::new();
        for chunk in bytes.chunks(3) {
            decoded.push_str(&relay.decode_utf8(chunk));
        }
        assert_eq!(decoded, text);
        assert!(relay.carry.is_empty());
    }

    #[tokio::test]
    async fn a_truncated_final_event_is_reported_rather_than_completed() {
        let ledger = Arc::new(Ledger::default());
        let base_url = upstream_serving(concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\"",
        ))
        .await;
        let resp = router(state_for(&base_url, ledger.clone()))
            .oneshot(stream_request())
            .await
            .expect("response");

        let mut body = resp.into_body().into_data_stream();
        let mut relayed = String::new();
        while let Some(chunk) = body.next().await {
            relayed.push_str(&String::from_utf8_lossy(&chunk.expect("chunk")));
        }
        assert!(relayed.contains("event: error"));
        assert!(relayed.contains("incomplete"));

        let record = settled(&ledger).await;
        assert_eq!(record["status"], "upstream_error");
    }

    #[tokio::test]
    async fn client_disconnect_settles_the_stream_as_cancelled() {
        let ledger = Arc::new(Ledger::default());
        let base_url = upstream_serving(OPENAI_STREAM).await;
        let resp = router(state_for(&base_url, ledger.clone()))
            .oneshot(stream_request())
            .await
            .expect("response");

        let mut body = resp.into_body().into_data_stream();
        body.next().await.expect("first chunk").expect("chunk");
        drop(body);

        let record = settled(&ledger).await;
        assert_eq!(record["status"], "client_cancelled");
        assert_eq!(ledger.commits.lock().expect("ledger").len(), 1);
    }

    /// An upstream that never opens a stream: it answers a non-200 status, so
    /// the dispatch fails before a single byte is relayed.
    async fn failing_to_open_upstream(status: StatusCode) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let app = axum::Router::new().route(
            "/chat/completions",
            post(move || async move { (status, "upstream is unavailable") }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    fn two_target_stream_state(url_a: &str, url_b: &str, ledger: Arc<Ledger>) -> AppState {
        let cfg = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "pa"
kind = "openai"
base_url = "{url_a}"

[[provider]]
id = "pb"
kind = "openai"
base_url = "{url_b}"

[[credential]]
namespace = "platform"
provider = "pa"
env = "KA"

[[credential]]
namespace = "platform"
provider = "pb"
env = "KB"

[[model]]
name = "gpt-4o"
targets = [
  {{ provider = "pa", model = "m-a", price = {{ input_microdollars_per_million = 1000000, output_microdollars_per_million = 2000000 }} }},
  {{ provider = "pb", model = "m-b", price = {{ input_microdollars_per_million = 1000000, output_microdollars_per_million = 2000000 }} }},
]
"#
        ))
        .expect("config");
        let env = HashMap::from([
            ("KA".to_owned(), "a".to_owned()),
            ("KB".to_owned(), "b".to_owned()),
        ]);
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(LedgerSink(ledger.clone()))];
        AppState::new(
            cfg,
            &env,
            UsageFanout::new(sinks),
            Box::new(LedgerBudget(ledger)),
        )
        .expect("state")
    }

    #[tokio::test]
    async fn streaming_fails_over_to_the_next_target_before_the_first_byte() {
        let ledger = Arc::new(Ledger::default());
        let url_a = failing_to_open_upstream(StatusCode::INTERNAL_SERVER_ERROR).await;
        let url_b = upstream_serving(OPENAI_STREAM).await;
        let resp = router(two_target_stream_state(&url_a, &url_b, ledger.clone()))
            .oneshot(stream_request())
            .await
            .expect("response");

        // The first target failed to open, so failover picked the second and the
        // client only ever saw a successful `200` stream.
        assert_eq!(resp.status(), StatusCode::OK);
        let mut body = resp.into_body().into_data_stream();
        let mut relayed = String::new();
        while let Some(chunk) = body.next().await {
            relayed.push_str(&String::from_utf8_lossy(&chunk.expect("chunk")));
        }
        assert!(relayed.contains("\"content\":\"hel\""));
        assert!(relayed.ends_with("data: [DONE]\n\n"));

        let record = settled(&ledger).await;
        assert_eq!(record["status"], "ok");
        assert_eq!(record["target_provider"], "pb");
        assert_eq!(record["target_model"], "m-b");
        assert_eq!(record["attempts"], 2);
    }
}
