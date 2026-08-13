//! A fake provider upstream: the OpenAI and Anthropic wire shapes, served from
//! committed fixtures with no network.
//!
//! The server records what the gateway sent it (rewritten model, injected
//! credential, wire headers, body) and tracks how many response bodies are
//! open, which is what makes an upstream connection leak observable: a soak run
//! asserts opens and closes balance once the clients are gone.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use bytes::Bytes;
use serde_json::{Value, json};
use tokio::sync::oneshot;

/// Target model names the fake upstream understands. An alias in the test
/// config points at one of these, so a test picks upstream behaviour by asking
/// for a model rather than by reaching into the server.
pub mod target {
    /// Buffered/streamed OpenAI chat from the committed fixtures.
    pub const CHAT: &str = "fixture-chat";
    /// Buffered OpenAI embeddings from the committed fixture.
    pub const EMBEDDINGS: &str = "fixture-embeddings";
    pub const RESPONSES: &str = "fixture-responses";
    /// Buffered/streamed Anthropic Messages from the committed fixtures.
    pub const MESSAGES: &str = "fixture-messages";
    /// A long-lived stream of many small events, for soak.
    pub const SLOW_STREAM: &str = "slow-stream";
    /// A stream that dies mid-event after relaying some output.
    pub const DROP_STREAM: &str = "drop-stream";
    /// An upstream that answers `500` before any byte is relayed.
    pub const FAIL: &str = "fail-500";
    /// An upstream that answers `429` before any byte is relayed: the provider
    /// verdict that is attributable to the credential rather than to the target.
    pub const RATE_LIMITED: &str = "rate-limited-429";
    /// Accepts the request and never sends response headers.
    pub const NO_HEADERS: &str = "no-headers";
    /// Thinks for a while and *then* answers, like a non-streamed completion:
    /// no headers arrive until the whole answer exists.
    pub const LATE_HEADERS: &str = "late-headers";
    /// Sends `200` headers immediately, then never finishes the body.
    pub const SLOW_BODY: &str = "slow-body";
    /// A buffered `200` body far larger than a test's byte bound.
    pub const HUGE_BODY: &str = "huge-body";
    /// A `500` whose *error* body is far larger than a test's byte bound.
    pub const HUGE_ERROR: &str = "huge-error";
    /// Opens an SSE stream and then goes silent before any event.
    pub const STALL_STREAM: &str = "stall-stream";
    /// Relays a few events and then goes silent, with bytes already committed.
    pub const STALL_AFTER_BYTES: &str = "stall-after-bytes";
    /// A stream that keeps producing, slowly, for longer than a short failover
    /// budget: productive, so it must not be cut off.
    pub const LONG_STREAM: &str = "long-stream";
    /// Buffered OpenAI chat answers of a fixed size, for a response-size sweep.
    /// Unlike [`HUGE_BODY`] these are well-formed completions carrying usage, so
    /// the size is the only variable: the request still settles a real charge.
    pub const SIZED_BODY_SMALL: &str = "sized-body-1k";
    pub const SIZED_BODY_MEDIUM: &str = "sized-body-32k";
    pub const SIZED_BODY_LARGE: &str = "sized-body-256k";
}

/// Answer sizes [`target::SIZED_BODY_SMALL`] and friends serve, in bytes of
/// completion text.
pub const SIZED_BODIES: [(&str, usize); 3] = [
    (target::SIZED_BODY_SMALL, 1024),
    (target::SIZED_BODY_MEDIUM, 32 * 1024),
    (target::SIZED_BODY_LARGE, 256 * 1024),
];

/// Long enough that the bound under test always fires first, short enough that
/// a leaked task cannot outlive the suite.
const FOREVER: Duration = Duration::from_secs(60);

/// How long [`target::LATE_HEADERS`] withholds its answer. Long enough that a
/// header bound tightened below it would fail the request.
const THINKING: Duration = Duration::from_millis(2_000);

/// Filler bytes in the oversized bodies: well above the byte bounds the
/// transport suite configures, cheap enough to build per request.
const OVERSIZED: usize = 512 * 1024;

/// How a scripted stream is paced between chunks.
#[derive(Clone, Copy)]
enum Pace {
    None,
    /// Soak pacing: many streams, small gaps.
    Fast,
    /// Slower than a short failover budget, so a productive stream outlives it.
    Slow,
}

impl Pace {
    fn gap(self) -> Option<Duration> {
        match self {
            Self::None => None,
            Self::Fast => Some(Duration::from_millis(5)),
            Self::Slow => Some(Duration::from_millis(60)),
        }
    }
}

/// One upstream request as the fake saw it.
#[derive(Clone, Debug)]
pub struct Recorded {
    pub path: String,
    pub model: String,
    pub authorization: Option<String>,
    pub api_key: Option<String>,
    pub anthropic_version: Option<String>,
    pub anthropic_beta: Option<String>,
    pub body: Value,
}

#[derive(Default)]
struct Counters {
    open: AtomicI64,
    opened: AtomicU64,
    received: AtomicU64,
}

/// How many recorded requests the fake keeps. Assertions read the last request,
/// or all of a handful; an endurance run offers millions, and retaining every
/// body would make the *harness* the thing that runs out of memory. The count
/// is exact regardless ([`UpstreamState::received`]).
const RECORDED_LIMIT: usize = 1024;

/// Fixture bytes, loaded once at boot so replay never touches the filesystem
/// mid-request.
struct Fixtures {
    files: HashMap<&'static str, Bytes>,
}

impl Fixtures {
    fn load() -> Self {
        let root = fixtures_dir();
        let names = [
            "openai/chat_completion.json",
            "openai/chat_completion.sse",
            "openai/embeddings.json",
            "openai/responses.json",
            "openai/responses.sse",
            "anthropic/message_thinking_tool_use.json",
            "anthropic/message_thinking_tool_use.sse",
        ];
        let files = names
            .into_iter()
            .map(|name| {
                let bytes = std::fs::read(root.join(name))
                    .unwrap_or_else(|e| panic!("fixture `{name}` is unreadable: {e}"));
                (name, Bytes::from(bytes))
            })
            .collect();
        Self { files }
    }

    fn get(&self, name: &str) -> Bytes {
        self.files
            .get(name)
            .unwrap_or_else(|| panic!("unknown fixture `{name}`"))
            .clone()
    }
}

/// The committed fixture tree, resolved from this crate rather than from the
/// working directory the test runner happens to have.
pub fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures")
}

pub fn fixture(name: &str) -> Bytes {
    Bytes::from(std::fs::read(fixtures_dir().join(name)).expect("fixture is readable"))
}

/// The separator a target model name may carry a caller label behind, so a
/// request that is otherwise identical to another caller's can still be told
/// apart at the upstream. Everything before it selects the fixture behaviour;
/// everything after it names who was routed here.
pub const CALLER_TAG: char = '#';

/// One caller reaching the upstream under one credential. Two callers whose
/// keys were swapped with each other produce two of these, each naming the
/// wrong pairing, where two per-credential totals would each still balance.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Dispatch {
    /// The label behind [`CALLER_TAG`] on the target model, or empty for a
    /// request routed by a target that carries none.
    pub caller: String,
    /// [`credential_digest`] of the presented material, or [`UNCREDENTIALED`]
    /// when the request carried none. Recording the absence is what keeps an
    /// uncredentialed dispatch visible to a count of who paid for what, rather
    /// than dropping it and leaving the tally looking clean.
    pub credential: String,
}

/// The credential label of a request that presented none. It is not a digest,
/// and no key can produce it, so nobody owns it — which is what makes such a
/// call foreign to every caller.
pub const UNCREDENTIALED: &str = "none";

/// A stable, short, non-reversing name for a presented credential. The upstream
/// keeps this rather than the material, so a tally can be printed, serialised,
/// or retained without carrying a secret with it; a caller that knows a key
/// recognises it by digesting its own copy.
pub fn credential_digest(presented: &str) -> String {
    let material = presented
        .strip_prefix("Bearer ")
        .unwrap_or(presented)
        .trim();
    let digest = ring::digest::digest(&ring::digest::SHA256, material.as_bytes());
    digest.as_ref()[..8]
        .iter()
        .fold(String::with_capacity(16), |mut hex: String, byte: &u8| {
            use std::fmt::Write;
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

pub struct UpstreamState {
    requests: Mutex<VecDeque<Recorded>>,
    /// How many requests arrived from each caller bearing each credential, kept
    /// for every request rather than only the retained ones: a multi-tenant run
    /// offers more requests than [`RECORDED_LIMIT`], and "was anyone served
    /// with someone else's key" has to be answered over all of them — and per
    /// request, because two callers whose keys were swapped with each other are
    /// invisible in two separate totals. No secret is kept: the presented
    /// material is reduced to a digest on arrival, which a caller compares
    /// against the digest of a key it already holds.
    dispatches: Mutex<BTreeMap<Dispatch, u64>>,
    counters: Counters,
    fixtures: Fixtures,
}

impl UpstreamState {
    /// The requests the gateway has made, in arrival order — the most recent
    /// [`RECORDED_LIMIT`] of them.
    pub fn requests(&self) -> Vec<Recorded> {
        self.requests
            .lock()
            .expect("upstream lock")
            .iter()
            .cloned()
            .collect()
    }

    /// Who asked, what credential answered for them, and how many times. Exact
    /// over the whole run.
    pub fn dispatches(&self) -> BTreeMap<Dispatch, u64> {
        self.dispatches.lock().expect("upstream lock").clone()
    }

    /// How many requests arrived, whether or not they were retained.
    pub fn received(&self) -> u64 {
        self.counters.received.load(Ordering::SeqCst)
    }

    pub fn last_request(&self) -> Recorded {
        self.requests()
            .pop()
            .expect("the gateway made an upstream request")
    }

    /// Upstream responses currently open: streamed bodies, and the buffered
    /// attempts that are still stalling. Balanced opens and closes are the leak
    /// assertion: a cancelled client must take its upstream with it.
    pub fn open_streams(&self) -> i64 {
        self.counters.open.load(Ordering::SeqCst)
    }

    pub fn opened_streams(&self) -> u64 {
        self.counters.opened.load(Ordering::SeqCst)
    }
}

/// Decrements the open-response count whenever a response body — or a handler
/// still deciding on one — is dropped: completed, cancelled, or aborted alike.
struct ConnGuard(Arc<UpstreamState>);

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.counters.open.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct FakeUpstream {
    pub base_url: String,
    pub state: Arc<UpstreamState>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl FakeUpstream {
    pub async fn start() -> Self {
        let state = Arc::new(UpstreamState {
            requests: Mutex::new(VecDeque::new()),
            dispatches: Mutex::new(BTreeMap::new()),
            counters: Counters::default(),
            fixtures: Fixtures::load(),
        });
        let app = Router::new()
            .route("/chat/completions", post(handle))
            .route("/messages", post(handle))
            .route("/embeddings", post(handle))
            .route("/responses", post(handle))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .expect("fake upstream binds");
        let addr = listener.local_addr().expect("fake upstream has an address");
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = rx.await;
                })
                .await;
        });
        Self {
            base_url: format!("http://{addr}"),
            state,
            shutdown: Some(tx),
        }
    }
}

impl Drop for FakeUpstream {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

async fn handle(
    State(state): State<Arc<UpstreamState>>,
    headers: HeaderMap,
    path: axum::extract::OriginalUri,
    body: Bytes,
) -> Response {
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let model = body
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let streamed = body.get("stream").and_then(Value::as_bool) == Some(true);
    let path = path.0.path().to_owned();
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    let (model, caller) = match model.split_once(CALLER_TAG) {
        Some((target, caller)) => (target.to_owned(), caller.to_owned()),
        None => (model, String::new()),
    };
    state.counters.received.fetch_add(1, Ordering::SeqCst);
    *state
        .dispatches
        .lock()
        .expect("upstream lock")
        .entry(Dispatch {
            caller,
            credential: header("authorization")
                .or_else(|| header("x-api-key"))
                .map_or_else(|| UNCREDENTIALED.to_owned(), |p| credential_digest(&p)),
        })
        .or_default() += 1;
    {
        let mut recorded = state.requests.lock().expect("upstream lock");
        if recorded.len() == RECORDED_LIMIT {
            recorded.pop_front();
        }
        recorded.push_back(Recorded {
            path: path.clone(),
            model: model.clone(),
            authorization: header("authorization"),
            api_key: header("x-api-key"),
            anthropic_version: header("anthropic-version"),
            anthropic_beta: header("anthropic-beta"),
            body,
        });
    }

    let anthropic = path == "/messages";
    let responses = path == "/responses";
    match model.as_str() {
        target::FAIL => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [("content-type", "application/json")],
            json!({ "error": { "type": "server_error", "message": "fixture failure" } })
                .to_string(),
        )
            .into_response(),
        target::RATE_LIMITED => (
            StatusCode::TOO_MANY_REQUESTS,
            [("content-type", "application/json"), ("retry-after", "1")],
            json!({ "error": { "type": "rate_limit_error", "message": "fixture rate limit" } })
                .to_string(),
        )
            .into_response(),
        target::SLOW_STREAM => sse(state.clone(), slow_events(anthropic, 40), Pace::Fast),
        target::DROP_STREAM => sse(state.clone(), truncated_events(anthropic), Pace::None),
        target::LONG_STREAM => sse(state.clone(), slow_events(anthropic, 20), Pace::Slow),
        target::STALL_STREAM => stalling_sse(state.clone(), Vec::new()),
        target::STALL_AFTER_BYTES => {
            let mut events = slow_events(anthropic, 2);
            events.truncate(if anthropic { 4 } else { 2 });
            stalling_sse(state.clone(), events)
        }
        // Accepts the request and never answers: only a header bound ends this.
        // The guard is held across the wait, so an abandoned buffered attempt is
        // as observable as an abandoned stream: it is released when the gateway
        // drops the connection and this handler is cancelled with it.
        target::NO_HEADERS => {
            let _guard = open_guard(state.clone());
            tokio::time::sleep(FOREVER).await;
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
        // A slow *successful* completion: the answer is worth waiting for, and
        // the header bound must not be what decides it is not.
        target::LATE_HEADERS => {
            tokio::time::sleep(THINKING).await;
            (
                StatusCode::OK,
                [("content-type", "application/json")],
                state.fixtures.get("openai/chat_completion.json"),
            )
                .into_response()
        }
        // Headers land at once, so only a body bound ends this. The unfinished
        // body holds the guard, so abandoning it is observable.
        target::SLOW_BODY => {
            let guard = open_guard(state.clone());
            let stream = futures::stream::unfold(guard, |guard| async {
                tokio::time::sleep(FOREVER).await;
                Some((
                    Ok::<Bytes, std::io::Error>(Bytes::from_static(b"{}")),
                    guard,
                ))
            });
            let mut response = Response::new(Body::from_stream(stream));
            response
                .headers_mut()
                .insert("content-type", "application/json".parse().expect("static"));
            response
        }
        target::SIZED_BODY_SMALL | target::SIZED_BODY_MEDIUM | target::SIZED_BODY_LARGE => {
            let text = SIZED_BODIES
                .iter()
                .find_map(|(name, bytes)| (*name == model).then_some(*bytes))
                .expect("a sized target has a size");
            (
                StatusCode::OK,
                [("content-type", "application/json")],
                sized_completion(&model, text),
            )
                .into_response()
        }
        target::HUGE_BODY => (
            StatusCode::OK,
            [("content-type", "application/json")],
            json!({ "filler": "x".repeat(OVERSIZED) }).to_string(),
        )
            .into_response(),
        target::HUGE_ERROR => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [("content-type", "application/json")],
            json!({ "error": { "type": "server_error", "message": "x".repeat(OVERSIZED) } })
                .to_string(),
        )
            .into_response(),
        _ if streamed => {
            let name = if anthropic {
                "anthropic/message_thinking_tool_use.sse"
            } else if responses {
                "openai/responses.sse"
            } else {
                "openai/chat_completion.sse"
            };
            sse(
                state.clone(),
                split_events(&state.fixtures.get(name)),
                Pace::None,
            )
        }
        _ => {
            let name = match (anthropic, path.as_str()) {
                (true, _) => "anthropic/message_thinking_tool_use.json",
                (_, "/embeddings") => "openai/embeddings.json",
                (_, "/responses") => "openai/responses.json",
                _ => "openai/chat_completion.json",
            };
            (
                StatusCode::OK,
                [("content-type", "application/json")],
                state.fixtures.get(name),
            )
                .into_response()
        }
    }
}

/// Serve a scripted event sequence as a real chunked SSE response, one chunk
/// per element, holding the open-stream guard for the body's whole life.
fn sse(state: Arc<UpstreamState>, chunks: Vec<Bytes>, pace: Pace) -> Response {
    let guard = open_guard(state);
    let stream = futures::stream::unfold(
        (chunks.into_iter(), guard),
        move |(mut chunks, guard)| async move {
            let chunk = chunks.next()?;
            if let Some(gap) = pace.gap() {
                tokio::time::sleep(gap).await;
            }
            Some((Ok::<Bytes, std::io::Error>(chunk), (chunks, guard)))
        },
    );
    event_stream(Body::from_stream(stream))
}

/// Serve `chunks` and then go silent with the body still open: an idle stream,
/// which is what a stream-idle bound exists for. The open-stream guard is held
/// until the gateway drops the connection, so cleanup stays observable.
fn stalling_sse(state: Arc<UpstreamState>, chunks: Vec<Bytes>) -> Response {
    let guard = open_guard(state);
    let stream = futures::stream::unfold(
        (chunks.into_iter(), guard),
        move |(mut chunks, guard)| async move {
            match chunks.next() {
                Some(chunk) => Some((Ok::<Bytes, std::io::Error>(chunk), (chunks, guard))),
                None => {
                    tokio::time::sleep(FOREVER).await;
                    None
                }
            }
        },
    );
    event_stream(Body::from_stream(stream))
}

fn open_guard(state: Arc<UpstreamState>) -> ConnGuard {
    state.counters.open.fetch_add(1, Ordering::SeqCst);
    state.counters.opened.fetch_add(1, Ordering::SeqCst);
    ConnGuard(state)
}

fn event_stream(body: Body) -> Response {
    let mut response = Response::new(body);
    response
        .headers_mut()
        .insert("content-type", "text/event-stream".parse().expect("static"));
    response
}

/// Split recorded SSE bytes into one chunk per event, then halve each chunk, so
/// the relay sees events that straddle a chunk boundary the way a real socket
/// delivers them.
fn split_events(bytes: &Bytes) -> Vec<Bytes> {
    let text = std::str::from_utf8(bytes).expect("fixtures are UTF-8");
    let mut chunks = Vec::new();
    for event in text.split_inclusive("\n\n") {
        let event = Bytes::copy_from_slice(event.as_bytes());
        let mid = event.len() / 2;
        chunks.push(event.slice(..mid));
        chunks.push(event.slice(mid..));
    }
    chunks
}

/// A long stream of small events in the wire shape the route speaks, ending
/// cleanly. Used to hold hundreds of streams open at once.
/// Public so a test can assert against the bytes a client actually sees rather
/// than a hand-written approximation of them.
pub fn slow_events(anthropic: bool, count: usize) -> Vec<Bytes> {
    let mut chunks = Vec::new();
    if anthropic {
        chunks.push(event(
            "message_start",
            &json!({
                "type": "message_start",
                "message": { "id": "msg_soak", "type": "message", "role": "assistant",
                             "model": "soak", "content": [], "stop_reason": null,
                             "usage": { "input_tokens": 10, "output_tokens": 1 } }
            }),
        ));
        chunks.push(event(
            "content_block_start",
            &json!({ "type": "content_block_start", "index": 0,
                     "content_block": { "type": "text", "text": "" } }),
        ));
        for i in 0..count {
            chunks.push(event(
                "content_block_delta",
                &json!({ "type": "content_block_delta", "index": 0,
                         "delta": { "type": "text_delta", "text": format!("tok{i} ") } }),
            ));
        }
        chunks.push(event(
            "content_block_stop",
            &json!({ "type": "content_block_stop", "index": 0 }),
        ));
        chunks.push(event(
            "message_delta",
            &json!({ "type": "message_delta", "delta": { "stop_reason": "end_turn" },
                     "usage": { "output_tokens": 20 } }),
        ));
        chunks.push(event("message_stop", &json!({ "type": "message_stop" })));
    } else {
        for i in 0..count {
            chunks.push(event(
                "",
                &json!({ "id": "chatcmpl-soak", "object": "chat.completion.chunk",
                         "created": 1_750_000_000u64, "model": "soak",
                         "choices": [{ "index": 0, "delta": { "content": format!("tok{i} ") },
                                       "finish_reason": null }] }),
            ));
        }
        chunks.push(event(
            "",
            &json!({ "id": "chatcmpl-soak", "object": "chat.completion.chunk",
                     "created": 1_750_000_000u64, "model": "soak", "choices": [],
                     "usage": { "prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30 } }),
        ));
        chunks.push(Bytes::from_static(b"data: [DONE]\n\n"));
    }
    chunks
}

/// An upstream that relays a few events and then vanishes mid-event: the
/// stream is already open, so the gateway cannot fail over and must terminate
/// the caller honestly while charging the partial spend.
fn truncated_events(anthropic: bool) -> Vec<Bytes> {
    let mut chunks = slow_events(anthropic, 3);
    chunks.truncate(if anthropic { 4 } else { 2 });
    chunks.push(Bytes::from_static(b"data: {\"partial\": tr"));
    chunks
}

/// A well-formed buffered chat completion whose answer is `text_bytes` long,
/// reporting usage proportional to the text so the settled charge is non-zero.
fn sized_completion(model: &str, text_bytes: usize) -> String {
    json!({
        "id": "chatcmpl-sized",
        "object": "chat.completion",
        "created": 1_750_000_000u64,
        "model": model,
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": "x".repeat(text_bytes) },
            "finish_reason": "stop",
        }],
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": text_bytes / 4,
            "total_tokens": 10 + text_bytes / 4,
        },
    })
    .to_string()
}

fn event(name: &str, data: &Value) -> Bytes {
    let payload = serde_json::to_string(data).expect("fixture data serializes");
    if name.is_empty() {
        Bytes::from(format!("data: {payload}\n\n"))
    } else {
        Bytes::from(format!("event: {name}\ndata: {payload}\n\n"))
    }
}
