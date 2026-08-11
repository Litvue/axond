//! A fake provider upstream: the OpenAI and Anthropic wire shapes, served from
//! committed fixtures with no network.
//!
//! The server records what the gateway sent it (rewritten model, injected
//! credential, wire headers, body) and tracks how many response bodies are
//! open, which is what makes an upstream connection leak observable: a soak run
//! asserts opens and closes balance once the clients are gone.

use std::collections::HashMap;
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
}

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

pub struct UpstreamState {
    requests: Mutex<Vec<Recorded>>,
    counters: Counters,
    fixtures: Fixtures,
}

impl UpstreamState {
    /// Every request the gateway has made, in arrival order.
    pub fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().expect("upstream lock").clone()
    }

    pub fn last_request(&self) -> Recorded {
        self.requests()
            .pop()
            .expect("the gateway made an upstream request")
    }

    /// Streamed response bodies currently open. Balanced opens and closes are
    /// the leak assertion: a cancelled client must take its upstream with it.
    pub fn open_streams(&self) -> i64 {
        self.counters.open.load(Ordering::SeqCst)
    }

    pub fn opened_streams(&self) -> u64 {
        self.counters.opened.load(Ordering::SeqCst)
    }
}

/// Decrements the open-stream count whenever a response body is dropped —
/// completed, cancelled, or aborted alike.
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
            requests: Mutex::new(Vec::new()),
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
    state
        .requests
        .lock()
        .expect("upstream lock")
        .push(Recorded {
            path: path.clone(),
            model: model.clone(),
            authorization: header("authorization"),
            api_key: header("x-api-key"),
            anthropic_version: header("anthropic-version"),
            anthropic_beta: header("anthropic-beta"),
            body,
        });

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
        target::SLOW_STREAM => sse(state.clone(), slow_events(anthropic, 40), true),
        target::DROP_STREAM => sse(state.clone(), truncated_events(anthropic), false),
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
                false,
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
fn sse(state: Arc<UpstreamState>, chunks: Vec<Bytes>, paced: bool) -> Response {
    state.counters.open.fetch_add(1, Ordering::SeqCst);
    state.counters.opened.fetch_add(1, Ordering::SeqCst);
    let guard = ConnGuard(state);
    let stream = futures::stream::unfold(
        (chunks.into_iter(), guard),
        move |(mut chunks, guard)| async move {
            let chunk = chunks.next()?;
            if paced {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Some((Ok::<Bytes, std::io::Error>(chunk), (chunks, guard)))
        },
    );
    let mut response = Response::new(Body::from_stream(stream));
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
fn slow_events(anthropic: bool, count: usize) -> Vec<Bytes> {
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

fn event(name: &str, data: &Value) -> Bytes {
    let payload = serde_json::to_string(data).expect("fixture data serializes");
    if name.is_empty() {
        Bytes::from(format!("data: {payload}\n\n"))
    } else {
        Bytes::from(format!("event: {name}\ndata: {payload}\n\n"))
    }
}
