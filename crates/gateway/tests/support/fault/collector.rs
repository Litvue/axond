//! An OTLP/HTTP receiver, so a matrix row can assert on the telemetry the
//! process actually exported rather than on the logs it happened to print.
//!
//! The payloads are protobuf and are kept as bytes. Two questions are asked of
//! them, and neither needs a decoder: whether an instrument or span *name*
//! appears — an instrument only reaches an export once it has recorded a point,
//! so a name is proof the fault was counted — and whether any secret, DSN, or
//! provider URL appears, which is the leakage check on the one surface an
//! operator forwards off the box.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use bytes::Bytes;
use tokio::sync::oneshot;

/// One export the collector received.
#[derive(Clone, Debug)]
pub struct Export {
    pub signal: &'static str,
    pub bytes: Bytes,
}

#[derive(Default)]
struct CollectorState {
    exports: Mutex<Vec<Export>>,
}

pub struct Collector {
    /// The base endpoint, as `OTEL_EXPORTER_OTLP_ENDPOINT` wants it.
    pub endpoint: String,
    state: Arc<CollectorState>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl Collector {
    pub async fn start() -> Self {
        let state = Arc::new(CollectorState::default());
        let app = Router::new()
            .route("/v1/traces", post(traces))
            .route("/v1/metrics", post(metrics))
            .route("/v1/logs", post(logs))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .expect("the OTLP collector binds");
        let addr = listener.local_addr().expect("a bound address");
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = rx.await;
                })
                .await;
        });
        Self {
            endpoint: format!("http://{addr}"),
            state,
            shutdown: Some(tx),
        }
    }

    pub fn exports(&self) -> Vec<Export> {
        self.state.exports.lock().expect("collector lock").clone()
    }

    /// How many exports arrived, per signal.
    pub fn counts(&self) -> BTreeMap<String, u64> {
        let mut counts = BTreeMap::new();
        for export in self.exports() {
            *counts.entry(export.signal.to_owned()).or_default() += 1;
        }
        counts
    }

    pub fn bytes(&self) -> u64 {
        self.exports().iter().map(|e| e.bytes.len() as u64).sum()
    }

    /// Whether `needle` appears in any exported payload of `signal`.
    pub fn signal_contains(&self, signal: &str, needle: &str) -> bool {
        self.exports()
            .iter()
            .any(|export| export.signal == signal && contains(&export.bytes, needle.as_bytes()))
    }

    /// Whether `needle` appears in any exported payload at all.
    pub fn contains(&self, needle: &str) -> bool {
        self.exports()
            .iter()
            .any(|export| contains(&export.bytes, needle.as_bytes()))
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack.len() >= needle.len()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

type Accepted = (StatusCode, [(&'static str, &'static str); 1], Bytes);

async fn traces(state: State<Arc<CollectorState>>, body: Bytes) -> Accepted {
    receive(state, "traces", body)
}

async fn metrics(state: State<Arc<CollectorState>>, body: Bytes) -> Accepted {
    receive(state, "metrics", body)
}

async fn logs(state: State<Arc<CollectorState>>, body: Bytes) -> Accepted {
    receive(state, "logs", body)
}

fn receive(
    State(state): State<Arc<CollectorState>>,
    signal: &'static str,
    body: Bytes,
) -> Accepted {
    state.exports.lock().expect("collector lock").push(Export {
        signal,
        bytes: body,
    });
    // An empty `Export*ServiceResponse` is the success answer, and an empty
    // protobuf message is zero bytes.
    (
        StatusCode::OK,
        [("content-type", "application/x-protobuf")],
        Bytes::new(),
    )
}
