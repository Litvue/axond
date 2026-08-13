//! A representative ingress: the load balancer a rolling deployment is
//! actually qualified through.
//!
//! It is deliberately the dumbest thing that is still honest about production —
//! round-robin over members, readiness polled on an interval, a member dropped
//! from rotation the moment its `/readyz` fails, and one retry onto another
//! member when a replica refuses or drops a request before any byte of the
//! answer was committed. Every ingress worth deploying behind Axond does at
//! least this, and the properties the harness gates on (no traffic to a
//! withdrawn replica, no unanswered caller) are the ones that hold *because* of
//! this behaviour rather than in spite of it.
//!
//! The point of running a real proxy rather than choosing a base URL per request
//! is that routing then has a witness other than the driver: the ingress records
//! which member answered each request, and stamps it on the response as well, so
//! the driver's attribution and the balancer's own log have to agree.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use serde::Serialize;
use tokio::sync::oneshot;

/// The replica that answered, stamped on every response the ingress relays.
pub const REPLICA_HEADER: &str = "x-axond-replica";
/// The revision that replica is running.
pub const REVISION_HEADER: &str = "x-axond-revision";

/// How long a member has to answer a readiness probe before the ingress treats
/// it as gone. A draining replica keeps answering, so this only fires once the
/// process is actually away.
const PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// One replica in rotation.
pub struct Member {
    pub id: String,
    pub revision: String,
    pub base_url: String,
    ready: AtomicBool,
    /// Whether readiness has ever been observed, which is what makes admission
    /// distinguishable from "not yet probed".
    admitted: AtomicBool,
    admitted_at: Mutex<Option<Duration>>,
    withdrawn_at: Mutex<Option<Duration>>,
    forwards: AtomicU64,
    forwards_after_withdrawal: AtomicU64,
    refusals: AtomicU64,
}

impl Member {
    fn new(id: String, revision: String, base_url: String) -> Self {
        Self {
            id,
            revision,
            base_url,
            ready: AtomicBool::new(false),
            admitted: AtomicBool::new(false),
            admitted_at: Mutex::new(None),
            withdrawn_at: Mutex::new(None),
            forwards: AtomicU64::new(0),
            forwards_after_withdrawal: AtomicU64::new(0),
            refusals: AtomicU64::new(0),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    /// When the ingress first saw this member ready, as an offset from the run's
    /// start.
    pub fn admitted_at(&self) -> Option<Duration> {
        *self.admitted_at.lock().expect("ingress lock")
    }

    /// When the ingress first saw an admitted member stop being ready — the
    /// instant a rolling deployment cares about, because from here on no caller
    /// may be sent to it.
    pub fn withdrawn_at(&self) -> Option<Duration> {
        *self.withdrawn_at.lock().expect("ingress lock")
    }

    pub fn forwards(&self) -> u64 {
        self.forwards.load(Ordering::SeqCst)
    }

    /// Requests the balancer sent here *after* it had already recorded the
    /// withdrawal. The gate: this must be zero.
    pub fn forwards_after_withdrawal(&self) -> u64 {
        self.forwards_after_withdrawal.load(Ordering::SeqCst)
    }

    /// Requests this member refused (a `503` during its drain, or a dropped
    /// connection) and the ingress retried elsewhere.
    pub fn refusals(&self) -> u64 {
        self.refusals.load(Ordering::SeqCst)
    }

    fn observe(&self, ready: bool, elapsed: Duration) {
        let was = self.ready.swap(ready, Ordering::SeqCst);
        if ready {
            if !self.admitted.swap(true, Ordering::SeqCst) {
                *self.admitted_at.lock().expect("ingress lock") = Some(elapsed);
            }
        } else if was && self.admitted.load(Ordering::SeqCst) {
            let mut withdrawn = self.withdrawn_at.lock().expect("ingress lock");
            if withdrawn.is_none() {
                *withdrawn = Some(elapsed);
            }
        }
    }
}

/// One request as the balancer routed it.
#[derive(Debug, Clone, Serialize)]
pub struct Forward {
    pub replica: String,
    pub revision: String,
    pub status: u16,
    pub at_ms: u128,
    /// How many members refused before this one answered.
    pub retries: u32,
}

pub struct IngressState {
    members: RwLock<Vec<Arc<Member>>>,
    cursor: AtomicUsize,
    forwards: Mutex<Vec<Forward>>,
    /// Requests the balancer could not place at all: no member was ready. The
    /// caller-visible availability gap of a rollout.
    unavailable: AtomicU64,
    client: reqwest::Client,
    started: Instant,
}

impl IngressState {
    pub fn members(&self) -> Vec<Arc<Member>> {
        self.members.read().expect("ingress lock").clone()
    }

    pub fn member(&self, id: &str) -> Option<Arc<Member>> {
        self.members().into_iter().find(|m| m.id == id)
    }

    pub fn forwards(&self) -> Vec<Forward> {
        self.forwards.lock().expect("ingress lock").clone()
    }

    pub fn unavailable(&self) -> u64 {
        self.unavailable.load(Ordering::SeqCst)
    }

    fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    /// The next ready member, round-robin, skipping `exclude` — the members that
    /// already refused this request.
    fn pick(&self, exclude: &[String]) -> Option<Arc<Member>> {
        let members = self.members();
        if members.is_empty() {
            return None;
        }
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);
        (0..members.len()).find_map(|offset| {
            let member = &members[(start + offset) % members.len()];
            (member.is_ready() && !exclude.contains(&member.id)).then(|| member.clone())
        })
    }
}

pub struct Ingress {
    pub base_url: String,
    pub state: Arc<IngressState>,
    shutdown: Option<oneshot::Sender<()>>,
    probes: tokio::task::JoinHandle<()>,
}

impl Ingress {
    /// Start the balancer with no members and begin probing readiness every
    /// `poll`.
    pub async fn start(poll: Duration, started: Instant) -> Self {
        let state = Arc::new(IngressState {
            members: RwLock::new(Vec::new()),
            cursor: AtomicUsize::new(0),
            forwards: Mutex::new(Vec::new()),
            unavailable: AtomicU64::new(0),
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .build()
                .expect("the ingress client builds"),
            started,
        });
        let app = axum::Router::new()
            .fallback(proxy)
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .expect("the ingress binds");
        let addr = listener.local_addr().expect("the ingress has an address");
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = rx.await;
                })
                .await;
        });
        let probes = tokio::spawn(probe_loop(state.clone(), poll));
        Self {
            base_url: format!("http://{addr}"),
            state,
            shutdown: Some(tx),
            probes,
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    /// Put a replica into rotation. It carries no traffic until a readiness
    /// probe succeeds, which is what makes admission measurable rather than
    /// assumed.
    pub fn add(&self, id: &str, revision: &str, base_url: &str) -> Arc<Member> {
        let member = Arc::new(Member::new(
            id.to_owned(),
            revision.to_owned(),
            base_url.to_owned(),
        ));
        self.state
            .members
            .write()
            .expect("ingress lock")
            .push(member.clone());
        member
    }

    /// Wait until the balancer has seen `id` become ready, and report how long
    /// that took from the moment the member was added.
    pub async fn await_admission(&self, id: &str, within: Duration) -> Option<Duration> {
        let member = self.state.member(id)?;
        let started = Instant::now();
        while started.elapsed() < within {
            if member.is_ready() {
                return Some(started.elapsed());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        None
    }

    /// Wait until the balancer has taken `id` out of rotation, and report how
    /// long that took from `since`.
    pub async fn await_withdrawal(
        &self,
        id: &str,
        since: Instant,
        within: Duration,
    ) -> Option<Duration> {
        let member = self.state.member(id)?;
        let started = Instant::now();
        while started.elapsed() < within {
            if !member.is_ready() {
                return Some(since.elapsed());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        None
    }
}

impl Drop for Ingress {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        self.probes.abort();
    }
}

/// Poll every member's `/readyz` forever. A transport failure counts as not
/// ready: a replica that has gone away is one the balancer must stop using,
/// whatever the reason.
async fn probe_loop(state: Arc<IngressState>, poll: Duration) {
    loop {
        for member in state.members() {
            let ready = state
                .client
                .get(format!("{}/readyz", member.base_url))
                .timeout(PROBE_TIMEOUT)
                .send()
                .await
                .is_ok_and(|response| response.status().is_success());
            member.observe(ready, state.elapsed());
        }
        tokio::time::sleep(poll).await;
    }
}

/// Relay one caller request to a ready member.
async fn proxy(State(state): State<Arc<IngressState>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, 8 * 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let path = parts
        .uri
        .path_and_query()
        .map_or_else(|| parts.uri.path().to_owned(), ToString::to_string);

    let mut refused = Vec::new();
    // One attempt per member: a request is only ever retried onto a member that
    // has not already refused it, so a fleet-wide refusal is reported rather
    // than retried forever.
    let budget = state.members().len().max(1);
    for _ in 0..budget {
        let Some(member) = state.pick(&refused) else {
            break;
        };
        member.forwards.fetch_add(1, Ordering::SeqCst);
        if member.withdrawn_at().is_some() {
            member
                .forwards_after_withdrawal
                .fetch_add(1, Ordering::SeqCst);
        }
        let sent = state
            .client
            .request(parts.method.clone(), format!("{}{path}", member.base_url))
            .headers(forwarded(&parts.headers))
            .body(bytes.clone())
            .send()
            .await;
        let retries = refused.len() as u32;
        match sent {
            // A replica that has begun draining refuses new work. That is the
            // contract working, not a lost request: the balancer places it on a
            // member that is still serving.
            Ok(response) if response.status() == StatusCode::SERVICE_UNAVAILABLE => {
                member.refusals.fetch_add(1, Ordering::SeqCst);
                refused.push(member.id.clone());
            }
            Ok(response) => {
                let status = response.status();
                state.forwards.lock().expect("ingress lock").push(Forward {
                    replica: member.id.clone(),
                    revision: member.revision.clone(),
                    status: status.as_u16(),
                    at_ms: state.elapsed().as_millis(),
                    retries,
                });
                return relayed(&member, status, response);
            }
            // No answer at all: the member is gone. Take it out of rotation
            // without waiting for the next probe and try the next one.
            Err(_) => {
                member.refusals.fetch_add(1, Ordering::SeqCst);
                member.observe(false, state.elapsed());
                refused.push(member.id.clone());
            }
        }
    }

    state.unavailable.fetch_add(1, Ordering::SeqCst);
    (
        StatusCode::SERVICE_UNAVAILABLE,
        "no ready replica accepted the request",
    )
        .into_response()
}

/// The caller's headers, minus the ones that describe the hop rather than the
/// request. `content-length` in particular is the balancer's to recompute.
fn forwarded(headers: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::new();
    for (name, value) in headers {
        if matches!(name.as_str(), "host" | "content-length" | "connection") {
            continue;
        }
        out.insert(name.clone(), value.clone());
    }
    out
}

/// Stream the member's answer back to the caller, stamped with who served it.
/// Streamed, not buffered: a balancer that collects an SSE body before relaying
/// it would hide every streaming property the harness is here to measure.
fn relayed(member: &Member, status: StatusCode, response: reqwest::Response) -> Response {
    let mut builder = Response::builder().status(status);
    for (name, value) in response.headers() {
        if matches!(name.as_str(), "content-length" | "transfer-encoding") {
            continue;
        }
        builder = builder.header(name.clone(), value.clone());
    }
    for (name, value) in [
        (REPLICA_HEADER, &member.id),
        (REVISION_HEADER, &member.revision),
    ] {
        if let Ok(value) = HeaderValue::from_str(value) {
            builder = builder.header(HeaderName::from_static(name), value);
        }
    }
    builder
        .body(Body::from_stream(response.bytes_stream().map(
            |chunk| -> Result<_, std::io::Error> {
                chunk.map_err(|error| std::io::Error::other(error.to_string()))
            },
        )))
        .expect("the relayed response builds")
}
