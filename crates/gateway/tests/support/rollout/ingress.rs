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

/// What the balancer currently believes about one member. Readiness and
/// withdrawal live under one lock rather than in separate atomics, so a request
/// picked while the member was ready can never be attributed to the withdrawal
/// that happened after the pick.
#[derive(Default)]
struct Health {
    ready: bool,
    /// Whether readiness has ever been observed, which is what makes admission
    /// distinguishable from "not yet probed".
    admitted: bool,
    admitted_at: Option<Duration>,
    /// Whether the member stands withdrawn *now*. Cleared when it is seen ready
    /// again: a probe that timed out once under load is a flap, and a member
    /// permanently branded by it would fail the zero gate for the rest of the
    /// run on traffic the balancer was entitled to place.
    withdrawn: bool,
    /// When the balancer last took an admitted member out of rotation.
    withdrawn_at: Option<Duration>,
}

/// One replica in rotation.
pub struct Member {
    pub id: String,
    pub revision: String,
    pub base_url: String,
    health: Mutex<Health>,
    forwards: AtomicU64,
    forwards_after_withdrawal: AtomicU64,
    /// When the balancer handed this member a request, as offsets from the run's
    /// start. Every attempt, including the ones the member refused: the event a
    /// drain is judged on is the dispatch, not the answer.
    dispatches: Mutex<Vec<Duration>>,
    refusals: AtomicU64,
    draining_refusals: AtomicU64,
}

impl Member {
    fn new(id: String, revision: String, base_url: String) -> Self {
        Self {
            id,
            revision,
            base_url,
            health: Mutex::new(Health::default()),
            forwards: AtomicU64::new(0),
            forwards_after_withdrawal: AtomicU64::new(0),
            dispatches: Mutex::new(Vec::new()),
            refusals: AtomicU64::new(0),
            draining_refusals: AtomicU64::new(0),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.health.lock().expect("ingress lock").ready
    }

    /// When the ingress first saw this member ready, as an offset from the run's
    /// start.
    pub fn admitted_at(&self) -> Option<Duration> {
        self.health.lock().expect("ingress lock").admitted_at
    }

    /// When the ingress last saw an admitted member stop being ready — the
    /// instant a rolling deployment cares about, because from here on no caller
    /// may be sent to it.
    pub fn withdrawn_at(&self) -> Option<Duration> {
        self.health.lock().expect("ingress lock").withdrawn_at
    }

    pub fn forwards(&self) -> u64 {
        self.forwards.load(Ordering::SeqCst)
    }

    /// Requests the balancer sent here *after* it had already recorded the
    /// withdrawal, counted as it selected them. The gate: this must be zero.
    pub fn forwards_after_withdrawal(&self) -> u64 {
        self.forwards_after_withdrawal.load(Ordering::SeqCst)
    }

    /// The same property recomputed from the recorded events rather than
    /// asserted while selecting: dispatches that happened strictly after the
    /// withdrawal instant. The selection gate holds it at zero by construction,
    /// so this is what would catch a balancer that stopped holding it.
    pub fn dispatches_after(&self, withdrawn_at: Duration) -> u64 {
        self.dispatches
            .lock()
            .expect("ingress lock")
            .iter()
            .filter(|at| **at > withdrawn_at)
            .count() as u64
    }

    /// Stamp a dispatch: the instant the balancer actually handed the request
    /// to this member, which is the event a drain is judged on. Deliberately
    /// *not* the instant it was selected — a balancer whose selection is stale
    /// by the time it forwards has still put a caller on a drained replica, and
    /// that is exactly what the gate exists to catch.
    fn dispatched(&self, at: Duration) {
        self.dispatches.lock().expect("ingress lock").push(at);
    }

    /// Requests this member refused (a `503` during its drain, or a dropped
    /// connection) and the ingress retried elsewhere.
    pub fn refusals(&self) -> u64 {
        self.refusals.load(Ordering::SeqCst)
    }

    /// Of those, the ones the member answered `503` to rather than dropping.
    /// Only these can have reached the request path, so only these can have
    /// left a usage record behind for a caller request another member answered.
    pub fn draining_refusals(&self) -> u64 {
        self.draining_refusals.load(Ordering::SeqCst)
    }

    fn observe(&self, ready: bool, elapsed: Duration) {
        let mut health = self.health.lock().expect("ingress lock");
        let was = std::mem::replace(&mut health.ready, ready);
        if ready {
            if !std::mem::replace(&mut health.admitted, true) {
                health.admitted_at = Some(elapsed);
            }
            health.withdrawn = false;
        } else if was && health.admitted {
            health.withdrawn = true;
            health.withdrawn_at = Some(elapsed);
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

/// One attempt inside a caller request: the replica the balancer handed it to,
/// and how that ended. A refusal the replica *answered* is the only kind that
/// can have reached its request path, and therefore the only kind that can
/// leave a usage record behind for a caller request another replica went on to
/// answer.
#[derive(Debug, Clone, Serialize)]
pub struct Attempt {
    pub replica: String,
    pub status: Option<u16>,
    pub refused_while_draining: bool,
}

/// One caller request, by the identity the balancer gave it. This — not the
/// `request_id` a replica mints per event — is what usage is accounted by: a
/// caller request that two replicas both wrote a record for is one caller
/// request with a duplicate, not two answered callers.
#[derive(Debug, Clone, Serialize)]
pub struct CallerRequest {
    pub id: u64,
    pub attempts: Vec<Attempt>,
}

impl CallerRequest {
    /// The replica that answered, whatever the status.
    pub fn answered_by(&self) -> Option<&Attempt> {
        self.attempts
            .last()
            .filter(|attempt| attempt.status.is_some_and(|status| status != 503))
    }

    /// The replicas that refused it mid-drain and may therefore hold a record
    /// for work they had already begun.
    pub fn draining_refusals(&self) -> impl Iterator<Item = &str> {
        self.attempts
            .iter()
            .filter(|attempt| attempt.refused_while_draining)
            .map(|attempt| attempt.replica.as_str())
    }
}

pub struct IngressState {
    members: RwLock<Vec<Arc<Member>>>,
    cursor: AtomicUsize,
    forwards: Mutex<Vec<Forward>>,
    /// Every caller request the balancer has seen, by its own identity.
    callers: Mutex<Vec<CallerRequest>>,
    next_caller: AtomicU64,
    /// Requests the balancer could not place at all: no member was ready. The
    /// caller-visible availability gap of a rollout.
    unavailable: AtomicU64,
    /// A door between selection and forwarding, opened only by the test that
    /// has to produce the race for real: hold a request there, withdraw the
    /// member it was placed on, and let it go.
    pause: Mutex<Option<Arc<Pause>>>,
    client: reqwest::Client,
    started: Instant,
}

/// The seam that makes the withdrawal race reproducible rather than hoped for.
pub struct Pause {
    reached: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

impl Pause {
    fn new() -> Self {
        Self {
            reached: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        }
    }

    /// Called by the balancer: announce arrival and wait to be let go.
    async fn hold(&self) {
        self.reached.add_permits(1);
        self.release
            .acquire()
            .await
            .expect("the pause is open")
            .forget();
    }

    /// Called by the test: wait until a request is held at the seam.
    pub async fn await_arrival(&self) {
        self.reached
            .acquire()
            .await
            .expect("the pause is open")
            .forget();
    }

    /// Called by the test: let one held request continue.
    pub fn release(&self) {
        self.release.add_permits(1);
    }
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

    pub fn callers(&self) -> Vec<CallerRequest> {
        self.callers.lock().expect("ingress lock").clone()
    }

    /// Hold requests between selection and forwarding until they are released.
    pub fn pause_before_forwarding(&self) -> Arc<Pause> {
        let pause = Arc::new(Pause::new());
        *self.pause.lock().expect("ingress lock") = Some(pause.clone());
        pause
    }

    fn record_caller(&self, caller: CallerRequest) {
        self.callers.lock().expect("ingress lock").push(caller);
    }

    fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    /// The next ready member, round-robin, skipping `exclude` — the members that
    /// already refused this request. Also reports whether that member had
    /// already been withdrawn when it was chosen: read under the same lock as
    /// its readiness, so this selection-time invariant is decided on one
    /// consistent view. The dispatch instant is *not* stamped here: it is taken
    /// when the request is actually forwarded, so a withdrawal that lands
    /// between the two is caught rather than defined away.
    fn pick(&self, exclude: &[String]) -> Option<(Arc<Member>, bool)> {
        let members = self.members();
        if members.is_empty() {
            return None;
        }
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);
        (0..members.len()).find_map(|offset| {
            let member = &members[(start + offset) % members.len()];
            if exclude.contains(&member.id) {
                return None;
            }
            let health = member.health.lock().expect("ingress lock");
            health.ready.then(|| (member.clone(), health.withdrawn))
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
            callers: Mutex::new(Vec::new()),
            next_caller: AtomicU64::new(0),
            unavailable: AtomicU64::new(0),
            pause: Mutex::new(None),
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

    let mut caller = CallerRequest {
        id: state.next_caller.fetch_add(1, Ordering::SeqCst),
        attempts: Vec::new(),
    };
    let mut refused = Vec::new();
    // One attempt per member: a request is only ever retried onto a member that
    // has not already refused it, so a fleet-wide refusal is reported rather
    // than retried forever.
    let budget = state.members().len().max(1);
    for _ in 0..budget {
        let Some((member, withdrawn)) = state.pick(&refused) else {
            break;
        };
        member.forwards.fetch_add(1, Ordering::SeqCst);
        if withdrawn {
            member
                .forwards_after_withdrawal
                .fetch_add(1, Ordering::SeqCst);
        }
        let held = state.pause.lock().expect("ingress lock").clone();
        if let Some(pause) = held {
            pause.hold().await;
        }
        member.dispatched(state.elapsed());
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
                member.draining_refusals.fetch_add(1, Ordering::SeqCst);
                caller.attempts.push(Attempt {
                    replica: member.id.clone(),
                    status: Some(503),
                    refused_while_draining: true,
                });
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
                caller.attempts.push(Attempt {
                    replica: member.id.clone(),
                    status: Some(status.as_u16()),
                    refused_while_draining: false,
                });
                state.record_caller(caller);
                return relayed(&member, status, response);
            }
            // No answer at all: the member is gone. Take it out of rotation
            // without waiting for the next probe and try the next one.
            Err(_) => {
                member.refusals.fetch_add(1, Ordering::SeqCst);
                member.observe(false, state.elapsed());
                caller.attempts.push(Attempt {
                    replica: member.id.clone(),
                    status: None,
                    refused_while_draining: false,
                });
                refused.push(member.id.clone());
            }
        }
    }

    state.record_caller(caller);
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

/// The withdrawal rules the routing gate rests on, exercised directly: they are
/// the difference between a gate that can catch a balancer routing to a draining
/// replica and one that only ever restates how selection is written.
///
/// Plain `#[test]`s rather than a `cfg(test)` module: this is an integration
/// test crate, where `cfg(test)` is never set and such a module would be
/// compiled out.
mod withdrawal_rules {
    use super::*;

    fn member() -> Member {
        Member::new(
            "previous-0".to_owned(),
            "previous".to_owned(),
            String::new(),
        )
    }

    #[test]
    fn a_drain_marks_a_member_withdrawn_and_dates_it() {
        let member = member();
        member.observe(true, Duration::from_millis(10));
        member.observe(false, Duration::from_millis(80));

        assert_eq!(member.withdrawn_at(), Some(Duration::from_millis(80)));
        assert!(member.health.lock().expect("lock").withdrawn);
    }

    /// A probe that times out once under load is not a drain. Latching the
    /// withdrawal across the rest of the run would fail the zero gate on traffic
    /// the balancer was entitled to place, which is a harness artefact rather
    /// than a rollout defect.
    #[test]
    fn a_readiness_flap_does_not_leave_a_member_withdrawn_for_the_run() {
        let member = member();
        member.observe(true, Duration::from_millis(10));
        member.observe(false, Duration::from_millis(20));
        member.observe(true, Duration::from_millis(30));

        assert!(!member.health.lock().expect("lock").withdrawn);
        member.dispatched(Duration::from_millis(40));
        assert_eq!(
            member.dispatches_after(member.withdrawn_at().expect("the flap is dated")),
            1,
            "the recomputed witness counts events, so a flap is visible in it"
        );

        member.observe(false, Duration::from_millis(50));
        assert_eq!(
            member.dispatches_after(member.withdrawn_at().expect("the drain is dated")),
            0,
            "and the drain that follows is judged from the drain's own instant"
        );
    }

    /// The witness the gate needs: dispatches are compared against the recorded
    /// withdrawal instant, so a balancer that keeps handing work to a drained
    /// member is caught even if its selection stops flagging it.
    #[test]
    fn dispatches_are_counted_against_the_withdrawal_instant() {
        let member = member();
        member.observe(true, Duration::from_millis(10));
        member.dispatched(Duration::from_millis(30));
        member.observe(false, Duration::from_millis(40));
        member.dispatched(Duration::from_millis(41));
        member.dispatched(Duration::from_millis(90));

        assert_eq!(member.forwards_after_withdrawal(), 0);
        assert_eq!(
            member.dispatches_after(member.withdrawn_at().expect("the drain is dated")),
            2
        );
    }

    /// The race itself, produced rather than argued about: a request is held
    /// between selection and forwarding, the member it was placed on is
    /// withdrawn while it waits, and it is then let go. Selection was entitled
    /// to it — the member was ready when it was chosen — so the selection-time
    /// invariant stays zero, and the dispatch witness is what catches it. If
    /// the harness stamped the dispatch at selection instead, this would read
    /// as a clean run and the zero gate could never fail.
    #[tokio::test]
    async fn a_withdrawal_between_selection_and_forwarding_fails_the_gate() {
        let replica = stub_replica().await;
        // A probe interval longer than the test: readiness here is driven by
        // hand, so the outcome does not depend on when a poll happens to land.
        let ingress = Ingress::start(Duration::from_secs(3600), Instant::now()).await;
        let member = ingress.add("previous-0", "previous", &replica);
        member.observe(true, Duration::from_millis(10));

        let pause = ingress.state.pause_before_forwarding();
        let url = ingress.url("/healthz");
        let call = tokio::spawn(async move { reqwest::get(url).await.map(|r| r.status()) });

        pause.await_arrival().await;
        member.observe(false, ingress.state.elapsed());
        pause.release();
        let status = call
            .await
            .expect("the caller task finishes")
            .expect("the held request completes");

        assert!(status.is_success(), "the held request was still served");
        assert_eq!(
            member.forwards_after_withdrawal(),
            0,
            "selection read a ready member, so the selection-time invariant holds"
        );
        let withdrawn_at = member.withdrawn_at().expect("the withdrawal is dated");
        assert_eq!(
            member.dispatches_after(withdrawn_at),
            1,
            "the request nonetheless landed on a withdrawn member, and the \
             witness the gate is decided on says so"
        );
    }

    /// Re-admission clears the mark, and a request forwarded afterwards is not
    /// charged to the drain that preceded it: a replica put back into rotation
    /// is a replica the balancer may use again.
    #[tokio::test]
    async fn a_re_admitted_member_carries_no_withdrawal_mark() {
        let replica = stub_replica().await;
        let ingress = Ingress::start(Duration::from_secs(3600), Instant::now()).await;
        let member = ingress.add("previous-0", "previous", &replica);
        // Dated from the balancer's own clock, so the dispatch that follows is
        // unambiguously later than the flap.
        member.observe(true, ingress.state.elapsed());
        member.observe(false, ingress.state.elapsed());
        member.observe(true, ingress.state.elapsed());
        tokio::time::sleep(Duration::from_millis(5)).await;

        let status = reqwest::get(ingress.url("/healthz"))
            .await
            .expect("the re-admitted member serves")
            .status();

        assert!(status.is_success());
        assert_eq!(
            member.forwards_after_withdrawal(),
            0,
            "the mark was cleared when the member came back"
        );
        assert_eq!(
            member.dispatches_after(member.withdrawn_at().expect("the earlier drain is dated")),
            1,
            "the dispatch is later than that drain, which is why the mark \
             being cleared is what keeps the gate honest"
        );
    }

    /// A replica that answers everything, standing in for a real one: these
    /// tests are about the balancer's bookkeeping, not the gateway's.
    async fn stub_replica() -> String {
        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
                .await
                .expect("the stub binds");
        let addr = listener.local_addr().expect("the stub has an address");
        tokio::spawn(async move {
            let app = axum::Router::new().fallback(|| async { "ok" });
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }
}
