//! A fault gate: a loopback TCP forwarder the harness puts in front of a
//! backend so it can make that backend slow, or make it disappear, while the
//! run continues.
//!
//! Why a gate rather than a fixture alias. A slow or refusing *route* on the
//! fake upstream tests the gateway's handling of a slow or refusing response;
//! it does not test what happens when the backend itself stops accepting
//! connections and the sockets already open to it die. That second failure is
//! the one a stateful deployment actually meets — a database failover, a
//! provider's edge going away — and it is the one that decides whether buffered
//! accounting survives. The gate reproduces it without asking the run to have
//! permission to stop a container.
//!
//! Two knobs, both live:
//!
//! * **latency** delays each new connection before it is joined to the backend,
//!   which is what a caller sees when a backend's accept queue is deep;
//! * **outage** refuses new connections *and* cuts the ones already open, which
//!   is what a caller sees when the backend is gone.
//!
//! Every transition is counted, so an artifact can say how much traffic met a
//! declared fault instead of asserting that some of it must have.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use serde::Serialize;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

const ACCEPT_RETRY_INITIAL_MS: u64 = 5;
const ACCEPT_RETRY_MAX_MS: u64 = 250;
const ACCEPT_RETRY_MAX_SHIFT: u32 = 6;

/// Backoff for an accept failure. The gate keeps retrying until it is dropped,
/// because an accept error is allowed to be transient for the whole run; only
/// the delay is bounded so a persistent error cannot busy-loop the runtime.
#[derive(Debug, Default)]
pub(crate) struct AcceptBackoff {
    consecutive_errors: u32,
}

impl AcceptBackoff {
    pub(crate) fn on_success(&mut self) {
        self.consecutive_errors = 0;
    }

    pub(crate) fn on_error(&mut self) -> Duration {
        self.consecutive_errors = self.consecutive_errors.saturating_add(1);
        let shift = self
            .consecutive_errors
            .saturating_sub(1)
            .min(ACCEPT_RETRY_MAX_SHIFT);
        let millis = ACCEPT_RETRY_INITIAL_MS
            .saturating_mul(1_u64 << shift)
            .min(ACCEPT_RETRY_MAX_MS);
        Duration::from_millis(millis)
    }
}

/// What the gate is doing to the backend behind it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Pass,
    /// Every new connection waits this long before it reaches the backend.
    Latency(u64),
    /// Nothing reaches the backend, and nothing that already did survives.
    Outage,
}

#[derive(Debug, Default)]
struct Counters {
    accepted: AtomicU64,
    refused: AtomicU64,
    cut: AtomicU64,
    delayed: AtomicU64,
}

/// What the gate did, for the artifact.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct GateCounts {
    pub accepted: u64,
    pub refused: u64,
    pub cut: u64,
    pub delayed: u64,
}

struct State {
    target: String,
    latency_ms: AtomicU64,
    outage: AtomicBool,
    /// Bumped on every cut, so connections already joined to the backend end
    /// rather than outliving the outage that was supposed to kill them.
    generation: watch::Sender<u64>,
    counters: Counters,
}

/// A running gate. Dropping it stops accepting: the accept loop is aborted and
/// its listening socket closed, so a run does not leave a listener behind for
/// the next profile to be measured with. The connections it has already joined
/// end with the run's runtime.
pub struct Gate {
    pub addr: SocketAddr,
    state: Arc<State>,
    accepting: tokio::task::JoinHandle<()>,
}

impl Drop for Gate {
    fn drop(&mut self) {
        self.accepting.abort();
    }
}

impl Gate {
    /// Start a gate forwarding to `target` (`host:port`).
    pub async fn start(target: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the fault gate binds a loopback port");
        let addr = listener.local_addr().expect("the gate has an address");
        let (generation, _) = watch::channel(0);
        let state = Arc::new(State {
            target: target.to_owned(),
            latency_ms: AtomicU64::new(0),
            outage: AtomicBool::new(false),
            generation,
            counters: Counters::default(),
        });

        let serving = state.clone();
        let accepting = tokio::spawn(async move {
            let mut accept_backoff = AcceptBackoff::default();
            loop {
                match listener.accept().await {
                    Ok((inbound, _)) => {
                        accept_backoff.on_success();
                        let state = serving.clone();
                        tokio::spawn(async move { serve(inbound, state).await });
                    }
                    // An accept error is transient — a client that hung up
                    // half-open, a descriptor table briefly full — and not the
                    // end of the gate. A loop that stopped here would close the
                    // listening socket and leave the backend unreachable for the
                    // rest of the run, which reads on the artifact as the
                    // deployment refusing every request rather than as the
                    // harness having stopped forwarding.
                    Err(_) => {
                        tokio::time::sleep(accept_backoff.on_error()).await;
                    }
                }
            }
        });

        Self {
            addr,
            state,
            accepting,
        }
    }

    /// Where a client should be pointed to reach the backend through the gate.
    pub fn authority(&self) -> String {
        self.addr.to_string()
    }

    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn set(&self, mode: Mode) {
        match mode {
            Mode::Pass => {
                self.state.outage.store(false, Ordering::SeqCst);
                self.state.latency_ms.store(0, Ordering::SeqCst);
            }
            Mode::Latency(ms) => {
                self.state.outage.store(false, Ordering::SeqCst);
                self.state.latency_ms.store(ms, Ordering::SeqCst);
            }
            Mode::Outage => {
                self.state.outage.store(true, Ordering::SeqCst);
                // Established connections are cut as well: a backend that has
                // gone away does not keep serving the sockets it already had.
                self.state.generation.send_modify(|generation| {
                    *generation += 1;
                });
            }
        }
    }

    pub fn counts(&self) -> GateCounts {
        GateCounts {
            accepted: self.state.counters.accepted.load(Ordering::Relaxed),
            refused: self.state.counters.refused.load(Ordering::Relaxed),
            cut: self.state.counters.cut.load(Ordering::Relaxed),
            delayed: self.state.counters.delayed.load(Ordering::Relaxed),
        }
    }
}

async fn serve(mut inbound: TcpStream, state: Arc<State>) {
    // Subscribed before the outage is read rather than after the backend is
    // joined: a `watch` receiver marks the value it was created on as seen, so
    // a cut published while this connection was being set up would otherwise be
    // missed and the connection would outlive the outage that meant to kill it.
    let mut cuts = state.generation.subscribe();
    if state.outage.load(Ordering::SeqCst) {
        state.counters.refused.fetch_add(1, Ordering::Relaxed);
        return;
    }
    let latency = state.latency_ms.load(Ordering::SeqCst);
    if latency > 0 {
        state.counters.delayed.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(std::time::Duration::from_millis(latency)).await;
    }
    let Ok(mut outbound) = TcpStream::connect(&state.target).await else {
        state.counters.refused.fetch_add(1, Ordering::Relaxed);
        return;
    };
    // Re-read after connecting: an outage declared while the backend was being
    // dialled is one this connection is on the wrong side of.
    if state.outage.load(Ordering::SeqCst) {
        state.counters.refused.fetch_add(1, Ordering::Relaxed);
        return;
    }
    state.counters.accepted.fetch_add(1, Ordering::Relaxed);

    tokio::select! {
        _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
        _ = cuts.changed() => {
            state.counters.cut.fetch_add(1, Ordering::Relaxed);
        }
    }
}
