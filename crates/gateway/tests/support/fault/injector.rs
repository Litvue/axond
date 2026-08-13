//! The fault injectors: unreachable provider endpoints, and a TCP fault proxy
//! that sits between the gateway and a real Redis or Postgres.
//!
//! Everything here is loopback and hermetic. A DNS failure is a name that
//! cannot resolve by construction (`.invalid`), a refused connect is a port
//! whose listener has been closed, and a TLS failure is a socket that answers a
//! handshake with bytes that are not one — no external host is contacted, and
//! no test depends on how a resolver treats a name someone else owns.
//!
//! The backend proxy is what makes "latency", "outage", and "recovery" real
//! rather than simulated: the gateway speaks the wire protocol to a socket the
//! harness controls, so an outage severs live connections the way a lost
//! datastore does, and recovery is the same socket answering again.

use std::io;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, watch};

/// A hostname that cannot resolve. `.invalid` is reserved for exactly this
/// (RFC 2606), so no resolver anywhere is allowed to answer it.
pub const UNRESOLVABLE_BASE_URL: &str = "http://axond-fault-unresolvable.invalid:9";

/// A loopback address with nothing behind it: the listener is bound to reserve
/// the port, then closed, so a connect is refused rather than hanging.
pub fn refused_addr() -> SocketAddr {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("a free port");
    listener.local_addr().expect("a bound address")
}

/// A socket that accepts a TLS connection and answers with bytes that are not
/// a handshake, so the client fails in TLS rather than in TCP.
pub struct GarbageTls {
    pub addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
}

impl GarbageTls {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the TLS fault listener binds");
        let addr = listener.local_addr().expect("a bound address");
        let (tx, mut rx) = oneshot::channel();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut rx => return,
                    accepted = listener.accept() => {
                        let Ok((mut socket, _)) = accepted else { return };
                        tokio::spawn(async move {
                            // Not a `ServerHello`, and not a TLS record either.
                            let _ = socket.write_all(b"axond fault injector: not TLS\r\n").await;
                            let _ = socket.shutdown().await;
                        });
                    }
                }
            }
        });
        Self {
            addr,
            shutdown: Some(tx),
        }
    }

    pub fn base_url(&self) -> String {
        format!("https://{}", self.addr)
    }
}

impl Drop for GarbageTls {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// What the backend proxy is doing to the traffic it carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Forward untouched.
    Pass,
    /// Forward, delayed by this much on every read carried in either direction.
    Latency(Duration),
    /// Refuse new connections and sever live ones: the datastore is gone.
    Outage,
}

struct ProxyState {
    /// Where the real service lives. Derived from the harness's own DSN and
    /// never logged: the artifact records the *proxy* address only.
    upstream: String,
    mode: watch::Sender<Mode>,
    accepted: AtomicU64,
    severed: AtomicU64,
}

impl ProxyState {
    fn mode(&self) -> Mode {
        *self.mode.borrow()
    }
}

/// A TCP proxy in front of a real datastore, with an injectable fault.
pub struct FaultProxy {
    pub addr: SocketAddr,
    state: Arc<ProxyState>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl FaultProxy {
    /// Start a proxy in front of `upstream` (`host:port`), passing traffic.
    pub async fn start(upstream: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the fault proxy binds");
        let addr = listener.local_addr().expect("a bound address");
        let (mode, _) = watch::channel(Mode::Pass);
        let state = Arc::new(ProxyState {
            upstream: upstream.to_owned(),
            mode,
            accepted: AtomicU64::new(0),
            severed: AtomicU64::new(0),
        });
        let (tx, mut rx) = oneshot::channel();
        let accepting = state.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut rx => return,
                    accepted = listener.accept() => {
                        let Ok((client, _)) = accepted else { return };
                        let state = accepting.clone();
                        tokio::spawn(serve(client, state));
                    }
                }
            }
        });
        Self {
            addr,
            state,
            shutdown: Some(tx),
        }
    }

    pub fn set(&self, mode: Mode) {
        self.state.mode.send_replace(mode);
    }

    pub fn mode(&self) -> Mode {
        self.state.mode()
    }

    /// Connections the proxy has carried, and how many an outage tore down.
    /// Recorded on the row: an outage that severed nothing was not an outage.
    pub fn accepted(&self) -> u64 {
        self.state.accepted.load(Ordering::SeqCst)
    }

    pub fn severed(&self) -> u64 {
        self.state.severed.load(Ordering::SeqCst)
    }
}

impl Drop for FaultProxy {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

async fn serve(client: TcpStream, state: Arc<ProxyState>) {
    if state.mode() == Mode::Outage {
        // Accepted and closed at once: the connect succeeds and the protocol
        // handshake fails immediately, which is what a client sees from a
        // datastore whose listener is gone behind a load balancer.
        state.severed.fetch_add(1, Ordering::SeqCst);
        return;
    }
    let Ok(upstream) = TcpStream::connect(&state.upstream).await else {
        state.severed.fetch_add(1, Ordering::SeqCst);
        return;
    };
    state.accepted.fetch_add(1, Ordering::SeqCst);
    let _ = client.set_nodelay(true);
    let _ = upstream.set_nodelay(true);
    let (client_read, client_write) = client.into_split();
    let (upstream_read, upstream_write) = upstream.into_split();
    let outbound = pump(client_read, upstream_write, state.clone());
    let inbound = pump(upstream_read, client_write, state.clone());

    // A connection sitting idle in a pool is not reading anything, so an outage
    // has to reach it from outside the copy loops.
    let mut watcher = state.mode.subscribe();
    let severed = async {
        loop {
            if watcher.borrow_and_update().eq(&Mode::Outage) {
                return;
            }
            if watcher.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    };

    tokio::select! {
        _ = outbound => {}
        _ = inbound => {}
        _ = severed => {
            state.severed.fetch_add(1, Ordering::SeqCst);
        }
    }
}

async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    state: Arc<ProxyState>,
) -> io::Result<()> {
    let mut buffer = vec![0u8; 16 * 1024];
    loop {
        let read = from.read(&mut buffer).await?;
        if read == 0 {
            let _ = to.shutdown().await;
            return Ok(());
        }
        match state.mode() {
            Mode::Pass => {}
            Mode::Latency(delay) => tokio::time::sleep(delay).await,
            Mode::Outage => return Err(io::Error::other("severed by the fault proxy")),
        }
        to.write_all(&buffer[..read]).await?;
    }
}

/// The `host:port` a DSN points at, and the DSN with that authority replaced by
/// `addr` — `None` for a DSN the proxy cannot stand in front of, which the
/// caller reports as a skip. The proxy carries TCP, so redirecting a TLS
/// endpoint through it would fail the handshake and be read as the injected
/// fault; only the plaintext schemes are accepted.
/// Neither the input nor the output is ever recorded: the caller uses
/// the rewritten DSN as the *value* of an env reference, exactly as an operator
/// would, and the artifact names the variable rather than its contents.
pub fn redirect(dsn: &str, addr: SocketAddr) -> Option<(String, String)> {
    let (scheme, rest) = dsn.split_once("://")?;
    // Checked for its own sake rather than only as a default port: a TLS DSN
    // that names its port would otherwise be redirected anyway.
    default_port(scheme)?;
    let (authority, tail) = match rest.find(['/', '?']) {
        Some(at) => (&rest[..at], &rest[at..]),
        None => (rest, ""),
    };
    let (userinfo, hostport) = match authority.rsplit_once('@') {
        Some((userinfo, hostport)) => (format!("{userinfo}@"), hostport),
        None => (String::new(), authority),
    };
    let hostport = if hostport.contains(':') {
        hostport.to_owned()
    } else {
        format!("{hostport}:{}", default_port(scheme)?)
    };
    Some((hostport, format!("{scheme}://{userinfo}{addr}{tail}")))
}

fn default_port(scheme: &str) -> Option<u16> {
    match scheme {
        "redis" => Some(6379),
        "postgres" | "postgresql" => Some(5432),
        _ => None,
    }
}
