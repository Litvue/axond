//! A control-plane link the harness can cut and restore.
//!
//! An outage has to be done *to* the replica rather than simulated inside it. A
//! store wrapper that returned `Unavailable` would be a test of the wrapper: the
//! replica would never see a connection reset, the pool would never reconnect,
//! and the reconnect path — the one that decides whether an outage costs a
//! replica its snapshot — would not run at all.
//!
//! So the replica connects to a loopback listener that forwards to the real
//! database, and [`SeverableLink::sever`] takes the listener away and drops
//! every forwarded connection. From the replica's side that is exactly a
//! database that went away: in-flight statements fail, the connection is dead,
//! and reconnection is refused until [`SeverableLink::restore`] puts the
//! listener back on the same port. The database itself is untouched, which is
//! what makes the recovery half meaningful — the rows are still there when the
//! link comes back.

use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// A cuttable TCP path to the control-plane database.
pub(crate) struct SeverableLink {
    /// The loopback port the replica dials. Stable across a sever/restore cycle,
    /// so the DSN a replica was built with keeps pointing at the same place —
    /// a recovery a replica only survives because it was handed a new address is
    /// not a recovery.
    port: u16,
    upstream: SocketAddr,
    accepting: Mutex<Option<JoinHandle<()>>>,
    /// The forwarders to drop on a cut, and whether the link is currently cut.
    /// Held together under one lock: the accept task registers a forwarder it
    /// has already spawned, so without a flag read under the same lock a
    /// connection accepted at the instant of the cut would be registered into a
    /// vector [`Self::sever`] had already drained, and would keep proxying to
    /// the real database for the whole outage window.
    forwarding: Arc<Mutex<Forwarding>>,
}

#[derive(Default)]
struct Forwarding {
    severed: bool,
    tasks: Vec<JoinHandle<()>>,
}

impl SeverableLink {
    /// Open a link to `upstream` and start forwarding.
    pub(crate) async fn open(upstream: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let port = listener.local_addr()?.port();
        let link = Self {
            port,
            upstream,
            accepting: Mutex::new(None),
            forwarding: Arc::new(Mutex::new(Forwarding::default())),
        };
        link.spawn(listener);
        Ok(link)
    }

    /// The address a replica dials.
    pub(crate) fn port(&self) -> u16 {
        self.port
    }

    /// Cut the link: no new connection is accepted, and every forwarded one is
    /// dropped mid-flight.
    pub(crate) fn sever(&self) {
        if let Some(accepting) = self.accepting.lock().expect("not poisoned").take() {
            accepting.abort();
        }
        let mut forwarding = self.forwarding.lock().expect("not poisoned");
        forwarding.severed = true;
        for task in std::mem::take(&mut forwarding.tasks) {
            task.abort();
        }
    }

    /// Put the link back on the same port.
    ///
    /// The bind can lose a race with the kernel releasing the port, so it is
    /// retried briefly rather than failing a recovery scenario for a reason that
    /// has nothing to do with recovery.
    pub(crate) async fn restore(&self) -> io::Result<()> {
        let mut last = None;
        for _ in 0..50 {
            match TcpListener::bind(("127.0.0.1", self.port)).await {
                Ok(listener) => {
                    self.forwarding.lock().expect("not poisoned").severed = false;
                    self.spawn(listener);
                    return Ok(());
                }
                Err(error) => {
                    last = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            }
        }
        Err(last.expect("at least one attempt"))
    }

    fn spawn(&self, listener: TcpListener) {
        let upstream = self.upstream;
        let forwarding = Arc::clone(&self.forwarding);
        let accepting = tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                // Checked inside the forwarder as well as at registration: the
                // cut can land after the spawn and before the connect, and a
                // connection that escaped it would make an outage stage observe
                // a database that never went away.
                let cut = Arc::clone(&forwarding);
                let forwarding_task = tokio::spawn(async move {
                    let Ok(mut outbound) = TcpStream::connect(upstream).await else {
                        return;
                    };
                    if cut.lock().expect("not poisoned").severed {
                        return;
                    }
                    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                });
                let mut registry = forwarding.lock().expect("not poisoned");
                if registry.severed {
                    forwarding_task.abort();
                } else {
                    registry.tasks.push(forwarding_task);
                }
            }
        });
        *self.accepting.lock().expect("not poisoned") = Some(accepting);
    }
}

impl Drop for SeverableLink {
    fn drop(&mut self) {
        self.sever();
    }
}

/// Point a `postgres://` DSN at a different host and port, keeping everything
/// else — user, password, database, parameters — as the operator wrote it.
///
/// Returns `None` for a DSN this cannot rewrite faithfully; the caller skips
/// rather than guessing, because a stage that qualified the wrong database
/// would be worse than a stage that did not run.
pub(crate) fn redirect(dsn: &str, port: u16) -> Option<String> {
    let (scheme, rest) = dsn.split_once("://")?;
    if !matches!(scheme, "postgres" | "postgresql") {
        return None;
    }
    // The authority ends at the first `/`, `?`, or the end of the string; the
    // host starts after the last `@` inside it, since a password may contain one.
    let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(authority_end);
    let (credentials, host) = match authority.rsplit_once('@') {
        Some((credentials, host)) => (format!("{credentials}@"), host),
        None => (String::new(), authority),
    };
    // A comma-separated host list is a multi-host failover DSN: rewriting it to
    // one address would change what is being qualified.
    if host.contains(',') {
        return None;
    }
    Some(format!("{scheme}://{credentials}127.0.0.1:{port}{tail}"))
}

/// PostgreSQL's default port, used when a DSN names a host and no port, exactly
/// as a client would.
const DEFAULT_PORT: u16 = 5432;

/// The address a DSN points at, for the link to forward to.
///
/// Resolved rather than parsed: a CI database is as likely to be `postgres:5432`
/// or `db.internal` as a numeric address, and refusing a name would skip every
/// stage on exactly the deployments worth qualifying. Multi-host and Unix-socket
/// DSNs are still refused — there is no single link to cut.
pub(crate) async fn upstream(dsn: &str) -> Option<SocketAddr> {
    let config: tokio_postgres::Config = dsn.parse().ok()?;
    let hosts = config.get_hosts();
    let [tokio_postgres::config::Host::Tcp(host)] = hosts else {
        return None;
    };
    let port = config.get_ports().first().copied().unwrap_or(DEFAULT_PORT);
    tokio::net::lookup_host((host.as_str(), port))
        .await
        .ok()?
        .next()
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    /// A connection accepted at the instant of the cut must not survive it.
    ///
    /// The forwarder is spawned before its handle is registered, so a cut in
    /// between could once leave a live proxy running for the whole outage
    /// window — an outage stage observing a database that never went away. The
    /// severed flag is read under the registration lock and again inside the
    /// forwarder, so the connection is dropped whichever side of the race it
    /// lands on.
    #[tokio::test]
    async fn a_connection_accepted_at_the_cut_does_not_outlive_it() {
        let echo = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("an upstream to forward to");
        let upstream_addr = echo.local_addr().expect("bound");
        tokio::spawn(async move {
            while let Ok((mut inbound, _)) = echo.accept().await {
                tokio::spawn(async move {
                    let mut buffer = [0_u8; 64];
                    while let Ok(read) = inbound.read(&mut buffer).await {
                        if read == 0 || inbound.write_all(&buffer[..read]).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });

        let link = SeverableLink::open(upstream_addr)
            .await
            .expect("a link to the echo upstream");
        // Racing the cut against the accept: whichever wins, no byte may cross
        // the link afterwards.
        let mut client = TcpStream::connect(("127.0.0.1", link.port()))
            .await
            .expect("the link accepts while it is up");
        link.sever();

        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    if client.write_all(b"ping").await.is_err() {
                        return true;
                    }
                    let mut buffer = [0_u8; 4];
                    match client.read(&mut buffer).await {
                        Ok(0) | Err(_) => return true,
                        Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
                    }
                }
            })
            .await
            .unwrap_or(false),
            "a severed link cannot keep carrying traffic to the upstream"
        );
    }

    /// The same guarantee when the cut lands *during* a burst of connects,
    /// which is the ordering the registration race needed: a forwarder spawned
    /// before the drain and registered after it.
    #[tokio::test]
    async fn connections_racing_the_cut_do_not_outlive_it() {
        let echo = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("an upstream to forward to");
        let upstream_addr = echo.local_addr().expect("bound");
        tokio::spawn(async move { while echo.accept().await.is_ok() {} });

        let link = Arc::new(
            SeverableLink::open(upstream_addr)
                .await
                .expect("a link to the upstream"),
        );
        let port = link.port();
        let dialing = tokio::spawn(async move {
            let mut connected = Vec::new();
            for _ in 0..64 {
                if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)).await {
                    connected.push(stream);
                }
            }
            connected
        });
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        link.sever();
        let _connected = dialing.await.expect("the dialer finishes");

        assert!(
            link.forwarding
                .lock()
                .expect("not poisoned")
                .tasks
                .is_empty(),
            "no forwarder may be registered once the link is severed"
        );
    }

    /// The rewrite keeps the credentials and the database, because a link that
    /// silently dropped either would connect somewhere else and qualify it.
    #[test]
    fn a_dsn_is_redirected_without_losing_its_credentials_or_database() {
        assert_eq!(
            redirect("postgres://postgres:secret@db.internal:5432/axond", 6100).as_deref(),
            Some("postgres://postgres:secret@127.0.0.1:6100/axond")
        );
        assert_eq!(
            redirect("postgresql://db:5432/axond?sslmode=disable", 6100).as_deref(),
            Some("postgresql://127.0.0.1:6100/axond?sslmode=disable")
        );
    }

    /// A password containing an `@` is the case a naive split gets wrong: the
    /// host is what follows the *last* one.
    #[test]
    fn a_password_containing_an_at_sign_is_not_mistaken_for_a_host() {
        assert_eq!(
            redirect("postgres://user:p@ss@db:5432/axond", 6100).as_deref(),
            Some("postgres://user:p@ss@127.0.0.1:6100/axond")
        );
    }

    /// Anything this cannot rewrite faithfully is refused rather than guessed.
    #[test]
    fn a_dsn_this_cannot_rewrite_is_refused() {
        assert_eq!(redirect("host=db port=5432 user=postgres", 6100), None);
        assert_eq!(redirect("mysql://db:3306/axond", 6100), None);
        assert_eq!(redirect("postgres://a:5432,b:5432/axond", 6100), None);
    }

    /// A DSN with no port is a DSN on 5432, and a name is resolved rather than
    /// refused: both are how an operator's DSN is actually written.
    #[tokio::test]
    async fn a_named_host_resolves_and_a_missing_port_defaults() {
        let resolved = upstream("postgres://postgres@localhost/axond")
            .await
            .expect("localhost resolves");
        assert!(resolved.ip().is_loopback());
        assert_eq!(resolved.port(), DEFAULT_PORT);
        assert_eq!(
            upstream("postgres://postgres@127.0.0.1:6543/axond").await,
            Some(([127, 0, 0, 1], 6543).into())
        );
    }

    /// No single address means no single link to cut, so the harness refuses
    /// instead of qualifying one arbitrary member of a failover set.
    #[tokio::test]
    async fn a_dsn_without_one_tcp_host_has_no_link() {
        assert_eq!(upstream("postgres://a:5432,b:5432/axond").await, None);
        assert_eq!(
            upstream("host=/var/run/postgresql dbname=axond").await,
            None
        );
    }
}
