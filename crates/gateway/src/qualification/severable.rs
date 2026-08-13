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
    forwarding: Arc<Mutex<Vec<JoinHandle<()>>>>,
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
            forwarding: Arc::new(Mutex::new(Vec::new())),
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
        for forwarding in std::mem::take(&mut *self.forwarding.lock().expect("not poisoned")) {
            forwarding.abort();
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
                let forwarding_task = tokio::spawn(async move {
                    let Ok(mut outbound) = TcpStream::connect(upstream).await else {
                        return;
                    };
                    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                });
                forwarding
                    .lock()
                    .expect("not poisoned")
                    .push(forwarding_task);
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

/// The address a DSN points at, for the link to forward to.
pub(crate) fn upstream(dsn: &str) -> Option<SocketAddr> {
    let config: tokio_postgres::Config = dsn.parse().ok()?;
    let port = *config.get_ports().first()?;
    let host = config.get_hosts().first()?;
    let tokio_postgres::config::Host::Tcp(host) = host else {
        return None;
    };
    let addr = if host == "localhost" {
        "127.0.0.1".to_owned()
    } else {
        host.clone()
    };
    format!("{addr}:{port}").parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
