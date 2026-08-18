//! The durable side of the run: the PostgreSQL table the replicas' usage sink
//! writes to, and the queries that reconcile it against what the processes said
//! they charged.
//!
//! Two rules shape this module.
//!
//! **Nothing here holds a run's rows in memory.** A twelve-hour run settles
//! millions. PostgreSQL rows are streamed one at a time into fixed-width,
//! sharded spill ledgers, then exact identity sets are merged one shard at a
//! time. Cardinality equality alone cannot prove the durable rows are the rows
//! the processes emitted.
//!
//! **Nothing here writes a DSN anywhere.** The connection string is read from
//! the environment, passed to the replicas by variable *name*, and never
//! recorded: an artifact says which backend and which schema, never how to
//! reach it or as whom.

use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use futures::StreamExt;
use serde::Serialize;
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::support::endurance::ledger::IdentityPairLedger;

/// The DSN the durable scenarios need, or `None` to skip them. The shared rule:
/// absent configuration skips, and `AXOND_TEST_REQUIRE_SERVICES=1` turns the
/// skip into a panic so CI cannot report green for a run that never happened.
pub fn dsn() -> Option<String> {
    crate::support::stateful::postgres_dsn()
}

/// One libpq keyword/value field, quoted so a credential is passed as it was
/// given. A value with a space in it ends the field otherwise, and a quote or a
/// backslash in it ends or escapes the wrong thing: single-quote everything and
/// escape the two characters that mean something inside the quotes.
pub fn quoted(value: &str) -> String {
    let escaped = value.replace('\\', r"\\").replace('\'', r"\'");
    format!("'{escaped}'")
}

/// Whether every host the DSN names is the loopback interface. Only a loopback
/// database is put behind the gate: the gate binds `127.0.0.1`, so a loopback
/// connection reaches it under the same name it would have used anyway, and any
/// TLS the sink negotiates is verified against that same name. A database
/// somewhere else is a different name and, under the default `prefer`, a
/// handshake the gate cannot stand in for — so it is left alone rather than
/// having its credentials rewritten towards a plaintext forwarder.
fn is_loopback(config: &tokio_postgres::Config) -> bool {
    use tokio_postgres::config::Host;

    !config.get_hosts().is_empty()
        && config.get_hosts().iter().all(|host| match host {
            Host::Tcp(host) => {
                host == "localhost"
                    || host
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|address| address.is_loopback())
            }
            // A Unix socket is already local, but it is not something a TCP
            // forwarder can front.
            _ => false,
        })
}

/// The DSN a replica is given so its usage sink reaches the database through
/// `gate`, and how it reaches it. A free function so the rebuild can be tested
/// against credentials a run would rather not have in its fixtures.
pub fn through_gate(dsn: &str, gate: &str) -> (String, Reach) {
    let config: tokio_postgres::Config = dsn.parse().expect("the test DSN is a valid one");
    // The gate is a byte forwarder, so it can carry plaintext or PostgreSQL's
    // TLS negotiation without terminating it. Keep the original host as the
    // certificate identity and use `hostaddr` for the gate's loopback socket.
    // Remote hosts stay direct: rewriting them would hand their credentials to
    // a local forwarder and change which outage the run is measuring.
    if !is_loopback(&config) {
        return (dsn.to_owned(), Reach::Direct);
    }
    let original_host = match config.get_hosts().first() {
        Some(tokio_postgres::config::Host::Tcp(host)) => host,
        _ => unreachable!("a loopback DSN has a TCP host"),
    };
    let (host, port) = gate
        .rsplit_once(':')
        .expect("the gate authority is host:port");
    let mut rebuilt = format!("host={} hostaddr={host} port={port}", quoted(original_host));
    if let Some(user) = config.get_user() {
        rebuilt.push_str(&format!(" user={}", quoted(user)));
    }
    if let Some(password) = config.get_password() {
        let password = String::from_utf8_lossy(password).into_owned();
        rebuilt.push_str(&format!(" password={}", quoted(&password)));
    }
    if let Some(dbname) = config.get_dbname() {
        rebuilt.push_str(&format!(" dbname={}", quoted(dbname)));
    }
    // Carried over rather than dropped: a DSN that named the application or set
    // server options meant them, and a replica connecting without them is not
    // the deployment the run means to qualify.
    if let Some(name) = config.get_application_name() {
        rebuilt.push_str(&format!(" application_name={}", quoted(name)));
    }
    if let Some(options) = config.get_options() {
        rebuilt.push_str(&format!(" options={}", quoted(options)));
    }
    if let Some(timeout) = config.get_connect_timeout() {
        rebuilt.push_str(&format!(" connect_timeout={}", timeout.as_secs()));
    }
    // Keep the parsed TLS mode explicit. This matters for `sslmode=disable`:
    // omitting it would make the rebuilt DSN fall back to `prefer`, and for
    // `require`, which must remain a TLS-capable connection through the gate.
    rebuilt.push_str(match config.get_ssl_mode() {
        tokio_postgres::config::SslMode::Disable => " sslmode=disable",
        tokio_postgres::config::SslMode::Prefer => " sslmode=prefer",
        tokio_postgres::config::SslMode::Require => " sslmode=require",
        _ => unreachable!("the parsed DSN has an unsupported TLS mode"),
    });
    (rebuilt, Reach::Gated)
}

/// The durable table one run owns, and how the run reaches it.
pub struct Durable {
    /// The schema this run created, dropped when the run ends.
    pub schema: String,
    /// `schema.table`, as the sink is configured with it.
    pub qualified_table: String,
    dsn: String,
}

/// How the replicas reach the database: directly, or through the fault gate
/// that can take it away.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reach {
    /// Through the gate, so the usage backend can be made to disappear.
    Gated,
    /// Directly, because the DSN names a database that is not on the loopback
    /// interface. A remote endpoint is not safe to rewrite towards a local
    /// forwarder; its outage is recorded as not evaluated rather than silently
    /// changing the connection's security or destination.
    Direct,
}

impl Durable {
    /// Create a schema of this run's own. The sink creates the table inside it
    /// at boot (`create_table = true`), so the shipped DDL is what the run is
    /// qualified against.
    pub async fn create(dsn: &str, stem: &str) -> Self {
        let schema = format!(
            "stateful_endurance_{stem}_{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        )
        .replace('-', "_");
        let client = connect(dsn).await;
        client
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .expect("the run's own schema is created");
        Self {
            qualified_table: format!("{schema}.axond_usage"),
            schema,
            dsn: dsn.to_owned(),
        }
    }

    /// The DSN the replicas are given, and how they reach the database with it.
    ///
    /// Rebuilt from the parsed connection string rather than string-substituted
    /// into it: a DSN can name its host in several shapes, and a harness that
    /// guesses which one produces a run that fails for the wrong reason.
    pub fn replica_dsn(&self, gate: &str) -> (String, Reach) {
        through_gate(&self.dsn, gate)
    }

    /// Where the database actually is, for the gate to forward to. Read from
    /// the parsed connection string rather than from the text of it, and never
    /// recorded anywhere.
    pub fn backend_authority(&self) -> String {
        let config: tokio_postgres::Config = self.dsn.parse().expect("the test DSN is a valid one");
        let host = match config.get_hosts().first() {
            Some(tokio_postgres::config::Host::Tcp(host)) => host.clone(),
            _ => "127.0.0.1".to_owned(),
        };
        let port = config.get_ports().first().copied().unwrap_or(5432);
        format!("{host}:{port}")
    }

    /// The server's version, for the artifact's provenance. A qualification
    /// result that does not say what it ran against cannot be compared with
    /// one that ran against something else.
    pub async fn backend_version(&self) -> String {
        let client = connect(&self.dsn).await;
        let row = client
            .query_one("SELECT version()", &[])
            .await
            .expect("the backend reports its version");
        row.get::<_, String>(0)
    }

    /// Every row the run's table holds, counted rather than fetched.
    pub async fn counts(&self) -> Counts {
        let client = connect(&self.dsn).await;
        let row = client
            .query_one(
                &format!(
                    "SELECT count(*)::bigint, count(DISTINCT request_id)::bigint FROM {}",
                    self.qualified_table
                ),
                &[],
            )
            .await
            .expect("the durable usage table can be counted");
        Counts {
            rows: row.get::<_, i64>(0).max(0) as u64,
            distinct: row.get::<_, i64>(1).max(0) as u64,
        }
    }

    /// Rows the gateway settled outside `[from, to)`, by the gateway's own
    /// `recorded_at`. The window the driver passes is its own fault window,
    /// widened by the attribution slack, so a row settled a moment either side
    /// of the outage is attributed to the outage rather than to a defect.
    pub async fn distinct_outside(&self, from: SystemTime, to: SystemTime) -> u64 {
        let client = connect(&self.dsn).await;
        let row = client
            .query_one(
                &format!(
                    "SELECT count(DISTINCT request_id)::bigint FROM {} \
                     WHERE recorded_at < $1 OR recorded_at >= $2",
                    self.qualified_table
                ),
                &[&from, &to],
            )
            .await
            .expect("the durable usage table can be counted by window");
        row.get::<_, i64>(0).max(0) as u64
    }

    /// Stream every durable request identity into the whole-run and
    /// outside-outage reconciliation ledgers. `query_raw` is deliberate: the
    /// database may hold millions of rows and the harness must not collect them
    /// in memory merely to prove their identities.
    pub async fn record_identities(
        &self,
        all: &mut IdentityPairLedger,
        outside: &mut IdentityPairLedger,
        outage: Option<(SystemTime, SystemTime)>,
    ) {
        let client = connect(&self.dsn).await;
        let query = format!(
            "SELECT request_id, recorded_at FROM {} ORDER BY request_id, recorded_at",
            self.qualified_table
        );
        let rows = client
            .query_raw(
                &query,
                std::iter::empty::<&(dyn tokio_postgres::types::ToSql + Sync)>(),
            )
            .await
            .expect("the durable usage identities can be streamed");
        tokio::pin!(rows);
        while let Some(row) = rows.next().await {
            let row = row.expect("a durable usage identity row is readable");
            let request_id: String = row.get(0);
            let recorded_at: SystemTime = row.get(1);
            all.record_observed(&request_id)
                .expect("PostgreSQL holds a canonical request id");
            if outage.is_none_or(|(from, to)| recorded_at < from || recorded_at >= to) {
                outside
                    .record_observed(&request_id)
                    .expect("PostgreSQL holds a canonical request id");
            }
        }
    }

    /// Wait until the table stops growing, and report how long that took.
    ///
    /// The sink batches, so the last rows of a run land after the last caller
    /// has gone; "settled" is therefore a table that has been still for
    /// `quiet`, not a table that has reached a number the harness predicted.
    pub async fn await_settled(&self, within: Duration, quiet: Duration) -> Settled {
        let deadline = std::time::Instant::now() + within;
        let started = std::time::Instant::now();
        let mut last = self.counts().await.rows;
        let mut still_since = std::time::Instant::now();
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let now = self.counts().await.rows;
            if now != last {
                last = now;
                still_since = std::time::Instant::now();
            }
            if still_since.elapsed() >= quiet {
                return Settled {
                    lag_ms: started.elapsed().as_millis() as u64,
                    within_bound: true,
                };
            }
            if std::time::Instant::now() >= deadline {
                return Settled {
                    lag_ms: started.elapsed().as_millis() as u64,
                    within_bound: false,
                };
            }
        }
    }

    /// Drop the run's schema. Called once the replicas are gone, so nothing is
    /// still writing to the table being dropped.
    pub async fn drop_schema(&self) {
        let client = connect(&self.dsn).await;
        let _ = client
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema))
            .await;
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Counts {
    pub rows: u64,
    pub distinct: u64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Settled {
    /// How long the table took to stop growing after the load stopped.
    pub lag_ms: u64,
    /// Whether it stopped growing at all before the bound.
    pub within_bound: bool,
}

async fn connect(dsn: &str) -> tokio_postgres::Client {
    // Use the same Rustls-capable connector as the gateway's PostgreSQL sinks,
    // so a TLS-enabled database is reconciled rather than refused. The gate only
    // forwards bytes, so `sslmode=prefer` and `sslmode=require` reach the server
    // as they were given.
    let config: tokio_postgres::Config = dsn.parse().expect("the test DSN is valid");
    match config.connect(tls_connector()).await {
        Ok((client, connection)) => {
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
        }
        // `prefer` prefers encryption rather than demanding it, as libpq does:
        // a loopback server offering a certificate this machine has no reason to
        // trust is not the finding this run is looking for, and refusing to
        // reconcile at all would abort the whole qualification over the
        // harness's own connection. `require` and `disable` are honoured as
        // they were given.
        Err(error) if may_fall_back(&config) => {
            let (client, connection) =
                config
                    .connect(tokio_postgres::NoTls)
                    .await
                    .unwrap_or_else(|plain| {
                        panic!(
                            "the stateful endurance run connects to PostgreSQL: TLS was \
                         preferred and failed ({error}), and so did the plaintext \
                         connection ({plain})"
                        )
                    });
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
        }
        Err(error) => panic!("the stateful endurance run connects to PostgreSQL: {error}"),
    }
}

/// Whether a failed TLS attempt may be retried without encryption. Only for
/// `prefer`, which is what libpq itself does — and what a DSN naming no mode at
/// all asks for.
pub fn may_fall_back(config: &tokio_postgres::Config) -> bool {
    matches!(
        config.get_ssl_mode(),
        tokio_postgres::config::SslMode::Prefer
    )
}

fn tls_connector() -> MakeRustlsConnect {
    static CONNECTOR: OnceLock<MakeRustlsConnect> = OnceLock::new();
    CONNECTOR
        .get_or_init(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
            MakeRustlsConnect::with_webpki_roots()
        })
        .clone()
}
