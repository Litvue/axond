//! Provider and backend fault qualification (issue #218).
//!
//! Every row in `qualification/faults/manifest.toml` is injected against a real
//! `axond` process and a deterministic fake provider — and, for the state-tier
//! rows, a TCP fault proxy in front of a real Redis or Postgres — and each row
//! writes a machine-readable artifact under `target/faults/` carrying the
//! injected fault and timing, the classification, the bound that ended the
//! request, the retries it cost, the upstream cleanup, the settled usage
//! outcome, the telemetry it exported, and the leakage scan of every surface a
//! caller or an operator can see.
//!
//! What fails here does not move with the machine. Milliseconds are asserted
//! only as ceilings — a row's own `deadline_ms` — and everything else is a
//! property of the gateway: the status and typed error, the attempt count, the
//! dispatch count, the usage status, that the upstream was released, and that
//! no provider endpoint, credential, DSN, or inbound key reached a surface it
//! must not.
//!
//! The rows run one at a time on purpose. Each boots its own process, and a
//! fault matrix that shared a replica between rows could no longer attribute
//! what it recorded to the fault it injected.

mod support;

use support::fault::{self, Fault, Outcome, Row};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_committed_fault_row_qualifies_and_publishes_its_evidence() {
    let (manifest, text) = fault::manifest::load();
    let mut failures = Vec::new();
    let mut skipped = Vec::new();

    for row in &manifest.rows {
        match fault::run(row, &text).await {
            Outcome::Skipped { row, reason } => {
                eprintln!("skipped {row}: {reason}");
                skipped.push(row);
            }
            Outcome::Ran(result) => {
                let path = result.write();
                eprintln!("{} -> {}", result.summary(), path.display());
                for verdict in result.failures() {
                    failures.push(format!(
                        "{}: {} expected {} but observed {}",
                        result.row.id, verdict.check, verdict.expected, verdict.observed
                    ));
                }
            }
        }
    }

    assert!(
        failures.is_empty(),
        "the fault matrix did not qualify:\n{}",
        failures.join("\n")
    );
    if !skipped.is_empty() {
        eprintln!(
            "skipped {} state-tier rows without a datastore: {}",
            skipped.len(),
            skipped.join(", ")
        );
    }
}

/// The matrix covers every fault the harness can inject, with unique ids and a
/// family that agrees with the fault. A row that quietly loses its coverage
/// would still produce an artifact, and the artifact would read as evidence.
#[test]
fn the_committed_matrix_covers_every_fault_the_harness_can_inject() {
    let (manifest, _) = fault::manifest::load();
    let mut ids: Vec<&str> = manifest.rows.iter().map(|row| row.id.as_str()).collect();
    ids.sort_unstable();
    let total = ids.len();
    ids.dedup();
    assert_eq!(total, ids.len(), "row ids must be unique: {ids:?}");

    for fault in EVERY_FAULT {
        assert!(
            manifest.rows.iter().any(|row| row.fault == fault),
            "no committed row injects {}",
            fault.as_str()
        );
    }
    for row in &manifest.rows {
        assert_eq!(
            row.family,
            row.fault.family(),
            "{}: the row's family disagrees with its fault",
            row.id
        );
        assert!(
            !row.fault.requires_stream() || row.streamed,
            "{}: {} is only meaningful on a streamed request",
            row.id,
            row.fault.as_str()
        );
        assert!(
            row.deadline_ms > 0,
            "{}: a row without a deadline asserts nothing about when the fault ends",
            row.id
        );
        assert_eq!(
            row.injected_latency_ms.is_some(),
            matches!(row.fault, Fault::RedisLatency | Fault::PostgresLatency),
            "{}: only a latency row injects a delay",
            row.id
        );
        assert!(
            !row.expect.metrics.is_empty(),
            "{}: a row that names no instrument cannot prove the fault was counted",
            row.id
        );
        assert_eq!(
            row.expect.during_outage_status.is_some(),
            row.fault.recovers(),
            "{}: only a recovery row observes the tier while it is down",
            row.id
        );
        assert!(
            !matches!(row.expect.status, Some(200)) || row.expect.usage_records > 0,
            "{}: a served request settles a usage record",
            row.id
        );
        assert_covers_both_policies(&manifest.rows, row);
    }
}

/// Every state tier that is exercised at all is exercised under both configured
/// stances: a fail-closed row without its fail-open twin qualifies half a
/// contract.
fn assert_covers_both_policies(rows: &[Row], row: &Row) {
    let Some(service) = row.service() else { return };
    for policy in ["deny", "allow"] {
        assert!(
            rows.iter().any(|other| other.service() == Some(service)
                && other.fault.on_unavailable() == Some(policy)),
            "{}: no committed row exercises `on_unavailable = \"{policy}\"` for {}",
            row.id,
            service.as_str()
        );
    }
}

/// The proxy the state-tier rows inject through carries TCP, so a DSN it cannot
/// stand in front of must be reported as a skip rather than redirected into a
/// handshake failure that would be read as the injected fault.
#[test]
fn only_a_dsn_the_fault_proxy_can_carry_is_redirected_through_it() {
    let proxy = "127.0.0.1:7000".parse().expect("a loopback address");
    let redirect = |dsn| fault::injector::redirect(dsn, proxy).map(|(_, dsn)| dsn);

    assert_eq!(
        redirect("redis://127.0.0.1:6399"),
        Some("redis://127.0.0.1:7000".to_owned())
    );
    assert_eq!(
        redirect("postgres://postgres:pw@127.0.0.1:55432/postgres"),
        Some("postgres://postgres:pw@127.0.0.1:7000/postgres".to_owned())
    );
    for tls in ["rediss://127.0.0.1:6380", "rediss://example.invalid"] {
        assert_eq!(redirect(tls), None, "a TLS endpoint cannot be proxied");
    }
}

const EVERY_FAULT: [Fault; 22] = [
    Fault::ProviderRateLimited,
    Fault::ProviderRateLimitedFailover,
    Fault::ProviderServerError,
    Fault::ProviderServerErrorFailover,
    Fault::DnsFailure,
    Fault::ConnectRefused,
    Fault::TlsHandshake,
    Fault::ResponseHeaderTimeout,
    Fault::BufferedBodyTimeout,
    Fault::StreamIdleBeforeBytes,
    Fault::StreamIdleAfterBytes,
    Fault::StreamTruncation,
    Fault::OversizedResponseBody,
    Fault::OversizedErrorBody,
    Fault::RedisLatency,
    Fault::RedisOutageFailClosed,
    Fault::RedisOutageFailOpen,
    Fault::RedisRecovery,
    Fault::PostgresLatency,
    Fault::PostgresOutageFailClosed,
    Fault::PostgresOutageFailOpen,
    Fault::PostgresRecovery,
];
