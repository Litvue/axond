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
//! what it recorded to the fault it injected. Ordering inside one test binary
//! is not enough for that: a `cargo test --workspace` run has other binaries
//! loading the same machine at the same time. So the matrix runs only when
//! [`LANE`] is set, which is what the dedicated lane — `just faults`, or the
//! fault-qualification step in CI — does when it runs this binary on its own.
//! Every other test in this file is a pure assertion and runs everywhere.

mod support;

use serde_json::json;
use support::fault::result::{Cleanup, Outage};
use support::fault::{self, Fault, Outcome, Row};

/// Set by the lane that runs this binary alone, and by nothing else.
const LANE: &str = "AXOND_FAULT_MATRIX";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_committed_fault_row_qualifies_and_publishes_its_evidence() {
    if std::env::var(LANE).as_deref() != Ok("1") {
        eprintln!(
            "skipped the fault matrix: it qualifies timing, so it runs only in its own lane ({LANE}=1, or `just faults`)"
        );
        return;
    }
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

/// Attribution is by the identity a record carries, not by where it landed in
/// the stream: a backend row primes the tier and probes it before it measures
/// anything, and settlement is detached from the response, so an earlier
/// request's record can arrive after the measured one's.
#[test]
fn a_record_belongs_to_the_request_whose_identity_it_carries() {
    // A UUIDv7 whose first 48 bits are the mint time in milliseconds.
    let minted =
        |at: u128| json!({ "request_id": format!("req_{at:012x}-7000-8000-0000-0000000000") });
    let measured_from = 2_000;

    let (measured, counts) = fault::run::attribute(
        &[
            minted(1_000),
            minted(3_000),
            // The priming request's record, landing after the measured one's.
            minted(1_500),
            json!({ "request_id": "not-an-identity" }),
        ],
        measured_from,
    );

    assert_eq!(
        measured.len(),
        1,
        "only the record minted after the request"
    );
    assert_eq!(counts.measured, 1);
    assert_eq!(counts.earlier, 2, "both earlier records stay earlier");
    assert_eq!(counts.unattributable, 1);
    assert_eq!(fault::run::minted_at_unix_ms("req_nonsense"), None);
}

/// A Postgres row keeps its spend in a run-scoped table, and the store derives
/// a fence trigger name from that table. Postgres refuses an identifier of 64
/// characters, so the table name has to leave room for what is derived from it.
#[test]
fn a_postgres_rows_table_leaves_room_for_the_identifiers_derived_from_it() {
    let (manifest, _) = fault::manifest::load();
    for row in manifest
        .rows
        .iter()
        .filter(|row| row.service() == Some(fault::Service::Postgres))
    {
        let table = fault::run::budget_table(&row.id);
        assert!(
            table.len() + "_namespace_fence".len() < 64,
            "`{table}` is too long for the store's derived identifiers"
        );
    }
}

/// The cleanup timing field is a gate, not a note. A release that arrives only
/// at process shutdown is the regression it exists to catch.
#[test]
fn cleanup_evidence_fails_when_the_upstream_is_released_late_or_not_at_all() {
    let cleanup = |settled_within_ms, open_at_end| Cleanup {
        upstream_streams_opened: 1,
        upstream_streams_open_at_end: open_at_end,
        settled_within_ms,
        process_exited_cleanly: true,
    };
    let failed = |cleanup: &Cleanup, cleaned_up| {
        fault::run::cleanup_verdicts(cleanup, cleaned_up, true)
            .into_iter()
            .filter(|verdict| !verdict.passed)
            .map(|verdict| verdict.check)
            .collect::<Vec<_>>()
    };

    assert!(failed(&cleanup(20, 0), true).is_empty());
    assert_eq!(
        failed(&cleanup(fault::run::CLEANUP_SETTLE_BOUND_MS + 1, 0), true),
        ["upstream_released_promptly"],
        "a release that took longer than the bound is a delayed release"
    );
    assert_eq!(
        failed(&cleanup(20, 1), false),
        ["upstream_cleanup"],
        "an upstream still open once the caller is gone is a leak"
    );
    assert_eq!(
        fault::run::cleanup_verdicts(
            &Cleanup {
                upstream_streams_opened: 0,
                ..cleanup(20, 0)
            },
            true,
            true,
        )
        .into_iter()
        .filter(|verdict| !verdict.passed)
        .map(|verdict| verdict.check)
        .collect::<Vec<_>>(),
        ["upstream_abandoned_response_tracked"],
        "a row that abandons a response must have opened one"
    );
}

/// The outage window is a gate too: a window that closed before the request it
/// is offered as the explanation for explains nothing.
#[test]
fn outage_evidence_fails_when_the_window_does_not_cover_what_it_explains() {
    let request_started = 10_000;
    let request_elapsed = 500;
    let failed = |outage: &Outage| {
        fault::run::outage_verdicts(outage, request_started, request_elapsed)
            .into_iter()
            .filter(|verdict| !verdict.passed)
            .map(|verdict| verdict.check)
            .collect::<Vec<_>>()
    };
    let outage = |restored_at_unix_ms, duration_ms| Outage {
        began_at_unix_ms: 9_000,
        restored_at_unix_ms,
        duration_ms,
        connections_carried: 1,
        connections_severed: 1,
    };

    assert!(
        failed(&outage(None, 1_500)).is_empty(),
        "began to past the request"
    );
    assert_eq!(
        failed(&outage(None, 900)),
        ["outage_window_recorded"],
        "a window that stopped at the request start does not cover it"
    );
    assert!(failed(&outage(Some(9_600), 600)).is_empty());
    assert_eq!(
        failed(&outage(Some(9_600), 100)),
        ["outage_window_recorded"],
        "a recovery window shorter than its own restore point"
    );
    assert_eq!(
        failed(&Outage {
            connections_severed: 0,
            ..outage(None, 1_500)
        }),
        ["outage_severed_connections"],
        "an outage that severed nothing was not an outage"
    );
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
