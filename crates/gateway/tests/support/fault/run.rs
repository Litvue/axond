//! The fault qualification driver: injects one matrix row's fault against a
//! real `axond` process and records what the gateway did about it.
//!
//! One row, one process. A fault suite that shared a replica between rows would
//! have to reason about a previous row's circuit state, credential parking, and
//! datastore connections, and the artifact could no longer claim the row caused
//! what it recorded. Booting per row costs a second and buys attribution.
//!
//! The process is stopped with `SIGTERM` at the end of every row rather than
//! killed: a clean shutdown is what flushes the OpenTelemetry batch exporters,
//! so the telemetry evidence and the cleanup evidence are the same event.

use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use serde_json::{Value, json};

use super::collector::Collector;
use super::injector::{
    FaultProxy, GarbageTls, Mode, UNRESOLVABLE_BASE_URL, redirect, refused_addr,
};
use super::manifest::{self, Fault, Row, Service};
use super::result::{
    Classification, Cleanup, Deadline, Environment, FaultResult, Finding, Injection, Leakage,
    Outage, Retries, RowEcho, RunMeta, Surface, Telemetry, Timing, UsageOutcome, Verdict,
};
use crate::support::gateway::{self, Axond, GATEWAY_KEY, Options, alias};
use crate::support::upstream::{FakeUpstream, target};

/// The bounds every row is served under. Written out rather than defaulted, so
/// the recorded config hash pins them: the phase a fault ends in is only
/// evidence if the bound that ended it is part of the artifact.
///
/// `max_attempts = 2` for every row, including the rows that must *not* retry:
/// a walk that stops at one attempt with two allowed has stopped because the
/// gateway decided to, which is the property under test.
const TUNING: &str = r#"
[failover]
max_attempts = 2
overall_timeout_ms = 20000

[transport]
connect_timeout_ms = 2000
response_header_timeout_ms = 600
buffered_body_timeout_ms = 600
stream_idle_timeout_ms = 600
max_response_bytes = 65536
max_error_bytes = 4096
"#;

const MAX_ATTEMPTS: u32 = 2;
const CONNECT_TIMEOUT_MS: u64 = 2_000;
const RESPONSE_HEADER_TIMEOUT_MS: u64 = 600;
const BUFFERED_BODY_TIMEOUT_MS: u64 = 600;
const STREAM_IDLE_TIMEOUT_MS: u64 = 600;
const OVERALL_TIMEOUT_MS: u64 = 20_000;

/// The variable a backend row's connection string arrives in. The generated
/// config references it by name, exactly as a deployment does: no DSN is ever
/// written into a config file, a log line, or an artifact.
const STATE_DSN_ENV: &str = "AXOND_FAULT_STATE_DSN";

/// Aliases the driver adds on top of the shared harness config.
mod fault_alias {
    pub const RATE_LIMITED: &str = "fault-rate-limited";
    pub const RATE_LIMITED_FAILOVER: &str = "fault-rate-limited-failover";
    pub const SERVER_ERROR_FAILOVER: &str = "fault-server-error-failover";
    pub const DNS: &str = "fault-dns";
    pub const CONNECT: &str = "fault-connect";
    pub const TLS: &str = "fault-tls";
}

/// How long a row waits for the accounting to settle behind the last client
/// byte, and for the upstream to be released.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(20);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(20);
/// How long the process is given to shut down cleanly, which is also how long
/// its exporters have to flush.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(15);
/// Extra delay the latency rows inject into every datastore read.
const LATENCY_SETTLE: Duration = Duration::from_millis(200);
/// How long the usage stream must stand still before a request that has already
/// answered is taken to have settled everything it was going to.
const USAGE_QUIET_WINDOW: Duration = Duration::from_millis(400);

/// The result of asking the harness to run a row.
pub enum Outcome {
    Ran(Box<FaultResult>),
    /// The row needs a datastore this run has not been given. Skipping is
    /// reported rather than silently passing, and CI sets
    /// `AXOND_TEST_REQUIRE_SERVICES=1` so a skipped backend row fails there.
    Skipped {
        row: String,
        reason: String,
    },
}

/// Where a backend row's datastore lives and how the row reaches it.
struct Backend {
    proxy: FaultProxy,
    /// The connection string the *process* is given: the same DSN, pointed at
    /// the proxy. Never recorded.
    dsn: String,
}

/// Everything the boot and the request need for one row.
struct Wiring {
    alias: String,
    extra_config: String,
    env: Vec<(String, String)>,
    /// A description of the injection with no address in it.
    how: String,
    bound: &'static str,
    bound_ms: Option<u64>,
}

pub async fn run(row: &Row, manifest_text: &str) -> Outcome {
    let service = row.service();
    let dsn = match service {
        Some(service) => match dsn_for(service) {
            Some(dsn) => Some(dsn),
            None => {
                return Outcome::Skipped {
                    row: row.id.clone(),
                    reason: format!(
                        "{} is not configured, so the {} row cannot run",
                        service.dsn_env(),
                        service.as_str()
                    ),
                };
            }
        },
        None => None,
    };

    let collector = Collector::start().await;
    let upstream = FakeUpstream::start().await;
    let tls = GarbageTls::start().await;
    let refused = refused_addr();

    let backend = match dsn.as_deref() {
        Some(dsn) => {
            let Some((authority, _)) = redirect(dsn, refused) else {
                return Outcome::Skipped {
                    row: row.id.clone(),
                    reason: "the configured connection string is not a URL the harness can \
                             redirect through its fault proxy"
                        .to_owned(),
                };
            };
            let proxy = FaultProxy::start(&authority).await;
            let (_, dsn) = redirect(dsn, proxy.addr).expect("the DSN redirects to the proxy");
            Some(Backend { proxy, dsn })
        }
        None => None,
    };

    let wiring = wiring_for(row, &upstream, refused, &tls, backend.as_ref());
    let mut env: Vec<(&str, &str)> = wiring
        .env
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    // The gateway's spans are emitted at info on `axond.*` targets, so the
    // suite's default `warn` filter would export metrics and no traces at all.
    env.push(("RUST_LOG", "warn,axond=info"));
    env.push(("OTEL_EXPORTER_OTLP_ENDPOINT", &collector.endpoint));
    env.push(("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf"));
    // The default periodic reader interval is a minute; a row lives for a
    // second, and its shutdown flush is what actually delivers the points.
    env.push(("OTEL_METRIC_EXPORT_INTERVAL", "1000"));
    env.push(("OTEL_BSP_SCHEDULE_DELAY", "200"));

    let mut gateway = Axond::start_with_options(
        &upstream.base_url,
        Options::new(TUNING)
            .with_config(&wiring.extra_config)
            .with_env(&env),
    )
    .await;
    let bind = gateway
        .base_url
        .strip_prefix("http://")
        .expect("a loopback base URL")
        .to_owned();
    let environment = Environment::collect_normalizing(
        &gateway.config,
        &bind,
        &upstream.base_url,
        &[
            (format!("http://{refused}"), "http://127.0.0.1:REFUSED_PORT"),
            (tls.base_url(), "https://127.0.0.1:TLS_PORT"),
            (key_prefix_root(), "axond-fault-PID"),
        ],
        manifest::MANIFEST_RELATIVE,
        manifest_text,
    );

    // A backend row's first request is served while the tier is healthy, so the
    // pool has a live connection for the outage to sever and the recovery rows
    // have something to recover.
    if backend.is_some() {
        let _ = request(&gateway, &wiring.alias, row.streamed).await;
    }
    let carried_before = backend.as_ref().map_or(0, |b| b.proxy.accepted());

    let mut outage = None;
    let mut during_outage_status = None;
    let outage_began_at = Instant::now();
    if let Some(backend) = backend.as_ref() {
        let began = unix_ms();
        let began_at = outage_began_at;
        match row.fault {
            Fault::RedisLatency | Fault::PostgresLatency => {
                backend.proxy.set(Mode::Latency(Duration::from_millis(
                    row.injected_latency_ms.unwrap_or(0),
                )));
            }
            _ => {
                backend.proxy.set(Mode::Outage);
                // The proxy tears live pooled connections down from outside the
                // copy loops; give that a moment to land before the tier is
                // asked for anything.
                tokio::time::sleep(LATENCY_SETTLE).await;
                if row.fault.recovers() {
                    // Prove the tier is actually down before recovering it: a
                    // recovery row that never observed an outage proves nothing.
                    let probe = request(&gateway, &wiring.alias, row.streamed).await;
                    during_outage_status = probe.status;
                    backend.proxy.set(Mode::Pass);
                    tokio::time::sleep(LATENCY_SETTLE).await;
                }
            }
        }
        if !matches!(row.fault, Fault::RedisLatency | Fault::PostgresLatency) {
            outage = Some(Outage {
                began_at_unix_ms: began,
                restored_at_unix_ms: row.fault.recovers().then(unix_ms),
                duration_ms: began_at.elapsed().as_millis(),
                connections_carried: 0,
                connections_severed: 0,
            });
        }
    }

    // Everything below is attributed to the measured request alone: a backend
    // row has already sent a priming request and possibly an outage probe, and
    // their records and dispatches are not this row's. Settlement is detached
    // from the response, so the earlier records are waited out rather than
    // assumed written: one still in flight would be read as this row's.
    if backend.is_some() {
        await_quiet_usage(&gateway).await;
    }
    let records_before = gateway.usage_records().len();
    let upstream_before = upstream.state.requests().len() as u64;
    let run_started_at = SystemTime::now();
    let started_at = unix_ms();
    let started = Instant::now();
    let observed = request(&gateway, &wiring.alias, row.streamed).await;
    let elapsed = started.elapsed();

    // Counted once the measured request is in: a connection is severed by the
    // proxy's watcher, not by the call that flipped the mode. A row that never
    // restores the tier is still down here, so its window runs to now.
    if let (Some(outage), Some(backend)) = (outage.as_mut(), backend.as_ref()) {
        outage.connections_carried = backend.proxy.accepted() - carried_before;
        outage.connections_severed = backend.proxy.severed();
        if outage.restored_at_unix_ms.is_none() {
            outage.duration_ms = outage_began_at.elapsed().as_millis();
        }
    }

    let records = await_usage_records(&gateway, records_before, row.expect.usage_records).await;
    let upstream_requests = upstream.state.requests().len() as u64 - upstream_before;
    let cleanup_started = Instant::now();
    let cleaned = await_upstream_release(&upstream).await;
    // Read before the process is stopped: a settle time that also contained the
    // shutdown could not be used to spot a slow release.
    let settled_within_ms = cleanup_started.elapsed().as_millis();

    gateway.terminate();
    let exit = gateway.await_exit(SHUTDOWN_TIMEOUT).await;
    // The exporters flush on shutdown; the last export lands just after the
    // process is gone.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let output = gateway.output();
    let measured = records.last().cloned();
    let usage = usage_outcome(&records, measured.as_ref());
    let attempts = measured
        .as_ref()
        .and_then(|record| record["attempts"].as_u64())
        .unwrap_or(0);
    let (metrics_observed, metrics_missing) = metric_evidence(&collector, &row.expect.metrics);

    let leakage = scan(
        &upstream,
        backend.as_ref(),
        &observed.body_text,
        &output,
        &records,
        &collector,
    );

    let result = FaultResult {
        schema_version: manifest::RESULT_SCHEMA_VERSION,
        row: RowEcho::new(row),
        run: RunMeta::for_harness("axond fault matrix harness", run_started_at, elapsed),
        environment,
        injection: Injection {
            fault: row.fault.as_str().to_owned(),
            family: row.family.as_str().to_owned(),
            service: service.map(|s| s.as_str().to_owned()),
            on_unavailable: row.fault.on_unavailable().map(str::to_owned),
            how: wiring.how.clone(),
            injected_latency_ms: row.injected_latency_ms,
            outage,
            timing: Timing {
                started_at_unix_ms: started_at,
                elapsed_ms: elapsed.as_millis(),
                first_byte_ms: observed.first_byte_ms,
            },
        },
        classification: Classification {
            status: observed.status,
            error_type: observed.error_type.clone(),
            phase: observed.phase.clone(),
            transport_failure: observed.status.is_none(),
            relayed_output_bytes: observed.relayed_output_bytes,
            during_outage_status,
            after_recovery_status: row.fault.recovers().then_some(observed.status).flatten(),
        },
        deadline: Deadline {
            bound: wiring.bound.to_owned(),
            bound_ms: wiring.bound_ms,
            wall_clock_ms: row.deadline_ms,
            elapsed_ms: elapsed.as_millis(),
        },
        retries: Retries {
            attempts,
            upstream_requests,
            max_attempts: MAX_ATTEMPTS,
        },
        usage,
        cleanup: Cleanup {
            upstream_streams_opened: upstream.state.opened_streams(),
            upstream_streams_open_at_end: upstream.state.open_streams(),
            settled_within_ms,
            process_exited_cleanly: exit.is_some_and(|status| status.success()),
        },
        telemetry: Telemetry {
            collector: true,
            exports: collector.counts(),
            bytes: collector.bytes(),
            metrics_observed,
            metrics_missing,
            spans_observed: spans_observed(&collector),
        },
        leakage,
        verdicts: Vec::new(),
    };
    let cleaned_within = cleaned;
    Outcome::Ran(Box::new(judge(row, result, cleaned_within)))
}

/// Everything the driver measured about one request.
#[derive(Default)]
struct ObservedRequest {
    status: Option<u16>,
    error_type: Option<String>,
    phase: Option<String>,
    body_text: String,
    relayed_output_bytes: u64,
    first_byte_ms: Option<u128>,
}

async fn request(gateway: &Axond, alias: &str, streamed: bool) -> ObservedRequest {
    let client = crate::support::client();
    let started = Instant::now();
    let response = client
        .post(gateway.url("/v1/chat/completions"))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({
            "model": alias,
            "stream": streamed,
            "messages": [{ "role": "user", "content": "fault matrix" }],
        }))
        .send()
        .await;
    let Ok(response) = response else {
        return ObservedRequest::default();
    };
    let status = response.status().as_u16();
    if !streamed || !response.status().is_success() {
        let body_text = response.text().await.unwrap_or_default();
        let first_byte_ms = Some(started.elapsed().as_millis());
        let json: Option<Value> = serde_json::from_str(&body_text).ok();
        let relayed_output_bytes = if status == 200 {
            body_text.len() as u64
        } else {
            0
        };
        return ObservedRequest {
            status: Some(status),
            error_type: json
                .as_ref()
                .and_then(|body| body["error"]["type"].as_str().map(str::to_owned)),
            phase: json
                .as_ref()
                .and_then(|body| body["error"]["message"].as_str())
                .and_then(phase_of),
            body_text,
            relayed_output_bytes,
            first_byte_ms,
        };
    }

    // A streamed answer: the relayed bytes are the evidence that output was
    // committed before the fault, which is what forbids a retry.
    let mut body_text = String::new();
    let mut first_byte_ms = None;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else { break };
        first_byte_ms.get_or_insert_with(|| started.elapsed().as_millis());
        body_text.push_str(&String::from_utf8_lossy(&chunk));
    }
    let relayed_output_bytes = provider_output_bytes(&body_text);
    let error_type = body_text
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter_map(|data| serde_json::from_str::<Value>(data).ok())
        .find_map(|event| event["error"]["type"].as_str().map(str::to_owned));
    ObservedRequest {
        status: Some(status),
        phase: error_type.as_deref().and_then(|_| phase_of(&body_text)),
        error_type,
        body_text,
        relayed_output_bytes,
        first_byte_ms,
    }
}

/// The provider's own output in a relayed stream: everything up to the
/// gateway's in-band error event. Counting that event too would make a stream
/// that carried no provider output look committed, which is exactly the
/// distinction the idle-before-bytes and idle-after-bytes rows exist to draw.
fn provider_output_bytes(body: &str) -> u64 {
    let mut bytes = 0;
    for event in body.split_inclusive("\n\n") {
        let gateway_error = event
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .filter_map(|data| serde_json::from_str::<Value>(data).ok())
            .any(|event| event["error"]["type"].is_string());
        if gateway_error {
            break;
        }
        bytes += event.len() as u64;
    }
    bytes
}

/// The transport phase a typed message names, when it names one. The messages
/// are part of the shipped contract, so reading them is reading the contract.
fn phase_of(message: &str) -> Option<String> {
    for phase in [
        "response header",
        "buffered body",
        "stream idle",
        "connect",
        "overall",
    ] {
        if message.contains(phase) {
            return Some(phase.replace(' ', "_"));
        }
    }
    None
}

fn wiring_for(
    row: &Row,
    upstream: &FakeUpstream,
    refused: std::net::SocketAddr,
    tls: &GarbageTls,
    backend: Option<&Backend>,
) -> Wiring {
    let price = format!(
        "{{ input_microdollars_per_million = {}, output_microdollars_per_million = {} }}",
        gateway::INPUT_PRICE,
        gateway::OUTPUT_PRICE
    );
    let provider = |id: &str, base_url: &str, env: &str| {
        format!(
            "[[provider]]\nid = \"{id}\"\nkind = \"openai\"\nbase_url = \"{base_url}\"\n\n\
             [[credential]]\nnamespace = \"platform\"\nprovider = \"{id}\"\nenv = \"{env}\"\nid = \"{id}-key\"\n\n"
        )
    };
    let model = |name: &str, targets: &[(&str, &str)]| {
        let targets = targets
            .iter()
            .map(|(provider, target)| {
                format!("{{ provider = \"{provider}\", model = \"{target}\", price = {price} }}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("[[model]]\nname = \"{name}\"\ntargets = [ {targets} ]\n\n")
    };

    // A second provider on the same fake upstream, with its own credential, so
    // a failover row fails *over* rather than retrying a parked key.
    let secondary = provider(
        "fake-openai-standby",
        &upstream.base_url,
        gateway::OPENAI_SECONDARY_ENV,
    );

    let (row_alias, extra, how, bound, bound_ms) = match row.fault {
        Fault::ProviderRateLimited => (
            fault_alias::RATE_LIMITED,
            model(
                fault_alias::RATE_LIMITED,
                &[("fake-openai", target::RATE_LIMITED)],
            ),
            "the fake provider answers 429 with no standby target behind it",
            "failover.overall_timeout_ms",
            Some(OVERALL_TIMEOUT_MS),
        ),
        Fault::ProviderRateLimitedFailover => (
            fault_alias::RATE_LIMITED_FAILOVER,
            format!(
                "{secondary}{}",
                model(
                    fault_alias::RATE_LIMITED_FAILOVER,
                    &[
                        ("fake-openai", target::RATE_LIMITED),
                        ("fake-openai-standby", target::CHAT),
                    ],
                )
            ),
            "the fake provider answers 429 on the first target and serves on the standby",
            "failover.overall_timeout_ms",
            Some(OVERALL_TIMEOUT_MS),
        ),
        Fault::ProviderServerError => (
            alias::CHAT_FAIL,
            String::new(),
            "the fake provider answers 500 with no standby target behind it",
            "failover.overall_timeout_ms",
            Some(OVERALL_TIMEOUT_MS),
        ),
        Fault::ProviderServerErrorFailover => (
            fault_alias::SERVER_ERROR_FAILOVER,
            format!(
                "{secondary}{}",
                model(
                    fault_alias::SERVER_ERROR_FAILOVER,
                    &[
                        ("fake-openai", target::FAIL),
                        ("fake-openai-standby", target::CHAT),
                    ],
                )
            ),
            "the fake provider answers 500 on the first target and serves on the standby",
            "failover.overall_timeout_ms",
            Some(OVERALL_TIMEOUT_MS),
        ),
        Fault::DnsFailure => (
            fault_alias::DNS,
            format!(
                "{}{}",
                provider("fault-dns", UNRESOLVABLE_BASE_URL, "GW_FAKE_OPENAI_KEY"),
                model(fault_alias::DNS, &[("fault-dns", target::CHAT)])
            ),
            "the provider's host is a reserved `.invalid` name that cannot resolve",
            "transport.connect_timeout_ms",
            Some(CONNECT_TIMEOUT_MS),
        ),
        Fault::ConnectRefused => (
            fault_alias::CONNECT,
            format!(
                "{}{}",
                provider(
                    "fault-connect",
                    &format!("http://{refused}"),
                    "GW_FAKE_OPENAI_KEY"
                ),
                model(fault_alias::CONNECT, &[("fault-connect", target::CHAT)])
            ),
            "the provider's loopback port has no listener, so the connect is refused",
            "transport.connect_timeout_ms",
            Some(CONNECT_TIMEOUT_MS),
        ),
        Fault::TlsHandshake => (
            fault_alias::TLS,
            format!(
                "{}{}",
                provider("fault-tls", &tls.base_url(), "GW_FAKE_OPENAI_KEY"),
                model(fault_alias::TLS, &[("fault-tls", target::CHAT)])
            ),
            "the provider's port accepts the connection and answers the handshake \
             with bytes that are not TLS",
            "transport.connect_timeout_ms",
            Some(CONNECT_TIMEOUT_MS),
        ),
        Fault::ResponseHeaderTimeout => (
            alias::CHAT_NO_HEADERS,
            String::new(),
            "the fake provider accepts the request and never sends response headers",
            "transport.response_header_timeout_ms",
            Some(RESPONSE_HEADER_TIMEOUT_MS),
        ),
        Fault::BufferedBodyTimeout => (
            alias::CHAT_SLOW_BODY,
            String::new(),
            "the fake provider sends headers and then never finishes the body",
            "transport.buffered_body_timeout_ms",
            Some(BUFFERED_BODY_TIMEOUT_MS),
        ),
        Fault::StreamIdleBeforeBytes => (
            alias::CHAT_STALL,
            String::new(),
            "the fake provider opens a stream and goes silent before any event",
            "transport.stream_idle_timeout_ms",
            Some(STREAM_IDLE_TIMEOUT_MS),
        ),
        Fault::StreamIdleAfterBytes => (
            alias::CHAT_STALL_AFTER_BYTES,
            String::new(),
            "the fake provider relays events and then goes silent, with bytes committed",
            "transport.stream_idle_timeout_ms",
            Some(STREAM_IDLE_TIMEOUT_MS),
        ),
        Fault::StreamTruncation => (
            alias::CHAT_DROP,
            String::new(),
            "the fake provider dies mid-event after relaying output",
            "transport.stream_idle_timeout_ms",
            Some(STREAM_IDLE_TIMEOUT_MS),
        ),
        Fault::OversizedResponseBody => (
            alias::CHAT_HUGE_BODY,
            String::new(),
            "the fake provider answers with a buffered body far above the byte bound",
            "transport.max_response_bytes",
            None,
        ),
        Fault::OversizedErrorBody => (
            alias::CHAT_HUGE_ERROR,
            String::new(),
            "the fake provider answers 500 with an error body far above the byte bound",
            "transport.max_error_bytes",
            None,
        ),
        _ => {
            let service = row.fault.service().expect("a backend row names a service");
            let policy = row.fault.on_unavailable().expect("a backend row has one");
            let prefix = format!("{}-{}", key_prefix_root(), row.id);
            let extra = match service {
                Service::Redis => format!(
                    r#"
[rate_limit]
backend = "redis"
dsn_env = "{STATE_DSN_ENV}"
on_unavailable = "{policy}"
key_prefix = "{prefix}"
max_in_flight_per_subject = 256
lease_ttl_seconds = 60
timeout_ms = 500
connect_timeout_ms = 500
"#
                ),
                Service::Postgres => format!(
                    r#"
[budget]
backend = "postgres"
dsn_env = "{STATE_DSN_ENV}"
on_unavailable = "{policy}"
limit_microdollars = 1000000000000
table = "axond_fault_budget"
create_table = true
key_prefix = "{prefix}"
reservation_ttl_seconds = 60
"#
                ),
            };
            let how = match (service, row.fault) {
                (_, Fault::RedisLatency | Fault::PostgresLatency) => {
                    "a TCP fault proxy in front of the datastore delays every forwarded read"
                }
                (_, fault) if fault.recovers() => {
                    "a TCP fault proxy severs the datastore connections, then carries them again"
                }
                _ => "a TCP fault proxy severs the datastore connections and refuses new ones",
            };
            let bound = match service {
                Service::Redis => "rate_limit.timeout_ms",
                Service::Postgres => "budget.postgres_connect_timeout",
            };
            (
                alias::CHAT,
                extra,
                how,
                bound,
                match service {
                    Service::Redis => Some(500),
                    Service::Postgres => None,
                },
            )
        }
    };

    Wiring {
        alias: row_alias.to_owned(),
        extra_config: extra,
        env: backend
            .map(|backend| vec![(STATE_DSN_ENV.to_owned(), backend.dsn.clone())])
            .unwrap_or_default(),
        how: how.to_owned(),
        bound,
        bound_ms,
    }
}

/// The run-scoped prefix a backend row's keys and rows live under, so two
/// harnesses sharing a datastore cannot read each other's state. It carries the
/// process id, and is normalised out of the recorded config for that reason.
fn key_prefix_root() -> String {
    format!("axond-fault-{}", std::process::id())
}

fn dsn_for(service: Service) -> Option<String> {
    match std::env::var(service.dsn_env()) {
        Ok(dsn) if !dsn.trim().is_empty() => Some(dsn),
        _ => {
            assert!(
                std::env::var("AXOND_TEST_REQUIRE_SERVICES").as_deref() != Ok("1"),
                "{} is required when AXOND_TEST_REQUIRE_SERVICES=1",
                service.dsn_env()
            );
            None
        }
    }
}

/// Wait until no further usage record appears, so a record already in flight
/// is not mistaken for the next request's. Settlement is detached from the
/// response, and a priming or probe request's record can land after it.
async fn await_quiet_usage(gateway: &Axond) {
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    let mut settled = gateway.usage_records().len();
    let mut quiet_since = Instant::now();
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let now = gateway.usage_records().len();
        if now != settled {
            settled = now;
            quiet_since = Instant::now();
        } else if quiet_since.elapsed() >= USAGE_QUIET_WINDOW {
            return;
        }
    }
}

/// The records the measured request settled: everything written after `before`.
/// A row expecting none still waits, because a record it should not have
/// written is the finding.
async fn await_usage_records(gateway: &Axond, before: usize, expected: u64) -> Vec<Value> {
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    let quiet = Instant::now() + Duration::from_millis(750);
    loop {
        let mut records = gateway.usage_records();
        let settled = (records.len().saturating_sub(before)) as u64;
        let done = if expected == 0 {
            Instant::now() >= quiet
        } else {
            settled >= expected
        };
        if done || Instant::now() >= deadline {
            return records.split_off(before.min(records.len()));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Whether every upstream body the row opened was released once the caller was
/// gone. A stalled upstream that outlives its request is a leak.
async fn await_upstream_release(upstream: &FakeUpstream) -> bool {
    let deadline = Instant::now() + CLEANUP_TIMEOUT;
    loop {
        if upstream.state.open_streams() <= 0 {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn usage_outcome(records: &[Value], measured: Option<&Value>) -> UsageOutcome {
    let mut by_status: BTreeMap<String, u64> = BTreeMap::new();
    for record in records {
        let status = record["status"].as_str().unwrap_or("unknown").to_owned();
        *by_status.entry(status).or_default() += 1;
    }
    UsageOutcome {
        records: records.len() as u64,
        by_status,
        measured_status: measured.and_then(|r| r["status"].as_str().map(str::to_owned)),
        cost_microdollars: measured.and_then(|r| r["cost_microdollars"].as_u64()),
        carries_request_id: measured
            .and_then(|r| r["request_id"].as_str())
            .is_some_and(|id| !id.is_empty()),
    }
}

fn metric_evidence(collector: &Collector, expected: &[String]) -> (Vec<String>, Vec<String>) {
    let mut observed = Vec::new();
    let mut missing = Vec::new();
    for metric in expected {
        if collector.signal_contains("metrics", metric) {
            observed.push(metric.clone());
        } else {
            missing.push(metric.clone());
        }
    }
    (observed, missing)
}

/// Span names the exported traces carry. Only the gateway's own spans are
/// looked for: a name that is not there is evidence, and a name nobody named is
/// not.
fn spans_observed(collector: &Collector) -> Vec<String> {
    [
        "axond.request",
        "axond.upstream.attempt",
        "http.server.request",
    ]
    .into_iter()
    .filter(|name| collector.signal_contains("traces", name))
    .map(str::to_owned)
    .collect()
}

/// The leakage scan. Needle *values* never enter the artifact: a finding names
/// the surface and the label of what leaked.
fn scan(
    upstream: &FakeUpstream,
    backend: Option<&Backend>,
    response: &str,
    output: &str,
    records: &[Value],
    collector: &Collector,
) -> Leakage {
    let authority = upstream
        .base_url
        .strip_prefix("http://")
        .expect("a loopback base URL")
        .to_owned();
    let mut needles: Vec<(&str, &str, String)> = vec![
        ("url", "upstream_base_url", upstream.base_url.clone()),
        ("url", "upstream_authority", authority),
        (
            "credential",
            "provider_openai_key",
            gateway::OPENAI_KEY.into(),
        ),
        (
            "credential",
            "provider_openai_key_standby",
            gateway::OPENAI_KEY_SECONDARY.into(),
        ),
        (
            "credential",
            "provider_anthropic_key",
            gateway::ANTHROPIC_KEY.into(),
        ),
        ("secret", "inbound_gateway_key", GATEWAY_KEY.into()),
    ];
    if let Some(backend) = backend {
        needles.push(("dsn", "state_dsn", backend.dsn.clone()));
        if let Some(password) = password_of(&backend.dsn) {
            needles.push(("dsn", "state_dsn_password", password));
        }
    }

    let usage_text = records
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    let telemetry_text: String = collector
        .exports()
        .iter()
        .map(|export| String::from_utf8_lossy(&export.bytes).into_owned())
        .collect();

    // Which needle kinds each surface must be free of. A provider endpoint in
    // the *operator's* logs is not a leak — an operator configured it — so the
    // process output and the telemetry are scanned for credentials, DSNs, and
    // secrets, while the caller-facing answer must not carry an endpoint either.
    let surfaces: [(&str, &str, &[&str]); 4] = [
        (
            "caller_response",
            response,
            &["url", "credential", "dsn", "secret"],
        ),
        (
            "usage_records",
            &usage_text,
            &["url", "credential", "dsn", "secret"],
        ),
        ("process_output", output, &["credential", "dsn", "secret"]),
        (
            "telemetry_exports",
            &telemetry_text,
            &["credential", "dsn", "secret"],
        ),
    ];

    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    for (kind, _, _) in &needles {
        *counts.entry((*kind).to_owned()).or_default() += 1;
    }
    let mut findings = Vec::new();
    let mut scanned = Vec::new();
    for (name, text, kinds) in surfaces {
        scanned.push(Surface {
            name: name.to_owned(),
            bytes_scanned: text.len() as u64,
        });
        for (kind, label, value) in &needles {
            if kinds.contains(kind) && !value.is_empty() && text.contains(value.as_str()) {
                findings.push(Finding {
                    surface: name.to_owned(),
                    kind: (*kind).to_owned(),
                    needle: (*label).to_owned(),
                });
            }
        }
    }
    Leakage {
        surfaces: scanned,
        needles: counts,
        findings,
    }
}

fn password_of(dsn: &str) -> Option<String> {
    let (_, rest) = dsn.split_once("://")?;
    let authority = rest.split(['/', '?']).next()?;
    let (userinfo, _) = authority.rsplit_once('@')?;
    let password = userinfo.split_once(':').map_or(userinfo, |(_, pw)| pw);
    (!password.is_empty()).then(|| password.to_owned())
}

/// Turn the measurements into the row's verdicts. Every property issue #218
/// asks a row to retain is checked here and written into the artifact, so a
/// stored result says why it passed rather than only that it did.
fn judge(row: &Row, mut result: FaultResult, cleaned_up: bool) -> FaultResult {
    let expect = &row.expect;
    let mut verdicts = vec![
        Verdict::equals("status", expect.status, result.classification.status),
        Verdict::equals(
            "error_type",
            expect.error_type.clone(),
            result.classification.error_type.clone(),
        ),
        Verdict::equals("attempts", expect.attempts, result.retries.attempts),
        Verdict::equals(
            "upstream_requests",
            expect.upstream_requests,
            result.retries.upstream_requests,
        ),
        Verdict::equals("usage_records", expect.usage_records, result.usage.records),
        Verdict::equals(
            "usage_status",
            Some(expect.usage_status.clone()).filter(|status| status != "none"),
            result.usage.measured_status.clone(),
        ),
        Verdict::equals(
            "relayed_output",
            expect.relayed_output,
            result.classification.relayed_output_bytes > 0,
        ),
        Verdict::at_most(
            "deadline",
            result.deadline.elapsed_ms,
            u128::from(row.deadline_ms),
        ),
        Verdict::holds(
            "upstream_cleanup",
            cleaned_up && result.cleanup.upstream_streams_open_at_end <= 0,
            format!("{} open", result.cleanup.upstream_streams_open_at_end),
        ),
        // A row that abandons an upstream response must have opened one, or its
        // clean count is clean only because nothing was ever at risk.
        Verdict::holds(
            "upstream_abandoned_response_tracked",
            !row.fault.abandons_upstream() || result.cleanup.upstream_streams_opened > 0,
            format!("{} opened", result.cleanup.upstream_streams_opened),
        ),
        Verdict::holds(
            "clean_shutdown",
            result.cleanup.process_exited_cleanly,
            result.cleanup.process_exited_cleanly.to_string(),
        ),
        Verdict::holds(
            "telemetry_exported",
            result.telemetry.exports.values().sum::<u64>() > 0,
            format!("{:?}", result.telemetry.exports),
        ),
        Verdict::holds(
            "telemetry_metrics",
            result.telemetry.metrics_missing.is_empty(),
            format!("missing {:?}", result.telemetry.metrics_missing),
        ),
        // A dispatched attempt has a span, and the span is how an operator
        // attributes a fault to a target at all.
        Verdict::holds(
            "telemetry_attempt_span",
            expect.upstream_requests == 0
                || result
                    .telemetry
                    .spans_observed
                    .iter()
                    .any(|span| span == "axond.upstream.attempt"),
            format!("{:?}", result.telemetry.spans_observed),
        ),
        Verdict::holds(
            "no_leakage",
            result.leakage.findings.is_empty(),
            format!("{:?}", result.leakage.findings),
        ),
    ];
    if let Some(during) = expect.during_outage_status {
        verdicts.push(Verdict::equals(
            "during_outage_status",
            Some(during),
            result.classification.during_outage_status,
        ));
    }
    if let Some(latency) = row.injected_latency_ms {
        verdicts.push(Verdict::at_least(
            "injected_latency_is_observable",
            result.deadline.elapsed_ms,
            u128::from(latency),
        ));
    }
    if let Some(outage) = result.injection.outage {
        verdicts.push(Verdict::holds(
            "outage_severed_connections",
            outage.connections_severed > 0,
            outage.connections_severed.to_string(),
        ));
    }
    if expect.usage_records > 0 {
        verdicts.push(Verdict::holds(
            "usage_carries_request_id",
            result.usage.carries_request_id,
            result.usage.carries_request_id.to_string(),
        ));
    }
    result.verdicts = verdicts;
    result
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock after 1970")
        .as_millis()
}
