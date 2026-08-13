//! The committed fault matrix: every scenario a qualification run injects.
//!
//! Rows are data rather than code, for the same reason capacity profiles are
//! (ADR 0033): a result artifact names the exact inputs that produced it, and
//! the manifest's own hash is recorded alongside the binary's and the fixtures'.
//! What a row *may* say is bounded by the [`Fault`] enum, so the manifest cannot
//! describe an injection the harness does not implement — and cannot claim a
//! backend row is a provider one.

use std::path::PathBuf;

use figment::Figment;
use figment::providers::{Format, Toml};
use serde::{Deserialize, Serialize};

/// The manifest, relative to the workspace root.
pub const MANIFEST_RELATIVE: &str = "qualification/faults/manifest.toml";

/// The manifest schema this harness understands.
pub const MANIFEST_SCHEMA_VERSION: u32 = 1;

/// The result-artifact schema version. Bumped when a field changes meaning, so
/// a stored artifact is never reinterpreted under a newer contract.
pub const RESULT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Manifest {
    pub schema_version: u32,
    #[serde(rename = "row")]
    pub rows: Vec<Row>,
}

/// One injected fault and everything the run must be able to prove about it.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Row {
    pub id: String,
    /// Redundant with `fault`, and checked against it: the family is what a
    /// reader scans the manifest by, so it is written down rather than inferred.
    pub family: Family,
    pub fault: Fault,
    pub description: String,
    /// Whether the caller asks for a stream. A fault that can only happen to a
    /// stream declares it, and the harness refuses a row that disagrees.
    #[serde(default)]
    pub streamed: bool,
    /// Wall-clock bound for the measured request. It is the row's own deadline
    /// evidence: a fault that ends on the right bound also ends *in time*.
    pub deadline_ms: u64,
    /// Backend rows: the delay the fault proxy adds to every forwarded read.
    #[serde(default)]
    pub injected_latency_ms: Option<u64>,
    pub expect: Expect,
}

impl Row {
    /// The state service the row needs, or `None` for a row that runs against
    /// the fake provider alone.
    pub fn service(&self) -> Option<Service> {
        self.fault.service()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Family {
    /// A verdict the provider itself returned.
    Provider,
    /// A failure of the path to the provider: name resolution, TCP, TLS, or a
    /// phase of the exchange that never finished.
    Transport,
    /// A state tier the gateway depends on: Redis or Postgres.
    Backend,
}

impl Family {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Provider => "provider",
            Self::Transport => "transport",
            Self::Backend => "backend",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Service {
    Redis,
    Postgres,
}

impl Service {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Redis => "redis",
            Self::Postgres => "postgres",
        }
    }

    /// The environment variable carrying the real service's connection string.
    /// The harness never inlines a DSN: it reads this one and points the process
    /// at a fault proxy in front of it, through an env reference of its own.
    pub fn dsn_env(self) -> &'static str {
        match self {
            Self::Redis => "AXOND_TEST_REDIS_URL",
            Self::Postgres => "AXOND_TEST_POSTGRES_DSN",
        }
    }
}

/// Every fault the harness can inject. One variant per matrix row shape: the
/// driver owns how each is produced, so a manifest cannot ask for an injection
/// nothing implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Fault {
    /// The provider answers `429` and the walk has nowhere else to go.
    ProviderRateLimited,
    /// The provider answers `429` and a second target serves the request.
    ProviderRateLimitedFailover,
    /// The provider answers `500` and the walk has nowhere else to go.
    ProviderServerError,
    /// The provider answers `500` and a second target serves the request.
    ProviderServerErrorFailover,
    /// The provider's hostname does not resolve.
    DnsFailure,
    /// Nothing is listening on the provider's port.
    ConnectRefused,
    /// The provider's port answers, but not with TLS.
    TlsHandshake,
    /// The request is accepted and no response headers ever arrive.
    ResponseHeaderTimeout,
    /// Headers arrive and the buffered body never finishes.
    BufferedBodyTimeout,
    /// A stream opens and goes silent before any event.
    StreamIdleBeforeBytes,
    /// A stream relays output and then goes silent, with bytes committed.
    StreamIdleAfterBytes,
    /// A stream dies mid-event after relaying output.
    StreamTruncation,
    /// A successful body larger than the configured response bound.
    OversizedResponseBody,
    /// An *error* body larger than the configured error bound.
    OversizedErrorBody,
    /// Redis is reachable but slow.
    RedisLatency,
    /// Redis is gone and the configured policy is fail-closed.
    RedisOutageFailClosed,
    /// Redis is gone and the configured policy is fail-open.
    RedisOutageFailOpen,
    /// Redis is gone, then comes back, and the replica serves again.
    RedisRecovery,
    /// Postgres is reachable but slow.
    PostgresLatency,
    /// Postgres is gone and the configured policy is fail-closed.
    PostgresOutageFailClosed,
    /// Postgres is gone and the configured policy is fail-open.
    PostgresOutageFailOpen,
    /// Postgres is gone, then comes back, and the replica serves again.
    PostgresRecovery,
}

impl Fault {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProviderRateLimited => "provider_rate_limited",
            Self::ProviderRateLimitedFailover => "provider_rate_limited_failover",
            Self::ProviderServerError => "provider_server_error",
            Self::ProviderServerErrorFailover => "provider_server_error_failover",
            Self::DnsFailure => "dns_failure",
            Self::ConnectRefused => "connect_refused",
            Self::TlsHandshake => "tls_handshake",
            Self::ResponseHeaderTimeout => "response_header_timeout",
            Self::BufferedBodyTimeout => "buffered_body_timeout",
            Self::StreamIdleBeforeBytes => "stream_idle_before_bytes",
            Self::StreamIdleAfterBytes => "stream_idle_after_bytes",
            Self::StreamTruncation => "stream_truncation",
            Self::OversizedResponseBody => "oversized_response_body",
            Self::OversizedErrorBody => "oversized_error_body",
            Self::RedisLatency => "redis_latency",
            Self::RedisOutageFailClosed => "redis_outage_fail_closed",
            Self::RedisOutageFailOpen => "redis_outage_fail_open",
            Self::RedisRecovery => "redis_recovery",
            Self::PostgresLatency => "postgres_latency",
            Self::PostgresOutageFailClosed => "postgres_outage_fail_closed",
            Self::PostgresOutageFailOpen => "postgres_outage_fail_open",
            Self::PostgresRecovery => "postgres_recovery",
        }
    }

    pub fn family(self) -> Family {
        match self {
            Self::ProviderRateLimited
            | Self::ProviderRateLimitedFailover
            | Self::ProviderServerError
            | Self::ProviderServerErrorFailover => Family::Provider,
            Self::DnsFailure
            | Self::ConnectRefused
            | Self::TlsHandshake
            | Self::ResponseHeaderTimeout
            | Self::BufferedBodyTimeout
            | Self::StreamIdleBeforeBytes
            | Self::StreamIdleAfterBytes
            | Self::StreamTruncation
            | Self::OversizedResponseBody
            | Self::OversizedErrorBody => Family::Transport,
            Self::RedisLatency
            | Self::RedisOutageFailClosed
            | Self::RedisOutageFailOpen
            | Self::RedisRecovery
            | Self::PostgresLatency
            | Self::PostgresOutageFailClosed
            | Self::PostgresOutageFailOpen
            | Self::PostgresRecovery => Family::Backend,
        }
    }

    pub fn service(self) -> Option<Service> {
        match self {
            Self::RedisLatency
            | Self::RedisOutageFailClosed
            | Self::RedisOutageFailOpen
            | Self::RedisRecovery => Some(Service::Redis),
            Self::PostgresLatency
            | Self::PostgresOutageFailClosed
            | Self::PostgresOutageFailOpen
            | Self::PostgresRecovery => Some(Service::Postgres),
            _ => None,
        }
    }

    /// The configured behaviour a backend row exercises, for the artifact.
    pub fn on_unavailable(self) -> Option<&'static str> {
        match self {
            Self::RedisOutageFailOpen | Self::PostgresOutageFailOpen => Some("allow"),
            Self::RedisLatency
            | Self::RedisOutageFailClosed
            | Self::RedisRecovery
            | Self::PostgresLatency
            | Self::PostgresOutageFailClosed
            | Self::PostgresRecovery => Some("deny"),
            _ => None,
        }
    }

    /// Whether the row takes the service away and brings it back, rather than
    /// leaving it down for the measured request.
    pub fn recovers(self) -> bool {
        matches!(self, Self::RedisRecovery | Self::PostgresRecovery)
    }

    /// Whether the fault is only meaningful for a streamed request.
    pub fn requires_stream(self) -> bool {
        matches!(
            self,
            Self::StreamIdleBeforeBytes | Self::StreamIdleAfterBytes | Self::StreamTruncation
        )
    }
}

/// What the row must be able to prove. Everything here is a property of the
/// gateway rather than of the machine: a slow runner changes the milliseconds,
/// not the classification, the retry count, or whether the charge settled.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Expect {
    /// The status the caller sees. `None` means the caller's request fails at
    /// the transport with no status at all, which no row asserts today but the
    /// schema admits rather than hiding.
    #[serde(default)]
    pub status: Option<u16>,
    /// The typed `error.type` in the answer body, when the answer is an error.
    #[serde(default)]
    pub error_type: Option<String>,
    /// How many upstream attempts the walk was allowed to make, as the usage
    /// record counts them. A retry that silently doubles is a cost regression.
    pub attempts: u64,
    /// Requests the fake provider must have seen for the measured request.
    pub upstream_requests: u64,
    /// Usage records the whole row must settle, probes included.
    #[serde(default = "one")]
    pub usage_records: u64,
    /// The status the measured request settles as.
    pub usage_status: String,
    /// Whether the caller received relayed provider output before the fault
    /// ended the request. A committed byte is what forbids a retry.
    #[serde(default)]
    pub relayed_output: bool,
    /// Metric instruments the run must have exported. An instrument only
    /// appears in an export once it has recorded a point, so naming one here is
    /// an assertion that the fault was *counted*, not only handled.
    #[serde(default)]
    pub metrics: Vec<String>,
    /// Recovery rows: the status the caller sees while the service is down.
    #[serde(default)]
    pub during_outage_status: Option<u16>,
}

fn one() -> u64 {
    1
}

/// The workspace root, resolved from this crate rather than from whatever
/// working directory the test runner happens to have.
pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

pub fn manifest_path() -> PathBuf {
    workspace_root().join(MANIFEST_RELATIVE)
}

/// Load the matrix, refusing a schema this harness does not understand: a
/// silently misread row would still produce an artifact, and the artifact would
/// read as evidence.
pub fn load() -> (Manifest, String) {
    let path = manifest_path();
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{} is unreadable: {e}", path.display()));
    let manifest: Manifest = Figment::from(Toml::file(&path))
        .extract()
        .unwrap_or_else(|e| panic!("{} is not a valid fault matrix: {e}", path.display()));
    assert_eq!(
        manifest.schema_version, MANIFEST_SCHEMA_VERSION,
        "unsupported fault matrix schema"
    );
    assert!(
        !manifest.rows.is_empty(),
        "the fault matrix declares no rows"
    );
    (manifest, text)
}
