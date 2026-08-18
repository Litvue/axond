//! The driver: offers a profile's load to a real `axond` process and records
//! what happened.
//!
//! Closed-loop by construction. The driver holds `concurrency` requests in
//! flight and sends a fixed *number* of requests rather than a fixed arrival
//! rate, because a rate the machine cannot serve turns every measurement into a
//! measurement of the machine. A fixed count is reproducible: the same manifest
//! sends the same requests in the same rotation on any runner, and only the
//! timings differ.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};

use futures::StreamExt;
use serde_json::{Value, json};
use tokio::sync::Mutex;

use super::manifest::{Profile, RESULT_SCHEMA_VERSION, Tier, Workload};
use super::probe::{ResourceReport, Sampler};
use super::result::{
    CapacityResult, Deadlines, Environment, Occupancy, Outcomes, Percentiles, ProfileEcho,
    QueueEvidence, Recovery, RunMeta, Tenancy, TenantCounts, Throughput, Upstream, UsageRecords,
    Verdict,
};
use crate::support::fault::collector::Collector;
use crate::support::gateway::{
    ANTHROPIC_SECONDARY_ENV, Axond, GATEWAY_KEY, OPENAI_SECONDARY_ENV, alias, config_toml,
};
use crate::support::upstream::{CALLER_TAG, Dispatch, FakeUpstream, credential_digest, target};

/// The bounds a capacity run is served under. The admission ceilings sit far
/// above any manifest concurrency, so what is measured is the process rather
/// than its own load shedding — shedding has its own suite
/// (`admission_bounds.rs`), and here a `503` is a finding.
/// Written out rather than defaulted
/// so the recorded config hash pins them: a later change to a shipped default
/// must not silently move a qualification result.
const TUNING: &str = r"
[failover]
max_attempts = 1
overall_timeout_ms = 60000
failure_threshold = 3
cooldown_seconds = 30

[transport]
connect_timeout_ms = 10000
response_header_timeout_ms = 30000
buffered_body_timeout_ms = 30000
stream_idle_timeout_ms = 30000
max_response_bytes = 33554432
max_error_bytes = 65536

[admission]
max_request_bytes = 1048576
max_in_flight = 8192
max_in_flight_streams = 8192
max_in_flight_per_tenant = 0
queue_capacity = 0
queue_wait_ms = 0
max_prompt_tokens = 0
max_output_tokens = 0
max_stream_duration_ms = 0
max_stream_bytes = 0
";

/// The transport bounds a `backend-limits` profile moves down to the value it
/// holds upstreams to. All three, because the profile stalls upstreams before
/// headers and mid-body, and a bound left at the shared default would end the
/// request on a number the profile did not declare.
const UPSTREAM_BOUNDS: [&str; 3] = [
    "response_header_timeout_ms",
    "buffered_body_timeout_ms",
    "stream_idle_timeout_ms",
];

/// `tuning` with the value of `key` replaced.
///
/// A string rewrite that silently finds nothing is worse than no rewrite at
/// all: the profile would boot at the shared default while its artifact
/// recorded the ceiling or bound the manifest asked for, and the run would
/// pass without ever reaching the limit it exists to measure. So a key the
/// tuning does not declare panics rather than no-ops.
pub fn retuned(tuning: &str, key: &str, value: u64) -> String {
    let assignment = format!("{key} = ");
    let declared = tuning
        .lines()
        .find(|line| line.starts_with(&assignment))
        .unwrap_or_else(|| panic!("the capacity tuning declares no `{key}` for a profile to move"));
    tuning.replace(declared, &format!("{key} = {value}"))
}

/// The bounds every capacity profile shares, for a suite checking that the
/// keys a profile moves are keys this declares.
pub fn tuning() -> &'static str {
    TUNING
}

const QUEUE_DEPTH_INSTRUMENT: &str = "axond.admission.queue.depth";
const QUEUE_DEPTH_BOUNDS: [u64; 11] = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024];

/// How far past the bound the replica declares a request may still end before
/// it counts as having outlived it. The bound is the gateway's own promise, so
/// it is asserted — but the measurement includes the driver's own scheduling on
/// a loaded machine, and a tenfold margin fails a broken deadline while a busy
/// runner does not fail a working one.
const DEADLINE_SLACK: u32 = 10;

/// The tenants of a multi-tenant profile. Each has its own inbound key and its
/// own upstream credential, so "served by its own credential" is a property the
/// driver can read off the fake upstream rather than take on trust.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Tenant {
    pub namespace: &'static str,
    inbound_key: &'static str,
    inbound_env: &'static str,
    upstream_key: &'static str,
    upstream_env: &'static str,
    /// This tenant's own aliases. They exist so its requests are distinguishable
    /// from the other tenant's at the upstream: each points at the same fixture
    /// behaviour behind a target tagged with the namespace, which is what lets
    /// the run pair *who asked* with *which credential answered* rather than
    /// compare two totals that a swap leaves balanced.
    chat_alias: &'static str,
    slow_alias: &'static str,
}

const PLATFORM: Tenant = Tenant {
    namespace: "platform",
    inbound_key: GATEWAY_KEY,
    inbound_env: "GW_INBOUND_KEY",
    upstream_key: crate::support::gateway::OPENAI_KEY,
    upstream_env: "GW_FAKE_OPENAI_KEY",
    chat_alias: alias::CHAT,
    slow_alias: alias::CHAT_SLOW,
};

const ACME: Tenant = Tenant {
    namespace: "capacity-acme",
    inbound_key: "test-capacity-acme-key",
    inbound_env: "GW_CAPACITY_ACME_KEY",
    upstream_key: "test-upstream-capacity-acme",
    upstream_env: "GW_CAPACITY_ACME_UPSTREAM",
    chat_alias: "capacity-acme-chat",
    slow_alias: "capacity-acme-chat-slow",
};

const GLOBEX: Tenant = Tenant {
    namespace: "capacity-globex",
    inbound_key: "test-capacity-globex-key",
    inbound_env: "GW_CAPACITY_GLOBEX_KEY",
    upstream_key: "test-upstream-capacity-globex",
    upstream_env: "GW_CAPACITY_GLOBEX_UPSTREAM",
    chat_alias: "capacity-globex-chat",
    slow_alias: "capacity-globex-chat-slow",
};

/// The tenants a multi-tenant run serves, in rotation order.
const TENANTS: [Tenant; 2] = [ACME, GLOBEX];

impl Tenant {
    /// The digest the upstream reports this tenant's own credential under.
    pub fn credential(&self) -> String {
        credential_digest(self.upstream_key)
    }

    /// The alias this tenant reaches for a buffered completion, and the one a
    /// usage row of its should name.
    pub fn chat_alias(&self) -> &'static str {
        self.chat_alias
    }
}

/// How many upstream calls were answered by a credential belonging to someone
/// other than the caller — including one belonging to nobody in the run.
///
/// Pairing is the whole point: a symmetric swap, where each tenant is served
/// with the other's key, leaves every per-credential total exactly where an
/// honest run would leave it.
pub fn crossed_credential_uses(dispatches: &BTreeMap<Dispatch, u64>) -> u64 {
    let owners: BTreeMap<String, &'static str> = TENANTS
        .iter()
        .chain(std::iter::once(&PLATFORM))
        .map(|tenant| (tenant.credential(), tenant.namespace))
        .collect();
    dispatches
        .iter()
        .filter(|(dispatch, _)| {
            // An untagged target is one the platform's own aliases routed.
            let caller = if dispatch.caller.is_empty() {
                PLATFORM.namespace
            } else {
                dispatch.caller.as_str()
            };
            owners.get(&dispatch.credential).copied() != Some(caller)
        })
        .map(|(_, calls)| *calls)
        .sum()
}

/// How many usage rows were filed against a namespace that does not own the
/// alias the row names — including a row naming an alias nobody in the run
/// owns.
///
/// Paired for the same reason the credential count is: each tenant reaches its
/// own aliases, so a row is checkable against the caller it should have been
/// charged to. Two per-namespace totals are not — under a rotation that offers
/// both tenants the same number of requests, a run that filed every ACME charge
/// under GLOBEX and every GLOBEX charge under ACME leaves both totals where an
/// honest run leaves them.
pub fn crossed_usage_records(records: &[Value]) -> u64 {
    let owners: BTreeMap<&'static str, &'static str> = TENANTS
        .iter()
        .chain(std::iter::once(&PLATFORM))
        .flat_map(|tenant| {
            [
                (tenant.chat_alias, tenant.namespace),
                (tenant.slow_alias, tenant.namespace),
            ]
        })
        .collect();
    records
        .iter()
        .filter(|record| {
            let alias = record["model"].as_str().unwrap_or_default();
            owners.get(alias).copied() != record["namespace"].as_str()
        })
        .count() as u64
}

/// The tenants a multi-tenant run serves, for a suite checking what the run
/// recorded about them.
pub fn tenants() -> &'static [Tenant] {
    &TENANTS
}

/// Two namespaces beside the platform one, each holding its own pool. Platform
/// fallback stays off, so a tenant reaching an alias is a tenant reaching it
/// with a credential it owns.
fn tenant_namespaces() -> String {
    TENANTS.iter().fold(String::new(), |mut config, tenant| {
        use std::fmt::Write;
        let _ = write!(
            config,
            r#"
[[namespace]]
id = "{namespace}"
allow_platform_fallback = false

[[credential]]
namespace = "{namespace}"
provider = "fake-openai"
env = "{upstream_env}"
id = "{namespace}-openai"

[[gateway_key]]
env = "{inbound_env}"
namespace = "{namespace}"

[[model]]
name = "{chat_alias}"
targets = [ {{ provider = "fake-openai", model = "{chat_target}{CALLER_TAG}{namespace}", price = {price} }} ]

[[model]]
name = "{slow_alias}"
targets = [ {{ provider = "fake-openai", model = "{slow_target}{CALLER_TAG}{namespace}", price = {price} }} ]
"#,
            namespace = tenant.namespace,
            upstream_env = tenant.upstream_env,
            inbound_env = tenant.inbound_env,
            chat_alias = tenant.chat_alias,
            slow_alias = tenant.slow_alias,
            chat_target = target::CHAT,
            slow_target = target::SLOW_STREAM,
            price = crate::support::gateway::price(),
        );
        config
    })
}

/// A second credential per provider, so the mixed profile exercises pool
/// rotation rather than one key on every dispatch.
fn credential_pool() -> String {
    format!(
        r#"
[[credential]]
namespace = "platform"
provider = "fake-openai"
env = "{OPENAI_SECONDARY_ENV}"
id = "fake-openai-secondary"

[[credential]]
namespace = "platform"
provider = "fake-anthropic"
env = "{ANTHROPIC_SECONDARY_ENV}"
id = "fake-anthropic-secondary"
"#
    )
}

/// How long usage records may trail the last client byte before they are
/// counted as dropped. Settlement is detached from the request, so this is a
/// bound on the *sink*, not on the request path.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(120);

/// How long an upstream body may stay open after every client is gone. Beyond
/// this it is a leak, which is a hard failure.
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(60);

/// One request the driver sends.
#[derive(Clone, Copy)]
struct Shape {
    route: &'static str,
    alias: &'static str,
    stream: bool,
    /// Who sends it, and therefore which inbound key it carries.
    tenant: Tenant,
}

impl Shape {
    const fn buffered(route: &'static str, alias: &'static str) -> Self {
        Self {
            route,
            alias,
            stream: false,
            tenant: PLATFORM,
        }
    }

    const fn streamed(route: &'static str, alias: &'static str) -> Self {
        Self {
            route,
            alias,
            stream: true,
            tenant: PLATFORM,
        }
    }

    const fn sent_by(self, tenant: Tenant) -> Self {
        Self { tenant, ..self }
    }

    fn body(self) -> Value {
        match self.route {
            "/v1/embeddings" => json!({ "model": self.alias, "input": "capacity" }),
            "/v1/responses" => {
                json!({ "model": self.alias, "stream": self.stream, "input": "capacity" })
            }
            "/v1/messages" => json!({
                "model": self.alias,
                "stream": self.stream,
                "max_tokens": 1024,
                "messages": [{ "role": "user", "content": "capacity" }],
            }),
            _ => json!({
                "model": self.alias,
                "stream": self.stream,
                "messages": [{ "role": "user", "content": "capacity" }],
            }),
        }
    }
}

const CHAT: &str = "/v1/chat/completions";
const MESSAGES: &str = "/v1/messages";
const EMBEDDINGS: &str = "/v1/embeddings";
const RESPONSES: &str = "/v1/responses";

/// Both wire families, four routes, both providers, buffered and streamed
/// interleaved. Rotation is by request index, so the mix is identical on every
/// run of the same profile.
const MIXED_ROTATION: [Shape; 6] = [
    Shape::buffered(CHAT, alias::CHAT),
    Shape::buffered(MESSAGES, alias::MESSAGES),
    Shape::buffered(EMBEDDINGS, alias::EMBEDDINGS),
    Shape::buffered(RESPONSES, alias::RESPONSES),
    Shape::streamed(CHAT, alias::CHAT_SLOW),
    Shape::streamed(MESSAGES, alias::MESSAGES_SLOW),
];

const SIZE_ROTATION: [Shape; 3] = [
    Shape::buffered(CHAT, alias::CHAT_SIZED_SMALL),
    Shape::buffered(CHAT, alias::CHAT_SIZED_MEDIUM),
    Shape::buffered(CHAT, alias::CHAT_SIZED_LARGE),
];

/// Two tenants interleaved, each buffered and streamed, each under its own key.
/// Alternating by request index rather than by worker keeps both namespaces in
/// flight at once, which is the condition an isolation claim is about.
const TENANT_ROTATION: [Shape; 4] = [
    Shape::buffered(CHAT, ACME.chat_alias).sent_by(ACME),
    Shape::buffered(CHAT, GLOBEX.chat_alias).sent_by(GLOBEX),
    Shape::streamed(CHAT, ACME.slow_alias).sent_by(ACME),
    Shape::streamed(CHAT, GLOBEX.slow_alias).sent_by(GLOBEX),
];

/// One healthy answer per two unhealthy upstreams: one that never sends
/// response headers, and one that sends them and then stops mid-body. Both are
/// ended by a bound the gateway owns rather than by the upstream relenting, and
/// the healthy third is what shows the replica still serving while they hang.
const BACKEND_LIMIT_ROTATION: [Shape; 3] = [
    Shape::buffered(CHAT, alias::CHAT),
    Shape::buffered(CHAT, alias::CHAT_NO_HEADERS),
    Shape::buffered(CHAT, alias::CHAT_SLOW_BODY),
];

fn shape_for(workload: Workload, index: usize) -> Shape {
    match workload {
        Workload::Buffered => Shape::buffered(CHAT, alias::CHAT),
        // An upstream that thinks before it answers, so an admitted request
        // holds its slot for a known time and the ceiling is reached by the
        // offered concurrency rather than by how slow the runner happened to
        // be.
        Workload::Shedding | Workload::Queueing => Shape::buffered(CHAT, alias::CHAT_LATE_HEADERS),
        Workload::Streaming | Workload::Cancellation => Shape::streamed(CHAT, alias::CHAT_SLOW),
        Workload::Mixed => MIXED_ROTATION[index % MIXED_ROTATION.len()],
        Workload::ResponseSize => SIZE_ROTATION[index % SIZE_ROTATION.len()],
        Workload::Tenants => TENANT_ROTATION[index % TENANT_ROTATION.len()],
        Workload::BackendLimits => BACKEND_LIMIT_ROTATION[index % BACKEND_LIMIT_ROTATION.len()],
    }
}

/// How many of `offered` requests the backend-limits rotation sends to the
/// upstream that answers. A tripped target must not cost a healthy one a single
/// request, so the healthy share is expected exactly rather than approximately.
pub fn offered_to_healthy_backend(offered: u64) -> u64 {
    (0..offered)
        .filter(|index| {
            BACKEND_LIMIT_ROTATION[(index % BACKEND_LIMIT_ROTATION.len() as u64) as usize].alias
                == alias::CHAT
        })
        .count() as u64
}

/// How many of `offered` requests a tenant sends, under the rotation above.
/// Counted the way the driver selects rather than divided out, so an offered
/// count that is not a whole number of rotations is still exact.
pub fn offered_per_tenant(offered: u64, tenant: &Tenant) -> u64 {
    (0..offered)
        .filter(|index| {
            TENANT_ROTATION[(index % TENANT_ROTATION.len() as u64) as usize]
                .tenant
                .namespace
                == tenant.namespace
        })
        .count() as u64
}

/// How one offered request ended.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// Served, and read to the end.
    Accepted,
    /// Served, and the driver hung up deliberately.
    Cancelled,
    /// Shed by admission or a tenant ceiling: `429`/`503`.
    Rejected,
    /// Answered with an error status.
    Failed,
    /// Never answered: the transport gave up.
    TransportFailure,
}

struct Attempt {
    outcome: Outcome,
    /// The namespace that sent it. `platform` unless the profile is
    /// multi-tenant.
    namespace: &'static str,
    status: Option<u16>,
    error_type: Option<String>,
    latency_ms: f64,
    ttft_ms: Option<f64>,
    stream_lifetime_ms: Option<f64>,
}

/// Whether the driver hangs up on the request at `index`, given a cadence. Every
/// index divisible by the cadence over `0..requests`, which is
/// [`expected_cancellations`] of them — one more than integer division when the
/// count is not a multiple of the cadence.
pub fn cancels(index: usize, every: usize) -> bool {
    index.is_multiple_of(every)
}

/// How many of `offered` requests [`cancels`] selects at cadence `every`.
pub fn expected_cancellations(offered: u64, every: usize) -> u64 {
    offered.div_ceil(every as u64)
}

/// Only one profile offers load at a time, whatever the harness runs it from:
/// two tiers driving two gateways on one machine measure each other's
/// contention, and the artifact would still read as an envelope. The libtest
/// `--test-threads=1` in the `capacity` workflow says the same thing; this makes
/// it true rather than configured.
fn load_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(Mutex::default)
}

/// Driver-side gauges. `awaiting_first_byte` is the closest a client can get to
/// the replica's queue occupancy without trusting the replica's own telemetry.
#[derive(Default)]
pub struct Gauges {
    in_flight: AtomicU64,
    in_flight_peak: AtomicU64,
    awaiting: AtomicU64,
    awaiting_peak: AtomicU64,
}

impl Gauges {
    pub fn enter(&self) {
        let now = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        self.in_flight_peak.fetch_max(now, Ordering::Relaxed);
        let waiting = self.awaiting.fetch_add(1, Ordering::Relaxed) + 1;
        self.awaiting_peak.fetch_max(waiting, Ordering::Relaxed);
    }

    /// Called when the first byte of the *answer* arrives: response headers for a
    /// buffered request, the first relayed chunk for a stream. A stream's headers
    /// can precede its first token by hundreds of milliseconds, and the request is
    /// still waiting during that gap.
    pub fn first_byte(&self, waiting: &mut bool) {
        if *waiting {
            *waiting = false;
            self.awaiting.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub fn leave(&self, waiting: &mut bool) {
        self.first_byte(waiting);
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    /// The most requests that waited for a first byte at once.
    pub fn awaiting_peak(&self) -> u64 {
        self.awaiting_peak.load(Ordering::Relaxed)
    }

    /// How many are waiting for a first byte right now.
    pub fn awaiting(&self) -> u64 {
        self.awaiting.load(Ordering::Relaxed)
    }
}

/// The replica a profile is served by: the tuning it boots with, and the
/// environment the credentials it names are read from.
///
/// Every profile writes its bounds out rather than inheriting a shipped
/// default, and the tuning is part of the config the record hashes — so a
/// profile that provokes shedding says at what ceiling, and one that provokes
/// an upstream timeout says at what bound.
fn deployment(profile: &Profile) -> (String, Vec<(String, String)>) {
    match profile.workload {
        Workload::Mixed => (format!("{TUNING}{}", credential_pool()), Vec::new()),
        Workload::Tenants => {
            let env = TENANTS
                .iter()
                .flat_map(|tenant| {
                    [
                        (tenant.inbound_env.to_owned(), tenant.inbound_key.to_owned()),
                        (
                            tenant.upstream_env.to_owned(),
                            tenant.upstream_key.to_owned(),
                        ),
                    ]
                })
                .collect();
            (format!("{TUNING}{}", tenant_namespaces()), env)
        }
        Workload::Shedding => {
            let ceiling = profile
                .max_in_flight
                .expect("a shedding profile declares the ceiling it sheds at");
            (
                retuned(
                    &retuned(TUNING, "max_in_flight", ceiling),
                    "max_in_flight_streams",
                    ceiling,
                ),
                Vec::new(),
            )
        }
        Workload::Queueing => {
            let ceiling = profile
                .max_in_flight
                .expect("a queueing profile declares its request ceiling");
            let capacity = profile
                .queue_capacity
                .expect("a queueing profile declares its queue capacity");
            let wait = profile
                .queue_wait_ms
                .expect("a queueing profile declares its queue wait bound");
            let tuning = retuned(
                &retuned(
                    &retuned(
                        &retuned(TUNING, "max_in_flight", ceiling),
                        "max_in_flight_streams",
                        ceiling,
                    ),
                    "queue_capacity",
                    capacity,
                ),
                "queue_wait_ms",
                wait,
            );
            (tuning, Vec::new())
        }
        Workload::BackendLimits => {
            let bound = profile
                .upstream_timeout_ms
                .expect("a backend-limits profile declares the bound it holds upstreams to");
            (
                UPSTREAM_BOUNDS
                    .iter()
                    .fold(TUNING.to_owned(), |tuning, key| {
                        retuned(&tuning, key, bound)
                    }),
                Vec::new(),
            )
        }
        _ => (TUNING.to_owned(), Vec::new()),
    }
}

/// Run `profile` at `tier` and return its result artifact.
pub async fn run(profile: &Profile, tier: Tier, manifest_text: &str) -> CapacityResult {
    let _offering = load_lock().lock().await;
    let scale = *profile.scale(tier);
    let collector = if profile.workload == Workload::Queueing {
        Some(Collector::start().await)
    } else {
        None
    };
    let (tuning, mut extra_env) = deployment(profile);
    if let Some(collector) = collector.as_ref() {
        extra_env.extend([
            (
                "OTEL_EXPORTER_OTLP_ENDPOINT".to_owned(),
                collector.endpoint.clone(),
            ),
            (
                "OTEL_EXPORTER_OTLP_PROTOCOL".to_owned(),
                "http/protobuf".to_owned(),
            ),
            ("OTEL_METRIC_EXPORT_INTERVAL".to_owned(), "1000".to_owned()),
            ("OTEL_BSP_SCHEDULE_DELAY".to_owned(), "200".to_owned()),
        ]);
    }
    let upstream = FakeUpstream::start().await;
    let upstream_base = upstream.base_url.clone();
    let mut gateway = Axond::start_custom(
        &|addr| config_toml(addr, &upstream_base, &tuning, ""),
        &extra_env,
    )
    .await;
    let bind = gateway
        .base_url
        .strip_prefix("http://")
        .expect("a loopback base URL")
        .to_owned();
    let environment = Environment::collect(
        &gateway.config,
        &bind,
        &upstream.base_url,
        super::manifest::MANIFEST_RELATIVE,
        manifest_text,
    );

    let client = crate::support::client();
    let gauges = Arc::new(Gauges::default());
    let next = Arc::new(AtomicUsize::new(0));
    let sampler = Sampler::start(gateway.pid());
    let started_at = SystemTime::now();
    let started = Instant::now();

    let mut workers = Vec::with_capacity(scale.concurrency);
    for _ in 0..scale.concurrency {
        let (client, gauges, next) = (client.clone(), gauges.clone(), next.clone());
        let base_url = gateway.base_url.clone();
        let workload = profile.workload;
        let cancel_every = profile.cancel_every;
        let cancel_after = profile.cancel_after_output_chunks.unwrap_or(2);
        let total = scale.requests;
        workers.push(tokio::spawn(async move {
            let mut attempts = Vec::new();
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= total {
                    return attempts;
                }
                let shape = shape_for(workload, index);
                let cancel = cancel_every.is_some_and(|every| cancels(index, every));
                attempts.push(
                    attempt(
                        &client,
                        &base_url,
                        shape,
                        cancel.then_some(cancel_after),
                        &gauges,
                    )
                    .await,
                );
            }
        }));
    }

    let mut attempts = Vec::with_capacity(scale.requests);
    for worker in workers {
        attempts.extend(worker.await.expect("a capacity worker does not panic"));
    }
    let elapsed = started.elapsed();
    let resources = sampler.finish(elapsed);

    let accepted = count(&attempts, |a| {
        matches!(a.outcome, Outcome::Accepted | Outcome::Cancelled)
    });
    let failed = count(&attempts, |a| a.outcome == Outcome::Failed);

    // With the load gone, is the replica still serving? A ceiling that keeps a
    // permit, or an upstream bound that keeps a slot, leaves a process that
    // measured well and cannot take the next request — so the profiles that
    // provoke either one ask for the next request.
    let recovery = match profile.thresholds.max_unserved_after_load {
        Some(_) => Some(recovery_probe(&client, &gateway.base_url, &gauges).await),
        None => None,
    };
    let served_probe = u64::from(recovery.as_ref().is_some_and(|probe| probe.served));

    // A request the replica dispatched settles a usage record whatever the
    // upstream then did, so a profile that ends requests on its own bound
    // expects one for those too (ADR 0010). A shed request never reached
    // accounting and expects none.
    let expected_records = accepted + failed_settlements(profile.workload, failed) + served_probe;
    let observed = await_usage_records(&gateway, expected_records).await;
    let leaked = await_closed_upstreams(&upstream).await;
    let queue = if let Some(collector) = collector.as_ref() {
        gateway.terminate();
        let exit = gateway.await_exit(Duration::from_secs(30)).await;
        assert!(
            exit.is_some_and(|status| status.success()),
            "the queueing replica did not flush telemetry and exit cleanly: {exit:?}"
        );
        gateway.settle_output(Duration::from_secs(2)).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        Some(queue_evidence(collector))
    } else {
        None
    };
    let tenancy = (profile.workload == Workload::Tenants).then(|| {
        let dispatches = upstream.state.dispatches();
        // An upstream call the tally never saw is one the isolation counts
        // cannot speak for, and its absence reads as a cleaner run rather than
        // an unmeasured one.
        assert_eq!(
            dispatches.values().sum::<u64>(),
            upstream.state.received(),
            "{}: the upstream answered calls the dispatch tally did not record",
            profile.id
        );
        tenancy(&attempts, &observed, &dispatches)
    });
    let deadlines = profile.upstream_timeout_ms.map(|bound| Deadlines {
        bound_ms: bound,
        slack_multiple: DEADLINE_SLACK,
        over_bound: count(&attempts, |a| {
            a.latency_ms > (bound * u64::from(DEADLINE_SLACK)) as f64
        }),
        max_latency_ms: attempts
            .iter()
            .map(|a| a.latency_ms)
            .fold(0.0_f64, f64::max),
    });

    let latency: Vec<f64> = attempts.iter().map(|a| a.latency_ms).collect();
    let ttft: Vec<f64> = attempts.iter().filter_map(|a| a.ttft_ms).collect();
    let lifetime: Vec<f64> = attempts
        .iter()
        .filter_map(|a| a.stream_lifetime_ms)
        .collect();
    let offered = attempts.len() as u64;
    let rejected = count(&attempts, |a| a.outcome == Outcome::Rejected);
    let transport_failures = count(&attempts, |a| a.outcome == Outcome::TransportFailure);
    let errors = failed + transport_failures;
    let seconds = elapsed.as_secs_f64().max(f64::EPSILON);

    let result = CapacityResult {
        schema_version: RESULT_SCHEMA_VERSION,
        profile: ProfileEcho::new(profile, tier),
        run: RunMeta::new(started_at, elapsed),
        environment,
        throughput: Throughput {
            offered,
            accepted,
            rejected,
            errors,
            elapsed_ms: elapsed.as_millis(),
            offered_rps: offered as f64 / seconds,
            accepted_rps: accepted as f64 / seconds,
            closed_loop: true,
        },
        latency_ms: Percentiles::of(&latency).expect("a profile offers at least one request"),
        ttft_ms: Percentiles::of(&ttft),
        stream_lifetime_ms: Percentiles::of(&lifetime),
        resources,
        occupancy: Occupancy {
            offered_concurrency: scale.concurrency,
            in_flight_peak: gauges.in_flight_peak.load(Ordering::Relaxed),
            awaiting_first_byte_peak: gauges.awaiting_peak.load(Ordering::Relaxed),
            admission_queue_capacity: profile.queue_capacity.unwrap_or(0),
            admission_max_in_flight: profile.max_in_flight,
        },
        queue,
        outcomes: Outcomes {
            by_status: tally(attempts.iter().filter_map(|a| a.status.map(u64::from))),
            rejections_by_error_type: error_types(&attempts, Outcome::Rejected),
            errors_by_error_type: error_types(&attempts, Outcome::Failed),
            client_cancelled: count(&attempts, |a| a.outcome == Outcome::Cancelled),
            transport_failures,
        },
        usage_records: UsageRecords {
            expected: expected_records,
            observed: observed.len() as u64,
            missing: expected_records.saturating_sub(observed.len() as u64),
            by_status: tally(
                observed
                    .iter()
                    .map(|record| record["status"].as_str().unwrap_or("unknown").to_owned()),
            ),
        },
        upstream: Upstream {
            requests: upstream.state.received(),
            streams_opened: upstream.state.opened_streams(),
            streams_open_at_end: leaked,
        },
        tenancy,
        deadlines,
        recovery,
        verdicts: Vec::new(),
    };
    let verdicts = verdicts(&result);
    CapacityResult { verdicts, ..result }
}

fn queue_evidence(collector: &Collector) -> QueueEvidence {
    let points = collector
        .histogram_points(QUEUE_DEPTH_INSTRUMENT)
        .unwrap_or_else(|error| panic!("cannot decode queue depth evidence: {error}"));
    let latest = points
        .iter()
        .max_by_key(|point| point.time_unix_nano)
        .expect("the queueing profile exports a queue-depth histogram point");
    let explicit_bounds: Vec<u64> = latest
        .explicit_bounds
        .iter()
        .map(|value| exact_u64(*value).expect("queue histogram bounds are exact integers"))
        .collect();
    let min_depth = latest.min.and_then(exact_u64);
    let max_depth = latest.max.and_then(exact_u64);
    let exact = explicit_bounds == QUEUE_DEPTH_BOUNDS
        && latest.attributes == 0
        && latest.bucket_counts.iter().sum::<u64>() == latest.count
        && min_depth.is_some()
        && max_depth.is_some();
    QueueEvidence {
        instrument: QUEUE_DEPTH_INSTRUMENT,
        points_exported: points.len(),
        observations: latest.count,
        min_depth,
        max_depth,
        explicit_bounds,
        bucket_counts: latest.bucket_counts.clone(),
        attributes: latest.attributes,
        exact,
    }
}

fn exact_u64(value: f64) -> Option<u64> {
    (value.is_finite() && value >= 0.0 && value <= u64::MAX as f64 && value.fract() == 0.0)
        .then_some(value as u64)
}

/// Whether a request the replica dispatched and then ended on its own bound
/// still settles a usage record. It does — that is what makes a timed-out
/// upstream a charge rather than a hole in the ledger — but only the profile
/// that provokes one expects any, and everywhere else a failure is already a
/// threshold breach rather than an expected settlement.
fn failed_settlements(workload: Workload, failed: u64) -> u64 {
    match workload {
        Workload::BackendLimits => failed,
        _ => 0,
    }
}

/// One request offered after the load stops, under the platform key.
async fn recovery_probe(client: &reqwest::Client, base_url: &str, gauges: &Gauges) -> Recovery {
    let probe = attempt(
        client,
        base_url,
        Shape::buffered(CHAT, alias::CHAT),
        None,
        gauges,
    )
    .await;
    Recovery {
        served: probe.outcome == Outcome::Accepted,
        status: probe.status,
        latency_ms: probe.latency_ms,
    }
}

/// Who was served, who was charged, and whose credential paid for it.
///
/// The credential count is paired rather than totalled: the upstream reports
/// each request as *who asked* and *what answered for them*, and a use is
/// foreign when those two disagree. Two per-tenant totals cannot say this —
/// under a rotation that offers both tenants the same number of requests, two
/// customers served with each other's key leave both totals balanced, which is
/// the one failure the profile exists to catch.
///
/// The usage count is paired the same way, against the alias each row names,
/// since a tenant reaches only its own aliases. Beside it, a namespace charged
/// for *more* requests than it sent is counted too — a double charge names an
/// alias its owner does own — while *fewer* is not: that is a request which
/// failed before accounting, already a threshold breach of its own, and
/// reporting an ordinary upstream failure as a leak between customers would
/// spend the credibility this measurement exists to have.
fn tenancy(
    attempts: &[Attempt],
    records: &[Value],
    dispatches: &BTreeMap<Dispatch, u64>,
) -> Tenancy {
    let mut by_namespace = BTreeMap::new();
    let mut misattributed_usage_records = crossed_usage_records(records);
    let foreign_credential_uses = crossed_credential_uses(dispatches);
    for tenant in &TENANTS {
        let accepted = attempts
            .iter()
            .filter(|a| {
                a.namespace == tenant.namespace
                    && matches!(a.outcome, Outcome::Accepted | Outcome::Cancelled)
            })
            .count() as u64;
        // Everything the replica took on: a shed request never reaches an
        // upstream or accounting, and everything else may.
        let dispatched = attempts
            .iter()
            .filter(|a| a.namespace == tenant.namespace && a.outcome != Outcome::Rejected)
            .count() as u64;
        let usage_records = records
            .iter()
            .filter(|record| record["namespace"].as_str() == Some(tenant.namespace))
            .count() as u64;
        // The upstream reports a digest, never the material, so a tally can be
        // retained: this is how often this tenant's own key answered anyone.
        let own = tenant.credential();
        let upstream_calls: u64 = dispatches
            .iter()
            .filter(|(dispatch, _)| dispatch.credential == own)
            .map(|(_, calls)| *calls)
            .sum();
        misattributed_usage_records += usage_records.saturating_sub(dispatched);
        by_namespace.insert(
            tenant.namespace.to_owned(),
            TenantCounts {
                offered: attempts
                    .iter()
                    .filter(|a| a.namespace == tenant.namespace)
                    .count() as u64,
                accepted,
                dispatched,
                rejected: attempts
                    .iter()
                    .filter(|a| a.namespace == tenant.namespace && a.outcome == Outcome::Rejected)
                    .count() as u64,
                usage_records,
                upstream_calls,
            },
        );
    }
    Tenancy {
        by_namespace,
        foreign_credential_uses,
        misattributed_usage_records,
    }
}

/// The hard failures, evaluated against the manifest's thresholds. Every one is
/// a property the gateway either has or does not have, independently of how fast
/// the runner was.
fn verdicts(result: &CapacityResult) -> Vec<Verdict> {
    let thresholds = &result.profile.thresholds;
    let offered = result.throughput.offered.max(1) as f64;
    let mut verdicts = vec![
        Verdict::at_most(
            "max_missing_usage_records",
            result.usage_records.missing as f64,
            thresholds.max_missing_usage_records as f64,
        ),
        Verdict::at_most(
            "max_leaked_upstream_streams",
            result.upstream.streams_open_at_end as f64,
            thresholds.max_leaked_upstream_streams as f64,
        ),
    ];
    verdicts.extend(memory_verdict(
        &result.resources,
        thresholds.max_rss_growth_kib,
    ));
    verdicts.extend(
        [
            thresholds.min_accepted.map(|floor| {
                Verdict::at_least(
                    "min_accepted",
                    result.throughput.accepted as f64,
                    floor as f64,
                )
            }),
            thresholds.min_accepted_fraction.map(|floor| {
                Verdict::at_least(
                    "min_accepted_fraction",
                    result.throughput.accepted as f64 / offered,
                    floor,
                )
            }),
        ]
        .into_iter()
        .flatten(),
    );
    verdicts.extend(
        [
            thresholds
                .max_rejections
                .map(|bound| ("max_rejections", Some(result.throughput.rejected), bound)),
            thresholds
                .max_errors
                .map(|bound| ("max_errors", Some(result.throughput.errors), bound)),
            thresholds.max_untyped_errors.map(|bound| {
                (
                    "max_untyped_errors",
                    Some(untyped_errors(&result.outcomes)),
                    bound,
                )
            }),
            thresholds.max_over_deadline.map(|bound| {
                (
                    "max_over_deadline",
                    result
                        .deadlines
                        .as_ref()
                        .map(|deadlines| deadlines.over_bound),
                    bound,
                )
            }),
            thresholds.max_foreign_credential_uses.map(|bound| {
                (
                    "max_foreign_credential_uses",
                    result
                        .tenancy
                        .as_ref()
                        .map(|tenancy| tenancy.foreign_credential_uses),
                    bound,
                )
            }),
            thresholds.max_misattributed_usage_records.map(|bound| {
                (
                    "max_misattributed_usage_records",
                    result
                        .tenancy
                        .as_ref()
                        .map(|tenancy| tenancy.misattributed_usage_records),
                    bound,
                )
            }),
            thresholds.max_unserved_after_load.map(|bound| {
                (
                    "max_unserved_after_load",
                    result
                        .recovery
                        .as_ref()
                        .map(|probe| u64::from(!probe.served)),
                    bound,
                )
            }),
        ]
        .into_iter()
        .flatten()
        .map(|(name, measured, bound)| measured_verdict(name, measured, bound)),
    );
    verdicts.extend(
        [
            thresholds.max_rejected_fraction.map(|bound| {
                Verdict::at_most(
                    "max_rejected_fraction",
                    result.throughput.rejected as f64 / offered,
                    bound,
                )
            }),
            thresholds.min_rejected_fraction.map(|bound| {
                Verdict::at_least(
                    "min_rejected_fraction",
                    result.throughput.rejected as f64 / offered,
                    bound,
                )
            }),
            thresholds.max_error_fraction.map(|bound| {
                Verdict::at_most(
                    "max_error_fraction",
                    result.throughput.errors as f64 / offered,
                    bound,
                )
            }),
        ]
        .into_iter()
        .flatten(),
    );
    if result.profile.queue_capacity.is_some() {
        let evidence = result.queue.as_ref();
        verdicts.push(Verdict::at_least(
            "queue_telemetry_exact",
            f64::from(u8::from(evidence.is_some_and(|queue| queue.exact))),
            1.0,
        ));
        verdicts.push(Verdict::at_least(
            "queue_observations",
            evidence.map_or(0, |queue| queue.observations) as f64,
            1.0,
        ));
    }
    if let Some(bound) = thresholds.min_queue_depth {
        verdicts.push(Verdict::at_least(
            "min_queue_depth",
            result
                .queue
                .as_ref()
                .and_then(|queue| queue.max_depth)
                .map_or(f64::NEG_INFINITY, |depth| depth as f64),
            bound as f64,
        ));
    }
    if let Some(bound) = thresholds.max_queue_depth {
        verdicts.push(Verdict::at_most(
            "max_queue_depth",
            result
                .queue
                .as_ref()
                .and_then(|queue| queue.max_depth)
                .map_or(f64::INFINITY, |depth| depth as f64),
            bound as f64,
        ));
    }
    verdicts
}

/// A threshold against a measurement the run may not have taken.
///
/// A threshold whose measurement is absent must not read as a threshold that
/// was met: a profile declaring `max_foreign_credential_uses` while producing
/// no tenancy block measured no isolation, and a verdict of zero would retain
/// the claim anyway. The absence is recorded as its own failed verdict, the
/// same way a lost resource sample is.
pub fn measured_verdict(threshold: &str, measured: Option<u64>, bound: u64) -> Verdict {
    match measured {
        Some(value) => Verdict::at_most(threshold, value as f64, bound as f64),
        None => Verdict::at_most(&format!("{threshold}_unmeasured"), 1.0, 0.0),
    }
}

/// Failures that carry no typed body: an error the gateway answered without
/// one, and a request that ended at the transport with no answer at all. The
/// second is the more serious of the two — a caller that got a reset cannot be
/// told why — so a profile bounding untyped failures bounds both.
pub fn untyped_errors(outcomes: &Outcomes) -> u64 {
    outcomes
        .errors_by_error_type
        .get("untyped")
        .copied()
        .unwrap_or_default()
        + outcomes.transport_failures
}

/// What a run's resource sampling is worth as a gate.
///
/// Off a `/proc` platform there is no memory evidence, so there is nothing to
/// assert rather than a threshold that would pass vacuously. On a `/proc` host an
/// absent measurement is a different thing: the sampler lost its subject — the
/// process exited, or its `/proc` entries could not be read — and a run that
/// cannot say what memory did must not read like one that measured and passed.
/// A run whose sampler never got a turn is the same case: the baseline it seeded
/// the peak from is not a measurement of the load.
pub fn memory_verdict(resources: &ResourceReport, max_growth_kib: u64) -> Option<Verdict> {
    if resources.procfs && resources.samples == 0 {
        return Some(Verdict::at_most("resource_sampling", 1.0, 0.0));
    }
    match resources.rss_kib {
        Some(rss) => Some(Verdict::at_most(
            "max_rss_growth_kib",
            rss.growth() as f64,
            max_growth_kib as f64,
        )),
        None if resources.procfs => Some(Verdict::at_most("resource_sampling", 1.0, 0.0)),
        None => None,
    }
}

/// Offer one request and measure it.
async fn attempt(
    client: &reqwest::Client,
    base_url: &str,
    shape: Shape,
    cancel_after_output_chunks: Option<usize>,
    gauges: &Gauges,
) -> Attempt {
    let started = Instant::now();
    gauges.enter();
    // Cleared by the answer's first byte, and unconditionally when the attempt
    // ends, so an attempt that never gets one cannot pin the gauge.
    let waiting = &mut true;
    let sent = client
        .post(format!("{base_url}{}", shape.route))
        .bearer_auth(shape.tenant.inbound_key)
        .json(&shape.body())
        .send()
        .await;
    let response = match sent {
        Ok(response) => response,
        Err(_) => {
            gauges.leave(waiting);
            return Attempt {
                outcome: Outcome::TransportFailure,
                namespace: shape.tenant.namespace,
                status: None,
                error_type: None,
                latency_ms: millis(started.elapsed()),
                ttft_ms: None,
                stream_lifetime_ms: None,
            };
        }
    };
    let status = response.status();
    let headers_at = started.elapsed();

    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        gauges.leave(waiting);
        let error_type = serde_json::from_str::<Value>(&body)
            .ok()
            .and_then(|value| value.get("error")?.get("type")?.as_str().map(str::to_owned));
        return Attempt {
            outcome: if status.as_u16() == 429 || status.as_u16() == 503 {
                Outcome::Rejected
            } else {
                Outcome::Failed
            },
            namespace: shape.tenant.namespace,
            status: Some(status.as_u16()),
            error_type,
            latency_ms: millis(started.elapsed()),
            ttft_ms: None,
            stream_lifetime_ms: None,
        };
    }

    if !shape.stream {
        // A buffered answer's headers *are* its first bytes: the gateway has the
        // whole body before it answers at all.
        gauges.first_byte(waiting);
        let read = response.bytes().await;
        gauges.leave(waiting);
        return Attempt {
            outcome: if read.is_ok() {
                Outcome::Accepted
            } else {
                Outcome::TransportFailure
            },
            namespace: shape.tenant.namespace,
            status: Some(status.as_u16()),
            error_type: None,
            latency_ms: millis(started.elapsed()),
            ttft_ms: None,
            stream_lifetime_ms: None,
        };
    }

    // A stream: time to first token is the first relayed byte, and its lifetime
    // is what happens after that.
    let mut stream = response.bytes_stream();
    let mut first_byte: Option<Duration> = None;
    // Relayed output *events*, not transport chunks: the two do not map
    // one-to-one, and a starved reader can take several events in one read. The
    // text is accumulated because a marker can also straddle a chunk boundary.
    let mut relayed = String::new();
    let mut cancelled = false;
    let mut torn = false;
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else {
            torn = true;
            break;
        };
        first_byte.get_or_insert_with(|| started.elapsed());
        gauges.first_byte(waiting);
        let Some(after) = cancel_after_output_chunks else {
            continue;
        };
        relayed.push_str(&String::from_utf8_lossy(&chunk));
        // Only relayed output makes a cancellation charge real, so the hang-up
        // waits for it rather than for the stream's preamble.
        if output_events(&relayed) >= after {
            cancelled = true;
            break;
        }
    }
    // Dropping the body without draining it is what a closed browser tab looks
    // like, and it is the case that leaks an upstream when it is mishandled.
    drop(stream);
    gauges.leave(waiting);
    let total = started.elapsed();
    Attempt {
        outcome: match (cancelled, torn) {
            (true, _) => Outcome::Cancelled,
            (false, true) => Outcome::TransportFailure,
            (false, false) => Outcome::Accepted,
        },
        namespace: shape.tenant.namespace,
        status: Some(status.as_u16()),
        error_type: None,
        latency_ms: millis(total),
        ttft_ms: Some(millis(first_byte.unwrap_or(headers_at))),
        stream_lifetime_ms: Some(millis(
            total.saturating_sub(first_byte.unwrap_or(headers_at)),
        )),
    }
}

/// Wait for `expected` usage records, returning whatever arrived. A shortfall is
/// reported as a drop rather than panicked on: the artifact is the evidence, and
/// the threshold is what fails the run.
async fn await_usage_records(gateway: &Axond, expected: u64) -> Vec<Value> {
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    loop {
        let records = gateway.usage_records();
        if records.len() as u64 >= expected || Instant::now() >= deadline {
            return records;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Upstream response bodies still open once every client is gone.
async fn await_closed_upstreams(upstream: &FakeUpstream) -> i64 {
    let deadline = Instant::now() + CLEANUP_TIMEOUT;
    loop {
        let open = upstream.state.open_streams();
        if open == 0 || Instant::now() >= deadline {
            return open;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Relayed model-output events in a stream read so far, over both wire families.
///
/// Framed per SSE event rather than counted as substrings: a stream's preamble
/// mentions `content` without relaying any (`message_start` carries an empty
/// content array), and an Anthropic delta names its type in both the event line
/// and the payload, so substring counting would both over- and under-state the
/// output. Only complete events count — a trailing partial one is counted when
/// the rest of it arrives, which is why the driver accumulates the text.
pub fn output_events(relayed: &str) -> usize {
    relayed
        .split_inclusive("\n\n")
        .filter(|event| event.ends_with("\n\n") && relays_output(event))
        .count()
}

/// Whether one SSE event carries model output, over either wire family.
fn relays_output(event: &str) -> bool {
    let named_delta = event
        .lines()
        .filter_map(|line| line.strip_prefix("event:"))
        .any(|name| name.trim() == "content_block_delta");
    if named_delta {
        return true;
    }
    event
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .filter_map(|payload| serde_json::from_str::<Value>(payload.trim()).ok())
        .any(|payload| {
            let kind = payload["type"].as_str().unwrap_or_default();
            // Anthropic without an event line, and the Responses wire.
            if kind == "content_block_delta" || kind == "response.output_text.delta" {
                return true;
            }
            // A chat chunk relays output when a choice's delta holds text.
            payload["choices"].as_array().is_some_and(|choices| {
                choices.iter().any(|choice| {
                    choice["delta"]["content"]
                        .as_str()
                        .is_some_and(|text| !text.is_empty())
                })
            })
        })
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn count(attempts: &[Attempt], predicate: impl Fn(&Attempt) -> bool) -> u64 {
    attempts.iter().filter(|a| predicate(a)).count() as u64
}

fn error_types(attempts: &[Attempt], outcome: Outcome) -> BTreeMap<String, u64> {
    tally(
        attempts
            .iter()
            .filter(|a| a.outcome == outcome)
            .map(|a| a.error_type.clone().unwrap_or_else(|| "untyped".to_owned())),
    )
}

fn tally<T: ToString>(values: impl Iterator<Item = T>) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for value in values {
        *counts.entry(value.to_string()).or_default() += 1;
    }
    counts
}
