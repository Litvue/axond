//! Declarative configuration — the source of truth for the gateway.
//!
//! A TOML file owns all *structure* (providers, models, namespaces, quota,
//! sinks); the environment owns *secrets* (referenced by name, never inlined)
//! and may override scalars for containerized deploys. This mirrors the design
//! note in the assessment (§5, delta B4): config is a public API, so it is
//! validated as a whole at boot (delta B2) rather than coping with invalid
//! entries at request time.
//!
//! The same load + validate path serves hot reload (ADR 0011): a reload builds a
//! candidate through [`Config::load`], so a reloaded config passes exactly the
//! gate a booting one does. The environment is read at *reload* time, so a
//! credential env-var added after boot resolves without a restart.

#[cfg(test)]
use std::collections::HashSet;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::time::Duration;

use gateway_core::ModelPrice;
use gateway_transport::TransportLimits;
use serde::{Deserialize, Deserializer, Serialize};

use crate::admission::MAX_PERMITS;
use crate::aliases::AliasScope;
use crate::backends::catalog_refresh::{Bootstrap, RefreshSchedule};
use crate::backends::catalog_store::postgres::CatalogStoreSettings;
use crate::backoff::BackoffPolicy;
use crate::usage::journal::{Capacity, CapacityPolicy, ConsumerId};
use crate::usage::{BatchSettings, validate_table_name};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: Server,
    /// Required durable store (ADR 0063). SQLite WAL or Postgres.
    #[serde(default)]
    pub storage: Option<StorageConfig>,
    #[serde(default)]
    pub namespace: Vec<Namespace>,
    #[serde(default)]
    pub provider: Vec<Provider>,
    /// Deployment price-book. First matching `(provider, model-id glob)` in
    /// file order wins; this is not a routing table.
    #[serde(default)]
    pub price: Vec<PriceRule>,
    /// Deployment-wide model-id glob denials. Unioned with the namespace list
    /// at request time.
    #[serde(default)]
    pub blocklist: BlocklistConfig,
    #[serde(default)]
    pub credential: Vec<Credential>,
    /// Pool-wide policy for `(namespace, provider)` pairs that bind more than
    /// one credential: how a credential is picked and when a bad one is parked.
    #[serde(default)]
    pub credential_pool: CredentialPool,
    /// Ordered failover across an alias's targets and per-target circuit health.
    #[serde(default)]
    pub failover: Failover,
    /// Per-phase bounds on every upstream call: connecting, waiting for headers,
    /// reading a buffered body, and waiting for the next chunk of an open stream.
    #[serde(default)]
    pub transport: Transport,
    /// How termination is sequenced: how long readiness fails before admission
    /// closes, how long admitted requests then have, and the flush bound.
    #[serde(default)]
    pub shutdown: Shutdown,
    /// Inbound gateway keys. Each binds a secret (resolved from `env` or
    /// `file`) to a namespace. At least one is required: inbound authentication
    /// fails closed, so there is no keyless mode (ADR 0013).
    #[serde(default)]
    pub gateway_key: Vec<GatewayKey>,
    /// Where raw usage records go. Empty means the no-datastore default: one
    /// JSON line per record on stdout (ADR 0002).
    #[serde(default)]
    pub usage_sink: Vec<UsageSinkConfig>,
    /// Durable, replayed usage delivery. Defaults to `backend = "none"`: the
    /// telemetry-grade path stays exactly as it is and no datastore joins the
    /// default deployment (ADR 0002, ADR 0049).
    #[serde(default)]
    pub usage_journal: UsageJournalConfig,
    /// Bounds on what one request may consume, plus the global and per-tenant
    /// admission ceilings that shed load before it reaches a provider.
    #[serde(default)]
    pub admission: AdmissionConfig,
    /// The upstream catalogue this deployment imports provider and model
    /// metadata from, and how often. Defaults to `backend = "none"`: nothing is
    /// fetched, and an operator's own resources are the whole catalogue
    /// (ADR 0043, ADR 0051).
    #[serde(default)]
    pub catalog: CatalogConfig,
    /// Background refresh of each provider's upstream `GET /models` listing.
    /// Default interval is five minutes; first round runs at boot. Not on the
    /// inference path.
    #[serde(default)]
    pub discovery: DiscoveryConfig,
    /// Top-level keys no field claims. Kept only so a section ADR 0063
    /// withdrew fails boot by name instead of being silently ignored.
    #[serde(flatten, default)]
    unrecognized: BTreeMap<String, figment::value::Value>,
}

/// Top-level sections ADR 0063 withdrew. Each once changed enforcement or
/// authentication, so ignoring one would quietly drop a control an operator
/// configured.
const WITHDRAWN_SECTIONS: [&str; 16] = [
    "admin_breakglass",
    "admin_oidc",
    "budget",
    "control_plane",
    "convergence",
    "core_middleware",
    "gateway_minting",
    "gateway_token",
    "gateway_token_epoch",
    "gateway_verifier",
    "mode",
    "model",
    "rate_limit",
    "reload",
    "revocation",
    "secret_store",
];

/// The top-level keys the `AXOND_` environment layer can address, since
/// [`Config::load`] merges `Env::prefixed("AXOND_")` over the file.
///
/// A *secret-bearing* variable must not be named after one of them: the
/// override layer would merge its value as that key instead of leaving it for a
/// reference to resolve, and figment's resulting type error would carry the
/// secret into the load diagnostic. Kept in step with `Config` by
/// `the_override_key_list_matches_every_config_field`.
#[cfg(test)]
const OVERRIDE_KEYS: [&str; 17] = [
    "server",
    "storage",
    "namespace",
    "provider",
    "price",
    "blocklist",
    "credential",
    "credential_pool",
    "failover",
    "transport",
    "shutdown",
    "gateway_key",
    "usage_sink",
    "usage_journal",
    "admission",
    "catalog",
    "discovery",
];

fn parse_glob(pattern: &str) -> Result<AliasScope, String> {
    AliasScope::parse(std::iter::once(pattern)).map_err(|_| pattern.to_owned())
}

pub(crate) fn validate_glob_pattern(pattern: &str) -> Result<(), String> {
    parse_glob(pattern).map(|_| ()).map_err(|pattern| {
        format!(
            "blocklist glob `{pattern}` is invalid: use an exact id, `prefix*`, `*suffix`, or `*`"
        )
    })
}

fn glob_permits(pattern: &str, value: &str) -> bool {
    parse_glob(pattern).is_ok_and(|scope| scope.permits(value))
}

/// A `SET search_path` argument, validated rather than forwarded.
///
/// Schema names reach SQL *text* — there is no parameter form of `SET` — so every
/// configured one is an identifier this build checks at boot. The table-name
/// validator allows one qualifying dot, which a search path cannot use, so that
/// grammar is refused here rather than left as a gap on a value that reaches a
/// statement.
fn validate_schema_name(key: &str, schema: &str) -> Result<(), ConfigError> {
    crate::usage::validate_table_name(schema)
        .map_err(|message| ConfigError::Invalid(format!("`{key}`: {message}")))?;
    if schema.contains('.') {
        return Err(ConfigError::Invalid(format!(
            "`{key}` must be a single unqualified schema name: it names the search path, not a \
             table"
        )));
    }
    Ok(())
}

/// A reference is only a reference if the environment layer leaves it alone.
#[cfg(test)]
fn reject_env_override_collision(key: &str, name: &str) -> Result<(), ConfigError> {
    let Some(field) = name.strip_prefix("AXOND_") else {
        return Ok(());
    };
    let field = field.to_ascii_lowercase();
    if !OVERRIDE_KEYS.contains(&field.as_str()) {
        return Ok(());
    }
    Err(ConfigError::Invalid(format!(
        "`{key}` names the env var `{name}`, which the `AXOND_` override layer reads as the \
         `{field}` config key rather than as a reference: exporting it would fail config load and \
         put its value in the error. Name the variable outside the `AXOND_<section>` shape — the \
         examples use `GW_`"
    )))
}

/// Background provider-model listing. Off the inference path.
#[derive(Debug, Clone, Deserialize)]
pub struct DiscoveryConfig {
    #[serde(default = "default_discovery_refresh_interval_seconds")]
    pub refresh_interval_seconds: u64,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            refresh_interval_seconds: default_discovery_refresh_interval_seconds(),
        }
    }
}

fn default_discovery_refresh_interval_seconds() -> u64 {
    300
}

impl DiscoveryConfig {
    pub fn interval(&self) -> Duration {
        Duration::from_secs(self.refresh_interval_seconds)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Server {
    #[serde(default = "default_bind")]
    pub bind: SocketAddr,
}

impl Default for Server {
    fn default() -> Self {
        Self {
            bind: default_bind(),
        }
    }
}

fn default_bind() -> SocketAddr {
    "0.0.0.0:8080".parse().expect("static bind addr")
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageBackend {
    #[default]
    Sqlite,
    Postgres,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct StorageConfig {
    #[serde(default)]
    pub backend: StorageBackend,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub dsn_env: Option<String>,
    #[serde(default)]
    pub on_unavailable: StoreUnavailable,
    /// Apply namespace DDL at connect. SQLite always needs this; Postgres
    /// deployments that migrate out of band set `false`.
    #[serde(default = "default_storage_create_table")]
    pub create_table: bool,
    /// Batching for the management usage index (`axond_store_usage`).
    #[serde(default)]
    pub usage_index: UsageIndexConfig,
}

fn default_storage_create_table() -> bool {
    true
}

/// How usage events reach the Store's management index (`GET .../usage`).
///
/// The index is best effort and off the request path: the request `try_reserve`s
/// a slot on a bounded queue and one worker writes what has queued in bounded
/// batches, so a Store outage costs index rows (counted on
/// `axond.usage.index.appends`) rather than latency. Same vocabulary as the
/// `[[usage_sink]]` batching keys.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct UsageIndexConfig {
    /// Events queued ahead of the worker before the request path drops rather
    /// than waits.
    #[serde(default = "default_usage_index_buffer_capacity")]
    pub buffer_capacity: usize,
    /// Rows per Store transaction. Bounds how long SQLite's single connection
    /// is held away from admits and charges.
    #[serde(default = "default_usage_index_max_batch")]
    pub max_batch: usize,
    /// How long a partial batch waits for company before it is written anyway.
    /// `0` writes whatever has queued as soon as the worker is free.
    #[serde(default = "default_usage_index_flush_interval_ms")]
    pub flush_interval_ms: u64,
}

fn default_usage_index_buffer_capacity() -> usize {
    1024
}

fn default_usage_index_max_batch() -> usize {
    256
}

fn default_usage_index_flush_interval_ms() -> u64 {
    50
}

/// Tokio's bounded channel panics above this; the queue is a semaphore.
fn max_usage_index_buffer_capacity() -> usize {
    tokio::sync::Semaphore::MAX_PERMITS
}

/// Ceiling on `[storage.usage_index] flush_interval_ms`. Larger values cannot
/// form a worker deadline (`Instant + Duration` overflows) and would stall the
/// only index writer for longer than a batch is worth.
pub(crate) const MAX_USAGE_INDEX_FLUSH_INTERVAL_MS: u64 = 86_400_000;

impl Default for UsageIndexConfig {
    fn default() -> Self {
        Self {
            buffer_capacity: default_usage_index_buffer_capacity(),
            max_batch: default_usage_index_max_batch(),
            flush_interval_ms: default_usage_index_flush_interval_ms(),
        }
    }
}

impl UsageIndexConfig {
    pub fn settings(&self) -> crate::usage::UsageIndexSettings {
        crate::usage::UsageIndexSettings {
            capacity: self.buffer_capacity,
            max_batch: self.max_batch,
            flush_interval: Duration::from_millis(self.flush_interval_ms),
            ..crate::usage::UsageIndexSettings::default()
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Namespace {
    pub id: String,
    /// The namespace used when a request carries no identity (dev) or when a
    /// gateway key does not name one. Exactly one namespace must set this.
    #[serde(default)]
    pub default: bool,
    /// When a namespace lacks its own credential for a provider, may it borrow
    /// the platform namespace's key? Defaults to `false`, so "bring your own
    /// key" means exactly that (assessment §5.1, delta A/B).
    #[serde(default)]
    pub allow_platform_fallback: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Provider {
    pub id: String,
    pub kind: ProviderKind,
    pub base_url: String,
    /// Unpriced model ids: `deny` refuses before dispatch; `allow` dispatches
    /// and records `cost_microdollars` as NULL. "Unpriced" means neither the
    /// imported models.dev snapshot nor a `[[price]]` row covers the id.
    #[serde(default)]
    pub unpriced_models: UnpricedModels,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UnpricedModels {
    #[default]
    Deny,
    Allow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderKind {
    Openai,
    Anthropic,
    OpenaiCompatible,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderWire {
    Openai,
    Anthropic,
}

impl std::fmt::Display for ProviderWire {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Openai => f.write_str("OpenAI"),
            Self::Anthropic => f.write_str("Anthropic"),
        }
    }
}

impl ProviderKind {
    pub const fn wire(self) -> ProviderWire {
        match self {
            Self::Openai | Self::OpenaiCompatible => ProviderWire::Openai,
            Self::Anthropic => ProviderWire::Anthropic,
        }
    }
}

/// The one observed `(provider, model)` a request routes to. Alias failover is
/// gone; credential-pool rotation still walks this single target.
#[derive(Debug, Clone)]
pub struct Model {
    pub targets: Vec<Target>,
}

impl Model {
    pub(crate) fn single(provider: String, model: String) -> Self {
        Self {
            targets: vec![Target { provider, model }],
        }
    }
}

/// Deployment default blocklist. Namespaced extras are store-backed.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct BlocklistConfig {
    #[serde(default)]
    pub models: Vec<String>,
}

/// One price-book row: exact provider id, glob against the bare upstream id.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct PriceRule {
    pub provider: String,
    pub model: String,
    #[serde(flatten)]
    pub price: ModelPrice,
}

#[derive(Debug, Clone)]
pub struct Target {
    pub provider: String,
    /// Concrete upstream model / deployment id.
    pub model: String,
}

/// Explicit (namespace, provider) → env-var binding. Declared, never inferred
/// from a mangled namespace id (assessment delta A/§5.1).
///
/// Several entries may share a `(namespace, provider)` pair; together they form
/// that pair's credential pool (ADR 0006).
#[derive(Debug, Clone, Deserialize)]
pub struct Credential {
    pub namespace: String,
    pub provider: String,
    /// The env var the material is read from. Present for every credential a
    /// *file* declares, and absent for one a revision projected — whose material
    /// comes from the secret store by reference instead.
    #[serde(default)]
    pub env: Option<String>,
    /// Stable label for attribution. Defaults to the env-var *name*, which is a
    /// reference rather than a secret, so it is safe to log and to carry on a
    /// usage record.
    #[serde(default)]
    pub id: Option<String>,
    /// Relative share of pool traffic under the `weighted` strategy. Ignored by
    /// `round-robin`.
    #[serde(default = "default_weight")]
    pub weight: u32,
}

impl Credential {
    /// The attribution label for this credential — never its value.
    ///
    /// A projected credential always carries an `id` (its resource slug), and a
    /// declared one always carries an `env`; validation refuses a credential with
    /// neither, so the fallback is unreachable rather than a silent default.
    pub fn label(&self) -> &str {
        self.id
            .as_deref()
            .or(self.env.as_deref())
            .unwrap_or("unlabelled")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CredentialPool {
    #[serde(default)]
    pub strategy: SelectionStrategy,
    /// Consecutive credential-scoped failures (rate limit / quota) that park a
    /// single credential. The pool's other credentials keep serving.
    #[serde(default = "default_credential_failure_threshold")]
    pub failure_threshold: u32,
    /// How long a parked credential waits before a half-open probe.
    #[serde(default = "default_credential_cooldown_seconds")]
    pub cooldown_seconds: u64,
}

impl Default for CredentialPool {
    fn default() -> Self {
        Self {
            strategy: SelectionStrategy::default(),
            failure_threshold: default_credential_failure_threshold(),
            cooldown_seconds: default_credential_cooldown_seconds(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SelectionStrategy {
    #[default]
    RoundRobin,
    Weighted,
}

/// Ordered failover across an alias's `targets`, plus the per-target circuit
/// breaker. This is the *outer* loop around credential-pool dispatch: a target
/// is skipped while its circuit is open, and a retryable upstream failure
/// advances to the next target. The bounds cap how much failover can amplify a
/// request's latency (ADR 0008).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Failover {
    /// Upper bound on upstream target attempts for one request. The retry count
    /// a request can add is `max_attempts - 1`, so this caps latency
    /// amplification even for an alias with many targets.
    #[serde(default = "default_failover_max_attempts")]
    pub max_attempts: u32,
    /// Overall wall-clock budget for the whole failover walk, in milliseconds.
    /// No further target is attempted once it is spent.
    #[serde(default = "default_failover_overall_timeout_ms")]
    pub overall_timeout_ms: u64,
    /// Consecutive target-scoped failures that trip a target's circuit. Distinct
    /// from `credential_pool.failure_threshold`, which parks a single credential.
    #[serde(default = "default_target_failure_threshold")]
    pub failure_threshold: u32,
    /// How long a tripped target circuit waits before a half-open probe.
    #[serde(default = "default_target_cooldown_seconds")]
    pub cooldown_seconds: u64,
}

impl Default for Failover {
    fn default() -> Self {
        Self {
            max_attempts: default_failover_max_attempts(),
            overall_timeout_ms: default_failover_overall_timeout_ms(),
            failure_threshold: default_target_failure_threshold(),
            cooldown_seconds: default_target_cooldown_seconds(),
        }
    }
}

/// Bounds on one upstream call, per phase (ADR 0008's walk budget is the outer
/// bound; these are the inner ones).
///
/// `failover.overall_timeout_ms` stays authoritative for everything before a
/// response is being usefully consumed — connecting, waiting for headers,
/// reading a buffered body, and rotating credentials — and the tighter of it and
/// the phase bound below governs each phase. `stream_idle_timeout_ms` applies
/// after a stream opens, because a long answer is not a stalled one: only
/// silence between chunks is. After a byte-faithful semantic terminal event,
/// `stream_terminal_grace_ms` becomes the fixed close bound instead.
///
/// The defaults are therefore deliberately not tighter than the walk budget for
/// the two bounds that cover *producing* an answer: a non-streamed provider call
/// sends nothing until the completion exists, so a header bound below the walk
/// budget would cut off slow completions the walk still had time for. Whichever
/// bound ends a wait, the stalled phase is still named, so a target that goes
/// silent is attributed to the target rather than to the gateway's budget.
///
/// These are process-level (they configure the shared HTTP client), so a change
/// is validated on reload but takes effect on restart.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Transport {
    /// Bound on establishing the TCP + TLS connection to a provider.
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Bound on waiting for a provider's response headers (time to first byte).
    /// For a non-streamed call this covers the whole completion, since the
    /// provider sends no headers before it is finished.
    #[serde(default = "default_response_header_timeout_ms")]
    pub response_header_timeout_ms: u64,
    /// Bound on reading a whole buffered response body once headers arrived.
    #[serde(default = "default_buffered_body_timeout_ms")]
    pub buffered_body_timeout_ms: u64,
    /// Bound on waiting for the next chunk of an already-open stream. Not a
    /// total stream lifetime: it resets on every chunk.
    #[serde(default = "default_stream_idle_timeout_ms")]
    pub stream_idle_timeout_ms: u64,
    /// How long a byte-faithful stream may keep its HTTP body open after its
    /// semantic terminal event. The grace preserves trailing provider
    /// extension bytes without retaining request capacity for the general
    /// stream-idle bound.
    #[serde(default = "default_stream_terminal_grace_ms")]
    pub stream_terminal_grace_ms: u64,
    /// Largest buffered response body that will be read. A larger one is
    /// refused rather than buffered.
    #[serde(default = "default_max_response_bytes")]
    pub max_response_bytes: u64,
    /// Largest provider *error* body that will be read; the remainder is
    /// discarded, since an error body is diagnostic rather than the answer.
    #[serde(default = "default_max_error_bytes")]
    pub max_error_bytes: u64,
}

impl Default for Transport {
    fn default() -> Self {
        Self {
            connect_timeout_ms: default_connect_timeout_ms(),
            response_header_timeout_ms: default_response_header_timeout_ms(),
            buffered_body_timeout_ms: default_buffered_body_timeout_ms(),
            stream_idle_timeout_ms: default_stream_idle_timeout_ms(),
            stream_terminal_grace_ms: default_stream_terminal_grace_ms(),
            max_response_bytes: default_max_response_bytes(),
            max_error_bytes: default_max_error_bytes(),
        }
    }
}

impl Transport {
    /// The transport's own view of these bounds.
    pub fn limits(&self) -> TransportLimits {
        TransportLimits {
            connect_timeout: Duration::from_millis(self.connect_timeout_ms),
            response_header_timeout: Duration::from_millis(self.response_header_timeout_ms),
            buffered_body_timeout: Duration::from_millis(self.buffered_body_timeout_ms),
            stream_idle_timeout: Duration::from_millis(self.stream_idle_timeout_ms),
            max_response_bytes: self.max_response_bytes,
            max_error_bytes: self.max_error_bytes,
        }
    }
}

fn default_connect_timeout_ms() -> u64 {
    5_000
}

/// Generous by design, and for the same reason as the idle bound: for a
/// *non-streamed* call the provider sends no headers until the whole completion
/// exists, so this bound is the model's thinking time, not a liveness signal.
/// The walk's `failover.overall_timeout_ms` is what keeps it finite in practice.
fn default_response_header_timeout_ms() -> u64 {
    30_000
}

fn default_buffered_body_timeout_ms() -> u64 {
    30_000
}

/// Generous by design: a reasoning model can think for a long time between
/// tokens, and cutting that off looks like a gateway bug to a caller.
fn default_stream_idle_timeout_ms() -> u64 {
    120_000
}

/// Long enough for a proxy/provider to flush extension bytes already behind a
/// semantic terminal event, but intentionally far below the ordinary idle
/// allowance because the answer itself is complete.
fn default_stream_terminal_grace_ms() -> u64 {
    1_000
}

fn default_max_response_bytes() -> u64 {
    32 * 1024 * 1024
}

/// Inbound resource bounds and load shedding (see [`crate::admission`]).
///
/// These are the *inbound* half of the bounds `[transport]` sets on upstream
/// calls: how large a request may be, how many may be in flight at once for the
/// process and for one tenant, how long one may wait for capacity, and how long
/// a stream may stay open. Every ceiling is explicit here rather than inherited
/// from a library default, and `0` means "this ceiling is off" — never
/// "unbounded by accident".
///
/// The ceilings are process-level: they own semaphores built at boot, so a
/// change is validated on reload but takes effect on restart, exactly like
/// `[transport]`.
///
/// The two sub-ceilings default *below* the global one, so lowering only
/// `max_in_flight` would leave a stock 256-request tenant ceiling above a
/// 16-request process. A ceiling the operator did not write is therefore clamped
/// to `max_in_flight` on load rather than refused; one they did write is a boot
/// error, because a contradiction between two configured numbers has no obvious
/// resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionConfig {
    /// Largest inbound request body that will be buffered and parsed. A larger
    /// one is refused with `413` before it is read into memory, which is what
    /// bounds the prompt a caller can send.
    pub max_request_bytes: usize,
    /// Concurrent requests this replica will serve on the provider-dispatching
    /// routes. `0` disables the ceiling.
    pub max_in_flight: usize,
    /// Concurrent open streams, counted separately because a stream holds a
    /// socket for as long as the model talks. `0` disables the ceiling.
    pub max_in_flight_streams: usize,
    #[doc(hidden)]
    pub max_in_flight_streams_explicit: bool,
    /// Concurrent requests one namespace may hold. Keep it below
    /// `max_in_flight` so no single tenant can take the whole replica. `0`
    /// disables the ceiling.
    ///
    /// In a deployment with one namespace this, not `max_in_flight`, is the
    /// ceiling traffic meets — and it answers `429`, which reads as the caller's
    /// fault. Raise it to `max_in_flight`, or disable it, when one namespace is
    /// the whole deployment.
    pub max_in_flight_per_tenant: usize,
    #[doc(hidden)]
    pub max_in_flight_per_tenant_explicit: bool,
    /// Tenants tracked concurrently by the per-tenant ceiling. Entries exist
    /// only while a tenant has work in flight; a new tenant beyond this bound is
    /// refused rather than admitted without a ceiling.
    pub max_tenants: usize,
    /// Requests that may wait for global capacity. `0` — the default — rejects
    /// immediately instead of queueing, which is the bounded behavior.
    pub queue_capacity: usize,
    /// How long a queued request waits before it is shed. Required with, and
    /// only meaningful with, `queue_capacity`.
    pub queue_wait_ms: u64,
    /// Total lifetime of one open stream, as opposed to
    /// `transport.stream_idle_timeout_ms`, which resets on every chunk. This is
    /// what bounds a socket held open by an endless answer. `0` disables it.
    ///
    /// Evaluated as the relay is polled, so it bounds a stream the caller is
    /// draining: a client that stops reading applies write backpressure, the
    /// relay stops being polled, and the deadline cannot fire. A proxy's
    /// write/response timeout is the bound for that case.
    pub max_stream_duration_ms: u64,
    /// Largest prompt, in the gateway's pre-dispatch token estimate, that a
    /// request may carry. Bounds the input a caller can send more meaningfully
    /// than `max_request_bytes` alone, which cannot tell a large body from a
    /// large prompt. `0` disables it.
    pub max_prompt_tokens: u64,
    /// Largest output allowance a request may *ask* for (`max_tokens` and its
    /// per-surface spellings). A request asking for more is refused rather than
    /// silently clamped, so a caller is never billed for a bound it did not
    /// choose. `0` disables it.
    pub max_output_tokens: u64,
    /// Bytes one stream may relay before it is ended. Bounds the output of a
    /// model that never stops talking, which neither the idle timeout nor the
    /// token allowance can (a provider need not honor `max_tokens`). `0`
    /// disables it.
    pub max_stream_bytes: u64,
    /// Requests whose spend this replica is still carrying toward the Store:
    /// admitted and not yet settled, settlements queued for an execution slot,
    /// and settlements executing. Reserved at admission (no ledger write) and
    /// released when the settlement finishes, so a slow Store pushes back on
    /// new admissions with `503 settlement_capacity_exhausted` rather than
    /// accumulating detached work. Defaults to four times `max_in_flight`,
    /// so the ceiling only binds when settlement falls behind serving. `0`
    /// disables the ceiling.
    pub max_pending_settlements: usize,
    #[doc(hidden)]
    pub max_pending_settlements_explicit: bool,
    /// Settlements executing against the Store at once. Bounds the charge
    /// concurrency one replica presents to the ledger. `0` disables it.
    pub max_in_flight_settlements: usize,
    /// How long a settlement waits for one of those execution slots before it
    /// is abandoned — counted in `axond.settlement.failures`, never retried.
    /// `0` waits without bound (shutdown still bounds it).
    pub settlement_queue_wait_ms: u64,
    /// How long one settlement may run once it has a slot: the charge plus the
    /// usage append. One that overruns is abandoned and counted, never retried,
    /// because a budget charge is not idempotent. `0` disables it.
    pub settlement_timeout_ms: u64,
}

/// The `[admission]` section as written, before an unset sub-ceiling is clamped
/// to a lowered `max_in_flight`.
#[derive(Debug, Deserialize)]
struct AdmissionConfigWire {
    #[serde(default = "default_max_request_bytes")]
    max_request_bytes: usize,
    #[serde(default = "default_max_in_flight")]
    max_in_flight: usize,
    #[serde(default)]
    max_in_flight_streams: Option<usize>,
    #[serde(default)]
    max_in_flight_per_tenant: Option<usize>,
    #[serde(default = "default_max_tenants")]
    max_tenants: usize,
    #[serde(default = "default_queue_capacity")]
    queue_capacity: usize,
    #[serde(default = "default_queue_wait_ms")]
    queue_wait_ms: u64,
    #[serde(default = "default_max_stream_duration_ms")]
    max_stream_duration_ms: u64,
    #[serde(default = "default_max_prompt_tokens")]
    max_prompt_tokens: u64,
    #[serde(default = "default_max_output_tokens")]
    max_output_tokens: u64,
    #[serde(default = "default_max_stream_bytes")]
    max_stream_bytes: u64,
    #[serde(default)]
    max_pending_settlements: Option<usize>,
    #[serde(default = "default_max_in_flight_settlements")]
    max_in_flight_settlements: usize,
    #[serde(default = "default_settlement_queue_wait_ms")]
    settlement_queue_wait_ms: u64,
    #[serde(default = "default_settlement_timeout_ms")]
    settlement_timeout_ms: u64,
}

impl<'de> Deserialize<'de> for AdmissionConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = AdmissionConfigWire::deserialize(deserializer)?;
        // A defaulted sub-ceiling follows a lowered global one instead of
        // contradicting it; a written one is left alone so validation can refuse
        // it by name.
        let clamp = |written: Option<usize>, default: usize| match written {
            Some(value) => (value, true),
            None if wire.max_in_flight > 0 => (default.min(wire.max_in_flight), false),
            None => (default, false),
        };
        let (max_in_flight_streams, max_in_flight_streams_explicit) =
            clamp(wire.max_in_flight_streams, default_max_in_flight_streams());
        // A tenant ceiling *equal* to the global one isolates nothing, and it
        // would shed at the same point with the wrong verdict: the tenant gate
        // never queues and answers `429`. So a defaulted ceiling that reaches the
        // global one is turned off instead of clamped to it, leaving the global
        // gate — which queues and answers `503` — as the operative bound.
        let (max_in_flight_per_tenant, max_in_flight_per_tenant_explicit) =
            match wire.max_in_flight_per_tenant {
                Some(value) => (value, true),
                None if wire.max_in_flight > 0
                    && default_max_in_flight_per_tenant() >= wire.max_in_flight =>
                {
                    (0, false)
                }
                None => (default_max_in_flight_per_tenant(), false),
            };
        // Every admitted request reserves one settlement, so a defaulted
        // settlement ceiling follows the global one rather than sitting below a
        // raised `max_in_flight` and refusing requests the operator sized for.
        let (max_pending_settlements, max_pending_settlements_explicit) =
            match wire.max_pending_settlements {
                Some(value) => (value, true),
                None => (default_max_pending_settlements(wire.max_in_flight), false),
            };
        Ok(Self {
            max_request_bytes: wire.max_request_bytes,
            max_in_flight: wire.max_in_flight,
            max_in_flight_streams,
            max_in_flight_streams_explicit,
            max_in_flight_per_tenant,
            max_in_flight_per_tenant_explicit,
            max_tenants: wire.max_tenants,
            queue_capacity: wire.queue_capacity,
            queue_wait_ms: wire.queue_wait_ms,
            max_stream_duration_ms: wire.max_stream_duration_ms,
            max_prompt_tokens: wire.max_prompt_tokens,
            max_output_tokens: wire.max_output_tokens,
            max_stream_bytes: wire.max_stream_bytes,
            max_pending_settlements,
            max_pending_settlements_explicit,
            max_in_flight_settlements: wire.max_in_flight_settlements,
            settlement_queue_wait_ms: wire.settlement_queue_wait_ms,
            settlement_timeout_ms: wire.settlement_timeout_ms,
        })
    }
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        Self {
            max_request_bytes: default_max_request_bytes(),
            max_in_flight: default_max_in_flight(),
            max_in_flight_streams: default_max_in_flight_streams(),
            max_in_flight_streams_explicit: false,
            max_in_flight_per_tenant: default_max_in_flight_per_tenant(),
            max_in_flight_per_tenant_explicit: false,
            max_tenants: default_max_tenants(),
            queue_capacity: default_queue_capacity(),
            queue_wait_ms: default_queue_wait_ms(),
            max_stream_duration_ms: default_max_stream_duration_ms(),
            max_prompt_tokens: default_max_prompt_tokens(),
            max_output_tokens: default_max_output_tokens(),
            max_stream_bytes: default_max_stream_bytes(),
            max_pending_settlements: default_max_pending_settlements(default_max_in_flight()),
            max_pending_settlements_explicit: false,
            max_in_flight_settlements: default_max_in_flight_settlements(),
            settlement_queue_wait_ms: default_settlement_queue_wait_ms(),
            settlement_timeout_ms: default_settlement_timeout_ms(),
        }
    }
}

/// Two mebibytes: large enough for a long conversation with inlined context,
/// small enough that a burst of oversized requests cannot exhaust memory.
fn default_max_request_bytes() -> usize {
    2 * 1024 * 1024
}

fn default_max_in_flight() -> usize {
    1_024
}

fn default_max_in_flight_streams() -> usize {
    512
}

/// A quarter of the global ceiling, so four saturated tenants are needed to
/// fill the replica and a fifth still gets served.
fn default_max_in_flight_per_tenant() -> usize {
    256
}

fn default_max_tenants() -> usize {
    1_024
}

/// Four settlements per admitted request: room for the Store to fall three
/// requests' worth behind serving before admission pushes back. Tied to the
/// global ceiling so that raising `max_in_flight` alone never leaves a lower
/// settlement ceiling to shed at; with the global ceiling off, the shipped
/// default's four times.
fn default_max_pending_settlements(max_in_flight: usize) -> usize {
    let base = if max_in_flight > 0 {
        max_in_flight
    } else {
        default_max_in_flight()
    };
    base.saturating_mul(4).min(MAX_PERMITS)
}

/// Charges the ledger sees from one replica at once. Well above what a Store
/// connection pool serves in parallel, so it bounds a stampede without
/// throttling normal settlement.
fn default_max_in_flight_settlements() -> usize {
    64
}

/// Long enough to ride out a Store hiccup, short enough that a charge is
/// abandoned — and counted — rather than held indefinitely behind an outage.
fn default_settlement_queue_wait_ms() -> u64 {
    10_000
}

/// Above the Store's own operation timeouts, so a settlement is only ever cut
/// here after its writes have already been refused or timed out by the Store.
fn default_settlement_timeout_ms() -> u64 {
    10_000
}

/// Immediate rejection by default: a queue that is not tuned for a deployment's
/// traffic converts saturation into latency the caller cannot see.
fn default_queue_capacity() -> usize {
    0
}

fn default_queue_wait_ms() -> u64 {
    0
}

/// An hour. Long enough for any legitimate completion, short enough that a
/// forgotten stream cannot hold a socket for the process's lifetime.
fn default_max_stream_duration_ms() -> u64 {
    60 * 60 * 1_000
}

/// Roughly the largest context the current frontier models accept, so the bound
/// refuses what no provider would serve rather than second-guessing a model.
///
/// It is deliberately above what [`default_max_request_bytes`] admits: the
/// estimate is four bytes per token, so with the default body ceiling the body
/// bound refuses first, at roughly 525k estimated tokens. An operator who wants
/// a prompt-shaped refusal lowers this below `max_request_bytes / 4`; one who
/// raises the body ceiling gets this one back.
fn default_max_prompt_tokens() -> u64 {
    1_000_000
}

fn default_max_output_tokens() -> u64 {
    200_000
}

/// Sixty-four mebibytes of relayed bytes: orders of magnitude above a real
/// completion, and still a bound.
fn default_max_stream_bytes() -> u64 {
    64 * 1024 * 1024
}

fn default_max_error_bytes() -> u64 {
    64 * 1024
}

/// Graceful shutdown bounds. Every value is a *bound*, not a target: the
/// process moves on as soon as the work it is waiting for is done, and the sum
/// of the three is the worst case an orchestrator's termination grace period
/// has to cover.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Shutdown {
    /// How long `/readyz` fails while the replica keeps admitting work, giving
    /// the load balancer time to observe the drain. `0` closes admission as soon
    /// as the signal arrives, which is only safe behind a `preStop` hook that
    /// already waited.
    #[serde(default = "default_shutdown_drain_grace_ms")]
    pub drain_grace_ms: u64,
    /// How long requests admitted before the drain have to finish once
    /// admission is closed. Whatever is still open at the deadline is dropped.
    #[serde(default = "default_shutdown_deadline_ms")]
    pub deadline_ms: u64,
    /// The bound on the whole post-serving sequence: settling the responses the
    /// deadline ended, flushing the buffered usage sinks, and flushing the
    /// telemetry exporters. `terminationGracePeriodSeconds` must exceed
    /// `drain_grace_ms + deadline_ms + flush_timeout_ms`.
    #[serde(default = "default_shutdown_flush_timeout_ms")]
    pub flush_timeout_ms: u64,
}

impl Default for Shutdown {
    fn default() -> Self {
        Self {
            drain_grace_ms: default_shutdown_drain_grace_ms(),
            deadline_ms: default_shutdown_deadline_ms(),
            flush_timeout_ms: default_shutdown_flush_timeout_ms(),
        }
    }
}

/// Two readiness probe periods at the shipped manifest's 5s interval: long
/// enough for a load balancer to stop routing, short enough that a rollout is
/// not perceptibly slower.
fn default_shutdown_drain_grace_ms() -> u64 {
    5_000
}

/// Leaves headroom under the shipped `terminationGracePeriodSeconds = 30`
/// for the drain window and the flush that follows.
fn default_shutdown_deadline_ms() -> u64 {
    15_000
}

fn default_shutdown_flush_timeout_ms() -> u64 {
    5_000
}

fn default_weight() -> u32 {
    1
}

fn default_credential_failure_threshold() -> u32 {
    2
}

fn default_credential_cooldown_seconds() -> u64 {
    30
}

fn default_failover_max_attempts() -> u32 {
    3
}

fn default_failover_overall_timeout_ms() -> u64 {
    30_000
}

fn default_target_failure_threshold() -> u32 {
    3
}

fn default_target_cooldown_seconds() -> u64 {
    30
}

/// One usage destination. `kind` decides which of the remaining fields apply;
/// they are validated as a set at boot, so a Postgres sink without a DSN (or a
/// batch size the wire protocol cannot carry) refuses to start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageSinkConfig {
    pub kind: UsageSinkKind,
    /// `postgres`: name of the env var holding the connection string. The DSN is
    /// a secret, so it is referenced rather than inlined, like every credential.
    pub dsn_env: Option<String>,
    /// `postgres`: destination table. Defaults to `axond_usage`, matching the
    /// shipped DDL.
    pub table: Option<String>,
    /// `postgres`: apply the shipped DDL at boot. Off by default — most
    /// deployments give the gateway's role no DDL rights.
    pub create_table: bool,
    /// Records buffered before the fan-out starts dropping (`postgres`).
    pub buffer_capacity: usize,
    /// Rows per write (`postgres`).
    pub max_batch: usize,
    #[doc(hidden)]
    pub max_batch_explicit: bool,
    /// How long a partial batch waits before it is written anyway (`postgres`).
    pub flush_interval_ms: u64,
}

#[derive(Debug, Deserialize)]
struct UsageSinkConfigWire {
    kind: UsageSinkKind,
    #[serde(default)]
    dsn_env: Option<String>,
    #[serde(default)]
    table: Option<String>,
    #[serde(default)]
    create_table: bool,
    #[serde(default = "default_buffer_capacity")]
    buffer_capacity: usize,
    #[serde(default)]
    max_batch: Option<usize>,
    #[serde(default = "default_flush_interval_ms")]
    flush_interval_ms: u64,
}

impl<'de> Deserialize<'de> for UsageSinkConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = UsageSinkConfigWire::deserialize(deserializer)?;
        Ok(Self {
            kind: wire.kind,
            dsn_env: wire.dsn_env,
            table: wire.table,
            create_table: wire.create_table,
            buffer_capacity: wire.buffer_capacity,
            max_batch: wire.max_batch.unwrap_or_else(default_max_batch),
            max_batch_explicit: wire.max_batch.is_some(),
            flush_interval_ms: wire.flush_interval_ms,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UsageSinkKind {
    Stdout,
    Postgres,
    Otlp,
}

impl UsageSinkKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Postgres => "postgres",
            Self::Otlp => "otlp",
        }
    }
}

impl Default for UsageSinkConfig {
    fn default() -> Self {
        Self {
            kind: UsageSinkKind::Stdout,
            dsn_env: None,
            table: None,
            create_table: false,
            buffer_capacity: default_buffer_capacity(),
            max_batch: default_max_batch(),
            max_batch_explicit: false,
            flush_interval_ms: default_flush_interval_ms(),
        }
    }
}

impl UsageSinkConfig {
    pub fn table(&self) -> String {
        self.table
            .clone()
            .unwrap_or_else(|| DEFAULT_USAGE_TABLE.to_owned())
    }

    pub fn batch_settings(&self) -> BatchSettings {
        BatchSettings {
            capacity: self.buffer_capacity,
            max_batch: self.max_batch.min(self.buffer_capacity),
            flush_interval: Duration::from_millis(self.flush_interval_ms),
        }
    }
}

const DEFAULT_USAGE_TABLE: &str = "axond_usage";

/// Billing-grade usage delivery: durable append before the request is answered,
/// replayed until the destinations acknowledge it (ADR 0049).
///
/// Off by default, and off in every configuration written so far, because the
/// guarantee costs a datastore on the request path. Turning it on is the operator
/// saying that a missing usage row is a missing invoice line.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct UsageJournalConfig {
    pub backend: UsageJournalBackend,
    /// Name of the env var holding the outbox connection string. Required for
    /// `postgres`; the DSN is a secret, so it is referenced rather than inlined.
    pub dsn_env: Option<String>,
    /// The schema the outbox tables live in, if not the connection's default. A
    /// plain unqualified identifier: it is interpolated into `SET search_path`.
    pub schema: Option<String>,
    /// Apply the shipped outbox DDL at boot. Off by default, like every other
    /// store here.
    pub create_schema: bool,
    /// The consumer name delivery state is kept under. Stable across restarts:
    /// renaming it starts delivery again from the beginning of the retained
    /// outbox, which is a replay of everything still there.
    pub consumer: String,
    /// Events the outbox holds before `capacity_policy` applies.
    pub max_events: u64,
    /// Attempts one event gets before it is quarantined as poison.
    pub max_delivery_attempts: u32,
    /// How long an acknowledged event is retained, measured from when the
    /// request was observed rather than from its acknowledgement: the horizon
    /// it has to cover is the caller's retry horizon, and that starts at the
    /// request. Must exceed the longest retry horizon a caller can have,
    /// because pruning forgets the idempotency key.
    pub retain_acknowledged_seconds: u64,
    /// What a full outbox does. `refuse` is the only policy that keeps the
    /// billing-grade promise.
    pub capacity_policy: UsageCapacityPolicy,
    /// What a request does when its event could not be journaled.
    pub on_undurable: UndurablePolicy,
    /// Bound on the append a request waits for, and on every other outbox
    /// operation.
    pub operation_timeout_ms: u64,
    pub connect_timeout_ms: u64,
    /// Connections the outbox holds open. One is reserved for the delivery
    /// worker's claims, so the rest bound how many appends a replica can have in
    /// flight: a claim that waits on a slow destination cannot hold a connection
    /// a request needs.
    pub connections: usize,
    /// Events one claim takes.
    pub claim_batch: usize,
    /// How long a claimed batch stays invisible to other claimants. Must exceed
    /// the slowest write the destinations do.
    pub lease_seconds: u64,
    /// How long the worker waits after finding nothing to deliver.
    pub poll_interval_ms: u64,
}

impl Default for UsageJournalConfig {
    fn default() -> Self {
        Self {
            backend: UsageJournalBackend::None,
            dsn_env: None,
            schema: None,
            create_schema: false,
            consumer: DEFAULT_USAGE_CONSUMER.to_owned(),
            max_events: Capacity::BILLING_GRADE.max_events,
            max_delivery_attempts: Capacity::BILLING_GRADE.max_delivery_attempts,
            retain_acknowledged_seconds: Capacity::BILLING_GRADE.retain_acknowledged.as_secs(),
            capacity_policy: UsageCapacityPolicy::Refuse,
            on_undurable: UndurablePolicy::Refuse,
            operation_timeout_ms: 5_000,
            connect_timeout_ms: 5_000,
            connections: 8,
            claim_batch: 256,
            lease_seconds: 30,
            poll_interval_ms: 250,
        }
    }
}

const DEFAULT_USAGE_CONSUMER: &str = "billing";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UsageJournalBackend {
    /// No journal: telemetry-grade delivery, exactly as before.
    #[default]
    None,
    /// A durable outbox in PostgreSQL (`ops/postgres/usage_outbox_v1.sql`).
    Postgres,
}

impl UsageJournalBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Postgres => "postgres",
        }
    }

    pub fn is_enabled(self) -> bool {
        matches!(self, Self::Postgres)
    }
}

/// The TOML spelling of [`CapacityPolicy`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UsageCapacityPolicy {
    #[default]
    Refuse,
    DropOldest,
}

impl UsageCapacityPolicy {
    pub fn policy(self) -> CapacityPolicy {
        match self {
            Self::Refuse => CapacityPolicy::Refuse,
            Self::DropOldest => CapacityPolicy::DropOldest,
        }
    }
}

/// What a request does when the journal could not make its event durable.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UndurablePolicy {
    /// Answer `503 usage_not_durable`. The default, and the only setting under
    /// which a bill cannot silently miss a line: the caller learns the request
    /// was not recorded and can retry it.
    #[default]
    Refuse,
    /// Answer the request anyway and count the event as lost. Telemetry-grade
    /// behaviour for the failure case, chosen deliberately.
    Serve,
}

impl UndurablePolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Refuse => "refuse",
            Self::Serve => "serve",
        }
    }

    pub fn refuses(self) -> bool {
        matches!(self, Self::Refuse)
    }
}

impl UsageJournalConfig {
    /// The bounds the journal reports about itself.
    pub fn capacity(&self) -> Capacity {
        Capacity {
            max_events: self.max_events,
            max_delivery_attempts: self.max_delivery_attempts,
            retain_acknowledged: Duration::from_secs(self.retain_acknowledged_seconds),
            policy: self.capacity_policy.policy(),
        }
    }
}

fn default_buffer_capacity() -> usize {
    10_000
}

fn default_max_batch() -> usize {
    500
}

fn default_flush_interval_ms() -> u64 {
    1_000
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StoreUnavailable {
    #[default]
    Deny,
    Allow,
}

/// Where imported provider and model metadata comes from, where the imports are
/// kept, and how often one is attempted (ADR 0043, ADR 0051).
///
/// Every field is process-local: a catalogue import is *ingestion*, not a
/// durable resource an operator declares, so this section is read in both modes
/// rather than being surrendered to the control plane in a stateful one. What it
/// produces — immutable snapshots and an active pointer — is durable, and
/// `store` is what decides whether they survive a restart.
///
/// The default is inert. Nothing is fetched, nothing is stored, and no task is
/// spawned, so a deployment that has not enabled `[catalog]` does not contact
/// models.dev. Enabled, admitted snapshots are the default charging source.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CatalogConfig {
    /// Which upstream is imported. `none` disables the whole pipeline.
    #[serde(default)]
    pub source: CatalogSourceBackend,
    /// Where imported snapshots are retained. `in-memory` is a single-replica
    /// development convenience and is refused in a stateful deployment, which
    /// must not lose its catalogue history to a restart.
    #[serde(default)]
    pub store: CatalogStoreBackend,
    /// The models.dev document to fetch. Only `/catalog.json` is supported: the
    /// other published documents have different shapes.
    #[serde(default)]
    pub source_url: Option<String>,
    /// The env var holding the retention DSN. Defaults to the control plane's,
    /// so a stateful deployment does not name the same database twice.
    #[serde(default)]
    pub dsn_env: Option<String>,
    /// The schema retention tables live in.
    #[serde(default)]
    pub schema: Option<String>,
    /// Whether retention creates its tables if they are absent.
    #[serde(default = "default_catalog_create_table")]
    pub create_table: bool,
    /// How long between scheduled refresh attempts.
    #[serde(default = "default_catalog_refresh_interval_seconds")]
    pub refresh_interval_seconds: u64,
    /// The bound on one attempt: the conditional fetch *and* its retention.
    #[serde(default = "default_catalog_refresh_timeout_seconds")]
    pub refresh_timeout_seconds: u64,
    /// The first delay after a refusal, doubled per consecutive refusal.
    #[serde(default = "default_catalog_retry_initial_seconds")]
    pub retry_initial_seconds: u64,
    /// The ceiling that doubling converges to.
    #[serde(default = "default_catalog_retry_max_seconds")]
    pub retry_max_seconds: u64,
    /// What an empty store starts from: nothing, or the bundled seed, which lets
    /// a deployment with no egress serve a known catalogue.
    #[serde(default)]
    pub bootstrap: CatalogBootstrap,
    /// The most of one answer that is ever held in memory.
    #[serde(default = "default_catalog_max_payload_bytes")]
    pub max_payload_bytes: usize,
    /// Bounded timeout for the retention connection.
    #[serde(default = "default_catalog_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Bounded timeout for each retention operation.
    #[serde(default = "default_catalog_operation_timeout_ms")]
    pub operation_timeout_ms: u64,
}

/// Which upstream provides imported metadata.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CatalogSourceBackend {
    /// No import at all: the operator's own resources are the catalogue.
    #[default]
    None,
    /// models.dev over HTTPS, conditionally.
    ModelsDev,
    /// The bundled seed only, with no network at all — an air-gapped
    /// deployment's whole source.
    Seed,
}

impl CatalogSourceBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ModelsDev => "models-dev",
            Self::Seed => "seed",
        }
    }
}

/// Where imported snapshots are retained.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CatalogStoreBackend {
    /// Process memory: lost on restart, so every boot re-imports.
    #[default]
    InMemory,
    /// Postgres, keyed by content identity, with a transactional active pointer.
    Postgres,
}

impl CatalogStoreBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InMemory => "in-memory",
            Self::Postgres => "postgres",
        }
    }
}

/// What an empty store starts from.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CatalogBootstrap {
    /// Nothing until an import succeeds.
    #[default]
    Empty,
    /// The bundled seed, admitted without claiming upstream confirmation.
    Seed,
}

impl Default for CatalogConfig {
    fn default() -> Self {
        Self {
            source: CatalogSourceBackend::None,
            store: CatalogStoreBackend::InMemory,
            source_url: None,
            dsn_env: None,
            schema: None,
            create_table: default_catalog_create_table(),
            refresh_interval_seconds: default_catalog_refresh_interval_seconds(),
            refresh_timeout_seconds: default_catalog_refresh_timeout_seconds(),
            retry_initial_seconds: default_catalog_retry_initial_seconds(),
            retry_max_seconds: default_catalog_retry_max_seconds(),
            bootstrap: CatalogBootstrap::Empty,
            max_payload_bytes: default_catalog_max_payload_bytes(),
            connect_timeout_ms: default_catalog_connect_timeout_ms(),
            operation_timeout_ms: default_catalog_operation_timeout_ms(),
        }
    }
}

impl CatalogConfig {
    /// Whether anything at all is imported.
    ///
    /// The one question boot asks: a disabled section spawns no task, opens no
    /// connection, and builds no HTTP client.
    pub fn enabled(&self) -> bool {
        self.source != CatalogSourceBackend::None
    }

    /// The document a models.dev import fetches.
    pub fn url(&self) -> &str {
        self.source_url
            .as_deref()
            .unwrap_or(crate::backends::models_dev::MODELS_DEV_CATALOG_URL)
    }

    /// The pacing the background refresh runs at.
    ///
    /// Built here rather than validated field by field, so the coherence rules a
    /// schedule already states — a timeout inside its interval, a backoff
    /// ceiling that cannot make a refusing deployment refresh less often than a
    /// healthy one — are checked at boot by their owner.
    pub fn schedule(&self) -> RefreshSchedule {
        RefreshSchedule {
            interval: Duration::from_secs(self.refresh_interval_seconds),
            timeout: Duration::from_secs(self.refresh_timeout_seconds),
            backoff: BackoffPolicy {
                initial: Duration::from_secs(self.retry_initial_seconds),
                max: Duration::from_secs(self.retry_max_seconds),
                multiplier: 2,
            },
        }
    }

    /// What an empty store starts from.
    pub fn bootstrap_mode(&self) -> Bootstrap {
        match self.bootstrap {
            CatalogBootstrap::Empty => Bootstrap::Empty,
            CatalogBootstrap::Seed => Bootstrap::Seed,
        }
    }

    /// The retention settings a Postgres store connects with.
    pub fn store_settings(&self) -> CatalogStoreSettings {
        CatalogStoreSettings {
            schema: self.schema.clone(),
            create_table: self.create_table,
            connect_timeout: Duration::from_millis(self.connect_timeout_ms),
            operation_timeout: Duration::from_millis(self.operation_timeout_ms),
        }
    }
}

fn default_catalog_create_table() -> bool {
    true
}

/// Six hours: models.dev publishes on the order of days, and a conditional
/// request that answers `304` costs one round trip and no transfer.
fn default_catalog_refresh_interval_seconds() -> u64 {
    21_600
}

fn default_catalog_refresh_timeout_seconds() -> u64 {
    60
}

fn default_catalog_retry_initial_seconds() -> u64 {
    60
}

fn default_catalog_retry_max_seconds() -> u64 {
    3_600
}

fn default_catalog_max_payload_bytes() -> usize {
    crate::backends::models_dev::MAX_PAYLOAD_BYTES
}

fn default_catalog_connect_timeout_ms() -> u64 {
    10_000
}

fn default_catalog_operation_timeout_ms() -> u64 {
    30_000
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayKey {
    /// Env var holding the inbound key secret.
    #[serde(default)]
    pub env: Option<String>,
    /// File path holding the inbound key secret.
    #[serde(default)]
    pub file: Option<String>,
    pub namespace: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyMaterialSource<'a> {
    Env(&'a str),
    File(&'a str),
}

impl GatewayKey {
    pub fn source(&self) -> Option<KeyMaterialSource<'_>> {
        let env = self.env.as_deref().filter(|value| !value.trim().is_empty());
        let file = self
            .file
            .as_deref()
            .filter(|value| !value.trim().is_empty());
        match (env, file) {
            (Some(env), None) => Some(KeyMaterialSource::Env(env)),
            (None, Some(file)) => Some(KeyMaterialSource::File(file)),
            _ => None,
        }
    }

    pub fn source_label(&self) -> Option<&str> {
        self.source().map(|source| match source {
            KeyMaterialSource::Env(value) | KeyMaterialSource::File(value) => value,
        })
    }
}

// Deliberate policy ceiling for configured token lifetimes, not a protocol limit.

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("config load: {0}")]
    Load(String),
    #[error("invalid config: {0}")]
    Invalid(String),
}

impl Config {
    /// Load from a TOML file with environment overrides layered on top.
    pub fn load(path: &str) -> Result<Self, ConfigError> {
        use figment::{
            Figment,
            providers::{Env, Format, Toml},
        };
        let cfg: Config = Figment::new()
            .merge(Toml::file(path))
            .merge(Env::prefixed("AXOND_").split("__"))
            .extract()
            .map_err(|e| ConfigError::Load(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Reject a structurally-invalid config at boot rather than at request time
    /// (delta B2).
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validate_inner(false)
    }

    fn validate_inner(&self, allow_memory_sqlite: bool) -> Result<(), ConfigError> {
        self.reject_withdrawn_sections()?;
        self.validate_storage(allow_memory_sqlite)?;
        if self.discovery.refresh_interval_seconds == 0 {
            return Err(ConfigError::Invalid(
                "discovery.refresh_interval_seconds must be at least 1".into(),
            ));
        }
        self.validate_resource_graph()
    }

    /// Every withdrawn section present, named in one error, so an operator
    /// migrating an old config sees the whole list at once.
    fn reject_withdrawn_sections(&self) -> Result<(), ConfigError> {
        let sections: Vec<String> = WITHDRAWN_SECTIONS
            .iter()
            .filter(|key| self.unrecognized.contains_key(**key))
            .map(|key| format!("`{key}`"))
            .collect();
        if sections.is_empty() {
            return Ok(());
        }
        Err(ConfigError::Invalid(format!(
            "{} {} withdrawn (ADR 0063): remove it. Axond is a store-backed gateway with one \
             static `[[gateway_key]]`.",
            sections.join(", "),
            if sections.len() == 1 { "is" } else { "are" },
        )))
    }

    fn validate_storage(&self, allow_memory_sqlite: bool) -> Result<(), ConfigError> {
        let Some(storage) = self.storage.as_ref() else {
            return Err(ConfigError::Invalid(
                "`[storage]` is required (ADR 0063): set `backend = \"sqlite\"` with `path`, or `backend = \"postgres\"` with `dsn_env`".into(),
            ));
        };
        let index = &storage.usage_index;
        if index.buffer_capacity == 0 {
            return Err(ConfigError::Invalid(
                "`[storage.usage_index]` buffer_capacity must be at least 1".into(),
            ));
        }
        if index.buffer_capacity > max_usage_index_buffer_capacity() {
            return Err(ConfigError::Invalid(format!(
                "`[storage.usage_index]` buffer_capacity ({}) must not exceed {}",
                index.buffer_capacity,
                max_usage_index_buffer_capacity()
            )));
        }
        if index.max_batch == 0 {
            return Err(ConfigError::Invalid(
                "`[storage.usage_index]` max_batch must be at least 1".into(),
            ));
        }
        if index.max_batch > crate::store::MAX_USAGE_INDEX_BATCH {
            return Err(ConfigError::Invalid(format!(
                "`[storage.usage_index]` max_batch ({}) must not exceed {}",
                index.max_batch,
                crate::store::MAX_USAGE_INDEX_BATCH
            )));
        }
        if index.max_batch > index.buffer_capacity {
            return Err(ConfigError::Invalid(format!(
                "`[storage.usage_index]` max_batch ({}) must not exceed buffer_capacity ({})",
                index.max_batch, index.buffer_capacity
            )));
        }
        if index.flush_interval_ms > MAX_USAGE_INDEX_FLUSH_INTERVAL_MS {
            return Err(ConfigError::Invalid(format!(
                "`[storage.usage_index]` flush_interval_ms ({}) must not exceed {} (24h)",
                index.flush_interval_ms, MAX_USAGE_INDEX_FLUSH_INTERVAL_MS
            )));
        }
        match storage.backend {
            StorageBackend::Sqlite => {
                if storage
                    .path
                    .as_deref()
                    .is_none_or(|path| path.trim().is_empty())
                {
                    return Err(ConfigError::Invalid(
                        "`[storage]` sqlite requires a non-empty `path`".into(),
                    ));
                }
                if !allow_memory_sqlite
                    && storage
                        .path
                        .as_deref()
                        .is_some_and(|path| path.trim() == ":memory:")
                {
                    return Err(ConfigError::Invalid(
                        "`[storage]` sqlite `:memory:` is not durable; use a file path".into(),
                    ));
                }
                if storage
                    .dsn_env
                    .as_deref()
                    .is_some_and(|name| !name.trim().is_empty())
                {
                    return Err(ConfigError::Invalid(
                        "`[storage]` sqlite ignores `dsn_env`; omit it or use backend = \"postgres\""
                            .into(),
                    ));
                }
            }
            StorageBackend::Postgres => {
                if storage
                    .dsn_env
                    .as_deref()
                    .is_none_or(|name| name.trim().is_empty())
                {
                    return Err(ConfigError::Invalid(
                        "`[storage]` postgres requires a non-empty `dsn_env`".into(),
                    ));
                }
                if storage
                    .path
                    .as_deref()
                    .is_some_and(|path| !path.trim().is_empty())
                {
                    return Err(ConfigError::Invalid(
                        "`[storage]` postgres ignores `path`; omit it or use backend = \"sqlite\""
                            .into(),
                    ));
                }
            }
        }
        Ok(())
    }

    /// The whole-graph gate: every provider, credential, key, and sink the file
    /// declares must reference something it also declares.
    fn validate_resource_graph(&self) -> Result<(), ConfigError> {
        let defaults = self.namespace.iter().filter(|n| n.default).count();
        if defaults != 1 {
            return Err(ConfigError::Invalid(format!(
                "exactly one namespace must set `default = true` (found {defaults})"
            )));
        }
        let providers: HashMap<&str, &Provider> =
            self.provider.iter().map(|p| (p.id.as_str(), p)).collect();
        let namespaces: HashMap<&str, &Namespace> =
            self.namespace.iter().map(|n| (n.id.as_str(), n)).collect();

        self.validate_price_book(&providers)?;
        self.validate_blocklist()?;
        if self.credential_pool.failure_threshold == 0 {
            return Err(ConfigError::Invalid(
                "credential_pool.failure_threshold must be at least 1".into(),
            ));
        }
        if self.credential_pool.cooldown_seconds == 0 {
            return Err(ConfigError::Invalid(
                "credential_pool.cooldown_seconds must be at least 1".into(),
            ));
        }
        if self.failover.max_attempts == 0 {
            return Err(ConfigError::Invalid(
                "failover.max_attempts must be at least 1".into(),
            ));
        }
        if self.failover.overall_timeout_ms == 0 {
            return Err(ConfigError::Invalid(
                "failover.overall_timeout_ms must be at least 1".into(),
            ));
        }
        if self.failover.failure_threshold == 0 {
            return Err(ConfigError::Invalid(
                "failover.failure_threshold must be at least 1".into(),
            ));
        }
        if self.failover.cooldown_seconds == 0 {
            return Err(ConfigError::Invalid(
                "failover.cooldown_seconds must be at least 1".into(),
            ));
        }
        self.validate_process_local_bounds()?;
        let mut labels: HashMap<(&str, &str), Vec<&str>> = HashMap::new();
        for c in &self.credential {
            if c.env.as_deref().map(str::trim).unwrap_or("").is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "credential for namespace `{}` provider `{}` has an empty `env`",
                    c.namespace, c.provider
                )));
            }
            if c.weight == 0 {
                return Err(ConfigError::Invalid(format!(
                    "credential `{}` has weight 0; remove it instead",
                    c.label()
                )));
            }
            let pool = labels
                .entry((c.namespace.as_str(), c.provider.as_str()))
                .or_default();
            if pool.contains(&c.label()) {
                return Err(ConfigError::Invalid(format!(
                    "duplicate credential id `{}` for namespace `{}` provider `{}`",
                    c.label(),
                    c.namespace,
                    c.provider
                )));
            }
            pool.push(c.label());
            if !namespaces.contains_key(c.namespace.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "credential references undefined namespace `{}`",
                    c.namespace
                )));
            }
            if !providers.contains_key(c.provider.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "credential references undefined provider `{}`",
                    c.provider
                )));
            }
        }
        self.validate_gateway_keys(&namespaces)?;
        self.validate_usage_sinks()?;
        Ok(())
    }

    /// Bounds the process applies to itself: per-phase upstream limits, the
    /// inbound admission ceilings, and how long termination may take.
    fn validate_process_local_bounds(&self) -> Result<(), ConfigError> {
        self.validate_admission()?;
        for (field, value) in [
            ("connect_timeout_ms", self.transport.connect_timeout_ms),
            (
                "response_header_timeout_ms",
                self.transport.response_header_timeout_ms,
            ),
            (
                "buffered_body_timeout_ms",
                self.transport.buffered_body_timeout_ms,
            ),
            (
                "stream_idle_timeout_ms",
                self.transport.stream_idle_timeout_ms,
            ),
            (
                "stream_terminal_grace_ms",
                self.transport.stream_terminal_grace_ms,
            ),
            ("max_response_bytes", self.transport.max_response_bytes),
            ("max_error_bytes", self.transport.max_error_bytes),
        ] {
            if value == 0 {
                return Err(ConfigError::Invalid(format!(
                    "transport.{field} must be at least 1"
                )));
            }
        }
        if self.transport.max_error_bytes > self.transport.max_response_bytes {
            return Err(ConfigError::Invalid(
                "transport.max_error_bytes must not exceed transport.max_response_bytes: an error \
                 body is a response body"
                    .into(),
            ));
        }
        // `0` would mean "wait forever", which is the unbounded wait a
        // termination grace period ends with `SIGKILL` — before anything flushes.
        for (field, value) in [
            ("deadline_ms", self.shutdown.deadline_ms),
            ("flush_timeout_ms", self.shutdown.flush_timeout_ms),
        ] {
            if value == 0 {
                return Err(ConfigError::Invalid(format!(
                    "shutdown.{field} must be at least 1: shutdown waits are bounded"
                )));
            }
        }
        self.validate_catalog()?;
        Ok(())
    }

    /// Inbound authentication fails closed (ADR 0013): a config that declares no
    /// usable gateway key describes a gateway nobody could call, which is a boot
    /// failure rather than an open door.
    fn validate_gateway_keys(
        &self,
        namespaces: &HashMap<&str, &Namespace>,
    ) -> Result<(), ConfigError> {
        if self.gateway_key.is_empty() {
            return Err(ConfigError::Invalid(
                "exactly one `[[gateway_key]]` is required: inbound authentication fails closed and there is no keyless mode"
                    .into(),
            ));
        }
        if self.gateway_key.len() > 1 {
            return Err(ConfigError::Invalid(
                "`[[gateway_key]]` as a per-namespace list is withdrawn (ADR 0063): declare exactly one deployment-wide static key"
                    .into(),
            ));
        }
        for k in &self.gateway_key {
            let env = k.env.as_deref().unwrap_or("");
            let file = k.file.as_deref().unwrap_or("");
            if !env.trim().is_empty() && !file.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "gateway_key for namespace `{}` declares both `env` and `file`; exactly one source is permitted",
                    k.namespace
                )));
            }
            if env.trim().is_empty() && file.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "gateway_key for namespace `{}` must declare exactly one non-empty source (`env` or `file`)",
                    k.namespace
                )));
            }
            if !namespaces.contains_key(k.namespace.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "gateway_key `{}` references undefined namespace `{}`",
                    k.source_label().unwrap_or(""),
                    k.namespace
                )));
            }
        }
        Ok(())
    }

    /// The admission bounds only mean anything as a set: a per-tenant ceiling
    /// above the global one cannot isolate a tenant, and a queue is either sized
    /// and time-bounded or absent.
    fn validate_admission(&self) -> Result<(), ConfigError> {
        let admission = &self.admission;
        if admission.max_request_bytes == 0 {
            return Err(ConfigError::Invalid(
                "admission.max_request_bytes must be at least 1".into(),
            ));
        }
        if admission.max_in_flight_per_tenant > 0 && admission.max_tenants == 0 {
            return Err(ConfigError::Invalid(
                "admission.max_tenants must be at least 1 when max_in_flight_per_tenant is set"
                    .into(),
            ));
        }
        // Only a ceiling the operator wrote can contradict another: a defaulted
        // one was already clamped to `max_in_flight` on load, so nobody is told
        // to fix a key they never set.
        if admission.max_in_flight > 0
            && admission.max_in_flight_per_tenant_explicit
            && admission.max_in_flight_per_tenant > admission.max_in_flight
        {
            return Err(ConfigError::Invalid(format!(
                "admission.max_in_flight_per_tenant ({}) must not exceed admission.max_in_flight \
                 ({}): a per-tenant ceiling above the global one cannot isolate a tenant",
                admission.max_in_flight_per_tenant, admission.max_in_flight
            )));
        }
        if admission.max_in_flight > 0
            && admission.max_in_flight_streams_explicit
            && admission.max_in_flight_streams > 0
            && admission.max_in_flight_streams > admission.max_in_flight
        {
            return Err(ConfigError::Invalid(format!(
                "admission.max_in_flight_streams ({}) must not exceed admission.max_in_flight \
                 ({}): a stream is an in-flight request",
                admission.max_in_flight_streams, admission.max_in_flight
            )));
        }
        // Each ceiling becomes a semaphore, which asserts on an absurd size.
        // Refused here so it is the same typed boot error as every other bound
        // rather than a panic naming no key.
        for (field, value) in [
            ("admission.max_in_flight", admission.max_in_flight),
            (
                "admission.max_in_flight_streams",
                admission.max_in_flight_streams,
            ),
            ("admission.queue_capacity", admission.queue_capacity),
            (
                "admission.max_pending_settlements",
                admission.max_pending_settlements,
            ),
            (
                "admission.max_in_flight_settlements",
                admission.max_in_flight_settlements,
            ),
        ] {
            if value > MAX_PERMITS {
                return Err(ConfigError::Invalid(format!(
                    "{field} ({value}) must not exceed {}: a larger ceiling is not a bound this \
                     process can hold",
                    MAX_PERMITS
                )));
            }
        }
        if (admission.queue_capacity == 0) != (admission.queue_wait_ms == 0) {
            return Err(ConfigError::Invalid(
                "admission.queue_capacity and admission.queue_wait_ms must be set together: a \
                 queue without a wait bound is unbounded latency, and a wait without a queue is \
                 never used"
                    .into(),
            ));
        }
        if admission.queue_capacity > 0 && admission.max_in_flight == 0 {
            return Err(ConfigError::Invalid(
                "admission.queue_capacity requires admission.max_in_flight: nothing queues when \
                 the global ceiling is off"
                    .into(),
            ));
        }
        // Every admitted request reserves one settlement, so a settlement
        // ceiling below the global one would shed at the lower number with the
        // wrong verdict. Only a written one can contradict: a defaulted one
        // follows `max_in_flight`.
        if admission.max_in_flight > 0
            && admission.max_pending_settlements_explicit
            && admission.max_pending_settlements > 0
            && admission.max_pending_settlements < admission.max_in_flight
        {
            return Err(ConfigError::Invalid(format!(
                "admission.max_pending_settlements ({}) must be at least admission.max_in_flight \
                 ({}): every admitted request reserves one settlement",
                admission.max_pending_settlements, admission.max_in_flight
            )));
        }
        Ok(())
    }

    /// The catalogue import section, checked as the set it is.
    ///
    /// A disabled section is not checked at all beyond being disabled: fields
    /// left at their defaults describe an import that will never be attempted,
    /// and refusing to boot over them would make the inert default fragile.
    ///
    /// Enabled, the rules are the ones a background loop cannot recover from: a
    /// zero interval or timeout is a busy loop or an instant abandonment, a
    /// backoff ceiling below its first delay never converges, retention needs a
    /// DSN reference it can resolve *by name* (the value stays in the
    /// environment), and a stateful deployment may not retain its catalogue in
    /// memory it is about to lose.
    fn validate_catalog(&self) -> Result<(), ConfigError> {
        let catalog = &self.catalog;
        if !catalog.enabled() {
            return Ok(());
        }
        for (field, value) in [
            ("refresh_interval_seconds", catalog.refresh_interval_seconds),
            ("refresh_timeout_seconds", catalog.refresh_timeout_seconds),
            ("retry_initial_seconds", catalog.retry_initial_seconds),
            ("retry_max_seconds", catalog.retry_max_seconds),
            ("connect_timeout_ms", catalog.connect_timeout_ms),
            ("operation_timeout_ms", catalog.operation_timeout_ms),
        ] {
            if value == 0 {
                return Err(ConfigError::Invalid(format!(
                    "catalog.{field} must be at least 1"
                )));
            }
        }
        if catalog.max_payload_bytes == 0 {
            return Err(ConfigError::Invalid(
                "catalog.max_payload_bytes must be at least 1".into(),
            ));
        }
        catalog
            .schedule()
            .validate()
            .map_err(|error| ConfigError::Invalid(format!("catalog: {error}")))?;
        if catalog.source == CatalogSourceBackend::ModelsDev {
            let source_url = reqwest::Url::parse(catalog.url()).map_err(|error| {
                ConfigError::Invalid(format!("catalog.source_url is not a valid URL: {error}"))
            })?;
            if source_url.scheme() != "https" {
                return Err(ConfigError::Invalid(format!(
                    "catalog.source_url `{}` must be `https://`: imported metadata is read for \
                     pricing and enablement decisions, so a source that can be substituted in \
                     transit is refused rather than trusted",
                    catalog.url()
                )));
            }
            let has_authority = catalog
                .url()
                .split_once("://")
                .and_then(|(_, rest)| rest.split(['/', '?', '#']).next())
                .is_some_and(|authority| !authority.is_empty());
            if source_url.host_str().is_none() || !has_authority {
                return Err(ConfigError::Invalid(
                    "catalog.source_url must name an HTTPS host".into(),
                ));
            }
            if !source_url.username().is_empty() || source_url.password().is_some() {
                return Err(ConfigError::Invalid(
                    "catalog.source_url must not contain embedded credentials".into(),
                ));
            }
            crate::backends::models_dev::ModelsDevAdapter::new(catalog.url())
                .map_err(|error| ConfigError::Invalid(format!("catalog.source_url: {error}")))?;
        } else if catalog.source_url.is_some() {
            return Err(ConfigError::Invalid(format!(
                "catalog `{}`: `source_url` applies only to `models-dev`",
                catalog.source.as_str()
            )));
        }
        if catalog.store == CatalogStoreBackend::Postgres {
            let dsn_env = catalog
                .dsn_env
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty());
            if dsn_env.is_none() {
                return Err(ConfigError::Invalid(
                    "catalog `postgres`: `dsn_env` must name the env var holding the connection \
                     string"
                        .into(),
                ));
            }
            if let Some(schema) = catalog.schema.as_deref() {
                validate_schema_name("catalog.schema", schema)?;
            }
        }
        Ok(())
    }

    /// A sink's fields only make sense together, so they are checked as a set:
    /// a Postgres sink needs a DSN reference and a table name that is safe to
    /// interpolate.
    fn validate_usage_sinks(&self) -> Result<(), ConfigError> {
        for sink in &self.usage_sink {
            let kind = sink.kind.as_str();
            if sink.kind == UsageSinkKind::Postgres {
                if sink.buffer_capacity == 0 {
                    return Err(ConfigError::Invalid(format!(
                        "usage_sink `{kind}`: buffer_capacity must be at least 1"
                    )));
                }
                if sink.max_batch == 0 {
                    return Err(ConfigError::Invalid(format!(
                        "usage_sink `{kind}`: max_batch must be at least 1"
                    )));
                }
                if sink.max_batch_explicit && sink.max_batch > sink.buffer_capacity {
                    return Err(ConfigError::Invalid(format!(
                        "usage_sink `{kind}`: max_batch ({}) must not exceed buffer_capacity ({})",
                        sink.max_batch, sink.buffer_capacity
                    )));
                }
                if sink.flush_interval_ms == 0 {
                    return Err(ConfigError::Invalid(format!(
                        "usage_sink `{kind}`: flush_interval_ms must be at least 1"
                    )));
                }
                match sink.dsn_env.as_deref().map(str::trim) {
                    Some(dsn_env) if !dsn_env.is_empty() => {}
                    _ => {
                        return Err(ConfigError::Invalid(
                            "usage_sink `postgres`: `dsn_env` must name the env var holding the connection string"
                                .into(),
                        ));
                    }
                }
                validate_table_name(&sink.table()).map_err(|message| {
                    ConfigError::Invalid(format!("usage_sink `postgres`: {message}"))
                })?;
            }
        }
        self.validate_usage_journal()
    }

    /// The journal's fields are checked as a set too, and the check is where a
    /// deployment learns that a setting it chose cannot hold the guarantee it
    /// asked for: an outbox with no destination, or a destination that cannot
    /// report a failed write, is refused rather than run.
    fn validate_usage_journal(&self) -> Result<(), ConfigError> {
        let journal = &self.usage_journal;
        if !journal.backend.is_enabled() {
            // Every other field is inert without a backend, so a half-written
            // section is not an error — it is a section that does nothing.
            return Ok(());
        }
        match journal.dsn_env.as_deref().map(str::trim) {
            Some(dsn_env) if !dsn_env.is_empty() => {}
            _ => {
                return Err(ConfigError::Invalid(
                    "usage_journal `postgres`: `dsn_env` must name the env var holding the \
                     connection string"
                        .into(),
                ));
            }
        }
        if let Some(schema) = journal.schema.as_deref() {
            validate_table_name(schema).map_err(|message| {
                ConfigError::Invalid(format!("usage_journal `schema`: {message}"))
            })?;
            if schema.contains('.') {
                return Err(ConfigError::Invalid(format!(
                    "usage_journal `schema`: `{schema}` is qualified, but a search path takes one \
                     unqualified schema name"
                )));
            }
        }
        ConsumerId::parse(&journal.consumer).map_err(|message| {
            ConfigError::Invalid(format!("usage_journal `consumer`: {message}"))
        })?;
        if journal.max_events == 0 {
            return Err(ConfigError::Invalid(
                "usage_journal: max_events must be at least 1".into(),
            ));
        }
        if journal.max_delivery_attempts == 0 {
            return Err(ConfigError::Invalid(
                "usage_journal: max_delivery_attempts must be at least 1, or no event is ever \
                 delivered"
                    .into(),
            ));
        }
        if journal.claim_batch == 0 {
            return Err(ConfigError::Invalid(
                "usage_journal: claim_batch must be at least 1".into(),
            ));
        }
        if journal.connections < 2 {
            return Err(ConfigError::Invalid(
                "usage_journal: connections must be at least 2, because one is reserved for the \
                 delivery worker"
                    .into(),
            ));
        }
        for (field, value) in [
            ("operation_timeout_ms", journal.operation_timeout_ms),
            ("connect_timeout_ms", journal.connect_timeout_ms),
            ("poll_interval_ms", journal.poll_interval_ms),
            ("lease_seconds", journal.lease_seconds),
            (
                "retain_acknowledged_seconds",
                journal.retain_acknowledged_seconds,
            ),
        ] {
            if value == 0 {
                return Err(ConfigError::Invalid(format!(
                    "usage_journal: {field} must be at least 1"
                )));
            }
        }
        if self.usage_sink.is_empty() {
            return Err(ConfigError::Invalid(
                "usage_journal `postgres`: at least one `[[usage_sink]]` must be configured — the \
                 journal is the durable path *to* the sinks, and with none of them an \
                 acknowledgement would mean nothing"
                    .into(),
            ));
        }
        // An OTLP sink is not refused, because a deployment that exports usage
        // telemetry has every reason to store it durably too. It is carried
        // alongside the acknowledged destinations instead: the OTel SDK's batch
        // processor owns the write and never says whether it landed, so
        // acknowledging on its behalf would forget events while reporting
        // success. What is refused is a journal where *every* destination is
        // like that, since then an acknowledgement rests on nothing.
        if self
            .usage_sink
            .iter()
            .all(|sink| sink.kind == UsageSinkKind::Otlp)
        {
            return Err(ConfigError::Invalid(
                "usage_journal `postgres`: an `otlp` sink cannot answer for a write, so at least \
                 one other `[[usage_sink]]` must be configured for the worker to acknowledge on"
                    .into(),
            ));
        }
        Ok(())
    }

    pub fn default_namespace(&self) -> &str {
        self.namespace
            .iter()
            .find(|n| n.default)
            .map(|n| n.id.as_str())
            .unwrap_or("platform")
    }

    pub fn provider(&self, id: &str) -> Option<&Provider> {
        self.provider.iter().find(|p| p.id == id)
    }

    /// First matching optional `[[price]]` rule in file order, or `None`.
    ///
    /// Request-path charging prefers imported models.dev rates; this book is
    /// the fallback for offerings the catalogue does not price.
    pub fn price_for(&self, provider: &str, model: &str) -> Option<ModelPrice> {
        self.price.iter().find_map(|rule| {
            if rule.provider != provider {
                return None;
            }
            glob_permits(&rule.model, model).then_some(rule.price)
        })
    }

    /// Union of the deployment default and a namespace's extra globs.
    pub fn is_blocked(&self, prefixed: &str, bare: &str, extra: &[String]) -> bool {
        self.blocklist
            .models
            .iter()
            .chain(extra)
            .any(|pattern| glob_permits(pattern, prefixed) || glob_permits(pattern, bare))
    }

    fn validate_price_book(&self, providers: &HashMap<&str, &Provider>) -> Result<(), ConfigError> {
        for rule in &self.price {
            if rule.provider.trim().is_empty() || rule.model.trim().is_empty() {
                return Err(ConfigError::Invalid(
                    "`[[price]]` requires a non-empty `provider` and `model` glob".into(),
                ));
            }
            if !providers.contains_key(rule.provider.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "`[[price]]` references undefined provider `{}`",
                    rule.provider
                )));
            }
            parse_glob(&rule.model).map_err(|pattern| {
                ConfigError::Invalid(format!(
                    "`[[price]]` model glob `{pattern}` is invalid: use an exact id, `prefix*`, `*suffix`, or `*`"
                ))
            })?;
        }
        Ok(())
    }

    fn validate_blocklist(&self) -> Result<(), ConfigError> {
        for pattern in &self.blocklist.models {
            parse_glob(pattern).map_err(|pattern| {
                ConfigError::Invalid(format!(
                    "blocklist glob `{pattern}` is invalid: use an exact id, `prefix*`, `*suffix`, or `*`"
                ))
            })?;
        }
        Ok(())
    }

    pub fn namespace(&self, id: &str) -> Option<&Namespace> {
        self.namespace.iter().find(|n| n.id == id)
    }

    #[cfg(test)]
    pub fn distinct_namespace_count(&self) -> usize {
        self.namespace
            .iter()
            .map(|namespace| namespace.id.as_str())
            .collect::<HashSet<_>>()
            .len()
    }

    /// Parse + validate from an in-memory TOML string (tests, and the planned
    /// `axond --check` config linter).
    #[cfg(any(test, fuzzing))]
    pub fn from_toml_str(s: &str) -> Result<Self, ConfigError> {
        use figment::{
            Figment,
            providers::{Format, Toml},
        };
        let owned;
        let source = if s.contains("[storage]") {
            s
        } else {
            // Append, never prepend: a leading `[storage]` table would capture
            // subsequent root keys (`mode = "stateful"`) as storage fields.
            owned = format!("{s}\n\n[storage]\nbackend = \"sqlite\"\npath = \":memory:\"\n");
            &owned
        };
        let cfg: Config = Figment::new()
            .merge(Toml::string(source))
            .extract()
            .map_err(|e| ConfigError::Load(e.to_string()))?;
        cfg.validate_inner(true)?;
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
[storage]
backend = "sqlite"
path = ":memory:"

[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

[[gateway_key]]
env = "AXOND_KEY"
namespace = "platform"

[[price]]
provider = "openai"
model = "*"
input_microdollars_per_million = 2500000
output_microdollars_per_million = 10000000
"#;

    #[test]
    fn price_book_first_match_in_file_order() {
        let cfg = Config::from_toml_str(&format!(
            r#"
{VALID}

[[price]]
provider = "openai"
model = "gpt-4o"
input_microdollars_per_million = 1
output_microdollars_per_million = 2
"#
        ))
        .expect("exact before glob");
        // VALID already has `model = "*"` first, so the catch-all wins.
        assert_eq!(
            cfg.price_for("openai", "gpt-4o")
                .expect("priced")
                .input_microdollars_per_million,
            2_500_000
        );

        let cfg = Config::from_toml_str(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

[[gateway_key]]
env = "AXOND_KEY"
namespace = "platform"

[[price]]
provider = "openai"
model = "gpt-4o"
input_microdollars_per_million = 1
output_microdollars_per_million = 2

[[price]]
provider = "openai"
model = "*"
input_microdollars_per_million = 9
output_microdollars_per_million = 9
"#,
        )
        .expect("exact first");
        assert_eq!(
            cfg.price_for("openai", "gpt-4o")
                .expect("exact")
                .input_microdollars_per_million,
            1
        );
        assert_eq!(
            cfg.price_for("openai", "o3")
                .expect("glob")
                .input_microdollars_per_million,
            9
        );
    }

    #[test]
    fn rejects_an_invalid_blocklist_or_price_glob() {
        let err = Config::from_toml_str(&format!("{VALID}\n[blocklist]\nmodels = [\"foo*bar\"]\n"))
            .expect_err("middle star");
        assert!(err.to_string().contains("blocklist glob"), "{err}");

        let err = Config::from_toml_str(&format!(
            r#"
{VALID}
[[price]]
provider = "openai"
model = "foo*bar"
input_microdollars_per_million = 1
output_microdollars_per_million = 1
"#
        ))
        .expect_err("price glob");
        assert!(err.to_string().contains("model glob"), "{err}");
    }

    #[test]
    fn rejects_a_config_with_no_storage() {
        use figment::{
            Figment,
            providers::{Format, Toml},
        };
        let toml = r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

[[gateway_key]]
env = "AXOND_KEY"
namespace = "platform"
"#;
        let cfg: Config = Figment::new()
            .merge(Toml::string(toml))
            .extract()
            .expect("parses without storage");
        let err = cfg.validate().expect_err("storage is required");
        assert!(err.to_string().contains("[storage]"), "{err}");
    }

    #[test]
    fn usage_index_batching_defaults_and_bounds() {
        let cfg = Config::from_toml_str(VALID).expect("valid");
        let index = &cfg.storage.as_ref().expect("storage").usage_index;
        assert_eq!(*index, UsageIndexConfig::default());
        let settings = index.settings();
        assert_eq!(settings.capacity, 1024);
        assert_eq!(settings.max_batch, 256);
        assert_eq!(settings.flush_interval, Duration::from_millis(50));

        let tuned = Config::from_toml_str(&VALID.replace(
            "path = \":memory:\"",
            "path = \":memory:\"\n[storage.usage_index]\nbuffer_capacity = 64\nmax_batch = 8\nflush_interval_ms = 0",
        ))
        .expect("tuned");
        let index = &tuned.storage.as_ref().expect("storage").usage_index;
        assert_eq!(
            *index,
            UsageIndexConfig {
                buffer_capacity: 64,
                max_batch: 8,
                flush_interval_ms: 0,
            }
        );

        for (body, needle) in [
            ("buffer_capacity = 0", "buffer_capacity must be at least 1"),
            ("max_batch = 0", "max_batch must be at least 1"),
            (
                "buffer_capacity = 8\nmax_batch = 9",
                "must not exceed buffer_capacity",
            ),
            (
                "buffer_capacity = 100000\nmax_batch = 5000",
                "must not exceed 4096",
            ),
            (
                &format!(
                    "buffer_capacity = {}",
                    tokio::sync::Semaphore::MAX_PERMITS.saturating_add(1)
                ),
                "must not exceed",
            ),
            ("flush_interval_ms = 86400001", "flush_interval_ms"),
        ] {
            let err = Config::from_toml_str(&VALID.replace(
                "path = \":memory:\"",
                &format!("path = \":memory:\"\n[storage.usage_index]\n{body}"),
            ))
            .expect_err(needle);
            assert!(err.to_string().contains(needle), "{err}");
        }
    }

    #[test]
    fn load_rejects_sqlite_memory() {
        let path = std::env::temp_dir().join(format!(
            "axond-memory-config-{}-{}.toml",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::write(&path, VALID).expect("write fixture");
        let err = Config::load(path.to_str().expect("utf8 path")).expect_err(":memory: refused");
        let _ = std::fs::remove_file(&path);
        assert!(err.to_string().contains(":memory:"), "{err}");
    }

    #[test]
    fn shipped_runnable_configs_load_without_test_storage_injection() {
        for relative in [
            "axond.example.toml",
            "ops/compose/axond.quickstart.toml",
            "ops/compose/axond.stateful.toml",
            "tests/tier0/axond.tier0.toml",
            "deploy/kubernetes/base/axond.toml",
            "deploy/azure-container-apps/axond.toml",
        ] {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join(relative);
            Config::load(path.to_str().expect("utf8 path"))
                .unwrap_or_else(|error| panic!("{relative} must load via Config::load: {error}"));
        }
    }

    #[test]
    fn compose_env_example_declares_one_inbound_key() {
        let text = repository_file("ops/compose/env.example");
        let inbound: Vec<&str> = text
            .lines()
            .map(str::trim)
            .filter(|line| !line.starts_with('#') && line.starts_with("GW_INBOUND_"))
            .collect();
        assert_eq!(
            inbound,
            ["GW_INBOUND_PLATFORM_KEY=quickstart-platform-key"],
            "env.example must declare exactly one inbound key (ADR 0063):\n{text}"
        );
    }

    /// Inbound auth fails closed (ADR 0013), so a config that would leave the
    /// gateway callable without a credential is refused at boot.
    #[test]
    fn rejects_plural_gateway_keys() {
        let error = Config::from_toml_str(&format!(
            "{VALID}\n[[gateway_key]]\nenv = \"AXOND_KEY_2\"\nnamespace = \"platform\"\n"
        ))
        .expect_err("one deployment-wide key");
        let message = error.to_string();
        assert!(
            message.contains("[[gateway_key]]") && message.contains("ADR 0063"),
            "{message}"
        );
    }

    #[test]
    fn rejects_a_config_with_no_gateway_keys() {
        let toml = r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"
"#;
        let err = Config::from_toml_str(toml).expect_err("a keyless gateway must not boot");
        assert!(
            matches!(err, ConfigError::Invalid(ref msg) if msg.contains("gateway_key")),
            "{err:?}"
        );
    }

    #[test]
    fn rejects_a_gateway_key_that_names_nothing_resolvable() {
        for key in [
            "[[gateway_key]]\nenv = \"\"\nnamespace = \"platform\"",
            "[[gateway_key]]\nenv = \"K\"\nnamespace = \"ghost\"",
        ] {
            let result = Config::from_toml_str(&format!("{VALID}\n{key}\n"));
            assert!(
                matches!(result, Err(ConfigError::Invalid(_))),
                "expected `{key}` to be rejected"
            );
        }
    }

    #[test]
    fn gateway_key_requires_exactly_one_source() {
        for source in [
            "env = \"K\"\nfile = \"/run/key\"",
            "env = \"\"\nfile = \"\"",
        ] {
            let result = Config::from_toml_str(&format!(
                "[storage]\nbackend = \"sqlite\"\npath = \":memory:\"\n\
                 [[namespace]]\nid = \"platform\"\ndefault = true\n\
                 [[provider]]\nid = \"openai\"\nkind = \"openai\"\nbase_url = \"https://api.openai.com/v1\"\n\
                 [[gateway_key]]\n{source}\nnamespace = \"platform\"\n\
                 [[price]]\nprovider = \"openai\"\nmodel = \"*\"\n\
                 input_microdollars_per_million = 1\noutput_microdollars_per_million = 1\n"
            ));
            let err = result.expect_err("source shape must be rejected");
            assert!(err.to_string().contains("exactly one"), "{err}");
        }
    }

    #[test]
    fn blank_file_is_absent_when_gateway_key_uses_env() {
        let config = Config::from_toml_str(
            "[storage]\nbackend = \"sqlite\"\npath = \":memory:\"\n\
             [[namespace]]\nid = \"platform\"\ndefault = true\n\
             [[provider]]\nid = \"openai\"\nkind = \"openai\"\nbase_url = \"https://api.openai.com/v1\"\n\
             [[gateway_key]]\nenv = \"K\"\nfile = \"\"\nnamespace = \"platform\"\n\
             [[price]]\nprovider = \"openai\"\nmodel = \"*\"\n\
             input_microdollars_per_million = 1\noutput_microdollars_per_million = 1\n",
        )
        .expect("blank file must not count as a declared source");
        let snapshot = crate::state::ConfigSnapshot::build(
            config,
            &std::collections::HashMap::from([("K".to_owned(), "secondary-secret".to_owned())]),
            0,
        )
        .expect("the non-empty env source resolves");
        assert_eq!(snapshot.inbound_key_count(), 1);
    }

    #[test]
    fn withdrawn_sections_fail_boot_by_name() {
        let toml = format!(
            "{VALID}\n[gateway_token]\naudience = \"test\"\n\n[rate_limit]\nbackend = \"redis\"\n"
        );
        let err = Config::from_toml_str(&toml).expect_err("a withdrawn section is not ignored");
        let err = err.to_string();
        assert!(err.contains("`gateway_token`"), "{err}");
        assert!(err.contains("`rate_limit`"), "{err}");
        assert!(err.contains("withdrawn (ADR 0063)"), "{err}");
    }

    #[test]
    fn distinct_namespace_count_ignores_duplicate_ids() {
        let cfg = Config::from_toml_str(&format!(
            "{VALID}\n[[namespace]]\nid = \"platform\"\n\n[[namespace]]\nid = \"tenant\"\n"
        ))
        .expect("duplicate namespace ids remain valid");

        assert_eq!(cfg.namespace.len(), 3);
        assert_eq!(cfg.distinct_namespace_count(), 2);
    }

    /// One connection is the delivery worker's, so a single-connection journal
    /// would be a worker and no lane for the appends requests wait on. The
    /// default is wide enough to serve concurrent requests rather than to merely
    /// boot.
    #[test]
    fn a_journal_needs_a_connection_for_appends_besides_the_workers() {
        let journal = "[usage_journal]\nbackend = \"postgres\"\ndsn_env = \"OUTBOX_DSN\"\n";
        for connections in ["0", "1"] {
            let error = Config::from_toml_str(&format!(
                "{VALID}\n[[usage_sink]]\nkind = \"stdout\"\n{journal}connections = {connections}\n"
            ))
            .expect_err("a journal without a request lane cannot serve");
            assert!(
                matches!(error, ConfigError::Invalid(ref message)
                    if message.contains("connections must be at least 2")),
                "{connections}: {error:?}"
            );
        }
        let config = Config::from_toml_str(&format!(
            "{VALID}\n[[usage_sink]]\nkind = \"stdout\"\n{journal}"
        ))
        .expect("a journal with default connections validates");
        assert_eq!(config.usage_journal.connections, 8);
    }

    /// Exporting usage over OTLP is an ordinary thing to be doing when billing
    /// grade is switched on, and it is the sink list as a whole that the journal
    /// would otherwise refuse — so the export is kept and simply not
    /// acknowledged on. Only a journal with nothing but OTLP is refused, because
    /// then there is nothing an acknowledgement could rest on.
    #[test]
    fn an_otlp_sink_beside_a_storing_one_does_not_cost_the_journal_its_boot() {
        let journal = "[usage_journal]\nbackend = \"postgres\"\ndsn_env = \"OUTBOX_DSN\"\n";
        let config = Config::from_toml_str(&format!(
            "{VALID}\n[[usage_sink]]\nkind = \"postgres\"\ndsn_env = \"USAGE_DSN\"\n\
             [[usage_sink]]\nkind = \"otlp\"\n{journal}"
        ))
        .expect("a journal may export telemetry beside a destination that stores the row");
        assert_eq!(config.usage_sink.len(), 2);

        let error = Config::from_toml_str(&format!(
            "{VALID}\n[[usage_sink]]\nkind = \"otlp\"\n{journal}"
        ))
        .expect_err("a journal whose every destination confirms nothing cannot acknowledge");
        assert!(
            matches!(error, ConfigError::Invalid(ref message)
                if message.contains("cannot answer for a write")),
            "{error:?}"
        );
    }

    #[test]
    fn failover_has_sane_defaults_when_omitted() {
        let cfg = Config::from_toml_str(VALID).expect("valid config");
        assert_eq!(cfg.failover.max_attempts, 3);
        assert_eq!(cfg.failover.overall_timeout_ms, 30_000);
        assert_eq!(cfg.failover.failure_threshold, 3);
        assert_eq!(cfg.failover.cooldown_seconds, 30);
    }

    #[test]
    fn rejects_zero_valued_failover_bounds() {
        for field in [
            "max_attempts",
            "overall_timeout_ms",
            "failure_threshold",
            "cooldown_seconds",
        ] {
            let toml = format!("{VALID}\n[failover]\n{field} = 0\n");
            let err = Config::from_toml_str(&toml).expect_err("zero must be rejected");
            assert!(
                matches!(err, ConfigError::Invalid(msg) if msg.contains(field)),
                "expected an Invalid error mentioning `{field}`",
            );
        }
    }

    /// The defaults are the shipped bounds: generous enough that no legitimate
    /// provider call is cut off, finite so nothing waits forever.
    #[test]
    fn transport_bounds_default_to_finite_values() {
        let cfg = Config::from_toml_str(VALID).expect("valid config");
        assert_eq!(cfg.transport.connect_timeout_ms, 5_000);
        assert_eq!(cfg.transport.response_header_timeout_ms, 30_000);
        assert_eq!(cfg.transport.buffered_body_timeout_ms, 30_000);
        assert_eq!(cfg.transport.stream_idle_timeout_ms, 120_000);
        assert_eq!(cfg.transport.stream_terminal_grace_ms, 1_000);
        assert_eq!(cfg.transport.max_response_bytes, 32 * 1024 * 1024);
        assert_eq!(cfg.transport.max_error_bytes, 64 * 1024);

        // A non-streamed completion produces no headers until it is finished,
        // so a header bound tighter than the walk budget would cut off slow
        // completions the walk still had time for.
        assert!(cfg.transport.response_header_timeout_ms >= cfg.failover.overall_timeout_ms);
        assert!(cfg.transport.buffered_body_timeout_ms >= cfg.failover.overall_timeout_ms);

        let limits = cfg.transport.limits();
        assert_eq!(limits.connect_timeout, Duration::from_millis(5_000));
        assert_eq!(limits.stream_idle_timeout, Duration::from_millis(120_000));
        assert_eq!(limits.max_error_bytes, 64 * 1024);
    }

    /// Zero is not "no bound" here; it is a gateway that cannot call anything.
    #[test]
    fn rejects_transport_bounds_that_disable_a_phase() {
        for field in [
            "connect_timeout_ms",
            "response_header_timeout_ms",
            "buffered_body_timeout_ms",
            "stream_idle_timeout_ms",
            "stream_terminal_grace_ms",
            "max_response_bytes",
            "max_error_bytes",
        ] {
            let toml = format!("{VALID}\n[transport]\n{field} = 0\n");
            let err = Config::from_toml_str(&toml).expect_err("zero must be rejected");
            assert!(
                matches!(err, ConfigError::Invalid(msg) if msg.contains(field)),
                "expected an Invalid error mentioning `{field}`",
            );
        }
    }

    #[test]
    fn rejects_an_error_bound_wider_than_the_body_bound() {
        let toml =
            format!("{VALID}\n[transport]\nmax_response_bytes = 1024\nmax_error_bytes = 2048\n");
        let err = Config::from_toml_str(&toml).expect_err("an error body is a response body");
        assert!(
            matches!(err, ConfigError::Invalid(msg) if msg.contains("max_error_bytes")),
            "expected an Invalid error mentioning `max_error_bytes`",
        );
    }

    #[test]
    fn rejects_price_pointing_at_undefined_provider() {
        let toml = r#"
[[namespace]]
id = "platform"
default = true

[[gateway_key]]
env = "AXOND_KEY"
namespace = "platform"

[[price]]
provider = "ghost"
model = "gpt-4o"
input_microdollars_per_million = 1
output_microdollars_per_million = 1
"#;
        let err = Config::from_toml_str(toml).unwrap_err();
        assert!(err.to_string().contains("undefined provider"), "{err:?}");
    }

    #[test]
    fn accepts_two_providers_without_alias_failover() {
        let toml = r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

[[provider]]
id = "anthropic"
kind = "anthropic"
base_url = "https://api.anthropic.com/v1"

[[gateway_key]]
env = "AXOND_KEY"
namespace = "platform"
"#;
        Config::from_toml_str(toml).expect("providers only");
    }

    #[test]
    fn rejects_config_without_exactly_one_default_namespace() {
        let toml = r#"
[[namespace]]
id = "a"
[[namespace]]
id = "b"
"#;
        assert!(matches!(
            Config::from_toml_str(toml),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn deployment_and_namespace_blocklists_union() {
        let cfg =
            Config::from_toml_str(&format!("{VALID}\n[blocklist]\nmodels = [\"*-preview\"]\n"))
                .expect("valid blocklist");
        assert!(cfg.is_blocked("openai/gpt-4o-preview", "gpt-4o-preview", &[]));
        assert!(!cfg.is_blocked("openai/gpt-4o", "gpt-4o", &[]));
        assert!(cfg.is_blocked("openai/gpt-4o", "gpt-4o", &["gpt-4o".to_owned()]));
        assert!(cfg.is_blocked("openai/secret-x", "secret-x", &["secret-*".to_owned()]));
    }

    #[test]
    fn accepts_a_pool_of_credentials_for_one_namespace_and_provider() {
        let cfg = Config::from_toml_str(&format!(
            r#"
{VALID}

[credential_pool]
strategy = "weighted"

[[credential]]
namespace = "platform"
provider = "openai"
env = "K1"
weight = 3

[[credential]]
namespace = "platform"
provider = "openai"
env = "K2"
id = "overflow"
"#
        ))
        .expect("valid pool");
        assert_eq!(cfg.credential_pool.strategy, SelectionStrategy::Weighted);
        assert_eq!(cfg.credential[0].label(), "K1");
        assert_eq!(cfg.credential[1].label(), "overflow");
        assert_eq!(cfg.credential[1].weight, 1);
    }

    #[test]
    fn rejects_a_pool_with_duplicate_credential_ids() {
        let toml = format!(
            r#"
{VALID}

[[credential]]
namespace = "platform"
provider = "openai"
env = "K1"
id = "same"

[[credential]]
namespace = "platform"
provider = "openai"
env = "K2"
id = "same"
"#
        );
        assert!(matches!(
            Config::from_toml_str(&toml),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn rejects_a_zero_weighted_credential() {
        let toml = format!(
            r#"
{VALID}

[[credential]]
namespace = "platform"
provider = "openai"
env = "K1"
weight = 0
"#
        );
        assert!(matches!(
            Config::from_toml_str(&toml),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn accepts_declared_usage_sinks_and_defaults_their_batching() {
        let cfg = Config::from_toml_str(&format!(
            r#"
{VALID}

[[usage_sink]]
kind = "postgres"
dsn_env = "AXOND_USAGE_POSTGRES_DSN"
table = "billing.axond_usage"
create_table = true
max_batch = 250

[[usage_sink]]
kind = "otlp"
"#
        ))
        .expect("valid sinks");
        assert_eq!(cfg.usage_sink[0].kind, UsageSinkKind::Postgres);
        assert_eq!(cfg.usage_sink[0].table(), "billing.axond_usage");
        assert_eq!(cfg.usage_sink[0].max_batch, 250);
        assert_eq!(cfg.usage_sink[0].buffer_capacity, default_buffer_capacity());
        assert_eq!(cfg.usage_sink[1].table(), "axond_usage");
    }

    #[test]
    fn no_usage_sink_is_the_no_datastore_default() {
        assert!(Config::from_toml_str(VALID).unwrap().usage_sink.is_empty());
    }

    #[test]
    fn rejects_a_postgres_sink_without_a_dsn_reference() {
        let toml = format!(
            r#"
{VALID}

[[usage_sink]]
kind = "postgres"
"#
        );
        assert!(matches!(
            Config::from_toml_str(&toml),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn rejects_a_table_name_that_is_not_a_bare_identifier() {
        let toml = format!(
            r#"
{VALID}

[[usage_sink]]
kind = "postgres"
dsn_env = "DSN"
table = "usage\"; drop table users --"
"#
        );
        assert!(matches!(
            Config::from_toml_str(&toml),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn rejects_zero_batch_size_or_buffer_capacity() {
        for bad in ["max_batch = 0", "buffer_capacity = 0"] {
            let toml = format!(
                r#"
{VALID}

[[usage_sink]]
kind = "postgres"
dsn_env = "DSN"
{bad}
"#
            );
            assert!(
                matches!(Config::from_toml_str(&toml), Err(ConfigError::Invalid(_))),
                "accepted `{bad}`"
            );
        }
    }

    #[test]
    fn ignores_batch_validation_for_non_batching_sinks() {
        let toml = format!(
            r#"
{VALID}

[[usage_sink]]
kind = "stdout"
buffer_capacity = 0
max_batch = 0
flush_interval_ms = 0

[[usage_sink]]
kind = "otlp"
buffer_capacity = 0
max_batch = 0
flush_interval_ms = 0
"#
        );
        Config::from_toml_str(&toml).expect("non-batching sinks ignore batch settings");
    }

    #[test]
    fn rejects_a_batch_larger_than_its_buffer() {
        let toml = format!(
            r#"
{VALID}

[[usage_sink]]
kind = "postgres"
dsn_env = "DSN"
buffer_capacity = 99
max_batch = 100
"#
        );
        let error = Config::from_toml_str(&toml).expect_err("batch must fit its buffer");
        assert!(
            error
                .to_string()
                .contains("max_batch (100) must not exceed buffer_capacity (99)")
        );
    }

    #[test]
    fn clamps_default_batch_to_a_small_buffer() {
        let toml = format!(
            r#"
{VALID}

[[usage_sink]]
kind = "postgres"
dsn_env = "DSN"
buffer_capacity = 100
"#
        );
        let cfg = Config::from_toml_str(&toml).expect("default batch should be clamped");
        let sink = &cfg.usage_sink[0];
        assert!(!sink.max_batch_explicit);
        assert_eq!(sink.max_batch, default_max_batch());
        assert_eq!(sink.batch_settings().max_batch, 100);
    }

    #[test]
    fn accepts_a_batch_larger_than_one_statement() {
        let toml = format!(
            r#"
{VALID}

[[usage_sink]]
kind = "postgres"
dsn_env = "DSN"
buffer_capacity = 100000
max_batch = 100000
"#
        );
        assert!(Config::from_toml_str(&toml).is_ok());
    }

    fn repository_file(relative: &str) -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(relative);
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()))
    }

    /// Turning one number down is the common tuning move, and it must not fail
    /// boot over the stock sub-ceilings the operator never wrote. A defaulted
    /// tenant ceiling that reaches the global one is turned off rather than
    /// clamped onto it, so the replica sheds at the gate that queues.
    ///
    /// A tenant ceiling the operator *lowers* under a lowered global one is
    /// honored, because that is a request for isolation rather than a default.
    #[test]
    fn a_lowered_global_ceiling_pulls_the_defaulted_sub_ceilings_down_with_it() {
        let config = Config::from_toml_str(&format!("{VALID}\n[admission]\nmax_in_flight = 16\n"))
            .expect("lowering only the global ceiling boots");
        assert_eq!(config.admission.max_in_flight, 16);
        assert_eq!(config.admission.max_in_flight_streams, 16);
        assert_eq!(
            config.admission.max_in_flight_per_tenant, 0,
            "a tenant ceiling at the global one isolates nothing and would shed with a 429"
        );
        assert!(!config.admission.max_in_flight_per_tenant_explicit);
        assert!(!config.admission.max_in_flight_streams_explicit);

        let written = Config::from_toml_str(&format!(
            "{VALID}\n[admission]\nmax_in_flight = 16\nmax_in_flight_per_tenant = 0\n"
        ))
        .expect("a written ceiling is honored, including the disabling zero");
        assert_eq!(written.admission.max_in_flight_per_tenant, 0);
        assert!(written.admission.max_in_flight_per_tenant_explicit);

        let isolated = Config::from_toml_str(&format!(
            "{VALID}\n[admission]\nmax_in_flight = 16\nmax_in_flight_per_tenant = 4\n"
        ))
        .expect("a tenant ceiling under the global one is isolation the operator asked for");
        assert_eq!(isolated.admission.max_in_flight_per_tenant, 4);

        let error = Config::from_toml_str(&format!(
            "{VALID}\n[admission]\nmax_in_flight = 16\nmax_in_flight_streams = 32\n"
        ))
        .expect_err("two written ceilings that contradict each other are a boot error");
        assert!(
            error
                .to_string()
                .contains("admission.max_in_flight_streams"),
            "{error}"
        );
    }

    /// Each ceiling becomes a semaphore, and a semaphore asserts above
    /// `MAX_PERMITS`. A refusal naming the key beats a panic naming nothing.
    #[test]
    fn rejects_a_ceiling_larger_than_a_semaphore_can_hold() {
        let absurd = MAX_PERMITS as u64 + u64::from(u32::MAX);
        for (key, extra) in [
            ("max_in_flight", String::new()),
            (
                "max_in_flight_streams",
                format!("max_in_flight = {MAX_PERMITS}\n"),
            ),
            ("queue_capacity", format!("max_in_flight = {MAX_PERMITS}\n")),
        ] {
            let toml = format!("{VALID}\n[admission]\n{extra}{key} = {absurd}\n");
            let error = Config::from_toml_str(&toml).expect_err("an absurd ceiling is refused");
            assert!(
                error.to_string().contains(&format!("admission.{key}")),
                "{error}"
            );
        }
    }

    /// The shipped configurations and fixtures must themselves name variables
    /// the override layer leaves alone, since an operator copies them verbatim.
    #[test]
    fn no_shipped_configuration_references_a_claimable_variable() {
        for relative in ["axond.example.toml", "tests/tier0/axond.tier0.toml"] {
            for line in repository_file(relative).lines() {
                let line = line.trim_start_matches('#').trim();
                let Some((key, value)) = line.split_once(" = ") else {
                    continue;
                };
                if !matches!(key, "env" | "dsn_env" | "kek_env" | "cache_key_env") {
                    continue;
                }
                let reference = value.trim().trim_matches('"');
                assert!(
                    reject_env_override_collision(key, reference).is_ok(),
                    "{relative}: `{key} = \"{reference}\"` would be claimed by the override layer"
                );
            }
        }
    }

    #[test]
    fn unpriced_models_defaults_to_deny() {
        let cfg = Config::from_toml_str(VALID).expect("valid");
        assert_eq!(cfg.provider[0].unpriced_models, UnpricedModels::Deny);
        let cfg = Config::from_toml_str(&VALID.replace(
            r#"base_url = "https://api.openai.com/v1""#,
            r#"base_url = "https://api.openai.com/v1"
unpriced_models = "allow""#,
        ))
        .expect("allow");
        assert_eq!(cfg.provider[0].unpriced_models, UnpricedModels::Allow);
    }

    /// The default is inert: a file that never mentions `[catalog]` imports
    /// nothing, reaches no network, and opens no connection. #146 adds a source
    /// an operator may enable, not a fetch every deployment starts performing.
    #[test]
    fn a_file_that_does_not_configure_a_catalogue_imports_none() {
        let config = Config::from_toml_str(VALID).expect("the stateless example still parses");
        assert_eq!(config.catalog.source, CatalogSourceBackend::None);
        assert!(
            !config.catalog.enabled(),
            "an unconfigured catalogue must not import"
        );
    }

    /// What a file is refused for, whether it is refused while loading or while
    /// its bounds are checked. Which of the two stages a rule lives in is an
    /// implementation detail to an operator reading the message.
    fn catalogue_refusal(toml: &str) -> String {
        match Config::from_toml_str(toml) {
            Err(error) => error.to_string(),
            Ok(config) => config
                .validate_process_local_bounds()
                .expect_err("the configuration was expected to be refused")
                .to_string(),
        }
    }

    /// A file that must be accepted, with its catalogue section.
    fn catalogue_config(toml: &str) -> Config {
        let config = Config::from_toml_str(toml).expect("the configuration must be accepted");
        config
            .validate_process_local_bounds()
            .expect("the configuration must be accepted");
        config
    }

    /// Every bound the background loop depends on is checked as a *set* at boot:
    /// a zero interval is a busy loop against an upstream, a zero timeout
    /// abandons every import instantly, and a backoff ceiling below its first
    /// delay never describes a retry.
    #[test]
    fn a_catalogue_bound_of_zero_is_refused_at_boot() {
        for field in [
            "refresh_interval_seconds",
            "refresh_timeout_seconds",
            "retry_initial_seconds",
            "retry_max_seconds",
            "connect_timeout_ms",
            "operation_timeout_ms",
            "max_payload_bytes",
        ] {
            let refusal = catalogue_refusal(&format!(
                "{VALID}\n[catalog]\nsource = \"models-dev\"\n{field} = 0\n"
            ));
            assert!(
                refusal.contains(field),
                "the refusal must name `{field}`, said: {refusal}"
            );
        }

        let refusal = catalogue_refusal(&format!(
            "{VALID}\n[catalog]\nsource = \"models-dev\"\nretry_initial_seconds = \
             600\nretry_max_seconds = 60\n"
        ));
        assert!(
            !refusal.is_empty(),
            "a backoff ceiling below its first delay is not a schedule"
        );
    }

    /// A URL the models.dev adapter does not recognise is refused where it is
    /// written rather than at the first refresh: an operator who typo'd the
    /// document path learns at boot, not from a stale catalogue six hours later.
    #[test]
    fn a_catalogue_source_url_is_checked_against_the_adapter() {
        let config = catalogue_config(&format!(
            "{VALID}\n[catalog]\nsource = \"models-dev\"\nsource_url = \
             \"https://models.dev/catalog.json\"\n"
        ));
        assert_eq!(config.catalog.url(), "https://models.dev/catalog.json");

        let refusal = catalogue_refusal(&format!(
            "{VALID}\n[catalog]\nsource = \"models-dev\"\nsource_url = \
             \"https://models.dev/nope\"\n"
        ));
        assert!(
            refusal.contains("catalog.json"),
            "the refusal must name the document that is supported, said: {refusal}"
        );

        // A source that reaches no network has no URL to configure, and silently
        // ignoring one would hide that the file's endpoint is not being used.
        let refusal = catalogue_refusal(&format!(
            "{VALID}\n[catalog]\nsource = \"seed\"\nsource_url = \
             \"https://models.dev/catalog.json\"\n"
        ));
        assert!(
            refusal.contains("source_url"),
            "`source_url` applies only to the models.dev source, said: {refusal}"
        );
    }

    /// A mirror is allowed; a downgradeable one is not. Imported metadata is
    /// what an operator reads to approve a price or enable a model later, so a
    /// plaintext source — whose document anyone on the path may substitute — is
    /// refused where it is written rather than trusted at every refresh.
    #[test]
    fn a_catalogue_source_url_must_be_https() {
        for rejected in [
            "http://models.dev/catalog.json",
            "http://internal.mirror.example/catalog.json",
            "http://127.0.0.1:8080/catalog.json",
        ] {
            let refusal = catalogue_refusal(&format!(
                "{VALID}\n[catalog]\nsource = \"models-dev\"\nsource_url = \"{rejected}\"\n"
            ));
            assert!(
                refusal.contains("https://"),
                "the refusal must say which transport is required, said: {refusal}"
            );
            assert!(
                !refusal.contains("must be at least"),
                "`{rejected}` must be refused for its transport, said: {refusal}"
            );
        }

        // An HTTPS mirror is a legitimate deployment choice: the rule is the
        // transport, not the host.
        let config = catalogue_config(&format!(
            "{VALID}\n[catalog]\nsource = \"models-dev\"\nsource_url = \
             \"https://mirror.internal.example/models.dev/catalog.json\"\n"
        ));
        assert_eq!(
            config.catalog.url(),
            "https://mirror.internal.example/models.dev/catalog.json"
        );
    }

    /// A source URL is operator configuration, not a place to carry a secret or
    /// an incomplete authority. Hosts are deliberately not allowlisted:
    /// deployments may use an HTTPS mirror in an air-gapped network.
    #[test]
    fn a_catalogue_source_url_must_have_a_host_without_credentials() {
        for rejected in [
            "https:///catalog.json",
            "https://user:secret@mirror.internal.example/catalog.json",
        ] {
            let refusal = match Config::from_toml_str(&format!(
                "{VALID}\n[catalog]\nsource = \"models-dev\"\nsource_url = \"{rejected}\"\n"
            )) {
                Err(error) => error.to_string(),
                Ok(_) => panic!("`{rejected}` was accepted as a catalogue source URL"),
            };
            assert!(
                refusal.contains("source_url"),
                "the refusal must identify the source URL, said: {refusal}"
            );
            assert!(
                !refusal.contains("secret"),
                "source URL credentials must not be echoed, said: {refusal}"
            );
        }

        let config = catalogue_config(&format!(
            "{VALID}\n[catalog]\nsource = \"models-dev\"\nsource_url = \
             \"https://127.0.0.1/catalog.json\"\n"
        ));
        assert_eq!(config.catalog.url(), "https://127.0.0.1/catalog.json");
    }

    #[test]
    fn accepts_a_well_formed_config() {
        let cfg = Config::from_toml_str(VALID).expect("valid config");
        assert_eq!(cfg.default_namespace(), "platform");
        assert!(cfg.price_for("openai", "gpt-4o").is_some());
    }
}
