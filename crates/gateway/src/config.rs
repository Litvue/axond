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

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::Duration;

use gateway_core::ModelPrice;
use gateway_transport::TransportLimits;
use serde::{Deserialize, Deserializer};

use crate::aliases::AliasScope;
use crate::principals::Capability;
use crate::usage::{BatchSettings, validate_table_name};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Which authority owns durable resources (ADR 0027). Omitting the key
    /// selects `stateless`, so every configuration written before the key
    /// existed keeps the meaning it had.
    #[serde(default)]
    pub mode: Mode,
    #[serde(default)]
    pub server: Server,
    /// Stateful bootstrap: the control-plane database's connection *reference*.
    /// Required by `mode = "stateful"`, rejected in stateless mode.
    #[serde(default)]
    pub control_plane: Option<ControlPlane>,
    /// Stateful bootstrap: which `SecretStore` unwraps tenant secret material,
    /// and the key-encryption key it unwraps with — both by reference.
    #[serde(default)]
    pub secret_store: Option<SecretStore>,
    /// Stateful bootstrap: the mandatory static `/admin/v1` breakglass operator
    /// credential, referenced the way `[[gateway_key]]` is.
    #[serde(default)]
    pub admin_breakglass: Vec<AdminBreakglass>,
    #[serde(default)]
    pub namespace: Vec<Namespace>,
    #[serde(default)]
    pub provider: Vec<Provider>,
    #[serde(default)]
    pub model: Vec<Model>,
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
    /// How the running config is replaced without a restart.
    #[serde(default)]
    pub reload: Reload,
    /// How termination is sequenced: how long readiness fails before admission
    /// closes, how long admitted requests then have, and the flush bound.
    #[serde(default)]
    pub shutdown: Shutdown,
    /// Inbound gateway keys. Each binds a secret (resolved from `env` or
    /// `file`) to a namespace. At least one is required: inbound authentication
    /// fails closed, so there is no keyless mode (ADR 0013).
    #[serde(default)]
    pub gateway_key: Vec<GatewayKey>,
    /// Token verification authority. Verifiers are additive to static gateway
    /// keys and require a deployment audience when any are configured.
    #[serde(default)]
    pub gateway_verifier: Vec<GatewayVerifier>,
    #[serde(default)]
    pub gateway_minting: Option<GatewayMinting>,
    /// Issuance epochs that invalidate older minted tokens without runtime
    /// revocation state.
    #[serde(default)]
    pub gateway_token_epoch: Vec<GatewayTokenEpoch>,
    #[serde(default)]
    pub gateway_token: Option<GatewayToken>,
    /// Where raw usage records go. Empty means the no-datastore default: one
    /// JSON line per record on stdout (ADR 0002).
    #[serde(default)]
    pub usage_sink: Vec<UsageSinkConfig>,
    /// Spend cap enforcement. Defaults to no budget at all, so nothing drags a
    /// datastore onto the default path (ADR 0002).
    #[serde(default)]
    pub budget: BudgetConfig,
    /// Inbound per-caller concurrency enforcement. Defaults to no limit.
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    /// Precise minted-token revocation. Defaults to no denylist.
    #[serde(default)]
    pub revocation: RevocationConfig,
}

/// Which authority owns durable resources for the whole process (ADR 0027).
///
/// The mode is process-wide and exclusive: there is no per-dimension or
/// per-namespace migration state, precisely so no merge policy between a file
/// and a database ever has to exist.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Mode {
    /// TOML plus the environment and files it references own every resource.
    /// The default, and what every configuration written so far means.
    #[default]
    Stateless,
    /// A durable Postgres control plane owns tenants, identities, providers,
    /// credentials, catalogues, prices, aliases, and policy; bootstrap TOML
    /// shrinks to what a process needs before it can read anything else.
    Stateful,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stateless => "stateless",
            Self::Stateful => "stateful",
        }
    }
}

/// The top-level keys the `AXOND_` environment layer can address, since
/// [`Config::load`] merges `Env::prefixed("AXOND_")` over the file.
///
/// A *secret-bearing* variable must not be named after one of them: the
/// override layer would merge its value as that key instead of leaving it for a
/// reference to resolve, and figment's resulting type error would carry the
/// secret into the load diagnostic. Kept in step with `Config` by
/// `the_override_key_list_matches_every_config_field`.
const OVERRIDE_KEYS: [&str; 22] = [
    "mode",
    "server",
    "control_plane",
    "secret_store",
    "admin_breakglass",
    "namespace",
    "provider",
    "model",
    "credential",
    "credential_pool",
    "failover",
    "transport",
    "reload",
    "gateway_key",
    "gateway_verifier",
    "gateway_minting",
    "gateway_token_epoch",
    "gateway_token",
    "usage_sink",
    "budget",
    "rate_limit",
    "revocation",
];

/// A reference is only a reference if the environment layer leaves it alone.
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

/// Connectivity to the durable control plane. A DSN is a secret, so — like
/// every other DSN in this file — it is named rather than inlined.
///
/// Parsing this section connects to nothing: the `ControlPlaneStore` that uses
/// it lands with #141.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ControlPlane {
    /// Name of the env var holding the control-plane Postgres connection string.
    #[serde(default)]
    pub dsn_env: Option<String>,
    /// Bound on establishing a control-plane connection.
    #[serde(default = "default_control_plane_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
}

fn default_control_plane_connect_timeout_ms() -> u64 {
    5_000
}

/// Which `SecretStore` unwraps tenant secret material, and the KEK it unwraps
/// with. Both the store's DSN and the KEK are references; no key material is
/// ever expressible here, so nothing in this struct is secret and `Debug` on it
/// leaks nothing.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SecretStore {
    #[serde(default)]
    pub backend: SecretStoreBackend,
    /// Name of the env var holding the secret store's connection string.
    /// Defaults to the control plane's own reference, since encrypted Postgres
    /// is normally the same database.
    #[serde(default)]
    pub dsn_env: Option<String>,
    /// Name of the env var holding the key-encryption key. Exactly one of
    /// `kek_env` and `kek_file` must be non-empty.
    #[serde(default)]
    pub kek_env: Option<String>,
    /// Path to a file holding the key-encryption key.
    #[serde(default)]
    pub kek_file: Option<String>,
}

/// Encrypted Postgres is the first — and, for now, only — `SecretStore`
/// implementation ADR 0027 approves. External managers are later adapters
/// behind the same contract, so this is an enum rather than a bool.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SecretStoreBackend {
    #[default]
    Postgres,
}

impl SecretStoreBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
        }
    }
}

impl SecretStore {
    /// The KEK's reference: which source names it, and the name itself. Only
    /// ever a name or a path — never material.
    pub fn kek_reference(&self) -> Option<(&'static str, &str)> {
        let env = self.kek_env.as_deref().unwrap_or("").trim();
        let file = self.kek_file.as_deref().unwrap_or("").trim();
        match (env.is_empty(), file.is_empty()) {
            (false, true) => Some(("kek_env", env)),
            (true, false) => Some(("kek_file", file)),
            _ => None,
        }
    }
}

/// The static breakglass operator credential for `/admin/v1`. It exists for
/// "the identity provider is down" and "the control plane rejected the last
/// change", which is why stateful mode requires one even though human
/// administration is OIDC.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AdminBreakglass {
    /// Name of the env var holding the credential. Exactly one of `env` and
    /// `file` must be non-empty.
    #[serde(default)]
    pub env: Option<String>,
    /// Path to a file holding the credential.
    #[serde(default)]
    pub file: Option<String>,
    /// Non-secret attribution label for audit events. Defaults to the source
    /// reference, which is a name, not a value.
    #[serde(default)]
    pub id: Option<String>,
}

impl AdminBreakglass {
    /// Which source names the credential, and the reference itself.
    pub fn source(&self) -> Option<(&'static str, &str)> {
        let env = self.env.as_deref().unwrap_or("").trim();
        let file = self.file.as_deref().unwrap_or("").trim();
        match (env.is_empty(), file.is_empty()) {
            (false, true) => Some(("env", env)),
            (true, false) => Some(("file", file)),
            _ => None,
        }
    }

    /// Non-secret label for diagnostics and audit events.
    pub fn label(&self) -> &str {
        self.id
            .as_deref()
            .filter(|id| !id.trim().is_empty())
            .or_else(|| self.source().map(|(_, reference)| reference))
            .unwrap_or("<unnamed>")
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

/// A caller-facing model name (alias) → an ordered list of concrete targets.
/// The name is what SDKs already send (`gpt-4o`); callers never need to know
/// the provider topology (assessment delta A2).
#[derive(Debug, Clone, Deserialize)]
pub struct Model {
    pub name: String,
    pub targets: Vec<Target>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Target {
    pub provider: String,
    /// Concrete upstream model / deployment id.
    pub model: String,
    /// Per-token pricing for this concrete target, in micro-dollars per million
    /// tokens. Required — a target that can't be priced can't be budget-checked,
    /// so an unpriced target fails config parsing at boot (delta B2).
    pub price: ModelPrice,
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
    pub env: String,
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
    pub fn label(&self) -> &str {
        self.id.as_deref().unwrap_or(self.env.as_str())
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
/// the phase bound below governs each phase. `stream_idle_timeout_ms` is the one
/// bound that applies *after* a stream opens, because a long answer is not a
/// stalled one: only silence between chunks is.
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

fn default_max_response_bytes() -> u64 {
    32 * 1024 * 1024
}

fn default_max_error_bytes() -> u64 {
    64 * 1024
}

/// Config hot-reload (ADR 0011). `SIGHUP` always reloads; watching the config
/// file is opt-in, since a watch reloads whatever the file says the moment it
/// says it, while a signal is an explicit operator action.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Reload {
    /// Reload when the config file's contents change.
    #[serde(default)]
    pub watch: bool,
    /// How often the watcher compares the file's contents. Also bounds how long
    /// a change to this section itself takes to be picked up.
    #[serde(default = "default_reload_poll_interval_ms")]
    pub poll_interval_ms: u64,
}

impl Default for Reload {
    fn default() -> Self {
        Self {
            watch: false,
            poll_interval_ms: default_reload_poll_interval_ms(),
        }
    }
}

fn default_reload_poll_interval_ms() -> u64 {
    2_000
}

/// Below this the watcher would spend more time reading the file than serving.
const MIN_RELOAD_POLL_INTERVAL_MS: u64 = 100;

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

fn default_buffer_capacity() -> usize {
    10_000
}

fn default_max_batch() -> usize {
    500
}

fn default_flush_interval_ms() -> u64 {
    1_000
}

/// The spend cap and the store that enforces it. `backend` decides which of the
/// remaining fields apply; they are validated as a set at boot, so a shared
/// backend without a DSN reference — or a cap of zero, which would deny every
/// request — refuses to start (ADR 0010).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct BudgetConfig {
    #[serde(default)]
    pub backend: BudgetBackend,
    /// The cap, in micro-dollars, per `(namespace, subject)`. Required by every
    /// backend but `none`.
    #[serde(default)]
    pub limit_microdollars: u64,
    /// An additional exact cap, in micro-dollars, on everything a *namespace*
    /// spends — every subject in it combined. Omitted by default, which keeps
    /// per-subject-only enforcement. Only the shared backends can enforce it
    /// exactly, so `none` and `in-memory` reject it at boot (ADR 0010).
    #[serde(default)]
    pub namespace_limit_microdollars: Option<u64>,
    /// What to do when the store cannot be reached. Fail-closed by default: an
    /// unenforceable cap denies rather than silently admitting.
    #[serde(default)]
    pub on_unavailable: StoreUnavailable,
    /// `redis` / `postgres`: name of the env var holding the connection string.
    /// The DSN is a secret, so it is referenced rather than inlined.
    #[serde(default)]
    pub dsn_env: Option<String>,
    /// `postgres`: base table name. The reservation table is
    /// `<table>_reservation`. Defaults to `axond_budget`, matching the shipped
    /// DDL.
    #[serde(default)]
    pub table: Option<String>,
    /// `postgres`: apply the shipped DDL at boot. Off by default — most
    /// deployments give the gateway's role no DDL rights.
    #[serde(default)]
    pub create_table: bool,
    /// `redis`: key namespace for budget state.
    #[serde(default)]
    pub key_prefix: Option<String>,
    /// How long a reservation is held before the store reclaims it. It bounds
    /// how long a replica that died mid-request holds budget it will never
    /// settle, so it should exceed the longest expected request.
    #[serde(default = "default_reservation_ttl_seconds")]
    pub reservation_ttl_seconds: u64,
    /// `in-memory`: remove unheld ledgers after this many idle seconds when
    /// the subject bound is reached. The in-memory cap is per-replica and
    /// approximate; exact shared enforcement uses Redis.
    #[serde(default = "default_idle_ttl_seconds")]
    pub idle_ttl_seconds: u64,
    /// `in-memory`: maximum number of `(namespace, subject)` ledgers retained.
    #[serde(default = "default_max_subjects")]
    pub max_subjects: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BudgetBackend {
    /// Always admit. The default: no cap, no datastore.
    #[default]
    None,
    /// Per-replica holds and counters; a fleet enforces per-replica ceilings.
    InMemory,
    /// Shared across replicas, atomic per key.
    Redis,
    Postgres,
}

impl BudgetBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::InMemory => "in-memory",
            Self::Redis => "redis",
            Self::Postgres => "postgres",
        }
    }

    /// Whether the backend keeps its state outside the process, and so needs a
    /// connection string and an unavailability stance.
    fn is_shared(self) -> bool {
        matches!(self, Self::Redis | Self::Postgres)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StoreUnavailable {
    #[default]
    Deny,
    Allow,
}

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            backend: BudgetBackend::None,
            limit_microdollars: 0,
            namespace_limit_microdollars: None,
            on_unavailable: StoreUnavailable::Deny,
            dsn_env: None,
            table: None,
            create_table: false,
            key_prefix: None,
            reservation_ttl_seconds: default_reservation_ttl_seconds(),
            idle_ttl_seconds: default_idle_ttl_seconds(),
            max_subjects: default_max_subjects(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RateLimitBackend {
    /// Always admit. The default: no cap, no state.
    #[default]
    None,
    /// Per-replica in-flight holds and counters.
    InMemory,
    /// Exact shared leases across replicas.
    Redis,
}

impl RateLimitBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::InMemory => "in-memory",
            Self::Redis => "redis",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RateLimitConfig {
    /// Selects the inbound concurrency backend.
    #[serde(default)]
    pub backend: RateLimitBackend,
    /// Maximum concurrent dispatches for one `(namespace, subject)` key.
    #[serde(default = "default_max_in_flight_per_subject")]
    pub max_in_flight_per_subject: usize,
    /// Maximum number of active caller keys retained by the in-memory backend.
    #[serde(default = "default_max_subjects")]
    pub max_subjects: usize,
    /// Name of the env var holding the Redis connection string.
    #[serde(default)]
    pub dsn_env: Option<String>,
    /// Redis key namespace.
    #[serde(default)]
    pub key_prefix: Option<String>,
    /// What to do when Redis cannot enforce the limit.
    #[serde(default)]
    pub on_unavailable: StoreUnavailable,
    /// How long an abandoned lease remains live.
    #[serde(default = "default_lease_ttl_seconds")]
    pub lease_ttl_seconds: u64,
    /// Bounded timeout for each Redis operation.
    #[serde(default = "default_rate_limit_timeout_ms")]
    pub timeout_ms: u64,
    /// Bounded timeout for the Redis connection and boot-time PING.
    #[serde(default = "default_rate_limit_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RevocationConfig {
    #[serde(default)]
    pub backend: RevocationBackend,
    #[serde(default)]
    pub dsn_env: Option<String>,
    #[serde(default)]
    pub key_prefix: Option<String>,
    #[serde(default)]
    pub table: Option<String>,
    #[serde(default)]
    pub create_table: bool,
    #[serde(default)]
    pub on_unavailable: StoreUnavailable,
    #[serde(default = "default_revocation_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_revocation_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RevocationBackend {
    #[default]
    None,
    Redis,
    Postgres,
}

impl RevocationBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Redis => "redis",
            Self::Postgres => "postgres",
        }
    }
}

impl Default for RevocationConfig {
    fn default() -> Self {
        Self {
            backend: RevocationBackend::None,
            dsn_env: None,
            key_prefix: None,
            table: None,
            create_table: false,
            on_unavailable: StoreUnavailable::Deny,
            timeout_ms: default_revocation_timeout_ms(),
            connect_timeout_ms: default_revocation_connect_timeout_ms(),
        }
    }
}

fn default_revocation_timeout_ms() -> u64 {
    250
}

fn default_revocation_connect_timeout_ms() -> u64 {
    5_000
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            backend: RateLimitBackend::None,
            max_in_flight_per_subject: default_max_in_flight_per_subject(),
            max_subjects: default_max_subjects(),
            dsn_env: None,
            key_prefix: None,
            on_unavailable: StoreUnavailable::Deny,
            lease_ttl_seconds: default_lease_ttl_seconds(),
            timeout_ms: default_rate_limit_timeout_ms(),
            connect_timeout_ms: default_rate_limit_connect_timeout_ms(),
        }
    }
}

fn default_max_in_flight_per_subject() -> usize {
    16
}

fn default_lease_ttl_seconds() -> u64 {
    300
}

fn default_rate_limit_timeout_ms() -> u64 {
    250
}

fn default_rate_limit_connect_timeout_ms() -> u64 {
    5_000
}

impl BudgetConfig {
    pub fn table(&self) -> String {
        self.table
            .clone()
            .unwrap_or_else(|| DEFAULT_BUDGET_TABLE.to_owned())
    }

    pub fn key_prefix(&self) -> String {
        self.key_prefix
            .clone()
            .unwrap_or_else(|| DEFAULT_BUDGET_KEY_PREFIX.to_owned())
    }
}

impl RateLimitConfig {
    pub fn key_prefix(&self) -> String {
        self.key_prefix
            .clone()
            .unwrap_or_else(|| DEFAULT_RATE_LIMIT_KEY_PREFIX.to_owned())
    }
}

impl RevocationConfig {
    pub fn key_prefix(&self) -> String {
        self.key_prefix
            .clone()
            .unwrap_or_else(|| DEFAULT_REVOCATION_KEY_PREFIX.to_owned())
    }
}

const DEFAULT_BUDGET_TABLE: &str = "axond_budget";
const DEFAULT_BUDGET_KEY_PREFIX: &str = "axond:budget";
const DEFAULT_RATE_LIMIT_KEY_PREFIX: &str = "axond:rate_limit";
const DEFAULT_REVOCATION_KEY_PREFIX: &str = "axond:revocation";

fn default_reservation_ttl_seconds() -> u64 {
    300
}

fn default_idle_ttl_seconds() -> u64 {
    60 * 60
}

fn default_max_subjects() -> usize {
    10_000
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
    #[serde(default)]
    pub can_mint: bool,
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

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayToken {
    #[serde(deserialize_with = "deserialize_gateway_audience")]
    pub audience: String,
}

fn deserialize_gateway_audience<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(String::deserialize(deserializer)?.trim().to_owned())
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayTokenEpoch {
    pub namespace: String,
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(deserialize_with = "deserialize_gateway_min_iat")]
    pub min_iat: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum GatewayVerifierAlgorithm {
    #[serde(rename = "EdDSA")]
    EdDsa,
    #[serde(rename = "HS256")]
    Hs256,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayVerifier {
    pub kid: String,
    pub alg: GatewayVerifierAlgorithm,
    #[serde(default)]
    pub env: Option<String>,
    #[serde(default)]
    pub file: Option<String>,
    pub namespaces: Vec<String>,
    #[serde(deserialize_with = "deserialize_gateway_ttl")]
    pub max_ttl: Duration,
}

impl GatewayVerifier {
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

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayMinting {
    pub kid: String,
    #[serde(default)]
    pub env: Option<String>,
    #[serde(default)]
    pub file: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_gateway_ttl")]
    pub max_ttl: Option<Duration>,
    #[serde(default)]
    pub scope: Option<Vec<String>>,
    #[serde(default)]
    pub aliases: Option<Vec<String>>,
    #[serde(default)]
    pub max_request_microdollars: Option<u64>,
}

impl GatewayMinting {
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

fn deserialize_optional_gateway_ttl<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Some(deserialize_gateway_ttl(deserializer)?))
}

// Deliberate policy ceiling for configured token lifetimes, not a protocol limit.
pub(crate) const MAX_GATEWAY_VERIFIER_TTL_SECONDS: u64 = 24 * 60 * 60;

fn deserialize_gateway_ttl<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    let seconds = match value {
        serde_json::Value::Number(number) => number
            .as_u64()
            .ok_or_else(|| serde::de::Error::custom("max_ttl must be a positive number"))?,
        serde_json::Value::String(text) => {
            parse_gateway_ttl(&text).map_err(serde::de::Error::custom)?
        }
        _ => {
            return Err(serde::de::Error::custom(
                "max_ttl must be a duration such as `15m`",
            ));
        }
    };
    if seconds == 0 || seconds > MAX_GATEWAY_VERIFIER_TTL_SECONDS {
        return Err(serde::de::Error::custom(format!(
            "max_ttl must be between 1s and {MAX_GATEWAY_VERIFIER_TTL_SECONDS}s"
        )));
    }
    Ok(Duration::from_secs(seconds))
}

fn parse_gateway_ttl(value: &str) -> Result<u64, String> {
    let value = value.trim();
    let (number, multiplier) = if let Some(number) = value.strip_suffix('s') {
        (number, 1)
    } else if let Some(number) = value.strip_suffix('m') {
        (number, 60)
    } else if let Some(number) = value.strip_suffix('h') {
        (number, 60 * 60)
    } else if let Some(number) = value.strip_suffix('d') {
        (number, 24 * 60 * 60)
    } else {
        (value, 1)
    };
    let number = number
        .parse::<u64>()
        .map_err(|_| "max_ttl must be a duration such as `15m`".to_owned())?;
    number
        .checked_mul(multiplier)
        .ok_or_else(|| "max_ttl is too large".to_owned())
}

fn deserialize_gateway_min_iat<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Number(number) => number.as_u64().ok_or_else(|| {
            serde::de::Error::custom("min_iat must be a non-negative unix-seconds integer")
        }),
        serde_json::Value::String(text) => {
            parse_gateway_rfc3339_utc(&text).map_err(serde::de::Error::custom)
        }
        _ => Err(serde::de::Error::custom(
            "min_iat must be a unix-seconds integer or an RFC 3339 UTC timestamp",
        )),
    }
}

pub(crate) fn parse_gateway_rfc3339_utc(value: &str) -> Result<u64, String> {
    let value = value.trim();
    let value = value
        .strip_suffix('Z')
        .ok_or_else(|| "min_iat must be an RFC 3339 UTC timestamp ending in `Z`".to_owned())?;
    let (date_time, fraction) = match value.split_once('.') {
        Some((date_time, fraction)) => (date_time, Some(fraction)),
        None => (value, None),
    };
    let bytes = date_time.as_bytes();
    if bytes.len() != 19
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || !bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| [4, 7, 10, 13, 16].contains(&index) || byte.is_ascii_digit())
    {
        return Err(
            "min_iat must be an RFC 3339 UTC timestamp such as `2026-08-10T12:00:00Z`".into(),
        );
    }
    if let Some(fraction) = fraction
        && (fraction.is_empty() || !fraction.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return Err("min_iat fractional seconds must contain only digits".into());
    }
    let number = |start, end| {
        date_time[start..end]
            .parse::<u32>()
            .expect("validated timestamp digits")
    };
    let year = number(0, 4);
    let month = number(5, 7);
    let day = number(8, 10);
    let hour = number(11, 13);
    let minute = number(14, 16);
    let second = number(17, 19);
    if year == 0 || !(1..=12).contains(&month) || hour > 23 || minute > 59 || second > 59 {
        return Err("min_iat is not a valid UTC instant".into());
    }
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days_in_month = match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if day == 0 || day > days_in_month {
        return Err("min_iat is not a valid UTC instant".into());
    }
    let days = days_from_civil(year as i64, month as i64, day as i64);
    let seconds = days
        .checked_mul(86_400)
        .and_then(|seconds| {
            seconds.checked_add(hour as i64 * 3_600 + minute as i64 * 60 + second as i64)
        })
        .ok_or_else(|| "min_iat is outside the supported unix timestamp range".to_owned())?;
    u64::try_from(seconds).map_err(|_| "min_iat must not be before the unix epoch".into())
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = (if year >= 0 { year } else { year - 399 }) / 400;
    let year_of_era = year - era * 400;
    let month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146097 + day_of_era - 719468
}

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
    /// (delta B2). Which checks apply depends on which authority owns durable
    /// resources: the mode is the first thing read, and each mode rejects the
    /// other's sections outright rather than merging them (ADR 0027).
    pub fn validate(&self) -> Result<(), ConfigError> {
        match self.mode {
            Mode::Stateless => self.validate_stateless(),
            Mode::Stateful => self.validate_stateful(),
        }
    }

    /// The whole-graph gate as it has always been: exactly one default
    /// namespace, aliases point at defined providers, credentials/keys reference
    /// defined namespaces. Nothing here is tightened by ADR 0027 — a
    /// configuration that booted before `mode` existed takes exactly this path.
    fn validate_stateless(&self) -> Result<(), ConfigError> {
        self.reject_stateful_bootstrap()?;
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

        for model in &self.model {
            if model.targets.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "model `{}` has no targets",
                    model.name
                )));
            }
            for t in &model.targets {
                if !providers.contains_key(t.provider.as_str()) {
                    return Err(ConfigError::Invalid(format!(
                        "model `{}` targets undefined provider `{}`",
                        model.name, t.provider
                    )));
                }
            }
            let first = &model.targets[0];
            let first_provider = providers[first.provider.as_str()];
            let first_wire = first_provider.kind.wire();
            for target in model.targets.iter().skip(1) {
                let provider = providers[target.provider.as_str()];
                let wire = provider.kind.wire();
                if wire != first_wire {
                    return Err(ConfigError::Invalid(format!(
                        "model `{}` has incompatible failover targets: provider `{}` uses {} wire, \
                         but provider `{}` uses {} wire; no route can serve such an alias",
                        model.name, first.provider, first_wire, provider.id, wire
                    )));
                }
            }
        }
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
            if c.env.trim().is_empty() {
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
        self.validate_gateway_verifiers(&namespaces)?;
        self.validate_gateway_minting(&namespaces)?;
        self.validate_gateway_token_epochs(&namespaces)?;
        self.validate_usage_sinks()?;
        self.validate_budget()?;
        self.validate_rate_limit()?;
        self.validate_revocation()?;
        Ok(())
    }

    /// Stateful mode's gate. It **rejects** rather than reconciles: a
    /// stateful-owned section in the file is a boot error before the listener
    /// binds, because the alternative is a merge policy between two disagreeing
    /// authorities (ADR 0027). Nothing here connects to anything — the
    /// `ControlPlaneStore` and `SecretStore` that consume these references land
    /// with #163 and #141.
    fn validate_stateful(&self) -> Result<(), ConfigError> {
        self.reject_stateful_owned_sections()?;
        self.validate_control_plane()?;
        self.validate_secret_store()?;
        self.validate_admin_breakglass()?;
        self.validate_process_local_bounds()?;
        self.validate_usage_sinks()?;
        self.validate_hot_state_connectivity()?;
        self.validate_revocation()?;
        Ok(())
    }

    /// Bounds the process applies to itself, in both modes: per-phase upstream
    /// limits, how often the file is re-read, and how long termination may take.
    /// They are process-local serving parameters rather than durable resources,
    /// so the control plane does not own them.
    fn validate_process_local_bounds(&self) -> Result<(), ConfigError> {
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
        if self.reload.poll_interval_ms < MIN_RELOAD_POLL_INTERVAL_MS {
            return Err(ConfigError::Invalid(format!(
                "reload.poll_interval_ms must be at least {MIN_RELOAD_POLL_INTERVAL_MS}"
            )));
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
        Ok(())
    }

    /// Stateful bootstrap sections describe a control plane stateless mode does
    /// not read, so their presence there is a mistake — most often a `mode` key
    /// that was never added — rather than harmless extra.
    fn reject_stateful_bootstrap(&self) -> Result<(), ConfigError> {
        let mut sections = Vec::new();
        if self.control_plane.is_some() {
            sections.push("`[control_plane]`");
        }
        if self.secret_store.is_some() {
            sections.push("`[secret_store]`");
        }
        if !self.admin_breakglass.is_empty() {
            sections.push("`[[admin_breakglass]]`");
        }
        if sections.is_empty() {
            return Ok(());
        }
        Err(ConfigError::Invalid(format!(
            "{} {} stateful bootstrap, which stateless mode never reads: add `mode = \"stateful\"` \
             to select the control plane, or remove the section",
            sections.join(", "),
            if sections.len() == 1 { "is" } else { "are" },
        )))
    }

    /// Every section whose resources the control plane owns, named in one
    /// error: an operator mid-cutover wants the whole list, not the first
    /// offender re-discovered one restart at a time.
    fn reject_stateful_owned_sections(&self) -> Result<(), ConfigError> {
        let mut sections: Vec<&'static str> = Vec::new();
        if !self.namespace.is_empty() {
            sections.push("`[[namespace]]`");
        }
        if !self.provider.is_empty() {
            sections.push("`[[provider]]`");
        }
        if !self.model.is_empty() {
            sections.push("`[[model]]`");
        }
        if !self.credential.is_empty() {
            sections.push("`[[credential]]`");
        }
        if self.credential_pool != CredentialPool::default() {
            sections.push("`[credential_pool]`");
        }
        if self.failover != Failover::default() {
            sections.push("`[failover]`");
        }
        if !self.gateway_key.is_empty() {
            sections.push("`[[gateway_key]]`");
        }
        if !self.gateway_verifier.is_empty() {
            sections.push("`[[gateway_verifier]]`");
        }
        if self.gateway_minting.is_some() {
            sections.push("`[gateway_minting]`");
        }
        if self.gateway_token.is_some() {
            sections.push("`[gateway_token]`");
        }
        if !self.gateway_token_epoch.is_empty() {
            sections.push("`[[gateway_token_epoch]]`");
        }
        sections.extend(self.stateful_owned_policy_keys());
        if sections.is_empty() {
            return Ok(());
        }
        Err(ConfigError::Invalid(format!(
            "`mode = \"stateful\"` gives the control plane exclusive authority over these, so they \
             cannot also be declared in TOML: {}. Import them through `/admin/v1` and publish a \
             revision instead; bootstrap TOML carries `mode`, `[server]`, `[transport]`, \
             `[reload]`, telemetry (`[[usage_sink]]`), `[control_plane]`, `[secret_store]`, \
             `[[admin_breakglass]]`, and backend selection plus DSN references for the opt-in \
             `[budget]`, `[rate_limit]`, and `[revocation]` backends",
            sections.join(", ")
        )))
    }

    /// Bootstrap owns *connectivity* to the opt-in enforcement backends; the
    /// control plane owns their *policy values*. A limit, window, or scope in
    /// bootstrap TOML is therefore the same split-brain error as an alias.
    ///
    /// Presence is inferred by comparing against the documented defaults, so a
    /// section that merely restates a default is indistinguishable from an
    /// absent one — and means the same thing either way.
    fn stateful_owned_policy_keys(&self) -> Vec<&'static str> {
        let budget = BudgetConfig::default();
        let rate_limit = RateLimitConfig::default();
        let mut keys = Vec::new();
        if self.budget.limit_microdollars != budget.limit_microdollars {
            keys.push("`[budget] limit_microdollars`");
        }
        if self.budget.namespace_limit_microdollars.is_some() {
            keys.push("`[budget] namespace_limit_microdollars`");
        }
        if self.budget.reservation_ttl_seconds != budget.reservation_ttl_seconds {
            keys.push("`[budget] reservation_ttl_seconds`");
        }
        if self.budget.idle_ttl_seconds != budget.idle_ttl_seconds {
            keys.push("`[budget] idle_ttl_seconds`");
        }
        if self.budget.max_subjects != budget.max_subjects {
            keys.push("`[budget] max_subjects`");
        }
        if self.rate_limit.max_in_flight_per_subject != rate_limit.max_in_flight_per_subject {
            keys.push("`[rate_limit] max_in_flight_per_subject`");
        }
        if self.rate_limit.max_subjects != rate_limit.max_subjects {
            keys.push("`[rate_limit] max_subjects`");
        }
        if self.rate_limit.lease_ttl_seconds != rate_limit.lease_ttl_seconds {
            keys.push("`[rate_limit] lease_ttl_seconds`");
        }
        keys
    }

    /// A stateful process with no control-plane reference has nothing to serve:
    /// initial cold boot requires Postgres, and it fails loudly rather than
    /// serving an empty configuration (ADR 0027).
    fn validate_control_plane(&self) -> Result<(), ConfigError> {
        let Some(control_plane) = &self.control_plane else {
            return Err(ConfigError::Invalid(
                "`mode = \"stateful\"` requires a `[control_plane]` section: the control plane owns \
                 every durable resource, so a stateful process with no control-plane reference has \
                 nothing to serve"
                    .into(),
            ));
        };
        if !control_plane
            .dsn_env
            .as_deref()
            .is_some_and(|dsn_env| !dsn_env.trim().is_empty())
        {
            return Err(ConfigError::Invalid(
                "`[control_plane] dsn_env` must name the env var holding the control-plane Postgres \
                 connection string"
                    .into(),
            ));
        }
        if control_plane.connect_timeout_ms == 0 {
            return Err(ConfigError::Invalid(
                "`[control_plane] connect_timeout_ms` must be at least 1".into(),
            ));
        }
        reject_env_override_collision(
            "[control_plane] dsn_env",
            control_plane.dsn_env.as_deref().unwrap_or_default().trim(),
        )?;
        Ok(())
    }

    /// Secret material is resolved during snapshot compilation, so a stateful
    /// process needs a store and a KEK *reference* before it can compile
    /// anything. Both are named here; neither is ever a value.
    fn validate_secret_store(&self) -> Result<(), ConfigError> {
        let Some(secret_store) = &self.secret_store else {
            return Err(ConfigError::Invalid(
                "`mode = \"stateful\"` requires a `[secret_store]` section: a snapshot is only \
                 publishable once every credential reference it needs is already unwrapped into \
                 memory, which needs a store and a key-encryption key"
                    .into(),
            ));
        };
        if secret_store.kek_reference().is_none() {
            return Err(ConfigError::Invalid(
                "`[secret_store]` must declare exactly one non-empty key-encryption-key reference \
                 (`kek_env` or `kek_file`); wrapped material cannot be unwrapped without it"
                    .into(),
            ));
        }
        let own_dsn = secret_store
            .dsn_env
            .as_deref()
            .map(str::trim)
            .filter(|dsn_env| !dsn_env.is_empty());
        let inherited_dsn = self
            .control_plane
            .as_ref()
            .and_then(|control_plane| control_plane.dsn_env.as_deref())
            .map(str::trim)
            .filter(|dsn_env| !dsn_env.is_empty());
        let (kek_key, kek_reference) = secret_store
            .kek_reference()
            .expect("a missing KEK reference is rejected above");
        if kek_key == "kek_env" {
            reject_env_override_collision("[secret_store] kek_env", kek_reference)?;
        }
        if let Some(dsn_env) = own_dsn {
            reject_env_override_collision("[secret_store] dsn_env", dsn_env)?;
        }
        if own_dsn.is_none() && inherited_dsn.is_none() {
            return Err(ConfigError::Invalid(format!(
                "`[secret_store] dsn_env` must name the env var holding the `{}` secret-store \
                 connection string, or `[control_plane] dsn_env` must supply one to inherit",
                secret_store.backend.as_str()
            )));
        }
        Ok(())
    }

    /// The breakglass credential is mandatory because it is the only way in when
    /// the identity provider is down or the control plane rejected the last
    /// change (ADR 0027). More than one would make "which operator acted" a
    /// guess, so exactly one is permitted.
    fn validate_admin_breakglass(&self) -> Result<(), ConfigError> {
        match self.admin_breakglass.len() {
            0 => {
                return Err(ConfigError::Invalid(
                    "`mode = \"stateful\"` requires one `[[admin_breakglass]]`: human `/admin/v1` \
                     identity is OIDC, and the static breakglass credential is what remains when \
                     the identity provider is unavailable"
                        .into(),
                ));
            }
            1 => {}
            found => {
                return Err(ConfigError::Invalid(format!(
                    "exactly one `[[admin_breakglass]]` is permitted (found {found}): a second \
                     breakglass identity makes an audited operator action ambiguous"
                )));
            }
        }
        let breakglass = &self.admin_breakglass[0];
        let Some((source, reference)) = breakglass.source() else {
            return Err(ConfigError::Invalid(format!(
                "admin_breakglass `{}` must declare exactly one non-empty source (`env` or `file`)",
                breakglass.label()
            )));
        };
        if source == "env" {
            reject_env_override_collision("[[admin_breakglass]] env", reference)?;
        }
        Ok(())
    }

    /// In stateful mode the opt-in admission backends keep their *connectivity*
    /// in bootstrap and take their policy from the control plane, so only the
    /// reference-shaped checks of [`Config::validate_budget`] and
    /// [`Config::validate_rate_limit`] apply.
    fn validate_hot_state_connectivity(&self) -> Result<(), ConfigError> {
        let budget = &self.budget;
        if budget.backend.is_shared()
            && !budget
                .dsn_env
                .as_deref()
                .is_some_and(|dsn_env| !dsn_env.trim().is_empty())
        {
            return Err(ConfigError::Invalid(format!(
                "budget `{}`: `dsn_env` must name the env var holding the connection string",
                budget.backend.as_str()
            )));
        }
        if budget.backend == BudgetBackend::Postgres {
            validate_table_name(&budget.table())
                .map_err(|message| ConfigError::Invalid(format!("budget `postgres`: {message}")))?;
        }
        let rate_limit = &self.rate_limit;
        if rate_limit.backend == RateLimitBackend::Redis {
            if rate_limit.timeout_ms == 0 {
                return Err(ConfigError::Invalid(
                    "rate_limit `redis`: timeout_ms must be at least 1".into(),
                ));
            }
            if rate_limit.connect_timeout_ms == 0 {
                return Err(ConfigError::Invalid(
                    "rate_limit `redis`: connect_timeout_ms must be at least 1".into(),
                ));
            }
            let has_rate_limit_dsn = rate_limit
                .dsn_env
                .as_deref()
                .is_some_and(|name| !name.trim().is_empty());
            let has_budget_fallback = self.budget.backend == BudgetBackend::Redis
                && self
                    .budget
                    .dsn_env
                    .as_deref()
                    .is_some_and(|name| !name.trim().is_empty());
            if !has_rate_limit_dsn && !has_budget_fallback {
                return Err(ConfigError::Invalid(
                    "rate_limit `redis`: `dsn_env` must name the env var holding the connection \
                     string (or use the Redis budget `dsn_env` fallback)"
                        .into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_gateway_token_epochs(
        &self,
        namespaces: &HashMap<&str, &Namespace>,
    ) -> Result<(), ConfigError> {
        let mut entries = HashMap::new();
        for epoch in &self.gateway_token_epoch {
            if !namespaces.contains_key(epoch.namespace.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "gateway_token_epoch references undefined namespace `{}`",
                    epoch.namespace
                )));
            }
            if epoch
                .subject
                .as_deref()
                .is_some_and(|subject| subject.trim().is_empty())
            {
                return Err(ConfigError::Invalid(format!(
                    "gateway_token_epoch subject for namespace `{}` must not be empty",
                    epoch.namespace
                )));
            }
            let subject = epoch.subject.as_deref().unwrap_or("");
            if entries
                .insert((epoch.namespace.as_str(), subject), ())
                .is_some()
            {
                return Err(ConfigError::Invalid(format!(
                    "duplicate gateway_token_epoch for namespace `{}` subject `{}`",
                    epoch.namespace, subject
                )));
            }
        }
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
                "at least one `[[gateway_key]]` is required: inbound authentication fails closed and there is no keyless mode"
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

    fn validate_gateway_verifiers(
        &self,
        namespaces: &HashMap<&str, &Namespace>,
    ) -> Result<(), ConfigError> {
        if self.gateway_verifier.is_empty() {
            return Ok(());
        }
        let audience = self
            .gateway_token
            .as_ref()
            .map(|token| token.audience.trim())
            .filter(|audience| !audience.is_empty())
            .ok_or_else(|| {
                ConfigError::Invalid(
                    "`[gateway_token] audience` is required when gateway verifiers are declared"
                        .into(),
                )
            })?;
        let _ = audience;
        let mut kids = HashMap::new();
        for verifier in &self.gateway_verifier {
            if verifier.kid.trim().is_empty() {
                return Err(ConfigError::Invalid(
                    "gateway_verifier `kid` must not be empty".into(),
                ));
            }
            let env = verifier.env.as_deref().unwrap_or("");
            let file = verifier.file.as_deref().unwrap_or("");
            if !env.trim().is_empty() && !file.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "gateway_verifier `{}` declares both `env` and `file`; exactly one source is permitted",
                    verifier.kid
                )));
            }
            if env.trim().is_empty() && file.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "gateway_verifier `{}` must declare exactly one non-empty source (`env` or `file`)",
                    verifier.kid
                )));
            }
            if kids.insert(verifier.kid.as_str(), ()).is_some() {
                return Err(ConfigError::Invalid(format!(
                    "duplicate gateway_verifier kid `{}`",
                    verifier.kid
                )));
            }
            if verifier.namespaces.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "gateway_verifier `{}` must permit at least one namespace",
                    verifier.kid
                )));
            }
            for namespace in &verifier.namespaces {
                if !namespaces.contains_key(namespace.as_str()) {
                    return Err(ConfigError::Invalid(format!(
                        "gateway_verifier `{}` references undefined namespace `{namespace}`",
                        verifier.kid
                    )));
                }
            }
        }
        Ok(())
    }

    fn validate_gateway_minting(
        &self,
        namespaces: &HashMap<&str, &Namespace>,
    ) -> Result<(), ConfigError> {
        let Some(minting) = &self.gateway_minting else {
            let inert_keys = self
                .gateway_key
                .iter()
                .filter(|key| key.can_mint)
                .map(|key| key.source_label().unwrap_or("<unknown>").to_owned())
                .collect::<Vec<_>>();
            if !inert_keys.is_empty() {
                tracing::warn!(
                    keys = ?inert_keys,
                    "`can_mint = true` is inert because `[gateway_minting]` is absent"
                );
            }
            return Ok(());
        };
        if minting.kid.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "gateway_minting `kid` must not be empty".into(),
            ));
        }
        if minting.source().is_none() {
            return Err(ConfigError::Invalid(
                "gateway_minting must declare exactly one non-empty source (`env` or `file`)"
                    .into(),
            ));
        }
        let verifier = self
            .gateway_verifier
            .iter()
            .find(|verifier| verifier.kid == minting.kid)
            .ok_or_else(|| {
                ConfigError::Invalid(format!(
                    "gateway_minting references unknown gateway_verifier kid `{}`",
                    minting.kid
                ))
            })?;
        if minting
            .max_ttl
            .is_some_and(|max_ttl| max_ttl > verifier.max_ttl)
        {
            return Err(ConfigError::Invalid(format!(
                "gateway_minting max_ttl exceeds verifier `{}` max_ttl",
                verifier.kid
            )));
        }
        if minting.max_request_microdollars == Some(0) {
            return Err(ConfigError::Invalid(
                "gateway_minting max_request_microdollars must be at least 1".into(),
            ));
        }
        if let Some(scope) = &minting.scope {
            if scope.is_empty() {
                return Err(ConfigError::Invalid(
                    "gateway_minting scope must contain at least one capability".into(),
                ));
            }
            for value in scope {
                if Capability::parse(value).is_none() {
                    return Err(ConfigError::Invalid(format!(
                        "gateway_minting scope contains unknown capability `{value}`"
                    )));
                }
            }
            if let Some(capability) = scope.iter().find_map(|value| {
                Capability::parse(value).filter(|capability| capability.is_operator_only())
            }) {
                return Err(ConfigError::Invalid(format!(
                    "gateway_minting scope capability `{capability}` can never be minted"
                )));
            }
        }
        if let Some(aliases) = &minting.aliases {
            if aliases.is_empty() {
                return Err(ConfigError::Invalid(
                    "gateway_minting aliases must contain at least one pattern".into(),
                ));
            }
            AliasScope::parse(aliases.iter().map(String::as_str)).map_err(|error| {
                ConfigError::Invalid(format!("gateway_minting aliases: {error}"))
            })?;
        }
        for key in self.gateway_key.iter().filter(|key| key.can_mint) {
            if !namespaces.contains_key(key.namespace.as_str()) {
                continue;
            }
            if !verifier.namespaces.iter().any(|ns| ns == &key.namespace) {
                return Err(ConfigError::Invalid(format!(
                    "gateway_key namespace `{}` with can_mint is not permitted by verifier `{}`",
                    key.namespace, verifier.kid
                )));
            }
        }
        Ok(())
    }

    /// A budget's fields only make sense together: a cap of zero would deny
    /// every request, and a shared backend without a DSN reference cannot
    /// enforce anything.
    fn validate_budget(&self) -> Result<(), ConfigError> {
        let budget = &self.budget;
        let backend = budget.backend.as_str();
        // Checked before the `none` short-circuit: a namespace cap on a backend
        // that cannot enforce it exactly is a boot failure, not a no-op.
        match budget.namespace_limit_microdollars {
            Some(_) if !budget.backend.is_shared() => {
                return Err(ConfigError::Invalid(format!(
                    "budget `{backend}`: namespace_limit_microdollars is supported only by `redis` and `postgres`, which enforce it exactly across replicas"
                )));
            }
            Some(0) => {
                return Err(ConfigError::Invalid(format!(
                    "budget `{backend}`: namespace_limit_microdollars must be at least 1"
                )));
            }
            _ => {}
        }
        if budget.backend == BudgetBackend::None {
            return Ok(());
        }
        if budget.limit_microdollars == 0 {
            return Err(ConfigError::Invalid(format!(
                "budget `{backend}`: limit_microdollars must be at least 1"
            )));
        }
        if budget.reservation_ttl_seconds == 0 {
            return Err(ConfigError::Invalid(format!(
                "budget `{backend}`: reservation_ttl_seconds must be at least 1"
            )));
        }
        if budget.idle_ttl_seconds == 0 {
            return Err(ConfigError::Invalid(format!(
                "budget `{backend}`: idle_ttl_seconds must be at least 1"
            )));
        }
        if budget.max_subjects == 0 {
            return Err(ConfigError::Invalid(format!(
                "budget `{backend}`: max_subjects must be at least 1"
            )));
        }
        if budget.backend.is_shared()
            && !budget
                .dsn_env
                .as_deref()
                .is_some_and(|dsn_env| !dsn_env.trim().is_empty())
        {
            return Err(ConfigError::Invalid(format!(
                "budget `{backend}`: `dsn_env` must name the env var holding the connection string"
            )));
        }
        if budget.backend == BudgetBackend::Postgres {
            validate_table_name(&budget.table())
                .map_err(|message| ConfigError::Invalid(format!("budget `postgres`: {message}")))?;
        }
        Ok(())
    }

    fn validate_rate_limit(&self) -> Result<(), ConfigError> {
        let rate_limit = &self.rate_limit;
        if rate_limit.backend == RateLimitBackend::None {
            return Ok(());
        }
        if rate_limit.max_in_flight_per_subject == 0 {
            return Err(ConfigError::Invalid(format!(
                "rate_limit `{}`: max_in_flight_per_subject must be at least 1",
                rate_limit.backend.as_str()
            )));
        }
        if rate_limit.max_subjects == 0 {
            return Err(ConfigError::Invalid(format!(
                "rate_limit `{}`: max_subjects must be at least 1",
                rate_limit.backend.as_str()
            )));
        }
        if rate_limit.backend == RateLimitBackend::Redis {
            if rate_limit.lease_ttl_seconds == 0 {
                return Err(ConfigError::Invalid(
                    "rate_limit `redis`: lease_ttl_seconds must be at least 1".into(),
                ));
            }
            if rate_limit.timeout_ms == 0 {
                return Err(ConfigError::Invalid(
                    "rate_limit `redis`: timeout_ms must be at least 1".into(),
                ));
            }
            if rate_limit.connect_timeout_ms == 0 {
                return Err(ConfigError::Invalid(
                    "rate_limit `redis`: connect_timeout_ms must be at least 1".into(),
                ));
            }
            let has_rate_limit_dsn = rate_limit
                .dsn_env
                .as_deref()
                .is_some_and(|name| !name.trim().is_empty());
            let has_budget_fallback = self.budget.backend == BudgetBackend::Redis
                && self
                    .budget
                    .dsn_env
                    .as_deref()
                    .is_some_and(|name| !name.trim().is_empty());
            if !has_rate_limit_dsn && !has_budget_fallback {
                return Err(ConfigError::Invalid(
                    "rate_limit `redis`: `dsn_env` must name the env var holding the connection string (or use the Redis budget `dsn_env` fallback)"
                        .into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_revocation(&self) -> Result<(), ConfigError> {
        let revocation = &self.revocation;
        if revocation.backend == RevocationBackend::None {
            return Ok(());
        }
        if revocation.timeout_ms == 0 {
            return Err(ConfigError::Invalid(format!(
                "revocation `{}`: timeout_ms must be at least 1",
                revocation.backend.as_str()
            )));
        }
        if revocation.connect_timeout_ms == 0 {
            return Err(ConfigError::Invalid(format!(
                "revocation `{}`: connect_timeout_ms must be at least 1",
                revocation.backend.as_str()
            )));
        }
        let has_revocation_dsn = revocation
            .dsn_env
            .as_deref()
            .is_some_and(|name| !name.trim().is_empty());
        let has_budget_fallback = revocation.backend == RevocationBackend::Redis
            && self.budget.backend == BudgetBackend::Redis
            && self
                .budget
                .dsn_env
                .as_deref()
                .is_some_and(|name| !name.trim().is_empty());
        if !has_revocation_dsn && !has_budget_fallback {
            return Err(ConfigError::Invalid(format!(
                "revocation `{}`: `dsn_env` must name the env var holding the connection string (or use the Redis budget `dsn_env` fallback)",
                revocation.backend.as_str()
            )));
        }
        if revocation.backend == RevocationBackend::Postgres {
            validate_table_name(revocation.table.as_deref().unwrap_or("axond_revocation"))
                .map_err(|message| {
                    ConfigError::Invalid(format!("revocation `postgres`: {message}"))
                })?;
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

    pub fn namespace(&self, id: &str) -> Option<&Namespace> {
        self.namespace.iter().find(|n| n.id == id)
    }

    pub fn distinct_namespace_count(&self) -> usize {
        self.namespace
            .iter()
            .map(|namespace| namespace.id.as_str())
            .collect::<HashSet<_>>()
            .len()
    }

    pub fn model(&self, name: &str) -> Option<&Model> {
        self.model.iter().find(|m| m.name == name)
    }

    /// Parse + validate from an in-memory TOML string (tests, and the planned
    /// `axond --check` config linter).
    #[allow(dead_code)]
    pub fn from_toml_str(s: &str) -> Result<Self, ConfigError> {
        use figment::{
            Figment,
            providers::{Format, Toml},
        };
        let cfg: Config = Figment::new()
            .merge(Toml::string(s))
            .extract()
            .map_err(|e| ConfigError::Load(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
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

[[model]]
name = "gpt-4o"
targets = [{ provider = "openai", model = "gpt-4o", price = { input_microdollars_per_million = 2500000, output_microdollars_per_million = 10000000 } }]
"#;

    /// Inbound auth fails closed (ADR 0013), so a config that would leave the
    /// gateway callable without a credential is refused at boot.
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

[[model]]
name = "gpt-4o"
targets = [{ provider = "openai", model = "gpt-4o", price = { input_microdollars_per_million = 1, output_microdollars_per_million = 1 } }]
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
                "{VALID}\n[[gateway_key]]\n{source}\nnamespace = \"platform\"\n"
            ));
            let err = result.expect_err("source shape must be rejected");
            assert!(err.to_string().contains("exactly one"), "{err}");
        }
    }

    #[test]
    fn gateway_verifier_requires_exactly_one_source() {
        for source in [
            "env = \"K\"\nfile = \"/run/key\"",
            "env = \"\"\nfile = \"\"",
        ] {
            let result = Config::from_toml_str(&format!(
                "{VALID}\n[gateway_token]\naudience = \"test\"\n[[gateway_verifier]]\nkid = \"test\"\nalg = \"HS256\"\n{source}\nnamespaces = [\"platform\"]\nmax_ttl = \"15m\"\n"
            ));
            let err = result.expect_err("source shape must be rejected");
            assert!(err.to_string().contains("exactly one"), "{err}");
        }
    }

    #[test]
    fn blank_file_is_absent_when_gateway_key_uses_env() {
        let config = Config::from_toml_str(&format!(
            "{VALID}\n[[gateway_key]]\nenv = \"K\"\nfile = \"\"\nnamespace = \"platform\"\n"
        ))
        .expect("blank file must not count as a declared source");
        let snapshot = crate::state::ConfigSnapshot::build(
            config,
            &std::collections::HashMap::from([
                ("AXOND_KEY".to_owned(), "primary-secret".to_owned()),
                ("K".to_owned(), "secondary-secret".to_owned()),
            ]),
            0,
        )
        .expect("the non-empty env source resolves");
        assert_eq!(snapshot.inbound_key_count(), 2);
    }

    #[test]
    fn blank_file_is_absent_when_gateway_verifier_uses_env() {
        let config = Config::from_toml_str(&format!(
            "{VALID}\n[gateway_token]\naudience = \"test\"\n[[gateway_verifier]]\nkid = \"test\"\nalg = \"HS256\"\nenv = \"K\"\nfile = \"\"\nnamespaces = [\"platform\"]\nmax_ttl = \"15m\"\n"
        ))
        .expect("blank file must not count as a declared source");
        let snapshot = crate::state::ConfigSnapshot::build(
            config,
            &std::collections::HashMap::from([
                ("AXOND_KEY".to_owned(), "primary-secret".to_owned()),
                (
                    "K".to_owned(),
                    "secondary-secret-012345678901234567890".to_owned(),
                ),
            ]),
            0,
        )
        .expect("the non-empty env source resolves");
        assert_eq!(snapshot.gateway_verifier_fingerprints.len(), 1);
    }

    #[test]
    fn rejects_verifiers_without_a_gateway_token_audience() {
        let toml = format!(
            "{VALID}\n[[gateway_verifier]]\nkid = \"test\"\nalg = \"HS256\"\nenv = \"JWT_SECRET\"\nnamespaces = [\"platform\"]\nmax_ttl = \"15m\"\n"
        );
        let err = Config::from_toml_str(&toml).expect_err("verifiers need an audience");
        assert!(err.to_string().contains("gateway_token"), "{err}");
    }

    #[test]
    fn canonicalizes_gateway_token_audience_whitespace() {
        let config = Config::from_toml_str(&format!(
            "{VALID}\n[gateway_token]\naudience = \"  padded-audience  \"\n"
        ))
        .expect("padded audience is valid");
        assert_eq!(
            config.gateway_token.expect("gateway token").audience,
            "padded-audience"
        );
    }

    #[test]
    fn rejects_a_verifier_only_config() {
        let toml = r#"
[[namespace]]
id = "platform"
default = true

[[gateway_verifier]]
kid = "test"
alg = "HS256"
env = "JWT_SECRET"
namespaces = ["platform"]
max_ttl = "15m"

[gateway_token]
audience = "test"
"#;
        let err = Config::from_toml_str(toml).expect_err("static breakglass key is mandatory");
        assert!(err.to_string().contains("gateway_key"), "{err}");
    }

    #[test]
    fn rejects_duplicate_or_unknown_verifier_configuration() {
        let duplicate = format!(
            "{VALID}\n[gateway_token]\naudience = \"test\"\n[[gateway_verifier]]\nkid = \"test\"\nalg = \"HS256\"\nenv = \"A\"\nnamespaces = [\"platform\"]\nmax_ttl = \"15m\"\n[[gateway_verifier]]\nkid = \"test\"\nalg = \"HS256\"\nenv = \"B\"\nnamespaces = [\"platform\"]\nmax_ttl = \"15m\"\n"
        );
        assert!(Config::from_toml_str(&duplicate).is_err());

        let unknown_namespace = format!(
            "{VALID}\n[gateway_token]\naudience = \"test\"\n[[gateway_verifier]]\nkid = \"test\"\nalg = \"HS256\"\nenv = \"A\"\nnamespaces = [\"ghost\"]\nmax_ttl = \"15m\"\n"
        );
        assert!(Config::from_toml_str(&unknown_namespace).is_err());
    }

    #[test]
    fn accepts_a_well_formed_config() {
        let cfg = Config::from_toml_str(VALID).expect("valid config");
        assert_eq!(cfg.default_namespace(), "platform");
        assert!(cfg.model("gpt-4o").is_some());
        assert_eq!(cfg.revocation.backend, RevocationBackend::None);
        assert_eq!(cfg.revocation.key_prefix(), "axond:revocation");
        assert_eq!(cfg.revocation.timeout_ms, 250);
        assert_eq!(cfg.revocation.connect_timeout_ms, 5_000);
    }

    #[test]
    fn revocation_reuses_redis_budget_dsn_and_rejects_zero_timeouts() {
        let config = format!(
            "{VALID}\n[budget]\nbackend = \"redis\"\ndsn_env = \"REDIS_URL\"\nlimit_microdollars = 1\n[revocation]\nbackend = \"redis\"\n"
        );
        let cfg = Config::from_toml_str(&config).expect("budget DSN fallback");
        assert_eq!(cfg.revocation.backend, RevocationBackend::Redis);

        for section in [
            "[revocation]\nbackend = \"redis\"\ntimeout_ms = 0\ndsn_env = \"R\"",
            "[revocation]\nbackend = \"postgres\"\nconnect_timeout_ms = 0\ndsn_env = \"P\"",
        ] {
            let error = Config::from_toml_str(&format!("{VALID}\n{section}"))
                .expect_err("zero timeout must fail");
            assert!(error.to_string().contains("timeout_ms"), "{error}");
        }
    }

    /// Epochs accept both the compact unix representation and an explicit UTC
    /// instant, while malformed timestamps fail during config loading.
    #[test]
    fn parses_gateway_token_epoch_instants() {
        let config = format!(
            "{VALID}\n[[gateway_token_epoch]]\nnamespace = \"platform\"\nmin_iat = 1_786_380_000\n[[gateway_token_epoch]]\nnamespace = \"platform\"\nsubject = \"caller\"\nmin_iat = \"2026-08-10T12:00:00Z\"\n"
        );
        let cfg = Config::from_toml_str(&config).expect("epoch config");
        assert_eq!(cfg.gateway_token_epoch[0].min_iat, 1_786_380_000);
        assert_eq!(cfg.gateway_token_epoch[1].min_iat, 1_786_363_200);

        let malformed = format!(
            "{VALID}\n[[gateway_token_epoch]]\nnamespace = \"platform\"\nmin_iat = \"2026-08-10T12:00:00+01:00\"\n"
        );
        let error = Config::from_toml_str(&malformed).expect_err("non-UTC offset must fail");
        assert!(error.to_string().contains("RFC 3339"), "{error}");
    }

    /// An epoch must name a declared namespace and each namespace/subject pair
    /// has exactly one effective policy.
    #[test]
    fn rejects_unknown_and_duplicate_gateway_token_epochs() {
        let unknown =
            format!("{VALID}\n[[gateway_token_epoch]]\nnamespace = \"ghost\"\nmin_iat = 1\n");
        assert!(
            Config::from_toml_str(&unknown)
                .expect_err("unknown namespace must fail")
                .to_string()
                .contains("undefined namespace")
        );

        let duplicate = format!(
            "{VALID}\n[[gateway_token_epoch]]\nnamespace = \"platform\"\nmin_iat = 1\n[[gateway_token_epoch]]\nnamespace = \"platform\"\nmin_iat = 2\n"
        );
        assert!(
            Config::from_toml_str(&duplicate)
                .expect_err("duplicate namespace epoch must fail")
                .to_string()
                .contains("duplicate")
        );

        for subject in ["", "   "] {
            let blank_subject = format!(
                "{VALID}\n[[gateway_token_epoch]]\nnamespace = \"platform\"\nsubject = \"{subject}\"\nmin_iat = 1\n"
            );
            let error = Config::from_toml_str(&blank_subject).expect_err("blank subject must fail");
            assert!(error.to_string().contains("subject"), "{error}");
            assert!(error.to_string().contains("empty"), "{error}");
        }
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

    #[test]
    fn gateway_minting_validation_rejects_invalid_definitions() {
        let cases = [
            (
                "unknown kid",
                "kid = \"missing\"\nenv = \"SIGN\"",
                "unknown gateway_verifier",
            ),
            (
                "unauthorized namespace",
                "kid = \"test\"\nenv = \"SIGN\"",
                "not permitted",
            ),
            (
                "missing source",
                "kid = \"test\"",
                "exactly one non-empty source",
            ),
            (
                "both sources",
                "kid = \"test\"\nenv = \"SIGN\"\nfile = \"/run/sign\"",
                "exactly one non-empty source",
            ),
            (
                "ttl above verifier",
                "kid = \"test\"\nenv = \"SIGN\"\nmax_ttl = \"16m\"",
                "exceeds verifier",
            ),
            (
                "bad capability",
                "kid = \"test\"\nenv = \"SIGN\"\nscope = [\"not-a-capability\"]",
                "unknown capability",
            ),
            (
                "operator-only capability",
                "kid = \"test\"\nenv = \"SIGN\"\nscope = [\"credentials:all\"]",
                "can never be minted",
            ),
            (
                "empty scope",
                "kid = \"test\"\nenv = \"SIGN\"\nscope = []",
                "at least one capability",
            ),
            (
                "bad alias",
                "kid = \"test\"\nenv = \"SIGN\"\naliases = [\"gpt-*-bad\"]",
                "invalid alias pattern",
            ),
            (
                "empty aliases",
                "kid = \"test\"\nenv = \"SIGN\"\naliases = []",
                "at least one pattern",
            ),
        ];
        for (name, minting, expected) in cases {
            let minting = if minting.contains("scope =") {
                minting.to_owned()
            } else {
                format!("{minting}\nscope = [\"chat\"]")
            };
            let extra_namespace = if name == "unauthorized namespace" {
                "\n[[namespace]]\nid = \"other\"\n"
            } else {
                ""
            };
            let verifier_namespaces = if name == "unauthorized namespace" {
                "[\"other\"]"
            } else {
                "[\"platform\"]"
            };
            let toml = format!(
                r#"
[[namespace]]
id = "platform"
default = true
{extra_namespace}
[[gateway_key]]
env = "INBOUND"
namespace = "platform"
can_mint = true

[gateway_token]
audience = "test"

[[gateway_verifier]]
kid = "test"
alg = "HS256"
env = "JWT"
namespaces = {verifier_namespaces}
max_ttl = "15m"

[gateway_minting]
{minting}
"#
            );
            let error = Config::from_toml_str(&toml).expect_err(name);
            assert!(error.to_string().contains(expected), "{name}: {error}");
        }
    }

    #[test]
    fn can_mint_without_gateway_minting_is_inert() {
        let config = Config::from_toml_str(
            r#"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "INBOUND"
namespace = "platform"
can_mint = true
"#,
        )
        .expect("can_mint is inert without minting config");
        assert!(config.gateway_minting.is_none());
    }

    #[test]
    fn gateway_minting_without_authorized_key_is_valid() {
        let config = Config::from_toml_str(
            r#"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "INBOUND"
namespace = "platform"
can_mint = false
[gateway_token]
audience = "test"
[[gateway_verifier]]
kid = "test"
alg = "HS256"
env = "JWT"
namespaces = ["platform"]
max_ttl = "15m"
[gateway_minting]
kid = "test"
env = "SIGN"
"#,
        );
        assert!(config.is_ok(), "{config:?}");
    }

    #[test]
    fn no_budget_section_means_no_cap_and_no_datastore() {
        let cfg = Config::from_toml_str(VALID).expect("valid config");
        assert_eq!(cfg.budget.backend, BudgetBackend::None);
        assert_eq!(cfg.budget.on_unavailable, StoreUnavailable::Deny);
        assert_eq!(cfg.budget.reservation_ttl_seconds, 300);
        assert_eq!(cfg.budget.idle_ttl_seconds, 3_600);
        assert_eq!(cfg.budget.max_subjects, 10_000);
        assert_eq!(cfg.rate_limit.backend, RateLimitBackend::None);
        assert_eq!(cfg.rate_limit.max_in_flight_per_subject, 16);
        assert_eq!(cfg.rate_limit.max_subjects, 10_000);
    }

    #[test]
    fn rate_limit_reads_backend_and_rejects_zero_bounds_when_enabled() {
        let cfg = Config::from_toml_str(&format!(
            "{VALID}\n[rate_limit]\nbackend = \"in-memory\"\nmax_in_flight_per_subject = 3\nmax_subjects = 25\n"
        ))
        .expect("valid rate limit");
        assert_eq!(cfg.rate_limit.backend, RateLimitBackend::InMemory);
        assert_eq!(cfg.rate_limit.max_in_flight_per_subject, 3);
        assert_eq!(cfg.rate_limit.max_subjects, 25);

        for section in [
            "[rate_limit]\nbackend = \"in-memory\"\nmax_in_flight_per_subject = 0",
            "[rate_limit]\nbackend = \"in-memory\"\nmax_subjects = 0",
        ] {
            assert!(Config::from_toml_str(&format!("{VALID}\n{section}\n")).is_err());
        }
    }

    #[test]
    fn redis_rate_limit_reads_defaults_and_budget_dsn_fallback() {
        let cfg = Config::from_toml_str(&format!(
            "{VALID}\n[budget]\nbackend = \"redis\"\nlimit_microdollars = 1\ndsn_env = \"REDIS_URL\"\n[rate_limit]\nbackend = \"redis\"\n"
        ))
        .expect("valid config");
        assert_eq!(cfg.rate_limit.lease_ttl_seconds, 300);
        assert_eq!(cfg.rate_limit.timeout_ms, 250);
        assert_eq!(cfg.rate_limit.connect_timeout_ms, 5_000);
        assert_eq!(cfg.rate_limit.key_prefix(), "axond:rate_limit");
        assert_eq!(
            crate::rate_limit::resolve_dsn_env(&cfg.rate_limit, &cfg.budget),
            Some("REDIS_URL")
        );
    }

    #[test]
    fn redis_rate_limit_rejects_missing_dsn_and_zero_bounds() {
        for section in [
            "[rate_limit]\nbackend = \"redis\"",
            "[rate_limit]\nbackend = \"redis\"\nlease_ttl_seconds = 0\ndsn_env = \"R\"",
            "[rate_limit]\nbackend = \"redis\"\ntimeout_ms = 0\ndsn_env = \"R\"",
            "[rate_limit]\nbackend = \"redis\"\nconnect_timeout_ms = 0\ndsn_env = \"R\"",
        ] {
            assert!(Config::from_toml_str(&format!("{VALID}\n{section}\n")).is_err());
        }
    }

    #[test]
    fn a_budget_reads_its_backend_and_stance() {
        let cfg = Config::from_toml_str(&format!(
            r#"{VALID}
[budget]
backend = "redis"
limit_microdollars = 10000
dsn_env = "AXOND_BUDGET_REDIS_URL"
on_unavailable = "allow"
"#
        ))
        .expect("valid config");
        assert_eq!(cfg.budget.backend, BudgetBackend::Redis);
        assert_eq!(cfg.budget.on_unavailable, StoreUnavailable::Allow);
        assert_eq!(cfg.budget.key_prefix(), "axond:budget");
    }

    #[test]
    fn a_shared_backend_reads_the_optional_namespace_cap() {
        let cfg = Config::from_toml_str(&format!(
            r#"{VALID}
[budget]
backend = "redis"
limit_microdollars = 10000
namespace_limit_microdollars = 100000
dsn_env = "AXOND_BUDGET_REDIS_URL"
"#
        ))
        .expect("valid config");
        assert_eq!(cfg.budget.namespace_limit_microdollars, Some(100_000));
    }

    /// Omitting it must keep the previous per-subject-only behavior exactly.
    #[test]
    fn omitting_the_namespace_cap_leaves_subject_only_enforcement() {
        let cfg = Config::from_toml_str(&format!(
            "{VALID}\n[budget]\nbackend = \"redis\"\nlimit_microdollars = 1\ndsn_env = \"R\"\n"
        ))
        .expect("valid config");
        assert_eq!(cfg.budget.namespace_limit_microdollars, None);
    }

    /// Only the shared backends can enforce a namespace cap *exactly*, so the
    /// others reject it rather than pretending to honour it per replica.
    #[test]
    fn a_namespace_cap_needs_a_backend_that_can_enforce_it_exactly() {
        for budget in [
            "[budget]\nnamespace_limit_microdollars = 100",
            "[budget]\nbackend = \"none\"\nnamespace_limit_microdollars = 100",
            "[budget]\nbackend = \"in-memory\"\nlimit_microdollars = 1\nnamespace_limit_microdollars = 100",
        ] {
            let error = Config::from_toml_str(&format!("{VALID}\n{budget}\n"))
                .err()
                .unwrap_or_else(|| panic!("`{budget}` must be rejected"));
            assert!(
                format!("{error}").contains("namespace_limit_microdollars is supported only by"),
                "{error}"
            );
        }
        // Zero would deny every request in the namespace.
        let error = Config::from_toml_str(&format!(
            "{VALID}\n[budget]\nbackend = \"redis\"\nlimit_microdollars = 1\ndsn_env = \"R\"\nnamespace_limit_microdollars = 0\n"
        ))
        .expect_err("zero must be rejected");
        assert!(format!("{error}").contains("must be at least 1"), "{error}");
    }

    /// A budget whose fields do not add up is a boot failure, not a surprise at
    /// request time.
    #[test]
    fn rejects_budgets_that_could_not_enforce_anything() {
        for budget in [
            // A shared backend with nowhere to connect.
            "[budget]\nbackend = \"redis\"\nlimit_microdollars = 10000",
            // A cap of zero would deny every request.
            "[budget]\nbackend = \"in-memory\"",
            "[budget]\nbackend = \"in-memory\"\nlimit_microdollars = 1\nidle_ttl_seconds = 0",
            "[budget]\nbackend = \"in-memory\"\nlimit_microdollars = 1\nmax_subjects = 0",
            "[budget]\nbackend = \"postgres\"\nlimit_microdollars = 1\ndsn_env = \"D\"\nreservation_ttl_seconds = 0",
            // A table name that could carry SQL.
            "[budget]\nbackend = \"postgres\"\nlimit_microdollars = 1\ndsn_env = \"D\"\ntable = \"caps; drop table users\"",
        ] {
            let result = Config::from_toml_str(&format!("{VALID}\n{budget}\n"));
            assert!(
                matches!(result, Err(ConfigError::Invalid(_))),
                "expected `{budget}` to be rejected"
            );
        }
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
    fn hot_reload_is_signal_only_until_watching_is_asked_for() {
        let cfg = Config::from_toml_str(VALID).expect("valid config");
        assert!(!cfg.reload.watch);
        assert_eq!(cfg.reload.poll_interval_ms, 2_000);

        let cfg = Config::from_toml_str(&format!(
            "{VALID}\n[reload]\nwatch = true\npoll_interval_ms = 500\n"
        ))
        .expect("valid config");
        assert!(cfg.reload.watch);
        assert_eq!(cfg.reload.poll_interval_ms, 500);
    }

    #[test]
    fn rejects_a_watch_interval_that_would_busy_read_the_config() {
        let toml = format!("{VALID}\n[reload]\nwatch = true\npoll_interval_ms = 5\n");
        assert!(matches!(
            Config::from_toml_str(&toml),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn rejects_alias_pointing_at_undefined_provider() {
        let toml = r#"
[[namespace]]
id = "platform"
default = true

[[model]]
name = "gpt-4o"
targets = [{ provider = "ghost", model = "gpt-4o", price = { input_microdollars_per_million = 1, output_microdollars_per_million = 1 } }]
"#;
        let err = Config::from_toml_str(toml).unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(_)), "{err:?}");
    }

    #[test]
    fn rejects_alias_with_targets_from_incompatible_wires() {
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

[[model]]
name = "mixed"
targets = [
    { provider = "openai", model = "gpt-4o", price = { input_microdollars_per_million = 1, output_microdollars_per_million = 1 } },
    { provider = "anthropic", model = "claude", price = { input_microdollars_per_million = 1, output_microdollars_per_million = 1 } },
]
"#;
        let err = Config::from_toml_str(toml).expect_err("cross-wire failover must fail");
        let message = err.to_string();
        assert!(message.contains("mixed"), "{message}");
        assert!(message.contains("openai"), "{message}");
        assert!(message.contains("anthropic"), "{message}");
        assert!(message.contains("OpenAI"), "{message}");
        assert!(message.contains("Anthropic"), "{message}");
        assert!(message.contains("no route can serve"), "{message}");
    }

    #[test]
    fn accepts_openai_family_failover_targets() {
        let toml = r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

[[provider]]
id = "compatible"
kind = "openai-compatible"
base_url = "https://example.test/v1"

[[gateway_key]]
env = "AXOND_KEY"
namespace = "platform"

[[model]]
name = "mixed-openai"
targets = [
    { provider = "openai", model = "gpt-4o", price = { input_microdollars_per_million = 1, output_microdollars_per_million = 1 } },
    { provider = "compatible", model = "gpt-4o", price = { input_microdollars_per_million = 1, output_microdollars_per_million = 1 } },
]
"#;
        Config::from_toml_str(toml).expect("OpenAI-family targets are compatible");
    }

    #[test]
    fn accepts_aliases_each_confined_to_one_wire_family() {
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

[[model]]
name = "openai-alias"
targets = [{ provider = "openai", model = "gpt-4o", price = { input_microdollars_per_million = 1, output_microdollars_per_million = 1 } }]

[[model]]
name = "anthropic-alias"
targets = [{ provider = "anthropic", model = "claude", price = { input_microdollars_per_million = 1, output_microdollars_per_million = 1 } }]
"#;
        Config::from_toml_str(toml).expect("single-wire aliases are compatible");
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
    fn rejects_model_with_no_targets() {
        let toml = r#"
[[namespace]]
id = "platform"
default = true

[[model]]
name = "gpt-4o"
targets = []
"#;
        assert!(matches!(
            Config::from_toml_str(toml),
            Err(ConfigError::Invalid(_))
        ));
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

    /// The minimum a stateful replica needs before it can read anything else.
    const STATEFUL: &str = r#"
mode = "stateful"

[control_plane]
dsn_env = "GW_CONTROL_PLANE_DSN"

[secret_store]
kek_env = "GW_SECRET_STORE_KEK"

[[admin_breakglass]]
env = "GW_ADMIN_BREAKGLASS"
"#;

    fn repository_file(relative: &str) -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(relative);
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()))
    }

    /// The compatibility promise ADR 0027 makes: a configuration written before
    /// `mode` existed keeps meaning exactly what it meant, without adding a key.
    #[test]
    fn omitting_mode_means_stateless() {
        let config = Config::from_toml_str(VALID).expect("today's config still boots");
        assert_eq!(config.mode, Mode::Stateless);
    }

    #[test]
    fn declaring_stateless_explicitly_changes_nothing() {
        let explicit = Config::from_toml_str(&format!("mode = \"stateless\"\n{VALID}"))
            .expect("an explicit stateless mode is the same configuration");
        assert_eq!(explicit.mode, Mode::Stateless);
    }

    #[test]
    fn rejects_an_unknown_mode() {
        let error = Config::from_toml_str(&format!("mode = \"hybrid\"\n{VALID}"))
            .expect_err("there are exactly two modes");
        assert!(matches!(error, ConfigError::Load(_)), "{error:?}");
    }

    /// Stateful bootstrap in a stateless configuration is a forgotten `mode`
    /// key, not an inert extra, so it is refused rather than ignored.
    #[test]
    fn stateless_mode_rejects_stateful_bootstrap_sections() {
        for (section, snippet) in [
            (
                "`[control_plane]`",
                "[control_plane]\ndsn_env = \"GW_CONTROL_PLANE_DSN\"",
            ),
            (
                "`[secret_store]`",
                "[secret_store]\nkek_env = \"GW_SECRET_STORE_KEK\"",
            ),
            (
                "`[[admin_breakglass]]`",
                "[[admin_breakglass]]\nenv = \"GW_ADMIN_BREAKGLASS\"",
            ),
        ] {
            let error = Config::from_toml_str(&format!("{VALID}\n{snippet}"))
                .expect_err("stateless mode has no control plane to bootstrap");
            assert!(
                matches!(error, ConfigError::Invalid(ref message) if message.contains(section)),
                "{section}: {error:?}"
            );
        }
    }

    #[test]
    fn accepts_a_minimal_stateful_bootstrap() {
        let config = Config::from_toml_str(STATEFUL).expect("the approved bootstrap set validates");
        assert_eq!(config.mode, Mode::Stateful);
        assert_eq!(
            config
                .control_plane
                .as_ref()
                .and_then(|cp| cp.dsn_env.as_deref()),
            Some("GW_CONTROL_PLANE_DSN")
        );
        assert_eq!(
            config
                .secret_store
                .as_ref()
                .and_then(SecretStore::kek_reference),
            Some(("kek_env", "GW_SECRET_STORE_KEK"))
        );
    }

    /// The property ADR 0027 keeps from ADR 0017: one authority per resource
    /// class. Mixed ownership fails before the listener binds instead of being
    /// merged, overlaid, or preferred.
    #[test]
    fn stateful_mode_rejects_every_stateful_owned_section() {
        for (section, snippet) in [
            (
                "`[[namespace]]`",
                "[[namespace]]\nid = \"acme\"\ndefault = true",
            ),
            (
                "`[[provider]]`",
                "[[provider]]\nid = \"openai\"\nkind = \"openai\"\nbase_url = \"https://api.openai.com/v1\"",
            ),
            (
                "`[[model]]`",
                "[[model]]\nname = \"gpt-4o\"\ntargets = [{ provider = \"openai\", model = \"gpt-4o\", price = { input_microdollars_per_million = 1, output_microdollars_per_million = 1 } }]",
            ),
            (
                "`[[credential]]`",
                "[[credential]]\nnamespace = \"acme\"\nprovider = \"openai\"\nenv = \"GW_ACME_OPENAI\"",
            ),
            (
                "`[credential_pool]`",
                "[credential_pool]\nstrategy = \"weighted\"",
            ),
            ("`[failover]`", "[failover]\nmax_attempts = 5"),
            (
                "`[[gateway_key]]`",
                "[[gateway_key]]\nenv = \"GW_INBOUND\"\nnamespace = \"acme\"",
            ),
            (
                "`[[gateway_verifier]]`",
                "[[gateway_verifier]]\nkid = \"acme\"\nalg = \"EdDSA\"\nenv = \"GW_VERIFY\"\nnamespaces = [\"acme\"]\nmax_ttl = \"15m\"",
            ),
            (
                "`[gateway_minting]`",
                "[gateway_minting]\nkid = \"acme\"\nenv = \"GW_SIGN\"",
            ),
            (
                "`[gateway_token]`",
                "[gateway_token]\naudience = \"https://gw.test\"",
            ),
            (
                "`[[gateway_token_epoch]]`",
                "[[gateway_token_epoch]]\nnamespace = \"acme\"\nmin_iat = 1",
            ),
            (
                "`[budget] limit_microdollars`",
                "[budget]\nbackend = \"redis\"\ndsn_env = \"AXOND_REDIS_URL\"\nlimit_microdollars = 10000",
            ),
            (
                "`[rate_limit] max_in_flight_per_subject`",
                "[rate_limit]\nbackend = \"redis\"\ndsn_env = \"AXOND_REDIS_URL\"\nmax_in_flight_per_subject = 4",
            ),
            (
                "`[rate_limit] lease_ttl_seconds`",
                "[rate_limit]\nbackend = \"redis\"\ndsn_env = \"AXOND_REDIS_URL\"\nlease_ttl_seconds = 60",
            ),
        ] {
            let error = Config::from_toml_str(&format!("{STATEFUL}\n{snippet}"))
                .expect_err("the control plane owns this, so TOML may not also declare it");
            assert!(
                matches!(error, ConfigError::Invalid(ref message) if message.contains(section)),
                "{section}: {error:?}"
            );
        }
    }

    /// An operator mid-cutover should learn the whole list once rather than one
    /// offender per restart.
    #[test]
    fn stateful_rejection_names_every_offending_section_at_once() {
        let toml = format!(
            r#"{STATEFUL}

[[namespace]]
id = "acme"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

[failover]
max_attempts = 5
"#
        );
        let error = Config::from_toml_str(&toml).expect_err("mixed ownership never boots");
        let ConfigError::Invalid(message) = error else {
            panic!("{error:?}");
        };
        for section in ["`[[namespace]]`", "`[[provider]]`", "`[failover]`"] {
            assert!(
                message.contains(section),
                "{section} missing from {message}"
            );
        }
    }

    /// Bootstrap owns connectivity to the opt-in admission backends; the control
    /// plane owns their policy values. Selecting a backend with references only
    /// is therefore valid.
    #[test]
    fn stateful_mode_accepts_admission_backend_connectivity_without_policy() {
        let toml = format!(
            r#"{STATEFUL}

[budget]
backend = "redis"
dsn_env = "AXOND_REDIS_URL"
on_unavailable = "deny"

[rate_limit]
backend = "redis"
dsn_env = "AXOND_REDIS_URL"

[revocation]
backend = "redis"
dsn_env = "AXOND_REDIS_URL"
"#
        );
        let config =
            Config::from_toml_str(&toml).expect("connectivity references are bootstrap-owned");
        assert_eq!(config.budget.backend, BudgetBackend::Redis);
    }

    #[test]
    fn stateful_mode_still_requires_a_dsn_reference_for_a_shared_backend() {
        let toml = format!("{STATEFUL}\n[budget]\nbackend = \"redis\"\n");
        let error = Config::from_toml_str(&toml)
            .expect_err("a shared backend without a reference enforces nothing");
        assert!(
            matches!(error, ConfigError::Invalid(ref message) if message.contains("dsn_env")),
            "{error:?}"
        );
    }

    /// Cold boot in stateful mode requires Postgres, so a bootstrap without a
    /// control-plane reference describes a replica with nothing to serve.
    #[test]
    fn stateful_mode_requires_a_complete_control_plane_reference() {
        for (expected, toml) in [
            (
                "`[control_plane]`",
                "mode = \"stateful\"\n[secret_store]\nkek_env = \"KEK\"\n[[admin_breakglass]]\nenv = \"BG\"",
            ),
            (
                "`[control_plane] dsn_env`",
                "mode = \"stateful\"\n[control_plane]\n[secret_store]\nkek_env = \"KEK\"\n[[admin_breakglass]]\nenv = \"BG\"",
            ),
            (
                "`[control_plane] dsn_env`",
                "mode = \"stateful\"\n[control_plane]\ndsn_env = \"   \"\n[secret_store]\nkek_env = \"KEK\"\n[[admin_breakglass]]\nenv = \"BG\"",
            ),
            (
                "`[control_plane] connect_timeout_ms`",
                "mode = \"stateful\"\n[control_plane]\ndsn_env = \"DSN\"\nconnect_timeout_ms = 0\n[secret_store]\nkek_env = \"KEK\"\n[[admin_breakglass]]\nenv = \"BG\"",
            ),
        ] {
            let error = Config::from_toml_str(toml)
                .expect_err("stateful cold boot needs the control plane");
            assert!(
                matches!(error, ConfigError::Invalid(ref message) if message.contains(expected)),
                "{expected}: {error:?}"
            );
        }
    }

    /// A snapshot is only publishable once its credential references are
    /// unwrapped, so the store and the KEK are boot requirements.
    #[test]
    fn stateful_mode_requires_a_secret_store_and_exactly_one_kek_reference() {
        for (expected, toml) in [
            (
                "`[secret_store]`",
                "mode = \"stateful\"\n[control_plane]\ndsn_env = \"DSN\"\n[[admin_breakglass]]\nenv = \"BG\"",
            ),
            (
                "kek_env",
                "mode = \"stateful\"\n[control_plane]\ndsn_env = \"DSN\"\n[secret_store]\n[[admin_breakglass]]\nenv = \"BG\"",
            ),
            (
                "kek_env",
                "mode = \"stateful\"\n[control_plane]\ndsn_env = \"DSN\"\n[secret_store]\nkek_env = \"KEK\"\nkek_file = \"/run/secrets/kek\"\n[[admin_breakglass]]\nenv = \"BG\"",
            ),
        ] {
            let error = Config::from_toml_str(toml).expect_err("wrapped material needs a KEK");
            assert!(
                matches!(error, ConfigError::Invalid(ref message) if message.contains(expected)),
                "{expected}: {error:?}"
            );
        }
    }

    /// Encrypted Postgres is normally the control-plane database itself, so the
    /// store may inherit that reference instead of repeating it.
    #[test]
    fn the_secret_store_inherits_the_control_plane_dsn_reference() {
        let config = Config::from_toml_str(STATEFUL).expect("inheriting the reference is valid");
        let secret_store = config.secret_store.expect("secret store");
        assert_eq!(secret_store.dsn_env, None);
        assert_eq!(secret_store.backend, SecretStoreBackend::Postgres);
    }

    /// The breakglass credential is the way in when OIDC is down, and a second
    /// one would make an audited operator action ambiguous.
    #[test]
    fn stateful_mode_requires_exactly_one_usable_breakglass_credential() {
        for (expected, toml) in [
            (
                "`[[admin_breakglass]]`",
                "mode = \"stateful\"\n[control_plane]\ndsn_env = \"DSN\"\n[secret_store]\nkek_env = \"KEK\"",
            ),
            (
                "exactly one `[[admin_breakglass]]`",
                "mode = \"stateful\"\n[control_plane]\ndsn_env = \"DSN\"\n[secret_store]\nkek_env = \"KEK\"\n[[admin_breakglass]]\nenv = \"BG\"\n[[admin_breakglass]]\nenv = \"BG2\"",
            ),
            (
                "exactly one non-empty source",
                "mode = \"stateful\"\n[control_plane]\ndsn_env = \"DSN\"\n[secret_store]\nkek_env = \"KEK\"\n[[admin_breakglass]]\nenv = \"BG\"\nfile = \"/run/secrets/bg\"",
            ),
            (
                "exactly one non-empty source",
                "mode = \"stateful\"\n[control_plane]\ndsn_env = \"DSN\"\n[secret_store]\nkek_env = \"KEK\"\n[[admin_breakglass]]\nid = \"breakglass\"",
            ),
        ] {
            let error = Config::from_toml_str(toml)
                .expect_err("one usable breakglass credential is mandatory");
            assert!(
                matches!(error, ConfigError::Invalid(ref message) if message.contains(expected)),
                "{expected}: {error:?}"
            );
        }
    }

    /// Stateful bootstrap can hold references and nothing else, so a diagnostic
    /// (and `Debug`) names an env var or a path, never material.
    #[test]
    fn stateful_bootstrap_diagnostics_name_references_only() {
        let config = Config::from_toml_str(STATEFUL).expect("valid bootstrap");
        let rendered = format!(
            "{:?} {:?} {:?}",
            config.control_plane, config.secret_store, config.admin_breakglass
        );
        for reference in [
            "GW_CONTROL_PLANE_DSN",
            "GW_SECRET_STORE_KEK",
            "GW_ADMIN_BREAKGLASS",
        ] {
            assert!(rendered.contains(reference), "{reference} missing");
        }
        assert_eq!(
            config.admin_breakglass[0].label(),
            "GW_ADMIN_BREAKGLASS",
            "an unlabelled credential is attributed by its reference"
        );
    }

    /// A secret-bearing variable named after a config key is merged by the
    /// `AXOND_` override layer instead of being left for the reference to
    /// resolve, and figment's type error would then carry the credential into
    /// the load diagnostic. Every such reference is refused by name.
    #[test]
    fn a_bootstrap_reference_that_the_env_override_layer_would_claim_is_rejected() {
        for (key, toml) in [
            (
                "[[admin_breakglass]] env",
                "mode = \"stateful\"\n[control_plane]\ndsn_env = \"GW_DSN\"\n[secret_store]\nkek_env = \"GW_KEK\"\n[[admin_breakglass]]\nenv = \"AXOND_ADMIN_BREAKGLASS\"",
            ),
            (
                "[control_plane] dsn_env",
                "mode = \"stateful\"\n[control_plane]\ndsn_env = \"AXOND_CONTROL_PLANE\"\n[secret_store]\nkek_env = \"GW_KEK\"\n[[admin_breakglass]]\nenv = \"GW_BG\"",
            ),
            (
                "[secret_store] kek_env",
                "mode = \"stateful\"\n[control_plane]\ndsn_env = \"GW_DSN\"\n[secret_store]\nkek_env = \"AXOND_SECRET_STORE\"\n[[admin_breakglass]]\nenv = \"GW_BG\"",
            ),
        ] {
            let error = Config::from_toml_str(toml)
                .expect_err("the override layer would claim this variable");
            let ConfigError::Invalid(message) = error else {
                panic!("{key}: expected a validation error");
            };
            assert!(message.contains(key), "{key}: {message}");
            assert!(
                message.contains("`AXOND_` override layer"),
                "{key}: {message}"
            );
        }
    }

    /// A file path cannot be claimed by the environment layer, so the same name
    /// shape is fine there — and a variable outside the override shape is fine
    /// anywhere.
    #[test]
    fn a_reference_outside_the_override_shape_is_accepted() {
        let toml = "mode = \"stateful\"\n[control_plane]\ndsn_env = \"AXOND_CONTROL_PLANE_DSN\"\n[secret_store]\nkek_file = \"/run/secrets/AXOND_SECRET_STORE\"\n[[admin_breakglass]]\nfile = \"/run/secrets/breakglass\"";
        Config::from_toml_str(toml)
            .expect("`AXOND_CONTROL_PLANE_DSN` is not a config key, and paths are never merged");
    }

    /// The override-key list only protects references if it still matches the
    /// keys the environment layer can actually address.
    #[test]
    fn the_override_key_list_matches_every_config_field() {
        let source = repository_file("crates/gateway/src/config.rs");
        let (_, rest) = source
            .split_once("pub struct Config {")
            .expect("Config struct");
        let (body, _) = rest.split_once("\n}").expect("Config struct body");
        let fields: Vec<&str> = body
            .lines()
            .filter_map(|line| line.trim().strip_prefix("pub "))
            .filter_map(|line| line.split_once(':'))
            .map(|(field, _)| field)
            .collect();
        assert_eq!(fields, OVERRIDE_KEYS, "OVERRIDE_KEYS drifted from `Config`");
    }

    /// The shipped configurations and fixtures must themselves name variables
    /// the override layer leaves alone, since an operator copies them verbatim.
    #[test]
    fn no_shipped_configuration_references_a_claimable_variable() {
        for relative in [
            "axond.stateful.example.toml",
            "axond.example.toml",
            "tests/tier0/axond.stateful-bootstrap.toml",
            "tests/tier0/axond.tier0.toml",
        ] {
            for line in repository_file(relative).lines() {
                let line = line.trim_start_matches('#').trim();
                let Some((key, value)) = line.split_once(" = ") else {
                    continue;
                };
                if !matches!(key, "env" | "dsn_env" | "kek_env") {
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

    /// The shipped stateful example is the operator-facing copy of the approved
    /// bootstrap set; it must keep validating as the parser evolves.
    #[test]
    fn the_shipped_stateful_example_validates() {
        let config = Config::from_toml_str(&repository_file("axond.stateful.example.toml"))
            .expect("axond.stateful.example.toml must validate");
        assert_eq!(config.mode, Mode::Stateful);
        assert!(
            config.namespace.is_empty(),
            "the control plane owns tenants"
        );
    }

    /// Documentation drift gate: every key the shipped stateful example uses is
    /// documented in the configuration reference, so a new bootstrap key cannot
    /// ship undocumented.
    #[test]
    fn the_configuration_reference_documents_the_stateful_bootstrap_surface() {
        let reference = repository_file("docs/configuration.md");
        let example = repository_file("axond.stateful.example.toml");
        let mut documented_keys = 0;
        for line in example.lines() {
            let line = line.trim_start_matches('#').trim();
            let Some((key, _)) = line.split_once(" = ") else {
                continue;
            };
            // Prose mentions a key inside backticks; a setting is bare.
            if key.is_empty() || !key.chars().all(|c| c.is_ascii_lowercase() || c == '_') {
                continue;
            }
            documented_keys += 1;
            assert!(
                reference.contains(&format!("`{key}`")),
                "docs/configuration.md does not document `{key}`"
            );
        }
        assert!(
            documented_keys >= 8,
            "expected the stateful example to exercise the bootstrap surface, found \
             {documented_keys} keys"
        );
        for section in [
            "[control_plane]",
            "[secret_store]",
            "[[admin_breakglass]]",
            "mode = \"stateful\"",
        ] {
            assert!(
                reference.contains(section),
                "docs/configuration.md does not document {section}"
            );
        }
    }

    #[test]
    fn rejects_unpriced_target_at_parse() {
        let toml = r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

[[model]]
name = "gpt-4o"
targets = [{ provider = "openai", model = "gpt-4o" }]
"#;
        // Missing `price` → deserialization fails before validation runs.
        assert!(matches!(
            Config::from_toml_str(toml),
            Err(ConfigError::Load(_))
        ));
    }
}
