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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use gateway_core::ModelPrice;
use serde::Deserialize;

use crate::usage::{BatchSettings, MAX_ROWS_PER_STATEMENT, validate_table_name};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: Server,
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
    /// How the running config is replaced without a restart.
    #[serde(default)]
    pub reload: Reload,
    /// Inbound gateway keys. Each binds a secret (resolved from `env`) to a
    /// namespace. When empty, the gateway is unauthenticated and every request
    /// uses the default namespace — intended for local dev only.
    #[serde(default)]
    pub gateway_key: Vec<GatewayKey>,
    /// Where raw usage records go. Empty means the no-datastore default: one
    /// JSON line per record on stdout (ADR 0002).
    #[serde(default)]
    pub usage_sink: Vec<UsageSinkConfig>,
    /// Spend cap enforcement. Defaults to no budget at all, so nothing drags a
    /// datastore onto the default path (ADR 0002).
    #[serde(default)]
    pub budget: BudgetConfig,
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

#[derive(Debug, Clone, Deserialize)]
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
#[derive(Debug, Clone, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct UsageSinkConfig {
    pub kind: UsageSinkKind,
    /// `postgres`: name of the env var holding the connection string. The DSN is
    /// a secret, so it is referenced rather than inlined, like every credential.
    #[serde(default)]
    pub dsn_env: Option<String>,
    /// `postgres`: destination table. Defaults to `axond_usage`, matching the
    /// shipped DDL.
    #[serde(default)]
    pub table: Option<String>,
    /// `postgres`: apply the shipped DDL at boot. Off by default — most
    /// deployments give the gateway's role no DDL rights.
    #[serde(default)]
    pub create_table: bool,
    /// Records buffered before the fan-out starts dropping (`postgres`).
    #[serde(default = "default_buffer_capacity")]
    pub buffer_capacity: usize,
    /// Rows per write (`postgres`).
    #[serde(default = "default_max_batch")]
    pub max_batch: usize,
    /// How long a partial batch waits before it is written anyway (`postgres`).
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
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
            max_batch: self.max_batch,
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
#[derive(Debug, Clone, Deserialize)]
pub struct BudgetConfig {
    #[serde(default)]
    pub backend: BudgetBackend,
    /// The cap, in micro-dollars, per `(namespace, subject)`. Required by every
    /// backend but `none`.
    #[serde(default)]
    pub limit_microdollars: u64,
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
            on_unavailable: StoreUnavailable::Deny,
            dsn_env: None,
            table: None,
            create_table: false,
            key_prefix: None,
            reservation_ttl_seconds: default_reservation_ttl_seconds(),
        }
    }
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

const DEFAULT_BUDGET_TABLE: &str = "axond_budget";
const DEFAULT_BUDGET_KEY_PREFIX: &str = "axond:budget";

fn default_reservation_ttl_seconds() -> u64 {
    300
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayKey {
    /// Env var holding the inbound key secret.
    pub env: String,
    pub namespace: String,
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

    /// Reject a structurally-invalid config at boot rather than at request
    /// time (delta B2): exactly one default namespace, aliases point at
    /// defined providers, credentials/keys reference defined namespaces.
    pub fn validate(&self) -> Result<(), ConfigError> {
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
        if self.reload.poll_interval_ms < MIN_RELOAD_POLL_INTERVAL_MS {
            return Err(ConfigError::Invalid(format!(
                "reload.poll_interval_ms must be at least {MIN_RELOAD_POLL_INTERVAL_MS}"
            )));
        }
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
        for k in &self.gateway_key {
            if !namespaces.contains_key(k.namespace.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "gateway_key references undefined namespace `{}`",
                    k.namespace
                )));
            }
        }
        self.validate_usage_sinks()?;
        self.validate_budget()?;
        Ok(())
    }

    /// A budget's fields only make sense together: a cap of zero would deny
    /// every request, and a shared backend without a DSN reference cannot
    /// enforce anything.
    fn validate_budget(&self) -> Result<(), ConfigError> {
        let budget = &self.budget;
        let backend = budget.backend.as_str();
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

    /// A sink's fields only make sense together, so they are checked as a set:
    /// a Postgres sink needs a DSN reference and a table name that is safe to
    /// interpolate, and its batch has to fit one statement's parameter budget.
    fn validate_usage_sinks(&self) -> Result<(), ConfigError> {
        for sink in &self.usage_sink {
            let kind = sink.kind.as_str();
            if sink.buffer_capacity == 0 {
                return Err(ConfigError::Invalid(format!(
                    "usage_sink `{kind}`: buffer_capacity must be at least 1"
                )));
            }
            if sink.max_batch == 0 || sink.max_batch > MAX_ROWS_PER_STATEMENT {
                return Err(ConfigError::Invalid(format!(
                    "usage_sink `{kind}`: max_batch must be between 1 and {MAX_ROWS_PER_STATEMENT}"
                )));
            }
            if sink.flush_interval_ms == 0 {
                return Err(ConfigError::Invalid(format!(
                    "usage_sink `{kind}`: flush_interval_ms must be at least 1"
                )));
            }
            if sink.kind == UsageSinkKind::Postgres {
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

[[model]]
name = "gpt-4o"
targets = [{ provider = "openai", model = "gpt-4o", price = { input_microdollars_per_million = 2500000, output_microdollars_per_million = 10000000 } }]
"#;

    #[test]
    fn accepts_a_well_formed_config() {
        let cfg = Config::from_toml_str(VALID).expect("valid config");
        assert_eq!(cfg.default_namespace(), "platform");
        assert!(cfg.model("gpt-4o").is_some());
    }

    #[test]
    fn no_budget_section_means_no_cap_and_no_datastore() {
        let cfg = Config::from_toml_str(VALID).expect("valid config");
        assert_eq!(cfg.budget.backend, BudgetBackend::None);
        assert_eq!(cfg.budget.on_unavailable, StoreUnavailable::Deny);
        assert_eq!(cfg.budget.reservation_ttl_seconds, 300);
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

    /// A budget whose fields do not add up is a boot failure, not a surprise at
    /// request time.
    #[test]
    fn rejects_budgets_that_could_not_enforce_anything() {
        for budget in [
            // A shared backend with nowhere to connect.
            "[budget]\nbackend = \"redis\"\nlimit_microdollars = 10000",
            // A cap of zero would deny every request.
            "[budget]\nbackend = \"in-memory\"",
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
    fn rejects_batch_sizes_the_wire_protocol_cannot_carry() {
        for bad in ["max_batch = 0", "max_batch = 100000", "buffer_capacity = 0"] {
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
