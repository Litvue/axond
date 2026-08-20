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

use crate::admission::MAX_PERMITS;
use crate::aliases::AliasScope;
use crate::backends::catalog::{InvalidCatalogId, ProviderId};
use crate::backends::catalog_refresh::{Bootstrap, RefreshSchedule};
use crate::backends::catalog_store::postgres::CatalogStoreSettings;
use crate::backends::control_plane::ControlPlaneBackend;
use crate::convergence::backoff::BackoffPolicy;
use crate::desired_state::policy::{PolicyBody, PolicyGeneration};
use crate::desired_state::{ProjectId, SecretRef, TenantId};
use crate::principals::Capability;
use crate::usage::journal::{Capacity, CapacityPolicy, ConsumerId};
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
    /// Stateful bootstrap: the durable backend's discriminated connection
    /// contract. Required by `mode = "stateful"`, rejected in stateless mode.
    #[serde(default)]
    pub control_plane: Option<ControlPlane>,
    /// Stateful convergence and its authenticated local last-known-good cache.
    /// The cache contains desired-state references only; the secret store still
    /// has to be available before a restored revision can be published.
    #[serde(default)]
    pub convergence: ConvergenceConfig,
    /// Stateful bootstrap: which `SecretStore` unwraps tenant secret material,
    /// and the key-encryption key it unwraps with — both by reference.
    #[serde(default)]
    pub secret_store: Option<SecretStore>,
    /// Stateful bootstrap: the mandatory static `/admin/v1` breakglass operator
    /// credential, referenced the way `[[gateway_key]]` is.
    #[serde(default)]
    pub admin_breakglass: Vec<AdminBreakglass>,
    /// Optional OIDC issuer used to authenticate human `/admin/v1` callers.
    /// The issuer, audience, and JWKS endpoint are bootstrap references; the
    /// resulting identity is authorized against the active desired-state
    /// directory, never against a request-time control-plane read.
    #[serde(default)]
    pub admin_oidc: Option<AdminOidc>,
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
    /// Inbound workload principals projected from a durable revision.
    ///
    /// This is deliberately not deserializable: stateful bootstrap TOML cannot
    /// declare inference identities, and the key material never enters the
    /// process. A revision contributes only the namespace, stable subject, and
    /// one-way digest needed by the snapshot's in-memory verifier.
    #[serde(skip)]
    pub(crate) projected_principals: Vec<ProjectedPrincipal>,
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
    /// Durable, replayed usage delivery. Defaults to `backend = "none"`: the
    /// telemetry-grade path stays exactly as it is and no datastore joins the
    /// default deployment (ADR 0002, ADR 0049).
    #[serde(default)]
    pub usage_journal: UsageJournalConfig,
    /// Spend cap enforcement. Defaults to no budget at all, so nothing drags a
    /// datastore onto the default path (ADR 0002).
    #[serde(default)]
    pub budget: BudgetConfig,
    /// Inbound per-caller concurrency enforcement. Defaults to no limit.
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    /// Reversible migration gate for the fixed rate-limit and budget stages.
    /// The default owns both holds in the response-lifetime middleware owner;
    /// `legacy` restores the previous straight-line ownership without a binary
    /// rollback.
    #[serde(default)]
    pub core_middleware: CoreMiddlewareConfig,
    /// Bounds on what one request may consume, plus the global and per-tenant
    /// admission ceilings that shed load before it reaches a provider.
    #[serde(default)]
    pub admission: AdmissionConfig,
    /// Precise minted-token revocation. Defaults to no denylist.
    #[serde(default)]
    pub revocation: RevocationConfig,
    /// The upstream catalogue this deployment imports provider and model
    /// metadata from, and how often. Defaults to `backend = "none"`: nothing is
    /// fetched, and an operator's own resources are the whole catalogue
    /// (ADR 0043, ADR 0051).
    #[serde(default)]
    pub catalog: CatalogConfig,
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
    /// A durable control plane owns the revisioned serving resources; bootstrap
    /// TOML shrinks to what a process needs before it can read anything else.
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
const OVERRIDE_KEYS: [&str; 29] = [
    "mode",
    "server",
    "control_plane",
    "convergence",
    "secret_store",
    "admin_breakglass",
    "admin_oidc",
    "namespace",
    "provider",
    "model",
    "credential",
    "credential_pool",
    "failover",
    "transport",
    "reload",
    "shutdown",
    "gateway_key",
    "gateway_verifier",
    "gateway_minting",
    "gateway_token_epoch",
    "gateway_token",
    "usage_sink",
    "usage_journal",
    "budget",
    "rate_limit",
    "core_middleware",
    "admission",
    "revocation",
    "catalog",
];

/// Whether one segment of a namespace id is a slug: ASCII alphanumerics, `-`,
/// and `_`, beginning and ending alphanumeric.
///
/// The rule the durable [`Slug`](crate::desired_state::Slug) already enforces,
/// restated over a `String` because a compiled config carries the rendered id and
/// not the typed one.
fn is_namespace_segment(segment: &str) -> bool {
    let alphanumeric = |character: char| character.is_ascii_alphanumeric();
    segment.starts_with(alphanumeric)
        && segment.ends_with(alphanumeric)
        && segment
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
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

/// ADR 0062 environment identifiers are object-key segments, not display
/// names: lowercase ASCII with stable boundaries and a deliberately small
/// grammar that needs no URL or provider normalization.
fn validate_object_storage_environment_id(value: &str) -> Result<(), ConfigError> {
    const MAX_LEN: usize = 128;
    let trimmed = value.trim();
    if value != trimmed {
        return Err(ConfigError::Invalid(
            "`[control_plane] environment_id` must not contain surrounding whitespace".into(),
        ));
    }
    let value = trimmed;
    if value.is_empty() {
        return Err(ConfigError::Invalid(
            "`[control_plane] environment_id` must not be empty".into(),
        ));
    }
    if value.len() > MAX_LEN {
        return Err(ConfigError::Invalid(format!(
            "`[control_plane] environment_id` exceeds the {MAX_LEN}-byte object-key segment limit"
        )));
    }
    let bytes = value.as_bytes();
    let boundary = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    if !boundary(bytes[0]) || !boundary(bytes[bytes.len() - 1]) {
        return Err(ConfigError::Invalid(
            "`[control_plane] environment_id` must begin and end with a lowercase ASCII letter or digit"
                .into(),
        ));
    }
    if bytes.iter().any(|byte| {
        !byte.is_ascii_lowercase() && !byte.is_ascii_digit() && !matches!(byte, b'-' | b'_' | b'.')
    }) {
        return Err(ConfigError::Invalid(
            "`[control_plane] environment_id` may contain only lowercase ASCII letters, digits, `-`, `_`, and `.`"
                .into(),
        ));
    }
    // Keep the configuration grammar tied to the adapter-neutral key contract.
    crate::backends::object_store::ObjectKey::parse(format!("environments/{value}/head.json"))
        .map_err(|error| {
            ConfigError::Invalid(format!(
                "`[control_plane] environment_id` cannot form an object key: {error}"
            ))
        })?;
    Ok(())
}

fn validate_object_storage_container_url(
    raw: &str,
    allow_loopback_http: bool,
) -> Result<(), ConfigError> {
    if raw != raw.trim() {
        return Err(ConfigError::Invalid(
            "`[control_plane] container_url` must not contain surrounding whitespace".into(),
        ));
    }
    let url = reqwest::Url::parse(raw).map_err(|_| {
        ConfigError::Invalid(
            "`[control_plane] container_url` must be an absolute container URL".into(),
        )
    })?;
    if url.username() != "" || url.password().is_some() {
        return Err(ConfigError::Invalid(
            "`[control_plane] container_url` must not contain user information or credentials"
                .into(),
        ));
    }
    if url.query().is_some() {
        return Err(ConfigError::Invalid(
            "`[control_plane] container_url` must not contain a query (including a SAS token)"
                .into(),
        ));
    }
    if url.fragment().is_some() {
        return Err(ConfigError::Invalid(
            "`[control_plane] container_url` must not contain a fragment".into(),
        ));
    }
    let host = url.host_str().ok_or_else(|| {
        ConfigError::Invalid("`[control_plane] container_url` must include a host".into())
    })?;
    let loopback = host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    match url.scheme() {
        "https" if !allow_loopback_http => {}
        "http" if allow_loopback_http && loopback => {}
        "https" => {
            return Err(ConfigError::Invalid(
                "`[control_plane] allow_loopback_http` is only valid with an `http://` loopback Azurite endpoint"
                    .into(),
            ));
        }
        "http" => {
            return Err(ConfigError::Invalid(
                "`[control_plane] container_url` must use HTTPS; loopback HTTP requires `allow_loopback_http = true` for local development"
                    .into(),
            ));
        }
        _ => {
            return Err(ConfigError::Invalid(
                "`[control_plane] container_url` must use HTTPS".into(),
            ));
        }
    }
    let path = url.path();
    let container = path.strip_prefix('/').unwrap_or(path);
    if container.is_empty()
        || container.contains('/')
        || container.contains('%')
        || path.ends_with('/')
    {
        return Err(ConfigError::Invalid(
            "`[control_plane] container_url` path must contain exactly one unescaped container object-key segment"
                .into(),
        ));
    }
    crate::backends::object_store::ObjectKey::parse(container.to_owned()).map_err(|error| {
        ConfigError::Invalid(format!(
            "`[control_plane] container_url` has an invalid container object-key segment: {error}"
        ))
    })?;
    Ok(())
}

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

/// How an object-store adapter acquires short-lived credentials.
///
/// The configuration deliberately has no bearer-token, account-key, client
/// secret, or SAS variant. Workload identity is resolved by the production
/// adapter's credential chain and this enum records only that choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ObjectStorageAuthentication {
    WorkloadIdentity,
}

const DEFAULT_OBJECT_STORAGE_BOUND_BYTES: usize = 16 * 1024 * 1024;
const MAX_OBJECT_STORAGE_BOUND_BYTES: usize = 64 * 1024 * 1024;

fn default_object_storage_bound_bytes() -> usize {
    DEFAULT_OBJECT_STORAGE_BOUND_BYTES
}

/// Connectivity to the durable control plane, discriminated by `backend`.
///
/// Omitting `backend` preserves the original PostgreSQL configuration contract.
/// Object storage carries only a credential-free container URL, deployment
/// identity, bounded operation settings, and an authentication mode. Parsing
/// connects to nothing; runtime object-store wiring is a separate slice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlane {
    /// Preferred object storage or the legacy/optional PostgreSQL backend.
    pub backend: ControlPlaneBackend,
    /// Name of the env var holding the legacy Postgres connection string.
    pub dsn_env: Option<String>,
    /// The PostgreSQL schema the journal lives in, if not the connection's
    /// default. A plain identifier: it is interpolated into `SET search_path`,
    /// so it is validated here rather than trusted.
    pub schema: Option<String>,
    /// Whether a replica may apply pending migrations while booting.
    ///
    /// Off by default, which is the safe order: `axond migrate apply` moves the
    /// schema forward once, before replicas start, so a rollout cannot have one
    /// replica migrating a database the others are already reading. A boot
    /// always *checks* the schema either way — this only decides whether it may
    /// also change it.
    pub migrate: bool,
    /// Stable object-key segment selecting this deployment environment.
    pub environment_id: Option<String>,
    /// Absolute provider container URL. It never includes credentials or a SAS
    /// query; the adapter authenticates independently.
    pub container_url: Option<String>,
    /// Credential acquisition contract for the object-store adapter.
    pub authentication: Option<ObjectStorageAuthentication>,
    /// Absolute ceiling for any one object accepted by this deployment.
    pub max_object_bytes: usize,
    /// Ceiling enforced while streaming an exact-key read.
    pub max_read_bytes: usize,
    /// Ceiling enforced before issuing a conditional write.
    pub max_write_bytes: usize,
    /// Explicit local-development exception for Azurite over loopback HTTP.
    pub allow_loopback_http: bool,
    /// Bound on establishing a control-plane connection.
    pub connect_timeout_ms: u64,
    /// Bound on one control-plane operation, including a PostgreSQL migration.
    /// Generous by inference-path standards: nothing here runs with a request
    /// in flight.
    pub operation_timeout_ms: u64,
    // Presence bits retain the distinction between an omitted field and an
    // explicitly supplied default, so mixed backend contracts fail closed.
    pub(crate) migrate_explicit: bool,
    pub(crate) object_storage_fields_explicit: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawControlPlane {
    #[serde(default)]
    backend: ControlPlaneBackend,
    #[serde(default)]
    dsn_env: Option<String>,
    #[serde(default)]
    schema: Option<String>,
    #[serde(default)]
    migrate: Option<bool>,
    #[serde(default)]
    environment_id: Option<String>,
    #[serde(default)]
    container_url: Option<String>,
    #[serde(default)]
    authentication: Option<ObjectStorageAuthentication>,
    #[serde(default)]
    max_object_bytes: Option<usize>,
    #[serde(default)]
    max_read_bytes: Option<usize>,
    #[serde(default)]
    max_write_bytes: Option<usize>,
    #[serde(default)]
    allow_loopback_http: Option<bool>,
    #[serde(default = "default_control_plane_connect_timeout_ms")]
    connect_timeout_ms: u64,
    #[serde(default = "default_control_plane_operation_timeout_ms")]
    operation_timeout_ms: u64,
}

impl<'de> Deserialize<'de> for ControlPlane {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = RawControlPlane::deserialize(deserializer)?;
        let object_storage_fields_explicit = raw.environment_id.is_some()
            || raw.container_url.is_some()
            || raw.authentication.is_some()
            || raw.max_object_bytes.is_some()
            || raw.max_read_bytes.is_some()
            || raw.max_write_bytes.is_some()
            || raw.allow_loopback_http.is_some();
        Ok(Self {
            backend: raw.backend,
            dsn_env: raw.dsn_env,
            schema: raw.schema,
            migrate: raw.migrate.unwrap_or(false),
            environment_id: raw.environment_id,
            container_url: raw.container_url,
            authentication: raw.authentication,
            max_object_bytes: raw
                .max_object_bytes
                .unwrap_or_else(default_object_storage_bound_bytes),
            max_read_bytes: raw
                .max_read_bytes
                .unwrap_or_else(default_object_storage_bound_bytes),
            max_write_bytes: raw
                .max_write_bytes
                .unwrap_or_else(default_object_storage_bound_bytes),
            allow_loopback_http: raw.allow_loopback_http.unwrap_or(false),
            connect_timeout_ms: raw.connect_timeout_ms,
            operation_timeout_ms: raw.operation_timeout_ms,
            migrate_explicit: raw.migrate.is_some(),
            object_storage_fields_explicit,
        })
    }
}

/// The small process-local surface needed to make a stateful replica recoverable
/// during a control-plane outage. Omitting both fields disables the local cache,
/// which deliberately leaves cold boot fail-closed when the journal is absent.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
pub struct ConvergenceConfig {
    /// Durable path shared with replacement replicas on the local volume.
    #[serde(default)]
    pub cache_path: Option<String>,
    /// Environment variable containing the deployment-wide HMAC key reference.
    /// Its value must be canonical padded base64 encoding of exactly 32 CSPRNG
    /// bytes; the key itself never enters configuration or diagnostics.
    #[serde(default)]
    pub cache_key_env: Option<String>,
}

fn default_control_plane_connect_timeout_ms() -> u64 {
    5_000
}

fn default_control_plane_operation_timeout_ms() -> u64 {
    30_000
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
    /// The PostgreSQL schema the store's table lives in, if not the connection's
    /// default. A plain identifier, validated here because it is interpolated
    /// into `SET search_path`.
    #[serde(default)]
    pub schema: Option<String>,
    /// Whether boot may apply the shipped `secret_store_v1.sql`.
    ///
    /// On by default, unlike `[control_plane] migrate`: this DDL is a single
    /// `CREATE TABLE IF NOT EXISTS`, not a migration ledger a rollout can race
    /// on. An operator who applies it out of band turns it off and gets a
    /// refusal at boot instead of a schema change.
    #[serde(default = "default_secret_store_create_table")]
    pub create_table: bool,
}

fn default_secret_store_create_table() -> bool {
    true
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

/// The non-secret OIDC verifier configuration for human administration.
///
/// JWKS is configured explicitly rather than discovered from an untrusted token
/// issuer. This keeps the network destination operator-owned and makes the
/// bootstrap contract deterministic; the endpoint is fetched only by the
/// administrative authentication path and is cached between requests.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AdminOidc {
    /// Exact `iss` claim accepted in an ID/access token.
    pub issuer: String,
    /// Audience accepted in the token's `aud` claim.
    pub audience: String,
    /// HTTPS (or a loopback HTTP endpoint for local qualification) JWKS URL.
    pub jwks_url: String,
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
    /// The durable objects this namespace *is*, when a projection made it from
    /// desired state; `None` for a namespace a file declared.
    ///
    /// Never read from TOML: a file has no tenants or projects to name, and an id
    /// shaped like one would be a claim about durable state the file cannot make.
    ///
    /// Consumed by the runtime, which keys per-namespace durable state on it
    /// rather than on the renameable [`Namespace::id`]. A projected namespace
    /// is served only after a complete stateful candidate has been published.
    #[serde(skip)]
    #[allow(dead_code)]
    pub project: Option<ProjectIdentity>,
    /// The policy document governing this namespace, as a revision published it;
    /// `None` when the bootstrap file's limits govern it (#150).
    ///
    /// Never read from TOML, for the same reason [`Namespace::project`] is not: a
    /// file cannot claim a generation, and the values it *can* state live in
    /// `[budget]` and `[rate_limit]`.
    #[serde(skip)]
    pub policy: Option<NamespacePolicy>,
}

/// A published policy document, and the generation it is enforced under.
///
/// Carried on the namespace rather than resolved per request so that one
/// compiled snapshot answers "what governs this namespace, under which
/// generation" without reading desired state on the request path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespacePolicy {
    pub body: PolicyBody,
    pub generation: PolicyGeneration,
}

/// What a projected namespace is, independently of what it is called.
///
/// [`Namespace::id`] is a *name*: it is derived from a tenant's and a project's
/// slugs because a request names a namespace, and an id no operator can read
/// would make every metric label, log line, and budget report unreadable. Slugs
/// are renameable, though, so the name is not identity — and budgets, credential
/// pools, and gateway-key bindings are keyed per namespace, which is exactly the
/// state a rename must not re-key.
///
/// So a projected namespace carries both: the name it is reached by, and the ids
/// it *is*. Durable, per-namespace state belongs to this pair; a rename then
/// changes what callers say and nothing that was accounted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProjectIdentity {
    pub tenant: TenantId,
    pub project: ProjectId,
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
    /// The namespace that owns this alias, or `None` for one every namespace
    /// may reach.
    ///
    /// An owned alias is invisible and unroutable outside its namespace, which is
    /// what makes an alias name a *tenant's* name rather than the deployment's:
    /// two namespaces may publish `fast` over different targets, and neither can
    /// enumerate or invoke the other's (ADR 0058). An unowned alias is the
    /// single-tenant configuration every release before this one wrote, so a file
    /// that names no namespace behaves exactly as it did.
    ///
    /// Ownership is not entitlement on its own: a namespace still sees only the
    /// aliases it holds a credential for, and an alias scope still narrows what a
    /// key may name.
    #[serde(default)]
    pub namespace: Option<String>,
    pub targets: Vec<Target>,
}

impl Model {
    /// Whether `namespace` may see and invoke this alias.
    pub fn reachable_from(&self, namespace: &str) -> bool {
        self.namespace
            .as_deref()
            .is_none_or(|owner| owner == namespace)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Target {
    pub provider: String,
    /// Concrete upstream model / deployment id.
    pub model: String,
    /// Per-token pricing for this concrete target, in micro-dollars per million
    /// tokens. Required — a target that can't be priced can't be budget-checked,
    /// so an unpriced target fails config parsing at boot (delta B2).
    ///
    /// What a request is *actually* charged at is
    /// [`RequestPrice`](crate::pricing::RequestPrice): a deployment whose serving
    /// snapshot carries an approved price book covering this target's [`catalog`]
    /// binding is billed from the book instead (ADR 0056). These rates stay the
    /// authority for a file-configured deployment, and for a target the book does
    /// not claim.
    ///
    /// [`catalog`]: Target::catalog
    pub price: ModelPrice,
    /// The catalogue offering this target calls, when the operator bound it to
    /// one.
    ///
    /// Optional and explicit. An approved price book prices *catalogue
    /// offerings*, keyed by the upstream's own provider id and the id that
    /// provider publishes a model under — neither of which the `[[provider]] id`
    /// and `model` above are: those are operator-chosen routing names that only
    /// coincide with the catalogue's vocabulary by accident. Inferring the
    /// binding from them would silently bill one provider's rates for another's
    /// traffic, so an unbound target is simply not something a book prices.
    #[serde(default)]
    pub catalog: Option<CatalogBinding>,
}

/// The catalogue offering a routed target calls.
///
/// The pair an approved price rule is keyed by
/// ([`PricedTarget`](crate::desired_state::pricing::PricedTarget)): the
/// catalogue's provider id, and that provider's own published model id. Parsed at
/// boot, so the request path holds an identifier the pricing domain can be asked
/// about directly and a malformed binding is a startup refusal rather than a
/// silently unpriced target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogBinding {
    pub provider: ProviderId,
    /// The id the provider publishes the model under — what a request to that
    /// provider carries, which is what a price rule is keyed by.
    pub model: String,
}

impl CatalogBinding {
    /// A binding from the two ids an operator writes, refusing an id the
    /// catalogue vocabulary cannot hold.
    pub fn new(provider: &str, model: &str) -> Result<Self, InvalidCatalogId> {
        Ok(Self {
            provider: ProviderId::parse(provider)?,
            model: model.to_owned(),
        })
    }
}

impl<'de> Deserialize<'de> for CatalogBinding {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Stated {
            provider: String,
            model: String,
        }

        let stated = Stated::deserialize(deserializer)?;
        Self::new(&stated.provider, &stated.model).map_err(serde::de::Error::custom)
    }
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
    /// The exact secret version this credential's material is unwrapped from.
    ///
    /// Never read from TOML, for the reason [`Namespace::project`] is not: a file
    /// has no secret store to reference, and durable material is not something a
    /// process-local fact can name. A projection sets it, and
    /// [`Credentials::resolve`](crate::credentials::Credentials::resolve) takes
    /// the material from the set the candidate's compilation already unwrapped —
    /// so this field is a *reference*, and the plaintext it names never appears in
    /// a config value.
    #[serde(skip)]
    pub secret: Option<SecretRef>,
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
    /// Whether the store's keys are laid out to carry a scope-wide cap, when the
    /// cap's *value* is not this file's to state.
    ///
    /// Stateful mode only, and the reason it exists: the layout is a durable fact
    /// with a migration behind it, so it stays bootstrap-owned
    /// ([`BOOTSTRAP_OWNED_FIELDS`](crate::desired_state::policy::BOOTSTRAP_OWNED_FIELDS)),
    /// while the number it caps at is published. In stateless mode
    /// `namespace_limit_microdollars` states both at once and this is rejected as
    /// a redundant way to say half of it.
    #[serde(default)]
    pub namespace_scope: bool,
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
            namespace_scope: false,
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

/// Which implementation owns the fixed rate-limit and budget stages.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CoreAccountingMode {
    /// Previous straight-line ownership in `serve()`. Retained as an operational
    /// rollback while the middleware-owned path is qualified.
    Legacy,
    /// Fixed core middleware stages owned through the response lifetime.
    #[default]
    Middleware,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoreMiddlewareConfig {
    #[serde(default)]
    pub accounting: CoreAccountingMode,
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
/// spawned, so a deployment that hand-authors its models keeps exactly the
/// behaviour it has today and no third party is contacted on its behalf.
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

    /// Whether this store's keys are laid out to carry a scope-wide cap.
    ///
    /// One question, asked the same way by both modes: stateless says it by
    /// stating the cap, stateful says it directly because the cap is published.
    pub const fn enforces_namespace_scope(&self) -> bool {
        self.namespace_limit_microdollars.is_some() || self.namespace_scope
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

/// A recoverable inbound workload principal carried by a compiled stateful
/// revision.
///
/// The presented `axw1.` key is never stored here. Its digest is sufficient for
/// verification and is the only credential material the durable identity model
/// exposes. `namespace` is already resolved by the projection because request
/// authentication has no later namespace-selection step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectedPrincipal {
    pub(crate) namespace: String,
    pub(crate) subject: String,
    pub(crate) digest: crate::desired_state::Checksum,
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
        self.validate_resource_graph()
    }

    /// The whole-graph gate over resources and policies, independent of which
    /// authority produced them.
    ///
    /// Split out of [`Config::validate_stateless`] so a candidate compiled from a
    /// durable revision runs *this* gate rather than a second implementation of
    /// it (#142): an alias pointing at an undefined provider, a wire-family
    /// mismatch across failover targets, and a credential naming an unknown
    /// namespace must be rejected identically whether they arrived from TOML or
    /// from Postgres. The mode-specific section rules are deliberately not here —
    /// those govern what a *file* may say, and a compiled candidate is not a
    /// file.
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

        // An alias name is unique within the namespace that owns it, not across
        // the deployment: two tenants naming their own `fast` is the point of
        // ownership. What must stay unique is the pair, because resolution takes
        // the first row that matches and a second row for the same pair would be
        // unreachable configuration.
        let mut owned: HashSet<(Option<&str>, &str)> = HashSet::new();

        for principal in &self.projected_principals {
            if principal.subject.trim().is_empty() {
                return Err(ConfigError::Invalid(
                    "a projected inbound principal must have a non-empty subject".into(),
                ));
            }
            if !namespaces.contains_key(principal.namespace.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "projected inbound principal `{}` references undefined namespace `{}`",
                    principal.subject, principal.namespace
                )));
            }
        }
        for model in &self.model {
            if model.targets.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "model `{}` has no targets",
                    model.name
                )));
            }
            if let Some(namespace) = model.namespace.as_deref()
                && !namespaces.contains_key(namespace)
            {
                return Err(ConfigError::Invalid(format!(
                    "model `{}` is owned by undefined namespace `{namespace}`",
                    model.name
                )));
            }
            if !owned.insert((model.namespace.as_deref(), model.name.as_str())) {
                return Err(ConfigError::Invalid(match model.namespace.as_deref() {
                    Some(namespace) => format!(
                        "namespace `{namespace}` defines model `{}` twice",
                        model.name
                    ),
                    None => format!("model `{}` is defined twice", model.name),
                }));
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
            // Exactly one source of material. A file names an env var; a
            // projection names an exact secret version. Both would be two
            // authorities over one credential, and neither is a credential at all.
            match (c.env.as_deref().map(str::trim), c.secret) {
                (Some("") | None, None) => {
                    return Err(ConfigError::Invalid(format!(
                        "credential for namespace `{}` provider `{}` has an empty `env`",
                        c.namespace, c.provider
                    )));
                }
                (Some(env), Some(reference)) if !env.is_empty() => {
                    return Err(ConfigError::Invalid(format!(
                        "credential `{}` for namespace `{}` provider `{}` names both env var \
                         `{env}` and secret `{reference}`; material has one source",
                        c.label(),
                        c.namespace,
                        c.provider
                    )));
                }
                _ => {}
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
        // A stateful candidate may replace file-owned gateway keys with
        // digest-backed principals projected from durable workload identities.
        // The empty-key refusal still applies to every candidate: this branch
        // is reachable only when at least one projected principal has already
        // passed the namespace and subject checks above.
        if self.mode != Mode::Stateful || self.projected_principals.is_empty() {
            self.validate_gateway_keys(&namespaces)?;
        }
        self.validate_gateway_verifiers(&namespaces)?;
        self.validate_gateway_minting(&namespaces)?;
        self.validate_gateway_token_epochs(&namespaces)?;
        self.validate_usage_sinks()?;
        // A compiled stateful candidate carries the bootstrap backend
        // selection, but its cap values arrive in the durable policy attached
        // to each projected namespace. Re-running the file-level budget-value
        // gate here would reject the deliberate stateful shape
        // (`limit_microdollars = 0`) before PolicyRuntime can apply those
        // published values. The bootstrap path already validates backend
        // connectivity and layout; stateless candidates still validate their
        // complete file-owned budget.
        if self.mode != Mode::Stateful {
            self.validate_budget()?;
        }
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
        self.validate_convergence()?;
        self.validate_control_plane()?;
        self.validate_secret_store()?;
        self.validate_admin_breakglass()?;
        self.validate_admin_oidc()?;
        self.validate_process_local_bounds()?;
        self.validate_usage_sinks()?;
        self.validate_hot_state_connectivity()?;
        self.validate_budget_layout()?;
        self.validate_revocation()?;
        Ok(())
    }

    /// The last-known-good cache is an all-or-nothing bootstrap dependency.
    /// A path without its signing-key reference would look configured while
    /// remaining unusable, and a reference without a path would leave an
    /// operator believing cold-boot recovery was enabled when it is not.
    /// References also pass through the same override-collision guard as every
    /// other secret-bearing environment name.
    fn validate_convergence(&self) -> Result<(), ConfigError> {
        let cache_path = self
            .convergence
            .cache_path
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let cache_key_env = self
            .convergence
            .cache_key_env
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        match (cache_path, cache_key_env) {
            (None, None) => Ok(()),
            (Some(_), Some(name)) => {
                reject_env_override_collision("[convergence] cache_key_env", name)
            }
            _ => Err(ConfigError::Invalid(
                "`[convergence]` requires both `cache_path` and `cache_key_env` when a \
                 last-known-good cache is configured"
                    .into(),
            )),
        }
    }

    /// Bounds the process applies to itself, in both modes: per-phase upstream
    /// limits, the inbound admission ceilings, how often the file is re-read,
    /// and how long termination may take. They are process-local serving
    /// parameters rather than durable resources, so the control plane does not
    /// own them.
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
        self.validate_catalog()?;
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
        if self.admin_oidc.is_some() {
            sections.push("`[admin_oidc]`");
        }
        if self.convergence != ConvergenceConfig::default() {
            sections.push("`[convergence]`");
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
             `[[admin_breakglass]]`, `[catalog]`, and backend selection plus DSN references for \
             the opt-in `[budget]`, `[rate_limit]`, and `[revocation]` backends",
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

    /// A stateful process with no control-plane reference has nothing to serve.
    /// The backend discriminator makes PostgreSQL and object-storage fields
    /// mutually exclusive before any adapter sees them.
    fn validate_control_plane(&self) -> Result<(), ConfigError> {
        let Some(control_plane) = &self.control_plane else {
            return Err(ConfigError::Invalid(
                "`mode = \"stateful\"` requires a `[control_plane]` section: the control plane owns \
                 every durable resource, so a stateful process with no control-plane reference has \
                 nothing to serve"
                    .into(),
            ));
        };
        if control_plane.connect_timeout_ms == 0 {
            return Err(ConfigError::Invalid(
                "`[control_plane] connect_timeout_ms` must be at least 1".into(),
            ));
        }
        if control_plane.operation_timeout_ms == 0 {
            return Err(ConfigError::Invalid(
                "`[control_plane] operation_timeout_ms` must be at least 1: control-plane \
                 operations are bounded"
                    .into(),
            ));
        }
        match control_plane.backend {
            ControlPlaneBackend::Postgres => {
                if control_plane.object_storage_fields_explicit {
                    return Err(ConfigError::Invalid(
                        "`[control_plane] backend = \"postgres\"` rejects object-storage-only \
                         fields (`environment_id`, `container_url`, `authentication`, object \
                         bounds, and `allow_loopback_http`)"
                            .into(),
                    ));
                }
                let dsn_env = control_plane
                    .dsn_env
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        ConfigError::Invalid(
                            "`[control_plane] dsn_env` must name the env var holding the legacy \
                             Postgres control-plane connection string"
                                .into(),
                        )
                    })?;
                if let Some(schema) = control_plane.schema.as_deref() {
                    validate_schema_name("[control_plane] schema", schema)?;
                }
                reject_env_override_collision("[control_plane] dsn_env", dsn_env)
            }
            ControlPlaneBackend::ObjectStorage => {
                if control_plane.dsn_env.is_some()
                    || control_plane.schema.is_some()
                    || control_plane.migrate_explicit
                {
                    return Err(ConfigError::Invalid(
                        "`[control_plane] backend = \"object-storage\"` rejects Postgres-only \
                         fields (`dsn_env`, `schema`, and `migrate`)"
                            .into(),
                    ));
                }
                let environment_id = control_plane.environment_id.as_deref().ok_or_else(|| {
                    ConfigError::Invalid(
                        "`[control_plane] backend = \"object-storage\"` requires \
                             `environment_id`"
                            .into(),
                    )
                })?;
                validate_object_storage_environment_id(environment_id)?;
                let container_url = control_plane.container_url.as_deref().ok_or_else(|| {
                    ConfigError::Invalid(
                        "`[control_plane] backend = \"object-storage\"` requires \
                             `container_url`"
                            .into(),
                    )
                })?;
                if control_plane.authentication.is_none() {
                    return Err(ConfigError::Invalid(
                        "`[control_plane] backend = \"object-storage\"` requires \
                         `authentication = \"workload-identity\"`"
                            .into(),
                    ));
                }
                for (field, value) in [
                    ("max_object_bytes", control_plane.max_object_bytes),
                    ("max_read_bytes", control_plane.max_read_bytes),
                    ("max_write_bytes", control_plane.max_write_bytes),
                ] {
                    if value == 0 {
                        return Err(ConfigError::Invalid(format!(
                            "`[control_plane] {field}` must be at least 1"
                        )));
                    }
                    if value > MAX_OBJECT_STORAGE_BOUND_BYTES {
                        return Err(ConfigError::Invalid(format!(
                            "`[control_plane] {field}` exceeds the \
                             {MAX_OBJECT_STORAGE_BOUND_BYTES}-byte safety limit"
                        )));
                    }
                }
                if control_plane.max_read_bytes > control_plane.max_object_bytes
                    || control_plane.max_write_bytes > control_plane.max_object_bytes
                {
                    return Err(ConfigError::Invalid(
                        "`[control_plane] max_read_bytes` and `max_write_bytes` must not exceed \
                         `max_object_bytes`"
                            .into(),
                    ));
                }
                validate_object_storage_container_url(
                    container_url,
                    control_plane.allow_loopback_http,
                )
            }
        }
    }

    /// Secret material is resolved during snapshot compilation, so a stateful
    /// process needs a store and a KEK *reference* before it can compile
    /// anything. Both are named here; neither is ever a value.
    fn validate_secret_store(&self) -> Result<(), ConfigError> {
        if self
            .control_plane
            .as_ref()
            .is_some_and(|plane| plane.backend == ControlPlaneBackend::ObjectStorage)
        {
            if self.secret_store.is_some() {
                return Err(ConfigError::Invalid(
                    "`[secret_store]` is the legacy Postgres secret-store bootstrap and cannot be \
                     mixed with `[control_plane] backend = \"object-storage\"`; blob secret \
                     runtime wiring is pending"
                        .into(),
                ));
            }
            return Ok(());
        }
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
        // Interpolated into `SET search_path`, so it is validated at boot rather
        // than trusted at connection time — exactly as `[control_plane] schema`
        // is.
        if let Some(schema) = secret_store
            .schema
            .as_deref()
            .map(str::trim)
            .filter(|schema| !schema.is_empty())
        {
            validate_schema_name("[secret_store] schema", schema)?;
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

    /// Validate the operator-owned OIDC network boundary without contacting the
    /// provider. Human administration remains optional because the mandatory
    /// breakglass credential is the recovery path when no IdP is configured or
    /// when it is unavailable.
    fn validate_admin_oidc(&self) -> Result<(), ConfigError> {
        let Some(oidc) = self.admin_oidc.as_ref() else {
            return Ok(());
        };
        for (field, value) in [
            ("issuer", oidc.issuer.trim()),
            ("audience", oidc.audience.trim()),
            ("jwks_url", oidc.jwks_url.trim()),
        ] {
            if value.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "`[admin_oidc] {field}` must not be empty"
                )));
            }
        }
        let issuer = reqwest::Url::parse(oidc.issuer.trim()).map_err(|error| {
            ConfigError::Invalid(format!(
                "`[admin_oidc] issuer` is not an absolute URL: {error}"
            ))
        })?;
        let jwks = reqwest::Url::parse(oidc.jwks_url.trim()).map_err(|error| {
            ConfigError::Invalid(format!(
                "`[admin_oidc] jwks_url` is not an absolute URL: {error}"
            ))
        })?;
        if !matches!(issuer.scheme(), "https" | "http") {
            return Err(ConfigError::Invalid(
                "`[admin_oidc] issuer` must use `https` (or `http` for a local qualification endpoint)"
                    .into(),
            ));
        }
        if !matches!(jwks.scheme(), "https" | "http") {
            return Err(ConfigError::Invalid(
                "`[admin_oidc] jwks_url` must use `https` (or `http` for a local qualification endpoint)"
                    .into(),
            ));
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

    /// The one budget rule a *stateful* file still owns: whether the ledger's
    /// keys carry a scope-wide cap.
    ///
    /// Split out of [`Config::validate_budget`] because that gate reads values
    /// the control plane publishes in stateful mode, so stateful boot does not
    /// run it — and this layout claim is only meaningful in exactly that mode.
    /// Left inside it, the check would fire only where it cannot apply, and a
    /// stateful file declaring `namespace_scope` on a per-replica backend would
    /// boot and then refuse every published revision instead of refusing to
    /// start.
    fn validate_budget_layout(&self) -> Result<(), ConfigError> {
        let budget = &self.budget;
        let backend = budget.backend.as_str();
        if !budget.namespace_scope {
            return Ok(());
        }
        if !budget.backend.is_shared() {
            return Err(ConfigError::Invalid(format!(
                "budget `{backend}`: namespace_scope is supported only by `redis` and \
                 `postgres`, which enforce a scope-wide cap exactly across replicas"
            )));
        }
        if budget.namespace_limit_microdollars.is_some() {
            return Err(ConfigError::Invalid(format!(
                "budget `{backend}`: namespace_limit_microdollars already declares the \
                 scope-wide layout, so namespace_scope restates it. Set the limit in \
                 stateless mode, and namespace_scope in stateful mode, where the limit is \
                 published rather than declared"
            )));
        }
        if self.mode != Mode::Stateful {
            return Err(ConfigError::Invalid(format!(
                "budget `{backend}`: namespace_scope declares a layout whose cap the control \
                 plane publishes, so it is only meaningful under `mode = \"stateful\"`. Set \
                 namespace_limit_microdollars instead"
            )));
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
        self.validate_budget_layout()?;
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
                .or_else(|| {
                    self.control_plane
                        .as_ref()
                        .and_then(|plane| plane.dsn_env.as_deref())
                })
                .map(str::trim)
                .filter(|name| !name.is_empty());
            if dsn_env.is_none() {
                return Err(ConfigError::Invalid(
                    "catalog `postgres`: `dsn_env` must name the env var holding the connection \
                     string (or configure `[control_plane]`, whose `dsn_env` it inherits)"
                        .into(),
                ));
            }
            if let Some(schema) = catalog.schema.as_deref() {
                validate_schema_name("catalog.schema", schema)?;
            }
        } else if self.mode == Mode::Stateful {
            return Err(ConfigError::Invalid(
                "catalog `in-memory`: a stateful deployment must retain imported catalogues in \
                 `postgres`, since an in-memory store loses every snapshot and its provenance on \
                 restart"
                    .into(),
            ));
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

    /// An alias by name, ignoring ownership: "does this deployment define `fast`
    /// at all", which is a question about a file rather than about a caller.
    ///
    /// Deliberately not reachable from a request. Resolution goes through
    /// [`Config::model_for`], which cannot return an alias another namespace owns,
    /// and a deployment-wide lookup on the request path would defeat that.
    #[cfg(test)]
    pub fn model(&self, name: &str) -> Option<&Model> {
        self.model.iter().find(|m| m.name == name)
    }

    /// The alias `name` resolves to *for* `namespace`: the namespace's own row if
    /// it owns one, otherwise an unowned row.
    ///
    /// The precedence is the same one desired state gives a project override over
    /// a tenant default (ADR 0042): a namespace that names an alias replaces the
    /// deployment-wide one for itself alone, and nothing here can reach a row
    /// another namespace owns.
    pub fn model_for(&self, namespace: &str, name: &str) -> Option<&Model> {
        self.model
            .iter()
            .find(|m| m.name == name && m.namespace.as_deref() == Some(namespace))
            .or_else(|| {
                self.model
                    .iter()
                    .find(|m| m.name == name && m.namespace.is_none())
            })
    }

    /// Run the boot-time resource-graph gate on a config this process compiled
    /// rather than parsed.
    ///
    /// Stateful convergence (#142) builds a candidate by filling the
    /// control-plane-owned resources into the bootstrap config it booted with, so
    /// the candidate legitimately carries both `mode = "stateful"` and the
    /// resource sections [`Config::validate`] refuses to read from a file. What
    /// must still hold is every whole-graph invariant boot enforces, which is
    /// exactly what this runs.
    pub(crate) fn validate_compiled(&self) -> Result<(), ConfigError> {
        self.validate_namespace_ids()?;
        self.validate_resource_graph()
    }

    /// The charset and uniqueness gate for namespace ids a *process* wrote.
    ///
    /// Only those: a file's namespace ids are reviewed by whoever wrote them, and
    /// duplicates there are deliberately legal (see
    /// [`Config::distinct_namespace_count`], and the budget key parser that treats
    /// a repeated id as one namespace). Holding a file's ids here would let a
    /// configuration boot and then refuse every published revision forever,
    /// blaming a name boot accepted. Generated ids are written by no one: a
    /// projection derives them from durable state, and they end up in metric label
    /// values, in Redis and Postgres key composition, and in gateway-key
    /// bindings — so [`Namespace::project`] is what marks an id this holds.
    ///
    /// Two things have to hold of a generated id. It is one slug, or two joined by
    /// `/` (a project under its tenant, `acme/core`), where a slug is ASCII
    /// alphanumerics, `-`, and `_`, beginning and ending alphanumeric. And it is
    /// claimed by nothing else in the config — another projected namespace or a
    /// declared one — because sharing a name would put two namespaces' budgets,
    /// credential pools, and key bindings on it.
    fn validate_namespace_ids(&self) -> Result<(), ConfigError> {
        for namespace in self
            .namespace
            .iter()
            .filter(|namespace| namespace.project.is_some())
        {
            let id = namespace.id.as_str();
            let segments = id.split('/').collect::<Vec<_>>();
            let shaped = matches!(segments.len(), 1 | 2)
                && segments.iter().all(|segment| is_namespace_segment(segment));
            if !shaped {
                return Err(ConfigError::Invalid(format!(
                    "namespace `{id}` is not a usable identifier: a namespace id is a slug, or a \
                     project's slug qualified by its tenant's (`acme/core`), where a slug is \
                     ASCII letters, digits, `-`, and `_`, beginning and ending alphanumeric"
                )));
            }
            let claims = self
                .namespace
                .iter()
                .filter(|other| other.id == namespace.id)
                .count();
            if claims > 1 {
                return Err(ConfigError::Invalid(format!(
                    "namespace `{id}` is declared twice; one name cannot key two namespaces' \
                     budgets, credentials, and gateway keys"
                )));
            }
        }
        Ok(())
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
    use crate::desired_state::Uuid7;

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

    /// A catalogue binding is optional, and when written it is parsed into the
    /// pricing domain's own vocabulary at boot, so a binding no catalogue could
    /// hold is a startup refusal rather than a request that prices nothing.
    #[test]
    fn a_targets_catalogue_binding_is_optional_and_parsed_at_boot() {
        let unbound = Config::from_toml_str(VALID).expect("a target need not be bound");
        assert_eq!(unbound.model[0].targets[0].catalog, None);

        let bound = Config::from_toml_str(&VALID.replace(
            r#"model = "gpt-4o", price"#,
            r#"model = "gpt-4o", catalog = { provider = "openai", model = "gpt-4o-2024-08-06" }, price"#,
        ))
        .expect("a bound target parses");
        assert_eq!(
            bound.model[0].targets[0].catalog,
            Some(CatalogBinding::new("openai", "gpt-4o-2024-08-06").expect("a valid binding"))
        );

        let err = Config::from_toml_str(&VALID.replace(
            r#"model = "gpt-4o", price"#,
            r#"model = "gpt-4o", catalog = { provider = "Open AI", model = "gpt-4o" }, price"#,
        ))
        .expect_err("a provider id the catalogue cannot hold must not boot");
        assert!(matches!(err, ConfigError::Load(_)), "{err:?}");
    }

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
    fn a_compiled_namespace_id_is_held_to_a_shape_a_file_never_was() {
        // A projection's namespaces: what makes one is that a projection made it,
        // which is exactly what `project` records.
        let projected = |index: u64| ProjectIdentity {
            tenant: TenantId::new(Uuid7::from_parts(index, 0, index).expect("seed in range")),
            project: ProjectId::new(Uuid7::from_parts(index, 0, index + 1).expect("seed in range")),
        };
        let compiled = |ids: &[&str]| {
            let mut config = Config::from_toml_str(VALID).expect("the fixture is valid");
            config
                .namespace
                .extend(ids.iter().enumerate().map(|(index, id)| Namespace {
                    id: (*id).to_owned(),
                    default: false,
                    allow_platform_fallback: false,
                    project: Some(projected(index as u64 + 1)),
                    policy: None,
                }));
            config.validate_compiled()
        };

        // What a projection emits: a tenant-qualified project.
        compiled(&["acme/core", "globex/core"]).expect("a qualified id is a namespace id");

        // A generated id is not reviewed by anyone, so the charset is enforced
        // where the id is produced rather than trusted because it parsed.
        for rejected in [
            "acme/core/edge",
            "acme//core",
            "acme/",
            "/core",
            "acme core",
            "acme.core",
            "-acme/core",
            "acme/core-",
            "acme/cœur",
            "",
        ] {
            let error = compiled(&[rejected]).expect_err("a malformed id must not compile");
            assert!(
                error.to_string().contains("not a usable identifier"),
                "`{rejected}` is refused as a shape, not by a later gate: {error}"
            );
        }

        // A repeated id is legal in a file and cannot be in a compiled config:
        // there it would put two tenants' budgets, credentials, and keys on one
        // name.
        let error =
            compiled(&["acme/core", "acme/core"]).expect_err("a duplicate must not compile");
        assert!(error.to_string().contains("declared twice"), "{error}");
        let error = compiled(&["platform"]).expect_err("including one the file already declared");
        assert!(error.to_string().contains("declared twice"), "{error}");

        // And the file's own ids stay the file's business: a name boot accepted
        // cannot become a reason a replica converges on nothing forever.
        let mut declared = Config::from_toml_str(VALID).expect("the fixture is valid");
        declared
            .namespace
            .extend(["team.a", "platform"].iter().map(|id| Namespace {
                id: (*id).to_owned(),
                default: false,
                allow_platform_fallback: false,
                project: None,
                policy: None,
            }));
        declared
            .validate_compiled()
            .expect("a declared id keeps whatever shape boot accepted");
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
        assert_eq!(
            cfg.core_middleware.accounting,
            CoreAccountingMode::Middleware
        );
    }

    #[test]
    fn core_accounting_migration_gate_is_closed_and_typed() {
        let legacy = Config::from_toml_str(&format!(
            "{VALID}\n[core_middleware]\naccounting = \"legacy\"\n"
        ))
        .expect("legacy rollback mode parses");
        assert_eq!(
            legacy.core_middleware.accounting,
            CoreAccountingMode::Legacy
        );
        assert!(
            Config::from_toml_str(&format!(
                "{VALID}\n[core_middleware]\naccounting = \"other\"\n"
            ))
            .is_err()
        );
        assert!(
            Config::from_toml_str(&format!("{VALID}\n[core_middleware]\nunknown = true\n"))
                .is_err()
        );
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

    /// Two namespaces may publish the same alias name, and each resolves to its
    /// own: the pair `(namespace, name)` is what has to be unique, not the name.
    #[test]
    fn an_owned_alias_is_its_namespaces_own_and_shadows_the_deployments() {
        let cfg = Config::from_toml_str(&format!(
            r#"
{VALID}

[[namespace]]
id = "acme"

[[namespace]]
id = "globex"

[[model]]
name = "shared"
targets = [{{ provider = "openai", model = "gpt-4o", price = {{ input_microdollars_per_million = 1, output_microdollars_per_million = 1 }} }}]

[[model]]
name = "shared"
namespace = "acme"
targets = [{{ provider = "openai", model = "gpt-4o-mini", price = {{ input_microdollars_per_million = 1, output_microdollars_per_million = 1 }} }}]

[[model]]
name = "private"
namespace = "globex"
targets = [{{ provider = "openai", model = "o3", price = {{ input_microdollars_per_million = 1, output_microdollars_per_million = 1 }} }}]
"#
        ))
        .expect("an owned alias beside an unowned one");

        // The owner gets its own row; everyone else gets the unowned one.
        assert_eq!(
            cfg.model_for("acme", "shared").expect("acme's own").targets[0].model,
            "gpt-4o-mini"
        );
        assert_eq!(
            cfg.model_for("globex", "shared")
                .expect("the deployment's")
                .targets[0]
                .model,
            "gpt-4o"
        );
        // An owned alias is not reachable from anywhere else, with no unowned row
        // to fall back to.
        assert!(cfg.model_for("acme", "private").is_none());
        assert!(cfg.model_for("globex", "private").is_some());
    }

    #[test]
    fn rejects_an_alias_owned_by_a_namespace_the_deployment_does_not_define() {
        let toml = format!(
            r#"
{VALID}

[[model]]
name = "shared"
namespace = "nowhere"
targets = [{{ provider = "openai", model = "gpt-4o", price = {{ input_microdollars_per_million = 1, output_microdollars_per_million = 1 }} }}]
"#
        );
        assert!(matches!(
            Config::from_toml_str(&toml),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn rejects_one_namespace_defining_the_same_alias_twice() {
        let toml = format!(
            r#"
{VALID}

[[namespace]]
id = "acme"

[[model]]
name = "shared"
namespace = "acme"
targets = [{{ provider = "openai", model = "gpt-4o", price = {{ input_microdollars_per_million = 1, output_microdollars_per_million = 1 }} }}]

[[model]]
name = "shared"
namespace = "acme"
targets = [{{ provider = "openai", model = "o3", price = {{ input_microdollars_per_million = 1, output_microdollars_per_million = 1 }} }}]
"#
        );
        assert!(matches!(
            Config::from_toml_str(&toml),
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

    /// The minimum accepted ADR 0062 object-storage bootstrap. Runtime wiring
    /// intentionally remains a later slice; this proves the configuration does
    /// not smuggle PostgreSQL into the target topology.
    const BLOB_STATEFUL: &str = r#"
mode = "stateful"

[control_plane]
backend = "object-storage"
environment_id = "prod-us-east"
container_url = "https://axondstate.blob.core.windows.net/control-plane"
authentication = "workload-identity"

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

    /// `[admission]` bounds the process, not a durable resource, so the control
    /// plane never owns it and a stateful replica must refuse a nonsensical
    /// ceiling for the same reason a stateless one does: the alternative is a
    /// gateway that boots and then refuses every request.
    #[test]
    fn stateful_mode_validates_the_process_local_admission_bounds() {
        for (snippet, expected) in [
            ("max_request_bytes = 0", "admission.max_request_bytes"),
            (
                "max_in_flight = 4\nmax_in_flight_per_tenant = 8",
                "admission.max_in_flight_per_tenant",
            ),
            ("queue_capacity = 4", "admission.queue_wait_ms"),
        ] {
            let toml = format!("{STATEFUL}\n[admission]\n{snippet}\n");
            let error = Config::from_toml_str(&toml)
                .expect_err("a stateful replica refuses an invalid ceiling too");
            assert!(error.to_string().contains(expected), "{snippet} => {error}");
        }
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

    /// A compiled stateful revision gets its cap values from the durable policy,
    /// so the resource-graph gate must not re-run the file-level zero-cap check
    /// against the bootstrap connectivity shape.
    #[test]
    fn compiled_stateful_candidates_do_not_reject_bootstrap_budget_values() {
        let toml = format!(
            "{STATEFUL}\n[budget]\nbackend = \"postgres\"\ndsn_env = \"AXOND_BUDGET_DSN\"\nnamespace_scope = true\n"
        );
        let mut config = Config::from_toml_str(&toml).expect("shared layout is valid");
        config.namespace.push(Namespace {
            id: "platform".to_owned(),
            default: true,
            allow_platform_fallback: false,
            project: None,
            policy: None,
        });
        config.projected_principals.push(ProjectedPrincipal {
            namespace: "platform".to_owned(),
            subject: "workload".to_owned(),
            digest: crate::desired_state::Checksum::of(b"axw1.dddddd"),
        });
        config
            .validate_compiled()
            .expect("compiled policy supplies the budget values");
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

    /// `namespace_scope` is the one budget key stateful mode reads, and it is
    /// only meaningful there — so stateful boot, which skips the value gate the
    /// control plane owns, must still refuse a layout no per-replica backend can
    /// hold. Otherwise the replica boots and refuses every published revision
    /// forever, which is the same misconfiguration reported far from its cause.
    #[test]
    fn stateful_boot_refuses_a_scope_wide_layout_the_backend_cannot_enforce() {
        for backend in ["none", "in-memory"] {
            let toml =
                format!("{STATEFUL}\n[budget]\nbackend = \"{backend}\"\nnamespace_scope = true\n");
            let error = Config::from_toml_str(&toml)
                .expect_err("a per-replica ledger cannot carry a fleet-wide scope");
            assert!(
                matches!(error, ConfigError::Invalid(ref message)
                    if message.contains("namespace_scope is supported only by")),
                "{backend}: {error:?}"
            );
        }
        let toml = format!(
            "{STATEFUL}\n[budget]\nbackend = \"redis\"\ndsn_env = \"AXOND_REDIS_URL\"\nnamespace_scope = true\n"
        );
        Config::from_toml_str(&toml).expect("a shared backend may declare the layout");
    }

    /// Cold boot in stateful mode requires one complete durable control-plane
    /// reference, so an incomplete backend contract describes nothing to serve.
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
            (
                "`[control_plane] operation_timeout_ms`",
                "mode = \"stateful\"\n[control_plane]\ndsn_env = \"DSN\"\noperation_timeout_ms = 0\n[secret_store]\nkek_env = \"KEK\"\n[[admin_breakglass]]\nenv = \"BG\"",
            ),
            // The schema is interpolated into `SET search_path`, so anything
            // that is not a plain identifier is refused at load rather than at
            // the point it would become a statement.
            (
                "`[control_plane] schema`",
                "mode = \"stateful\"\n[control_plane]\ndsn_env = \"DSN\"\nschema = \"public; DROP SCHEMA public\"\n[secret_store]\nkek_env = \"KEK\"\n[[admin_breakglass]]\nenv = \"BG\"",
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

    /// What an operator gets without writing the keys: the connection's own
    /// schema, and no migration at boot. Migration is opt-in because the safe
    /// order is one `axond migrate apply` before any replica starts, not each
    /// replica racing to migrate the database the others are reading.
    #[test]
    fn the_control_plane_defaults_to_no_schema_and_no_boot_migration() {
        let control_plane = Config::from_toml_str(STATEFUL)
            .expect("the approved bootstrap set validates")
            .control_plane
            .expect("stateful mode requires a control plane");
        assert_eq!(control_plane.schema, None);
        assert_eq!(control_plane.backend, ControlPlaneBackend::Postgres);
        assert!(
            !control_plane.migrate,
            "a replica must not migrate a database on the way up unless asked"
        );
        assert_eq!(control_plane.connect_timeout_ms, 5_000);
        assert_eq!(control_plane.operation_timeout_ms, 30_000);
    }

    /// The identifier validator this borrows is written for table names, so it
    /// allows one qualifying dot. A search path takes one schema, so the config has
    /// to be narrower than the validator it reuses.
    #[test]
    fn a_qualified_control_plane_schema_is_rejected() {
        let error = Config::from_toml_str(
            "mode = \"stateful\"\n[control_plane]\ndsn_env = \"DSN\"\nschema = \"public.axond\"\n\
             [secret_store]\nkek_env = \"KEK\"\n[[admin_breakglass]]\nenv = \"BG\"",
        )
        .expect_err("`a.b` is not a schema name");
        assert!(
            error.to_string().contains("unqualified"),
            "the error has to say which grammar is wanted: {error}"
        );
    }

    #[test]
    fn the_control_plane_settings_are_read_as_written() {
        let control_plane = Config::from_toml_str(
            "mode = \"stateful\"\n[control_plane]\ndsn_env = \"DSN\"\nschema = \"axond_cp\"\n\
             migrate = true\nconnect_timeout_ms = 250\noperation_timeout_ms = 750\n\
             [secret_store]\nkek_env = \"KEK\"\n[[admin_breakglass]]\nenv = \"BG\"",
        )
        .expect("every control-plane key is settable")
        .control_plane
        .expect("stateful mode requires a control plane");
        assert_eq!(control_plane.schema.as_deref(), Some("axond_cp"));
        assert_eq!(control_plane.backend, ControlPlaneBackend::Postgres);
        assert!(control_plane.migrate);
        assert_eq!(control_plane.connect_timeout_ms, 250);
        assert_eq!(control_plane.operation_timeout_ms, 750);
    }

    #[test]
    fn object_storage_is_complete_without_a_postgres_dsn() {
        let config = Config::from_toml_str(BLOB_STATEFUL)
            .expect("a credential-free object-storage reference is complete");
        let control_plane = config.control_plane.expect("control plane");
        assert_eq!(control_plane.backend, ControlPlaneBackend::ObjectStorage);
        assert_eq!(control_plane.dsn_env, None);
        assert_eq!(control_plane.schema, None);
        assert!(!control_plane.migrate);
        assert_eq!(
            control_plane.environment_id.as_deref(),
            Some("prod-us-east")
        );
        assert_eq!(
            control_plane.authentication,
            Some(ObjectStorageAuthentication::WorkloadIdentity)
        );
        assert_eq!(
            control_plane.max_object_bytes,
            DEFAULT_OBJECT_STORAGE_BOUND_BYTES
        );
        assert!(config.secret_store.is_none());
    }

    #[test]
    fn checked_blob_control_plane_example_matches_the_minimal_contract() {
        let example = repository_file("ops/compose/axond.blob-contract.toml");
        let config = Config::from_toml_str(&example).expect("checked example must stay valid");
        assert_eq!(
            config.control_plane.expect("control plane").backend,
            ControlPlaneBackend::ObjectStorage
        );
        for forbidden in [
            "dsn_env",
            "schema =",
            "migrate =",
            "access_token",
            "account_key",
            "sas_query",
        ] {
            assert!(!example.contains(forbidden), "found `{forbidden}`");
        }
    }

    #[test]
    fn control_plane_backends_reject_fields_owned_by_the_other_contract() {
        for (expected, toml) in [
            (
                "Postgres-only fields",
                BLOB_STATEFUL.replace(
                    "authentication = \"workload-identity\"",
                    "authentication = \"workload-identity\"\ndsn_env = \"DO_NOT_READ\"",
                ),
            ),
            (
                "Postgres-only fields",
                BLOB_STATEFUL.replace(
                    "authentication = \"workload-identity\"",
                    "authentication = \"workload-identity\"\nmigrate = false",
                ),
            ),
            (
                "object-storage-only fields",
                STATEFUL.replace(
                    "dsn_env = \"GW_CONTROL_PLANE_DSN\"",
                    "dsn_env = \"GW_CONTROL_PLANE_DSN\"\nenvironment_id = \"prod\"",
                ),
            ),
        ] {
            let error = Config::from_toml_str(&toml).expect_err("mixed backends fail closed");
            assert!(error.to_string().contains(expected), "{error}");
            assert!(!error.to_string().contains("DO_NOT_READ"), "{error}");
        }

        let with_legacy_secret_store = format!(
            "{BLOB_STATEFUL}\n[secret_store]\ndsn_env = \"LEGACY_DSN\"\nkek_env = \"LEGACY_KEK\"\n"
        );
        let error = Config::from_toml_str(&with_legacy_secret_store)
            .expect_err("the target topology cannot silently add Postgres secrets");
        assert!(error.to_string().contains("legacy Postgres"), "{error}");
        assert!(!error.to_string().contains("LEGACY_DSN"), "{error}");
    }

    #[test]
    fn object_storage_requires_each_discriminating_setting() {
        for (line, expected) in [
            ("environment_id = \"prod-us-east\"\n", "environment_id"),
            (
                "container_url = \"https://axondstate.blob.core.windows.net/control-plane\"\n",
                "container_url",
            ),
            ("authentication = \"workload-identity\"\n", "authentication"),
        ] {
            let error = Config::from_toml_str(&BLOB_STATEFUL.replace(line, ""))
                .expect_err("a partial backend contract fails closed");
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn object_storage_requires_credential_free_https_container_urls() {
        for (replacement, expected) in [
            (
                "https://user:super-secret@axondstate.blob.core.windows.net/control-plane",
                "user information or credentials",
            ),
            (
                "https://axondstate.blob.core.windows.net/control-plane?sig=super-secret",
                "must not contain a query",
            ),
            (
                "https://axondstate.blob.core.windows.net/control-plane#super-secret",
                "must not contain a fragment",
            ),
            (
                "http://axondstate.blob.core.windows.net/control-plane",
                "must use HTTPS",
            ),
            (
                "https://axondstate.blob.core.windows.net/one/two",
                "exactly one unescaped container",
            ),
        ] {
            let toml = BLOB_STATEFUL.replace(
                "https://axondstate.blob.core.windows.net/control-plane",
                replacement,
            );
            let error = Config::from_toml_str(&toml).expect_err("unsafe URL must be refused");
            let rendered = error.to_string();
            assert!(rendered.contains(expected), "{rendered}");
            assert!(!rendered.contains("super-secret"), "{rendered}");
        }
    }

    #[test]
    fn loopback_http_requires_an_explicit_development_exception() {
        let loopback = BLOB_STATEFUL.replace(
            "https://axondstate.blob.core.windows.net/control-plane",
            "http://127.0.0.1:10000/devstoreaccount1",
        );
        let error = Config::from_toml_str(&loopback).expect_err("HTTP cannot default on");
        assert!(error.to_string().contains("allow_loopback_http = true"));

        let explicit = loopback.replace(
            "authentication = \"workload-identity\"",
            "authentication = \"workload-identity\"\nallow_loopback_http = true",
        );
        Config::from_toml_str(&explicit).expect("explicit loopback Azurite is valid");

        let remote = BLOB_STATEFUL
            .replace(
                "https://axondstate.blob.core.windows.net/control-plane",
                "http://blob.internal/control-plane",
            )
            .replace(
                "authentication = \"workload-identity\"",
                "authentication = \"workload-identity\"\nallow_loopback_http = true",
            );
        let error = Config::from_toml_str(&remote).expect_err("the flag cannot weaken remote TLS");
        assert!(error.to_string().contains("must use HTTPS"), "{error}");
    }

    #[test]
    fn object_storage_bounds_are_nonzero_bounded_and_coherent() {
        for (field, value, expected) in [
            ("max_object_bytes", 0, "must be at least 1"),
            ("max_read_bytes", 0, "must be at least 1"),
            ("max_write_bytes", 0, "must be at least 1"),
            (
                "max_object_bytes",
                MAX_OBJECT_STORAGE_BOUND_BYTES + 1,
                "safety limit",
            ),
            (
                "max_read_bytes",
                MAX_OBJECT_STORAGE_BOUND_BYTES + 1,
                "safety limit",
            ),
            (
                "max_write_bytes",
                MAX_OBJECT_STORAGE_BOUND_BYTES + 1,
                "safety limit",
            ),
        ] {
            let toml = BLOB_STATEFUL.replace(
                "authentication = \"workload-identity\"",
                &format!("authentication = \"workload-identity\"\n{field} = {value}"),
            );
            let error = Config::from_toml_str(&toml).expect_err("unsafe bound must be refused");
            assert!(error.to_string().contains(field), "{error}");
            assert!(error.to_string().contains(expected), "{error}");
        }

        let incoherent = BLOB_STATEFUL.replace(
            "authentication = \"workload-identity\"",
            "authentication = \"workload-identity\"\nmax_object_bytes = 1024\nmax_read_bytes = 1025",
        );
        let error = Config::from_toml_str(&incoherent).expect_err("read exceeds object cap");
        assert!(error.to_string().contains("must not exceed"), "{error}");
    }

    #[test]
    fn object_storage_rejects_invalid_key_segments_and_secret_fields() {
        for environment in [
            "",
            "Prod",
            "/prod",
            "prod/east",
            "prod east",
            " prod-us-east ",
        ] {
            let toml = BLOB_STATEFUL.replace("prod-us-east", environment);
            let error = Config::from_toml_str(&toml).expect_err("invalid key segment");
            assert!(error.to_string().contains("environment_id"), "{error}");
        }

        for field in ["access_token", "account_key", "sas_query"] {
            let toml = BLOB_STATEFUL.replace(
                "authentication = \"workload-identity\"",
                &format!(
                    "authentication = \"workload-identity\"\n{field} = \"super-secret-material\""
                ),
            );
            let error = Config::from_toml_str(&toml).expect_err("secret fields are inexpressible");
            let rendered = error.to_string();
            assert!(rendered.contains(field), "{rendered}");
            assert!(!rendered.contains("super-secret-material"), "{rendered}");
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
            (
                "[convergence] cache_key_env",
                "mode = \"stateful\"\n[convergence]\ncache_path = \"/tmp/lkg\"\ncache_key_env = \"AXOND_CONVERGENCE\"\n[control_plane]\ndsn_env = \"GW_DSN\"\n[secret_store]\nkek_env = \"GW_KEK\"\n[[admin_breakglass]]\nenv = \"GW_BG\"",
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

    #[test]
    fn a_partial_last_known_good_cache_is_rejected_before_boot() {
        for snippet in [
            "[convergence]\ncache_path = \"/tmp/lkg\"",
            "[convergence]\ncache_key_env = \"GW_LAST_KNOWN_GOOD_KEY\"",
        ] {
            let error = Config::from_toml_str(&format!("{STATEFUL}\n{snippet}"))
                .expect_err("a cache path and key reference must be configured together");
            assert!(
                error
                    .to_string()
                    .contains("requires both `cache_path` and `cache_key_env`"),
                "{error}"
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

    /// The shipped stateful example is the operator-facing copy of the approved
    /// bootstrap set; it must keep validating as the parser evolves. The
    /// Recreate deployment deliberately leaves the optional cache disabled until
    /// a durable StatefulSet/PVC mount exists.
    #[test]
    fn the_shipped_stateful_example_validates() {
        let config = Config::from_toml_str(&repository_file("axond.stateful.example.toml"))
            .expect("axond.stateful.example.toml must validate");
        assert_eq!(config.mode, Mode::Stateful);
        assert!(
            config.namespace.is_empty(),
            "the control plane owns tenants"
        );
        assert_eq!(
            config.convergence.cache_path.as_deref(),
            Some("/var/lib/axond/last-known-good.snapshot")
        );
        assert_eq!(
            config.convergence.cache_key_env.as_deref(),
            Some("GW_LAST_KNOWN_GOOD_KEY")
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

    /// Landing the durable tenancy schemas (#191) does not give a *file* tenants
    /// or projects: a stateless deployment's namespaces are still exactly the ids
    /// it wrote, and the body schemas are not a TOML surface.
    #[test]
    fn tenancy_schemas_do_not_change_what_a_file_can_declare() {
        let config = Config::from_toml_str(VALID).expect("the stateless example still parses");
        assert_eq!(config.mode, Mode::Stateless);
        assert_eq!(
            config
                .namespace
                .iter()
                .map(|namespace| namespace.id.as_str())
                .collect::<Vec<_>>(),
            ["platform"],
            "a namespace id is the id the file wrote, unqualified"
        );

        // Durable tenancy is published, never declared: a file naming a tenant
        // configures nothing, and in particular does not become a namespace.
        let with_tenant = format!("{VALID}\n[[tenant]]\nid = \"acme\"\ndisplay_name = \"Acme\"\n");
        let parsed =
            Config::from_toml_str(&with_tenant).expect("an unread section is not a boot failure");
        assert_eq!(
            parsed
                .namespace
                .iter()
                .map(|namespace| (namespace.id.as_str(), namespace.default))
                .collect::<Vec<_>>(),
            [("platform", true)]
        );
        assert_eq!(parsed.mode, config.mode);
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

    /// Retention needs its DSN *by name*: the connection string stays in the
    /// environment, and the config holds the name of the variable holding it.
    /// A deployment that already configured `[control_plane]` inherits its name
    /// rather than repeating it.
    #[test]
    fn postgres_retention_needs_a_dsn_reference_it_can_resolve() {
        let refusal = catalogue_refusal(&format!(
            "{VALID}\n[catalog]\nsource = \"models-dev\"\nstore = \"postgres\"\n"
        ));
        assert!(
            refusal.contains("dsn_env"),
            "the refusal must name the missing reference, said: {refusal}"
        );

        let config = catalogue_config(&format!(
            "{VALID}\n[catalog]\nsource = \"models-dev\"\nstore = \"postgres\"\ndsn_env = \
             \"AXOND_CATALOG_DSN\"\n"
        ));
        assert!(
            !format!("{:?}", config.catalog).contains("postgres://"),
            "the config must never hold a connection string"
        );

        catalogue_config(&format!(
            "{STATEFUL}\n[catalog]\nsource = \"models-dev\"\nstore = \"postgres\"\n"
        ));
    }

    /// A stateful deployment may not retain imported catalogues in memory it is
    /// about to lose. The in-memory store is a development affordance, and
    /// letting a stateful mode select it would substitute a process-local store
    /// for the durable contract without saying so — while `[catalog]` itself
    /// stays bootstrap-owned, like every other backend selection.
    #[test]
    fn a_stateful_deployment_may_not_retain_its_catalogue_in_memory() {
        let refusal = catalogue_refusal(&format!(
            "{STATEFUL}\n[catalog]\nsource = \"models-dev\"\nstore = \"in-memory\"\n"
        ));
        assert!(
            refusal.contains("postgres"),
            "the refusal must say where a stateful catalogue is retained, said: {refusal}"
        );

        catalogue_config(&format!(
            "{STATEFUL}\n[catalog]\nsource = \"models-dev\"\nstore = \"postgres\"\n"
        ));
    }
}
