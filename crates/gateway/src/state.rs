//! Shared application state.
//!
//! State splits in two. Process-level resources — the HTTP client pool, the
//! connected usage sinks, the budget store — are built once at boot and live for
//! the process. Everything *derived from the config file* lives in a
//! [`ConfigSnapshot`] behind an [`ArcSwap`], so a reload publishes a whole new
//! snapshot in one atomic store (ADR 0011).
//!
//! Readers take the snapshot once, at the top of a request, and hold that `Arc`
//! for the request's lifetime. A request therefore resolves its alias, its
//! credential, and its circuit against one consistent config, even if a reload
//! lands mid-flight.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use gateway_core::{
    AnthropicAdapter, CircuitBreaker, OpenAiCompatibleAdapter, OpenAiFlavor, ProviderAdapter,
};
use gateway_transport::{HttpDispatcher, build_client};
use secrecy::{ExposeSecret, SecretString};

use crate::admission::AdmissionControl;

use crate::backends::catalog_runtime::CatalogStatus;
use crate::budget::BudgetStore;
#[cfg(test)]
use crate::config::StorageBackend;
use crate::config::{Config, ProviderKind};
use crate::credentials::{CredentialError, Credentials};
use crate::key_material::{self, KeyMaterialError};
use crate::principals::{ConfigPrincipals, GatewayKeyEntry, Presented, PrincipalAuthority};
use crate::settlement::Settlements;
use crate::shutdown::Lifecycle;
use crate::usage::UsageDelivery;
#[cfg(test)]
use crate::usage::UsageFanout;

pub use crate::principals::InboundKey;

#[derive(Clone)]
pub struct AppState(pub Arc<Inner>);

pub struct Inner {
    pub dispatcher: HttpDispatcher,
    /// Process-level grace for byte-faithful provider bytes that follow a
    /// semantic terminal event. Read at boot with the rest of `[transport]`.
    pub stream_terminal_grace: Duration,
    /// How a terminated request's usage leaves the process. Telemetry-grade by
    /// default; a durable append when a journal is configured.
    pub usage: Arc<UsageDelivery>,
    pub budget: Box<dyn BudgetStore>,
    /// Process-level ceilings. Like the HTTP client's bounds these own state
    /// built at boot, so a reloaded `[admission]` section applies on restart.
    pub admission: AdmissionControl,
    /// Bounded capacity for the background accounting every admitted request
    /// leaves behind. Process-level like `admission`, whose section sizes it.
    pub settlements: Settlements,
    /// Drain state and the in-flight count. Process-level, like the sinks.
    pub lifecycle: Arc<Lifecycle>,
    /// What the background catalogue import last reported, when this deployment
    /// imports one at all. A read of a mutex over a bounded report: the request
    /// path never reaches the source or the store, and holding this handle is
    /// what makes that structural rather than a rule (ADR 0043).
    pub catalogue: Option<Arc<CatalogStatus>>,
    /// Durable namespace (and later budget) store. Required (ADR 0063).
    pub store: std::sync::Arc<dyn crate::store::Store>,
    config: ArcSwap<ConfigSnapshot>,
}

/// The config and everything resolved from it: the credential graph, the
/// inbound-key table, and the per-target circuits. Immutable once published —
/// a reload builds a replacement rather than mutating this one.
pub struct ConfigSnapshot {
    pub config: Config,
    pub credentials: Credentials,
    /// Per-target circuit breaker, keyed by the target's qualified model
    /// (`provider/model`). In-memory and per-replica, consistent with running
    /// stateless by default (ADR 0002); distinct from the per-credential health
    /// that lives on `Credentials` (ADR 0008).
    pub target_circuits: CircuitBreaker,
    principals: ConfigPrincipals,
    /// How many times the config has been replaced: `0` is the boot config, and
    /// each applied reload increments it. Published as a metric so an operator
    /// can tell which generation a replica is serving.
    pub generation: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error(transparent)]
    Credentials(#[from] CredentialError),
    #[cfg(any(test, fuzzing))]
    #[error("store: {0}")]
    Store(String),
    #[error(
        "gateway_key for namespace `{namespace}` references env var `{env}`, which is unset or empty"
    )]
    MissingGatewayKey { namespace: String, env: String },
    #[error("gateway_key for namespace `{namespace}` file `{path}` failed ({kind}): {error}")]
    GatewayKeyFile {
        namespace: String,
        path: String,
        kind: std::io::ErrorKind,
        error: String,
    },
    #[error("gateway_key for namespace `{namespace}` file `{path}` is empty")]
    EmptyGatewayKeyFile { namespace: String, path: String },
    #[error("gateway_key for namespace `{namespace}` file `{path}` is not valid UTF-8")]
    InvalidGatewayKeyFileUtf8 { namespace: String, path: String },
    #[error("gateway_key for namespace `{namespace}` must declare exactly one non-empty source")]
    InvalidGatewayKeySource { namespace: String },
    #[error(
        "gateway_key sources `{env}` (namespace `{namespace}`) and `{other_env}` (namespace `{other_namespace}`) hold the same secret, so the caller's namespace would be ambiguous"
    )]
    DuplicateGatewayKey {
        env: String,
        namespace: String,
        other_env: String,
        other_namespace: String,
    },
    #[error(
        "no inbound gateway key resolved: inbound authentication fails closed and there is no keyless mode"
    )]
    NoInboundKeys,
}

impl ConfigSnapshot {
    /// Resolve a validated config against an environment snapshot. Fails when a
    /// declared credential's or gateway key's env var is missing or empty, or
    /// when two gateway keys resolve to the same secret — the credential graph
    /// and the inbound-key table are both resolved before the snapshot is
    /// published, never at request time.
    pub fn build(
        config: Config,
        env: &HashMap<String, String>,
        generation: u64,
    ) -> Result<Self, SnapshotError> {
        // The one place both kinds of provider credential become one pool: env
        // references from the boot environment, projected ones from the material
        // this candidate resolved. Neither reaches a store from here.
        let credentials = Credentials::resolve(&config, env)?;
        let target_circuits = CircuitBreaker::new(
            config.failover.failure_threshold,
            Duration::from_secs(config.failover.cooldown_seconds),
        );
        let mut inbound_keys: Vec<GatewayKeyEntry> = Vec::new();
        for k in &config.gateway_key {
            let source = k
                .source()
                .ok_or_else(|| SnapshotError::InvalidGatewayKeySource {
                    namespace: k.namespace.clone(),
                })?;
            let label = k
                .source_label()
                .ok_or_else(|| SnapshotError::InvalidGatewayKeySource {
                    namespace: k.namespace.clone(),
                })?;
            let secret = key_material::resolve(source, env).map_err(|error| match error {
                KeyMaterialError::MissingEnv { name } => SnapshotError::MissingGatewayKey {
                    namespace: k.namespace.clone(),
                    env: name,
                },
                KeyMaterialError::FileRead { path, kind, error } => SnapshotError::GatewayKeyFile {
                    namespace: k.namespace.clone(),
                    path,
                    kind,
                    error,
                },
                KeyMaterialError::EmptyFile { path } => SnapshotError::EmptyGatewayKeyFile {
                    namespace: k.namespace.clone(),
                    path,
                },
                KeyMaterialError::InvalidUtf8 { path } => {
                    SnapshotError::InvalidGatewayKeyFileUtf8 {
                        namespace: k.namespace.clone(),
                        path,
                    }
                }
            })?;
            // Two keys resolving to one secret is ambiguous authority — one
            // namespace would silently win — so reject it. Compared here on the
            // operator-supplied values at boot, never at request time.
            if let Some(other) = inbound_keys.iter().find(|e| {
                crate::principals::constant_time_eq(
                    e.secret.expose_secret().as_bytes(),
                    secret.as_bytes(),
                )
            }) {
                return Err(SnapshotError::DuplicateGatewayKey {
                    env: label.to_owned(),
                    namespace: k.namespace.clone(),
                    other_env: other.caller.subject.clone(),
                    other_namespace: other.caller.namespace.clone(),
                });
            }
            inbound_keys.push(GatewayKeyEntry {
                secret: SecretString::from(secret.clone()),
                caller: InboundKey {
                    namespace: k.namespace.clone(),
                    subject: label.to_owned(),
                    authority: PrincipalAuthority::StaticKey,
                    signer_kid: None,
                    scope: None,
                    max_request_microdollars: None,
                    namespace_grant: Some(crate::namespace::NamespaceGrant::all()),
                    attrs: None,
                },
            });
        }
        // Inbound authentication fails closed: there is no keyless deployment
        // that serves inference (ADR 0013).
        if inbound_keys.is_empty() {
            return Err(SnapshotError::NoInboundKeys);
        }
        let principals = ConfigPrincipals::new(inbound_keys.into());
        Ok(Self {
            config,
            credentials,
            target_circuits,
            principals,
            generation,
        })
    }

    pub fn resolve_principal(&self, presented: &Presented<'_>) -> Option<InboundKey> {
        self.principals.resolve_static(presented.credential)
    }

    /// How many inbound gateway keys are enforced. For the boot log and reload
    /// metrics — the count is safe to surface, the secrets are not.
    pub fn inbound_key_count(&self) -> usize {
        self.principals.count()
    }
}

impl AppState {
    /// A test state over a telemetry-grade usage fanout and a SQLite store
    /// opened from the config.
    #[cfg(test)]
    pub fn new(
        config: Config,
        env: &HashMap<String, String>,
        usage: UsageFanout,
        budget: Box<dyn BudgetStore>,
    ) -> Result<Self, SnapshotError> {
        Self::with_resources(
            config,
            env,
            Arc::new(UsageDelivery::telemetry(usage)),
            budget,
            None,
        )
    }

    /// A test state with every process-level resource supplied except the
    /// store, which is opened synchronously from the config's SQLite path.
    #[cfg(test)]
    pub fn with_resources(
        config: Config,
        env: &HashMap<String, String>,
        usage: Arc<UsageDelivery>,
        budget: Box<dyn BudgetStore>,
        catalogue: Option<Arc<CatalogStatus>>,
    ) -> Result<Self, SnapshotError> {
        let store = open_store_sync(&config)?;
        Self::serving(config, env, usage, budget, catalogue, store)
    }

    /// The serving constructor. Every process-level resource is already
    /// connected, so a deployment that cannot reach a datastore it asked for
    /// has already failed before this is called.
    pub fn serving(
        config: Config,
        env: &HashMap<String, String>,
        usage: Arc<UsageDelivery>,
        budget: Box<dyn BudgetStore>,
        catalogue: Option<Arc<CatalogStatus>>,
        store: Arc<dyn crate::store::Store>,
    ) -> Result<Self, SnapshotError> {
        // The transport bounds configure the shared client, so they are read
        // once here.
        let limits = config.transport.limits();
        let stream_terminal_grace =
            Duration::from_millis(config.transport.stream_terminal_grace_ms);
        let admission = AdmissionControl::from_config(&config.admission);
        let settlements = Settlements::from_config(&config.admission);
        let snapshot = ConfigSnapshot::build(config, env, 0)?;
        let index_settings = snapshot
            .config
            .storage
            .as_ref()
            .map(|storage| storage.usage_index.settings())
            .unwrap_or_default();
        usage.attach_store(Arc::clone(&store), index_settings);
        Ok(AppState(Arc::new(Inner {
            dispatcher: HttpDispatcher::with_limits(
                build_client(&limits).expect("the upstream HTTP client builds"),
                limits,
            ),
            stream_terminal_grace,
            usage,
            budget,
            admission,
            settlements,
            lifecycle: Arc::new(Lifecycle::new()),
            catalogue,
            store,
            config: ArcSwap::from_pointee(snapshot),
        })))
    }

    pub fn store(&self) -> Option<&std::sync::Arc<dyn crate::store::Store>> {
        Some(&self.0.store)
    }

    /// The process lifecycle: what readiness reports and what admission checks.
    pub fn lifecycle(&self) -> &Arc<Lifecycle> {
        &self.0.lifecycle
    }

    /// Compiled models.dev rates for this offering from the snapshot the
    /// importer last admitted, if any. Copied at admission so a later import
    /// cannot reprice an in-flight request.
    pub fn catalog_price_for(
        &self,
        provider: &str,
        published_model_id: &str,
    ) -> Option<gateway_core::ModelPrice> {
        self.0
            .catalogue
            .as_ref()
            .and_then(|catalogue| catalogue.price_for(provider, published_model_id))
    }

    /// The config snapshot a request runs against. Taken once per request and
    /// held for its duration, so a concurrent reload cannot half-apply.
    pub fn config(&self) -> Arc<ConfigSnapshot> {
        self.0.config.load_full()
    }
}

/// Build the zero-size adapter for a provider kind. Adapters carry no state,
/// so this is cheap to call per request.
pub fn adapter_for(kind: ProviderKind) -> Box<dyn ProviderAdapter> {
    match kind {
        ProviderKind::Openai => Box::new(OpenAiCompatibleAdapter::openai()),
        ProviderKind::OpenaiCompatible => {
            Box::new(OpenAiCompatibleAdapter::new(OpenAiFlavor::Compatible))
        }
        ProviderKind::Anthropic => Box::new(AnthropicAdapter::new()),
    }
}

#[cfg(test)]
fn open_store_sync(config: &Config) -> Result<Arc<dyn crate::store::Store>, SnapshotError> {
    let storage = config
        .storage
        .as_ref()
        .ok_or_else(|| SnapshotError::Store("`[storage]` is required".into()))?;
    match storage.backend {
        StorageBackend::Sqlite => {
            let path = storage.path.as_deref().unwrap_or(":memory:");
            let store = crate::store::SqliteStore::open(path)
                .map_err(|error| SnapshotError::Store(error.to_string()))?;
            store
                .seed_config_namespaces_sync(&config.namespace)
                .map_err(|error| SnapshotError::Store(error.to_string()))?;
            Ok(Arc::new(store))
        }
        StorageBackend::Postgres => Err(SnapshotError::Store(
            "postgres must be opened asynchronously in `serve`; tests should use sqlite".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_file(contents: &[u8]) -> String {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "axond-state-key-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&path, contents).unwrap();
        path.to_str().unwrap().to_owned()
    }

    fn config_with(gateway_keys: &str) -> Config {
        Config::from_toml_str(&format!(
            r#"
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

{gateway_keys}

[[price]]
provider = "openai"
model = "gpt-4o"
input_microdollars_per_million = 1
output_microdollars_per_million = 1
"#
        ))
        .expect("valid config")
    }

    const PLATFORM_KEY: &str = r#"
[[gateway_key]]
env = "AXOND_KEY"
namespace = "platform"
"#;

    /// A declared key whose env var is unset or empty is a boot failure, not a
    /// silently dropped entry that would widen or empty the key table.
    #[test]
    fn a_declared_gateway_key_without_its_env_var_refuses_to_resolve() {
        for env in [
            HashMap::new(),
            HashMap::from([("AXOND_KEY".to_owned(), String::new())]),
        ] {
            let Err(err) = ConfigSnapshot::build(config_with(PLATFORM_KEY), &env, 0) else {
                panic!("the key cannot be resolved");
            };
            assert!(
                matches!(
                    err,
                    SnapshotError::MissingGatewayKey { ref env, ref namespace }
                        if env == "AXOND_KEY" && namespace == "platform"
                ),
                "{err}"
            );
            // The message names the reference, never a value.
            assert!(err.to_string().contains("AXOND_KEY"), "{err}");
        }
    }

    /// Two keys holding one secret cannot both be honoured: the table is keyed
    /// by the secret, so one namespace would silently win. ADR 0063 refuses the
    /// second `[[gateway_key]]` at boot rather than at snapshot build.
    #[test]
    fn two_gateway_keys_sharing_one_secret_refuse_to_resolve() {
        let error = Config::from_toml_str(
            r#"
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

[[gateway_key]]
env = "AXOND_OTHER_KEY"
namespace = "platform"

[[price]]
provider = "openai"
model = "gpt-4o"
input_microdollars_per_million = 1
output_microdollars_per_million = 1
"#,
        )
        .expect_err("plural [[gateway_key]] is withdrawn");
        let message = error.to_string();
        assert!(
            message.contains("[[gateway_key]]") && message.contains("ADR 0063"),
            "{message}"
        );
        assert!(!message.contains("shared"), "{message}");
    }

    #[test]
    fn file_backed_gateway_key_errors_redact_material() {
        let failed = temp_file(b"");
        let config = config_with(&format!(
            "[[gateway_key]]\nfile = \"{failed}\"\nnamespace = \"platform\"\n"
        ));
        let Err(error) = ConfigSnapshot::build(config, &HashMap::new(), 0) else {
            panic!("empty file-backed key must be rejected");
        };
        let message = format!("{error:?} {error}");
        assert!(message.contains(&failed), "{message}");
        assert!(!message.contains("file-shared-secret"), "{message}");
        std::fs::remove_file(failed).unwrap();
    }

    /// The secret is held as `SecretString`, so debugging or logging an entry
    /// renders the redaction placeholder, never the key material.
    #[test]
    fn a_resolved_key_entry_never_renders_its_secret() {
        let env = HashMap::from([("AXOND_KEY".to_owned(), "inbound-secret".to_owned())]);
        let snapshot = ConfigSnapshot::build(config_with(PLATFORM_KEY), &env, 0).expect("resolves");
        let rendered = snapshot.principals.first_secret_debug();
        assert!(!rendered.contains("inbound-secret"), "{rendered}");
    }

    #[test]
    fn a_resolved_key_is_bound_to_its_namespace_and_env_var() {
        let env = HashMap::from([("AXOND_KEY".to_owned(), "inbound-secret".to_owned())]);
        let snapshot = ConfigSnapshot::build(config_with(PLATFORM_KEY), &env, 0).expect("resolves");
        let key = snapshot
            .resolve_principal(&Presented {
                credential: "inbound-secret",
            })
            .expect("the presented secret resolves its caller");
        assert_eq!(key.namespace, "platform");
        assert_eq!(key.subject, "AXOND_KEY");
        assert_eq!(snapshot.inbound_key_count(), 1);
        assert!(
            snapshot
                .resolve_principal(&Presented {
                    credential: "wrong-secret",
                })
                .is_none()
        );
    }

    #[test]
    fn a_static_gateway_key_resolves_from_a_file_and_uses_its_path_as_subject() {
        let path = temp_file(b"static-file-secret");
        let config = config_with(&format!(
            "[[gateway_key]]\nfile = \"{path}\"\nnamespace = \"platform\"\n"
        ));
        let snapshot = ConfigSnapshot::build(config, &HashMap::new(), 0).expect("resolves");
        let principal = snapshot
            .resolve_principal(&Presented {
                credential: "static-file-secret",
            })
            .unwrap();
        assert_eq!(principal.subject, path);
        std::fs::remove_file(path).unwrap();
    }
}
