//! Declarative configuration — the source of truth for the gateway.
//!
//! A TOML file owns all *structure* (providers, models, namespaces, quota,
//! sinks); the environment owns *secrets* (referenced by name, never inlined)
//! and may override scalars for containerized deploys. This mirrors the design
//! note in the assessment (§5, delta B4): config is a public API, so it is
//! validated as a whole at boot (delta B2) rather than coping with invalid
//! entries at request time.

use std::collections::HashMap;
use std::net::SocketAddr;

use gateway_core::ModelPrice;
use serde::Deserialize;

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
    /// Inbound gateway keys. Each binds a secret (resolved from `env`) to a
    /// namespace. When empty, the gateway is unauthenticated and every request
    /// uses the default namespace — intended for local dev only.
    #[serde(default)]
    pub gateway_key: Vec<GatewayKey>,
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

fn default_weight() -> u32 {
    1
}

fn default_credential_failure_threshold() -> u32 {
    2
}

fn default_credential_cooldown_seconds() -> u64 {
    30
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
