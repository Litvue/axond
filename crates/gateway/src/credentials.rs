//! Namespaced provider-credential resolution.
//!
//! Credentials are read from the process environment once at startup (they are
//! fixed for the process lifetime) into a `(namespace, provider) → pool` map. A
//! future watched-file layer can swap the `Arc` for hot reload; the lookup stays
//! a pure function of the snapshot so it is testable without mutating the global
//! environment (assessment §5.1).
//!
//! A pool holds one *or more* credentials for the same pair (ADR 0006). A
//! request gets an ordered plan of attempts: the selection strategy decides who
//! goes first, per-credential health parks a credential that keeps returning
//! rate-limit/quota errors, and the caller walks the plan on such a failure.
//! Credential health is deliberately scoped *below* the per-target circuit: a
//! bad key parks that key, never the provider target.
//!
//! Invariant: credentials are **write-only**. Nothing here ever returns a key to
//! a caller — only presence, and the credential's label, are observable.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use secrecy::SecretString;

use crate::config::{Config, SelectionStrategy};

/// Which key served a request, for usage attribution (delta A3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialSource {
    Platform,
    Byok,
}

/// One credential handed to a single upstream attempt. The `id` is a label, so
/// it can be logged and attributed; the secret never is.
#[derive(Clone)]
pub struct CredentialLease {
    pub id: String,
    pub secret: SecretString,
    health_key: String,
}

/// The ordered attempts for one request against one `(namespace, provider)`.
pub struct CredentialPlan {
    pub source: CredentialSource,
    pub attempts: Vec<CredentialLease>,
    pub parked: Vec<CredentialSkip>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialSkip {
    pub id: String,
    pub reason: &'static str,
}

#[derive(Debug, thiserror::Error)]
pub enum CredentialError {
    #[error(
        "credential `{id}` for namespace `{namespace}` provider `{provider}` references env var `{env}`, which is unset or empty"
    )]
    MissingEnv {
        namespace: String,
        provider: String,
        id: String,
        env: String,
    },
}

/// Per-credential circuit state, reported for observability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialState {
    Healthy,
    Parked,
}

/// What one request may do with a credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Eligibility {
    Healthy,
    /// Parked, and this request holds the single half-open probe.
    Probe,
    Parked,
}

struct PoolEntry {
    id: String,
    secret: SecretString,
    weight: u32,
    health_key: String,
}

struct Pool {
    entries: Vec<PoolEntry>,
    cursor: AtomicUsize,
    total_weight: u64,
}

impl Pool {
    fn new(entries: Vec<PoolEntry>) -> Self {
        let total_weight: u64 = entries.iter().map(|e| u64::from(e.weight)).sum();
        Self {
            entries,
            cursor: AtomicUsize::new(0),
            total_weight: total_weight.max(1),
        }
    }

    /// Indices of the pool's credentials, best-first for this request. Both
    /// strategies rotate a cursor so consecutive requests spread across the
    /// pool; `weighted` advances through a cumulative-weight ladder so a
    /// credential with weight `n` starts `n` out of every `total_weight`
    /// requests. The remaining credentials follow in rotation order, which is
    /// what makes a skip-on-429 walk deterministic.
    fn order(&self, strategy: SelectionStrategy) -> Vec<usize> {
        let n = self.entries.len();
        if n == 0 {
            return Vec::new();
        }
        let tick = self.cursor.fetch_add(1, Ordering::Relaxed) as u64;
        let start = match strategy {
            SelectionStrategy::RoundRobin => (tick % n as u64) as usize,
            SelectionStrategy::Weighted => {
                let mut offset = tick % self.total_weight;
                let mut chosen = n - 1;
                for (i, entry) in self.entries.iter().enumerate() {
                    let weight = u64::from(entry.weight);
                    if offset < weight {
                        chosen = i;
                        break;
                    }
                    offset -= weight;
                }
                chosen
            }
        };
        (0..n).map(|i| (start + i) % n).collect()
    }
}

/// Per-credential circuit breaker. Distinct from `gateway-core`'s per-target
/// breaker: a credential that is rate-limited or out of quota is parked on its
/// own, and recovers through a single half-open probe once the cooldown elapses.
struct CredentialHealth {
    threshold: u32,
    cooldown: Duration,
    circuits: Mutex<HashMap<String, Circuit>>,
}

#[derive(Debug, Clone, Copy, Default)]
struct Circuit {
    failures: u32,
    parked_at: Option<Instant>,
}

impl CredentialHealth {
    fn new(threshold: u32, cooldown: Duration) -> Self {
        Self {
            threshold: threshold.max(1),
            cooldown,
            circuits: Mutex::new(HashMap::new()),
        }
    }

    /// Classify a credential for one request, **taking** the half-open probe when
    /// this is the request that gets it. Handing out a probe re-arms the cooldown
    /// under the same lock, so exactly one request per cooldown window pays a
    /// round-trip to a credential that is still known-bad.
    fn classify_at(&self, key: &str, now: Instant) -> Eligibility {
        let mut circuits = self.lock();
        let Some(circuit) = circuits.get_mut(key) else {
            return Eligibility::Healthy;
        };
        match circuit.parked_at {
            None => Eligibility::Healthy,
            Some(parked_at) if now.saturating_duration_since(parked_at) >= self.cooldown => {
                circuit.parked_at = Some(now);
                Eligibility::Probe
            }
            Some(_) => Eligibility::Parked,
        }
    }

    fn record_success(&self, key: &str) {
        self.lock().remove(key);
    }

    fn record_failure_at(&self, key: &str, now: Instant) {
        let mut circuits = self.lock();
        let circuit = circuits.entry(key.to_owned()).or_default();
        circuit.failures = circuit.failures.saturating_add(1);
        if circuit.failures >= self.threshold {
            circuit.parked_at = Some(now);
        }
    }

    fn snapshot_at(&self, now: Instant) -> Vec<(String, CredentialState)> {
        self.lock()
            .iter()
            .map(|(key, circuit)| {
                let parked = circuit.parked_at.is_some_and(|parked_at| {
                    now.saturating_duration_since(parked_at) < self.cooldown
                });
                (
                    key.clone(),
                    if parked {
                        CredentialState::Parked
                    } else {
                        CredentialState::Healthy
                    },
                )
            })
            .collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Circuit>> {
        self.circuits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub struct Credentials {
    /// (namespace, provider) → credential pool
    pools: HashMap<(String, String), Pool>,
    platform_ns: String,
    strategy: SelectionStrategy,
    health: CredentialHealth,
}

impl Credentials {
    /// Build the snapshot from config + a captured environment map. A declared
    /// credential whose env var is unset or empty is a boot failure, not a
    /// request-time surprise (fail at boot, delta B2).
    pub fn from_env(
        config: &Config,
        env: &HashMap<String, String>,
    ) -> Result<Self, CredentialError> {
        let mut pools: HashMap<(String, String), Vec<PoolEntry>> = HashMap::new();
        for c in &config.credential {
            let secret = env.get(&c.env).filter(|v| !v.is_empty()).ok_or_else(|| {
                CredentialError::MissingEnv {
                    namespace: c.namespace.clone(),
                    provider: c.provider.clone(),
                    id: c.label().to_string(),
                    env: c.env.clone(),
                }
            })?;
            pools
                .entry((c.namespace.clone(), c.provider.clone()))
                .or_default()
                .push(PoolEntry {
                    id: c.label().to_string(),
                    secret: SecretString::from(secret.clone()),
                    weight: c.weight,
                    health_key: health_key(&c.namespace, &c.provider, c.label()),
                });
        }
        Ok(Self {
            pools: pools
                .into_iter()
                .map(|(key, entries)| (key, Pool::new(entries)))
                .collect(),
            platform_ns: config.default_namespace().to_string(),
            strategy: config.credential_pool.strategy,
            health: CredentialHealth::new(
                config.credential_pool.failure_threshold,
                Duration::from_secs(config.credential_pool.cooldown_seconds),
            ),
        })
    }

    /// Plan the credential attempts for `(namespace, provider)`, applying
    /// platform fallback only when the namespace explicitly allows it. The whole
    /// plan comes from a single namespace's pool: BYOK isolation is decided
    /// before selection, so a pool walk can never cross the boundary.
    pub fn plan(&self, config: &Config, namespace: &str, provider: &str) -> Option<CredentialPlan> {
        self.plan_at(config, namespace, provider, Instant::now())
    }

    fn plan_at(
        &self,
        config: &Config,
        namespace: &str,
        provider: &str,
        now: Instant,
    ) -> Option<CredentialPlan> {
        let own = self
            .pools
            .get(&(namespace.to_string(), provider.to_string()));
        let (pool, source) = match own {
            Some(pool) => {
                let source = if namespace == self.platform_ns {
                    CredentialSource::Platform
                } else {
                    CredentialSource::Byok
                };
                (pool, source)
            }
            None => {
                let allow_fallback = config
                    .namespace(namespace)
                    .is_some_and(|n| n.allow_platform_fallback);
                if !allow_fallback || namespace == self.platform_ns {
                    return None;
                }
                let pool = self
                    .pools
                    .get(&(self.platform_ns.clone(), provider.to_string()))?;
                (pool, CredentialSource::Platform)
            }
        };

        let order = pool.order(self.strategy);
        let entries = order.iter().map(|&i| &pool.entries[i]);
        // A parked credential whose cooldown elapsed leads the plan as a
        // half-open probe, and only one request per cooldown window gets it. The
        // probe is first so it is actually attempted; if it fails, the walk
        // continues into the healthy credentials and the request still succeeds.
        let mut attempts: Vec<CredentialLease> = Vec::new();
        let mut parked: Vec<&PoolEntry> = Vec::new();
        for entry in entries {
            match self.health.classify_at(&entry.health_key, now) {
                Eligibility::Healthy => attempts.push(lease(entry)),
                Eligibility::Probe => attempts.insert(0, lease(entry)),
                Eligibility::Parked => parked.push(entry),
            }
        }
        // Every credential is parked and none is due a probe. Health is
        // advisory, so the request still gets the rotation's first choice rather
        // than being failed on stale bookkeeping.
        let forced = if attempts.is_empty() {
            attempts = parked
                .first()
                .map(|entry| vec![lease(entry)])
                .into_iter()
                .flatten()
                .collect();
            attempts.first().map(|lease| lease.id.clone())
        } else {
            None
        };
        if attempts.is_empty() {
            return None;
        }
        let parked = parked
            .into_iter()
            .filter(|entry| forced.as_deref() != Some(entry.id.as_str()))
            .map(|entry| CredentialSkip {
                id: entry.id.clone(),
                reason: "parked",
            })
            .collect();
        Some(CredentialPlan {
            source,
            attempts,
            parked,
        })
    }

    /// A credential served a request: clear its failure history.
    pub fn record_success(&self, lease: &CredentialLease) {
        self.health.record_success(&lease.health_key);
    }

    /// A credential-scoped failure (rate limit / quota). Enough of these park
    /// this credential, never the provider target.
    pub fn record_failure(&self, lease: &CredentialLease) {
        self.health
            .record_failure_at(&lease.health_key, Instant::now());
    }

    /// Whether `(namespace, provider)` resolves to any credential — its own
    /// pool, or the platform's under `allow_platform_fallback`, mirroring the
    /// same branching [`plan`](Self::plan) walks. Presence only, never the value
    /// (write-only invariant). Backs the per-namespace `/v1/models` scoping and
    /// the "which providers are live here" read surface.
    ///
    /// This is a pure query: unlike `plan`, it must not advance the pool's
    /// rotation cursor or consume a parked credential's half-open probe, so a
    /// read path (a catalogue listing) cannot perturb which credential real
    /// dispatch picks or starve a rate-limited key of its recovery attempt. A
    /// pool always holds at least one entry, so pool presence is exactly what
    /// `plan(...).is_some()` would report.
    pub fn is_present(&self, config: &Config, namespace: &str, provider: &str) -> bool {
        if self
            .pools
            .contains_key(&(namespace.to_string(), provider.to_string()))
        {
            return true;
        }
        namespace != self.platform_ns
            && config
                .namespace(namespace)
                .is_some_and(|n| n.allow_platform_fallback)
            && self
                .pools
                .contains_key(&(self.platform_ns.clone(), provider.to_string()))
    }

    /// Labels + circuit state per credential, for the status endpoint and logs.
    #[allow(dead_code)] // backs the credential-status endpoint (follow-up)
    pub fn health_snapshot(&self) -> Vec<(String, CredentialState)> {
        self.health.snapshot_at(Instant::now())
    }
}

fn lease(entry: &PoolEntry) -> CredentialLease {
    CredentialLease {
        id: entry.id.clone(),
        secret: entry.secret.clone(),
        health_key: entry.health_key.clone(),
    }
}

fn health_key(namespace: &str, provider: &str, id: &str) -> String {
    format!("{namespace}/{provider}/{id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(extra: &str) -> Config {
        let toml = format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[namespace]]
id = "acme"

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

[[gateway_key]]
env = "AXOND_INBOUND_KEY"
namespace = "platform"
{extra}
"#
        );
        Config::from_toml_str(&toml).expect("valid config")
    }

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    const TWO_PLATFORM_KEYS: &str = r#"
[[credential]]
namespace = "platform"
provider = "openai"
env = "K1"
id = "openai-a"

[[credential]]
namespace = "platform"
provider = "openai"
env = "K2"
id = "openai-b"
"#;

    fn two_key_credentials(cfg: &Config) -> Credentials {
        Credentials::from_env(cfg, &env(&[("K1", "sk-a"), ("K2", "sk-b")])).expect("credentials")
    }

    #[test]
    fn round_robin_spreads_requests_across_the_pool() {
        let cfg = config(TWO_PLATFORM_KEYS);
        let creds = two_key_credentials(&cfg);
        let first: Vec<String> = (0..4)
            .map(|_| {
                creds
                    .plan(&cfg, "platform", "openai")
                    .expect("plan")
                    .attempts[0]
                    .id
                    .clone()
            })
            .collect();
        assert_eq!(first, ["openai-a", "openai-b", "openai-a", "openai-b"]);
    }

    #[test]
    fn is_present_is_a_pure_query_that_does_not_perturb_rotation_or_health() {
        let cfg = config(TWO_PLATFORM_KEYS);
        let creds = two_key_credentials(&cfg);

        // A presence check must not advance the round-robin cursor, so a read
        // path (a `/v1/models` listing) cannot bias which credential the next
        // real dispatch picks: after any number of listings the first attempt
        // still starts at the pool head.
        for _ in 0..5 {
            assert!(creds.is_present(&cfg, "platform", "openai"));
        }
        assert_eq!(
            creds
                .plan(&cfg, "platform", "openai")
                .expect("plan")
                .attempts[0]
                .id,
            "openai-a",
        );

        // Park the head credential and let its cooldown elapse, then poll
        // presence repeatedly: the single half-open probe must survive so a
        // real request still re-tests the key. If `is_present` consumed it, the
        // probe would be re-armed and the plan below would skip the parked key.
        let now = Instant::now();
        let (head_id, head_key) = {
            let plan = creds
                .plan_at(&cfg, "platform", "openai", now)
                .expect("plan");
            let head = &plan.attempts[0];
            (head.id.clone(), head.health_key.clone())
        };
        creds.health.record_failure_at(&head_key, now);
        creds.health.record_failure_at(&head_key, now);
        let after_cooldown = now + Duration::from_secs(31);
        for _ in 0..5 {
            assert!(creds.is_present(&cfg, "platform", "openai"));
        }
        let plan = creds
            .plan_at(&cfg, "platform", "openai", after_cooldown)
            .expect("plan");
        assert_eq!(
            plan.attempts[0].id, head_id,
            "the recovery probe must not be consumed by presence checks",
        );
    }

    #[test]
    fn plan_lists_every_credential_so_a_rate_limit_falls_to_the_next() {
        let cfg = config(TWO_PLATFORM_KEYS);
        let creds = two_key_credentials(&cfg);
        let plan = creds.plan(&cfg, "platform", "openai").expect("plan");
        let ids: Vec<&str> = plan.attempts.iter().map(|a| a.id.as_str()).collect();
        assert_eq!(ids, ["openai-a", "openai-b"]);
        assert_eq!(plan.source, CredentialSource::Platform);
    }

    #[test]
    fn weighted_selection_follows_configured_shares() {
        let cfg = config(
            r#"
[credential_pool]
strategy = "weighted"

[[credential]]
namespace = "platform"
provider = "openai"
env = "K1"
id = "openai-a"
weight = 3

[[credential]]
namespace = "platform"
provider = "openai"
env = "K2"
id = "openai-b"
weight = 1
"#,
        );
        let creds = two_key_credentials(&cfg);
        let firsts: Vec<String> = (0..8)
            .map(|_| {
                creds
                    .plan(&cfg, "platform", "openai")
                    .expect("plan")
                    .attempts[0]
                    .id
                    .clone()
            })
            .collect();
        assert_eq!(firsts.iter().filter(|id| *id == "openai-a").count(), 6);
        assert_eq!(firsts.iter().filter(|id| *id == "openai-b").count(), 2);
    }

    #[test]
    fn repeated_failures_park_one_credential_and_a_probe_recovers_it() {
        let cfg = config(TWO_PLATFORM_KEYS);
        let creds = two_key_credentials(&cfg);
        let now = Instant::now();
        let plan = creds
            .plan_at(&cfg, "platform", "openai", now)
            .expect("plan");
        let bad = &plan.attempts[0];
        assert_eq!(bad.id, "openai-a");
        creds.health.record_failure_at(&bad.health_key, now);
        creds.health.record_failure_at(&bad.health_key, now);

        for _ in 0..4 {
            let plan = creds
                .plan_at(&cfg, "platform", "openai", now)
                .expect("plan");
            let ids: Vec<&str> = plan.attempts.iter().map(|a| a.id.as_str()).collect();
            assert_eq!(ids, ["openai-b"], "parked credential must be skipped");
        }

        let after_cooldown = now + Duration::from_secs(31);
        let plan = creds
            .plan_at(&cfg, "platform", "openai", after_cooldown)
            .expect("plan");
        let ids: Vec<&str> = plan.attempts.iter().map(|a| a.id.as_str()).collect();
        assert_eq!(ids[0], "openai-a", "the probe leads the plan");

        let next = creds
            .plan_at(&cfg, "platform", "openai", after_cooldown)
            .expect("plan");
        let ids: Vec<&str> = next.attempts.iter().map(|a| a.id.as_str()).collect();
        assert_eq!(
            ids,
            ["openai-b"],
            "the probe is single-shot: a concurrent request must not retry the parked key"
        );
        let probed = plan
            .attempts
            .iter()
            .find(|a| a.id == "openai-a")
            .expect("probe lease");
        creds.record_success(probed);
        assert!(
            creds
                .health_snapshot()
                .iter()
                .all(|(_, state)| { *state == CredentialState::Healthy })
        );
    }

    #[test]
    fn a_fully_parked_pool_still_serves_a_forced_probe() {
        let cfg = config(TWO_PLATFORM_KEYS);
        let creds = two_key_credentials(&cfg);
        let now = Instant::now();
        for key in ["platform/openai/openai-a", "platform/openai/openai-b"] {
            creds.health.record_failure_at(key, now);
            creds.health.record_failure_at(key, now);
        }
        let plan = creds
            .plan_at(&cfg, "platform", "openai", now)
            .expect("a parked pool still yields one attempt");
        assert_eq!(plan.attempts.len(), 1);
    }

    #[test]
    fn byok_namespace_uses_its_own_pool_and_never_borrows_by_default() {
        let cfg = config(
            r#"
[[credential]]
namespace = "platform"
provider = "openai"
env = "K1"

[[credential]]
namespace = "acme"
provider = "openai"
env = "K2"
"#,
        );
        let creds = two_key_credentials(&cfg);
        let plan = creds.plan(&cfg, "acme", "openai").expect("plan");
        assert_eq!(plan.source, CredentialSource::Byok);
        let ids: Vec<&str> = plan.attempts.iter().map(|a| a.id.as_str()).collect();
        assert_eq!(ids, ["K2"], "BYOK pool must not include platform keys");

        let no_key = config(
            r#"
[[credential]]
namespace = "platform"
provider = "openai"
env = "K1"
"#,
        );
        let creds = Credentials::from_env(&no_key, &env(&[("K1", "sk-a")])).expect("credentials");
        assert!(creds.plan(&no_key, "acme", "openai").is_none());
    }

    #[test]
    fn platform_fallback_yields_the_whole_platform_pool_attributed_to_platform() {
        let cfg = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[namespace]]
id = "acme"
allow_platform_fallback = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

[[gateway_key]]
env = "AXOND_INBOUND_KEY"
namespace = "platform"
{TWO_PLATFORM_KEYS}
"#
        ))
        .expect("valid config");
        let creds = two_key_credentials(&cfg);
        let plan = creds.plan(&cfg, "acme", "openai").expect("plan");
        assert_eq!(plan.source, CredentialSource::Platform);
        assert_eq!(plan.attempts.len(), 2);
    }

    #[test]
    fn a_dangling_credential_reference_fails_at_boot() {
        let cfg = config(TWO_PLATFORM_KEYS);
        let Err(err) = Credentials::from_env(&cfg, &env(&[("K1", "sk-a"), ("K2", "")])) else {
            panic!("an unset credential env var must refuse to boot");
        };
        assert!(matches!(err, CredentialError::MissingEnv { .. }), "{err:?}");
    }
}
