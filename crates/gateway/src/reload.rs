//! Config hot-reload (ADR 0011).
//!
//! Two triggers, one path. `SIGHUP` is the explicit operator action; watching the
//! config file is opt-in (`[reload] watch = true`). Both end up in
//! [`Reloader::reload`], which re-reads the file *and* the process environment,
//! runs the full boot-time validation on the candidate, and publishes it as one
//! atomic snapshot swap.
//!
//! The semantics are reject-and-keep: any load, validation, or credential error
//! leaves the running config exactly as it was, so a bad edit fails at reload
//! rather than at request time — the fail-at-boot posture, applied again.
//!
//! Not everything a config file describes can be replaced in a live process.
//! The listening socket is already bound, the usage sinks already own
//! connections and flush tasks, and the budget store, rate limiter, and
//! revocation store already own their state, so changes to `[server] bind`,
//! `[[usage_sink]]`, `[usage_journal]`, `[budget]` (including
//! `limit_microdollars`), `[rate_limit]`, and `[revocation]` are reported and
//! ignored until the next restart.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use crate::config::{
    AdmissionConfig, BudgetConfig, Config, ConfigError, Mode, RateLimitConfig, Reload,
    RevocationConfig, Transport, UsageJournalConfig, UsageSinkConfig,
};
use crate::state::{AppState, ConfigSnapshot, SnapshotError};
use crate::telemetry;

/// A reload asked for by `SIGHUP`.
pub const TRIGGER_SIGNAL: &str = "sighup";
/// A reload the file watcher noticed.
pub const TRIGGER_WATCH: &str = "watch";

#[derive(Debug, thiserror::Error)]
pub enum ReloadError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("config resolution failed: {0}")]
    Snapshot(#[from] SnapshotError),
}

/// What the process committed to at startup and cannot redo while serving, so a
/// candidate is compared against what is *in effect* rather than against the
/// previous candidate.
struct Boot {
    /// Which authority the process booted with. `mode` is a bootstrap property:
    /// a reload cannot move a serving process between authorities (ADR 0027).
    mode: Mode,
    bind: SocketAddr,
    usage_sink: Vec<UsageSinkConfig>,
    usage_journal: UsageJournalConfig,
    budget: BudgetConfig,
    rate_limit: RateLimitConfig,
    revocation: RevocationConfig,
    transport: Transport,
    admission: AdmissionConfig,
}

/// Owns the config path and the state whose snapshot it replaces.
pub struct Reloader {
    path: String,
    state: AppState,
    boot: Boot,
    /// The file contents the last reload acted on — and the lock that serializes
    /// reloads. Shared between the triggers so the watcher does not repeat what a
    /// signal just applied, and two triggers cannot race the generation counter.
    seen: Mutex<Option<Vec<u8>>>,
}

impl Reloader {
    pub fn new(path: impl Into<String>, state: AppState) -> Self {
        let path = path.into();
        let booted = state.config();
        Self {
            seen: Mutex::new(std::fs::read(&path).ok()),
            boot: Boot {
                mode: booted.config.mode,
                bind: booted.config.server.bind,
                usage_sink: booted.config.usage_sink.clone(),
                usage_journal: booted.config.usage_journal.clone(),
                budget: booted.config.budget.clone(),
                rate_limit: booted.config.rate_limit.clone(),
                revocation: booted.config.revocation.clone(),
                transport: booted.config.transport.clone(),
                admission: booted.config.admission.clone(),
            },
            path,
            state,
        }
    }

    /// Reload from the config file and the *current* process environment, so a
    /// credential env-var exported after boot (a new BYOK tenant's key) resolves
    /// without a restart.
    pub fn reload(&self, trigger: &'static str) -> Result<ReloadSummary, ReloadError> {
        self.reload_with_env(trigger, &std::env::vars().collect())
    }

    /// An unconditional reload, against an explicit environment snapshot.
    pub fn reload_with_env(
        &self,
        trigger: &'static str,
        env: &HashMap<String, String>,
    ) -> Result<ReloadSummary, ReloadError> {
        let mut seen = self.lock_seen();
        *seen = std::fs::read(&self.path).ok();
        self.apply(trigger, env)
    }

    /// Reload only if the file's bytes differ from what the last reload acted on.
    /// The watcher's entry point: an edit its operator then `SIGHUP`s is applied
    /// once, not once per trigger.
    pub fn reload_if_changed_with_env(
        &self,
        trigger: &'static str,
        env: &HashMap<String, String>,
    ) -> Option<Result<ReloadSummary, ReloadError>> {
        let mut seen = self.lock_seen();
        // A momentarily unreadable path (mid rename) is not a change.
        let current = std::fs::read(&self.path).ok()?;
        if seen.as_deref() == Some(current.as_slice()) {
            return None;
        }
        *seen = Some(current);
        Some(self.apply(trigger, env))
    }

    fn reload_if_changed(
        &self,
        trigger: &'static str,
    ) -> Option<Result<ReloadSummary, ReloadError>> {
        self.reload_if_changed_with_env(trigger, &std::env::vars().collect())
    }

    /// Only ever held across synchronous work, so a poisoned guard carries no
    /// torn state: a reload reads, then publishes, and holds nothing else.
    fn lock_seen(&self) -> MutexGuard<'_, Option<Vec<u8>>> {
        self.seen
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn apply(
        &self,
        trigger: &'static str,
        env: &HashMap<String, String>,
    ) -> Result<ReloadSummary, ReloadError> {
        let span = telemetry::config_reload_span(trigger);
        let _entered = span.enter();
        let current = self.state.config();

        match self.candidate(env, &current, current.generation + 1) {
            Ok(candidate) => {
                // A file reload replaces what the file describes and nothing
                // else. Approved pricing is not in the file — it came from a
                // desired revision this replica converged on — so it is carried
                // onto the candidate rather than dropped: publishing a snapshot
                // without it would unprice a converged deployment on the next
                // `SIGHUP`, which is the one way pricing could disappear without
                // a revision saying so. Which pricing convergence *replaces* is
                // its own business (#142); this only refuses to lose it.
                let candidate = match current.pricing() {
                    None => candidate,
                    Some(pricing) => candidate.with_pricing(pricing.clone()),
                };
                let summary = ReloadSummary::between(&self.boot, &current, &candidate);
                let generation = candidate.generation;
                self.state.publish(candidate);
                telemetry::finish_config_reload(
                    &span,
                    trigger,
                    telemetry::RELOAD_APPLIED,
                    generation,
                );
                summary.log_applied(trigger, &self.path);
                Ok(summary)
            }
            Err(err) => {
                telemetry::finish_config_reload(
                    &span,
                    trigger,
                    telemetry::RELOAD_REJECTED,
                    current.generation,
                );
                tracing::error!(
                    trigger,
                    path = %self.path,
                    generation = current.generation,
                    error = %err,
                    "config reload rejected; the running config keeps serving"
                );
                Err(err)
            }
        }
    }

    /// Build the candidate snapshot. Nothing here touches the running state, so
    /// a failure at any step is a no-op for the serving config.
    ///
    /// The candidate inherits `current`'s resolved secret material rather than
    /// starting empty: a file reload is not a convergence step and has no
    /// resolver to unwrap anything with, so publishing an empty set would drop
    /// the material the reconciler resolved — and zeroize it as the old snapshot
    /// went — leaving the replica serving without credentials it holds. Editing
    /// the file cannot change which versions a revision pins; only a new revision
    /// can, and that path resolves them.
    fn candidate(
        &self,
        env: &HashMap<String, String>,
        current: &ConfigSnapshot,
        generation: u64,
    ) -> Result<ConfigSnapshot, ReloadError> {
        let config = Config::load(&self.path)?;
        if config.mode != self.boot.mode {
            return Err(ReloadError::Config(ConfigError::Invalid(format!(
                "`mode` is a bootstrap property: this process booted in `{}` mode and a reload \
                 cannot switch it to `{}`; restart with the new configuration",
                self.boot.mode.as_str(),
                config.mode.as_str()
            ))));
        }
        Ok(ConfigSnapshot::build_with(
            config,
            env,
            generation,
            current.secrets().clone(),
        )?)
    }

    fn watch_settings(&self) -> Reload {
        self.state.config().config.reload.clone()
    }
}

/// What one reload changed, for the log line an operator reads afterwards.
/// Identifiers and short fingerprints only — sources are references, never
/// secrets.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReloadSummary {
    pub namespaces: Delta,
    pub providers: Delta,
    pub models: Delta,
    pub credentials: Delta,
    pub gateway_keys: Delta,
    pub gateway_verifiers: Delta,
    pub gateway_minting: Delta,
    pub gateway_token_epochs: Delta,
    pub gateway_token_audience: Delta,
    pub gateway_key_fingerprints: HashMap<String, String>,
    pub gateway_verifier_fingerprints: HashMap<String, String>,
    pub gateway_minting_fingerprint: Option<String>,
    /// Minting is configured, but no static key is authorized to use it.
    pub gateway_minting_without_authorized_key: bool,
    /// Static keys declaring `can_mint` while minting is disabled.
    pub gateway_minting_inert_keys: Vec<String>,
    /// `[server] bind` differs from what the process bound at startup.
    pub bind_changed: bool,
    /// `[[usage_sink]]` differs from the connected sinks.
    pub usage_sinks_changed: bool,
    /// `[usage_journal]` differs from the outbox the delivery worker was built
    /// with.
    pub usage_journal_changed: bool,
    /// `[budget]` differs from the booted store configuration.
    pub budget_changed: bool,
    /// `[rate_limit]` differs from the booted limiter configuration.
    pub rate_limit_changed: bool,
    /// `[revocation]` differs from the booted revocation store configuration.
    pub revocation_changed: bool,
    /// `[transport]` differs from the bounds the shared HTTP client was built
    /// with. Validated on reload, applied on restart.
    pub transport_changed: bool,
    /// `[admission]` differs from the ceilings admission control was built with.
    /// Validated on reload, applied on restart.
    pub admission_changed: bool,
}

/// The added and removed identifiers of one config collection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Delta {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub changed: Vec<String>,
}

impl Delta {
    fn between(before: impl Iterator<Item = String>, after: impl Iterator<Item = String>) -> Self {
        let before: BTreeSet<String> = before.collect();
        let after: BTreeSet<String> = after.collect();
        Self {
            added: after.difference(&before).cloned().collect(),
            removed: before.difference(&after).cloned().collect(),
            changed: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.changed.is_empty()
    }
}

impl std::fmt::Display for Delta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_empty() {
            return f.write_str("unchanged");
        }
        write!(
            f,
            "+[{}] -[{}]",
            self.added.join(","),
            self.removed.join(",")
        )?;
        if !self.changed.is_empty() {
            write!(f, " ~[{}]", self.changed.join(","))?;
        }
        Ok(())
    }
}

impl ReloadSummary {
    fn gateway_minting_route_added(&self) -> bool {
        !self.gateway_minting.added.is_empty()
    }

    fn gateway_minting_route_removed(&self) -> bool {
        !self.gateway_minting.removed.is_empty()
    }

    fn between(boot: &Boot, before: &ConfigSnapshot, after: &ConfigSnapshot) -> Self {
        let before_config = &before.config;
        let after_config = &after.config;
        Self {
            namespaces: Delta::between(
                before_config.namespace.iter().map(|n| n.id.clone()),
                after_config.namespace.iter().map(|n| n.id.clone()),
            ),
            providers: Delta::between(
                before_config.provider.iter().map(|p| p.id.clone()),
                after_config.provider.iter().map(|p| p.id.clone()),
            ),
            models: Delta::between(
                before_config.model.iter().map(|m| m.name.clone()),
                after_config.model.iter().map(|m| m.name.clone()),
            ),
            credentials: Delta::between(
                before_config.credential.iter().map(credential_key),
                after_config.credential.iter().map(credential_key),
            )
            .with_changed(credential_version_changes(before_config, after_config)),
            gateway_keys: Delta::between(
                before_config
                    .gateway_key
                    .iter()
                    .map(|k| k.source_label().unwrap_or_default().to_owned()),
                after_config
                    .gateway_key
                    .iter()
                    .map(|k| k.source_label().unwrap_or_default().to_owned()),
            )
            .with_changed(
                gateway_key_definition_changes(before_config, after_config)
                    .into_iter()
                    .chain(material_changes(
                        &before.gateway_key_fingerprints,
                        &after.gateway_key_fingerprints,
                    )),
            ),
            gateway_verifiers: Delta::between(
                before_config.gateway_verifier.iter().map(|v| v.kid.clone()),
                after_config.gateway_verifier.iter().map(|v| v.kid.clone()),
            )
            .with_changed(
                verifier_definition_changes(before_config, after_config)
                    .into_iter()
                    .chain(material_changes(
                        &before.gateway_verifier_fingerprints,
                        &after.gateway_verifier_fingerprints,
                    )),
            ),
            gateway_minting: Delta::between(
                before_config
                    .gateway_minting
                    .iter()
                    .map(|_| "enabled".to_owned()),
                after_config
                    .gateway_minting
                    .iter()
                    .map(|_| "enabled".to_owned()),
            )
            .with_changed(
                (before_config.gateway_minting.is_some()
                    && after_config.gateway_minting.is_some()
                    && before_config.gateway_minting.as_ref().map(|m| {
                        (
                            &m.kid,
                            m.source_label(),
                            &m.max_ttl,
                            &m.scope,
                            &m.aliases,
                            &m.max_request_microdollars,
                        )
                    }) != after_config.gateway_minting.as_ref().map(|m| {
                        (
                            &m.kid,
                            m.source_label(),
                            &m.max_ttl,
                            &m.scope,
                            &m.aliases,
                            &m.max_request_microdollars,
                        )
                    })
                    || (before_config.gateway_minting.is_some()
                        && after_config.gateway_minting.is_some()
                        && before.gateway_minting.as_ref().map(|m| m.max_ttl)
                            != after.gateway_minting.as_ref().map(|m| m.max_ttl)))
                .then(|| "enabled".to_owned())
                .into_iter()
                .chain(
                    (before_config.gateway_minting.is_some()
                        && after_config.gateway_minting.is_some()
                        && before.gateway_minting_fingerprint != after.gateway_minting_fingerprint)
                        .then(|| "gateway_minting".to_owned()),
                ),
            ),
            gateway_token_epochs: Delta::between(
                before_config
                    .gateway_token_epoch
                    .iter()
                    .map(gateway_token_epoch_key),
                after_config
                    .gateway_token_epoch
                    .iter()
                    .map(gateway_token_epoch_key),
            )
            .with_changed(gateway_token_epoch_definition_changes(
                before_config,
                after_config,
            )),
            gateway_token_audience: Delta::between(
                before_config
                    .gateway_token
                    .iter()
                    .map(|token| token.audience.clone()),
                after_config
                    .gateway_token
                    .iter()
                    .map(|token| token.audience.clone()),
            ),
            gateway_key_fingerprints: after.gateway_key_fingerprints.clone(),
            gateway_verifier_fingerprints: after.gateway_verifier_fingerprints.clone(),
            gateway_minting_fingerprint: after.gateway_minting_fingerprint.clone(),
            gateway_minting_without_authorized_key: after_config.gateway_minting.is_some()
                && !after_config.gateway_key.iter().any(|key| key.can_mint),
            gateway_minting_inert_keys: if after_config.gateway_minting.is_none() {
                after_config
                    .gateway_key
                    .iter()
                    .filter(|key| key.can_mint)
                    .filter_map(|key| key.source_label().map(str::to_owned))
                    .collect()
            } else {
                Vec::new()
            },
            bind_changed: boot.bind != after_config.server.bind,
            usage_sinks_changed: boot.usage_sink != after_config.usage_sink,
            usage_journal_changed: boot.usage_journal != after_config.usage_journal,
            budget_changed: boot.budget != after_config.budget,
            rate_limit_changed: boot.rate_limit != after_config.rate_limit,
            revocation_changed: boot.revocation != after_config.revocation,
            transport_changed: boot.transport != after_config.transport,
            admission_changed: boot.admission != after_config.admission,
        }
    }

    /// Whether anything a reload can actually apply differs.
    pub fn is_empty(&self) -> bool {
        self.namespaces.is_empty()
            && self.providers.is_empty()
            && self.models.is_empty()
            && self.credentials.is_empty()
            && self.gateway_keys.is_empty()
            && self.gateway_verifiers.is_empty()
            && self.gateway_minting.is_empty()
            && self.gateway_token_epochs.is_empty()
            && self.gateway_token_audience.is_empty()
    }

    fn log_applied(&self, trigger: &'static str, path: &str) {
        tracing::info!(
            trigger,
            path = %path,
            namespaces = %self.namespaces,
            providers = %self.providers,
            models = %self.models,
            credentials = %self.credentials,
            gateway_keys = %self.gateway_keys,
            gateway_verifiers = %self.gateway_verifiers,
            gateway_minting = %self.gateway_minting,
            gateway_token_epochs = %self.gateway_token_epochs,
            gateway_token_audience = %self.gateway_token_audience,
            gateway_key_fingerprints = ?self.gateway_key_fingerprints,
            gateway_verifier_fingerprints = ?self.gateway_verifier_fingerprints,
            gateway_minting_fingerprint = ?self.gateway_minting_fingerprint,
            budget_changed = self.budget_changed,
            rate_limit_changed = self.rate_limit_changed,
            revocation_changed = self.revocation_changed,
            changed = !self.is_empty(),
            "config reloaded"
        );
        if self.bind_changed {
            tracing::warn!(
                "`[server] bind` changed, but the listening socket is already bound; restart to apply it"
            );
        }
        if self.usage_sinks_changed {
            tracing::warn!(
                "`[[usage_sink]]` changed, but sinks own live connections; restart to apply it"
            );
        }
        if self.usage_journal_changed {
            tracing::warn!(
                "`[usage_journal]` changed, but the outbox connection and its delivery worker are \
                 built at boot; restart to apply it"
            );
        }
        if self.budget_changed {
            tracing::warn!(
                "`[budget]` changed, but the budget store is already serving; restart to apply it"
            );
        }
        if self.rate_limit_changed {
            tracing::warn!(
                "`[rate_limit]` changed, but the limiter is already serving; restart to apply it"
            );
        }
        if self.gateway_minting_route_added() {
            tracing::warn!(
                "`[gateway_minting]` was enabled, but `/v1/tokens` route registration is boot-time; restart to expose it"
            );
        }
        if self.gateway_minting_route_removed() {
            tracing::warn!(
                "`[gateway_minting]` was removed; issuance is disabled immediately and `/v1/tokens` returns typed 404"
            );
        }
        if self.gateway_minting_without_authorized_key {
            tracing::warn!(
                "`[gateway_minting]` is configured, but no gateway key has `can_mint = true`; `/v1/tokens` rejects every caller"
            );
        }
        if !self.gateway_minting_inert_keys.is_empty() {
            tracing::warn!(
                keys = ?self.gateway_minting_inert_keys,
                "`can_mint = true` has no effect because `[gateway_minting]` is absent"
            );
        }
        if self.revocation_changed {
            tracing::warn!(
                "`[revocation]` changed, but the revocation store is already serving; restart to apply it"
            );
        }
        if self.transport_changed {
            tracing::warn!(
                "`[transport]` changed, but the upstream HTTP client is already pooled; restart to apply it"
            );
        }
        if self.admission_changed {
            tracing::warn!(
                "`[admission]` changed, but the admission ceilings are already serving requests; restart to apply them"
            );
        }
    }
}

/// `namespace/provider/label` — the pool member a credential entry declares.
fn credential_key(c: &crate::config::Credential) -> String {
    format!("{}/{}/{}", c.namespace, c.provider, c.label())
}

/// Pool members whose material moved to another version of the same secret.
///
/// Rotation keeps the label — a projected credential's label is its resource
/// slug — and moves only the version it references, so the key set alone would
/// report an unchanged pool while every entry in it authenticates with new
/// material. The reference is opaque, so naming it is not a disclosure.
fn credential_version_changes(before: &Config, after: &Config) -> Vec<String> {
    let versions = |config: &Config| -> HashMap<String, crate::desired_state::SecretRef> {
        config
            .credential
            .iter()
            .filter_map(|credential| {
                credential
                    .secret
                    .map(|secret| (credential_key(credential), secret))
            })
            .collect()
    };
    let (before, after) = (versions(before), versions(after));
    let mut changed = before
        .iter()
        .filter(|(key, secret)| after.get(*key).is_some_and(|current| current != *secret))
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    changed.sort();
    changed
}

fn gateway_token_epoch_key(epoch: &crate::config::GatewayTokenEpoch) -> String {
    match epoch.subject.as_deref() {
        Some(subject) => format!("{}/{}", epoch.namespace, subject),
        None => epoch.namespace.clone(),
    }
}

impl Delta {
    fn with_changed(mut self, changed: impl IntoIterator<Item = String>) -> Self {
        self.changed = changed
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        self
    }
}

fn verifier_definition_changes(before: &Config, after: &Config) -> Vec<String> {
    let before: HashMap<&str, &crate::config::GatewayVerifier> = before
        .gateway_verifier
        .iter()
        .map(|verifier| (verifier.kid.as_str(), verifier))
        .collect();
    let after: HashMap<&str, &crate::config::GatewayVerifier> = after
        .gateway_verifier
        .iter()
        .map(|verifier| (verifier.kid.as_str(), verifier))
        .collect();
    let mut changed = before
        .keys()
        .filter_map(|kid| {
            let before = before[kid];
            let after = after.get(kid)?;
            (before.alg != after.alg
                || before.source_label() != after.source_label()
                || before.namespaces != after.namespaces
                || before.max_ttl != after.max_ttl)
                .then(|| (*kid).to_owned())
        })
        .collect::<Vec<_>>();
    changed.sort();
    changed
}

fn gateway_token_epoch_definition_changes(before: &Config, after: &Config) -> Vec<String> {
    let before: HashMap<String, u64> = before
        .gateway_token_epoch
        .iter()
        .map(|epoch| (gateway_token_epoch_key(epoch), epoch.min_iat))
        .collect();
    let after: HashMap<String, u64> = after
        .gateway_token_epoch
        .iter()
        .map(|epoch| (gateway_token_epoch_key(epoch), epoch.min_iat))
        .collect();
    let mut changed = before
        .iter()
        .filter(|(key, min_iat)| after.get(*key).is_some_and(|current| current != *min_iat))
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    changed.sort();
    changed
}

fn gateway_key_definition_changes(before: &Config, after: &Config) -> Vec<String> {
    let before: HashMap<&str, &crate::config::GatewayKey> = before
        .gateway_key
        .iter()
        .filter_map(|key| key.source_label().map(|label| (label, key)))
        .collect();
    let after: HashMap<&str, &crate::config::GatewayKey> = after
        .gateway_key
        .iter()
        .filter_map(|key| key.source_label().map(|label| (label, key)))
        .collect();
    let mut changed = before
        .keys()
        .filter_map(|label| {
            let before = before[label];
            let after = after.get(label)?;
            (before.namespace != after.namespace || before.can_mint != after.can_mint)
                .then(|| (*label).to_owned())
        })
        .collect::<Vec<_>>();
    changed.sort();
    changed
}

fn material_changes(
    before: &HashMap<String, String>,
    after: &HashMap<String, String>,
) -> Vec<String> {
    let mut changed = before
        .iter()
        .filter(|(label, fingerprint)| {
            after
                .get(*label)
                .is_some_and(|current| current != *fingerprint)
        })
        .map(|(label, _)| label.clone())
        .collect::<Vec<_>>();
    changed.sort();
    changed
}

/// Wire the reload triggers up for the process lifetime: the `SIGHUP` handler,
/// and the file watcher (which consults `[reload]` on the *current* config each
/// pass, so watching can itself be turned on by a reload).
pub fn spawn(reloader: Arc<Reloader>) {
    #[cfg(unix)]
    tokio::spawn(signal_loop(reloader.clone()));
    tokio::spawn(watch_loop(reloader));
}

#[cfg(unix)]
async fn signal_loop(reloader: Arc<Reloader>) {
    use tokio::signal::unix::{SignalKind, signal};

    let mut hangup = match signal(SignalKind::hangup()) {
        Ok(stream) => stream,
        Err(err) => {
            tracing::error!(error = %err, "SIGHUP handler could not be installed; config reload on signal is unavailable");
            return;
        }
    };
    while hangup.recv().await.is_some() {
        let _ = reloader.reload(TRIGGER_SIGNAL);
    }
}

/// Watch by comparing the file's bytes rather than its mtime, so an editor's
/// in-place write and a Kubernetes ConfigMap's symlink swap both register, while
/// a touched-but-identical file — or an edit a `SIGHUP` already applied — does
/// not. The comparison lives on the `Reloader`, shared with the signal path.
async fn watch_loop(reloader: Arc<Reloader>) {
    loop {
        let settings = reloader.watch_settings();
        tokio::time::sleep(Duration::from_millis(settings.poll_interval_ms)).await;
        if !settings.watch {
            continue;
        }
        let _ = reloader.reload_if_changed(TRIGGER_WATCH);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::NoBudget;
    use crate::principals::Presented;
    use crate::routes;
    use crate::usage::{StdoutSink, UsageFanout, UsageSink};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use serde::Serialize;
    use serde_json::Value;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tower::util::ServiceExt;

    /// A config file this test owns, removed when the test ends.
    struct ConfigFile(PathBuf);

    impl ConfigFile {
        fn new(contents: &str) -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "axond-reload-{}-{}.toml",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::write(&path, contents).expect("write config");
            Self(path)
        }

        fn rewrite(&self, contents: &str) {
            std::fs::write(&self.0, contents).expect("rewrite config");
        }

        fn path(&self) -> &str {
            self.0.to_str().expect("utf-8 path")
        }
    }

    impl Drop for ConfigFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    /// The inbound key every candidate declares, resolved from the test
    /// environment: inbound auth fails closed, so a config without one would not
    /// boot at all (ADR 0013).
    const INBOUND_KEY_ENV: &str = "AXOND_INBOUND_KEY";

    const PLATFORM_ONLY: &str = r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

[[credential]]
namespace = "platform"
provider = "openai"
env = "PLATFORM_OPENAI_KEY"

[[gateway_key]]
env = "AXOND_INBOUND_KEY"
namespace = "platform"

[[model]]
name = "gpt-4o"
targets = [{ provider = "openai", model = "gpt-4o", price = { input_microdollars_per_million = 2500000, output_microdollars_per_million = 10000000 } }]
"#;

    /// The BYOK onboarding this feature exists for: a new namespace, its
    /// credential, and an alias that routes to it.
    const WITH_BYOK_TENANT: &str = r#"
[[namespace]]
id = "platform"
default = true

[[namespace]]
id = "acme"

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

[[credential]]
namespace = "platform"
provider = "openai"
env = "PLATFORM_OPENAI_KEY"

[[gateway_key]]
env = "AXOND_INBOUND_KEY"
namespace = "platform"

[[credential]]
namespace = "acme"
provider = "openai"
env = "ACME_OPENAI_KEY"

[[model]]
name = "gpt-4o"
targets = [{ provider = "openai", model = "gpt-4o", price = { input_microdollars_per_million = 2500000, output_microdollars_per_million = 10000000 } }]

[[model]]
name = "acme-fast"
targets = [{ provider = "openai", model = "gpt-4o-mini", price = { input_microdollars_per_million = 150000, output_microdollars_per_million = 600000 } }]
"#;

    const WITH_MINTED_NAMESPACES: &str = r#"
[[namespace]]
id = "platform"
default = true

[[namespace]]
id = "acme"

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

[[credential]]
namespace = "platform"
provider = "openai"
env = "PLATFORM_OPENAI_KEY"

[[gateway_key]]
env = "AXOND_INBOUND_KEY"
namespace = "platform"

[gateway_token]
audience = "reload-test"

[[gateway_verifier]]
kid = "reload-kid"
alg = "HS256"
env = "JWT_SECRET"
namespaces = ["platform", "acme"]
max_ttl = "15m"
"#;

    const WITH_GATEWAY_MINTING: &str = r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

[[credential]]
namespace = "platform"
provider = "openai"
env = "PLATFORM_OPENAI_KEY"

[[gateway_key]]
env = "AXOND_INBOUND_KEY"
namespace = "platform"
can_mint = true

[[gateway_key]]
env = "AXOND_SECOND_KEY"
namespace = "platform"
can_mint = true

[gateway_token]
audience = "reload-test"

[[gateway_verifier]]
kid = "reload-kid"
alg = "HS256"
env = "JWT_SECRET"
namespaces = ["platform"]
max_ttl = "15m"

[gateway_minting]
kid = "reload-kid"
env = "SIGNING_KEY"
max_ttl = "10m"
scope = ["chat", "models"]
"#;

    fn state_from(file: &ConfigFile) -> AppState {
        let config = Config::load(file.path()).expect("valid boot config");
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(StdoutSink)];
        AppState::new(
            config,
            &inbound_env(),
            UsageFanout::new(sinks),
            Box::new(NoBudget),
        )
        .expect("boot state")
    }

    /// The inbound gateway key every servable config needs, plus the platform
    /// provider credential both fixtures declare.
    fn inbound_env() -> HashMap<String, String> {
        [
            (INBOUND_KEY_ENV.to_string(), "inbound-secret".to_string()),
            ("PLATFORM_OPENAI_KEY".to_string(), "sk-platform".to_string()),
            (
                "JWT_SECRET".to_string(),
                "jwt-test-secret-0123456789012345".to_string(),
            ),
        ]
        .into_iter()
        .collect()
    }

    fn minting_env() -> HashMap<String, String> {
        let mut env = inbound_env();
        env.insert("AXOND_SECOND_KEY".to_string(), "second-secret".to_string());
        env.insert(
            "SIGNING_KEY".to_string(),
            "jwt-test-secret-0123456789012345".to_string(),
        );
        env
    }

    fn tenant_env() -> HashMap<String, String> {
        let mut env = inbound_env();
        env.insert("ACME_OPENAI_KEY".to_string(), "sk-acme".to_string());
        env
    }

    async fn listed_aliases(state: &AppState) -> Vec<String> {
        let resp = routes::router(state.clone())
            .oneshot(
                Request::get("/v1/models")
                    .header(axum::http::header::AUTHORIZATION, "Bearer inbound-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&bytes).unwrap();
        json["data"]
            .as_array()
            .expect("data array")
            .iter()
            .map(|m| m["id"].as_str().expect("alias").to_string())
            .collect()
    }

    #[tokio::test]
    async fn onboarding_a_byok_namespace_serves_without_a_restart() {
        let file = ConfigFile::new(PLATFORM_ONLY);
        let state = state_from(&file);
        let reloader = Reloader::new(file.path(), state.clone());
        assert_eq!(listed_aliases(&state).await, vec!["gpt-4o".to_string()]);

        file.rewrite(WITH_BYOK_TENANT);
        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &tenant_env())
            .expect("candidate is valid");

        assert_eq!(summary.namespaces.added, vec!["acme".to_string()]);
        assert_eq!(summary.models.added, vec!["acme-fast".to_string()]);
        assert_eq!(
            summary.credentials.added,
            vec!["acme/openai/ACME_OPENAI_KEY".to_string()]
        );
        assert!(!summary.bind_changed);
        assert_eq!(state.config().generation, 1);
        assert!(
            state
                .config()
                .credentials
                .plan(&state.config().config, "acme", "openai")
                .is_some()
        );
        let aliases = listed_aliases(&state).await;
        assert!(aliases.contains(&"acme-fast".to_string()));
    }

    /// A file reload republishes the snapshot, and the durable material the
    /// reconciler unwrapped has to survive that: a reload has no resolver, so
    /// publishing an empty set would drop every resolved version — zeroizing it
    /// as the old snapshot went — and leave the replica serving credentials it
    /// can no longer open.
    #[tokio::test]
    async fn a_reload_carries_the_resolved_material_the_running_snapshot_holds() {
        let file = ConfigFile::new(PLATFORM_ONLY);
        let materialization = crate::convergence::secrets::testing::permissive();
        let resolved = materialization
            .resolve(&crate::desired_state::fixtures::state())
            .await
            .expect("the fixture's material resolves");
        let held = resolved.references();
        assert!(!held.is_empty(), "the fixture pins material");

        // A replica that converged once: the reconciler's snapshot, owning the
        // versions its compilation resolved.
        let state = state_from(&file);
        state.publish(
            ConfigSnapshot::build_with(
                Config::load(file.path()).expect("valid boot config"),
                &inbound_env(),
                0,
                resolved,
            )
            .expect("a servable snapshot"),
        );
        let reloader = Reloader::new(file.path(), state.clone());

        file.rewrite(&format!("{PLATFORM_ONLY}\n[reload]\nwatch = false\n"));
        reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("candidate is valid");

        assert_eq!(state.config().generation, 1, "the edit was applied");
        assert_eq!(
            state.config().secrets().references(),
            held,
            "the reloaded snapshot holds the same versions"
        );
        assert_eq!(
            materialization.ledger().retained(),
            held,
            "and nothing was released, so nothing was zeroized"
        );
    }

    /// The mode picks which authority owns durable resources, which a serving
    /// process cannot change under itself: the reload is refused and the
    /// previous configuration keeps serving (ADR 0027).
    #[tokio::test]
    async fn a_reload_cannot_switch_operating_mode() {
        let file = ConfigFile::new(PLATFORM_ONLY);
        let state = state_from(&file);
        let reloader = Reloader::new(file.path(), state.clone());

        file.rewrite(
            r#"
mode = "stateful"

[control_plane]
dsn_env = "GW_CONTROL_PLANE_DSN"

[secret_store]
kek_env = "GW_SECRET_STORE_KEK"

[[admin_breakglass]]
env = "GW_ADMIN_BREAKGLASS"
"#,
        );
        let error = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect_err("a mode switch needs a restart");

        let message = error.to_string();
        assert!(message.contains("stateless"), "{message}");
        assert!(message.contains("stateful"), "{message}");
        assert_eq!(state.config().generation, 0, "the old config keeps serving");
        assert_eq!(listed_aliases(&state).await, vec!["gpt-4o".to_string()]);
    }

    /// An epoch-only edit is visible in the applied reload summary, while a
    /// second reload of the same content is a no-op.
    #[tokio::test]
    async fn reload_summary_reports_token_epoch_changes_and_noop_reloads() {
        let file = ConfigFile::new(PLATFORM_ONLY);
        let state = state_from(&file);
        let reloader = Reloader::new(file.path(), state);
        let epoch_config = format!(
            "{PLATFORM_ONLY}\n[[gateway_token_epoch]]\nnamespace = \"platform\"\nmin_iat = 1\n"
        );

        file.rewrite(&epoch_config);
        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("epoch candidate is valid");
        assert_eq!(
            summary.gateway_token_epochs.added,
            vec!["platform".to_string()]
        );
        assert!(summary.gateway_token_epochs.removed.is_empty());
        assert!(summary.gateway_token_epochs.changed.is_empty());
        assert!(!summary.is_empty());

        file.rewrite(&epoch_config.replace("min_iat = 1", "min_iat = 2"));
        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("changed epoch candidate is valid");
        assert_eq!(
            summary.gateway_token_epochs.changed,
            vec!["platform".to_string()]
        );

        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("no-op candidate is valid");
        assert!(summary.gateway_token_epochs.is_empty());
        assert!(summary.is_empty());
    }

    #[test]
    fn reload_summary_reports_can_mint_toggles() {
        let before_config = Config::from_toml_str(WITH_GATEWAY_MINTING).unwrap();
        let after_config = Config::from_toml_str(&WITH_GATEWAY_MINTING.replacen(
            "can_mint = true",
            "can_mint = false",
            1,
        ))
        .unwrap();
        let before = ConfigSnapshot::build(before_config, &minting_env(), 0).unwrap();
        let after = ConfigSnapshot::build(after_config, &minting_env(), 1).unwrap();
        let boot = Boot {
            mode: before.config.mode,
            bind: before.config.server.bind,
            usage_sink: before.config.usage_sink.clone(),
            usage_journal: before.config.usage_journal.clone(),
            budget: before.config.budget.clone(),
            rate_limit: before.config.rate_limit.clone(),
            revocation: before.config.revocation.clone(),
            transport: before.config.transport.clone(),
            admission: before.config.admission.clone(),
        };
        let summary = ReloadSummary::between(&boot, &before, &after);
        assert_eq!(
            summary.gateway_keys.changed,
            vec!["AXOND_INBOUND_KEY".to_owned()]
        );
        assert!(!summary.is_empty());
    }

    #[test]
    fn reload_summary_reports_minting_material_rotation() {
        let config = Config::from_toml_str(WITH_GATEWAY_MINTING).unwrap();
        let before = ConfigSnapshot::build(config.clone(), &minting_env(), 0).unwrap();
        let mut rotated_env = minting_env();
        rotated_env.insert(
            "SIGNING_KEY".to_string(),
            "rotated-signing-secret-012345678901234567".to_string(),
        );
        rotated_env.insert(
            "JWT_SECRET".to_string(),
            "rotated-signing-secret-012345678901234567".to_string(),
        );
        let after = ConfigSnapshot::build(config, &rotated_env, 1).unwrap();
        let boot = Boot {
            mode: before.config.mode,
            bind: before.config.server.bind,
            usage_sink: before.config.usage_sink.clone(),
            usage_journal: before.config.usage_journal.clone(),
            budget: before.config.budget.clone(),
            rate_limit: before.config.rate_limit.clone(),
            revocation: before.config.revocation.clone(),
            transport: before.config.transport.clone(),
            admission: before.config.admission.clone(),
        };
        let summary = ReloadSummary::between(&boot, &before, &after);
        assert_eq!(
            summary.gateway_minting.changed,
            vec!["gateway_minting".to_owned()]
        );
        assert!(!summary.is_empty());
        assert!(summary.gateway_minting.added.is_empty());
        assert!(summary.gateway_minting.removed.is_empty());
        assert!(!summary.gateway_minting_route_added());
        assert!(!summary.gateway_minting_route_removed());
    }

    #[test]
    fn reload_summary_separates_minting_add_remove_from_changes() {
        let disabled_config = WITH_GATEWAY_MINTING
            .replace("[gateway_minting]\nkid = \"reload-kid\"\nenv = \"SIGNING_KEY\"\nmax_ttl = \"10m\"\n", "")
            .replace("can_mint = true", "can_mint = false");
        let disabled = ConfigSnapshot::build(
            Config::from_toml_str(&disabled_config).unwrap(),
            &minting_env(),
            0,
        )
        .unwrap();
        let enabled = ConfigSnapshot::build(
            Config::from_toml_str(WITH_GATEWAY_MINTING).unwrap(),
            &minting_env(),
            1,
        )
        .unwrap();
        let boot = Boot {
            mode: disabled.config.mode,
            bind: disabled.config.server.bind,
            usage_sink: disabled.config.usage_sink.clone(),
            usage_journal: disabled.config.usage_journal.clone(),
            budget: disabled.config.budget.clone(),
            rate_limit: disabled.config.rate_limit.clone(),
            revocation: disabled.config.revocation.clone(),
            transport: disabled.config.transport.clone(),
            admission: disabled.config.admission.clone(),
        };

        let added = ReloadSummary::between(&boot, &disabled, &enabled);
        assert_eq!(added.gateway_minting.added, vec!["enabled".to_owned()]);
        assert!(added.gateway_minting.changed.is_empty());

        let removed = ReloadSummary::between(&boot, &enabled, &disabled);
        assert_eq!(removed.gateway_minting.removed, vec!["enabled".to_owned()]);
        assert!(removed.gateway_minting.changed.is_empty());

        let inert = ConfigSnapshot::build(
            Config::from_toml_str(
                &WITH_GATEWAY_MINTING.replace(
                    "[gateway_minting]\nkid = \"reload-kid\"\nenv = \"SIGNING_KEY\"\nmax_ttl = \"10m\"\n",
                    "",
                ),
            )
            .unwrap(),
            &minting_env(),
            2,
        )
        .unwrap();
        let inert_summary = ReloadSummary::between(&boot, &enabled, &inert);
        assert_eq!(
            inert_summary.gateway_minting_inert_keys,
            vec![
                "AXOND_INBOUND_KEY".to_owned(),
                "AXOND_SECOND_KEY".to_owned()
            ]
        );

        let no_authorized_key = ConfigSnapshot::build(
            Config::from_toml_str(
                &WITH_GATEWAY_MINTING.replace("can_mint = true", "can_mint = false"),
            )
            .unwrap(),
            &minting_env(),
            2,
        )
        .unwrap();
        let warning = ReloadSummary::between(&boot, &no_authorized_key, &no_authorized_key);
        assert!(warning.gateway_minting_without_authorized_key);
    }

    #[test]
    fn reload_summary_reports_inherited_minting_ttl_changes() {
        let before_config = WITH_GATEWAY_MINTING.replace("max_ttl = \"10m\"\n", "");
        let after_config = before_config.replace("max_ttl = \"15m\"", "max_ttl = \"20m\"");
        let before = ConfigSnapshot::build(
            Config::from_toml_str(&before_config).unwrap(),
            &minting_env(),
            0,
        )
        .unwrap();
        let after = ConfigSnapshot::build(
            Config::from_toml_str(&after_config).unwrap(),
            &minting_env(),
            1,
        )
        .unwrap();
        let boot = Boot {
            mode: before.config.mode,
            bind: before.config.server.bind,
            usage_sink: before.config.usage_sink.clone(),
            usage_journal: before.config.usage_journal.clone(),
            budget: before.config.budget.clone(),
            rate_limit: before.config.rate_limit.clone(),
            revocation: before.config.revocation.clone(),
            transport: before.config.transport.clone(),
            admission: before.config.admission.clone(),
        };
        let summary = ReloadSummary::between(&boot, &before, &after);
        assert_eq!(summary.gateway_minting.changed, vec!["enabled".to_owned()]);
    }

    /// An issuance epoch is part of the immutable candidate snapshot: SIGHUP
    /// applies it to one namespace while a different namespace keeps serving.
    #[tokio::test]
    async fn minted_token_epochs_apply_after_reload_without_affecting_other_namespaces() {
        let file = ConfigFile::new(WITH_MINTED_NAMESPACES);
        let state = state_from(&file);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_secs();
        let make_token = |namespace: &str| {
            let claims = ReloadTokenClaims {
                exp: now + 890,
                iat: now - 10,
                jti: format!("reload-{namespace}"),
                ns: namespace.to_owned(),
                sub: "reload-subject".to_owned(),
                aud: "reload-test".to_owned(),
            };
            let mut header = Header::new(Algorithm::HS256);
            header.kid = Some("reload-kid".to_owned());
            format!(
                "axt1.{}",
                encode(
                    &header,
                    &claims,
                    &EncodingKey::from_secret(b"jwt-test-secret-0123456789012345"),
                )
                .expect("token signs")
            )
        };
        let platform_token = make_token("platform");
        let acme_token = make_token("acme");
        for token in [&platform_token, &acme_token] {
            assert!(
                state
                    .config()
                    .resolve_principal(&Presented { credential: token })
                    .await
                    .expect("token resolves")
                    .is_some()
            );
        }

        let reloader = Reloader::new(file.path(), state.clone());
        file.rewrite(&format!(
            "{WITH_MINTED_NAMESPACES}\n[[gateway_token_epoch]]\nnamespace = \"platform\"\nmin_iat = {}\n",
            now
        ));
        reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("epoch candidate is valid");

        assert!(matches!(
            state
                .config()
                .resolve_principal(&Presented {
                    credential: &platform_token
                })
                .await,
            Err(crate::principals::PrincipalStoreError::Unauthorized(
                crate::principals::TokenVerificationError::IssuedBeforeEpoch { .. }
            ))
        ));
        assert!(
            state
                .config()
                .resolve_principal(&Presented {
                    credential: &acme_token
                })
                .await
                .expect("other namespace resolves")
                .is_some()
        );
        assert!(
            state
                .config()
                .resolve_principal(&Presented {
                    credential: "inbound-secret"
                })
                .await
                .expect("static key resolves")
                .is_some()
        );
    }

    #[tokio::test]
    async fn reload_summary_reports_gateway_verifier_kid_changes() {
        let file = ConfigFile::new(PLATFORM_ONLY);
        let state = state_from(&file);
        let reloader = Reloader::new(file.path(), state);
        file.rewrite(&format!(
            "{PLATFORM_ONLY}\n[gateway_token]\naudience = \"reload-test\"\n\n[[gateway_verifier]]\nkid = \"reload-kid\"\nalg = \"HS256\"\nenv = \"JWT_SECRET\"\nnamespaces = [\"platform\"]\nmax_ttl = \"15m\"\n"
        ));

        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("verifier candidate is valid");
        assert_eq!(summary.gateway_verifiers.added, vec!["reload-kid"]);
        assert!(summary.gateway_verifiers.removed.is_empty());

        file.rewrite(PLATFORM_ONLY);
        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("verifier removal is valid");
        assert!(summary.gateway_verifiers.added.is_empty());
        assert_eq!(summary.gateway_verifiers.removed, vec!["reload-kid"]);
    }

    #[tokio::test]
    async fn reload_summary_reports_gateway_verifier_definition_changes() {
        let file = ConfigFile::new(PLATFORM_ONLY);
        let state = state_from(&file);
        let reloader = Reloader::new(file.path(), state);
        let verifier = |audience: &str, max_ttl: &str| {
            format!(
                "{PLATFORM_ONLY}\n[gateway_token]\naudience = \"{audience}\"\n\n[[gateway_verifier]]\nkid = \"reload-kid\"\nalg = \"HS256\"\nenv = \"JWT_SECRET\"\nnamespaces = [\"platform\"]\nmax_ttl = \"{max_ttl}\"\n"
            )
        };

        file.rewrite(&verifier("reload-test", "15m"));
        reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("verifier candidate is valid");

        file.rewrite(&verifier("reload-test", "30m"));
        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("changed verifier candidate is valid");
        assert!(summary.gateway_verifiers.added.is_empty());
        assert!(summary.gateway_verifiers.removed.is_empty());
        assert_eq!(summary.gateway_verifiers.changed, vec!["reload-kid"]);
        assert!(summary.gateway_token_audience.is_empty());
        assert!(!summary.is_empty());
    }

    #[tokio::test]
    async fn reload_summary_reports_file_material_changes_and_new_fingerprint() {
        let material = ConfigFile::new("jwt-test-secret-012345678901234567890");
        let file = ConfigFile::new(&format!(
            "{PLATFORM_ONLY}\n[gateway_token]\naudience = \"reload-test\"\n\n[[gateway_verifier]]\nkid = \"reload-kid\"\nalg = \"HS256\"\nfile = \"{}\"\nnamespaces = [\"platform\"]\nmax_ttl = \"15m\"\n",
            material.path()
        ));
        let state = state_from(&file);
        let old_fingerprint = state.config().gateway_verifier_fingerprints["reload-kid"].clone();
        let reloader = Reloader::new(file.path(), state);
        material.rewrite("jwt-test-secret-012345678901234567891");
        file.rewrite(&format!(
            "{PLATFORM_ONLY}\n[gateway_token]\naudience = \"reload-test\"\n\n[[gateway_verifier]]\nkid = \"reload-kid\"\nalg = \"HS256\"\nfile = \"{}\"\nnamespaces = [\"platform\"]\nmax_ttl = \"30m\"\n",
            material.path()
        ));
        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("changed definition and file material are valid");
        assert_eq!(summary.gateway_verifiers.changed, vec!["reload-kid"]);
        assert_ne!(
            summary.gateway_verifier_fingerprints["reload-kid"],
            old_fingerprint
        );
        assert_eq!(
            summary.gateway_verifier_fingerprints["reload-kid"].len(),
            16
        );
    }

    /// A projected credential that rotates to another version of its secret has
    /// the same pool key, so only the reference it pins tells a reload that every
    /// call in that pool now authenticates with different material.
    #[test]
    fn a_projected_credentials_version_rotation_is_a_change() {
        let projected = |secret: crate::desired_state::SecretRef| {
            let mut config = Config::from_toml_str(PLATFORM_ONLY).expect("a valid config");
            config.credential = vec![crate::config::Credential {
                namespace: "platform".to_owned(),
                provider: "openai".to_owned(),
                env: None,
                secret: Some(secret),
                id: Some("platform-openai".to_owned()),
                weight: 1,
            }];
            config
        };
        let before = projected(crate::desired_state::fixtures::secret_ref_at(1, 1));
        let rotated = projected(crate::desired_state::fixtures::secret_ref_at(1, 2));

        assert_eq!(
            credential_version_changes(&before, &rotated),
            vec!["platform/openai/platform-openai".to_owned()]
        );
        assert!(credential_version_changes(&before, &before).is_empty());
    }

    #[derive(Serialize)]
    struct ReloadTokenClaims {
        exp: u64,
        iat: u64,
        jti: String,
        ns: String,
        sub: String,
        aud: String,
    }

    #[tokio::test]
    async fn invalid_verifier_file_reload_keeps_previous_snapshot_serving() {
        let material = ConfigFile::new("reload-secret-012345678901234567890");
        let file = ConfigFile::new(&format!(
            "{PLATFORM_ONLY}\n[gateway_token]\naudience = \"reload-test\"\n\n[[gateway_verifier]]\nkid = \"reload-kid\"\nalg = \"HS256\"\nfile = \"{}\"\nnamespaces = [\"platform\"]\nmax_ttl = \"15m\"\n",
            material.path()
        ));
        let state = state_from(&file);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_secs();
        let claims = ReloadTokenClaims {
            exp: now + 900,
            iat: now,
            jti: "reload-jti".to_owned(),
            ns: "platform".to_owned(),
            sub: "reload-subject".to_owned(),
            aud: "reload-test".to_owned(),
        };
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some("reload-kid".to_owned());
        let token = format!(
            "axt1.{}",
            encode(
                &header,
                &claims,
                &EncodingKey::from_secret(b"reload-secret-012345678901234567890"),
            )
            .expect("token signs")
        );
        assert!(
            state
                .config()
                .resolve_principal(&Presented { credential: &token })
                .await
                .expect("token resolves")
                .is_some()
        );
        let generation = state.config().generation;
        let reloader = Reloader::new(file.path(), state.clone());
        material.rewrite("");
        let error = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect_err("empty verifier material must reject candidate");
        assert!(error.to_string().contains(material.path()));
        assert_eq!(state.config().generation, generation);
        assert!(
            state
                .config()
                .resolve_principal(&Presented { credential: &token })
                .await
                .expect("previous token still resolves")
                .is_some()
        );
    }

    #[tokio::test]
    async fn reload_summary_reports_gateway_token_audience_changes() {
        let file = ConfigFile::new(PLATFORM_ONLY);
        let state = state_from(&file);
        let reloader = Reloader::new(file.path(), state);
        let config = |audience: &str| {
            format!(
                "{PLATFORM_ONLY}\n[gateway_token]\naudience = \"{audience}\"\n\n[[gateway_verifier]]\nkid = \"reload-kid\"\nalg = \"HS256\"\nenv = \"JWT_SECRET\"\nnamespaces = [\"platform\"]\nmax_ttl = \"15m\"\n"
            )
        };

        file.rewrite(&config("reload-test"));
        reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("verifier candidate is valid");

        file.rewrite(&config("new-audience"));
        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("audience change is valid");
        assert_eq!(summary.gateway_token_audience.added, vec!["new-audience"]);
        assert_eq!(summary.gateway_token_audience.removed, vec!["reload-test"]);
        assert!(summary.gateway_verifiers.is_empty());
        assert!(!summary.is_empty());
    }

    /// The reload reads the environment, so a key exported after boot resolves —
    /// and a declared credential with no key still fails the reload rather than
    /// the request.
    #[tokio::test]
    async fn credential_env_vars_are_read_at_reload_time() {
        let file = ConfigFile::new(PLATFORM_ONLY);
        let state = state_from(&file);
        let reloader = Reloader::new(file.path(), state.clone());

        file.rewrite(WITH_BYOK_TENANT);
        let err = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect_err("the tenant's key is not exported yet");
        assert!(matches!(
            err,
            ReloadError::Snapshot(SnapshotError::Credentials(_))
        ));
        assert_eq!(state.config().generation, 0);

        reloader
            .reload_with_env(TRIGGER_SIGNAL, &tenant_env())
            .expect("resolves once the key is exported");
        assert_eq!(state.config().generation, 1);
    }

    /// The config file says nothing about prices, so a reload has nothing to say
    /// about them either. Convergence publishes approved pricing onto the
    /// snapshot it compiles; a `SIGHUP` afterwards must not be the way a
    /// deployment silently stops being priced.
    #[tokio::test]
    async fn a_reload_does_not_unprice_a_converged_snapshot() {
        let file = ConfigFile::new(PLATFORM_ONLY);
        let state = state_from(&file);
        let pricing = crate::desired_state::fixtures::approved_pricing_snapshot();
        let converged = ConfigSnapshot::build(
            Config::load(file.path()).expect("valid boot config"),
            &inbound_env(),
            7,
        )
        .expect("the boot config compiles")
        .with_pricing(pricing.clone());
        state.publish(converged);

        // A real edit, so the candidate is published rather than skipped.
        file.rewrite(&format!(
            r#"{PLATFORM_ONLY}
[[model]]
name = "gpt-4o-mini"
targets = [{{ provider = "openai", model = "gpt-4o-mini", price = {{ input_microdollars_per_million = 150000, output_microdollars_per_million = 600000 }} }}]
"#
        ));
        Reloader::new(file.path(), state.clone())
            .reload_with_env(TRIGGER_WATCH, &inbound_env())
            .expect("the candidate is valid");

        let after = state.config();
        assert_eq!(after.generation, 8);
        assert_eq!(after.pricing(), Some(&pricing));
        assert_eq!(
            listed_aliases(&state).await,
            vec!["gpt-4o".to_string(), "gpt-4o-mini".to_string()]
        );
    }

    #[tokio::test]
    async fn an_invalid_candidate_is_rejected_and_the_previous_config_keeps_serving() {
        let file = ConfigFile::new(PLATFORM_ONLY);
        let state = state_from(&file);
        let reloader = Reloader::new(file.path(), state.clone());
        let before = state.config();

        // Two default namespaces: the same violation that refuses to boot.
        file.rewrite(
            r#"
[[namespace]]
id = "platform"
default = true

[[namespace]]
id = "acme"
default = true
"#,
        );
        let err = reloader
            .reload_with_env(TRIGGER_WATCH, &inbound_env())
            .expect_err("candidate is invalid");

        assert!(matches!(err, ReloadError::Config(ConfigError::Invalid(_))));
        assert!(Arc::ptr_eq(&before, &state.config()));
        assert_eq!(listed_aliases(&state).await, vec!["gpt-4o".to_string()]);
    }

    #[tokio::test]
    async fn a_cross_wire_alias_is_rejected_and_the_previous_config_keeps_serving() {
        let file = ConfigFile::new(PLATFORM_ONLY);
        let state = state_from(&file);
        let reloader = Reloader::new(file.path(), state.clone());
        let before = state.config();

        file.rewrite(
            &format!(
                r#"{PLATFORM_ONLY}
[[provider]]
id = "anthropic"
kind = "anthropic"
base_url = "https://api.anthropic.com/v1"

[[model]]
name = "mixed"
targets = [
    {{ provider = "openai", model = "gpt-4o", price = {{ input_microdollars_per_million = 1, output_microdollars_per_million = 1 }} }},
    {{ provider = "anthropic", model = "claude", price = {{ input_microdollars_per_million = 1, output_microdollars_per_million = 1 }} }},
]
"#
            ),
        );
        let err = reloader
            .reload_with_env(TRIGGER_WATCH, &inbound_env())
            .expect_err("cross-wire candidate must be rejected");
        let message = err.to_string();
        assert!(message.contains("mixed"), "{message}");
        assert!(message.contains("no route can serve"), "{message}");
        assert!(Arc::ptr_eq(&before, &state.config()));
        assert_eq!(state.config().generation, 0);
    }

    /// Reload runs the same fail-closed validation boot does, so a candidate
    /// whose gateway key cannot be resolved never replaces a config that can be
    /// authenticated against (ADR 0013).
    #[tokio::test]
    async fn a_candidate_with_an_unresolvable_gateway_key_is_rejected_and_kept() {
        let file = ConfigFile::new(PLATFORM_ONLY);
        let state = state_from(&file);
        let reloader = Reloader::new(file.path(), state.clone());
        let before = state.config();

        file.rewrite(&PLATFORM_ONLY.replace(INBOUND_KEY_ENV, "AXOND_ROTATED_KEY"));
        let err = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect_err("the rotated key is not exported");

        assert!(
            matches!(
                err,
                ReloadError::Snapshot(SnapshotError::MissingGatewayKey { ref env, .. })
                    if env == "AXOND_ROTATED_KEY"
            ),
            "{err}"
        );
        // The running config still serves, still with its own key table.
        assert!(Arc::ptr_eq(&before, &state.config()));
        assert_eq!(state.config().generation, 0);
        assert!(
            state
                .config()
                .resolve_principal(&Presented {
                    credential: "inbound-secret",
                })
                .await
                .expect("principal resolution succeeds")
                .is_some()
        );

        // Exporting the rotated key is all the candidate was waiting for.
        let mut rotated = inbound_env();
        rotated.insert(
            "AXOND_ROTATED_KEY".to_string(),
            "rotated-secret".to_string(),
        );
        reloader
            .reload_with_env(TRIGGER_SIGNAL, &rotated)
            .expect("resolves once the key is exported");
        let after = state.config();
        assert_eq!(after.generation, 1);
        assert!(
            after
                .resolve_principal(&Presented {
                    credential: "rotated-secret",
                })
                .await
                .expect("principal resolution succeeds")
                .is_some()
        );
        assert!(
            after
                .resolve_principal(&Presented {
                    credential: "inbound-secret",
                })
                .await
                .expect("principal resolution succeeds")
                .is_none()
        );
    }

    /// A request holds its snapshot for its whole life, so a reload that lands
    /// mid-flight cannot move the alias out from under it.
    #[tokio::test]
    async fn an_in_flight_snapshot_is_unaffected_by_a_reload() {
        let file = ConfigFile::new(WITH_BYOK_TENANT);
        let config = Config::load(file.path()).expect("valid boot config");
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(StdoutSink)];
        let state = AppState::new(
            config,
            &tenant_env(),
            UsageFanout::new(sinks),
            Box::new(NoBudget),
        )
        .expect("boot state");
        let reloader = Reloader::new(file.path(), state.clone());

        let in_flight = state.config();
        file.rewrite(PLATFORM_ONLY);
        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("candidate is valid");

        assert_eq!(summary.models.removed, vec!["acme-fast".to_string()]);
        assert!(in_flight.config.model("acme-fast").is_some());
        assert!(
            in_flight
                .credentials
                .plan(&in_flight.config, "acme", "openai")
                .is_some()
        );
        assert!(state.config().config.model("acme-fast").is_none());
    }

    /// The warning names what the *process* is doing, so it stays true for as
    /// long as the file disagrees with the socket that is actually bound.
    #[tokio::test]
    async fn process_level_changes_are_reported_on_every_reload_rather_than_applied() {
        let file = ConfigFile::new(PLATFORM_ONLY);
        let state = state_from(&file);
        let reloader = Reloader::new(file.path(), state.clone());

        file.rewrite(&format!(
            "[server]\nbind = \"127.0.0.1:9999\"\n{PLATFORM_ONLY}"
        ));
        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("candidate is valid");
        assert!(summary.bind_changed);
        assert!(summary.is_empty());

        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("candidate is valid");
        assert!(summary.bind_changed);
        assert_eq!(state.config().generation, 2);
    }

    #[tokio::test]
    async fn budget_changes_are_reported_as_restart_required() {
        let file = ConfigFile::new(PLATFORM_ONLY);
        let state = state_from(&file);
        let reloader = Reloader::new(file.path(), state);

        file.rewrite(&format!(
            "{PLATFORM_ONLY}\n[budget]\nbackend = \"in-memory\"\nlimit_microdollars = 1_000\nreservation_ttl_seconds = 60\nidle_ttl_seconds = 120\nmax_subjects = 32\n"
        ));
        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("budget candidate is valid");
        assert!(summary.budget_changed);
        assert!(summary.is_empty());

        file.rewrite(PLATFORM_ONLY);
        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("budget removal is valid");
        assert!(!summary.budget_changed);
    }

    /// Turning billing-grade usage recording on or off is a restart, because the
    /// outbox connection and its delivery worker are built once. An operator who
    /// edits the section and reloads has to be told that, or they will believe a
    /// serving process is journaling when it is not.
    #[tokio::test]
    async fn usage_journal_changes_are_reported_as_restart_required() {
        let file = ConfigFile::new(PLATFORM_ONLY);
        let state = state_from(&file);
        let reloader = Reloader::new(file.path(), state);

        file.rewrite(&format!(
            "{PLATFORM_ONLY}\n[[usage_sink]]\nkind = \"stdout\"\n\n[usage_journal]\nbackend = \"postgres\"\ndsn_env = \"AXOND_USAGE_OUTBOX_DSN\"\n"
        ));
        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("journal candidate is valid");
        assert!(summary.usage_journal_changed);
        assert!(summary.usage_sinks_changed);
        assert!(summary.is_empty(), "neither is applied by a reload");

        file.rewrite(PLATFORM_ONLY);
        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("journal removal is valid");
        assert!(!summary.usage_journal_changed);
    }

    #[tokio::test]
    async fn rate_limit_changes_are_reported_as_restart_required() {
        let file = ConfigFile::new(PLATFORM_ONLY);
        let state = state_from(&file);
        let reloader = Reloader::new(file.path(), state);

        file.rewrite(&format!(
            "{PLATFORM_ONLY}\n[rate_limit]\nbackend = \"in-memory\"\nmax_in_flight_per_subject = 3\nmax_subjects = 32\n"
        ));
        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("rate limit candidate is valid");
        assert!(summary.rate_limit_changed);
        assert!(summary.is_empty());

        file.rewrite(PLATFORM_ONLY);
        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("rate limit removal is valid");
        assert!(!summary.rate_limit_changed);
    }

    #[tokio::test]
    async fn revocation_changes_are_reported_as_restart_required() {
        let file = ConfigFile::new(PLATFORM_ONLY);
        let state = state_from(&file);
        let reloader = Reloader::new(file.path(), state);

        file.rewrite(&format!(
            "{PLATFORM_ONLY}\n[revocation]\nbackend = \"redis\"\ndsn_env = \"REDIS_URL\"\n"
        ));
        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &{
                let mut env = inbound_env();
                env.insert("REDIS_URL".to_owned(), "redis://127.0.0.1:6399".to_owned());
                env
            })
            .expect("revocation candidate is valid");
        assert!(summary.revocation_changed);
        assert!(summary.is_empty());

        file.rewrite(PLATFORM_ONLY);
        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect("revocation removal is valid");
        assert!(!summary.revocation_changed);
    }

    /// Both triggers share one view of what has been acted on, so the watcher
    /// does not re-apply the edit an operator signalled.
    #[tokio::test]
    async fn the_watcher_does_not_repeat_a_reload_the_signal_already_applied() {
        let file = ConfigFile::new(PLATFORM_ONLY);
        let state = state_from(&file);
        let reloader = Reloader::new(file.path(), state.clone());

        file.rewrite(WITH_BYOK_TENANT);
        reloader
            .reload_with_env(TRIGGER_SIGNAL, &tenant_env())
            .expect("candidate is valid");
        assert_eq!(state.config().generation, 1);

        assert!(
            reloader
                .reload_if_changed_with_env(TRIGGER_WATCH, &tenant_env())
                .is_none()
        );
        assert_eq!(state.config().generation, 1);

        file.rewrite(PLATFORM_ONLY);
        reloader
            .reload_if_changed_with_env(TRIGGER_WATCH, &inbound_env())
            .expect("the file changed")
            .expect("candidate is valid");
        assert_eq!(state.config().generation, 2);
    }

    #[tokio::test]
    async fn a_missing_config_file_is_rejected() {
        let file = ConfigFile::new(PLATFORM_ONLY);
        let state = state_from(&file);
        let reloader = Reloader::new("/nonexistent/axond.toml", state.clone());

        let err = reloader
            .reload_with_env(TRIGGER_SIGNAL, &inbound_env())
            .expect_err("no file, no candidate");
        assert!(matches!(err, ReloadError::Config(_)));
        assert_eq!(state.config().generation, 0);
    }
}
