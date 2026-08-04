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
//! The listening socket is already bound and the usage sinks already own
//! connections and flush tasks, so changes to `[server] bind` and
//! `[[usage_sink]]` are reported and ignored until the next restart.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use crate::config::{Config, ConfigError, Reload, UsageSinkConfig};
use crate::credentials::CredentialError;
use crate::state::{AppState, ConfigSnapshot};
use crate::telemetry;

/// A reload asked for by `SIGHUP`.
pub const TRIGGER_SIGNAL: &str = "sighup";
/// A reload the file watcher noticed.
pub const TRIGGER_WATCH: &str = "watch";

#[derive(Debug, thiserror::Error)]
pub enum ReloadError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("credential validation failed: {0}")]
    Credentials(#[from] CredentialError),
}

/// What the process committed to at startup and cannot redo while serving, so a
/// candidate is compared against what is *in effect* rather than against the
/// previous candidate.
struct Boot {
    bind: SocketAddr,
    usage_sink: Vec<UsageSinkConfig>,
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
                bind: booted.config.server.bind,
                usage_sink: booted.config.usage_sink.clone(),
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

        match self.candidate(env, current.generation + 1) {
            Ok(candidate) => {
                let summary =
                    ReloadSummary::between(&self.boot, &current.config, &candidate.config);
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
    fn candidate(
        &self,
        env: &HashMap<String, String>,
        generation: u64,
    ) -> Result<ConfigSnapshot, ReloadError> {
        let config = Config::load(&self.path)?;
        Ok(ConfigSnapshot::build(config, env, generation)?)
    }

    fn watch_settings(&self) -> Reload {
        self.state.config().config.reload.clone()
    }
}

/// What one reload changed, for the log line an operator reads afterwards.
/// Identifiers only — env-var *names* are references, never secrets.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReloadSummary {
    pub namespaces: Delta,
    pub providers: Delta,
    pub models: Delta,
    pub credentials: Delta,
    pub gateway_keys: Delta,
    /// `[server] bind` differs from what the process bound at startup.
    pub bind_changed: bool,
    /// `[[usage_sink]]` differs from the connected sinks.
    pub usage_sinks_changed: bool,
}

/// The added and removed identifiers of one config collection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Delta {
    pub added: Vec<String>,
    pub removed: Vec<String>,
}

impl Delta {
    fn between(before: impl Iterator<Item = String>, after: impl Iterator<Item = String>) -> Self {
        let before: BTreeSet<String> = before.collect();
        let after: BTreeSet<String> = after.collect();
        Self {
            added: after.difference(&before).cloned().collect(),
            removed: before.difference(&after).cloned().collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
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
        )
    }
}

impl ReloadSummary {
    fn between(boot: &Boot, before: &Config, after: &Config) -> Self {
        Self {
            namespaces: Delta::between(
                before.namespace.iter().map(|n| n.id.clone()),
                after.namespace.iter().map(|n| n.id.clone()),
            ),
            providers: Delta::between(
                before.provider.iter().map(|p| p.id.clone()),
                after.provider.iter().map(|p| p.id.clone()),
            ),
            models: Delta::between(
                before.model.iter().map(|m| m.name.clone()),
                after.model.iter().map(|m| m.name.clone()),
            ),
            credentials: Delta::between(
                before.credential.iter().map(credential_key),
                after.credential.iter().map(credential_key),
            ),
            gateway_keys: Delta::between(
                before.gateway_key.iter().map(|k| k.env.clone()),
                after.gateway_key.iter().map(|k| k.env.clone()),
            ),
            bind_changed: boot.bind != after.server.bind,
            usage_sinks_changed: boot.usage_sink != after.usage_sink,
        }
    }

    /// Whether anything a reload can actually apply differs.
    pub fn is_empty(&self) -> bool {
        self.namespaces.is_empty()
            && self.providers.is_empty()
            && self.models.is_empty()
            && self.credentials.is_empty()
            && self.gateway_keys.is_empty()
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
    }
}

/// `namespace/provider/label` — the pool member a credential entry declares.
fn credential_key(c: &crate::config::Credential) -> String {
    format!("{}/{}/{}", c.namespace, c.provider, c.label())
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
    use crate::routes;
    use crate::usage::{StdoutSink, UsageFanout, UsageSink};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use serde_json::Value;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
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

    const PLATFORM_ONLY: &str = r#"
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

    fn state_from(file: &ConfigFile) -> AppState {
        let config = Config::load(file.path()).expect("valid boot config");
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(StdoutSink)];
        AppState::new(
            config,
            &HashMap::new(),
            UsageFanout::new(sinks),
            Box::new(NoBudget),
        )
        .expect("boot state")
    }

    fn tenant_env() -> HashMap<String, String> {
        [("ACME_OPENAI_KEY".to_string(), "sk-acme".to_string())]
            .into_iter()
            .collect()
    }

    async fn listed_aliases(state: &AppState) -> Vec<String> {
        let resp = routes::router(state.clone())
            .oneshot(Request::get("/v1/models").body(Body::empty()).unwrap())
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
            .reload_with_env(TRIGGER_SIGNAL, &HashMap::new())
            .expect_err("the tenant's key is not exported yet");
        assert!(matches!(err, ReloadError::Credentials(_)));
        assert_eq!(state.config().generation, 0);

        reloader
            .reload_with_env(TRIGGER_SIGNAL, &tenant_env())
            .expect("resolves once the key is exported");
        assert_eq!(state.config().generation, 1);
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
            .reload_with_env(TRIGGER_WATCH, &HashMap::new())
            .expect_err("candidate is invalid");

        assert!(matches!(err, ReloadError::Config(ConfigError::Invalid(_))));
        assert!(Arc::ptr_eq(&before, &state.config()));
        assert_eq!(listed_aliases(&state).await, vec!["gpt-4o".to_string()]);
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
            .reload_with_env(TRIGGER_SIGNAL, &HashMap::new())
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
            .reload_with_env(TRIGGER_SIGNAL, &HashMap::new())
            .expect("candidate is valid");
        assert!(summary.bind_changed);
        assert!(summary.is_empty());

        let summary = reloader
            .reload_with_env(TRIGGER_SIGNAL, &HashMap::new())
            .expect("candidate is valid");
        assert!(summary.bind_changed);
        assert_eq!(state.config().generation, 2);
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
            .reload_if_changed_with_env(TRIGGER_WATCH, &HashMap::new())
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
            .reload_with_env(TRIGGER_SIGNAL, &HashMap::new())
            .expect_err("no file, no candidate");
        assert!(matches!(err, ReloadError::Config(_)));
        assert_eq!(state.config().generation, 0);
    }
}
