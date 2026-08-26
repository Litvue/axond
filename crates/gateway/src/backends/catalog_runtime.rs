//! The catalogue import as a running thing: what boot builds, what the
//! background task drives, and what an operator can read about it.
//!
//! Everything here is off the request path, and structurally rather than by
//! convention: the source, the store, and the [`CatalogRefresher`] that owns them
//! are moved into one spawned task, and the only handle the rest of the process
//! keeps is a [`CatalogStatus`] — a mutex over a bounded report and a channel
//! that asks for a refresh. A request cannot reach models.dev through it because
//! it holds nothing that can fetch, and it cannot reach the catalogue store
//! because it holds nothing that can query.
//!
//! The pieces below are the ones an operator configures rather than a second
//! architecture: [`RuntimeSource`] and [`RuntimeStore`] are closed enumerations of
//! what `[catalog]` may select, so a deployment cannot be handed a source or a
//! store this build did not ship, and a stateful deployment cannot be handed the
//! in-memory one at all ([`crate::config::Config`] validation refuses it — a
//! catalogue that disappears on restart is not a catalogue an operator can
//! approve prices against).
//!
//! What the loop does *not* do is as deliberate as what it does. An import is an
//! observation: it never enables a model, never moves a price, and never changes
//! what a request routes to. It retains content, moves an active pointer, and
//! reports what a human should look at.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

use super::Capabilities;
use super::catalog::{
    Admission, CatalogContentId, CatalogError, CatalogRefresh, CatalogReport, CatalogSource,
    RefusalReason, SourceValidators,
};
use super::catalog_refresh::{
    CatalogRefresher, InvalidSchedule, RefreshOutcome, RefreshTrigger, Restored,
};
use super::catalog_store::postgres::PostgresCatalogStore;
use super::catalog_store::{
    CatalogStore, CatalogStoreError, InMemoryCatalogStore, RetainedCatalog, Retention,
    StoredCatalogState,
};
use super::models_dev::{HttpCatalogFetch, ModelsDevAdapter, ModelsDevSource, SeedCatalogSource};
use crate::config::{CatalogConfig, CatalogSourceBackend, CatalogStoreBackend};

/// What is operationally true about the catalogue, readable without touching a
/// backend.
///
/// A mutex over a `Copy` report, written once per refresh by the task that owns
/// the refresher and read by the authenticated status view. The lock is never
/// held across an `await`, so a status read cannot be delayed by an import that
/// is waiting on an upstream — the two never contend for anything but a memory
/// write.
#[derive(Debug, Default)]
pub struct CatalogStatus {
    report: Mutex<Option<CatalogReport>>,
}

impl CatalogStatus {
    pub fn new() -> Self {
        Self::default()
    }

    /// The last report, aged to now.
    ///
    /// Age is recomputed rather than replayed: a report published an hour ago
    /// described content that was an hour younger then, and a surface that
    /// answered with the stored number would hide exactly the staleness it
    /// exists to show.
    pub fn report(&self) -> Option<CatalogReport> {
        let mut report = (*self.report.lock().expect("catalogue status lock"))?;
        let now = SystemTime::now();
        if let Some(active) = report.active.as_mut() {
            active.age = now
                .duration_since(active.fetched_at)
                .unwrap_or(Duration::ZERO);
        }
        Some(report)
    }

    fn publish(&self, report: CatalogReport) {
        *self.report.lock().expect("catalogue status lock") = Some(report);
    }
}

/// Every source `[catalog]` can select, as one type.
///
/// An enumeration rather than a trait object because the set is closed by
/// design: the boundary a source implements is [`CatalogSource`], and which
/// implementations exist is a property of the build, not of a config file.
#[derive(Debug)]
pub enum RuntimeSource {
    /// models.dev over HTTPS, conditionally.
    ModelsDev(ModelsDevSource<HttpCatalogFetch>),
    /// The bundled excerpt, which reaches no network at all.
    Seed(SeedCatalogSource),
}

#[async_trait]
impl CatalogSource for RuntimeSource {
    fn name(&self) -> &'static str {
        match self {
            Self::ModelsDev(source) => source.name(),
            Self::Seed(source) => source.name(),
        }
    }

    fn capabilities(&self) -> Capabilities {
        match self {
            Self::ModelsDev(source) => source.capabilities(),
            Self::Seed(source) => source.capabilities(),
        }
    }

    async fn refresh(
        &self,
        since: Option<&SourceValidators>,
    ) -> Result<CatalogRefresh, CatalogError> {
        match self {
            Self::ModelsDev(source) => source.refresh(since).await,
            Self::Seed(source) => source.refresh(since).await,
        }
    }
}

/// Every store `[catalog]` can select, as one type.
#[derive(Debug)]
pub enum RuntimeStore {
    /// Durable retention, keyed by content identity.
    Postgres(Box<PostgresCatalogStore>),
    /// Process memory: a single-replica development store, refused in a stateful
    /// deployment.
    InMemory(InMemoryCatalogStore),
}

#[async_trait]
impl CatalogStore for RuntimeStore {
    fn name(&self) -> &'static str {
        match self {
            Self::Postgres(store) => store.name(),
            Self::InMemory(store) => store.name(),
        }
    }

    fn capabilities(&self) -> Capabilities {
        match self {
            Self::Postgres(store) => store.capabilities(),
            Self::InMemory(store) => store.capabilities(),
        }
    }

    async fn load(&self) -> Result<StoredCatalogState, CatalogStoreError> {
        match self {
            Self::Postgres(store) => store.load().await,
            Self::InMemory(store) => store.load().await,
        }
    }

    async fn retained(
        &self,
        content_id: CatalogContentId,
    ) -> Result<Option<RetainedCatalog>, CatalogStoreError> {
        match self {
            Self::Postgres(store) => store.retained(content_id).await,
            Self::InMemory(store) => store.retained(content_id).await,
        }
    }

    async fn retained_by_raw_digest(
        &self,
        digest: crate::desired_state::Checksum,
    ) -> Result<Option<RetainedCatalog>, CatalogStoreError> {
        match self {
            Self::Postgres(store) => store.retained_by_raw_digest(digest).await,
            Self::InMemory(store) => store.retained_by_raw_digest(digest).await,
        }
    }

    async fn retain(&self, import: &RetainedCatalog) -> Result<Retention, CatalogStoreError> {
        match self {
            Self::Postgres(store) => store.retain(import).await,
            Self::InMemory(store) => store.retain(import).await,
        }
    }

    async fn activate(
        &self,
        import: &RetainedCatalog,
        activated_at: SystemTime,
    ) -> Result<Retention, CatalogStoreError> {
        match self {
            Self::Postgres(store) => store.activate(import, activated_at).await,
            Self::InMemory(store) => store.activate(import, activated_at).await,
        }
    }

    async fn confirm(
        &self,
        content_id: CatalogContentId,
        validators: &SourceValidators,
        confirmed_at: SystemTime,
    ) -> Result<bool, CatalogStoreError> {
        match self {
            Self::Postgres(store) => store.confirm(content_id, validators, confirmed_at).await,
            Self::InMemory(store) => store.confirm(content_id, validators, confirmed_at).await,
        }
    }

    async fn refuse(
        &self,
        reason: RefusalReason,
        refused_at: SystemTime,
    ) -> Result<(), CatalogStoreError> {
        match self {
            Self::Postgres(store) => store.refuse(reason, refused_at).await,
            Self::InMemory(store) => store.refuse(reason, refused_at).await,
        }
    }
}

/// Why a catalogue import could not be brought up at boot.
///
/// Distinct from a *refused refresh*, which is an expected operational event the
/// running deployment absorbs. These are the ones a process cannot start
/// holding: a schedule that does not describe a loop, a DSN reference nothing
/// resolves, a store that cannot be reached before the listener binds.
#[derive(Debug, thiserror::Error)]
pub enum CatalogBootError {
    #[error("the catalogue refresh schedule is unusable: {0}")]
    Schedule(#[from] InvalidSchedule),
    #[error("`{0}` is not a supported models.dev document")]
    Source(String),
    #[error(
        "catalogue retention names `{name}`, which holds no connection string: the DSN stays in \
         the environment and is never written to the config"
    )]
    MissingDsn { name: String },
    #[error("the catalogue store could not be opened: {0}")]
    Store(#[from] CatalogStoreError),
    #[error("the HTTP client for catalogue imports could not be built: {0}")]
    Client(String),
}

/// A refresh an operator asked for, and where to send what it did.
struct ManualRefresh {
    answer: oneshot::Sender<RefreshOutcome>,
}

/// The handle boot keeps: what to read, and how to ask for an import now.
///
/// Deliberately not the refresher. Two callers driving one refresher would race
/// over the active pointer for no benefit, so the refresher stays owned by its
/// task and this is a channel to it — which also means a manual refresh and a
/// scheduled one take the same code path, and the only difference between them
/// stays the one that is real: a manual refresh is never skipped for not being
/// due.
#[derive(Debug, Clone)]
pub struct CatalogHandle {
    status: Arc<CatalogStatus>,
    refresh: mpsc::Sender<ManualRefresh>,
    store: Arc<RuntimeStore>,
}

impl std::fmt::Debug for ManualRefresh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManualRefresh").finish_non_exhaustive()
    }
}

impl CatalogHandle {
    /// What the status surface reads.
    pub fn status(&self) -> &Arc<CatalogStatus> {
        &self.status
    }

    /// The durable catalogue reader used by convergence only. It can hydrate
    /// retained payloads, but it is never placed in request state.
    pub fn store(&self) -> Arc<dyn CatalogStore> {
        self.store.clone()
    }

    /// Ask for an import now and wait for what it did.
    ///
    /// `None` when the task is gone — a deployment that is shutting down, which
    /// is not a refusal and must not be reported as one.
    pub async fn refresh_now(&self) -> Option<RefreshOutcome> {
        let (answer, wait) = oneshot::channel();
        self.refresh.send(ManualRefresh { answer }).await.ok()?;
        wait.await.ok()
    }
}

/// Build the source `[catalog]` selected.
fn source(config: &CatalogConfig) -> Result<RuntimeSource, CatalogBootError> {
    match config.source {
        CatalogSourceBackend::None => unreachable!("a disabled catalogue builds no source"),
        CatalogSourceBackend::Seed => Ok(RuntimeSource::Seed(SeedCatalogSource)),
        CatalogSourceBackend::ModelsDev => {
            let adapter = ModelsDevAdapter::new(config.url())
                .map_err(|_| CatalogBootError::Source(config.url().to_owned()))?;
            let fetch = HttpCatalogFetch::new(Duration::from_secs(config.refresh_timeout_seconds))
                .map_err(|error| CatalogBootError::Client(error.to_string()))?
                .holding_at_most(config.max_payload_bytes);
            Ok(RuntimeSource::ModelsDev(
                ModelsDevSource::new(adapter, fetch).with_payload_limit(config.max_payload_bytes),
            ))
        }
    }
}

/// Open the store `[catalog]` selected, resolving its DSN by name.
///
/// The DSN is read out of the environment here and never held anywhere else: it
/// carries a password, so it reaches [`PostgresCatalogStore::connect`] and stops
/// there.
async fn store(
    config: &CatalogConfig,
    control_plane_dsn_env: Option<&str>,
    env: &std::collections::HashMap<String, String>,
    allow_unavailable: bool,
) -> Result<RuntimeStore, CatalogBootError> {
    match config.store {
        CatalogStoreBackend::InMemory => Ok(RuntimeStore::InMemory(InMemoryCatalogStore::new())),
        CatalogStoreBackend::Postgres => {
            let name = config
                .dsn_env
                .as_deref()
                .or(control_plane_dsn_env)
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .ok_or_else(|| CatalogBootError::MissingDsn {
                    name: "catalog.dsn_env".to_owned(),
                })?;
            let dsn = env
                .get(name)
                .map(String::as_str)
                .map(str::trim)
                .filter(|dsn| !dsn.is_empty())
                .ok_or_else(|| CatalogBootError::MissingDsn {
                    name: name.to_owned(),
                })?;
            let settings = config.store_settings();
            let store = if allow_unavailable {
                PostgresCatalogStore::connect_or_defer(dsn, settings).await?
            } else {
                PostgresCatalogStore::connect(dsn, settings).await?
            };
            Ok(RuntimeStore::Postgres(Box::new(store)))
        }
    }
}

/// Bring the catalogue import up: build the pieces, adopt what is already
/// retained, and spawn the loop that keeps it current.
///
/// `Ok(None)` for a deployment that imports nothing, which is the default. No
/// client is built, no connection is opened, and no task is spawned, so the
/// inert configuration costs a branch.
///
/// Restoration failing does *not* fail boot. A store that cannot be read, or a
/// retained catalogue this build no longer reproduces, is a refusal the running
/// deployment reports and retries against — the failure mode to avoid is a fleet
/// that will not start because a *metadata* import is unhappy, while every
/// request it would have served needs nothing from that metadata.
pub async fn start(
    config: &CatalogConfig,
    control_plane_dsn_env: Option<&str>,
    env: &std::collections::HashMap<String, String>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<Option<CatalogHandle>, CatalogBootError> {
    start_with_recovery(config, control_plane_dsn_env, env, shutdown, false).await
}

/// Start catalogue refresh with a permitted initial backend outage. The store
/// remains retryable and the serving snapshot is supplied by the compiled
/// cache; a catalogue import never becomes an inference dependency.
pub async fn start_allow_unavailable(
    config: &CatalogConfig,
    control_plane_dsn_env: Option<&str>,
    env: &std::collections::HashMap<String, String>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<Option<CatalogHandle>, CatalogBootError> {
    start_with_recovery(config, control_plane_dsn_env, env, shutdown, true).await
}

async fn start_with_recovery(
    config: &CatalogConfig,
    control_plane_dsn_env: Option<&str>,
    env: &std::collections::HashMap<String, String>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
    allow_unavailable: bool,
) -> Result<Option<CatalogHandle>, CatalogBootError> {
    if !config.enabled() {
        return Ok(None);
    }
    let source = source(config)?;
    let store = Arc::new(store(config, control_plane_dsn_env, env, allow_unavailable).await?);
    let source_name = source.name();
    let store_name = store.name();
    let mut refresher = CatalogRefresher::new(
        source,
        Arc::clone(&store),
        config.schedule(),
        config.bootstrap_mode(),
        SystemTime::now(),
    )?;
    let status = Arc::new(CatalogStatus::new());
    match refresher.restore(SystemTime::now()).await {
        Ok(Restored::Stored {
            content_id,
            confirmed_at,
        }) => tracing::info!(
            source = source_name,
            store = store_name,
            content = %content_id.short(),
            age_s = SystemTime::now()
                .duration_since(confirmed_at)
                .unwrap_or(Duration::ZERO)
                .as_secs(),
            "catalogue restored",
        ),
        Ok(Restored::Seeded { content_id }) => tracing::info!(
            source = source_name,
            store = store_name,
            content = %content_id.short(),
            "catalogue seeded; the first refresh transfers the upstream document",
        ),
        Ok(Restored::Empty) => tracing::info!(
            source = source_name,
            store = store_name,
            "no catalogue retained yet; the first refresh imports one",
        ),
        Err(error) => tracing::warn!(
            source = source_name,
            store = store_name,
            %error,
            "the retained catalogue could not be adopted; the deployment reports it as refused \
             and keeps refreshing",
        ),
    }
    status.publish(refresher.report(SystemTime::now()));
    let (sender, receiver) = mpsc::channel(1);
    tokio::spawn(run(refresher, Arc::clone(&status), receiver, shutdown));
    Ok(Some(CatalogHandle {
        status,
        refresh: sender,
        store,
    }))
}

/// The loop: shut down, refresh because someone asked, or refresh because the
/// interval elapsed.
///
/// Biased toward shutdown for the reason every loop in this codebase is: a
/// terminating process must not start a fresh upstream request it will then
/// abandon, and a catalogue import is the least urgent work in the deployment.
///
/// The sleep is computed from what the refresher says is due rather than from a
/// fixed interval, so backoff after a refusal and the ordinary cadence are the
/// same mechanism, and a refresh that took time does not shorten the next wait.
async fn run(
    mut refresher: CatalogRefresher<RuntimeSource, Arc<RuntimeStore>>,
    status: Arc<CatalogStatus>,
    mut manual: mpsc::Receiver<ManualRefresh>,
    shutdown: impl std::future::Future<Output = ()> + Send,
) {
    let mut shutdown = std::pin::pin!(shutdown);
    // Whether anything can still ask for an import. Once every handle is gone the
    // arm is disabled rather than re-armed: a closed receiver is *ready*
    // forever, and a ready arm ahead of the sleep under `biased` would be a spin
    // rather than the schedule this keeps serving.
    let mut askable = true;
    loop {
        let now = SystemTime::now();
        let delay = refresher
            .next_due()
            .duration_since(now)
            .unwrap_or(Duration::ZERO);
        tokio::select! {
            biased;
            () = &mut shutdown => {
                tracing::debug!("catalogue refresh stopped");
                return;
            }
            asked = manual.recv(), if askable => {
                let Some(ManualRefresh { answer }) = asked else {
                    // Every handle is gone, so nothing can ask again. The
                    // schedule is still worth keeping: the deployment is serving.
                    tracing::debug!("no catalogue handle remains; refreshing on schedule only");
                    askable = false;
                    continue;
                };
                let outcome = refresh(&mut refresher, &status, RefreshTrigger::Manual).await;
                let _ = answer.send(outcome);
            }
            () = tokio::time::sleep(delay) => {
                refresh(&mut refresher, &status, RefreshTrigger::Scheduled).await;
            }
        }
    }
}

/// One import, published to the status surface whatever it did.
///
/// A refusal is published for the same reason an admission is: the report is how
/// an operator learns the catalogue stopped advancing, and a surface that only
/// updated on success would answer most confidently exactly when it was most
/// wrong.
async fn refresh(
    refresher: &mut CatalogRefresher<RuntimeSource, Arc<RuntimeStore>>,
    status: &CatalogStatus,
    trigger: RefreshTrigger,
) -> RefreshOutcome {
    let now = SystemTime::now();
    let outcome = refresher.refresh(trigger, now).await;
    status.publish(refresher.report(SystemTime::now()));
    match &outcome {
        RefreshOutcome::Admitted {
            admission,
            retention,
            ..
        } => tracing::info!(
            trigger = trigger_name(trigger),
            content = %admission.content_id().short(),
            change = admitted_change(admission),
            retained = retention.is_some(),
            "catalogue import admitted",
        ),
        RefreshOutcome::Refused {
            refusal, retry_in, ..
        } => tracing::warn!(
            trigger = trigger_name(trigger),
            reason = refusal.reason().as_str(),
            retry_in_s = retry_in.as_secs(),
            "catalogue import refused; the active catalogue is unchanged",
        ),
        RefreshOutcome::NotDue { .. } => {}
    }
    outcome
}

/// What an admission did to the catalogue, in one bounded word.
///
/// The diff itself is not logged: it names every model that moved, which is an
/// unbounded line for a document with thousands of them. What changed is a
/// question the retained snapshots answer exactly.
const fn admitted_change(admission: &Admission) -> &'static str {
    match admission {
        Admission::Unchanged { .. } => "unchanged",
        Admission::Updated { .. } => "updated",
        Admission::Initial { .. } => "initial",
    }
}

const fn trigger_name(trigger: RefreshTrigger) -> &'static str {
    match trigger {
        RefreshTrigger::Scheduled => "scheduled",
        RefreshTrigger::Manual => "manual",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CatalogBootstrap;

    /// A seed source and a development store: an import that reaches no network
    /// at all, which is what makes these tests exercise the runtime rather than
    /// models.dev.
    fn offline() -> CatalogConfig {
        CatalogConfig {
            source: CatalogSourceBackend::Seed,
            store: CatalogStoreBackend::InMemory,
            bootstrap: CatalogBootstrap::Seed,
            ..CatalogConfig::default()
        }
    }

    fn no_env() -> std::collections::HashMap<String, String> {
        std::collections::HashMap::new()
    }

    /// The default costs a branch: no client, no connection, no task, and
    /// nothing for the status surface to report. A deployment that did not ask
    /// for imported metadata must not acquire an upstream dependency by
    /// upgrading.
    #[tokio::test]
    async fn a_disabled_catalogue_starts_nothing() {
        let handle = start(
            &CatalogConfig::default(),
            None,
            &no_env(),
            std::future::pending(),
        )
        .await
        .expect("an inert configuration cannot fail");
        assert!(handle.is_none(), "nothing to report and nothing to stop");
    }

    /// Boot adopts whatever the store holds — here, the seed — and publishes it
    /// before the listener binds, so the first status read after boot already
    /// describes a catalogue rather than an absence.
    #[tokio::test]
    async fn boot_publishes_what_it_adopted_before_anything_is_served() {
        let handle = start(&offline(), None, &no_env(), std::future::pending())
            .await
            .expect("an offline catalogue starts")
            .expect("an enabled catalogue yields a handle");
        let report = handle
            .status()
            .report()
            .expect("boot published its restoration");
        assert!(
            report.active.is_some(),
            "a seeded bootstrap is active immediately"
        );
        assert_eq!(report.consecutive_refusals, 0);
    }

    /// A manual refresh is never skipped for not being due, which is the whole
    /// difference between the two triggers: an operator asking now has already
    /// decided the schedule is not what they want to wait for.
    #[tokio::test]
    async fn a_manual_refresh_is_not_skipped_for_not_being_due() {
        let config = CatalogConfig {
            // Longer than this test could ever wait: any refresh that happens is
            // therefore the manual one.
            refresh_interval_seconds: 86_400,
            ..offline()
        };
        let handle = start(&config, None, &no_env(), std::future::pending())
            .await
            .expect("an offline catalogue starts")
            .expect("an enabled catalogue yields a handle");
        let outcome = handle
            .refresh_now()
            .await
            .expect("the refresh task is running");
        assert!(
            !matches!(outcome, RefreshOutcome::NotDue { .. }),
            "a manual refresh must run, said: {outcome:?}"
        );
        assert!(
            handle
                .status()
                .report()
                .is_some_and(|report| report.active.is_some()),
            "the refresh published what it left active"
        );
    }

    /// Dropping every handle leaves the schedule running, and *asleep*.
    ///
    /// A closed receiver is ready forever, so the arm reading it has to stop
    /// being selected rather than be re-armed: an arm ahead of the sleep under
    /// `biased` that is permanently ready would spin a core and starve the
    /// schedule it was meant to preserve. The observable version of that is
    /// this: with no handle left, the next interval still imports, which is only
    /// reachable through the sleep arm.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_catalogue_nothing_holds_a_handle_to_keeps_refreshing_on_schedule() {
        let config = CatalogConfig {
            refresh_interval_seconds: 1,
            refresh_timeout_seconds: 1,
            retry_initial_seconds: 1,
            retry_max_seconds: 1,
            ..offline()
        };
        let handle = start(&config, None, &no_env(), std::future::pending())
            .await
            .expect("an offline catalogue starts")
            .expect("an enabled catalogue yields a handle");
        let status = Arc::clone(handle.status());
        drop(handle);

        // What the boot import left active. Every later import re-stamps it with
        // when this process last confirmed the content, so the timestamp moving
        // is the schedule having run after the last handle went away.
        let confirmed = |report: Option<CatalogReport>| {
            report
                .and_then(|report| report.active)
                .map(|active| active.fetched_at)
        };
        let before = confirmed(status.report());
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if confirmed(status.report()) > before {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the schedule stopped importing once the last handle dropped"
            );
        }
    }

    /// Shutdown outranks catalogue work: a terminating process must not start an
    /// import it will abandon, and the handle answering `None` is how a caller
    /// tells "the deployment is stopping" from "the import was refused".
    #[tokio::test]
    async fn shutdown_stops_the_loop_and_refuses_nothing() {
        let (stop, stopped) = oneshot::channel::<()>();
        let handle = start(&offline(), None, &no_env(), async move {
            let _ = stopped.await;
        })
        .await
        .expect("an offline catalogue starts")
        .expect("an enabled catalogue yields a handle");
        let before = handle.status().report().expect("boot published a report");
        stop.send(()).expect("the task is listening");

        // The task ends without publishing anything further: the last report
        // stays readable, and no refusal is recorded for having stopped.
        while handle.refresh_now().await.is_some() {
            tokio::task::yield_now().await;
        }
        let after = handle.status().report().expect("the report is still there");
        assert_eq!(after.consecutive_refusals, before.consecutive_refusals);
        assert_eq!(
            after.active.map(|active| active.content_id),
            before.active.map(|active| active.content_id)
        );
    }

    /// Retention names its DSN by name, and a name nothing resolves is a boot
    /// failure that says which variable is empty — never what it would have
    /// held.
    #[tokio::test]
    async fn a_dsn_reference_nothing_resolves_fails_boot_without_the_dsn() {
        let config = CatalogConfig {
            store: CatalogStoreBackend::Postgres,
            dsn_env: Some("AXOND_CATALOG_DSN_ABSENT".to_owned()),
            ..offline()
        };
        let error = start(&config, None, &no_env(), std::future::pending())
            .await
            .expect_err("retention cannot be opened without a connection string");
        let message = error.to_string();
        assert!(
            message.contains("AXOND_CATALOG_DSN_ABSENT"),
            "the failure must name the variable, said: {message}"
        );
        assert!(
            !message.contains("postgres://"),
            "the failure must never carry a connection string, said: {message}"
        );
    }
}
