//! Axond — a store-backed, single-binary, self-hosted AI gateway.
//!
//! Boot sequence: install telemetry (logs always, OTLP only when configured),
//! load + validate config (fail fast, delta B2), snapshot the environment for
//! credential resolution, connect the configured usage sinks, open the Store,
//! build shared state, then serve.
//!
//! Termination is the boot sequence in reverse and bounded at every step:
//! `SIGTERM` fails readiness, then closes admission, then lets admitted requests
//! finish, then flushes the usage sinks and the exporters. [`shutdown`] owns the
//! sequencing; this module owns the order the resources are released in.

mod admission;
mod aliases;
mod api;
// The catalogue import (`[catalog]`) and its models.dev adapter.
mod backends;
mod backoff;
mod budget;
mod config;
mod credentials;
// Value types the live gateway borrows from the withdrawn desired-state domain.
mod core_accounting;
mod desired_state;
mod discovery;
mod error;
mod key_material;
mod namespace;
mod pricing;
mod principals;
mod routes;
mod settlement;
mod shutdown;
mod state;
mod store;
mod streaming;
mod telemetry;
#[cfg(test)]
mod test_services;
mod usage;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use budget::{BudgetStore, StoreBudget};
use clap::Command;
use config::Config;
use state::AppState;
use usage::UsageRuntime;

fn main() -> anyhow::Result<()> {
    cli().get_matches();
    serve()
}

fn cli() -> Command {
    Command::new("axond")
        .about("A store-backed, self-hosted AI gateway")
        .version(env!("CARGO_PKG_VERSION"))
}

#[tokio::main]
async fn serve() -> anyhow::Result<()> {
    // Held until shutdown so the exporters flush; a no-op when telemetry is off.
    let mut telemetry_guard = telemetry::init().map_err(|e| anyhow::anyhow!("telemetry: {e}"))?;
    // Installed before the listener exists: a platform that will not give us a
    // handler must fail at boot, not when the rollout depends on it.
    let signals = shutdown::Signals::install()
        .map_err(|e| anyhow::anyhow!("failed to install termination signal handlers: {e}"))?;

    let config_path = std::env::var("AXOND_CONFIG").unwrap_or_else(|_| "axond.toml".to_string());
    let config = Config::load(&config_path)
        .map_err(|e| anyhow::anyhow!("failed to load config from `{config_path}`: {e}"))?;

    let env: HashMap<String, String> = std::env::vars().collect();

    // Usage goes to stdout unless a sink is configured; durable sinks are
    // connected here, so a misconfigured datastore fails at boot rather than
    // discarding records later. A `[usage_journal]` section is what turns the
    // best-effort path into a durable one, and it is connected here for the
    // same reason: a deployment that asked for billing-grade usage and cannot
    // reach its outbox must fail at boot rather than fail closed on every
    // request (ADR 0049).
    let UsageRuntime {
        delivery: usage,
        worker: usage_worker,
    } = usage::build_runtime(&config.usage_sink, &config.usage_journal, &env)
        .await
        .map_err(|e| anyhow::anyhow!("usage sink configuration failed: {e}"))?;
    tracing::info!(
        mode = usage.mode().as_str(),
        durable = usage.mode().is_durable(),
        journal = config.usage_journal.backend.as_str(),
        on_undurable = config.usage_journal.on_undurable.as_str(),
        "usage delivery"
    );
    let storage = config
        .storage
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("`[storage]` is required (ADR 0063)"))?;
    let store = crate::store::open(storage, &env)
        .await
        .map_err(|e| anyhow::anyhow!("store: {e}"))?;
    crate::store::seed_config_namespaces(store.as_ref(), &config.namespace)
        .await
        .map_err(|e| anyhow::anyhow!("store seed: {e}"))?;
    let budget: Box<dyn BudgetStore> =
        Box::new(StoreBudget::new(Arc::clone(&store), storage.on_unavailable));
    tracing::info!(backend = budget.name(), "budget enforcement");
    // Metadata ingestion, brought up before the listener and owned by a task of
    // its own: every import runs off the request path, and a request cannot reach
    // the source or the store even indirectly (#146). A deployment that imports
    // nothing — the default — builds no client and opens no connection.
    //
    // The stop signal fires after serving ends rather than at `SIGTERM`, which
    // is the cheap end of the trade: an import already in flight is abandoned no
    // later than the drain it cannot outlive, and nothing in the drain waits on
    // it.
    let (stop_catalogue, catalogue_stopped) = tokio::sync::oneshot::channel::<()>();
    let catalogue = backends::catalog_runtime::start(&config.catalog, None, &env, async move {
        let _ = catalogue_stopped.await;
    })
    .await
    .map_err(|e| anyhow::anyhow!("catalogue import configuration failed: {e}"))?;
    if catalogue.is_some() {
        tracing::info!(
            source = config.catalog.source.as_str(),
            store = config.catalog.store.as_str(),
            interval_s = config.catalog.refresh_interval_seconds,
            "catalogue imports"
        );
    }

    let bind = config.server.bind;
    let state = AppState::serving(
        config.clone(),
        &env,
        usage,
        budget,
        catalogue.as_ref().map(|handle| Arc::clone(handle.status())),
        store,
    )
    .map_err(|e| anyhow::anyhow!("config resolution failed: {e}"))?;

    tracing::info!(
        gateway_keys = state.config().inbound_key_count(),
        "inbound auth enforced"
    );
    let lifecycle = Arc::clone(state.lifecycle());
    // Kept past the router so the sinks can be flushed after the last request:
    // shutdown is the one point where durability outranks the request path.
    let resources = state.clone();

    // Inference under `/ns/{ns}/v1` and management under `/api/v1` (ADR 0063).
    let app = routes::router(state.clone()).layer(telemetry::TelemetryLayer);

    tracing::info!(
        %bind,
        otlp = telemetry::is_exporting(),
        "axond listening"
    );
    let listener = tokio::net::TcpListener::bind(bind).await?;
    // The plan is read when the signal arrives rather than now, so a reload of
    // `[shutdown]` applies to the termination that follows it. The drain
    // publishes what it read, and every later step reads it back from there:
    // all three bounds come from one snapshot.
    let resolved = shutdown::ResolvedPlan::new();
    let drain = shutdown::drain(
        Arc::clone(&lifecycle),
        signals,
        {
            let resources = resources.clone();
            move || shutdown::Plan::from(&resources.config().config.shutdown)
        },
        resolved.clone(),
    );
    let (stop_discovery, stop_discovery_rx) = tokio::sync::oneshot::channel::<()>();
    let discovering = {
        let state = state.clone();
        tokio::spawn(async move {
            discovery::run(state, stop_discovery_rx).await;
        })
    };
    let served = axum::serve(listener, app).with_graceful_shutdown(drain);
    // Only used if the server ends without ever being signalled.
    let boot = shutdown::Plan::from(&resources.config().config.shutdown);
    let outcome = shutdown::serve_bounded(served, &lifecycle, &resolved, boot).await;
    let plan = resolved.or(boot);

    let _ = stop_discovery.send(());
    // Best-effort cache: do not spend the settle/flush budget on a stuck
    // store write. Abort if cooperative stop does not finish immediately.
    let mut discovering = discovering;
    if tokio::time::timeout(Duration::from_millis(50), &mut discovering)
        .await
        .is_err()
    {
        discovering.abort();
        tracing::debug!("discovery task aborted at shutdown");
    }

    // Nothing below waits on the import: its work is metadata, and the budget
    // that follows belongs to spend that was already incurred.
    drop(catalogue);
    let _ = stop_catalogue.send(());

    // One budget for the whole post-serving sequence, not one per step: what an
    // orchestrator's termination grace period has to cover is the total, and the
    // steps are ordered by how much of the record depends on them. The waits
    // get at most half of it ([`shutdown::Plan::settle_share`]) so that a
    // request which cannot end cannot cost the records already accepted their
    // write.
    let started = Instant::now();
    let flush_by = started + plan.flush_timeout;
    let settle_by = started + plan.settle_share();
    let until = |deadline: Instant| deadline.saturating_duration_since(Instant::now());

    // Abandoned responses settle as they end, so the settlements queued by the
    // requests that just finished have to land before the sinks are flushed.
    let stuck = lifecycle.quiesce(until(settle_by)).await;
    let leftovers = resources.0.settlements.await_idle(until(settle_by)).await;
    let unsettled = leftovers.unsettled();
    if stuck > 0 || unsettled > 0 {
        // Counted as abandoned here as well as at the deadline: work that
        // outlives the settle window is work whose spend this process will
        // never record, whether or not the deadline was what cut it.
        telemetry::metrics::record_shutdown_abandoned(stuck);
        telemetry::metrics::record_shutdown_abandoned_settlements(unsettled);
        tracing::error!(
            in_flight = stuck,
            unsettled,
            settlements_queued = leftovers.queued,
            settlements_executing = leftovers.executing,
            settlements_reserved = leftovers.reserved,
            oldest_settlement_ms = leftovers
                .oldest_age
                .map(|age| age.as_millis() as u64)
                .unwrap_or(0),
            settle_share_ms = plan.settle_share().as_millis() as u64,
            "some spend could not be settled within the settle share of the flush budget"
        );
    }
    // Records already accepted are written even when requests were abandoned:
    // spend that was incurred must be accounted for either way, which is why the
    // waits above cannot spend this reserve.
    let flushed = resources.0.usage.flush(until(flush_by)).await;
    flushed.log();
    // The journal's own drain, and a distinct report: a backlog left in a durable
    // outbox is delivered by whichever replica claims it next, so it is undelivered
    // work rather than lost usage and must not be logged as a drop.
    //
    // It gets half of what is left rather than all of it, and pays its own
    // abandonment margin out of that share: a drain costs its caller
    // `budget + DRAIN_MARGIN`, and a drain that spent the whole remainder — the
    // normal case behind a backlog — would push the process past `flush_timeout`
    // and leave the telemetry export a deadline already in the past. Under the
    // margin there is no honest wait left to make, so the worker is stopped
    // without one.
    // Journal delivery and the management usage-index share whatever is left of
    // the flush budget: both cost `budget + DRAIN_MARGIN`, both are abandoned
    // without a wait when that cannot be paid, and they run together so a long
    // outbox drain cannot starve index writes (or the reverse). Index leftovers
    // are in-memory and lost at exit; journal leftovers are durable.
    let share = (until(flush_by) / 2).checked_sub(usage::DRAIN_MARGIN);
    let (journal_drain, index_drain): (Option<usage::DrainReport>, usage::IndexDrainReport) =
        match (usage_worker, share) {
            (Some(worker), Some(budget)) => {
                let (journal, index) =
                    tokio::join!(worker.drain(budget), resources.0.usage.drain_index(budget));
                (Some(journal), index)
            }
            (Some(worker), None) => (Some(worker.abandon()), resources.0.usage.abandon_index()),
            (None, Some(budget)) => (None, resources.0.usage.drain_index(budget).await),
            (None, None) => (None, resources.0.usage.abandon_index()),
        };
    if let Some(report) = journal_drain.as_ref() {
        report.log();
    }
    index_drain.log();
    if let Some(leftover) = index_drain.abandoned() {
        telemetry::metrics::record_shutdown_abandoned_index(leftover);
    }
    let telemetry_failures = telemetry_guard.shutdown(flush_by);
    tracing::info!(
        outcome = outcome.as_str(),
        usage_flushed = flushed.is_complete(),
        usage_journal_drained = journal_drain.as_ref().and_then(|report| report.caught_up()),
        usage_index_drained = index_drain.caught_up(),
        telemetry_flushed = telemetry_failures.is_empty(),
        "axond stopped"
    );

    match outcome {
        // Abandoned work and an incomplete flush are reported, not fatal: the
        // process did what it promised within its bounds, and exiting non-zero
        // would make an orchestrator treat a clean rollout as a crash.
        shutdown::Outcome::Completed | shutdown::Outcome::Abandoned { .. } => Ok(()),
        shutdown::Outcome::Failed(error) => Err(anyhow::anyhow!("serving failed: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_subcommand_is_still_serve() {
        let matches = cli().try_get_matches_from(["axond"]).expect("serve");
        assert!(matches.subcommand().is_none());
    }

    #[test]
    fn the_binary_reports_the_version_embedded_in_its_own_bytes() {
        let version = cli()
            .try_get_matches_from(["axond", "--version"])
            .expect_err("version exits after printing");
        assert_eq!(version.kind(), clap::error::ErrorKind::DisplayVersion);
        assert_eq!(
            version.to_string(),
            format!("axond {}\n", env!("CARGO_PKG_VERSION"))
        );
    }

    /// Every former operator subcommand is unknown to the parser.
    #[test]
    fn the_withdrawn_commands_are_not_parsed() {
        for command in [
            "mint", "keygen", "revoke", "check", "migrate", "admin", "budget",
        ] {
            let error = cli()
                .try_get_matches_from(["axond", command])
                .expect_err("a withdrawn command must not parse");
            assert_eq!(
                error.kind(),
                clap::error::ErrorKind::UnknownArgument,
                "`{command}`: {error}"
            );
        }
    }
}
