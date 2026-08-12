//! Axond — a stateless, single-binary, self-hosted AI gateway.
//!
//! Boot sequence: install telemetry (logs always, OTLP only when configured),
//! load + validate config (fail fast, delta B2), snapshot the environment for
//! credential resolution, connect the configured usage sinks, build shared
//! state, install the reload triggers, then serve.
//!
//! Termination is the boot sequence in reverse and bounded at every step:
//! `SIGTERM` fails readiness, then closes admission, then lets admitted requests
//! finish, then flushes the usage sinks and the exporters. [`shutdown`] owns the
//! sequencing; this module owns the order the resources are released in.
//!
//! The config the process serves is replaceable at runtime: `SIGHUP` (and, when
//! `[reload] watch` is on, a change to the config file) re-runs this same load +
//! validate path and swaps the result in atomically (ADR 0011).

mod admission;
mod aliases;
// Contracts only: the durable implementations land in #141/#142, so nothing
// here is constructed by `serve` yet and the runtime stays stateless.
#[allow(dead_code)]
mod backends;
mod budget;
mod config;
// Stateful revision convergence (#142). Dead code until a projection from
// resource bodies to a servable config lands with the body-schema slices; the
// loop, its contract, and its tests are complete without one.
#[allow(dead_code)]
mod convergence;
mod credentials;
// The desired-state domain the durable contracts are expressed in. Contract
// only, for the same reason `backends` is: no revision is loaded or published on
// the request path yet.
#[allow(dead_code)]
mod desired_state;
mod error;
mod key_material;
mod mint;
mod principals;
mod rate_limit;
mod redis_support;
mod reload;
mod revocation;
mod routes;
mod shutdown;
mod state;
mod streaming;
mod telemetry;
#[cfg(test)]
mod test_services;
mod usage;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use budget::BudgetStore;
use clap::{Arg, ArgAction, Command};
use config::{Config, Mode};
use rate_limit::RateLimiter;
use revocation::RevocationStore;
use state::AppState;
use usage::UsageFanout;

fn main() -> anyhow::Result<()> {
    let matches = cli().get_matches();
    match matches.subcommand() {
        Some(("mint", args)) => mint::run(args),
        Some(("keygen", args)) => mint::keygen(args),
        Some(("revoke", args)) => mint::revoke(args),
        Some(("budget", args)) => match args.subcommand() {
            Some(("migrate-redis", args)) => migrate_redis_budget(args),
            _ => unreachable!("clap validates subcommands"),
        },
        None => serve(),
        _ => unreachable!("clap validates subcommands"),
    }
}

fn cli() -> Command {
    Command::new("axond")
        .about("A stateless, self-hosted AI gateway")
        .subcommand_required(false)
        .arg_required_else_help(false)
        .subcommand(
            Command::new("revoke")
                .about("Add a minted-token JTI to the revocation denylist")
                .arg(
                    Arg::new("jti")
                        .long("jti")
                        .required(true)
                        .help("Token JTI to deny"),
                )
                .arg(
                    Arg::new("ttl")
                        .long("ttl")
                        .conflicts_with("expires-at")
                        .help("How long the JTI should remain revoked"),
                )
                .arg(
                    Arg::new("expires-at")
                        .long("expires-at")
                        .conflicts_with("ttl")
                        .help("Absolute expiry as Unix seconds or RFC3339 UTC"),
                )
                .arg(
                    Arg::new("config")
                        .long("config")
                        .value_name("PATH")
                        .help("Config file path"),
                ),
        )
        .subcommand(
            Command::new("budget")
                .about("Budget state maintenance")
                .subcommand_required(true)
                .subcommand(
                    Command::new("migrate-redis")
                        .about(
                            "Move Redis budget state to the v2 layout `namespace_limit_microdollars` needs",
                        )
                        .arg(
                            Arg::new("config")
                                .long("config")
                                .value_name("PATH")
                                .help("Config file path"),
                        ),
                ),
        )
        .subcommand(
            Command::new("mint")
                .about("Mint an offline inbound identity token")
                .arg(
                    Arg::new("kid")
                        .long("kid")
                        .required(true)
                        .help("JWS key identifier"),
                )
                .arg(
                    Arg::new("alg")
                        .long("alg")
                        .value_parser(["EdDSA", "HS256"])
                        .help("Signing algorithm; inferred from matching config verifier"),
                )
                .arg(
                    Arg::new("key-env")
                        .long("key-env")
                        .required(true)
                        .help("Environment variable containing signing key material"),
                )
                .arg(
                    Arg::new("namespace")
                        .long("namespace")
                        .required(true)
                        .help("Namespace claim"),
                )
                .arg(
                    Arg::new("subject")
                        .long("subject")
                        .required(true)
                        .help("Subject claim"),
                )
                .arg(
                    Arg::new("ttl")
                        .long("ttl")
                        .required(true)
                        .help("Token lifetime, such as 15m or 1h"),
                )
                .arg(
                    Arg::new("alias")
                        .long("alias")
                        .action(ArgAction::Append)
                        .help("Alias pattern claim; repeatable"),
                )
                .arg(
                    Arg::new("audience")
                        .long("audience")
                        .visible_alias("aud")
                        .help("Audience claim; defaults from a matching verifier config"),
                )
                .arg(
                    Arg::new("scope")
                        .long("scope")
                        .action(ArgAction::Append)
                        .help("Route capability claim; repeat for multiple capabilities"),
                )
                .arg(
                    Arg::new("max-request-microdollars")
                        .long("max-request-microdollars")
                        .value_name("MICRODOLLARS")
                        .value_parser(clap::value_parser!(u64))
                        .help("Optional per-request cost ceiling in microdollars"),
                )
                .arg(
                    Arg::new("config")
                        .long("config")
                        .value_name("PATH")
                        .help("Optional config used to infer verifier settings and max_ttl"),
                ),
        )
        .subcommand(
            Command::new("keygen")
                .about("Generate an Ed25519 verifier keypair")
                .arg(
                    Arg::new("private-key")
                        .long("private-key")
                        .required(true)
                        .value_name("PATH")
                        .help("New file for base64 PKCS#8 private key material"),
                )
                .arg(
                    Arg::new("kid")
                        .long("kid")
                        .required(true)
                        .help("JWS key identifier"),
                )
                .arg(
                    Arg::new("env")
                        .long("env")
                        .required(true)
                        .help("Environment variable for the public key"),
                )
                .arg(
                    Arg::new("namespace")
                        .long("namespace")
                        .required(true)
                        .action(ArgAction::Append)
                        .help("Namespace permitted for this verifier; repeatable"),
                )
                .arg(
                    Arg::new("max-ttl")
                        .long("max-ttl")
                        .default_value("15m")
                        .help("max_ttl shown in the verifier snippet"),
                ),
        )
}

/// Carry Redis budget state into the v2 key layout, with the fleet stopped.
/// Separate from `serve` on purpose: enabling a namespace cap must not silently
/// migrate (or reset) shared spend as a side effect of a rolling restart.
fn migrate_redis_budget(args: &clap::ArgMatches) -> anyhow::Result<()> {
    let config_path = args
        .get_one::<String>("config")
        .cloned()
        .or_else(|| std::env::var("AXOND_CONFIG").ok())
        .unwrap_or_else(|| "axond.toml".to_owned());
    let config = Config::load(&config_path)
        .map_err(|e| anyhow::anyhow!("failed to load config from `{config_path}`: {e}"))?;
    let env: HashMap<String, String> = std::env::vars().collect();
    let runtime = tokio::runtime::Runtime::new()?;
    // The v1 keys are attributed against the configured namespaces, so this must
    // run with the config that wrote them.
    // Distinct: an id may legitimately appear twice, and the same id offered
    // twice as a candidate owner of a key is not an ambiguity.
    let namespaces: Vec<String> = config
        .namespace
        .iter()
        .map(|namespace| namespace.id.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let report = runtime.block_on(budget::migrate_redis(&config.budget, &namespaces, &env))?;
    eprintln!(
        "migrated {} subject ledger(s) into {} namespace total(s), carrying {} micro-dollars; \
         dropped {} stale reservation hash(es)",
        report.subjects, report.namespaces, report.carried_microdollars, report.reservation_hashes
    );
    Ok(())
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

    // Stateful bootstrap parses and validates (ADR 0027, #162), but the durable
    // control plane it points at does not exist yet (#141, #163, #142). A
    // stateful cold boot must reach the control plane or fail loudly, so this
    // refuses rather than serving the empty snapshot an unread control plane
    // would leave behind.
    if config.mode == Mode::Stateful {
        anyhow::bail!(
            "`mode = \"stateful\"` is accepted by configuration validation, but the durable \
             control plane it bootstraps is not implemented yet; a stateful replica must reach \
             the control plane rather than serve an empty snapshot, so this process refuses to \
             start. Use `mode = \"stateless\"` (the default) until the control plane ships."
        );
    }

    let env: HashMap<String, String> = std::env::vars().collect();

    // No-datastore defaults: usage to stdout, budget always-allow. Durable
    // usage sinks and shared (Redis / Postgres) budget backends are opt-in via
    // config. Both are connected here, so a misconfigured datastore fails at
    // boot rather than discarding records — or denying every request — later.
    let usage = UsageFanout::new(
        usage::build_sinks(&config.usage_sink, &env)
            .await
            .map_err(|e| anyhow::anyhow!("usage sink configuration failed: {e}"))?,
    );
    let budget: Box<dyn BudgetStore> =
        budget::build(&config.budget, &env, config.distinct_namespace_count())
            .await
            .map_err(|e| anyhow::anyhow!("budget configuration failed: {e}"))?;
    tracing::info!(backend = budget.name(), "budget enforcement");
    let rate_limiter: Box<dyn RateLimiter> =
        rate_limit::build(&config.rate_limit, &config.budget, &env)
            .await
            .map_err(|e| anyhow::anyhow!("rate-limit configuration failed: {e}"))?;
    tracing::info!(backend = rate_limiter.name(), "inbound rate limiting");
    let revocation: Box<dyn RevocationStore> =
        revocation::build(&config.revocation, &config.budget, &env)
            .await
            .map_err(|e| anyhow::anyhow!("revocation configuration failed: {e}"))?;
    if revocation.name() != "none" {
        tracing::info!(backend = revocation.name(), "token revocation");
    }

    let bind = config.server.bind;
    let watching = config.reload.watch;
    let state =
        AppState::new_with_rate_limiter(config, &env, usage, budget, rate_limiter, revocation)
            .map_err(|e| anyhow::anyhow!("config resolution failed: {e}"))?;
    tracing::info!(
        gateway_keys = state.config().inbound_key_count(),
        gateway_verifiers = state.config().token_verifier_count(),
        "inbound auth enforced"
    );
    if let Some(minting) = state.config().config.gateway_minting.as_ref() {
        tracing::info!(
            kid = %minting.kid,
            "gateway token minting enabled; this replica can sign tokens"
        );
    }
    reload::spawn(Arc::new(reload::Reloader::new(config_path, state.clone())));
    let lifecycle = Arc::clone(state.lifecycle());
    // Kept past the router so the sinks can be flushed after the last request:
    // shutdown is the one point where durability outranks the request path.
    let resources = state.clone();
    let app = routes::router(state).layer(telemetry::TelemetryLayer);

    tracing::info!(
        %bind,
        otlp = telemetry::is_exporting(),
        config_watch = watching,
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
    let served = axum::serve(listener, app).with_graceful_shutdown(drain);
    // Only used if the server ends without ever being signalled.
    let boot = shutdown::Plan::from(&resources.config().config.shutdown);
    let outcome = shutdown::serve_bounded(served, &lifecycle, &resolved, boot).await;
    let plan = resolved.or(boot);

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
    let unsettled = streaming::await_settlements(until(settle_by)).await;
    if stuck > 0 || unsettled > 0 {
        // Counted as abandoned here as well as at the deadline: work that
        // outlives the settle window is work whose spend this process will
        // never record, whether or not the deadline was what cut it.
        telemetry::metrics::record_shutdown_abandoned(stuck);
        tracing::error!(
            in_flight = stuck,
            unsettled,
            settle_share_ms = plan.settle_share().as_millis() as u64,
            "some spend could not be settled within the settle share of the flush budget"
        );
    }
    // Records already accepted are written even when requests were abandoned:
    // spend that was incurred must be accounted for either way, which is why the
    // waits above cannot spend this reserve.
    let flushed = resources.0.usage.flush(until(flush_by)).await;
    flushed.log();
    let telemetry_failures = telemetry_guard.shutdown(flush_by);
    tracing::info!(
        outcome = outcome.as_str(),
        usage_flushed = flushed.is_complete(),
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
