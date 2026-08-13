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

// The `/admin/v1` surface: the only way durable desired state changes (#143).
// Mounted by `serve` beside the inference router, with its own authentication
// and its own error envelope; the inference request path is unchanged and still
// never reads the control plane.
#[allow(dead_code)]
mod admin;
mod admission;
mod aliases;
// Derived availability and discovery evaluation (#206). Contract only: no
// provider is polled, no observation is persisted, and no request is enforced
// against a verdict, so `serve` constructs no index and every snapshot carries
// the empty one.
#[allow(dead_code)]
mod availability;
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
// Operator commands: `axond check preflight`, `axond migrate status`,
// `axond migrate apply`, and `axond migrate adopt`. Nothing here is on the
// request path or reachable from `serve`.
mod ops;
mod principals;
// The recovery qualification driver (#219). Tests only: it holds a replica's
// reconciler, its cache, and a real Postgres journal at once, and takes the
// database away from underneath them, which is not reachable from outside the
// binary while stateful boot is not wired to `serve`.
#[cfg(test)]
mod qualification;
mod rate_limit;
mod redis_support;
mod reload;
mod revocation;
mod routes;
// What the secret contracts add up to, asserted over the composition rather
// than over the pieces: material an administrator stages reaches the provider
// it authenticates to and no other surface, and the lifecycle around it —
// rotation, failure, retirement — is safe for requests in flight.
#[cfg(test)]
mod secret_redaction;
mod shutdown;
mod state;
// The authenticated status contract (#199). Contract only, like `backends` and
// `convergence`: the dependencies it reports on are not constructed by `serve`
// yet, and `/healthz` and `/readyz` keep answering from process state alone.
#[allow(dead_code)]
mod status;
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
use config::Config;
use rate_limit::RateLimiter;
use revocation::RevocationStore;
use state::{AppState, ReplicaObservability};
use usage::UsageRuntime;

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
        Some(("check", args)) => match args.subcommand() {
            Some(("preflight", args)) => preflight(args),
            _ => unreachable!("clap validates subcommands"),
        },
        Some(("admin", args)) => admin::cli::run(args),
        Some(("migrate", args)) => match args.subcommand() {
            Some(("status", args)) => migrate_control_plane(args, Migration::Status),
            Some(("apply", args)) => migrate_control_plane(args, Migration::Apply),
            Some(("adopt", args)) => migrate_control_plane(args, Migration::Adopt),
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
        .subcommand(admin::cli::command())
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
            Command::new("check")
                .about("Check a deployment without starting one")
                .subcommand_required(true)
                .subcommand(
                    Command::new("preflight")
                        .about(
                            "Report everything a replica would fail at boot: config ownership and \
                             mode, bootstrap references, control-plane connectivity, and schema \
                             compatibility. Reads only.",
                        )
                        .arg(config_arg()),
                ),
        )
        .subcommand(
            Command::new("migrate")
                .about("Control-plane schema, reported and moved forward")
                .subcommand_required(true)
                .subcommand(
                    Command::new("status")
                        .about(
                            "Report the control-plane schema and what an apply would do. Reads \
                             only; exits non-zero while a migration is outstanding.",
                        )
                        .arg(config_arg()),
                )
                .subcommand(
                    Command::new("apply")
                        .about(
                            "Apply pending control-plane migrations, forward only. Idempotent and \
                             safe to run before replicas start.",
                        )
                        .arg(config_arg()),
                )
                .subcommand(
                    Command::new("adopt")
                        .about(
                            "Record the baseline an out-of-band `psql` apply left unrecorded, for \
                             the migrations whose tables this database actually holds. Executes no \
                             migration SQL; refuses anything but an empty ledger.",
                        )
                        .arg(config_arg()),
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

/// `--config PATH`, spelled identically for every operator command.
///
/// One shared definition rather than a copy per subcommand: the grammar is
/// `axond <command> <action> --config PATH`, and a flag that moved depending on
/// which action it followed would be a grammar an operator has to remember.
fn config_arg() -> Arg {
    Arg::new("config")
        .long("config")
        .value_name("PATH")
        .help("Config file path")
}

/// Where the config comes from: the flag, then `AXOND_CONFIG`, then the default
/// filename — the same order `serve` resolves it in, because these commands exist
/// to answer questions about what `serve` would do.
fn config_path(args: &clap::ArgMatches) -> String {
    args.get_one::<String>("config")
        .cloned()
        .or_else(|| std::env::var("AXOND_CONFIG").ok())
        .unwrap_or_else(|| "axond.toml".to_owned())
}

/// Turn an operator-command failure into the process' exit, keeping the
/// distinction the error type makes: an outage is worth another attempt, and a
/// refusal will refuse identically forever.
fn ops_failure(error: ops::OpsError) -> anyhow::Error {
    if error.is_retryable() {
        anyhow::anyhow!("{error} (the database was not reached; this is worth retrying)")
    } else {
        anyhow::anyhow!("{error}")
    }
}

/// `axond check preflight --config PATH`.
///
/// Reports every check and *then* fails, because an operator fixing a deployment
/// wants the whole list rather than the first item on it. Exits non-zero if any
/// check failed, so this is usable as a deployment gate.
fn preflight(args: &clap::ArgMatches) -> anyhow::Result<()> {
    let path = config_path(args);
    let config = ops::load(&path).map_err(ops_failure)?;
    let env: HashMap<String, String> = std::env::vars().collect();
    let runtime = tokio::runtime::Runtime::new()?;
    let report = runtime.block_on(ops::preflight::run(
        &config,
        std::path::Path::new(&path),
        &env,
    ));
    print!("{report}");
    if report.is_ok() {
        return Ok(());
    }
    anyhow::bail!(
        "preflight failed: {}",
        report
            .failures()
            .map(|check| check.name)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// Which part of `axond migrate` is running. They are one function because they
/// share every step except the last one — and separate subcommands because two of
/// them change a database and one cannot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Migration {
    Status,
    Apply,
    Adopt,
}

/// `axond migrate status`, `axond migrate apply`, and `axond migrate adopt`,
/// each `--config PATH`.
///
/// `status` is read-only and exits non-zero while a migration is outstanding, so
/// a rollout can gate on it. `apply` is forward-only and idempotent: running it
/// twice reports a current schema rather than migrating twice. `adopt` records the
/// baseline of a database whose DDL was applied out of band, on the evidence of
/// the objects it holds, and executes no migration file at all.
fn migrate_control_plane(args: &clap::ArgMatches, which: Migration) -> anyhow::Result<()> {
    let path = config_path(args);
    let config = ops::load(&path).map_err(ops_failure)?;
    let env: HashMap<String, String> = std::env::vars().collect();
    let runtime = tokio::runtime::Runtime::new()?;
    let report = match which {
        Migration::Status => runtime.block_on(ops::migrate::status(&config, &env)),
        Migration::Apply => runtime.block_on(ops::migrate::apply(&config, &env)),
        Migration::Adopt => runtime.block_on(ops::migrate::adopt(&config, &env)),
    }
    .map_err(ops_failure)?;
    println!("{report}");
    migration_exit(which, &report)
}

/// What a migration command's exit code says, separated from performing it so it
/// can be checked directly for every state.
///
/// A rollout gate reads the code and not the report, so "succeeded" has to mean
/// "and there is nothing left to do": `status` and `adopt` both exit non-zero on a
/// schema that still needs migrating. Adoption is the less obvious of the two —
/// recording a baseline below the required version *is* a success, and it is also
/// a database no replica may serve until `apply` runs.
fn migration_exit(which: Migration, report: &ops::migrate::Report) -> anyhow::Result<()> {
    match which {
        Migration::Status | Migration::Adopt if !report.is_settled() => {
            anyhow::bail!("the control-plane schema is not ready to serve")
        }
        _ if !report.is_ok() => anyhow::bail!("the control-plane schema was refused"),
        _ => Ok(()),
    }
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

    // The same refusal `axond check preflight` reports, read from the same place,
    // so the command cannot describe a surface this function would not serve.
    let inference_refusal = ops::inference_refusal(&config);

    let env: HashMap<String, String> = std::env::vars().collect();

    // No-datastore defaults: usage to stdout, budget always-allow. Durable
    // usage sinks and shared (Redis / Postgres) budget backends are opt-in via
    // config. Both are connected here, so a misconfigured datastore fails at
    // boot rather than discarding records — or denying every request — later.
    // A `[usage_journal]` section is what turns the best-effort path into a
    // durable one, and it is connected here for the same reason: a deployment
    // that asked for billing-grade usage and cannot reach its outbox must fail
    // at boot rather than fail closed on every request (ADR 0038).
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

    // Built before the inference state takes ownership of the config, and before
    // the listener exists: a stateful replica whose administrative surface cannot
    // come up must fail at boot rather than serve a deployment nobody can
    // administer. In stateless mode this opens nothing at all.
    let (admin_surface, admin_mode) =
        admin::runtime::surface(&config, &env).await.map_err(|e| {
            anyhow::anyhow!(
                "a stateful deployment could not bring up its administrative surface: {e}"
            )
        })?;
    tracing::info!(
        mode = admin_mode,
        prefix = admin::ADMIN_PREFIX,
        "administrative surface"
    );

    let bind = config.server.bind;
    let watching = config.reload.watch;
    let state = AppState::with_resources(
        config,
        &env,
        usage,
        budget,
        rate_limiter,
        revocation,
        ReplicaObservability::stateless(),
    )
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
    // A replica that cannot compile a revision into a snapshot still administers
    // one: the administrative surface is mounted either way, and only inference
    // is replaced by its refusal. The replica diagnostic is mounted either way
    // too — an unconverged replica is precisely the one an operator asks about.
    let inference = match inference_refusal {
        None => routes::router(state),
        Some(reason) => {
            tracing::warn!(reason, "inference is refused on this replica");
            routes::unconverged_router(reason).merge(routes::diagnostic_router(state))
        }
    };
    let app = inference
        .merge(admin_surface)
        .layer(telemetry::TelemetryLayer);

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
    // The journal's own drain, and a distinct report: a backlog left in a durable
    // outbox is delivered by whichever replica claims it next, so it is undelivered
    // work rather than lost usage and must not be logged as a drop.
    let journal_drain: Option<usage::DrainReport> = match usage_worker {
        Some(worker) => Some(worker.drain(until(flush_by)).await),
        None => None,
    };
    if let Some(report) = journal_drain.as_ref() {
        report.log();
    }
    let telemetry_failures = telemetry_guard.shutdown(flush_by);
    tracing::info!(
        outcome = outcome.as_str(),
        usage_flushed = flushed.is_complete(),
        usage_journal_drained = journal_drain.as_ref().map(|report| report.drained),
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

    /// The grammar itself, pinned: `axond <command> <action> --config PATH`, with
    /// the flag in one place. Operators write these into runbooks and Helm hooks,
    /// so a moved flag is a broken deployment rather than a cosmetic change.
    #[test]
    fn the_operator_commands_take_config_after_the_action() {
        for argv in [
            vec!["axond", "check", "preflight", "--config", "/etc/axond.toml"],
            vec!["axond", "migrate", "status", "--config", "/etc/axond.toml"],
            vec!["axond", "migrate", "apply", "--config", "/etc/axond.toml"],
        ] {
            let matches = cli()
                .try_get_matches_from(&argv)
                .unwrap_or_else(|error| panic!("`{}` must parse: {error}", argv.join(" ")));
            let (_, command) = matches.subcommand().expect("a command");
            let (_, action) = command.subcommand().expect("an action");
            assert_eq!(
                action.get_one::<String>("config").map(String::as_str),
                Some("/etc/axond.toml"),
                "`{}` must carry the config path on the action",
                argv.join(" ")
            );
        }
    }

    /// `axond check --config x preflight` is *not* the grammar. Accepting both
    /// spellings would be two grammars to document and one of them wrong.
    #[test]
    fn a_config_flag_before_the_action_is_rejected_rather_than_guessed() {
        for argv in [
            vec!["axond", "check", "--config", "/etc/axond.toml", "preflight"],
            vec!["axond", "migrate", "--config", "/etc/axond.toml", "status"],
        ] {
            assert!(
                cli().try_get_matches_from(&argv).is_err(),
                "`{}` must not parse",
                argv.join(" ")
            );
        }
    }

    /// A bare `axond check` or `axond migrate` does nothing implicitly: there is
    /// no default action, so neither can become an accidental migration.
    #[test]
    fn the_operator_commands_have_no_default_action() {
        for argv in [vec!["axond", "check"], vec!["axond", "migrate"]] {
            assert!(
                cli().try_get_matches_from(&argv).is_err(),
                "`{}` must require an action",
                argv.join(" ")
            );
        }
    }

    /// The exit code every migration command hands a rollout gate.
    ///
    /// `adopt` is the one worth pinning: recording a baseline succeeds, and a
    /// baseline below the required version still leaves `apply` to run, so a zero
    /// there would let replicas start against a schema that is not ready. The same
    /// holds for an `adopt` that found a ledger already recording a *behind*
    /// history, which reports pending migrations rather than an adoption.
    #[test]
    fn only_a_settled_schema_lets_status_or_adopt_exit_zero() {
        use ops::migrate::{Report, State};

        let control_plane = |state: State| Report::ControlPlane {
            dsn_env: "GW_CONTROL_PLANE_DSN".to_owned(),
            state,
        };
        let pending = vec![(2, "control_plane_0002_example")];
        let adopted = vec![(1, "control_plane_0001_initial")];

        for state in [
            State::Adopted {
                adopted: adopted.clone(),
                pending: pending.clone(),
            },
            State::Pending {
                pending: pending.clone(),
            },
        ] {
            let report = control_plane(state);
            assert!(report.is_ok(), "{report}");
            for which in [Migration::Status, Migration::Adopt] {
                assert!(
                    migration_exit(which, &report).is_err(),
                    "{which:?} must not exit zero while a migration is outstanding: {report}"
                );
            }
        }

        // A whole baseline, and a refusal: settled succeeds for both commands, and
        // a refused schema fails for all three whatever `is_settled` says.
        let whole = control_plane(State::Adopted {
            adopted,
            pending: Vec::new(),
        });
        let refused = control_plane(State::Refused {
            reason: "the ledger records nothing".to_owned(),
        });
        for which in [Migration::Status, Migration::Apply, Migration::Adopt] {
            assert!(migration_exit(which, &whole).is_ok(), "{whole}");
            assert!(migration_exit(which, &refused).is_err(), "{refused}");
        }
    }

    /// `axond` with no subcommand still serves, and no operator command is
    /// reachable without naming it.
    #[test]
    fn no_subcommand_is_still_serve() {
        let matches = cli().try_get_matches_from(["axond"]).expect("serve");
        assert!(matches.subcommand().is_none());
    }
}
