//! Axond — a stateless, single-binary, self-hosted AI gateway.
//!
//! Boot sequence: install telemetry (logs always, OTLP only when configured),
//! load + validate config (fail fast, delta B2), snapshot the environment for
//! credential resolution, connect the configured usage sinks, build shared
//! state, install the reload triggers, then serve.
//!
//! The config the process serves is replaceable at runtime: `SIGHUP` (and, when
//! `[reload] watch` is on, a change to the config file) re-runs this same load +
//! validate path and swaps the result in atomically (ADR 0011).

mod aliases;
mod budget;
mod config;
mod credentials;
mod error;
mod key_material;
mod mint;
mod principals;
mod rate_limit;
mod redis_support;
mod reload;
mod revocation;
mod routes;
mod state;
mod streaming;
mod telemetry;
#[cfg(test)]
mod test_services;
mod usage;

use std::collections::HashMap;
use std::sync::Arc;

use budget::BudgetStore;
use clap::{Arg, ArgAction, Command};
use config::Config;
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

#[tokio::main]
async fn serve() -> anyhow::Result<()> {
    // Held until shutdown so the exporters flush; a no-op when telemetry is off.
    let _telemetry = telemetry::init().map_err(|e| anyhow::anyhow!("telemetry: {e}"))?;

    let config_path = std::env::var("AXOND_CONFIG").unwrap_or_else(|_| "axond.toml".to_string());
    let config = Config::load(&config_path)
        .map_err(|e| anyhow::anyhow!("failed to load config from `{config_path}`: {e}"))?;

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
    reload::spawn(Arc::new(reload::Reloader::new(config_path, state.clone())));
    let app = routes::router(state).layer(telemetry::TelemetryLayer);

    tracing::info!(
        %bind,
        otlp = telemetry::is_exporting(),
        config_watch = watching,
        "axond listening"
    );
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
