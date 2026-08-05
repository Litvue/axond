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

mod budget;
mod config;
mod credentials;
mod error;
mod reload;
mod routes;
mod state;
mod streaming;
mod telemetry;
mod usage;

use std::collections::HashMap;
use std::sync::Arc;

use budget::BudgetStore;
use config::Config;
use state::AppState;
use usage::UsageFanout;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
    let budget: Box<dyn BudgetStore> = budget::build(&config.budget, &env)
        .await
        .map_err(|e| anyhow::anyhow!("budget configuration failed: {e}"))?;
    tracing::info!(backend = budget.name(), "budget enforcement");

    let bind = config.server.bind;
    let watching = config.reload.watch;
    let state = AppState::new(config, &env, usage, budget)
        .map_err(|e| anyhow::anyhow!("config resolution failed: {e}"))?;
    tracing::info!(
        gateway_keys = state.config().inbound_keys.len(),
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
