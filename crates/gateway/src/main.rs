//! Axond — a stateless, single-binary, self-hosted AI gateway.
//!
//! Boot sequence: install telemetry (logs always, OTLP only when configured),
//! load + validate config (fail fast, delta B2), snapshot the environment for
//! credential resolution, connect the configured usage sinks, build shared
//! state, then serve.

mod budget;
mod config;
mod credentials;
mod error;
mod routes;
mod state;
mod streaming;
mod telemetry;
mod usage;

use std::collections::HashMap;

use budget::{BudgetStore, NoBudget};
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
    // usage sinks and Redis / in-memory budget are opt-in via config. Sinks are
    // connected here, so a misconfigured datastore fails at boot rather than
    // discarding records at request time.
    let usage = UsageFanout::new(
        usage::build_sinks(&config.usage_sink, &env)
            .await
            .map_err(|e| anyhow::anyhow!("usage sink configuration failed: {e}"))?,
    );
    let budget: Box<dyn BudgetStore> = Box::new(NoBudget);

    let bind = config.server.bind;
    let state = AppState::new(config, &env, usage, budget)
        .map_err(|e| anyhow::anyhow!("credential validation failed: {e}"))?;
    let app = routes::router(state).layer(telemetry::TelemetryLayer);

    tracing::info!(%bind, otlp = telemetry::is_exporting(), "axond listening");
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
