//! Axond — a stateless, single-binary, self-hosted AI gateway.
//!
//! Boot sequence: load + validate config (fail fast, delta B2), snapshot the
//! environment for credential resolution, build shared state with the default
//! no-datastore usage sink and budget store, then serve.

mod budget;
mod config;
mod credentials;
mod error;
mod routes;
mod state;
mod usage;

use std::collections::HashMap;

use budget::{BudgetStore, NoBudget};
use config::Config;
use state::AppState;
use usage::{StdoutSink, UsageFanout, UsageSink};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .json()
        .init();

    let config_path =
        std::env::var("AXOND_CONFIG").unwrap_or_else(|_| "axond.toml".to_string());
    let config = Config::load(&config_path)
        .map_err(|e| anyhow::anyhow!("failed to load config from `{config_path}`: {e}"))?;

    let env: HashMap<String, String> = std::env::vars().collect();

    // No-datastore defaults: usage to stdout, budget always-allow. Postgres /
    // Tinybird sinks and Redis / in-memory budget are opt-in via config.
    let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(StdoutSink)];
    let usage = UsageFanout::new(sinks);
    let budget: Box<dyn BudgetStore> = Box::new(NoBudget);

    let bind = config.server.bind;
    let state = AppState::new(config, &env, usage, budget);
    let app = routes::router(state);

    tracing::info!(%bind, "axond listening");
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
