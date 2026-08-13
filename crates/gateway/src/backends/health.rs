//! Reachability of a store the request path enforces against, for diagnostics
//! only.
//!
//! Every request-path seam ([`crate::budget`], [`crate::rate_limit`],
//! [`crate::revocation`]) already proves its store answers at boot and already
//! classifies its own failures. What it does not have is a way to say so *now*,
//! off the request path: an operator reading `GET /admin/v1/status` during a
//! wave of `503`s needs to know whether the budget store is unreachable or the
//! caps are simply being hit, and the request path cannot tell them that without
//! a request to fail first.
//!
//! This is that seam, and it is deliberately the smallest one that can exist:
//!
//! * **Diagnostic only.** A store hands its handle to
//!   [`crate::status::probes::BackendProbe`], which is reachable from the status
//!   refresher alone ([`crate::status::registry::ComponentProbe`]). No request
//!   handler can call [`BackendHealth::check`], because no request handler can
//!   reach a probe.
//! * **No tenant input and no tenant output.** A check takes no key, so it
//!   cannot read one namespace's spend or one subject's `jti`, and it returns a
//!   [`FailureCategory`] plus an operator-facing detail that is logged and
//!   dropped — never a value, never a key, never a DSN
//!   ([ADR 0031](../../../../docs/adr/0031-bounded-status-contract.md)).
//! * **Bounded by the store's own configuration.** [`BackendHealth::bound`] is
//!   how long the store is entitled to take, so the probe cannot manufacture a
//!   timeout for a call the store would have completed.
//!
//! A store with no remote dependency — `none`, `in-memory` — returns `None` from
//! its `health()` accessor and its component reports `disabled`, which is the
//! honest answer: there is nothing to be unreachable.

use std::time::Duration;

use async_trait::async_trait;
use tokio_postgres::Config;

use super::FailureCategory;

/// Why one reachability check failed, in the bounded vocabulary status projects
/// from.
#[derive(Debug)]
pub struct HealthFailure {
    category: FailureCategory,
    detail: String,
}

impl HealthFailure {
    pub fn new(category: FailureCategory, detail: impl Into<String>) -> Self {
        Self {
            category,
            detail: detail.into(),
        }
    }

    /// The store could not be reached: the common case, and the only retryable
    /// one.
    pub fn unavailable(detail: impl Into<String>) -> Self {
        Self::new(FailureCategory::Unavailable, detail)
    }

    pub fn category(&self) -> FailureCategory {
        self.category
    }

    /// The operator-facing text. Logged once by
    /// [`crate::status::registry::CachedStatusRegistry::publish`] and never
    /// projected into a response, so it may name the backend but must not carry
    /// a DSN, a credential, or a tenant identifier.
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

/// A request-path store's reachability, asked from the status refresher only.
#[async_trait]
pub trait BackendHealth: Send + Sync {
    /// The backend implementation's name, as its `name()` reports it — `redis`,
    /// `postgres`. Never a URL.
    fn backend(&self) -> &'static str;

    /// The longest one check may take before the refresher is entitled to call
    /// it a timeout, derived from the store's configured bounds rather than
    /// chosen: a probe cut off before those have elapsed does not observe an
    /// outage, it invents one.
    fn bound(&self) -> Duration;

    /// Ask the store whether it is reachable. Takes no key and returns no data.
    async fn check(&self) -> Result<(), HealthFailure>;
}

/// Reachability of a Postgres-backed request-path store, on a connection of the
/// probe's own.
///
/// The request-path Postgres stores serialise their work on one client behind a
/// mutex. Probing *through* that mutex would put diagnostic work in front of a
/// request holding it, which is exactly the "status must not influence
/// inference" rule the cached registry exists to keep. So the check opens its
/// own short-lived session from the same parsed [`Config`] — the same host,
/// role, TLS, and connect bound the store itself uses — and closes it again.
/// What it gives up is detecting a *saturated* store; what it keeps is that the
/// diagnostic can never be the reason a request waited.
pub struct PostgresHealth {
    backend: &'static str,
    config: Config,
    bound: Duration,
}

impl PostgresHealth {
    pub fn new(backend: &'static str, config: Config, bound: Duration) -> Self {
        Self {
            backend,
            config,
            bound,
        }
    }
}

#[async_trait]
impl BackendHealth for PostgresHealth {
    fn backend(&self) -> &'static str {
        self.backend
    }

    fn bound(&self) -> Duration {
        self.bound
    }

    async fn check(&self) -> Result<(), HealthFailure> {
        let (client, connection) = self
            .config
            .connect(crate::usage::tls_connector())
            .await
            .map_err(|error| classify(&error))?;
        // The connection future drives the socket; it ends when the client is
        // dropped at the end of this call, so the probe leaves nothing behind.
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        let queried = client.simple_query("SELECT 1").await;
        drop(client);
        driver.abort();
        queried.map(|_| ()).map_err(|error| classify(&error))
    }
}

/// Map a driver error onto the bounded vocabulary. Authentication and
/// permission refusals are separated from unreachability because they need a
/// different operator: a rotated password is not an outage of Postgres, and
/// paging the storage owner for it wastes the page.
fn classify(error: &tokio_postgres::Error) -> HealthFailure {
    use tokio_postgres::error::SqlState;
    let category = match error.code() {
        Some(code)
            if *code == SqlState::INVALID_PASSWORD
                || *code == SqlState::INVALID_AUTHORIZATION_SPECIFICATION
                || *code == SqlState::INSUFFICIENT_PRIVILEGE =>
        {
            FailureCategory::Denied
        }
        _ => FailureCategory::Unavailable,
    };
    HealthFailure::new(category, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_unreachable_postgres_is_unavailable_and_names_no_dsn() {
        let config: Config = "host=127.0.0.1 port=1 user=axond connect_timeout=1"
            .parse()
            .expect("parsable DSN");
        let health = PostgresHealth::new("postgres", config, Duration::from_secs(2));
        let failure = health.check().await.expect_err("port 1 refuses");
        assert_eq!(failure.category(), FailureCategory::Unavailable);
        assert!(!failure.detail().contains("user=axond"), "{failure:?}");
    }
}
