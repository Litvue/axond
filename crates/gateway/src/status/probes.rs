//! The probes that fill the registry, and the only place a backend is asked
//! how it is.
//!
//! A probe is deliberately thin: it calls the one reachability method its
//! backend already offers for diagnostics, and turns whatever comes back into a
//! [`ComponentObservation`] — a state, a code from the closed
//! [`StatusReason`] vocabulary, and an operator-facing detail that is logged and
//! never projected into a response. Nothing here retries, caches, or interprets:
//! the refresher paces it, the registry ages it, and
//! [`crate::status::StatusResponse`] decides who may see what.
//!
//! Only components a deployment actually *has* get a probe. Everything else
//! reports `disabled`, which is why a stateless replica still answers the
//! diagnostic without ever touching a network.

use std::sync::Arc;

use async_trait::async_trait;

use super::registry::ComponentProbe;
use super::{Component, ComponentObservation, StatusReason};
use crate::backends::BackendFailure;
use crate::backends::control_plane::ControlPlaneStore;

/// Observes the control plane a stateful replica administers against.
///
/// It shares the store the administrative surface was built on rather than
/// opening its own connection: a second pool would make the diagnostic report on
/// a path no administrative request uses, which is the failure mode where status
/// says `ok` through an outage of the thing being asked about.
pub struct ControlPlaneProbe {
    store: Arc<dyn ControlPlaneStore>,
}

impl ControlPlaneProbe {
    pub fn new(store: Arc<dyn ControlPlaneStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ComponentProbe for ControlPlaneProbe {
    fn component(&self) -> Component {
        Component::ControlPlane
    }

    async fn observe(&self) -> ComponentObservation {
        match self.store.health().await {
            Ok(()) => ComponentObservation::ok(Component::ControlPlane),
            // Classified through the backend's own category rather than by
            // matching its variants: a store that grows a failure mode gets a
            // safe code here instead of silently falling through to `ok`.
            Err(error) => {
                let reason = StatusReason::from_failure(error.category());
                // A control plane that answers "denied" or "corrupt" is reachable
                // and wrong, which an operator triages differently from one that
                // is not there at all.
                let detail = format!("{}: {error}", self.store.name());
                if reason == StatusReason::Unreachable {
                    ComponentObservation::unavailable(Component::ControlPlane, reason, detail)
                } else {
                    ComponentObservation::degraded(Component::ControlPlane, reason, detail)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::Capabilities;
    use crate::backends::control_plane::ControlPlaneError;
    use crate::desired_state::oracle::InMemoryControlPlane;
    use crate::desired_state::{
        AccessDenial, AuditEvent, DenialPage, LoadedRevision, RevisionCandidate, RevisionId,
        RevisionManifest,
    };
    use crate::status::ComponentState;

    type Health = Box<dyn Fn() -> Result<(), ControlPlaneError> + Send + Sync>;

    /// The in-memory oracle with a `health` answer of the test's choosing, so
    /// each failure category can be classified without a database. Built as a
    /// closure because [`ControlPlaneError`] is not `Clone`.
    struct Answering {
        inner: Arc<InMemoryControlPlane>,
        health: Health,
    }

    #[async_trait]
    impl ControlPlaneStore for Answering {
        fn name(&self) -> &'static str {
            self.inner.name()
        }

        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }

        async fn health(&self) -> Result<(), ControlPlaneError> {
            (self.health)()
        }

        async fn desired_revision(&self) -> Result<Option<RevisionId>, ControlPlaneError> {
            self.inner.desired_revision().await
        }

        async fn load_manifest(
            &self,
            id: RevisionId,
        ) -> Result<RevisionManifest, ControlPlaneError> {
            self.inner.load_manifest(id).await
        }

        async fn load_revision(&self, id: RevisionId) -> Result<LoadedRevision, ControlPlaneError> {
            self.inner.load_revision(id).await
        }

        async fn publish_revision(
            &self,
            candidate: RevisionCandidate,
        ) -> Result<RevisionManifest, ControlPlaneError> {
            self.inner.publish_revision(candidate).await
        }

        async fn audit_trail(&self, id: RevisionId) -> Result<Vec<AuditEvent>, ControlPlaneError> {
            self.inner.audit_trail(id).await
        }

        async fn record_denial(&self, denial: &AccessDenial) -> Result<(), ControlPlaneError> {
            self.inner.record_denial(denial).await
        }

        async fn denials(
            &self,
            page: &DenialPage,
            limit: usize,
        ) -> Result<Vec<AccessDenial>, ControlPlaneError> {
            self.inner.denials(page, limit).await
        }
    }

    fn probing(health: Health) -> ControlPlaneProbe {
        ControlPlaneProbe::new(Arc::new(Answering {
            inner: Arc::new(InMemoryControlPlane::new()),
            health,
        }))
    }

    fn healthy() -> Health {
        Box::new(|| Ok(()))
    }

    fn failing(error: fn() -> ControlPlaneError) -> Health {
        Box::new(move || Err(error()))
    }

    #[tokio::test]
    async fn a_reachable_control_plane_is_ok_and_says_nothing_else() {
        let observation = probing(healthy()).observe().await;
        assert_eq!(observation.state, ComponentState::Ok);
        assert_eq!(observation.reason, None);
        // An `ok` with a detail would be a log line per component per round.
        assert_eq!(observation.detail, None);
    }

    /// The distinction an operator acts on: a control plane that cannot be
    /// reached is an outage of the administrative path, while one that answers
    /// and refuses is a configuration or storage problem on a reachable
    /// dependency. Reporting both as `unavailable` would send the second one to
    /// the wrong runbook section.
    #[tokio::test]
    async fn unreachable_and_refusing_are_different_observations() {
        let unreachable = probing(failing(|| ControlPlaneError::Unavailable {
            backend: "postgres",
            message: "connection refused".to_owned(),
        }))
        .observe()
        .await;
        assert_eq!(unreachable.state, ComponentState::Unavailable);
        assert_eq!(unreachable.reason, Some(StatusReason::Unreachable));

        let refusing = probing(failing(|| ControlPlaneError::Denied {
            backend: "postgres",
            message: "permission denied for relation revisions".to_owned(),
        }))
        .observe()
        .await;
        assert_eq!(refusing.state, ComponentState::Degraded);
        assert_eq!(refusing.reason, Some(StatusReason::PermissionDenied));
    }

    /// The backend's message is for the log, and the response has nowhere to put
    /// it: every field of the projection is an enum or a number. This pins the
    /// half that is easy to lose — that the detail is *collected* — since the
    /// redaction half is enforced by the response types.
    #[tokio::test]
    async fn the_backend_message_stays_on_the_detail() {
        let observation = probing(failing(|| ControlPlaneError::Unavailable {
            backend: "postgres",
            message: "host=db.internal port=5432: connection refused".to_owned(),
        }))
        .observe()
        .await;
        let detail = observation.detail.expect("a failure carries a detail");
        assert!(detail.contains("connection refused"), "{detail}");
    }
}
