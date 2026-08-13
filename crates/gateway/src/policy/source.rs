//! The handle a request-path store reads its caps through.
//!
//! A store is a *mechanism*: Redis holds counters and runs a script, Postgres
//! holds rows and takes a lock. What number that mechanism enforces is policy,
//! and after #150 it can change while the process runs — so a store reads it per
//! request rather than capturing it at construction.
//!
//! Two sources, and the distinction is the whole point of the type: a fixed one
//! for a stateless deployment (and for tests, where a cap is a constant of the
//! scenario), and a live one for a stateful deployment, where the caps follow the
//! [`PolicyRuntime`] the convergence pipeline publishes into. Both answer the same
//! question — *what governs this namespace right now* — so a store's enforcement
//! code has one path, not one per mode.

use std::sync::Arc;

use crate::desired_state::policy::PolicyGeneration;

use super::{ActivePolicy, PolicyRuntime};

/// Where a request-path store reads the caps it enforces.
#[derive(Debug, Clone)]
pub struct Ceilings(Source);

#[derive(Debug, Clone)]
enum Source {
    /// One policy, for the life of the process: the bootstrap file's, in a
    /// stateless deployment.
    Fixed(ActivePolicy),
    /// Whatever the runtime is enforcing now.
    Published(Arc<PolicyRuntime>),
}

impl Ceilings {
    /// Caps that never change, and that no generation drains.
    pub const fn fixed(policy: ActivePolicy) -> Self {
        Self(Source::Fixed(policy))
    }

    /// Caps that follow what the control plane publishes.
    pub fn published(runtime: &Arc<PolicyRuntime>) -> Self {
        Self(Source::Published(Arc::clone(runtime)))
    }

    /// What governs `namespace` for the request being admitted right now.
    ///
    /// Read once per admission and carried through that request, so a
    /// publication landing mid-request cannot settle a hold against a cap that
    /// was not the one it was granted under.
    pub fn active(&self, namespace: &str) -> ActivePolicy {
        match &self.0 {
            Source::Fixed(policy) => *policy,
            Source::Published(runtime) => runtime.active(namespace),
        }
    }

    /// Record a hold taken under `generation`.
    pub fn enter(&self, generation: Option<PolicyGeneration>) {
        if let Source::Published(runtime) = &self.0 {
            runtime.enter(generation);
        }
    }

    /// Record that a hold taken under `generation` is finished.
    pub fn exit(&self, generation: Option<PolicyGeneration>) {
        if let Source::Published(runtime) = &self.0 {
            runtime.exit(generation);
        }
    }
}

/// An outstanding hold, counted against the generation that granted it for as
/// long as it lives.
///
/// A rate-limit lease is released by dropping its permit, on every path — including
/// a cancelled request — so its drain accounting is a drop guard rather than an
/// explicit call the request path could miss.
///
/// A store takes one *before* the round-trip that admits, not after: an operator
/// treats an empty drain list as proof that nothing is still running under the
/// replaced document, so the count has to over-report an admission that is about
/// to be denied rather than miss one that succeeded while a publication landed.
/// A budget reservation settles explicitly, so its store calls [`PolicyHold::kept`]
/// once admitted and pairs the count with [`Ceilings::exit`] at settlement.
///
/// The count outlives the *request*, not just the caller: a shared-store acquire
/// that overran its caller's wait may still have written a lease, so the
/// in-flight round trip and the compensating release that removes such a lease
/// each hold the generation too. An empty drain list therefore means no request
/// is running under the generation *and* nothing it admitted is left in the
/// store — which is what a stop-the-fleet migration needs it to mean.
#[derive(Debug)]
pub struct PolicyHold {
    ceilings: Ceilings,
    generation: Option<PolicyGeneration>,
    kept: bool,
}

impl PolicyHold {
    /// Count a hold against `generation` until this value is dropped.
    pub fn take(ceilings: &Ceilings, generation: Option<PolicyGeneration>) -> Self {
        ceilings.enter(generation);
        Self {
            ceilings: ceilings.clone(),
            generation,
            kept: false,
        }
    }

    /// Hand the count off to an explicit settlement path: the generation stays
    /// counted after this guard goes away.
    pub fn kept(mut self) {
        self.kept = true;
    }
}

impl Drop for PolicyHold {
    fn drop(&mut self) {
        if !self.kept {
            self.ceilings.exit(self.generation);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::desired_state::fixtures::tenant_id;
    use crate::desired_state::policy::PolicyScope;
    use crate::policy::fixtures::{body, generation};
    use crate::policy::view::tests::stateless_config;

    #[test]
    fn fixed_ceilings_answer_the_same_policy_for_every_namespace_and_drain_nothing() {
        let policy = ActivePolicy::default();
        let ceilings = Ceilings::fixed(policy);
        assert_eq!(ceilings.active("anything"), policy);

        // A fixed source has no runtime to account against; entering a hold is
        // not an error, it is simply nothing to drain.
        let orphan = generation(&body(PolicyScope::Tenant(tenant_id(1)), 1, 10), 1);
        ceilings.enter(Some(orphan));
        ceilings.exit(Some(orphan));
    }

    #[test]
    fn published_ceilings_account_holds_against_the_runtime_they_read() {
        let runtime = Arc::new(PolicyRuntime::bootstrap(&stateless_config()));
        let ceilings = Ceilings::published(&runtime);
        let held = generation(&body(PolicyScope::Tenant(tenant_id(1)), 1, 10), 1);

        ceilings.enter(Some(held));
        assert_eq!(runtime.outstanding(held), 1);
        ceilings.exit(Some(held));
        assert_eq!(runtime.outstanding(held), 0);
    }

    #[test]
    fn a_hold_taken_before_a_store_call_survives_only_the_admission_that_keeps_it() {
        let runtime = Arc::new(PolicyRuntime::bootstrap(&stateless_config()));
        let ceilings = Ceilings::published(&runtime);
        let held = generation(&body(PolicyScope::Tenant(tenant_id(1)), 1, 10), 1);

        // A store takes the hold before the round-trip; a denial drops it.
        let denied = PolicyHold::take(&ceilings, Some(held));
        assert_eq!(runtime.outstanding(held), 1);
        drop(denied);
        assert_eq!(runtime.outstanding(held), 0);

        // An admission that settles explicitly keeps the count and releases it
        // at settlement, so exactly one exit answers the one entry.
        let admitted = PolicyHold::take(&ceilings, Some(held));
        admitted.kept();
        assert_eq!(runtime.outstanding(held), 1);
        ceilings.exit(Some(held));
        assert_eq!(runtime.outstanding(held), 0);
    }
}
