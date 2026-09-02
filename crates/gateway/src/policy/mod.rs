//! Enforcing published policy at runtime (#150).
//!
//! [`desired_state::policy`](crate::desired_state::policy) says what a scope's
//! policy *is*; this module is what a replica does with it. It holds the values
//! the request path enforces — the spend caps, the hold lifetime, the concurrency
//! ceiling — behind one atomically replaceable [`PolicyView`], so a publication
//! changes what the fleet enforces without a deploy and without rebuilding a
//! single connection.
//!
//! # What is dynamic, and what is not
//!
//! Dynamic (control-plane owned, replaced per revision): the subject spend cap,
//! the scope-wide spend cap's *value*, the reservation TTL, the per-subject
//! concurrency ceiling, the lease TTL, and the minted-token floor.
//!
//! Not dynamic (bootstrap owned, fixed for the life of a process): which backend
//! enforces each responsibility, the DSN it is reached through, the key prefix or
//! table it lays state out under, whether that layout carries a scope-wide cap at
//! all, and what happens when the backend is unreachable
//! ([`BOOTSTRAP_OWNED_FIELDS`](crate::desired_state::policy::BOOTSTRAP_OWNED_FIELDS)).
//! Those are connection and layout facts a running replica cannot change under
//! its own outstanding holds, so a publication that needs one changed is refused
//! here and performed by an operator with a documented procedure
//! (`docs/operations/policy-activation.md`).
//!
//! # The control plane is not a hot path
//!
//! Nothing in this module reads control-plane Postgres. Convergence compiles a
//! revision off the request path and hands the result here; enforcement then runs
//! entirely against whichever responsibility-specific backend the bootstrap file
//! selected. A deployment whose control plane is down keeps enforcing the last
//! view it installed, which is the same last-known-good posture the rest of
//! convergence has.
//!
//! # Generations, and why a hold keeps its own
//!
//! Every admitted request records the [`PolicyGeneration`] it was admitted under
//! ([`Reservation::generation`](crate::budget::Reservation::generation),
//! [`RateLimitPermit`](crate::rate_limit::RateLimitPermit)), and settles against
//! that generation rather than against whatever is active when it finishes. A
//! publication therefore binds from the next admission and never rewrites,
//! re-prices, or invalidates what is already held — which is exactly what
//! [`TransitionClass::Drain`](crate::desired_state::policy::TransitionClass)
//! promises, and what makes a rollback safe: the older
//! document is republished forward, new admissions bind to it, and the holds taken
//! under the higher caps run to completion on the terms they were granted.
//!
//! [`PolicyRuntime`] counts those outstanding holds per generation, so "the drain
//! has finished" is an observable fact rather than a guess.

mod activation;
mod source;
pub(crate) mod ungoverned;
pub(crate) mod view;

pub use activation::{Activation, ActivationRefusal, BackendSupport};
pub use source::{Ceilings, PolicyHold};
pub(crate) use ungoverned::{Unenforceable, denied};
pub use view::{ActivePolicy, BudgetCaps, ConcurrencyCaps, PolicyView};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;

use arc_swap::ArcSwap;

use crate::config::Config;
use crate::desired_state::policy::PolicyGeneration;

/// The policy a replica is enforcing, and the holds outstanding under it.
///
/// One per process, shared by the stores that enforce it and by the convergence
/// pipeline that replaces it. Replacement is a single atomic store, so a request
/// reads one whole coherent view or the previous one, never a half-applied mix.
#[derive(Debug)]
pub struct PolicyRuntime {
    /// What this process's backends can enforce. A boot fact: changing it means
    /// changing the bootstrap file and restarting.
    support: BackendSupport,
    view: ArcSwap<PolicyView>,
    /// Outstanding holds and leases, by the generation that admitted them.
    ///
    /// The unit of "may this drain be considered finished": a superseded
    /// generation with a non-zero count still has work running under the terms it
    /// granted.
    holds: Mutex<HashMap<PolicyGeneration, u64>>,
    /// Holds that nothing will ever settle, and the deadline they are released
    /// on — one entry per generation, not one per hold.
    ///
    /// A store outage produces one of these per failed reserve, so waiting each
    /// one out on its own timer would be a task per request for a whole
    /// reservation TTL, precisely when the deployment is already unhealthy. They
    /// all wait out the same TTL, so they share one timer and the last one in
    /// pushes the deadline out.
    lingering: Mutex<HashMap<PolicyGeneration, Lingering>>,
}

/// Holds under one generation that are waiting out a store's reservation TTL.
#[derive(Debug)]
struct Lingering {
    held: u64,
    until: Instant,
}

impl PolicyRuntime {
    /// The runtime a process boots with: whatever the bootstrap file states, and
    /// no published document.
    ///
    /// In stateless mode that is the whole story — the file's limits are the
    /// policy, forever, and nothing about enforcement changes from what it was
    /// before this module existed.
    pub fn bootstrap(config: &Config) -> Self {
        Self {
            support: BackendSupport::of(config),
            view: ArcSwap::from_pointee(PolicyView::of(config)),
            holds: Mutex::new(HashMap::new()),
            lingering: Mutex::new(HashMap::new()),
        }
    }

    /// The policy governing `namespace` right now.
    pub fn active(&self, namespace: &str) -> ActivePolicy {
        self.view.load().policy(namespace)
    }

    /// Whether `candidate` may be activated on this replica, and what activating
    /// it costs.
    ///
    /// Pure: it answers from the active view without touching it, which is what
    /// lets convergence run this as a gate *before* anything is published (see
    /// [`RevisionCompiler`](crate::convergence::RevisionCompiler)).
    pub fn plan(&self, candidate: &PolicyView) -> Result<Activation, ActivationRefusal> {
        activation::plan(&self.view.load(), candidate, self.support)
    }

    /// Install `candidate` as the policy this replica enforces from the next
    /// admission on.
    ///
    /// Infallible by design. Publication installs this view after durable
    /// namespace seed and before the snapshot swap, so a request never observes
    /// a new snapshot under the previous policy. The refusal happens at the
    /// compile gate, where a candidate can still be dropped without touching a
    /// serving replica. Reaching a refusal here would mean the snapshot being
    /// installed disagrees with the policy view derived from it, so it is logged
    /// loudly and the view is still installed — a replica must not serve a
    /// configuration under a policy it does not hold.
    pub fn install(&self, candidate: PolicyView) -> Activation {
        let activation = match self.plan(&candidate) {
            Ok(activation) => activation,
            Err(refusal) => {
                tracing::error!(
                    %refusal,
                    "a policy view that the compile gate admitted was refused at installation; \
                     installing it anyway to keep the snapshot and the policy it is served under \
                     consistent"
                );
                Activation::forced()
            }
        };
        self.view.store(std::sync::Arc::new(candidate));
        activation.log(&self.draining());
        activation
    }

    /// Record a hold admitted under `generation`.
    ///
    /// `None` — a namespace whose policy is the bootstrap file's — is not counted:
    /// there is no generation to drain.
    pub fn enter(&self, generation: Option<PolicyGeneration>) {
        let Some(generation) = generation else { return };
        *self
            .holds
            .lock()
            .expect("not poisoned")
            .entry(generation)
            .or_insert(0) += 1;
    }

    /// Record that a hold admitted under `generation` has been settled, released,
    /// or dropped.
    pub fn exit(&self, generation: Option<PolicyGeneration>) {
        let Some(generation) = generation else { return };
        let mut holds = self.holds.lock().expect("not poisoned");
        if let Some(count) = holds.get_mut(&generation) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                holds.remove(&generation);
            }
        }
    }

    /// Keep a hold already entered under `generation` counted for `ttl`, then
    /// release it, without holding up the caller.
    ///
    /// For the reserve whose answer was lost: nothing will settle it, so the only
    /// honest release is the deadline the store itself reclaims the entry on. One
    /// timer serves every such hold under a generation, and a later one extends
    /// the shared deadline rather than adding a timer of its own — conservative
    /// in the safe direction, since a hold released late only delays a drain.
    pub fn linger(self: &Arc<Self>, generation: PolicyGeneration, ttl: Duration) {
        let until = Instant::now() + ttl;
        let mut lingering = self.lingering.lock().expect("not poisoned");
        if let Some(waiting) = lingering.get_mut(&generation) {
            waiting.held += 1;
            waiting.until = waiting.until.max(until);
            return;
        }
        lingering.insert(generation, Lingering { held: 1, until });
        drop(lingering);

        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            // No runtime to outlive the caller on (a synchronous test, or
            // shutdown): releasing now is all this thread can do, and it is what
            // a process that is going away would do anyway.
            self.release_lingering(generation);
            return;
        };
        let runtime = Arc::clone(self);
        handle.spawn(async move {
            while let Some(until) = runtime.release_lingering_if_expired(generation) {
                tokio::time::sleep_until(until).await;
            }
        });
    }

    /// Release the holds waiting under `generation` if their deadline has
    /// passed, or answer the deadline still to wait for.
    ///
    /// One lock acquisition decides *and* removes, because the two cannot be
    /// separated: between a timer deciding the deadline had passed and removing
    /// the entry, another failed reserve would join the entry it can still see —
    /// adding a hold and a later deadline that the removal would then throw away,
    /// reporting a drain finished over spend the store may still hold.
    fn release_lingering_if_expired(&self, generation: PolicyGeneration) -> Option<Instant> {
        let held = {
            let mut lingering = self.lingering.lock().expect("not poisoned");
            let waiting = lingering.get(&generation)?;
            if Instant::now() < waiting.until {
                return Some(waiting.until);
            }
            lingering.remove(&generation)?.held
        };
        for _ in 0..held {
            self.exit(Some(generation));
        }
        None
    }

    /// Exit every hold waiting out `generation`'s reservation TTL, deadline or
    /// no deadline: for the caller that has no runtime to wait on.
    fn release_lingering(&self, generation: PolicyGeneration) {
        let Some(waiting) = self
            .lingering
            .lock()
            .expect("not poisoned")
            .remove(&generation)
        else {
            return;
        };
        for _ in 0..waiting.held {
            self.exit(Some(generation));
        }
    }

    /// How many holds are outstanding under `generation`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn outstanding(&self, generation: PolicyGeneration) -> u64 {
        self.holds
            .lock()
            .expect("not poisoned")
            .get(&generation)
            .copied()
            .unwrap_or_default()
    }

    /// Every superseded generation that still has work running under it.
    ///
    /// Empty means every drain this replica knows about has finished: nothing
    /// admitted under a replaced document is still in flight *here*. It says
    /// nothing about other replicas, which is why the operational procedure is a
    /// fleet-wide check rather than one replica's answer.
    pub fn draining(&self) -> Vec<(PolicyGeneration, u64)> {
        let view = self.view.load();
        let mut draining: Vec<(PolicyGeneration, u64)> = self
            .holds
            .lock()
            .expect("not poisoned")
            .iter()
            .filter(|(generation, _)| !view.enforces(**generation))
            .map(|(generation, count)| (*generation, *count))
            .collect();
        // Ordered so two reads of one state produce one answer.
        draining.sort_by_key(|(generation, _)| {
            (
                generation.scope(),
                generation.epoch().get(),
                generation.content().to_string(),
            )
        });
        draining
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    use crate::desired_state::fixtures::revision_id;
    use crate::desired_state::policy::{
        BudgetPolicy, ConcurrencyPolicy, PolicyBody, PolicyEpoch, PolicyGeneration, PolicyScope,
        RevocationPolicy,
    };

    /// A policy body for `scope`, at `epoch`, with caps a test can vary.
    pub(crate) fn body(scope: PolicyScope, epoch: u64, subject_limit: u64) -> PolicyBody {
        detailed(scope, epoch, subject_limit, None, 300, 8, 60, 0)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn detailed(
        scope: PolicyScope,
        epoch: u64,
        subject_limit: u64,
        namespace_limit: Option<u64>,
        reservation_ttl_seconds: u64,
        max_in_flight: u64,
        lease_ttl_seconds: u64,
        minimum_token_epoch: u64,
    ) -> PolicyBody {
        PolicyBody::new(
            scope,
            PolicyEpoch::new(epoch).expect("a positive epoch"),
            BudgetPolicy::new(subject_limit, namespace_limit, reservation_ttl_seconds)
                .expect("a positive reservation ttl"),
            ConcurrencyPolicy::new(max_in_flight, lease_ttl_seconds)
                .expect("a positive concurrency policy"),
            RevocationPolicy::new(minimum_token_epoch),
        )
    }

    /// A body an earlier build stored with a cap of zero, which
    /// [`BudgetPolicy::new`] refuses to build and
    /// [`BudgetPolicy::stored`] reads back.
    pub(crate) fn stored_zero_cap(
        scope: PolicyScope,
        epoch: u64,
        subject_limit: u64,
        namespace_limit: Option<u64>,
    ) -> PolicyBody {
        PolicyBody::new(
            scope,
            PolicyEpoch::new(epoch).expect("a positive epoch"),
            BudgetPolicy::stored(subject_limit, namespace_limit, 300)
                .expect("a stored cap of zero reads back"),
            ConcurrencyPolicy::new(8, 60).expect("a positive concurrency policy"),
            RevocationPolicy::new(0),
        )
    }

    pub(crate) fn generation(body: &PolicyBody, revision: u64) -> PolicyGeneration {
        body.generation(revision_id(revision))
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::{body, generation};
    use super::*;
    use crate::desired_state::fixtures::tenant_id;
    use crate::desired_state::policy::PolicyScope;

    fn scope() -> PolicyScope {
        PolicyScope::Tenant(tenant_id(1))
    }

    fn runtime() -> PolicyRuntime {
        PolicyRuntime::bootstrap(&crate::policy::view::tests::stateless_config())
    }

    #[test]
    fn a_hold_is_outstanding_until_it_exits_and_only_then_stops_draining() {
        let runtime = runtime();
        let first = generation(&body(scope(), 1, 1_000), 1);
        runtime.enter(Some(first));
        runtime.enter(Some(first));
        assert_eq!(runtime.outstanding(first), 2);

        runtime.exit(Some(first));
        assert_eq!(runtime.outstanding(first), 1);
        runtime.exit(Some(first));
        assert_eq!(runtime.outstanding(first), 0);
        assert!(runtime.draining().is_empty());
    }

    /// The property a rollback and a mixed-version rollout both depend on: what
    /// was admitted under the superseded document is still counted under *its*
    /// generation after the replacement is installed, so nothing rewrites the
    /// terms a running request was granted.
    #[test]
    fn a_hold_survives_the_installation_that_supersedes_its_generation() {
        use crate::config::NamespacePolicy;
        use crate::policy::view::tests::governed;

        let old = body(
            PolicyScope::Project {
                tenant: tenant_id(1),
                project: crate::desired_state::fixtures::project_id(1),
            },
            1,
            10_000,
        );
        let held = generation(&old, 1);
        let runtime = PolicyRuntime::bootstrap(&governed(
            "acme/core",
            NamespacePolicy {
                body: old.clone(),
                generation: held,
            },
        ));
        runtime.enter(Some(held));

        // The rollback: the previous values, published forward under a new epoch.
        let rolled_back =
            crate::policy::fixtures::detailed(old.scope(), 2, 1_000, None, 300, 8, 60, 0);
        let next = generation(&rolled_back, 2);
        runtime.install(PolicyView::of(&governed(
            "acme/core",
            NamespacePolicy {
                body: rolled_back,
                generation: next,
            },
        )));

        // New admissions see the new cap...
        let active = runtime.active("acme/core");
        assert_eq!(active.budget.expect("governed").subject_microdollars, 1_000);
        assert_eq!(active.generation, Some(next));
        // ...and the hold taken under the old one is still draining under it.
        assert_eq!(runtime.outstanding(held), 1);
        assert_eq!(runtime.draining(), vec![(held, 1)]);

        runtime.exit(Some(held));
        assert!(runtime.draining().is_empty());
    }

    /// A store outage fails every request in flight, and each failure may have
    /// left a reservation nothing can settle. They all wait out the same TTL, so
    /// they wait on one timer: the count is exact, the later deadline wins, and
    /// the number of tasks does not follow the request rate.
    #[tokio::test(start_paused = true)]
    async fn holds_lingering_under_one_generation_share_a_single_deadline() {
        let runtime = std::sync::Arc::new(runtime());
        let held = generation(&body(scope(), 1, 1_000), 1);
        let ttl = Duration::from_secs(300);

        for _ in 0..1_000 {
            runtime.enter(Some(held));
            runtime.linger(held, ttl);
        }
        assert_eq!(runtime.outstanding(held), 1_000);
        assert_eq!(
            runtime.lingering.lock().expect("not poisoned").len(),
            1,
            "a thousand failed reserves waited on a thousand timers"
        );

        // A later failure extends the shared deadline rather than releasing the
        // earlier holds with it: at the first deadline nothing has expired yet.
        tokio::time::sleep(ttl - Duration::from_secs(1)).await;
        runtime.enter(Some(held));
        runtime.linger(held, ttl);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(runtime.outstanding(held), 1_001);

        tokio::time::sleep(ttl).await;
        assert_eq!(runtime.outstanding(held), 0);
        assert!(runtime.lingering.lock().expect("not poisoned").is_empty());
    }

    /// A failure arriving at the deadline the shared timer is firing on joins
    /// the *next* wait or starts one, and is never swept out with the batch it
    /// missed: deciding the deadline has passed and removing the entry is one
    /// step, so nothing can be added in between and released early.
    #[tokio::test(start_paused = true)]
    async fn a_hold_lingering_at_the_deadline_still_waits_out_its_own_ttl() {
        let runtime = std::sync::Arc::new(runtime());
        let held = generation(&body(scope(), 1, 1_000), 1);
        let ttl = Duration::from_secs(300);

        runtime.enter(Some(held));
        runtime.linger(held, ttl);
        tokio::time::sleep(ttl).await;

        // Whether the timer has already swept the first one or is about to, the
        // second joins a live entry or opens a new one — either way it owes a
        // full TTL from now, and nothing releases it with the batch it missed.
        runtime.enter(Some(held));
        runtime.linger(held, ttl);

        tokio::time::sleep(ttl - Duration::from_secs(1)).await;
        assert!(
            runtime.outstanding(held) >= 1,
            "a hold taken at the deadline was released with the batch before it"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(runtime.outstanding(held), 0);
    }

    /// A namespace served under the bootstrap file has no generation, so it has
    /// nothing to drain and cannot be made to look like it does.
    #[test]
    fn a_bootstrap_hold_is_not_counted() {
        let runtime = runtime();
        runtime.enter(None);
        runtime.exit(None);
        assert!(runtime.draining().is_empty());
    }
}
