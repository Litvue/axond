//! Deciding whether a replica may start enforcing a candidate policy.
//!
//! Three questions, in order, and a candidate has to answer all three before any
//! of it is activated:
//!
//! 1. **Can these backends enforce it at all?** A cap is a fleet-wide statement,
//!    so the store that enforces it has to be one every replica shares. Redis and
//!    Postgres qualify for spend, Redis qualifies for concurrency, and the
//!    per-replica and no-op backends qualify for neither: they would report a
//!    published cap as enforced while each replica enforced its own copy of it.
//! 2. **Does it fit the layout this process booted on?** Whether a store's keys
//!    carry a scope-wide cap is a durable layout fact with a migration behind it,
//!    so a document that turns one on or off is refused *before* publication, with
//!    the procedure named, rather than half-enforced against keys laid out for the
//!    other shape.
//! 3. **Is the move from what is active a move this model performs?**
//!    [`PolicyTransition`] answers that, and this module turns its classes into
//!    outcomes: `Live` and `Drain` activate, `MigrationRequired` and `Refused` do
//!    not.
//!
//! A `Drain` activates because that is what the class means: what was admitted
//! under the looser document keeps its terms until it settles, and the new
//! document binds from the next admission. That is enforced structurally — a hold
//! carries the generation that granted it — so activating a drain cannot shorten
//! a lease someone is already holding.
//!
//! # Rolling back
//!
//! Rolling back is *publishing the old values forward*: a new document, a higher
//! epoch, the previous caps. It is classified like any other change (usually a
//! drain, since it lowers something), and the holds taken under the higher caps
//! finish on the terms they were granted. What is refused is walking the epoch
//! itself backwards — repointing a fleet at an older revision — because two
//! replicas would then disagree about which document one epoch names, and the
//! fence could no longer tell a stale writer from a current one. The refusal says
//! so, and names the generation it is enforcing.

use std::fmt::Write as _;

use crate::config::{BudgetBackend, Config, RateLimitBackend};
use crate::desired_state::policy::{
    Fenced, PolicyFence, PolicyGeneration, PolicyScope, PolicyTransition, TransitionClass,
    TransitionReason,
};

use super::view::PolicyView;

/// What the backends this process booted with are able to enforce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackendSupport {
    budget: BudgetBackend,
    /// Whether the budget store's keys are laid out to carry a scope-wide cap.
    /// Bootstrap-owned, because changing it is a migration over durable ledgers.
    namespace_scope: bool,
    rate_limit: RateLimitBackend,
}

impl BackendSupport {
    pub fn of(config: &Config) -> Self {
        Self {
            budget: config.budget.backend,
            namespace_scope: config.budget.enforces_namespace_scope(),
            rate_limit: config.rate_limit.backend,
        }
    }

    /// Whether spend is enforced against a store every replica shares.
    const fn shares_spend(self) -> bool {
        matches!(self.budget, BudgetBackend::Redis | BudgetBackend::Postgres)
    }

    /// Whether concurrency is enforced against leases every replica shares.
    const fn shares_concurrency(self) -> bool {
        matches!(self.rate_limit, RateLimitBackend::Redis)
    }

    /// Whether the budget store's layout carries a scope-wide cap.
    pub const fn namespace_scope(self) -> bool {
        self.namespace_scope
    }
}

/// Why a candidate policy is not activated.
///
/// Every arm is a refusal an operator acts on, and every one of them happens
/// before anything is enforced: the candidate is dropped, and the replica keeps
/// serving the policy it already had.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ActivationRefusal {
    /// The backends this process booted with cannot enforce a published value at
    /// all. A bootstrap change and a restart, not a publication.
    #[error("{scope} cannot be enforced by this deployment's backends: {detail}")]
    Unsupported { scope: PolicyScope, detail: String },
    /// Durable state has to be reconciled before the change means anything.
    #[error("{scope} needs a backend migration before it can be activated: {detail}")]
    Migration { scope: PolicyScope, detail: String },
    /// Not a transition this model performs.
    #[error("{scope} is not a policy transition this build performs: {detail}")]
    Refused { scope: PolicyScope, detail: String },
    /// A namespace that is still served, whose policy the candidate drops.
    #[error(
        "{scope} governs namespace `{namespace}`, which this candidate still serves without a \
         policy document: withdrawing a document from a served namespace would leave it with no \
         enforceable cap. Delete the namespace, or publish a document for it"
    )]
    Withdrawn {
        scope: PolicyScope,
        namespace: String,
    },
}

impl ActivationRefusal {
    /// A stable, low-cardinality label for metrics and log filtering.
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::Unsupported { .. } => "unsupported",
            Self::Migration { .. } => "migration",
            Self::Refused { .. } => "refused",
            Self::Withdrawn { .. } => "withdrawn",
        }
    }

    /// The scope whose document was refused.
    #[cfg_attr(not(test), allow(dead_code))]
    pub const fn scope(&self) -> PolicyScope {
        match self {
            Self::Unsupported { scope, .. }
            | Self::Migration { scope, .. }
            | Self::Refused { scope, .. }
            | Self::Withdrawn { scope, .. } => *scope,
        }
    }
}

/// What activating a candidate does to a replica.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Activation {
    /// Scopes whose new document binds immediately and strands nothing.
    live: Vec<PolicyScope>,
    /// Scopes whose new document binds from the next admission, with the reasons
    /// that make the old one worth draining.
    draining: Vec<(PolicyScope, Vec<TransitionReason>)>,
    /// Scopes whose document went away because the namespace it governed did.
    withdrawn: Vec<PolicyScope>,
}

impl Activation {
    /// The activation recorded when installation had to proceed past a refusal.
    /// Reported rather than silently empty, so the log says what happened.
    pub(super) const fn forced() -> Self {
        Self {
            live: Vec::new(),
            draining: Vec::new(),
            withdrawn: Vec::new(),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn live(&self) -> &[PolicyScope] {
        &self.live
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn draining(&self) -> &[(PolicyScope, Vec<TransitionReason>)] {
        &self.draining
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn withdrawn(&self) -> &[PolicyScope] {
        &self.withdrawn
    }

    /// Whether anything about what is enforced changed.
    pub fn is_noop(&self) -> bool {
        self.live.is_empty() && self.draining.is_empty() && self.withdrawn.is_empty()
    }

    /// Report the activation, and what is still running under a replaced
    /// document.
    ///
    /// At `info` because it is the record of a fleet-wide enforcement change: an
    /// operator reading a denial an hour later needs to know which generation was
    /// binding when it happened.
    pub(super) fn log(&self, outstanding: &[(PolicyGeneration, u64)]) {
        if self.is_noop() && outstanding.is_empty() {
            return;
        }
        let mut draining = String::new();
        for (scope, reasons) in &self.draining {
            let _ = write!(draining, "{scope} (");
            for (index, reason) in reasons.iter().enumerate() {
                let _ = write!(draining, "{}{reason:?}", if index == 0 { "" } else { ", " });
            }
            draining.push_str(") ");
        }
        tracing::info!(
            live = self.live.len(),
            draining = self.draining.len(),
            withdrawn = self.withdrawn.len(),
            draining_scopes = %draining.trim_end(),
            outstanding_generations = outstanding.len(),
            outstanding_holds = outstanding.iter().map(|(_, count)| count).sum::<u64>(),
            "policy activated; holds admitted under a replaced generation keep the terms they \
             were granted"
        );
    }
}

/// Classify a candidate against what is active, without touching either.
pub(super) fn plan(
    active: &PolicyView,
    candidate: &PolicyView,
    support: BackendSupport,
) -> Result<Activation, ActivationRefusal> {
    let mut activation = Activation::default();
    for (scope, published) in candidate.published() {
        supportable(
            *scope,
            published.body.budget().namespace_limit_microdollars(),
            support,
        )?;
        let Some(current) = active.published().get(scope) else {
            // Nothing was enforced for *this scope*, but its namespaces may have
            // been inheriting another one's document — a project publishing over
            // its tenant's. Report the drain that handover really is, judged
            // against the values that were binding. Only the drain: epochs are
            // per-scope, so a different scope's epoch is not this one's history
            // and cannot make the document stale, forked, or a layout change.
            let drains = published
                .namespaces
                .iter()
                .filter_map(|namespace| active.governing(namespace))
                .map(|inherited| inherited.displaced_by(&published.body))
                .find(|transition| transition.class() == TransitionClass::Drain);
            match drains {
                Some(transition) => activation
                    .draining
                    .push((*scope, transition.reasons().to_vec())),
                None => activation.live.push(*scope),
            }
            continue;
        };
        // `same_policy`, not equality: a generation names the revision that
        // published it, and every revision restates every document, so an
        // unrelated change would otherwise activate policy that never moved.
        if current.body == published.body && current.generation.same_policy(&published.generation) {
            continue;
        }
        let transition = current.body.transition(&published.body);
        match transition.class() {
            TransitionClass::Live => activation.live.push(*scope),
            TransitionClass::Drain => activation
                .draining
                .push((*scope, transition.reasons().to_vec())),
            TransitionClass::MigrationRequired => {
                return Err(ActivationRefusal::Migration {
                    scope: *scope,
                    detail: format!(
                        "{} changes the shape of what is stored, not only its values. Drain the \
                         fleet's holds for this scope, run the backend migration for the new \
                         layout, restart the fleet on a bootstrap that declares it, and publish \
                         the document again",
                        reasons(&transition)
                    ),
                });
            }
            TransitionClass::Refused => {
                return Err(ActivationRefusal::Refused {
                    scope: *scope,
                    detail: format!(
                        "{}{}",
                        reasons(&transition),
                        fenced(current.generation, published.generation)
                    ),
                });
            }
        }
    }
    for (scope, current) in active.published() {
        if candidate.published().contains_key(scope) {
            continue;
        }
        // A tenant document governs every one of its projects' namespaces, and
        // one of them still being served *uncapped* is enough: the others going
        // away does not make the survivor's cap withdrawable. A namespace the
        // candidate governs under a different document is a handover, not a
        // withdrawal — a project publishing over its tenant's document, or
        // dropping its own so the tenant's applies again, retires this scope
        // while the namespace stays capped.
        if let Some(served) = current
            .namespaces
            .iter()
            .find(|namespace| candidate.ungoverned(namespace))
        {
            return Err(ActivationRefusal::Withdrawn {
                scope: *scope,
                namespace: served.clone(),
            });
        }
        activation.withdrawn.push(*scope);
    }
    Ok(activation)
}

/// Whether these backends can enforce a document for `scope` at all, and whether
/// its scope-wide cap matches the layout they were built on.
fn supportable(
    scope: PolicyScope,
    namespace_limit: Option<u64>,
    support: BackendSupport,
) -> Result<(), ActivationRefusal> {
    if !support.shares_spend() {
        return Err(ActivationRefusal::Unsupported {
            scope,
            detail: format!(
                "a published spend cap is a fleet-wide statement, and `[budget] backend = \"{}\"` \
                 cannot make one: it enforces nothing, or enforces a separate copy per replica. \
                 Select `redis` or `postgres` in the bootstrap file and restart",
                support.budget.as_str()
            ),
        });
    }
    if !support.shares_concurrency() {
        return Err(ActivationRefusal::Unsupported {
            scope,
            detail: format!(
                "a published concurrency ceiling needs leases every replica shares, and \
                 `[rate_limit] backend = \"{}\"` has none. Select `redis` in the bootstrap file \
                 and restart",
                support.rate_limit.as_str()
            ),
        });
    }
    match (namespace_limit.is_some(), support.namespace_scope()) {
        (true, false) => Err(ActivationRefusal::Migration {
            scope,
            detail: "the document sets a scope-wide cap, but this deployment's budget store is \
                     laid out without one. Stop the fleet, set `[budget] namespace_scope = true`, \
                     migrate the ledgers (`axond budget migrate-redis`, or \
                     `ops/postgres/budget_v2.sql`), restart on that bootstrap, and publish the \
                     document again"
                .to_owned(),
        }),
        (false, true) => Err(ActivationRefusal::Migration {
            scope,
            detail: "this deployment's budget store is laid out to carry a scope-wide cap, and \
                     the document states none. Serving it would leave the scope-wide ledgers \
                     accumulating against nothing. Publish a document with \
                     `namespace_limit_microdollars`, or migrate the store back and restart the \
                     fleet without `[budget] namespace_scope`"
                .to_owned(),
        }),
        _ => Ok(()),
    }
}

fn reasons(transition: &PolicyTransition) -> String {
    let mut rendered = String::new();
    for (index, reason) in transition.reasons().iter().enumerate() {
        let _ = write!(rendered, "{}{reason:?}", if index == 0 { "" } else { ", " });
    }
    if rendered.is_empty() {
        rendered.push_str("no classified change");
    }
    rendered
}

/// The fence's reading of the same refusal, when it has one to add: which of
/// stale, forked, ahead, or another scope's document this is.
fn fenced(active: PolicyGeneration, candidate: PolicyGeneration) -> String {
    match PolicyFence::new(active).admit(candidate) {
        Ok(()) => String::new(),
        Err(error) => {
            let hint = match error {
                Fenced::Stale(_) => {
                    " Roll back by publishing the previous values forward under a higher epoch, \
                     not by repointing the fleet at an older revision"
                }
                Fenced::Forked(_) => {
                    " Two documents claim one epoch, which a restored backup or a second control \
                     plane produces. Reconcile them before publishing"
                }
                Fenced::Ahead(_) | Fenced::OtherScope(_) => "",
            };
            format!(" ({error}).{hint}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NamespacePolicy;
    use crate::desired_state::fixtures::tenant_id;
    use crate::policy::fixtures::{body, detailed, generation};
    use crate::policy::view::tests::{governed, stateful_config, stateless_config};

    fn scope() -> PolicyScope {
        PolicyScope::Tenant(tenant_id(1))
    }

    /// A stateful deployment whose budget is Redis and whose rate limiter is
    /// Redis: the shape a published document is enforceable on.
    fn shared() -> BackendSupport {
        let mut config = stateful_config();
        config.rate_limit.backend = RateLimitBackend::Redis;
        config.rate_limit.dsn_env = Some("GW_RATE_LIMIT_REDIS".to_owned());
        BackendSupport::of(&config)
    }

    fn view(document: &crate::desired_state::policy::PolicyBody, revision: u64) -> PolicyView {
        PolicyView::of(&governed(
            "acme/core",
            NamespacePolicy {
                body: *document,
                generation: generation(document, revision),
            },
        ))
    }

    fn empty() -> PolicyView {
        PolicyView::of(&stateful_config())
    }

    #[test]
    fn a_first_document_binds_live() {
        let document = body(scope(), 1, 1_000);
        let activation = plan(&empty(), &view(&document, 1), shared()).expect("enforceable");
        assert_eq!(activation.live(), [scope()]);
        assert!(activation.draining().is_empty());
    }

    #[test]
    fn raising_a_cap_is_live_and_lowering_one_drains() {
        let first = body(scope(), 1, 1_000);
        let raised = body(scope(), 2, 5_000);
        let lowered = body(scope(), 3, 100);

        let activation =
            plan(&view(&first, 1), &view(&raised, 2), shared()).expect("a raise is live");
        assert_eq!(activation.live(), [scope()]);

        let activation =
            plan(&view(&raised, 2), &view(&lowered, 3), shared()).expect("a lowering drains");
        assert_eq!(activation.draining().len(), 1);
        assert_eq!(
            activation.draining()[0].1,
            [TransitionReason::BudgetLowered]
        );
    }

    /// The rollback rule: republishing the old values forward is an ordinary
    /// drain, and walking the epoch backwards is refused with the procedure named.
    #[test]
    fn a_rollback_is_published_forward_and_a_reversed_epoch_is_refused() {
        let old = body(scope(), 4, 1_000);
        let new = body(scope(), 5, 9_000);
        let rolled_back = body(scope(), 6, 1_000);

        plan(&view(&new, 2), &view(&rolled_back, 3), shared())
            .expect("restoring the old values under a higher epoch is a drain");

        let refusal = plan(&view(&new, 2), &view(&old, 1), shared())
            .expect_err("repointing at an older revision is refused");
        assert_eq!(refusal.reason(), "refused");
        assert!(refusal.to_string().contains("EpochRegressed"), "{refusal}");
        assert!(
            refusal.to_string().contains("higher epoch"),
            "the refusal names the supported rollback: {refusal}"
        );
    }

    /// Two publications claiming one epoch: a restored backup, or a second
    /// control plane. Refused before either can be enforced, because a fence
    /// could no longer tell them apart.
    #[test]
    fn a_forked_epoch_is_refused_and_named() {
        let mine = body(scope(), 7, 1_000);
        let theirs = body(scope(), 7, 2_000);
        let refusal = plan(&view(&mine, 1), &view(&theirs, 2), shared())
            .expect_err("one epoch, two policies");
        assert_eq!(refusal.reason(), "refused");
        assert_eq!(
            refusal.scope(),
            scope(),
            "the refusal names the document it is about"
        );
        assert!(
            refusal.to_string().contains("EpochNotAdvanced"),
            "{refusal}"
        );
        assert!(
            refusal.to_string().contains("claims the epoch"),
            "the fence's reading is included: {refusal}"
        );
    }

    #[test]
    fn turning_a_scope_wide_cap_on_needs_a_migration_not_a_publication() {
        let flat = body(scope(), 1, 1_000);
        let scoped = detailed(scope(), 2, 1_000, Some(10_000), 300, 8, 60, 0);
        let refusal = plan(&view(&flat, 1), &view(&scoped, 2), shared())
            .expect_err("a layout change is not a publication");
        assert_eq!(refusal.reason(), "migration");
        assert!(refusal.to_string().contains("namespace_scope"), "{refusal}");
    }

    /// The rule that keeps control-plane Postgres, or a per-replica map, from
    /// being quietly promoted into a fleet-wide cap.
    #[test]
    fn a_per_replica_or_absent_backend_cannot_enforce_a_published_document() {
        let document = body(scope(), 1, 1_000);
        let mut config = stateful_config();
        config.budget.backend = BudgetBackend::InMemory;
        let refusal = plan(&empty(), &view(&document, 1), BackendSupport::of(&config))
            .expect_err("a per-replica cap is not a fleet-wide cap");
        assert_eq!(refusal.reason(), "unsupported");
        assert!(refusal.to_string().contains("fleet-wide"), "{refusal}");

        let refusal = plan(
            &empty(),
            &view(&document, 1),
            BackendSupport::of(&stateful_config()),
        )
        .expect_err("no shared lease store, no published ceiling");
        assert_eq!(refusal.reason(), "unsupported");
        assert!(refusal.to_string().contains("concurrency"), "{refusal}");
    }

    #[test]
    fn a_document_withdrawn_from_a_namespace_that_is_still_served_is_refused() {
        let document = body(scope(), 1, 1_000);
        let mut without = stateful_config();
        without.namespace.push(crate::config::Namespace {
            id: "acme/core".to_owned(),
            default: true,
            allow_platform_fallback: false,
            project: None,
            policy: None,
        });
        let refusal = plan(&view(&document, 1), &PolicyView::of(&without), shared())
            .expect_err("a served namespace cannot lose its policy");
        assert_eq!(refusal.reason(), "withdrawn");

        // The namespace itself going away is the deletion it looks like.
        let activation = plan(&view(&document, 1), &empty(), shared())
            .expect("a deleted namespace is not a gap");
        assert_eq!(activation.withdrawn(), [scope()]);
    }

    /// A tenant document governs every namespace its projects have, so one of
    /// them being deleted does not make the rest withdrawable.
    #[test]
    fn a_tenant_document_is_only_withdrawable_when_every_namespace_it_governs_is_gone() {
        let document = body(scope(), 1, 1_000);
        let policy = NamespacePolicy {
            body: document,
            generation: generation(&document, 1),
        };
        let mut both = stateful_config();
        both.namespace.push(crate::policy::view::tests::projected(
            "acme/core",
            Some(policy),
        ));
        both.namespace.push(crate::policy::view::tests::projected(
            "acme/edge",
            Some(policy),
        ));
        let active = PolicyView::of(&both);

        // One project deleted, its sibling still served and still capped by the
        // same document: dropping the document would uncap the survivor.
        let mut survivor = stateful_config();
        survivor
            .namespace
            .push(crate::policy::view::tests::projected("acme/edge", None));
        let refusal = plan(&active, &PolicyView::of(&survivor), shared())
            .expect_err("a sibling namespace is still governed by this document");
        assert_eq!(refusal.reason(), "withdrawn");
        assert!(refusal.to_string().contains("acme/edge"), "{refusal}");

        let activation = plan(&active, &empty(), shared())
            .expect("every namespace the document governed is gone");
        assert_eq!(activation.withdrawn(), [scope()]);
    }

    /// Which document governs a namespace is allowed to change in both
    /// directions. Only the namespace being left with nothing is a withdrawal.
    #[test]
    fn a_scope_handover_is_not_a_withdrawal_in_either_direction() {
        let project = PolicyScope::Project {
            tenant: tenant_id(1),
            project: crate::desired_state::fixtures::project_id(1),
        };
        let tenants = view(&body(scope(), 1, 1_000), 1);
        let projects = PolicyView::of(&governed(
            "acme/core",
            NamespacePolicy {
                body: body(project, 1, 1_000),
                generation: generation(&body(project, 1, 1_000), 2),
            },
        ));

        plan(&tenants, &projects, shared())
            .expect("a project publishing its own document still caps the namespace");
        plan(&projects, &tenants, shared())
            .expect("dropping it hands the namespace back to its tenant's document");
    }

    /// A scope's first document is new to *that scope*, but what it displaces is
    /// what the namespace was actually enforcing. A tightening handover drains,
    /// and says so, rather than being reported as a fresh binding.
    #[test]
    fn a_first_project_document_is_classified_against_what_it_displaces() {
        let project = PolicyScope::Project {
            tenant: tenant_id(1),
            project: crate::desired_state::fixtures::project_id(1),
        };
        let tighter = body(project, 1, 500);
        let looser = body(project, 1, 5_000);
        let tenants = view(&body(scope(), 1, 1_000), 1);
        let handover = |document: &crate::desired_state::policy::PolicyBody| {
            PolicyView::of(&governed(
                "acme/core",
                NamespacePolicy {
                    body: *document,
                    generation: generation(document, 2),
                },
            ))
        };

        let activation = plan(&tenants, &handover(&tighter), shared())
            .expect("a project may cap itself below its tenant");
        assert_eq!(
            activation.draining().len(),
            1,
            "cutting the cap a namespace was enforcing is a drain, whichever \
             document states it"
        );

        let activation = plan(&tenants, &handover(&looser), shared())
            .expect("raising the cap binds immediately");
        assert!(activation.draining().is_empty());
        assert_eq!(activation.live(), [project]);
    }

    /// A revision that moved an unrelated resource restates every document under
    /// its own id. Nothing about enforcement changed, so nothing activates.
    #[test]
    fn a_document_restated_by_a_later_revision_is_not_a_transition() {
        let document = body(scope(), 1, 1_000);
        let activation = plan(&view(&document, 1), &view(&document, 2), shared())
            .expect("a restatement is enforceable");
        assert!(
            activation.is_noop(),
            "the same policy, published again: {activation:?}"
        );
    }

    /// Every refusal is counted and reported, so every label it produces has to
    /// be one the metric and status vocabularies declared.
    #[test]
    fn every_refusal_reason_is_a_catalogued_revision_label() {
        let refusals = [
            ActivationRefusal::Unsupported {
                scope: scope(),
                detail: String::new(),
            },
            ActivationRefusal::Migration {
                scope: scope(),
                detail: String::new(),
            },
            ActivationRefusal::Refused {
                scope: scope(),
                detail: String::new(),
            },
            ActivationRefusal::Withdrawn {
                scope: scope(),
                namespace: "acme/core".to_owned(),
            },
        ];
        for refusal in refusals {
            let reason = refusal.reason();
            assert!(
                crate::convergence::reconciler::REVISION_REASONS.contains(&reason),
                "`{reason}` is a label a refused publication produces"
            );
            assert_eq!(
                crate::status::StatusReason::from_revision_reason(reason),
                crate::status::StatusReason::PolicyRejected,
                "`{reason}` reaches the status contract as a decided code"
            );
        }
    }

    /// Nothing about a stateless deployment reaches activation: there are no
    /// documents to classify, so the plan is empty and the file keeps governing.
    #[test]
    fn a_stateless_deployment_has_nothing_to_activate() {
        let view = PolicyView::of(&stateless_config());
        let activation = plan(&view, &view, BackendSupport::of(&stateless_config()))
            .expect("a file is not a publication");
        assert!(activation.is_noop());
    }
}
