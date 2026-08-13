//! The values a replica enforces, as one immutable snapshot.
//!
//! A view is derived from a whole [`Config`] — the bootstrap file in stateless
//! mode, a compiled revision in stateful mode — so there is exactly one way
//! policy reaches the request path, and a namespace cannot be enforced under
//! values that no configuration ever described.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::config::{Config, Mode};
use crate::desired_state::policy::{PolicyBody, PolicyGeneration, PolicyScope};

/// What a scope may spend, and how long an unsettled hold survives.
///
/// The request-path shape of [`BudgetPolicy`](crate::desired_state::policy::BudgetPolicy):
/// a `Duration` rather than a count of seconds, because that is what the stores
/// take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetCaps {
    pub subject_microdollars: u64,
    /// The scope-wide cap, or `None` when this scope has none.
    ///
    /// Whether the *store* is laid out to carry one is a bootstrap fact
    /// ([`BackendSupport`](super::BackendSupport)); this is only whether one is
    /// enforced. A view whose caps disagree with the layout is refused at
    /// activation, and — belt and braces, for a store built before this check
    /// existed — denied rather than mis-enforced at reserve time.
    pub namespace_microdollars: Option<u64>,
    pub reservation_ttl: Duration,
}

/// How many requests one subject may have in flight, and how long an abandoned
/// lease survives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConcurrencyCaps {
    pub max_in_flight_per_subject: u64,
    pub lease_ttl: Duration,
}

/// The policy governing one namespace, and the generation it is enforced under.
///
/// `None` caps mean *this replica cannot enforce a cap for this namespace*, which
/// happens in exactly one situation: a stateful deployment whose control plane has
/// published no document for a namespace whose backend enforces one. That denies
/// rather than admits — an unenforced cap is indistinguishable from an infinite
/// one, and the whole point of the section is that it is finite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ActivePolicy {
    pub budget: Option<BudgetCaps>,
    pub concurrency: Option<ConcurrencyCaps>,
    /// The generation these values came from, or `None` when they are the
    /// bootstrap file's.
    pub generation: Option<PolicyGeneration>,
}

impl ActivePolicy {
    /// The policy the bootstrap file states, which is the whole policy of a
    /// stateless deployment.
    fn bootstrap(config: &Config) -> Self {
        Self {
            budget: Some(BudgetCaps {
                subject_microdollars: config.budget.limit_microdollars,
                namespace_microdollars: config.budget.namespace_limit_microdollars,
                reservation_ttl: Duration::from_secs(config.budget.reservation_ttl_seconds),
            }),
            concurrency: Some(ConcurrencyCaps {
                max_in_flight_per_subject: config.rate_limit.max_in_flight_per_subject as u64,
                lease_ttl: Duration::from_secs(config.rate_limit.lease_ttl_seconds),
            }),
            generation: None,
        }
    }

    fn published(body: &PolicyBody, generation: PolicyGeneration) -> Self {
        Self {
            budget: Some(BudgetCaps {
                subject_microdollars: body.budget().subject_limit_microdollars(),
                namespace_microdollars: body.budget().namespace_limit_microdollars(),
                reservation_ttl: Duration::from_secs(body.budget().reservation_ttl_seconds()),
            }),
            concurrency: Some(ConcurrencyCaps {
                max_in_flight_per_subject: body.concurrency().max_in_flight_per_subject(),
                lease_ttl: Duration::from_secs(body.concurrency().lease_ttl_seconds()),
            }),
            generation: Some(generation),
        }
    }
}

/// One published document, as the view holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Published {
    pub(super) body: PolicyBody,
    pub(super) generation: PolicyGeneration,
    /// Every namespace this document governs, for refusals an operator reads.
    /// A tenant document governs each of its projects' namespaces, so a
    /// withdrawal has to be judged against all of them, not against whichever
    /// one happened to be seen last.
    pub(super) namespaces: Vec<String>,
}

/// Every value this replica enforces, keyed the two ways it is asked for them:
/// by namespace on the request path, and by scope when classifying a transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyView {
    /// What governs a namespace the view does not name. In stateless mode the
    /// file's values, so an unknown namespace enforces what every namespace does;
    /// in stateful mode nothing, so it cannot be admitted under an absent policy.
    default: ActivePolicy,
    by_namespace: BTreeMap<String, ActivePolicy>,
    published: BTreeMap<PolicyScope, Published>,
}

impl PolicyView {
    /// Derive the view a configuration describes.
    ///
    /// Stateless: every namespace is governed by the file. Stateful: a *projected*
    /// namespace is governed by the document a revision published for it
    /// ([`Namespace::policy`](crate::config::Namespace::policy)) and by nothing
    /// otherwise, while a namespace the bootstrap file itself declares keeps being
    /// governed by the file that declared it — the control plane never published a
    /// policy for a namespace it does not know about.
    pub fn of(config: &Config) -> Self {
        let stateful = config.mode == Mode::Stateful;
        let bootstrap = ActivePolicy::bootstrap(config);
        let mut by_namespace = BTreeMap::new();
        let mut published = BTreeMap::new();
        for namespace in &config.namespace {
            let policy = match &namespace.policy {
                Some(policy) => {
                    published
                        .entry(policy.body.scope())
                        .or_insert_with(|| Published {
                            body: policy.body,
                            generation: policy.generation,
                            namespaces: Vec::new(),
                        })
                        .namespaces
                        .push(namespace.id.clone());
                    ActivePolicy::published(&policy.body, policy.generation)
                }
                None if stateful && namespace.project.is_some() => ActivePolicy::default(),
                None => bootstrap,
            };
            by_namespace.insert(namespace.id.clone(), policy);
        }
        Self {
            default: if stateful {
                ActivePolicy::default()
            } else {
                bootstrap
            },
            by_namespace,
            published,
        }
    }

    /// The policy governing `namespace`.
    pub fn policy(&self, namespace: &str) -> ActivePolicy {
        self.by_namespace
            .get(namespace)
            .copied()
            .unwrap_or(self.default)
    }

    /// Whether this view enforces `generation` for any namespace.
    ///
    /// Compared with [`PolicyGeneration::same_policy`] rather than equality: a
    /// generation names the revision that published it, and every revision
    /// restates every document, so a revision that moved an unrelated resource
    /// would otherwise make every hold taken before it look like the leftover of
    /// a superseded policy.
    pub fn enforces(&self, generation: PolicyGeneration) -> bool {
        self.by_namespace.values().any(|policy| {
            policy
                .generation
                .is_some_and(|active| active.same_policy(&generation))
        })
    }

    /// Every published document, ordered by scope.
    pub(super) fn published(&self) -> &BTreeMap<PolicyScope, Published> {
        &self.published
    }

    /// Whether any namespace still carries the name `namespace`.
    pub(super) fn names(&self, namespace: &str) -> bool {
        self.by_namespace.contains_key(namespace)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::config::NamespacePolicy;
    use crate::desired_state::fixtures::tenant_id;
    use crate::policy::fixtures::{body, generation};

    pub(crate) fn stateless_config() -> Config {
        Config::from_toml_str(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

[[gateway_key]]
env = "STATIC_KEY"
namespace = "platform"

[budget]
backend = "in-memory"
limit_microdollars = 5_000
reservation_ttl_seconds = 120

[rate_limit]
max_in_flight_per_subject = 4
"#,
        )
        .expect("a valid stateless config")
    }

    pub(crate) fn stateful_config() -> Config {
        Config::from_toml_str(
            r#"
mode = "stateful"

[control_plane]
dsn_env = "GW_CONTROL_PLANE_DSN"

[secret_store]
kek_env = "GW_SECRET_STORE_KEK"

[[admin_breakglass]]
env = "GW_ADMIN_BREAKGLASS"

[budget]
backend = "redis"
dsn_env = "GW_BUDGET_REDIS"
"#,
        )
        .expect("a valid stateful bootstrap")
    }

    /// A projected namespace, as the tenancy projection produces one.
    pub(crate) fn projected(
        namespace: &str,
        policy: Option<NamespacePolicy>,
    ) -> crate::config::Namespace {
        crate::config::Namespace {
            id: namespace.to_owned(),
            default: true,
            allow_platform_fallback: false,
            project: Some(crate::config::ProjectIdentity {
                tenant: tenant_id(1),
                project: crate::desired_state::fixtures::project_id(1),
            }),
            policy,
        }
    }

    /// A stateful config with a projected `namespace` governed by `policy`.
    pub(crate) fn governed(namespace: &str, policy: NamespacePolicy) -> Config {
        let mut config = stateful_config();
        config.namespace.push(projected(namespace, Some(policy)));
        config
    }

    #[test]
    fn a_stateless_deployment_enforces_the_file_for_every_namespace() {
        let view = PolicyView::of(&stateless_config());
        let policy = view.policy("platform");
        assert_eq!(
            policy.budget,
            Some(BudgetCaps {
                subject_microdollars: 5_000,
                namespace_microdollars: None,
                reservation_ttl: Duration::from_secs(120),
            })
        );
        assert_eq!(
            policy.concurrency.map(|c| c.max_in_flight_per_subject),
            Some(4)
        );
        assert_eq!(policy.generation, None, "a file has no generation");
        assert_eq!(
            view.policy("a-namespace-the-file-never-named"),
            policy,
            "the file governs the deployment, not a list of names"
        );
    }

    /// The refusal that keeps a stateful replica from admitting against a cap
    /// nobody published: no document, no enforcement values, no admission.
    #[test]
    fn a_stateful_namespace_with_no_document_has_no_policy_at_all() {
        let mut config = stateful_config();
        config.namespace.push(projected("acme/core", None));
        let view = PolicyView::of(&config);
        assert_eq!(view.policy("acme/core"), ActivePolicy::default());
        assert!(view.policy("acme/core").budget.is_none());
    }

    #[test]
    fn a_published_document_governs_its_namespace_and_carries_its_generation() {
        let scope = PolicyScope::Tenant(tenant_id(1));
        let document = body(scope, 3, 9_000);
        let generation = generation(&document, 7);
        let view = PolicyView::of(&governed(
            "acme/core",
            NamespacePolicy {
                body: document,
                generation,
            },
        ));

        let policy = view.policy("acme/core");
        assert_eq!(
            policy.budget.expect("published").subject_microdollars,
            9_000
        );
        assert_eq!(policy.generation, Some(generation));
        assert!(view.enforces(generation));
        assert!(view.names("acme/core"));

        // A later revision that touched something else restates this document
        // verbatim under its own id. That is the same policy, still enforced —
        // not a generation this replica has stopped serving.
        let restated = crate::policy::fixtures::generation(&document, 8);
        assert_ne!(restated, generation, "a new revision, a new generation");
        assert!(view.enforces(restated));
    }
}
