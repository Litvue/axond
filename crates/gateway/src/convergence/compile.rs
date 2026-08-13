//! Turning a hydrated revision into a runtime snapshot, or refusing to.
//!
//! Convergence has exactly one way to accept desired state: compile it into a
//! whole [`ConfigSnapshot`] and publish that snapshot atomically. This module is
//! that compilation, and it is deliberately three separable steps:
//!
//! 1. **Projection.** A [`RevisionProjection`] reads resource *bodies* and fills
//!    the control-plane-owned sections of the bootstrap config. Body schemas —
//!    tenancy, providers, catalogue, pricing, policy — are owned by later slices
//!    (see [`crate::desired_state::resource`]), so #142 takes the projection as a
//!    seam rather than inventing schemas it would have to unship.
//! 2. **The boot gate.** The projected config runs
//!    [`Config::validate_compiled`], which *is* the whole-graph gate boot runs on
//!    a file. An alias pointing at an undefined provider is refused identically
//!    whether an operator wrote it in TOML or an administrator published it.
//! 3. **Pricing.** The revision's approved price book (#201) is read and resolved
//!    at the compiling instant into a [`PricingSnapshot`], which is attached to
//!    the snapshot the routing config produced. Pricing is therefore published
//!    *with* routing and never separately: a request holds one `Arc`, so it cannot
//!    be routed by one revision and priced by another. A price book this build
//!    cannot bill never reaches publication — reading the revision refuses it
//!    first (see [`CompileError::Pricing`]), before any durable material is
//!    unwrapped — so the previous snapshot, prices included, keeps serving.
//! 4. **Materialization.** [`SecretMaterialization`] unwraps every exact secret
//!    version the revision's typed credentials pin, through the deployment's
//!    `SecretStore`. This is the only step that awaits, and the only place
//!    durable material enters the process.
//! 5. **Availability.** When the process holds [`AvailabilityEvidence`], the
//!    revision's catalogue pins, enablements, credentials, and policy are
//!    projected into an availability view over the discovery evidence the replica
//!    has already accumulated (#148), and the result rides on the snapshot.
//!    Derived, never desired state: it cannot add a model, a namespace, or a
//!    credential to what the revision declares, and a deployment that derives
//!    none publishes snapshots exactly as before.
//! 6. **Snapshot build.** [`ConfigSnapshot::build_with`] resolves credentials,
//!    gateway keys, verifiers, and minting material, and takes ownership of the
//!    material step 4 unwrapped — so every secret a candidate needs is resolved
//!    *here*, off the request path, before anything is published, and is held for
//!    exactly as long as the snapshot is.
//!
//! Every failure is a value returned to the caller, never a partially applied
//! change: nothing in this module can touch the running snapshot, because it
//! never receives it. That is what makes "the previous revision keeps serving"
//! structural rather than a rule the reconciler has to remember.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::SystemTime;

use async_trait::async_trait;

use crate::availability::{AvailabilityEvidence, AvailabilityProjectionError, CredentialReadiness};
use crate::config::{Config, ConfigError};
use crate::desired_state::pricing::{
    EffectiveInstant, InvalidInstant, PriceBooks, PricingError, PricingSnapshot,
};
use crate::desired_state::{DesiredState, LoadedRevision, ResourceRef, RevisionId};
use crate::policy::ActivationRefusal;
use crate::state::{ConfigSnapshot, SnapshotError};

use super::secrets::{MaterialLedger, SecretMaterialization};

/// Filling the control-plane-owned sections of a config from desired state.
///
/// The bootstrap config is passed in rather than rebuilt, because the sections a
/// stateful process owns *locally* — listener, transport bounds, admission,
/// telemetry, datastore connectivity — are boot-validated facts that convergence
/// must not silently rewrite. A projection therefore returns the bootstrap config
/// with resources filled in, and a projection that mutated a process-local bound
/// would be visible as exactly that in review.
pub trait RevisionProjection: Send + Sync {
    /// The name recorded on rejections, so an operator can tell which projection
    /// refused a revision.
    fn name(&self) -> &'static str;

    /// Project desired state onto the bootstrap config.
    ///
    /// `source` is the revision the state came from. A projection that derives
    /// something the runtime must be able to *name* later — a policy generation,
    /// which is `(scope, epoch, source, content)` — needs it, and taking it here
    /// rather than reading it back off the state keeps one revision's projection
    /// reproducible by every replica.
    fn project(
        &self,
        bootstrap: &Config,
        state: &DesiredState,
        source: RevisionId,
    ) -> Result<Config, ProjectionError>;
}

/// Why desired state does not describe something this build can serve.
///
/// Every arm is an operator-actionable refusal rather than a transient failure:
/// retrying the same revision produces the same answer, which is why the
/// reconciler counts these against a revision instead of against the store.
#[derive(Debug, thiserror::Error)]
pub enum ProjectionError {
    /// The revision is internally consistent but does not describe a servable
    /// deployment — no default namespace, an alias with no reachable target.
    #[error("desired state does not describe a servable deployment: {detail}")]
    Incomplete { detail: String },
    /// A resource body could not be read as the schema its kind implies. A
    /// body written by a newer build lands here rather than being ignored.
    #[error("{reference} carries a body this build cannot read: {detail}")]
    Body {
        reference: ResourceRef,
        detail: String,
    },
    /// A secret a resource references could not be resolved. The *reference* is
    /// named, never the material.
    #[error("secret `{reference}` referenced by {holder} could not be resolved: {detail}")]
    Secret {
        holder: ResourceRef,
        reference: String,
        detail: String,
    },
}

/// Why a hydrated revision did not become a snapshot.
///
/// The arms are the compilation stages, in order, because the stage is what an
/// operator acts on: a projection failure is a body this build cannot read, a
/// validation failure is a graph an operator has to fix, and a snapshot failure
/// is usually secret material that is missing or wrong.
#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error("revision {revision} does not project onto a servable configuration: {source}")]
    Projection {
        revision: RevisionId,
        #[source]
        source: ProjectionError,
    },
    #[error("revision {revision} fails the configuration gate boot applies: {source}")]
    Validation {
        revision: RevisionId,
        #[source]
        source: ConfigError,
    },
    /// A book that survived being read and then could not be resolved into
    /// billable rates. Reading a revision validates its price book against this
    /// same domain, so a bad book is already refused as skew or damage before
    /// compilation: this is the guard for those two stages disagreeing, kept
    /// because a compiler may not assume its input was checked.
    #[error("revision {revision} does not carry pricing this build can bill: {source}")]
    Pricing {
        revision: RevisionId,
        #[source]
        /// Boxed to keep every compile `Result` narrow; a pricing refusal is a
        /// wide value that names a target, an interval, and a rate.
        source: Box<PricingError>,
    },
    /// The host's clock is not on the effective-dating timeline, so *which* rates
    /// are in force has no answer. A refusal rather than a fallback: compiling
    /// against a clamped instant would activate a rate schedule nobody dated.
    #[error("revision {revision} cannot be priced against this host's clock: {source}")]
    Clock {
        revision: RevisionId,
        #[source]
        source: InvalidInstant,
    },
    #[error("revision {revision} could not be compiled into a runtime snapshot: {source}")]
    Snapshot {
        revision: RevisionId,
        #[source]
        source: SnapshotError,
    },
    /// The candidate is servable, but the *stateful policy* in it cannot replace
    /// what this replica is enforcing without breaking a hold it already granted
    /// or a durable layout it booted on (#150). Refused before publication, so
    /// the previous policy and the previous config both keep serving.
    #[error("revision {revision} cannot activate its policy: {source}")]
    Activation {
        revision: RevisionId,
        #[source]
        source: ActivationRefusal,
    },
    /// The revision's own bodies could not be read into an availability view.
    ///
    /// A refusal rather than a snapshot carrying an empty view: these are bodies
    /// the projection already read, so a disagreement between the two stages means
    /// this build understands the revision less well than it just claimed to, and
    /// publishing would answer "which models may this tenant call" from a view
    /// nobody derived. Not the arm a discovery outage takes — no provider is
    /// reached here, and absent evidence is a verdict rather than an error.
    #[error("revision {revision} could not be projected into an availability view: {source}")]
    Availability {
        revision: RevisionId,
        /// Boxed because every other variant of this error is small and it is
        /// returned by the compile path on the way to a refusal, never in the
        /// common case.
        #[source]
        source: Box<AvailabilityProjectionError>,
    },
}

impl CompileError {
    /// Every label [`CompileError::reason`] returns, next to the match that
    /// returns them so a new variant's label is added here in the same edit. The
    /// metric catalogue and the status vocabulary are both checked against this
    /// list, so a compile refusal cannot ship a label an alert cannot see.
    ///
    /// [`Self::Activation`] forwards a label it does not own, so every one of
    /// [`ActivationRefusal::REASONS`] appears here too — held in step by
    /// `every_activation_refusal_is_a_compile_reason` rather than by hand.
    pub const REASONS: &'static [&'static str] = &[
        "secret",
        "projection",
        "validation",
        "pricing",
        "clock",
        "snapshot",
        "unsupported",
        "migration",
        "refused",
        "withdrawn",
        "ungoverned",
        "invalid_policy",
        "availability",
    ];

    /// The revision that was refused.
    pub const fn revision(&self) -> RevisionId {
        match self {
            Self::Projection { revision, .. }
            | Self::Validation { revision, .. }
            | Self::Pricing { revision, .. }
            | Self::Clock { revision, .. }
            | Self::Snapshot { revision, .. }
            | Self::Activation { revision, .. }
            | Self::Availability { revision, .. } => *revision,
        }
    }

    /// A stable, low-cardinality label for metrics and log filtering.
    ///
    /// Secret resolution gets its own label even though it happens inside two
    /// different stages: "the candidate was fine but a secret was not" is the
    /// distinction an on-call engineer needs first.
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::Projection {
                source: ProjectionError::Secret { .. },
                ..
            } => "secret",
            Self::Projection { .. } => "projection",
            Self::Validation { .. } => "validation",
            Self::Pricing { .. } => "pricing",
            Self::Clock { .. } => "clock",
            Self::Snapshot { .. } => "snapshot",
            Self::Activation { source, .. } => source.reason(),
            Self::Availability { .. } => "availability",
        }
    }
}

/// Compiling a hydrated revision, as an object-safe seam.
///
/// Object-safe so the reconciler holds one `Arc<dyn CandidateCompiler>` rather
/// than being generic over a projection that only exists in later slices — and so
/// a test can drive convergence with a compiler that refuses on demand.
///
/// Asynchronous because compiling a candidate resolves durable secret material,
/// which is a datastore call. Deliberately *only* here: the request path holds a
/// published snapshot and never awaits a secret store, so a store outage delays
/// convergence and cannot fail an inference request.
#[async_trait]
pub trait CandidateCompiler: Send + Sync {
    async fn compile(
        &self,
        revision: &LoadedRevision,
        generation: u64,
    ) -> Result<ConfigSnapshot, CompileError>;

    /// Tell the compiler the candidate it just produced will never be served.
    ///
    /// Compilation is asked for before the sink is asked to admit, so a refusal
    /// at activation lands after any replica-local state the compilation moved
    /// has already moved. Nothing in a snapshot needs this — a refused snapshot
    /// is simply dropped — but availability is derived into a holder that
    /// outlives snapshots, and leaving it describing a revision no snapshot ever
    /// served would let a later re-projection fold looks over dimensions the
    /// deployment refused. The default does nothing, for compilers that keep no
    /// such state.
    fn abandoned(&self) {}
}

/// The production compiler: projection, then the boot gate, then the snapshot.
pub struct RevisionCompiler<P> {
    bootstrap: Config,
    /// The environment secrets resolve from, captured once at boot.
    ///
    /// Fixed rather than re-read per attempt, so two replicas compiling one
    /// revision cannot disagree because someone edited a unit file between their
    /// polls. Durable, rotatable material belongs to the `SecretStore` a
    /// projection resolves through, not to this map.
    env: HashMap<String, String>,
    projection: P,
    /// How durable material is unwrapped for a candidate.
    ///
    /// A [`SecretMaterialization`], not a `SecretStore`: this component resolves
    /// exact versions and cannot stage, rotate, or transition anything, so the
    /// thing that holds plaintext is not the thing that can change what a
    /// credential points at.
    secrets: Arc<SecretMaterialization>,
    /// This replica's availability state, or `None` when the process derives
    /// none.
    ///
    /// Held by the compiler rather than by a snapshot because it outlives
    /// snapshots: the discovery evidence accumulated under one revision is
    /// exactly what the next revision must not cost.
    availability: Option<Arc<AvailabilityEvidence>>,
    /// Which derivation the last compilation published, so a candidate refused at
    /// activation names the one to undo.
    derived: Mutex<Option<u64>>,
    /// The clock effective-dated pricing is resolved against.
    ///
    /// Injected as a function rather than read inline so a test can compile the
    /// *same* revision at two instants and assert which rules were in force,
    /// which is the only way boundary behaviour is checkable at all.
    clock: fn() -> SystemTime,
}

impl<P: RevisionProjection> RevisionCompiler<P> {
    /// A compiler for a process with no secret store: file and env references
    /// compile as they always have, and a revision carrying typed credentials is
    /// refused rather than published without its material.
    pub fn new(bootstrap: Config, env: HashMap<String, String>, projection: P) -> Self {
        Self::with_secrets(
            bootstrap,
            env,
            projection,
            Arc::new(SecretMaterialization::stateless(MaterialLedger::new())),
        )
    }

    pub const fn with_secrets(
        bootstrap: Config,
        env: HashMap<String, String>,
        projection: P,
        secrets: Arc<SecretMaterialization>,
    ) -> Self {
        Self {
            bootstrap,
            env,
            projection,
            secrets,
            availability: None,
            derived: Mutex::new(None),
            clock: SystemTime::now,
        }
    }

    /// The same compiler, deriving an availability view onto every snapshot it
    /// publishes.
    #[must_use]
    pub fn with_availability(mut self, availability: Arc<AvailabilityEvidence>) -> Self {
        self.availability = Some(availability);
        self
    }

    /// The same compiler, resolving pricing against a fixed clock.
    #[must_use]
    pub const fn with_clock(mut self, clock: fn() -> SystemTime) -> Self {
        self.clock = clock;
        self
    }

    /// Which projection this compiler runs, for diagnostics.
    pub fn projection_name(&self) -> &'static str {
        self.projection.name()
    }

    /// The materialization this compiler resolves through, for diagnostics and
    /// for the status surface that reports which versions are held.
    pub fn secrets(&self) -> &Arc<SecretMaterialization> {
        &self.secrets
    }

    /// Resolve the revision's approved pricing at the compiling instant.
    ///
    /// `None` when the revision publishes no price book, which is a valid
    /// deployment: its offerings are discoverable and simply have no approved
    /// price.
    fn pricing(&self, revision: &LoadedRevision) -> Result<Option<PricingSnapshot>, CompileError> {
        let id = revision.id();
        let books = PriceBooks::of(revision.state()).map_err(|source| CompileError::Pricing {
            revision: id,
            source: Box::new(source),
        })?;
        if books.book().is_none() {
            return Ok(None);
        }
        let at = EffectiveInstant::of((self.clock)()).map_err(|source| CompileError::Clock {
            revision: id,
            source,
        })?;
        Ok(books.snapshot_at(at))
    }
}

#[async_trait]
impl<P: RevisionProjection> CandidateCompiler for RevisionCompiler<P> {
    async fn compile(
        &self,
        revision: &LoadedRevision,
        generation: u64,
    ) -> Result<ConfigSnapshot, CompileError> {
        let id = revision.id();
        let config = self
            .projection
            .project(&self.bootstrap, revision.state(), id)
            .map_err(|source| CompileError::Projection {
                revision: id,
                source,
            })?;
        config
            .validate_compiled()
            .map_err(|source| CompileError::Validation {
                revision: id,
                source,
            })?;
        // Before the snapshot build, so a price book this build cannot bill
        // refuses the candidate without any secret material being resolved.
        let pricing = self.pricing(revision)?;
        // Material is unwrapped after the boot gate and before the snapshot: a
        // revision that cannot be served at all does not touch the secret store,
        // and material that cannot be unwrapped is a refusal rather than a
        // snapshot with holes in it. Either way the resolved set is dropped here,
        // which zeroizes it, and only a published snapshot keeps it alive.
        let secrets = self
            .secrets
            .resolve(revision.state())
            .await
            .map_err(|source| CompileError::Projection {
                revision: id,
                source,
            })?;
        // Read before the set is handed to the snapshot: "this credential's exact
        // version is in hand" is what separates a credential a tenant holds from
        // one it can use, and it is a set of references, never material.
        let readiness = CredentialReadiness::of(&secrets);
        let snapshot = ConfigSnapshot::build_with(config, &self.env, generation, secrets).map_err(
            |source| CompileError::Snapshot {
                revision: id,
                source,
            },
        )?;
        let snapshot = match pricing {
            None => snapshot,
            Some(pricing) => snapshot.with_pricing(pricing),
        };
        let Some(evidence) = self.availability.as_ref() else {
            return Ok(snapshot);
        };
        let projected = evidence
            .derive(revision.state(), &readiness)
            .map_err(|source| CompileError::Availability {
                revision: id,
                source: Box::new(source),
            })?;
        // Named, so a refusal undoes *this* derivation or nothing: a discovery
        // re-projection between here and the sink's answer has folded looks over
        // this candidate's index, and undoing that one would restore the very
        // dimensions the deployment refused.
        *self.derived.lock().unwrap_or_else(PoisonError::into_inner) = Some(projected.derivation());
        Ok(snapshot.with_availability(Arc::new(projected.into_index())))
    }

    fn abandoned(&self) {
        let Some(evidence) = self.availability.as_ref() else {
            return;
        };
        let derived = self
            .derived
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(derivation) = derived {
            evidence.abandon(derivation);
        }
    }
}

/// The compilation fixtures the convergence tests share.
///
/// Shared rather than duplicated per module because the reconciler tests need the
/// *same* pipeline these tests characterise: a rejection asserted here is the
/// rejection the loop is asserted to survive.
#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::desired_state::fixtures;

    /// A projection that appends one alias per `Alias` resource in the revision,
    /// pointing at a provider named by `provider`.
    ///
    /// Enough to exercise the pipeline honestly: the resulting config is a real
    /// config, and naming a provider the bootstrap does not define produces
    /// exactly the dangling-target rejection boot produces.
    pub(crate) struct AliasProjection {
        pub(crate) provider: &'static str,
    }

    impl RevisionProjection for AliasProjection {
        fn name(&self) -> &'static str {
            "test-alias"
        }

        fn project(
            &self,
            bootstrap: &Config,
            state: &DesiredState,
            _source: RevisionId,
        ) -> Result<Config, ProjectionError> {
            let mut config = bootstrap.clone();
            for resource in state.resources() {
                if resource.reference.kind != crate::desired_state::ResourceKind::Alias {
                    continue;
                }
                config.model.push(crate::config::Model {
                    name: resource.slug.as_str().to_owned(),
                    namespace: None,
                    targets: vec![crate::config::Target {
                        provider: self.provider.to_owned(),
                        model: "gpt-4o".to_owned(),
                        price: gateway_core::catalog::ModelPrice {
                            input_microdollars_per_million: 1,
                            output_microdollars_per_million: 1,
                            reasoning_microdollars_per_million: None,
                            cache_read_microdollars_per_million: None,
                            cache_write_microdollars_per_million: None,
                        },
                    }],
                });
            }
            Ok(config)
        }
    }

    pub(crate) struct RefusingProjection;

    impl RevisionProjection for RefusingProjection {
        fn name(&self) -> &'static str {
            "test-refusing"
        }

        fn project(
            &self,
            _: &Config,
            _: &DesiredState,
            _: RevisionId,
        ) -> Result<Config, ProjectionError> {
            Err(ProjectionError::Incomplete {
                detail: "no tenant is enabled".to_owned(),
            })
        }
    }

    /// A minimal servable bootstrap: one default namespace, one provider, one
    /// inbound key. Everything a projection adds hangs off this.
    pub(crate) fn bootstrap() -> Config {
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
env = "AXOND_KEY"
namespace = "platform"
"#,
        )
        .expect("a valid bootstrap config")
    }

    pub(crate) fn env() -> HashMap<String, String> {
        HashMap::from([("AXOND_KEY".to_owned(), "inbound-secret".to_owned())])
    }

    /// A hydrated revision carrying the domain's standard fixture state.
    pub(crate) fn revision() -> LoadedRevision {
        revision_with(fixtures::state())
    }

    /// A hydrated revision carrying `state`, assembled the way one loaded from
    /// storage is.
    pub(crate) fn revision_with(state: DesiredState) -> LoadedRevision {
        let candidate = fixtures::candidate(
            crate::desired_state::ExpectedRevision::Empty,
            "first",
            state,
        );
        let manifest = crate::desired_state::RevisionManifest::of(
            fixtures::revision_id(9),
            None,
            std::time::SystemTime::UNIX_EPOCH,
            &candidate,
        )
        .expect("a valid manifest");
        LoadedRevision::assemble(manifest, candidate.state).expect("a consistent revision")
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::testing::{
        AliasProjection, RefusingProjection, bootstrap, env, revision, revision_with,
    };
    use super::*;
    use crate::backends::catalog::ProviderId;
    use crate::desired_state::fixtures;
    use crate::desired_state::pricing::{
        Approval, EffectiveInterval, PriceBookBody, RulePrecedence,
    };

    #[tokio::test]
    async fn a_projected_revision_compiles_into_a_snapshot_at_the_requested_generation() {
        let compiler = RevisionCompiler::with_secrets(
            bootstrap(),
            env(),
            AliasProjection { provider: "openai" },
            crate::convergence::secrets::testing::permissive(),
        );
        let snapshot = compiler
            .compile(&revision(), 7)
            .await
            .expect("the projected config is servable");
        assert_eq!(snapshot.generation, 7);
        // The fixture holds one alias, which the projection turns into one model.
        assert!(
            snapshot
                .config
                .model
                .iter()
                .any(|model| model.name == "fast")
        );
    }

    /// The reason the boot gate is *reused* rather than reimplemented: a
    /// revision whose alias points at a provider no one defined is rejected with
    /// the same error a file would produce, and nothing is returned that a caller
    /// could publish.
    #[tokio::test]
    async fn a_revision_whose_alias_targets_an_undefined_provider_is_refused_by_the_boot_gate() {
        let compiler = RevisionCompiler::with_secrets(
            bootstrap(),
            env(),
            AliasProjection {
                provider: "nonexistent",
            },
            crate::convergence::secrets::testing::permissive(),
        );
        let error = compiler
            .compile(&revision(), 1)
            .await
            .err()
            .expect("an undefined target cannot be served");
        assert_eq!(error.reason(), "validation");
        assert!(error.to_string().contains("undefined provider"), "{error}");
    }

    #[tokio::test]
    async fn an_unreadable_revision_is_refused_before_any_configuration_is_built() {
        let compiler = RevisionCompiler::with_secrets(
            bootstrap(),
            env(),
            RefusingProjection,
            crate::convergence::secrets::testing::permissive(),
        );
        let error = compiler
            .compile(&revision(), 1)
            .await
            .err()
            .expect("the projection refuses");
        assert_eq!(error.reason(), "projection");
        assert_eq!(error.revision(), fixtures::revision_id(9));
    }

    /// Pricing is resolved into the *same* snapshot the routing config lands in,
    /// so a request that reads one reads both: there is no window in which a
    /// replica routes at a generation whose prices have not arrived yet.
    #[tokio::test]
    async fn an_approved_book_is_published_in_the_same_snapshot_as_the_routing_config() {
        let body = fixtures::approved_price_book();
        let compiler = RevisionCompiler::with_secrets(
            bootstrap(),
            env(),
            AliasProjection { provider: "openai" },
            crate::convergence::secrets::testing::permissive(),
        );
        let snapshot = compiler
            .compile(&revision_with(fixtures::state_with_price_book(&body)), 3)
            .await
            .expect("an approved book is servable");
        let pricing = snapshot.pricing().expect("the revision carries pricing");
        assert!(pricing.is_approved());
        assert_eq!(pricing.catalog(), fixtures::catalog_content_id());
        assert_eq!(
            pricing
                .price(&ProviderId::parse("openai").expect("id"), "gpt-4o")
                .expect("the book prices the routed target")
                .input_microdollars_per_million,
            2_500
        );
        // The same snapshot, at the same generation, holds the routing config.
        assert_eq!(snapshot.generation, 3);
        assert!(
            snapshot
                .config
                .model
                .iter()
                .any(|model| model.name == "fast")
        );
    }

    /// A deployment that has approved nothing still serves: its offerings are
    /// discoverable, and carry no price to bill a budget against.
    #[tokio::test]
    async fn a_revision_without_a_price_book_compiles_without_pricing() {
        let compiler = RevisionCompiler::with_secrets(
            bootstrap(),
            env(),
            AliasProjection { provider: "openai" },
            crate::convergence::secrets::testing::permissive(),
        );
        let snapshot = compiler
            .compile(&revision(), 1)
            .await
            .expect("pricing is not a precondition for routing");
        assert!(snapshot.pricing().is_none());
        assert!(
            snapshot
                .config
                .model
                .iter()
                .any(|model| model.name == "fast")
        );
    }

    /// A book awaiting approval publishes its identity and no prices, so an
    /// operator can see what is staged without it billing anything.
    #[tokio::test]
    async fn a_draft_book_publishes_its_identity_and_no_prices() {
        let body = PriceBookBody::new(fixtures::catalog_content_id(), Approval::Draft).with_rule(
            fixtures::price_rule(
                fixtures::priced_target("openai", "gpt-4o"),
                RulePrecedence::Baseline,
                EffectiveInterval::from(EffectiveInstant::EPOCH),
                1_000,
                1_000,
            ),
        );
        let compiler = RevisionCompiler::with_secrets(
            bootstrap(),
            env(),
            AliasProjection { provider: "openai" },
            crate::convergence::secrets::testing::permissive(),
        );
        let snapshot = compiler
            .compile(&revision_with(fixtures::state_with_price_book(&body)), 1)
            .await
            .expect("a draft book does not refuse the candidate");
        let pricing = snapshot.pricing().expect("the identity is published");
        assert!(!pricing.is_approved());
        assert!(pricing.is_empty());
    }

    /// Pricing is resolved against the host's clock, and a clock off the timeline
    /// refuses the candidate rather than pricing at an instant it invented.
    /// Nothing is returned, so whatever is already serving keeps serving.
    #[tokio::test]
    async fn a_host_clock_before_the_epoch_refuses_the_candidate_rather_than_guessing() {
        let body = fixtures::approved_price_book();
        let compiler = RevisionCompiler::with_secrets(
            bootstrap(),
            env(),
            AliasProjection { provider: "openai" },
            crate::convergence::secrets::testing::permissive(),
        )
        .with_clock(|| SystemTime::UNIX_EPOCH - Duration::from_secs(1));
        let error = compiler
            .compile(&revision_with(fixtures::state_with_price_book(&body)), 1)
            .await
            .err()
            .expect("an instant off the timeline cannot price a revision");
        assert_eq!(error.reason(), "clock");
        assert_eq!(error.revision(), fixtures::revision_id(9));
    }

    /// Secret resolution happens during compilation, so missing material is a
    /// refusal with a named *reference* and no leaked value.
    #[tokio::test]
    async fn unresolvable_secret_material_refuses_the_candidate_without_disclosing_it() {
        let compiler = RevisionCompiler::with_secrets(
            bootstrap(),
            HashMap::new(),
            AliasProjection { provider: "openai" },
            crate::convergence::secrets::testing::permissive(),
        );
        let error = compiler
            .compile(&revision(), 1)
            .await
            .err()
            .expect("an unresolvable gateway key cannot be published");
        assert_eq!(error.reason(), "snapshot");
        assert!(error.to_string().contains("AXOND_KEY"), "{error}");
    }

    /// A refused activation is reported under a label it did not coin, so the
    /// guards that read [`CompileError::REASONS`] — the metric catalogue and the
    /// status vocabulary — would otherwise stop covering the one kind of
    /// compile refusal whose labels are declared somewhere else.
    #[test]
    fn every_activation_refusal_is_a_compile_reason() {
        for reason in ActivationRefusal::REASONS {
            assert!(
                CompileError::REASONS.contains(reason),
                "`{reason}` is forwarded by `CompileError::Activation` and has to be catalogued \
                 with the rest"
            );
        }
    }
}
