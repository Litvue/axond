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
//! 3. **Materialization.** [`SecretMaterialization`] unwraps every exact secret
//!    version the revision's typed credentials pin, through the deployment's
//!    `SecretStore`. This is the only step that awaits, and the only place
//!    durable material enters the process.
//! 4. **Snapshot build.** [`ConfigSnapshot::build_with`] resolves credentials,
//!    gateway keys, verifiers, and minting material, and takes ownership of the
//!    material step 3 unwrapped — so every secret a candidate needs is resolved
//!    *here*, off the request path, before anything is published, and is held for
//!    exactly as long as the snapshot is.
//!
//! Every failure is a value returned to the caller, never a partially applied
//! change: nothing in this module can touch the running snapshot, because it
//! never receives it. That is what makes "the previous revision keeps serving"
//! structural rather than a rule the reconciler has to remember.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::config::{Config, ConfigError};
use crate::desired_state::{DesiredState, LoadedRevision, ResourceRef, RevisionId};
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
    fn project(&self, bootstrap: &Config, state: &DesiredState) -> Result<Config, ProjectionError>;
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
    #[error("revision {revision} could not be compiled into a runtime snapshot: {source}")]
    Snapshot {
        revision: RevisionId,
        #[source]
        source: SnapshotError,
    },
}

impl CompileError {
    /// The revision that was refused.
    pub const fn revision(&self) -> RevisionId {
        match self {
            Self::Projection { revision, .. }
            | Self::Validation { revision, .. }
            | Self::Snapshot { revision, .. } => *revision,
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
            Self::Snapshot { .. } => "snapshot",
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
        }
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
            .project(&self.bootstrap, revision.state())
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
        ConfigSnapshot::build_with(config, &self.env, generation, secrets).map_err(|source| {
            CompileError::Snapshot {
                revision: id,
                source,
            }
        })
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
        ) -> Result<Config, ProjectionError> {
            let mut config = bootstrap.clone();
            for resource in state.resources() {
                if resource.reference.kind != crate::desired_state::ResourceKind::Alias {
                    continue;
                }
                config.model.push(crate::config::Model {
                    name: resource.slug.as_str().to_owned(),
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

        fn project(&self, _: &Config, _: &DesiredState) -> Result<Config, ProjectionError> {
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
        let candidate = fixtures::candidate(
            crate::desired_state::ExpectedRevision::Empty,
            "first",
            fixtures::state(),
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
    use super::testing::{AliasProjection, RefusingProjection, bootstrap, env, revision};
    use super::*;
    use crate::desired_state::fixtures;

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
}
