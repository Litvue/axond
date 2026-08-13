//! The recovery qualification driver (axond #219).
//!
//! Each test here runs one *stage* of a scenario in
//! `qualification/recovery/manifest.toml` against a real PostgreSQL journal, and
//! writes what it observed to `target/recovery/<scenario>.<stage>.json`. The
//! stages driven today are the control-plane halves: a converged replica losing
//! the journal, the three cold boots the signed cache defines, and the fleet
//! converging when the journal comes back. The serving halves are blocked
//! stages, because a replica cannot yet serve a projected revision.
//!
//! # What makes this a recovery test rather than a mock
//!
//! Two things, and both are deliberate:
//!
//! - The journal is a real [`PostgresControlPlane`] in its own schema, migrated
//!   by this build. Without `AXOND_TEST_POSTGRES_DSN` the stages do not run and
//!   no artifact is written; they never fall back to
//!   [`InMemoryControlPlane`](crate::desired_state::oracle::InMemoryControlPlane),
//!   because an outage of an in-process oracle qualifies the oracle.
//! - The outage is a real cut: the replica reaches Postgres through a
//!   [`SeverableLink`], and severing it drops the live connection and refuses
//!   reconnection. The replica sees a dead socket, not an injected error, so the
//!   reconnect path runs — and the database keeps its rows, which is what makes
//!   the recovery half mean anything.
//!
//! # What the cold-boot stages can and cannot do
//!
//! A stateful replica's store handle is built by `connect`, and `connect`
//! against an unreachable database fails before a [`Reconciler`] exists: a real
//! replica exits there. So the cold-boot stages build the store while the link
//! is up and cut it before `bootstrap`, which is the boot of the convergence
//! machinery — a reconciler with no active revision, deciding between the cache
//! and a refusal. That is the decision the scenarios are about; the artifact
//! says so in `boot_note` rather than implying a process was started.
//!
//! Determinism comes from the structure rather than from sleeping: every
//! convergence step is an explicit [`Reconciler::converge_once`], and the only
//! bound asserted against wall clock is the convergence bound the manifest
//! declares in whole seconds.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio_postgres::Config;

use super::evidence::Recorder;
use super::severable::{self, SeverableLink};
use crate::backends::BackendFailure;
use crate::backends::control_plane::postgres::{ControlPlaneSettings, PostgresControlPlane};
use crate::backends::control_plane::{ControlPlaneError, ControlPlaneStore};
use crate::backends::secrets::envelope::DeploymentKek;
use crate::backends::secrets::postgres::{PostgresSecrets, SecretStoreSettings};
use crate::backends::secrets::{KekRef, SecretMaterial, SecretResolver, SecretStore};
use crate::budget::NoBudget;
use crate::convergence::compile::testing::{AliasProjection, bootstrap, env};
use crate::convergence::lkg::testing::{KEY, cache_path};
use crate::convergence::reconciler::category_reason;
use crate::convergence::{
    BackoffPolicy, BootstrapError, ConvergenceSettings, LastKnownGood, MaterialLedger, Reconciler,
    RevisionCompiler, RevisionReport, SecretMaterialization, SnapshotSource, SystemClock,
};
use crate::desired_state::credentials::ProviderCredentialBody;
use crate::desired_state::secrets::{SecretOwner, SecretRef};
use crate::desired_state::{
    DesiredState, ExpectedRevision, ResourceKind, RevisionId, RevisionManifest, fixtures,
};
use crate::state::AppState;
use crate::usage::{UsageFanout, UsageSink};

/// The lane this driver is, as the manifest spells it.
pub(crate) const RUNNER: &str = "stateful-tests";

/// The stages this driver runs, as `scenario/stage`.
///
/// The honesty gate for the whole harness: a manifest stage marked `executable`
/// under this driver's [`RUNNER`] that is not in this list, or a stage in this
/// list the manifest still calls blocked, fails
/// [`the_driver_runs_exactly_the_stages_the_manifest_calls_executable`].
/// A recovery claim is only as good as the code behind it, and this is the one
/// place the two are compared.
pub(crate) const DRIVEN_STAGES: [&str; 5] = [
    "control-plane-outage/journal-outage",
    "cold-boot-valid-cache/cold-boot",
    "cold-boot-no-cache/cold-boot",
    "cold-boot-invalid-cache/cold-boot",
    "recovery-convergence/journal-recovery",
];

// ── The manifest, as the driver reads it ─────────────────────────────────────

/// The manifest fields the driver needs: the gate it evaluates against and the
/// evidence classes it echoes into the artifact. The full contract — dependency
/// map, prose agreement, evidence coverage — is asserted in
/// `tests/recovery_contract.rs`; this is deliberately the smaller read.
#[derive(Debug, Clone, Deserialize)]
struct Manifest {
    #[serde(rename = "scenario")]
    scenarios: Vec<Scenario>,
}

#[derive(Debug, Clone, Deserialize)]
struct Scenario {
    id: String,
    capability: String,
    gate: Gate,
    #[serde(rename = "stage")]
    stages: Vec<Stage>,
}

#[derive(Debug, Clone, Deserialize)]
struct Stage {
    id: String,
    status: String,
    /// The lane that runs the stage. This driver is one of two, so it claims
    /// only the stages that name it — the restore and PITR stages belong to
    /// `ops/restore-drill.sh`, which needs a cluster it can promote.
    #[serde(default)]
    runner: Option<String>,
    evidence: Vec<String>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
struct Gate {
    readiness: Readiness,
    admin_writes: AdminWrites,
    max_serving_error_fraction: f64,
    max_convergence_lag_seconds: u64,
    max_data_loss_revisions: u64,
    max_unauthenticated_admin_successes: u64,
}

/// The two non-numeric gate fields. A stage records the bound it read here and
/// evaluates against it, so flipping the manifest flips the verdict instead of
/// leaving a literal behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Readiness {
    Serves,
    Refuses,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AdminWrites {
    Accepted,
    Unavailable,
}

impl Gate {
    /// The verdict for the `readiness` gate: what the stage observed has to be
    /// what the manifest demanded, so editing the manifest edits the verdict.
    /// `held` carries the rest of what the stage checked.
    const fn readiness_met(self, observed: Readiness, held: bool) -> bool {
        matches!(
            (self.readiness, observed),
            (Readiness::Serves, Readiness::Serves) | (Readiness::Refuses, Readiness::Refuses)
        ) && held
    }

    /// The same, for `admin_writes`.
    const fn admin_writes_met(self, observed: AdminWrites, held: bool) -> bool {
        matches!(
            (self.admin_writes, observed),
            (AdminWrites::Accepted, AdminWrites::Accepted)
                | (AdminWrites::Unavailable, AdminWrites::Unavailable)
        ) && held
    }
}

impl Readiness {
    const fn bound(self) -> &'static str {
        match self {
            Self::Serves => "serves",
            Self::Refuses => "refuses",
        }
    }
}

impl AdminWrites {
    const fn bound(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Unavailable => "unavailable",
        }
    }
}

fn manifest() -> Manifest {
    let path = super::evidence::workspace_root().join("qualification/recovery/manifest.toml");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{} is unreadable: {e}", path.display()));
    toml_manifest(&text)
}

fn toml_manifest(text: &str) -> Manifest {
    use figment::providers::Format;
    figment::Figment::from(figment::providers::Toml::string(text))
        .extract()
        .expect("the recovery manifest parses")
}

/// The scenario and stage a driver is about to run, with everything the artifact
/// echoes from the contract.
struct StageSpec {
    scenario: String,
    stage: String,
    capability: String,
    evidence: Vec<String>,
    gate: Gate,
}

impl StageSpec {
    fn load(key: &str) -> Self {
        let (scenario_id, stage_id) = key.split_once('/').expect("a `scenario/stage` key");
        let manifest = manifest();
        let scenario = manifest
            .scenarios
            .iter()
            .find(|scenario| scenario.id == scenario_id)
            .unwrap_or_else(|| panic!("the manifest declares no `{scenario_id}` scenario"));
        let stage = scenario
            .stages
            .iter()
            .find(|stage| stage.id == stage_id)
            .unwrap_or_else(|| panic!("`{scenario_id}` declares no `{stage_id}` stage"));
        Self {
            scenario: scenario.id.clone(),
            stage: stage.id.clone(),
            capability: scenario.capability.clone(),
            evidence: stage.evidence.clone(),
            gate: scenario.gate,
        }
    }

    fn recorder(&self, deployment: &Deployment) -> Recorder {
        let classes: Vec<&str> = self.evidence.iter().map(String::as_str).collect();
        let mut recorder = Recorder::new(
            &self.scenario,
            &self.stage,
            RUNNER,
            &self.capability,
            &classes,
            &deployment.schema,
            &deployment.schema_identity,
        );
        // Which secret store compiled these revisions, and whether the stage's
        // outage crossed it: a reader judging a rotation or restore claim needs
        // to know the material path was real and which side of the cut it was
        // on. References and paths only — never material.
        recorder.observe("secret_store", deployment.secrets.name());
        recorder.observe("secret_store_path", "operator-dsn (not severed)");
        recorder
    }
}

// ── The deployment under qualification ───────────────────────────────────────

/// A real journal in its own schema, reached through a link the harness can cut.
struct Deployment {
    /// The DSN the replicas use: the operator's, redirected through the link.
    dsn: String,
    schema: String,
    /// What the journal's own migration ledger says it is, read after this build
    /// migrated it: the artifact's answer to "which control plane produced this
    /// evidence?".
    schema_identity: String,
    link: SeverableLink,
    /// The deployment's encrypted secret store, holding the material the
    /// credentials in these revisions point at.
    ///
    /// Reached on the operator's DSN rather than through [`Self::link`], because
    /// the link models a *journal* outage: the secret store is a separate
    /// dependency whose own outage — and the shared-database case where one cut
    /// takes both — belongs to the blocked `secret-rotation` scenario. Every
    /// artifact records which path was used, so no reader has to infer it.
    secrets: Arc<PostgresSecrets>,
    /// One staged secret version per credential owner, so republishing a
    /// variation of the same desired state does not silently rotate material.
    material: Mutex<BTreeMap<SecretOwner, SecretRef>>,
}

impl Deployment {
    /// Create an isolated schema, open the link, and migrate the journal through
    /// it. `None` when there is nothing to qualify — a recovery stage without a
    /// database produces nothing rather than producing evidence about a fake.
    ///
    /// A DSN the harness cannot cut faithfully skips for the same reason. The
    /// rest of the stateful suite accepts shapes this one cannot — libpq
    /// `key=value`, a multi-host failover list, a Unix socket — and a
    /// contributor configured that way should get no recovery evidence rather
    /// than five failures about the harness. Both checks run before anything is
    /// created, so a skip leaves no schema behind.
    ///
    /// Under `AXOND_TEST_REQUIRE_SERVICES=1` the same shapes panic instead:
    /// that variable is CI's statement that a green run must mean the stages
    /// ran, and a DSN this harness cannot use is a configuration failure there,
    /// not a contributor's local setup.
    async fn open() -> Option<Self> {
        let operator_dsn = crate::test_services::postgres_dsn()?;
        let required = crate::test_services::required();
        let Some(upstream) = severable::upstream(&operator_dsn).await else {
            return unusable_dsn(
                required,
                "it does not resolve to exactly one TCP host, so there is no single link to cut",
            );
        };
        // Resolved before the schema exists: a DSN the link cannot be spliced
        // into is a skip, not a half-built deployment.
        if severable::redirect(&operator_dsn, 0).is_none() {
            return unusable_dsn(
                required,
                "it is not a `postgres://` URL this harness can redirect through a severable link",
            );
        }
        let schema = format!(
            "recovery_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("the clock is after the epoch")
                .as_nanos()
        );

        let mut config: Config = operator_dsn.parse().expect("the configured DSN parses");
        config.connect_timeout(Duration::from_secs(5));
        let (client, connection) = config
            .connect(crate::usage::tls_connector())
            .await
            .expect("connect to create the qualification schema");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .expect("create the qualification schema");

        let link = SeverableLink::open(upstream)
            .await
            .expect("a loopback link to the control-plane database");
        let dsn = severable::redirect(&operator_dsn, link.port())
            .expect("a DSN that redirects at all redirects to the link's port");

        let secrets = PostgresSecrets::connect(
            &operator_dsn,
            SecretStoreSettings {
                schema: Some(schema.clone()),
                create_table: true,
                connect_timeout: Duration::from_secs(5),
                operation_timeout: Duration::from_secs(10),
            },
            qualification_kek(),
        )
        .await
        .expect("an encrypted secret store in the qualification schema");

        let mut deployment = Self {
            dsn,
            schema,
            schema_identity: String::new(),
            link,
            secrets: Arc::new(secrets),
            material: Mutex::new(BTreeMap::new()),
        };
        // Migrating here rather than inside a stage keeps the schema this build
        // owns out of every stage's outage window, and gives the artifact the
        // ledger's own account of what it migrated to.
        let migrator = PostgresControlPlane::connect(&deployment.dsn, deployment.settings(true))
            .await
            .expect("boot against a fresh schema");
        let status = migrator
            .schema_status()
            .await
            .expect("read the migrated schema's ledger");
        deployment.schema_identity = format!("{status:?}");
        Some(deployment)
    }

    /// The administrator's connection: the operator's, publishing revisions
    /// against a schema this build already migrated.
    async fn administrator(&self) -> PostgresControlPlane {
        PostgresControlPlane::connect(&self.dsn, self.settings(false))
            .await
            .expect("boot against a current schema")
    }

    /// A second connection on a schema that is already current, as an operator's
    /// second replica is: it must not need permission to migrate.
    async fn store(&self) -> Arc<PostgresControlPlane> {
        Arc::new(
            PostgresControlPlane::connect(&self.dsn, self.settings(false))
                .await
                .expect("boot against a current schema"),
        )
    }

    /// `state`, with every provider credential repointed at material this
    /// deployment really staged in its encrypted secret store.
    ///
    /// The fixtures pin references no store was ever asked to hold, and a
    /// candidate whose material does not resolve is refused before it is
    /// published — correctly, and by the production compiler. So the harness
    /// stages material the way an administrator does, per owner, and publishes
    /// desired state that names what the store actually holds.
    async fn materialized(&self, state: DesiredState) -> DesiredState {
        let mut materialized = DesiredState::new();
        for blob in state.blobs() {
            materialized.declare_blob(*blob);
        }
        for resource in state.resources() {
            let repointed = if resource.reference.kind == ResourceKind::ProviderCredential {
                match ProviderCredentialBody::read(resource) {
                    Ok(body) => {
                        let secret = self.staged_for(body.owner()).await;
                        ProviderCredentialBody::staged(
                            body.credential(),
                            body.owner(),
                            body.provider(),
                            body.display_name().clone(),
                            secret,
                        )
                        .version_at(resource.slug.clone(), resource.reference.version)
                    }
                    // An untyped credential body carries no reference to repoint,
                    // and compilation does not resolve one: it passes through.
                    Err(_) => resource.clone(),
                }
            } else {
                resource.clone()
            };
            materialized
                .insert(repointed)
                .expect("repointing a credential preserves every reference");
        }
        materialized
    }

    /// The version this owner's material is stored under, staging it on first
    /// use. Staged rather than active because that is the lifecycle a newly
    /// loaded credential has, and staged material is resolvable by design.
    async fn staged_for(&self, owner: SecretOwner) -> SecretRef {
        let mut material = self.material.lock().await;
        if let Some(reference) = material.get(&owner) {
            return *reference;
        }
        let staged = self
            .secrets
            .stage(
                owner,
                SecretMaterial::new(QUALIFICATION_MATERIAL.to_owned()),
            )
            .await
            .expect("the secret store accepts the qualification material")
            .reference;
        material.insert(owner, staged);
        staged
    }

    /// A materialization the replicas compile through: the real encrypted store,
    /// with a ledger of its own so one replica's retained versions are its own.
    fn materialization(&self) -> Arc<SecretMaterialization> {
        Arc::new(SecretMaterialization::new(
            Arc::clone(&self.secrets) as Arc<dyn SecretResolver>,
            MaterialLedger::new(),
        ))
    }

    fn settings(&self, migrate: bool) -> ControlPlaneSettings {
        ControlPlaneSettings {
            schema: Some(self.schema.clone()),
            migrate,
            // Short, so a severed link is reported as an outage in about a second
            // rather than holding a stage open for the production timeout.
            connect_timeout: Duration::from_secs(2),
            operation_timeout: Duration::from_secs(5),
            ..ControlPlaneSettings::default()
        }
    }
}

/// One replica: a store, the convergence loop, and the `ArcSwap` it publishes
/// into.
///
/// Assembled exactly as `convergence::tests` assembles one — same compiler, same
/// sink, same settings shape — with the control plane swapped for the real
/// journal. The projection is still the test projection, which is precisely why
/// the serving stages are blocked: what this replica publishes is a real
/// snapshot compiled by a real pipeline, but the mapping from resource bodies to
/// a servable config is the slice that has not landed.
struct Replica {
    reconciler: Arc<Reconciler>,
    state: AppState,
}

impl Replica {
    async fn build(deployment: &Deployment, cache: Option<LastKnownGood>) -> Self {
        let store = deployment.store().await;
        let sinks: Vec<Box<dyn UsageSink>> = Vec::new();
        let state = AppState::new(
            bootstrap(),
            &env(),
            UsageFanout::new(sinks),
            Box::new(NoBudget),
        )
        .expect("the bootstrap config is servable");
        let reconciler = Arc::new(Reconciler::new(
            store as Arc<dyn ControlPlaneStore>,
            Arc::new(RevisionCompiler::with_secrets(
                bootstrap(),
                env(),
                AliasProjection { provider: "openai" },
                deployment.materialization(),
            )),
            Arc::new(state.clone()),
            settings(),
            cache,
            Arc::new(SystemClock),
        ));
        Self { reconciler, state }
    }

    fn generation(&self) -> u64 {
        self.state.config().generation
    }

    /// The aliases this replica is serving right now, as the projection compiled
    /// them.
    fn served_aliases(&self) -> Vec<String> {
        self.state
            .config()
            .config
            .model
            .iter()
            .map(|model| model.name.clone())
            .collect()
    }
}

/// The material the qualification credentials authenticate with.
///
/// A literal, and deliberately an obviously fake one: it is sealed under a
/// throwaway KEK, written to a schema this run created, and never reaches an
/// artifact — the evidence records references and counts, never material.
const QUALIFICATION_MATERIAL: &str = "sk-recovery-qualification-not-a-live-key";

/// The deployment KEK for one qualification run.
///
/// Generated per process from a fixed pattern rather than read from the
/// environment: the harness seals material it staged itself, so a key that
/// outlives the run would be a key somebody could be tempted to reuse.
fn qualification_kek() -> DeploymentKek {
    DeploymentKek::parse(
        KekRef("AXOND_RECOVERY_QUALIFICATION_KEK".to_owned()),
        &BASE64.encode([0x5a_u8; 32]),
    )
    .expect("32 base64 bytes are a key")
}

/// Tight but valid pacing. Nothing here polls — every step is an explicit
/// `converge_once` — so these bound the backoff a failed attempt takes rather
/// than the pace of a loop.
fn settings() -> ConvergenceSettings {
    ConvergenceSettings {
        poll_interval: Duration::from_millis(100),
        target: Duration::from_secs(1),
        backoff: BackoffPolicy {
            initial: Duration::from_millis(50),
            max: Duration::from_millis(500),
            multiplier: 2,
        },
    }
}

/// Publish one revision as an administrator would.
async fn publish(
    store: &PostgresControlPlane,
    expected: ExpectedRevision,
    key: &str,
    state: DesiredState,
) -> Result<RevisionManifest, ControlPlaneError> {
    store
        .publish_revision(fixtures::candidate(expected, key, state))
        .await
}

/// A configured DSN the harness cannot splice a link into: a skip locally, a
/// failure where CI promised the stages would run.
///
/// `required` is `AXOND_TEST_REQUIRE_SERVICES=1`, taken as an argument so the
/// decision is testable without mutating the environment of a parallel suite.
fn unusable_dsn(required: bool, reason: &str) -> Option<Deployment> {
    assert!(
        !required,
        "AXOND_TEST_REQUIRE_SERVICES=1 promises the recovery stages ran, but \
         AXOND_TEST_POSTGRES_DSN cannot be qualified: {reason}"
    );
    eprintln!("recovery qualification skipped: {reason}");
    None
}

fn cache(name: &str) -> LastKnownGood {
    LastKnownGood::new(cache_path(name), KEY).expect("a long enough signing key")
}

/// Retain all three revision identities the recovery contract promises. They
/// answer different questions: desired is what the journal asks for, loaded is
/// what this replica hydrated and compiled, and active is what its snapshot is
/// actually serving. Keeping them in every executable recovery-driver artifact
/// makes a partially converged replica distinguishable from one that merely
/// reports the right active revision.
fn observe_revision_report(recorder: &mut Recorder, prefix: &str, report: &RevisionReport) {
    let revision = |id: Option<RevisionId>| {
        id.map_or_else(|| "none".to_owned(), |revision| revision.to_string())
    };
    recorder.observe(
        &format!("{prefix}_desired_revision"),
        revision(report.desired),
    );
    recorder.observe(
        &format!("{prefix}_loaded_revision"),
        revision(report.loaded),
    );
    recorder.observe(
        &format!("{prefix}_active_revision"),
        revision(report.active),
    );
}

#[test]
fn revision_evidence_retains_desired_loaded_and_active() {
    let desired = fixtures::revision_id(1);
    let loaded = fixtures::revision_id(2);
    let active = fixtures::revision_id(3);
    let report = RevisionReport {
        desired: Some(desired),
        loaded: Some(loaded),
        active: Some(active),
        ..RevisionReport::default()
    };
    let mut recorder = Recorder::new(
        "recovery-convergence",
        "journal-recovery",
        RUNNER,
        "recovery_convergence",
        &["revisions"],
        "test-schema",
        "test-identity",
    );

    observe_revision_report(&mut recorder, "replica", &report);
    let artifact = recorder.finish();
    let json = serde_json::to_value(artifact).expect("the test artifact serializes");
    assert_eq!(
        json["observations"]["replica_desired_revision"],
        desired.to_string()
    );
    assert_eq!(
        json["observations"]["replica_loaded_revision"],
        loaded.to_string()
    );
    assert_eq!(
        json["observations"]["replica_active_revision"],
        active.to_string()
    );
}

// ── Failing through the artifact ─────────────────────────────────────────────

/// The step this stage cannot continue past, as a recorded check rather than an
/// unwind: on the failing side the check is written, the artifact is finished,
/// and the stage returns.
///
/// An `expect` here would panic mid-stage and take the evidence with it, which
/// is the failure mode a retained evidence directory cannot describe — the
/// regression arrives as a missing file rather than as an artifact saying which
/// step failed and what came before it.
macro_rules! demand_ok {
    ($recorder:expr, $check:expr, $result:expr, $detail:expr) => {
        match $result {
            Ok(value) => {
                $recorder.require_that($check, true, $detail);
                value
            }
            Err(error) => {
                $recorder.require_that($check, false, format!("{}: refused with {error}", $detail));
                return finish($recorder);
            }
        }
    };
}

/// The same, for a step whose whole point is that it is refused.
macro_rules! demand_err {
    ($recorder:expr, $check:expr, $result:expr, $detail:expr) => {
        match $result {
            Err(error) => {
                $recorder.require_that($check, true, $detail);
                error
            }
            Ok(_) => {
                $recorder.require_that($check, false, format!("{}: it succeeded", $detail));
                return finish($recorder);
            }
        }
    };
}

// ── The stages ───────────────────────────────────────────────────────────────

/// `control-plane-outage/journal-outage`: a converged replica loses the journal.
///
/// The property is that an outage degrades *change*, not what is already
/// serving: the active revision and the compiled snapshot survive the cut
/// untouched, the administrative publish is refused with a category a caller can
/// retry, and the replica's own report says `unavailable` instead of going
/// quiet.
#[tokio::test]
async fn control_plane_outage_journal_outage() {
    let Some(deployment) = Deployment::open().await else {
        return;
    };
    let spec = StageSpec::load("control-plane-outage/journal-outage");
    let mut recorder = spec.recorder(&deployment);

    let administrator = deployment.administrator().await;
    let baseline = demand_ok!(
        recorder,
        "the_journal_accepts_the_baseline",
        publish(
            &administrator,
            ExpectedRevision::Empty,
            "recovery-baseline",
            deployment.materialized(fixtures::state()).await,
        )
        .await,
        "a baseline revision is published before the cut"
    );
    recorder.mark("published", format!("baseline revision {}", baseline.id));

    let replica = Replica::build(&deployment, None).await;
    let active = demand_ok!(
        recorder,
        "the_replica_converges_before_the_outage",
        replica.reconciler.bootstrap().await,
        "the replica reaches the baseline while the journal is reachable"
    );
    recorder.require(
        "the_replica_converged_on_the_baseline",
        baseline.id,
        active,
        "the snapshot the outage must not cost is the baseline",
    );
    let generation_before = replica.generation();
    let aliases_before = replica.served_aliases();
    recorder.mark("converged", format!("active revision {active}"));
    observe_revision_report(&mut recorder, "before_outage", &replica.reconciler.report());
    recorder.observe("active_revision_before_outage", active.to_string());
    recorder.observe("snapshot_generation_before_outage", generation_before);

    deployment.link.sever();
    recorder.mark(
        "severed",
        "the loopback path to the journal was dropped mid-flight; reconnection is refused",
    );

    let refusal = demand_err!(
        recorder,
        "the_publish_is_refused_during_the_outage",
        publish(
            &administrator,
            ExpectedRevision::Exactly(baseline.id),
            "recovery-during-outage",
            deployment
                .materialized(fixtures::state_with_renamed_alias())
                .await,
        )
        .await,
        "an administrative write cannot succeed without the journal"
    );
    let category = category_reason(refusal.category());
    recorder.mark("publish-refused", format!("{category}: {refusal}"));
    recorder.observe("admin_write_outcome", category);
    recorder.observe(
        "admin_write_retryable",
        u64::from(BackendFailure::retryable(&refusal)),
    );

    let outcome = replica.reconciler.converge_once("qualification").await;
    recorder.mark("convergence-failed", format!("{outcome:?}"));
    let report = replica.reconciler.report();
    observe_revision_report(&mut recorder, "during_outage", &report);
    let rejection = demand_ok!(
        recorder,
        "the_failed_attempt_is_reported",
        report.last_rejection.as_ref().ok_or("nothing was reported"),
        "a replica that cannot read the journal says so rather than going quiet"
    );
    recorder.observe("convergence_rejection_reason", rejection.reason);
    // Zero, and the artifact says so rather than omitting the class the manifest
    // promises: a replica that cannot read desired state cannot know it is
    // behind, which is exactly why an outage is measured by failures and a
    // recovery by elapsed time.
    recorder.observe("convergence_lag_seconds", report.lag);
    recorder.observe(
        "consecutive_convergence_failures",
        u64::from(report.consecutive_failures),
    );
    recorder.observe(
        "active_revision_during_outage",
        report
            .active
            .map_or_else(|| "none".to_owned(), |id| id.to_string()),
    );
    recorder.observe("snapshot_generation_during_outage", replica.generation());

    // What the outage must not have cost: the compiled snapshot the replica was
    // already serving.
    recorder.require(
        "the_active_revision_survives_the_cut",
        baseline.id,
        report
            .active
            .map_or_else(|| "none".to_owned(), |id| id.to_string()),
        "the outage degrades change, not what is already serving",
    );
    recorder.require(
        "the_snapshot_generation_does_not_move",
        generation_before,
        replica.generation(),
        "no snapshot was swapped in during the outage",
    );
    recorder.require(
        "the_served_aliases_do_not_change",
        aliases_before.join(","),
        replica.served_aliases().join(","),
        "the routing the replica answers with is the one it converged on",
    );
    recorder.require(
        "the_rejection_names_the_unavailable_journal",
        "unavailable",
        rejection.reason,
        "an unreachable journal is a retryable condition, not an invalid revision",
    );
    recorder.require_that(
        "the_refused_publish_is_retryable",
        BackendFailure::retryable(&refusal),
        "a caller is told to retry rather than to change the request",
    );

    recorder.require(
        "the_refusal_is_categorised_unavailable",
        "unavailable",
        category,
        "the refused publish carries the category an operator retries on, not a request fault",
    );
    let refused_retryably = recorder.held("the_refused_publish_is_retryable")
        && recorder.held("the_refusal_is_categorised_unavailable")
        && recorder.held("the_rejection_names_the_unavailable_journal");
    recorder.gate(
        "admin_writes",
        spec.gate.admin_writes.bound(),
        category,
        spec.gate
            .admin_writes_met(AdminWrites::Unavailable, refused_retryably),
        "the publish was refused with a retryable category and wrote nothing",
    );
    recorder.deferred(
        "max_serving_error_fraction",
        spec.gate.max_serving_error_fraction.to_string(),
        "the blocked `serving` stage offers the requests this ceiling is measured over",
    );
    recorder.deferred(
        "readiness",
        spec.gate.readiness.bound(),
        "the blocked `serving` stage owns the readiness probe; this stage records that the \
         active snapshot and its generation survived the cut",
    );
    recorder.deferred(
        "max_convergence_lag_seconds",
        spec.gate.max_convergence_lag_seconds.to_string(),
        "convergence resumes when the journal returns, which `recovery-convergence` measures",
    );
    recorder.deferred(
        "max_data_loss_revisions",
        spec.gate.max_data_loss_revisions.to_string(),
        "a severed link writes nothing; loss is measured by the restore scenarios",
    );
    recorder.deferred(
        "max_unauthenticated_admin_successes",
        spec.gate.max_unauthenticated_admin_successes.to_string(),
        "the blocked `administration` stage authenticates administrative callers",
    );

    finish(recorder);
}

/// `cold-boot-valid-cache/cold-boot`: a replica boots into the outage with a
/// signed cache.
///
/// The cache is not hand-written: it is the one the *previous* replica exported
/// when it converged, which is the only version of this scenario worth
/// qualifying — a cache produced by the export path, restored by the boot path.
#[tokio::test]
async fn cold_boot_valid_cache_cold_boot() {
    let Some(deployment) = Deployment::open().await else {
        return;
    };
    let spec = StageSpec::load("cold-boot-valid-cache/cold-boot");
    let mut recorder = spec.recorder(&deployment);

    let administrator = deployment.administrator().await;
    let baseline = demand_ok!(
        recorder,
        "the_journal_accepts_the_baseline",
        publish(
            &administrator,
            ExpectedRevision::Empty,
            "cold-boot-cache",
            deployment.materialized(fixtures::state()).await,
        )
        .await,
        "a baseline revision is published before the cache is exported"
    );

    // A converged replica exports its cache; the booting replica reads that file.
    let seeded = cache("valid");
    let path = seeded.path().to_path_buf();
    let warm = Replica::build(&deployment, Some(seeded)).await;
    demand_ok!(
        recorder,
        "the_seeding_replica_converges",
        warm.reconciler.bootstrap().await,
        "the cache under test is one a converged replica exported"
    );
    recorder.mark(
        "cache-exported",
        format!("revision {} written to the signed cache", baseline.id),
    );
    drop(warm);

    let booting = Replica::build(
        &deployment,
        Some(LastKnownGood::new(&path, KEY).expect("the same signing key")),
    )
    .await;
    deployment.link.sever();
    recorder.mark("severed", "the journal is unreachable for the cold boot");
    recorder.observe(
        "boot_note",
        "the store handle is built before the cut, because `connect` refuses an unreachable \
         database; what is qualified is the bootstrap decision between the cache and a refusal",
    );

    let started = Instant::now();
    let restored = demand_ok!(
        recorder,
        "the_cold_boot_restores_from_the_cache",
        booting.reconciler.bootstrap().await,
        "a signed cache is a servable snapshot when the journal is unreachable"
    );
    let took = started.elapsed();
    let report = booting.reconciler.report();
    observe_revision_report(&mut recorder, "after_cold_boot", &report);
    recorder.mark(
        "cold-boot-restored",
        format!(
            "revision {restored} restored from {}",
            report
                .source
                .map_or("unknown", crate::convergence::SnapshotSource::as_str)
        ),
    );
    recorder.observe("cold_start_outcome", "restored");
    recorder.observe("cold_start_seconds", took);
    recorder.observe("restored_revision", restored.to_string());
    recorder.observe(
        "snapshot_source",
        report
            .source
            .map_or("unknown", crate::convergence::SnapshotSource::as_str),
    );
    recorder.observe("snapshot_generation_after_cold_boot", booting.generation());

    recorder.require(
        "the_restored_revision_is_the_cached_one",
        baseline.id,
        restored,
        "the boot restored the revision the previous replica exported",
    );
    recorder.require(
        "the_snapshot_came_from_the_cache",
        SnapshotSource::LastKnownGood.as_str(),
        report
            .source
            .map_or("none", crate::convergence::SnapshotSource::as_str),
        "the journal was unreachable, so the only lawful source is the signed cache",
    );
    recorder.require(
        "the_active_revision_is_the_cached_one",
        baseline.id,
        report
            .active
            .map_or_else(|| "none".to_owned(), |id| id.to_string()),
        "the replica reports what it restored",
    );
    recorder.require_that(
        "the_restored_snapshot_routes_somewhere",
        !booting.served_aliases().is_empty(),
        "a snapshot with no aliases is an empty configuration wearing a revision id",
    );

    let restored_from_the_cache = recorder.held("the_snapshot_came_from_the_cache")
        && recorder.held("the_active_revision_is_the_cached_one");
    recorder.gate(
        "readiness",
        spec.gate.readiness.bound(),
        "restored from last-known-good",
        spec.gate
            .readiness_met(Readiness::Serves, restored_from_the_cache),
        "the booting replica reached a servable snapshot without the journal, from the cache the \
         previous replica exported",
    );
    recorder.deferred(
        "max_serving_error_fraction",
        spec.gate.max_serving_error_fraction.to_string(),
        "the blocked `serving` stage offers requests against the restored snapshot",
    );
    recorder.deferred(
        "max_convergence_lag_seconds",
        spec.gate.max_convergence_lag_seconds.to_string(),
        "a replica serving from cache is not converging; `recovery-convergence` measures the bound",
    );
    recorder.deferred(
        "max_data_loss_revisions",
        spec.gate.max_data_loss_revisions.to_string(),
        "the cache holds one revision by construction; loss is measured by the restore scenarios",
    );
    recorder.deferred(
        "admin_writes",
        spec.gate.admin_writes.bound(),
        "`control-plane-outage/journal-outage` measures the administrative write",
    );
    recorder.deferred(
        "max_unauthenticated_admin_successes",
        spec.gate.max_unauthenticated_admin_successes.to_string(),
        "no administrative surface authenticates callers yet",
    );

    finish(recorder);
}

/// `cold-boot-no-cache/cold-boot`: the same boot with nothing to restore.
///
/// A stateful replica has no implicit empty configuration, so the only correct
/// outcome is a refusal that names the control plane — and, just as importantly,
/// nothing published: a replica that refused readiness while having swapped an
/// empty snapshot in would be serving a gateway with no aliases.
#[tokio::test]
async fn cold_boot_no_cache_cold_boot() {
    let Some(deployment) = Deployment::open().await else {
        return;
    };
    let spec = StageSpec::load("cold-boot-no-cache/cold-boot");
    let mut recorder = spec.recorder(&deployment);

    let administrator = deployment.administrator().await;
    demand_ok!(
        recorder,
        "the_journal_accepts_the_baseline",
        publish(
            &administrator,
            ExpectedRevision::Empty,
            "cold-boot-no-cache",
            deployment.materialized(fixtures::state()).await,
        )
        .await,
        "there is desired state to serve, so the refusal is about the cut and not about an empty \
         journal"
    );

    let booting = Replica::build(&deployment, None).await;
    let generation_before = booting.generation();
    deployment.link.sever();
    recorder.mark("severed", "the journal is unreachable for the cold boot");
    recorder.observe(
        "boot_note",
        "the store handle is built before the cut, because `connect` refuses an unreachable \
         database; what is qualified is the bootstrap decision between the cache and a refusal",
    );

    let started = Instant::now();
    let error = demand_err!(
        recorder,
        "the_cold_boot_is_refused",
        booting.reconciler.bootstrap().await,
        "a replica with no cache and no journal has nothing to serve"
    );
    let took = started.elapsed();
    let report = booting.reconciler.report();
    observe_revision_report(&mut recorder, "after_cold_boot", &report);
    recorder.mark("cold-boot-refused", error.to_string());
    recorder.observe("cold_start_outcome", "refused");
    recorder.observe("cold_start_seconds", took);
    recorder.observe("refusal", error.to_string());
    recorder.observe("snapshot_generation_after_cold_boot", booting.generation());

    let refused_for_the_journal = matches!(error, BootstrapError::Unavailable { .. });
    recorder.require_that(
        "the_refusal_names_the_unreachable_journal",
        refused_for_the_journal,
        format!("an operator is told which dependency is missing: {error}"),
    );
    // Nothing was published: the replica is refusing, not serving an empty
    // configuration behind a failing probe.
    recorder.require(
        "the_snapshot_generation_does_not_move",
        generation_before,
        booting.generation(),
        "a refusing replica publishes nothing, not even an empty configuration",
    );
    recorder.require_that(
        "no_revision_is_reported_active",
        booting.reconciler.report().active.is_none(),
        "a replica that never converged claims no active revision",
    );

    let refused_and_published_nothing = recorder.held("the_refusal_names_the_unreachable_journal")
        && recorder.held("the_snapshot_generation_does_not_move");
    recorder.gate(
        "readiness",
        spec.gate.readiness.bound(),
        "refused: control plane unreachable, no cache",
        spec.gate
            .readiness_met(Readiness::Refuses, refused_and_published_nothing),
        "boot refused and published nothing, so no empty configuration reached the snapshot",
    );
    recorder.deferred(
        "max_serving_error_fraction",
        spec.gate.max_serving_error_fraction.to_string(),
        "a refusing scenario offers no traffic, so the ceiling is vacuous by contract",
    );
    recorder.deferred(
        "max_convergence_lag_seconds",
        spec.gate.max_convergence_lag_seconds.to_string(),
        "a replica that never became ready is not converging",
    );
    recorder.deferred(
        "max_data_loss_revisions",
        spec.gate.max_data_loss_revisions.to_string(),
        "a refused boot writes nothing; loss is measured by the restore scenarios",
    );
    recorder.deferred(
        "admin_writes",
        spec.gate.admin_writes.bound(),
        "`control-plane-outage/journal-outage` measures the administrative write",
    );
    recorder.deferred(
        "max_unauthenticated_admin_successes",
        spec.gate.max_unauthenticated_admin_successes.to_string(),
        "the blocked `readiness` stage owns the probe an operator's tooling calls",
    );

    finish(recorder);
}

/// `cold-boot-invalid-cache/cold-boot`: every way a cache can fail its own
/// authentication.
///
/// Three variants rather than one, because they are three different operator
/// stories — an edited record, a replica handed the wrong signing key, and a
/// file that lost its tail to a crash — and a boot that accepted any of them
/// would serve state nobody published.
#[tokio::test]
async fn cold_boot_invalid_cache_cold_boot() {
    let Some(deployment) = Deployment::open().await else {
        return;
    };
    let spec = StageSpec::load("cold-boot-invalid-cache/cold-boot");
    let mut recorder = spec.recorder(&deployment);

    let administrator = deployment.administrator().await;
    demand_ok!(
        recorder,
        "the_journal_accepts_the_baseline",
        publish(
            &administrator,
            ExpectedRevision::Empty,
            "cold-boot-invalid",
            deployment.materialized(fixtures::state()).await,
        )
        .await,
        "the cache under test is damaged from an authentic one, not invented"
    );

    // One authentic cache, exported by a converged replica, then damaged three
    // ways.
    let seeded = cache("invalid");
    let authentic = seeded.path().to_path_buf();
    let warm = Replica::build(&deployment, Some(seeded)).await;
    demand_ok!(
        recorder,
        "the_seeding_replica_converges",
        warm.reconciler.bootstrap().await,
        "an authentic cache is exported before it is damaged"
    );
    drop(warm);
    let bytes = demand_ok!(
        recorder,
        "the_exported_cache_is_readable",
        std::fs::read(&authentic),
        "the damaged variants are made from the file the replica wrote"
    );
    recorder.mark("cache-exported", format!("{} bytes", bytes.len()));

    let mut edited = bytes.clone();
    let last = edited.len() - 1;
    edited[last] ^= 0x01;
    let mut truncated = bytes.clone();
    truncated.truncate(bytes.len() / 2);

    let variants: [(&str, Vec<u8>, &[u8]); 3] = [
        ("edited-record", edited, KEY),
        (
            "foreign-signing-key",
            bytes.clone(),
            b"a-different-key-of-the-same-length--",
        ),
        ("truncated-file", truncated, KEY),
    ];

    // The damaged caches and the replicas that will read them are prepared while
    // the journal is still reachable, for the reason `boot_note` records.
    let mut booting = Vec::new();
    for (variant, content, key) in variants {
        let path = cache_path(variant);
        std::fs::write(&path, &content).expect("the damaged cache is writable");
        let replica = Replica::build(
            &deployment,
            Some(LastKnownGood::new(&path, key).expect("a long enough signing key")),
        )
        .await;
        booting.push((variant, path, replica));
    }

    deployment.link.sever();
    recorder.mark("severed", "the journal is unreachable for the cold boot");
    recorder.observe(
        "boot_note",
        "the store handle is built before the cut, because `connect` refuses an unreachable \
         database; what is qualified is the bootstrap decision between the cache and a refusal",
    );

    let mut refusals = 0u64;
    for (variant, path, booting) in booting {
        let generation_before = booting.generation();

        let error = demand_err!(
            recorder,
            "the_unauthentic_cache_is_refused",
            booting.reconciler.bootstrap().await,
            format!("{variant}: a cache that fails its authentication is not a snapshot")
        );
        let cache_refused = matches!(error, BootstrapError::Cache { .. });
        observe_revision_report(
            &mut recorder,
            &format!("{variant}_after_cold_boot"),
            &booting.reconciler.report(),
        );
        recorder.require_that(
            "the_refusal_names_the_cache",
            cache_refused,
            format!(
                "{variant}: an operator is told the cache is the problem, not the journal: {error}"
            ),
        );
        recorder.require(
            "the_snapshot_generation_does_not_move",
            generation_before,
            booting.generation(),
            format!("{variant}: nothing unauthentic reached the served snapshot"),
        );
        recorder.require_that(
            "no_revision_is_reported_active",
            booting.reconciler.report().active.is_none(),
            format!("{variant}: the replica claims no active revision after refusing"),
        );
        if cache_refused {
            refusals += 1;
        }

        recorder.mark(
            &format!("cold-boot-refused-{variant}"),
            format!("{error} ({error:?})"),
        );
        recorder.observe(
            &format!("refusal_{}", variant.replace('-', "_")),
            error.to_string(),
        );
        let _ = std::fs::remove_file(&path);
    }
    let _ = std::fs::remove_file(&authentic);

    recorder.observe("cold_start_outcome", "refused");
    recorder.observe("unauthentic_cache_variants_refused", refusals);

    recorder.gate(
        "readiness",
        spec.gate.readiness.bound(),
        format!("{refusals}/3 unauthentic caches refused the boot"),
        spec.gate.readiness_met(Readiness::Refuses, refusals == 3),
        "an edited record, a foreign signing key, and a truncated file each refused the boot and \
         published nothing",
    );
    recorder.deferred(
        "max_serving_error_fraction",
        spec.gate.max_serving_error_fraction.to_string(),
        "a refusing scenario offers no traffic, so the ceiling is vacuous by contract",
    );
    recorder.deferred(
        "max_convergence_lag_seconds",
        spec.gate.max_convergence_lag_seconds.to_string(),
        "a replica that never became ready is not converging",
    );
    recorder.deferred(
        "max_data_loss_revisions",
        spec.gate.max_data_loss_revisions.to_string(),
        "a refused boot writes nothing; loss is measured by the restore scenarios",
    );
    recorder.deferred(
        "admin_writes",
        spec.gate.admin_writes.bound(),
        "`control-plane-outage/journal-outage` measures the administrative write",
    );
    recorder.deferred(
        "max_unauthenticated_admin_successes",
        spec.gate.max_unauthenticated_admin_successes.to_string(),
        "the blocked `readiness` stage owns the probe an operator's tooling calls",
    );

    finish(recorder);
}

/// `recovery-convergence/journal-recovery`: the journal comes back, holding
/// revisions the fleet never saw.
///
/// The fleet here is the two replicas the outage produced — the one that kept
/// serving and the one that cold-booted from cache — and the property is that
/// neither needs an operator: the same convergence step that failed during the
/// outage succeeds afterwards, both reach the head revision, and the lag they
/// reported goes back to zero inside the declared bound.
#[tokio::test]
async fn recovery_convergence_journal_recovery() {
    let Some(deployment) = Deployment::open().await else {
        return;
    };
    let spec = StageSpec::load("recovery-convergence/journal-recovery");
    let mut recorder = spec.recorder(&deployment);

    let administrator = deployment.administrator().await;
    let baseline = demand_ok!(
        recorder,
        "the_journal_accepts_the_baseline",
        publish(
            &administrator,
            ExpectedRevision::Empty,
            "recovery-head-baseline",
            deployment.materialized(fixtures::state()).await,
        )
        .await,
        "the fleet has a revision to converge on before the cut"
    );

    let survivor = Replica::build(&deployment, Some(cache("survivor"))).await;
    demand_ok!(
        recorder,
        "the_surviving_replica_converges_before_the_outage",
        survivor.reconciler.bootstrap().await,
        "one replica enters the outage already serving the baseline"
    );
    let cold_cache = cache("cold-booter");
    let cold_path = cold_cache.path().to_path_buf();
    let seeding = Replica::build(&deployment, Some(cold_cache)).await;
    demand_ok!(
        recorder,
        "the_second_replica_exports_a_cache",
        seeding.reconciler.bootstrap().await,
        "the other replica enters the outage with a cache to boot from"
    );
    drop(seeding);
    recorder.mark("converged", format!("fleet at revision {}", baseline.id));

    // Built before the cut, for the reason the cold-boot stages record: `connect`
    // refuses an unreachable database, so a replica's store handle cannot be
    // created during the outage.
    let cold_booter = Replica::build(
        &deployment,
        Some(LastKnownGood::new(&cold_path, KEY).expect("the same signing key")),
    )
    .await;
    deployment.link.sever();
    recorder.mark("severed", "the fleet loses the journal");
    let restored_from_cache = demand_ok!(
        recorder,
        "the_cold_booting_replica_restores_its_cache",
        cold_booter.reconciler.bootstrap().await,
        "the fleet that has to converge is one survivor and one cold boot"
    );
    recorder.require(
        "the_cold_boot_restores_the_baseline",
        baseline.id,
        restored_from_cache,
        "both replicas enter the recovery from the same revision",
    );
    recorder.require_that(
        "convergence_fails_while_the_journal_is_gone",
        matches!(
            survivor.reconciler.converge_once("qualification").await,
            crate::convergence::Outcome::Rejected { .. }
        ),
        "the step that succeeds after the recovery is the step that failed during the outage",
    );

    // The journal is untouched by the cut, so an administrator connected to it
    // directly keeps publishing: this is the fleet arriving to find revisions it
    // never saw, which is what recovery has to reconcile.
    let mut head = baseline.id;
    for (index, state) in [
        deployment
            .materialized(fixtures::state_with_renamed_alias())
            .await,
        deployment.materialized(fixtures::state_with_policy()).await,
    ]
    .into_iter()
    .enumerate()
    {
        head = demand_ok!(
            recorder,
            "the_journal_keeps_accepting_writes_from_elsewhere",
            publish(
                &direct_administrator(&deployment).await,
                ExpectedRevision::Exactly(head),
                &format!("recovery-during-outage-{index}"),
                state,
            )
            .await,
            "the fleet returns to a journal that moved on without it"
        )
        .id;
    }
    recorder.mark(
        "published-during-outage",
        format!("the journal advanced to {head} while the fleet was disconnected"),
    );
    recorder.observe("revisions_published_during_outage", 2u64);

    demand_ok!(
        recorder,
        "the_link_comes_back_on_the_same_dsn",
        deployment.link.restore().await,
        "the recovery is the dependency returning, not the replicas being reconfigured"
    );
    recorder.mark("restored", "the journal is reachable again on the same DSN");

    let accepted = demand_ok!(
        recorder,
        "the_publish_is_accepted_after_the_recovery",
        publish_until_accepted(
            &administrator,
            ExpectedRevision::Exactly(head),
            "recovery-after-restore",
            deployment
                .materialized(fixtures::state_with_second_tenant())
                .await,
        )
        .await,
        "administrative writes are accepted once the journal returns"
    );
    head = accepted.id;
    recorder.mark("publish-accepted", format!("head revision {head}"));
    recorder.observe("admin_write_outcome", "accepted");

    let started = Instant::now();
    let mut converged = Vec::new();
    for (name, replica) in [("survivor", &survivor), ("cold-booter", &cold_booter)] {
        let outcome = converge_until_head(replica, head).await;
        let report = replica.reconciler.report();
        recorder.require(
            "the_replica_reaches_the_head",
            head,
            report
                .active
                .map_or_else(|| "none".to_owned(), |id| id.to_string()),
            format!("{name} converged onto the revisions published while it was disconnected"),
        );
        recorder.require_that(
            "the_replica_reports_itself_converged",
            report.converged(),
            format!("{name} says it is at desired state rather than only being at it"),
        );
        recorder.mark(
            &format!("converged-{name}"),
            format!("{outcome:?} after the journal returned"),
        );
        observe_revision_report(&mut recorder, name, &report);
        recorder.observe(&format!("{name}_convergence_lag_seconds"), report.lag);
        recorder.observe(
            &format!("{name}_snapshot_source"),
            report
                .source
                .map_or("unknown", crate::convergence::SnapshotSource::as_str),
        );
        converged.push(report.lag);
    }
    let recovery = started.elapsed();
    let worst_lag = converged.iter().copied().max().unwrap_or_default();
    recorder.observe("fleet_recovery_seconds", recovery);
    recorder.observe("worst_residual_lag_seconds", worst_lag);

    // Nothing the journal accepted — before, during, or after the outage — was
    // lost, and the chain the fleet converged onto is the one it holds.
    let trail = demand_ok!(
        recorder,
        "the_audit_trail_survives_the_outage",
        administrator.audit_trail(head).await,
        "the head published after the recovery carries its audit"
    );
    recorder.observe("audit_events_for_head", trail.len() as u64);
    let mut surviving = 0u64;
    let mut walked = Some(head);
    while let Some(id) = walked {
        let manifest = demand_ok!(
            recorder,
            "every_published_revision_is_still_readable",
            administrator.load_manifest(id).await,
            "the chain the fleet converged onto is walked to its root"
        );
        surviving += 1;
        walked = manifest.parent;
    }
    recorder.observe("revisions_readable_after_recovery", surviving);
    recorder.require(
        "no_revision_is_lost_across_the_outage",
        4u64,
        surviving,
        "the baseline, two outage-window revisions, and the post-recovery head all survive",
    );

    // The bound is how long a replica may still be behind desired state after
    // the journal returns, so the measurement is the elapsed time from the
    // post-restore publish until every replica is at the head. `worst_lag` is
    // the *residual* lag of an already converged replica: structurally zero, and
    // therefore an observation rather than a gate.
    let bound = Duration::from_secs(spec.gate.max_convergence_lag_seconds);
    recorder.gate(
        "max_convergence_lag_seconds",
        spec.gate.max_convergence_lag_seconds.to_string(),
        format!("{:.3}", recovery.as_secs_f64()),
        recovery <= bound,
        "both replicas converged to the head revision without intervention within the bound once \
         the journal returned",
    );
    let publish_recovered = recorder.held("the_publish_is_accepted_after_the_recovery");
    recorder.gate(
        "admin_writes",
        spec.gate.admin_writes.bound(),
        "accepted",
        spec.gate
            .admin_writes_met(AdminWrites::Accepted, publish_recovered),
        "the publish refused during the outage succeeded against the recovered journal",
    );
    let nothing_was_lost = recorder.held("no_revision_is_lost_across_the_outage")
        && recorder.held("the_audit_trail_survives_the_outage");
    recorder.gate(
        "max_data_loss_revisions",
        spec.gate.max_data_loss_revisions.to_string(),
        "0",
        nothing_was_lost,
        "every revision the journal accepted before, during, and after the outage is readable, \
         and the head's audit trail came back with it",
    );
    recorder.deferred(
        "max_serving_error_fraction",
        spec.gate.max_serving_error_fraction.to_string(),
        "the blocked `serving` stage offers the requests this ceiling is measured over",
    );
    recorder.deferred(
        "readiness",
        spec.gate.readiness.bound(),
        "the blocked `serving` stage owns the readiness probe",
    );
    recorder.deferred(
        "max_unauthenticated_admin_successes",
        spec.gate.max_unauthenticated_admin_successes.to_string(),
        "the blocked `administration` stage authenticates administrative callers",
    );

    finish(recorder);
}

/// A second administrator, connected after the cut: the fleet's link is severed,
/// the database is not, and this is what keeps publishing while they are
/// disconnected.
async fn direct_administrator(deployment: &Deployment) -> PostgresControlPlane {
    let dsn = crate::test_services::postgres_dsn().expect("a configured database");
    PostgresControlPlane::connect(
        &dsn,
        ControlPlaneSettings {
            schema: Some(deployment.schema.clone()),
            migrate: false,
            connect_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(10),
            ..ControlPlaneSettings::default()
        },
    )
    .await
    .expect("the database itself is reachable throughout")
}

/// Publish through a link that was just restored, retrying while the refusal is
/// the dead connection the cut left behind.
///
/// The administrator here reaches the journal through the severed link, so its
/// first statement after `restore()` can meet the same dead socket a replica
/// does — which is why convergence is retried. Retrying only `unavailable`
/// keeps the assertion honest: a rejected precondition or an invalid revision
/// still fails immediately, on the first attempt.
async fn publish_until_accepted(
    store: &PostgresControlPlane,
    expected: ExpectedRevision,
    key: &str,
    state: DesiredState,
) -> Result<RevisionManifest, ControlPlaneError> {
    let mut last = publish(store, expected, key, state.clone()).await;
    for _ in 0..50 {
        match last {
            Ok(manifest) => return Ok(manifest),
            Err(failure) if BackendFailure::retryable(&failure) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                last = publish(store, expected, key, state.clone()).await;
            }
            Err(failure) => return Err(failure),
        }
    }
    last
}

/// Converge until the replica is serving `head`, or give up loudly.
///
/// A bounded retry rather than a sleep: the first attempt after a restored link
/// can still meet the connection the cut left dead, and the reconnect is the
/// behaviour under test, not an obstacle to it.
async fn converge_until_head(replica: &Replica, head: RevisionId) -> crate::convergence::Outcome {
    let mut last = replica.reconciler.converge_once("qualification").await;
    for _ in 0..50 {
        if replica.reconciler.report().active == Some(head) {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        last = replica.reconciler.converge_once("qualification").await;
    }
    last
}

/// Write the artifact, print where it landed, and fail on any gate this stage
/// evaluated and did not meet.
fn finish(recorder: Recorder) {
    let artifact = recorder.finish();
    let path = artifact.write();
    println!("{} -> {}", artifact.summary(), path.display());
    // Evidence is retained and published as a CI artifact, so the rule that no
    // material reaches it is checked rather than trusted.
    let retained = std::fs::read_to_string(&path).expect("the artifact just written is readable");
    assert!(
        !retained.contains(QUALIFICATION_MATERIAL),
        "{}: an artifact must retain references and counts, never secret material",
        path.display()
    );
    let failures = artifact.failures();
    assert!(
        failures.is_empty(),
        "recovery gates failed: {failures:#?} (evidence: {})",
        path.display()
    );
}

// ── The honesty gate ─────────────────────────────────────────────────────────

/// The claim this whole harness rests on: the manifest's `executable` stages and
/// the stages the driver runs are the same set. Marking a stage executable
/// without a driver fails here, and writing a driver without marking the stage
/// fails here too.
#[test]
fn the_driver_runs_exactly_the_stages_the_manifest_calls_executable() {
    let manifest = manifest();
    let mut executable: Vec<String> = Vec::new();
    for scenario in &manifest.scenarios {
        for stage in &scenario.stages {
            if stage.status == "executable" && stage.runner.as_deref() == Some(RUNNER) {
                executable.push(format!("{}/{}", scenario.id, stage.id));
            }
        }
    }
    executable.sort();
    let mut driven: Vec<String> = DRIVEN_STAGES.iter().map(|key| (*key).to_owned()).collect();
    driven.sort();
    assert_eq!(
        executable, driven,
        "the manifest and the driver disagree about which `{RUNNER}` stages run"
    );
}

/// The manifest is the contract for the non-numeric bounds too: a stage records
/// and evaluates the `readiness` and `admin_writes` it read, so editing the
/// manifest changes the verdict rather than leaving a literal in the driver.
///
/// The edit here is the one that would otherwise pass silently: telling
/// `cold-boot-no-cache` to serve, which a refusing replica cannot satisfy.
#[test]
fn editing_a_non_numeric_gate_changes_the_verdict() {
    let text = std::fs::read_to_string(
        super::evidence::workspace_root().join("qualification/recovery/manifest.toml"),
    )
    .expect("the recovery manifest is readable");
    let declared = |manifest: &Manifest, id: &str| -> Gate {
        manifest
            .scenarios
            .iter()
            .find(|scenario| scenario.id == id)
            .unwrap_or_else(|| panic!("the manifest declares `{id}`"))
            .gate
    };

    // As the contract stands: a refusal is the bound, and observing one meets it.
    let gate = declared(&toml_manifest(&text), "cold-boot-no-cache");
    assert_eq!(gate.readiness.bound(), "refuses");
    assert!(gate.readiness_met(Readiness::Refuses, true));
    assert!(!gate.readiness_met(Readiness::Serves, true));

    // Flip that one scenario's bound. The bound the artifact echoes follows the
    // edit, and the refusal the driver observes no longer meets it.
    let flipped = text.replacen(
        "readiness = \"refuses\"\nadmin_writes = \"unavailable\"",
        "readiness = \"serves\"\nadmin_writes = \"accepted\"",
        1,
    );
    assert_ne!(flipped, text, "the edit must reach the first refusing gate");
    let gate = declared(&toml_manifest(&flipped), "cold-boot-no-cache");
    assert_eq!(gate.readiness.bound(), "serves");
    assert_eq!(gate.admin_writes.bound(), "accepted");
    assert!(
        !gate.readiness_met(Readiness::Refuses, true),
        "a stage observing a refusal must fail a manifest that demands serving"
    );
    assert!(
        !gate.admin_writes_met(AdminWrites::Unavailable, true),
        "a stage observing an unavailable write must fail a manifest that demands acceptance"
    );
}

/// A DSN this harness cannot cut is a skip locally and a failure in CI.
///
/// `AXOND_TEST_REQUIRE_SERVICES=1` is the promise that a green run means the
/// stages ran, so silently skipping under it would let a DNS hiccup or a DSN
/// shape change turn five recovery checks into no-ops behind a green tick —
/// with `if-no-files-found: warn` on the upload, nothing else would notice.
#[test]
fn an_unusable_dsn_skips_locally_and_fails_where_services_are_required() {
    assert!(unusable_dsn(false, "a Unix socket has no link to cut").is_none());
    let required = std::panic::catch_unwind(|| unusable_dsn(true, "a Unix socket has no link"));
    assert!(
        required.is_err(),
        "a run that promised the services must fail rather than skip"
    );
}

/// Every stage the driver runs is a stage the manifest declares, and it is
/// spelled the same way: a driver writing an artifact for `scenario/stage` a
/// reader cannot find in the contract is evidence about nothing.
#[test]
fn every_driven_stage_resolves_against_the_manifest() {
    for key in DRIVEN_STAGES {
        let spec = StageSpec::load(key);
        assert_eq!(format!("{}/{}", spec.scenario, spec.stage), key);
        assert!(
            !spec.evidence.is_empty(),
            "{key}: a driven stage retains at least one evidence class"
        );
    }
}
