//! The fixtures the secret-redaction tests are driven from.
//!
//! Three things live here, and nothing else: the sentinel material, a compiler
//! that resolves desired-state credentials through a `SecretStore`, and a fake
//! provider that records what it was authenticated with.
//!
//! The compiler needs a word of explanation. Unwrapping material is production
//! code — [`SecretMaterialization`] resolves a candidate's exact versions during
//! compilation and hands the snapshot a [`ResolvedSecrets`] that keeps them
//! alive — and [`SecretResolvingCompiler`] uses it rather than reimplementing
//! it, so every retention, rotation, and zeroization property asserted here is
//! asserted about the shipped seam.
//!
//! The pool wiring is production code too: [`RuntimeProjection`] emits the
//! `[[credential]]` entries a provider call leases from, each naming the exact
//! secret version it is authenticated with, and the snapshot fills them from the
//! same [`ResolvedSecrets`] it retains. So a request that reaches the fake
//! provider with the sentinel key proves the shipped path carried it there.
//!
//! Two things the harness still supplies, both owed by other slices and neither
//! touching material:
//!
//! - **an alias's targets**, because projecting a catalogue is its own slice;
//! - **which namespace an inbound key binds to**, because binding a caller to a
//!   projected namespace is the principal slice's (#252). A projected project is
//!   reached by a qualified id no `axond.toml` can declare, so the harness
//!   rebinds the bootstrap key to it rather than inventing an identity model.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::routing::post;
use serde_json::json;
use tokio::sync::{Semaphore, mpsc};

use super::sweep::LeakSweep;
use crate::backends::control_plane::ControlPlaneStore;
use crate::backends::fakes::InMemorySecrets;
use crate::backends::secrets::{SecretMaterial, SecretResolver as _, SecretStore};
use crate::budget::NoBudget;
use crate::config::{Config, Model, Target};
use crate::convergence::compile::{CandidateCompiler, CompileError, RevisionProjection};
use crate::convergence::credentials::RuntimeProjection;
use crate::convergence::secrets::{MaterialLedger, SecretMaterialization};
use crate::convergence::status::testing::ManualClock;
use crate::convergence::{BackoffPolicy, ConvergenceSettings, Outcome, Reconciler};
use crate::desired_state::credentials::ProviderCredentialBody;
use crate::desired_state::models::WireFamily;
use crate::desired_state::oracle::InMemoryControlPlane;
use crate::desired_state::providers::ProviderBody;
use crate::desired_state::{
    CanonicalValue, DesiredState, ExpectedRevision, LoadedRevision, ResourceBody, ResourceKind,
    ResourceRef, ResourceScope, ResourceVersion, ResourceVersionNumber, RevisionId,
    SecretLifecycle, SecretOwner, SecretRef, Slug, fixtures,
};
use crate::state::{AppState, ConfigSnapshot};
use crate::telemetry;
use crate::usage::{UsageFanout, UsageRecord, UsageSink};

/// The provider key a credential's first secret version holds.
///
/// Shaped like a real provider key — a recognisable prefix and a long
/// high-entropy tail — because that shape is what makes a *partial* disclosure
/// dangerous, and the sweep's fragment search exists for exactly that.
pub(crate) const PROVIDER_MATERIAL: &str = "sk-axond-sentinel-provider-6f21a9d0c7b4";

/// The key the credential's second version holds, after a rotation.
pub(crate) const ROTATED_MATERIAL: &str = "sk-axond-sentinel-rotated-b48c37e1590a";

/// The inbound gateway key callers authenticate to the replica with.
///
/// Swept alongside the provider material because a redaction bug is rarely
/// specific to one kind of secret: a log line that renders an `Authorization`
/// header leaks whichever key was in it.
pub(crate) const INBOUND_MATERIAL: &str = "axond-sentinel-inbound-2c8a11f5e304";

/// A sweep over every sentinel this module family puts into the system.
pub(crate) fn sweep() -> LeakSweep {
    LeakSweep::of([
        ("provider", PROVIDER_MATERIAL),
        ("rotated", ROTATED_MATERIAL),
        ("inbound", INBOUND_MATERIAL),
    ])
}

/// A servable bootstrap pointed at `base_url`, with no credential of its own.
///
/// The credential section is deliberately empty: every provider credential in
/// these tests arrives from desired state and is resolved through the secret
/// store, so a snapshot that can authenticate to the fake provider proves the
/// resolution happened.
pub(crate) fn bootstrap(base_url: &str) -> Config {
    Config::from_toml_str(&format!(
        r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "{base_url}"

[[gateway_key]]
env = "AXOND_SENTINEL_INBOUND"
namespace = "platform"
"#
    ))
    .expect("a valid bootstrap config")
}

/// The boot environment: the inbound key, and nothing else.
pub(crate) fn bootstrap_env() -> HashMap<String, String> {
    HashMap::from([(
        "AXOND_SENTINEL_INBOUND".to_owned(),
        INBOUND_MATERIAL.to_owned(),
    )])
}

/// The namespace the fixture tenant's project is projected as: what a request
/// names to reach the credential this suite resolves.
pub(crate) const SERVING_NAMESPACE: &str = "acme/core";

/// A compiler that resolves each revision's credential references through a
/// secret store, then builds a whole snapshot from the result.
pub(crate) struct SecretResolvingCompiler {
    bootstrap: Config,
    env: HashMap<String, String>,
    materialization: Arc<SecretMaterialization>,
    /// The bootstrap provider aliases are pointed at.
    provider: &'static str,
    resolutions: AtomicUsize,
}

impl SecretResolvingCompiler {
    pub(crate) fn new(bootstrap: Config, secrets: Arc<dyn SecretStore>) -> Self {
        Self {
            bootstrap,
            env: bootstrap_env(),
            materialization: Arc::new(SecretMaterialization::new(secrets, MaterialLedger::new())),
            provider: "openai",
            resolutions: AtomicUsize::new(0),
        }
    }

    /// The ledger the production materialization registers unwrapped versions
    /// in: references only, which is how a test observes retention and
    /// destruction without observing material.
    pub(crate) fn ledger(&self) -> &Arc<MaterialLedger> {
        self.materialization.ledger()
    }

    /// How many times material has crossed the store boundary.
    ///
    /// Counted in distinct secret versions unwrapped, which is what the
    /// materialization actually reads: a version two credentials share is one
    /// read, not two. The number a test asserts on to show resolution happens
    /// per compilation — off the request path — rather than per request.
    pub(crate) fn resolutions(&self) -> usize {
        self.resolutions.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl CandidateCompiler for SecretResolvingCompiler {
    async fn compile(
        &self,
        revision: &LoadedRevision,
        generation: u64,
    ) -> Result<ConfigSnapshot, CompileError> {
        let id = revision.id();
        let projection = |source| CompileError::Projection {
            revision: id,
            source,
        };
        // The shipped projection: the revision's projects become namespaces and
        // its active credentials become the pools serving them, each naming the
        // exact version its material comes from.
        let mut config = RuntimeProjection
            .project(&self.bootstrap, revision.state(), id)
            .map_err(projection)?;
        // All of the candidate's material or none of it, resolved by the shipped
        // materialization: a version it cannot unwrap is a refusal here, before
        // anything is published.
        let resolved = self
            .materialization
            .resolve(revision.state())
            .await
            .map_err(projection)?;
        // Counted here, not per credential: the materialization unwraps each
        // distinct version once, so two credentials pinning one version are one
        // crossing of the store boundary and must count as one.
        self.resolutions
            .fetch_add(resolved.len(), Ordering::Relaxed);
        // Owed by #252: a projected namespace is reached by a qualified id, and
        // nothing yet binds an inbound caller to one, so the suite's key is bound
        // to the namespace the fixture project projects as.
        assert!(
            config
                .namespace
                .iter()
                .any(|namespace| namespace.id == SERVING_NAMESPACE),
            "the fixture project must project as {SERVING_NAMESPACE}"
        );
        for key in &mut config.gateway_key {
            key.namespace = SERVING_NAMESPACE.to_owned();
        }
        for resource in revision.state().resources() {
            if resource.reference.kind != ResourceKind::Alias {
                continue;
            }
            config.model.push(Model {
                name: resource.slug.as_str().to_owned(),
                namespace: None,
                targets: vec![Target {
                    provider: self.provider.to_owned(),
                    model: "gpt-4o".to_owned(),
                    price: gateway_core::catalog::ModelPrice {
                        input_microdollars_per_million: 1_000_000,
                        output_microdollars_per_million: 2_000_000,
                        reasoning_microdollars_per_million: None,
                        cache_read_microdollars_per_million: None,
                        cache_write_microdollars_per_million: None,
                    },
                    catalog: None,
                }],
            });
        }
        config
            .validate_compiled()
            .map_err(|source| CompileError::Validation {
                revision: id,
                source,
            })?;
        // The snapshot takes the resolved set, so the material stays alive for
        // exactly as long as this snapshot can be serving a request and is
        // zeroized when the last holder drops it.
        ConfigSnapshot::build_with(config, &self.env, generation, resolved).map_err(|source| {
            CompileError::Snapshot {
                revision: id,
                source,
            }
        })
    }
}

/// Material as an administrator hands it to the store.
pub(crate) fn material(plaintext: &str) -> SecretMaterial {
    SecretMaterial::new(plaintext.to_owned())
}

/// A sink that keeps every record, so the billing surface can be swept — in
/// memory by [`super::request_path`], and on disk by [`super::journal`] once the
/// same record has been through the outbox.
#[derive(Clone, Default)]
pub(crate) struct CapturingSink(Arc<Mutex<Vec<UsageRecord>>>);

impl CapturingSink {
    pub(crate) fn records(&self) -> Vec<UsageRecord> {
        self.0.lock().expect("not poisoned").clone()
    }
}

#[async_trait]
impl UsageSink for CapturingSink {
    fn name(&self) -> &'static str {
        "capture"
    }

    async fn record(&self, record: &UsageRecord) {
        self.0.lock().expect("not poisoned").push(record.clone());
    }
}

/// A stand-in provider that records the credential it was presented with.
///
/// Recording is the point: an assertion that a *response* is clean proves
/// nothing unless the key really was used, and the recorded header is what the
/// sweep's tripwire is checked against.
pub(crate) struct FakeProvider {
    pub(crate) base_url: String,
    presented: Arc<Mutex<Vec<String>>>,
    /// Handed one permit per request that is allowed to answer. `None` when the
    /// provider answers immediately.
    release: Option<Arc<Semaphore>>,
    arrivals: tokio::sync::Mutex<mpsc::UnboundedReceiver<()>>,
}

impl FakeProvider {
    /// A provider that answers every request straight away.
    pub(crate) async fn serving() -> Self {
        Self::spawn(None).await
    }

    /// A provider that holds every request until it is released, so a test can
    /// keep one in flight across a rotation.
    pub(crate) async fn gated() -> Self {
        Self::spawn(Some(Arc::new(Semaphore::new(0)))).await
    }

    /// A provider that is not there, so a dispatch to it fails at the transport.
    ///
    /// Transport failure is the interesting error case for redaction because the
    /// error value carries the request that failed, and a request carries a
    /// credential.
    ///
    /// The address is a privileged loopback port rather than an ephemeral one
    /// that was bound and released: releasing a port only makes it *probably*
    /// free, and the sibling providers in this suite bind `127.0.0.1:0`
    /// concurrently, so the kernel could hand one of them the very port this
    /// dispatch was supposed to be refused by. Nothing in the suite can bind
    /// port 1, so the refusal is immediate and certain.
    pub(crate) fn unreachable() -> Self {
        let (_, arrivals) = mpsc::unbounded_channel();
        Self {
            base_url: "http://127.0.0.1:1".to_owned(),
            presented: Arc::new(Mutex::new(Vec::new())),
            release: None,
            arrivals: tokio::sync::Mutex::new(arrivals),
        }
    }

    async fn spawn(release: Option<Arc<Semaphore>>) -> Self {
        let presented: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let (arrived, arrivals) = mpsc::unbounded_channel();
        let handler = {
            let presented = Arc::clone(&presented);
            let release = release.clone();
            move |headers: HeaderMap, _: axum::body::Bytes| {
                let presented = Arc::clone(&presented);
                let release = release.clone();
                let arrived = arrived.clone();
                async move {
                    let credential = headers
                        .get(axum::http::header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_owned();
                    presented.lock().expect("not poisoned").push(credential);
                    let _ = arrived.send(());
                    if let Some(release) = release {
                        release
                            .acquire()
                            .await
                            .expect("the gate outlives the request")
                            .forget();
                    }
                    (
                        StatusCode::OK,
                        axum::Json(json!({
                            "id": "chatcmpl-sentinel",
                            "choices": [],
                            "usage": { "prompt_tokens": 7, "completion_tokens": 3 }
                        })),
                    )
                }
            }
        };
        let app = Router::new()
            .route("/chat/completions", post(handler.clone()))
            .route("/responses", post(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port");
        let addr = listener.local_addr().expect("a bound address");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self {
            base_url: format!("http://{addr}"),
            presented,
            release,
            arrivals: tokio::sync::Mutex::new(arrivals),
        }
    }

    /// Wait until one more request has reached the provider.
    pub(crate) async fn await_arrival(&self) {
        let mut arrivals = self.arrivals.lock().await;
        tokio::time::timeout(std::time::Duration::from_secs(5), arrivals.recv())
            .await
            .expect("a request reaches the provider")
            .expect("the provider outlives the test");
    }

    /// Let `count` held requests answer.
    pub(crate) fn release(&self, count: usize) {
        self.release
            .as_ref()
            .expect("a gated provider")
            .add_permits(count);
    }

    /// Every `Authorization` header the provider has been presented with.
    pub(crate) fn presented(&self) -> Vec<String> {
        self.presented.lock().expect("not poisoned").clone()
    }
}

/// The tenant every fixture in this module belongs to, and the secret its
/// credential points at.
pub(crate) fn owner() -> SecretOwner {
    SecretOwner::tenant(fixtures::tenant_id(1))
}

pub(crate) fn first() -> SecretRef {
    fixtures::secret_ref(3)
}

/// The credential resource pinning `secret`, at resource version `version`.
///
/// Two distinct facts, deliberately kept distinct: the *resource* version is
/// what an administrator edited, and the *secret* version is which material it
/// points at. A rotation moves both, and a test that conflated them could not
/// tell a republished body from a new key.
///
/// Active, because these tests are about material that *serves*: staged material
/// resolves so a candidate can be compiled against it, and is deliberately not
/// projected onto a pool until it is activated.
pub(crate) fn credential(secret: SecretRef, version: ResourceVersionNumber) -> ResourceVersion {
    ProviderCredentialBody::staged(
        fixtures::resource_id(3),
        owner(),
        fixtures::provider_id(3),
        fixtures::display_name("Primary"),
        secret,
    )
    .transitioned(SecretLifecycle::Active)
    .expect("staged material may be activated")
    .version_at(Slug::parse("primary").expect("fixture slug"), version)
}

/// The provider connection the credential authenticates to, owned by the tenant
/// so every project of it reaches the same connection.
///
/// Its endpoint is not what the request is sent to: provider endpoints are still
/// bootstrap-owned, and the projection only needs the connection to exist to know
/// which `[[provider]]` a credential's pool belongs to.
pub(crate) fn provider_connection() -> ResourceVersion {
    ProviderBody::for_tenant(
        fixtures::provider_id(3),
        fixtures::tenant_id(1),
        fixtures::display_name("OpenAI"),
        WireFamily::OpenaiChat,
        "https://api.openai.com/v1",
    )
    .version(Slug::parse("openai").expect("fixture slug"))
}

/// A revision serving one alias through one credential.
///
/// The tenant has one project, because a project is what becomes a namespace: a
/// credential is projected onto the pools of the namespaces its owner serves, and
/// a tenant with no project serves none.
pub(crate) fn state_pinning(secret: SecretRef, version: ResourceVersionNumber) -> DesiredState {
    let credential = credential(secret, version);
    // The alias moves with the credential it depends on: a resource version is
    // immutable, so pointing at a new credential version is a new alias version,
    // not an edit of the old one.
    let alias = ResourceVersion::new(
        ResourceRef::new(ResourceKind::Alias, fixtures::resource_id(4), version),
        ResourceScope::Tenant(fixtures::tenant_id(1)),
        Slug::parse("fast").expect("fixture slug"),
        ResourceBody::Inline(CanonicalValue::map([(
            "wire_family",
            CanonicalValue::string("openai-chat"),
        )])),
    )
    .depending_on([credential.reference]);
    let mut state = DesiredState::new();
    state
        .insert(fixtures::tenant(1, "acme"))
        .and_then(|state| state.insert(fixtures::project(&fixtures::tenant_id(1), 2, "core")))
        .and_then(|state| state.insert(provider_connection()))
        .and_then(|state| state.insert(credential))
        .and_then(|state| state.insert(alias))
        .expect("a valid revision");
    state
}

/// A revision whose alias is served by two credentials pinning the *same*
/// secret version.
///
/// The shape that distinguishes counting credentials from counting store
/// reads: the materialization unwraps a version once however many bodies point
/// at it.
pub(crate) fn state_sharing(secret: SecretRef, version: ResourceVersionNumber) -> DesiredState {
    let primary = credential(secret, version);
    let secondary = ProviderCredentialBody::staged(
        fixtures::resource_id(5),
        owner(),
        fixtures::provider_id(3),
        fixtures::display_name("Secondary"),
        secret,
    )
    // Active like the primary: two bodies pointing at one version must agree
    // about its state, and it is serving material the count is about.
    .transitioned(SecretLifecycle::Active)
    .expect("staged material may be activated")
    .version_at(Slug::parse("secondary").expect("fixture slug"), version);
    let alias = ResourceVersion::new(
        ResourceRef::new(ResourceKind::Alias, fixtures::resource_id(4), version),
        ResourceScope::Tenant(fixtures::tenant_id(1)),
        Slug::parse("fast").expect("fixture slug"),
        ResourceBody::Inline(CanonicalValue::map([(
            "wire_family",
            CanonicalValue::string("openai-chat"),
        )])),
    )
    .depending_on([primary.reference, secondary.reference]);
    let mut state = DesiredState::new();
    state
        .insert(fixtures::tenant(1, "acme"))
        .and_then(|state| state.insert(fixtures::project(&fixtures::tenant_id(1), 2, "core")))
        .and_then(|state| state.insert(provider_connection()))
        .and_then(|state| state.insert(primary))
        .and_then(|state| state.insert(secondary))
        .and_then(|state| state.insert(alias))
        .expect("a valid revision");
    state
}

/// One replica: a control plane, a secret store, the compiler that joins them,
/// and the state the reconciler publishes into.
///
/// Generic in its secret store so the same replica can be driven against the
/// fake and against PostgreSQL: every property here is a property of the
/// composition, and a store-specific one would be asserted in the store's own
/// tests instead.
pub(crate) struct Replica<S = InMemorySecrets> {
    pub(crate) store: Arc<InMemoryControlPlane>,
    pub(crate) secrets: Arc<S>,
    pub(crate) compiler: Arc<SecretResolvingCompiler>,
    pub(crate) state: AppState,
    pub(crate) reconciler: Arc<Reconciler>,
}

impl Replica<InMemorySecrets> {
    pub(crate) fn new(provider: &FakeProvider) -> Self {
        Self::with_sinks(provider, Vec::new())
    }

    /// A replica whose usage fan-out is the caller's, so a test can read the
    /// records a served request produced.
    pub(crate) fn with_sinks(provider: &FakeProvider, sinks: Vec<Box<dyn UsageSink>>) -> Self {
        Self::backed_by(provider, Arc::new(InMemorySecrets::new()), sinks)
    }
}

impl<S: SecretStore + 'static> Replica<S> {
    /// A replica resolving its material out of `secrets`.
    pub(crate) fn backed_by(
        provider: &FakeProvider,
        secrets: Arc<S>,
        sinks: Vec<Box<dyn UsageSink>>,
    ) -> Self {
        let store = Arc::new(InMemoryControlPlane::new());
        let compiler = Arc::new(SecretResolvingCompiler::new(
            bootstrap(&provider.base_url),
            Arc::clone(&secrets) as Arc<dyn SecretStore>,
        ));
        let state = AppState::new(
            bootstrap(&provider.base_url),
            &bootstrap_env(),
            UsageFanout::new(sinks),
            Box::new(NoBudget),
        )
        .expect("the bootstrap config is servable");
        let reconciler = Arc::new(Reconciler::new(
            Arc::clone(&store) as Arc<dyn ControlPlaneStore>,
            Arc::clone(&compiler) as Arc<dyn CandidateCompiler>,
            Arc::new(state.clone()),
            settings(),
            None,
            Arc::new(ManualClock::new()),
        ));
        Self {
            store,
            secrets,
            compiler,
            state,
            reconciler,
        }
    }

    /// Publish `state` on top of whatever this replica's control plane holds.
    ///
    /// The expectation is read rather than passed because these tests are about
    /// what a *sequence* of publications does to secret material; concurrent
    /// writers racing on the expectation are [`InMemoryControlPlane`]'s own
    /// tests' subject, not this module's.
    pub(crate) async fn publish(&self, key: &str, state: DesiredState) -> RevisionId {
        let expected = self
            .store
            .desired_revision()
            .await
            .expect("the control plane is readable")
            .map_or(ExpectedRevision::Empty, ExpectedRevision::Exactly);
        self.store
            .publish_revision(fixtures::candidate(expected, key, state))
            .await
            .expect("the candidate is valid")
            .id
    }

    pub(crate) async fn converge(&self) -> Outcome {
        self.reconciler
            .converge_once(telemetry::CONVERGENCE_POLLED)
            .await
    }

    pub(crate) fn generation(&self) -> u64 {
        self.state.config().generation
    }
}

fn settings() -> ConvergenceSettings {
    ConvergenceSettings {
        poll_interval: Duration::from_millis(100),
        target: Duration::from_secs(1),
        backoff: BackoffPolicy {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(4),
            multiplier: 2,
        },
    }
}

/// A caller's request, authenticated with the inbound sentinel.
pub(crate) fn chat_request() -> Request<Body> {
    Request::post(format!("/ns/{SERVING_NAMESPACE}/v1/chat/completions"))
        .header("content-type", "application/json")
        .header(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {INBOUND_MATERIAL}"),
        )
        .body(Body::from(r#"{"model":"fast","messages":[]}"#))
        .expect("a valid request")
}

/// Stage each `(reference, plaintext)` pair into a store and resolve it back,
/// returning the plaintext the store handed over.
///
/// The store is dropped; the material is not. What the caller holds is the same
/// thing the runtime holds between a compilation and a publication — a resolved
/// key, alive in the process under test — which is the only state in which "the
/// surface never carried it" is a claim with content. A test whose scenario
/// never resolves anything (a refusal, a durable-state sweep) uses this to put
/// the material in the process anyway, so its sweep can fail.
pub(crate) async fn live_material(pairs: &[(SecretRef, &'static str)]) -> Vec<String> {
    let secrets = InMemorySecrets::new();
    let mut resolved = Vec::with_capacity(pairs.len());
    for (reference, plaintext) in pairs {
        secrets.seed(owner(), *reference, plaintext, SecretLifecycle::Active);
        resolved.push(
            secrets
                .resolve(owner(), reference)
                .await
                .expect("active material resolves")
                .expose()
                .to_owned(),
        );
    }
    resolved
}
