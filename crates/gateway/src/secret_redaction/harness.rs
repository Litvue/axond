//! The fixtures the secret-redaction tests are driven from.
//!
//! Three things live here, and nothing else: the sentinel material, a compiler
//! that resolves desired-state credentials through a `SecretStore`, and a fake
//! provider that records what it was authenticated with.
//!
//! The compiler needs a word of explanation. Production convergence takes its
//! projection as a seam ([`crate::convergence::compile`]), and the projection
//! that reads credential bodies and resolves their references through a store is
//! part of the runtime slice tracked by #145; it is not on `main`. Rather than
//! assert nothing until it is, [`SecretResolvingCompiler`] wires the two landed
//! halves together exactly as that slice will have to: read the revision's
//! credentials with [`Credentials::of`], resolve each body's [`SecretRef`]
//! through the [`SecretStore`], hand the material to
//! [`ConfigSnapshot::build`] as the resolved environment, and publish the whole
//! snapshot atomically or none of it.
//!
//! That makes the lifecycle assertions in this module family real — they run
//! against the actual `Reconciler`, the actual `ArcSwap`, the actual request
//! path — while keeping the harness honest about what it is: when the
//! production projection lands, these tests should be repointed at it, and any
//! behaviour they assert that it does not have is a bug in it, not here.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::routing::post;
use futures::FutureExt as _;
use serde_json::json;
use tokio::sync::{Semaphore, mpsc};

use super::sweep::LeakSweep;
use crate::backends::control_plane::ControlPlaneStore;
use crate::backends::fakes::InMemorySecrets;
use crate::backends::secrets::{SecretMaterial, SecretResolver as _};
use crate::budget::NoBudget;
use crate::config::{Config, Model, Target};
use crate::convergence::compile::{CandidateCompiler, CompileError, ProjectionError};
use crate::convergence::status::testing::ManualClock;
use crate::convergence::{BackoffPolicy, ConvergenceSettings, Outcome, Reconciler};
use crate::desired_state::credentials::{Credentials, ProviderCredentialBody};
use crate::desired_state::oracle::InMemoryControlPlane;
use crate::desired_state::{
    CanonicalValue, DesiredState, ExpectedRevision, LoadedRevision, ResourceBody, ResourceId,
    ResourceKind, ResourceRef, ResourceScope, ResourceVersion, ResourceVersionNumber, RevisionId,
    SecretOwner, SecretRef, Slug, fixtures,
};
use crate::state::{AppState, ConfigSnapshot};
use crate::telemetry;
use crate::usage::{UsageFanout, UsageSink};

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

/// The env-var name a resolved credential is handed to `ConfigSnapshot::build`
/// under. A name, never a value — which is the reason it is safe to log.
///
/// Keyed by resource id rather than slug: slugs are unique within a scope, not
/// across them, and two credentials colliding on this name would silently
/// authenticate one tenant with another tenant's material.
fn env_name(id: ResourceId) -> String {
    format!(
        "AXOND_RESOLVED_{}",
        id.to_string().to_uppercase().replace(['-', '.'], "_")
    )
}

/// A compiler that resolves each revision's credential references through a
/// secret store, then builds a whole snapshot from the result.
pub(crate) struct SecretResolvingCompiler {
    bootstrap: Config,
    env: HashMap<String, String>,
    secrets: Arc<InMemorySecrets>,
    /// The bootstrap provider aliases are pointed at.
    provider: &'static str,
    resolutions: AtomicUsize,
}

impl SecretResolvingCompiler {
    pub(crate) fn new(bootstrap: Config, secrets: Arc<InMemorySecrets>) -> Self {
        Self {
            bootstrap,
            env: bootstrap_env(),
            secrets,
            provider: "openai",
            resolutions: AtomicUsize::new(0),
        }
    }

    /// How many times material has been taken out of the store.
    ///
    /// The number a test asserts on to show resolution happens per compilation —
    /// off the request path — rather than per request.
    pub(crate) fn resolutions(&self) -> usize {
        self.resolutions.load(Ordering::Relaxed)
    }
}

impl CandidateCompiler for SecretResolvingCompiler {
    fn compile(
        &self,
        revision: &LoadedRevision,
        generation: u64,
    ) -> Result<ConfigSnapshot, CompileError> {
        let id = revision.id();
        let projection = |source| CompileError::Projection {
            revision: id,
            source,
        };
        let mut config = self.bootstrap.clone();
        let mut env = self.env.clone();
        let credentials = Credentials::of(revision.state()).map_err(|error| {
            projection(ProjectionError::Body {
                reference: error.reference(),
                detail: error.to_string(),
            })
        })?;
        for credential in credentials.all() {
            if !credential.body.permits_resolution() {
                continue;
            }
            let reference = credential.body.secret();
            let material = self
                .secrets
                .resolve(credential.body.owner(), &reference)
                .now_or_never()
                .expect("the in-memory store resolves without yielding")
                .map_err(|error| {
                    // The reference is named; the material is what could not be
                    // obtained, so there is nothing of it to name.
                    projection(ProjectionError::Secret {
                        holder: credential.reference,
                        reference: reference.to_string(),
                        detail: error.to_string(),
                    })
                })?;
            self.resolutions.fetch_add(1, Ordering::Relaxed);
            let name = env_name(credential.reference.id);
            config.credential.push(crate::config::Credential {
                namespace: "platform".to_owned(),
                provider: self.provider.to_owned(),
                env: name.clone(),
                id: Some(credential.slug.as_str().to_owned()),
                weight: 1,
            });
            env.insert(name, material.expose().to_owned());
        }
        for resource in revision.state().resources() {
            if resource.reference.kind != ResourceKind::Alias {
                continue;
            }
            config.model.push(Model {
                name: resource.slug.as_str().to_owned(),
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
                }],
            });
        }
        config
            .validate_compiled()
            .map_err(|source| CompileError::Validation {
                revision: id,
                source,
            })?;
        ConfigSnapshot::build(config, &env, generation).map_err(|source| CompileError::Snapshot {
            revision: id,
            source,
        })
    }
}

/// Material as an administrator hands it to the store.
pub(crate) fn material(plaintext: &str) -> SecretMaterial {
    SecretMaterial::new(plaintext.to_owned())
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
pub(crate) fn credential(secret: SecretRef, version: ResourceVersionNumber) -> ResourceVersion {
    ProviderCredentialBody::staged(
        fixtures::resource_id(3),
        owner(),
        fixtures::provider_id(3),
        fixtures::display_name("Primary"),
        secret,
    )
    .version_at(Slug::parse("primary").expect("fixture slug"), version)
}

/// A revision serving one alias through one credential.
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
        .and_then(|state| state.insert(credential))
        .and_then(|state| state.insert(alias))
        .expect("a valid revision");
    state
}

/// One replica: a control plane, a secret store, the compiler that joins them,
/// and the state the reconciler publishes into.
pub(crate) struct Replica {
    pub(crate) store: Arc<InMemoryControlPlane>,
    pub(crate) secrets: Arc<InMemorySecrets>,
    pub(crate) compiler: Arc<SecretResolvingCompiler>,
    pub(crate) state: AppState,
    pub(crate) reconciler: Arc<Reconciler>,
}

impl Replica {
    pub(crate) fn new(provider: &FakeProvider) -> Self {
        Self::with_sinks(provider, Vec::new())
    }

    /// A replica whose usage fan-out is the caller's, so a test can read the
    /// records a served request produced.
    pub(crate) fn with_sinks(provider: &FakeProvider, sinks: Vec<Box<dyn UsageSink>>) -> Self {
        let store = Arc::new(InMemoryControlPlane::new());
        let secrets = Arc::new(InMemorySecrets::new());
        let compiler = Arc::new(SecretResolvingCompiler::new(
            bootstrap(&provider.base_url),
            Arc::clone(&secrets),
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
    Request::post("/v1/chat/completions")
        .header("content-type", "application/json")
        .header(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {INBOUND_MATERIAL}"),
        )
        .body(Body::from(r#"{"model":"fast","messages":[]}"#))
        .expect("a valid request")
}
