//! The authorities a running gateway builds: who `/admin/v1` accepts, and what
//! they may do.
//!
//! Stateful administration always retains one breakglass credential for
//! identity-provider recovery. When `[admin_oidc]` is configured, the same
//! surface also verifies issuer-scoped human bearer tokens against the explicit
//! JWKS endpoint; both authenticators produce an [`AdminIdentity`]. The active
//! revision's immutable directory then supplies the human's scoped grant, so
//! authentication alone never creates authority.
//!
//! Without `[admin_oidc]`, a non-breakglass credential is refused as
//! [`AdminAuthError::UnknownCredential`]. Without an active directory entry, a
//! valid OIDC token is authenticated but refused authorization — never accepted
//! because no authority is configured.
//!
//! [`Config::validate`]: crate::config::Config::validate

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use axum::Router;
use secrecy::SecretString;

use super::auth::{
    AdminAction, AdminAuthError, AdminAuthenticator, AdminAuthorizer, AdminGrant, AdminIdentity,
    AdminPresented,
};
use super::oidc::{OidcBootError, OidcVerifier};
use super::router::{self, AdminApi};
use super::service::AdminService;
use crate::availability::AvailabilityReader;
use crate::backends::catalog_runtime::CatalogHandle;
use crate::backends::catalog_store::CatalogStore;
use crate::backends::control_plane::postgres::{ControlPlaneSettings, PostgresControlPlane};
use crate::backends::control_plane::{ControlPlaneError, ControlPlaneStore};
use crate::backends::secrets::{SecretError, SecretResolver};
use crate::config::{AdminBreakglass, Config, KeyMaterialSource};
use crate::desired_state::{Action, AuthorizationSnapshot, Caller, DenialReason, ResourceScope};
use crate::key_material::{self, KeyMaterialError};
use crate::status::probes::ControlPlaneProbe;
use crate::status::registry::StatusSettings;

/// Why an administrative surface could not be built.
///
/// Both arms fail the boot. A stateful replica whose administrative surface did
/// not come up would be a replica an operator cannot reach to fix the reason it
/// did not come up.
#[derive(Debug, thiserror::Error)]
pub enum BootError {
    #[error("the `[[admin_breakglass]]` credential could not be resolved: {0}")]
    Credential(#[from] KeyMaterialError),
    #[error("the control plane could not be opened for administration: {0}")]
    ControlPlane(#[from] ControlPlaneError),
    #[error(
        "`mode = \"stateful\"` requires `[control_plane]` and one `[[admin_breakglass]]`, which \
         configuration validation should already have required"
    )]
    Incomplete,
    // The reference is named, never the value: a DSN carries a password.
    #[error("the control-plane DSN reference `{name}` is unset or empty")]
    MissingDsn { name: String },
    /// The secret store could not be opened. A stateful deployment that came up
    /// without it would answer every credential-material call `503` and — worse
    /// — would compile candidate revisions that resolve nothing, so the boot
    /// fails here instead.
    ///
    /// The [`SecretError`] arms name references, owners, and backends, never
    /// material or a KEK, so this is safe to print at boot.
    #[error("the secret store could not be opened: {0}")]
    SecretStore(#[from] SecretError),
    #[error("the OIDC administrative verifier could not be built: {0}")]
    Oidc(#[from] OidcBootError),
    #[error(
        "`mode = \"stateful\"` requires `[secret_store]`, which configuration validation should \
         already have required"
    )]
    MissingSecretStore,
}

/// The active revision's administrative directory, swapped with the serving
/// snapshot after a candidate is admitted. It is intentionally empty until a
/// typed stateful revision is active, so an authenticated human cannot gain
/// authority from a bootstrap or partially compiled state.
#[derive(Clone)]
pub struct AuthorizationState(Arc<ArcSwapOption<AuthorizationSnapshot>>);

impl AuthorizationState {
    pub fn new() -> Self {
        Self(Arc::new(ArcSwapOption::empty()))
    }

    pub fn update(&self, authorization: Option<Arc<AuthorizationSnapshot>>) {
        self.0.store(authorization);
    }

    fn active(&self) -> Option<Arc<AuthorizationSnapshot>> {
        self.0.load_full()
    }
}

impl Default for AuthorizationState {
    fn default() -> Self {
        Self::new()
    }
}

/// Build the `/admin/v1` surface this process serves.
///
/// Stateless mode gets [`router::refusing_router`]: no store is opened, no
/// credential is resolved, and every administrative request is answered
/// `stateful_mode_required` without a backend being touched. Stateful mode gets
/// the real table over a Postgres control plane and the configured breakglass
/// authority.
///
/// The store is handed back beside the router because the replica diagnostic
/// observes the *same* connection administration uses. A second pool opened for
/// probing would report on a path no administrative request takes, which is the
/// shape of bug where status says `ok` throughout an outage of the thing being
/// asked about.
///
/// The router is merged into the inference router rather than served on a second
/// listener: the surfaces are separated by authentication and state, which is
/// what makes an inference key powerless here — a second port would only make
/// them separated by firewall rules.
pub async fn surface(config: &Config, env: &HashMap<String, String>) -> Result<Surface, BootError> {
    surface_with_change_signal(config, env, None).await
}

/// Build the administrative surface and optionally wake the serving
/// reconciler after a durable publication or secret lifecycle change.
pub async fn surface_with_change_signal(
    config: &Config,
    env: &HashMap<String, String>,
    change_signal: Option<Arc<crate::convergence::ChangeSignal>>,
) -> Result<Surface, BootError> {
    surface_with_change_signal_and_recovery(config, env, change_signal, false).await
}

/// Build the administrative surface with an already authenticated serving
/// cache as a cold-start recovery permit. Only an initial backend-unavailable
/// result is deferred; all other boot refusals retain the strict posture.
pub async fn surface_with_change_signal_and_recovery(
    config: &Config,
    env: &HashMap<String, String>,
    change_signal: Option<Arc<crate::convergence::ChangeSignal>>,
    allow_recovery: bool,
) -> Result<Surface, BootError> {
    if !config.is_stateful() {
        return Ok(Surface {
            api: None,
            mode: "stateless",
            control_plane: None,
            secret_resolver: None,
            authorization: AuthorizationState::new(),
        });
    }
    let (Some(control_plane), Some(breakglass)) = (
        config.control_plane.as_ref(),
        config.admin_breakglass.first(),
    ) else {
        return Err(BootError::Incomplete);
    };
    let breakglass = BreakglassAuthenticator::resolve(breakglass, env)?;
    let authorization = AuthorizationState::new();
    let authenticator: Arc<dyn AdminAuthenticator> = match config.admin_oidc.as_ref() {
        Some(oidc) => Arc::new(OidcAdminAuthenticator {
            breakglass,
            verifier: OidcVerifier::new(oidc)?,
        }),
        None => Arc::new(breakglass),
    };
    let dsn = env
        .get(control_plane.dsn_env.as_deref().unwrap_or_default())
        .map(String::as_str)
        .filter(|dsn| !dsn.trim().is_empty())
        .ok_or_else(|| BootError::MissingDsn {
            name: control_plane
                .dsn_env
                .as_deref()
                .unwrap_or("dsn_env")
                .to_owned(),
        })?;
    let settings = ControlPlaneSettings::from_config(control_plane);
    // The diagnostic has to be paced against the bounds the store will actually
    // take, so they are read here rather than re-derived from config elsewhere.
    let pacing = ControlPlaneProbe::pacing(&settings);
    let store: Arc<dyn ControlPlaneStore> = if allow_recovery {
        match PostgresControlPlane::connect(dsn, settings.clone()).await {
            Ok(store) => Arc::new(store),
            Err(ControlPlaneError::Unavailable { .. }) => {
                Arc::new(PostgresControlPlane::deferred(dsn, settings)?)
            }
            Err(error) => return Err(BootError::ControlPlane(error)),
        }
    } else {
        Arc::new(PostgresControlPlane::connect(dsn, settings).await?)
    };
    // Opened at boot, for the reason the control plane is: the deployment that
    // cannot reach its material cannot rotate a credential, and finding that out
    // during an incident is finding it out too late. Configuration validation
    // already requires `[secret_store]` in stateful mode; this is the refusal for
    // the case it did not run.
    let secret_store = config
        .secret_store
        .as_ref()
        .ok_or(BootError::MissingSecretStore)?;
    let secrets = if allow_recovery {
        crate::backends::secrets::build_allow_unavailable(secret_store, control_plane, env).await?
    } else {
        crate::backends::secrets::build(secret_store, control_plane, env).await?
    };
    let resolver: Arc<dyn SecretResolver> = secrets.clone();
    let service = AdminService::stateful(Arc::clone(&store))
        .with_secrets(secrets)
        .with_change_signal(change_signal);
    let api = AdminApi::new(
        Arc::new(service),
        authenticator,
        Arc::new(ActiveSnapshotAuthorizer::new(authorization.clone())),
    );
    Ok(Surface {
        api: Some(api),
        mode: "stateful",
        control_plane: Some(ObservedControlPlane { store, pacing }),
        secret_resolver: Some(resolver),
        authorization,
    })
}

/// What administration brought up, and what the diagnostic may observe.
pub struct Surface {
    /// The administrative API in stateful mode; `None` in stateless mode, where
    /// every request is refused without a backend being touched.
    api: Option<AdminApi>,
    /// The posture this process booted in, for the log line that records it.
    pub mode: &'static str,
    /// The store administration was built on, or `None` in stateless mode where
    /// no store is opened at all.
    pub control_plane: Option<ObservedControlPlane>,
    /// The read-only resolver used by candidate compilation. It is the same
    /// store administration owns, so outages and ownership checks agree.
    pub secret_resolver: Option<Arc<dyn SecretResolver>>,
    /// Handle updated by the convergence sink when a complete revision becomes
    /// active. The serving process and `/admin/v1` therefore authorize against
    /// one generation rather than independently observed state.
    pub authorization: AuthorizationState,
}

impl Surface {
    /// Attach the retained catalogue reader after the background import has
    /// opened its store. The administrative router is not wrapped in `Arc` until
    /// `router`, so this remains a one-time boot-time attachment.
    pub fn with_catalogue(mut self, catalogue: Arc<dyn CatalogStore>) -> Self {
        if let Some(api) = self.api.take() {
            self.api = Some(api.with_catalogue(catalogue));
        }
        self
    }

    /// Attach the running catalogue import: the store the management read uses,
    /// and the handle a manual refresh waits on.
    pub fn with_catalog_handle(mut self, handle: CatalogHandle) -> Self {
        if let Some(api) = self.api.take() {
            self.api = Some(api.with_catalog_handle(handle));
        }
        self
    }

    /// The router to merge into the served application, reading this replica's
    /// derived availability from `availability` where it has any.
    ///
    /// Built here rather than in [`surface`] because the reader is the inference
    /// state, and the inference state is built from the observability this
    /// surface's own store paces: deferring the router is what lets one boot
    /// order satisfy both, with a single connection pool behind administration,
    /// the diagnostic, and an availability read.
    pub fn router(self, availability: Option<Arc<dyn AvailabilityReader>>) -> Router {
        self.router_with_convergence(availability, None)
    }

    pub fn router_with_convergence(
        self,
        availability: Option<Arc<dyn AvailabilityReader>>,
        convergence: Option<Arc<crate::convergence::RevisionStatus>>,
    ) -> Router {
        let Some(api) = self.api else {
            return router::refusing_router();
        };
        let api = match availability {
            None => api,
            Some(reader) => api.with_availability(reader),
        };
        let api = match convergence {
            None => api,
            Some(status) => api.with_convergence(status),
        };
        router::router(Arc::new(api))
    }
}

/// A control plane the replica diagnostic may report on, with the pacing its
/// own timeouts allow. The two travel together because probing the store at a
/// cadence its bounds do not permit reports outages that are not happening.
pub struct ObservedControlPlane {
    pub store: Arc<dyn ControlPlaneStore>,
    pub pacing: StatusSettings,
}

/// The live administrative authenticator. Breakglass is checked first so an
/// identity-provider outage never removes the recovery credential; every other
/// bearer is sent through the configured OIDC/JWKS verifier.
pub struct OidcAdminAuthenticator {
    breakglass: BreakglassAuthenticator,
    verifier: OidcVerifier,
}

#[async_trait]
impl AdminAuthenticator for OidcAdminAuthenticator {
    fn name(&self) -> &'static str {
        "oidc+breakglass"
    }

    async fn authenticate(
        &self,
        presented: &AdminPresented,
    ) -> Result<AdminIdentity, AdminAuthError> {
        if presented.credential.matches(&self.breakglass.material) {
            return self.breakglass.authenticate(presented).await;
        }
        self.verifier
            .authenticate(presented.credential.expose())
            .await
    }
}

/// Authorizes OIDC humans against the directory of the active revision while
/// retaining the deployment-wide recovery authority of breakglass.
pub struct ActiveSnapshotAuthorizer {
    state: AuthorizationState,
}

impl ActiveSnapshotAuthorizer {
    pub fn new(state: AuthorizationState) -> Self {
        Self { state }
    }
}

impl AdminAuthorizer for ActiveSnapshotAuthorizer {
    fn name(&self) -> &'static str {
        "active-revision-directory"
    }

    fn authorize(
        &self,
        identity: &AdminIdentity,
        action: AdminAction,
        surface: crate::desired_state::Surface,
        scope: &ResourceScope,
    ) -> Result<AdminGrant, AdminAuthError> {
        if matches!(identity, AdminIdentity::Breakglass { .. }) {
            return Ok(AdminGrant::granted(identity.clone(), action, scope.clone()));
        }
        let AdminIdentity::Human { issuer, subject } = identity else {
            return Err(AdminAuthError::Directory {
                reason: DenialReason::UnknownPrincipal,
            });
        };
        let Some(snapshot) = self.state.active() else {
            return Err(AdminAuthError::Directory {
                reason: DenialReason::UnknownPrincipal,
            });
        };
        let caller = Caller::Human {
            issuer: issuer.clone(),
            subject: subject.clone(),
        };
        let operation = match action {
            AdminAction::ReadState
            | AdminAction::ReadHistory
            | AdminAction::ReadAudit
            | AdminAction::ReadConvergence
            | AdminAction::ReadAvailability
            | AdminAction::ReadSecrets => Action::Read,
            AdminAction::Publish | AdminAction::Rollback | AdminAction::RefreshCatalog => {
                Action::Update
            }
            AdminAction::WriteSecrets => Action::Rotate,
        };
        snapshot
            .authorize(&caller, surface, operation, scope.clone())
            .map(|_| AdminGrant::granted(identity.clone(), action, scope.clone()))
            .map_err(|denial| AdminAuthError::Directory {
                reason: denial.reason(),
            })
    }
}

/// Authenticates the one configured breakglass credential.
///
/// Material is resolved once at boot and held in a [`SecretString`]: a request
/// re-reading a file or an env var would make administration depend on the
/// filesystem at exactly the moment it is being used to recover from something.
pub struct BreakglassAuthenticator {
    /// The non-secret label the audit trail records — a name, never material.
    label: String,
    material: SecretString,
}

impl BreakglassAuthenticator {
    /// Resolve the configured credential.
    ///
    /// Fails the boot rather than starting an administrative surface nothing can
    /// authenticate against: a stateful replica whose breakglass credential is
    /// unresolvable has no way in when OIDC is down.
    pub fn resolve(
        breakglass: &AdminBreakglass,
        env: &HashMap<String, String>,
    ) -> Result<Self, KeyMaterialError> {
        let label = breakglass.label().to_owned();
        let source = match breakglass.source() {
            Some(("file", path)) => KeyMaterialSource::File(path),
            Some((_, name)) => KeyMaterialSource::Env(name),
            // Unreachable through `Config::validate`, which refuses a breakglass
            // declaration with no usable source; treated as the missing-source
            // failure it is rather than by panicking on a config path.
            None => {
                return Err(KeyMaterialError::MissingEnv {
                    name: label.clone(),
                });
            }
        };
        let material = key_material::resolve(source, env)?;
        Ok(Self {
            label,
            material: SecretString::from(material),
        })
    }

    /// The non-secret label, for boot diagnostics.
    pub fn label(&self) -> &str {
        &self.label
    }
}

#[async_trait]
impl AdminAuthenticator for BreakglassAuthenticator {
    fn name(&self) -> &'static str {
        "breakglass"
    }

    async fn authenticate(
        &self,
        presented: &AdminPresented,
    ) -> Result<AdminIdentity, AdminAuthError> {
        // Compared before attribution is examined, and in constant time: whether
        // the headers were well formed must not tell an unauthenticated caller
        // whether the credential was right.
        if !presented.credential.matches(&self.material) {
            return Err(AdminAuthError::UnknownCredential);
        }
        let attribution = presented.attribution.require()?;
        Ok(AdminIdentity::Breakglass {
            attribution,
            credential: self.label.clone(),
        })
    }
}

/// Grants an authenticated identity whatever it asked for.
///
/// Correct precisely because of what authenticates today: the breakglass
/// credential is the deployment's root authority, and a scope restriction on it
/// would be a restriction on the one credential that exists to recover a
/// deployment. It is bounded instead by *attribution* — every breakglass action
/// names an operator and a reason, and lands in the audit trail as
/// [`Actor::Breakglass`](crate::desired_state::Actor::Breakglass).
///
/// A human OIDC identity is refused: an issuer this build cannot verify must
/// not be granted authority by an authorizer that never saw a token. Scoped
/// grants arrive with the identities that can carry a scope, and
/// [`AdminService::within_scope`](super::service::AdminService) already refuses
/// a mutation that touches a resource outside its grant, so a narrower
/// authorizer restricts without any handler changing.
pub struct BreakglassAuthorizer;

impl AdminAuthorizer for BreakglassAuthorizer {
    fn name(&self) -> &'static str {
        "breakglass"
    }

    fn authorize(
        &self,
        identity: &AdminIdentity,
        action: AdminAction,
        _surface: crate::desired_state::Surface,
        scope: &ResourceScope,
    ) -> Result<AdminGrant, AdminAuthError> {
        match identity {
            AdminIdentity::Breakglass { .. } => {
                Ok(AdminGrant::granted(identity.clone(), action, scope.clone()))
            }
            AdminIdentity::Human { issuer, .. } => Err(AdminAuthError::UntrustedIssuer {
                issuer: issuer.clone(),
            }),
        }
    }
}
