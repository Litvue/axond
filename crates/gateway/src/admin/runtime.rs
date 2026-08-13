//! The authorities a running gateway builds: who `/admin/v1` accepts, and what
//! they may do.
//!
//! This slice ships breakglass only. Human administration is OIDC (ADR 0027),
//! and an OIDC verifier is a network dependency with its own configuration,
//! discovery, and key rotation; what a stateful deployment cannot boot without
//! is the credential that works *when the identity provider does not*, which is
//! why [`Config::validate`] already requires exactly one `[[admin_breakglass]]`.
//! An OIDC authenticator lands as a second [`AdminAuthenticator`] beside this
//! one, and nothing downstream changes: both produce an [`AdminIdentity`].
//!
//! Until then a presented credential that is not the configured breakglass one
//! is [`AdminAuthError::UnknownCredential`] — never "accepted because no
//! authority is configured".
//!
//! [`Config::validate`]: crate::config::Config::validate

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use secrecy::SecretString;

use super::auth::{
    AdminAction, AdminAuthError, AdminAuthenticator, AdminAuthorizer, AdminGrant, AdminIdentity,
    AdminPresented,
};
use super::router::{self, AdminApi};
use super::service::AdminService;
use crate::backends::control_plane::ControlPlaneError;
use crate::backends::control_plane::postgres::{ControlPlaneSettings, PostgresControlPlane};
use crate::config::{AdminBreakglass, Config, KeyMaterialSource, Mode};
use crate::desired_state::ResourceScope;
use crate::key_material::{self, KeyMaterialError};

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
}

/// Build the `/admin/v1` surface this process serves.
///
/// Stateless mode gets [`router::refusing_router`]: no store is opened, no
/// credential is resolved, and every administrative request is answered
/// `stateful_mode_required` without a backend being touched. Stateful mode gets
/// the real table over a Postgres control plane and the configured breakglass
/// authority.
///
/// The router is merged into the inference router rather than served on a second
/// listener: the surfaces are separated by authentication and state, which is
/// what makes an inference key powerless here — a second port would only make
/// them separated by firewall rules.
pub async fn surface(
    config: &Config,
    env: &HashMap<String, String>,
) -> Result<(Router, &'static str), BootError> {
    if config.mode == Mode::Stateless {
        return Ok((router::refusing_router(), "stateless"));
    }
    let (Some(control_plane), Some(breakglass)) = (
        config.control_plane.as_ref(),
        config.admin_breakglass.first(),
    ) else {
        return Err(BootError::Incomplete);
    };
    let authenticator = BreakglassAuthenticator::resolve(breakglass, env)?;
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
    let store =
        PostgresControlPlane::connect(dsn, ControlPlaneSettings::from_config(control_plane))
            .await?;
    let api = AdminApi::new(
        Arc::new(AdminService::stateful(Arc::new(store))),
        Arc::new(authenticator),
        Arc::new(BreakglassAuthorizer),
    );
    Ok((router::router(Arc::new(api)), "stateful"))
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
        let attribution = presented
            .attribution
            .clone()
            .ok_or(AdminAuthError::Attribution(
                super::auth::InvalidAttribution::Missing,
            ))?;
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
