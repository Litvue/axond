//! The `/admin/v1` router: a separate surface with its own state, its own
//! authentication layer, and its own error envelope.
//!
//! Separate from [`crate::routes`] in the way that matters: the inference router
//! is built over [`AppState`](crate::state::AppState) and layers
//! `authenticate_middleware`, which resolves an inference principal from the
//! configured gateway keys and minted-token verifiers. This router is built over
//! [`AdminApi`] and layers [`admin_authenticate`], which resolves an
//! [`AdminIdentity`] from an [`AdminAuthenticator`]. Neither layer can see the
//! other's credentials, and nothing merges the two route tables — an inference
//! key cannot administer because there is no code path that would let it, not
//! because a capability table says no.
//!
//! Two properties the table enforces rather than documents:
//!
//! - **Every administrative route is authenticated.** Registration goes through
//!   [`AdminRouteSpec`], which has no unauthenticated posture to declare. There is
//!   no `/admin/v1` analogue of `/healthz`: liveness is answered by the
//!   unauthenticated probes on the inference surface, which never consult a
//!   backend.
//! - **Every mutating route parses its preconditions before its handler runs.**
//!   A spec declaring [`AdminAction::mutates`] gets
//!   [`MutationPreconditions`] parsed in the layer and inserted as an extension,
//!   so a handler cannot publish without an idempotency key and an expected
//!   revision — it would have nothing to build a candidate from.
//!
//! # What the unauthenticated fallbacks disclose
//!
//! The 404/405 split answers before authentication, so an anonymous caller can
//! tell a registered administrative path from an unregistered one and thereby
//! enumerate the route table. That is deliberate and acceptable under
//! [ADR 0027][adr]: the administrative route table is published API surface,
//! identical in every deployment and documented, so hiding it protects nothing —
//! while the alternative, answering `404` to a wrong method, would make a
//! client's own protocol mistake indistinguishable from a typo, on the surface
//! an operator reaches during an incident.
//!
//! What the fallbacks must never disclose is anything a credential would have
//! decided: they never state whether a credential was presented, whether one
//! would have been accepted, or what the deployment contains. They read no
//! request state, consult neither authority, and touch no backend, so the answer
//! to a wrong method is the same for an anonymous caller and an administrator.
//! ADR 0027's disjointness rule is unaffected — enumerating paths grants no
//! authority over any of them.
//!
//! [adr]: https://github.com/Litvue/axond/blob/main/docs/adr/0027-stateless-and-stateful-operating-modes.md
//!
//! # The table
//!
//! Every row is a resource document plus an edit: the handlers in
//! [`super::handlers`] parse, plan, and delegate, and
//! [`AdminService`] owns validation, preconditions, diffing, and publication.
//! Adding a resource is a row here — never a second way to write state.
//!
//! [`AdminIdentity`]: super::auth::AdminIdentity
//! [`MutationPreconditions`]: super::protocol::MutationPreconditions

use std::error::Error as _;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::HeaderMap;
use axum::middleware::{Next, from_fn_with_state};
use axum::response::Response;
use axum::routing::MethodRouter;
use tracing::warn;

use super::auth::{
    AdminAction, AdminAuthenticator, AdminAuthorizer, AdminGrant, AdminIdentity, AdminPresented,
};
use super::error::AdminError;
use super::handlers;
use super::protocol::{ADMIN_PREFIX, MutationPreconditions};
use super::resources::{
    AliasRequest, CatalogRequest, CredentialRequest, ModelRequest, PolicyRequest, ProjectRequest,
    ProviderRequest, TenantRequest,
};
use super::service::AdminService;
use crate::convergence::{RevisionReport, RevisionStatus};
use crate::desired_state::{ResourceScope, Surface};

/// Everything an administrative handler needs: the service, and the two
/// authorities that decide who may call it.
pub struct AdminApi {
    pub service: Arc<AdminService>,
    pub authenticator: Arc<dyn AdminAuthenticator>,
    pub authorizer: Arc<dyn AdminAuthorizer>,
    /// This replica's own convergence state, or `None` before a reconciler is
    /// running. Read from the replica's cached status rather than from the
    /// control plane, so "what am I serving" is answerable during an outage of
    /// the store that would be needed to answer "what should I be serving".
    pub convergence: Option<Arc<RevisionStatus>>,
}

impl AdminApi {
    pub fn new(
        service: Arc<AdminService>,
        authenticator: Arc<dyn AdminAuthenticator>,
        authorizer: Arc<dyn AdminAuthorizer>,
    ) -> Self {
        Self {
            service,
            authenticator,
            authorizer,
            convergence: None,
        }
    }

    /// Attach the replica's convergence status.
    #[must_use]
    pub fn with_convergence(mut self, status: Arc<RevisionStatus>) -> Self {
        self.convergence = Some(status);
        self
    }

    /// What this replica has converged onto, or `None` when no reconciler is
    /// attached.
    ///
    /// Not an empty report: for a reconciler "nothing desired, nothing active"
    /// *is* convergence, and answering that to an operator gating a rollout
    /// would be a false all-clear from a replica serving nothing.
    pub fn convergence_report(&self) -> Option<RevisionReport> {
        self.convergence.as_ref().map(|status| status.report())
    }

    /// Establish an identity from what the request presented.
    pub async fn authenticate(&self, headers: &HeaderMap) -> Result<AdminIdentity, AdminError> {
        let presented = AdminPresented::from_headers(headers)?;
        Ok(self.authenticator.authenticate(&presented).await?)
    }

    /// Turn an established identity into authority for one action at one scope.
    ///
    /// The scope comes from the request, so authorization happens in the handler
    /// rather than in the layer: the layer knows the route's action but cannot
    /// know which tenant a body names.
    ///
    /// A refusal is written to the denial trail before it is returned, which is
    /// why this is the only way a handler reaches the authorizer: an
    /// authenticated caller reaching for authority it does not hold is exactly
    /// the event an investigator asks the control plane about, and a code path
    /// that could refuse without recording would be the one that hides it.
    pub async fn authorize(
        &self,
        identity: &AdminIdentity,
        action: AdminAction,
        surface: Surface,
        scope: &ResourceScope,
    ) -> Result<AdminGrant, AdminError> {
        match self.authorizer.authorize(identity, action, scope) {
            Ok(grant) => Ok(grant),
            Err(refusal) => {
                let error = AdminError::from(refusal);
                self.service
                    .record_denial(identity, action, surface, scope, &error)
                    .await;
                Err(error)
            }
        }
    }
}

/// A route's complete administrative registration.
///
/// The action is declared here rather than derived from the path, because it is
/// what the authorizer decides on and what the middleware reads to know whether
/// the route mutates.
///
/// One action, and therefore one method per spec: precondition parsing is keyed
/// to this action, so a spec whose router combined `get(read).post(publish)`
/// would either demand an idempotency key of its reader or let its writer
/// publish without one. A path that answers both registers two specs on the same
/// path — mounting merges them and each keeps its own layer, which is what makes
/// "every mutating route parses its preconditions" hold per method rather than
/// per path.
pub struct AdminRouteSpec {
    /// Path *within* [`ADMIN_PREFIX`], so no spec can register itself outside the
    /// administrative surface.
    pub path: &'static str,
    pub action: AdminAction,
    pub router: fn() -> MethodRouter<Arc<AdminApi>>,
}

/// The administrative route table.
///
/// Reads first, then the resource writes, then rollback. Every write is a
/// `POST` upsert of one complete resource document rather than a `PATCH`: a
/// partial write would have to merge against state the caller never saw, and
/// the whole point of the expected-revision precondition is that a caller
/// changes state it has read.
pub fn admin_route_specs() -> Vec<AdminRouteSpec> {
    vec![
        AdminRouteSpec {
            path: "/state",
            action: AdminAction::ReadState,
            router: handlers::state_route,
        },
        AdminRouteSpec {
            path: "/history",
            action: AdminAction::ReadHistory,
            router: handlers::history_route,
        },
        AdminRouteSpec {
            path: "/audit/{revision}",
            action: AdminAction::ReadAudit,
            router: handlers::audit_route,
        },
        AdminRouteSpec {
            path: "/convergence",
            action: AdminAction::ReadConvergence,
            router: handlers::convergence_route,
        },
        AdminRouteSpec {
            path: "/tenants",
            action: AdminAction::Publish,
            router: handlers::publish_route::<TenantRequest>,
        },
        AdminRouteSpec {
            path: "/projects",
            action: AdminAction::Publish,
            router: handlers::publish_route::<ProjectRequest>,
        },
        AdminRouteSpec {
            path: "/providers",
            action: AdminAction::Publish,
            router: handlers::publish_route::<ProviderRequest>,
        },
        AdminRouteSpec {
            path: "/credentials",
            action: AdminAction::Publish,
            router: handlers::publish_route::<CredentialRequest>,
        },
        AdminRouteSpec {
            path: "/catalogs",
            action: AdminAction::Publish,
            router: handlers::publish_route::<CatalogRequest>,
        },
        AdminRouteSpec {
            path: "/models",
            action: AdminAction::Publish,
            router: handlers::publish_route::<ModelRequest>,
        },
        AdminRouteSpec {
            path: "/aliases",
            action: AdminAction::Publish,
            router: handlers::publish_route::<AliasRequest>,
        },
        AdminRouteSpec {
            path: "/policies",
            action: AdminAction::Publish,
            router: handlers::publish_route::<PolicyRequest>,
        },
        AdminRouteSpec {
            path: "/rollback",
            action: AdminAction::Rollback,
            router: handlers::rollback_route,
        },
    ]
}

/// Mount a table under [`ADMIN_PREFIX`].
///
/// `pub(crate)` so the contract tests can mount a synthetic spec and assert the
/// layer's behaviour directly, rather than waiting for a real handler to exist.
pub(crate) fn mount(api: Arc<AdminApi>, specs: Vec<AdminRouteSpec>) -> Router {
    let inner = specs
        .into_iter()
        .fold(Router::new(), |router, spec| {
            let route = (spec.router)().layer(from_fn_with_state(
                (api.clone(), spec.action),
                admin_authenticate,
            ));
            router.route(spec.path, route)
        })
        .fallback(unknown_route)
        // Both fallbacks, because both are part of the declared vocabulary: a
        // client branching on `AdminError::CODES` must never meet axum's empty
        // body. The method fallback runs outside the authentication layer, which
        // is attached per route — a wrong method on an administrative path is a
        // protocol mistake, and answering it does not need an identity or reveal
        // whether one would have been accepted. See the module docs on what the
        // 404/405 split does and does not disclose. The custom handler replaces
        // the body, not the response's `Allow` header, which axum still sets from
        // the method router — asserted, because RFC 9110 requires it on a 405.
        .method_not_allowed_fallback(wrong_method)
        .with_state(api);
    Router::new().nest(ADMIN_PREFIX, inner)
}

/// The administrative surface.
pub fn router(api: Arc<AdminApi>) -> Router {
    mount(api, admin_route_specs())
}

/// The administrative surface a stateless deployment serves: every path, every
/// method, refused as [`AdminError::StatefulModeRequired`].
///
/// Mounted rather than omitted, and refused *before* authentication rather than
/// after, for two reasons. A stateless deployment has no administrative
/// credential to authenticate against — `[[admin_breakglass]]` is rejected
/// outside stateful mode — so `401` would be the answer to a question about the
/// deployment's mode, which is not a secret and is exactly what the operator
/// asked. And a `404` would be indistinguishable from an older build, leaving a
/// tool to guess whether the surface is absent or the mode is wrong.
///
/// Nothing behind this can reach a backend: there is no state, no service, and
/// no store — the refusal is the whole router.
pub fn refusing_router() -> Router {
    Router::new().nest(
        ADMIN_PREFIX,
        Router::new()
            .fallback(stateful_mode_required)
            .method_not_allowed_fallback(stateful_mode_required),
    )
}

async fn stateful_mode_required() -> AdminError {
    AdminError::StatefulModeRequired
}

/// An unknown `/admin/v1` path answers in the administrative envelope, so a
/// client parses one error shape from this surface rather than axum's empty
/// body.
async fn unknown_route() -> AdminError {
    AdminError::RouteNotFound
}

/// A known `/admin/v1` path reached with a method it does not serve.
async fn wrong_method() -> AdminError {
    AdminError::MethodNotAllowed
}

/// Authenticate once per administrative request, and parse the preconditions a
/// mutating route requires.
///
/// Authorization is *not* done here: it needs the scope the request names, which
/// only the handler can extract. What the layer guarantees is that a handler runs
/// with an established identity, and — on a mutating route — with preconditions
/// that parsed.
async fn admin_authenticate(
    State((api, action)): State<(Arc<AdminApi>, AdminAction)>,
    headers: HeaderMap,
    mut request: Request,
    next: Next,
) -> Result<Response, AdminError> {
    let identity = match api.authenticate(&headers).await {
        Ok(identity) => identity,
        Err(error) => {
            // The body says only that authentication failed, deliberately. The
            // operator still needs the distinction — most of all when breakglass
            // was refused for want of its two attribution headers, during the
            // incident that is the reason breakglass exists — and no
            // `AdminAuthError` has anywhere to put presented material, so the
            // cause is safe to log even though it is not safe to return.
            warn!(
                code = error.code(),
                cause = error.source().map(ToString::to_string).as_deref(),
                "administrative authentication failed"
            );
            return Err(error);
        }
    };
    if action.mutates() {
        let preconditions = MutationPreconditions::from_headers(&headers)?;
        request.extensions_mut().insert(preconditions);
    }
    request.extensions_mut().insert(identity);
    request.extensions_mut().insert(action);
    Ok(next.run(request).await)
}
