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
//! # Contract only
//!
//! [`admin_route_specs`] is empty in this slice, and `serve` does not mount this
//! router. Resource-specific handlers — providers, credentials, catalogues,
//! prices, aliases, policy, tenants — are #143's, and each one is a spec plus an
//! edit over the complete state, with the protocol properties above already held.
//! What ships here is the boundary those handlers register into, its middleware,
//! and the tests that hold it, mounted on a synthetic spec so the layer's
//! behaviour is asserted rather than asserted-about.
//!
//! [`AdminIdentity`]: super::auth::AdminIdentity
//! [`MutationPreconditions`]: super::protocol::MutationPreconditions

use std::sync::Arc;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::HeaderMap;
use axum::middleware::{Next, from_fn_with_state};
use axum::response::Response;
use axum::routing::MethodRouter;

use super::auth::{
    AdminAction, AdminAuthenticator, AdminAuthorizer, AdminGrant, AdminIdentity, AdminPresented,
};
use super::error::AdminError;
use super::protocol::{ADMIN_PREFIX, MutationPreconditions};
use super::service::AdminService;
use crate::desired_state::ResourceScope;

/// Everything an administrative handler needs: the service, and the two
/// authorities that decide who may call it.
pub struct AdminApi {
    pub service: Arc<AdminService>,
    pub authenticator: Arc<dyn AdminAuthenticator>,
    pub authorizer: Arc<dyn AdminAuthorizer>,
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
        }
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
    pub fn authorize(
        &self,
        identity: &AdminIdentity,
        action: AdminAction,
        scope: &ResourceScope,
    ) -> Result<AdminGrant, AdminError> {
        Ok(self.authorizer.authorize(identity, action, scope)?)
    }
}

/// A route's complete administrative registration.
///
/// The action is declared here rather than derived from the path, because it is
/// what the authorizer decides on and what the middleware reads to know whether
/// the route mutates.
pub struct AdminRouteSpec {
    /// Path *within* [`ADMIN_PREFIX`], so no spec can register itself outside the
    /// administrative surface.
    pub path: &'static str,
    pub action: AdminAction,
    pub router: fn() -> MethodRouter<Arc<AdminApi>>,
}

/// The administrative route table.
///
/// Empty until #143 lands resource handlers. Deliberately a function returning a
/// table rather than an absent module: the boundary, its middleware, and its
/// tests exist now, so a handler is a row here plus an edit.
pub fn admin_route_specs() -> Vec<AdminRouteSpec> {
    Vec::new()
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
        .with_state(api);
    Router::new().nest(ADMIN_PREFIX, inner)
}

/// The administrative surface.
pub fn router(api: Arc<AdminApi>) -> Router {
    mount(api, admin_route_specs())
}

/// An unknown `/admin/v1` path answers in the administrative envelope, so a
/// client parses one error shape from this surface rather than axum's empty
/// body.
async fn unknown_route() -> AdminError {
    AdminError::RouteNotFound
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
    let identity = api.authenticate(&headers).await?;
    if action.mutates() {
        let preconditions = MutationPreconditions::from_headers(&headers)?;
        request.extensions_mut().insert(preconditions);
    }
    request.extensions_mut().insert(identity);
    request.extensions_mut().insert(action);
    Ok(next.run(request).await)
}
