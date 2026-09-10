//! Route authentication: inbound keys, namespace grant intersection, and the
//! middleware that runs before any handler extractor.
//!
//! Permit ownership: the diagnostic authenticating permit is released in
//! [`authenticate_middleware`] after identity is resolved and before the
//! handler runs. Admission permits live on the request until the inference
//! path takes them. This module does not clone the request body.

use std::sync::Arc;

use axum::extract::{OriginalUri, Request, State};
use axum::http::HeaderMap;
use axum::middleware::Next;
use axum::response::Response;
use tracing::{debug, warn};

use crate::admission::DiagnosticCredential;
use crate::error::GatewayError;
use crate::namespace::NamespaceId;
use crate::principals::{Capability, Presented, PrincipalStoreError, TokenVerificationError};
use crate::state::{AppState, ConfigSnapshot, InboundKey};

use super::{Route, RouteAuthority, convergence_refusal};

pub(super) async fn diagnostic_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, GatewayError> {
    let _permit = state.0.admission.admit_diagnostic()?;
    Ok(next.run(request).await)
}

/// Bound the work of *authenticating* a diagnostic read.
///
/// The ceiling above is inside authentication, so it bounds the answer rather
/// than the signature verification and revocation lookup that precede it. This
/// one is outside, and is wide enough that only a flood reaches it: the two
/// together mean neither an anonymous flood can close the route to operators
/// nor a credentialled one can spend the replica's CPU and revocation store
/// without limit.
///
/// Which partition of it a request may take is decided here, from the shape of
/// the credential alone — the only thing known before the credential is spent.
/// A token's verification can block on the revocation store, so tokens are held
/// to their own share and cannot fill the share that resolves in memory: the
/// operator's static key is the credential the runbook sends through a
/// revocation outage, and a store that is slow rather than down must not be
/// able to refuse it. Callers presenting nothing at all are held to a third
/// share for the same reason — a flood needs no credential to mount.
pub(super) async fn diagnostic_authentication_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, GatewayError> {
    let credential = presented_credential(request.headers()).map_or(
        DiagnosticCredential::Anonymous,
        |credential| {
            state
                .config()
                .diagnostic_credential(&Presented { credential })
        },
    );
    let permit = state
        .0
        .admission
        .admit_diagnostic_authentication(credential)?;
    let mut request = request;
    request
        .extensions_mut()
        .insert(AuthenticatingPermit(Arc::new(permit)));
    Ok(next.run(request).await)
}

/// The pre-authentication permit, carried on the request so that
/// [`authenticate_middleware`] can give it back the moment the credential is
/// settled.
///
/// Holding it to the end of the response would make the share drain at the speed
/// of *answering*, not of authenticating, which is the opposite of what it is
/// for: sixteen slow readers would then close the in-memory share against the
/// static key, and the inner ceiling already bounds the answering.
#[derive(Clone)]
pub(super) struct AuthenticatingPermit(Arc<crate::admission::DiagnosticPermit>);

impl AuthenticatingPermit {
    /// Give the permit back. Dropping the extension would do it too, but only
    /// once the request itself is dropped, which is the timing this exists to
    /// avoid.
    pub(super) fn release(self) {
        drop(self.0);
    }
}

/// Reserve a slot for a request and hold it until the response body is fully
/// delivered, so an open SSE stream counts as in-flight for as long as it runs.
pub(super) fn presented_credential(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .or_else(|| headers.get("x-api-key").and_then(|v| v.to_str().ok()))
}

/// Resolve the caller's namespace + subject from the inbound key. Every request
/// must present a configured gateway key: authentication fails closed, and a
/// snapshot with no key never reaches a request (ADR 0013).
pub(super) async fn authenticate(
    snapshot: &ConfigSnapshot,
    headers: &HeaderMap,
) -> Result<InboundKey, GatewayError> {
    let credential = presented_credential(headers).ok_or(GatewayError::Unauthorized)?;
    if credential.starts_with("axt1.") {
        return Err(GatewayError::Unauthorized);
    }
    let presented = Presented { credential };
    let store = snapshot.principal_store_name(&presented);
    let principal = match snapshot.resolve_principal(&presented).await {
        Ok(principal) => principal,
        Err(PrincipalStoreError::Unauthorized(error)) => {
            debug!(
                store,
                error = %error,
                "token rejected during principal resolution"
            );
            return Err(GatewayError::TokenUnauthorized(error));
        }
        Err(PrincipalStoreError::Forbidden(error)) => {
            debug!(
                store,
                error = %error,
                "token rejected during principal resolution"
            );
            return Err(GatewayError::TokenForbidden(error));
        }
        Err(error) => {
            // A layer error is terminal by design; it must not fall through to
            // another authority just because the owning layer is unavailable.
            warn!(
                store,
                error = %error,
                "principal store resolution failed"
            );
            return Err(GatewayError::Unauthorized);
        }
    };
    principal.ok_or(GatewayError::Unauthorized)
}

/// Authenticate once per request, before handler extractors, and carry the
/// resolved snapshot and caller into the handler. A reload landing mid-request
/// therefore cannot change what this request resolved. Invalid callers return
/// `401` first; a valid caller on a replica with no active projected revision
/// gets the typed `503` convergence refusal before the handler runs.
pub(super) async fn authenticate_middleware(
    State((state, capability, authority)): State<(AppState, Option<Capability>, RouteAuthority)>,
    headers: HeaderMap,
    mut request: Request,
    next: Next,
) -> Result<Response, GatewayError> {
    let snapshot = state.config();
    let mut caller = authenticate(&snapshot, &headers).await?;
    if let Some(jti) = &caller.jti {
        match state.0.revocation.is_revoked(jti).await {
            Ok(true) => {
                crate::telemetry::metrics::record_revocation_denial();
                return Err(GatewayError::TokenUnauthorized(
                    TokenVerificationError::Revoked,
                ));
            }
            Ok(false) => {}
            Err(crate::revocation::RevocationError::Unavailable { .. }) => {
                crate::telemetry::metrics::record_revocation_unavailable_denial();
                return Err(GatewayError::RevocationUnavailable);
            }
            Err(error) => {
                warn!(error = %error, "revocation store check failed");
                crate::telemetry::metrics::record_revocation_unavailable_denial();
                return Err(GatewayError::RevocationUnavailable);
            }
        }
    }
    // The path selects the namespace. Perform this intersection after inbound
    // authentication but before convergence disclosure or any handler
    // extractor: anonymous callers receive `401`, and an existing namespace
    // outside the grant is indistinguishable from an absent namespace.
    if authority == RouteAuthority::Namespaced {
        let path = request
            .extensions()
            .get::<OriginalUri>()
            .map_or_else(|| request.uri().path(), |original| original.path());
        let namespace = namespace_from_canonical_path(path)?;
        let grant = caller
            .namespace_grant()
            .map_err(|_| GatewayError::NamespaceNotAuthorized)?;
        let authorized = grant.permits(&namespace);
        let missing = if !authorized {
            GatewayError::NamespaceNotAuthorized
        } else {
            GatewayError::UnknownNamespace
        };
        let reuse_admit = match (state.store(), state.0.budget.store_ledger()) {
            (Some(namespaces), Some(ledger)) => Arc::ptr_eq(namespaces, ledger),
            _ => false,
        };
        let record = if reuse_admit {
            let resolved = match state.store() {
                Some(store) => store
                    .resolve_namespace(namespace.as_str())
                    .await
                    .map_err(GatewayError::from)?,
                None => None,
            }
            .ok_or(missing)?;
            request.extensions_mut().insert(resolved.admit);
            resolved.record
        } else {
            match state.store() {
                Some(store) => store
                    .get_namespace(namespace.as_str())
                    .await
                    .map_err(GatewayError::from)?,
                None => None,
            }
            .ok_or(missing)?
        };
        if !authorized {
            debug!(
                namespace = %namespace,
                subject = %caller.subject,
                signer_kid = ?caller.signer_kid,
                "namespace route denied"
            );
            return Err(GatewayError::NamespaceNotAuthorized);
        }

        // Downstream code reads one effective namespace from the caller
        // context. Replacing it here makes the path authoritative when a later
        // grant implementation permits a set or all namespaces. Attrs are
        // copied at admission so usage records carry the workspace metadata
        // Litvue stored (ADR 0063). When the budget ledger is this Store,
        // admit is loaded in the same round trip so inference does not hit
        // Postgres again before dispatch.
        caller.namespace = namespace.to_string();
        caller.attrs = Some(record.attrs.clone());
        request.extensions_mut().insert(namespace);
        request.extensions_mut().insert(record);
    }
    // Route capability is evaluated only after the canonical path has selected
    // the effective namespace. That ordering prevents an outside-grant path
    // from learning whether its requested wire is servable in the caller's
    // original namespace and prepares this boundary for set/all grants.
    if let Some(capability) = capability
        && let Some(scope) = caller.scope.as_ref()
        && (!scope.contains(&capability)
            || !namespace_allows(&snapshot, &caller.namespace, capability))
    {
        debug!(
            namespace = %caller.namespace,
            subject = %caller.subject,
            signer_kid = ?caller.signer_kid,
            %capability,
            "token scope denied route"
        );
        return Err(GatewayError::ScopeInsufficient(capability));
    }
    // Keep the serving boundary here as well as in the route layer. The route
    // table currently adds `convergence_middleware` to every authenticated
    // inference route, but putting the invariant after successful
    // authentication means a future authenticated route cannot accidentally
    // serve the keyless stateful bootstrap by omitting that layer. Diagnostic
    // status is intentionally exempt: it is the operator's view of why the
    // replica is not ready, not inference traffic.
    if !matches!(capability, Some(Capability::Status))
        && state
            .revision_report()
            .is_some_and(|report| report.active.is_none())
    {
        request.extensions_mut().remove::<AuthenticatingPermit>();
        return Ok(convergence_refusal());
    }
    // Authentication is over, whatever it cost, so the permit that bounded it
    // goes back before the handler runs rather than after.
    if let Some(permit) = request.extensions_mut().remove::<AuthenticatingPermit>() {
        permit.release();
    }
    request.extensions_mut().insert(snapshot);
    request.extensions_mut().insert(caller);
    Ok(next.run(request).await)
}

/// Parse the raw namespace segment from the original URI. A nested axum router
/// may rewrite the active URI; accepting a decoded equivalent such as `%61cme`
/// would give one namespace several URL spellings and make routing ambiguous.
pub(super) fn namespace_from_canonical_path(path: &str) -> Result<NamespaceId, GatewayError> {
    let rest = path
        .strip_prefix("/ns/")
        .or_else(|| path.strip_prefix("/namespaces/"))
        .ok_or(GatewayError::InvalidNamespace)?;
    let (namespace, suffix) = rest.split_once('/').ok_or(GatewayError::InvalidNamespace)?;
    if suffix.is_empty() {
        return Err(GatewayError::InvalidNamespace);
    }
    NamespaceId::parse(namespace).map_err(|_| GatewayError::InvalidNamespace)
}

pub(super) fn namespace_allows(
    snapshot: &ConfigSnapshot,
    namespace: &str,
    capability: Capability,
) -> bool {
    let route = match capability {
        Capability::Chat => Some(Route::ChatCompletions),
        Capability::Messages => Some(Route::NativeMessages),
        Capability::Embeddings => Some(Route::Embeddings),
        Capability::Responses => Some(Route::Responses),
        Capability::Models => None,
        Capability::Credentials | Capability::CredentialsAll => None,
        // Status reports on the replica's own dependencies, so it is not
        // gated on a namespace having a servable model.
        Capability::Status => None,
    };
    let Some(route) = route else {
        return true;
    };
    snapshot.config.provider.iter().any(|provider| {
        route.serves(provider.kind)
            && snapshot
                .credentials
                .is_present(&snapshot.config, namespace, &provider.id)
    })
}
