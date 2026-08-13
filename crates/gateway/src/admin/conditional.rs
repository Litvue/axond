//! Conditional reads: an `ETag` on every administrative projection, and the
//! `304` that lets a poller ask again for free.
//!
//! The administrative read surface is polled — by `axond admin`, by a
//! reconciliation loop watching for a revision it published to converge, and by
//! whatever an operator points at a dashboard. Each of those reads costs a
//! control-plane round trip and a complete desired state hydrated into memory,
//! and almost all of them answer exactly what the previous one did. A validator
//! turns the repeat into a header comparison.
//!
//! # What the validator is
//!
//! A strong `ETag` over the bytes this surface would send: the checksum of the
//! serialized projection. Not the revision id, which is the tempting shortcut and
//! the wrong one — [`convergence`](super::handlers) has no revision of its own to
//! name, a projection's *shape* changes when this build changes even though the
//! revision did not, and a scope-narrowed read of one revision is a different
//! answer to a different caller. Hashing what is about to be written cannot
//! disagree with what is about to be written.
//!
//! A validator is therefore not a name for a revision and must not be parsed as
//! one: it is opaque, and its only operations are equality and echoing it back.
//! It also discloses nothing a caller could not already read — it is a digest of
//! the response body that caller is authorized to receive.
//!
//! # The one projection that cannot be validated by its bytes
//!
//! `/convergence` reports how long this replica has been behind, so while it *is*
//! behind its bytes differ on every read and a digest of them could never match —
//! and a reconciler waiting for a publication to take effect is exactly the
//! caller this is for. That read is therefore validated over the state it
//! describes — everything but the growing `lag_ms`: revisions, generation,
//! source, last convergence duration, failures, the rejection reason — and
//! answers a **weak** validator, `W/"…"`, which is the honest label for "the same
//! state, not necessarily the same bytes": a `304` there may withhold a body
//! whose reported lag has moved on. `If-None-Match` is compared weakly
//! anyway ([RFC 9110 §13.1.2][inm]), so a caller needs no special handling.
//!
//! [inm]: https://www.rfc-editor.org/rfc/rfc9110#name-if-none-match
//!
//! # What an unreadable `If-None-Match` does
//!
//! Nothing. A conditional this surface cannot parse is treated as absent and
//! answered in full: refusing would let a mangled header deny an operator the
//! state read they are diagnosing an incident with, and matching would serve a
//! `304` against a validator nobody issued. `*` is honoured — it matches any
//! current representation, and a read that answers at all has one.
//!
//! # What a cache may do with one
//!
//! Nothing but revalidate. These responses are per-caller — a scope-narrowed
//! grant reads a narrower projection of the same revision — so every one carries
//! `Cache-Control: private, no-cache` and `Vary: Authorization` beside the
//! validator. [RFC 9111][store] already forbids a shared cache from reusing a
//! response to an `Authorization`-bearing request without explicit permission,
//! but an administrator's state projection is not a thing to leave to an
//! intermediary's conformance: the directives say it outright, and `no-cache`
//! still lets the caller keep the representation it holds and revalidate with the
//! validator, which is the whole point.
//!
//! [store]: https://www.rfc-editor.org/rfc/rfc9111#name-storing-responses-to-authen

use axum::body::Body;
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE, ETAG, IF_NONE_MATCH, VARY};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::desired_state::Checksum;

/// A projection, answered conditionally.
///
/// Holds the caller's validators rather than reading them from a task-local or a
/// request extension, so a handler that forgets them cannot silently answer
/// unconditionally: the type has no constructor that does not take them.
pub struct Conditional<T> {
    projection: T,
    /// What the validator is taken over, when that is not the response body: the
    /// serialized identity of the state the projection describes.
    identity: Option<Vec<u8>>,
    if_none_match: Vec<HeaderValue>,
}

impl<T: Serialize> Conditional<T> {
    /// Prepare a projection to be answered against the request's conditionals,
    /// validated strongly by its own bytes.
    pub fn new(headers: &HeaderMap, projection: T) -> Self {
        Self {
            projection,
            identity: None,
            if_none_match: Self::conditionals(headers),
        }
    }

    /// Prepare a projection whose bytes move on their own — see the module note
    /// on `/convergence` — validated weakly over the state `identity` names.
    ///
    /// An identity that cannot be serialized falls back to validating by the
    /// body, which is never *less* specific: the result is a validator that
    /// changes too often rather than one that matches when it should not.
    pub fn identified_by<K: Serialize>(headers: &HeaderMap, projection: T, identity: &K) -> Self {
        Self {
            projection,
            identity: serde_json::to_vec(identity).ok(),
            if_none_match: Self::conditionals(headers),
        }
    }

    fn conditionals(headers: &HeaderMap) -> Vec<HeaderValue> {
        headers.get_all(IF_NONE_MATCH).iter().cloned().collect()
    }
}

impl<T: Serialize> IntoResponse for Conditional<T> {
    fn into_response(self) -> Response {
        let Ok(body) = serde_json::to_vec(&self.projection) else {
            // Unreachable: a projection is a tree of strings, numbers, and maps
            // keyed by strings. Answered the way axum answers an unserializable
            // `Json`, rather than by inventing an administrative code for a
            // condition no request can cause.
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        // A validator over an identity is weak by construction: two bodies
        // agreeing on the state they describe may still differ byte for byte.
        let (prefix, over) = match &self.identity {
            None => ("", body.as_slice()),
            Some(identity) => ("W/", identity.as_slice()),
        };
        let Ok(etag) = HeaderValue::from_str(&format!("{prefix}\"{}\"", Checksum::of(over))) else {
            // Also unreachable: a checksum renders as algorithm-prefixed hex.
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };

        let mut response = if self.matched(&etag) {
            // No body, and the same validator: a poller keeps conditioning on
            // the validator it holds instead of falling back to full reads.
            StatusCode::NOT_MODIFIED.into_response()
        } else {
            let mut response = Body::from(body).into_response();
            response
                .headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            response
        };
        let headers = response.headers_mut();
        headers.insert(ETAG, etag);
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("private, no-cache"));
        headers.insert(VARY, HeaderValue::from_static("authorization"));
        response
    }
}

impl<T> Conditional<T> {
    /// Whether an `If-None-Match` names the representation being served.
    ///
    /// Every listed validator is compared, because a caller holding two
    /// representations of a route may condition on both, and an unreadable field
    /// is simply no match.
    fn matched(&self, etag: &HeaderValue) -> bool {
        // `If-None-Match` is compared weakly (RFC 9110 §13.1.2): the `W/` prefix
        // is stripped from both sides, so a strong validator matches the weak
        // form an intermediary may have handed back, and `/convergence`'s own
        // weak validator matches itself.
        let served = etag.to_str().unwrap_or_default().trim_start_matches("W/");
        self.if_none_match
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .map(str::trim)
            .any(|candidate| candidate == "*" || candidate.trim_start_matches("W/") == served)
    }
}
