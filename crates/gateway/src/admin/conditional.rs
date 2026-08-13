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
//! # What an unreadable `If-None-Match` does
//!
//! Nothing. A conditional this surface cannot parse is treated as absent and
//! answered in full: refusing would let a mangled header deny an operator the
//! state read they are diagnosing an incident with, and matching would serve a
//! `304` against a validator nobody issued. `*` is honoured — it matches any
//! current representation, and a read that answers at all has one.

use axum::body::Body;
use axum::http::header::{CONTENT_TYPE, ETAG, IF_NONE_MATCH};
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
    if_none_match: Vec<HeaderValue>,
}

impl<T: Serialize> Conditional<T> {
    /// Prepare a projection to be answered against the request's conditionals.
    pub fn new(headers: &HeaderMap, projection: T) -> Self {
        Self {
            projection,
            if_none_match: headers.get_all(IF_NONE_MATCH).iter().cloned().collect(),
        }
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
        let Ok(etag) = HeaderValue::from_str(&format!("\"{}\"", Checksum::of(&body))) else {
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
        response.headers_mut().insert(ETAG, etag);
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
        let served = etag.to_str().unwrap_or_default();
        self.if_none_match
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .map(str::trim)
            .any(|candidate| {
                // A weak validator compares equal: this surface never issues
                // one, so `W/"…"` can only be an intermediary weakening a
                // validator that came from here.
                candidate == "*" || candidate.trim_start_matches("W/") == served
            })
    }
}
