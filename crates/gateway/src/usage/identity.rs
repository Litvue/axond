//! Usage event identity: one globally unique, time-ordered id per accepted
//! event.
//!
//! Before this, a request id was a per-process counter (`req_0000000000000001`),
//! so two replicas minted the same ids and a reader could only deduplicate on
//! `(request_id, recorded_at)` (ADR 0009). A billing consumer needs the stronger
//! thing: an identity it can put a unique constraint on, so a replayed delivery
//! collides with the row it already has instead of becoming a second billable
//! event.
//!
//! So a request id is now a [`Uuid7`] behind its `req_` prefix — the same
//! identity primitive the control-plane domain uses, for the same two reasons
//! (RFC 9562 §5.7): the leading 48 bits are a Unix millisecond timestamp, so ids
//! sort in mint order and an index on them stays append-mostly, and the
//! remaining bits are random, so a fleet of replicas cannot collide.
//!
//! The *shape* changes, the type does not: `request_id` stays `text` in the
//! shipped DDL and a string in the serialized record, so no schema version is
//! bumped and no reader has to be redeployed. What changes is what a reader may
//! now assume — see [`docs/usage-schema.md`](../../../../docs/usage-schema.md).

use std::fmt;
use std::sync::LazyLock;

use crate::desired_state::ids::{InvalidId, Uuid7, Uuid7Generator};

/// The identity of one accepted usage event.
///
/// Shaped like the control plane's typed ids (`ten_…`, `prj_…`) — prefix,
/// [`Uuid7`], one lowercase text form — and written out rather than generated,
/// because that module's `typed_id!` macro is private to it and usage identity is
/// a different domain that should not start depending on the control plane's.
///
/// One terminated request produces exactly one usage record, so the request id
/// *is* the event id: there is no second identifier to keep in step, and the
/// idempotency key a consumer constrains is this value's text form
/// ([`super::journal::IdempotencyKey`]).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestId(Uuid7);

impl RequestId {
    /// The text-form prefix, kept from the counter era so log lines, span
    /// fields, and stored rows read the same as before.
    pub const PREFIX: &'static str = "req_";

    pub const fn new(uuid: Uuid7) -> Self {
        Self(uuid)
    }

    /// The underlying UUID. `pub(crate)` and currently only read by tests: the
    /// text form is what every writer and reader uses, and the binary form waits
    /// for the outbox worker that stores it.
    #[cfg(test)]
    pub(crate) const fn uuid(&self) -> Uuid7 {
        self.0
    }

    /// Parse the prefixed text form. Used when an id makes a round trip through
    /// a stored row or a serialized record: a value that is not a UUIDv7 is not
    /// an event identity, and must not be treated as one.
    pub fn parse(text: &str) -> Result<Self, InvalidId> {
        let uuid = text
            .strip_prefix(Self::PREFIX)
            .ok_or_else(|| InvalidId::Prefix {
                expected: Self::PREFIX,
                found: text.to_owned(),
            })?;
        Ok(Self(Uuid7::parse(uuid)?))
    }
}

/// Renders the prefixed text form, so an id in a `Debug`-formatted structure is
/// the same string an operator can search a usage table for.
impl fmt::Debug for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::PREFIX, self.0)
    }
}

/// The process's id source. One generator, so the buffered path and the
/// streaming relay mint from the same strictly increasing sequence and neither
/// can hand out an id the other already used.
static REQUEST_IDS: LazyLock<Uuid7Generator> = LazyLock::new(Uuid7Generator::new);

/// The next event identity. Globally unique, and strictly increasing within this
/// process.
pub fn next_request_id() -> RequestId {
    RequestId::new(REQUEST_IDS.next())
}

/// The correlation a request's single usage event will carry, captured once when
/// the request is accepted.
///
/// Captured once, and early, for two different reasons:
///
/// - The id has to be *stable*. It used to be minted at settlement, in whichever
///   of the buffered, terminal, cancelled, or upstream-error paths got there
///   first, so nothing before settlement could refer to the event that a request
///   was going to produce. Minting it at acceptance means a credential rotation,
///   a retry across targets, and the record itself all name the same event.
/// - The trace id has to be *readable*. `telemetry::trace_id()` reads the current
///   span, and a streamed request settles in a detached task where the server
///   span is no longer current — so it is read in the handler, while the span is
///   live, and carried.
#[derive(Debug, Clone)]
pub struct EventIdentity {
    pub request_id: RequestId,
    /// Set when the request was traced. One trace covers a caller's whole agent
    /// loop, so it correlates but does not identify.
    pub trace_id: Option<String>,
}

impl EventIdentity {
    /// Mint an identity for a request being accepted. Call while the server span
    /// is current.
    pub fn capture() -> Self {
        Self {
            request_id: next_request_id(),
            trace_id: crate::telemetry::trace_id(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn a_minted_id_is_a_uuid7_behind_the_req_prefix() {
        let id = next_request_id();
        let text = id.to_string();
        assert!(text.starts_with("req_"), "{text}");
        // `req_` plus the hyphenated 8-4-4-4-12 form.
        assert_eq!(text.len(), 4 + 36, "{text}");
        assert_eq!(id.uuid().as_bytes()[6] >> 4, 7, "version bits");
        assert_eq!(id.uuid().as_bytes()[8] >> 6, 0b10, "variant bits");
        assert_eq!(RequestId::parse(&text).expect("round trip"), id);
    }

    #[test]
    fn ids_are_unique_and_sort_in_mint_order() {
        let minted: Vec<RequestId> = (0..10_000).map(|_| next_request_id()).collect();
        let distinct: BTreeSet<RequestId> = minted.iter().copied().collect();
        assert_eq!(distinct.len(), minted.len(), "ids must not repeat");
        assert!(
            minted.windows(2).all(|pair| pair[0] < pair[1]),
            "ids must sort in mint order"
        );
        // The text form is what a reader sees, and it has to sort the same way,
        // or "usage since id X" is a different question in SQL than in Rust.
        let text: Vec<String> = minted.iter().map(RequestId::to_string).collect();
        assert!(text.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn an_id_carries_the_millisecond_it_was_minted_in() {
        let before = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("after the epoch")
            .as_millis() as u64;
        // The timestamp is a property of the id, not a field a reader should
        // parse: `recorded_at` is the column that answers "when".
        assert!(next_request_id().uuid().timestamp_millis() >= before.saturating_sub(1));
    }

    #[test]
    fn a_counter_era_id_is_not_an_event_identity() {
        // The shape the gateway used to mint. Parsing has to refuse it rather
        // than accept it as a globally unique id, so a row written by an older
        // writer is recognisable instead of silently trusted.
        assert!(RequestId::parse("req_0000000000000001").is_err());
        assert!(RequestId::parse("0192f5e1-2b3c-7def-8123-456789abcdef").is_err());
        assert!(
            RequestId::parse("req_0192f5e1-2b3c-4def-8123-456789abcdef").is_err(),
            "a v4 uuid is not time-ordered and must not pass as one"
        );
    }
}
