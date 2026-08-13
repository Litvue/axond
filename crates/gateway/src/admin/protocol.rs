//! The wire protocol of an administrative mutation: the headers that make a
//! write safe to retry, and the header that makes it a rehearsal.
//!
//! Three preconditions, all mandatory for a mutation, all parsed here so no
//! handler can forget one:
//!
//! - `Idempotency-Key` — a caller-supplied token. A retry carrying the same key
//!   and the same desired state replays its own outcome; the same key carrying
//!   *different* state is refused rather than replayed, because replaying would
//!   report a change that never happened.
//! - `X-Axond-Expected-Revision` — either `empty` (there must be no revision
//!   yet) or the revision the caller read before building its candidate. Absent,
//!   a write is last-write-wins, which is how two administrators silently undo
//!   each other.
//! - `X-Axond-Dry-Run` — optional, `true` or `false`. A dry run validates the
//!   complete candidate and returns the diff it would publish, and does not
//!   publish anything.
//!
//! `Idempotency-Key` is the IETF-draft name and is deliberately not prefixed;
//! the two axond-specific headers are, because they are not standard and should
//! not look like they are.

use axum::http::HeaderMap;

use super::error::AdminError;
use crate::desired_state::{
    ExpectedRevision, IdempotencyKey, InvalidIdempotencyKey, MutationKind, ResourceScope,
    RevisionId,
};

/// The route prefix the administrative surface is mounted under. Disjoint from
/// `/v1` by construction, so no inference middleware and no inference credential
/// reaches it (ADR 0027).
pub const ADMIN_PREFIX: &str = "/admin/v1";

pub const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
pub const EXPECTED_REVISION_HEADER: &str = "x-axond-expected-revision";
pub const DRY_RUN_HEADER: &str = "x-axond-dry-run";

/// The `X-Axond-Expected-Revision` value that means "nothing is published yet".
///
/// A distinct token rather than an omitted header: "I expect an empty control
/// plane" and "I did not think about concurrency" are different claims, and only
/// the first is one a store can check.
pub const EXPECTED_REVISION_EMPTY: &str = "empty";

/// Whether a mutation publishes or only rehearses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    Apply,
    /// Validate the complete candidate and compute its diff, then stop. No
    /// revision, no audit event, no idempotency record, no history entry.
    DryRun,
}

impl WriteMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Apply => "apply",
            Self::DryRun => "dry-run",
        }
    }

    pub const fn is_dry_run(self) -> bool {
        matches!(self, Self::DryRun)
    }
}

/// A bounded, printable audit summary.
///
/// Free prose, but not unbounded prose: it is stored durably and appears in log
/// lines, so the same limits an [`IdempotencyKey`] has apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditSummary(String);

impl AuditSummary {
    pub const MAX_LEN: usize = 300;

    pub fn parse(input: &str) -> Result<Self, AdminError> {
        let input = input.trim();
        if input.is_empty() || input.len() > Self::MAX_LEN {
            return Err(AdminError::AuditSummaryInvalid);
        }
        if !input
            .bytes()
            .all(|byte| byte.is_ascii_graphic() || byte == b' ')
        {
            return Err(AdminError::AuditSummaryInvalid);
        }
        Ok(Self(input.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The preconditions a mutation carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationPreconditions {
    pub expected: ExpectedRevision,
    pub idempotency_key: IdempotencyKey,
    pub mode: WriteMode,
}

impl MutationPreconditions {
    /// Parse all three headers, refusing a mutation that omits either mandatory
    /// one.
    ///
    /// Order matters for the message a caller sees: the idempotency key is
    /// reported first because a client that omits both is usually not retry-safe
    /// at all, and telling it about concurrency first would send it to the wrong
    /// fix.
    pub fn from_headers(headers: &HeaderMap) -> Result<Self, AdminError> {
        let idempotency_key = match text(headers, IDEMPOTENCY_KEY_HEADER) {
            Header::Absent => return Err(AdminError::IdempotencyKeyRequired),
            // Unreadable is invalid, not absent: the caller sent a key, and it is
            // not one. `Unprintable` is the same refusal the domain gives a key
            // whose characters are visible but not printable, and for the same
            // reason — this token becomes a durable map key and a log field.
            Header::Unreadable => {
                return Err(AdminError::IdempotencyKeyInvalid(
                    InvalidIdempotencyKey::Unprintable,
                ));
            }
            Header::Text(value) => {
                IdempotencyKey::parse(value).map_err(AdminError::IdempotencyKeyInvalid)?
            }
        };
        let expected = match text(headers, EXPECTED_REVISION_HEADER) {
            Header::Absent => return Err(AdminError::ExpectedRevisionRequired),
            Header::Unreadable => return Err(AdminError::ExpectedRevisionInvalid),
            Header::Text(value) => parse_expected_revision(value)?,
        };
        // Unreadable is refused rather than read as `false`: a caller that asked
        // for a rehearsal must never be published for real because its header did
        // not survive the wire.
        let mode = match text(headers, DRY_RUN_HEADER) {
            Header::Absent | Header::Text("false") => WriteMode::Apply,
            Header::Text("true") => WriteMode::DryRun,
            Header::Unreadable | Header::Text(_) => return Err(AdminError::DryRunInvalid),
        };
        Ok(Self {
            expected,
            idempotency_key,
            mode,
        })
    }
}

/// What a header slot holds, keeping "absent" and "present but not text" apart.
///
/// The distinction is the difference between a client that forgot a precondition
/// and one whose precondition did not survive the wire, and — for the dry-run
/// header — between refusing a mutation and publishing it.
enum Header<'a> {
    Absent,
    Unreadable,
    Text(&'a str),
}

fn text<'a>(headers: &'a HeaderMap, name: &str) -> Header<'a> {
    match headers.get(name) {
        None => Header::Absent,
        Some(value) => value.to_str().map_or(Header::Unreadable, Header::Text),
    }
}

/// `empty`, or a revision id in its prefixed text form.
fn parse_expected_revision(value: &str) -> Result<ExpectedRevision, AdminError> {
    let value = value.trim();
    if value == EXPECTED_REVISION_EMPTY {
        return Ok(ExpectedRevision::Empty);
    }
    RevisionId::parse(value)
        .map(ExpectedRevision::Exactly)
        .map_err(|_| AdminError::ExpectedRevisionInvalid)
}

/// Everything a mutation states about itself, independent of which resources it
/// touches.
///
/// The resource-shaped part of a request — which provider, which alias, which
/// credential — is deliberately absent: those bodies land with their own slices,
/// and a handler expresses its change as a [`DesiredStateEdit`] over the complete
/// state it was handed.
///
/// [`DesiredStateEdit`]: super::service::DesiredStateEdit
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationRequest {
    pub preconditions: MutationPreconditions,
    pub kind: MutationKind,
    /// The scope the change is attributed to, which must be the scope the
    /// caller's grant covers.
    pub scope: ResourceScope,
    pub summary: AuditSummary,
}

impl MutationRequest {
    pub const fn mode(&self) -> WriteMode {
        self.preconditions.mode
    }
}
