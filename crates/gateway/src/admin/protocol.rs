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
    ExpectedRevision, IdempotencyKey, MutationKind, ResourceScope, RevisionId,
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
        let idempotency_key = headers
            .get(IDEMPOTENCY_KEY_HEADER)
            .and_then(|value| value.to_str().ok())
            .ok_or(AdminError::IdempotencyKeyRequired)?;
        let idempotency_key =
            IdempotencyKey::parse(idempotency_key).map_err(AdminError::IdempotencyKeyInvalid)?;
        let expected = headers
            .get(EXPECTED_REVISION_HEADER)
            .and_then(|value| value.to_str().ok())
            .ok_or(AdminError::ExpectedRevisionRequired)?;
        let expected = parse_expected_revision(expected)?;
        let mode = match headers
            .get(DRY_RUN_HEADER)
            .and_then(|value| value.to_str().ok())
        {
            None | Some("false") => WriteMode::Apply,
            Some("true") => WriteMode::DryRun,
            Some(_) => return Err(AdminError::DryRunInvalid),
        };
        Ok(Self {
            expected,
            idempotency_key,
            mode,
        })
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
