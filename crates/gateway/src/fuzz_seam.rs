//! A private seam that lets the out-of-tree fuzz project drive the untrusted
//! parsers this binary runs on its request and boot paths.
//!
//! Axond ships as a binary, so its modules have no library target a fuzz
//! harness could link against. This file is a second target over the very same
//! module sources (`[lib]` in `Cargo.toml`), compiled only when the
//! `fuzzing` feature is on: a default build, the published `.crate`, and every
//! consumer see an empty library, so nothing here widens the published API. The
//! feature is not part of the compatibility contract and must not be enabled by
//! anything but [`fuzz/`](https://github.com/Litvue/axond/tree/main/fuzz).
//!
//! Every entry point below returns a typed, owned outcome rather than an
//! internal type, for two reasons: the fuzz project asserts on the *shape* of a
//! rejection (a recoverable error, never a panic or an abort), and the internal
//! types stay free to change without a fuzz-side edit.
//!
//! No entry point performs I/O, reads the environment, or touches a real
//! secret: the verifier material below is committed synthetic test material.
#![cfg(fuzzing)]
// Only the handful of items the seams below reach are live in this target; the
// rest of the crate is compiled for its `crate::` paths. A re-export whose only
// consumer is `main.rs` is unused here for the same reason, since this target
// compiles the modules without the binary that drives them.
#![allow(dead_code, unused_imports)]

// Keep this list identical to `main.rs`. `tests/fuzz_seam.rs` fails if it drifts.
mod admin;
mod admission;
mod aliases;
mod api;
mod availability;
mod backends;
mod budget;
mod config;
mod convergence;
mod credentials;
mod desired_state;
mod error;
mod key_material;
#[allow(dead_code)]
mod middleware;
mod mint;
mod namespace;
mod ops;
mod policy;
mod pricing;
mod principals;
mod rate_limit;
mod redis_support;
mod reload;
mod revocation;
mod routes;
mod shutdown;
mod state;
mod status;
mod store;
mod streaming;
// The layer re-export this module makes for `main.rs` has no consumer here.
#[allow(unused_imports)]
mod telemetry;
mod usage;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, UNIX_EPOCH};

use crate::backends::catalog::{
    self, Admission, CatalogContent, CatalogDiff, CatalogSnapshot, CatalogSource, JsonPointer,
    LastKnownGoodCatalog, ModelField, Refusable, Refusal, RefusalReason, SourceValidators,
};
use crate::backends::models_dev::{
    self, ModelsDevAdapter, ModelsDevError, SEED_PAYLOAD, seed_snapshot,
};
use crate::budget::NoBudget;
use crate::config::{Config, Model};
use crate::desired_state::{
    CanonicalValue, DeploymentBody, InboundGrantBody, NamespaceBody, ResourceBody, ResourceId,
    ResourceKind, ResourceRef, ResourceScope, ResourceVersion, ResourceVersionNumber, Slug, Uuid7,
};
use crate::mint::{MintAlgorithm, MintRequest};
use crate::principals::{
    Presented, PrincipalStore, PrincipalStoreError, TokenVerificationError, TokenVerifier,
};
use crate::rate_limit::NoLimit;
use crate::revocation::NoDenylist;
use crate::state::{AppState, ConfigSnapshot, ReplicaObservability, SnapshotError};
use crate::usage::{UsageDelivery, UsageFanout};

/// How a parser refused an input: the variant carries the class of failure, the
/// string carries the operator-facing message the process would have logged.
///
/// Presence of a variant is the fuzz assertion — a refusal is a value, so it
/// cannot have unwound, aborted, or exited.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rejection {
    /// The input was not loadable at all (TOML syntax, wrong types, unknown keys).
    Load(String),
    /// The input parsed but failed a whole-graph invariant.
    Invalid(String),
    /// The input was refused as a bad request, the way a caller would see it.
    BadRequest(String),
    /// Authentication failed. The payload is the stable error code.
    Unauthenticated(&'static str),
    /// Authentication succeeded and authorization failed. Stable error code.
    Unauthorized(&'static str),
    /// A catalogue payload was refused. `code` is the stable class of the
    /// refusal, `pointer` the JSON Pointer into the payload it names when the
    /// refusal is about one location rather than the document as a whole.
    Catalog {
        code: &'static str,
        message: String,
        pointer: Option<String>,
    },
    /// A store the check needs was unavailable. Unreachable through this seam,
    /// which is stateless, and mapped rather than asserted away.
    Unavailable,
}

/// A typed refusal from one durable publication-document parser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredDocumentRejection {
    pub code: &'static str,
    pub message: String,
}

/// Decode one untrusted namespace-native sealed-secret object through the
/// production parser. An accepted object is returned in its one canonical
/// spelling; a refusal exposes only a stable class and never rejected bytes.
pub fn blob_secret_envelope_cbor(input: &[u8]) -> Result<Vec<u8>, &'static str> {
    use backends::secrets::blob_envelope::{CodecError, SealedBlobSecret};

    SealedBlobSecret::from_canonical_cbor(input)
        .map(|sealed| sealed.to_canonical_cbor())
        .map_err(|error| match error {
            CodecError::Oversized => "oversized",
            CodecError::Truncated => "truncated",
            CodecError::Shape => "shape",
            CodecError::Compatibility => "compatibility",
            CodecError::NonCanonical => "noncanonical",
            CodecError::KekId => "kek_id",
            CodecError::FixedField => "fixed_field",
            CodecError::Ciphertext => "ciphertext",
            CodecError::Trailing => "trailing",
        })
}

/// Parser ceiling exported as a value, not as an internal type dependency.
pub const BLOB_SECRET_MAX_SEALED_BYTES: usize = backends::secrets::blob_envelope::MAX_SEALED_BYTES;

/// Plaintext byte ceiling used by structured boundary scenarios.
pub const BLOB_SECRET_MAX_PLAINTEXT_BYTES: usize =
    backends::secrets::blob_envelope::MAX_PLAINTEXT_BYTES;

/// Drive bounded synthetic seal/open scenarios through the private v2 codec.
/// No production key or authenticated manifest enters this seam.
pub fn blob_secret_seal_open(
    material: &[u8],
    scenario: u8,
    primary_seed: u8,
    secondary_seed: u8,
    identity_seed: u64,
    version_seed: u16,
) -> &'static str {
    backends::secrets::blob_envelope::fuzz_seal_open(
        material,
        scenario,
        primary_seed,
        secondary_seed,
        identity_seed,
        version_seed,
    )
}

/// The bounded reason behind a seam rejection, so a fuzzed import can be admitted
/// over a last-known-good catalogue the same way the refresh admits one.
///
/// The seam's [`Rejection::Catalog`] already carries the stable class the parser
/// chose, and those classes are [`RefusalReason::as_str`] by construction: the
/// mapping is a lookup rather than a second table, so a reason renamed on one
/// side degrades to [`RefusalReason::Unknown`] here instead of drifting quietly.
impl Refusable for Rejection {
    fn refusal(&self) -> Refusal {
        let Self::Catalog { code, pointer, .. } = self else {
            return Refusal::new(RefusalReason::Unknown);
        };
        let reason = RefusalReason::ALL
            .iter()
            .copied()
            .find(|reason| reason.as_str() == *code)
            .unwrap_or(RefusalReason::Unknown);
        pointer.as_ref().map_or_else(
            || Refusal::new(reason),
            |pointer| Refusal::at(reason, JsonPointer::new(pointer.clone())),
        )
    }
}

/// What a config the fuzzer produced turned into, without exposing [`Config`].
///
/// The mode is part of the shape because it selects which invariants the
/// validator applied: stateless mode owns its resources in TOML, stateful mode
/// forbids every one of those sections (ADR 0027).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigShape {
    pub stateful: bool,
    pub namespaces: usize,
    pub providers: usize,
    pub models: usize,
    pub credentials: usize,
    pub gateway_keys: usize,
    pub verifiers: usize,
}

/// Decode an untrusted object-store environment head through the production
/// bounded JSON parser.
pub fn publication_head_document(input: &[u8]) -> Result<(), StoredDocumentRejection> {
    publication_head_document_with_state(input, "production-us-east", 0, [0; 32])
}

/// Exercise the public tuple guard with independently selected expectations.
pub fn publication_head_document_with_state(
    input: &[u8],
    expected_environment: &str,
    accepted_sequence: u64,
    accepted_revision: [u8; 32],
) -> Result<(), StoredDocumentRejection> {
    let accepted = (accepted_sequence > 0).then_some((accepted_sequence, accepted_revision));
    desired_state::publication::fuzz_decode_head(input, expected_environment, accepted)
        .map_err(|(code, message)| StoredDocumentRejection { code, message })
}

/// Decode an untrusted immutable revision manifest through the production
/// deterministic CBOR parser.
pub fn publication_revision_manifest(input: &[u8]) -> Result<(), StoredDocumentRejection> {
    publication_revision_manifest_with_expectations(
        input,
        "production-us-east",
        *desired_state::Checksum::of(input).as_bytes(),
        1,
        None,
    )
}

/// Verify a history manifest against independently selected link expectations.
pub fn publication_revision_manifest_with_expectations(
    input: &[u8],
    expected_environment: &str,
    expected_digest: [u8; 32],
    expected_sequence: u64,
    expected_parent: Option<[u8; 32]>,
) -> Result<(), StoredDocumentRejection> {
    desired_state::publication::fuzz_decode_revision_manifest(
        input,
        expected_environment,
        expected_digest,
        expected_sequence,
        expected_parent,
    )
    .map_err(|(code, message)| StoredDocumentRejection { code, message })
}

/// Exercise the production active-revision and final-fence helpers without
/// exposing constructors for either security wrapper.
#[allow(clippy::too_many_arguments)]
pub fn publication_active_revision(
    head: &[u8],
    manifest: &[u8],
    current_head: &[u8],
    expected_environment: &str,
    expected_digest: [u8; 32],
    expected_sequence: u64,
    expected_parent: Option<[u8; 32]>,
    accepted_sequence: u64,
    accepted_revision: [u8; 32],
    observed_version: &str,
    current_version: &str,
) -> Result<(), StoredDocumentRejection> {
    let accepted = (accepted_sequence > 0).then_some((accepted_sequence, accepted_revision));
    desired_state::publication::fuzz_verify_active_revision(
        head,
        manifest,
        current_head,
        expected_environment,
        expected_digest,
        expected_sequence,
        expected_parent,
        accepted,
        observed_version,
        current_version,
    )
    .map_err(|(code, message)| StoredDocumentRejection { code, message })
}

pub const PUBLICATION_HEAD_MAX_BYTES: usize = desired_state::publication::MAX_HEAD_DOCUMENT_BYTES;
pub const PUBLICATION_MANIFEST_MAX_BYTES: usize =
    desired_state::publication::MAX_REVISION_MANIFEST_BYTES;

/// Parse and validate untrusted configuration text, the way `axond` does at
/// boot and on reload.
///
/// # Errors
///
/// [`Rejection::Load`] for text the loader refuses, [`Rejection::Invalid`] for
/// a config that loads but fails validation.
pub fn config_from_toml_str(input: &str) -> Result<ConfigShape, Rejection> {
    match Config::from_toml_str(input) {
        Ok(config) => Ok(ConfigShape {
            stateful: config.mode == config::Mode::Stateful,
            namespaces: config.namespace.len(),
            providers: config.provider.len(),
            models: config.model.len(),
            credentials: config.credential.len(),
            gateway_keys: config.gateway_key.len(),
            verifiers: config.gateway_verifier.len(),
        }),
        Err(config::ConfigError::Load(message)) => Err(Rejection::Load(message)),
        Err(config::ConfigError::Invalid(message)) => Err(Rejection::Invalid(message)),
    }
}

/// Parse one durable ADR 0062 body from untrusted JSON.
///
/// The first byte selects deployment (`D`), namespace (`N`), or inbound grant
/// (`G`); the remaining bytes are the canonical body represented as JSON. The
/// outcome classes are intentionally bounded for seed-replay coverage.
pub fn flat_v2_body(input: &[u8]) -> &'static str {
    let Some((&selector, body)) = input.split_first() else {
        return "empty";
    };
    let Ok(json) = serde_json::from_slice::<serde_json::Value>(body) else {
        return "invalid_json";
    };
    let namespace_deployment = (selector == b'N')
        .then(|| {
            let id = ResourceId::parse(json.get("deployment_id")?.as_str()?).ok()?;
            let version = ResourceVersionNumber::new(json.get("deployment_version")?.as_u64()?)?;
            Some(ResourceRef::new(ResourceKind::Deployment, id, version))
        })
        .flatten();
    let Ok(body) = CanonicalValue::try_from_json(&json) else {
        return "noncanonical_json";
    };
    let resource_id =
        ResourceId::new(Uuid7::from_parts(1, 0, 1).expect("fixed fuzz resource id is valid"));
    let kind = match selector {
        b'D' => ResourceKind::Deployment,
        b'N' => ResourceKind::Namespace,
        b'G' => ResourceKind::InboundGrant,
        _ => return "unknown_selector",
    };
    let mut resource = ResourceVersion::new(
        ResourceRef::new(kind, resource_id, ResourceVersionNumber::FIRST),
        ResourceScope::Deployment,
        Slug::parse("fuzz-body").expect("fixed fuzz slug is valid"),
        ResourceBody::Inline(body),
    );
    if kind == ResourceKind::Namespace {
        let Some(deployment) = namespace_deployment else {
            return "invalid";
        };
        resource = resource.depending_on([deployment]);
    }
    let outcome = match kind {
        ResourceKind::Deployment => DeploymentBody::read(&resource).map(|_| ()),
        ResourceKind::Namespace => NamespaceBody::read(&resource).map(|_| ()),
        ResourceKind::InboundGrant => InboundGrantBody::read(&resource).map(|_| ()),
        _ => unreachable!(),
    };
    match outcome {
        Ok(()) => "accepted",
        Err(error) if error.is_incompatible() => "incompatible",
        Err(_) => "invalid",
    }
}

/// Parse the `namespaces` filter out of an untrusted `GET
/// /v1/credentials/status` query string, percent-decoding included.
///
/// `None` means the caller sent no `namespaces` parameter; `Some` carries the
/// decoded value, which may be empty.
///
/// # Errors
///
/// [`Rejection::BadRequest`] for a duplicate parameter or an undecodable value.
pub fn credentials_query_namespaces(raw_query: Option<&str>) -> Result<Option<String>, Rejection> {
    routes::fuzz_parse_credential_query(raw_query)
        .map_err(|error| Rejection::BadRequest(error.to_string()))
}

/// What a token that verified carried, without exposing `InboundKey`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedToken {
    pub namespace: String,
    pub subject: String,
    pub capabilities: usize,
    pub scoped_aliases: bool,
    pub max_request_microdollars: Option<u64>,
}

/// Verify an untrusted `axt1.` credential against the seam's synthetic
/// verifiers: one HS256 signer, one EdDSA signer, two namespaces, and an
/// issuance epoch, so signature, algorithm, audience, lifetime, namespace,
/// scope, and epoch checks are all reachable.
///
/// # Errors
///
/// [`Rejection::Unauthenticated`] or [`Rejection::Unauthorized`], carrying the
/// same stable code the gateway would answer with.
pub fn verify_token(credential: &str) -> Result<Option<VerifiedToken>, Rejection> {
    let presented = Presented { credential };
    let resolved = futures::executor::block_on(verifier().resolve(&presented));
    match resolved {
        Ok(None) => Ok(None),
        Ok(Some(key)) => Ok(Some(VerifiedToken {
            namespace: key.namespace,
            subject: key.subject,
            capabilities: key.scope.map_or(0, |scope| scope.len()),
            scoped_aliases: key.alias_scope.is_some(),
            max_request_microdollars: key.max_request_microdollars,
        })),
        Err(PrincipalStoreError::Unauthorized(error)) => {
            Err(Rejection::Unauthenticated(code(&error)))
        }
        Err(PrincipalStoreError::Forbidden(error)) => Err(Rejection::Unauthorized(code(&error))),
        Err(PrincipalStoreError::Unavailable) => Err(Rejection::Unavailable),
    }
}

/// Mint an `axt1.` credential with the seam's synthetic HS256 signer so a
/// fuzzer can reach the claim checks that sit past signature verification.
///
/// `scope` is written into the claim **verbatim**: a name the capability
/// vocabulary does not define has to reach the verifier, because discarding it
/// here would leave the verifier's own handling of an unknown capability
/// unfuzzed.
///
/// Returns `None` when the requested claims cannot be encoded at all, which is
/// a rejection of the fuzzer's request rather than a finding.
pub fn mint_hs256_token(
    namespace: &str,
    subject: &str,
    audience: &str,
    ttl_seconds: u64,
    issued_at: Option<u64>,
    scope: Option<Vec<String>>,
    aliases: Option<Vec<String>>,
) -> Option<String> {
    mint::fuzz_mint_token_with_raw_scope(
        MintRequest {
            kid: HS256_KID,
            algorithm: MintAlgorithm::Hs256,
            key_material: HS256_MATERIAL,
            namespace,
            subject,
            audience,
            ttl: Duration::from_secs(ttl_seconds),
            aliases,
            max_request_microdollars: None,
            // Ignored by the raw-scope mint, which takes the claim below.
            scope: None,
        },
        issued_at,
        scope,
    )
    .ok()
    .map(|minted| minted.token)
}

/// Re-sign a committed token seed with its timestamps *translated* onto the
/// current run, so the claim check the seed is named for is the one it reaches.
///
/// A committed `axt1.` token expires the moment the date passes its `exp`, after
/// which every seed collapses onto the expiry check and the checks behind it —
/// scope, aliases, subject, `jti`, namespace, issuance epoch — go unexercised.
/// Translating rather than replacing the timestamps is what preserves each
/// seed's intent: the offset that moves `iat` onto now is applied to `exp` too,
/// so `exp - iat` is unchanged and a seed built to sit past the lifetime ceiling
/// still does, while one built with `exp` before `iat` still is.
///
/// The header is carried over verbatim and the payload is *not* verified first —
/// that is the point, since the interesting seeds are the ones a verifier would
/// refuse. Returns `None` when the seed is not a signable `axt1.` JWS with a
/// numeric `iat`, which is most of the corpus and not a finding.
pub fn resign_seed_onto_this_run(token: &str) -> Option<String> {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

    let mut segments = token.strip_prefix("axt1.")?.split('.');
    let header: jsonwebtoken::Header =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(segments.next()?).ok()?).ok()?;
    let mut claims: serde_json::Map<String, serde_json::Value> =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(segments.next()?).ok()?).ok()?;
    let iat = claims.get("iat")?.as_u64()?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs();
    // Signed, and in a wider type: an `exp` deliberately placed *before* its
    // `iat` has to stay before it, and a saturating unsigned subtraction would
    // quietly move it onto `iat` instead.
    let offset = i128::from(now) - i128::from(iat);
    let shift = |value: &mut serde_json::Value| {
        if let Some(seconds) = value.as_u64() {
            let shifted = (i128::from(seconds) + offset).clamp(0, i128::from(u64::MAX));
            *value = serde_json::Value::from(u64::try_from(shifted).unwrap_or(0));
        }
    };
    for claim in ["iat", "exp", "nbf"] {
        if let Some(value) = claims.get_mut(claim) {
            shift(value);
        }
    }
    let kid = header.kid.clone().unwrap_or_else(|| HS256_KID.to_owned());
    mint::fuzz_sign_claims(
        &header,
        &serde_json::Value::Object(claims),
        MintAlgorithm::Hs256,
        HS256_MATERIAL,
        &kid,
    )
    .ok()
}

/// The audience the seam's verifiers accept, so a fuzzer can aim at the
/// audience check from either side.
pub const AUDIENCE: &str = "fuzz.axond.invalid";

/// The namespaces the seam's config defines. `denied` is deliberately outside
/// the HS256 signer's permitted set.
pub const NAMESPACES: [&str; 2] = ["fuzz", "denied"];

/// How many capabilities the scope vocabulary defines, which bounds what any
/// token can present however its `scope` claim is shaped.
pub const CAPABILITY_COUNT: usize = principals::Capability::ALL.len();

/// The longest lifetime the seam's verifiers accept, so a fuzzer can mint a
/// token that straddles the issuance epoch without tripping the lifetime check
/// on the way there.
pub const MAX_TTL_SECONDS: u64 = 900;

/// The issuance epoch the seam declares for [`NAMESPACES`]`[0]`, as unix
/// seconds: a token this namespace's signer produced *before* this instant is
/// refused with `token_issued_before_epoch`.
///
/// It is anchored to the run rather than committed, because the check sits
/// behind the lifetime check — a fixed past epoch is unreachable, since any
/// token old enough to precede it is either expired or over its TTL. Resolved
/// once per process, so a replay stays internally consistent.
pub fn epoch_min_iat() -> u64 {
    static MIN_IAT: OnceLock<u64> = OnceLock::new();
    *MIN_IAT.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_secs())
            .saturating_sub(EPOCH_LOOKBACK_SECONDS)
    })
}

/// How far behind the start of a run the issuance epoch sits. Long enough that
/// a token minted before it still has a live `exp` inside [`MAX_TTL_SECONDS`],
/// short enough that an ordinary minted token lands after it.
const EPOCH_LOOKBACK_SECONDS: u64 = 300;

/// The `kid` of the seam's HS256 signer.
pub const HS256_KID: &str = "fuzz-hs256";

/// The `kid` of the seam's EdDSA verifier, whose private half does not exist.
pub const EDDSA_KID: &str = "fuzz-eddsa";

// ── Catalogue imports ────────────────────────────────────────────────────────

/// What an accepted catalogue import turned into, without exposing
/// `CatalogSnapshot`.
///
/// Everything here is derived from the *normalized* content, so an assertion
/// about it is an assertion about what the gateway would store rather than about
/// the payload's spelling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogImport {
    /// The content identity: the SHA-256 of the canonical normalized content,
    /// as text. Independent of key order, whitespace, fetch time, and
    /// validators.
    pub content_id: String,
    pub source_url: String,
    pub schema_version: &'static str,
    pub providers: usize,
    pub models: usize,
    pub offerings: usize,
    pub priced_offerings: usize,
    pub overrides: usize,
    /// Model ids in stored order, so a caller can assert the normalization
    /// sorted them.
    pub model_ids: Vec<String>,
    /// Every offering, `model|provider|published_model_id`, in stored order.
    pub offering_keys: Vec<String>,
    /// Every override, `model|provider|field|pointer`, in stored order.
    pub override_pointers: Vec<String>,
    /// Whether every recorded override is a field on which the provider really
    /// does contradict the neutral record — recomputed from the stored facts
    /// rather than trusted.
    pub overrides_are_contradictions: bool,
    /// Whether every override pointer points inside its own offering.
    pub overrides_point_into_offerings: bool,
    /// The digest and size of the payload as retrieved, which provenance keeps
    /// and identity does not.
    pub raw_digest: String,
    pub raw_bytes: u64,
}

/// How many changes of each class an import's diff carried.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CatalogDiffShape {
    pub changes: usize,
    pub providers_added: usize,
    pub providers_removed: usize,
    pub providers_changed: usize,
    pub models_added: usize,
    pub models_removed: usize,
    pub offerings_added: usize,
    pub offerings_removed: usize,
    pub neutral_changed: usize,
    pub lifecycle_changed: usize,
    pub capabilities_changed: usize,
    pub metadata_changed: usize,
    pub prices_changed: usize,
}

impl CatalogDiffShape {
    fn of(diff: &CatalogDiff) -> Self {
        let counts = diff.counts();
        Self {
            changes: diff.changes().len(),
            providers_added: counts.providers_added,
            providers_removed: counts.providers_removed,
            providers_changed: counts.providers_changed,
            models_added: counts.models_added,
            models_removed: counts.models_removed,
            offerings_added: counts.offerings_added,
            offerings_removed: counts.offerings_removed,
            neutral_changed: counts.neutral_changed,
            lifecycle_changed: counts.lifecycle_changed,
            capabilities_changed: counts.capabilities_changed,
            metadata_changed: counts.metadata_changed,
            prices_changed: counts.prices_changed,
        }
    }

    /// Changes that are about a price, and nothing else.
    pub const fn is_price_only(self) -> bool {
        self.prices_changed > 0 && self.prices_changed == self.changes
    }

    /// Changes that describe metadata — including the lifecycle and capability
    /// classes it splits into — and no price.
    pub const fn is_metadata_only(self) -> bool {
        self.prices_changed == 0
            && self.changes > 0
            && self.changes
                == self.metadata_changed
                    + self.capabilities_changed
                    + self.lifecycle_changed
                    + self.neutral_changed
    }
}

/// What one import did to the last-known-good catalogue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogAdmission {
    /// `initial`, `unchanged`, `updated`, or `refused`.
    pub outcome: &'static str,
    /// The typed refusal, when the payload was refused.
    pub refusal: Option<Rejection>,
    /// The URLs the import path asked its fetcher for. The fetcher is in
    /// memory, so this is also the proof that a fuzzed import is offline: a real
    /// transfer would need a `CatalogFetch` that opens a socket, and this seam
    /// never builds one.
    pub fetched: Vec<String>,
    /// The content identity that is active *after* the import.
    pub active_content_id: String,
    pub active_models: usize,
    /// Whether the active content is still the seed the import started from.
    pub active_is_seed: bool,
    pub diff: Option<CatalogDiffShape>,
    pub import: Option<CatalogImport>,
}

/// The bundled models.dev seed payload: a valid catalogue to mutate from.
pub const CATALOG_SEED_PAYLOAD: &str = SEED_PAYLOAD;

/// The `/catalog.json` URL the seam's adapter is configured with. Nothing
/// resolves or connects to it; it is the string the in-memory fetcher is asked
/// for.
pub fn catalog_source_url() -> String {
    ModelsDevAdapter::default().source_url().to_owned()
}

/// The content identity of the bundled seed, which every import in a fuzz run
/// starts from.
pub fn catalog_seed_content_id() -> String {
    seed_snapshot().content.content_id().to_string()
}

/// Parse an untrusted models.dev catalogue payload, exactly as the background
/// refresh does, and describe what it normalized to.
///
/// `fetched_at_secs` and `etag` are the provenance the caller controls: they are
/// carried into the snapshot and must not reach the content identity.
///
/// # Errors
///
/// [`Rejection::Catalog`], carrying the stable class of the refusal and the JSON
/// Pointer it names, for every malformed or schema-drifted payload.
pub fn catalog_parse(
    payload: &[u8],
    fetched_at_secs: u64,
    etag: Option<&str>,
) -> Result<CatalogImport, Rejection> {
    parse_catalog(payload, fetched_at_secs, etag).map(|snapshot| describe_catalog(&snapshot))
}

/// Import an untrusted payload over a last-known-good catalogue that already
/// holds the bundled seed, through the real source: a conditional fetch — served
/// from memory — then the strict parse, then admission.
///
/// This is the whole property the catalogue path exists to hold: whatever the
/// payload is, the active catalogue afterwards is either that payload's content
/// or exactly what was active before.
pub fn catalog_import_over_seed(payload: &[u8], etag: Option<&str>) -> CatalogAdmission {
    let seed = seed_snapshot();
    let seed_content_id = seed.content.content_id();
    let mut active = LastKnownGoodCatalog::default();
    active.admit(seed);

    let requested = Arc::new(Mutex::new(Vec::new()));
    let fetch = RecordingFetch {
        payload: payload.to_vec(),
        etag: etag.map(ToOwned::to_owned),
        requested: Arc::clone(&requested),
    };
    let source = models_dev::ModelsDevSource::new(ModelsDevAdapter::default(), fetch);
    let refreshed = futures::executor::block_on(source.refresh(None));
    let fetched = requested
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();

    // The typed refusal lives on the parser; the source maps it onto the
    // backend-failure taxonomy. Both are run so a payload can never be refused
    // by one and accepted by the other.
    let parsed = parse_catalog(payload, CATALOG_FETCHED_AT_SECS, etag);
    assert_eq!(
        parsed.is_ok(),
        matches!(refreshed, Ok(catalog::CatalogRefresh::Updated { .. })),
        "the source and the parser disagree about whether a payload is usable"
    );

    let import = parsed.as_ref().ok().map(describe_catalog);
    let (outcome, refusal, diff) = match active.admit_result(parsed) {
        Ok(Admission::Initial { .. }) => ("initial", None, None),
        Ok(Admission::Unchanged { .. }) => ("unchanged", None, None),
        Ok(Admission::Updated { diff, .. }) => ("updated", None, Some(CatalogDiffShape::of(&diff))),
        Err((rejection, _)) => ("refused", Some(rejection), None),
    };
    let active = active.active().expect("the seed stays active");
    CatalogAdmission {
        outcome,
        refusal,
        fetched,
        active_content_id: active.content.content_id().to_string(),
        active_models: active.content.models().len(),
        active_is_seed: active.content.content_id() == seed_content_id,
        diff,
        import,
    }
}

/// The semantic difference between two payloads, as the background refresh would
/// classify it: `previous` is admitted first, then `current`.
///
/// `None` for the diff means the second payload normalized to the same content
/// as the first — the classification a reordered or cosmetically different
/// payload must produce.
///
/// # Errors
///
/// [`Rejection::Catalog`] when either payload is refused.
pub fn catalog_diff(
    previous: &[u8],
    current: &[u8],
) -> Result<Option<CatalogDiffShape>, Rejection> {
    let mut active = LastKnownGoodCatalog::default();
    active.admit(parse_catalog(previous, CATALOG_FETCHED_AT_SECS, None)?);
    match active.admit(parse_catalog(current, CATALOG_FETCHED_AT_SECS + 60, None)?) {
        Admission::Unchanged { .. } => Ok(None),
        Admission::Updated { diff, .. } => Ok(Some(CatalogDiffShape::of(&diff))),
        Admission::Initial { .. } => unreachable!("the previous payload was admitted first"),
    }
}

/// The routing table the request path would serve, as `alias => provider/model`
/// entries, read from the seam's [`AppState`] through [`AppState::config`] —
/// the same load a request performs, off the same `ArcSwap` that
/// [`AppState::publish`] stores into.
///
/// Read fresh on every call rather than cached, so publication is what the
/// comparison watches: anything that reached runtime state by the one route
/// runtime state is reached by would move it. That the observation is live, and
/// not a constant compared with itself, is [`publication_moves_runtime_routes`]:
/// it publishes into a state built the same way and shows the routes move.
pub fn runtime_routes() -> Vec<String> {
    routes_of(&runtime_state().config())
}

/// Whether publishing a snapshot moves what [`runtime_routes`] reports.
///
/// The calibration of the no-publication assertion: it runs on a *separate*
/// state built exactly like the seam's, publishes a snapshot carrying one more
/// alias, and answers whether the routing table read afterwards differs. `false`
/// means the assertion has gone blind and every no-publication claim made with
/// it is worthless, which is why the smoke asserts it directly.
pub fn publication_moves_runtime_routes() -> bool {
    let state = build_state(seam_config().clone()).expect("the seam's own config resolves");
    let before = routes_of(&state.config());
    let mut config = seam_config().clone();
    let Some(extra) = config.model.first().cloned() else {
        return false;
    };
    config.model.push(Model {
        name: format!("{}-published", extra.name),
        ..extra
    });
    let snapshot = ConfigSnapshot::build(config, &seam_env(), 1)
        .expect("the seam's config resolves with one more alias");
    state.publish(snapshot).expect("publish");
    routes_of(&state.config()) != before
}

/// Whether the override oracle notices an override the import failed to record.
///
/// The calibration of `overrides_are_contradictions`, and in the direction that
/// is easy to lose: the check once filtered the differences it recomputed down
/// to the ones already recorded, which made a dropped override invisible. This
/// takes the seed, removes one recorded override from an offering that has one,
/// and answers whether describing the result reports the omission. `false`
/// means every override claim the run makes is worthless.
pub fn override_oracle_notices_a_missing_override() -> bool {
    let seed = seed_snapshot();
    let mut models = seed.content.models().to_vec();
    let mut removed = false;
    for model in &mut models {
        if model.neutral.is_none() {
            continue;
        }
        for offering in &mut model.offerings {
            if !offering.overrides.is_empty() {
                offering.overrides.remove(0);
                removed = true;
                break;
            }
        }
        if removed {
            break;
        }
    }
    if !removed {
        return false;
    }
    let Ok(content) = CatalogContent::new(seed.content.providers().to_vec(), models) else {
        return false;
    };
    !describe_catalog(&CatalogSnapshot {
        source: seed.source,
        content,
    })
    .overrides_are_contradictions
}

fn routes_of(snapshot: &ConfigSnapshot) -> Vec<String> {
    snapshot
        .config
        .model
        .iter()
        .flat_map(|model| {
            model.targets.iter().map(move |target| {
                format!("{} => {}/{}", model.name, target.provider, target.model)
            })
        })
        .collect()
}

/// The default fetch time an import is stamped with, as unix seconds. Fixed, so
/// a replay is deterministic; provenance, so it must not reach a content id.
pub const CATALOG_FETCHED_AT_SECS: u64 = 1_767_225_600;

fn parse_catalog(
    payload: &[u8],
    fetched_at_secs: u64,
    etag: Option<&str>,
) -> Result<CatalogSnapshot, Rejection> {
    let validators = etag.map_or_else(SourceValidators::default, SourceValidators::etag);
    let fetched_at = UNIX_EPOCH + Duration::from_secs(fetched_at_secs);
    ModelsDevAdapter::default()
        .parse(payload, validators, fetched_at)
        .map_err(|error| catalog_rejection(&error))
}

/// Map a parser error onto a stable class and the location it names.
fn catalog_rejection(error: &ModelsDevError) -> Rejection {
    use ModelsDevError as E;
    let (code, pointer) = match error {
        E::UnsupportedEndpoint { .. } => ("unsupported_endpoint", None),
        E::NotJson { .. } => ("not_json", None),
        E::Schema { pointer, .. } => ("schema", pointer.as_ref()),
        E::IdMismatch { pointer, .. } => ("id_mismatch", Some(pointer)),
        E::Identifier { pointer, .. } => ("identifier", Some(pointer)),
        E::UnknownStatus { pointer, .. } => ("unknown_status", Some(pointer)),
        E::UnknownModality { pointer, .. } => ("unknown_modality", Some(pointer)),
        E::Price { pointer, .. } => ("price", Some(pointer)),
        E::UnknownTierType { pointer, .. } => ("unknown_tier_type", Some(pointer)),
        E::DuplicateTier { pointer } => ("duplicate_tier", Some(pointer)),
        E::NeutralPrice { pointer } => ("neutral_price", Some(pointer)),
        E::UncanonicalizableText { pointer, .. } => ("uncanonicalizable_text", Some(pointer)),
        E::AmbiguousModelKey { pointer, .. } => ("ambiguous_model_key", Some(pointer)),
        E::Content { .. } => ("content", None),
    };
    Rejection::Catalog {
        code,
        message: error.to_string(),
        pointer: pointer.map(|pointer| pointer.as_str().to_owned()),
    }
}

fn describe_catalog(snapshot: &CatalogSnapshot) -> CatalogImport {
    let content = &snapshot.content;
    let mut model_ids = Vec::with_capacity(content.models().len());
    let mut offering_keys = Vec::new();
    let mut override_pointers = Vec::new();
    let mut priced_offerings = 0;
    let mut overrides = 0;
    let mut overrides_are_contradictions = true;
    let mut overrides_point_into_offerings = true;
    for model in content.models() {
        model_ids.push(model.id.as_str().to_owned());
        for offering in &model.offerings {
            offering_keys.push(format!(
                "{}|{}|{}",
                model.id, offering.provider, offering.published_model_id
            ));
            priced_offerings += usize::from(offering.price.is_some());
            overrides += offering.overrides.len();
            let stated: Vec<ModelField> =
                offering.overrides.iter().map(|(field, _)| *field).collect();
            // An override is a claim that this provider contradicts the neutral
            // record on that field. Recomputed from what was stored, so a
            // normalization that recorded an override it cannot justify — or
            // dropped a provider value in favour of the neutral one — is a
            // finding rather than a passing run.
            match model.neutral.as_ref() {
                Some(neutral) => {
                    // Compared whole, in the fixed field order both sides are
                    // built in, so a difference the import failed to record
                    // fails the check as loudly as one it invented.
                    let contradicted: Vec<ModelField> = offering.facts.differences(neutral);
                    if stated != contradicted {
                        overrides_are_contradictions = false;
                    }
                }
                // Nothing to contradict: a model the source describes only
                // through its offerings cannot have an override.
                None => {
                    if !stated.is_empty() {
                        overrides_are_contradictions = false;
                    }
                }
            }
            for (field, pointer) in &offering.overrides {
                if !pointer.as_str().starts_with(offering.pointer.as_str()) {
                    overrides_point_into_offerings = false;
                }
                override_pointers.push(format!(
                    "{}|{}|{}|{}",
                    model.id,
                    offering.provider,
                    field.as_str(),
                    pointer
                ));
            }
        }
    }
    CatalogImport {
        content_id: content.content_id().to_string(),
        source_url: snapshot.source.source_url.clone(),
        schema_version: snapshot.source.schema_version.as_str(),
        providers: content.providers().len(),
        models: content.models().len(),
        offerings: content.offering_count(),
        priced_offerings,
        overrides,
        model_ids,
        offering_keys,
        override_pointers,
        overrides_are_contradictions,
        overrides_point_into_offerings,
        raw_digest: snapshot.source.raw.digest.to_string(),
        raw_bytes: snapshot.source.raw.size_bytes,
    }
}

/// The state a request would be served from: a real [`AppState`], so the
/// no-publication assertion reads the snapshot pointer the request path reads
/// and a publication would swap, rather than a constant of the seam's own.
///
/// Its sinks and stores are the inert ones — usage goes nowhere, no budget, no
/// rate limiter, no denylist — and building it opens nothing: the HTTP client is
/// constructed, never used.
fn runtime_state() -> &'static AppState {
    static STATE: OnceLock<AppState> = OnceLock::new();
    STATE
        .get_or_init(|| build_state(seam_config().clone()).expect("the seam's own config resolves"))
}

fn build_state(config: Config) -> Result<AppState, SnapshotError> {
    AppState::with_resources(
        config,
        &seam_env(),
        Arc::new(UsageDelivery::telemetry(UsageFanout::new(Vec::new()))),
        Box::new(NoBudget),
        Box::new(NoLimit),
        Box::new(NoDenylist),
        ReplicaObservability::stateless(),
    )
}

/// A [`models_dev::CatalogFetch`] that serves bytes already in hand and records
/// what it was asked for. No socket, no DNS, no environment: the import path
/// runs whole, and the only thing it can reach is this buffer.
struct RecordingFetch {
    payload: Vec<u8>,
    etag: Option<String>,
    requested: Arc<Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl models_dev::CatalogFetch for RecordingFetch {
    async fn get(
        &self,
        url: &str,
        _validators: Option<&SourceValidators>,
    ) -> Result<models_dev::FetchResponse, models_dev::FetchError> {
        self.requested
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(url.to_owned());
        Ok(models_dev::FetchResponse::Payload {
            bytes: self.payload.clone(),
            validators: self
                .etag
                .as_deref()
                .map_or_else(SourceValidators::default, SourceValidators::etag),
        })
    }
}

/// Synthetic HS256 material. Not a secret: it is committed, published in the
/// fuzz corpus, and accepted by nothing but this seam.
const HS256_MATERIAL: &str = "axond-fuzz-hs256-material-not-a-secret";

/// A synthetic 32-byte Ed25519 public key. There is no matching private key in
/// this repository, so every EdDSA signature the fuzzer produces is invalid by
/// construction — which is the point: it keeps the signature-failure path hot.
const EDDSA_PUBLIC_BASE64: &str = "ZnV6ei1heG9uZC1lZDI1NTE5LXB1YmxpYy1rZXktMzI=";

const CONFIG: &str = r#"
[[namespace]]
id = "fuzz"
default = true

[[namespace]]
id = "denied"

[[gateway_key]]
env = "AXOND_FUZZ_STATIC_KEY"
namespace = "fuzz"

[gateway_token]
audience = "fuzz.axond.invalid"

[[gateway_verifier]]
kid = "fuzz-hs256"
alg = "HS256"
env = "AXOND_FUZZ_HS256"
namespaces = ["fuzz"]
max_ttl = "15m"

[[gateway_verifier]]
kid = "fuzz-eddsa"
alg = "EdDSA"
env = "AXOND_FUZZ_EDDSA"
namespaces = ["fuzz", "denied"]
max_ttl = "15m"

[[gateway_token_epoch]]
namespace = "fuzz"
min_iat = {MIN_IAT}

# One provider and one alias, so the request path this seam compiles has a
# routing table with something in it: `catalog_import` asserts that importing a
# catalogue leaves that table byte-for-byte alone, and a table that was empty to
# begin with would assert nothing. Neither is reachable — the base URL resolves
# nowhere and no target is ever dispatched.
[[provider]]
id = "fuzz-provider"
kind = "openai-compatible"
base_url = "https://provider.fuzz.axond.invalid/v1"

[[credential]]
namespace = "fuzz"
provider = "fuzz-provider"
env = "AXOND_FUZZ_PROVIDER_KEY"

[[price]]
provider = "fuzz-provider"
model = "fuzz-upstream-model"
input_microdollars_per_million = 1
output_microdollars_per_million = 1
"#;

/// The configuration this seam compiles: the one an assertion about the request
/// path is made against.
fn seam_config() -> &'static Config {
    static CONFIG_ONCE: OnceLock<Config> = OnceLock::new();
    CONFIG_ONCE.get_or_init(|| {
        // The epoch is the one value that cannot be committed: see
        // [`epoch_min_iat`].
        let text = CONFIG.replace("{MIN_IAT}", &epoch_min_iat().to_string());
        Config::from_toml_str(&text).expect("the seam's own config is valid")
    })
}

/// The environment the seam resolves its config against. Committed synthetic
/// values; the process environment is never read.
fn seam_env() -> HashMap<String, String> {
    HashMap::from([
        (
            "AXOND_FUZZ_STATIC_KEY".to_owned(),
            "axond-fuzz-static-key-not-a-secret".to_owned(),
        ),
        ("AXOND_FUZZ_HS256".to_owned(), HS256_MATERIAL.to_owned()),
        (
            "AXOND_FUZZ_EDDSA".to_owned(),
            EDDSA_PUBLIC_BASE64.to_owned(),
        ),
        (
            "AXOND_FUZZ_PROVIDER_KEY".to_owned(),
            "axond-fuzz-provider-key-not-a-secret".to_owned(),
        ),
    ])
}

fn verifier() -> &'static TokenVerifier {
    static VERIFIER: OnceLock<TokenVerifier> = OnceLock::new();
    VERIFIER.get_or_init(|| {
        let config = seam_config();
        let env = seam_env();
        TokenVerifier::build(config, &env)
            .expect("the seam's own verifiers build")
            .expect("the seam configures verifiers")
    })
}

fn code(error: &TokenVerificationError) -> &'static str {
    error.code()
}
