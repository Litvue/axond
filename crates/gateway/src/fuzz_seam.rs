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
//! secret: the key material below is committed synthetic test material.
#![cfg(fuzzing)]
// Only the handful of items the seams below reach are live in this target; the
// rest of the crate is compiled for its `crate::` paths. A re-export whose only
// consumer is `main.rs` is unused here for the same reason, since this target
// compiles the modules without the binary that drives them.
#![allow(dead_code, unused_imports)]

// Keep this list identical to `main.rs`. `tests/fuzz_seam.rs` fails if it drifts.
mod admission;
mod aliases;
mod api;
mod backends;
mod backoff;
mod budget;
mod config;
mod core_accounting;
mod credentials;
mod desired_state;
mod discovery;
mod error;
mod key_material;
mod namespace;
mod pricing;
mod principals;
mod routes;
mod settlement;
mod shutdown;
mod state;
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
use crate::budget::StoreBudget;
use crate::config::{Config, Provider};
use crate::state::{AppState, ConfigSnapshot, SnapshotError};
use crate::store::SqliteStore;
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigShape {
    pub namespaces: usize,
    pub providers: usize,
    pub credentials: usize,
    pub gateway_keys: usize,
}

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
            namespaces: config.namespace.len(),
            providers: config.provider.len(),
            credentials: config.credential.len(),
            gateway_keys: config.gateway_key.len(),
        }),
        Err(config::ConfigError::Load(message)) => Err(Rejection::Load(message)),
        Err(config::ConfigError::Invalid(message)) => Err(Rejection::Invalid(message)),
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

const CONFIG: &str = r#"
# The path is never opened: the seam builds no store, and validation refuses a
# non-durable `:memory:` path here.
[storage]
backend = "sqlite"
path = "axond-fuzz-never-opened.db"

[[namespace]]
id = "fuzz"
default = true

[[namespace]]
id = "denied"

[[gateway_key]]
env = "AXOND_FUZZ_STATIC_KEY"
namespace = "fuzz"

# One provider so the request path this seam compiles has a routing table
# with something in it: `catalog_import` asserts that importing a catalogue
# leaves those provider ids byte-for-byte alone, and a table that was empty
# to begin with would assert nothing. Catalogue import is last-known-good /
# metadata-only (ADR 0063): it must not publish this table. The provider is
# unreachable — the base URL resolves nowhere and no target is ever dispatched.
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
    CONFIG_ONCE
        .get_or_init(|| Config::from_toml_str(CONFIG).expect("the seam's own config is valid"))
}

/// The environment the seam resolves its config against. Committed synthetic
/// values; the process environment is never read.
fn seam_env() -> HashMap<String, String> {
    HashMap::from([
        (
            "AXOND_FUZZ_STATIC_KEY".to_owned(),
            "axond-fuzz-static-key-not-a-secret".to_owned(),
        ),
        (
            "AXOND_FUZZ_PROVIDER_KEY".to_owned(),
            "axond-fuzz-provider-key-not-a-secret".to_owned(),
        ),
    ])
}
