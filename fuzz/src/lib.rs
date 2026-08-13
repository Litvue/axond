//! The bodies of Axond's fuzz targets, plus the invariants they assert.
//!
//! The `fuzz_targets/*.rs` files are thin `fuzz_target!` shells around the
//! functions here, and `src/bin/smoke.rs` replays the committed seed corpora
//! through the same functions on a stable toolchain. Both drivers therefore
//! check identical properties, and a scheduled finding reproduces from a seed
//! file without a nightly compiler.
//!
//! Every target consumes untrusted bytes and asserts three things:
//!
//! 1. the parser returns — a panic, an abort, or a hang is the finding;
//! 2. a refusal is a *typed* value the gateway could answer with, never a
//!    stringly-typed surprise;
//! 3. what the parser accepts stays inside the bounds the gateway relies on
//!    (decoded lengths, namespaces, subjects, signer authority).
//!
//! Each body also returns the *class* of outcome it produced. libFuzzer ignores
//! it; the smoke aggregates it, so a seam that silently began refusing every
//! input at the door — the failure mode that makes a green fuzz lane worthless —
//! fails the lane instead of passing it.
//!
//! Nothing here opens a socket, reads a file, or looks at the environment: the
//! seam in `axond` is built from a config compiled into the binary with
//! synthetic key material, so a fuzz run is hermetic and holds no real secret.

mod wire;

use std::sync::OnceLock;

use arbitrary::Arbitrary;
use axond_fuzz_seam::{Rejection, VerifiedToken};

pub use wire::{
    GATEWAY_CREDENTIAL_CANARY, PROVIDER_URL_CANARY, ProviderStreamInput, SseInput, StreamShape,
    UpstreamFailure, assert_disclosure_check_survives_escaping, assert_valid_fixtures_are_stable,
    provider_error, provider_stream, sse_decode, sse_decode_at_limit, sse_events,
};

/// The ceiling a catalogue refusal's operator-facing text stays under.
///
/// Generous next to the excerpts the parser emits and tiny next to the 64 MiB a
/// payload may be: the property is that refusal size does not scale with the
/// payload, not that any particular wording fits.
const CATALOG_REFUSAL_BYTES: usize = 4096;

/// A refusal must carry an operator-facing reason. An empty one would reach a
/// log or a response body as a blank message.
///
/// Returns the outcome class: a token's own stable code, or the variant name.
fn assert_typed(rejection: &Rejection) -> &'static str {
    match rejection {
        Rejection::Load(message) => {
            assert!(!message.is_empty(), "typed rejection carries no message");
            "load"
        }
        Rejection::Invalid(message) => {
            assert!(!message.is_empty(), "typed rejection carries no message");
            "invalid"
        }
        Rejection::BadRequest(message) => {
            assert!(!message.is_empty(), "typed rejection carries no message");
            "bad_request"
        }
        Rejection::Unauthenticated(code) | Rejection::Unauthorized(code) => {
            assert!(
                code.starts_with("token_"),
                "unexpected authentication code {code:?}"
            );
            code
        }
        Rejection::Catalog {
            code,
            message,
            pointer,
        } => {
            assert!(!message.is_empty(), "typed rejection carries no message");
            // A refusal is logged on every scheduled retry, so an upstream that
            // chooses its own text must not get to choose how much of this
            // gateway's log it occupies.
            assert!(
                message.len() <= CATALOG_REFUSAL_BYTES,
                "a catalogue refusal quoted {} bytes of the payload back: {message:.256?}",
                message.len()
            );
            if let Some(pointer) = pointer {
                // A location an operator can act on: RFC 6901, so rooted.
                assert!(
                    pointer.starts_with('/'),
                    "catalogue refusal names a pointer that is not one: {pointer:?}"
                );
                assert!(
                    pointer.len() <= CATALOG_REFUSAL_BYTES,
                    "a catalogue refusal named a {}-byte pointer",
                    pointer.len()
                );
            }
            code
        }
        // The seam resolves no store, so this is unreachable by construction.
        Rejection::Unavailable => panic!("the stateless seam reported a store as unavailable"),
    }
}

/// Untrusted configuration text: what an operator's file, a mounted ConfigMap,
/// or a reload of either hands the loader.
pub fn config_toml(data: &[u8]) -> &'static str {
    let Ok(text) = str::from_utf8(data) else {
        return "not_utf8";
    };
    let first = axond_fuzz_seam::config_from_toml_str(text);
    let outcome = match &first {
        Ok(shape) => {
            // A config that validated is a config the process would serve, so
            // its own invariants have to hold on the way out — and which
            // invariants those are is the mode's decision (ADR 0027).
            if shape.stateful {
                // The control plane owns every resource in stateful mode, so a
                // file that declared one must have been refused. An accepted
                // config that still carries one would mean two authorities
                // disagree about the same resource at boot.
                assert_eq!(
                    (
                        shape.namespaces,
                        shape.providers,
                        shape.models,
                        shape.credentials,
                        shape.gateway_keys,
                        shape.verifiers
                    ),
                    (0, 0, 0, 0, 0, 0),
                    "a stateful config was accepted with control-plane-owned sections: {shape:?}"
                );
                "accepted_stateful"
            } else {
                // Stateless mode resolves everything from the file, so it must
                // name the namespace a request resolves into, and a verifier or
                // a credential is only meaningful scoped to one.
                assert!(
                    shape.namespaces >= 1,
                    "an accepted stateless config defines no namespace"
                );
                "accepted"
            }
        }
        Err(rejection) => assert_typed(rejection),
    };
    // Loading is a pure function of the text: boot and every later reload of an
    // unchanged file must agree, or a replica could serve a config its peer
    // refused.
    assert_eq!(
        first.is_ok(),
        axond_fuzz_seam::config_from_toml_str(text).is_ok(),
        "the same configuration text was accepted and refused"
    );
    outcome
}

/// An untrusted `GET /v1/credentials/status?...` query string: malformed
/// percent-encoding, duplicate keys, empty values, and oversized inputs.
pub fn credentials_query(data: &[u8]) -> &'static str {
    let Ok(text) = str::from_utf8(data) else {
        return "not_utf8";
    };
    let outcome = axond_fuzz_seam::credentials_query_namespaces(Some(text));
    let class = match &outcome {
        Ok(Some(value)) => {
            // Percent-decoding only ever shrinks, so an accepted value cannot
            // be an amplification vector: three bytes in, one byte out.
            assert!(
                value.len() <= text.len(),
                "decoding expanded a {}-byte query into {} bytes",
                text.len(),
                value.len()
            );
            // A `%` can only survive decoding as an escaped one (`%25`), which
            // shrinks; a bare one is a rejection.
            assert!(
                value.matches('%').count() <= text.matches('%').count(),
                "decoding invented percent signs: {value:?} from {text:?}"
            );
            if value.is_empty() {
                "accepted_empty"
            } else {
                "accepted"
            }
        }
        Ok(None) => "absent",
        Err(rejection) => assert_typed(rejection),
    };
    assert_eq!(
        outcome.is_ok(),
        axond_fuzz_seam::credentials_query_namespaces(Some(text)).is_ok(),
        "the same query string was accepted and refused"
    );
    // A query the router never received must parse like an absent filter, and
    // an empty one must not be confused with it.
    assert_eq!(
        axond_fuzz_seam::credentials_query_namespaces(None).expect("no query is not a rejection"),
        None
    );
    class
}

/// What the catalogue target does with the bytes it is given.
///
/// Raw payloads find the decoding, schema, and normalization paths; edits of the
/// bundled models.dev seed find the *semantic* ones — an import that still
/// parses but says something different, which is the case a byte flip almost
/// never reaches and the case an operator actually gets.
#[derive(Debug, Arbitrary)]
pub enum CatalogInput<'a> {
    /// An arbitrary payload, as a mirror could answer with.
    Payload {
        bytes: &'a [u8],
        etag: Option<&'a str>,
    },
    /// The bundled seed, re-rendered with its object keys rotated by `rotate`
    /// and its whitespace chosen by `pretty`, then edited.
    ///
    /// The rendering is part of every case on purpose: normalization has to be
    /// blind to key order and whitespace, so every semantic assertion below is
    /// also a reordering assertion.
    Edited {
        edit: CatalogEdit<'a>,
        rotate: u8,
        pretty: bool,
    },
}

/// One edit to the seed. Exactly one, so the class of change it should produce
/// is unambiguous: a target that applied two could not tell a price-only import
/// from a metadata-only one.
#[derive(Debug, Arbitrary)]
pub enum CatalogEdit<'a> {
    /// Nothing but the re-rendering: the identity check on its own.
    None,
    /// A field the schema does not define, on one offering. Unknown fields add
    /// information the gateway does not model; they must not change what it
    /// stored.
    Unknown {
        provider: u16,
        model: u16,
        key: &'a str,
        value: &'a str,
    },
    /// One published rate of one offering.
    Cost {
        provider: u16,
        model: u16,
        field: CostField,
        value: f32,
    },
    /// One descriptive field of one offering.
    Metadata {
        provider: u16,
        model: u16,
        field: MetaField,
        value: &'a str,
    },
    /// One capability flag of one offering.
    Capability {
        provider: u16,
        model: u16,
        field: CapabilityField,
        value: bool,
    },
    /// The lifecycle status of one offering.
    Lifecycle {
        provider: u16,
        model: u16,
        status: LifecycleValue,
    },
    /// One descriptive field of a provider-neutral record, which is what an
    /// offering's overrides are measured against.
    Neutral {
        model: u16,
        field: MetaField,
        value: &'a str,
    },
    /// Arbitrary bytes spliced into the rendered document: schema drift, with a
    /// valid catalogue around it.
    Splice { at: u16, bytes: &'a [u8] },
}

#[derive(Debug, Arbitrary, Clone, Copy)]
pub enum CostField {
    Input,
    Output,
    CacheRead,
}

#[derive(Debug, Arbitrary, Clone, Copy)]
pub enum MetaField {
    Name,
    Family,
    Knowledge,
    ReleaseDate,
    LastUpdated,
}

#[derive(Debug, Arbitrary, Clone, Copy)]
pub enum CapabilityField {
    Attachment,
    Reasoning,
    ToolCall,
    StructuredOutput,
    Temperature,
}

#[derive(Debug, Arbitrary, Clone, Copy)]
pub enum LifecycleValue {
    Available,
    Alpha,
    Beta,
    Deprecated,
    /// Whatever the fuzzer wants, including a status the vocabulary does not
    /// define — which must be refused rather than folded onto a default.
    Unknown(u8),
}

impl CostField {
    const fn key(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Output => "output",
            Self::CacheRead => "cache_read",
        }
    }
}

impl MetaField {
    const fn key(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::Family => "family",
            Self::Knowledge => "knowledge",
            Self::ReleaseDate => "release_date",
            Self::LastUpdated => "last_updated",
        }
    }
}

impl CapabilityField {
    const fn key(self) -> &'static str {
        match self {
            Self::Attachment => "attachment",
            Self::Reasoning => "reasoning",
            Self::ToolCall => "tool_call",
            Self::StructuredOutput => "structured_output",
            Self::Temperature => "temperature",
        }
    }
}

impl LifecycleValue {
    fn value(self) -> String {
        match self {
            Self::Available => "available".to_owned(),
            Self::Alpha => "alpha".to_owned(),
            Self::Beta => "beta".to_owned(),
            Self::Deprecated => "deprecated".to_owned(),
            Self::Unknown(byte) => format!("status-{byte}"),
        }
    }
}

/// A models.dev catalogue import: decoding, schema validation, normalization,
/// content identity, semantic classification, and admission over a
/// last-known-good catalogue.
pub fn catalog_import(input: &CatalogInput<'_>) -> &'static str {
    match input {
        CatalogInput::Payload { bytes, etag } => import(bytes, *etag).0,
        CatalogInput::Edited {
            edit,
            rotate,
            pretty,
        } => {
            let Some(seed) = seed_document() else {
                return "unrenderable";
            };
            let render = |value: &serde_json::Value| render_json(value, *rotate, *pretty);
            match edit {
                CatalogEdit::None => {
                    // Same catalogue, different spelling: the identity must not
                    // notice, so the import is a no-op rather than an update.
                    let (class, import) = import(render(&seed).as_bytes(), None);
                    let import = import.unwrap_or_else(|| {
                        panic!("re-rendering the seed made it unimportable: {class}")
                    });
                    assert_eq!(
                        import.content_id,
                        axond_fuzz_seam::catalog_seed_content_id(),
                        "reordering keys or whitespace changed the content identity"
                    );
                    assert_eq!(class, "unchanged");
                    "rendered"
                }
                CatalogEdit::Unknown {
                    provider,
                    model,
                    key,
                    value,
                } => {
                    let mut document = seed.clone();
                    // Namespaced, so it is unknown whatever the fuzzer chose: a
                    // key that collided with a schema field would be testing
                    // the schema, not the tolerance of unknown fields.
                    let key = format!("x-fuzz-{key}");
                    if !edit_offering(&mut document, *provider, *model, |offering| {
                        offering
                            .insert(key.clone(), serde_json::Value::String((*value).to_owned()));
                    }) {
                        return "unaddressable";
                    }
                    let (class, import) = import(render(&document).as_bytes(), None);
                    if let Some(import) = import {
                        assert_eq!(
                            import.content_id,
                            axond_fuzz_seam::catalog_seed_content_id(),
                            "an unknown field changed what was stored"
                        );
                        assert_eq!(class, "unchanged");
                        "unknown_field_ignored"
                    } else {
                        class
                    }
                }
                CatalogEdit::Cost {
                    provider,
                    model,
                    field,
                    value,
                } => {
                    let Some(number) = serde_json::Number::from_f64(f64::from(*value)) else {
                        return "unrepresentable";
                    };
                    let mut document = seed.clone();
                    let key = field.key();
                    if !edit_offering(&mut document, *provider, *model, |offering| {
                        let cost = offering
                            .entry("cost")
                            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
                        if let Some(cost) = cost.as_object_mut() {
                            cost.insert(key.to_owned(), serde_json::Value::Number(number.clone()));
                        }
                    }) {
                        return "unaddressable";
                    }
                    let (class, _) = import(render(&document).as_bytes(), None);
                    if let Some(diff) = last_diff(&document, *rotate, *pretty) {
                        assert!(
                            diff.is_price_only(),
                            "changing a published rate was classified as more than a price change: {diff:?}"
                        );
                        return "price_changed";
                    }
                    class
                }
                CatalogEdit::Metadata {
                    provider,
                    model,
                    field,
                    value,
                } => {
                    let mut document = seed.clone();
                    let key = field.key();
                    if !edit_offering(&mut document, *provider, *model, |offering| {
                        offering.insert(
                            key.to_owned(),
                            serde_json::Value::String((*value).to_owned()),
                        );
                    }) {
                        return "unaddressable";
                    }
                    let (class, _) = import(render(&document).as_bytes(), None);
                    assert_metadata_only(&document, *rotate, *pretty, class, "metadata_changed")
                }
                CatalogEdit::Capability {
                    provider,
                    model,
                    field,
                    value,
                } => {
                    let mut document = seed.clone();
                    let key = field.key();
                    if !edit_offering(&mut document, *provider, *model, |offering| {
                        offering.insert(key.to_owned(), serde_json::Value::Bool(*value));
                    }) {
                        return "unaddressable";
                    }
                    let (class, _) = import(render(&document).as_bytes(), None);
                    assert_metadata_only(&document, *rotate, *pretty, class, "capability_changed")
                }
                CatalogEdit::Lifecycle {
                    provider,
                    model,
                    status,
                } => {
                    let mut document = seed.clone();
                    let status = status.value();
                    if !edit_offering(&mut document, *provider, *model, |offering| {
                        offering.insert(
                            "status".to_owned(),
                            serde_json::Value::String(status.clone()),
                        );
                    }) {
                        return "unaddressable";
                    }
                    let (class, _) = import(render(&document).as_bytes(), None);
                    assert_metadata_only(&document, *rotate, *pretty, class, "lifecycle_changed")
                }
                CatalogEdit::Neutral {
                    model,
                    field,
                    value,
                } => {
                    let mut document = seed.clone();
                    let key = field.key();
                    if !edit_neutral(&mut document, *model, |neutral| {
                        neutral.insert(
                            key.to_owned(),
                            serde_json::Value::String((*value).to_owned()),
                        );
                    }) {
                        return "unaddressable";
                    }
                    let (class, _) = import(render(&document).as_bytes(), None);
                    assert_metadata_only(&document, *rotate, *pretty, class, "neutral_changed")
                }
                CatalogEdit::Splice { at, bytes } => {
                    let mut rendered = render(&seed).into_bytes();
                    let at = (*at as usize) % (rendered.len() + 1);
                    rendered.splice(at..at, bytes.iter().copied());
                    import(&rendered, None).0
                }
            }
        }
    }
}

/// Drive one payload through the whole import path and assert what must hold
/// however it was produced.
///
/// Returns the outcome class and, when the payload was accepted, what it
/// normalized to.
fn import(
    payload: &[u8],
    etag: Option<&str>,
) -> (&'static str, Option<axond_fuzz_seam::CatalogImport>) {
    // The routing table a request would be served from, read before and after:
    // a catalogue import records metadata and must never publish runtime state.
    // Read live off the state's snapshot pointer, so publication is what the
    // comparison watches — calibrated once per process against a separate state
    // that is deliberately published into, since a comparison that cannot move
    // proves nothing about the path that must not move it.
    static PUBLICATION_IS_OBSERVABLE: OnceLock<bool> = OnceLock::new();
    assert!(
        *PUBLICATION_IS_OBSERVABLE.get_or_init(axond_fuzz_seam::publication_moves_runtime_routes),
        "publishing a snapshot did not move the observed routing table: the \
         no-publication check is blind"
    );
    let routes = axond_fuzz_seam::runtime_routes();

    let parsed = axond_fuzz_seam::catalog_parse(payload, FETCHED_AT, etag);
    let admission = axond_fuzz_seam::catalog_import_over_seed(payload, etag);

    // Offline by construction: the only thing the import path reached for is the
    // configured URL, served from a buffer already in hand. A second request, or
    // a request for anything else, would mean the path grew a transfer.
    assert_eq!(
        admission.fetched,
        vec![axond_fuzz_seam::catalog_source_url()],
        "the import path made an unexpected fetch"
    );

    let class = match &parsed {
        Err(rejection) => {
            let class = assert_typed(rejection);
            // The whole point of the last-known-good rule: a payload that was
            // refused cannot have disturbed what is being served.
            assert_eq!(admission.outcome, "refused");
            assert!(
                admission.active_is_seed,
                "a refused payload replaced the active catalogue"
            );
            assert_eq!(
                admission.active_content_id,
                axond_fuzz_seam::catalog_seed_content_id(),
                "a refused payload changed the active content identity"
            );
            class
        }
        Ok(import) => {
            assert_accepted_catalog(import);
            // Provenance is not identity: the same bytes retrieved at another
            // time, with other validators, are the same catalogue.
            let again =
                axond_fuzz_seam::catalog_parse(payload, FETCHED_AT + 86_400, Some("W/\"other\""))
                    .expect("a payload that parsed once parses again");
            assert_eq!(
                import.content_id, again.content_id,
                "the fetch time or the validators reached the content identity"
            );
            assert_eq!(
                import.model_ids, again.model_ids,
                "normalization is not deterministic"
            );
            assert_eq!(
                admission.active_content_id, import.content_id,
                "the admitted content is not what was imported"
            );
            if import.content_id == axond_fuzz_seam::catalog_seed_content_id() {
                assert_eq!(admission.outcome, "unchanged");
                assert!(admission.diff.is_none(), "unchanged content carried a diff");
                "unchanged"
            } else {
                assert_eq!(admission.outcome, "updated");
                let diff = admission.diff.expect("an update carries a diff");
                assert!(diff.changes > 0, "an update carried an empty diff");
                "updated"
            }
        }
    };
    assert_eq!(
        routes,
        axond_fuzz_seam::runtime_routes(),
        "a catalogue import changed the routing table a request is served from"
    );
    (class, parsed.ok())
}

/// What an accepted catalogue must be, whatever the payload said.
fn assert_accepted_catalog(import: &axond_fuzz_seam::CatalogImport) {
    // A catalogue nothing offers is refused, not admitted: a document that kept
    // its model records and lost its providers section would otherwise take the
    // place of one that can still be routed and priced from.
    assert!(
        import.models > 0 && import.providers > 0 && import.offerings > 0,
        "an empty catalogue was accepted: {import:?}"
    );
    assert_eq!(
        import.offering_keys.len(),
        import.offerings,
        "the stored offerings and their keys disagree"
    );
    assert_eq!(
        import.model_ids.len(),
        import.models,
        "the stored models and their ids disagree"
    );
    // Normalization sorts and de-duplicates, which is what makes the content
    // identity a function of the catalogue rather than of the document.
    let mut sorted = import.model_ids.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted, import.model_ids, "stored models are not sorted");
    let mut offerings = import.offering_keys.clone();
    offerings.sort();
    offerings.dedup();
    assert_eq!(
        offerings.len(),
        import.offering_keys.len(),
        "the same offering was stored twice"
    );
    // Calibrated the same way the no-publication check is: an oracle that can
    // only catch an invented override, and not a dropped one, would pass every
    // run while the guarantee the docs state went missing.
    static OMISSIONS_ARE_VISIBLE: OnceLock<bool> = OnceLock::new();
    assert!(
        *OMISSIONS_ARE_VISIBLE
            .get_or_init(axond_fuzz_seam::override_oracle_notices_a_missing_override),
        "removing a recorded override went unnoticed: the override check is blind"
    );
    assert!(
        import.overrides_are_contradictions,
        "an override was recorded where the provider agrees with the neutral \
         record, or a difference it states was not recorded"
    );
    assert!(
        import.overrides_point_into_offerings,
        "an override points outside the offering that states it"
    );
    assert_eq!(
        import.override_pointers.len(),
        import.overrides,
        "an override was recorded without a pointer"
    );
    assert!(
        import.priced_offerings <= import.offerings,
        "more prices than offerings"
    );
    assert_eq!(
        import.source_url,
        axond_fuzz_seam::catalog_source_url(),
        "an import was attributed to another source"
    );
    assert!(
        !import.content_id.is_empty() && !import.raw_digest.is_empty(),
        "an accepted import has no identity"
    );
}

/// The diff between the seed and an edited document, when the edit changed the
/// catalogue at all.
fn last_diff(
    document: &serde_json::Value,
    rotate: u8,
    pretty: bool,
) -> Option<axond_fuzz_seam::CatalogDiffShape> {
    axond_fuzz_seam::catalog_diff(
        axond_fuzz_seam::CATALOG_SEED_PAYLOAD.as_bytes(),
        render_json(document, rotate, pretty).as_bytes(),
    )
    .ok()
    .flatten()
}

/// An edit to something the catalogue calls metadata must be classified as
/// metadata — never as a price change, which is what a spend decision reads.
fn assert_metadata_only(
    document: &serde_json::Value,
    rotate: u8,
    pretty: bool,
    class: &'static str,
    changed: &'static str,
) -> &'static str {
    match last_diff(document, rotate, pretty) {
        Some(diff) => {
            assert!(
                diff.is_metadata_only(),
                "a metadata edit was classified as more than metadata: {diff:?}"
            );
            changed
        }
        None => class,
    }
}

/// The bundled seed as a JSON document, ready to edit.
fn seed_document() -> Option<serde_json::Value> {
    serde_json::from_str(axond_fuzz_seam::CATALOG_SEED_PAYLOAD).ok()
}

/// Apply an edit to the `providers[…].models[…]` object the indices select.
///
/// `false` when the document has no such offering, which is not a finding: the
/// fuzzer chose an index the seed does not have.
fn edit_offering(
    document: &mut serde_json::Value,
    provider: u16,
    model: u16,
    edit: impl FnOnce(&mut serde_json::Map<String, serde_json::Value>),
) -> bool {
    let providers = document
        .get_mut("providers")
        .and_then(|providers| providers.as_object_mut());
    let Some(providers) = providers else {
        return false;
    };
    if providers.is_empty() {
        return false;
    }
    let index = (provider as usize) % providers.len();
    let Some(provider) = providers.values_mut().nth(index) else {
        return false;
    };
    let models = provider
        .get_mut("models")
        .and_then(|models| models.as_object_mut());
    let Some(models) = models else {
        return false;
    };
    if models.is_empty() {
        return false;
    }
    let index = (model as usize) % models.len();
    let Some(offering) = models
        .values_mut()
        .nth(index)
        .and_then(|offering| offering.as_object_mut())
    else {
        return false;
    };
    edit(offering);
    true
}

/// Apply an edit to the `models[…]` provider-neutral record the index selects.
fn edit_neutral(
    document: &mut serde_json::Value,
    model: u16,
    edit: impl FnOnce(&mut serde_json::Map<String, serde_json::Value>),
) -> bool {
    let models = document
        .get_mut("models")
        .and_then(|models| models.as_object_mut());
    let Some(models) = models else {
        return false;
    };
    if models.is_empty() {
        return false;
    }
    let index = (model as usize) % models.len();
    let Some(neutral) = models
        .values_mut()
        .nth(index)
        .and_then(|model| model.as_object_mut())
    else {
        return false;
    };
    edit(neutral);
    true
}

/// Serialize a document with every object's keys rotated by `rotate` and its
/// whitespace chosen by `pretty`.
///
/// `serde_json` stores objects in a sorted map, so re-serializing one is already
/// a reordering of the committed seed; rotating on top of that reaches the
/// orders a mirror, a proxy, or a regenerated upstream document would produce.
/// Key order and whitespace are exactly what a content identity must be blind
/// to, so every edited case is rendered this way.
fn render_json(value: &serde_json::Value, rotate: u8, pretty: bool) -> String {
    let mut out = String::new();
    write_json(&mut out, value, rotate, pretty, 0);
    out
}

fn write_json(out: &mut String, value: &serde_json::Value, rotate: u8, pretty: bool, depth: usize) {
    match value {
        serde_json::Value::Object(map) if !map.is_empty() => {
            let keys: Vec<&String> = map.keys().collect();
            let start = (rotate as usize) % keys.len();
            out.push('{');
            for (position, offset) in (0..keys.len()).enumerate() {
                let key = keys[(start + offset) % keys.len()];
                if position > 0 {
                    out.push(',');
                }
                newline(out, pretty, depth + 1);
                out.push_str(&serde_json::Value::String((*key).clone()).to_string());
                out.push(':');
                if pretty {
                    out.push(' ');
                }
                write_json(out, &map[key], rotate, pretty, depth + 1);
            }
            newline(out, pretty, depth);
            out.push('}');
        }
        serde_json::Value::Array(items) if !items.is_empty() => {
            out.push('[');
            for (position, item) in items.iter().enumerate() {
                if position > 0 {
                    out.push(',');
                }
                newline(out, pretty, depth + 1);
                write_json(out, item, rotate, pretty, depth + 1);
            }
            newline(out, pretty, depth);
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

fn newline(out: &mut String, pretty: bool, depth: usize) {
    if pretty {
        out.push('\n');
        for _ in 0..depth {
            out.push_str("  ");
        }
    }
}

/// The fetch time an import is stamped with in this target. Fixed, because it is
/// provenance: an identity that moved with it would be the finding.
const FETCHED_AT: u64 = axond_fuzz_seam::CATALOG_FETCHED_AT_SECS;

/// What the token target does with the bytes it is given.
///
/// Raw credentials find decoding and signature bugs; minted ones get past the
/// signature so the claim checks — audience, lifetime, namespace, scope, epoch —
/// are reachable at all.
#[derive(Debug, Arbitrary)]
pub enum TokenInput<'a> {
    /// An arbitrary credential presented as `Authorization: Bearer …`.
    Presented(&'a str),
    /// Claims signed with the seam's synthetic HS256 material.
    Minted {
        namespace: &'a str,
        subject: &'a str,
        audience: Option<&'a str>,
        ttl_seconds: u64,
        issued_at: Option<u64>,
        /// `None` omits the claim, which is an *unrestricted* token; `Some` of an
        /// empty vector writes `"scope": []`, which permits nothing. Both
        /// shapes have to be reachable, because confusing them is the bug worth
        /// finding.
        scope: Option<Vec<&'a str>>,
        aliases: Option<Vec<&'a str>>,
    },
}

/// Inbound token verification: JWS decoding, key selection, signature, and
/// every claim check behind them.
pub fn token_verify(input: &TokenInput<'_>) -> &'static str {
    match input {
        TokenInput::Presented(credential) => check_verification(credential, None),
        TokenInput::Minted {
            namespace,
            subject,
            audience,
            ttl_seconds,
            issued_at,
            scope,
            aliases,
        } => {
            let audience = audience.unwrap_or(axond_fuzz_seam::AUDIENCE);
            let Some(token) = axond_fuzz_seam::mint_hs256_token(
                namespace,
                subject,
                audience,
                *ttl_seconds,
                *issued_at,
                scope
                    .as_ref()
                    .map(|values| values.iter().map(|value| (*value).to_owned()).collect()),
                aliases
                    .as_ref()
                    .map(|values| values.iter().map(|value| (*value).to_owned()).collect()),
            ) else {
                return "unmintable";
            };
            check_verification(&token, Some(audience))
        }
    }
}

/// Verify a committed token seed re-signed onto the current run, so the claim
/// check the seed is named for is reached however long ago the seed was written.
///
/// `None` for a seed that is not a signable JWS — most of the corpus, which
/// exists for the decoding path and is replayed as bytes instead.
pub fn token_verify_resigned_seed(seed: &str) -> Option<&'static str> {
    let token = axond_fuzz_seam::resign_seed_onto_this_run(seed)?;
    Some(check_verification(&token, None))
}

/// The properties that hold for every credential, however it was produced.
fn check_verification(credential: &str, minted_audience: Option<&str>) -> &'static str {
    match axond_fuzz_seam::verify_token(credential) {
        Ok(None) => {
            // The verifier owns the `axt1.` shape; declining to answer would
            // hand the credential to a store that does not own it.
            panic!("the token verifier declined to rule on {credential:?}");
        }
        Ok(Some(verified)) => {
            assert_accepted(&verified);
            if let Some(audience) = minted_audience {
                assert_eq!(
                    audience,
                    axond_fuzz_seam::AUDIENCE,
                    "a token for a foreign audience verified"
                );
                // The HS256 signer is scoped to one namespace; a signature it
                // produced must never confer authority over another.
                assert_eq!(
                    verified.namespace,
                    axond_fuzz_seam::NAMESPACES[0],
                    "the HS256 signer minted authority over a namespace it does not hold"
                );
            }
            "accepted"
        }
        Err(rejection) => assert_typed(&rejection),
    }
}

/// Prove the seam verifies signatures for real before any target trusts it.
///
/// Every `token_verify` assertion is worthless against a stubbed verifier, and
/// the fuzz workspace compiles its whole dependency graph with `--cfg fuzzing`
/// (see `.cargo/config.toml`) — a flag some crates use to weaken cryptography on
/// purpose. Rather than trust an audit of the lockfile to stay true across
/// dependency bumps, the required smoke starts here: a token the seam minted
/// verifies, and the same token with one bit flipped in each of its three
/// segments does not.
///
/// # Panics
///
/// If a signature check is not actually happening.
pub fn assert_signature_verification_is_real() {
    let token = axond_fuzz_seam::mint_hs256_token(
        axond_fuzz_seam::NAMESPACES[0],
        "signature-check",
        axond_fuzz_seam::AUDIENCE,
        300,
        None,
        None,
        None,
    )
    .expect("the seam mints its own token");
    assert!(
        matches!(axond_fuzz_seam::verify_token(&token), Ok(Some(_))),
        "the seam cannot verify a token it just minted"
    );

    let body = token
        .strip_prefix("axt1.")
        .expect("a minted token carries the axt1 prefix");
    let segments: Vec<&str> = body.split('.').collect();
    assert_eq!(segments.len(), 3, "a JWS has three segments: {body:?}");
    for segment in 0..3 {
        let mut tampered: Vec<String> = segments.iter().map(|part| (*part).to_owned()).collect();
        // The *first* character, because it carries the leading six bits of the
        // segment's first byte: rewriting a trailing character can land in
        // padding bits that decode to the same bytes.
        let mut characters: Vec<char> = tampered[segment].chars().collect();
        assert!(
            !characters.is_empty(),
            "segment {segment} of a minted token is empty"
        );
        characters[0] = if characters[0] == 'A' { 'B' } else { 'A' };
        tampered[segment] = characters.into_iter().collect();
        let credential = format!("axt1.{}", tampered.join("."));
        let outcome = axond_fuzz_seam::verify_token(&credential);
        assert!(
            matches!(
                outcome,
                Err(Rejection::Unauthenticated(_) | Rejection::Unauthorized(_))
            ),
            "tampering with segment {segment} of a minted token still verified: {outcome:?}"
        );
    }
}

fn assert_accepted(verified: &VerifiedToken) {
    assert!(
        axond_fuzz_seam::NAMESPACES.contains(&verified.namespace.as_str()),
        "a token verified into undeclared namespace {:?}",
        verified.namespace
    );
    assert!(
        !verified.subject.is_empty(),
        "a token verified without a subject"
    );
    // The scope vocabulary is closed, so a token cannot present more distinct
    // capabilities than the gateway defines.
    assert!(
        verified.capabilities <= axond_fuzz_seam::CAPABILITY_COUNT,
        "a token presented {} capabilities, more than the {} defined",
        verified.capabilities,
        axond_fuzz_seam::CAPABILITY_COUNT
    );
}
