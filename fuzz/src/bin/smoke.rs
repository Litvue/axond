//! The bounded, deterministic fuzz smoke that runs on every pull request.
//!
//! Coverage-guided fuzzing is unbounded and needs nightly, so it runs on a
//! schedule (`.github/workflows/fuzz.yml`). What a pull request gets instead is
//! this: every committed seed, plus a fixed set of derived inputs, replayed
//! through the very same target bodies the scheduled run uses, on the pinned
//! stable toolchain, with three bounds that turn the acceptance criteria of
//! issue #212 into a pass/fail signal.
//!
//! - **No panic or abort.** A target body that unwinds fails the run, because
//!   every assertion in `lib.rs` is a property the gateway relies on.
//! - **No hang.** Each input must complete inside [`PER_INPUT_BUDGET`] and the
//!   whole replay inside [`TOTAL_BUDGET`]; a quadratic parser trips these long
//!   before CI's job timeout does.
//! - **No uncontrolled allocation.** Every allocation goes through
//!   [`Capped`], which refuses to hand out more than [`ALLOCATION_CAP`] of live
//!   memory. A parser that sizes a buffer from an attacker-controlled length
//!   dies here with a diagnosis rather than on an OOM-killed runner.
//!
//! It is also evidence that the corpus still reaches the parsers: each target
//! declares how many distinct outcome classes its seeds must produce, so a seam
//! that regressed into refusing everything at the door fails the lane rather
//! than passing it quickly.
//!
//! The derived inputs are truncations, single-byte flips, and one oversized
//! repetition of each seed: enough to exercise the boundary handling that
//! percent-decoding and JWS segment splitting get wrong, and computed from the
//! seed bytes alone, so the run is reproducible from the repository.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arbitrary::{Arbitrary, Unstructured};
use axond_fuzz::{
    BlobSecretCryptoInput, CapabilityField, CatalogEdit, CatalogInput, CostField, LifecycleValue,
    MetaField, ProviderStreamInput, SseInput, StreamShape, TokenInput,
};

/// Live heap the whole replay may hold at once. The parsers under test are
/// bounded by their input, which is why this is generous in absolute terms and
/// still tiny next to what an unbounded pre-allocation would ask for.
const ALLOCATION_CAP: usize = 512 * 1024 * 1024;

/// A single input that takes longer than this is reported as a hang.
const PER_INPUT_BUDGET: Duration = Duration::from_secs(2);

/// The whole replay is a pull-request lane, so it stays inside a minute.
const TOTAL_BUDGET: Duration = Duration::from_secs(60);

/// How large the oversized derivation of each seed is.
const OVERSIZED_BYTES: usize = 66 * 1024;

/// How many outcome classes the freshly-minted token scenarios must reach.
/// [`EXPECTED_MINTED_CLASSES`] pins which ones; this is the floor for the rest.
const MINIMUM_MINTED_CLASSES: usize = 8;

/// How many outcome classes the re-signed token seeds must reach.
const MINIMUM_RESIGNED_CLASSES: usize = 10;

/// How many outcome classes the catalogue edit scenarios must reach.
/// [`EXPECTED_CATALOG_CLASSES`] pins which ones.
const MINIMUM_CATALOG_EDIT_CLASSES: usize = 6;

#[global_allocator]
static ALLOCATOR: Capped = Capped;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);

/// A global allocator that refuses to exceed [`ALLOCATION_CAP`] of live memory.
///
/// Returning null makes Rust's allocation-failure path abort with a message,
/// which is the finding: an input reached a parser that allocated from an
/// attacker-controlled size.
struct Capped;

unsafe impl GlobalAlloc for Capped {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let live = LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
        if live > ALLOCATION_CAP {
            LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
            return std::ptr::null_mut();
        }
        PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
        // SAFETY: the layout is the caller's, forwarded unchanged.
        let pointer = unsafe { System.alloc(layout) };
        if pointer.is_null() {
            // Nothing was handed out, so nothing is live: only `dealloc`
            // subtracts, and a refusal leaves no pointer to deallocate.
            LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        // SAFETY: the pointer and layout are the caller's, forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

struct Target {
    /// The `cargo fuzz` target name, which is also its seed directory.
    name: &'static str,
    /// Replays one input, returning every outcome class it produced.
    run: fn(&[u8]) -> Vec<&'static str>,
    /// How many distinct outcome classes the seeds must still reach.
    minimum_classes: usize,
}

const TARGETS: &[Target] = &[
    Target {
        name: "config_toml",
        run: replay_config_toml,
        minimum_classes: 3,
    },
    Target {
        name: "credentials_query",
        run: replay_credentials_query,
        minimum_classes: 4,
    },
    Target {
        // Below what the corpus reaches today (8), because part of that count
        // comes from arbitrary `Minted` decodings whose lifetime claims are
        // compared against the wall clock: pinning the floor at the observed
        // value would let the clock, not a regression, fail a required lane.
        // The named scenarios below assert the time-sensitive classes exactly.
        name: "token_verify",
        run: replay_token_verify,
        minimum_classes: 6,
    },
    Target {
        name: "sse_decode",
        run: replay_sse_decode,
        minimum_classes: 7,
    },
    Target {
        name: "provider_stream",
        run: replay_provider_stream,
        minimum_classes: 7,
    },
    Target {
        name: "provider_error",
        run: replay_provider_error,
        minimum_classes: 5,
    },
    Target {
        name: "catalog_import",
        run: replay_catalog_import,
        minimum_classes: 6,
    },
    Target {
        name: "blob_secret_envelope",
        run: replay_blob_secret_envelope,
        minimum_classes: 10,
    },
    Target {
        name: "blob_secret_crypto",
        run: replay_blob_secret_crypto,
        minimum_classes: 1,
    },
    Target {
        name: "publication_parsers",
        run: replay_publication_parsers,
        minimum_classes: 6,
    },
];

/// Chunk boundaries the SSE seeds are replayed on. Coprime with nothing in
/// particular — the point is that they land inside `data:` prefixes, inside
/// `\r\n\r\n` delimiters, and inside multi-byte characters.
const SMOKE_CUTS: &[u16] = &[1, 3, 6, 7, 11, 13, 17, 23, 29, 31];

/// A buffer limit the seeds can reach, so the refusal path is replayed rather
/// than merely defined.
const SMOKE_BUFFER_LIMIT: u16 = 48;

/// The statuses every provider-error seed body is classified under, chosen to
/// reach each arm of `from_upstream`: a client refusal, a missing model, a rate
/// limit, and two server-side failures.
const SMOKE_STATUSES: &[u16] = &[400, 404, 413, 429, 500, 503];

/// SSE seeds are readable wire captures, so a seed file is replayed twice: the
/// way libFuzzer replays it, decoded through `Arbitrary`, and as the body it
/// literally is — split on fixed boundaries, once under a limit it cannot trip
/// and once under one it can.
fn replay_sse_decode(data: &[u8]) -> Vec<&'static str> {
    let mut classes = Vec::new();
    if let Ok(input) = SseInput::arbitrary_take_rest(Unstructured::new(data)) {
        classes.extend(axond_fuzz::sse_decode(&input));
    }
    let Ok(body) = str::from_utf8(data) else {
        classes.push("not_utf8");
        return classes;
    };
    // A limit the body cannot trip, then one it always can. The first is sized
    // from the body rather than pinned to `u16::MAX`, because the oversized
    // derivation is `OVERSIZED_BYTES` — one byte past what a `u16` can express,
    // which would make this pass a second refusal rather than a clean decode.
    for max_buffer_bytes in [body.len().max(1), usize::from(SMOKE_BUFFER_LIMIT)] {
        classes.extend(axond_fuzz::sse_decode_at_limit(
            body,
            SMOKE_CUTS,
            max_buffer_bytes,
        ));
    }
    classes
}

/// Provider-stream seeds are wire captures too, so each is decoded into SSE
/// events first and then fed to every decoder shape. A capture of one provider
/// reaching another provider's decoder is the interesting case: it is what a
/// misconfigured or swapped upstream produces.
fn replay_provider_stream(data: &[u8]) -> Vec<&'static str> {
    let mut classes = Vec::new();
    if let Ok(input) = ProviderStreamInput::arbitrary_take_rest(Unstructured::new(data)) {
        classes.extend(axond_fuzz::provider_stream(&input));
    }
    let Ok(body) = str::from_utf8(data) else {
        classes.push("not_utf8");
        return classes;
    };
    let events = axond_fuzz::sse_events(body);
    let borrowed: Vec<(Option<&str>, &str)> = events
        .iter()
        .map(|(name, data)| (name.as_deref(), data.as_str()))
        .collect();
    for shape in [
        StreamShape::OpenAiChat,
        StreamShape::OpenAiResponses,
        StreamShape::FoundryChat,
        StreamShape::AnthropicTranslated,
        StreamShape::AnthropicNative,
    ] {
        classes.extend(axond_fuzz::provider_stream(&ProviderStreamInput {
            shape,
            events: borrowed.clone(),
        }));
    }
    classes
}

/// Provider-error seeds are upstream failure bodies, replayed under every
/// status in [`SMOKE_STATUSES`] so one body exercises every classification arm.
fn replay_provider_error(data: &[u8]) -> Vec<&'static str> {
    let mut classes = Vec::new();
    if let Ok(input) = axond_fuzz::UpstreamFailure::arbitrary_take_rest(Unstructured::new(data)) {
        classes.extend(axond_fuzz::provider_error(&input));
    }
    let Ok(body) = str::from_utf8(data) else {
        classes.push("not_utf8");
        return classes;
    };
    for status in SMOKE_STATUSES {
        classes.extend(axond_fuzz::provider_error(&axond_fuzz::UpstreamFailure {
            provider: "smoke-provider",
            status: *status,
            body,
        }));
    }
    classes
}

fn replay_config_toml(data: &[u8]) -> Vec<&'static str> {
    vec![axond_fuzz::config_toml(data)]
}

fn replay_credentials_query(data: &[u8]) -> Vec<&'static str> {
    vec![axond_fuzz::credentials_query(data)]
}

fn replay_blob_secret_envelope(data: &[u8]) -> Vec<&'static str> {
    vec![axond_fuzz::blob_secret_envelope(data)]
}

fn replay_blob_secret_crypto(data: &[u8]) -> Vec<&'static str> {
    BlobSecretCryptoInput::arbitrary_take_rest(Unstructured::new(data))
        .map(|input| vec![axond_fuzz::blob_secret_crypto(&input)])
        .unwrap_or_default()
}

fn replay_publication_parsers(data: &[u8]) -> Vec<&'static str> {
    axond_fuzz::publication_parsers(data)
}

const EXPECTED_BLOB_CRYPTO_CLASSES: &[&str] = &[
    "roundtrip",
    "wrong_environment",
    "wrong_namespace",
    "wrong_reference",
    "wrong_version",
    "wrong_purpose",
    "wrapped_mutation",
    "nonce_mutation",
    "ciphertext_mutation",
    "unknown_key",
    "rotation",
    "alias_rejected",
    "invalid_utf8_refused",
    "stored_id_mutation",
];

/// Exact direct outcome of every committed publication seed.
const EXPECTED_PUBLICATION_SEED_CLASSES: &[(&str, &[&str])] = &[
    (
        "cross-environment-head.json",
        &["head_environment_mismatch", "manifest_malformed"],
    ),
    (
        "digest-mismatch-probe.txt",
        &[
            "head_malformed",
            "manifest_malformed",
            "head_guard_accepted",
            "manifest_digest_mismatch",
            "manifest_digest_mismatch",
        ],
    ),
    (
        "fence-changed-probe.txt",
        &[
            "head_malformed",
            "manifest_malformed",
            "head_guard_accepted",
            "manifest_verified",
            "active_head_changed",
        ],
    ),
    (
        "duplicate-objects-manifest.hex",
        &["head_malformed", "manifest_non_canonical_objects"],
    ),
    (
        "invalid-signature-head.json",
        &["head_invalid_signature", "manifest_malformed"],
    ),
    (
        "malformed-head.json",
        &["head_malformed", "manifest_malformed"],
    ),
    (
        "malformed-manifest.hex",
        &["head_malformed", "manifest_malformed"],
    ),
    (
        "overflow-sequence-head.json",
        &["head_malformed", "manifest_malformed"],
    ),
    (
        "oversized-manifest",
        &["head_oversized", "manifest_oversized"],
    ),
    (
        "same-sequence-equivocation-probe.txt",
        &[
            "head_malformed",
            "manifest_malformed",
            "head_equivocation",
            "manifest_verified",
            "head_equivocation",
        ],
    ),
    (
        "signed-orphan-activation-probe.txt",
        &[
            "head_malformed",
            "manifest_malformed",
            "head_guard_accepted",
            "manifest_verified",
            "active_orphan",
        ],
    ),
    (
        "signed-orphan-manifest.hex",
        &["head_malformed", "manifest_accepted"],
    ),
    (
        "tampered-signature-manifest.hex",
        &["head_malformed", "manifest_invalid_signature"],
    ),
    (
        "too-many-objects-manifest.hex",
        &["head_malformed", "manifest_too_many_objects"],
    ),
    (
        "unknown-algorithm-head.json",
        &["head_unknown_algorithm", "manifest_malformed"],
    ),
    (
        "unknown-algorithm-manifest.hex",
        &["head_malformed", "manifest_unknown_algorithm"],
    ),
    (
        "unknown-key-head.json",
        &["head_unknown_key", "manifest_malformed"],
    ),
    (
        "unknown-schema-head.json",
        &["head_unknown_schema", "manifest_malformed"],
    ),
    (
        "unknown-schema-manifest.hex",
        &["head_malformed", "manifest_unknown_schema"],
    ),
    (
        "unknown-signature-schema-head.json",
        &["head_unknown_signature_schema", "manifest_malformed"],
    ),
    (
        "unknown-signature-schema-manifest.hex",
        &["head_malformed", "manifest_unknown_signature_schema"],
    ),
    (
        "unsigned-head-v2.json",
        &["head_unsigned", "manifest_malformed"],
    ),
    (
        "unsigned-manifest.hex",
        &["head_malformed", "manifest_unsigned"],
    ),
    (
        "valid-active-probe.txt",
        &[
            "head_malformed",
            "manifest_malformed",
            "head_guard_accepted",
            "manifest_verified",
            "active_verified",
        ],
    ),
    ("valid-head.json", &["head_accepted", "manifest_malformed"]),
    (
        "valid-manifest.hex",
        &["head_malformed", "manifest_accepted"],
    ),
    (
        "zero-sequence-manifest.hex",
        &["head_malformed", "manifest_zero_sequence"],
    ),
];

const EXPECTED_BLOB_ENVELOPE_SEEDS: &[(&str, &str)] = &[
    ("accepted.cbor", "accepted"),
    ("ciphertext.cbor", "ciphertext"),
    ("compatibility.cbor", "compatibility"),
    ("fixed-field.cbor", "fixed_field"),
    ("kek-id.cbor", "kek_id"),
    ("noncanonical.cbor", "noncanonical"),
    ("oversized.cbor", "oversized"),
    ("shape.cbor", "shape"),
    ("trailing.cbor", "trailing"),
    ("truncated.cbor", "truncated"),
];

const EXPECTED_BLOB_CRYPTO_SEEDS: &[(&str, &str)] = &[
    ("00-roundtrip.bin", "roundtrip"),
    ("01-wrong-environment.bin", "wrong_environment"),
    ("02-wrong-namespace.bin", "wrong_namespace"),
    ("03-wrong-reference.bin", "wrong_reference"),
    ("04-wrong-version.bin", "wrong_version"),
    ("05-wrong-purpose.bin", "wrong_purpose"),
    ("06-wrapped-mutation.bin", "wrapped_mutation"),
    ("07-nonce-mutation.bin", "nonce_mutation"),
    ("08-ciphertext-mutation.bin", "ciphertext_mutation"),
    ("09-unknown-key.bin", "unknown_key"),
    ("10-rotation.bin", "rotation"),
    ("11-alias-rejected.bin", "alias_rejected"),
    ("12-invalid-utf8-refused.bin", "invalid_utf8_refused"),
    ("13-stored-id-mutation.bin", "stored_id_mutation"),
    ("boundary-empty.bin", "empty_refused"),
    ("boundary-input-not-utf8.bin", "input_not_utf8"),
    ("boundary-multibyte-limit.bin", "roundtrip"),
    ("boundary-multibyte-over-limit.bin", "oversized_refused"),
];

fn assert_exact_seed_outcomes(
    target: &str,
    expected: &[(&str, &str)],
    replay: fn(&[u8]) -> Vec<&'static str>,
) {
    let corpus = seeds(target);
    assert_eq!(
        corpus.len(),
        expected.len(),
        "{target}: every raw seed must have exactly one filename pin"
    );
    for (filename, bytes) in corpus {
        let (_, expected_class) = expected
            .iter()
            .find(|(name, _)| *name == filename)
            .unwrap_or_else(|| panic!("{target}/{filename} has no exact outcome pin"));
        let classes = replay(&bytes);
        assert_eq!(
            classes.as_slice(),
            &[*expected_class],
            "{target}/{filename} no longer reaches its exact named outcome"
        );
    }
    for (filename, _) in expected {
        assert!(
            seed_directory(target).join(filename).is_file(),
            "{target}/{filename} is pinned but absent"
        );
    }
}

fn assert_publication_seed_outcome(seed: &str, bytes: &[u8]) {
    let expected = EXPECTED_PUBLICATION_SEED_CLASSES
        .iter()
        .find(|(name, _)| *name == seed)
        .unwrap_or_else(|| panic!("publication seed {seed} has no explicit expected outcome"))
        .1;
    let actual = axond_fuzz::publication_parsers(bytes);
    assert_eq!(
        actual.as_slice(),
        expected,
        "publication seed {seed} no longer reaches its pinned verification outcome"
    );
}

/// The token target takes a structured input, so a seed file is replayed twice:
/// the way libFuzzer replays it, decoded through `Arbitrary` — which reaches the
/// freshly-minted claims a committed seed cannot carry, because a seed's `exp`
/// is in the past the day after it is written — and as a presented credential,
/// so a seed file stays a readable token even though the corpus is bytes.
fn replay_token_verify(data: &[u8]) -> Vec<&'static str> {
    let mut classes = Vec::new();
    if let Ok(input) = TokenInput::arbitrary_take_rest(Unstructured::new(data)) {
        classes.push(axond_fuzz::token_verify(&input));
    }
    match str::from_utf8(data) {
        Ok(text) => classes.push(axond_fuzz::token_verify(&TokenInput::Presented(text))),
        Err(_) => classes.push("not_utf8"),
    }
    classes
}

/// The catalogue target takes a structured input, so a seed file is replayed
/// twice: decoded through `Arbitrary`, the way libFuzzer replays it, and as a
/// payload, so a seed file stays a readable catalogue document rather than an
/// encoding of one.
fn replay_catalog_import(data: &[u8]) -> Vec<&'static str> {
    let mut classes = Vec::new();
    if let Ok(input) = CatalogInput::arbitrary_take_rest(Unstructured::new(data)) {
        classes.push(axond_fuzz::catalog_import(&input));
    }
    classes.push(axond_fuzz::catalog_import(&CatalogInput::Payload {
        bytes: data,
        etag: None,
    }));
    classes
}

/// Edits of the bundled seed, applied at replay time.
///
/// The committed corpus is documents, which reaches decoding, the schema, and
/// normalization; it cannot reach the *semantic* classification, because that
/// needs two catalogues that differ in one stated way. These are that second
/// catalogue: one edit each, pinned below to the class it must be understood as.
fn catalog_scenarios() -> Vec<(&'static str, CatalogInput<'static>)> {
    let edited = |edit| CatalogInput::Edited {
        edit,
        // A rotation and pretty-printing on every scenario, so each semantic
        // assertion is also an assertion that key order and whitespace did not
        // reach the content identity.
        rotate: 3,
        pretty: true,
    };
    vec![
        ("reordered-and-reprinted", edited(CatalogEdit::None)),
        // These are acceptance-critical refusal paths, so pin them as named
        // scenarios rather than relying only on corpus discovery. Each still
        // runs through the in-memory fetch, strict parse, and last-known-good
        // admission checks in `catalog_import`.
        (
            "empty-catalogue",
            CatalogInput::Payload {
                bytes: include_bytes!("../../seeds/catalog_import/drift-empty.json"),
                etag: None,
            },
        ),
        (
            "provider-less-catalogue",
            CatalogInput::Payload {
                bytes: include_bytes!("../../seeds/catalog_import/drift-missing-providers.json"),
                etag: None,
            },
        ),
        (
            "empty-provider-section",
            CatalogInput::Payload {
                bytes: include_bytes!("../../seeds/catalog_import/drift-providers-empty.json"),
                etag: None,
            },
        ),
        (
            "malformed-catalogue",
            CatalogInput::Payload {
                bytes: include_bytes!("../../seeds/catalog_import/drift-not-json.json"),
                etag: None,
            },
        ),
        (
            "unknown-field",
            edited(CatalogEdit::Unknown {
                provider: 0,
                model: 0,
                key: "speculative",
                value: "a field the schema does not define",
            }),
        ),
        (
            "price-only",
            edited(CatalogEdit::Cost {
                provider: 0,
                model: 0,
                field: CostField::Input,
                value: 4.25,
            }),
        ),
        (
            "metadata-only",
            edited(CatalogEdit::Metadata {
                provider: 0,
                model: 0,
                field: MetaField::Name,
                value: "Renamed by the smoke",
            }),
        ),
        (
            "capability-only",
            edited(CatalogEdit::Capability {
                provider: 0,
                model: 0,
                field: CapabilityField::ToolCall,
                value: false,
            }),
        ),
        (
            "lifecycle-only",
            edited(CatalogEdit::Lifecycle {
                provider: 0,
                model: 0,
                status: LifecycleValue::Deprecated,
            }),
        ),
        (
            "lifecycle-unknown-status",
            edited(CatalogEdit::Lifecycle {
                provider: 0,
                model: 0,
                status: LifecycleValue::Unknown(7),
            }),
        ),
        (
            "neutral-record-only",
            edited(CatalogEdit::Neutral {
                model: 0,
                field: MetaField::Family,
                value: "regenerated-family",
            }),
        ),
        (
            "spliced-garbage",
            edited(CatalogEdit::Splice {
                at: 0,
                bytes: b"\x00not json",
            }),
        ),
    ]
}

/// The class each catalogue scenario exists to land in.
///
/// These are the acceptance criteria of issue #222 written as pins: a price
/// change understood as metadata, or a metadata change understood as a price,
/// would still satisfy a class count and is exactly the confusion a spend
/// decision cannot survive.
const EXPECTED_CATALOG_CLASSES: &[(&str, &str)] = &[
    // Key order and whitespace are not content: the same catalogue, re-rendered,
    // is not an update.
    ("reordered-and-reprinted", "rendered"),
    // Empty and provider-less documents are not usable catalogues, and malformed
    // bytes must preserve the last-known-good snapshot through the offline path.
    ("empty-catalogue", "content"),
    ("provider-less-catalogue", "schema"),
    ("empty-provider-section", "content"),
    ("malformed-catalogue", "not_json"),
    // Additive drift is tolerated rather than refused, and adds nothing.
    ("unknown-field", "unknown_field_ignored"),
    ("price-only", "price_changed"),
    ("metadata-only", "metadata_changed"),
    ("capability-only", "capability_changed"),
    ("lifecycle-only", "lifecycle_changed"),
    // Drift in the *meaning* of a field is refused, not folded onto a default.
    ("lifecycle-unknown-status", "unknown_status"),
    ("neutral-record-only", "neutral_changed"),
    ("spliced-garbage", "not_json"),
];

/// Claim scenarios minted at replay time, with the seam's own HS256 material.
///
/// A committed seed cannot cover these: its `exp` is in the past by the time it
/// is replayed, so every one of them stops at the expiry check. Minting here
/// instead is what keeps the checks *behind* expiry — audience, lifetime,
/// namespace, signer authority, scope, subject — exercised on every pull
/// request, including the invariant that matters most: an HS256 signature over a
/// namespace its `kid` does not hold must never verify.
fn minted_scenarios() -> Vec<(&'static str, TokenInput<'static>)> {
    let minted =
        |namespace, subject, audience, ttl_seconds, issued_at, scope, aliases| TokenInput::Minted {
            namespace,
            subject,
            audience,
            ttl_seconds,
            issued_at,
            scope,
            aliases,
        };
    let in_namespace = axond_fuzz_seam::NAMESPACES[0];
    let other_namespace = axond_fuzz_seam::NAMESPACES[1];
    vec![
        (
            // No `scope` claim at all: what a plain `axond mint` issues, and
            // unrestricted rather than empty.
            "unscoped",
            minted(in_namespace, "smoke", None, 300, None, None, None),
        ),
        (
            // `"scope": []` instead, which permits nothing. Confusing the two is
            // the bug worth catching, so both are replayed.
            "empty-scope",
            minted(
                in_namespace,
                "smoke",
                None,
                300,
                None,
                Some(Vec::new()),
                None,
            ),
        ),
        (
            "every-capability-and-one-unknown",
            minted(
                in_namespace,
                "smoke",
                None,
                300,
                None,
                Some(vec![
                    "chat",
                    "messages",
                    "embeddings",
                    "responses",
                    "models",
                    "credentials",
                    "credentials:all",
                    "not-a-capability",
                ]),
                None,
            ),
        ),
        (
            "alias-scoped",
            minted(
                in_namespace,
                "smoke",
                None,
                300,
                None,
                None,
                Some(vec!["gpt-4o", ""]),
            ),
        ),
        (
            "namespace-the-signer-does-not-hold",
            minted(other_namespace, "smoke", None, 300, None, None, None),
        ),
        (
            "undeclared-namespace",
            minted("not-configured", "smoke", None, 300, None, None, None),
        ),
        (
            "foreign-audience",
            minted(
                in_namespace,
                "smoke",
                Some("someone-elses-gateway"),
                300,
                None,
                None,
                None,
            ),
        ),
        (
            "lifetime-past-the-policy-ceiling",
            minted(in_namespace, "smoke", None, 86_400 * 7, None, None, None),
        ),
        (
            "issued-far-in-the-future",
            minted(
                in_namespace,
                "smoke",
                None,
                300,
                Some(u64::MAX / 2),
                None,
                None,
            ),
        ),
        (
            "empty-subject",
            minted(in_namespace, "", None, 300, None, None, None),
        ),
        (
            // The issuance epoch is the last check `resolve` runs, so reaching it
            // needs a token that is old enough to precede the epoch and still
            // live: `iat` a minute below it, `exp` a full permitted lifetime
            // later.
            "issued-before-the-namespace-epoch",
            minted(
                in_namespace,
                "smoke",
                None,
                axond_fuzz_seam::MAX_TTL_SECONDS,
                Some(axond_fuzz_seam::epoch_min_iat() - 60),
                None,
                None,
            ),
        ),
        (
            // The other side of the same epoch, so the check is shown to accept
            // as well as refuse.
            "issued-just-after-the-namespace-epoch",
            minted(
                in_namespace,
                "smoke",
                None,
                axond_fuzz_seam::MAX_TTL_SECONDS,
                Some(axond_fuzz_seam::epoch_min_iat() + 1),
                None,
                None,
            ),
        ),
    ]
}

/// The outcome each committed token seed is named for, asserted after the seed
/// is re-signed onto the current run.
///
/// A committed token expires as soon as the date passes its `exp`, so replaying
/// the bytes alone lands all of these on the expiry check and the checks behind
/// it go unexercised — the coverage would silently decay with the calendar
/// rather than with a code change. Re-signing translates the timestamps instead
/// of replacing them, so the relationships each seed encodes (a lifetime past the
/// ceiling, an `exp` before its `iat`) survive.
const EXPECTED_RESIGNED_SEED_CLASSES: &[(&str, &str)] = &[
    ("hs256-well-formed.txt", "accepted"),
    ("hs256-scope-array.txt", "accepted"),
    ("hs256-aliases-list.txt", "accepted"),
    // A space-delimited `scope` string is as valid as the array form, so this
    // seed proves both spellings resolve rather than that one is refused.
    ("hs256-scope-string.txt", "accepted"),
    ("hs256-aliases-null.txt", "token_alias_claim_invalid"),
    ("hs256-aliases-wrong-type.txt", "token_alias_claim_invalid"),
    ("hs256-missing-jti.txt", "token_missing_claim"),
    ("hs256-empty-subject.txt", "token_missing_claim"),
    // `exp` before `iat` is refused as expired, not as an invalid lifetime:
    // decoding validates `exp` against the clock before `resolve` compares the
    // two claims, and an `exp` behind a translated `iat` is behind now as well.
    // The `exp < iat` arm is only reachable inside the five-second skew window,
    // which the coverage-guided lane can hit and a committed seed cannot.
    ("hs256-exp-before-iat.txt", "token_expired"),
    ("hs256-lifetime-too-long.txt", "token_invalid_lifetime"),
    ("hs256-unknown-namespace.txt", "token_unknown_namespace"),
    ("hs256-denied-namespace.txt", "token_signer_not_permitted"),
    ("hs256-wrong-audience.txt", "token_wrong_audience"),
];

/// Scenarios whose whole purpose is the class they land in: a check that stopped
/// being reachable would otherwise still satisfy the class-count threshold.
const EXPECTED_MINTED_CLASSES: &[(&str, &str)] = &[
    ("unscoped", "accepted"),
    ("empty-scope", "accepted"),
    // The unknown capability is the verifier's to discard, not the seam's.
    ("every-capability-and-one-unknown", "accepted"),
    (
        "namespace-the-signer-does-not-hold",
        "token_signer_not_permitted",
    ),
    ("undeclared-namespace", "token_unknown_namespace"),
    ("foreign-audience", "token_wrong_audience"),
    ("lifetime-past-the-policy-ceiling", "token_invalid_lifetime"),
    ("empty-subject", "token_missing_claim"),
    // An empty alias is not a name, so the whole claim is refused rather than
    // partially honoured.
    ("alias-scoped", "token_alias_claim_invalid"),
    // `iat` is bounded from above as well as below: the lifetime check refuses an
    // `iat` more than the clock skew ahead of now, so a token stamped billions of
    // years out is refused even though its `exp - iat` is a legal 300 seconds.
    // Pinned so that upper bound cannot quietly disappear.
    ("issued-far-in-the-future", "token_invalid_lifetime"),
    (
        "issued-before-the-namespace-epoch",
        "token_issued_before_epoch",
    ),
    ("issued-just-after-the-namespace-epoch", "accepted"),
];

/// Re-sign one token seed onto this run and check it against its pin.
///
/// `None` for a seed that is not a signable JWS — but only if it carries no pin:
/// skipping is how the corpus keeps its decode-path inputs, and skipping a
/// *pinned* seed would retire the check it stands for in silence.
fn resigned_seed_class(seed: &str, bytes: &[u8], asserted: &mut usize) -> Option<&'static str> {
    let pinned = EXPECTED_RESIGNED_SEED_CLASSES
        .iter()
        .find(|(name, _)| *name == seed)
        .map(|(_, expected)| *expected);
    let text = match str::from_utf8(bytes) {
        Ok(text) => text,
        Err(error) => {
            assert!(
                pinned.is_none(),
                "{seed} is pinned to an outcome but is no longer utf-8: {error}"
            );
            return None;
        }
    };
    let class = match axond_fuzz::token_verify_resigned_seed(text) {
        Some(class) => class,
        None => {
            assert!(
                pinned.is_none(),
                "{seed} is pinned to an outcome but can no longer be re-signed, so its check \
                 would go unexercised"
            );
            return None;
        }
    };
    if let Some(expected) = pinned {
        *asserted += 1;
        assert_eq!(
            class, expected,
            "re-signed seed {seed} reached {class} rather than {expected}, so the check it is \
             named for is no longer the one it lands on"
        );
    }
    Some(class)
}

/// Prove [`resigned_seed_class`] refuses to skip a pinned seed.
///
/// The guard's whole value is that it fails instead of continuing, which nothing
/// in a passing run demonstrates: the corpus is signable, so the arm never runs.
/// Feeding it a pinned name with unsignable bytes is the only evidence that the
/// arm is still wired to a failure.
fn assert_pinning_guard_fires() {
    let (pinned, _) = EXPECTED_RESIGNED_SEED_CLASSES
        .first()
        .expect("at least one seed is pinned to an outcome");
    let previous = std::panic::take_hook();
    // The panic this expects is the pass condition, so keep it off the output.
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = std::panic::catch_unwind(|| {
        resigned_seed_class(pinned, b"axt1.not-a-jws", &mut 0);
    });
    std::panic::set_hook(previous);
    assert!(
        outcome.is_err(),
        "an unsignable {pinned} was skipped rather than failing the run"
    );
}

fn seed_directory(target: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("seeds")
        .join(target)
}

fn seeds(target: &str) -> Vec<(String, Vec<u8>)> {
    let directory = seed_directory(target);
    let mut entries: Vec<_> = std::fs::read_dir(&directory)
        .unwrap_or_else(|error| {
            panic!("seed corpus {} is unreadable: {error}", directory.display())
        })
        .map(|entry| entry.expect("seed directory entry").path())
        .filter(|path| path.is_file())
        .collect();
    // Sorted, so the run order is the repository's, not the filesystem's.
    entries.sort();
    assert!(
        !entries.is_empty(),
        "seed corpus {} is empty",
        directory.display()
    );
    entries
        .into_iter()
        .map(|path| {
            let bytes = std::fs::read(&path).expect("seed file is readable");
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .expect("seed file name is utf-8")
                .to_owned();
            (name, bytes)
        })
        .collect()
}

/// The fixed derivations of a seed: prefixes, single-byte flips, and one
/// oversized repetition. All computed from the seed, so nothing here is random.
fn derivations(seed: &[u8]) -> Vec<(String, Vec<u8>)> {
    let mut derived = Vec::new();
    if seed.is_empty() {
        return derived;
    }
    for eighth in 1..8 {
        let cut = seed.len() * eighth / 8;
        if cut > 0 && cut < seed.len() {
            derived.push((format!("truncated:{cut}"), seed[..cut].to_vec()));
        }
    }
    for step in 0..4 {
        let index = (step * 7 + 1) % seed.len();
        let mut flipped = seed.to_vec();
        flipped[index] ^= 0x80;
        derived.push((format!("flipped:{index}"), flipped));
    }
    let repeats = OVERSIZED_BYTES.div_ceil(seed.len());
    derived.push((
        format!("oversized:{OVERSIZED_BYTES}"),
        seed.repeat(repeats)[..OVERSIZED_BYTES.min(seed.len() * repeats)].to_vec(),
    ));
    derived
}

fn main() {
    let started = Instant::now();
    // Before anything asserts on a refusal, prove the verifier refuses for a
    // reason: a stubbed signature check would make every token assertion below
    // vacuous.
    axond_fuzz::assert_signature_verification_is_real();
    println!("token_verify: signature verification is live (minted accepted, tampered refused)");
    // Likewise for the stream targets: their properties are relative, so a
    // decoder that returned nothing would satisfy them. The pinned fixtures are
    // what proves a valid stream still decodes to the events it must, under
    // every boundary it can be split on.
    axond_fuzz::assert_valid_fixtures_are_stable();
    println!("sse_decode: valid fixtures decode identically under every chunk boundary");
    // And the leakage oracle itself: a canary spelled with JSON escapes is the
    // input carrying it, not a decoder disclosing it.
    axond_fuzz::assert_disclosure_check_survives_escaping();
    println!("provider_error: an escaped canary is read as the input that carried it");
    assert_exact_seed_outcomes(
        "blob_secret_envelope",
        EXPECTED_BLOB_ENVELOPE_SEEDS,
        replay_blob_secret_envelope,
    );
    assert_exact_seed_outcomes(
        "blob_secret_crypto",
        EXPECTED_BLOB_CRYPTO_SEEDS,
        replay_blob_secret_crypto,
    );
    let publication_corpus = seeds("publication_parsers");
    for (seed, bytes) in &publication_corpus {
        assert_publication_seed_outcome(seed, bytes);
    }
    for (seed, _) in EXPECTED_PUBLICATION_SEED_CLASSES {
        assert!(
            publication_corpus.iter().any(|(name, _)| name == seed),
            "publication outcome pin {seed} names no committed seed"
        );
    }
    assert_eq!(
        publication_corpus.len(),
        EXPECTED_PUBLICATION_SEED_CLASSES.len(),
        "publication seeds and explicit outcome pins must remain one-to-one"
    );
    println!(
        "blob secret and publication corpora: every filename reaches its exact pinned outcome"
    );
    let mut inputs = 0_usize;
    for target in TARGETS {
        let mut target_inputs = 0_usize;
        let mut classes: BTreeMap<&'static str, usize> = BTreeMap::new();
        for (seed, bytes) in seeds(target.name) {
            for (label, input) in
                std::iter::once(("seed".to_owned(), bytes.clone())).chain(derivations(&bytes))
            {
                let input_started = Instant::now();
                for class in (target.run)(&input) {
                    *classes.entry(class).or_default() += 1;
                }
                let elapsed = input_started.elapsed();
                assert!(
                    elapsed < PER_INPUT_BUDGET,
                    "{}/{seed} [{label}] took {elapsed:?}, over the {PER_INPUT_BUDGET:?} budget",
                    target.name
                );
                target_inputs += 1;
            }
        }
        inputs += target_inputs;
        let reached = classes
            .iter()
            .map(|(class, count)| format!("{class}={count}"))
            .collect::<Vec<_>>()
            .join(" ");
        assert!(
            classes.len() >= target.minimum_classes,
            "{}: seeds reached {} outcome classes, fewer than the {} required ({reached})",
            target.name,
            classes.len(),
            target.minimum_classes
        );
        println!(
            "{}: {target_inputs} inputs replayed, {} outcome classes: {reached}",
            target.name,
            classes.len()
        );
    }
    // The guard below refuses to skip a pinned seed. Prove it fires before
    // trusting it, the same way `ops/check-docs.py --self-test` does.
    assert_pinning_guard_fires();
    let mut resigned_classes: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut resigned_seeds_asserted = 0_usize;
    for (seed, bytes) in seeds("token_verify") {
        let input_started = Instant::now();
        let Some(class) = resigned_seed_class(&seed, &bytes, &mut resigned_seeds_asserted) else {
            continue;
        };
        *resigned_classes.entry(class).or_default() += 1;
        let elapsed = input_started.elapsed();
        assert!(
            elapsed < PER_INPUT_BUDGET,
            "re-signed seed {seed} took {elapsed:?}, over the {PER_INPUT_BUDGET:?} budget"
        );
        inputs += 1;
    }
    for (seed, _) in EXPECTED_RESIGNED_SEED_CLASSES {
        assert!(
            seed_directory("token_verify").join(seed).is_file(),
            "{seed} is pinned to an outcome but no longer exists in the corpus"
        );
    }
    assert_eq!(
        resigned_seeds_asserted,
        EXPECTED_RESIGNED_SEED_CLASSES.len(),
        "only {resigned_seeds_asserted} of the {} pinned seeds were asserted",
        EXPECTED_RESIGNED_SEED_CLASSES.len()
    );
    assert!(
        resigned_classes.len() >= MINIMUM_RESIGNED_CLASSES,
        "re-signed seeds reached {} outcome classes, fewer than the {MINIMUM_RESIGNED_CLASSES} \
         required",
        resigned_classes.len()
    );
    println!(
        "token_verify (seeds re-signed onto this run): {} seeds, {} outcome classes: {}",
        resigned_classes.values().sum::<usize>(),
        resigned_classes.len(),
        resigned_classes
            .iter()
            .map(|(class, count)| format!("{class}={count}"))
            .collect::<Vec<_>>()
            .join(" ")
    );

    let mut minted_classes: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut minted_scenarios_asserted = 0_usize;
    for (label, input) in minted_scenarios() {
        let input_started = Instant::now();
        let class = axond_fuzz::token_verify(&input);
        if let Some((_, expected)) = EXPECTED_MINTED_CLASSES
            .iter()
            .find(|(scenario, _)| *scenario == label)
        {
            minted_scenarios_asserted += 1;
            assert_eq!(
                class, *expected,
                "minted scenario {label} reached {class} rather than {expected}, so the check it \
                 exists for is no longer the one it lands on"
            );
        }
        *minted_classes.entry(class).or_default() += 1;
        let elapsed = input_started.elapsed();
        assert!(
            elapsed < PER_INPUT_BUDGET,
            "minted scenario {label} took {elapsed:?}, over the {PER_INPUT_BUDGET:?} budget"
        );
        inputs += 1;
    }
    // A pin nothing looks up is a check nobody runs, and the class count below
    // can be satisfied by a different scenario, so renaming one has to fail here.
    assert_eq!(
        minted_scenarios_asserted,
        EXPECTED_MINTED_CLASSES.len(),
        "only {minted_scenarios_asserted} of the {} pinned minted scenarios were asserted; a \
         pinned label no longer appears in `minted_scenarios`",
        EXPECTED_MINTED_CLASSES.len()
    );
    assert!(
        minted_classes.len() >= MINIMUM_MINTED_CLASSES,
        "minted scenarios reached {} outcome classes, fewer than the {MINIMUM_MINTED_CLASSES} required",
        minted_classes.len()
    );
    println!(
        "token_verify (minted at replay time): {} scenarios, {} outcome classes: {}",
        minted_classes.values().sum::<usize>(),
        minted_classes.len(),
        minted_classes
            .iter()
            .map(|(class, count)| format!("{class}={count}"))
            .collect::<Vec<_>>()
            .join(" ")
    );

    let mut catalog_classes: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut catalog_scenarios_asserted = 0_usize;
    for (label, input) in catalog_scenarios() {
        let input_started = Instant::now();
        let class = axond_fuzz::catalog_import(&input);
        if let Some((_, expected)) = EXPECTED_CATALOG_CLASSES
            .iter()
            .find(|(scenario, _)| *scenario == label)
        {
            catalog_scenarios_asserted += 1;
            assert_eq!(
                class, *expected,
                "catalogue scenario {label} was understood as {class} rather than {expected}"
            );
        }
        *catalog_classes.entry(class).or_default() += 1;
        let elapsed = input_started.elapsed();
        assert!(
            elapsed < PER_INPUT_BUDGET,
            "catalogue scenario {label} took {elapsed:?}, over the {PER_INPUT_BUDGET:?} budget"
        );
        inputs += 1;
    }
    assert_eq!(
        catalog_scenarios_asserted,
        EXPECTED_CATALOG_CLASSES.len(),
        "only {catalog_scenarios_asserted} of the {} pinned catalogue scenarios were asserted; a \
         pinned label no longer appears in `catalog_scenarios`",
        EXPECTED_CATALOG_CLASSES.len()
    );
    assert!(
        catalog_classes.len() >= MINIMUM_CATALOG_EDIT_CLASSES,
        "catalogue scenarios reached {} outcome classes, fewer than the \
         {MINIMUM_CATALOG_EDIT_CLASSES} required",
        catalog_classes.len()
    );
    println!(
        "catalog_import (seed edited at replay time): {} scenarios, {} outcome classes: {}",
        catalog_classes.values().sum::<usize>(),
        catalog_classes.len(),
        catalog_classes
            .iter()
            .map(|(class, count)| format!("{class}={count}"))
            .collect::<Vec<_>>()
            .join(" ")
    );

    let mut crypto_classes = BTreeMap::new();
    for (scenario, expected) in EXPECTED_BLOB_CRYPTO_CLASSES.iter().enumerate() {
        let input = BlobSecretCryptoInput {
            material: b"synthetic-provider-key",
            scenario: u8::try_from(scenario).expect("bounded scenario"),
            primary_seed: 0x11,
            secondary_seed: 0x22,
            identity_seed: 7,
            version_seed: 3,
        };
        let class = axond_fuzz::blob_secret_crypto(&input);
        assert_eq!(class, *expected, "blob crypto scenario {scenario}");
        *crypto_classes.entry(class).or_default() += 1;
        inputs += 1;
    }
    for (label, material, expected) in [
        ("empty", Vec::new(), "empty_refused"),
        (
            "multibyte-limit",
            "é".repeat(axond_fuzz_seam::BLOB_SECRET_MAX_PLAINTEXT_BYTES / 2)
                .into_bytes(),
            "roundtrip",
        ),
        (
            "multibyte-over-limit",
            format!(
                "{}x",
                "é".repeat(axond_fuzz_seam::BLOB_SECRET_MAX_PLAINTEXT_BYTES / 2)
            )
            .into_bytes(),
            "oversized_refused",
        ),
    ] {
        let input = BlobSecretCryptoInput {
            material: &material,
            scenario: 0,
            primary_seed: 0x33,
            secondary_seed: 0x44,
            identity_seed: 9,
            version_seed: 1,
        };
        let class = axond_fuzz::blob_secret_crypto(&input);
        assert_eq!(class, expected, "blob crypto boundary {label}");
        *crypto_classes.entry(class).or_default() += 1;
        inputs += 1;
    }
    println!(
        "blob_secret_crypto (pinned scenarios): {} scenarios, {} outcome classes: {}",
        crypto_classes.values().sum::<usize>(),
        crypto_classes.len(),
        crypto_classes
            .iter()
            .map(|(class, count)| format!("{class}={count}"))
            .collect::<Vec<_>>()
            .join(" ")
    );

    let elapsed = started.elapsed();
    assert!(
        elapsed < TOTAL_BUDGET,
        "the replay took {elapsed:?}, over the {TOTAL_BUDGET:?} budget"
    );
    println!(
        "fuzz smoke passed: {inputs} inputs in {elapsed:?}, peak live heap {} KiB of the {} KiB cap",
        PEAK_BYTES.load(Ordering::Relaxed) / 1024,
        ALLOCATION_CAP / 1024
    );
}
