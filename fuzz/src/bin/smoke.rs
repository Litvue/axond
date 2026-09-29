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
//! percent-decoding and SSE framing get wrong, and computed from the
//! seed bytes alone, so the run is reproducible from the repository.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arbitrary::{Arbitrary, Unstructured};
use axond_fuzz::{
    CapabilityField, CatalogEdit, CatalogInput, CostField, LifecycleValue, MetaField,
    ProviderStreamInput, SseInput, StreamShape,
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
    // The stream targets' properties are relative, so a
    // decoder that returned nothing would satisfy them. The pinned fixtures are
    // what proves a valid stream still decodes to the events it must, under
    // every boundary it can be split on.
    axond_fuzz::assert_valid_fixtures_are_stable();
    println!("sse_decode: valid fixtures decode identically under every chunk boundary");
    // And the leakage oracle itself: a canary spelled with JSON escapes is the
    // input carrying it, not a decoder disclosing it.
    axond_fuzz::assert_disclosure_check_survives_escaping();
    println!("provider_error: an escaped canary is read as the input that carried it");
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
