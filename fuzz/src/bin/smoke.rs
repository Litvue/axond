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
use axond_fuzz::TokenInput;

/// Live heap the whole replay may hold at once. The parsers under test are
/// bounded by their input, which is why this is generous in absolute terms and
/// still tiny next to what an unbounded pre-allocation would ask for.
const ALLOCATION_CAP: usize = 512 * 1024 * 1024;

/// A single input that takes longer than this is reported as a hang.
const PER_INPUT_BUDGET: Duration = Duration::from_secs(2);

/// The whole replay is a pull-request lane, so it stays inside a minute.
const TOTAL_BUDGET: Duration = Duration::from_secs(60);

/// How large the oversized derivation of each seed is.
const OVERSIZED_BYTES: usize = 64 * 1024;

/// How many outcome classes the freshly-minted token scenarios must reach.
const MINIMUM_MINTED_CLASSES: usize = 5;

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
        unsafe { System.alloc(layout) }
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
        name: "token_verify",
        run: replay_token_verify,
        minimum_classes: 8,
    },
];

fn replay_config_toml(data: &[u8]) -> Vec<&'static str> {
    vec![axond_fuzz::config_toml(data)]
}

fn replay_credentials_query(data: &[u8]) -> Vec<&'static str> {
    vec![axond_fuzz::credentials_query(data)]
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
    let in_namespace = axond::NAMESPACES[0];
    let other_namespace = axond::NAMESPACES[1];
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
    ]
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
    let mut minted_classes: BTreeMap<&'static str, usize> = BTreeMap::new();
    for (label, input) in minted_scenarios() {
        let input_started = Instant::now();
        *minted_classes
            .entry(axond_fuzz::token_verify(&input))
            .or_default() += 1;
        let elapsed = input_started.elapsed();
        assert!(
            elapsed < PER_INPUT_BUDGET,
            "minted scenario {label} took {elapsed:?}, over the {PER_INPUT_BUDGET:?} budget"
        );
        inputs += 1;
    }
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
