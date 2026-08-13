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
/// [`EXPECTED_MINTED_CLASSES`] pins which ones; this is the floor for the rest.
const MINIMUM_MINTED_CLASSES: usize = 8;

/// How many outcome classes the re-signed token seeds must reach.
const MINIMUM_RESIGNED_CLASSES: usize = 10;

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
