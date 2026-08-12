//! Deterministic capacity qualification (ADR 0033).
//!
//! Every profile in `qualification/capacity/manifest.toml` is offered to a real
//! `axond` process talking to the deterministic fake upstream, and each run
//! writes a machine-readable artifact under `target/capacity/` carrying both the
//! measurements and the exact inputs that produced them.
//!
//! What fails here is deliberately narrow. Throughput, latency, and TTFT are
//! recorded and never asserted: a shared CI runner cannot bound them without
//! flaking, and a flaky capacity gate is one that gets disabled. The hard
//! failures are the properties that do not move with the machine — every
//! request accepted, nothing shed, no upstream socket leaked, one usage record
//! per admitted request, and resident memory that does not grow with the load.
//!
//! The reduced tier runs under `cargo test`. The heavy tier is the same code and
//! the same assertions at a scale that needs its own runner, behind
//! `AXOND_CAPACITY=1` and the `capacity` workflow — which runs this binary with
//! `--test-threads=1`, so the two tiers never offer load at the same time and
//! the heavy numbers are not measured against the reduced tier's contention.
//!
//! Nothing here qualifies stateful serving: the profiles run a Tier 0 process.

mod support;

use support::capacity::{self, CapacityResult, Gauges, ResourceReport, Span, Tier, Workload};
use support::upstream;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_reduced_capacity_profiles_qualify_and_publish_their_evidence() {
    qualify(Tier::Reduced).await;
}

/// The heavy tier: the same profiles at a scale that takes minutes. Opt-in,
/// because it is slow and wants a runner to itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_heavy_capacity_profiles_qualify_and_publish_their_evidence() {
    if std::env::var("AXOND_CAPACITY").as_deref() != Ok("1") {
        eprintln!("skipping the heavy capacity profiles; set AXOND_CAPACITY=1 to run them");
        return;
    }
    qualify(Tier::Heavy).await;
}

/// Every workload the driver implements is exercised by the committed manifest,
/// and every profile carries the thresholds that make it a gate. A profile that
/// silently loses its thresholds would still produce an artifact, and the
/// artifact would look like evidence.
#[test]
fn the_committed_manifest_covers_every_workload_with_thresholds() {
    let (manifest, _) = capacity::manifest::load();
    let mut ids: Vec<&str> = manifest.profiles.iter().map(|p| p.id.as_str()).collect();
    ids.sort_unstable();
    let unique = ids.len();
    ids.dedup();
    assert_eq!(unique, ids.len(), "profile ids must be unique: {ids:?}");

    for workload in [
        Workload::Buffered,
        Workload::Streaming,
        Workload::Mixed,
        Workload::ResponseSize,
        Workload::Cancellation,
    ] {
        assert!(
            manifest
                .profiles
                .iter()
                .any(|profile| profile.workload == workload),
            "no committed profile exercises the {} workload",
            workload.as_str()
        );
    }
    for profile in &manifest.profiles {
        for tier in [Tier::Reduced, Tier::Heavy] {
            let scale = profile.scale(tier);
            assert!(
                scale.concurrency > 0 && scale.requests >= scale.concurrency,
                "{} [{}]: a scale must offer at least one request per worker",
                profile.id,
                tier.as_str()
            );
        }
        assert!(
            profile.thresholds.min_accepted_fraction > 0.0,
            "{}: a profile without an acceptance threshold asserts nothing",
            profile.id
        );
        if profile.workload == Workload::Cancellation {
            assert!(
                profile.cancel_every.is_some(),
                "{}: the cancellation workload needs `cancel_every`",
                profile.id
            );
        }
    }
}

async fn qualify(tier: Tier) {
    let (manifest, text) = capacity::manifest::load();
    let mut failures = Vec::new();
    for profile in &manifest.profiles {
        let result = capacity::run(profile, tier, &text).await;
        let path = result.write();
        eprintln!(
            "capacity: {}\n           -> {}",
            result.summary(),
            path.display()
        );
        assert_expected_outcomes(profile, &result);
        for verdict in result.failures() {
            failures.push(format!(
                "{} [{}]: {} {} {} (measured {})",
                profile.id,
                tier.as_str(),
                verdict.threshold,
                verdict.comparison,
                verdict.bound,
                verdict.value
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "capacity thresholds failed:\n{}",
        failures.join("\n")
    );
}

/// The expected hang-up count must agree with the driver's own selection for
/// *any* request count, not only the multiples of the cadence the manifest
/// happens to hold today. Rounding it down turns an odd request count into a red
/// build with a misleading message.
#[test]
fn the_expected_cancellation_count_matches_the_driver_selection() {
    for every in 1..8usize {
        for requests in 0..64usize {
            let selected = (0..requests)
                .filter(|&i| capacity::cancels(i, every))
                .count() as u64;
            assert_eq!(
                capacity::expected_cancellations(requests as u64, every),
                selected,
                "cadence {every} over {requests} requests"
            );
        }
    }
    assert_eq!(capacity::expected_cancellations(49, 2), 25);
}

/// A stream is waiting for its answer until the first *relayed* byte, not until
/// its response headers: the gateway answers a stream's headers as soon as the
/// upstream does, and the first token can be far behind them. Releasing at
/// headers would report an occupancy the replica never had.
#[test]
fn a_stream_waits_in_the_first_byte_gauge_until_its_first_relayed_chunk() {
    let gauges = Gauges::default();
    let (headers, chunk) = (&mut true, &mut true);
    gauges.enter();
    gauges.enter();
    assert_eq!(gauges.awaiting(), 2, "both requests are waiting");

    // One gets a first byte; the other only ever got headers.
    gauges.first_byte(chunk);
    assert_eq!(gauges.awaiting(), 1);
    assert_eq!(gauges.awaiting_peak(), 2);

    // An attempt that never sees a first byte still releases when it ends, so a
    // torn stream cannot pin the gauge for the rest of the run.
    gauges.leave(headers);
    gauges.leave(chunk);
    assert_eq!(gauges.awaiting(), 0);
}

/// Output events are counted from the bytes the fake upstream really emits, over
/// both wire families, however the transport frames them. Substring counting
/// would miscount both ways — a preamble mentions `content` without relaying
/// any, and an Anthropic delta names its type twice — and counting transport
/// reads instead of events would let a starved reader miss its hang-up and fail
/// the cadence assertion with nothing wrong.
#[test]
fn output_events_are_counted_however_the_transport_frames_them() {
    for anthropic in [false, true] {
        let wire: String = upstream::slow_events(anthropic, 5)
            .iter()
            .map(|chunk| String::from_utf8(chunk.to_vec()).expect("UTF-8 events"))
            .collect();
        assert_eq!(
            capacity::output_events(&wire),
            5,
            "anthropic={anthropic}: only the five deltas relay output:\n{wire}"
        );

        // A preamble relays nothing, so a cancellation cannot fire on it.
        let (preamble, _) = wire.split_at(wire.find("tok0").expect("a first token"));
        assert_eq!(
            capacity::output_events(preamble),
            0,
            "anthropic={anthropic}: nothing is relayed before the first token"
        );

        // Event by event: the greeting and the trailers score nothing, and each
        // delta scores exactly one — never two for naming its type twice.
        for event in upstream::slow_events(anthropic, 5) {
            let event = String::from_utf8(event.to_vec()).expect("UTF-8 events");
            let relays_a_token = (0..5).any(|i| event.contains(&format!("tok{i} ")));
            assert_eq!(
                capacity::output_events(&event),
                usize::from(relays_a_token),
                "anthropic={anthropic}: misclassified event:\n{event}"
            );
        }

        // Whole events arriving in one read count as several; a partial event
        // counts once its remainder arrives, which is why the text accumulates.
        let mid = wire.len() / 2;
        let (head, tail) = wire.split_at(mid);
        let progressive = capacity::output_events(head);
        assert!(
            progressive <= 5,
            "anthropic={anthropic}: a partial read cannot overcount"
        );
        assert_eq!(
            capacity::output_events(&format!("{head}{tail}")),
            5,
            "anthropic={anthropic}: an event split across two reads counts once"
        );
    }
}

/// An absent memory measurement means two different things, and they must not
/// look alike: a platform with no `/proc` had no evidence to give, while a
/// `/proc` host that produced none lost its subject mid-run. The second is a
/// failure; treating it as the first makes the memory gate pass vacuously.
#[test]
fn a_lost_resource_sample_fails_rather_than_skipping_the_memory_gate() {
    let report = |procfs: bool, rss: Option<Span>| ResourceReport {
        sampled: rss.is_some(),
        procfs,
        samples: 0,
        rss_kib: rss,
        sockets: None,
        cpu_seconds: None,
        cpu_utilization: None,
        user_hz: 100.0,
    };
    let span = Span {
        baseline: 1_000,
        peak: 1_400,
        settled: 1_100,
    };

    // 400 KiB of growth, against the manifest's 256 MiB bound.
    let bound = 256 * 1024;
    let measured = capacity::memory_verdict(&report(true, Some(span)), bound).expect("a verdict");
    assert_eq!(measured.threshold, "max_rss_growth_kib");
    assert!(measured.passed, "{measured:?}");

    let lost = capacity::memory_verdict(&report(true, None), bound).expect("a verdict");
    assert_eq!(lost.threshold, "resource_sampling");
    assert!(!lost.passed, "a /proc host that measured nothing must fail");

    assert!(
        capacity::memory_verdict(&report(false, None), bound).is_none(),
        "off a /proc platform there is nothing to assert"
    );
}

/// Both tiers live in one test binary, so the harness that runs the heavy tier
/// must run it alone: two tiers offering load at once measure each other's
/// contention, and the artifact still reads as an envelope. The driver holds a
/// lock as well, and this keeps the invocations honest about why.
#[test]
fn every_heavy_invocation_runs_one_test_at_a_time() {
    for (path, contents) in [
        (
            ".github/workflows/capacity.yml",
            include_str!("../../../.github/workflows/capacity.yml"),
        ),
        ("justfile", include_str!("../../../justfile")),
    ] {
        let lines: Vec<&str> = contents.lines().collect();
        let at = lines
            .iter()
            .position(|line| line.contains("--test capacity"))
            .unwrap_or_else(|| panic!("{path}: no capacity invocation"));
        // The invocation may be wrapped across a line continuation.
        let invocation = lines[at..(at + 2).min(lines.len())].join(" ");
        assert!(
            invocation.contains("--test-threads=1"),
            "{path}: the capacity binary must run one tier at a time:{invocation}"
        );
    }
}

/// The workload-specific shape of a run, beyond the numeric thresholds: a
/// cancellation profile that recorded no cancelled stream measured something
/// else, and a streaming profile with no TTFT never opened a stream.
fn assert_expected_outcomes(profile: &capacity::Profile, result: &CapacityResult) {
    let usage = &result.usage_records;
    match profile.workload {
        Workload::Cancellation => {
            let cancelled = result.outcomes.client_cancelled;
            let expected = capacity::expected_cancellations(
                result.throughput.offered,
                profile.cancel_every.expect("a cancellation cadence"),
            );
            assert_eq!(
                cancelled, expected,
                "{}: the cancellation cadence did not hold: {result:#?}",
                profile.id
            );
            assert_eq!(
                usage.by_status.get("client_cancelled").copied(),
                Some(cancelled),
                "{}: every cancelled stream must settle as `client_cancelled`: {:?}",
                profile.id,
                usage.by_status
            );
            assert!(result.ttft_ms.is_some(), "{}: no stream opened", profile.id);
        }
        Workload::Streaming => {
            assert!(result.ttft_ms.is_some(), "{}: no stream opened", profile.id);
            assert_eq!(
                usage.by_status.get("ok").copied(),
                Some(result.throughput.accepted),
                "{}: every completed stream must settle as `ok`: {:?}",
                profile.id,
                usage.by_status
            );
        }
        Workload::Mixed => {
            assert!(
                result.ttft_ms.is_some(),
                "{}: the mix must include streams",
                profile.id
            );
            assert!(
                result.upstream.streams_opened > 0,
                "{}: no upstream stream was opened",
                profile.id
            );
        }
        Workload::Buffered | Workload::ResponseSize => {
            assert_eq!(
                usage.by_status.get("ok").copied(),
                Some(result.throughput.accepted),
                "{}: every buffered request must settle as `ok`: {:?}",
                profile.id,
                usage.by_status
            );
        }
    }
    assert_eq!(
        result.upstream.requests, result.throughput.accepted,
        "{}: every accepted request must have reached the upstream exactly once",
        profile.id
    );
}
