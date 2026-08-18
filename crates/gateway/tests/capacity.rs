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

use std::collections::BTreeMap;

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

    for workload in Workload::ALL {
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
        // Every threshold below is optional in the schema, because the profiles
        // that exist to be refused cannot state their floor the way the others
        // do. Optional must not become ungated: whatever form it takes, a
        // profile says how much of its offered load has to be served and how
        // much of it may fail — and the suite separately asserts that offered
        // load is conserved, so a bound on one side bounds the other.
        let thresholds = &profile.thresholds;
        assert!(
            thresholds
                .min_accepted_fraction
                .is_some_and(|floor| floor > 0.0)
                || thresholds.min_accepted.is_some_and(|floor| floor > 0),
            "{}: a profile without an acceptance floor asserts nothing",
            profile.id
        );
        assert!(
            thresholds.max_errors.is_some() || thresholds.max_error_fraction.is_some(),
            "{}: nothing bounds how much of the offered load may fail",
            profile.id
        );
        match profile.workload {
            Workload::Cancellation => assert!(
                profile.cancel_every.is_some(),
                "{}: the cancellation workload needs `cancel_every`",
                profile.id
            ),
            Workload::Shedding => {
                let ceiling = profile
                    .max_in_flight
                    .expect("the shedding workload needs `max_in_flight`");
                for tier in [Tier::Reduced, Tier::Heavy] {
                    assert!(
                        profile.scale(tier).concurrency as u64 > ceiling,
                        "{} [{}]: a ceiling at or above the offered concurrency \
                         would never shed, and the profile would pass by \
                         measuring nothing",
                        profile.id,
                        tier.as_str()
                    );
                }
                assert!(
                    thresholds.min_rejected_fraction.unwrap_or_default() > 0.0,
                    "{}: a shedding profile that need not shed asserts nothing",
                    profile.id
                );
            }
            Workload::Queueing => {
                let ceiling = profile
                    .max_in_flight
                    .expect("the queueing workload needs `max_in_flight`");
                let capacity = profile
                    .queue_capacity
                    .expect("the queueing workload needs `queue_capacity`");
                assert!(
                    profile.queue_wait_ms.is_some_and(|wait| wait > 0),
                    "{}: the queueing workload needs a positive `queue_wait_ms`",
                    profile.id
                );
                for tier in [Tier::Reduced, Tier::Heavy] {
                    assert!(
                        profile.scale(tier).concurrency as u64 > ceiling + capacity,
                        "{} [{}]: offered concurrency must overflow both admission and queue",
                        profile.id,
                        tier.as_str()
                    );
                }
                assert_eq!(
                    (thresholds.min_queue_depth, thresholds.max_queue_depth),
                    (Some(capacity), Some(capacity)),
                    "{}: the queue must be proved to reach, and never exceed, its bound",
                    profile.id
                );
                assert!(
                    thresholds.min_rejected_fraction.unwrap_or_default() > 0.0,
                    "{}: a queueing profile that need not overflow asserts nothing",
                    profile.id
                );
            }
            Workload::BackendLimits => {
                assert!(
                    profile.upstream_timeout_ms.is_some(),
                    "{}: the backend-limits workload needs `upstream_timeout_ms`",
                    profile.id
                );
                assert_eq!(
                    thresholds.max_over_deadline,
                    Some(0),
                    "{}: the bound the replica declares is the whole claim",
                    profile.id
                );
            }
            Workload::Tenants => assert_eq!(
                (
                    thresholds.max_foreign_credential_uses,
                    thresholds.max_misattributed_usage_records
                ),
                (Some(0), Some(0)),
                "{}: a multi-tenant profile that tolerates a crossed credential \
                 or a misfiled charge is not an isolation claim",
                profile.id
            ),
            Workload::Buffered | Workload::Streaming | Workload::Mixed | Workload::ResponseSize => {
            }
        }
    }
}

/// The tenant rotation splits the offered load evenly and exactly, for any
/// request count: a per-tenant expectation divided out of the total would be
/// wrong whenever the count is not a whole number of rotations, and the
/// isolation assertion is built on it.
#[test]
fn the_tenant_rotation_accounts_for_every_offered_request() {
    for offered in 0..64u64 {
        let per_tenant: Vec<u64> = capacity::tenants()
            .iter()
            .map(|tenant| capacity::offered_per_tenant(offered, tenant))
            .collect();
        assert_eq!(
            per_tenant.iter().sum::<u64>(),
            offered,
            "every offered request belongs to exactly one tenant: {per_tenant:?}"
        );
        assert!(
            per_tenant.iter().max().unwrap_or(&0) - per_tenant.iter().min().unwrap_or(&0) <= 1,
            "{offered} offered requests split unevenly: {per_tenant:?}"
        );
    }
}

/// The isolation count is what an operator is asked to trust, and the shape of
/// failure it must survive is the symmetric one: each customer served with the
/// other's key. Two per-credential totals cannot see it — under a rotation that
/// offers both tenants the same load, both totals balance exactly as they would
/// on an honest run — so the count is paired, caller against credential.
#[test]
fn a_swap_between_two_tenants_is_not_a_clean_run() {
    let tenants = capacity::tenants();
    let [acme, globex] = [&tenants[0], &tenants[1]];
    let dispatches = |pairs: [(&capacity::Tenant, &capacity::Tenant); 2]| {
        pairs
            .iter()
            .map(|(caller, credential)| {
                (
                    upstream::Dispatch {
                        caller: caller.namespace.to_owned(),
                        credential: credential.credential(),
                    },
                    600,
                )
            })
            .collect::<BTreeMap<_, _>>()
    };
    assert_eq!(
        capacity::crossed_credential_uses(&dispatches([(acme, acme), (globex, globex)])),
        0,
        "each tenant served with its own key is the clean run"
    );
    assert_eq!(
        capacity::crossed_credential_uses(&dispatches([(acme, globex), (globex, acme)])),
        1200,
        "every request of a symmetric swap is a crossing, however even the totals are"
    );
    assert_eq!(
        capacity::crossed_credential_uses(&BTreeMap::from([(
            upstream::Dispatch {
                caller: acme.namespace.to_owned(),
                credential: upstream::credential_digest("a-key-this-run-never-configured"),
            },
            7,
        )])),
        7,
        "a credential belonging to nobody in the run is foreign too"
    );
    // The upstream records a request that presented nothing rather than
    // dropping it: an unrecorded call is one the isolation count cannot see, so
    // a run that reached an upstream with no credential at all would have been
    // the cleanest-looking run of the lot.
    assert_eq!(
        capacity::crossed_credential_uses(&BTreeMap::from([(
            upstream::Dispatch {
                caller: acme.namespace.to_owned(),
                credential: upstream::UNCREDENTIALED.to_owned(),
            },
            3,
        )])),
        3,
        "a request that carried no credential is nobody's, so it is foreign"
    );
}

/// The ledger has the same blind spot the credential count had, and closes it
/// the same way: a tenant reaches only its own aliases, so a row names the
/// caller it should have been charged to.
#[test]
fn a_swap_of_the_charges_is_not_a_clean_run_either() {
    let tenants = capacity::tenants();
    let [acme, globex] = [&tenants[0], &tenants[1]];
    let rows = |charged: [(&capacity::Tenant, &capacity::Tenant); 2]| {
        charged
            .iter()
            .map(|(namespace, alias)| {
                serde_json::json!({
                    "namespace": namespace.namespace,
                    "model": alias.chat_alias(),
                })
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        capacity::crossed_usage_records(&rows([(acme, acme), (globex, globex)])),
        0,
        "each tenant charged for what it asked for is the clean run"
    );
    assert_eq!(
        capacity::crossed_usage_records(&rows([(acme, globex), (globex, acme)])),
        2,
        "a swapped pair of charges is two misattributions, not a balanced ledger"
    );
    assert_eq!(
        capacity::crossed_usage_records(&[serde_json::json!({
            "namespace": "a-namespace-this-run-never-configured",
            "model": acme.chat_alias(),
        })]),
        1,
        "a row filed under a namespace the run does not have is a crossing"
    );
    assert_eq!(
        capacity::crossed_usage_records(&[serde_json::json!({
            "namespace": acme.namespace,
            "model": "an-alias-nobody-owns",
        })]),
        1,
        "so is a row naming an alias nobody in the run owns"
    );
}

/// The label standing for "no credential" has to be one no credential can
/// produce, or a key could be mistaken for its own absence.
#[test]
fn the_uncredentialed_label_is_not_a_digest() {
    for material in ["", "none", "Bearer none", "test-upstream-capacity-acme"] {
        assert_ne!(
            upstream::credential_digest(material),
            upstream::UNCREDENTIALED,
            "{material:?} digests to the label reserved for presenting nothing"
        );
    }
}

/// A profile that moves a bound moves it in the config the process boots. A
/// string rewrite that matched nothing would leave the shared default in place
/// while the artifact recorded the ceiling the manifest asked for — a run that
/// never reached its own limit, retained as evidence that it did.
#[test]
fn a_profile_can_only_retune_a_bound_the_shared_tuning_declares() {
    let tuning = capacity::tuning();
    let moved = capacity::retuned(tuning, "max_in_flight", 8);
    assert!(
        moved.contains("\nmax_in_flight = 8\n"),
        "the ceiling moves:\n{moved}"
    );
    assert!(
        moved.contains("\nmax_in_flight_streams = 8192\n"),
        "and the key that merely starts the same does not:\n{moved}"
    );

    for key in [
        "max_in_flight",
        "max_in_flight_streams",
        "response_header_timeout_ms",
        "buffered_body_timeout_ms",
        "stream_idle_timeout_ms",
    ] {
        assert_ne!(
            capacity::retuned(tuning, key, 1),
            tuning,
            "{key} is a bound a profile moves, so the tuning has to declare it"
        );
    }

    let renamed = std::panic::catch_unwind(|| capacity::retuned(tuning, "max_in_flite", 8));
    assert!(
        renamed.is_err(),
        "a bound the tuning does not declare must fail loudly rather than \
         leaving the profile at the default it meant to move"
    );
}

/// Two ways a threshold could be met by measuring nothing, and neither may
/// pass: a failure with no answer at all is the most untyped failure there is,
/// and a threshold whose measurement block is absent measured no property.
#[test]
fn a_threshold_cannot_be_satisfied_by_an_absent_measurement() {
    let outcomes = |untyped: u64, transport: u64| support::capacity::result::Outcomes {
        by_status: BTreeMap::new(),
        rejections_by_error_type: BTreeMap::new(),
        errors_by_error_type: BTreeMap::from([("untyped".to_owned(), untyped)]),
        client_cancelled: 0,
        transport_failures: transport,
    };
    assert_eq!(capacity::untyped_errors(&outcomes(0, 0)), 0);
    assert_eq!(
        capacity::untyped_errors(&outcomes(0, 3)),
        3,
        "a request that ended at the transport carries no typed body either"
    );
    assert_eq!(capacity::untyped_errors(&outcomes(2, 3)), 5);

    let measured = capacity::measured_verdict("max_over_deadline", Some(0), 0);
    assert!(measured.passed && measured.threshold == "max_over_deadline");
    let unmeasured = capacity::measured_verdict("max_over_deadline", None, 0);
    assert!(
        !unmeasured.passed && unmeasured.threshold == "max_over_deadline_unmeasured",
        "a declared threshold with no measurement behind it is a failure, not a \
         zero: {unmeasured:?}"
    );
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
    let report = |procfs: bool, samples: u64, rss: Option<Span>| ResourceReport {
        sampled: rss.is_some(),
        procfs,
        samples,
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
    let measured =
        capacity::memory_verdict(&report(true, 42, Some(span)), bound).expect("a verdict");
    assert_eq!(measured.threshold, "max_rss_growth_kib");
    assert!(measured.passed, "{measured:?}");

    let lost = capacity::memory_verdict(&report(true, 42, None), bound).expect("a verdict");
    assert_eq!(lost.threshold, "resource_sampling");
    assert!(!lost.passed, "a /proc host that measured nothing must fail");

    // A sampler starved by the load it was measuring reports a span made only of
    // the baseline it seeded. That is not a measurement of the run either.
    let starved = capacity::memory_verdict(&report(true, 0, Some(span)), bound).expect("a verdict");
    assert_eq!(starved.threshold, "resource_sampling");
    assert!(
        !starved.passed,
        "a span with no samples behind it must not pass the memory gate"
    );

    assert!(
        capacity::memory_verdict(&report(false, 0, None), bound).is_none(),
        "off a /proc platform there is nothing to assert"
    );
}

/// Growth counts the settled reading as well as the sampled peak: the settled
/// value is taken after the sampler stops, so memory that grew between the last
/// sample and the end of the run would otherwise go unseen.
#[test]
fn growth_after_the_last_sample_still_counts() {
    assert_eq!(
        Span {
            baseline: 1_000,
            peak: 1_200,
            settled: 9_000,
        }
        .growth(),
        8_000
    );
    assert_eq!(
        Span {
            baseline: 1_000,
            peak: 4_000,
            settled: 1_050,
        }
        .growth(),
        3_000,
        "a transient peak is still the peak"
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
    // Offered load is conserved: every caller was either served or told no, and
    // none was left holding a request the replica quietly dropped. This is what
    // lets a profile whose served share cannot be a fraction bound one side of
    // the split and still be gated on both.
    assert_eq!(
        result.throughput.accepted + result.throughput.rejected + result.throughput.errors,
        result.throughput.offered,
        "{}: the offered load does not add up",
        profile.id
    );
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
        Workload::Shedding => {
            assert_eq!(
                usage.by_status.get("ok").copied(),
                Some(result.throughput.accepted + served_probe(result)),
                "{}: every served request must settle as `ok`: {:?}",
                profile.id,
                usage.by_status
            );
            // Shed with the verdict that names the ceiling, rather than with
            // a served-but-broken answer or a queue the caller cannot see.
            assert_eq!(
                error_types(&result.outcomes.rejections_by_error_type),
                vec!["gateway_overloaded"],
                "{}: a shed request must say the replica was full",
                profile.id
            );
            assert!(
                result.occupancy.admission_max_in_flight.is_some(),
                "{}: a shedding run records the ceiling it booted",
                profile.id
            );
        }
        Workload::Queueing => {
            let evidence = result
                .queue
                .as_ref()
                .unwrap_or_else(|| panic!("{}: queue telemetry was not captured", profile.id));
            let capacity = profile.queue_capacity.expect("a queue capacity");
            let ceiling = profile.max_in_flight.expect("an admission ceiling");
            assert!(
                evidence.exact && evidence.observations > 0 && evidence.attributes == 0,
                "{}: queue histogram is not exact label-free evidence: {evidence:?}",
                profile.id
            );
            assert_eq!(
                evidence.max_depth,
                Some(capacity),
                "{}: the bounded queue did not fill exactly: {evidence:?}",
                profile.id
            );
            assert!(
                result.throughput.accepted > ceiling,
                "{}: no request was proved to leave the queue and be served",
                profile.id
            );
            assert_eq!(
                error_types(&result.outcomes.rejections_by_error_type),
                vec!["admission_queue_full"],
                "{}: overflow must be attributed to the queue bound",
                profile.id
            );
            assert_eq!(
                usage.by_status.get("ok").copied(),
                Some(result.throughput.accepted + served_probe(result)),
                "{}: every admitted request must settle as `ok`",
                profile.id
            );
        }
        Workload::Tenants => {
            let tenancy = result
                .tenancy
                .as_ref()
                .unwrap_or_else(|| panic!("{}: a multi-tenant run records tenancy", profile.id));
            for tenant in capacity::tenants() {
                let counts = tenancy
                    .by_namespace
                    .get(tenant.namespace)
                    .unwrap_or_else(|| panic!("{}: {} sent nothing", profile.id, tenant.namespace));
                assert_eq!(
                    counts.offered,
                    capacity::offered_per_tenant(result.throughput.offered, tenant),
                    "{}: {} did not offer its share of the rotation",
                    profile.id,
                    tenant.namespace
                );
                // Without this, a run where one tenant was served nothing —
                // the loudest isolation failure there is — would satisfy every
                // count above by having nothing to misattribute.
                assert!(
                    counts.accepted > 0 && counts.upstream_calls > 0,
                    "{}: {} was never served, so nothing about it was measured",
                    profile.id,
                    tenant.namespace
                );
                // The isolation counts are measured against what the replica
                // dispatched rather than what it served, so that an upstream
                // failure reads as a failure rather than as a crossed
                // credential. This profile has no faults in it, so the two are
                // the same number — and the equality is what keeps the
                // one-directional counts as strict here as an exact match.
                assert_eq!(
                    counts.dispatched, counts.accepted,
                    "{}: {} lost a dispatched request, so its isolation counts \
                     are measured against a load it did not carry",
                    profile.id, tenant.namespace
                );
            }
            assert!(
                result.ttft_ms.is_some(),
                "{}: the tenants must include streams",
                profile.id
            );
        }
        Workload::BackendLimits => {
            let deadlines = result
                .deadlines
                .as_ref()
                .unwrap_or_else(|| panic!("{}: a bounded run records its bound", profile.id));
            assert!(
                result.throughput.errors > 0,
                "{}: no upstream was cut off, so no bound was exercised",
                profile.id
            );
            assert!(
                deadlines.max_latency_ms >= deadlines.bound_ms as f64,
                "{}: nothing waited for the bound, so the profile measured \
                 healthy upstreams: {deadlines:?}",
                profile.id
            );
            // A stalling target trips its own circuit, and everything after
            // that is shed rather than made to wait out the bound again. That
            // is the replica protecting itself, so it is allowed — but only
            // with that verdict, and only for the stalling targets: the
            // healthy one owes an answer to every request sent to it.
            assert_eq!(
                error_types(&result.outcomes.rejections_by_error_type),
                if result.throughput.rejected > 0 {
                    vec!["all_provider_circuits_open"]
                } else {
                    Vec::new()
                },
                "{}: a shed request here must be a tripped circuit, nothing else",
                profile.id
            );
            assert_eq!(
                result.throughput.accepted,
                capacity::offered_to_healthy_backend(result.throughput.offered),
                "{}: a target that stalls must not cost the healthy target a \
                 single request",
                profile.id
            );
            // The charge a cut-off upstream still earns is the point: a bound
            // that ends a request without settling it is a hole in the ledger.
            assert_eq!(
                usage.observed, usage.expected,
                "{}: a request the replica dispatched must settle whatever the \
                 upstream did: {:?}",
                profile.id, usage.by_status
            );
        }
    }
    let dispatched_failures = match profile.workload {
        // The bound ends the request after it reached the upstream.
        Workload::BackendLimits => result.throughput.errors - result.outcomes.transport_failures,
        _ => 0,
    };
    assert_eq!(
        result.upstream.requests,
        result.throughput.accepted + dispatched_failures + served_probe(result),
        "{}: every request the replica dispatched must have reached the \
         upstream exactly once, and a shed one must not have reached it at all",
        profile.id
    );
}

/// The one request offered after the load stopped, when the profile asks for
/// one. It is served by the same fake upstream and settles its own charge, so
/// every count that reconciles against the load has to know about it.
/// The verdicts a set of outcomes carried, in a stable order.
fn error_types(tally: &BTreeMap<String, u64>) -> Vec<&str> {
    tally.keys().map(String::as_str).collect()
}

fn served_probe(result: &CapacityResult) -> u64 {
    u64::from(result.recovery.as_ref().is_some_and(|probe| probe.served))
}
