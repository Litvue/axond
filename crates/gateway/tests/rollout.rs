//! Multi-replica rollout and rollback qualification (#220).
//!
//! Every scenario in `qualification/rollout/manifest.toml` is run against a
//! fleet of real `axond` processes behind a real load balancer, and each run
//! writes a machine-readable artifact under `target/rollout/` carrying the
//! traffic, the drains, the loss ledger, the migration evidence, the rollback
//! decisions, and a timeline of what happened when.
//!
//! What fails here is narrow and machine-independent. Throughput is recorded and
//! never asserted — a shared runner moves it, and a flaky rollout gate is one
//! that gets disabled. The hard failures are the properties of the deployment
//! sequence: no caller sent to a replica the balancer has seen drain, no
//! unanswered request while a replica is being replaced, a buffered request
//! admitted before the signal finished rather than dropped, a stream the
//! upstream never ends cut inside the advertised deadline and accounted for as
//! partial, every usage record flushed before the process exits, and a
//! termination inside the bound an orchestrator's grace period is set from.
//!
//! The reduced tier runs under `cargo test`. The heavy tier is the same code and
//! the same assertions at a scale that wants a runner to itself, behind
//! `AXOND_ROLLOUT=1`.

mod support;

use support::rollout::{self, RolloutResult, Tier};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_reduced_rollout_scenarios_qualify_and_publish_their_evidence() {
    qualify(Tier::Reduced).await;
}

/// The heavy tier: the same scenarios under load a single-replica test never
/// reaches. Opt-in, because it is slow.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_heavy_rollout_scenarios_qualify_and_publish_their_evidence() {
    if std::env::var("AXOND_ROLLOUT").as_deref() != Ok("1") {
        eprintln!("skipping the heavy rollout scenarios; set AXOND_ROLLOUT=1 to run them");
        return;
    }
    qualify(Tier::Heavy).await;
}

/// The manifest is the contract, so it is checked as one: a scenario that lost
/// its thresholds, or that was written with a single replica, would still
/// produce an artifact — and the artifact would look like evidence.
#[test]
fn the_committed_manifest_describes_a_real_rolling_deployment() {
    let (manifest, text) = rollout::manifest::load();
    assert!(
        !text.trim().is_empty(),
        "the manifest's own bytes are what the artifact's hash names"
    );
    let mut ids: Vec<&str> = manifest
        .scenarios
        .iter()
        .map(|scenario| scenario.id.as_str())
        .collect();
    ids.sort_unstable();
    let unique = ids.len();
    ids.dedup();
    assert_eq!(unique, ids.len(), "scenario ids must be unique: {ids:?}");

    for scenario in &manifest.scenarios {
        assert!(
            scenario.replicas >= rollout::manifest::MIN_REPLICAS,
            "{}: a rollout needs somewhere to route while a replica drains",
            scenario.id
        );
        for tier in [Tier::Reduced, Tier::Heavy] {
            let scale = scenario.scale(tier);
            assert!(
                scale.workers > 0 && scale.requests_per_phase >= scale.workers,
                "{} [{}]: every worker must get at least one request",
                scenario.id,
                tier.as_str()
            );
            assert!(
                (1..=scale.requests_per_phase).contains(&scale.stream_every),
                "{} [{}]: a phase with no streams cannot exercise a drain",
                scenario.id,
                tier.as_str()
            );
        }
        let shutdown = scenario.shutdown;
        assert!(
            shutdown.drain_grace_ms > 0 && shutdown.deadline_ms > shutdown.drain_grace_ms,
            "{}: work must be cut after the grace window, not during it",
            scenario.id
        );
        assert!(
            shutdown.flush_timeout_ms > 0,
            "{}: accounting needs a budget to flush in",
            scenario.id
        );
        let thresholds = scenario.thresholds;
        assert_eq!(
            (
                thresholds.max_requests_to_drained_replica,
                thresholds.max_request_loss,
                thresholds.max_usage_record_loss
            ),
            (0, 0, 0),
            "{}: routing to a drained replica, losing a request, and losing a usage record are \
             not budgets to spend",
            scenario.id
        );
        assert!(
            thresholds.min_mixed_version_requests >= 1,
            "{}: a rollout that never served both revisions is a restart",
            scenario.id
        );
    }
}

async fn qualify(tier: Tier) {
    let (manifest, text) = rollout::manifest::load();
    for scenario in &manifest.scenarios {
        let result = rollout::run(scenario, tier, &text).await;
        let path = result.write();
        eprintln!("{}\n  artifact: {}", result.summary(), path.display());
        report(&result);
    }
}

/// Fail with everything that failed, and where the evidence is: a rollout that
/// broke two invariants is not diagnosed by the first one.
fn report(result: &RolloutResult) {
    let failures = result.failures();
    assert!(
        failures.is_empty(),
        "{} [{}] failed {} threshold(s):\n{}\ntimeline:\n{}",
        result.scenario.id,
        result.scenario.tier,
        failures.len(),
        failures
            .iter()
            .map(|verdict| format!(
                "  {} {} {} (measured {})",
                verdict.threshold, verdict.comparison, verdict.bound, verdict.value
            ))
            .collect::<Vec<_>>()
            .join("\n"),
        result
            .timeline
            .iter()
            .map(|event| format!(
                "  {:>7} ms  {:<16} {:<20} {}",
                event.at_ms, event.phase, event.kind, event.detail
            ))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}

/// The withdrawal rules the routing gate rests on, exercised directly: they are
/// the difference between a gate that can catch a balancer routing to a draining
/// replica and one that only ever restates how selection is written.
///
/// They live in this binary rather than beside the harness: `tests/support/` is
/// compiled into every integration test target, so a `#[test]` written there
/// would be collected and run once per target.
mod withdrawal_rules {
    use std::time::{Duration, Instant};

    use super::support::rollout::ingress::{Ingress, Member};

    fn member() -> Member {
        Member::new(
            "previous-0".to_owned(),
            "previous".to_owned(),
            String::new(),
        )
    }

    #[test]
    fn a_drain_marks_a_member_withdrawn_and_dates_it() {
        let member = member();
        member.observe(true, Duration::from_millis(10));
        member.observe(false, Duration::from_millis(80));

        assert_eq!(member.withdrawn_at(), Some(Duration::from_millis(80)));
        assert!(member.is_withdrawn());
    }

    /// A probe that times out once under load is not a drain. Latching the
    /// withdrawal across the rest of the run would fail the zero gate on traffic
    /// the balancer was entitled to place, which is a harness artefact rather
    /// than a rollout defect.
    #[test]
    fn a_readiness_flap_does_not_leave_a_member_withdrawn_for_the_run() {
        let member = member();
        member.observe(true, Duration::from_millis(10));
        member.observe(false, Duration::from_millis(20));
        member.observe(true, Duration::from_millis(30));

        assert!(!member.is_withdrawn());
        member.dispatched(Duration::from_millis(40));
        assert_eq!(
            member.dispatches_after(member.withdrawn_at().expect("the flap is dated")),
            1,
            "the recomputed witness counts events, so a flap is visible in it"
        );

        member.observe(false, Duration::from_millis(50));
        assert_eq!(
            member.dispatches_after(member.withdrawn_at().expect("the drain is dated")),
            0,
            "and the drain that follows is judged from the drain's own instant"
        );
    }

    /// The witness the gate needs: dispatches are compared against the recorded
    /// withdrawal instant, so a balancer that keeps handing work to a drained
    /// member is caught even if its selection stops flagging it.
    #[test]
    fn dispatches_are_counted_against_the_withdrawal_instant() {
        let member = member();
        member.observe(true, Duration::from_millis(10));
        member.dispatched(Duration::from_millis(30));
        member.observe(false, Duration::from_millis(40));
        member.dispatched(Duration::from_millis(41));
        member.dispatched(Duration::from_millis(90));

        assert_eq!(member.forwards_after_withdrawal(), 0);
        assert_eq!(
            member.dispatches_after(member.withdrawn_at().expect("the drain is dated")),
            2
        );
    }

    /// The gate's contract, stated as a test: a dispatch inside the drain grace
    /// is not a defect — the replica is still admitting, and only the scheduler
    /// decided which side of the withdrawal instant it landed on — while one
    /// past the grace is, because by then the replica refuses work.
    #[test]
    fn only_dispatches_past_the_drain_grace_count_against_the_gate() {
        let grace = Duration::from_millis(500);
        let member = member();
        member.observe(true, Duration::from_millis(10));
        member.observe(false, Duration::from_millis(1_000));
        member.dispatched(Duration::from_millis(1_002));
        member.dispatched(Duration::from_millis(1_400));

        let withdrawn_at = member.withdrawn_at().expect("the drain is dated");
        assert_eq!(member.dispatches_after(withdrawn_at), 2);
        assert_eq!(member.dispatches_beyond(withdrawn_at, grace), 0);
        assert_eq!(
            member.worst_dispatch_lag(withdrawn_at),
            Some(Duration::from_millis(400))
        );

        member.dispatched(Duration::from_millis(1_600));
        assert_eq!(
            member.dispatches_beyond(withdrawn_at, grace),
            1,
            "past the grace the replica no longer admits, so this is routing at \
             a drained member rather than a scheduling artefact"
        );
    }

    /// The race itself, produced rather than argued about: a request is held
    /// between selection and forwarding, the member it was placed on is
    /// withdrawn while it waits, and it is then let go. Selection was entitled
    /// to it — the member was ready when it was chosen — so the selection-time
    /// invariant stays zero, and the dispatch witness is what catches it. If
    /// the harness stamped the dispatch at selection instead, this would read
    /// as a clean run and the zero gate could never fail.
    #[tokio::test]
    async fn a_withdrawal_between_selection_and_forwarding_fails_the_gate() {
        let replica = stub_replica().await;
        // A probe interval longer than the test: readiness here is driven by
        // hand, so the outcome does not depend on when a poll happens to land.
        let ingress = Ingress::start(Duration::from_secs(3600), Instant::now()).await;
        let member = ingress.add("previous-0", "previous", &replica);
        member.observe(true, Duration::from_millis(10));

        let pause = ingress.state.pause_before_forwarding();
        let url = ingress.url("/healthz");
        let call = tokio::spawn(async move { reqwest::get(url).await.map(|r| r.status()) });

        pause.await_arrival().await;
        member.observe(false, ingress.state.elapsed());
        pause.release();
        let status = call
            .await
            .expect("the caller task finishes")
            .expect("the held request completes");

        assert!(status.is_success(), "the held request was still served");
        assert_eq!(
            member.forwards_after_withdrawal(),
            0,
            "selection read a ready member, so the selection-time invariant holds"
        );
        let withdrawn_at = member.withdrawn_at().expect("the withdrawal is dated");
        assert_eq!(
            member.dispatches_after(withdrawn_at),
            1,
            "the request nonetheless landed on a withdrawn member, and the \
             witness the gate is decided on says so"
        );
    }

    /// Re-admission clears the mark, and a request forwarded afterwards is not
    /// charged to the drain that preceded it: a replica put back into rotation
    /// is a replica the balancer may use again.
    #[tokio::test]
    async fn a_re_admitted_member_carries_no_withdrawal_mark() {
        let replica = stub_replica().await;
        let ingress = Ingress::start(Duration::from_secs(3600), Instant::now()).await;
        let member = ingress.add("previous-0", "previous", &replica);
        // Dated from the balancer's own clock, so the dispatch that follows is
        // unambiguously later than the flap.
        member.observe(true, ingress.state.elapsed());
        member.observe(false, ingress.state.elapsed());
        member.observe(true, ingress.state.elapsed());
        tokio::time::sleep(Duration::from_millis(5)).await;

        let status = reqwest::get(ingress.url("/healthz"))
            .await
            .expect("the re-admitted member serves")
            .status();

        assert!(status.is_success());
        assert_eq!(
            member.forwards_after_withdrawal(),
            0,
            "the mark was cleared when the member came back"
        );
        assert_eq!(
            member.dispatches_after(member.withdrawn_at().expect("the earlier drain is dated")),
            1,
            "the dispatch is later than that drain, which is why the mark \
             being cleared is what keeps the gate honest"
        );
    }

    /// The typed refusal is the contract: the replica says it is going away,
    /// so the balancer moves the request and the ledger knows the refusing
    /// member may hold a record for work it had begun.
    #[tokio::test]
    async fn a_typed_draining_refusal_is_retried_onto_another_member() {
        let draining =
            stub_answering(503, r#"{"error":{"type":"draining","message":"draining"}}"#).await;
        let serving = stub_replica().await;
        let ingress = Ingress::start(Duration::from_secs(3600), Instant::now()).await;
        let refuser = ingress.add("previous-0", "previous", &draining);
        let server = ingress.add("next-0", "next", &serving);
        refuser.observe(true, ingress.state.elapsed());
        server.observe(true, ingress.state.elapsed());

        let status = reqwest::get(ingress.url("/v1/models"))
            .await
            .expect("the caller is answered")
            .status();

        assert!(status.is_success(), "the retry found a serving member");
        assert_eq!(refuser.draining_refusals(), 1);
        let caller = ingress
            .state
            .callers()
            .pop()
            .expect("the caller request is recorded");
        assert_eq!(
            caller.draining_refusals().collect::<Vec<_>>(),
            ["previous-0"]
        );
        assert_eq!(caller.answered_by().expect("answered").replica, "next-0");
    }

    /// An untyped `503` is a replica failing, not a replica draining. Retrying
    /// it would hide the failure behind a healthy-looking run, and counting it
    /// as a drain refusal would let it excuse a surplus usage record.
    #[tokio::test]
    async fn an_untyped_service_unavailable_is_an_ordinary_answer() {
        let shedding = stub_answering(503, "upstream capacity exhausted").await;
        let serving = stub_replica().await;
        let ingress = Ingress::start(Duration::from_secs(3600), Instant::now()).await;
        let shedder = ingress.add("previous-0", "previous", &shedding);
        let server = ingress.add("next-0", "next", &serving);
        shedder.observe(true, ingress.state.elapsed());
        server.observe(true, ingress.state.elapsed());

        let response = reqwest::get(ingress.url("/v1/models"))
            .await
            .expect("the caller is answered");

        assert_eq!(response.status().as_u16(), 503);
        assert_eq!(
            response.text().await.expect("the body relays"),
            "upstream capacity exhausted",
            "the replica's own answer reaches the caller instead of a retry"
        );
        assert_eq!(shedder.draining_refusals(), 0);
        assert_eq!(shedder.refusals(), 0);
        let caller = ingress
            .state
            .callers()
            .pop()
            .expect("the caller request is recorded");
        assert_eq!(caller.draining_refusals().count(), 0);
        assert!(
            caller.answered_by().is_none(),
            "an unavailable answer owes no usage record"
        );
    }

    /// A replica that answers everything, standing in for a real one: these
    /// tests are about the balancer's bookkeeping, not the gateway's.
    async fn stub_replica() -> String {
        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
                .await
                .expect("the stub binds");
        let addr = listener.local_addr().expect("the stub has an address");
        tokio::spawn(async move {
            let app = axum::Router::new().fallback(|| async { "ok" });
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    /// A replica that answers every request with one fixed status and body,
    /// which is how the two shapes of `503` are told apart.
    async fn stub_answering(status: u16, body: &'static str) -> String {
        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
                .await
                .expect("the stub binds");
        let addr = listener.local_addr().expect("the stub has an address");
        tokio::spawn(async move {
            let app = axum::Router::new().fallback(move || async move {
                (
                    axum::http::StatusCode::from_u16(status).expect("a valid status"),
                    body,
                )
            });
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }
}

/// What may reach an uploaded artifact, decided by test rather than by trusting
/// every future failure path of every operator command not to echo its
/// environment.
///
/// In this binary rather than beside the harness, for the reason the withdrawal
/// rules above give.
mod artifact_redaction {
    use super::support::rollout::run::redacted;

    #[test]
    fn command_output_carries_no_credential_it_was_given() {
        let dsn = "postgres://postgres:hunter2@127.0.0.1:55432/postgres";
        let kek = "0".repeat(64);
        let secrets = [
            ("GW_CONTROL_PLANE_DSN", dsn),
            ("GW_KEK", kek.as_str()),
            ("GW_BREAKGLASS", "fence-breakglass"),
        ];
        let output = redacted(
            &format!("error: connecting to {dsn} failed\nkek={kek}\nbreakglass=fence-breakglass\n"),
            &secrets,
        );

        for secret in [dsn, kek.as_str(), "fence-breakglass", "hunter2"] {
            assert!(
                !output.contains(secret),
                "the artifact still carries `{secret}`:\n{output}"
            );
        }
        assert!(
            output.contains("${GW_KEK}") && output.contains("${GW_BREAKGLASS}"),
            "the evidence still names what was redacted:\n{output}"
        );
        assert!(output.contains("connecting to"), "the message survives");
    }

    /// A URL the harness never handed over — one the binary composed itself, or
    /// one a library logged — is still a credential, so the scrub is by shape
    /// rather than only by known value.
    #[test]
    fn an_unknown_database_url_is_scrubbed_by_shape() {
        let output = redacted(
            "checking postgresql://admin:s3cret@db.internal:5432/axond, then done",
            &[],
        );

        assert_eq!(
            output, "checking ${redacted-url}, then done",
            "the URL is gone whether or not the harness knew it"
        );
    }
}

/// The usage reconciliation, exercised on hand-built ledgers: these are the
/// cases that decide whether a lost record can hide behind a retry, and they are
/// too rare in a live run to be left to one.
///
/// In this binary rather than beside the harness, for the reason the withdrawal
/// rules above give.
mod usage_accounting {
    use std::collections::BTreeMap;

    use super::support::rollout::ingress::{Attempt, CallerRequest};
    use super::support::rollout::result::ReplicaUsage;
    use super::support::rollout::run::reconcile;

    fn caller(id: u64, attempts: Vec<Attempt>) -> CallerRequest {
        CallerRequest { id, attempts }
    }

    fn refused(replica: &str) -> Attempt {
        Attempt {
            replica: replica.to_owned(),
            status: Some(503),
            refused_while_draining: true,
        }
    }

    fn answered(replica: &str) -> Attempt {
        Attempt {
            replica: replica.to_owned(),
            status: Some(200),
            refused_while_draining: false,
        }
    }

    fn dropped(replica: &str) -> Attempt {
        Attempt {
            replica: replica.to_owned(),
            status: None,
            refused_while_draining: false,
        }
    }

    fn records(rows: [(&str, u64); 2]) -> BTreeMap<String, u64> {
        rows.into_iter()
            .map(|(id, count)| (id.to_owned(), count))
            .collect()
    }

    fn row<'a>(ledger: &'a [ReplicaUsage], replica: &str) -> &'a ReplicaUsage {
        ledger
            .iter()
            .find(|row| row.replica == replica)
            .expect("the replica is in the ledger")
    }

    /// One caller request, two replicas: the draining one refused it and the
    /// other answered. Only the answering replica owes a record, and the record
    /// the refusing one kept for the work it had begun is a duplicate of that
    /// caller request rather than a second answered caller.
    #[test]
    fn a_refusal_retried_elsewhere_is_one_caller_request_not_two() {
        let ledger = reconcile(
            &[caller(0, vec![refused("previous-0"), answered("next-0")])],
            &BTreeMap::new(),
            &records([("previous-0", 1), ("next-0", 1)]),
        );

        assert_eq!(row(&ledger, "next-0").caller_requests_answered, 1);
        assert_eq!(row(&ledger, "previous-0").caller_requests_answered, 0);
        assert_eq!(row(&ledger, "previous-0").retry_duplicates, 1);
        assert!(ledger.iter().all(|row| row.missing == 0));
        assert!(ledger.iter().all(|row| row.unexplained_surplus == 0));
    }

    /// Two records for one caller request on the replica that answered it, with
    /// no refusal to explain either: double accounting, and the surplus says so.
    #[test]
    fn a_duplicate_record_for_one_caller_request_is_surplus() {
        let ledger = reconcile(
            &[caller(0, vec![answered("next-0")])],
            &BTreeMap::new(),
            &records([("next-0", 2), ("previous-0", 0)]),
        );

        assert_eq!(row(&ledger, "next-0").unexplained_surplus, 1);
        assert_eq!(row(&ledger, "next-0").retry_duplicates, 0);
    }

    /// The case a fleet-wide count gets wrong. One caller request was refused
    /// and retried, so a duplicate exists and is explained; a second caller
    /// request was answered by a different replica that lost its record. The
    /// totals match — two callers, two records — and the loss is still reported,
    /// because the comparison is made replica by replica.
    #[test]
    fn a_duplicate_on_one_replica_cannot_mask_a_loss_on_another() {
        let ledger = reconcile(
            &[
                caller(0, vec![refused("previous-0"), answered("next-0")]),
                caller(1, vec![answered("previous-1")]),
            ],
            &BTreeMap::new(),
            &[("previous-0", 1), ("next-0", 1), ("previous-1", 0)]
                .into_iter()
                .map(|(id, count)| (id.to_owned(), count))
                .collect(),
        );

        let owed: u64 = ledger.iter().map(|row| row.caller_requests_answered).sum();
        let wrote: u64 = ledger.iter().map(|row| row.usage_records).sum();
        assert_eq!(
            owed, wrote,
            "the fleet-wide totals agree, which is the trap"
        );
        assert_eq!(row(&ledger, "previous-1").missing, 1);
        assert_eq!(row(&ledger, "previous-0").retry_duplicates, 1);
    }

    /// A replica that dropped the connection never reached its request path, so
    /// it is owed nothing and entitled to nothing: a transport failure is not a
    /// draining refusal, and cannot be spent explaining a record.
    #[test]
    fn a_transport_failure_is_not_a_draining_refusal() {
        let ledger = reconcile(
            &[caller(0, vec![dropped("previous-0"), answered("next-0")])],
            &BTreeMap::new(),
            &records([("previous-0", 1), ("next-0", 1)]),
        );

        assert_eq!(
            row(&ledger, "previous-0").caller_requests_refused_while_draining,
            0
        );
        assert_eq!(row(&ledger, "previous-0").retry_duplicates, 0);
        assert_eq!(
            row(&ledger, "previous-0").unexplained_surplus,
            1,
            "a record from a replica that never answered is unexplained"
        );
    }

    /// Requests the harness pinned to a replica bypass the balancer, so they
    /// have no caller identity of their own and are owed against that replica
    /// directly.
    #[test]
    fn pinned_requests_are_owed_by_the_replica_they_were_sent_to() {
        let ledger = reconcile(
            &[],
            &[("previous-0".to_owned(), 2)].into_iter().collect(),
            &records([("previous-0", 1), ("next-0", 0)]),
        );

        assert_eq!(row(&ledger, "previous-0").caller_requests_answered, 2);
        assert_eq!(row(&ledger, "previous-0").missing, 1);
    }
}
