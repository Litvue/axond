//! The rollout driver: one scenario, start to finish, with an artifact.
//!
//! The shape of a run follows the sequence `docs/operations/upgrades.md`
//! prescribes, because the point is to qualify *that* sequence rather than a
//! convenient approximation of it:
//!
//! 1. gate on `axond check preflight` and `axond migrate status` before any
//!    replica exists;
//! 2. bring up the previous revision and offer traffic through the balancer;
//! 3. for each old replica: surge in a next-revision replacement, wait for the
//!    balancer to admit it, then `SIGTERM` the old one with a buffered request
//!    and a stream already in flight on it, and keep offering traffic across the
//!    whole window;
//! 4. offer traffic to the fully replaced fleet;
//! 5. roll one replica back to the previous revision, which is allowed here
//!    because no migration ran;
//! 6. show the rollback that is *not* allowed: a control plane a newer build has
//!    migrated is refused rather than served.
//!
//! Everything measured is recorded; the thresholds in the manifest decide what
//! fails. Throughput is deliberately not a gate — a shared runner moves it — but
//! the fleet properties (no caller sent to a withdrawn replica, no unanswered
//! request, no lost usage record, a termination inside the bound the process
//! advertises) do not move with the machine.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};

use futures::StreamExt;
use serde_json::{Value, json};

use crate::support::capacity::manifest::sha256_hex;
use crate::support::capacity::result::{BinaryMeta, ConfigMeta, Percentiles, Verdict, binary_meta};
use crate::support::gateway::{self, GATEWAY_KEY, alias};

use super::fleet::{Drained, Fleet, NEXT, NEXT_ONLY_ALIAS, PREVIOUS, Revision, pinned};
use super::ingress::{CallerRequest, Forward, Ingress, REPLICA_HEADER, REVISION_HEADER};
use super::manifest::{RESULT_SCHEMA_VERSION, Scale, Scenario, ShutdownBounds, Tier};
use super::result::{
    CapacityEnvelope, CommandRecord, DrainRecord, Environment, Event, Fence, InFlight, LossLedger,
    MigrationEvidence, MixedVersion, PatchRollback, PhaseTraffic, ReplicaRecord, ReplicaUsage,
    RevisionMeta, RollbackEvidence, RolloutResult, RunMeta, ScenarioEcho, StreamCut,
};

/// How often the balancer re-probes readiness. Fast enough that the measured
/// removal is dominated by the replica's own drain rather than by the poll, and
/// slow enough to be a plausible ingress setting.
const PROBE_POLL: Duration = Duration::from_millis(50);

/// The bind address the gate's config carries. A preflight never listens, and
/// the artifact records the config it checked, so the port is fixed rather than
/// ephemeral — an artifact whose config hash changed every run would be
/// uncomparable.
const GATE_BIND: &str = "127.0.0.1:8080";

/// How long the harness waits for the fake upstream to see a pinned request
/// before it signals the replica. Being *in flight* is the precondition of the
/// whole drain measurement, so it is established rather than assumed.
const IN_FLIGHT_WAIT: Duration = Duration::from_secs(5);

/// The prompt every request carries. Fixed, so the fake upstream's answer — and
/// therefore the byte counts and the priced tokens — are the same on every run.
const PROMPT: &str = "qualify the rollout";

pub async fn run(scenario: &Scenario, tier: Tier, manifest_text: &str) -> RolloutResult {
    let scale = *scenario.scale(tier);
    let started_at = SystemTime::now();
    let started = Instant::now();
    let mut harness = Harness::new(scenario.clone(), scale, started).await;

    let revisions = revisions(&harness.fleet.upstream.base_url, scenario.shutdown);
    let migration = harness.gate(&revisions).await;

    // The fleet as it was before anyone touched it.
    for _ in 0..scenario.replicas {
        harness.admit(Revision::previous()).await;
    }
    harness.phase("steady-previous").await;

    // The rollout proper: one replacement at a time, never below the original
    // replica count, which is what makes it a rolling deployment rather than a
    // restart.
    let mut drains = Vec::new();
    let mut mixed = None;
    for index in 0..scenario.replicas {
        let victim = harness
            .fleet
            .oldest(Revision::previous())
            .expect("a previous-revision replica is still serving")
            .id
            .clone();
        harness.admit(Revision::next()).await;
        harness.phase(&format!("mixed-{index}")).await;
        if index == 0 {
            // Both revisions are serving right now, which is the only window in
            // which the mixed-version rule can be observed at all.
            mixed = Some(harness.mixed_version().await);
        }
        drains.push(harness.drain(&victim, &format!("drain-{index}")).await);
    }
    harness.phase("steady-next").await;

    // The rollback an operator is allowed to perform, performed the same way the
    // rollout was: surge the previous revision back in, then drain a new one.
    let rollback_replica = harness.admit(Revision::previous()).await;
    let replaced = harness
        .fleet
        .oldest(Revision::next())
        .expect("a next-revision replica is serving")
        .id
        .clone();
    drains.push(harness.drain(&replaced, "rollback-drain").await);
    let rollback_traffic = harness.phase("rolled-back").await;
    let served = rollback_traffic
        .by_replica
        .get(&rollback_replica)
        .copied()
        .unwrap_or_default();

    let fence = fence(&mut harness.timeline).await;

    // Everything is quiet now, so the accounting can settle.
    let expected_usage = harness.expected_usage;
    let records = harness
        .fleet
        .await_usage_records(expected_usage as usize, Duration::from_secs(10))
        .await;
    let elapsed = started.elapsed();

    let mixed = mixed.expect("the rollout has at least one mixed-version window");
    let loss = ledger(&harness, expected_usage, &records);
    let capacity = envelope(&harness.traffic);
    let fleet_records = fleet_records(&harness, &drains);
    let rollback = RollbackEvidence {
        compatible_patch_rollback: PatchRollback {
            performed: true,
            replica: rollback_replica,
            answered: served,
            errors: rollback_traffic.errors,
            served_traffic: served > 0,
        },
        migrated_layout_fence: fence,
    };

    let result = RolloutResult {
        schema_version: RESULT_SCHEMA_VERSION,
        scenario: ScenarioEcho::new(scenario, tier),
        run: RunMeta::new(started_at, elapsed),
        environment: Environment::collect(manifest_text),
        revisions,
        fleet: fleet_records,
        traffic: harness.traffic,
        drains,
        mixed_version: mixed,
        loss,
        capacity,
        migration,
        rollback,
        timeline: harness.timeline.events,
        verdicts: Vec::new(),
    };
    let verdicts = verdicts(&result);
    RolloutResult { verdicts, ..result }
}

/// The run's mutable state, in one place so the phases can be methods: a phase
/// needs the fleet, the balancer, the clock, and the ledger, and threading four
/// of those through free functions is how a driver becomes unreadable.
struct Harness {
    scenario: Scenario,
    scale: Scale,
    fleet: Fleet,
    ingress: Ingress,
    client: reqwest::Client,
    started: Instant,
    timeline: Timeline,
    traffic: Vec<PhaseTraffic>,
    /// One per request that reached a replica's request path. Counted as the run
    /// goes, so the denominator of the loss ledger is derived from what was
    /// offered rather than from what turned up.
    expected_usage: u64,
    /// Records expected from requests the harness pinned past the balancer,
    /// under the replica it pinned them to. The balancer's caller ledger cannot
    /// know about these, and usage is reconciled per replica.
    pinned_expectations: BTreeMap<String, u64>,
    mixed_probe: Option<MixedVersion>,
    /// How long each replica took from being booted to carrying traffic. Kept
    /// per replica because the offset the balancer records is an offset from the
    /// run's start, which grows with the run rather than with how slowly a
    /// replacement was admitted.
    admissions: BTreeMap<String, Duration>,
}

impl Harness {
    async fn new(scenario: Scenario, scale: Scale, started: Instant) -> Self {
        let fleet = Fleet::start(scenario.shutdown).await;
        let ingress = Ingress::start(PROBE_POLL, started).await;
        Self {
            scenario,
            scale,
            fleet,
            ingress,
            client: crate::support::client(),
            started,
            timeline: Timeline::new(started),
            traffic: Vec::new(),
            expected_usage: 0,
            pinned_expectations: BTreeMap::new(),
            mixed_probe: None,
            admissions: BTreeMap::new(),
        }
    }

    /// The deployment gate, run before a replica exists — which is the whole
    /// point of it: a config that fails preflight must never become a process.
    async fn gate(&mut self, revisions: &[RevisionMeta]) -> MigrationEvidence {
        let dir = scratch_dir("gate");
        let next = revisions
            .iter()
            .find(|revision| revision.label == NEXT)
            .expect("the next revision is described");
        let path = write_config(&dir, "next.toml", &next.config.normalized_toml);
        let preflight = axond(&["check", "preflight", "--config", &path], &[]);
        let status = axond(&["migrate", "status", "--config", &path], &[]);
        let gate_passed = preflight.succeeded && status.succeeded;
        self.timeline.at(
            "gate",
            "migration-gate",
            format!(
                "preflight {} and migrate status {} for the incoming revision",
                verdict_word(preflight.succeeded),
                verdict_word(status.succeeded)
            ),
        );
        assert!(
            gate_passed,
            "the incoming revision failed its own deployment gate, so the rollout must not \
             start:\npreflight:\n{}\nmigrate status:\n{}",
            preflight.output, status.output
        );
        MigrationEvidence {
            preflight,
            status,
            gate_passed,
            control_plane: "none: the qualified deployment is stateless, so there is no schema to \
                            migrate. The forward-only fence is exercised separately against a real \
                            control plane."
                .to_owned(),
        }
    }

    /// Boot a replica, put it in rotation, and wait for the balancer to start
    /// using it. Returns its id.
    async fn admit(&mut self, revision: Revision) -> String {
        let bound = Duration::from_millis(self.scenario.thresholds.max_replacement_admission_ms);
        let booting = Instant::now();
        let replica = self.fleet.admit(revision).await;
        let (id, base_url) = (replica.id.clone(), replica.base_url().to_owned());
        self.ingress.add(&id, revision.label, &base_url);
        let admitted = self.ingress.await_admission(&id, bound).await;
        if admitted.is_some() {
            self.admissions.insert(id.clone(), booting.elapsed());
        }
        self.timeline.at(
            "admission",
            "replica-admitted",
            match admitted {
                Some(_) => format!(
                    "{id} ({}) took {} ms from boot to carrying traffic",
                    revision.label,
                    booting.elapsed().as_millis()
                ),
                None => format!(
                    "{id} ({}) was never admitted within {} ms",
                    revision.label,
                    bound.as_millis()
                ),
            },
        );
        assert!(
            admitted.is_some(),
            "{id} never became ready, so the rollout has no replacement to route to"
        );
        id
    }

    /// Offer one phase of caller traffic through the balancer.
    async fn phase(&mut self, name: &str) -> PhaseTraffic {
        let before = self.ingress.state.forwards().len();
        self.timeline.at(name, "phase-start", "offering traffic");
        let (outcomes, elapsed) = offer(
            self.ingress.base_url.clone(),
            self.scale,
            self.client.clone(),
        )
        .await;
        let traffic = self.settle(name, &outcomes, elapsed, before);
        self.timeline.at(name, "phase-end", summary_of(&traffic));
        traffic
    }

    /// Turn a phase's outcomes into the recorded traffic, and cross-check the
    /// driver's attribution against the balancer's own log: two witnesses to the
    /// same routing, and a disagreement is a harness bug rather than a result.
    fn settle(
        &mut self,
        name: &str,
        outcomes: &[Outcome],
        elapsed: Duration,
        forwards_before: usize,
    ) -> PhaseTraffic {
        let forwards = self.ingress.state.forwards();
        let placed = &forwards[forwards_before.min(forwards.len())..];
        let answered = outcomes.iter().filter(|o| o.ok()).count() as u64;
        let torn = outcomes.iter().filter(|o| o.torn).count() as u64;
        let mut by_status: BTreeMap<String, u64> = BTreeMap::new();
        let mut by_replica: BTreeMap<String, u64> = BTreeMap::new();
        let mut by_revision: BTreeMap<String, u64> = BTreeMap::new();
        for outcome in outcomes {
            match outcome.status {
                Some(status) => *by_status.entry(status.to_string()).or_default() += 1,
                None => *by_status.entry("transport-failure".to_owned()).or_default() += 1,
            }
            if let Some(replica) = outcome.replica.as_ref() {
                *by_replica.entry(replica.clone()).or_default() += 1;
            }
            if let Some(revision) = outcome.revision.as_ref() {
                *by_revision.entry(revision.clone()).or_default() += 1;
            }
        }
        let latencies: Vec<f64> = outcomes.iter().map(|o| o.latency_ms).collect();
        self.expected_usage += answered;
        let traffic = PhaseTraffic {
            phase: name.to_owned(),
            offered: outcomes.len() as u64,
            answered,
            errors: outcomes
                .iter()
                .filter(|o| o.status.is_some_and(|s| !(200..300).contains(&s)))
                .count() as u64,
            unanswered: outcomes.iter().filter(|o| o.status.is_none()).count() as u64,
            torn_streams: torn,
            streamed: outcomes.iter().filter(|o| o.streamed).count() as u64,
            elapsed_ms: elapsed.as_millis(),
            answered_rps: rate(answered, elapsed),
            latency_ms: Percentiles::of(&latencies),
            by_status,
            by_replica: by_replica.clone(),
            by_revision,
            retried: placed.iter().filter(|forward| forward.retries > 0).count() as u64,
        };
        assert_eq!(
            balancer_counts(placed),
            by_replica,
            "the balancer's own log disagrees with the caller's attribution in {name}"
        );
        self.traffic.push(traffic.clone());
        traffic
    }

    /// The mixed-version window, put to the processes rather than assumed: the
    /// capability only the incoming revision has is asked of one replica of each
    /// revision, at the moment both are in rotation.
    async fn mixed_version(&mut self) -> MixedVersion {
        let (_, previous) = self.pinned_replica(PREVIOUS);
        let (next_id, next) = self.pinned_replica(NEXT);
        let on_next = self
            .capability(&next)
            .await
            .expect("the incoming revision answered the capability probe");
        let on_previous = self
            .capability(&previous)
            .await
            .expect("the outgoing revision answered the capability probe");
        // The probe that succeeded is a request like any other, so it is in the
        // accounting; the one that was refused never reached a provider and has
        // no usage record to expect.
        if (200..300).contains(&on_next) {
            self.expected_usage += 1;
            *self.pinned_expectations.entry(next_id).or_default() += 1;
        }
        let phase = self
            .traffic
            .last()
            .expect("a mixed-version phase has already run");
        let mixed = MixedVersion {
            previous_requests: phase.by_revision.get(PREVIOUS).copied().unwrap_or_default(),
            next_requests: phase.by_revision.get(NEXT).copied().unwrap_or_default(),
            exclusive_alias: NEXT_ONLY_ALIAS.to_owned(),
            next_serves_exclusive_alias: (200..300).contains(&on_next),
            previous_refuses_exclusive_alias: !(200..300).contains(&on_previous),
            previous_status_for_exclusive_alias: Some(on_previous),
        };
        self.timeline.at(
            "mixed-version",
            "capability-probe",
            format!(
                "`{NEXT_ONLY_ALIAS}` answered {on_next} on the incoming revision and \
                 {on_previous} on the outgoing one, with {} and {} requests served in the window",
                mixed.next_requests, mixed.previous_requests
            ),
        );
        self.mixed_probe = Some(mixed.clone());
        mixed
    }

    /// Ask one replica for the alias only the incoming revision serves.
    async fn capability(&self, base_url: &str) -> Option<u16> {
        self.client
            .post(format!("{base_url}/v1/chat/completions"))
            .bearer_auth(GATEWAY_KEY)
            .json(&body(NEXT_ONLY_ALIAS, false))
            .send()
            .await
            .ok()
            .map(|response| response.status().as_u16())
    }

    /// A live replica at `revision`, as the pair a pinned request needs: the id
    /// its records will be accounted under, and the address to send to.
    fn pinned_replica(&self, revision: &str) -> (String, String) {
        let replica = self
            .fleet
            .replicas()
            .iter()
            .find(|replica| replica.revision.label == revision)
            .unwrap_or_else(|| panic!("a {revision}-revision replica is serving"));
        (replica.id.clone(), replica.base_url().to_owned())
    }

    /// Take one replica out of the rollout: a buffered request and a stream are
    /// pinned to it and confirmed in flight, the balancer keeps offering traffic
    /// across the whole window, and the process is held to the bound it
    /// advertises.
    async fn drain(&mut self, id: &str, phase: &str) -> DrainRecord {
        let base_url = self.fleet.replica(id).base_url().to_owned();
        let revision = self.fleet.replica(id).revision.label.to_owned();

        let buffered = self.pin(&base_url, pinned::BUFFERED, false).await;
        let stream = self.pin(&base_url, pinned::STREAM, true).await;
        // Both are through the replica's request path and at the upstream, so
        // both will settle a usage record however the drain ends them. The
        // stream is the one the deadline cuts, so its record is a cancellation.
        self.expected_usage += 2;
        *self.pinned_expectations.entry(id.to_owned()).or_default() += 2;

        let forwards_before = self.ingress.state.forwards().len();
        let traffic = tokio::spawn(offer(
            self.ingress.base_url.clone(),
            self.scale,
            self.client.clone(),
        ));

        let signalled = Instant::now();
        self.fleet.signal(id);
        self.timeline.at(
            phase,
            "sigterm",
            format!("{id} ({revision}) was signalled with two requests in flight on it"),
        );

        let thresholds = self.scenario.thresholds;
        let drain_grace = Duration::from_millis(self.scenario.shutdown.drain_grace_ms);
        let removal_bound = Duration::from_millis(thresholds.max_readiness_removal_ms.max(1) * 4)
            .max(Duration::from_secs(2));
        let slack = Duration::from_millis(thresholds.max_drain_exit_slack_ms);
        let (withdrawn, drained) = tokio::join!(
            self.ingress.await_withdrawal(id, signalled, removal_bound),
            self.fleet.retire(id, signalled, slack),
        );
        let (outcomes, elapsed) = traffic.await.expect("the drain-window traffic completes");
        self.settle(phase, &outcomes, elapsed, forwards_before);

        let buffered = buffered.settle().await;
        let stream = stream.settle().await;
        let member = self
            .ingress
            .state
            .member(id)
            .expect("the drained replica is a balancer member");

        self.timeline.at(
            phase,
            "readiness-removed",
            match withdrawn {
                Some(after) => format!(
                    "the balancer stopped routing to {id} {} ms after the signal",
                    after.as_millis()
                ),
                None => format!("the balancer never stopped routing to {id}"),
            },
        );
        self.timeline.at(
            phase,
            "stream-cut",
            format!(
                "the pinned stream ended {} ms after the signal with {} bytes relayed",
                stream.ended_after(signalled).as_millis(),
                stream.bytes
            ),
        );
        self.timeline.at(
            phase,
            "buffered-completed",
            format!(
                "the pinned buffered request answered {:?} {} ms after the signal",
                buffered.status,
                buffered.ended_after(signalled).as_millis()
            ),
        );
        self.timeline.at(
            phase,
            "exited",
            match drained.took {
                Some(took) => format!(
                    "{id} exited {} after {} ms, having flushed {} usage records",
                    if drained.clean { "cleanly" } else { "non-zero" },
                    took.as_millis(),
                    drained.usage_records.len()
                ),
                None => format!(
                    "{id} outlived the {} ms bound it advertises:\n{}",
                    drained.budget.as_millis(),
                    drained.output
                ),
            },
        );

        DrainRecord {
            replica: id.to_owned(),
            revision,
            signalled_at_ms: signalled.duration_since(self.started).as_millis(),
            readiness_removed_after_ms: withdrawn.map(|after| after.as_millis()),
            exited_after_ms: drained.took.map(|took| took.as_millis()),
            exit_clean: drained.clean,
            exit_budget_ms: drained.budget.as_millis(),
            requests_after_withdrawal: member.forwards_after_withdrawal(),
            // Recomputed from the recorded dispatch instants against the
            // recorded withdrawal instant, rather than from the flag the
            // selection carried: two witnesses to the same boundary, so the gate
            // survives the selection stopping to enforce it.
            dispatches_after_withdrawal: member
                .withdrawn_at()
                .map_or(0, |at| member.dispatches_after(at)),
            // Only dispatches past the replica's own grace window are a defect:
            // inside it the replica is still admitting, so a hand-over the
            // scheduler delayed across the withdrawal instant is served exactly
            // as it would be in production.
            dispatches_beyond_drain_grace: member
                .withdrawn_at()
                .map_or(0, |at| member.dispatches_beyond(at, drain_grace)),
            worst_dispatch_lag_ms: member
                .withdrawn_at()
                .and_then(|at| member.worst_dispatch_lag(at))
                .map(|lag| lag.as_millis()),
            drain_grace_ms: self.scenario.shutdown.drain_grace_ms,
            buffered_in_flight: InFlight {
                status: buffered.status,
                completed_after_signal_ms: buffered.ended_after(signalled).as_millis(),
                usage_status: usage_status(&drained, pinned::BUFFERED),
            },
            stream_in_flight: StreamCut {
                cut_after_signal_ms: stream.ended_after(signalled).as_millis(),
                relayed_bytes: stream.bytes,
                usage_status: usage_status(&drained, pinned::STREAM),
                within_deadline: stream.ended_after(signalled) <= drained.budget,
            },
            usage_records_flushed: drained.usage_records.len() as u64,
        }
    }

    /// Start a request pinned to one replica — past the balancer, so the drain
    /// cannot route it away — and return once the upstream has seen it, which is
    /// what makes "in flight" a fact rather than a hope.
    async fn pin(&self, base_url: &str, alias: &str, stream: bool) -> Pinned {
        // The exact arrival count, not the retained-request list: that list is
        // capped, so at heavy scale its length stops growing and a wait on it
        // could never be satisfied.
        let seen = self.fleet.upstream.state.received();
        let client = self.client.clone();
        let url = format!("{base_url}/v1/chat/completions");
        let payload = body(alias, stream);
        let handle = tokio::spawn(async move {
            let response = client
                .post(url)
                .bearer_auth(GATEWAY_KEY)
                .json(&payload)
                .send()
                .await;
            let Ok(response) = response else {
                return (None, 0, Instant::now());
            };
            let status = response.status().as_u16();
            let mut body = response.bytes_stream();
            let mut bytes = 0u64;
            while let Some(chunk) = body.next().await {
                match chunk {
                    Ok(chunk) => bytes += chunk.len() as u64,
                    Err(_) => break,
                }
            }
            (Some(status), bytes, Instant::now())
        });
        let deadline = Instant::now() + IN_FLIGHT_WAIT;
        while self.fleet.upstream.state.received() <= seen {
            assert!(
                Instant::now() < deadline,
                "the pinned `{alias}` request never reached the upstream, so the drain would not \
                 have found it in flight"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Pinned { handle }
    }
}

/// A request the harness holds against one replica across its drain.
struct Pinned {
    handle: tokio::task::JoinHandle<(Option<u16>, u64, Instant)>,
}

impl Pinned {
    async fn settle(self) -> Settled {
        let (status, bytes, ended) = self.handle.await.expect("the pinned request settles");
        Settled {
            status,
            bytes,
            ended,
        }
    }
}

struct Settled {
    status: Option<u16>,
    bytes: u64,
    ended: Instant,
}

impl Settled {
    fn ended_after(&self, signalled: Instant) -> Duration {
        self.ended.saturating_duration_since(signalled)
    }
}

/// One caller request as the driver saw it.
struct Outcome {
    status: Option<u16>,
    replica: Option<String>,
    revision: Option<String>,
    latency_ms: f64,
    streamed: bool,
    /// A stream whose body failed part-way: the answer started and did not
    /// finish.
    torn: bool,
}

impl Outcome {
    fn ok(&self) -> bool {
        self.status
            .is_some_and(|status| (200..300).contains(&status))
    }
}

/// Offer one phase of load through the balancer, closed-loop over `workers`.
///
/// Owned arguments only, so a phase can be spawned to run *across* a drain
/// rather than before or after it.
async fn offer(
    base_url: String,
    scale: Scale,
    client: reqwest::Client,
) -> (Vec<Outcome>, Duration) {
    let next = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let workers = (0..scale.workers).map(|_| {
        let (client, base_url, next) = (client.clone(), base_url.clone(), next.clone());
        tokio::spawn(async move {
            let mut mine = Vec::new();
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= scale.requests_per_phase {
                    return mine;
                }
                mine.push(one(&client, &base_url, scale.streams(index)).await);
            }
        })
    });
    let outcomes = futures::future::join_all(workers)
        .await
        .into_iter()
        .flat_map(|worker| worker.expect("a traffic worker completes"))
        .collect();
    (outcomes, started.elapsed())
}

/// One request through the balancer, read to the last byte.
async fn one(client: &reqwest::Client, base_url: &str, streamed: bool) -> Outcome {
    let alias = if streamed {
        alias::CHAT_SLOW
    } else {
        alias::CHAT
    };
    let at = Instant::now();
    let sent = client
        .post(format!("{base_url}/v1/chat/completions"))
        .bearer_auth(GATEWAY_KEY)
        .json(&body(alias, streamed))
        .send()
        .await;
    let Ok(response) = sent else {
        return Outcome {
            status: None,
            replica: None,
            revision: None,
            latency_ms: at.elapsed().as_secs_f64() * 1000.0,
            streamed,
            torn: false,
        };
    };
    let status = response.status().as_u16();
    let header = |name: &str| {
        response
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned)
    };
    let (replica, revision) = (header(REPLICA_HEADER), header(REVISION_HEADER));
    let mut body = response.bytes_stream();
    let mut torn = false;
    while let Some(chunk) = body.next().await {
        if chunk.is_err() {
            torn = true;
            break;
        }
    }
    Outcome {
        status: Some(status),
        replica,
        revision,
        latency_ms: at.elapsed().as_secs_f64() * 1000.0,
        streamed,
        torn,
    }
}

fn body(alias: &str, stream: bool) -> Value {
    json!({
        "model": alias,
        "messages": [{"role": "user", "content": PROMPT}],
        "stream": stream,
    })
}

/// The rollback that must *not* be possible: a control plane a newer build has
/// migrated is refused rather than served, and the refusal names the reason.
///
/// Needs a real PostgreSQL, because the fence lives in the ledger rather than in
/// the binary; without one the artifact says the fence was not evaluated instead
/// of implying it passed.
async fn fence(timeline: &mut Timeline) -> Fence {
    let Ok(dsn) = std::env::var("AXOND_TEST_POSTGRES_DSN") else {
        let reason = "AXOND_TEST_POSTGRES_DSN is unset, so no control plane exists to migrate \
                      past this build";
        timeline.at("rollback", "fence-skipped", reason);
        return Fence {
            evaluated: false,
            skipped_reason: Some(reason.to_owned()),
            status: None,
            refused: false,
            refusal_names_newer_build: false,
        };
    };
    let schema = format!(
        "rollout_fence_{}",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    );
    let client = connect(&dsn).await;
    client
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .expect("the fence's own schema is created");

    let dir = scratch_dir("fence");
    let path = write_config(&dir, "stateful.toml", &stateful_config(&schema));
    let kek = "0".repeat(64);
    let env = [
        ("GW_CONTROL_PLANE_DSN", dsn.as_str()),
        ("GW_KEK", kek.as_str()),
        ("GW_BREAKGLASS", "fence-breakglass"),
    ];
    let applied = axond(&["migrate", "apply", "--config", &path], &env);
    assert!(
        applied.succeeded,
        "the fence needs a migrated control plane to roll back onto:\n{}",
        applied.output
    );

    // What a *newer* build leaves behind: a ledger entry this build has never
    // heard of. Rolling this binary onto that database is the rollback the
    // forward-only rule prohibits.
    client
        .batch_execute(&format!(
            "INSERT INTO {schema}.axond_cp_schema_migration (version, name, checksum) VALUES \
             (999, 'control_plane_0999_namespace_cap', 'sha256:{}')",
            sha256_hex(b"a newer, namespace-cap-aware build wrote this")
        ))
        .await
        .expect("a newer build's ledger entry is written");

    let status = axond(&["migrate", "status", "--config", &path], &env);
    let refused = !status.succeeded;
    let names_newer = status.output.contains("newer gateway");
    timeline.at(
        "rollback",
        "fence-evaluated",
        format!(
            "rolling this build onto a migrated control plane was {}",
            if refused { "refused" } else { "ALLOWED" }
        ),
    );
    let _ = client
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await;
    Fence {
        evaluated: true,
        skipped_reason: None,
        status: Some(status),
        refused,
        refusal_names_newer_build: names_newer,
    }
}

async fn connect(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("the fence connects to PostgreSQL");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// A stateful config pointed at one schema of its own. Names variables, never
/// values: nothing here or in the artifact carries a DSN.
fn stateful_config(schema: &str) -> String {
    format!(
        "mode = \"stateful\"\n\
         [control_plane]\n\
         dsn_env = \"GW_CONTROL_PLANE_DSN\"\n\
         schema = \"{schema}\"\n\
         [secret_store]\n\
         kek_env = \"GW_KEK\"\n\
         [[admin_breakglass]]\n\
         env = \"GW_BREAKGLASS\"\n"
    )
}

/// Run one operator command against the built binary and keep what an operator
/// would read.
fn axond(args: &[&str], env: &[(&str, &str)]) -> CommandRecord {
    let mut command = Command::new(env!("CARGO_BIN_EXE_axond"));
    let secrets: Vec<(&str, &str)> = [
        ("GW_INBOUND_KEY", GATEWAY_KEY),
        // The gate resolves every reference the config makes, including the
        // per-boot key a replica is given, so the command's environment is the
        // one a replica would boot with.
        (gateway::BOOT_KEY_ENV, "gate-boot-key"),
        ("GW_FAKE_OPENAI_KEY", gateway::OPENAI_KEY),
        ("GW_FAKE_ANTHROPIC_KEY", gateway::ANTHROPIC_KEY),
        (gateway::OPENAI_SECONDARY_ENV, gateway::OPENAI_KEY_SECONDARY),
        (
            gateway::ANTHROPIC_SECONDARY_ENV,
            gateway::ANTHROPIC_KEY_SECONDARY,
        ),
    ]
    .into_iter()
    .chain(env.iter().copied())
    .collect();
    command.args(args).env("RUST_LOG", "warn");
    for (name, value) in &secrets {
        command.env(name, value);
    }
    let output = command.output().expect("the axond binary runs");
    CommandRecord {
        argv: std::iter::once("axond".to_owned())
            .chain(args.iter().map(ToString::to_string))
            .collect(),
        exit_code: output.status.code(),
        succeeded: output.status.success(),
        output: redacted(
            &format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ),
            &secrets,
        ),
    }
    // `args` are not part of the environment, so they are recorded verbatim:
    // the harness never passes a credential on the command line.
}

/// Command output goes into an uploaded artifact, so it may only carry what an
/// operator could paste into a ticket. Every value the command was given is
/// treated as a credential and replaced by the name it came from, and any
/// database URL is dropped whole — a failure path that echoes its environment
/// must not turn the artifact into a secret.
pub fn redacted(text: &str, secrets: &[(&str, &str)]) -> String {
    let mut out = text.to_owned();
    for (name, value) in secrets {
        // Short values would match unrelated text; nothing this harness passes
        // as a credential is that short.
        if value.len() >= 8 {
            out = out.replace(value, &format!("${{{name}}}"));
        }
    }
    scrub_urls(&out)
}

/// Replace every `scheme://…` run with a placeholder. Coarse on purpose: a DSN
/// the harness never learned (one the binary composed, say) is still a DSN.
fn scrub_urls(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find("://") {
        // Resume after the delimiter itself, which is one `char` rather than one
        // byte: output carrying a non-ASCII glyph before a URL must be redacted,
        // not panicked on.
        let scheme_start = rest[..at]
            .char_indices()
            .rev()
            .find(|(_, c)| !c.is_ascii_alphanumeric() && *c != '+' && *c != '-' && *c != '.')
            .map_or(0, |(index, c)| index + c.len_utf8());
        let end = rest[at..]
            .find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | ',' | ')'))
            .map_or(rest.len(), |offset| at + offset);
        out.push_str(&rest[..scheme_start]);
        out.push_str("${redacted-url}");
        rest = &rest[end..];
    }
    out.push_str(rest);
    out
}

/// The two revisions the rollout moves between, with the artifact identity of
/// each. One binary, two configs: the artifact says so rather than implying a
/// cross-build rollout the test did not perform.
fn revisions(upstream: &str, shutdown: ShutdownBounds) -> Vec<RevisionMeta> {
    let bind: SocketAddr = GATE_BIND.parse().expect("the gate address parses");
    [Revision::previous(), Revision::next()]
        .into_iter()
        .map(|revision| {
            let config = gateway::config_toml(bind, upstream, &revision.tuning(shutdown), "")
                .replace(upstream, "http://127.0.0.1:UPSTREAM_PORT");
            RevisionMeta {
                label: revision.label.to_owned(),
                binary: binary_meta(),
                config: ConfigMeta {
                    sha256: sha256_hex(config.as_bytes()),
                    normalized_toml: config,
                },
                distinct_binary: false,
                exclusive_aliases: match revision.label {
                    NEXT => vec![NEXT_ONLY_ALIAS.to_owned()],
                    _ => Vec::new(),
                },
            }
        })
        .collect()
}

/// Everything the balancer and the fleet know about each replica, live or gone.
fn fleet_records(harness: &Harness, drains: &[DrainRecord]) -> Vec<ReplicaRecord> {
    let usage: BTreeMap<&str, u64> = drains
        .iter()
        .map(|drain| (drain.replica.as_str(), drain.usage_records_flushed))
        .collect();
    harness
        .ingress
        .state
        .members()
        .iter()
        .map(|member| {
            let live = harness
                .fleet
                .replicas()
                .iter()
                .find(|replica| replica.id == member.id);
            ReplicaRecord {
                id: member.id.clone(),
                revision: member.revision.clone(),
                admitted_at_ms: member.admitted_at().map(|at| at.as_millis()),
                admission_took_ms: harness
                    .admissions
                    .get(&member.id)
                    .map(|took| took.as_millis()),
                withdrawn_at_ms: member.withdrawn_at().map(|at| at.as_millis()),
                requests_served: member.forwards(),
                requests_after_withdrawal: member.forwards_after_withdrawal(),
                refusals: member.refusals(),
                usage_records: live.map_or_else(
                    || usage.get(member.id.as_str()).copied().unwrap_or_default(),
                    |replica| replica.process.usage_records().len() as u64,
                ),
                retired: live.is_none(),
            }
        })
        .collect()
}

fn ledger(harness: &Harness, expected: u64, records: &[Value]) -> LossLedger {
    let mut by_status: BTreeMap<String, u64> = BTreeMap::new();
    for record in records {
        let status = record["status"].as_str().unwrap_or("unknown").to_owned();
        *by_status.entry(status).or_default() += 1;
    }
    let observed = records.len() as u64;
    let distinct: BTreeSet<&str> = records
        .iter()
        .filter_map(|record| record["request_id"].as_str())
        .collect();

    // Usage is reconciled per replica, against the caller requests the balancer
    // attempted on it. A caller request is one identity however many replicas
    // it touched: the replica that answered it owes exactly one record, and a
    // replica that refused it mid-drain may hold one for the work it had
    // already begun. Because the comparison is made replica by replica, a
    // duplicate one replica wrote cannot fill the hole another replica's lost
    // record left — which a fleet-wide count of records would let it do.
    let callers = harness.ingress.state.callers();
    let per_replica = reconcile(
        &callers,
        &harness.pinned_expectations,
        &harness
            .fleet
            .usage_records_by_replica()
            .into_iter()
            .map(|(id, rows)| (id, rows.len() as u64))
            .collect(),
    );
    let duplicates = per_replica.iter().map(|row| row.retry_duplicates).sum();
    let refusals_retried = per_replica
        .iter()
        .map(|row| row.caller_requests_refused_while_draining)
        .sum();
    LossLedger {
        caller_requests: callers.len() as u64,
        usage_records_missing: per_replica.iter().map(|row| row.missing).sum(),
        usage_records_surplus: per_replica.iter().map(|row| row.unexplained_surplus).sum(),
        per_replica,
        offered: harness.traffic.iter().map(|phase| phase.offered).sum(),
        answered: harness.traffic.iter().map(|phase| phase.answered).sum(),
        errors: harness.traffic.iter().map(|phase| phase.errors).sum(),
        unanswered: harness.traffic.iter().map(|phase| phase.unanswered).sum(),
        torn_streams: harness.traffic.iter().map(|phase| phase.torn_streams).sum(),
        unavailable: harness.ingress.state.unavailable(),
        usage_records_expected: expected,
        usage_records_observed: observed,
        usage_records_distinct: distinct.len() as u64,
        usage_records_retry_duplicates: duplicates,
        refusals_retried,
        usage_by_status: by_status,
        upstream_streams_open_at_end: harness.fleet.upstream.state.open_streams(),
    }
}

/// Reconcile usage replica by replica: what each replica owes for the caller
/// requests it answered against what it actually wrote.
///
/// A caller request is one identity however many replicas it was attempted on,
/// so the replica that answered owes exactly one record for it. A replica that
/// refused it mid-drain may also hold one — it settles the work it had already
/// begun — and that is the only thing that entitles a replica to a record
/// beyond what it answered. Doing this per replica is the point: a fleet-wide
/// count lets a duplicate one replica wrote stand in for the record another
/// replica lost, and then a loss of one and a duplicate of one reads as a clean
/// run.
pub fn reconcile(
    callers: &[CallerRequest],
    pinned: &BTreeMap<String, u64>,
    records: &BTreeMap<String, u64>,
) -> Vec<ReplicaUsage> {
    let mut answered: BTreeMap<&str, u64> = BTreeMap::new();
    let mut refused: BTreeMap<&str, u64> = BTreeMap::new();
    for caller in callers {
        if let Some(attempt) = caller.answered_by() {
            *answered.entry(attempt.replica.as_str()).or_default() += 1;
        }
        for replica in caller.draining_refusals() {
            *refused.entry(replica).or_default() += 1;
        }
    }
    let ids: BTreeSet<&str> = answered
        .keys()
        .copied()
        .chain(pinned.keys().map(String::as_str))
        .chain(records.keys().map(String::as_str))
        .collect();
    ids.into_iter()
        .map(|id| {
            let owed = answered.get(id).copied().unwrap_or_default()
                + pinned.get(id).copied().unwrap_or_default();
            let wrote = records.get(id).copied().unwrap_or_default();
            let refused_here = refused.get(id).copied().unwrap_or_default();
            let over = wrote.saturating_sub(owed);
            ReplicaUsage {
                replica: id.to_owned(),
                caller_requests_answered: owed,
                usage_records: wrote,
                caller_requests_refused_while_draining: refused_here,
                retry_duplicates: over.min(refused_here),
                missing: owed.saturating_sub(wrote),
                unexplained_surplus: over.saturating_sub(refused_here),
            }
        })
        .collect()
}

/// What the rollout cost in throughput while the fleet was short a replica.
/// Recorded, never asserted: it is the number a surge is sized from.
fn envelope(traffic: &[PhaseTraffic]) -> CapacityEnvelope {
    let mean = |select: fn(&PhaseTraffic) -> bool| {
        let phases: Vec<&PhaseTraffic> = traffic.iter().filter(|phase| select(phase)).collect();
        if phases.is_empty() {
            return (0.0, None);
        }
        let rps = phases.iter().map(|phase| phase.answered_rps).sum::<f64>() / phases.len() as f64;
        let p95 = phases
            .iter()
            .filter_map(|phase| phase.latency_ms.map(|latency| latency.p95))
            .fold(None::<f64>, |worst, p95| {
                Some(worst.map_or(p95, |w| w.max(p95)))
            });
        (rps, p95)
    };
    let (steady, steady_p95) = mean(|phase| phase.phase.starts_with("steady"));
    // `contains`, not `starts_with`: the rollback's drain is a replica short of
    // the fleet too, and leaving `rollback-drain` out would average the cost of
    // a rollout over two of the three windows that have it.
    let (degraded, degraded_p95) = mean(|phase| phase.phase.contains("drain"));
    CapacityEnvelope {
        steady_answered_rps: steady,
        degraded_answered_rps: degraded,
        degraded_fraction: if steady > 0.0 { degraded / steady } else { 0.0 },
        steady_latency_p95_ms: steady_p95,
        degraded_latency_p95_ms: degraded_p95,
    }
}

/// The manifest's thresholds, applied to what was measured.
fn verdicts(result: &RolloutResult) -> Vec<Verdict> {
    let thresholds = &result.scenario.thresholds;
    let drains = &result.drains;
    let worst = |select: fn(&DrainRecord) -> Option<u128>| {
        drains.iter().filter_map(select).max().unwrap_or_default() as f64
    };
    let mut verdicts = vec![
        Verdict::at_most(
            "max_requests_to_drained_replica",
            drains
                .iter()
                .map(|drain| {
                    drain
                        .requests_after_withdrawal
                        .max(drain.dispatches_beyond_drain_grace)
                })
                .max()
                .unwrap_or_default() as f64,
            thresholds.max_requests_to_drained_replica as f64,
        ),
        Verdict::at_most(
            "max_request_loss",
            (result.loss.unanswered + result.loss.errors + result.loss.torn_streams) as f64,
            thresholds.max_request_loss as f64,
        ),
        Verdict::at_most(
            "max_unavailable_responses",
            result.loss.unavailable as f64,
            thresholds.max_unavailable_responses as f64,
        ),
        Verdict::at_most(
            "max_usage_record_loss",
            result.loss.usage_records_missing as f64,
            thresholds.max_usage_record_loss as f64,
        ),
        // Retry duplicates are already discounted from the count loss is
        // measured against, so anything still in surplus is double accounting.
        Verdict::at_most(
            "unexplained_usage_record_surplus",
            result.loss.usage_records_surplus as f64,
            0.0,
        ),
        // Records are identified by `request_id`, so a repeated one is the same
        // event billed twice rather than two requests.
        Verdict::at_most(
            "duplicate_usage_record_ids",
            (result.loss.usage_records_observed - result.loss.usage_records_distinct) as f64,
            0.0,
        ),
        // Every drain must have been *observed* to leave rotation. A drain with
        // no removal time is a balancer that never noticed, which a maximum over
        // the ones that did notice would hide.
        Verdict::at_most(
            "readiness_removal_observed",
            drains
                .iter()
                .filter(|drain| drain.readiness_removed_after_ms.is_none())
                .count() as f64,
            0.0,
        ),
        Verdict::at_most(
            "max_readiness_removal_ms",
            worst(|drain| drain.readiness_removed_after_ms),
            thresholds.max_readiness_removal_ms as f64,
        ),
        // How long a replacement took to start carrying traffic, not when in the
        // run it did so: the offset grows with every phase, the admission does
        // not.
        Verdict::at_most(
            "max_replacement_admission_ms",
            result
                .fleet
                .iter()
                .filter_map(|replica| replica.admission_took_ms)
                .max()
                .unwrap_or_default() as f64,
            thresholds.max_replacement_admission_ms as f64,
        ),
        Verdict::at_most(
            "bounded_termination",
            drains
                .iter()
                .filter(|drain| drain.exited_after_ms.is_none())
                .count() as f64,
            0.0,
        ),
        Verdict::at_most(
            "max_drain_exit_slack_ms",
            drains
                .iter()
                .filter_map(|drain| {
                    Some(drain.exited_after_ms?.saturating_sub(drain.exit_budget_ms))
                })
                .max()
                .unwrap_or_default() as f64,
            thresholds.max_drain_exit_slack_ms as f64,
        ),
        Verdict::at_least(
            "min_mixed_version_requests",
            result
                .mixed_version
                .previous_requests
                .min(result.mixed_version.next_requests) as f64,
            thresholds.min_mixed_version_requests as f64,
        ),
        // The mixed-version rule itself: during the window, the capability only
        // the incoming revision has is served by it and refused by the other.
        Verdict::at_most(
            "mixed_version_capability_split",
            f64::from(
                !(result.mixed_version.next_serves_exclusive_alias
                    && result.mixed_version.previous_refuses_exclusive_alias),
            ),
            0.0,
        ),
        // A buffered request the replica admitted before the signal is finished
        // rather than dropped.
        Verdict::at_most(
            "buffered_requests_completed_during_drain",
            drains
                .iter()
                .filter(|drain| {
                    !drain
                        .buffered_in_flight
                        .status
                        .is_some_and(|status| (200..300).contains(&status))
                })
                .count() as f64,
            0.0,
        ),
        // A stream the upstream never ends is ended by the deadline, and only
        // after it had relayed something: a stream cut before any byte is a
        // different failure with the same shape.
        Verdict::at_most(
            "streams_cut_within_deadline",
            drains
                .iter()
                .filter(|drain| {
                    !drain.stream_in_flight.within_deadline
                        || drain.stream_in_flight.relayed_bytes == 0
                })
                .count() as f64,
            0.0,
        ),
        // The partial stream is accounted for, and accounted for as partial.
        Verdict::at_most(
            "partial_streams_accounted",
            drains
                .iter()
                .filter(|drain| {
                    drain.stream_in_flight.usage_status.as_deref() != Some("client_cancelled")
                })
                .count() as f64,
            0.0,
        ),
        Verdict::at_most(
            "upstream_streams_open_at_end",
            result.loss.upstream_streams_open_at_end as f64,
            0.0,
        ),
        Verdict::at_most(
            "migration_gate_passed",
            f64::from(!result.migration.gate_passed),
            0.0,
        ),
        Verdict::at_most(
            "compatible_rollback_serves_traffic",
            f64::from(!result.rollback.compatible_patch_rollback.served_traffic),
            0.0,
        ),
    ];
    // The fence is a gate only where it could be evaluated; an artifact from a
    // runner with no PostgreSQL says it was skipped rather than passing it.
    if result.rollback.migrated_layout_fence.evaluated {
        verdicts.push(Verdict::at_most(
            "migrated_layout_rollback_refused",
            f64::from(
                !(result.rollback.migrated_layout_fence.refused
                    && result
                        .rollback
                        .migrated_layout_fence
                        .refusal_names_newer_build),
            ),
            0.0,
        ));
    }
    verdicts
}

/// The usage status the replica settled a pinned alias' request as.
fn usage_status(drained: &Drained, alias: &str) -> Option<String> {
    drained
        .usage_records
        .iter()
        .find(|record| record["model"] == alias)
        .and_then(|record| record["status"].as_str())
        .map(ToOwned::to_owned)
}

fn balancer_counts(forwards: &[Forward]) -> BTreeMap<String, u64> {
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    for forward in forwards {
        *counts.entry(forward.replica.clone()).or_default() += 1;
    }
    counts
}

fn summary_of(traffic: &PhaseTraffic) -> String {
    format!(
        "{} offered, {} answered, {} errors, {} unanswered, {} retried, across {:?}",
        traffic.offered,
        traffic.answered,
        traffic.errors,
        traffic.unanswered,
        traffic.retried,
        traffic.by_replica,
    )
}

fn rate(count: u64, elapsed: Duration) -> f64 {
    let seconds = elapsed.as_secs_f64();
    if seconds > 0.0 {
        count as f64 / seconds
    } else {
        0.0
    }
}

fn verdict_word(passed: bool) -> &'static str {
    if passed { "passed" } else { "FAILED" }
}

/// A directory of this run's own, under the target directory rather than the
/// system temp: an operator reading a failed run wants the config it checked.
fn scratch_dir(name: &str) -> PathBuf {
    let dir = crate::support::capacity::manifest::workspace_root()
        .join("target/rollout/scratch")
        .join(format!("{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("the scratch directory is writable");
    dir
}

/// Write a config the operator commands will read. Mode 0600, because
/// `axond check preflight` fails a config another account can rewrite — the
/// harness has to satisfy the gate it is qualifying.
fn write_config(dir: &Path, name: &str, text: &str) -> String {
    let path = dir.join(name);
    std::fs::write(&path, text).expect("the config is written");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("the config is owner-only");
    }
    path.display().to_string()
}

/// The run's chronological record.
struct Timeline {
    started: Instant,
    events: Vec<Event>,
}

impl Timeline {
    fn new(started: Instant) -> Self {
        Self {
            started,
            events: Vec::new(),
        }
    }

    fn at(&mut self, phase: &str, kind: &str, detail: impl Into<String>) {
        self.events.push(Event {
            at_ms: self.started.elapsed().as_millis(),
            phase: phase.to_owned(),
            kind: kind.to_owned(),
            detail: detail.into(),
        });
    }
}
