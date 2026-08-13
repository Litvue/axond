# Stateful endurance qualification

What a *deployment* looks like after hours of mixed traffic, rather than what
one process looks like. [Endurance qualification](./endurance.md) soaks a single
Tier 0 replica with no datastore and no control plane
([ADR 0040](../adr/0040-endurance-qualification-harness.md)); this page soaks a
fleet whose catalogue, credential pool, tenant policy, provider, usage database,
and processes all change while it is serving
([ADR 0053](../adr/0053-stateful-endurance-qualification.md)) — the failures that
only appear when duration and change happen at once:

- an accounting row lost to a restart, a rotation, or a database that went away
  and came back;
- a revision that converged on one replica and not on the one that replaced it;
- a tenant that keeps reaching a pool its policy no longer lets it borrow;
- a circuit that opens for a declared outage and never closes again.

Where this sits in what production qualification has and has not measured — the
soak tier has not been dispatched — is the
[qualification packet](./qualification.md).

## What the harness runs

`qualification/stateful-endurance/manifest.toml` is the committed input: one
profile, `mixed-stateful-endurance`, at two tiers. The driver boots two real
`axond` replicas against a real PostgreSQL usage sink and a deterministic fake
upstream reached through a loopback fault gate, puts a round-robin balancer in
front of them, and offers a closed-loop mixed workload drawn from a seeded
rotation.

Every request is a point in four dimensions, as in the stateless soak:

| Dimension | Values |
| --- | --- |
| Tenant | `platform` (operator credentials), `stateful-byok` (its own), `stateful-fallback` (none, `allow_platform_fallback`), and `stateful-probe`, which only probes tenant policy. |
| Provider and route | `fake-openai` over `/v1/chat/completions`, `/v1/embeddings`, `/v1/responses`; `fake-anthropic` over `/v1/messages`. |
| Wire | Buffered and streamed, interleaved. |
| Ending | `complete`, `cancelled`, `dropped` (the upstream dies mid-stream), `faulted` (it refuses first). |

What makes this run stateful is the script offered *underneath* that workload.
Each event is a fraction of the run, so both tiers execute the same script in
the same order:

| At | Event | What the run then looks for |
| --- | --- | --- |
| 6% | Catalogue revision — a new alias is published and every replica reloaded | the `chat-catalogue-v2` alias begins serving *on every replica* |
| 11% | Credential revision — the pool is rotated | a usage record attributed to `fake-openai-rotated` |
| 16% | Policy revision — the probe tenant loses `allow_platform_fallback` | every replica stops serving the probe tenant |
| 20% | The provider is slowed by 250 ms at the gate | latency moves; nothing fails |
| 28% | The provider is taken away — connections refused and cut | refusals typed as circuit-open, and recovery afterwards |
| 52% | The usage database is taken away | dropped sink batches, reported by the process and reconciled |
| 72% | Rolling restart — each replica is drained and replaced one at a time | no request refused for want of a ready replica, the flushed rows arrive, and the run keeps offering load afterwards |

The gaps between them are sized for the *shorter* tier. Attribution runs past
the end of a fault by `recovery_allowance_ms`, which is an absolute duration
while every offset is a fraction, so a gap comfortable over twelve hours can be
nothing at ninety seconds — and a smoke tier that restarted the fleet inside the
database outage's attribution window would be excusing errors the soak tier
counts. The database outage's window is widened *backwards* as well, by
`usage_outage_attribution_slack_ms`, so the provider outage has to clear it by
the allowance plus the slack rather than by the allowance alone. A test asserts
every one of those separations at both durations.

| Tier | Duration | Concurrency | Sample interval | Segment |
| --- | --- | --- | --- | --- |
| `smoke` | 90 s | 8 | 200 ms | 10 s |
| `soak` | 12 h | 24 | 1 s | 15 min |

The smoke tier is the same code, manifest, script, and gates as the soak; only
the time between the events differs.

## Run it

The harness needs a PostgreSQL it may create a schema in. Without
`AXOND_TEST_POSTGRES_DSN` the run is skipped rather than shortened — a stateful
qualification without a datastore is not a smaller one.

Point it at a database on this machine if you want the usage-backend outage
evaluated. The replicas reach a loopback database through the fault gate, which
forwards PostgreSQL bytes without terminating them, so loopback DSNs retain
their original host for certificate verification and their `sslmode` while
`hostaddr` points at the gate. This keeps both `prefer` and `require` TLS
connections intact while the database outage can still be introduced. A DSN
naming a host somewhere else is handed to replicas untouched: rewriting a remote
destination would hand its credentials to a local forwarder, so the artifact
records the backend as reached `direct` and the usage-backend outage as not
evaluated.

An outage that is not evaluated excuses nothing. A run against a directly
reached database judges the stretch the script reserved for the database outage
like any other: a lost accounting row there counts against
`max_durable_usage_loss_outside_windows`, a refusal counts against
`max_unplanned_errors`, and a silent usage stream is measured rather than
skipped. Only the upstream faults, which this harness does inject whatever the
database is, attribute anything.

```bash
export AXOND_TEST_POSTGRES_DSN=postgres://postgres:axond-ci@127.0.0.1:5432/postgres

# The deterministic checks. The 90-second smoke is opt-in and runs in its own CI
# lane so an ordinary stateful test invocation cannot spend its budget on load.
cargo test --locked --all-features --test stateful_endurance -- --nocapture

# The smoke tier, when explicitly requested.
AXOND_STATEFUL_ENDURANCE_SMOKE=1 cargo test --locked --all-features \
  --test stateful_endurance -- \
  the_stateful_endurance_smoke_tier_qualifies_and_publishes_its_evidence \
  --exact --nocapture --test-threads=1

# The soak tier: twelve hours, by name.
just stateful-endurance

# A shorter dispatched run — forty minutes here. The override applies to the
# soak tier alone, so the smoke tier in the same binary keeps its committed
# ninety seconds. Segments shrink to match.
just stateful-endurance 2400000
```

The `Endurance` workflow's second job runs this soak monthly and on dispatch,
against a PostgreSQL service container, and uploads the result with its time
series.

## What a run leaves behind

`target/stateful-endurance/<tier>/<profile>.json` is the result, and
`<profile>.replica-N.samples.jsonl` beside it is every resource sample each
replica — including the ones that were retired and replaced — was observed to
take, written as it went.

The result carries the measurements and the identity of everything that produced
them: the SHA-256 of the binary, of the normalised config, and of the manifest
and every fixture; the seed; the duration offered (`profile.duration_ms`) next
to the manifest's (`profile.manifest_duration_ms`) and which of the two was used
(`run.duration_source`); the PostgreSQL server version and the ephemeral schema
name; the toolchain, git commit and dirty flag; and the host's CPU, kernel, core
count and memory. **Numbers from artifacts whose provenance differs are not
comparable.**

It carries no credential. The normalised config replaces the ephemeral ports,
the per-run key directory, and the run's own usage schema — all three change
every run, and a fingerprint that changed with them would make every artifact
incomparable. Tenant keys are delivered as files under the run directory and
named rather than quoted, and the usage DSN travels as the *name*
of the environment variable the replicas read it from. Credential evidence is
label attribution — `fake-openai-rotated` — not material.

## What fails, and what does not

Hard failures, asserted at both tiers:

- **exactly one usage record per dispatched request** — none missing, none
  duplicated, none carrying a status the plan cannot account for;
- **no durable row lost outside a declared window**
  (`max_durable_usage_loss_outside_windows = 0`), where *outside* is decided by
  when the records were settled rather than by how many the processes reported
  dropping — see below;
- **no tenant boundary crossed** (`max_tenant_boundary_violations = 0`), which
  is also an early abort: the rest of a run that mixed two tenants' credentials
  measures nothing. Counted from the moment every replica has been *observed* to
  honour the policy revision, not from the moment it was published — a replica
  still reloading is a slow reload, and it fails the convergence bound rather
  than this one;
- **no error outside a declared fault window** (`max_unplanned_errors = 0`);
- **every published revision converged** within `max_convergence_ms`, observed
  by a request rather than by a log line;
- **no request refused for want of a ready replica** during the rolling restart
  (`max_restart_unavailable = 0`), and no readiness gap longer than
  `max_readiness_gap_ms`. The restart is scheduled early enough that the run
  keeps offering load after the last replacement joined, and
  `restart.offered_after_last_replacement` is asserted non-zero: a restart the
  load finished before is one an idle deployment would survive. A restart that
  ran long enough to reach the end of a short tier anyway does not turn that
  into a coin toss — the run offers for a little longer instead, and says so on
  `restart.extended_for_load_ms`;
- **every declared fault recovered** within `max_recovery_ms`, measured from the
  moment the fault is lifted to the first request that settles a usage record
  afterwards. A window with no such request never recovered, and fails;
- **bounded resident growth** (`max_rss_growth_kib`), over enough segments to
  fit a trend through (`min_segments`). The soak tier adds the per-hour drift
  gate a short run cannot support.

Recorded and **never** asserted: throughput, latency percentiles, TTFT, CPU. A
shared runner cannot bound them without flaking.

## Declared outages, and what they are allowed to cost

Two backends are never out at once, so every error can be attributed to one
window. Inside a window the errors are the point and are counted as
`workload.errors_in_fault_windows` rather than against the unplanned gate.

Attribution runs past the end of a fault by `recovery_allowance_ms`, because a
backend that goes away trips the circuit breakers in front of it and those have
a cooldown (`failover.cooldown_seconds`); a breaker that reopened the instant
the backend returned would be a breaker that never protected anything. What is
not allowed is never recovering, which is what `max_recovery_ms` bounds.

The usage-database outage is the interesting one. The durable sink batches,
reconnects, retries once, and then drops the batch rather than growing a queue
for a database that is not there ([ADR 0009](../adr/0009-durable-usage-sinks.md)).
A dropped batch is a logged event, and the harness keeps those events apart from
its bounded scrollback: `usage.sink_drops` records how many batches were
dropped, how many records they held, why, and whether each fell inside the
declared window.

Which half of the loss the outage excuses is a question about *when*, so it is
answered on the window both sides can be counted over rather than by comparing
magnitudes. `usage.settled_outside_usage_window` is what the processes settled
outside the outage — the driver's count, less every duplicate the run saw, since
nothing says which side of the window a second copy arrived on.
`usage.durable_outside_usage_window` is what the database holds outside it, by
the `recorded_at` the gateway stamped when the record was produced rather than
when the batch flushed. The difference is `durable_loss_outside_windows`, and it
is gated at zero; the rest of the whole-run loss is `durable_loss_in_window`,
which the outage accounts for. A row lost at a safe moment therefore stays a
finding however many batches the sink reported dropping while the backend was
gone. The edge between the two is carried one drain interval past the declared
close, so a record the driver sees a tick late is not read as a safe-time loss.

Being inside the window is not by itself an excuse. `durable_loss_in_window` is
gated against `sink_drops.records_in_usage_window` — what the processes
themselves said they lost — so a run that dropped one record and lost a thousand
fails. The single allowance is the buffer-full report, which the gateway samples
rather than writes in full: where the in-window drops were reported that way,
`sink_drops.sampled_records_in_usage_window` is non-zero and the bound is
widened by one sampling interval, the most the tail below the next report can
hide. Where nothing was reported, nothing is excused.

## Reading an artifact

```bash
jq '{tier: .profile.tier, offered: .workload.offered, usage: .usage,
     revisions: [.revisions[] | {event, converged_ms}],
     faults: [.faults[] | {event, errors_inside, recovered_ms}],
     restart: .restart, tenancy: .tenancy,
     failed: [.verdicts[] | select(.passed == false)]}' \
  target/stateful-endurance/soak/soak.json
```

Fields worth knowing:

- `run.stop` is why the run ended: `duration_elapsed` is the normal ending, and
  anything else names the abort condition that cut it short.
- `revisions[].converged_ms` is measured from publication to the first request
  that *observed* the new behaviour, so it includes the reload rather than
  reporting the signal.
- `faults[].gate` is what the loopback gate did — accepted, refused, cut,
  delayed — which is how a run shows the fault it declared actually met traffic.
- `usage.distinct` is what the *workload* settled and `usage.probe_distinct`
  what the driver's own boundary and convergence probes settled. Only the first
  is reconciled against `usage.owed`: the probes are the harness asking, nothing
  owed them, and counting them together would let a probe record stand in for a
  workload record the deployment lost. Both are rejoined where the database is
  compared, because the database holds them too.
- `usage.durable_loss_total` is every emitted record the database never
  received, split into the part the outage explains
  (`durable_loss_in_window`) and the part it does not
  (`durable_loss_outside_windows`, gated at zero), with the two counts the split
  was made from beside them. Both halves are gated: the first against what the
  fleet reported dropping, the second against nothing at all.
- `telemetry.worst_usage_silence_ms` is the longest stretch under load in which
  the fleet produced no accounting at all, excluding the declared outages. A
  gateway that keeps answering while its usage records stop is invisible in a
  total that only counts what arrived by the end.
- `restart.flushed_on_exit` counts the usage rows a retiring replica emitted
  while draining; they are reconciled with the rest, so a restart cannot hide a
  row by taking the process that owed it away.
- `revisions[].converged_ms` is `null` for a revision that was published and
  never observed, so an artifact fails on its own terms rather than by omitting
  the revision that did not land.
- `tenancy.probe_served_before_policy` and `probe_refused_after_policy` are the
  same probe on either side of the policy revision. Both must be non-zero, or
  the isolation gate passed without ever being tested.
- `resources[]` is per replica *incarnation*, so a rolling restart produces more
  entries than the fleet has replicas; `growth_kib` is against that
  incarnation's own baseline.
