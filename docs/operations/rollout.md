# Rollout qualification

Whether the rolling upgrade in [upgrades and rollback](./upgrades.md) actually
holds, executed against a fleet of real Axond processes behind a real
readiness-driven load balancer. The design and its boundaries are
[ADR 0041](../adr/0041-rollout-qualification-harness.md); the per-replica
envelope the fleet is sized from is
[capacity qualification](./capacity.md).

The promotable heavy lane qualifies the deployment sequence with two verified
builds: the retained release executable and the candidate executable. A revision
is the (binary, config) pair a process was started from; every executable and
normalised config is recorded by SHA-256. The always-on reduced lane uses the
candidate for both sides and is explicitly diagnostic, never promotable.

## What the harness runs

`qualification/rollout/manifest.toml` is the committed input: replica count, a
reduced and a heavy scale, the shutdown bounds every replica is started with, and
the thresholds a run is gated on.

The `rolling-replace` scenario, in order:

1. **Operator gate.** `axond check preflight` and `axond migrate status` are run
   as subprocesses against the incoming revision's own config, before any replica
   boots. Either failing stops the rollout.
2. **Steady state.** Two previous-revision replicas are admitted to the balancer
   after their `/readyz` probes pass, and serve a full traffic phase.
3. **Previous-config compatibility.** Admit the candidate executable with the
   previous revision's unchanged config, serve traffic, and drain that canary.
4. **Rolling replacement**, once per outgoing replica: admit a next-revision
   replica, serve a mixed phase, probe the incoming revision's exclusive alias
   against both revisions, pin a buffered request and an unending stream to the
   victim, confirm both reached the upstream, `SIGTERM` the victim, and watch the
   balancer's withdrawal and the child's exit while traffic keeps flowing.
5. **Migration matrix.** On a private PostgreSQL schema, apply and inspect the
   retained release's migrations, inspect and apply the candidate's migrations,
   then run the retained binary's status command against the resulting ledger.
   The exact ledger rows classify the transition as `unchanged` or
   `forward-only`.
6. **Rollback.** For `unchanged`, admit the retained binary with its retained
   config, drain a candidate, and require the retained binary to serve traffic.
   For `forward-only`, require the retained binary to refuse the candidate layout
   and do not start it as a replica.

Traffic throughout is buffered and streamed, offered concurrently through the
balancer, with the replica and revision that served each request recorded from
the response headers the balancer stamps.

The harness also starts a dedicated loopback OTLP/HTTP receiver for every
replica. This is required for the retained v0.3.40 executable:
that release copied an inbound W3C trace into a usage row only while an
OpenTelemetry provider was active. The receiver lets the qualification keep the
strong contract—every retained and candidate row is joined to the exact caller
trace—rather than weakening loss detection to a count. A passing artifact names
every exact-trace replica and records every exact caller trace decoded by that
replica's receiver. A receiver refuses a different process identity, and the
promoter requires the exported trace set to equal the complete caller trace
ledger: usage-bearing requests plus explicitly reasoned capability and typed
drain refusals that deliberately owe no usage row. The harness waits for
caller-domain activity to remain quiet across five exporter intervals and
judges that exact settled snapshot, so a duplicate span resets the window and a
delayed extra trace cannot arrive after the gate has already taken its evidence.
Typed drain exemptions retain the exact ingress attempt and the replica that
subsequently accepted the same trace. Untyped 503 and transport-failure attempts
are retained separately to attribute matching surplus spans without exempting
them.

## What a run gates on

Hard failures — all of them environment-independent:

| Threshold | Meaning |
| --- | --- |
| `max_requests_to_drained_replica` | The balancer never routes to a replica after it has seen it withdraw. Taken from two witnesses: the flag selection carried, and the logged dispatch instants recompared with the recorded withdrawal instant. |
| `max_request_loss` / `max_unavailable_responses` | Every offered request is answered; no `503` during a replacement. |
| `max_usage_record_loss` | One usage record per request, all flushed before the process exits. A record left behind by a refusal the balancer retried is discounted before the comparison, so a duplicate cannot fill a lost record's place. |
| `unexplained_usage_record_surplus` / `duplicate_usage_record_ids` | No record beyond what a caller request explains, and no `request_id` recorded twice. |
| `otlp_trace_context_exported` | Every replica-dedicated receiver observed at least one caller-domain trace, proving the qualification instrumentation was exercised by the full fleet. Readiness-only batches cannot satisfy this gate. |
| `otlp_trace_export_identity_mismatches` | The settled exact caller traces decoded by the replica-dedicated receivers equal the complete caller trace ledger. Usage-bearing identities and exact retried drain-attempt evidence are disclosed separately, so neither a missing trace, substituted exemption, delayed arrival, nor unexplained extra trace can hide. A span from an untyped `503` or transport failure remains a mismatch even when a transport retry later succeeds; it is never converted into a passing exemption. |
| `max_readiness_removal_ms` | How long after `SIGTERM` the balancer still considers the replica ready. |
| `max_replacement_admission_ms` | How long a new replica takes from boot to carrying traffic. |
| `max_drain_exit_slack_ms` | How far past `drain_grace_ms + deadline_ms + flush_timeout_ms` a termination may run. |
| `max_stream_cut_observation_slack_ms` | External timing allowance between the harness recording that it will send `SIGTERM` and the process observing it. This does not extend the configured request deadline or termination budget. |
| `min_mixed_version_requests` | The mixed-version window genuinely served both revisions. |

Also asserted: the pinned buffered request finished after the signal rather than
being dropped, the pinned stream was cut inside the shutdown deadline and
accounted for as partial (`client_cancelled`), no upstream stream was left open,
the operator gate passed, two distinct executable digests served in the heavy
lane, the candidate served the retained config before candidate-only enablement,
the real migration matrix completed, the outgoing revision answered the
candidate-only alias with the exact typed `404 unknown_model` contract, and
rollback matched its classification. Stream cutoff uses drain grace plus request
deadline, with only the committed signal-observation allowance applied by the
external witness; the accounting flush timeout can extend process exit but
cannot extend caller work.

Throughput and latency are recorded and never asserted, for the reason
[ADR 0033](../adr/0033-capacity-qualification-harness.md) gives: a shared runner
moves them, and a flaky rollout gate is one that gets disabled.

## The artifact

Each run writes `target/rollout/<tier>/<scenario>.json`, carrying the same
provenance block as a capacity artifact — SHA-256 of the binary, each revision's
normalised config, the manifest, toolchain, git commit and dirty flag, host CPU,
kernel, cores, and memory — plus:

- `fleet` — every replica, its revision, when the balancer admitted it, when it
  withdrew it, and how much traffic it took.
- `traffic` — per phase: offered, answered, errors, retries, and the split across
  replicas.
- `drains` — per drain: readiness removal, exit time against the budget, requests
  after withdrawal, the pinned buffered request's status, the pinned stream's cut
  time and relayed bytes, and usage records flushed.
- `mixed_version` — the capability probe and the request counts per revision.
- `loss` — the offered/answered ledger, exact caller/sink identities, usage
  records by status, and the loopback OTLP trace-context witness.
- `migration` / `rollback` — the operator commands as an operator saw them, and
  both rollback decisions.
- `timeline` — every rollout event in the order it happened.

No DSN or secret value appears in an artifact; configs name environment
variables, never their contents.
The compact observation preserves both executable versions and SHA-256 digests
and the retained archive digest in addition to binding this raw JSON, so the
release pin remains reviewable after disposable workflow artifacts expire.

## Run it

```bash
# The reduced tier. Part of the normal suite, and of CI.
cargo test --locked --all-features --test rollout -- --nocapture

# The promotable heavy tier. The previous path must be a verified release asset.
AXOND_ROLLOUT=1 \
AXOND_ROLLOUT_PREVIOUS_BINARY=/path/to/verified/v0.3.40/axond \
AXOND_ROLLOUT_EXPECTED_PREVIOUS_VERSION=0.3.40 \
AXOND_ROLLOUT_EXPECTED_PREVIOUS_SHA256=<extracted-binary-sha256> \
AXOND_ROLLOUT_RETAINED_ARCHIVE_SHA256=<verified-release-archive-sha256> \
AXOND_TEST_POSTGRES_DSN=postgres://postgres:axond-ci@127.0.0.1:55432/postgres \
  cargo test --release --locked --all-features --test rollout -- \
  --nocapture --test-threads=1
```

The reduced diagnostic can attach PostgreSQL to exercise the matrix, but remains
non-promotable because it does not require distinct binaries:

```bash
docker run -d --name axond-test-postgres -e POSTGRES_PASSWORD=axond-ci \
  -p 55432:5432 postgres:17.6-alpine
AXOND_TEST_POSTGRES_DSN=postgres://postgres:axond-ci@127.0.0.1:55432/postgres \
  cargo test --locked --all-features --test rollout -- --nocapture
```

The `Rollout` workflow pins v0.3.40's Linux archive digest, verifies its release
checksum, records the extracted executable digest, runs the release-profile
heavy tier with PostgreSQL attached, independently promotes the raw artifacts,
and uploads only the validated compact record. The CI `tests` lane uploads
non-promotable reduced diagnostics.

## Reading a failed run

The failure prints every threshold that was missed followed by the whole
timeline, because a rollout that broke two invariants is not diagnosed by the
first one. Start at the drain whose phase the timeline names, then read that
drain's record in the artifact: readiness removal, requests after withdrawal, and
the pinned request outcomes distinguish a balancer problem from a shutdown
problem.

## What this does not qualify

- A production load balancer's own behaviour. The fixture is a representative
  readiness-driven proxy — round-robin, one retry onto another member, no outlier
  ejection.
- Arbitrary control-plane mutation or convergence while a rollout is in
  progress. The heavy lane does qualify both binaries against one shared,
  durable serving revision; mutation drills remain part of the separate
  stateful endurance and recovery slices.
