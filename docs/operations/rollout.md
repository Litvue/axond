# Rollout qualification

Whether the rolling upgrade in [upgrades and rollback](./upgrades.md) actually
holds, executed against a fleet of real Axond processes behind a real
readiness-driven load balancer. The design and its boundaries are
[ADR 0037](../adr/0037-rollout-qualification-harness.md); the per-replica
envelope the fleet is sized from is
[capacity qualification](./capacity.md).

This page qualifies the **deployment sequence**, not a second build. A revision
here is the (binary, config) pair a process was started from; the incoming one
serves a model alias the outgoing one has never heard of, which is how a
mixed-version window is proven to be mixed. Nothing here compares two
compilations.

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
3. **Rolling replacement**, once per outgoing replica: admit a next-revision
   replica, serve a mixed phase, probe the incoming revision's exclusive alias
   against both revisions, pin a buffered request and an unending stream to the
   victim, confirm both reached the upstream, `SIGTERM` the victim, and watch the
   balancer's withdrawal and the child's exit while traffic keeps flowing.
4. **Rollback.** A previous-revision replica is admitted and a next-revision
   replica drained — the compatible patch rollback the guide permits — and the
   older build then serves a phase.
5. **Fence.** Against a real PostgreSQL, a migrated control plane is given a
   ledger entry only a newer build could have written, and this build's refusal
   to serve it is recorded.

Traffic throughout is buffered and streamed, offered concurrently through the
balancer, with the replica and revision that served each request recorded from
the response headers the balancer stamps.

## What a run gates on

Hard failures — all of them environment-independent:

| Threshold | Meaning |
| --- | --- |
| `max_requests_to_drained_replica` | The balancer never routes to a replica after it has seen it withdraw. |
| `max_request_loss` / `max_unavailable_responses` | Every offered request is answered; no `503` during a replacement. |
| `max_usage_record_loss` | One usage record per request, all flushed before the process exits. |
| `max_readiness_removal_ms` | How long after `SIGTERM` the balancer still considers the replica ready. |
| `max_replacement_admission_ms` | How long a new replica takes from boot to carrying traffic. |
| `max_drain_exit_slack_ms` | How far past `drain_grace_ms + deadline_ms + flush_timeout_ms` a termination may run. |
| `min_mixed_version_requests` | The mixed-version window genuinely served both revisions. |

Also asserted: the pinned buffered request finished after the signal rather than
being dropped, the pinned stream was cut inside the shutdown deadline and
accounted for as partial (`client_cancelled`), no upstream stream was left open,
the operator gate passed, and the rolled-back replica served traffic.

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
- `loss` — the offered/answered ledger and usage records by status.
- `migration` / `rollback` — the operator commands as an operator saw them, and
  both rollback decisions.
- `timeline` — every rollout event in the order it happened.

No DSN or secret value appears in an artifact; configs name environment
variables, never their contents.

## Run it

```bash
# The reduced tier. Part of the normal suite, and of CI.
cargo test --locked --all-features --test rollout -- --nocapture

# The heavy tier. Same driver, manifest, and assertions at a larger scale.
AXOND_ROLLOUT=1 cargo test --locked --all-features --test rollout -- \
  --nocapture --test-threads=1
```

The forward-only rollback fence needs PostgreSQL and is skipped — recorded in the
artifact as `evaluated: false` with a reason — when the DSN is unset:

```bash
docker run -d --name axond-test-postgres -e POSTGRES_PASSWORD=axond-ci \
  -p 55432:5432 postgres:17.6-alpine
AXOND_TEST_POSTGRES_DSN=postgres://postgres:axond-ci@127.0.0.1:55432/postgres \
  cargo test --locked --all-features --test rollout -- --nocapture
```

The `Rollout` workflow runs the heavy tier on dispatch and weekly with the
database attached, and uploads the artifacts; the CI `tests` lane uploads the
reduced ones.

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
- A wire-format or binary-level incompatibility between two builds.
- Stateful serving or revision convergence during a rollout. The fleet is
  config-only; the control plane appears here only as the rollback fence.
