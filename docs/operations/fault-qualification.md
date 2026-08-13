# Fault qualification

How a replica behaves when the provider, the network to it, or a state tier
fails — and the evidence that says so. This page is the fault half of the
production qualification programme; the load half is
[Capacity qualification](./capacity.md).

The two are deliberately separate. A capacity profile qualifies a healthy
replica and treats any error as a finding. A fault row expects the failure and
qualifies its *shape*: what the caller is told, which bound ended the request,
what the retry cost, whether the upstream was released, what was charged, what
was counted, and that nothing about the provider or the datastore leaked into a
surface a caller or an operator can read.

## What the matrix covers

`qualification/faults/manifest.toml` is the committed input; each row names one
injected fault and the properties it must produce.

| Family | Rows |
| --- | --- |
| Provider verdicts | `429` and `5xx`, each with and without a standby target to fail over to. |
| The path to the provider | Unresolvable DNS, refused connect, a TLS handshake answered with garbage, headers that never arrive, a buffered body that never finishes. |
| Streams | Idle before the first provider event, idle after output is committed, truncated mid-event. |
| Bounded bodies | An oversized success body and an oversized provider *error* body. |
| State tiers | Redis and Postgres under injected latency, outage under `on_unavailable = "deny"`, outage under `on_unavailable = "allow"`, and recovery without a restart. |

Every fault is injected locally and deterministically: a fake provider on
loopback, a TCP listener that never speaks TLS, a closed port, and — for the
state tiers — a TCP fault proxy in front of a real Redis or Postgres that can
delay bytes, refuse connects, and sever the connections a pool is already
holding.

No provider credential, no provider network, and no control plane is involved.
Control-plane outage, revision convergence, rollout, and the long soak are the
remaining parts of the programme and are **not** qualified here.

## What each row retains

Each row writes `target/faults/<family>/<row>.json`, carrying:

| Section | What it is evidence of |
| --- | --- |
| `injection` | The fault, how it was injected, the delay or outage window, and when the request began. |
| `classification` | The status, typed error, phase, how many bytes of *provider* output were relayed before the failure — the gateway's own in-band error event does not count — and, for a recovery row, what the tier answered while it was down. |
| `deadline` | The bound the row is ended by, its configured value, and the elapsed time against the row's ceiling. |
| `retries` | Attempts spent, dispatches the provider saw, and the configured `max_attempts`. |
| `cleanup` | Upstream responses opened and still open when the caller was gone, how long the release took, and whether the process exited cleanly on `SIGTERM`. A stalled buffered attempt is counted as well as a stream, so a header- or body-bound row proves the abandoned response was released rather than reporting a zero nothing ever contributed to. |
| `usage` | Records settled by the measured request, their statuses, the cost, and whether the record carries a request id. |
| `telemetry` | Exports received by a real OTLP collector, the instruments and spans observed, and any the row named and did not get. |
| `leakage` | Every surface scanned — caller response, usage records, process output, OTLP payloads — with byte counts and any finding. |
| `verdicts` | Every check, with expected and observed, so a passing row is auditable and a failing row is diagnosable from the artifact alone. |

The leakage scan looks for the provider base URL and endpoint, the upstream
credentials, the inbound gateway key, and the datastore DSN and its authority.
The needle *values* never enter the artifact: a finding names the surface and a
label.

Milliseconds are asserted only as ceilings (`deadline_ms`). Everything a row
fails on is a property that does not move with the machine.

## Run it

```bash
# Provider and transport rows. No datastore needed.
cargo test --locked --all-features --test faults -- --nocapture

# All rows, including the state tiers.
AXOND_TEST_REDIS_URL=redis://127.0.0.1:6379 \
AXOND_TEST_POSTGRES_DSN=postgres://postgres:axond-ci@127.0.0.1:5432/postgres \
  cargo test --locked --all-features --test faults -- --nocapture
```

Or `just faults`, which supplies both.

State-tier rows skip when their connection string is unset and **fail** when
`AXOND_TEST_REQUIRE_SERVICES=1`, which the CI stateful lane sets — so a skipped
backend row there is a failure, not a quiet gap. The rows run one at a time on
purpose: each boots its own process, and a matrix that shared a replica between
rows could not attribute what it recorded to the fault it injected.

## Reading a result

A row's `verdicts` are the gate; the rest is context. To compare two artifacts,
compare their `environment` first — the binary, config, manifest, and fixture
hashes, plus toolchain and host. Results whose provenance differs are not
comparable. The config hash is taken over the config with its per-run values
replaced by placeholders — the gateway and fake-upstream ports, the injector
ports the transport rows point a provider at, and the run-scoped key prefix a
state-tier row keeps its keys under — so two runs of the same row on the same
build agree, and a real change of the row's wiring still does not.

Some expectations are worth knowing before reading one:

- A failed-over request settles as a success, so `axond.upstream.errors` is not
  counted for it. The retry shows up as `attempts = 2` on the usage record.
- A stream that fails after its head is committed cannot be a status. It ends as
  an in-band SSE error with `status = 200` and settles as `upstream_error`.
- A fail-closed state tier refuses before any dispatch: no attempt, no upstream
  request, and no usage record.

## See also

- [Capacity qualification](./capacity.md) — the load half of the programme.
- [Observability and runbook](../observability.md) — the instruments and spans
  the rows assert on.
- [Troubleshooting](./troubleshooting.md) — the same typed errors, from the
  operator's side.
