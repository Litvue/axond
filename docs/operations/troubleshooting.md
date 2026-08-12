# Troubleshooting

Axond fails closed and returns typed JSON errors. Start with the process log,
HTTP status, and `error.type`; do not infer the cause from status alone.

## The process never listens

Axond validates configuration, resolves every declared secret reference, and
connects configured backends before binding the socket.

| Log message shape | Likely cause | Action |
| --- | --- | --- |
| `failed to load config` | Missing file, invalid TOML, invalid graph, or unsupported combination. | Check `AXOND_CONFIG`, then compare against the configuration reference. |
| `references env var ... unset or empty` | A credential, gateway key, verifier, or DSN reference is absent from the process environment. | Set it on the actual service/container and restart. |
| `at least one gateway key` | No static `[[gateway_key]]` exists. | Add a breakglass key; minted verifiers are additive. |
| `hold the same secret` | Two gateway-key entries resolve to one value. | Give each principal a distinct value. |
| `usage sink configuration failed` | Postgres or OTLP configuration/connectivity failed. | Verify DSN/endpoint, DNS, TLS, schema, and credentials. |
| `budget configuration failed` | Budget backend unavailable or layout migration incomplete. | Restore the backend or complete the named migration. |
| `rate-limit configuration failed` | Redis is unavailable or invalid. | Verify URL, TLS, DNS, and connectivity. |
| `revocation configuration failed` | Redis/Postgres revocation store is unavailable or missing schema. | Apply DDL or restore connectivity. |

Boot errors name references and identifiers, not secret values.

## HTTP decision table

| Status / type | Meaning | First check |
| --- | --- | --- |
| `401 unauthorized` | No presented credential matched a static key or valid minted token. | Header value, key namespace, token audience/signature/expiry. |
| `403 token_scope_insufficient` | Auth succeeded but route/operator authority did not. | Token scope and whether the action requires the default-namespace static operator key. |
| `403 token_signer_not_permitted` | Verifier does not permit the token namespace. | Verifier `namespaces` and token `ns`. |
| `404 unknown_model` | Alias is absent or unavailable to the caller namespace. | `/v1/models`, credential coverage, alias spelling. |
| `400 unsupported_wire` | Route and alias provider family differ. | Keep every alias target in one wire family and use the matching route. |
| `400 bad_request` | Invalid request or query shape. | Error message; repeated/invalid `namespaces` values are rejected deliberately. |
| `429 budget_exceeded` | Subject or namespace spend cap cannot admit the estimate. | Budget metrics and namespace-denial metric. |
| `429 rate_limit_exceeded` | In-flight concurrency cap reached. | Caller concurrency and limiter metrics. |
| `502 upstream_transport` | Axond could not establish/complete the provider transport. | Provider URL, DNS, TLS, egress, proxy, timeout. |
| `502 invalid_request` | Provider returned a non-retryable request/auth error. | Provider credential, model deployment, and provider body. |
| `503 budget_unavailable` | Shared budget backend failed under fail-closed policy. | Redis/Postgres health and latency. |
| `503 rate_limit_unavailable` | Redis limiter failed under fail-closed policy. | Redis health, invoke saturation, connection recovery. |
| `503 revocation_unavailable` | JTI store failed under fail-closed policy. | Redis/Postgres health and configured policy. |
| `503 continuation_affinity_unavailable` | A request carrying `previous_response_id` cannot safely use its pinned first target or credential. | First-target circuit and first-credential state; retry later. |

All error bodies use `{"error":{"type":...,"message":...}}`.

## A `/v1/responses` request fails while chat on the same alias succeeds

Expected. Every Responses request — initial calls included — uses only the
alias's first target and first credential, so it neither fails over nor rotates;
chat on the same alias still walks the remaining targets and keys. This is what
keeps a response id continuable without gateway state
([ADR 0023](../adr/0023-openai-responses-passthrough.md)).

An **initial** Responses request reports the ordinary cause —
`503 all_provider_circuits_open` when the first target's circuit is open,
`no_credential` when the first credential is missing, or the upstream error
itself — because nothing was continued. Only a request with a non-empty
`previous_response_id` reports `continuation_affinity_unavailable`. Check the
first target and first credential of the alias; do not expect a later target to
absorb the failure, and do not reorder `targets` or the credential pool to route
around it, because that strands response ids created under the previous order.

## Health is green but requests fail

`/healthz` and `/readyz` report a serving process, not continuous provider or
datastore health. Backends were connected before boot, but may fail later.
Inspect typed errors, `axond.*.unavailable_denials`, upstream attempt spans, and
provider/network telemetry.

## Credential status

```bash
curl --fail \
  -H "Authorization: Bearer $GW_INBOUND_PLATFORM_KEY" \
  http://127.0.0.1:8080/v1/credentials
```

- `healthy`: available to selection.
- `parked`: recent provider `429`s crossed the credential threshold.
- `probe`: cooldown elapsed; the next real request may test it.

Status reads are pure and do not consume a probe. A connection-refused provider
does not park a credential; only provider `429` exhaustion does.

The all-namespace view requires a scope-less static key in the configured
default namespace:

```bash
curl --fail \
  -H "Authorization: Bearer $GW_INBOUND_PLATFORM_KEY" \
  'http://127.0.0.1:8080/v1/credentials?namespaces=all'
```

Minted tokens cannot access this operator view, even if they carry a forged
`credentials:all` claim.

## Reload did not apply

A rejected candidate leaves the old snapshot serving. Look for
`config reload rejected` and fix the complete error before retrying.

- A new environment variable cannot be injected into a running process.
- File-backed key material can be replaced and re-read.
- `[server]`, `[[usage_sink]]`, and `[budget]` changes require restart.
- ConfigMap/projected-volume updates require `[reload] watch = true` or an
  explicit `SIGHUP`.

## Streams fail through a proxy

Test the same stream directly against Axond and through ingress. If direct works:

- disable response buffering;
- raise idle and total request timeouts;
- verify chunked/SSE transfer is preserved;
- inspect proxy retries, which must not replay a committed stream;
- verify client disconnects reach Axond so usage/finalization can complete.

See the [observability runbook](../observability.md) for metric names and alert
recommendations.
