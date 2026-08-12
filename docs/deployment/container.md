# Container deployment

`ghcr.io/litvue/axond` is a distroless image containing the static Axond binary,
published as a multi-architecture index for `linux/amd64` and `linux/arm64`. It
runs as non-root and contains no shell or package manager.

## Runtime contract

| Contract | Value |
| --- | --- |
| Entrypoint | `/axond` |
| Default listener | `0.0.0.0:8080` |
| Configuration | TOML path from `AXOND_CONFIG`, default `axond.toml` |
| Secret delivery | Environment references, plus supported mounted files for gateway keys and verifier material |
| Writable filesystem | Not required while serving |
| Public probes | `GET /healthz`, `GET /readyz` |
| Logs | Structured JSON on stdout/stderr |
| Architecture | `linux/amd64` and `linux/arm64`, selected by the index |

The image ships no config. Mount one read-only and provide every environment
variable it references:

```bash
docker run --rm --read-only \
  --publish 127.0.0.1:8080:8080 \
  --env AXOND_CONFIG=/etc/axond/axond.toml \
  --env GW_PLATFORM_OPENAI_API_KEY \
  --env GW_INBOUND_PLATFORM_KEY \
  --volume "$PWD/axond.toml:/etc/axond/axond.toml:ro" \
  ghcr.io/litvue/axond@sha256:<verified-digest>
```

An unset or empty referenced value is a fatal boot error. At least one
`[[gateway_key]]` is required.

## Verify before deployment

Resolve the release tag to a digest, verify its cosign identity and GitHub
attestation, then deploy the digest. Complete commands are in
[Installation and verification](../installation.md#oci-image).

There is intentionally no `latest` tag. Version and short-SHA tags are
convenient discovery pointers, not production locks.

The version and short-SHA tags resolve to the multi-architecture index, so the
digest they name deploys unchanged on both architectures — pin that index digest
rather than a per-architecture one unless a platform must be forced. When it
must, `:<version>-amd64` and `:<version>-arm64` name the single-platform images
directly, and each carries its own signature, provenance, and SBOM attestation.
Both architectures are booted and probed in the release pipeline, natively, from
the index digest that ships.

## Health and dependency semantics

`/healthz` and `/readyz` become reachable only after Axond has:

- parsed and validated the complete configuration graph;
- resolved every declared credential and gateway key;
- connected configured usage, budget, rate-limit, and revocation backends;
- bound its listener.

`/readyz` does not continuously probe providers or datastores after boot.
Monitor typed request failures and the metrics in the
[observability runbook](../observability.md).

## Networking and streaming

Axond does not terminate inbound TLS. Place it behind a trusted proxy or load
balancer and configure that hop to:

- preserve chunked/SSE responses instead of buffering them;
- allow idle and total durations appropriate for long model generations;
- pass `traceparent` when distributed tracing is used;
- expose only the intended network boundary;
- preserve either `Authorization` or `x-api-key` headers.

## Reloads and container environments

`SIGHUP` and `[reload] watch = true` rebuild the configuration snapshot. A
mounted ConfigMap-style symlink swap is detected. The environment of an
already-running process cannot gain a new variable, so adding a new
environment-backed credential or verifier generally requires a replacement
container. Existing file-backed key material can be replaced and reloaded.

`[server]`, `[[usage_sink]]`, and `[budget]` changes require a restart even when
other configuration changes can reload atomically.

## Shutdown

The server drains on `SIGTERM`: `/readyz` fails immediately, admission closes
after `shutdown.drain_grace_ms`, admitted requests finish within
`shutdown.deadline_ms`, and usage/telemetry flush within
`shutdown.flush_timeout_ms`. The container's stop timeout (`docker stop -t`,
`--stop-timeout`, the platform equivalent) must exceed the sum of those bounds,
or the runtime `SIGKILL`s the process mid-flush and buffered usage records are
lost.

Make sure the signal reaches PID 1 unwrapped: an entrypoint shell that does not
`exec` swallows `SIGTERM` and turns every stop into a kill. Streams still open
at the deadline are cut, so clients must be able to retry.
