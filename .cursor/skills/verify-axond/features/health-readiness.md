# Health and readiness

Health and readiness let an operator (or load balancer) see that a replica is alive and still admitting work, without presenting a gateway key.

## Sub-features

- `healthz-ok` returns plain `ok` on `GET /healthz` with no `Authorization` header.
- `readyz-ready` returns plain `ready` on `GET /readyz` while the replica is serving.
- `probes-ignore-key` still answer when a Bearer token is sent; they do not require one.
- `readyz-not-catalogue` does not list models or credentials.

## How to get to it (user POV)

- Probe `GET {base}/healthz` from curl, a container runtime, or a Kubernetes liveness probe.
- Probe `GET {base}/readyz` from a readiness probe or `curl --fail`.
- Do not use `/admin/v1/status` (unmounted) or `/api/v1` for liveness.

## Driving it with drive.sh

Preconditions:

- Axond is healthy at `$AXOND_VERIFY_BASE_URL` from this run's `launch.sh`.
- `helpers/doctor.sh` reports `healthz ok` and `readyz ready`.
- No budget needs to exist.

- **Liveness.** Call the liveness probe with no key. Run `.cursor/skills/verify-axond/helpers/drive.sh --name healthz --no-auth /healthz`. Status `200` and `response.body` is exactly `ok`.
- **Readiness.** Call the readiness probe with no key. Run `.cursor/skills/verify-axond/helpers/drive.sh --name readyz --no-auth /readyz`. Status `200` and `response.body` is exactly `ready`.
- **Key is ignored, not required.** Repeat liveness with a Bearer token. Run `.cursor/skills/verify-axond/helpers/drive.sh --name healthz-keyed --auth /healthz`. Status `200` and body `ok`.
- **Closed neighbour.** Ask the catalogue without a key. Run `.cursor/skills/verify-axond/helpers/drive.sh --name models-anon --no-auth /ns/platform/v1/models`. Status `401`. Body is JSON `{"error":{"type":"unauthorized",...}}`, not a model list.
- **Proof.** Keep `drive/healthz/response.body`, `drive/readyz/response.body`, and `drive/models-anon/response.body`. Bodies `ok`/`ready` plus a typed `401` prove the probe pair is public and nothing else is.

## Gotchas

- `/readyz` becomes `503` `draining` after SIGTERM. Doctor and this recipe assume the process is still serving; do not SIGTERM first.
- Probe bodies are raw text, not JSON. Do not parse them as `error.type`.
- A Compose stack on `:8080` can also answer `ok`. Doctor must show this run's bind and PID before the probe counts.
- Unprefixed `/v1/models` is not a probe and is not served.
