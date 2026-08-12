# Kubernetes

The repository's `deploy/kubernetes/base` directory is a runnable Kustomize
base with a ConfigMap, example Secret, two-replica Deployment, ClusterIP
Service, probes, security context, resource starting points, and a
PodDisruptionBudget.

## Before applying

1. Replace the public values in `secret.example.yaml`, or replace that resource
   with a Secret supplied by your secret manager.
2. Replace the version tag with the verified release digest.
3. Review resource requests and limits against your payload and concurrency.
4. Confirm nodes can run `linux/amd64`; the public image is not yet multi-arch.

For example:

```bash
digest=sha256:<verified-digest>
kubectl apply -k deploy/kubernetes/base
kubectl -n axond set image deployment/axond \
  "axond=ghcr.io/litvue/axond@${digest}"
kubectl -n axond rollout status deployment/axond
```

Probe the service from inside the cluster or port-forward it locally:

```bash
kubectl -n axond port-forward service/axond 8080:8080
curl --fail http://127.0.0.1:8080/healthz
curl --fail \
  -H 'Authorization: Bearer replace-before-exposing' \
  http://127.0.0.1:8080/v1/models
```

## Configuration and secrets

The stable `axond-config` ConfigMap name and `[reload] watch = true` allow a
projected-volume symlink swap to be observed without a process restart. A bad
candidate is rejected while the previous snapshot keeps serving.

Not every setting is live-reloadable. Changes to `[server]`, `[[usage_sink]]`,
or `[budget]` require a rollout restart. Environment-backed secrets also require
replacement Pods because a running process cannot acquire new environment
values:

```bash
kubectl -n axond rollout restart deployment/axond
```

Use a CSI secret volume and file-backed `[[gateway_key]]` or
`[[gateway_verifier]]` when same-process key-material reload is required.
Provider credentials and backend DSNs are environment references today.

## Probes

The listener binds only after boot validation, secret resolution, and initial
connections to configured backends succeed. `/readyz` therefore proves a valid
serving snapshot at boot, but it does not continuously probe providers, Redis,
or Postgres. Alert on request-path metrics and typed `503` responses for
runtime dependency health.

`/readyz` also reports the drain: it answers `503 draining` from the moment
`SIGTERM` arrives (see [Rollouts and termination](#rollouts-and-termination)).
Both probes stay unauthenticated.

## Scaling

The HTTP process is stateless, so replicas can scale horizontally. These parts
remain replica-local unless a shared backend is selected:

- credential and target circuit state;
- round-robin/weighted cursors;
- in-memory budgets;
- in-memory rate limits;
- `[admission]` request bounds and load shedding.

Use Redis or Postgres when a control must be exact across replicas. See
[Stateful backends](./stateful-backends.md).

Do not add an HPA blindly. Base it on measured concurrency or request metrics,
then verify that scale-out does not change the semantics of any intentionally
in-memory control.

Admission is the sharpest case of that. `[admission]` ceilings are per replica,
so a fleet of *N* Pods admits *N* x `max_in_flight`, and one tenant behind the
Service gets *N* x `max_in_flight_per_tenant`. Size them from what a single Pod
can hold — `resources.limits.memory` against several times `max_in_flight` x
`max_request_bytes`, since a buffered body also costs a parsed JSON value, a
re-serialization for the usage estimate, and a clone per failover attempt, and
the node's descriptor budget against the sockets an in-flight stream holds — and
treat `axond.admission.rejections` rising as either a saturation signal for the
HPA or a ceiling set below what the Pod can serve.
The base overlay sets these explicitly for its own 512Mi limit rather than
inheriting the built-in defaults — a 1 MiB body ceiling and 32 in-flight
requests, so worst-case bodies stay a small fraction of the limit; change them
together. It also leaves `max_in_flight_per_tenant` at `0`, because the manifest
declares a single namespace: a per-tenant ceiling below `max_in_flight` would be
the ceiling all traffic meets, and it would answer `429` where the Pod's own
saturation should answer `503`. Set it when a second namespace exists.

## Ingress and streaming

Axond does not terminate TLS. The Ingress or external load balancer must:

- preserve `Authorization` and `x-api-key`;
- disable response buffering for streamed routes;
- allow idle and total durations appropriate for model responses;
- preserve `traceparent` when traces should join caller context;
- restrict the service to the intended callers.

## Rollouts and termination

`SIGTERM` starts a bounded drain in the process itself, so a rollout no longer
depends on endpoint removal winning a race
([ADR 0029](../adr/0029-bounded-termination.md)):

1. `/readyz` answers `503 draining` immediately. A Pod deleted through the API
   is removed from the Service endpoints as soon as it is marked terminating, so
   for in-cluster traffic the failing probe is confirmation rather than the
   mechanism; it is the mechanism for anything that only polls readiness, such
   as an ingress or cloud load balancer with its own target health. `/healthz`
   keeps answering `200` for the whole sequence — a draining Pod is not a wedged
   one, and failing liveness would only earn it a `SIGKILL`.
2. For `shutdown.drain_grace_ms` the Pod keeps serving, including requests that
   arrive while endpoint propagation catches up.
3. The listener then stops accepting. Anything that still reaches the process is
   refused with a typed `503` (`error.type = "draining"`).
4. Requests admitted earlier have `shutdown.deadline_ms` to finish. A stream
   still open at the deadline is ended with a body error; its spend is settled
   as `client_cancelled` up to the last relayed token, so an interrupted stream
   is charged for what the caller received, not discarded.
5. Usage sinks and telemetry exporters flush within one
   `shutdown.flush_timeout_ms`. Records that cannot be written are counted as
   `shutdown` drops on `axond.usage.records_dropped`, never dropped silently.

Size the grace period above the sum of the three bounds:

```
terminationGracePeriodSeconds > (drain_grace_ms + deadline_ms + flush_timeout_ms) / 1000
```

The shipped manifest pairs `terminationGracePeriodSeconds: 30` with the
defaults (5s drain, 15s deadline, 5s flush — 25s worst case). Raise both
together if callers hold longer streams; the process, not Kubernetes, is what
decides when a stream is cut.

A `preStop` hook is not required, because readiness fails before admission
closes. Add one only to lengthen the endpoint-removal window on a load balancer
that does not watch endpoints (some cloud L7 ingresses), and set
`shutdown.drain_grace_ms = 0` if the hook already waited — with no drain window
the first `SIGTERM` closes admission straight away. During a drain window, a
second `SIGTERM` closes admission immediately; after admission has closed,
further signals are logged and ignored so a `kubectl delete --now` cannot cut
the flush short.

Keep `maxUnavailable: 0`, a PodDisruptionBudget, and enough replicas: they are
what keeps capacity up during the drain. Clients should still retry requests
that end before response commitment.
