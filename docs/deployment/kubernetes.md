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
4. Confirm nodes run `linux/amd64` or `linux/arm64`; the public image index
   covers both, so one pinned index digest schedules onto either.

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

## Production overlay

`deploy/kubernetes/base` is deliberately minimal: it is what you apply to
evaluate axond in a cluster. `deploy/kubernetes/overlays/production` is the base
plus the posture a production fleet needs, and nothing that is only useful for
evaluation:

- the image pinned by digest instead of by tag, and by an unresolvable sentinel
  digest until you resolve it (below);
- three replicas with `minAvailable: 2`, so exactly one voluntary disruption —
  a node drain, a cluster upgrade, a descheduler — proceeds at a time and the
  rolling update's `maxUnavailable: 0` stays honest;
- topology spread across nodes (`DoNotSchedule`) and zones (`ScheduleAnyway`),
  because a zone that cannot hold the fleet should cost you spread, not
  scheduling, and the node constraint counts skew per ReplicaSet
  (`matchLabelKeys: [pod-template-hash]`) so a rolling update's extra Pod still
  schedules on a cluster with three nodes;
- two NetworkPolicies: a default-deny for both directions, and an allow-list
  for ingress from the ingress controller, DNS, egress to public HTTPS with the
  link-local and private ranges excluded, and label-selected rules for Postgres,
  Redis, and an OpenTelemetry collector — those three select Pods in axond's own
  namespace, so widen them with a `namespaceSelector` (or an `ipBlock` for a
  managed service) when a store runs elsewhere, and delete the ones this
  deployment does not configure;
- requests raised to `500m`/`512Mi` with memory request equal to limit, so the
  Pod's memory usage can never exceed its request and it is the last candidate
  kubelet evicts under memory pressure while holding buffered request bodies
  (the CPU limit exceeds its request, so the QoS class is Burstable, not
  Guaranteed);
- a five-second `preStop` sleep, with `terminationGracePeriodSeconds: 45`
  covering it plus the process's own 25-second shutdown budget (see
  [Rollouts and termination](#rollouts-and-termination));
- the base's `secret.example.yaml` deleted (`$patch: delete` in
  `secret.yaml`), because its values are published in this repository — see
  [The Secret the overlay does not ship](#the-secret-the-overlay-does-not-ship).

The overlay requires **Kubernetes 1.32 or newer**, because two of its fields have
version floors and an older API server drops them rather than rejecting the
manifest:

- the `sleep` `preStop` lifecycle action, on by default from 1.30 and GA in 1.32;
  the distroless image has no shell, so an `exec` hook is not a fallback, and a
  dropped hook means Pods stop receiving traffic only after `SIGTERM`;
- `matchLabelKeys` on a topology spread constraint, on by default from 1.27; a
  dropped key turns the hard per-node spread into the fleet-wide form that
  deadlocks a rolling update on a cluster with as many nodes as replicas, which
  surfaces as a hung upgrade rather than as a rejected manifest.

On an older cluster, confirm both survive a round trip through
`kubectl -n axond get deployment axond -o yaml` before trusting a rollout.

```bash
kubectl create namespace axond
kubectl -n axond create secret generic axond-secrets \
  --from-literal=GW_INBOUND_PLATFORM_KEY=... \
  --from-literal=GW_PLATFORM_OPENAI_API_KEY=...
digest="$(ops/pin-image-digest.sh --print 0.3.27)" # x-release-please-version
SIGNER_IDENTITY=... GITHUB_REPOSITORY=Litvue/axond \
  ops/verify-image-evidence.sh "ghcr.io/litvue/axond@${digest}"
ops/pin-image-digest.sh 0.3.27 # x-release-please-version
kubectl apply -k deploy/kubernetes/overlays/production
kubectl -n axond rollout status deployment/axond
```

### The Secret the overlay does not ship

The base ships `secret.example.yaml` so an evaluation renders something bootable.
Its two values are readable by anyone with this repository, so the production
overlay deletes the resource rather than inheriting it. The container still takes
its credentials from `envFrom: secretRef: axond-secrets`, so applying the overlay
without supplying that Secret leaves the Pods in `CreateContainerConfigError` —
the intended failure, because a gateway that never starts is safer than one
serving with an inbound key published on GitHub.

Supply it however your platform supplies secrets: the `kubectl create secret`
above for a first rollout, or an External Secrets/sealed-Secret resource named
`axond-secrets` in the `axond` namespace. `ops/check-deploy-manifests.py` renders
the overlay and fails if any of the base's published placeholder values reappear
in it.

### Resolving the image digest

The committed overlay pins an all-zero digest, which no registry can serve. That
is the intended state of the file in the repository: a tag can be repointed at
different bytes after the review that approved it, and a real digest committed
here is stale by the next release. So the digest is resolved at deploy time from
the release you verified:

```bash
ops/pin-image-digest.sh --check          # fails while the sentinel is unresolved
ops/pin-image-digest.sh --print 0.3.27 # x-release-please-version, prints the digest
ops/pin-image-digest.sh 0.3.27 # x-release-please-version, rewrites the overlay
```

Resolution insists on the multi-architecture index, so a digest naming one
architecture's child image — schedulable onto that architecture alone — is
refused rather than pinned. The script resolves a reference and makes no claim
about its evidence; run `ops/verify-image-evidence.sh` on the digest for the
signature, SBOM, and provenance chain (see
[releasing](../maintainers/releasing.md)).

Run `ops/pin-image-digest.sh --check` in the job that applies the overlay. It is
the check that separates "digest not yet resolved" from a rollout that pulls a
placeholder.

### Optional autoscaling

`deploy/kubernetes/components/autoscaling` is a Kustomize component, not part of
the overlay, because an HPA is a claim that request load and CPU move together
for your traffic — measure that before adopting it. Take it from an overlay *of*
the production overlay:

```yaml
# my-cluster/kustomization.yaml
resources:
  - ../deploy/kubernetes/overlays/production
components:
  - ../deploy/kubernetes/components/autoscaling
```

Adding the component to `overlays/production/kustomization.yaml` instead does not
work, and fails quietly: Kustomize accumulates a Kustomization's components
before applying that same Kustomization's patches, so the component removes
`spec.replicas` and the overlay's own `deployment.yaml` patch then puts it back.
The rendered Deployment keeps `replicas: 3`, and every apply fights the HPA for
the field — which is the failure the component exists to avoid. Layering it above
the overlay, as here and as the manifest gate renders it, runs the removal last.

It adds an HPA at 60% CPU utilization between 3 and 12 replicas, and removes
`spec.replicas` from the Deployment so an apply does not fight the autoscaler for
the field. `minReplicas` stays at the disruption budget's floor plus one. Scale-up
doubles per minute; scale-down gives up one Pod every two minutes after ten
minutes of stability, because a scale-down is a drain and each one spends the
termination budget above.

Autoscaling on CPU does not make `[admission]` ceilings fleet-wide — read
[Scaling](#scaling) before enabling it, and treat
`axond.admission.rejections` as the saturation signal that CPU may not show.

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

`maxUnavailable: 0` and a hard per-node spread interact: on a cluster with as
many schedulable nodes as replicas, a surge Pod counted against the Pods it
replaces cannot be placed, and nothing is allowed to terminate to make room —
the rollout hangs with a `Pending` Pod and `didn't match pod topology spread
constraints`. The overlay avoids it with `matchLabelKeys: [pod-template-hash]`,
which is a scheduling outcome no rendered manifest can verify, so it is proven
on a real cluster:

```bash
just rollout-drill                # ops/rollout-drill.sh on a three-worker kind
                                  # cluster, ~3 minutes
```

The drill rolls the overlay out, then removes `matchLabelKeys` and requires the
same rollout to deadlock — the assertion that keeps the first result meaningful.
It renders the overlay with a runnable image instead of the digest sentinel and
never writes to `deploy/`. Run it when you change the spread constraints, the
rollout strategy, or the replica count.
