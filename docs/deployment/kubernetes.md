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
can hold — `resources.limits.memory` against `max_in_flight` x
`max_request_bytes`, and the node's descriptor budget against the sockets an
in-flight stream holds — and treat `axond.admission.rejections` rising as either
a saturation signal for the HPA or a ceiling set below what the Pod can serve.
The base overlay sets these explicitly for its own 512Mi limit rather than
inheriting the built-in defaults; change them together.

## Ingress and streaming

Axond does not terminate TLS. The Ingress or external load balancer must:

- preserve `Authorization` and `x-api-key`;
- disable response buffering for streamed routes;
- allow idle and total durations appropriate for model responses;
- preserve `traceparent` when traces should join caller context;
- restrict the service to the intended callers.

## Rollouts and termination

The current binary has no application-level SIGTERM drain. A Pod may therefore
terminate an in-flight request or stream. Use `maxUnavailable: 0`, a
PodDisruptionBudget, sufficient replicas, and load-balancer endpoint removal to
reduce interruption. Clients must be prepared to retry requests that end before
response commitment.

Do not describe `terminationGracePeriodSeconds` as graceful draining; it is
only the outer Kubernetes deadline until Axond implements a shutdown handler.
