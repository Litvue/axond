# Stateful Kubernetes deployment runbook

This is the operator procedure for
[deploy/kubernetes/overlays/production-stateful](../../deploy/kubernetes/overlays/production-stateful).
The stateful process serves authenticated /admin/v1 and can compile a published
revision into an inference snapshot when the control plane contains a complete
provider, project workload principal, retained catalogue payload, and effective
approved price book. A healthy Pod with no such snapshot is still **Running and
not Ready**: /healthz returns 200, /readyz returns 503, and inference remains
fail-closed. The compiler and request path are implemented; the remaining
qualification work is to automate the controlled-upstream outage, restore, and
long-soak evidence in the integration matrix.

For durable per-replica last-known-good storage, use the separate
`deploy/kubernetes/overlays/production-stateful-persistent` option documented in
the [Kubernetes deployment guide](../deployment/kubernetes.md#durable-statefulset-option).
The procedure below intentionally remains the Recreate/emptyDir path; it is
appropriate for administrative operation and for a serving snapshot whose
recovery does not need to survive Pod replacement. The persistent option uses
the same Secret and migration ordering, but creates retained PVCs and requires
explicit Pod replacement because its StatefulSet uses `OnDelete`.

The persistent deployment boundary is qualified by
`ops/stateful-persistent-drill.sh`; the stateful integration recovery scenario
qualifies the signed desired-state and encrypted compiled-serving cache contents
that the PVC retains.

This distinction is important during an incident: Kubernetes availability and
inference availability are not claims this overlay makes today. The procedure
below treats the migration Job, Pod state, and the direct administrative
port-forward as the acceptance checks instead.

## Preconditions

- Kubernetes 1.32 or newer. The production overlay uses the GA sleep
  lifecycle action and matchLabelKeys for topology spread.
- A Postgres primary reachable from the axond namespace. The deployment's
  GW_CONTROL_PLANE_DSN and GW_SECRET_STORE_KEK are supplied through the Secret
  named axond-secrets.
- A verified multi-architecture image-index digest. The committed all-zero
  digest is intentionally not pullable.
- Four Secret values for this bootstrap: GW_CONTROL_PLANE_DSN,
  GW_SECRET_STORE_KEK, GW_ADMIN_BREAKGLASS, and GW_LAST_KNOWN_GOOD_KEY. The
  last-known-good value must be one canonical padded base64 string encoding 32
  CSPRNG bytes, with no surrounding whitespace; use the same exact value on
  every replica. The KEK must be the deployment's existing key material;
  changing it makes stored ciphertext unrecoverable. Provision the
  last-known-good key before the ConfigMap that names it.

Resolve and verify the image before applying the overlay. Set
`RELEASE_VERSION` to the verified release; the resolver updates both production
overlays, so scope the check to the one being deployed:

~~~bash
RELEASE_VERSION=REPLACE_WITH_VERIFIED_RELEASE_VERSION
ops/pin-image-digest.sh "$RELEASE_VERSION"
ops/pin-image-digest.sh --check overlays/production-stateful
SIGNER_IDENTITY=... GITHUB_REPOSITORY=Litvue/axond \
  ops/verify-image-evidence.sh ghcr.io/litvue/axond@sha256:VERIFIED_INDEX_DIGEST
~~~

## First install

Create the namespace and the Secret out of band; the overlay deletes the
repository's public example Secret. The values below are placeholders and must
be replaced:

~~~bash
set -euo pipefail

namespace=axond
overlay=deploy/kubernetes/overlays/production-stateful

kubectl create namespace "$namespace" --dry-run=client -o yaml |
  kubectl apply -f -
kubectl -n "$namespace" create secret generic axond-secrets \
  --from-literal=GW_CONTROL_PLANE_DSN='postgres://<user>:<password>@<host>:5432/<db>' \
  --from-literal=GW_SECRET_STORE_KEK='<base64-encoded-key-material>' \
  --from-literal=GW_ADMIN_BREAKGLASS='<breakglass-value>' \
  --from-literal=GW_LAST_KNOWN_GOOD_KEY='<44-character-padded-base64-32-byte-key>' \
  --dry-run=client -o yaml | kubectl apply -f -

ops/pin-image-digest.sh --check overlays/production-stateful
kubectl apply -k "$overlay"
~~~

The Secret step is deliberately first. For an existing fleet, update or create
`axond-secrets` with `GW_LAST_KNOWN_GOOD_KEY` and verify it is present before
applying a release whose `axond.toml` contains `[convergence]`. Applying that
ConfigMap first makes the new cache configuration a boot dependency while the
key is absent, so replicas can crash-loop instead of reaching the administrative
surface. The key is a deployment-wide reference used to authenticate the cache;
it is not written into the ConfigMap or logs.

The Job and Deployment are created by the same apply and are not ordered by
Kubernetes. A Pod may briefly restart against a schema that is not present yet;
that is expected. To keep the fleet empty while the migration completes, scale
the Deployment down immediately after the apply, then start the three replicas:

~~~bash
kubectl -n "$namespace" scale deployment/axond --replicas=0
kubectl -n "$namespace" wait \
  --for=condition=complete job/axond-migrate --timeout=5m
kubectl -n "$namespace" logs job/axond-migrate
kubectl -n "$namespace" scale deployment/axond --replicas=3
~~~

For a failed or exhausted Job, inspect it before restarting anything:

~~~bash
kubectl -n axond get job axond-migrate
kubectl -n axond describe job axond-migrate
kubectl -n axond logs job/axond-migrate
~~~

BackoffLimitExceeded is terminal for that Job. Once Postgres and the Secret
are fixed, delete and re-apply it; axond migrate apply is forward-only and
idempotent:

~~~bash
kubectl -n axond delete job axond-migrate --ignore-not-found
kubectl apply -k deploy/kubernetes/overlays/production-stateful
kubectl -n axond wait \
  --for=condition=complete job/axond-migrate --timeout=5m
~~~

## Readiness acceptance

Do not use kubectl wait --for=condition=available,
kubectl wait --for=condition=ready, or kubectl rollout status for this
overlay. They wait for a condition the current stateful process deliberately
does not report. Inspect the replacement and process state instead:

~~~bash
kubectl -n axond get pods -l app.kubernetes.io/name=axond \
  -o custom-columns='NAME:.metadata.name,PHASE:.status.phase,READY:.status.containerStatuses[0].ready,RESTARTS:.status.containerStatuses[0].restartCount'
kubectl -n axond get deployment axond \
  -o jsonpath='{.status.updatedReplicas}/{.status.replicas}{" updated/desired\n"}'
kubectl -n axond get endpointslices \
  -l kubernetes.io/service-name=axond
~~~

Acceptance for an empty or unconverged deployment is three stable Running Pods,
READY false for each, no ready inference endpoints, and no continuing restart
loop after the migration Job completed. Once a complete revision is published,
repeat the same checks expecting /readyz 200 and a Ready endpoint only after the
convergence status reports the revision active. A node drain remains permitted because the stateful
PodDisruptionBudget sets unhealthyPodEvictionPolicy: AlwaysAllow.

Probe one Pod through an operator-controlled port-forward. The administrative
surface is intentionally not published through an axond-admin Service:

~~~bash
pod="$(kubectl -n axond get pods -l app.kubernetes.io/name=axond \
  -o jsonpath='{.items[0].metadata.name}')"
kubectl -n axond port-forward "pod/$pod" 18080:8080 >/tmp/axond-stateful-forward.log 2>&1 &
forward_pid=$!
trap 'kill "$forward_pid" 2>/dev/null || true' EXIT

for attempt in $(seq 1 30); do
  curl -fsS http://127.0.0.1:18080/healthz >/dev/null && break
  sleep 1
done
test "$(curl -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:18080/healthz)" = 200
test "$(curl -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:18080/readyz)" = 503

# Before an inbound principal has been projected, the authentication-first
# boundary returns 401 to an anonymous caller. Once a valid workload principal
# exists, the same pre-convergence route returns 503 `inference_unavailable`;
# the stateful integration qualification covers that authenticated case.
inference_status="$(curl -sS -o /tmp/axond-inference-body \
  -w '%{http_code}' -X POST -H 'content-type: application/json' \
  -d '{}' http://127.0.0.1:18080/v1/chat/completions)"
test "$inference_status" = 401
grep -q '"unauthorized"' /tmp/axond-inference-body

# The value, not the variable name, is the breakglass token.
test -n "$GW_ADMIN_BREAKGLASS"
curl -fsS -H "Authorization: Bearer $GW_ADMIN_BREAKGLASS" \
  http://127.0.0.1:18080/admin/v1/state >/dev/null
~~~

## Upgrade

An upgrade is a maintenance window for this overlay: Recreate replaces the
whole fleet because there is no Ready Pod to keep available. Re-run the schema
Job explicitly, then watch updated replicas rather than availability:

~~~bash
kubectl -n axond delete job axond-migrate --ignore-not-found
kubectl apply -k deploy/kubernetes/overlays/production-stateful
kubectl -n axond wait \
  --for=condition=complete job/axond-migrate --timeout=5m
kubectl -n axond logs job/axond-migrate

for attempt in $(seq 1 90); do
  updated="$(kubectl -n axond get deployment axond -o jsonpath='{.status.updatedReplicas}')"
  desired="$(kubectl -n axond get deployment axond -o jsonpath='{.status.replicas}')"
  if test "$updated" = 3 && test "$desired" = 3; then
    break
  fi
  sleep 2
done
test "$updated" = 3
test "$desired" = 3
~~~

Repeat the readiness acceptance after the replacement. The admin port-forward
is unavailable while Recreate is deleting and creating Pods, so schedule
administrative work around that window.

## Rollback

There are two different rollbacks. Both are forward actions; neither rewinds
the Postgres journal in place.

### Roll back desired state

/admin/v1/rollback republishes an earlier complete desired state as a new
revision. It retains the incident revision and its audit trail. Through the
same Pod port-forward as above:

~~~bash
export AXOND_ADMIN_ENDPOINT=http://127.0.0.1:18080
export AXOND_ADMIN_TOKEN="$GW_ADMIN_BREAKGLASS"
axond admin history --limit 20
axond admin rollback \
  --revision KNOWN_GOOD_REVISION \
  --summary 'restore the known-good desired state' \
  --idempotency-key rollback-INCIDENT_ID \
  --expected-revision CURRENT_HEAD
~~~

At the current stateful boundary this changes the control-plane history, not
inference readiness: Pods continue to report 503 readiness until revision
convergence is available. Use the expected-revision from the history read; the
gateway rejects a stale rollback instead of overwriting a newer change.

### Roll back a compatible image

Only roll an image back when the retained binary accepts the schema already
applied. Migrations are forward-only; an older binary that cannot read the
current schema must be fixed forward, or paired with a database recovery whose
schema and image belong together. Do not run a reverse migration.

For an image-only emergency rollback, keep the completed migration Job and
change the Deployment to the previously verified index digest:

~~~bash
old_image=ghcr.io/litvue/axond@sha256:PREVIOUS_VERIFIED_INDEX_DIGEST
kubectl -n axond set image deployment/axond axond="$old_image"
~~~

Because the strategy is Recreate, verify the replacement with
.status.updatedReplicas and the readiness acceptance above. Record the image
change in the version-controlled overlay after the incident; a later
kubectl apply -k will otherwise restore its declared image. Do not delete the
migration Job for an image-only rollback: the schema is not rolled backward.

## Related procedures

- [Kubernetes deployment shape](../deployment/kubernetes.md#stateful-mode)
- [Upgrades and rollback](./upgrades.md)
- [Administering a stateful deployment](./admin-api.md)
- [Control-plane journal and migration states](./control-plane-journal.md#operator-commands)
- [Backup, restore, and PITR](./backup-and-recovery.md)
