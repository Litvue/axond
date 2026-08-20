#!/usr/bin/env bash
# Prove that the stateful overlay deploys the lifecycle the runtime actually has:
# a replica boots, serves `/admin/v1`, and refuses inference until revision
# convergence ships — and that the three manifest decisions that follow from a
# fleet which is never Ready are the ones that make it operable.
#
# `ops/check-deploy-manifests.py` asserts the *shape* of those decisions. Shape is
# not behaviour: whether `/admin/v1` is reachable, whether an upgrade completes,
# and whether a node can be drained are answers only an API server gives. So this
# drill runs a real cluster and asserts, with a counterfactual for each:
#
#   1. `axond migrate apply` runs once as a Job, before any replica, and a rerun
#      against the migrated database is a no-op rather than a second migration;
#   2. replicas are Running and never Ready, so the `axond` Service has no
#      endpoints; the default component publishes no admin Service because a
#      CNI that ignores NetworkPolicy would make it cluster-wide. An operator
#      port-forwards directly to a selected Pod, where `/admin/v1` answers while
#      anonymous `/v1/chat/completions` answers `401 unauthorized` before an
#      inbound principal exists; an authenticated caller is covered by the
#      stateful integration lane and receives `503 inference_unavailable` until
#      an active serving revision exists;
#   3. an upgrade completes with `strategy: Recreate`, and the counterfactual —
#      the base's `RollingUpdate` with `maxUnavailable: 0` — hangs, because it
#      waits for an availability this fleet never reports;
#   4. a Pod can be evicted with `unhealthyPodEvictionPolicy: AlwaysAllow`, and
#      the counterfactual — the default `IfHealthyBudget` — is refused `429`,
#      which is a node drain that never finishes.
#
# The counterfactuals are the half that matters: without them a future change
# that quietly restored the stateless defaults would still pass, and the failure
# would arrive during an upgrade or a node drain of a production cluster.
#
# The image is built from this workspace by default. The published release does
# not serve `/admin/v1` from `serve` yet, so a drill pinned to a tag would assert
# against a gateway older than the manifests it is checking.
#
# Usage:
#     ops/stateful-deploy-drill.sh                              # ~10 minutes
#     AXOND_STATEFUL_IMAGE=ghcr.io/litvue/axond:0.4.0 ops/stateful-deploy-drill.sh
#     AXOND_STATEFUL_KEEP=1 ops/stateful-deploy-drill.sh        # keep the cluster
#
# Needs Docker, `kind`, `kubectl`, and `curl`; `kustomize` if kubectl's built-in
# copy is too old. Everything it creates lives in its own kind cluster and is
# deleted on exit. No credential outside this script's throwaway values is read.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cluster="${AXOND_STATEFUL_CLUSTER:-axond-stateful-drill}"
node_image="${AXOND_STATEFUL_NODE_IMAGE:-kindest/node:v1.33.4}"
# Postgres is pinned to the image the CI service container uses, so the drill
# exercises the version the rest of the qualification does.
postgres_image="${AXOND_STATEFUL_POSTGRES_IMAGE:-postgres:17.6-alpine}"
overlay="${root}/deploy/kubernetes/overlays/production-stateful"
image="${AXOND_STATEFUL_IMAGE:-}"

workdir="$(mktemp -d)"
port_forward=""
cleanup() {
  [[ -z "$port_forward" ]] || kill "$port_forward" 2>/dev/null || true
  if [[ -z "${AXOND_STATEFUL_KEEP:-}" ]]; then
    kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
  fi
  rm -rf "$workdir"
}
trap cleanup EXIT

step() { printf '\n== %s\n' "$1"; }
fail() {
  printf 'stateful deploy drill failed: %s\n' "$1" >&2
  exit 1
}
ok() { printf '  ok  %s\n' "$1"; }

for tool in docker kind kubectl curl; do
  command -v "$tool" >/dev/null 2>&1 || fail "${tool} is required"
done

kube() { kubectl --context "kind-${cluster}" "$@"; }
render() {
  if command -v kustomize >/dev/null 2>&1; then
    kustomize build "$1"
  else
    kubectl kustomize "$1"
  fi
}

step "Building the gateway image"
if [[ -z "$image" ]]; then
  image="axond-stateful-drill:$(git -C "$root" rev-parse --short HEAD 2>/dev/null || echo local)"
  docker build --tag "$image" "$root" >"${workdir}/build.log" 2>&1 ||
    fail "docker build failed: $(tail -n 20 "${workdir}/build.log")"
else
  docker image inspect "$image" >/dev/null 2>&1 || docker pull "$image" >/dev/null ||
    fail "cannot pull ${image}"
fi
ok "image ${image}"

step "Creating a three-worker cluster"
# The overlay inherits production's hard per-node spread for three replicas, so
# the cluster needs three schedulable nodes exactly as ops/rollout-drill.sh does.
cat >"${workdir}/kind.yaml" <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
  - role: worker
EOF
kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
kind create cluster --name "$cluster" --config "${workdir}/kind.yaml" \
  --image "$node_image" >/dev/null
kind load docker-image "$image" --name "$cluster" >/dev/null
docker image inspect "$postgres_image" >/dev/null 2>&1 ||
  docker pull "$postgres_image" >/dev/null || fail "cannot pull ${postgres_image}"
kind load docker-image "$postgres_image" --name "$cluster" >/dev/null

step "Standing up the control-plane database"
kube create namespace axond >/dev/null 2>&1 || true
cat >"${workdir}/postgres.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
  namespace: axond
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: postgres
  template:
    metadata:
      labels:
        app.kubernetes.io/name: postgres
    spec:
      containers:
        - name: postgres
          image: ${postgres_image}
          imagePullPolicy: IfNotPresent
          env:
            - name: POSTGRES_PASSWORD
              value: stateful-drill
          ports:
            - containerPort: 5432
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
  namespace: axond
spec:
  selector:
    app.kubernetes.io/name: postgres
  ports:
    - port: 5432
      targetPort: 5432
EOF
kube apply -f "${workdir}/postgres.yaml" >/dev/null
kube -n axond rollout status deployment/postgres --timeout=180s >/dev/null ||
  fail "the drill's Postgres never became ready"
# The three references the stateful bootstrap names. Throwaway values on a
# cluster this script deletes; nothing here is read from the environment.
kube -n axond create secret generic axond-secrets \
  --from-literal=GW_CONTROL_PLANE_DSN='postgres://postgres:stateful-drill@postgres.axond.svc:5432/postgres' \
  --from-literal=GW_SECRET_STORE_KEK='c3RhdGVmdWwtZHJpbGwta2VrLTMyLWJ5dGVzLWxvbmc=' \
  --from-literal=GW_ADMIN_BREAKGLASS='stateful-drill-breakglass-credential' \
  --from-literal=GW_LAST_KNOWN_GOOD_KEY='MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=' \
  --dry-run=client -o yaml | kube apply -f - >/dev/null
ok "postgres.axond.svc:5432 and the axond-secrets references"

step "Applying the stateful overlay"
# The sentinel is replaced in the rendered output only, so nothing under deploy/
# is written and a failed drill cannot leave the overlay pinned to a tag.
render "$overlay" |
  sed "s|ghcr.io/litvue/axond@sha256:0\{64\}|${image}|g" >"${workdir}/rendered.yaml"
[[ "$(grep -c "image: ${image}$" "${workdir}/rendered.yaml")" == 2 ]] ||
  fail "expected the Deployment and the migration Job to share the sentinel digest; \
the overlay's image pinning changed and this drill needs updating"
kube apply -f "${workdir}/rendered.yaml" >/dev/null

# One Job migrates for the whole fleet. Nothing orders it against the Deployment
# — `kubectl apply` creates both — so the replicas crash-loop against an
# unrecognised schema until it completes; what matters here is that exactly one
# process writes DDL and that it converges.
step "One Job migrates the schema, and a rerun is a no-op"
kube -n axond wait --for=condition=complete job/axond-migrate --timeout=180s >/dev/null ||
  fail "the axond-migrate Job did not complete: $(kube -n axond logs job/axond-migrate 2>&1 | tail -n 5)"
# Every attempt's Pod survives its Job (`restartPolicy: Never`, `backoffLimit: 6`),
# and `logs job/...` prints whichever one it picks first, so a migration that
# needed a retry would be judged by the attempt that failed. Aggregate them.
migration_log="$(kube -n axond logs -l job-name=axond-migrate --tail=-1)"
grep -q "applied .* migration" <<<"$migration_log" ||
  fail "the migration Job completed without applying anything: ${migration_log}"
ok "$(grep -m 1 "applied .* migration" <<<"$migration_log")"

# Forward-only and idempotent is the property that makes it safe in front of an
# upgrade, so it is asserted rather than assumed: the same Job spec, run again,
# has to report a current schema instead of migrating a second time.
# Foreground cascade, because the default one deletes the Job and leaves its
# Pod to the garbage collector: the first attempt's log stays readable under the
# same job-name label, and the rerun's aggregation would read a migration that
# this run did not perform.
kube -n axond delete job axond-migrate --cascade=foreground --wait=true >/dev/null
kube -n axond wait --for=delete pod -l job-name=axond-migrate --timeout=120s >/dev/null ||
  fail "the first migration's Pod outlived its Job, so the rerun's log is not its own"
kube apply -f "${workdir}/rendered.yaml" >/dev/null
kube -n axond wait --for=condition=complete job/axond-migrate --timeout=180s >/dev/null ||
  fail "the rerun of the migration Job did not complete"
rerun_log="$(kube -n axond logs -l job-name=axond-migrate --tail=-1)"
grep -q "is current" <<<"$rerun_log" ||
  fail "a rerun against a migrated database was not a no-op: ${rerun_log}"
! grep -q "applied .* migration" <<<"$rerun_log" ||
  fail "the rerun migrated a second time: ${rerun_log}"
ok "rerun: $(grep -m 1 "is current" <<<"$rerun_log")"

step "Replicas boot, serve /admin/v1, and stay unready"
kube -n axond wait --for=jsonpath='{.status.phase}'=Running pod \
  -l app.kubernetes.io/name=axond --timeout=180s >/dev/null ||
  fail "the replicas never reached Running: $(kube -n axond get pods -o wide 2>&1)"
# `Running` and not Ready is also what a crash-looping replica reports, and one
# that cannot read its schema crash-loops exactly that way — so the container
# has to be up, not merely scheduled, or this step would pass on a fleet that
# never served anything.
#
# Polled rather than sampled: the Job and the Deployment are applied together,
# so the replicas legitimately crash-loop until the schema lands, and kubelet's
# backoff reaches five minutes. A single reading would fail a healthy fleet that
# is merely still in a backoff it is about to leave for good, and the window has
# to clear that cap with room to spare: a replica that entered it just before the
# Job completed waits the full five minutes, and the three do not enter it
# together.
running_containers() {
  kube -n axond get pods -l app.kubernetes.io/name=axond \
    -o jsonpath='{range .items[*]}{.status.containerStatuses[*].state.running.startedAt}{"\n"}{end}' |
    grep -c . || true
}
serving=0
for _ in $(seq 1 156); do
  serving="$(running_containers)"
  [[ "$serving" == 3 ]] && break
  sleep 5
done
[[ "$serving" == 3 ]] ||
  fail "only ${serving} of three replicas have a running container after waiting out \
kubelet's backoff; a Pod stuck in CrashLoopBackOff reports Running and not Ready too: \
$(kube -n axond get pods -o wide 2>&1)"
running="$(kube -n axond get pods -l app.kubernetes.io/name=axond \
  --field-selector=status.phase=Running -o name | wc -l | tr -d '[:space:]')"
[[ "$running" == 3 ]] || fail "expected three Running replicas, got ${running}"
# Only once the fleet is up is a Ready condition worth reading: the readiness
# probe runs every 5s, so one would have appeared by now if it were coming.
sleep 20
ready="$(kube -n axond get pods -l app.kubernetes.io/name=axond \
  -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' |
  grep -c True || true)"
[[ "$ready" == 0 ]] ||
  fail "${ready} replica(s) reported Ready; a replica that refuses inference must not, \
and this overlay's Recreate/AlwaysAllow/admin-Service decisions exist because none does"
# Restarts before the Job completed are expected — the two are applied together
# — so what has to hold is that they stopped: a replica on a migrated schema
# refuses inference in place rather than exiting.
restart_counts() {
  kube -n axond get pods -l app.kubernetes.io/name=axond \
    -o jsonpath='{range .items[*]}{.status.containerStatuses[*].restartCount}{","}{end}'
}
before="$(restart_counts)"
sleep 15
[[ "$(restart_counts)" == "$before" ]] ||
  fail "a replica restarted while the schema was already current; it is crash-looping \
rather than serving a refusal: $(kube -n axond get pods 2>&1)"
ok "three replicas Running with a live container, none Ready, none restarting"

inference_endpoints="$(kube -n axond get endpointslices -l kubernetes.io/service-name=axond \
  -o jsonpath='{range .items[*].endpoints[*]}{.conditions.ready}{"\n"}{end}' | grep -c true || true)"
[[ "$inference_endpoints" == 0 ]] ||
  fail "the inference Service has ${inference_endpoints} ready endpoint(s); a replica \
refusing inference must not be routed to"
admin_pod="$(kube -n axond get pods -l app.kubernetes.io/name=axond \
  -o jsonpath='{.items[0].metadata.name}')"
[[ -n "$admin_pod" ]] || fail "no Running axond Pod is available for the operator port-forward"
ok "axond: no inference endpoints; direct operator access uses Pod ${admin_pod}"

step "Probing the surfaces through an operator Pod port-forward"
# The default component intentionally has no admin Service. Port-forwarding is
# a kubelet-mediated operator action and remains available even when the CNI
# does not implement NetworkPolicy; a cluster-wide Service would not be safe in
# that case.
kube -n axond port-forward "pod/${admin_pod}" 18443:8080 >"${workdir}/forward.log" 2>&1 &
port_forward=$!
for _ in $(seq 1 30); do
  curl -sf -o /dev/null "http://127.0.0.1:18443/healthz" && break
  sleep 1
done

probe() { curl -s -o "${workdir}/body" -w '%{http_code}' "$@"; }
[[ "$(probe http://127.0.0.1:18443/healthz)" == 200 ]] ||
  fail "liveness did not answer 200 through the operator Pod port-forward"
[[ "$(probe http://127.0.0.1:18443/readyz)" == 503 ]] ||
  fail "readiness answered something other than 503 on an unconverged replica"
ok "/healthz 200, /readyz 503"

code="$(probe -X POST -H 'content-type: application/json' -d '{}' \
  http://127.0.0.1:18443/namespaces/platform/v1/chat/completions)"
[[ "$code" == 401 ]] || fail "anonymous inference answered ${code}, not 401"
grep -q '"unauthorized"' "${workdir}/body" ||
  fail "the anonymous refusal is not the typed unauthorized error: $(cat "${workdir}/body")"
ok "/namespaces/platform/v1/chat/completions 401 unauthorized before convergence"

# The administrative surface is authenticated, so an unauthenticated probe is
# what proves it is *there*: a typed admin error is the surface answering, and it
# is the answer the refusal router's fallback would not give.
code="$(probe http://127.0.0.1:18443/admin/v1/tenants)"
grep -q '"admin_' "${workdir}/body" ||
  fail "/admin/v1 did not answer with a typed admin error (${code}): $(cat "${workdir}/body")"
grep -q 'inference_unavailable' "${workdir}/body" &&
  fail "/admin/v1 fell through to the inference refusal, so the admin surface is not served"
ok "/admin/v1 answers ${code} from the administrative surface"

step "Upgrading as shipped: every replica has to reach the new template"
# Not `kubectl rollout status`: that waits for *available* replicas, and this
# fleet reports none by design, so it times out on a completed upgrade as
# readily as on a stalled one. `updatedReplicas` is what distinguishes them —
# how many Pods the new template actually reached — and it is what the runbook
# tells an operator to watch (docs/deployment/kubernetes.md#stateful-mode).
#
# The status is written asynchronously, so a poll that lands between a patch and
# the controller's first write still describes the previous template — 3/3 on a
# fleet that has not started upgrading. `observedGeneration` is what separates
# the two; without it the Recreate assertion passes vacuously and the
# RollingUpdate counterfactual fails a cluster that stalled exactly as intended.
updated_replicas() {
  local generation observed updated total
  # Read one field per call: an absent `.status.updatedReplicas` renders as
  # nothing, so a single space-separated template would silently shift the
  # remaining values into the wrong variables.
  generation="$(kube -n axond get deployment axond -o jsonpath='{.metadata.generation}')"
  observed="$(kube -n axond get deployment axond -o jsonpath='{.status.observedGeneration}')"
  updated="$(kube -n axond get deployment axond -o jsonpath='{.status.updatedReplicas}')"
  total="$(kube -n axond get deployment axond -o jsonpath='{.status.replicas}')"
  if ((${observed:-0} < ${generation:-0})); then
    printf 'stale'
    return
  fi
  printf '%s/%s' "${updated:-0}" "${total:-0}"
}
await_updated() {
  for _ in $(seq 1 "$2"); do
    [[ "$(updated_replicas)" == "$1" ]] && return 0
    sleep 2
  done
  return 1
}
# Every upgrade here replaces Pods that are Running and never Ready, so a stall
# and a slow replacement look alike from outside; the Pod list is what tells them
# apart when an assertion fails.
diagnose() {
  kube -n axond get pods -l app.kubernetes.io/name=axond \
    -o custom-columns='NAME:.metadata.name,PHASE:.status.phase,HASH:.metadata.labels.pod-template-hash' >&2
  kube -n axond get deployment axond \
    -o jsonpath='{.spec.strategy}{"\n"}{.status}{"\n"}' >&2
}
kube -n axond patch deployment axond --type=json \
  -p='[{"op":"add","path":"/spec/template/metadata/annotations","value":{"axond.dev/drill":"1"}}]' \
  >/dev/null
await_updated 3/3 90 || {
  diagnose
  fail "the Recreate upgrade left the fleet at $(updated_replicas) on the new template"
}
ok "the upgrade reached every replica with strategy: Recreate"

step "The counterfactual: RollingUpdate has to stall"
kube -n axond patch deployment axond --type=merge -p='{"spec":{"strategy":{"type":"RollingUpdate","rollingUpdate":{"maxUnavailable":0,"maxSurge":1}},"template":{"metadata":{"annotations":{"axond.dev/drill":"2"}}}}}' \
  >/dev/null
if await_updated 3/3 45; then
  fail "a RollingUpdate replaced every replica on a fleet with no Ready Pods, so Recreate \
is no longer what makes the upgrade land"
fi
observed="$(updated_replicas)"
[[ "$observed" == 1/4 ]] ||
  fail "expected one surge Pod beside three untouched replicas, observed ${observed} \
updated/total; the drill is not watching the stall it describes"
ok "the RollingUpdate stalled at ${observed} updated/total replicas"
# The strategy is restored *and* the template rolled, in that order and in one
# patch, because the strategy alone does not release the stall: the surge Pod
# already carries the target template, so the Deployment has nothing new to
# converge on and its controller left the fleet at 1/1 for as long as this drill
# waited (observed on kind v1.33.4). Rolling the template is the recovery an
# operator actually performs, and the runbook says so.
kube -n axond patch deployment axond --type=merge \
  -p='{"spec":{"strategy":{"type":"Recreate","rollingUpdate":null},"template":{"metadata":{"annotations":{"axond.dev/drill":"3"}}}}}' \
  >/dev/null
await_updated 3/3 90 || {
  diagnose
  fail "restoring Recreate left the fleet at $(updated_replicas); it did not release the \
stalled upgrade"
}
ok "restoring Recreate and rolling the template released the stalled upgrade"

step "Evicting an unready replica: AlwaysAllow has to permit it"
evict() {
  kube -n axond create -f - \
    --raw "/api/v1/namespaces/axond/pods/$1/eviction" <<EOF
{"apiVersion":"policy/v1","kind":"Eviction","metadata":{"name":"$1","namespace":"axond"}}
EOF
}
# A Pod already being deleted is evictable whatever the budget says, so both
# halves of this pair have to name a settled one or the counterfactual would pass
# for the wrong reason.
settled_pod() {
  kube -n axond get pods -l app.kubernetes.io/name=axond \
    --field-selector=status.phase=Running \
    -o custom-columns='NAME:.metadata.name,DEL:.metadata.deletionTimestamp' --no-headers |
    awk '$2 == "<none>" { print $1 }'
}
await_settled_fleet() {
  for _ in $(seq 1 90); do
    [[ "$(settled_pod | wc -l | tr -d '[:space:]')" == 3 ]] && return 0
    sleep 2
  done
  return 1
}
await_settled_fleet || fail "the fleet never settled at three Running replicas to evict from"
pod="$(settled_pod | head -n 1)"
evict "$pod" >/dev/null 2>&1 || fail "the eviction of unready Pod ${pod} was refused \
while the budget declares unhealthyPodEvictionPolicy: AlwaysAllow"
ok "evicted ${pod}"

step "The counterfactual: the default budget has to refuse it"
kube -n axond patch pdb axond --type=merge \
  -p='{"spec":{"unhealthyPodEvictionPolicy":"IfHealthyBudget"}}' >/dev/null
# The budget's own controller has to have seen the new policy: the API server
# refuses an eviction against a stale budget with the same 429 the budget itself
# would give, and the two would be indistinguishable below.
for _ in $(seq 1 30); do
  [[ "$(kube -n axond get pdb axond -o jsonpath='{.spec.unhealthyPodEvictionPolicy}')" == IfHealthyBudget &&
    "$(kube -n axond get pdb axond -o jsonpath='{.status.observedGeneration}')" == "$(kube -n axond get pdb axond -o jsonpath='{.metadata.generation}')" ]] && break
  sleep 2
done
await_settled_fleet || fail "the fleet never settled back at three Running replicas after the eviction"
pod="$(settled_pod | head -n 1)"
if evict "$pod" >"${workdir}/eviction.log" 2>&1; then
  fail "the default budget allowed an eviction with no Ready Pods, so AlwaysAllow is \
not what keeps a node drainable"
fi
grep -qi "Cannot evict pod\|disruption budget\|TooManyRequests" "${workdir}/eviction.log" ||
  fail "the eviction failed for a reason other than the disruption budget: \
$(cat "${workdir}/eviction.log")"
ok "the default budget refuses the eviction, which is a node drain that never finishes"

printf '\nstateful deploy drill passed: the overlay migrates once for the whole fleet,\n'
printf 'serves /admin/v1 on Pods that refuse inference and never report Ready, upgrades\n'
printf 'with Recreate where a RollingUpdate stalls, and stays drainable where the\n'
printf 'default disruption budget would block every eviction.\n'
