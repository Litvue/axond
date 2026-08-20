#!/usr/bin/env bash
# Prove the durable StatefulSet option on a real cluster:
#
#   * the same migration Job and stateful administrative boundary as the
#     Recreate overlay are usable with a StatefulSet;
#   * every ordinal receives a retained, per-replica PVC mounted at the signed
#     last-known-good cache path;
#   * deleting a Pod preserves its ordinal and cache volume, so a replacement
#     can recover the authenticated cache rather than cold-booting empty.
#
# The process-level integration lane proves the signed desired-state and
# encrypted serving-cache formats. This drill proves the deployment decision
# that makes those files survive Pod replacement. Together they qualify the
# StatefulSet/PVC recovery path without applying anything to a real cluster.
#
# Usage:
#     ops/stateful-persistent-drill.sh
#     AXOND_STATEFUL_PERSISTENT_KEEP=1 ops/stateful-persistent-drill.sh
#
# Needs Docker, kind, kubectl, and curl. Everything created here lives in the
# named kind cluster and is deleted on exit unless KEEP is set.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cluster="${AXOND_STATEFUL_PERSISTENT_CLUSTER:-axond-stateful-persistent-drill}"
node_image="${AXOND_STATEFUL_PERSISTENT_NODE_IMAGE:-kindest/node:v1.33.4}"
postgres_image="${AXOND_STATEFUL_PERSISTENT_POSTGRES_IMAGE:-postgres:17.6-alpine}"
helper_image="${AXOND_STATEFUL_PERSISTENT_HELPER_IMAGE:-busybox:1.36.1}"
overlay="${root}/deploy/kubernetes/overlays/production-stateful-persistent"
image="${AXOND_STATEFUL_PERSISTENT_IMAGE:-}"

workdir="$(mktemp -d)"
port_forward=""
cleanup() {
  [[ -z "$port_forward" ]] || kill "$port_forward" 2>/dev/null || true
  if [[ -z "${AXOND_STATEFUL_PERSISTENT_KEEP:-}" ]]; then
    kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
  fi
  rm -rf "$workdir"
}
trap cleanup EXIT

step() { printf '\n== %s\n' "$1"; }
fail() {
  printf 'stateful persistent drill failed: %s\n' "$1" >&2
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
  image="axond-stateful-persistent-drill:$(git -C "$root" rev-parse --short HEAD 2>/dev/null || echo local)"
  docker build --tag "$image" "$root" >"${workdir}/build.log" 2>&1 ||
    fail "docker build failed: $(tail -n 20 "${workdir}/build.log")"
else
  docker image inspect "$image" >/dev/null 2>&1 || docker pull "$image" >/dev/null ||
    fail "cannot pull ${image}"
fi
ok "image ${image}"

step "Creating a three-worker cluster"
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
docker image inspect "$helper_image" >/dev/null 2>&1 ||
  docker pull "$helper_image" >/dev/null || fail "cannot pull ${helper_image}"
kind load docker-image "$helper_image" --name "$cluster" >/dev/null

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
              value: stateful-persistent-drill
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
kube -n axond create secret generic axond-secrets \
  --from-literal=GW_CONTROL_PLANE_DSN='postgres://postgres:stateful-persistent-drill@postgres.axond.svc:5432/postgres' \
  --from-literal=GW_SECRET_STORE_KEK='MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=' \
  --from-literal=GW_ADMIN_BREAKGLASS='stateful-persistent-drill-breakglass-credential' \
  --from-literal=GW_LAST_KNOWN_GOOD_KEY='MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=' \
  --dry-run=client -o yaml | kube apply -f - >/dev/null
ok "postgres and all four stateful bootstrap Secret values"

step "Applying the persistent StatefulSet overlay"
render "$overlay" |
  sed "s|ghcr.io/litvue/axond@sha256:0\{64\}|${image}|g" >"${workdir}/rendered.yaml"
[[ "$(grep -c "image: ${image}$" "${workdir}/rendered.yaml")" == 2 ]] ||
  fail "expected the StatefulSet and migration Job to share the image sentinel"
kube apply -f "${workdir}/rendered.yaml" >/dev/null

step "One Job migrates the schema, and a rerun is a no-op"
kube -n axond wait --for=condition=complete job/axond-migrate --timeout=180s >/dev/null ||
  fail "the migration Job did not complete: $(kube -n axond logs job/axond-migrate 2>&1 | tail -n 5)"
migration_log="$(kube -n axond logs -l job-name=axond-migrate --tail=-1)"
grep -q "applied .* migration" <<<"$migration_log" ||
  fail "the migration Job completed without applying anything: ${migration_log}"
kube -n axond delete job axond-migrate --cascade=foreground --wait=true >/dev/null
kube -n axond wait --for=delete pod -l job-name=axond-migrate --timeout=120s >/dev/null ||
  fail "the first migration Pod outlived its Job"
kube apply -f "${workdir}/rendered.yaml" >/dev/null
kube -n axond wait --for=condition=complete job/axond-migrate --timeout=180s >/dev/null ||
  fail "the migration rerun did not complete"
rerun_log="$(kube -n axond logs -l job-name=axond-migrate --tail=-1)"
grep -q "is current" <<<"$rerun_log" ||
  fail "the migration rerun was not a no-op: ${rerun_log}"
! grep -q "applied .* migration" <<<"$rerun_log" ||
  fail "the migration rerun applied DDL a second time: ${rerun_log}"
ok "migration is forward-only and idempotent"

step "StatefulSet ordinals boot with retained PVCs"
kube -n axond wait --for=jsonpath='{.status.phase}'=Running pod \
  -l app.kubernetes.io/name=axond --timeout=180s >/dev/null ||
  fail "no axond Pod reached Running: $(kube -n axond get pods -o wide 2>&1)"
running_containers() {
  kube -n axond get pods -l app.kubernetes.io/name=axond \
    -o jsonpath='{range .items[*]}{.status.containerStatuses[*].state.running.startedAt}{"\n"}{end}' |
    grep -c . || true
}
running=0
for _ in $(seq 1 156); do
  running="$(running_containers)"
  [[ "$running" == 3 ]] && break
  sleep 5
done
[[ "$running" == 3 ]] || fail "only ${running} of three replicas have running containers"
replicas="$(kube -n axond get statefulset axond -o jsonpath='{.spec.replicas}')"
service_name="$(kube -n axond get statefulset axond -o jsonpath='{.spec.serviceName}')"
strategy="$(kube -n axond get statefulset axond -o jsonpath='{.spec.updateStrategy.type}')"
[[ "$replicas" == 3 && "$service_name" == axond-headless && "$strategy" == OnDelete ]] ||
  fail "unexpected StatefulSet shape: replicas=${replicas}, service=${service_name}, strategy=${strategy}"
pvc_count="$(kube -n axond get pvc -l app.kubernetes.io/name=axond --no-headers | wc -l | tr -d '[:space:]')"
bound_count="$(kube -n axond get pvc -l app.kubernetes.io/name=axond \
  -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' | grep -c '^Bound$' || true)"
[[ "$pvc_count" == 3 && "$bound_count" == 3 ]] ||
  fail "expected three Bound per-replica PVCs, got ${bound_count}/${pvc_count}"
cluster_ip="$(kube -n axond get service axond-headless -o jsonpath='{.spec.clusterIP}')"
[[ "$cluster_ip" == None ]] || fail "axond-headless is not headless: ${cluster_ip}"
ok "three Running ordinals, three Bound PVCs, ${service_name}, OnDelete"

step "The mounted cache path is writable and survives Pod replacement"
ordinal="axond-0"
volume_name="$(kube -n axond get pod "$ordinal" -o jsonpath='{.spec.containers[?(@.name=="axond")].volumeMounts[?(@.mountPath=="/var/lib/axond")].name}')"
[[ "$volume_name" == last-known-good ]] ||
  fail "${ordinal} does not mount last-known-good at /var/lib/axond: ${volume_name}"
node_name="$(kube -n axond get pod "$ordinal" -o jsonpath='{.spec.nodeName}')"
marker="/mnt/cache/stateful-persistent-drill-marker"
cat >"${workdir}/marker-write.yaml" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: pvc-marker-write
  namespace: axond
spec:
  nodeName: ${node_name}
  restartPolicy: Never
  containers:
    - name: marker
      image: ${helper_image}
      command: ["sh", "-c", "printf 'pvc-survives-ordinal-replacement' > ${marker}"]
      volumeMounts:
        - name: cache
          mountPath: /mnt/cache
  volumes:
    - name: cache
      persistentVolumeClaim:
        claimName: last-known-good-axond-0
EOF
kube apply -f "${workdir}/marker-write.yaml" >/dev/null
kube -n axond wait --for=jsonpath='{.status.phase}'=Succeeded pod/pvc-marker-write --timeout=120s >/dev/null ||
  fail "the helper Pod could not write the ordinal PVC"
kube -n axond delete pod pvc-marker-write --wait=true >/dev/null
kube -n axond delete pod "$ordinal" --wait=true >/dev/null
kube -n axond wait --for=jsonpath='{.status.phase}'=Running pod "$ordinal" --timeout=180s >/dev/null ||
  fail "${ordinal} did not return after explicit replacement"
sed "s|pvc-marker-write|pvc-marker-read|g; s|printf 'pvc-survives-ordinal-replacement' > ${marker}|test -f ${marker} \&\& cat ${marker}|" \
  "${workdir}/marker-write.yaml" >"${workdir}/marker-read.yaml"
kube apply -f "${workdir}/marker-read.yaml" >/dev/null
kube -n axond wait --for=jsonpath='{.status.phase}'=Succeeded pod/pvc-marker-read --timeout=120s >/dev/null ||
  fail "the replacement ordinal could not read its retained PVC marker"
[[ "$(kube -n axond logs pod/pvc-marker-read)" == pvc-survives-ordinal-replacement ]] ||
  fail "the retained PVC marker did not survive ordinal replacement"
kube -n axond delete pod pvc-marker-read --wait=true >/dev/null
[[ "$(kube -n axond get pvc last-known-good-axond-0 -o jsonpath='{.status.phase}')" == Bound ]] ||
  fail "the ordinal PVC was not retained after Pod replacement"
ok "${ordinal} returned with its same retained PVC and cache mount"

step "Fail-closed surfaces remain intact after replacement"
admin_pod="$ordinal"
kube -n axond port-forward "pod/${admin_pod}" 18444:8080 >"${workdir}/forward.log" 2>&1 &
port_forward=$!
for _ in $(seq 1 30); do
  curl -sf -o /dev/null "http://127.0.0.1:18444/healthz" && break
  sleep 1
done
probe() { curl -s -o "${workdir}/body" -w '%{http_code}' "$@"; }
[[ "$(probe http://127.0.0.1:18444/healthz)" == 200 ]] || fail "/healthz did not answer 200"
[[ "$(probe http://127.0.0.1:18444/readyz)" == 503 ]] || fail "/readyz did not remain fail-closed"
code="$(probe -X POST -H 'content-type: application/json' -d '{}' \
  http://127.0.0.1:18444/namespaces/platform/v1/chat/completions)"
[[ "$code" == 401 ]] || fail "anonymous inference answered ${code}, not 401"
grep -q '"unauthorized"' "${workdir}/body" || fail "anonymous inference was not typed unauthorized"
ok "/namespaces/platform/v1/chat/completions remains authentication-first after replacement"

printf '%s\n' ''
printf '%s\n' 'stateful persistent drill passed: the persistent overlay migrates once,'
printf '%s\n' 'creates three retained PVC-backed ordinals, preserves an ordinal cache'
printf '%s\n' 'mount across Pod replacement, and keeps the fail-closed request boundary.'
