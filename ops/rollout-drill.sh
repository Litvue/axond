#!/usr/bin/env bash
# Prove that the production overlay rolls out on a cluster with as many nodes as
# replicas, and that the property which makes that true is the one the manifest
# names.
#
# `ops/check-deploy-manifests.py` asserts the *shape* of the overlay's spread
# constraints. Shape is not scheduling: whether a rolling update completes is
# decided by kube-scheduler counting skew across ReplicaSets, and no rendered
# manifest can answer that. So this drill runs a real three-worker cluster and a
# real rolling update, twice:
#
#   1. as shipped — `maxSurge: 1`, `maxUnavailable: 0`, a hard per-node spread
#      scoped with `matchLabelKeys: [pod-template-hash]` — and the rollout must
#      finish;
#   2. with `matchLabelKeys` removed, and the surge Pod must stay `Pending`. That
#      is the deadlock the scoping exists to prevent: the fourth Pod cannot be
#      placed without exceeding the fleet-wide skew, and `maxUnavailable: 0`
#      never frees a node for it.
#
# The second half is the half that matters. Without it, a future change that
# silently un-scopes the skew would still pass the first, and the failure would
# arrive during an upgrade of a production cluster instead of here.
#
# The digest sentinel in the overlay is replaced with a runnable image reference
# for the drill only, in a rendered copy; the checked-in overlay is never
# written to, because an operator resolving a release digest is what pins it.
#
# Usage:
#     ops/rollout-drill.sh                                  # ~3 minutes
#     AXOND_ROLLOUT_IMAGE=ghcr.io/litvue/axond:0.4.0 ops/rollout-drill.sh
#     AXOND_ROLLOUT_KEEP=1 ops/rollout-drill.sh             # keep the cluster
#
# Needs Docker, `kind`, and `kubectl`; `kustomize` if kubectl's built-in copy is
# too old to render the overlay. Everything it creates lives in its own kind
# cluster and is deleted on exit.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cluster="${AXOND_ROLLOUT_CLUSTER:-axond-rollout-drill}"
node_image="${AXOND_ROLLOUT_NODE_IMAGE:-kindest/node:v1.33.4}"
overlay="${root}/deploy/kubernetes/overlays/production"
# The published image for the version in this workspace, so the drill runs the
# gateway the overlay would deploy rather than a stand-in that cannot fail its
# own readiness probe.
version="$(sed -n '/^\[workspace\.package\]/,/^\[/s/^version *= *"\([^"]*\)".*/\1/p' \
  "${root}/Cargo.toml" | head -n 1)"
image="${AXOND_ROLLOUT_IMAGE:-ghcr.io/litvue/axond:${version}}"

workdir="$(mktemp -d)"
cleanup() {
  if [[ -z "${AXOND_ROLLOUT_KEEP:-}" ]]; then
    kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
  fi
  rm -rf "$workdir"
}
trap cleanup EXIT

step() { printf '\n== %s\n' "$1"; }
fail() {
  printf 'rollout drill failed: %s\n' "$1" >&2
  exit 1
}
ok() { printf '  ok  %s\n' "$1"; }

for tool in docker kind kubectl; do
  command -v "$tool" >/dev/null 2>&1 || fail "${tool} is required"
done

kube() { kubectl --context "kind-${cluster}" "$@"; }
# Kustomize proper when it is installed, kubectl's built-in copy otherwise — the
# same either-or as the manifest gate, so the drill needs no extra tool.
render() {
  if command -v kustomize >/dev/null 2>&1; then
    kustomize build "$1"
  else
    kubectl kustomize "$1"
  fi
}

step "Creating a three-worker cluster"
# Three workers and a tainted control plane: exactly `replicas` schedulable
# nodes, which is the cluster shape the deadlock needs.
#
# The control-plane node is part of that shape, not incidental to it. Topology
# spread counts domains, and the default `nodeTaintsPolicy: Ignore` counts a
# tainted node's domain too, so the control plane is a fourth hostname domain
# holding zero axond Pods: the global minimum is 0, and a surge Pod on any
# worker would take that worker to a skew of 2. Give the constraint
# `nodeTaintsPolicy: Honor`, or let something schedule onto the control plane,
# and the unscoped counterfactual below converges instead of deadlocking.
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

step "Loading ${image}"
docker image inspect "$image" >/dev/null 2>&1 || docker pull "$image" >/dev/null ||
  fail "cannot pull ${image}; set AXOND_ROLLOUT_IMAGE to a published tag"
kind load docker-image "$image" --name "$cluster" >/dev/null

step "Applying the production overlay"
# `kustomize edit` is not used: the sentinel is replaced in the rendered output,
# so nothing under deploy/ changes and a failed drill cannot leave the overlay
# pinned to a tag.
render "$overlay" |
  sed "s|ghcr.io/litvue/axond@sha256:0\{64\}|${image}|" >"${workdir}/rendered.yaml"
grep -q "image: ${image}$" "${workdir}/rendered.yaml" ||
  fail "the overlay's image is not the digest sentinel any more; update this drill"
kube apply -f "${workdir}/rendered.yaml" >/dev/null
# The overlay deletes the base's published example Secret, so the credentials an
# operator supplies out of band are supplied here too — throwaway values, on a
# cluster this script deletes. Without it the Pods sit in
# CreateContainerConfigError, which is the overlay working as intended.
kube -n axond create secret generic axond-secrets \
  --from-literal=GW_PLATFORM_OPENAI_API_KEY=rollout-drill-openai-key \
  --from-literal=GW_INBOUND_PLATFORM_KEY=rollout-drill-inbound-key \
  --dry-run=client -o yaml | kube apply -f - >/dev/null
# The whole overlay is applied, NetworkPolicies included, so the drill deploys
# what an operator would rather than a subset. kindnet does not implement
# NetworkPolicy, so they are inert here — a CNI that enforced them and did not
# exempt kubelet probe traffic would fail readiness rather than scheduling, which
# is why that is called out below instead of being read as a spread failure.
kube -n axond rollout status deployment/axond --timeout=240s || {
  kube -n axond get pods -o wide >&2 || true
  fail "the initial rollout did not converge; if the Pods are Running but never Ready,
this cluster's CNI enforces the overlay's default-deny NetworkPolicy against
kubelet probes and needs its node CIDR allowed (see the policy's own comment)"
}

nodes="$(kube -n axond get pods -o jsonpath='{.items[*].spec.nodeName}' | tr ' ' '\n' |
  sort -u | wc -l)"
[[ "$nodes" == 3 ]] || fail "expected one replica per node, got ${nodes} distinct nodes"
ok "three replicas on three distinct nodes"

step "Rolling update as shipped: it has to finish"
kube -n axond patch deployment axond --type=json \
  -p='[{"op":"add","path":"/spec/template/metadata/annotations","value":{"axond.dev/drill":"1"}}]' \
  >/dev/null
kube -n axond rollout status deployment/axond --timeout=240s ||
  fail "the rolling update deadlocked with the ReplicaSet-scoped spread; the surge \
Pod could not be placed"
ok "the rollout converged with matchLabelKeys: [pod-template-hash]"

step "The counterfactual: fleet-wide skew has to deadlock"
# Same cluster, same rollout, one property removed. If this converges, the skew
# is no longer what keeps the shipped rollout schedulable and the assertion above
# proves nothing.
scoped="$(kube -n axond get deployment axond \
  -o jsonpath='{.spec.template.spec.topologySpreadConstraints}')"
kube -n axond patch deployment axond --type=json -p='[
  {"op":"remove","path":"/spec/template/spec/topologySpreadConstraints/1/matchLabelKeys"},
  {"op":"replace","path":"/spec/template/metadata/annotations/axond.dev~1drill","value":"2"}
]' >/dev/null
if kube -n axond rollout status deployment/axond --timeout=90s >/dev/null 2>&1; then
  fail "a fleet-wide hard spread rolled out on a three-node cluster, so this \
drill no longer demonstrates why the skew is scoped"
fi
pending="$(kube -n axond get pods \
  -o jsonpath='{range .items[?(@.status.phase=="Pending")]}{.metadata.name}{"\n"}{end}' |
  wc -l)"
[[ "$pending" == 1 ]] || fail "expected exactly one unschedulable surge Pod, got ${pending}"
kube -n axond get events --field-selector reason=FailedScheduling \
  -o jsonpath='{.items[*].message}' | grep -q "topology spread constraints" ||
  fail "the surge Pod is Pending for some reason other than topology spread"
ok "the surge Pod is Pending on 'didn't match pod topology spread constraints'"

step "Restoring the scoped constraint: the same rollout has to recover"
# Not cleanup — evidence. The deadlock is a scheduling decision, not a failed
# Pod, so re-scoping the skew is enough to let the pending Pod through, which is
# also the fix an operator would apply mid-incident.
kube -n axond patch deployment axond --type=json \
  -p="[{\"op\":\"replace\",\"path\":\"/spec/template/spec/topologySpreadConstraints\",\"value\":${scoped}}]" \
  >/dev/null
kube -n axond rollout status deployment/axond --timeout=240s ||
  fail "re-scoping the skew did not release the pending surge Pod"
ok "re-scoping the skew released the rollout"

printf '\nrollout drill passed: the production overlay completes a rolling update on\n'
printf 'three nodes, and the same rollout deadlocks once the per-node skew stops\n'
printf 'being counted per ReplicaSet.\n'
