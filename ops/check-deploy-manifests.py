#!/usr/bin/env python3
"""Gate the shipped Kubernetes manifests on the properties they promise.

The manifests are the deployment documentation operators actually apply, so the
claims made about them in `docs/deployment/kubernetes.md` are checked against the
*rendered* output of Kustomize rather than against the source files: an overlay
patch that stops matching, a component that stops removing `replicas`, or a
digest that silently becomes a tag all render differently while every file still
looks correct on its own.

Two classes of check live here:

* internal consistency of one manifest set — every image pinned, every container
  bounded, the disruption budget survivable at the replica count it ships with;
* consistency between a manifest and the process it runs — the termination grace
  period against axond's own `[shutdown]` defaults, and the container's memory
  limit against the `[admission]` ceilings in its own ConfigMap. Those are the
  pairs that are silently wrong in production: a `SIGKILL` mid-flush loses
  buffered usage records, and admission sized above the limit turns saturation
  into an OOM kill instead of a typed `503`.

Usage:
    ops/check-deploy-manifests.py              # gate the committed manifests
    ops/check-deploy-manifests.py --self-test  # prove each check can fail
"""

from __future__ import annotations

import copy
import os
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parent.parent
BASE = ROOT / "deploy/kubernetes/base"
PRODUCTION = ROOT / "deploy/kubernetes/overlays/production"
AUTOSCALING = ROOT / "deploy/kubernetes/components/autoscaling"
KUBERNETES_DOC = ROOT / "docs/deployment/kubernetes.md"

IMAGE_REPOSITORY = "ghcr.io/litvue/axond"
# The digest the production overlay ships with. It is not a real image, and that
# is the point: an overlay applied without resolving it fails to pull rather than
# deploying whatever bytes a tag happens to point at today.
SENTINEL_DIGEST = "sha256:" + "0" * 64
SELECTOR = {"app.kubernetes.io/name": "axond"}
PRIVATE_RANGES = {"169.254.0.0/16", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}

Document = dict[str, Any]


def kustomize_command() -> list[str]:
    """`kustomize` when it is installed, otherwise the copy inside kubectl."""
    if shutil.which("kustomize"):
        return ["kustomize", "build"]
    if shutil.which("kubectl"):
        return ["kubectl", "kustomize"]
    raise RuntimeError("neither kustomize nor kubectl is available to render the manifests")


def render(directory: Path, components: tuple[Path, ...] = ()) -> list[Document]:
    """Render a manifest directory, optionally with components layered on.

    Kustomize refuses absolute roots, so a component overlay is written into a
    temporary directory that refers to the repository by relative path — the same
    way an operator's own overlay would refer to a vendored copy.
    """
    if not components:
        target = str(directory)
        cwd: Path | None = None
    else:
        holder = tempfile.mkdtemp(prefix="axond-kustomize-")
        relative = os.path.relpath(directory, holder)
        lines = [
            "apiVersion: kustomize.config.k8s.io/v1beta1",
            "kind: Kustomization",
            "resources:",
            f"  - {relative}",
            "components:",
        ]
        lines.extend(f"  - {os.path.relpath(component, holder)}" for component in components)
        Path(holder, "kustomization.yaml").write_text("\n".join(lines) + "\n", encoding="utf-8")
        target = "."
        cwd = Path(holder)
    completed = subprocess.run(  # noqa: S603 - fixed argv, no shell
        [*kustomize_command(), target],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
    )
    return [document for document in yaml.safe_load_all(completed.stdout) if document]


def of_kind(documents: list[Document], kind: str) -> list[Document]:
    return [document for document in documents if document.get("kind") == kind]


def one(documents: list[Document], kind: str) -> Document:
    found = of_kind(documents, kind)
    if len(found) != 1:
        raise AssertionError(f"expected exactly one {kind}, found {len(found)}")
    return found[0]


def containers(deployment: Document) -> list[Document]:
    return deployment["spec"]["template"]["spec"].get("containers", [])


def gateway_config(documents: list[Document]) -> dict[str, Any]:
    """The `axond.toml` the manifests mount, parsed as the gateway would read it."""
    config_map = one(documents, "ConfigMap")
    return tomllib.loads(config_map["data"]["axond.toml"])


def shutdown_budget_ms() -> int:
    """The shipped `[shutdown]` bounds, read from the gateway's own defaults.

    Hardcoding 25s here would make this gate agree with a stale copy of the
    defaults rather than with the process the manifest runs.
    """
    source = (ROOT / "crates/gateway/src/config.rs").read_text(encoding="utf-8")
    total = 0
    for field in ("drain_grace_ms", "deadline_ms", "flush_timeout_ms"):
        match = re.search(
            rf"fn default_shutdown_{field}\(\) -> u64 \{{\s*([\d_]+)\s*\}}",
            source,
        )
        if match is None:
            raise RuntimeError(f"could not read default_shutdown_{field} from config.rs")
        total += int(match.group(1).replace("_", ""))
    return total


def check_image_pinning(base: list[Document], production: list[Document]) -> list[str]:
    """The base names a released version; production names one immutable digest.

    A tag is a name that can be repointed at different bytes after the review
    that approved it, so it is acceptable in the evaluation base — where the
    release marker keeps it truthful — and not in the overlay that carries the
    word production.
    """
    failures: list[str] = []
    for deployment in of_kind(base, "Deployment"):
        for container in containers(deployment):
            image = container["image"]
            if "@sha256:" in image:
                continue
            if not image.startswith(f"{IMAGE_REPOSITORY}:"):
                failures.append(f"base: container {container['name']!r} image {image!r} is not an axond release")
            if image.endswith(":latest") or ":" not in image:
                failures.append(f"base: container {container['name']!r} image {image!r} is not pinned to a version")
    for deployment in of_kind(production, "Deployment"):
        for container in containers(deployment):
            image = container["image"]
            repository, _, digest = image.partition("@")
            if not digest.startswith("sha256:") or len(digest) != len(SENTINEL_DIGEST):
                failures.append(
                    f"overlays/production: container {container['name']!r} image {image!r} "
                    "is not pinned by digest"
                )
            elif repository != IMAGE_REPOSITORY:
                failures.append(
                    f"overlays/production: container {container['name']!r} image {image!r} "
                    f"is not {IMAGE_REPOSITORY}"
                )
            elif digest != SENTINEL_DIGEST:
                failures.append(
                    f"overlays/production: container {container['name']!r} pins {digest!r}; the "
                    "committed overlay must carry the unresolvable sentinel so a stale digest "
                    "cannot ship as a reviewed one (resolve it with ops/pin-image-digest.sh)"
                )
    return failures


def check_termination_budget(documents: list[Document], label: str) -> list[str]:
    """`terminationGracePeriodSeconds` exceeds every bound that runs inside it.

    A `preStop` hook runs *before* `SIGTERM`, so its sleep adds to the process's
    own drain, deadline, and flush rather than overlapping them.
    """
    budget_ms = shutdown_budget_ms()
    failures: list[str] = []
    for deployment in of_kind(documents, "Deployment"):
        pod = deployment["spec"]["template"]["spec"]
        grace = pod.get("terminationGracePeriodSeconds")
        if grace is None:
            failures.append(f"{label}: the Deployment does not set terminationGracePeriodSeconds")
            continue
        for container in containers(deployment):
            hook = container.get("lifecycle", {}).get("preStop", {})
            sleep_seconds = hook.get("sleep", {}).get("seconds", 0)
            if "exec" in hook:
                failures.append(
                    f"{label}: container {container['name']!r} uses a preStop exec hook; the "
                    "distroless image has no shell, so the sleep lifecycle action is the only "
                    "hook that runs"
                )
            required = sleep_seconds + budget_ms / 1000
            if grace <= required:
                failures.append(
                    f"{label}: terminationGracePeriodSeconds {grace} does not exceed the "
                    f"{required:g}s a drain takes (preStop {sleep_seconds}s plus the "
                    f"{budget_ms / 1000:g}s [shutdown] budget); a SIGKILL there loses buffered "
                    "usage records"
                )
    return failures


def check_resources(documents: list[Document], label: str) -> list[str]:
    """Every container is bounded, and the bound fits the admission ceilings.

    `max_in_flight` bodies of `max_request_bytes` are held in process memory at
    once, and a buffered body costs several times its own size once parsed,
    re-serialized for the usage estimate, and cloned per failover attempt. Raw
    bodies are held to a quarter of the limit so the rest of that cost, and the
    runtime, are not racing the limit.
    """
    admission = gateway_config(documents).get("admission", {})
    failures: list[str] = []
    for deployment in of_kind(documents, "Deployment"):
        for container in containers(deployment):
            resources = container.get("resources", {})
            for section in ("requests", "limits"):
                for resource in ("cpu", "memory"):
                    if resource not in resources.get(section, {}):
                        failures.append(
                            f"{label}: container {container['name']!r} declares no "
                            f"{section}.{resource}"
                        )
            limit = resources.get("limits", {}).get("memory")
            if limit is None or not admission:
                continue
            limit_bytes = parse_quantity(limit)
            worst_case = admission["max_in_flight"] * admission["max_request_bytes"]
            if worst_case * 4 > limit_bytes:
                failures.append(
                    f"{label}: [admission] admits {admission['max_in_flight']} bodies of "
                    f"{admission['max_request_bytes']} bytes, which does not fit a quarter of "
                    f"container {container['name']!r}'s {limit} memory limit; raise the limit or "
                    "lower the ceilings together"
                )
    return failures


def parse_quantity(quantity: str | int) -> int:
    """Bytes from a Kubernetes memory quantity (`512Mi`, `1Gi`, `1000000`)."""
    text = str(quantity)
    suffixes = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3, "K": 10**3, "M": 10**6, "G": 10**9}
    for suffix, multiplier in suffixes.items():
        if text.endswith(suffix):
            return int(float(text[: -len(suffix)]) * multiplier)
    return int(text)


def check_topology_spread(documents: list[Document]) -> list[str]:
    """Replicas are spread across nodes at minimum, and zones where they exist."""
    deployment = one(documents, "Deployment")
    constraints = deployment["spec"]["template"]["spec"].get("topologySpreadConstraints", [])
    by_key = {constraint.get("topologyKey"): constraint for constraint in constraints}
    failures: list[str] = []
    for key in ("kubernetes.io/hostname", "topology.kubernetes.io/zone"):
        if key not in by_key:
            failures.append(f"overlays/production: no topology spread constraint on {key}")
    node = by_key.get("kubernetes.io/hostname")
    if node is not None and node.get("whenUnsatisfiable") != "DoNotSchedule":
        failures.append(
            "overlays/production: the per-node spread constraint is best-effort; two replicas "
            "of a fleet sized to survive one disruption must not share a node"
        )
    for key, constraint in by_key.items():
        selector = constraint.get("labelSelector", {}).get("matchLabels")
        if selector != SELECTOR:
            failures.append(
                f"overlays/production: the {key} spread constraint selects {selector!r} rather "
                f"than the Deployment's own {SELECTOR!r}, so it spreads a different set of Pods"
            )
    return failures


def check_network_policies(documents: list[Document]) -> list[str]:
    """Default deny in both directions, and no allowance back into the cluster.

    The open-internet egress rule is what lets a gateway reach provider APIs; the
    exceptions on it are what keep the same rule from reaching the cloud metadata
    service or a neighbouring service on a private address.
    """
    policies = of_kind(documents, "NetworkPolicy")
    failures: list[str] = []
    denies = [
        policy
        for policy in policies
        if not policy["spec"].get("ingress") and not policy["spec"].get("egress")
    ]
    if not denies:
        failures.append(
            "overlays/production: no NetworkPolicy denies by default; a policy that only allows "
            "leaves every unnamed flow open"
        )
    for policy in denies:
        if sorted(policy["spec"].get("policyTypes", [])) != ["Egress", "Ingress"]:
            failures.append(
                f"overlays/production: NetworkPolicy {policy['metadata']['name']!r} does not deny "
                "both directions"
            )
    for policy in policies:
        if policy["spec"].get("podSelector", {}).get("matchLabels") != SELECTOR:
            failures.append(
                f"overlays/production: NetworkPolicy {policy['metadata']['name']!r} does not "
                f"select {SELECTOR!r}"
            )
        for rule in policy["spec"].get("ingress", []):
            if not rule.get("from"):
                failures.append(
                    f"overlays/production: NetworkPolicy {policy['metadata']['name']!r} admits "
                    "ingress from every source"
                )
        for rule in policy["spec"].get("egress", []):
            for peer in rule.get("to", []):
                block = peer.get("ipBlock")
                if block is None or block.get("cidr") != "0.0.0.0/0":
                    continue
                missing = PRIVATE_RANGES - set(block.get("except", []))
                if missing:
                    failures.append(
                        f"overlays/production: NetworkPolicy {policy['metadata']['name']!r} "
                        f"allows egress to {sorted(missing)} through an open-internet rule; the "
                        "metadata service and the private ranges stay excepted"
                    )
    return failures


def check_service_port(documents: list[Document], label: str) -> list[str]:
    """The Service, the container port, and `[server] bind` name one port."""
    config = gateway_config(documents)
    bind_port = int(config["server"]["bind"].rsplit(":", 1)[1])
    failures: list[str] = []
    for container in containers(one(documents, "Deployment")):
        ports = {port["containerPort"] for port in container.get("ports", [])}
        if bind_port not in ports:
            failures.append(
                f"{label}: container {container['name']!r} exposes {sorted(ports)} but the "
                f"mounted config binds {bind_port}"
            )
    for port in one(documents, "Service").get("spec", {}).get("ports", []):
        if port.get("port") != bind_port:
            failures.append(
                f"{label}: the Service publishes {port.get('port')} but the mounted config "
                f"binds {bind_port}"
            )
    for policy in of_kind(documents, "NetworkPolicy"):
        for rule in policy["spec"].get("ingress", []):
            for port in rule.get("ports", []):
                if port.get("port") not in (bind_port, None):
                    failures.append(
                        f"{label}: NetworkPolicy {policy['metadata']['name']!r} admits ingress on "
                        f"{port.get('port')}, which is not the port the gateway binds"
                    )
    return failures


def check_disruption_budget(documents: list[Document], autoscaled: list[Document]) -> list[str]:
    """A drain can always proceed: budget, replica count, and HPA floor agree.

    `minAvailable` equal to the replica count is a deadlocked node drain rather
    than a strict guarantee, and an autoscaler allowed to scale below
    `minAvailable + 1` reintroduces the same deadlock at its floor.
    """
    deployment = one(documents, "Deployment")
    budget = one(documents, "PodDisruptionBudget")
    minimum = budget["spec"]["minAvailable"]
    replicas = deployment["spec"].get("replicas")
    failures: list[str] = []
    if replicas is None:
        failures.append("overlays/production: the Deployment does not declare replicas")
    elif replicas < minimum + 1:
        failures.append(
            f"overlays/production: {replicas} replicas against minAvailable {minimum} leaves no "
            "room for a voluntary disruption"
        )
    strategy = deployment["spec"].get("strategy", {}).get("rollingUpdate", {})
    if strategy.get("maxUnavailable") != 0:
        failures.append(
            "overlays/production: the rolling update may take a replica below the fleet size "
            "the disruption budget assumes"
        )
    if one(autoscaled, "Deployment")["spec"].get("replicas") is not None:
        failures.append(
            "components/autoscaling: the Deployment still declares replicas, so every apply "
            "fights the autoscaler for the field"
        )
    autoscaler = one(autoscaled, "HorizontalPodAutoscaler")
    floor = autoscaler["spec"]["minReplicas"]
    if floor < minimum + 1:
        failures.append(
            f"components/autoscaling: minReplicas {floor} can scale the fleet to where "
            f"minAvailable {minimum} blocks a node drain"
        )
    if autoscaler["spec"]["maxReplicas"] < floor:
        failures.append("components/autoscaling: maxReplicas is below minReplicas")
    target = autoscaler["spec"]["scaleTargetRef"]
    if (target.get("kind"), target.get("name")) != ("Deployment", deployment["metadata"]["name"]):
        failures.append("components/autoscaling: the HPA does not target the axond Deployment")
    return failures


def check_namespaces(documents: list[Document], label: str) -> list[str]:
    """Nothing lands in `default` because a patch forgot a namespace."""
    namespace = one(documents, "Namespace")["metadata"]["name"]
    return [
        f"{label}: {document['kind']} {document['metadata']['name']!r} is not in the "
        f"{namespace!r} namespace"
        for document in documents
        if document.get("kind") != "Namespace"
        and document.get("metadata", {}).get("namespace") != namespace
    ]


def check_documented() -> list[str]:
    """The operator-facing page names the paths and the sentinel workflow."""
    page = KUBERNETES_DOC.read_text(encoding="utf-8")
    failures: list[str] = []
    for path in (
        "deploy/kubernetes/base",
        "deploy/kubernetes/overlays/production",
        "deploy/kubernetes/components/autoscaling",
        "ops/pin-image-digest.sh",
    ):
        if path not in page:
            failures.append(f"docs/deployment/kubernetes.md: {path} is not documented")
    return failures


def check_sentinel_refused() -> list[str]:
    """`ops/pin-image-digest.sh --check` refuses the committed sentinel.

    The overlay ships unapplyable on purpose, so the script that says so is part
    of the gate: a `--check` that passed here would let a rollout apply a
    placeholder digest.
    """
    completed = subprocess.run(  # noqa: S603 - fixed argv, no shell
        ["bash", str(ROOT / "ops/pin-image-digest.sh"), "--check"],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode == 0:
        return [
            "ops/pin-image-digest.sh --check accepted the committed overlay; it must refuse the "
            "unresolved sentinel digest"
        ]
    return []


def gate(base: list[Document], production: list[Document], autoscaled: list[Document]) -> list[str]:
    return [
        *check_image_pinning(base, production),
        *check_termination_budget(base, "base"),
        *check_termination_budget(production, "overlays/production"),
        *check_resources(base, "base"),
        *check_resources(production, "overlays/production"),
        *check_service_port(base, "base"),
        *check_service_port(production, "overlays/production"),
        *check_topology_spread(production),
        *check_network_policies(production),
        *check_disruption_budget(production, autoscaled),
        *check_namespaces(base, "base"),
        *check_namespaces(production, "overlays/production"),
    ]


def self_test() -> int:
    """Prove each check fails on the manifest mistake it exists to catch."""
    base = render(BASE)
    production = render(PRODUCTION)
    autoscaled = render(PRODUCTION, (AUTOSCALING,))
    failures: list[str] = []

    def expect_failure(name: str, produced: list[str]) -> None:
        if not produced:
            failures.append(f"self-test: {name} did not fail on a manifest it must reject")

    if gate(base, production, autoscaled):
        failures.append("self-test: the committed manifests must pass the gate")

    tagged = copy.deepcopy(production)
    containers(one(tagged, "Deployment"))[0]["image"] = f"{IMAGE_REPOSITORY}:latest"
    expect_failure("image pinning (mutable tag)", check_image_pinning(base, tagged))

    resolved = copy.deepcopy(production)
    containers(one(resolved, "Deployment"))[0]["image"] = f"{IMAGE_REPOSITORY}@sha256:{'a' * 64}"
    expect_failure("image pinning (committed digest)", check_image_pinning(base, resolved))

    untagged_base = copy.deepcopy(base)
    containers(one(untagged_base, "Deployment"))[0]["image"] = IMAGE_REPOSITORY
    expect_failure("image pinning (base without a version)", check_image_pinning(untagged_base, production))

    hurried = copy.deepcopy(production)
    one(hurried, "Deployment")["spec"]["template"]["spec"]["terminationGracePeriodSeconds"] = 10
    expect_failure("termination budget", check_termination_budget(hurried, "overlays/production"))

    shelled = copy.deepcopy(production)
    containers(one(shelled, "Deployment"))[0]["lifecycle"]["preStop"] = {
        "exec": {"command": ["sleep", "5"]}
    }
    expect_failure("preStop exec hook", check_termination_budget(shelled, "overlays/production"))

    unbounded = copy.deepcopy(production)
    containers(one(unbounded, "Deployment"))[0]["resources"].pop("limits")
    expect_failure("resource bounds", check_resources(unbounded, "overlays/production"))

    greedy = copy.deepcopy(production)
    config = one(greedy, "ConfigMap")
    config["data"]["axond.toml"] = config["data"]["axond.toml"].replace(
        "max_in_flight = 32", "max_in_flight = 512"
    )
    expect_failure("admission against the memory limit", check_resources(greedy, "overlays/production"))

    moved = copy.deepcopy(production)
    one(moved, "Service")["spec"]["ports"][0]["port"] = 9090
    expect_failure("service port", check_service_port(moved, "overlays/production"))

    flat = copy.deepcopy(production)
    one(flat, "Deployment")["spec"]["template"]["spec"]["topologySpreadConstraints"] = [
        constraint
        for constraint in one(flat, "Deployment")["spec"]["template"]["spec"][
            "topologySpreadConstraints"
        ]
        if constraint["topologyKey"] != "kubernetes.io/hostname"
    ]
    expect_failure("topology spread", check_topology_spread(flat))

    stacked = copy.deepcopy(production)
    for constraint in one(stacked, "Deployment")["spec"]["template"]["spec"][
        "topologySpreadConstraints"
    ]:
        constraint["whenUnsatisfiable"] = "ScheduleAnyway"
    expect_failure("per-node spread enforcement", check_topology_spread(stacked))

    open_egress = copy.deepcopy(production)
    for policy in of_kind(open_egress, "NetworkPolicy"):
        for rule in policy["spec"].get("egress", []):
            for peer in rule.get("to", []):
                if peer.get("ipBlock", {}).get("cidr") == "0.0.0.0/0":
                    peer["ipBlock"].pop("except")
    expect_failure("private-range egress", check_network_policies(open_egress))

    open_ingress = copy.deepcopy(production)
    for policy in of_kind(open_ingress, "NetworkPolicy"):
        for rule in policy["spec"].get("ingress", []):
            rule.pop("from", None)
    expect_failure("unrestricted ingress", check_network_policies(open_ingress))

    allow_only = copy.deepcopy(production)
    allow_only[:] = [
        document
        for document in allow_only
        if document.get("kind") != "NetworkPolicy" or document["spec"].get("egress")
    ]
    expect_failure("default deny", check_network_policies(allow_only))

    tight = copy.deepcopy(production)
    one(tight, "PodDisruptionBudget")["spec"]["minAvailable"] = 3
    expect_failure("disruption budget", check_disruption_budget(tight, autoscaled))

    contended = copy.deepcopy(autoscaled)
    one(contended, "Deployment")["spec"]["replicas"] = 3
    expect_failure("autoscaled replica ownership", check_disruption_budget(production, contended))

    shrunk = copy.deepcopy(autoscaled)
    one(shrunk, "HorizontalPodAutoscaler")["spec"]["minReplicas"] = 1
    expect_failure("autoscaler floor", check_disruption_budget(production, shrunk))

    stray = copy.deepcopy(production)
    one(stray, "Service")["metadata"].pop("namespace")
    expect_failure("namespace placement", check_namespaces(stray, "overlays/production"))

    for failure in failures:
        print(failure, file=sys.stderr)
    if failures:
        print(f"{len(failures)} self-test failure(s)", file=sys.stderr)
        return 1
    print("deployment manifest gate self-test passed")
    return 0


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        return self_test()
    failures = [
        *gate(render(BASE), render(PRODUCTION), render(PRODUCTION, (AUTOSCALING,))),
        *check_documented(),
        *check_sentinel_refused(),
    ]
    for failure in failures:
        print(failure, file=sys.stderr)
    if failures:
        print(f"{len(failures)} deployment manifest failure(s)", file=sys.stderr)
        return 1
    print("deployment manifests checked")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
