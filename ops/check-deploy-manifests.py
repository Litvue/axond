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
* consistency between a documented policy and the thing that enforces it — the
  supported Postgres/Redis versions against the version the gateway refuses below
  and the images CI actually exercises, and the recovery drill against the lane
  that runs it;
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
import runpy
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import yaml

# `tomllib` is standard from 3.11; on the repository's 3.10 floor the lockfile
# supplies the backport it was extracted from.
try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised only on Python 3.10
    import tomli as tomllib

ROOT = Path(__file__).resolve().parent.parent
BASE = ROOT / "deploy/kubernetes/base"
PRODUCTION = ROOT / "deploy/kubernetes/overlays/production"
AUTOSCALING = ROOT / "deploy/kubernetes/components/autoscaling"
PRODUCTION_STATEFUL = ROOT / "deploy/kubernetes/overlays/production-stateful"
PRODUCTION_STATEFUL_PERSISTENT = (
    ROOT / "deploy/kubernetes/overlays/production-stateful-persistent"
)
KUBERNETES_DOC = ROOT / "docs/deployment/kubernetes.md"
STATEFUL_DOC = ROOT / "docs/deployment/stateful-backends.md"
RECOVERY_DOC = ROOT / "docs/operations/backup-and-recovery.md"
CI_WORKFLOW = ROOT / ".github/workflows/ci.yml"
SCHEMA_SOURCE = ROOT / "crates/gateway/src/backends/control_plane/schema.rs"
REVOCATION_SOURCE = ROOT / "crates/gateway/src/revocation/redis.rs"
ROLLOUT_DRILL = ROOT / "ops/rollout-drill.sh"
STATEFUL_DRILL = ROOT / "ops/stateful-deploy-drill.sh"
STATEFUL_PERSISTENT_DRILL = ROOT / "ops/stateful-persistent-drill.sh"
TELEMETRY_SOURCE = ROOT / "crates/gateway/src/telemetry/mod.rs"

IMAGE_REPOSITORY = "ghcr.io/litvue/axond"
# The digest the production overlay ships with. It is not a real image, and that
# is the point: an overlay applied without resolving it fails to pull rather than
# deploying whatever bytes a tag happens to point at today.
SENTINEL_DIGEST = "sha256:" + "0" * 64
SELECTOR = {"app.kubernetes.io/name": "axond"}
# gcr.io/distroless/static-debian12:nonroot's documented nonroot UID/GID.
DISTROLESS_NONROOT_GROUP = 65532
PRIVATE_RANGES = {"169.254.0.0/16", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}
# The Redis release that added `SET … PXAT`, which the revocation write uses and
# which is therefore what the documented floor has to say.
REDIS_PXAT_FLOOR = "6.2"

Document = dict[str, Any]


def kustomize_command() -> list[str]:
    """`kustomize` when it is installed, otherwise the copy inside kubectl."""
    if shutil.which("kustomize"):
        return ["kustomize", "build"]
    if shutil.which("kubectl"):
        return ["kubectl", "kustomize"]
    raise SystemExit(
        "check-deploy-manifests: neither kustomize nor kubectl is installed, and this gate "
        "renders the overlays rather than reading them; install one of the two (the CI runner "
        "image ships both)"
    )


def render(directory: Path, components: tuple[Path, ...] = ()) -> list[Document]:
    """Render a manifest directory, optionally with components layered on.

    Kustomize refuses absolute roots, so a component overlay is written into a
    temporary directory that refers to the repository by relative path — the same
    way an operator's own overlay would refer to a vendored copy.
    """
    if not components:
        return kustomize(str(directory), None)
    with tempfile.TemporaryDirectory(prefix="axond-kustomize-") as holder:
        # On macOS, the repository may be addressed as `/private/tmp` while
        # tempfile returns its `/var/folders` symlink spelling. Compute both
        # sides from resolved paths or the relative resource becomes
        # `/private/private/...` inside kubectl's cwd.
        holder_path = Path(holder).resolve()
        lines = [
            "apiVersion: kustomize.config.k8s.io/v1beta1",
            "kind: Kustomization",
            "resources:",
            f"  - {os.path.relpath(directory.resolve(), holder_path)}",
            "components:",
        ]
        lines.extend(
            f"  - {os.path.relpath(component.resolve(), holder_path)}" for component in components
        )
        (holder_path / "kustomization.yaml").write_text(
            "\n".join(lines) + "\n", encoding="utf-8"
        )
        return kustomize(".", holder_path)


def kustomize(target: str, cwd: Path | None) -> list[Document]:
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


WORKLOAD_KINDS = ("Deployment", "StatefulSet")


def workload_documents(documents: list[Document]) -> list[Document]:
    """Return the controller workloads whose Pod templates the gate checks."""
    return [document for document in documents if document.get("kind") in WORKLOAD_KINDS]


def one_workload(documents: list[Document]) -> Document:
    found = workload_documents(documents)
    if len(found) != 1:
        raise AssertionError(f"expected exactly one workload, found {len(found)}")
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
    for deployment in workload_documents(documents):
        pod = deployment["spec"]["template"]["spec"]
        grace = pod.get("terminationGracePeriodSeconds")
        if grace is None:
            failures.append(
                f"{label}: the {deployment['kind']} does not set terminationGracePeriodSeconds"
            )
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
    for deployment in workload_documents(documents):
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
            if limit is None:
                continue
            if not all(key in admission for key in ("max_in_flight", "max_request_bytes")):
                failures.append(
                    f"{label}: the mounted axond.toml declares no [admission] "
                    "max_in_flight/max_request_bytes, so the gateway runs on its compiled-in "
                    "ceilings and nothing pairs them with "
                    f"container {container['name']!r}'s {limit} memory limit; keep the ceilings "
                    "in the ConfigMap"
                )
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


def check_topology_spread(documents: list[Document], label: str) -> list[str]:
    """Replicas are spread across nodes at minimum, and zones where they exist."""
    deployment = one_workload(documents)
    constraints = deployment["spec"]["template"]["spec"].get("topologySpreadConstraints", [])
    by_key = {constraint.get("topologyKey"): constraint for constraint in constraints}
    failures: list[str] = []
    for key in ("kubernetes.io/hostname", "topology.kubernetes.io/zone"):
        if key not in by_key:
            failures.append(f"{label}: no topology spread constraint on {key}")
    node = by_key.get("kubernetes.io/hostname")
    if node is not None and node.get("whenUnsatisfiable") != "DoNotSchedule":
        failures.append(
            f"{label}: the per-node spread constraint is best-effort; two replicas "
            "of a fleet sized to survive one disruption must not share a node"
        )
    # A hard constraint that counts the Pods it is replacing deadlocks against
    # `maxUnavailable: 0` on a cluster with as many nodes as replicas: the surge
    # Pod is unschedulable and nothing may be evicted to make room for it.
    if (
        deployment["kind"] == "Deployment"
        and node is not None
        and "pod-template-hash" not in node.get("matchLabelKeys", [])
    ):
        failures.append(
            f"{label}: the per-node spread constraint counts every axond Pod, so a "
            "rolling update's surge Pod exceeds its own skew and never schedules; scope the skew "
            "with matchLabelKeys: [pod-template-hash]"
        )
    for key, constraint in by_key.items():
        selector = constraint.get("labelSelector", {}).get("matchLabels")
        if selector != SELECTOR:
            failures.append(
                f"{label}: the {key} spread constraint selects {selector!r} rather "
                f"than the workload's own {SELECTOR!r}, so it spreads a different set of Pods"
            )
    return failures


def check_network_policies(
    documents: list[Document], label: str, workloads: tuple[Document, ...] = (SELECTOR,)
) -> list[str]:
    """Default deny in both directions, and no allowance back into the cluster.

    The open-internet egress rule is what lets a gateway reach provider APIs; the
    exceptions on it are what keep the same rule from reaching the cloud metadata
    service or a neighbouring service on a private address.

    `workloads` is every Pod label set the overlay runs. An overlay that adds a
    Pod adds a selector here too: a default deny that covers the gateway says
    nothing about the migration Pod holding the control-plane DSN beside it.
    """
    policies = of_kind(documents, "NetworkPolicy")
    failures: list[str] = []
    denies = [
        policy
        for policy in policies
        if not policy["spec"].get("ingress") and not policy["spec"].get("egress")
    ]
    for workload in workloads:
        if not any(
            policy["spec"].get("podSelector", {}).get("matchLabels") == workload
            for policy in denies
        ):
            failures.append(
                f"{label}: no NetworkPolicy denies by default for {workload!r}; a policy that "
                "only allows leaves every unnamed flow open"
            )
    for policy in denies:
        if sorted(policy["spec"].get("policyTypes", [])) != ["Egress", "Ingress"]:
            failures.append(
                f"{label}: NetworkPolicy {policy['metadata']['name']!r} does not deny "
                "both directions"
            )
    for policy in policies:
        if policy["spec"].get("podSelector", {}).get("matchLabels") not in workloads:
            failures.append(
                f"{label}: NetworkPolicy {policy['metadata']['name']!r} does not "
                f"select any of this overlay's Pods {list(workloads)!r}"
            )
        for rule in policy["spec"].get("ingress", []):
            if not rule.get("from"):
                failures.append(
                    f"{label}: NetworkPolicy {policy['metadata']['name']!r} admits "
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
                        f"{label}: NetworkPolicy {policy['metadata']['name']!r} "
                        f"allows egress to {sorted(missing)} through an open-internet rule; the "
                        "metadata service and the private ranges stay excepted"
                    )
    return failures


def pod_labels(documents: list[Document]) -> tuple[Document, ...]:
    """Every Pod label set the overlay schedules, gateway and Job alike."""
    seen: list[Document] = []
    for document in documents:
        if document.get("kind") not in (*WORKLOAD_KINDS, "Job"):
            continue
        labels = document["spec"]["template"]["metadata"].get("labels", {})
        selector = {"app.kubernetes.io/name": labels.get("app.kubernetes.io/name")}
        if selector not in seen:
            seen.append(selector)
    return tuple(seen)


def check_telemetry_egress(
    documents: list[Document], telemetry_source: str, label: str
) -> list[str]:
    """The collector egress port is the one axond's exporter can actually dial.

    Axond exports OTLP over HTTP only, so an allowance for the gRPC receiver is a
    policy that permits a flow that never happens while denying the one that does
    — telemetry then stops with no error the cluster reports. A deleted or
    renamed allowance fails the same way, so the rule is required rather than
    merely constrained when it happens to be present.
    """
    if 'Some("http/protobuf")' not in telemetry_source:
        return [
            "crates/gateway/src/telemetry/mod.rs no longer names the OTLP protocol this policy "
            "was written for; re-derive the collector egress port"
        ]
    failures: list[str] = []
    allowed: set[int] = set()
    for policy in of_kind(documents, "NetworkPolicy"):
        for rule in policy["spec"].get("egress", []):
            selectors = [
                peer.get("podSelector", {}).get("matchLabels", {}).get("app.kubernetes.io/name")
                for peer in rule.get("to", [])
            ]
            if "opentelemetry-collector" not in selectors:
                continue
            allowed |= {port.get("port") for port in rule.get("ports", [])}
    if not allowed:
        failures.append(
            f"{label}: no egress rule reaches a Pod labelled "
            "app.kubernetes.io/name: opentelemetry-collector, so the default-deny policy drops "
            "every OTLP export while the cluster reports nothing"
        )
    elif allowed != {4318}:
        failures.append(
            f"{label}: telemetry egress allows {sorted(allowed)}, but axond exports "
            "OTLP/HTTP only, whose receiver is 4318; a gRPC allowance drops every export"
        )
    return failures


def check_service_port(documents: list[Document], label: str) -> list[str]:
    """The Service, the container port, and `[server] bind` name one port."""
    config = gateway_config(documents)
    bind_port = int(config["server"]["bind"].rsplit(":", 1)[1])
    failures: list[str] = []
    for container in containers(one_workload(documents)):
        ports = {port["containerPort"] for port in container.get("ports", [])}
        if bind_port not in ports:
            failures.append(
                f"{label}: container {container['name']!r} exposes {sorted(ports)} but the "
                f"mounted config binds {bind_port}"
            )
    # Every Service, not one: the stateful overlay publishes the administrative
    # surface on a second Service, and it reaches the same listener. Emptiness is
    # its own failure, because `of_kind` reports nothing where `one` raised: a
    # manifest set that publishes no Service at all reaches no caller.
    services = of_kind(documents, "Service")
    if not services:
        failures.append(f"{label}: no Service publishes the port the gateway binds")
    for service in services:
        for port in service.get("spec", {}).get("ports", []):
            if port.get("port") != bind_port:
                failures.append(
                    f"{label}: Service {service['metadata']['name']!r} publishes "
                    f"{port.get('port')} but the mounted config binds {bind_port}"
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


def check_disruption_budget(
    documents: list[Document], label: str, autoscaled: list[Document] | None = None
) -> list[str]:
    """A drain can always proceed: budget, replica count, and HPA floor agree.

    `minAvailable` equal to the replica count is a deadlocked node drain rather
    than a strict guarantee, and an autoscaler allowed to scale below
    `minAvailable + 1` reintroduces the same deadlock at its floor.
    """
    deployment = one_workload(documents)
    budget = one(documents, "PodDisruptionBudget")
    minimum = budget["spec"]["minAvailable"]
    replicas = deployment["spec"].get("replicas")
    failures: list[str] = []
    if replicas is None:
        failures.append(f"{label}: the {deployment['kind']} does not declare replicas")
    elif replicas < minimum + 1:
        failures.append(
            f"{label}: {replicas} replicas against minAvailable {minimum} leaves no "
            "room for a voluntary disruption"
        )
    # A Recreate fleet has no rolling update to bound; `check_stateful` asserts
    # that it upgrades that way and keeps no rollingUpdate settings beside it.
    if (
        deployment["kind"] == "Deployment"
        and deployment["spec"].get("strategy", {}).get("type") != "Recreate"
    ):
        strategy = deployment["spec"].get("strategy", {}).get("rollingUpdate", {})
        if strategy.get("maxUnavailable") != 0:
            failures.append(
                f"{label}: the rolling update may take a replica below the fleet size "
                "the disruption budget assumes"
            )
    # The autoscaling component is not part of every overlay: an overlay that
    # does not ship it has no floor to reconcile against the budget.
    if autoscaled is None:
        return failures
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


def check_example_secret(
    production: list[Document], base: list[Document], label: str
) -> list[str]:
    """The base's published placeholder Secret does not survive into production.

    The base ships one so an evaluation renders something bootable, and its values
    are readable by anyone with the repository. Inheriting it into the overlay that
    carries the word production turns the documented `kubectl apply -k` into a
    gateway whose inbound credential is public; a missing `axond-secrets` is the
    safer failure, because the Pod never serves.
    """
    published = {
        value
        for secret in of_kind(base, "Secret")
        for value in secret.get("stringData", {}).values()
    }
    failures: list[str] = []
    for secret in of_kind(production, "Secret"):
        name = secret["metadata"]["name"]
        leaked = sorted(set(secret.get("stringData", {}).values()) & published)
        if leaked:
            failures.append(
                f"{label}: Secret {name!r} still carries the base's published "
                f"placeholders {leaked}; delete the resource in the overlay so an operator has "
                "to supply the credential rather than serving with one from this repository"
            )
    return failures


def ci_service_images(workflow: dict[str, Any]) -> dict[str, str]:
    """Backend images required CI actually runs, keyed by service name.

    Request-path qualification boots SQLite and does not attach Redis or
    Postgres. Overlay drills are opt-in and are not this map.
    """
    images: dict[str, str] = {}
    for job in workflow.get("jobs", {}).values():
        for name, service in (job.get("services") or {}).items():
            image = service.get("image")
            if image:
                images[name] = image
    return images


REQUIRED_CI_IMAGE = re.compile(r"^`([^`]+)`$")


def documented_backends(page: str) -> dict[str, tuple[str, str]]:
    """The supported-version table, as `{backend: (supported column, exercised)}`."""
    rows: dict[str, tuple[str, str]] = {}
    for line in page.splitlines():
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) < 3 or cells[0] not in {"PostgreSQL", "Redis"}:
            continue
        rows[cells[0]] = (cells[1], cells[2])
    return rows


def claimed_required_ci_image(exercised: str) -> str | None:
    """A backtick-only cell is a required-CI image claim; anything else is not."""
    match = REQUIRED_CI_IMAGE.fullmatch(exercised.strip())
    return match.group(1) if match else None


def enforced_postgres_floor(source: str) -> int:
    """The major version the gateway refuses to run below, from its own constant."""
    match = re.search(r"MINIMUM_SERVER_VERSION_NUM: i32 = ([0-9_]+);", source)
    if match is None:
        raise SystemExit(
            "crates/gateway/src/backends/control_plane/schema.rs: MINIMUM_SERVER_VERSION_NUM is gone; "
            "the supported-version gate reads it"
        )
    return int(match.group(1).replace("_", "")) // 10_000


def check_supported_backends(
    page: str, images: dict[str, str], floor: int, revocation_source: str
) -> list[str]:
    """The documented support window is the one that is enforced and exercised.

    Three ways this table rots, each of which turns a support statement into a
    guess: the floor drifts from the version the gateway refuses below, the
    "exercised in CI" column keeps naming an image the workflow no longer runs, and
    the reason for the Redis floor disappears from the code that needed it.
    """
    failures: list[str] = []
    rows = documented_backends(page)
    for backend, service in (("PostgreSQL", "postgres"), ("Redis", "redis")):
        if backend not in rows:
            failures.append(
                f"docs/deployment/stateful-backends.md: no supported-version row for {backend}"
            )
            continue
        supported, exercised = rows[backend]
        running = images.get(service)
        documented_image = claimed_required_ci_image(exercised)
        if documented_image is None:
            marker = exercised.lower()
            if "not exercised" not in marker and "opt-in" not in marker:
                failures.append(
                    f"docs/deployment/stateful-backends.md: {backend} exercised-in-CI cell "
                    f"{exercised!r} is neither a required-CI image nor an explicit "
                    "opt-in / not-exercised marker"
                )
            elif "not exercised" in marker and running is not None:
                failures.append(
                    f"docs/deployment/stateful-backends.md: {backend} is documented as not "
                    f"exercised, but required CI runs `{running}`"
                )
        elif running is None:
            failures.append(
                f"docs/deployment/stateful-backends.md: {backend} is documented as exercised on "
                f"`{documented_image}`, but required CI runs no `{service}` service"
            )
        elif documented_image != running:
            failures.append(
                f"docs/deployment/stateful-backends.md: {backend} is documented as exercised on "
                f"`{documented_image}`, but CI runs `{running}`"
            )
        documented_floor = supported.split(",")[0].strip()
        if backend == "PostgreSQL" and documented_floor != str(floor):
            failures.append(
                f"docs/deployment/stateful-backends.md: PostgreSQL is documented from "
                f"{documented_floor}, but the gateway refuses below {floor} "
                "(MINIMUM_SERVER_VERSION_NUM)"
            )
        if backend == "Redis":
            # The floor is a consequence of the revocation write, not a free
            # choice: `SET … PXAT` is 6.2. Asserting the number itself — rather
            # than only checking the code while the number happens to read 6.2 —
            # keeps an edit to the table from switching the check off.
            if "PXAT" not in revocation_source:
                failures.append(
                    "docs/deployment/stateful-backends.md: the Redis floor is justified by the "
                    "`SET … PXAT` revocation write, which is no longer in "
                    "crates/gateway/src/revocation/redis.rs; re-derive the floor from whatever "
                    "replaced it"
                )
            elif documented_floor != REDIS_PXAT_FLOOR:
                failures.append(
                    f"docs/deployment/stateful-backends.md: Redis is documented from "
                    f"{documented_floor}, but the revocation write is `SET … PXAT`, which needs "
                    f"{REDIS_PXAT_FLOOR}; a different floor has to follow a change in "
                    "crates/gateway/src/revocation/redis.rs"
                )
    return failures


def unblocked_lane(jobs: dict[str, Any], lane: str) -> str | None:
    """Why a drill would not block its selected CI event (ADR 0004).

    The aggregate delegates to ci-success.py. Check both its workflow wiring
    and that a failed drill actually fails that event's gate, including the
    explicit manual opt-in for legacy drills.
    """
    success = jobs["CI-Success"]
    if lane not in success["needs"]:
        return f"CI-Success does not require {lane}"
    required_env = {
        "CI_NEEDS": "${{ toJSON(needs) }}",
        "CI_EVENT": "${{ github.event_name }}",
        "CI_RUST": "${{ needs.changes.outputs.rust }}",
        "CI_DEPENDENCIES": "${{ needs.changes.outputs.dependencies }}",
        "CI_LEGACY": "${{ inputs.run_legacy_postgres_qualification }}",
    }
    if not any(
        str(step.get("run", "")).strip() == "python3 ops/ci-success.py"
        and all(step.get("env", {}).get(key) == value for key, value in required_env.items())
        for step in success["steps"]
    ):
        return f"CI-Success does not pass the job results and event to the gate for {lane}"
    gate = runpy.run_path(str(ROOT / "ops/ci-success.py"))
    event = "workflow_dispatch" if lane in gate["LEGACY"] else "push"
    expected = gate["expected_results"](event, "true", "true", "true")
    if expected.get(lane) != "success":
        return f"CI-Success does not require a successful {lane} on {event}"
    needs = {job: {"result": result} for job, result in expected.items()}
    if gate["failures"](needs, event, "true", "true", "true"):
        return f"CI-Success cannot pass the selected {event} lanes"
    needs[lane]["result"] = "failure"
    if not gate["failures"](needs, event, "true", "true", "true"):
        return f"CI-Success accepts a failed {lane} on {event}"
    return None


def check_recovery_objectives(page: str) -> list[str]:
    """Operator recovery objectives stay documented after the qualification drill retired."""
    failures: list[str] = []
    for wanted in ("RPO", "RTO"):
        if wanted not in page:
            failures.append(
                f"docs/operations/backup-and-recovery.md: {wanted} is not documented"
            )
    return failures


def check_rollout_drill(workflow: dict[str, Any], page: str, drill: str) -> list[str]:
    """The spread constraints have a scheduling proof, and CI runs it.

    Whether a rolling update completes is decided by kube-scheduler, not by the
    manifest, so `check_topology_spread` can only assert the shape that makes it
    possible. The drill is the scheduling half: a real three-node cluster, a real
    rolling update, and — the assertion that keeps the first result honest — the
    same rollout deadlocking once the skew stops being counted per ReplicaSet. A
    drill reduced to its happy path would pass on a manifest whose surge Pod can
    never be placed.
    """
    failures: list[str] = []
    jobs = workflow["jobs"]
    lane = jobs.get("rollout-drill")
    if lane is None:
        failures.append(".github/workflows/ci.yml: the rollout-drill lane is missing")
    elif not any("ops/rollout-drill.sh" in str(step.get("run", "")) for step in lane["steps"]):
        failures.append(".github/workflows/ci.yml: the rollout-drill lane does not run the drill")
    elif (reason := unblocked_lane(jobs, "rollout-drill")) is not None:
        failures.append(
            f".github/workflows/ci.yml: {reason}, so a rollout that cannot schedule would not "
            "block a merge"
        )
    if "ops/rollout-drill.sh" not in page:
        failures.append("docs/deployment/kubernetes.md: ops/rollout-drill.sh is not documented")
    if "has to deadlock" not in drill:
        failures.append(
            "ops/rollout-drill.sh: the counterfactual is gone; without a rollout that must hang, "
            "the drill no longer shows the scoped skew is what makes the shipped one converge"
        )
    return failures


def check_component_layering(kustomization: str) -> list[str]:
    """The autoscaling component is layered above the overlay, never inside it.

    Kustomize accumulates a Kustomization's components before applying that same
    Kustomization's patches. A component listed here would have its
    `remove /spec/replicas` undone by the overlay's own `deployment.yaml` patch,
    so the rendered Deployment would keep `replicas` and every apply would fight
    the HPA for the field — the exact failure the component exists to prevent,
    and one that renders without error. The gate renders the component the
    supported way, so nothing else would catch the inlined arrangement.
    """
    inlined = [
        component
        for component in yaml.safe_load(kustomization).get("components") or []
        if "components/" in str(component)
    ]
    if inlined:
        return [
            "overlays/production: the kustomization declares components "
            f"({', '.join(inlined)}); a component's patches run before this overlay's own, so "
            "spec.replicas is removed and then re-added. Layer components in an overlay above "
            "this one instead"
        ]
    return []


def check_stateful(documents: list[Document]) -> list[str]:
    """The stateful overlay deploys the lifecycle the runtime actually has.

    A stateful replica boots, serves `/admin/v1`, and refuses inference until
    revision convergence ships, so it never reports Ready. Three defaults of a
    serving fleet are wrong for one that does not, and each is a silent wrongness
    — an upgrade that hangs, an administrative surface with no endpoints, a node
    drain that never finishes — so each is asserted here rather than left to the
    comment that explains it. The fourth assertion is the schema ordering: the
    migration is a Job, and a booting replica is not allowed to apply it.
    """
    label = "overlays/production-stateful"
    failures: list[str] = []
    deployment = one(documents, "Deployment")

    strategy = deployment["spec"].get("strategy", {})
    if strategy.get("type") != "Recreate":
        failures.append(
            f"{label}: the Deployment upgrades with {strategy.get('type')!r}; a rolling update "
            "waits for an availability a fleet refusing inference never reports, so the upgrade "
            "stalls instead of landing"
        )
    if strategy.get("rollingUpdate") is not None:
        failures.append(f"{label}: the Deployment keeps rollingUpdate settings beside Recreate")

    template = deployment["spec"].get("template")
    if not isinstance(template, dict) or not isinstance(template.get("spec"), dict):
        failures.append(
            f"{label}: the Deployment is missing spec.template.spec, so Kubernetes cannot create a Pod"
        )
        return failures
    # This is a Deployment with Recreate semantics, not a StatefulSet. There is
    # no durable per-replica volume, so the shipped config must not enable the
    # local last-known-good cache and promise recovery across Pod replacement.
    # A future StatefulSet/PVC overlay gets its own manifest gate when it opts
    # into `[convergence]`.

    budget = one(documents, "PodDisruptionBudget")
    if budget["spec"].get("unhealthyPodEvictionPolicy") != "AlwaysAllow":
        failures.append(
            f"{label}: the disruption budget does not set unhealthyPodEvictionPolicy: "
            "AlwaysAllow, so with no Ready Pod it allows no eviction at all and every node "
            "drain blocks"
        )

    services = {service["metadata"]["name"]: service for service in of_kind(documents, "Service")}
    # The default stateful component must not publish `/admin/v1` through a
    # Service. NetworkPolicy is not a security boundary on clusters whose CNI
    # ignores it, so even a ClusterIP would make the break-glass surface
    # reachable by every Pod. Operators use direct Pod port-forwarding; an
    # administrative Service is an explicit opt-in outside this component.
    if "axond-admin" in services:
        failures.append(
            f"{label}: the default stateful overlay publishes axond-admin; a Service cannot "
            "secure /admin/v1 when the cluster CNI ignores NetworkPolicy — use Pod "
            "port-forwarding or an independently enforced operator boundary"
        )
    # The inference ingress path is the boundary that matters here. `/admin/v1`
    # shares one listener with inference, and on this fleet inference is refused
    # — so the overlay's allowance for the ingress controller's namespace, which
    # exists to admit inference callers, would admit the public path into the
    # administrative surface and nothing else. The component replaces it with an
    # opt-in Pod selector, and that replacement is asserted rather than trusted:
    # a namespace-wide allowance is how it silently comes back.
    for policy in of_kind(documents, "NetworkPolicy"):
        if policy["spec"].get("podSelector", {}).get("matchLabels") != SELECTOR:
            continue
        for rule in policy["spec"].get("ingress", []):
            for peer in rule.get("from", []):
                if peer.get("podSelector") is None:
                    failures.append(
                        f"{label}: NetworkPolicy {policy['metadata']['name']!r} admits "
                        f"{sorted(peer)} to the gateway's port; on a fleet that refuses "
                        "inference the only surface behind that port is /admin/v1, so ingress "
                        "is named Pod by Pod (axond.dev/admin-client) rather than by namespace"
                    )

    for ingress in of_kind(documents, "Ingress"):
        backends = [
            path.get("backend", {}).get("service", {}).get("name")
            for rule in ingress["spec"].get("rules", [])
            for path in rule.get("http", {}).get("paths", [])
        ]
        if "axond-admin" in backends:
            failures.append(
                f"{label}: Ingress {ingress['metadata']['name']!r} routes to axond-admin; this "
                "overlay does not carry the operator authentication that would make publishing "
                "/admin/v1 safe, so the ingress belongs beside it, not here"
            )
    if services.get("axond", {}).get("spec", {}).get("publishNotReadyAddresses"):
        failures.append(
            f"{label}: the inference Service publishes not-ready addresses, so an ingress would "
            "route callers to a replica that refuses them"
        )

    jobs = of_kind(documents, "Job")
    if len(jobs) != 1 or jobs[0]["metadata"]["name"] != "axond-migrate":
        failures.append(f"{label}: the forward migration is not deployed as one axond-migrate Job")
    else:
        job = jobs[0]
        pod = job["spec"]["template"]["spec"]
        job_containers = pod.get("containers", [])
        if ["migrate", "apply"] != job_containers[0].get("args", [])[:2]:
            failures.append(f"{label}: the axond-migrate Job does not run `axond migrate apply`")
        if pod.get("restartPolicy") != "Never":
            failures.append(f"{label}: the axond-migrate Job restarts its Pod in place")
        job_labels = job["spec"]["template"]["metadata"].get("labels", {})
        if job_labels.get("app.kubernetes.io/name") == "axond":
            failures.append(
                f"{label}: the migration Pod carries the serving label, so it becomes a Service "
                "endpoint and is selected by the gateway's NetworkPolicies"
            )
        selected = {
            policy["spec"]["podSelector"].get("matchLabels", {}).get("app.kubernetes.io/name")
            for policy in of_kind(documents, "NetworkPolicy")
        }
        if job_labels.get("app.kubernetes.io/name") not in selected:
            failures.append(
                f"{label}: no NetworkPolicy selects the migration Pod, so the Pod holding the "
                "control-plane DSN has unrestricted egress under a default-deny overlay"
            )

    for document in (deployment, *jobs):
        for container in document["spec"]["template"]["spec"].get("containers", []):
            image = container["image"]
            if f"{IMAGE_REPOSITORY}@{SENTINEL_DIGEST}" != image:
                failures.append(
                    f"{label}: container {container['name']!r} runs {image!r}; this overlay's "
                    "own images: block has to pin every container — including the Job the "
                    "production overlay's transformer never sees — to the sentinel digest"
                )

    config = gateway_config(documents)
    if config.get("mode") != "stateful":
        failures.append(f"{label}: the mounted config is not `mode = \"stateful\"`")
    if "convergence" in config:
        failures.append(
            f"{label}: the Recreate Deployment ships [convergence] without durable per-replica "
            "storage; use a StatefulSet/PVC overlay before enabling the cache"
        )
    owned_by_the_control_plane = sorted(
        key
        for key in ("namespace", "provider", "credential", "model", "gateway_key", "alias")
        if key in config
    )
    if owned_by_the_control_plane:
        failures.append(
            f"{label}: the bootstrap declares {owned_by_the_control_plane}, which the control "
            "plane owns in this mode — a boot error before the listener binds"
        )
    if config.get("control_plane", {}).get("migrate"):
        failures.append(
            f"{label}: booting replicas may apply migrations, so a restart can have one replica "
            "migrating a database its peers are reading; the Job is the migration"
        )
    return failures


def check_stateful_persistent(documents: list[Document]) -> list[str]:
    """The opt-in StatefulSet keeps one authenticated cache per replica."""
    label = "overlays/production-stateful-persistent"
    failures: list[str] = []
    deployments = of_kind(documents, "Deployment")
    if deployments:
        failures.append(
            f"{label}: the persistent option still renders a Deployment; the parent Recreate "
            "workload must be replaced by a StatefulSet"
        )
    statefulsets = of_kind(documents, "StatefulSet")
    if len(statefulsets) != 1:
        failures.append(
            f"{label}: expected exactly one axond StatefulSet, found {len(statefulsets)}"
        )
        return failures

    statefulset = statefulsets[0]
    spec = statefulset["spec"]
    if statefulset["metadata"]["name"] != "axond":
        failures.append(f"{label}: the StatefulSet is not named axond")
    if spec.get("serviceName") != "axond-headless":
        failures.append(
            f"{label}: serviceName is {spec.get('serviceName')!r}, not axond-headless"
        )
    if spec.get("replicas") != 3:
        failures.append(f"{label}: the StatefulSet must declare three replicas")
    if spec.get("podManagementPolicy") != "Parallel":
        failures.append(
            f"{label}: podManagementPolicy is {spec.get('podManagementPolicy')!r}; "
            "unready replicas must not block creation of later PVC-backed ordinals"
        )
    if spec.get("updateStrategy", {}).get("type") != "OnDelete":
        failures.append(
            f"{label}: updateStrategy is {spec.get('updateStrategy', {}).get('type')!r}; "
            "the current intentionally unready stateful process requires explicit restarts"
        )
    if spec.get("selector", {}).get("matchLabels") != SELECTOR:
        failures.append(f"{label}: the StatefulSet selector is not {SELECTOR!r}")

    retention = spec.get("persistentVolumeClaimRetentionPolicy", {})
    for field in ("whenDeleted", "whenScaled"):
        if retention.get(field) != "Retain":
            failures.append(
                f"{label}: persistentVolumeClaimRetentionPolicy.{field} must be Retain; "
                "a replica's signed cache must survive workload deletion and scale changes"
            )

    budget = one(documents, "PodDisruptionBudget")
    if budget["spec"].get("unhealthyPodEvictionPolicy") != "AlwaysAllow":
        failures.append(
            f"{label}: the disruption budget must set unhealthyPodEvictionPolicy: AlwaysAllow "
            "while the current stateful replicas remain unready"
        )

    claims = spec.get("volumeClaimTemplates", [])
    if (
        len(claims) != 1
        or claims[0].get("metadata", {}).get("name") != "last-known-good"
    ):
        failures.append(
            f"{label}: the StatefulSet must declare exactly one last-known-good PVC template"
        )
    else:
        claim = claims[0]
        claim_spec = claim.get("spec", {})
        if claim_spec.get("accessModes") != ["ReadWriteOnce"]:
            failures.append(
                f"{label}: the last-known-good PVC must use ReadWriteOnce per-replica storage"
            )
        if claim_spec.get("resources", {}).get("requests", {}).get("storage") != "1Gi":
            failures.append(
                f"{label}: the last-known-good PVC must request 1Gi of durable storage"
            )

    pod = spec["template"]["spec"]
    if pod.get("securityContext", {}).get("fsGroup") != DISTROLESS_NONROOT_GROUP:
        failures.append(
            f"{label}: pod securityContext.fsGroup must be the distroless nonroot group "
            f"{DISTROLESS_NONROOT_GROUP} so the writable PVC is usable by the image"
        )
    axond = next(
        (
            container
            for container in pod.get("containers", [])
            if container.get("name") == "axond"
        ),
        None,
    )
    cache_mount = next(
        (
            mount
            for mount in (axond or {}).get("volumeMounts", [])
            if mount.get("name") == "last-known-good"
        ),
        None,
    )
    if cache_mount is None or cache_mount.get("mountPath") != "/var/lib/axond":
        failures.append(
            f"{label}: the StatefulSet does not mount last-known-good at /var/lib/axond"
        )
    elif cache_mount.get("readOnly"):
        failures.append(f"{label}: the last-known-good PVC is mounted read-only")
    if any(volume.get("name") == "last-known-good" for volume in pod.get("volumes", [])):
        failures.append(
            f"{label}: last-known-good is declared as a Pod volume; it must come from the "
            "StatefulSet PVC template rather than emptyDir or hostPath"
        )

    service = next(
        (
            service
            for service in of_kind(documents, "Service")
            if service["metadata"]["name"] == "axond-headless"
        ),
        None,
    )
    if service is None:
        failures.append(f"{label}: the StatefulSet governing headless Service is missing")
    else:
        service_spec = service["spec"]
        if service_spec.get("clusterIP") != "None":
            failures.append(f"{label}: axond-headless is not headless")
        if service_spec.get("publishNotReadyAddresses"):
            failures.append(
                f"{label}: axond-headless publishes not-ready addresses; the current stateful "
                "fleet must not gain a bypass around /readyz"
            )
        if service_spec.get("selector") != SELECTOR:
            failures.append(f"{label}: axond-headless does not select {SELECTOR!r}")

    caller_service = next(
        (
            service
            for service in of_kind(documents, "Service")
            if service["metadata"]["name"] == "axond"
        ),
        None,
    )
    if caller_service is not None and caller_service["spec"].get("publishNotReadyAddresses"):
        failures.append(
            f"{label}: the caller-facing axond Service publishes not-ready addresses; "
            "the current stateful fleet must remain unreachable until /readyz passes"
        )

    config = gateway_config(documents)
    convergence = config.get("convergence", {})
    if convergence.get("cache_path") != "/var/lib/axond/last-known-good.snapshot":
        failures.append(
            f"{label}: [convergence].cache_path does not point into the PVC-backed cache mount"
        )
    if convergence.get("cache_key_env") != "GW_LAST_KNOWN_GOOD_KEY":
        failures.append(
            f"{label}: [convergence].cache_key_env is not GW_LAST_KNOWN_GOOD_KEY"
        )

    for document in (statefulset, *of_kind(documents, "Job")):
        for container in containers(document):
            image = container["image"]
            if image != f"{IMAGE_REPOSITORY}@{SENTINEL_DIGEST}":
                failures.append(
                    f"{label}: container {container['name']!r} runs {image!r}; every image "
                    "must remain pinned to this overlay's unresolved sentinel"
                )
    return failures


def check_stateful_drill(workflow: dict[str, Any], page: str, drill: str) -> list[str]:
    """The stateful overlay's behaviour has a cluster proof, and CI runs it.

    `check_stateful` reads the rendered shape. Whether `/admin/v1` is reachable,
    whether an upgrade lands, and whether a Pod can be evicted are answers only an
    API server gives, and each of the three has a counterfactual in the drill —
    without them a change restoring the stateless defaults would still pass.
    """
    failures: list[str] = []
    jobs = workflow["jobs"]
    lane = jobs.get("stateful-deploy-drill")
    if lane is None:
        failures.append(".github/workflows/ci.yml: the stateful-deploy-drill lane is missing")
    elif not any(
        "ops/stateful-deploy-drill.sh" in str(step.get("run", "")) for step in lane["steps"]
    ):
        failures.append(
            ".github/workflows/ci.yml: the stateful-deploy-drill lane does not run the drill"
        )
    elif (reason := unblocked_lane(jobs, "stateful-deploy-drill")) is not None:
        failures.append(
            f".github/workflows/ci.yml: {reason}, so a stateful deployment that cannot be "
            "upgraded or drained would not block a merge"
        )
    if "ops/stateful-deploy-drill.sh" not in page:
        failures.append(
            "docs/deployment/kubernetes.md: ops/stateful-deploy-drill.sh is not documented"
        )
    for counterfactual, lost in (
        ("RollingUpdate has to stall", "an upgrade that never lands would read as one that did"),
        (
            "the default budget has to refuse it",
            "a budget that blocks every drain would read as one that permits them",
        ),
    ):
        if counterfactual not in drill:
            failures.append(
                f"ops/stateful-deploy-drill.sh: the {counterfactual!r} counterfactual is gone; "
                f"{lost}"
            )
    for contract, lost in (
        (
            "/namespaces/platform/v1/chat/completions 401 unauthorized",
            "the canonical anonymous inference probe would no longer prove auth-first refusal",
        ),
        (
            "503 inference_unavailable",
            "the drill would no longer document the authenticated convergence contract",
        ),
        (
            "an active serving revision exists",
            "the drill would claim serving without an active projected revision",
        ),
    ):
        if contract not in drill:
            failures.append(f"ops/stateful-deploy-drill.sh: {lost}")
    return failures


def check_stateful_persistent_drill(
    workflow: dict[str, Any], page: str, drill: str
) -> list[str]:
    """The opt-in StatefulSet has a runtime PVC-retention proof in CI."""
    failures: list[str] = []
    jobs = workflow["jobs"]
    lane = jobs.get("stateful-persistent-drill")
    if lane is None:
        failures.append(
            ".github/workflows/ci.yml: the stateful-persistent-drill lane is missing"
        )
    elif not any(
        "ops/stateful-persistent-drill.sh" in str(step.get("run", ""))
        for step in lane["steps"]
    ):
        failures.append(
            ".github/workflows/ci.yml: the stateful-persistent-drill lane does not run the drill"
        )
    elif (reason := unblocked_lane(jobs, "stateful-persistent-drill")) is not None:
        failures.append(
            f".github/workflows/ci.yml: {reason}, so PVC loss across Pod replacement "
            "would not block a merge"
        )
    if "ops/stateful-persistent-drill.sh" not in page:
        failures.append(
            "docs/deployment/kubernetes.md: ops/stateful-persistent-drill.sh is not documented"
        )
    for assertion in (
        "three retained PVC-backed ordinals",
        "survives Pod replacement",
        "/namespaces/platform/v1/chat/completions remains authentication-first",
    ):
        if assertion not in drill:
            failures.append(
                f"ops/stateful-persistent-drill.sh: the {assertion!r} assertion is gone; "
                "the persistent overlay would have only a manifest check"
            )
    return failures


def check_documented() -> list[str]:
    """The operator-facing page names the paths and the sentinel workflow."""
    page = KUBERNETES_DOC.read_text(encoding="utf-8")
    failures: list[str] = []
    for path in (
        "deploy/kubernetes/base",
        "deploy/kubernetes/overlays/production",
        "deploy/kubernetes/overlays/production-stateful",
        "deploy/kubernetes/overlays/production-stateful-persistent",
        "deploy/kubernetes/components/autoscaling",
        "deploy/kubernetes/components/stateful",
        "ops/pin-image-digest.sh",
        "ops/stateful-persistent-drill.sh",
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
    failures: list[str] = []
    if completed.returncode == 0:
        failures.append(
            "ops/pin-image-digest.sh --check accepted the committed overlay; it must refuse the "
            "unresolved sentinel digest"
        )
    # A `--check` that passes is only worth what it covers. Every overlay
    # carrying the sentinel has to be in the helper's list, or an operator
    # resolves one overlay, sees the check pass, and applies another that still
    # names an image no node can pull.
    helper = (ROOT / "ops/pin-image-digest.sh").read_text(encoding="utf-8")
    for overlay in sorted((ROOT / "deploy/kubernetes/overlays").glob("*/kustomization.yaml")):
        relative = overlay.relative_to(ROOT).as_posix()
        if SENTINEL_DIGEST in overlay.read_text(encoding="utf-8") and relative not in helper:
            failures.append(
                f"ops/pin-image-digest.sh: {relative} pins the sentinel digest but the helper "
                "neither rewrites nor checks it, so its placeholder survives a passing --check"
            )
    return failures


def check_digest_scope() -> list[str]:
    """`--check` answers for the whole fleet, or for the overlay it is given.

    A repository gate wants the fleet. An operator rolling out one overlay wants
    that overlay: naming it must not fail on a sentinel in an overlay nobody is
    applying, and must still fail on one in the overlay they are. The contract is
    exercised on a copy, because the committed tree has no resolved overlay in it.
    """
    resolved = "sha256:" + "1" * 64
    failures: list[str] = []
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        (root / "ops").mkdir()
        shutil.copy(ROOT / "ops/pin-image-digest.sh", root / "ops/pin-image-digest.sh")
        overlays = ROOT / "deploy/kubernetes/overlays"
        copied = root / "deploy/kubernetes/overlays"
        copied.mkdir(parents=True)
        for overlay in ("production", "production-stateful", "production-stateful-persistent"):
            (copied / overlay).mkdir()
            shutil.copy(
                overlays / overlay / "kustomization.yaml", copied / overlay / "kustomization.yaml"
            )
        pinned = copied / "production/kustomization.yaml"
        pinned.write_text(
            pinned.read_text(encoding="utf-8").replace(SENTINEL_DIGEST, resolved), encoding="utf-8"
        )
        persistent_pinned = copied / "production-stateful-persistent/kustomization.yaml"
        persistent_pinned.write_text(
            persistent_pinned.read_text(encoding="utf-8").replace(SENTINEL_DIGEST, resolved),
            encoding="utf-8",
        )

        def check(*arguments: str) -> int:
            return subprocess.run(  # noqa: S603 - fixed argv, no shell
                ["bash", str(root / "ops/pin-image-digest.sh"), "--check", *arguments],
                capture_output=True,
                text=True,
                check=False,
            ).returncode

        expectations = (
            ((), 0, "the fleet still carries an unresolved overlay"),
            (("overlays/production",), 1, "the named overlay is resolved"),
            (
                ("deploy/kubernetes/overlays/production/kustomization.yaml",),
                1,
                "the named overlay is resolved, named by path",
            ),
            (("overlays/production-stateful",), 0, "the named overlay is unresolved"),
            (("overlays/production-stateful-persistent",), 1, "the named overlay is resolved"),
            (("overlays/nowhere",), 0, "the named overlay does not exist"),
        )
        for arguments, forbidden, because in expectations:
            if check(*arguments) == forbidden:
                verdict = "accepted" if forbidden == 0 else "refused"
                failures.append(
                    f"ops/pin-image-digest.sh --check {' '.join(arguments)}: {verdict} a tree "
                    f"where {because}"
                )
    return failures


def gate(
    base: list[Document],
    production: list[Document],
    autoscaled: list[Document],
    stateful: list[Document],
    stateful_persistent: list[Document],
) -> list[str]:
    return [
        *check_stateful(stateful),
        *check_termination_budget(stateful, "overlays/production-stateful"),
        *check_resources(stateful, "overlays/production-stateful"),
        *check_service_port(stateful, "overlays/production-stateful"),
        *check_topology_spread(stateful, "overlays/production-stateful"),
        *check_namespaces(stateful, "overlays/production-stateful"),
        *check_example_secret(stateful, base, "overlays/production-stateful"),
        # The stateful overlay inherits the production overlay's policies, PDB
        # and telemetry egress, and inheritance is exactly what a render can
        # lose: a component that renames a label or drops a patch leaves this
        # fleet default-open while the stateless one still passes.
        *check_network_policies(
            stateful, "overlays/production-stateful", pod_labels(stateful)
        ),
        *check_telemetry_egress(
            stateful, TELEMETRY_SOURCE.read_text(encoding="utf-8"), "overlays/production-stateful"
        ),
        *check_disruption_budget(stateful, "overlays/production-stateful"),
        *check_stateful_persistent(stateful_persistent),
        *check_termination_budget(
            stateful_persistent, "overlays/production-stateful-persistent"
        ),
        *check_resources(stateful_persistent, "overlays/production-stateful-persistent"),
        *check_service_port(stateful_persistent, "overlays/production-stateful-persistent"),
        *check_topology_spread(
            stateful_persistent, "overlays/production-stateful-persistent"
        ),
        *check_namespaces(stateful_persistent, "overlays/production-stateful-persistent"),
        *check_example_secret(
            stateful_persistent, base, "overlays/production-stateful-persistent"
        ),
        *check_network_policies(
            stateful_persistent,
            "overlays/production-stateful-persistent",
            pod_labels(stateful_persistent),
        ),
        *check_telemetry_egress(
            stateful_persistent,
            TELEMETRY_SOURCE.read_text(encoding="utf-8"),
            "overlays/production-stateful-persistent",
        ),
        *check_disruption_budget(
            stateful_persistent, "overlays/production-stateful-persistent"
        ),
        *check_image_pinning(base, production),
        *check_termination_budget(base, "base"),
        *check_termination_budget(production, "overlays/production"),
        *check_resources(base, "base"),
        *check_resources(production, "overlays/production"),
        *check_service_port(base, "base"),
        *check_service_port(production, "overlays/production"),
        *check_topology_spread(production, "overlays/production"),
        *check_network_policies(production, "overlays/production"),
        *check_telemetry_egress(
            production, TELEMETRY_SOURCE.read_text(encoding="utf-8"), "overlays/production"
        ),
        *check_disruption_budget(production, "overlays/production", autoscaled),
        *check_namespaces(base, "base"),
        *check_namespaces(production, "overlays/production"),
        *check_example_secret(production, base, "overlays/production"),
    ]


def self_test() -> int:
    """Prove each check fails on the manifest mistake it exists to catch."""
    base = render(BASE)
    production = render(PRODUCTION)
    autoscaled = render(PRODUCTION, (AUTOSCALING,))
    stateful = render(PRODUCTION_STATEFUL)
    stateful_persistent = render(PRODUCTION_STATEFUL_PERSISTENT)
    failures: list[str] = []

    def expect_failure(name: str, produced: list[str]) -> None:
        if not produced:
            failures.append(f"self-test: {name} did not fail on a manifest it must reject")

    if gate(base, production, autoscaled, stateful, stateful_persistent):
        failures.append("self-test: the committed manifests must pass the gate")

    persistent_deployment = copy.deepcopy(stateful_persistent)
    persistent_deployment.append(
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "axond", "namespace": "axond"},
            "spec": {"replicas": 3},
        }
    )
    expect_failure(
        "the persistent option retaining the Recreate Deployment",
        check_stateful_persistent(persistent_deployment),
    )

    persistent_empty_dir = copy.deepcopy(stateful_persistent)
    persistent_pod = one(persistent_empty_dir, "StatefulSet")["spec"]["template"]["spec"]
    persistent_pod["volumes"].append({"name": "last-known-good", "emptyDir": {}})
    expect_failure(
        "a persistent option falling back to emptyDir",
        check_stateful_persistent(persistent_empty_dir),
    )

    persistent_deleted = copy.deepcopy(stateful_persistent)
    one(persistent_deleted, "StatefulSet")["spec"]["persistentVolumeClaimRetentionPolicy"][
        "whenDeleted"
    ] = "Delete"
    expect_failure(
        "a persistent option deleting a replica cache with the workload",
        check_stateful_persistent(persistent_deleted),
    )

    persistent_missing_fs_group = copy.deepcopy(stateful_persistent)
    one(persistent_missing_fs_group, "StatefulSet")["spec"]["template"]["spec"][
        "securityContext"
    ].pop("fsGroup", None)
    expect_failure(
        "a persistent option leaving the writable PVC without a distroless nonroot group",
        check_stateful_persistent(persistent_missing_fs_group),
    )

    rolling = copy.deepcopy(stateful)
    one(rolling, "Deployment")["spec"]["strategy"] = {
        "type": "RollingUpdate",
        "rollingUpdate": {"maxUnavailable": 0, "maxSurge": 1},
    }
    expect_failure("a stateful fleet upgraded with RollingUpdate", check_stateful(rolling))

    blocked = copy.deepcopy(stateful)
    one(blocked, "PodDisruptionBudget")["spec"].pop("unhealthyPodEvictionPolicy")
    expect_failure("a budget that blocks every drain", check_stateful(blocked))

    missing_template = copy.deepcopy(stateful)
    one(missing_template, "Deployment")["spec"].pop("template")
    expect_failure("a stateful Deployment without a Pod template", check_stateful(missing_template))

    published_admin = copy.deepcopy(stateful)
    published_admin.append(
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "axond-admin", "namespace": "axond"},
            "spec": {"type": "ClusterIP", "selector": dict(SELECTOR)},
        }
    )
    expect_failure(
        "the default stateful overlay publishes an admin Service",
        check_stateful(published_admin),
    )

    ingressed_admin = copy.deepcopy(stateful)
    ingressed_admin.append(
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "Ingress",
            "metadata": {"name": "axond-admin", "namespace": "axond"},
            "spec": {
                "rules": [
                    {
                        "http": {
                            "paths": [
                                {
                                    "path": "/admin",
                                    "pathType": "Prefix",
                                    "backend": {
                                        "service": {
                                            "name": "axond-admin",
                                            "port": {"number": 8080},
                                        }
                                    },
                                }
                            ]
                        }
                    }
                ]
            },
        }
    )
    expect_failure("an ingress that fronts /admin/v1", check_stateful(ingressed_admin))

    routed = copy.deepcopy(stateful)
    for service in of_kind(routed, "Service"):
        if service["metadata"]["name"] == "axond":
            service["spec"]["publishNotReadyAddresses"] = True
    expect_failure("callers routed to a refusing replica", check_stateful(routed))

    cache_enabled_without_durable_storage = copy.deepcopy(stateful)
    cache_config = one(cache_enabled_without_durable_storage, "ConfigMap")
    cache_config["data"]["axond.toml"] += (
        "\n[convergence]\n"
        "cache_path = \"/var/lib/axond/last-known-good.snapshot\"\n"
        "cache_key_env = \"GW_LAST_KNOWN_GOOD_KEY\"\n"
    )
    expect_failure(
        "a Recreate stateful Deployment that enables a non-durable cache",
        check_stateful(cache_enabled_without_durable_storage),
    )

    self_migrating = copy.deepcopy(stateful)
    config = one(self_migrating, "ConfigMap")
    # The first occurrence only: the table header ends `[control_plane]`, and a
    # comment further down may mention the same name without opening a table.
    config["data"]["axond.toml"] = config["data"]["axond.toml"].replace(
        "[secret_store]", "migrate = true\n\n[secret_store]", 1
    )
    expect_failure("replicas allowed to migrate at boot", check_stateful(self_migrating))

    statelessly_configured = copy.deepcopy(stateful)
    config = one(statelessly_configured, "ConfigMap")
    config["data"]["axond.toml"] += '\n[[namespace]]\nid = "platform"\n'
    expect_failure("a bootstrap that also declares resources", check_stateful(statelessly_configured))

    tagged_job = copy.deepcopy(stateful)
    containers(one(tagged_job, "Job"))[0]["image"] = f"{IMAGE_REPOSITORY}:0.3.27"
    expect_failure("a migration Job left on a mutable tag", check_stateful(tagged_job))

    serving_label = copy.deepcopy(stateful)
    one(serving_label, "Job")["spec"]["template"]["metadata"]["labels"] = dict(SELECTOR)
    expect_failure("a migration Pod wearing the serving label", check_stateful(serving_label))

    unpoliced = copy.deepcopy(stateful)
    unpoliced[:] = [
        document
        for document in unpoliced
        if document.get("kind") != "NetworkPolicy"
        or not document["metadata"]["name"].startswith("axond-migrate")
    ]
    expect_failure("a migration Pod no NetworkPolicy selects", check_stateful(unpoliced))

    unpublished = copy.deepcopy(stateful)
    unpublished[:] = [document for document in unpublished if document.get("kind") != "Service"]
    expect_failure(
        "a manifest set that publishes no Service",
        check_service_port(unpublished, "overlays/production-stateful"),
    )

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

    # Deleting the section is the same regression as raising it: the gateway then
    # runs on compiled-in ceilings no manifest bounds.
    unbounded_admission = copy.deepcopy(production)
    config = one(unbounded_admission, "ConfigMap")
    config["data"]["axond.toml"] = re.sub(
        r"^max_(in_flight|request_bytes) = .*$",
        "",
        config["data"]["axond.toml"],
        flags=re.MULTILINE,
    )
    expect_failure(
        "a deleted [admission] ceiling",
        check_resources(unbounded_admission, "overlays/production"),
    )

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
    expect_failure("topology spread", check_topology_spread(flat, "overlays/production"))

    stacked = copy.deepcopy(production)
    for constraint in one(stacked, "Deployment")["spec"]["template"]["spec"][
        "topologySpreadConstraints"
    ]:
        constraint["whenUnsatisfiable"] = "ScheduleAnyway"
    expect_failure("per-node spread enforcement", check_topology_spread(stacked, "overlays/production"))

    fleet_wide = copy.deepcopy(production)
    for constraint in one(fleet_wide, "Deployment")["spec"]["template"]["spec"][
        "topologySpreadConstraints"
    ]:
        constraint.pop("matchLabelKeys", None)
    expect_failure("a hard spread that deadlocks its own rollout", check_topology_spread(fleet_wide, "overlays/production"))

    open_egress = copy.deepcopy(production)
    for policy in of_kind(open_egress, "NetworkPolicy"):
        for rule in policy["spec"].get("egress", []):
            for peer in rule.get("to", []):
                if peer.get("ipBlock", {}).get("cidr") == "0.0.0.0/0":
                    peer["ipBlock"].pop("except")
    expect_failure("private-range egress", check_network_policies(open_egress, "overlays/production"))

    open_ingress = copy.deepcopy(production)
    for policy in of_kind(open_ingress, "NetworkPolicy"):
        for rule in policy["spec"].get("ingress", []):
            rule.pop("from", None)
    expect_failure("unrestricted ingress", check_network_policies(open_ingress, "overlays/production"))

    allow_only = copy.deepcopy(production)
    allow_only[:] = [
        document
        for document in allow_only
        if document.get("kind") != "NetworkPolicy" or document["spec"].get("egress")
    ]
    expect_failure("default deny", check_network_policies(allow_only, "overlays/production"))

    tight = copy.deepcopy(production)
    one(tight, "PodDisruptionBudget")["spec"]["minAvailable"] = 3
    expect_failure("disruption budget", check_disruption_budget(tight, "overlays/production", autoscaled))

    contended = copy.deepcopy(autoscaled)
    one(contended, "Deployment")["spec"]["replicas"] = 3
    expect_failure("autoscaled replica ownership", check_disruption_budget(production, "overlays/production", contended))

    shrunk = copy.deepcopy(autoscaled)
    one(shrunk, "HorizontalPodAutoscaler")["spec"]["minReplicas"] = 1
    expect_failure("autoscaler floor", check_disruption_budget(production, "overlays/production", shrunk))

    # The same three gates, against the stateful render: the overlay inherits
    # them through a component, and a component that stops applying is exactly
    # the regression a gate on the stateless render alone would not see.
    stateful_open = copy.deepcopy(stateful)
    for policy in of_kind(stateful_open, "NetworkPolicy"):
        for rule in policy["spec"].get("egress", []):
            for peer in rule.get("to", []):
                if peer.get("ipBlock", {}).get("cidr") == "0.0.0.0/0":
                    peer["ipBlock"].pop("except")
    expect_failure(
        "private-range egress on the stateful fleet",
        check_network_policies(
            stateful_open, "overlays/production-stateful", pod_labels(stateful_open)
        ),
    )

    unpoliced_job = copy.deepcopy(stateful)
    migration = {"app.kubernetes.io/name": "axond-migrate"}
    unpoliced_job[:] = [
        document
        for document in unpoliced_job
        if document.get("kind") != "NetworkPolicy"
        or document["spec"].get("podSelector", {}).get("matchLabels") != migration
    ]
    expect_failure(
        "a migration Pod no policy denies by default",
        check_network_policies(
            unpoliced_job, "overlays/production-stateful", pod_labels(unpoliced_job)
        ),
    )

    inherited_ingress = copy.deepcopy(stateful)
    for policy in of_kind(inherited_ingress, "NetworkPolicy"):
        if policy["spec"].get("podSelector", {}).get("matchLabels") != SELECTOR:
            continue
        if policy["spec"].get("ingress"):
            policy["spec"]["ingress"] = [
                {
                    "from": [
                        {
                            "namespaceSelector": {
                                "matchLabels": {"kubernetes.io/metadata.name": "ingress-nginx"}
                            }
                        }
                    ],
                    "ports": [{"protocol": "TCP", "port": 8080}],
                }
            ]
    expect_failure(
        "the inference ingress path inherited onto the admin surface",
        check_stateful(inherited_ingress),
    )

    stateful_grpc = copy.deepcopy(stateful)
    for policy in of_kind(stateful_grpc, "NetworkPolicy"):
        for rule in policy["spec"].get("egress", []):
            for peer in rule.get("to", []):
                labels = peer.get("podSelector", {}).get("matchLabels", {})
                if labels.get("app.kubernetes.io/name") == "opentelemetry-collector":
                    rule["ports"] = [{"protocol": "TCP", "port": 4317}]
    expect_failure(
        "stateful telemetry egress on a port axond cannot dial",
        check_telemetry_egress(
            stateful_grpc,
            TELEMETRY_SOURCE.read_text(encoding="utf-8"),
            "overlays/production-stateful",
        ),
    )

    stateful_tight = copy.deepcopy(stateful)
    one(stateful_tight, "PodDisruptionBudget")["spec"]["minAvailable"] = 3
    expect_failure(
        "a stateful budget that leaves no room for a drain",
        check_disruption_budget(stateful_tight, "overlays/production-stateful"),
    )

    inherited = copy.deepcopy(production)
    inherited.extend(copy.deepcopy(of_kind(base, "Secret")))
    expect_failure(
        "the base's example Secret inherited",
        check_example_secret(inherited, base, "overlays/production"),
    )

    stray = copy.deepcopy(production)
    one(stray, "Service")["metadata"].pop("namespace")
    expect_failure("namespace placement", check_namespaces(stray, "overlays/production"))

    grpc = copy.deepcopy(production)
    for policy in of_kind(grpc, "NetworkPolicy"):
        for rule in policy["spec"].get("egress", []):
            for peer in rule.get("to", []):
                labels = peer.get("podSelector", {}).get("matchLabels", {})
                if labels.get("app.kubernetes.io/name") == "opentelemetry-collector":
                    rule["ports"] = [{"protocol": "TCP", "port": 4317}]
    telemetry = TELEMETRY_SOURCE.read_text(encoding="utf-8")
    expect_failure(
        "telemetry egress on a port axond cannot dial",
        check_telemetry_egress(grpc, telemetry, "overlays/production"),
    )
    unreachable = copy.deepcopy(production)
    for policy in of_kind(unreachable, "NetworkPolicy"):
        egress = policy["spec"].get("egress")
        if not egress:
            continue
        policy["spec"]["egress"] = [
            rule
            for rule in egress
            if all(
                peer.get("podSelector", {}).get("matchLabels", {}).get("app.kubernetes.io/name")
                != "opentelemetry-collector"
                for peer in rule.get("to", [])
            )
        ]
    expect_failure(
        "telemetry egress deleted altogether",
        check_telemetry_egress(unreachable, telemetry, "overlays/production"),
    )
    expect_failure(
        "telemetry egress derived from a protocol the gateway no longer names",
        check_telemetry_egress(
            production,
            telemetry.replace('Some("http/protobuf")', 'Some("grpc")'),
            "overlays/production",
        ),
    )

    workflow = yaml.safe_load(CI_WORKFLOW.read_text(encoding="utf-8"))
    backends = STATEFUL_DOC.read_text(encoding="utf-8")
    images = ci_service_images(workflow)
    floor = enforced_postgres_floor(SCHEMA_SOURCE.read_text(encoding="utf-8"))
    revocation = REVOCATION_SOURCE.read_text(encoding="utf-8")
    recovery = RECOVERY_DOC.read_text(encoding="utf-8")

    if check_supported_backends(backends, images, floor, revocation):
        failures.append("self-test: the committed support window must pass the gate")
    required_redis = backends.replace(
        "| Redis | 6.2, 7.x, 8.x | not exercised (ADR 0063) |",
        "| Redis | 6.2, 7.x, 8.x | `redis:7.4.2-alpine` |",
    )
    expect_failure(
        "required-CI image with no CI service",
        check_supported_backends(required_redis, images, floor, revocation),
    )
    expect_failure(
        "backend image drift",
        check_supported_backends(
            required_redis, {**images, "redis": "redis:6.0-alpine"}, floor, revocation
        ),
    )
    expect_failure(
        "not-exercised backend that CI still runs",
        check_supported_backends(
            backends, {**images, "redis": "redis:7.4.2-alpine"}, floor, revocation
        ),
    )
    expect_failure(
        "documented floor below what the gateway accepts",
        check_supported_backends(backends, images, floor + 1, revocation),
    )
    expect_failure(
        "Redis floor without its reason",
        check_supported_backends(backends, images, floor, revocation.replace("PXAT", "EX")),
    )
    expect_failure(
        "a Redis floor below the one PXAT needs",
        check_supported_backends(
            backends.replace("| Redis | 6.2", "| Redis | 6.0"), images, floor, revocation
        ),
    )

    production_kustomization = (PRODUCTION / "kustomization.yaml").read_text(encoding="utf-8")
    if check_component_layering(production_kustomization):
        failures.append("self-test: the committed overlay must not be read as inlining a component")
    expect_failure(
        "an autoscaling component inlined into the overlay it patches",
        check_component_layering(
            production_kustomization + "components:\n  - ../../components/autoscaling\n"
        ),
    )

    if check_recovery_objectives(recovery):
        failures.append("self-test: the committed recovery objectives must pass the gate")
    expect_failure(
        "recovery page without RPO",
        check_recovery_objectives(recovery.replace("RPO", "recovery-point")),
    )

    kubernetes_page = KUBERNETES_DOC.read_text(encoding="utf-8")
    rollout = ROLLOUT_DRILL.read_text(encoding="utf-8")
    if check_rollout_drill(workflow, kubernetes_page, rollout):
        failures.append("self-test: the committed rollout drill wiring must pass the gate")
    optional_rollout = copy.deepcopy(workflow)
    optional_rollout["jobs"]["CI-Success"]["needs"].remove("rollout-drill")
    expect_failure(
        "optional rollout lane", check_rollout_drill(optional_rollout, kubernetes_page, rollout)
    )
    unasserted_rollout = copy.deepcopy(workflow)
    for step in unasserted_rollout["jobs"]["CI-Success"]["steps"]:
        if "run" in step:
            step["run"] = "true"
    expect_failure(
        "a needed rollout lane CI-Success never asserts",
        check_rollout_drill(unasserted_rollout, kubernetes_page, rollout),
    )
    expect_failure(
        "rollout drill without its counterfactual",
        check_rollout_drill(workflow, kubernetes_page, rollout.replace("has to deadlock", "runs")),
    )

    stateful_drill = STATEFUL_DRILL.read_text(encoding="utf-8")
    if check_stateful_drill(workflow, kubernetes_page, stateful_drill):
        failures.append("self-test: the committed stateful drill wiring must pass the gate")
    optional_stateful = copy.deepcopy(workflow)
    optional_stateful["jobs"]["CI-Success"]["needs"].remove("stateful-deploy-drill")
    expect_failure(
        "optional stateful deploy lane",
        check_stateful_drill(optional_stateful, kubernetes_page, stateful_drill),
    )
    unasserted_stateful = copy.deepcopy(workflow)
    for step in unasserted_stateful["jobs"]["CI-Success"]["steps"]:
        if "run" in step:
            step["run"] = "true"
    expect_failure(
        "a needed stateful lane CI-Success never asserts",
        check_stateful_drill(unasserted_stateful, kubernetes_page, stateful_drill),
    )
    for counterfactual in ("RollingUpdate has to stall", "the default budget has to refuse it"):
        expect_failure(
            f"stateful drill without {counterfactual!r}",
            check_stateful_drill(
                workflow, kubernetes_page, stateful_drill.replace(counterfactual, "runs")
            ),
        )
    canonical_stateful_probe = "/namespaces/platform/v1/chat/completions 401 unauthorized"
    expect_failure(
        "stateful drill using a legacy inference probe",
        check_stateful_drill(
            workflow,
            kubernetes_page,
            stateful_drill.replace(
                canonical_stateful_probe, "/v1/chat/completions 401 unauthorized"
            ),
        ),
    )

    persistent_drill = STATEFUL_PERSISTENT_DRILL.read_text(encoding="utf-8")
    if check_stateful_persistent_drill(workflow, kubernetes_page, persistent_drill):
        failures.append(
            "self-test: the committed stateful persistent drill wiring must pass the gate"
        )
    optional_persistent = copy.deepcopy(workflow)
    optional_persistent["jobs"]["CI-Success"]["needs"].remove("stateful-persistent-drill")
    expect_failure(
        "optional stateful persistent lane",
        check_stateful_persistent_drill(
            optional_persistent, kubernetes_page, persistent_drill
        ),
    )
    unasserted_persistent = copy.deepcopy(workflow)
    for step in unasserted_persistent["jobs"]["CI-Success"]["steps"]:
        if "run" in step:
            step["run"] = "true"
    expect_failure(
        "a needed stateful persistent lane CI-Success never asserts",
        check_stateful_persistent_drill(
            unasserted_persistent, kubernetes_page, persistent_drill
        ),
    )
    for assertion in (
        "three retained PVC-backed ordinals",
        "survives Pod replacement",
        "/namespaces/platform/v1/chat/completions remains authentication-first",
    ):
        expect_failure(
            f"persistent drill without {assertion!r}",
            check_stateful_persistent_drill(
                workflow, kubernetes_page, persistent_drill.replace(assertion, "runs")
            ),
        )

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
        *gate(
            render(BASE),
            render(PRODUCTION),
            render(PRODUCTION, (AUTOSCALING,)),
            render(PRODUCTION_STATEFUL),
            render(PRODUCTION_STATEFUL_PERSISTENT),
        ),
        *check_component_layering(
            (PRODUCTION / "kustomization.yaml").read_text(encoding="utf-8")
        ),
        *check_documented(),
        *check_sentinel_refused(),
        *check_digest_scope(),
        *check_supported_backends(
            STATEFUL_DOC.read_text(encoding="utf-8"),
            ci_service_images(yaml.safe_load(CI_WORKFLOW.read_text(encoding="utf-8"))),
            enforced_postgres_floor(SCHEMA_SOURCE.read_text(encoding="utf-8")),
            REVOCATION_SOURCE.read_text(encoding="utf-8"),
        ),
        *check_recovery_objectives(RECOVERY_DOC.read_text(encoding="utf-8")),
        *check_rollout_drill(
            yaml.safe_load(CI_WORKFLOW.read_text(encoding="utf-8")),
            KUBERNETES_DOC.read_text(encoding="utf-8"),
            ROLLOUT_DRILL.read_text(encoding="utf-8"),
        ),
        *check_stateful_drill(
            yaml.safe_load(CI_WORKFLOW.read_text(encoding="utf-8")),
            KUBERNETES_DOC.read_text(encoding="utf-8"),
            STATEFUL_DRILL.read_text(encoding="utf-8"),
        ),
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
