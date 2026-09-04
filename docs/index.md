# Axond documentation

Use this page as the task-oriented entry point. The architecture decision
records explain *why* Axond works this way; the guides below explain how to use
and operate it.

The live product is [ADR 0063](./adr/0063-stateful-only-namespaced-gateway.md):
a store-backed, namespace-scoped gateway. Historical ADRs that described
stateless mode, `/admin/v1`, minted tokens, or `[[model]]` aliases remain on
disk as records.

## Evaluate Axond

- [Getting started](./getting-started.md) — boot the public image, publish a
  period budget, prove `/ns/{ns}/v1` authentication and routing, make a first
  provider request.
- [Compatibility contract](./compatibility.md) — supported routes, provider
  wires, client behavior, and the `0.x` stability policy.
- [Store](./configuration.md#state-tiers) — required SQLite or Postgres; Redis
  and in-memory budget backends are withdrawn.
- [ADR 0063](./adr/0063-stateful-only-namespaced-gateway.md) — current product
  shape. [ADR 0027](./adr/0027-stateless-and-stateful-operating-modes.md) is
  superseded.

## Install

- [Installation and verification](./installation.md) — crates.io, signed
  binaries, OCI images, checksums, SBOMs, signatures, and attestations.
- [Docker Compose](./deployment/docker-compose.md) — pull-first SQLite
  quickstart and Postgres overlay.

## Connect a client

- [OpenAI clients](./clients/openai.md) — chat completions, Responses,
  embeddings, streaming, Python, and TypeScript. Base URL
  `/ns/{namespace}/v1`; model `provider-id/model-id`.
- [Anthropic clients](./clients/anthropic.md) — native Messages and streaming.
  Base URL `/ns/{namespace}`.
- [Compatibility contract](./compatibility.md) — route and wire-family matrix.

## Configure

- [Configuration reference](./configuration.md) — every key, default, and
  validation rule. `[[model]]` and `mode` are boot errors.
- [`axond.example.toml`](../axond.example.toml) — the complete annotated
  configuration surface.
- Management API — `/api/v1` (OpenAPI at `/api/v1/openapi.json`): namespaces,
  period budgets, usage, cached provider models.

## Deploy

- [Deployment overview](./deployment.md) — choose an environment.
- [Container runtime](./deployment/container.md) — Docker or Podman, image
  verification, mounts, and runtime contract.
- [Linux and systemd](./deployment/systemd.md) — prebuilt binary and hardened
  service unit.
- [Kubernetes](./deployment/kubernetes.md) — ConfigMap, Secret, Deployment,
  Service, probes, security context, and rollout constraints.
- [Azure Container Apps](./deployment/azure-container-apps.md) — production
  replica: GHCR image, Key Vault keys, TOML mount, probes, telemetry, usage.
- [Managed containers](./deployment/managed-containers.md) — the portable
  contract for ECS/Fargate, Cloud Run, Azure Container Apps, and Nomad.
- [Store backends](./deployment/stateful-backends.md) — SQLite vs Postgres.
  Redis/control-plane pages are historical.
- [Production checklist](./deployment/production-checklist.md) — security,
  streaming, rollout, observability, and recovery review.

## Operate

- [Observability and runbook](./observability.md) — traces, metrics, logs,
  usage records, alerts, and boot failures.
- [Observability runbook](./operations/observability-runbook.md) — first
  response per failure mode, bounded drill-downs, and the shipped dashboards and
  alert rules.
- [Troubleshooting](./operations/troubleshooting.md) — symptom and typed-error
  decision tree.
- [Upgrades and rollback](./operations/upgrades.md) — compatibility policy,
  migration ordering, mixed-version rules, and rollback limits.
- [Backup, restore, and PITR](./operations/backup-and-recovery.md) — what is
  durable, the recovery objectives, the archiving and dump mechanisms, and the
  drill that proves a restore lands where it was aimed.
- [Qualification packet](./operations/qualification.md) — request-path
  evidence after ADR 0063: what is harnessed, what is retained, what is gone.
- [Capacity qualification](./operations/capacity.md) — per-replica envelopes
  on SQLite + `/ns/{ns}/v1`.
- [Fault qualification](./operations/fault-qualification.md) — provider and
  transport faults on SQLite; Redis rows skipped (ADR 0063).
- [Endurance qualification](./operations/endurance.md) — mixed-workload soak
  on SQLite + `/ns/{ns}/v1`.
- [Usage schema](./usage-schema.md) — durable row contract and delivery
  guarantees.
- [Billing-grade usage outbox](./operations/usage-outbox.md) — the opt-in durable
  outbox: setup, guarantees, refusals, recovery, poison, upgrades, and alerts.
- [Deployment security model](./security/deployment-model.md) — trust
  boundaries, TLS termination, secret delivery, and image verification.
- [Tenant isolation evidence](./security/tenant-isolation-evidence.md) — which
  layer enforces each part of isolation, the test that proves it, and what is
  not covered yet.
- [Security policy](../SECURITY.md) — private vulnerability reporting, the
  supported-version window, response targets, and how a fix and advisory ship.
- [Fuzzing](./security/fuzzing.md) — the config, token, and query targets, the
  properties they assert, and the required-versus-scheduled lanes.

### Withdrawn operator surfaces (historical)

These pages describe the pre-0063 control plane, minted tokens, or `/admin/v1`.
They are not runbooks for a current deployment:

- [Minted-token guide](./minted-token-guide.md)
- [Administering a stateful deployment](./operations/admin-api.md)
- [Stateful Kubernetes deployment runbook](./operations/stateful-deployment-runbook.md)
- [Control-plane revision journal](./operations/control-plane-journal.md)
- [Revision convergence](./operations/revision-convergence.md)
- [Stateful integration](./operations/stateful-integration.md)
- [Policy activation](./operations/policy-activation.md)
- [Secret material in the stateful control plane](./security/secret-material.md)

## Develop and maintain

- [Contributing](../CONTRIBUTING.md) — local checks and dependency-lock policy.
- [Threat-model review triggers](./security/threat-model-review.md) — which
  changes require a security review, and the regression tests, threat-model or
  ADR updates, and release-impact statement each one owes.
- [Compatibility gates](./compatibility.md#the-published-rust-api) — the MSRV
  floor, the published-crate API check, and the reviewed override for an
  intentional break.
- [Backend responsibility boundaries](./maintainers/backend-contracts.md) — the
  eight responsibility-specific backend contracts, which paths they may be
  called from, and why there is no universal state backend.
- [Namespace and blob control-plane migration](./maintainers/namespace-control-plane-migration.md)
  — historical staged work for the superseded ADR 0062 target.
- [Release runbook](./maintainers/releasing.md) — release-please, artifact
  repair, crates.io ordering, and verification.
- [Release readiness](../RELEASE.md) — current public-beta evidence and known
  limitations.
- [Architecture decisions](./adr) — accepted design decisions and consequences.
- [ADR 0063: stateful-only namespaced gateway](./adr/0063-stateful-only-namespaced-gateway.md)
  — current product shape.
- [ADR 0064: charge actuals after the response](./adr/0064-charge-actuals-after-response.md)
  — no pre-dispatch budget hold; `remaining = limit - spent`.
- [ADR 0062](./adr/0062-blob-backed-flat-namespace-control-plane.md) and
  [ADR 0027](./adr/0027-stateless-and-stateful-operating-modes.md) — superseded.
