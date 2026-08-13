# Axond documentation

Use this page as the task-oriented entry point. The architecture decision
records explain *why* Axond works this way; the guides below explain how to use
and operate it.

## Evaluate Axond

- [Getting started](./getting-started.md) — boot the public image, prove
  authentication and routing, and make a first provider request.
- [Compatibility contract](./compatibility.md) — supported routes, provider
  wires, client behavior, and the `0.x` stability policy.
- [State tiers](./configuration.md#state-tiers) — decide whether a deployment
  needs Redis, Postgres, or neither.
- [Operating modes](./adr/0027-stateless-and-stateful-operating-modes.md) —
  the accepted stateless/stateful ownership, failure, and request-path design
  (stateless is today's behavior and the default).

## Install

- [Installation and verification](./installation.md) — crates.io, signed
  binaries, OCI images, checksums, SBOMs, signatures, and attestations.
- [Docker Compose](./deployment/docker-compose.md) — pull-first Tier 0 and
  stateful local stacks.

## Connect a client

- [OpenAI clients](./clients/openai.md) — chat completions, Responses,
  embeddings, streaming, Python, and TypeScript.
- [Anthropic clients](./clients/anthropic.md) — native Messages and streaming.
- [Compatibility contract](./compatibility.md) — route and wire-family matrix.

## Configure

- [Configuration reference](./configuration.md) — every key, default, and
  validation rule.
- [`axond.example.toml`](../axond.example.toml) — the complete annotated
  configuration surface.
- [Minted-token guide](./minted-token-guide.md) — key generation, issuance,
  scopes, rotation, delegation, and revocation.

## Deploy

- [Deployment overview](./deployment.md) — choose an environment.
- [Container runtime](./deployment/container.md) — Docker or Podman, image
  verification, mounts, and runtime contract.
- [Linux and systemd](./deployment/systemd.md) — prebuilt binary and hardened
  service unit.
- [Kubernetes](./deployment/kubernetes.md) — ConfigMap, Secret, Deployment,
  Service, probes, security context, and rollout constraints.
- [Managed containers](./deployment/managed-containers.md) — the portable
  contract for ECS/Fargate, Cloud Run, Azure Container Apps, and Nomad.
- [Stateful backends](./deployment/stateful-backends.md) — Redis/Postgres
  availability, migrations, and scaling consequences.
- [Production checklist](./deployment/production-checklist.md) — security,
  streaming, rollout, observability, and recovery review.

## Operate

- [Observability and runbook](./observability.md) — traces, metrics, logs,
  usage records, alerts, boot failures, and reloads.
- [Troubleshooting](./operations/troubleshooting.md) — symptom and typed-error
  decision tree.
- [Upgrades and rollback](./operations/upgrades.md) — compatibility policy,
  migration ordering, mixed-version rules, and rollback limits.
- [Backup, restore, and PITR](./operations/backup-and-recovery.md) — what is
  durable, the recovery objectives, the archiving and dump mechanisms, and the
  drill that proves a restore lands where it was aimed.
- [Qualification packet](./operations/qualification.md) — what production
  qualification has measured, what is only declared, what is unbuilt, and the
  retained evidence behind each.
- [Capacity qualification](./operations/capacity.md) — per-replica envelopes,
  candidate SLOs, the committed load profiles, and what a capacity run gates on.
- [Recovery qualification](./operations/recovery-qualification.md) — the
  committed outage, cold-boot, convergence, rotation, and restore scenarios, the
  evidence each retains, and the slices the harness still waits on.
- [Endurance qualification](./operations/endurance.md) — the twelve-hour mixed
  workload, what a long run leaves behind, and the leak and accounting
  properties it gates on.
- [Rollout qualification](./operations/rollout.md) — the rolling upgrade executed
  against a real fleet: drain, readiness removal, replacement, mixed-version
  windows, rollback limits, and the evidence a run leaves behind.
- [Usage schema](./usage-schema.md) — durable row contract and delivery
  guarantees.
- [Administering a stateful deployment](./operations/admin-api.md) — the
  `/admin/v1` routes and `axond admin`, their preconditions, and what a stateful
  replica serves before revision convergence ships.
- [Control-plane revision journal](./operations/control-plane-journal.md) — the
  stateful mode's Postgres schema, migrations, schema-status refusals, and
  outage behaviour.
- [Revision convergence](./operations/revision-convergence.md) — how a published
  revision reaches replicas, convergence targets, what a replica reports, and
  the signed last-known-good cache.
- [Stateful integration](./operations/stateful-integration.md) — the #160
  release gates, who owns each slice, and the harness scenario that proves each
  gate.
- [Deployment security model](./security/deployment-model.md) — trust
  boundaries, TLS termination, secret delivery, and image verification.
- [Secret material in the stateful control plane](./security/secret-material.md)
  — what a stored secret reference guarantees, and the regression suite that
  proves material stays out of state, responses, logs, and telemetry.
- [Tenant isolation evidence](./security/tenant-isolation-evidence.md) — which
  layer enforces each part of isolation, the test that proves it, and what is
  not covered yet.
- [Security policy](../SECURITY.md) — private vulnerability reporting, the
  supported-version window, response targets, and how a fix and advisory ship.
- [Fuzzing](./security/fuzzing.md) — the config, token, and query targets, the
  properties they assert, and the required-versus-scheduled lanes.

## Develop and maintain

- [Contributing](../CONTRIBUTING.md) — local checks and dependency-lock policy.
- [Threat-model review triggers](./security/threat-model-review.md) — which
  changes require a security review, and the regression tests, threat-model or
  ADR updates, and release-impact statement each one owes.
- [Compatibility gates](./compatibility.md#the-published-rust-api) — the MSRV
  floor, the published-crate API check, and the reviewed override for an
  intentional break.
- [Backend responsibility boundaries](./maintainers/backend-contracts.md) — the
  seven responsibility-specific backend contracts, which paths they may be
  called from, and why there is no universal state backend.
- [Release runbook](./maintainers/releasing.md) — release-please, artifact
  repair, crates.io ordering, and verification.
- [Release readiness](../RELEASE.md) — current public-beta evidence and known
  limitations.
- [Architecture decisions](./adr) — accepted design decisions and consequences.
- [ADR 0027: stateless and stateful operating modes](./adr/0027-stateless-and-stateful-operating-modes.md)
  — state ownership matrix, failure/outage matrix, request-path database rules,
  and the dependency map for the control-plane implementation slices.
