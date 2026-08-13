# 43. Stated recovery objectives, a supported backend window, and a deny-by-default production posture

Date: 2026-08-13

## Status

Accepted

Makes the restore posture of
[ADR 0032](./0032-operator-preflight-and-forward-only-migrations.md) numeric, and
supplies the objectives the scenarios of
[ADR 0037](./0037-recovery-qualification-harness.md) will be measured against
once its driver can boot. The state tiers are those of
[ADR 0002](./0002-stateless-by-default-stateful-by-opt-in.md).

## Context

Stateful mode already says what is durable and that restores are forward-only.
What it did not say is how much data a disaster may cost, how long recovery is
allowed to take, which backend versions the promise holds for, or what a
production deployment's network surface is. Each of those is a commitment an
operator has to plan against, and each was previously implied by prose, by the
version of Postgres CI happened to run, or by whatever an operator's own manifests
did.

An unstated objective is not a weak objective, it is an unfalsifiable one: a
backup procedure nobody has restored from is a hypothesis, and a point-in-time
restore that quietly replays to the end of the WAL passes every "the rows are
there" check while being useless for the incident it exists for.

## Decision

**The objectives are numbers, and they are executable.** RPO ≤ 5 minutes and
RTO ≤ 30 minutes for control-plane recovery, per PostgreSQL cluster, documented
in [Backup, restore, and PITR](../operations/backup-and-recovery.md) with what
each requires of the deployment. `ops/restore-drill.sh` performs both recoveries
on every change and asserts the asymmetry a PITR exists for — committed before
the target present, the write after it absent — with `axond migrate status` as
the acceptance test. Redis is deliberately outside the objectives: it is hot
enforcement state, not history.

**The supported window is one fact with one source.** PostgreSQL 14–17 and Redis
6.2 and newer, tied to the enforced boot floor and to the images CI runs, with a
gate that fails when the documentation, the floor, and CI disagree. A supported
major upgrade of Postgres needs no axond migration; it needs the backend's own
upgrade and this drill.

**Production networking is deny-by-default.** The shipped production overlay
denies both directions and then names what a gateway needs: ingress from the
ingress controller, DNS, public HTTPS with the link-local and RFC 1918 ranges
excepted, and label-selected rules for the stores and the collector. This is a
default for the manifests axond ships, not a new requirement of the software: the
gateway's own boundary is unchanged, and a deployment that keeps its existing
networking loses no supported behaviour.

### State tier

Tier 0. This decision adds no state and raises no deployment's tier. It states
objectives for Tier 2 (Postgres) state where a deployment has opted into it, and
explicitly excludes Tier 1 (Redis) from those objectives; a Tier 0 deployment
holds nothing durable and nothing here applies to it.

## Consequences

Recovery becomes something a deployment can be measured against, and a
regression in it fails CI rather than being discovered during an incident. The
cost is that the numbers now constrain us: raising `archive_timeout`, dropping
WAL archiving, or letting the drill's runtime grow past the RTO are changes to a
published commitment, and the supported-backend window has to be widened
deliberately — with CI images and the enforced floor moved together — rather than
by noticing that a newer Postgres seems to work. Deny-by-default egress makes a
misconfigured peer a denied flow instead of an open one, which is the safer
direction to be wrong in but does mean an operator whose stores live in another
namespace has to widen the rules before traffic works.
