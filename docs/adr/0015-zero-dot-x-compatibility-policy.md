# 15. The `0.x` compatibility policy

Date: 2026-08-05

## Status

Accepted

## Context

Beta is the point at which other people's systems start depending on ours. Three
of axond's interfaces are already load-bearing outside the process, and each has
a different blast radius when it changes:

* **The config file.** Operators template it, commit it, and roll it out through
  ConfigMaps. A renamed key does not fail gracefully — the gateway refuses to
  boot (ADR 0002's fail-at-boot principle, working exactly as intended), which
  means a routine upgrade takes the fleet down.
* **The usage row shape.** `UsageRecord::SCHEMA_VERSION = 2` and
  `ops/postgres/usage_v2.sql` land in the operator's *own* Postgres, feeding
  their invoices and dashboards. ADR 0009 already committed to versioning it and
  to never editing shipped DDL in place, but the rule lives in that ADR and in
  `docs/usage-schema.md`, scoped to usage alone.
* **The HTTP error vocabulary and the telemetry names.** Callers branch on
  `429 budget_exceeded` versus `503 budget_unavailable`; dashboards select on
  `axond.usage.records_dropped`. Both break silently — a renamed metric just
  stops plotting.

SemVer says almost nothing useful here. Under SemVer, `0.x` is a blanket
disclaimer: any release may break anything. That is a fair licence for a
pre-release library and a poor basis for asking someone to run a gateway in
front of production traffic. Meanwhile the repo already has a Cargo-shaped
convention through release-please (`bump-minor-pre-major`), where a breaking
change bumps the minor and a feature bumps the patch — but nothing anywhere says
*what counts as breaking* for interfaces that are not Rust APIs. Nobody has to
resolve that question until the first release, which is now.

The alternative to writing it down is deciding case by case, and the failure
mode of that is well known: the interface stays "unstable" in principle, changes
freely in practice, and operators discover the policy by outage.

## Decision

Adopt an explicit `0.x` compatibility policy covering the config surface, the
usage schema, the HTTP error contract, and the telemetry names. It is published
in `docs/compatibility.md` — the operator-facing statement of what is supported
and what may change — and summarised here as the decision of record.

**Within `0.x`, a patch release is upgrade-safe.** Concretely: a config that
boots on `0.x.y` boots on `0.x.(y+1)`; a status code and error `type` keep their
meaning; a metric, span, or attribute name keeps existing; a usage reader keeps
working. Additive change — a new key with a default, a new enum variant, a new
error `type`, a new nullable column — is allowed in a patch, because a consumer
that ignores what it does not recognise is unaffected.

**A break is a minor bump plus a changelog entry with the migration.** Renaming
or removing a config key, changing a documented default, tightening validation
so a previously-valid config is refused, redefining an existing error `type` or
its status, renaming a metric or attribute — all minor, never patch.

**The usage schema versions independently of the gateway.** It is the operator's
table, not ours, so it carries its own `SCHEMA_VERSION` and its own DDL file,
and a breaking row-shape change ships as `usage_v<N+1>.sql` alongside the old
one rather than as an edit. This restates ADR 0009's rule and extends its
reasoning to the surfaces above.

**Explicitly outside the promise:** in-memory, per-replica behaviour (circuit
thresholds, credential-health timing, `in-memory` budget scope), diagnostic
error `message` text, and any claim about the `0.x` → `1.0` migration. `1.0` is
reserved for a genuine API commitment; beta does not pre-commit to it.

## Consequences

The policy is stronger than SemVer requires for `0.x`, and deliberately so: it
is what makes "beta" mean something to someone deciding whether to put the
gateway in a request path. The cost is real — a poorly-chosen config key name
now has to be lived with until a minor bump, and additive-with-a-default becomes
the default shape of a change. That cost is the point; it is paid in review, not
in someone else's incident.

It also gives release-please's existing pre-major configuration a meaning it did
not have: `bump-minor-pre-major` was a mechanical setting, and it now encodes
"this release breaks a published interface". Which release is a minor is
therefore decided by the policy above rather than by feel.

Nothing in this ADR changes code. It constrains future changes, and it makes the
first tag's contents a statement rather than a snapshot.
