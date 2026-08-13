# 36. Recovery qualification: scenarios committed before the driver

Date: 2026-08-13

## Status

Accepted

Extends the qualification approach of
[ADR 0033](./0033-capacity-qualification-harness.md) from *what does a stateless
replica cost to serve* to *what does a stateful deployment do when it loses its
control plane*. The behaviour being qualified is the outage contract of
[ADR 0027](./0027-stateless-and-stateful-operating-modes.md) and the forward-only
migration and restore posture of
[ADR 0032](./0032-operator-preflight-and-forward-only-migrations.md).

## Context

Stateful mode makes two promises an operator has to be able to check. A
control-plane outage freezes change but not inference: replicas keep serving the
immutable snapshot they hold, a replica that boots into the outage may restore a
signed last-known-good cache, and the fleet converges by itself when Postgres
returns. And the journal is a store of record: a restore brings back revisions,
tenancy, secret metadata, pricing, and audit together, with a data-loss boundary
an operator can state.

Both promises are currently documented and unit-tested, and neither has ever been
observed end to end. That is the gap axond #219 exists to close.

It cannot be closed yet. A stateful replica still refuses to boot, because a
revision's resource *bodies* — tenancy, providers, catalogue, pricing, policy —
belong to slices that have not landed, so there is nothing to serve and therefore
no serving, convergence, or restore behaviour to observe. Waiting is the honest
option; waiting *silently* is not, because the thing most likely to go wrong is
not that the harness is late but that the scenarios get renegotiated one at a
time as each slice lands, until what ships is whatever was convenient to test.

The instinct to write the harness now against fakes is worse than waiting: a
recovery harness whose control plane is an in-process fake qualifies the fake.
The whole value of this evidence is that the database really went away.

## Decision

Commit the recovery **contract** as machine-readable data now, and the driver
when the deployment it drives can boot.

**The scenarios are committed as data.**
`qualification/recovery/manifest.toml` declares eight scenarios — control-plane
outage with last-known-good serving, three cold boots (valid, absent, and
unauthenticatable cache), recovery convergence, secret rotation without
redeployment, backup restore, and point-in-time recovery. Each carries a driver
capability, the evidence classes it retains, its gate, and the slices it waits
on with what it needs from each. A scenario the driver has no capability for
cannot be written, which is what keeps a future result reproducible from the
repository.

**The gate is properties, not durations.** A scenario fails on the fraction of
offered requests that failed, the convergence lag after recovery, revisions lost
before the recovery target, the terminal readiness verdict, whether
administrative writes behaved as retryably-unavailable or accepted, and whether
any unauthenticated administrative call succeeded — that last one bounded at
zero everywhere, because an outage may refuse a change and may never admit a
caller. Restore duration, outage duration, and lag samples are recorded and
never asserted: a shared runner moves them, and a flaky recovery gate is one that
gets switched off.

**The dependency map is enforced, in both directions.**
`crates/gateway/tests/recovery_contract.rs` fails when a blocked scenario names
no blocker, when it names an issue that is not one of the slices #219 waits on,
and when a slice #219 waits on is named by no scenario at all. The second-to-last
is how an unrelated dependency creeps in; the last is how a scenario quietly
disappears — the issue it needed goes unclaimed.

**"Executable" is a claim about code.** A scenario's `status` is cross-checked
against `Capability::is_implemented`, so flipping a manifest entry without
writing a driver fails, and writing a driver without flipping the entry fails
too. Today every capability reports unimplemented, and the suite asserts exactly
that rather than skipping.

**The subject will be real.** When the driver lands it runs against real
PostgreSQL and real replica processes — the outage is the database becoming
unreachable, the restore is a restore — in a lane with its own service
containers, the way the stateful test lane already runs. Nothing in this contract
may be satisfied by an in-process fake control plane.

### State tier

Tier 2. The harness qualifies a deployment that requires PostgreSQL, and its
lane needs a database it may take away and bring back. It raises no existing
deployment's tier: no shipped code path changes, and the contract test itself is
Tier 0 — it parses committed files and needs no service container, so the
default suite stays runnable with no datastore.

## Consequences

- The scenarios are settled while nobody is under delivery pressure to shrink
  them. When a slice lands, the question is "write this driver", not "decide what
  recovery means".
- A reader is told plainly that Axond has no recovery evidence yet. The
  [operations page](../operations/recovery-qualification.md) is labelled a
  contract, and the manifest marks every scenario blocked, so the file cannot be
  cited as a passing qualification.
- The contract will be wrong in places, and that is the cheaper direction of
  error: a gate written against a surface that does not exist may need
  correcting, and correcting it is a reviewed change to a committed file rather
  than a decision made inside a test that was going to be written anyway.
- Adding a scenario means changing the capability enum, not just the manifest —
  the same rule ADR 0033 applies to workloads, for the same reason.
- The contract test is a cheap gate that runs in the default suite. It compiles
  no new dependency and takes milliseconds, and its only job is to make the
  waiting visible: as long as the harness is blocked, the suite says so on every
  run.
