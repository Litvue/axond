# 37. Recovery qualification: scenarios committed before the driver

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

It cannot be closed yet. A stateful replica boots and administers, but it refuses
*inference*, because a revision's resource *bodies* — tenancy, providers,
catalogue, pricing, policy — belong to slices that have not landed, so there is
nothing to serve and therefore no serving, convergence, or restore behaviour to
observe. Waiting is the honest option; waiting *silently* is not, because the
thing most likely to go wrong is not that the harness is late but that the
scenarios get renegotiated one at a time as each slice lands, until what ships is
whatever was convenient to test.

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

**"Executable" is a claim about code.** A `status` is cross-checked against the
driver, so flipping a manifest entry without writing a driver fails, and writing
a driver without flipping the entry fails too.

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
- A reader is told plainly how much has been observed. The
  [operations page](../operations/recovery-qualification.md) states which stages
  run and which are declared, and no scenario is executable, so neither file can
  be cited as a passing qualification.
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

## Amendment: stages, and the driver that runs today

Date: 2026-08-13

The original decision treated a scenario as one indivisible claim, which turned
out to be wrong in a way worth recording: the two halves of a recovery scenario
become executable at different times. Losing the journal under a converged
replica, cold-booting from the signed cache, and converging when it returns are
all reachable against a real database *now*. Offering inference across the same
window is not, and will not be until a replica can serve a projected revision.
Under the original shape, the honest status for those scenarios was `blocked`,
which hid five runnable outages behind a slice they do not need.

**A scenario is split into stages.** Each stage carries its own status,
evidence, blockers, and prose; a scenario is executable exactly when all of its
stages are. Nothing about the gate changes: the gate stays whole and
scenario-level, and each stage evaluates the fields it is in a position to
measure and records the others as `not_evaluated` with the reason. A partially
executable scenario therefore cannot report a whole gate as met.

**The outage is a cut, not an injected error.** The replica reaches Postgres
through a loopback link the harness owns; severing it drops the live connection
and refuses reconnection, so the replica meets a dead socket and its reconnect
path runs. A store wrapper returning `Unavailable` would have qualified the
wrapper — and, more importantly, the database keeps its rows through the cut,
which is what makes the recovery half mean anything.

**The driver lives in the crate, not in `tests/`.** Unlike capacity and soak,
which qualify a process from outside, a recovery stage has to hold a replica's
reconciler, its signed cache, and a real `PostgresControlPlane` at once and then
take the database away from underneath them. None of that is reachable from
outside the binary while stateful boot is not wired to `serve`. The cost is
recorded in the artifacts: the cold-boot stages note that the store handle is
built before the cut, because `connect` against an unreachable database fails
before a reconciler exists. When stateful `serve` lands, the serving stages
bring the driver back out to a process.

**No database, no evidence.** A run without `AXOND_TEST_POSTGRES_DSN` writes no
artifact rather than falling back to an in-process control plane, which is the
original decision's rule applied to the driver that now exists.

Consequences of the amendment:

- Five stages produce machine-readable evidence under `target/recovery/`, one
  JSON document per stage, carrying the build and schema identity, the timeline,
  the observations, and a verdict per gate field.
- No scenario is qualified end to end, and the operations page and manifest both
  say so. The change is that they now also say which halves *are* observed.
- Landing #148 and #149 unblocks the serving stages of scenarios whose
  control-plane stages already pass, so those scenarios flip to executable
  without their outage behaviour being re-litigated.

## Amendment: two lanes, and evidence that survives its own failure

Date: 2026-08-13

Two things the first amendment left implicit turned out to matter once the
restore half became executable.

**A stage names the lane that runs it.** The durable half of `backup-restore`
and `point-in-time-recovery` cannot run inside the in-process driver: it needs a
database that is dumped, dropped, restored, and recovered to a target, and a
*second* replica booted on the result to read it. That is a shell drill around
Docker, not a `cargo test`. So the manifest gained a `runner` field —
`stateful-tests` or `restore-drill` — and `ops/restore-drill.sh` became the
second lane rather than a drill standing beside the harness. It publishes the
state a recovery has to bring back through `axond admin apply` against a running
replica, so what is restored is a deployment a replica produced rather than rows
this script invented, and it writes the same artifact schema through
`ops/recovery-evidence.py`. A stage that names no lane, and a lane that owns no
stage, both fail the contract test.

**A stage that fails must still leave its evidence.** The first driver asserted
its conditions, so a genuine regression unwound the stage before its artifact
was written — a missing file, which is the one failure mode a retained evidence
directory cannot describe, and which CI's `if-no-files-found: warn` then reads
as nothing to upload. Conditions are now *recorded* as checks and judged at the
end of the stage: a failing stage writes an artifact naming the check, its
bound, and what was observed, and fails afterwards. `ops/check-recovery-evidence.py`
closes the other end by reading the manifest as the list of artifacts each lane
owes, refusing a missing file, a foreign schema, a wrong lane, a failed verdict,
an empty timeline, or an artifact carrying a forbidden string. It is run in both
lanes in CI, and its own `--self-test` keeps it from degenerating into a checker
that accepts everything.

Consequences of the amendment:

- Ten stages produce evidence: five in-process, five in the restore lane.
- The drill's credential and cache-signing key are generated per run and never
  printed, and the checker is given both, so an artifact that leaked one fails
  the lane instead of being uploaded.
- The restore lane qualifies the revision journal, its checksums, the tenancy
  and access projections, the credential references, the restored secret owner
  and lifecycle, the retained catalogue snapshot, and the audit rows. Approved
  price books and usage records remain explicitly blocked rather than being
  implied by a restore that "came back".
