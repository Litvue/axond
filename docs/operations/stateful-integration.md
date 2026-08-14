# Stateful integration: the release gates and what proves them

Stateful mode
([#160](https://github.com/Litvue/axond/issues/160)) is being built as a set of
*contract* slices — durable schemas, typed documents, protocol boundaries — each
landing on its own. The integration seam connects a bootstrap file to a control
plane, a control plane to a compiled immutable snapshot, and that snapshot to
the request path, plus the evidence that each of #160's release gates holds on
the assembled system. A cold or unconverged replica remains fail-closed. A
valid projected snapshot or signed last-known-good cache is the intended future
serving posture, but this build's production projection has no inbound
caller-principal source, so no stateful serving or cache-recovery claim is
active yet.

This page is the integration plan and its acceptance matrix. It exists so that
"is stateful mode ready?" has a single answer with a reference behind each line,
rather than a set of merged pull requests nobody has run together.

The current #345 slice is intentionally **fail-closed convergence wiring**. Its
production projection has no inbound caller-principal source, so stateful
compilation returns typed `unsupported`, no revision becomes active, and no
outage-serving or cache-recovery claim is active in this build. The principal-
projection slice is the explicit dependency that moves IG-03, IG-06, IG-07, and
the later serving gates from blocked to executable. The shipped Recreate
Deployment also omits `[convergence]` until durable per-replica storage exists.

- [ADR 0027](../adr/0027-stateless-and-stateful-operating-modes.md) — the two
  operating modes and what each one owns.
- [Control-plane revision journal](./control-plane-journal.md) — the durable
  storage layer.
- [Revision convergence](./revision-convergence.md) — how a published revision
  reaches a replica.

## Who owns what

| Owner | Owns | Examples |
| --- | --- | --- |
| A contract slice | One durable schema or typed document, its validation, and its unit-level tests | tenancy/RBAC rows, `axond.policy.v1`, price books, the `/admin/v1` protocol, the SecretStore trait |
| Integration | The seams between slices, the `serve` path, and the end-to-end evidence | constructing the control plane at boot, compiling a revision into a snapshot, publishing it atomically, readiness semantics, the smoke harness |

Integration never re-implements a contract. When a gate below is blocked, it is
blocked on a *body schema* or a *protocol* that a slice owns, and the integration
work is the wiring that becomes possible once that lands.

Integration-owned files, so that parallel slices and this work do not collide:

- `crates/gateway/tests/stateful_integration.rs` — the smoke harness.
- `crates/gateway/tests/support/stateful.rs` — its fixtures.
- `docs/operations/stateful-integration.md` — this page.
- The `serve`-path wiring in `crates/gateway/src/main.rs` and
  `crates/gateway/src/state.rs`, once the gates below unblock it.

## The dependency graph

Waves, in landing order. Everything inside a wave is independent of everything
else in it.

```
wave 0  (landed)   revision journal · convergence loop · LKG cache · preflight/migrate
                   status contract · typed credentials + secret lifecycle contract
                   #253 axond.policy.v1 documents          #250 derived availability
                   #254 /admin/v1 protocol boundary        #275 SecretStore (#145)
                   #207 catalogue import                   #143 admin API/CLI served
                   #251 approved price books               #247 catalogue aliases
                   #252 tenancy/principals/RBAC/audit      #255 model enablement
                   #238 authenticated dependency status    #295 store → credential pools
                   #244 empty-ledger reconciliation and adoption
                   #276 runtime policy activation          #249 usage outbox
                   #307 live control-plane status          #315 forced refresh
                        │
wave 1  (in flight) #302 availability projection          #311 catalogue import at runtime
                   #318 request-path pricing
                   — every one of them edits the `serve` seam
                   #312 credential lifecycle administered — edits this harness
                        │
wave 2  (integration) IG-01 … IG-05: boot → connect → hydrate → compile → publish → serve
                        │
wave 3  (integration) IG-06 … IG-10: the properties that only hold on the whole system
                        │
wave 4              #156 qualification evidence over the integrated system
```

A wave-1 slice that slips does not block the whole of wave 2 on paper — the gates
below name their own dependencies, and each is wired and qualified as soon as
*its* dependencies land. It does block it in practice when the slice edits the
same seam. At the time of writing, #302, #311 and #318 were each proposing
changes to `crates/gateway/src/main.rs`, `crates/gateway/src/state.rs`, or the
compiler — which is exactly where IG-03's wiring goes — and #312 to this page's
harness. That is a reading of unmerged branches rather than of this tree, so it is
provisional and worth re-checking; what is not provisional is the consequence.
Integration waits for that seam to settle rather than rebasing a fourth patch
through it.

## Acceptance matrix

Every gate has an identifier, the #160 release gate it discharges, the wiring
integration owns, what it depends on, and the harness scenario that proves it.
`Status` is one of three:

- `wired` — the scenario runs and asserts the property on a running process.
- `partial` — a running process proves the path that exists, and the `Depends on`
  cell names the part a contract slice still owns. Never a status a gate can hold
  without a service-backed scenario.
- `blocked` — the scenario asserts that the system remains fail-closed without a
  valid projected serving snapshot, and names what it waits for.

`Depends on` lists only what is still outstanding — an unlanded contract slice,
or an earlier gate that has to serve first. A dependency that lands moves to
wave 0 above and leaves the cell, so a cell naming no contract means the gate
waits on integration alone.

`crates/gateway/tests/stateful_integration.rs` parses this table. A gate added
here without a scenario, or a scenario without a row, fails the suite.

| Gate | #160 release gate | Integration wiring | Depends on | Evidence | Status |
| --- | --- | --- | --- | --- | --- |
| IG-01 | Explicit operating modes | `serve` boots stateless with no datastore, and a stateful bootstrap reaches its control plane and serves `/admin/v1`; anonymous inference is refused by auth first and authenticated inference remains behind the typed convergence refusal until principal projection exists | | `stateless_boot_serves_with_no_control_plane`, `stateful_boot_serves_administration_and_refuses_inference`, `stateful_boot_refuses_an_unresolved_reference` | wired |
| IG-02 | Postgres-first control plane | Operator preflight, forward-only migration, and the connect a replica performs before it serves | | `preflight_describes_a_stateless_install`, `migrate_prepares_a_control_plane_before_replicas_start` | wired |
| IG-03 | Configuration changes take effect atomically, without a restart | Hydrate the head revision, compile it into a whole snapshot, publish it atomically, keep serving the previous one when compilation or the database fails | | `hydrate_compile_publish_is_one_atomic_step` | blocked |
| IG-04 | Provider secrets rotate without redeployment | Resolve every credential a candidate snapshot needs through the SecretStore during compilation, never on the request path | IG-03 | `secrets_resolve_during_compilation_only` | blocked |
| IG-05 | Every mutation validated, revisioned, authorized, audited | The authenticated `/admin/v1` path from request to published revision: preconditions, idempotent replay, revision conflicts, and the audit event that attributes the credential. Breakglass end to end; OIDC principals against scoped grants once a replica authenticates one | admin OIDC authenticator | `an_admin_mutation_publishes_an_audited_revision` | partial |
| IG-06 | No control-plane reads on ordinary inference | Routing, catalogue, authentication, and pricing read only the published snapshot | IG-03 | `inference_touches_no_control_plane_connection` | blocked |
| IG-07 | Control-plane loss leaves last-known-good serving | Bounded backoff, staleness reporting, and cold boot from the signed last-known-good cache | IG-03 | `control_plane_loss_keeps_the_last_known_good_snapshot_serving` | blocked |
| IG-08 | Bounded, observable runtime | Readiness reflects convergence rather than process liveness; `/status` reports desired, loaded, active, and lag | IG-03 | `readiness_and_status_report_convergence` | blocked |
| IG-09 | Every request records the effective price version | The compiled snapshot carries the approved price-book identity into each usage record | IG-03 | `every_usage_record_names_the_price_version` | blocked |
| IG-10 | Tenant catalogue views isolated and explained | The tenant-facing catalogue is projected from the snapshot and explains effective availability. The administrative half serves now — `GET /admin/v1/catalogue` reads one tenant's enablements, aliases and unavailability reasons from the published revision — so what this gate waits on is the *served* catalogue: the alias a caller invokes, projected into a snapshot | IG-03 | `a_tenant_catalogue_is_isolated_and_explains_itself` | blocked |
| IG-11 | Published capacity and failure-recovery evidence | Stateful profiles in the qualification harness: convergence under load, control-plane outage, rolling upgrade | IG-03 … IG-08, #156 | `stateful_qualification_profiles_are_published` | blocked |

## The next gate that can become executable

IG-01 is wired: a replica boots against a migrated control plane, serves
`/admin/v1`, and remains fail-closed and unready until a valid projected
snapshot or cache is active — so the scenarios above assert a running stateful
process, and the loud-failure half asserts the *reference* a boot could not
resolve rather than any nonzero exit.

IG-03 is next, and it is what the remaining blocked gates wait behind: until a
published revision compiles into a runtime snapshot, IG-04 and IG-06 through
IG-11 have no served revision to assert against, which is why their scenarios
assert today's fail-closed posture instead. Much of its foundation is on main — the
policy document type (#253), the derived availability contracts (#250), the
`/admin/v1` boundary and its served runtime (#254, #143), the models.dev
catalogue import (#207), the envelope-encrypted SecretStore (#275), and now
tenancy, principals, RBAC and audit boundaries (#252) and model enablement with
project aliases (#255), on top of the wave-0 journal, convergence loop, and
last-known-good cache. Runtime policy activation (#276) was the last contract a
compiled snapshot could not resolve, and it has landed with the usage outbox
(#249): no gate waiting on IG-03 waits on a contract slice any more. IG-03 waits
on integration alone. Two gates still name a slice of their own: IG-05 the admin
OIDC authenticator, which is not downstream of compilation, and IG-11 the
qualification epic (#156), which is — it waits on IG-03 … IG-08 as well.

What it waits on is the principal-projection seam. The reconciler and compiler
are now constructed by `serve`, but the production chain returns typed
`unsupported` before it can publish a keyless candidate. The desired-state
identity model retains only workload-key digests, not recoverable caller
secrets, so this PR does not invent a projection from unrelated material. The
principal-projection slice must provide that source and its isolation rules
before IG-03 can become executable; the cache-storage slice must provide durable
per-replica storage before cold-boot recovery is enabled in the Recreate
deployment.

So the next integration pull request is IG-03, opened once that seam settles —
not against those branches, and not duplicating the compilation a contract slice
owns. In order, it:

1. constructs the control-plane backend, `MaterialLedger`, `LastKnownGood` cache
   and `Reconciler` in the stateful arm of `serve`, from the bootstrap file the
   administrative surface is already built from, and drops the `dead_code`
   allowance that says nothing does;
2. bootstraps once before the listener binds — head revision hydrated and
   compiled, or the signed cache adopted, or a loud refusal — so a replica never
   reaches the request path with an empty snapshot;
3. publishes the compiled snapshot into the inference state as one whole value,
   swapped behind the same generation-held drain runtime policy activation
   (#276) established, so no request observes half a revision;
4. runs the convergence loop behind the boot, leaving the previous snapshot
   serving when a compile or the database fails;
5. replaces `hydrate_compile_publish_is_one_atomic_step`'s refusal assertion with
   a scenario that publishes a revision through `/admin/v1`, waits for the
   snapshot it compiles into, and asserts inference is served from it — and moves
   the IG-03 row with it.

IG-07 and IG-08 follow it immediately and cheaply, because steps 2 and 4 are what
their scenarios assert: a cold boot from the cache with the control plane down,
and readiness that reflects convergence rather than process liveness.

IG-05's authenticated administrative path moved independently of
compilation, since `/admin/v1` already serves: a breakglass mutation is
validated, published as a revision, replayed under its idempotency key, refused
when it expects a superseded revision, and read back with the audit event that
attributes it — all on a running replica against a real control plane. It is
`partial` rather than `wired` because the credential doing all of that is
breakglass; a replica does not yet authenticate an OIDC principal, so
authorizing one against a scoped grant has no running process to assert against.
That a deployment can be administered before it can serve inference is the
honest shape of stateful mode today: durable desired state is written and
audited, and nothing projects it into the request path yet.

IG-11 is furthest out, and the [qualification
packet](./qualification.md) says why in the terms it owns: capacity is
`evidenced`, endurance, rollout and recovery are `harnessed` with no heavy run
behind them, and fault is `unbuilt`. Recovery gained its driver with #219 —
ten stages against a real Postgres — but every one of its scenarios still has a
blocked stage, for the reason above: stateful serving is not assembled.
Everything measured so far is one stateless process. Nothing in this page's
status column, and nothing merged so far, should be read as evidence that
stateful serving runs in production.

## What "wired" requires

A gate moves to `wired` in one pull request that does all four of:

1. lands the seam in integration-owned files;
2. turns its scenario in `crates/gateway/tests/stateful_integration.rs` into one
   that asserts the property on a running process, not on a type;
3. updates this row, including dropping the dependency that unblocked it;
4. records the operator-visible consequence on the page that owns it
   ([convergence](./revision-convergence.md), [the
   journal](./control-plane-journal.md), or
   [upgrades](./upgrades.md)).

A gate is never moved to `wired` because its dependencies merged. The scenario
runs, or the gate is blocked — and for a scenario that needs a datastore,
"runs" means it runs in a lane `CI Success` requires, where
`AXOND_TEST_REQUIRE_SERVICES=1` turns the local skip into a failure. A `wired`
row whose evidence only executes when a developer happens to export a DSN is a
claim nothing enforces.

## Running the harness

```sh
# The scenarios that need no datastore.
cargo test -p axond --all-features --test stateful_integration

# Including the control-plane scenarios, against the throwaway PostgreSQL
# CONTRIBUTING.md pins — on 55432, so it cannot be confused with a local one.
docker run -d --name axond-test-postgres -e POSTGRES_PASSWORD=axond-ci \
  -p 55432:5432 postgres:17.6-alpine
AXOND_TEST_POSTGRES_DSN=postgres://postgres:axond-ci@127.0.0.1:55432/postgres \
  cargo test -p axond --all-features --test stateful_integration
```

Without `AXOND_TEST_POSTGRES_DSN` the control-plane scenarios skip, the way the
rest of the suite treats optional datastores. CI's stateful lane sets
`AXOND_TEST_REQUIRE_SERVICES=1`, so a skipped scenario there is a failure rather
than a quiet pass. Each run works in a schema of its own and drops it when the
fixture goes out of scope — including when an assertion fails — so a shared
database stays clean and no run poisons the next.
