# Stateful integration: the release gates and what proves them

Stateful mode
([#160](https://github.com/Litvue/axond/issues/160)) is being built as a set of
*contract* slices — durable schemas, typed documents, protocol boundaries — each
landing on its own. None of them makes a replica *serve inference* statefully:
a stateful replica boots and serves `/admin/v1`, and refuses inference until a
published revision compiles into a runtime snapshot. That last step
is **integration**: the wiring that connects a bootstrap file to a control plane,
a control plane to a compiled snapshot, and a snapshot to the request path, plus
the evidence that each of #160's release gates actually holds on the assembled
system.

This page is the integration plan and its acceptance matrix. It exists so that
"is stateful mode ready?" has a single answer with a reference behind each line,
rather than a set of merged pull requests nobody has run together.

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
                        │
wave 1  (in flight) #252 tenancy/principals/RBAC/audit      #244 empty-ledger adoption
                    #255 model enablement + project aliases #249 usage outbox
                    #276 runtime policy activation
                        │
wave 2  (integration) IG-01 … IG-05: boot → connect → hydrate → compile → publish → serve
                        │
wave 3  (integration) IG-06 … IG-10: the properties that only hold on the whole system
                        │
wave 4              #156 qualification evidence over the integrated system
```

A wave-1 slice that slips does not block the whole of wave 2: the gates below
name their own dependencies, and each is wired and qualified as soon as *its*
dependencies land.

## Acceptance matrix

Every gate has an identifier, the #160 release gate it discharges, the wiring
integration owns, what it depends on, and the harness scenario that proves it.
`Status` is either `wired` (the scenario runs and asserts the property) or
`blocked` (the scenario asserts that the system still refuses inference rather
than pretending otherwise, and names what it waits for).

`Depends on` lists only what is still outstanding — an unlanded contract slice,
or an earlier gate that has to serve first. A dependency that lands moves to
wave 0 above and leaves the cell, so a cell naming no contract means the gate
waits on integration alone.

`crates/gateway/tests/stateful_integration.rs` parses this table. A gate added
here without a scenario, or a scenario without a row, fails the suite.

| Gate | #160 release gate | Integration wiring | Depends on | Evidence | Status |
| --- | --- | --- | --- | --- | --- |
| IG-01 | Explicit operating modes | `serve` boots stateless with no datastore, and a stateful bootstrap either reaches its control plane and serves `/admin/v1` — refusing inference while no revision is compiled — or fails loudly on the reference it could not resolve | | `stateless_boot_serves_with_no_control_plane`, `stateful_boot_serves_administration_and_refuses_inference`, `stateful_boot_refuses_an_unresolved_reference` | wired |
| IG-02 | Postgres-first control plane | Operator preflight, forward-only migration, and the connect a replica performs before it serves | #244 | `preflight_describes_a_stateless_install`, `migrate_prepares_a_control_plane_before_replicas_start` | wired |
| IG-03 | Configuration changes take effect atomically, without a restart | Hydrate the head revision, compile it into a whole snapshot, publish it atomically, keep serving the previous one when compilation or the database fails | #252, #255, #276 | `hydrate_compile_publish_is_one_atomic_step` | blocked |
| IG-04 | Provider secrets rotate without redeployment | Resolve every credential a candidate snapshot needs through the SecretStore during compilation, never on the request path | IG-03 | `secrets_resolve_during_compilation_only` | blocked |
| IG-05 | Every mutation validated, revisioned, authorized, audited | The authenticated `/admin/v1` path from request to published revision, including breakglass | #252 | `an_admin_mutation_publishes_an_audited_revision` | blocked |
| IG-06 | No control-plane reads on ordinary inference | Routing, catalogue, authentication, and pricing read only the published snapshot | IG-03 | `inference_touches_no_control_plane_connection` | blocked |
| IG-07 | Control-plane loss leaves last-known-good serving | Bounded backoff, staleness reporting, and cold boot from the signed last-known-good cache | IG-03 | `control_plane_loss_keeps_the_last_known_good_snapshot_serving` | blocked |
| IG-08 | Bounded, observable runtime | Readiness reflects convergence rather than process liveness; `/status` reports desired, loaded, active, and lag | IG-03, #238 | `readiness_and_status_report_convergence` | blocked |
| IG-09 | Every request records the effective price version | The compiled snapshot carries the approved price-book identity into each usage record | IG-03, #249 | `every_usage_record_names_the_price_version` | blocked |
| IG-10 | Tenant catalogue views isolated and explained | The tenant-facing catalogue is projected from the snapshot and explains effective availability | IG-03, #255 | `a_tenant_catalogue_is_isolated_and_explains_itself` | blocked |
| IG-11 | Published capacity and failure-recovery evidence | Stateful profiles in the qualification harness: convergence under load, control-plane outage, rolling upgrade | IG-01 … IG-08, #156 | `stateful_qualification_profiles_are_published` | blocked |

## The first gate that can become executable

IG-01 — a stateful `serve` that reaches its control plane or refuses — is the
gate every other stateful one waits behind: until a replica boots statefully,
IG-03 through IG-11 have nothing running to assert against, which is why their
scenarios assert today's refusal instead.

Its foundations are now on main: the policy document type (#253), the derived
availability contracts (#250), the `/admin/v1` boundary and its served runtime
(#254, #143), the models.dev catalogue import (#207), and the envelope-encrypted
SecretStore (#275), on top of the wave-0 journal, convergence loop, and
last-known-good cache. What a stateful boot still cannot resolve is
*who* a request belongs to and *what* it may reach: tenancy, principals, and RBAC
(#252) and model enablement and project aliases (#255). A boot wired before those
land would hydrate a revision it cannot fully compile.

So the next integration pull request is IG-01, opened when #252 and #255 are on
main — not against their branches, and not duplicating the compilation a
contract slice owns. It replaces
`stateful_boot_refuses_to_serve_an_empty_snapshot` with a scenario that boots a
replica against a migrated control plane and asserts it serves the head revision,
and moves the IG-01 row with it. IG-03 follows once IG-01 serves.

IG-11 is furthest out, and the [qualification
packet](./qualification.md) says why in the terms it owns: capacity is
`evidenced`, endurance and rollout are `harnessed` with no heavy run behind
them, recovery is a `declared` contract with no driver, and fault is `unbuilt`.
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
runs, or the gate is blocked.

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
