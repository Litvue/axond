# TypeScript conversion review stack

Start with [#534](https://github.com/Litvue/axond/pull/534); it is open for review.
Later slices remain drafts until their prerequisites have been reviewed.

The review stack replaces the combined diffs of PR #531 and its regression
follow-up #533. Their branches retain the original work and validation history.
The stack contains the conversion at `cb1ead38a4cb3a0dd04689946eda07e2cbecf255`
and all regression fixes through `e1264ea1a903c21393a6872821021b8c78f7b147`.
Its main/Rust baseline is `503103cf6df769063648d7285c6358ded043a419`.

## Review order

Review each PR against its immediate parent branch. Each slice changes fewer
than 4,000 lines, including tests; the first contains the generated dependency
lock. The implementation and test files stay below 2,000 lines apiece. The
[machine-readable map](../../ops/typescript-review-stack.json) assigns every
changed file to a slice and records its base branch and review focus.

| Order | Review slice | Changed lines | PR |
| --- | --- | ---: | --- |
| 1 | define TypeScript SDK and review-stage CI | 3630 | [#534](https://github.com/Litvue/axond/pull/534) |
| 2 | add TypeScript request and JSON parsers | 2048 | [#535](https://github.com/Litvue/axond/pull/535) |
| 3 | add namespace and budget state primitives | 1754 | [#536](https://github.com/Litvue/axond/pull/536) |
| 4 | add provider wire accounting and telemetry | 2429 | [#537](https://github.com/Litvue/axond/pull/537) |
| 5 | add TOML scanning and Figment value primitives | 2749 | [#538](https://github.com/Litvue/axond/pull/538) |
| 6 | add configuration shape and extraction validation | 1486 | [#539](https://github.com/Litvue/axond/pull/539) |
| 7 | load and validate TypeScript gateway configuration | 3201 | [#540](https://github.com/Litvue/axond/pull/540) |
| 8 | cover bind and environment configuration compatibility | 1430 | [#541](https://github.com/Litvue/axond/pull/541) |
| 9 | add provider dispatch and credential failover | 1540 | [#542](https://github.com/Litvue/axond/pull/542) |
| 10 | mount the namespaced TypeScript Hono gateway | 3131 | [#543](https://github.com/Litvue/axond/pull/543) |
| 11 | qualify gateway routing and budget management | 3349 | [#544](https://github.com/Litvue/axond/pull/544) |
| 12 | qualify extension ordering and streaming deadlines | 2304 | [#545](https://github.com/Litvue/axond/pull/545) |
| 13 | qualify terminal streams rotation and admission | 2309 | [#546](https://github.com/Litvue/axond/pull/546) |
| 14 | qualify settlement faults and provider secret omission | 1965 | [#547](https://github.com/Litvue/axond/pull/547) |
| 15 | add Postgres persistence and durable-store qualification | 3391 | [#548](https://github.com/Litvue/axond/pull/548) |
| 16 | add CLI discovery usage delivery and shutdown services | 2482 | [#549](https://github.com/Litvue/axond/pull/549) |
| 17 | host the TypeScript gateway and operator extensions | 2768 | [#550](https://github.com/Litvue/axond/pull/550) |
| 18 | qualify CLI configuration diagnostics and refusal order | 1670 | [#551](https://github.com/Litvue/axond/pull/551) |
| 19 | add rate limit redaction and token extensions | 1020 | [#552](https://github.com/Litvue/axond/pull/552) |
| 20 | host the TypeScript gateway on Workers Hyperdrive | 1848 | [#553](https://github.com/Litvue/axond/pull/553) |
| 21 | qualify TypeScript artifacts and regression gates | 2774 | [#554](https://github.com/Litvue/axond/pull/554) |

## How to review a slice

1. Read the stated contract and the slice's review focus. Automated review should
   use this PR's diff plus the specific prerequisite definitions it needs.
2. Compare behavior to the Rust oracle, especially refusal order, namespace
   authority, integer precision, durable idempotency and streaming terminals.
   Passing tests supplement that comparison.
3. Inspect the relevant tests for plausible failure cases. Gateway-level tests
   land once the factory exists; executable tests land once the CLI exists.
   The configuration helper slices are exercised by the loader/property suites
   in slices 7–8. Their declarations are unchanged from the source conversion.
4. Record remaining concerns in the appropriate PR. Authentication/extension
   authority, durable budgets/migrations, settlement and release workflows need
   a human reviewer in addition to automated review.

The foundational CI job runs all unit tests available at that stack head, the
host-independent import check and the unchanged dependency audit. Postgres 16 is
configured for every unit-test job. The final slice restores the complete
TypeScript workflow: Node/SDK/workerd, Bun, Rust comparison and signed binary.
A green early slice qualifies its available components, not the whole gateway.

## Preservation and validation

The large config file was divided into `config-toml.ts`, `config-values.ts`,
`config-shapes.ts` and `config.ts`. Dependencies run in that order; there are no
cycles between these modules. Public config imports remain available through
`config.ts`. A TypeScript syntax-tree comparison confirmed that all 256 original
configuration declarations retained their bodies/initializers; only imports,
exports, declaration placement and surrounding whitespace changed.

The large app, behavior, configuration and CLI config-file test files were split
at complete top-level test declarations. A syntax-tree comparison confirmed all
223 test bodies, with their order within each original suite, remained intact.
Shared fixtures were copied without changing their bodies. One store test that
reads the CLI host source moved unchanged to `postgres-host.test.ts`. An extra
blank line at the end of `app.ts` was removed. No runtime behavior was added by
this restructuring; the earlier regression fixes remain included.

Local qualification after the split:

- Node 22.14 with Postgres 16: 376 passed, no skips.
- Bun 1.4.2 with Postgres 16: 375 passed, one declared connect-timeout skip.
- workerd/Hyperdrive integration: nine passed, no skips.
- Compiled binary: all six extension/health/restart smoke markers passed.
- Rust differential replay through the compiled binary: all 49 cases, seven
  usage rows, durable budgets/cadence and restart recovery passed.
- Python vendor SDK: 13 passed; Node vendor SDK: 18 passed.
- The audit reports zero vulnerabilities and 43 licensed production packages.
- Every local import/export resolves at each stack head. Available new tests
  are checked at the stage that introduces them; database tests use an isolated
  Postgres fixture. The foundation and complete tree pass docs/workflow policy.

The compiled review binary is 82,339,296 bytes, SHA-256
`6874c7d1fa7cb8f7ab376cf9e8a1405fde358474f4d6c6f9f9f810128fa65371`.

Full source/runtime evidence is also recorded in the
[regression evidence](typescript-regression-evidence-2026-10-07.md).

## Merge and integration review

Use the repository's required squash merges in order. Before merging a parent,
record the current base/head tips for every remaining branch. After its squash
lands, rebase the next branch's own commit(s) onto main, using its old parent tip
as the exclusion boundary. Restack every descendant in order using its saved
old parent tip and the newly rebased parent. Push with `--force-with-lease`, then
retarget the next PR to main. Retargeting alone after a squash leaves obsolete
parent commits in the diff. Keep the original #531/#533 branches as references.

The final slice is the integration checkpoint. Its reviewer must examine the
cross-module request/admission/settlement lifecycle, cumulative schema changes,
artifact/extension loading and every declared difference in the
[feature matrix](typescript-regression-gates.md). Run the complete workflow on
the actual merge/release commit. Merge approval and production cutover are
separate decisions: live Hyperdrive/PlanetScale, release-tag OIDC signing,
target rollback and canary evidence are still owed.
