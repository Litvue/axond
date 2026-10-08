# TypeScript conversion review stack

All 21 PRs are ready for review. Start with [#534](https://github.com/Litvue/axond/pull/534);
reviews can proceed across the stack, and merges must follow dependency order.

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
| 1 | define TypeScript SDK and review-stage CI | 3763 | [#534](https://github.com/Litvue/axond/pull/534) |
| 2 | add TypeScript request and JSON parsers | 2115 | [#535](https://github.com/Litvue/axond/pull/535) |
| 3 | add namespace and budget state primitives | 1830 | [#536](https://github.com/Litvue/axond/pull/536) |
| 4 | add provider wire accounting and telemetry | 2544 | [#537](https://github.com/Litvue/axond/pull/537) |
| 5 | add TOML scanning and Figment value primitives | 2756 | [#538](https://github.com/Litvue/axond/pull/538) |
| 6 | add configuration shape and extraction validation | 1487 | [#539](https://github.com/Litvue/axond/pull/539) |
| 7 | load and validate TypeScript gateway configuration | 3207 | [#540](https://github.com/Litvue/axond/pull/540) |
| 8 | cover bind and environment configuration compatibility | 1442 | [#541](https://github.com/Litvue/axond/pull/541) |
| 9 | add provider dispatch and credential failover | 1560 | [#542](https://github.com/Litvue/axond/pull/542) |
| 10 | mount the namespaced TypeScript Hono gateway | 3188 | [#543](https://github.com/Litvue/axond/pull/543) |
| 11 | qualify gateway routing and budget management | 3045 | [#544](https://github.com/Litvue/axond/pull/544) |
| 12 | qualify extension ordering and streaming deadlines | 2201 | [#545](https://github.com/Litvue/axond/pull/545) |
| 13 | qualify terminal streams rotation and admission | 1980 | [#546](https://github.com/Litvue/axond/pull/546) |
| 14 | qualify settlement faults and provider secret omission | 1633 | [#547](https://github.com/Litvue/axond/pull/547) |
| 15 | add Postgres persistence and durable-store qualification | 3455 | [#548](https://github.com/Litvue/axond/pull/548) |
| 16 | add CLI discovery usage delivery and shutdown services | 2542 | [#549](https://github.com/Litvue/axond/pull/549) |
| 17 | host the TypeScript gateway and operator extensions | 2759 | [#550](https://github.com/Litvue/axond/pull/550) |
| 18 | qualify CLI configuration diagnostics and refusal order | 1670 | [#551](https://github.com/Litvue/axond/pull/551) |
| 19 | add rate limit redaction and token extensions | 1101 | [#552](https://github.com/Litvue/axond/pull/552) |
| 20 | host the TypeScript gateway on Workers Hyperdrive | 1845 | [#553](https://github.com/Litvue/axond/pull/553) |
| 21 | qualify TypeScript artifacts and regression gates | 3074 | [#554](https://github.com/Litvue/axond/pull/554) |

## How to review a slice

1. Read the stated contract and the slice's review focus. Automated review should
   use this PR's diff plus the specific prerequisite definitions it needs.
2. Compare behavior to the Rust oracle, especially refusal order, namespace
   authority, integer precision, durable idempotency and streaming terminals.
   Passing tests supplement that comparison.
3. Inspect the relevant tests for plausible failure cases. Gateway-level tests
   land once the factory exists; executable tests land once the CLI exists.
   The configuration helper slices are exercised by the loader/property suites
   in slices 7–8. Their original split retained the source declarations; the review hardening now adds explicit parser and configuration fixes.
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
Shared fixtures were initially copied without changing their bodies. The review hardening consolidates them and adds cleanup after failed assertions. One store test that
reads the CLI host source moved unchanged to `postgres-host.test.ts`. An extra
blank line at the end of `app.ts` was removed. No runtime behavior was added by
this restructuring; the earlier regression fixes remain included.

Initial qualification after the original split (2026-10-07):

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

The original compiled review binary is 82,339,296 bytes, SHA-256
`6874c7d1fa7cb8f7ab376cf9e8a1405fde358474f4d6c6f9f9f810128fa65371`.

Full source/runtime evidence is also recorded in the
[regression evidence](typescript-regression-evidence-2026-10-07.md).

## Review hardening (2026-10-08)

The [finding dispositions](typescript-review-findings.md) record all 85 original
review findings: 79 addressed by code, tests, fixtures or gates, and six staging
or contract clarifications. Fixes land in their owning slices and flow through
all descendants. The static import check passes at all 21 stack heads. Shared
HTTP test fixtures are now introduced at their first caller and close servers
after assertion failures. The required CI Success job includes TypeScript;
merge queues also run the TypeScript workflow.

Qualification of the complete corrected source:

- Node 22.14 with Postgres 16: 402 passed, no skips.
- Bun 1.4.2 with Postgres 16: 401 passed; its existing native connect-timeout
  test is explicitly skipped because Bun's connector cannot simulate it.
- workerd/Hyperdrive integration: nine passed, no skips.
- Python vendor SDK: 13 passed; Node vendor SDK: 18 passed.
- Rust differential replay: all fixture responses, seven usage rows, durable
  budgets/cadence and restart recovery matched the Rust oracle.
- Compiled binary: health, extension loading/version refusal, package bundling
  and SQLite restart smoke passed. cosign 2.5.2 signed and verified with an
  ephemeral key; private signing material was deleted.
- Web imports, alert catalogue, documentation samples, OpenAPI and workflow
  policies passed. npm audit: zero vulnerabilities; 43 licensed production
  packages.

The corrected binary is 82,351,584 bytes, SHA-256
`40dabd9fceae76f9f19bc5c9f5fac133cc9a3663c7351faccdab4319971f9815`.
Local qualification does not replace CI at the pushed heads or maintainer
approval. ADR 0067 records the authority and schema upgrade impact. Rust still
owns the release binary/image; live deployment and release-tag OIDC evidence
remain part of a separately authorized cutover.

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
