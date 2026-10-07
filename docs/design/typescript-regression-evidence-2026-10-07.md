# TypeScript regression audit — 2026-10-07

This is local validation of the regression-gate changes on top of conversion
head `cb1ead38a4cb3a0dd04689946eda07e2cbecf255` (PR #531). The Rust sources on
that head are unchanged from `503103cf6df769063648d7285c6358ded043a419`, the
checkout used to build the oracle. It is not production deployment evidence.

## Results

| Gate | Result |
| --- | --- |
| Node 22.14.0 unit/behavior suite, Postgres 16 enabled | 376 passed; 0 failed; 0 skipped |
| Bun 1.4.2 unit/behavior suite, Postgres 16 enabled | 372 passed; 1 skipped (separate connect timeout, declared runtime gap); 0 failed |
| New comparison self-tests on Bun 1.4.2 | 3 passed; 0 failed (also included in Node's 376) |
| Rust vs Node 22 gateway | All 49 cases, 7 expected usage rows, durable budgets/cadence and restart passed |
| Rust vs Bun compiled binary | All 49 cases, 7 expected usage rows, durable budgets/cadence and restart passed |
| Python vendor SDK compatibility | 13 passed |
| Node vendor SDK compatibility, Node 22.14.0 | 18 passed |
| Local workerd / Hyperdrive emulation, Postgres 16 | 9 passed; 0 failed; 0 skipped |
| Compiled binary smoke | Health, extension load, API-version refusal, unbundled-package refusal, bundled-package load and SQLite restart passed |
| Container, Node 22.14 image | Build, health, readiness, namespace creation and persisted restart passed |
| Host-independent imports, alert catalogue, doc samples, OpenAPI, repository docs | Passed |
| Dependency policy (`npm run check:npm`) | Passed after a clean install: 0 vulnerabilities; 43 production packages licensed; `sharp` overridden to 0.35.5 to fix GHSA-wq5f-xc86-pv6w |

The compiled binary is 82,339,296 bytes, SHA-256
`ec6af430f63d697ff97adbd736babe79f96a505edd273bf20d503e862fc73515`.
The local container image is
`sha256:0a08938275a0ea56952c12a197233571aa984c949ed4bc0b2f6f215acedbd126`.
Its build used the repository Dockerfile with a temporary build-only CA secret
mount for npm; the session CA was not copied into the image.

## Differences found and corrected

The previous 12-case differential runner passed. The expanded runner found that
TypeScript omitted Rust's `bad request:` diagnostic prefix and returned a JSON
error body for withdrawn routes where Rust returns an empty 404. Those behaviors
now agree. Existing tests retain their validation/error assertions with the
corrected wire expectations.

The comparison previously parsed large JSON integers into JavaScript numbers,
which could hide a one-unit difference above 2^53. It now preserves numeric source
digits and has tests that demonstrate the previously hidden mismatch. Expected
status assertions prevent two processes returning the same wrong status from
passing the gate. Usage comparison preserves null costs and checks namespace and
period as well as model, status and cost; seven expected rows prevent two empty
usage tables from passing.

The external-IP connect-timeout test was environment-dependent: this executor
immediately refused the test address. It now uses a local TCP peer that never
completes TLS, exercising the actual connect timer and verifying the gateway's
504 mapping and address omission deterministically.
The workerd special-password test now uses `AXOND_TEST_POSTGRES` rather than
hardcoding port 5432 and credentials, so the full lane can run on an isolated
database at another port.

## Release blockers and evidence still owed

The first CI run failed the dependency audit on Wrangler's transitive `sharp`
0.35.4 (GHSA-wq5f-xc86-pv6w). A root override pins the patched 0.35.5 and
regenerates only sharp and its native image-library lock entries, retaining the
existing Wrangler/miniflare versions. The audit gate remains unchanged. A clean
`npm ci`, dependency policy check, SVG-to-PNG native-library smoke and all nine
workerd/Hyperdrive integration tests passed with the patched graph.

This environment has no configured Cloudflare or PlanetScale credentials. Live
Hyperdrive/PlanetScale qualification, release-tag OIDC signing, target-database
rollback, and a production canary were not run. Node and compiled-binary fixture
agreement does not establish those properties. Nor does it close differential
fault/concurrency replay or full OpenAPI schema equivalence.

Use the [feature matrix and release gates](typescript-regression-gates.md) to
review each declared difference and complete the target-specific evidence. An
unqualified “no feature regressions” claim is not supported until that review and
the outstanding gates are complete.
