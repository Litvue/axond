# TypeScript release conformance qualification

This qualification compares the downloaded Rust **v0.6.3** release with a Bun
compiled TypeScript candidate based on merged main
`5509d29983536e7581c1a29bb9c6f50d16ab3d81`. It also compares the fault suite with
Rust built from that main commit. The Rust source is identical to the conversion
baseline; the separate Rustdoc repair changes comments only.

## Findings and fixes

The initial merged binary matched all 49 ordinary contract cases. Extending the
comparison to provider failures and partial streams exposed two conversion
regressions, also reproduced against current Rust:

1. **Provider error display.** TypeScript returned a raw diagnostic where Rust
   prefixes invalid-request/context errors and uses a fixed public sentence for
   dependency failures and unavailable models. Public responses now match Rust;
   bounded, redacted provider diagnostics remain on attempt spans.
2. **Partial-stream charge estimates.** TypeScript estimated the rewritten
   upstream body, including its injected `stream_options`, instead of the
   admitted caller body. In the fixture, cancellation charged 77 instead of 60
   microdollars, and idle expiry charged 97 instead of 80. The estimate now
   excludes gateway wire rewrites while preserving prompt changes made by
   pre-dispatch extensions. Separate tests check both unchanged and extended
   prompts without provider-reported usage.

The first slow-consumer byte hash differed only because Rust reserializes chat
JSON. The comparator permits JSON key order/whitespace while preserving event
order and payload values, as the ordinary comparator already does. Native
Responses/Messages remain byte-faithful in the ordinary suite. Incorrect initial
expectations that upstream 401/500 pass through as 401/500 were corrected to
Rust's gateway status 502; status remains an explicit assertion.

## Qualified scope

| Check | Scope |
| --- | --- |
| Ordinary release comparison | 49 cases: routes, auth, validation, buffered inference, streams, namespace management, fixed/monthly budgets, usage summary; seven charged rows; graceful restart |
| Fault comparison | 37 assertions: provider 400/401/403/404/429/500, header/body/idle expiry, truncated native stream, proven alternate-credential rotation, 16 concurrent requests, cancellation after content, a paused consumer, exact aggregate charges |
| Crash and SQLite rollback | Kill after confirmed settlement; reopen with each runtime; switch each database to the other runtime and back without a schema reset |
| PostgreSQL rollback | Nine assertions on one database: Rust → TS → Rust, API-created namespace retention, further inference/charges, then TS → Rust with a restricted role and `create_table=false` |
| PostgreSQL store qualification | 46 tests, including concurrent settlement, schema migration ownership, absent columns, non-owner access, lossless attributes, and bounded socket cleanup |
| Runtime suites | Node and Bun workspace suites; local workerd/Hyperdrive; vendor Python and Node SDKs; compiled artifact sign/verify, extension loading, and SQLite restart |

The fault comparator is part of the required Rust-shadow CI job and retains its
JSON report as an Actions artifact. It records executable hashes, full bounded
fault responses, settlement state, and every assertion. For a launcher script,
its hash identifies the launcher; the checked-out source commit identifies the
runtime implementation. Compiled-binary runs identify the complete executable.

## Reproduction

Install the pinned Node/Bun versions and workspace dependencies. Download and
verify the Rust release archive against its GitHub asset digest:

```text
Rust v0.6.3 source: 78202ec137ac00075bae3b9f4aec1a6649899850
x86_64 musl archive SHA256:
1edc75386b00d1f0a39f70b585bd2ed17cdb66fa88d0a8b6e45f16b637539273
Extracted Rust executable SHA256:
c4c95efea1dbf831dbe4acf73bffe0f627b74ced0f3816c8ee4d94363123e02a
Qualified TypeScript executable SHA256:
9ab15b70a531adeca80e0aef8ae3cb5ac16b3465983a8e0b97a1e316e0eb5c05
```

From `ts/`, using absolute executable/report paths:

```bash
bun build packages/cli/src/main.ts --compile --outfile "$TS_BINARY"
node --experimental-strip-types --disable-warning=ExperimentalWarning \
  scripts/shadow-compare.ts --rust "$RUST_BINARY" --ts "$TS_BINARY"
node --experimental-strip-types --disable-warning=ExperimentalWarning \
  scripts/release-conformance.ts --rust "$RUST_BINARY" --ts "$TS_BINARY" \
  --report "$FAULT_REPORT"
```

The PostgreSQL script requires an **empty local disposable database** whose name
starts with `axond_conformance_`. It refuses an existing schema and does not
reset data. Its initial role must own the schema and be able to create the
temporary restricted application role. The DSN is not written into the report.

```bash
AXOND_CONFORMANCE_DSN="$LOCAL_DISPOSABLE_DSN" \
  node --experimental-strip-types --disable-warning=ExperimentalWarning \
  scripts/postgres-rollback.ts --rust "$RUST_BINARY" --ts "$TS_BINARY" \
  --report "$POSTGRES_REPORT"
```

## Limits and rollout gates

This proves the tested fixture contracts. It does not prove zero loss when a
process dies before settlement is durable, restore a billing-grade journal, or
approve the other [declared differences](typescript-regression-gates.md#declared-differences-that-block-an-unqualified-parity-claim).
The v0.6.3 release predates Rust's ADR 0063 withdrawals; the comparison covers the
remaining shared contracts, not every feature present in that release.

One local workerd silent-query check exceeded its 70-second test deadline; an
isolated retry returned the expected redacted 503 in approximately 60.6 seconds.
Retain that observation in the release packet and confirm stable bounds on the
intended Hyperdrive deployment before a Worker cutover.

Production qualification still needs the target's real schema/roles, ingress,
collector/alert configuration, startup/readiness/shutdown behavior, and a rollback
drill on its database. Use one runtime as the owner of each live inference
request; never duplicate billable calls to compare implementations. Start with
fixture traffic in an opted-in namespace, then an agreed small canary, and stop
expansion on unexplained response/charge differences or failed recovery.

No production deployment, release tag, or package publication was performed.
