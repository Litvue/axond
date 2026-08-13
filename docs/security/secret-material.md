# Secret material in the stateful control plane

The stateless deployment keeps provider credentials in environment variables:
TOML carries a reference, the process reads the value at boot, and nothing
durable ever holds it. The stateful control plane adds a second path — an
administrator stages material, a credential resource pins an opaque
`SecretRef`, and a replica resolves that reference when it compiles a snapshot —
and this page states what that path guarantees, and where each guarantee is
enforced as a test.

The claims in [the deployment security model](./deployment-model.md) are
unchanged by it. This page is the evidence for them under stateful mode.

## What is guaranteed

- **A reference is durable; material is not.** A revision, a resource body, an
  audit event, and every row of the journal carry a `sct_…` identifier and a
  version number. Material lives only in the secret store and in the snapshot a
  replica compiled from it.
- **Material is disclosed once, to the runtime.** Staging accepts plaintext.
  Afterwards `describe`, `exists`, and `transition` answer about the version,
  and only `resolve` — which a replica calls while compiling — answers with the
  material.
- **Material is not renderable by accident.** `SecretMaterial` has a redacted
  `Debug`, no `Display`, no `Serialize`, and no deref to `str`; reading it is an
  explicit call.
- **A rotation cannot cut a request in flight.** A snapshot is immutable and
  publication is a pointer swap, so a request that has already dispatched to a
  provider finishes with the credential it started with, and the next request
  uses the new one.
- **A resolution failure is not an outage.** A candidate whose material cannot
  be resolved is refused whole; the replica keeps serving its last known good
  snapshot, and the refusal names the reference and the reason, never the value.
- **Retired material is destroyed.** Tombstoning a version removes the material
  rather than relabelling it; afterwards the version does not resolve and does
  not exist.
- **Material is not reachable from an operational surface.** Response bodies and
  headers, logs at every level, spans, usage records, and status responses are
  swept for it.
- **An administrative surface renders references, not material.** Diffs, publish
  responses, idempotent replays, refusals, state, history, audit trails, and
  convergence all describe the credential by reference. A document that pastes
  material where a `sct_…` reference belongs is refused by naming the form
  expected, rather than by echoing what arrived. The refusal distinguishes a
  value of the wrong kind from a right-prefixed identifier whose UUID is
  malformed, so an operator can tell a mispaste from a typo, and it says so from
  the parse failure rather than from the text: no identifier refusal anywhere —
  request field, record field, log line, or audit trail — renders the string it
  refused.

## How it is tested

`crates/gateway/src/secret_redaction/` runs these as one suite against the real
reconciler, the real publication seam, the real router, and — for durable state
— a real Postgres journal.

| Module | What it asserts |
| --- | --- |
| `sweep` | The detector itself: raw, case-folded, base64 (three alphabets), hex, and every 12-character fragment of a sentinel, over text and raw bytes. |
| `harness` | Sentinel material, a fake provider that records the `Authorization` header it was presented with, and a compiler that resolves desired-state credential references through a `SecretStore`. |
| `lifecycle` | One-time disclosure, rotation overlapping an in-flight request, last-known-good retention on a failed resolution, destruction of retired material, and that the stateless environment-variable path still serves and still redacts. |
| `request_path` | One served request's response, logs, spans, and usage record; a transport failure's error surfaces; a rejected caller's refusal; and the status projection of an unresolved secret. |
| `admin_surface` | Every `/admin/v1` response about a credential that names material genuinely staged and activated in the production store: dry-run diff, publish, idempotent replay, reused-key conflict, a document with material pasted into `secret`, and the `state`, `history`, `audit` and `convergence` reads. |
| `journal` | Every row of every table in the control-plane schema, rendered as text, plus manifests, hydrated revisions, audit trails, and idempotency replays and conflicts. |
| `stateful` | The zero-redeploy sequence against the *production* secret store: stage, activate, serve, rotate, roll back to the previous version, and a revoked version refusing a candidate while the last known good keeps serving — plus a sweep of every stored row and a cross-owner read that never reaches a pool. |

Two properties of the suite are worth stating, because a redaction test that
lacks either is theatre:

- **It searches encodings, not plaintext.** Canonical resource bodies are stored
  as bytes and render as hex; a base64-encoded header would survive a plaintext
  search. Every assertion runs the full encoding set, and a failure names the
  surface and the sentinel's label without reprinting the material.
- **Every assertion has a tripwire.** Each test also asserts that the material
  really was in play — that the fake provider was presented with it, or that the
  store resolves it — the journal test holds material resolved out of a store
  live across every sweep, so "nothing leaked"
  cannot silently degrade into "nothing happened".

## Running it

The `journal` and `stateful` tests need Postgres. Locally they skip when no DSN
is configured:

```sh
AXOND_TEST_POSTGRES_DSN=postgres://postgres:axond-ci@127.0.0.1:5432/postgres \
  cargo test -p axond --all-features --locked secret_redaction
```

CI runs the stateful lane with `AXOND_TEST_REQUIRE_SERVICES=1`, which turns a
missing DSN into a panic: a green run means the durable sweep executed rather
than being skipped.

## What is not covered yet

Unwrapping is production code: the harness compiler resolves through
`SecretMaterialization` and hands the resulting `ResolvedSecrets` to
`ConfigSnapshot::build_with`, so retention, rotation overlap, and zeroization
are asserted about the shipped seam and its `MaterialLedger` rather than about a
mock. Turning a resolved version into a credential pool entry is production code
too: `RuntimeProjection` emits the `[[credential]]` entries a provider call
leases from, each naming the exact version the candidate resolved, so a request
that reaches the fake provider with the sentinel key proves the shipped path
carried it there.

Two seams the harness still supplies, neither touching material: an alias's
targets, because projecting a catalogue is its own slice, and which namespace an
inbound key binds to, because binding a caller to a projected namespace is
#252's. A projected project is reached by a qualified id no `axond.toml` can
declare, so the suite rebinds its bootstrap key to it rather than inventing an
identity model. Stateful `serve` is still not wired for the same reason.

The `/admin/v1` boundary is swept over its real route table and the real Postgres
journal, with authentication and authorization faked — an administrator's
identity is #252's, and no administrative response is shaped by who asked for it.
