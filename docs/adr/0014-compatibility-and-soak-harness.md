# 14. Compatibility and soak harness

Date: 2026-08-05

## Status

Accepted

## Context

Everything the beta promises about the request path is a *wire* promise. A
caller points its existing OpenAI or Anthropic SDK at the gateway and expects
the same bytes it would have got from the provider — including the parts that
are not merely cosmetic: an Anthropic thinking block's `signature` must survive
so the block can be replayed back to the provider on the next turn (ADR 0012),
and a streamed tool call's `input_json_delta` fragments must arrive in one
piece for an SDK to reassemble them.

None of that was covered. The suites that existed are unit tests inside the
crates: they assert an adapter's translation, a decoder's output, a circuit's
state machine. They cannot catch a regression in what the *process* does — how
it parses config, which header it injects upstream, what its relay emits, what
it charges — because they never build one.

Two further properties are invisible to unit tests by construction. A streamed
request holds an upstream connection for as long as the client reads; a bug in
the cancellation path leaks that connection, and a leak only shows up under
concurrency. And spend reconciliation (ADR 0010) is a claim about *outcomes* —
a cancelled stream charges its partial, a stream that never opened charges
nothing — which needs streams that actually get cancelled.

The historical wiremock acceptance suite covered some of this and did not
survive the rewrite. What is wanted back is its spirit — recorded provider
bytes, replayed offline — rather than its mechanism.

## Decision

Three test pillars, all hermetic, sharing one set of committed fixtures.

### 1. Black-box against the built binary

`crates/gateway/tests/` boots the real `axond` binary (`CARGO_BIN_EXE_axond`)
with a generated config, pointed at a fake upstream that speaks the OpenAI and
Anthropic wire shapes. Not an in-process router: the qualified artefact is the
process, so config parsing, credential injection, inbound auth, and the usage
records on stdout are all in the assertion surface. The gateway's stdout usage
sink is the black-box view of accounting — no test reaches into a
`BudgetStore`, because a caller cannot either.

Provider SDK compatibility is asserted twice, deliberately: by a raw
OpenAI-shaped HTTP client in the Rust suite, and by the vendors' own Python
SDKs in `tests/compat/`. The SDKs are the honest test of the wire — they parse
strictly, and they are what a customer will actually run — but pinning them
into the Rust workspace would drag a language runtime into `cargo test`, so
they get their own CI lane.

### 2. Fixtures are a stability contract

Recorded provider responses live in `tests/fixtures/` as plain `.json` bodies
and byte-exact `.sse` files. Both harnesses serve the same files, and the
Anthropic streaming test asserts the client-visible bytes are **identical** to
the bytes the upstream sent — the relay may not reframe, reserialize, or
reorder them. That is a fidelity assertion about the *gateway*, and it is
symmetric in the fixture by construction, so the fixtures' own content is
pinned separately: `replay.rs` asserts each committed `.sse` still carries the
frames accounting depends on, and that the settled charge matches the tokens
the fixture reports. Editing a fixture therefore moves a stated expectation,
not just a sample. Fixtures are redacted (thinking signatures become
`REDACTED_...` placeholders, which the tests then assert survive), and
`tests/fixtures/README.md` documents how to capture and redact a new one.

The format is deliberately dumb — no cassette library, no recorder — because
the value is in the bytes being reviewable in a diff, and a hand-editable
fixture is one a reviewer can reason about.

### 3. Soak is split: short on every PR, long out of band

The soak suite runs concurrent long-lived streams over both wire shapes, with
three endings mixed in — read to completion, client hangs up mid-stream,
upstream dies mid-event — and asserts:

- **no leaked upstream connections**: the fake upstream counts response bodies
  opened and dropped, and the counts must balance once the clients are gone;
- **budget reconciliation**: exactly one usage record per stream, with the
  status its ending earns and a charge that matches ADR 0010 — a partial for
  what a cancelled or broken stream relayed, `$0` for a stream that never
  opened;
- **bounded memory**: the gateway's RSS is sampled while the streams are in
  flight and must not grow by more than a loose ceiling, which is what
  distinguishes "relays event by event" from "buffers the body".

The short run (a few dozen streams, sub-second) is part of `cargo test` and so
part of the `CI-Success` gate. The long run (hundreds of streams) is the same
code behind `AXOND_SOAK=1`, in a `workflow_dispatch` + weekly `soak` workflow.
Splitting on a *scale* knob rather than on a separate implementation is the
point: the per-PR lane exercises every assertion the nightly one does, so the
nightly cannot rot into something nobody runs.

## Consequences

- A wire-fidelity or accounting regression fails deterministically, offline,
  with no provider account, no secret, and no network in any lane.
- Editing a fixture moves an asserted expectation and reads like one in review.
- The `sdk-compat` lane adds a Python runtime and a hash-pinned lockfile to CI.
  Refresh `tests/compat/requirements.txt` from its direct pins with
  `just compat-lock`; the refresh excludes releases published less than seven
  days ago. Bumps are deliberate: when a new SDK release breaks, the bump
  commit is the report. Nothing enters the Rust dependency graph, so
  `deny.toml` is untouched.
- A provider changing its wire is still invisible until a fixture is
  re-captured against the live API — this harness proves the gateway is
  faithful to what was recorded, not that the recording is current. Refreshing
  fixtures stays a deliberate, reviewed act.
- The long soak can only fail out of band, so a leak introduced at a scale the
  short run does not reach is caught within a week rather than at the PR.

## Amendment: a second SDK runtime

The SDK pillar was extended to the vendors' **Node** SDKs in `tests/compat-ts/`,
a required `sdk-compat-ts` lane holding the same fixtures and the same generated
config as the Python lane. The reason is not a second opinion on the same bytes:
a TypeScript caller is what a large share of customers actually run, its SDKs
disagree with the Python ones on defaults that reach the wire — the Node
embeddings client asks for `base64` unless told otherwise — and the lane is
compiled with `tsc --strict`, so the gateway is also held to what the SDKs' own
type definitions describe rather than to what a dynamically-typed client
tolerates. Pins are exact down to the Node runtime (`.nvmrc`) and enforced by
`ops/compat-ts-pins.py`, and nothing enters the Rust dependency graph. Go stays
deferred: it would re-assert this wire, and the case for a third runtime is the
stateful/admin API, which is not stable yet.
