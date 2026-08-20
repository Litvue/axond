# Fuzzing Axond

Axond parses four kinds of untrusted input: what an operator's configuration
file says, what a caller puts in a request, what a provider sends back, and what
an upstream catalogue publishes. This project fuzzes all four, on the parsers
the process actually runs.

| Target | What it drives | Why |
| --- | --- | --- |
| `config_toml` | `Config::from_toml_str` and the whole validation graph | Boot and every `SIGHUP` reload re-parse a file the gateway does not control |
| `token_verify` | `axt1.` JWS decoding, key selection, signature, and every claim check | A minted token is the one credential an attacker can shape freely |
| `credentials_query` | `GET /v1/credentials/status?namespaces=…` parsing | Hand-rolled percent-decoding, so malformed escapes, duplicate keys, empty values, and oversized inputs all land here |
| `flat_v2_body` | Deployment, namespace, and inbound-grant v2 durable body decoding | Signed object-store and LKG hydration must distinguish malformed state from forward schema skew without panics or unbounded parsing |
| `sse_decode` | `SseDecoder` on arbitrary bodies split at arbitrary chunk boundaries | A stream arrives in whatever pieces the network produced, and an event, a delimiter, or a character can straddle two of them |
| `provider_stream` | The OpenAI, Foundry, and Anthropic stream decoders — translated and native | They interpret provider JSON mid-relay, where a panic loses a live response |
| `provider_error` | `ProviderError::from_upstream`, `::transport`, and the classification behind them | An upstream failure body decides whether the gateway retries, fails over, or opens a circuit |
| `catalog_import` | The models.dev import: decoding, schema validation, normalization, content identity, semantic classification, and admission | A third party publishes it, a background refresh imports it unattended, and what it says about prices and capabilities feeds routing and spend decisions |

The properties each target asserts live in [`src/lib.rs`](./src/lib.rs) and
[`src/wire.rs`](./src/wire.rs): a parser returns rather than panicking, a
refusal is a typed value the gateway could answer with, and what it accepts
stays inside the bounds the request path relies on — including that a signature
from one namespace's signer never verifies into another.

The `flat_v2_body` smoke replay pins every committed filename to one exact
semantic outcome through the same `flat_v2_body_target` entrypoint libFuzzer
uses. The table is intentionally exhaustive in both directions, so adding,
removing, or renaming a seed requires updating the asserted contract:

| Seed | Expected outcome |
| --- | --- |
| `deployment-missing-field.json` | `incompatible` |
| `deployment-unknown-field.json` | `incompatible` |
| `deployment-unknown-variant.json` | `incompatible` |
| `deployment-valid.json` | `accepted` |
| `grant-semantic-invalid.json` | `invalid` |
| `grant-valid.json` | `accepted` |
| `namespace-valid.json` | `accepted` |
| `namespace-wrong-schema-shape.json` | `incompatible` |

The wire targets add what a relay depends on: the decoder never holds more than
its limit and never retains a complete event, decoding never expands its input,
a chunk boundary cannot change what a body decodes to, a stream that ends
mid-event is refused by `finish`, every provider diagnostic is truncated to
`MAX_DIAGNOSTIC_BYTES`, and classification stays deterministic and internally
consistent. Disclosure is asserted with canaries: the harness holds a gateway
credential and an upstream URL that it passes to nothing, so either appearing in
a rendered error is a finding.

The catalogue target takes a *structured* input, because a byte flip almost never
produces a document that still parses and says something different — which is the
case that matters. Half its inputs are arbitrary payloads, reaching decoding, the
schema, and normalization; the other half are single edits of the bundled
models.dev seed (a published rate, a description, a capability flag, a lifecycle
status, a provider-neutral record, a field the schema does not define, or
arbitrary bytes spliced into the document), each rendered with its object keys
rotated and its whitespace re-chosen. On top of the properties every target
asserts, it asserts:

- **Key order and whitespace are not content.** The same catalogue re-rendered
  has the same content id, and importing it is not an update. Neither is the
  fetch time nor the validators: provenance is kept, and kept out of identity.
- **Drift is refused, not absorbed.** A field the schema does not define is
  ignored and changes nothing; a field whose *meaning* drifted — an unknown
  status, an unknown modality, a negative price — is a typed refusal naming a
  JSON Pointer.
- **A refused payload cannot replace a good one.** Every import runs over a
  last-known-good catalogue holding the seed; after a refusal the active content
  id is still the seed's.
- **A price change is a price change.** A published-rate edit classifies as
  price-only and a descriptive edit as metadata-only, so the two can never be
  confused — which is the confusion a spend decision cannot survive.
- **An override is a contradiction, and it is local.** The recorded overrides
  are compared with *every* difference recomputed against the neutral record, so
  a difference the import dropped fails as loudly as an override it invented,
  and each JSON Pointer must point inside the offering that states it. Like the
  no-publication check, this one is calibrated once per run: removing a recorded
  override must be noticed, or the run fails as blind.
- **Importing is not publishing.** The seam reads the routing table a request
  would be served from off a real `AppState`, through the same snapshot load a
  request performs, before and after each import; anything published would move
  it. The check is calibrated once per run against a separate state that *is*
  published into, so a comparison that could never move cannot pass for proof.
- **What is accepted is a catalogue.** Every accepted import holds at least one
  model, one provider, and one offering — a routable catalogue, not one whose
  every model is offered: a single model no provider serves is an ordinary
  upstream state. A document that kept its model records and lost its providers
  section offers nothing at all to route or price, so it is a typed refusal
  (`Unoffered`) rather than a replacement for the held catalogue;
  `drift-providers-empty.json` in the corpus pins it.
- **A refusal is not sized by the payload.** Refusals quote upstream text as a
  bounded excerpt and lean on the JSON Pointer beside it for the exact location,
  so a map key the size of the payload ceiling cannot turn each scheduled retry
  into megabytes of upstream-chosen log; `drift-oversized-key.json` pins it. A
  list of them is bounded in its length too, since how many models share one
  offered key is the document's choice as well. The two refusals that carry no
  pointer — a payload that is not JSON, and one the schema rejects — keep the
  deserializer's trailing `at line L column C` as well as the head, since it is
  the only locator they have; `drift-oversized-schema-message.json` pins it.
- **An absent wrapper states nothing, and a malformed one is refused.** A
  record that omits `modalities` or `limit` imports with neither stated rather
  than with an invented ceiling (`drift-missing-limit.json`), while a stated
  wrapper of the wrong shape still refuses the whole import
  (`drift-limit-type.json`). The corpus carries both so the two cannot quietly
  collapse into one.

Runs are hermetic. The seam the targets call
([`crates/gateway/src/fuzz_seam.rs`](../crates/gateway/src/fuzz_seam.rs)) builds
its verifiers from a configuration compiled into the binary with synthetic key
material, so nothing here opens a socket, reads a file, or holds a real secret.
The catalogue import is hermetic the same way and demonstrably so: it runs the
real source — conditional fetch, strict parse, admission — against an in-memory
`CatalogFetch` that serves bytes already in hand and records what it was asked
for, and the target asserts that the only thing the import path reached for was
the configured `/catalog.json` URL.

## Layout

- `fuzz_targets/` — the `cargo fuzz` entry points, one per target.
- `src/lib.rs` — the config and credential target bodies, shared with the smoke.
- `src/wire.rs` — the SSE, provider-stream, and provider-error target bodies.
- `src/bin/smoke.rs` — the bounded, deterministic seed replay CI requires.
- `seeds/<target>/` — the committed corpus. `corpus/` and `artifacts/` are
  generated and ignored.

This is deliberately its own Cargo workspace: the targets need nightly and a
sanitizer runtime, and they reach `axond`'s parsers through a seam only
`--cfg fuzzing` compiles, which no other consumer can switch on. Nothing at the
repository root builds it, so this project runs its own `cargo fmt --check` and
`cargo clippy -- -D warnings` in the same CI lane as the smoke.
[ADR 0052](../docs/adr/0052-fuzzing-the-untrusted-input-parsers.md) records the
decision and what it costs.

That flag reaches every dependency rather than just `axond` — Cargo has no
per-crate rustflags, and it is what `cargo fuzz` sets anyway. Since a crate is
allowed to weaken cryptography under it, the smoke's first act is
`assert_signature_verification_is_real`: the seam mints a token, verifies it, and
must then refuse that token with a bit flipped in each of its three JWS segments.
That is the durable check; [`.cargo/config.toml`](./.cargo/config.toml) records
the lockfile audit behind it.

## The smoke (stable, bounded, on every pull request)

```bash
just fuzz-smoke
```

Proves signature verification is live, and that pinned valid SSE fixtures still
decode to the events they must under every boundary they can be split on — both
are there because the other assertions are relative, and a parser that returned
nothing at all would satisfy them. Then it replays every seed plus fixed
derivations — truncations, single-byte flips, one oversized repetition — a set
of tokens minted at replay time, and a set of catalogue edits applied at replay
time. The token scenarios and the catalogue scenarios are each pinned to the
outcome they exist for by name, so a check that stopped being reachable fails the
lane rather than being covered for by another scenario's class. It fails on a
panic, on an input slower than its
budget, or on an allocation past a hard cap enforced by the binary's own global
allocator. It also fails if the corpus stops reaching a spread of outcome
classes, so the lane cannot go green by refusing everything at the door.

Every command here runs `--locked`, and this workspace has its own lockfile that
records the gateway crates through the path dependency on `axond`. So a pull
request that adds or bumps a dependency of any `crates/` member has to refresh it:

```bash
just fuzz-lock   # cargo fetch in fuzz/, recording the change and nothing else
```

The lane also lints the seam (`just fuzz-seam-clippy`), because
`crates/gateway/src/fuzz_seam.rs` is `#![cfg(fuzzing)]`: the root clippy builds it
as an empty library, and this workspace's clippy skips `axond` as a path
dependency, so nothing else here has it under `-D warnings`. And it checks the
lockfile first and says so, rather than leaving a bare `--locked`
error to interpret. Releases are handled for you: `release-please.yml` syncs both
lockfiles when the workspace version changes, using this same command for this
one.

## Coverage-guided runs (nightly toolchain)

```bash
cargo install cargo-fuzz --locked
just fuzz config_toml         # one target, a bounded local run
just fuzz-all                 # every target, the way the scheduled lane does
```

On a musl Linux host, `just fuzz` translates the auto-detected host target to
the matching GNU target because cargo-fuzz's prebuilt installer may otherwise
fail to link AddressSanitizer. Install that standard library target before the
first run (replace the example triple if the host architecture differs):

```bash
rustup target add x86_64-unknown-linux-gnu
```

An explicit `AXOND_FUZZ_TARGET` is used exactly as supplied, so it can select a
deliberately installed target without the automatic musl-to-GNU translation.

`just fuzz` copies `seeds/<target>/` into `corpus/<target>/` first, so a local
run starts where the committed corpus left off.

## When a run finds something

1. `cargo fuzz tmin <target> artifacts/<target>/<crash>` to shrink it.
2. Commit the minimized reproducer to `seeds/<target>/` with a name that says
   what it is. The smoke then covers it on every pull request.
3. Fix the parser. A finding is a bug in the parser, not in the fuzzer, unless
   the assertion itself was wrong — in which case fix the assertion and say why
   in the commit.

Scheduled runs keep their corpus between runs and upload both the corpus and any
artifact, so a finding is reproducible from the workflow run alone.
