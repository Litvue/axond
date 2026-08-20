# Fuzzing

Axond parses input it does not control on five paths: an operator's
configuration file at boot and on every reload, a caller's credential and query
string on every request, everything a *provider* sends back — the SSE stream
relayed to a tenant byte for byte and the failure body an error response is
classified from — the models.dev catalogue a background refresh imports
unattended, and immutable sealed-secret objects hydrated from blob storage. All
five are fuzzed continuously — a scheduled
coverage-guided run for exploration, and a bounded deterministic replay on every
pull request so the result is required CI evidence rather than a dashboard
nobody reads.

The project lives in [`fuzz/`](https://github.com/Litvue/axond/tree/main/fuzz),
which is its own Cargo workspace: the targets need a nightly toolchain and a
sanitizer runtime, so nothing at the repository root builds, lints, or packages
them. [ADR 0052](../adr/0052-fuzzing-the-untrusted-input-parsers.md) records why
the project sits outside the workspace, why the seam is a cfg rather than a Cargo
feature, and what that costs.

## What is fuzzed, and what is asserted

| Target | Parser under test | Reached from |
| --- | --- | --- |
| `config_toml` | `Config::from_toml_str` and the whole validation graph | Boot, `SIGHUP`, and the config-file watcher |
| `token_verify` | `axt1.` JWS decoding, key selection, signature checking, and every claim check behind them | Any request presenting a minted token |
| `credentials_query` | The `namespaces` filter of `GET /v1/credentials/status`, percent-decoding included | Any authenticated caller |
| `sse_decode` | `SseDecoder`: event framing, `data:`/`event:` fields, LF and CRLF delimiters, the buffer limit | Every streamed response, on whatever chunk boundaries the network produced |
| `provider_stream` | The provider stream decoders: OpenAI chat and Responses, Azure AI Foundry, Anthropic translated into OpenAI chunks, and a native Anthropic relay | Every streamed response, once framed |
| `provider_error` | `ProviderError::from_upstream` and `ProviderError::transport`, and the classification, retry, and health judgements built on them | Every non-2xx or failed upstream call |
| `catalog_import` | The models.dev import: decoding, schema validation, normalization, content identity, semantic classification, and admission over the last-known-good catalogue | The scheduled catalogue refresh, unattended |
| `blob_secret_envelope` | The v2 fixed-array canonical-CBOR sealed-secret decoder | Snapshot compilation after an authenticated blob manifest names immutable ciphertext |
| `blob_secret_crypto` | Bounded synthetic v2 seal/open, context substitution, mutation, and rotation | Cryptographic invariants need structured valid objects that raw parser mutations rarely produce |

Every target asserts the same three properties, because they are what the
gateway relies on.

1. **The parser returns.** A panic, an abort, or a hang is the finding. An
   unwind on the request path is a `500` at best and a lost stream at worst; one
   at boot is a replica that will not start.
2. **A refusal is typed.** Each rejection maps to the error the gateway would
   actually answer with — a `ConfigError`, a `400`, or a stable `token_*` code —
   and carries a non-empty operator-facing message.
3. **What is accepted stays in bounds.** Percent-decoding never expands its
   input; an accepted stateless config defines a namespace, while an accepted
   stateful one declares none of the sections the control plane owns; a verified
   token names a declared namespace, carries a subject, and presents no more
   capabilities than the vocabulary defines. Most importantly, a signature from
   one namespace's signer never verifies into another.

The wire targets add the properties a relay depends on:

4. **Nothing grows without a bound.** The SSE decoder never holds more than its
   configured limit, never *retains* a complete event, and never decodes to more
   bytes than it was handed; a stream decoder's output stays within a fixed
   multiple of its input, which rules out re-emitting accumulated state per
   event. Every provider diagnostic is truncated to `MAX_DIAGNOSTIC_BYTES` on a
   character boundary, so a megabyte error body cannot become a megabyte log
   line or response.
5. **A chunk boundary is invisible.** The same body decodes to the same events
   however it is split, including inside a `\r\n\r\n` delimiter or a multi-byte
   character, and a stream that ends mid-event is refused by `finish` rather
   than silently accepted. Pinned valid fixtures are replayed under *every*
   boundary they can be split on, so "decodes to nothing" cannot satisfy the
   relative properties.
6. **Classification is consistent, and diagnostics disclose nothing.**
   `from_upstream` is deterministic, answers with a known code, and agrees with
   itself: a `404` is a missing model that fails over without marking the
   provider unhealthy, a client refusal is never retried, and health is only
   ever a narrower judgement than retryability. A failure names the provider it
   was told about and nothing more — the harness holds a gateway credential and
   an upstream URL as canaries that it passes to nothing, so either of them
   appearing in a rendered error, a `Debug`, or a diagnostic is a finding.

The catalogue target adds the properties that make an unattended import safe to
run against a document a third party publishes:

- **A refused payload cannot replace a good one.** Every fuzzed import runs over
  a last-known-good catalogue that already holds a valid snapshot; whatever the
  payload is, the active content afterwards is either that payload's content or
  exactly what was active before.
- **Identity is content, not spelling.** Object key order, whitespace, the fetch
  time, and the validators are provenance: they are recorded and they never reach
  the content id, so a regenerated upstream document is not an update.
- **Additive drift is tolerated; changed meaning is refused.** A field the schema
  does not define is ignored and changes nothing stored. A field whose meaning
  drifted — an unknown status or modality, a malformed or negative price, a
  duplicated tier, an id that contradicts its key — is a typed refusal naming the
  JSON Pointer an operator can act on, never a value folded onto a default.
- **A price change is classified as a price change.** An edit to a published rate
  classifies as price-only, and an edit to a description, a capability flag, a
  lifecycle status, or a provider-neutral record classifies as metadata-only.
  Confusing the two would mean a spend decision made from a stale rate or a
  cosmetic edit alerting as a price move.
- **Provider overrides stay honest and local.** The overrides an offering
  records are compared with the complete set of differences recomputed against
  the neutral record, so an override the import invented and a provider value it
  quietly dropped both fail, and each pointer must point inside the offering
  that states it. The comparison is calibrated once per run by removing a
  recorded override and requiring it to be noticed.
- **Importing is not publishing.** The routing table a request would be served
  from is read off a real `AppState` — the same snapshot load a request performs,
  on the pointer `AppState::publish` stores into — before and after every import,
  and must be unchanged: a catalogue import records metadata, and nothing in this
  path activates runtime state. The observation is calibrated once per run
  against a separate state that is deliberately published into: if publishing
  stopped moving the table, the check has gone blind and the run fails rather
  than reporting a vacuous pass.
- **A catalogue nothing offers is refused.** An accepted import holds at least
  one model, one provider, and one offering; a document that kept its models and
  lost its providers is a typed `Unoffered` refusal and leaves the held
  catalogue active. The guarantee is that an accepted catalogue is routable
  somewhere, not that every model in it is offered — a single record no provider
  serves is an ordinary upstream state, and refusing the whole document over it
  would let one upstream edit freeze every other model.
- **A refusal is not sized by the payload.** A refusal quotes upstream text only
  in a bounded excerpt — the JSON Pointer beside it is what locates the value
  exactly — so a 64 MiB map key cannot make a refused refresh write 64 MiB into
  the log pipeline on every scheduled retry. The bound is on the count as well
  as on each value: a refusal that lists the models an ambiguous key could mean
  names a few of them and says how many there were. Both the message and the
  pointer are asserted to stay under a fixed ceiling. A payload that is not JSON
  and one whose shape the schema rejects carry no pointer at all, so their bound
  keeps the tail as well as the head: `serde_json` states the position last, and
  a type error repeats the value it rejected, so an upstream that files a
  megabyte where a number belongs is exactly the case whose location an
  unqualified head-only cut would drop.
- **An absent wrapper states nothing rather than something.** A record that
  omits `modalities` or `limit` imports with neither stated — no ceiling, no
  modality, nothing a caller can mistake for an observed answer. A *stated*
  wrapper of the wrong shape is still a refusal, and the fuzz corpus carries
  both shapes so a future default cannot appear unnoticed.

The raw blob-secret target adds the properties required before ciphertext
reaches a key: the complete object is bounded before decoding; only the
six-element schema-2 array and minimal definite CBOR lengths are accepted;
material nonce, RFC 3394 wrapped-DEK, KEK-id, and ciphertext lengths stay exact;
and an accepted object re-encodes byte-for-byte to its input. Coverage-guided
bytes are never decoded or transformed by the harness. Binary corpus seeds pin
acceptance and every refusal class: oversized, truncated, shape, compatibility,
noncanonical, KEK id, fixed field, ciphertext, and trailing data. The bounded
smoke maps every raw seed filename to one exact outcome and fails on an added,
removed, renamed, or reclassified file.

`blob_secret_crypto` complements parser mutation with structured bounded inputs.
Pinned smoke scenarios cover environment, namespace, secret-id, version, KEK-id
and purpose substitution; wrapped-key, nonce and ciphertext mutation; unknown
keys; active/decrypt-only rotation; duplicate-material alias refusal; invalid
UTF-8 after authenticated opening; and the exact multibyte 64 KiB byte boundary.
Only synthetic repeated-byte KEKs enter the seam.
Each scenario is also a committed binary seed using an Axond-owned stable
layout: three one-byte scenario/key selectors, little-endian identity and
version seeds, then the remaining bytes as material. Smoke decodes every file
through `BlobSecretCryptoInput::arbitrary_take_rest` and asserts its exact named
outcome. Separate seeds pin empty material, invalid input UTF-8, and exact/over
multibyte 64 KiB boundaries.

Runs are hermetic: the seam the targets call builds its verifiers from a
configuration compiled into the binary with synthetic key material, so a fuzz run
makes no network call, reads no file, and holds no real secret. The catalogue
target runs the *real* source — conditional fetch, strict parse, admission —
against an in-memory fetcher that serves bytes already in hand and records what
it was asked for, and then asserts that the only thing the import path reached
for was the configured `/catalog.json` URL. No socket, and evidence of it rather
than an assurance. The corpora are public for the same reason.

## The two lanes

**Pull requests — `Fuzz smoke`, required.** Replays every committed seed plus
fixed derivations of it (truncations, single-byte flips, one oversized
repetition), a set of tokens minted at replay time, and a set of catalogue edits
applied at replay time, on the pinned stable toolchain. The catalogue corpus is
documents, which reaches decoding, the schema, and normalization; the semantic
classification needs two catalogues that differ in one stated way, so the smoke
edits the bundled seed at replay time — one edit each, every one of them pinned
by name to the class it must be understood as, and every one of them re-rendered
with its keys rotated so each semantic assertion is a reordering assertion too.
A committed token seed has expired by the time it is replayed, so the
checks *behind* `exp` — audience, lifetime, namespace, signer authority, subject,
`jti`, aliases, scope, issuance epoch — are reached two other ways, both of which
assert the outcome each case exists for by name rather than counting classes:
each token seed is re-signed with its timestamps *translated* onto the run (the
offset that moves `iat` onto now moves `exp` with it, so a seed built to sit past
the lifetime ceiling still does), and a set of claim shapes minting alone cannot
express is signed fresh. Coverage therefore decays with a code change rather than
with the calendar. The issuance epoch the seam declares is anchored to the run
for the same reason: that check runs after the lifetime check, so a token old
enough to precede a *committed* epoch would already have been refused as expired.
It fails on a panic, on an input slower than its per-input budget, on
an allocation past a hard cap the binary enforces through its own global
allocator, and on the corpus no longer reaching a spread of outcome classes —
that last one is what stops the lane from going green by refusing every input at
the door. The whole replay is bounded to under a minute.

**Nightly — `Fuzz`, scheduled and manually dispatchable.** One coverage-guided
`cargo fuzz` job per target with a wall-clock budget, an RSS ceiling, and a
single-allocation ceiling, so an uncontrolled allocation is a finding rather than
an OOM-killed runner. The accumulated corpus is restored at the start and saved
at the end, so each night continues the previous exploration, and both the corpus
and any reproducer are uploaded as artifacts — a scheduled finding is
reproducible from the workflow run alone.

Locally: `just fuzz-smoke` for the required lane (no nightly needed),
`just fuzz <target> [seconds]` for a bounded coverage-guided run.

## When a target finds something

A finding is triaged like any other security-relevant bug, through
[the security policy](../../SECURITY.md) when it is reachable from an untrusted
caller. The fix carries the minimized reproducer into `fuzz/seeds/<target>/`, so
the required smoke covers that exact input from then on — the same
regression-test rule a security fix already follows.

## Scope, and what it is not

Fuzzing covers the parsers above. On the catalogue path, what is fuzzed is
the import: the transport that retrieves the document is covered by the ordinary
suite, not here, because the fuzz seam deliberately replaces it with an in-memory
fetcher. Fuzzing is also
not a substitute for the typed-error and tenant-isolation tests in the ordinary
suite: it explores inputs those tests do not enumerate, and it asserts *fewer*
things about each one.
