# Fuzzing

Axond parses input it does not control on three paths: an operator's
configuration file at boot and on every reload, a caller's credential and query
string on every request, and everything a *provider* sends back — the SSE stream
relayed to a tenant byte for byte, and the failure body an error response is
classified from. All three are fuzzed continuously — a scheduled
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

Runs are hermetic: the seam the targets call builds its verifiers from a
configuration compiled into the binary with synthetic key material, so a fuzz run
makes no network call, reads no file, and holds no real secret. The corpora are
public for the same reason.

## The two lanes

**Pull requests — `Fuzz smoke`, required.** Replays every committed seed plus
fixed derivations of it (truncations, single-byte flips, one oversized
repetition) and a set of tokens minted at replay time, on the pinned stable
toolchain. A committed token seed has expired by the time it is replayed, so the
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

Fuzzing covers the parsers above. Catalogue imports are named in the umbrella
program (issue #159) and are not covered yet; they need the same treatment
through the same seam. Fuzzing is also
not a substitute for the typed-error and tenant-isolation tests in the ordinary
suite: it explores inputs those tests do not enumerate, and it asserts *fewer*
things about each one.
