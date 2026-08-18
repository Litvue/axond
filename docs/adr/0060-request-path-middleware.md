# 60. A middleware primitive for the request path

Date: 2026-08-15

## Status

Accepted

Opens the extension seam the request path has never had, under the accounting
contract of [ADR 0005](./0005-streaming-relay.md) and
[ADR 0010](./0010-shared-budget-backends-and-charging-policy.md), the
byte-faithfulness guarantees of [ADR 0012](./0012-native-provider-routes.md) and
[ADR 0023](./0023-openai-responses-passthrough.md), and the phase bounds of
[ADR 0028](./0028-transport-phase-bounds.md) and
[ADR 0030](./0030-request-bounds-and-load-shedding.md). Policy delivery reuses
[ADR 0036](./0036-typed-policy-documents-generations-and-transitions.md) and
[ADR 0050](./0050-runtime-policy-activation.md) rather than inventing a second
one.

## Context

Nothing in the gateway could observe or alter the content of a request. Two
initial-scaffold modules in `gateway-core` — `guardrail` and `governance` —
described parts of such a feature and had no call site. This decision wires the
guardrail behavior through the middleware seam and removes the dead governance
module rather than preserving two competing, unowned policy abstractions. The
capability an operator actually asks for is broader than either: redact before a provider
sees a prompt and restore the masking in the answer, refuse a prompt outright,
inject defaults, filter tool definitions, cache semantically. Writing one of them
as a bespoke hook would put the seam question off by exactly one feature.

Half the path is already middleware. `routes::mount` composes admission,
authentication, convergence, and the diagnostic ceiling as `from_fn_with_state`
layers, in an order its comments explain at length. Those layers see bytes and
headers, and sit outside body parsing, so none of them can express a content
policy. Everything that needs the parsed body — alias resolution, per-request
bounds, pricing, the rate-limit permit, the budget hold, the failover walk, the
settlement, the usage record — lives in one function, `serve()`, and is
straight-line code.

The temptation is to declare uniformity: make every stage a middleware, keep one
primitive, and write each capability against it. That is the right instinct about
the primitive and the wrong instinct about the ordering, for five reasons the
existing code makes concrete.

**A stage's resources outlive the stage, and sometimes the handler.** The
admission permit, the rate-limit permit, and the budget reservation are not
scoped to the handler. On a streamed request they are moved into the relay's
`Accounting`, which settles when the *response body* drops — that is how a client
that hangs up mid-stream is charged for the tokens it actually received, and how
a cancelled stream returns its capacity (ADR 0005). Middleware that released its
hold when the inner call returned would release it before a single token had been
relayed.

**Ordering is load-bearing, and parts of it fail closed.** Authentication is the
first externally visible refusal by decision, so an anonymous caller gets `401`
and never a convergence-state `503`
([ADR 0013](./0013-inbound-auth-fails-closed.md),
[ADR 0018](./0018-tier-0-hermetic-boot-gate.md)). Admission is outermost so a
request arriving after the drain window is refused before it touches
authentication or a budget. Per-request bounds and pricing are computed *before*
admission precisely so a request that cannot legally be served does not occupy
capacity while being refused. None of that is arbitrary sequencing that a
configuration file should be free to permute.

**The inner call runs more than once.** `dispatch_with_failover` walks an alias's
targets, and `dispatch_over_pool` rotates credentials within a target. A
middleware wrapping the walk sees one request; a middleware inside it sees
attempts, and would see the same request mutated once per attempt. No single
scope describes both.

**Two routes promise byte-faithfulness.** `/v1/messages` and `/v1/responses`
forward the caller's body verbatim and, when streaming, return the provider's own
bytes untouched — only `Framing::OpenAiSse` re-emits. That is what keeps signed
thinking blocks and tool-use blocks intact through the gateway, and continuation
requests take a pinned-affinity path with no failover. A middleware that rewrites
response content is incompatible with that framing by construction, not by
oversight.

**Accounting is derived from the body.** The token estimate, the per-request
ceilings, the budget hold, and the settled row are all computed from the body a
request arrived with. Mutating the body changes its size, so where mutation
happens relative to the estimate decides whether the hold is honest.

## Decision

One primitive, with declared scopes, a fixed core order, and ownership that can
reach the response body's lifetime.

**The contract is a trait in `gateway-core`; invocation and bounds live in the
gateway crate.** `gateway-core` stays I/O-free: it owns the middleware's types,
its verdicts, and its declaration of what it needs, and knows nothing about
permits, snapshots, or sockets. The gateway crate owns the chain, the ordering,
the bounds, and the mapping from a middleware's refusal to a typed
`GatewayError`. A middleware therefore cannot reach the network, the clock, or a
credential except through what it is handed.

**Scopes are declared, not inferred.** The contract reserves `Request` (once,
on the parsed body, before the failover walk), `Response` (once, on a buffered
`ProviderResponse`), and `StreamEvent` (per decoded data event, followed by one
payload-free successful-stream finalizer). All three scopes are operational.
`Attempt` scope — inside the failover walk, per target — is deliberately **not**
in this decision: a body mutated
differently per attempt would make any request-scoped state, a redaction map
above all, disagree with the attempt that served. Adding it later is an additive
change to a registration enum; getting it wrong now would be a correctness bug
in the walk.

Request middleware cannot weaken the admission byte ceiling. The router refuses
an oversized wire body before buffering, and the request path measures the
serialized post-middleware body against the same `admission.max_request_bytes`
limit before provider dispatch. Token, output, caller-cost, and budget checks
likewise use the post-middleware body.

**A middleware may own state for as long as the response lives.** The primitive
lets a `Request`-scope middleware return state that is moved into the response's
ownership: dropped at scope end on a buffered request, and moved into the relay's
`Accounting` on a streamed one, so it is released by the same drop that settles
the spend. This is the element that makes the uniform primitive viable at all —
without it, the three stages most worth migrating (rate limit, admission, budget)
structurally cannot be middleware, and a redact/unredact pair cannot survive its
own stream. It is in the primitive from the first commit for that reason, not as
a later capability.

**Content middleware runs after the permits and before the estimate, and
everything derived from the estimate is derived from the mutated body.** The
cheap pre-admission check on the arriving body is kept as a fail-fast, the chain
runs once the admission and rate-limit permits are held, and then the whole
estimate-derived group is recomputed as the authoritative one: the token
estimate, the `max_prompt_tokens` and `max_output_tokens` ceilings, the caller's
`max_request_microdollars` per-request cost ceiling, and the amount the budget
reservation is taken for. Today all of these come from a single `estimate`
computed once, well before admission, so this splits that computation in two —
and moving only the token bounds would leave a middleware that grows a body able
to walk a request past the cost ceiling its caller was given and take a hold
priced for a body that was never sent. Running the chain before the permits would
make a middleware — which may be expensive — into an amplification surface
reachable by anything that authenticates.

**Core order is compile-time; only content middleware is configurable.** The
stages that fail closed compose in source, in the order `routes::mount` and
`serve()` already establish, and no policy document can permute them: an
authentication layer whose position is configuration is an authentication layer
one malformed document away from being absent. Content middleware is registered
per namespace through the typed policy documents of ADR 0036, activated by the
generations and epochs of ADR 0050 — which is what gives a guardrail policy
namespace scoping, versioning, rollback, and hot reload without a single new
delivery mechanism.

| Registration path | Stages |
| --- | --- |
| Compiled in source | admission, authentication, convergence, request bounds, pricing, rate limit, budget/accounting, provider failover, settlement |
| Declarative policy | compiled in-process content middleware selected by `content_middleware` only |

Policy can select and order only the second row. Core-stage identifiers and
fields such as `core_stages` are rejected while the document is validated.

**Response mutation follows the phase the request actually executes.** A
buffered provider response runs `Response` scopes once in reverse registration
order. A stream runs `StreamEvent` scopes on decoded data events, also in reverse
order; terminal provider usage remains gateway-owned and is never mutable.
Responses completion requires matching SSE/data discriminators, a response
object, and `status: "completed"`; Native Messages completion requires one
ordered `message_start`, balanced content-block lifecycles, one or more valid
`message_delta` events including a concrete terminal stop reason, and then
`message_stop`. Matched-discriminator extension
events remain byte-faithful but cannot advance lifecycle state. Native block
identity binds `text_delta` declassification to a declared `text` block;
`citations_delta` remains valid on that block but never enters the text
restoration path. A thinking block must receive exactly one terminal
`signature_delta` before it closes, with no later block delta. After that
semantic terminal event, strict SSE/UTF-8 and
provider-decoder EOF checks run, then the same scopes receive one payload-free
finalizer in reverse order. Only a successful finalizer permits reconstructed or
validated buffered bytes and the normal terminal marker to be released.
Truncation, malformed or post-terminal data, transport failure, timeout,
cancellation, and client hangup never masquerade as successful finalization. A
finalizer failure emits the route's
`middleware_stream_error`; incrementally re-emitted deltas already delivered stay
delivered, but the error precedes `[DONE]`.
`Response` scope alone is therefore intentionally inactive for `stream: true`;
snapshot compilation warns once with the namespace and middleware id for every
such registration so an operator cannot mistake phase selection for streaming
coverage. A guardrail intended to cover both shapes must declare both scopes.
OpenAI chat already re-emits decoded events, so applicable stream middleware can
run incrementally. `Native` and `Responses` framing remain byte-faithful unless
the governing policy explicitly selects that route in
`buffered_response_routes`; without that opt-in any applicable stream-event
middleware is refused as `middleware_response_incompatible` before permits,
reservations, or provider dispatch. This includes non-mutating middleware,
because it can still refuse and fail-closed policy cannot release bytes before
that verdict.

The opt-in holds reconstructed output until the upstream terminates
successfully, then releases the transformed events. A non-mutating stream chain
instead releases the provider's original bytes after validation, preserving the
byte-faithful contract. That verdict is over the gateway's parsed SSE/JSON view,
not over each lexical byte spelling: strict parsing applies SSE's standard
one-optional-space and multi-line `data:` normalization, refuses fields that the
callback cannot see, and rejects duplicate JSON object keys before invoking the
chain. Client SDKs remain responsible for applying standard SSE and JSON parsing
to the released provider bytes. Refusing unseen fields deliberately includes SSE
comments, blank heartbeat blocks, `id:`, and `retry:`; operators must qualify a
provider/proxy's exact stream before enabling validated byte-faithful passthrough.
Both raw upstream bytes and reconstructed output are bounded by the lower of
`admission.max_stream_bytes` and a 64 MiB hard ceiling; the hard ceiling remains
when the ordinary stream limit is disabled. Middleware failure releases no held
content and ends the already-open stream with a wire-native
`middleware_stream_error`. This buffering cost is never implicit: provider TTFT,
caller-visible TTFT, and gateway-added buffering duration are separate metrics.

**Every middleware declares a failure posture and runs under bounds.**
Fail-closed or fail-open is part of the registration, because a guardrail that
fails open is not a guardrail and a cache that fails closed is an outage. Each
request invocation runs in a blocking task against a private request copy under
an asynchronous timeout. The declared bound is end to end: it includes waiting
for middleware capacity, blocking-executor scheduling, and execution. The
gateway stops waiting at that deadline and applies the failure posture; a late
task cannot mutate the request that proceeds to routing. Because Rust cannot
preempt synchronous code safely, the timed-out task may continue until it
returns. A global semaphore remains owned by that task and bounds such work to
64 blocking invocations, while a second semaphore limits each middleware id to
four. Healthy callers that encounter either bound wait for a permit within their
remaining declared deadline; only expiry applies the middleware's failure
posture. If an invocation actually outlives its deadline, only that middleware
id is quarantined while the abandoned closure is still running. Its later calls
apply its posture immediately, other ids continue to run, and the quarantine
clears when the closure returns. Rust cannot prove or force termination of the
closure, so its permits remain held for its real lifetime. This keeps ordinary
concurrency above 64 from becoming an immediate refusal while preventing one
pathological implementation from consuming more than four blocking workers. A refusal inherits the existing
refusal discipline: a typed error, a stable caller-facing reason that never
echoes the body, and no usage event when nothing reached a provider.

Stream-event scope invokes one synchronous callback for every decoded data
event plus one successful-stream finalizer. Events and finalization within one
stream remain serial so state and wire order cannot race, while independent
streams use the bounded blocking capacity concurrently; there is no replica-wide
stream-event lock. The finalizer uses the same process and per-id semaphores,
declared timeout, cancellation guard, quarantine, stranded-state rules, and
failure posture as an event callback, and is at most once even if terminal phases
are polled repeatedly. This deliberately pays one bounded dispatch per event
instead of batching events and changing mutation or refusal boundaries. Before
activating a stream-event implementation, operators qualify its worst-case event
rate and concurrent streams against the 64 replica slots, four per-id slots, and
its declared end-to-end duration. The deterministic capacity test exercises
concurrent streams through a saturated runtime and proves queued events drain
without exceeding either ownership bound.

Operators distinguish a slow implementation from provider latency through
`axond.middleware.capacity_wait` and
`axond.middleware.capacity_timeouts`. A sustained timeout increase means late
synchronous invocations are retaining the bounded slots. A warning whose detail
names a quarantined middleware id means its earlier invocation is still running;
other ids remain operational. Repair the implementation, and restart the replica
only when the abandoned call does not return. It is not an upstream slowdown.

**Three things are excluded from v1, on purpose.** A middleware may not call a
model or spend money: a classifier call would be a second chargeable event inside
a request that mints exactly one usage identity, and that attribution deserves its
own decision rather than a corner of this one. No out-of-process callout: the
extension mechanism worth proving first is the in-process one, and a network hop
in the request path needs the failure posture above to already be real. No WASM
host: a sandbox, its fuel and memory bounds, and its supply-chain surface are a
larger dependency than this seam should carry to be useful.

**Migration is inward, and it is ordered by risk.** The primitive lands with the
new content stages only, and redact/unredact is the first middleware written
against it — including its multi-turn problem, which is solved by deterministic
keyed placeholders (`HMAC(namespace_key, secret)` truncated into a stable token)
rather than a session-scoped mapping table, so a client echoing masked text back
on the next turn re-masks identically and the gateway stays stateless by default
([ADR 0002](./0002-stateless-by-default-stateful-by-opt-in.md)). This stability is
an intentional disclosure tradeoff: providers see the tokens, equal values in one
namespace produce equal tokens, and a same-namespace chosen-plaintext oracle can
identify low-entropy values. Namespace-derived keys prevent comparison across
namespaces. Operators must use separate namespaces where callers must not share
equality and must not treat redaction as protection against guessing low-entropy
secrets. The rate-limit permit and budget hold then move into the fixed core
chain: asynchronous acquire, reserve, settle, and release stay gateway-owned,
while `MiddlewareExecution` owns their handles beside content state and follows
them to the response body's drop boundary. The rollback setting,
`[core_middleware] accounting = "legacy"`, retains the previous straight-line
owners during qualification; both paths keep the same fixed ordering and caller
contract. Authentication does **not** migrate:
[ADR 0061](./0061-authentication-remains-an-outer-boundary.md) records the
completed decision to retain it as a compiled outer HTTP boundary. It has no
response-lifetime hold, needs headers and route capability before body parsing,
and must remain ahead of convergence and every configurable policy. The
asymmetry is intentional rather than unfinished migration work.

### State tier

Tier 0 (config-only). The primitive is in-process, the chain is built from the
snapshot a request already holds, and no scope invocation performs I/O in v1 —
which is what the exclusion of callouts and model-calling middleware buys. Policy
documents that register content middleware are delivered by the mechanism ADR
0036 and ADR 0050 already define and are read at whatever tier that deployment
already runs; a deployment that registers no middleware is unchanged and its tier
is not raised.

Policy also carries an optional normalized `buffered_response_routes` set. Its
closed members are `messages` and `responses`; absence is the compatible empty
set, and duplicates or unknown routes are refused. The setting is permission for
an applicable response-mutating chain to trade native byte-faithful passthrough
for bounded buffering. It neither activates middleware nor changes a request's
upstream streaming or affinity semantics by itself. Changes are live and bind to
new requests through the immutable snapshot.

Adding this field changes `PolicyContent` for every document, including one whose
set is empty. That digest is deliberately process-local activation and drain
identity: it is neither serialized into a Redis/Postgres key nor compared across
replicas. Shared budget and rate-limit keys remain namespace/subject plus their
reservation or lease identity, while each replica enters and exits holds under
the generation it computed. A mixed-version fleet can therefore compute different
digests for the same old document without splitting shared enforcement state;
after rollout, every new process computes the new identity.

## Consequences

The request path gains a place where content policy belongs. The old inbound-only
guardrail scaffold becomes the first production middleware, `axond.redact`:
block rules refuse before dispatch, while redaction rules replace matches with
deterministic keyed placeholders and keep the reversible mapping only in the
request's response-lifetime state. Buffered and stream-event scopes restore only
placeholders generated by that request, including placeholders split across
decoded display-text events. Restoration is route-aware and allowlisted to the
documented Chat, Messages, and Responses display-text fields. Structured tool
arguments, URL fields, names, identifiers, metadata, and protocol controls never
receive declassification. Display text itself is an explicit trust boundary: a
provider can place a token inside Markdown, HTML, a URL-looking substring, or an
instruction in an otherwise valid display string, and Axond restores it there.
Callers must treat restored display text as untrusted model output and must not
auto-fetch, execute, or render it in a privileged context. Preserving useful
round-trip prose while preventing every semantic relocation inside prose is not
possible at this JSON boundary; deployments that cannot accept that boundary
must use block-only policy or disable redaction. Carry is keyed by the provider's
semantic output identity plus typed structural paths, so distinct choices,
content blocks, or Responses items cannot combine fragments and object key `"0"`
cannot alias array index `0`. At successful
finalization, a strict prefix of a concrete generated placeholder fails closed;
unrelated AXOND-like text is preserved. Errors and cancellation discard carry
rather than flushing it, and dropping the response-lifetime owner explicitly
zeroizes originals and carry. Request-lifetime redaction state fails closed
above 4,096 distinct original values. Stream restoration likewise retains at
most 4,096 carry channels, 1 MiB of aggregate carry-key identity, and 64 KiB of
aggregate carry prefixes; these fixed ceilings bound provider-controlled
channel cardinality and identity length independently of the rendered-byte
limit.

The gateway supplies this trusted identity through `MiddlewareSurface` and
`Middleware::apply_for_surface`; `DeterministicGuardrail` rejects an invocation
whose surface is absent rather than inferring one from provider-controlled JSON.

Root `model` and `stream` remain routing-owned and are excluded from mutation.
Direct rewriting is limited to route-specific prompt-bearing fields such as
message content, instructions, input text, descriptions, and serialized tool
arguments. Every matching string outside that allowlist—including roles, types,
reasoning controls, tool names, identifiers, locators, metadata, and future
fields whose semantics are unknown—refuses atomically instead of silently
changing provider protocol behavior. Other caller-controlled channels that
reach the provider but cannot safely be rewritten—JSON member names, root
`previous_response_id`, and forwarded native wire headers—follow the same
refusal rule. Protected header/continuation fragments are also checked against
each other and the untouched body, in both directions, so moving part of a match
outside JSON cannot bypass policy. Each route canonicalizes both the complete
provider-wire string sequence and its semantic prompt-text sequence before
mutation. The first keeps
URL, file/media, and protocol-control strings in scope; the second keeps text on
either side of non-text media adjacent. A match crossing either sequence refuses
rather than allowing structural splitting to bypass policy. Identically named
nested routing fields are content and remain covered. Malformed `stream` and
`previous_response_id` controls are rejected before middleware or provider
dispatch.

This deliberately removes the exported `Governance` scaffold and the legacy
inbound-only `Guardrail`/`RegexGuardrail` API instead of leaving unused policy
engines beside the production path. Downstream `gateway-core` users must move
content inspection to `DeterministicGuardrail` through the `Middleware` contract
and keep admission/rate-limit governance in the gateway enforcement layer. That
is an intentional pre-1.0 Rust API break: it requires the reviewed compatibility
override and a 0.4.0 minor release, not a 0.3.x patch release.

`axond.redact` is valid only with `FailClosed`; both administrative publication
and snapshot compilation reject any other posture. Durable policy stores regexes
and a versioned key reference, never key material. A replica derives the
namespace key before compiling the chain, making placeholders stable for the
same secret and namespace while separating namespaces. Missing or malformed key references,
invalid or empty-matching regexes, and non-fail-closed declarations preserve the
last-known-good serving snapshot by failing compilation. The encrypted compiled
cache binds that reference to a non-secret fingerprint of the resolved namespace
key; changing a referenced environment value in place refuses cold recovery.
Rotation therefore publishes a new policy naming a new environment-variable
reference, which makes the identity change explicit in the revision.
The compiled-serving payload moves to layout v4 and is not restored across the
layout boundary in either direction. During the v0.4.0 rollout, every new
replica must reach the control plane and SecretStore once, compile the admitted
revision, and write its v4 cache before its PVC is qualified for outage recovery.

The `serve()` function grows a chain invocation and a second bounds check, and
the token estimate stops being derivable from the request as it arrived on the
wire. Anything reasoning about a usage row's input tokens is reasoning about the
post-middleware body, which is the right answer — that is what was sent — but it
is a new thing to know when reading a row against a captured request.

The fixed rate-limit and budget stages use the same `MiddlewareExecution` owner
without implementing the synchronous, I/O-free `gateway-core::Middleware`
trait. Their backend calls remain asynchronous gateway responsibilities at
compile-time positions: rate limit before content, budget after authoritative
post-content estimation, and settlement before terminal usage persistence. The
owner releases a permit synchronously and detaches an unsettled budget release
on every cancellation/drop path. The temporary `legacy` gate keeps the previous
guards available without a binary rollback and is required to prove equivalent
refusals and charges rather than merely make rollback plausible.

Response-mutating middleware on native framing is refused unless the route is
present in `buffered_response_routes`. The refusal is typed and explicit rather
than a quiet no-op, which turns a subtle policy gap into a visible error at the
cost of an error an operator must then understand. An empty chain remains
byte-faithful even when the route is selected. The immutable serving snapshot
pins both middleware and buffering selection for the request; opting in does not
alter Responses target or credential affinity, including continuations.

Excluding `Attempt` scope means a middleware cannot shape a body per provider,
which is the natural place some future compatibility shim would want to live. That
work stays in the provider adapters where it is today.

Excluding money-spending middleware rules out, for now, exactly the class of
guardrail most people mean by the word — an LLM judge on the prompt. Regex and
declarative policy cover secrets, patterns, and structural rules; semantic
classification waits for the usage-attribution decision.

Keeping the core order in source means the uniformity is internal rather than
operator-visible: two registration paths exist, one compiled and one declarative,
and a reader has to know which stages are which. The alternative — one
reconfigurable chain — was rejected because its failure mode is an unauthenticated
request path, and no ergonomic gain pays for that.

Authentication remains outside the parsed-content primitive by the explicit
decision in ADR 0061. The outer Axum layer establishes identity from headers and
route capability; the inner primitive operates only after that identity is in
the request extensions. This is the final migration boundary for this ADR.
