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

Nothing in the gateway can observe or alter the content of a request. Two modules
in `gateway-core` — `guardrail` and `governance` — describe exactly such a
feature, are exported from the crate root, and have no call site anywhere in the
gateway. They date from the initial scaffold and were never wired. The capability
an operator actually asks for is broader than either: redact before a provider
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
`ProviderResponse`), and `StreamEvent` (per relayed event). The first runtime
slice invokes `Request` only. Registration rejects `Response` and `StreamEvent`
instead of accepting an inert declaration; those scopes become activatable only
when their invocation paths land. `Attempt` scope — inside the failover walk,
per target — is deliberately **not** in this decision: a body mutated
differently per attempt would make any request-scoped state, a redaction map
above all, disagree with the attempt that served. Adding it later is an additive
change to a registration enum; getting it wrong now would be a correctness bug
in the walk.

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

**Response mutation is refused until its invocation and framing contract are
implemented.** The first runtime slice rejects every `Response` and
`StreamEvent` registration. The follow-up may activate response mutation where
the relay already re-emits (`Framing::OpenAiSse`). On `Native` and `Responses`
framing it must remain refused with a typed incompatibility unless policy opts
that route into buffering, because silently re-emitting those streams would
revoke a documented guarantee for every caller. Buffering costs the
time-to-first-token the relay exists to protect — an operator's trade to make
explicitly, per policy, never a default.

**Every middleware declares a failure posture and runs under bounds.**
Fail-closed or fail-open is part of the registration, because a guardrail that
fails open is not a guardrail and a cache that fails closed is an outage. Each
request invocation runs in a blocking task against a private request copy under
an asynchronous timeout. The gateway stops waiting at the declared bound and
applies the failure posture; a late task cannot mutate the request that proceeds
to routing. A refusal inherits the existing refusal discipline: a typed error,
a stable caller-facing reason that never echoes the body, and no usage event
when nothing reached a provider.

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
([ADR 0002](./0002-stateless-by-default-stateful-by-opt-in.md)). The rate-limit
permit and budget hold move into the chain next, once the response-lifetime
ownership has been exercised by real traffic. Authentication moves last, or not at
all: it is the highest-risk and lowest-payoff member of the set, and uniformity is
not worth purchasing with a subtle regression in the one stage that must fail
closed.

### State tier

Tier 0 (config-only). The primitive is in-process, the chain is built from the
snapshot a request already holds, and no scope invocation performs I/O in v1 —
which is what the exclusion of callouts and model-calling middleware buys. Policy
documents that register content middleware are delivered by the mechanism ADR
0036 and ADR 0050 already define and are read at whatever tier that deployment
already runs; a deployment that registers no middleware is unchanged and its tier
is not raised.

## Consequences

The request path gains a place where content policy belongs, and the two dead
modules in `gateway-core` become either the first middleware or deleted code —
`RegexGuardrail` is a reasonable inbound matcher and has no notion of the response
half, so it is a starting point and not the design.

The `serve()` function grows a chain invocation and a second bounds check, and
the token estimate stops being derivable from the request as it arrived on the
wire. Anything reasoning about a usage row's input tokens is reasoning about the
post-middleware body, which is the right answer — that is what was sent — but it
is a new thing to know when reading a row against a captured request.

Refusing response-mutating middleware on native framing means a tenant can
configure a policy that is silently inert on `/v1/messages` unless it opts into
buffering. The refusal is typed and explicit rather than a quiet no-op, which
turns a subtle policy gap into a visible error at the cost of an error an operator
must then understand.

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
