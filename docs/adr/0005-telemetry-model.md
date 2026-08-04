# 5. Telemetry model: span shape, propagation, metrics, and a default-off exporter

Date: 2026-08-04

## Status

Accepted

## Context

A gateway is the one hop that sees every model call an organization makes, so its
telemetry is the product, not decoration: without it, a caller cannot answer "why
was this request slow", "which target served it", or "what did it cost". The
sibling services (`actord`, `custodian`) already export OTLP with
`service.name` set per service, so a caller tracing a request through Litvue's
stack expects axond to appear in the same trace rather than to end it.

Three forces pull against each other:

- **Completeness.** Traces, metrics, and logs, all dimensioned well enough to
  slice by tenant, alias, and target.
- **The stateless default.** ADR 0002 says the binary boots with zero external
  dependencies. A telemetry stack that assumes a collector would break that.
- **Merge surface.** Streaming, ordered failover, and the native routes are all
  landing in `routes.rs` concurrently. Instrumentation scattered across handlers
  would collide with all three.

## Decision

**One server span per request, one child span per upstream attempt.** The server
span (`http.server.request`) is created by a tower layer and carries what the
caller asked for and how the request ended: namespace, subject, requested alias,
resolved target, `credential_source`, status, retry count, tokens, cost, latency,
and TTFT. Each attempt (`axond.upstream.attempt`) carries where it was sent and
how that one call went. Ordered failover therefore adds *attempts*, not a new
span shape: N children under one server span, the last one holding the status the
caller saw. TTFT is recorded on both; for a non-streamed response the first token
arrives with the last, so it equals the attempt latency, and the streaming relay
reports the real first chunk.

**Instrumentation lives in a middleware layer plus one seam in the route.** The
layer owns span creation, inbound context extraction, and the coarse HTTP
metrics. The route contributes exactly two things: it opens the attempt span
around dispatch, and it hands the canonical `UsageRecord` to telemetry at the
point it already emits it. Sinks, budgets, spans, and metrics are therefore all
derived from *one* record per request and cannot disagree.

**Propagation is W3C, both directions.** Inbound `traceparent`/`tracestate` are
extracted and become the parent of the server span, so the gateway joins the
caller's trace rather than starting a new one. The transport injects the current
context into the upstream request, so the trace continues past the gateway. The
request id in the usage record is the trace id when a trace is being recorded, so
a usage row, a log line, and a span all name the same request.

**Two metric families, on purpose.** `axond.http.*` covers every HTTP request —
including ones that never reach a provider, like `unknown_model` — dimensioned
only by method/route/status. `axond.request.*` and `axond.upstream.*` carry the
gateway's own dimensions (namespace, alias, target provider/model,
`credential_source`, status) and cover count, latency, TTFT, input/output tokens,
cost in micro-dollars, upstream errors, and per-target circuit state.

**Telemetry is off unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set.** With no
endpoint, no tracer or meter provider and no propagator are installed, so the
OpenTelemetry globals stay no-ops; metric instruments are never built, and the
instrumentation checks a single atomic before doing propagation work. Logs are
still emitted as JSON on stdout, which is the no-datastore default the rest of
the gateway follows. `service.name` is fixed to `axond`.

**Export is OTLP/HTTP over the gateway's own `reqwest` client.** An explicit
`OTEL_EXPORTER_OTLP_PROTOCOL=grpc` is rejected at boot rather than silently
exporting nowhere, consistent with "fail at boot, not at request time". The
exporter is handed the same pooled client the gateway uses, because the
alternative — the exporter's bundled client — pulls a second `reqwest` major
version and a second TLS provider into a binary whose selling point is being one
static file.

**Nothing sensitive is recorded.** Spans and metrics carry identifiers, counts,
and durations. Prompts, completions, and credentials never appear; a per-tenant
debug mode that captures bodies would be a separate, explicitly opt-in decision.

## Consequences

- Failover (#3) adds attempt spans by calling the existing attempt-span helper in
  its retry loop, and publishes breaker transitions through the circuit-state
  gauge defined here; the streaming relay (#2) records the real TTFT at its first
  chunk. Neither needs to change the span shape.
- Metric cardinality is bounded by config (namespaces × aliases × targets) plus
  subject, which is deliberately *not* a metric dimension — it stays on spans and
  usage records, where high cardinality is affordable.
- The default posture is deliberately unobservable beyond stdout logs. Operators
  who want traces must run a collector; that is a conscious trade for an
  operational floor of zero.
- Boxing the OTel layer costs one dynamic dispatch per span when export is
  enabled, which is the price of keeping a single subscriber-construction path.
