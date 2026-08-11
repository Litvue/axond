# 22. Provider cache usage normalization

Date: 2026-08-11

## Status

Accepted

## Context

Provider usage counters do not share one convention. OpenAI's cached prompt
counter is a subset of its inclusive prompt total, while Anthropic reports
cache-read and ordinary input tokens as disjoint counters. Pricing and
reporting need one provider-independent vocabulary.

## Decision

Normalize counters at the provider parser, where the source convention is
known. `ModelUsage::input_tokens` is the non-cached prompt remainder and
`cache_read_tokens` is disjoint from it for every provider. This lets
`cost_microdollars` apply one additive rule without double-counting cached
OpenAI input, while usage sinks and telemetry retain the cache counters needed
to reconstruct the provider total. The parser infers the convention from the
provider-specific key shape; an upstream that mixes conventions, such as an
inclusive `prompt_tokens` with Anthropic-style `cache_read_input_tokens`, is
indistinguishable from the payload alone. Onboarding such a provider requires
an explicit per-provider mapping rather than a heuristic.

`reasoning_tokens` remains normalized at pricing time rather than being
persisted in the usage record: the record excludes cached tokens from
`input_tokens` while including reasoning tokens in `output_tokens`. That
deliberate asymmetry is a known follow-up and must not be mistaken for a
provider-total convention.

The Anthropic-to-OpenAI response translation emits an inclusive
`prompt_tokens` value, adding the disjoint cache-read count back for clients
that consume the OpenAI-shaped response.

### State tier

Normalization is Tier 0: it is parse-time, in-process accounting with no
runtime datastore. However, the `UsageRecord::SCHEMA_VERSION` bump to 2 means
Tier 2 deployments must apply the version 2 usage DDL before writing rows under
the new `input_tokens` meaning.

## Consequences

Cached OpenAI traffic is billed at the cache-read rate rather than charging the
cached subset again at the regular input rate. `input_tokens` now means the
non-cached prompt remainder, with cache-read and cache-write counters retained
separately in usage records and telemetry. Existing Tier 2 installations must
apply the version 2 DDL before enabling writers that emit the new schema.
