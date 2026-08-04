# 1. Record architecture decisions

Date: 2026-08-03

## Status

Accepted

## Context

Axond is a greenfield extraction of a provider-proxy that previously lived
inside a private monorepo. Several load-bearing decisions (passthrough vs.
canonical schema, the usage/quota split, namespaced credentials) are expensive
to reverse once they land in callers' code and customers' data tables. As an
open-source project, the reasoning needs to be legible to outside contributors,
not just the original authors.

## Decision

We record architecture decisions as short markdown files in `docs/adr`,
numbered sequentially, following Michael Nygard's ADR format. An ADR captures
the context and consequences of a decision, not just the decision.

## Consequences

- New significant decisions get an ADR in the same PR that implements them.
- Superseded ADRs are marked as such and link to their replacement rather than
  being deleted, preserving the history of why the project is shaped as it is.
