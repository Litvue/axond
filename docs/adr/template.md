# Title

Date: YYYY-MM-DD

## Status

Proposed

## Context

What problem or constraint motivates this decision?

## Decision

What is the decision, and what are its boundaries?

### State tier

Declare the state tier this feature requires: Tier 0 (config-only), Tier 1
(shared hot coordination, currently Redis), or Tier 2 (durable external state,
currently object storage or Postgres). State which implementation is selected
at each lower tier, including the Tier 0 default where applicable. Confirm that
this decision does not raise the tier of an existing deployment.

## Consequences

What becomes easier or harder as a result? Include operational costs,
availability coupling, migrations, and failure behavior where relevant.
