# 59. Effective-dated pricing activation

Date: 2026-08-13

## Status

Accepted

Closes [issue #329](https://github.com/Litvue/axond/issues/329), extending
[ADR 0046](./0046-approved-price-books.md).

## Context

ADR 0046 makes price-book rules half-open and records the interval over which a
compiled `PricingSnapshot` is valid. Compilation resolves the book once, so a
request can hold one immutable routing and pricing snapshot through settlement.
That leaves one control-plane seam: a replica that is already serving a
future-dated rule must publish the next resolution when its interval ends, even
when the control plane is idle and no configuration revision changes.

Waiting for an inference request to notice the date would put mutable time or a
control-plane read on the request path. Waiting for a PostgreSQL notification
would miss the boundary when no revision is published. Recompiling on every
poll would preserve correctness but create unnecessary generations and reset
request-protection state for no change.

## Decision

The reconciler owns a pricing boundary schedule. After every successful
publication it reads `PricingSnapshot::effective().ends()` and arms a
control-plane timer for that instant. The timer wakes at the boundary and asks
the existing force-refresh path to compile, admit, and publish the current
durable revision. Requests continue to load one `ConfigSnapshot`; they do not
read the schedule, clock, price book, or stateful backend.

The interval remains half-open. A rule ending at `t` is not active at `t`, and a
rule beginning at `t` is active at `t`; the compiler resolves the new snapshot
against the exact wall-clock instant after the timer wakes. A bounded override
therefore restores its baseline at its `effective_until` boundary, or leaves a
target unpriced when no approved rule covers it.

The timer is a wake-up hint, not the pricing decision. The reconciler checks the
wall clock again after the monotonic timer fires and also checks it on its normal
poll wake-up. A backwards clock adjustment cannot activate a future rate early.
A clock before the Unix epoch or outside the representable timeline is reported
as a clock scheduling problem and is not clamped into a made-up pricing instant;
normal convergence polling remains the recovery path. A forward adjustment is
noticed by the next timer or poll, and the compiler still resolves against the
current instant rather than the timer's originally calculated instant.

The schedule is replaced only after publication succeeds. If compilation,
policy admission, secret resolution, or another candidate stage refuses the
boundary refresh, the old snapshot and its pricing remain active. The existing
refresh-pending flag and bounded backoff retry the same durable revision; a
failed boundary does not become a zero-delay timer loop or silently change
rates.

Restart recovery is ordinary bootstrap. The new reconciler compiles the durable
revision against its current wall clock and arms the same next-boundary schedule
from the resulting snapshot. No timer state is persisted separately, so there
is no second source of truth to reconcile with the price book.

The convergence telemetry trigger vocabulary includes `pricing-boundary`, so an
operator can distinguish scheduled pricing activation from ordinary polling and
revision notifications without adding a high-cardinality label.

## Consequences

Effective-dated pricing now advances while the control plane is idle, with no
request-path I/O or time lookup. An in-flight request keeps the old snapshot and
therefore the old rate through settlement, while requests begun after the
successful boundary publication see the new rate.

The timer is process-local and intentionally not durable. A restart may publish
the currently effective rule immediately and reconstruct the next boundary from
the durable book, which is equivalent to replaying the schedule and avoids
storing derived timer state. A prolonged process pause or clock problem is
visible as pricing convergence lag and is retried through the same control-plane
observability path as other refused candidates.
