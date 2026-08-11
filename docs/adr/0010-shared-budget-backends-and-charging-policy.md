# 10. Shared budget backends, held reservations, and charging what was consumed

Date: 2026-08-04

## Status

Accepted

## Context

ADR 0002 put spend caps behind a `BudgetStore` trait with `NoBudget` as the
default, and named the two things that would have to land for the trait to be
worth anything: a **shared** backend, and a real **reserve-then-reconcile**
lifecycle. Until now neither existed. The scaffold shipped `InMemoryBudget`,
which counts only *settled* spend in one process, so:

- A replica set enforced one cap per replica. Ten replicas with a `$100` cap
  enforced `$1000`, silently.
- Reservations were not held. A request's estimate vanished the moment `reserve`
  returned, so a hundred concurrent requests could each be admitted against
  budget only one of them would leave.
- A failed or cancelled request committed `$0`. The tokens it burned upstream
  were real, invoiced by the provider, and invisible to the cap.

The last one is not a rounding error: streaming is the dominant surface for
agents, cancellation is normal (a user stops a generation, a supervisor kills a
subagent), and a `$0`-on-cancel policy is an unmetered path through a paid
gateway.

Two constraints shape the answer. Budgets are on the **request path**, so their
store must be fast and fresh — the same reason ADR 0002 rejected a usage-style
eventually-consistent store for admission. And a datastore may never be dragged
onto the default path: the binary must still boot and serve with nothing
configured.

## Decision

**Redis and Postgres, both opt-in, behind one `[budget]` section.** With no
`[budget]` the store is `NoBudget` and behaviour is bit-for-bit what it was.
`backend = "in-memory"` keeps the per-replica ceiling for single-replica
deployments. `redis` and `postgres` keep the state in one place, so a fleet
enforces one cap. Both connect at boot, so a wrong DSN or an unreachable store
refuses to start rather than denying every request once traffic arrives —
fail-at-boot, as everywhere else in the config graph.

Postgres is offered because `tokio-postgres` is already in the tree for the usage
sink (ADR 0009) and many adopters already run one; Redis is offered because a
budget check is a hot-path counter operation and that is what Redis is for.

**The trait now owns a reservation.** `reserve` returns an `Admission` carrying a
`Reservation { id, estimate_microdollars }`, and `settle(key, reservation, actual)`
replaces `commit(key, actual)`. A hold *counts against the cap while it is
outstanding*, which is what stops concurrent in-flight requests from collectively
overshooting; settlement releases the hold and adds the measured spend in the same
operation, so a settlement can never charge without releasing or release without
charging. `release` is `settle(..., 0)`.

**Atomicity is the store's job, per key.** Redis runs reserve and settle as Lua
scripts, so the read-compare-write happens on the server; the two keys one budget
occupies (`:spent`, `:reservations`) share a hash tag, so the scripts are
cluster-safe. Postgres runs each in a transaction that takes `SELECT ... FOR
UPDATE` on the budget's spend row, which serializes admissions for that key across
replicas. Two replicas racing one cap therefore cannot both be admitted.

**Reservations expire.** A replica that dies mid-request never settles, and a
hold that lived forever would wedge the budget. Each reservation carries an
absolute deadline (`reservation_ttl_seconds`, default 300) and a reserve reclaims
the expired holds for its key before it decides, so recovery needs no sweeper
process. A settlement that cannot reach the store logs and leaves the hold to
expire rather than blocking the caller on a retry.

**In-memory retention is bounded and lazy.** The per-replica backend retains at
most `max_subjects` ledgers. When that capacity is reached, a reserve lazily
prunes ledgers whose holds have expired and are unheld and idle beyond
`idle_ttl_seconds`; there is no background task, since a timer thread would
regress the Tier 0 default. Holds expire after `reservation_ttl_seconds`, and
only live holds make a ledger unevictable. Eviction can discard accumulated
`spent`, but that is the same class of approximation as a replica restart
resetting the in-memory counter, not a new guarantee; exact caps remain a Tier
1/Redis concern. If capacity is full and nothing is evictable, admission
returns `Denial::StoreUnavailable`: the cap cannot be enforced, so the request
is denied rather than incorrectly reported as over-budget. This capacity path
follows `on_unavailable`: `allow` admits an unheld reservation, while `deny`
preserves the fail-closed default. Expiry reclaims only the hold; a later
settlement still records measured spend.

The in-memory bound reserves a derived equal floor for each configured
namespace: `max_subjects / configured_namespace_count`, with a minimum of one.
There is no operator override. A namespace may use headroom only when it is not
reserved for another configured namespace's unmet floor.
When a reserve would consume another namespace's floor it reclaims and evicts
only unheld, idle ledgers in the requesting namespace. Because every configured
namespace holds a floor, including namespaces never seen on this replica, many
configured but inactive namespaces can leave the table mostly empty while an
active namespace receives `503` capacity denials; operators should size
`max_subjects` for the busiest namespace rather than the fleet average.
Namespace identity is configuration-owned, rather than learned from observed
subjects, so the guarantee is stable across traffic patterns. If no namespaces
are configured, or `max_subjects` is below the configured namespace count, floors
are disabled and the original global lazy behavior is retained (with a boot
warning in the latter case). This remains per-replica and fail-closed by
default; exact cross-replica retention and enforcement still requires Redis.

**Charging policy: charge for what was consumed.** Not `$0`, not the reserved
estimate.

- A completed request is charged its provider-reported usage, priced from the
  catalog. The estimate is a ceiling, not a bill.
- A stream that ends early — cancelled by the client, or broken mid-flight —
  usually never receives the provider's usage block. It is charged its **measured
  partial** spend: the prompt it consumed (the input estimate the hold was priced
  from) plus the generated text it actually relayed, counted as it was relayed.
  The relay already decodes every event, so this is measurement, not guesswork.
- Where usage is genuinely unknowable, the charge is `$0`: a buffered request
  whose upstream returned an error (providers do not return a usage block with an
  error, and nothing was relayed to measure), a stream that failed before its
  first byte, and anything rejected before dispatch. The hold is released in full.

When an OpenAI-normalized stream rotates before content is emitted, the request
still owns one reservation and produces one usage record. Input/prompt tokens
are charged once, using serving-attempt usage or the original estimate; output
usage from an abandoned attempt is carried forward, falling back to its
relayed-character estimate when no provider output count exists.

The measured partial figure lands on the canonical `UsageRecord`
(`input_tokens`, `output_tokens`, `cost_microdollars`) with its real status
(`client_cancelled` / `upstream_error`), so budgets and billing read the same
number — the invariant ADR 0002 set.

**Unavailability fails closed by default.** When a shared store cannot be reached,
`on_unavailable = "deny"` (the default) rejects with `503 budget_unavailable`,
distinct from the `429 budget_exceeded` an over-cap caller gets. `"allow"` is
available and logs a warning on every admission, because an operator may
legitimately prefer serving to enforcing — but it is a choice they make, not a
default they discover. This is a deliberate reversal of the previous service's
Redis limiter, which failed open and so disabled enforcement precisely when it
was least observable.

**Scope stays `BudgetKey { namespace, subject }`.** Per-model and hierarchical
(org → team → subject) caps are real requests and are deliberately *not* built
here: they change the admission decision from one key to a set of keys, which
changes the atomic unit both backends are built around. Follow-up, not beta.

**Dependency:** `redis` 1.4.1 (MIT), pinned exactly, with
`default-features = false` and only `tokio-comp`, `tokio-rustls-comp`,
`connection-manager`, and `script` — no sync client, no cluster or pubsub code,
and TLS through the `rustls` already in the tree. It is the de-facto Rust Redis
client. It pulls one transitive crate (`xxhash-rust`) under **BSL-1.0**, which is
added to the `deny.toml` allowlist: Boost is a standard permissive licence, OSI
approved and FSF free, with no attribution obligation on binaries. Nothing else in
the supply-chain policy changes. Postgres reuses the
existing `tokio-postgres` stack, including its TLS connector, rather than adding a
second one; its schema ships as
[`crates/gateway/sql/budget_v1.sql`](../../crates/gateway/sql/budget_v1.sql) and is versioned
by the same rule as the usage table — a change to the row shape is a new file.

## Consequences

- Shared caps are exact per key, at the cost of one round trip to reserve and one
  to settle. Settlement is off the request path (the streaming path already
  detaches it); reservation is not, which is the price of admission control.
- A held estimate makes the gateway *more* conservative than before under
  concurrency: `max_tokens` on a request now materially affects how many
  concurrent requests fit under a cap, since the reserved output allowance
  defaults to 1024 tokens when it is absent.
- Partial charging means a cancelled stream now consumes budget. That is the
  intent, and it makes the cap trustworthy, but operators reading spend across
  this change will see cancelled and failed streams stop being free.
- The character-count fallback for a stream with no reported usage is an
  approximation (~4 characters per token) and is only reached when the provider
  told us nothing. Its bias is toward under-charging on the input side, since the
  input estimate is the pre-dispatch one.
- Two datastore backends is two operational surfaces to document and test. The
  cross-replica tests exercise real servers when `AXOND_TEST_REDIS_URL` /
  `AXOND_TEST_POSTGRES_DSN` are set and skip otherwise, so the hermetic suite
  still runs with no datastore; CI's stateful lane sets
  `AXOND_TEST_REQUIRE_SERVICES=1` to make missing services fail.
- Per-model and hierarchical caps, and a budget-state admin/reset surface, remain
  open.
