# 11. Config hot-reload: the boot gate, applied again, swapped atomically

Date: 2026-08-04

## Status

Accepted

## Context

ADR 0003 made credentials namespaced and explicitly declared, and noted that
reading them once at startup was the deliberate first cut. Onboarding a BYOK
customer is therefore a two-part operation today: add a `[[namespace]]` plus a
`[[credential]]` to the config, export the key, and then **restart the gateway**.
A restart is a bad price for a routine sales event — it drops in-flight streams,
empties the per-replica circuit and credential health, and turns "add a tenant"
into a change-window conversation.

Three existing commitments constrain how reload can work:

- **Fail at boot, not at request time** (ADR 0002 / delta B2). The whole config
  graph is validated before the process serves. A reload that could publish a
  half-valid config would move validation back onto the request path — exactly
  what the boot gate exists to prevent.
- **A request is resolved against one config.** Routing, credential selection,
  circuit gating, and pricing all read the config at different points in a
  request. If the config could change between those reads, a request could be
  priced from one generation and dispatched under another.
- **Some of the config is not process-reloadable.** The listening socket is bound
  at startup, and usage sinks own live connections and flush tasks that were
  validated and connected at boot (ADR 0009).

## Decision

**One reload path, two triggers.** `SIGHUP` is the trigger that always exists —
it is the operator's explicit "I have edited the file and exported the key".
Watching the config file is opt-in (`[reload] watch = true`), because a watch
reloads whatever the file says the moment it says it, which is right for a
ConfigMap or a config-management agent and wrong for someone editing in place on
a box. Both triggers run `Reloader::reload`, so there is one behaviour to reason
about and one place it is tested.

**The candidate goes through the boot gate, unchanged.** A reload calls the same
`Config::load` (TOML + `AXOND_` env overrides + `validate`) and then builds the
same resolved snapshot the boot path builds, including the credential graph. Any
error — unreadable file, bad TOML, an alias pointing at an undefined provider, a
declared credential whose env var is unset — **rejects the candidate and leaves
the running config serving**. Reload failures are loud (a structured `error!` and
a rejected outcome in telemetry) and inert. This is the fail-at-boot posture
applied a second time: the gate is the same, so a config that reloads is a config
that would have booted.

**The environment is re-read at reload time.** `std::env::vars()` is snapshotted
per reload, which is what makes the driving use case work: export the new
tenant's key, add its `[[credential]]`, `SIGHUP`. Note the consequence — the
gateway reads *process* env, so a key exported in a different shell is invisible
to it; the export has to reach the process (systemd `EnvironmentFile`, container
env, `envFrom`). Nothing rewrites the process environment on its behalf.

**Everything config-derived lives in one snapshot behind `ArcSwap`.** The config,
the resolved credential pools, the inbound gateway-key table, and the per-target
circuits form a `ConfigSnapshot`; `AppState` holds `ArcSwap<ConfigSnapshot>` and
publishing is a single atomic store. A handler takes the snapshot **once**, at
the top of the request, and holds that `Arc` for the request's life — including
across a streamed response. So a reload can never half-apply to a request: every
request runs entirely on the generation it started on, and in-flight work is
untouched. `arc-swap` (MIT OR Apache-2.0) was added for this; it needed no change
to `deny.toml` and no newly-allowed licence. The alternative, an
`RwLock<Arc<Config>>`, would put a lock acquisition on every request path read
for no benefit, since readers never mutate.

**The per-target circuits and per-credential health belong to the snapshot, and
so reset on reload.** They are in-memory and per-replica by design (ADR 0002 /
0008), rebuilt from thresholds that are themselves config. Carrying health across
a reload would mean reconciling breaker keys against a changed target set and
deciding what a threshold change means for an already-tripped circuit — real
complexity to preserve seconds of state that a cooldown re-derives. A reload is
rare and operator-initiated; re-probing a target after one is acceptable.

**Watching is a content poll, not an inotify subscription.** The watcher compares
the file's bytes at `[reload] poll_interval_ms` (default 2 s, floor 100 ms). That
registers an in-place editor write *and* the symlink swap Kubernetes uses for a
mounted ConfigMap — the case where filesystem-notification APIs are most
awkward — while a touched-but-identical file reloads nothing. A momentarily
unreadable path (mid-rename) is skipped rather than treated as a change. It also
avoids a dependency: `notify` would be the natural choice and is licensed
CC0-1.0, which is not in `deny.toml`'s allow-list, and weakening the supply-chain
policy to save a `tokio::time::sleep` loop is a bad trade. The watcher reads
`[reload]` from the *current* snapshot each pass, so watching can itself be
turned on, retuned, or turned off by a `SIGHUP` reload.

**Reloads are serialized, and the file is read once per applied reload.** The
triggers are independent tasks, so the read-current, build-candidate,
publish-with-`generation + 1` sequence is taken under one mutex: the generation
counter stays monotonic, and the newest read always wins. The same lock holds the
bytes the last reload acted on, which the watcher compares against — so an
operator who edits the file *and* signals gets one reload, not one per trigger,
and watching being turned on does not re-apply the edit that turned it on.

**Process-level changes are reported, not applied.** `[server] bind` and
`[[usage_sink]]` differences are logged as warnings naming the restart
requirement, so an operator is told rather than left wondering why their edit did
nothing. They are compared against what the process bound and connected **at
boot**, not against the previous candidate: the config in the snapshot is the
file's opinion, and the warning has to keep being true for as long as the file
and the running socket disagree.

**Reload outcomes are observable** on the ADR 0007 stack: an
`axond.config.reload` span with `axond.reload.trigger` / `axond.reload.outcome`,
a counter `axond.config.reloads{trigger,outcome}`, and a gauge
`axond.config.generation` — `0` at boot, `+1` per applied reload — so a fleet
where one replica missed a reload is visible as a generation skew. The applied
log line carries an added/removed diff of namespaces, providers, aliases,
credential labels, and gateway-key env-var *names*: references, never secrets.

## Consequences

- Adding a namespace, a provider, an alias, a credential, or a gateway key is now
  a zero-restart operation, and a bad edit costs a log line instead of a failed
  boot loop.
- Reload is coarse: the whole graph is rebuilt and republished. That is what makes
  the atomicity easy to state, and the rebuild is microseconds of map-building
  against a config that is kilobytes.
- Circuit and credential health reset on every reload, so a reload storm (a
  misconfigured watcher against a churning file) would keep re-probing unhealthy
  targets. The poll floor and the opt-in default bound this; a rate limit on
  reloads is a follow-up if it is ever observed.
- Bind address and usage-sink changes still need a restart. Rebinding a listener
  and re-connecting sinks under load are each their own design (draining the old
  socket, flushing the old sink) and are deliberately not attempted here.
- The `[reload]` section is itself hot-reloadable, which means a reload can
  disable watching. That is intended: `SIGHUP` always remains.
