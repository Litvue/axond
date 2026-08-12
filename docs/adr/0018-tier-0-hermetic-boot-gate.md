# 18. Hermetic Tier 0 boot-and-serve gate

Date: 2026-08-09

## Status

Accepted

## Context

ADR 0002 makes config-only operation the default, and ADR 0017 names that
deployment Tier 0: no datastore or network dependency at boot or on the
serving path. ADR 0014 already provides a black-box harness and committed
provider fixtures, but its process runs with the host's network privileges.
The Tier 0 promise needs a mechanical CI check rather than a convention that
can silently regress when a new feature starts a client or store.

## Decision

The existing static-musl CI lane runs `ops/tier0-gate.sh` after building the
binary. The script re-executes itself in an unprivileged user and network
namespace using `unshare --user --map-root-user --net --fork`, brings up only
loopback, and fails if the namespace can reach a public TCP endpoint or resolve
public DNS. It also asserts that the usual Redis (6379) and Postgres (5432)
loopback ports are not started by the gateway: after boot, the listener set
inside the namespace must contain exactly the gateway and fake-upstream ports.
The namespace itself excludes every external datastore because no
non-loopback interface is available; an attempted external Redis or Postgres
dependency therefore appears as a boot or serving failure.

Inside that namespace the gate boots a committed Tier 0 configuration, probes
health and readiness, checks inbound authentication and typed unknown-model
errors, and sends a real chat request to the committed-fixture fake upstream on
loopback. The configuration includes token-verifier env resolution so all
startup dependencies are exercised before process exec.

The same namespace also runs a committed stateful bootstrap configuration
([ADR 0027](./0027-stateless-and-stateful-operating-modes.md)) with its
referenced env vars unset. Validating it must not resolve a DSN or reach
Postgres, so a clean refusal inside a network-denied namespace is the mechanical
evidence that stateful bootstrap parsing connects to nothing — and that a
stateful process refuses to start rather than serve an empty snapshot while the
control plane is unimplemented. The gate also fails if that diagnostic contains
a connection string rather than a reference name.

The gate deliberately does **not** claim that loopback is a network boundary.
Loopback is allowed so a local fake upstream can prove the serving path. The
post-boot listener assertion catches an embedded or spawned datastore (or
sidecar) inside the namespace by requiring exactly the two listeners the gate
starts. It does not inspect unrelated host processes, and a datastore on the
host's loopback is outside this namespace and intentionally irrelevant.

### Amendment: the release lanes reuse the gate, without depending on the sandbox

The release workflow boots every Linux archive it publishes through this same
gate, which is the point of building ARM archives on ARM runners: the binary that
ships is executed, not only compiled. But a namespace is a property of the
*runner*, not of the artifact, so an AppArmor or seccomp restriction on a hosted
image would turn an otherwise valid release into a failed one.

`AXOND_TIER0_ALLOW_NO_NETNS=1` — set only by `release-binaries` — therefore lets
the gate degrade rather than refuse when neither unprivileged `unshare` nor
passwordless `sudo unshare` works. Boot, health, readiness, authentication, typed
errors, the fixture serving path, and the stateful refusal are all still asserted;
the two guarantees that came *from* the namespace, egress denial and the listener
set, are skipped and reported as `DEGRADED` in the log and the final line. A
degraded run additionally requires both fixed ports to be free, since outside a
namespace they are the host's. Any other value than a boolean is rejected rather
than treated as false.

CI leaves the variable unset, and `ops/check-release-config.py` fails if
`ci.yml` ever sets it, so the hermetic guarantee is still proven on every change —
the relaxation applies only where the alternative is an unpublishable release.

This extends the static-musl lane instead of adding another job: the qualified
artifact is already built there, and the namespace gate adds focused runtime
coverage without multiplying CI scheduling and aggregate status. `CI Success`
therefore remains the stable required aggregate.

## Consequences

- CI proves no outbound connectivity, no public DNS, and no gateway-started
  listener beyond the gateway and fake upstream. External Redis or Postgres
  dependencies cannot be reached in the namespace and fail through boot or
  serving behavior.
- Failures dump gateway logs and explain that a stateful feature must be
  gated behind an explicitly selected higher tier.
- New stateful features must declare their tier in an ADR and stay off the
  default path unless the deployment opts into that tier.
- Tier 1 and Tier 2 still need their own integration tests for unavailable
  stores, timeouts, fail-closed behavior, and recovery; this gate does not
  qualify those deployments.
