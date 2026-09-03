# Linux and systemd

The static musl archive is the simplest bare-metal or VM deployment. This guide
uses a dedicated service account, an environment file for secret references,
and the hardened unit in `deploy/systemd/axond.service`.

## Install the binary

Download and verify the `x86_64-unknown-linux-musl` release as described in
[Installation and verification](../installation.md#prebuilt-release-binary),
then install it:

```bash
sudo install -o root -g root -m 0755 axond /usr/local/bin/axond
sudo useradd --system --home-dir /var/lib/axond --shell /usr/sbin/nologin axond
sudo install -d -o axond -g axond -m 0750 /var/lib/axond
sudo install -d -o root -g axond -m 0750 /etc/axond
```

Copy and edit a configuration:

```bash
sudo install -o root -g axond -m 0640 \
  ops/compose/axond.quickstart.toml /etc/axond/axond.toml
```

## Install secrets

The environment file contains secret values named by the TOML. It must not be
world-readable:

```dotenv
GW_PLATFORM_OPENAI_API_KEY=replace-me
GW_PLATFORM_ANTHROPIC_API_KEY=replace-me
GW_ACME_OPENAI_API_KEY=replace-me
GW_INBOUND_PLATFORM_KEY=replace-me
```

```bash
sudo install -o root -g axond -m 0640 axond.env /etc/axond/axond.env
```

Do not place secret values directly in the TOML or unit. For static gateway
keys and verifier public material, file-backed configuration is also available;
write exact bytes with no accidental newline for static/HS256 secrets.

## Install and start the service

```bash
sudo install -o root -g root -m 0644 \
  deploy/systemd/axond.service /etc/systemd/system/axond.service
sudo systemctl daemon-reload
sudo systemctl enable --now axond
systemctl status axond
curl --fail http://127.0.0.1:8080/healthz
```

Structured logs go to journald:

```bash
journalctl -u axond -f
```

## Reload and rotate

```bash
sudo systemctl reload axond
```

The reload revalidates the whole candidate and leaves the previous snapshot
serving if it fails. Add a replacement credential/key alongside the old one,
reload, move callers, remove the retired entry, and reload again.

A running process cannot gain a new environment variable from an edited
`EnvironmentFile`; systemd reads that file only when starting the process. A
new environment-backed reference requires `systemctl restart`. File-backed
material can be replaced atomically and reloaded.

## Front it with TLS

Bind Axond to a private interface or loopback and terminate TLS in Caddy, nginx,
HAProxy, or the platform load balancer. Disable proxy response buffering and
set timeouts for long-lived SSE streams. Preserve caller authentication and
`traceparent` headers.

## Bound the host, not just the gateway

The unit file sets `LimitNOFILE`, `MemoryMax`, and `TasksMax` deliberately.
Axond's own `[admission]` ceilings are what should refuse work — a shed request
is a typed `429`/`503` the caller can act on — and these are the kernel's backstop
if one of those ceilings is ever set above what the host can hold. Keep them
consistent: `MemoryMax` above `admission.max_in_flight` x
`admission.max_request_bytes` plus steady-state footprint — with the shipped
defaults that product is 2 GiB, which is why the unit ships `MemoryMax=6G` rather
than 2G, and lowering it means lowering the `[admission]` ceilings with it — and
`LimitNOFILE`
above twice `max_in_flight` (one caller socket and one upstream socket per
in-flight request) plus the listener and store connections. A unit that hits
`MemoryMax` is killed rather than shedding, so the gateway's bounds should always
fire first.

## Rollout behavior

`systemctl stop` sends `SIGTERM`, which starts the process's own bounded drain:
`/readyz` fails immediately, admission closes after
`shutdown.drain_grace_ms`, admitted requests get `shutdown.deadline_ms`, and
usage/telemetry sinks flush within `shutdown.flush_timeout_ms`. A second
`SIGTERM` closes admission at once instead of waiting out the grace window.

`TimeoutStopSec` must exceed the sum of those three bounds, or systemd
`SIGKILL`s the process mid-flush and buffered usage records are lost. The unit
ships `TimeoutStopSec=30s` against 25s of defaults; raise it with the
configuration.

A load balancer that watches `/readyz` drains the instance on its own. One that
does not should have the instance removed before the stop, or
`shutdown.drain_grace_ms` raised to cover its polling interval. Interrupted
clients must still be able to retry. See
[Upgrades and rollback](../operations/upgrades.md).
