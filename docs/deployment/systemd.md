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
GW_INBOUND_ACME_KEY=replace-me
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

## Rollout behavior

The current process does not implement application-level SIGTERM draining.
Before stopping a service instance, remove it from the load balancer and wait
for the configured upstream drain window. Interrupted clients must be able to
retry. See [Upgrades and rollback](../operations/upgrades.md).
