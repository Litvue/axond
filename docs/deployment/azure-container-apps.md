# Production on Azure Container Apps

A Tier 0 Axond replica: the public GHCR image, TOML for structure, Azure Key
Vault for keys, JSON usage on stdout, and optional OTLP. No Redis or Postgres.
Inference, telemetry, and usage records work with this shape.

The [managed-container contract](./managed-containers.md) is the portable
checklist. This page is the worked path.

## What you deploy

| Piece | Where it lives |
| --- | --- |
| Binary | `ghcr.io/litvue/axond@sha256:<digest>` — pin the **index** digest from [installation](../installation.md#oci-image). There is no `latest` tag. |
| Structure | `axond.toml` mounted at `/etc/axond/axond.toml`. Providers, aliases, and the *names* of secrets. Never the secret values. |
| Provider keys and the inbound gateway key | Azure Key Vault → Container Apps secrets → environment variables named by the TOML. |
| Logs and usage | JSON on stdout. Container Apps sends them to Log Analytics. |
| Traces and metrics | Optional. Set `OTEL_EXPORTER_OTLP_ENDPOINT` (OTLP/HTTP only). Unset is a supported production posture: logs and usage still work. |

A Key Vault rotation of an **environment** secret does not reach a running
replica. Create a new revision. That is the rotation mechanism on this
platform; it is a drained rolling replace, not a config-file edit.

Do not enable Container Apps built-in authentication. Axond owns
`Authorization` / `x-api-key`. Platform auth would intercept those headers.

## 1. Pin the image

Resolve the current release tag to an index digest and verify it with the
commands in [Installation](../installation.md#oci-image). Deploy that digest,
not a tag:

```text
ghcr.io/litvue/axond@sha256:<digest>
```

## 2. Write the TOML

Commit a copy next to this guide at
[`deploy/azure-container-apps/axond.toml`](../../deploy/azure-container-apps/axond.toml).
Edit prices, provider ids, and the env-var *names* to match the keys you will
store. Do not put key material in the file. Callers send `provider-id/model-id`;
`[[model]]` is a boot error.

Minimum surface:

- `[storage]` (SQLite path or Postgres `dsn_env`)
- one `[[namespace]]` with `default = true`
- one `[[provider]]` per upstream (OpenAI, Anthropic, Azure OpenAI, …)
- one `[[credential]]` per `(namespace, provider)` with `env = "GW_..."`
- exactly one `[[gateway_key]]`
- `[[price]]` rules for billed model globs

Adding a provider later is a TOML edit plus a new Key Vault secret of the
referenced name, then a new revision. Rotating a key whose env-var name is
unchanged is only a Key Vault update plus a new revision.

## 3. Put keys in Key Vault

Create a vault, a user-assigned identity, and grant that identity `Key Vault
Secrets User` on the vault. Write each secret from a file so the value never
lands in shell history:

```bash
printf %s "$OPENAI_KEY" > /tmp/openai.key
az keyvault secret set --vault-name "$KV" --name gw-platform-openai --file /tmp/openai.key
shred -u /tmp/openai.key
```

Create one secret per TOML `env` reference, including the inbound gateway key.
The inbound value is what *your* app sends as `Authorization: Bearer …`. It is
not a provider key.

## 4. Create the Container App

The YAML at
[`deploy/azure-container-apps/containerapp.yaml`](../../deploy/azure-container-apps/containerapp.yaml)
is the revision spec. Fill the placeholders (`<subscription>`, `<digest>`,
Key Vault URIs, identity resource id), then:

```bash
az containerapp env create \
  --name axond-env \
  --resource-group "$RG" \
  --location "$LOC"

az containerapp create \
  --name axond \
  --resource-group "$RG" \
  --environment axond-env \
  --yaml deploy/azure-container-apps/containerapp.yaml
```

The spec:

- listens on `8080` with external HTTPS ingress (TLS terminates at ACA)
- injects every TOML `env` reference from a Key Vault-backed secret
- mounts the TOML as a secret volume at `/etc/axond/axond.toml`
- probes HTTP `GET /healthz` (startup + liveness) and `GET /readyz` (readiness)
- keeps `minReplicas: 1` so a cold start is not on the request path
- sets `terminationGracePeriodSeconds` above
  `drain_grace_ms + deadline_ms + flush_timeout_ms` (25 s with the example TOML)

HTTP probes on `/readyz` are required for drain: on `SIGTERM` Axond fails
readiness while `/healthz` stays `ok`. A TCP probe cannot see that.

## 5. Prove it

Replace `$FQDN` with the app's FQDN and `$INBOUND` with the inbound gateway
key.

```bash
curl --fail "https://$FQDN/healthz"
curl --fail "https://$FQDN/readyz"
curl --fail \
  -H "Authorization: Bearer $INBOUND" \
  "https://$FQDN/ns/platform/v1/models"
curl -sS -o /dev/null -w '%{http_code}\n' "https://$FQDN/ns/platform/v1/models"
```

Expect `ok`, `ready`, a catalogue, and `401` without a key.

Publish a period budget before inference; a namespace with no budget row is
`429 budget_exceeded`:

```bash
curl --fail \
  -H "Authorization: Bearer $INBOUND" \
  -H 'content-type: application/json' \
  -d '{"limit_microdollars":1000000000000}' \
  -X PUT "https://$FQDN/api/v1/namespaces/platform/budgets/aca"
```

Real inference:

```bash
curl --fail "https://$FQDN/ns/platform/v1/chat/completions" \
  -H "Authorization: Bearer $INBOUND" \
  -H 'content-type: application/json' \
  -d '{"model":"openai/gpt-4o","messages":[{"role":"user","content":"Say hello in one word."}]}'
```

Streamed:

```bash
curl --fail -N "https://$FQDN/ns/platform/v1/chat/completions" \
  -H "Authorization: Bearer $INBOUND" \
  -H 'content-type: application/json' \
  -d '{"model":"openai/gpt-4o","stream":true,"messages":[{"role":"user","content":"Count to five."}]}'
```

Credential labels (never secret values):

```bash
curl --fail \
  -H "Authorization: Bearer $INBOUND" \
  "https://$FQDN/ns/platform/v1/credentials"
```

Usage: one JSON object per completed request on stdout, collected by Log
Analytics. Query for `"name":"axond.usage"` or the record's `schema` field —
see [usage schema](../usage-schema.md).

Telemetry: if `OTEL_EXPORTER_OTLP_ENDPOINT` is set, traces and metrics leave
on OTLP/HTTP (`http/protobuf` only). See [observability](../observability.md).

## Rotation, reload, and what needs a new revision

| Change | How it lands |
| --- | --- |
| Provider or inbound key bytes, same env-var name | Update Key Vault, create a new Container Apps revision. |
| Add or remove a provider, alias, or credential *name* | Edit TOML, update the mounted secret, new revision. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | New revision (environment). |
| `[server] bind`, `[admission]`, `[transport]`, `[[usage_sink]]`, `[budget]` | New revision. These are boot-owned even when the file is re-read. |

`[reload] watch = true` re-reads the mounted TOML if the platform rewrites
that file in place. Container Apps secret volumes are populated at revision
start, so watching does not pick up a Key Vault edit by itself. Treat a new
revision as the reload.

`mode = "stateful"` and `/admin/v1/secrets` are withdrawn
([ADR 0063](../adr/0063-stateful-only-namespaced-gateway.md)). Rotate provider
keys by changing the env var behind `[[credential]]` and replacing the
revision.

## Ingress limits that affect streams

Default HTTP ingress times out a request at **240 seconds**. A generation that
runs longer, or a stream that is silent that long, is cut with `504` /
`stream timeout` at the proxy, not inside Axond.

For longer generations enable [premium
ingress](https://learn.microsoft.com/azure/container-apps/ingress-environment-configuration)
and raise the idle timeout (4–30 minutes). Keep
`[transport] stream_idle_timeout_ms` below that idle timeout so Axond, not the
proxy, is the first to name a stalled stream.

Do not scale to zero in front of interactive inference: boot includes config
validation and credential resolution.

## Next

- [Production checklist](./production-checklist.md)
- [Configuration reference](../configuration.md)
- [Client guides](../index.md#connect-a-client)
- [Upgrades and rollback](../operations/upgrades.md)
