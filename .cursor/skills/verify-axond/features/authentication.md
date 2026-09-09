# Authentication

Authentication requires the deployment-wide static gateway key on every `/api/v1` and `/ns/...` route. Only `/healthz` and `/readyz` are public. The key is the value of `GW_VERIFY_INBOUND_KEY`, not the environment-variable name.

## Sub-features

- `auth-models` lists the namespaced catalogue with `Authorization: Bearer`.
- `auth-x-api-key` accepts the same value as `x-api-key` (Anthropic-style).
- `auth-missing` returns typed `401 unauthorized` when the header is absent.
- `auth-wrong` returns typed `401 unauthorized` for the wrong Bearer value.
- `auth-unprefixed` does not serve `/v1/models` (no namespace prefix).
- `auth-admin` does not serve `/admin/v1/status`.

## How to get to it (user POV)

- Point an OpenAI SDK at `{base}/ns/{namespace}/v1` with the gateway key as `api_key`.
- Point an Anthropic SDK at `{base}/ns/{namespace}` (the SDK appends `/v1/messages`) with `x-api-key`.
- Call `GET /ns/{namespace}/v1/models` or `GET /ns/{namespace}/v1/credentials` with curl.
- OpenAPI for management is `GET /api/v1/openapi.json` with the same key.

## Driving it with drive.sh

Preconditions:

- Axond is healthy at `$AXOND_VERIFY_BASE_URL`.
- `helpers/doctor.sh` passed.
- `AXOND_VERIFY_GATEWAY_KEY` is `VERIFY-INBOUND-KEY`.

- **Anonymous catalogue.** Omit the key. Run `.cursor/skills/verify-axond/helpers/drive.sh --name models-anon --no-auth /ns/platform/v1/models`. Status `401`. `response.body` contains `"type":"unauthorized"`.
- **Wrong key.** Send a different Bearer. Run `.cursor/skills/verify-axond/helpers/drive.sh --name models-wrong --no-auth --header 'Authorization: Bearer not-the-key' /ns/platform/v1/models`. Status `401` with typed `unauthorized`.
- **Bearer catalogue.** Use the launch key. Run `.cursor/skills/verify-axond/helpers/drive.sh --name models /ns/platform/v1/models`. Status `200`. Body is JSON with `"object":"list"` and `"data"` an array. Each `id`, when present, is `provider-id/model-id`. An empty `data` array is valid when discovery has not populated the cache.
- **x-api-key catalogue.** Send the same value on `x-api-key` and no Bearer. Run `.cursor/skills/verify-axond/helpers/drive.sh --name models-x-api-key --no-auth --header "x-api-key: ${AXOND_VERIFY_GATEWAY_KEY}" /ns/platform/v1/models`. Status `200` and the same list shape.
- **Credentials labels.** Run `.cursor/skills/verify-axond/helpers/drive.sh --name credentials /ns/platform/v1/credentials`. Status `200`. Body `"object":"list"`. Entries may include `credential_id` `verify-openai`; they must not contain `VERIFY-UPSTREAM-KEY` or `VERIFY-INBOUND-KEY`.
- **Unprefixed inference path.** Run `.cursor/skills/verify-axond/helpers/drive.sh --name unprefixed-models --no-auth /v1/models`. Status is not `200` with a catalogue (typically `404`). This path is not an entry point.
- **Withdrawn admin.** Run `.cursor/skills/verify-axond/helpers/drive.sh --name admin-status --no-auth /admin/v1/status`. Status is not a diagnostic document (typically `404`).
- **Proof.** Keep `drive/models-anon/response.body`, `drive/models/response.body`, and `drive/credentials/response.body`. The first is `401`; the others are `200` list envelopes with no secret markers.

## Gotchas

- The TOML field is `env = "GW_VERIFY_INBOUND_KEY"`. Sending that string as Bearer fails. Send the **value**.
- Minted `axt1.` tokens are not inbound identity. Do not treat `axond mint` output as a working key.
- `/ns/{ns}/v1/models` can be an empty list. That is still a successful authenticated catalogue, not a boot failure.
- `?namespaces=all` on `/v1/credentials` is an operator-only view. A wrong query value is typed `400 bad_request`, not a leak.
