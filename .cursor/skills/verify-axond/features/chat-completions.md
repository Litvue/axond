# Chat completions

Chat completions is the OpenAI chat wire on `POST /ns/{namespace}/v1/chat/completions`. The caller sends `model` as `provider-id/model-id`. Axond rewrites `model` and forwards the rest to the provider. This recipe uses the fixture upstream, not a live OpenAI account.

## Sub-features

- `chat-dispatch` returns `200` `object: chat.completion` for `fixture-openai/fixture-chat` after a budget exists.
- `chat-upstream` records a fixture POST to `/chat/completions` with the provider model id `fixture-chat` (not the inbound prefix).
- `chat-unprefixed` refuses a model without `provider-id/` as `400 model_unprefixed`.
- `chat-unsupported-wire` refuses an OpenAI-kind model on `/v1/messages` as `400 unsupported_wire`.
- `chat-secrets` leaves `VERIFY-INBOUND-KEY` and `VERIFY-UPSTREAM-KEY` out of the caller-visible body.

## How to get to it (user POV)

- OpenAI SDK: `base_url="{base}/ns/platform/v1"`, `api_key` = gateway key, `model="fixture-openai/fixture-chat"` (or `openai/gpt-4o` against a real provider).
- curl `POST /ns/platform/v1/chat/completions` with `Authorization: Bearer` and a JSON body of `model` plus `messages`.
- Streaming: the same path with `"stream": true`. Buffered is the default proof here.
- Related wires (not this recipe's required entry): `POST /ns/{ns}/v1/embeddings`, `POST /ns/{ns}/v1/responses`, `POST /ns/{ns}/v1/messages`.

## Driving it with drive.sh

Preconditions:

- Axond is healthy at `$AXOND_VERIFY_BASE_URL`.
- `helpers/doctor.sh` passed.
- Period budget `verify` exists for `platform` (run the period-budgets PUT if this is a fresh launch).
- Fixture upstream is the process launch started (`doctor.sh` shows its PID).

- **Publish cap if needed.** Run `.cursor/skills/verify-axond/helpers/drive.sh --name put-budget --method PUT --json '{"limit_microdollars":1000000000000}' /api/v1/namespaces/platform/budgets/verify`. Status `200` or an already-active ledger with that limit.
- **Unprefixed model.** Run `.cursor/skills/verify-axond/helpers/drive.sh --name chat-unprefixed --method POST --json '{"model":"does-not-exist","messages":[{"role":"user","content":"hello"}]}' /ns/platform/v1/chat/completions`. Status `400`. `error.type` is `model_unprefixed`. No new `upstream.jsonl` line.
- **Unsupported wire.** Send the OpenAI fixture model to Anthropic Messages. Run `.cursor/skills/verify-axond/helpers/drive.sh --name chat-unsupported-wire --method POST --json '{"model":"fixture-openai/fixture-chat","max_tokens":16,"messages":[{"role":"user","content":"hello"}]}' /ns/platform/v1/messages`. Status `400`. `error.type` is `unsupported_wire`.
- **Dispatch.** Run `.cursor/skills/verify-axond/helpers/drive.sh --name chat --method POST --json '{"model":"fixture-openai/fixture-chat","messages":[{"role":"user","content":"What is the capital of France?"}]}' /ns/platform/v1/chat/completions`. Status `200`. `response.body` has `"object":"chat.completion"` and a `choices` array. It must not contain `VERIFY-INBOUND-KEY` or `VERIFY-UPSTREAM-KEY`.
- **Upstream side effect.** Read `$AXOND_VERIFY_RUN_DIR/upstream.jsonl` (or the evidence copy after cleanup). A line has `"path":"/chat/completions"`, `"model":"fixture-chat"`, `"status":200`. The inbound prefix `fixture-openai/` does not appear as the upstream `model`.
- **Proof.** Keep `drive/chat/request.txt`, `drive/chat/response.body`, `drive/chat-unprefixed/response.body`, and `upstream.jsonl`. HTTP `200` without an upstream line is not dispatch; an upstream line without the caller `200` is not a completed user path.

## Gotchas

- No budget row is `429 budget_exceeded` and looks like a dispatch bug. Publish `verify` first.
- The fixture model id is `fixture-chat`. `openai/gpt-4o` against this launch's config is not priced as a live OpenAI call; it will not hit api.openai.com because `base_url` is the fixture.
- Upstream `base_url` is path-concatenated. The fixture listens at `/chat/completions`, not `/v1/chat/completions`.
- `/v1/responses` does not rotate credentials. Do not use it as a substitute proof of the chat pool.
- JSON usage records on stdout may appear after the HTTP response. Prefer `upstream.jsonl` plus the caller body for this feature.
