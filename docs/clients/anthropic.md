# Anthropic clients

Axond serves Anthropic's native Messages API at `/ns/{ns}/v1/messages`. Use
`http://host/ns/{namespace}` as the SDK base URL (the SDK appends
`/v1/messages`) and the deployment gateway key as the API key. Provider
credentials remain inside Axond.

The model value is `provider-id/model-id` (for example
`anthropic/claude-3-7-sonnet-latest`). Axond forwards native request and
response values, including tool-use and signed thinking blocks, while rewriting
only `model`.

## Python

```python
from anthropic import Anthropic

client = Anthropic(
    base_url="http://localhost:8080/ns/platform",
    api_key="quickstart-platform-key",
)

message = client.messages.create(
    model="anthropic/claude-3-7-sonnet-latest",
    max_tokens=64,
    messages=[{"role": "user", "content": "hello"}],
)
print(message.content)
```

Streaming uses the native SDK stream:

```python
with client.messages.stream(
    model="anthropic/claude-3-7-sonnet-latest",
    max_tokens=64,
    messages=[{"role": "user", "content": "count to three"}],
) as stream:
    for text in stream.text_stream:
        print(text, end="", flush=True)
```

## curl

Anthropic clients normally send `x-api-key`; Axond accepts it alongside bearer
authentication:

```bash
curl http://localhost:8080/ns/platform/v1/messages \
  -H 'x-api-key: quickstart-platform-key' \
  -H 'anthropic-version: 2023-06-01' \
  -H 'content-type: application/json' \
  -d '{"model":"anthropic/claude-3-7-sonnet-latest","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}'
```

Publish a period budget before the first inference call
(`PUT /api/v1/namespaces/{ns}/budgets/{period}`); a namespace with no budget
row is `429 budget_exceeded`.

## Behavioral boundaries

- Native Messages is passthrough, not OpenAI-to-Anthropic translation.
- An OpenAI-kind provider sent to `/v1/messages` returns
  `400 unsupported_wire` before dispatch.
- Credential rotation may retry the stream open before relay bytes begin.
  Native streams are terminal once relay begins.
- Unprefixed `/v1` is not served. Minted tokens are not inbound identity.

The provider SDK compatibility lane exercises the supported Python Anthropic
client against a real Axond process on every PR. See the
[compatibility contract](../compatibility.md).
