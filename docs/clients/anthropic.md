# Anthropic clients

Axond serves Anthropic's native Messages API at `/v1/messages`. Use the Axond
host as the SDK base URL and an Axond gateway key as the API key. Provider
credentials remain inside Axond.

The model value is an Axond alias whose targets all use the Anthropic wire
family. Axond forwards native request and response values, including tool-use
and signed thinking blocks, while rewriting only `model`.

## Python

```python
from anthropic import Anthropic

client = Anthropic(
    base_url="http://localhost:8080",
    api_key="quickstart-platform-key",
)

message = client.messages.create(
    model="claude-sonnet",
    max_tokens=64,
    messages=[{"role": "user", "content": "hello"}],
)
print(message.content)
```

Streaming uses the native SDK stream:

```python
with client.messages.stream(
    model="claude-sonnet",
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
curl http://localhost:8080/v1/messages \
  -H 'x-api-key: quickstart-platform-key' \
  -H 'anthropic-version: 2023-06-01' \
  -H 'content-type: application/json' \
  -d '{"model":"claude-sonnet","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}'
```

## Behavioral boundaries

- Native Messages is passthrough, not OpenAI-to-Anthropic translation.
- An OpenAI-family alias sent to `/v1/messages` returns
  `400 unsupported_wire` before dispatch.
- Credential rotation may retry the stream open before relay bytes begin.
  Native streams are terminal once relay begins.
- Scoped minted tokens need the `messages` capability.

The provider SDK compatibility lane exercises the supported Python Anthropic
client against a real Axond process on every PR. See the
[compatibility contract](../compatibility.md).
