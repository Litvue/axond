# OpenAI clients

Axond serves OpenAI-compatible chat completions, Responses, and embeddings.
Set the SDK base URL to Axond's `/v1` prefix and use an Axond gateway key as the
SDK API key. Provider credentials remain only in the Axond deployment.

The model value is an Axond alias. Every target of that alias must use the
OpenAI or OpenAI-compatible wire family.

## Python

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8080/v1",
    api_key="quickstart-platform-key",
)

completion = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "hello"}],
)
print(completion.choices[0].message.content)
```

Streaming uses the SDK's ordinary interface:

```python
stream = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "count to three"}],
    stream=True,
)
for event in stream:
    print(event)
```

Responses and embeddings are native passthrough routes:

```python
response = client.responses.create(model="gpt-4o", input="hello")
embedding = client.embeddings.create(
    model="text-embedding-3-small",
    input="hello",
)
```

## TypeScript

```typescript
import OpenAI from "openai";

const client = new OpenAI({
  baseURL: "http://localhost:8080/v1",
  apiKey: "quickstart-platform-key",
});

const completion = await client.chat.completions.create({
  model: "gpt-4o",
  messages: [{ role: "user", content: "hello" }],
});
console.log(completion.choices[0].message.content);
```

## curl

```bash
curl http://localhost:8080/v1/responses \
  -H 'Authorization: Bearer quickstart-platform-key' \
  -H 'content-type: application/json' \
  -d '{"model":"gpt-4o","input":"hello"}'
```

## Behavioral boundaries

- Axond forwards the OpenAI wire and rewrites only `model`; it is not a generic
  request translator.
- Ordered failover and credential rotation happen before streamed response
  bytes are committed. Once content is emitted, a stream cannot move to another
  target without corrupting the wire.
- A Responses request carrying `previous_response_id` is pinned to the first
  configured target so a continuation is not silently sent elsewhere.
- `/v1/models` lists only aliases available to the authenticated namespace.
- Scoped minted tokens need `chat`, `responses`, `embeddings`, or `models` for
  the corresponding route.

The provider SDK compatibility lane exercises the supported Python OpenAI
client against a real Axond process on every PR. See the
[compatibility contract](../compatibility.md) for exact support and deferrals.
