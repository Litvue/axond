# OpenAI clients

Axond serves OpenAI-compatible chat completions, Responses, and embeddings.
Set the SDK base URL to Axond's `/ns/{namespace}/v1` prefix and use the
deployment gateway key as the SDK API key. Provider credentials remain only in
the Axond deployment.

The model value is `provider-id/model-id` (for example `openai/gpt-4o`). Axond
splits on the first `/`, selects the configured provider, and forwards the bare
id.

## Python

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8080/ns/platform/v1",
    api_key="quickstart-platform-key",
)

completion = client.chat.completions.create(
    model="openai/gpt-4o",
    messages=[{"role": "user", "content": "hello"}],
)
print(completion.choices[0].message.content)
```

Streaming uses the SDK's ordinary interface:

```python
stream = client.chat.completions.create(
    model="openai/gpt-4o",
    messages=[{"role": "user", "content": "count to three"}],
    stream=True,
)
for event in stream:
    print(event)
```

Responses and embeddings are native passthrough routes:

```python
# Responses pins the first credential of the provider pool so `response.id`
# stays resolvable upstream. There is no alias-level failover.
response = client.responses.create(model="openai/gpt-4o", input="hello")
follow_up = client.responses.create(
    model="openai/gpt-4o",
    input="and again",
    previous_response_id=response.id,
)
embedding = client.embeddings.create(
    model="openai/text-embedding-3-small",
    input="hello",
)
```

## TypeScript

```typescript
import OpenAI from "openai";

const client = new OpenAI({
  baseURL: "http://localhost:8080/ns/platform/v1",
  apiKey: "quickstart-platform-key",
});

const completion = await client.chat.completions.create({
  model: "openai/gpt-4o",
  messages: [{ role: "user", content: "hello" }],
});
console.log(completion.choices[0].message.content);
```

## curl

```bash
curl http://localhost:8080/ns/platform/v1/responses \
  -H 'Authorization: Bearer quickstart-platform-key' \
  -H 'content-type: application/json' \
  -d '{"model":"openai/gpt-4o","input":"hello"}'
```

Publish a period budget before the first inference call
(`PUT /api/v1/namespaces/platform/budgets/{period}`); a namespace with no
budget row is `429 budget_exceeded`.

## Behavioral boundaries

- Axond forwards the OpenAI wire and rewrites only `model`; it is not a generic
  request translator.
- Credential-pool rotation happens before streamed response bytes are
  committed. Once content is emitted, a stream cannot move to another
  credential without corrupting the wire.
- Every Responses request is pinned to the provider pool's first credential.
  Continuations therefore reach the provider that stored the response, but the
  Responses route gets no credential rotation.
- Only a request with a non-empty `previous_response_id` can return
  `503 continuation_affinity_unavailable`.
- `GET /ns/{ns}/v1/models` lists cached `provider-id/model-id` ids available
  after the namespace blocklist.
- Unprefixed `/v1` is not served. Minted tokens are not inbound identity.

The provider SDK compatibility lane exercises the supported Python OpenAI
client against a real Axond process on every PR. See the
[compatibility contract](../compatibility.md) for exact support and deferrals.
