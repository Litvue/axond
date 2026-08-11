use serde_json::{Value, json};

use crate::{
    Capabilities, ModelUsage, ProviderAdapter, ProviderError, ProviderRequest, ProviderResponse,
    ProviderStreamDecoder, ProviderStreamEvent, SseEvent, Surface, provider::chat_usage,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenAiFlavor {
    OpenAi,
    Foundry,
    Compatible,
}

pub struct OpenAiCompatibleAdapter {
    flavor: OpenAiFlavor,
}

impl OpenAiCompatibleAdapter {
    pub fn new(flavor: OpenAiFlavor) -> Self {
        Self { flavor }
    }

    pub fn openai() -> Self {
        Self::new(OpenAiFlavor::OpenAi)
    }

    pub fn foundry() -> Self {
        Self::new(OpenAiFlavor::Foundry)
    }
}

pub fn normalize_foundry_endpoint(endpoint: &str) -> String {
    let endpoint = endpoint.trim().trim_end_matches('/');
    let has_path = endpoint
        .split_once("://")
        .is_some_and(|(_, authority)| authority.contains('/'));
    if has_path {
        endpoint.to_owned()
    } else {
        format!("{endpoint}/openai/v1")
    }
}

/// Usage from an OpenAI-compatible embeddings response. Embeddings generate no
/// completion, so only the prompt is billed: whatever the provider reports as
/// output is ignored rather than priced.
pub fn embeddings_usage(response: &Value) -> ModelUsage {
    ModelUsage {
        output_tokens: 0,
        ..chat_usage(response)
    }
}

impl ProviderAdapter for OpenAiCompatibleAdapter {
    fn name(&self) -> &'static str {
        match self.flavor {
            OpenAiFlavor::OpenAi => "openai",
            OpenAiFlavor::Foundry => "foundry",
            OpenAiFlavor::Compatible => "openai_compatible",
        }
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            chat: true,
            responses: true,
            vision: true,
            reasoning: true,
            embeddings: false,
        }
    }

    fn encode_request(
        &self,
        surface: Surface,
        request: ProviderRequest,
    ) -> Result<Value, ProviderError> {
        let mut body = request.body;
        let object = body.as_object_mut().ok_or_else(|| {
            ProviderError::InvalidRequest("request body must be an object".into())
        })?;
        object.insert("model".into(), json!(request.model));
        if surface == Surface::ChatCompletions && object.get("stream") == Some(&Value::Bool(true)) {
            if let Some(options) = object
                .get_mut("stream_options")
                .and_then(Value::as_object_mut)
            {
                options.insert("include_usage".into(), Value::Bool(true));
            } else {
                object.insert("stream_options".into(), json!({ "include_usage": true }));
            }
        }
        Ok(body)
    }

    fn decode_response(
        &self,
        surface: Surface,
        response: Value,
    ) -> Result<ProviderResponse, ProviderError> {
        let usage = match surface {
            Surface::ChatCompletions => chat_usage(&response),
            Surface::Responses => response.get("usage").map(chat_usage).unwrap_or_default(),
        };
        Ok(ProviderResponse {
            body: response,
            usage,
        })
    }

    fn stream_decoder(
        &self,
        surface: Surface,
    ) -> Result<Box<dyn ProviderStreamDecoder>, ProviderError> {
        Ok(Box::new(OpenAiStreamDecoder {
            surface,
            usage: ModelUsage::default(),
            done: false,
        }))
    }
}

struct OpenAiStreamDecoder {
    surface: Surface,
    usage: ModelUsage,
    done: bool,
}

impl ProviderStreamDecoder for OpenAiStreamDecoder {
    fn decode(&mut self, event: SseEvent) -> Result<Vec<ProviderStreamEvent>, ProviderError> {
        if event.data.trim() == "[DONE]" {
            self.done = true;
            return Ok(vec![ProviderStreamEvent::Done(self.usage)]);
        }
        let data: Value = serde_json::from_str(&event.data)
            .map_err(|error| ProviderError::InvalidStream(error.to_string()))?;
        if crate::is_rate_limit_payload(&data) {
            return Err(ProviderError::RateLimitedStream(event.data));
        }
        let usage = match self.surface {
            Surface::ChatCompletions => data.get("usage"),
            Surface::Responses => data
                .pointer("/response/usage")
                .or_else(|| data.get("usage")),
        };
        if let Some(usage) = usage.filter(|usage| usage.is_object()) {
            self.usage = chat_usage(usage);
        }
        let event_name = match self.surface {
            Surface::ChatCompletions => event.event,
            Surface::Responses => event
                .event
                .or_else(|| data.get("type").and_then(Value::as_str).map(str::to_owned)),
        };
        Ok(vec![ProviderStreamEvent::Data {
            event: event_name,
            data,
        }])
    }

    fn finish(&mut self) -> Result<Vec<ProviderStreamEvent>, ProviderError> {
        if self.done {
            Ok(Vec::new())
        } else {
            self.done = true;
            Ok(vec![ProviderStreamEvent::Done(self.usage)])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foundry_endpoint_normalization_preserves_explicit_paths() {
        assert_eq!(
            normalize_foundry_endpoint("https://example.openai.azure.com/"),
            "https://example.openai.azure.com/openai/v1"
        );
        assert_eq!(
            normalize_foundry_endpoint("https://example.test/custom/v1/"),
            "https://example.test/custom/v1"
        );
    }

    #[test]
    fn rewrites_model_and_forces_stream_usage() {
        let body = OpenAiCompatibleAdapter::foundry()
            .encode_request(
                Surface::ChatCompletions,
                ProviderRequest {
                    model: "deployment".into(),
                    body: json!({ "model": "foundry/deployment", "stream": true }),
                },
            )
            .unwrap();
        assert_eq!(body["model"], "deployment");
        assert_eq!(body["stream_options"]["include_usage"], true);
    }

    #[test]
    fn embeddings_usage_is_prompt_only() {
        assert_eq!(
            embeddings_usage(&json!({
                "object": "list",
                "data": [{ "embedding": [0.1, 0.2] }],
                "usage": { "prompt_tokens": 8, "total_tokens": 8, "completion_tokens": 3 }
            })),
            ModelUsage {
                input_tokens: 8,
                ..ModelUsage::default()
            }
        );
    }

    #[test]
    fn chat_and_responses_preserve_unknown_fields_verbatim() {
        for surface in [Surface::ChatCompletions, Surface::Responses] {
            let original = json!({
                "model": "qualified/model",
                "stream": false,
                "future_field": { "nested": [1, 2, 3] },
                "tools": [{ "future_tool_field": true }],
                "reasoning": { "effort": "high" }
            });
            let encoded = OpenAiCompatibleAdapter::openai()
                .encode_request(
                    surface,
                    ProviderRequest {
                        model: "bare-model".into(),
                        body: original.clone(),
                    },
                )
                .unwrap();
            let mut expected = original;
            expected["model"] = json!("bare-model");
            assert_eq!(encoded, expected);

            let response = json!({
                "id": "response_1",
                "future_response_field": { "opaque": true },
                "usage": { "input_tokens": 3, "output_tokens": 4 }
            });
            assert_eq!(
                OpenAiCompatibleAdapter::openai()
                    .decode_response(surface, response.clone())
                    .unwrap()
                    .body,
                response
            );
        }
    }

    #[test]
    fn stream_usage_rewrite_preserves_other_stream_options() {
        let body = OpenAiCompatibleAdapter::openai()
            .encode_request(
                Surface::ChatCompletions,
                ProviderRequest {
                    model: "model".into(),
                    body: json!({
                        "stream": true,
                        "stream_options": { "future_option": "keep", "include_usage": false }
                    }),
                },
            )
            .unwrap();
        assert_eq!(body["stream_options"]["future_option"], "keep");
        assert_eq!(body["stream_options"]["include_usage"], true);
    }

    #[test]
    fn stream_decoder_forwards_verbatim_and_finishes_with_usage() {
        let mut decoder = OpenAiCompatibleAdapter::openai()
            .stream_decoder(Surface::ChatCompletions)
            .unwrap();
        let chunk = json!({
            "id": "chunk_1",
            "choices": [],
            "opaque": { "keep": true },
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "completion_tokens_details": { "reasoning_tokens": 2 },
                "prompt_tokens_details": { "cached_tokens": 3 }
            }
        });
        let forwarded = decoder
            .decode(SseEvent {
                event: Some("custom".into()),
                data: chunk.to_string(),
            })
            .unwrap();
        assert_eq!(
            forwarded,
            vec![ProviderStreamEvent::Data {
                event: Some("custom".into()),
                data: chunk
            }]
        );
        assert_eq!(
            decoder
                .decode(SseEvent {
                    event: None,
                    data: "[DONE]".into(),
                })
                .unwrap(),
            vec![ProviderStreamEvent::Done(ModelUsage {
                input_tokens: 10,
                output_tokens: 5,
                reasoning_tokens: 2,
                cache_read_tokens: 3,
                cache_write_tokens: 0,
            })]
        );

        let mut responses = OpenAiCompatibleAdapter::openai()
            .stream_decoder(Surface::Responses)
            .unwrap();
        responses
            .decode(SseEvent {
                event: None,
                data: json!({
                    "type": "response.completed",
                    "response": { "usage": {
                        "input_tokens": 20,
                        "output_tokens": 8,
                        "output_tokens_details": { "reasoning_tokens": 6 },
                        "input_tokens_details": { "cached_tokens": 4 }
                    }}
                })
                .to_string(),
            })
            .unwrap();
        assert_eq!(
            responses.finish().unwrap(),
            vec![ProviderStreamEvent::Done(ModelUsage {
                input_tokens: 20,
                output_tokens: 8,
                reasoning_tokens: 6,
                cache_read_tokens: 4,
                cache_write_tokens: 0,
            })]
        );
    }
}
