use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ProviderStreamEvent;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallFragment {
    pub choice_index: u64,
    pub tool_index: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default)]
    pub arguments: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssembledToolCall {
    pub choice_index: u64,
    pub tool_index: u64,
    pub id: String,
    pub name: String,
    pub arguments: String,
}

impl AssembledToolCall {
    pub fn arguments_json(&self) -> Result<Value, serde_json::Error> {
        serde_json::from_str(&self.arguments)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ToolCallAssemblyError {
    #[error("tool call {choice_index}/{tool_index} changed {field} from '{first}' to '{next}'")]
    ConflictingMetadata {
        choice_index: u64,
        tool_index: u64,
        field: &'static str,
        first: String,
        next: String,
    },
    #[error("tool call {choice_index}/{tool_index} is missing {field}")]
    MissingMetadata {
        choice_index: u64,
        tool_index: u64,
        field: &'static str,
    },
}

#[derive(Debug, Clone, Default)]
struct PendingToolCall {
    id: Option<String>,
    name: Option<String>,
    arguments: String,
}

#[derive(Debug, Clone, Default)]
pub struct ToolCallAssembler {
    pending: BTreeMap<(u64, u64), PendingToolCall>,
}

impl ToolCallAssembler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_fragment(
        &mut self,
        fragment: ToolCallFragment,
    ) -> Result<(), ToolCallAssemblyError> {
        let key = (fragment.choice_index, fragment.tool_index);
        let pending = self.pending.entry(key).or_default();
        merge_metadata(&mut pending.id, fragment.id, key, "id")?;
        merge_metadata(&mut pending.name, fragment.name, key, "name")?;
        pending.arguments.push_str(&fragment.arguments);
        Ok(())
    }

    pub fn push_event(
        &mut self,
        event: &ProviderStreamEvent,
    ) -> Result<usize, ToolCallAssemblyError> {
        let ProviderStreamEvent::Data { data, .. } = event else {
            return Ok(0);
        };
        let mut count = 0;
        for choice in data
            .get("choices")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let choice_index = choice.get("index").and_then(Value::as_u64).unwrap_or(0);
            for call in choice
                .pointer("/delta/tool_calls")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let fragment = ToolCallFragment {
                    choice_index,
                    tool_index: call.get("index").and_then(Value::as_u64).unwrap_or(0),
                    id: call.get("id").and_then(Value::as_str).map(str::to_owned),
                    name: call
                        .pointer("/function/name")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    arguments: call
                        .pointer("/function/arguments")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                };
                self.push_fragment(fragment)?;
                count += 1;
            }
        }
        Ok(count)
    }

    pub fn finish(self) -> Result<Vec<AssembledToolCall>, ToolCallAssemblyError> {
        self.pending
            .into_iter()
            .map(|((choice_index, tool_index), pending)| {
                let id = pending.id.ok_or(ToolCallAssemblyError::MissingMetadata {
                    choice_index,
                    tool_index,
                    field: "id",
                })?;
                let name = pending.name.ok_or(ToolCallAssemblyError::MissingMetadata {
                    choice_index,
                    tool_index,
                    field: "name",
                })?;
                Ok(AssembledToolCall {
                    choice_index,
                    tool_index,
                    id,
                    name,
                    arguments: pending.arguments,
                })
            })
            .collect()
    }
}

fn merge_metadata(
    current: &mut Option<String>,
    next: Option<String>,
    (choice_index, tool_index): (u64, u64),
    field: &'static str,
) -> Result<(), ToolCallAssemblyError> {
    let Some(next) = next.filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    if let Some(first) = current {
        if first != &next {
            return Err(ToolCallAssemblyError::ConflictingMetadata {
                choice_index,
                tool_index,
                field,
                first: first.clone(),
                next,
            });
        }
    } else {
        *current = Some(next);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn assembles_interleaved_calls_in_index_order() {
        let mut assembler = ToolCallAssembler::new();
        for data in [
            json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"id":"b","type":"function","function":{"name":"second","arguments":""}}]}}]}),
            json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"a","type":"function","function":{"name":"first","arguments":"{\"x\":"}}]}}]}),
            json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"function":{"arguments":"{}"}},{"index":0,"function":{"arguments":"1}"}}]}}]}),
        ] {
            assembler
                .push_event(&ProviderStreamEvent::Data { event: None, data })
                .unwrap();
        }
        let calls = assembler.finish().unwrap();
        assert_eq!(
            calls.iter().map(|call| call.tool_index).collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(calls[0].arguments_json().unwrap(), json!({"x": 1}));
        assert_eq!(calls[1].arguments_json().unwrap(), json!({}));
    }
}
