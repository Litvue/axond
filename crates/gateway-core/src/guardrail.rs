use std::collections::HashMap;

use regex::Regex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuardrailAction {
    Block,
    Redact,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardrailRequest<'a> {
    pub scope_id: &'a str,
    pub prompts: &'a [String],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardrailVerdict {
    Allow,
    Block {
        policy_id: String,
    },
    Redact {
        policy_ids: Vec<String>,
        prompts: Vec<String>,
    },
}

pub trait Guardrail: Send + Sync {
    fn inspect(&self, request: GuardrailRequest<'_>) -> GuardrailVerdict;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuardrailRule {
    pub id: String,
    pub pattern: String,
    pub action: GuardrailAction,
    #[serde(default = "default_redaction")]
    pub redaction: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuardrailPolicy {
    pub scope_id: String,
    pub rules: Vec<GuardrailRule>,
}

struct CompiledRule {
    id: String,
    regex: Regex,
    action: GuardrailAction,
    redaction: String,
}

pub struct RegexGuardrail {
    policies: HashMap<String, Vec<CompiledRule>>,
}

impl RegexGuardrail {
    pub fn compile(policies: &[GuardrailPolicy]) -> Result<Self, regex::Error> {
        let mut compiled = HashMap::new();
        for policy in policies {
            let rules = policy
                .rules
                .iter()
                .map(|rule| {
                    Ok(CompiledRule {
                        id: rule.id.clone(),
                        regex: Regex::new(&rule.pattern)?,
                        action: rule.action,
                        redaction: rule.redaction.clone(),
                    })
                })
                .collect::<Result<_, regex::Error>>()?;
            compiled.insert(policy.scope_id.clone(), rules);
        }
        Ok(Self { policies: compiled })
    }
}

impl Guardrail for RegexGuardrail {
    fn inspect(&self, request: GuardrailRequest<'_>) -> GuardrailVerdict {
        let Some(rules) = self
            .policies
            .get(request.scope_id)
            .or_else(|| self.policies.get("*"))
        else {
            return GuardrailVerdict::Allow;
        };
        for rule in rules
            .iter()
            .filter(|rule| rule.action == GuardrailAction::Block)
        {
            if request
                .prompts
                .iter()
                .any(|prompt| rule.regex.is_match(prompt))
            {
                return GuardrailVerdict::Block {
                    policy_id: rule.id.clone(),
                };
            }
        }
        let mut prompts = request.prompts.to_vec();
        let mut policy_ids = Vec::new();
        for rule in rules
            .iter()
            .filter(|rule| rule.action == GuardrailAction::Redact)
        {
            let mut matched = false;
            for prompt in &mut prompts {
                let replaced = rule
                    .regex
                    .replace_all(prompt, regex::NoExpand(&rule.redaction));
                if replaced != prompt.as_str() {
                    *prompt = replaced.into_owned();
                    matched = true;
                }
            }
            if matched {
                policy_ids.push(rule.id.clone());
            }
        }
        if policy_ids.is_empty() {
            GuardrailVerdict::Allow
        } else {
            GuardrailVerdict::Redact {
                policy_ids,
                prompts,
            }
        }
    }
}

fn default_redaction() -> String {
    "[REDACTED]".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_precedes_redaction_and_scope_policy_falls_back() {
        let guardrail = RegexGuardrail::compile(&[GuardrailPolicy {
            scope_id: "*".into(),
            rules: vec![
                GuardrailRule {
                    id: "email".into(),
                    pattern: r"\S+@\S+".into(),
                    action: GuardrailAction::Redact,
                    redaction: "[EMAIL]".into(),
                },
                GuardrailRule {
                    id: "deny".into(),
                    pattern: "forbidden".into(),
                    action: GuardrailAction::Block,
                    redaction: default_redaction(),
                },
            ],
        }])
        .unwrap();
        assert!(matches!(
            guardrail.inspect(GuardrailRequest {
                scope_id: "scope-a",
                prompts: &["a@example.com forbidden".into()],
            }),
            GuardrailVerdict::Block { policy_id } if policy_id == "deny"
        ));
        assert_eq!(
            guardrail.inspect(GuardrailRequest {
                scope_id: "scope-a",
                prompts: &["email a@example.com".into()],
            }),
            GuardrailVerdict::Redact {
                policy_ids: vec!["email".into()],
                prompts: vec!["email [EMAIL]".into()],
            }
        );
    }
}
