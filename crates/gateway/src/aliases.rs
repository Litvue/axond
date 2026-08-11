//! Tiny, case-sensitive alias globs: exact names, one leading or trailing `*`,
//! or bare `*`. Invalid patterns and the empty set fail closed; a scope can
//! only narrow configured authority, never widen it.

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AliasPattern {
    Exact(String),
    Prefix(String),
    Suffix(String),
    Any,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AliasScope {
    patterns: Vec<AliasPattern>,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid alias pattern `{pattern}`")]
pub struct InvalidAliasPattern {
    pattern: String,
}

impl AliasScope {
    pub fn parse<I, S>(patterns: I) -> Result<Self, InvalidAliasPattern>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        patterns
            .into_iter()
            .map(Into::into)
            .map(|pattern| {
                let stars = pattern.bytes().filter(|&byte| byte == b'*').count();
                if pattern.is_empty()
                    || stars > 1
                    || (stars == 1 && !pattern.starts_with('*') && !pattern.ends_with('*'))
                {
                    return Err(InvalidAliasPattern { pattern });
                }
                Ok(match stars {
                    0 => AliasPattern::Exact(pattern),
                    1 if pattern == "*" => AliasPattern::Any,
                    1 if pattern.starts_with('*') => AliasPattern::Suffix(pattern[1..].to_owned()),
                    1 => AliasPattern::Prefix(pattern[..pattern.len() - 1].to_owned()),
                    _ => unreachable!(),
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|patterns| Self { patterns })
    }

    pub fn permits(&self, alias: &str) -> bool {
        self.patterns.iter().any(|pattern| match pattern {
            AliasPattern::Exact(value) => alias == value,
            AliasPattern::Prefix(value) => alias.starts_with(value),
            AliasPattern::Suffix(value) => alias.ends_with(value),
            AliasPattern::Any => true,
        })
    }

    pub fn subsumes(&self, pattern: &AliasPattern) -> bool {
        self.patterns
            .iter()
            .any(|ceiling| match (ceiling, pattern) {
                (AliasPattern::Any, _) => true,
                (AliasPattern::Exact(left), AliasPattern::Exact(right)) => left == right,
                (AliasPattern::Prefix(left), AliasPattern::Exact(right)) => right.starts_with(left),
                (AliasPattern::Prefix(left), AliasPattern::Prefix(right)) => {
                    right.starts_with(left)
                }
                (AliasPattern::Suffix(left), AliasPattern::Exact(right)) => right.ends_with(left),
                (AliasPattern::Suffix(left), AliasPattern::Suffix(right)) => right.ends_with(left),
                _ => false,
            })
    }

    pub fn is_subset_of(&self, ceiling: &Self) -> bool {
        self.patterns
            .iter()
            .all(|pattern| ceiling.subsumes(pattern))
    }

    pub fn patterns_for_claim(&self) -> Vec<String> {
        self.patterns
            .iter()
            .map(|pattern| match pattern {
                AliasPattern::Exact(value) => value.clone(),
                AliasPattern::Prefix(value) => format!("{value}*"),
                AliasPattern::Suffix(value) => format!("*{value}"),
                AliasPattern::Any => "*".to_owned(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patterns_match_case_sensitively_and_union() {
        let scope = AliasScope::parse(["gpt-4o", "claude-*", "*-latest", "*"]).unwrap();
        assert!(scope.permits("gpt-4o"));
        assert!(scope.permits("claude-3"));
        assert!(scope.permits("anything-latest"));
        assert!(scope.permits("other"));
        assert!(!AliasScope::parse(["gpt-*"]).unwrap().permits("GPT-4O"));
    }

    #[test]
    fn invalid_patterns_are_rejected() {
        for pattern in ["", "*middle*", "foo*bar", "**", "foo**"] {
            assert!(AliasScope::parse([pattern]).is_err(), "{pattern}");
        }
    }

    #[test]
    fn an_empty_scope_permits_nothing() {
        assert!(
            !AliasScope::parse(Vec::<String>::new())
                .unwrap()
                .permits("anything")
        );
    }

    #[test]
    fn prefix_does_not_subsume_other_globs() {
        let ceiling = AliasScope::parse(["gpt-*"]).unwrap();
        let any = AliasScope::parse(["*"]).unwrap();
        let other = AliasScope::parse(["claude-*"]).unwrap();
        let exact = AliasScope::parse(["gpt-4o"]).unwrap();
        assert!(!ceiling.subsumes(&any.patterns[0]));
        assert!(!ceiling.subsumes(&other.patterns[0]));
        assert!(ceiling.subsumes(&exact.patterns[0]));
        assert!(exact.is_subset_of(&ceiling));
    }
}
