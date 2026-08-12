//! The drift gate for the shipped observability assets.
//!
//! `ops/observability/` holds Grafana dashboards and Prometheus rules that name
//! metrics, labels, and label values. Those names live twice — once in
//! [`catalog`](super::catalog) inside the binary, once in an asset an operator
//! imports — and two copies of an interface is a drift hazard: a renamed
//! instrument leaves a dashboard silently graphing nothing and an alert that can
//! never fire, which is worse than no alert at all because it looks like
//! coverage.
//!
//! So the assets are checked against the catalogue rather than against a review.
//! Everything here is pure: callers read the files and pass their contents in,
//! which is what lets the same functions check the shipped assets in
//! [`tests`](self::tests) and check hand-written drift cases beside them.
//!
//! Three things are checked:
//!
//! * **Every metric reference resolves.** A PromQL expression's metric
//!   identifiers must be Prometheus spellings of catalogued instruments, its
//!   label matchers must be labels those instruments declare, and a matcher on a
//!   closed vocabulary must name a value in it.
//! * **Drill-down stays bounded.** A dashboard variable must be a
//!   `label_values` query over a [`LabelClass::Configured`] label — the four
//!   dimensions that grow with an operator's own configuration — so a dashboard
//!   cannot offer a drill-down the metrics refuse to carry.
//! * **Every failure mode has a rule, and every rule has a runbook.** Each alert
//!   carries a `runbook_url` whose anchor must exist in the runbook, and each
//!   failure mode in the runbook must be named by at least one alert.
//!
//! ## The name translation
//!
//! axond exports OTLP, so Prometheus-side names come from the collector. The
//! assets assume `add_metric_suffixes: false`, which makes the mapping total and
//! reversible: dots become underscores, histograms gain the usual `_bucket`,
//! `_sum`, and `_count` families, and nothing else is appended.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use super::catalog::{self, CATALOG, LabelClass, MetricSpec};

/// Where a rule's `runbook_url` must point.
pub const RUNBOOK_URL: &str =
    "https://github.com/Litvue/axond/blob/main/docs/operations/observability-runbook.md";

/// The severities a rule may declare. A third value would be a routing decision
/// nobody configured a receiver for.
const SEVERITIES: &[&str] = &["critical", "warning"];

/// PromQL words that are not metric names: operators, aggregation modifiers, and
/// the `bool` result modifier. Function names are recognised structurally (an
/// identifier followed by `(`) rather than enumerated.
const KEYWORDS: &[&str] = &[
    "and",
    "or",
    "unless",
    "by",
    "without",
    "on",
    "ignoring",
    "group_left",
    "group_right",
    "bool",
    "offset",
    "atan2",
];

/// Why an asset was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AssetError {
    #[error("{asset}: {message}")]
    Malformed { asset: String, message: String },
    #[error(
        "{asset}: expression `{expr}` references `{name}`, which is not a Prometheus spelling of any catalogued metric"
    )]
    UnknownFamily {
        asset: String,
        expr: String,
        name: String,
    },
    #[error("{asset}: expression `{expr}` references no catalogued metric at all")]
    NoMetricReference { asset: String, expr: String },
    #[error("{asset}: {source}")]
    Catalog {
        asset: String,
        #[source]
        source: catalog::CatalogError,
    },
    #[error(
        "{asset}: variable `{variable}` drills down on `{label}`, which is `{class}` rather than a configured dimension"
    )]
    UnboundedDrillDown {
        asset: String,
        variable: String,
        label: String,
        class: &'static str,
    },
    #[error(
        "{asset}: alert `{alert}` points at runbook anchor `{anchor}`, which the runbook does not define"
    )]
    UnknownRunbookAnchor {
        asset: String,
        alert: String,
        anchor: String,
    },
    #[error(
        "runbook failure mode `{anchor}` has no alert rule; every documented failure mode owes a signal, an alert, and a first response"
    )]
    UncoveredFailureMode { anchor: String },
}

impl AssetError {
    fn malformed(asset: &str, message: impl Into<String>) -> Self {
        Self::Malformed {
            asset: asset.to_owned(),
            message: message.into(),
        }
    }
}

/// The Prometheus families a catalogued instrument exports, in the translation
/// documented above.
pub fn families(spec: &MetricSpec) -> Vec<String> {
    let base = spec.name.replace('.', "_");
    match spec.kind {
        catalog::InstrumentKind::Histogram => ["_bucket", "_sum", "_count"]
            .iter()
            .map(|suffix| format!("{base}{suffix}"))
            .collect(),
        _ => vec![base],
    }
}

/// Every Prometheus family name the catalogue can produce, mapped back to the
/// instrument that produces it.
fn family_index() -> BTreeMap<String, &'static MetricSpec> {
    let mut index = BTreeMap::new();
    for spec in CATALOG {
        for family in families(spec) {
            index.insert(family, spec);
        }
    }
    index
}

/// The class name that appears in a refusal, so the message says *why* a
/// dimension is not drillable rather than only that it is not.
fn class_name(class: LabelClass) -> &'static str {
    match class {
        LabelClass::Closed => "closed",
        LabelClass::Numeric => "numeric",
        LabelClass::Route => "route",
        LabelClass::Configured => "configured",
    }
}

/// The canonical label key a Prometheus label key came from, when the instrument
/// declares one.
fn canonical_label(spec: &MetricSpec, prometheus_key: &str) -> Option<&'static str> {
    spec.labels
        .iter()
        .find(|label| label.key.replace('.', "_") == prometheus_key)
        .map(|label| label.key)
}

/// One metric selector found in an expression: the family, and the label
/// matchers written against it.
#[derive(Debug, PartialEq, Eq)]
struct Selector {
    family: String,
    matchers: Vec<Matcher>,
}

/// What an expression names: the series it selects, and the labels it aggregates
/// by. Both drift, and a grouping label the aggregated instrument does not
/// declare collapses a panel into one meaningless series rather than failing.
#[derive(Debug, Default, PartialEq, Eq)]
struct Expression {
    selectors: Vec<Selector>,
    grouping: Vec<Grouping>,
}

/// One label in an aggregation modifier, bound to the selectors that aggregation
/// actually applies to. A compound expression aggregates each of its arms
/// separately — `sum by (a) (x) / sum(y)` groups `x` and not `y` — so a grouping
/// label is only answerable against the arm it modifies.
#[derive(Debug, PartialEq, Eq)]
struct Grouping {
    label: String,
    /// Indexes into [`Expression::selectors`].
    selectors: Vec<usize>,
}

/// An open aggregation body: the labels its modifier named, and the selectors
/// found inside it so far.
#[derive(Debug)]
struct Scope {
    labels: Vec<String>,
    body_depth: usize,
    selectors: Vec<usize>,
}

/// The histogram bucket boundary: carried by every histogram, declared by none.
const IMPLICIT_LABELS: &[&str] = &["le"];

#[derive(Debug, PartialEq, Eq)]
struct Matcher {
    key: String,
    /// Whether the matcher is an equality on a literal value, which is the only
    /// shape whose value can be checked against a closed vocabulary. A regex
    /// matcher or a dashboard variable is accepted without a value check.
    literal: Option<String>,
}

/// Pull every metric selector out of a PromQL expression.
///
/// This is deliberately a lexer rather than a parser: the question asked of an
/// expression is only "which series does it name", and answering it needs the
/// three distinctions a lexer can make — an identifier followed by `(` is a
/// function, identifiers inside a `by (...)`/`on (...)` list are label keys, and
/// identifiers inside `{...}` are matchers. Everything else is a metric name.
fn selectors(expr: &str) -> Result<Expression, String> {
    let bytes: Vec<char> = expr.chars().collect();
    let mut index = 0;
    let mut found = Expression::default();
    // Set when the previous identifier was an aggregation modifier, so the
    // identifiers in the parenthesised list that follows are label keys.
    let mut grouping_depth: Option<usize> = None;
    // The labels of the modifier list being read, then of a closed list waiting
    // for the body it modifies.
    let mut reading: Vec<String> = Vec::new();
    let mut pending: Vec<String> = Vec::new();
    let mut open: Vec<Scope> = Vec::new();
    let mut depth = 0usize;

    while index < bytes.len() {
        let character = bytes[index];
        match character {
            '(' => {
                depth += 1;
                if !pending.is_empty() {
                    open.push(Scope {
                        labels: std::mem::take(&mut pending),
                        body_depth: depth,
                        selectors: Vec::new(),
                    });
                }
                index += 1;
            }
            ')' => {
                depth = depth.saturating_sub(1);
                if grouping_depth == Some(depth + 1) {
                    grouping_depth = None;
                    pending = std::mem::take(&mut reading);
                }
                while open.last().is_some_and(|scope| scope.body_depth > depth) {
                    close_scope(&mut open, &mut found.grouping);
                }
                index += 1;
            }
            '"' | '\'' => {
                let quote = character;
                index += 1;
                while index < bytes.len() && bytes[index] != quote {
                    index += if bytes[index] == '\\' { 2 } else { 1 };
                }
                index += 1;
            }
            '{' => {
                let close = bytes[index..]
                    .iter()
                    .position(|character| *character == '}')
                    .ok_or_else(|| format!("unterminated label matcher in `{expr}`"))?;
                let body: String = bytes[index + 1..index + close].iter().collect();
                let matchers = parse_matchers(&body)?;
                match found.selectors.last_mut() {
                    Some(selector) if selector.matchers.is_empty() => selector.matchers = matchers,
                    _ => return Err(format!("label matcher with no metric name in `{expr}`")),
                }
                index += close + 1;
            }
            character if character.is_ascii_digit() => {
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == '.')
                {
                    index += 1;
                }
            }
            character if character.is_alphabetic() || character == '_' || character == ':' => {
                let start = index;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric()
                        || bytes[index] == '_'
                        || bytes[index] == ':')
                {
                    index += 1;
                }
                let word: String = bytes[start..index].iter().collect();
                if opens_a_call(&bytes[index..]) {
                    if matches!(word.as_str(), "by" | "without" | "on" | "ignoring") {
                        grouping_depth = Some(depth + 1);
                    }
                    continue;
                }
                if grouping_depth.is_some() {
                    reading.push(word);
                    continue;
                }
                if KEYWORDS.contains(&word.as_str()) {
                    continue;
                }
                let position = found.selectors.len();
                found.selectors.push(Selector {
                    family: word,
                    matchers: Vec::new(),
                });
                for scope in &mut open {
                    scope.selectors.push(position);
                }
            }
            _ => index += 1,
        }
    }
    while !open.is_empty() {
        close_scope(&mut open, &mut found.grouping);
    }
    // A trailing modifier — `sum(x) by (a)` — names its labels after the body it
    // modifies, so there is no scope left to bind them to. Bind them to every
    // selector in the expression, which is the strictest reading available.
    if !pending.is_empty() {
        let every = (0..found.selectors.len()).collect::<Vec<_>>();
        for label in pending {
            found.grouping.push(Grouping {
                label,
                selectors: every.clone(),
            });
        }
    }
    Ok(found)
}

/// Retire the innermost open aggregation body, recording one [`Grouping`] per
/// label it named.
fn close_scope(open: &mut Vec<Scope>, grouping: &mut Vec<Grouping>) {
    let Some(scope) = open.pop() else {
        return;
    };
    for label in scope.labels {
        grouping.push(Grouping {
            label,
            selectors: scope.selectors.clone(),
        });
    }
}

/// Whether an identifier is a function or aggregation name rather than a metric
/// name: it is followed by its argument list, or by the aggregation modifier that
/// precedes one (`sum by (...) (...)`).
fn opens_a_call(rest: &[char]) -> bool {
    let mut index = 0;
    while index < rest.len() && rest[index].is_whitespace() {
        index += 1;
    }
    if rest.get(index) == Some(&'(') {
        return true;
    }
    let start = index;
    while index < rest.len() && (rest[index].is_ascii_alphabetic() || rest[index] == '_') {
        index += 1;
    }
    let word: String = rest[start..index].iter().collect();
    matches!(word.as_str(), "by" | "without" | "on" | "ignoring")
}

/// Split a `{...}` body into matchers. Values are always quoted in PromQL, which
/// is what makes splitting on commas outside quotes sufficient.
fn parse_matchers(body: &str) -> Result<Vec<Matcher>, String> {
    let mut matchers = Vec::new();
    for part in split_outside_quotes(body) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let operator = ["=~", "!~", "!=", "="]
            .into_iter()
            .find(|operator| part.contains(operator))
            .ok_or_else(|| format!("matcher `{part}` has no operator"))?;
        let (key, value) = part
            .split_once(operator)
            .ok_or_else(|| format!("matcher `{part}` has no operator"))?;
        let value = value.trim().trim_matches('"');
        // A dashboard variable is substituted by Grafana before the query runs,
        // so its value cannot be checked against a vocabulary here.
        let literal = (operator == "=" && !value.contains('$')).then(|| value.to_owned());
        matchers.push(Matcher {
            key: key.trim().to_owned(),
            literal,
        });
    }
    Ok(matchers)
}

fn split_outside_quotes(body: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    for character in body.chars() {
        match character {
            '"' => {
                quoted = !quoted;
                current.push(character);
            }
            ',' if !quoted => parts.push(std::mem::take(&mut current)),
            _ => current.push(character),
        }
    }
    parts.push(current);
    parts
}

/// Check one expression against the catalogue.
pub fn validate_expression(asset: &str, expr: &str) -> Vec<AssetError> {
    let index = family_index();
    let mut failures = Vec::new();
    let found = match selectors(expr) {
        Ok(found) => found,
        Err(message) => return vec![AssetError::malformed(asset, message)],
    };
    let mut spelled_like_ours = 0usize;
    // Position in `found.selectors` to the instrument it resolved to, so a
    // grouping label can be asked of the series it actually aggregates.
    let mut selected: BTreeMap<usize, &'static MetricSpec> = BTreeMap::new();
    for (position, selector) in found.selectors.iter().enumerate() {
        if !selector.family.starts_with("axond") {
            // Recording rules, `vector(0)` arguments, and a scrape's own labels
            // are not ours to catalogue. Anything spelled like one of our
            // metrics is.
            continue;
        }
        spelled_like_ours += 1;
        let Some(spec) = index.get(&selector.family) else {
            failures.push(AssetError::UnknownFamily {
                asset: asset.to_owned(),
                expr: expr.to_owned(),
                name: selector.family.clone(),
            });
            continue;
        };
        selected.insert(position, spec);
        for matcher in &selector.matchers {
            let Some(canonical) = canonical_label(spec, &matcher.key) else {
                failures.push(AssetError::Catalog {
                    asset: asset.to_owned(),
                    source: catalog::CatalogError::UndeclaredLabel {
                        metric: spec.name.to_owned(),
                        key: matcher.key.clone(),
                    },
                });
                continue;
            };
            if let Some(literal) = &matcher.literal
                && let Err(error) = catalog::validate_label_value(spec.name, canonical, literal)
            {
                failures.push(AssetError::Catalog {
                    asset: asset.to_owned(),
                    source: error,
                });
            }
        }
    }
    // Every instrument the aggregation groups has to declare the grouping label.
    // Asking only that *some* instrument in the expression declares it would
    // pass a compound expression whose grouped arm cannot carry the breakdown,
    // which silently produces one series where the panel promised a split.
    for grouping in &found.grouping {
        if IMPLICIT_LABELS.contains(&grouping.label.as_str()) {
            continue;
        }
        for position in &grouping.selectors {
            let Some(spec) = selected.get(position) else {
                // Not one of ours, or already refused as an unknown family.
                continue;
            };
            if canonical_label(spec, &grouping.label).is_some() {
                continue;
            }
            failures.push(AssetError::Catalog {
                asset: asset.to_owned(),
                source: catalog::CatalogError::UndeclaredLabel {
                    metric: spec.name.to_owned(),
                    key: grouping.label.clone(),
                },
            });
        }
    }
    if spelled_like_ours == 0 {
        failures.push(AssetError::NoMetricReference {
            asset: asset.to_owned(),
            expr: expr.to_owned(),
        });
    }
    failures
}

/// The anchors a Grafana dashboard or a rule may link to: the failure modes the
/// runbook documents, as GitHub renders their heading anchors.
pub fn runbook_anchors(runbook: &str) -> BTreeSet<String> {
    let mut anchors = BTreeSet::new();
    let mut inside = false;
    for line in runbook.lines() {
        if let Some(heading) = line.strip_prefix("## ") {
            inside = heading.trim() == "Failure modes";
            continue;
        }
        if inside && let Some(heading) = line.strip_prefix("### ") {
            anchors.insert(slug(heading.trim()));
        }
    }
    anchors
}

/// GitHub's heading-anchor slug for the headings this runbook uses: lower-case,
/// spaces to hyphens, punctuation dropped.
fn slug(heading: &str) -> String {
    heading
        .chars()
        .filter_map(|character| match character {
            ' ' => Some('-'),
            character if character.is_alphanumeric() || character == '-' || character == '_' => {
                Some(character.to_ascii_lowercase())
            }
            _ => None,
        })
        .collect()
}

/// Check a Grafana dashboard: it must import without editing, every panel must
/// query something the catalogue declares, and every drill-down variable must
/// stay inside the configured dimensions.
pub fn validate_dashboard(
    asset: &str,
    source: &str,
    anchors: &BTreeSet<String>,
) -> Vec<AssetError> {
    let mut failures = Vec::new();
    let dashboard: Value = match serde_json::from_str(source) {
        Ok(value) => value,
        Err(error) => return vec![AssetError::malformed(asset, error.to_string())],
    };
    for field in ["uid", "title", "schemaVersion", "panels"] {
        if dashboard.get(field).is_none() {
            failures.push(AssetError::malformed(
                asset,
                format!("`{field}` is missing"),
            ));
        }
    }
    // Portability: a dashboard that hard-codes a datasource uid imports into one
    // Grafana and nowhere else.
    let inputs = dashboard
        .get("__inputs")
        .and_then(Value::as_array)
        .map(|inputs| {
            inputs.iter().any(|input| {
                input.get("name").and_then(Value::as_str) == Some("DS_PROMETHEUS")
                    && input.get("pluginId").and_then(Value::as_str) == Some("prometheus")
            })
        })
        .unwrap_or(false);
    if !inputs {
        failures.push(AssetError::malformed(
            asset,
            "no `DS_PROMETHEUS` datasource input, so the dashboard is not portable",
        ));
    }
    if !source.contains(RUNBOOK_URL) {
        failures.push(AssetError::malformed(
            asset,
            "no link to the observability runbook",
        ));
    }

    for variable in dashboard
        .get("templating")
        .and_then(|templating| templating.get("list"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
    {
        let name = variable
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("<unnamed>");
        let Some(definition) = variable.get("definition").and_then(Value::as_str) else {
            failures.push(AssetError::malformed(
                asset,
                format!("variable `{name}` has no `label_values` definition"),
            ));
            continue;
        };
        failures.extend(validate_drill_down(asset, name, definition));
    }

    for panel in panels(&dashboard) {
        let title = panel
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or("<untitled>");
        if panel.get("type").and_then(Value::as_str) == Some("row") {
            continue;
        }
        let targets = panel
            .get("targets")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if targets.is_empty() {
            failures.push(AssetError::malformed(
                asset,
                format!("panel `{title}` queries nothing"),
            ));
        }
        for target in targets {
            match target.get("expr").and_then(Value::as_str) {
                Some(expr) => {
                    failures.extend(validate_expression(&format!("{asset}: {title}"), expr));
                }
                None => failures.push(AssetError::malformed(
                    asset,
                    format!("panel `{title}` has a target with no expression"),
                )),
            }
        }
        for link in panel
            .get("links")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default()
        {
            let url = link.get("url").and_then(Value::as_str).unwrap_or_default();
            if let Some(anchor) = url.strip_prefix(&format!("{RUNBOOK_URL}#"))
                && !anchors.contains(anchor)
            {
                failures.push(AssetError::UnknownRunbookAnchor {
                    asset: asset.to_owned(),
                    alert: title.to_owned(),
                    anchor: anchor.to_owned(),
                });
            }
        }
    }
    failures
}

/// Rows nest their children in Grafana's JSON when collapsed, so a flat walk of
/// `panels` would miss them.
fn panels(dashboard: &Value) -> Vec<&Value> {
    let mut collected = Vec::new();
    let mut queue: Vec<&Value> = dashboard
        .get("panels")
        .and_then(Value::as_array)
        .map(|panels| panels.iter().collect())
        .unwrap_or_default();
    while let Some(panel) = queue.pop() {
        collected.push(panel);
        if let Some(children) = panel.get("panels").and_then(Value::as_array) {
            queue.extend(children.iter());
        }
    }
    collected
}

/// A drill-down variable must be `label_values(<metric>, <label>)` over a
/// configured dimension. Anything else either asks for a series the metrics do
/// not carry, or invites free text into a query.
fn validate_drill_down(asset: &str, variable: &str, definition: &str) -> Vec<AssetError> {
    let Some(arguments) = definition
        .trim()
        .strip_prefix("label_values(")
        .and_then(|rest| rest.strip_suffix(')'))
    else {
        return vec![AssetError::malformed(
            asset,
            format!("variable `{variable}` is not a `label_values` query: `{definition}`"),
        )];
    };
    let Some((family, label)) = arguments.split_once(',') else {
        return vec![AssetError::malformed(
            asset,
            format!("variable `{variable}` names no label: `{definition}`"),
        )];
    };
    let family = family.trim();
    let label = label.trim();
    let index = family_index();
    let Some(spec) = index.get(family) else {
        return vec![AssetError::UnknownFamily {
            asset: asset.to_owned(),
            expr: definition.to_owned(),
            name: family.to_owned(),
        }];
    };
    let Some(canonical) = canonical_label(spec, label) else {
        return vec![AssetError::Catalog {
            asset: asset.to_owned(),
            source: catalog::CatalogError::UndeclaredLabel {
                metric: spec.name.to_owned(),
                key: label.to_owned(),
            },
        }];
    };
    let class = spec
        .label(canonical)
        .map(|label| label.class)
        .unwrap_or(LabelClass::Closed);
    if class != LabelClass::Configured {
        return vec![AssetError::UnboundedDrillDown {
            asset: asset.to_owned(),
            variable: variable.to_owned(),
            label: canonical.to_owned(),
            class: class_name(class),
        }];
    }
    Vec::new()
}

/// Check the Prometheus rule file: every rule's expression against the
/// catalogue, and every rule's shape against what an on-call rotation needs — a
/// severity, a summary, and a runbook anchor that exists.
///
/// Returns the anchors the rules cover, so the caller can assert the reverse
/// direction: a documented failure mode with no rule is a gap this file cannot
/// see on its own.
pub fn validate_rules(
    asset: &str,
    source: &str,
    anchors: &BTreeSet<String>,
) -> (Vec<AssetError>, BTreeSet<String>) {
    let mut failures = Vec::new();
    let mut covered = BTreeSet::new();
    let document = match parse_yaml(source) {
        Ok(document) => document,
        Err(message) => return (vec![AssetError::malformed(asset, message)], covered),
    };
    let Some(groups) = document.get("groups").and_then(Yaml::as_sequence) else {
        return (
            vec![AssetError::malformed(asset, "no `groups` sequence")],
            covered,
        );
    };
    let mut names = BTreeSet::new();
    for group in groups {
        let group_name = group.get("name").and_then(Yaml::as_str).unwrap_or_default();
        if group_name.is_empty() {
            failures.push(AssetError::malformed(asset, "a group has no `name`"));
        }
        let Some(rules) = group.get("rules").and_then(Yaml::as_sequence) else {
            failures.push(AssetError::malformed(
                asset,
                format!("group `{group_name}` has no `rules`"),
            ));
            continue;
        };
        for rule in rules {
            let Some(alert) = rule.get("alert").and_then(Yaml::as_str) else {
                failures.push(AssetError::malformed(
                    asset,
                    format!("group `{group_name}` has a rule that is not an alert"),
                ));
                continue;
            };
            if !names.insert(alert.to_owned()) {
                failures.push(AssetError::malformed(
                    asset,
                    format!("alert `{alert}` is declared twice"),
                ));
            }
            match rule.get("expr").and_then(Yaml::as_str) {
                Some(expr) => {
                    failures.extend(validate_expression(&format!("{asset}: {alert}"), expr))
                }
                None => failures.push(AssetError::malformed(
                    asset,
                    format!("alert `{alert}` has no `expr`"),
                )),
            }
            if rule.get("for").and_then(Yaml::as_str).is_none() {
                failures.push(AssetError::malformed(
                    asset,
                    format!("alert `{alert}` has no `for` window, so a single scrape can page"),
                ));
            }
            match rule
                .get("labels")
                .and_then(|labels| labels.get("severity"))
                .and_then(Yaml::as_str)
            {
                Some(severity) if SEVERITIES.contains(&severity) => {}
                Some(severity) => failures.push(AssetError::malformed(
                    asset,
                    format!("alert `{alert}` declares unknown severity `{severity}`"),
                )),
                None => failures.push(AssetError::malformed(
                    asset,
                    format!("alert `{alert}` declares no severity"),
                )),
            }
            for annotation in ["summary", "description"] {
                if rule
                    .get("annotations")
                    .and_then(|annotations| annotations.get(annotation))
                    .and_then(Yaml::as_str)
                    .is_none_or(str::is_empty)
                {
                    failures.push(AssetError::malformed(
                        asset,
                        format!("alert `{alert}` has no `{annotation}` annotation"),
                    ));
                }
            }
            match rule
                .get("annotations")
                .and_then(|annotations| annotations.get("runbook_url"))
                .and_then(Yaml::as_str)
            {
                Some(url) => match url.strip_prefix(&format!("{RUNBOOK_URL}#")) {
                    Some(anchor) if anchors.contains(anchor) => {
                        covered.insert(anchor.to_owned());
                    }
                    Some(anchor) => failures.push(AssetError::UnknownRunbookAnchor {
                        asset: asset.to_owned(),
                        alert: alert.to_owned(),
                        anchor: anchor.to_owned(),
                    }),
                    None => failures.push(AssetError::malformed(
                        asset,
                        format!("alert `{alert}` links outside the runbook: `{url}`"),
                    )),
                },
                None => failures.push(AssetError::malformed(
                    asset,
                    format!("alert `{alert}` has no `runbook_url`, so it pages without a response"),
                )),
            }
        }
    }
    (failures, covered)
}

/// Which documented failure modes no rule fires on.
pub fn uncovered_failure_modes(
    anchors: &BTreeSet<String>,
    covered: &BTreeSet<String>,
) -> Vec<AssetError> {
    anchors
        .difference(covered)
        .map(|anchor| AssetError::UncoveredFailureMode {
            anchor: anchor.clone(),
        })
        .collect()
}

/// A YAML value, in the subset a Prometheus rule file needs: nested mappings,
/// sequences of mappings, and scalars.
///
/// Hand-written rather than taken from a dependency because the alternative is a
/// YAML implementation in the supply chain of a gateway that never reads YAML at
/// runtime. The parser refuses anything outside the subset instead of
/// interpreting it loosely, so an asset written with a tab, an anchor, or a block
/// scalar fails the gate rather than being half-understood.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Yaml {
    Mapping(BTreeMap<String, Yaml>),
    Sequence(Vec<Yaml>),
    Scalar(String),
}

impl Yaml {
    pub fn get(&self, key: &str) -> Option<&Yaml> {
        match self {
            Self::Mapping(entries) => entries.get(key),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Scalar(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_sequence(&self) -> Option<&[Yaml]> {
        match self {
            Self::Sequence(items) => Some(items),
            _ => None,
        }
    }
}

/// One significant line: its indentation, whether it opens a sequence item, and
/// its content with the dash removed.
struct Line {
    indent: usize,
    item: bool,
    content: String,
    number: usize,
}

/// Parse the supported YAML subset.
pub fn parse_yaml(source: &str) -> Result<Yaml, String> {
    let mut lines = Vec::new();
    for (offset, raw) in source.lines().enumerate() {
        let number = offset + 1;
        if raw.contains('\t') {
            return Err(format!("line {number}: tabs are not valid indentation"));
        }
        let trimmed = raw.trim_start();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let indent = raw.len() - trimmed.len();
        match trimmed.strip_prefix("- ") {
            Some(rest) => lines.push(Line {
                indent,
                item: true,
                content: rest.trim().to_owned(),
                number,
            }),
            None => lines.push(Line {
                indent,
                item: false,
                content: trimmed.to_owned(),
                number,
            }),
        }
    }
    if lines.is_empty() {
        return Err("the document is empty".to_owned());
    }
    let mut cursor = 0;
    let value = parse_block(&lines, &mut cursor, lines[0].indent)?;
    if cursor != lines.len() {
        let line = &lines[cursor];
        return Err(format!(
            "line {}: unexpected indentation `{}`",
            line.number, line.content
        ));
    }
    Ok(value)
}

/// A sequence item's own keys sit two columns in from its dash, which is how the
/// item's mapping is told apart from the sequence that contains it.
const ITEM_INDENT: usize = 2;

fn parse_block(lines: &[Line], cursor: &mut usize, indent: usize) -> Result<Yaml, String> {
    if lines[*cursor].item {
        let mut items = Vec::new();
        while *cursor < lines.len() && lines[*cursor].item && lines[*cursor].indent == indent {
            *cursor += 1;
            let mut entries = BTreeMap::new();
            parse_entry(lines, cursor, indent + ITEM_INDENT, &mut entries, true)?;
            parse_mapping_entries(lines, cursor, indent + ITEM_INDENT, &mut entries)?;
            items.push(Yaml::Mapping(entries));
        }
        return Ok(Yaml::Sequence(items));
    }
    let mut entries = BTreeMap::new();
    parse_mapping_entries(lines, cursor, indent, &mut entries)?;
    Ok(Yaml::Mapping(entries))
}

fn parse_mapping_entries(
    lines: &[Line],
    cursor: &mut usize,
    indent: usize,
    entries: &mut BTreeMap<String, Yaml>,
) -> Result<(), String> {
    while *cursor < lines.len() && lines[*cursor].indent == indent && !lines[*cursor].item {
        parse_entry(lines, cursor, indent, entries, false)?;
    }
    Ok(())
}

/// Consume one `key: value` or `key:` entry, recursing into the block a bare key
/// opens. `first` marks the line that carried the sequence dash, whose recorded
/// indentation is the dash's rather than the key's.
fn parse_entry(
    lines: &[Line],
    cursor: &mut usize,
    indent: usize,
    entries: &mut BTreeMap<String, Yaml>,
    first: bool,
) -> Result<(), String> {
    let line = &lines[*cursor - usize::from(first)];
    let number = line.number;
    let (key, rest) = split_entry(&line.content).ok_or_else(|| {
        format!(
            "line {number}: `{}` is not a `key: value` entry",
            line.content
        )
    })?;
    if !first {
        *cursor += 1;
    }
    if rest.is_empty() {
        if *cursor >= lines.len() || lines[*cursor].indent <= indent {
            return Err(format!("line {number}: `{key}` opens an empty block"));
        }
        let child = parse_block(lines, cursor, lines[*cursor].indent)?;
        entries.insert(key, child);
        return Ok(());
    }
    entries.insert(key, Yaml::Scalar(scalar(&rest, number)?));
    Ok(())
}

/// Split on the first `:` that ends a key: one followed by a space or by nothing.
/// A `:` inside a value — a URL, a PromQL range — is not a key separator.
fn split_entry(content: &str) -> Option<(String, String)> {
    let bytes = content.as_bytes();
    for (index, byte) in bytes.iter().enumerate() {
        if *byte != b':' {
            continue;
        }
        let followed_by_space = bytes.get(index + 1).is_none_or(|next| *next == b' ');
        if followed_by_space {
            let key = content[..index].trim();
            if key.is_empty() {
                return None;
            }
            return Some((key.to_owned(), content[index + 1..].trim().to_owned()));
        }
    }
    None
}

/// A scalar is a double-quoted string or a plain one. Block scalars, anchors,
/// and flow collections are outside the subset.
fn scalar(raw: &str, number: usize) -> Result<String, String> {
    if let Some(rest) = raw.strip_prefix('"') {
        let mut value = String::new();
        let mut characters = rest.chars();
        while let Some(character) = characters.next() {
            match character {
                '\\' => match characters.next() {
                    Some('n') => value.push('\n'),
                    Some(escaped) => value.push(escaped),
                    None => return Err(format!("line {number}: trailing escape")),
                },
                '"' => {
                    let trailing: String = characters.collect();
                    if !trailing.trim().is_empty() {
                        return Err(format!(
                            "line {number}: trailing `{}` after a quoted scalar",
                            trailing.trim()
                        ));
                    }
                    return Ok(value);
                }
                character => value.push(character),
            }
        }
        return Err(format!("line {number}: unterminated quoted scalar"));
    }
    if raw.starts_with(['&', '*', '|', '>', '[', '{']) {
        return Err(format!(
            "line {number}: `{raw}` uses YAML the gate does not accept"
        ));
    }
    Ok(raw.to_owned())
}

#[cfg(test)]
mod tests;
