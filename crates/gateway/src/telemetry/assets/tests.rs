//! The gate itself, in three groups:
//!
//! * **The shipped assets.** Every dashboard panel and alert rule under
//!   `ops/observability/` is checked against the catalogue, and the runbook is
//!   checked against the rules in both directions.
//! * **Drift cases.** Hand-written assets that must be *refused*: a renamed
//!   metric, a label an instrument does not declare, a value outside a closed
//!   vocabulary, an unbounded drill-down, an alert without a runbook.
//! * **The lexer and the YAML subset.** The two pieces of machinery the checks
//!   above rest on, tested directly so a failure names the cause.

use std::fs;
use std::path::{Path, PathBuf};

use super::*;

/// The repository root, reached from the package directory. `ops/` and `docs/`
/// sit outside the published package, so these are read at test time rather than
/// embedded.
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(relative: &str) -> String {
    let path = root().join(relative);
    fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("{} is readable: {error}", path.display()))
}

const DASHBOARDS: &[&str] = &[
    "ops/observability/dashboards/axond-fleet.json",
    "ops/observability/dashboards/axond-tenancy.json",
];
const RULES: &str = "ops/observability/alerts/axond-alerts.yml";
const RUNBOOK: &str = "docs/operations/observability-runbook.md";

fn anchors() -> BTreeSet<String> {
    let anchors = runbook_anchors(&read(RUNBOOK));
    assert!(
        anchors.len() >= 12,
        "the runbook documents {} failure modes, which is fewer than the operational surface it covers",
        anchors.len()
    );
    anchors
}

// ---------------------------------------------------------------- shipped assets

/// The catalogue has to be self-consistent before anything is checked against
/// it, or a drift failure could be the catalogue's own.
#[test]
fn the_catalogue_is_self_consistent() {
    assert_eq!(catalog::validate_catalog(), Vec::new());
}

#[test]
fn every_shipped_dashboard_references_only_catalogued_metrics() {
    let anchors = anchors();
    for dashboard in DASHBOARDS {
        let failures = validate_dashboard(dashboard, &read(dashboard), &anchors);
        assert_eq!(
            failures,
            Vec::new(),
            "{dashboard} drifted from the catalogue"
        );
    }
}

#[test]
fn every_shipped_alert_rule_references_only_catalogued_metrics() {
    let anchors = anchors();
    let (failures, _) = validate_rules(RULES, &read(RULES), &anchors);
    assert_eq!(failures, Vec::new(), "{RULES} drifted from the catalogue");
}

/// The acceptance criterion behind this gate: a documented failure mode with no
/// alert is a gap, and an alert pointing at a section nobody wrote is a page with
/// no first response. Both directions are checked, so neither file can be edited
/// alone.
#[test]
fn every_documented_failure_mode_has_an_alert_and_every_alert_has_a_runbook() {
    let anchors = anchors();
    let (failures, covered) = validate_rules(RULES, &read(RULES), &anchors);
    assert_eq!(failures, Vec::new());
    assert_eq!(uncovered_failure_modes(&anchors, &covered), Vec::new());
}

/// Cardinality, from the asset side: the only drill-downs offered are the four
/// configured dimensions. A variable over a closed or numeric label would be a
/// dashboard control the metrics cannot answer; one over an identity label could
/// not exist, because the catalogue refuses those outright.
#[test]
fn dashboard_drill_downs_stay_within_the_configured_dimensions() {
    let tenancy: Value = serde_json::from_str(&read(DASHBOARDS[1])).expect("valid JSON");
    let variables = tenancy["templating"]["list"]
        .as_array()
        .expect("the tenancy dashboard declares variables");
    let names: Vec<&str> = variables
        .iter()
        .map(|variable| variable["name"].as_str().expect("a named variable"))
        .collect();
    assert_eq!(
        names,
        vec!["namespace", "alias", "provider", "target_model"]
    );
    for variable in variables {
        let definition = variable["definition"].as_str().expect("a definition");
        assert!(
            definition.starts_with("label_values("),
            "variable `{definition}` is not a label query"
        );
        assert_eq!(
            validate_drill_down("tenancy", variable["name"].as_str().unwrap(), definition),
            Vec::new()
        );
    }
}

/// The identity dimensions are refused as metric labels, so no asset can select
/// on them even by accident. This asserts the property from the asset side: a
/// panel that tried would fail this gate.
#[test]
fn an_asset_cannot_select_on_an_identity_dimension() {
    for (key, _) in catalog::FORBIDDEN_LABEL_KEYS {
        let prometheus = key.replace('.', "_");
        let expr = format!("sum(rate(axond_request_count{{{prometheus}=\"x\"}}[5m]))");
        let failures = validate_expression("hostile", &expr);
        assert!(
            failures.iter().any(|failure| matches!(
                failure,
                AssetError::Catalog {
                    source: catalog::CatalogError::UndeclaredLabel { .. },
                    ..
                }
            )),
            "selecting on `{key}` was accepted"
        );
    }
}

/// Rules must be loadable by Prometheus, which means our YAML subset is not a
/// private dialect: the file has to be ordinary rule-file YAML. This asserts the
/// shape a `promtool check rules` run would, for the parts a parser can see.
#[test]
fn the_rule_file_has_the_shape_prometheus_expects() {
    let document = parse_yaml(&read(RULES)).expect("the rule file parses");
    let groups = document
        .get("groups")
        .and_then(Yaml::as_sequence)
        .expect("groups");
    assert!(groups.len() >= 5, "rules are grouped by concern");
    for group in groups {
        assert!(group.get("name").and_then(Yaml::as_str).is_some());
        assert!(
            group
                .get("interval")
                .and_then(Yaml::as_str)
                .is_some_and(|interval| interval.ends_with('s') || interval.ends_with('m')),
            "every group paces itself"
        );
        for rule in group
            .get("rules")
            .and_then(Yaml::as_sequence)
            .expect("rules")
        {
            let alert = rule.get("alert").and_then(Yaml::as_str).expect("an alert");
            assert!(
                alert.starts_with("Axond"),
                "`{alert}` does not carry the product prefix a shared Alertmanager needs"
            );
        }
    }
}

// ---------------------------------------------------------------- drift cases

fn dashboard_with(expr: &str) -> String {
    serde_json::json!({
        "__inputs": [{"name": "DS_PROMETHEUS", "pluginId": "prometheus"}],
        "uid": "test",
        "title": "test",
        "schemaVersion": 39,
        "links": [{"url": RUNBOOK_URL}],
        "panels": [{
            "type": "timeseries",
            "title": "panel",
            "targets": [{"expr": expr}],
        }],
    })
    .to_string()
}

fn dashboard_failures(expr: &str) -> Vec<AssetError> {
    validate_dashboard("drift", &dashboard_with(expr), &BTreeSet::new())
}

/// The failure this whole module exists for: an instrument is renamed in the
/// catalogue and a dashboard keeps graphing the old name, which renders as an
/// empty panel rather than as an error.
#[test]
fn a_renamed_metric_is_refused() {
    let failures = dashboard_failures("sum(rate(axond_request_counts[5m]))");
    assert!(matches!(
        failures.as_slice(),
        [AssetError::UnknownFamily { name, .. }] if name == "axond_request_counts"
    ));
}

/// The exporter's default suffixes are the other half of that trap: the same
/// series under a different name, which looks correct and matches nothing.
#[test]
fn the_suffixed_spelling_of_a_counter_is_refused() {
    let failures = dashboard_failures("sum(rate(axond_request_count_total[5m]))");
    assert!(matches!(
        failures.as_slice(),
        [AssetError::UnknownFamily { name, .. }] if name == "axond_request_count_total"
    ));
}

#[test]
fn a_histogram_is_selected_through_its_families() {
    assert_eq!(
        dashboard_failures(
            "histogram_quantile(0.95, sum by (le) (rate(axond_request_duration_bucket[10m])))"
        ),
        Vec::new()
    );
    assert_eq!(
        dashboard_failures("sum(rate(axond_request_duration_count[10m]))"),
        Vec::new()
    );
    // The bare instrument name is not a series a histogram exports.
    assert!(matches!(
        dashboard_failures("sum(rate(axond_request_duration[10m]))").as_slice(),
        [AssetError::UnknownFamily { .. }]
    ));
}

#[test]
fn a_label_the_instrument_does_not_declare_is_refused() {
    let failures =
        dashboard_failures("max(axond_status_component_state{axond_namespace=\"acme\"})");
    assert!(matches!(
        failures.as_slice(),
        [AssetError::Catalog {
            source: catalog::CatalogError::UndeclaredLabel { metric, key },
            ..
        }] if metric == "axond.status.component_state" && key == "axond_namespace"
    ));
}

#[test]
fn a_value_outside_a_closed_vocabulary_is_refused() {
    let failures =
        dashboard_failures("max(axond_status_component_state{axond_status_component=\"kafka\"})");
    assert!(matches!(
        failures.as_slice(),
        [AssetError::Catalog {
            source: catalog::CatalogError::UnknownLabelValue { value, .. },
            ..
        }] if value == "kafka"
    ));
    assert_eq!(
        dashboard_failures(
            "max(axond_status_component_state{axond_status_component=\"budget_store\"})"
        ),
        Vec::new()
    );
}

/// A dashboard variable is substituted before the query runs, so a matcher
/// against one cannot be checked against a vocabulary — and must not be refused
/// for it either.
#[test]
fn a_matcher_against_a_dashboard_variable_is_accepted() {
    assert_eq!(
        dashboard_failures("sum(rate(axond_request_count{axond_namespace=~\"$namespace\"}[5m]))"),
        Vec::new()
    );
}

/// The other half of a drifting breakdown: the metric exists, the label does
/// not, and the aggregation quietly returns one series instead of the per-tenant
/// split the panel title promises.
#[test]
fn grouping_by_a_label_the_instrument_does_not_declare_is_refused() {
    let failures =
        dashboard_failures("sum by (axond_namespace) (rate(axond_status_refreshes[5m]))");
    assert!(matches!(
        failures.as_slice(),
        [AssetError::Catalog {
            source: catalog::CatalogError::UndeclaredLabel { key, .. },
            ..
        }] if key == "axond_namespace"
    ));
    assert_eq!(
        dashboard_failures("sum by (axond_status_component) (rate(axond_status_refreshes[5m]))"),
        Vec::new()
    );
}

/// The grouping label belongs to the arm that aggregates it, not to the
/// expression. A ratio whose numerator can carry the breakdown does not license
/// the denominator, which cannot.
#[test]
fn a_compound_expression_is_judged_arm_by_arm() {
    let failures = dashboard_failures(
        "sum by (axond_namespace) (rate(axond_request_count[5m])) / sum by (axond_namespace) (rate(axond_status_refreshes[5m]))",
    );
    assert!(
        matches!(
            failures.as_slice(),
            [AssetError::Catalog {
                source: catalog::CatalogError::UndeclaredLabel { metric, key },
                ..
            }] if metric == "axond.status.refreshes" && key == "axond_namespace"
        ),
        "{failures:?}"
    );
    // The same two instruments, with the breakdown only on the arm that declares
    // it: the ungrouped arm is not asked to carry a label it never groups by.
    assert_eq!(
        dashboard_failures(
            "sum by (axond_namespace) (rate(axond_request_count[5m])) / sum(rate(axond_status_refreshes[5m]))"
        ),
        Vec::new()
    );
}

/// A grouping label over series we do not catalogue — a recording rule, a
/// `vector(0)` guard — is not ours to judge, and must not be refused as drift.
/// The expression is still refused for naming nothing of ours, which is the
/// separate check.
#[test]
fn grouping_over_series_we_do_not_catalogue_is_not_drift() {
    assert!(matches!(
        dashboard_failures("sum by (namespace) (job:axond_requests:rate5m) or vector(0)")
            .as_slice(),
        [AssetError::NoMetricReference { .. }]
    ));
    assert_eq!(
        dashboard_failures(
            "sum by (axond_status_component) (rate(axond_status_refreshes[5m])) or on() vector(0)"
        ),
        Vec::new()
    );
}

#[test]
fn a_panel_that_queries_nothing_of_ours_is_refused() {
    assert!(matches!(
        dashboard_failures("vector(1)").as_slice(),
        [AssetError::NoMetricReference { .. }]
    ));
}

#[test]
fn a_dashboard_that_hard_codes_its_datasource_is_refused() {
    let source = serde_json::json!({
        "uid": "test",
        "title": "test",
        "schemaVersion": 39,
        "links": [{"url": RUNBOOK_URL}],
        "panels": [],
    })
    .to_string();
    let failures = validate_dashboard("pinned", &source, &BTreeSet::new());
    assert!(
        failures
            .iter()
            .any(|failure| failure.to_string().contains("not portable")),
        "{failures:?}"
    );
}

#[test]
fn an_unbounded_drill_down_is_refused() {
    let failures = validate_drill_down(
        "drift",
        "component",
        "label_values(axond_status_component_state, axond_status_component)",
    );
    assert!(matches!(
        failures.as_slice(),
        [AssetError::UnboundedDrillDown { label, class, .. }]
            if label == "axond.status.component" && *class == "closed"
    ));
}

fn rules_with(body: &str) -> String {
    format!(
        "groups:\n  - name: test\n    interval: 30s\n    rules:\n      - alert: AxondTest\n{body}"
    )
}

#[test]
fn an_alert_without_a_runbook_link_is_refused() {
    let source = rules_with(
        "        expr: \"max(axond_revision_lag) > 1000\"\n        for: 5m\n        labels:\n          severity: warning\n        annotations:\n          summary: \"s\"\n          description: \"d\"\n",
    );
    let (failures, covered) = validate_rules("drift", &source, &BTreeSet::new());
    assert!(covered.is_empty());
    assert!(
        failures
            .iter()
            .any(|failure| failure.to_string().contains("no `runbook_url`")),
        "{failures:?}"
    );
}

#[test]
fn an_alert_pointing_at_a_missing_runbook_section_is_refused() {
    let source = rules_with(&format!(
        "        expr: \"max(axond_revision_lag) > 1000\"\n        for: 5m\n        labels:\n          severity: warning\n        annotations:\n          summary: \"s\"\n          description: \"d\"\n          runbook_url: \"{RUNBOOK_URL}#a-section-nobody-wrote\"\n"
    ));
    let (failures, _) = validate_rules("drift", &source, &BTreeSet::new());
    assert!(matches!(
        failures.as_slice(),
        [AssetError::UnknownRunbookAnchor { anchor, .. }] if anchor == "a-section-nobody-wrote"
    ));
}

#[test]
fn an_alert_without_a_hold_window_or_severity_is_refused() {
    let source = rules_with(
        "        expr: \"max(axond_revision_lag) > 1000\"\n        annotations:\n          summary: \"s\"\n          description: \"d\"\n",
    );
    let (failures, _) = validate_rules("drift", &source, &BTreeSet::new());
    let text: Vec<String> = failures.iter().map(ToString::to_string).collect();
    assert!(
        text.iter().any(|failure| failure.contains("`for` window")),
        "{text:?}"
    );
    assert!(
        text.iter().any(|failure| failure.contains("no severity")),
        "{text:?}"
    );
}

#[test]
fn a_documented_failure_mode_with_no_rule_is_reported() {
    let anchors = BTreeSet::from(["a-dependency-is-impaired".to_owned(), "orphan".to_owned()]);
    let covered = BTreeSet::from(["a-dependency-is-impaired".to_owned()]);
    assert!(matches!(
        uncovered_failure_modes(&anchors, &covered).as_slice(),
        [AssetError::UncoveredFailureMode { anchor }] if anchor == "orphan"
    ));
}

// ---------------------------------------------------------------- machinery

/// The three distinctions the lexer has to make, and the one it must not: a
/// grouping label is not a metric name, a function is not a metric name, and a
/// quoted regex is not either.
#[test]
fn the_lexer_separates_metrics_from_grouping_labels_and_functions() {
    let found = selectors(
        "histogram_quantile(0.95, sum by (le, axond_namespace) (rate(axond_request_duration_bucket{axond_status=\"ok\"}[10m]))) / 1e6",
    )
    .expect("lexes");
    assert_eq!(
        found
            .selectors
            .iter()
            .map(|selector| selector.family.as_str())
            .collect::<Vec<_>>(),
        vec!["axond_request_duration_bucket"]
    );
    assert_eq!(
        found
            .grouping
            .iter()
            .map(|grouping| (grouping.label.as_str(), grouping.selectors.as_slice()))
            .collect::<Vec<_>>(),
        vec![("le", &[0usize][..]), ("axond_namespace", &[0usize][..])]
    );
    assert_eq!(
        found.selectors[0].matchers,
        vec![Matcher {
            key: "axond_status".to_owned(),
            literal: Some("ok".to_owned()),
        }]
    );
}

/// `on`/`ignoring` after an identifier is vector matching on a *metric*, not an
/// aggregation's grouping, so the left-hand series is still selected — and still
/// checked. Reading it as a call would hide it from the gate, or refuse the
/// expression for naming nothing of ours.
#[test]
fn a_metric_matched_against_another_vector_is_still_validated() {
    assert_eq!(
        dashboard_failures("axond_request_count or on() vector(0)"),
        Vec::new()
    );
    assert_eq!(
        dashboard_failures(
            "axond_request_count / on(axond_namespace) group_left() axond_cost_microdollars"
        ),
        Vec::new()
    );
    let failures = dashboard_failures("axond_request_counts or on() vector(0)");
    assert!(
        matches!(
            failures.as_slice(),
            [AssetError::UnknownFamily { name, .. }] if name == "axond_request_counts"
        ),
        "{failures:?}"
    );
    // The label list still binds: vector matching on a label the left-hand
    // instrument does not declare is the same drift as an undeclared grouping.
    // Including across a `group_left()`, whose empty argument list sits between
    // the labels and the series they apply to and must not absorb them.
    for expr in [
        "axond_status_refreshes / on(axond_namespace) axond_request_count",
        "axond_status_refreshes / on(axond_namespace) group_left() axond_request_count",
    ] {
        let failures = dashboard_failures(expr);
        assert!(
            matches!(
                failures.as_slice(),
                [AssetError::Catalog {
                    source: catalog::CatalogError::UndeclaredLabel { metric, key },
                    ..
                }] if metric == "axond.status.refreshes" && key == "axond_namespace"
            ),
            "{expr}: {failures:?}"
        );
    }
}

/// `without` and `ignoring` name the labels a result *drops*, so naming one the
/// instrument never declared is legal — `sum without (le)` over a histogram is
/// the ordinary spelling. Only `by` and `on` keep the labels they name, and only
/// those have to be declared.
#[test]
fn an_excluded_label_does_not_have_to_be_one_the_instrument_declares() {
    for expr in [
        "sum without (axond_namespace) (rate(axond_status_refreshes[5m]))",
        "sum without (le) (rate(axond_request_duration_bucket[5m]))",
        "axond_status_refreshes / ignoring(axond_namespace) axond_status_refreshes",
    ] {
        assert_eq!(dashboard_failures(expr), Vec::new(), "{expr}");
    }
    // The kept-label modifiers are unaffected: the same label under `by` is still
    // drift, since the result would carry a dimension the instrument has not got.
    let failures =
        dashboard_failures("sum by (axond_namespace) (rate(axond_status_refreshes[5m]))");
    assert!(
        matches!(
            failures.as_slice(),
            [AssetError::Catalog {
                source: catalog::CatalogError::UndeclaredLabel { metric, .. },
                ..
            }] if metric == "axond.status.refreshes"
        ),
        "{failures:?}"
    );
}

/// A Prometheus name is ASCII. A stray letter from somewhere else has to be a
/// refusal with a message: consuming nothing and continuing would spin.
#[test]
fn the_lexer_refuses_a_character_a_prometheus_name_cannot_contain() {
    let message = selectors("sum(rate(axond_requést_count[5m]))").expect_err("refuses");
    assert!(message.contains("unexpected character"), "{message}");
    assert!(matches!(
        dashboard_failures("sum(rate(axond_requést_count[5m]))").as_slice(),
        [AssetError::Malformed { .. }]
    ));
}

#[test]
fn the_lexer_reads_every_matcher_operator() {
    let found = selectors(
        "axond_http_server_requests{http_response_status_code=~\"5..\", http_request_method!=\"GET\", http_route=\"/v1/models\"}",
    )
    .expect("lexes");
    let matchers = &found.selectors[0].matchers;
    assert_eq!(matchers.len(), 3);
    // Only a literal equality can be checked against a vocabulary.
    assert_eq!(
        matchers
            .iter()
            .filter(|matcher| matcher.literal.is_some())
            .count(),
        1
    );
}

#[test]
fn runbook_anchors_come_only_from_the_failure_modes_section() {
    let runbook = "# Title\n\n## Where to look\n\n### Not a failure mode\n\n## Failure modes\n\n### A dependency is impaired\n\n### The fleet is split across revisions\n\n## Bounded drill-down\n\n### Also not one\n";
    assert_eq!(
        runbook_anchors(runbook),
        BTreeSet::from([
            "a-dependency-is-impaired".to_owned(),
            "the-fleet-is-split-across-revisions".to_owned(),
        ])
    );
}

#[test]
fn the_yaml_subset_parses_nested_mappings_and_sequences() {
    let document = parse_yaml(
        "groups:\n  - name: one\n    rules:\n      - alert: A\n        labels:\n          severity: warning\n      - alert: B\n        expr: \"a: b\"\n  - name: two\n    rules:\n      - alert: C\n",
    )
    .expect("parses");
    let groups = document.get("groups").and_then(Yaml::as_sequence).unwrap();
    assert_eq!(groups.len(), 2);
    let rules = groups[0].get("rules").and_then(Yaml::as_sequence).unwrap();
    assert_eq!(rules.len(), 2);
    assert_eq!(
        rules[0]
            .get("labels")
            .and_then(|labels| labels.get("severity"))
            .and_then(Yaml::as_str),
        Some("warning")
    );
    // A colon inside a quoted scalar is not a key separator.
    assert_eq!(rules[1].get("expr").and_then(Yaml::as_str), Some("a: b"));
    assert_eq!(
        groups[1]
            .get("rules")
            .and_then(Yaml::as_sequence)
            .map(<[Yaml]>::len),
        Some(1)
    );
}

/// The parser refuses what it does not understand instead of half-reading it: a
/// silently ignored anchor or block scalar would be a rule nobody checked.
#[test]
fn the_yaml_subset_refuses_what_it_does_not_implement() {
    for source in [
        "groups:\n\t- name: tabbed\n",
        "base: &anchor\n  name: one\n",
        "expr: |\n  multi\n  line\n",
        "groups:\n",
        "not a mapping\n",
    ] {
        assert!(
            parse_yaml(source).is_err(),
            "`{source}` was accepted by the subset parser"
        );
    }
}
