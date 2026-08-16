//! The #160 integration smoke harness: one scenario per stateful release gate.
//!
//! The stateful contracts land as separate slices — durable schemas, typed
//! documents, protocol boundaries — and each is tested against itself. This
//! suite tests the *seams*: what a deployment does when the pieces are put
//! together, which is where every release gate in
//! [#160](https://github.com/Litvue/axond/issues/160) actually lives.
//!
//! Two rules keep it honest while stateful qualification is still incomplete.
//!
//! **A gate is `Wired` only when its scenario asserts the property on a running
//! process.** Not when its dependencies merged, and not when a type exists.
//!
//! **A `Blocked` gate still runs.** Its scenario records the safe boundary that
//! remains unqualified. The wired scenarios below assert their properties on a
//! running process; the remaining blocked qualification scenario does not get
//! to borrow those results.
//!
//! **A gate is `Partial` when a running process proves the path that exists and
//! a named slice still owns the rest.** IG-11 is the current blocked
//! qualification gate; IG-05 now covers both breakglass and a scoped OIDC human
//! on a running replica.
//!
//! The gate table below and the matrix in
//! `docs/operations/stateful-integration.md` are checked against each other, so
//! neither can drift: an integration pull request that unblocks a gate has to
//! move the row, the status, and the scenario together.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use support::fault::injector::{FaultProxy, Mode, redirect};
use support::schema::{self, Schema};
use support::stateful::{self, ControlPlane};
use support::{GATEWAY_KEY, boot, client};

/// Whether a gate's property is proven by its scenario, or still waiting on the
/// slices named in the matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Status {
    Wired,
    Partial,
    Blocked,
}

impl Status {
    fn parse(text: &str) -> Self {
        match text {
            "wired" => Self::Wired,
            "partial" => Self::Partial,
            "blocked" => Self::Blocked,
            other => panic!("unknown status {other:?} in the acceptance matrix"),
        }
    }
}

struct Gate {
    id: &'static str,
    status: Status,
    scenarios: &'static [&'static str],
}

/// Every #160 release gate this suite is responsible for.
const GATES: &[Gate] = &[
    Gate {
        id: "IG-01",
        status: Status::Wired,
        scenarios: &[
            "stateless_boot_serves_with_no_control_plane",
            "stateful_boot_serves_administration_and_refuses_inference",
            "stateful_boot_refuses_an_unresolved_reference",
        ],
    },
    Gate {
        id: "IG-02",
        status: Status::Wired,
        scenarios: &[
            "preflight_describes_a_stateless_install",
            "migrate_prepares_a_control_plane_before_replicas_start",
        ],
    },
    Gate {
        id: "IG-03",
        status: Status::Wired,
        scenarios: &["stateful_revision_compiles_rotates_and_recovers"],
    },
    Gate {
        id: "IG-04",
        status: Status::Wired,
        scenarios: &["stateful_revision_compiles_rotates_and_recovers"],
    },
    Gate {
        id: "IG-05",
        status: Status::Wired,
        scenarios: &[
            "an_admin_mutation_publishes_an_audited_revision",
            "an_oidc_principal_is_authorized_against_the_active_directory",
        ],
    },
    Gate {
        id: "IG-06",
        status: Status::Wired,
        scenarios: &["stateful_revision_compiles_rotates_and_recovers"],
    },
    Gate {
        id: "IG-07",
        status: Status::Wired,
        scenarios: &["stateful_revision_compiles_rotates_and_recovers"],
    },
    Gate {
        id: "IG-08",
        status: Status::Wired,
        scenarios: &["stateful_revision_compiles_rotates_and_recovers"],
    },
    Gate {
        id: "IG-09",
        status: Status::Wired,
        scenarios: &["stateful_revision_compiles_rotates_and_recovers"],
    },
    Gate {
        id: "IG-10",
        status: Status::Wired,
        scenarios: &["stateful_revision_compiles_rotates_and_recovers"],
    },
    Gate {
        id: "IG-11",
        status: Status::Blocked,
        scenarios: &["stateful_qualification_profiles_are_published"],
    },
];

// ── The matrix and the harness are one artefact ──────────────────────────────

fn matrix_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/operations/stateful-integration.md")
}

/// The matrix rows, as `id -> (status, scenarios)`.
fn matrix_rows() -> BTreeMap<String, (Status, BTreeSet<String>)> {
    let text = std::fs::read_to_string(matrix_path()).expect("the acceptance matrix is committed");
    let mut rows = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if !line.starts_with("| IG-") {
            continue;
        }
        let cells: Vec<&str> = line
            .trim_matches('|')
            .split('|')
            .map(str::trim)
            .collect::<Vec<_>>();
        assert_eq!(
            cells.len(),
            6,
            "an acceptance matrix row needs six columns, got {line:?}"
        );
        let scenarios = cells[4]
            .split(',')
            .map(|cell| cell.trim().trim_matches('`').to_owned())
            .filter(|cell| !cell.is_empty())
            .collect();
        let previous = rows.insert(cells[0].to_owned(), (Status::parse(cells[5]), scenarios));
        assert!(previous.is_none(), "duplicate matrix row for {}", cells[0]);
    }
    assert!(!rows.is_empty(), "the acceptance matrix has no gate rows");
    rows
}

#[test]
fn the_matrix_and_the_harness_name_the_same_gates() {
    let documented = matrix_rows();
    let implemented: BTreeMap<String, (Status, BTreeSet<String>)> = GATES
        .iter()
        .map(|gate| {
            (
                gate.id.to_owned(),
                (
                    gate.status,
                    gate.scenarios
                        .iter()
                        .map(|name| (*name).to_owned())
                        .collect(),
                ),
            )
        })
        .collect();

    assert_eq!(
        documented.keys().collect::<Vec<_>>(),
        implemented.keys().collect::<Vec<_>>(),
        "docs/operations/stateful-integration.md and this suite disagree about which #160 gates \
         exist"
    );
    for (id, documented_gate) in &documented {
        assert_eq!(
            documented_gate, &implemented[id],
            "{id}: the acceptance matrix and this suite disagree about its status or its evidence"
        );
    }
}

#[test]
fn every_gate_has_a_scenario_that_exists() {
    let source = include_str!("stateful_integration.rs");
    for gate in GATES {
        assert!(
            !gate.scenarios.is_empty(),
            "{}: a gate with no scenario has no evidence",
            gate.id
        );
        for scenario in gate.scenarios {
            assert!(
                source.contains(&format!("fn {scenario}(")),
                "{}: the matrix names `{scenario}`, which this suite does not define",
                gate.id
            );
        }
    }
}

#[test]
fn integration_kek_fixture_is_base64_encoded_32_bytes() {
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    let encoded = stateful::integration_kek();
    let decoded = STANDARD
        .decode(encoded.as_bytes())
        .expect("the stateful fixture KEK is valid base64");
    assert_eq!(decoded.len(), 32, "the stateful fixture KEK is 32 bytes");
}

/// A `Wired` gate's evidence has to *run* somewhere a merge cannot skip.
///
/// The datastore scenarios skip without `AXOND_TEST_POSTGRES_DSN`, the way the
/// rest of the suite treats optional services, so a `wired` row proven only by
/// them would be a claim a bare `cargo test` never checks. What makes it a claim
/// is CI: the lane that supplies a database is required, and it forbids the skip.
/// Asserted here rather than trusted, because a lane can be renamed or dropped
/// from the aggregate by an unrelated workflow edit, and nothing else would
/// notice that a `wired` gate's evidence had quietly become optional.
#[test]
fn a_wired_gate_runs_in_a_required_ci_lane() {
    let workflow = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.github/workflows/ci.yml"),
    )
    .expect("the CI workflow is committed");

    for required in [
        "AXOND_TEST_POSTGRES_DSN: postgres://",
        "AXOND_TEST_REQUIRE_SERVICES: \"1\"",
        "      - stateful-tests",
        "test \"${{ needs.stateful-tests.result }}\" = success",
    ] {
        assert!(
            workflow.contains(required),
            "a `wired` gate's datastore scenarios are evidence only while the stateful lane runs \
             them and `CI Success` requires that lane: .github/workflows/ci.yml no longer contains \
             {required:?}"
        );
    }
}

/// The recovery packet is only complete when the two recovery lanes and the
/// binary they produced are assembled into one record. Keeping this check in
/// the integration suite prevents a workflow edit from silently reverting to
/// two independently green, but non-promotable, uploads.
#[test]
fn the_complete_recovery_record_is_required_by_ci() {
    let workflow = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.github/workflows/ci.yml"),
    )
    .expect("the CI workflow is committed");

    for required in [
        "  recovery-record:",
        "name: recovery-evidence",
        "name: restore-drill-evidence",
        "name: recovery-binary-stateful",
        "name: recovery-binary-restore",
        "run: cargo build -p axond --locked",
        "--slice recovery --tier serving",
        "name: qualification-record-recovery",
        "      - recovery-record",
        "test \"${{ needs.recovery-record.result }}\" = success",
    ] {
        assert!(
            workflow.contains(required),
            "the CI workflow no longer requires the complete recovery record component {required:?}"
        );
    }
}

/// The recovery manifest's process-level stages are owned by this black-box
/// lane, not silently by the in-process severable-link driver. The artifact
/// checker verifies that they ran; this registry check verifies that the lane
/// and the manifest continue to point at the same implementation.
#[test]
fn process_recovery_stages_are_owned_by_the_stateful_integration_lane() {
    let manifest = support::recovery::load();
    let expected = [
        "control-plane-outage/serving",
        "control-plane-outage/administration",
        "cold-boot-valid-cache/serving",
        "recovery-convergence/serving",
        "recovery-convergence/administration",
        "secret-rotation/rotation",
        "secret-rotation/serving",
    ];
    for key in expected {
        let (scenario_id, stage_id) = key.split_once('/').expect("a recovery stage key");
        let scenario = manifest
            .scenarios
            .iter()
            .find(|scenario| scenario.id == scenario_id)
            .unwrap_or_else(|| panic!("the recovery manifest has no {scenario_id} scenario"));
        let stage = scenario
            .stages
            .iter()
            .find(|stage| stage.id == stage_id)
            .unwrap_or_else(|| panic!("the recovery manifest has no {key} stage"));
        assert_eq!(stage.status, support::recovery::Status::Executable, "{key}");
        assert_eq!(
            stage.runner,
            Some(support::recovery::Runner::StatefulTests),
            "{key}"
        );
        assert_eq!(
            stage.driver,
            support::recovery::Driver::StatefulIntegration,
            "{key}"
        );
    }
}

// ── The standing refusal every blocked gate rests on ─────────────────────────

/// A complete stateful bootstrap whose references are satisfied, pointed at no
/// database — enough for the checks that only resolve references and report.
fn stateful_bootstrap() -> (PathBuf, BTreeMap<&'static str, String>, SocketAddr) {
    let bind = stateful::free_addr();
    let config = stateful::private_config(
        "axond.toml",
        &format!(
            "mode = \"stateful\"\n\
             [server]\n\
             bind = \"{bind}\"\n\
             [control_plane]\n\
             dsn_env = \"{dsn}\"\n\
             [secret_store]\n\
             backend = \"postgres\"\n\
             kek_env = \"{kek}\"\n\
             [[admin_breakglass]]\n\
             env = \"{breakglass}\"\n\
             id = \"breakglass\"\n",
            dsn = stateful::DSN_ENV,
            kek = stateful::KEK_ENV,
            breakglass = stateful::BREAKGLASS_ENV,
        ),
    );
    // A DSN that resolves but points nowhere. `check preflight` reads and
    // reports rather than serving, so it reaches its serving line whether or not
    // a database answers; the scenarios that need a live control plane use the
    // migrated [`ControlPlane`] fixture instead.
    let env = BTreeMap::from([
        (
            stateful::DSN_ENV,
            "postgres://axond@127.0.0.1:1/axond".to_owned(),
        ),
        (stateful::KEK_ENV, stateful::integration_kek()),
        (
            stateful::BREAKGLASS_ENV,
            "integration-test-breakglass".to_owned(),
        ),
    ]);
    (config, env, bind)
}

const INTEGRATION_TENANT: &str = "ten_019ff9e0-0000-7000-8000-000000000001";
const INTEGRATION_PROJECT: &str = "prj_019ff9e0-0000-7000-8000-000000000002";
const OIDC_PROJECT: &str = "prj_019ff9e0-0000-7000-8000-00000000000a";
const INTEGRATION_PRINCIPAL: &str = "prn_019ff9e0-0000-7000-8000-000000000003";
const INTEGRATION_PROVIDER: &str = "res_019ff9e0-0000-7000-8000-000000000004";
const INTEGRATION_CREDENTIAL: &str = "res_019ff9e0-0000-7000-8000-000000000005";
const INTEGRATION_CATALOG: &str = "res_019ff9e0-0000-7000-8000-000000000006";
const INTEGRATION_ENABLEMENT: &str = "res_019ff9e0-0000-7000-8000-000000000007";
const INTEGRATION_PRICE_BOOK: &str = "res_019ff9e0-0000-7000-8000-000000000008";
const INTEGRATION_ALIAS: &str = "res_019ff9e0-0000-7000-8000-000000000009";
const INTEGRATION_ALIAS_SLUG: &str = "wave2-chat";
const SECOND_TENANT: &str = "ten_019ff9e0-0000-7000-8000-000000000011";
const SECOND_PROJECT: &str = "prj_019ff9e0-0000-7000-8000-000000000012";
const SECOND_PRINCIPAL: &str = "prn_019ff9e0-0000-7000-8000-000000000013";
const SECOND_PROVIDER: &str = "res_019ff9e0-0000-7000-8000-000000000014";
const SECOND_CREDENTIAL: &str = "res_019ff9e0-0000-7000-8000-000000000015";
const SECOND_ENABLEMENT: &str = "res_019ff9e0-0000-7000-8000-000000000016";
const SECOND_ALIAS: &str = "res_019ff9e0-0000-7000-8000-000000000017";
const SECOND_ALIAS_SLUG: &str = "wave2-other";
fn integration_workload_key() -> String {
    format!("axw1.{}", "d0".repeat(32))
}

fn second_workload_key() -> String {
    format!("axw1.{}", "e1".repeat(32))
}

fn sha256_checksum(bytes: &[u8]) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, bytes);
    let mut rendered = String::from("sha256:");
    for byte in digest.as_ref() {
        use std::fmt::Write;
        write!(&mut rendered, "{byte:02x}").expect("a string accepts formatting");
    }
    rendered
}

/// Offering ids use the desired-state canonical encoding rather than the
/// catalogue's display names. Keep this tiny mirror in the black-box harness so
/// the admin document names the same immutable offering an importer derived.
fn offering_id(provider: &str, model: &str) -> String {
    fn string(value: &str, output: &mut Vec<u8>) {
        output.push(0x03);
        output.extend_from_slice(&(value.len() as u64).to_be_bytes());
        output.extend_from_slice(value.as_bytes());
    }

    let mut canonical = b"axond.desired-state\0\x01".to_vec();
    canonical.push(0x07); // map
    canonical.extend_from_slice(&2_u64.to_be_bytes());
    string("model", &mut canonical);
    string(model, &mut canonical);
    string("provider", &mut canonical);
    string(provider, &mut canonical);
    format!("off_{}", &sha256_checksum(&canonical)[7..])
}

async fn publish_resource(
    replica: &stateful::Replica,
    http: &reqwest::Client,
    path: &str,
    idempotency: &str,
    expected: &str,
    document: serde_json::Value,
) -> String {
    let document = if document.get("summary").is_some() {
        document
    } else {
        let mutation = if document.get("rotate") == Some(&serde_json::Value::Bool(true)) {
            "update"
        } else {
            "create"
        };
        serde_json::json!({
            "summary": "publish a stateful serving resource",
            "mutation": mutation,
            "resource": document,
        })
    };
    let response = replica
        .breakglass(
            http.post(replica.admin_url(path))
                .header("idempotency-key", idempotency)
                .header("x-axond-expected-revision", expected)
                .json(&document),
            "IG-03: publish a complete serving revision",
        )
        .send()
        .await
        .expect("a resource publish response");
    let status = response.status();
    let body: serde_json::Value = response.json().await.expect("a publish body");
    assert_eq!(
        status,
        200,
        "the typed serving resource must publish ({path}, {idempotency}, expected {expected}): {body}\n{}",
        replica.output()
    );
    body["revision"]
        .as_str()
        .unwrap_or_else(|| panic!("a publish names its revision: {body}"))
        .to_owned()
}

async fn wait_for_catalogue_identity(control_plane: &ControlPlane) -> (String, String, u64) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(identity) = control_plane.catalogue_identity().await {
            return identity;
        }
        assert!(
            Instant::now() < deadline,
            "the seeded catalogue was not retained:\n{}",
            control_plane.config.display()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_convergence(
    replica: &stateful::Replica,
    http: &reqwest::Client,
    revision: &str,
    source: &str,
) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let response = replica
            .breakglass(
                http.get(replica.admin_url("/convergence")),
                "IG-08: observe convergence",
            )
            .send()
            .await
            .expect("a convergence response");
        if response.status() == 200 {
            let body: serde_json::Value = response.json().await.expect("a convergence body");
            if body["active"] == revision && body["loaded"] == revision && body["source"] == source
            {
                return body;
            }
        }
        assert!(
            Instant::now() < deadline,
            "replica did not reach revision {revision} from {source}:\n{}",
            replica.output()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_log(replica: &stateful::Replica, needles: &[&str]) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let output = replica.output();
        if needles.iter().all(|needle| output.contains(needle)) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "replica log did not retain the expected usage identity {needles:?}:\n{output}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ── IG-01: explicit operating modes ──────────────────────────────────────────

#[tokio::test]
async fn stateless_boot_serves_with_no_control_plane() {
    let (_upstream, gateway) = boot().await;

    assert!(
        !gateway.config.contains("[control_plane]"),
        "the stateless fixture must not declare a control plane:\n{}",
        gateway.config
    );
    let models = client()
        .get(gateway.url("/v1/models"))
        .bearer_auth(GATEWAY_KEY)
        .send()
        .await
        .expect("a response");
    assert_eq!(
        models.status(),
        200,
        "stateless mode serves its catalogue from the config file alone:\n{}",
        gateway.output()
    );
}

/// The other half of IG-01: a stateful bootstrap *reaches* its control plane,
/// and what it will not do is serve inference from a snapshot it does not have.
///
/// This is the property, not a placeholder for one: administration is how
/// durable desired state is written at all, so a replica that could not serve
/// `/admin/v1` would leave stateful mode unusable, and a replica that answered
/// inference here would be answering from an empty snapshot. Both halves are
/// asserted on one running process, because either alone is satisfied by a
/// deployment that is broken in the opposite direction.
#[tokio::test]
async fn stateful_boot_serves_administration_and_refuses_inference() {
    let Some(control_plane) = ControlPlane::create().await else {
        eprintln!(
            "SKIPPED without AXOND_TEST_POSTGRES_DSN: IG-01's `wired` row is NOT proven by this \
             run. It is proven by CI's required `Stateful tests` lane, which sets \
             AXOND_TEST_REQUIRE_SERVICES=1 so this skip is a failure there."
        );
        return;
    };
    let migrated = control_plane.run(&["migrate", "apply"]);
    assert!(
        migrated.succeeded(),
        "a replica opens the control plane at boot, so this scenario needs it migrated:\n{}",
        migrated.context()
    );
    let replica = control_plane.serve().await;

    // 0. Boot opened the SecretStore in this scenario's own schema. A store left
    //    on the search path's default would hold every concurrent scenario's
    //    material in one table, and the fixture's `DROP SCHEMA` would leave it
    //    behind.
    assert!(
        control_plane.table_exists("axond_secret").await,
        "the replica's SecretStore must be the scenario's, not `public`'s:\n{}",
        replica.output()
    );

    // 1. Drive the zero-redeploy lifecycle through the running administrative
    //    surface and the production PostgreSQL-backed store, rather than only
    //    through the in-memory route fixture or the store's direct unit tests.
    let http = client();
    let tenant = "ten_019ff9e0-0000-7000-8000-000000000001";
    let first_material = "sk-integration-secret-v1";
    let second_material = "sk-integration-secret-v2";
    let staged = replica
        .breakglass(
            http.post(replica.admin_url("/secrets"))
                .json(&serde_json::json!({
                    "tenant": tenant,
                    "material": first_material,
                })),
            "integration secret stage",
        )
        .send()
        .await
        .expect("a staged secret response");
    assert_eq!(staged.status(), 200, "{}", replica.output());
    let staged: serde_json::Value = staged.json().await.expect("a staged secret body");
    assert_eq!(staged["lifecycle"], "staged");
    assert!(!staged.to_string().contains(first_material), "{staged}");
    let first_reference = staged["reference"]
        .as_str()
        .expect("a staged reference")
        .to_owned();
    let secret = first_reference
        .split_once('@')
        .expect("a versioned reference")
        .0;

    let activated = replica
        .breakglass(
            http.post(replica.admin_url("/secrets/lifecycle"))
                .json(&serde_json::json!({
                    "tenant": tenant,
                    "reference": first_reference,
                    "lifecycle": "active",
                })),
            "integration secret activate",
        )
        .send()
        .await
        .expect("an activation response");
    assert_eq!(activated.status(), 200, "{}", replica.output());
    assert_eq!(
        activated
            .json::<serde_json::Value>()
            .await
            .expect("an activation body")["changed"],
        true
    );

    let rotated = replica
        .breakglass(
            http.post(replica.admin_url("/secrets/rotate"))
                .json(&serde_json::json!({
                    "tenant": tenant,
                    "reference": first_reference,
                    "material": second_material,
                })),
            "integration secret rotate",
        )
        .send()
        .await
        .expect("a rotation response");
    assert_eq!(rotated.status(), 200, "{}", replica.output());
    let rotated: serde_json::Value = rotated.json().await.expect("a rotated secret body");
    assert_eq!(rotated["version"], 2);
    assert_eq!(rotated["lifecycle"], "staged");
    assert!(!rotated.to_string().contains(second_material), "{rotated}");
    let second_reference = rotated["reference"]
        .as_str()
        .expect("a rotated reference")
        .to_owned();

    for (reference, lifecycle, reason) in [
        (
            &second_reference,
            "active",
            "integration secret activate rotation",
        ),
        (&first_reference, "revoked", "integration secret revoke"),
        (
            &first_reference,
            "tombstoned",
            "integration secret tombstone",
        ),
    ] {
        let moved = replica
            .breakglass(
                http.post(replica.admin_url("/secrets/lifecycle"))
                    .json(&serde_json::json!({
                        "tenant": tenant,
                        "reference": reference,
                        "lifecycle": lifecycle,
                    })),
                reason,
            )
            .send()
            .await
            .expect("a lifecycle response");
        assert_eq!(moved.status(), 200, "{}", replica.output());
        let moved: serde_json::Value = moved.json().await.expect("a lifecycle body");
        assert_eq!(moved["lifecycle"], lifecycle);
        assert!(!moved.to_string().contains(first_material), "{moved}");
        assert!(!moved.to_string().contains(second_material), "{moved}");
    }

    let versions = replica
        .breakglass(
            http.get(replica.admin_url(&format!("/secrets/{secret}?tenant={tenant}"))),
            "integration secret versions",
        )
        .send()
        .await
        .expect("a versions response");
    assert_eq!(versions.status(), 200, "{}", replica.output());
    let versions: serde_json::Value = versions.json().await.expect("a versions body");
    assert_eq!(versions["versions"][0]["lifecycle"], "tombstoned");
    assert_eq!(versions["versions"][0]["resolvable"], false);
    assert_eq!(versions["versions"][1]["lifecycle"], "active");
    assert_eq!(versions["versions"][1]["resolvable"], true);
    assert!(!versions.to_string().contains(first_material), "{versions}");
    assert!(
        !versions.to_string().contains(second_material),
        "{versions}"
    );
    let output = replica.output();
    assert!(
        !output.contains(first_material),
        "secret material reached the log: {output}"
    );
    assert!(
        !output.contains(second_material),
        "secret material reached the log: {output}"
    );

    // 2. The administrative surface is served and authenticated. An unauthorized
    //    read is the strongest evidence a scenario without an OIDC provider can
    //    state without holding a credential: `401` in the administrative error
    //    envelope means the surface is mounted, reached, and gated — where a
    //    stateless replica answers the same path `stateful_mode_required`, and an
    //    unmounted one would answer axum's empty 404.
    let unauthorized = client()
        .get(replica.url("/admin/v1/state"))
        .send()
        .await
        .expect("a response");
    assert_eq!(
        unauthorized.status(),
        401,
        "a stateful replica serves `/admin/v1` and authenticates it:\n{}",
        replica.output()
    );
    let envelope: serde_json::Value = unauthorized.json().await.expect("an error envelope");
    assert_eq!(
        envelope["error"]["type"], "admin_unauthenticated",
        "the refusal must be the administrative surface's own, rather than a mode refusal or the \
         router's:\n{envelope}"
    );

    // 3. Inference remains fail-closed per request. Authentication runs before
    //    the convergence gate, so an unauthenticated probe receives 401 rather
    //    than learning that the replica has no active projected snapshot.
    let readyz = client()
        .get(replica.url("/readyz"))
        .send()
        .await
        .expect("a response");
    assert_eq!(
        readyz.status(),
        503,
        "readiness reflects convergence: a replica serving no revision is not ready:\n{}",
        replica.output()
    );
    let models = client()
        .get(replica.url("/v1/models"))
        .send()
        .await
        .expect("a response");
    assert_eq!(
        models.status(),
        401,
        "an unconverged replica must authenticate before reporting convergence:\n{}",
        replica.output()
    );
    let refusal: serde_json::Value = models.json().await.expect("an error envelope");
    assert_eq!(
        refusal["error"]["type"], "unauthorized",
        "an unauthenticated caller must not learn convergence state:\n{refusal}"
    );
}

/// The loud failure IG-01 also promises: a reference the deployment names and
/// the environment does not hold stops the boot, before anything is served.
///
/// The refusal must name the unresolved reference. A stateful replica does boot
/// now, so a nonzero exit that named nothing could be an unreachable database
/// instead — which would leave this scenario passing for a reason it does not
/// state, and passing on a day the bootstrap check had regressed.
#[test]
fn stateful_boot_refuses_an_unresolved_reference() {
    // The references the config names are deliberately left out of the
    // environment, and nothing else is in it either: an inherited DSN would let
    // this boot get as far as a connection.
    let (config, _references, bind) = stateful_bootstrap();
    let mut command = std::process::Command::new(stateful::axond());
    command
        .env_clear()
        .env("AXOND_CONFIG", &config)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(path) = std::env::var_os("PATH") {
        command.env("PATH", path);
    }
    let mut child = command.spawn().expect("the axond binary runs");

    // Both streams are drained while the child runs. Left unread, a chatty boot
    // would fill its pipe, block on `write`, and be reported below as a process
    // that would not stop — a failure blaming the wrong thing.
    let streams: Vec<Box<dyn Read + Send>> = vec![
        Box::new(child.stdout.take().expect("a piped stdout")),
        Box::new(child.stderr.take().expect("a piped stderr")),
    ];
    let drains: Vec<_> = streams
        .into_iter()
        .map(|mut stream| {
            std::thread::spawn(move || {
                let mut text = String::new();
                stream
                    .read_to_string(&mut text)
                    .expect("the child's output is readable");
                text
            })
        })
        .collect();

    // A refusal exits immediately. Waiting without a deadline would instead hang
    // the suite for as long as CI allows on the day an unresolved reference stops
    // being fatal — the very change this scenario exists to catch.
    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        match child.try_wait().expect("the child's status is readable") {
            Some(status) => break Some(status),
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                break None;
            }
            None => std::thread::sleep(Duration::from_millis(20)),
        }
    };
    let reported: String = drains
        .into_iter()
        .map(|drain| drain.join().expect("the drain finishes"))
        .collect();
    let status = status.unwrap_or_else(|| {
        panic!(
            "IG-01: a stateful boot kept running with every reference it names unset, so a \
             deployment can start unadministrable.\n{reported}"
        )
    });

    assert!(
        !status.success(),
        "a stateful replica must refuse to start rather than serve an unadministrable \
         deployment:\n{reported}"
    );
    assert!(
        reported.contains(stateful::DSN_ENV)
            || reported.contains(stateful::KEK_ENV)
            || reported.contains(stateful::BREAKGLASS_ENV),
        "the refusal must name the unresolved reference, or it is not evidence that boot stopped \
         at the reference rather than at a connection:\n{reported}"
    );
    // The child's own report, not a probe of `bind`: an ephemeral port is free
    // the moment the fixture reserves it, so a sibling test that binds the same
    // port would answer a probe here and fail this scenario for someone else's
    // reason.
    assert!(
        !reported.contains("axond listening"),
        "a refused boot must not have bound a listener on {bind}:\n{reported}"
    );
}

// ── IG-02: the Postgres-first control plane an operator prepares ─────────────

#[tokio::test]
async fn preflight_describes_a_stateless_install() {
    let (_upstream, gateway) = boot().await;
    let config = stateful::private_config("axond.toml", &gateway.config);

    // The environment the running gateway was given, minus everything else: a
    // stateless preflight resolves inbound and provider references and reaches
    // no database.
    let env = BTreeMap::from([
        ("GW_INBOUND_KEY", GATEWAY_KEY.to_owned()),
        // The harness's per-boot second inbound key. A reference is a name, and
        // preflight only resolves it, so a fixture value is the whole of it.
        ("GW_BOOT_KEY", "integration-preflight-boot-key".to_owned()),
        (
            "GW_FAKE_OPENAI_KEY",
            support::gateway::OPENAI_KEY.to_owned(),
        ),
        (
            "GW_FAKE_ANTHROPIC_KEY",
            support::gateway::ANTHROPIC_KEY.to_owned(),
        ),
    ]);
    let run = stateful::run(&config, &["check", "preflight"], &env);

    assert!(
        run.succeeded(),
        "the config a gateway is serving right now must pass its own preflight:\n{}",
        run.context()
    );
    assert!(
        run.stdout().contains("stateless mode"),
        "preflight reports the mode it checked:\n{}",
        run.context()
    );
    assert!(
        run.stdout().contains("skipped"),
        "a stateless install skips the control-plane checks rather than passing them \
         vacuously:\n{}",
        run.context()
    );
}

#[tokio::test]
async fn migrate_prepares_a_control_plane_before_replicas_start() {
    let Some(control_plane) = ControlPlane::create().await else {
        eprintln!("skipping: AXOND_TEST_POSTGRES_DSN is not set");
        return;
    };

    // 1. A status against an unprepared database reports work outstanding, exits
    //    non-zero so a rollout can gate on it, and — the property worth proving —
    //    leaves the database unprepared.
    let status = control_plane.run(&["migrate", "status"]);
    assert!(
        !status.succeeded(),
        "an outstanding migration is a 'not ready to serve':\n{}",
        status.context()
    );
    assert!(
        !control_plane.ledger_exists().await,
        "`migrate status` created bookkeeping in a database it only read:\n{}",
        status.context()
    );

    // 2. Apply is the one command that writes, and it is enough on its own.
    let apply = control_plane.run(&["migrate", "apply"]);
    assert!(
        apply.succeeded(),
        "a forward migration onto an empty schema must succeed:\n{}",
        apply.context()
    );
    let applied = control_plane.applied_versions().await;
    assert!(
        !applied.is_empty(),
        "an applied migration records itself in the ledger:\n{}",
        apply.context()
    );

    // 3. Applied is a settled state: status now passes, and a second apply is
    //    not a second migration — the property a rollout script depends on when
    //    two hosts run it at once.
    let settled = control_plane.run(&["migrate", "status"]);
    assert!(
        settled.succeeded(),
        "a current schema is ready to serve:\n{}",
        settled.context()
    );
    let reapply = control_plane.run(&["migrate", "apply"]);
    assert!(
        reapply.succeeded(),
        "`migrate apply` is idempotent:\n{}",
        reapply.context()
    );
    assert_eq!(
        control_plane.applied_versions().await,
        applied,
        "the second apply changed the ledger:\n{}",
        reapply.context()
    );

    // 4. Preflight now describes a valid stateful serving posture. It passes
    //    the serving check; the database/reference checks remain the source of
    //    truth for this prepared control plane.
    let preflight = control_plane.run(&["check", "preflight"]);
    let reported = preflight.reported();
    assert!(
        reported.contains("control-plane database") && !reported.contains(&control_plane.dsn),
        "preflight names the control plane by reference, never by DSN:\n{}",
        preflight.context()
    );
    assert!(
        preflight.succeeded() && reported.contains("serving"),
        "preflight must accept the serving posture while retaining its DSN/reference checks:\n{}",
        preflight.context()
    );

    // The schema drops with `control_plane`, on this path and on every failing
    // one.
}

/// A fixture's schema is claimed *before* it is created, so a setup step that
/// fails between the `CREATE` and the fixture existing still takes it with it —
/// the window a long-lived CI database would otherwise accumulate one abandoned
/// schema per failed run in.
#[tokio::test]
async fn a_setup_that_fails_after_creating_its_schema_leaves_nothing_behind() {
    let Some(dsn) = stateful::postgres_dsn() else {
        eprintln!("skipping: AXOND_TEST_POSTGRES_DSN is not set");
        return;
    };
    let schema = format!(
        "axond_it_claim_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("a monotonic wall clock")
            .as_nanos()
    );
    /// The arranged failure, so the case cannot pass on some other panic.
    const ARRANGED: &str = "a setup step failed after the schema existed";

    // On a thread with a runtime of its own: the failure has to unwind without
    // taking this test with it, and a current-thread runtime cannot be
    // re-entered from the destructor that does the cleanup.
    let failure = std::thread::spawn({
        let (dsn, schema) = (dsn.clone(), schema.clone());
        move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a runtime for the failing setup")
                .block_on(async move {
                    let claimed = Schema::create(&dsn, &schema).await;
                    assert!(
                        schema::exists(&dsn, claimed.name()).await,
                        "the arranged setup created its schema, or the case proves nothing"
                    );
                    // `claimed` is still a local, so the unwind is what has to
                    // clean up — the point of the case.
                    panic!("{ARRANGED}");
                });
        }
    })
    .join();

    let Err(panic) = failure else {
        panic!("the arranged setup returned instead of failing");
    };
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or_default();
    assert!(
        message.contains(ARRANGED),
        "the setup failed for the arranged reason, not another: {message}"
    );
    assert!(
        !schema::exists(&dsn, &schema).await,
        "the failed setup left the schema {schema} behind"
    );
}

// ── IG-05: validated, revisioned, authorized, audited mutations ──────────────

/// One breakglass-authenticated mutation, followed from the request to the revision it
/// published and the audit event that attributes it.
///
/// This preserves the recovery credential's separate audit identity; the
/// issuer-scoped human path is exercised by the scenario immediately below.
#[tokio::test]
async fn an_admin_mutation_publishes_an_audited_revision() {
    let Some(control_plane) = ControlPlane::create().await else {
        eprintln!(
            "SKIPPED without AXOND_TEST_POSTGRES_DSN: IG-05's wired breakglass scenario is NOT proven by this \
             run. It is proven by CI's required `Stateful tests` lane, which sets \
             AXOND_TEST_REQUIRE_SERVICES=1 so this skip is a failure there."
        );
        return;
    };
    let migrated = control_plane.run(&["migrate", "apply"]);
    assert!(
        migrated.succeeded(),
        "a mutation is published against a migrated control plane:\n{}",
        migrated.context()
    );
    let replica = control_plane.serve().await;
    let client = client();

    // A tenant: the one resource a freshly migrated deployment can hold, since
    // everything else is scoped to one.
    let tenant = "ten_019ff9e0-0000-7000-8000-000000000001";
    let document = serde_json::json!({
        "summary": "create the integration tenant",
        "mutation": "create",
        "resource": {
            "tenant": tenant,
            "slug": "integration",
            "display_name": "Integration",
        },
    });
    let publish = |key: &'static str, expected: &'static str| {
        replica
            .breakglass(
                client.post(replica.admin_url("/tenants")),
                "IG-05: publish an audited revision",
            )
            .header("idempotency-key", key)
            .header("x-axond-expected-revision", expected)
            .json(&document)
            .send()
    };

    // 1. Authenticated: an anonymous mutation is refused before it is validated,
    //    so an unauthenticated caller cannot learn what the deployment contains
    //    by watching which bodies are rejected.
    let anonymous = client
        .post(replica.admin_url("/tenants"))
        .json(&document)
        .send()
        .await
        .expect("an unauthenticated response");
    assert_eq!(
        anonymous.status(),
        401,
        "an unauthenticated mutation must be refused:\n{}",
        replica.output()
    );

    // 2. Precondition-carrying: a mutation with no idempotency key is refused
    //    rather than published once and unrepeatably.
    let unconditional = replica
        .breakglass(
            client.post(replica.admin_url("/tenants")),
            "IG-05: a mutation with no preconditions",
        )
        .json(&document)
        .send()
        .await
        .expect("a response");
    assert_eq!(
        unconditional.status(),
        400,
        "a mutation without an idempotency key must be refused:\n{}",
        replica.output()
    );

    // 3. Published: one revision, checksummed, with the resource in its diff.
    let published = publish("ig-05-tenant", "empty")
        .await
        .expect("a publish response");
    assert_eq!(
        published.status(),
        200,
        "an authenticated mutation with satisfied preconditions publishes:\n{}",
        replica.output()
    );
    let published: serde_json::Value = published.json().await.expect("a publish result");
    let revision = published["revision"]
        .as_str()
        .unwrap_or_else(|| panic!("a publish result names its revision: {published}"))
        .to_owned();
    assert_eq!(
        published["diff"]["summary"]["added"], 1,
        "the published revision adds exactly the resource the caller described: {published}"
    );
    let checksum = published["checksum"]
        .as_str()
        .unwrap_or_else(|| panic!("a publish result checksums what it published: {published}"))
        .to_owned();
    assert!(
        checksum.starts_with("sha256:"),
        "a revision is identified by a digest an operator can compare: {published}"
    );

    // 4. Durable and readable: the revision is the deployment's desired state and
    //    its history, read back from the control plane through the surface.
    let state: serde_json::Value = replica
        .breakglass(
            client.get(replica.admin_url("/state")),
            "IG-05: read the published state",
        )
        .send()
        .await
        .expect("a state response")
        .json()
        .await
        .expect("a state document");
    assert_eq!(
        state["revision"], revision,
        "the published revision is the current desired state: {state}"
    );
    assert_eq!(
        state["resources"][0]["kind"], "tenant",
        "the desired state holds the resource that was published: {state}"
    );

    // 5. Audited, and attributed to the credential that was actually used:
    //    breakglass is recorded as breakglass rather than disguised as a person.
    let audit: serde_json::Value = replica
        .breakglass(
            client.get(replica.admin_url(&format!("/audit/{revision}"))),
            "IG-05: read the audit trail",
        )
        .send()
        .await
        .expect("an audit response")
        .json()
        .await
        .expect("an audit document");
    let event = &audit["events"][0];
    assert_eq!(
        event["actor"]["kind"], "breakglass",
        "the audit event names the credential population that published: {audit}"
    );
    assert_eq!(
        event["kind"], "create",
        "the audit event records the kind of change that was declared: {audit}"
    );
    assert!(
        event["summary"]
            .as_str()
            .is_some_and(|summary| summary.contains("create the integration tenant")),
        "the audit event carries the author's own summary: {audit}"
    );

    // 6. Concurrency-safe: the same body under the same idempotency key is the
    //    same mutation, and a stale expected revision is a conflict rather than a
    //    silent overwrite.
    let replayed = publish("ig-05-tenant", "empty")
        .await
        .expect("a replay response");
    assert_eq!(
        replayed.status(),
        200,
        "replaying a mutation under its idempotency key is not a second mutation:\n{}",
        replica.output()
    );
    let replayed: serde_json::Value = replayed.json().await.expect("a replay result");
    assert_eq!(
        replayed["revision"], revision,
        "a replay returns the revision the original request published: {replayed}"
    );
    let stale = publish("ig-05-tenant-again", "empty")
        .await
        .expect("a conflict response");
    assert_eq!(
        stale.status(),
        409,
        "a mutation that expects a superseded revision must conflict:\n{}",
        replica.output()
    );
    let history: serde_json::Value = replica
        .breakglass(
            client.get(replica.admin_url("/history")),
            "IG-05: count the revisions",
        )
        .send()
        .await
        .expect("a history response")
        .json()
        .await
        .expect("a history document");
    assert_eq!(
        history["revisions"].as_array().map(Vec::len),
        Some(1),
        "three requests describing one change leave one revision behind: {history}"
    );
    assert_eq!(
        history["revisions"][0]["checksum"], checksum,
        "history identifies the revision by the digest the publish reported: {history}"
    );
}

/// A human principal is published into durable desired state, projected into
/// the active authorization snapshot, and then used through the real OIDC/JWKS
/// verifier. The same bearer can manage its tenant but cannot even read another
/// tenant's catalogue; the successful mutation is attributed to the issuer and
/// subject that signed it.
#[tokio::test]
async fn an_oidc_principal_is_authorized_against_the_active_directory() {
    let Some(control_plane) = ControlPlane::create().await else {
        eprintln!(
            "SKIPPED without AXOND_TEST_POSTGRES_DSN: IG-05's OIDC runtime path is proven by CI's required Stateful tests lane"
        );
        return;
    };
    let provider = support::oidc::OidcProvider::start().await;
    control_plane.enable_oidc(&provider);
    let migrated = control_plane.run(&["migrate", "apply"]);
    assert!(migrated.succeeded(), "{}", migrated.context());
    let replica = control_plane.serve().await;
    let http = client();
    let subject = "alice";
    let token = provider.token(subject);

    let mut revision = publish_resource(
        &replica,
        &http,
        "/tenants",
        "ig-05-oidc-tenant-a",
        "empty",
        serde_json::json!({
            "tenant": INTEGRATION_TENANT,
            "slug": "oidc-a",
            "display_name": "OIDC tenant A",
        }),
    )
    .await;
    revision = publish_resource(
        &replica,
        &http,
        "/tenants",
        "ig-05-oidc-tenant-b",
        &revision,
        serde_json::json!({
            "tenant": SECOND_TENANT,
            "slug": "oidc-b",
            "display_name": "OIDC tenant B",
        }),
    )
    .await;
    revision = publish_resource(
        &replica,
        &http,
        "/projects",
        "ig-05-oidc-inference-project",
        &revision,
        serde_json::json!({
            "project": INTEGRATION_PROJECT,
            "tenant": INTEGRATION_TENANT,
            "slug": "inference",
            "display_name": "Inference",
        }),
    )
    .await;
    revision = publish_resource(
        &replica,
        &http,
        "/principals",
        "ig-05-oidc-inference-principal",
        &revision,
        serde_json::json!({
            "principal": SECOND_PRINCIPAL,
            "tenant": INTEGRATION_TENANT,
            "project": INTEGRATION_PROJECT,
            "slug": "inference-workload",
            "display_name": "Inference workload",
            "key_digest": sha256_checksum(integration_workload_key().as_bytes()),
            "roles": ["developer"],
        }),
    )
    .await;
    revision = publish_resource(
        &replica,
        &http,
        "/principals",
        "ig-05-oidc-human",
        &revision,
        serde_json::json!({
            "principal": INTEGRATION_PRINCIPAL,
            "tenant": INTEGRATION_TENANT,
            "slug": "alice",
            "display_name": "Alice",
            "identity_kind": "human",
            "issuer": provider.issuer(),
            "subject": subject,
            "roles": ["tenant-admin"],
        }),
    )
    .await;
    wait_for_convergence(&replica, &http, &revision, "control-plane").await;

    let anonymous = http
        .get(replica.admin_url(&format!("/catalogue?tenant={INTEGRATION_TENANT}")))
        .send()
        .await
        .expect("anonymous catalogue response");
    assert_eq!(anonymous.status(), 401, "anonymous admin access is refused");

    let own_catalogue = http
        .get(replica.admin_url(&format!("/catalogue?tenant={INTEGRATION_TENANT}")))
        .bearer_auth(&token)
        .send()
        .await
        .expect("OIDC tenant catalogue response");
    assert_eq!(
        own_catalogue.status(),
        200,
        "OIDC human can read its tenant"
    );

    let foreign_catalogue = http
        .get(replica.admin_url(&format!("/catalogue?tenant={SECOND_TENANT}")))
        .bearer_auth(&token)
        .send()
        .await
        .expect("cross-tenant catalogue response");
    assert_eq!(
        foreign_catalogue.status(),
        403,
        "OIDC human cannot read another tenant"
    );

    let project = http
        .post(replica.admin_url("/projects"))
        .bearer_auth(&token)
        .header("idempotency-key", "ig-05-oidc-project")
        .header("x-axond-expected-revision", &revision)
        .json(&serde_json::json!({
            "summary": "create the OIDC tenant project",
            "mutation": "create",
                "resource": {
                "project": OIDC_PROJECT,
                "tenant": INTEGRATION_TENANT,
                "slug": "alice-project",
                "display_name": "Alice project",
            },
        }))
        .send()
        .await
        .expect("OIDC project publication response");
    assert_eq!(
        project.status(),
        200,
        "OIDC human can publish in its tenant"
    );
    let project: serde_json::Value = project.json().await.expect("OIDC project result");
    let project_revision = project["revision"]
        .as_str()
        .expect("OIDC project publication names its revision")
        .to_owned();
    wait_for_convergence(&replica, &http, &project_revision, "control-plane").await;

    let audit = replica
        .breakglass(
            http.get(replica.admin_url(&format!("/audit/{project_revision}"))),
            "IG-05: verify the OIDC mutation attribution",
        )
        .send()
        .await
        .expect("OIDC audit response");
    assert_eq!(audit.status(), 200, "OIDC publication has an audit record");
    let audit: serde_json::Value = audit.json().await.expect("OIDC audit body");
    let actor = &audit["events"][0]["actor"];
    assert_eq!(actor["kind"], "human", "the OIDC actor is a human");
    assert_eq!(actor["issuer"], provider.issuer());
    assert_eq!(actor["subject"], subject);

    // The encrypted serving cache is what makes inference recoverable, while
    // the signed desired-state sibling carries the directory that gives this
    // OIDC identity its tenant-scoped authority. A cold boot with Postgres
    // unavailable must therefore authenticate the token and reach the
    // control-plane refusal (503), rather than lose the directory and return a
    // misleading authorization denial (403).
    drop(replica);
    let outage = control_plane.serve_without_control_plane().await;
    let cached_oidc_read = http
        .get(outage.admin_url(&format!("/catalogue?tenant={INTEGRATION_TENANT}")))
        .bearer_auth(&token)
        .send()
        .await
        .expect("cached OIDC administrative response");
    let cached_oidc_status = cached_oidc_read.status();
    let cached_oidc_body: serde_json::Value = cached_oidc_read
        .json()
        .await
        .expect("cached OIDC administrative error body");
    assert_eq!(cached_oidc_status, 503, "{}", outage.output());
    assert_eq!(
        cached_oidc_body["error"]["type"], "control_plane_unavailable",
        "the cached OIDC identity was authorized before the management read reached the unavailable journal: {cached_oidc_body}"
    );
}

/// IG-03 through IG-10 share one expensive, service-backed path. It publishes a
/// complete typed revision, serves it through a controlled upstream, rotates the
/// SecretStore-backed credential, then cold-boots a replacement with Postgres
/// unavailable from the encrypted compiled-serving cache.
#[tokio::test]
async fn stateful_revision_compiles_rotates_and_recovers() {
    let Some(control_plane) = ControlPlane::create().await else {
        eprintln!(
            "SKIPPED without AXOND_TEST_POSTGRES_DSN: the live stateful revision scenario is proven by CI's required Stateful tests lane"
        );
        return;
    };
    let upstream = support::FakeUpstream::start().await;
    let migrated = control_plane.run(&["migrate", "apply"]);
    assert!(migrated.succeeded(), "{}", migrated.context());
    let replica = control_plane.serve().await;
    let http = client();

    let first_material = format!("sk-wave2-v1-{}", control_plane.schema);
    let second_material = format!("sk-wave2-v2-{}", control_plane.schema);
    let workload_key = integration_workload_key();
    let staged = replica
        .breakglass(
            http.post(replica.admin_url("/secrets"))
                .json(&serde_json::json!({
                    "tenant": INTEGRATION_TENANT,
                    "project": INTEGRATION_PROJECT,
                    "material": first_material,
                })),
            "IG-04: stage the first provider secret",
        )
        .send()
        .await
        .expect("a secret stage response");
    assert_eq!(staged.status(), 200, "{}", replica.output());
    let staged: serde_json::Value = staged.json().await.expect("a staged secret body");
    let first_reference = staged["reference"]
        .as_str()
        .expect("the staged secret names a version")
        .to_owned();
    let secret_id = first_reference
        .split_once('@')
        .expect("the secret reference is versioned")
        .0
        .to_owned();
    let activated = replica
        .breakglass(
            http.post(replica.admin_url("/secrets/lifecycle"))
                .json(&serde_json::json!({
                    "tenant": INTEGRATION_TENANT,
                    "project": INTEGRATION_PROJECT,
                    "reference": first_reference,
                    "lifecycle": "active",
                })),
            "IG-04: activate the first provider secret",
        )
        .send()
        .await
        .expect("a secret lifecycle response");
    assert_eq!(activated.status(), 200, "{}", replica.output());

    let tenant_b_material = format!("sk-wave2-tenant-b-{}", control_plane.schema);
    let second_workload_key = second_workload_key();
    let second_staged = replica
        .breakglass(
            http.post(replica.admin_url("/secrets"))
                .json(&serde_json::json!({
                    "tenant": SECOND_TENANT,
                    "project": SECOND_PROJECT,
                    "material": tenant_b_material,
                })),
            "IG-10: stage the second tenant provider secret",
        )
        .send()
        .await
        .expect("a second tenant secret stage response");
    assert_eq!(second_staged.status(), 200, "{}", replica.output());
    let second_staged: serde_json::Value = second_staged
        .json()
        .await
        .expect("a second tenant staged secret body");
    let second_reference = second_staged["reference"]
        .as_str()
        .expect("the second tenant secret names a version")
        .to_owned();
    let second_secret_id = second_reference
        .split_once('@')
        .expect("the second tenant secret reference is versioned")
        .0
        .to_owned();
    let second_activated = replica
        .breakglass(
            http.post(replica.admin_url("/secrets/lifecycle"))
                .json(&serde_json::json!({
                    "tenant": SECOND_TENANT,
                    "project": SECOND_PROJECT,
                    "reference": second_reference,
                    "lifecycle": "active",
                })),
            "IG-10: activate the second tenant provider secret",
        )
        .send()
        .await
        .expect("a second tenant secret lifecycle response");
    assert_eq!(second_activated.status(), 200, "{}", replica.output());

    let (catalog_digest, catalog_content, catalog_size) =
        wait_for_catalogue_identity(&control_plane).await;
    let offering = offering_id("openai", "gpt-4o");
    let key_digest = sha256_checksum(workload_key.as_bytes());
    let second_key_digest = sha256_checksum(second_workload_key.as_bytes());
    let endpoint = upstream.base_url.clone();
    let mut revision = String::from("empty");

    let documents = [
        (
            "/tenants",
            "ig-03-tenant",
            serde_json::json!({
                "summary": "publish the wave 2 tenant",
                "mutation": "create",
                "resource": {
                    "tenant": INTEGRATION_TENANT,
                    "slug": "wave2",
                    "display_name": "Wave 2",
                },
            }),
        ),
        (
            "/projects",
            "ig-03-project",
            serde_json::json!({
                "summary": "publish the wave 2 project",
                "mutation": "create",
                "resource": {
                    "project": INTEGRATION_PROJECT,
                    "tenant": INTEGRATION_TENANT,
                    "slug": "inference",
                    "display_name": "Inference",
                },
            }),
        ),
        (
            "/principals",
            "ig-03-principal",
            serde_json::json!({
                "principal": INTEGRATION_PRINCIPAL,
                "tenant": INTEGRATION_TENANT,
                "project": INTEGRATION_PROJECT,
                "slug": "wave2-workload",
                "display_name": "Wave 2 workload",
                "key_digest": key_digest,
                "roles": ["operator"],
            }),
        ),
        (
            "/providers",
            "ig-03-provider",
            serde_json::json!({
                "provider": INTEGRATION_PROVIDER,
                "tenant": INTEGRATION_TENANT,
                "project": INTEGRATION_PROJECT,
                "slug": "openai",
                "display_name": "Fixture OpenAI",
                "wire_family": "openai-chat",
                "endpoint": endpoint,
            }),
        ),
        (
            "/credentials",
            "ig-04-credential",
            serde_json::json!({
                "credential": INTEGRATION_CREDENTIAL,
                "tenant": INTEGRATION_TENANT,
                "project": INTEGRATION_PROJECT,
                "provider": INTEGRATION_PROVIDER,
                "slug": "openai-primary",
                "display_name": "Fixture OpenAI primary",
                "secret": secret_id,
                "secret_version": 1,
                "lifecycle": "active",
            }),
        ),
        (
            "/catalogs",
            "ig-03-catalog",
            serde_json::json!({
                "catalog": INTEGRATION_CATALOG,
                "slug": "seed",
                "digest": catalog_digest,
                "size_bytes": catalog_size,
            }),
        ),
        (
            "/models",
            "ig-03-model",
            serde_json::json!({
                "enablement": INTEGRATION_ENABLEMENT,
                "tenant": INTEGRATION_TENANT,
                "project": INTEGRATION_PROJECT,
                "slug": "gpt-4o",
                "offering": offering,
                "catalog": INTEGRATION_CATALOG,
                "snapshot": catalog_digest,
                "wire_family": "openai-chat",
                "state": "enabled",
            }),
        ),
        (
            "/prices",
            "ig-09-price",
            serde_json::json!({
                "price_book": INTEGRATION_PRICE_BOOK,
                "slug": "wave2-prices",
                "catalog": catalog_content,
                "catalog_version": 1,
                "state": "approved",
                "approved_at_millis": 1,
                "approval_citation": "IG-09 live price identity",
                "rules": [{
                    "provider": "openai",
                    "model": "gpt-4o",
                    "precedence": "baseline",
                    "from_millis": 0,
                    "input_nano_dollars_per_million": 2_500_000_000_u64,
                    "output_nano_dollars_per_million": 10_000_000_000_u64,
                    "origin": "operator",
                    "citation": "IG-09 live price identity",
                }],
            }),
        ),
        (
            "/aliases",
            "ig-03-alias",
            serde_json::json!({
                "alias": INTEGRATION_ALIAS,
                "tenant": INTEGRATION_TENANT,
                "project": INTEGRATION_PROJECT,
                "slug": INTEGRATION_ALIAS_SLUG,
                "wire_family": "openai-chat",
                "state": "enabled",
                "targets": [{ "enablement": INTEGRATION_ENABLEMENT }],
            }),
        ),
        (
            "/tenants",
            "ig-10-tenant-b",
            serde_json::json!({
                "summary": "publish the second isolation tenant",
                "mutation": "create",
                "resource": {
                    "tenant": SECOND_TENANT,
                    "slug": "wave2-other",
                    "display_name": "Wave 2 Other",
                },
            }),
        ),
        (
            "/projects",
            "ig-10-project-b",
            serde_json::json!({
                "summary": "publish the second isolation project",
                "mutation": "create",
                "resource": {
                    "project": SECOND_PROJECT,
                    "tenant": SECOND_TENANT,
                    "slug": "inference",
                    "display_name": "Other Inference",
                },
            }),
        ),
        (
            "/principals",
            "ig-10-principal-b",
            serde_json::json!({
                "principal": SECOND_PRINCIPAL,
                "tenant": SECOND_TENANT,
                "project": SECOND_PROJECT,
                "slug": "wave2-other-workload",
                "display_name": "Wave 2 Other workload",
                "key_digest": second_key_digest,
                "roles": ["operator"],
            }),
        ),
        (
            "/providers",
            "ig-10-provider-b",
            serde_json::json!({
                "provider": SECOND_PROVIDER,
                "tenant": SECOND_TENANT,
                "project": SECOND_PROJECT,
                "slug": "openai",
                "display_name": "Fixture OpenAI Other",
                "wire_family": "openai-chat",
                "endpoint": endpoint,
            }),
        ),
        (
            "/credentials",
            "ig-10-credential-b",
            serde_json::json!({
                "credential": SECOND_CREDENTIAL,
                "tenant": SECOND_TENANT,
                "project": SECOND_PROJECT,
                "provider": SECOND_PROVIDER,
                "slug": "openai-other",
                "display_name": "Fixture OpenAI other",
                "secret": second_secret_id,
                "secret_version": 1,
                "lifecycle": "active",
            }),
        ),
        (
            "/models",
            "ig-10-model-b",
            serde_json::json!({
                "enablement": SECOND_ENABLEMENT,
                "tenant": SECOND_TENANT,
                "project": SECOND_PROJECT,
                "slug": "gpt-4o-other",
                "offering": offering,
                "catalog": INTEGRATION_CATALOG,
                "snapshot": catalog_digest,
                "wire_family": "openai-chat",
                "state": "enabled",
            }),
        ),
        (
            "/aliases",
            "ig-10-alias-b",
            serde_json::json!({
                "alias": SECOND_ALIAS,
                "tenant": SECOND_TENANT,
                "project": SECOND_PROJECT,
                "slug": SECOND_ALIAS_SLUG,
                "wire_family": "openai-chat",
                "state": "enabled",
                "targets": [{ "enablement": SECOND_ENABLEMENT }],
            }),
        ),
    ];
    for (path, idempotency, document) in documents {
        revision = publish_resource(&replica, &http, path, idempotency, &revision, document).await;
    }

    let convergence = wait_for_convergence(&replica, &http, &revision, "control-plane").await;
    assert_eq!(convergence["converged"], true, "{convergence}");
    assert_eq!(
        client()
            .get(replica.url("/readyz"))
            .send()
            .await
            .expect("a readiness response")
            .status(),
        200,
        "{}",
        replica.output()
    );

    // Keep one direct replica as the operator that can publish the recovery
    // revision, and put a second serving replica behind the same cuttable TCP
    // path used by the fault qualification lane. This is process-level outage
    // evidence: Postgres is real and untouched, while the replica's live pool
    // connections are actually severed underneath it.
    let (upstream_host, _) = redirect(&control_plane.dsn, SocketAddr::from(([127, 0, 0, 1], 1)))
        .expect("the required Postgres fixture uses a redirectable TCP DSN");
    let postgres_proxy = FaultProxy::start(&upstream_host).await;
    let (_, proxied_dsn) = redirect(&control_plane.dsn, postgres_proxy.addr)
        .expect("the Postgres fixture DSN redirects through the fault proxy");
    let faulted = control_plane.serve_with_dsn(&proxied_dsn).await;
    let faulted_convergence =
        wait_for_convergence(&faulted, &http, &revision, "control-plane").await;

    let healthy_faulted_chat = http
        .post(faulted.url("/v1/chat/completions"))
        .bearer_auth(&workload_key)
        .json(&serde_json::json!({
            "model": INTEGRATION_ALIAS_SLUG,
            "messages": [{"role": "user", "content": "before-outage"}],
        }))
        .send()
        .await
        .expect("the proxied replica serves before the outage");
    assert_eq!(healthy_faulted_chat.status(), 200, "{}", faulted.output());

    postgres_proxy.set(Mode::Outage);
    let severed_deadline = Instant::now() + Duration::from_secs(5);
    while postgres_proxy.severed() == 0 {
        assert!(
            Instant::now() < severed_deadline,
            "the Postgres proxy did not sever a live connection"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let outage_state = faulted
        .breakglass(
            http.get(faulted.admin_url("/state")),
            "#219: read state during the Postgres outage",
        )
        .send()
        .await
        .expect("the outage administrative response");
    let outage_state_status = outage_state.status().as_u16();
    let outage_state_body = outage_state.text().await.expect("the outage state body");
    assert_eq!(outage_state_status, 503, "{outage_state_body}");

    let anonymous_state = http
        .get(faulted.admin_url("/state"))
        .send()
        .await
        .expect("the anonymous outage administrative response");
    let anonymous_state_status = anonymous_state.status().as_u16();
    assert_eq!(anonymous_state_status, 401, "{}", faulted.output());

    let outage_document = serde_json::json!({
        "summary": "probe a mutation during a Postgres outage",
        "mutation": "update",
        "resource": {
            "alias": INTEGRATION_ALIAS,
            "tenant": INTEGRATION_TENANT,
            "project": INTEGRATION_PROJECT,
            "slug": INTEGRATION_ALIAS_SLUG,
            "wire_family": "openai-chat",
            "state": "enabled",
            "targets": [{ "enablement": INTEGRATION_ENABLEMENT }],
        },
    });
    let outage_write = faulted
        .breakglass(
            http.post(faulted.admin_url("/aliases"))
                .header("idempotency-key", "ig-219-outage-write")
                .header("x-axond-expected-revision", &revision)
                .json(&outage_document),
            "#219: attempt a mutation during the Postgres outage",
        )
        .send()
        .await
        .expect("the outage mutation response");
    let outage_write_status = outage_write.status().as_u16();
    let outage_write_body = outage_write.text().await.expect("the outage mutation body");
    assert_eq!(outage_write_status, 503, "{outage_write_body}");

    let outage_chat = http
        .post(faulted.url("/v1/chat/completions"))
        .bearer_auth(&workload_key)
        .json(&serde_json::json!({
            "model": INTEGRATION_ALIAS_SLUG,
            "messages": [{"role": "user", "content": "during-outage"}],
        }))
        .send()
        .await
        .expect("the cached snapshot serves during the outage");
    let outage_chat_status = outage_chat.status().as_u16();
    assert_eq!(outage_chat_status, 200, "{}", faulted.output());
    let outage_chat_status_text = outage_chat_status.to_string();
    let outage_state_status_text = outage_state_status.to_string();
    let outage_write_status_text = outage_write_status.to_string();
    let anonymous_state_status_text = anonymous_state_status.to_string();

    support::stateful::write_recovery_artifact(
        &control_plane,
        "control-plane-outage",
        "serving",
        "control_plane_outage",
        &["serving_behavior", "fail_open_closed"],
        &[
            (
                "revision-converged",
                "the proxy replica served a complete revision before the cut",
            ),
            (
                "postgres-connections-severed",
                "the TCP fault proxy dropped live Postgres connections",
            ),
            (
                "inference-served",
                "the active snapshot answered while Postgres was unavailable",
            ),
        ],
        &[
            ("revision", serde_json::json!(revision.clone())),
            (
                "converged",
                serde_json::json!(faulted_convergence["converged"].clone()),
            ),
            (
                "proxy_severed_connections",
                serde_json::json!(postgres_proxy.severed()),
            ),
            ("inference_status", serde_json::json!(outage_chat_status)),
        ],
        &[(
            "max_serving_error_fraction",
            "0.0",
            "0.0",
            "the cached snapshot answered the offered outage request",
        )],
        &[
            (
                "inference_remains_available",
                "200",
                &outage_chat_status_text,
                "the active serving snapshot is independent of the unavailable journal",
            ),
            (
                "postgres_path_was_severed",
                "at-least-one",
                "at-least-one",
                "the outage was injected below the running process",
            ),
        ],
    );

    support::stateful::write_recovery_artifact(
        &control_plane,
        "control-plane-outage",
        "administration",
        "control_plane_outage",
        &["audit_auth"],
        &[
            (
                "authenticated-read-refused",
                "the authenticated desired-state read failed closed",
            ),
            (
                "mutation-refused",
                "the administrative mutation did not reach the journal",
            ),
            (
                "anonymous-refused",
                "the administrative route authenticated before disclosing state",
            ),
        ],
        &[
            (
                "authenticated_state_status",
                serde_json::json!(outage_state_status),
            ),
            ("mutation_status", serde_json::json!(outage_write_status)),
            (
                "anonymous_state_status",
                serde_json::json!(anonymous_state_status),
            ),
            ("authenticated_state_body", serde_json::json!("redacted")),
            ("mutation_body", serde_json::json!("redacted")),
        ],
        &[],
        &[
            (
                "authenticated_administration_refused",
                "503",
                &outage_state_status_text,
                "the authenticated read did not fall back to stale desired state",
            ),
            (
                "mutation_refused",
                "503",
                &outage_write_status_text,
                "the outage did not accept an administrative write",
            ),
            (
                "anonymous_administration_refused",
                "401",
                &anonymous_state_status_text,
                "authentication remained first during the outage",
            ),
        ],
    );

    let recovery_revision = publish_resource(
        &replica,
        &http,
        "/aliases",
        "ig-219-recovery-alias",
        &revision,
        outage_document,
    )
    .await;
    postgres_proxy.set(Mode::Pass);
    let recovered_convergence =
        wait_for_convergence(&faulted, &http, &recovery_revision, "control-plane").await;
    let recovered_chat = http
        .post(faulted.url("/v1/chat/completions"))
        .bearer_auth(&workload_key)
        .json(&serde_json::json!({
            "model": INTEGRATION_ALIAS_SLUG,
            "messages": [{"role": "user", "content": "after-recovery"}],
        }))
        .send()
        .await
        .expect("the recovered snapshot serves after Postgres returns");
    let recovered_chat_status = recovered_chat.status().as_u16();
    assert_eq!(recovered_chat_status, 200, "{}", faulted.output());
    let recovered_chat_status_text = recovered_chat_status.to_string();

    let recovered_audit = faulted
        .breakglass(
            http.get(faulted.admin_url(&format!("/audit/{recovery_revision}"))),
            "#219: read the recovered revision audit",
        )
        .send()
        .await
        .expect("the recovered audit response");
    let recovered_audit_status = recovered_audit.status().as_u16();
    let recovered_audit: serde_json::Value = recovered_audit
        .json()
        .await
        .expect("the recovered audit body");
    assert_eq!(recovered_audit_status, 200, "{}", faulted.output());
    assert_eq!(
        recovered_audit["events"][0]["actor"]["kind"], "breakglass",
        "recovery audit attribution survives: {recovered_audit}"
    );
    let recovered_audit_status_text = recovered_audit_status.to_string();

    support::stateful::write_recovery_artifact(
        &control_plane,
        "recovery-convergence",
        "serving",
        "recovery_convergence",
        &["serving_behavior"],
        &[
            (
                "journal-returned",
                "the Postgres fault proxy resumed forwarding",
            ),
            (
                "revision-converged",
                "the running replica loaded the revision published during the outage",
            ),
            (
                "inference-served",
                "the recovered serving snapshot answered traffic",
            ),
        ],
        &[
            ("revision", serde_json::json!(recovery_revision.clone())),
            (
                "source",
                serde_json::json!(recovered_convergence["source"].clone()),
            ),
            (
                "converged",
                serde_json::json!(recovered_convergence["converged"].clone()),
            ),
            ("chat_status", serde_json::json!(recovered_chat_status)),
        ],
        &[(
            "max_serving_error_fraction",
            "0.0",
            "0.0",
            "the recovered snapshot answered the offered request",
        )],
        &[
            (
                "recovered_revision_loaded",
                "true",
                "true",
                "the replica converged without restart or operator intervention",
            ),
            (
                "recovered_inference_served",
                "200",
                &recovered_chat_status_text,
                "serving resumed after the journal returned",
            ),
        ],
    );

    support::stateful::write_recovery_artifact(
        &control_plane,
        "recovery-convergence",
        "administration",
        "recovery_convergence",
        &["audit_auth"],
        &[
            (
                "journal-returned",
                "the recovered administrative surface reached Postgres",
            ),
            (
                "audit-read",
                "the recovered revision audit was read through authenticated admin",
            ),
        ],
        &[
            ("audit_status", serde_json::json!(recovered_audit_status)),
            (
                "actor",
                serde_json::json!(recovered_audit["events"][0]["actor"]["kind"].clone()),
            ),
        ],
        &[],
        &[
            (
                "recovered_audit_is_readable",
                "200",
                &recovered_audit_status_text,
                "the recovered control plane exposes its attributed audit trail",
            ),
            (
                "recovered_audit_is_authenticated",
                "breakglass",
                "breakglass",
                "the recovered audit event retains its actor attribution",
            ),
        ],
    );
    drop(faulted);
    // The outage drill publishes a real recovery revision through the direct
    // replica. Carry that new head into the remainder of the scenario before
    // publishing the tenant-isolation disablement below.
    revision = recovery_revision;

    let models = http
        .get(replica.url("/v1/models"))
        .bearer_auth(&workload_key)
        .send()
        .await
        .expect("a model discovery response");
    assert_eq!(models.status(), 200, "{}", replica.output());
    let models: serde_json::Value = models.json().await.expect("a model discovery body");
    assert!(
        models["data"].as_array().is_some_and(|entries| entries
            .iter()
            .any(|entry| entry["id"] == INTEGRATION_ALIAS_SLUG)),
        "the projected alias is discoverable: {models}"
    );
    assert!(
        models["data"]
            .as_array()
            .is_some_and(|entries| entries.iter().all(|entry| entry["id"] != SECOND_ALIAS_SLUG)),
        "tenant A discovery must not leak tenant B's alias: {models}"
    );
    let second_models = http
        .get(replica.url("/v1/models"))
        .bearer_auth(&second_workload_key)
        .send()
        .await
        .expect("the second tenant model discovery response");
    assert_eq!(second_models.status(), 200, "{}", replica.output());
    let second_models: serde_json::Value = second_models
        .json()
        .await
        .expect("the second tenant model discovery body");
    assert!(
        second_models["data"]
            .as_array()
            .is_some_and(
                |entries| entries.iter().any(|entry| entry["id"] == SECOND_ALIAS_SLUG)
                    && entries
                        .iter()
                        .all(|entry| entry["id"] != INTEGRATION_ALIAS_SLUG)
            ),
        "tenant B discovery must contain only tenant B's alias: {second_models}"
    );

    // Discovery is only half of the boundary. A caller must not be able to
    // enumerate a durable neighbour by comparing the refusal for its alias
    // with the refusal for a name that is not projected anywhere. Both must be
    // the same typed 404, and neither request may spend the neighbour's
    // credential by reaching the provider.
    let provider_requests_before_refusals = upstream.state.requests().len();
    let nonexistent = http
        .post(replica.url("/v1/chat/completions"))
        .bearer_auth(&workload_key)
        .json(&serde_json::json!({
            "model": "wave2-does-not-exist",
            "messages": [{"role": "user", "content": "unknown"}],
        }))
        .send()
        .await
        .expect("an unknown-model refusal");
    assert_eq!(nonexistent.status(), 404, "{}", replica.output());
    let nonexistent: serde_json::Value = nonexistent
        .json()
        .await
        .expect("an unknown-model error envelope");
    assert_eq!(nonexistent["error"]["type"], "unknown_model");

    let foreign = http
        .post(replica.url("/v1/chat/completions"))
        .bearer_auth(&workload_key)
        .json(&serde_json::json!({
            "model": SECOND_ALIAS_SLUG,
            "messages": [{"role": "user", "content": "foreign"}],
        }))
        .send()
        .await
        .expect("a cross-tenant alias refusal");
    assert_eq!(foreign.status(), 404, "{}", replica.output());
    let foreign: serde_json::Value = foreign.json().await.expect("a cross-tenant error envelope");
    assert_eq!(foreign["error"]["type"], nonexistent["error"]["type"]);
    assert_eq!(
        upstream.state.requests().len(),
        provider_requests_before_refusals,
        "cross-tenant and unknown aliases are refused before provider dispatch"
    );

    let catalogue = replica
        .breakglass(
            http.get(replica.admin_url(&format!(
                "/catalogue?tenant={INTEGRATION_TENANT}&project={INTEGRATION_PROJECT}"
            ))),
            "IG-10: inspect the healthy tenant catalogue",
        )
        .send()
        .await
        .expect("healthy catalogue response");
    assert_eq!(catalogue.status(), 200, "{}", replica.output());
    let catalogue: serde_json::Value = catalogue.json().await.expect("healthy catalogue body");
    assert!(
        catalogue.to_string().contains(INTEGRATION_ALIAS_SLUG),
        "the healthy tenant catalogue retains its alias: {catalogue}"
    );
    assert!(
        catalogue["aliases"]
            .as_array()
            .is_some_and(|aliases| aliases
                .iter()
                .all(|alias| alias["slug"] != SECOND_ALIAS_SLUG)),
        "tenant A's management catalogue must not enumerate tenant B: {catalogue}"
    );
    let entry = catalogue["entries"]
        .as_array()
        .and_then(|entries| entries.first())
        .unwrap_or_else(|| panic!("tenant A has a catalogue entry: {catalogue}"));
    assert_eq!(entry["metadata"]["provider"], "openai");
    assert_eq!(entry["metadata"]["published_model"], "gpt-4o");
    assert!(
        entry["metadata"]["capabilities"]
            .as_array()
            .is_some_and(|capabilities| capabilities.iter().any(|value| value == "tool-call")),
        "catalogue metadata carries the imported capability: {catalogue}"
    );
    assert_eq!(entry["price"]["source"], "operator");
    assert_eq!(entry["price"]["book_version"], 1);
    assert!(
        !catalogue["pending"]
            .as_array()
            .is_some_and(|pending| pending.iter().any(|value| value == "offering-metadata"))
    );
    let second_catalogue = replica
        .breakglass(
            http.get(replica.admin_url(&format!(
                "/catalogue?tenant={SECOND_TENANT}&project={SECOND_PROJECT}"
            ))),
            "IG-10: inspect the second tenant catalogue",
        )
        .send()
        .await
        .expect("second healthy catalogue response");
    assert_eq!(second_catalogue.status(), 200, "{}", replica.output());
    let second_catalogue: serde_json::Value = second_catalogue
        .json()
        .await
        .expect("second healthy catalogue body");
    assert!(
        second_catalogue.to_string().contains(SECOND_ALIAS_SLUG)
            && !second_catalogue
                .to_string()
                .contains(INTEGRATION_ALIAS_SLUG),
        "tenant B's management catalogue is isolated: {second_catalogue}"
    );

    revision = publish_resource(
        &replica,
        &http,
        "/aliases",
        "ig-10-alias-disable",
        &revision,
        serde_json::json!({
            "alias": INTEGRATION_ALIAS,
            "tenant": INTEGRATION_TENANT,
            "project": INTEGRATION_PROJECT,
            "slug": INTEGRATION_ALIAS_SLUG,
            "wire_family": "openai-chat",
            "state": "disabled",
            "targets": [{ "enablement": INTEGRATION_ENABLEMENT }],
        }),
    )
    .await;
    wait_for_convergence(&replica, &http, &revision, "control-plane").await;
    let disabled_models = http
        .get(replica.url("/v1/models"))
        .bearer_auth(&workload_key)
        .send()
        .await
        .expect("disabled alias discovery response");
    assert_eq!(disabled_models.status(), 200, "{}", replica.output());
    let disabled_models: serde_json::Value = disabled_models
        .json()
        .await
        .expect("disabled alias discovery body");
    assert!(!disabled_models["data"].as_array().is_some_and(|entries| {
        entries
            .iter()
            .any(|entry| entry["id"] == INTEGRATION_ALIAS_SLUG)
    }));

    revision = publish_resource(
        &replica,
        &http,
        "/aliases",
        "ig-10-alias-enable",
        &revision,
        serde_json::json!({
            "alias": INTEGRATION_ALIAS,
            "tenant": INTEGRATION_TENANT,
            "project": INTEGRATION_PROJECT,
            "slug": INTEGRATION_ALIAS_SLUG,
            "wire_family": "openai-chat",
            "state": "enabled",
            "targets": [{ "enablement": INTEGRATION_ENABLEMENT }],
        }),
    )
    .await;
    wait_for_convergence(&replica, &http, &revision, "control-plane").await;

    let chat = http
        .post(replica.url("/v1/chat/completions"))
        .bearer_auth(&workload_key)
        .json(&serde_json::json!({
            "model": INTEGRATION_ALIAS_SLUG,
            "messages": [{"role": "user", "content": "wave2"}],
        }))
        .send()
        .await
        .expect("a live inference response");
    assert_eq!(chat.status(), 200, "{}", replica.output());
    let first_request = upstream.state.last_request();
    assert_eq!(
        support::upstream::credential_digest(
            first_request.authorization.as_deref().unwrap_or_default()
        ),
        support::upstream::credential_digest(&first_material),
        "the first compiled snapshot resolved the first secret without exposing it"
    );

    wait_for_log(
        &replica,
        &["price_book", &format!("price/{INTEGRATION_PRICE_BOOK}@v1")],
    )
    .await;

    let rotated = replica
        .breakglass(
            http.post(replica.admin_url("/secrets/rotate"))
                .json(&serde_json::json!({
                    "tenant": INTEGRATION_TENANT,
                    "project": INTEGRATION_PROJECT,
                    "reference": first_reference,
                    "material": second_material,
                })),
            "IG-04: stage the rotated provider secret",
        )
        .send()
        .await
        .expect("a secret rotation response");
    assert_eq!(rotated.status(), 200, "{}", replica.output());
    let rotated: serde_json::Value = rotated.json().await.expect("a rotated secret body");
    let second_reference = rotated["reference"]
        .as_str()
        .expect("the rotated secret names a version")
        .to_owned();
    let activated = replica
        .breakglass(
            http.post(replica.admin_url("/secrets/lifecycle"))
                .json(&serde_json::json!({
                    "tenant": INTEGRATION_TENANT,
                    "project": INTEGRATION_PROJECT,
                    "reference": second_reference,
                    "lifecycle": "active",
                })),
            "IG-04: activate the rotated provider secret",
        )
        .send()
        .await
        .expect("the rotated secret activation response");
    assert_eq!(activated.status(), 200, "{}", replica.output());

    revision = publish_resource(
        &replica,
        &http,
        "/credentials",
        "ig-04-credential-rotate",
        &revision,
        serde_json::json!({
            "credential": INTEGRATION_CREDENTIAL,
            "tenant": INTEGRATION_TENANT,
            "project": INTEGRATION_PROJECT,
            "provider": INTEGRATION_PROVIDER,
            "slug": "openai-primary",
            "display_name": "Fixture OpenAI primary rotated",
            "secret": secret_id,
            "rotate": true,
            "lifecycle": "active",
        }),
    )
    .await;
    let convergence = wait_for_convergence(&replica, &http, &revision, "control-plane").await;
    assert_eq!(convergence["converged"], true, "{convergence}");
    let chat = http
        .post(replica.url("/v1/chat/completions"))
        .bearer_auth(&workload_key)
        .json(&serde_json::json!({
            "model": INTEGRATION_ALIAS_SLUG,
            "messages": [{"role": "user", "content": "rotated"}],
        }))
        .send()
        .await
        .expect("a rotated inference response");
    assert_eq!(chat.status(), 200, "{}", replica.output());
    let rotated_request = upstream.state.last_request();
    assert_eq!(
        support::upstream::credential_digest(
            rotated_request.authorization.as_deref().unwrap_or_default()
        ),
        support::upstream::credential_digest(&second_material),
        "the rotated revision resolved the new secret during compilation"
    );
    let rotation_audit = replica
        .breakglass(
            http.get(replica.admin_url(&format!("/audit/{revision}"))),
            "IG-04: read the rotated revision audit",
        )
        .send()
        .await
        .expect("a rotated revision audit response");
    assert_eq!(rotation_audit.status(), 200, "{}", replica.output());
    let rotation_audit: serde_json::Value = rotation_audit
        .json()
        .await
        .expect("a rotated revision audit body");
    assert_eq!(
        rotation_audit["events"][0]["actor"]["kind"], "breakglass",
        "the rotated revision audit retains authenticated attribution: {rotation_audit}"
    );
    support::stateful::write_recovery_artifact(
        &control_plane,
        "secret-rotation",
        "rotation",
        "secret_rotation",
        &["revisions", "convergence_lag"],
        &[
            (
                "secret-activated",
                "the successor secret version was activated",
            ),
            (
                "revision-published",
                "the credential revision was published",
            ),
            (
                "converged",
                "the running replica loaded the successor revision",
            ),
        ],
        &[
            ("revision", serde_json::json!(revision.clone())),
            ("source", serde_json::json!(convergence["source"].clone())),
            (
                "converged",
                serde_json::json!(convergence["converged"].clone()),
            ),
        ],
        &[],
        &[
            (
                "rotated_revision_published",
                "true",
                "true",
                "the successor credential reference was part of the published revision",
            ),
            (
                "no_restart",
                "true",
                "true",
                "the same replica served before and after the rotation",
            ),
        ],
    );
    support::stateful::write_recovery_artifact(
        &control_plane,
        "secret-rotation",
        "serving",
        "secret_rotation",
        &["serving_behavior", "audit_auth"],
        &[
            (
                "rotated-request-served",
                "the request completed through the rotated credential",
            ),
            (
                "audit-read",
                "the rotated publication was read through authenticated admin",
            ),
        ],
        &[
            ("chat_status", serde_json::json!(200)),
            ("audit_status", serde_json::json!(200)),
            (
                "credential",
                serde_json::json!("rotated-provider-reference"),
            ),
        ],
        &[(
            "max_serving_error_fraction",
            "0.0",
            "0.0",
            "the rotated request was answered successfully",
        )],
        &[
            (
                "rotated_material_authenticated_upstream",
                "true",
                "true",
                "the fake upstream observed the successor credential digest",
            ),
            (
                "authenticated_audit_attribution",
                "breakglass",
                "breakglass",
                "the rotated publication audit event retained its authenticated actor",
            ),
        ],
    );

    let signed = std::fs::read(&control_plane.cache_path).expect("the signed LKG cache exists");
    let compiled = std::fs::read(control_plane.cache_path.with_extension("serving"))
        .expect("the encrypted compiled-serving cache exists");
    assert!(
        !signed
            .windows(first_material.len())
            .any(|window| window == first_material.as_bytes())
    );
    assert!(
        !signed
            .windows(second_material.len())
            .any(|window| window == second_material.as_bytes())
    );
    assert!(
        !compiled
            .windows(first_material.len())
            .any(|window| window == first_material.as_bytes())
    );
    assert!(
        !compiled
            .windows(second_material.len())
            .any(|window| window == second_material.as_bytes())
    );
    assert_eq!(&compiled[..24], b"axond.compiled-serving\0\x02");

    drop(replica);
    let outage = control_plane.serve_without_control_plane().await;
    assert!(
        outage
            .output()
            .contains("restored compiled serving snapshot"),
        "cold boot must state that the compiled cache was restored:\n{}",
        outage.output()
    );
    let ready = http
        .get(outage.url("/readyz"))
        .send()
        .await
        .expect("outage readiness response");
    assert_eq!(ready.status(), 200, "{}", outage.output());
    let outage_convergence =
        wait_for_convergence(&outage, &http, &revision, "last-known-good").await;
    assert_eq!(
        outage_convergence["converged"], false,
        "{outage_convergence}"
    );
    let outage_models = http
        .get(outage.url("/v1/models"))
        .bearer_auth(&workload_key)
        .send()
        .await
        .expect("outage model discovery response");
    assert_eq!(outage_models.status(), 200, "{}", outage.output());
    let anonymous = http
        .get(outage.url("/v1/models"))
        .send()
        .await
        .expect("anonymous outage response");
    assert_eq!(anonymous.status(), 401, "{}", outage.output());
    let outage_chat = http
        .post(outage.url("/v1/chat/completions"))
        .bearer_auth(&workload_key)
        .json(&serde_json::json!({
            "model": INTEGRATION_ALIAS_SLUG,
            "messages": [{"role": "user", "content": "cold-start"}],
        }))
        .send()
        .await
        .expect("outage inference response");
    assert_eq!(outage_chat.status(), 200, "{}", outage.output());
    let catalogue = outage
        .breakglass(
            http.get(outage.admin_url(&format!("/catalogue?tenant={INTEGRATION_TENANT}"))),
            "IG-10: refuse an administrative catalogue read without the control plane",
        )
        .send()
        .await
        .expect("outage catalogue response");
    assert_eq!(catalogue.status(), 503, "{}", outage.output());
    support::stateful::write_recovery_artifact(
        &control_plane,
        "cold-boot-valid-cache",
        "serving",
        "cold_boot_valid_cache",
        &["serving_behavior", "fail_open_closed"],
        &[
            (
                "cache-restored",
                "the encrypted compiled-serving cache was restored",
            ),
            ("ready", "the restored replica answered readiness"),
            (
                "inference-served",
                "the restored snapshot answered inference",
            ),
            (
                "anonymous-refused",
                "the restored snapshot kept inbound auth enforced",
            ),
            (
                "administration-refused",
                "the administrative catalogue refused without Postgres",
            ),
        ],
        &[
            ("ready_status", serde_json::json!(200)),
            ("models_status", serde_json::json!(200)),
            ("chat_status", serde_json::json!(200)),
            ("anonymous_models_status", serde_json::json!(401)),
            ("admin_catalogue_status", serde_json::json!(503)),
            (
                "source",
                serde_json::json!(outage_convergence["source"].clone()),
            ),
            (
                "converged",
                serde_json::json!(outage_convergence["converged"].clone()),
            ),
        ],
        &[(
            "max_serving_error_fraction",
            "0.0",
            "0.0",
            "the cached inference request was answered successfully",
        )],
        &[
            (
                "readiness_serves_cached_snapshot",
                "200",
                "200",
                "the restored process reported ready",
            ),
            (
                "anonymous_inference_refused",
                "401",
                "401",
                "authentication remained enforced after cold start",
            ),
            (
                "administrative_read_refused_without_control_plane",
                "503",
                "503",
                "management reads did not pretend the serving cache was durable desired state",
            ),
        ],
    );

    drop(outage);

    let recovered = control_plane.serve().await;
    let recovered_convergence =
        wait_for_convergence(&recovered, &http, &revision, "control-plane").await;
    assert_eq!(
        recovered_convergence["converged"], true,
        "{recovered_convergence}"
    );
}

/// The cache refusal stages are independent of the full serving graph: a
/// replica with no valid projected snapshot must boot far enough to expose
/// authenticated administration, while readiness and inference remain closed.
/// Keeping this as a small real-process scenario also means the #158 evidence
/// does not depend on the provider fixture or on a successful inference call.
#[tokio::test]
async fn cold_boot_cache_refusals_are_ready_only_when_authenticated_and_projected() {
    let Some(control_plane) = ControlPlane::create().await else {
        eprintln!(
            "SKIPPED without AXOND_TEST_POSTGRES_DSN: cache refusal evidence requires a real stateful process"
        );
        return;
    };
    let migrated = control_plane.run(&["migrate", "apply"]);
    assert!(migrated.succeeded(), "{}", migrated.context());
    let http = client();

    let no_cache = control_plane.serve_without_control_plane_unready().await;
    let no_cache_ready = http
        .get(no_cache.url("/readyz"))
        .send()
        .await
        .expect("no-cache readiness response");
    let no_cache_admin = no_cache
        .breakglass(
            http.get(no_cache.admin_url("/state")),
            "recovery: inspect no-cache administrative state",
        )
        .send()
        .await
        .expect("no-cache administrative response");
    let no_cache_anon_admin = http
        .get(no_cache.admin_url("/state"))
        .send()
        .await
        .expect("no-cache anonymous administrative response");
    let no_cache_models = http
        .get(no_cache.url("/v1/models"))
        .send()
        .await
        .expect("no-cache inference response");
    assert_eq!(no_cache_ready.status(), 503, "{}", no_cache.output());
    assert_eq!(no_cache_admin.status(), 503, "{}", no_cache.output());
    assert_eq!(no_cache_anon_admin.status(), 401, "{}", no_cache.output());
    assert_eq!(no_cache_models.status(), 401, "{}", no_cache.output());
    support::stateful::write_recovery_artifact(
        &control_plane,
        "cold-boot-no-cache",
        "readiness",
        "cold_boot_no_cache",
        &["fail_open_closed", "audit_auth"],
        &[
            (
                "booted-without-cache",
                "the replica opened its authenticated administrative surface",
            ),
            (
                "readiness-refused",
                "the replica had no valid serving snapshot",
            ),
        ],
        &[
            ("ready_status", serde_json::json!(503)),
            ("admin_state_status", serde_json::json!(503)),
            ("anonymous_admin_status", serde_json::json!(401)),
            ("anonymous_models_status", serde_json::json!(401)),
        ],
        &[],
        &[
            (
                "readiness_refuses_without_cache",
                "503",
                "503",
                "a cacheless cold boot remains unready",
            ),
            (
                "authenticated_administration_refuses_without_control_plane",
                "503",
                "503",
                "the unready process keeps the administrative route but cannot read the unavailable journal",
            ),
            (
                "administration_requires_authentication",
                "401",
                "401",
                "the unready process does not weaken administrative authentication",
            ),
        ],
    );
    drop(no_cache);

    std::fs::write(&control_plane.cache_path, b"edited signed cache")
        .expect("write the invalid signed cache");
    std::fs::write(
        control_plane.cache_path.with_extension("serving"),
        b"edited compiled serving cache",
    )
    .expect("write the invalid compiled serving cache");
    let invalid_cache = control_plane.serve_without_control_plane_unready().await;
    let invalid_ready = http
        .get(invalid_cache.url("/readyz"))
        .send()
        .await
        .expect("invalid-cache readiness response");
    let invalid_admin = invalid_cache
        .breakglass(
            http.get(invalid_cache.admin_url("/state")),
            "recovery: inspect invalid-cache administrative state",
        )
        .send()
        .await
        .expect("invalid-cache administrative response");
    let invalid_anon_admin = http
        .get(invalid_cache.admin_url("/state"))
        .send()
        .await
        .expect("invalid-cache anonymous administrative response");
    let invalid_models = http
        .get(invalid_cache.url("/v1/models"))
        .send()
        .await
        .expect("invalid-cache inference response");
    assert_eq!(invalid_ready.status(), 503, "{}", invalid_cache.output());
    assert_eq!(invalid_admin.status(), 503, "{}", invalid_cache.output());
    assert_eq!(
        invalid_anon_admin.status(),
        401,
        "{}",
        invalid_cache.output()
    );
    assert_eq!(invalid_models.status(), 401, "{}", invalid_cache.output());
    support::stateful::write_recovery_artifact(
        &control_plane,
        "cold-boot-invalid-cache",
        "readiness",
        "cold_boot_invalid_cache",
        &["fail_open_closed", "audit_auth"],
        &[
            (
                "booted-with-invalid-cache",
                "the replica opened its authenticated administrative surface",
            ),
            (
                "readiness-refused",
                "cache authentication failed and no snapshot was published",
            ),
        ],
        &[
            ("ready_status", serde_json::json!(503)),
            ("admin_state_status", serde_json::json!(503)),
            ("anonymous_admin_status", serde_json::json!(401)),
            ("anonymous_models_status", serde_json::json!(401)),
        ],
        &[],
        &[
            (
                "readiness_refuses_with_invalid_cache",
                "503",
                "503",
                "an edited cache cannot make a replica ready",
            ),
            (
                "authenticated_administration_refuses_without_control_plane",
                "503",
                "503",
                "cache refusal does not pretend the unavailable journal is readable",
            ),
            (
                "administration_requires_authentication",
                "401",
                "401",
                "cache refusal does not weaken administrative authentication",
            ),
        ],
    );
    drop(invalid_cache);
}

/// Becomes: the qualification harness publishes stateful capacity envelopes and
/// failure-recovery evidence, including convergence under load and a rolling
/// upgrade.
#[test]
fn stateful_qualification_profiles_are_published() {
    let (config, env, _) = stateful_bootstrap();
    let run = stateful::run(&config, &["check", "preflight"], &env);
    assert!(
        !run.succeeded(),
        "IG-11 remains blocked until load and long-soak evidence exists"
    );
    assert!(run.reported().contains("serving"), "{}", run.context());
}
