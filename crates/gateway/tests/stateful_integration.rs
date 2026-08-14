//! The #160 integration smoke harness: one scenario per stateful release gate.
//!
//! The stateful contracts land as separate slices — durable schemas, typed
//! documents, protocol boundaries — and each is tested against itself. This
//! suite tests the *seams*: what a deployment does when the pieces are put
//! together, which is where every release gate in
//! [#160](https://github.com/Litvue/axond/issues/160) actually lives.
//!
//! Two rules keep it honest while stateful mode is still being assembled.
//!
//! **A gate is `Wired` only when its scenario asserts the property on a running
//! process.** Not when its dependencies merged, and not when a type exists.
//!
//! **A `Blocked` gate still runs.** Its scenario asserts the standing refusal:
//! a stateful replica boots and serves `/admin/v1`, and refuses *inference*
//! rather than serving the empty snapshot an uncompiled revision would leave
//! behind. That refusal is what makes the unproven gates safe, so it is the
//! thing worth testing until each one is wired — and when convergence lands,
//! the scenario fails here and has to be rewritten into the real assertion
//! instead of quietly passing.
//!
//! **A gate is `Partial` when a running process proves the path that exists and
//! a named slice still owns the rest.** IG-05 is the current one: a mutation is
//! validated, revisioned, and audited against a real control plane through the
//! breakglass credential, while authorizing an OIDC principal against a scoped
//! grant waits on a replica that authenticates one.
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
        status: Status::Blocked,
        scenarios: &["hydrate_compile_publish_is_one_atomic_step"],
    },
    Gate {
        id: "IG-04",
        status: Status::Blocked,
        scenarios: &["secrets_resolve_during_compilation_only"],
    },
    Gate {
        id: "IG-05",
        status: Status::Partial,
        scenarios: &["an_admin_mutation_publishes_an_audited_revision"],
    },
    Gate {
        id: "IG-06",
        status: Status::Blocked,
        scenarios: &["inference_touches_no_control_plane_connection"],
    },
    Gate {
        id: "IG-07",
        status: Status::Blocked,
        scenarios: &["control_plane_loss_keeps_the_last_known_good_snapshot_serving"],
    },
    Gate {
        id: "IG-08",
        status: Status::Blocked,
        scenarios: &["readiness_and_status_report_convergence"],
    },
    Gate {
        id: "IG-09",
        status: Status::Blocked,
        scenarios: &["every_usage_record_names_the_price_version"],
    },
    Gate {
        id: "IG-10",
        status: Status::Blocked,
        scenarios: &["a_tenant_catalogue_is_isolated_and_explains_itself"],
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

/// A stateful deployment still refuses *inference*, so a gate that depends on a
/// revision reaching the request path cannot be silently unproven: a replica
/// administers, and nothing it is told to serve is served.
///
/// Every blocked gate asserts this, through the operator-facing report of it:
/// `check preflight` fails on its `serving` line. When convergence lands, these
/// assertions fail — which is the point: each gate is then rewritten into the
/// property it was always meant to prove, and its matrix row moves with it.
fn stateful_serving_is_still_refused(gate: &str) {
    let (config, env, _bind) = stateful_bootstrap();
    let run = stateful::run(&config, &["check", "preflight"], &env);
    assert!(
        !run.succeeded(),
        "{gate}: a stateful deployment no longer refuses inference, so this gate's real scenario is \
         now possible and required. Rewrite it and move its row in \
         docs/operations/stateful-integration.md.\n{}",
        run.context()
    );
    assert!(
        run.reported().contains("serving"),
        "{gate}: preflight failed for some reason other than the serving refusal, which is not \
         what this gate is waiting on.\n{}",
        run.context()
    );
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

// ── Gates whose wiring has not landed ────────────────────────────────────────
//
// Each of these is a placeholder for one property, and each asserts the refusal
// that stands in for it. The doc comment on every scenario is the assertion it
// becomes; see docs/operations/stateful-integration.md for what it waits on.

/// Becomes: a published revision is hydrated, compiled, and swapped in as one
/// whole snapshot, and a candidate that fails any step changes nothing.
#[test]
fn hydrate_compile_publish_is_one_atomic_step() {
    stateful_serving_is_still_refused("IG-03");
}

/// Becomes: every credential a snapshot needs is resolved through the
/// SecretStore during compilation, and rotating one is a new snapshot rather
/// than a redeploy.
#[test]
fn secrets_resolve_during_compilation_only() {
    stateful_serving_is_still_refused("IG-04");
}

// ── IG-05: validated, revisioned, authorized, audited mutations ──────────────

/// One authenticated mutation, followed from the request to the revision it
/// published and the audit event that attributes it.
///
/// `partial` in the matrix, and precisely: everything here happens through the
/// breakglass credential, which is the only administrative principal a
/// deployment has before it has been administered into existence. Authorizing an
/// OIDC principal against a scoped grant waits on a replica that authenticates
/// one, and gets a scenario of its own rather than an assumption here.
#[tokio::test]
async fn an_admin_mutation_publishes_an_audited_revision() {
    let Some(control_plane) = ControlPlane::create().await else {
        eprintln!(
            "SKIPPED without AXOND_TEST_POSTGRES_DSN: IG-05's `partial` row is NOT proven by this \
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

/// Becomes: a served inference request opens no control-plane connection and
/// issues no control-plane query, observed with the database made unreachable.
#[test]
fn inference_touches_no_control_plane_connection() {
    stateful_serving_is_still_refused("IG-06");
}

/// Becomes: with the control plane down, a running replica keeps serving its
/// active snapshot and a restarting one cold-boots from the signed
/// last-known-good cache.
#[test]
fn control_plane_loss_keeps_the_last_known_good_snapshot_serving() {
    stateful_serving_is_still_refused("IG-07");
}

/// Becomes: readiness reflects convergence rather than process liveness, and
/// `/status` reports desired, loaded, active, and lag.
#[test]
fn readiness_and_status_report_convergence() {
    stateful_serving_is_still_refused("IG-08");
}

/// Becomes: every usage record names the approved price-book version the
/// request was charged against.
#[test]
fn every_usage_record_names_the_price_version() {
    stateful_serving_is_still_refused("IG-09");
}

/// Becomes: a tenant's catalogue view contains only what that tenant may call
/// and explains why each entry is available or not.
#[test]
fn a_tenant_catalogue_is_isolated_and_explains_itself() {
    stateful_serving_is_still_refused("IG-10");
}

/// Becomes: the qualification harness publishes stateful capacity envelopes and
/// failure-recovery evidence, including convergence under load and a rolling
/// upgrade.
#[test]
fn stateful_qualification_profiles_are_published() {
    stateful_serving_is_still_refused("IG-11");
}
