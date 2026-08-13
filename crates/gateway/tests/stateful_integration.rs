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
//! `serve` declines a stateful config rather than serving the empty snapshot an
//! unread control plane would leave behind. That refusal is what makes the
//! unproven gates safe, so it is the thing worth testing until each one is
//! wired — and when the wiring lands, the scenario fails here and has to be
//! rewritten into the real assertion instead of quietly passing.
//!
//! The gate table below and the matrix in
//! `docs/operations/stateful-integration.md` are checked against each other, so
//! neither can drift: an integration pull request that unblocks a gate has to
//! move the row, the status, and the scenario together.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use support::stateful::{self, ControlPlane};
use support::{GATEWAY_KEY, boot, client};

/// Whether a gate's property is proven by its scenario, or still waiting on the
/// slices named in the matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Status {
    Wired,
    Blocked,
}

impl Status {
    fn parse(text: &str) -> Self {
        match text {
            "wired" => Self::Wired,
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
        status: Status::Blocked,
        scenarios: &[
            "stateless_boot_serves_with_no_control_plane",
            "stateful_boot_refuses_to_serve_an_empty_snapshot",
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
        status: Status::Blocked,
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

// ── The standing refusal every blocked gate rests on ─────────────────────────

/// A complete stateful bootstrap whose references are satisfied — the config
/// closest to a production one that can exist before the control plane is wired.
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
    // A DSN that resolves but points nowhere: the refusal must come from the
    // missing wiring, not from an unreachable database, so the scenario cannot
    // pass for the wrong reason.
    let env = BTreeMap::from([
        (
            stateful::DSN_ENV,
            "postgres://axond@127.0.0.1:1/axond".to_owned(),
        ),
        (
            stateful::KEK_ENV,
            "integration-test-kek-0123456789abcdef".to_owned(),
        ),
        (
            stateful::BREAKGLASS_ENV,
            "integration-test-breakglass".to_owned(),
        ),
    ]);
    (config, env, bind)
}

/// `serve` still declines a stateful config, so a gate that depends on stateful
/// serving cannot be silently unproven: nothing serves.
///
/// Every blocked gate asserts this. When the wiring lands, these assertions fail
/// — which is the point: each gate is then rewritten into the property it was
/// always meant to prove, and its matrix row moves with it.
fn stateful_serving_is_still_refused(gate: &str) {
    let (config, env, _bind) = stateful_bootstrap();
    let run = stateful::run(&config, &["check", "preflight"], &env);
    assert!(
        !run.succeeded(),
        "{gate}: `serve` no longer refuses a stateful config, so this gate's real scenario is now \
         possible and required. Rewrite it and move its row in \
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

#[test]
fn stateful_boot_refuses_to_serve_an_empty_snapshot() {
    let (config, env, bind) = stateful_bootstrap();
    let mut command = std::process::Command::new(stateful::axond());
    command
        .env_clear()
        .env("AXOND_CONFIG", &config)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(path) = std::env::var_os("PATH") {
        command.env("PATH", path);
    }
    for (key, value) in &env {
        command.env(key, value);
    }
    let mut child = command.spawn().expect("the axond binary runs");

    // A refusal exits immediately. Waiting without a deadline would instead hang
    // the suite for as long as CI allows on the day `serve` learns to boot
    // statefully — the very change this scenario exists to catch.
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
    let output = child.wait_with_output().expect("the child's output");
    let reported = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let status = status.unwrap_or_else(|| {
        panic!(
            "IG-01: a stateful boot kept running instead of refusing, so this gate's real scenario \
             is now possible and required. Rewrite it and move its row in \
             docs/operations/stateful-integration.md.\n{reported}"
        )
    });

    assert!(
        !status.success(),
        "a stateful replica must refuse to start rather than serve an empty snapshot:\n{reported}"
    );
    assert!(
        reported.contains("stateful"),
        "the refusal must name the mode it refuses, so an operator can act on it:\n{reported}"
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

    // 4. Preflight now describes a prepared database. It still fails overall,
    //    because `serve` still refuses stateful mode (IG-01), and reporting that
    //    honestly is what keeps a rollout from gating green on a crash loop.
    let preflight = control_plane.run(&["check", "preflight"]);
    let reported = preflight.reported();
    assert!(
        reported.contains("control-plane database") && !reported.contains(&control_plane.dsn),
        "preflight names the control plane by reference, never by DSN:\n{}",
        preflight.context()
    );
    assert!(
        !preflight.succeeded() && reported.contains("serving"),
        "preflight must keep reporting the serving refusal until IG-01 is wired:\n{}",
        preflight.context()
    );

    // The schema drops with `control_plane`, on this path and on every failing
    // one.
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

/// Becomes: an authenticated `/admin/v1` mutation is validated, revisioned,
/// authorized, and audited in the transaction that publishes it.
#[test]
fn an_admin_mutation_publishes_an_audited_revision() {
    stateful_serving_is_still_refused("IG-05");
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
