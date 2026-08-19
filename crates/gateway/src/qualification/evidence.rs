//! The machine-readable evidence a recovery stage retains.
//!
//! One JSON document per stage under `target/recovery/`, carrying what happened
//! and what it was measured against: the timeline of the outage, the
//! observations the stage's evidence classes name, and a verdict per gate field
//! the stage is in a position to evaluate. A gate the stage cannot evaluate is
//! recorded as `not_evaluated` with the stage that will, rather than silently
//! omitted — an artifact that reported only the gates it happened to meet would
//! read as a qualified scenario.
//!
//! Provenance travels with the numbers for the same reason it does in the
//! capacity artifact (ADR 0033): a recovery result from another build, or from a
//! database this build did not migrate, is not comparable, and comparing it
//! anyway is how a recovery claim becomes folklore.
//!
//! Nothing written here is secret. The observations are revision ids, error
//! categories, durations, and counts; the signed cache the cold-boot stages
//! exercise carries secret *references* rather than material, and no artifact
//! field is fed from a resolved credential.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

/// The artifact schema. Bumped when a field a reader depends on changes
/// meaning, so an old artifact is recognisable rather than misread.
pub(crate) const EVIDENCE_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Artifact {
    pub(crate) schema_version: u32,
    /// The manifest scenario this stage belongs to.
    pub(crate) scenario: String,
    /// The stage within it. `scenario/stage` is the key the manifest and the
    /// driver registry agree on.
    pub(crate) stage: String,
    /// The lane that produced it, as the manifest spells it. A lane is checked
    /// against the stages it claims, so the artifact says which lane wrote it
    /// rather than leaving that to the file name.
    pub(crate) runner: String,
    pub(crate) capability: String,
    /// The evidence classes the manifest says this stage retains, echoed so a
    /// reader can check the artifact against the contract without loading it.
    pub(crate) evidence: Vec<String>,
    pub(crate) run: RunMeta,
    /// What happened, in order, from the start of the stage.
    pub(crate) timeline: Vec<Event>,
    /// The measurements, keyed by name. Strings and numbers only: an
    /// observation a reader has to deserialize a Rust type to interpret is not
    /// machine-readable evidence.
    pub(crate) observations: BTreeMap<String, Observation>,
    pub(crate) gates: Vec<Verdict>,
    /// The conditions the stage itself requires — the ones that are not
    /// manifest gate fields, like "the snapshot generation did not move during
    /// the outage".
    ///
    /// They are recorded rather than asserted so that a stage that fails still
    /// writes its evidence: an `assert!` in the middle of a stage unwinds
    /// before the artifact is written, which turns a real regression into a
    /// missing file, and a missing file is the one failure mode a retained
    /// evidence directory cannot describe.
    pub(crate) checks: Vec<Verdict>,
}

impl Artifact {
    /// The gate verdicts this stage evaluated and failed. Empty is the passing
    /// case; a caller asserts on it rather than on the artifact as a whole, so a
    /// failure names the gate rather than printing the document.
    pub(crate) fn failures(&self) -> Vec<&Verdict> {
        self.gates
            .iter()
            .chain(&self.checks)
            .filter(|verdict| verdict.outcome == Outcome::Failed)
            .collect()
    }

    /// A one-line human summary, for a runner's log.
    pub(crate) fn summary(&self) -> String {
        let evaluated = self
            .gates
            .iter()
            .filter(|verdict| verdict.outcome != Outcome::NotEvaluated)
            .count();
        format!(
            "{}/{}: {} events, {} observations, {evaluated}/{} gates evaluated, {} checks, {} \
             failed",
            self.scenario,
            self.stage,
            self.timeline.len(),
            self.observations.len(),
            self.gates.len(),
            self.checks.len(),
            self.failures().len(),
        )
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RunMeta {
    pub(crate) started_at_unix_ms: u128,
    pub(crate) elapsed_ms: u128,
    /// The build that produced the evidence.
    pub(crate) axond_version: &'static str,
    /// The durable backend the stage ran against. Always a real one: a stage
    /// with no database available writes no artifact at all.
    pub(crate) control_plane: String,
    /// The isolated schema the journal was created in, so a run can be traced
    /// back to the rows it wrote.
    pub(crate) schema: String,
    /// What the journal's migration ledger said after this build migrated it.
    /// Evidence from a database at a different schema version is not comparable,
    /// and this is how a reader can tell.
    pub(crate) schema_identity: String,
}

/// One thing that happened, offset from the start of the stage.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Event {
    pub(crate) at_ms: u128,
    /// A stable label: `severed`, `restored`, `publish-refused`, `converged`.
    pub(crate) event: String,
    /// What a reader needs to interpret it, including the error category a
    /// refusal carried.
    pub(crate) detail: String,
}

/// A measurement. Deliberately three shapes rather than arbitrary JSON: an
/// artifact is compared field by field across runs, and a shape that varies
/// cannot be.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub(crate) enum Observation {
    Text(String),
    Count(u64),
    Seconds(f64),
}

impl From<&str> for Observation {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl From<String> for Observation {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<u64> for Observation {
    fn from(value: u64) -> Self {
        Self::Count(value)
    }
}

impl From<std::time::Duration> for Observation {
    fn from(value: std::time::Duration) -> Self {
        Self::Seconds(value.as_secs_f64())
    }
}

/// What a gate field said about this stage.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Verdict {
    /// The manifest gate field, spelled as the manifest spells it.
    pub(crate) gate: String,
    pub(crate) bound: String,
    pub(crate) observed: String,
    pub(crate) outcome: Outcome,
    /// Why, for a reader who has the artifact and not the driver.
    pub(crate) detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Outcome {
    Met,
    Failed,
    /// This stage is not in a position to evaluate the field; another stage of
    /// the same scenario is, and is blocked.
    NotEvaluated,
}

/// Builds an [`Artifact`] while the stage runs.
pub(crate) struct Recorder {
    scenario: String,
    stage: String,
    runner: String,
    capability: String,
    evidence: Vec<String>,
    schema: String,
    schema_identity: String,
    control_plane: String,
    started: Instant,
    started_at_unix_ms: u128,
    timeline: Vec<Event>,
    observations: BTreeMap<String, Observation>,
    gates: Vec<Verdict>,
    checks: Vec<Verdict>,
}

impl Recorder {
    pub(crate) fn new(
        scenario: &str,
        stage: &str,
        runner: &str,
        capability: &str,
        evidence: &[&str],
        schema: &str,
        schema_identity: &str,
    ) -> Self {
        Self {
            scenario: scenario.to_owned(),
            stage: stage.to_owned(),
            runner: runner.to_owned(),
            capability: capability.to_owned(),
            evidence: evidence.iter().map(|class| (*class).to_owned()).collect(),
            schema: schema.to_owned(),
            schema_identity: schema_identity.to_owned(),
            control_plane: "postgres".to_owned(),
            started: Instant::now(),
            started_at_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("the clock is after the epoch")
                .as_millis(),
            timeline: Vec::new(),
            observations: BTreeMap::new(),
            gates: Vec::new(),
            checks: Vec::new(),
        }
    }

    /// Record something that happened, at the offset it happened at.
    pub(crate) fn mark(&mut self, event: &str, detail: impl Into<String>) {
        self.timeline.push(Event {
            at_ms: self.started.elapsed().as_millis(),
            event: event.to_owned(),
            detail: detail.into(),
        });
    }

    pub(crate) fn observe(&mut self, key: &str, value: impl Into<Observation>) {
        self.observations.insert(key.to_owned(), value.into());
    }

    /// Record a gate field this stage evaluated.
    pub(crate) fn gate(
        &mut self,
        gate: &str,
        bound: impl Into<String>,
        observed: impl Into<String>,
        met: bool,
        detail: impl Into<String>,
    ) {
        self.gates.push(Verdict {
            gate: gate.to_owned(),
            bound: bound.into(),
            observed: observed.into(),
            outcome: if met { Outcome::Met } else { Outcome::Failed },
            detail: detail.into(),
        });
    }

    /// Record a gate field this stage cannot evaluate, and say who does.
    pub(crate) fn deferred(
        &mut self,
        gate: &str,
        bound: impl Into<String>,
        why: impl Into<String>,
    ) {
        self.gates.push(Verdict {
            gate: gate.to_owned(),
            bound: bound.into(),
            observed: "not measured".to_owned(),
            outcome: Outcome::NotEvaluated,
            detail: why.into(),
        });
    }

    /// Record a condition the stage requires, comparing what it expected with
    /// what it saw.
    ///
    /// This is the driver's `assert_eq!`, with the difference that matters: it
    /// keeps running to the end of the stage and fails through the artifact, so
    /// the evidence for the failure is written before the failure is raised.
    pub(crate) fn require(
        &mut self,
        check: &str,
        expected: impl std::fmt::Display,
        observed: impl std::fmt::Display,
        detail: impl Into<String>,
    ) {
        let (expected, observed) = (expected.to_string(), observed.to_string());
        let met = expected == observed;
        self.checks.push(Verdict {
            gate: check.to_owned(),
            bound: expected,
            observed,
            outcome: if met { Outcome::Met } else { Outcome::Failed },
            detail: detail.into(),
        });
    }

    /// The same, for a condition that is already a boolean.
    pub(crate) fn require_that(&mut self, check: &str, held: bool, detail: impl Into<String>) {
        self.require(check, true, held, detail);
    }

    /// Whether a check this stage recorded held — for the few places where the
    /// stage cannot carry on meaningfully once one has failed.
    ///
    /// A name nothing recorded did not hold: the names are free-form strings, so
    /// a typo would otherwise read as a pass and carry the stage on.
    pub(crate) fn held(&self, check: &str) -> bool {
        let mut recorded = self
            .checks
            .iter()
            .filter(|verdict| verdict.gate == check)
            .peekable();
        recorded.peek().is_some() && recorded.all(|verdict| verdict.outcome != Outcome::Failed)
    }

    pub(crate) fn finish(self) -> Artifact {
        Artifact {
            schema_version: EVIDENCE_SCHEMA_VERSION,
            scenario: self.scenario,
            stage: self.stage,
            runner: self.runner,
            capability: self.capability,
            evidence: self.evidence,
            run: RunMeta {
                started_at_unix_ms: self.started_at_unix_ms,
                elapsed_ms: self.started.elapsed().as_millis(),
                axond_version: env!("CARGO_PKG_VERSION"),
                control_plane: self.control_plane,
                schema: self.schema,
                schema_identity: self.schema_identity,
            },
            timeline: self.timeline,
            observations: self.observations,
            gates: self.gates,
            checks: self.checks,
        }
    }
}

/// The workspace root, resolved from this crate rather than from whatever
/// working directory the test runner happens to have.
pub(crate) fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recorder() -> Recorder {
        Recorder::new(
            "control-plane-outage",
            "journal-outage",
            "stateful-tests",
            "control_plane_outage",
            &["outage_timeline"],
            "recovery_1",
            "Current { version: 9 }",
        )
    }

    /// A gate a stage could not measure is not a gate a stage passed. Both are
    /// carried, and only the failed one fails the run.
    #[test]
    fn a_deferred_gate_is_neither_a_pass_nor_a_failure() {
        let mut recorder = recorder();
        recorder.gate(
            "admin_writes",
            "unavailable",
            "unavailable",
            true,
            "refused",
        );
        recorder.deferred("readiness", "serves", "the `serving` stage is blocked");
        recorder.gate("max_data_loss_revisions", "0", "1", false, "lost one");
        let artifact = recorder.finish();

        assert_eq!(artifact.gates.len(), 3);
        assert_eq!(
            artifact.failures().len(),
            1,
            "only the evaluated, unmet gate fails the stage"
        );
        assert!(artifact.summary().contains("2/3 gates evaluated"));
    }

    /// The caveat this schema exists to close: a stage whose condition failed
    /// still produces an artifact, and the artifact is what fails the run.
    #[test]
    fn a_failed_check_fails_the_stage_through_the_artifact() {
        let mut recorder = recorder();
        recorder.require("active_revision_survived_the_cut", "rev_1", "rev_1", "held");
        recorder.require_that("the_publish_was_retryable", false, "it was not");
        assert!(recorder.held("active_revision_survived_the_cut"));
        assert!(!recorder.held("the_publish_was_retryable"));
        assert!(
            !recorder.held("a_check_nobody_recorded"),
            "an unrecorded condition did not hold, so a mistyped name cannot read as a pass"
        );

        let artifact = recorder.finish();
        let failures = artifact.failures();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].gate, "the_publish_was_retryable");
        assert!(artifact.summary().contains("2 checks, 1 failed"));
    }

    /// The provenance a reader needs to know whether two artifacts are
    /// comparable travels in the document, not in the runner's log.
    #[test]
    fn the_artifact_carries_the_build_and_the_schema_it_ran_against() {
        let artifact = recorder().finish();
        assert_eq!(artifact.run.control_plane, "postgres");
        assert_eq!(artifact.run.schema, "recovery_1");
        assert_eq!(artifact.run.schema_identity, "Current { version: 9 }");
        assert_eq!(artifact.run.axond_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(artifact.schema_version, EVIDENCE_SCHEMA_VERSION);
    }

    /// Observations serialize as the scalars they are, so a reader can compare
    /// two runs field by field without knowing any Rust type.
    #[test]
    fn observations_serialize_as_plain_scalars() {
        let mut recorder = recorder();
        recorder.observe("active_revision", "rev_1");
        recorder.observe("consecutive_convergence_failures", 3u64);
        recorder.observe("cold_start_seconds", std::time::Duration::from_millis(1500));
        let json = serde_json::to_value(recorder.finish()).expect("serializes");
        let observations = &json["observations"];
        assert_eq!(observations["active_revision"], "rev_1");
        assert_eq!(observations["consecutive_convergence_failures"], 3);
        assert_eq!(observations["cold_start_seconds"], 1.5);
    }
}
