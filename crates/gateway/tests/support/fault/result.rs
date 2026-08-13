//! The machine-readable fault-matrix artifact.
//!
//! One JSON document per row. Everything issue #218 asks a row to retain is a
//! field here rather than an assertion that lived and died inside a test
//! process: what was injected and when, how the gateway classified it, which
//! bound ended it, how many attempts it cost, whether the upstream was cleaned
//! up, what the request settled as, what telemetry it produced, and the
//! leakage scan of every surface a caller or an operator can see.
//!
//! Provenance is shared with the capacity harness, so a fault artifact and a
//! capacity artifact from the same commit name the same binary, config,
//! fixtures, and machine.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::Serialize;

use super::manifest::{self, Row};
pub use crate::support::capacity::result::{Environment, RunMeta};

#[derive(Debug, Clone, Serialize)]
pub struct FaultResult {
    pub schema_version: u32,
    pub row: RowEcho,
    pub run: RunMeta,
    pub environment: Environment,
    pub injection: Injection,
    pub classification: Classification,
    pub deadline: Deadline,
    pub retries: Retries,
    pub cleanup: Cleanup,
    pub usage: UsageOutcome,
    pub telemetry: Telemetry,
    pub leakage: Leakage,
    pub verdicts: Vec<Verdict>,
}

impl FaultResult {
    pub fn failures(&self) -> Vec<&Verdict> {
        self.verdicts.iter().filter(|v| !v.passed).collect()
    }

    /// Write the artifact under `target/faults/<family>/<row>.json` and return
    /// where it landed.
    pub fn write(&self) -> PathBuf {
        let dir = manifest::workspace_root()
            .join("target/faults")
            .join(&self.row.family);
        std::fs::create_dir_all(&dir).expect("the fault artifact directory is writable");
        let path = dir.join(format!("{}.json", self.row.id));
        let json = serde_json::to_string_pretty(self).expect("the result artifact serializes");
        std::fs::write(&path, format!("{json}\n")).expect("the fault artifact is writable");
        path
    }

    pub fn summary(&self) -> String {
        format!(
            "{} [{}]: {} -> {} {} in {} ms (attempts {}, usage {}, telemetry {} exports, leaks {})",
            self.row.id,
            self.row.family,
            self.injection.fault,
            self.classification
                .status
                .map_or_else(|| "transport-failure".to_owned(), |s| s.to_string()),
            self.classification.error_type.as_deref().unwrap_or("-"),
            self.deadline.elapsed_ms,
            self.retries.attempts,
            self.usage.measured_status.as_deref().unwrap_or("none"),
            self.telemetry.exports.values().sum::<u64>(),
            self.leakage.findings.len(),
        )
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RowEcho {
    pub id: String,
    pub family: String,
    pub fault: String,
    pub description: String,
    pub streamed: bool,
}

impl RowEcho {
    pub fn new(row: &Row) -> Self {
        Self {
            id: row.id.clone(),
            family: row.family.as_str().to_owned(),
            fault: row.fault.as_str().to_owned(),
            description: row.description.clone(),
            streamed: row.streamed,
        }
    }
}

/// What was injected, and when. A row's evidence starts here: a classification
/// without the fault that produced it is an anecdote.
#[derive(Debug, Clone, Serialize)]
pub struct Injection {
    pub fault: String,
    pub family: String,
    /// The state tier a backend row needs, if any.
    pub service: Option<String>,
    /// The configured behaviour when that tier cannot answer.
    pub on_unavailable: Option<String>,
    /// How the fault was produced, in words. Never an address that could carry
    /// a credential: the endpoints are loopback fixtures and are named as such.
    pub how: String,
    pub injected_latency_ms: Option<u64>,
    pub outage: Option<Outage>,
    pub timing: Timing,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Outage {
    pub began_at_unix_ms: u128,
    /// `None` for a row that leaves the tier down.
    pub restored_at_unix_ms: Option<u128>,
    pub duration_ms: u128,
    /// Connections the proxy carried before the outage, and how many it tore
    /// down: an outage that severed nothing was not an outage.
    pub connections_carried: u64,
    pub connections_severed: u64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Timing {
    pub started_at_unix_ms: u128,
    pub elapsed_ms: u128,
    /// Time to the caller's first byte of answer.
    pub first_byte_ms: Option<u128>,
}

/// How the gateway answered. The typed error contract is the product here: a
/// caller has to be able to tell a provider verdict from a bound the gateway
/// imposed without reading a message.
#[derive(Debug, Clone, Serialize)]
pub struct Classification {
    pub status: Option<u16>,
    pub error_type: Option<String>,
    /// The transport phase named in the error body, when it names one.
    pub phase: Option<String>,
    /// The request never got an answer at all.
    pub transport_failure: bool,
    /// Bytes of provider output the caller received before the fault ended the
    /// request. A committed byte is what forbids a retry.
    pub relayed_output_bytes: u64,
    /// Recovery rows: what the caller saw while the tier was down.
    pub during_outage_status: Option<u16>,
    /// Recovery rows: what the caller saw once it came back.
    pub after_recovery_status: Option<u16>,
    /// Transport rows: the caller's answer names no endpoint, so the operator's
    /// log is the only surface left carrying why the call failed. `None` on a
    /// row that injects no endpoint of its own.
    pub operator_reason_retained: Option<bool>,
}

/// Which bound was supposed to end the request, and whether it did.
#[derive(Debug, Clone, Serialize)]
pub struct Deadline {
    /// The configured bound the row exercises, named as the config names it.
    pub bound: String,
    pub bound_ms: Option<u64>,
    /// The wall-clock ceiling the row declares.
    pub wall_clock_ms: u64,
    pub elapsed_ms: u128,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Retries {
    /// Attempts the usage record accounts for.
    pub attempts: u64,
    /// Dispatches the fake provider actually saw for the measured request.
    pub upstream_requests: u64,
    /// The walk's ceiling, for context.
    pub max_attempts: u32,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Cleanup {
    pub upstream_streams_opened: u64,
    /// Upstream response bodies still open once the caller is gone: a leak.
    pub upstream_streams_open_at_end: i64,
    pub settled_within_ms: u128,
    /// Whether the process shut down cleanly after the row, which is also what
    /// flushes its telemetry.
    pub process_exited_cleanly: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct UsageOutcome {
    pub records: u64,
    pub by_status: BTreeMap<String, u64>,
    /// The status of the measured request's record.
    pub measured_status: Option<String>,
    pub cost_microdollars: Option<u64>,
    /// Whether the record carried the identity an operator correlates by.
    pub carries_request_id: bool,
    /// How a record was decided to belong to the measured request. Position in
    /// the stream and a quiet window are not attribution: a priming record that
    /// lands late is still the priming request's, and the identity says so.
    pub attributed_by: String,
    /// Records this row settled for an earlier request — a backend row's
    /// priming request and its outage probe — recognised by their identities
    /// and excluded rather than waited out.
    pub records_before_measured: u64,
    /// Records whose identity could not be read at all, so nothing could be
    /// attributed by it. Any at all is a finding.
    pub unattributable_records: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Telemetry {
    /// The row exported to a collector the harness owns; the endpoint is a
    /// loopback fixture.
    pub collector: bool,
    pub exports: BTreeMap<String, u64>,
    pub bytes: u64,
    /// Instruments the row expected to see counted, and whether they arrived.
    pub metrics_observed: Vec<String>,
    pub metrics_missing: Vec<String>,
    /// Span names seen in the exported traces.
    pub spans_observed: Vec<String>,
}

/// The leakage scan. Needles are never written into the artifact — a matrix
/// that records evidence of a leak by quoting the leak is the same leak.
#[derive(Debug, Clone, Serialize)]
pub struct Leakage {
    pub surfaces: Vec<Surface>,
    /// How many needles of each kind were searched for.
    pub needles: BTreeMap<String, u64>,
    pub findings: Vec<Finding>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Surface {
    pub name: String,
    pub bytes_scanned: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Finding {
    pub surface: String,
    pub kind: String,
    /// A label for the needle, never its value.
    pub needle: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Verdict {
    pub check: String,
    pub expected: String,
    pub observed: String,
    pub passed: bool,
}

impl Verdict {
    pub fn equals<T: std::fmt::Debug + PartialEq>(check: &str, expected: T, observed: T) -> Self {
        Self {
            check: check.to_owned(),
            expected: format!("{expected:?}"),
            observed: format!("{observed:?}"),
            passed: expected == observed,
        }
    }

    pub fn at_most(check: &str, value: u128, bound: u128) -> Self {
        Self {
            check: check.to_owned(),
            expected: format!("<= {bound}"),
            observed: value.to_string(),
            passed: value <= bound,
        }
    }

    pub fn at_least(check: &str, value: u128, bound: u128) -> Self {
        Self {
            check: check.to_owned(),
            expected: format!(">= {bound}"),
            observed: value.to_string(),
            passed: value >= bound,
        }
    }

    pub fn holds(check: &str, passed: bool, observed: impl Into<String>) -> Self {
        Self {
            check: check.to_owned(),
            expected: "true".to_owned(),
            observed: observed.into(),
            passed,
        }
    }
}
