//! The committed stateful endurance manifest: every input a run reads, and
//! every criterion it is judged by.
//!
//! Data rather than code, as the capacity, endurance, and rollout manifests
//! are: the artifact names the inputs that produced it, and the manifest's hash
//! is recorded beside the binary's (ADR 0033). The part that is specific to
//! this slice is that the *criteria* are committed too — a stateful run is
//! judged on convergence, durability, and isolation, and none of those means
//! anything unless the bound was written down before the run.

use std::path::PathBuf;
use std::time::Duration;

use figment::Figment;
use figment::providers::{Format, Toml};
use serde::{Deserialize, Serialize};

use crate::support::capacity::manifest::workspace_root;
use crate::support::endurance::manifest::Mix;

/// The manifest, relative to the workspace root.
pub const MANIFEST_RELATIVE: &str = "qualification/stateful-endurance/manifest.toml";

/// The manifest schema this harness understands.
pub const MANIFEST_SCHEMA_VERSION: u32 = 2;

/// The result-artifact schema version. Bumped when a field changes meaning, so
/// a stored artifact is never reinterpreted under a newer contract.
pub const RESULT_SCHEMA_VERSION: u32 = 3;

/// Overrides the soak tier's duration, for an operator dispatching a shorter
/// run than the manifest commits to. The soak alone, for the reason the
/// stateless harness gives: both tiers live in one binary, and honouring it for
/// the smoke tier would offer the dispatched duration twice.
pub const DURATION_ENV: &str = "AXOND_STATEFUL_ENDURANCE_DURATION_MS";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Manifest {
    pub schema_version: u32,
    #[serde(rename = "profile")]
    pub profiles: Vec<Profile>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Profile {
    pub id: String,
    pub description: String,
    pub seed: u64,
    pub mix: Mix,
    pub smoke: Scale,
    pub soak: Scale,
    pub schedule: Schedule,
    pub slo: Slo,
    pub termination: Termination,
}

impl Profile {
    pub fn scale(&self, tier: Tier) -> &Scale {
        match tier {
            Tier::Smoke => &self.smoke,
            Tier::Soak => &self.soak,
        }
    }

    /// The criteria this tier is judged by: the committed ones, with the tier's
    /// own overrides applied. A drift gate belongs to the tier long enough to
    /// fit a slope through, and to no other.
    pub fn slo(&self, tier: Tier) -> Slo {
        let mut slo = self.slo;
        if let Some(overrides) = self.scale(tier).slo_overrides {
            if let Some(drift) = overrides.max_rss_drift_kib_per_hour {
                slo.max_rss_drift_kib_per_hour = Some(drift);
            }
            if let Some(segments) = overrides.min_segments {
                slo.min_segments = segments;
            }
        }
        slo
    }
}

/// How one tier is offered.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct Scale {
    pub duration_ms: u64,
    pub concurrency: usize,
    pub think_time_ms: u64,
    pub sample_interval_ms: u64,
    pub segment_ms: u64,
    #[serde(default)]
    pub slo_overrides: Option<SloOverrides>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct SloOverrides {
    #[serde(default)]
    pub max_rss_drift_kib_per_hour: Option<f64>,
    #[serde(default)]
    pub min_segments: Option<u64>,
}

/// When the run does something to the deployment it is measuring. Fractions of
/// the offered duration rather than offsets, so both tiers run the same script.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct Schedule {
    /// Maximum permitted delay between a committed fault-gate offset and the
    /// independent gate scheduler actually applying that transition.
    pub event_dispatch_slack_ms: u64,
    pub catalogue_revision_at: f64,
    pub credential_revision_at: f64,
    pub policy_revision_at: f64,
    pub upstream_latency_at: f64,
    pub upstream_latency_for: f64,
    pub upstream_latency_ms: u64,
    pub upstream_outage_at: f64,
    pub upstream_outage_for: f64,
    /// Leading-edge allowance for the gateway to observe a caller close. A
    /// client can finish just before the gate cuts the upstream while the
    /// gateway settles that same request just after it. This is deliberately
    /// separate from the much larger recovery allowance.
    pub upstream_outage_correlation_slack_ms: u64,
    pub usage_backend_outage_at: f64,
    pub usage_backend_outage_for: f64,
    /// How far either side of the usage-backend outage a record still counts as
    /// the outage's. The driver's clock and the record's `recorded_at` are set
    /// moments apart, and a boundary drawn to the millisecond would blame the
    /// deployment for the harness' own scheduling.
    pub usage_outage_attribution_slack_ms: u64,
    /// How long after a declared fault is lifted the deployment may still be
    /// refusing traffic before its refusals count as findings.
    ///
    /// A backend that goes away trips the circuit breakers in front of it, and
    /// they are supposed to stay tripped for their cooldown: shedding while a
    /// known-bad upstream cools down is the breaker working. What is *not*
    /// allowed is never closing again, so the allowance is a bound the run is
    /// judged against rather than a blanket excuse — the artifact records how
    /// long recovery actually took for each window.
    pub recovery_allowance_ms: u64,
    pub rolling_restart_at: f64,
}

/// One thing the driver does to the deployment, at a known offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    CatalogueRevision,
    CredentialRevision,
    PolicyRevision,
    UpstreamLatencyBegins,
    UpstreamLatencyEnds,
    UpstreamOutageBegins,
    UpstreamOutageEnds,
    UsageBackendOutageBegins,
    UsageBackendOutageEnds,
    RollingRestart,
}

impl Event {
    /// Whether this event changes one of the two fault gates. Gate transitions
    /// run on their own timer so convergence and probe work in the supervisor
    /// cannot broaden a committed fault window.
    pub fn is_gate_transition(self) -> bool {
        matches!(
            self,
            Self::UpstreamLatencyBegins
                | Self::UpstreamLatencyEnds
                | Self::UpstreamOutageBegins
                | Self::UpstreamOutageEnds
                | Self::UsageBackendOutageBegins
                | Self::UsageBackendOutageEnds
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::CatalogueRevision => "catalogue-revision",
            Self::CredentialRevision => "credential-revision",
            Self::PolicyRevision => "policy-revision",
            Self::UpstreamLatencyBegins => "upstream-latency-begins",
            Self::UpstreamLatencyEnds => "upstream-latency-ends",
            Self::UpstreamOutageBegins => "upstream-outage-begins",
            Self::UpstreamOutageEnds => "upstream-outage-ends",
            Self::UsageBackendOutageBegins => "usage-backend-outage-begins",
            Self::UsageBackendOutageEnds => "usage-backend-outage-ends",
            Self::RollingRestart => "rolling-restart",
        }
    }
}

/// A scheduled event, resolved against a concrete duration.
#[derive(Debug, Clone, Copy)]
pub struct Scheduled {
    pub event: Event,
    pub at: Duration,
}

impl Schedule {
    /// The deterministic opening edge and nominal closing edge used to seed
    /// stateful cancellation correlation before the provider gate is cut.
    /// The supervisor replaces the closing edge with the observed restoration
    /// timestamp while the run is live.
    pub fn upstream_correlation_window_ms(&self, duration: Duration) -> (u64, u64) {
        let duration_ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        let at = |fraction: f64| ((duration_ms as f64) * fraction.clamp(0.0, 1.0)).floor() as u64;
        (
            at(self.upstream_outage_at).saturating_sub(self.upstream_outage_correlation_slack_ms),
            at(self.upstream_outage_at + self.upstream_outage_for),
        )
    }

    /// The script, in the order it happens. Resolving fractions against the
    /// duration here — rather than in the driver — is what makes the smoke tier
    /// a shorter run of the same qualification rather than a different one.
    pub fn resolve(&self, duration: Duration) -> Vec<Scheduled> {
        let at = |fraction: f64| duration.mul_f64(fraction.clamp(0.0, 1.0));
        let mut script = vec![
            Scheduled {
                event: Event::CatalogueRevision,
                at: at(self.catalogue_revision_at),
            },
            Scheduled {
                event: Event::CredentialRevision,
                at: at(self.credential_revision_at),
            },
            Scheduled {
                event: Event::PolicyRevision,
                at: at(self.policy_revision_at),
            },
            Scheduled {
                event: Event::UpstreamLatencyBegins,
                at: at(self.upstream_latency_at),
            },
            Scheduled {
                event: Event::UpstreamLatencyEnds,
                at: at(self.upstream_latency_at + self.upstream_latency_for),
            },
            Scheduled {
                event: Event::UpstreamOutageBegins,
                at: at(self.upstream_outage_at),
            },
            Scheduled {
                event: Event::UpstreamOutageEnds,
                at: at(self.upstream_outage_at + self.upstream_outage_for),
            },
            Scheduled {
                event: Event::UsageBackendOutageBegins,
                at: at(self.usage_backend_outage_at),
            },
            Scheduled {
                event: Event::UsageBackendOutageEnds,
                at: at(self.usage_backend_outage_at + self.usage_backend_outage_for),
            },
            Scheduled {
                event: Event::RollingRestart,
                at: at(self.rolling_restart_at),
            },
        ];
        script.sort_by_key(|scheduled| scheduled.at);
        script
    }

    /// The usage-backend outage, widened by the attribution slack.
    pub fn usage_outage_window(&self, duration: Duration) -> (Duration, Duration) {
        let at = |fraction: f64| duration.mul_f64(fraction.clamp(0.0, 1.0));
        let slack = Duration::from_millis(self.usage_outage_attribution_slack_ms);
        (
            at(self.usage_backend_outage_at).saturating_sub(slack),
            at(self.usage_backend_outage_at + self.usage_backend_outage_for) + slack,
        )
    }

    /// The windows during which an error is the point rather than a finding.
    pub fn fault_windows(&self, duration: Duration) -> Vec<(Duration, Duration)> {
        self.fault_windows_of(duration, Injected::EveryDeclaredFault)
    }

    /// The same, for a run that applies only some of the declared faults: the
    /// window of a fault nobody injected excuses nothing, because nothing in it
    /// was caused by this harness.
    pub fn fault_windows_of(
        &self,
        duration: Duration,
        injected: Injected,
    ) -> Vec<(Duration, Duration)> {
        let at = |fraction: f64| duration.mul_f64(fraction.clamp(0.0, 1.0));
        let mut windows = vec![(
            at(self.upstream_outage_at),
            at(self.upstream_outage_at + self.upstream_outage_for),
        )];
        if injected == Injected::EveryDeclaredFault {
            windows.push((
                at(self.usage_backend_outage_at),
                at(self.usage_backend_outage_at + self.usage_backend_outage_for),
            ));
        }
        windows
    }

    /// The same windows, each extended by the recovery allowance: the span in
    /// which a refusal is attributed to the declared fault rather than counted
    /// against the deployment.
    pub fn attribution_windows(&self, duration: Duration) -> Vec<(Duration, Duration)> {
        self.attribution_windows_of(duration, Injected::EveryDeclaredFault)
    }

    /// The attribution windows of the faults the run actually applies.
    pub fn attribution_windows_of(
        &self,
        duration: Duration,
        injected: Injected,
    ) -> Vec<(Duration, Duration)> {
        let allowance = Duration::from_millis(self.recovery_allowance_ms);
        self.fault_windows_of(duration, injected)
            .into_iter()
            .map(|(from, to)| (from, to + allowance))
            .collect()
    }
}

/// Which of the script's declared faults a run is able to inject. The
/// usage-backend outage needs the database to be behind this harness's own
/// fault gate; a database reached directly is never taken away, so the stretch
/// of the run the script set aside for that outage must go on being measured
/// like any other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Injected {
    EveryDeclaredFault,
    UpstreamFaultsOnly,
}

/// What a passing run means.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct Slo {
    pub replicas: usize,
    pub max_convergence_ms: u64,
    pub max_durable_usage_lag_ms: u64,
    pub max_missing_usage_records: u64,
    pub max_duplicate_usage_records: u64,
    pub max_durable_usage_loss_outside_windows: u64,
    pub max_tenant_boundary_violations: u64,
    pub max_unplanned_errors: u64,
    pub max_restart_unavailable: u64,
    pub max_readiness_gap_ms: u64,
    /// How long after a declared fault is lifted the deployment may take to
    /// serve again. Recovering is not optional; taking a bounded while over it
    /// is.
    pub max_recovery_ms: u64,
    pub max_rss_growth_kib: u64,
    #[serde(default)]
    pub max_rss_drift_kib_per_hour: Option<f64>,
    pub min_segments: u64,
}

/// When a run stops, and what stops it early.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct Termination {
    pub settle_ms: u64,
    pub abort_on_tenant_boundary_violation: bool,
    pub abort_on_replica_exit: bool,
    pub abort_after_consecutive_unplanned_errors: u64,
    pub abort_after_unready_ms: u64,
}

/// Why a run stopped. On the artifact, because "ran to the end and passed" and
/// "was abandoned and had passed so far" are not the same evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Stop {
    /// The offered duration elapsed.
    DurationElapsed,
    /// A caller reached across a tenant boundary.
    TenantBoundaryViolation,
    /// A replica exited without being asked to.
    ReplicaExited,
    /// Too many consecutive errors outside every declared fault window.
    UnplannedErrors,
    /// A restarted replica never came back ready.
    ReplicaNeverReady,
    /// The harness could not apply a committed fault-gate edge in bound. The
    /// transition is still applied and the run is finalized so its diagnostic
    /// artifact survives; it is not promotable qualification evidence.
    EventDispatchLate,
}

impl Stop {
    /// Whether this is the ending the manifest calls normal.
    pub fn is_normal(self) -> bool {
        matches!(self, Self::DurationElapsed)
    }
}

/// Which tier to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Smoke,
    Soak,
}

impl Tier {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Soak => "soak",
        }
    }
}

pub fn manifest_path() -> PathBuf {
    workspace_root().join(MANIFEST_RELATIVE)
}

/// Load the manifest, refusing a schema this harness does not understand.
pub fn load() -> (Manifest, String) {
    let path = manifest_path();
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{} is unreadable: {e}", path.display()));
    let manifest: Manifest = Figment::from(Toml::file(&path))
        .extract()
        .unwrap_or_else(|e| {
            panic!(
                "{} is not a valid stateful endurance manifest: {e}",
                path.display()
            )
        });
    assert_eq!(
        manifest.schema_version, MANIFEST_SCHEMA_VERSION,
        "unsupported stateful endurance manifest schema"
    );
    assert!(
        !manifest.profiles.is_empty(),
        "the stateful endurance manifest declares no profiles"
    );
    (manifest, text)
}
