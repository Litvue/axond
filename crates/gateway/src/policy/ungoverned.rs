//! Throttling for the "no document governs this namespace" report.
//!
//! A namespace no published document governs is denied on every request that
//! touches it (`docs/adr/0041-runtime-policy-activation.md`), and a denial an
//! operator cannot see is a denial they cannot fix — but the condition is a
//! property of the *view*, not of the request, so logging it per request scales
//! the log volume with traffic and buries the line that explains it. Every
//! denial is still counted; only the explanation is sampled.
//!
//! One report per condition, backend and namespace, then at most one more every
//! [`REPORT_EVERY`], for as long as the condition lasts.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

/// How often a namespace that stays ungoverned is re-reported.
const REPORT_EVERY: Duration = Duration::from_secs(60);

/// How many (condition, backend, namespace) triples are remembered before the record is
/// dropped and rebuilt. A namespace comes from a projection, so the set is
/// bounded by the fleet's configuration rather than by traffic; the cap is there
/// so a pathological one cannot grow this map without bound.
const REMEMBERED: usize = 1024;

static REPORTED: LazyLock<Mutex<Reported>> = LazyLock::new(|| Mutex::new(Reported::default()));

/// Whether this denial should be explained in the log, or is one of the
/// repetitions the earlier report already covers.
pub(crate) fn should_report(
    condition: Unenforceable,
    backend: &'static str,
    namespace: &str,
) -> bool {
    let mut reported = match REPORTED.lock() {
        Ok(reported) => reported,
        // A poisoned record is a record, not a reason to go quiet or to panic on
        // the request path: report and carry on with what it holds.
        Err(poisoned) => poisoned.into_inner(),
    };
    reported.should_report(condition, backend, namespace, Instant::now())
}

/// Why a store cannot enforce a namespace. Reported separately, because they
/// call for different operator action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Unenforceable {
    /// No published document governs the namespace.
    Ungoverned,
    /// The published cap and the key layout this process booted on disagree.
    Layout,
}

#[derive(Debug, Default)]
struct Reported(HashMap<(Unenforceable, &'static str, String), Instant>);

impl Reported {
    fn should_report(
        &mut self,
        condition: Unenforceable,
        backend: &'static str,
        namespace: &str,
        now: Instant,
    ) -> bool {
        if let Some(last) = self.0.get_mut(&(condition, backend, namespace.to_owned())) {
            if now.duration_since(*last) < REPORT_EVERY {
                return false;
            }
            *last = now;
            return true;
        }
        if self.0.len() >= REMEMBERED {
            self.0.clear();
        }
        self.0
            .insert((condition, backend, namespace.to_owned()), now);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_namespace_that_stays_ungoverned_is_explained_once_per_interval() {
        let mut reported = Reported::default();
        let start = Instant::now();

        assert!(reported.should_report(Unenforceable::Ungoverned, "redis", "alpha", start));
        // The traffic behind the first report does not repeat it.
        assert!(!reported.should_report(Unenforceable::Ungoverned, "redis", "alpha", start));
        assert!(!reported.should_report(
            Unenforceable::Ungoverned,
            "redis",
            "alpha",
            start + REPORT_EVERY - Duration::from_millis(1)
        ));
        // A condition that outlives the interval says so again.
        assert!(reported.should_report(
            Unenforceable::Ungoverned,
            "redis",
            "alpha",
            start + REPORT_EVERY
        ));
    }

    #[test]
    fn each_condition_backend_and_namespace_is_explained_on_its_own() {
        let mut reported = Reported::default();
        let start = Instant::now();

        assert!(reported.should_report(Unenforceable::Ungoverned, "redis", "alpha", start));
        assert!(reported.should_report(Unenforceable::Ungoverned, "redis", "beta", start));
        // Two stores denying the same namespace are two different operator
        // problems: the budget cap and the concurrency ceiling are published
        // separately.
        assert!(reported.should_report(Unenforceable::Ungoverned, "postgres", "alpha", start));
        // As are two different reasons one store cannot enforce it.
        assert!(reported.should_report(Unenforceable::Layout, "redis", "alpha", start));
    }

    #[test]
    fn the_record_cannot_grow_without_bound() {
        let mut reported = Reported::default();
        let start = Instant::now();

        for namespace in 0..REMEMBERED {
            assert!(reported.should_report(
                Unenforceable::Ungoverned,
                "redis",
                &namespace.to_string(),
                start
            ));
        }
        assert_eq!(reported.0.len(), REMEMBERED);

        assert!(reported.should_report(Unenforceable::Ungoverned, "redis", "one-too-many", start));
        assert_eq!(reported.0.len(), 1);
    }
}
