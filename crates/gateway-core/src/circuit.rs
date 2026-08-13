use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitDecision {
    Allow { probe: bool },
    Skip,
}

#[derive(Debug, Clone, Copy)]
struct Circuit {
    state: CircuitState,
    failures: u32,
    phase_started: Option<Instant>,
}

impl Default for Circuit {
    fn default() -> Self {
        Self {
            state: CircuitState::Closed,
            failures: 0,
            phase_started: None,
        }
    }
}

pub struct CircuitBreaker {
    threshold: u32,
    cooldown: Duration,
    circuits: Mutex<HashMap<String, Circuit>>,
}

impl CircuitBreaker {
    pub fn new(threshold: u32, cooldown: Duration) -> Self {
        Self {
            threshold: threshold.max(1),
            cooldown,
            circuits: Mutex::new(HashMap::new()),
        }
    }

    pub fn allow(&self, provider: &str) -> CircuitDecision {
        self.allow_at(provider, Instant::now())
    }

    pub fn record_success(&self, provider: &str) {
        let mut circuits = self.lock();
        let circuit = circuits.entry(provider.to_owned()).or_default();
        circuit.state = CircuitState::Closed;
        circuit.failures = 0;
        circuit.phase_started = None;
    }

    pub fn record_failure(&self, provider: &str) {
        self.record_failure_at(provider, Instant::now());
    }

    pub fn state(&self, provider: &str) -> CircuitState {
        self.lock()
            .get(provider)
            .map_or(CircuitState::Closed, |circuit| circuit.state)
    }

    pub fn snapshot(&self) -> Vec<(String, CircuitState)> {
        self.lock()
            .iter()
            .map(|(provider, circuit)| (provider.clone(), circuit.state))
            .collect()
    }

    /// The phase the next request would find, without being that request.
    ///
    /// [`Self::snapshot`] reports the phase stored, which is the phase the last
    /// request left behind: a target whose cooldown has elapsed still reads
    /// `Open` until something calls [`Self::allow`] and moves it. That is the
    /// right answer for bookkeeping and the wrong one for a reader describing
    /// what this process would do now — during recovery it reports a target as
    /// refused that the next request would in fact probe.
    ///
    /// So the cooldown is applied to the *answer* and not to the breaker: an open
    /// or half-open circuit past its cooldown reports [`CircuitState::HalfOpen`],
    /// the phase in which a probe is allowed, and nothing is written. A reader
    /// cannot spend a target's probe budget by looking at it.
    pub fn observed(&self) -> Vec<(String, CircuitState)> {
        self.observed_at(Instant::now())
    }

    fn observed_at(&self, now: Instant) -> Vec<(String, CircuitState)> {
        self.lock()
            .iter()
            .map(|(provider, circuit)| {
                let phase = match circuit.state {
                    CircuitState::Open | CircuitState::HalfOpen
                        if elapsed(circuit.phase_started, now) >= self.cooldown =>
                    {
                        CircuitState::HalfOpen
                    }
                    held => held,
                };
                (provider.clone(), phase)
            })
            .collect()
    }

    fn allow_at(&self, provider: &str, now: Instant) -> CircuitDecision {
        let mut circuits = self.lock();
        let circuit = circuits.entry(provider.to_owned()).or_default();
        match circuit.state {
            CircuitState::Closed => CircuitDecision::Allow { probe: false },
            CircuitState::Open if elapsed(circuit.phase_started, now) >= self.cooldown => {
                circuit.state = CircuitState::HalfOpen;
                circuit.phase_started = Some(now);
                CircuitDecision::Allow { probe: true }
            }
            CircuitState::HalfOpen if elapsed(circuit.phase_started, now) >= self.cooldown => {
                circuit.phase_started = Some(now);
                CircuitDecision::Allow { probe: true }
            }
            CircuitState::Open | CircuitState::HalfOpen => CircuitDecision::Skip,
        }
    }

    fn record_failure_at(&self, provider: &str, now: Instant) {
        let mut circuits = self.lock();
        let circuit = circuits.entry(provider.to_owned()).or_default();
        match circuit.state {
            CircuitState::Closed => {
                circuit.failures = circuit.failures.saturating_add(1);
                if circuit.failures >= self.threshold {
                    circuit.state = CircuitState::Open;
                    circuit.phase_started = Some(now);
                }
            }
            CircuitState::HalfOpen => {
                circuit.state = CircuitState::Open;
                circuit.failures = self.threshold;
                circuit.phase_started = Some(now);
            }
            CircuitState::Open => {}
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Circuit>> {
        self.circuits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn elapsed(started: Option<Instant>, now: Instant) -> Duration {
    started.map_or(Duration::MAX, |started| {
        now.saturating_duration_since(started)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opens_skips_probes_and_recovers() {
        let breaker = CircuitBreaker::new(2, Duration::from_secs(10));
        let now = Instant::now();
        breaker.record_failure_at("openai", now);
        breaker.record_failure_at("openai", now);
        assert_eq!(breaker.allow_at("openai", now), CircuitDecision::Skip);
        assert_eq!(
            breaker.allow_at("openai", now + Duration::from_secs(10)),
            CircuitDecision::Allow { probe: true }
        );
        breaker.record_success("openai");
        assert_eq!(breaker.state("openai"), CircuitState::Closed);
    }

    #[test]
    fn an_elapsed_cooldown_is_visible_before_a_request_spends_it() {
        let breaker = CircuitBreaker::new(2, Duration::from_secs(10));
        let now = Instant::now();
        breaker.record_failure_at("openai", now);
        breaker.record_failure_at("openai", now);

        assert_eq!(
            breaker.observed_at(now),
            vec![("openai".to_owned(), CircuitState::Open)],
            "a target inside its cooldown is one this replica is refusing"
        );

        let recovered = now + Duration::from_secs(10);
        assert_eq!(
            breaker.observed_at(recovered),
            vec![("openai".to_owned(), CircuitState::HalfOpen)],
            "a target whose cooldown elapsed is one the next request would probe"
        );
        assert_eq!(
            breaker.snapshot(),
            vec![("openai".to_owned(), CircuitState::Open)],
            "and looking at it moved nothing"
        );
        assert_eq!(
            breaker.allow_at("openai", recovered),
            CircuitDecision::Allow { probe: true },
            "so the probe is still the next request's to spend"
        );
    }
}
