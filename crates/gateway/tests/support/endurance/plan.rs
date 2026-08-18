//! What the endurance driver offers, and to whom.
//!
//! The plan is a pure function of the manifest: given the seed, the mix, and a
//! request index, the tenant, the route, the alias, and the ending are decided
//! without consulting the clock or the host. Two runs of the same profile
//! therefore offer the same traffic in the same order, and only the timings
//! differ — which is what makes two artifacts comparable at all.

use std::path::Path;

use super::manifest::{Ending, Mix};
use crate::support::gateway::{ANTHROPIC_SECONDARY_ENV, GATEWAY_KEY, OPENAI_SECONDARY_ENV, alias};

pub const CHAT: &str = "/v1/chat/completions";
pub const MESSAGES: &str = "/v1/messages";
pub const EMBEDDINGS: &str = "/v1/embeddings";
pub const RESPONSES: &str = "/v1/responses";

/// The placeholder a tenant key file's directory is replaced by before the
/// booted config is hashed: the directory is per process, and a config hash
/// that changed every run would make every artifact incomparable.
pub const KEY_DIR_PLACEHOLDER: &str = "/TENANT_KEY_DIR";

/// One caller. Three tenants, deliberately not alike: the operator namespace
/// with a platform credential pool, a namespace that brings its own keys, and a
/// namespace with no credentials at all that falls back to the platform pool.
/// Tenancy is the axis that decides which pool a request draws from, so a soak
/// that runs one namespace has not soaked the credential path.
#[derive(Debug, Clone)]
pub struct Tenant {
    pub namespace: &'static str,
    /// The bearer the driver authenticates with.
    pub key: String,
    /// What the usage record's `credential_source` is expected to say.
    pub credential_source: &'static str,
}

pub const PLATFORM: &str = "platform";
pub const BYOK: &str = "endurance-byok";
pub const FALLBACK: &str = "endurance-fallback";

/// How many tenants the plan rotates over, so a result can be checked for
/// having offered all of them rather than for having offered *some*.
pub const TENANTS: usize = 3;

/// Inbound keys for the two extra namespaces. They are fixtures, not secrets —
/// the only thing they can reach is a fake upstream on loopback — and they are
/// delivered as files because the boot's environment is fixed by the shared
/// harness while `[[gateway_key]] file` is config the profile owns.
const BYOK_KEY: &str = "endurance-byok-inbound-key";
const FALLBACK_KEY: &str = "endurance-fallback-inbound-key";

/// Write the tenant key files under `dir` and return the tenants together with
/// the config that declares them. The returned TOML is appended to the harness
/// config, so the namespaces, their credential pools, and their inbound keys
/// are all recorded in the artifact's config hash.
pub fn tenants(dir: &Path) -> (Vec<Tenant>, String) {
    let byok_key_path = dir.join("byok.key");
    let fallback_key_path = dir.join("fallback.key");
    // No trailing newline: a static key is exact bytes, and a newline makes the
    // file unusable as a bearer token.
    std::fs::write(&byok_key_path, BYOK_KEY).expect("the byok tenant key file is written");
    std::fs::write(&fallback_key_path, FALLBACK_KEY)
        .expect("the fallback tenant key file is written");

    let tenants = vec![
        Tenant {
            namespace: PLATFORM,
            key: GATEWAY_KEY.to_owned(),
            credential_source: "platform",
        },
        Tenant {
            namespace: BYOK,
            key: BYOK_KEY.to_owned(),
            credential_source: "byok",
        },
        Tenant {
            namespace: FALLBACK,
            key: FALLBACK_KEY.to_owned(),
            credential_source: "platform",
        },
    ];

    let config = format!(
        r#"
[[namespace]]
id = "{BYOK}"

[[namespace]]
id = "{FALLBACK}"
allow_platform_fallback = true

# A second platform credential per provider, so the operator namespace rotates a
# pool rather than pinning one key for the whole run.
[[credential]]
namespace = "{PLATFORM}"
provider = "fake-openai"
env = "{OPENAI_SECONDARY_ENV}"
id = "fake-openai-secondary"

[[credential]]
namespace = "{PLATFORM}"
provider = "fake-anthropic"
env = "{ANTHROPIC_SECONDARY_ENV}"
id = "fake-anthropic-secondary"

# The BYOK tenant's own keys: same material, a different pool and a different
# `credential_source` on every record it settles.
[[credential]]
namespace = "{BYOK}"
provider = "fake-openai"
env = "{OPENAI_SECONDARY_ENV}"
id = "endurance-byok-openai"

[[credential]]
namespace = "{BYOK}"
provider = "fake-anthropic"
env = "{ANTHROPIC_SECONDARY_ENV}"
id = "endurance-byok-anthropic"

[[gateway_key]]
file = "{byok}"
namespace = "{BYOK}"

[[gateway_key]]
file = "{fallback}"
namespace = "{FALLBACK}"
"#,
        byok = byok_key_path.display(),
        fallback = fallback_key_path.display(),
    );
    (tenants, config)
}

/// One request the driver sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shape {
    pub route: &'static str,
    pub alias: &'static str,
    pub provider: &'static str,
    pub stream: bool,
}

const OPENAI: &str = "fake-openai";
const ANTHROPIC: &str = "fake-anthropic";

const fn buffered(route: &'static str, alias: &'static str, provider: &'static str) -> Shape {
    Shape {
        route,
        alias,
        provider,
        stream: false,
    }
}

const fn streamed(route: &'static str, alias: &'static str, provider: &'static str) -> Shape {
    Shape {
        route,
        alias,
        provider,
        stream: true,
    }
}

/// Successful traffic: both wire families, four routes, both providers,
/// buffered and streamed interleaved.
const COMPLETE: [Shape; 6] = [
    buffered(CHAT, alias::CHAT, OPENAI),
    buffered(MESSAGES, alias::MESSAGES, ANTHROPIC),
    buffered(EMBEDDINGS, alias::EMBEDDINGS, OPENAI),
    buffered(RESPONSES, alias::RESPONSES, OPENAI),
    streamed(CHAT, alias::CHAT_SLOW, OPENAI),
    streamed(MESSAGES, alias::MESSAGES_SLOW, ANTHROPIC),
];

/// Callers who hang up mid-answer, over both wires.
const CANCELLED: [Shape; 2] = [
    streamed(CHAT, alias::CHAT_SLOW, OPENAI),
    streamed(MESSAGES, alias::MESSAGES_SLOW, ANTHROPIC),
];

/// Upstreams that die mid-stream, once relay has committed.
const DROPPED: [Shape; 2] = [
    streamed(CHAT, alias::CHAT_DROP, OPENAI),
    streamed(MESSAGES, alias::MESSAGES_DROP, ANTHROPIC),
];

/// Upstreams that refuse before a byte is relayed, asked for both buffered and
/// streamed: the stream case is the one that has a relay to *not* start.
const FAULTED: [Shape; 2] = [
    buffered(CHAT, alias::CHAT_FAIL, OPENAI),
    streamed(CHAT, alias::CHAT_FAIL, OPENAI),
];

pub fn shapes(ending: Ending) -> &'static [Shape] {
    match ending {
        Ending::Complete => &COMPLETE,
        Ending::Cancelled => &CANCELLED,
        Ending::Dropped => &DROPPED,
        Ending::Faulted => &FAULTED,
    }
}

/// The whole plan for one request index.
#[derive(Debug, Clone)]
pub struct Planned {
    pub tenant: Tenant,
    pub shape: Shape,
    pub ending: Ending,
    /// Caller-controlled W3C trace identity used only to prove that this exact
    /// planned request produced this exact usage record. The gateway continues
    /// to mint the billing `request_id` itself.
    pub correlation: CorrelationId,
}

/// One run-scoped, deterministic request correlation identity.
///
/// The first word domains the manifest seed and the second is the one-based
/// request index. The mapping is injective and cannot produce W3C's forbidden
/// all-zero trace id. It is a qualification-driver identity, never a production
/// trace-id generator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CorrelationId([u8; 16]);

impl CorrelationId {
    const DOMAIN: u64 = 0x6178_6f6e_642d_656e;

    pub fn new(seed: u64, index: usize) -> Self {
        let sequence = u64::try_from(index)
            .ok()
            .and_then(|index| index.checked_add(1))
            .expect("an endurance request index fits a nonzero u64");
        let mut bytes = [0_u8; 16];
        bytes[..8].copy_from_slice(&(seed ^ Self::DOMAIN).to_be_bytes());
        bytes[8..].copy_from_slice(&sequence.to_be_bytes());
        Self(bytes)
    }

    pub const fn bytes(self) -> [u8; 16] {
        self.0
    }

    pub fn trace_id(self) -> String {
        let high = u64::from_be_bytes(self.0[..8].try_into().expect("eight-byte high word"));
        let low = u64::from_be_bytes(self.0[8..].try_into().expect("eight-byte low word"));
        format!("{high:016x}{low:016x}")
    }

    pub fn traceparent(self) -> String {
        format!("00-{}-0000000000000001-01", self.trace_id())
    }
}

/// The ending rotation: one full mix cycle, permuted once with the manifest's
/// seed and then repeated. Permuting a whole cycle rather than drawing
/// independently is what keeps the offered proportions exact — over any whole
/// number of cycles a run offers precisely the committed mix, and a run cut off
/// mid-cycle is off by at most one cycle rather than by a sampling error nobody
/// can reproduce.
pub fn rotation(mix: &Mix, seed: u64) -> Vec<Ending> {
    let mut cycle: Vec<Ending> = Ending::ALL
        .iter()
        .flat_map(|&ending| std::iter::repeat_n(ending, mix.weight(ending)))
        .collect();
    let mut rng = SplitMix64::new(seed);
    // Fisher-Yates, so the mix is spread through the cycle instead of arriving
    // in four blocks: a soak that runs every fault back to back has not
    // interleaved anything.
    for i in (1..cycle.len()).rev() {
        cycle.swap(i, (rng.next() % (i as u64 + 1)) as usize);
    }
    cycle
}

/// What request `index` is.
pub fn planned(index: usize, seed: u64, tenants: &[Tenant], rotation: &[Ending]) -> Planned {
    let ending = rotation[index % rotation.len()];
    let shapes = shapes(ending);
    Planned {
        // Tenant and shape advance on different cycles from the ending, so the
        // combinations are covered rather than the same tenant always getting
        // the same failure.
        tenant: tenants[index % tenants.len()].clone(),
        shape: shapes[index % shapes.len()],
        ending,
        correlation: CorrelationId::new(seed, index),
    }
}

/// A small, fixed PRNG. Seeding the shuffle from the manifest rather than from
/// the host is the whole point: the permutation is part of the committed input.
struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correlations_are_deterministic_injective_and_valid_w3c_ids() {
        let first = CorrelationId::new(7, 0);
        let again = CorrelationId::new(7, 0);
        let next = CorrelationId::new(7, 1);
        let another_seed = CorrelationId::new(8, 0);
        assert_eq!(first, again);
        assert_ne!(first, next);
        assert_ne!(first, another_seed);
        assert_ne!(first.bytes(), [0; 16]);
        assert_eq!(first.trace_id().len(), 32);
        assert_eq!(first.traceparent().len(), 55);
        assert!(first.traceparent().starts_with("00-"));
        assert!(first.traceparent().ends_with("-0000000000000001-01"));
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    #[should_panic(expected = "fits a nonzero u64")]
    fn correlation_refuses_index_wraparound() {
        let _ = CorrelationId::new(0, usize::MAX);
    }
}
