//! The leak detector the redaction tests share.
//!
//! Every assertion in this module family has the same shape — "this surface
//! does not contain that material" — and the only interesting question is what
//! *contain* means. A test that searched for the raw bytes alone would pass on a
//! response that base64-encoded a key, on a journal row that stored it as
//! `bytea` (rendered `\x…` by Postgres), and on a log line that emitted the
//! first half of it. So a [`LeakSweep`] searches for a sentinel in every form it
//! could plausibly survive a trip through the system:
//!
//! * the raw value, and its upper- and lower-case spellings, because a header
//!   value that round-trips through a case-normalising layer is still the key;
//! * base64, padded and unpadded, standard and URL-safe, because that is what an
//!   envelope, a JWT payload, or a `bytea`-to-JSON conversion produces;
//! * hex, both cases, because that is what Postgres renders binary columns as
//!   and what most hash-and-dump helpers emit;
//! * every 12-character window of the value, because a *partial* disclosure is
//!   still a disclosure: half a live key plus the provider's key format is a
//!   materially reduced search space, and truncation is exactly what a
//!   well-meaning "log the first N characters" helper does.
//!
//! The sweep is deliberately paranoid in the other direction too:
//! [`LeakSweep::assert_present`] exists so every test can prove its own detector
//! works against the one surface that *is* allowed to hold the material. A
//! redaction test whose sentinel never entered the system in the first place is
//! a test that passes for the wrong reason, and that failure mode is invisible
//! without a tripwire.

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE_NO_PAD};

/// How short a fragment of a sentinel still counts as a leak.
///
/// Twelve characters is well below any prefix a "safe" truncation helper would
/// keep and well above the length at which random text collides with a
/// high-entropy sentinel.
const FRAGMENT: usize = 12;

/// One secret value, plus every encoding of it a leak could arrive in.
struct Sentinel {
    /// What this material *is*, for the failure message: an assertion that fires
    /// has to say which secret escaped, and it cannot say so by printing it.
    label: &'static str,
    needles: Vec<(&'static str, String)>,
}

impl Sentinel {
    fn new(label: &'static str, value: &str) -> Self {
        let bytes = value.as_bytes();
        let mut needles = vec![
            ("raw", value.to_owned()),
            ("lowercase", value.to_lowercase()),
            ("uppercase", value.to_uppercase()),
            ("base64", STANDARD.encode(bytes)),
            ("base64-unpadded", STANDARD_NO_PAD.encode(bytes)),
            ("base64url", URL_SAFE_NO_PAD.encode(bytes)),
            ("hex", hex(bytes, false)),
            ("hex-uppercase", hex(bytes, true)),
        ];
        for window in value.as_bytes().windows(FRAGMENT.min(value.len())) {
            // Labelled rather than indexed: a failure says a fragment escaped,
            // never which one, so the message cannot reconstruct the value.
            let fragment = String::from_utf8(window.to_vec()).expect("sentinels are ASCII");
            needles.push(("fragment", fragment));
        }
        needles.sort();
        needles.dedup_by(|left, right| left.1 == right.1);
        Self { label, needles }
    }

    /// The first encoding of this sentinel present in `haystack`, if any.
    fn found_in(&self, haystack: &str) -> Option<&'static str> {
        self.whole_in(haystack).or_else(|| {
            self.needles
                .iter()
                .find(|(_, needle)| haystack.contains(needle.as_str()))
                .map(|(encoding, _)| *encoding)
        })
    }

    /// As [`Self::found_in`], but only whole-value encodings count.
    ///
    /// Fragments are what makes the *absence* check strict and the *identity*
    /// check useless: sentinels that share a readable prefix share fragments, so
    /// a twelve-character window proves some sentinel was there, never which.
    fn whole_in(&self, haystack: &str) -> Option<&'static str> {
        self.needles
            .iter()
            .filter(|(encoding, _)| *encoding != "fragment")
            .find(|(_, needle)| haystack.contains(needle.as_str()))
            .map(|(encoding, _)| *encoding)
    }
}

fn hex(bytes: &[u8], upper: bool) -> String {
    bytes
        .iter()
        .map(|byte| {
            if upper {
                format!("{byte:02X}")
            } else {
                format!("{byte:02x}")
            }
        })
        .collect()
}

/// A set of sentinels, swept over whatever a surface renders.
pub(crate) struct LeakSweep {
    sentinels: Vec<Sentinel>,
}

impl LeakSweep {
    /// A sweep over `(label, material)` pairs.
    pub(crate) fn of<'a>(materials: impl IntoIterator<Item = (&'static str, &'a str)>) -> Self {
        Self {
            sentinels: materials
                .into_iter()
                .map(|(label, value)| Sentinel::new(label, value))
                .collect(),
        }
    }

    /// Assert that no sentinel appears in `rendered`, in any encoding.
    ///
    /// `surface` names what was swept — "the usage record", "the journal's
    /// `resource_version` rows" — because that name is the whole content of the
    /// failure report: the material itself must not be printed by the assertion
    /// that catches it, or CI logs become the leak.
    pub(crate) fn assert_absent(&self, surface: &str, rendered: &str) {
        // Whole values first, so a leak of one sentinel is not reported under
        // the label of another that happens to share a fragment with it.
        let found = self
            .sentinels
            .iter()
            .find_map(|sentinel| Some((sentinel.label, sentinel.whole_in(rendered)?)))
            .or_else(|| {
                self.sentinels
                    .iter()
                    .find_map(|sentinel| Some((sentinel.label, sentinel.found_in(rendered)?)))
            });
        if let Some((label, encoding)) = found {
            panic!(
                "{surface} discloses the `{label}` sentinel ({encoding} encoding); \
                 {} bytes of surface were swept and the material is deliberately not printed",
                rendered.len()
            );
        }
    }

    /// [`assert_absent`](Self::assert_absent) over bytes, which are swept both as
    /// text and as raw memory.
    ///
    /// A `Vec<u8>` that is not valid UTF-8 still leaks perfectly well if the key
    /// is in it, and lossy conversion alone would let a leak hide behind a
    /// replacement character adjacent to the material.
    pub(crate) fn assert_absent_bytes(&self, surface: &str, bytes: &[u8]) {
        self.assert_absent(surface, &String::from_utf8_lossy(bytes));
        for sentinel in &self.sentinels {
            for (encoding, needle) in &sentinel.needles {
                assert!(
                    !bytes
                        .windows(needle.len())
                        .any(|window| window == needle.as_bytes()),
                    "{surface} discloses the `{}` sentinel ({encoding} encoding) in its raw bytes",
                    sentinel.label
                );
            }
        }
    }

    /// The tripwire: assert `rendered` *does* contain the named sentinel.
    ///
    /// Used on the one surface that legitimately holds the material — the fake
    /// upstream's `Authorization` header, the store's own resolution — so a test
    /// proves its detector fires before asserting that it does not.
    ///
    /// Whole-value encodings only. A tripwire's job is to say *this* key was
    /// presented, and a shared fragment of two sentinels would let it accept the
    /// other one — which is exactly the confusion (an upstream authenticated
    /// with the pre-rotation key) these tests exist to catch.
    pub(crate) fn assert_present(&self, surface: &str, label: &str, rendered: &str) {
        let sentinel = self
            .sentinels
            .iter()
            .find(|sentinel| sentinel.label == label)
            .unwrap_or_else(|| panic!("no `{label}` sentinel in this sweep"));
        assert!(
            sentinel.whole_in(rendered).is_some(),
            "{surface} does not carry the `{label}` sentinel, so a redaction assertion \
             against it would pass for the wrong reason"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MATERIAL: &str = "sk-axond-sentinel-detector-90ab";

    fn sweep() -> LeakSweep {
        LeakSweep::of([("material", MATERIAL)])
    }

    #[test]
    fn clean_text_sweeps_clean() {
        sweep().assert_absent(
            "a redacted surface",
            "SecretMaterial(<redacted>) sk-… 30 bytes",
        );
    }

    /// Each encoding is a separate way the same key escapes, so each is asserted
    /// separately: a detector that only caught the raw bytes would pass every
    /// test in this module family while a base64 envelope leaked.
    #[test]
    fn every_encoding_of_the_material_is_detected() {
        let bytes = MATERIAL.as_bytes();
        for rendered in [
            MATERIAL.to_owned(),
            MATERIAL.to_uppercase(),
            format!("token={}", STANDARD.encode(bytes)),
            format!("token={}", STANDARD_NO_PAD.encode(bytes)),
            format!("token={}", URL_SAFE_NO_PAD.encode(bytes)),
            format!("\\x{}", hex(bytes, false)),
            format!("\\X{}", hex(bytes, true)),
            // What a "log a safe prefix" helper produces.
            format!("credential={}…", &MATERIAL[..16]),
        ] {
            let sweep = sweep();
            let caught = std::panic::catch_unwind(move || {
                sweep.assert_absent("a leaking surface", &rendered);
            });
            assert!(caught.is_err(), "an encoded leak went undetected");
        }
    }

    /// The failure report names the surface and the encoding, and prints neither
    /// the material nor any fragment of it.
    #[test]
    fn the_failure_report_does_not_reprint_the_material() {
        let sweep = sweep();
        let panic = std::panic::catch_unwind(move || {
            sweep.assert_absent("the response body", MATERIAL);
        })
        .expect_err("the leak is detected");
        let message = panic
            .downcast_ref::<String>()
            .expect("a formatted panic message");
        assert!(message.contains("the response body"), "{message}");
        assert!(!message.contains(&MATERIAL[..FRAGMENT]), "{message}");
    }

    #[test]
    fn raw_bytes_are_swept_even_when_they_are_not_utf8() {
        let mut bytes = vec![0xff, 0xfe];
        bytes.extend_from_slice(MATERIAL.as_bytes());
        let sweep = sweep();
        let caught = std::panic::catch_unwind(move || {
            sweep.assert_absent_bytes("a binary column", &bytes);
        });
        assert!(caught.is_err(), "a leak in non-UTF-8 bytes went undetected");
    }

    /// Sentinels are readable on purpose, so they overlap; the tripwire has to
    /// distinguish them anyway.
    #[test]
    fn the_tripwire_does_not_accept_a_sentinel_that_merely_looks_similar() {
        let sweep = LeakSweep::of([
            ("provider", "sk-axond-sentinel-provider-6f21a9d0c7b4"),
            ("rotated", "sk-axond-sentinel-rotated-b48c37e1590a"),
        ]);
        let caught = std::panic::catch_unwind(move || {
            sweep.assert_present(
                "an upstream presented the wrong key",
                "provider",
                "Bearer sk-axond-sentinel-rotated-b48c37e1590a",
            );
        });
        assert!(
            caught.is_err(),
            "a shared prefix let the tripwire accept a different sentinel"
        );
    }

    #[test]
    fn the_tripwire_fires_when_the_material_never_entered_the_surface() {
        let sweep = sweep();
        let caught = std::panic::catch_unwind(move || {
            sweep.assert_present("a fake upstream", "material", "Bearer something-else");
        });
        assert!(
            caught.is_err(),
            "a vacuous redaction test would not be caught"
        );
    }
}
