//! Keeping span callsites answerable while the test binary runs in parallel.
//!
//! `tracing` caches one `Interest` per callsite for the whole process, and
//! rebuilds it whenever a subscriber is registered or dropped. While only one
//! subscriber is registered, that rebuild asks *the rebuilding thread's* current
//! subscriber — so a thread with no subscriber of its own answers for every
//! other thread, and the callsite is cached as `never`. Tests that install a
//! subscriber of their own with `set_default` then lose spans they did create:
//! the macro reads the cached `never` and skips the callsite before the
//! thread's subscriber is ever consulted.
//!
//! One process-wide subscriber closes that window. It records nothing, but it
//! answers `sometimes` for every callsite, so no rebuild can settle on `never`
//! and every span is decided by the subscriber actually in scope.

use std::sync::Once;

use tracing::subscriber::{Interest, Subscriber};
use tracing::{Event, Metadata, span};

/// Records nothing, refuses nothing: it exists only so a callsite's cached
/// interest stays `sometimes` and the decision falls to the thread's subscriber.
struct KeepAsking;

impl Subscriber for KeepAsking {
    fn register_callsite(&self, _: &'static Metadata<'static>) -> Interest {
        Interest::sometimes()
    }

    fn enabled(&self, _: &Metadata<'_>) -> bool {
        false
    }

    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }

    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}

    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}

    fn event(&self, _: &Event<'_>) {}

    fn enter(&self, _: &span::Id) {}

    fn exit(&self, _: &span::Id) {}
}

/// Installs that subscriber once per process. Call it before installing a
/// scoped subscriber whose spans the test then asserts on.
///
/// Setting the global default fails when a test already installed one; that is
/// fine, because any real subscriber answers for the callsites too.
pub(crate) fn keep_callsites_answerable() {
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| {
        let _ = tracing::subscriber::set_global_default(KeepAsking);
    });
}
