//! Qualification harnesses that need the gateway's own internals.
//!
//! The capacity and soak harnesses live in `tests/`, because what they qualify
//! is a *process*: they start `axond`, offer it traffic, and read what comes
//! back. The recovery harness cannot, and the reason is the point of #219.
//! Losing a control plane under a converged replica means holding the replica's
//! [`Reconciler`](crate::convergence::Reconciler), its
//! [`LastKnownGood`](crate::convergence::LastKnownGood) cache, and a real
//! [`PostgresControlPlane`](crate::backends::control_plane::PostgresControlPlane)
//! at once, and then taking the database away from underneath them — none of
//! which is reachable from outside the binary while stateful boot is not wired
//! to `serve`.
//!
//! So the driver lives here, in the crate, and is honest about what that buys:
//! it qualifies the *control-plane* half of each scenario against a real
//! Postgres, and the serving half stays a blocked stage in
//! `qualification/recovery/manifest.toml` until a projection a replica can serve
//! lands. Nothing here substitutes an in-process control plane for the database:
//! a run without `AXOND_TEST_POSTGRES_DSN` produces no evidence rather than
//! evidence about a fake.

pub(crate) mod evidence;
pub(crate) mod recovery;
pub(crate) mod severable;
