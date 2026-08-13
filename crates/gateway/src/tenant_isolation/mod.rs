//! One tenant cannot reach another, asserted at every layer that could let it.
//!
//! The runtime half of #225 is a black-box suite over a booted gateway
//! (`tests/tenant_isolation.rs`): two tenants' namespaces, aliases, credentials,
//! budgets and usage, with a fake upstream recording what it was authenticated
//! with. That suite can only assert about what a *request* can reach, because
//! from outside the process there is nothing else to look at.
//!
//! This module family is the other half: the layers a request never touches
//! directly and an operator still has to be able to state a property about.
//!
//! * [`database`] — PostgreSQL row-level security, asserted as an ordinary login
//!   role rather than the schema owner: what a session pinned to one tenant can
//!   read and write if the service layer above it has a bug.
//! * [`control_plane`] — the administrative service over a real journal: a
//!   tenant-scoped grant cannot publish into another tenant or read a
//!   deployment-wide projection, the refusal it receives names nothing of that
//!   tenant, and nothing durable moves.
//! * [`catalogue`] — the typed projections every later reader goes through:
//!   credentials, models and policy, resolved from a revision that carries two
//!   tenants who enable the same offering.
//! * [`projection`] — what a converging replica makes of two tenants' stored
//!   state: one namespace per project, tenant-qualified, carrying its own durable
//!   identity, with platform fallback off.
//! * [`harness`] — the journal, the pinned sessions, the two-tenant state, and
//!   the exact-identifier absence assertions they share.
//!
//! # Every scenario is stated in both directions
//!
//! An isolation test that only asserts absence passes just as well when the
//! fixture never created the thing being hidden. So each scenario here also
//! asserts the positive: the pinned session reads its *own* rows, the unpinned
//! publisher reads the rows the pinned one could not, the deployment-wide
//! projection a scoped grant was refused does carry the other tenant's state, and
//! the other tenant's durable rows exist before a refused write and are identical
//! after it.
//!
//! # Required, not optional
//!
//! Every scenario needs PostgreSQL and returns early without one, which locally
//! is a convenience and in CI would be a hole. `AXOND_TEST_REQUIRE_SERVICES=1`
//! turns the absence of a DSN into a panic ([`crate::test_services`]), so the
//! stateful lane cannot report green by running none of this.
//!
//! # What is still not asserted here
//!
//! Tenant-scoped *human* administration is not wired into the stateful runtime —
//! `/admin/v1` authenticates a deployment-scoped breakglass credential, and the
//! projections it serves are of the whole deployment by construction. These
//! scenarios therefore assert the service's authorization at the grant seam a
//! tenant-scoped authorizer will hand it, not through an authenticated HTTP
//! request, and say so. `docs/security/tenant-isolation-evidence.md` records
//! which layer each property is enforced at and what remains blocked on the
//! runtime and admin slices.

mod catalogue;
mod control_plane;
mod database;
mod harness;
mod projection;
