//! Secret *references* and their lifecycle: the domain half of the
//! [`SecretStore`] contract (#198).
//!
//! Secret material is the one thing in a deployment that desired state must be
//! able to talk about without holding. So the durable model holds a reference and
//! a lifecycle state, and nothing else about a secret: a [`SecretRef`] names
//! material exactly, a [`SecretOwner`] says whose it is, and a
//! [`SecretLifecycle`] says what may be done with it. The bytes live behind
//! [`SecretStore`], which desired state never calls.
//!
//! Three properties are worth stating, because each of them is a rule the types
//! enforce rather than a convention to remember:
//!
//! - **A reference is opaque and exact.** [`SecretId`] is its own type, not a
//!   [`ResourceId`](super::ids::ResourceId), and a reference always carries a
//!   [`SecretVersion`]. Rotation mints a new version under the same id, so a
//!   revision pins the exact material it was published against and a later
//!   rotation cannot retroactively change what a published revision meant.
//! - **Ownership travels with the reference.** A [`SecretOwner`] is a tenant and
//!   optionally a project, and it is the *envelope's* scope: a credential cannot
//!   declare material owned by anybody but itself. Resolution takes the owner as
//!   an argument, so a store's answer to "give me this secret" is scoped by
//!   construction rather than by the caller remembering to check.
//! - **Lifecycle is a total, deterministic relation.** Every ordered pair of
//!   states is either a permitted move, an idempotent no-op, or a typed refusal —
//!   see [`SecretLifecycle::transition_to`]. Nothing depends on wall-clock time,
//!   the order two administrators arrived in, or how many times a request was
//!   retried.
//!
//! [`SecretStore`]: crate::backends::secrets::SecretStore

use std::fmt;
use std::num::NonZeroU64;

use super::ids::{InvalidId, ProjectId, SecretId, TenantId};
use super::resource::ResourceScope;

/// Which version of a secret's material a reference names.
///
/// One-based and monotonic: version 1 is the material first stored, and each
/// rotation is the next number. A version is never reused, so "version 3 of this
/// secret" names one immutable value for the life of the deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SecretVersion(NonZeroU64);

impl SecretVersion {
    /// The version material first stored is written under.
    pub const FIRST: Self = Self(NonZeroU64::MIN);

    /// A specific version. Zero is not a version, so it is refused rather than
    /// silently treated as the first.
    pub const fn new(version: u64) -> Option<Self> {
        match NonZeroU64::new(version) {
            Some(version) => Some(Self(version)),
            None => None,
        }
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// The version a rotation of this one produces.
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for SecretVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

/// An opaque handle to one exact version of stored secret material.
///
/// This is the only secret-shaped value that may enter a revision body, an audit
/// summary, an error, or a log line: it identifies material and reveals nothing
/// about it. Two references compare equal or unequal; neither decodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SecretRef {
    pub secret: SecretId,
    pub version: SecretVersion,
}

impl SecretRef {
    pub const fn new(secret: SecretId, version: SecretVersion) -> Self {
        Self { secret, version }
    }

    /// The first version of a newly stored secret.
    pub const fn first(secret: SecretId) -> Self {
        Self::new(secret, SecretVersion::FIRST)
    }

    /// The reference a rotation of this material produces.
    pub const fn rotated(self) -> Self {
        Self::new(self.secret, self.version.next())
    }

    /// Whether two references name the same secret, whatever version each names.
    pub fn is_same_secret(self, other: Self) -> bool {
        self.secret == other.secret
    }

    /// Read back the text form [`fmt::Display`] writes: `sct_…@v2`.
    ///
    /// Exact, and only exact. There is no way to spell "the newest version of
    /// this secret" here, because an administrative call that meant one version
    /// and reached another is precisely the mistake exact references exist to
    /// make impossible.
    pub fn parse(text: &str) -> Result<Self, InvalidSecretRef> {
        let (secret, version) = text.split_once('@').ok_or(InvalidSecretRef::Unversioned)?;
        let secret = SecretId::parse(secret)?;
        let version = version
            .strip_prefix('v')
            .and_then(|digits| digits.parse::<u64>().ok())
            .and_then(SecretVersion::new)
            .ok_or(InvalidSecretRef::Version)?;
        Ok(Self::new(secret, version))
    }
}

/// Why a text reference is not one.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidSecretRef {
    #[error(
        "a secret reference names an exact version, as `{}…@v1`",
        SecretId::PREFIX
    )]
    Unversioned,
    #[error(transparent)]
    Secret(#[from] InvalidId),
    #[error("a secret version is `v` and a number from 1 upwards")]
    Version,
}

/// `sct_…@v2`: the id and the version, never anything derived from the material.
impl fmt::Display for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.secret, self.version)
    }
}

/// Who owns a secret, and therefore who may resolve it.
///
/// A tenant, and optionally one of that tenant's projects. Derived from a
/// resource's [`ResourceScope`] rather than authored beside it, so the owner of
/// the material and the owner of the resource that points at it cannot disagree.
///
/// Ownership is *exact*: a project's credential resolves material owned by that
/// project, and a tenant's credential resolves material owned by the tenant.
/// Neither reaches the other's. Sharing one secret across a tenant's projects
/// would be a delegation, which is a decision this contract deliberately does not
/// make on an operator's behalf.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SecretOwner {
    pub tenant: TenantId,
    pub project: Option<ProjectId>,
}

impl SecretOwner {
    pub const fn tenant(tenant: TenantId) -> Self {
        Self {
            tenant,
            project: None,
        }
    }

    pub const fn project(tenant: TenantId, project: ProjectId) -> Self {
        Self {
            tenant,
            project: Some(project),
        }
    }

    /// The owner a resource's scope implies, if the scope has one.
    ///
    /// [`ResourceScope::Deployment`] has none: deployment-wide material would be
    /// material no tenant owns, which is what a *reference to nothing* looks like
    /// in this model.
    pub const fn from_scope(scope: &ResourceScope) -> Option<Self> {
        match scope {
            ResourceScope::Deployment => None,
            ResourceScope::Tenant(tenant) => Some(Self::tenant(*tenant)),
            ResourceScope::Project { tenant, project } => Some(Self::project(*tenant, *project)),
        }
    }

    /// The scope this owner is the owner of.
    pub const fn scope(self) -> ResourceScope {
        match self.project {
            None => ResourceScope::Tenant(self.tenant),
            Some(project) => ResourceScope::Project {
                tenant: self.tenant,
                project,
            },
        }
    }
}

impl fmt::Display for SecretOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.project {
            None => write!(f, "{}", self.tenant),
            Some(project) => write!(f, "{}/{project}", self.tenant),
        }
    }
}

/// What may be done with a secret version.
///
/// The states are about *material*, not about the credential resource that
/// points at it: a credential is a versioned resource whose history is the
/// revision chain, while its material is staged, put in service, taken out of
/// service, or destroyed independently of any revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum SecretLifecycle {
    /// Stored, resolvable, and not yet in service. Where every version starts, so
    /// material can be loaded and a candidate revision compiled against it before
    /// anything routes through it.
    #[default]
    Staged,
    /// In service. Exactly what a request's provider call is authorized by.
    Active,
    /// Withheld, reversibly. Material is intact and not resolvable; a
    /// misbehaving credential is disabled first and diagnosed after.
    Disabled,
    /// Withdrawn, irreversibly. Material may still exist at rest — a store
    /// destroys it on its own schedule — but nothing resolves it again, so a
    /// leaked key is revoked without waiting for a deletion to complete.
    Revoked,
    /// Material destroyed. Terminal: no state follows it, and a reference to it
    /// resolves to nothing rather than to an error an operator might retry.
    Tombstoned,
}

impl SecretLifecycle {
    /// Every state, in lifecycle order. Iterated by the contract tests, so a new
    /// state cannot be added without the transition matrix being re-stated.
    pub const ALL: &'static [Self] = &[
        Self::Staged,
        Self::Active,
        Self::Disabled,
        Self::Revoked,
        Self::Tombstoned,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Staged => "staged",
            Self::Active => "active",
            Self::Disabled => "disabled",
            Self::Revoked => "revoked",
            Self::Tombstoned => "tombstoned",
        }
    }

    /// The state a stored identifier names, or `None` for text no release wrote.
    pub fn parse(input: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|state| state.as_str() == input)
    }

    /// Whether material in this state may be unwrapped.
    ///
    /// Staged and active only. Staged is resolvable on purpose: compiling a
    /// candidate revision against material that has never served a request is how
    /// a rotation is proven before it is switched on.
    pub const fn permits_resolution(self) -> bool {
        matches!(self, Self::Staged | Self::Active)
    }

    /// Whether this state permits no further transition.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Tombstoned)
    }

    /// Whether material in this state can still be resolved *later*, after some
    /// permitted transition. Revoked and tombstoned material cannot: the
    /// distinction is what makes revocation meaningful.
    pub const fn is_withdrawn(self) -> bool {
        matches!(self, Self::Revoked | Self::Tombstoned)
    }

    /// The transition from this state to `next`, or why there is none.
    ///
    /// The whole matrix, in one place:
    ///
    /// | From | To |
    /// | --- | --- |
    /// | `Staged` | `Active`, `Disabled`, `Revoked` |
    /// | `Active` | `Disabled`, `Revoked` |
    /// | `Disabled` | `Active`, `Revoked` |
    /// | `Revoked` | `Tombstoned` |
    /// | `Tombstoned` | — |
    ///
    /// Two rules make the relation total. A move to the state a secret is
    /// already in is [`LifecycleTransition::Unchanged`], never an error: an
    /// administrative call is retried by clients, proxies, and operators, and a
    /// retry must not turn into a refusal that looks like a conflict. Everything
    /// else absent from the table is [`ForbiddenTransition`] — including every
    /// move out of `Revoked` other than tombstoning, so withdrawn material is
    /// never put back in service, and every move out of `Tombstoned`, which has
    /// no material to move.
    pub fn transition_to(self, next: Self) -> Result<LifecycleTransition, ForbiddenTransition> {
        if self == next {
            // Idempotent by construction, terminal states included: asking for
            // what already holds is an answer, not a conflict.
            return Ok(LifecycleTransition::Unchanged(self));
        }
        let permitted = match self {
            Self::Staged => matches!(next, Self::Active | Self::Disabled | Self::Revoked),
            Self::Active | Self::Disabled => {
                matches!(next, Self::Active | Self::Disabled | Self::Revoked)
            }
            Self::Revoked => matches!(next, Self::Tombstoned),
            Self::Tombstoned => false,
        };
        if permitted {
            Ok(LifecycleTransition::Moved {
                from: self,
                to: next,
            })
        } else {
            Err(ForbiddenTransition {
                from: self,
                to: next,
            })
        }
    }
}

impl fmt::Display for SecretLifecycle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a permitted lifecycle request did.
///
/// A caller that needs to know whether its own call was the one that moved the
/// secret can ask; a caller that only needs the resulting state reads
/// [`LifecycleTransition::state`]. Distinguishing them is what lets an audit
/// trail record one transition per actual change rather than one per retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleTransition {
    /// The secret was already in the requested state.
    Unchanged(SecretLifecycle),
    /// The secret moved.
    Moved {
        from: SecretLifecycle,
        to: SecretLifecycle,
    },
}

impl LifecycleTransition {
    /// The state the secret is in afterwards, either way.
    pub const fn state(self) -> SecretLifecycle {
        match self {
            Self::Unchanged(state) => state,
            Self::Moved { to, .. } => to,
        }
    }

    /// Whether this call was the one that changed the state.
    pub const fn changed(self) -> bool {
        matches!(self, Self::Moved { .. })
    }
}

/// A lifecycle move the contract does not define.
///
/// Carries the two states and nothing else: a refusal is safe to log, and it is
/// the same refusal whichever layer produced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a {from} secret cannot become {to}")]
pub struct ForbiddenTransition {
    pub from: SecretLifecycle,
    pub to: SecretLifecycle,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::desired_state::fixtures::{project_id, secret_id, tenant_id};

    #[test]
    fn a_reference_names_an_exact_version_and_prints_no_material() {
        let secret = secret_id(1);
        let reference = SecretRef::first(secret);
        assert_eq!(reference.version, SecretVersion::FIRST);
        assert_eq!(reference.to_string(), format!("{secret}@v1"));

        let rotated = reference.rotated();
        assert_eq!(rotated.version.get(), 2);
        assert!(rotated.is_same_secret(reference));
        assert_ne!(rotated, reference, "a rotation is a different reference");
        // Debug is the same opaque pair: an id and a number.
        assert!(format!("{reference:?}").contains(&secret.uuid().to_string()));
    }

    #[test]
    fn version_zero_is_not_a_version() {
        assert_eq!(SecretVersion::new(0), None);
        assert_eq!(SecretVersion::new(1), Some(SecretVersion::FIRST));
        assert_eq!(SecretVersion::new(7).map(SecretVersion::get), Some(7));
    }

    #[test]
    fn a_parsed_reference_is_the_one_that_was_printed() {
        let reference = SecretRef::first(secret_id(3)).rotated();
        assert_eq!(SecretRef::parse(&reference.to_string()), Ok(reference));

        // An administrator names a version or names nothing: a bare id would
        // have to mean "whichever is current", which is the ambiguity the
        // exact reference exists to remove.
        let bare = reference.secret.to_string();
        assert_eq!(SecretRef::parse(&bare), Err(InvalidSecretRef::Unversioned));
        for text in [
            format!("{bare}@v0"),
            format!("{bare}@2"),
            format!("{bare}@vlatest"),
            format!("{bare}@v-1"),
            format!("{bare}@v"),
        ] {
            assert_eq!(
                SecretRef::parse(&text),
                Err(InvalidSecretRef::Version),
                "{text} is not an exact version"
            );
        }
        assert!(matches!(
            SecretRef::parse("not-a-secret@v1"),
            Err(InvalidSecretRef::Secret(_))
        ));
    }

    #[test]
    fn an_owner_is_the_scope_it_came_from() {
        let tenant = tenant_id(1);
        let project = project_id(2);
        assert_eq!(
            SecretOwner::from_scope(&ResourceScope::Tenant(tenant)),
            Some(SecretOwner::tenant(tenant))
        );
        assert_eq!(
            SecretOwner::from_scope(&ResourceScope::Project { tenant, project }),
            Some(SecretOwner::project(tenant, project))
        );
        assert_eq!(SecretOwner::from_scope(&ResourceScope::Deployment), None);

        // Round trip: an owner names one scope, and that scope names it back.
        for owner in [
            SecretOwner::tenant(tenant),
            SecretOwner::project(tenant, project),
        ] {
            assert_eq!(SecretOwner::from_scope(&owner.scope()), Some(owner));
        }
        // A project's owner is not its tenant, and neither contains the other.
        assert_ne!(
            SecretOwner::tenant(tenant),
            SecretOwner::project(tenant, project)
        );
        assert_eq!(
            SecretOwner::project(tenant, project).to_string(),
            format!("{tenant}/{project}")
        );
    }

    #[test]
    fn the_lifecycle_matrix_is_total_and_deterministic() {
        use SecretLifecycle::{Active, Disabled, Revoked, Staged, Tombstoned};

        let permitted = [
            (Staged, Active),
            (Staged, Disabled),
            (Staged, Revoked),
            (Active, Disabled),
            (Active, Revoked),
            (Disabled, Active),
            (Disabled, Revoked),
            (Revoked, Tombstoned),
        ];
        for (from, to) in permitted {
            assert_eq!(
                from.transition_to(to),
                Ok(LifecycleTransition::Moved { from, to }),
                "{from} -> {to} is permitted"
            );
        }

        // Every pair the table does not name is either the idempotent case or a
        // refusal, and no pair is undefined.
        for from in SecretLifecycle::ALL.iter().copied() {
            for to in SecretLifecycle::ALL.iter().copied() {
                let outcome = from.transition_to(to);
                if from == to {
                    assert_eq!(outcome, Ok(LifecycleTransition::Unchanged(from)));
                } else if permitted.contains(&(from, to)) {
                    assert!(outcome.expect("permitted").changed());
                } else {
                    assert_eq!(outcome, Err(ForbiddenTransition { from, to }));
                }
            }
        }
    }

    #[test]
    fn withdrawn_material_is_never_put_back_in_service() {
        use SecretLifecycle::{Active, Disabled, Revoked, Staged, Tombstoned};

        for to in [Staged, Active, Disabled] {
            assert_eq!(
                Revoked.transition_to(to),
                Err(ForbiddenTransition { from: Revoked, to })
            );
        }
        for to in [Staged, Active, Disabled, Revoked] {
            assert_eq!(
                Tombstoned.transition_to(to),
                Err(ForbiddenTransition {
                    from: Tombstoned,
                    to
                })
            );
        }
        assert!(Tombstoned.is_terminal());
        assert!(
            !Revoked.is_terminal(),
            "revoked material is still tombstonable"
        );
        assert!(Revoked.is_withdrawn() && Tombstoned.is_withdrawn());
        assert!(!Disabled.is_withdrawn(), "disabling is reversible");
    }

    #[test]
    fn only_staged_and_active_material_resolves() {
        use SecretLifecycle::{Active, Disabled, Revoked, Staged, Tombstoned};

        assert!(Staged.permits_resolution());
        assert!(Active.permits_resolution());
        for state in [Disabled, Revoked, Tombstoned] {
            assert!(!state.permits_resolution(), "{state} does not resolve");
        }
        assert_eq!(SecretLifecycle::default(), Staged);
    }

    #[test]
    fn lifecycle_identifiers_round_trip_and_reject_unknown_text() {
        for state in SecretLifecycle::ALL.iter().copied() {
            assert_eq!(SecretLifecycle::parse(state.as_str()), Some(state));
            assert_eq!(state.to_string(), state.as_str());
        }
        for unknown in ["", "ACTIVE", "deleted", "staged "] {
            assert_eq!(SecretLifecycle::parse(unknown), None, "`{unknown}`");
        }
    }
}
