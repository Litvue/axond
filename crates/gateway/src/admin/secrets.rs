//! `/admin/v1/secrets`: the credential lifecycle that needs no redeployment.
//!
//! The other administrative routes publish *documents*. This one is the only
//! place secret material crosses the process boundary, and it crosses in one
//! direction: material goes in, and references, owners, and lifecycle states
//! come out. There is no route here — and no method on [`SecretStore`] — that
//! returns material that was stored earlier, so an administrator who can rotate
//! a provider key still cannot read the one in service (ADR 0039).
//!
//! # What each operation is for
//!
//! | Route | Does |
//! | --- | --- |
//! | `POST /admin/v1/secrets` | store material as a new secret's first version, staged |
//! | `POST /admin/v1/secrets/rotate` | store material as the *next* version of an existing secret, staged |
//! | `POST /admin/v1/secrets/lifecycle` | move one version: activate, disable, revoke, tombstone |
//! | `GET /admin/v1/secrets/{secret}` | every version of one secret, with its state |
//!
//! Staging and activation are separate calls because that separation is what
//! makes a rotation reversible: the new version is stored and provable while the
//! old one keeps serving, a credential document is published to pin the new
//! reference, and only a candidate revision that compiled against real material
//! ever becomes the active snapshot. Two versions of one secret therefore
//! overlap for as long as the operator wants them to.
//!
//! # What this surface deliberately does not do
//!
//! It does not publish a revision. Storing material changes nothing a request
//! can observe: a credential document pinning the new version is a separate,
//! ordinary [`AdminAction::Publish`] against `/admin/v1/credentials`, with its
//! own idempotency key and expected revision. Keeping them apart is what lets a
//! rotation be staged in one change window and cut over in another, and it is
//! why nothing here needs the mutation preconditions — see
//! [`AdminAction::mutates`].
//!
//! It does not read the secret store on the request path, and cannot: the store
//! reaches the running gateway through [`AdminService`], which
//! [`crate::routes`] has no handle to. Material is resolved once, while a
//! candidate snapshot is compiled ([`crate::convergence::secrets`]).
//!
//! # Deletion safety
//!
//! Tombstoning destroys material, so it is refused while the *current* revision
//! still pins the version: the operator publishes a credential that no longer
//! references it, and destroys it after. That gate reads deployment-wide desired
//! state, so it runs only once the store has confirmed the caller owns the
//! version: otherwise `secret_in_use` against `secret_not_found` would answer
//! whether another owner's material exists and is in service. Revocation is not
//! gated the same way —
//! a leaked key must be withdrawable immediately, and withdrawing it is exactly
//! what makes the next candidate compilation fail rather than silently keep
//! authorizing. The snapshot serving requests at that moment holds its own
//! resolved copy and keeps serving until a candidate replaces it, which is the
//! same last-known-good behaviour every other convergence failure has.
//!
//! [`AdminAction::mutates`]: super::auth::AdminAction::mutates
//! [`SecretStore`]: crate::backends::secrets::SecretStore

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::auth::{AdminAction, AdminAuthError, AdminGrant, AdminIdentity};
use super::error::AdminError;
use super::service::{AdminService, log_secret, log_store};
use crate::backends::secrets::{SecretDescriptor, SecretMaterial, SecretStore};
use crate::desired_state::credentials::Credentials;
use crate::desired_state::secrets::{SecretLifecycle, SecretOwner, SecretRef};
use crate::desired_state::{Actor, ResourceScope, SecretId};

/// Who did it, for the operational record.
///
/// An identifier, never a credential: [`AdminIdentity`] holds administrative
/// material in a `SecretString` that does not render, and this reads only the
/// attribution [`Actor`] carries.
fn actor_label(identity: &AdminIdentity) -> String {
    match identity.actor() {
        Actor::Human { issuer, subject } => format!("{issuer}#{subject}"),
        Actor::Breakglass => "breakglass".to_owned(),
        Actor::Workload { tenant, principal } => format!("{tenant}/{principal}"),
        Actor::System { component } => format!("system:{component}"),
    }
}

/// The operational record for a secret lifecycle call.
///
/// This type intentionally has no material-bearing field. It is the boundary
/// between the secret operation and its process log: the log can say who acted,
/// on which opaque reference, for which owner, and what state resulted, but it
/// cannot be handed plaintext by construction. These are operational records,
/// not durable audit events.
fn log_secret_operation(
    operation: &'static str,
    grant: &AdminGrant,
    reference: SecretRef,
    owner: SecretOwner,
    lifecycle: SecretLifecycle,
    changed: Option<bool>,
    base: Option<SecretRef>,
) {
    tracing::info!(
        target: "axond.admin.secrets",
        operation,
        actor = actor_label(grant.identity()),
        base = ?base,
        reference = %reference,
        owner = %owner,
        lifecycle = lifecycle.as_str(),
        changed = ?changed,
        "secret lifecycle operation"
    );
}

/// Material presented for storage.
///
/// A `String` on the way in and nothing on the way out: [`Debug`] is written by
/// hand, `Serialize` is not derived, and the value is moved into a
/// [`SecretMaterial`] — which zeroizes — before anything else touches it.
#[derive(Deserialize)]
#[serde(transparent)]
pub struct PresentedMaterial(String);

impl PresentedMaterial {
    /// Take the material, leaving nothing behind to log.
    fn into_material(self) -> SecretMaterial {
        SecretMaterial::new(self.0)
    }
}

/// The reason no request type in this module derives `Debug`: a derived one
/// would print the field.
impl std::fmt::Debug for PresentedMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PresentedMaterial(<redacted>)")
    }
}

/// `POST /admin/v1/secrets`: store material as a new secret.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StageSecretRequest {
    /// The owning tenant. Required: there is no deployment-wide material,
    /// because material no tenant owns is material no credential may resolve.
    pub tenant: String,
    #[serde(default)]
    pub project: Option<String>,
    pub material: PresentedMaterial,
}

/// `POST /admin/v1/secrets/rotate`: store material as the next version of an
/// existing secret.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RotateSecretRequest {
    pub tenant: String,
    #[serde(default)]
    pub project: Option<String>,
    /// The exact version being rotated *from*, as `sct_…@v1`. Exact, so a
    /// rotation raced against another administrator's rotation is a refusal
    /// rather than a silently skipped version.
    pub reference: String,
    pub material: PresentedMaterial,
}

/// `POST /admin/v1/secrets/lifecycle`: move one version's state.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretLifecycleRequest {
    pub tenant: String,
    #[serde(default)]
    pub project: Option<String>,
    pub reference: String,
    /// `staged`, `active`, `disabled`, `revoked`, or `tombstoned`.
    pub lifecycle: String,
}

/// What a secret version is, said without unwrapping it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SecretVersionView {
    /// `sct_…@v2`: the only secret-shaped value this surface ever returns.
    pub reference: String,
    pub secret: String,
    pub version: u64,
    pub owner: String,
    pub lifecycle: &'static str,
    /// Whether a candidate revision pinning this version could compile against
    /// it — the *rotation status* an operator reads before cutting over.
    /// Lifecycle only: whether the material still unwraps is answered by
    /// compiling a candidate, which is the one thing that proves it.
    pub resolvable: bool,
}

impl SecretVersionView {
    fn of(descriptor: SecretDescriptor) -> Self {
        Self {
            reference: descriptor.reference.to_string(),
            secret: descriptor.reference.secret.to_string(),
            version: descriptor.reference.version.get(),
            owner: descriptor.owner.to_string(),
            lifecycle: descriptor.lifecycle.as_str(),
            resolvable: descriptor.permits_resolution(),
        }
    }
}

/// Every version of one secret, oldest first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SecretVersionsView {
    pub secret: String,
    pub owner: String,
    pub versions: Vec<SecretVersionView>,
}

/// What a lifecycle call did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SecretTransitionView {
    pub reference: String,
    pub lifecycle: &'static str,
    /// `false` when the version was already in the requested state. A retry is
    /// not a second change, and an audit reader should not read it as one.
    pub changed: bool,
}

impl AdminService {
    /// The secret store, or the refusal a deployment without one owes.
    ///
    /// Stateless mode has neither store, and says so with the answer every other
    /// administrative operation gives it. A stateful deployment always has one:
    /// configuration requires `[secret_store]`, and boot refuses to come up
    /// without reaching it.
    fn secret_store(&self) -> Result<&Arc<dyn SecretStore>, AdminError> {
        self.store()?;
        self.secrets
            .as_ref()
            .ok_or(AdminError::StatefulModeRequired)
    }

    /// Check that a grant covers this action at this owner's scope.
    ///
    /// The same containment [`AdminService::apply`] enforces over a candidate's
    /// delta, applied to the one owner a material call names: a project-scoped
    /// administrator reaches that project's material and no other's, and a
    /// tenant-scoped one reaches the tenant's own and its projects'. The store
    /// enforces ownership again on every operation, so a grant that passed here
    /// still cannot reach material the owner does not hold.
    fn permits_material(
        grant: &AdminGrant,
        action: AdminAction,
        owner: SecretOwner,
    ) -> Result<(), AdminError> {
        if grant.action() != action {
            return Err(AdminError::Forbidden(AdminAuthError::ActionNotPermitted {
                action,
            }));
        }
        let scope = owner.scope();
        let covered = match grant.scope() {
            ResourceScope::Deployment => true,
            ResourceScope::Tenant(tenant) => scope.tenant() == Some(*tenant),
            granted => granted == &scope,
        };
        if covered {
            Ok(())
        } else {
            Err(AdminError::Forbidden(AdminAuthError::ScopeNotPermitted))
        }
    }

    /// Store material as a new secret's first version.
    ///
    /// Staged, never active: material becomes servable when a credential
    /// document pins it and a candidate revision compiles against it, not when
    /// it is stored.
    pub async fn stage_secret(
        &self,
        grant: &AdminGrant,
        owner: SecretOwner,
        material: SecretMaterial,
    ) -> Result<SecretVersionView, AdminError> {
        Self::permits_material(grant, AdminAction::WriteSecrets, owner)?;
        let store = self.secret_store()?;
        let descriptor = store.stage(owner, material).await.map_err(log_secret)?;
        if let Some(signal) = &self.change_signal {
            signal.force_refresh();
        }
        log_secret_operation(
            "stage",
            grant,
            descriptor.reference,
            descriptor.owner,
            descriptor.lifecycle,
            None,
            None,
        );
        Ok(SecretVersionView::of(descriptor))
    }

    /// Store material as the next version of an existing secret.
    pub async fn rotate_secret(
        &self,
        grant: &AdminGrant,
        owner: SecretOwner,
        reference: SecretRef,
        material: SecretMaterial,
    ) -> Result<SecretVersionView, AdminError> {
        Self::permits_material(grant, AdminAction::WriteSecrets, owner)?;
        let store = self.secret_store()?;
        let descriptor = store
            .rotate(owner, &reference, material)
            .await
            .map_err(log_secret)?;
        if let Some(signal) = &self.change_signal {
            signal.force_refresh();
        }
        log_secret_operation(
            "rotate",
            grant,
            descriptor.reference,
            descriptor.owner,
            descriptor.lifecycle,
            None,
            Some(reference),
        );
        Ok(SecretVersionView::of(descriptor))
    }

    /// Move one version's lifecycle state.
    ///
    /// Tombstoning is refused while the current revision still pins the version
    /// (see this module's documentation); every other move is the store's
    /// lifecycle matrix, unchanged.
    pub async fn move_secret(
        &self,
        grant: &AdminGrant,
        owner: SecretOwner,
        reference: SecretRef,
        next: SecretLifecycle,
    ) -> Result<SecretTransitionView, AdminError> {
        Self::permits_material(grant, AdminAction::WriteSecrets, owner)?;
        let store = self.secret_store()?;
        if next == SecretLifecycle::Tombstoned {
            // Establish ownership before looking at desired-state references:
            // that state is deployment-wide, so consulting it first would let
            // a caller distinguish a foreign reference that is in service
            // from one that is absent or retired.
            store
                .describe(owner, &reference)
                .await
                .map_err(log_secret)?;
            self.refuse_destroying_referenced_material(owner, reference)
                .await?;
        }
        let transition = store
            .transition(owner, &reference, next)
            .await
            .map_err(log_secret)?;
        if let Some(signal) = &self.change_signal {
            signal.force_refresh();
        }
        log_secret_operation(
            "lifecycle",
            grant,
            reference,
            owner,
            transition.state(),
            Some(transition.changed()),
            None,
        );
        Ok(SecretTransitionView {
            reference: reference.to_string(),
            lifecycle: transition.state().as_str(),
            changed: transition.changed(),
        })
    }

    /// Every version of one secret, with the state each is in.
    ///
    /// Empty rather than forbidden for a secret another owner holds: this route
    /// must not be a way to learn that a reference exists.
    pub async fn secret_versions(
        &self,
        grant: &AdminGrant,
        owner: SecretOwner,
        secret: SecretId,
    ) -> Result<SecretVersionsView, AdminError> {
        Self::permits_material(grant, AdminAction::ReadSecrets, owner)?;
        let store = self.secret_store()?;
        let descriptors = store.versions(owner, secret).await.map_err(log_secret)?;
        Ok(SecretVersionsView {
            secret: secret.to_string(),
            owner: owner.to_string(),
            versions: descriptors.into_iter().map(SecretVersionView::of).collect(),
        })
    }

    /// Refuse to destroy material the current desired state still pins.
    ///
    /// Read from the control plane rather than from this replica's snapshot: the
    /// question is what the deployment intends to serve, and a replica that has
    /// not converged yet is not the authority on that. A control-plane outage
    /// therefore refuses the destruction rather than allowing it — the one
    /// direction that cannot take a serving deployment down.
    async fn refuse_destroying_referenced_material(
        &self,
        owner: SecretOwner,
        reference: SecretRef,
    ) -> Result<(), AdminError> {
        let store = self.store()?;
        let Some(revision) = store.load_desired_revision().await.map_err(log_store)? else {
            return Ok(());
        };
        let credentials =
            Credentials::of(revision.state()).map_err(|error| AdminError::RevisionUnreadable {
                revision: Some(revision.id()),
                detail: error.to_string(),
            })?;
        // The references a candidate compiled from this state would resolve,
        // not every reference it mentions: a credential the operator already
        // retired names its old version forever, and material nothing resolves
        // is material nothing loses by destroying.
        let pinned = credentials
            .required_secrets()
            .any(|(pinned_owner, pinned)| pinned_owner == owner && pinned == reference);
        if pinned {
            return Err(AdminError::SecretInUse { reference });
        }
        Ok(())
    }
}

/// Parse the exact version a request names.
///
/// The refusal states the shape and never the text: these bodies sit next to
/// material, and a key pasted into the wrong field must not come back out of
/// the response or a log line built from the operator detail.
pub(super) fn reference_of(schema: &'static str, text: &str) -> Result<SecretRef, AdminError> {
    SecretRef::parse(text).map_err(|_| AdminError::RequestInvalid {
        schema,
        detail: format!(
            "`reference` names an exact version, as `{}…@v1`; the text presented is not reported, \
             because these requests carry material",
            SecretId::PREFIX
        ),
    })
}

/// Parse the lifecycle state a request names.
pub(super) fn lifecycle_of(
    schema: &'static str,
    text: &str,
) -> Result<SecretLifecycle, AdminError> {
    SecretLifecycle::parse(text).ok_or_else(|| AdminError::RequestInvalid {
        schema,
        detail: format!(
            "`lifecycle` must be one of {}",
            SecretLifecycle::ALL
                .iter()
                .map(|state| state.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    })
}

/// The owner a request names, refusing a deployment-wide one: material belongs
/// to a tenant, and optionally to one of its projects.
///
/// Like [`reference_of`], an unparseable id is described rather than quoted.
pub(super) fn owner_of(
    schema: &'static str,
    tenant: &str,
    project: Option<&str>,
) -> Result<SecretOwner, AdminError> {
    let scope =
        super::handlers::scope_of(schema, Some(tenant), project).map_err(|error| match error {
            AdminError::RequestInvalid { schema, .. } => AdminError::RequestInvalid {
                schema,
                detail: "`tenant` and `project` are prefixed ids, as `ten_…` and `prj_…`; the \
                         text presented is not reported, because these requests carry material"
                    .to_owned(),
            },
            other => other,
        })?;
    SecretOwner::from_scope(&scope).ok_or(AdminError::RequestInvalid {
        schema,
        detail: "`tenant`: secret material is owned by a tenant, never by the deployment"
            .to_owned(),
    })
}

/// The secret a versions read names.
///
/// Non-echoing for the same reason as [`reference_of`], and because a path
/// segment lands in more places than a body field does.
pub(super) fn secret_of(schema: &'static str, text: &str) -> Result<SecretId, AdminError> {
    SecretId::parse(text).map_err(|_| AdminError::RequestInvalid {
        schema,
        detail: format!(
            "`secret` is a prefixed id, as `{}…`; the text presented is not reported, because \
             this surface neighbours material",
            SecretId::PREFIX
        ),
    })
}

/// Material a store would refuse anyway, refused before it is moved anywhere.
pub(super) fn material_of(
    schema: &'static str,
    presented: PresentedMaterial,
) -> Result<SecretMaterial, AdminError> {
    let material = presented.into_material();
    if material.is_empty() {
        return Err(AdminError::SecretMaterialRefused {
            detail: format!("`{schema}`: the presented material is empty"),
        });
    }
    Ok(material)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::{
        AdminAction, AdminGrant, AdminIdentity, AdminService, lifecycle_of, owner_of, reference_of,
        secret_of,
    };
    use crate::admin::error::AdminError;
    use crate::backends::fakes::InMemorySecrets;
    use crate::backends::secrets::SecretMaterial;
    use crate::desired_state::ResourceScope;
    use crate::desired_state::fixtures;
    use crate::desired_state::oracle::InMemoryControlPlane;
    use tracing_subscriber::layer::SubscriberExt as _;

    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    impl CapturedLogs {
        fn rendered(&self) -> String {
            String::from_utf8(self.0.lock().expect("not poisoned").clone()).expect("utf-8 logs")
        }
    }

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("not poisoned")
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CapturedLogs {
        type Writer = Self;

        fn make_writer(&'writer self) -> Self::Writer {
            self.clone()
        }
    }

    fn withholds(error: &AdminError, presented: &str) {
        assert!(!error.to_string().contains(presented));
        assert!(
            !error
                .operator_detail()
                .is_some_and(|detail| detail.contains(presented))
        );
    }

    #[test]
    fn an_invalid_lifecycle_does_not_echo_the_caller_value_to_response_or_log() {
        let caller_value = "sk-lifecycle-value-must-not-echo";
        let error = lifecycle_of("secret_lifecycle", caller_value).expect_err("invalid state");

        withholds(&error, caller_value);
    }

    #[test]
    fn a_field_that_neighbours_material_never_quotes_what_was_presented() {
        // The mistake this exists for: a provider key pasted into the field that
        // was meant to hold the reference to it.
        let pasted = "sk-live-must-not-echo@v1";

        withholds(
            &reference_of("secret_rotate", pasted).expect_err("not a reference"),
            "sk-live-must-not-echo",
        );
        withholds(
            &secret_of("secret_versions", pasted).expect_err("not a secret"),
            "sk-live-must-not-echo",
        );
        withholds(
            &owner_of("secret_rotate", pasted, None).expect_err("not a tenant"),
            "sk-live-must-not-echo",
        );
        withholds(
            &owner_of(
                "secret_rotate",
                "ten_00000000-0000-7000-8000-000000000000",
                Some(pasted),
            )
            .expect_err("not a project"),
            "sk-live-must-not-echo",
        );
    }

    #[test]
    fn a_missing_material_permission_names_the_requested_action() {
        let owner = crate::desired_state::secrets::SecretOwner::tenant(fixtures::tenant_id(1));
        let grant = AdminGrant::granted(
            AdminIdentity::Human {
                issuer: "https://idp.example".to_owned(),
                subject: "operator@example".to_owned(),
            },
            AdminAction::Publish,
            ResourceScope::Tenant(fixtures::tenant_id(1)),
        );

        let error = AdminService::permits_material(&grant, AdminAction::WriteSecrets, owner)
            .expect_err("publishing authority does not administer secret material");
        match error {
            AdminError::Forbidden(super::AdminAuthError::ActionNotPermitted { action }) => {
                assert_eq!(action, AdminAction::WriteSecrets);
            }
            other => panic!("unexpected refusal: {other:?}"),
        }
    }

    #[tokio::test]
    async fn secret_material_is_absent_from_operational_logs() {
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(logs.clone()),
        );
        crate::telemetry::testing::keep_callsites_answerable();
        let dispatch = tracing::Dispatch::new(subscriber);
        let _default = tracing::dispatcher::set_default(&dispatch);

        let store = Arc::new(InMemoryControlPlane::new());
        let secrets = Arc::new(InMemorySecrets::new());
        let service = AdminService::stateful(store).with_secrets(secrets);
        let owner = crate::desired_state::secrets::SecretOwner::tenant(fixtures::tenant_id(1));
        let grant = AdminGrant::granted(
            AdminIdentity::Human {
                issuer: "https://idp.example".to_owned(),
                subject: "operator@example".to_owned(),
            },
            AdminAction::WriteSecrets,
            ResourceScope::Tenant(fixtures::tenant_id(1)),
        );
        let presented = "sk-live-must-not-enter-operational-logs";

        service
            .stage_secret(&grant, owner, SecretMaterial::new(presented.to_owned()))
            .await
            .expect("secret is staged");

        let rendered = logs.rendered();
        assert!(
            rendered.contains("secret lifecycle operation"),
            "{rendered}"
        );
        assert!(rendered.contains("operation=\"stage\""), "{rendered}");
        assert!(
            !rendered.contains(presented),
            "material reached a log: {rendered}"
        );
    }
}
