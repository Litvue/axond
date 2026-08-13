//! The fixtures the isolation scenarios share: a real journal, sessions pinned
//! to one tenant, and the two-tenant state every scenario is stated against.
//!
//! # A schema per scenario, dropped with the fixture
//!
//! Every scenario owns a PostgreSQL schema, migrates the real journal into it,
//! and drops it — and the login roles it created — in [`Drop`], so a failing
//! assertion leaves nothing behind for the next run to inherit. Roles are
//! cluster-wide rather than schema-scoped, which is precisely why they are
//! tracked and dropped here: a leaked `LOGIN` role from a panicking test is a
//! credential on the CI database that outlives the test that made it.
//!
//! # Why the pinned session is a separate role
//!
//! A superuser bypasses row-level security unconditionally, and the schema's
//! owner is the connection the tests migrate with. So a scenario that asserted
//! about RLS through the store's own connection would assert nothing at all:
//! [`Journal::session`] therefore creates an ordinary `LOGIN` role, grants it
//! exactly the privileges the scenario needs, connects as it, and pins it with
//! `SET axond.tenant_id`. That is the shape a deployment gets when its
//! application role is not the migrating role — the shape the policies in
//! `control_plane_0002_tenancy_access.sql` were written for.
//!
//! # Absence is asserted by exact name, not by fragment
//!
//! [`Absent`] looks for exact identifiers rather than reusing
//! [`LeakSweep`](crate::secret_redaction::sweep::LeakSweep). A leak sweep also
//! matches twelve-character fragments, which is right for high-entropy key
//! material and wrong here: the fixtures' ids are derived from small seeds, so
//! two tenants' ids differ in a few characters and share every fragment. A
//! fragment match would report a tenant's own id as its neighbour's. What these
//! scenarios need is narrower and exact — *this* tenant id, project id, slug,
//! principal id or credential id does not appear in what the caller was told.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio_postgres::{Client, Config};

use crate::backends::control_plane::postgres::{ControlPlaneSettings, PostgresControlPlane};
use crate::backends::control_plane::{ControlPlaneError, ControlPlaneStore};
use crate::desired_state::{
    DesiredState, ExpectedRevision, LoadedRevision, RevisionId, TenantId, fixtures,
};

/// The password the scenario roles are created with.
///
/// A literal, and deliberately not a secret: the role exists for the length of
/// one test against a test database, and a generated password would only make
/// the fixture look like it was protecting something.
const ROLE_PASSWORD: &str = "isolation";

/// The tenant every scenario calls as: `acme`, seed 1 of the domain fixtures.
pub(crate) fn caller() -> TenantId {
    fixtures::tenant_id(1)
}

/// The tenant every scenario must fail to reach: `globex`, seed 11.
pub(crate) fn other() -> TenantId {
    fixtures::tenant_id(11)
}

/// Two tenants, each with a project, a directory, a credential, an alias and a
/// policy of its own, and nothing of the other's.
///
/// Built on [`fixtures::two_tenant_directory_state`] and extended with the
/// surfaces #225 names — credentials, aliases, policies — because a projection
/// isolation claim about tenancy alone would leave exactly the rows an operator
/// worries about untested. Neither tenant references anything of the other's, so
/// a cross-tenant edge in any scenario below is something the scenario *makes*
/// rather than something the fixture contains.
pub(crate) fn two_tenant_state() -> DesiredState {
    let mut state = fixtures::two_tenant_directory_state();
    let credential = fixtures::credential(&other(), 13, "secondary");
    state
        .insert(credential.clone())
        .and_then(|state| state.insert(fixtures::alias(&other(), 14, "steady", &[credential.reference])))
        .and_then(|state| state.insert(fixtures::tenant_policy(1, 1)))
        .and_then(|state| state.insert(fixtures::tenant_policy(11, 1)))
        .expect("two tenants that reference nothing of each other's are valid");
    state
}

/// [`two_tenant_state`] plus each tenant's own catalogue: one tenant-wide
/// enablement and one typed project alias resolving to it, per tenant.
///
/// Both tenants enable *the same offering* from the same deployment-wide
/// catalogue snapshot, which is the case worth asserting: a lookup keyed on the
/// offering alone, or on the alias slug alone, would answer one tenant's question
/// with the other's row. Distinct slugs, so a slug collision is not what the
/// scenarios are measuring.
pub(crate) fn two_tenant_catalogue_state() -> DesiredState {
    let mut state = two_tenant_state();
    let mine = fixtures::tenant_enablement(&caller(), 50, MODEL);
    let theirs = fixtures::tenant_enablement(&other(), 60, MODEL);
    state
        .insert(mine.clone())
        .and_then(|state| {
            state.insert(fixtures::typed_alias(
                &caller(),
                &fixtures::project_id(2),
                51,
                "quick",
                &[mine.reference],
            ))
        })
        .and_then(|state| state.insert(theirs.clone()))
        .and_then(|state| {
            state.insert(fixtures::typed_alias(
                &other(),
                &fixtures::project_id(12),
                61,
                "swift",
                &[theirs.reference],
            ))
        })
        .expect("each tenant enabling the same offering for itself is valid");
    state
}

/// The offering both tenants enable in [`two_tenant_catalogue_state`].
pub(crate) const MODEL: &str = "gpt-4o";

/// A real control-plane journal on a schema of its own.
pub(crate) struct Journal {
    pub(crate) store: Arc<PostgresControlPlane>,
    dsn: String,
    schema: String,
    /// The login roles this fixture created, so [`Drop`] can remove them.
    roles: Mutex<Vec<String>>,
}

impl Journal {
    /// A migrated journal, or `None` when no Postgres is configured and the
    /// suite is not running in required mode.
    ///
    /// `AXOND_TEST_REQUIRE_SERVICES=1` turns the `None` into a panic
    /// ([`crate::test_services`]), so the stateful lane cannot report green by
    /// skipping every scenario in this module family.
    pub(crate) async fn open() -> Option<Self> {
        let dsn = crate::test_services::postgres_dsn()?;
        let schema = format!(
            "ti_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("a monotonic wall clock")
                .as_nanos()
        );
        connect(&dsn, None)
            .await
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .expect("a fresh scenario schema");
        let store = PostgresControlPlane::connect(
            &dsn,
            ControlPlaneSettings {
                schema: Some(schema.clone()),
                operation_timeout: Duration::from_secs(10),
                connect_timeout: Duration::from_secs(5),
                ..ControlPlaneSettings::default()
            },
        )
        .await
        .expect("a migrated journal");
        Some(Self {
            store: Arc::new(store),
            dsn,
            schema,
            roles: Mutex::new(Vec::new()),
        })
    }

    /// The store as the administrative service holds it.
    pub(crate) fn store(&self) -> Arc<dyn ControlPlaneStore> {
        self.store.clone()
    }

    pub(crate) fn schema(&self) -> &str {
        &self.schema
    }

    /// Publish `state` as the next revision, attributed by `key`.
    pub(crate) async fn publish(
        &self,
        key: &str,
        expected: ExpectedRevision,
        state: DesiredState,
    ) -> Result<RevisionId, ControlPlaneError> {
        self.store
            .publish_revision(fixtures::candidate(expected, key, state))
            .await
            .map(|manifest| manifest.id)
    }

    /// Publish [`two_tenant_state`] as the first revision.
    pub(crate) async fn publish_two_tenants(&self) -> RevisionId {
        self.publish("two-tenants", ExpectedRevision::Empty, two_tenant_state())
            .await
            .expect("two tenants that reference nothing of each other's publish")
    }

    pub(crate) async fn head(&self) -> Option<RevisionId> {
        self.store
            .desired_revision()
            .await
            .expect("the head is readable")
    }

    /// The head revision, hydrated as a replica hydrates it.
    pub(crate) async fn hydrated(&self) -> LoadedRevision {
        self.store
            .load_desired_revision()
            .await
            .expect("the head hydrates")
            .expect("a published head")
    }

    /// A session as an ordinary application role: `privileges` on every table in
    /// the schema, pinned to `tenant` when one is given.
    ///
    /// An unpinned session (`None`) is the publisher: `axond.tenant_id` is
    /// unset, every policy admits everything, and it is what the scenarios read
    /// through to prove a row a pinned session could not see is nevertheless
    /// still there.
    pub(crate) async fn session(
        &self,
        label: &str,
        privileges: &str,
        tenant: Option<TenantId>,
    ) -> Client {
        let role = format!("{}_{label}", self.schema);
        let schema = &self.schema;
        connect(&self.dsn, None)
            .await
            .batch_execute(&format!(
                "CREATE ROLE {role} LOGIN PASSWORD '{ROLE_PASSWORD}'; \
                 GRANT USAGE ON SCHEMA {schema} TO {role}; \
                 GRANT {privileges} ON ALL TABLES IN SCHEMA {schema} TO {role}"
            ))
            .await
            .expect("a scenario role");
        self.roles.lock().expect("the role list").push(role.clone());

        let client = connect(&self.dsn, Some(&role)).await;
        let pin = match tenant {
            Some(tenant) => format!("SET axond.tenant_id = '{tenant}'"),
            None => String::from("RESET axond.tenant_id"),
        };
        client
            .batch_execute(&format!("SET search_path TO {schema}; {pin}"))
            .await
            .expect("a pinned session");
        client
    }

    /// One text column of a query, run as the migrating role: what is *actually*
    /// stored, whatever a pinned session can see of it.
    pub(crate) async fn stored(&self, sql: &str) -> Vec<String> {
        let client = connect(&self.dsn, None).await;
        client
            .batch_execute(&format!("SET search_path TO {}", self.schema))
            .await
            .expect("the journal's schema");
        column(&client, sql).await
    }
}

impl Drop for Journal {
    /// Drop the schema and every role this scenario created, even when the
    /// scenario panicked.
    ///
    /// On its own thread with its own runtime, because [`Drop`] is synchronous
    /// and the surrounding runtime may already be unwinding. A cleanup that
    /// itself fails is a panic *unless* the test is already panicking, where a
    /// second panic would replace the assertion failure the operator needs to
    /// read with a teardown error.
    fn drop(&mut self) {
        let dsn = self.dsn.clone();
        let schema = self.schema.clone();
        let roles = std::mem::take(&mut *self.roles.lock().expect("the role list"));
        let cleanup = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a cleanup runtime");
            runtime.block_on(async {
                let client = connect(&dsn, None).await;
                client
                    .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                    .await
                    .expect("the scenario's schema is dropped");
                for role in roles {
                    client
                        .batch_execute(&format!(
                            "DROP OWNED BY {role} CASCADE; DROP ROLE IF EXISTS {role}"
                        ))
                        .await
                        .expect("the scenario's role is dropped");
                }
            });
        });
        if cleanup.join().is_err() && !std::thread::panicking() {
            panic!("a scenario left its schema or its login roles behind");
        }
    }
}

/// A connection to the test database, as `role` or as the configured user.
async fn connect(dsn: &str, role: Option<&str>) -> Client {
    let mut config: Config = dsn.parse().expect("a parseable test DSN");
    config.connect_timeout(Duration::from_secs(5));
    if let Some(role) = role {
        config.user(role).password(ROLE_PASSWORD);
    }
    let (client, connection) = config
        .connect(crate::usage::tls_connector())
        .await
        .expect("a connection to the test database");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// A database error with the server's own message, which
/// [`tokio_postgres::Error`]'s own rendering keeps in its source rather than its
/// `Display`. An assertion about *why* a write was refused is worthless if the
/// text it matches on is the constant `db error`.
pub(crate) fn detail(error: &tokio_postgres::Error) -> String {
    let mut rendered = error.to_string();
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        rendered.push_str(": ");
        rendered.push_str(&cause.to_string());
        source = cause.source();
    }
    rendered
}

/// The first column of `sql`, as text, with `NULL` rendered rather than dropped.
pub(crate) async fn column(client: &Client, sql: &str) -> Vec<String> {
    client
        .query(sql, &[])
        .await
        .unwrap_or_else(|error| panic!("the read itself must succeed: {}", detail(&error)))
        .iter()
        .map(|row| {
            row.try_get::<_, Option<String>>(0)
                .expect("a text column")
                .unwrap_or_else(|| "<null>".to_owned())
        })
        .collect()
}

/// Whether `sql` is refused by the database, and how.
pub(crate) async fn refused(client: &Client, sql: &str) -> String {
    let refusal = client
        .batch_execute(sql)
        .await
        .expect_err("the database must refuse the write");
    detail(&refusal)
}

/// How many rows `sql` affected. Zero is the answer row-level security gives a
/// pinned `UPDATE` or `DELETE` that names another tenant's rows: they are not
/// there to be matched.
pub(crate) async fn affected(client: &Client, sql: &str) -> u64 {
    client
        .execute(sql, &[])
        .await
        .unwrap_or_else(|error| panic!("the statement itself must run: {}", detail(&error)))
}

/// Identifiers that must not appear in a surface a caller can see.
///
/// Exact matches, per this module's header: the fixtures' ids are seeded, so a
/// fragment search would flag a tenant's own id as its neighbour's.
pub(crate) struct Absent {
    names: Vec<(&'static str, String)>,
}

impl Absent {
    pub(crate) fn of(names: impl IntoIterator<Item = (&'static str, String)>) -> Self {
        let names: Vec<_> = names.into_iter().collect();
        assert!(
            names.iter().all(|(_, value)| !value.is_empty()),
            "an empty identifier would make every absence assertion vacuous"
        );
        Self { names }
    }

    /// Every identifier of the tenant a scenario must not reach: its id, its
    /// slug, its project, its administrator, its workload, its credential and
    /// the secret that credential points at.
    pub(crate) fn of_the_other_tenant() -> Self {
        let credential = fixtures::credential(&other(), 13, "secondary");
        Self::of([
            ("tenant id", other().to_string()),
            ("tenant slug", "globex".to_owned()),
            ("project id", fixtures::project_id(12).to_string()),
            ("principal id", fixtures::principal_id(40).to_string()),
            ("workload id", fixtures::principal_id(41).to_string()),
            ("credential id", credential.reference.id.to_string()),
            ("secret id", fixtures::secret_id(13).to_string()),
        ])
    }

    /// The same identifiers minus the other tenant's registration — its tenant id
    /// and its slug — which the second wall does not claim to hide.
    ///
    /// A tenant *resource* is deployment-scoped: it is the row that declares a
    /// tenant exists, `tenant_id` is `NULL` on it, and every policy in
    /// `control_plane_0002_tenancy_access.sql` admits a `NULL` owner because the
    /// journal is deployment-wide history. So a pinned session reading
    /// `axond_cp_resource_version` can enumerate which tenants exist and what they
    /// are called, and only the service layer refuses that read. Named here rather
    /// than swept for, because an assertion that quietly excluded it would hide a
    /// real surface: [`super::database`] asserts the visibility instead of the
    /// absence, so the day the policy changes, the test that changes is this one.
    pub(crate) fn of_the_other_tenants_own_rows() -> Self {
        let all = Self::of_the_other_tenant();
        Self {
            names: all
                .names
                .into_iter()
                .filter(|(label, _)| !matches!(*label, "tenant id" | "tenant slug"))
                .collect(),
        }
    }

    /// The identifiers, so a scenario can assert the other direction: that what it
    /// is looking for is stored at all, and absence is therefore the wall's doing.
    pub(crate) fn names(&self) -> &[(&'static str, String)] {
        &self.names
    }

    /// Assert that no identifier appears in `rendered`.
    ///
    /// `surface` names what was read — "the refusal the caller was given", "the
    /// pinned session's projected rows" — because that is what makes a failure
    /// actionable.
    pub(crate) fn assert_absent(&self, surface: &str, rendered: &str) {
        for (label, value) in &self.names {
            assert!(
                !rendered.contains(value.as_str()),
                "{surface} discloses the other tenant's {label}: {rendered}"
            );
        }
    }
}
