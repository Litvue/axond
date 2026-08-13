//! The second wall: what the database itself refuses.
//!
//! `control_plane_0002_tenancy_access.sql` enables row-level security on every
//! control-plane table, keyed on the `axond.tenant_id` session setting. These
//! scenarios assert that wall the only way it can honestly be asserted — as an
//! ordinary `LOGIN` role that is not the schema's owner and cannot bypass RLS
//! (see [`super::harness`]) — against state published through the real store.
//!
//! Two scenarios, one per direction:
//!
//! * **Reads.** Every table in the schema is rendered whole for a pinned session,
//!   and nothing in what comes back names the other tenant — while the same sweep
//!   run unpinned does name it, so the silence is the policy's and not the
//!   fixture's.
//! * **Writes.** A pinned session cannot create a tenant, cannot put a project or
//!   a principal in another tenant, cannot hand its own project to another tenant,
//!   cannot forge a denial into another tenant's trail, and its `UPDATE`s and
//!   `DELETE`s against another tenant's rows match nothing. The other tenant's
//!   rows are read back afterwards, as the migrating role, unchanged. What the
//!   policies *do* admit — an ownerless, deployment-scoped row — is asserted in
//!   the same scenario, together with the reason it gains nothing: the
//!   publication chain admits no pinned session, so such a row can never become
//!   part of a revision.
//!
//! Nothing here relies on the service layer, which is the point: these are the
//! refusals that stand if the service layer above has a bug.

use tokio_postgres::Client;

use super::harness::{Absent, Journal, affected, caller, column, other, refused};
use crate::desired_state::{DesiredState, ExpectedRevision, fixtures};

/// Every table in the schema, rendered row by row.
///
/// Whole rows as text rather than named columns: a column this scenario forgot to
/// list is exactly where a leak would hide, and `t::text` cannot forget one. Read
/// through `pg_tables` for the same reason — a table added by a later migration is
/// swept without anybody remembering to add it here.
async fn everything_readable(client: &Client, schema: &str) -> String {
    let tables = column(
        client,
        &format!(
            "SELECT tablename FROM pg_tables WHERE schemaname = '{schema}' ORDER BY tablename"
        ),
    )
    .await;
    assert!(
        tables.len() > 10,
        "the sweep found {} tables, so it is not sweeping the journal: {tables:?}",
        tables.len()
    );
    let mut rendered = String::new();
    for table in tables {
        for row in column(client, &format!("SELECT t::text FROM {schema}.{table} t")).await {
            rendered.push_str(&table);
            rendered.push(' ');
            rendered.push_str(&row);
            rendered.push('\n');
        }
    }
    // A row rendered as text renders its `bytea` columns as hex, so the canonical
    // bodies — where a credential's secret reference lives — would be swept in a
    // form no identifier could be found in. Decoded separately rather than left
    // out: a body is exactly the kind of column a leak hides in. `escape` rather
    // than `convert_from(…, 'UTF8')`, because a body is a binary encoding whose
    // framing bytes are not valid UTF-8 — but every identifier inside it is ASCII,
    // and `escape` leaves ASCII exactly as it is.
    for body in column(
        client,
        &format!(
            "SELECT encode(body_inline, 'escape') \
             FROM {schema}.axond_cp_resource_version \
             WHERE body_inline IS NOT NULL"
        ),
    )
    .await
    {
        rendered.push_str("body ");
        rendered.push_str(&body);
        rendered.push('\n');
    }
    rendered
}

/// Everything durably stored about `tenant`, read as the migrating role: what a
/// pinned session's refused writes must have left untouched.
async fn rows_of(journal: &Journal, tenant: &str) -> Vec<String> {
    let mut rows = Vec::new();
    for table in [
        "axond_cp_tenant",
        "axond_cp_project",
        "axond_cp_principal",
        "axond_cp_resource_version",
        "axond_cp_access_denial",
    ] {
        rows.extend(
            journal
                .stored(&format!(
                    "SELECT t::text FROM {table} t WHERE tenant_id = '{tenant}' ORDER BY t::text"
                ))
                .await,
        );
    }
    rows
}

/// Nothing a session pinned to one tenant can read anywhere in the schema names
/// the other tenant — and the same sweep, unpinned, does.
#[tokio::test]
async fn nothing_a_pinned_session_can_read_names_the_other_tenant() {
    let Some(journal) = Journal::open().await else {
        return;
    };
    journal.publish_two_tenants().await;
    let schema = journal.schema().to_owned();

    let pinned = journal.session("reader", "SELECT", Some(caller())).await;
    let ours = everything_readable(&pinned, &schema).await;

    // Non-vacuity in the other direction first: the pinned session does see its
    // own tenancy, so the sweep is reading rows rather than empty tables.
    for (label, id) in [
        ("its own tenant", caller().to_string()),
        ("its own project", fixtures::project_id(2).to_string()),
        (
            "its own administrator",
            fixtures::principal_id(31).to_string(),
        ),
    ] {
        assert!(
            ours.contains(&id),
            "the pinned session cannot read {label}, so the sweep proves nothing"
        );
    }

    let absent = Absent::of_the_other_tenants_own_rows();
    absent.assert_absent("a session pinned to one tenant", &ours);

    // What the wall does *not* hide, stated rather than skipped: a tenant's
    // registration is a deployment-scoped journal row with no owner, so a pinned
    // session can read that `globex` exists. Nothing of that tenant's own — no
    // project, principal, credential, secret or policy — comes with it, and the
    // administrative service is the wall that refuses this read (see
    // [`super::control_plane`]). Asserted so that narrowing the journal's policy
    // is a deliberate change to this line, not a silent loosening nobody notices.
    assert!(
        ours.contains(&other().to_string()),
        "the journal's deployment-scoped rows no longer name every tenant — if that \
         was deliberate, this scenario should now assert the absence instead"
    );

    // And the wall is what hid them: unpinned, the same sweep of the same schema
    // reads every identifier the pinned session could not.
    let publisher = journal.session("publisher", "SELECT", None).await;
    let all = everything_readable(&publisher, &schema).await;
    for (label, id) in absent.names() {
        assert!(
            all.contains(id.as_str()),
            "the other tenant's {label} is not stored at all, so hiding it proves nothing"
        );
    }
}

/// A session pinned to one tenant cannot write another tenant's rows: the writes
/// that would name it are refused, the writes that would match it match nothing,
/// and afterwards its rows are exactly as they were.
#[tokio::test]
async fn a_pinned_session_cannot_write_another_tenants_rows() {
    let Some(journal) = Journal::open().await else {
        return;
    };
    let revision = journal.publish_two_tenants().await.to_string();
    let theirs = other().to_string();
    let before = rows_of(&journal, &theirs).await;

    let session = journal
        .session("writer", "SELECT, INSERT, UPDATE, DELETE", Some(caller()))
        .await;

    // Refused outright: every one of these rows names a tenant the session is not
    // pinned to, so the policy's `WITH CHECK` rejects it before any constraint is
    // reached.
    for (attempt, sql) in [
        (
            "declaring a tenant of its own",
            format!(
                "INSERT INTO axond_cp_tenant (tenant_id, slug, lifecycle, revision_id) \
                 VALUES ('{}', 'invented', 'active', '{revision}')",
                fixtures::tenant_id(99)
            ),
        ),
        (
            "putting a project in the other tenant",
            format!(
                "INSERT INTO axond_cp_project (project_id, tenant_id, slug, revision_id) \
                 VALUES ('{}', '{theirs}', 'seized', '{revision}')",
                fixtures::project_id(98)
            ),
        ),
        (
            "putting a principal in the other tenant",
            format!(
                "INSERT INTO axond_cp_principal (principal_id, resource_id, identity_kind, \
                 scope_kind, tenant_id, slug, display_name, issuer, subject, revision_id) \
                 VALUES ('{}', '{}', 'human', 'tenant', '{theirs}', 'planted', 'Planted', \
                 'https://idp.example', 'planted', '{revision}')",
                fixtures::principal_id(97),
                fixtures::resource_id(97)
            ),
        ),
        (
            "handing its own project to the other tenant",
            format!(
                "UPDATE axond_cp_project SET tenant_id = '{theirs}' WHERE project_id = '{}'",
                fixtures::project_id(2)
            ),
        ),
        (
            "forging a denial into the other tenant's trail",
            format!(
                "INSERT INTO axond_cp_access_denial (denial_id, actor_kind, actor_issuer, \
                 actor_subject, surface, action, scope_kind, tenant_id, reason, recorded_at) \
                 VALUES ('{}', 'human', 'https://idp.example', 'planted', 'tenant', \
                 'publish', 'tenant', '{theirs}', 'out-of-scope', now())",
                forged_denial_id()
            ),
        ),
    ] {
        let error = refused(&session, &sql).await;
        assert!(
            error.contains("row-level security"),
            "{attempt} was refused for the wrong reason: {error}"
        );
    }

    // Matched nothing: an `UPDATE` or a `DELETE` naming rows the session cannot
    // see is not an error, it is a statement about no rows — which is the answer
    // that leaks nothing about whether those rows exist.
    for (attempt, sql) in [
        (
            "renaming the other tenant's project",
            format!("UPDATE axond_cp_project SET slug = 'seized' WHERE tenant_id = '{theirs}'"),
        ),
        (
            "disabling the other tenant",
            format!(
                "UPDATE axond_cp_tenant SET lifecycle = 'disabled' WHERE tenant_id = '{theirs}'"
            ),
        ),
        (
            "deleting the other tenant's principals",
            format!("DELETE FROM axond_cp_principal WHERE tenant_id = '{theirs}'"),
        ),
        (
            "deleting the other tenant's resources",
            format!("DELETE FROM axond_cp_resource_version WHERE tenant_id = '{theirs}'"),
        ),
    ] {
        assert_eq!(
            affected(&session, &sql).await,
            0,
            "{attempt} affected rows the session should not be able to see"
        );
    }

    // What the wall admits, stated rather than skipped, as on the read side: a
    // deployment-scoped row carries no owner, and every policy that admits
    // `tenant_id IS NULL` admits *writing* one — so a pinned session can append a
    // deployment-scoped resource version, which is the shape a tenant declaration
    // is stored in. It buys nothing, and that is the assertion: a resource version
    // is desired state only once a revision carries it, and the publication chain
    // admits no pinned session at all, so the forged version cannot be entered
    // into a revision and no tenant appears for it.
    let forged = fixtures::resource_id(96).to_string();
    assert_eq!(
        affected(
            &session,
            &format!(
                "INSERT INTO axond_cp_resource_version (resource_kind, resource_id, version, \
                 scope_kind, slug, body_form, body_inline, content_checksum, serializer) \
                 VALUES ('tenant', '{forged}', 1, 'deployment', 'invented', 'inline', '\\x7b7d', \
                 'sha256:{}', 'json')",
                "0".repeat(64)
            ),
        )
        .await,
        1,
        "a deployment-scoped write is refused now — if that was deliberate, this \
         scenario should assert the refusal instead"
    );
    let chained = refused(
        &session,
        &format!(
            "INSERT INTO axond_cp_revision_entry (revision_id, resource_kind, resource_id, version) \
             VALUES ('{revision}', 'tenant', '{forged}', 1)"
        ),
    )
    .await;
    assert!(
        chained.contains("row-level security"),
        "a pinned session entered a forged version into the publication chain: {chained}"
    );
    assert!(
        journal
            .stored("SELECT t::text FROM axond_cp_tenant t WHERE slug = 'invented'")
            .await
            .is_empty(),
        "a deployment-scoped write became a tenant of its own"
    );

    let after = rows_of(&journal, &theirs).await;
    assert_eq!(
        after, before,
        "the other tenant's durable rows changed under a session pinned elsewhere"
    );
    assert!(
        !before.is_empty(),
        "the other tenant has no rows to protect, so this scenario is vacuous"
    );
}

/// A syntactically valid denial id that no store issued: what a forged trail entry
/// would have to look like.
fn forged_denial_id() -> String {
    fixtures::candidate(ExpectedRevision::Empty, "forge", DesiredState::new())
        .audit
        .id
        .to_string()
}
