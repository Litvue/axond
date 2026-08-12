//! Reading a retained revision back: deterministic, bounded, and all-or-nothing.
//!
//! Publication (#165) is the write side of the journal; this is the read side
//! (#166), and it is the seam #142 compiles a runtime snapshot from. Three
//! properties define it, and every query below exists to keep one of them.
//!
//! - **Deterministic.** The same stored revision hydrates to the same
//!   [`DesiredState`], and therefore the same canonical bytes and the same
//!   [`Checksum`](crate::desired_state::Checksum), regardless of the order
//!   PostgreSQL happened to return rows in. Every read is ordered, and every
//!   collection the domain treats as a set is keyed rather than appended to, so
//!   "the checksum I loaded equals the checksum that was published" is a
//!   decision rather than a coincidence.
//! - **Complete or refused.** A caller either gets a whole
//!   [`LoadedRevision`] — which cannot be constructed without passing
//!   [`LoadedRevision::assemble`] — or a typed error. Nothing partial is
//!   returned, cached, or logged, so a revision whose rows no longer add up
//!   cannot become a snapshot a replica serves.
//! - **Bounded.** [`HydrationLimits`] caps the rows read, the resources and
//!   blobs a revision may name, the body bytes transferred, the depth of the
//!   dependency graph walked, and the size of the candidate produced. The caps
//!   are checked *before* the expensive step they bound — the body-size checks
//!   are `octet_length` predicates, not measurements of bytes already in
//!   memory — so a corrupt or hostile row cannot make hydration the thing that
//!   exhausts a replica.
//!
//! What the read layer refuses versus what the domain refuses is a deliberate
//! split. SQL answers questions about rows: does the version a manifest entry
//! names exist, is a body within its bound, does a dependency edge cross a
//! tenant boundary. The domain answers questions about *state*: is this a valid
//! revision, does it hash to what the manifest recorded. Cross-tenant isolation
//! is therefore enforced twice on purpose — once as a reference-layer query that
//! names the offending edge, and once by [`DesiredState::validate`] — because
//! that is the one class of corruption where a single missed check leaks one
//! tenant's state into another's hydration.

use std::collections::{BTreeMap, BTreeSet};
use std::time::SystemTime;

use tokio_postgres::{Row, Transaction};

use super::ControlPlaneError;
use super::postgres::{corrupt_storage, unavailable};
use super::rows;
use crate::desired_state::{
    BlobRef, Canonical, DesiredState, IntegrityError, LoadedRevision, ManifestEntry, ResourceRef,
    ResourceVersion, RevisionId, RevisionManifest, SerializerVersion, ValidationError,
};

/// What one hydration is allowed to consume.
///
/// These are refusals, not tuning: a revision that exceeds one of them is
/// reported as [`ControlPlaneError::TooLarge`] and nothing is returned. They
/// exist because hydration reads storage that a restored backup, an out-of-band
/// `UPDATE`, or a future writer may have made larger than this build expects,
/// and "hydration ran the replica out of memory" must not be a way to take
/// serving down through the control plane.
///
/// | Limit | Bounds |
/// | --- | --- |
/// | [`max_entries`](Self::max_entries) | manifest entry rows, and therefore resource versions |
/// | [`max_blobs`](Self::max_blobs) | blob references one revision may declare |
/// | [`max_blob_bytes`](Self::max_blob_bytes) | the declared payload bytes those references sum to |
/// | [`max_dependency_edges`](Self::max_dependency_edges) | dependency rows walked |
/// | [`max_dependency_depth`](Self::max_dependency_depth) | how deeply dependencies may nest |
/// | [`max_inline_body_bytes`](Self::max_inline_body_bytes) | one inline body |
/// | [`max_body_bytes`](Self::max_body_bytes) | every inline body in the revision |
/// | [`max_state_bytes`](Self::max_state_bytes) | the hydrated candidate's canonical form |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HydrationLimits {
    pub max_entries: usize,
    pub max_blobs: usize,
    pub max_blob_bytes: u64,
    pub max_dependency_edges: usize,
    pub max_dependency_depth: usize,
    pub max_inline_body_bytes: u64,
    pub max_body_bytes: u64,
    pub max_state_bytes: usize,
}

impl Default for HydrationLimits {
    /// Generous for a deployment, small for a database.
    ///
    /// A real revision is thousands of small rows plus a handful of large
    /// content-addressed payloads, so these bounds are far above what a
    /// deployment reaches and far below what would make a replica unhealthy.
    fn default() -> Self {
        Self {
            max_entries: 50_000,
            max_blobs: 1_024,
            max_blob_bytes: 4 * 1024 * 1024 * 1024,
            max_dependency_edges: 200_000,
            max_dependency_depth: 32,
            max_inline_body_bytes: 1024 * 1024,
            max_body_bytes: 256 * 1024 * 1024,
            max_state_bytes: 512 * 1024 * 1024,
        }
    }
}

impl HydrationLimits {
    /// A `LIMIT` that reads one row past a bound, so the bound is *detected*
    /// rather than silently applied. Truncating to the cap would hydrate a
    /// revision missing rows, which is the one outcome this module exists to
    /// prevent.
    fn probe(bound: usize) -> i64 {
        i64::try_from(bound.saturating_add(1)).unwrap_or(i64::MAX)
    }
}

/// Which bound a stored revision exceeded.
///
/// Typed and specific because it is an operator's answer: "this revision names
/// more resources than this build hydrates" and "this revision's bodies are
/// larger than this build reads" call for different actions.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HydrationLimit {
    #[error("it names more than {limit} resource versions")]
    Entries { limit: usize },
    #[error("it declares more than {limit} blobs")]
    Blobs { limit: usize },
    #[error("its blobs declare {observed} bytes, more than the {limit} hydration reads")]
    BlobBytes { limit: u64, observed: u64 },
    #[error("it walks more than {limit} dependency edges")]
    DependencyEdges { limit: usize },
    #[error("{reference} nests dependencies deeper than {limit}")]
    DependencyDepth {
        reference: ResourceRef,
        limit: usize,
    },
    #[error("{reference} has a {observed}-byte inline body, more than the {limit} hydration reads")]
    InlineBodyBytes {
        reference: ResourceRef,
        limit: u64,
        observed: u64,
    },
    #[error("its inline bodies total {observed} bytes, more than the {limit} hydration reads")]
    BodyBytes { limit: u64, observed: u64 },
    #[error("its canonical form is {observed} bytes, more than the {limit} hydration returns")]
    StateBytes { limit: usize, observed: usize },
}

/// A retained revision's manifest, without hydrating any body.
///
/// The cheap read #142 polls with, and the read publication replays a retried
/// candidate through. Bounded like the full hydration, and equally unwilling to
/// return an approximation: an entry whose resource version is not stored is
/// [`IntegrityError::MissingResource`], never an entry silently dropped from the
/// manifest by a join.
pub(super) async fn manifest(
    transaction: &Transaction<'_>,
    id: RevisionId,
    limits: &HydrationLimits,
) -> Result<RevisionManifest, ControlPlaneError> {
    let revision = transaction
        .query_opt(
            "SELECT parent_id, mutation_id, serializer, state_checksum, created_at \
             FROM axond_cp_revision WHERE revision_id = $1",
            &[&id.to_string()],
        )
        .await
        .map_err(|error| unavailable("read revision", &error))?
        .ok_or(ControlPlaneError::RevisionNotFound(id))?;

    let corrupt = |error: IntegrityError| ControlPlaneError::corrupt(id, error);
    let parent: Option<String> = revision.get(0);
    let parent = parent
        .map(|text| rows::revision_id(&text))
        .transpose()
        .map_err(corrupt)?;
    let mutation_text: String = revision.get(1);
    let mutation = rows::mutation_id(&mutation_text).map_err(corrupt)?;
    let serializer_text: String = revision.get(2);
    let serializer = rows::serializer(&serializer_text).map_err(corrupt)?;
    let checksum_text: String = revision.get(3);
    let checksum = rows::checksum(&checksum_text).map_err(corrupt)?;
    let created_at: SystemTime = revision.get(4);

    let named = entry_references(transaction, id, limits).await?;
    let mut entries = Vec::with_capacity(named.len());
    for row in transaction
        .query(
            "SELECT v.resource_kind, v.resource_id, v.version, v.scope_kind, v.tenant_id, \
             v.project_id, v.slug, v.content_checksum \
             FROM axond_cp_revision_entry e \
             JOIN axond_cp_resource_version v \
             USING (resource_kind, resource_id, version) \
             WHERE e.revision_id = $1 \
             ORDER BY v.resource_kind, v.resource_id, v.version \
             LIMIT $2",
            &[&id.to_string(), &HydrationLimits::probe(limits.max_entries)],
        )
        .await
        .map_err(|error| unavailable("read manifest entries", &error))?
    {
        entries.push(manifest_entry(&row).map_err(corrupt)?);
    }
    // The entry rows are the manifest; the versions they name are a join. A
    // reference that lost its version row is a dangling reference reported as
    // one, rather than a shorter manifest whose checksum then fails to match for
    // reasons that name no row.
    let hydrated: BTreeSet<ResourceRef> = entries.iter().map(|entry| entry.reference).collect();
    if let Some(reference) = named.iter().find(|reference| !hydrated.contains(reference)) {
        return Err(corrupt(IntegrityError::MissingResource {
            reference: *reference,
        }));
    }
    entries.sort_by_key(|entry| entry.reference);

    let blobs = blob_references(transaction, id, limits).await?;

    Ok(RevisionManifest {
        id,
        parent,
        created_at,
        serializer,
        mutation,
        entries,
        blobs,
        checksum,
    })
}

/// Hydrate a retained revision into a complete, verified candidate.
///
/// The order is the contract's order, and every step is a refusal rather than a
/// repair: bound the manifest, refuse a cross-tenant edge at the reference
/// layer, bound the bodies before reading them, rebuild the state, bound the
/// graph and the candidate, and only then pair manifest with state through
/// [`LoadedRevision::assemble`]. Nothing before that last step is observable to
/// a caller, which is what "no partial candidate escapes" means here.
pub(super) async fn revision(
    transaction: &Transaction<'_>,
    id: RevisionId,
    limits: &HydrationLimits,
) -> Result<LoadedRevision, ControlPlaneError> {
    let manifest = manifest(transaction, id, limits).await?;
    let corrupt = |error: IntegrityError| ControlPlaneError::corrupt(id, error);

    refuse_cross_tenant_edges(transaction, id).await?;
    let dependencies = dependency_edges(transaction, id, limits).await?;
    refuse_oversized_bodies(transaction, id, limits).await?;

    let mut state = DesiredState::new();
    for blob in &manifest.blobs {
        state.declare_blob(*blob);
    }
    for row in transaction
        .query(
            "SELECT v.resource_kind, v.resource_id, v.version, v.scope_kind, v.tenant_id, \
             v.project_id, v.slug, v.body_form, v.body_inline, v.body_blob_kind, \
             v.body_blob_digest, b.size_bytes, v.serializer \
             FROM axond_cp_revision_entry e \
             JOIN axond_cp_resource_version v \
             USING (resource_kind, resource_id, version) \
             LEFT JOIN axond_cp_blob b \
             ON b.blob_kind = v.body_blob_kind AND b.digest = v.body_blob_digest \
             WHERE e.revision_id = $1 \
             ORDER BY v.resource_kind, v.resource_id, v.version \
             LIMIT $2",
            &[&id.to_string(), &HydrationLimits::probe(limits.max_entries)],
        )
        .await
        .map_err(|error| unavailable("read resource versions", &error))?
    {
        let resource = resource_version(&row, &dependencies).map_err(corrupt)?;
        state
            .insert(resource)
            .map_err(|error| corrupt(error.into()))?;
    }

    refuse_deep_dependencies(id, &state, limits)?;
    refuse_oversized_candidate(id, &state, limits)?;

    LoadedRevision::assemble(manifest, state).map_err(corrupt)
}

/// The revision the head points at, hydrated in the same read.
///
/// One transaction, because "what is desired?" and "hydrate it" are one question
/// for #142: reading the head and then hydrating in a second transaction can
/// answer with a revision that is no longer the head, which would make a replica
/// report convergence onto a revision it never held.
pub(super) async fn desired(
    transaction: &Transaction<'_>,
    limits: &HydrationLimits,
) -> Result<Option<LoadedRevision>, ControlPlaneError> {
    let row = transaction
        .query_opt("SELECT revision_id FROM axond_cp_head WHERE singleton", &[])
        .await
        .map_err(|error| unavailable("read desired revision", &error))?
        .ok_or_else(|| {
            corrupt_storage(
                "the control-plane head row is missing; the schema was modified out of band",
            )
        })?;
    let head: Option<String> = row.get(0);
    let Some(head) = head else {
        return Ok(None);
    };
    let head = rows::revision_id(&head)
        .map_err(|error| corrupt_storage(format!("the desired revision is unreadable: {error}")))?;
    revision(transaction, head, limits).await.map(Some)
}

/// Every resource version a manifest names, read from the entry rows alone.
async fn entry_references(
    transaction: &Transaction<'_>,
    id: RevisionId,
    limits: &HydrationLimits,
) -> Result<BTreeSet<ResourceRef>, ControlPlaneError> {
    let rows = transaction
        .query(
            "SELECT resource_kind, resource_id, version FROM axond_cp_revision_entry \
             WHERE revision_id = $1 ORDER BY resource_kind, resource_id, version LIMIT $2",
            &[&id.to_string(), &HydrationLimits::probe(limits.max_entries)],
        )
        .await
        .map_err(|error| unavailable("read manifest entry references", &error))?;
    if rows.len() > limits.max_entries {
        return Err(ControlPlaneError::too_large(
            id,
            HydrationLimit::Entries {
                limit: limits.max_entries,
            },
        ));
    }
    let mut references = BTreeSet::new();
    for row in &rows {
        references
            .insert(reference(row, 0).map_err(|error| ControlPlaneError::corrupt(id, error))?);
    }
    Ok(references)
}

/// The blobs a revision declares, bounded by count and by the bytes they name.
async fn blob_references(
    transaction: &Transaction<'_>,
    id: RevisionId,
    limits: &HydrationLimits,
) -> Result<Vec<BlobRef>, ControlPlaneError> {
    let corrupt = |error: IntegrityError| ControlPlaneError::corrupt(id, error);
    let rows = transaction
        .query(
            "SELECT b.blob_kind, b.digest, b.size_bytes FROM axond_cp_revision_blob rb \
             JOIN axond_cp_blob b USING (blob_kind, digest) WHERE rb.revision_id = $1 \
             ORDER BY b.digest, b.blob_kind LIMIT $2",
            &[&id.to_string(), &HydrationLimits::probe(limits.max_blobs)],
        )
        .await
        .map_err(|error| unavailable("read revision blobs", &error))?;
    if rows.len() > limits.max_blobs {
        return Err(ControlPlaneError::too_large(
            id,
            HydrationLimit::Blobs {
                limit: limits.max_blobs,
            },
        ));
    }

    let mut blobs = Vec::with_capacity(rows.len());
    let mut declared: u64 = 0;
    for row in &rows {
        let kind: String = row.get(0);
        let digest: String = row.get(1);
        let size: i64 = row.get(2);
        let size_bytes = u64::try_from(size).map_err(|_| {
            corrupt(rows::unreadable(format!(
                "blob {digest} has a negative size"
            )))
        })?;
        declared = declared.saturating_add(size_bytes);
        if declared > limits.max_blob_bytes {
            return Err(ControlPlaneError::too_large(
                id,
                HydrationLimit::BlobBytes {
                    limit: limits.max_blob_bytes,
                    observed: declared,
                },
            ));
        }
        blobs.push(BlobRef {
            kind: rows::blob_kind(&kind).map_err(corrupt)?,
            digest: rows::checksum(&digest).map_err(corrupt)?,
            size_bytes,
        });
    }
    blobs.sort_by_key(|blob| blob.digest);
    Ok(blobs)
}

/// The dependency edges of the versions this revision pins, keyed by dependent.
async fn dependency_edges(
    transaction: &Transaction<'_>,
    id: RevisionId,
    limits: &HydrationLimits,
) -> Result<BTreeMap<ResourceRef, BTreeSet<ResourceRef>>, ControlPlaneError> {
    let rows = transaction
        .query(
            "SELECT d.resource_kind, d.resource_id, d.version, d.depends_on_kind, \
             d.depends_on_id, d.depends_on_version FROM axond_cp_resource_dependency d \
             JOIN axond_cp_revision_entry e USING (resource_kind, resource_id, version) \
             WHERE e.revision_id = $1 \
             ORDER BY d.resource_kind, d.resource_id, d.version, d.depends_on_kind, \
             d.depends_on_id, d.depends_on_version \
             LIMIT $2",
            &[
                &id.to_string(),
                &HydrationLimits::probe(limits.max_dependency_edges),
            ],
        )
        .await
        .map_err(|error| unavailable("read resource dependencies", &error))?;
    if rows.len() > limits.max_dependency_edges {
        return Err(ControlPlaneError::too_large(
            id,
            HydrationLimit::DependencyEdges {
                limit: limits.max_dependency_edges,
            },
        ));
    }

    let mut dependencies: BTreeMap<ResourceRef, BTreeSet<ResourceRef>> = BTreeMap::new();
    for row in &rows {
        let corrupt = |error: IntegrityError| ControlPlaneError::corrupt(id, error);
        let dependent = reference(row, 0).map_err(corrupt)?;
        let dependency = reference(row, 3).map_err(corrupt)?;
        dependencies
            .entry(dependent)
            .or_default()
            .insert(dependency);
    }
    Ok(dependencies)
}

/// Refuse a stored dependency edge that crosses a tenant boundary, naming it.
///
/// This is the reference-layer half of cross-tenant isolation: the comparison is
/// the join's, so it holds for every edge stored against this revision's
/// versions whether or not the row parses into something the domain would
/// accept. [`DesiredState::validate`] checks the same rule again on the
/// hydrated state, and neither check is redundant — one guards the rows, the
/// other guards the value a caller receives.
async fn refuse_cross_tenant_edges(
    transaction: &Transaction<'_>,
    id: RevisionId,
) -> Result<(), ControlPlaneError> {
    let violation = transaction
        .query_opt(
            "SELECT d.resource_kind, d.resource_id, d.version, d.depends_on_kind, \
             d.depends_on_id, d.depends_on_version, dependent.tenant_id IS NULL \
             FROM axond_cp_resource_dependency d \
             JOIN axond_cp_revision_entry e USING (resource_kind, resource_id, version) \
             JOIN axond_cp_resource_version dependent \
             USING (resource_kind, resource_id, version) \
             JOIN axond_cp_resource_version target \
             ON target.resource_kind = d.depends_on_kind \
             AND target.resource_id = d.depends_on_id \
             AND target.version = d.depends_on_version \
             WHERE e.revision_id = $1 AND target.tenant_id IS NOT NULL \
             AND (dependent.tenant_id IS NULL OR dependent.tenant_id <> target.tenant_id) \
             ORDER BY d.resource_kind, d.resource_id, d.version, d.depends_on_kind, \
             d.depends_on_id, d.depends_on_version \
             LIMIT 1",
            &[&id.to_string()],
        )
        .await
        .map_err(|error| unavailable("check tenant isolation", &error))?;
    let Some(row) = violation else {
        return Ok(());
    };
    let corrupt = |error: IntegrityError| ControlPlaneError::corrupt(id, error);
    let from = reference(&row, 0).map_err(corrupt)?;
    let to = reference(&row, 3).map_err(corrupt)?;
    let shared: bool = row.get(6);
    Err(corrupt(IntegrityError::Invalid(if shared {
        ValidationError::TenantScopedDependency { from, to }
    } else {
        ValidationError::CrossTenantReference { from, to }
    })))
}

/// Refuse a body larger than this build reads — before reading it.
///
/// `octet_length` is evaluated by the server, so an oversized body is a refusal
/// that transferred none of it. Measuring after `SELECT body_inline` would be a
/// bound the replica has already paid for.
async fn refuse_oversized_bodies(
    transaction: &Transaction<'_>,
    id: RevisionId,
    limits: &HydrationLimits,
) -> Result<(), ControlPlaneError> {
    let per_body = i64::try_from(limits.max_inline_body_bytes).unwrap_or(i64::MAX);
    if let Some(row) = transaction
        .query_opt(
            "SELECT v.resource_kind, v.resource_id, v.version, \
             octet_length(v.body_inline)::bigint \
             FROM axond_cp_revision_entry e \
             JOIN axond_cp_resource_version v \
             USING (resource_kind, resource_id, version) \
             WHERE e.revision_id = $1 AND octet_length(v.body_inline)::bigint > $2 \
             ORDER BY v.resource_kind, v.resource_id, v.version LIMIT 1",
            &[&id.to_string(), &per_body],
        )
        .await
        .map_err(|error| unavailable("measure inline bodies", &error))?
    {
        let reference =
            reference(&row, 0).map_err(|error| ControlPlaneError::corrupt(id, error))?;
        let observed: i64 = row.get(3);
        return Err(ControlPlaneError::too_large(
            id,
            HydrationLimit::InlineBodyBytes {
                reference,
                limit: limits.max_inline_body_bytes,
                observed: u64::try_from(observed).unwrap_or(u64::MAX),
            },
        ));
    }

    let total: i64 = transaction
        .query_one(
            "SELECT coalesce(sum(octet_length(v.body_inline)), 0)::bigint \
             FROM axond_cp_revision_entry e \
             JOIN axond_cp_resource_version v \
             USING (resource_kind, resource_id, version) \
             WHERE e.revision_id = $1",
            &[&id.to_string()],
        )
        .await
        .map_err(|error| unavailable("measure body bytes", &error))?
        .get(0);
    let total = u64::try_from(total).unwrap_or(u64::MAX);
    if total > limits.max_body_bytes {
        return Err(ControlPlaneError::too_large(
            id,
            HydrationLimit::BodyBytes {
                limit: limits.max_body_bytes,
                observed: total,
            },
        ));
    }
    Ok(())
}

/// One step of the dependency walk: descending into a version, or finishing it.
///
/// Explicit rather than recursive, because the depth this bounds is exactly the
/// depth a recursive walk would put on the stack — a graph storage says is 10⁵
/// deep must be a refusal, not an overflow.
enum Step {
    Enter(ResourceRef),
    Leave(ResourceRef),
}

/// Bound how deeply a revision's dependencies nest, and terminate on a cycle.
///
/// Memoized: each version's depth is computed once, so a diamond costs one visit
/// per edge instead of one per path. A version that is re-entered while it is
/// still on the current path is a cycle, which storage can hold — the domain
/// refuses one at publication, a restored backup does not — and which is
/// reported as the depth bound it cannot satisfy rather than followed.
fn refuse_deep_dependencies(
    id: RevisionId,
    state: &DesiredState,
    limits: &HydrationLimits,
) -> Result<(), ControlPlaneError> {
    let too_deep = |reference: ResourceRef| {
        ControlPlaneError::too_large(
            id,
            HydrationLimit::DependencyDepth {
                reference,
                limit: limits.max_dependency_depth,
            },
        )
    };
    // How deeply each version's own dependencies nest below it.
    let mut depth: BTreeMap<ResourceRef, usize> = BTreeMap::new();
    let mut on_path: BTreeSet<ResourceRef> = BTreeSet::new();
    for root in state.resources() {
        let mut pending = vec![Step::Enter(root.reference)];
        while let Some(step) = pending.pop() {
            match step {
                Step::Enter(current) => {
                    if depth.contains_key(&current) {
                        continue;
                    }
                    if !on_path.insert(current) {
                        return Err(too_deep(current));
                    }
                    pending.push(Step::Leave(current));
                    // A version the state does not contain is a dangling edge,
                    // which `validate` names precisely; the walk does not follow
                    // it and does not pretend it has a depth.
                    if let Some(resource) = state.get(&current) {
                        for dependency in &resource.depends_on {
                            if on_path.contains(dependency) {
                                return Err(too_deep(*dependency));
                            }
                            if !depth.contains_key(dependency) {
                                pending.push(Step::Enter(*dependency));
                            }
                        }
                    }
                }
                Step::Leave(current) => {
                    on_path.remove(&current);
                    let below = state
                        .get(&current)
                        .into_iter()
                        .flat_map(|resource| resource.depends_on.iter())
                        .filter_map(|dependency| depth.get(dependency).copied())
                        .max()
                        .map_or(0, |deepest| deepest + 1);
                    if below > limits.max_dependency_depth {
                        return Err(too_deep(current));
                    }
                    depth.insert(current, below);
                }
            }
        }
    }
    Ok(())
}

/// Bound the candidate itself, so a caller cannot be handed a state larger than
/// this build is willing to publish a snapshot from.
fn refuse_oversized_candidate(
    id: RevisionId,
    state: &DesiredState,
    limits: &HydrationLimits,
) -> Result<(), ControlPlaneError> {
    let bytes = state
        .canonical()
        .to_canonical_bytes()
        .map_err(|error| ControlPlaneError::corrupt(id, ValidationError::from(error).into()))?;
    if bytes.len() > limits.max_state_bytes {
        return Err(ControlPlaneError::too_large(
            id,
            HydrationLimit::StateBytes {
                limit: limits.max_state_bytes,
                observed: bytes.len(),
            },
        ));
    }
    Ok(())
}

/// A resource reference from three consecutive columns.
fn reference(row: &Row, at: usize) -> Result<ResourceRef, IntegrityError> {
    let kind: String = row.get(at);
    let id: String = row.get(at + 1);
    let version: i64 = row.get(at + 2);
    Ok(ResourceRef::new(
        rows::resource_kind(&kind)?,
        rows::resource_id(&id)?,
        rows::version_number(version)?,
    ))
}

fn manifest_entry(row: &Row) -> Result<ManifestEntry, IntegrityError> {
    let scope_kind: String = row.get(3);
    let tenant: Option<String> = row.get(4);
    let project: Option<String> = row.get(5);
    let slug: String = row.get(6);
    let content: String = row.get(7);
    Ok(ManifestEntry {
        reference: reference(row, 0)?,
        scope: rows::scope(&scope_kind, tenant.as_deref(), project.as_deref())?,
        slug: rows::slug(&slug)?,
        content: rows::checksum(&content)?,
    })
}

fn resource_version(
    row: &Row,
    dependencies: &BTreeMap<ResourceRef, BTreeSet<ResourceRef>>,
) -> Result<ResourceVersion, IntegrityError> {
    let reference = reference(row, 0)?;
    let scope_kind: String = row.get(3);
    let tenant: Option<String> = row.get(4);
    let project: Option<String> = row.get(5);
    let slug: String = row.get(6);
    let form: String = row.get(7);
    let inline: Option<Vec<u8>> = row.get(8);
    let blob_kind: Option<String> = row.get(9);
    let blob_digest: Option<String> = row.get(10);
    let blob_size: Option<i64> = row.get(11);
    let serializer_text: String = row.get(12);
    // A version is only readable under the encoding it was written with. The
    // revision's serializer is checked by `assemble`; this is the per-row one,
    // because a restored backup can hold rows from two builds.
    let stored = rows::serializer(&serializer_text)?;
    let current = SerializerVersion::default();
    if stored != current {
        return Err(IntegrityError::Serializer { stored, current });
    }
    let body = rows::body(
        &form,
        inline.as_deref(),
        blob_kind.as_deref(),
        blob_digest.as_deref(),
        blob_size,
    )?;
    let version = ResourceVersion::new(
        reference,
        rows::scope(&scope_kind, tenant.as_deref(), project.as_deref())?,
        rows::slug(&slug)?,
        body,
    );
    Ok(match dependencies.get(&reference) {
        Some(edges) => version.depending_on(edges.iter().copied()),
        None => version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::{BackendFailure, FailureCategory};
    use crate::desired_state::fixtures::{alias, deep_chain_state, state, tenant_id};
    use crate::desired_state::{ResourceKind, Uuid7};

    fn revision(seed: u64) -> RevisionId {
        RevisionId::new(Uuid7::from_parts(seed, 0, seed).expect("seed in range"))
    }

    #[test]
    fn a_bound_is_probed_one_row_past_itself_so_it_is_detected_not_applied() {
        assert_eq!(HydrationLimits::probe(0), 1);
        assert_eq!(HydrationLimits::probe(50_000), 50_001);
        // A bound that cannot be expressed as a `LIMIT` still reads as many rows
        // as possible rather than none.
        assert_eq!(HydrationLimits::probe(usize::MAX), i64::MAX);
    }

    #[test]
    fn exceeding_a_bound_is_a_refusal_an_operator_can_act_on() {
        let error =
            ControlPlaneError::too_large(revision(1), HydrationLimit::Entries { limit: 10 });
        assert_eq!(error.category(), FailureCategory::Denied);
        assert!(!error.retryable(), "a bound is not cleared by retrying");
        let message = error.to_string();
        assert!(
            message.contains("more than 10 resource versions"),
            "{message}"
        );
    }

    #[test]
    fn a_candidate_larger_than_the_bound_is_refused_rather_than_returned() {
        let state = state();
        let limits = HydrationLimits {
            max_state_bytes: 1,
            ..HydrationLimits::default()
        };
        let error = refuse_oversized_candidate(revision(2), &state, &limits)
            .expect_err("a one-byte ceiling cannot hold a revision");
        assert!(
            matches!(
                error,
                ControlPlaneError::TooLarge {
                    limit: HydrationLimit::StateBytes { limit: 1, .. },
                    ..
                }
            ),
            "{error:?}"
        );
        // The same state is accepted under the shipped bound, so the refusal is
        // the limit's and not the state's.
        refuse_oversized_candidate(revision(2), &state, &HydrationLimits::default())
            .expect("a fixture revision is far below the shipped bound");
    }

    #[test]
    fn nesting_deeper_than_the_bound_is_refused() {
        let state = deep_chain_state(6);
        let limits = HydrationLimits {
            max_dependency_depth: 3,
            ..HydrationLimits::default()
        };
        let error = refuse_deep_dependencies(revision(3), &state, &limits)
            .expect_err("a six-deep chain must not hydrate under a depth of three");
        assert!(
            matches!(
                error,
                ControlPlaneError::TooLarge {
                    limit: HydrationLimit::DependencyDepth { limit: 3, .. },
                    ..
                }
            ),
            "{error:?}"
        );
        refuse_deep_dependencies(revision(3), &state, &HydrationLimits::default())
            .expect("six is far below the shipped depth bound");
    }

    #[test]
    fn a_cyclic_dependency_graph_terminates_as_a_refusal() {
        // Storage the domain would never have accepted: two aliases that depend
        // on each other. Hydration must refuse it in bounded time rather than
        // descend forever.
        let tenant = tenant_id(1);
        let left = alias(&tenant, 41, "left", &[]);
        let right = alias(&tenant, 42, "right", &[left.reference]);
        let left = alias(&tenant, 41, "left", &[right.reference]);
        let mut state = DesiredState::new();
        state.insert(left).expect("distinct references");
        state.insert(right).expect("distinct references");
        assert_eq!(
            state
                .resources()
                .filter(|resource| resource.reference.kind == ResourceKind::Alias)
                .count(),
            2
        );
        let error = refuse_deep_dependencies(revision(4), &state, &HydrationLimits::default())
            .expect_err("a cycle is not hydratable");
        assert!(
            matches!(
                error,
                ControlPlaneError::TooLarge {
                    limit: HydrationLimit::DependencyDepth { .. },
                    ..
                }
            ),
            "{error:?}"
        );
    }
}
