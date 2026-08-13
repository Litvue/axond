//! The redacted semantic diff: what publishing a candidate would change.
//!
//! A diff is what makes a dry run worth running and a review worth reading, and
//! it is also the most tempting place in the system to print a resource body.
//! Provider credentials, signing keys, and secret references *are* resource
//! bodies, so this module never renders one. Instead a body is described by:
//!
//! - its **form** — inline or content-addressed — which is enough to see that a
//!   catalogue snapshot was swapped for an inline value;
//! - the **checksum of its canonical bytes**, which changes exactly when the body
//!   changes, so "this credential was rotated" is visible without the material
//!   being; and
//! - for a blob, its **digest and size**, which are already public identifiers of
//!   immutable content.
//!
//! So the diff answers "what changed" and never "to what". A test drives a state
//! whose bodies contain secret-looking values and asserts they appear nowhere in
//! the serialized diff.
//!
//! Everything else about the diff is stable by construction: both sides are
//! *complete* desired states (a revision is never a patch), resources are matched
//! on `(kind, id)` rather than on slug — so a rename is an update rather than an
//! unrelated add and remove — and the output is ordered by that same key, so two
//! replicas diffing the same pair of revisions produce byte-identical output.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::desired_state::{
    BlobRef, Canonical, Checksum, DesiredState, ResourceBody, ResourceId, ResourceKind,
    ResourceScope, ResourceVersion, ValidationError,
};

/// Whether a resource or blob appeared, disappeared, or changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeKind {
    Added,
    Removed,
    Updated,
}

impl ChangeKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Added => "added",
            Self::Removed => "removed",
            Self::Updated => "updated",
        }
    }
}

/// A scope, rendered as ids rather than as a debug-formatted enum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ScopeView {
    pub kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
}

impl ScopeView {
    pub fn of(scope: &ResourceScope) -> Self {
        match scope {
            ResourceScope::Deployment => Self {
                kind: "deployment",
                tenant: None,
                project: None,
            },
            ResourceScope::Tenant(tenant) => Self {
                kind: "tenant",
                tenant: Some(tenant.to_string()),
                project: None,
            },
            ResourceScope::Project { tenant, project } => Self {
                kind: "project",
                tenant: Some(tenant.to_string()),
                project: Some(project.to_string()),
            },
        }
    }
}

/// A body, described without being disclosed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BodyView {
    /// `inline` or `blob`.
    pub form: &'static str,
    /// The checksum of the body's canonical bytes. Identity of content, not
    /// content: equal checksums mean an unchanged body, and an unequal one means
    /// a change whose value is not shown.
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_kind: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
}

impl BodyView {
    fn of(body: &ResourceBody) -> Result<Self, ValidationError> {
        let content = body.canonical().checksum()?;
        Ok(match body {
            ResourceBody::Inline(_) => Self {
                form: "inline",
                content: content.to_string(),
                blob_kind: None,
                size_bytes: None,
            },
            ResourceBody::Blob(blob) => Self {
                form: "blob",
                content: content.to_string(),
                blob_kind: Some(blob.kind.as_str()),
                size_bytes: Some(blob.size_bytes),
            },
        })
    }
}

/// One resource's change. Identity fields are always present; the before/after
/// fields are present on the sides that exist.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResourceDelta {
    pub change: &'static str,
    /// The resource class: `alias`, `provider-credential`, and so on.
    pub kind: &'static str,
    /// The durable resource id, stable across renames and versions.
    pub resource: String,
    pub scope: ScopeView,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slug: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_slug: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<BodyView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_body: Option<BodyView>,
    /// Whether the resource was renamed by this change — the one derived field,
    /// because a rename is what a reviewer most often scans for and comparing two
    /// optional slugs by eye is where that goes wrong.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub renamed: bool,
}

/// A blob's appearance or disappearance. A blob is immutable content, so it is
/// never `updated`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BlobDelta {
    pub change: &'static str,
    pub kind: &'static str,
    pub digest: String,
    pub size_bytes: u64,
}

/// Counts, so a reviewer can see the shape of a change before reading it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub struct DiffSummary {
    pub added: usize,
    pub removed: usize,
    pub updated: usize,
    pub unchanged: usize,
}

/// What publishing a candidate would change, in stable order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SemanticDiff {
    pub summary: DiffSummary,
    pub resources: Vec<ResourceDelta>,
    pub blobs: Vec<BlobDelta>,
}

impl SemanticDiff {
    /// Diff a complete candidate against the complete state it would replace.
    ///
    /// `None` is the state of a deployment that has published nothing, which is
    /// an empty state rather than a special case.
    pub fn between(
        previous: Option<&DesiredState>,
        candidate: &DesiredState,
    ) -> Result<Self, ValidationError> {
        let before = by_resource(previous);
        let after = by_resource(Some(candidate));
        let mut summary = DiffSummary::default();
        let mut resources = Vec::new();
        // A set, so a resource present on both sides is visited once, and in
        // `(kind, id)` order — the output needs no sort, and cannot depend on
        // which side a resource was found on.
        let keys: BTreeSet<(ResourceKind, ResourceId)> =
            before.keys().chain(after.keys()).copied().collect();
        for key in keys {
            match (before.get(&key), after.get(&key)) {
                (Some(old), Some(new)) => {
                    if old.content_checksum()? == new.content_checksum()? {
                        summary.unchanged += 1;
                    } else {
                        summary.updated += 1;
                        resources.push(updated(old, new)?);
                    }
                }
                (None, Some(new)) => {
                    summary.added += 1;
                    resources.push(added(new)?);
                }
                (Some(old), None) => {
                    summary.removed += 1;
                    resources.push(removed(old)?);
                }
                (None, None) => unreachable!("the key came from one of the two maps"),
            }
        }
        let previous_blobs = blobs(previous);
        let candidate_blobs = blobs(Some(candidate));
        let mut blob_deltas: Vec<BlobDelta> = previous_blobs
            .iter()
            .filter(|(digest, _)| !candidate_blobs.contains_key(*digest))
            .map(|(_, blob)| blob_delta(ChangeKind::Removed, blob))
            .chain(
                candidate_blobs
                    .iter()
                    .filter(|(digest, _)| !previous_blobs.contains_key(*digest))
                    .map(|(_, blob)| blob_delta(ChangeKind::Added, blob)),
            )
            .collect();
        blob_deltas
            .sort_by(|left, right| (left.change, &left.digest).cmp(&(right.change, &right.digest)));

        Ok(Self {
            summary,
            resources,
            blobs: blob_deltas,
        })
    }

    /// Whether publishing would change nothing. A caller that dry-ran and got
    /// this can skip the write rather than publish an empty revision.
    pub fn is_empty(&self) -> bool {
        self.resources.is_empty() && self.blobs.is_empty()
    }
}

/// The newest version of each resource, keyed by identity rather than by
/// reference, so a version bump is an update of one resource rather than two
/// unrelated rows.
fn by_resource(
    state: Option<&DesiredState>,
) -> BTreeMap<(ResourceKind, ResourceId), &ResourceVersion> {
    let mut map: BTreeMap<(ResourceKind, ResourceId), &ResourceVersion> = BTreeMap::new();
    for resource in state.into_iter().flat_map(DesiredState::resources) {
        map.entry((resource.reference.kind, resource.reference.id))
            .and_modify(|existing| {
                if existing.reference.version < resource.reference.version {
                    *existing = resource;
                }
            })
            .or_insert(resource);
    }
    map
}

fn blobs(state: Option<&DesiredState>) -> BTreeMap<Checksum, BlobRef> {
    state
        .into_iter()
        .flat_map(DesiredState::blobs)
        .map(|blob| (blob.digest, *blob))
        .collect()
}

fn blob_delta(change: ChangeKind, blob: &BlobRef) -> BlobDelta {
    BlobDelta {
        change: change.as_str(),
        kind: blob.kind.as_str(),
        digest: blob.digest.to_string(),
        size_bytes: blob.size_bytes,
    }
}

fn added(resource: &ResourceVersion) -> Result<ResourceDelta, ValidationError> {
    Ok(ResourceDelta {
        change: ChangeKind::Added.as_str(),
        kind: resource.reference.kind.as_str(),
        resource: resource.reference.id.to_string(),
        scope: ScopeView::of(&resource.scope),
        slug: Some(resource.slug.as_str().to_owned()),
        previous_slug: None,
        version: Some(resource.reference.version.get()),
        previous_version: None,
        body: Some(BodyView::of(&resource.body)?),
        previous_body: None,
        renamed: false,
    })
}

fn removed(resource: &ResourceVersion) -> Result<ResourceDelta, ValidationError> {
    Ok(ResourceDelta {
        change: ChangeKind::Removed.as_str(),
        kind: resource.reference.kind.as_str(),
        resource: resource.reference.id.to_string(),
        scope: ScopeView::of(&resource.scope),
        slug: None,
        previous_slug: Some(resource.slug.as_str().to_owned()),
        version: None,
        previous_version: Some(resource.reference.version.get()),
        body: None,
        previous_body: Some(BodyView::of(&resource.body)?),
        renamed: false,
    })
}

fn updated(old: &ResourceVersion, new: &ResourceVersion) -> Result<ResourceDelta, ValidationError> {
    Ok(ResourceDelta {
        change: ChangeKind::Updated.as_str(),
        kind: new.reference.kind.as_str(),
        resource: new.reference.id.to_string(),
        scope: ScopeView::of(&new.scope),
        slug: Some(new.slug.as_str().to_owned()),
        previous_slug: Some(old.slug.as_str().to_owned()),
        version: Some(new.reference.version.get()),
        previous_version: Some(old.reference.version.get()),
        body: Some(BodyView::of(&new.body)?),
        previous_body: Some(BodyView::of(&old.body)?),
        renamed: old.slug != new.slug,
    })
}
