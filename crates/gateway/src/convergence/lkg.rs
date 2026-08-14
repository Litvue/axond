//! The signed last-known-good revision cache.
//!
//! ADR 0027 leaves a replica with an active snapshot serving through a
//! control-plane outage, but a replica that *boots* during one has nothing to
//! serve. That is the worst moment to be unable to add capacity: an outage that
//! also freezes the fleet size turns a Postgres incident into an inference
//! incident. So every accepted revision is exported to a local file, and a cold
//! boot that cannot reach the control plane may restore from it.
//!
//! Restoring cached state is only safe if the cache cannot be *edited*, so the
//! record is authenticated:
//!
//! - the file is `magic || version || HMAC-SHA256 || record`, and the MAC covers
//!   the magic and version as well as the record, so bytes from another format or
//!   another encoding version cannot be replayed into this one;
//! - the MAC is verified in constant time before a single field is interpreted,
//!   so an operator who edits the file gets a refusal rather than a running
//!   gateway with hand-written desired state;
//! - a record that passes the MAC is *still* rebuilt through
//!   [`LoadedRevision::assemble`], so checksums, scope rules, and dangling
//!   references are re-checked rather than trusted;
//! - and the export itself is written to a temporary file, fsynced, and renamed,
//!   so a crash mid-write leaves the previous good cache instead of a truncated
//!   one.
//!
//! The record encoding is the canonical serializer ([`super::super::desired_state::canonical`]),
//! reused rather than replaced: desired state already has exactly one byte
//! representation, and introducing a second one here would be introducing a
//! second way for a checksum to disagree. What this module adds is the *inverse*
//! of the domain's `Canonical` impls — the field-by-field reconstruction the
//! domain has no reason to carry — which is why the mapping is written out
//! explicitly instead of derived.
//!
//! The cache holds no secret material. Bodies are resource envelopes and
//! canonical values; every credential is a *reference* that is resolved through
//! the secret store during compilation, so a stolen cache file discloses topology
//! an operator could read from the admin API, not keys.
//!
//! The deployment-facing signing key is standard padded base64 encoding of
//! exactly 32 bytes (256 bits), generated from a CSPRNG. The parser rejects raw
//! passphrases, non-canonical encodings, and leading or trailing whitespace, so
//! every replica derives the same HMAC key from the same Secret bytes. The
//! format enforces the 256-bit material size; only the operator's CSPRNG can
//! supply its entropy.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use ring::hmac;

use crate::desired_state::canonical::CanonicalDecodeError;
use crate::desired_state::revision::{ManifestEntry, RevisionManifest};
use crate::desired_state::{
    BlobKind, BlobRef, CanonicalError, CanonicalValue, Checksum, DesiredState, IntegrityError,
    InvalidId, LoadedRevision, MutationId, ProjectId, ResourceBody, ResourceId, ResourceKind,
    ResourceRef, ResourceScope, ResourceVersion, ResourceVersionNumber, RevisionId,
    SerializerVersion, Slug, TenantId,
};

/// The domain separator for cache files, so cached-state bytes cannot be
/// confused with canonical desired-state bytes.
const MAGIC: &[u8] = b"axond.last-known-good\0";

/// The cache record layout. A future layout is a new value, and an old file is
/// then refused by its version rather than misread.
const RECORD_VERSION: u8 = 1;

/// The shortest key accepted, in bytes. Shorter material would make the MAC a
/// formality.
const MIN_KEY_BYTES: usize = 32;
/// The environment contract is a standard padded base64 encoding of one
/// 256-bit deployment key. The encoding is fixed so every replica interprets
/// the same Secret bytes identically; surrounding whitespace is not silently
/// normalized into a different key.
const ENCODED_KEY_BYTES: usize = 32;

/// Why the last-known-good cache could not be written or read.
#[derive(Debug, thiserror::Error)]
pub enum LastKnownGoodError {
    #[error("last-known-good cache `{path}` could not be accessed: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("last-known-good signing material must be at least {MIN_KEY_BYTES} bytes, not {bytes}")]
    KeyTooShort { bytes: usize },
    #[error(
        "last-known-good signing material must be standard padded base64 encoding of exactly +         {ENCODED_KEY_BYTES} bytes"
    )]
    KeyEncoding,
    #[error("last-known-good signing material must not have leading or trailing whitespace")]
    KeyWhitespace,
    #[error(
        "last-known-good signing material must decode to exactly {ENCODED_KEY_BYTES} bytes, +         not {bytes}"
    )]
    KeyWrongLength { bytes: usize },
    /// The MAC did not verify: the file was edited, truncated, or written with a
    /// different key. Never repaired, never partially read.
    #[error(
        "last-known-good cache `{path}` is not authentic; it was edited, truncated, \
         or written with different signing material"
    )]
    Signature { path: PathBuf },
    #[error(
        "last-known-good cache `{path}` was written by an unsupported layout (version {found})"
    )]
    Version { path: PathBuf, found: u8 },
    #[error("last-known-good cache `{path}` is malformed: {detail}")]
    Malformed { path: PathBuf, detail: String },
    #[error("last-known-good cache could not be encoded: {0}")]
    Encoding(#[from] CanonicalError),
    #[error("last-known-good cache does not decode as canonical bytes: {0}")]
    Decode(#[from] CanonicalDecodeError),
    /// The record was authentic but this build did not accept the revision it
    /// describes: either the rows do not add up, or they describe a revision this
    /// build cannot read. The inner error says which, in the same words hydration
    /// from Postgres uses, because the operator response differs — repair a cache
    /// that is inconsistent, and expect an incompatible one on a replica that was
    /// rolled back onto an older build.
    #[error("last-known-good cache holds a revision this build did not accept: {0}")]
    Integrity(#[from] IntegrityError),
}

/// A local, authenticated copy of the newest revision this replica accepted.
pub struct LastKnownGood {
    path: PathBuf,
    key: hmac::Key,
}

/// Renders the path only: the signing material never reaches a log line.
impl std::fmt::Debug for LastKnownGood {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LastKnownGood")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl LastKnownGood {
    /// Construct a cache from the deployment-facing key contract: canonical
    /// padded standard base64, exactly 32 decoded bytes, and no surrounding
    /// whitespace. The value is decoded only in memory and never enters an
    /// error or diagnostic.
    pub fn from_base64(
        path: impl Into<PathBuf>,
        encoded: &str,
    ) -> Result<Self, LastKnownGoodError> {
        if encoded.trim() != encoded {
            return Err(LastKnownGoodError::KeyWhitespace);
        }
        let decoded = STANDARD
            .decode(encoded)
            .map_err(|_| LastKnownGoodError::KeyEncoding)?;
        if decoded.len() != ENCODED_KEY_BYTES {
            return Err(LastKnownGoodError::KeyWrongLength {
                bytes: decoded.len(),
            });
        }
        if STANDARD.encode(&decoded) != encoded {
            return Err(LastKnownGoodError::KeyEncoding);
        }
        Self::new(path, &decoded)
    }

    /// A cache at `path`, authenticated with `key`.
    ///
    /// The key is deployment-wide material an operator provisions like any other
    /// secret; sharing it across replicas is what lets a fresh replica read a
    /// cache restored from a sibling's volume.
    pub fn new(path: impl Into<PathBuf>, key: &[u8]) -> Result<Self, LastKnownGoodError> {
        if key.len() < MIN_KEY_BYTES {
            return Err(LastKnownGoodError::KeyTooShort { bytes: key.len() });
        }
        Ok(Self {
            path: path.into(),
            key: hmac::Key::new(hmac::HMAC_SHA256, key),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Replace the cache with `revision`.
    ///
    /// Called after publication rather than before: the cache is a record of what
    /// this replica *served*, so a candidate that was refused can never become
    /// the state a cold boot restores.
    pub fn export(&self, revision: &LoadedRevision) -> Result<(), LastKnownGoodError> {
        self.write_record(revision.manifest(), revision.state())
    }

    /// Write an authentic cache holding state this build would not assemble: what
    /// a *newer* build's export looks like to an older one, which no code path on
    /// this build can produce.
    #[cfg(test)]
    pub(crate) fn export_unassembled(
        &self,
        manifest: &RevisionManifest,
        state: &DesiredState,
    ) -> Result<(), LastKnownGoodError> {
        self.write_record(manifest, state)
    }

    fn write_record(
        &self,
        manifest: &RevisionManifest,
        state: &DesiredState,
    ) -> Result<(), LastKnownGoodError> {
        let mut file = Vec::new();
        file.extend_from_slice(MAGIC);
        file.push(RECORD_VERSION);
        let record = encode(manifest, state)?;
        let tag = hmac::sign(&self.key, &signed_bytes(&record));
        file.extend_from_slice(tag.as_ref());
        file.extend_from_slice(&record);
        self.write_atomically(&file)
    }

    /// The cached revision, or `None` when this replica has never exported one.
    ///
    /// A *present but unreadable* cache is an error rather than a `None`: silently
    /// ignoring it would boot an empty replica while telling an operator nothing
    /// about why their cache did not work.
    pub fn load(&self) -> Result<Option<LoadedRevision>, LastKnownGoodError> {
        let file = match fs::read(&self.path) {
            Ok(file) => file,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(LastKnownGoodError::Io {
                    path: self.path.clone(),
                    source,
                });
            }
        };
        let rest = file
            .strip_prefix(MAGIC)
            .ok_or_else(|| self.malformed("the file does not begin with the cache marker"))?;
        let (version, rest) = rest
            .split_first()
            .ok_or_else(|| self.malformed("the file ends before its layout version"))?;
        if *version != RECORD_VERSION {
            return Err(LastKnownGoodError::Version {
                path: self.path.clone(),
                found: *version,
            });
        }
        if rest.len() < 32 {
            return Err(self.malformed("the file ends before its signature"));
        }
        let (tag, record) = rest.split_at(32);
        // Verified before anything is parsed: an unauthentic file is never
        // interpreted, so a hand-edited cache cannot influence what is served.
        hmac::verify(&self.key, &signed_bytes(record), tag).map_err(|_| {
            LastKnownGoodError::Signature {
                path: self.path.clone(),
            }
        })?;
        decode(record)
            .map_err(|detail| self.malformed(detail))
            .and_then(|(manifest, state)| {
                LoadedRevision::assemble(manifest, state).map_err(LastKnownGoodError::from)
            })
            .map(Some)
    }

    fn malformed(&self, detail: impl Into<String>) -> LastKnownGoodError {
        LastKnownGoodError::Malformed {
            path: self.path.clone(),
            detail: detail.into(),
        }
    }

    /// Write, flush, and rename. A crash therefore leaves either the previous
    /// cache or the new one, never a half-written record that would fail its MAC
    /// on the next boot.
    fn write_atomically(&self, bytes: &[u8]) -> Result<(), LastKnownGoodError> {
        use std::io::Write as _;

        let temporary = self.path.with_extension("tmp");
        let io = |path: &Path, source: io::Error| LastKnownGoodError::Io {
            path: path.to_path_buf(),
            source,
        };
        if let Some(parent) = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|source| io(parent, source))?;
        }
        let mut file = fs::File::create(&temporary).map_err(|source| io(&temporary, source))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            // Topology, not secrets — but it is still this deployment's state.
            file.set_permissions(fs::Permissions::from_mode(0o600))
                .map_err(|source| io(&temporary, source))?;
        }
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(|source| io(&temporary, source))?;
        drop(file);
        fs::rename(&temporary, &self.path).map_err(|source| io(&self.path, source))
    }
}

/// What the MAC covers: the marker, the layout version, and the record.
fn signed_bytes(record: &[u8]) -> Vec<u8> {
    let mut signed = Vec::with_capacity(MAGIC.len() + 1 + record.len());
    signed.extend_from_slice(MAGIC);
    signed.push(RECORD_VERSION);
    signed.extend_from_slice(record);
    signed
}

fn encode(manifest: &RevisionManifest, state: &DesiredState) -> Result<Vec<u8>, CanonicalError> {
    let record = CanonicalValue::map([
        ("manifest", encode_manifest(manifest)?),
        (
            "resources",
            CanonicalValue::List(state.resources().map(encode_resource).collect()),
        ),
        (
            "blobs",
            CanonicalValue::List(state.blobs().map(encode_blob).collect()),
        ),
    ]);
    record.to_canonical_bytes()
}

fn encode_manifest(manifest: &RevisionManifest) -> Result<CanonicalValue, CanonicalError> {
    let created_at = manifest
        .created_at
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| CanonicalError::Null)?;
    Ok(CanonicalValue::map([
        ("id", CanonicalValue::string(manifest.id.to_string())),
        (
            "parent",
            // A one-or-zero element list rather than a sentinel: the canonical
            // model has no null, and "absent" must have exactly one spelling.
            CanonicalValue::List(
                manifest
                    .parent
                    .map(|parent| CanonicalValue::string(parent.to_string()))
                    .into_iter()
                    .collect(),
            ),
        ),
        (
            "created_at_nanos",
            CanonicalValue::Integer(i128::from(created_at.as_nanos() as u64)),
        ),
        (
            "serializer",
            CanonicalValue::string(manifest.serializer.as_str()),
        ),
        (
            "mutation",
            CanonicalValue::string(manifest.mutation.to_string()),
        ),
        (
            "entries",
            CanonicalValue::List(manifest.entries.iter().map(encode_entry).collect()),
        ),
        (
            "blobs",
            CanonicalValue::List(manifest.blobs.iter().map(encode_blob).collect()),
        ),
        (
            "checksum",
            CanonicalValue::Bytes(manifest.checksum.as_bytes().to_vec()),
        ),
    ]))
}

fn encode_entry(entry: &ManifestEntry) -> CanonicalValue {
    CanonicalValue::map([
        ("reference", encode_reference(&entry.reference)),
        ("scope", encode_scope(&entry.scope)),
        ("slug", CanonicalValue::string(entry.slug.as_str())),
        (
            "content",
            CanonicalValue::Bytes(entry.content.as_bytes().to_vec()),
        ),
    ])
}

fn encode_resource(resource: &ResourceVersion) -> CanonicalValue {
    CanonicalValue::map([
        ("reference", encode_reference(&resource.reference)),
        ("scope", encode_scope(&resource.scope)),
        ("slug", CanonicalValue::string(resource.slug.as_str())),
        (
            "body",
            match &resource.body {
                ResourceBody::Inline(value) => CanonicalValue::map([
                    ("form", CanonicalValue::string("inline")),
                    ("value", value.clone()),
                ]),
                ResourceBody::Blob(blob) => CanonicalValue::map([
                    ("form", CanonicalValue::string("blob")),
                    ("blob", encode_blob(blob)),
                ]),
            },
        ),
        (
            "depends_on",
            CanonicalValue::List(resource.depends_on.iter().map(encode_reference).collect()),
        ),
    ])
}

fn encode_reference(reference: &ResourceRef) -> CanonicalValue {
    CanonicalValue::map([
        ("kind", CanonicalValue::string(reference.kind.as_str())),
        ("id", CanonicalValue::string(reference.id.to_string())),
        (
            "version",
            CanonicalValue::Integer(i128::from(reference.version.get())),
        ),
    ])
}

fn encode_scope(scope: &ResourceScope) -> CanonicalValue {
    match scope {
        ResourceScope::Deployment => {
            CanonicalValue::map([("kind", CanonicalValue::string("deployment"))])
        }
        ResourceScope::Tenant(tenant) => CanonicalValue::map([
            ("kind", CanonicalValue::string("tenant")),
            ("tenant", CanonicalValue::string(tenant.to_string())),
        ]),
        ResourceScope::Project { tenant, project } => CanonicalValue::map([
            ("kind", CanonicalValue::string("project")),
            ("tenant", CanonicalValue::string(tenant.to_string())),
            ("project", CanonicalValue::string(project.to_string())),
        ]),
    }
}

fn encode_blob(blob: &BlobRef) -> CanonicalValue {
    CanonicalValue::map([
        ("kind", CanonicalValue::string(blob.kind.as_str())),
        (
            "digest",
            CanonicalValue::Bytes(blob.digest.as_bytes().to_vec()),
        ),
        (
            "size_bytes",
            CanonicalValue::Integer(i128::from(blob.size_bytes)),
        ),
    ])
}

/// Rebuild a manifest and its state from cache bytes.
///
/// Errors are `String` details rather than a typed tree: every one of them means
/// "the bytes that passed the MAC are not the record this build writes", which is
/// a single operator-facing conclusion, and the detail says which field made it
/// obvious.
fn decode(record: &[u8]) -> Result<(RevisionManifest, DesiredState), String> {
    let value = SerializerVersion::default()
        .decode(record)
        .map_err(|error| error.to_string())?;
    let record = map(&value, "record")?;
    let manifest = decode_manifest(field(record, "manifest")?)?;

    let mut state = DesiredState::new();
    for blob in list(field(record, "blobs")?, "blobs")? {
        state.declare_blob(decode_blob(blob)?);
    }
    for resource in list(field(record, "resources")?, "resources")? {
        state
            .insert(decode_resource(resource)?)
            .map_err(|error| format!("resource cannot be restored: {error}"))?;
    }
    Ok((manifest, state))
}

fn decode_manifest(value: &CanonicalValue) -> Result<RevisionManifest, String> {
    let fields = map(value, "manifest")?;
    let parents = list(field(fields, "parent")?, "manifest.parent")?;
    let parent = match parents {
        [] => None,
        [parent] => Some(revision_id(parent, "manifest.parent")?),
        _ => return Err("manifest.parent holds more than one revision".to_owned()),
    };
    let serializer = {
        let text = string(field(fields, "serializer")?, "manifest.serializer")?;
        // One variant today; an unknown spelling is refused rather than defaulted,
        // so a cache written by a future encoding is not read as this one.
        if text == SerializerVersion::V1.as_str() {
            SerializerVersion::V1
        } else {
            return Err(format!(
                "manifest.serializer `{text}` is not a known encoding"
            ));
        }
    };
    Ok(RevisionManifest {
        id: revision_id(field(fields, "id")?, "manifest.id")?,
        parent,
        created_at: SystemTime::UNIX_EPOCH
            + Duration::from_nanos(unsigned(
                field(fields, "created_at_nanos")?,
                "manifest.created_at_nanos",
            )?),
        serializer,
        mutation: mutation_id(field(fields, "mutation")?, "manifest.mutation")?,
        entries: list(field(fields, "entries")?, "manifest.entries")?
            .iter()
            .map(decode_entry)
            .collect::<Result<Vec<_>, _>>()?,
        blobs: list(field(fields, "blobs")?, "manifest.blobs")?
            .iter()
            .map(decode_blob)
            .collect::<Result<Vec<_>, _>>()?,
        checksum: digest(field(fields, "checksum")?, "manifest.checksum")?,
    })
}

fn decode_entry(value: &CanonicalValue) -> Result<ManifestEntry, String> {
    let fields = map(value, "manifest entry")?;
    Ok(ManifestEntry {
        reference: decode_reference(field(fields, "reference")?)?,
        scope: decode_scope(field(fields, "scope")?)?,
        slug: decode_slug(field(fields, "slug")?)?,
        content: digest(field(fields, "content")?, "entry.content")?,
    })
}

fn decode_resource(value: &CanonicalValue) -> Result<ResourceVersion, String> {
    let fields = map(value, "resource")?;
    let body = map(field(fields, "body")?, "resource.body")?;
    let body = match string(field(body, "form")?, "resource.body.form")?.as_str() {
        "inline" => ResourceBody::Inline(field(body, "value")?.clone()),
        "blob" => ResourceBody::Blob(decode_blob(field(body, "blob")?)?),
        form => return Err(format!("resource.body.form `{form}` is not a body shape")),
    };
    let mut resource = ResourceVersion::new(
        decode_reference(field(fields, "reference")?)?,
        decode_scope(field(fields, "scope")?)?,
        decode_slug(field(fields, "slug")?)?,
        body,
    );
    for reference in list(field(fields, "depends_on")?, "resource.depends_on")? {
        resource.depends_on.insert(decode_reference(reference)?);
    }
    Ok(resource)
}

fn decode_reference(value: &CanonicalValue) -> Result<ResourceRef, String> {
    let fields = map(value, "reference")?;
    let kind = string(field(fields, "kind")?, "reference.kind")?;
    let kind = ResourceKind::ALL
        .iter()
        .copied()
        .find(|candidate| candidate.as_str() == kind)
        .ok_or_else(|| format!("reference.kind `{kind}` is not a resource kind"))?;
    let version = unsigned(field(fields, "version")?, "reference.version")?;
    Ok(ResourceRef::new(
        kind,
        ResourceId::parse(&string(field(fields, "id")?, "reference.id")?)
            .map_err(|error| invalid_id("reference.id", error))?,
        ResourceVersionNumber::new(version)
            .ok_or_else(|| "reference.version 0 names no content".to_owned())?,
    ))
}

fn decode_scope(value: &CanonicalValue) -> Result<ResourceScope, String> {
    let fields = map(value, "scope")?;
    let tenant = |fields: &[(String, CanonicalValue)]| -> Result<TenantId, String> {
        TenantId::parse(&string(field(fields, "tenant")?, "scope.tenant")?)
            .map_err(|error| invalid_id("scope.tenant", error))
    };
    match string(field(fields, "kind")?, "scope.kind")?.as_str() {
        "deployment" => Ok(ResourceScope::Deployment),
        "tenant" => Ok(ResourceScope::Tenant(tenant(fields)?)),
        "project" => Ok(ResourceScope::Project {
            tenant: tenant(fields)?,
            project: ProjectId::parse(&string(field(fields, "project")?, "scope.project")?)
                .map_err(|error| invalid_id("scope.project", error))?,
        }),
        kind => Err(format!("scope.kind `{kind}` is not a scope")),
    }
}

fn decode_blob(value: &CanonicalValue) -> Result<BlobRef, String> {
    let fields = map(value, "blob")?;
    let kind = string(field(fields, "kind")?, "blob.kind")?;
    Ok(BlobRef {
        kind: BlobKind::ALL
            .iter()
            .copied()
            .find(|candidate| candidate.as_str() == kind)
            .ok_or_else(|| format!("blob.kind `{kind}` is not a blob kind"))?,
        digest: digest(field(fields, "digest")?, "blob.digest")?,
        size_bytes: unsigned(field(fields, "size_bytes")?, "blob.size_bytes")?,
    })
}

fn decode_slug(value: &CanonicalValue) -> Result<Slug, String> {
    Slug::parse(&string(value, "slug")?).map_err(|error| format!("slug is not valid: {error}"))
}

/// Describe an ID in the authenticated cache without copying the stored text
/// into a malformed-cache diagnostic. The field path and parser reason remain
/// bounded and actionable; the parser's retained input is for structured
/// inspection only, never for this operator-facing string.
fn invalid_id(at: &str, error: InvalidId) -> String {
    format!("{at} is not a valid id: {error}")
}

fn mutation_id(value: &CanonicalValue, at: &str) -> Result<MutationId, String> {
    MutationId::parse(&string(value, at)?).map_err(|error| invalid_id(at, error))
}

fn revision_id(value: &CanonicalValue, at: &str) -> Result<RevisionId, String> {
    RevisionId::parse(&string(value, at)?).map_err(|error| invalid_id(at, error))
}

fn map<'a>(value: &'a CanonicalValue, at: &str) -> Result<&'a [(String, CanonicalValue)], String> {
    match value {
        CanonicalValue::Map(fields) => Ok(fields),
        _ => Err(format!("{at} is not a record")),
    }
}

fn list<'a>(value: &'a CanonicalValue, at: &str) -> Result<&'a [CanonicalValue], String> {
    match value {
        CanonicalValue::List(items) => Ok(items),
        _ => Err(format!("{at} is not a list")),
    }
}

fn string(value: &CanonicalValue, at: &str) -> Result<String, String> {
    match value {
        CanonicalValue::String(text) => Ok(text.clone()),
        _ => Err(format!("{at} is not a string")),
    }
}

fn unsigned(value: &CanonicalValue, at: &str) -> Result<u64, String> {
    match value {
        CanonicalValue::Integer(number) => {
            u64::try_from(*number).map_err(|_| format!("{at} is out of range"))
        }
        _ => Err(format!("{at} is not an integer")),
    }
}

fn digest(value: &CanonicalValue, at: &str) -> Result<Checksum, String> {
    match value {
        CanonicalValue::Bytes(bytes) => <[u8; 32]>::try_from(bytes.as_slice())
            .map(Checksum::from_bytes)
            .map_err(|_| format!("{at} is not a 32-byte digest")),
        _ => Err(format!("{at} is not a digest")),
    }
}

fn field<'a>(
    fields: &'a [(String, CanonicalValue)],
    key: &str,
) -> Result<&'a CanonicalValue, String> {
    fields
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value)
        .ok_or_else(|| format!("field `{key}` is missing"))
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// The signing material the tests use. Length matters (the constructor
    /// refuses short keys); the value does not.
    pub(crate) const KEY: &[u8] = b"last-known-good-test-signing-key-32b";

    /// A unique path under the process temp directory, so parallel tests never
    /// share a cache file.
    pub(crate) fn cache_path(name: &str) -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "axond-lkg-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::testing::{KEY, cache_path};
    use super::*;
    use crate::desired_state::ExpectedRevision;
    use crate::desired_state::fixtures;

    fn revision(seed: u64, state: DesiredState) -> LoadedRevision {
        let candidate = fixtures::candidate(ExpectedRevision::Empty, "cached", state);
        let manifest = RevisionManifest::of(
            fixtures::revision_id(seed),
            Some(fixtures::revision_id(seed - 1)),
            SystemTime::UNIX_EPOCH + Duration::from_nanos(1_234_567_891),
            &candidate,
        )
        .expect("a valid manifest");
        LoadedRevision::assemble(manifest, candidate.state).expect("a consistent revision")
    }

    fn cache(name: &str) -> LastKnownGood {
        LastKnownGood::new(cache_path(name), KEY).expect("a long enough key")
    }

    /// The property a cold boot depends on: what comes back is the revision that
    /// went in, field for field, including the blob-backed body and the
    /// dependency edges.
    #[test]
    fn an_exported_revision_is_restored_exactly() {
        let cache = cache("round-trip");
        let revision = revision(9, fixtures::state());
        cache.export(&revision).expect("export succeeds");
        let restored = cache.load().expect("the cache reads back");
        assert_eq!(restored.as_ref(), Some(&revision));
        // And the restored copy is not merely equal: it re-passed assembly, so its
        // checksums were recomputed rather than trusted.
        assert_eq!(
            restored.expect("restored").manifest().checksum,
            revision.manifest().checksum
        );
        let _ = fs::remove_file(cache.path());
    }

    /// A rollback that reuses the volume: the cache is authentic, and this build
    /// still cannot read the revision it holds. The refusal says which of the two
    /// it is, because repairing a cache and rolling a replica forward are
    /// different actions.
    #[test]
    fn a_cache_written_by_a_newer_build_is_an_incompatibility_not_damage() {
        let cache = cache("newer-build");
        let readable = revision(9, fixtures::state());
        cache
            .export_unassembled(readable.manifest(), &fixtures::state_with_legacy_tenant())
            .expect("export succeeds");

        let error = cache.load().expect_err("this build does not read it");
        let LastKnownGoodError::Integrity(integrity) = error else {
            panic!("an authentic record that does not assemble is an integrity failure: {error}");
        };
        assert!(
            integrity.is_incompatible(),
            "a body this build cannot read is a version skew, not damage: {integrity}"
        );
        let _ = fs::remove_file(cache.path());
    }

    #[test]
    fn cached_id_refusals_keep_field_context_without_echoing_material() {
        const MATERIAL: &str = "sk-live-provider-material";
        let bad = CanonicalValue::string(MATERIAL);
        let valid_tenant = fixtures::tenant_id(1).to_string();
        let valid_project = fixtures::project_id(2).to_string();

        let reference = CanonicalValue::map([
            (
                "kind",
                CanonicalValue::string(ResourceKind::Tenant.as_str()),
            ),
            ("id", bad.clone()),
            ("version", CanonicalValue::Integer(1)),
        ]);
        let tenant_scope = CanonicalValue::map([
            ("kind", CanonicalValue::string("tenant")),
            ("tenant", bad.clone()),
        ]);
        let project_scope_with_bad_tenant = CanonicalValue::map([
            ("kind", CanonicalValue::string("project")),
            ("tenant", bad.clone()),
            ("project", CanonicalValue::string(&valid_project)),
        ]);
        let project_scope_with_bad_project = CanonicalValue::map([
            ("kind", CanonicalValue::string("project")),
            ("tenant", CanonicalValue::string(&valid_tenant)),
            ("project", bad.clone()),
        ]);

        let errors = [
            revision_id(&bad, "manifest.id").expect_err("the cache is corrupt"),
            mutation_id(&bad, "manifest.mutation").expect_err("the cache is corrupt"),
            decode_reference(&reference).expect_err("the cache is corrupt"),
            decode_scope(&tenant_scope).expect_err("the cache is corrupt"),
            decode_scope(&project_scope_with_bad_tenant).expect_err("the cache is corrupt"),
            decode_scope(&project_scope_with_bad_project).expect_err("the cache is corrupt"),
        ];
        for error in &errors {
            assert!(
                !error.contains(MATERIAL),
                "the cache refusal echoed material: {error}"
            );
        }
        assert!(errors[0].contains("manifest.id"));
        assert!(errors[1].contains("manifest.mutation"));
        assert!(errors[2].contains("reference.id"));
        assert!(errors[3].contains("scope.tenant"));
        assert!(errors[4].contains("scope.tenant"));
        assert!(errors[5].contains("scope.project"));
    }

    #[test]
    fn a_replica_that_has_never_exported_has_no_cached_revision() {
        assert!(
            cache("absent")
                .load()
                .expect("an absent cache is not an error")
                .is_none()
        );
    }

    /// Exporting twice leaves exactly one file holding the newer revision, which
    /// is what makes the cache "last known good" rather than "first known good".
    #[test]
    fn a_second_export_replaces_the_first() {
        let cache = cache("replace");
        cache
            .export(&revision(9, fixtures::state()))
            .expect("first export");
        let newer = revision(11, fixtures::state_with_renamed_alias());
        cache.export(&newer).expect("second export");
        assert_eq!(cache.load().expect("reads back"), Some(newer));
        let _ = fs::remove_file(cache.path());
    }

    /// The reason the cache is signed: an operator (or anything else with write
    /// access) editing desired state on disk must not be able to make a replica
    /// serve it.
    #[test]
    fn an_edited_cache_is_refused_rather_than_served() {
        let cache = cache("tampered");
        cache
            .export(&revision(9, fixtures::state()))
            .expect("export succeeds");
        let mut bytes = fs::read(cache.path()).expect("the cache exists");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        fs::write(cache.path(), &bytes).expect("rewrite");
        let error = cache.load().expect_err("an edited cache is not authentic");
        assert!(
            matches!(error, LastKnownGoodError::Signature { .. }),
            "{error}"
        );
        let _ = fs::remove_file(cache.path());
    }

    /// Signing material is per deployment: a cache from another deployment is not
    /// readable here, so a copied volume cannot inject another deployment's state.
    #[test]
    fn a_cache_written_with_other_material_is_not_readable() {
        let path = cache_path("other-key");
        let writer = LastKnownGood::new(&path, KEY).expect("a long enough key");
        writer
            .export(&revision(9, fixtures::state()))
            .expect("export succeeds");
        let reader = LastKnownGood::new(&path, b"a-different-but-long-enough-key-32b")
            .expect("a long enough key");
        assert!(matches!(
            reader.load(),
            Err(LastKnownGoodError::Signature { .. })
        ));
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn a_truncated_cache_is_refused_before_anything_is_parsed() {
        let cache = cache("truncated");
        cache
            .export(&revision(9, fixtures::state()))
            .expect("export succeeds");
        let bytes = fs::read(cache.path()).expect("the cache exists");
        fs::write(cache.path(), &bytes[..bytes.len() / 2]).expect("truncate");
        assert!(matches!(
            cache.load(),
            Err(LastKnownGoodError::Signature { .. })
        ));
        let _ = fs::remove_file(cache.path());
    }

    #[test]
    fn a_cache_from_an_unsupported_layout_names_its_version() {
        let path = cache_path("version");
        let mut bytes = MAGIC.to_vec();
        bytes.push(RECORD_VERSION + 1);
        bytes.extend_from_slice(&[0u8; 32]);
        fs::write(&path, &bytes).expect("write");
        let cache = LastKnownGood::new(&path, KEY).expect("a long enough key");
        assert!(matches!(
            cache.load(),
            Err(LastKnownGoodError::Version { found, .. }) if found == RECORD_VERSION + 1
        ));
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn a_file_that_is_not_a_cache_is_refused_by_its_marker() {
        let path = cache_path("foreign");
        fs::write(&path, b"{\"desired\":\"state\"}").expect("write");
        let cache = LastKnownGood::new(&path, KEY).expect("a long enough key");
        let error = cache.load().expect_err("a foreign file is not a cache");
        assert!(
            matches!(error, LastKnownGoodError::Malformed { ref detail, .. } if detail.contains("marker")),
            "{error}"
        );
        let _ = fs::remove_file(&path);
    }

    /// Short signing material would make the MAC decorative, so it is refused at
    /// construction rather than accepted and reported later.
    #[test]
    fn short_signing_material_is_refused_at_construction() {
        let error = LastKnownGood::new(cache_path("short"), b"too-short").expect_err("refused");
        assert!(
            matches!(error, LastKnownGoodError::KeyTooShort { bytes } if bytes == 9),
            "{error}"
        );
    }

    #[test]
    fn the_deployment_key_is_canonical_base64_of_exactly_256_bits() {
        let encoded = STANDARD.encode([7u8; ENCODED_KEY_BYTES]);
        let cache = LastKnownGood::from_base64(cache_path("encoded"), &encoded)
            .expect("a canonical 256-bit key is accepted");
        assert!(cache.path().to_string_lossy().contains("encoded"));

        let short = STANDARD.encode([7u8; ENCODED_KEY_BYTES / 2]);
        assert!(matches!(
            LastKnownGood::from_base64(cache_path("short-encoded"), &short),
            Err(LastKnownGoodError::KeyWrongLength { bytes }) if bytes == ENCODED_KEY_BYTES / 2
        ));
    }

    #[test]
    fn the_deployment_key_rejects_whitespace_and_noncanonical_encoding() {
        let encoded = STANDARD.encode([9u8; ENCODED_KEY_BYTES]);
        assert!(matches!(
            LastKnownGood::from_base64(cache_path("leading-space"), &format!(" {encoded}")),
            Err(LastKnownGoodError::KeyWhitespace)
        ));
        assert!(matches!(
            LastKnownGood::from_base64(cache_path("trailing-newline"), &format!("{encoded}\n")),
            Err(LastKnownGoodError::KeyWhitespace)
        ));
        let unpadded = encoded.trim_end_matches('=');
        assert!(matches!(
            LastKnownGood::from_base64(cache_path("unpadded"), unpadded),
            Err(LastKnownGoodError::KeyEncoding)
        ));
    }
}
