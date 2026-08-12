#!/usr/bin/env bash
# Prove that ops/publish-image-index.sh cannot publish an operator-facing tag it
# would then reject.
#
# A registry tag cannot be withdrawn, so the ordering inside that script is the
# whole guarantee: in promotion mode every check must run before the first
# `imagetools create`. That is invisible to a linter and only reachable at a real
# tag, so this drives the script against a stub `docker` that records every
# invocation and serves crafted manifests. Each case asserts both the failure and
# that no tag was applied.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
script="$PWD/ops/publish-image-index.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

amd64_child="sha256:$(printf 'a%.0s' {1..64})"
arm64_child="sha256:$(printf 'b%.0s' {1..64})"
other_child="sha256:$(printf 'c%.0s' {1..64})"
index_digest="sha256:$(printf 'd%.0s' {1..64})"

mkdir -p "$work/bin"
cat > "$work/bin/docker" <<'STUB'
#!/usr/bin/env bash
# Stubbed `docker buildx imagetools`. Every call is appended to $CALL_LOG; the
# manifests come from $FIXTURES, keyed by the reference being inspected.
set -euo pipefail
printf '%s\n' "$*" >> "$CALL_LOG"
[[ "${1:-}" == buildx && "${2:-}" == imagetools ]] || exit 0
case "${3:-}" in
  inspect)
    shift 3
    raw=0
    if [[ "${1:-}" == --raw ]]; then
      raw=1
      shift
    fi
    ref="$1"
    key="${ref##*[:@]}"
    if [[ "$raw" == 1 ]]; then
      file="$FIXTURES/raw-$key.json"
    else
      file="$FIXTURES/descriptor-$key.json"
    fi
    if [[ ! -f "$file" ]]; then
      echo "stub: no fixture for $ref ($file)" >&2
      exit 1
    fi
    cat "$file"
    ;;
  create) ;;
  *) ;;
esac
STUB
chmod +x "$work/bin/docker"

fixtures="$work/fixtures"
mkdir -p "$fixtures"
manifest_media=application/vnd.oci.image.manifest.v1+json
printf '{"mediaType":"%s","digest":"%s"}\n' "$manifest_media" "$amd64_child" \
  > "$fixtures/descriptor-9.9.9-amd64.json"
printf '{"mediaType":"%s","digest":"%s"}\n' "$manifest_media" "$arm64_child" \
  > "$fixtures/descriptor-9.9.9-arm64.json"

write_index() {
  # $1 is the fixture key (the digest without its algorithm-independent prefix
  # handling: the stub keys on the text after `:` or `@`), $2.. are the child
  # digests to list as linux/amd64 and linux/arm64.
  local key="$1" amd64="$2" arm64="$3"
  cat > "$fixtures/raw-${key##*:}.json" <<JSON
{
  "mediaType": "application/vnd.oci.image.index.v1+json",
  "manifests": [
    {"digest": "$amd64", "platform": {"os": "linux", "architecture": "amd64"}},
    {"digest": "$arm64", "platform": {"os": "linux", "architecture": "arm64"}}
  ]
}
JSON
}

run_promotion() {
  # Runs the script in promotion mode and prints its exit status; the caller
  # inspects the log.
  local status=0
  CALL_LOG="$work/calls.log" FIXTURES="$fixtures" \
  PATH="$work/bin:$PATH" \
  IMAGE_NAME=ghcr.io/litvue/axond \
  RELEASE_VERSION=9.9.9 \
  RELEASE_SHORT_SHA=abcdef1 \
  RELEASE_COMMIT_SHA=abcdef1234567890 \
  GITHUB_REPOSITORY=litvue/axond \
  INDEX_TAGS="9.9.9 sha-abcdef1" \
  EXPECT_INDEX_DIGEST="$index_digest" \
    bash "$script" > "$work/out.txt" 2> "$work/err.txt" || status=$?
  echo "$status"
}

assert_no_tag_applied() {
  if grep -q "imagetools create" "$work/calls.log"; then
    echo "FAIL: $1 applied a tag before failing:" >&2
    grep "imagetools create" "$work/calls.log" >&2
    exit 1
  fi
}

# 1. A child tag moved since staging: the smoked index no longer matches the
#    children the release resolves now, so promotion must refuse *before*
#    `<version>` is applied.
: > "$work/calls.log"
write_index "$index_digest" "$other_child" "$arm64_child"
status="$(run_promotion)"
[[ "$status" != 0 ]] || {
  echo "FAIL: a moved child tag did not fail the promotion" >&2
  exit 1
}
grep -q "index platforms do not match the release matrix" "$work/err.txt" || {
  echo "FAIL: the mismatch was not reported:" >&2
  cat "$work/err.txt" >&2
  exit 1
}
assert_no_tag_applied "a moved child tag"
echo "index promotion check: a child that moved since staging fails before any tag is applied"

# 2. An unexpected descriptor inside the smoked index: same requirement.
: > "$work/calls.log"
cat > "$fixtures/raw-${index_digest##*:}.json" <<JSON
{
  "mediaType": "application/vnd.oci.image.index.v1+json",
  "manifests": [
    {"digest": "$amd64_child", "platform": {"os": "linux", "architecture": "amd64"}},
    {"digest": "$arm64_child", "platform": {"os": "linux", "architecture": "arm64"}},
    {"digest": "$other_child", "platform": {"os": "unknown", "architecture": "unknown"}}
  ]
}
JSON
status="$(run_promotion)"
[[ "$status" != 0 ]] || {
  echo "FAIL: an unclassified descriptor did not fail the promotion" >&2
  exit 1
}
grep -q "is not an attestation manifest" "$work/err.txt" || {
  echo "FAIL: the descriptor was not rejected:" >&2
  cat "$work/err.txt" >&2
  exit 1
}
assert_no_tag_applied "an unclassified descriptor"
echo "index promotion check: an unexpected descriptor fails before any tag is applied"

# 3. The index is not an index at all.
: > "$work/calls.log"
printf '{"mediaType":"%s"}\n' "$manifest_media" \
  > "$fixtures/raw-${index_digest##*:}.json"
status="$(run_promotion)"
[[ "$status" != 0 ]] || {
  echo "FAIL: a single-platform manifest did not fail the promotion" >&2
  exit 1
}
grep -q "not a multi-architecture index" "$work/err.txt" || {
  echo "FAIL: the media type was not rejected:" >&2
  cat "$work/err.txt" >&2
  exit 1
}
assert_no_tag_applied "a single-platform manifest"
echo "index promotion check: a non-index digest fails before any tag is applied"

# 4. The happy path: the smoked index is retagged *from its own digest*, never
#    reassembled from the child references, so the tags cannot end up on a
#    different index than the one that booted.
: > "$work/calls.log"
write_index "$index_digest" "$amd64_child" "$arm64_child"
printf '{"mediaType":"application/vnd.oci.image.index.v1+json","digest":"%s"}\n' \
  "$index_digest" > "$fixtures/descriptor-9.9.9.json"
cp "$fixtures/descriptor-9.9.9.json" "$fixtures/descriptor-sha-abcdef1.json"
status="$(run_promotion)"
[[ "$status" == 0 ]] || {
  echo "FAIL: a valid promotion failed:" >&2
  cat "$work/err.txt" >&2
  exit 1
}
create="$(grep "imagetools create" "$work/calls.log")"
grep -qF -e "--tag ghcr.io/litvue/axond:9.9.9" <<<"$create" || {
  echo "FAIL: the version tag was not applied: $create" >&2
  exit 1
}
grep -qF -e "ghcr.io/litvue/axond@$index_digest" <<<"$create" || {
  echo "FAIL: the promotion did not retag the smoked digest: $create" >&2
  exit 1
}
if grep -qF -e "$amd64_child" <<<"$create"; then
  echo "FAIL: the promotion reassembled the index from child references: $create" >&2
  exit 1
fi
echo "index promotion check: a valid promotion retags the smoked digest itself"

echo "index promotion checks passed"
