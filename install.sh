#!/bin/sh
# Install the latest prebuilt Axond binary for supported Unix platforms.
set -eu

repo="Litvue/axond"
version="${AXOND_VERSION:-}"
target="${AXOND_TARGET:-}"
dry_run="${AXOND_INSTALL_DRY_RUN:-0}"
require_attestation="${AXOND_REQUIRE_ATTESTATION:-0}"

fail() {
  printf 'axond installer: %s\n' "$*" >&2
  exit 1
}

command -v curl >/dev/null 2>&1 || fail "curl is required"

case "$require_attestation" in
  1|true|TRUE|yes|YES|on|ON)
    require_attestation=1
    ;;
  0|false|FALSE|no|NO|off|OFF|'')
    require_attestation=0
    ;;
  *)
    fail "AXOND_REQUIRE_ATTESTATION must be 1/0, true/false, yes/no, or on/off"
    ;;
esac

if [ -z "$version" ]; then
  latest_url="$(curl --proto '=https' --tlsv1.2 -fLsS -o /dev/null \
    -w '%{url_effective}' "https://github.com/${repo}/releases/latest")"
  version="${latest_url##*/}"
  version="${version#v}"
fi

printf '%s\n' "$version" | grep -Eq \
  '^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$' || \
  fail "invalid version: $version"

if [ -z "$target" ]; then
  os="$(uname -s)"
  arch="$(uname -m)"
  case "$os/$arch" in
    Linux/x86_64|Linux/amd64)
      target="x86_64-unknown-linux-musl"
      ;;
    Darwin/arm64|Darwin/aarch64)
      target="aarch64-apple-darwin"
      ;;
    *)
      fail "no prebuilt binary for $os/$arch; see https://github.com/${repo}/releases"
      ;;
  esac
fi

case "$target" in
  x86_64-unknown-linux-gnu|x86_64-unknown-linux-musl|aarch64-apple-darwin)
    ;;
  *)
    fail "unsupported prebuilt target: $target"
    ;;
esac

if [ -z "${AXOND_INSTALL_DIR:-}" ]; then
  [ -n "${HOME:-}" ] || fail "HOME is unset; set AXOND_INSTALL_DIR explicitly"
  install_dir="$HOME/.local/bin"
else
  install_dir="$AXOND_INSTALL_DIR"
fi

asset="axond-${version}-${target}.tar.gz"
base_url="https://github.com/${repo}/releases/download/v${version}"

if [ "$dry_run" = "1" ]; then
  printf 'version=%s\ntarget=%s\nasset=%s\ninstall_dir=%s\nrequire_attestation=%s\nurl=%s/%s\n' \
    "$version" "$target" "$asset" "$install_dir" "$require_attestation" \
    "$base_url" "$asset"
  exit 0
fi

command -v tar >/dev/null 2>&1 || fail "tar is required"
command -v install >/dev/null 2>&1 || fail "install is required"

temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/axond-install.XXXXXX")"
cleanup() {
  rm -rf "$temp_dir"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

curl --proto '=https' --tlsv1.2 -fLsS \
  -o "$temp_dir/$asset" "$base_url/$asset"
curl --proto '=https' --tlsv1.2 -fLsS \
  -o "$temp_dir/$asset.sha256" "$base_url/$asset.sha256"

if command -v sha256sum >/dev/null 2>&1; then
  (cd "$temp_dir" && sha256sum -c "$asset.sha256")
elif command -v shasum >/dev/null 2>&1; then
  (cd "$temp_dir" && shasum -a 256 -c "$asset.sha256")
else
  fail "sha256sum or shasum is required to verify the release"
fi

if [ "$require_attestation" = "1" ]; then
  command -v gh >/dev/null 2>&1 || \
    fail "GitHub CLI is required by AXOND_REQUIRE_ATTESTATION=1"
  gh auth status >/dev/null 2>&1 || \
    fail "authenticated GitHub CLI is required by AXOND_REQUIRE_ATTESTATION=1"
  gh attestation --help >/dev/null 2>&1 || \
    fail "GitHub CLI with attestation support is required by AXOND_REQUIRE_ATTESTATION=1"
  printf 'verifying GitHub build provenance for %s\n' "$asset"
  gh attestation verify "$temp_dir/$asset" --repo "$repo"
else
  printf '%s\n' \
    "axond installer: checksum verified; set AXOND_REQUIRE_ATTESTATION=1 to verify GitHub build provenance" >&2
fi

tar -xzf "$temp_dir/$asset" -C "$temp_dir"
[ -f "$temp_dir/axond" ] || fail "release archive did not contain axond"
mkdir -p "$install_dir"
install -m 0755 "$temp_dir/axond" "$install_dir/axond"

printf 'installed axond %s to %s/axond\n' "$version" "$install_dir"
case ":${PATH:-}:" in
  *:"$install_dir":*)
    ;;
  *)
    printf 'add %s to PATH to run axond directly\n' "$install_dir"
    ;;
esac
