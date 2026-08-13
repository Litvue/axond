# Installation and verification

Axond ships through crates.io, signed GitHub release archives, and GHCR. Use a
released artifact in production; build from source when developing Axond.

## Prebuilt binary installer

The installer is the shortest path to the released single binary. It detects
Linux x86-64, Linux ARM64, or macOS Apple Silicon, downloads the matching GitHub
Release archive, verifies the published SHA-256 sidecar, and installs into
`$HOME/.local/bin` by default:

```bash
curl --proto '=https' --tlsv1.2 -LsSf \
  https://raw.githubusercontent.com/Litvue/axond/main/install.sh | sh
```

For Windows x86-64 in PowerShell:

```powershell
irm https://raw.githubusercontent.com/Litvue/axond/main/install.ps1 | iex
```

For environments that prohibit piping downloaded code into a shell, download
and inspect `install.sh` or `install.ps1` first. Both support an explicit
version and destination:

```bash
AXOND_VERSION=0.3.36 AXOND_INSTALL_DIR=/usr/local/bin sh ./install.sh # x-release-please-version
```

```powershell
.\install.ps1 -Version 0.3.36 -InstallDir C:\Tools\axond # x-release-please-version
```

Supported installer targets match the release matrix: Linux x86-64 and Linux
ARM64 (static musl by default, glibc selectable through `AXOND_TARGET`), macOS
Apple Silicon, and Windows x86-64. Other architectures must build from source
until a release artifact is added; the full matrix is in
[the compatibility contract](./compatibility.md#supported-platforms).

A same-origin checksum detects corruption but is not independent proof of
provenance. By default, both installers verify that checksum and do not depend
on GitHub CLI or API availability. Production automation can opt into the
stronger `gh attestation verify` check against `Litvue/axond`:

```bash
AXOND_REQUIRE_ATTESTATION=1 sh ./install.sh
```

```powershell
.\install.ps1 -RequireAttestation
```

## crates.io source install

Cargo registries distribute source packages, not precompiled executables.
Consequently, `cargo install` downloads the published crate and compiles it on
the local machine:

```bash
cargo install axond --locked
axond --help
```

Pin a deployment to an explicit version when reproducibility matters:

```bash
AXOND_VERSION=0.3.36 # x-release-please-version
cargo install axond --version "$AXOND_VERSION" --locked
```

The published workspace libraries are also available to external Rust
consumers:

```bash
cargo add gateway-core gateway-transport
```

`gateway-core` contains runtime-neutral provider adapters and routing
primitives. `gateway-transport` adds HTTP dispatch, credential injection,
timeouts, retries, streaming, and tracing integration.

## Prebuilt release binary

Release archives are published for:

| Target | Artifact |
| --- | --- |
| Linux x86_64, glibc | `x86_64-unknown-linux-gnu.tar.gz` |
| Linux x86_64, static musl | `x86_64-unknown-linux-musl.tar.gz` |
| Linux aarch64, glibc | `aarch64-unknown-linux-gnu.tar.gz` |
| Linux aarch64, static musl | `aarch64-unknown-linux-musl.tar.gz` |
| macOS Apple Silicon | `aarch64-apple-darwin.tar.gz` |
| Windows x86_64 | `x86_64-pc-windows-msvc.zip` |

Download and verify an archive:

```bash
tag=v0.3.36 # x-release-please-version
version="${tag#v}"
target=x86_64-unknown-linux-musl

gh release download "$tag" --repo Litvue/axond \
  --pattern "axond-${version}-${target}.tar.gz*"
sha256sum -c "axond-${version}-${target}.tar.gz.sha256"
gh attestation verify "axond-${version}-${target}.tar.gz" \
  --repo Litvue/axond
tar -xzf "axond-${version}-${target}.tar.gz"
./axond --help
```

The musl binary is static PIE and is the simplest Linux server installation, on
either architecture.

## OCI image

The public image is distroless, runs as non-root, contains no shell or package
manager, and is published as a multi-architecture index covering `linux/amd64`
and `linux/arm64`. There is no `latest` tag.

Both platform children are addressable directly as `:<version>-amd64` and
`:<version>-arm64`; an ARM host pulling the index resolves its native child
without emulation ([architecture
selection](./deployment/docker-compose.md#architecture-selection)).

```bash
AXOND_VERSION=0.3.36 # x-release-please-version
image="ghcr.io/litvue/axond:${AXOND_VERSION}"
docker pull "$image"
digest="$(docker buildx imagetools inspect "$image" | \
  awk '$1 == "Digest:" { print $2 }')"
test -n "$digest"
```

That digest is the index: pulling it resolves to the child image for the host's
architecture, so one pinned digest serves a mixed-architecture fleet. List the
platforms it contains, or pin a single platform explicitly:

```bash
docker buildx imagetools inspect "ghcr.io/litvue/axond@${digest}" \
  --format '{{range .Manifest.Manifests}}{{.Platform.OS}}/{{.Platform.Architecture}} {{.Digest}}
{{end}}'

# Single-platform tags remain published alongside the index.
docker pull "ghcr.io/litvue/axond:${AXOND_VERSION}-arm64"
```

Verify the keyless signature and GitHub provenance:

```bash
cosign verify \
  --certificate-identity-regexp '^https://github\.com/Litvue/axond/\.github/workflows/release-please\.yml@(refs/heads/main|refs/tags/v[0-9]+\.[0-9]+\.[0-9]+)$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  "ghcr.io/litvue/axond@${digest}"

gh attestation verify "oci://ghcr.io/litvue/axond@${digest}" \
  --repo Litvue/axond \
  --predicate-type https://slsa.dev/provenance/v1
```

Those two commands cover the index digest: it is signed and carries provenance.
SBOM attestations are on the single-platform children, so to verify the SBOM of
what you actually run, resolve your platform's child digest and verify that:

```bash
child="$(docker buildx imagetools inspect "ghcr.io/litvue/axond@${digest}" \
  --format '{{json .Manifest}}' |
  jq -r '.manifests[] | select(.platform.architecture == "arm64") | .digest')"

gh attestation verify "oci://ghcr.io/litvue/axond@${child}" \
  --repo Litvue/axond \
  --predicate-type https://spdx.dev/Document/v2.3
```

Deploy `ghcr.io/litvue/axond@${digest}`, not a mutable tag. The release page
carries `axond-image-<version>.digest` (the index), the per-architecture
`axond-image-<version>-amd64.digest` and `-arm64.digest`, and one SPDX SBOM per
architecture.

### Older releases

Linux ARM64 archives and the multi-architecture image start with 0.3.18;
releases up to and including 0.3.17 carry the x86-64 Linux, macOS, and Windows
artifacts and a single `linux/amd64` image only. Asking one of those releases for
an ARM64 archive fails with an explicit message naming the release and target
rather than a transfer error, and pinning one in the Compose quickstart needs an
explicit `AXOND_PLATFORM=linux/amd64` for an ARM host to run it under emulation
rather than fail to pull — see [architecture
selection](./deployment/docker-compose.md#architecture-selection).

## Build from source

The toolchain is pinned by `rust-toolchain.toml`:

```bash
git clone https://github.com/Litvue/axond.git
cd axond
cargo build --release --locked -p axond
```

For the static Linux build, install `musl-tools`, add the musl target to the
*pinned* toolchain, and run `just build-static`. See
[CONTRIBUTING.md](../CONTRIBUTING.md) for the complete local verification gate.
