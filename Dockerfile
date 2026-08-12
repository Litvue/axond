# Static musl build → distroless. Produces a self-contained single binary with
# no libc or shell in the final image (delta: static packaging from day one).
#
# The Rust target follows the requested platform, so the same Dockerfile builds
# every published architecture. The release workflow builds each platform on a
# runner of that architecture, which keeps this a native build; a plain
# `docker build` inherits the host's architecture the same way.

FROM rust:1.97 AS build
ARG TARGETARCH
RUN set -eux; \
    case "${TARGETARCH:-amd64}" in \
      amd64) rust_target=x86_64-unknown-linux-musl ;; \
      arm64) rust_target=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    echo "$rust_target" > /rust-target; \
    rustup target add "$rust_target"; \
    apt-get update; \
    apt-get install -y --no-install-recommends musl-tools; \
    rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN set -eux; \
    rust_target="$(cat /rust-target)"; \
    cargo build --release --target "$rust_target" -p axond; \
    cp "target/${rust_target}/release/axond" /axond

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /axond /axond
EXPOSE 8080
ENTRYPOINT ["/axond"]
