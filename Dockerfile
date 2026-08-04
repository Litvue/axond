# Static musl build → distroless. Produces a self-contained single binary with
# no libc or shell in the final image (delta: static packaging from day one).

FROM rust:1.97 AS build
RUN rustup target add x86_64-unknown-linux-musl \
    && apt-get update && apt-get install -y --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --target x86_64-unknown-linux-musl -p axond

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /src/target/x86_64-unknown-linux-musl/release/axond /axond
EXPOSE 8080
ENTRYPOINT ["/axond"]
