# TypeScript gateway image. The Rust Dockerfile remains the Rust release artifact.
FROM node:22.14-bookworm-slim AS build
WORKDIR /app
COPY package.json package-lock.json ./
COPY packages ./packages
COPY bin ./bin
RUN npm ci --omit=dev
COPY --from=oven/bun:1.4.4 /usr/local/bin/bun /usr/local/bin/bun
RUN mkdir -p /out/data && bun build packages/cli/src/main.ts --compile --outfile /out/axond

FROM gcr.io/distroless/base-debian12:nonroot
ENV NODE_ENV=production
ENV AXOND_CONFIG=/etc/axond/axond.toml
COPY --from=build /out/axond /axond
COPY --from=build --chown=65532:65532 /out/data /var/lib/axond
WORKDIR /var/lib/axond
USER nonroot
EXPOSE 8080
ENTRYPOINT ["/axond"]
