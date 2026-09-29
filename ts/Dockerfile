# TypeScript gateway image. The Rust Dockerfile remains the release artifact
# until a later minor makes this image the one operators pull.
FROM node:22.14-bookworm-slim
WORKDIR /app
COPY package.json package-lock.json ./
COPY packages ./packages
COPY bin ./bin
RUN npm ci --omit=dev
ENV NODE_ENV=production
EXPOSE 8080
ENTRYPOINT ["/app/bin/axond"]
