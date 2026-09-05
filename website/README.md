# Axond website

Static Astro 7 pages with plain CSS. No React, server runtime, or hosting adapter.

## Local development

```sh
npm ci
npm run dev -- --host 127.0.0.1 --port 3000
```

## Validate and build

```sh
npm run check
npm run build
npm run preview
```

The `dist/` directory can be served by any static host. Code tabs and copy buttons
use a small browser script. Installer URLs point to `https://axond.dev`.

`predev` and `prebuild` copy the canonical root `install.sh` and `install.ps1` into
`public/`. Edit the root scripts, not the generated copies. Build from within the
repository. `/quickstart` uses `/axond.toml` for local SQLite setup.

Typography uses locally hosted Inter and JetBrains Mono; their SIL Open Font
License files are in `public/fonts/`.

## Deployment

Cloudflare Workers Static Assets serves `dist/` at **https://axond.dev**.
`wrangler.jsonc` owns the domain binding; no Worker script or Astro adapter is used.

```sh
npm run check
npm run build
npm run deploy
```

Authenticate locally with `npx wrangler login`. GitHub Actions checks the build;
Cloudflare Workers Builds handles production deployment through its GitHub app.
No Cloudflare credentials are stored in GitHub Actions.

Connect the existing `axond-website` Worker to `Litvue/axond` in Cloudflare:

- Production branch: `main`
- Root directory: `website`
- Build command: `npm run check && npm run build`
- Deploy command: `npx wrangler deploy`
- Node version: `24` (set `NODE_VERSION` in build variables)
- Build watch paths: `website/**`, `install.sh`, `install.ps1`

Keep the full repository checkout: `prebuild` copies the root installers.
