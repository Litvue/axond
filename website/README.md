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

Authenticate locally with `npx wrangler login`. GitHub Actions checks website PRs
and deploys changes merged into `main` when website files, either root installer,
or the website workflow change. Manual workflow runs deploy only from `main`.
Configure repository secret `CLOUDFLARE_API_TOKEN` and repository variable
`CLOUDFLARE_ACCOUNT_ID` for CI. Never commit credentials.

The token needs Workers Scripts Edit for the Litvue account, and Workers Routes
Edit plus Zone Read for the axond.dev zone. Keep the checkout at repository root:
the build copies the root installers into the static output.
