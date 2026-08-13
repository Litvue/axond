# TypeScript provider-SDK compatibility

The vendors' own **Node** SDKs — [`openai`](https://www.npmjs.com/package/openai)
and [`@anthropic-ai/sdk`](https://www.npmjs.com/package/@anthropic-ai/sdk) —
driving a real `axond` process against the committed fixtures in
[`../fixtures`](../fixtures). The sibling of [`../compat`](../compat), which does
the same with the Python SDKs, and of `crates/gateway/tests`, which does it with
a raw HTTP client ([ADR 0014](../../docs/adr/0014-compatibility-and-soak-harness.md)).

Offline by construction: no provider account, no network call to a provider, no
real secret. The fake upstream is a loopback `node:http` server serving the same
fixture bytes the other two harnesses serve, and the gateway key and provider
keys are literals generated into a temporary config.

```bash
just compat-ts    # build the gateway, install the locked SDKs, run the lane
```

Or by hand, from this directory, with a built `axond` on the path `AXOND_BIN`
names (the debug build by default):

```bash
npm ci --ignore-scripts
npm test
```

## What it asserts

Beyond the wire round-trip on each supported route, two things a unit test
cannot see:

- **The provider credential, not the caller's.** Every assertion on an upstream
  request checks the injected `Authorization` / `x-api-key` is the *provider*
  key. The gateway key the SDK authenticated with must not appear upstream.
- **The SDK's types.** The lane is compiled with `tsc --strict` before it runs,
  so a shape the SDK's own type definitions no longer describe fails the build
  rather than passing as `any`. `skipLibCheck` is deliberately off: the vendors'
  shipped declarations are *checked*, not trusted, because a release whose own
  `.d.ts` no longer type-checks — or which references an optional peer nobody
  installed — is a compatibility fact about that release, which is exactly what
  this lane exists to notice. The cost is that an SDK bump can fail here for a
  declaration reason rather than a wire one; the fix is to satisfy the
  declaration (installing a peer at an exact version, say), not to stop looking.
  A compiler no longer enforcing any of this would leave every test green, so
  `negative/` holds code that must *not* compile and `src/typecheck.test.ts`
  requires it to fail.
- **Its own failure paths.** `harness.test.ts` boots stub gateways that are
  missing, refuse to launch, crash, or accept the readiness probe without
  answering it, and asserts each fails promptly and leaves nothing listening. A
  broken gateway must report as a failed test, never as a CI job that hangs until
  the workflow times out.

## Pins

Everything is exact — the SDK versions, the toolchain, and the Node runtime in
[`.nvmrc`](./.nvmrc), which is what CI's `setup-node` reads. `npm ci` installs
from [`package-lock.json`](./package-lock.json), so each package is fetched by
integrity hash. `ops/compat-ts-pins.py` enforces that in CI: a floating range, a
lockfile that disagrees with `package.json`, an entry without an integrity hash,
or a Node pin outside `engines.node` fails the lane.

Bumps are deliberate. When a new SDK release breaks the gateway, the bump commit
is the report:

```bash
cd tests/compat-ts
npm install --ignore-scripts --save-exact openai@<version> @anthropic-ai/sdk@<version>
npm test
```

Prefer a release at least a week old, the same rule the Python lockfile follows.

### When a bump fails, read which half failed

Because the SDKs' declarations are checked rather than trusted, a bump can fail
before any request is made. `npm run build` says which it was, so a declaration
problem is not mistaken for a wire regression:

```text
compat-ts: the failure is in shipped type declarations, not in axond's wire
behaviour — no request was made. Declarations at fault:
  node_modules/openai/src/foo.d.ts
```

That is a fact about the SDK release. The usual cause is an optional peer the SDK
now references from its public types (`zod`, say) that this lane does not install;
the fix is to install that peer at an exact version, not to turn on
`skipLibCheck`. The other wording names the opposite case:

```text
compat-ts: a call this lane makes is rejected by the SDKs' own types.
```

There the claim under test really did move — either the gateway's shape or the
SDK's description of it — and the tests below the build are the ones to read.

## Scope

Go is deliberately **not** covered: the Go SDKs' surface for these routes adds no
wire coverage the two lanes here do not already have, and the case for a third
runtime is the stateful/admin API, which is not stable yet. Revisit it then.
