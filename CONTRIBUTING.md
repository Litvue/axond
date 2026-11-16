# Contributing to Axond

## Website and Axond CI

Changes confined to `website/` and `.github/workflows/website.yml` run the website
checks without installing gateway dependencies or starting Axond's build, test, Docker, or
Kubernetes jobs. A small change detector and workflow-policy check still report
`CI Success`, so branch protection does not wait on a missing workflow.

PRs use the complete diff from their merge base; main pushes and merge queues
use the full event range. Mixed website/Axond changes retain normal TypeScript CI.
Release tags and manual workflow runs always execute the full suite.
Unknown paths keep Axond checks enabled, and failed change detection fails CI.

Cloudflare's GitHub app deploys the website independently. Website-only changes
do not create Axond releases; see the [release runbook](docs/maintainers/releasing.md).


Thanks for your interest. Axond is early — the architecture is settling, so
opening an issue to discuss a change before a large PR is appreciated.

Found a vulnerability? Do not open an issue or a pull request — follow
[`SECURITY.md`](./SECURITY.md) and report it privately.

Changing authentication, namespace scoping, secret handling, model entitlement,
durable schema or telemetry, or the release workflows? Start from the
[threat-model review triggers](./docs/security/threat-model-review.md): they name
the regression tests your change owes, when it needs an ADR or a threat-model
update, and what to say about release impact.

## Development

Use Node 22.14.0 and npm, matching the TypeScript workflow:

```bash
cd ts
npm ci
npm test
cd ..
just check
just workflow-policy
just actionlint
```

`just check` runs TS tests, web lint, alert checks, executable documentation
samples, and npm dependency policy. `just run` starts `ts/bin/axond` against
`axond.toml`; set its referenced environment variables first.
`just quickstart-smoke` exercises Compose; the source overlay
`docker-compose.build.yml` builds `ts/Dockerfile`.

The gateway job in [typescript.yml](./.github/workflows/typescript.yml) runs
the Python and Node SDK compatibility suites against `ts/bin/axond`.
Their dependency owners are [tests/compat/requirements.in](./tests/compat/requirements.in)
and [tests/compat-ts/package.json](./tests/compat-ts/package.json).
Postgres integration tests use `AXOND_TEST_POSTGRES`; without it, service
tests skip. Request-path tests use isolated SQLite or memory stores.

Rust harnesses, fuzzing, MSRV and public Rust API gates were retired under
[ADR 0069](./docs/adr/0069-retire-the-rust-gateway.md). Parser changes still need
regression tests; TypeScript fuzz equivalents remain an open question.

A documented HTTP, config or telemetry break is a minor bump via a breaking
commit title; see [version classification](./docs/maintainers/releasing.md#version-classification).

## Conventions

- **Keep all TypeScript checks green.** Do not disable tests or dependency policy.
- **Public contract breaks are minor releases.** They are covered by
  [the compatibility contract](./docs/compatibility.md) and gated in CI; a break
  is not a patch. Contract breaks are the *only* things that make a pre-1.0 release a
  minor, and they get there through a breaking commit title — everything else,
  features included, ships as a patch. See
  [version classification](./docs/maintainers/releasing.md#version-classification).
- **One open PR per change.** Before opening a pull request, search open PRs
  for the same title and for the same `Closes` / `Fixes` issue. If one exists,
  push onto that branch. Do not open a sibling. CI fails a newer open PR that
  repeats the title or the closed issue of an older one (`ops/check-duplicate-prs.py`).
- **Squash-merge pull requests.** release-please reads one Conventional Commit
  per commit on `main`; a merge commit repeats its branch's commit in the release
  notes. See
  [duplicated changelog entries](./docs/maintainers/releasing.md#duplicated-changelog-entries).
- **Report vulnerabilities privately.** See [`SECURITY.md`](./SECURITY.md).
- **Workflow steps are pinned to commit SHAs.** Every `uses:` names a full commit
  SHA with the version in a trailing comment; a tag or branch ref fails the
  `workflow-policy` lane. Dependabot proposes the bumps — see
  [ADR 0035](./docs/adr/0035-pinned-github-actions.md) and
  [the runbook](./docs/maintainers/releasing.md#workflow-action-pins).
- **Never commit secrets.** Credentials are referenced by env-var name in
  config; real keys and `axond.toml` are gitignored.
- **Keep `gateway-core` I/O-free.** No HTTP client, no runtime, no config, no
  secrets in the core crate — that is what keeps provider-wire logic testable in
  isolation.
- **Significant decisions get an ADR** in `docs/adr` in the same PR. Start from
  the [ADR template](./docs/adr/template.md), including its required state-tier
  declaration.
- **A fired security trigger is answered in the PR body.** Which trigger, the
  tests that hold the property, whether the threat model or an ADR changed, and
  the release impact — see the
  [threat-model review triggers](./docs/security/threat-model-review.md).
  "No trigger fired" is a fine answer; an unstated one is not.

## License

By contributing, you agree that your contributions are dual-licensed under
Apache-2.0 and MIT, matching the project (inbound = outbound).
