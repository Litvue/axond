# Security policy

Axond sits in a request path and holds provider credentials, so a vulnerability
here is an outage or a key disclosure in someone else's production. Reports are
handled accordingly.

## Reporting a vulnerability

**Report privately, not in a public issue or pull request.**

Use GitHub's private vulnerability reporting for this repository:
[**Report a vulnerability**](https://github.com/Litvue/axond/security/advisories/new).
That opens a draft advisory visible only to you and the maintainers, and it is
the only reporting channel — there is no security mailing list to fall back to.

A useful report contains:

- the axond version (`axond --version`) or commit, and the deployment shape
  (single binary, container, Compose, Kubernetes) and [state tier](./docs/configuration.md#state-tiers);
- the configuration that reproduces it, with credentials removed — config is a
  public interface here, and most reachability questions are answered by it;
- what an attacker gains, and which trust boundary from the
  [deployment security model](./docs/security/deployment-model.md) is crossed
  (untrusted caller, tenant namespace, operator, provider upstream);
- a reproduction: request, response, and log or trace excerpt where possible.

Please do not run tests against infrastructure you do not operate.

### In scope

The gateway and its libraries as configured by a documented deployment:
authentication and minted tokens (issuance, scope, revocation, epochs),
namespace and tenant isolation, credential handling and redaction, budget and
usage integrity, the HTTP surface and its typed errors, the published crates,
the release artifacts (binaries, images, checksums, SBOMs, attestations), and
the installers.

### Not a vulnerability on its own

Findings that require an already-compromised trust boundary the
[deployment security model](./docs/security/deployment-model.md) treats as
trusted — for example an operator who can read the process environment, or a
holder of a static `[[gateway_key]]` acting within its own namespace. Missing
hardening with no demonstrated impact, dependency advisories with no reachable
path (the [dependency policy](./deny.toml) lane tracks those in the open), and
volumetric denial of service against your own deployment are handled as ordinary
issues. Report it privately anyway if you are unsure; triage is our job, not
yours.

## Supported versions

Axond is pre-1.0. Support tracks releases, not calendar dates:

| Version | Supported |
| --- | --- |
| Latest `0.x` release | yes — fixes land here |
| Immediately previous `0.x` minor | yes — security fixes only, while pre-1.0 |
| Anything older | no — upgrade to a supported release |

Concretely: with `0.4.z` released, `0.4` receives fixes and the last `0.3.z`
receives a backported patch for a security fix; `0.2` and earlier receive
nothing. A patch release within a supported minor is upgrade-safe by policy, so
the fix for a supported version is always a patch upgrade — see the
[compatibility contract](./docs/compatibility.md#stability-promises). Pre-release
and unreleased `main` commits are not separately supported; report against them
and the fix ships in the next release.

Only crates published from this repository (`axond`, `gateway-core`,
`gateway-transport`) and the release artifacts described in
[installation and verification](./docs/installation.md) are supported. A fork,
a patched build, or a vendored copy is yours to fix.

## What to expect

Times are targets on business days, measured from a report that reaches the
draft advisory:

| Stage | Target |
| --- | --- |
| Acknowledgement | 3 business days |
| Triage: accepted or declined, with severity | 10 business days |
| Fix released — critical or high | 30 days from triage |
| Fix released — moderate or low | the next scheduled release |

Severity uses CVSS v3.1 as a starting point and is adjusted for what the
[deployment security model](./docs/security/deployment-model.md) actually
exposes: a finding reachable by an unauthenticated caller outranks the same
weakness reachable only by an operator.

If a report is declined we say why, in the advisory thread, rather than closing
it silently. If you disagree, say so there — a declined report with a new
reachability argument gets re-triaged.

We ask for coordinated disclosure: keep the report private until the fix is
released or 90 days have passed, whichever comes first. There is no bug bounty.
Reporters are credited in the advisory and the release notes unless they ask not
to be.

## How a fix ships

1. Triage in the draft advisory, with a private fork for the patch when the fix
   would otherwise disclose the issue.
2. A regression test that fails before the fix. A security fix without one is
   not finished. When the finding is a parser — configuration, a minted token, a
   query string — the regression is a seed in the
   [fuzz corpora](./docs/security/fuzzing.md), so the required smoke replays that
   exact input from then on.
3. A release from `main` through the ordinary
   [release runbook](./docs/maintainers/releasing.md) — a security fix is not a
   different pipeline, so it inherits the same required CI, signed artifacts, and
   attestations.
4. A backport to the previous supported minor when that release is affected.
5. Publication of the GitHub Security Advisory with a CVE requested through
   GitHub, the affected and fixed version ranges, and the workaround if one
   exists. `RUSTSEC` coordination for the published library crates happens here
   too, so `cargo audit` and the repository's
   [dependency policy](./.github/workflows/dependency-audit.yml) lane see it.
6. A `CHANGELOG.md` entry referencing the advisory, and any configuration
   change the fix requires documented in the
   [compatibility contract](./docs/compatibility.md).

A fix that must break a documented interface is still a minor bump with a
migration note, per the compatibility contract; being a security fix does not
make a break invisible.

## Hardening your deployment

Prevention is documented separately, and is the more useful read if you are
deploying rather than reporting: the
[deployment security model](./docs/security/deployment-model.md) for trust
boundaries, the [production checklist](./docs/deployment/production-checklist.md)
before going live, the [minted-token guide](./docs/minted-token-guide.md) for
scoped credentials and revocation, [fuzzing](./docs/security/fuzzing.md) for what
the config, token, and query parsers are continuously tested against, and the
[security review](./docs/security-review-2026-08-05.md) for the reviewed
baseline.
