# 3. Namespaced credentials and bring-your-own-key

Date: 2026-08-03

## Status

Accepted

## Context

Tollgate has two credential audiences at once:

1. The operator's **platform** keys (the company running the gateway supplies
   its own provider keys).
2. **Customer** keys, when the operator offers bring-your-own-key (BYOK) to its
   own customers.

The prior implementation had exactly one key per provider, wrapped in a
`SecretString`, captured from the environment at startup. It had no notion of
per-customer key isolation and no attribution of which key served a request.

## Decision

Credentials are organized by **namespace**. Namespaces are declared explicitly
in config (not inferred from mangled environment-variable names — `acme-corp`
and `acme.corp` must not collide). Each `(namespace, provider)` pair binds to a
named environment variable, read once at startup into a `(namespace, provider) →
secret` map.

- One namespace is the **platform** default.
- BYOK namespaces set `allow_platform_fallback` (default `false`): a namespace
  missing a provider key hard-fails rather than silently spending platform
  credits.
- Every usage record carries `credential_source: platform | byok`, so spend can
  be attributed and billed.
- Credentials are **write-only**: no endpoint returns a key; only presence is
  observable.

The data model anticipates **multiple credentials per provider per namespace**
(pooling, weighted selection, skip-on-429, per-credential circuit health), even
though the scaffold resolves a single key — because retrofitting `Option<Secret>`
→ pool later would touch credential resolution, provider selection, circuit
breaking, and attribution all at once.

## Consequences

- Onboarding a BYOK customer is a config + env-var change; no code and no
  issuance/revocation subsystem (which would fight statelessness).
- Namespace identity of the *caller* (which namespace a request belongs to) is
  resolved from an inbound gateway key; the mapping is explicit in config.
- The environment is the initial canonical credential layer. Watched credential
  files + `SIGHUP` for zero-restart onboarding are a deliberate later addition,
  not a rewrite.
