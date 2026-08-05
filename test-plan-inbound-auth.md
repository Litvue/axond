# Test plan — inbound auth fails closed (PR #27, issue #17)

Runtime testing only, against the real `target/debug/axond` binary booted locally
with `AXOND_CONFIG` pointing at a copy of `axond.example.toml`. Shell-only (headless
HTTP gateway) — no GUI, so no recording; evidence is captured command output.

Code grounding: `crates/gateway/src/config.rs:621-655` (`validate_gateway_keys`),
`crates/gateway/src/state.rs:96-120` (`SnapshotError::MissingGatewayKey`),
`crates/gateway/src/main.rs:56-63` (boot log), `crates/gateway/src/routes.rs:52-105`
(router + `authenticate`), `crates/gateway/src/reload.rs:34-40,150-165`.

Env used (placeholders): GW_PLATFORM_OPENAI_API_KEY, GW_PLATFORM_OPENAI_API_KEY_OVERFLOW,
GW_PLATFORM_ANTHROPIC_API_KEY, GW_PLATFORM_AZURE_OPENAI_API_KEY, GW_ACME_OPENAI_API_KEY,
GW_INBOUND_PLATFORM_KEY=platform-token-abc, GW_INBOUND_ACME_KEY=acme-token-xyz.

## T1 — Keyless config refuses to boot
Delete both `[[gateway_key]]` blocks from the config copy; run the binary.
- PASS iff process exits non-zero and stderr contains
  `at least one `[[gateway_key]]` is required` and no listener binds 8080.
- Broken-impl signal: gateway would boot and serve.

## T2 — Declared key with unset env var is a fatal, non-secret boot error
Full config, `GW_INBOUND_PLATFORM_KEY` unset (and separately empty-string).
- PASS iff exit non-zero with exactly
  ``config resolution failed: gateway_key for namespace `platform` references env var `GW_INBOUND_PLATFORM_KEY`, which is unset or empty``
  and the output contains no secret value (grep for `platform-token-abc`, `sk-fake`, `acme-token-xyz` → 0 hits).
- Broken-impl signal: boot succeeds with a silently dropped key (old behavior).

## T3 — Boot log states the enforced posture
Boot with all env set.
- PASS iff stdout contains a JSON line with `"message":"inbound auth enforced"` and
  `"gateway_keys":2`, and contains zero case-insensitive matches for `anonymous`.

## T4 — Unauthenticated / wrong-credential requests are 401 on all three routes
For each of `/v1/chat/completions`, `/v1/messages`, `/v1/embeddings`, POST a valid JSON body:
(a) no credential header, (b) `Authorization: Bearer wrong-token`, (c) `x-api-key: wrong-token`.
- PASS iff all 9 responses are HTTP `401` with body error code `unauthorized`.

## T5 — Configured key passes in BOTH schemes and is namespace-attributed
Same POST to `/v1/chat/completions` with (a) `Authorization: Bearer $GW_INBOUND_PLATFORM_KEY`
and (b) `x-api-key: $GW_INBOUND_PLATFORM_KEY`.
- PASS iff both are NOT 401 — expected `502` with an upstream provider error body
  (fake OpenAI key), proving the request got past auth to dispatch.
- Also POST with the acme key (`$GW_INBOUND_ACME_KEY`) and confirm not 401.
- Check the emitted usage JSON line carries `subject` = the env-var NAME
  (`GW_INBOUND_PLATFORM_KEY`), never the secret value, and namespace `platform`
  (`acme` for the acme key).

## T6 — /healthz and /v1/models remain unauthenticated (Regression)
GET both with no headers.
- PASS iff `/healthz` → 200 body `ok`, `/v1/models` → 200 JSON listing aliases.

## T7 — SIGHUP reload with an unresolvable rotated key is rejected; old key keeps working
With the gateway running (generation 0):
1. Rewrite the config file, renaming `GW_INBOUND_PLATFORM_KEY` → `GW_INBOUND_PLATFORM_KEY_V2`
   (not exported). `kill -HUP <pid>`.
   - PASS iff log contains `config reload rejected; the running config keeps serving`
     with an error naming `GW_INBOUND_PLATFORM_KEY_V2`, AND a request with the OLD key
     `platform-token-abc` is still not 401 (502 upstream).
2. Since the reload re-reads the *process's own* environment, the gateway is booted with
   BOTH `GW_INBOUND_PLATFORM_KEY=platform-token-abc` and
   `GW_INBOUND_PLATFORM_KEY_V2=rotated-token-def` exported, while the config initially
   names only the old var. Step 1's rename therefore targets a third, never-exported var
   (`GW_INBOUND_PLATFORM_KEY_V3`) for the rejection case. Now rewrite the config to name
   `GW_INBOUND_PLATFORM_KEY_V2` and `kill -HUP` again.
   - PASS iff the log shows an accepted reload (reload summary line, generation advanced),
     the NEW token `rotated-token-def` is not 401 (502 upstream), and the OLD token
     `platform-token-abc` now returns `401`.
