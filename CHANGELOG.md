# Changelog

## 0.1.0 (2026-08-05)


### Features

* **config:** hot-reload on SIGHUP and an optional watched file ([643b524](https://github.com/Litvue/axond/commit/643b524062e5a5d62e5e66505f3ca5c06515c434))
* **config:** hot-reload on SIGHUP and an optional watched file ([fa8fb20](https://github.com/Litvue/axond/commit/fa8fb20e6c6a2c99d1ce893a9d88dae35c9cb7be))
* **credentials:** pool multiple credentials per namespace and provider ([bdf57f8](https://github.com/Litvue/axond/commit/bdf57f83fb0c665937f8045f451bd8784fc47083))
* **credentials:** pool multiple credentials per namespace and provider ([5c6eaab](https://github.com/Litvue/axond/commit/5c6eaabd899866689178a91657db0bf46b19f756))
* fail closed on inbound auth, removing the keyless mode ([4d7bb99](https://github.com/Litvue/axond/commit/4d7bb9916103811c4269d1e11fdaff1bc38aa64b)), closes [#17](https://github.com/Litvue/axond/issues/17)
* **gateway:** relay streaming (SSE) responses end-to-end ([ec03325](https://github.com/Litvue/axond/commit/ec0332588de40c3a1afbcb2e75835052b849e367))
* **gateway:** relay streaming (SSE) responses end-to-end ([16b1769](https://github.com/Litvue/axond/commit/16b1769df219959e3e154fa669669be9d12c9c9f))
* ordered failover across targets with per-target circuit health ([96c3a18](https://github.com/Litvue/axond/commit/96c3a1832c841d52235fc4e6bd1b3c0fa33243b2))
* ordered failover across targets with per-target circuit health ([a33b8c0](https://github.com/Litvue/axond/commit/a33b8c03b4ae52ae72a6bdaead84d393ecb1a14e))
* serve native /v1/messages and /v1/embeddings as passthrough ([5231c0a](https://github.com/Litvue/axond/commit/5231c0a668534e77f5e31267e1c3d2700d93a5b9))
* serve native /v1/messages and /v1/embeddings as passthrough ([0c5de37](https://github.com/Litvue/axond/commit/0c5de37f2299a3749dbbc14c637c9cb63cbc5986))
* shared budget backends with held reservations and partial charging ([55cba03](https://github.com/Litvue/axond/commit/55cba0355d92e670c6c176d23cd375f6f0fdb825))
* shared budget backends with held reservations and partial charging ([6b09234](https://github.com/Litvue/axond/commit/6b092349fd3b5060ac614a14be71ab707975be00))
* **telemetry:** instrument the streamed path and fix ADR references ([4ee228c](https://github.com/Litvue/axond/commit/4ee228ce3a5c79ded9e7b7be67d33832010ae59a))
* **telemetry:** OTLP traces, metrics, and log correlation ([56a7485](https://github.com/Litvue/axond/commit/56a74859fbd6400b761c83b81d23076f056973ae))
* **telemetry:** OTLP traces, metrics, and log correlation ([c621edc](https://github.com/Litvue/axond/commit/c621edc2bb915f79d03c253cd62da372df570cbf))
* **usage:** add durable Postgres and OTLP usage sinks ([b21eaf8](https://github.com/Litvue/axond/commit/b21eaf842cc24fdd538619891ed549bc5919bfc2))
* **usage:** add durable Postgres and OTLP usage sinks ([7c393f6](https://github.com/Litvue/axond/commit/7c393f6dde4015ab5e031bdf7545fef37afd9849))


### Bug Fixes

* **config:** serialize reloads and compare process-level config against boot ([4a37663](https://github.com/Litvue/axond/commit/4a376637c72fb743bf06c20f8c88fbe1ebe83457))
* **credentials:** hand the half-open probe to one request per cooldown ([f83e097](https://github.com/Litvue/axond/commit/f83e09701f8142fc056a62264ad97c76b2e71367))
* **gateway:** decode streamed bytes across chunk boundaries and reject truncated streams ([b9410c6](https://github.com/Litvue/axond/commit/b9410c66e92467e555c8987ed7e7bee224b18eec))
* reject two gateway keys that resolve to the same secret ([52000ce](https://github.com/Litvue/axond/commit/52000ce3ec94c90cdccb0a1bf0bc813c8b35c55d))
* reuse the postgres budget connection and count characters, not bytes ([b44f183](https://github.com/Litvue/axond/commit/b44f1838a07e8ae60625290ceb917dd0817a9707))
* **routes:** reject wire-incompatible targets on /v1/chat/completions ([aeacc64](https://github.com/Litvue/axond/commit/aeacc64816ec0f156aaec3c16eacab39a91790bd))
* **routes:** reject wire-incompatible targets on /v1/chat/completions ([2f1c765](https://github.com/Litvue/axond/commit/2f1c7655eae12ee79725bcdd638ac27cc0880094))
* supply the example's azure-openai key in docker smoke and quick start ([f3e38cf](https://github.com/Litvue/axond/commit/f3e38cf64232f57c5622bd2beebfa9bc7244a95c))
* **telemetry:** export from the runtime and keep ids and labels bounded ([bc7367b](https://github.com/Litvue/axond/commit/bc7367b8b14adb04b02720839969b6e653559eb0))
* **usage:** keep index names unqualified when retargeting the usage DDL ([79a376b](https://github.com/Litvue/axond/commit/79a376b833da2c3716d47e5c66c429d2ebd1f92b))


### Documentation

* **adr:** renumber credential-pools ADR to 0006 to avoid collision with streaming ([b8ee13a](https://github.com/Litvue/axond/commit/b8ee13a3a183627a295f8638cef7967f2eb2ca97))
* **adr:** renumber telemetry ADR to 0007 to avoid collision with streaming and credential pools ([16e68b8](https://github.com/Litvue/axond/commit/16e68b8e017cec796d1613a02175c1c5d797c6ea))
* **credentials:** point the pool doc comments at the renumbered ADR 0006 ([e180ddf](https://github.com/Litvue/axond/commit/e180ddf4ad5d165e8e2573d61e65d5ba11b4f477))
* qualify the beta with deployment, compatibility, security, and release docs ([9ab7726](https://github.com/Litvue/axond/commit/9ab772641549e5f917324beaa1d09252f451e8aa))
* qualify the beta with deployment, compatibility, security, and release docs ([a78e2cb](https://github.com/Litvue/axond/commit/a78e2cbb8d1b02aca765a2952417b9bf7f6a404c))
* scope the native passthrough promise in ADR 0012 ([115d8a2](https://github.com/Litvue/axond/commit/115d8a272143a9bbdbaa0c25dba13d143316a53d))
* state the fixture byte assertion as relay fidelity ([a85aa29](https://github.com/Litvue/axond/commit/a85aa29a14d3efd7cc935fb299e8221f6691ed0e))


### Tests

* authorize the chat-wire-guard test under the fail-closed contract ([b1d7f12](https://github.com/Litvue/axond/commit/b1d7f1242322113a7312ab66d1bc3967cc4c65c9))
* compatibility, record/replay, and SSE soak harness ([1dd3a14](https://github.com/Litvue/axond/commit/1dd3a1444c269ac1e870e8a0234b585eb209edbd))
* drive the URL redaction through a real reqwest failure ([66b43f5](https://github.com/Litvue/axond/commit/66b43f596d4d861ce03f6eb2750a2752c5913e28))
* make the soak's cancel and memory assertions non-vacuous ([635b673](https://github.com/Litvue/axond/commit/635b673e669f71f2761b73fba8965c78b9e4c05d))
* qualify the request path with a compat, replay, and soak harness ([50c854a](https://github.com/Litvue/axond/commit/50c854a5ff37d0fe90ec1ff6ac008604dcf1da4f))
* **streaming:** unwrap fallible AppState::new in the stream test helper ([42d0585](https://github.com/Litvue/axond/commit/42d0585a9efc9378782b039aa38100f9d4b50d33))


### Continuous Integration

* accept static-pie musl binaries and record release-pipeline ADR ([1c3f30d](https://github.com/Litvue/axond/commit/1c3f30d1d8bb8209342542d0cc3f19cd49afc3e5))
* add release-please pipeline, CI matrix, and supply-chain gates ([b7536c3](https://github.com/Litvue/axond/commit/b7536c3ab8c326d083ef79ba3613af8238d42d8f))
* add release-please pipeline, CI matrix, and supply-chain gates ([480a826](https://github.com/Litvue/axond/commit/480a8269c9cbd83f27a752c1f8ee0a6cfd6dbd95))
* rely on the org-wide release GitHub App token ([58eff31](https://github.com/Litvue/axond/commit/58eff315924bb6d5ed3faa49c08a0743836568da))


### Miscellaneous

* cut the first beta release as v0.1.0 ([9707af2](https://github.com/Litvue/axond/commit/9707af281ed4160f4edbba5359e91cd991a72019))

## Changelog

All notable changes to this project are documented in this file.

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
and the changelog is generated automatically by
[release-please](https://github.com/googleapis/release-please) from
[Conventional Commits](https://www.conventionalcommits.org/). Do not edit
released sections by hand.
