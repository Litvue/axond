# Changelog

## [0.1.6](https://github.com/Litvue/axond/compare/v0.1.5...v0.1.6) (2026-08-10)


### Bug Fixes

* **limits:** make Redis reply attribution observable via echoed lease ids ([bbb9282](https://github.com/Litvue/axond/commit/bbb928248f8e5bd3a3f075a3fc04eb8ef0c6894c))
* **rate-limit:** decouple invoke deadline and handoff ([11244a6](https://github.com/Litvue/axond/commit/11244a6536ea1f2145291ddf77d0c62540795dee))
* **rate-limit:** echo acquire lease ids ([cdf4a9c](https://github.com/Litvue/axond/commit/cdf4a9c338e643db6232bd66d76e438dac101723))
* **rate-limit:** make timeout handoff observable ([52859a2](https://github.com/Litvue/axond/commit/52859a2ada942d97920948047a973f2f5b5eec94))
* **rate-limit:** preserve timeout handoff attribution ([e2893d8](https://github.com/Litvue/axond/commit/e2893d855f748387be7c36bcf0e69030c983413d))
* **rate-limit:** share mismatch escalation path ([1ca5d22](https://github.com/Litvue/axond/commit/1ca5d2226a3008a959da2969ca5da2ac64d56159))
* **rate-limit:** unify abandoned result compensation ([53c3f02](https://github.com/Litvue/axond/commit/53c3f02db6628b91c0d35824b7282c54b32b160b))

## [0.1.5](https://github.com/Litvue/axond/compare/v0.1.4...v0.1.5) (2026-08-10)


### Bug Fixes

* bound Redis lease release retries ([50bcb68](https://github.com/Litvue/axond/commit/50bcb685c31b767e5d583687c6d2d990fa18b37f))
* continue contended Redis retry attempts ([051b408](https://github.com/Litvue/axond/commit/051b4083a89b7f821ea8f09838eb85c12cffcca5))
* derive Redis release budget from admission timeout ([312158e](https://github.com/Litvue/axond/commit/312158e8d1e3b28322d3cbc574c7ac06e20b9a09))
* **limits:** bound owned Redis invokes and stop treating saturation as poisoning ([ab678a1](https://github.com/Litvue/axond/commit/ab678a1915fefaeea08e4a21b73f8a3dc5f4fee5))
* **limits:** retire poisoned Redis limiter connections after a cancelled request ([af0200e](https://github.com/Litvue/axond/commit/af0200e5426057f0107f7cedab3566fc482394b2))
* **limits:** retry ambiguous Redis lease releases on a fresh connection ([9569bc9](https://github.com/Litvue/axond/commit/9569bc9f5d1876813484f315bd41e63179c878f1))
* narrow Redis retry permit scope ([8686e39](https://github.com/Litvue/axond/commit/8686e397d4983810c274b6daa016c39d6cd91cdd))
* **rate-limit:** bound owned Redis invokes ([b6e24ae](https://github.com/Litvue/axond/commit/b6e24ae18d96d6677d735e374c58e98d632a1ce1))
* **rate-limit:** preserve ambiguous acquire compensation ([6c8549e](https://github.com/Litvue/axond/commit/6c8549e78e740cd85441ce3b42e327bd98f3980c))
* **rate-limit:** release recovery worker state ([405bb41](https://github.com/Litvue/axond/commit/405bb411b8378b34470aa9a4caad96213d77a273))
* recover poisoned Redis limiter connections ([e988303](https://github.com/Litvue/axond/commit/e988303ae6f10ff7fffa647a3ec232c9e27098ed))
* retire Redis generations on dropped invokes ([8d0f505](https://github.com/Litvue/axond/commit/8d0f505e841a73e896776ae2fa836d6dacbc14b3))
* retire shared Redis connections on release timeout ([4449d18](https://github.com/Litvue/axond/commit/4449d1817d2e9dcfb74d1799b623aeb13ac85705))
* retry ambiguous Redis lease releases ([a27a49a](https://github.com/Litvue/axond/commit/a27a49ae32e1703850662429b11c02bc8ff0f6cc))
* reuse healthy current Redis connections ([dd005ca](https://github.com/Litvue/axond/commit/dd005ca57e3dfb8e6cf3cb4c89982b5aaa4963a6))
* scope Redis suspicion to connection generations ([fe2f320](https://github.com/Litvue/axond/commit/fe2f320dcc77ff246f8cf8067d5216a467090413))
* skip suspect Redis connection for release ([ab6f627](https://github.com/Litvue/axond/commit/ab6f627fcd26501c6b60b6f5581b97d29dc990eb))


### Documentation

* document the stateful test service resolver ([0ffd529](https://github.com/Litvue/axond/commit/0ffd5292b4480074aa155985868cae17d466ea42))
* record the release retry concurrency cap ([06066f4](https://github.com/Litvue/axond/commit/06066f4009f81fba5e509834c5d4a2f6ad688b88))
* tighten the lease release backstop wording ([e5ebe4f](https://github.com/Litvue/axond/commit/e5ebe4f0b270a8cb34b6e8ea5de70575ecf133d9))


### Tests

* cover exhausted Redis release retries ([6d9e097](https://github.com/Litvue/axond/commit/6d9e097367655abb09c38803ddde37d10aa1a097))
* model dropped Redis responses faithfully ([63fbcd7](https://github.com/Litvue/axond/commit/63fbcd797bb2c67b9003cd0ed7ad2993f90d63b3))
* remove vacuous retry assertion ([154acc2](https://github.com/Litvue/axond/commit/154acc2a6c469e59d7f35a5f3038260b62154b94))
* restore pre-existing Redis recovery sequence ([d72d27e](https://github.com/Litvue/axond/commit/d72d27ef8106f29e6e2f2b141db78cc855d33f0f))


### Continuous Integration

* run stateful datastore tests ([1a3a7a3](https://github.com/Litvue/axond/commit/1a3a7a385d84c2b8813dda33d65771893239c358))
* run the Redis and Postgres stateful tests in CI ([aa2820a](https://github.com/Litvue/axond/commit/aa2820a0755c26045d2bd8a811ee5f5d4db4c7b6))

## [0.1.4](https://github.com/Litvue/axond/compare/v0.1.3...v0.1.4) (2026-08-10)


### Features

* **gateway:** add Redis-backed inbound rate limiting ([ca5139d](https://github.com/Litvue/axond/commit/ca5139df0493c714682d1a9a1f7ca5e4cc6d2ffe))
* **gateway:** separate Redis connect timeout ([c7a4d67](https://github.com/Litvue/axond/commit/c7a4d67e8306d459ccfbb33633bb4b09dac03af4))
* **limits:** exact cross-replica Redis rate limiting ([9f13050](https://github.com/Litvue/axond/commit/9f130504e768f99d5a46984d6d73d9c2c60e2346))


### Bug Fixes

* **gateway:** harden Redis rate-limit lease recovery ([dd41854](https://github.com/Litvue/axond/commit/dd418546cddf20d15fa889a6cad5808ee3adb734))
* **gateway:** harden Redis rate-limit leases ([2342006](https://github.com/Litvue/axond/commit/2342006bc8a69e593e1d0265461e1b202a10126c))


### Tests

* **gateway:** cover rate limit store errors ([03c74e5](https://github.com/Litvue/axond/commit/03c74e54ad4cb09290ec1708f8dd093d67ce4b00))
* **gateway:** cover Redis limiter failure paths ([8e86a44](https://github.com/Litvue/axond/commit/8e86a441a58e7ea7edc6c4a1f1d005a1e58062e8))
* **gateway:** verify Redis limiter reconnects after outage ([c206b35](https://github.com/Litvue/axond/commit/c206b357cc879570a9c55dc889fc76c3d4c444c1))

## [0.1.3](https://github.com/Litvue/axond/compare/v0.1.2...v0.1.3) (2026-08-10)


### Features

* **auth:** add minted token issuance epochs ([d6b2c1b](https://github.com/Litvue/axond/commit/d6b2c1b06853404cc86c00d5cbe9db25d0d90838))
* **auth:** enforce max_request_microdollars per-request ceiling ([a1f8609](https://github.com/Litvue/axond/commit/a1f86096c2b45d092cc7d40658a055c00aae2380))
* **auth:** enforce max_request_microdollars per-request ceiling ([604f962](https://github.com/Litvue/axond/commit/604f962be7137d78222c650b565eaadf8135df4f))
* **auth:** enforce scoped route capabilities ([dd8711b](https://github.com/Litvue/axond/commit/dd8711bb95e7fb5da429cda14b0398cd3713160b))
* **auth:** enforce scoped route capabilities on every provider route ([49d378d](https://github.com/Litvue/axond/commit/49d378db2473b9254219433b4bca2865b46c8cff))
* **auth:** enforce the minted `aliases` narrowing claim ([a1465ad](https://github.com/Litvue/axond/commit/a1465ad9963643cbd7f2a360347e0076be5ff01c))
* **auth:** min_iat issuance epochs for stateless mass revocation ([c2033c2](https://github.com/Litvue/axond/commit/c2033c2c7ee85992521d62980a06493007e9a847))
* **budget:** bound in-memory ledger retention by namespace ([9346c1d](https://github.com/Litvue/axond/commit/9346c1d550918c79b3c2dd0136865c8a92460928))
* **budget:** bound in-memory ledger retention per namespace ([ba08041](https://github.com/Litvue/axond/commit/ba0804184a778c6129711fc11f1538d82c00bfd0))
* **gateway:** enforce minted alias claims ([f5d0adb](https://github.com/Litvue/axond/commit/f5d0adbbd5b9a525eb5c195aed4df9e1c2023c2a))
* **limits:** add inbound rate limiter with in-memory backend ([50d4592](https://github.com/Litvue/axond/commit/50d45921075513b3bb8138fdc45e58c35a54e421))
* **limits:** inbound RateLimiter trait with NoLimit default and in-memory limiter ([91a7e8e](https://github.com/Litvue/axond/commit/91a7e8ebd69c7b785f4f004636823e00b65b1ee8))


### Bug Fixes

* **auth:** avoid allocations for epoch checks ([755b80a](https://github.com/Litvue/axond/commit/755b80aa20dedc37653aff94822744400a0a8202))
* **auth:** repair merged epoch documentation and test ([4eb5784](https://github.com/Litvue/axond/commit/4eb578469197337f88b236a851a22b8809fea188))
* **auth:** report and reject invalid token epochs ([c314b10](https://github.com/Litvue/axond/commit/c314b1073f0d5154d70160944e77c5493ce093ac))
* **auth:** tighten request ceiling response and coverage ([6c90e97](https://github.com/Litvue/axond/commit/6c90e97fc58bf20935931041cade2f53029ffd1c))
* **budget:** clamp post-reload namespace reservations ([45a883f](https://github.com/Litvue/axond/commit/45a883f3c4641ee6a4b734cc55d317905d17e927))
* **budget:** size namespace floors by distinct IDs ([cbe4959](https://github.com/Litvue/axond/commit/cbe4959227ba71423927248cebf544b95cf0b437))
* **config:** report rate limit changes on reload ([b996640](https://github.com/Litvue/axond/commit/b99664027ea37f78e16050fabd87b98a98813cdd))
* **gateway:** reject null aliases claims ([5dc4dd8](https://github.com/Litvue/axond/commit/5dc4dd80010b7efc9d3e0b1d4b6f8f204f25a4bf))
* **gateway:** release cancelled buffered reservations ([d18009a](https://github.com/Litvue/axond/commit/d18009a2f9121537e4ee62867add858d03d40e16))
* **gateway:** release cancelled buffered reservations ([23c07e7](https://github.com/Litvue/axond/commit/23c07e75f579a2dae528f0758aa35f93e6cd0c85))
* **gateway:** strengthen alias claim coverage ([5314825](https://github.com/Litvue/axond/commit/53148256a5f5cd4f0ba1bac2f3809b2e3ccadfc4))
* **limits:** tighten permit and rejection handling ([26479d8](https://github.com/Litvue/axond/commit/26479d8c30a8390ae399cc094509b97e78b5be0b))


### Refactors

* **budget:** unify in-memory ledger state locking ([339e49d](https://github.com/Litvue/axond/commit/339e49d9e7b21ac36fcf1fd0dbe9aee41004752a))


### Documentation

* **adr:** document scoped capability state tier ([09a43a8](https://github.com/Litvue/axond/commit/09a43a8ce785c4d19f07195c2eec4127137efbe6))
* **adr:** note that a null scope claim is an absent claim ([10a36c7](https://github.com/Litvue/axond/commit/10a36c7f844ed5ac8298c1f9b6c449ae8f6edce6))
* **auth:** note the zero-ceiling and estimate-granularity edges ([2ee0e26](https://github.com/Litvue/axond/commit/2ee0e2638dc6982cf90ede987ea7e48e65e25632))
* **budget:** clarify namespace retention headroom ([38ecda7](https://github.com/Litvue/axond/commit/38ecda7db350dbb5cc2af99e10559fdda429ea07))
* clarify alias claim behavior ([3dd9f1b](https://github.com/Litvue/axond/commit/3dd9f1bbb8e4fec23137b8b1ebe5de49cd8d5978))
* declare the state tier of every existing feature ([629884c](https://github.com/Litvue/axond/commit/629884c6ad4ccba927985914988078d567f2c7b1))
* document state tiers ([8c7294f](https://github.com/Litvue/axond/commit/8c7294f5f3c6eee04de0d86b633bd0c26d40a855))
* **gateway:** narrow reservation guard coverage ([4220e68](https://github.com/Litvue/axond/commit/4220e688998e1a6a2206881123f6333b96eacdd7))
* keep config loading order ahead of the tier table ([453325f](https://github.com/Litvue/axond/commit/453325f3f0e6b63bfed5774ccf0b5e920b6e956e))


### Tests

* **auth:** cover issuance epoch reloads ([0edb5cc](https://github.com/Litvue/axond/commit/0edb5ccbcbf1dd97c52b1c1de5eddb89e8a3134f))
* **gateway:** wait for cancellation release ([3a3f789](https://github.com/Litvue/axond/commit/3a3f78961fc3e4ae3e3e11423582564ec8f71f73))


### Continuous Integration

* **release:** narrow lockfile sync concurrency ([1f5d605](https://github.com/Litvue/axond/commit/1f5d605f29b0d899f3e496f5b1729e4692ec03fd))
* **release:** serialize lockfile sync retries ([b4615e0](https://github.com/Litvue/axond/commit/b4615e0efcd2efb1d6ef8ff13d25e695013f87ab))
* **release:** serialize release runs and re-base the lockfile sync ([6e13a32](https://github.com/Litvue/axond/commit/6e13a32234e94868fffee4598b2981520e301523))

## [0.1.2](https://github.com/Litvue/axond/compare/v0.1.1...v0.1.2) (2026-08-09)


### Features

* add hermetic tier 0 gate ([ecb965a](https://github.com/Litvue/axond/commit/ecb965ab7e098524faa982d4ce03196194f5ceea))
* **auth:** configure and verify minted tokens ([bfed409](https://github.com/Litvue/axond/commit/bfed40905e19245d6d937e2a2dfb3a3d72c0cac1))
* **auth:** read gateway key material from files ([9b8aad2](https://github.com/Litvue/axond/commit/9b8aad2493d6faab48cdc37492adb25e99dabfe7))
* **auth:** read gateway key material from files ([16e06a9](https://github.com/Litvue/axond/commit/16e06a9295c16477df293895fc8c100dbce819ec))
* **budget:** bound in-memory ledger retention ([8f54c71](https://github.com/Litvue/axond/commit/8f54c71ce658f75950f1290d5fc9977a9f3b6d0e))
* **cli:** add offline mint and keygen commands ([a6be07f](https://github.com/Litvue/axond/commit/a6be07fbf96bab69dce7daf51292e74f64bf2174))
* **cli:** add offline mint and keygen commands ([adb88d4](https://github.com/Litvue/axond/commit/adb88d4b47e80e97148ee520af827a652a7b1865))
* **usage:** attribute inbound signer kid ([9385478](https://github.com/Litvue/axond/commit/938547838009557dec24c9c5e6a322d21e5c34a4))


### Bug Fixes

* **auth:** enforce token lifetime and reload diffs ([da9cc62](https://github.com/Litvue/axond/commit/da9cc6266026021335dd3247784f344be447a556))
* authenticate responses route ([fc2026b](https://github.com/Litvue/axond/commit/fc2026be37b3b7038611462e7010a325090a9be3))
* **auth:** fingerprint verifier material after one read ([106296a](https://github.com/Litvue/axond/commit/106296a1c1cc15d809ea5c5cc720d28982731c81))
* **auth:** harden token rejection handling ([216fb35](https://github.com/Litvue/axond/commit/216fb3508a7ed06d1cb57d14e08fd9003177df2f))
* **auth:** harden token verifier configuration ([ec0aaed](https://github.com/Litvue/axond/commit/ec0aaed4fba75db18836d357a908a94e0f179b0f))
* **auth:** harden verifier lifetime and secret policy ([d92b891](https://github.com/Litvue/axond/commit/d92b891a239ea4fa96454f91e4c573ff4d9c5172))
* **auth:** reject overlapping principal shapes ([49d97d6](https://github.com/Litvue/axond/commit/49d97d68d1cd241a29e02761c25a1c3e751be257))
* **auth:** report verifier definition reload changes ([a52e2a0](https://github.com/Litvue/axond/commit/a52e2a0a0c80ce5f85cc6592a78c8a9e7677bc62))
* **auth:** salt reload fingerprints ([a744849](https://github.com/Litvue/axond/commit/a744849bb83bc3713dbdf416152dc8b50b1bdfce))
* **auth:** tolerate whitespace around ed25519 keys ([dd4048b](https://github.com/Litvue/axond/commit/dd4048bf0a15f3f634d8cc6a41e5f074844673c5))
* bound tier 0 probe failures ([15ffdea](https://github.com/Litvue/axond/commit/15ffdea40ff7701769772da825f51a4eaa7addfb))
* bound tier 0 readiness probes ([d5c7c37](https://github.com/Litvue/axond/commit/d5c7c371ed46b734db573ad95b8a41597842fbd3))
* **budget:** clarify unenforceable cap logs ([e8d429c](https://github.com/Litvue/axond/commit/e8d429c759897ade428dc77f9e5545011c57c221))
* **budget:** honor unavailable policy at capacity ([047ce98](https://github.com/Litvue/axond/commit/047ce985512b4f920b240c27483b972eab411167))
* **budget:** reclaim expired in-memory holds ([eb62995](https://github.com/Litvue/axond/commit/eb629953460ff9679c2c0cb7c1691382fe32bda5))
* **budget:** record late settlement spend ([ce14be7](https://github.com/Litvue/axond/commit/ce14be774f7361604f9531cabaa7d6c5596f7308))
* **cli:** clarify key permissions and config hints ([f3df44c](https://github.com/Litvue/axond/commit/f3df44cd6b05d74f0948597aa2a8ace7d124e7b4))
* **cli:** protect generated private key buffers ([1d5ea46](https://github.com/Litvue/axond/commit/1d5ea46040e5622b787c9eab5fca7dc0b4990df8))
* **cli:** reject zero token lifetimes ([d8bb0a8](https://github.com/Litvue/axond/commit/d8bb0a8624a95d0022e4fc58c7ccaf1c65fbf3b5))
* **cli:** tighten config-backed minting ([9ffbaa6](https://github.com/Litvue/axond/commit/9ffbaa6a536db565d1041764f961d802a350c67a))
* **cli:** validate configured mint audiences ([e5864b6](https://github.com/Litvue/axond/commit/e5864b630a7f7e0abc7de54245adbbb305cae9f6))
* **cli:** validate keygen identifiers ([86449ef](https://github.com/Litvue/axond/commit/86449ef029303e1c9eb0c4ddbef1ba61346fe322))
* **config:** ignore blank alternate key sources ([e59cc0d](https://github.com/Litvue/axond/commit/e59cc0dcc8808f636d3ace559a9e318076b64b26))
* **config:** name budget idle ttl units ([c3d963e](https://github.com/Litvue/axond/commit/c3d963ed493800c0a2039bcf6648a68129a6c520))
* enforce route authentication posture ([af7e7a0](https://github.com/Litvue/axond/commit/af7e7a041acc993823b8766a0793cc3dfd4ca59b))
* **gateway:** authenticate /v1/responses and enforce route auth posture ([e0e89c6](https://github.com/Litvue/axond/commit/e0e89c687e625712ef8cbb2119f27b0cc0e2824b))
* harden tier 0 listener checks ([89e889c](https://github.com/Litvue/axond/commit/89e889ca35da30e9b7c489ae96e34d66231b376c))
* **metrics:** count only denied budget capacity ([b0fc3a5](https://github.com/Litvue/axond/commit/b0fc3a5c8bb43643882ca64a4ef0c130321fad4d))
* preserve tier 0 gate diagnostics ([a6ba2b9](https://github.com/Litvue/axond/commit/a6ba2b9166c05af36b56511fb55cbc33012af6dc))
* **reload:** exclude budget changes from applied delta ([9766906](https://github.com/Litvue/axond/commit/97669064a10263d58bc8b61070919170e8d183a3))


### Refactors

* **auth:** drop the orphaned static-key accessors ([7880e8a](https://github.com/Litvue/axond/commit/7880e8ae4a4aad6fcb245d6cc3ee62ef4b590dc0))
* **auth:** resolve inbound identity through a PrincipalStore seam ([23e8b34](https://github.com/Litvue/axond/commit/23e8b34721b71ca96c0338ea4001a8fd1b8539de))
* clarify route authentication seams ([a6e8d1a](https://github.com/Litvue/axond/commit/a6e8d1a77e23bdb939d9aecfa897f8e8d7bdef20))


### Documentation

* add minted identity operator guide ([5fe1605](https://github.com/Litvue/axond/commit/5fe1605a1e3a96b20dcb3c42ce5e3424ca0e7db7))
* **adr:** accept 0016 and 0017 ([9240b27](https://github.com/Litvue/axond/commit/9240b2703ec4e379140357d55fdb390f85dd0659))
* **adr:** clarify principal store failure semantics ([9621618](https://github.com/Litvue/axond/commit/962161861e44c8464502ff69095d2e80d0d5bc7b))
* **adr:** define minted identity and state tiers ([0e09acb](https://github.com/Litvue/axond/commit/0e09acbef72eab44ddf69ee0d748421c8fd79c28))
* **adr:** define principal shape ownership ([4de618f](https://github.com/Litvue/axond/commit/4de618f3300963c12ae3ac3c33613eebc38ad96e))
* **adr:** minted inbound identity and explicit state tiers ([7409ad2](https://github.com/Litvue/axond/commit/7409ad20f3d2c2ed5a89bdc5156a0cf1a19c0390))
* **auth:** clarify static file key newline behavior ([29f7e2c](https://github.com/Litvue/axond/commit/29f7e2c60f94efcef2cd1e1be7bd1fa335ee25cb))
* **auth:** minted-token operator guide and rotation runbook ([6b94a3d](https://github.com/Litvue/axond/commit/6b94a3d9d00ba057e4fccef0e0b91bed2b0785fb))
* **auth:** note verifier env vars must precede boot ([6136e32](https://github.com/Litvue/axond/commit/6136e32b11b0dde2113ac6551ad67c961b5e14b7))
* **budget:** document in-memory ledger retention ([09e46d1](https://github.com/Litvue/axond/commit/09e46d180768b02569edbbd8b6413eb1263291e6))
* correct minted identity rotation runbook ([4fbdfc8](https://github.com/Litvue/axond/commit/4fbdfc86d394a1cd3c7d17d82cdd3a9ead8d09af))
* document budget reload and metrics ([6b6fccc](https://github.com/Litvue/axond/commit/6b6fccc11f2e66bf94ea77ef3669e7e5f5e7f8c5))
* document minted identity configuration ([f52445c](https://github.com/Litvue/axond/commit/f52445c558d60c7cd992d5d236e4915556d03a26))
* fix hs256 guide setup ([3f8b8cc](https://github.com/Litvue/axond/commit/3f8b8cc90b42f12c7b23f840f80a6ca6fabeb7c2))
* record budget reload semantics ([aac34c6](https://github.com/Litvue/axond/commit/aac34c656736b08b4c9007a69f32bf4604513257))
* **skill:** record tier 0 gate and musl prerequisites ([e3a26b3](https://github.com/Litvue/axond/commit/e3a26b30897c251b20b30cacd2b2d14d45e8c40b))
* **skills:** add minted-identity testing recipe to testing-axond ([618b68c](https://github.com/Litvue/axond/commit/618b68c39b19a881aae5abc87933cc58a51be9b8))
* **skills:** fix no-config minted token recipe ([d171066](https://github.com/Litvue/axond/commit/d1710662625cc4295e0c8910776321bcfff7d21d))
* **usage:** explain additive postgres migrations ([d961e3c](https://github.com/Litvue/axond/commit/d961e3ca6e19b6d06a7d63d5875f03564aeb21d1))


### Continuous Integration

* enforce tier 0 with a hermetic boot-and-serve gate ([30da901](https://github.com/Litvue/axond/commit/30da901e26033867d51c4b903e935d42ad031567))

## [0.1.1](https://github.com/Litvue/axond/compare/v0.1.0...v0.1.1) (2026-08-05)


### Features

* **auth:** gate /v1/models and scope its catalogue to the caller's namespace ([7d89041](https://github.com/Litvue/axond/commit/7d89041fd937675b1f8d146692e96892cfe5a63e))
* **auth:** gate /v1/models and scope its catalogue to the caller's namespace ([51e08fe](https://github.com/Litvue/axond/commit/51e08fef615c04f2b23b92ef942cda8af3830374)), closes [#34](https://github.com/Litvue/axond/issues/34)


### Bug Fixes

* **credentials:** make is_present a pure query, no rotation/health side effects ([01ec109](https://github.com/Litvue/axond/commit/01ec109567f64a3ad79225561e7866c6c7b9e3d7))
* **security:** hold inbound gateway keys as SecretString ([eb3c562](https://github.com/Litvue/axond/commit/eb3c562657eb1f68bbcac20d0b521a4099f6d437))
* **security:** hold inbound gateway keys as SecretString ([9473677](https://github.com/Litvue/axond/commit/9473677f6f5c679efa36758f5b86b9e7bcd624af)), closes [#35](https://github.com/Litvue/axond/issues/35)


### Continuous Integration

* drop the invalid bootstrap-sha from release-please config ([febe5ba](https://github.com/Litvue/axond/commit/febe5ba6147f6422a1502d0f51f4e015f1a13ce8))
* drop the invalid bootstrap-sha from release-please config ([1d000ed](https://github.com/Litvue/axond/commit/1d000ed77e231968135f159e76c21c3386711079)), closes [#36](https://github.com/Litvue/axond/issues/36)

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
