# TypeScript automated review dispositions

Date: 2026-10-08. This pass evaluated 85 existing findings across the stacked
PRs. The rows identify findings, including duplicates; they are not a count
of distinct defects. 79 are addressed by implementation, regression tests,
fixtures, or gates; six document intended staging or contract boundaries.

The complete stack must pass Node, Bun, workerd, vendor SDK compatibility,
shadow comparison, binary smoke, OpenAPI, and policy checks before adoption.
This report is implementation evidence, not a maintainer approval or proof
of full Rust equivalence. Existing release differences in
[the parity contract](typescript-parity.md) remain explicit, including the
model-ID discovery cache's separation from Rust catalogue pricing/offerings.
The security boundaries and TypeScript upgrade impact are described in
[ADR 0067](../adr/0067-typescript-review-hardening.md).

For staged integration observations, later slices provide the caller and full
qualification. Pre-authentication extensions intentionally authenticate under
ADR 0066; installed extension code is reviewed operator code. The token
extension now restricts grants to inference and validates claims independently.

| PR | Existing finding | Disposition |
| --- | --- | --- |
| [#534](https://github.com/Litvue/axond/pull/534) | [Undelimited SSE exhausts gateway memory](https://github.com/Litvue/axond/pull/534#discussion_r4211718719) | Addressed; see owning patch and regression checks |
| [#534](https://github.com/Litvue/axond/pull/534) | [Merge queue skips TypeScript tests](https://github.com/Litvue/axond/pull/534#discussion_r4211718859) | Addressed; see owning patch and regression checks |
| [#534](https://github.com/Litvue/axond/pull/534) | [Mixed-newline SSE events evade transforms](https://github.com/Litvue/axond/pull/534#discussion_r4211719153) | Addressed; see owning patch and regression checks |
| [#534](https://github.com/Litvue/axond/pull/534) | [Parity documentation precedes its implementation](https://github.com/Litvue/axond/pull/534#discussion_r4211719278) | Contract/staging clarification |
| [#534](https://github.com/Litvue/axond/pull/534) | [TypeScript checks need separate branch protection](https://github.com/Litvue/axond/pull/534#discussion_r4211719501) | Addressed; see owning patch and regression checks |
| [#535](https://github.com/Litvue/axond/pull/535) | [Deep attributes exhaust the parser stack](https://github.com/Litvue/axond/pull/535#discussion_r4211952129) | Addressed; see owning patch and regression checks |
| [#535](https://github.com/Litvue/axond/pull/535) | [Replaced bodies retain the wrong model](https://github.com/Litvue/axond/pull/535#discussion_r4211952295) | Addressed; see owning patch and regression checks |
| [#535](https://github.com/Litvue/axond/pull/535) | [Null attributes bypass empty-object normalization](https://github.com/Litvue/axond/pull/535#discussion_r4211952477) | Addressed; see owning patch and regression checks |
| [#535](https://github.com/Litvue/axond/pull/535) | [Positional bodies accept invalid trailing commas](https://github.com/Litvue/axond/pull/535#discussion_r4211952633) | Addressed; see owning patch and regression checks |
| [#535](https://github.com/Litvue/axond/pull/535) | [Concurrent body reads fail unpredictably](https://github.com/Litvue/axond/pull/535#discussion_r4211955223) | Addressed; see owning patch and regression checks |
| [#535](https://github.com/Litvue/axond/pull/535) | [Parser qualification depends on later slices](https://github.com/Litvue/axond/pull/535#discussion_r4211955487) | Addressed; see owning patch and regression checks |
| [#536](https://github.com/Litvue/axond/pull/536) | [Large budgets admit at the wrong balance](https://github.com/Litvue/axond/pull/536#discussion_r4211948374) | Addressed; see owning patch and regression checks |
| [#536](https://github.com/Litvue/axond/pull/536) | [Existing SQLite files reject policy writes](https://github.com/Litvue/axond/pull/536#discussion_r4211948531) | Addressed; see owning patch and regression checks |
| [#536](https://github.com/Litvue/axond/pull/536) | [Interrupted deletion preserves the old budget](https://github.com/Litvue/axond/pull/536#discussion_r4211948703) | Addressed; see owning patch and regression checks |
| [#536](https://github.com/Litvue/axond/pull/536) | [Returned namespaces mutate live memory state](https://github.com/Litvue/axond/pull/536#discussion_r4211948891) | Addressed; see owning patch and regression checks |
| [#536](https://github.com/Litvue/axond/pull/536) | [Usage summaries block SQLite admissions](https://github.com/Litvue/axond/pull/536#discussion_r4211949029) | Addressed; see owning patch and regression checks |
| [#536](https://github.com/Litvue/axond/pull/536) | [Extension queries escape namespace isolation](https://github.com/Litvue/axond/pull/536#discussion_r4211949229) | Addressed; see owning patch and regression checks |
| [#537](https://github.com/Litvue/axond/pull/537) | [Large charges exceed the ledger range](https://github.com/Litvue/axond/pull/537#discussion_r4211956361) | Addressed; see owning patch and regression checks |
| [#537](https://github.com/Litvue/axond/pull/537) | [In-flight metrics export as monotonic counters](https://github.com/Litvue/axond/pull/537#discussion_r4211956553) | Addressed; see owning patch and regression checks |
| [#537](https://github.com/Litvue/axond/pull/537) | [Admission queue depth loses its histogram](https://github.com/Litvue/axond/pull/537#discussion_r4211956766) | Addressed; see owning patch and regression checks |
| [#537](https://github.com/Litvue/axond/pull/537) | [Collector refusals silently drop telemetry](https://github.com/Litvue/axond/pull/537#discussion_r4211957040) | Addressed; see owning patch and regression checks |
| [#537](https://github.com/Litvue/axond/pull/537) | [Runtime parity depends on later slices](https://github.com/Litvue/axond/pull/537#discussion_r4211957277) | Contract/staging clarification |
| [#537](https://github.com/Litvue/axond/pull/537) | [Undelimited provider streams exhaust gateway memory](https://github.com/Litvue/axond/pull/537#discussion_r4211957446) | Addressed; see owning patch and regression checks |
| [#537](https://github.com/Litvue/axond/pull/537) | [Sensitive metric labels reach the collector](https://github.com/Litvue/axond/pull/537#discussion_r4211957623) | Addressed; see owning patch and regression checks |
| [#538](https://github.com/Litvue/axond/pull/538) | [This slice cannot test its config primitives end to end](https://github.com/Litvue/axond/pull/538#discussion_r4211974229) | Contract/staging clarification |
| [#538](https://github.com/Litvue/axond/pull/538) | [Lowercase variables override gateway settings](https://github.com/Litvue/axond/pull/538#discussion_r4211974431) | Addressed; see owning patch and regression checks |
| [#538](https://github.com/Litvue/axond/pull/538) | [Multiline float loads as integer](https://github.com/Litvue/axond/pull/538#discussion_r4211974629) | Addressed; see owning patch and regression checks |
| [#538](https://github.com/Litvue/axond/pull/538) | [Multiline strings reject trailing quotes](https://github.com/Litvue/axond/pull/538#discussion_r4211974864) | Addressed; see owning patch and regression checks |
| [#539](https://github.com/Litvue/axond/pull/539) | [Malformed IPv6 bind passes validation](https://github.com/Litvue/axond/pull/539#discussion_r4211955150) | Addressed; see owning patch and regression checks |
| [#540](https://github.com/Litvue/axond/pull/540) | [Configured token prices are discarded](https://github.com/Litvue/axond/pull/540#discussion_r4211954286) | Addressed; see owning patch and regression checks |
| [#540](https://github.com/Litvue/axond/pull/540) | [Long admission waits expire immediately](https://github.com/Litvue/axond/pull/540#discussion_r4211954410) | Addressed; see owning patch and regression checks |
| [#540](https://github.com/Litvue/axond/pull/540) | [Catalogue URLs bypass host validation](https://github.com/Litvue/axond/pull/540#discussion_r4211954564) | Addressed; see owning patch and regression checks |
| [#542](https://github.com/Litvue/axond/pull/542) | [Silent streams outlive the idle timeout](https://github.com/Litvue/axond/pull/542#discussion_r4211966195) | Addressed; see owning patch and regression checks |
| [#542](https://github.com/Litvue/axond/pull/542) | [Recovery probes vanish from credential walks](https://github.com/Litvue/axond/pull/542#discussion_r4211966320) | Addressed; see owning patch and regression checks |
| [#542](https://github.com/Litvue/axond/pull/542) | [Fallback credentials bypass shared failure history](https://github.com/Litvue/axond/pull/542#discussion_r4211966479) | Addressed; see owning patch and regression checks |
| [#542](https://github.com/Litvue/axond/pull/542) | [Failed streams retain upstream connections](https://github.com/Litvue/axond/pull/542#discussion_r4211966704) | Addressed; see owning patch and regression checks |
| [#542](https://github.com/Litvue/axond/pull/542) | [Dispatch integration awaits a later slice](https://github.com/Litvue/axond/pull/542#discussion_r4211966850) | Contract/staging clarification |
| [#543](https://github.com/Litvue/axond/pull/543) | [Oversized bodies exhaust gateway memory](https://github.com/Litvue/axond/pull/543#discussion_r4212029879) | Addressed; see owning patch and regression checks |
| [#543](https://github.com/Litvue/axond/pull/543) | [Noncanonical inference paths reach providers](https://github.com/Litvue/axond/pull/543#discussion_r4212029989) | Addressed; see owning patch and regression checks |
| [#543](https://github.com/Litvue/axond/pull/543) | [Credential retries disappear from usage records](https://github.com/Litvue/axond/pull/543#discussion_r4212030154) | Addressed; see owning patch and regression checks |
| [#543](https://github.com/Litvue/axond/pull/543) | [Management OpenAPI omits path parameters](https://github.com/Litvue/axond/pull/543#discussion_r4212030297) | Addressed; see owning patch and regression checks |
| [#543](https://github.com/Litvue/axond/pull/543) | [Telemetry repeatedly exports accumulated metrics](https://github.com/Litvue/axond/pull/543#discussion_r4212030460) | Addressed; see owning patch and regression checks |
| [#543](https://github.com/Litvue/axond/pull/543) | [Untrusted extensions can modify other namespaces](https://github.com/Litvue/axond/pull/543#discussion_r4212030721) | Addressed; see owning patch and regression checks |
| [#543](https://github.com/Litvue/axond/pull/543) | [Pre-auth extension flag bypasses gateway authentication](https://github.com/Litvue/axond/pull/543#discussion_r4212030930) | Contract/staging clarification |
| [#544](https://github.com/Litvue/axond/pull/544) | [Settlement assertions depend on timing](https://github.com/Litvue/axond/pull/544#discussion_r4211956826) | Addressed; see owning patch and regression checks |
| [#545](https://github.com/Litvue/axond/pull/545) | [Extension ordering lacks direct coverage](https://github.com/Litvue/axond/pull/545#discussion_r4211958115) | Addressed; see owning patch and regression checks |
| [#545](https://github.com/Litvue/axond/pull/545) | [Duplicated gateway fixtures invite drift](https://github.com/Litvue/axond/pull/545#discussion_r4211958295) | Addressed; see owning patch and regression checks |
| [#546](https://github.com/Litvue/axond/pull/546) | [Queue saturation test has a scheduling race](https://github.com/Litvue/axond/pull/546#discussion_r4211963096) | Addressed; see owning patch and regression checks |
| [#546](https://github.com/Litvue/axond/pull/546) | [Terminal tail test races the grace deadline](https://github.com/Litvue/axond/pull/546#discussion_r4211963282) | Addressed; see owning patch and regression checks |
| [#547](https://github.com/Litvue/axond/pull/547) | [Duplicated test fixtures across slices](https://github.com/Litvue/axond/pull/547#discussion_r4211957688) | Addressed; see owning patch and regression checks |
| [#548](https://github.com/Litvue/axond/pull/548) | [Large namespace attributes lose precision](https://github.com/Litvue/axond/pull/548#discussion_r4211968625) | Addressed; see owning patch and regression checks |
| [#548](https://github.com/Litvue/axond/pull/548) | [Monthly admission uses an obsolete policy](https://github.com/Litvue/axond/pull/548#discussion_r4211968707) | Addressed; see owning patch and regression checks |
| [#548](https://github.com/Litvue/axond/pull/548) | [Incomplete extension schema marked migrated](https://github.com/Litvue/axond/pull/548#discussion_r4211968923) | Addressed; see owning patch and regression checks |
| [#548](https://github.com/Litvue/axond/pull/548) | [Usage summaries scan all historical requests](https://github.com/Litvue/axond/pull/548#discussion_r4211969041) | Addressed; see owning patch and regression checks |
| [#548](https://github.com/Litvue/axond/pull/548) | [CTE writes bypass lock wait limits](https://github.com/Litvue/axond/pull/548#discussion_r4211969201) | Addressed; see owning patch and regression checks |
| [#548](https://github.com/Litvue/axond/pull/548) | [Untrusted extensions can read other namespaces](https://github.com/Litvue/axond/pull/548#discussion_r4211969331) | Addressed; see owning patch and regression checks |
| [#549](https://github.com/Litvue/axond/pull/549) | [Slow database writes bypass usage buffer limit](https://github.com/Litvue/axond/pull/549#discussion_r4211968086) | Addressed; see owning patch and regression checks |
| [#549](https://github.com/Litvue/axond/pull/549) | [Failed batches pass the shutdown flush](https://github.com/Litvue/axond/pull/549#discussion_r4211968232) | Addressed; see owning patch and regression checks |
| [#549](https://github.com/Litvue/axond/pull/549) | [Unrelated discovery prevents base URL refresh](https://github.com/Litvue/axond/pull/549#discussion_r4211968394) | Addressed; see owning patch and regression checks |
| [#549](https://github.com/Litvue/axond/pull/549) | [Database close exceeds shutdown flush deadline](https://github.com/Litvue/axond/pull/549#discussion_r4211968568) | Addressed; see owning patch and regression checks |
| [#549](https://github.com/Litvue/axond/pull/549) | [Failed sink initialization leaks database connections](https://github.com/Litvue/axond/pull/549#discussion_r4211968690) | Addressed; see owning patch and regression checks |
| [#549](https://github.com/Litvue/axond/pull/549) | [Invalid catalogue responses replace valid models](https://github.com/Litvue/axond/pull/549#discussion_r4211968837) | Addressed; see owning patch and regression checks |
| [#549](https://github.com/Litvue/axond/pull/549) | [Provider models stay empty after boot](https://github.com/Litvue/axond/pull/549#discussion_r4211968984) | Addressed; see owning patch and regression checks |
| [#549](https://github.com/Litvue/axond/pull/549) | [Catalogue parity scope needs clarification](https://github.com/Litvue/axond/pull/549#discussion_r4211969251) | Contract/staging clarification |
| [#550](https://github.com/Litvue/axond/pull/550) | [Disabled schema creation still creates tables](https://github.com/Litvue/axond/pull/550#discussion_r4211971200) | Addressed; see owning patch and regression checks |
| [#552](https://github.com/Litvue/axond/pull/552) | [Redacted prompts send the wrong model](https://github.com/Litvue/axond/pull/552#discussion_r4212029850) | Addressed; see owning patch and regression checks |
| [#552](https://github.com/Litvue/axond/pull/552) | [Separate limiters share one allowance](https://github.com/Litvue/axond/pull/552#discussion_r4212029997) | Addressed; see owning patch and regression checks |
| [#552](https://github.com/Litvue/axond/pull/552) | [Replacement text breaks valid JSON requests](https://github.com/Litvue/axond/pull/552#discussion_r4212030180) | Addressed; see owning patch and regression checks |
| [#552](https://github.com/Litvue/axond/pull/552) | [Expired rate-limit windows never expire](https://github.com/Litvue/axond/pull/552#discussion_r4212030419) | Addressed; see owning patch and regression checks |
| [#552](https://github.com/Litvue/axond/pull/552) | [Zero limit admits the first request](https://github.com/Litvue/axond/pull/552#discussion_r4212030574) | Addressed; see owning patch and regression checks |
| [#552](https://github.com/Litvue/axond/pull/552) | [Accepted patterns can exhaust the step budget](https://github.com/Litvue/axond/pull/552#discussion_r4212030786) | Addressed; see owning patch and regression checks |
| [#552](https://github.com/Litvue/axond/pull/552) | [Pattern syntax differs from regex expectations](https://github.com/Litvue/axond/pull/552#discussion_r4212030996) | Addressed; see owning patch and regression checks |
| [#552](https://github.com/Litvue/axond/pull/552) | [Minting shares its key with token signing](https://github.com/Litvue/axond/pull/552#discussion_r4212031209) | Addressed; see owning patch and regression checks |
| [#552](https://github.com/Litvue/axond/pull/552) | [Mint API grants unchecked authority](https://github.com/Litvue/axond/pull/552#discussion_r4212031370) | Addressed; see owning patch and regression checks |
| [#552](https://github.com/Litvue/axond/pull/552) | [Malformed signed claims bypass validation](https://github.com/Litvue/axond/pull/552#discussion_r4212031564) | Addressed; see owning patch and regression checks |
| [#552](https://github.com/Litvue/axond/pull/552) | [Request-ID collisions swap token authority](https://github.com/Litvue/axond/pull/552#discussion_r4212031707) | Addressed; see owning patch and regression checks |
| [#552](https://github.com/Litvue/axond/pull/552) | [Any token authorizes management APIs](https://github.com/Litvue/axond/pull/552#discussion_r4212031825) | Addressed; see owning patch and regression checks |
| [#553](https://github.com/Litvue/axond/pull/553) | [Default models.dev catalogue never refreshes](https://github.com/Litvue/axond/pull/553#discussion_r4211972952) | Addressed; see owning patch and regression checks |
| [#553](https://github.com/Litvue/axond/pull/553) | [Refused catalogue leaves stale models fresh](https://github.com/Litvue/axond/pull/553#discussion_r4211973146) | Addressed; see owning patch and regression checks |
| [#553](https://github.com/Litvue/axond/pull/553) | [Public default key grants gateway access](https://github.com/Litvue/axond/pull/553#discussion_r4211973360) | Addressed; see owning patch and regression checks |
| [#553](https://github.com/Litvue/axond/pull/553) | [Catalogue URL validation accepts malformed authorities](https://github.com/Litvue/axond/pull/553#discussion_r4211973524) | Addressed; see owning patch and regression checks |
| [#554](https://github.com/Litvue/axond/pull/554) | [Management API paths lack required parameters](https://github.com/Litvue/axond/pull/554#discussion_r4211986888) | Addressed; see owning patch and regression checks |
| [#554](https://github.com/Litvue/axond/pull/554) | [Large usage charges lose comparison precision](https://github.com/Litvue/axond/pull/554#discussion_r4211987034) | Addressed; see owning patch and regression checks |
| [#554](https://github.com/Litvue/axond/pull/554) | [Metric gate overlooks common alert expressions](https://github.com/Litvue/axond/pull/554#discussion_r4211987210) | Addressed; see owning patch and regression checks |
| [#554](https://github.com/Litvue/axond/pull/554) | [Tag signature remains on the runner](https://github.com/Litvue/axond/pull/554#discussion_r4211987391) | Addressed; see owning patch and regression checks |
