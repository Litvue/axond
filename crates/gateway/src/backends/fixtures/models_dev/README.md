# models.dev catalogue fixtures

Inputs for the [`models_dev`](../../models_dev.rs) adapter's tests, and the
offline seed a deployment can import with no network.

| File | What it is |
| --- | --- |
| `catalog.seed.json` | The offline seed: a trimmed excerpt of a real `https://models.dev/catalog.json` response (retrieved 2026-08-12, `ETag: "38a27321531a976c916911889525f559"`), keeping four providers and the shapes that matter — a provider-neutral record, an offering that overrides it, a deprecated offering, tiered pricing, and upstream fields this adapter does not model. |
| `catalog.identity.json` | The smallest payload that still has a neutral record and two offerings, one of them overriding it. The golden content identity is asserted against this file. |
| `catalog.aliases.json` | One provider publishing one model under two callable ids, as `qiniu-ai` does upstream. Both keys name the one model they are, so it is listed once, and both stay offerings because both are callable. |
| `catalog.aliases-repriced.json` | `catalog.aliases.json` with one of the two aliases repriced, which the catalogue diff names on its own — a provider's several aliases are paired by published id — while the callable projection reports nothing, since no callable id moved. |
| `catalog.aliases-unauthored.json` | `catalog.aliases.json` with no provider-neutral record at all, so each published id is its own model. Paired with `catalog.aliases.json` it is the refresh where an authored record appears and both callable ids come to name one model. |
| `catalog.cross-provider.json` | One model offered under four callable ids by three providers: a provider-local alias pair, the same published id from two different providers at different prices, and an aggregator republishing the authored id. |
| `catalog.cross-provider-renamed.json` | `catalog.cross-provider.json` after one provider renames the id callers must send and another withdraws its alias — a rename and a removal, in one refresh. |
| `catalog.cross-provider-substituted.json` | `catalog.cross-provider-renamed.json` with the new id's limits and price changed, so the withdrawn id and the added one are not the same offering — a removal and an addition rather than a rename. |
| `catalog.cross-provider-relocated.json` | `catalog.cross-provider-renamed.json` with the new id reached at another endpoint, so the withdrawal and the addition are not the same offering either. |
| `catalog.cross-provider-relabelled.json` | `catalog.cross-provider-renamed.json` with the new id's display name and last-updated date changed as well, as a provider renaming an id normally does — still one rename, because a label is not a term of service. |
| `catalog.identity-reordered.json` | `catalog.identity.json` with every object's keys and every array reversed, whitespace removed, and two fields the adapter does not know added. Same normalized content, so the same content identity — and a different raw digest. |
| `drift.*.json` | One rejected payload each: malformed JSON, the wrong document shape, a missing required field, a type change, an unrecognized enumerated value, an id that does not match its key, a provider-local key that names two authored models at once, every way a price can be unusable or would be silently dropped, and text with no canonical form. None of them may replace last-known-good state. |

The excerpt is checked in rather than fetched so the suite is hermetic and the
seed is reviewable: `just test` never contacts models.dev, and neither does the
inference path.
