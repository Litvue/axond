# models.dev catalogue fixtures

Inputs for the [`models_dev`](../../models_dev.rs) adapter's tests, and the
offline seed a deployment can import with no network.

| File | What it is |
| --- | --- |
| `catalog.seed.json` | The offline seed: a trimmed excerpt of a real `https://models.dev/catalog.json` response (retrieved 2026-08-12, `ETag: "38a27321531a976c916911889525f559"`), keeping four providers and the shapes that matter — a provider-neutral record, an offering that overrides it, a deprecated offering, tiered pricing, and upstream fields this adapter does not model. |
| `catalog.identity.json` | The smallest payload that still has a neutral record and two offerings, one of them overriding it. The golden content identity is asserted against this file. |
| `catalog.aliases.json` | One provider publishing one model under two callable ids, as `qiniu-ai` does upstream. Both keys name the one model they are, so it is listed once, and both stay offerings because both are callable. |
| `catalog.identity-reordered.json` | `catalog.identity.json` with every object's keys and every array reversed, whitespace removed, and two fields the adapter does not know added. Same normalized content, so the same content identity — and a different raw digest. |
| `drift.*.json` | One rejected payload each: malformed JSON, the wrong document shape, a missing required field, a type change, an unrecognized enumerated value, an id that does not match its key, a provider-local key that names two authored models at once, every way a price can be unusable or would be silently dropped, and text with no canonical form. None of them may replace last-known-good state. |

The excerpt is checked in rather than fetched so the suite is hermetic and the
seed is reviewable: `just test` never contacts models.dev, and neither does the
inference path.
