# Namespaces

Namespaces are the tenancy boundary. File TOML seeds `platform`. Further namespaces are created with `POST /api/v1/namespaces`. Callers address inference as `/ns/{id}/v1/...`. A missing id is a typed unknown-namespace error, not a shared catalogue.

## Sub-features

- `ns-list` lists seeded `platform` (and any API-created ids) on `GET /api/v1/namespaces`.
- `ns-create` creates an id with `attrs` and returns `201`.
- `ns-get` reads the same id back.
- `ns-store` shows the row in `axond_namespace`.
- `ns-unknown` refuses `/ns/ghost/v1/models` as a typed namespace miss after auth (not a catalogue leak).

## How to get to it (user POV)

- Getting-started: `POST /api/v1/namespaces` with `{"id":"wsp_demo","attrs":{"label":"demo"}}`.
- List with `GET /api/v1/namespaces` (cursor-paginated).
- `GET` / `PUT` / `DELETE /api/v1/namespaces/{ns}` for read, replace attrs, and idempotent delete.
- Inference and catalogue: `GET /ns/{id}/v1/models` using the same gateway key (one deployment key; namespace is in the path).

## Driving it with drive.sh

Preconditions:

- Axond is healthy at `$AXOND_VERIFY_BASE_URL`.
- `helpers/doctor.sh` passed.
- No namespace `wsp_verify` exists yet (fresh launch, or delete it first).

- **List seeded.** Run `.cursor/skills/verify-axond/helpers/drive.sh --name ns-list /api/v1/namespaces`. Status `200`. `data` includes an object with `"id":"platform"`.
- **Create.** Run `.cursor/skills/verify-axond/helpers/drive.sh --name ns-create --method POST --json '{"id":"wsp_verify","attrs":{"label":"verify"}}' /api/v1/namespaces`. Status `201`. Body `id` is `wsp_verify`.
- **Get.** Run `.cursor/skills/verify-axond/helpers/drive.sh --name ns-get /api/v1/namespaces/wsp_verify`. Status `200`. `attrs.label` is `verify`.
- **Store.** Run `python3 .cursor/skills/verify-axond/helpers/store-get.py --sqlite "$AXOND_VERIFY_SQLITE" --namespaces`. Output includes `platform` and `wsp_verify`. Copy to `$AXOND_VERIFY_EVIDENCE_DIR/store-namespaces.json`.
- **Address the new id.** Run `.cursor/skills/verify-axond/helpers/drive.sh --name ns-models /ns/wsp_verify/v1/models`. Status `200` with a list envelope (possibly empty `data`). Status is not `401` when the key is sent.
- **Unknown id.** Run `.cursor/skills/verify-axond/helpers/drive.sh --name ns-ghost /ns/ghost/v1/models`. Status is an error (not a `platform` catalogue). Typed `error.type` names the miss (`unknown_namespace` or equivalent); the body is not another tenant's models.
- **Anonymous create.** Run `.cursor/skills/verify-axond/helpers/drive.sh --name ns-create-anon --no-auth --method POST --json '{"id":"wsp_nope","attrs":{}}' /api/v1/namespaces`. Status `401`.
- **Proof.** Keep `drive/ns-create/response.body`, `drive/ns-get/response.body`, `store-namespaces.json`, and `drive/ns-ghost/response.body`. Create without a Store read or a second GET is incomplete.

## Gotchas

- Duplicate `POST` of the same id is a conflict, not `201`. Use `wsp_verify` once per run.
- `DELETE` is idempotent. After delete, `/ns/wsp_verify/v1/models` must not keep succeeding as that tenant.
- File-seeded `platform` cannot be replaced by this recipe's create step. Do not DELETE `platform` during verification.
- Namespace ids are validated; `wsp_verify` is a legal id. Spaces and mixed-case surprises fail as `400 bad_request`.
