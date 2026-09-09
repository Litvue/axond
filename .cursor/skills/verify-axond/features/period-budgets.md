# Period budgets

Period budgets are the spend cap a namespace must have before inference is admitted. With no row, chat completions fail closed. Publishing a cap is a `PUT` on the management API; a later `GET` and a Store read must show the same limit.

## Sub-features

- `budget-fail-closed` refuses chat completions with `429 budget_exceeded` when no period row exists.
- `budget-put` publishes `limit_microdollars` for a period id.
- `budget-get` returns the same ledger (`limit_microdollars`, `spent_microdollars`, `active`).
- `budget-store` shows the row in `axond_store_budget`.
- `budget-enables-dispatch` allows a later fixture chat request (see chat-completions) once a cap exists.

## How to get to it (user POV)

- `PUT /api/v1/namespaces/{ns}/budgets/{period}` with `{"limit_microdollars":...}` and the gateway key (getting-started and Compose quickstart).
- `GET /api/v1/namespaces/{ns}/budgets/{period}` to read the ledger.
- `PUT /api/v1/namespaces/{ns}/budget` for a cadence policy (`monthly` or `fixed`); monthly derives `YYYY-MM` and wins over the active-period marker.
- A missing cap is visible to callers as `429 budget_exceeded` on `/ns/{ns}/v1/chat/completions`, not as a management error.

## Driving it with drive.sh

Preconditions:

- Axond is healthy at `$AXOND_VERIFY_BASE_URL`.
- `helpers/doctor.sh` passed.
- This run has **not** yet published period `verify` for `platform` (fresh launch).
- Do not publish a budget before the fail-closed step.

- **Fail closed.** Send a chat request with no budget row. Run `.cursor/skills/verify-axond/helpers/drive.sh --name chat-no-budget --method POST --json '{"model":"fixture-openai/fixture-chat","messages":[{"role":"user","content":"hello"}]}' /ns/platform/v1/chat/completions`. Status `429`. `error.type` is `budget_exceeded`. `upstream.jsonl` has no new line for this attempt (admission never reached the fixture).
- **Publish cap.** Run `.cursor/skills/verify-axond/helpers/drive.sh --name put-budget --method PUT --json '{"limit_microdollars":1000000000000}' /api/v1/namespaces/platform/budgets/verify`. Status `200`. Body `namespace` is `platform`, `period` is `verify`, `limit_microdollars` is `1000000000000`, `spent_microdollars` is `0`, `active` is `true`.
- **Read back over HTTP.** Run `.cursor/skills/verify-axond/helpers/drive.sh --name get-budget /api/v1/namespaces/platform/budgets/verify`. Status `200`. `limit_microdollars` matches the PUT.
- **Read back from Store.** Run `python3 .cursor/skills/verify-axond/helpers/store-get.py --sqlite "$AXOND_VERIFY_SQLITE" --budget platform verify`. The JSON array contains one row with `limit_microdollars` `1000000000000`. Copy the output to `$AXOND_VERIFY_EVIDENCE_DIR/store-budget-verify.json`.
- **Anonymous PUT is closed.** Run `.cursor/skills/verify-axond/helpers/drive.sh --name put-budget-anon --no-auth --method PUT --json '{"limit_microdollars":1}' /api/v1/namespaces/platform/budgets/verify`. Status `401`.
- **Proof.** Keep `drive/chat-no-budget/response.body`, `drive/put-budget/response.body`, `drive/get-budget/response.body`, and `store-budget-verify.json`. Fail-closed plus matching GET/Store values is the feature; a PUT status alone is not.

## Gotchas

- Launch does not PUT a budget. If a previous recipe already published `verify`, fail-closed will not trigger; use a fresh run or a new period id (`verify2`).
- `remaining_microdollars` is `limit - spent`. `reserved_microdollars` is always `0`.
- Cadence `PUT /budget` (singular) is a different document from `PUT /budgets/{period}`. Do not treat them as the same entry point.
- Usage rows in `axond_store_usage` are not the budget ledger and may lag. Assert the budget table, not usage, for this feature.
