# Axond verification map

This directory is the maintained source for verifying the user-facing behavior of the axond HTTP gateway. Read the index before driving the app, then use the matching feature file as the recipe.

## Baseline preconditions

- Launch with `.cursor/skills/verify-axond/helpers/launch.sh` so the replica has a disposable SQLite file and a loopback bind that is not `:8080`.
- Export `AXOND_VERIFY_RUN` from launch stdout and `source .cursor/skills/verify-axond/helpers/env.sh`.
- Run `.cursor/skills/verify-axond/helpers/doctor.sh` and require the printed bind, version, and `healthz`/`readyz` lines.
- Never drive an instance that was not started by this verification run. Compose on `:8080` and a developer `axond.toml` are out of scope.
- Launch does not publish a budget. Recipes that dispatch inference must `PUT` one first, except when they are proving fail-closed admission.

## Driving conventions

- Start every recipe from the baseline state unless its preconditions say otherwise.
- Prefer route paths (`/healthz`, `/ns/platform/v1/models`, `/api/v1/namespaces`) over any other handle.
- Treat every command as literal. Keep quoted JSON and header names unchanged.
- Run HTTP through `helpers/drive.sh`. Capture Store rows with `helpers/store-get.py` and upstream hits via `upstream.jsonl`.
- Restore mutated namespaces after a create/delete recipe. Do not remove proof artifacts during cleanup.

## Proof and skip reporting

- Capture the user action and the resulting state, not only the final body.
- HTTP proof includes `request.txt`, `status`, `response.headers`, and `response.body`.
- Mutation proof includes a second GET (or a Store read) of the written value.
- Dispatch proof includes an `upstream.jsonl` line whose `path` and `model` match the rewritten provider call.
- Record the feature ID and entry point used with every artifact (`drive/<name>/`).
- Report an unreachable path with the attempted command and the unmet precondition.
- Do not report a skipped entry point as verified through a different path.

## Feature entry contract

Each feature file starts with an H1 title and one paragraph describing the user-visible behavior. It then uses exactly four H2 sections in this order.

1. `Sub-features` lists short IDs with one line for each behavior.
2. `How to get to it (user POV)` lists every user entry point.
3. `Driving it with drive.sh` starts with `Preconditions:` and uses labeled bullets that pair each user action with an exact command and observable result.
4. `Gotchas` lists traps that can waste or invalidate a verification run.

Keep implementation details out of the map. Name only user paths, stable handles, required state, commands, and observable proof.

## Features

- [Health and readiness](./health-readiness.md) covers unauthenticated `/healthz` and `/readyz`, and that every other route stays closed.
- [Authentication](./authentication.md) covers the deployment gateway key, `401 unauthorized`, the namespaced catalogue, and unmounted routes.
- [Period budgets](./period-budgets.md) covers fail-closed inference, publishing a period cap, and reading it back from HTTP and the Store.
- [Chat completions](./chat-completions.md) covers namespaced OpenAI chat dispatch against the fixture upstream, typed wire refusals, and secret non-leakage.
- [Namespaces](./namespaces.md) covers listing seeded namespaces, API create, and addressing `/ns/{id}/v1`.
