# Packaged copies of the shipped DDL — do not edit

The operator contract lives in [`ops/postgres/`](../../../ops/postgres). That is
what the deployment guide, the ADRs, and an operator's `psql -f` point at, and
ADR 0009 forbids editing a shipped file in place: a row-shape change is a new
`*_v<N>.sql`, never an edit.

`ops/` is outside `crates/gateway/`, so it cannot be part of the published
`axond` package, while the gateway has to embed the DDL it applies when
`create_table = true`. The files here are byte-identical copies that exist only
so the packaged crate compiles and ships the same schema.

So: change `ops/postgres/`, then copy the file here.

```bash
cp ops/postgres/<file>.sql crates/gateway/sql/<file>.sql
```

Two gates fail on drift or on a file that exists in only one of the two
directories — including a future `budget_v2.sql`:

- `crates/gateway/tests/shipped_ddl.rs`, in the normal test run;
- `ops/publish-crates.sh`, before any packaging or upload.
