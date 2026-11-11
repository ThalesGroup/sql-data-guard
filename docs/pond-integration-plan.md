# Plan: integrating sql-data-guard into Pond

Status: **plan only, nothing implemented.** Pond lives at `/Users/orin/PycharmProjects/pond`.

## Goal and scope

Pond is a local, single-user tool (UI + MCP). Once data is materialized as a dataset it is
local, so the guard is **not** needed there. The guard protects **external sources whose
credentials are over-permissive** (MySQL, Postgres, BigQuery, Athena) by enforcing a narrower
policy on the SQL that leaves Pond.

In scope (SQL sent to an external source):

- `dbconn.ingest_db_query` (`src/pond/dbconn.py`) - used by MCP `db_query` and
  `POST /api/datasets/database` for MySQL, Postgres, BigQuery.
- `athena.ingest_athena_query` and the passthrough variant - separate code path, used by MCP
  `db_query` and `POST /api/datasets/athena`.

Out of scope:

- `dataset_query`, `export_dataset`, browser-side DuckDB WASM (all work on cached local data).
- `local_db` datasources (read-only snapshot copies of uploaded files).
- Pond's internal metadata queries (`fetch_schema_catalog`, `find_table`, `information_schema`,
  `SELECT 1`, ingestion `CREATE VIEW`): these bypass the guard via a trusted flag.

## Behavior

### Policy
- Optional `sql_guard` object on each datasource: `{enabled, config}` where `config` is the
  sql-data-guard config (`tables`, `columns`, `restrictions`, `force_limit`,
  `blocked_functions`).
- No policy -> no-op, existing setups unchanged.
- Not exposed to the agent via `_DATASOURCE_SAFE_FIELDS`; the agent sees only rejection
  reasons and a status of `"on" | "off" | "none"` in `list_datasources`.
- Dialect map: `mysql`, `postgres`, `bigquery`, `athena`.
- If `verify_sql` returns a `fixed` query (columns dropped, `LIMIT`/`WHERE` injected), the fixed
  SQL is run, the response carries `query_rewritten: true` plus the fixed SQL, and the original
  and fixed text are kept in dataset metadata.
- The schema catalog output (`fetch_schema_catalog`, `find_table`) is filtered by the effective
  policy so the agent sees only allowed tables and columns.

### Approval flow
1. The agent's query is blocked. Pond stores a pending request (datasource, SQL, reasons,
   proposed config delta) and returns an error with the request id and a readable delta,
   for example `+ salaries: emp_id, amount`.
2. The user reviews it in the UI (badge + dialog showing the SQL and the new exposure), or tells
   the agent to approve.
3. Approval with a scope:
   - **session**: a temporary grant layered over the saved policy; cleared on backend restart.
   - **always**: the delta is merged into the datasource's saved `sql_guard.config`.
   - Default for the agent tool is `session`.
4. The agent retries and the query passes.

Effective policy = saved config + active session grants. `guard_sql` and the catalog filter
both use it.

Approvable: only widening the table/column allow-list. **Not approvable by anyone:** DML/DDL,
multiple statements, blocked functions.

Approval channels:
- UI: Approve (session / always), Edit, Deny.
- MCP tool `approve_sql_guard_request(request_id, scope="session" | "always")`. The tool
  description says to call it only after the user said yes in chat. The real gate is the MCP
  client's per-call permission prompt, so this tool must **not** be allow-listed in
  `.claude/settings` / `.mcp.json`. Document this in the docs and Pond's `CLAUDE.md`.

Audit: every approval logs timestamp, SQL, delta, scope and `approved_by: ui | agent`. The UI
lists active session grants (with **revoke**) and saved widenings (with **revert**).

### Turning it off
The guard is a security layer and must be switchable. It runs for a datasource only if all of
these hold:

1. `POND_SQL_GUARD` env is not `off` (deployment override, read at startup; set in Docker /
   `start_server.sh` args, **not** in the secret `server.env`).
2. The global UI setting is on.
3. The datasource's `sql_guard.enabled` is true.

Turning it off keeps the saved policy and grants. Only UI/REST can change these switches;
there is **no MCP tool** to disable the guard. When off: nothing is blocked, no approval
requests are created, the catalog is not filtered, pending requests are marked inactive. The UI
shows a persistent banner/badge while the guard is off. Toggles are logged.

### Trust boundary (document it)
An agent with shell access on the same machine could call the REST endpoints directly, and a
client configured to auto-approve tools would defeat the approval gate. These are process
boundaries, not hard ones. The guard also does not replace read-only database credentials.

## Work breakdown

### 1. sql-data-guard (this repo, `dev`, then release)
- `suggest_config(sql, config, dialect)`: minimal allow-list extension that makes the query
  pass; returns nothing for non-approvable violations. Built on the existing AST walk.
- `merge_config(config, delta)`: shared merge logic for both repos.
- Optionally structured `violations` in the `verify_sql` result for the UI.
- Tests, including the invariant `verify_sql(sql, merge_config(config, suggestion))` is
  allowed, and non-approvable cases return no suggestion. Add bypass cases to
  `test/test_bypass_unit.py` where relevant. Check oldest and newest allowed sqlglot.
- `CHANGELOG.md` entry; tag `vX.Y.Z` (Pond pins a published version).

### 2. Pond
1. Bump `sql-data-guard` in `requirements.txt` (published 0.1.9 at the time of writing; need
   the release from step 1). Keep Pond's `sqlglot` bound compatible with `>=25.20.1,<31`.
2. New `src/pond/sqlguard.py`: `guard_sql(sql, ds, trusted=False)`, `is_active(ds)`, effective
   policy, pending-request store, `SqlGuardError`.
3. Datasource models in `main.py`: optional `sql_guard` on create/patch.
4. Wire `guard_sql` into `dbconn.ingest_db_query` and the Athena ingest paths; internal
   metadata queries pass `trusted=True`.
5. Filter `fetch_schema_catalog` / `find_table` output by effective policy.
6. REST: pending, approve, deny, revoke, revert, global and per-datasource toggle. MCP: only
   `approve_sql_guard_request`. Map `SqlGuardError` to a structured MCP error and HTTP 403.
7. UI: pending badge, review dialog, grants list, toggles, off-banner.
8. Tests in `tests/test_sqlguard_unit.py` (Pond convention `*_unit.py`): no policy = unchanged,
   blocked table, dropped column, injected `LIMIT`, dialect mapping, each ingest path receives
   guarded SQL, trusted bypass, approve session vs always, revoke/revert, non-approvable,
   all three off-levels, policy survives off/on, no MCP tool disables the guard, audit entries.
9. Docs: README, `usage_guide.md`, `SECURITY.md`, Pond `CLAUDE.md` (do not allow-list the
   approval tool).

## To verify before building
- Are the MCP server and the UI backend the same process? Session grants held in memory only
  work if so. `.mcp.json` and the Docker scripts suggest they may be separate. If separate, use
  a small shared store (for example a file in `cache/` stamped with a boot id so a restart
  clears it).
- Dialect coverage: Athena, BigQuery backtick quoting, DuckDB-style functions.
- Whether the config needs a "read-only, any table" mode (today `verify_sql` requires
  `tables`). Could be generated from the schema catalog or added upstream.
