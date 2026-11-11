# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

sql-data-guard is a safety layer for LLM-generated SQL. `verify_sql(sql, config, dialect)` (in `src/sql_data_guard/sql_data_guard.py`) parses a query with **sqlglot**, checks it against a JSON config of allowed tables/columns/row restrictions, and returns `{allowed, errors, fixed, risk}`. `fixed` is a rewritten query when the problems are fixable (disallowed SELECT columns dropped, `*` expanded, static conditions like `1=1` removed, missing restriction predicates injected). The only runtime dependency is `sqlglot`, bounded in `pyproject.toml`/`requirements.txt`; CI tests the oldest and newest allowed version, so check both when touching parsing code.

## Commands

Use the repo's `venv/` (or any env with `requirements.txt` + `test/test.requirements.txt`).

```sh
# all unit tests (what CI runs)
PYTHONPATH=src python -m pytest --color=yes test/*_unit.py

# single file / single test
PYTHONPATH=src python -m pytest test/test_sql_guard_unit.py -k "test_name"
```

- Test files are named `*_unit.py` (not `test_*.py` only), so pass them explicitly or by glob. `test_sql_guard_llm.py` needs AWS Bedrock credentials and is manual-only.
- Tests import `conftest.verify_sql_test`, which asserts exact `errors`, `fixed`, `risk` and can execute the (fixed) SQL against a sqlite/duckdb connection.
- Lint/types (CI runs both): `ruff check src test` and `mypy` (type-check with the oldest supported sqlglot, `pip install sqlglot==25.20.1`: newer releases change the type hierarchy). Coverage: add `--cov` to the pytest command (minimum 90%). Versions come from git tags (`pyproject.toml` has a placeholder version patched by the publish workflow).
- Work happens directly on the `dev` branch; `main` is the release branch.

## Architecture

The verification pipeline lives in `src/sql_data_guard/` and spans several modules:

- `sql_data_guard.py` – entry point and the recursive walk. `verify_sql` checks the config, length (`max_length`, default 10,000), parses with `sqlglot.parse` (more than one statement is rejected), rejects DML/DDL/commands, then `_verify_query_statement` recurses through unions, CTEs, FROM/JOIN subqueries, SELECT subqueries, WHERE subqueries and ORDER/GROUP/HAVING subqueries. The AST is mutated in place and `fixed` is `parsed.sql()` at the end.
- `verification_context.py` – per-call state: errors, a `can_fix` flag, risk scores (averaged), `dynamic_tables` (CTE/subquery/lateral aliases and their columns) and `cte_names` (the only dynamic names that may be referenced as a table).
- `restriction_verification.py` – checks that each configured restriction appears among the top-level AND conjuncts of WHERE and injects the missing ones. Comparisons are numeric.
- `restriction_validation.py` – validates the config's restriction operations; raises `UnsupportedRestrictionError` / `ValueError`, which `verify_sql` turns into an error result.
- `verification_utils.py` – `split_to_expressions`, `find_direct` (direct children only; prefer it over `find`, which searches nested subqueries too), and `table_matches` (name plus optional `database_name` match).

Key semantics to keep in mind:
- `add_error(msg, can_fix, risk)`: a `can_fix=False` error means `fixed` is `None` and the query is simply rejected. Disallowed columns in SELECT are fixable (removed); disallowed columns in WHERE/JOIN/GROUP BY/HAVING/ORDER BY are not.
- The column allow-list applies to every clause, not just SELECT (`_verify_other_clauses_columns`). Tests that filter on a column must include it in the test config's `columns`; note that adding a column to a config also changes `SELECT *` expansion.
- Functions are checked against a default deny-list plus `config["blocked_functions"]` (`_verify_functions`).
- `result["errors"]` is a sorted list. `verify_sql` never raises: unexpected errors become a rejection.
- `test/test_bypass_unit.py` holds adversarial scenarios executed against seeded sqlite data (fails if forbidden data comes back). Add new bypass attempts there; list unfixed ones in its `_KNOWN_*` tables as strict xfails.

Integrations built on the library:
- `rest/sql_data_guard_rest.py` – Flask `POST /verify-sql`; the client supplies the policy config, so it is not an enforcement point for untrusted callers (`Dockerfile`).
- `mcpwrapper/mcp_wrapper.py` – stdio proxy that runs a real MCP server in Docker, intercepts `tools/call`, and verifies/rewrites the SQL argument (`wrapper.Dockerfile`; examples in `examples/`).
- `plugins/dify/` – Dify tool plugin wrapping `verify_sql`; pins a published `sql_data_guard` version, so library changes do not reach it until released.

## Release

Push a git tag `vX.Y.Z`; `pypi-publish.yaml` patches the version, publishes to TestPyPI then PyPI. Docker images and the Dify plugin have their own workflows in `.github/workflows/`. User-visible and breaking changes go in `CHANGELOG.md`.
