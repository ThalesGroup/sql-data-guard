# Changelog

## Unreleased

### Added
- `suggest_config(sql, config, dialect)` and `merge_config(config, delta)`: compute the table/column additions a rejected query needs, and apply them to a copy of the configuration (see the manual, "Suggesting configuration changes"). `suggest_config` returns `None` when widening the allow-list cannot make the query acceptable, and its result is verified with `verify_sql` before it is returned.
- Column masking: `column_masks` per table (`redact` policy). A masked column is returned as a constant under its own name when selected directly, and rejected in every other use.
- `allowed_functions` config option: allow-list mode for functions (deny-list and `blocked_functions` still win). `blocked_functions` and `allowed_functions` must be lists of non-empty strings.
- `force_limit` config option: caps the rows returned by the outermost query (adds, clamps or replaces `LIMIT`, including `LIMIT ALL`/`NULL`/`-1`, expressions and `FETCH FIRST`).

### Security
- Correlated sub-queries: a column qualified with an enclosing query's table alias (`o.notes`) is now checked against that table. Before, it was accepted if the sub-query's own table allowed a column of that name, giving an inference oracle on a forbidden column. Derived tables and `LATERAL` bodies are verified with the other tables of their FROM clause in scope.
- Reject queries with more than one statement (`SELECT ...; DROP ...`) and empty input.
- Columns used in WHERE, JOIN ON, GROUP BY, HAVING, ORDER BY and QUALIFY are now checked against the allow-list.
- Removed the `sub_select` qualifier bypass. Qualified columns resolve strictly against the table named in FROM.
- Only CTE names can be referenced as tables; a subquery alias can no longer let an unlisted table through.
- A schema/database qualifier on a table must match the configured `database_name`.
- Block dangerous functions (file access, sleep, command execution, etc.). Extend with `blocked_functions` in the config.
- Tokenizer errors no longer raise; they return a rejection. Parse errors no longer log the SQL text.
- Restriction values are escaped for the SQL dialect.
- An outer query no longer inherits the WHERE of a nested CTE/subquery, so its own restrictions are injected.
- A column selected inside a derived table/CTE no longer authorizes itself: aliases are registered after their body is verified, and visible only to the query that uses them.

- `JOIN ... USING (col)` columns are checked against the allow-list (also on the joined table).
- `SELECT ... INTO` and locking clauses (`FOR UPDATE`/`FOR SHARE`) are rejected.
- More functions are blocked by default (`set_config`, `getenv`, `randomblob`, `zeroblob`, `version`, `current_setting`, `sqlite_version`, `UTL_*`/`DBMS_*` packages) and `@@` system variables.
- `verify_sql` fails closed: any unexpected error (deep nesting, malformed config, non-string SQL) returns a rejection instead of raising.
- Duplicate table entries in the config are rejected instead of silently widening the allow-list.

- CTE names are scoped to the query that defines them: a CTE in one subquery no longer makes a real table of the same name usable elsewhere, and a non-recursive CTE cannot read the real table it shadows.
- CTE bodies on set operations (`WITH ... SELECT ... UNION ...`) are verified, and data-modifying CTEs (`WITH x AS (DELETE ...)`) are rejected explicitly.

### Fixed
- Unquoted table and column names match the config case-insensitively (`FROM ORDERS`); quoted names must match exactly.
- Recursive CTEs work again with scoped aliases.
- `<`, `>`, `<=`, `>=` and `BETWEEN` restrictions compare numerically, and `BETWEEN` no longer crashes on non-numeric values.
- Injected conditions use the configured comparison operator instead of always `=`.
- `IN` restrictions accept any non-empty list of numbers or strings.
- An invalid restriction config returns an error result instead of raising `ValueError`.

### Changed
- `sqlglot` is now bounded to `>=25.20.1,<31` (the range the test-suite passes on). CI tests the oldest and newest allowed versions, plus an informational job against the newest sqlglot ignoring the bound.
- CI: full unit-suite on the Python 3.8-3.12 matrix and on pull requests, coverage (minimum 90%), ruff and mypy.
- Release workflow retries the post-publish install instead of sleeping a fixed time, and also runs the bypass scenarios against the published package.

- Docs: `docs/manual.md` is now a full configuration and behavior reference, the README examples show real output (and no longer claim a payload-detection module), `SECURITY.md` and `CONTRIBUTING.md` describe the actual policy and workflow.
- CI: lint/type-check pinned to the oldest supported sqlglot; the Python compatibility matrix runs on `ubuntu-22.04` (older Python builds are not available on `ubuntu-latest`); workflow file typo fixed (`python-compatibility-test.yaml`).

- Tests that execute SQL on SQLite skip only when the installed SQLite is too old for the feature (`FULL/RIGHT JOIN` < 3.39, `CONCAT` < 3.44). The library logs through its own `sql_data_guard` logger instead of the root logger.

- REST service: bodies that are not JSON objects and wrongly typed fields return 400 instead of 500; optional bearer token (`APP_API_TOKEN`), request size limit, and it binds to `127.0.0.1` unless `APP_HOST` says otherwise (the Docker image sets `0.0.0.0`). An unknown or invalid `dialect` is rejected with a clear error instead of an internal one.
- The REST Docker image runs gunicorn (`gthread` workers, `WEB_CONCURRENCY`/`APP_THREADS`) instead of the Flask development server. gunicorn is installed in the image only; the library's dependencies are unchanged.
- Docker images: built only after a successful PyPI release, from the released commit, pinned to the released library version, tagged with the version (they were tagged `main`/`latest`), and the REST image runs as a non-root user.

### Breaking changes
- Queries that filter, join, group or order by columns outside the allow-list are now rejected.
- `result["errors"]` is a sorted list instead of a set.
- `<`, `>` and `<=` restrictions require a numeric value (as `>=` already did).
