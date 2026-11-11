# Changelog

## Unreleased

### Security
- Reject queries with more than one statement (`SELECT ...; DROP ...`) and empty input.
- Columns used in WHERE, JOIN ON, GROUP BY, HAVING, ORDER BY and QUALIFY are now checked against the allow-list.
- Removed the `sub_select` qualifier bypass. Qualified columns resolve strictly against the table named in FROM.
- Only CTE names can be referenced as tables; a subquery alias can no longer let an unlisted table through.
- A schema/database qualifier on a table must match the configured `database_name`.
- Block dangerous functions (file access, sleep, command execution, etc.). Extend with `blocked_functions` in the config.
- Tokenizer errors no longer raise; they return a rejection. Parse errors no longer log the SQL text.
- Restriction values are escaped for the SQL dialect.

### Fixed
- `<`, `>`, `<=`, `>=` and `BETWEEN` restrictions compare numerically, and `BETWEEN` no longer crashes on non-numeric values.
- Injected conditions use the configured comparison operator instead of always `=`.
- `IN` restrictions accept any non-empty list of numbers or strings.
- An invalid restriction config returns an error result instead of raising `ValueError`.

### Breaking changes
- Queries that filter, join, group or order by columns outside the allow-list are now rejected.
- `result["errors"]` is a sorted list instead of a set.
- `<`, `>` and `<=` restrictions require a numeric value (as `>=` already did).
