# 01 · Code Explanation

> Module-by-module, function-by-function walkthrough of `src/sql_data_guard`. The goal is not to repeat the code, but to explain **what it does, why it exists, and how data moves through it**.

See also: [00-INDEX](00-INDEX.md) · [02-HLD](02-HLD.md) · [03-LLD](03-LLD.md)

---

## 1. `__init__.py` — the public surface

```python
from .sql_data_guard import verify_sql
```

The entire package exposes exactly **one** public symbol, `verify_sql`. Everything else is an implementation detail. This is a deliberate *facade*: callers depend on a stable single entry point while the internals are free to evolve.

---

## 2. `sql_data_guard.py` — the orchestrator

This is the heart of the system. It owns `verify_sql()` plus the recursive query-tree traversal that enforces table/column/restriction rules.

### 2.1 `verify_sql(sql, config, dialect=None) -> dict`

The end-to-end pipeline. Reading top to bottom:

1. **Config sanity gate.** If `config` is falsy, not a `dict`, or lacks `"tables"`, it returns a hard block immediately (`risk = 1.0`). *Why:* a missing config must never fail open.
2. **Length gate.** `max_length` (default `10_000`) caps the raw SQL string. *Why:* bounds parser cost and denial-of-service via gigantic inputs.
3. **Config-shape validation.** `validate_restrictions(config)` and `validate_column_masks(config)` run before any parsing. Errors here are hard blocks. *Why:* fail fast on operator misconfiguration rather than producing misleading query errors later.
4. **Context creation.** A fresh `VerificationContext` accumulates findings for this one query.
5. **Pre-parse raw scan.** `scan_raw_sql(sql, result)` looks for comment-evasion **before** sqlglot can discard the comment tail (the key bypass F1 closes).
6. **Parse.** `sqlglot.parse(sql, dialect)` returns a list of statements.
   - **> 1 statement** → explicit *stacked query* hard block.
   - **1 statement** → proceed.
   - **0 statements with no prior errors** → "Could not find a query statement".
   - **ParseError** → recorded as a `risk = 0.9` finding.
7. **AST scans + statement-type gating** (only when a single statement parsed):
   - `scan_parsed_sql` — always-on malicious-payload AST scan.
   - `_verify_functions` — config-driven function allow/deny (F2).
   - `Command` / `Delete` / `Insert` / `Update` / `Create` → not allowed (read-only posture).
   - `Query` (SELECT / set operations) → `_verify_query_statement` (the deep traversal).
8. **Force-limit enforcement.** `_enforce_force_limit` runs only if the query is still fixable and is a `Query` (F3).
9. **Auto-fix materialisation.** If the context is still fixable and there are findings, `result.fixed = parsed.sql(dialect)` renders the (mutated) AST back to SQL.
10. **Risk threshold (F5).** If `max_risk` is set and exceeded, the result is hard-blocked and `fixed` is suppressed — refusing to "auto-fix" a query the policy deems too risky.

> **Key insight:** the AST is *mutated in place* throughout traversal (columns removed, restrictions appended, masks substituted, limits set). The final `fixed` string is simply that mutated tree serialised back to SQL.

### 2.2 `_verify_functions(parsed, context)` — F2 function policy

Reads two optional config keys, matched case-insensitively:

- `blocked_functions` — any call is blocked.
- `allowed_functions` — if present, *only* these may be called.

Every `expr.Func` node is inspected via `function_name()`. A violation is a **hard block** (not auto-fixable) — stripping a function from an arbitrary position could silently change query semantics or produce invalid SQL. Composes with the always-on dangerous-function deny-list in `injection_detection`.

### 2.3 `_enforce_force_limit(parsed, context)` — F3 row cap

If `force_limit` is a positive `int` (and not a `bool`):

- No `LIMIT` → inject `LIMIT force_limit`.
- `LIMIT` above the cap → clamp down.
- `LIMIT` at/below the cap → leave unchanged.

Only the **outermost** statement is touched — sub-queries and CTEs are intentionally left alone, because the cap protects the final result set returned to the caller. This is a *fixable* finding (`risk = 0.3`).

### 2.4 The recursive traversal: `_verify_query_statement`

This is the recursion backbone. For a given `Query` node:

- **Set operations** (`UNION` / `EXCEPT` / `INTERSECT`, all `SetOperation` subclasses) → recurse into `left` and `right`. *Why:* each arm is its own SELECT that must independently satisfy table/column rules (S4).
- **CTEs** → register the alias *before* and *after* verifying the CTE body. The double registration captures columns expanded from `SELECT *` and lets forward/recursive references resolve (S3).
- **FROM tables** → `_verify_from_tables` confirms every table is allow-listed (or dynamic).
- When still fixable: verify the SELECT clause, the WHERE clause, then any sub-queries in ORDER/GROUP/HAVING/LIMIT/OFFSET.

### 2.5 SELECT-clause verification

`_verify_select_clause` / `_verify_select_clause_element` classify each projected expression:

| Element | Handling |
|---------|----------|
| `Star` (`*`) | `_expand_star` — replace with the allow-listed columns (minus `denied_columns`); records "SELECT * is not allowed" (fixable). |
| `Column` `t.*` | Qualified wildcard — same expansion, scoped to one table (F10). |
| `Column` | `_verify_col` (allow-list check) then `_apply_column_mask` (F-mask). |
| `Tuple` | Recurse over members. |
| Function / expression | Recurse into the inner `Column` nodes (so `SUM(col)` is checked on `col`). |

If the SELECT clause becomes empty after stripping, "No legal elements in SELECT clause" is a hard block.

### 2.6 `_verify_col` — column authorisation

A column is allowed if **any** of these hold:

1. It belongs to an allow-listed config table.
2. Its table is a dynamic (sub-select / CTE) source, and the column is one that source actually exposes.
3. It is un-prefixed but exposed by some dynamic source.
4. It is in `dynamic_columns`.

A column is **denied** outright (deny wins over allow) if it appears in `denied_columns` — stripped from SELECT with a fixable finding (F10). Disallowed columns are also stripped, fixable.

> **Security note (S3):** when *all* FROM tables are dynamic, the code does **not** blindly allow every column. It only allows columns the dynamic sources expose — closing a wildcard-trust bypass.

### 2.7 `_apply_column_mask` — F-mask rewrite

If a column maps unambiguously to exactly one masked table, its expression is rewritten (`build_mask_expression`) into the masking form while preserving the output column name. Ambiguous or table-qualified mismatches are skipped. The rewrite is fixable (`risk = 0.2`).

### 2.8 WHERE-clause + static-expression handling

- `_verify_where_clause` recurses into `Subquery`/`Exists` inside the WHERE, then enforces restrictions.
- `_verify_static_expression` / `_has_static_expression` detect **always-true** style payloads (`OR 1=1`): an OR-branch with no column reference is flagged "Static expression is not allowed" and replaced with `FALSE`, then `simplify()` collapses it. This is the classic injection-neutralising rewrite.

### 2.9 FROM-clause table discovery: `_get_from_clause_tables`

Walks the FROM clause and each JOIN. For sub-queries it **verifies first, then records exposed columns**, so the alias maps to *real allowed columns* rather than `*`. Handles `Lateral` and `Unnest` join sources too. `_add_table_alias` / `_register_dynamic_columns` populate the context's dynamic maps.

---

## 3. `verification_context.py` — the result accumulator

`VerificationContext` is a small mutable object threaded through the whole traversal.

| Member | Role |
|--------|------|
| `_errors: List[str]` | Ordered + **de-duplicated** findings (a list, not a set, so the API response is deterministic). |
| `_can_fix: bool` | Flips to `False` the moment any non-fixable finding is added. |
| `_risk: List[float]` | Per-finding weights; `risk` property returns their **mean**. |
| `_dynamic_tables` / `_dynamic_columns` | Columns exposed by sub-queries / CTEs / lateral / unnest. |
| `_column_masks` | `{table: {column: mask_spec}}` lookup built once from config. |

`add_error(error, can_fix, risk)` is the single mutation entry point: de-dups the message, lowers `can_fix`, and appends the risk weight.

> **Design caveat:** risk is an *average*. Adding several low-risk findings can dilute a single high-risk one. (See [04-ARCHITECTURE](04-ARCHITECTURE.md#risk-scoring) for the recommended max-weight refactor.)

---

## 4. `injection_detection.py` — F1 malicious-payload scans

Two complementary scans that **only report** (never mutate):

- **`scan_raw_sql`** (pre-parse, opt-in via `detect_comments` / `detect_injection.comments`): regex for `--`, `/* */`, `#`. Catches comment-evasion that hides a payload tail from sqlglot.
- **`scan_parsed_sql`** (AST, always-on):
  - `_scan_stacked_statements` — newer sqlglot wraps `a; b` in a `Block` node.
  - `_scan_dangerous_functions` — `_DANGEROUS_FUNCTIONS` frozenset (`load_file`, `xp_cmdshell`, `pg_sleep`, `sleep`, `benchmark`, `waitfor`, …).
  - `_scan_system_catalogs` — `information_schema`, `sqlite_master`, `pg_catalog`, etc.

`function_name(func)` is a cross-version helper: dialect-specific functions parse as `Anonymous` nodes whose real name is on `.name`, while built-ins expose it via `sql_names()`.

Risk weights are named constants (stacked `0.9`, dangerous fn `0.9`, comment `0.8`, catalog `0.8`).

---

## 5. `restriction_validation.py` — fail-fast config validation

Runs *before* parsing. Validates:

- `force_limit` is a positive non-bool int (F3).
- `allowed_functions` / `blocked_functions` are lists of strings (F2).
- Each table has `table_name` and non-empty `columns`.
- `denied_columns` is a list of strings (F10).
- Each restriction's operation is supported (`=`, `>`, `<`, `>=`, `<=`, `BETWEEN`, `IN`), with structural checks per operator.

`_validate_restriction` also **normalises the operation to upper case in place**, so configs can use `"between"` while downstream AST matching only compares upper-case operators (case-insensitive UX, single internal representation).

---

## 6. `restriction_verification.py` — enforcing row-level restrictions

`verify_restrictions` is where the WHERE clause is checked against configured restrictions.

1. Split the WHERE clause into its AND-ed sub-expressions.
2. For each `(config_table, from_table, restriction)`, look for a sub-expression that *satisfies* the restriction (`_verify_restriction`).
3. If none satisfies it → record "Missing restriction…" (fixable) and **inject** the condition (`_create_new_condition`), ANDing it onto the existing WHERE (or creating one).

`_verify_restriction` matches the query expression to the restriction:

- `IN` → all query values must be a subset of allowed values.
- `EQ` → the value must be in the allowed set.
- `BETWEEN` → query range must be *inside* the allowed range.
- `<`, `<=`, `>`, `>=` → operator must match the restriction's operator, and `_compare_values` compares **numerically when possible** (fixes the "9" < "18" string-sort bug, S2).

`_format_value` renders Python values as **safe SQL literals**, escaping embedded single quotes via sqlglot — preventing the fix itself from being an injection vector (S1).

---

## 7. `column_masking.py` — redact / hash / partial

- `validate_column_masks` — masked column must be in the table's `columns` allow-list; policy must be supported; `show_last` must be a non-negative int.
- `build_mask_lookup` — `{table: {column: mask_spec}}`.
- `build_mask_expression` — produces the dialect-portable masking expression, aliased back to the original output name:
  - **redact** → constant string (default `****`).
  - **hash** → `MD5(col)`.
  - **partial** → `CONCAT('****', SUBSTRING(CAST(col AS VARCHAR), -show_last))`.

> Masking keeps the **result shape** intact (same output column name), unlike dropping the column — so downstream consumers don't break.

---

## 8. `verification_utils.py` — tiny AST helpers

- `split_to_expressions(exp, type)` — flatten a binary AND/OR chain into its leaves.
- `find_direct(exp, type)` — yield only the *direct* children of a node matching a type (non-recursive), used to avoid pulling nested clauses out of context.

---

## 9. `rest/sql_data_guard_rest.py` — Flask REST API

- Single endpoint: `POST /verify-sql`, JSON body `{ sql, config, dialect? }`.
- Swagger UI via flasgger at `/apidocs`.
- Optional API-key auth (S6): if `SQL_GUARD_API_KEY` env var is set, requests must carry a matching `X-API-Key` header; otherwise open (dev default).
- Thin shim: validates request shape, calls `verify_sql`, JSON-ifies the result.

---

## 10. `mcpwrapper/mcp_wrapper.py` — MCP stdio proxy

Wraps a containerised MCP server (e.g. `mcp/sqlite`). On every `tools/call` it extracts the SQL argument, runs `verify_sql`, and:

- **Blocked + fixable** → forwards the `fixed` SQL.
- **Blocked + not fixable** → replaces with a `SELECT 'Blocked by SQL Data Guard'` message (optionally `UNION`-ing the error strings, or injecting them into the response when `inject-response` is on).

This lets an LLM agent talk to a real database server through a transparent validation layer, with no change to the agent.
