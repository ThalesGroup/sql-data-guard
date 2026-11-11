# sql-data-guard manual

`verify_sql(sql, config, dialect=None)` checks one SQL query against a policy (the *configuration*) and returns whether it may run. When the problems it finds can be repaired, it also returns a rewritten query.

```python
from sql_data_guard import verify_sql

result = verify_sql("SELECT id FROM orders", config, dialect="postgres")
if result["allowed"]:
    run(sql)
elif result["fixed"] is not None:
    run(result["fixed"])        # the repaired query
else:
    ...                         # cannot be repaired: do not run anything
```

`verify_sql` never raises: an invalid configuration, input it cannot parse, or any unexpected internal error is returned as `allowed: false`.

## Result

| Key | Meaning |
|-----|---------|
| `allowed` | `true` only if the query can run exactly as given. |
| `errors` | Sorted list of problems found (empty if allowed). |
| `fixed` | A repaired query, or `null` when there was nothing to fix **or** the query cannot be repaired. If `allowed` is `false` and `fixed` is `null`, do not run the query. |
| `risk` | Number from 0 (no risk) to 1 (high risk): the average of the risk of each problem found. |

A repaired query has been verified to comply with the policy: for example a disallowed column is dropped from `SELECT`, `SELECT *` is expanded to the allowed columns, an always-true condition such as `OR 1 = 1` is removed, and a missing row restriction is added to `WHERE`.

## Configuration

```json
{
  "tables": [
    {
      "table_name": "orders",
      "database_name": "orders_db",
      "columns": ["id", "product_name", "account_id"],
      "restrictions": [
        {"column": "account_id", "value": 123}
      ]
    }
  ],
  "max_length": 10000,
  "blocked_functions": ["my_dangerous_udf"]
}
```

| Key | Required | Description |
|-----|----------|-------------|
| `tables` | yes | Non-empty list of the tables the query may read. Any other table is rejected. A table must be listed only once. |
| `tables[].table_name` | yes | Table name. |
| `tables[].database_name` | no | Database/schema of the table. When set, a query that qualifies the table (`other_db.orders`) must use this name. |
| `tables[].columns` | yes | Non-empty list of allowed columns. See [What is checked](#what-is-checked). |
| `tables[].restrictions` | no | Row restrictions that must hold on every query of this table (see below). |
| `tables[].column_masks` | no | Columns returned masked instead of in clear text. See [Column masking](#column-masking). |
| `max_length` | no | Maximum SQL length in characters (default 10,000). Longer queries are rejected. |
| `blocked_functions` | no | Extra function names to reject, added to the built-in list. |
| `allowed_functions` | no | Allow-list mode: when present, only these functions may be called. See [Function policy](#function-policy). |
| `force_limit` | no | Maximum number of rows the outermost query may return. See [Row limit](#row-limit). |

### Identifier case

Unquoted table and column names are matched case-insensitively (`FROM ORDERS` matches `orders`), as in SQL. A quoted identifier must match the configuration exactly, so `"Orders"` is a different table than `orders`.

## Restrictions

A restriction forces a condition on the rows a query can read. If the query's `WHERE` clause does not contain it (as a top-level `AND` condition), the query is repaired by adding it:

```text
SELECT id FROM orders              ->  SELECT id FROM orders WHERE account_id = 123
SELECT id FROM orders WHERE x = 1  ->  SELECT id FROM orders WHERE (x = 1) AND account_id = 123
```

A restriction that is only present inside an `OR`, a `NOT`, or only in a nested query or CTE does **not** count: it has to hold for the table in the query that reads it.

Each restriction has a `column`, a `value` or `values`, and an optional `operation`:

| Operation | Config | A query satisfies it when its `WHERE` has |
|-----------|--------|-------------------------------------------|
| `=` (default) | `"value": 123` or `"values": [1, 2]` | `col = 123`, or `col IN (...)` using only allowed values |
| `IN` | `"values": ["eu", "us"]` | `col IN (...)` / `col = ...` using only values from the list. A non-empty list of numbers or strings. |
| `BETWEEN` | `"values": [100, 200]` | `col BETWEEN a AND b` inside the range. Two numbers, the first lower than the second. |
| `>`, `>=`, `<`, `<=` | `"value": 100` | A comparison of the same kind whose value is at least as strict. `value` must be a number. |

Comparisons are numeric (`9 > 10` is false). String values in the configuration are escaped for the dialect when a condition is added.

### Invalid configuration

An unsupported `operation`, a malformed `BETWEEN`/`IN`, a missing `table_name`/`columns`, an empty `tables` list, or a duplicated table makes `verify_sql` return `allowed: false` with a message explaining the problem (the validation itself is `restriction_validation.validate_restrictions`, which raises `UnsupportedRestrictionError` / `ValueError`).

## Column masking

Dropping a sensitive column from the result changes its shape and can break the caller. A *masked* column stays in the result under the same name, but its value is replaced, so the real value never leaves the database:

```json
{
  "table_name": "users",
  "columns": ["id", "name", "ssn"],
  "column_masks": [{"column": "ssn", "policy": "redact", "replacement": "****"}]
}
```

```text
SELECT id, ssn FROM users WHERE id = 1   ->   SELECT id, '****' AS ssn FROM users WHERE id = 1
SELECT * FROM users                      ->   SELECT id, name, '****' AS ssn FROM users   (plus the missing restriction)
```

- A masked column must also be listed in `columns`, cannot be used in a `restriction`, and can be masked only once. `replacement` defaults to `****` and is escaped for the dialect.
- `redact` is the only policy for now. Other policies (a partial or hashed value) need functions that are named differently in each database, so they will be added per dialect.
- A masked column may **only be selected directly**: `ssn`, `u.ssn`, `ssn AS x`, or through `SELECT *`. Every other use is rejected, because the mask could not be applied and the caller could otherwise learn the value a comparison at a time: `WHERE ssn LIKE 'a%'`, `ORDER BY ssn`, `GROUP BY`, `JOIN ... ON/USING`, and use inside a function or expression (`lower(ssn)`, `count(ssn)`, `CASE ... ssn ...`), including from a correlated sub-query that refers to the outer table's masked column.
- The mask is applied where the table is selected, so derived tables, CTEs, sub-queries and `UNION` branches that select the column return the masked value.
- A column that is not qualified and could come from several tables is masked if any of them masks it. A qualifier (`o.ssn`) decides which table is meant.

## Function policy

Function names are compared case-insensitively. Three layers apply, and a deny always wins:

1. A built-in deny-list (file access, command execution, sleeping, other servers, server details, `@@` variables).
2. `blocked_functions`: extra names to reject.
3. `allowed_functions` (optional): when present, **every** function call in the statement (including CTEs and sub-queries) must be in the list, so a function you did not think of is rejected by default. `"allowed_functions": []` allows no function at all.

```json
{"allowed_functions": ["count", "sum", "max", "upper", "round"]}
```

Notes:
- sqlglot normalises some names (for example `DATE_TRUNC` is reported as `TIMESTAMP_TRUNC`), and how aliases such as `SUBSTR`/`SUBSTRING` are treated can differ between sqlglot versions. The rejection message prints the name to put in the list; when unsure, list both spellings.
- A call qualified with a schema or package (`myschema.count(x)`) must be listed by its qualified name, so an unknown schema cannot use the permission of a bare name.
- Syntax that is not a call to a named function (`CASE`, `CAST(...)`, `EXISTS (...)`, `ARRAY[...]`, operators) is never restricted. A stand-alone `IF(a, b, c)` is a function and must be listed.
- Both lists must be lists of non-empty strings; anything else is a configuration error.

## Row limit

With `"force_limit": 100` the outermost query can never return more than 100 rows: a missing `LIMIT` is added, a larger one is lowered, and `OFFSET` is kept.

```text
SELECT id FROM orders WHERE account_id = 123                ->  ... LIMIT 100
SELECT id FROM orders WHERE account_id = 123 LIMIT 100000   ->  ... LIMIT 100
```

Anything that is not a plain positive integer is replaced too, because several databases treat it as "no limit": `LIMIT ALL`, `LIMIT NULL`, `LIMIT -1`, `LIMIT 5 + 1000000`, a sub-query as the limit, `FETCH FIRST n ROWS`. `UNION` queries are limited as a whole; sub-queries and CTEs are not changed (the cap is on the result returned to the caller). The change is reported as a fixable error, so the result has `allowed: false` and the capped query in `fixed`. `force_limit` must be a positive integer.

## Suggesting configuration changes

When a query is rejected because it uses a table or column the configuration does not allow, `suggest_config` tells you what would have to be added, and `merge_config` applies it. This lets a person review and approve exactly what a query needs, instead of editing the configuration by hand.

```python
from sql_data_guard import suggest_config, merge_config, verify_sql

config = {"tables": [{"table_name": "orders", "columns": ["id", "total"]}]}
sql = "SELECT emp_id, amount FROM salaries"

delta = suggest_config(sql, config, "postgres")
# {"tables": [{"table_name": "salaries", "columns": ["emp_id", "amount"]}]}

new_config = merge_config(config, delta)      # a copy; `config` is not modified
verify_sql(sql, new_config, "postgres")       # allowed
```

`suggest_config(sql, config, dialect=None)` returns:
- `{"tables": [...]}` with only what is missing: new tables with the columns the query uses (in any clause, in sub-queries, CTEs and joins), and new columns for tables already configured. A table in a database/schema keeps its `database_name`.
- `{"tables": []}` if the query needs nothing added.
- `None` when adding to the allow-list cannot make the query acceptable. This is the case for anything that is not a plain read-only query (several statements, `INSERT`/`DROP`/..., `SELECT INTO`), a blocked function, a column that cannot be attributed to one table (an unqualified column over several tables), and a table the query uses without naming any of its columns (`SELECT *`, `COUNT(*)`), because a configured table must list its columns.

The suggestion is checked before it is returned: `verify_sql` accepts the query with `merge_config(config, delta)`, and no table or column of the query is left outside the allow-list. Only `tables` and their `columns` are ever suggested; restrictions, masks, function lists and `force_limit` are never changed, and a rejection for one of those reasons gives `None`. `suggest_config` never raises.

`merge_config(config, delta)` returns a copy of `config` with the delta added: columns are added to the table of the same name (and `database_name`), other tables are appended, and duplicates (compared case-insensitively) are skipped. A malformed delta, or one with keys other than `table_name`, `database_name` and `columns`, raises `ValueError`, so a delta that was edited by hand cannot add restrictions or other settings.

Whether to apply a suggestion is a decision for a person: it widens what queries can read.

## What is checked

- **One statement, read-only.** Only a single `SELECT` (including `UNION`/`INTERSECT`/`EXCEPT` and `WITH`) is allowed. Multiple statements, `INSERT`/`UPDATE`/`DELETE`/DDL, commands (`PRAGMA`, `SET`, `COPY`, `ATTACH` ...), `SELECT ... INTO`, locking clauses (`FOR UPDATE`) and data-modifying CTEs are rejected.
- **Correlated sub-queries.** A column qualified with the alias of a table of an enclosing query (`o.notes` inside a sub-query) is checked against that table, not against the tables the sub-query selects from.
- **Tables.** Every table in `FROM`/`JOIN`, in every nested query, must be in `tables`. A CTE name can be used as a table only inside the query that defines it; it never makes a real table of the same name usable.
- **Columns, in every clause.** Columns in `SELECT`, `WHERE`, `JOIN ... ON`/`USING`, `GROUP BY`, `HAVING`, `ORDER BY` and window clauses must be allowed for their table. A disallowed column in `SELECT` is removed (a fixable error); anywhere else the query is rejected, because removing it would change the meaning of the query. Columns of derived tables and CTEs are checked where they come from.
- **Row restrictions** for each table, in each query that reads it (see above).
- **Always-true conditions** (`OR 1 = 1`, `OR 'a' = 'a'`) are removed.
- **Functions** (see [Function policy](#function-policy)). Calls to functions that read files, run commands, sleep, reach other servers, exhaust memory, or expose server details are rejected. The built-in list covers common cases (for example `pg_read_file`, `load_extension`, `pg_sleep`, `sleep`, `lo_import`, `dblink`, `set_config`, `getenv`, `read_csv`, `version`, `randomblob`, `UTL_*`/`DBMS_*` packages, `@@` system variables); add more with `blocked_functions`. This is a deny-list: for a database you do not fully trust the callers of, also use database permissions.
- **Limits.** `max_length` characters; any other unexpected problem is reported as `allowed: false`.

## Dialects

`dialect` is passed to [sqlglot](https://github.com/tobymao/sqlglot) for parsing and for generating the repaired query (`"sqlite"`, `"postgres"`, `"mysql"`, `"duckdb"`, `"trino"`, `"tsql"`, ...). Pass the dialect of the database that will run the query: the query is verified as parsed with that dialect, so a different dialect can interpret it differently.

## Scope and limits

- sql-data-guard verifies a query; it does not run it and does not replace database permissions. Use it as an extra layer, especially for row- and column-level rules the database cannot express.
- The configuration is trusted input. The REST service takes the configuration from the request, so it is not an enforcement point for untrusted callers.
- Unknown or unusual SQL fails closed: if the query cannot be verified, it is not allowed.
- Report vulnerabilities as described in [SECURITY.md](../SECURITY.md).
