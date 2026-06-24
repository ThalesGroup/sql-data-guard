# sql-data-guard — Technical Documentation Set

> A multi-document technical analysis of the **sql-data-guard** project, generated for architecture reviews, knowledge transfer, onboarding, code reviews, and refactoring planning.

`sql-data-guard` is a **safety layer for LLM ↔ database interactions**. It accepts an SQL query plus a JSON restriction configuration and returns whether the query is *allowed*, a list of *errors*, an optional auto-*fixed* query, and a numeric *risk* score. It also detects malicious payloads (stacked queries, dangerous functions, system-catalog probing, comment evasion).

---

## Document map

| # | Document | Audience | What it covers |
|---|----------|----------|----------------|
| 00 | **[00-INDEX.md](00-INDEX.md)** | Everyone | This page — navigation, project summary, glossary |
| 01 | **[01-CODE_EXPLANATION.md](01-CODE_EXPLANATION.md)** | New developers, reviewers | Module-by-module, function-by-function walkthrough with reasoning |
| 02 | **[02-HLD.md](02-HLD.md)** | Architects, stakeholders | High-Level Design: purpose, components, data flow, deployment surfaces |
| 03 | **[03-LLD.md](03-LLD.md)** | Implementers | Low-Level Design: classes, signatures, algorithms, sequence diagrams |
| 04 | **[04-ARCHITECTURE.md](04-ARCHITECTURE.md)** | Architects | Architecture, design patterns, security model, performance, refactoring |
| 05 | **[05-CODING_GUIDELINES.md](05-CODING_GUIDELINES.md)** | Contributors | Conventions, security mindset, testing, PR checklist |

---

## Project at a glance

| Attribute | Value |
|-----------|-------|
| **Project type** | Library + Flask REST microservice + MCP (Model Context Protocol) stdio wrapper |
| **Language** | Python (>= 3.8) |
| **Core dependency** | [`sqlglot`](https://github.com/tobymao/sqlglot) (SQL parser / AST) |
| **Public API** | `verify_sql(sql: str, config: dict, dialect: str = None) -> dict` |
| **Distribution** | PyPI (`pip install sql-data-guard`) + Docker image (`ghcr.io/thalesgroup/sql-data-guard`) |
| **License** | MIT |
| **Validation strategy** | AST-based (sqlglot) + a thin pre-parse regex scan for comment evasion |

### Source layout

```text
src/sql_data_guard/
├── __init__.py                  # exports verify_sql
├── sql_data_guard.py            # orchestrator: verify_sql() + query traversal
├── injection_detection.py       # F1: malicious-payload scans (raw + AST)
├── restriction_validation.py    # config-shape validation (fail fast)
├── restriction_verification.py  # row-level restriction enforcement on the AST
├── column_masking.py            # column redact / hash / partial masking
├── verification_context.py      # mutable per-query result accumulator
├── verification_utils.py        # small AST traversal helpers
├── rest/
│   ├── sql_data_guard_rest.py   # Flask app + Swagger, POST /verify-sql
│   └── logging.conf
└── mcpwrapper/
    └── mcp_wrapper.py           # MCP stdio proxy around a containerised MCP server
```

---

## The core contract

```python
from sql_data_guard import verify_sql

config = {
    "tables": [{
        "table_name": "orders",
        "columns": ["id", "product_name", "account_id"],
        "restrictions": [{"column": "account_id", "value": 123}],
    }]
}

verify_sql("SELECT id, name FROM orders WHERE 1 = 1", config)
# {
#   "allowed": False,
#   "errors": [
#       "Column name is not allowed. Column removed from SELECT clause",
#       "Static expression is not allowed: 1 = 1",
#       "Missing restriction for table: orders column: account_id value: 123"
#   ],
#   "fixed": "SELECT id, product_name, account_id FROM orders WHERE account_id = 123",
#   "risk": <float 0..1>
# }
```

Return dictionary keys:

| Key | Type | Meaning |
|-----|------|---------|
| `allowed` | `bool` | `True` only when zero errors remain (and risk ≤ `max_risk` if set) |
| `errors` | `List[str]` | Ordered, de-duplicated human-readable findings |
| `fixed` | `Optional[str]` | A compliant rewrite, when all findings were auto-fixable |
| `risk` | `float` | Mean of per-finding risk weights, `0` (safe) .. `1` (high) |

---

## Glossary

| Term | Definition |
|------|-----------|
| **Restriction** | A row-level rule (`column`, `operation`, `value(s)`) that *must* appear in the query's `WHERE` clause; injected if missing. |
| **Fixable finding** | A violation the verifier can auto-correct (drop a column, add a restriction, clamp a limit). Surfaces in `fixed`. |
| **Hard block** | A non-fixable finding (stacked query, disallowed table, dangerous function). Forces `allowed = False`, `fixed = None`. |
| **Dynamic table / column** | Columns exposed by sub-queries, CTEs, lateral joins, `UNNEST` — resolved at verification time. |
| **Risk score** | Average of every finding's weight; compared against the optional `max_risk` config threshold. |
| **AST** | Abstract Syntax Tree produced by sqlglot from the raw SQL string. |
| **MCP** | Model Context Protocol — the stdio wrapper proxies tool calls and validates embedded SQL. |
