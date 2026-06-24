## 1. Core Cypher Guard Module

- [x] 1.1 Create the new core module `src/sql_data_guard/cypher_guard.py` implementing robust, custom parsing/verification for Cypher queries.
- [x] 1.2 Implement a lexer/tokenizer to safely strip Cypher comments and string literals to prevent injection/bypassing of guard checks.
- [x] 1.3 Implement mutation check in `cypher_guard.py` to identify and block keywords: `CREATE`, `DELETE`, `DETACH`, `SET`, `REMOVE`, and `MERGE`.
- [x] 1.4 Implement label and relationship type extraction from patterns like `(n:Label)`, `(:Label)`, and `-[r:REL_TYPE]->`.
- [x] 1.5 Implement property extraction from patterns like `n.prop` and `{prop: value}` in query clauses.
- [x] 1.6 Implement schema validation matching extracted labels to `table_name` and properties to `columns`.
- [x] 1.7 Implement property-value restriction validation (matching equivalent logic to SQL's WHERE-clause restrictions).

## 2. Routing and REST Integration

- [x] 2.1 Update `verify_sql` in `src/sql_data_guard/sql_data_guard.py` to route queries to `verify_cypher` when `dialect` is `"cypher"` or `"neo4j"`.
- [x] 2.2 Validate that the FastAPI application in `src/sql_data_guard/rest/sql_data_guard_rest.py` correctly handles Cypher dialect in HTTP POST requests.

## 3. Testing and Quality Verification

- [x] 3.1 Create `tests/test_cypher_guard_unit.py` with comprehensive unit tests for mutation blocking, label whitelisting, property whitelisting, and property restrictions.
- [x] 3.2 Run the pytest unit tests specifically targetting the Cypher guard logic.
- [x] 3.3 Execute the general quality suite via `ruff check` and make sure it is compliant.
- [x] 3.4 Generate updated project-wide quality report using `python scripts/generate_quality_report.py`.
