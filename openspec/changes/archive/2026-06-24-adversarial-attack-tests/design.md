## Context

SQL Data Guard parses and rewrite SQL statements using the `sqlglot` library, enforcing column-level and row-level restrictions. While basic SELECT/JOIN queries are covered in the test suite, we need a robust adversarial test suite to ensure that malicious actors cannot bypass constraints through complex queries, long queries, stacked queries (with semicolons `;`), comment/whitespace obfuscation, and Common Table Expressions (CTEs).

## Goals / Non-Goals

**Goals:**
- Design and implement a robust, dedicated adversarial test suite under `tests/test_sql_guard_adversarial_unit.py`.
- Formulate a diverse set of test payloads covering:
  - Stacked queries (multiple statements separated by semicolons).
  - SQL injection patterns and comment obfuscation (`/**/`, inline `--` comments).
  - Deeply nested queries (parentheses/subqueries) and extremely long queries.
  - CTE trickery and complex subselects attempting to bypass restrictions.
- Ensure 100% of these test cases correctly block, rewrite, or safely raise exceptions instead of letting unauthorized access pass.

**Non-Goals:**
- Modifying the underlying parser (`sqlglot`) implementation itself, unless a severe parsing bug or safety loophole is identified.
- Changing production REST API code or Dify plug-in wrapper logic unless required to handle exceptions raised by adversarial queries.

## Decisions

### Decision 1: Separate Test Suite Module
We will create `tests/test_sql_guard_adversarial_unit.py` instead of adding to `tests/test_sql_guard_unit.py`.
- *Rationale:* Keeps the adversarial and security-focused tests separate and clear. Allows running specifically security/adversarial tests when auditing the security boundary of the project.

### Decision 2: Use pytest Parameterization
We will utilize `pytest.mark.parametrize` with rich test payloads (SQL, config, expected outcome, expected errors).
- *Rationale:* Enables adding a large volume of diverse test cases cleanly and programmatically, matching the style of existing test modules.

### Decision 3: SQLite Attachment and Verification
We will use a mocked or memory-backed SQLite connection (similar to `TestSingleTable` in `test_sql_guard_unit.py`) to execute rewrote queries.
- *Rationale:* Ensures that the rewritten SQL can be safely executed on actual SQLite, verifying that the semantic meaning is preserved and restrictions are successfully applied without database syntax errors.

## Risks / Trade-offs

### Risk: Python Recursion Limit / Parser Crashes on Deeply Nested Queries
- *Impact:* An extremely deep query AST could crash the validator or consume excessive memory.
- *Mitigation:* Explicitly verify how the system handles deep AST nesting, confirming it throws a clean parsing or validation exception rather than crashing the process.
