## 1. Setup and Test Suite Scaffolding

- [x] 1.1 Create the adversarial unit test file `tests/test_sql_guard_adversarial_unit.py` with standard imports and pytest/sqlite setup.

## 2. Stacked Query and Comment Obfuscation Tests

- [x] 2.1 Add test cases covering stacked queries containing semicolons (e.g. multiple statements, injection attempts).
- [x] 2.2 Add test cases covering inline comments and comment obfuscation (e.g. `/**/`, `--`) attempting to bypass filters.

## 3. CTE, Subquery, and Nesting Tests

- [x] 3.1 Add test cases verifying permissions and row restrictions inside CTEs and complex subqueries.
- [x] 3.2 Add test cases to check behavior of exceptionally long/deeply nested query syntax to ensure safe, graceful handling.

## 4. Verification

- [x] 4.1 Execute the new adversarial tests with `pytest` to confirm they pass correctly.
- [x] 4.2 Run the full suite of unit tests and regenerate the quality report to ensure there are no regressions.
