## 1. Fix Mypy and Ruff Issues in Source Code (src/)

- [x] 1.1 Fix implicit Optionals and Union type attribute accesses in `src/sql_data_guard/sql_data_guard.py`
- [x] 1.2 Resolve missing type annotations and Ruff violations in `src/sql_data_guard/restriction_verification.py`
- [x] 1.3 Update path usages and remove legacy `open` and string-based joins in `src/sql_data_guard/mcpwrapper/mcp_wrapper.py`
- [x] 1.4 Resolve logging config paths and `os.path` usages in `src/sql_data_guard/rest/sql_data_guard_rest.py`

## 2. Fix Mypy and Ruff Issues in Test Files (tests/)

- [x] 2.1 Standardize return and parameter type annotations on test fixtures and helper methods in `tests/test_utils.py`
- [x] 2.2 Fix test fixture return type annotations and implicit optionals in `tests/test_duckdb_unit.py`
- [x] 2.3 Add `-> None` return annotations to all test functions in `tests/test_sql_guard_unit.py` and other test files
- [x] 2.4 Resolve type signatures and Ruff path violations across all other unit and integration test files

## 3. Fix Mypy and Ruff Issues in Scripts

- [x] 3.1 Refactor `scripts/generate_quality_report.py` to use `pathlib.Path` APIs, simplify assignments, and add proper type annotations

## 4. Verification and Validation

- [x] 4.1 Verify all Ruff linting checks pass with zero errors using `uv run ruff check .`
- [x] 4.2 Verify all Mypy type checks pass with zero errors using `uv run mypy .`
- [x] 4.3 Run the full pytest suite to ensure no regressions were introduced
- [x] 4.4 Run the quality report script to generate an updated `QUALITY_REPORT.md`
