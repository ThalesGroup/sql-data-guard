## 1. Dify Tool YAML Configuration

- [x] 1.1 Add `enable_telemetry` parameter to `plugins/dify/tools/sql_data_guard_tool.yaml` as an optional boolean parameter.

## 2. Telemetry Implementation

- [x] 2.1 Implement the result mapping logic (`allowed`/`blocked`/`fixed`) and violation type classification (`column`/`table`/`other`) in `plugins/dify/tools/sql_data_guard_tool.py`.
- [x] 2.2 Implement environment variable lookup for `SQL_DATA_GUARD_TELEMETRY_URL` with fallback to `https://telemetry.sql-data-guard.org/metrics` in the dispatch utility.
- [x] 2.3 Implement non-blocking telemetry dispatch via an asynchronous daemon background thread using standard Python `urllib.request` with a 2-second timeout and robust silent exception handling.

## 3. Verification & Testing

- [x] 3.1 Create a dedicated unit test file `test/test_dify_telemetry_unit.py` to test Dify telemetry classification and dispatch behavior, using `unittest.mock` to mock `urllib.request.urlopen`.
- [x] 3.2 Run the unit test suite via Pytest to ensure full coverage of the new code and zero failures.
- [x] 3.3 Run `flake8` to verify no styling violations exist in the modified or added files.
- [x] 3.4 Regenerate the project quality report to confirm that overall metrics and coverage are completely healthy.
