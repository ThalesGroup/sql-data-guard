## Why

The helper functions `split_to_expressions` and `find_direct` inside `src/sql_data_guard/verification_utils.py` are critical components for query parsing and expression manipulation, but they currently lack dedicated, isolated unit tests. Adding comprehensive unit tests ensures no regressions occur during core parsing updates.

## What Changes

- Create a new unit test suite file `test/test_verification_utils.py` with full coverage for both `split_to_expressions` and `find_direct`.

## Capabilities

### New Capabilities
- `test-verification-utils`: High-coverage unit testing suite for verification utilities.

### Modified Capabilities

## Impact

- `test/test_verification_utils.py`: New file containing the unit tests.
