## Context

The utility functions inside `src/sql_data_guard/verification_utils.py` utilize `sqlglot` expressions. To test these utility functions correctly and reliably, we need to construct specific valid `sqlglot` expressions and pass them to our functions.

## Goals / Non-Goals

**Goals:**
- Implement dedicated pytest unit tests for `split_to_expressions` and `find_direct`.
- Achieve 100% code coverage on `src/sql_data_guard/verification_utils.py`.

**Non-Goals:**
- Testing database connection behavior or queries outside parsing helpers.

## Decisions

### Decision 1: Use `sqlglot.parse_one` to construct test ASTs
Instead of manually building `sqlglot.expressions.Expression` objects (which is verbose and error-prone), we will use `sqlglot.parse_one()` to construct real AST objects from SQL string fragments (e.g., `x AND y AND z` for split tests). This guarantees realistic and valid inputs for the functions being tested.
