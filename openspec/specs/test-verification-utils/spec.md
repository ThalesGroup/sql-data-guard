# test-verification-utils Specification

## Purpose
TBD - created by archiving change test-verification-utils. Update Purpose after archive.
## Requirements
### Requirement: Test split_to_expressions function
The test suite residing in `/tests` SHALL verify the behavior of `split_to_expressions` with flattened and non-flattened `sqlglot` expressions.

#### Scenario: Split expression of matching type
- **WHEN** passing a nested expression of matching type (e.g. nested AND expressions)
- **THEN** the function yields each nested sub-expression individually

#### Scenario: Do not split expression of non-matching type
- **WHEN** passing an expression of a non-matching type
- **THEN** the function yields the expression itself unmodified

### Requirement: Test find_direct function
The test suite residing in `/tests` SHALL verify that `find_direct` correctly identifies and yields direct child expressions of a specified type from a parent expression.

#### Scenario: Find direct child matching type
- **WHEN** passing a parent expression containing direct matching child types
- **THEN** the function yields only those direct children

#### Scenario: Ignore indirect child types
- **WHEN** passing a parent expression with matching types nested inside other types (not direct children)
- **THEN** those nested types are ignored

