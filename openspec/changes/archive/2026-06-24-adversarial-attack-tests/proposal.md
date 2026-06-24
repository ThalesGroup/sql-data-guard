## Why

The current SQL Data Guard testing suite lacks systematic, rigorous adversarial and security attack scenarios (such as SQL injection, stacked queries with semicolon `;`, complex CTE/subquery bypasses, extremely long queries, and unexpected functions) that stress-test the SQL guard library. Adding these adversarial scenarios is critical to ensuring the library is robust, secure, and cannot be bypassed.

## What Changes

- Add a new dedicated unit test suite focusing on security attacks and bypass attempts to comprehensively evaluate the library's security boundaries.
- Implement specific attack scenarios covering:
  - Stacked queries containing semicolons (`;`) or multiple statements to verify the library rejects or properly handles multiple commands.
  - Exceptionally long or deeply nested queries (complex ASTs) to ensure they are parsed correctly or safely rejected.
  - UNION-based queries, subselects, and CTE (Common Table Expressions) tricks aimed at bypassing restriction policies.
  - SQL injection patterns and comment/whitespace obfuscation (e.g. `/**/`, `--`, inline comments) attempting to bypass filtering.
  - Use of disallowed database functions or unauthorized catalog tables/metadata views.

## Capabilities

### New Capabilities
- `adversarial-attack-tests`: A comprehensive adversarial testing harness and test suite validating that SQL Data Guard successfully intercepts, rejects, or rewrites a wide variety of query bypass techniques.

### Modified Capabilities
