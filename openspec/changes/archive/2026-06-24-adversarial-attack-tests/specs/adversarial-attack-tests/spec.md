## ADDED Requirements

### Requirement: Stacked Query Rejection
The system SHALL detect and reject any query payload containing stacked queries or multiple independent SQL statements separated by semicolons to prevent stacked query injection attacks.

#### Scenario: Rejection of multiple statements
- **WHEN** a SQL input containing stacked queries (e.g., `"SELECT id FROM orders; DROP TABLE orders"`) is supplied to `verify_sql`
- **THEN** the system SHALL mark the query as not allowed and return appropriate validation errors

### Requirement: Complex CTE and Subquery Validation
The system SHALL correctly analyze, parse, and enforce column and table permissions, as well as missing row-level restrictions, within Common Table Expressions (CTEs) and deeply nested subqueries.

#### Scenario: Enforcing column restrictions in nested subquery
- **WHEN** a nested subquery includes unauthorized columns (e.g., `"SELECT id FROM (SELECT id, not_allowed FROM orders) WHERE id = 123"`)
- **THEN** the system SHALL detect and remove/reject the disallowed column `"not_allowed"`

#### Scenario: Enforcing row restrictions in Common Table Expressions
- **WHEN** a CTE query references restricted columns but lacks the required WHERE clause restriction (e.g., `"WITH cte AS (SELECT id, account_id FROM orders) SELECT id FROM cte"`)
- **THEN** the system SHALL reject the query or rewrite the SQL to include the required restriction `"account_id = 123"`

### Requirement: Heavy Nesting and Size Limits
The system SHALL safely reject exceptionally long, complex, or deeply nested queries that could result in recursion limits, stack overflows, or parser resource exhaustion.

#### Scenario: Rejection of extremely nested queries
- **WHEN** a query nested beyond safe AST limits (e.g., more than 100 levels of nested parentheses/subqueries) is sent for validation
- **THEN** the system SHALL reject the query cleanly with a parsing or validation error

### Requirement: Comment and Whitespace Obfuscation Parsing
The system SHALL correctly parse SQL inputs that employ comment-based, inline, or whitespace obfuscation techniques to ensure filters and checks cannot be bypassed.

#### Scenario: Normalizing comment obfuscated queries
- **WHEN** a query utilizing inline SQL comments (e.g., `"SELECT/**/id/**/FROM/**/orders/**/WHERE/**/id=123"`) is checked
- **THEN** the system SHALL parse the query accurately and properly enforce row and column permissions without being bypassed
