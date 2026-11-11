# SQL Data Guard Plugin for Dify

This is a Dify Python plugin that integrates [sql-data-guard](https://github.com/thalesgroup/sql-data-guard) to validate SQL queries and enforce security policies.

## Overview

The SQL Data Guard plugin allows Dify users to enforce table/column/row restrictions, remove always-true conditions such as `OR 1 = 1`, and rewrite non-compliant queries. This is particularly useful when using LLMs to generate SQL queries, ensuring that only permitted data is accessed and SQL injection attacks are prevented.

For more information about the main project, visit the [sql-data-guard repository](https://github.com/thalesgroup/sql-data-guard).

## Plugin Inputs

- `sql` (string, required) – SQL query to validate  
- `config` (string, required) – Policy configuration; can be static or templated  
- `dialect` (string, optional) – SQL dialect for parsing

## Example

Config Input:
```json
{
  "tables": [
    {
      "table_name": "orders",
      "columns": ["id", "product_name", "account_id"],
      "restrictions": [{"column": "account_id", "value": 123}]
    }
  ]
}
```
SQL Input:
```sql
SELECT id, name FROM orders WHERE 1 = 1
```

Result Output Variables:

| Field        | Description | Example value |
|--------------|-------------|---------------|
| allowed      | Whether the query complies with the policy as it is | false |
| errors       | List of problems found (restricted columns, missing restrictions, always-true conditions...) | `Column name is not allowed. Column removed from SELECT clause`, `Missing restriction for table: orders column: account_id value: 123`, `Static expression is not allowed: 1 = 1` |
| fixed_sql    | The repaired query. Empty when the query was allowed, or when it cannot be repaired | `SELECT id FROM orders WHERE account_id = 123` |
| verified_sql | The query that is safe to run: the original if it was allowed, otherwise `fixed_sql` (empty when it cannot be repaired: do not run anything) | `SELECT id FROM orders WHERE account_id = 123` |
| risk         | Risk between 0 (none) and 1 (high) | 0.53 |

Run `verified_sql` only when it is not empty.

## Example Workflow

The following diagrams illustrate how SQL Data Guard protects your application when using LLMs to generate SQL queries:

### Without SQL Data Guard (Unsafe)
```
User Question
     ↓
   LLM
     ↓
Database ← (Unvalidated SQL)
     ↓
  Output
```
**Risk**: LLM-generated SQL could access restricted data or exploit injection vulnerabilities. The query runs directly on the database without any security validation, making it vulnerable to:
- Unauthorized data access
- SQL injection attacks
- Privilege escalation
- Accidental data exposure

### With SQL Data Guard (Secure)
```
User Question
     ↓
   LLM
     ↓
SQL Data Guard (Validates against Policy)
     ↓
  Policy Compliant?
   /        \
 YES        NO
  |          |
  ↓          ↓
 DB        Blocked
  |          |
  ↓          ↓
Output     Error Response
```
**Security**: The plugin validates every query before execution:
- **If compliant**: The validated query proceeds to the database for execution
- **If non-compliant**: The query is blocked and returns an error response, preventing unauthorized access
- **Query modification**: Non-compliant queries can be automatically fixed to comply with your policy (e.g., removing restricted columns, adding missing restrictions)

This ensures that only permitted data is accessed and SQL injection attacks are prevented.
