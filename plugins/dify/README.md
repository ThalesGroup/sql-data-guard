# SQL Data Guard Plugin

This is a Dify Python plugin that validates SQL queries using `sql-data-guard`.  
It allows users to enforce table/column restrictions and other policies dynamically.

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

| Field         | Value                                                                                                                                                                                                 |
|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| allowed       | false                                                                                                                                                                                                 |
| errors        | Column name not allowed. Column removed from SELECT clause;<br>Always-True expression is not allowed;<br>Missing restriction for table: orders column: account_id value: 123                           |
| fixed         | SELECT id, product_name, account_id FROM orders WHERE account_id = 123                                                                                                                               |
| verified_sql  | SELECT id, product_name, account_id FROM orders WHERE account_id = 123                                                                                                                               |
| risk          | 0.7                                                                                                                                                                                                  |
