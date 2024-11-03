# sql-guard

sql-guard is an open-source project designed to verify that SQL queries access only the data they are allowed to. It takes a query and a restriction configuration, and returns whether the query is allowed to run or not. Additionally, it can modify the query to ensure it complies with the restrictions.

## Why Use sql-guard?

Consider using sql-guard if your application constructs SQL queries and you need to ensure that only permitted data is accessed. This is particularly beneficial if:
- Your application generates complex SQL queries.
- Your application employs LLM (Large Language Models) to create SQL queries, making it difficult to fully control the queries.
- Different application users should have different permissions, and it is often hard to correlate an application user or role with a database user or role.

sql-guard does not replace the database permissions model. Instead, it adds an extra layer of security, which is crucial when implementing fine-grained, column-level, and row-level security is challenging or impossible. Data restrictions are often complex and cannot be expressed by the database permissions model. For instance, you may need to restrict access to specific columns or rows based on intricate business logic, which many database implementations do not support. Instead of relying on the database to enforce these restrictions, sql-guard helps you overcome vendor-specific limitations by verifying and modifying queries before they are executed.

## How It Works

1. **Input**: sql-guard takes an SQL query and a restriction configuration as input.
2. **Verification**: It verifies whether the query complies with the restrictions specified in the configuration.
3. **Modification**: If the query does not comply, sql-guard can modify the query to ensure it meets the restrictions.
4. **Output**: It returns whether the query is allowed to run or not, and if necessary, the modified query.

## Example

```python
from sql_guard import verify_sql

config = {
    "tables": {
        "orders": {
            "columns": ["id", "product_name", "account_id"],
            "restrictions": [{"column": "account_id", "value": 123}]
        }
    }           
}

query = "SELECT * FROM orders WHERE account_id = 123"
result = verify_sql(query, config)
print(result)
```
Output:
```json
{
    "allowed": false,
    "errors": ["SELECT * is not allowed"],
    "fixed": "SELECT id, product_name, account_id FROM orders WHERE account_id = 123"
}
```
Here is a table with more examples of SQL queries and their corresponding JSON outputs:

| SQL Query                                                    | JSON Output                                                                                                                                                                                  |
|--------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SELECT id, product_name FROM orders WHERE account_id = 123` | ```{ "allowed": true, "errors": [], "fixed": null } ```                                                                                                                                      |
| `SELECT id FROM orders WHERE account_id = 456`               | ```{ "allowed": false, "errors": ["Missing restriction for table: orders column: account_id value: 123"], "fixed": "SELECT id FROM orders WHERE account_id = 456 AND account_id = 123" } ``` |
| `SELECT id, col FROM orders WHERE account_id = 123`          | ```{ "allowed": false, "errors": ["Column col is not allowed. Column removed from SELECT clause"], "fixed": "SELECT id FROM orders WHERE account_id = 123" } ```                             |
| `SELECT id FROM orders WHERE account_id = 123 OR 1 = 1`      | ```{ "allowed": false, "errors": ["Always-True expression is not allowed"], "fixed": "SELECT id FROM orders WHERE account_id = 123" } ```                                                    |


This table provides a variety of SQL queries and their corresponding JSON outputs, demonstrating how `sql-guard` handles different scenarios.

## Installation
To install sql-guard, use pip:

```bash
pip install sql-guard
```

## Docker Repository

sql-guard is also available as a Docker image, which can be used to run the application in a containerized environment. This is particularly useful for deployment in cloud environments or for maintaining consistency across different development setups.

### Running the Docker Container

To run the sql-guard Docker container, use the following command:

```bash
docker run -d --name sql-guard -p 8080:8080 imperva/sql-guard:latest

### Calling the Docker Container Using REST API

Once the `sql-guard` Docker container is running, you can interact with it using its REST API. Below is an example of how to verify an SQL query using `curl`:

```bash
curl -X POST http://localhost:8080/verify_sql \
     -H "Content-Type: application/json" \
     -d '{
           "query": "SELECT * FROM orders WHERE account_id = 123",
           "config": {
             "tables": {
               "orders": {
                 "columns": ["id", "product_name", "account_id"],
                 "restrictions": [{"column": "account_id", "value": 123}]
               }
             }
           }
         }'
```

## Contributing
We welcome contributions! Please see our [CONTRIBUTING.md](CONTRIBUTING.md) for more details.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
